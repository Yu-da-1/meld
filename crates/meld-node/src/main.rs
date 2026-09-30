mod controller_client;
mod identity;
mod resource_reporter;

use std::{env, error::Error, io, time::Duration};

use meld_core::{NodeDescriptor, ResourceCapacity};
use tokio::time::{MissedTickBehavior, interval, sleep};
use tracing_subscriber::EnvFilter;

use crate::{
    controller_client::ControllerClient, identity::load_or_create_node_id,
    resource_reporter::ResourceReporter,
};

const HEARTBEAT_INTERVAL_ENV: &str = "MELD_HEARTBEAT_INTERVAL_SECS";
const DEFAULT_HEARTBEAT_INTERVAL_SECS: u64 = 5;
const INITIAL_RECONNECT_DELAY: Duration = Duration::from_secs(1);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("meld_node=info")),
        )
        .init();

    let controller_url =
        env::var("MELD_CONTROLLER_URL").unwrap_or_else(|_| "http://127.0.0.1:3000".to_owned());
    let heartbeat_interval = heartbeat_interval_from_env()?;
    let mut resource_reporter = ResourceReporter::new();
    let descriptor = local_node_descriptor(&resource_reporter)?;
    let node_id = descriptor.id;
    let client = ControllerClient::new(&controller_url)?;

    run_node(
        &client,
        descriptor,
        heartbeat_interval,
        &mut resource_reporter,
    )
    .await?;
    tracing::info!(%node_id, "node shut down");
    Ok(())
}

async fn run_node(
    client: &ControllerClient,
    descriptor: NodeDescriptor,
    heartbeat_interval: Duration,
    resource_reporter: &mut ResourceReporter,
) -> Result<(), Box<dyn Error>> {
    let node_id = descriptor.id;
    let mut backoff = ReconnectBackoff::new(INITIAL_RECONNECT_DELAY, MAX_RECONNECT_DELAY);

    'connection: loop {
        let registration = tokio::select! {
            result = client.register(&descriptor) => result,
            shutdown = tokio::signal::ctrl_c() => {
                shutdown?;
                return Ok(());
            }
        };

        match registration {
            Ok(()) => {
                backoff.reset();
                tracing::info!(%node_id, "node registration accepted");
            }
            Err(error) if error.is_retryable() => {
                let delay = backoff.next_delay();
                tracing::warn!(%node_id, %error, retry_in_secs = delay.as_secs(), "registration failed");
                if wait_for_retry_or_shutdown(delay).await? {
                    return Ok(());
                }
                continue;
            }
            Err(error) => return Err(error.into()),
        }

        let mut ticker = interval(heartbeat_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            let (snapshot, heartbeat) = tokio::select! {
                result = async {
                    ticker.tick().await;
                    let snapshot = resource_reporter.snapshot();
                    let heartbeat = client.heartbeat(node_id, snapshot).await;
                    (snapshot, heartbeat)
                } => result,
                shutdown = tokio::signal::ctrl_c() => {
                    shutdown?;
                    return Ok(());
                }
            };

            match heartbeat {
                Ok(()) => {
                    tracing::debug!(
                        %node_id,
                        cpu_usage_percent = snapshot.cpu_usage_percent,
                        available_memory_bytes = snapshot.available_memory_bytes,
                        "heartbeat acknowledged"
                    );
                }
                Err(error) if error.requires_registration() => {
                    tracing::warn!(%node_id, %error, "controller requested node re-registration");
                    continue 'connection;
                }
                Err(error) if error.is_retryable() => {
                    let delay = backoff.next_delay();
                    tracing::warn!(%node_id, %error, retry_in_secs = delay.as_secs(), "heartbeat failed");
                    if wait_for_retry_or_shutdown(delay).await? {
                        return Ok(());
                    }
                    continue 'connection;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
}

async fn wait_for_retry_or_shutdown(delay: Duration) -> io::Result<bool> {
    tokio::select! {
        _ = sleep(delay) => Ok(false),
        shutdown = tokio::signal::ctrl_c() => {
            shutdown?;
            Ok(true)
        }
    }
}

#[derive(Debug)]
struct ReconnectBackoff {
    initial: Duration,
    current: Duration,
    maximum: Duration,
}

impl ReconnectBackoff {
    fn new(initial: Duration, maximum: Duration) -> Self {
        Self {
            initial,
            current: initial,
            maximum,
        }
    }

    fn next_delay(&mut self) -> Duration {
        let delay = self.current;
        self.current = self.current.saturating_mul(2).min(self.maximum);
        delay
    }

    fn reset(&mut self) {
        self.current = self.initial;
    }
}

fn local_node_descriptor(
    resource_reporter: &ResourceReporter,
) -> Result<NodeDescriptor, Box<dyn Error>> {
    let hostname = hostname::get()?.to_string_lossy().into_owned();
    if hostname.trim().is_empty() {
        return Err(io::Error::other("local hostname is empty").into());
    }

    let logical_cpus = u32::try_from(std::thread::available_parallelism()?.get())
        .map_err(|_| io::Error::other("logical CPU count exceeds u32"))?;

    Ok(NodeDescriptor {
        id: load_or_create_node_id()?,
        hostname,
        operating_system: env::consts::OS.to_owned(),
        architecture: env::consts::ARCH.to_owned(),
        capacity: ResourceCapacity {
            logical_cpus,
            memory_bytes: resource_reporter.total_memory(),
        },
    })
}

fn heartbeat_interval_from_env() -> io::Result<Duration> {
    match env::var(HEARTBEAT_INTERVAL_ENV) {
        Ok(value) => parse_heartbeat_interval(&value),
        Err(env::VarError::NotPresent) => Ok(Duration::from_secs(DEFAULT_HEARTBEAT_INTERVAL_SECS)),
        Err(env::VarError::NotUnicode(_)) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{HEARTBEAT_INTERVAL_ENV} must contain valid Unicode"),
        )),
    }
}

fn parse_heartbeat_interval(value: &str) -> io::Result<Duration> {
    let seconds = value.parse::<u64>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{HEARTBEAT_INTERVAL_ENV} must be a positive integer: {error}"),
        )
    })?;
    if seconds == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{HEARTBEAT_INTERVAL_ENV} must be greater than zero"),
        ));
    }

    Ok(Duration::from_secs(seconds))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_interval_must_be_positive() {
        let error = parse_heartbeat_interval("0").expect_err("zero interval must be rejected");

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn reconnect_backoff_grows_to_maximum_and_resets() {
        let mut backoff = ReconnectBackoff::new(Duration::from_secs(1), Duration::from_secs(4));

        assert_eq!(backoff.next_delay(), Duration::from_secs(1));
        assert_eq!(backoff.next_delay(), Duration::from_secs(2));
        assert_eq!(backoff.next_delay(), Duration::from_secs(4));
        assert_eq!(backoff.next_delay(), Duration::from_secs(4));

        backoff.reset();
        assert_eq!(backoff.next_delay(), Duration::from_secs(1));
    }
}
