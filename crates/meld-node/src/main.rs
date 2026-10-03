mod controller_client;
mod executor;
mod identity;
mod resource_reporter;

use std::{env, error::Error, io, time::Duration};

use meld_core::{
    ExecutionAssignment, ExecutionEvent, ExecutionOutput, ExecutionResult, NodeCommand,
    NodeDescriptor, ResourceCapacity,
};
use tokio::{
    sync::oneshot,
    time::{Instant as TokioInstant, MissedTickBehavior, interval, sleep, sleep_until},
};
use tracing_subscriber::EnvFilter;

use crate::{
    controller_client::{ControllerClient, ControllerClientError},
    executor::{CompletedExecution, ControlledExecutionOutcome, NativeExecutor},
    identity::{load_or_create_node_id, state_directory},
    resource_reporter::ResourceReporter,
};

const HEARTBEAT_INTERVAL_ENV: &str = "MELD_HEARTBEAT_INTERVAL_SECS";
const DEFAULT_HEARTBEAT_INTERVAL_SECS: u64 = 5;
const COMMAND_POLL_RETRY_DELAY: Duration = Duration::from_secs(1);
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
    let executor = NativeExecutor::new(&state_directory()?);

    run_node(
        &client,
        &executor,
        descriptor,
        heartbeat_interval,
        &mut resource_reporter,
    )
    .await?;
    tracing::info!(%node_id, "node shut down");
    Ok(())
}

struct ActiveExecution {
    execution_id: meld_core::ExecutionId,
    task: tokio::task::JoinHandle<()>,
    cancellation: Option<oneshot::Sender<()>>,
}

async fn run_node(
    client: &ControllerClient,
    executor: &NativeExecutor,
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
        let mut command_poll_not_before = TokioInstant::now();
        let mut active_execution = None;
        loop {
            let running_executions = u32::from(active_execution.is_some());
            let active_execution_id = active_execution
                .as_ref()
                .map(|execution: &ActiveExecution| execution.execution_id);
            tokio::select! {
                heartbeat = async {
                    ticker.tick().await;
                    let snapshot = resource_reporter.snapshot_with_running_executions(
                        running_executions,
                    );
                    let heartbeat = client.heartbeat(node_id, snapshot).await;
                    (snapshot, heartbeat)
                } => {
                    let (snapshot, heartbeat) = heartbeat;
                    match heartbeat {
                        Ok(()) => {
                            backoff.reset();
                            tracing::debug!(
                                %node_id,
                                cpu_usage_percent = snapshot.cpu_usage_percent,
                                available_memory_bytes = snapshot.available_memory_bytes,
                                running_executions = snapshot.running_executions,
                                "heartbeat acknowledged"
                            );
                        }
                        Err(error) if error.requires_registration() => {
                            tracing::warn!(%node_id, %error, "controller requested node re-registration");
                            abort_execution(&mut active_execution).await;
                            continue 'connection;
                        }
                        Err(error) if error.is_retryable() => {
                            let delay = backoff.next_delay();
                            tracing::warn!(%node_id, %error, retry_in_secs = delay.as_secs(), "heartbeat failed");
                            if wait_for_retry_or_shutdown(delay).await? {
                                abort_execution(&mut active_execution).await;
                                return Ok(());
                            }
                            continue;
                        }
                        Err(error) => {
                            abort_execution(&mut active_execution).await;
                            return Err(error.into());
                        }
                    }
                }
                command = async {
                    sleep_until(command_poll_not_before).await;
                    client.poll_node_command(node_id, active_execution_id).await
                } => {
                    if command.is_ok() {
                        command_poll_not_before = TokioInstant::now();
                    }
                    match command {
                        Ok(Some(NodeCommand::Start { assignment })) => {
                            tracing::info!(
                                %node_id,
                                job_id = %assignment.job_id,
                                execution_id = %assignment.execution_id,
                                "execution assignment received"
                            );
                            let client = client.clone();
                            let executor = executor.clone();
                            let execution_id = assignment.execution_id;
                            let (cancellation, cancellation_receiver) = oneshot::channel();
                            let task = tokio::spawn(async move {
                                run_assignment(
                                    &client,
                                    &executor,
                                    assignment,
                                    cancellation_receiver,
                                )
                                .await;
                            });
                            active_execution = Some(ActiveExecution {
                                execution_id,
                                task,
                                cancellation: Some(cancellation),
                            });
                        }
                        Ok(Some(NodeCommand::Cancel { execution_id })) => {
                            if let Some(active) = active_execution.as_mut()
                                && active.execution_id == execution_id
                                && let Some(cancellation) = active.cancellation.take()
                            {
                                let _ = cancellation.send(());
                                tracing::info!(%node_id, %execution_id, "execution cancellation requested");
                            }
                        }
                        Ok(None) => {}
                        Err(error) if error.requires_registration() => {
                            tracing::warn!(%node_id, %error, "controller requested node re-registration");
                            continue 'connection;
                        }
                        Err(error) if error.is_retryable() => {
                            tracing::warn!(%node_id, %error, "assignment poll failed");
                            command_poll_not_before =
                                TokioInstant::now() + COMMAND_POLL_RETRY_DELAY;
                            continue;
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
                completed = async {
                    let active = active_execution
                        .as_mut()
                        .expect("branch is disabled without an active execution");
                    (&mut active.task).await
                }, if active_execution.is_some() => {
                    if let Err(error) = completed {
                        tracing::error!(%node_id, %error, "execution task failed");
                    }
                    active_execution = None;
                }
                shutdown = tokio::signal::ctrl_c() => {
                    shutdown?;
                    abort_execution(&mut active_execution).await;
                    return Ok(());
                }
            }
        }
    }
}

async fn run_assignment(
    client: &ControllerClient,
    executor: &NativeExecutor,
    assignment: ExecutionAssignment,
    mut cancellation: oneshot::Receiver<()>,
) {
    let node_id = assignment.node_id;
    let execution_id = assignment.execution_id;
    let timeout = assignment
        .spec
        .execution_timeout_secs
        .map(Duration::from_secs);
    if let Err(error) =
        report_event_with_retry(client, node_id, execution_id, ExecutionEvent::Accepted).await
    {
        tracing::error!(%node_id, %execution_id, %error, "failed to accept execution assignment");
        return;
    }
    if cancellation.try_recv().is_ok() {
        if let Err(error) = report_event_with_retry(
            client,
            node_id,
            execution_id,
            ExecutionEvent::Cancelled {
                output: ExecutionOutput::default(),
            },
        )
        .await
        {
            tracing::error!(%node_id, %execution_id, %error, "failed to report cancellation before process start");
        }
        return;
    }

    let execution = match executor.start(&assignment) {
        Ok(execution) => execution,
        Err(error) => {
            tracing::warn!(%node_id, %execution_id, %error, "execution process failed to start");
            if let Err(report_error) = report_event_with_retry(
                client,
                node_id,
                execution_id,
                ExecutionEvent::StartFailed {
                    reason: error.to_string(),
                },
            )
            .await
            {
                tracing::error!(%node_id, %execution_id, %report_error, "failed to report rejected execution");
            }
            return;
        }
    };

    if let Err(error) =
        report_event_with_retry(client, node_id, execution_id, ExecutionEvent::Running).await
    {
        tracing::error!(%node_id, %execution_id, %error, "failed to report running execution");
    }

    let outcome = match execution.wait_controlled(cancellation, timeout).await {
        Ok(outcome) => outcome,
        Err(error) => {
            tracing::error!(%node_id, %execution_id, %error, "failed to wait for execution process");
            ControlledExecutionOutcome::Finished(CompletedExecution {
                result: ExecutionResult { exit_code: None },
                output: ExecutionOutput::default(),
            })
        }
    };
    let (event, result) = match outcome {
        ControlledExecutionOutcome::Finished(completed) => (
            ExecutionEvent::Finished {
                result: completed.result,
                output: completed.output,
            },
            Some(completed.result),
        ),
        ControlledExecutionOutcome::Cancelled { output } => {
            (ExecutionEvent::Cancelled { output }, None)
        }
        ControlledExecutionOutcome::TimedOut { output } => {
            (ExecutionEvent::TimedOut { output }, None)
        }
    };
    if let Err(error) = report_event_with_retry(client, node_id, execution_id, event).await {
        tracing::error!(%node_id, %execution_id, %error, "failed to report execution result");
        return;
    }

    tracing::info!(
        %node_id,
        %execution_id,
        exit_code = ?result.and_then(|result| result.exit_code),
        "execution finished"
    );
}

async fn report_event_with_retry(
    client: &ControllerClient,
    node_id: meld_core::NodeId,
    execution_id: meld_core::ExecutionId,
    event: ExecutionEvent,
) -> Result<(), ControllerClientError> {
    let mut backoff = ReconnectBackoff::new(INITIAL_RECONNECT_DELAY, MAX_RECONNECT_DELAY);
    loop {
        match client
            .report_execution_event(node_id, execution_id, event.clone())
            .await
        {
            Ok(()) => return Ok(()),
            Err(error) if error.is_retryable() => {
                let delay = backoff.next_delay();
                tracing::warn!(
                    %node_id,
                    %execution_id,
                    %error,
                    retry_in_secs = delay.as_secs(),
                    "execution event report failed"
                );
                sleep(delay).await;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn abort_execution(active_execution: &mut Option<ActiveExecution>) {
    if let Some(execution) = active_execution.take() {
        execution.task.abort();
        let _ = execution.task.await;
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
