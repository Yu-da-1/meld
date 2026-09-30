use std::{
    env,
    error::Error,
    io,
    time::{Duration, Instant},
};

use meld_controller::{
    api::{ControllerState, ControllerStateError, router},
    failure_detector::FailureDetector,
};
use tokio::net::TcpListener;
use tokio::time::{MissedTickBehavior, interval};
use tracing_subscriber::EnvFilter;

const HEARTBEAT_TIMEOUT_ENV: &str = "MELD_HEARTBEAT_TIMEOUT_SECS";
const DEFAULT_HEARTBEAT_TIMEOUT_SECS: u64 = 15;
const LIVENESS_CHECK_INTERVAL: Duration = Duration::from_secs(1);

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("meld_controller=info")),
        )
        .init();

    let bind_address =
        env::var("MELD_CONTROLLER_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".to_owned());
    let heartbeat_timeout = heartbeat_timeout_from_env()?;
    let state = ControllerState::new();
    let listener = TcpListener::bind(&bind_address).await?;
    tracing::info!(
        address = %listener.local_addr()?,
        heartbeat_timeout_secs = heartbeat_timeout.as_secs(),
        "controller listening"
    );

    tokio::select! {
        result = axum::serve(listener, router(state.clone())) => result?,
        result = monitor_liveness(state, FailureDetector::new(heartbeat_timeout)) => result?,
    }
    Ok(())
}

async fn monitor_liveness(
    state: ControllerState,
    detector: FailureDetector,
) -> Result<(), ControllerStateError> {
    let mut ticker = interval(LIVENESS_CHECK_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;
        for node_id in state.detect_unreachable_nodes(&detector, Instant::now())? {
            tracing::warn!(%node_id, "node became unreachable");
        }
    }
}

fn heartbeat_timeout_from_env() -> io::Result<Duration> {
    match env::var(HEARTBEAT_TIMEOUT_ENV) {
        Ok(value) => parse_heartbeat_timeout(&value),
        Err(env::VarError::NotPresent) => Ok(Duration::from_secs(DEFAULT_HEARTBEAT_TIMEOUT_SECS)),
        Err(env::VarError::NotUnicode(_)) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{HEARTBEAT_TIMEOUT_ENV} must contain valid Unicode"),
        )),
    }
}

fn parse_heartbeat_timeout(value: &str) -> io::Result<Duration> {
    let seconds = value.parse::<u64>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{HEARTBEAT_TIMEOUT_ENV} must be a positive integer: {error}"),
        )
    })?;
    if seconds == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{HEARTBEAT_TIMEOUT_ENV} must be greater than zero"),
        ));
    }

    Ok(Duration::from_secs(seconds))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_timeout_must_be_positive() {
        let error = parse_heartbeat_timeout("0").expect_err("zero timeout must be rejected");

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }
}
