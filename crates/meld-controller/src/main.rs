use std::{
    env,
    error::Error,
    io,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use directories::BaseDirs;
use meld_controller::{
    api::{ControllerState, ControllerStateError, DEFAULT_OUTPUT_RETENTION, router},
    blob_store::{BlobLimits, BlobStore},
    failure_detector::FailureDetector,
    job_manager::RetryBackoff,
    store::StateStore,
};
use tokio::net::TcpListener;
use tokio::time::{MissedTickBehavior, interval};
use tracing_subscriber::EnvFilter;

const HEARTBEAT_TIMEOUT_ENV: &str = "MELD_HEARTBEAT_TIMEOUT_SECS";
const DEFAULT_HEARTBEAT_TIMEOUT_SECS: u64 = 15;
/// How long an unreachable node may stay silent before its executions are given up on.
const LOST_GRACE_ENV: &str = "MELD_LOST_GRACE_SECS";
const DEFAULT_LOST_GRACE_SECS: u64 = 30;
const RETRY_BACKOFF_ENV: &str = "MELD_RETRY_BACKOFF_SECS";
const RETRY_BACKOFF_MAX_ENV: &str = "MELD_RETRY_BACKOFF_MAX_SECS";
const LIVENESS_CHECK_INTERVAL: Duration = Duration::from_secs(1);
const STATE_DB_FILE: &str = "state.db";
const STATE_DIR_ENV: &str = "MELD_CONTROLLER_STATE_DIR";
const MAX_BLOB_BYTES_ENV: &str = "MELD_MAX_BLOB_BYTES";
const BLOB_QUOTA_BYTES_ENV: &str = "MELD_BLOB_QUOTA_BYTES";
const OUTPUT_RETENTION_ENV: &str = "MELD_OUTPUT_RETENTION_SECS";
const JOB_TIMEOUT_CHECK_INTERVAL: Duration = Duration::from_millis(100);

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
    let lost_grace = Duration::from_secs(positive_u64_from_env(
        LOST_GRACE_ENV,
        DEFAULT_LOST_GRACE_SECS,
    )?);
    let blob_limits = blob_limits_from_env()?;
    let state_directory = state_directory()?;
    let blobs = BlobStore::new(&state_directory, blob_limits)?;
    let output_retention = Duration::from_secs(positive_u64_from_env(
        OUTPUT_RETENTION_ENV,
        DEFAULT_OUTPUT_RETENTION.as_secs(),
    )?);
    let store = StateStore::open(&state_directory.join(STATE_DB_FILE))?;
    let retry_backoff = retry_backoff_from_env()?;
    let state = ControllerState::new()
        .with_retry_backoff(retry_backoff)
        .with_blob_store(Arc::new(blobs))
        .with_output_retention(output_retention)
        .with_store(Arc::new(store))?;
    let listener = TcpListener::bind(&bind_address).await?;
    tracing::info!(
        address = %listener.local_addr()?,
        heartbeat_timeout_secs = heartbeat_timeout.as_secs(),
        lost_grace_secs = lost_grace.as_secs(),
        retry_backoff_secs = retry_backoff.initial.as_secs(),
        retry_backoff_max_secs = retry_backoff.max.as_secs(),
        state_directory = %state_directory.display(),
        max_blob_bytes = blob_limits.max_blob_bytes,
        blob_quota_bytes = blob_limits.quota_bytes,
        output_retention_secs = output_retention.as_secs(),
        "controller listening"
    );

    tokio::select! {
        result = axum::serve(listener, router(state.clone())) => result?,
        result = monitor_liveness(
            state.clone(),
            FailureDetector::new(heartbeat_timeout),
            heartbeat_timeout + lost_grace,
        ) => result?,
        result = monitor_job_timeouts(state) => result?,
    }
    Ok(())
}

async fn monitor_job_timeouts(state: ControllerState) -> Result<(), ControllerStateError> {
    let mut ticker = interval(JOB_TIMEOUT_CHECK_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;
        let now = Instant::now();
        for job_id in state.expire_jobs(now)? {
            tracing::warn!(%job_id, "job timeout elapsed");
        }
        for job_id in state.release_due_retries(now)? {
            tracing::info!(%job_id, "automatic retry is due");
        }
    }
}

async fn monitor_liveness(
    state: ControllerState,
    detector: FailureDetector,
    lost_after: Duration,
) -> Result<(), ControllerStateError> {
    let mut ticker = interval(LIVENESS_CHECK_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;
        for node_id in state.detect_unreachable_nodes(&detector, Instant::now())? {
            tracing::warn!(%node_id, "node became unreachable");
        }
        let (lost, requeued) = state.give_up_on_silent_nodes(Instant::now(), lost_after)?;
        for execution_id in lost {
            tracing::warn!(%execution_id, "node stayed silent; execution marked lost");
        }
        for execution_id in requeued {
            tracing::warn!(%execution_id, "node stayed silent; unacknowledged execution requeued");
        }
    }
}

/// Delay before the first automatic retry, and the cap it doubles up to.
fn retry_backoff_from_env() -> io::Result<RetryBackoff> {
    let defaults = RetryBackoff::default();
    let backoff = RetryBackoff {
        initial: Duration::from_secs(positive_u64_from_env(
            RETRY_BACKOFF_ENV,
            defaults.initial.as_secs(),
        )?),
        max: Duration::from_secs(positive_u64_from_env(
            RETRY_BACKOFF_MAX_ENV,
            defaults.max.as_secs(),
        )?),
    };
    if backoff.max < backoff.initial {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{RETRY_BACKOFF_MAX_ENV} must not be smaller than {RETRY_BACKOFF_ENV}"),
        ));
    }
    Ok(backoff)
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

fn state_directory() -> io::Result<PathBuf> {
    if let Some(configured) = env::var_os(STATE_DIR_ENV) {
        if configured.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{STATE_DIR_ENV} must not be empty"),
            ));
        }
        return Ok(PathBuf::from(configured));
    }

    BaseDirs::new()
        .map(|directories| directories.data_local_dir().join("meld").join("controller"))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "the operating system did not provide a local data directory",
            )
        })
}

fn blob_limits_from_env() -> io::Result<BlobLimits> {
    let defaults = BlobLimits::default();
    let limits = BlobLimits {
        max_blob_bytes: positive_u64_from_env(MAX_BLOB_BYTES_ENV, defaults.max_blob_bytes)?,
        quota_bytes: positive_u64_from_env(BLOB_QUOTA_BYTES_ENV, defaults.quota_bytes)?,
        ..defaults
    };
    validate_blob_limits(limits)?;
    Ok(limits)
}

fn validate_blob_limits(limits: BlobLimits) -> io::Result<()> {
    if limits.quota_bytes < limits.max_blob_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{BLOB_QUOTA_BYTES_ENV} must not be smaller than {MAX_BLOB_BYTES_ENV}"),
        ));
    }
    Ok(())
}

fn positive_u64_from_env(name: &str, default: u64) -> io::Result<u64> {
    match env::var(name) {
        Ok(value) => parse_positive_u64(name, &value),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(_)) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} must contain valid Unicode"),
        )),
    }
}

fn parse_positive_u64(name: &str, value: &str) -> io::Result<u64> {
    let parsed = value.parse::<u64>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} must be a positive integer: {error}"),
        )
    })?;
    if parsed == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} must be greater than zero"),
        ));
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_limits_must_be_positive_integers() {
        for value in ["0", "-1", "abc", ""] {
            let error = parse_positive_u64(MAX_BLOB_BYTES_ENV, value)
                .expect_err("invalid limit must be rejected");
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{value:?}");
        }
        assert_eq!(
            parse_positive_u64(MAX_BLOB_BYTES_ENV, "4096").ok(),
            Some(4096)
        );
    }

    #[test]
    fn quota_must_hold_at_least_one_maximum_sized_blob() {
        let limits = BlobLimits {
            max_blob_bytes: 100,
            quota_bytes: 99,
            ..BlobLimits::default()
        };

        let error = validate_blob_limits(limits).expect_err("quota is too small");

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(validate_blob_limits(BlobLimits::default()).is_ok());
    }

    #[test]
    fn heartbeat_timeout_must_be_positive() {
        let error = parse_heartbeat_timeout("0").expect_err("zero timeout must be rejected");

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }
}
