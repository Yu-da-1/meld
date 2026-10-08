//! Durable controller state in a local SQLite database.
//!
//! The store is deliberately dumb: it keeps `(kind, id) -> JSON body` rows and
//! applies batches atomically. What a row means belongs to the component that
//! owns the data (`JobManager`, `NodeRegistry`), so the domain types do not
//! depend on the storage layout.
//!
//! Rows are returned in the order they were first written, which is how the
//! job queue and the attempt history of a job keep their order across restarts.

use std::{
    error::Error,
    fmt, io,
    path::Path,
    sync::{Mutex, MutexGuard},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, params};

/// Bumped when the meaning of stored rows changes incompatibly.
const SCHEMA_VERSION: i64 = 1;

/// Which part of the controller state a row belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Kind {
    Job,
    Execution,
    Output,
    DataFailure,
    OutputFiles,
    OutputsRecordedAt,
    Placement,
    CancellationRequest,
    JobTimeoutRequest,
    JobSubmittedAt,
    Queue,
    AutoRetries,
    RetryNotBefore,
    Node,
}

impl Kind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Job => "job",
            Self::Execution => "execution",
            Self::Output => "output",
            Self::DataFailure => "data_failure",
            Self::OutputFiles => "output_files",
            Self::OutputsRecordedAt => "outputs_recorded_at",
            Self::Placement => "placement",
            Self::CancellationRequest => "cancellation_request",
            Self::JobTimeoutRequest => "job_timeout_request",
            Self::JobSubmittedAt => "job_submitted_at",
            Self::Queue => "queue",
            Self::AutoRetries => "auto_retries",
            Self::RetryNotBefore => "retry_not_before",
            Self::Node => "node",
        }
    }
}

/// One row to write, or to remove when `body` is `None`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub kind: Kind,
    pub id: String,
    pub body: Option<String>,
}

impl Change {
    pub fn put(kind: Kind, id: impl ToString, body: String) -> Self {
        Self {
            kind,
            id: id.to_string(),
            body: Some(body),
        }
    }

    pub fn delete(kind: Kind, id: impl ToString) -> Self {
        Self {
            kind,
            id: id.to_string(),
            body: None,
        }
    }
}

/// A row read back from the database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub id: String,
    pub body: String,
}

#[derive(Debug)]
pub enum StoreError {
    Database(rusqlite::Error),
    /// The database was written by an incompatible version.
    UnsupportedSchema {
        found: i64,
    },
    /// A stored row could not be understood.
    Corrupt(String),
    /// A previous writer panicked while holding the connection.
    Poisoned,
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(error) => write!(formatter, "state database error: {error}"),
            Self::UnsupportedSchema { found } => write!(
                formatter,
                "state database has schema version {found}, but this controller supports {SCHEMA_VERSION}"
            ),
            Self::Corrupt(detail) => write!(formatter, "state database is corrupt: {detail}"),
            Self::Poisoned => formatter.write_str("state database lock is poisoned"),
        }
    }
}

impl Error for StoreError {}

impl From<rusqlite::Error> for StoreError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Database(error)
    }
}

impl From<StoreError> for io::Error {
    fn from(error: StoreError) -> Self {
        Self::other(error)
    }
}

/// Handle to the controller's state database.
#[derive(Debug)]
pub struct StateStore {
    connection: Mutex<Connection>,
}

impl StateStore {
    /// Opens the database at `path`, creating it when absent.
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        Self::from_connection(Connection::open(path)?)
    }

    /// An in-memory database, for tests that do not need a restart.
    pub fn open_in_memory() -> Result<Self, StoreError> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(connection: Connection) -> Result<Self, StoreError> {
        // WAL keeps a commit to one sequential append; the controller writes
        // after every state change, so this is on the request path.
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;

        let found: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
        match found {
            0 => {
                connection.execute_batch(
                    "CREATE TABLE IF NOT EXISTS records (
                         kind TEXT NOT NULL,
                         id   TEXT NOT NULL,
                         body TEXT NOT NULL,
                         PRIMARY KEY (kind, id)
                     );",
                )?;
                connection.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            }
            SCHEMA_VERSION => {}
            found => return Err(StoreError::UnsupportedSchema { found }),
        }

        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    /// Applies every change, or none of them.
    pub fn apply(&self, changes: &[Change]) -> Result<(), StoreError> {
        if changes.is_empty() {
            return Ok(());
        }
        let mut connection = self.lock()?;
        let transaction = connection.transaction()?;
        {
            // The upsert keeps the row's position, so first-write order survives updates.
            let mut put = transaction.prepare_cached(
                "INSERT INTO records (kind, id, body) VALUES (?1, ?2, ?3)
                 ON CONFLICT (kind, id) DO UPDATE SET body = excluded.body",
            )?;
            let mut delete =
                transaction.prepare_cached("DELETE FROM records WHERE kind = ?1 AND id = ?2")?;
            for change in changes {
                match &change.body {
                    Some(body) => put.execute(params![change.kind.as_str(), change.id, body])?,
                    None => delete.execute(params![change.kind.as_str(), change.id])?,
                };
            }
        }
        transaction.commit()?;
        Ok(())
    }

    /// Reads every row of one kind in the order it was first written.
    pub fn load(&self, kind: Kind) -> Result<Vec<Row>, StoreError> {
        let connection = self.lock()?;
        let mut statement = connection
            .prepare_cached("SELECT id, body FROM records WHERE kind = ?1 ORDER BY rowid")?;
        let rows = statement
            .query_map(params![kind.as_str()], |row| {
                Ok(Row {
                    id: row.get(0)?,
                    body: row.get(1)?,
                })
            })?
            .collect::<Result<_, _>>()?;
        Ok(rows)
    }

    fn lock(&self) -> Result<MutexGuard<'_, Connection>, StoreError> {
        self.connection.lock().map_err(|_| StoreError::Poisoned)
    }
}

/// State that can write its unsaved changes to a [`StateStore`].
pub trait Persist {
    /// Saves everything changed since the last successful call.
    fn save(&mut self, store: &StateStore) -> Result<(), StoreError>;
}

/// Encodes a value as the JSON body of a row.
pub fn encode<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_string(value).expect("controller state serializes to JSON")
}

/// Decodes a stored body, naming the row in the error.
pub fn decode<T: serde::de::DeserializeOwned>(
    kind: Kind,
    id: &str,
    body: &str,
) -> Result<T, StoreError> {
    serde_json::from_str(body).map_err(|error| {
        StoreError::Corrupt(format!("{} {id} cannot be decoded: {error}", kind.as_str()))
    })
}

/// Wall-clock time of `instant`, in milliseconds since the epoch.
///
/// `Instant` cannot be stored, but deadlines must keep counting while the
/// controller is down, so they are persisted as wall-clock time. The instant
/// may lie in the past (a job's submission) or the future (a retry's delay).
pub fn instant_to_unix_ms(instant: Instant) -> u64 {
    let now = Instant::now();
    let now_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    if instant >= now {
        millis(now_unix + (instant - now))
    } else {
        millis(now_unix.saturating_sub(now - instant))
    }
}

/// The `Instant` for a stored wall-clock time, in the past or the future.
///
/// A time before the process could have started is clamped to now.
pub fn unix_ms_to_instant(unix_ms: u64) -> Instant {
    let now = Instant::now();
    let now_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let stored = Duration::from_millis(unix_ms);
    if stored >= now_unix {
        now + (stored - now_unix)
    } else {
        now.checked_sub(now_unix - stored).unwrap_or(now)
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_come_back_in_first_write_order_even_after_updates() {
        let store = StateStore::open_in_memory().expect("store should open");
        store
            .apply(&[
                Change::put(Kind::Job, "b", "1".to_owned()),
                Change::put(Kind::Job, "a", "2".to_owned()),
            ])
            .expect("write should succeed");
        store
            .apply(&[Change::put(Kind::Job, "b", "3".to_owned())])
            .expect("update should succeed");

        let rows = store.load(Kind::Job).expect("load should succeed");

        assert_eq!(
            rows,
            vec![
                Row {
                    id: "b".to_owned(),
                    body: "3".to_owned()
                },
                Row {
                    id: "a".to_owned(),
                    body: "2".to_owned()
                },
            ]
        );
    }

    #[test]
    fn kinds_are_separate_and_delete_removes_the_row() {
        let store = StateStore::open_in_memory().expect("store should open");
        store
            .apply(&[
                Change::put(Kind::Job, "x", "job".to_owned()),
                Change::put(Kind::Execution, "x", "execution".to_owned()),
            ])
            .expect("write should succeed");
        store
            .apply(&[Change::delete(Kind::Job, "x")])
            .expect("delete should succeed");

        assert!(store.load(Kind::Job).expect("load").is_empty());
        assert_eq!(store.load(Kind::Execution).expect("load").len(), 1);
    }

    #[test]
    fn data_survives_reopening_the_database() {
        let directory = tempfile::tempdir().expect("temp dir");
        let path = directory.path().join("state.db");
        StateStore::open(&path)
            .expect("store should open")
            .apply(&[Change::put(Kind::Node, "n", "body".to_owned())])
            .expect("write should succeed");

        let rows = StateStore::open(&path)
            .expect("store should reopen")
            .load(Kind::Node)
            .expect("load should succeed");

        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn newer_schema_is_refused_instead_of_misread() {
        let directory = tempfile::tempdir().expect("temp dir");
        let path = directory.path().join("state.db");
        Connection::open(&path)
            .expect("raw open")
            .pragma_update(None, "user_version", SCHEMA_VERSION + 1)
            .expect("set version");

        let error = StateStore::open(&path).expect_err("unknown schema must be refused");

        assert!(matches!(error, StoreError::UnsupportedSchema { .. }));
    }

    #[test]
    fn a_future_instant_is_still_in_the_future_after_a_round_trip() {
        let later = Instant::now() + Duration::from_secs(30);

        let restored = unix_ms_to_instant(instant_to_unix_ms(later));

        let drift = if restored > later {
            restored - later
        } else {
            later - restored
        };
        assert!(drift < Duration::from_millis(50), "drift was {drift:?}");
        assert!(restored > Instant::now() + Duration::from_secs(29));
    }

    #[test]
    fn wall_clock_conversion_round_trips_within_a_few_milliseconds() {
        let earlier = Instant::now() - Duration::from_secs(5);

        let restored = unix_ms_to_instant(instant_to_unix_ms(earlier));

        let drift = if restored > earlier {
            restored - earlier
        } else {
            earlier - restored
        };
        assert!(drift < Duration::from_millis(50), "drift was {drift:?}");
    }
}
