//! Persistent identity for one Meld node installation.

use std::{
    env,
    fs::{self, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

use directories::BaseDirs;
use meld_core::NodeId;

const STATE_DIR_ENV: &str = "MELD_STATE_DIR";
const NODE_ID_FILE: &str = "node-id";
const IDENTITY_LOCK_FILE: &str = ".identity.lock";

/// Loads the existing node identity or creates it on first startup.
pub fn load_or_create_node_id() -> io::Result<NodeId> {
    load_or_create_at(&state_directory()?)
}

pub(crate) fn state_directory() -> io::Result<PathBuf> {
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
        .map(|directories| directories.data_local_dir().join("meld"))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "the operating system did not provide a local data directory",
            )
        })
}

fn load_or_create_at(state_directory: &Path) -> io::Result<NodeId> {
    fs::create_dir_all(state_directory)
        .map_err(|error| with_path("create state directory", state_directory, error))?;

    let lock_path = state_directory.join(IDENTITY_LOCK_FILE);
    let lock_file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(|error| with_path("open identity lock", &lock_path, error))?;
    lock_file
        .lock()
        .map_err(|error| with_path("lock node identity", &lock_path, error))?;

    let node_id_path = state_directory.join(NODE_ID_FILE);
    match read_node_id(&node_id_path) {
        Ok(node_id) => Ok(node_id),
        Err(error) if error.kind() == io::ErrorKind::NotFound => create_node_id(&node_id_path),
        Err(error) => Err(error),
    }
}

fn read_node_id(path: &Path) -> io::Result<NodeId> {
    let contents =
        fs::read_to_string(path).map_err(|error| with_path("read node identity", path, error))?;

    contents.trim().parse::<NodeId>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("node identity at {} is invalid: {error}", path.display()),
        )
    })
}

fn create_node_id(path: &Path) -> io::Result<NodeId> {
    let node_id = NodeId::generate();
    let temporary_path = path.with_extension("tmp");
    let mut temporary_file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary_path)
        .map_err(|error| with_path("create temporary node identity", &temporary_path, error))?;

    writeln!(temporary_file, "{node_id}")
        .map_err(|error| with_path("write temporary node identity", &temporary_path, error))?;
    temporary_file
        .sync_all()
        .map_err(|error| with_path("sync temporary node identity", &temporary_path, error))?;
    drop(temporary_file);

    fs::rename(&temporary_path, path)
        .map_err(|error| with_path("install node identity", path, error))?;
    Ok(node_id)
}

fn with_path(operation: &str, path: &Path, error: io::Error) -> io::Error {
    io::Error::new(
        error.kind(),
        format!("{operation} at {}: {error}", path.display()),
    )
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc, thread};

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn identity_is_created_once_and_reused() {
        let state_directory = tempdir().expect("temporary directory should be created");

        let initial =
            load_or_create_at(state_directory.path()).expect("identity should be created");
        let reloaded =
            load_or_create_at(state_directory.path()).expect("identity should be reloaded");

        assert_eq!(reloaded, initial);
        assert_eq!(
            fs::read_to_string(state_directory.path().join(NODE_ID_FILE))
                .expect("identity file should be readable")
                .trim(),
            initial.to_string()
        );
    }

    #[test]
    fn invalid_identity_is_reported_without_replacement() {
        let state_directory = tempdir().expect("temporary directory should be created");
        let identity_path = state_directory.path().join(NODE_ID_FILE);
        fs::write(&identity_path, "not-a-node-id")
            .expect("invalid identity fixture should be written");

        let error = load_or_create_at(state_directory.path())
            .expect_err("invalid identity must not be silently replaced");

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            fs::read_to_string(identity_path).expect("fixture should remain readable"),
            "not-a-node-id"
        );
    }

    #[test]
    fn concurrent_first_startup_uses_one_identity() {
        let state_directory = Arc::new(tempdir().expect("temporary directory should be created"));
        let workers = (0..8)
            .map(|_| {
                let state_directory = Arc::clone(&state_directory);
                thread::spawn(move || {
                    load_or_create_at(state_directory.path())
                        .expect("concurrent identity access should succeed")
                })
            })
            .collect::<Vec<_>>();

        let identities = workers
            .into_iter()
            .map(|worker| worker.join().expect("identity worker should not panic"))
            .collect::<HashSet<_>>();

        assert_eq!(identities.len(), 1);
    }
}
