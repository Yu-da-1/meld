//! Directories that hold what only the owner should read.

use std::{fs::DirBuilder, io, path::Path};

/// Creates `path` and any missing parents, readable by the owner only.
///
/// On Unix a directory that already exists keeps its permissions: changing
/// something an operator set up on purpose would be a surprise. Elsewhere the
/// platform's own defaults for a user's data directory apply.
pub fn create_private_directory(path: &Path) -> io::Result<()> {
    let mut builder = DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    fn temporary_directory() -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("meld-private-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&path).expect("temporary directory");
        path
    }

    #[cfg(unix)]
    #[test]
    fn new_directories_are_private_and_existing_ones_are_left_alone() {
        use std::os::unix::fs::PermissionsExt;

        let root = temporary_directory();
        let existing = root.join("existing");
        fs::create_dir(&existing).expect("existing directory");
        fs::set_permissions(&existing, fs::Permissions::from_mode(0o755)).expect("mode");
        let mode = |path: &Path| fs::metadata(path).expect("metadata").permissions().mode() & 0o777;

        create_private_directory(&existing).expect("existing directory is fine");
        let nested = root.join("a/b/c");
        create_private_directory(&nested).expect("nested directories are created");

        assert_eq!(mode(&existing), 0o755);
        for path in [root.join("a"), root.join("a/b"), nested] {
            assert_eq!(mode(&path), 0o700, "{}", path.display());
        }
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn creating_the_same_directory_twice_is_not_an_error() {
        let root = temporary_directory();
        let path = root.join("state");

        create_private_directory(&path).expect("first");
        create_private_directory(&path).expect("second");

        assert!(path.is_dir());
        fs::remove_dir_all(root).expect("cleanup");
    }
}
