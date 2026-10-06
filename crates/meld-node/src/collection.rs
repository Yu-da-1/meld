//! Collects a job's declared output files from its workspace.
//!
//! Only declared paths are read, each is hashed and uploaded to the
//! controller by digest, and the result is reported as a manifest. Nothing in
//! the workspace is followed outside of it: the process that ran there is not
//! trusted to leave honest files behind.

use std::{
    future::Future,
    io,
    path::{Path, PathBuf},
    time::Duration,
};

use meld_core::{
    DataFailure, DataSpec, MAX_FILE_BYTES, OutputFile, Sha256Digest, validate_relative_path,
};
use sha2::{Digest, Sha256};
use tokio::{fs::File, io::AsyncReadExt, time::sleep};

const UPLOAD_ATTEMPTS: u32 = 3;
const RETRY_DELAY: Duration = Duration::from_millis(500);
const READ_BUFFER_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadError {
    /// The transfer failed in a way that may succeed on another attempt.
    Unavailable(String),
    /// The controller refused the content; retrying cannot help.
    Rejected(String),
}

/// Where output content goes.
pub trait BlobSink: Sync {
    /// Stores the content of `file`, which hashes to `digest`, on the controller.
    fn upload(
        &self,
        digest: &Sha256Digest,
        file: &Path,
        size_bytes: u64,
    ) -> impl Future<Output = Result<(), UploadError>> + Send;
}

/// Uploads every declared output and returns their manifest.
pub async fn collect_outputs<S: BlobSink>(
    sink: &S,
    data: &DataSpec,
    workspace: &Path,
) -> Result<Vec<OutputFile>, DataFailure> {
    collect_outputs_with_retry_delay(sink, data, workspace, RETRY_DELAY).await
}

async fn collect_outputs_with_retry_delay<S: BlobSink>(
    sink: &S,
    data: &DataSpec,
    workspace: &Path,
    retry_delay: Duration,
) -> Result<Vec<OutputFile>, DataFailure> {
    let mut collected = Vec::with_capacity(data.outputs.len());
    for output in &data.outputs {
        let path = &output.path;
        if validate_relative_path(path).is_err() {
            return Err(DataFailure::InvalidPath { path: path.clone() });
        }
        let file = locate(workspace, path).await?;
        let (sha256, size_bytes) = digest_file(&file, path).await?;
        upload(sink, &sha256, &file, size_bytes, path, retry_delay).await?;
        collected.push(OutputFile {
            path: path.clone(),
            sha256,
            size_bytes,
        });
    }
    Ok(collected)
}

/// Resolves a declared path to a regular file, refusing any symbolic link
/// along the way so the job cannot point an output at files outside its
/// workspace.
async fn locate(workspace: &Path, relative: &str) -> Result<PathBuf, DataFailure> {
    let missing = || DataFailure::OutputMissing {
        path: relative.to_owned(),
    };
    let mut path = workspace.to_owned();
    let mut components = relative.split('/').peekable();
    while let Some(component) = components.next() {
        path.push(component);
        let metadata = match tokio::fs::symlink_metadata(&path).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Err(missing()),
            Err(error) => {
                tracing::warn!(path = %path.display(), %error, "failed to inspect output");
                return Err(DataFailure::OutputUploadFailed {
                    path: relative.to_owned(),
                });
            }
        };
        if metadata.file_type().is_symlink() {
            return Err(DataFailure::InvalidPath {
                path: relative.to_owned(),
            });
        }
        let is_last = components.peek().is_none();
        let acceptable = if is_last {
            metadata.is_file()
        } else {
            metadata.is_dir()
        };
        if !acceptable {
            return Err(missing());
        }
    }
    Ok(path)
}

async fn digest_file(file: &Path, declared: &str) -> Result<(Sha256Digest, u64), DataFailure> {
    let failed = |error: io::Error| {
        tracing::warn!(path = %file.display(), %error, "failed to read output");
        DataFailure::OutputUploadFailed {
            path: declared.to_owned(),
        }
    };
    let too_large = || DataFailure::TooLarge {
        path: declared.to_owned(),
        limit_bytes: MAX_FILE_BYTES,
    };

    let mut reader = File::open(file).await.map_err(failed)?;
    let mut hasher = Sha256::new();
    let mut size_bytes: u64 = 0;
    let mut buffer = vec![0; READ_BUFFER_BYTES];
    loop {
        let read = reader.read(&mut buffer).await.map_err(failed)?;
        if read == 0 {
            break;
        }
        size_bytes = size_bytes.saturating_add(read as u64);
        // Checked as it is read: a file can grow while it is being hashed.
        if size_bytes > MAX_FILE_BYTES {
            return Err(too_large());
        }
        hasher.update(&buffer[..read]);
    }
    Ok((
        Sha256Digest::from_bytes(hasher.finalize().into()),
        size_bytes,
    ))
}

async fn upload<S: BlobSink>(
    sink: &S,
    digest: &Sha256Digest,
    file: &Path,
    size_bytes: u64,
    declared: &str,
    retry_delay: Duration,
) -> Result<(), DataFailure> {
    let failed = || DataFailure::OutputUploadFailed {
        path: declared.to_owned(),
    };
    for attempt in 1..=UPLOAD_ATTEMPTS {
        match sink.upload(digest, file, size_bytes).await {
            Ok(()) => return Ok(()),
            Err(UploadError::Rejected(message)) => {
                tracing::warn!(path = declared, %message, "controller refused an output");
                return Err(failed());
            }
            Err(UploadError::Unavailable(message)) => {
                tracing::warn!(path = declared, attempt, %message, "output upload failed");
                if attempt == UPLOAD_ATTEMPTS {
                    return Err(failed());
                }
                sleep(retry_delay).await;
            }
        }
    }
    Err(failed())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        fs,
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use meld_core::{InputFile, OutputSpec};
    use tempfile::TempDir;

    use super::*;

    fn digest_of(data: &[u8]) -> Sha256Digest {
        Sha256Digest::from_bytes(Sha256::digest(data).into())
    }

    /// Keeps what it is given and can be told to fail.
    #[derive(Default)]
    struct FakeSink {
        stored: Mutex<BTreeMap<Sha256Digest, Vec<u8>>>,
        attempts: AtomicUsize,
        transient_failures: AtomicUsize,
        reject: bool,
    }

    impl FakeSink {
        fn stored(&self) -> BTreeMap<Sha256Digest, Vec<u8>> {
            self.stored.lock().expect("lock").clone()
        }

        fn attempts(&self) -> usize {
            self.attempts.load(Ordering::SeqCst)
        }
    }

    impl BlobSink for FakeSink {
        async fn upload(
            &self,
            digest: &Sha256Digest,
            file: &Path,
            size_bytes: u64,
        ) -> Result<(), UploadError> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            if self.reject {
                return Err(UploadError::Rejected("storage is full".to_owned()));
            }
            if self
                .transient_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    left.checked_sub(1)
                })
                .is_ok()
            {
                return Err(UploadError::Unavailable("connection reset".to_owned()));
            }
            let content = fs::read(file).expect("uploaded file should be readable");
            assert_eq!(content.len() as u64, size_bytes);
            assert_eq!(&digest_of(&content), digest);
            self.stored
                .lock()
                .expect("lock")
                .insert(digest.clone(), content);
            Ok(())
        }
    }

    fn data(outputs: &[&str]) -> DataSpec {
        DataSpec {
            inputs: Vec::<InputFile>::new(),
            outputs: outputs
                .iter()
                .map(|path| OutputSpec {
                    path: (*path).to_owned(),
                })
                .collect(),
        }
    }

    async fn collect(
        sink: &FakeSink,
        workspace: &TempDir,
        outputs: &[&str],
    ) -> Result<Vec<OutputFile>, DataFailure> {
        collect_outputs_with_retry_delay(sink, &data(outputs), workspace.path(), Duration::ZERO)
            .await
    }

    fn write(workspace: &TempDir, path: &str, content: &[u8]) {
        let path = workspace.path().join(path);
        fs::create_dir_all(path.parent().expect("has parent")).expect("create directories");
        fs::write(path, content).expect("write file");
    }

    #[tokio::test]
    async fn declared_outputs_are_hashed_uploaded_and_listed() {
        let workspace = TempDir::new().expect("workspace");
        write(&workspace, "result.json", b"{}");
        write(&workspace, "report/summary.txt", b"all good");
        let sink = FakeSink::default();

        let collected = collect(&sink, &workspace, &["result.json", "report/summary.txt"])
            .await
            .expect("collection should succeed");

        assert_eq!(
            collected,
            vec![
                OutputFile {
                    path: "result.json".to_owned(),
                    sha256: digest_of(b"{}"),
                    size_bytes: 2,
                },
                OutputFile {
                    path: "report/summary.txt".to_owned(),
                    sha256: digest_of(b"all good"),
                    size_bytes: 8,
                },
            ]
        );
        assert_eq!(sink.stored().len(), 2);
    }

    #[tokio::test]
    async fn only_declared_files_leave_the_workspace() {
        let workspace = TempDir::new().expect("workspace");
        write(&workspace, "result.json", b"{}");
        write(&workspace, "scratch.tmp", b"not declared");
        let sink = FakeSink::default();

        collect(&sink, &workspace, &["result.json"])
            .await
            .expect("collection should succeed");

        assert_eq!(
            sink.stored().into_keys().collect::<Vec<_>>(),
            vec![digest_of(b"{}")]
        );
    }

    #[tokio::test]
    async fn empty_output_is_collected() {
        let workspace = TempDir::new().expect("workspace");
        write(&workspace, "empty.txt", b"");
        let sink = FakeSink::default();

        let collected = collect(&sink, &workspace, &["empty.txt"])
            .await
            .expect("an empty file is a valid output");

        assert_eq!(collected[0].size_bytes, 0);
    }

    #[tokio::test]
    async fn missing_output_is_reported_by_its_declared_path() {
        let workspace = TempDir::new().expect("workspace");
        let sink = FakeSink::default();

        let failure = collect(&sink, &workspace, &["missing/result.json"])
            .await
            .expect_err("a missing output must fail");

        assert_eq!(
            failure,
            DataFailure::OutputMissing {
                path: "missing/result.json".to_owned()
            }
        );
        assert_eq!(sink.attempts(), 0);
    }

    #[tokio::test]
    async fn directory_is_not_an_output_file() {
        let workspace = TempDir::new().expect("workspace");
        fs::create_dir(workspace.path().join("result")).expect("directory");
        let sink = FakeSink::default();

        let failure = collect(&sink, &workspace, &["result"])
            .await
            .expect_err("a directory is not a file");

        assert!(matches!(failure, DataFailure::OutputMissing { .. }));
    }

    #[tokio::test]
    async fn unsafe_declared_path_is_refused_before_touching_the_disk() {
        let workspace = TempDir::new().expect("workspace");
        let sink = FakeSink::default();

        for path in ["../outside", "/etc/passwd", "a/../../b"] {
            let failure = collect(&sink, &workspace, &[path])
                .await
                .expect_err("unsafe path must be refused");
            assert_eq!(
                failure,
                DataFailure::InvalidPath {
                    path: path.to_owned()
                }
            );
        }
        assert_eq!(sink.attempts(), 0);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlinked_output_is_refused_and_its_target_is_never_read() {
        let workspace = TempDir::new().expect("workspace");
        let outside = TempDir::new().expect("outside");
        fs::write(outside.path().join("secret"), b"private").expect("secret");
        std::os::unix::fs::symlink(
            outside.path().join("secret"),
            workspace.path().join("result.txt"),
        )
        .expect("symlink");
        let sink = FakeSink::default();

        let failure = collect(&sink, &workspace, &["result.txt"])
            .await
            .expect_err("symlinks must be refused");

        assert_eq!(
            failure,
            DataFailure::InvalidPath {
                path: "result.txt".to_owned()
            }
        );
        assert!(sink.stored().is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn output_below_a_symlinked_directory_is_refused() {
        let workspace = TempDir::new().expect("workspace");
        let outside = TempDir::new().expect("outside");
        fs::write(outside.path().join("passwd"), b"private").expect("secret");
        std::os::unix::fs::symlink(outside.path(), workspace.path().join("out")).expect("symlink");
        let sink = FakeSink::default();

        let failure = collect(&sink, &workspace, &["out/passwd"])
            .await
            .expect_err("a symlinked directory must not be followed");

        assert!(matches!(failure, DataFailure::InvalidPath { .. }));
        assert!(sink.stored().is_empty());
    }

    #[tokio::test]
    async fn transient_upload_failures_are_retried() {
        let workspace = TempDir::new().expect("workspace");
        write(&workspace, "result.json", b"{}");
        let sink = FakeSink::default();
        sink.transient_failures.store(2, Ordering::SeqCst);

        collect(&sink, &workspace, &["result.json"])
            .await
            .expect("third attempt should succeed");

        assert_eq!(sink.attempts(), 3);
    }

    #[tokio::test]
    async fn persistent_upload_failures_give_up_after_three_attempts() {
        let workspace = TempDir::new().expect("workspace");
        write(&workspace, "result.json", b"{}");
        let sink = FakeSink::default();
        sink.transient_failures.store(10, Ordering::SeqCst);

        let failure = collect(&sink, &workspace, &["result.json"])
            .await
            .expect_err("repeated failure must be reported");

        assert_eq!(
            failure,
            DataFailure::OutputUploadFailed {
                path: "result.json".to_owned()
            }
        );
        assert_eq!(sink.attempts(), 3);
    }

    #[tokio::test]
    async fn refused_upload_is_not_retried() {
        let workspace = TempDir::new().expect("workspace");
        write(&workspace, "result.json", b"{}");
        let sink = FakeSink {
            reject: true,
            ..FakeSink::default()
        };

        let failure = collect(&sink, &workspace, &["result.json"])
            .await
            .expect_err("a refusal must fail the output");

        assert!(matches!(failure, DataFailure::OutputUploadFailed { .. }));
        assert_eq!(sink.attempts(), 1);
    }
}
