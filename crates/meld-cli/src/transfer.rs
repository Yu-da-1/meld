//! Turns local files into a job's input manifest and uploads their content.
//!
//! A job names every file it needs. This module walks what the user pointed
//! at, hashes each file, and sends to the controller only the content it does
//! not already hold.

use std::{
    collections::BTreeMap,
    error::Error,
    fmt, fs, io,
    path::{Path, PathBuf},
    str::FromStr,
};

use meld_core::{
    InputFile, JobSpec, JobSpecValidationError, MAX_FILE_BYTES, MAX_FILES_PER_JOB,
    MAX_JOB_INPUT_BYTES, Sha256Digest, SubmitJobResponse,
};
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

use crate::controller_client::{ControllerClient, ControllerClientError};

const READ_BUFFER_BYTES: usize = 64 * 1024;

/// One `--input SRC[=DEST]` argument.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputArg {
    pub source: PathBuf,
    /// Where the file, or the directory's contents, go in the job's workspace.
    pub destination: Option<String>,
}

impl FromStr for InputArg {
    type Err = String;

    /// Splits at the last `=`, so only the destination may not contain one.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (source, destination) = match value.rsplit_once('=') {
            Some((source, destination)) => (source, Some(destination.to_owned())),
            None => (value, None),
        };
        if source.is_empty() {
            return Err("input source must not be empty".to_owned());
        }
        Ok(Self {
            source: PathBuf::from(source),
            destination,
        })
    }
}

/// A local file that will be part of the job's inputs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedFile {
    pub source: PathBuf,
    /// `/`-separated path in the job's workspace.
    pub destination: String,
    pub size_bytes: u64,
    pub executable: bool,
}

/// A planned file together with the digest of its content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedInput {
    pub source: PathBuf,
    pub file: InputFile,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct UploadSummary {
    pub uploaded_files: usize,
    pub uploaded_bytes: u64,
    /// Files whose content the controller already held.
    pub reused_files: usize,
}

impl UploadSummary {
    fn add(&mut self, other: Self) {
        self.uploaded_files += other.uploaded_files;
        self.uploaded_bytes += other.uploaded_bytes;
        self.reused_files += other.reused_files;
    }
}

#[derive(Debug)]
pub enum TransferError {
    Io {
        path: PathBuf,
        source: io::Error,
    },
    /// Links inside a directory are refused rather than silently skipped or followed.
    Symlink(PathBuf),
    NotARegularFile(PathBuf),
    NonUnicodeName(PathBuf),
    NeedsDestination(PathBuf),
    TooManyFiles,
    FileTooLarge(PathBuf),
    InputsTooLarge,
    ChangedWhileReading(PathBuf),
    InvalidSpec(JobSpecValidationError),
    Controller(ControllerClientError),
}

impl fmt::Display for TransferError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, source } => write!(formatter, "{}: {source}", path.display()),
            Self::Symlink(path) => write!(
                formatter,
                "{} is a symbolic link; pass the file it points to instead",
                path.display()
            ),
            Self::NotARegularFile(path) => {
                write!(
                    formatter,
                    "{} is not a regular file or directory",
                    path.display()
                )
            }
            Self::NonUnicodeName(path) => {
                write!(
                    formatter,
                    "{} has a name that is not valid Unicode",
                    path.display()
                )
            }
            Self::NeedsDestination(path) => write!(
                formatter,
                "{} has no name of its own; give a destination, for example `{}=project`",
                path.display(),
                path.display()
            ),
            Self::TooManyFiles => {
                write!(
                    formatter,
                    "a job may have at most {MAX_FILES_PER_JOB} input files"
                )
            }
            Self::FileTooLarge(path) => write!(
                formatter,
                "{} exceeds the {MAX_FILE_BYTES} byte file limit",
                path.display()
            ),
            Self::InputsTooLarge => write!(
                formatter,
                "input files exceed the combined {MAX_JOB_INPUT_BYTES} byte limit"
            ),
            Self::ChangedWhileReading(path) => {
                write!(
                    formatter,
                    "{} changed while it was being read",
                    path.display()
                )
            }
            Self::InvalidSpec(error) => error.fmt(formatter),
            Self::Controller(error) => error.fmt(formatter),
        }
    }
}

impl Error for TransferError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Controller(error) => Some(error),
            Self::InvalidSpec(error) => Some(error),
            _ => None,
        }
    }
}

impl From<ControllerClientError> for TransferError {
    fn from(error: ControllerClientError) -> Self {
        Self::Controller(error)
    }
}

/// Lists every file the arguments select, with its place in the workspace.
///
/// Directories are expanded recursively into individual files, so the
/// manifest always names each file explicitly.
pub fn plan_inputs(args: &[InputArg]) -> Result<Vec<PlannedFile>, TransferError> {
    let mut planned = Vec::new();
    for arg in args {
        let io_error = |source| TransferError::Io {
            path: arg.source.clone(),
            source,
        };
        // The user named this path, so a link here is followed.
        let metadata = fs::metadata(&arg.source).map_err(io_error)?;
        if metadata.is_file() {
            let destination = match &arg.destination {
                Some(destination) => destination.clone(),
                None => unicode_name(&arg.source)?,
            };
            push_file(&mut planned, arg.source.clone(), destination, &metadata)?;
        } else if metadata.is_dir() {
            let prefix = match arg.destination.as_deref() {
                Some("" | ".") => None,
                Some(destination) => Some(destination.trim_end_matches('/').to_owned()),
                None => Some(directory_name(&arg.source)?),
            };
            walk(&arg.source, prefix.as_deref(), &mut planned)?;
        } else {
            return Err(TransferError::NotARegularFile(arg.source.clone()));
        }
    }
    Ok(planned)
}

fn walk(
    directory: &Path,
    prefix: Option<&str>,
    planned: &mut Vec<PlannedFile>,
) -> Result<(), TransferError> {
    let io_error = |source| TransferError::Io {
        path: directory.to_owned(),
        source,
    };
    let mut entries = fs::read_dir(directory)
        .map_err(io_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(io_error)?;
    // A stable order keeps the manifest, and so any error, reproducible.
    entries.sort_by_key(fs::DirEntry::file_name);

    for entry in entries {
        let path = entry.path();
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| TransferError::NonUnicodeName(path.clone()))?;
        let destination = match prefix {
            Some(prefix) => format!("{prefix}/{name}"),
            None => name,
        };
        let metadata = fs::symlink_metadata(&path).map_err(|source| TransferError::Io {
            path: path.clone(),
            source,
        })?;
        if metadata.file_type().is_symlink() {
            return Err(TransferError::Symlink(path));
        } else if metadata.is_dir() {
            walk(&path, Some(&destination), planned)?;
        } else if metadata.is_file() {
            push_file(planned, path, destination, &metadata)?;
        } else {
            return Err(TransferError::NotARegularFile(path));
        }
    }
    Ok(())
}

fn push_file(
    planned: &mut Vec<PlannedFile>,
    source: PathBuf,
    destination: String,
    metadata: &fs::Metadata,
) -> Result<(), TransferError> {
    if metadata.len() > MAX_FILE_BYTES {
        return Err(TransferError::FileTooLarge(source));
    }
    planned.push(PlannedFile {
        source,
        destination,
        size_bytes: metadata.len(),
        executable: is_executable(metadata),
    });
    // Checked while walking, so a huge directory fails before any hashing.
    if planned.len() > MAX_FILES_PER_JOB {
        return Err(TransferError::TooManyFiles);
    }
    let total = planned
        .iter()
        .fold(0u64, |total, file| total.saturating_add(file.size_bytes));
    if total > MAX_JOB_INPUT_BYTES {
        return Err(TransferError::InputsTooLarge);
    }
    Ok(())
}

#[cfg(unix)]
fn is_executable(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;

    metadata.permissions().mode() & 0o111 != 0
}

/// Windows decides what is executable by file name, so there is no bit to carry.
#[cfg(not(unix))]
fn is_executable(_metadata: &fs::Metadata) -> bool {
    false
}

fn unicode_name(path: &Path) -> Result<String, TransferError> {
    path.file_name()
        .ok_or_else(|| TransferError::NeedsDestination(path.to_owned()))?
        .to_str()
        .map(str::to_owned)
        .ok_or_else(|| TransferError::NonUnicodeName(path.to_owned()))
}

/// The name of a directory, resolving `.` and `..` so `--input .` still works.
fn directory_name(path: &Path) -> Result<String, TransferError> {
    if path.file_name().is_some() {
        return unicode_name(path);
    }
    match path.canonicalize() {
        Ok(resolved) if resolved.file_name().is_some() => unicode_name(&resolved),
        _ => Err(TransferError::NeedsDestination(path.to_owned())),
    }
}

/// Hashes every planned file.
pub async fn hash_inputs(planned: Vec<PlannedFile>) -> Result<Vec<StagedInput>, TransferError> {
    let mut staged = Vec::with_capacity(planned.len());
    for file in planned {
        let (sha256, size_bytes) = hash_file(&file.source).await?;
        if size_bytes != file.size_bytes {
            return Err(TransferError::ChangedWhileReading(file.source));
        }
        staged.push(StagedInput {
            source: file.source,
            file: InputFile {
                path: file.destination,
                sha256,
                size_bytes,
                executable: file.executable,
            },
        });
    }
    Ok(staged)
}

async fn hash_file(path: &Path) -> Result<(Sha256Digest, u64), TransferError> {
    let io_error = |source| TransferError::Io {
        path: path.to_owned(),
        source,
    };
    let mut file = tokio::fs::File::open(path).await.map_err(io_error)?;
    let mut hasher = Sha256::new();
    let mut size_bytes: u64 = 0;
    let mut buffer = vec![0; READ_BUFFER_BYTES];
    loop {
        let read = file.read(&mut buffer).await.map_err(io_error)?;
        if read == 0 {
            break;
        }
        size_bytes = size_bytes.saturating_add(read as u64);
        hasher.update(&buffer[..read]);
    }
    Ok((
        Sha256Digest::from_bytes(hasher.finalize().into()),
        size_bytes,
    ))
}

/// Sends to the controller each distinct content it does not already hold.
pub async fn upload_inputs(
    client: &ControllerClient,
    staged: &[StagedInput],
    on_upload: &dyn Fn(&StagedInput),
) -> Result<UploadSummary, TransferError> {
    // Identical content under several names is sent once.
    let mut distinct: BTreeMap<&Sha256Digest, &StagedInput> = BTreeMap::new();
    for input in staged {
        distinct.entry(&input.file.sha256).or_insert(input);
    }

    let mut summary = UploadSummary::default();
    for (digest, input) in distinct {
        if client.has_blob(digest).await? {
            summary.reused_files += 1;
            continue;
        }
        on_upload(input);
        client
            .upload_blob(digest, &input.source, input.file.size_bytes)
            .await
            .map_err(|error| match error {
                // The controller refuses content that does not match its name.
                ControllerClientError::Rejected { status, .. }
                    if status == reqwest::StatusCode::BAD_REQUEST =>
                {
                    TransferError::ChangedWhileReading(input.source.clone())
                }
                other => TransferError::Controller(other),
            })?;
        summary.uploaded_files += 1;
        summary.uploaded_bytes += input.file.size_bytes;
    }
    Ok(summary)
}

/// Uploads a job's input files and submits it.
///
/// If the controller reports that content uploaded a moment ago is gone, for
/// example evicted while the job was being submitted, it is uploaded again and
/// submission is retried once.
pub async fn submit_with_inputs(
    client: &ControllerClient,
    mut spec: JobSpec,
    staged: &[StagedInput],
    on_upload: &dyn Fn(&StagedInput),
) -> Result<(SubmitJobResponse, UploadSummary), TransferError> {
    spec.data.inputs = staged.iter().map(|input| input.file.clone()).collect();
    // Fail on a bad manifest before sending potentially gigabytes.
    spec.validate().map_err(TransferError::InvalidSpec)?;

    let mut summary = upload_inputs(client, staged, on_upload).await?;
    match client.submit(&spec).await {
        Err(ControllerClientError::MissingInputs(_)) => {
            summary.add(upload_inputs(client, staged, on_upload).await?);
            let submitted = client.submit(&spec).await?;
            Ok((submitted, summary))
        }
        other => Ok((other?, summary)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(root: &Path, path: &str, content: &[u8]) {
        let path = root.join(path);
        fs::create_dir_all(path.parent().expect("has parent")).expect("create directories");
        fs::write(path, content).expect("write file");
    }

    fn destinations(planned: &[PlannedFile]) -> Vec<&str> {
        planned
            .iter()
            .map(|file| file.destination.as_str())
            .collect()
    }

    fn arg(source: &Path, destination: Option<&str>) -> InputArg {
        InputArg {
            source: source.to_owned(),
            destination: destination.map(str::to_owned),
        }
    }

    #[test]
    fn input_argument_splits_source_and_destination() {
        assert_eq!(
            "data.csv".parse::<InputArg>(),
            Ok(InputArg {
                source: PathBuf::from("data.csv"),
                destination: None
            })
        );
        assert_eq!(
            "local/data.csv=input/data.csv".parse::<InputArg>(),
            Ok(InputArg {
                source: PathBuf::from("local/data.csv"),
                destination: Some("input/data.csv".to_owned())
            })
        );
        assert!("".parse::<InputArg>().is_err());
        assert!("=dest".parse::<InputArg>().is_err());
    }

    #[test]
    fn a_file_keeps_its_name_unless_told_otherwise() {
        let root = tempfile::tempdir().expect("temp dir");
        write(root.path(), "data.csv", b"1,2,3");

        let by_name = plan_inputs(&[arg(&root.path().join("data.csv"), None)]).expect("plan");
        let renamed =
            plan_inputs(&[arg(&root.path().join("data.csv"), Some("in/x.csv"))]).expect("plan");

        assert_eq!(destinations(&by_name), ["data.csv"]);
        assert_eq!(destinations(&renamed), ["in/x.csv"]);
        assert_eq!(by_name[0].size_bytes, 5);
    }

    #[test]
    fn a_directory_is_expanded_into_files_in_a_stable_order() {
        let root = tempfile::tempdir().expect("temp dir");
        write(root.path(), "scripts/b.py", b"b");
        write(root.path(), "scripts/a.py", b"a");
        write(root.path(), "scripts/lib/util.py", b"u");

        let planned = plan_inputs(&[arg(&root.path().join("scripts"), None)]).expect("plan");

        assert_eq!(
            destinations(&planned),
            ["scripts/a.py", "scripts/b.py", "scripts/lib/util.py"]
        );
    }

    #[test]
    fn a_directory_can_be_placed_under_another_name_or_at_the_root() {
        let root = tempfile::tempdir().expect("temp dir");
        write(root.path(), "project/main.rs", b"fn main() {}");
        let project = root.path().join("project");

        let renamed = plan_inputs(&[arg(&project, Some("src/"))]).expect("plan");
        let at_root = plan_inputs(&[arg(&project, Some("."))]).expect("plan");

        assert_eq!(destinations(&renamed), ["src/main.rs"]);
        assert_eq!(destinations(&at_root), ["main.rs"]);
    }

    #[test]
    fn dot_is_named_after_the_current_directory() {
        let current = std::env::current_dir().expect("current directory");
        let expected = current
            .file_name()
            .and_then(|name| name.to_str())
            .expect("the test runs in a named directory");

        assert_eq!(directory_name(Path::new(".")).expect("a name"), expected);
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_inside_a_directory_are_refused() {
        let root = tempfile::tempdir().expect("temp dir");
        write(root.path(), "dir/real.txt", b"x");
        std::os::unix::fs::symlink("/etc/hostname", root.path().join("dir/link")).expect("symlink");

        let error = plan_inputs(&[arg(&root.path().join("dir"), None)])
            .expect_err("a link must not be followed silently");

        assert!(matches!(error, TransferError::Symlink(_)), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn the_executable_bit_is_carried_over() {
        use std::os::unix::fs::PermissionsExt;

        let root = tempfile::tempdir().expect("temp dir");
        write(root.path(), "run.sh", b"#!/bin/sh\n");
        write(root.path(), "data.txt", b"x");
        fs::set_permissions(
            root.path().join("run.sh"),
            fs::Permissions::from_mode(0o755),
        )
        .expect("chmod");
        fs::set_permissions(
            root.path().join("data.txt"),
            fs::Permissions::from_mode(0o644),
        )
        .expect("chmod");

        let planned = plan_inputs(&[
            arg(&root.path().join("run.sh"), None),
            arg(&root.path().join("data.txt"), None),
        ])
        .expect("plan");

        assert!(planned[0].executable);
        assert!(!planned[1].executable);
    }

    #[test]
    fn missing_input_names_the_path() {
        let root = tempfile::tempdir().expect("temp dir");

        let error = plan_inputs(&[arg(&root.path().join("absent.txt"), None)])
            .expect_err("a missing file must fail");

        assert!(error.to_string().contains("absent.txt"), "{error}");
    }

    #[test]
    fn too_many_files_fail_before_anything_is_hashed() {
        let root = tempfile::tempdir().expect("temp dir");
        for index in 0..=MAX_FILES_PER_JOB {
            write(root.path(), &format!("many/f{index}"), b"");
        }

        let error =
            plan_inputs(&[arg(&root.path().join("many"), None)]).expect_err("too many files");

        assert!(matches!(error, TransferError::TooManyFiles));
    }

    #[tokio::test]
    async fn hashing_records_digest_size_and_destination() {
        let root = tempfile::tempdir().expect("temp dir");
        write(root.path(), "data.txt", b"hello");
        let planned = plan_inputs(&[arg(&root.path().join("data.txt"), None)]).expect("plan");

        let staged = hash_inputs(planned).await.expect("hash");

        assert_eq!(staged[0].file.path, "data.txt");
        assert_eq!(staged[0].file.size_bytes, 5);
        assert_eq!(
            staged[0].file.sha256.as_str(),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[tokio::test]
    async fn a_file_that_changes_after_planning_is_detected() {
        let root = tempfile::tempdir().expect("temp dir");
        write(root.path(), "data.txt", b"hello");
        let planned = plan_inputs(&[arg(&root.path().join("data.txt"), None)]).expect("plan");
        write(root.path(), "data.txt", b"hello, and then some");

        let error = hash_inputs(planned).await.expect_err("size changed");

        assert!(matches!(error, TransferError::ChangedWhileReading(_)));
    }

    mod with_controller {
        use std::sync::Arc;

        use meld_controller::{
            api::{ControllerState, router},
            blob_store::{BlobLimits, BlobStore},
        };
        use meld_core::{JobState, ResourceRequirements};
        use tempfile::TempDir;

        use super::*;

        async fn controller(with_storage: bool) -> (ControllerClient, TempDir) {
            let storage = TempDir::new().expect("storage");
            let mut state = ControllerState::new();
            if with_storage {
                let store =
                    BlobStore::new(storage.path(), BlobLimits::default()).expect("blob store");
                state = state.with_blob_store(Arc::new(store));
            }
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            let address = listener.local_addr().expect("address");
            tokio::spawn(async move { axum::serve(listener, router(state)).await });
            let client = ControllerClient::new(&format!("http://{address}")).expect("client");
            (client, storage)
        }

        fn spec() -> JobSpec {
            JobSpec {
                program: "cat".to_owned(),
                args: vec!["data.txt".to_owned()],
                requirements: ResourceRequirements {
                    logical_cpus: 1,
                    memory_bytes: 1_000_000,
                },
                job_timeout_secs: None,
                execution_timeout_secs: None,
                constraints: meld_core::PlacementConstraints::default(),
                data: meld_core::DataSpec::default(),
            }
        }

        async fn stage(root: &TempDir, files: &[(&str, &[u8], &str)]) -> Vec<StagedInput> {
            for (name, content, _) in files {
                write(root.path(), name, content);
            }
            let args: Vec<InputArg> = files
                .iter()
                .map(|(name, _, destination)| arg(&root.path().join(name), Some(destination)))
                .collect();
            hash_inputs(plan_inputs(&args).expect("plan"))
                .await
                .expect("hash")
        }

        #[tokio::test]
        async fn inputs_are_uploaded_and_listed_in_the_submitted_job() {
            let (client, _storage) = controller(true).await;
            let root = TempDir::new().expect("root");
            let staged = stage(
                &root,
                &[("a.txt", b"alpha", "data.txt"), ("b.txt", b"beta", "b.txt")],
            )
            .await;

            let (submitted, summary) = submit_with_inputs(&client, spec(), &staged, &|_| {})
                .await
                .expect("submission should succeed");

            assert_eq!(submitted.state, JobState::Queued);
            assert_eq!(summary.uploaded_files, 2);
            assert_eq!(summary.uploaded_bytes, 9);
            let status = client.status(submitted.job_id).await.expect("status");
            let recorded: Vec<&str> = status
                .spec
                .data
                .inputs
                .iter()
                .map(|input| input.path.as_str())
                .collect();
            assert_eq!(recorded, ["data.txt", "b.txt"]);
            for input in &staged {
                assert!(client.has_blob(&input.file.sha256).await.expect("lookup"));
            }
        }

        #[tokio::test]
        async fn content_the_controller_already_holds_is_not_sent_again() {
            let (client, _storage) = controller(true).await;
            let root = TempDir::new().expect("root");
            let staged = stage(&root, &[("a.txt", b"alpha", "data.txt")]).await;
            submit_with_inputs(&client, spec(), &staged, &|_| {})
                .await
                .expect("first submission");

            let (_, summary) = submit_with_inputs(&client, spec(), &staged, &|_| {
                panic!("nothing should be uploaded the second time")
            })
            .await
            .expect("second submission");

            assert_eq!(summary.uploaded_files, 0);
            assert_eq!(summary.reused_files, 1);
        }

        #[tokio::test]
        async fn identical_content_under_two_names_is_sent_once() {
            let (client, _storage) = controller(true).await;
            let root = TempDir::new().expect("root");
            let staged = stage(
                &root,
                &[
                    ("one.txt", b"same", "one.txt"),
                    ("two.txt", b"same", "copy/two.txt"),
                ],
            )
            .await;

            let (_, summary) = submit_with_inputs(&client, spec(), &staged, &|_| {})
                .await
                .expect("submission should succeed");

            assert_eq!(summary.uploaded_files, 1);
        }

        #[tokio::test]
        async fn an_invalid_manifest_is_rejected_before_anything_is_uploaded() {
            let (client, _storage) = controller(true).await;
            let root = TempDir::new().expect("root");
            let staged = stage(&root, &[("a.txt", b"alpha", "../escape.txt")]).await;

            let error = submit_with_inputs(&client, spec(), &staged, &|_| {
                panic!("nothing should be uploaded")
            })
            .await
            .expect_err("an unsafe destination must be refused");

            assert!(matches!(error, TransferError::InvalidSpec(_)), "{error}");
            assert!(
                !client
                    .has_blob(&staged[0].file.sha256)
                    .await
                    .expect("lookup")
            );
        }

        #[tokio::test]
        async fn submitting_without_uploading_names_the_missing_content() {
            let (client, _storage) = controller(true).await;
            let root = TempDir::new().expect("root");
            let staged = stage(&root, &[("a.txt", b"alpha", "data.txt")]).await;
            let mut spec = spec();
            spec.data.inputs = staged.iter().map(|input| input.file.clone()).collect();

            let error = client
                .submit(&spec)
                .await
                .expect_err("nothing was uploaded");

            assert!(matches!(
                error,
                ControllerClientError::MissingInputs(ref missing)
                    if missing == &[staged[0].file.sha256.clone()]
            ));
        }

        #[tokio::test]
        async fn a_controller_without_storage_refuses_jobs_with_inputs() {
            let (client, _storage) = controller(false).await;
            let root = TempDir::new().expect("root");
            let staged = stage(&root, &[("a.txt", b"alpha", "data.txt")]).await;

            let error = submit_with_inputs(&client, spec(), &staged, &|_| {})
                .await
                .expect_err("no storage is configured");

            assert!(error.to_string().contains("503"), "{error}");
        }
    }
}
