//! Downloads the files a job left behind.
//!
//! Each file is written next to its destination as `<name>.part`, checked
//! against the size and digest the controller reported, and only then moved
//! into place, so a failed or interrupted download never leaves a file that
//! looks complete.

use std::{
    error::Error,
    fmt, fs, io,
    path::{Path, PathBuf},
};

use futures_util::StreamExt;
use meld_core::{OutputFile, Sha256Digest, validate_relative_path};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

use crate::controller_client::{ControllerClient, ControllerClientError};

#[derive(Debug, Clone)]
pub struct FetchOptions {
    pub out_dir: PathBuf,
    /// Replace files that already exist.
    pub force: bool,
}

#[derive(Debug)]
pub enum FetchError {
    /// The controller reported a path that would leave the output directory.
    UnsafePath,
    AlreadyExists(PathBuf),
    /// The controller no longer holds this content.
    NoLongerStored,
    ChecksumMismatch,
    Controller(ControllerClientError),
    Io(io::Error),
}

impl fmt::Display for FetchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsafePath => formatter.write_str("the controller reported an unsafe path"),
            Self::AlreadyExists(path) => write!(
                formatter,
                "{} already exists; use --force to replace it",
                path.display()
            ),
            Self::NoLongerStored => formatter.write_str(
                "the controller no longer stores this file; outputs are kept for a limited time",
            ),
            Self::ChecksumMismatch => {
                formatter.write_str("the downloaded content does not match its checksum")
            }
            Self::Controller(error) => error.fmt(formatter),
            Self::Io(error) => error.fmt(formatter),
        }
    }
}

impl Error for FetchError {}

/// Where a declared output ends up under `out_dir`.
///
/// The path comes from the controller, so it is checked again here rather
/// than trusted to stay inside the directory.
pub fn destination_for(out_dir: &Path, output: &OutputFile) -> Result<PathBuf, FetchError> {
    validate_relative_path(&output.path).map_err(|_| FetchError::UnsafePath)?;
    Ok(output
        .path
        .split('/')
        .fold(out_dir.to_owned(), |path, component| path.join(component)))
}

/// Downloads every output, reporting each file's result separately so one
/// failure does not hide the others.
pub async fn fetch_outputs(
    client: &ControllerClient,
    outputs: &[OutputFile],
    options: &FetchOptions,
) -> Vec<(String, Result<PathBuf, FetchError>)> {
    let mut results = Vec::with_capacity(outputs.len());
    for output in outputs {
        let result = fetch_one(client, output, options).await;
        results.push((output.path.clone(), result));
    }
    results
}

async fn fetch_one(
    client: &ControllerClient,
    output: &OutputFile,
    options: &FetchOptions,
) -> Result<PathBuf, FetchError> {
    let destination = destination_for(&options.out_dir, output)?;
    if !options.force && fs::symlink_metadata(&destination).is_ok() {
        return Err(FetchError::AlreadyExists(destination));
    }
    if let Some(parent) = destination.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(FetchError::Io)?;
    }

    let partial = partial_path(&destination);
    let downloaded = download(client, output, &partial).await;
    if downloaded.is_err() {
        let _ = tokio::fs::remove_file(&partial).await;
    }
    downloaded?;

    // Windows will not rename over an existing file.
    if options.force && destination.exists() {
        tokio::fs::remove_file(&destination)
            .await
            .map_err(FetchError::Io)?;
    }
    tokio::fs::rename(&partial, &destination)
        .await
        .map_err(FetchError::Io)?;
    Ok(destination)
}

async fn download(
    client: &ControllerClient,
    output: &OutputFile,
    partial: &Path,
) -> Result<(), FetchError> {
    let response = client
        .open_blob(&output.sha256)
        .await
        .map_err(FetchError::Controller)?
        .ok_or(FetchError::NoLongerStored)?;

    let mut file = tokio::fs::File::create(partial)
        .await
        .map_err(FetchError::Io)?;
    let mut hasher = Sha256::new();
    let mut received: u64 = 0;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk =
            chunk.map_err(|error| FetchError::Controller(ControllerClientError::Request(error)))?;
        received = received.saturating_add(chunk.len() as u64);
        // Stop at the reported size instead of filling the disk.
        if received > output.size_bytes {
            return Err(FetchError::ChecksumMismatch);
        }
        hasher.update(&chunk);
        file.write_all(&chunk).await.map_err(FetchError::Io)?;
    }
    file.flush().await.map_err(FetchError::Io)?;

    if received != output.size_bytes
        || Sha256Digest::from_bytes(hasher.finalize().into()) != output.sha256
    {
        return Err(FetchError::ChecksumMismatch);
    }
    Ok(())
}

fn partial_path(destination: &Path) -> PathBuf {
    let mut name = destination
        .file_name()
        .map(std::ffi::OsStr::to_owned)
        .unwrap_or_default();
    name.push(".part");
    destination.with_file_name(name)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use meld_controller::{
        api::{ControllerState, router},
        blob_store::{BlobLimits, BlobStore},
    };
    use tempfile::TempDir;

    use super::*;

    fn digest_of(data: &[u8]) -> Sha256Digest {
        Sha256Digest::from_bytes(Sha256::digest(data).into())
    }

    fn output(path: &str, data: &[u8]) -> OutputFile {
        OutputFile {
            path: path.to_owned(),
            sha256: digest_of(data),
            size_bytes: data.len() as u64,
        }
    }

    /// A controller holding the given contents, and a client for it.
    async fn controller_with(contents: &[&[u8]]) -> (ControllerClient, TempDir, TempDir) {
        let storage = TempDir::new().expect("storage");
        let store = BlobStore::new(storage.path(), BlobLimits::default()).expect("blob store");
        let state = ControllerState::new().with_blob_store(Arc::new(store));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr().expect("address");
        tokio::spawn(async move { axum::serve(listener, router(state)).await });
        let client = ControllerClient::new(&format!("http://{address}")).expect("client");

        let source = TempDir::new().expect("source");
        for (index, data) in contents.iter().enumerate() {
            let file = source.path().join(format!("blob{index}"));
            fs::write(&file, data).expect("write blob");
            client
                .upload_blob(&digest_of(data), &file, data.len() as u64)
                .await
                .expect("upload");
        }
        (client, storage, source)
    }

    fn options(out_dir: &TempDir, force: bool) -> FetchOptions {
        FetchOptions {
            out_dir: out_dir.path().to_owned(),
            force,
        }
    }

    #[tokio::test]
    async fn outputs_are_downloaded_into_their_declared_places() {
        let (client, _storage, _source) = controller_with(&[b"{}", b"all good"]).await;
        let out = TempDir::new().expect("out dir");
        let outputs = [
            output("result.json", b"{}"),
            output("report/summary.txt", b"all good"),
        ];

        let results = fetch_outputs(&client, &outputs, &options(&out, false)).await;

        assert!(
            results.iter().all(|(_, result)| result.is_ok()),
            "{results:?}"
        );
        assert_eq!(
            fs::read(out.path().join("result.json")).expect("file"),
            b"{}"
        );
        assert_eq!(
            fs::read(out.path().join("report/summary.txt")).expect("file"),
            b"all good"
        );
    }

    #[tokio::test]
    async fn existing_files_are_kept_unless_forced() {
        let (client, _storage, _source) = controller_with(&[b"new"]).await;
        let out = TempDir::new().expect("out dir");
        fs::write(out.path().join("result.txt"), b"mine").expect("existing file");
        let outputs = [output("result.txt", b"new")];

        let refused = fetch_outputs(&client, &outputs, &options(&out, false)).await;
        assert!(matches!(refused[0].1, Err(FetchError::AlreadyExists(_))));
        assert_eq!(
            fs::read(out.path().join("result.txt")).expect("file"),
            b"mine"
        );

        let forced = fetch_outputs(&client, &outputs, &options(&out, true)).await;
        assert!(forced[0].1.is_ok());
        assert_eq!(
            fs::read(out.path().join("result.txt")).expect("file"),
            b"new"
        );
    }

    #[tokio::test]
    async fn content_the_controller_no_longer_stores_is_reported_and_leaves_nothing() {
        let (client, _storage, _source) = controller_with(&[]).await;
        let out = TempDir::new().expect("out dir");

        let results = fetch_outputs(
            &client,
            &[output("gone.txt", b"gone")],
            &options(&out, false),
        )
        .await;

        assert!(matches!(results[0].1, Err(FetchError::NoLongerStored)));
        assert_eq!(fs::read_dir(out.path()).expect("out dir").count(), 0);
    }

    #[tokio::test]
    async fn content_that_does_not_match_the_reported_checksum_is_discarded() {
        let (client, _storage, _source) = controller_with(&[b"actual"]).await;
        let out = TempDir::new().expect("out dir");
        let mut lying = output("result.txt", b"actual");
        lying.size_bytes = 3;

        let results = fetch_outputs(&client, &[lying], &options(&out, false)).await;

        assert!(matches!(results[0].1, Err(FetchError::ChecksumMismatch)));
        assert_eq!(
            fs::read_dir(out.path()).expect("out dir").count(),
            0,
            "neither the file nor its .part may remain"
        );
    }

    #[tokio::test]
    async fn one_failure_does_not_stop_the_other_downloads() {
        let (client, _storage, _source) = controller_with(&[b"present"]).await;
        let out = TempDir::new().expect("out dir");
        let outputs = [
            output("missing.txt", b"missing"),
            output("present.txt", b"present"),
        ];

        let results = fetch_outputs(&client, &outputs, &options(&out, false)).await;

        assert!(results[0].1.is_err());
        assert!(results[1].1.is_ok());
    }

    #[test]
    fn paths_that_would_leave_the_output_directory_are_refused() {
        let out = Path::new("/work/out");
        for path in ["../escape", "/etc/passwd", "a/../../b", "a\\b", ""] {
            let result = destination_for(out, &output(path, b"x"));
            assert!(matches!(result, Err(FetchError::UnsafePath)), "{path:?}");
        }
        assert_eq!(
            destination_for(out, &output("report/summary.txt", b"x")).expect("safe"),
            Path::new("/work/out/report/summary.txt")
        );
    }
}
