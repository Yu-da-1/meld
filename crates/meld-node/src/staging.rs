//! Brings a job's declared input files into its workspace.
//!
//! Content is fetched from the controller by digest into a local cache, then
//! copied into the workspace. The cache lets repeated jobs skip the transfer;
//! the copy keeps a job from altering cached content.

use std::{
    collections::BTreeMap,
    fs,
    future::Future,
    io,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::Duration,
};

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use meld_core::{
    DataFailure, DataSpec, InputFile, MessageId, Sha256Digest, validate_relative_path,
};
use sha2::{Digest, Sha256};
use sysinfo::Disks;
use tokio::{fs::File, io::AsyncWriteExt, sync::Mutex as AsyncMutex, time::sleep};

pub const DEFAULT_CACHE_MAX_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const DOWNLOAD_ATTEMPTS: u32 = 3;
const RETRY_DELAY: Duration = Duration::from_millis(500);

pub type BlobStream = Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DownloadError {
    /// The controller does not hold this content; retrying cannot help.
    NotFound,
    /// The transfer failed in a way that may succeed on another attempt.
    Unavailable(String),
}

/// Where input content comes from.
pub trait BlobSource: Sync {
    fn open(
        &self,
        digest: &Sha256Digest,
    ) -> impl Future<Output = Result<BlobStream, DownloadError>> + Send;
}

#[derive(Debug, Clone, Copy)]
struct Entry {
    size_bytes: u64,
    /// Orders entries by recency without depending on clock resolution.
    last_used: u64,
    /// Copies in progress; such an entry is never evicted.
    in_use: u32,
}

#[derive(Debug, Default)]
struct Index {
    entries: BTreeMap<Sha256Digest, Entry>,
    used_bytes: u64,
    clock: u64,
}

impl Index {
    fn insert(&mut self, digest: Sha256Digest, size_bytes: u64, in_use: u32) {
        self.clock += 1;
        let entry = Entry {
            size_bytes,
            last_used: self.clock,
            in_use,
        };
        if let Some(replaced) = self.entries.insert(digest, entry) {
            self.used_bytes = self.used_bytes.saturating_sub(replaced.size_bytes);
        }
        self.used_bytes = self.used_bytes.saturating_add(size_bytes);
    }

    fn remove(&mut self, digest: &Sha256Digest) {
        if let Some(removed) = self.entries.remove(digest) {
            self.used_bytes = self.used_bytes.saturating_sub(removed.size_bytes);
        }
    }
}

#[derive(Debug)]
struct Shared {
    blobs_directory: PathBuf,
    staging_directory: PathBuf,
    max_bytes: u64,
    retry_delay: Duration,
    index: Mutex<Index>,
    /// Serializes concurrent fetches of the same content.
    flights: Mutex<BTreeMap<Sha256Digest, Arc<AsyncMutex<()>>>>,
}

/// Local copies of input content, keyed by digest.
#[derive(Debug, Clone)]
pub struct InputCache {
    shared: Arc<Shared>,
}

/// Keeps a cache entry from being evicted while it is being copied.
struct Lease {
    shared: Arc<Shared>,
    digest: Sha256Digest,
}

impl Drop for Lease {
    fn drop(&mut self) {
        let mut index = lock(&self.shared.index);
        if let Some(entry) = index.entries.get_mut(&self.digest) {
            entry.in_use = entry.in_use.saturating_sub(1);
        }
    }
}

/// Removes a partial download unless it was handed on.
struct PartialFile {
    path: PathBuf,
    keep: bool,
}

impl Drop for PartialFile {
    fn drop(&mut self) {
        if !self.keep {
            let _ = fs::remove_file(&self.path);
        }
    }
}

enum Attempt {
    /// Worth trying again.
    Retry,
    Fatal(DataFailure),
}

impl InputCache {
    /// Opens the cache under `state_directory`, adopting content left by an
    /// earlier run.
    pub fn new(state_directory: &Path, max_bytes: u64) -> io::Result<Self> {
        let root = state_directory.join("cache");
        let blobs_directory = root.join("blobs");
        let staging_directory = root.join("staging");
        fs::create_dir_all(&blobs_directory)?;
        fs::create_dir_all(&staging_directory)?;

        // A download that did not finish never became cached content.
        for entry in fs::read_dir(&staging_directory)? {
            let _ = fs::remove_file(entry?.path());
        }

        let mut index = Index::default();
        for entry in fs::read_dir(&blobs_directory)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            let digest = entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse::<Sha256Digest>().ok());
            if let (Some(digest), true) = (digest, metadata.is_file()) {
                index.insert(digest, metadata.len(), 0);
            }
        }

        Ok(Self {
            shared: Arc::new(Shared {
                blobs_directory,
                staging_directory,
                max_bytes,
                retry_delay: RETRY_DELAY,
                index: Mutex::new(index),
                flights: Mutex::new(BTreeMap::new()),
            }),
        })
    }

    #[cfg(test)]
    fn without_retry_delay(self) -> Self {
        let shared = Arc::try_unwrap(self.shared).expect("cache must not be shared yet");
        Self {
            shared: Arc::new(Shared {
                retry_delay: Duration::ZERO,
                ..shared
            }),
        }
    }

    pub fn contains(&self, digest: &Sha256Digest) -> bool {
        lock(&self.shared.index).entries.contains_key(digest)
    }

    /// Places the content of `input` at `destination`.
    pub async fn place<S: BlobSource>(
        &self,
        source: &S,
        input: &InputFile,
        destination: &Path,
    ) -> Result<(), DataFailure> {
        let flight = self.flight(&input.sha256);
        let result = async {
            // Another assignment fetching the same content goes first, and
            // this one then finds it cached.
            let _turn = flight.lock().await;
            if let Some(lease) = self.lease_cached(input) {
                return self.copy_cached(&lease, input, destination).await;
            }
            let mut partial = self.download(source, input).await?;
            match self.admit(input, &mut partial)? {
                Some(lease) => self.copy_cached(&lease, input, destination).await,
                // Too large to cache: hand the download over directly.
                None => move_file(&partial.path, destination).map_err(|error| {
                    tracing::warn!(path = %destination.display(), %error, "failed to place input");
                    DataFailure::LocalStorage {
                        path: input.path.clone(),
                    }
                }),
            }
        }
        .await;
        self.release_flight(&input.sha256, flight);
        result
    }

    fn flight(&self, digest: &Sha256Digest) -> Arc<AsyncMutex<()>> {
        Arc::clone(
            lock(&self.shared.flights)
                .entry(digest.clone())
                .or_default(),
        )
    }

    fn release_flight(&self, digest: &Sha256Digest, flight: Arc<AsyncMutex<()>>) {
        drop(flight);
        let mut flights = lock(&self.shared.flights);
        if flights
            .get(digest)
            .is_some_and(|remaining| Arc::strong_count(remaining) == 1)
        {
            flights.remove(digest);
        }
    }

    /// Reserves a cached copy of the content, if a sound one exists.
    fn lease_cached(&self, input: &InputFile) -> Option<Lease> {
        let mut index = lock(&self.shared.index);
        let entry = *index.entries.get(&input.sha256)?;
        let on_disk = fs::metadata(self.blob_path(&input.sha256)).map(|meta| meta.len());
        if entry.size_bytes != input.size_bytes || on_disk.ok() != Some(entry.size_bytes) {
            // Truncated or replaced since it was cached: do not trust it.
            index.remove(&input.sha256);
            let _ = fs::remove_file(self.blob_path(&input.sha256));
            return None;
        }

        index.clock += 1;
        let clock = index.clock;
        let entry = index.entries.get_mut(&input.sha256)?;
        entry.last_used = clock;
        entry.in_use += 1;
        Some(Lease {
            shared: Arc::clone(&self.shared),
            digest: input.sha256.clone(),
        })
    }

    async fn copy_cached(
        &self,
        lease: &Lease,
        input: &InputFile,
        destination: &Path,
    ) -> Result<(), DataFailure> {
        let failure = || DataFailure::LocalStorage {
            path: input.path.clone(),
        };
        let copied = tokio::fs::copy(self.blob_path(&lease.digest), destination)
            .await
            .map_err(|error| {
                tracing::warn!(path = %destination.display(), %error, "failed to copy input");
                failure()
            })?;
        if copied == input.size_bytes {
            Ok(())
        } else {
            Err(failure())
        }
    }

    /// Downloads and verifies the content into a partial file.
    async fn download<S: BlobSource>(
        &self,
        source: &S,
        input: &InputFile,
    ) -> Result<PartialFile, DataFailure> {
        let partial = PartialFile {
            path: self
                .shared
                .staging_directory
                .join(format!("{}.part", MessageId::generate())),
            keep: false,
        };
        let unavailable = || DataFailure::InputUnavailable {
            path: input.path.clone(),
        };

        for attempt in 1..=DOWNLOAD_ATTEMPTS {
            match self.try_download(source, input, &partial.path).await {
                Ok(()) => return Ok(partial),
                Err(Attempt::Fatal(failure)) => return Err(failure),
                Err(Attempt::Retry) if attempt == DOWNLOAD_ATTEMPTS => return Err(unavailable()),
                Err(Attempt::Retry) => sleep(self.shared.retry_delay).await,
            }
        }
        Err(unavailable())
    }

    async fn try_download<S: BlobSource>(
        &self,
        source: &S,
        input: &InputFile,
        path: &Path,
    ) -> Result<(), Attempt> {
        let local = |error: io::Error| {
            tracing::warn!(path = %path.display(), %error, "failed to write download");
            Attempt::Fatal(DataFailure::LocalStorage {
                path: input.path.clone(),
            })
        };
        let mismatch = || {
            Attempt::Fatal(DataFailure::ChecksumMismatch {
                path: input.path.clone(),
            })
        };

        let mut stream = match source.open(&input.sha256).await {
            Ok(stream) => stream,
            Err(DownloadError::NotFound) => {
                return Err(Attempt::Fatal(DataFailure::InputUnavailable {
                    path: input.path.clone(),
                }));
            }
            Err(DownloadError::Unavailable(message)) => {
                tracing::warn!(digest = %input.sha256, %message, "input download failed");
                return Err(Attempt::Retry);
            }
        };

        let mut file = File::create(path).await.map_err(local)?;
        let mut hasher = Sha256::new();
        let mut received: u64 = 0;
        while let Some(chunk) = stream.next().await {
            let Ok(chunk) = chunk else {
                return Err(Attempt::Retry);
            };
            received = received.saturating_add(chunk.len() as u64);
            // Stop at the declared size instead of filling the disk.
            if received > input.size_bytes {
                return Err(mismatch());
            }
            hasher.update(&chunk);
            file.write_all(&chunk).await.map_err(local)?;
        }
        if received != input.size_bytes
            || Sha256Digest::from_bytes(hasher.finalize().into()) != input.sha256
        {
            return Err(mismatch());
        }
        // A crash must not leave a short file under a name that vouches for it.
        file.sync_all().await.map_err(local)?;
        Ok(())
    }

    /// Moves a verified download into the cache, evicting older content if
    /// needed. Returns `None` when the content cannot be cached at all.
    fn admit(
        &self,
        input: &InputFile,
        partial: &mut PartialFile,
    ) -> Result<Option<Lease>, DataFailure> {
        let mut index = lock(&self.shared.index);
        if !self.make_room(&mut index, input.size_bytes) {
            return Ok(None);
        }
        move_file(&partial.path, &self.blob_path(&input.sha256)).map_err(|error| {
            tracing::warn!(digest = %input.sha256, %error, "failed to cache input");
            DataFailure::LocalStorage {
                path: input.path.clone(),
            }
        })?;
        partial.keep = true;
        index.insert(input.sha256.clone(), input.size_bytes, 1);
        Ok(Some(Lease {
            shared: Arc::clone(&self.shared),
            digest: input.sha256.clone(),
        }))
    }

    /// Evicts least-recently-used idle content until `needed_bytes` fit.
    ///
    /// Nothing is evicted unless that is enough.
    fn make_room(&self, index: &mut Index, needed_bytes: u64) -> bool {
        let max = self.shared.max_bytes;
        let overflow = index
            .used_bytes
            .saturating_add(needed_bytes)
            .saturating_sub(max);
        if needed_bytes > max {
            return false;
        }
        if overflow == 0 {
            return true;
        }

        let mut idle: Vec<(u64, u64, Sha256Digest)> = index
            .entries
            .iter()
            .filter(|(_, entry)| entry.in_use == 0)
            .map(|(digest, entry)| (entry.last_used, entry.size_bytes, digest.clone()))
            .collect();
        if idle.iter().map(|(_, size, _)| *size).sum::<u64>() < overflow {
            return false;
        }

        idle.sort();
        let mut freed: u64 = 0;
        for (_, size_bytes, victim) in idle {
            if freed >= overflow {
                break;
            }
            match fs::remove_file(self.blob_path(&victim)) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => {
                    tracing::warn!(blob = %victim, %error, "failed to evict cached input");
                    continue;
                }
            }
            index.remove(&victim);
            freed = freed.saturating_add(size_bytes);
        }
        freed >= overflow
    }

    fn blob_path(&self, digest: &Sha256Digest) -> PathBuf {
        self.shared.blobs_directory.join(digest.as_str())
    }
}

/// Renames `from` to `to`, copying when they are on different filesystems.
fn move_file(from: &Path, to: &Path) -> io::Result<()> {
    if fs::rename(from, to).is_ok() {
        return Ok(());
    }
    fs::copy(from, to)?;
    fs::remove_file(from)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // The index stays consistent between statements, so a panic elsewhere
    // does not make it unusable.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Writes every declared input into `workspace`, or reports why it cannot.
pub async fn stage_inputs<S: BlobSource>(
    cache: &InputCache,
    source: &S,
    data: &DataSpec,
    workspace: &Path,
) -> Result<(), DataFailure> {
    stage_inputs_with_free_space(cache, source, data, workspace, free_space(workspace)).await
}

async fn stage_inputs_with_free_space<S: BlobSource>(
    cache: &InputCache,
    source: &S,
    data: &DataSpec,
    workspace: &Path,
    free_bytes: Option<u64>,
) -> Result<(), DataFailure> {
    // The controller validates paths, but a node should not rely on that
    // before writing files.
    for input in &data.inputs {
        if validate_relative_path(&input.path).is_err() {
            return Err(DataFailure::InvalidPath {
                path: input.path.clone(),
            });
        }
    }
    check_free_space(cache, data, free_bytes)?;

    for input in &data.inputs {
        let destination = destination_path(workspace, &input.path);
        if let Some(parent) = destination.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|error| {
                tracing::warn!(path = %parent.display(), %error, "failed to create input directory");
                DataFailure::LocalStorage {
                    path: input.path.clone(),
                }
            })?;
        }
        cache.place(source, input, &destination).await?;
        if input.executable {
            make_executable(&destination).await.map_err(|error| {
                tracing::warn!(path = %destination.display(), %error, "failed to mark input executable");
                DataFailure::LocalStorage {
                    path: input.path.clone(),
                }
            })?;
        }
    }
    Ok(())
}

#[cfg(unix)]
async fn make_executable(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    tokio::fs::set_permissions(path, fs::Permissions::from_mode(0o755)).await
}

/// Windows decides what is executable by file name, so there is nothing to set.
#[cfg(not(unix))]
async fn make_executable(_path: &Path) -> io::Result<()> {
    Ok(())
}

/// Content not yet cached must be stored twice: once in the cache and once in
/// the workspace. Concurrent jobs can still race for the same space.
fn check_free_space(
    cache: &InputCache,
    data: &DataSpec,
    free_bytes: Option<u64>,
) -> Result<(), DataFailure> {
    let Some(available_bytes) = free_bytes else {
        return Ok(());
    };
    let mut uncached: BTreeMap<&Sha256Digest, u64> = BTreeMap::new();
    for input in &data.inputs {
        if !cache.contains(&input.sha256) {
            uncached.insert(&input.sha256, input.size_bytes);
        }
    }
    let needed_bytes = data
        .total_input_bytes()
        .saturating_add(uncached.values().sum::<u64>());
    if needed_bytes > available_bytes {
        return Err(DataFailure::InsufficientDisk {
            needed_bytes,
            available_bytes,
        });
    }
    Ok(())
}

/// Maps a `/`-separated workspace path onto the platform's separator.
fn destination_path(workspace: &Path, relative: &str) -> PathBuf {
    relative
        .split('/')
        .fold(workspace.to_owned(), |path, component| path.join(component))
}

/// Free bytes on the filesystem holding `path`, if the system reports it.
fn free_space(path: &Path) -> Option<u64> {
    let path = path.canonicalize().ok()?;
    Disks::new_with_refreshed_list()
        .list()
        .iter()
        .filter(|disk| path.starts_with(disk.mount_point()))
        .max_by_key(|disk| disk.mount_point().as_os_str().len())
        .map(sysinfo::Disk::available_space)
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use futures_util::stream;
    use meld_core::OutputSpec;
    use tempfile::TempDir;

    use super::*;

    fn digest_of(data: &[u8]) -> Sha256Digest {
        Sha256Digest::from_bytes(Sha256::digest(data).into())
    }

    fn input(path: &str, data: &[u8]) -> InputFile {
        InputFile {
            path: path.to_owned(),
            sha256: digest_of(data),
            size_bytes: data.len() as u64,
            executable: false,
        }
    }

    /// Serves fixed content and counts how often it is asked for.
    #[derive(Default)]
    struct FakeSource {
        blobs: BTreeMap<Sha256Digest, Vec<u8>>,
        opens: AtomicUsize,
        /// Failures to return before serving normally.
        transient_failures: AtomicUsize,
        /// Serve this instead of the stored content.
        tamper: Option<Vec<u8>>,
        stall: bool,
    }

    impl FakeSource {
        fn with(contents: &[&[u8]]) -> Self {
            Self {
                blobs: contents
                    .iter()
                    .map(|data| (digest_of(data), data.to_vec()))
                    .collect(),
                ..Self::default()
            }
        }

        fn opens(&self) -> usize {
            self.opens.load(Ordering::SeqCst)
        }
    }

    impl BlobSource for FakeSource {
        async fn open(&self, digest: &Sha256Digest) -> Result<BlobStream, DownloadError> {
            self.opens.fetch_add(1, Ordering::SeqCst);
            tokio::task::yield_now().await;
            if self
                .transient_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    left.checked_sub(1)
                })
                .is_ok()
            {
                return Err(DownloadError::Unavailable("connection reset".to_owned()));
            }
            if self.stall {
                return Ok(Box::pin(stream::pending()));
            }
            let content = match &self.tamper {
                Some(content) => content.clone(),
                None => self
                    .blobs
                    .get(digest)
                    .cloned()
                    .ok_or(DownloadError::NotFound)?,
            };
            // Small chunks exercise the streaming path.
            let chunks: Vec<io::Result<Bytes>> = content
                .chunks(3)
                .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
                .collect();
            Ok(Box::pin(stream::iter(chunks)))
        }
    }

    struct Fixture {
        state: TempDir,
        workspace: TempDir,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                state: TempDir::new().expect("state dir"),
                workspace: TempDir::new().expect("workspace dir"),
            }
        }

        fn cache(&self, max_bytes: u64) -> InputCache {
            InputCache::new(self.state.path(), max_bytes)
                .expect("cache should open")
                .without_retry_delay()
        }

        fn at(&self, name: &str) -> PathBuf {
            self.workspace.path().join(name)
        }

        fn partial_files(&self) -> usize {
            fs::read_dir(self.state.path().join("cache").join("staging"))
                .expect("staging directory should exist")
                .count()
        }
    }

    #[tokio::test]
    async fn downloaded_input_is_cached_and_placed() {
        let fixture = Fixture::new();
        let cache = fixture.cache(1000);
        let source = FakeSource::with(&[b"hello world"]);
        let input = input("data.txt", b"hello world");

        cache
            .place(&source, &input, &fixture.at("data.txt"))
            .await
            .expect("placement should succeed");

        assert_eq!(
            fs::read(fixture.at("data.txt")).expect("placed file"),
            b"hello world"
        );
        assert!(cache.contains(&input.sha256));
        assert_eq!(fixture.partial_files(), 0);
    }

    #[tokio::test]
    async fn cached_input_is_not_downloaded_again() {
        let fixture = Fixture::new();
        let cache = fixture.cache(1000);
        let source = FakeSource::with(&[b"reused"]);
        let input = input("a", b"reused");

        cache
            .place(&source, &input, &fixture.at("first"))
            .await
            .expect("first placement");
        cache
            .place(&source, &input, &fixture.at("second"))
            .await
            .expect("second placement");

        assert_eq!(source.opens(), 1);
        assert_eq!(fs::read(fixture.at("second")).expect("copy"), b"reused");
    }

    #[tokio::test]
    async fn placed_copy_does_not_alias_the_cache() {
        let fixture = Fixture::new();
        let cache = fixture.cache(1000);
        let source = FakeSource::with(&[b"original"]);
        let input = input("a", b"original");
        cache
            .place(&source, &input, &fixture.at("first"))
            .await
            .expect("first placement");

        fs::write(fixture.at("first"), b"modified by the job").expect("job edits its copy");
        cache
            .place(&source, &input, &fixture.at("second"))
            .await
            .expect("second placement");

        assert_eq!(fs::read(fixture.at("second")).expect("copy"), b"original");
    }

    #[tokio::test]
    async fn content_with_the_wrong_digest_is_rejected_and_not_cached() {
        let fixture = Fixture::new();
        let cache = fixture.cache(1000);
        let mut source = FakeSource::with(&[b"genuine"]);
        source.tamper = Some(b"tampered".to_vec());
        let declared = input("data.txt", b"genuine");

        let failure = cache
            .place(&source, &declared, &fixture.at("data.txt"))
            .await
            .expect_err("tampered content must be rejected");

        assert_eq!(
            failure,
            DataFailure::ChecksumMismatch {
                path: "data.txt".to_owned()
            }
        );
        assert!(!cache.contains(&declared.sha256));
        assert!(!fixture.at("data.txt").exists());
        assert_eq!(fixture.partial_files(), 0);
    }

    #[tokio::test]
    async fn content_longer_than_declared_is_rejected() {
        let fixture = Fixture::new();
        let cache = fixture.cache(1000);
        let mut source = FakeSource::with(&[b"short"]);
        source.tamper = Some(b"short and then some".to_vec());

        let failure = cache
            .place(&source, &input("f", b"short"), &fixture.at("f"))
            .await
            .expect_err("oversized content must be rejected");

        assert!(matches!(failure, DataFailure::ChecksumMismatch { .. }));
        assert_eq!(fixture.partial_files(), 0);
    }

    #[tokio::test]
    async fn missing_content_fails_without_retrying() {
        let fixture = Fixture::new();
        let cache = fixture.cache(1000);
        let source = FakeSource::default();

        let failure = cache
            .place(&source, &input("f", b"absent"), &fixture.at("f"))
            .await
            .expect_err("absent content must fail");

        assert_eq!(
            failure,
            DataFailure::InputUnavailable {
                path: "f".to_owned()
            }
        );
        assert_eq!(source.opens(), 1);
    }

    #[tokio::test]
    async fn transient_failures_are_retried() {
        let fixture = Fixture::new();
        let cache = fixture.cache(1000);
        let source = FakeSource::with(&[b"flaky"]);
        source.transient_failures.store(2, Ordering::SeqCst);

        cache
            .place(&source, &input("f", b"flaky"), &fixture.at("f"))
            .await
            .expect("third attempt should succeed");

        assert_eq!(source.opens(), 3);
    }

    #[tokio::test]
    async fn persistent_failures_give_up_after_three_attempts() {
        let fixture = Fixture::new();
        let cache = fixture.cache(1000);
        let source = FakeSource::with(&[b"down"]);
        source.transient_failures.store(10, Ordering::SeqCst);

        let failure = cache
            .place(&source, &input("f", b"down"), &fixture.at("f"))
            .await
            .expect_err("repeated failure must be reported");

        assert!(matches!(failure, DataFailure::InputUnavailable { .. }));
        assert_eq!(source.opens(), 3);
    }

    #[tokio::test]
    async fn concurrent_requests_for_the_same_content_download_once() {
        let fixture = Fixture::new();
        let cache = fixture.cache(1000);
        let source = FakeSource::with(&[b"shared"]);
        let input = input("f", b"shared");

        let (one, two) = (fixture.at("one"), fixture.at("two"));

        let (first, second) = tokio::join!(
            cache.place(&source, &input, &one),
            cache.place(&source, &input, &two),
        );

        first.expect("first placement");
        second.expect("second placement");
        assert_eq!(source.opens(), 1);
        assert_eq!(fs::read(fixture.at("two")).expect("copy"), b"shared");
    }

    #[tokio::test]
    async fn full_cache_evicts_the_least_recently_used_content() {
        let fixture = Fixture::new();
        let cache = fixture.cache(8);
        let source = FakeSource::with(&[b"aaaa", b"bbbb", b"cccc"]);
        for (name, data) in [("a", b"aaaa"), ("b", b"bbbb")] {
            cache
                .place(&source, &input(name, data), &fixture.at(name))
                .await
                .expect("fill cache");
        }
        // Using `a` again makes `b` the least recently used.
        cache
            .place(&source, &input("a", b"aaaa"), &fixture.at("a2"))
            .await
            .expect("reuse a");

        cache
            .place(&source, &input("c", b"cccc"), &fixture.at("c"))
            .await
            .expect("c should evict b");

        assert!(cache.contains(&digest_of(b"aaaa")));
        assert!(!cache.contains(&digest_of(b"bbbb")));
        assert!(cache.contains(&digest_of(b"cccc")));
    }

    #[tokio::test]
    async fn content_larger_than_the_cache_is_placed_without_caching() {
        let fixture = Fixture::new();
        let cache = fixture.cache(4);
        let source = FakeSource::with(&[b"too large to cache"]);
        let input = input("big", b"too large to cache");

        cache
            .place(&source, &input, &fixture.at("big"))
            .await
            .expect("placement should still succeed");

        assert_eq!(
            fs::read(fixture.at("big")).expect("placed file"),
            b"too large to cache"
        );
        assert!(!cache.contains(&input.sha256));
        assert_eq!(fixture.partial_files(), 0);
    }

    #[tokio::test]
    async fn cached_content_that_vanished_is_downloaded_again() {
        let fixture = Fixture::new();
        let cache = fixture.cache(1000);
        let source = FakeSource::with(&[b"fragile"]);
        let input = input("f", b"fragile");
        cache
            .place(&source, &input, &fixture.at("one"))
            .await
            .expect("first placement");
        fs::remove_file(
            fixture
                .state
                .path()
                .join("cache")
                .join("blobs")
                .join(input.sha256.as_str()),
        )
        .expect("external delete");

        cache
            .place(&source, &input, &fixture.at("two"))
            .await
            .expect("second placement");

        assert_eq!(source.opens(), 2);
        assert_eq!(fs::read(fixture.at("two")).expect("copy"), b"fragile");
    }

    #[tokio::test]
    async fn cache_survives_a_restart_and_discards_partial_downloads() {
        let fixture = Fixture::new();
        let source = FakeSource::with(&[b"durable"]);
        let input = input("f", b"durable");
        fixture
            .cache(1000)
            .place(&source, &input, &fixture.at("one"))
            .await
            .expect("first placement");
        fs::write(
            fixture
                .state
                .path()
                .join("cache")
                .join("staging")
                .join("stale.part"),
            b"x",
        )
        .expect("leftover download");

        let reopened = fixture.cache(1000);
        reopened
            .place(&source, &input, &fixture.at("two"))
            .await
            .expect("placement after restart");

        assert_eq!(source.opens(), 1);
        assert_eq!(fixture.partial_files(), 0);
    }

    #[tokio::test]
    async fn abandoned_download_leaves_nothing_and_does_not_block_the_next() {
        let fixture = Fixture::new();
        let cache = fixture.cache(1000);
        let mut stalled = FakeSource::with(&[b"slow"]);
        stalled.stall = true;
        let input = input("f", b"slow");

        // What a cancellation does: the staging future is dropped mid-transfer.
        let abandoned = tokio::time::timeout(
            Duration::from_millis(50),
            cache.place(&stalled, &input, &fixture.at("f")),
        )
        .await;
        assert!(abandoned.is_err());
        assert_eq!(fixture.partial_files(), 0);

        let working = FakeSource::with(&[b"slow"]);
        cache
            .place(&working, &input, &fixture.at("f"))
            .await
            .expect("a later job can fetch the same content");
    }

    #[tokio::test]
    async fn staging_creates_directories_and_places_every_input() {
        let fixture = Fixture::new();
        let cache = fixture.cache(1000);
        let source = FakeSource::with(&[b"one", b"two"]);
        let data = DataSpec {
            inputs: vec![
                input("data/a.txt", b"one"),
                input("scripts/run/b.txt", b"two"),
            ],
            outputs: vec![OutputSpec {
                path: "out.txt".to_owned(),
            }],
        };

        stage_inputs(&cache, &source, &data, fixture.workspace.path())
            .await
            .expect("staging should succeed");

        assert_eq!(fs::read(fixture.at("data/a.txt")).expect("a"), b"one");
        assert_eq!(
            fs::read(fixture.at("scripts/run/b.txt")).expect("b"),
            b"two"
        );
        assert!(!fixture.at("out.txt").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn only_inputs_marked_executable_can_be_run() {
        use std::os::unix::fs::PermissionsExt;

        let fixture = Fixture::new();
        let cache = fixture.cache(1000);
        let script = b"#!/bin/sh\necho staged\n";
        let source = FakeSource::with(&[script, b"data"]);
        let data = DataSpec {
            inputs: vec![
                InputFile {
                    executable: true,
                    ..input("run.sh", script)
                },
                input("data.txt", b"data"),
            ],
            outputs: vec![],
        };

        stage_inputs(&cache, &source, &data, fixture.workspace.path())
            .await
            .expect("staging should succeed");

        let mode = |name: &str| {
            fs::metadata(fixture.at(name))
                .expect("staged file")
                .permissions()
                .mode()
        };
        assert_ne!(mode("run.sh") & 0o111, 0);
        assert_eq!(mode("data.txt") & 0o111, 0);
        let output = std::process::Command::new(fixture.at("run.sh"))
            .output()
            .expect("the staged script should run");
        assert_eq!(output.stdout, b"staged\n");
        // The cached copy stays unmarked, so other jobs get what they ask for.
        let cached = fixture
            .state
            .path()
            .join("cache")
            .join("blobs")
            .join(digest_of(script).as_str());
        assert_eq!(
            fs::metadata(cached)
                .expect("cached file")
                .permissions()
                .mode()
                & 0o111,
            0
        );
    }

    #[tokio::test]
    async fn staging_refuses_paths_that_could_escape_the_workspace() {
        let fixture = Fixture::new();
        let cache = fixture.cache(1000);
        let source = FakeSource::with(&[b"x"]);

        for path in ["../escape", "/etc/passwd", "a/../../b"] {
            let data = DataSpec {
                inputs: vec![input(path, b"x")],
                outputs: vec![],
            };

            let failure = stage_inputs(&cache, &source, &data, fixture.workspace.path())
                .await
                .expect_err("unsafe path must be refused");

            assert_eq!(
                failure,
                DataFailure::InvalidPath {
                    path: path.to_owned()
                }
            );
        }
        assert_eq!(source.opens(), 0);
    }

    #[tokio::test]
    async fn staging_fails_before_downloading_when_the_disk_is_too_small() {
        let fixture = Fixture::new();
        let cache = fixture.cache(1000);
        let source = FakeSource::with(&[b"0123456789"]);
        let data = DataSpec {
            inputs: vec![input("f", b"0123456789")],
            outputs: vec![],
        };

        // Uncached content needs room twice: in the cache and the workspace.
        let failure = stage_inputs_with_free_space(
            &cache,
            &source,
            &data,
            fixture.workspace.path(),
            Some(15),
        )
        .await
        .expect_err("not enough room");

        assert_eq!(
            failure,
            DataFailure::InsufficientDisk {
                needed_bytes: 20,
                available_bytes: 15
            }
        );
        assert_eq!(source.opens(), 0);
    }

    #[tokio::test]
    async fn cached_content_needs_room_only_for_the_workspace_copy() {
        let fixture = Fixture::new();
        let cache = fixture.cache(1000);
        let source = FakeSource::with(&[b"0123456789"]);
        let data = DataSpec {
            inputs: vec![input("f", b"0123456789")],
            outputs: vec![],
        };
        cache
            .place(&source, &data.inputs[0], &fixture.at("warm"))
            .await
            .expect("warm the cache");

        stage_inputs_with_free_space(&cache, &source, &data, fixture.workspace.path(), Some(10))
            .await
            .expect("ten bytes are enough for one copy");
    }

    #[test]
    fn free_space_lookup_reports_something_sensible() {
        let directory = TempDir::new().expect("temp dir");

        // Some platforms report nothing; the check is then skipped.
        assert!(free_space(directory.path()).is_none_or(|bytes| bytes > 0));
    }
}
