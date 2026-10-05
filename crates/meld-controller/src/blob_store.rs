//! Content-addressed storage for job input and output files.
//!
//! A blob is named by the SHA-256 of its bytes, so storing the same content
//! twice is a no-op and a stored name can never disagree with its content.
//! Uploads stream into a staging file and are renamed into place only after
//! the digest matches, so a reader never sees a partial blob.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt, fs, io,
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard},
    time::{Duration, Instant},
};

use futures_util::{Stream, StreamExt};
use meld_core::{MAX_FILE_BYTES, MessageId, Sha256Digest};
use sha2::{Digest, Sha256};
use tokio::{fs::File, io::AsyncWriteExt};

pub const DEFAULT_QUOTA_BYTES: u64 = 16 * 1024 * 1024 * 1024;
/// A blob touched this recently is never evicted.
///
/// This keeps a freshly uploaded input alive until the job that needs it is
/// submitted and starts protecting it.
pub const DEFAULT_MIN_RETENTION: Duration = Duration::from_secs(10 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobLimits {
    /// Largest single blob accepted.
    pub max_blob_bytes: u64,
    /// Total bytes the store may hold.
    pub quota_bytes: u64,
    pub min_retention: Duration,
}

impl Default for BlobLimits {
    fn default() -> Self {
        Self {
            max_blob_bytes: MAX_FILE_BYTES,
            quota_bytes: DEFAULT_QUOTA_BYTES,
            min_retention: DEFAULT_MIN_RETENTION,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutOutcome {
    Created {
        size_bytes: u64,
    },
    /// The content was already stored; the upload changed nothing.
    Existing {
        size_bytes: u64,
    },
}

impl PutOutcome {
    pub const fn size_bytes(self) -> u64 {
        match self {
            Self::Created { size_bytes } | Self::Existing { size_bytes } => size_bytes,
        }
    }
}

#[derive(Debug)]
pub struct StoredBlob {
    pub file: File,
    pub size_bytes: u64,
}

#[derive(Debug)]
pub enum BlobError {
    TooLarge {
        limit_bytes: u64,
    },
    DigestMismatch,
    QuotaExceeded {
        quota_bytes: u64,
    },
    /// The upload stream failed before the content was complete.
    Body(String),
    Io(io::Error),
    /// A previous failure left the index in an unknown state.
    Unavailable,
}

impl fmt::Display for BlobError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLarge { limit_bytes } => {
                write!(formatter, "blob exceeds the {limit_bytes} byte limit")
            }
            Self::DigestMismatch => formatter.write_str("content does not match its digest"),
            Self::QuotaExceeded { quota_bytes } => write!(
                formatter,
                "blob storage is full ({quota_bytes} byte quota) and nothing can be evicted"
            ),
            Self::Body(message) => write!(formatter, "upload was interrupted: {message}"),
            Self::Io(error) => write!(formatter, "blob storage failed: {error}"),
            Self::Unavailable => formatter.write_str("blob storage is unavailable"),
        }
    }
}

impl Error for BlobError {}

impl From<io::Error> for BlobError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Debug, Clone, Copy)]
struct Entry {
    size_bytes: u64,
    /// Orders entries by recency without depending on clock resolution.
    last_used: u64,
    touched_at: Instant,
}

#[derive(Debug, Default)]
struct Index {
    entries: BTreeMap<Sha256Digest, Entry>,
    used_bytes: u64,
    clock: u64,
}

impl Index {
    fn insert(&mut self, digest: Sha256Digest, size_bytes: u64) {
        self.clock += 1;
        let entry = Entry {
            size_bytes,
            last_used: self.clock,
            touched_at: Instant::now(),
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

    /// Marks a blob as just used and returns its size.
    fn touch(&mut self, digest: &Sha256Digest) -> Option<u64> {
        self.clock += 1;
        let entry = self.entries.get_mut(digest)?;
        entry.last_used = self.clock;
        entry.touched_at = Instant::now();
        Some(entry.size_bytes)
    }
}

/// Removes a staging file unless the upload committed it.
struct StagedFile {
    path: PathBuf,
    committed: bool,
}

impl Drop for StagedFile {
    fn drop(&mut self) {
        if !self.committed {
            // A leftover partial file is harmless and cleared on startup.
            let _ = fs::remove_file(&self.path);
        }
    }
}

#[derive(Debug)]
pub struct BlobStore {
    blobs_directory: PathBuf,
    staging_directory: PathBuf,
    limits: BlobLimits,
    index: Mutex<Index>,
}

impl BlobStore {
    /// Opens the store under `root`, adopting blobs left by an earlier run.
    pub fn new(root: &Path, limits: BlobLimits) -> io::Result<Self> {
        let blobs_directory = root.join("blobs");
        let staging_directory = root.join("staging");
        fs::create_dir_all(&blobs_directory)?;
        fs::create_dir_all(&staging_directory)?;

        // An upload that did not finish never became a blob.
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
                index.insert(digest, metadata.len());
            }
        }

        Ok(Self {
            blobs_directory,
            staging_directory,
            limits,
            index: Mutex::new(index),
        })
    }

    pub const fn limits(&self) -> BlobLimits {
        self.limits
    }

    /// Returns the stored size of a blob and counts the lookup as a use.
    pub fn size_of(&self, digest: &Sha256Digest) -> Result<Option<u64>, BlobError> {
        Ok(self.lock()?.touch(digest))
    }

    /// Opens a stored blob for reading.
    pub async fn read(&self, digest: &Sha256Digest) -> Result<Option<StoredBlob>, BlobError> {
        let Some(size_bytes) = self.lock()?.touch(digest) else {
            return Ok(None);
        };
        match File::open(self.blob_path(digest)).await {
            Ok(file) => Ok(Some(StoredBlob { file, size_bytes })),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                // Removed behind our back; forget it so it can be uploaded again.
                self.lock()?.remove(digest);
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Stores the content of `body` under `digest` if it hashes to `digest`.
    ///
    /// `pinned` blobs are never evicted to make room.
    pub async fn put<S, B, E>(
        &self,
        digest: &Sha256Digest,
        mut body: S,
        pinned: &BTreeSet<Sha256Digest>,
    ) -> Result<PutOutcome, BlobError>
    where
        S: Stream<Item = Result<B, E>> + Unpin,
        B: AsRef<[u8]>,
        E: fmt::Display,
    {
        if let Some(size_bytes) = self.lock()?.touch(digest) {
            return Ok(PutOutcome::Existing { size_bytes });
        }

        let staged = StagedFile {
            path: self
                .staging_directory
                .join(format!("{}.part", MessageId::generate())),
            committed: false,
        };
        let mut file = File::create(&staged.path).await?;
        let mut hasher = Sha256::new();
        let mut size_bytes: u64 = 0;
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|error| BlobError::Body(error.to_string()))?;
            let chunk = chunk.as_ref();
            size_bytes = size_bytes.saturating_add(chunk.len() as u64);
            if size_bytes > self.limits.max_blob_bytes {
                return Err(BlobError::TooLarge {
                    limit_bytes: self.limits.max_blob_bytes,
                });
            }
            hasher.update(chunk);
            file.write_all(chunk).await?;
        }
        // A crash must not leave a short file under a name that vouches for it.
        file.sync_all().await?;
        drop(file);

        if Sha256Digest::from_bytes(hasher.finalize().into()) != *digest {
            return Err(BlobError::DigestMismatch);
        }
        self.commit(digest, size_bytes, staged, pinned)
    }

    fn commit(
        &self,
        digest: &Sha256Digest,
        size_bytes: u64,
        mut staged: StagedFile,
        pinned: &BTreeSet<Sha256Digest>,
    ) -> Result<PutOutcome, BlobError> {
        let mut index = self.lock()?;
        // Another upload of the same content may have finished first.
        if let Some(size_bytes) = index.touch(digest) {
            return Ok(PutOutcome::Existing { size_bytes });
        }
        self.make_room(&mut index, size_bytes, pinned)?;
        fs::rename(&staged.path, self.blob_path(digest))?;
        staged.committed = true;
        index.insert(digest.clone(), size_bytes);
        Ok(PutOutcome::Created { size_bytes })
    }

    /// Evicts least-recently-used blobs until `needed_bytes` fit in the quota.
    ///
    /// Nothing is evicted unless that is enough, so a refused upload does not
    /// cost the store its cache.
    fn make_room(
        &self,
        index: &mut Index,
        needed_bytes: u64,
        pinned: &BTreeSet<Sha256Digest>,
    ) -> Result<(), BlobError> {
        let quota = self.limits.quota_bytes;
        let overflow = index
            .used_bytes
            .saturating_add(needed_bytes)
            .saturating_sub(quota);
        if overflow == 0 {
            return Ok(());
        }

        let now = Instant::now();
        let mut candidates: Vec<(u64, u64, Sha256Digest)> = index
            .entries
            .iter()
            .filter(|(digest, entry)| {
                !pinned.contains(*digest)
                    && now.saturating_duration_since(entry.touched_at) >= self.limits.min_retention
            })
            .map(|(digest, entry)| (entry.last_used, entry.size_bytes, digest.clone()))
            .collect();
        let evictable: u64 = candidates.iter().map(|(_, size, _)| *size).sum();
        if evictable < overflow {
            return Err(BlobError::QuotaExceeded { quota_bytes: quota });
        }

        candidates.sort();
        let mut freed: u64 = 0;
        for (_, size_bytes, victim) in candidates {
            if freed >= overflow {
                break;
            }
            match fs::remove_file(self.blob_path(&victim)) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => {
                    tracing::warn!(blob = %victim, %error, "failed to evict blob");
                    continue;
                }
            }
            index.remove(&victim);
            freed = freed.saturating_add(size_bytes);
        }
        if freed < overflow {
            return Err(BlobError::QuotaExceeded { quota_bytes: quota });
        }
        Ok(())
    }

    fn blob_path(&self, digest: &Sha256Digest) -> PathBuf {
        self.blobs_directory.join(digest.as_str())
    }

    fn lock(&self) -> Result<MutexGuard<'_, Index>, BlobError> {
        self.index.lock().map_err(|_| BlobError::Unavailable)
    }
}

#[cfg(test)]
mod tests {
    use futures_util::stream;
    use tempfile::TempDir;
    use tokio::io::AsyncReadExt;

    use super::*;

    fn digest_of(data: &[u8]) -> Sha256Digest {
        Sha256Digest::from_bytes(Sha256::digest(data).into())
    }

    /// A body split into three-byte chunks, to exercise streaming.
    fn body(data: &[u8]) -> impl Stream<Item = Result<Vec<u8>, io::Error>> + Unpin {
        stream::iter(
            data.chunks(3)
                .map(|chunk| Ok(chunk.to_vec()))
                .collect::<Vec<_>>(),
        )
    }

    fn store(directory: &TempDir, quota_bytes: u64, min_retention: Duration) -> BlobStore {
        BlobStore::new(
            directory.path(),
            BlobLimits {
                max_blob_bytes: 100,
                quota_bytes,
                min_retention,
            },
        )
        .expect("store should open")
    }

    async fn put(store: &BlobStore, data: &[u8]) -> Result<PutOutcome, BlobError> {
        store
            .put(&digest_of(data), body(data), &BTreeSet::new())
            .await
    }

    fn staging_is_empty(directory: &TempDir) -> bool {
        fs::read_dir(directory.path().join("staging"))
            .expect("staging directory should exist")
            .next()
            .is_none()
    }

    #[tokio::test]
    async fn stored_blob_can_be_read_back() {
        let directory = TempDir::new().expect("temp dir");
        let store = store(&directory, 1000, Duration::ZERO);

        let outcome = put(&store, b"hello world").await.expect("put should work");
        let stored = store
            .read(&digest_of(b"hello world"))
            .await
            .expect("read should work")
            .expect("blob should exist");

        assert_eq!(outcome, PutOutcome::Created { size_bytes: 11 });
        assert_eq!(stored.size_bytes, 11);
        let mut content = Vec::new();
        let mut file = stored.file;
        file.read_to_end(&mut content).await.expect("readable");
        assert_eq!(content, b"hello world");
        assert!(staging_is_empty(&directory));
    }

    #[tokio::test]
    async fn empty_content_is_a_valid_blob() {
        let directory = TempDir::new().expect("temp dir");
        let store = store(&directory, 1000, Duration::ZERO);

        let outcome = put(&store, b"").await.expect("put should work");

        assert_eq!(outcome, PutOutcome::Created { size_bytes: 0 });
        assert_eq!(store.size_of(&digest_of(b"")).expect("lookup"), Some(0));
    }

    #[tokio::test]
    async fn storing_the_same_content_twice_changes_nothing() {
        let directory = TempDir::new().expect("temp dir");
        let store = store(&directory, 1000, Duration::ZERO);
        put(&store, b"same").await.expect("first put");

        let second = put(&store, b"same").await.expect("second put");

        assert_eq!(second, PutOutcome::Existing { size_bytes: 4 });
        assert_eq!(store.lock().expect("lock").used_bytes, 4);
    }

    #[tokio::test]
    async fn content_that_does_not_match_its_digest_is_rejected() {
        let directory = TempDir::new().expect("temp dir");
        let store = store(&directory, 1000, Duration::ZERO);
        let claimed = digest_of(b"expected");

        let error = store
            .put(&claimed, body(b"different"), &BTreeSet::new())
            .await
            .expect_err("mismatch must be rejected");

        assert!(matches!(error, BlobError::DigestMismatch));
        assert_eq!(store.size_of(&claimed).expect("lookup"), None);
        assert!(staging_is_empty(&directory));
    }

    #[tokio::test]
    async fn oversized_blob_is_rejected_while_streaming() {
        let directory = TempDir::new().expect("temp dir");
        let store = store(&directory, 1000, Duration::ZERO);
        let data = vec![7u8; 101];

        let error = put(&store, &data).await.expect_err("limit must apply");

        assert!(matches!(error, BlobError::TooLarge { limit_bytes: 100 }));
        assert!(staging_is_empty(&directory));
    }

    #[tokio::test]
    async fn interrupted_upload_stores_nothing() {
        let directory = TempDir::new().expect("temp dir");
        let store = store(&directory, 1000, Duration::ZERO);
        let data = b"partial";
        let broken = stream::iter(vec![
            Ok(b"par".to_vec()),
            Err(io::Error::other("connection reset")),
        ]);

        let error = store
            .put(&digest_of(data), broken, &BTreeSet::new())
            .await
            .expect_err("a broken stream must fail");

        assert!(matches!(error, BlobError::Body(_)));
        assert_eq!(store.size_of(&digest_of(data)).expect("lookup"), None);
        assert!(staging_is_empty(&directory));
    }

    #[tokio::test]
    async fn full_store_evicts_least_recently_used_blob() {
        let directory = TempDir::new().expect("temp dir");
        let store = store(&directory, 8, Duration::ZERO);
        put(&store, b"aaaa").await.expect("a");
        put(&store, b"bbbb").await.expect("b");
        // Using `a` makes `b` the least recently used.
        store.size_of(&digest_of(b"aaaa")).expect("touch");

        put(&store, b"cccc").await.expect("c should evict b");

        assert_eq!(store.size_of(&digest_of(b"aaaa")).expect("a"), Some(4));
        assert_eq!(store.size_of(&digest_of(b"bbbb")).expect("b"), None);
        assert_eq!(store.size_of(&digest_of(b"cccc")).expect("c"), Some(4));
        assert!(
            !directory
                .path()
                .join("blobs")
                .join(digest_of(b"bbbb").as_str())
                .exists()
        );
    }

    #[tokio::test]
    async fn pinned_blobs_are_never_evicted() {
        let directory = TempDir::new().expect("temp dir");
        let store = store(&directory, 8, Duration::ZERO);
        put(&store, b"aaaa").await.expect("a");
        put(&store, b"bbbb").await.expect("b");
        let pinned = BTreeSet::from([digest_of(b"aaaa"), digest_of(b"bbbb")]);

        let error = store
            .put(&digest_of(b"cccc"), body(b"cccc"), &pinned)
            .await
            .expect_err("nothing is evictable");

        assert!(matches!(error, BlobError::QuotaExceeded { quota_bytes: 8 }));
        assert_eq!(store.size_of(&digest_of(b"aaaa")).expect("a"), Some(4));
        assert_eq!(store.size_of(&digest_of(b"bbbb")).expect("b"), Some(4));
        assert!(staging_is_empty(&directory));
    }

    #[tokio::test]
    async fn refused_upload_does_not_evict_anything() {
        let directory = TempDir::new().expect("temp dir");
        let store = store(&directory, 8, Duration::ZERO);
        put(&store, b"aaaa").await.expect("a");
        put(&store, b"bbbb").await.expect("b");
        let pinned = BTreeSet::from([digest_of(b"aaaa")]);

        // Needs 6 bytes of room but only `b` (4 bytes) may go.
        let error = store
            .put(&digest_of(b"cccccc"), body(b"cccccc"), &pinned)
            .await
            .expect_err("not enough can be freed");

        assert!(matches!(error, BlobError::QuotaExceeded { .. }));
        assert_eq!(store.size_of(&digest_of(b"bbbb")).expect("b"), Some(4));
    }

    #[tokio::test]
    async fn recently_used_blobs_are_protected_by_min_retention() {
        let directory = TempDir::new().expect("temp dir");
        let store = store(&directory, 4, Duration::from_secs(3600));
        put(&store, b"aaaa").await.expect("a");

        let error = put(&store, b"bbbb").await.expect_err("a is still fresh");

        assert!(matches!(error, BlobError::QuotaExceeded { .. }));
        assert_eq!(store.size_of(&digest_of(b"aaaa")).expect("a"), Some(4));
    }

    #[tokio::test]
    async fn blob_larger_than_quota_is_rejected() {
        let directory = TempDir::new().expect("temp dir");
        let store = store(&directory, 3, Duration::ZERO);

        let error = put(&store, b"four").await.expect_err("exceeds quota");

        assert!(matches!(error, BlobError::QuotaExceeded { quota_bytes: 3 }));
    }

    #[tokio::test]
    async fn blobs_survive_a_restart_and_partial_uploads_are_discarded() {
        let directory = TempDir::new().expect("temp dir");
        {
            let store = store(&directory, 1000, Duration::ZERO);
            put(&store, b"persisted").await.expect("put");
        }
        fs::write(directory.path().join("staging").join("stale.part"), b"x")
            .expect("leftover upload");
        fs::write(directory.path().join("blobs").join("not-a-digest"), b"x")
            .expect("unrelated file");

        let reopened = store(&directory, 1000, Duration::ZERO);

        assert_eq!(
            reopened.size_of(&digest_of(b"persisted")).expect("lookup"),
            Some(9)
        );
        assert_eq!(reopened.lock().expect("lock").used_bytes, 9);
        assert!(staging_is_empty(&directory));
    }

    #[tokio::test]
    async fn blob_deleted_behind_the_stores_back_can_be_uploaded_again() {
        let directory = TempDir::new().expect("temp dir");
        let store = store(&directory, 1000, Duration::ZERO);
        let digest = digest_of(b"vanishing");
        put(&store, b"vanishing").await.expect("put");
        fs::remove_file(directory.path().join("blobs").join(digest.as_str()))
            .expect("external delete");

        assert!(store.read(&digest).await.expect("read").is_none());
        let outcome = put(&store, b"vanishing").await.expect("re-upload");

        assert_eq!(outcome, PutOutcome::Created { size_bytes: 9 });
    }
}
