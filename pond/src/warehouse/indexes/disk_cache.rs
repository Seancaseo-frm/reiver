//! Local disk cache for FST index blobs.
//!
//! Stores uncompressed blobs on the local filesystem so they can be
//! memory-mapped at query time without an R2 round-trip.
//!
//! Layout:
//! ```text
//! {base_path}/{project_id}/{table_name}.fst   -- version header (8 bytes LE i64) + uncompressed blob
//! ```
//!
//! The version is packed as the first 8 bytes of the file so that the
//! blob and its version are written atomically in a single temp-file +
//! rename operation, eliminating any race between the two values.
//!
//! Writes are atomic (temp file + rename) to avoid partial reads.

use memmap2::Mmap;
use std::fs;
use std::io::{self, Read as _, Write};
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// Size of the version header prepended to every cached blob file.
const VERSION_HEADER_SIZE: usize = 8;

/// Local disk cache for FST table index blobs.
pub struct DiskIndexCache {
    base_path: PathBuf,
}

impl DiskIndexCache {
    /// Create a new disk cache rooted at `base_path`.
    ///
    /// The directory is created if it does not exist.
    pub fn new(base_path: PathBuf) -> io::Result<Self> {
        fs::create_dir_all(&base_path)?;
        Ok(Self { base_path })
    }

    /// Path to the cached blob file (version header + uncompressed blob).
    pub fn local_path(&self, project_id: Uuid, table_name: &str) -> PathBuf {
        self.base_path
            .join(project_id.to_string())
            .join(format!("{}.fst", sanitize_table_name(table_name)))
    }

    /// Atomically store an uncompressed blob and its version number.
    ///
    /// The version is written as an 8-byte little-endian i64 header
    /// followed by the blob data, all in a single atomic write.
    pub fn store(
        &self,
        project_id: Uuid,
        table_name: &str,
        data: &[u8],
        version: i64,
    ) -> io::Result<()> {
        let blob_path = self.local_path(project_id, table_name);

        // Ensure parent directory exists
        if let Some(parent) = blob_path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Build a single buffer: 8-byte version header + blob data
        let mut buf = Vec::with_capacity(VERSION_HEADER_SIZE + data.len());
        buf.extend_from_slice(&version.to_le_bytes());
        buf.extend_from_slice(data);

        atomic_write(&blob_path, &buf)
    }

    /// Read the cached version number, or `None` if the file is missing or
    /// too short to contain a valid header.
    pub fn load_version(&self, project_id: Uuid, table_name: &str) -> Option<i64> {
        let path = self.local_path(project_id, table_name);
        let mut file = fs::File::open(&path).ok()?;
        let mut header = [0u8; VERSION_HEADER_SIZE];
        file.read_exact(&mut header).ok()?;
        Some(i64::from_le_bytes(header))
    }

    /// Memory-map the cached blob file, returning only the blob portion
    /// (skipping the 8-byte version header).
    ///
    /// # Safety
    ///
    /// The caller must ensure the file is not concurrently modified while
    /// the `Mmap` is alive. In practice, writes go through `store()` which
    /// uses atomic rename, so existing mmaps see the old (complete) file
    /// and new mmaps see the new one.
    pub fn mmap(&self, project_id: Uuid, table_name: &str) -> io::Result<Mmap> {
        let path = self.local_path(project_id, table_name);
        let file = fs::File::open(&path)?;

        let file_len = file.metadata()?.len() as usize;
        if file_len < VERSION_HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Cache file too short to contain version header",
            ));
        }

        // SAFETY: The file is written atomically (temp + rename), so it is
        // always complete. Concurrent readers see either the old or the new
        // file, never a partial write.
        //
        // We mmap the entire file; callers must use `&mmap[VERSION_HEADER_SIZE..]`
        // to access the blob data. Returning a full-file Mmap is necessary
        // because partial-file Mmap offsets must be page-aligned, and 8 bytes
        // is not page-aligned on any common platform.
        unsafe { Mmap::map(&file) }
    }

    /// Memory-map the cached blob file and return the blob data offset.
    ///
    /// This is a convenience wrapper that returns both the Mmap and the
    /// correct starting offset for the blob data (after the version header).
    pub fn mmap_blob_offset() -> usize {
        VERSION_HEADER_SIZE
    }
}

/// Replace path-unsafe characters in table names with underscores.
fn sanitize_table_name(name: &str) -> String {
    name.replace(['/', '\\', '\0'], "_").replace("..", "_")
}

/// Write `data` to `path` atomically via a temp file + rename.
fn atomic_write(path: &Path, data: &[u8]) -> io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "path has no parent directory")
    })?;

    // Create a temp file in the same directory so rename is atomic (same FS).
    let temp_path = parent.join(format!(".tmp_{}", Uuid::new_v4()));

    let mut file = fs::File::create(&temp_path)?;
    file.write_all(data)?;
    file.sync_all()?;

    // Atomic rename
    fs::rename(&temp_path, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> (DiskIndexCache, TempDir) {
        let dir = TempDir::new().unwrap();
        let cache = DiskIndexCache::new(dir.path().to_path_buf()).unwrap();
        (cache, dir)
    }

    #[test]
    fn test_store_and_load_version() {
        let (cache, _dir) = setup();
        let project = Uuid::new_v4();

        // Initially no version
        assert_eq!(cache.load_version(project, "orders"), None);

        // Store version 1
        cache.store(project, "orders", b"fake blob v1", 1).unwrap();
        assert_eq!(cache.load_version(project, "orders"), Some(1));

        // Update to version 2
        cache.store(project, "orders", b"fake blob v2", 2).unwrap();
        assert_eq!(cache.load_version(project, "orders"), Some(2));
    }

    #[test]
    fn test_mmap_readback() {
        let (cache, _dir) = setup();
        let project = Uuid::new_v4();
        let data = b"hello mmap world";

        cache.store(project, "users", data, 1).unwrap();

        let mmap = cache.mmap(project, "users").unwrap();
        // The first 8 bytes are the version header; blob starts at offset 8
        assert_eq!(&mmap[VERSION_HEADER_SIZE..], data);
    }

    #[test]
    fn test_missing_file_mmap_returns_error() {
        let (cache, _dir) = setup();
        let project = Uuid::new_v4();

        let result = cache.mmap(project, "nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_file_version_returns_none() {
        let (cache, _dir) = setup();
        let project = Uuid::new_v4();

        assert_eq!(cache.load_version(project, "nonexistent"), None);
    }

    #[test]
    fn test_sanitize_table_name() {
        assert_eq!(sanitize_table_name("orders"), "orders");
        assert_eq!(sanitize_table_name("public/orders"), "public_orders");
        assert_eq!(sanitize_table_name("a\\b"), "a_b");
        assert_eq!(sanitize_table_name("../etc/passwd"), "__etc_passwd");
    }

    #[test]
    fn test_sanitize_table_name_dotdot() {
        // ".." sequences are replaced with underscores
        assert_eq!(sanitize_table_name("..foo"), "_foo");
        assert_eq!(sanitize_table_name("a..b"), "a_b");
    }

    #[test]
    fn test_multiple_tables_same_project() {
        let (cache, _dir) = setup();
        let project = Uuid::new_v4();

        cache.store(project, "orders", b"orders data", 1).unwrap();
        cache.store(project, "users", b"users data", 5).unwrap();

        assert_eq!(cache.load_version(project, "orders"), Some(1));
        assert_eq!(cache.load_version(project, "users"), Some(5));

        let mmap_orders = cache.mmap(project, "orders").unwrap();
        assert_eq!(&mmap_orders[VERSION_HEADER_SIZE..], b"orders data");

        let mmap_users = cache.mmap(project, "users").unwrap();
        assert_eq!(&mmap_users[VERSION_HEADER_SIZE..], b"users data");
    }

    #[test]
    fn test_multiple_projects() {
        let (cache, _dir) = setup();
        let p1 = Uuid::new_v4();
        let p2 = Uuid::new_v4();

        cache.store(p1, "t", b"p1 data", 10).unwrap();
        cache.store(p2, "t", b"p2 data", 20).unwrap();

        assert_eq!(cache.load_version(p1, "t"), Some(10));
        assert_eq!(cache.load_version(p2, "t"), Some(20));
    }

    #[test]
    fn test_version_embedded_in_file() {
        let (cache, _dir) = setup();
        let project = Uuid::new_v4();

        cache.store(project, "t", b"data", 42).unwrap();

        // Read the raw file and verify the version header
        let raw = fs::read(cache.local_path(project, "t")).unwrap();
        assert_eq!(raw.len(), VERSION_HEADER_SIZE + 4);
        let version = i64::from_le_bytes(raw[..VERSION_HEADER_SIZE].try_into().unwrap());
        assert_eq!(version, 42);
        assert_eq!(&raw[VERSION_HEADER_SIZE..], b"data");
    }
}
