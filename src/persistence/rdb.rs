//! RDB persistence module for `FastHash`.
//!
//! Provides snapshot functionality to save and load the key-value store
//! to/from binary RDB format.

use crate::store::db::KvStore;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

/// RDB file magic header identifying this format.
const RDB_MAGIC: &[u8; 12] = b"FASTHASH_RDB";

/// RDB file format version.
const RDB_VERSION: &[u8; 4] = b"0002";

/// Saves the key-value store to an RDB file.
///
/// # Arguments
///
/// * `store` - The key-value store to save
/// * `path` - Path to the RDB file
///
/// # Errors
///
/// Returns an IO error if the file cannot be created or written.
#[allow(clippy::missing_errors_doc)]
pub fn save(store: &KvStore, path: &Path) -> io::Result<()> {
    let mut file = File::create(path)?;

    file.write_all(RDB_MAGIC)?;
    file.write_all(RDB_VERSION)?;
    for entry in &store.data {
        let key = entry.key();
        let value = entry.value();
        if value.is_expired() {
            continue;
        }

        let key_bytes = key.as_bytes();
        #[allow(clippy::cast_possible_truncation)]
        let key_len = [key_bytes.len() as u8];
        file.write_all(&key_len)?;

        let expires_at_secs = value.expires_at.map_or(0u64, |exp| {
            let remaining = exp.duration_since(std::time::Instant::now()).as_secs();
            let elapsed_unix = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or(std::time::Duration::ZERO)
                .as_secs();
            elapsed_unix + remaining
        });
        file.write_all(&expires_at_secs.to_le_bytes())?;

        file.write_all(key_bytes)?;

        let value_bytes = value.data.as_bytes();
        #[allow(clippy::cast_possible_truncation)]
        let value_len = [value_bytes.len() as u8];
        file.write_all(&value_len)?;
        file.write_all(value_bytes)?;
    }

    file.write_all(&[0])?;

    Ok(())
}

/// Loads the key-value store from an RDB file.
///
/// # Arguments
///
/// * `store` - The key-value store to load into (will be cleared first)
/// * `path` - Path to the RDB file
///
/// # Errors
///
/// Returns an IO error if the file cannot be opened or read.
#[allow(clippy::missing_errors_doc)]
pub fn load(store: &KvStore, path: &Path) -> io::Result<()> {
    if !path.exists() {
        return Ok(());
    }

    let mut file = OpenOptions::new().read(true).open(path)?;
    let mut magic = [0u8; 12];
    file.read_exact(&mut magic)?;
    if magic != *RDB_MAGIC {
        tracing::warn!("RDB file has invalid magic header");
        return Ok(());
    }

    let mut version = [0u8; 4];
    file.read_exact(&mut version)?;
    if version != *RDB_VERSION {
        tracing::warn!("RDB file has incompatible version");
        return Ok(());
    }

    loop {
        let mut key_len_buf = [0u8; 1];
        match file.read_exact(&mut key_len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }

        let key_len = key_len_buf[0] as usize;
        if key_len == 0 {
            break;
        }

        let mut ttl_buf = [0u8; 8];
        file.read_exact(&mut ttl_buf)?;
        let expires_at_secs = u64::from_le_bytes(ttl_buf);

        let mut key = vec![0u8; key_len];
        file.read_exact(&mut key)?;
        let key = String::from_utf8(key).unwrap_or_default();

        let mut value_len_buf = [0u8; 1];
        file.read_exact(&mut value_len_buf)?;
        let value_len = value_len_buf[0] as usize;

        let mut value = vec![0u8; value_len];
        file.read_exact(&mut value)?;
        let value = String::from_utf8(value).unwrap_or_default();

        let ttl_seconds = if expires_at_secs > 0 {
            let expires_at =
                std::time::UNIX_EPOCH + std::time::Duration::from_secs(expires_at_secs);
            let now = std::time::SystemTime::now();
            if expires_at > now {
                Some(
                    expires_at
                        .duration_since(now)
                        .unwrap_or(std::time::Duration::ZERO)
                        .as_secs(),
                )
            } else {
                continue;
            }
        } else {
            None
        };

        store.set(key, value, ttl_seconds);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;
    use tempfile::TempDir;

    #[test]
    fn test_save_load() {
        let temp_dir = TempDir::new().unwrap();
        let rdb_path = temp_dir.path().join("test.rdb");

        let store = KvStore::new();
        store.set("key1".into(), "value1".into(), None);
        store.set("key2".into(), "value2".into(), None);

        save(&store, &rdb_path).unwrap();
        assert!(rdb_path.exists());

        let new_store = KvStore::new();
        load(&new_store, &rdb_path).unwrap();

        assert_eq!(new_store.get("key1"), Some("value1".into()));
        assert_eq!(new_store.get("key2"), Some("value2".into()));
    }

    #[test]
    fn test_load_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let rdb_path = temp_dir.path().join("nonexistent.rdb");

        let store = KvStore::new();
        load(&store, &rdb_path).unwrap();

        assert_eq!(store.get("key1"), None);
    }

    #[test]
    fn test_save_empty_store() {
        let temp_dir = TempDir::new().unwrap();
        let rdb_path = temp_dir.path().join("empty.rdb");

        let store = KvStore::new();
        save(&store, &rdb_path).unwrap();
        assert!(rdb_path.exists());

        let new_store = KvStore::new();
        load(&new_store, &rdb_path).unwrap();

        assert_eq!(new_store.get("key1"), None);
    }

    #[test]
    fn test_save_load_with_ttl() {
        let temp_dir = TempDir::new().unwrap();
        let rdb_path = temp_dir.path().join("ttl.rdb");

        let store = KvStore::new();
        store.set("key1".into(), "value1".into(), Some(3600));
        store.set("key2".into(), "value2".into(), None);

        save(&store, &rdb_path).unwrap();

        let new_store = KvStore::new();
        load(&new_store, &rdb_path).unwrap();

        assert_eq!(new_store.get("key1"), Some("value1".into()));
        assert_eq!(new_store.get("key2"), Some("value2".into()));
    }

    #[test]
    fn test_expired_ttl_not_loaded() {
        let temp_dir = TempDir::new().unwrap();
        let rdb_path = temp_dir.path().join("expired.rdb");

        let store = KvStore::new();
        store.set("key1".into(), "value1".into(), Some(1));

        save(&store, &rdb_path).unwrap();

        thread::sleep(Duration::from_secs(2));

        let new_store = KvStore::new();
        load(&new_store, &rdb_path).unwrap();

        assert_eq!(new_store.get("key1"), None);
    }
}
