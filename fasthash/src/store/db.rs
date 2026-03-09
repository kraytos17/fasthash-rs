//! In-memory key-value store with TTL support.
//!
//! Uses `DashMap` for thread-safe concurrent access.

use dashmap::DashMap;
use std::time::{Duration, Instant};

/// A value stored in the database with optional expiration.
#[derive(Debug, Clone)]
pub struct Value {
    /// The actual string data.
    pub data: String,
    /// Optional expiration time.
    pub expires_at: Option<Instant>,
}

impl Value {
    /// Creates a new value with optional TTL.
    #[must_use]
    pub fn new(data: String, ttl_seconds: Option<u64>) -> Self {
        let expires_at = ttl_seconds.map(|secs| Instant::now() + Duration::from_secs(secs));
        Self { data, expires_at }
    }

    /// Returns `true` if this value has expired.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        self.expires_at.is_some_and(|exp| Instant::now() > exp)
    }
}

/// Thread-safe in-memory key-value store.
#[derive(Debug, Default)]
pub struct KvStore {
    pub(crate) data: DashMap<String, Value>,
}

impl KvStore {
    /// Creates a new empty key-value store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            data: DashMap::new(),
        }
    }

    /// Sets a key-value pair with optional TTL.
    pub fn set(&self, key: String, value: String, ttl: Option<u64>) {
        let val = Value::new(value, ttl);
        self.data.insert(key, val);
    }

    /// Gets a value by key, returning `None` if not found or expired.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<String> {
        self.data.get(key).and_then(|v| {
            if v.is_expired() {
                self.data.remove(key);
                None
            } else {
                Some(v.data.clone())
            }
        })
    }

    /// Deletes the specified keys and returns the count of deleted keys.
    #[must_use]
    pub fn del(&self, keys: &[String]) -> i64 {
        keys.iter()
            .filter(|key| self.data.remove(key.as_str()).is_some())
            .count() as i64
    }

    /// Checks which of the specified keys exist and haven't expired.
    #[must_use]
    pub fn exists(&self, keys: &[String]) -> i64 {
        keys.iter()
            .filter(|key| {
                self.data.get(key.as_str()).is_some_and(|v| {
                    if v.is_expired() {
                        self.data.remove(key.as_str());
                        false
                    } else {
                        true
                    }
                })
            })
            .count() as i64
    }

    /// Returns all keys matching the pattern (supports "*" for all).
    #[must_use]
    pub fn keys(&self, pattern: &str) -> Vec<String> {
        if pattern == "*" {
            self.data
                .iter()
                .filter_map(|entry| {
                    if entry.value().is_expired() {
                        self.data.remove(entry.key().as_str());
                        None
                    } else {
                        Some(entry.key().clone())
                    }
                })
                .collect()
        } else {
            // MVP: only supports "*" pattern
            Vec::new()
        }
    }

    /// Removes all entries from the store.
    pub fn flushall(&self) {
        self.data.clear();
    }

    /// Sets an expiration on a key. Returns `true` if the key existed.
    #[must_use]
    pub fn expire(&self, key: &str, seconds: u64) -> bool {
        if let Some(mut v) = self.data.get_mut(key) {
            v.expires_at = Some(Instant::now() + Duration::from_secs(seconds));
            true
        } else {
            false
        }
    }

    /// Returns the TTL of a key in seconds, or -1 if no TTL, -2 if not found.
    #[must_use]
    pub fn ttl(&self, key: &str) -> i64 {
        if let Some(v) = self.data.get(key) {
            if v.is_expired() {
                self.data.remove(key);
                -2
            } else if let Some(exp) = v.expires_at {
                exp.saturating_duration_since(Instant::now())
                    .as_secs()
                    .try_into()
                    .unwrap_or(-1)
            } else {
                -1
            }
        } else {
            -2
        }
    }

    /// Removes all expired entries from the store.
    #[allow(dead_code)]
    pub fn cleanup_expired(&self) {
        let expired_keys: Vec<_> = self
            .data
            .iter()
            .filter(|entry| entry.value().is_expired())
            .map(|entry| entry.key().clone())
            .collect();

        for key in expired_keys {
            self.data.remove(key.as_str());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_get() {
        let store = KvStore::new();
        store.set("key1".into(), "value1".into(), None);
        assert_eq!(store.get("key1"), Some("value1".into()));
    }

    #[test]
    fn test_get_nonexistent() {
        let store = KvStore::new();
        assert_eq!(store.get("nonexistent"), None);
    }

    #[test]
    fn test_del() {
        let store = KvStore::new();
        store.set("key1".into(), "value1".into(), None);
        assert_eq!(store.del(&["key1".into()]), 1);
        assert_eq!(store.get("key1"), None);
    }

    #[test]
    fn test_exists() {
        let store = KvStore::new();
        store.set("key1".into(), "value1".into(), None);
        assert_eq!(store.exists(&["key1".into(), "key2".into()]), 1);
    }

    #[test]
    fn test_keys() {
        let store = KvStore::new();
        store.set("key1".into(), "value1".into(), None);
        store.set("key2".into(), "value2".into(), None);
        let keys = store.keys("*");
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"key1".into()));
        assert!(keys.contains(&"key2".into()));
    }

    #[test]
    fn test_flushall() {
        let store = KvStore::new();
        store.set("key1".into(), "value1".into(), None);
        store.flushall();
        assert_eq!(store.get("key1"), None);
    }
}
