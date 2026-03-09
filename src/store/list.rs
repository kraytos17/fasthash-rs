use dashmap::DashMap;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct ListValue {
    pub elements: Vec<String>,
    pub expires_at: Option<Instant>,
}

impl ListValue {
    #[must_use]
    pub fn new(elements: Vec<String>, ttl_seconds: Option<u64>) -> Self {
        let expires_at = ttl_seconds.map(|secs| Instant::now() + Duration::from_secs(secs));
        Self {
            elements,
            expires_at,
        }
    }

    #[must_use]
    pub fn is_expired(&self) -> bool {
        self.expires_at.is_some_and(|exp| Instant::now() > exp)
    }

    #[must_use]
    pub fn ttl_remaining(&self) -> Option<u64> {
        self.expires_at
            .map(|exp| exp.saturating_duration_since(Instant::now()).as_secs())
    }
}

#[derive(Debug, Default)]
pub struct ListStore {
    pub(crate) data: DashMap<String, ListValue>,
}

impl ListStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            data: DashMap::new(),
        }
    }

    #[must_use]
    pub fn lpush(&self, key: &str, values: Vec<String>) -> usize {
        let mut list = match self.data.get_mut(key) {
            Some(mut entry) => {
                if entry.is_expired() {
                    self.data.remove(key);
                    Vec::new()
                } else {
                    std::mem::take(&mut entry.elements)
                }
            }
            None => Vec::new(),
        };

        for mut value in values {
            list.insert(0, value);
        }

        let len = list.len();
        self.data
            .insert(key.to_string(), ListValue::new(list, None));
        len
    }

    #[must_use]
    pub fn rpush(&self, key: &str, values: Vec<String>) -> usize {
        let mut list = match self.data.get_mut(key) {
            Some(mut entry) => {
                if entry.is_expired() {
                    self.data.remove(key);
                    Vec::new()
                } else {
                    std::mem::take(&mut entry.elements)
                }
            }
            None => Vec::new(),
        };

        list.extend(values);
        let len = list.len();
        self.data
            .insert(key.to_string(), ListValue::new(list, None));
        len
    }

    #[must_use]
    pub fn lpop(&self, key: &str, count: Option<u64>) -> (Vec<String>, usize) {
        let Some(mut entry) = self.data.get_mut(key) else {
            return (Vec::new(), 0);
        };

        if entry.is_expired() {
            self.data.remove(key);
            return (Vec::new(), 0);
        }

        let count = count
            .unwrap_or(1)
            .min(entry.elements.len().try_into().unwrap()) as usize;

        let removed: Vec<String> = entry.elements.drain(..count).collect();
        let remaining = entry.elements.len();
        if remaining == 0 {
            self.data.remove(key);
        }

        (removed, remaining)
    }

    pub fn rpop(&self, key: &str, count: Option<u64>) -> (Vec<String>, usize) {
        let Some(mut entry) = self.data.get_mut(key) else {
            return (Vec::new(), 0);
        };

        if entry.is_expired() {
            self.data.remove(key);
            return (Vec::new(), 0);
        }

        let count = count
            .unwrap_or(1)
            .min(entry.elements.len().try_into().unwrap()) as usize;

        let drain_start = entry.elements.len() - count;
        let removed: Vec<String> = entry.elements.drain(drain_start..).collect();
        let remaining = entry.elements.len();
        if remaining == 0 {
            self.data.remove(key);
        }

        (removed, remaining)
    }

    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Vec<String> {
        let Some(entry) = self.data.get(key) else {
            return Vec::new();
        };

        if entry.is_expired() {
            self.data.remove(key);
            return Vec::new();
        }

        let len = entry.elements.len() as i64;
        if len == 0 {
            return Vec::new();
        }

        let start = if start < 0 { len + start } else { start };
        let stop = if stop < 0 { len + stop } else { stop };

        let start = start.max(0).min(len) as usize;
        let stop = stop.max(0).min(len - 1) as usize;
        if start > stop {
            return Vec::new();
        }

        entry.elements[start..=stop].to_vec()
    }

    pub fn llen(&self, key: &str) -> usize {
        let Some(entry) = self.data.get(key) else {
            return 0;
        };

        if entry.is_expired() {
            self.data.remove(key);
            0
        } else {
            entry.elements.len()
        }
    }

    pub fn lindex(&self, key: &str, index: i64) -> Option<String> {
        let entry = self.data.get(key)?;
        if entry.is_expired() {
            self.data.remove(key);
            return None;
        }

        let len = entry.elements.len() as i64;
        let idx = if index < 0 { len + index } else { index };
        if idx < 0 || idx >= len {
            None
        } else {
            Some(entry.elements[idx as usize].clone())
        }
    }

    pub fn lset(&self, key: &str, index: i64, value: String) -> bool {
        let Some(mut entry) = self.data.get_mut(key) else {
            return false;
        };

        if entry.is_expired() {
            self.data.remove(key);
            return false;
        }

        let len = entry.elements.len() as i64;
        let idx = if index < 0 { len + index } else { index };
        if idx < 0 || idx >= len {
            false
        } else {
            entry.elements[idx as usize] = value;
            true
        }
    }

    pub fn ltrim(&self, key: &str, start: i64, stop: i64) -> bool {
        let Some(mut entry) = self.data.get_mut(key) else {
            return false;
        };

        if entry.is_expired() {
            self.data.remove(key);
            return false;
        }

        let len = entry.elements.len() as i64;
        let start = if start < 0 { len + start } else { start };
        let stop = if stop < 0 { len + stop } else { stop };
        if start > len || stop < 0 || start > stop {
            self.data.remove(key);
            return true;
        }

        let start = start.max(0) as usize;
        let stop = (stop.min(len - 1)) as usize;
        entry.elements = entry.elements[start..=stop].to_vec();
        true
    }

    pub fn exists(&self, key: &str) -> bool {
        let Some(entry) = self.data.get(key) else {
            return false;
        };

        if entry.is_expired() {
            self.data.remove(key);
            false
        } else {
            true
        }
    }

    pub fn del(&self, key: &str) -> bool {
        self.data.remove(key).is_some()
    }

    pub fn expire(&self, key: &str, seconds: u64) -> bool {
        let Some(mut entry) = self.data.get_mut(key) else {
            return false;
        };

        if entry.is_expired() {
            self.data.remove(key);
            return false;
        }

        entry.expires_at = Some(Instant::now() + Duration::from_secs(seconds));
        true
    }

    pub fn ttl(&self, key: &str) -> i64 {
        let Some(entry) = self.data.get(key) else {
            return -2;
        };

        if entry.is_expired() {
            self.data.remove(key);
            -2
        } else if let Some(exp) = entry.expires_at {
            exp.saturating_duration_since(Instant::now())
                .as_secs()
                .try_into()
                .unwrap_or(-1)
        } else {
            -1
        }
    }

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
    fn test_lpush_single() {
        let store = ListStore::new();
        let len = store.lpush("mylist", vec!["one".into()]);
        assert_eq!(len, 1);
        assert_eq!(store.lrange("mylist", 0, -1), vec!["one"]);
    }

    #[test]
    fn test_lpush_multiple() {
        let store = ListStore::new();
        let len = store.lpush("mylist", vec!["three".into(), "two".into(), "one".into()]);
        assert_eq!(len, 3);
        assert_eq!(store.lrange("mylist", 0, -1), vec!["one", "two", "three"]);
    }

    #[test]
    fn test_rpush() {
        let store = ListStore::new();
        store.rpush("mylist", vec!["one".into(), "two".into()]);
        assert_eq!(store.lrange("mylist", 0, -1), vec!["one", "two"]);
    }

    #[test]
    fn test_lrange_negative_indices() {
        let store = ListStore::new();
        store.rpush("mylist", vec!["one".into(), "two".into(), "three".into()]);
        assert_eq!(store.lrange("mylist", -2, -1), vec!["two", "three"]);
    }

    #[test]
    fn test_lpop() {
        let store = ListStore::new();
        store.rpush("mylist", vec!["one".into(), "two".into(), "three".into()]);
        let (removed, _) = store.lpop("mylist", None);
        assert_eq!(removed, vec!["one"]);
        assert_eq!(store.llen("mylist"), 2);
    }

    #[test]
    fn test_lpop_count() {
        let store = ListStore::new();
        store.rpush("mylist", vec!["one".into(), "two".into(), "three".into()]);
        let (removed, _) = store.lpop("mylist", Some(2));
        assert_eq!(removed, vec!["one", "two"]);
    }

    #[test]
    fn test_rpop() {
        let store = ListStore::new();
        store.rpush("mylist", vec!["one".into(), "two".into(), "three".into()]);
        let (removed, _) = store.rpop("mylist", None);
        assert_eq!(removed, vec!["three"]);
    }

    #[test]
    fn test_lindex() {
        let store = ListStore::new();
        store.rpush("mylist", vec!["one".into(), "two".into(), "three".into()]);
        assert_eq!(store.lindex("mylist", 0), Some("one".into()));
        assert_eq!(store.lindex("mylist", -1), Some("three".into()));
        assert_eq!(store.lindex("mylist", 5), None);
    }

    #[test]
    fn test_lset() {
        let store = ListStore::new();
        store.rpush("mylist", vec!["one".into(), "two".into()]);
        assert!(store.lset("mylist", 0, "ONE".into()));
        assert_eq!(store.lindex("mylist", 0), Some("ONE".into()));
        assert!(!store.lset("mylist", 10, "invalid".into()));
    }

    #[test]
    fn test_ltrim() {
        let store = ListStore::new();
        store.rpush(
            "mylist",
            vec!["one".into(), "two".into(), "three".into(), "four".into()],
        );
        assert!(store.ltrim("mylist", 1, 2));
        assert_eq!(store.lrange("mylist", 0, -1), vec!["two", "three"]);
    }

    #[test]
    fn test_llen() {
        let store = ListStore::new();
        assert_eq!(store.llen("nonexistent"), 0);
        store.rpush("mylist", vec!["one".into(), "two".into()]);
        assert_eq!(store.llen("mylist"), 2);
    }

    #[test]
    fn test_exists() {
        let store = ListStore::new();
        assert!(!store.exists("mylist"));
        store.lpush("mylist", vec!["value".into()]);
        assert!(store.exists("mylist"));
    }

    #[test]
    fn test_del() {
        let store = ListStore::new();
        store.lpush("mylist", vec!["value".into()]);
        assert!(store.del("mylist"));
        assert!(!store.exists("mylist"));
    }

    #[test]
    fn test_lrange_empty() {
        let store = ListStore::new();
        assert_eq!(store.lrange("nonexistent", 0, -1), Vec::<String>::new());
    }

    #[test]
    fn test_lrange_start_greater_than_stop() {
        let store = ListStore::new();
        store.rpush("mylist", vec!["one".into(), "two".into()]);
        assert_eq!(store.lrange("mylist", 2, 1), Vec::<String>::new());
    }
}
