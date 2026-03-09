//! AOF (Append-Only File) persistence for `FastHash`.
//!
//! Logs all commands to an append-only file for durability.
//! Supports configurable sync policies for performance vs durability tradeoffs.
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::time::{Duration, interval};

/// AOF sync policy determining when data is flushed to disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AofSync {
    /// Sync every write immediately (safest, slowest)
    Always,
    /// Sync approximately once per second (balanced)
    Everysec,
    /// Never sync automatically (fastest, risk of data loss)
    No,
}

/// Errors that can occur during AOF operations.
#[derive(Debug, Error)]
pub enum AofError {
    #[error("failed to open AOF file")]
    Open,

    #[error("failed to write to AOF: {0}")]
    Write(String),

    #[error("failed to sync AOF to disk")]
    Sync,
}

/// AOF writer that handles appending commands and periodic syncing.
pub struct AofWriter {
    file: Arc<Mutex<File>>,
    #[allow(dead_code)]
    sync_policy: AofSync,
    sync_task: tokio::task::JoinHandle<()>,
}

impl AofWriter {
    /// Creates a new AOF writer with the specified sync policy.
    pub fn new(path: &PathBuf, sync_policy: AofSync) -> Result<Self, AofError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|_| AofError::Open)?;

        let file = Arc::new(Mutex::new(file));
        let file_clone = file.clone();
        let sync_policy_copy = sync_policy;
        let sync_task = tokio::spawn(async move {
            if sync_policy_copy == AofSync::Everysec {
                let mut interval = interval(Duration::from_secs(1));
                loop {
                    interval.tick().await;
                    let file = file_clone.lock().await;
                    if let Err(e) = file.sync_all() {
                        tracing::warn!("failed to sync AOF: {}", e);
                    }
                }
            }
        });

        Ok(Self {
            file,
            sync_policy,
            sync_task,
        })
    }

    /// Appends a command to the AOF.
    #[allow(dead_code)]
    pub async fn append(&self, command: &[u8]) -> Result<(), AofError> {
        let mut file = self.file.lock().await;
        file.write_all(command)
            .map_err(|_| AofError::Write("write failed".into()))?;
        file.write_all(b"\r\n")
            .map_err(|_| AofError::Write("write failed".into()))?;

        if self.sync_policy == AofSync::Always {
            file.sync_all().map_err(|_| AofError::Sync)?;
            drop(file);
        }

        Ok(())
    }

    /// Appends a command synchronously (blocks until synced if policy is always).
    pub fn append_sync(&self, command: &[u8]) -> Result<(), AofError> {
        let mut file = self.file.blocking_lock();
        file.write_all(command)
            .map_err(|_| AofError::Write("write failed".into()))?;
        file.write_all(b"\r\n")
            .map_err(|_| AofError::Write("write failed".into()))?;
        file.sync_all().map_err(|_| AofError::Sync)?;

        drop(file);
        Ok(())
    }
}

impl Drop for AofWriter {
    fn drop(&mut self) {
        self.sync_task.abort();
    }
}

/// Replays commands from an AOF file onto a key-value store.
pub fn replay(
    store: &crate::store::db::KvStore,
    list_store: &crate::store::list::ListStore,
    aof_path: &PathBuf,
) -> Result<u64, std::io::Error> {
    if !aof_path.exists() {
        return Ok(0);
    }

    let content = std::fs::read(aof_path)?;
    let content_str = String::from_utf8_lossy(&content);
    let content = content_str.replace("\r\n", "\n");
    let lines: Vec<&str> = content.lines().collect();
    let mut commands_replayed = 0;
    let mut i = 0;

    while i < lines.len() {
        let line = lines[i].trim();
        if line.is_empty() {
            i += 1;
            continue;
        }

        if line.starts_with('*') {
            if let Some(cmd) = parse_resp_command(&lines, i) {
                execute_command(store, list_store, &cmd);
                commands_replayed += 1;
                i = cmd.end_index;
            } else {
                i += 1;
            }
        } else {
            i += 1;
        }
    }

    if commands_replayed > 0 {
        tracing::info!("Replayed {} commands from AOF", commands_replayed);
    }
    Ok(commands_replayed)
}

fn parse_resp_command(lines: &[&str], start: usize) -> Option<CommandParts> {
    if start >= lines.len() {
        return None;
    }

    let line = lines[start].trim();
    if !line.starts_with('*') {
        return None;
    }

    let count: usize = line[1..].parse().ok()?;
    if count < 1 {
        return None;
    }

    let mut args: Vec<String> = Vec::with_capacity(count);
    let mut current_index = start + 1;

    while current_index < lines.len() && lines[current_index].trim().is_empty() {
        current_index += 1;
    }

    for _ in 0..count {
        if current_index >= lines.len() {
            return None;
        }

        let bulk_header = lines[current_index].trim();
        if !bulk_header.starts_with('$') {
            return None;
        }

        current_index += 1;

        while current_index < lines.len() && lines[current_index].trim().is_empty() {
            current_index += 1;
        }

        if current_index >= lines.len() {
            return None;
        }

        let arg = lines[current_index].trim().to_string();
        args.push(arg);
        current_index += 1;
    }

    Some(CommandParts {
        args,
        end_index: current_index,
    })
}

struct CommandParts {
    args: Vec<String>,
    end_index: usize,
}

fn execute_command(
    store: &crate::store::db::KvStore,
    list_store: &crate::store::list::ListStore,
    cmd: &CommandParts,
) {
    let args: Vec<String> = cmd
        .args
        .iter()
        .map(|s| s.trim_end_matches('\r').to_string())
        .collect();
    if args.is_empty() {
        return;
    }

    let cmd_name = args[0].trim_end_matches('\r').to_uppercase();
    if cmd_name == "SET" && args.len() >= 3 {
        let key = args[1].trim_end_matches('\r').to_string();
        let value = args[2].trim_end_matches('\r').to_string();
        store.set(key, value, None);
    } else if cmd_name == "SET" && args.len() >= 6 {
        let third_arg = args[3].trim_end_matches('\r').to_uppercase();
        let key = args[1].trim_end_matches('\r').to_string();
        let value = args[2].trim_end_matches('\r').to_string();
        if third_arg == "EX" {
            let ttl_secs: Option<u64> = args[4].trim_end_matches('\r').parse().ok();
            store.set(key, value, ttl_secs);
        } else {
            store.set(key, value, None);
        }
    } else if cmd_name == "DEL" {
        let keys: Vec<String> = args[1..]
            .iter()
            .map(|s| s.trim_end_matches('\r').to_string())
            .collect();
        let _ = store.del(&keys);
    } else if cmd_name == "FLUSHALL" {
        store.flushall();
    } else if cmd_name == "SETEX" && args.len() >= 4 {
        let key = args[1].trim_end_matches('\r').to_string();
        let seconds: u64 = args[2].trim_end_matches('\r').parse().unwrap_or(0);
        let value = args[3].trim_end_matches('\r').to_string();
        store.set(key, value, Some(seconds));
    } else if cmd_name == "EXPIRE" && args.len() >= 3 {
        let key = args[1].trim_end_matches('\r').to_string();
        let seconds: u64 = args[2].trim_end_matches('\r').parse().unwrap_or(0);
        let _ = store.expire(&key, seconds);
    } else if cmd_name == "LPUSH" && args.len() >= 3 {
        let key = args[1].trim_end_matches('\r').to_string();
        let values: Vec<String> = args[2..]
            .iter()
            .map(|s| s.trim_end_matches('\r').to_string())
            .collect();
        let _ = list_store.lpush(&key, values);
    } else if cmd_name == "RPUSH" && args.len() >= 3 {
        let key = args[1].trim_end_matches('\r').to_string();
        let values: Vec<String> = args[2..]
            .iter()
            .map(|s| s.trim_end_matches('\r').to_string())
            .collect();
        let _ = list_store.rpush(&key, values);
    } else if cmd_name == "LTRIM" && args.len() >= 4 {
        let key = args[1].trim_end_matches('\r').to_string();
        let start: i64 = args[2].trim_end_matches('\r').parse().unwrap_or(0);
        let stop: i64 = args[3].trim_end_matches('\r').parse().unwrap_or(-1);
        let _ = list_store.ltrim(&key, start, stop);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_aof_write() {
        let temp_dir = TempDir::new().unwrap();
        let aof_path = temp_dir.path().join("test.aof");
        let aof = AofWriter::new(&aof_path, AofSync::No).unwrap();
        aof.append(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
            .await
            .unwrap();

        drop(aof);

        let content = fs::read_to_string(aof_path).unwrap();
        assert!(content.contains("SET"));
        assert!(content.contains("key"));
        assert!(content.contains("value"));
    }

    #[tokio::test]
    async fn test_aof_write_sync() {
        let temp_dir = TempDir::new().unwrap();
        let aof_path = temp_dir.path().join("test_sync.aof");

        let aof = AofWriter::new(&aof_path, AofSync::Always).unwrap();
        aof.append(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
            .await
            .unwrap();

        drop(aof);

        let content = fs::read_to_string(aof_path).unwrap();
        assert!(content.contains("SET"));
    }
}
