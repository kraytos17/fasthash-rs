//! Command handler for `FastHash`.
//!
//! Executes commands against the key-value store and returns responses.

use crate::commands::types::{Command, Response};
use crate::persistence::aof::AofWriter;
use crate::persistence::rdb;
use crate::store::db::KvStore;
use crate::store::list::ListStore;
use std::fmt::Write;
use std::path::PathBuf;
use std::sync::Arc;

/// Handles command execution for the key-value store.
pub struct CommandHandler {
    store: Arc<KvStore>,
    list_store: Arc<ListStore>,
    rdb_path: PathBuf,
    aof: Option<Arc<AofWriter>>,
}

impl CommandHandler {
    /// Creates a new handler with the given store.
    #[must_use]
    pub const fn new(
        store: Arc<KvStore>,
        list_store: Arc<ListStore>,
        rdb_path: PathBuf,
        aof: Option<Arc<AofWriter>>,
    ) -> Self {
        Self {
            store,
            list_store,
            rdb_path,
            aof,
        }
    }

    /// Appends a command to AOF if AOF is enabled.
    fn log_to_aof(&self, cmd: &[u8]) {
        if let Some(aof) = &self.aof {
            let aof_clone = aof.clone();
            let cmd_vec = cmd.to_vec();
            tokio::task::spawn_blocking(move || {
                if let Err(e) = aof_clone.append_sync(&cmd_vec) {
                    tracing::warn!("Failed to write to AOF: {}", e);
                }
            });
        }
    }

    /// Handles a command and returns the response.
    pub fn handle(&self, cmd: Command) -> Response {
        match cmd {
            Command::Ping => Response::Pong,
            Command::Set { key, value, ttl } => {
                let cmd_bytes = encode_set_command(&key, &value, ttl);
                self.log_to_aof(&cmd_bytes);
                self.store.set(key, value, ttl);
                Response::Ok
            }
            Command::Get { key } => self
                .store
                .get(&key)
                .map_or(Response::Null, Response::BulkString),
            Command::Del { keys } => {
                let cmd_bytes = encode_del_command(&keys);
                self.log_to_aof(&cmd_bytes);
                let count = self.store.del(&keys);
                Response::Integer(count)
            }
            Command::Exists { keys } => {
                let count = self.store.exists(&keys);
                Response::Integer(count)
            }
            Command::Keys { pattern } => {
                let keys = self.store.keys(&pattern);
                Response::Array(keys)
            }
            Command::FlushAll => {
                self.log_to_aof(b"*1\r\n$8\r\nFLUSHALL\r\n");
                self.store.flushall();
                Response::Ok
            }
            Command::Setex {
                key,
                seconds,
                value,
            } => {
                let cmd_bytes = encode_setex_command(&key, seconds, &value);
                self.log_to_aof(&cmd_bytes);
                self.store.set(key, value, Some(seconds));
                Response::Ok
            }
            Command::Expire { key, seconds } => {
                let cmd_bytes = encode_expire_command(&key, seconds);
                self.log_to_aof(&cmd_bytes);
                let result = self.store.expire(&key, seconds);
                Response::Integer(i64::from(result))
            }
            Command::Ttl { key } => {
                let ttl = self.store.ttl(&key);
                Response::Integer(ttl)
            }
            Command::Save { path } => {
                let save_path = path.map_or_else(|| self.rdb_path.clone(), PathBuf::from);
                self.log_to_aof(b"*1\r\n$4\r\nSAVE\r\n");
                match rdb::save(&self.store, &self.list_store, &save_path) {
                    Ok(()) => Response::Ok,
                    Err(e) => Response::Error(format!("ERR saving RDB: {e}")),
                }
            }
            Command::Load { path } => {
                let load_path = path.map_or_else(|| self.rdb_path.clone(), PathBuf::from);
                self.log_to_aof(b"*1\r\n$4\r\nLOAD\r\n");
                self.store.flushall();
                match rdb::load(&self.store, &self.list_store, &load_path) {
                    Ok(()) => Response::Ok,
                    Err(e) => Response::Error(format!("ERR loading RDB: {e}")),
                }
            }

            // ============ LIST COMMANDS ============
            Command::Lpush { key, values } => {
                let cmd_bytes = encode_lpush_command(&key, &values);
                self.log_to_aof(&cmd_bytes);
                let len = self.list_store.lpush(&key, values);
                Response::Integer(len as i64)
            }
            Command::Rpush { key, values } => {
                let cmd_bytes = encode_rpush_command(&key, &values);
                self.log_to_aof(&cmd_bytes);
                let len = self.list_store.rpush(&key, values);
                Response::Integer(len as i64)
            }
            Command::Lpop { key, count } => {
                let cmd_bytes = encode_lpop_command(&key, count);
                self.log_to_aof(&cmd_bytes);
                let (removed, _) = self.list_store.lpop(&key, count);
                if removed.is_empty() {
                    Response::Null
                } else if removed.len() == 1 {
                    Response::BulkString(removed[0].clone())
                } else {
                    Response::Array(removed)
                }
            }
            Command::Rpop { key, count } => {
                let cmd_bytes = encode_rpop_command(&key, count);
                self.log_to_aof(&cmd_bytes);
                let (removed, _) = self.list_store.rpop(&key, count);
                if removed.is_empty() {
                    Response::Null
                } else if removed.len() == 1 {
                    Response::BulkString(removed[0].clone())
                } else {
                    Response::Array(removed)
                }
            }
            Command::Lrange { key, start, stop } => {
                let elements = self.list_store.lrange(&key, start, stop);
                Response::Array(elements)
            }
            Command::Llen { key } => {
                let len = self.list_store.llen(&key);
                Response::Integer(len as i64)
            }
            Command::Lindex { key, index } => self
                .list_store
                .lindex(&key, index)
                .map_or(Response::Null, Response::BulkString),
            Command::Lset { key, index, value } => {
                let cmd_bytes = encode_lset_command(&key, index, &value);
                self.log_to_aof(&cmd_bytes);
                if self.list_store.lset(&key, index, value) {
                    Response::Ok
                } else {
                    Response::Error("ERR index out of range".to_string())
                }
            }
            Command::Ltrim { key, start, stop } => {
                let cmd_bytes = encode_ltrim_command(&key, start, stop);
                self.log_to_aof(&cmd_bytes);
                if self.list_store.ltrim(&key, start, stop) {
                    Response::Ok
                } else {
                    Response::Error("ERR error".to_string())
                }
            }
        }
    }
}

/// Encodes a SET command to RESP format for AOF logging.
fn encode_set_command(key: &str, value: &str, ttl: Option<u64>) -> Vec<u8> {
    let mut cmd = Vec::new();
    if let Some(ttl_secs) = ttl {
        cmd.extend_from_slice(
            format!(
                "*5\r\n$3\r\nSET\r\n${}\r\n{}\r\n$2\r\nEX\r\n{}\r\n",
                key.len(),
                key,
                ttl_secs
            )
            .as_bytes(),
        );
    } else {
        cmd.extend_from_slice(
            format!(
                "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                key.len(),
                key,
                value.len(),
                value
            )
            .as_bytes(),
        );
    }
    cmd
}

/// Encodes a DEL command to RESP format for AOF logging.
fn encode_del_command(keys: &[String]) -> Vec<u8> {
    let mut cmd = format!("*{}\r\n$3\r\nDEL\r\n", 1 + keys.len());
    for key in keys {
        let _ = write!(cmd, "${}\r\n{}\r\n", key.len(), key);
    }
    cmd.into_bytes()
}

/// Encodes a SETEX command to RESP format for AOF logging.
fn encode_setex_command(key: &str, seconds: u64, value: &str) -> Vec<u8> {
    format!(
        "*5\r\n$4\r\nSETEX\r\n${}\r\n{}\r\n{}\r\n${}\r\n{}\r\n",
        key.len(),
        key,
        seconds,
        value.len(),
        value
    )
    .into_bytes()
}

/// Encodes an EXPIRE command to RESP format for AOF logging.
fn encode_expire_command(key: &str, seconds: u64) -> Vec<u8> {
    format!(
        "*3\r\n$6\r\nEXPIRE\r\n${}\r\n{}\r\n{}\r\n",
        key.len(),
        key,
        seconds
    )
    .into_bytes()
}

fn encode_lpush_command(key: &str, values: &[String]) -> Vec<u8> {
    let mut cmd = format!(
        "*{}\r\n$5\r\nLPUSH\r\n${}\r\n{}\r\n",
        2 + values.len(),
        key.len(),
        key
    );
    for value in values {
        let _ = write!(cmd, "${}\r\n{}\r\n", value.len(), value);
    }
    cmd.into_bytes()
}

fn encode_rpush_command(key: &str, values: &[String]) -> Vec<u8> {
    let mut cmd = format!(
        "*{}\r\n$5\r\nRPUSH\r\n${}\r\n{}\r\n",
        2 + values.len(),
        key.len(),
        key
    );
    for value in values {
        let _ = write!(cmd, "${}\r\n{}\r\n", value.len(), value);
    }
    cmd.into_bytes()
}

fn encode_lpop_command(key: &str, count: Option<u64>) -> Vec<u8> {
    count.map_or_else(
        || format!("*2\r\n$4\r\nLPOP\r\n${}\r\n{}\r\n", key.len(), key).into_bytes(),
        |c| format!("*3\r\n$4\r\nLPOP\r\n${}\r\n{}\r\n{}\r\n", key.len(), key, c).into_bytes(),
    )
}

fn encode_rpop_command(key: &str, count: Option<u64>) -> Vec<u8> {
    count.map_or_else(
        || format!("*2\r\n$4\r\nRPOP\r\n${}\r\n{}\r\n", key.len(), key).into_bytes(),
        |c| format!("*3\r\n$4\r\nRPOP\r\n${}\r\n{}\r\n{}\r\n", key.len(), key, c).into_bytes(),
    )
}

fn encode_lset_command(key: &str, index: i64, value: &str) -> Vec<u8> {
    format!(
        "*4\r\n$4\r\nLSET\r\n${}\r\n{}\r\n{}\r\n${}\r\n{}\r\n",
        key.len(),
        key,
        index,
        value.len(),
        value
    )
    .into_bytes()
}

fn encode_ltrim_command(key: &str, start: i64, stop: i64) -> Vec<u8> {
    format!(
        "*4\r\n$5\r\nLTRIM\r\n${}\r\n{}\r\n{}\r\n{}\r\n",
        key.len(),
        key,
        start,
        stop
    )
    .into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::Arc;

    fn make_handler(store: Arc<KvStore>) -> CommandHandler {
        let list_store = Arc::new(ListStore::new());
        CommandHandler::new(store, list_store, PathBuf::from("test.rdb"), None)
    }

    #[tokio::test]
    async fn test_ping() {
        let store = Arc::new(KvStore::new());
        let handler = make_handler(store);
        let result = handler.handle(Command::Ping);
        assert_eq!(result, Response::Pong);
    }

    #[tokio::test]
    async fn test_set_get() {
        let store = Arc::new(KvStore::new());
        let handler = make_handler(store.clone());
        let set_cmd = Command::Set {
            key: "key1".into(),
            value: "value1".into(),
            ttl: None,
        };

        let result = handler.handle(set_cmd);
        assert_eq!(result, Response::Ok);

        let get_cmd = Command::Get { key: "key1".into() };
        let result = handler.handle(get_cmd);
        assert_eq!(result, Response::BulkString("value1".into()));
    }

    #[tokio::test]
    async fn test_get_nonexistent() {
        let store = Arc::new(KvStore::new());
        let handler = make_handler(store);
        let get_cmd = Command::Get {
            key: "nonexistent".into(),
        };

        let result = handler.handle(get_cmd);
        assert_eq!(result, Response::Null);
    }

    #[tokio::test]
    async fn test_del() {
        let store = Arc::new(KvStore::new());
        let handler = make_handler(store.clone());
        let set_cmd = Command::Set {
            key: "key1".into(),
            value: "value1".into(),
            ttl: None,
        };
        handler.handle(set_cmd);

        let del_cmd = Command::Del {
            keys: vec!["key1".into()],
        };

        let result = handler.handle(del_cmd);
        assert_eq!(result, Response::Integer(1));

        let get_cmd = Command::Get { key: "key1".into() };
        let result = handler.handle(get_cmd);
        assert_eq!(result, Response::Null);
    }

    #[tokio::test]
    async fn test_exists() {
        let store = Arc::new(KvStore::new());
        let handler = make_handler(store.clone());
        let set_cmd = Command::Set {
            key: "key1".into(),
            value: "value1".into(),
            ttl: None,
        };
        handler.handle(set_cmd);

        let exists_cmd = Command::Exists {
            keys: vec!["key1".into(), "key2".into()],
        };

        let result = handler.handle(exists_cmd);
        assert_eq!(result, Response::Integer(1));
    }

    #[tokio::test]
    async fn test_keys() {
        let store = Arc::new(KvStore::new());
        let handler = make_handler(store.clone());

        handler.handle(Command::Set {
            key: "key1".into(),
            value: "value1".into(),
            ttl: None,
        });
        handler.handle(Command::Set {
            key: "key2".into(),
            value: "value2".into(),
            ttl: None,
        });

        let keys_cmd = Command::Keys {
            pattern: "*".into(),
        };

        let result = handler.handle(keys_cmd);
        match result {
            Response::Array(keys) => {
                assert_eq!(keys.len(), 2);
                assert!(keys.contains(&"key1".into()));
                assert!(keys.contains(&"key2".into()));
            }
            _ => panic!("Expected array response"),
        }
    }

    #[tokio::test]
    async fn test_flushall() {
        let store = Arc::new(KvStore::new());
        let handler = make_handler(store.clone());

        handler.handle(Command::Set {
            key: "key1".into(),
            value: "value1".into(),
            ttl: None,
        });

        let flush_cmd = Command::FlushAll;
        let result = handler.handle(flush_cmd);
        assert_eq!(result, Response::Ok);

        let get_cmd = Command::Get { key: "key1".into() };
        let result = handler.handle(get_cmd);
        assert_eq!(result, Response::Null);
    }

    #[tokio::test]
    async fn test_ttl_no_expiry() {
        let store = Arc::new(KvStore::new());
        let handler = make_handler(store.clone());

        handler.handle(Command::Set {
            key: "key1".into(),
            value: "value1".into(),
            ttl: None,
        });

        let ttl_cmd = Command::Ttl { key: "key1".into() };
        let result = handler.handle(ttl_cmd);
        assert_eq!(result, Response::Integer(-1));
    }

    #[tokio::test]
    async fn test_ttl_with_expiry() {
        let store = Arc::new(KvStore::new());
        let handler = make_handler(store.clone());

        handler.handle(Command::Setex {
            key: "key1".into(),
            seconds: 60,
            value: "value1".into(),
        });

        let ttl_cmd = Command::Ttl { key: "key1".into() };
        let result = handler.handle(ttl_cmd);
        match result {
            Response::Integer(ttl) => {
                assert!(ttl > 0);
                assert!(ttl <= 60);
            }
            _ => panic!("Expected integer response"),
        }
    }

    #[tokio::test]
    async fn test_save_load() {
        let temp_dir = tempfile::tempdir().unwrap();
        let rdb_path = temp_dir.path().join("test.rdb");

        let store = Arc::new(KvStore::new());
        let list_store = Arc::new(ListStore::new());
        let handler =
            CommandHandler::new(store.clone(), list_store.clone(), rdb_path.clone(), None);

        handler.handle(Command::Set {
            key: "key1".into(),
            value: "value1".into(),
            ttl: None,
        });
        handler.handle(Command::Set {
            key: "key2".into(),
            value: "value2".into(),
            ttl: Some(3600),
        });

        let save_cmd = Command::Save {
            path: Some(rdb_path.to_string_lossy().into_owned()),
        };
        let result = handler.handle(save_cmd);
        assert_eq!(result, Response::Ok);

        let load_store = Arc::new(KvStore::new());
        let load_list_store = Arc::new(ListStore::new());
        let load_handler = CommandHandler::new(
            load_store.clone(),
            load_list_store.clone(),
            rdb_path.clone(),
            None,
        );

        let load_cmd = Command::Load {
            path: Some(rdb_path.to_string_lossy().into_owned()),
        };
        let result = load_handler.handle(load_cmd);
        assert_eq!(result, Response::Ok);

        assert_eq!(load_store.get("key1"), Some("value1".into()));
        assert_eq!(load_store.get("key2"), Some("value2".into()));
    }
}
