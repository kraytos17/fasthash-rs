//! Command and response types for `FastHash`.

use serde::{Deserialize, Serialize};

/// Commands supported by `FastHash`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    /// PING command
    Ping,
    /// SET key value [EX seconds]
    Set {
        key: String,
        value: String,
        ttl: Option<u64>,
    },
    /// GET key
    Get { key: String },
    /// DEL key [key ...]
    Del { keys: Vec<String> },
    /// EXISTS key [key ...]
    Exists { keys: Vec<String> },
    /// KEYS pattern
    Keys { pattern: String },
    /// FLUSHALL
    FlushAll,
    /// SETEX key seconds value
    Setex {
        key: String,
        seconds: u64,
        value: String,
    },
    /// EXPIRE key seconds
    Expire { key: String, seconds: u64 },
    /// TTL key
    Ttl { key: String },
    /// SAVE - Snapshot the database to disk
    Save { path: Option<String> },
    /// LOAD - Load database from disk
    Load { path: Option<String> },
}

/// Responses from `FastHash` commands.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum Response {
    /// OK response
    Ok,
    /// PONG response
    Pong,
    /// Bulk string response
    BulkString(String),
    /// Integer response
    Integer(i64),
    /// Array response
    Array(Vec<String>),
    /// Error response
    Error(String),
    /// Null response
    Null,
}
