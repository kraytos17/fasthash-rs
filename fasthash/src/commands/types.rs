//! Command and response types for `FastHash`.

/// Commands supported by `FastHash`.
#[derive(Debug, Clone)]
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

    // ============ LIST COMMANDS ============
    /// LPUSH key value [value ...]
    Lpush { key: String, values: Vec<String> },
    /// RPUSH key value [value ...]
    Rpush { key: String, values: Vec<String> },
    /// LPOP key [count]
    Lpop { key: String, count: Option<u64> },
    /// RPOP key [count]
    Rpop { key: String, count: Option<u64> },
    /// LRANGE key start stop
    Lrange { key: String, start: i64, stop: i64 },
    /// LLEN key
    Llen { key: String },
    /// LINDEX key index
    Lindex { key: String, index: i64 },
    /// LSET key index value
    Lset {
        key: String,
        index: i64,
        value: String,
    },
    /// LTRIM key start stop
    Ltrim { key: String, start: i64, stop: i64 },
}

/// Responses from `FastHash` commands.
#[derive(Debug, Clone, Eq, PartialEq)]
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
