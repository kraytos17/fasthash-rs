//! RESP protocol parser for `FastHash`.
//!
//! Implements parsing of Redis RESP2 protocol into command structures.

use crate::commands::types::Command;
use bytes::Bytes;
use std::io;
use thiserror::Error;

/// RESP protocol value types.
#[derive(Debug, Clone)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Bytes),
    Array(Vec<Self>),
    Null,
}

impl RespValue {
    /// Converts a RESP array to a command, if valid.
    pub fn to_command(&self) -> Option<Command> {
        match self {
            Self::Array(arr) => Self::array_to_command(arr),
            _ => None,
        }
    }

    fn array_to_command(arr: &[Self]) -> Option<Command> {
        if arr.is_empty() {
            return None;
        }

        let cmd_name = match arr[0] {
            Self::BulkString(ref bytes) => bytes.clone(),
            Self::SimpleString(ref s) => Bytes::from(s.clone()),
            _ => return None,
        };

        let cmd_upper = String::from_utf8_lossy(&cmd_name).to_uppercase();
        let args: Vec<String> = arr[1..]
            .iter()
            .filter_map(|v| match v {
                Self::BulkString(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
                Self::SimpleString(s) => Some(s.clone()),
                Self::Integer(n) => Some(n.to_string()),
                Self::Error(e) => Some(e.clone()),
                _ => None,
            })
            .collect();

        Some(match cmd_upper.as_str() {
            "PING" => Command::Ping,
            "SET" => Self::parse_set(&args)?,
            "GET" => Self::parse_get(&args)?,
            "DEL" => Self::parse_del(&args),
            "EXISTS" => Self::parse_exists(&args),
            "KEYS" => Self::parse_keys(&args),
            "FLUSHALL" => Command::FlushAll,
            "SETEX" => Self::parse_setex(&args)?,
            "EXPIRE" => Self::parse_expire(&args)?,
            "TTL" => Self::parse_ttl(&args)?,
            "SAVE" => Self::parse_save(&args)?,
            "LOAD" => Self::parse_load(&args)?,
            // List commands
            "LPUSH" => Self::parse_lpush(&args)?,
            "RPUSH" => Self::parse_rpush(&args)?,
            "LPOP" => Self::parse_lpop(&args)?,
            "RPOP" => Self::parse_rpop(&args)?,
            "LRANGE" => Self::parse_lrange(&args)?,
            "LLEN" => Self::parse_llen(&args)?,
            "LINDEX" => Self::parse_lindex(&args)?,
            "LSET" => Self::parse_lset(&args)?,
            "LTRIM" => Self::parse_ltrim(&args)?,
            _ => return None,
        })
    }

    fn parse_set(args: &[String]) -> Option<Command> {
        if args.len() < 2 {
            return None;
        }

        let ttl: Option<u64> =
            if args.len() > 2 && args[2].to_uppercase() == "EX" && args.len() >= 4 {
                args[3].parse().ok()
            } else {
                None
            };
        let value_idx = if args.len() > 2 && args[2].to_uppercase() == "EX" && args.len() >= 4 {
            4
        } else {
            1
        };
        if value_idx >= args.len() {
            return None;
        }

        Some(Command::Set {
            key: args[0].clone(),
            value: args[value_idx].clone(),
            ttl,
        })
    }

    fn parse_get(args: &[String]) -> Option<Command> {
        if args.len() != 1 {
            return None;
        }
        Some(Command::Get {
            key: args[0].clone(),
        })
    }

    fn parse_del(args: &[String]) -> Command {
        Command::Del {
            keys: args.to_vec(),
        }
    }

    fn parse_exists(args: &[String]) -> Command {
        Command::Exists {
            keys: args.to_vec(),
        }
    }

    fn parse_keys(args: &[String]) -> Command {
        let pattern = args.first().cloned().unwrap_or_else(|| "*".to_string());
        Command::Keys { pattern }
    }

    fn parse_setex(args: &[String]) -> Option<Command> {
        if args.len() != 3 {
            return None;
        }

        let seconds: u64 = args[1].parse().ok()?;
        Some(Command::Setex {
            key: args[0].clone(),
            seconds,
            value: args[2].clone(),
        })
    }

    fn parse_expire(args: &[String]) -> Option<Command> {
        if args.len() != 2 {
            return None;
        }

        let seconds: u64 = args[1].parse().ok()?;
        Some(Command::Expire {
            key: args[0].clone(),
            seconds,
        })
    }

    fn parse_ttl(args: &[String]) -> Option<Command> {
        if args.len() != 1 {
            return None;
        }
        Some(Command::Ttl {
            key: args[0].clone(),
        })
    }

    fn parse_save(args: &[String]) -> Option<Command> {
        if args.len() > 1 {
            return None;
        }
        let path = args.first().cloned();
        Some(Command::Save { path })
    }

    fn parse_load(args: &[String]) -> Option<Command> {
        if args.len() > 1 {
            return None;
        }
        let path = args.first().cloned();
        Some(Command::Load { path })
    }

    // ============ LIST PARSERS ============
    fn parse_lpush(args: &[String]) -> Option<Command> {
        if args.len() < 2 {
            return None;
        }
        Some(Command::Lpush {
            key: args[0].clone(),
            values: args[1..].to_vec(),
        })
    }

    fn parse_rpush(args: &[String]) -> Option<Command> {
        if args.len() < 2 {
            return None;
        }
        Some(Command::Rpush {
            key: args[0].clone(),
            values: args[1..].to_vec(),
        })
    }

    fn parse_lpop(args: &[String]) -> Option<Command> {
        if args.is_empty() {
            return None;
        }
        let count = if args.len() > 1 {
            args[1].parse().ok()
        } else {
            None
        };
        Some(Command::Lpop {
            key: args[0].clone(),
            count,
        })
    }

    fn parse_rpop(args: &[String]) -> Option<Command> {
        if args.is_empty() {
            return None;
        }
        let count = if args.len() > 1 {
            args[1].parse().ok()
        } else {
            None
        };
        Some(Command::Rpop {
            key: args[0].clone(),
            count,
        })
    }

    fn parse_lrange(args: &[String]) -> Option<Command> {
        if args.len() < 3 {
            return None;
        }
        let start: i64 = args[1].parse().ok()?;
        let stop: i64 = args[2].parse().ok()?;
        Some(Command::Lrange {
            key: args[0].clone(),
            start,
            stop,
        })
    }

    fn parse_llen(args: &[String]) -> Option<Command> {
        if args.len() != 1 {
            return None;
        }
        Some(Command::Llen {
            key: args[0].clone(),
        })
    }

    fn parse_lindex(args: &[String]) -> Option<Command> {
        if args.len() != 2 {
            return None;
        }
        let index: i64 = args[1].parse().ok()?;
        Some(Command::Lindex {
            key: args[0].clone(),
            index,
        })
    }

    fn parse_lset(args: &[String]) -> Option<Command> {
        if args.len() != 3 {
            return None;
        }
        let index: i64 = args[1].parse().ok()?;
        Some(Command::Lset {
            key: args[0].clone(),
            index,
            value: args[2].clone(),
        })
    }

    fn parse_ltrim(args: &[String]) -> Option<Command> {
        if args.len() < 3 {
            return None;
        }
        let start: i64 = args[1].parse().ok()?;
        let stop: i64 = args[2].parse().ok()?;
        Some(Command::Ltrim {
            key: args[0].clone(),
            start,
            stop,
        })
    }
}

/// RESP protocol parsing errors.
#[derive(Debug, Error)]
pub enum RespError {
    #[error("incomplete data")]
    Incomplete,

    #[error("invalid protocol")]
    InvalidProtocol,

    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

/// Parser for RESP protocol.
#[derive(Debug, Default)]
pub struct RespParser {
    buffer: Vec<u8>,
}

impl RespParser {
    /// Creates a new parser.
    #[must_use]
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(1024),
        }
    }

    /// Parses data into a RESP value.
    ///
    /// # Errors
    ///
    /// Returns `RespError::Incomplete` if not enough data is available,
    /// or `RespError::InvalidProtocol` if the data is malformed.
    pub fn parse(&mut self, data: &[u8]) -> Result<RespValue, RespError> {
        self.buffer.extend_from_slice(data);
        self.parse_internal()
    }

    fn parse_internal(&mut self) -> Result<RespValue, RespError> {
        if self.buffer.is_empty() {
            return Err(RespError::Incomplete);
        }

        match self.buffer[0] as char {
            '+' => self.parse_simple_string(),
            '-' => self.parse_error(),
            ':' => self.parse_integer(),
            '$' => self.parse_bulk_string(),
            '*' => self.parse_array(),
            _ => Err(RespError::InvalidProtocol),
        }
    }

    fn parse_simple_string(&mut self) -> Result<RespValue, RespError> {
        let end = self.find_crlf(1)?;
        let content = String::from_utf8_lossy(&self.buffer[1..end]).into_owned();
        self.consume(end + 2)?;
        Ok(RespValue::SimpleString(content))
    }

    fn parse_error(&mut self) -> Result<RespValue, RespError> {
        let end = self.find_crlf(1)?;
        let content = String::from_utf8_lossy(&self.buffer[1..end]).into_owned();
        self.consume(end + 2)?;
        Ok(RespValue::Error(content))
    }

    fn parse_integer(&mut self) -> Result<RespValue, RespError> {
        let end = self.find_crlf(1)?;
        let content = String::from_utf8_lossy(&self.buffer[1..end]);
        let num: i64 = content.parse().map_err(|_| RespError::InvalidProtocol)?;
        self.consume(end + 2)?;
        Ok(RespValue::Integer(num))
    }

    fn parse_bulk_string(&mut self) -> Result<RespValue, RespError> {
        let line_end = self.find_crlf(1)?;
        let len_str = String::from_utf8_lossy(&self.buffer[1..line_end]);
        let len: i64 = len_str.parse().map_err(|_| RespError::InvalidProtocol)?;
        if len < 0 {
            self.consume(line_end + 2)?;
            return Ok(RespValue::Null);
        }

        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let total_len = line_end + 2 + len as usize + 2;
        if self.buffer.len() < total_len {
            return Err(RespError::Incomplete);
        }

        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let content = Bytes::from(self.buffer[line_end + 2..line_end + 2 + len as usize].to_vec());
        self.consume(total_len)?;
        Ok(RespValue::BulkString(content))
    }

    fn parse_array(&mut self) -> Result<RespValue, RespError> {
        let line_end = self.find_crlf(1)?;
        let count_str = String::from_utf8_lossy(&self.buffer[1..line_end]);
        let count: i64 = count_str.parse().map_err(|_| RespError::InvalidProtocol)?;
        #[allow(clippy::cast_possible_truncation)]
        let mut arr = Vec::with_capacity(count as usize);

        self.consume(line_end + 2)?;
        for _ in 0..count {
            arr.push(self.parse_internal()?);
        }

        Ok(RespValue::Array(arr))
    }

    fn find_crlf(&self, start: usize) -> Result<usize, RespError> {
        for i in start..self.buffer.len().saturating_sub(1) {
            if self.buffer[i] == b'\r' && self.buffer[i + 1] == b'\n' {
                return Ok(i);
            }
        }
        Err(RespError::Incomplete)
    }

    fn consume(&mut self, n: usize) -> Result<(), RespError> {
        if n > self.buffer.len() {
            return Err(RespError::Incomplete);
        }
        self.buffer.drain(..n);
        Ok(())
    }

    /// Returns `true` if the buffer contains a complete RESP message.
    #[must_use]
    pub fn has_complete_message(&self) -> bool {
        if self.buffer.is_empty() {
            return false;
        }
        match self.buffer[0] as char {
            '+' | '-' | ':' => self
                .find_crlf(1)
                .is_ok_and(|pos| pos + 2 <= self.buffer.len()),
            '$' => self.find_crlf(1).is_ok_and(|len_end| {
                let len_str = String::from_utf8_lossy(&self.buffer[1..len_end]);
                len_str.parse::<i64>().is_ok_and(|len| {
                    if len < 0 {
                        len_end + 2 <= self.buffer.len()
                    } else {
                        let total = len_end + 2 + len as usize + 2;
                        total <= self.buffer.len()
                    }
                })
            }),
            '*' => {
                if let Ok(count_end) = self.find_crlf(1) {
                    let count_str = String::from_utf8_lossy(&self.buffer[1..count_end]);
                    if let Ok(count) = count_str.parse::<i64>() {
                        if count <= 0 {
                            count_end + 2 <= self.buffer.len()
                        } else {
                            let mut pos = count_end + 2;
                            for _ in 0..count {
                                if pos >= self.buffer.len() {
                                    return false;
                                }
                                match self.buffer[pos] as char {
                                    '+' | '-' | ':' => {
                                        if let Ok(end) = self.find_crlf(pos + 1) {
                                            pos = end + 2;
                                        } else {
                                            return false;
                                        }
                                    }
                                    '$' => {
                                        if let Ok(len_end) = self.find_crlf(pos + 1) {
                                            let len_str = String::from_utf8_lossy(
                                                &self.buffer[pos + 1..len_end],
                                            );
                                            if let Ok(len) = len_str.parse::<i64>() {
                                                if len < 0 {
                                                    pos = len_end + 2;
                                                } else {
                                                    let total = len_end + 2 + len as usize + 2;
                                                    if total > self.buffer.len() {
                                                        return false;
                                                    }
                                                    pos = total;
                                                }
                                            } else {
                                                return false;
                                            }
                                        } else {
                                            return false;
                                        }
                                    }
                                    '*' => {
                                        return false;
                                    }
                                    _ => return false,
                                }
                            }
                            pos <= self.buffer.len()
                        }
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}
