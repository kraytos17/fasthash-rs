//! RESP protocol encoding for `FastHash`.
//!
//! Implements Redis RESP2 protocol encoding for server responses.

use crate::commands::types::Response;
use bytes::BytesMut;

/// Encodes a response into RESP2 format.
#[must_use]
pub fn encode_response(response: &Response) -> BytesMut {
    let mut buffer = BytesMut::new();
    match response {
        Response::Ok => buffer.extend_from_slice(b"+OK\r\n"),
        Response::Pong => buffer.extend_from_slice(b"+PONG\r\n"),
        Response::BulkString(s) => encode_bulk_string(s, &mut buffer),
        Response::Integer(n) => encode_integer(*n, &mut buffer),
        Response::Array(keys) => encode_array_string(keys, &mut buffer),
        Response::Error(e) => {
            buffer.extend_from_slice(format!("-{e}\r\n").as_bytes());
        }
        Response::Null => buffer.extend_from_slice(b"$-1\r\n"),
    }
    buffer
}

fn encode_bulk_string(s: &str, buffer: &mut BytesMut) {
    buffer.extend_from_slice(format!("${}\r\n", s.len()).as_bytes());
    buffer.extend_from_slice(s.as_bytes());
    buffer.extend_from_slice(b"\r\n");
}

fn encode_integer(n: i64, buffer: &mut BytesMut) {
    buffer.extend_from_slice(format!(":{n}\r\n").as_bytes());
}

fn encode_array_string(arr: &[String], buffer: &mut BytesMut) {
    buffer.extend_from_slice(format!("*{}\r\n", arr.len()).as_bytes());
    for s in arr {
        encode_bulk_string(s, buffer);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_ok() {
        let result = encode_response(&Response::Ok);
        assert_eq!(&result[..], b"+OK\r\n");
    }

    #[test]
    fn test_encode_pong() {
        let result = encode_response(&Response::Pong);
        assert_eq!(&result[..], b"+PONG\r\n");
    }

    #[test]
    fn test_encode_bulk_string() {
        let result = encode_response(&Response::BulkString("hello".into()));
        assert_eq!(&result[..], b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_encode_integer() {
        let result = encode_response(&Response::Integer(42));
        assert_eq!(&result[..], b":42\r\n");
    }

    #[test]
    fn test_encode_array() {
        let result = encode_response(&Response::Array(vec!["key1".into(), "key2".into()]));
        assert_eq!(&result[..], b"*2\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n");
    }

    #[test]
    fn test_encode_error() {
        let result = encode_response(&Response::Error("ERR unknown command".into()));
        assert_eq!(&result[..], b"-ERR unknown command\r\n");
    }

    #[test]
    fn test_encode_empty_array() {
        let result = encode_response(&Response::Array(vec![]));
        assert_eq!(&result[..], b"*0\r\n");
    }

    #[test]
    fn test_encode_bulk_string_empty() {
        let result = encode_response(&Response::BulkString("".into()));
        assert_eq!(&result[..], b"$0\r\n\r\n");
    }
}
