//! TCP server for `FastHash`.
//!
//! Handles client connections and command processing using RESP protocol.

use crate::commands::{handler::CommandHandler, parser::RespParser, types::Response};
use crate::network::codec::encode_response;
use crate::persistence::aof::AofWriter;
use crate::store::db::KvStore;
use crate::store::list::ListStore;
use std::io::{Error, ErrorKind};
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};

/// Server errors.
#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum ServerError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid data: {0}")]
    InvalidData(String),
}

/// TCP server for the key-value store.
pub struct Server {
    addr: std::net::SocketAddr,
    store: Arc<KvStore>,
    list_store: Arc<ListStore>,
    aof: Option<Arc<AofWriter>>,
    rdb_path: PathBuf,
}

impl Server {
    /// Creates a new server instance.
    #[must_use]
    pub fn new(
        addr: std::net::SocketAddr,
        store: Arc<KvStore>,
        list_store: Arc<ListStore>,
        aof: AofWriter,
        rdb_path: PathBuf,
    ) -> Self {
        Self {
            addr,
            store,
            list_store,
            aof: Some(Arc::new(aof)),
            rdb_path,
        }
    }

    /// Starts the server and listens for connections.
    ///
    /// # Errors
    ///
    /// Returns `ServerError::Io` if binding to the address fails.
    pub async fn run(&self) -> Result<(), ServerError> {
        let listener = TcpListener::bind(&self.addr).await?;
        info!("Server listening on {}", self.addr);
        let rdb_path = self.rdb_path.clone();
        let aof = self.aof.clone();
        let list_store = self.list_store.clone();
        loop {
            let (socket, addr) = listener.accept().await?;
            info!("Client connected: {}", addr);
            let store = self.store.clone();
            tokio::spawn(handle_connection(
                socket,
                store,
                list_store.clone(),
                rdb_path.clone(),
                aof.clone(),
            ));
        }
    }
}

async fn handle_connection(
    socket: TcpStream,
    store: Arc<KvStore>,
    list_store: Arc<ListStore>,
    rdb_path: PathBuf,
    aof: Option<Arc<AofWriter>>,
) -> Result<(), Error> {
    let (mut read, mut write) = socket.into_split();
    let mut parser = RespParser::new();
    let handler = CommandHandler::new(store, list_store, rdb_path, aof);
    let mut buffer = vec![0u8; 4096];
    loop {
        match read.read(&mut buffer).await {
            Ok(0) => {
                info!("Client disconnected");
                return Ok(());
            }
            Ok(n) => {
                let data = &buffer[..n];
                match parser.parse(data) {
                    Ok(resp_value) => {
                        if let Some(cmd) = resp_value.to_command() {
                            let response = handler.handle(cmd);
                            let encoded = encode_response(&response);
                            if let Err(e) = write.write_all(&encoded).await {
                                warn!("Write error: {}", e);
                                return Err(e);
                            }
                        } else {
                            let error_response = Response::Error("ERR invalid command".into());
                            let encoded = encode_response(&error_response);
                            if let Err(e) = write.write_all(&encoded).await {
                                warn!("Write error: {}", e);
                                return Err(e);
                            }
                        }
                    }
                    Err(e) => {
                        if e.to_string() == "incomplete data" {
                            continue;
                        }

                        let error_response = Response::Error(format!("ERR parsing error: {e:?}"));
                        let encoded = encode_response(&error_response);
                        if let Err(write_err) = write.write_all(&encoded).await {
                            warn!("Write error: {}", write_err);
                            return Err(write_err);
                        }
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            format!("Parse error: {e:?}"),
                        ));
                    }
                }
            }
            Err(e) => {
                warn!("Read error: {}", e);
                return Err(e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{path::PathBuf, time::Duration};
    use tokio::time;

    async fn get_free_addr() -> std::net::SocketAddr {
        let listener = TcpListener::bind("[::1]:0").await.unwrap();
        listener.local_addr().unwrap()
    }

    #[tokio::test]
    async fn test_ping() {
        let addr = get_free_addr().await;
        let store = Arc::new(KvStore::new());
        let list_store = Arc::new(ListStore::new());
        let aof = AofWriter::new(
            &PathBuf::from("test.aof"),
            crate::persistence::aof::AofSync::No,
        )
        .unwrap();
        let server = Server::new(
            addr,
            store.clone(),
            list_store.clone(),
            aof,
            PathBuf::from("test.rdb"),
        );

        tokio::spawn(async move {
            let _ = server.run().await;
        });

        time::sleep(Duration::from_millis(100)).await;

        let stream = TcpStream::connect(addr).await.unwrap();
        let (mut read, mut write) = stream.into_split();

        let ping_cmd = b"*1\r\n$4\r\nPING\r\n";
        write.write_all(ping_cmd).await.unwrap();

        let mut response = vec![0u8; 1024];
        let n = read.read(&mut response).await.unwrap();
        assert_eq!(&response[..n], b"+PONG\r\n");
    }

    #[tokio::test]
    async fn test_set_get() {
        let addr = get_free_addr().await;
        let store = Arc::new(KvStore::new());
        let list_store = Arc::new(ListStore::new());
        let aof = AofWriter::new(
            &PathBuf::from("test.aof"),
            crate::persistence::aof::AofSync::No,
        )
        .unwrap();

        let server = Server::new(
            addr,
            store.clone(),
            list_store.clone(),
            aof,
            PathBuf::from("test.rdb"),
        );
        tokio::spawn(async move {
            let _ = server.run().await;
        });

        time::sleep(Duration::from_millis(100)).await;
        let stream = TcpStream::connect(addr).await.unwrap();
        let (mut read, mut write) = stream.into_split();
        let set_cmd = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        write.write_all(set_cmd).await.unwrap();

        let mut response = vec![0u8; 1024];
        let n = read.read(&mut response).await.unwrap();
        assert_eq!(&response[..n], b"+OK\r\n");

        let get_cmd = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
        write.write_all(get_cmd).await.unwrap();

        let mut response = vec![0u8; 1024];
        let n = read.read(&mut response).await.unwrap();
        assert_eq!(&response[..n], b"$3\r\nbar\r\n");
    }
}
