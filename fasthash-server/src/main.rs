//! `FastHash` - In-memory key-value store with RESP protocol support.
//!
//! A high-performance, in-memory key-value store compatible with Redis RESP2 protocol.

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use fasthash::persistence::{
    aof::{self, AofWriter},
    rdb,
};
use fasthash::store::{db::KvStore, list::ListStore};
use std::path::PathBuf;
use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(name = "fasthash-rs")]
#[command(about = "FastHash - In-memory key-value store", long_about = None)]
struct Args {
    #[arg(short, long, default_value = "6379")]
    port: u16,

    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    host: String,

    #[arg(long, default_value = "appendonly.aof")]
    aof_file: PathBuf,

    #[arg(long, value_enum, default_value = "everysec")]
    aof_sync: AofSync,

    #[arg(long, default_value = "dump.rdb")]
    rdb_file: PathBuf,

    #[arg(long)]
    load_rdb: bool,
}

#[derive(Debug, Clone, ValueEnum)]
enum AofSync {
    Always,
    Everysec,
    No,
}

impl From<AofSync> for fasthash::persistence::aof::AofSync {
    fn from(s: AofSync) -> Self {
        match s {
            AofSync::Always => Self::Always,
            AofSync::Everysec => Self::Everysec,
            AofSync::No => Self::No,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()),
        ))
        .init();

    tracing::info!("Starting FastHash server on {}:{}", args.host, args.port);

    let addr = format!("{}:{}", args.host, args.port);
    let store = Arc::new(KvStore::new());
    let list_store = Arc::new(ListStore::new());
    if args.load_rdb {
        tracing::info!("Loading RDB from {:?}", args.rdb_file);
        rdb::load(&store, &list_store, &args.rdb_file)?;
    }

    tracing::info!("Replaying AOF from {:?}", args.aof_file);
    match aof::replay(&store, &list_store, &args.aof_file) {
        Ok(count) if count > 0 => {
            tracing::info!("Replayed {} commands from AOF", count);
        }
        Ok(_) => {}
        Err(e) => {
            tracing::warn!("Failed to replay AOF: {}", e);
        }
    }

    let aof = AofWriter::new(&args.aof_file, args.aof_sync.into())
        .context("Failed to create AOF writer")?;

    let server = fasthash::network::server::Server::new(
        addr.parse().context("Failed to parse address")?,
        store,
        list_store,
        aof,
        args.rdb_file,
    );

    server.run().await.context("Server error")?;
    Ok(())
}
