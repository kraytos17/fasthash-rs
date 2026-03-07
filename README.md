<p align="center">
  <img src="media/banner-high-res.png" alt="FastHash Banner" width="100%">
</p>
<p align="center">FastHash is a simple, fast in-memory key-value store written in Rust.</p>

## Features

- [x] Core Features (Completed)
  - [x] Basic key-value `SET` and `GET` commands
  - [x] `DEL` command to delete keys
  - [x] `EXPIRE` command to set TTL (time-to-live) on keys
  - [x] `TTL` command to get remaining TTL of keys
  - [x] `SETEX` command (set key with expiry in one command)
  - [x] `EXISTS` command to check presence of keys
  - [x] `KEYS` command to list all keys (with pattern support planned)
  - [x] `FLUSHALL` command to clear all keys in the store
  - [x] `SAVE` command to persist the current key-value store to RDB format
  - [x] `LOAD` command to load persisted data from disk on startup

- [x] Persistence (Completed)
  - [x] RDB snapshotting in Redis-compatible binary format
  - [x] AOF (Append-Only File) logging for durability and incremental persistence
  - [x] Automatic AOF replay on server startup for crash recovery
  - [x] TTL support in persistence (absolute Unix timestamps)

- [x] Networking Layer (Completed)
  - [x] TCP server using Tokio async runtime
  - [x] Async multi-client handling with full concurrency support via Rust's async/await
  - [x] Command parsing over network (RESP2 protocol)
  - [x] Well-structured, production-grade request-response lifecycle
  - [x] Client session management, connection cleanup, error handling
  - [ ] TLS/SSL support for encrypted connections (planned)

- [ ] Advanced Data Types and Commands (Planned)
  - [ ] Hashes (maps/dictionaries) as value types
  - [ ] Lists, Sets, Sorted Sets (ZSets)
  - [ ] INCR/DECR numeric commands
  - [ ] Transactions and Lua scripting support

- [ ] SRE (Planned)
  - [ ] Replication (master-slave)
  - [ ] Persistence guarantees and durability levels
  - [ ] Cluster mode with sharding and failover
  - [ ] Monitoring and metrics (Prometheus integration)

- [x] Tooling & Developer Experience (Completed)
  - [x] Test suites using Rust's built-in `cargo test` (66 passing tests)
  - [x] CI/CD pipeline with Rust 1.94 and clippy linting
  - [ ] Docs and examples (via `cargo doc`)

## Quick Start

```bash
# Build
cargo build --release

# Run server
cargo run --release

# Or with AOF enabled (default)
cargo run --release -- --aof

# Run tests
cargo test
```

## Usage

```bash
# Connect with redis-cli
redis-cli -p 6379

# Basic commands
SET mykey "Hello"
GET mykey
DEL mykey
EXPIRE mykey 60
TTL mykey
SETEX tempkey 30 "expires soon"
KEYS *
FLUSHALL

# Persist to disk (RDB)
SAVE
# Or BGSAVE (background save coming soon)

# Load data on startup (automatic if dump.rdb exists)
```

## Architecture

- **Network**: Tokio async TCP server with RESP2 protocol support
- **Storage**: In-memory HashMap with TTL support
- **Persistence**: Redis-compatible RDB binary format + AOF logging
- **CLI**: clap for argument parsing
