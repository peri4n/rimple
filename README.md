# rimple
This is a Rust implementation of SimpleDB.

## Overview
rimple explores how databases work under the hood: storage, logging, buffering, and later indexing, transactions, and query execution. 
It prioritizes clarity over performance. APIs and on-disk formats are unstable and may change.

Inspired by the SimpleDB book:
https://link.springer.com/book/10.1007/978-3-030-33836-7

Not production-ready. Use for learning and experimentation.

## Current Status
- Storage primitives: pages/blocks and basic file manager
- Buffer management: in-memory page cache (WIP)
- Write-ahead logging: log manager and iterator (early)
- Top-level orchestration: `src/db.rs`

## Getting Started
Requirements: Rust toolchain. Optional: Nix flake dev shell (`nix develop`) or direnv.

Commands:
- Build: `cargo build`
- Test: `cargo test`
- Run: `cargo run`

## Project Structure
- `src/file/` — pages, blocks, file I/O
- `src/buffer/` — buffer frames and buffer manager
- `src/log/` — write-ahead log manager and iterator
- `src/db.rs` — top-level database wiring
- `src/main.rs` — entry point

## Roadmap

- [x] FileManager
- [x] BufferManager
- [x] LogManager
- [ ] Transactions
- [ ] Recovery

## Guiding Principles
- Educational-first: readable code and small steps
- Minimal dependencies beyond core Rust crates
- Clear boundaries between layers (file, buffer, log, higher layers later)

## License
AGPL-3.0-only. See `LICENSE`.
