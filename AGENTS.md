# AGENTS.md

Instructions for AI agents working on this codebase.

## Development Rules

Every change must leave the project in a fully working state:

1. **Compile first** — `cargo build` must pass with zero errors before moving on
2. **Test everything** — Write tests for each new feature before considering it done
3. **Lint clean** — `cargo clippy --workspace -- -D warnings` must pass
4. **Format** — `cargo fmt --all --check` must pass
5. **No dead code** — No `todo!()` or `unimplemented!()` in committed code
6. **One layer at a time** — Build bottom-up through the architecture stack. Do not start a higher layer until the layer beneath it compiles, passes tests, and is lint-clean.

## Benchmarks

Two benchmark suites in `ferridyn-core`:

- `ferridyn_bench` — In-memory (tmpfs) microbenchmarks. Run routinely.
- `ferridyn_file_bench` — File-backed benchmarks with real I/O. Uses tmpfs by default; set `BENCH_DIR` to point at real storage.

Default: run on tmpfs (`cargo bench`). Only run on real NVMe after major refactors. NVMe results are dominated by fsync latency (~5ms per commit) which masks algorithmic changes.

## Important Invariants

- **Magic bytes `b"DYNA"`** in `storage/header.rs` — File format identifier. Never change this.
- **4KB page size** — Hardcoded throughout the storage layer. Changing it breaks existing database files.
- **Double-buffered header** — Pages 0 and 1 alternate as the committed header. Both must always be valid.
