# Proposal: TTL Enhancements

**Status:** Accepted
**Author:** --
**Date:** 2026-02-06

## Motivation

FerridynDB already has solid TTL infrastructure: `ttl_attribute` on table schemas, read-side filtering (`is_ttl_expired`), and explicit cleanup via `sweep_expired_ttl()`. However, three gaps remain:

1. **No convenience methods** -- to set or remove TTL on an existing item, callers must know the TTL attribute name, compute the epoch timestamp, and use `update_item` manually. This is error-prone and verbose.

2. **No background cleanup** -- expired items are invisible to reads (good) but remain on disk until someone explicitly calls `sweep_expired_ttl()`. For long-running applications, this means unbounded storage growth unless the application implements its own cleanup loop.

3. **No server protocol support** -- TTL management operations aren't available over the wire.

## Current State

| Feature | Status |
|---------|--------|
| `ttl_attribute` on `TableSchema` | Implemented |
| Read-side filtering (`is_ttl_expired`) | Implemented on get, query, scan, query_index |
| `sweep_expired_ttl()` | Implemented (batch of 100, explicit call) |
| DynamoDB semantics (TTL=0 = never, non-numeric ignored) | Implemented |
| `set_ttl()` / `remove_ttl()` convenience methods | Missing |
| `get_ttl()` introspection | Missing |
| Background reaper | Missing |
| Server protocol for TTL ops | Missing |

## Proposed Design

### Convenience Methods

```rust
// Set TTL: item expires in 24 hours
db.set_ttl("cache", "session-key", None, 86400)?;

// Remove TTL: make item permanent
db.remove_ttl("cache", "session-key", None)?;

// Check remaining TTL
match db.get_ttl("cache", "session-key", None)? {
    Some(seconds) => println!("Expires in {seconds}s"),
    None => println!("No TTL set"),
}
```

**Implementation:** `set_ttl` uses `update_item` to SET the table's configured `ttl_attribute` to `now_epoch + ttl_seconds`. `remove_ttl` sets it to 0 (which means "never expires" per existing DynamoDB semantics). `get_ttl` reads the item, extracts the TTL attribute, computes `max(0, ttl_value - now)`.

### Background Reaper

```rust
// Start background cleanup every 30 minutes
let reaper = db.start_reaper(Duration::from_secs(1800));

// Later, stop it
reaper.stop();

// Or it stops automatically when db is dropped
```

The reaper thread:
1. Iterates all tables in the catalog
2. Skips tables without `ttl_attribute`
3. Calls `sweep_expired_ttl()` in a loop until it returns 0
4. Sleeps for the configured interval
5. Repeat

This is a simple `std::thread::spawn` with an `Arc<AtomicBool>` shutdown flag. No async runtime needed.

### Wire Protocol

Four new request types:
- `SetTTL { table, partition_key, sort_key?, ttl_seconds }`
- `RemoveTTL { table, partition_key, sort_key? }`
- `GetTTL { table, partition_key, sort_key? }` -> `{ remaining_seconds: Option<u64> }`
- `SweepExpiredTTL { table }` -> `{ deleted: usize }`

## Trade-offs

| Dimension | Impact |
|-----------|--------|
| Complexity | Low -- builds entirely on existing TTL and update_item infrastructure |
| Background thread | Opt-in; no overhead if not started; single thread for all tables |
| Storage model | No change -- still attribute-based TTL |
| API surface | +3 core methods, +4 protocol messages |

## Summary

These are quality-of-life improvements to an already-functional TTL system. The convenience methods eliminate boilerplate, the reaper automates what applications currently do manually, and the wire protocol support makes TTL management available to all clients.
