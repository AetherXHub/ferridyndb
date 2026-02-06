# Proposal: Sort Key Range Queries

**Status:** Accepted
**Author:** --
**Date:** 2026-02-06

## Motivation

FerridynDB's `query` operation supports only `begins_with` as a sort key condition. This forces callers to fetch entire partitions and filter client-side when they need range queries like "events between date A and B" or "sort keys greater than X" for keyset pagination.

DynamoDB supports six sort key conditions: `=`, `<`, `<=`, `>`, `>=`, and `BETWEEN`. FerridynDB should match this to enable efficient time-range queries, cursor-based pagination, and general-purpose range scans.

## Current State

- `QueryBuilder` has `.sort_key_begins_with(prefix)` -- the only sort key condition
- Internally, `compute_scan_bounds` converts `begins_with` into B+Tree start/end bounds
- The B+Tree already supports arbitrary range scans via encoded key bounds
- The encoding layer (`encoding/`) produces byte-ordered keys suitable for `memcmp`-style comparison

The infrastructure for range queries already exists at the B+Tree and encoding layers. The gap is purely in the query API.

## Proposed Design

### New Builder Methods

```rust
// Exact match
db.query("events").partition_key("user1").sort_key_eq("2026-02-06").execute()?;

// Range
db.query("events").partition_key("user1")
    .sort_key_between("2026-02-01", "2026-02-07").execute()?;

// Comparison
db.query("events").partition_key("user1").sort_key_gt("2026-02-01").execute()?;
db.query("events").partition_key("user1").sort_key_gte("2026-02-01").execute()?;
db.query("events").partition_key("user1").sort_key_lt("2026-02-07").execute()?;
db.query("events").partition_key("user1").sort_key_lte("2026-02-07").execute()?;
```

### Scan Bound Computation

Each condition maps to start/end bounds in the encoded key space:

| Condition | Start Bound | End Bound |
|-----------|-------------|-----------|
| `eq(v)` | `encode(pk) ++ encode(v)` | `encode(pk) ++ encode(v) ++ 0xFF` |
| `between(a, b)` | `encode(pk) ++ encode(a)` | `encode(pk) ++ encode(b) ++ 0xFF` |
| `gt(v)` | `encode(pk) ++ encode(v) ++ 0xFF` | `encode(pk) ++ 0xFF` |
| `gte(v)` | `encode(pk) ++ encode(v)` | `encode(pk) ++ 0xFF` |
| `lt(v)` | `encode(pk)` | `encode(pk) ++ encode(v)` |
| `lte(v)` | `encode(pk)` | `encode(pk) ++ encode(v) ++ 0xFF` |

The exact bound computation depends on the encoding scheme (null-terminated strings, byte-flipped IEEE 754 numbers). The existing `begins_with` computation provides the pattern.

### Wire Protocol

Extend the Query request's sort key condition field:

```json
{
  "action": "Query",
  "table": "events",
  "partition_key": "user1",
  "sort_key_condition": { "between": { "start": "2026-02-01", "end": "2026-02-07" } }
}
```

## Trade-offs

| Dimension | Impact |
|-----------|--------|
| API surface | +6 builder methods -- moderate increase but each is simple and well-motivated |
| Implementation | Low complexity -- maps to existing B+Tree range scan |
| Performance | No impact on existing queries; new queries are as efficient as `begins_with` |
| Compatibility | Pure addition; no breaking changes |

## Summary

Sort key range queries are a natural extension of the existing query infrastructure. The B+Tree and encoding layers already support arbitrary range scans; this PRD exposes that capability through the query API. Low complexity, high value for time-series and paginated workloads.
