# Proposal: Count / Basic Aggregation

**Status:** Accepted
**Author:** --
**Date:** 2026-02-06

## Motivation

The only way to count items matching a query is to fetch them all and measure the result length client-side. This wastes bandwidth (transferring full document bodies just to count them), wastes memory (allocating a `Vec<Value>` that's immediately discarded), and is unnecessarily slow for large partitions.

Common use cases that only need a count:
- "How many unresolved issues?" -- dashboard / health check
- "How many items in this partition?" -- rate limiting, capacity checks
- "How many items would this prune delete?" -- preview before destructive operation
- "Memory statistics per category" -- reporting

## Current State

```rust
// Current approach: fetch everything, count client-side
let count = db.query("memories")
    .partition_key("issues")
    .execute()?
    .items
    .len();
```

This transfers all item bodies over the wire (for server use) and deserializes every document, even though only the count is needed.

## Proposed Design

```rust
// Proposed: server-side count, no document transfer
let count = db.count("memories")
    .partition_key("issues")
    .execute()?;

// With sort key condition
let count = db.count("events")
    .partition_key("user1")
    .sort_key_between("2026-02-01", "2026-02-07")
    .execute()?;

// With filter (requires PRD #2)
let count = db.count("memories")
    .partition_key("issues")
    .filter(FilterExpr::eq("resolved", false))
    .execute()?;
```

### Optimization: Skip Document Deserialization

Without a filter expression, the count only needs to:
1. Scan B+Tree keys in the partition range
2. For each key, check if the item is TTL-expired (requires partial deserialization of just the TTL attribute)
3. Increment counter

This avoids full document deserialization, which is the expensive part of query processing.

With a filter expression, full deserialization is required (the filter may reference any attribute). But the wire transfer savings remain significant for server use.

### Wire Protocol

```json
// Request
{
  "action": "Count",
  "table": "memories",
  "partition_key": "issues",
  "sort_key_condition": { "begins_with": "2026-02" },
  "filter": null
}

// Response
{
  "count": 42
}
```

## Trade-offs

| Dimension | Impact |
|-----------|--------|
| Implementation | Low complexity -- same traversal as query, different accumulator |
| API surface | +1 builder, +1 protocol message |
| Performance (embedded) | Minor win -- skip document building, skip `Vec` allocation |
| Performance (server) | Major win -- response is 1 integer vs N full documents |

## Summary

`count` is the simplest useful aggregation. It reuses the exact same traversal logic as `query` but returns a single integer instead of a document array. Most of the implementation can share code with the query path.
