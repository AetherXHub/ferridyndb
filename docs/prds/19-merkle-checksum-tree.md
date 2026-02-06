# PRD: Merkle Checksum Tree

**Priority:** 19
**Status:** Approved
**Origin:** redb design analysis — long-term integrity hardening

**Roadmap Sequencing:** Do **after PRD-15** (Vector Index) as part of a v2 storage format initiative. This is a high-effort change that touches every B-tree node and is best bundled with other format-breaking improvements (B-tree rebalancing, potential buddy allocator).

## Summary

Replace the current per-page independent checksums with a Merkle checksum tree where internal B-tree nodes store the checksums of their child pages. This enables cascading integrity verification from the root downward, detecting not just single-page corruption but also page substitution (a valid-but-wrong page at a given offset) and stale-page references (an old version of a page from a previous transaction).

Inspired by redb's use of XXH3_128 Merkle trees across its B-tree structure.

## Problem Statement

The current checksum scheme (`storage/page.rs`) computes an independent xxHash64 for each page. This catches:
- Bit-rot / media corruption within a single page
- Partial page writes (torn pages)

But it does NOT catch:
- **Page substitution:** A valid page from one location read at a different location (mitigated partially by PRD-16's page_id-in-checksum fix, but not fully — an old version of the same page_id from a previous transaction would still pass)
- **Stale page references:** An internal node pointing to an old version of a child page (possible after a crash with in-place overwrites, addressed by PRD-17's true COW, but defense-in-depth is valuable)
- **Silent corruption chains:** If an internal node is corrupted to point to a different valid subtree, the database returns wrong data with no error

## Scope

### In Scope

- Store child page checksums in internal (branch) B-tree nodes alongside child page pointers
- Verify child checksum on every page read during tree traversal
- Upgrade checksum algorithm from xxHash64 (64-bit) to XXH3_128 (128-bit) for stronger collision resistance
- Root checksum stored in the file header for full-tree verification from the top
- Cascading verification: root checksum -> internal node checksums -> leaf checksums
- Apply to all B-tree structures: data trees, catalog tree, secondary index trees, system trees

### Out of Scope

- Cryptographic checksums (SHA-256, etc.) — non-cryptographic is sufficient for integrity; redb uses XXH3_128
- Full-tree verification on open (too expensive for large databases) — verify lazily on read
- Background scrubbing (proactive full-tree checksum verification) — defer to later
- Backward compatibility with v1 format — this is a breaking format change requiring migration

## Decisions

| Question | Decision | Rationale |
|----------|----------|-----------|
| Checksum algorithm | XXH3_128 (via `xxhash-rust` crate, already a dependency) | 128-bit provides strong collision resistance; same as redb; xxhash-rust already supports XXH3 |
| Checksum storage in internal nodes | Array of 16-byte checksums, one per child pointer | Matches redb's branch page format; directly verifiable on child read |
| When to verify | On every page read during tree traversal | Lazy verification; catches corruption at access time without upfront cost |
| Root checksum in header | Add `data_root_checksum: [u8; 16]` and `catalog_root_checksum: [u8; 16]` to `FileHeader` | Enables top-down verification from header |
| Migration strategy | Offline migration tool + format version bump | Clean break; no hybrid v1/v2 pages |

## Design

### Internal Node Layout Change

Current internal node format (from `btree/node.rs`):
```
[slot_count: u16] [slots: (offset, length) pairs]
[cells: (key, child_page_id) pairs growing backward]
```

New internal node format:
```
[slot_count: u16] [slots: (offset, length) pairs]
[child_checksums: [u8; 16] × (slot_count + 1)]
[cells: (key, child_page_id) pairs growing backward]
```

Each internal node gains `16 * (fan_out)` bytes of checksum storage. For a typical internal node with ~100 children, this is ~1.6KB — significant but acceptable given the integrity benefit.

### Verification Flow

```
read_page(page_id):
  1. Read raw page from disk
  2. Verify page's own checksum (xxHash64 -> XXH3_128)
  3. Return page

traverse_to_child(parent_node, child_index):
  1. Get expected_checksum from parent_node.child_checksums[child_index]
  2. Get child_page_id from parent_node.child_pointers[child_index]
  3. Read child page
  4. Compute actual_checksum of child page
  5. If expected != actual: return CorruptedTree error
  6. Return child page

open_database():
  1. Read header
  2. Verify header checksum
  3. Read data root page
  4. Verify data_root_checksum from header matches root page
  5. (Same for catalog root)
```

### Space Overhead

| Tree Depth | Internal Nodes | Checksum Overhead per Node | Total Overhead |
|------------|---------------|---------------------------|----------------|
| 3 (10K items) | ~100 | ~1.6 KB | ~160 KB |
| 4 (1M items) | ~10,000 | ~1.6 KB | ~16 MB |
| 5 (100M items) | ~1,000,000 | ~1.6 KB | ~1.6 GB |

For FerridynDB's target use case (embedded, local-first, < 1M items typically), the overhead is modest.

## Phases

### Phase 1: Upgrade Checksum Algorithm

| Component | Change |
|-----------|--------|
| `storage/page.rs` | Replace xxHash64 with XXH3_128 for page checksums |
| `storage/header.rs` | Replace xxHash64 with XXH3_128 for header checksum |
| `storage/header.rs` | Add format version field or bump magic number |
| All tests | Update expected checksum sizes |

### Phase 2: Child Checksums in Internal Nodes

| Component | Change |
|-----------|--------|
| `btree/node.rs` | Add `child_checksums: Vec<[u8; 16]>` to internal node representation |
| `btree/node.rs` | Serialize/deserialize child checksums in slotted page layout |
| `btree/ops.rs` | On split/insert/modify of internal node, compute and store child checksums |
| `btree/ops.rs` | On tree traversal, verify child checksum before descending |
| `storage/page.rs` | Add `compute_checksum_128()` method |
| Tests | Corrupt a child page, verify parent detects mismatch |

### Phase 3: Root Checksums in Header

| Component | Change |
|-----------|--------|
| `storage/header.rs` | Add `data_root_checksum: [u8; 16]`, `catalog_root_checksum: [u8; 16]` fields |
| `api/database.rs` | On commit, compute root page checksums and write to header |
| `api/database.rs` | On open, verify root checksums match root pages |
| Tests | Corrupt root page, verify open detects and reports error |

### Phase 4: Migration Tool

| Component | Change |
|-----------|--------|
| New: `tools/migrate_v2.rs` or CLI command | Read v1 database, write v2 with Merkle checksums |
| `api/database.rs` | On open, detect format version; reject v1 with helpful error message pointing to migration |

## Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| Space overhead in internal nodes | Reduced fan-out (fewer children per internal node) → deeper trees | Monitor B-tree depth in benchmarks; 1.6KB per node is acceptable for typical fan-out |
| Performance overhead on every read | Checksum computation on every page traversal | XXH3_128 is extremely fast (~30 GB/s); 4KB page checksum takes ~130ns; negligible vs I/O |
| Breaking format change | Existing databases must be migrated | Provide migration tool; clear error message on version mismatch |
| Increased write amplification | Every internal node modification must update parent's child checksum | Already addressed by PRD-17 (true COW); parent updates are already required for new page_ids |

## Bundle Opportunities

This PRD is best implemented alongside other v2 storage format changes:
- **B-tree rebalancing:** Add merge/redistribute on delete (currently mark-dead only)
- **Buddy allocator:** Variable-size pages for large values (currently overflow chains)
- **File compaction:** Ability to shrink the database file (currently only grows)

Bundling reduces the number of format migrations users need to perform.

## Success Criteria

- [ ] Every page read during tree traversal is verified against parent's stored checksum
- [ ] Root page checksums in header enable top-down verification on open
- [ ] Corrupted/substituted child page detected and reported as `CorruptedTree` error
- [ ] XXH3_128 used for all checksums (header, pages, Merkle)
- [ ] Benchmark shows < 5% performance regression from checksum verification overhead
- [ ] Migration tool converts v1 databases to v2 format
- [ ] All existing tests pass with new format
