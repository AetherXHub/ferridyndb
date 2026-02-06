# PRD: Storage Integrity Quick Fixes

**Priority:** 16
**Status:** Approved
**Origin:** redb design analysis — immediate correctness fixes

**Roadmap Sequencing:** Do **before PRD-02** (Filter Expressions). These are trivial fixes to the storage foundation that every subsequent feature builds on.

## Summary

Two small, low-risk fixes to the storage engine that improve data integrity and reduce unnecessary I/O overhead. Both are informed by redb's design and address gaps in the current page format and overflow handling.

## Scope

### In Scope

#### Fix 1: Include page_id in checksum range

The current page checksum (`storage/page.rs`) computes xxHash64 over bytes 20..4096, which excludes the first 20 bytes containing `page_type` (u8), `page_id` (u32), and the checksum itself (u64). This means:
- A page read from the wrong disk offset can appear valid (its page_id won't match but the checksum still passes)
- A page with a corrupted page_type still passes checksum verification

**Fix:** Restructure the page header so the checksum covers all meaningful fields. Two approaches:
1. **Move checksum to end of header:** Checksum bytes 0..12 (page_type + padding + page_id), skip checksum field, then continue 20..4096. Requires no on-disk format change beyond recomputing checksums.
2. **Compute checksum over page_type + page_id + data:** Hash `[page_type_bytes, page_id_bytes, data_bytes_20..4096]` as a concatenated input. Simpler code change, same protection.

Verify on read: if stored page_id doesn't match the expected page_id for the file offset, return `CorruptedPage`.

#### Fix 2: Increase overflow threshold

The current `OVERFLOW_THRESHOLD` of 1500 bytes (`btree/overflow.rs:35`) is conservative. The usable space in a slotted leaf page for a single cell is approximately 3800 bytes (4096 - header - slot overhead - key). Documents between 1501-3800 bytes unnecessarily trigger overflow page chains, adding pointer-chasing I/O.

**Fix:** Increase `OVERFLOW_THRESHOLD` to approximately 3800 bytes (or compute dynamically based on available page space). This reduces overflow frequency for typical JSON documents (most are 1-4KB) without any format change — existing overflow documents continue to work.

### Out of Scope

- Merkle checksum tree (cascading parent-child checksums) — deferred to PRD-19
- Page format version bump — these changes are backward-compatible with a migration pass
- Variable-size pages / buddy allocator — deferred to v2

## Decisions

| Question | Decision | Rationale |
|----------|----------|-----------|
| Checksum approach | Option 2 (concatenated hash input) | Simpler code change, no header restructure needed |
| Overflow threshold value | ~3800 bytes (compute from page size - header - max key overhead) | Maximizes in-page storage without risking page overflow |
| Migration for existing files | Recompute checksums on first write to each page | Lazy migration, no bulk rewrite needed |

## Phases

**Single phase** — both fixes are independent, small changes.

### Phase 1: Both Fixes

| Component | Change |
|-----------|--------|
| `storage/page.rs` | Update `compute_checksum()` to include page_type and page_id in hash input |
| `storage/page.rs` | Add page_id validation on read (expected vs stored) |
| `btree/overflow.rs` | Increase `OVERFLOW_THRESHOLD` constant |
| Tests | Add test for page_id mismatch detection |
| Tests | Add test verifying documents up to new threshold are stored inline |

## Risks

- **Checksum change breaks existing files:** Mitigated by lazy migration — old checksums will fail verification on read, but the page data is still valid. On next write, the new checksum is computed. Could add a one-time migration flag in the header if needed.
- **Overflow threshold too high:** If a leaf page has many small items, a single large item near the threshold could fail to fit. The insertion logic already handles this by falling back to overflow when the page is full, so this is safe.

## Success Criteria

- [ ] Page read from wrong offset detected as corrupted (page_id mismatch)
- [ ] Corrupted page_type detected by checksum verification
- [ ] Documents up to ~3800 bytes stored inline without overflow
- [ ] All existing tests pass
- [ ] Benchmarks show reduced overflow page reads for typical document sizes
