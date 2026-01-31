# DynaMite Implementation Plan (Revised)

## Design Decisions (Finalized)

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Writer model | Single writer, unlimited readers (LMDB model) | Simpler, proven, sufficient for embedded |
| Key encoding | Escaped-terminator for strings/binary, byte-flipped IEEE 754 for numbers | Preserves lexicographic `memcmp` ordering |
| Page checksums | xxHash64, 8 bytes per page header | Fast corruption detection |
| Header design | Double-buffered (pages 0 and 1) | Truly atomic commits |
| Max document size | 400KB (DynamoDB parity) | Bounded overflow chains |
| Isolation level | Snapshot isolation only (v1) | Defer serializable to v2 |
| Node.js bindings | Deferred to post-v1 | Focus on stable Rust API first |
| Page layout | Slotted pages | Variable-size keys/values, efficient insert/delete |
| MVCC version storage | Latest version inline in leaf, older versions in overflow chain | Keeps B+Tree clean, no duplicate keys |
| B+Tree delete strategy | Mark-as-dead, no rebalancing in v1 | Simpler, pages reclaimed when fully empty |
| Scan filters | Key-based only in v1 | Defer full document filter expressions to v2 |

## Improvements Over Original Proposal

1. **Double-buffered header** (pages 0+1) instead of single header — eliminates partial-write window
2. **Escaped-terminator key encoding** instead of `0x00` separator — preserves lexicographic ordering, no forbidden characters
3. **xxHash64 checksums** on every page — detects silent corruption
4. **Single-writer model** explicitly stated — avoids underspecified concurrent writer semantics
5. **Slotted page layout** specified — efficient variable-size storage
6. **Key encoding before B+Tree** in implementation order — B+Tree depends on comparable encoded keys
7. **Explicit file growth strategy** — grow by 2x, remap readers
8. **Structured error hierarchy** — five error categories defined upfront
9. **MVCC garbage collection** specified — track oldest active snapshot, reclaim unreachable versions
10. **IEEE 754 edge cases** addressed — NaN rejection, -0 normalization, negative number bit-flip
11. **File locking** — `flock()` prevents concurrent process access corruption
12. **Latest-inline version storage** — avoids B+Tree duplicate key complexity
13. **Deferred B+Tree rebalancing** — mark-as-dead delete, simpler and less bug-prone

---

## Phase 1: Project Scaffolding

Convert the bare skeleton into a Cargo workspace.

### Tasks

1.1. **Create workspace structure**
- Convert root `Cargo.toml` to a workspace manifest
- Create `crates/dynamite-core/Cargo.toml` as a library crate (edition 2024)
- Create `crates/dynamite-core/src/lib.rs` with module declarations
- Remove `src/main.rs` (no binary in v1, library only)
- Create module directories: `storage/`, `btree/`, `mvcc/`, `catalog/`, `api/`, `encoding/`
- Create `tests/` directory at workspace root for integration tests

1.2. **Add dependencies to `dynamite-core`**
```toml
[dependencies]
serde = { version = "1", features = ["derive"] }
serde_json = "1"
memmap2 = "0.9"
parking_lot = "0.12"
thiserror = "2"
bytes = "1"
xxhash-rust = { version = "0.8", features = ["xxh64"] }

[dev-dependencies]
tempfile = "3"
criterion = "0.5"
```

1.3. **Define error hierarchy** in `crates/dynamite-core/src/error.rs`
```rust
pub enum Error {
    Storage(StorageError),    // I/O, corruption, file locked
    Encoding(EncodingError),  // Invalid key types, NaN, doc too large
    Transaction(TxnError),    // Conflict, aborted, snapshot expired
    Schema(SchemaError),      // Table not found, already exists, key mismatch
    Query(QueryError),        // Invalid operator, type mismatch
}
```

1.4. **Define core types** in `crates/dynamite-core/src/types.rs`
```rust
pub type PageId = u64;
pub type TxnId = u64;
pub const PAGE_SIZE: usize = 4096;
pub const PAGE_HEADER_SIZE: usize = 32;
pub const MAX_DOCUMENT_SIZE: usize = 400 * 1024; // 400KB
pub const SLOT_SIZE: usize = 4; // (offset: u16, length: u16)

pub enum KeyType { String, Number, Binary }

pub struct TableSchema {
    pub name: String,
    pub partition_key: (String, KeyType),
    pub sort_key: Option<(String, KeyType)>,
}
```

1.5. **Update CLAUDE.md** to reflect finalized design decisions
- Double-buffered header (pages 0+1)
- Escaped-terminator key encoding (not `0x00` separator)
- Single-writer model
- Remove Node.js binding references from workspace layout (mark as future)
- Note deferred features: serializable isolation, Node.js bindings, scan filters

### Verification
- `cargo build -p dynamite-core` compiles with zero errors
- `cargo clippy --workspace` passes
- `cargo fmt --all --check` passes

---

## Phase 2: Storage Engine

Low-level file I/O, page abstractions, and file locking.

### Tasks

2.1. **Page abstraction** (`storage/page.rs`)
- `Page` struct: 4096-byte buffer, page ID, dirty flag
- Page type enum: `Header`, `BTreeInternal`, `BTreeLeaf`, `Overflow`, `FreeList`
- Common page header layout (first 32 bytes of every data page):
  ```
  [0..4]   page_type: u32
  [4..12]  page_id: u64
  [12..20] xxhash64 checksum (of bytes 20..4096)
  [20..24] entry_count: u32
  [24..28] free_space_offset: u32
  [28..32] reserved
  ```
- Page type validation on read: verify page_type matches expected type, return `StorageError::CorruptedPage` on mismatch
- Checksum strategy: data pages checksum bytes 20..4096 (everything after the checksum field); header pages checksum bytes 0..44 (everything before the checksum field). Document this explicitly in code comments.

2.2. **Slotted page layout** (`storage/slotted.rs`)
- Slot array starts at byte 32 (or after leaf-specific header), grows forward: each slot is `(offset: u16, length: u16)` = 4 bytes
- Cell data starts at end of page, grows backward
- Insert: append slot, write cell at free space pointer
- Delete: mark slot as dead (offset = 0), compact on demand
- Free space calculation: `free_space_offset - (header_size + slot_count * SLOT_SIZE)`
- BTreeLeaf pages have an additional `next_leaf: u64` field (8 bytes) at byte 32, so slot array starts at byte 40

2.3. **File header** (`storage/header.rs`)
- Double-buffered: page 0 and page 1 are both header pages
- Header layout:
  ```
  [0..4]   magic: "DYNA"
  [4..8]   version: u32 (1)
  [8..12]  page_size: u32 (4096)
  [12..20] txn_counter: u64
  [20..28] catalog_root_page: u64
  [28..36] free_list_head_page: u64
  [36..44] total_page_count: u64
  [44..52] xxhash64 checksum (of bytes 0..44)
  ```
- On open: read both headers, validate checksums, use the one with higher `txn_counter`
- On commit: write to the alternate header page

2.4. **File manager** (`storage/file.rs`)
- Create new database file: write two header pages, fsync
- Open existing file: validate magic bytes, read headers, pick current
- Read page by ID: `offset = page_id * PAGE_SIZE`, read 4096 bytes
- Write page: `pwrite()` at correct offset, update checksum before write
- File growth: when `page_id >= total_page_count`, grow file by 2x current size via `ftruncate`, update header

2.5. **Memory-mapped reader** (`storage/mmap.rs`)
- Read-only mmap of the database file (`MAP_SHARED`, read-only)
- Remap on file growth (close old mapping, create new one)
- Safety invariant (document in code): CoW guarantees no page visible to readers is ever modified in place. Readers only access pages reachable from the committed header pointer, so partially-written new pages are invisible until the header is updated and fsynced.
- Provide `fn read_page(page_id: PageId) -> &[u8]` via mmap slice
- Bounds check: verify `page_id < total_page_count` before indexing into mmap

2.6. **File locking** (`storage/lock.rs`)
- On open for writing: acquire exclusive `flock()` on the database file
- On open for read-only: acquire shared `flock()`
- Return `StorageError::FileLocked` if lock cannot be acquired
- Lock released automatically on file close (RAII via `Drop`)

### Verification
- Unit tests: create file, write header, close, reopen, validate header
- Unit tests: write pages, read back, verify checksum
- Unit tests: checksum mismatch detected on corrupted page
- Unit tests: slotted page insert/delete/compact
- Unit tests: double-buffered header alternation
- Unit tests: file growth and remap
- Unit tests: page type validation rejects wrong type
- Unit tests: file locking prevents double-open for write
- Unit tests: multiple shared locks (readers) allowed simultaneously

---

## Phase 3: Page Manager

Page allocation, free list, and copy-on-write mechanics.

### Tasks

3.1. **Free list** (`storage/freelist.rs`)
- Free list page layout (trunk pages): array of `PageId` values, plus a `next_trunk` pointer
- Each trunk page holds `(4096 - 32 - 8) / 8 = 507` free page IDs + next pointer
- `allocate_page() -> PageId`: pop from free list head, or extend file if empty
- `free_page(page_id)`: push onto free list head

3.2. **Pending-free list** (`storage/pending_free.rs`)
- In-memory structure: `Vec<(TxnId, Vec<PageId>)>` — pages freed by each transaction, keyed by the freeing txn's ID
- When a write transaction frees pages (via CoW or document delete), they go here, NOT directly onto the disk free list
- On each commit: scan pending list, move entries where `txn_id <= oldest_active_snapshot()` to the real on-disk free list
- On crash recovery: pending-free list is rebuilt by scanning the B+Tree for unreachable pages (rare, only needed after unclean shutdown with active readers)
- This list is NOT persisted to disk — it is transient, in-memory only

3.3. **Copy-on-write manager** (`storage/cow.rs`)
- Write transaction accumulates new/modified pages in memory (`HashMap<PageId, Page>`)
- `cow_page(page_id) -> &mut Page`: copy existing page into write buffer, return mutable reference
- On commit: write all dirty pages to file via `pwrite`, fsync, write new header to alternate slot, fsync
- On abort: discard write buffer, return allocated pages to pending-free list

3.4. **Snapshot tracker** (`storage/snapshot.rs`)
- Track all active read snapshots (their txn IDs) in a `BTreeSet<TxnId>` protected by `parking_lot::Mutex`
- `register_snapshot(txn_id)`: add to set (called when read txn begins)
- `deregister_snapshot(txn_id)`: remove from set (called when read txn drops — RAII guard)
- `oldest_active_snapshot() -> Option<TxnId>`: `set.iter().next()` (minimum)
- If no active snapshots, all pending-free pages can be immediately reclaimed

### Verification
- Unit tests: allocate pages, free them, reallocate (should reuse after reclamation)
- Unit tests: pending-free pages NOT reusable while snapshot is active
- Unit tests: pending-free pages reclaimed after snapshot deregisters
- Unit tests: CoW — modify a page, verify original is unchanged via mmap
- Unit tests: commit writes new header to alternate page
- Unit tests: abort discards changes

---

## Phase 4: Key Encoding

Byte-comparable encoding for all key types. Independent of storage — can be built in parallel with Phase 2.

### Tasks

4.1. **Key value enum** (`encoding/mod.rs`)
```rust
pub enum KeyValue {
    String(String),
    Number(f64),
    Binary(Vec<u8>),
}
```

4.2. **String encoding** (`encoding/string.rs`)
- Escaped-terminator scheme for lexicographic `memcmp` ordering:
  - Encode: for each byte in UTF-8 representation:
    - `0x00` → emit `0x00 0xFF` (escape)
    - all other bytes → emit as-is
    - After all bytes: emit `0x00 0x00` (terminator)
  - Decode: read bytes, when `0x00` encountered:
    - Next byte is `0xFF` → decoded byte is `0x00`
    - Next byte is `0x00` → end of string
  - This preserves lexicographic ordering because:
    - Non-zero bytes compare naturally
    - Escaped `0x00` bytes become `0x00 0xFF` which sorts after the terminator `0x00 0x00`, correctly placing strings containing NUL after shorter prefixes
    - The terminator ensures "abc" < "abcd" (terminator `0x00 0x00` < next char)

4.3. **Number encoding** (`encoding/number.rs`)
- Reject NaN: return `EncodingError::NaN` if `f64::is_nan()`
- Normalize negative zero: if `value.to_bits() == f64::NEG_ZERO.to_bits()`, use `0.0` instead
- Encode for byte ordering (8 bytes, big-endian):
  - `bits = f64::to_bits(value)`
  - If sign bit set (negative): flip ALL bits (`bits = !bits`)
  - If sign bit clear (positive/zero): flip ONLY sign bit (`bits ^= 1 << 63`)
  - Write as big-endian u64
- Decode: reverse the process
- Ordering verification: -inf < -1.0 < -0.0001 < 0.0 < 0.0001 < 1.0 < inf

4.4. **Binary encoding** (`encoding/binary.rs`)
- Same escaped-terminator scheme as strings:
  - `0x00` → `0x00 0xFF`, terminated with `0x00 0x00`
- Preserves lexicographic byte ordering

4.5. **Composite key encoding** (`encoding/composite.rs`)
- Partition key only: `[type_tag: u8][encoded_partition_key]`
- Partition + sort: `[type_tag: u8][encoded_partition_key][type_tag: u8][encoded_sort_key]`
- Type tags: `0x01` = String, `0x02` = Number, `0x03` = Binary
- Note: within a single table's B+Tree, all keys have the same type tags (enforced by schema). Cross-type comparison never occurs within one tree.
- The terminated encoding for strings/binary ensures unambiguous component boundaries
- Numbers are fixed 8 bytes so no terminator needed

4.6. **Decoding** (`encoding/mod.rs`)
- Decode composite key back to `(KeyValue, Option<KeyValue>)`
- Validate type tags match expected schema types
- Return `EncodingError` on malformed data

### Verification
- Unit tests: encode/decode round-trip for all types
- Unit tests: string ordering — "a" < "aa" < "ab" < "b" (lexicographic)
- Unit tests: string with NUL bytes — encode, decode, verify round-trip
- Unit tests: numeric ordering — -inf, -1.0, -0.0001, 0.0, 0.0001, 1.0, inf all sort correctly via `memcmp`
- Unit tests: negative zero normalized to positive zero
- Unit tests: NaN rejected
- Unit tests: binary ordering matches string ordering scheme
- Unit tests: composite key ordering — partition groups together, sort key orders within partition
- Property tests (proptest): random string pairs maintain ordering invariant after encoding

---

## Phase 5: B+Tree

The core index structure. Depends on Phase 3 (page manager) and Phase 4 (key encoding).

### Tasks

5.1. **B+Tree node types** (`btree/node.rs`)
- Internal node: slots contain `[encoded_key, child_page_id: u64]`, plus rightmost child pointer stored in page header reserved area
- Leaf node: slots contain `[encoded_key, value_bytes]`
  - Value is either inline document bytes or an overflow pointer
  - Overflow pointer: `[0x01 marker byte, overflow_start_page: u64, total_length: u32]` = 13 bytes
  - Inline document: `[0x00 marker byte, document_bytes...]`
- Use slotted page layout from Phase 2
- Leaf pages have `next_leaf: PageId` at byte 32 (8 bytes) for range scan traversal
- **Overflow threshold**: exact calculation:
  - Leaf usable space = `PAGE_SIZE - PAGE_HEADER_SIZE - 8 (next_leaf) - 2 * SLOT_SIZE` = `4096 - 32 - 8 - 8` = 4048 bytes
  - Minimum 2 entries per leaf (B+Tree invariant for splitting)
  - Max inline value size = `(4048 - 2 * min_key_size) / 2` — in practice, if key + value > ~2000 bytes, use overflow
  - Hard rule: any value > `PAGE_SIZE - PAGE_HEADER_SIZE - 8 - SLOT_SIZE - max_key_estimate` goes to overflow

5.2. **Search** (`btree/search.rs`)
- `search(root_page: PageId, key: &[u8]) -> Option<Vec<u8>>`
- Binary search within each node's slot array (compare encoded key bytes via `memcmp`)
- Follow child pointers for internal nodes
- Return value bytes from leaf node (resolving overflow chain if needed)

5.3. **Insert** (`btree/insert.rs`)
- Navigate to correct leaf page (via CoW — all pages on the path get copied)
- If leaf has space: insert into slotted page in sorted position
- If leaf is full: split
  - Allocate new leaf page via CoW
  - Move upper half of entries to new page
  - Update `next_leaf` pointers (old leaf → new leaf → old leaf's next)
  - Insert median key + new page pointer into parent (may cascade splits up to root)
  - If root splits: allocate new root internal page, update root pointer in caller

5.4. **Delete** (`btree/delete.rs`)
- Navigate to correct leaf page via CoW
- Mark slot as dead in slotted page (set offset = 0)
- **No rebalancing in v1**: dead slots stay until page is fully empty
- If ALL slots in a leaf are dead: free the page, remove key from parent, update sibling `next_leaf` pointers
- Parent cleanup: if an internal node loses its last key, collapse (simple case only)

5.5. **Range scan** (`btree/scan.rs`)
- `range_scan(root: PageId, start_key: &[u8], end_key: Option<&[u8]>, forward: bool) -> Iterator<(Vec<u8>, Vec<u8>)>`
- Navigate to start key in leaf level
- Scan forward through leaf pages via `next_leaf` pointers
- Skip dead slots
- Stop when key >= end bound (exclusive) or no more leaves
- Forward only in v1 (reverse scan would require `prev_leaf` pointers, defer)

5.6. **Overflow pages** (`btree/overflow.rs`)
- Write: split large value into chunks of `PAGE_SIZE - PAGE_HEADER_SIZE - 8 (next_overflow)` = 4056 bytes
- Each overflow page: `[page_header][next_overflow: u64][data bytes]`
- Read: follow chain starting from overflow pointer in leaf, concatenate data
- Free: when document is deleted/replaced, free all overflow pages in chain (add to pending-free)

### Verification
- Unit tests: insert single item, search it back
- Unit tests: insert enough items to trigger leaf split, verify all items still searchable
- Unit tests: insert enough to trigger multiple levels of splits (3+ levels)
- Unit tests: delete items, verify not found
- Unit tests: delete all items from a leaf, verify page freed
- Unit tests: range scan returns correct ordered subset
- Unit tests: range scan skips dead slots
- Unit tests: overflow — insert 400KB document, read it back correctly
- Stress test: insert 10,000 items, verify all searchable, delete half, verify remainder

---

## Phase 6: Table Catalog

Table metadata storage. The catalog itself is a B+Tree. Note: catalog operations are NOT yet transactional in this phase — they will be wrapped in transactions in Phase 7.

### Tasks

6.1. **Catalog B+Tree** (`catalog/mod.rs`)
- Stored in a dedicated B+Tree rooted at `catalog_root_page` from the header
- Key: table name encoded as a string key
- Value: serialized `CatalogEntry` (serde_json for simplicity in v1)
```rust
struct CatalogEntry {
    schema: TableSchema,
    data_root_page: PageId,
}
```

6.2. **Create table** (`catalog/ops.rs`)
- Check if table name already exists → `SchemaError::TableAlreadyExists`
- Allocate an empty leaf page as the new table's data B+Tree root
- Insert `CatalogEntry` into catalog B+Tree
- No schema evolution in v1 — partition/sort key types are immutable after creation

6.3. **Drop table** (`catalog/ops.rs`)
- Look up table in catalog → `SchemaError::TableNotFound` if missing
- Walk the table's data B+Tree and free all pages (leaf, internal, overflow) via pending-free
- Remove entry from catalog B+Tree

6.4. **List tables** (`catalog/ops.rs`)
- Full range scan of the catalog B+Tree
- Return `Vec<String>` (table names)

6.5. **Get table** (`catalog/ops.rs`)
- Lookup by table name in catalog B+Tree
- Return `CatalogEntry` (schema + data root page)

### Verification
- Unit tests: create table, list tables, verify schema matches
- Unit tests: create table that already exists → error
- Unit tests: drop table, verify it is gone
- Unit tests: get nonexistent table → error
- Unit tests: multiple tables coexist independently

---

## Phase 7: MVCC Transactions

Snapshot isolation, versioned documents, and garbage collection. Integration phase where all layers come together.

### Tasks

7.1. **Transaction manager** (`mvcc/manager.rs`)
- Single write lock: `parking_lot::Mutex<()>` — only one writer at a time
- Current committed txn ID: `parking_lot::RwLock<TxnId>`
- `begin_read() -> ReadTransaction`: captures current txn ID as snapshot, registers with snapshot tracker (RAII — deregisters on drop)
- `begin_write() -> WriteTransaction`: acquires write mutex, captures snapshot, creates CoW write buffer
- `commit(write_txn)`: increment txn counter, write all dirty pages via CoW manager, update header, release write mutex
- `abort(write_txn)`: discard write buffer, release write mutex, return allocated pages to pending-free

7.2. **Versioned document storage** (`mvcc/versioned.rs`)
```rust
struct VersionedDocument {
    created_txn: TxnId,
    deleted_txn: Option<TxnId>,
    data: Vec<u8>,  // serde_json serialized document
    prev_version_page: Option<PageId>,  // older version overflow chain
}
```
- **Latest version inline**: the B+Tree leaf stores the latest version's `VersionedDocument` as the value (or via overflow pointer if large)
- **Older versions**: when a put overwrites, the previous version is written to an overflow page and `prev_version_page` points to it
- **Single B+Tree entry per key**: no duplicate keys in the tree

7.3. **Visibility filter** (`mvcc/visibility.rs`)
- `is_visible(doc: &VersionedDocument, snapshot: TxnId) -> bool`
- Rule: `doc.created_txn <= snapshot && (doc.deleted_txn.is_none() || doc.deleted_txn.unwrap() > snapshot)`
- For reads: start at latest version in leaf. If not visible, follow `prev_version_page` chain until a visible version is found or chain ends.

7.4. **Write operations** (`mvcc/ops.rs`)
- `put(table, key, document)`:
  1. Search B+Tree for existing entry
  2. If exists: read current `VersionedDocument`, set its `deleted_txn = current_write_txn`, write it to an overflow page, create new `VersionedDocument` with `created_txn = current_write_txn` and `prev_version_page = overflow_page`, insert as leaf value
  3. If new: create `VersionedDocument` with `created_txn = current_write_txn`, `prev_version_page = None`, insert into B+Tree
- `delete(table, key)`:
  1. Search for visible version
  2. Set `deleted_txn = current_write_txn` on the latest version (update in place via CoW)
- `get(table, key, snapshot)`:
  1. Search B+Tree for key
  2. Walk version chain applying visibility filter
  3. Return first visible version's data, or None

7.5. **Auto-transaction wrapping**
- Direct API calls like `db.put_item(...)` outside an explicit transaction automatically:
  1. Begin a write transaction
  2. Perform the operation
  3. Commit on success, abort on error
- Direct reads like `db.get_item(...)` begin an implicit read transaction for the duration of the call

7.6. **Garbage collection** (`mvcc/gc.rs`)
- Triggered after each commit
- Check `oldest_active_snapshot()`
- Walk pending-free list, move reclaimable pages to on-disk free list
- Walk version chains: any version where `deleted_txn.is_some() && deleted_txn <= oldest_active_snapshot` can be physically removed (unlink from chain, free overflow page)
- GC batch limit: process at most 100 version removals per commit to avoid long pauses
- If no active snapshots: all deleted versions and pending-free pages can be immediately reclaimed

### Verification
- Unit tests: begin read txn, see consistent snapshot while writer modifies data
- Unit tests: two concurrent reads see same snapshot
- Unit tests: write txn commits, new read txn sees changes, old read txn still sees old data
- Unit tests: write txn aborts, data unchanged
- Unit tests: put overwrites — new reader sees new version, old reader sees old version
- Unit tests: delete — version invisible to new readers after commit
- Unit tests: version chain walk — reader with old snapshot follows chain to find visible version
- Unit tests: auto-transaction wrapping — put_item without explicit txn works correctly
- Unit tests: GC reclaims old versions after all readers finish
- Unit tests: GC respects active snapshots (does not reclaim versions still visible)
- Integration test: concurrent reader thread and writer thread operating correctly

---

## Phase 8: Public API

The user-facing Rust API. Facade over catalog + MVCC + B+Tree.

### Tasks

8.1. **Database handle** (`api/database.rs`)
```rust
pub struct DynaMite { /* inner: Arc<DatabaseInner> */ }

impl DynaMite {
    pub fn open(path: impl AsRef<Path>) -> Result<Self>;
    pub fn create_table(&self, name: &str) -> TableBuilder;
    pub fn drop_table(&self, name: &str) -> Result<()>;
    pub fn list_tables(&self) -> Result<Vec<String>>;
    pub fn put_item(&self, table: &str, document: Value) -> Result<()>;
    pub fn get_item(&self, table: &str) -> GetItemBuilder;
    pub fn delete_item(&self, table: &str) -> DeleteItemBuilder;
    pub fn query(&self, table: &str) -> QueryBuilder;
    pub fn scan(&self, table: &str) -> ScanBuilder;
    pub fn transact<F, R>(&self, f: F) -> Result<R>
    where F: FnOnce(&Transaction) -> Result<R>;
}
```
- `DynaMite` is `Clone` and `Send + Sync` (wraps `Arc<DatabaseInner>`)

8.2. **Builder pattern APIs** (`api/builders.rs`)
- `TableBuilder`: `.partition_key(name, type)`, `.sort_key(name, type)`, `.execute()`
- `GetItemBuilder`: `.partition_key(value)`, `.sort_key(value)`, `.execute()`
- `DeleteItemBuilder`: same as Get
- `QueryBuilder`: `.partition_key(value)`, `.sort_key_eq/gte/gt/lte/lt/between/begins_with(value)`, `.limit(n)`, `.scan_forward(bool)`, `.execute()`
- `ScanBuilder`: `.limit(n)`, `.execute()`
- All builders validate inputs and return typed errors

8.3. **Query execution** (`api/query.rs`)
- Sort key conditions: `=`, `<`, `>`, `<=`, `>=`, `between`, `begins_with` (string sort keys only)
- Translate conditions into B+Tree range scan bounds
- Pagination: `QueryResult` includes optional `last_evaluated_key` for continuation
  ```rust
  pub struct QueryResult {
      pub items: Vec<Value>,
      pub last_evaluated_key: Option<Value>,  // pass back to .exclusive_start_key() for next page
  }
  ```
- Scan: full table range scan with limit, returns same `QueryResult` structure

8.4. **Transaction handle** (`api/transaction.rs`)
- Exposes `get_item`, `put_item`, `delete_item` within a transaction scope
- Auto-commits on `Ok`, auto-aborts on `Err` (via `Result` return from closure)
- Document size validation: reject documents > 400KB with `EncodingError::DocumentTooLarge`

### Verification
- Integration tests (in `tests/` workspace directory): full CRUD lifecycle via public API
- Integration tests: query with various sort key conditions
- Integration tests: scan with limit and pagination
- Integration tests: transactional operations (read-modify-write)
- Integration tests: pagination via continuation keys
- Integration tests: error cases (table not found, key type mismatch, document too large)
- Integration tests: concurrent access from multiple threads via cloned `DynaMite` handle

---

## Phase 9: Hardening

Polish, performance, and robustness before v1.

### Tasks

9.1. **Crash recovery tests**
- Simulate crash mid-commit (write pages but don't write new header)
- Verify database opens correctly using previous valid header
- Verify no data corruption (checksums validate)

9.2. **Concurrency stress tests**
- Multiple reader threads + one writer thread, sustained workload
- Verify readers always see consistent snapshots
- Verify no panics, deadlocks, or data races
- Run under `cargo test` with `RUST_TEST_THREADS=1` and also with default parallelism

9.3. **Benchmarks** (criterion)
- Single-item put/get latency
- Bulk insert throughput (10K, 100K items)
- Range query throughput
- Concurrent read throughput (N reader threads)

9.4. **Linting and formatting**
- `cargo clippy --workspace -- -D warnings` passes
- `cargo fmt --all --check` passes
- No `unsafe` blocks without documented safety invariant comments

9.5. **Public API documentation**
- Rustdoc on all public types and methods
- Module-level doc comments explaining each layer
- Crate-level doc comment with usage example
- README.md with getting started example

---

## Dependency Graph

```
Phase 1 (Scaffolding)
  ├──→ Phase 2 (Storage Engine) ──→ Phase 3 (Page Manager) ─┐
  │                                                          ├──→ Phase 5 (B+Tree)
  └──→ Phase 4 (Key Encoding) ──────────────────────────────┘
                                                                    │
                                                              Phase 6 (Catalog)
                                                                    │
                                                              Phase 7 (MVCC)
                                                                    │
                                                              Phase 8 (API)
                                                                    │
                                                              Phase 9 (Hardening)
```

- Phases 2 and 4 can proceed in parallel after Phase 1
- Phase 5 requires both Phase 3 (page manager) and Phase 4 (key encoding)
- Phase 6 through 9 are sequential

---

## Revised File Format

```
Page 0:       Header A (double-buffered)
              [0..4]   magic: "DYNA"
              [4..8]   version: u32
              [8..12]  page_size: u32
              [12..20] txn_counter: u64
              [20..28] catalog_root_page: u64
              [28..36] free_list_head_page: u64
              [36..44] total_page_count: u64
              [44..52] xxhash64 checksum (of bytes 0..44)

Page 1:       Header B (double-buffered, same layout)

Page 2+:      Data pages (one of):
              BTreeInternal:
                [0..32]  common page header
                [32..]   slot_array→ ... ←cell_data: (encoded_key, child_page_id: u64)
                         rightmost child in reserved header area

              BTreeLeaf:
                [0..32]  common page header
                [32..40] next_leaf: u64
                [40..]   slot_array→ ... ←cell_data: (encoded_key, versioned_doc | overflow_ptr)

              Overflow:
                [0..32]  common page header
                [32..40] next_overflow: u64
                [40..4096] data bytes (up to 4056 bytes per page)

              FreeList (trunk):
                [0..32]  common page header
                [32..40] next_trunk: u64
                [40..4096] array of free PageId values (up to 507 entries)
```

## Revised Key Encoding

```
String/Binary encoding (escaped terminator):
  For each byte b in input:
    if b == 0x00: emit 0x00 0xFF
    else: emit b
  After all bytes: emit 0x00 0x00 (terminator)
  → preserves lexicographic memcmp ordering
  → "abc" < "abcd" because terminator 0x00 0x00 < next char

Number encoding (8 bytes, big-endian):
  bits = f64::to_bits(value)  // NaN rejected, -0 normalized
  if sign_bit_set: bits = !bits
  else: bits ^= 1 << 63
  → memcmp gives correct numeric ordering

Composite key:
  [type_tag: u8][encoded_partition_key][type_tag: u8][encoded_sort_key]
  Type tags: 0x01=String, 0x02=Number, 0x03=Binary
  String/Binary are self-terminating (0x00 0x00)
  Number is fixed 8 bytes
  → unambiguous parsing, no separator needed
```

## Deferred to v2

- Serializable isolation (predicate locks, SSI)
- Node.js bindings (napi-rs)
- Secondary indexes
- Scan filter expressions on arbitrary document attributes
- Reverse range scan (`prev_leaf` pointers)
- B+Tree rebalancing / merge on delete
- Configurable page size
- Schema evolution (add/remove sort key)
