# PRD: Vector / Embedding Index

**Priority:** 15
**Status:** In Progress (Phase 3 complete)
**Proposal:** [docs/proposals/accepted/vector-index.md](../proposals/accepted/vector-index.md)

## Summary

Add vector embedding storage and approximate nearest neighbor (ANN) search to FerridynDB using a pure Rust HNSW library as the indexing backend. Items can store float vectors as attributes; vector indexes enable efficient similarity search by cosine, euclidean, or dot product distance. Enables semantic search, recommendation, and hybrid queries (vector similarity + attribute filters).

FerridynDB manages document storage, index lifecycle, and persistence. The external library handles the HNSW graph construction and search algorithm.

This is the largest-scope feature in the roadmap and can be deferred. For small datasets (< 10k items), client-side similarity computation is viable as an interim solution.

## Scope

### In Scope

- Vector attribute storage: float arrays stored as JSON arrays, serialized as MessagePack
- `create_vector_index(table, name, attribute, dimensions, metric)` API
- HNSW-based ANN search via pure Rust dependency (`instant-distance` or `hnsw_rs`)
- `query_vector_index(table, index_name, vector, top_k, filter?)` returning scored results
- Three distance metrics: cosine similarity, euclidean distance, dot product
- Index persistence: serialize/deserialize HNSW graph to FerridynDB pages or sidecar file
- Index maintenance on put/update/delete (synchronous rebuild or incremental insert)
- Dimension validation on write (vector length must match index definition)
- Server protocol support

### Out of Scope

- Custom ANN algorithm implementation -- use external pure Rust crate
- Product quantization or scalar quantization -- store full float32 vectors initially
- Vector compression / dimensionality reduction
- Batch vector ingestion API
- GPU-accelerated similarity computation
- Multi-vector queries (query with multiple vectors)
- Vector attribute in secondary index projections

## Decisions

| Question | Decision | Rationale |
|----------|----------|-----------|
| ANN algorithm? | HNSW via external pure Rust crate | O(log n) query time; mature implementations available; no C/C++ build dependency |
| Library? | `hnsw_rs` (v0.3) | Pure Rust, actively maintained, HNSW with multiple metrics (cosine, euclidean, dot product), built-in serialization. `hora` was originally selected but is unmaintained (4+ years, v0.1.1). |
| Vector storage format? | Float32 array in MessagePack (documents) + serialized HNSW graph (index) | Documents in B+Tree as usual; HNSW graph persisted separately |
| Index persistence? | Serialize HNSW graph to dedicated pages or sidecar file | Must survive restart; rebuilt from documents on first open if missing (cold start fallback) |
| Dimension limit? | 4096 | Covers all common embedding models (OpenAI: 1536/3072, Cohere: 1024, etc.) |
| Sparse indexes? | Yes | Items without the vector attribute are not indexed |
| Filter integration? | Post-ANN filtering with oversampling | Simpler; pre-filtering requires specialized index structures |
| Score normalization? | Return raw metric value | Let callers interpret; cosine returns [-1, 1], euclidean returns [0, inf) |

## Data Structures

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorIndexDefinition {
    pub name: String,
    pub table: String,
    pub attribute: String,
    pub dimensions: u32,
    pub metric: VectorMetric,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum VectorMetric {
    Cosine,
    Euclidean,
    DotProduct,
}

#[derive(Debug, Clone)]
pub struct ScoredItem {
    pub item: Value,
    pub score: f64,
}
```

## External Dependency Evaluation

### Selected: `hnsw_rs` (v0.3)

| Crate | Algorithm | Pure Rust | Metrics | Stars | Notes |
|-------|-----------|-----------|---------|-------|-------|
| **`hnsw_rs`** | HNSW | Yes | 7+ types (via `anndists`) | 230 | **Selected** — actively maintained, excellent serialization, thread-safe, pure Rust |
| `hora` | HNSW, SSG, PQIVF, BruteForce | Yes | Cosine, Euclidean, Dot Product, Manhattan | 2,700 | Originally selected but unmaintained (4+ years, stuck at v0.1.1) |
| `instant-distance` | HNSW | Yes | Euclidean (trait-extensible) | 343 | Simpler but limited metric support and unclear serialization |
| `arroy` | Random projections | Yes | 4 types | 299 | LMDB-based persistence, but not HNSW (lower recall for high-dim) |

### Why hnsw_rs (changed from hora)

`hora` was originally selected but found to be unmaintained (4+ years, v0.1.1). `hnsw_rs` was chosen instead:

1. **Pure Rust** — no C/C++ FFI
2. **Actively maintained** — v0.3.x, regular updates
3. **All required metrics** — Cosine (`DistCosine`), Euclidean (`DistL2`), Dot Product (`DistDot`) via `anndists` crate
4. **Thread-safe** — `insert()` takes `&self` with internal synchronization
5. **Built-in serialization** — for future Phase 2 persistence
6. **Embeddable** — library crate, no server dependency

**Key difference:** `hnsw_rs` has no native delete. Handled with a deleted-IDs `HashSet` and 3x oversampling during search.

## Implementation Phases

### Phase 1: Index Creation, Storage, and Search

**Deliverables:**
- `VectorIndexDefinition` and `VectorMetric` types
- `FerridynDB::create_vector_index()` builder
- Vector index metadata stored in catalog (alongside secondary indexes)
- In-memory HNSW graph built via `hora`
- Write-path: extract vector attribute, validate dimensions, insert into HNSW graph
- `FerridynDB::query_vector_index()` builder with `.vector()`, `.top_k()`, `.execute()`
- Search delegates to `hora`, fetches full documents from B+Tree by primary key
- Distance metric mapping: FerridynDB's `VectorMetric` enum -> `hora`'s distance function

**Files:**
| File | Change |
|------|--------|
| `ferridyn-core/Cargo.toml` | Add `hora` dependency |
| `ferridyn-core/src/api/vector.rs` | New: vector index lifecycle, `hora` integration, query logic |
| `ferridyn-core/src/types.rs` | Add `VectorIndexDefinition`, `VectorMetric`, `ScoredItem` |
| `ferridyn-core/src/catalog/ops.rs` | Store/retrieve vector index definitions |
| `ferridyn-core/src/api/builders.rs` | Add `CreateVectorIndexBuilder`, `VectorQueryBuilder` |
| `ferridyn-core/src/api/database.rs` | Add `create_vector_index()`, `query_vector_index()`, write-path maintenance |
| `ferridyn-core/src/api/mod.rs` | Export vector module |

**Tests:**
- `test_create_vector_index`
- `test_vector_put_and_query_cosine` (insert items with vectors, query, verify ordering)
- `test_vector_put_and_query_euclidean`
- `test_vector_put_and_query_dot_product`
- `test_vector_dimension_mismatch` (wrong-length vector returns error)
- `test_vector_sparse_index` (items without vector attribute not indexed)
- `test_vector_top_k` (returns exactly k results when available)
- `test_vector_top_k_fewer_items` (returns all items when fewer than k)
- `test_vector_index_update` (update item vector, re-query reflects change)
- `test_vector_index_delete` (delete item, no longer in results)

### Phase 2: Index Persistence

**Deliverables:**
- Serialize HNSW graph to disk on database commit/close
- Deserialize HNSW graph on database open (warm start)
- Cold start fallback: if serialized graph is missing/corrupt, rebuild from all documents with vector attributes
- Storage options (choose one during implementation):
  - **Option A:** Dedicated pages in the database file (reuse page allocation)
  - **Option B:** Sidecar file (`<dbname>.vec`) alongside the main database file

**Tests:**
- `test_vector_index_persists_across_reopen` (close db, reopen, query returns same results)
- `test_vector_index_cold_start_rebuild` (delete serialized graph, reopen, index rebuilt)
- `test_vector_index_persistence_after_updates` (add items, close, reopen, new items searchable)

### Phase 3: Filter Integration

**Prerequisite:** PRD #2 (Filter Expressions)

**Deliverables:**
- `.filter()` method on `VectorQueryBuilder`
- Post-ANN filtering: query HNSW for `top_k * 3` candidates, apply filter, return up to `top_k` matching items
- Oversampling factor configurable via builder (default 3x)

**Tests:**
- `test_vector_query_with_filter` (filter reduces result set)
- `test_vector_query_filter_rejects_all` (empty result)

### Phase 4: Server Protocol Integration

**Deliverables:**
- `CreateVectorIndex` request/response variants
- `QueryVectorIndex` request/response variants (vector as float array, results include scores)
- `DropVectorIndex` request/response
- Server handlers forwarding to core API
- `FerridynClient` async methods

**Tests:**
- `test_vector_create_index_over_wire`
- `test_vector_query_over_wire`
- `test_vector_query_with_filter_over_wire`

## Acceptance Criteria

1. Vector attributes can be stored as float arrays in documents
2. Vector index can be created with specified dimensions and metric
3. `query_vector_index` returns top-k nearest neighbors with correct scores
4. Cosine, euclidean, and dot product metrics produce correct orderings
5. HNSW search is sub-linear (faster than brute-force for > 1000 items)
6. Dimension mismatches are caught at write time with clear errors
7. Index updates automatically when items are added/modified/deleted
8. Items without the vector attribute are silently skipped (sparse index)
9. Index persists across database close/reopen
10. No C/C++ build dependencies introduced
11. `cargo clippy --workspace -- -D warnings` clean

## Dependencies

- PRD #2 (Filter Expressions) -- for Phase 3 filter integration

## Dependents

- None

## Risk

This is architecturally the most complex new feature. Key risks:
- **Storage overhead:** Float32 vectors at 1536 dimensions = 6KB per vector, plus HNSW graph overhead (~20-50% additional). A 100k-item index = ~700-900MB total.
- **HNSW graph persistence:** Serializing/deserializing the graph adds complexity. Cold start rebuild may be slow for large indexes.
- **Write amplification:** Each put/update/delete modifies both the primary B+Tree and the HNSW graph.
- **Library stability:** `hora` dependency introduces maintenance risk. Mitigated by its 2.7k-star community, cross-language bindings, and wrapping behind an internal trait (allows swapping implementations later).

Recommend prototyping Phase 1 with `hora` first to validate integration, persistence model, and performance before committing to the full implementation.

## Documentation

**IMPORTANT:** Documentation MUST be updated to reflect the changes introduced by this PRD. This includes:

- **Root README** (`README.md`) — Feature list, code examples, design decisions
- **Server README** (`crates/ferridyn-server/README.md`) — Protocol examples, client usage, features list
- **CLAUDE.md** — Architecture description, workspace layout, test counts, API patterns

Documentation updates should be included in the same commit as the code or as an immediate follow-up commit.
