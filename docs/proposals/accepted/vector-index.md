# Proposal: Vector / Embedding Index

**Status:** Accepted
**Author:** --
**Date:** 2026-02-06

## Motivation

FerridynDB supports exact key lookup, prefix-based sort key queries, and secondary indexes on discrete attribute values. None of these enable similarity search -- finding items conceptually related to a query even when keywords don't match.

Vector embeddings (dense float arrays produced by models like OpenAI's text-embedding-ada-002) capture semantic meaning. Storing embeddings alongside documents and supporting nearest-neighbor search would enable:

- **Semantic memory search** -- "find memories similar to this concept" without requiring exact keyword matches
- **Cost-efficient retrieval** -- vector similarity is orders of magnitude cheaper than LLM-powered search per query
- **Hybrid queries** -- combine vector similarity with attribute filters ("similar to X, in category Y, created this week")

For small datasets (< 10k items), client-side similarity computation is viable: fetch all vectors, compute distances locally. Database-level support becomes essential as datasets grow beyond what fits in client memory.

## Current State

- No vector storage support (vectors can be stored as JSON arrays in documents, but no indexing or search)
- No distance/similarity functions
- Secondary indexes only support discrete value matching (equality on a single attribute)

## Approach: Pure Rust HNSW Library

Rather than implementing ANN algorithms from scratch, FerridynDB will use an external pure Rust HNSW crate. This aligns with the project's zero-C/C++ dependency policy while getting battle-tested search performance out of the box.

### Why Not Build It?

HNSW is a complex algorithm involving multi-layer graph construction, neighbor selection heuristics, and careful tuning of parameters (ef_construction, M, ef_search). Mature libraries have years of optimization and testing. Building from scratch would be the single largest engineering effort in FerridynDB's roadmap with no differentiation -- the value is in the integration, not the algorithm.

### Why Not Use a C/C++ Library?

FerridynDB currently has zero C/C++ dependencies. Adding one (e.g., usearch, faiss) would complicate the build, require users to have a C++ toolchain, and make cross-compilation harder. Pure Rust keeps the build simple and portable.

### Candidate Libraries

| Crate | Algorithm | SIMD | Metrics | Serialization | Maintenance |
|-------|-----------|------|---------|---------------|-------------|
| `instant-distance` | HNSW | No | Euclidean (trait-extensible) | Yes (serde) | Moderate |
| `hnsw_rs` | HNSW | Optional (`packed_simd`) | Cosine, euclidean, dot, custom | Yes (custom binary) | Active |

Both candidates support the core requirements. `hnsw_rs` has built-in multi-metric support and optional SIMD, making it the likely front-runner. `instant-distance` has a simpler API but would require implementing cosine/dot product as custom distance traits.

**Decision:** Evaluate both during prototyping. Wrap behind an internal trait so the implementation can be swapped later without API changes.

## Proposed Design

### Index Creation

```rust
db.create_vector_index("memories", "semantic-idx")
    .attribute("embedding")
    .dimensions(1536)
    .metric(VectorMetric::Cosine)
    .execute()?;
```

### Storing Vectors

Vectors are regular document attributes -- no special storage API needed:

```rust
db.put_item("memories", json!({
    "category": "notes",
    "key": "rust-ownership",
    "content": "Rust uses an ownership model...",
    "embedding": [0.012, -0.034, 0.056, ...]  // 1536 floats
}))?;
```

When a document with a matching vector attribute is written to a table with a vector index, the HNSW graph is updated automatically.

### Querying

```rust
let results = db.query_vector_index("memories", "semantic-idx")
    .vector(&query_embedding)  // Vec<f32>
    .top_k(10)
    .execute()?;

for scored in &results {
    println!("{}: score={:.4}", scored.item["key"], scored.score);
}
```

### Distance Metrics

| Metric | Range | Best For |
|--------|-------|----------|
| Cosine | [-1, 1] (1 = most similar) | Text embeddings (normalized vectors) |
| Euclidean | [0, inf) (0 = identical) | Spatial data, unnormalized vectors |
| Dot Product | (-inf, inf) (higher = more similar) | Recommendation systems |

### Architecture

```
FerridynDB (orchestration layer)
├── Document storage: B+Tree (existing)
├── Vector index metadata: Catalog (existing)
├── Vector index graph: HNSW crate (new dependency)
│   ├── In-memory graph for search
│   └── Serialized to disk for persistence
└── Integration glue:
    ├── Write path: extract vector → insert into HNSW
    ├── Delete path: remove from HNSW
    ├── Query path: HNSW search → fetch docs from B+Tree
    └── Persistence: serialize/deserialize on close/open
```

### Persistence Model

The HNSW graph lives in memory during operation. On database close (or periodic checkpoint), the graph is serialized to:

**Option A: Sidecar file** -- `<dbname>.vec` alongside the main `.db` file. Simpler; doesn't affect the copy-on-write page model.

**Option B: Dedicated pages** -- Store serialized graph in database pages. Single-file model preserved, but complicates page management (large contiguous allocations).

**Recommendation:** Start with Option A (sidecar file). Single-file purism can be addressed later if needed.

**Cold start fallback:** If the sidecar file is missing or corrupt, rebuild the HNSW graph from all documents in the table that have the vector attribute. This is O(n * log n) and may be slow for large datasets, but ensures correctness.

### Storage Overhead

**Per vector (1536 dimensions):**
- Vector data in document: 1536 * 4 bytes = 6,144 bytes (in B+Tree as MessagePack)
- HNSW graph node: ~200-400 bytes (neighbor lists, metadata)
- Total: ~6.5 KB per indexed item

**HNSW graph (1536 dims, 10k items):**
- In-memory: ~3-4 MB
- Serialized: ~3-4 MB

For the typical ferridyn-memory use case (< 10k items), overhead is modest.

## Trade-offs

| Dimension | Impact |
|-----------|--------|
| Storage | Moderate -- sidecar file adds ~4MB per 10k vectors |
| Write latency | Low -- HNSW insert is O(log n) |
| Query latency | Excellent -- HNSW search is O(log n), sub-millisecond for 10k items |
| Build complexity | None -- pure Rust dependency, standard `cargo build` |
| External dependency | 1 new crate -- wrapped behind internal trait for replaceability |
| Complexity | Moderate -- integration layer + persistence, but algorithm is external |

## Open Questions (Resolved)

### 1. Should we defer to a specialized vector DB?

**Resolved: No.** FerridynDB's value is single-file, zero-infrastructure. Adding a separate vector DB server defeats the purpose. An embedded pure Rust library gives us the best of both worlds.

### 2. HNSW vs brute-force?

**Resolved: HNSW from the start.** Using an external library means HNSW is no harder to implement than brute-force. No reason to start with the slower approach.

### 3. Pure Rust vs C++ bindings?

**Resolved: Pure Rust.** Keeps the build simple, maintains zero-C/C++ policy.

### 4. Pre-filtering vs post-filtering?

Post-filtering (HNSW search for top-k * 3, then filter) for v1. Pre-filtering would require maintaining separate HNSW graphs per filter partition, which is not worth the complexity initially.

## Summary

Vector indexing integrates a pure Rust HNSW library into FerridynDB's existing document storage model. FerridynDB handles documents, persistence, and API; the external crate handles the ANN algorithm. This division of responsibility keeps implementation tractable while delivering production-quality search performance. The internal trait boundary ensures the HNSW implementation can be swapped later without API changes.
