//! In-memory HNSW vector index for approximate nearest neighbor search.
//!
//! Each `VectorIndexState` wraps an `hnsw_rs::Hnsw` graph with bidirectional
//! ID mappings between primary key bytes and HNSW point IDs. Since `hnsw_rs`
//! has no native delete, we track deleted IDs in a `HashSet` and filter them
//! during search (oversampling to compensate).

use std::collections::{HashMap, HashSet};

use anndists::dist::distances::{DistCosine, DistDot, DistL2};
use hnsw_rs::hnsw::{Hnsw, Neighbour};

use crate::error::{Error, SchemaError};
use crate::types::{VectorIndexDefinition, VectorMetric};

/// Below this threshold, use brute-force search instead of HNSW.
/// HNSW is probabilistic and can miss points with very small datasets.
const BRUTE_FORCE_THRESHOLD: usize = 100;

/// Compute the distance between two vectors using the specified metric.
///
/// Returns the same distance values as the corresponding `anndists` types
/// used by HNSW: cosine distance (1 - cosine_similarity), squared L2, or
/// dot-product distance (1 - dot).
fn compute_distance(a: &[f32], b: &[f32], metric: VectorMetric) -> f32 {
    match metric {
        VectorMetric::Cosine => {
            let mut dot = 0.0f32;
            let mut norm_a = 0.0f32;
            let mut norm_b = 0.0f32;
            for (x, y) in a.iter().zip(b.iter()) {
                dot += x * y;
                norm_a += x * x;
                norm_b += y * y;
            }
            let denom = norm_a.sqrt() * norm_b.sqrt();
            if denom == 0.0 { 1.0 } else { 1.0 - dot / denom }
        }
        VectorMetric::Euclidean => {
            let mut sum = 0.0f32;
            for (x, y) in a.iter().zip(b.iter()) {
                let d = x - y;
                sum += d * d;
            }
            sum
        }
        VectorMetric::DotProduct => {
            let mut dot = 0.0f32;
            for (x, y) in a.iter().zip(b.iter()) {
                dot += x * y;
            }
            1.0 - dot
        }
    }
}

/// Default HNSW construction parameters.
const DEFAULT_MAX_NB_CONNECTION: usize = 16;
const DEFAULT_MAX_ELEMENTS: usize = 10_000;
const DEFAULT_MAX_LAYER: usize = 16;
const DEFAULT_EF_CONSTRUCTION: usize = 200;
/// ef_search parameter for queries.
const DEFAULT_EF_SEARCH: usize = 64;

/// Enum wrapping `Hnsw` with different distance metrics.
///
/// We use `'static` lifetime because vectors are copied into the graph on
/// insert (not borrowed), so no external lifetime is needed.
enum VectorGraph {
    Cosine(Hnsw<'static, f32, DistCosine>),
    Euclidean(Hnsw<'static, f32, DistL2>),
    DotProduct(Hnsw<'static, f32, DistDot>),
}

impl VectorGraph {
    fn new(metric: VectorMetric) -> Self {
        match metric {
            VectorMetric::Cosine => {
                let mut h = Hnsw::new(
                    DEFAULT_MAX_NB_CONNECTION,
                    DEFAULT_MAX_ELEMENTS,
                    DEFAULT_MAX_LAYER,
                    DEFAULT_EF_CONSTRUCTION,
                    DistCosine {},
                );
                h.set_keeping_pruned(true);
                VectorGraph::Cosine(h)
            }
            VectorMetric::Euclidean => {
                let mut h = Hnsw::new(
                    DEFAULT_MAX_NB_CONNECTION,
                    DEFAULT_MAX_ELEMENTS,
                    DEFAULT_MAX_LAYER,
                    DEFAULT_EF_CONSTRUCTION,
                    DistL2 {},
                );
                h.set_keeping_pruned(true);
                VectorGraph::Euclidean(h)
            }
            VectorMetric::DotProduct => {
                let mut h = Hnsw::new(
                    DEFAULT_MAX_NB_CONNECTION,
                    DEFAULT_MAX_ELEMENTS,
                    DEFAULT_MAX_LAYER,
                    DEFAULT_EF_CONSTRUCTION,
                    DistDot {},
                );
                h.set_keeping_pruned(true);
                VectorGraph::DotProduct(h)
            }
        }
    }

    fn insert(&self, vector: &[f32], id: usize) {
        match self {
            VectorGraph::Cosine(h) => h.insert((vector, id)),
            VectorGraph::Euclidean(h) => h.insert((vector, id)),
            VectorGraph::DotProduct(h) => h.insert((vector, id)),
        }
    }

    fn search(&self, query: &[f32], ef_search: usize, top_k: usize) -> Vec<Neighbour> {
        match self {
            VectorGraph::Cosine(h) => h.search(query, top_k, ef_search),
            VectorGraph::Euclidean(h) => h.search(query, top_k, ef_search),
            VectorGraph::DotProduct(h) => h.search(query, top_k, ef_search),
        }
    }

    fn nb_point(&self) -> usize {
        match self {
            VectorGraph::Cosine(h) => h.get_nb_point(),
            VectorGraph::Euclidean(h) => h.get_nb_point(),
            VectorGraph::DotProduct(h) => h.get_nb_point(),
        }
    }
}

/// In-memory state for a single vector index.
pub(crate) struct VectorIndexState {
    /// The HNSW graph (handles different distance metrics via enum).
    graph: VectorGraph,
    /// Definition from the catalog.
    pub(crate) definition: VectorIndexDefinition,
    /// Map: primary key bytes → HNSW point ID.
    key_to_id: HashMap<Vec<u8>, usize>,
    /// Map: HNSW point ID → primary key bytes.
    id_to_key: HashMap<usize, Vec<u8>>,
    /// Set of deleted HNSW point IDs (filtered during search).
    deleted_ids: HashSet<usize>,
    /// Next HNSW point ID to assign.
    next_id: usize,
    /// Cached vector data for persistence (hnsw_rs doesn't expose stored vectors).
    vectors: HashMap<usize, Vec<f32>>,
}

impl VectorIndexState {
    /// Create a new empty HNSW graph for the given vector index definition.
    pub(crate) fn new(definition: VectorIndexDefinition) -> Self {
        let graph = VectorGraph::new(definition.metric);
        Self {
            graph,
            definition,
            key_to_id: HashMap::new(),
            id_to_key: HashMap::new(),
            deleted_ids: HashSet::new(),
            next_id: 0,
            vectors: HashMap::new(),
        }
    }

    /// Insert a vector associated with the given primary key bytes.
    ///
    /// Validates that the vector has the correct number of dimensions.
    /// If the key already exists, the old entry is marked as deleted first.
    pub(crate) fn insert(&mut self, primary_key: &[u8], vector: &[f32]) -> Result<(), Error> {
        if vector.len() != self.definition.dimensions as usize {
            return Err(SchemaError::VectorDimensionMismatch {
                expected: self.definition.dimensions,
                actual: vector.len() as u32,
            }
            .into());
        }

        // If this key already exists, mark the old point as deleted.
        if let Some(&old_id) = self.key_to_id.get(primary_key) {
            self.deleted_ids.insert(old_id);
            self.id_to_key.remove(&old_id);
            self.vectors.remove(&old_id);
        }

        let id = self.next_id;
        self.next_id += 1;

        self.graph.insert(vector, id);
        self.key_to_id.insert(primary_key.to_vec(), id);
        self.id_to_key.insert(id, primary_key.to_vec());
        self.vectors.insert(id, vector.to_vec());

        Ok(())
    }

    /// Mark a primary key's vector as deleted.
    ///
    /// The point remains in the HNSW graph but is filtered out during search.
    pub(crate) fn remove(&mut self, primary_key: &[u8]) {
        if let Some(id) = self.key_to_id.remove(primary_key) {
            self.deleted_ids.insert(id);
            self.id_to_key.remove(&id);
            self.vectors.remove(&id);
        }
    }

    /// Search for the `top_k` nearest neighbors to the query vector.
    ///
    /// Returns `(primary_key_bytes, distance)` pairs sorted by distance.
    /// Uses brute-force for small datasets (< 100 live points) to guarantee
    /// correct results, and HNSW with oversampling for larger datasets.
    pub(crate) fn search(&self, query: &[f32], top_k: usize) -> Result<Vec<(Vec<u8>, f32)>, Error> {
        if query.len() != self.definition.dimensions as usize {
            return Err(SchemaError::VectorDimensionMismatch {
                expected: self.definition.dimensions,
                actual: query.len() as u32,
            }
            .into());
        }

        let live_count = self.id_to_key.len();
        if live_count == 0 {
            return Ok(Vec::new());
        }

        // For small datasets, compute distances directly for guaranteed correctness.
        if live_count < BRUTE_FORCE_THRESHOLD {
            return self.brute_force_search(query, top_k);
        }

        // HNSW search with oversampling for larger datasets.
        let total_points = self.graph.nb_point();
        let oversample = top_k.saturating_mul(3).max(total_points);
        let ef_search = DEFAULT_EF_SEARCH.max(oversample).max(total_points);

        let neighbours = self.graph.search(query, ef_search, oversample);

        let mut results = Vec::with_capacity(top_k);
        for n in neighbours {
            let origin_id = n.get_origin_id();
            if self.deleted_ids.contains(&origin_id) {
                continue;
            }
            if let Some(pk_bytes) = self.id_to_key.get(&origin_id) {
                results.push((pk_bytes.clone(), n.get_distance()));
            }
            if results.len() >= top_k {
                break;
            }
        }

        // Results from hnsw_rs are already sorted by distance.
        Ok(results)
    }

    /// Brute-force search: compute distance from query to every live vector.
    fn brute_force_search(
        &self,
        query: &[f32],
        top_k: usize,
    ) -> Result<Vec<(Vec<u8>, f32)>, Error> {
        let mut scored: Vec<(Vec<u8>, f32)> = Vec::with_capacity(self.id_to_key.len());
        for (&id, pk_bytes) in &self.id_to_key {
            if self.deleted_ids.contains(&id) {
                continue;
            }
            if let Some(vector) = self.vectors.get(&id) {
                let dist = compute_distance(query, vector, self.definition.metric);
                scored.push((pk_bytes.clone(), dist));
            }
        }
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(top_k);
        Ok(scored)
    }
}

/// Pending vector operation collected during a transaction.
///
/// These are applied to the in-memory HNSW graphs *after* the transaction
/// commits successfully.
#[derive(Debug)]
pub(crate) enum VectorOp {
    Insert {
        table: String,
        index_name: String,
        primary_key: Vec<u8>,
        vector: Vec<f32>,
    },
    Remove {
        table: String,
        index_name: String,
        primary_key: Vec<u8>,
    },
}

/// Extract a vector (array of numbers) from a document attribute.
///
/// Returns `None` if the attribute is missing. Returns an error if the
/// attribute exists but is not an array of numbers.
pub(crate) fn extract_vector(
    doc: &serde_json::Value,
    attribute: &str,
) -> Result<Option<Vec<f32>>, Error> {
    let val = match doc.get(attribute) {
        Some(v) => v,
        None => return Ok(None),
    };

    let arr = val
        .as_array()
        .ok_or_else(|| SchemaError::VectorAttributeNotArray(attribute.to_string()))?;

    let mut vector = Vec::with_capacity(arr.len());
    for elem in arr {
        let n = elem
            .as_f64()
            .ok_or_else(|| SchemaError::VectorAttributeNotArray(attribute.to_string()))?;
        vector.push(n as f32);
    }

    Ok(Some(vector))
}

// ── Sidecar persistence types ───────────────────────────────────────────

use serde::{Deserialize, Serialize};

/// Magic bytes identifying a vector sidecar file.
pub(crate) const VECTOR_SIDECAR_MAGIC: [u8; 4] = *b"FVEC";
/// Current sidecar format version.
pub(crate) const VECTOR_SIDECAR_VERSION: u32 = 1;

/// Top-level snapshot of all vector indexes across all tables.
#[derive(Serialize, Deserialize)]
pub(crate) struct VectorSnapshot {
    pub magic: [u8; 4],
    pub version: u32,
    pub txn_counter: u64,
    pub tables: Vec<TableVectorSnapshot>,
}

/// Per-table collection of vector index snapshots.
#[derive(Serialize, Deserialize)]
pub(crate) struct TableVectorSnapshot {
    pub table_name: String,
    pub indexes: Vec<IndexVectorSnapshot>,
}

/// Snapshot of a single vector index (definition + all live entries).
#[derive(Serialize, Deserialize)]
pub(crate) struct IndexVectorSnapshot {
    pub index_name: String,
    pub definition: VectorIndexDefinition,
    pub next_id: usize,
    pub entries: Vec<VectorEntry>,
}

/// A single live vector entry in the snapshot.
#[derive(Serialize, Deserialize)]
pub(crate) struct VectorEntry {
    pub id: usize,
    pub primary_key: Vec<u8>,
    pub vector: Vec<f32>,
}

impl VectorIndexState {
    /// Serialize this index state into a snapshot (only live entries).
    pub(crate) fn to_snapshot(&self, index_name: &str) -> IndexVectorSnapshot {
        let mut entries = Vec::with_capacity(self.id_to_key.len());
        for (&id, pk_bytes) in &self.id_to_key {
            if self.deleted_ids.contains(&id) {
                continue;
            }
            if let Some(vector) = self.vectors.get(&id) {
                entries.push(VectorEntry {
                    id,
                    primary_key: pk_bytes.clone(),
                    vector: vector.clone(),
                });
            }
        }
        IndexVectorSnapshot {
            index_name: index_name.to_string(),
            definition: self.definition.clone(),
            next_id: self.next_id,
            entries,
        }
    }

    /// Reconstruct a `VectorIndexState` from a snapshot.
    ///
    /// Creates a fresh HNSW graph and re-inserts all persisted vectors
    /// with their original IDs.
    pub(crate) fn from_snapshot(snapshot: IndexVectorSnapshot) -> Result<Self, Error> {
        let graph = VectorGraph::new(snapshot.definition.metric);
        let mut key_to_id = HashMap::with_capacity(snapshot.entries.len());
        let mut id_to_key = HashMap::with_capacity(snapshot.entries.len());
        let mut vectors = HashMap::with_capacity(snapshot.entries.len());

        for entry in &snapshot.entries {
            graph.insert(&entry.vector, entry.id);
            key_to_id.insert(entry.primary_key.clone(), entry.id);
            id_to_key.insert(entry.id, entry.primary_key.clone());
            vectors.insert(entry.id, entry.vector.clone());
        }

        Ok(Self {
            graph,
            definition: snapshot.definition,
            key_to_id,
            id_to_key,
            deleted_ids: HashSet::new(),
            next_id: snapshot.next_id,
            vectors,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::VectorMetric;

    #[test]
    fn test_vector_snapshot_roundtrip() {
        let def = VectorIndexDefinition {
            name: "emb-idx".to_string(),
            attribute: "embedding".to_string(),
            dimensions: 3,
            metric: VectorMetric::Euclidean,
        };
        let mut state = VectorIndexState::new(def);

        // Insert 3 vectors.
        state.insert(b"key-a", &[1.0, 0.0, 0.0]).unwrap();
        state.insert(b"key-b", &[0.0, 1.0, 0.0]).unwrap();
        state.insert(b"key-c", &[0.0, 0.0, 1.0]).unwrap();

        // Snapshot → from_snapshot roundtrip.
        let snapshot = state.to_snapshot("emb-idx");
        assert_eq!(snapshot.entries.len(), 3);
        assert_eq!(snapshot.next_id, 3);

        let restored = VectorIndexState::from_snapshot(snapshot).unwrap();

        // Validate key mappings are identical.
        assert_eq!(restored.key_to_id.len(), 3);
        assert!(restored.key_to_id.contains_key(b"key-a".as_slice()));
        assert!(restored.key_to_id.contains_key(b"key-b".as_slice()));
        assert!(restored.key_to_id.contains_key(b"key-c".as_slice()));
        assert_eq!(restored.vectors.len(), 3);
        assert_eq!(restored.deleted_ids.len(), 0);
        assert_eq!(restored.next_id, 3);

        // Verify vectors are preserved.
        for (pk, &id) in &restored.key_to_id {
            let vec = restored.vectors.get(&id).unwrap();
            let orig_id = state.key_to_id.get(pk.as_slice()).unwrap();
            let orig_vec = state.vectors.get(orig_id).unwrap();
            assert_eq!(vec, orig_vec);
        }

        // Verify search returns results (may be approximate, so just check
        // the nearest neighbor is correct for euclidean distance).
        let results = restored.search(&[1.0, 0.0, 0.0], 1).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, b"key-a"); // exact match should be closest
    }

    #[test]
    fn test_vector_snapshot_excludes_deleted() {
        let def = VectorIndexDefinition {
            name: "emb-idx".to_string(),
            attribute: "embedding".to_string(),
            dimensions: 2,
            metric: VectorMetric::Euclidean,
        };
        let mut state = VectorIndexState::new(def);

        state.insert(b"key-a", &[1.0, 0.0]).unwrap();
        state.insert(b"key-b", &[0.0, 1.0]).unwrap();
        state.remove(b"key-a");

        let snapshot = state.to_snapshot("emb-idx");
        // Only key-b should be in the snapshot.
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].primary_key, b"key-b");

        // Restored state should only find key-b.
        let restored = VectorIndexState::from_snapshot(snapshot).unwrap();
        let results = restored.search(&[0.0, 0.0], 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, b"key-b");
    }
}
