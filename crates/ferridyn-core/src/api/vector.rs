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
            VectorMetric::Cosine => VectorGraph::Cosine(Hnsw::new(
                DEFAULT_MAX_NB_CONNECTION,
                DEFAULT_MAX_ELEMENTS,
                DEFAULT_MAX_LAYER,
                DEFAULT_EF_CONSTRUCTION,
                DistCosine {},
            )),
            VectorMetric::Euclidean => VectorGraph::Euclidean(Hnsw::new(
                DEFAULT_MAX_NB_CONNECTION,
                DEFAULT_MAX_ELEMENTS,
                DEFAULT_MAX_LAYER,
                DEFAULT_EF_CONSTRUCTION,
                DistL2 {},
            )),
            VectorMetric::DotProduct => VectorGraph::DotProduct(Hnsw::new(
                DEFAULT_MAX_NB_CONNECTION,
                DEFAULT_MAX_ELEMENTS,
                DEFAULT_MAX_LAYER,
                DEFAULT_EF_CONSTRUCTION,
                DistDot {},
            )),
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
        }

        let id = self.next_id;
        self.next_id += 1;

        self.graph.insert(vector, id);
        self.key_to_id.insert(primary_key.to_vec(), id);
        self.id_to_key.insert(id, primary_key.to_vec());

        Ok(())
    }

    /// Mark a primary key's vector as deleted.
    ///
    /// The point remains in the HNSW graph but is filtered out during search.
    pub(crate) fn remove(&mut self, primary_key: &[u8]) {
        if let Some(id) = self.key_to_id.remove(primary_key) {
            self.deleted_ids.insert(id);
            self.id_to_key.remove(&id);
        }
    }

    /// Search for the `top_k` nearest neighbors to the query vector.
    ///
    /// Returns `(primary_key_bytes, distance)` pairs sorted by distance.
    /// Oversamples by 3x to compensate for deleted entries that are filtered out.
    pub(crate) fn search(&self, query: &[f32], top_k: usize) -> Result<Vec<(Vec<u8>, f32)>, Error> {
        if query.len() != self.definition.dimensions as usize {
            return Err(SchemaError::VectorDimensionMismatch {
                expected: self.definition.dimensions,
                actual: query.len() as u32,
            }
            .into());
        }

        if self.graph.nb_point() == 0 {
            return Ok(Vec::new());
        }

        // Oversample to account for deleted entries.
        let total_points = self.graph.nb_point();
        let oversample = top_k.saturating_mul(3).max(top_k).max(total_points);
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
