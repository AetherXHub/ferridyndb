//! Table catalog: schema storage, create/drop/list operations.

pub mod ops;

use serde::{Deserialize, Serialize};

use crate::types::{IndexDefinition, PageId, PartitionSchema, TableSchema};

/// A catalog entry storing a table's schema, data B+Tree root, partition schemas,
/// and secondary index definitions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogEntry {
    pub schema: TableSchema,
    pub data_root_page: PageId,
    /// Declared partition schemas for entity types within this table.
    #[serde(default)]
    pub partition_schemas: Vec<PartitionSchema>,
    /// Secondary index definitions scoped to partition schemas.
    #[serde(default)]
    pub indexes: Vec<IndexDefinition>,
}
