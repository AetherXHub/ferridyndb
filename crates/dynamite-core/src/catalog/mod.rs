//! Table catalog: schema storage, create/drop/list operations.

pub mod ops;

use serde::{Deserialize, Serialize};

use crate::types::{PageId, TableSchema};

/// A catalog entry storing a table's schema and its data B+Tree root.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogEntry {
    pub schema: TableSchema,
    pub data_root_page: PageId,
}
