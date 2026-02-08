//! Public API: database handle, builder-pattern CRUD, query/scan, transactions, and batching.

pub mod batch;
pub mod builders;
pub mod database;
pub mod filter;
pub mod key_utils;
pub mod page_store;
pub mod projection;
pub mod query;
pub mod transaction;
pub mod update;

pub use batch::{BatchOp, SyncMode, WriteBatch};
pub use builders::{
    BatchGetItemBuilder, CreateIndexBuilder, DeleteItemBuilder, GetItemBuilder, IndexQueryBuilder,
    NoReturn, PartitionSchemaBuilder, PutItemBuilder, QueryBuilder, ReturnNew, ReturnOld,
    ScanBuilder, TableBuilder, UpdateItemBuilder,
};
pub use database::FerridynDB;
pub use filter::FilterExpr;
pub use query::{QueryResult, SortCondition};
pub use transaction::Transaction;
pub use update::UpdateAction;
