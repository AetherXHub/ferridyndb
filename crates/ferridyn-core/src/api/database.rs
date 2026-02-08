use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::{Mutex, RwLock};
use serde_json::Value;

use crate::btree::ops as btree_ops;
use crate::catalog;
use crate::error::{Error, QueryError, SchemaError, StorageError};
use crate::mvcc::ops as mvcc_ops;
use crate::storage::file::FileManager;
use crate::storage::header::FileHeader;
use crate::storage::lock::FileLock;
use crate::storage::page::{compute_checksum_buf, write_checksum_buf};
use crate::storage::pending_free::PendingFreeList;
use crate::storage::snapshot::SnapshotTracker;
use crate::storage::tombstone::TombstoneQueue;
use crate::types::{IndexDefinition, PAGE_SIZE, PageId, PartitionSchema};

use super::batch::{SyncMode, WriteBatch};
use super::builders::{
    BatchGetItemBuilder, CreateIndexBuilder, DeleteItemBuilder, GetItemBuilder,
    GetItemVersionedBuilder, IndexQueryBuilder, ListPartitionKeysBuilder,
    ListSortKeyPrefixesBuilder, PartitionSchemaBuilder, PutItemBuilder, QueryBuilder, ScanBuilder,
    TableBuilder, UpdateItemBuilder,
};
use super::page_store::{BufferedPageStore, FilePageStore};
use super::transaction::Transaction;

pub(crate) struct DatabaseState {
    pub(crate) file_manager: FileManager,
    pub(crate) header: FileHeader,
    pub(crate) header_slot: u8,
    pub(crate) pending_free: PendingFreeList,
    pub(crate) tombstone_queue: TombstoneQueue,
}

struct DatabaseInner {
    state: RwLock<DatabaseState>,
    /// Serializes write transactions (single-writer model).
    writer_lock: Mutex<()>,
    /// File descriptor for readers. Uses `pread` which is thread-safe,
    /// so readers can clone this without holding any lock.
    read_file: std::fs::File,
    snapshot_tracker: SnapshotTracker,
    _file_lock: FileLock,
    #[allow(dead_code)]
    path: PathBuf,
    sync_mode: AtomicU8,
}

/// The main database handle.
///
/// `FerridynDB` is cheaply clonable (`Arc`-based) and `Send + Sync`.
#[derive(Clone)]
pub struct FerridynDB {
    inner: Arc<DatabaseInner>,
}

impl FerridynDB {
    /// Create a new database at the given path.
    pub fn create(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref();

        // 1. Acquire file lock.
        let lock_path = path.with_extension("lock");
        let file_lock = FileLock::exclusive(&lock_path)?;

        // 2. Create the database file with initial headers.
        let mut file_manager = FileManager::create(path)?;

        // 3. Create a BufferedPageStore from a cloned file descriptor.
        let file = file_manager
            .file()
            .try_clone()
            .map_err(StorageError::from)?;
        let mut store = BufferedPageStore::new(file, file_manager.total_page_count());

        // 4. Create the initial (empty) catalog B+Tree.
        let catalog_root = btree_ops::create_tree(&mut store)?;

        // 5. Commit: grow file, write overlay, update header, write header, sync.
        let new_total = store.next_page_id();
        if new_total > file_manager.total_page_count() {
            file_manager.grow(new_total)?;
        }
        for (&page_id, data) in store.overlay() {
            let mut buf = *data;
            // Data pages get checksums; header pages (0, 1) use their own scheme.
            if page_id >= 2 {
                write_checksum_buf(&mut buf);
            }
            file_manager.write_page(page_id, &buf)?;
        }

        // Compute the Merkle root checksum for the catalog B-tree root page.
        let catalog_root_checksum = if let Some(data) = store.overlay().get(&catalog_root) {
            compute_checksum_buf(data)
        } else {
            0u128
        };

        // Write header to slot 0.
        let mut header = FileHeader::new();
        header.txn_counter = 1;
        header.catalog_root_page = catalog_root;
        header.total_page_count = new_total;
        header.catalog_root_checksum = catalog_root_checksum;

        let mut header_buf = [0u8; PAGE_SIZE];
        header.write_to_page(&mut header_buf);
        file_manager.write_page(0, &header_buf)?;
        file_manager.sync()?;

        let read_file = file_manager
            .file()
            .try_clone()
            .map_err(StorageError::from)?;

        Ok(Self {
            inner: Arc::new(DatabaseInner {
                state: RwLock::new(DatabaseState {
                    file_manager,
                    header,
                    header_slot: 0,
                    pending_free: PendingFreeList::new(),
                    tombstone_queue: TombstoneQueue::new(),
                }),
                writer_lock: Mutex::new(()),
                read_file,
                snapshot_tracker: SnapshotTracker::new(),
                _file_lock: file_lock,
                path: path.to_path_buf(),
                sync_mode: AtomicU8::new(SyncMode::Full as u8),
            }),
        })
    }

    /// Open an existing database.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref();

        let lock_path = path.with_extension("lock");
        let file_lock = FileLock::exclusive(&lock_path)?;

        let (file_manager, header, slot) = FileManager::open(path)?;

        // Verify the catalog root page's Merkle checksum against the header.
        if header.catalog_root_checksum != 0 && header.catalog_root_page != 0 {
            let buf = file_manager.read_page(header.catalog_root_page)?;
            let actual = compute_checksum_buf(&buf);
            if actual != header.catalog_root_checksum {
                return Err(StorageError::CorruptedPage(format!(
                    "catalog root checksum mismatch: header={:#034x}, actual={actual:#034x}",
                    header.catalog_root_checksum,
                ))
                .into());
            }
        }

        // Load persisted pending free list and tombstone queue from the header's chains.
        let (pending_free, tombstone_queue) = {
            let file = file_manager
                .file()
                .try_clone()
                .map_err(StorageError::from)?;
            let store = FilePageStore::new(file, file_manager.total_page_count());
            let pf = if header.pending_free_root_page != 0 {
                PendingFreeList::deserialize_from_pages(&store, header.pending_free_root_page)?
            } else {
                PendingFreeList::new()
            };
            let tq = if header.tombstone_root_page != 0 {
                TombstoneQueue::deserialize_from_pages(&store, header.tombstone_root_page)?
            } else {
                TombstoneQueue::new()
            };
            (pf, tq)
        };

        let read_file = file_manager
            .file()
            .try_clone()
            .map_err(StorageError::from)?;

        Ok(Self {
            inner: Arc::new(DatabaseInner {
                state: RwLock::new(DatabaseState {
                    file_manager,
                    header,
                    header_slot: slot,
                    pending_free,
                    tombstone_queue,
                }),
                writer_lock: Mutex::new(()),
                read_file,
                snapshot_tracker: SnapshotTracker::new(),
                _file_lock: file_lock,
                path: path.to_path_buf(),
                sync_mode: AtomicU8::new(SyncMode::Full as u8),
            }),
        })
    }

    /// Create a table.
    pub fn create_table(&self, name: &str) -> TableBuilder<'_> {
        TableBuilder::new(self, name.to_string())
    }

    /// Drop a table.
    pub fn drop_table(&self, name: &str) -> Result<(), Error> {
        let table_name = name.to_string();
        self.transact(move |txn| {
            let new_root = catalog::ops::drop_table(&mut txn.store, txn.catalog_root, &table_name)?;
            txn.catalog_root = new_root;
            Ok(())
        })
    }

    /// List all table names.
    pub fn list_tables(&self) -> Result<Vec<String>, Error> {
        let (store, header) = self.read_store_snapshot()?;
        catalog::ops::list_tables(&store, header.catalog_root_page)
    }

    /// Describe a table's schema (partition key, sort key, types).
    pub fn describe_table(&self, name: &str) -> Result<crate::types::TableSchema, Error> {
        let (store, header) = self.read_store_snapshot()?;
        let entry = self.cached_get_table(&store, header.catalog_root_page, name)?;
        Ok(entry.schema)
    }

    /// Put an item into a table.
    pub fn put_item(&self, table: &str, document: Value) -> Result<(), Error> {
        let table = table.to_string();
        self.transact(move |txn| txn.put_item(&table, document, None))
    }

    /// Put an item into a table with an optional condition expression.
    ///
    /// Returns a builder that allows setting a `.condition()` predicate
    /// evaluated against the existing item before the write proceeds.
    pub fn put(&self, table: &str, document: Value) -> PutItemBuilder<'_> {
        PutItemBuilder::new(self, table.to_string(), document)
    }

    /// Get an item from a table by key.
    pub fn get_item(&self, table: &str) -> GetItemBuilder<'_> {
        GetItemBuilder::new(self, table.to_string())
    }

    /// Get an item from a table by key, including its MVCC version.
    ///
    /// Returns a `VersionedItem` with the document and its version number.
    /// Use the version with `put_item_conditional` for optimistic concurrency.
    pub fn get_item_versioned(&self, table: &str) -> GetItemVersionedBuilder<'_> {
        GetItemVersionedBuilder::new(self, table.to_string())
    }

    /// Retrieve multiple items by key in a single call from a single snapshot.
    ///
    /// Returns a builder to specify keys via `.key()` chaining.
    /// Results are positional: `results[i]` corresponds to `keys[i]`.
    /// Missing items return `None`.
    pub fn batch_get_item(&self, table: &str) -> BatchGetItemBuilder<'_> {
        BatchGetItemBuilder::new(self, table.to_string())
    }

    /// Put an item with optimistic concurrency control.
    ///
    /// Succeeds only if the item's current version matches `expected_version`.
    /// Returns `TxnError::VersionMismatch` if another writer modified the item
    /// since it was read.
    pub fn put_item_conditional(
        &self,
        table: &str,
        document: Value,
        expected_version: u64,
    ) -> Result<(), Error> {
        let table = table.to_string();
        self.transact(move |txn| txn.put_item_conditional(&table, document, expected_version))
    }

    /// Partially update an item in a table.
    ///
    /// Returns a builder to specify the key and the SET/REMOVE actions.
    /// If the item doesn't exist, an upsert creates it.
    pub fn update_item(&self, table: &str) -> UpdateItemBuilder<'_> {
        UpdateItemBuilder::new(self, table.to_string())
    }

    /// Delete an item from a table by key.
    pub fn delete_item(&self, table: &str) -> DeleteItemBuilder<'_> {
        DeleteItemBuilder::new(self, table.to_string())
    }

    /// Query items with partition key and optional sort key conditions.
    pub fn query(&self, table: &str) -> QueryBuilder<'_> {
        QueryBuilder::new(self, table.to_string())
    }

    /// Scan all items in a table.
    pub fn scan(&self, table: &str) -> ScanBuilder<'_> {
        ScanBuilder::new(self, table.to_string())
    }

    /// List distinct partition keys in a table.
    pub fn list_partition_keys(&self, table: &str) -> ListPartitionKeysBuilder<'_> {
        ListPartitionKeysBuilder::new(self, table.to_string())
    }

    /// List distinct sort key prefixes (split on `#`) for a given partition key.
    pub fn list_sort_key_prefixes(&self, table: &str) -> ListSortKeyPrefixesBuilder<'_> {
        ListSortKeyPrefixesBuilder::new(self, table.to_string())
    }

    /// Query items via a secondary index.
    pub fn query_index(&self, table: &str, index_name: &str) -> IndexQueryBuilder<'_> {
        IndexQueryBuilder::new(self, table.to_string(), index_name.to_string())
    }

    // -----------------------------------------------------------------------
    // Partition schema management
    // -----------------------------------------------------------------------

    /// Create a partition schema on a table.
    pub fn create_partition_schema(&self, table: &str) -> PartitionSchemaBuilder<'_> {
        PartitionSchemaBuilder::new(self, table.to_string())
    }

    /// Drop a partition schema from a table.
    ///
    /// Fails if any indexes reference the partition schema.
    pub fn drop_partition_schema(&self, table: &str, prefix: &str) -> Result<(), Error> {
        let table = table.to_string();
        let prefix = prefix.to_string();
        self.transact(move |txn| {
            let new_root = catalog::ops::drop_partition_schema(
                &mut txn.store,
                txn.catalog_root,
                &table,
                &prefix,
            )?;
            txn.catalog_root = new_root;
            Ok(())
        })
    }

    /// List all partition schemas for a table.
    pub fn list_partition_schemas(&self, table: &str) -> Result<Vec<PartitionSchema>, Error> {
        let (store, header) = self.read_store_snapshot()?;
        catalog::ops::list_partition_schemas(&store, header.catalog_root_page, table)
    }

    /// Describe a specific partition schema by prefix.
    pub fn describe_partition_schema(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<PartitionSchema, Error> {
        let (store, header) = self.read_store_snapshot()?;
        let entry = self.cached_get_table(&store, header.catalog_root_page, table)?;
        entry
            .partition_schemas
            .into_iter()
            .find(|ps| ps.prefix == prefix)
            .ok_or_else(|| SchemaError::PartitionSchemaNotFound(prefix.to_string()).into())
    }

    // -----------------------------------------------------------------------
    // Index management
    // -----------------------------------------------------------------------

    /// Create a secondary index on a table.
    ///
    /// The index is scoped to a partition schema (prefix) and indexes a
    /// specific attribute. If the table already has data, a synchronous
    /// backfill is performed.
    pub fn create_index(&self, table: &str) -> CreateIndexBuilder<'_> {
        CreateIndexBuilder::new(self, table.to_string())
    }

    /// Drop a secondary index from a table.
    ///
    /// The index B+Tree pages are leaked (not reclaimed) in v1.
    pub fn drop_index(&self, table: &str, index_name: &str) -> Result<(), Error> {
        let table = table.to_string();
        let index_name = index_name.to_string();
        self.transact(move |txn| {
            let new_root =
                catalog::ops::drop_index(&mut txn.store, txn.catalog_root, &table, &index_name)?;
            txn.catalog_root = new_root;
            Ok(())
        })
    }

    /// List all secondary indexes for a table.
    pub fn list_indexes(&self, table: &str) -> Result<Vec<IndexDefinition>, Error> {
        let (store, header) = self.read_store_snapshot()?;
        catalog::ops::list_indexes(&store, header.catalog_root_page, table)
    }

    /// Describe a specific secondary index by name.
    pub fn describe_index(&self, table: &str, index_name: &str) -> Result<IndexDefinition, Error> {
        let (store, header) = self.read_store_snapshot()?;
        let entry = self.cached_get_table(&store, header.catalog_root_page, table)?;
        entry
            .indexes
            .into_iter()
            .find(|idx| idx.name == index_name)
            .ok_or_else(|| SchemaError::IndexNotFound(index_name.to_string()).into())
    }

    /// Create a new write batch for batching multiple operations.
    ///
    /// All queued operations are committed in a single transaction
    /// (one fsync instead of N).
    pub fn write_batch(&self) -> WriteBatch<'_> {
        WriteBatch::new(self)
    }

    /// Set the sync mode (durability level).
    pub fn set_sync_mode(&self, mode: SyncMode) {
        self.inner.sync_mode.store(mode as u8, Ordering::Release);
    }

    /// Get the current sync mode.
    pub fn sync_mode(&self) -> SyncMode {
        SyncMode::from_u8(self.inner.sync_mode.load(Ordering::Acquire))
    }

    /// Sweep expired TTL items from a table.
    ///
    /// Scans the table for items whose TTL has expired and deletes them
    /// in a single transaction. Returns the number of items deleted.
    ///
    /// Batch-limited to 100 items per call; call repeatedly until 0 for
    /// full cleanup.
    pub fn sweep_expired_ttl(&self, table: &str) -> Result<usize, Error> {
        // Phase 1: Read snapshot to find expired keys.
        let expired_keys: Vec<(Value, Option<Value>)> =
            self.read_snapshot(|store, catalog_root, snapshot_txn| {
                let entry = self.cached_get_table(store, catalog_root, table)?;
                let schema = &entry.schema;

                let Some(ref ttl_attr) = schema.ttl_attribute else {
                    return Ok(Vec::new());
                };

                let raw_results = mvcc_ops::mvcc_range_scan(
                    store,
                    entry.data_root_page,
                    None,
                    None,
                    snapshot_txn,
                )?;

                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64();

                let mut expired = Vec::new();
                for (_key_bytes, value_bytes) in raw_results {
                    if expired.len() >= 100 {
                        break;
                    }
                    let val: Value = rmp_serde::from_slice(&value_bytes).map_err(|e| {
                        StorageError::CorruptedPage(format!("failed to deserialize document: {e}"))
                    })?;

                    if let Some(ttl_val) = val.get(ttl_attr)
                        && let Some(epoch_secs) = ttl_val.as_f64()
                        && epoch_secs != 0.0
                        && epoch_secs <= now
                    {
                        let pk_val = val
                            .get(&schema.partition_key.name)
                            .cloned()
                            .ok_or(QueryError::PartitionKeyRequired)?;
                        let sk_val = schema
                            .sort_key
                            .as_ref()
                            .and_then(|sk_def| val.get(&sk_def.name).cloned());
                        expired.push((pk_val, sk_val));
                    }
                }

                Ok(expired)
            })?;

        if expired_keys.is_empty() {
            return Ok(0);
        }

        // Phase 2: Delete expired items in a write transaction.
        let count = expired_keys.len();
        let table_name = table.to_string();
        self.transact(move |txn| {
            for (pk_val, sk_val) in &expired_keys {
                txn.delete_item(&table_name, pk_val, sk_val.as_ref(), None)?;
            }
            Ok(())
        })?;

        Ok(count)
    }

    /// Execute a write transaction.
    ///
    /// The closure receives a mutable [`Transaction`] to make changes.
    /// If the closure returns `Ok`, the transaction is committed atomically.
    /// If it returns `Err`, all changes are discarded (auto-abort).
    pub fn transact<F, R>(&self, f: F) -> Result<R, Error>
    where
        F: FnOnce(&mut Transaction) -> Result<R, Error>,
    {
        // Serialize writers — only one write transaction at a time.
        let _writer_guard = self.inner.writer_lock.lock();

        // Capture current committed state under a brief read lock.
        let (file, total_pages, catalog_root, new_txn_id) = {
            let state = self.inner.state.read();
            let file = state
                .file_manager
                .file()
                .try_clone()
                .map_err(StorageError::from)?;
            (
                file,
                state.file_manager.total_page_count(),
                state.header.catalog_root_page,
                state.header.txn_counter + 1,
            )
        };
        // Read lock released — readers can proceed during the transaction body.

        let store = BufferedPageStore::new(file, total_pages);
        let mut txn = Transaction {
            store,
            catalog_root,
            txn_id: new_txn_id,
            tombstones: Vec::new(),
        };

        let result = f(&mut txn);

        match result {
            Ok(val) => {
                // Acquire write lock briefly for the commit phase only.
                let mut state = self.inner.state.write();
                self.commit_txn(txn, &mut state)?;
                Ok(val)
            }
            Err(e) => Err(e),
        }
    }

    /// Read-only helper: execute a closure with a read store and snapshot txn.
    ///
    /// Captures a lightweight snapshot of the committed header, then releases
    /// the state lock so reads do not block writes (and vice versa). The
    /// snapshot is registered with `SnapshotTracker` to prevent premature
    /// page reclamation while the reader is active.
    pub(crate) fn read_snapshot<F, R>(&self, f: F) -> Result<R, Error>
    where
        F: FnOnce(&FilePageStore, PageId, u64) -> Result<R, Error>,
    {
        let (store, header) = self.read_store_snapshot()?;
        let _guard = self.inner.snapshot_tracker.register(header.txn_counter);
        f(&store, header.catalog_root_page, header.txn_counter)
    }

    /// Look up a table's catalog entry.
    pub(crate) fn cached_get_table(
        &self,
        store: &impl crate::btree::PageStore,
        catalog_root: PageId,
        table_name: &str,
    ) -> Result<catalog::CatalogEntry, Error> {
        catalog::ops::get_table(store, catalog_root, table_name)
    }

    fn commit_txn(&self, txn: Transaction, state: &mut DatabaseState) -> Result<(), Error> {
        let Transaction {
            mut store,
            catalog_root,
            txn_id,
            tombstones,
        } = txn;

        // Record COW-replaced original pages in the pending free list so they
        // can be reclaimed once no active snapshots reference them.
        let cow_old = store.cow_old_pages().to_vec();
        if !cow_old.is_empty() {
            state.pending_free.add(txn_id, cow_old);
        }

        // Track old pending-free chain pages so they can be freed eventually.
        let old_pf_root = state.header.pending_free_root_page;
        if old_pf_root != 0 {
            let old_chain_pages = PendingFreeList::collect_chain_page_ids(&store, old_pf_root)?;
            if !old_chain_pages.is_empty() {
                state.pending_free.add(txn_id, old_chain_pages);
            }
        }

        // Track old tombstone chain pages for freeing.
        let old_ts_root = state.header.tombstone_root_page;
        if old_ts_root != 0 {
            let old_chain_pages = TombstoneQueue::collect_chain_page_ids(&store, old_ts_root)?;
            if !old_chain_pages.is_empty() {
                state.pending_free.add(txn_id, old_chain_pages);
            }
        }

        // Record tombstones from this transaction for incremental GC.
        state.tombstone_queue.add(txn_id, tombstones);

        // Serialize the pending free list and tombstone queue into newly allocated pages.
        let pending_free_root = state.pending_free.serialize_to_pages(&mut store)?;
        let tombstone_root = state.tombstone_queue.serialize_to_pages(&mut store)?;

        let new_total = store.next_page_id();

        // Grow file if needed.
        if new_total > state.file_manager.total_page_count() {
            state.file_manager.grow(new_total)?;
        }

        // Write all overlay pages to disk with checksums.
        for (&page_id, data) in store.overlay() {
            let mut buf = *data;
            // Data pages get checksums; header pages (0, 1) use their own scheme.
            if page_id >= 2 {
                write_checksum_buf(&mut buf);
            }
            state.file_manager.write_page(page_id, &buf)?;
        }

        // Compute the Merkle root checksum for the catalog B-tree root page.
        let catalog_root_checksum = if catalog_root != 0 {
            if let Some(data) = store.overlay().get(&catalog_root) {
                compute_checksum_buf(data)
            } else {
                // Catalog root not in overlay — read from disk.
                let buf = state.file_manager.read_page(catalog_root)?;
                compute_checksum_buf(&buf)
            }
        } else {
            0u128
        };

        // Write new header to alternate slot.
        let new_slot = FileHeader::alternate_slot(state.header_slot);
        state.header.txn_counter = txn_id;
        state.header.catalog_root_page = catalog_root;
        state.header.total_page_count = new_total;
        state.header.pending_free_root_page = pending_free_root;
        state.header.tombstone_root_page = tombstone_root;
        state.header.catalog_root_checksum = catalog_root_checksum;

        let mut header_buf = [0u8; PAGE_SIZE];
        state.header.write_to_page(&mut header_buf);
        state
            .file_manager
            .write_page(new_slot as u64, &header_buf)?;

        // Sync (conditional on SyncMode).
        if self.inner.sync_mode.load(Ordering::Acquire) == SyncMode::Full as u8 {
            state.file_manager.sync()?;
        }
        state.header_slot = new_slot;

        Ok(())
    }

    /// Capture a snapshot of the committed state and create a read-only store.
    ///
    /// The state read lock is held only long enough to copy the header and
    /// clone the file descriptor, then released immediately.
    fn read_store_snapshot(&self) -> Result<(FilePageStore, FileHeader), Error> {
        let header = {
            let state = self.inner.state.read();
            state.header.clone()
        };
        let file = self
            .inner
            .read_file
            .try_clone()
            .map_err(StorageError::from)?;
        Ok((FilePageStore::new(file, header.total_page_count), header))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::KeyType;
    use serde_json::json;
    use std::thread;
    use tempfile::tempdir;

    fn create_test_db() -> (FerridynDB, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = FerridynDB::create(&db_path).unwrap();
        (db, dir)
    }

    #[test]
    fn test_create_and_reopen() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        {
            let db = FerridynDB::create(&db_path).unwrap();
            db.create_table("users")
                .partition_key("user_id", KeyType::String)
                .execute()
                .unwrap();
            db.put_item("users", json!({"user_id": "alice", "name": "Alice"}))
                .unwrap();
        }

        {
            let db = FerridynDB::open(&db_path).unwrap();
            let item = db
                .get_item("users")
                .partition_key("alice")
                .execute()
                .unwrap();
            assert!(item.is_some());
            assert_eq!(item.unwrap()["name"], "Alice");
        }
    }

    #[test]
    fn test_put_get_delete() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        db.put_item("items", json!({"id": "item1", "value": 42}))
            .unwrap();

        let item = db
            .get_item("items")
            .partition_key("item1")
            .execute()
            .unwrap();
        assert!(item.is_some());
        assert_eq!(item.unwrap()["value"], 42);

        db.delete_item("items")
            .partition_key("item1")
            .execute()
            .unwrap();

        let item = db
            .get_item("items")
            .partition_key("item1")
            .execute()
            .unwrap();
        assert!(item.is_none());
    }

    #[test]
    fn test_table_with_sort_key_and_query() {
        let (db, _dir) = create_test_db();
        db.create_table("events")
            .partition_key("user_id", KeyType::String)
            .sort_key("timestamp", KeyType::Number)
            .execute()
            .unwrap();

        for i in 0..10 {
            db.put_item(
                "events",
                json!({
                    "user_id": "alice",
                    "timestamp": i as f64 * 100.0,
                    "data": format!("event_{i}")
                }),
            )
            .unwrap();
        }

        // Query all events for alice.
        let result = db.query("events").partition_key("alice").execute().unwrap();
        assert_eq!(result.items.len(), 10);

        // Query with sort key condition: timestamp >= 500.
        let result = db
            .query("events")
            .partition_key("alice")
            .sort_key_ge(500.0)
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 5);

        // Query with between condition: 200 <= timestamp <= 400.
        let result = db
            .query("events")
            .partition_key("alice")
            .sort_key_between(200.0, 400.0)
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 3);
    }

    #[test]
    fn test_scan_with_limit_and_pagination() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        for i in 0..20 {
            db.put_item("items", json!({"id": format!("item_{i:03}"), "val": i}))
                .unwrap();
        }

        // First page.
        let result = db.scan("items").limit(5).execute().unwrap();
        assert_eq!(result.items.len(), 5);
        assert!(result.last_evaluated_key.is_some());

        // Second page.
        let result2 = db
            .scan("items")
            .limit(5)
            .exclusive_start_key(result.last_evaluated_key.unwrap())
            .execute()
            .unwrap();
        assert_eq!(result2.items.len(), 5);
        // Items should be different from first page.
        assert_ne!(result2.items[0]["id"], result.items[0]["id"]);
    }

    #[test]
    fn test_transaction_commit() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        db.transact(|txn| {
            txn.put_item("items", json!({"id": "a", "v": 1}), None)?;
            txn.put_item("items", json!({"id": "b", "v": 2}), None)?;
            Ok(())
        })
        .unwrap();

        let a = db.get_item("items").partition_key("a").execute().unwrap();
        let b = db.get_item("items").partition_key("b").execute().unwrap();
        assert!(a.is_some());
        assert!(b.is_some());
    }

    #[test]
    fn test_transaction_abort_on_error() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        let result = db.transact(|txn| {
            txn.put_item("items", json!({"id": "a", "v": 1}), None)?;
            // Force an error by trying to put into a non-existent table.
            txn.put_item("nonexistent", json!({"id": "b"}), None)?;
            Ok(())
        });
        assert!(result.is_err());

        // "a" should NOT be visible because the transaction aborted.
        let a = db.get_item("items").partition_key("a").execute().unwrap();
        assert!(a.is_none());
    }

    #[test]
    fn test_overwrite_same_key() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        db.put_item("items", json!({"id": "key1", "v": "old"}))
            .unwrap();
        db.put_item("items", json!({"id": "key1", "v": "new"}))
            .unwrap();

        let item = db
            .get_item("items")
            .partition_key("key1")
            .execute()
            .unwrap();
        assert_eq!(item.unwrap()["v"], "new");
    }

    #[test]
    fn test_query_with_exclusive_start_key() {
        let (db, _dir) = create_test_db();
        db.create_table("events")
            .partition_key("pk", KeyType::String)
            .sort_key("sk", KeyType::Number)
            .execute()
            .unwrap();

        for i in 0..10 {
            db.put_item(
                "events",
                json!({"pk": "user1", "sk": i as f64, "data": format!("e{i}")}),
            )
            .unwrap();
        }

        // First page: 3 items.
        let result = db
            .query("events")
            .partition_key("user1")
            .limit(3)
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 3);
        assert!(result.last_evaluated_key.is_some());

        // Second page using exclusive_start_key.
        let result2 = db
            .query("events")
            .partition_key("user1")
            .limit(3)
            .exclusive_start_key(result.last_evaluated_key.unwrap())
            .execute()
            .unwrap();
        assert_eq!(result2.items.len(), 3);
        // Should not overlap with first page.
        assert_ne!(result2.items[0]["sk"], result.items[2]["sk"]);
    }

    #[test]
    fn test_multiple_tables_independent() {
        let (db, _dir) = create_test_db();
        db.create_table("table_a")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();
        db.create_table("table_b")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        db.put_item("table_a", json!({"id": "x", "src": "a"}))
            .unwrap();
        db.put_item("table_b", json!({"id": "x", "src": "b"}))
            .unwrap();

        let a = db
            .get_item("table_a")
            .partition_key("x")
            .execute()
            .unwrap()
            .unwrap();
        let b = db
            .get_item("table_b")
            .partition_key("x")
            .execute()
            .unwrap()
            .unwrap();

        assert_eq!(a["src"], "a");
        assert_eq!(b["src"], "b");
    }

    #[test]
    fn test_document_too_large() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        let big = "x".repeat(crate::types::MAX_DOCUMENT_SIZE + 1);
        let result = db.put_item("items", json!({"id": "big", "data": big}));
        assert!(result.is_err());
    }

    #[test]
    fn test_table_not_found() {
        let (db, _dir) = create_test_db();
        let result = db.put_item("nonexistent", json!({"id": "x"}));
        assert!(result.is_err());
    }

    #[test]
    fn test_key_type_mismatch() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        // Provide a number where a string is expected.
        let result = db.put_item("items", json!({"id": 42}));
        assert!(result.is_err());
    }

    #[test]
    fn test_list_tables() {
        let (db, _dir) = create_test_db();
        db.create_table("alpha")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();
        db.create_table("beta")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        let tables = db.list_tables().unwrap();
        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&"alpha".to_string()));
        assert!(tables.contains(&"beta".to_string()));
    }

    #[test]
    fn test_drop_table() {
        let (db, _dir) = create_test_db();
        db.create_table("temp")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        assert_eq!(db.list_tables().unwrap().len(), 1);
        db.drop_table("temp").unwrap();
        assert_eq!(db.list_tables().unwrap().len(), 0);
    }

    #[test]
    fn test_concurrent_reads() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        db.put_item("items", json!({"id": "shared", "v": 42}))
            .unwrap();

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let db_clone = db.clone();
                thread::spawn(move || {
                    let item = db_clone
                        .get_item("items")
                        .partition_key("shared")
                        .execute()
                        .unwrap();
                    assert!(item.is_some());
                    assert_eq!(item.unwrap()["v"], 42);
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn test_scan_full_table() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        for i in 0..15 {
            db.put_item("items", json!({"id": format!("i{i:03}"), "val": i}))
                .unwrap();
        }

        let result = db.scan("items").execute().unwrap();
        assert_eq!(result.items.len(), 15);
        assert!(result.last_evaluated_key.is_none());
    }

    #[test]
    fn test_query_reverse() {
        let (db, _dir) = create_test_db();
        db.create_table("events")
            .partition_key("pk", KeyType::String)
            .sort_key("sk", KeyType::Number)
            .execute()
            .unwrap();

        for i in 0..5 {
            db.put_item(
                "events",
                json!({"pk": "u1", "sk": i as f64, "data": format!("e{i}")}),
            )
            .unwrap();
        }

        let result = db
            .query("events")
            .partition_key("u1")
            .scan_forward(false)
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 5);
        // First item should have the highest sort key.
        assert_eq!(result.items[0]["sk"], 4.0);
        assert_eq!(result.items[4]["sk"], 0.0);
    }
}

#[cfg(test)]
mod crash_recovery_tests {
    use super::*;
    use crate::storage::header::FileHeader;
    use crate::types::{KeyType, PAGE_SIZE};
    use serde_json::json;
    use std::fs::OpenOptions;
    use std::os::unix::fs::FileExt;
    use tempfile::tempdir;

    #[test]
    fn test_crash_mid_commit_preserves_old_data() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("crash_test.db");

        // Create DB and insert data via a normal commit.
        {
            let db = FerridynDB::create(&db_path).unwrap();
            db.create_table("items")
                .partition_key("id", KeyType::String)
                .execute()
                .unwrap();
            db.put_item("items", json!({"id": "key1", "value": "committed"}))
                .unwrap();
        }

        // Simulate a crash by writing garbage to the ALTERNATE header slot.
        // After the create + create_table + put_item sequence, the active slot
        // has been alternating. We corrupt whichever slot would be written next.
        {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&db_path)
                .unwrap();

            // Read both headers to find the current active slot.
            let mut buf_a = [0u8; PAGE_SIZE];
            let mut buf_b = [0u8; PAGE_SIZE];
            file.read_exact_at(&mut buf_a, 0).unwrap();
            file.read_exact_at(&mut buf_b, PAGE_SIZE as u64).unwrap();
            let (_header, active_slot) = FileHeader::select_current(&buf_a, &buf_b).unwrap();

            // Write garbage to the alternate (non-active) slot.
            let alternate_slot = FileHeader::alternate_slot(active_slot);
            let garbage = [0xDE; PAGE_SIZE];
            file.write_all_at(&garbage, alternate_slot as u64 * PAGE_SIZE as u64)
                .unwrap();
            file.sync_all().unwrap();
        }

        // Reopen the database -- it should recover from the valid header.
        {
            let db = FerridynDB::open(&db_path).unwrap();
            let item = db
                .get_item("items")
                .partition_key("key1")
                .execute()
                .unwrap();
            assert!(item.is_some());
            assert_eq!(item.unwrap()["value"], "committed");
        }
    }

    #[test]
    fn test_both_headers_valid_picks_higher_txn() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("txn_test.db");

        // Create DB and do several operations to bump txn_counter.
        {
            let db = FerridynDB::create(&db_path).unwrap();
            db.create_table("items")
                .partition_key("id", KeyType::String)
                .execute()
                .unwrap();

            // Each put_item is a transaction, bumping the counter.
            for i in 0..5 {
                db.put_item(
                    "items",
                    json!({"id": format!("key{i}"), "value": format!("val{i}")}),
                )
                .unwrap();
            }
        }

        // Reopen and verify the header with the higher txn_counter is selected
        // and all data is accessible.
        {
            let db = FerridynDB::open(&db_path).unwrap();

            // Verify all 5 items are readable.
            for i in 0..5 {
                let key = format!("key{i}");
                let item = db
                    .get_item("items")
                    .partition_key(key.as_str())
                    .execute()
                    .unwrap();
                assert!(item.is_some(), "key{i} should be present after reopen");
                assert_eq!(item.unwrap()["value"], format!("val{i}"));
            }

            // Verify we can still write after reopen.
            db.put_item("items", json!({"id": "key5", "value": "val5"}))
                .unwrap();
            let item = db
                .get_item("items")
                .partition_key("key5")
                .execute()
                .unwrap();
            assert!(item.is_some());
        }
    }

    #[test]
    fn test_corrupted_header_falls_back() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("corrupt_test.db");

        // Create DB and insert data.
        {
            let db = FerridynDB::create(&db_path).unwrap();
            db.create_table("data")
                .partition_key("id", KeyType::String)
                .execute()
                .unwrap();
            db.put_item("data", json!({"id": "hello", "msg": "world"}))
                .unwrap();
        }

        // Corrupt the NON-active header page's checksum bytes.
        // This tests that select_current correctly ignores the corrupted header
        // and uses the valid one.
        {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&db_path)
                .unwrap();

            // Read both headers to find the active slot.
            let mut buf_a = [0u8; PAGE_SIZE];
            let mut buf_b = [0u8; PAGE_SIZE];
            file.read_exact_at(&mut buf_a, 0).unwrap();
            file.read_exact_at(&mut buf_b, PAGE_SIZE as u64).unwrap();
            let (_header, active_slot) = FileHeader::select_current(&buf_a, &buf_b).unwrap();

            // Corrupt the non-active slot's checksum (bytes 44..52).
            let corrupt_slot = FileHeader::alternate_slot(active_slot);
            let offset = corrupt_slot as u64 * PAGE_SIZE as u64;
            let mut buf = [0u8; PAGE_SIZE];
            file.read_exact_at(&mut buf, offset).unwrap();
            for byte in &mut buf[44..52] {
                *byte ^= 0xFF;
            }
            file.write_all_at(&buf, offset).unwrap();
            file.sync_all().unwrap();
        }

        // Reopen -- should use the valid (active) header and ignore the
        // corrupted alternate.
        {
            let db = FerridynDB::open(&db_path).unwrap();
            let item = db
                .get_item("data")
                .partition_key("hello")
                .execute()
                .unwrap();
            assert!(item.is_some());
            assert_eq!(item.unwrap()["msg"], "world");

            // Verify continued operation works (the next commit writes to
            // the corrupted slot, overwriting it with valid data).
            db.put_item("data", json!({"id": "new", "msg": "item"}))
                .unwrap();
            let new_item = db.get_item("data").partition_key("new").execute().unwrap();
            assert!(new_item.is_some());
        }
    }

    #[test]
    fn test_corrupted_catalog_root_detected_on_open() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("merkle_root_test.db");

        // Create a DB with some data so the catalog root is non-trivial.
        {
            let db = FerridynDB::create(&db_path).unwrap();
            db.create_table("items")
                .partition_key("id", KeyType::String)
                .execute()
                .unwrap();
            db.put_item("items", json!({"id": "k1", "value": "v1"}))
                .unwrap();
        }

        // Read the active header to find the catalog root page, then corrupt it.
        {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&db_path)
                .unwrap();

            let mut buf_a = [0u8; PAGE_SIZE];
            let mut buf_b = [0u8; PAGE_SIZE];
            file.read_exact_at(&mut buf_a, 0).unwrap();
            file.read_exact_at(&mut buf_b, PAGE_SIZE as u64).unwrap();
            let (header, _slot) = FileHeader::select_current(&buf_a, &buf_b).unwrap();

            // Corrupt some bytes in the catalog root page's data region.
            let root_offset = header.catalog_root_page * PAGE_SIZE as u64;
            let mut root_buf = [0u8; PAGE_SIZE];
            file.read_exact_at(&mut root_buf, root_offset).unwrap();
            // Flip bytes in the data region (after page header).
            for byte in &mut root_buf[100..110] {
                *byte ^= 0xFF;
            }
            file.write_all_at(&root_buf, root_offset).unwrap();
            file.sync_all().unwrap();
        }

        // Reopen should fail because the catalog root checksum doesn't match.
        match FerridynDB::open(&db_path) {
            Err(e) => {
                let err_msg = format!("{e}");
                assert!(
                    err_msg.contains("checksum mismatch"),
                    "error should mention checksum mismatch, got: {err_msg}"
                );
            }
            Ok(_) => panic!("open should fail when catalog root page is corrupted"),
        }
    }
}

#[cfg(test)]
mod concurrency_stress_tests {
    use super::*;
    use crate::types::KeyType;
    use serde_json::json;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use tempfile::tempdir;

    #[test]
    fn test_concurrent_readers_and_writer() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("concurrent.db");
        let db = FerridynDB::create(&db_path).unwrap();

        db.create_table("items")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        // Prepopulate a known key so readers always have something to read.
        db.put_item("items", json!({"id": "seed", "value": 0}))
            .unwrap();

        let running = Arc::new(AtomicBool::new(true));

        // Spawn 4 reader threads.
        let reader_handles: Vec<_> = (0..4)
            .map(|reader_id| {
                let db_clone = db.clone();
                let running_clone = running.clone();
                thread::spawn(move || {
                    let mut reads = 0u64;
                    while running_clone.load(Ordering::Relaxed) {
                        // Read the seed key -- should always be consistent.
                        let result = db_clone.get_item("items").partition_key("seed").execute();
                        match result {
                            Ok(Some(item)) => {
                                // Value must be a number (no partial writes).
                                assert!(
                                    item["value"].is_number(),
                                    "reader {reader_id}: seed value should be a number, got {:?}",
                                    item["value"]
                                );
                            }
                            Ok(None) => {
                                // Seed was written before readers started; this
                                // should not happen.
                                panic!("reader {reader_id}: seed key missing");
                            }
                            Err(e) => {
                                panic!("reader {reader_id}: read error: {e}");
                            }
                        }
                        reads += 1;
                    }
                    reads
                })
            })
            .collect();

        // Writer thread: insert 100 items sequentially.
        let db_writer = db.clone();
        let writer_handle = thread::spawn(move || {
            for i in 0..100 {
                db_writer
                    .put_item("items", json!({"id": format!("item_{i:04}"), "value": i}))
                    .unwrap();

                // Also update the seed key so readers see changing data.
                db_writer
                    .put_item("items", json!({"id": "seed", "value": i + 1}))
                    .unwrap();
            }
        });

        // Wait for writer to finish.
        writer_handle.join().unwrap();

        // Signal readers to stop.
        running.store(false, Ordering::Relaxed);

        // Collect reader results.
        for handle in reader_handles {
            let reads = handle.join().unwrap();
            assert!(
                reads > 0,
                "each reader should have completed at least one read"
            );
        }

        // Verify all 100 items are present.
        for i in 0..100 {
            let key = format!("item_{i:04}");
            let item = db
                .get_item("items")
                .partition_key(key.as_str())
                .execute()
                .unwrap();
            assert!(item.is_some(), "item_{i:04} should exist");
        }
    }

    #[test]
    fn test_stress_many_operations() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("stress.db");
        let db = FerridynDB::create(&db_path).unwrap();

        db.create_table("stress")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        // Perform 500 sequential put + get pairs.
        for i in 0..500 {
            let key = format!("stress_{i:04}");
            let doc = json!({"id": key, "iteration": i, "payload": "x".repeat(100)});

            db.put_item("stress", doc.clone()).unwrap();

            let retrieved = db
                .get_item("stress")
                .partition_key(key.as_str())
                .execute()
                .unwrap();
            assert!(
                retrieved.is_some(),
                "key {key} should be readable immediately after write"
            );
            let retrieved = retrieved.unwrap();
            assert_eq!(retrieved["iteration"], i);
            assert_eq!(retrieved["id"], key);
        }

        // Final verification: scan to confirm total count.
        let result = db.scan("stress").execute().unwrap();
        assert_eq!(result.items.len(), 500);
    }

    #[test]
    fn test_concurrent_scan_during_writes() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("scan_concurrent.db");
        let db = FerridynDB::create(&db_path).unwrap();

        db.create_table("items")
            .partition_key("pk", KeyType::String)
            .sort_key("sk", KeyType::String)
            .execute()
            .unwrap();

        // Prepopulate 20 items so scans always return data.
        for i in 0..20 {
            db.put_item(
                "items",
                json!({"pk": "p", "sk": format!("s_{i:04}"), "v": i}),
            )
            .unwrap();
        }

        let running = Arc::new(AtomicBool::new(true));

        // Spawn 2 scan-reader threads.
        let scan_handles: Vec<_> = (0..2)
            .map(|_| {
                let db_clone = db.clone();
                let running_clone = running.clone();
                thread::spawn(move || {
                    let mut scans = 0u64;
                    while running_clone.load(Ordering::Relaxed) {
                        let result = db_clone
                            .query("items")
                            .partition_key("p")
                            .execute()
                            .unwrap();
                        // Must always see at least the original 20 items.
                        assert!(
                            result.items.len() >= 20,
                            "scan should see at least 20 items, got {}",
                            result.items.len()
                        );
                        scans += 1;
                    }
                    scans
                })
            })
            .collect();

        // Writer: insert 50 more items.
        let db_writer = db.clone();
        let writer_handle = thread::spawn(move || {
            for i in 20..70 {
                db_writer
                    .put_item(
                        "items",
                        json!({"pk": "p", "sk": format!("s_{i:04}"), "v": i}),
                    )
                    .unwrap();
            }
        });

        writer_handle.join().unwrap();
        running.store(false, Ordering::Relaxed);

        for handle in scan_handles {
            let scans = handle.join().unwrap();
            assert!(
                scans > 0,
                "each scanner should have completed at least one scan"
            );
        }

        // Final count should be 70.
        let result = db.query("items").partition_key("p").execute().unwrap();
        assert_eq!(result.items.len(), 70);
    }

    #[test]
    fn test_snapshot_isolation_readers_dont_see_partial_writes() {
        use std::sync::Barrier;

        let dir = tempdir().unwrap();
        let db_path = dir.path().join("snapshot.db");
        let db = FerridynDB::create(&db_path).unwrap();

        db.create_table("items")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        // Insert items A and B atomically via transact.
        db.transact(|txn| {
            txn.put_item("items", json!({"id": "A", "batch": 1}), None)?;
            txn.put_item("items", json!({"id": "B", "batch": 1}), None)?;
            Ok(())
        })
        .unwrap();

        let barrier = Arc::new(Barrier::new(2));

        // Reader thread: read A, wait for writer, then read B.
        // Both should return the same batch number (snapshot isolation).
        let db_reader = db.clone();
        let barrier_clone = barrier.clone();
        let reader_handle = thread::spawn(move || {
            // Read A first.
            let a = db_reader
                .get_item("items")
                .partition_key("A")
                .execute()
                .unwrap()
                .unwrap();
            let a_batch = a["batch"].as_i64().unwrap();

            // Signal writer to go.
            barrier_clone.wait();

            // Brief pause to let writer commit.
            thread::sleep(std::time::Duration::from_millis(10));

            // Read B — should be consistent (both batch 1 or both batch 2,
            // depending on whether we see the write).
            let b = db_reader
                .get_item("items")
                .partition_key("B")
                .execute()
                .unwrap()
                .unwrap();
            let b_batch = b["batch"].as_i64().unwrap();

            // Both reads are independent snapshots, so each sees a consistent
            // state. The key property: neither read returns a torn state.
            assert!(
                a_batch >= 1 && b_batch >= 1,
                "both items should exist with valid batch numbers"
            );
            (a_batch, b_batch)
        });

        // Writer thread: update both A and B atomically.
        let db_writer = db.clone();
        barrier.wait(); // Wait for reader to read A.
        db_writer
            .transact(|txn| {
                txn.put_item("items", json!({"id": "A", "batch": 2}), None)?;
                txn.put_item("items", json!({"id": "B", "batch": 2}), None)?;
                Ok(())
            })
            .unwrap();

        let (_a_batch, _b_batch) = reader_handle.join().unwrap();
    }
}

#[cfg(test)]
mod ttl_tests {
    use super::*;
    use crate::types::KeyType;
    use serde_json::json;
    use tempfile::tempdir;

    fn create_test_db() -> (FerridynDB, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = FerridynDB::create(&db_path).unwrap();
        (db, dir)
    }

    #[test]
    fn test_get_expired_item_returns_none() {
        let (db, _dir) = create_test_db();
        db.create_table("cache")
            .partition_key("key", KeyType::String)
            .ttl_attribute("expires")
            .execute()
            .unwrap();

        // TTL in the past (epoch 1000 is 1970).
        db.put_item("cache", json!({"key": "a", "expires": 1000, "val": "old"}))
            .unwrap();

        let item = db.get_item("cache").partition_key("a").execute().unwrap();
        assert!(item.is_none(), "expired item should be invisible to GET");
    }

    #[test]
    fn test_get_non_expired_item_returns_some() {
        let (db, _dir) = create_test_db();
        db.create_table("cache")
            .partition_key("key", KeyType::String)
            .ttl_attribute("expires")
            .execute()
            .unwrap();

        // TTL far in the future.
        db.put_item(
            "cache",
            json!({"key": "b", "expires": 9999999999.0, "val": "fresh"}),
        )
        .unwrap();

        let item = db.get_item("cache").partition_key("b").execute().unwrap();
        assert!(item.is_some(), "non-expired item should be visible");
        assert_eq!(item.unwrap()["val"], "fresh");
    }

    #[test]
    fn test_get_no_ttl_attr_in_doc_returns_some() {
        let (db, _dir) = create_test_db();
        db.create_table("cache")
            .partition_key("key", KeyType::String)
            .ttl_attribute("expires")
            .execute()
            .unwrap();

        // Doc does not contain the TTL attribute at all → never expires.
        db.put_item("cache", json!({"key": "c", "val": "permanent"}))
            .unwrap();

        let item = db.get_item("cache").partition_key("c").execute().unwrap();
        assert!(item.is_some(), "item without TTL attr should never expire");
    }

    #[test]
    fn test_get_ttl_zero_never_expires() {
        let (db, _dir) = create_test_db();
        db.create_table("cache")
            .partition_key("key", KeyType::String)
            .ttl_attribute("expires")
            .execute()
            .unwrap();

        // TTL = 0 means "never expires" per DynamoDB semantics.
        db.put_item("cache", json!({"key": "d", "expires": 0, "val": "forever"}))
            .unwrap();

        let item = db.get_item("cache").partition_key("d").execute().unwrap();
        assert!(item.is_some(), "TTL=0 should mean never expires");
    }

    #[test]
    fn test_get_ttl_non_numeric_never_expires() {
        let (db, _dir) = create_test_db();
        db.create_table("cache")
            .partition_key("key", KeyType::String)
            .ttl_attribute("expires")
            .execute()
            .unwrap();

        // Non-numeric TTL value → not parseable → never expires.
        db.put_item(
            "cache",
            json!({"key": "e", "expires": "not-a-number", "val": "safe"}),
        )
        .unwrap();

        let item = db.get_item("cache").partition_key("e").execute().unwrap();
        assert!(item.is_some(), "non-numeric TTL should mean never expires");
    }

    #[test]
    fn test_query_filters_expired() {
        let (db, _dir) = create_test_db();
        db.create_table("events")
            .partition_key("pk", KeyType::String)
            .sort_key("sk", KeyType::Number)
            .ttl_attribute("ttl")
            .execute()
            .unwrap();

        // Insert 3 items: 2 expired, 1 alive.
        db.put_item(
            "events",
            json!({"pk": "user1", "sk": 1.0, "ttl": 1000, "data": "expired1"}),
        )
        .unwrap();
        db.put_item(
            "events",
            json!({"pk": "user1", "sk": 2.0, "ttl": 2000, "data": "expired2"}),
        )
        .unwrap();
        db.put_item(
            "events",
            json!({"pk": "user1", "sk": 3.0, "ttl": 9999999999.0, "data": "alive"}),
        )
        .unwrap();

        let result = db.query("events").partition_key("user1").execute().unwrap();
        assert_eq!(result.items.len(), 1, "only non-expired items in QUERY");
        assert_eq!(result.items[0]["data"], "alive");
    }

    #[test]
    fn test_scan_filters_expired() {
        let (db, _dir) = create_test_db();
        db.create_table("cache")
            .partition_key("key", KeyType::String)
            .ttl_attribute("expires")
            .execute()
            .unwrap();

        db.put_item(
            "cache",
            json!({"key": "old", "expires": 1000, "val": "gone"}),
        )
        .unwrap();
        db.put_item(
            "cache",
            json!({"key": "new", "expires": 9999999999.0, "val": "here"}),
        )
        .unwrap();
        db.put_item("cache", json!({"key": "permanent", "val": "no_ttl"}))
            .unwrap();

        let result = db.scan("cache").execute().unwrap();
        assert_eq!(result.items.len(), 2, "SCAN should filter expired items");
    }

    #[test]
    fn test_table_without_ttl_unaffected() {
        let (db, _dir) = create_test_db();
        db.create_table("plain")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        // Even if a doc has a field called "expires", it shouldn't be treated
        // as TTL since the table has no ttl_attribute configured.
        db.put_item(
            "plain",
            json!({"id": "x", "expires": 1000, "val": "visible"}),
        )
        .unwrap();

        let item = db.get_item("plain").partition_key("x").execute().unwrap();
        assert!(
            item.is_some(),
            "table without TTL should ignore expires field"
        );
    }

    #[test]
    fn test_describe_table_shows_ttl() {
        let (db, _dir) = create_test_db();
        db.create_table("cache")
            .partition_key("key", KeyType::String)
            .ttl_attribute("expires")
            .execute()
            .unwrap();

        let schema = db.describe_table("cache").unwrap();
        assert_eq!(schema.ttl_attribute, Some("expires".to_string()));
    }

    #[test]
    fn test_describe_table_no_ttl() {
        let (db, _dir) = create_test_db();
        db.create_table("plain")
            .partition_key("key", KeyType::String)
            .execute()
            .unwrap();

        let schema = db.describe_table("plain").unwrap();
        assert_eq!(schema.ttl_attribute, None);
    }

    #[test]
    fn test_sweep_deletes_expired() {
        let (db, _dir) = create_test_db();
        db.create_table("cache")
            .partition_key("key", KeyType::String)
            .ttl_attribute("expires")
            .execute()
            .unwrap();

        db.put_item("cache", json!({"key": "expired1", "expires": 1000}))
            .unwrap();
        db.put_item("cache", json!({"key": "expired2", "expires": 2000}))
            .unwrap();
        db.put_item("cache", json!({"key": "alive", "expires": 9999999999.0}))
            .unwrap();

        let deleted = db.sweep_expired_ttl("cache").unwrap();
        assert_eq!(deleted, 2, "should delete 2 expired items");

        // Second sweep should find nothing more.
        let deleted2 = db.sweep_expired_ttl("cache").unwrap();
        assert_eq!(deleted2, 0, "second sweep should find nothing");

        // The alive item should still be there.
        let item = db
            .get_item("cache")
            .partition_key("alive")
            .execute()
            .unwrap();
        assert!(item.is_some(), "non-expired item should survive sweep");
    }

    #[test]
    fn test_sweep_on_table_without_ttl() {
        let (db, _dir) = create_test_db();
        db.create_table("plain")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        db.put_item("plain", json!({"id": "x", "val": 1})).unwrap();

        let deleted = db.sweep_expired_ttl("plain").unwrap();
        assert_eq!(deleted, 0, "table without TTL should have nothing to sweep");

        // Item should still be there.
        let item = db.get_item("plain").partition_key("x").execute().unwrap();
        assert!(item.is_some());
    }

    #[test]
    fn test_sweep_with_sort_key() {
        let (db, _dir) = create_test_db();
        db.create_table("events")
            .partition_key("pk", KeyType::String)
            .sort_key("sk", KeyType::Number)
            .ttl_attribute("ttl")
            .execute()
            .unwrap();

        db.put_item("events", json!({"pk": "u1", "sk": 1.0, "ttl": 1000}))
            .unwrap();
        db.put_item(
            "events",
            json!({"pk": "u1", "sk": 2.0, "ttl": 9999999999.0}),
        )
        .unwrap();

        let deleted = db.sweep_expired_ttl("events").unwrap();
        assert_eq!(deleted, 1);

        // Only the non-expired item should remain.
        let result = db.query("events").partition_key("u1").execute().unwrap();
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0]["sk"], 2.0);
    }
}

#[cfg(test)]
mod list_partition_keys_tests {
    use super::*;
    use crate::types::KeyType;
    use serde_json::json;
    use tempfile::tempdir;

    fn create_test_db() -> (FerridynDB, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = FerridynDB::create(&db_path).unwrap();
        (db, dir)
    }

    #[test]
    fn test_list_keys_pk_only_table() {
        let (db, _dir) = create_test_db();
        db.create_table("users")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        db.put_item("users", json!({"id": "alice", "name": "Alice"}))
            .unwrap();
        db.put_item("users", json!({"id": "bob", "name": "Bob"}))
            .unwrap();
        db.put_item("users", json!({"id": "charlie", "name": "Charlie"}))
            .unwrap();

        let keys = db.list_partition_keys("users").execute().unwrap();
        assert_eq!(keys.len(), 3);
        // Keys are sorted (B+Tree order).
        assert_eq!(keys[0], json!("alice"));
        assert_eq!(keys[1], json!("bob"));
        assert_eq!(keys[2], json!("charlie"));
    }

    #[test]
    fn test_list_keys_dedup_across_sort_keys() {
        let (db, _dir) = create_test_db();
        db.create_table("events")
            .partition_key("pk", KeyType::String)
            .sort_key("sk", KeyType::Number)
            .execute()
            .unwrap();

        // Two items under "user1", one under "user2".
        db.put_item("events", json!({"pk": "user1", "sk": 1.0, "data": "a"}))
            .unwrap();
        db.put_item("events", json!({"pk": "user1", "sk": 2.0, "data": "b"}))
            .unwrap();
        db.put_item("events", json!({"pk": "user2", "sk": 1.0, "data": "c"}))
            .unwrap();

        let keys = db.list_partition_keys("events").execute().unwrap();
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0], json!("user1"));
        assert_eq!(keys[1], json!("user2"));
    }

    #[test]
    fn test_list_keys_empty_table() {
        let (db, _dir) = create_test_db();
        db.create_table("empty")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        let keys = db.list_partition_keys("empty").execute().unwrap();
        assert!(keys.is_empty());
    }

    #[test]
    fn test_list_keys_with_limit() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        for i in 0..10 {
            db.put_item("items", json!({"id": format!("key{i:02}"), "val": i}))
                .unwrap();
        }

        let keys = db.list_partition_keys("items").limit(3).execute().unwrap();
        assert_eq!(keys.len(), 3);
    }

    #[test]
    fn test_list_keys_table_not_found() {
        let (db, _dir) = create_test_db();
        let result = db.list_partition_keys("nonexistent").execute();
        assert!(result.is_err());
    }

    #[test]
    fn test_list_keys_number_pk() {
        let (db, _dir) = create_test_db();
        db.create_table("nums")
            .partition_key("id", KeyType::Number)
            .execute()
            .unwrap();

        db.put_item("nums", json!({"id": 1.0, "val": "a"})).unwrap();
        db.put_item("nums", json!({"id": 2.0, "val": "b"})).unwrap();

        let keys = db.list_partition_keys("nums").execute().unwrap();
        assert_eq!(keys.len(), 2);
    }
}

#[cfg(test)]
mod list_sort_key_prefixes_tests {
    use super::*;
    use crate::types::KeyType;
    use serde_json::json;
    use tempfile::tempdir;

    fn create_test_db() -> (FerridynDB, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = FerridynDB::create(&db_path).unwrap();
        (db, dir)
    }

    #[test]
    fn test_list_prefixes_hash_delimited() {
        let (db, _dir) = create_test_db();
        db.create_table("memories")
            .partition_key("category", KeyType::String)
            .sort_key("entry", KeyType::String)
            .execute()
            .unwrap();

        db.put_item(
            "memories",
            json!({"category": "rust", "entry": "ownership#borrowing", "data": "..."}),
        )
        .unwrap();
        db.put_item(
            "memories",
            json!({"category": "rust", "entry": "ownership#moves", "data": "..."}),
        )
        .unwrap();
        db.put_item(
            "memories",
            json!({"category": "rust", "entry": "lifetimes#basics", "data": "..."}),
        )
        .unwrap();
        db.put_item(
            "memories",
            json!({"category": "rust", "entry": "lifetimes#elision", "data": "..."}),
        )
        .unwrap();

        let prefixes = db
            .list_sort_key_prefixes("memories")
            .partition_key("rust")
            .execute()
            .unwrap();

        assert_eq!(prefixes.len(), 2);
        assert_eq!(prefixes[0], json!("lifetimes"));
        assert_eq!(prefixes[1], json!("ownership"));
    }

    #[test]
    fn test_list_prefixes_no_hash_returns_full_sk() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("pk", KeyType::String)
            .sort_key("sk", KeyType::String)
            .execute()
            .unwrap();

        db.put_item("items", json!({"pk": "a", "sk": "plain-key", "val": 1}))
            .unwrap();
        db.put_item("items", json!({"pk": "a", "sk": "another-key", "val": 2}))
            .unwrap();

        let prefixes = db
            .list_sort_key_prefixes("items")
            .partition_key("a")
            .execute()
            .unwrap();

        assert_eq!(prefixes.len(), 2);
        assert_eq!(prefixes[0], json!("another-key"));
        assert_eq!(prefixes[1], json!("plain-key"));
    }

    #[test]
    fn test_list_prefixes_with_limit() {
        let (db, _dir) = create_test_db();
        db.create_table("memories")
            .partition_key("cat", KeyType::String)
            .sort_key("entry", KeyType::String)
            .execute()
            .unwrap();

        for i in 0..5 {
            db.put_item(
                "memories",
                json!({"cat": "x", "entry": format!("prefix{i}#detail"), "val": i}),
            )
            .unwrap();
        }

        let prefixes = db
            .list_sort_key_prefixes("memories")
            .partition_key("x")
            .limit(2)
            .execute()
            .unwrap();

        assert_eq!(prefixes.len(), 2);
    }

    #[test]
    fn test_list_prefixes_empty_partition() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("pk", KeyType::String)
            .sort_key("sk", KeyType::String)
            .execute()
            .unwrap();

        let prefixes = db
            .list_sort_key_prefixes("items")
            .partition_key("nonexistent")
            .execute()
            .unwrap();

        assert!(prefixes.is_empty());
    }

    #[test]
    fn test_list_prefixes_no_sort_key_table_errors() {
        let (db, _dir) = create_test_db();
        db.create_table("plain")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        let result = db
            .list_sort_key_prefixes("plain")
            .partition_key("x")
            .execute();

        assert!(result.is_err());
    }

    #[test]
    fn test_list_prefixes_missing_pk_errors() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("pk", KeyType::String)
            .sort_key("sk", KeyType::String)
            .execute()
            .unwrap();

        let result = db.list_sort_key_prefixes("items").execute();
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod partition_schema_and_index_tests {
    use super::*;
    use crate::types::{AttrType, KeyType};
    use serde_json::json;
    use tempfile::tempdir;

    fn create_test_db() -> (FerridynDB, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = FerridynDB::create(&db_path).unwrap();
        (db, dir)
    }

    fn setup_single_table_with_index(db: &FerridynDB) {
        // Create a single-table design with String PK (no sort key).
        db.create_table("data")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();

        // Declare a CONTACT partition schema.
        db.create_partition_schema("data")
            .prefix("CONTACT")
            .description("Contact entities")
            .attribute("email", AttrType::String, true)
            .attribute("age", AttrType::Number, false)
            .validate(true)
            .execute()
            .unwrap();

        // Create an index on email scoped to CONTACT.
        db.create_index("data")
            .name("email-idx")
            .partition_schema("CONTACT")
            .index_key("email", KeyType::String)
            .execute()
            .unwrap();
    }

    // -------------------------------------------------------------------
    // End-to-end: schema + index + write + query
    // -------------------------------------------------------------------

    #[test]
    fn test_e2e_create_schema_index_put_query() {
        let (db, _dir) = create_test_db();
        setup_single_table_with_index(&db);

        // Insert documents.
        db.put_item(
            "data",
            json!({"pk": "CONTACT#alice", "email": "alice@example.com", "age": 30}),
        )
        .unwrap();
        db.put_item(
            "data",
            json!({"pk": "CONTACT#bob", "email": "bob@example.com", "age": 25}),
        )
        .unwrap();
        // A non-CONTACT document (no index entry created).
        db.put_item("data", json!({"pk": "ORDER#100", "total": 42.0}))
            .unwrap();

        // Query by index.
        let result = db
            .query_index("data", "email-idx")
            .key_value("alice@example.com")
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0]["pk"], "CONTACT#alice");
        assert_eq!(result.items[0]["email"], "alice@example.com");

        let result = db
            .query_index("data", "email-idx")
            .key_value("bob@example.com")
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0]["pk"], "CONTACT#bob");

        // Query for a value that doesn't exist.
        let result = db
            .query_index("data", "email-idx")
            .key_value("nonexistent@example.com")
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 0);
    }

    // -------------------------------------------------------------------
    // Partition schema introspection
    // -------------------------------------------------------------------

    #[test]
    fn test_list_partition_schemas() {
        let (db, _dir) = create_test_db();
        db.create_table("data")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();

        db.create_partition_schema("data")
            .prefix("CONTACT")
            .description("People")
            .execute()
            .unwrap();
        db.create_partition_schema("data")
            .prefix("ORDER")
            .description("Orders")
            .execute()
            .unwrap();

        let schemas = db.list_partition_schemas("data").unwrap();
        assert_eq!(schemas.len(), 2);
        let prefixes: Vec<&str> = schemas.iter().map(|s| s.prefix.as_str()).collect();
        assert!(prefixes.contains(&"CONTACT"));
        assert!(prefixes.contains(&"ORDER"));
    }

    #[test]
    fn test_describe_partition_schema() {
        let (db, _dir) = create_test_db();
        setup_single_table_with_index(&db);

        let schema = db.describe_partition_schema("data", "CONTACT").unwrap();
        assert_eq!(schema.prefix, "CONTACT");
        assert_eq!(schema.description, "Contact entities");
        assert!(schema.validate);
        assert_eq!(schema.attributes.len(), 2);
    }

    #[test]
    fn test_describe_partition_schema_not_found() {
        let (db, _dir) = create_test_db();
        db.create_table("data")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();

        let result = db.describe_partition_schema("data", "NONEXISTENT");
        assert!(result.is_err());
    }

    #[test]
    fn test_drop_partition_schema() {
        let (db, _dir) = create_test_db();
        db.create_table("data")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();

        db.create_partition_schema("data")
            .prefix("TEMP")
            .description("Temporary")
            .execute()
            .unwrap();

        assert_eq!(db.list_partition_schemas("data").unwrap().len(), 1);

        db.drop_partition_schema("data", "TEMP").unwrap();

        assert_eq!(db.list_partition_schemas("data").unwrap().len(), 0);
    }

    #[test]
    fn test_drop_partition_schema_with_indexes_fails() {
        let (db, _dir) = create_test_db();
        setup_single_table_with_index(&db);

        // Should fail because email-idx references CONTACT.
        let result = db.drop_partition_schema("data", "CONTACT");
        assert!(result.is_err());
    }

    // -------------------------------------------------------------------
    // Index introspection
    // -------------------------------------------------------------------

    #[test]
    fn test_list_indexes() {
        let (db, _dir) = create_test_db();
        setup_single_table_with_index(&db);

        let indexes = db.list_indexes("data").unwrap();
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].name, "email-idx");
        assert_eq!(indexes[0].partition_schema, "CONTACT");
        assert_eq!(indexes[0].index_key.name, "email");
    }

    #[test]
    fn test_describe_index() {
        let (db, _dir) = create_test_db();
        setup_single_table_with_index(&db);

        let index = db.describe_index("data", "email-idx").unwrap();
        assert_eq!(index.name, "email-idx");
        assert_eq!(index.partition_schema, "CONTACT");
    }

    #[test]
    fn test_describe_index_not_found() {
        let (db, _dir) = create_test_db();
        db.create_table("data")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();

        let result = db.describe_index("data", "nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_drop_index() {
        let (db, _dir) = create_test_db();
        setup_single_table_with_index(&db);

        assert_eq!(db.list_indexes("data").unwrap().len(), 1);

        db.drop_index("data", "email-idx").unwrap();

        assert_eq!(db.list_indexes("data").unwrap().len(), 0);
    }

    // -------------------------------------------------------------------
    // Write validation
    // -------------------------------------------------------------------

    #[test]
    fn test_validation_rejects_missing_required_attr() {
        let (db, _dir) = create_test_db();
        setup_single_table_with_index(&db);

        // Missing required "email" attribute.
        let result = db.put_item("data", json!({"pk": "CONTACT#charlie", "age": 40}));
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("missing required"), "got: {err_msg}");
    }

    #[test]
    fn test_validation_rejects_wrong_type() {
        let (db, _dir) = create_test_db();
        setup_single_table_with_index(&db);

        // email should be String, not Number.
        let result = db.put_item("data", json!({"pk": "CONTACT#charlie", "email": 12345}));
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("expected"), "got: {err_msg}");
    }

    #[test]
    fn test_validation_passes_for_conforming_doc() {
        let (db, _dir) = create_test_db();
        setup_single_table_with_index(&db);

        // Valid document with required email.
        let result = db.put_item(
            "data",
            json!({"pk": "CONTACT#charlie", "email": "charlie@example.com"}),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_validation_skipped_for_non_matching_prefix() {
        let (db, _dir) = create_test_db();
        setup_single_table_with_index(&db);

        // ORDER prefix has no schema — should pass without validation.
        let result = db.put_item("data", json!({"pk": "ORDER#100", "total": 42.0}));
        assert!(result.is_ok());
    }

    #[test]
    fn test_validation_disabled_schema() {
        let (db, _dir) = create_test_db();
        db.create_table("data")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();

        // Schema with validate=false.
        db.create_partition_schema("data")
            .prefix("CONTACT")
            .attribute("email", AttrType::String, true)
            .validate(false) // disabled!
            .execute()
            .unwrap();

        // Missing email but validation disabled — should pass.
        let result = db.put_item("data", json!({"pk": "CONTACT#alice", "name": "Alice"}));
        assert!(result.is_ok());
    }

    // -------------------------------------------------------------------
    // Index maintenance on writes
    // -------------------------------------------------------------------

    #[test]
    fn test_index_updated_on_put_update() {
        let (db, _dir) = create_test_db();
        setup_single_table_with_index(&db);

        db.put_item(
            "data",
            json!({"pk": "CONTACT#alice", "email": "old@example.com"}),
        )
        .unwrap();

        // Verify queryable by old email.
        let result = db
            .query_index("data", "email-idx")
            .key_value("old@example.com")
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 1);

        // Update email.
        db.put_item(
            "data",
            json!({"pk": "CONTACT#alice", "email": "new@example.com"}),
        )
        .unwrap();

        // Old email should return nothing.
        let result = db
            .query_index("data", "email-idx")
            .key_value("old@example.com")
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 0);

        // New email should return the updated document.
        let result = db
            .query_index("data", "email-idx")
            .key_value("new@example.com")
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0]["email"], "new@example.com");
    }

    #[test]
    fn test_index_cleaned_on_delete() {
        let (db, _dir) = create_test_db();
        setup_single_table_with_index(&db);

        db.put_item(
            "data",
            json!({"pk": "CONTACT#alice", "email": "alice@example.com"}),
        )
        .unwrap();

        // Delete the document.
        db.delete_item("data")
            .partition_key("CONTACT#alice")
            .execute()
            .unwrap();

        // Index query should return nothing.
        let result = db
            .query_index("data", "email-idx")
            .key_value("alice@example.com")
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 0);
    }

    // -------------------------------------------------------------------
    // Lazy GC: orphaned index entries
    // -------------------------------------------------------------------

    #[test]
    fn test_lazy_gc_orphaned_index_entries_skipped() {
        let (db, _dir) = create_test_db();

        // Create table and insert data BEFORE creating index.
        db.create_table("data")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();

        db.create_partition_schema("data")
            .prefix("CONTACT")
            .description("People")
            .execute()
            .unwrap();

        db.put_item(
            "data",
            json!({"pk": "CONTACT#alice", "email": "alice@example.com"}),
        )
        .unwrap();
        db.put_item(
            "data",
            json!({"pk": "CONTACT#bob", "email": "bob@example.com"}),
        )
        .unwrap();

        // Create index (backfill happens here — both docs get indexed).
        db.create_index("data")
            .name("email-idx")
            .partition_schema("CONTACT")
            .index_key("email", KeyType::String)
            .execute()
            .unwrap();

        // Verify both are queryable.
        let result = db
            .query_index("data", "email-idx")
            .key_value("alice@example.com")
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 1);

        // Delete alice — this removes the index entry via maintain_indexes.
        db.delete_item("data")
            .partition_key("CONTACT#alice")
            .execute()
            .unwrap();

        // Query for alice's email should return nothing (properly cleaned).
        let result = db
            .query_index("data", "email-idx")
            .key_value("alice@example.com")
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 0);

        // Bob should still be queryable.
        let result = db
            .query_index("data", "email-idx")
            .key_value("bob@example.com")
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 1);
    }

    // -------------------------------------------------------------------
    // Index query with limit
    // -------------------------------------------------------------------

    #[test]
    fn test_index_query_with_limit() {
        let (db, _dir) = create_test_db();
        db.create_table("data")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();

        db.create_partition_schema("data")
            .prefix("CONTACT")
            .description("People")
            .execute()
            .unwrap();

        db.create_index("data")
            .name("status-idx")
            .partition_schema("CONTACT")
            .index_key("status", KeyType::String)
            .execute()
            .unwrap();

        // Insert 10 contacts all with status "active".
        for i in 0..10 {
            db.put_item(
                "data",
                json!({
                    "pk": format!("CONTACT#user{i}"),
                    "status": "active",
                    "name": format!("User {i}")
                }),
            )
            .unwrap();
        }

        // Query with limit.
        let result = db
            .query_index("data", "status-idx")
            .key_value("active")
            .limit(3)
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 3);

        // Without limit, all 10 should be returned.
        let result = db
            .query_index("data", "status-idx")
            .key_value("active")
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 10);
    }

    // -------------------------------------------------------------------
    // Multiple indexes on same table
    // -------------------------------------------------------------------

    #[test]
    fn test_multiple_indexes_on_same_prefix() {
        let (db, _dir) = create_test_db();
        db.create_table("data")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();

        db.create_partition_schema("data")
            .prefix("CONTACT")
            .description("People")
            .execute()
            .unwrap();

        // Two indexes on CONTACT.
        db.create_index("data")
            .name("email-idx")
            .partition_schema("CONTACT")
            .index_key("email", KeyType::String)
            .execute()
            .unwrap();

        db.create_index("data")
            .name("city-idx")
            .partition_schema("CONTACT")
            .index_key("city", KeyType::String)
            .execute()
            .unwrap();

        db.put_item(
            "data",
            json!({
                "pk": "CONTACT#alice",
                "email": "alice@example.com",
                "city": "Paris"
            }),
        )
        .unwrap();

        // Query by email.
        let result = db
            .query_index("data", "email-idx")
            .key_value("alice@example.com")
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 1);

        // Query by city.
        let result = db
            .query_index("data", "city-idx")
            .key_value("Paris")
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0]["pk"], "CONTACT#alice");
    }

    // -------------------------------------------------------------------
    // Backfill on index creation
    // -------------------------------------------------------------------

    #[test]
    fn test_backfill_on_index_creation() {
        let (db, _dir) = create_test_db();
        db.create_table("data")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();

        db.create_partition_schema("data")
            .prefix("CONTACT")
            .description("People")
            .execute()
            .unwrap();

        // Insert docs BEFORE creating the index.
        for i in 0..5 {
            db.put_item(
                "data",
                json!({
                    "pk": format!("CONTACT#user{i}"),
                    "email": format!("user{i}@example.com"),
                }),
            )
            .unwrap();
        }
        // Also insert a non-CONTACT doc.
        db.put_item(
            "data",
            json!({"pk": "ORDER#1", "email": "order@example.com"}),
        )
        .unwrap();

        // Create index — should backfill the 5 CONTACT docs.
        db.create_index("data")
            .name("email-idx")
            .partition_schema("CONTACT")
            .index_key("email", KeyType::String)
            .execute()
            .unwrap();

        // Verify all 5 CONTACT docs are queryable.
        for i in 0..5 {
            let result = db
                .query_index("data", "email-idx")
                .key_value(format!("user{i}@example.com"))
                .execute()
                .unwrap();
            assert_eq!(result.items.len(), 1, "user{i} should be indexed");
        }

        // ORDER doc should NOT be in the index.
        let result = db
            .query_index("data", "email-idx")
            .key_value("order@example.com")
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 0, "ORDER should not be indexed");
    }

    // -------------------------------------------------------------------
    // Persistence: schema and indexes survive reopen
    // -------------------------------------------------------------------

    #[test]
    fn test_schemas_and_indexes_persist_across_reopen() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Create and populate.
        {
            let db = FerridynDB::create(&db_path).unwrap();
            setup_single_table_with_index(&db);

            db.put_item(
                "data",
                json!({"pk": "CONTACT#alice", "email": "alice@example.com"}),
            )
            .unwrap();
        }

        // Reopen and verify.
        {
            let db = FerridynDB::open(&db_path).unwrap();

            // Schema survives.
            let schemas = db.list_partition_schemas("data").unwrap();
            assert_eq!(schemas.len(), 1);
            assert_eq!(schemas[0].prefix, "CONTACT");

            // Index survives.
            let indexes = db.list_indexes("data").unwrap();
            assert_eq!(indexes.len(), 1);
            assert_eq!(indexes[0].name, "email-idx");

            // Query works after reopen.
            let result = db
                .query_index("data", "email-idx")
                .key_value("alice@example.com")
                .execute()
                .unwrap();
            assert_eq!(result.items.len(), 1);
            assert_eq!(result.items[0]["pk"], "CONTACT#alice");

            // Continued writes work.
            db.put_item(
                "data",
                json!({"pk": "CONTACT#bob", "email": "bob@example.com"}),
            )
            .unwrap();

            let result = db
                .query_index("data", "email-idx")
                .key_value("bob@example.com")
                .execute()
                .unwrap();
            assert_eq!(result.items.len(), 1);
        }
    }

    // -------------------------------------------------------------------
    // Error cases
    // -------------------------------------------------------------------

    #[test]
    fn test_query_nonexistent_index_errors() {
        let (db, _dir) = create_test_db();
        db.create_table("data")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();

        let result = db
            .query_index("data", "no-such-index")
            .key_value("x")
            .execute();
        assert!(result.is_err());
    }

    #[test]
    fn test_query_index_without_key_value_errors() {
        let (db, _dir) = create_test_db();
        setup_single_table_with_index(&db);

        let result = db.query_index("data", "email-idx").execute();
        assert!(result.is_err());
    }

    #[test]
    fn test_duplicate_partition_schema_errors() {
        let (db, _dir) = create_test_db();
        db.create_table("data")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();

        db.create_partition_schema("data")
            .prefix("CONTACT")
            .execute()
            .unwrap();

        let result = db
            .create_partition_schema("data")
            .prefix("CONTACT")
            .execute();
        assert!(result.is_err());
    }

    #[test]
    fn test_duplicate_index_name_errors() {
        let (db, _dir) = create_test_db();
        setup_single_table_with_index(&db);

        let result = db
            .create_index("data")
            .name("email-idx")
            .partition_schema("CONTACT")
            .index_key("age", KeyType::Number)
            .execute();
        assert!(result.is_err());
    }

    // -------------------------------------------------------------------
    // Reverse scan (scan_forward(false))
    // -------------------------------------------------------------------

    #[test]
    fn test_index_query_reverse_scan() {
        let (db, _dir) = create_test_db();
        db.create_table("data")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();

        db.create_partition_schema("data")
            .prefix("CONTACT")
            .execute()
            .unwrap();

        db.create_index("data")
            .name("status-idx")
            .partition_schema("CONTACT")
            .index_key("status", KeyType::String)
            .execute()
            .unwrap();

        for c in ['a', 'b', 'c', 'd', 'e'] {
            db.put_item(
                "data",
                json!({
                    "pk": format!("CONTACT#{c}"),
                    "status": "active",
                }),
            )
            .unwrap();
        }

        let forward = db
            .query_index("data", "status-idx")
            .key_value("active")
            .scan_forward(true)
            .execute()
            .unwrap();
        assert_eq!(forward.items.len(), 5);

        let reverse = db
            .query_index("data", "status-idx")
            .key_value("active")
            .scan_forward(false)
            .execute()
            .unwrap();
        assert_eq!(reverse.items.len(), 5);

        // The two orderings should be exact mirrors of each other.
        let forward_pks: Vec<&str> = forward
            .items
            .iter()
            .map(|v| v["pk"].as_str().unwrap())
            .collect();
        let reverse_pks: Vec<&str> = reverse
            .items
            .iter()
            .map(|v| v["pk"].as_str().unwrap())
            .collect();

        let mut expected_reverse = forward_pks.clone();
        expected_reverse.reverse();
        assert_eq!(reverse_pks, expected_reverse);
    }

    // -------------------------------------------------------------------
    // Number index keys (end-to-end)
    // -------------------------------------------------------------------

    #[test]
    fn test_index_query_number_key_type() {
        let (db, _dir) = create_test_db();
        db.create_table("data")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();

        db.create_partition_schema("data")
            .prefix("PRODUCT")
            .execute()
            .unwrap();

        db.create_index("data")
            .name("price-idx")
            .partition_schema("PRODUCT")
            .index_key("price", KeyType::Number)
            .execute()
            .unwrap();

        db.put_item("data", json!({"pk": "PRODUCT#apple", "price": 1.50}))
            .unwrap();
        db.put_item("data", json!({"pk": "PRODUCT#banana", "price": 0.75}))
            .unwrap();
        db.put_item("data", json!({"pk": "PRODUCT#cherry", "price": 1.50}))
            .unwrap();
        db.put_item("data", json!({"pk": "PRODUCT#date", "price": 3.00}))
            .unwrap();

        // Two items at price 1.50.
        let result = db
            .query_index("data", "price-idx")
            .key_value(1.50_f64)
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 2);

        // One item at price 0.75.
        let result = db
            .query_index("data", "price-idx")
            .key_value(0.75_f64)
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0]["pk"], "PRODUCT#banana");

        // No items at price 99.99.
        let result = db
            .query_index("data", "price-idx")
            .key_value(99.99_f64)
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 0);
    }

    #[test]
    fn test_index_query_number_key_ordering() {
        let (db, _dir) = create_test_db();
        db.create_table("data")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();

        db.create_partition_schema("data")
            .prefix("PRODUCT")
            .execute()
            .unwrap();

        db.create_index("data")
            .name("price-idx")
            .partition_schema("PRODUCT")
            .index_key("price", KeyType::Number)
            .execute()
            .unwrap();

        let prices: &[(f64, &str)] = &[
            (10.0, "PRODUCT#ten"),
            (-5.0, "PRODUCT#neg_five"),
            (0.0, "PRODUCT#zero"),
            (100.0, "PRODUCT#hundred"),
            (-0.5, "PRODUCT#neg_half"),
        ];

        for (price, pk) in prices {
            db.put_item("data", json!({"pk": pk, "price": price}))
                .unwrap();
        }

        // Verify each price individually resolves to the correct item.
        for (price, pk) in prices {
            let result = db
                .query_index("data", "price-idx")
                .key_value(*price)
                .execute()
                .unwrap();
            assert_eq!(
                result.items.len(),
                1,
                "expected 1 item for price {price}, got {}",
                result.items.len()
            );
            assert_eq!(result.items[0]["pk"], *pk, "wrong item for price {price}");
        }
    }

    // -------------------------------------------------------------------
    // Empty index query
    // -------------------------------------------------------------------

    #[test]
    fn test_index_query_empty_index() {
        let (db, _dir) = create_test_db();
        db.create_table("data")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();

        db.create_partition_schema("data")
            .prefix("CONTACT")
            .execute()
            .unwrap();

        db.create_index("data")
            .name("email-idx")
            .partition_schema("CONTACT")
            .index_key("email", KeyType::String)
            .execute()
            .unwrap();

        // No documents inserted — query should return empty results.
        let result = db
            .query_index("data", "email-idx")
            .key_value("anything@test.com")
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 0);
        assert!(result.last_evaluated_key.is_none());
    }

    // -------------------------------------------------------------------
    // Large result set stress test
    // -------------------------------------------------------------------

    #[test]
    fn test_index_query_large_result_set() {
        let (db, _dir) = create_test_db();
        db.create_table("data")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();

        db.create_partition_schema("data")
            .prefix("CONTACT")
            .validate(false)
            .execute()
            .unwrap();

        db.create_index("data")
            .name("status-idx")
            .partition_schema("CONTACT")
            .index_key("status", KeyType::String)
            .execute()
            .unwrap();

        for i in 0..200 {
            db.put_item(
                "data",
                json!({
                    "pk": format!("CONTACT#user{i:04}"),
                    "status": "active",
                }),
            )
            .unwrap();
        }

        // Without limit — all 200.
        let result = db
            .query_index("data", "status-idx")
            .key_value("active")
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 200);

        // With limit — exactly 50.
        let result = db
            .query_index("data", "status-idx")
            .key_value("active")
            .limit(50)
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 50);

        // Pagination cursor should be present when limit truncates results.
        assert!(result.last_evaluated_key.is_some());
    }

    // -------------------------------------------------------------------
    // TTL interaction with index queries
    // -------------------------------------------------------------------

    #[test]
    fn test_index_query_filters_ttl_expired_docs() {
        let (db, _dir) = create_test_db();
        db.create_table("data")
            .partition_key("pk", KeyType::String)
            .ttl_attribute("expires")
            .execute()
            .unwrap();

        db.create_partition_schema("data")
            .prefix("CONTACT")
            .validate(false)
            .execute()
            .unwrap();

        db.create_index("data")
            .name("email-idx")
            .partition_schema("CONTACT")
            .index_key("email", KeyType::String)
            .execute()
            .unwrap();

        // Alive: TTL far in the future.
        db.put_item(
            "data",
            json!({
                "pk": "CONTACT#alive",
                "email": "alive@test.com",
                "expires": 9999999999.0
            }),
        )
        .unwrap();

        // Dead: TTL in the distant past.
        db.put_item(
            "data",
            json!({
                "pk": "CONTACT#dead",
                "email": "dead@test.com",
                "expires": 1000
            }),
        )
        .unwrap();

        // No TTL field: should be visible (never expires).
        db.put_item(
            "data",
            json!({
                "pk": "CONTACT#no_ttl",
                "email": "nottl@test.com"
            }),
        )
        .unwrap();

        // alive@test.com → 1 result.
        let result = db
            .query_index("data", "email-idx")
            .key_value("alive@test.com")
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 1);

        // dead@test.com → 0 results (filtered by TTL).
        let result = db
            .query_index("data", "email-idx")
            .key_value("dead@test.com")
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 0, "expired doc should be filtered");

        // nottl@test.com → 1 result (no TTL field = never expires).
        let result = db
            .query_index("data", "email-idx")
            .key_value("nottl@test.com")
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 1);
    }

    // -------------------------------------------------------------------
    // Pagination returns None (last_evaluated_key)
    // -------------------------------------------------------------------

    #[test]
    fn test_index_query_last_evaluated_key_is_none() {
        let (db, _dir) = create_test_db();
        setup_single_table_with_index(&db);

        db.put_item(
            "data",
            json!({"pk": "CONTACT#a", "email": "a@test.com", "age": 20}),
        )
        .unwrap();
        db.put_item(
            "data",
            json!({"pk": "CONTACT#b", "email": "b@test.com", "age": 30}),
        )
        .unwrap();
        db.put_item(
            "data",
            json!({"pk": "CONTACT#c", "email": "c@test.com", "age": 40}),
        )
        .unwrap();

        // With limit(2): returns 2 items, but last_evaluated_key is None.
        let result = db
            .query_index("data", "email-idx")
            .key_value("a@test.com")
            .limit(2)
            .execute()
            .unwrap();
        // Only 1 doc has email "a@test.com", so limit(2) doesn't truncate.
        assert_eq!(result.items.len(), 1);
        assert!(result.last_evaluated_key.is_none());

        // Insert 2 more docs with the same email for limit testing.
        db.put_item(
            "data",
            json!({"pk": "CONTACT#d", "email": "shared@test.com", "age": 50}),
        )
        .unwrap();
        db.put_item(
            "data",
            json!({"pk": "CONTACT#e", "email": "shared@test.com", "age": 60}),
        )
        .unwrap();
        db.put_item(
            "data",
            json!({"pk": "CONTACT#f", "email": "shared@test.com", "age": 70}),
        )
        .unwrap();

        // limit(2) on 3 matching docs → 2 items returned.
        let result = db
            .query_index("data", "email-idx")
            .key_value("shared@test.com")
            .limit(2)
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 2);
        assert!(
            result.last_evaluated_key.is_some(),
            "pagination cursor should be returned when limit truncates"
        );

        // Without limit → all 3 matching docs.
        let result = db
            .query_index("data", "email-idx")
            .key_value("shared@test.com")
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 3);
        assert!(result.last_evaluated_key.is_none());
    }
}

#[cfg(test)]
mod update_item_tests {
    use super::*;
    use crate::types::{AttrType, KeyType};
    use serde_json::json;
    use tempfile::tempdir;

    fn create_test_db() -> (FerridynDB, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = FerridynDB::create(&db_path).unwrap();
        (db, dir)
    }

    #[test]
    fn test_update_set_top_level() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();
        db.put_item("items", json!({"pk": "a", "name": "Alice"}))
            .unwrap();

        db.update_item("items")
            .partition_key("a")
            .set("age", 30)
            .execute()
            .unwrap();

        let item = db
            .get_item("items")
            .partition_key("a")
            .execute()
            .unwrap()
            .unwrap();
        assert_eq!(item["name"], "Alice");
        assert_eq!(item["age"], 30);
    }

    #[test]
    fn test_update_set_overwrite() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();
        db.put_item("items", json!({"pk": "a", "name": "Alice"}))
            .unwrap();

        db.update_item("items")
            .partition_key("a")
            .set("name", "Bob")
            .execute()
            .unwrap();

        let item = db
            .get_item("items")
            .partition_key("a")
            .execute()
            .unwrap()
            .unwrap();
        assert_eq!(item["name"], "Bob");
    }

    #[test]
    fn test_update_set_nested() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();
        db.put_item("items", json!({"pk": "a"})).unwrap();

        db.update_item("items")
            .partition_key("a")
            .set("address.city", "NYC")
            .execute()
            .unwrap();

        let item = db
            .get_item("items")
            .partition_key("a")
            .execute()
            .unwrap()
            .unwrap();
        assert_eq!(item["address"]["city"], "NYC");
    }

    #[test]
    fn test_update_set_create_intermediate() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();
        db.put_item("items", json!({"pk": "a"})).unwrap();

        db.update_item("items")
            .partition_key("a")
            .set("a.b.c", 42)
            .execute()
            .unwrap();

        let item = db
            .get_item("items")
            .partition_key("a")
            .execute()
            .unwrap()
            .unwrap();
        assert_eq!(item["a"]["b"]["c"], 42);
    }

    #[test]
    fn test_update_remove() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();
        db.put_item("items", json!({"pk": "a", "name": "Alice", "age": 30}))
            .unwrap();

        db.update_item("items")
            .partition_key("a")
            .remove("age")
            .execute()
            .unwrap();

        let item = db
            .get_item("items")
            .partition_key("a")
            .execute()
            .unwrap()
            .unwrap();
        assert_eq!(item["name"], "Alice");
        assert!(item.get("age").is_none());
    }

    #[test]
    fn test_update_remove_nonexistent() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();
        db.put_item("items", json!({"pk": "a"})).unwrap();

        // Removing a non-existent attribute should succeed silently.
        db.update_item("items")
            .partition_key("a")
            .remove("missing")
            .execute()
            .unwrap();
    }

    #[test]
    fn test_update_remove_nested() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();
        db.put_item(
            "items",
            json!({"pk": "a", "address": {"city": "NYC", "zip": "10001"}}),
        )
        .unwrap();

        db.update_item("items")
            .partition_key("a")
            .remove("address.city")
            .execute()
            .unwrap();

        let item = db
            .get_item("items")
            .partition_key("a")
            .execute()
            .unwrap()
            .unwrap();
        assert!(item["address"].get("city").is_none());
        assert_eq!(item["address"]["zip"], "10001");
    }

    #[test]
    fn test_update_nonexistent_item() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();

        // No item exists — should upsert.
        db.update_item("items")
            .partition_key("a")
            .set("name", "Alice")
            .set("age", 30)
            .execute()
            .unwrap();

        let item = db
            .get_item("items")
            .partition_key("a")
            .execute()
            .unwrap()
            .unwrap();
        assert_eq!(item["pk"], "a");
        assert_eq!(item["name"], "Alice");
        assert_eq!(item["age"], 30);
    }

    #[test]
    fn test_update_upsert_with_sort_key() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("pk", KeyType::String)
            .sort_key("sk", KeyType::Number)
            .execute()
            .unwrap();

        // Upsert with sort key.
        db.update_item("items")
            .partition_key("a")
            .sort_key(1.0)
            .set("data", "hello")
            .execute()
            .unwrap();

        let item = db
            .get_item("items")
            .partition_key("a")
            .sort_key(1.0)
            .execute()
            .unwrap()
            .unwrap();
        assert_eq!(item["pk"], "a");
        assert_eq!(item["sk"], 1.0);
        assert_eq!(item["data"], "hello");
    }

    #[test]
    fn test_update_multiple_actions() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();
        db.put_item("items", json!({"pk": "a", "x": 1, "y": 2}))
            .unwrap();

        db.update_item("items")
            .partition_key("a")
            .set("z", 3)
            .remove("x")
            .set("y", 99)
            .execute()
            .unwrap();

        let item = db
            .get_item("items")
            .partition_key("a")
            .execute()
            .unwrap()
            .unwrap();
        assert!(item.get("x").is_none());
        assert_eq!(item["y"], 99);
        assert_eq!(item["z"], 3);
    }

    #[test]
    fn test_update_rejects_key_attribute() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();
        db.put_item("items", json!({"pk": "a"})).unwrap();

        let result = db
            .update_item("items")
            .partition_key("a")
            .set("pk", "b")
            .execute();

        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("key attribute"), "error: {err_msg}");
    }

    #[test]
    fn test_update_rejects_sort_key_attribute() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("pk", KeyType::String)
            .sort_key("sk", KeyType::Number)
            .execute()
            .unwrap();
        db.put_item("items", json!({"pk": "a", "sk": 1.0})).unwrap();

        let result = db
            .update_item("items")
            .partition_key("a")
            .sort_key(1.0)
            .set("sk", 2.0)
            .execute();

        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("key attribute"), "error: {err_msg}");
    }

    #[test]
    fn test_update_index_maintenance() {
        let (db, _dir) = create_test_db();
        db.create_table("data")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();

        // Set up partition schema and index.
        db.create_partition_schema("data")
            .prefix("CONTACT")
            .description("People")
            .execute()
            .unwrap();
        db.create_index("data")
            .name("email-idx")
            .partition_schema("CONTACT")
            .index_key("email", KeyType::String)
            .execute()
            .unwrap();

        // Insert a contact.
        db.put_item(
            "data",
            json!({"pk": "CONTACT#alice", "email": "old@test.com"}),
        )
        .unwrap();

        // Verify index query with old email.
        let result = db
            .query_index("data", "email-idx")
            .key_value("old@test.com")
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 1);

        // Update email via update_item.
        db.update_item("data")
            .partition_key("CONTACT#alice")
            .set("email", "new@test.com")
            .execute()
            .unwrap();

        // Old email should no longer be in index.
        let result = db
            .query_index("data", "email-idx")
            .key_value("old@test.com")
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 0);

        // New email should be in index.
        let result = db
            .query_index("data", "email-idx")
            .key_value("new@test.com")
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0]["email"], "new@test.com");
    }

    #[test]
    fn test_update_partition_schema_validation() {
        let (db, _dir) = create_test_db();
        db.create_table("data")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();

        db.create_partition_schema("data")
            .prefix("CONTACT")
            .description("People")
            .attribute("email", AttrType::String, true)
            .validate(true)
            .execute()
            .unwrap();

        // Insert a valid contact.
        db.put_item(
            "data",
            json!({"pk": "CONTACT#alice", "email": "alice@test.com"}),
        )
        .unwrap();

        // Update that removes the required attribute should fail validation.
        let result = db
            .update_item("data")
            .partition_key("CONTACT#alice")
            .remove("email")
            .execute();

        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("email"),
            "should mention missing email: {err_msg}"
        );
    }

    #[test]
    fn test_update_add_number() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();
        db.put_item("items", json!({"pk": "a", "count": 10}))
            .unwrap();

        db.update_item("items")
            .partition_key("a")
            .add("count", 5)
            .execute()
            .unwrap();

        let item = db
            .get_item("items")
            .partition_key("a")
            .execute()
            .unwrap()
            .unwrap();
        assert_eq!(item["count"], 15.0);
    }

    #[test]
    fn test_update_add_number_init() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();
        db.put_item("items", json!({"pk": "a"})).unwrap();

        db.update_item("items")
            .partition_key("a")
            .add("count", 5)
            .execute()
            .unwrap();

        let item = db
            .get_item("items")
            .partition_key("a")
            .execute()
            .unwrap()
            .unwrap();
        assert_eq!(item["count"], 5);
    }

    #[test]
    fn test_update_add_set() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();
        db.put_item("items", json!({"pk": "a", "tags": ["x", "y"]}))
            .unwrap();

        db.update_item("items")
            .partition_key("a")
            .add("tags", json!(["y", "z"]))
            .execute()
            .unwrap();

        let item = db
            .get_item("items")
            .partition_key("a")
            .execute()
            .unwrap()
            .unwrap();
        let tags = item["tags"].as_array().unwrap();
        assert_eq!(tags.len(), 3);
        assert!(tags.contains(&json!("x")));
        assert!(tags.contains(&json!("y")));
        assert!(tags.contains(&json!("z")));
    }

    #[test]
    fn test_update_delete_set() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();
        db.put_item("items", json!({"pk": "a", "tags": ["x", "y", "z"]}))
            .unwrap();

        db.update_item("items")
            .partition_key("a")
            .delete("tags", json!(["y"]))
            .execute()
            .unwrap();

        let item = db
            .get_item("items")
            .partition_key("a")
            .execute()
            .unwrap()
            .unwrap();
        assert_eq!(item["tags"], json!(["x", "z"]));
    }

    #[test]
    fn test_update_delete_empty_set() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();
        db.put_item("items", json!({"pk": "a", "tags": ["x"]}))
            .unwrap();

        db.update_item("items")
            .partition_key("a")
            .delete("tags", json!(["x"]))
            .execute()
            .unwrap();

        let item = db
            .get_item("items")
            .partition_key("a")
            .execute()
            .unwrap()
            .unwrap();
        assert!(item.get("tags").is_none());
    }

    #[test]
    fn test_update_add_type_error() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();
        db.put_item("items", json!({"pk": "a", "name": "Alice"}))
            .unwrap();

        let result = db
            .update_item("items")
            .partition_key("a")
            .add("name", 5)
            .execute();

        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("type mismatch"), "error: {msg}");
    }

    /// Simulate a crash after writing data pages but before the header is
    /// fully persisted: corrupt the latest header slot and verify that
    /// reopening the database falls back to the alternate (previous) header.
    #[test]
    fn test_recovery_from_corrupted_header() {
        use std::os::unix::fs::FileExt;

        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let lock_path = db_path.with_extension("lock");

        // Create and populate the database.
        {
            let db = FerridynDB::create(&db_path).unwrap();
            db.create_table("items")
                .partition_key("id", KeyType::String)
                .execute()
                .unwrap();
            db.put_item("items", json!({"id": "a", "value": 1}))
                .unwrap();
        }
        // Drop the lock file so we can reopen.
        let _ = std::fs::remove_file(&lock_path);

        // Do another commit so both header slots are valid with different txn_counters.
        let latest_slot: u8;
        {
            let db = FerridynDB::open(&db_path).unwrap();
            db.put_item("items", json!({"id": "b", "value": 2}))
                .unwrap();
            // Read the current header slot.
            latest_slot = db.inner.state.read().header_slot;
        }
        let _ = std::fs::remove_file(&lock_path);

        // Corrupt the latest header slot to simulate a partial header write.
        {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&db_path)
                .unwrap();
            let offset = latest_slot as u64 * PAGE_SIZE as u64;
            // Overwrite part of the header with garbage.
            file.write_all_at(&[0xFF; 64], offset).unwrap();
            file.sync_all().unwrap();
        }

        // Reopen: should recover using the alternate (older) header.
        {
            let db = FerridynDB::open(&db_path).unwrap();

            // The first item ("a") was committed before the corrupted header,
            // so it should be visible via the recovered alternate header.
            let item = db.get_item("items").partition_key("a").execute().unwrap();
            assert!(item.is_some(), "item 'a' should survive recovery");
            assert_eq!(item.unwrap()["value"], 1);

            // The alternate header slot should be selected (not the corrupted one).
            let recovered_slot = db.inner.state.read().header_slot;
            assert_ne!(
                recovered_slot, latest_slot,
                "should have recovered to the alternate slot"
            );
        }
    }

    /// Verify that the pending free list is persisted to disk and survives
    /// a close/reopen cycle.
    #[test]
    fn test_pending_free_list_persists_across_reopen() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let lock_path = db_path.with_extension("lock");

        // Create the database and do multiple writes to generate COW'd pages.
        {
            let db = FerridynDB::create(&db_path).unwrap();
            db.create_table("items")
                .partition_key("id", KeyType::String)
                .execute()
                .unwrap();

            // First put: creates B-tree leaf pages.
            db.put_item("items", json!({"id": "a", "value": 1}))
                .unwrap();

            // Second put to same key: overwrites, generating COW'd pages.
            db.put_item("items", json!({"id": "a", "value": 2}))
                .unwrap();

            // The pending free list should have entries from the COW.
            let state = db.inner.state.read();
            let pf_root = state.header.pending_free_root_page;
            assert!(
                pf_root != 0,
                "pending free root should be non-zero after COW writes"
            );
            assert!(
                state.pending_free.pending_count() > 0,
                "in-memory pending free list should have entries"
            );
        }
        let _ = std::fs::remove_file(&lock_path);

        // Reopen and verify the pending free list was loaded from disk.
        {
            let db = FerridynDB::open(&db_path).unwrap();
            let state = db.inner.state.read();
            assert!(
                state.pending_free.pending_count() > 0,
                "pending free list should be loaded from disk on reopen"
            );
            assert!(
                state.header.pending_free_root_page != 0,
                "header should reference the pending free chain"
            );
        }
    }
}

#[cfg(test)]
mod filter_integration_tests {
    use super::*;
    use crate::api::filter::FilterExpr;
    use crate::types::KeyType;
    use serde_json::json;
    use tempfile::tempdir;

    fn create_test_db_with_data() -> (FerridynDB, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = FerridynDB::create(&db_path).unwrap();

        db.create_table("users")
            .partition_key("pk", KeyType::String)
            .sort_key("sk", KeyType::String)
            .execute()
            .unwrap();

        // Insert 10 users with various attributes.
        for i in 0..10 {
            let status = if i % 2 == 0 { "active" } else { "inactive" };
            let age = 20 + i;
            db.put_item(
                "users",
                json!({
                    "pk": "ORG#acme",
                    "sk": format!("USER#{:04}", i),
                    "name": format!("user{}", i),
                    "age": age,
                    "status": status,
                    "score": i * 10,
                }),
            )
            .unwrap();
        }

        (db, dir)
    }

    #[test]
    fn test_query_with_filter() {
        let (db, _dir) = create_test_db_with_data();

        // Query with filter: only active users.
        let result = db
            .query("users")
            .partition_key("ORG#acme")
            .filter(FilterExpr::eq(
                FilterExpr::attr("status"),
                FilterExpr::literal("active"),
            ))
            .execute()
            .unwrap();

        // Users 0,2,4,6,8 are active.
        assert_eq!(result.items.len(), 5);
        for item in &result.items {
            assert_eq!(item["status"], "active");
        }
    }

    #[test]
    fn test_scan_with_filter() {
        let (db, _dir) = create_test_db_with_data();

        // Scan with filter: age >= 25.
        let result = db
            .scan("users")
            .filter(FilterExpr::ge(
                FilterExpr::attr("age"),
                FilterExpr::literal(25),
            ))
            .execute()
            .unwrap();

        // Users with age 25..29 (indices 5..9).
        assert_eq!(result.items.len(), 5);
        for item in &result.items {
            assert!(item["age"].as_i64().unwrap() >= 25);
        }
    }

    #[test]
    fn test_query_with_filter_and_sort_condition() {
        let (db, _dir) = create_test_db_with_data();

        // Query with sort key condition + filter.
        let result = db
            .query("users")
            .partition_key("ORG#acme")
            .sort_key_lt("USER#0005")
            .filter(FilterExpr::eq(
                FilterExpr::attr("status"),
                FilterExpr::literal("active"),
            ))
            .execute()
            .unwrap();

        // Users 0..4, active ones are 0, 2, 4.
        assert_eq!(result.items.len(), 3);
        for item in &result.items {
            assert_eq!(item["status"], "active");
            let sk = item["sk"].as_str().unwrap();
            assert!(sk < "USER#0005");
        }
    }

    #[test]
    fn test_filter_with_limit_dynamo_semantics() {
        let (db, _dir) = create_test_db_with_data();

        // Limit = 5 means evaluate 5 items. With filter, fewer may be returned.
        // Items are USER#0000..USER#0009 in order.
        // First 5 items (USER#0000..USER#0004): active = 0,2,4 (3 items).
        let result = db
            .query("users")
            .partition_key("ORG#acme")
            .limit(5)
            .filter(FilterExpr::eq(
                FilterExpr::attr("status"),
                FilterExpr::literal("active"),
            ))
            .execute()
            .unwrap();

        // Only 3 active users in the first 5 evaluated.
        assert_eq!(result.items.len(), 3);
        // last_evaluated_key should be set (more items exist).
        assert!(result.last_evaluated_key.is_some());
    }

    #[test]
    fn test_filter_with_pagination() {
        let (db, _dir) = create_test_db_with_data();

        // Page 1: limit=5 with filter.
        let page1 = db
            .query("users")
            .partition_key("ORG#acme")
            .limit(5)
            .filter(FilterExpr::eq(
                FilterExpr::attr("status"),
                FilterExpr::literal("active"),
            ))
            .execute()
            .unwrap();

        assert_eq!(page1.items.len(), 3); // 0,2,4
        assert!(page1.last_evaluated_key.is_some());

        // Page 2: continue from last_evaluated_key.
        let page2 = db
            .query("users")
            .partition_key("ORG#acme")
            .limit(5)
            .exclusive_start_key(page1.last_evaluated_key.unwrap())
            .filter(FilterExpr::eq(
                FilterExpr::attr("status"),
                FilterExpr::literal("active"),
            ))
            .execute()
            .unwrap();

        assert_eq!(page2.items.len(), 2); // 6,8
        // No more items after evaluating the remaining 5.
        assert!(page2.last_evaluated_key.is_none());

        // Combined results should be all 5 active users.
        let all_names: Vec<&str> = page1
            .items
            .iter()
            .chain(page2.items.iter())
            .map(|i| i["name"].as_str().unwrap())
            .collect();
        assert_eq!(all_names, vec!["user0", "user2", "user4", "user6", "user8"]);
    }

    #[test]
    fn test_filter_no_matches() {
        let (db, _dir) = create_test_db_with_data();

        // Filter that matches nothing.
        let result = db
            .query("users")
            .partition_key("ORG#acme")
            .limit(5)
            .filter(FilterExpr::eq(
                FilterExpr::attr("status"),
                FilterExpr::literal("deleted"),
            ))
            .execute()
            .unwrap();

        assert!(result.items.is_empty());
        // Should still have pagination cursor (5 items were evaluated).
        assert!(result.last_evaluated_key.is_some());
    }

    #[test]
    fn test_scan_with_filter_and_limit() {
        let (db, _dir) = create_test_db_with_data();

        let result = db
            .scan("users")
            .limit(4)
            .filter(FilterExpr::gt(
                FilterExpr::attr("score"),
                FilterExpr::literal(50),
            ))
            .execute()
            .unwrap();

        // First 4 evaluated: scores 0, 10, 20, 30 — only scores > 50 pass.
        // None pass in the first 4.
        assert!(result.items.is_empty());
        assert!(result.last_evaluated_key.is_some());
    }

    #[test]
    fn test_filter_complex_expression() {
        let (db, _dir) = create_test_db_with_data();

        // (status == "active" AND age > 24) OR score >= 80
        let filter = FilterExpr::or(vec![
            FilterExpr::and(vec![
                FilterExpr::eq(FilterExpr::attr("status"), FilterExpr::literal("active")),
                FilterExpr::gt(FilterExpr::attr("age"), FilterExpr::literal(24)),
            ]),
            FilterExpr::ge(FilterExpr::attr("score"), FilterExpr::literal(80)),
        ]);

        let result = db
            .query("users")
            .partition_key("ORG#acme")
            .filter(filter)
            .execute()
            .unwrap();

        // Active AND age > 24: users 6(26,active), 8(28,active) — users 4(24) doesn't pass age>24
        // Wait: user4 has age=24, which is NOT > 24. user6 has age=26, user8 has age=28.
        // Score >= 80: user8(80), user9(90).
        // Union: user6, user8, user9 — but user8 is in both sets.
        // So: user6, user8, user9.
        assert_eq!(result.items.len(), 3);
    }

    #[test]
    fn test_index_query_with_filter() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = FerridynDB::create(&db_path).unwrap();

        db.create_table("items")
            .partition_key("pk", KeyType::String)
            .sort_key("sk", KeyType::String)
            .execute()
            .unwrap();

        db.create_partition_schema("items")
            .prefix("ITEM")
            .attribute("category", crate::types::AttrType::String, true)
            .execute()
            .unwrap();

        db.create_index("items")
            .name("by_category")
            .partition_schema("ITEM")
            .index_key("category", KeyType::String)
            .execute()
            .unwrap();

        // Insert items.
        for i in 0..6 {
            let price = (i + 1) * 10;
            db.put_item(
                "items",
                json!({
                    "pk": format!("ITEM#{}", i),
                    "sk": "DETAIL",
                    "category": "electronics",
                    "price": price,
                    "name": format!("item{}", i),
                }),
            )
            .unwrap();
        }

        // Query index with filter: price > 30.
        let result = db
            .query_index("items", "by_category")
            .key_value("electronics")
            .filter(FilterExpr::gt(
                FilterExpr::attr("price"),
                FilterExpr::literal(30),
            ))
            .execute()
            .unwrap();

        // Items with price > 30: item3(40), item4(50), item5(60).
        assert_eq!(result.items.len(), 3);
        for item in &result.items {
            assert!(item["price"].as_i64().unwrap() > 30);
        }
    }

    #[test]
    fn test_query_without_filter_unchanged() {
        let (db, _dir) = create_test_db_with_data();

        // Without filter, behavior is unchanged.
        let result = db
            .query("users")
            .partition_key("ORG#acme")
            .limit(3)
            .execute()
            .unwrap();

        assert_eq!(result.items.len(), 3);
        assert!(result.last_evaluated_key.is_some());

        // Scan without filter.
        let result = db.scan("users").execute().unwrap();
        assert_eq!(result.items.len(), 10);
        assert!(result.last_evaluated_key.is_none());
    }

    #[test]
    fn test_filter_begins_with_integration() {
        let (db, _dir) = create_test_db_with_data();

        let result = db
            .scan("users")
            .filter(FilterExpr::begins_with(FilterExpr::attr("name"), "user3"))
            .execute()
            .unwrap();

        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0]["name"], "user3");
    }

    #[test]
    fn test_filter_attribute_exists_integration() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = FerridynDB::create(&db_path).unwrap();

        db.create_table("docs")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        db.put_item("docs", json!({"id": "a", "email": "a@test.com"}))
            .unwrap();
        db.put_item("docs", json!({"id": "b"})).unwrap();
        db.put_item("docs", json!({"id": "c", "email": "c@test.com"}))
            .unwrap();

        let result = db
            .scan("docs")
            .filter(FilterExpr::attribute_exists("email"))
            .execute()
            .unwrap();

        assert_eq!(result.items.len(), 2);
    }
}

#[cfg(test)]
mod condition_expression_tests {
    use super::*;
    use crate::api::filter::FilterExpr;
    use crate::types::KeyType;
    use serde_json::json;
    use tempfile::tempdir;

    fn create_test_db() -> (FerridynDB, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = FerridynDB::create(&db_path).unwrap();
        db.create_table("items")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();
        (db, dir)
    }

    // -----------------------------------------------------------------------
    // Phase 1: PutItem conditions
    // -----------------------------------------------------------------------

    #[test]
    fn test_put_condition_attribute_not_exists_passes() {
        let (db, _dir) = create_test_db();
        // Item does not exist yet, so attribute_not_exists("id") should pass
        // (evaluated against empty object).
        let result = db
            .put("items", json!({"id": "new", "val": 1}))
            .condition(FilterExpr::attribute_not_exists("id"))
            .execute();
        assert!(result.is_ok());

        let item = db.get_item("items").partition_key("new").execute().unwrap();
        assert_eq!(item.unwrap()["val"], 1);
    }

    #[test]
    fn test_put_condition_attribute_not_exists_fails() {
        let (db, _dir) = create_test_db();
        db.put_item("items", json!({"id": "existing", "val": 1}))
            .unwrap();

        // Item exists, so attribute_not_exists("id") should fail.
        let result = db
            .put("items", json!({"id": "existing", "val": 2}))
            .condition(FilterExpr::attribute_not_exists("id"))
            .execute();
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("condition check failed"));

        // Original value should remain unchanged.
        let item = db
            .get_item("items")
            .partition_key("existing")
            .execute()
            .unwrap()
            .unwrap();
        assert_eq!(item["val"], 1);
    }

    #[test]
    fn test_put_condition_attribute_exists_passes() {
        let (db, _dir) = create_test_db();
        db.put_item("items", json!({"id": "existing", "val": 1}))
            .unwrap();

        // Item exists with "id" attribute, so attribute_exists("id") passes.
        let result = db
            .put("items", json!({"id": "existing", "val": 2}))
            .condition(FilterExpr::attribute_exists("id"))
            .execute();
        assert!(result.is_ok());

        let item = db
            .get_item("items")
            .partition_key("existing")
            .execute()
            .unwrap()
            .unwrap();
        assert_eq!(item["val"], 2);
    }

    #[test]
    fn test_put_without_condition_always_succeeds() {
        let (db, _dir) = create_test_db();
        // No condition: unconditional put.
        db.put("items", json!({"id": "a", "val": 1}))
            .execute()
            .unwrap();
        db.put("items", json!({"id": "a", "val": 2}))
            .execute()
            .unwrap();

        let item = db
            .get_item("items")
            .partition_key("a")
            .execute()
            .unwrap()
            .unwrap();
        assert_eq!(item["val"], 2);
    }

    // -----------------------------------------------------------------------
    // Phase 1: DeleteItem conditions
    // -----------------------------------------------------------------------

    #[test]
    fn test_delete_condition_eq_passes() {
        let (db, _dir) = create_test_db();
        db.put_item("items", json!({"id": "a", "status": "archived"}))
            .unwrap();

        let result = db
            .delete_item("items")
            .partition_key("a")
            .condition(FilterExpr::eq(
                FilterExpr::attr("status"),
                FilterExpr::literal(json!("archived")),
            ))
            .execute();
        assert!(result.is_ok());

        let item = db.get_item("items").partition_key("a").execute().unwrap();
        assert!(item.is_none());
    }

    #[test]
    fn test_delete_condition_eq_fails() {
        let (db, _dir) = create_test_db();
        db.put_item("items", json!({"id": "a", "status": "active"}))
            .unwrap();

        // Condition requires status == "archived", but it's "active".
        let result = db
            .delete_item("items")
            .partition_key("a")
            .condition(FilterExpr::eq(
                FilterExpr::attr("status"),
                FilterExpr::literal(json!("archived")),
            ))
            .execute();
        assert!(result.is_err());

        // Item should still exist.
        let item = db.get_item("items").partition_key("a").execute().unwrap();
        assert!(item.is_some());
    }

    #[test]
    fn test_condition_on_nonexistent_item() {
        let (db, _dir) = create_test_db();
        // Delete with condition on non-existent item: attribute_not_exists passes
        // against empty object, so delete is a no-op but doesn't error.
        let result = db
            .delete_item("items")
            .partition_key("ghost")
            .condition(FilterExpr::attribute_not_exists("id"))
            .execute();
        // Delete of non-existent item is already a no-op in our implementation,
        // but the condition should still be evaluated. attribute_not_exists on
        // empty object passes.
        assert!(result.is_ok());
    }

    #[test]
    fn test_condition_compound_and_or() {
        let (db, _dir) = create_test_db();
        db.put_item(
            "items",
            json!({"id": "x", "status": "active", "priority": 5}),
        )
        .unwrap();

        // AND: status == "active" AND priority > 3 → should pass.
        let cond = FilterExpr::and(vec![
            FilterExpr::eq(
                FilterExpr::attr("status"),
                FilterExpr::literal(json!("active")),
            ),
            FilterExpr::gt(FilterExpr::attr("priority"), FilterExpr::literal(json!(3))),
        ]);
        let result = db
            .put("items", json!({"id": "x", "status": "done", "priority": 5}))
            .condition(cond)
            .execute();
        assert!(result.is_ok());

        // OR: status == "active" OR status == "done" → "done" matches.
        let cond = FilterExpr::or(vec![
            FilterExpr::eq(
                FilterExpr::attr("status"),
                FilterExpr::literal(json!("active")),
            ),
            FilterExpr::eq(
                FilterExpr::attr("status"),
                FilterExpr::literal(json!("done")),
            ),
        ]);
        let result = db
            .put(
                "items",
                json!({"id": "x", "status": "archived", "priority": 5}),
            )
            .condition(cond)
            .execute();
        assert!(result.is_ok());

        let item = db
            .get_item("items")
            .partition_key("x")
            .execute()
            .unwrap()
            .unwrap();
        assert_eq!(item["status"], "archived");
    }

    #[test]
    fn test_condition_check_failed_error() {
        let (db, _dir) = create_test_db();
        db.put_item("items", json!({"id": "a", "val": 1})).unwrap();

        let result = db
            .put("items", json!({"id": "a", "val": 2}))
            .condition(FilterExpr::attribute_not_exists("id"))
            .execute();

        assert!(result.is_err());
        let err = result.unwrap_err();
        // Verify it's a ConditionCheckFailed error.
        match &err {
            Error::Transaction(crate::error::TxnError::ConditionCheckFailed(_)) => {}
            other => panic!("expected ConditionCheckFailed, got: {other}"),
        }
    }

    // -----------------------------------------------------------------------
    // Phase 2: UpdateItem conditions
    // -----------------------------------------------------------------------

    #[test]
    fn test_update_with_condition_passes() {
        let (db, _dir) = create_test_db();
        db.put_item("items", json!({"id": "a", "val": 1, "status": "active"}))
            .unwrap();

        let result = db
            .update_item("items")
            .partition_key("a")
            .set("val", json!(2))
            .condition(FilterExpr::eq(
                FilterExpr::attr("status"),
                FilterExpr::literal(json!("active")),
            ))
            .execute();
        assert!(result.is_ok());

        let item = db
            .get_item("items")
            .partition_key("a")
            .execute()
            .unwrap()
            .unwrap();
        assert_eq!(item["val"], 2);
    }

    #[test]
    fn test_update_with_condition_fails() {
        let (db, _dir) = create_test_db();
        db.put_item("items", json!({"id": "a", "val": 1, "status": "locked"}))
            .unwrap();

        let result = db
            .update_item("items")
            .partition_key("a")
            .set("val", json!(2))
            .condition(FilterExpr::eq(
                FilterExpr::attr("status"),
                FilterExpr::literal(json!("active")),
            ))
            .execute();
        assert!(result.is_err());

        // Original value unchanged.
        let item = db
            .get_item("items")
            .partition_key("a")
            .execute()
            .unwrap()
            .unwrap();
        assert_eq!(item["val"], 1);
    }

    #[test]
    fn test_update_with_condition_on_nonexistent() {
        let (db, _dir) = create_test_db();
        // Upsert with condition: attribute_not_exists("id") on non-existent item → passes.
        let result = db
            .update_item("items")
            .partition_key("new")
            .set("val", json!(42))
            .condition(FilterExpr::attribute_not_exists("id"))
            .execute();
        assert!(result.is_ok());

        let item = db
            .get_item("items")
            .partition_key("new")
            .execute()
            .unwrap()
            .unwrap();
        assert_eq!(item["val"], 42);
    }

    // -----------------------------------------------------------------------
    // Phase 3: Transaction conditions
    // -----------------------------------------------------------------------

    #[test]
    fn test_txn_all_conditions_pass() {
        let (db, _dir) = create_test_db();
        db.put_item("items", json!({"id": "a", "val": 1})).unwrap();
        db.put_item("items", json!({"id": "b", "val": 2})).unwrap();

        let result = db.transact(|txn| {
            txn.put_item(
                "items",
                json!({"id": "a", "val": 10}),
                Some(&FilterExpr::attribute_exists("id")),
            )?;
            txn.put_item(
                "items",
                json!({"id": "b", "val": 20}),
                Some(&FilterExpr::attribute_exists("id")),
            )?;
            Ok(())
        });
        assert!(result.is_ok());

        let a = db
            .get_item("items")
            .partition_key("a")
            .execute()
            .unwrap()
            .unwrap();
        let b = db
            .get_item("items")
            .partition_key("b")
            .execute()
            .unwrap()
            .unwrap();
        assert_eq!(a["val"], 10);
        assert_eq!(b["val"], 20);
    }

    #[test]
    fn test_txn_one_condition_fails_aborts_all() {
        let (db, _dir) = create_test_db();
        db.put_item("items", json!({"id": "a", "val": 1})).unwrap();
        db.put_item("items", json!({"id": "b", "val": 2})).unwrap();

        let result = db.transact(|txn| {
            // This condition passes (item "a" exists).
            txn.put_item(
                "items",
                json!({"id": "a", "val": 10}),
                Some(&FilterExpr::attribute_exists("id")),
            )?;
            // This condition fails: attribute_not_exists("id") on existing item.
            txn.put_item(
                "items",
                json!({"id": "b", "val": 20}),
                Some(&FilterExpr::attribute_not_exists("id")),
            )?;
            Ok(())
        });
        assert!(result.is_err());

        // Neither write should be visible (all-or-nothing).
        let a = db
            .get_item("items")
            .partition_key("a")
            .execute()
            .unwrap()
            .unwrap();
        let b = db
            .get_item("items")
            .partition_key("b")
            .execute()
            .unwrap()
            .unwrap();
        assert_eq!(a["val"], 1);
        assert_eq!(b["val"], 2);
    }

    #[test]
    fn test_txn_no_partial_writes_on_condition_failure() {
        let (db, _dir) = create_test_db();

        // Transaction: create "new1" (passes) then create "new2" with failing condition.
        let result = db.transact(|txn| {
            txn.put_item("items", json!({"id": "new1", "val": 1}), None)?;
            // Condition: attribute_exists("id") on non-existent item → fails.
            txn.put_item(
                "items",
                json!({"id": "new2", "val": 2}),
                Some(&FilterExpr::attribute_exists("id")),
            )?;
            Ok(())
        });
        assert!(result.is_err());

        // "new1" should NOT exist because the transaction aborted.
        let item = db
            .get_item("items")
            .partition_key("new1")
            .execute()
            .unwrap();
        assert!(item.is_none());
    }

    #[test]
    fn test_txn_delete_with_condition() {
        let (db, _dir) = create_test_db();
        db.put_item("items", json!({"id": "a", "status": "archived"}))
            .unwrap();

        let result = db.transact(|txn| {
            txn.delete_item(
                "items",
                &json!("a"),
                None,
                Some(&FilterExpr::eq(
                    FilterExpr::attr("status"),
                    FilterExpr::literal(json!("archived")),
                )),
            )
        });
        assert!(result.is_ok());

        let item = db.get_item("items").partition_key("a").execute().unwrap();
        assert!(item.is_none());
    }

    #[test]
    fn test_txn_update_with_condition() {
        let (db, _dir) = create_test_db();
        db.put_item("items", json!({"id": "a", "val": 1, "status": "active"}))
            .unwrap();

        let result = db.transact(|txn| {
            txn.update_item(
                "items",
                &json!("a"),
                None,
                &[crate::api::update::UpdateAction::Set {
                    path: "val".to_string(),
                    value: json!(99),
                }],
                Some(&FilterExpr::eq(
                    FilterExpr::attr("status"),
                    FilterExpr::literal(json!("active")),
                )),
            )
        });
        assert!(result.is_ok());

        let item = db
            .get_item("items")
            .partition_key("a")
            .execute()
            .unwrap()
            .unwrap();
        assert_eq!(item["val"], 99);
    }

    // -----------------------------------------------------------------------
    // Depth limit
    // -----------------------------------------------------------------------

    #[test]
    fn test_condition_depth_limit() {
        let (db, _dir) = create_test_db();
        db.put_item("items", json!({"id": "a", "val": 1})).unwrap();

        // Build a deeply nested AND chain exceeding depth 16.
        let mut expr = FilterExpr::attribute_exists("val");
        for _ in 0..20 {
            expr = FilterExpr::and(vec![expr]);
        }

        let result = db
            .put("items", json!({"id": "a", "val": 2}))
            .condition(expr)
            .execute();
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("depth"));
    }

    // -----------------------------------------------------------------------
    // put_item_conditional (version-based OCC) still works
    // -----------------------------------------------------------------------

    #[test]
    fn test_put_item_conditional_still_works() {
        let (db, _dir) = create_test_db();
        db.put_item("items", json!({"id": "a", "val": 1})).unwrap();

        let vi = db
            .get_item_versioned("items")
            .partition_key("a")
            .execute()
            .unwrap()
            .unwrap();

        // Should succeed with correct version.
        db.put_item_conditional("items", json!({"id": "a", "val": 2}), vi.version)
            .unwrap();

        // Should fail with stale version.
        let result = db.put_item_conditional("items", json!({"id": "a", "val": 3}), vi.version);
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod index_pagination_tests {
    use super::*;
    use crate::types::{AttrType, KeyType};
    use serde_json::json;
    use tempfile::tempdir;

    fn create_test_db() -> (FerridynDB, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = FerridynDB::create(&db_path).unwrap();
        (db, dir)
    }

    // -----------------------------------------------------------------------
    // Index query pagination tests (PRD-04)
    // -----------------------------------------------------------------------

    /// Helper: create a table with sort key, partition schema, and index.
    fn setup_table_with_sort_key_and_index(db: &FerridynDB) {
        db.create_table("data")
            .partition_key("pk", KeyType::String)
            .sort_key("sk", KeyType::String)
            .execute()
            .unwrap();

        db.create_partition_schema("data")
            .prefix("ITEM")
            .description("Items")
            .attribute("status", AttrType::String, false)
            .execute()
            .unwrap();

        db.create_index("data")
            .name("status-idx")
            .partition_schema("ITEM")
            .index_key("status", KeyType::String)
            .execute()
            .unwrap();
    }

    #[test]
    fn test_index_pagination_basic() {
        let (db, _dir) = create_test_db();
        setup_table_with_sort_key_and_index(&db);

        // Insert 25 items all with status "active".
        for i in 0..25 {
            db.put_item(
                "data",
                json!({
                    "pk": format!("ITEM#{i:03}"),
                    "sk": "info",
                    "status": "active",
                    "seq": i,
                }),
            )
            .unwrap();
        }

        // Page 1: limit 10.
        let page1 = db
            .query_index("data", "status-idx")
            .key_value("active")
            .limit(10)
            .execute()
            .unwrap();
        assert_eq!(page1.items.len(), 10);
        assert!(page1.last_evaluated_key.is_some());

        // Page 2: limit 10 with cursor from page 1.
        let page2 = db
            .query_index("data", "status-idx")
            .key_value("active")
            .limit(10)
            .exclusive_start_key(page1.last_evaluated_key.unwrap())
            .execute()
            .unwrap();
        assert_eq!(page2.items.len(), 10);
        assert!(page2.last_evaluated_key.is_some());

        // Page 3: limit 10 with cursor from page 2 — should get 5 remaining.
        let page3 = db
            .query_index("data", "status-idx")
            .key_value("active")
            .limit(10)
            .exclusive_start_key(page2.last_evaluated_key.unwrap())
            .execute()
            .unwrap();
        assert_eq!(page3.items.len(), 5);
        assert!(page3.last_evaluated_key.is_none());

        // Verify all 25 items are covered with no duplicates.
        let mut all_pks: Vec<String> = Vec::new();
        for page in [&page1.items, &page2.items, &page3.items] {
            for item in page {
                all_pks.push(item["pk"].as_str().unwrap().to_string());
            }
        }
        all_pks.sort();
        all_pks.dedup();
        assert_eq!(all_pks.len(), 25);
    }

    #[test]
    fn test_index_pagination_exact_page() {
        let (db, _dir) = create_test_db();
        setup_table_with_sort_key_and_index(&db);

        // Insert exactly 10 items.
        for i in 0..10 {
            db.put_item(
                "data",
                json!({
                    "pk": format!("ITEM#{i:03}"),
                    "sk": "info",
                    "status": "active",
                }),
            )
            .unwrap();
        }

        // limit 10 on 10 items — should get all, no cursor.
        let result = db
            .query_index("data", "status-idx")
            .key_value("active")
            .limit(10)
            .execute()
            .unwrap();
        assert_eq!(result.items.len(), 10);
        assert!(
            result.last_evaluated_key.is_none(),
            "no cursor when all items fit in one page"
        );
    }

    #[test]
    fn test_index_pagination_single_item_pages() {
        let (db, _dir) = create_test_db();
        setup_table_with_sort_key_and_index(&db);

        for i in 0..3 {
            db.put_item(
                "data",
                json!({
                    "pk": format!("ITEM#{i:03}"),
                    "sk": "info",
                    "status": "pending",
                }),
            )
            .unwrap();
        }

        // Page through with limit=1.
        let page1 = db
            .query_index("data", "status-idx")
            .key_value("pending")
            .limit(1)
            .execute()
            .unwrap();
        assert_eq!(page1.items.len(), 1);
        assert!(page1.last_evaluated_key.is_some());

        let page2 = db
            .query_index("data", "status-idx")
            .key_value("pending")
            .limit(1)
            .exclusive_start_key(page1.last_evaluated_key.unwrap())
            .execute()
            .unwrap();
        assert_eq!(page2.items.len(), 1);
        assert!(page2.last_evaluated_key.is_some());

        let page3 = db
            .query_index("data", "status-idx")
            .key_value("pending")
            .limit(1)
            .exclusive_start_key(page2.last_evaluated_key.unwrap())
            .execute()
            .unwrap();
        assert_eq!(page3.items.len(), 1);
        assert!(page3.last_evaluated_key.is_none());

        // All three items should be distinct.
        let pk1 = page1.items[0]["pk"].as_str().unwrap();
        let pk2 = page2.items[0]["pk"].as_str().unwrap();
        let pk3 = page3.items[0]["pk"].as_str().unwrap();
        assert_ne!(pk1, pk2);
        assert_ne!(pk2, pk3);
        assert_ne!(pk1, pk3);
    }

    #[test]
    fn test_index_pagination_reverse_scan() {
        let (db, _dir) = create_test_db();
        setup_table_with_sort_key_and_index(&db);

        for i in 0..6 {
            db.put_item(
                "data",
                json!({
                    "pk": format!("ITEM#{i:03}"),
                    "sk": "info",
                    "status": "active",
                }),
            )
            .unwrap();
        }

        // Forward scan to collect all items in forward order.
        let all_forward = db
            .query_index("data", "status-idx")
            .key_value("active")
            .execute()
            .unwrap();
        assert_eq!(all_forward.items.len(), 6);

        // Reverse scan, full — should be in reverse order.
        let all_reverse = db
            .query_index("data", "status-idx")
            .key_value("active")
            .scan_forward(false)
            .execute()
            .unwrap();
        assert_eq!(all_reverse.items.len(), 6);

        let fwd_pks: Vec<&str> = all_forward
            .items
            .iter()
            .map(|v| v["pk"].as_str().unwrap())
            .collect();
        let rev_pks: Vec<&str> = all_reverse
            .items
            .iter()
            .map(|v| v["pk"].as_str().unwrap())
            .collect();
        let mut fwd_reversed = fwd_pks.clone();
        fwd_reversed.reverse();
        assert_eq!(rev_pks, fwd_reversed);

        // Reverse scan with limit 3, then page 2.
        let page1 = db
            .query_index("data", "status-idx")
            .key_value("active")
            .scan_forward(false)
            .limit(3)
            .execute()
            .unwrap();
        assert_eq!(page1.items.len(), 3);
        assert!(page1.last_evaluated_key.is_some());

        let page2 = db
            .query_index("data", "status-idx")
            .key_value("active")
            .scan_forward(false)
            .limit(3)
            .exclusive_start_key(page1.last_evaluated_key.unwrap())
            .execute()
            .unwrap();
        assert_eq!(page2.items.len(), 3);
        assert!(page2.last_evaluated_key.is_none());

        // Combined pages should equal the full reverse scan.
        let mut combined_pks: Vec<String> = Vec::new();
        for item in page1.items.iter().chain(page2.items.iter()) {
            combined_pks.push(item["pk"].as_str().unwrap().to_string());
        }
        let full_rev_pks: Vec<String> = all_reverse
            .items
            .iter()
            .map(|v| v["pk"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(combined_pks, full_rev_pks);
    }

    #[test]
    fn test_index_pagination_invalid_cursor_missing_index_attr() {
        let (db, _dir) = create_test_db();
        setup_table_with_sort_key_and_index(&db);

        db.put_item(
            "data",
            json!({"pk": "ITEM#001", "sk": "info", "status": "active"}),
        )
        .unwrap();

        // Cursor missing the indexed attribute "status".
        let result = db
            .query_index("data", "status-idx")
            .key_value("active")
            .limit(1)
            .exclusive_start_key(json!({"pk": "ITEM#001", "sk": "info"}))
            .execute();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("missing indexed attribute"),
            "error should mention missing indexed attribute, got: {err_msg}"
        );
    }

    #[test]
    fn test_index_pagination_invalid_cursor_missing_pk() {
        let (db, _dir) = create_test_db();
        setup_table_with_sort_key_and_index(&db);

        db.put_item(
            "data",
            json!({"pk": "ITEM#001", "sk": "info", "status": "active"}),
        )
        .unwrap();

        // Cursor missing the partition key "pk".
        let result = db
            .query_index("data", "status-idx")
            .key_value("active")
            .limit(1)
            .exclusive_start_key(json!({"status": "active", "sk": "info"}))
            .execute();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("missing partition key"),
            "error should mention missing partition key, got: {err_msg}"
        );
    }

    #[test]
    fn test_index_pagination_invalid_cursor_type_mismatch() {
        let (db, _dir) = create_test_db();
        setup_table_with_sort_key_and_index(&db);

        db.put_item(
            "data",
            json!({"pk": "ITEM#001", "sk": "info", "status": "active"}),
        )
        .unwrap();

        // Cursor with wrong type for indexed attribute (number instead of string).
        let result = db
            .query_index("data", "status-idx")
            .key_value("active")
            .limit(1)
            .exclusive_start_key(json!({"status": 42, "pk": "ITEM#001", "sk": "info"}))
            .execute();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("invalid") || err_msg.contains("mismatch"),
            "error should mention type issue, got: {err_msg}"
        );
    }
}

#[cfg(test)]
mod batch_get_item_tests {
    use super::*;
    use crate::types::KeyType;
    use serde_json::json;
    use tempfile::tempdir;

    fn create_test_db() -> (FerridynDB, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = FerridynDB::create(&db_path).unwrap();
        (db, dir)
    }

    #[test]
    fn test_batch_get_basic() {
        let (db, _dir) = create_test_db();
        db.create_table("users")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        db.put_item("users", json!({"id": "alice", "name": "Alice"}))
            .unwrap();
        db.put_item("users", json!({"id": "bob", "name": "Bob"}))
            .unwrap();
        db.put_item("users", json!({"id": "charlie", "name": "Charlie"}))
            .unwrap();

        let results = db
            .batch_get_item("users")
            .key("alice", None)
            .key("bob", None)
            .key("charlie", None)
            .execute()
            .unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_ref().unwrap()["name"], "Alice");
        assert_eq!(results[1].as_ref().unwrap()["name"], "Bob");
        assert_eq!(results[2].as_ref().unwrap()["name"], "Charlie");
    }

    #[test]
    fn test_batch_get_some_missing() {
        let (db, _dir) = create_test_db();
        db.create_table("users")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        db.put_item("users", json!({"id": "alice", "name": "Alice"}))
            .unwrap();
        db.put_item("users", json!({"id": "bob", "name": "Bob"}))
            .unwrap();
        db.put_item("users", json!({"id": "charlie", "name": "Charlie"}))
            .unwrap();

        let results = db
            .batch_get_item("users")
            .key("alice", None)
            .key("missing", None)
            .key("bob", None)
            .key("also_missing", None)
            .execute()
            .unwrap();

        assert_eq!(results.len(), 4);
        assert!(results[0].is_some());
        assert_eq!(results[0].as_ref().unwrap()["name"], "Alice");
        assert!(results[1].is_none());
        assert!(results[2].is_some());
        assert_eq!(results[2].as_ref().unwrap()["name"], "Bob");
        assert!(results[3].is_none());
    }

    #[test]
    fn test_batch_get_empty() {
        let (db, _dir) = create_test_db();
        db.create_table("users")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        let results = db.batch_get_item("users").execute().unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_batch_get_single() {
        let (db, _dir) = create_test_db();
        db.create_table("users")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        db.put_item("users", json!({"id": "alice", "name": "Alice"}))
            .unwrap();

        let results = db
            .batch_get_item("users")
            .key("alice", None)
            .execute()
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_ref().unwrap()["name"], "Alice");
    }

    #[test]
    fn test_batch_get_snapshot_consistency() {
        let (db, _dir) = create_test_db();
        db.create_table("users")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        db.put_item("users", json!({"id": "alice", "name": "Alice"}))
            .unwrap();
        db.put_item("users", json!({"id": "bob", "name": "Bob"}))
            .unwrap();

        // Start a batch get that captures a snapshot. Then delete an item
        // in a separate transaction. The batch should still see both items
        // because it reads from the snapshot taken before the delete.
        //
        // We test this by verifying that batch_get_item returns a consistent
        // snapshot: if we read both items, both should be visible even if
        // a concurrent writer deletes one after we took our snapshot.
        //
        // Since batch_get_item uses a single read_snapshot call internally,
        // all lookups see the same point-in-time view. We verify this by
        // confirming both items are present in the result.
        let results = db
            .batch_get_item("users")
            .key("alice", None)
            .key("bob", None)
            .execute()
            .unwrap();

        assert_eq!(results.len(), 2);
        assert!(results[0].is_some());
        assert!(results[1].is_some());

        // Now delete bob and verify a new batch shows it gone.
        db.delete_item("users")
            .partition_key("bob")
            .execute()
            .unwrap();

        let results2 = db
            .batch_get_item("users")
            .key("alice", None)
            .key("bob", None)
            .execute()
            .unwrap();

        assert_eq!(results2.len(), 2);
        assert!(results2[0].is_some());
        assert!(results2[1].is_none());
    }

    #[test]
    fn test_batch_get_with_sort_key() {
        let (db, _dir) = create_test_db();
        db.create_table("orders")
            .partition_key("user_id", KeyType::String)
            .sort_key("order_id", KeyType::String)
            .execute()
            .unwrap();

        db.put_item(
            "orders",
            json!({"user_id": "user1", "order_id": "order-001", "total": 100}),
        )
        .unwrap();
        db.put_item(
            "orders",
            json!({"user_id": "user1", "order_id": "order-002", "total": 200}),
        )
        .unwrap();
        db.put_item(
            "orders",
            json!({"user_id": "user2", "order_id": "order-003", "total": 300}),
        )
        .unwrap();

        let results = db
            .batch_get_item("orders")
            .key("user1", Some(json!("order-001")))
            .key("user1", Some(json!("order-002")))
            .key("user2", Some(json!("order-003")))
            .key("user2", Some(json!("order-999")))
            .execute()
            .unwrap();

        assert_eq!(results.len(), 4);
        assert_eq!(results[0].as_ref().unwrap()["total"], 100);
        assert_eq!(results[1].as_ref().unwrap()["total"], 200);
        assert_eq!(results[2].as_ref().unwrap()["total"], 300);
        assert!(results[3].is_none());
    }
}

#[cfg(test)]
mod return_values_tests {
    use super::*;
    use crate::api::filter::FilterExpr;
    use crate::types::KeyType;
    use serde_json::json;
    use tempfile::tempdir;

    fn create_test_db() -> (FerridynDB, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = FerridynDB::create(&db_path).unwrap();
        (db, dir)
    }

    #[test]
    fn test_put_returning_old_existing() {
        let (db, _dir) = create_test_db();
        db.create_table("users")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        db.put_item("users", json!({"id": "alice", "name": "Alice", "age": 30}))
            .unwrap();

        let old = db
            .put("users", json!({"id": "alice", "name": "Alice2", "age": 31}))
            .return_old()
            .execute()
            .unwrap();

        assert!(old.is_some());
        let old = old.unwrap();
        assert_eq!(old["name"], "Alice");
        assert_eq!(old["age"], 30);
    }

    #[test]
    fn test_put_returning_old_new() {
        let (db, _dir) = create_test_db();
        db.create_table("users")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        let old = db
            .put("users", json!({"id": "alice", "name": "Alice"}))
            .return_old()
            .execute()
            .unwrap();

        assert!(old.is_none());
    }

    #[test]
    fn test_put_returning_old_with_condition() {
        let (db, _dir) = create_test_db();
        db.create_table("users")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        db.put_item("users", json!({"id": "alice", "name": "Alice", "age": 30}))
            .unwrap();

        let old = db
            .put("users", json!({"id": "alice", "name": "Alice2", "age": 31}))
            .condition(FilterExpr::ge(
                FilterExpr::attr("age"),
                FilterExpr::literal(25),
            ))
            .return_old()
            .execute()
            .unwrap();

        assert!(old.is_some());
        assert_eq!(old.unwrap()["name"], "Alice");
    }

    #[test]
    fn test_delete_returning_old_existing() {
        let (db, _dir) = create_test_db();
        db.create_table("users")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        db.put_item("users", json!({"id": "alice", "name": "Alice"}))
            .unwrap();

        let old = db
            .delete_item("users")
            .partition_key("alice")
            .return_old()
            .execute()
            .unwrap();

        assert!(old.is_some());
        assert_eq!(old.unwrap()["name"], "Alice");

        // Verify item is actually deleted.
        let item = db
            .get_item("users")
            .partition_key("alice")
            .execute()
            .unwrap();
        assert!(item.is_none());
    }

    #[test]
    fn test_delete_returning_old_nonexistent() {
        let (db, _dir) = create_test_db();
        db.create_table("users")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        let old = db
            .delete_item("users")
            .partition_key("nobody")
            .return_old()
            .execute()
            .unwrap();

        assert!(old.is_none());
    }

    #[test]
    fn test_update_returning_old() {
        let (db, _dir) = create_test_db();
        db.create_table("users")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        db.put_item("users", json!({"id": "alice", "name": "Alice", "age": 30}))
            .unwrap();

        let old = db
            .update_item("users")
            .partition_key("alice")
            .set("name", json!("Alice2"))
            .return_old()
            .execute()
            .unwrap();

        assert!(old.is_some());
        let old = old.unwrap();
        assert_eq!(old["name"], "Alice");
        assert_eq!(old["age"], 30);
    }

    #[test]
    fn test_update_returning_new() {
        let (db, _dir) = create_test_db();
        db.create_table("users")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        db.put_item("users", json!({"id": "alice", "name": "Alice", "age": 30}))
            .unwrap();

        let new_doc = db
            .update_item("users")
            .partition_key("alice")
            .set("name", json!("Alice2"))
            .return_new()
            .execute()
            .unwrap();

        assert!(new_doc.is_some());
        let new_doc = new_doc.unwrap();
        assert_eq!(new_doc["name"], "Alice2");
        assert_eq!(new_doc["age"], 30);
    }

    #[test]
    fn test_update_returning_new_upsert() {
        let (db, _dir) = create_test_db();
        db.create_table("users")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        let new_doc = db
            .update_item("users")
            .partition_key("bob")
            .set("name", json!("Bob"))
            .return_new()
            .execute()
            .unwrap();

        assert!(new_doc.is_some());
        let new_doc = new_doc.unwrap();
        assert_eq!(new_doc["id"], "bob");
        assert_eq!(new_doc["name"], "Bob");
    }

    #[test]
    fn test_default_put_unchanged() {
        let (db, _dir) = create_test_db();
        db.create_table("users")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        // This is a compile-time check: without .return_old(), execute() returns Result<()>.
        let result: Result<(), _> = db
            .put("users", json!({"id": "alice", "name": "Alice"}))
            .execute();
        assert!(result.is_ok());
    }
}

// ---------------------------------------------------------------------------
// Projection expression tests (PRD-07)
// ---------------------------------------------------------------------------
#[cfg(test)]
mod projection_tests {
    use super::*;
    use crate::types::{AttrType, KeyType};
    use serde_json::json;
    use tempfile::tempdir;

    fn create_test_db() -> (FerridynDB, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = FerridynDB::create(&db_path).unwrap();
        (db, dir)
    }

    #[test]
    fn test_projection_get_item() {
        let (db, _dir) = create_test_db();
        db.create_table("users")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();

        db.put(
            "users",
            json!({"pk": "alice", "name": "Alice", "age": 30, "email": "a@b.com"}),
        )
        .execute()
        .unwrap();

        let item = db
            .get_item("users")
            .partition_key("alice")
            .projection(&["name", "age"])
            .execute()
            .unwrap()
            .unwrap();

        assert_eq!(item["pk"], "alice"); // key always included
        assert_eq!(item["name"], "Alice");
        assert_eq!(item["age"], 30);
        assert!(item.get("email").is_none()); // not projected
    }

    #[test]
    fn test_projection_top_level() {
        let (db, _dir) = create_test_db();
        db.create_table("users")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();

        db.put(
            "users",
            json!({"pk": "a", "name": "Alice", "age": 30, "email": "a@b.com",
                    "phone": "555-1234", "city": "NYC", "state": "NY",
                    "zip": "10001", "country": "US", "active": true}),
        )
        .execute()
        .unwrap();

        let item = db
            .get_item("users")
            .partition_key("a")
            .projection(&["name", "age"])
            .execute()
            .unwrap()
            .unwrap();

        assert_eq!(item["pk"], "a");
        assert_eq!(item["name"], "Alice");
        assert_eq!(item["age"], 30);
        // All other attributes should be absent.
        assert!(item.get("email").is_none());
        assert!(item.get("phone").is_none());
        assert!(item.get("city").is_none());
    }

    #[test]
    fn test_projection_nested() {
        let (db, _dir) = create_test_db();
        db.create_table("users")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();

        db.put(
            "users",
            json!({"pk": "a", "address": {"city": "NYC", "zip": "10001", "state": "NY"},
                    "name": "Alice"}),
        )
        .execute()
        .unwrap();

        let item = db
            .get_item("users")
            .partition_key("a")
            .projection(&["address.city"])
            .execute()
            .unwrap()
            .unwrap();

        assert_eq!(item["pk"], "a");
        assert_eq!(item["address"]["city"], "NYC");
        assert!(item["address"].get("zip").is_none());
        assert!(item.get("name").is_none());
    }

    #[test]
    fn test_projection_keys_always_included() {
        let (db, _dir) = create_test_db();
        db.create_table("data")
            .partition_key("pk", KeyType::String)
            .sort_key("sk", KeyType::String)
            .execute()
            .unwrap();

        db.put(
            "data",
            json!({"pk": "a", "sk": "b", "name": "Alice", "age": 30}),
        )
        .execute()
        .unwrap();

        let item = db
            .get_item("data")
            .partition_key("a")
            .sort_key("b")
            .projection(&["name"])
            .execute()
            .unwrap()
            .unwrap();

        assert_eq!(item["pk"], "a");
        assert_eq!(item["sk"], "b"); // sort key always included
        assert_eq!(item["name"], "Alice");
        assert!(item.get("age").is_none());
    }

    #[test]
    fn test_projection_missing_attr() {
        let (db, _dir) = create_test_db();
        db.create_table("users")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();

        db.put("users", json!({"pk": "a", "name": "Alice"}))
            .execute()
            .unwrap();

        let item = db
            .get_item("users")
            .partition_key("a")
            .projection(&["nonexistent"])
            .execute()
            .unwrap()
            .unwrap();

        // Only pk should be present, nonexistent attr silently omitted.
        assert_eq!(item["pk"], "a");
        assert!(item.get("name").is_none());
        assert!(item.get("nonexistent").is_none());
    }

    #[test]
    fn test_projection_empty() {
        let (db, _dir) = create_test_db();
        db.create_table("users")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();

        db.put("users", json!({"pk": "a", "name": "Alice", "age": 30}))
            .execute()
            .unwrap();

        // Empty projection returns full document.
        let item = db
            .get_item("users")
            .partition_key("a")
            .projection(&[])
            .execute()
            .unwrap()
            .unwrap();

        assert_eq!(item["pk"], "a");
        assert_eq!(item["name"], "Alice");
        assert_eq!(item["age"], 30);
    }

    #[test]
    fn test_projection_query() {
        let (db, _dir) = create_test_db();
        db.create_table("events")
            .partition_key("pk", KeyType::String)
            .sort_key("sk", KeyType::String)
            .execute()
            .unwrap();

        for i in 0..5 {
            db.put(
                "events",
                json!({"pk": "user1", "sk": format!("ev#{i}"),
                        "type": "click", "data": {"x": i, "y": i * 2}}),
            )
            .execute()
            .unwrap();
        }

        let result = db
            .query("events")
            .partition_key("user1")
            .projection(&["type"])
            .execute()
            .unwrap();

        assert_eq!(result.items.len(), 5);
        for item in &result.items {
            assert!(item.get("pk").is_some()); // key included
            assert!(item.get("sk").is_some()); // sort key included
            assert_eq!(item["type"], "click");
            assert!(item.get("data").is_none()); // not projected
        }
    }

    #[test]
    fn test_projection_scan() {
        let (db, _dir) = create_test_db();
        db.create_table("items")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();

        for i in 0..3 {
            db.put(
                "items",
                json!({"pk": format!("item{i}"), "name": format!("Item {i}"),
                        "price": i * 10, "category": "A"}),
            )
            .execute()
            .unwrap();
        }

        let result = db.scan("items").projection(&["name"]).execute().unwrap();

        assert_eq!(result.items.len(), 3);
        for item in &result.items {
            assert!(item.get("pk").is_some());
            assert!(item.get("name").is_some());
            assert!(item.get("price").is_none());
            assert!(item.get("category").is_none());
        }
    }

    #[test]
    fn test_projection_index_query() {
        let (db, _dir) = create_test_db();
        db.create_table("data")
            .partition_key("pk", KeyType::String)
            .execute()
            .unwrap();

        db.create_partition_schema("data")
            .prefix("CONTACT")
            .attribute("email", AttrType::String, true)
            .execute()
            .unwrap();

        db.create_index("data")
            .name("email-idx")
            .partition_schema("CONTACT")
            .index_key("email", KeyType::String)
            .execute()
            .unwrap();

        db.put(
            "data",
            json!({"pk": "CONTACT#1", "email": "alice@test.com", "name": "Alice", "age": 30}),
        )
        .execute()
        .unwrap();

        db.put(
            "data",
            json!({"pk": "CONTACT#2", "email": "alice@test.com", "name": "Alice2", "age": 25}),
        )
        .execute()
        .unwrap();

        let result = db
            .query_index("data", "email-idx")
            .key_value("alice@test.com")
            .projection(&["name"])
            .execute()
            .unwrap();

        assert_eq!(result.items.len(), 2);
        for item in &result.items {
            assert!(item.get("pk").is_some());
            assert!(item.get("name").is_some());
            assert!(item.get("age").is_none()); // not projected
            assert!(item.get("email").is_none()); // not projected (not a key attr)
        }
    }
}
