use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;
use serde_json::Value;

use crate::btree::ops as btree_ops;
use crate::catalog;
use crate::error::{Error, StorageError};
use crate::storage::file::FileManager;
use crate::storage::header::FileHeader;
use crate::storage::lock::FileLock;
use crate::storage::pending_free::PendingFreeList;
use crate::storage::snapshot::SnapshotTracker;
use crate::types::{PAGE_SIZE, PageId};

use super::builders::{DeleteItemBuilder, GetItemBuilder, QueryBuilder, ScanBuilder, TableBuilder};
use super::page_store::{BufferedPageStore, FilePageStore};
use super::transaction::Transaction;

pub(crate) struct DatabaseState {
    pub(crate) file_manager: FileManager,
    pub(crate) header: FileHeader,
    pub(crate) header_slot: u8,
    #[allow(dead_code)]
    pub(crate) pending_free: PendingFreeList,
}

struct DatabaseInner {
    state: RwLock<DatabaseState>,
    #[allow(dead_code)]
    snapshot_tracker: SnapshotTracker,
    _file_lock: FileLock,
    #[allow(dead_code)]
    path: PathBuf,
}

/// The main database handle.
///
/// `DynaMite` is cheaply clonable (`Arc`-based) and `Send + Sync`.
#[derive(Clone)]
pub struct DynaMite {
    inner: Arc<DatabaseInner>,
}

impl DynaMite {
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
            file_manager.write_page(page_id, data)?;
        }

        // Write header to slot 0.
        let mut header = FileHeader::new();
        header.txn_counter = 1;
        header.catalog_root_page = catalog_root;
        header.total_page_count = new_total;

        let mut header_buf = [0u8; PAGE_SIZE];
        header.write_to_page(&mut header_buf);
        file_manager.write_page(0, &header_buf)?;
        file_manager.sync()?;

        Ok(Self {
            inner: Arc::new(DatabaseInner {
                state: RwLock::new(DatabaseState {
                    file_manager,
                    header,
                    header_slot: 0,
                    pending_free: PendingFreeList::new(),
                }),
                snapshot_tracker: SnapshotTracker::new(),
                _file_lock: file_lock,
                path: path.to_path_buf(),
            }),
        })
    }

    /// Open an existing database.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref();

        let lock_path = path.with_extension("lock");
        let file_lock = FileLock::exclusive(&lock_path)?;

        let (file_manager, header, slot) = FileManager::open(path)?;

        Ok(Self {
            inner: Arc::new(DatabaseInner {
                state: RwLock::new(DatabaseState {
                    file_manager,
                    header,
                    header_slot: slot,
                    pending_free: PendingFreeList::new(),
                }),
                snapshot_tracker: SnapshotTracker::new(),
                _file_lock: file_lock,
                path: path.to_path_buf(),
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
        let state = self.inner.state.read();
        let store = self.read_store(&state)?;
        catalog::ops::list_tables(&store, state.header.catalog_root_page)
    }

    /// Describe a table's schema (partition key, sort key, types).
    pub fn describe_table(&self, name: &str) -> Result<crate::types::TableSchema, Error> {
        let state = self.inner.state.read();
        let store = self.read_store(&state)?;
        let entry = catalog::ops::get_table(&store, state.header.catalog_root_page, name)?;
        Ok(entry.schema)
    }

    /// Put an item into a table.
    pub fn put_item(&self, table: &str, document: Value) -> Result<(), Error> {
        let table = table.to_string();
        self.transact(move |txn| txn.put_item(&table, document))
    }

    /// Get an item from a table by key.
    pub fn get_item(&self, table: &str) -> GetItemBuilder<'_> {
        GetItemBuilder::new(self, table.to_string())
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

    /// Execute a write transaction.
    ///
    /// The closure receives a mutable [`Transaction`] to make changes.
    /// If the closure returns `Ok`, the transaction is committed atomically.
    /// If it returns `Err`, all changes are discarded (auto-abort).
    pub fn transact<F, R>(&self, f: F) -> Result<R, Error>
    where
        F: FnOnce(&mut Transaction) -> Result<R, Error>,
    {
        let mut state = self.inner.state.write();
        let new_txn_id = state.header.txn_counter + 1;

        let file = state
            .file_manager
            .file()
            .try_clone()
            .map_err(StorageError::from)?;
        let store = BufferedPageStore::new(file, state.file_manager.total_page_count());

        let mut txn = Transaction {
            store,
            catalog_root: state.header.catalog_root_page,
            txn_id: new_txn_id,
        };

        let result = f(&mut txn);

        match result {
            Ok(val) => {
                self.commit_txn(txn, &mut state)?;
                Ok(val)
            }
            Err(e) => Err(e),
        }
    }

    /// Read-only helper: execute a closure with a read store and snapshot txn.
    pub(crate) fn read_snapshot<F, R>(&self, f: F) -> Result<R, Error>
    where
        F: FnOnce(&FilePageStore, PageId, u64) -> Result<R, Error>,
    {
        let state = self.inner.state.read();
        let store = self.read_store(&state)?;
        let snapshot_txn = state.header.txn_counter;
        let catalog_root = state.header.catalog_root_page;
        f(&store, catalog_root, snapshot_txn)
    }

    fn commit_txn(&self, txn: Transaction, state: &mut DatabaseState) -> Result<(), Error> {
        let Transaction {
            store,
            catalog_root,
            txn_id,
        } = txn;
        let new_total = store.next_page_id();

        // Grow file if needed.
        if new_total > state.file_manager.total_page_count() {
            state.file_manager.grow(new_total)?;
        }

        // Write all overlay pages to disk.
        for (&page_id, data) in store.overlay() {
            state.file_manager.write_page(page_id, data)?;
        }

        // Write new header to alternate slot.
        let new_slot = FileHeader::alternate_slot(state.header_slot);
        state.header.txn_counter = txn_id;
        state.header.catalog_root_page = catalog_root;
        state.header.total_page_count = new_total;

        let mut header_buf = [0u8; PAGE_SIZE];
        state.header.write_to_page(&mut header_buf);
        state
            .file_manager
            .write_page(new_slot as u64, &header_buf)?;

        // Sync.
        state.file_manager.sync()?;
        state.header_slot = new_slot;

        Ok(())
    }

    fn read_store(&self, state: &DatabaseState) -> Result<FilePageStore, Error> {
        let file = state
            .file_manager
            .file()
            .try_clone()
            .map_err(StorageError::from)?;
        Ok(FilePageStore::new(
            file,
            state.file_manager.total_page_count(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::KeyType;
    use serde_json::json;
    use std::thread;
    use tempfile::tempdir;

    fn create_test_db() -> (DynaMite, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = DynaMite::create(&db_path).unwrap();
        (db, dir)
    }

    #[test]
    fn test_create_and_reopen() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        {
            let db = DynaMite::create(&db_path).unwrap();
            db.create_table("users")
                .partition_key("user_id", KeyType::String)
                .execute()
                .unwrap();
            db.put_item("users", json!({"user_id": "alice", "name": "Alice"}))
                .unwrap();
        }

        {
            let db = DynaMite::open(&db_path).unwrap();
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
            txn.put_item("items", json!({"id": "a", "v": 1}))?;
            txn.put_item("items", json!({"id": "b", "v": 2}))?;
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
            txn.put_item("items", json!({"id": "a", "v": 1}))?;
            // Force an error by trying to put into a non-existent table.
            txn.put_item("nonexistent", json!({"id": "b"}))?;
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
            let db = DynaMite::create(&db_path).unwrap();
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
            let db = DynaMite::open(&db_path).unwrap();
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
            let db = DynaMite::create(&db_path).unwrap();
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
            let db = DynaMite::open(&db_path).unwrap();

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
            let db = DynaMite::create(&db_path).unwrap();
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
            let db = DynaMite::open(&db_path).unwrap();
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
        let db = DynaMite::create(&db_path).unwrap();

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
        let db = DynaMite::create(&db_path).unwrap();

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
}
