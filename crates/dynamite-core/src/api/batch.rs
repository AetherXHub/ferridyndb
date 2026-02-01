use serde_json::Value;

use crate::error::Error;

use super::database::DynamiteDB;

/// Controls when `fsync` is called during commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// Fsync after every commit (default). Maximum durability.
    Full = 0,
    /// Skip fsync entirely. Data may be lost on crash but writes are
    /// orders of magnitude faster. Useful for bulk loads and tests.
    None = 1,
}

impl SyncMode {
    pub(crate) fn from_u8(v: u8) -> Self {
        match v {
            0 => SyncMode::Full,
            _ => SyncMode::None,
        }
    }
}

/// A single operation within a write batch.
pub enum BatchOp {
    Put {
        table: String,
        document: Value,
    },
    Delete {
        table: String,
        partition_key: Value,
        sort_key: Option<Value>,
    },
}

/// Collects multiple write operations and commits them atomically in a
/// single transaction (one fsync instead of N).
pub struct WriteBatch<'a> {
    db: &'a DynamiteDB,
    ops: Vec<BatchOp>,
}

impl<'a> WriteBatch<'a> {
    pub(crate) fn new(db: &'a DynamiteDB) -> Self {
        Self {
            db,
            ops: Vec::new(),
        }
    }

    /// Queue a put_item operation.
    pub fn put_item(&mut self, table: &str, document: Value) -> &mut Self {
        self.ops.push(BatchOp::Put {
            table: table.to_string(),
            document,
        });
        self
    }

    /// Queue a delete_item operation.
    pub fn delete_item(
        &mut self,
        table: &str,
        partition_key: impl Into<Value>,
        sort_key: Option<Value>,
    ) -> &mut Self {
        self.ops.push(BatchOp::Delete {
            table: table.to_string(),
            partition_key: partition_key.into(),
            sort_key,
        });
        self
    }

    /// Number of queued operations.
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Whether the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Discard all queued operations without committing.
    pub fn clear(&mut self) {
        self.ops.clear();
    }

    /// Commit all queued operations as a single transaction.
    /// On success, all operations are visible atomically.
    /// On failure, none are applied (all-or-nothing).
    pub fn commit(self) -> Result<(), Error> {
        if self.ops.is_empty() {
            return Ok(());
        }
        self.db.transact(move |txn| {
            for op in self.ops {
                match op {
                    BatchOp::Put { table, document } => {
                        txn.put_item(&table, document)?;
                    }
                    BatchOp::Delete {
                        table,
                        partition_key,
                        sort_key,
                    } => {
                        txn.delete_item(&table, &partition_key, sort_key.as_ref())?;
                    }
                }
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::KeyType;
    use serde_json::json;
    use tempfile::tempdir;

    fn create_test_db() -> (DynamiteDB, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = DynamiteDB::create(&db_path).unwrap();
        db.create_table("items")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();
        (db, dir)
    }

    #[test]
    fn test_write_batch_put_and_read() {
        let (db, _dir) = create_test_db();
        let mut batch = db.write_batch();
        for i in 0..10 {
            batch.put_item("items", json!({"id": format!("k{i}"), "val": i}));
        }
        assert_eq!(batch.len(), 10);
        batch.commit().unwrap();

        for i in 0..10 {
            let key = format!("k{i}");
            let item = db
                .get_item("items")
                .partition_key(key.as_str())
                .execute()
                .unwrap();
            assert!(item.is_some(), "key {key} should exist");
            assert_eq!(item.unwrap()["val"], i);
        }
    }

    #[test]
    fn test_write_batch_mixed_ops() {
        let (db, _dir) = create_test_db();

        // Insert some items first.
        db.put_item("items", json!({"id": "a", "val": 1})).unwrap();
        db.put_item("items", json!({"id": "b", "val": 2})).unwrap();
        db.put_item("items", json!({"id": "c", "val": 3})).unwrap();

        // Batch: delete "a", put "d", delete "b".
        let mut batch = db.write_batch();
        batch.delete_item("items", json!("a"), None);
        batch.put_item("items", json!({"id": "d", "val": 4}));
        batch.delete_item("items", json!("b"), None);
        batch.commit().unwrap();

        assert!(
            db.get_item("items")
                .partition_key("a")
                .execute()
                .unwrap()
                .is_none()
        );
        assert!(
            db.get_item("items")
                .partition_key("b")
                .execute()
                .unwrap()
                .is_none()
        );
        assert!(
            db.get_item("items")
                .partition_key("c")
                .execute()
                .unwrap()
                .is_some()
        );
        assert!(
            db.get_item("items")
                .partition_key("d")
                .execute()
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_write_batch_empty_commit() {
        let (db, _dir) = create_test_db();
        let batch = db.write_batch();
        assert!(batch.is_empty());
        batch.commit().unwrap(); // no-op
    }

    #[test]
    fn test_write_batch_drop_without_commit() {
        let (db, _dir) = create_test_db();
        {
            let mut batch = db.write_batch();
            batch.put_item("items", json!({"id": "phantom", "val": 999}));
            // batch is dropped without calling commit
        }
        let item = db
            .get_item("items")
            .partition_key("phantom")
            .execute()
            .unwrap();
        assert!(
            item.is_none(),
            "uncommitted batch items should not be visible"
        );
    }

    #[test]
    fn test_write_batch_error_aborts_all() {
        let (db, _dir) = create_test_db();
        let mut batch = db.write_batch();
        batch.put_item("items", json!({"id": "good", "val": 1}));
        // Put into a non-existent table to trigger an error.
        batch.put_item("nonexistent", json!({"id": "bad"}));
        let result = batch.commit();
        assert!(result.is_err());

        // "good" should NOT be visible because the entire batch was aborted.
        let item = db
            .get_item("items")
            .partition_key("good")
            .execute()
            .unwrap();
        assert!(
            item.is_none(),
            "failed batch should not leave partial writes"
        );
    }

    #[test]
    fn test_write_batch_same_key_last_wins() {
        let (db, _dir) = create_test_db();
        let mut batch = db.write_batch();
        batch.put_item("items", json!({"id": "dup", "val": "first"}));
        batch.put_item("items", json!({"id": "dup", "val": "second"}));
        batch.commit().unwrap();

        let item = db
            .get_item("items")
            .partition_key("dup")
            .execute()
            .unwrap()
            .unwrap();
        assert_eq!(item["val"], "second");
    }

    #[test]
    fn test_sync_mode_none() {
        let (db, _dir) = create_test_db();
        db.set_sync_mode(SyncMode::None);
        assert_eq!(db.sync_mode(), SyncMode::None);

        // Writes should still work (just without fsync).
        for i in 0..10 {
            db.put_item("items", json!({"id": format!("n{i}"), "val": i}))
                .unwrap();
        }

        for i in 0..10 {
            let key = format!("n{i}");
            let item = db
                .get_item("items")
                .partition_key(key.as_str())
                .execute()
                .unwrap();
            assert!(item.is_some());
        }
    }

    #[test]
    fn test_sync_mode_default_is_full() {
        let (db, _dir) = create_test_db();
        assert_eq!(db.sync_mode(), SyncMode::Full);
    }
}
