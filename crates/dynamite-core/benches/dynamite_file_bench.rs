use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use dynamite_core::api::{DynamiteDB, SyncMode};
use dynamite_core::types::KeyType;
use serde_json::json;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::{TempDir, tempdir};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create a temp directory. If `BENCH_DIR` is set, creates a subdirectory
/// there (real disk); otherwise uses `tempdir()` (likely tmpfs).
fn bench_tempdir() -> TempDir {
    match std::env::var("BENCH_DIR") {
        Ok(base) => {
            let base = PathBuf::from(base);
            std::fs::create_dir_all(&base).unwrap();
            tempfile::tempdir_in(&base).unwrap()
        }
        Err(_) => tempdir().unwrap(),
    }
}

/// Create a file-backed DB with a table ("events", pk=String, sk=Number) and
/// `n` pre-populated items: pk = "user-{i/100}", sk = i as f64.
fn setup_db_with_items(dir: &Path, n: usize) -> DynamiteDB {
    let db_path = dir.join("bench.db");
    let db = DynamiteDB::create(&db_path).unwrap();
    db.create_table("events")
        .partition_key("pk", KeyType::String)
        .sort_key("sk", KeyType::Number)
        .execute()
        .unwrap();

    for i in 0..n {
        let pk = format!("user-{}", i / 100);
        db.put_item(
            "events",
            json!({"pk": pk, "sk": i as f64, "data": format!("val_{i}")}),
        )
        .unwrap();
    }
    db
}

/// Create a document of approximately `size` bytes (when MessagePack-encoded).
///
/// The returned object always has "pk" (String) and "sk" (Number) fields.
/// A "payload" field is padded with 'x' characters to reach the target size.
fn make_sized_doc(pk: &str, sk: f64, payload_bytes: usize) -> serde_json::Value {
    // Base document without payload to measure overhead.
    let base = json!({"pk": pk, "sk": sk, "payload": ""});
    let base_len = rmp_serde::to_vec(&base).unwrap().len();
    let pad = if payload_bytes > base_len {
        payload_bytes - base_len
    } else {
        0
    };
    json!({"pk": pk, "sk": sk, "payload": "x".repeat(pad)})
}

// ---------------------------------------------------------------------------
// Group 1: Point Operations
// ---------------------------------------------------------------------------

fn bench_point_ops(c: &mut Criterion) {
    let mut group = c.benchmark_group("point_ops");
    group.measurement_time(Duration::from_secs(10));

    // --- file_put_item ---
    group.bench_function("file_put_item", |b| {
        let dir = bench_tempdir();
        let db = setup_db_with_items(dir.path(), 100);
        let mut i = 100u64;
        b.iter(|| {
            let pk = format!("user-{}", i / 100);
            db.put_item(
                "events",
                json!({"pk": pk, "sk": i as f64, "data": format!("val_{i}")}),
            )
            .unwrap();
            i += 1;
        });
    });

    // --- file_get_item ---
    group.bench_function("file_get_item", |b| {
        let dir = bench_tempdir();
        let db = setup_db_with_items(dir.path(), 1_000);
        let mut i = 0u64;
        b.iter(|| {
            let pk = format!("user-{}", (i % 10));
            let sk = (i % 1_000) as f64;
            let item = db
                .get_item("events")
                .partition_key(pk.as_str())
                .sort_key(sk)
                .execute()
                .unwrap();
            black_box(item);
            i += 1;
        });
    });

    // --- file_get_item_miss ---
    group.bench_function("file_get_item_miss", |b| {
        let dir = bench_tempdir();
        let db = setup_db_with_items(dir.path(), 100);
        b.iter(|| {
            let item = db
                .get_item("events")
                .partition_key("nonexistent")
                .sort_key(999999.0)
                .execute()
                .unwrap();
            black_box(item);
        });
    });

    // --- file_delete_item ---
    group.bench_function("file_delete_item", |b| {
        let dir = bench_tempdir();
        let db = setup_db_with_items(dir.path(), 100);
        let mut i = 0u64;
        b.iter(|| {
            let idx = (i % 100) as usize;
            let pk = format!("user-{}", idx / 100);
            let sk = idx as f64;

            // Delete the item.
            db.delete_item("events")
                .partition_key(pk.as_str())
                .sort_key(sk)
                .execute()
                .unwrap();

            // Re-insert so the item is available for the next iteration.
            db.put_item(
                "events",
                json!({"pk": pk, "sk": sk, "data": format!("val_{idx}")}),
            )
            .unwrap();

            i += 1;
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Group 2: Bulk / Batch Operations
// ---------------------------------------------------------------------------

fn bench_bulk_ops(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_ops");
    group.sample_size(20);

    // --- file_bulk_insert_1k ---
    group.throughput(Throughput::Elements(1_000));
    group.bench_function("file_bulk_insert_1k", |b| {
        b.iter(|| {
            let dir = bench_tempdir();
            let db_path = dir.path().join("bench.db");
            let db = DynamiteDB::create(&db_path).unwrap();
            db.create_table("events")
                .partition_key("pk", KeyType::String)
                .sort_key("sk", KeyType::Number)
                .execute()
                .unwrap();

            for i in 0..1_000 {
                db.put_item(
                    "events",
                    json!({"pk": format!("user-{}", i / 100), "sk": i as f64, "data": format!("v{i}")}),
                )
                .unwrap();
            }
        });
    });

    // --- file_batch_insert_1k_txn ---
    group.throughput(Throughput::Elements(1_000));
    group.bench_function("file_batch_insert_1k_txn", |b| {
        b.iter(|| {
            let dir = bench_tempdir();
            let db_path = dir.path().join("bench.db");
            let db = DynamiteDB::create(&db_path).unwrap();
            db.create_table("events")
                .partition_key("pk", KeyType::String)
                .sort_key("sk", KeyType::Number)
                .execute()
                .unwrap();

            db.transact(|txn| {
                for i in 0..1_000 {
                    txn.put_item(
                        "events",
                        json!({"pk": format!("user-{}", i / 100), "sk": i as f64, "data": format!("v{i}")}),
                    )?;
                }
                Ok(())
            })
            .unwrap();
        });
    });

    // --- file_bulk_insert_10k ---
    group.throughput(Throughput::Elements(10_000));
    group.sample_size(10);
    group.bench_function("file_bulk_insert_10k", |b| {
        b.iter(|| {
            let dir = bench_tempdir();
            let db_path = dir.path().join("bench.db");
            let db = DynamiteDB::create(&db_path).unwrap();
            db.create_table("events")
                .partition_key("pk", KeyType::String)
                .sort_key("sk", KeyType::Number)
                .execute()
                .unwrap();

            for i in 0..10_000 {
                db.put_item(
                    "events",
                    json!({"pk": format!("user-{}", i / 100), "sk": i as f64, "data": format!("v{i}")}),
                )
                .unwrap();
            }
        });
    });

    // --- file_write_batch_1k ---
    group.throughput(Throughput::Elements(1_000));
    group.sample_size(20);
    group.bench_function("file_write_batch_1k", |b| {
        b.iter(|| {
            let dir = bench_tempdir();
            let db_path = dir.path().join("bench.db");
            let db = DynamiteDB::create(&db_path).unwrap();
            db.create_table("events")
                .partition_key("pk", KeyType::String)
                .sort_key("sk", KeyType::Number)
                .execute()
                .unwrap();

            let mut batch = db.write_batch();
            for i in 0..1_000 {
                batch.put_item(
                    "events",
                    json!({"pk": format!("user-{}", i / 100), "sk": i as f64, "data": format!("v{i}")}),
                );
            }
            batch.commit().unwrap();
        });
    });

    // --- file_write_batch_1k_no_sync ---
    group.throughput(Throughput::Elements(1_000));
    group.bench_function("file_write_batch_1k_no_sync", |b| {
        b.iter(|| {
            let dir = bench_tempdir();
            let db_path = dir.path().join("bench.db");
            let db = DynamiteDB::create(&db_path).unwrap();
            db.set_sync_mode(SyncMode::None);
            db.create_table("events")
                .partition_key("pk", KeyType::String)
                .sort_key("sk", KeyType::Number)
                .execute()
                .unwrap();

            let mut batch = db.write_batch();
            for i in 0..1_000 {
                batch.put_item(
                    "events",
                    json!({"pk": format!("user-{}", i / 100), "sk": i as f64, "data": format!("v{i}")}),
                );
            }
            batch.commit().unwrap();
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Group 3: Queries and Scans
// ---------------------------------------------------------------------------

fn bench_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("queries");
    group.measurement_time(Duration::from_secs(10));

    // --- file_query_100 ---
    // Setup: 1,000 items, all pk="user1", sk=0.0..999.0.
    let dir_query = bench_tempdir();
    let db_query = {
        let db_path = dir_query.path().join("bench.db");
        let db = DynamiteDB::create(&db_path).unwrap();
        db.create_table("events")
            .partition_key("pk", KeyType::String)
            .sort_key("sk", KeyType::Number)
            .execute()
            .unwrap();
        for i in 0..1_000 {
            db.put_item(
                "events",
                json!({"pk": "user1", "sk": i as f64, "data": format!("event_{i}")}),
            )
            .unwrap();
        }
        db
    };

    group.throughput(Throughput::Elements(100));
    group.bench_function("file_query_100", |b| {
        b.iter(|| {
            let result = db_query
                .query("events")
                .partition_key("user1")
                .sort_key_between(100.0, 199.0)
                .execute()
                .unwrap();
            assert_eq!(result.items.len(), 100);
            black_box(result);
        });
    });

    // --- file_scan_1k ---
    let dir_scan1k = bench_tempdir();
    let db_scan1k = setup_db_with_items(dir_scan1k.path(), 1_000);

    group.throughput(Throughput::Elements(1_000));
    group.bench_function("file_scan_1k", |b| {
        b.iter(|| {
            let result = db_scan1k.scan("events").execute().unwrap();
            assert_eq!(result.items.len(), 1_000);
            black_box(result);
        });
    });

    // --- file_scan_10k ---
    let dir_scan10k = bench_tempdir();
    let db_scan10k = setup_db_with_items(dir_scan10k.path(), 10_000);

    group.throughput(Throughput::Elements(10_000));
    group.sample_size(20);
    group.bench_function("file_scan_10k", |b| {
        b.iter(|| {
            let result = db_scan10k.scan("events").execute().unwrap();
            assert_eq!(result.items.len(), 10_000);
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Group 4: Concurrency
// ---------------------------------------------------------------------------

fn bench_concurrency(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrency");

    // Shared setup: 1,000 items.
    let dir = bench_tempdir();
    let db = setup_db_with_items(dir.path(), 1_000);

    // --- file_concurrent_reads_4t ---
    group.bench_function("file_concurrent_reads_4t", |b| {
        b.iter(|| {
            let db_arc = Arc::new(db.clone());
            let handles: Vec<_> = (0..4)
                .map(|t| {
                    let db_clone = Arc::clone(&db_arc);
                    thread::spawn(move || {
                        for j in 0..250 {
                            let idx = (t * 250 + j) % 1_000;
                            let pk = format!("user-{}", idx / 100);
                            let sk = idx as f64;
                            let item = db_clone
                                .get_item("events")
                                .partition_key(pk.as_str())
                                .sort_key(sk)
                                .execute()
                                .unwrap();
                            black_box(item);
                        }
                    })
                })
                .collect();
            for h in handles {
                h.join().unwrap();
            }
        });
    });

    // --- file_concurrent_reads_8t ---
    group.bench_function("file_concurrent_reads_8t", |b| {
        b.iter(|| {
            let db_arc = Arc::new(db.clone());
            let handles: Vec<_> = (0..8)
                .map(|t| {
                    let db_clone = Arc::clone(&db_arc);
                    thread::spawn(move || {
                        for j in 0..125 {
                            let idx = (t * 125 + j) % 1_000;
                            let pk = format!("user-{}", idx / 100);
                            let sk = idx as f64;
                            let item = db_clone
                                .get_item("events")
                                .partition_key(pk.as_str())
                                .sort_key(sk)
                                .execute()
                                .unwrap();
                            black_box(item);
                        }
                    })
                })
                .collect();
            for h in handles {
                h.join().unwrap();
            }
        });
    });

    // --- file_mixed_read_write ---
    group.bench_function("file_mixed_read_write", |b| {
        b.iter(|| {
            let db_arc = Arc::new(db.clone());

            // 3 reader threads, 100 reads each.
            let mut handles: Vec<_> = (0..3)
                .map(|t| {
                    let db_clone = Arc::clone(&db_arc);
                    thread::spawn(move || {
                        for j in 0..100 {
                            let idx = (t * 100 + j) % 1_000;
                            let pk = format!("user-{}", idx / 100);
                            let sk = idx as f64;
                            let item = db_clone
                                .get_item("events")
                                .partition_key(pk.as_str())
                                .sort_key(sk)
                                .execute()
                                .unwrap();
                            black_box(item);
                        }
                    })
                })
                .collect();

            // 1 writer thread, 50 puts.
            {
                let db_clone = Arc::clone(&db_arc);
                handles.push(thread::spawn(move || {
                    for j in 0..50 {
                        let pk = format!("user-w{j}");
                        db_clone
                            .put_item(
                                "events",
                                json!({"pk": pk, "sk": j as f64, "data": "written"}),
                            )
                            .unwrap();
                    }
                }));
            }

            for h in handles {
                h.join().unwrap();
            }
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Group 5: Document Sizes
// ---------------------------------------------------------------------------

fn bench_doc_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("doc_sizes");
    group.sample_size(20);

    // Each sub-benchmark gets its own fresh DB.

    // --- file_put_small_doc (~100 bytes) ---
    let dir_s = bench_tempdir();
    let db_s = setup_db_with_items(dir_s.path(), 100);
    group.bench_function("file_put_small_doc", |b| {
        let mut i = 1_000u64;
        b.iter(|| {
            let doc = make_sized_doc(&format!("u{i}"), i as f64, 50);
            db_s.put_item("events", doc).unwrap();
            i += 1;
        });
    });

    // --- file_put_medium_doc (~500 bytes, inline storage) ---
    let dir_m = bench_tempdir();
    let db_m = setup_db_with_items(dir_m.path(), 100);
    group.bench_function("file_put_medium_doc", |b| {
        let mut i = 100_000u64;
        b.iter(|| {
            let doc = make_sized_doc(&format!("u{i}"), i as f64, 400);
            db_m.put_item("events", doc).unwrap();
            i += 1;
        });
    });

    // --- file_put_large_doc (~4KB, triggers overflow at 1500 threshold) ---
    let dir_l = bench_tempdir();
    let db_l = setup_db_with_items(dir_l.path(), 100);
    group.bench_function("file_put_large_doc", |b| {
        let mut i = 1_000_000u64;
        b.iter(|| {
            let doc = make_sized_doc(&format!("u{i}"), i as f64, 4_000);
            db_l.put_item("events", doc).unwrap();
            i += 1;
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Group 6: Cold Start
// ---------------------------------------------------------------------------

fn bench_cold_start(c: &mut Criterion) {
    let mut group = c.benchmark_group("cold_start");

    // Pre-create a DB file with 1,000 items, then drop the handle.
    let dir = bench_tempdir();
    let db_path = dir.path().join("cold.db");
    {
        let db = DynamiteDB::create(&db_path).unwrap();
        db.create_table("events")
            .partition_key("pk", KeyType::String)
            .sort_key("sk", KeyType::Number)
            .execute()
            .unwrap();
        for i in 0..1_000 {
            db.put_item(
                "events",
                json!({"pk": format!("user-{}", i / 100), "sk": i as f64, "data": format!("v{i}")}),
            )
            .unwrap();
        }
        // db is dropped here, releasing the file lock.
    }

    // --- file_open_and_read ---
    group.bench_function("file_open_and_read", |b| {
        b.iter(|| {
            let db = DynamiteDB::open(&db_path).unwrap();
            let item = db
                .get_item("events")
                .partition_key("user-5")
                .sort_key(500.0)
                .execute()
                .unwrap();
            black_box(item);
            // db is dropped each iteration, releasing the file lock for the
            // next open.
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Criterion wiring
// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_point_ops,
    bench_bulk_ops,
    bench_queries,
    bench_concurrency,
    bench_doc_sizes,
    bench_cold_start,
);
criterion_main!(benches);
