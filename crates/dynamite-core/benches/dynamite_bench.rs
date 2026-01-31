use criterion::{Criterion, criterion_group, criterion_main};
use dynamite_core::api::DynaMite;
use dynamite_core::types::KeyType;
use serde_json::json;
use std::sync::Arc;
use std::thread;
use tempfile::tempdir;

fn bench_put_item(c: &mut Criterion) {
    c.bench_function("put_item", |b| {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("bench.db");
        let db = DynaMite::create(&db_path).unwrap();
        db.create_table("items")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        let mut i = 0u64;
        b.iter(|| {
            let key = format!("key_{i:08}");
            db.put_item("items", json!({"id": key, "value": i}))
                .unwrap();
            i += 1;
        });
    });
}

fn bench_get_item(c: &mut Criterion) {
    c.bench_function("get_item", |b| {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("bench.db");
        let db = DynaMite::create(&db_path).unwrap();
        db.create_table("items")
            .partition_key("id", KeyType::String)
            .execute()
            .unwrap();

        // Prepopulate with 100 items.
        for i in 0..100 {
            db.put_item("items", json!({"id": format!("key_{i:04}"), "value": i}))
                .unwrap();
        }

        let mut i = 0u64;
        b.iter(|| {
            let key = format!("key_{:04}", i % 100);
            let _item = db
                .get_item("items")
                .partition_key(key.as_str())
                .execute()
                .unwrap();
            i += 1;
        });
    });
}

fn bench_bulk_insert_1k(c: &mut Criterion) {
    c.bench_function("bulk_insert_1k", |b| {
        b.iter(|| {
            let dir = tempdir().unwrap();
            let db_path = dir.path().join("bench.db");
            let db = DynaMite::create(&db_path).unwrap();
            db.create_table("items")
                .partition_key("id", KeyType::String)
                .execute()
                .unwrap();

            for i in 0..1_000 {
                db.put_item("items", json!({"id": format!("key_{i:06}"), "value": i}))
                    .unwrap();
            }
        });
    });
}

fn bench_range_query(c: &mut Criterion) {
    // Prepopulate a database once outside the benchmark loop.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("bench.db");
    let db = DynaMite::create(&db_path).unwrap();
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

    c.bench_function("range_query_100", |b| {
        b.iter(|| {
            let result = db
                .query("events")
                .partition_key("user1")
                .sort_key_between(100.0, 199.0)
                .execute()
                .unwrap();
            assert_eq!(result.items.len(), 100);
        });
    });
}

fn bench_concurrent_reads(c: &mut Criterion) {
    // Prepopulate a database once.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("bench.db");
    let db = DynaMite::create(&db_path).unwrap();
    db.create_table("items")
        .partition_key("id", KeyType::String)
        .execute()
        .unwrap();
    db.put_item("items", json!({"id": "target", "value": 42}))
        .unwrap();

    c.bench_function("concurrent_reads_4x100", |b| {
        b.iter(|| {
            let db_arc = Arc::new(db.clone());
            let handles: Vec<_> = (0..4)
                .map(|_| {
                    let db_clone = Arc::clone(&db_arc);
                    thread::spawn(move || {
                        for _ in 0..100 {
                            let _item = db_clone
                                .get_item("items")
                                .partition_key("target")
                                .execute()
                                .unwrap();
                        }
                    })
                })
                .collect();

            for h in handles {
                h.join().unwrap();
            }
        });
    });
}

criterion_group!(
    benches,
    bench_put_item,
    bench_get_item,
    bench_bulk_insert_1k,
    bench_range_query,
    bench_concurrent_reads,
);
criterion_main!(benches);
