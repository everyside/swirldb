use automerge::ScalarValue;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use swirldb_core::core::SwirlDB;

/// Create a realistic chat-like document with many messages
fn create_chat_document(num_messages: usize) -> SwirlDB {
    let db = SwirlDB::new();

    for i in 0..num_messages {
        let path = format!("messages[{}].id", i);
        db.set_path(&path, ScalarValue::Str(format!("msg_{}", i).into()))
            .unwrap();

        let path = format!("messages[{}].text", i);
        db.set_path(&path, ScalarValue::Str("Hello world!".into()))
            .unwrap();

        let path = format!("messages[{}].from", i);
        db.set_path(&path, ScalarValue::Str("alice".into()))
            .unwrap();

        let path = format!("messages[{}].timestamp", i);
        db.set_path(&path, ScalarValue::Int(i as i64)).unwrap();
    }

    db
}

fn bench_save_state(c: &mut Criterion) {
    let mut group = c.benchmark_group("save_state");

    for size in [10, 100, 1000].iter() {
        let db = create_chat_document(*size);

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_messages", size)),
            size,
            |b, _| {
                b.iter(|| black_box(db.save_state()));
            },
        );
    }

    group.finish();
}

fn bench_load_state(c: &mut Criterion) {
    let mut group = c.benchmark_group("load_state");

    for size in [10, 100, 1000].iter() {
        let db = create_chat_document(*size);
        let state = db.save_state();

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_messages", size)),
            size,
            |b, _| {
                b.iter(|| {
                    let fresh_db = SwirlDB::new();
                    fresh_db.load_state(black_box(&state)).unwrap()
                });
            },
        );
    }

    group.finish();
}

fn bench_get_value(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_value");

    for size in [10, 100, 1000].iter() {
        let db = create_chat_document(*size);

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_messages", size)),
            size,
            |b, _| {
                b.iter(|| black_box(db.get_value("messages")));
            },
        );
    }

    group.finish();
}

fn bench_observer_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("observer_overhead");

    // Benchmark with no observers
    let db = SwirlDB::new();
    group.bench_function("no_observers", |b| {
        b.iter(|| {
            db.set_path("counter", ScalarValue::Int(42)).unwrap();
        });
    });

    // Benchmark with 10 observers
    let db = SwirlDB::new();
    for i in 0..10 {
        let path = format!("field_{}", i);
        db.observe(path.clone(), move |_| {
            // Empty observer
        });
    }
    group.bench_function("10_observers_no_match", |b| {
        b.iter(|| {
            db.set_path("counter", ScalarValue::Int(42)).unwrap();
        });
    });

    // Benchmark with 10 observers, one matches
    let db = SwirlDB::new();
    db.observe("counter".to_string(), |_| {});
    for i in 0..9 {
        let path = format!("field_{}", i);
        db.observe(path.clone(), move |_| {});
    }
    group.bench_function("10_observers_one_match", |b| {
        b.iter(|| {
            db.set_path("counter", ScalarValue::Int(42)).unwrap();
        });
    });

    // Benchmark with 100 observers
    let db = SwirlDB::new();
    for i in 0..100 {
        let path = format!("field_{}", i);
        db.observe(path.clone(), move |_| {});
    }
    group.bench_function("100_observers_no_match", |b| {
        b.iter(|| {
            db.set_path("counter", ScalarValue::Int(42)).unwrap();
        });
    });

    // Benchmark with 1000 observers
    let db = SwirlDB::new();
    for i in 0..1000 {
        let path = format!("field_{}", i);
        db.observe(path.clone(), move |_| {});
    }
    group.bench_function("1000_observers_no_match", |b| {
        b.iter(|| {
            db.set_path("counter", ScalarValue::Int(42)).unwrap();
        });
    });

    group.finish();
}

fn bench_apply_changes(c: &mut Criterion) {
    let mut group = c.benchmark_group("apply_changes");

    for size in [1, 10, 100].iter() {
        // Create a document and make changes
        let db1 = SwirlDB::new();
        for i in 0..*size {
            db1.set_path(&format!("msg_{}", i), ScalarValue::Str("data".into()))
                .unwrap();
        }
        let changes = db1.get_changes();

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_changes", size)),
            size,
            |b, _| {
                b.iter(|| {
                    let db2 = SwirlDB::new();
                    db2.apply_changes(black_box(changes.clone())).unwrap()
                });
            },
        );
    }

    group.finish();
}

fn bench_deep_path_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("deep_path_creation");

    for depth in [5, 10, 20].iter() {
        let path = (0..*depth)
            .map(|i| format!("level{}", i))
            .collect::<Vec<_>>()
            .join(".");

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_levels", depth)),
            depth,
            |b, _| {
                b.iter(|| {
                    let db = SwirlDB::new();
                    db.set_path(black_box(&path), ScalarValue::Str("value".into()))
                        .unwrap()
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_save_state,
    bench_load_state,
    bench_get_value,
    bench_observer_overhead,
    bench_apply_changes,
    bench_deep_path_creation
);
criterion_main!(benches);
