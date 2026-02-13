use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use swirldb_core::core::SwirlDB;
use swirldb_core::paths::PathRegistry;
use automerge::transaction::Transactable;
use automerge::{AutoCommit, ObjType, ROOT};

fn create_large_document(num_objects: usize) -> AutoCommit {
    let mut doc = AutoCommit::new();

    // Create a realistic nested structure with many objects
    // Structure: users -> [user objects] -> profile, settings, etc.
    let users = doc.put_object(&ROOT, "users", ObjType::Map).unwrap();

    for i in 0..num_objects {
        let user_id = format!("user_{}", i);
        let user = doc.put_object(&users, &user_id, ObjType::Map).unwrap();

        // Add some nested structure
        let profile = doc.put_object(&user, "profile", ObjType::Map).unwrap();
        doc.put(&profile, "name", format!("User {}", i)).unwrap();
        doc.put(&profile, "email", format!("user{}@example.com", i)).unwrap();

        let settings = doc.put_object(&user, "settings", ObjType::Map).unwrap();
        doc.put(&settings, "theme", "dark").unwrap();
        doc.put(&settings, "notifications", true).unwrap();
    }

    doc
}

fn bench_registry_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("registry_build");

    for size in [100, 1000, 10000].iter() {
        let doc = create_large_document(*size);

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_objects", size * 4)), // *4 because each user has ~4 objects
            size,
            |b, _| {
                b.iter(|| {
                    PathRegistry::from_document(black_box(&doc)).unwrap()
                });
            },
        );
    }

    group.finish();
}

fn bench_path_extraction(c: &mut Criterion) {
    let mut group = c.benchmark_group("path_extraction");

    // Create a document with some data
    let db = SwirlDB::new();
    db.set_path("user.profile.name", "Alice".into()).unwrap();
    db.set_path("user.profile.email", "alice@example.com".into()).unwrap();
    db.set_path("user.settings.theme", "dark".into()).unwrap();

    // Make some changes
    db.set_path("user.profile.name", "Bob".into()).unwrap();
    db.set_path("user.profile.avatar", "avatar.png".into()).unwrap();
    db.set_path("user.settings.notifications", true.into()).unwrap();

    let changes = db.get_changes();

    group.bench_function("extract_3_paths", |b| {
        b.iter(|| {
            db.extract_affected_paths(black_box(&changes)).unwrap()
        });
    });

    group.finish();
}

fn bench_incremental_updates(c: &mut Criterion) {
    let mut group = c.benchmark_group("incremental_updates");

    group.bench_function("set_path_with_registry_update", |b| {
        b.iter(|| {
            let db = SwirlDB::new();
            // This will create intermediate objects and update registry
            db.set_path("deeply.nested.path.to.value", "data".into()).unwrap();
        });
    });

    group.finish();
}

criterion_group!(benches, bench_registry_build, bench_path_extraction, bench_incremental_updates);
criterion_main!(benches);
