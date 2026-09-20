//! G6: a checkpoint must cost MUTATIONS, not total state.

use bicdb_core::aggregate_projection::AggregateProjection;
use bicdb_core::{BicDb, DbConfig, Record};
use serde_json::json;

fn bytes_written(dir: &std::path::Path) -> u64 {
    fn walk(path: &std::path::Path) -> u64 {
        let mut total = 0;
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    total += walk(&path);
                } else if let Ok(meta) = entry.metadata() {
                    total += meta.len();
                }
            }
        }
        total
    }
    walk(dir)
}

#[test]
fn checkpoint_cost_tracks_mutations_not_state_size() {
    let dir = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_audit_events(true),
    )
    .unwrap();
    db.create_collection("businesses").unwrap();
    let records: Vec<Record> = (0..40_000)
        .map(|index| {
            Record::new(format!("b{index:06}")).with_metadata(json!({
                "state": format!("s{}", index % 20),
                "category": format!("c{}", index % 50),
                "host": format!("h{}", index % 10),
                "score": (index % 100) as f64,
            }))
        })
        .collect();
    db.bulk_load_insert("businesses", records).unwrap();

    let mut projection = AggregateProjection::new(
        "big",
        "businesses",
        vec!["state".into(), "category".into(), "host".into()],
        vec!["score".into()],
    )
    .unwrap();
    projection.rebuild_from_base(&db).unwrap();
    projection.save(state.path(), false).unwrap();
    let after_full = bytes_written(state.path());
    assert!(
        after_full > 500_000,
        "the full checkpoint should be sizeable"
    );

    // Mutate a HANDFUL of rows, then checkpoint again.
    for index in 0..5 {
        db.insert(
            "businesses",
            Record::new(format!("b{index:06}")).with_metadata(json!({
                "state": "s0", "category": "c0", "host": "h9", "score": 1.0,
            })),
        )
        .unwrap();
    }
    projection.catch_up(&db).unwrap();
    projection.save(state.path(), false).unwrap();
    let after_incremental = bytes_written(state.path());

    let delta = after_incremental - after_full;
    assert!(
        delta * 4 < after_full,
        "an incremental checkpoint wrote {delta} bytes against a {after_full}-byte state — \
         that is not incremental"
    );

    // And it is still exactly correct after a reload.
    drop(projection);
    let reloaded = AggregateProjection::open(state.path(), "big", &db, || {
        AggregateProjection::new(
            "big",
            "businesses",
            vec!["state".into(), "category".into(), "host".into()],
            vec!["score".into()],
        )
    })
    .unwrap();
    assert!(reloaded.verify_durable_invariants(&db).unwrap().is_clean());
}

/// A publish that fails after writing its pages leaves them on disk. They are
/// inert, but until they are reclaimed a projection that fails to publish
/// repeatedly grows without bound.
#[test]
fn pages_from_a_failed_publish_are_reclaimed() {
    use bicdb_core::aggregate_projection::SaveFailpoint;

    let dir = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_audit_events(true),
    )
    .unwrap();
    db.create_collection("businesses").unwrap();
    let records: Vec<Record> = (0..8_000)
        .map(|index| {
            Record::new(format!("b{index:06}")).with_metadata(json!({
                "state": format!("s{}", index % 20),
                "category": format!("c{}", index % 50),
                "score": (index % 100) as f64,
            }))
        })
        .collect();
    db.bulk_load_insert("businesses", records).unwrap();

    let mut projection = AggregateProjection::new(
        "orphans",
        "businesses",
        vec!["state".into(), "category".into()],
        vec!["score".into()],
    )
    .unwrap();
    projection.rebuild_from_base(&db).unwrap();
    projection.save(state.path(), false).unwrap();

    let pages = state.path().join("orphans.projection").join("pages");
    let after_first = std::fs::read_dir(&pages).unwrap().count();

    // Three publishes that fail AFTER writing their pages.
    for index in 0..3 {
        db.insert(
            "businesses",
            Record::new(format!("late{index}")).with_metadata(json!({
                "state": "s1", "category": "c1", "score": 1.0,
            })),
        )
        .unwrap();
        projection.catch_up(&db).unwrap();
        assert!(
            projection
                .save_with_failpoint(state.path(), false, Some(SaveFailpoint::BeforePublish))
                .is_err(),
            "the failpoint did not fire"
        );
    }
    let after_failures = std::fs::read_dir(&pages).unwrap().count();
    assert!(
        after_failures > after_first,
        "the failed publishes wrote no pages, so nothing is being tested \
         ({after_first} -> {after_failures})"
    );

    // A successful publish must reclaim them.
    db.insert(
        "businesses",
        Record::new("final").with_metadata(json!({
            "state": "s2", "category": "c2", "score": 2.0,
        })),
    )
    .unwrap();
    projection.catch_up(&db).unwrap();
    projection.save(state.path(), false).unwrap();
    let after_success = std::fs::read_dir(&pages).unwrap().count();

    assert!(
        after_success <= after_first + 4,
        "orphan pages survived a successful publish: {after_first} -> \
         {after_failures} -> {after_success}"
    );

    // And the projection still reads correctly.
    let reloaded = AggregateProjection::load(state.path(), "orphans")
        .unwrap()
        .expect("snapshot");
    reloaded.verify_durable_invariants(&db).unwrap();
}
