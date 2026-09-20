//! Repeated checkpoints from one live projection.
//!
//! G6 made a checkpoint incremental by writing only dirty pages and publishing
//! a tiny manifest, and its crash-safety argument rests entirely on page files
//! being **immutable and generation-stamped**: a page written before the
//! manifest cannot corrupt the current checkpoint because nothing published
//! refers to it yet. These tests check that the property actually holds when a
//! projection checkpoints more than once.

use bicdb_core::aggregate_projection::AggregateProjection;
use bicdb_core::{BicDb, DbConfig, Record};
use serde_json::json;

fn audited_db(dir: &tempfile::TempDir) -> BicDb {
    BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_audit_events(true),
    )
    .unwrap()
}

fn projection() -> AggregateProjection {
    AggregateProjection::new(
        "hygiene",
        "rows",
        vec!["bucket".to_string()],
        vec!["value".to_string()],
    )
    .unwrap()
}

fn row(index: usize, value: f64) -> Record {
    Record::new(format!("r{index}")).with_metadata(json!({
        "bucket": format!("b{}", index % 64),
        "value": value,
    }))
}

fn page_names(store: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(store.join("hygiene.projection").join("pages"))
        .map(|entries| {
            entries
                .flatten()
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

fn manifest_generation(store: &std::path::Path) -> u64 {
    let bytes = std::fs::read(store.join("hygiene.projection").join("manifest.json")).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    value["generation"].as_u64().unwrap()
}

/// Each checkpoint must publish a NEW generation. If two checkpoints share a
/// generation they also share page filenames, which means the second one
/// rewrites files the first one's published manifest still points at.
#[test]
fn every_checkpoint_advances_the_generation() {
    let dir = tempfile::tempdir().unwrap();
    let store = tempfile::tempdir().unwrap();
    let mut db = audited_db(&dir);
    db.create_collection("rows").unwrap();
    for index in 0..500 {
        db.insert("rows", row(index, index as f64)).unwrap();
    }
    let mut projection = projection();
    projection.catch_up(&db).unwrap();

    let mut generations = Vec::new();
    for round in 0..4 {
        db.insert("rows", row(round, 1000.0 + round as f64))
            .unwrap();
        projection.catch_up(&db).unwrap();
        projection.save(store.path(), false).unwrap();
        generations.push(manifest_generation(store.path()));
    }

    let mut sorted = generations.clone();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        generations.len(),
        "checkpoints reused a generation: {generations:?} — a later save rewrote \
         page files that an already-published manifest still references"
    );
    assert!(
        generations.windows(2).all(|pair| pair[1] > pair[0]),
        "generations did not advance monotonically: {generations:?}"
    );
}

/// A published manifest's pages must never be rewritten in place. This checks
/// it directly: record the bytes of every page the manifest references, take
/// another checkpoint, and confirm none of those files changed.
#[test]
fn a_published_page_is_never_rewritten() {
    let dir = tempfile::tempdir().unwrap();
    let store = tempfile::tempdir().unwrap();
    let mut db = audited_db(&dir);
    db.create_collection("rows").unwrap();
    for index in 0..500 {
        db.insert("rows", row(index, index as f64)).unwrap();
    }
    let mut projection = projection();
    projection.catch_up(&db).unwrap();
    projection.save(store.path(), false).unwrap();

    let pages_dir = store.path().join("hygiene.projection").join("pages");
    let before: Vec<(String, Vec<u8>)> = page_names(store.path())
        .into_iter()
        .map(|name| {
            let bytes = std::fs::read(pages_dir.join(&name)).unwrap();
            (name, bytes)
        })
        .collect();

    // Mutate and checkpoint again.
    for index in 0..40 {
        db.insert("rows", row(index, 5000.0 + index as f64))
            .unwrap();
    }
    projection.catch_up(&db).unwrap();
    projection.save(store.path(), false).unwrap();

    for (name, original) in &before {
        let path = pages_dir.join(name);
        if !path.exists() {
            // Being pruned is fine — being CHANGED is not.
            continue;
        }
        let now = std::fs::read(&path).unwrap();
        assert_eq!(
            &now, original,
            "page `{name}` was rewritten in place; a crash during that write \
             would have corrupted the checkpoint published before it"
        );
    }
}

/// Superseded page files must not accumulate forever.
#[test]
fn superseded_pages_are_reclaimed() {
    let dir = tempfile::tempdir().unwrap();
    let store = tempfile::tempdir().unwrap();
    let mut db = audited_db(&dir);
    db.create_collection("rows").unwrap();
    for index in 0..2000 {
        db.insert("rows", row(index, index as f64)).unwrap();
    }
    let mut projection = projection();
    projection.catch_up(&db).unwrap();
    projection.save(store.path(), false).unwrap();
    let steady = page_names(store.path()).len();

    // Twenty more checkpoints, each touching a handful of pages.
    for round in 0..20 {
        for index in 0..10 {
            db.insert("rows", row(index + round * 10, 9000.0 + round as f64))
                .unwrap();
        }
        projection.catch_up(&db).unwrap();
        projection.save(store.path(), false).unwrap();
    }

    let after = page_names(store.path()).len();
    assert!(
        after <= steady * 3,
        "page files grew from {steady} to {after} over 20 checkpoints — \
         superseded generations are never reclaimed"
    );
}

/// The second checkpoint must write only what changed since the FIRST
/// checkpoint, not everything dirtied since the projection was built.
#[test]
fn checkpoints_stay_incremental_across_repeated_saves() {
    let dir = tempfile::tempdir().unwrap();
    let store = tempfile::tempdir().unwrap();
    let mut db = audited_db(&dir);
    db.create_collection("rows").unwrap();
    for index in 0..4000 {
        db.insert("rows", row(index, index as f64)).unwrap();
    }
    let mut projection = projection();
    projection.catch_up(&db).unwrap();

    // First checkpoint: the whole state.
    projection.save(store.path(), false).unwrap();
    let pages_dir = store.path().join("hygiene.projection").join("pages");
    let full_bytes: u64 = std::fs::read_dir(&pages_dir)
        .unwrap()
        .flatten()
        .map(|entry| entry.metadata().unwrap().len())
        .sum();
    let after_first: Vec<std::ffi::OsString> = std::fs::read_dir(&pages_dir)
        .unwrap()
        .flatten()
        .map(|entry| entry.file_name())
        .collect();

    // Touch five rows and checkpoint again into the SAME directory. Pages are
    // generation-stamped, so anything this checkpoint wrote has a filename
    // that did not exist before it.
    for index in 0..5 {
        db.insert("rows", row(index, 7777.0)).unwrap();
    }
    projection.catch_up(&db).unwrap();
    projection.save(store.path(), false).unwrap();

    let rewritten: u64 = std::fs::read_dir(&pages_dir)
        .unwrap()
        .flatten()
        .filter(|entry| !after_first.contains(&entry.file_name()))
        .map(|entry| entry.metadata().unwrap().len())
        .sum();

    assert!(
        rewritten * 4 < full_bytes,
        "the second checkpoint wrote {rewritten} bytes against a full state of \
         {full_bytes} — dirty pages are not being cleared, so every checkpoint \
         re-writes everything dirtied since the projection was built"
    );
}
