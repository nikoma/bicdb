//! G7b acceptance: non-retractable measures inside a retractable engine.
//!
//! Every other measure here is an abelian group — apply adds, retract
//! subtracts, and they cancel. Sketches are not, so they get a different
//! contract: a retract marks the cell stale and the cell is recomputed from
//! its own rows. These tests exist to prove the contract holds, because a
//! wrong distinct-count does not crash anything — it just quietly answers a
//! business question incorrectly.

use bicdb_core::aggregate_projection::{
    AggregateProjection, DimensionValue, SketchKind, SketchSpec,
};
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

/// Distinct hosts and the price distribution, per state.
fn new_projection() -> AggregateProjection {
    AggregateProjection::new(
        "hosts_by_state",
        "businesses",
        vec!["state".to_string()],
        vec!["price".to_string()],
    )
    .unwrap()
    .with_sketches(vec![
        SketchSpec {
            name: "distinct_hosts".to_string(),
            path: "host".to_string(),
            kind: SketchKind::DistinctCount,
        },
        SketchSpec {
            name: "price".to_string(),
            path: "price".to_string(),
            kind: SketchKind::Quantile,
        },
    ])
    .unwrap()
}

fn key(state: &str) -> Vec<DimensionValue> {
    vec![DimensionValue::Text(state.to_string())]
}

fn business(id: &str, state: &str, host: &str, price: f64) -> Record {
    Record::new(id).with_metadata(json!({ "state": state, "host": host, "price": price }))
}

fn distinct(projection: &mut AggregateProjection, state: &str) -> f64 {
    projection
        .cell_sketch(&key(state), 0)
        .unwrap()
        .distinct_estimate()
        .unwrap()
}

/// The baseline: distinct-count ignores duplicates, unlike `count`.
#[test]
fn distinct_count_is_not_row_count() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = audited_db(&dir);
    db.create_collection("businesses").unwrap();
    for index in 0..30 {
        // Ten rows each on three hosts.
        let host = format!("host{}", index % 3);
        db.insert(
            "businesses",
            business(&format!("b{index}"), "UK", &host, 10.0),
        )
        .unwrap();
    }

    let mut projection = new_projection();
    projection.catch_up(&db).unwrap();

    assert_eq!(projection.cell(&key("UK")).unwrap().count, 30);
    assert_eq!(distinct(&mut projection, "UK").round(), 3.0);
}

/// The property the whole design rests on: a cell rebuilt after a retract
/// must equal a cell that was only ever added to. If these can differ, the
/// reconcile oracle cannot check sketches at all.
#[test]
fn rebuild_after_retract_equals_never_retracted() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = audited_db(&dir);
    db.create_collection("businesses").unwrap();
    for index in 0..200 {
        let host = format!("host{}", index % 40);
        db.insert(
            "businesses",
            business(&format!("b{index}"), "UK", &host, index as f64),
        )
        .unwrap();
    }
    let mut incremental = new_projection();
    incremental.catch_up(&db).unwrap();

    // Delete a row: the sketch cannot subtract, so the cell must be rebuilt.
    db.delete("businesses", "b7").unwrap();
    incremental.catch_up(&db).unwrap();

    // A projection built fresh from the surviving base rows is the oracle.
    let mut authoritative = new_projection();
    authoritative.rebuild_from_base(&db).unwrap();

    assert_eq!(
        incremental.cell_sketch(&key("UK"), 0),
        authoritative.cell_sketch(&key("UK"), 0),
        "distinct sketch drifted after a retract-driven rebuild"
    );
    assert_eq!(
        incremental.cell_sketch(&key("UK"), 1),
        authoritative.cell_sketch(&key("UK"), 1),
        "quantile sketch drifted after a retract-driven rebuild"
    );
}

/// Deleting every row that carried a value must actually lower the answer.
/// A sketch that only ever grows would pass the test above and still be
/// useless.
#[test]
fn deleting_rows_lowers_the_distinct_count() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = audited_db(&dir);
    db.create_collection("businesses").unwrap();
    for index in 0..20 {
        db.insert(
            "businesses",
            business(&format!("b{index}"), "UK", &format!("host{index}"), 5.0),
        )
        .unwrap();
    }
    let mut projection = new_projection();
    projection.catch_up(&db).unwrap();
    assert_eq!(distinct(&mut projection, "UK").round(), 20.0);

    for index in 0..15 {
        db.delete("businesses", &format!("b{index}")).unwrap();
    }
    projection.catch_up(&db).unwrap();

    assert_eq!(
        distinct(&mut projection, "UK").round(),
        5.0,
        "the distinct-count did not shrink when its rows were deleted"
    );
}

/// A dimension change moves a row between cells; BOTH sketches must follow.
#[test]
fn a_dimension_move_rebuilds_both_cells() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = audited_db(&dir);
    db.create_collection("businesses").unwrap();
    db.insert("businesses", business("b1", "UK", "hostA", 10.0))
        .unwrap();
    db.insert("businesses", business("b2", "UK", "hostB", 20.0))
        .unwrap();
    db.insert("businesses", business("b3", "IE", "hostC", 30.0))
        .unwrap();

    let mut projection = new_projection();
    projection.catch_up(&db).unwrap();
    assert_eq!(distinct(&mut projection, "UK").round(), 2.0);
    assert_eq!(distinct(&mut projection, "IE").round(), 1.0);

    // b1 relocates to IE.
    db.insert("businesses", business("b1", "IE", "hostA", 10.0))
        .unwrap();
    projection.catch_up(&db).unwrap();

    assert_eq!(
        distinct(&mut projection, "UK").round(),
        1.0,
        "the vacated cell kept a host that left it"
    );
    assert_eq!(
        distinct(&mut projection, "IE").round(),
        2.0,
        "the receiving cell did not gain the arriving host"
    );

    let mut authoritative = new_projection();
    authoritative.rebuild_from_base(&db).unwrap();
    for state in ["UK", "IE"] {
        assert_eq!(
            projection.cell_sketch(&key(state), 0),
            authoritative.cell_sketch(&key(state), 0)
        );
    }
}

/// Sketch state is derived and deliberately NOT persisted, so a reload has to
/// reconstruct it exactly from the input slab.
#[test]
fn sketches_survive_a_checkpoint_and_reload() {
    let dir = tempfile::tempdir().unwrap();
    let store = tempfile::tempdir().unwrap();
    let mut db = audited_db(&dir);
    db.create_collection("businesses").unwrap();
    for index in 0..150 {
        let state = if index % 2 == 0 { "UK" } else { "IE" };
        db.insert(
            "businesses",
            business(
                &format!("b{index}"),
                state,
                &format!("host{}", index % 17),
                index as f64,
            ),
        )
        .unwrap();
    }
    let mut projection = new_projection();
    projection.catch_up(&db).unwrap();
    db.delete("businesses", "b4").unwrap();
    projection.catch_up(&db).unwrap();
    projection.save(store.path(), true).unwrap();

    let before: Vec<_> = ["UK", "IE"]
        .iter()
        .map(|state| {
            (
                projection.cell_sketch(&key(state), 0),
                projection.cell_sketch(&key(state), 1),
            )
        })
        .collect();

    let mut reloaded = AggregateProjection::load(store.path(), "hosts_by_state")
        .unwrap()
        .expect("snapshot");
    let after: Vec<_> = ["UK", "IE"]
        .iter()
        .map(|state| {
            (
                reloaded.cell_sketch(&key(state), 0),
                reloaded.cell_sketch(&key(state), 1),
            )
        })
        .collect();

    assert_eq!(before, after, "sketches did not survive the reload");
    // And the row chains they depend on were rebuilt correctly.
    reloaded.verify_durable_invariants(&db).unwrap();
}

/// Churn: interleaved inserts, updates, dimension moves and deletes, with the
/// chain invariant and the authoritative comparison checked throughout.
#[test]
fn churn_keeps_chains_and_sketches_honest() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = audited_db(&dir);
    db.create_collection("businesses").unwrap();
    let states = ["UK", "IE", "FR"];
    for index in 0..300 {
        db.insert(
            "businesses",
            business(
                &format!("b{index}"),
                states[index % 3],
                &format!("host{}", index % 23),
                (index % 50) as f64,
            ),
        )
        .unwrap();
    }
    let mut projection = new_projection();
    projection.catch_up(&db).unwrap();

    for round in 0..4 {
        for index in (round..300).step_by(7) {
            if index % 3 == 0 {
                db.delete("businesses", &format!("b{index}")).ok();
            } else {
                db.insert(
                    "businesses",
                    business(
                        &format!("b{index}"),
                        states[(index + round) % 3],
                        &format!("host{}", (index + round) % 23),
                        ((index + round) % 50) as f64,
                    ),
                )
                .ok();
            }
        }
        projection.catch_up(&db).unwrap();
        projection.verify_durable_invariants(&db).unwrap();
    }

    let mut authoritative = new_projection();
    authoritative.rebuild_from_base(&db).unwrap();
    for state in states {
        assert_eq!(
            projection.cell_sketch(&key(state), 0),
            authoritative.cell_sketch(&key(state), 0),
            "distinct sketch for {state} drifted under churn"
        );
        assert_eq!(
            projection.cell_sketch(&key(state), 1),
            authoritative.cell_sketch(&key(state), 1),
            "quantile sketch for {state} drifted under churn"
        );
    }
}

/// Quantiles over a known distribution. The sampling bound is generous
/// because the sketch is approximate — but a median that lands in the wrong
/// half of the data is a bug, not sampling error.
#[test]
fn percentiles_track_the_distribution() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = audited_db(&dir);
    db.create_collection("businesses").unwrap();
    for index in 1..=5000 {
        db.insert(
            "businesses",
            business(&format!("b{index}"), "UK", "host", index as f64),
        )
        .unwrap();
    }
    let mut projection = new_projection();
    projection.catch_up(&db).unwrap();

    let sketch = projection.cell_sketch(&key("UK"), 1).unwrap();
    let median = sketch.percentile(0.5).unwrap();
    assert!(
        (median - 2500.0).abs() < 250.0,
        "median {median} is not near 2500"
    );
    let p95 = sketch.percentile(0.95).unwrap();
    assert!((p95 - 4750.0).abs() < 250.0, "p95 {p95} is not near 4750");
}

/// A projection with no sketches must be byte-identical in behaviour and
/// width to one built before sketches existed — otherwise every existing
/// projection pays for a feature it did not ask for.
#[test]
fn declaring_no_sketches_costs_nothing() {
    let plain = AggregateProjection::new(
        "plain",
        "businesses",
        vec!["state".to_string()],
        vec!["price".to_string()],
    )
    .unwrap();
    let with_none = AggregateProjection::new(
        "plain",
        "businesses",
        vec!["state".to_string()],
        vec!["price".to_string()],
    )
    .unwrap()
    .with_sketches(Vec::new())
    .unwrap();
    assert_eq!(
        plain.layout().state_width(),
        with_none.layout().state_width()
    );
    assert_eq!(plain.layout().sketches, 0);
}
