//! G5 acceptance: crash-durable, effectively-once projection processing with
//! exact reconciliation against authoritative base state.

use bicdb_core::aggregate_projection::{AggregateProjection, DimensionValue, SaveFailpoint};
use bicdb_core::{BicDb, DbConfig, Record};
use serde_json::json;

fn audited_db(dir: &std::path::Path) -> BicDb {
    BicDb::open_with_config(
        dir,
        DbConfig::default()
            .with_fsync(false)
            .with_audit_events(true),
    )
    .unwrap()
}

fn market_projection() -> AggregateProjection {
    AggregateProjection::new(
        "market",
        "businesses",
        vec!["state".into(), "category".into(), "host".into()],
        vec!["score".into()],
    )
    .unwrap()
}

fn key(state: &str, category: &str, host: &str) -> Vec<DimensionValue> {
    vec![
        DimensionValue::Text(state.into()),
        DimensionValue::Text(category.into()),
        DimensionValue::Text(host.into()),
    ]
}

fn business(id: &str, host: &str, score: f64) -> Record {
    Record::new(id).with_metadata(json!({
        "state": "UK", "category": "dentist", "host": host, "score": score,
    }))
}

/// Reopening must preserve dictionary ID SEMANTICS. Persisted slab bytes hold
/// ids, so a reopen that renumbered the dictionary would silently reinterpret
/// every stored row.
#[test]
fn reopen_preserves_dictionary_id_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let mut db = audited_db(dir.path());
    db.create_collection("businesses").unwrap();
    for (index, host) in ["Hostinger", "GoDaddy", "OVH"].into_iter().enumerate() {
        db.insert("businesses", business(&format!("b{index}"), host, 10.0))
            .unwrap();
    }
    let mut projection = market_projection();
    projection.catch_up(&db).unwrap();
    projection.save(state.path(), true).unwrap();

    let reopened = AggregateProjection::load(state.path(), "market")
        .unwrap()
        .unwrap();
    // Same values resolve to the same cells after a reopen.
    for host in ["Hostinger", "GoDaddy", "OVH"] {
        assert_eq!(
            reopened.cell(&key("UK", "dentist", host)).unwrap().count,
            1,
            "dictionary id for {host} did not survive the reopen"
        );
    }
    assert!(reopened.reconcile(&db).unwrap().is_clean());
}

/// The canonical crash test: ONE logical event that retracts from one cell,
/// applies to another (a BRAND NEW dictionary value), rewrites input state and
/// advances the watermark. Killed at every durability boundary.
#[test]
fn dimension_move_survives_a_crash_at_every_boundary() {
    for failpoint in [
        None,
        Some(SaveFailpoint::BeforeWrite),
        Some(SaveFailpoint::BeforePublish),
        Some(SaveFailpoint::AfterPublish),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let mut db = audited_db(dir.path());
        db.create_collection("businesses").unwrap();
        db.insert("businesses", business("seed", "Hostinger", 10.0))
            .unwrap();
        db.insert("businesses", business("mover", "Hostinger", 62.0))
            .unwrap();

        // Durable starting point.
        let mut projection = market_projection();
        projection.catch_up(&db).unwrap();
        projection.save(state.path(), true).unwrap();

        // The nasty event: Hostinger -> Cloudflare (a value never interned
        // before) with a score change, so one event touches two cells, the
        // input state, a new dictionary id, and the watermark.
        db.insert("businesses", business("mover", "Cloudflare", 77.0))
            .unwrap();
        projection.catch_up(&db).unwrap();
        let _ = projection.save_with_failpoint(state.path(), true, failpoint);

        // "Crash": discard memory entirely and restart from disk.
        drop(projection);
        let recovered =
            AggregateProjection::open(state.path(), "market", &db, || Ok(market_projection()))
                .unwrap();

        let drift = recovered.reconcile(&db).unwrap();
        assert!(
            drift.is_clean(),
            "crash at {failpoint:?} left the projection inconsistent: {drift:?}"
        );
        // And the specific outcome is right, not merely self-consistent.
        assert_eq!(
            recovered
                .cell(&key("UK", "dentist", "Hostinger"))
                .unwrap()
                .count,
            1,
            "seed must remain, mover must have left ({failpoint:?})"
        );
        let moved = recovered.cell(&key("UK", "dentist", "Cloudflare")).unwrap();
        assert_eq!(moved.count, 1);
        assert_eq!(moved.sum(0), 77.0);
        for (_, cell) in recovered.cells() {
            assert!(
                cell.count >= 0,
                "a negative cell count became durable ({failpoint:?})"
            );
        }
    }
}

/// After a restart, replayed events must remain inert and the watermark must
/// never be ahead of the state it was published with.
#[test]
fn replay_after_restart_is_inert_and_watermark_never_leads() {
    let dir = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let mut db = audited_db(dir.path());
    db.create_collection("businesses").unwrap();
    for index in 0..30 {
        db.insert(
            "businesses",
            business(&format!("b{index:03}"), "Hostinger", 5.0),
        )
        .unwrap();
    }
    let mut projection = market_projection();
    projection.catch_up(&db).unwrap();
    projection.save(state.path(), true).unwrap();
    let before = projection.cell(&key("UK", "dentist", "Hostinger"));

    // Restart repeatedly; each reopen re-reads the stream from the watermark.
    for _ in 0..5 {
        let mut reopened =
            AggregateProjection::open(state.path(), "market", &db, || Ok(market_projection()))
                .unwrap();
        assert_eq!(
            reopened.cell(&key("UK", "dentist", "Hostinger")),
            before,
            "replay after restart changed the cells"
        );
        assert!(
            reopened.projection_position() <= AggregateProjection::source_position(&db),
            "watermark got ahead of the source"
        );
        reopened.save(state.path(), true).unwrap();
    }
}

/// Deleting the last contribution must remove the cell, durably.
#[test]
fn removing_the_last_contribution_removes_the_cell_durably() {
    let dir = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let mut db = audited_db(dir.path());
    db.create_collection("businesses").unwrap();
    db.insert("businesses", business("only", "OVH", 12.0))
        .unwrap();
    let mut projection = market_projection();
    projection.catch_up(&db).unwrap();
    projection.save(state.path(), true).unwrap();
    assert!(projection.cell(&key("UK", "dentist", "OVH")).is_some());

    db.delete("businesses", "only").unwrap();
    projection.catch_up(&db).unwrap();
    projection.save(state.path(), true).unwrap();
    drop(projection);

    let recovered =
        AggregateProjection::open(state.path(), "market", &db, || Ok(market_projection())).unwrap();
    assert!(
        recovered.cell(&key("UK", "dentist", "OVH")).is_none(),
        "an emptied cell survived a restart"
    );
    assert!(recovered.reconcile(&db).unwrap().is_clean());
}

/// The storm: churn with dimension moves and deletes, crashing at pseudo-random
/// points, restarting from disk each time. Reality gets to find what the model
/// did not.
#[test]
fn random_crash_storm_reconciles_exactly() {
    let dir = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let mut db = audited_db(dir.path());
    db.create_collection("businesses").unwrap();
    let hosts = ["Hostinger", "Cloudflare", "GoDaddy", "OVH", "Fastly"];
    for index in 0..200 {
        db.insert(
            "businesses",
            business(
                &format!("b{index:04}"),
                hosts[index % hosts.len()],
                (index % 50) as f64,
            ),
        )
        .unwrap();
    }
    let mut projection = market_projection();
    projection.catch_up(&db).unwrap();
    projection.save(state.path(), true).unwrap();

    let mut seed = 0xfeed_face_dead_beefu64;
    let mut next = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };

    for round in 0..60 {
        // Mutate: moves, score changes, deletes.
        for step in 0..7 {
            let index = (next() as usize) % 200;
            let id = format!("b{index:04}");
            if step == 3 {
                db.delete("businesses", &id).unwrap();
                continue;
            }
            db.insert(
                "businesses",
                business(
                    &id,
                    hosts[(next() as usize) % hosts.len()],
                    (next() % 90) as f64,
                ),
            )
            .unwrap();
        }

        // Reopen from disk, follow the stream, then crash at a random point.
        let mut live =
            AggregateProjection::open(state.path(), "market", &db, || Ok(market_projection()))
                .unwrap();
        let failpoint = match next() % 4 {
            0 => Some(SaveFailpoint::BeforeWrite),
            1 => Some(SaveFailpoint::BeforePublish),
            2 => Some(SaveFailpoint::AfterPublish),
            _ => None,
        };
        let _ = live.save_with_failpoint(state.path(), true, failpoint);
        drop(live);

        // Whatever survived on disk must reconcile exactly once caught up.
        let mut recovered =
            AggregateProjection::open(state.path(), "market", &db, || Ok(market_projection()))
                .unwrap();
        let drift = recovered.reconcile(&db).unwrap();
        assert!(
            drift.is_clean(),
            "round {round} (crash {failpoint:?}) drifted: {drift:?}"
        );
        for (_, cell) in recovered.cells() {
            assert!(cell.count >= 0, "negative cell count in round {round}");
        }
        // Persist a good snapshot so the next round starts from disk.
        recovered.save(state.path(), true).unwrap();
    }
}
