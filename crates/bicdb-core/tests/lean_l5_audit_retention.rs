//! L5: mesh-safe audit retention. Trimming superseded history must not
//! disturb per-origin positions — peers keep syncing pure deltas across a
//! trim, with no full resend and no divergence.

use bicdb_core::{BicDb, DbConfig, NodeId, Record, SyncCheckpoint};
use serde_json::json;
use uuid::Uuid;

fn node(value: u128) -> NodeId {
    NodeId(Uuid::from_u128(value))
}

fn sync_config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_audit_events(true)
        .with_mesh_signing(false)
        .with_require_signed_imports(false)
        .with_unsafe_legacy_mesh_collections(true)
}

fn sync_into(target: &mut BicDb, source: &mut BicDb) -> usize {
    let vector = target.sync_vector().unwrap();
    let bundle = source.export_sync_bundle_delta(&vector).unwrap();
    let sent = bundle.event_count;
    target.import_sync_bundle(bundle).unwrap();
    sent
}

#[test]
fn trim_drops_superseded_history_and_keeps_state() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_node_id(dir.path(), sync_config(), node(1)).unwrap();
    db.create_collection("docs").unwrap();
    for round in 0..5 {
        for index in 0..40 {
            db.insert(
                "docs",
                Record::new(format!("k{index:03}"))
                    .with_metadata(json!({"round": round, "index": index})),
            )
            .unwrap();
        }
    }
    let before = db.export_events_since(0).len();
    let report = db.trim_event_horizon(SyncCheckpoint::new(0)).unwrap();
    assert_eq!(
        report.superseded_dropped, 160,
        "4 superseded rounds x 40 records"
    );
    let after = db.export_events_since(0).len();
    assert_eq!(after, before - 160);

    // State intact: latest round everywhere.
    for index in 0..40 {
        let record = db.get("docs", &format!("k{index:03}")).unwrap().unwrap();
        assert_eq!(record.metadata["round"], 4);
    }

    // Base persists across reopen and keeps positions monotonic.
    let vector_before = db.sync_vector().unwrap();
    db.close().unwrap();
    let mut db = BicDb::open_with_node_id(dir.path(), sync_config(), node(1)).unwrap();
    assert_eq!(db.sync_vector().unwrap(), vector_before);
    db.insert("docs", Record::new("new-after-trim")).unwrap();
    let vector_after = db.sync_vector().unwrap();
    assert!(
        vector_after.watermark(&node(1)).unwrap() > vector_before.watermark(&node(1)).unwrap(),
        "positions must keep growing after a trim"
    );
}

#[test]
fn mesh_deltas_stay_pure_deltas_across_a_trim() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let mut a = BicDb::open_with_node_id(dir_a.path(), sync_config(), node(1)).unwrap();
    let mut b = BicDb::open_with_node_id(dir_b.path(), sync_config(), node(2)).unwrap();

    a.create_collection("docs").unwrap();
    for round in 0..3 {
        for index in 0..30 {
            a.insert(
                "docs",
                Record::new(format!("k{index:03}")).with_metadata(json!({"round": round})),
            )
            .unwrap();
        }
    }
    sync_into(&mut b, &mut a);
    assert_eq!(sync_into(&mut b, &mut a), 0, "converged before the trim");

    // A trims its superseded history, then keeps writing.
    let report = a.trim_event_horizon(SyncCheckpoint::new(0)).unwrap();
    assert!(report.superseded_dropped > 0);
    for index in 0..10 {
        a.insert(
            "docs",
            Record::new(format!("fresh{index:02}")).with_metadata(json!({"post_trim": true})),
        )
        .unwrap();
    }

    // The delta to B is EXACTLY the 10 new events — the trim moved log
    // offsets, but frozen origin positions keep B's vector valid.
    let sent = sync_into(&mut b, &mut a);
    assert_eq!(
        sent, 10,
        "post-trim sync must be a pure delta, not a resend"
    );
    for index in 0..10 {
        assert!(b
            .get("docs", &format!("fresh{index:02}"))
            .unwrap()
            .is_some());
    }
    // And nothing flows back or diverges.
    assert_eq!(sync_into(&mut a, &mut b), 0);
    assert_eq!(sync_into(&mut b, &mut a), 0);
    for index in 0..30 {
        assert_eq!(
            a.get("docs", &format!("k{index:03}"))
                .unwrap()
                .unwrap()
                .metadata["round"],
            2
        );
        assert_eq!(
            b.get("docs", &format!("k{index:03}"))
                .unwrap()
                .unwrap()
                .metadata["round"],
            2
        );
    }
}
