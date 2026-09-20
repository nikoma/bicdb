//! G2–G4 acceptance: incremental aggregation must equal authoritative
//! recomputation through updates, dimension moves, deletes, replay,
//! out-of-order delivery and restart.

use bicdb_core::aggregate_projection::{AggregateProjection, DimensionValue};
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
        "website_market",
        "businesses",
        vec![
            "state".to_string(),
            "category".to_string(),
            "host".to_string(),
        ],
        vec!["score".to_string()],
    )
    .unwrap()
}

fn key(state: &str, category: &str, host: &str) -> Vec<DimensionValue> {
    vec![
        DimensionValue::Text(state.to_string()),
        DimensionValue::Text(category.to_string()),
        DimensionValue::Text(host.to_string()),
    ]
}

fn business(id: &str, state: &str, category: &str, host: &str, score: f64) -> Record {
    Record::new(id).with_metadata(json!({
        "state": state, "category": category, "host": host, "score": score,
    }))
}

/// The simple case: a re-crawl changes only the measure.
#[test]
fn measure_change_updates_the_same_cell() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = audited_db(&dir);
    db.create_collection("businesses").unwrap();
    db.insert(
        "businesses",
        business("b1", "UK", "dentist", "Hostinger", 62.0),
    )
    .unwrap();

    let mut projection = projection();
    projection.catch_up(&db).unwrap();
    let cell = projection.cell(&key("UK", "dentist", "Hostinger")).unwrap();
    assert_eq!(cell.count, 1);
    assert_eq!(cell.sum(0), 62.0);

    db.insert(
        "businesses",
        business("b1", "UK", "dentist", "Hostinger", 77.0),
    )
    .unwrap();
    projection.catch_up(&db).unwrap();

    let cell = projection.cell(&key("UK", "dentist", "Hostinger")).unwrap();
    assert_eq!(cell.count, 1, "an update must not double-count the row");
    assert_eq!(cell.sum(0), 77.0, "the old 62 must have been retracted");
    assert_eq!(cell.avg(0), Some(77.0));
    assert!(projection.reconcile(&db).unwrap().is_clean());
}

/// The nasty case: the update moves the row BETWEEN cells.
#[test]
fn dimension_change_moves_the_row_between_cells() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = audited_db(&dir);
    db.create_collection("businesses").unwrap();
    db.insert(
        "businesses",
        business("b1", "UK", "dentist", "Hostinger", 62.0),
    )
    .unwrap();
    let mut projection = projection();
    projection.catch_up(&db).unwrap();

    // Re-crawl: hosting provider AND score change.
    db.insert(
        "businesses",
        business("b1", "UK", "dentist", "Cloudflare", 77.0),
    )
    .unwrap();
    projection.catch_up(&db).unwrap();

    assert!(
        projection
            .cell(&key("UK", "dentist", "Hostinger"))
            .is_none(),
        "the vacated cell must disappear, not linger at count 0"
    );
    let cell = projection
        .cell(&key("UK", "dentist", "Cloudflare"))
        .unwrap();
    assert_eq!(cell.count, 1);
    assert_eq!(cell.sum(0), 77.0);
    assert_eq!(projection.cell_count(), 1);
    assert!(projection.reconcile(&db).unwrap().is_clean());
}

/// Replay and duplicate delivery must be no-ops.
#[test]
fn replayed_events_do_not_drift_the_cells() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = audited_db(&dir);
    db.create_collection("businesses").unwrap();
    for index in 0..25 {
        db.insert(
            "businesses",
            business(&format!("b{index}"), "UK", "dentist", "Hostinger", 60.0),
        )
        .unwrap();
    }
    let mut projection = projection();
    projection.catch_up(&db).unwrap();
    let before = projection.cell(&key("UK", "dentist", "Hostinger"));

    // Fold the entire stream repeatedly — the shape of an at-least-once
    // consumer restarting and re-reading.
    for _ in 0..5 {
        projection.catch_up(&db).unwrap();
    }
    let after = projection.cell(&key("UK", "dentist", "Hostinger"));
    assert_eq!(before, after, "replay must be a no-op");
    assert_eq!(after.unwrap().count, 25);
    assert!(projection.reconcile(&db).unwrap().is_clean());

    // Re-applying individual events one at a time is likewise inert.
    for stored in db.events().read(bicdb_core::RECORD_AUDIT_STREAM) {
        assert!(!projection.apply_event(&stored).unwrap());
    }
    assert!(projection.reconcile(&db).unwrap().is_clean());
}

/// Out-of-order delivery: an older event must not overwrite a newer state.
#[test]
fn out_of_order_events_are_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = audited_db(&dir);
    db.create_collection("businesses").unwrap();
    db.insert(
        "businesses",
        business("b1", "UK", "dentist", "Hostinger", 62.0),
    )
    .unwrap();
    db.insert(
        "businesses",
        business("b1", "UK", "dentist", "Cloudflare", 77.0),
    )
    .unwrap();

    let events = db.events().read(bicdb_core::RECORD_AUDIT_STREAM);
    let mut projection = projection();
    // Deliver newest first, then the stale one.
    for stored in events.iter().rev() {
        projection.apply_event(stored).unwrap();
    }

    let cell = projection
        .cell(&key("UK", "dentist", "Cloudflare"))
        .unwrap();
    assert_eq!(cell.count, 1);
    assert_eq!(cell.sum(0), 77.0, "the stale event must not win");
    assert!(projection
        .cell(&key("UK", "dentist", "Hostinger"))
        .is_none());
    assert_eq!(projection.cell_count(), 1);
}

/// Deletes retract; the watermark and lag are reportable.
#[test]
fn deletes_retract_and_lag_is_visible() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = audited_db(&dir);
    db.create_collection("businesses").unwrap();
    db.insert(
        "businesses",
        business("b1", "UK", "dentist", "Hostinger", 62.0),
    )
    .unwrap();
    db.insert(
        "businesses",
        business("b2", "UK", "dentist", "Hostinger", 80.0),
    )
    .unwrap();
    let mut projection = projection();
    projection.catch_up(&db).unwrap();
    assert_eq!(
        projection
            .cell(&key("UK", "dentist", "Hostinger"))
            .unwrap()
            .count,
        2
    );

    db.delete("businesses", "b1").unwrap();

    // Before catching up, the projection must ADMIT it is behind rather than
    // silently serving stale numbers.
    let source_position = AggregateProjection::source_position(&db);
    assert!(
        projection.lag_events(source_position) > 0,
        "an un-caught-up projection must report lag"
    );

    projection.catch_up(&db).unwrap();
    let cell = projection.cell(&key("UK", "dentist", "Hostinger")).unwrap();
    assert_eq!(cell.count, 1);
    assert_eq!(cell.sum(0), 80.0);
    assert_eq!(projection.lag_events(source_position), 0);
    assert!(projection.reconcile(&db).unwrap().is_clean());
}

/// The acceptance bar: after a churn of inserts, dimension moves, measure
/// changes and deletes — with repeated interleaved replays standing in for
/// crash/restart — incremental must EQUAL authoritative recomputation.
#[test]
fn incremental_equals_authoritative_after_heavy_churn() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = audited_db(&dir);
    db.create_collection("businesses").unwrap();
    let states = ["UK", "MH", "KA", "TN"];
    let categories = ["dentist", "yoga", "restaurant"];
    let hosts = ["Hostinger", "Cloudflare", "GoDaddy"];

    let mut projection = projection();
    for index in 0..400usize {
        db.insert(
            "businesses",
            business(
                &format!("b{index:04}"),
                states[index % states.len()],
                categories[index % categories.len()],
                hosts[index % hosts.len()],
                (index % 100) as f64,
            ),
        )
        .unwrap();
        // Fold partway through, so the projection is repeatedly resumed from
        // a mid-stream watermark rather than built once at the end.
        if index % 37 == 0 {
            projection.catch_up(&db).unwrap();
        }
    }

    // Re-crawls: move hosts and change scores, including moving some rows
    // back to a host they previously left.
    for index in (0..400usize).step_by(3) {
        db.insert(
            "businesses",
            business(
                &format!("b{index:04}"),
                states[index % states.len()],
                categories[index % categories.len()],
                hosts[(index + 1) % hosts.len()],
                ((index * 7) % 100) as f64,
            ),
        )
        .unwrap();
        if index % 53 == 0 {
            projection.catch_up(&db).unwrap();
        }
    }

    // Deletions, including rows already moved.
    for index in (0..400usize).step_by(11) {
        db.delete("businesses", &format!("b{index:04}")).unwrap();
    }

    // Restart-ish: fold the whole stream again from scratch positions.
    projection.catch_up(&db).unwrap();
    projection.catch_up(&db).unwrap();

    let drift = projection.reconcile(&db).unwrap();
    assert!(
        drift.is_clean(),
        "incremental disagreed with recomputation: {drift:?}"
    );
    assert!(
        drift.cells_compared > 0,
        "the test must actually compare cells"
    );

    // And the totals independently match the base table.
    let live = db.scan_collection("businesses").unwrap();
    let total: i64 = projection.cells().map(|(_, cell)| cell.count).sum();
    assert_eq!(
        total as usize,
        live.len(),
        "cell counts must sum to the row count"
    );
    let sum_scores: f64 = projection.cells().map(|(_, cell)| cell.sum(0)).sum();
    let expected: f64 = live
        .iter()
        .filter_map(|record| record.metadata.get("score").and_then(|v| v.as_f64()))
        .sum();
    assert!(
        (sum_scores - expected).abs() < 1e-6,
        "summed measures drifted: {sum_scores} vs {expected}"
    );
}

/// Mergeability (docs §8): two partial states combine associatively, which is
/// what makes per-shard aggregation possible later.
#[test]
fn cell_states_merge_associatively() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = audited_db(&dir);
    db.create_collection("businesses").unwrap();
    for index in 0..10 {
        db.insert(
            "businesses",
            business(&format!("b{index}"), "UK", "dentist", "Hostinger", 10.0),
        )
        .unwrap();
    }
    let mut whole = projection();
    whole.catch_up(&db).unwrap();
    let whole_cell = whole.cell(&key("UK", "dentist", "Hostinger")).unwrap();

    // Two disjoint "shards" over the same rows.
    let events = db.events().read(bicdb_core::RECORD_AUDIT_STREAM);
    let (left_events, right_events) = events.split_at(events.len() / 2);
    let mut left = projection();
    let mut right = projection();
    for stored in left_events {
        left.apply_event(stored).unwrap();
    }
    for stored in right_events {
        right.apply_event(stored).unwrap();
    }

    let mut merged = left
        .cell(&key("UK", "dentist", "Hostinger"))
        .unwrap_or_default();
    if let Some(other) = right.cell(&key("UK", "dentist", "Hostinger")) {
        merged.merge(&other);
    }
    assert_eq!(merged.count, whole_cell.count);
    assert!((merged.sum(0) - whole_cell.sum(0)).abs() < 1e-9);
}

/// CI-sized version of the Tier-1 scale harness
/// (`examples/projection_scale.rs`): skewed, production-shaped corpus with
/// missing values, created by BULK LOAD (which emits no audit events, so the
/// projection is built by rebuild-from-base and then follows the stream —
/// the real lifecycle), then a recrawl storm that moves rows between cells.
#[test]
fn rebuild_then_catch_up_survives_a_recrawl_storm() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = audited_db(&dir);
    db.create_collection("businesses").unwrap();

    // Deterministic, skewed, with a realistic missing-host rate.
    let mut seed = 0x1234_5678_9abc_def0u64;
    let mut next = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    let rows = 4_000usize;
    let records: Vec<Record> = (0..rows)
        .map(|index| {
            let host = (next() % 9) as usize;
            let host_value = if next() % 100 < 22 {
                serde_json::Value::Null
            } else {
                json!(format!("host-{host}"))
            };
            Record::new(format!("biz-{index:06}")).with_metadata(json!({
                "state": format!("state-{}", next() % 6),
                "category": format!("cat-{}", next() % 12),
                "host": host_value,
                "score": (next() % 100) as f64,
            }))
        })
        .collect();
    db.bulk_load_insert("businesses", records).unwrap();

    let mut projection = projection();
    let built = projection.rebuild_from_base(&db).unwrap();
    assert_eq!(built, rows);
    assert!(
        projection.reconcile(&db).unwrap().is_clean(),
        "rebuild-from-base must match the table it was built from"
    );

    // Rows with a NULL host still have to land somewhere, or cell counts stop
    // summing to COUNT(*).
    let total: i64 = projection.cells().map(|(_, cell)| cell.count).sum();
    assert_eq!(
        total as usize, rows,
        "missing dimension values must still be counted"
    );

    // Recrawl storm: score changes, host MOVES, and deletes.
    for step in 0..rows / 2 {
        let index = (next() as usize) % rows;
        let id = format!("biz-{index:06}");
        if step % 17 == 0 {
            db.delete("businesses", &id).unwrap();
            continue;
        }
        let Some(existing) = db.get("businesses", &id).unwrap() else {
            continue;
        };
        let mut metadata = existing.metadata.clone();
        metadata["score"] = json!((next() % 100) as f64);
        if step % 3 == 0 {
            metadata["host"] = json!(format!("host-{}", next() % 9));
        }
        db.insert("businesses", Record::new(id).with_metadata(metadata))
            .unwrap();
    }

    projection.catch_up(&db).unwrap();
    // Replaying the stream again must change nothing.
    projection.catch_up(&db).unwrap();

    let drift = projection.reconcile(&db).unwrap();
    assert!(drift.is_clean(), "recrawl storm drifted: {drift:?}");

    let live = db.scan_collection("businesses").unwrap();
    let total: i64 = projection.cells().map(|(_, cell)| cell.count).sum();
    assert_eq!(total as usize, live.len());

    // The cost the design doc requires to be reportable.
    let residency = projection.residency();
    assert_eq!(residency.input_records, live.len());
    assert!(residency.input_bytes > 0 && residency.cell_bytes > 0);
}

/// G4.5: with interned dimensions, retract MUST use the id recorded when the
/// row was applied — never a re-resolution of the new row's values. If a
/// dimension move re-resolved "Cloudflare" and retracted from there, the
/// Hostinger cell would keep a phantom row forever and the new cell would go
/// negative. The stored id is authoritative.
#[test]
fn retract_uses_the_stored_dictionary_id_not_the_new_value() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = audited_db(&dir);
    db.create_collection("businesses").unwrap();

    // Mint several host ids so the moved-from and moved-to ids differ and are
    // not adjacent — a bug that reused the wrong slot would be visible.
    for (index, host) in ["Hostinger", "GoDaddy", "OVH", "Cloudflare"]
        .into_iter()
        .enumerate()
    {
        db.insert(
            "businesses",
            business(&format!("seed{index}"), "UK", "dentist", host, 10.0),
        )
        .unwrap();
    }
    db.insert(
        "businesses",
        business("mover", "UK", "dentist", "Hostinger", 62.0),
    )
    .unwrap();
    let mut projection = projection();
    projection.catch_up(&db).unwrap();
    assert_eq!(
        projection
            .cell(&key("UK", "dentist", "Hostinger"))
            .unwrap()
            .count,
        2,
        "seed + mover"
    );

    // Move it to a host interned much later.
    db.insert(
        "businesses",
        business("mover", "UK", "dentist", "Cloudflare", 77.0),
    )
    .unwrap();
    projection.catch_up(&db).unwrap();

    let from = projection.cell(&key("UK", "dentist", "Hostinger")).unwrap();
    assert_eq!(
        from.count, 1,
        "the vacated cell must lose exactly the mover"
    );
    assert_eq!(from.sum(0), 10.0, "the mover's 62 must have left this cell");
    let to = projection
        .cell(&key("UK", "dentist", "Cloudflare"))
        .unwrap();
    assert_eq!(to.count, 2);
    assert_eq!(to.sum(0), 87.0, "10 (seed) + 77 (mover)");

    // No cell may go negative — the signature of retracting the wrong id.
    for (_, cell) in projection.cells() {
        assert!(
            cell.count >= 0,
            "a cell went negative: retract hit the wrong id"
        );
    }
    assert!(projection.reconcile(&db).unwrap().is_clean());

    // A value never written must not mint an id on the read path.
    assert!(projection
        .cell(&key("UK", "dentist", "NeverSeen"))
        .is_none());
    assert!(projection.reconcile(&db).unwrap().is_clean());
}

/// G4.6a: input-state width is derived from the DECLARED grain, so a
/// COUNT-only projection carries no room for measures it will never have.
#[test]
fn layout_width_scales_with_the_declared_grain() {
    let count_only = AggregateProjection::new(
        "count_only",
        "businesses",
        vec!["state".into(), "category".into(), "host".into()],
        vec![],
    )
    .unwrap();
    let one_avg = AggregateProjection::new(
        "one_avg",
        "businesses",
        vec!["state".into(), "category".into(), "host".into()],
        vec!["score".into()],
    )
    .unwrap();
    let three_avg = AggregateProjection::new(
        "three_avg",
        "businesses",
        vec!["state".into(), "category".into(), "host".into()],
        vec!["score".into(), "lcp".into(), "ttfb".into()],
    )
    .unwrap();

    // width = 9 + 4*dimensions + 8*measures
    assert_eq!(count_only.layout().state_width(), 25 + 12);
    assert_eq!(one_avg.layout().state_width(), 25 + 12 + 8);
    assert_eq!(three_avg.layout().state_width(), 25 + 12 + 24);

    // Logical cost per row = 16-byte identity + that width. The point of the
    // change: cost tracks what was asked for, not MAX_EVERYTHING.
    assert_eq!(count_only.layout().logical_bytes_per_row(), 37);
    assert_eq!(one_avg.layout().logical_bytes_per_row(), 45);
    assert_eq!(three_avg.layout().logical_bytes_per_row(), 61);
    assert!(
        count_only.layout().state_width() < three_avg.layout().state_width(),
        "a COUNT-only projection must be strictly cheaper"
    );

    // Fewer dimensions is cheaper too.
    let one_dim = AggregateProjection::new(
        "one_dim",
        "businesses",
        vec!["state".into()],
        vec!["score".into()],
    )
    .unwrap();
    assert_eq!(one_dim.layout().state_width(), 25 + 4 + 8);
}

/// A COUNT-only projection (no measures) must still aggregate correctly —
/// the zero-measure layout is a real case, not a degenerate one.
#[test]
fn count_only_projection_aggregates_and_reconciles() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = audited_db(&dir);
    db.create_collection("businesses").unwrap();
    let mut projection = AggregateProjection::new(
        "count_only",
        "businesses",
        vec!["state".into(), "category".into(), "host".into()],
        vec![],
    )
    .unwrap();

    for index in 0..40 {
        db.insert(
            "businesses",
            business(&format!("b{index}"), "UK", "dentist", "Hostinger", 10.0),
        )
        .unwrap();
    }
    projection.catch_up(&db).unwrap();
    let cell = projection.cell(&key("UK", "dentist", "Hostinger")).unwrap();
    assert_eq!(cell.count, 40);
    assert_eq!(cell.avg(0), None, "no measure was declared");

    // A dimension move still retracts correctly with zero measures.
    db.insert(
        "businesses",
        business("b0", "UK", "dentist", "Cloudflare", 10.0),
    )
    .unwrap();
    projection.catch_up(&db).unwrap();
    assert_eq!(
        projection
            .cell(&key("UK", "dentist", "Hostinger"))
            .unwrap()
            .count,
        39
    );
    assert_eq!(
        projection
            .cell(&key("UK", "dentist", "Cloudflare"))
            .unwrap()
            .count,
        1
    );
    assert!(projection.reconcile(&db).unwrap().is_clean());

    let residency = projection.residency();
    assert_eq!(residency.input_logical_bytes, 40 * 37);
}

/// Deleted rows must return their slab slot rather than leaking it.
#[test]
fn deleted_rows_release_their_slot_for_reuse() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = audited_db(&dir);
    db.create_collection("businesses").unwrap();
    let mut projection = projection();
    for index in 0..50 {
        db.insert(
            "businesses",
            business(&format!("b{index:03}"), "UK", "dentist", "Hostinger", 5.0),
        )
        .unwrap();
    }
    projection.catch_up(&db).unwrap();
    let full = projection.residency().slab_bytes;

    for index in 0..50 {
        db.delete("businesses", &format!("b{index:03}")).unwrap();
    }
    projection.catch_up(&db).unwrap();
    assert_eq!(projection.residency().input_records, 0);

    // Re-inserting must reuse the freed slots, not grow the slab.
    for index in 0..50 {
        db.insert(
            "businesses",
            business(&format!("c{index:03}"), "UK", "yoga", "GoDaddy", 7.0),
        )
        .unwrap();
    }
    projection.catch_up(&db).unwrap();
    assert!(
        projection.residency().slab_bytes <= full,
        "slab grew instead of reusing vacated slots"
    );
    assert!(projection.reconcile(&db).unwrap().is_clean());
}
