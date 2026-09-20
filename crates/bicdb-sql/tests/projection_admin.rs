//! G10 (EXPLAIN) and G11 (RECONCILE) as operator commands.

use bicdb_core::aggregate_projection::{AggregateProjection, PROJECTIONS_DIR};
use bicdb_core::{BicDb, DbConfig, Record};
use bicdb_sql::{SqlSession, SqlValue};
use serde_json::json;

fn build(dims: Vec<String>, name: &str) -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_audit_events(true),
    )
    .unwrap();
    db.create_collection("businesses").unwrap();
    // 600 rows: 3 states x 4 categories = 12 good cells, but `id` is unique
    // per row — a deliberately terrible dimension.
    let records: Vec<Record> = (0..600)
        .map(|index| {
            Record::new(format!("b{index:04}")).with_metadata(json!({
                "state": format!("s{}", index % 3),
                "category": format!("c{}", index % 4),
                "uniq": format!("u{index}"),
                "score": (index % 50) as f64,
            }))
        })
        .collect();
    db.bulk_load_insert("businesses", records).unwrap();
    let mut projection =
        AggregateProjection::new(name, "businesses", dims, vec!["score".into()]).unwrap();
    projection.rebuild_from_base(&db).unwrap();
    projection
        .save(dir.path().join(PROJECTIONS_DIR), true)
        .unwrap();
    (dir, db)
}

#[test]
fn explain_reports_a_good_grain_without_warning() {
    let (_dir, mut db) = build(vec!["state".into(), "category".into()], "good");
    let mut sql = SqlSession::new(&mut db);
    let result = sql.execute("EXPLAIN MATERIALIZED AGGREGATE good").unwrap();
    assert_eq!(result.rows[0][0], SqlValue::Int(600), "source rows");
    assert_eq!(
        result.rows[0][1],
        SqlValue::Int(12),
        "3 states x 4 categories"
    );
    assert_eq!(result.rows[0][2], SqlValue::Float(50.0), "600/12 = 50x");
    assert_eq!(result.rows[0][3], SqlValue::String("3, 4".into()));
    assert_eq!(result.rows[0][6], SqlValue::String("none".into()));
}

#[test]
fn explain_warns_about_a_high_cardinality_grain() {
    let (_dir, mut db) = build(vec!["state".into(), "uniq".into()], "bad");
    let mut sql = SqlSession::new(&mut db);
    let result = sql.execute("EXPLAIN MATERIALIZED AGGREGATE bad").unwrap();
    assert_eq!(result.rows[0][1], SqlValue::Int(600), "one cell per row");
    assert_eq!(
        result.rows[0][2],
        SqlValue::Float(1.0),
        "no reduction at all"
    );
    let SqlValue::String(warnings) = &result.rows[0][6] else {
        panic!("expected warnings");
    };
    assert!(
        warnings.contains("high-cardinality"),
        "a 1x-reduction grain must warn: {warnings}"
    );
    assert!(
        warnings.contains("uniq"),
        "the warning must name the dominating dimension: {warnings}"
    );
}

#[test]
fn reconcile_reports_exact_and_detects_nothing_wrong() {
    let (_dir, mut db) = build(vec!["state".into(), "category".into()], "good");
    // Mutate past the saved snapshot; RECONCILE must catch up first.
    db.insert(
        "businesses",
        Record::new("b0000").with_metadata(json!({
            "state": "s0", "category": "c0", "uniq": "u0", "score": 99.0,
        })),
    )
    .unwrap();
    let mut sql = SqlSession::new(&mut db);
    let result = sql
        .execute("RECONCILE MATERIALIZED AGGREGATE good")
        .unwrap();
    assert_eq!(result.columns[0], "cells_checked");
    assert_eq!(result.rows[0][1], SqlValue::Int(0), "missing");
    assert_eq!(result.rows[0][2], SqlValue::Int(0), "extra");
    assert_eq!(result.rows[0][3], SqlValue::Int(0), "measure mismatches");
    assert_eq!(result.rows[0][4], SqlValue::String("EXACT".into()));
}

#[test]
fn unknown_aggregate_names_itself() {
    let (_dir, mut db) = build(vec!["state".into()], "good");
    let mut sql = SqlSession::new(&mut db);
    let error = sql
        .execute("EXPLAIN MATERIALIZED AGGREGATE nope")
        .unwrap_err()
        .to_string();
    assert!(error.contains("nope"), "{error}");
}
