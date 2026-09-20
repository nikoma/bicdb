//! G5.2: durable projections queryable as ordinary relations, with freshness
//! visible rather than implied.

use bicdb_core::aggregate_projection::{AggregateProjection, PROJECTIONS_DIR};
use bicdb_core::{BicDb, DbConfig, Record};
use bicdb_sql::{SqlSession, SqlValue};
use serde_json::json;

fn seeded() -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_audit_events(true),
    )
    .unwrap();
    db.create_collection("businesses").unwrap();
    let rows = [
        ("b1", "MH", "dentist", 80.0),
        ("b2", "MH", "dentist", 60.0),
        ("b3", "MH", "yoga", 90.0),
        ("b4", "KA", "dentist", 40.0),
    ];
    for (id, state, category, score) in rows {
        db.insert(
            "businesses",
            Record::new(id).with_metadata(json!({
                "state": state, "category": category, "score": score,
            })),
        )
        .unwrap();
    }
    let mut projection = AggregateProjection::new(
        "business_market",
        "businesses",
        vec!["state".into(), "category".into()],
        vec!["score".into()],
    )
    .unwrap();
    projection.catch_up(&db).unwrap();
    projection
        .save(dir.path().join(PROJECTIONS_DIR), true)
        .unwrap();
    (dir, db)
}

#[test]
fn projections_are_queryable_relations() {
    let (_dir, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);

    // The shape from the design doc: filter, project, order.
    let result = sql
        .execute(
            "SELECT state, category, count, avg_score \
             FROM bicdb_projection.business_market \
             WHERE state = 'MH' ORDER BY count DESC, category",
        )
        .unwrap();
    assert_eq!(
        result.rows.len(),
        2,
        "only Maharashtra cells: {:?}",
        result.rows
    );
    assert_eq!(result.rows[0][0], SqlValue::String("MH".into()));
    assert_eq!(result.rows[0][1], SqlValue::String("dentist".into()));
    assert_eq!(result.rows[0][2], SqlValue::Int(2));
    assert_eq!(result.rows[0][3], SqlValue::Float(70.0), "avg of 80 and 60");
    assert_eq!(result.rows[1][2], SqlValue::Int(1));

    // Ordinary aggregation OVER the projection works too — rollup for free.
    let rollup = sql
        .execute(
            "SELECT state, sum(count) FROM bicdb_projection.business_market \
             GROUP BY state ORDER BY state",
        )
        .unwrap();
    assert_eq!(rollup.rows.len(), 2);
    assert_eq!(rollup.rows[0][0], SqlValue::String("KA".into()));
    assert_eq!(rollup.rows[0][1], SqlValue::Int(1));
    assert_eq!(rollup.rows[1][1], SqlValue::Int(3), "MH has 3 businesses");
}

#[test]
fn projection_status_exposes_freshness() {
    let (_dir, mut db) = seeded();
    // Write past the saved watermark so the projection is genuinely behind.
    db.insert(
        "businesses",
        Record::new("b5").with_metadata(json!({"state": "MH", "category": "yoga", "score": 10.0})),
    )
    .unwrap();

    let mut sql = SqlSession::new(&mut db);
    let status = sql
        .execute(
            "SELECT name, collection, dimensions, measures, projection_position, \
             source_position, lag_events, cells, source_rows FROM bicdb_projections",
        )
        .unwrap();
    assert_eq!(status.rows.len(), 1);
    assert_eq!(
        status.rows[0][0],
        SqlValue::String("business_market".into())
    );
    assert_eq!(status.rows[0][1], SqlValue::String("businesses".into()));
    assert_eq!(
        status.rows[0][2],
        SqlValue::String("state, category".into())
    );
    assert_eq!(status.rows[0][3], SqlValue::String("score".into()));
    let SqlValue::Int(lag) = status.rows[0][6] else {
        panic!("lag must be an integer");
    };
    assert!(
        lag > 0,
        "a projection behind the stream must report lag, got {lag}"
    );

    // Reading the relation catches up, so the query never serves state older
    // than what is committed.
    let fresh = sql
        .execute(
            "SELECT count FROM bicdb_projection.business_market \
             WHERE state = 'MH' AND category = 'yoga'",
        )
        .unwrap();
    assert_eq!(fresh.rows[0][0], SqlValue::Int(2), "read must reflect b5");
}

#[test]
fn unknown_projection_is_a_clear_error() {
    let (_dir, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    let error = sql
        .execute("SELECT * FROM bicdb_projection.does_not_exist")
        .unwrap_err()
        .to_string();
    assert!(error.contains("does_not_exist"), "unhelpful error: {error}");
}
