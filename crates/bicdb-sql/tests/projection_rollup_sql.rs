//! G8 SQL surface: `ROLLUP MATERIALIZED AGGREGATE <name> TO (...)`.

use bicdb_core::aggregate_projection::{AggregateProjection, SketchKind, SketchSpec};
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
    db.create_collection("sales").unwrap();
    let categories = ["pizza", "sushi"];
    for index in 0..200 {
        let month = 1 + (index % 2);
        db.insert(
            "sales",
            Record::new(format!("s{index}")).with_metadata(json!({
                "day": format!("2026-{month:02}-{:02}", 1 + (index % 20)),
                "category": categories[(index / 2) % 2],
                "host": format!("host{}", index % 12),
                "price": (index % 50) as f64,
            })),
        )
        .unwrap();
    }
    let mut projection = AggregateProjection::new(
        "sales_daily",
        "sales",
        vec!["day".to_string(), "category".to_string()],
        vec!["price".to_string()],
    )
    .unwrap()
    .with_sketches(vec![SketchSpec {
        name: "hosts".to_string(),
        path: "host".to_string(),
        kind: SketchKind::DistinctCount,
    }])
    .unwrap();
    projection.catch_up(&db).unwrap();
    let directory = db
        .data_path()
        .join(bicdb_core::aggregate_projection::PROJECTIONS_DIR);
    std::fs::create_dir_all(&directory).unwrap();
    projection.save(&directory, true).unwrap();
    (dir, db)
}

#[test]
fn rollup_coarsens_a_dimension() {
    let (_dir, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    let result = sql
        .execute("ROLLUP MATERIALIZED AGGREGATE sales_daily TO (day PREFIX 7)")
        .unwrap();

    // Two months x two categories.
    assert_eq!(result.rows.len(), 4);
    assert_eq!(result.columns[0], "day");
    assert_eq!(result.columns[1], "category");
    assert!(result.columns.contains(&"distinct_hosts".to_string()));

    let total: i64 = result
        .rows
        .iter()
        .map(|row| match row[2] {
            SqlValue::Int(count) => count,
            _ => panic!("count is not an integer"),
        })
        .sum();
    assert_eq!(total, 200);
    for row in &result.rows {
        let SqlValue::String(month) = &row[0] else {
            panic!("day did not coarsen to a string month");
        };
        assert_eq!(month.len(), 7, "`{month}` is not YYYY-MM");
    }
}

/// The point of doing this in the engine: distinct-counts must merge, not add.
#[test]
fn rolled_distinct_counts_do_not_add_up() {
    let (_dir, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    let result = sql
        .execute("ROLLUP MATERIALIZED AGGREGATE sales_daily TO (day WHOLE, category WHOLE)")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let distinct = result
        .columns
        .iter()
        .position(|column| column == "distinct_hosts")
        .unwrap();
    // Twelve hosts total, however many day/category cells they were spread
    // across. Adding per-cell distinct-counts would give a much larger number.
    assert_eq!(result.rows[0][distinct], SqlValue::Int(12));
}

#[test]
fn whole_drops_the_column_from_the_result() {
    let (_dir, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    let result = sql
        .execute("ROLLUP MATERIALIZED AGGREGATE sales_daily TO (day WHOLE)")
        .unwrap();
    assert_eq!(result.columns[0], "category");
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn an_unknown_dimension_is_a_clear_error() {
    let (_dir, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    let error = sql
        .execute("ROLLUP MATERIALIZED AGGREGATE sales_daily TO (region PREFIX 2)")
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("not a dimension"),
        "unhelpful error: {error}"
    );
}

#[test]
fn an_unknown_level_is_a_clear_error() {
    let (_dir, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    let error = sql
        .execute("ROLLUP MATERIALIZED AGGREGATE sales_daily TO (day WOBBLE 3)")
        .unwrap_err()
        .to_string();
    assert!(error.contains("WOBBLE"), "unhelpful error: {error}");
}

/// H3 parents, which need `h3o` — the reason core takes a coarsening closure
/// instead of owning the vocabulary.
#[test]
fn h3_cells_roll_up_to_their_parent() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_audit_events(true),
    )
    .unwrap();
    db.create_collection("places").unwrap();

    // Four r7 children of one r6 cell.
    let parent: h3o::CellIndex = h3o::LatLng::new(51.5, -0.12)
        .unwrap()
        .to_cell(h3o::Resolution::Six);
    let children: Vec<h3o::CellIndex> = parent.children(h3o::Resolution::Seven).take(4).collect();
    for (index, cell) in children.iter().enumerate() {
        db.insert(
            "places",
            Record::new(format!("p{index}"))
                .with_metadata(json!({ "cell": cell.to_string(), "score": 1.0 })),
        )
        .unwrap();
    }
    let mut projection = AggregateProjection::new(
        "places_h3",
        "places",
        vec!["cell".to_string()],
        vec!["score".to_string()],
    )
    .unwrap();
    projection.catch_up(&db).unwrap();
    let directory = db
        .data_path()
        .join(bicdb_core::aggregate_projection::PROJECTIONS_DIR);
    std::fs::create_dir_all(&directory).unwrap();
    projection.save(&directory, true).unwrap();

    let mut sql = SqlSession::new(&mut db);
    let result = sql
        .execute("ROLLUP MATERIALIZED AGGREGATE places_h3 TO (cell H3 6)")
        .unwrap();
    assert_eq!(result.rows.len(), 1, "the four children did not converge");
    assert_eq!(result.rows[0][0], SqlValue::String(parent.to_string()));
    assert_eq!(result.rows[0][1], SqlValue::Int(4));
}

// ---- G9: MERGE MATERIALIZED AGGREGATE -------------------------------------

/// Two shard projections over the same collection, merged. The shards share
/// hosts, so a distinct-count that added instead of merging would inflate.
#[test]
fn merge_combines_shard_projections() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_audit_events(true),
    )
    .unwrap();
    db.create_collection("north").unwrap();
    db.create_collection("south").unwrap();
    for index in 0..60 {
        let collection = if index % 2 == 0 { "north" } else { "south" };
        db.insert(
            collection,
            Record::new(format!("s{index}")).with_metadata(json!({
                "category": if index % 4 < 2 { "pizza" } else { "sushi" },
                // The two shards must SHARE hosts, or merging and adding
                // would coincide and the assertion below would prove nothing.
                "host": format!("host{}", (index / 2) % 5),
                "price": (index % 30) as f64,
            })),
        )
        .unwrap();
    }
    let directory = db
        .data_path()
        .join(bicdb_core::aggregate_projection::PROJECTIONS_DIR);
    std::fs::create_dir_all(&directory).unwrap();
    for shard in ["north", "south"] {
        let mut projection = AggregateProjection::new(
            format!("shard_{shard}"),
            shard,
            vec!["category".to_string()],
            vec!["price".to_string()],
        )
        .unwrap()
        .with_sketches(vec![SketchSpec {
            name: "hosts".to_string(),
            path: "host".to_string(),
            kind: SketchKind::DistinctCount,
        }])
        .unwrap();
        projection.rebuild_from_base(&db).unwrap();
        projection.save(&directory, true).unwrap();
    }

    let mut sql = SqlSession::new(&mut db);
    let result = sql
        .execute("MERGE MATERIALIZED AGGREGATE shard_north, shard_south")
        .unwrap();

    assert_eq!(result.rows.len(), 2, "expected one row per category");
    let total: i64 = result
        .rows
        .iter()
        .map(|row| match row[1] {
            SqlValue::Int(count) => count,
            _ => panic!("count is not an integer"),
        })
        .sum();
    assert_eq!(total, 60, "the merge lost or duplicated rows");

    // Each shard sees all 5 hosts in each category, so the merged answer is
    // 5. Adding the two shards' distinct-counts would give 10.
    let distinct = result
        .columns
        .iter()
        .position(|column| column == "distinct_hosts")
        .unwrap();
    for row in &result.rows {
        assert_eq!(
            row[distinct],
            SqlValue::Int(5),
            "distinct hosts were added rather than merged"
        );
    }
}

#[test]
fn merging_across_different_grains_is_refused() {
    let (_dir, mut db) = seeded();
    let directory = db
        .data_path()
        .join(bicdb_core::aggregate_projection::PROJECTIONS_DIR);
    let mut narrow = AggregateProjection::new(
        "sales_narrow",
        "sales",
        vec!["category".to_string()],
        vec!["price".to_string()],
    )
    .unwrap();
    narrow.rebuild_from_base(&db).unwrap();
    narrow.save(&directory, true).unwrap();

    let mut sql = SqlSession::new(&mut db);
    let error = sql
        .execute("MERGE MATERIALIZED AGGREGATE sales_daily, sales_narrow")
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("dimensions differ"),
        "unhelpful refusal: {error}"
    );
}
