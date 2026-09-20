//! G8 acceptance: a rolled-up projection must equal one built natively at the
//! coarse grain.
//!
//! The interesting half is not `SUM` — addition rolls up correctly by
//! accident, so a plain `GROUP BY` over the projection relation would pass.
//! It is `COUNT(DISTINCT)` and percentiles, where adding two cells is simply
//! **wrong**: two cells that share a host would double-count it. Those have
//! to merge their sketches, and that is the whole reason rollup is an engine
//! operation rather than a query.

use bicdb_core::aggregate_projection::{
    AggregateProjection, DimensionValue, RollupLevel, SketchKind, SketchSpec,
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

fn sketches() -> Vec<SketchSpec> {
    vec![
        SketchSpec {
            name: "hosts".to_string(),
            path: "host".to_string(),
            kind: SketchKind::DistinctCount,
        },
        SketchSpec {
            name: "price".to_string(),
            path: "price".to_string(),
            kind: SketchKind::Quantile,
        },
    ]
}

/// Grain: (day, category). `day` is `YYYY-MM-DD`, so its month is a prefix.
fn fine() -> AggregateProjection {
    AggregateProjection::new(
        "sales_fine",
        "sales",
        vec!["day".to_string(), "category".to_string()],
        vec!["price".to_string()],
    )
    .unwrap()
    .with_sketches(sketches())
    .unwrap()
}

/// The same data aggregated natively at (month, category) — the oracle.
fn coarse() -> AggregateProjection {
    AggregateProjection::new(
        "sales_coarse",
        "sales",
        vec!["month".to_string(), "category".to_string()],
        vec!["price".to_string()],
    )
    .unwrap()
    .with_sketches(sketches())
    .unwrap()
}

fn text(value: &str) -> DimensionValue {
    DimensionValue::Text(value.to_string())
}

fn seed(db: &mut BicDb) {
    db.create_collection("sales").unwrap();
    let categories = ["pizza", "sushi", "curry"];
    for index in 0..600 {
        let month = 1 + (index % 3);
        let day = 1 + (index % 27);
        let record = Record::new(format!("s{index}")).with_metadata(json!({
            "day": format!("2026-{month:02}-{day:02}"),
            "month": format!("2026-{month:02}"),
            "category": categories[index % 3],
            // Deliberately overlapping hosts ACROSS days within a month, so
            // summing distinct-counts would over-count and merging would not.
            "host": format!("host{}", index % 25),
            "price": (index % 97) as f64,
        }));
        db.insert("sales", record).unwrap();
    }
}

/// The headline property.
#[test]
fn rollup_equals_a_natively_coarse_projection() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = audited_db(&dir);
    seed(&mut db);

    let mut fine = fine();
    fine.catch_up(&db).unwrap();
    let mut coarse = coarse();
    coarse.catch_up(&db).unwrap();

    // `2026-01-15` -> `2026-01` is a 7-character prefix.
    let rolled = fine.rollup(&[RollupLevel::Prefix(7), RollupLevel::Keep]);

    assert_eq!(
        rolled.len(),
        coarse.cell_count(),
        "rollup produced a different number of cells than the coarse projection"
    );
    for (key, cell) in &rolled {
        let native = coarse
            .cell(key)
            .unwrap_or_else(|| panic!("coarse projection has no cell {key:?}"));
        assert_eq!(cell.state.count, native.count, "count differs at {key:?}");
        assert!(
            (cell.state.sum(0) - native.sum(0)).abs() < 1e-9,
            "sum differs at {key:?}"
        );
    }
}

/// The half a `GROUP BY` gets wrong. Summing distinct-counts across days
/// would report far more hosts than exist, because the days share hosts.
#[test]
fn rolled_up_sketches_merge_rather_than_add() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = audited_db(&dir);
    seed(&mut db);

    let mut fine = fine();
    fine.catch_up(&db).unwrap();
    let mut coarse = coarse();
    coarse.catch_up(&db).unwrap();

    let rolled = fine.rollup(&[RollupLevel::Prefix(7), RollupLevel::Keep]);

    for (key, cell) in &rolled {
        let native_distinct = coarse
            .cell_sketch(key, 0)
            .and_then(|state| state.distinct_estimate())
            .unwrap();
        let rolled_distinct = cell.sketches[0].distinct_estimate().unwrap();
        assert_eq!(
            rolled_distinct.round(),
            native_distinct.round(),
            "distinct-count at {key:?} does not match a natively coarse build"
        );

        // Bottom-k of a union IS the union of bottom-ks — the property G7b's
        // identity-keyed priorities bought — so percentiles match exactly.
        assert_eq!(
            cell.sketches[1].percentile(0.5),
            coarse.cell_sketch(key, 1).unwrap().percentile(0.5),
            "median at {key:?} does not match a natively coarse build"
        );
    }

    // And prove the naive answer would have been wrong: the number of rows
    // that went into a month exceeds the distinct hosts in it by a lot.
    let (key, cell) = rolled.iter().next().unwrap();
    assert!(
        cell.sketches[0].distinct_estimate().unwrap() < cell.state.count as f64,
        "the corpus does not actually exercise sharing at {key:?}"
    );
}

/// Dropping a dimension shortens the key and merges everything under it.
#[test]
fn whole_drops_a_dimension() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = audited_db(&dir);
    seed(&mut db);
    let mut fine = fine();
    fine.catch_up(&db).unwrap();

    let rolled = fine.rollup(&[RollupLevel::Whole, RollupLevel::Keep]);
    assert_eq!(rolled.len(), 3, "expected one cell per category");
    for (key, _) in &rolled {
        assert_eq!(key.len(), 1, "the dropped dimension is still in the key");
    }
    let total: i64 = rolled.values().map(|cell| cell.state.count).sum();
    assert_eq!(total, 600);

    // Collapsing everything gives the grand total.
    let grand = fine.rollup(&[RollupLevel::Whole, RollupLevel::Whole]);
    assert_eq!(grand.len(), 1);
    assert_eq!(grand.values().next().unwrap().state.count, 600);
}

/// Segment and bucket levels, on the shapes they exist for.
#[test]
fn segment_and_bucket_levels_coarsen_correctly() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = audited_db(&dir);
    db.create_collection("sales").unwrap();
    for index in 0..40 {
        db.insert(
            "sales",
            Record::new(format!("s{index}")).with_metadata(json!({
                "path": format!("food/pizza/shop{index}"),
                "size": index as i64,
                "price": 1.0,
            })),
        )
        .unwrap();
    }
    let mut projection = AggregateProjection::new(
        "paths",
        "sales",
        vec!["path".to_string(), "size".to_string()],
        vec!["price".to_string()],
    )
    .unwrap();
    projection.catch_up(&db).unwrap();

    let rolled = projection.rollup(&[
        RollupLevel::Segment {
            separator: '/',
            depth: 2,
        },
        RollupLevel::Bucket(10),
    ]);
    // 40 rows collapse to one path prefix crossed with four size buckets.
    assert_eq!(rolled.len(), 4);
    for (key, cell) in &rolled {
        assert_eq!(key[0], text("food/pizza"));
        assert_eq!(cell.state.count, 10);
        let DimensionValue::Int(bucket) = key[1] else {
            panic!("size bucket is not an integer");
        };
        assert_eq!(bucket % 10, 0);
    }
}

/// A rollup taken after churn must still equal the coarse oracle — the point
/// being that rollup reads maintained state, so any drift in that state shows
/// up here too.
#[test]
fn rollup_survives_churn() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = audited_db(&dir);
    seed(&mut db);
    let mut fine = fine();
    fine.catch_up(&db).unwrap();

    for index in (0..600).step_by(11) {
        if index % 3 == 0 {
            db.delete("sales", &format!("s{index}")).ok();
        } else {
            let month = 1 + ((index + 1) % 3);
            db.insert(
                "sales",
                Record::new(format!("s{index}")).with_metadata(json!({
                    "day": format!("2026-{month:02}-05"),
                    "month": format!("2026-{month:02}"),
                    "category": "pizza",
                    "host": format!("host{}", index % 25),
                    "price": 3.0,
                })),
            )
            .ok();
        }
    }
    fine.catch_up(&db).unwrap();
    fine.verify_durable_invariants(&db).unwrap();

    let mut coarse = coarse();
    coarse.rebuild_from_base(&db).unwrap();
    let rolled = fine.rollup(&[RollupLevel::Prefix(7), RollupLevel::Keep]);

    assert_eq!(rolled.len(), coarse.cell_count());
    for (key, cell) in &rolled {
        let native = coarse.cell(key).unwrap();
        assert_eq!(cell.state.count, native.count, "count differs at {key:?}");
        assert_eq!(
            cell.sketches[0].distinct_estimate().unwrap().round(),
            coarse
                .cell_sketch(key, 0)
                .unwrap()
                .distinct_estimate()
                .unwrap()
                .round(),
            "distinct-count differs at {key:?} after churn"
        );
    }
}
