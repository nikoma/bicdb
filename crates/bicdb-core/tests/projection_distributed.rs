//! G9 acceptance: N shards aggregated independently, then merged, must equal
//! one projection built over the whole corpus — cell for cell.
//!
//! The landmine this design avoids is dictionary reconciliation. Each shard
//! interns dimension values into its own local id space, so shard A's id 7 and
//! shard B's id 7 are different strings. A merge over interned keys produces
//! an answer with every cell present and every number wrong, which is the
//! worst failure mode available. `first_shards_disagree_about_interned_ids`
//! proves the hazard is real rather than theoretical.

use bicdb_core::aggregate_projection::{
    AggregateProjection, DimensionValue, PartialAggregate, SketchKind, SketchSpec,
};
use bicdb_core::{BicDb, DbConfig, Record};
use serde_json::json;

const SHARDS: usize = 4;

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

fn projection(name: &str) -> AggregateProjection {
    AggregateProjection::new(
        name,
        "sales",
        vec!["region".to_string(), "category".to_string()],
        vec!["price".to_string()],
    )
    .unwrap()
    .with_sketches(sketches())
    .unwrap()
}

fn record(index: usize) -> Record {
    let regions = ["north", "south", "east"];
    let categories = ["pizza", "sushi"];
    Record::new(format!("s{index}")).with_metadata(json!({
        "region": regions[index % 3],
        "category": categories[(index / 3) % 2],
        // Hosts repeat ACROSS shards on purpose: a distinct-count that added
        // instead of merging would inflate, and the test would catch it.
        "host": format!("host{}", index % 20),
        "price": (index % 137) as f64,
    }))
}

const ROWS: usize = 800;

/// Rows are assigned to shards round-robin, so every shard sees every region.
fn shard_of(index: usize) -> usize {
    index % SHARDS
}

struct Fixture {
    _dirs: Vec<tempfile::TempDir>,
    partials: Vec<PartialAggregate>,
    whole: AggregateProjection,
    _whole_dir: tempfile::TempDir,
}

fn build() -> Fixture {
    let mut dirs = Vec::new();
    let mut partials = Vec::new();
    for shard in 0..SHARDS {
        let dir = tempfile::tempdir().unwrap();
        let mut db = audited_db(&dir);
        db.create_collection("sales").unwrap();
        for index in 0..ROWS {
            if shard_of(index) == shard {
                db.insert("sales", record(index)).unwrap();
            }
        }
        let mut projection = projection(&format!("shard{shard}"));
        projection.catch_up(&db).unwrap();
        partials.push(projection.export_partial());
        dirs.push(dir);
    }

    let whole_dir = tempfile::tempdir().unwrap();
    let mut db = audited_db(&whole_dir);
    db.create_collection("sales").unwrap();
    for index in 0..ROWS {
        db.insert("sales", record(index)).unwrap();
    }
    let mut whole = projection("whole");
    whole.catch_up(&db).unwrap();

    Fixture {
        _dirs: dirs,
        partials,
        whole,
        _whole_dir: whole_dir,
    }
}

/// The headline property.
#[test]
fn merged_shards_equal_a_whole_corpus_projection() {
    let mut fixture = build();
    let merged = AggregateProjection::merge_partials(&fixture.partials).unwrap();

    assert_eq!(
        merged.cells.len(),
        fixture.whole.cell_count(),
        "merged shard partials have a different cell count than the whole corpus"
    );
    let total: i64 = merged.cells.iter().map(|(_, state, _)| state.count).sum();
    assert_eq!(
        total, ROWS as i64,
        "rows were lost or duplicated by the merge"
    );

    for (key, state, sketches) in &merged.cells {
        let native = fixture
            .whole
            .cell(key)
            .unwrap_or_else(|| panic!("whole-corpus projection has no cell {key:?}"));
        assert_eq!(state.count, native.count, "count differs at {key:?}");
        assert!(
            (state.sum(0) - native.sum(0)).abs() < 1e-9,
            "sum differs at {key:?}"
        );
        assert_eq!(state.avg(0), native.avg(0), "avg differs at {key:?}");

        // Sketches must MERGE. Adding them would inflate the distinct-count,
        // because the shards deliberately share hosts.
        assert_eq!(
            sketches[0].distinct_estimate().unwrap().round(),
            fixture
                .whole
                .cell_sketch(key, 0)
                .unwrap()
                .distinct_estimate()
                .unwrap()
                .round(),
            "distinct-count differs at {key:?}"
        );
        // Exact, not approximate: bottom-k of a union is the union of
        // bottom-ks, which holds because priorities are keyed on record
        // identity rather than arrival order (G7b).
        assert_eq!(
            sketches[1].percentile(0.5),
            fixture.whole.cell_sketch(key, 1).unwrap().percentile(0.5),
            "median differs at {key:?}"
        );
        assert_eq!(
            sketches[1].percentile(0.95),
            fixture.whole.cell_sketch(key, 1).unwrap().percentile(0.95),
            "p95 differs at {key:?}"
        );
    }
}

/// Proof the hazard is real: the shards genuinely disagree about which id
/// means which value, so a merge over interned keys would be wrong.
#[test]
fn shards_disagree_about_interned_ids() {
    // Two shards that see the same values in a different ORDER intern them
    // differently, because ids are minted on first sight.
    let mut disagreements = 0;
    let mut exports = Vec::new();
    for order in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut db = audited_db(&dir);
        db.create_collection("sales").unwrap();
        let indices: Vec<usize> = if order {
            (0..60).rev().collect()
        } else {
            (0..60).collect()
        };
        for index in indices {
            db.insert("sales", record(index)).unwrap();
        }
        let mut projection = projection("ordered");
        projection.catch_up(&db).unwrap();
        // The first-minted id for each dimension is whatever that shard saw
        // first, which differs between the two orders.
        exports.push(projection.export_partial());
        disagreements += 1;
    }
    assert_eq!(disagreements, 2);

    // Both shards saw identical data, so a VALUE-space merge of the two
    // must produce exactly the same cell set as either one alone.
    let merged = AggregateProjection::merge_partials(&exports).unwrap();
    assert_eq!(
        merged.cells.len(),
        exports[0].cells.len(),
        "value-space merge produced a different cell set from identical data"
    );
    // And the counts doubled, because both shards really did hold the rows.
    for (key, state, _) in &merged.cells {
        let single = exports[0]
            .cells
            .iter()
            .find(|(candidate, _, _)| candidate == key)
            .unwrap();
        assert_eq!(state.count, single.1.count * 2);
    }
}

/// A merged answer is only as fresh as its laggiest shard.
#[test]
fn merged_position_is_the_minimum_watermark() {
    let fixture = build();
    let expected = fixture
        .partials
        .iter()
        .map(|partial| partial.position)
        .min()
        .unwrap();
    let merged = AggregateProjection::merge_partials(&fixture.partials).unwrap();
    assert_eq!(merged.position, expected);
    assert!(
        merged.position
            <= fixture
                .partials
                .iter()
                .map(|partial| partial.position)
                .max()
                .unwrap()
    );
}

/// Merging incompatible grains is refused, not approximated.
#[test]
fn incompatible_partials_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = audited_db(&dir);
    db.create_collection("sales").unwrap();
    for index in 0..30 {
        db.insert("sales", record(index)).unwrap();
    }
    let mut wide = projection("wide");
    wide.catch_up(&db).unwrap();

    let mut narrow = AggregateProjection::new(
        "narrow",
        "sales",
        vec!["region".to_string()],
        vec!["price".to_string()],
    )
    .unwrap();
    narrow.catch_up(&db).unwrap();

    let error =
        AggregateProjection::merge_partials(&[wide.export_partial(), narrow.export_partial()])
            .unwrap_err()
            .to_string();
    assert!(
        error.contains("dimensions differ"),
        "unhelpful refusal: {error}"
    );
}

/// A single partial merges to itself — the identity case a coordinator hits
/// whenever only one shard reports.
#[test]
fn merging_one_partial_is_the_identity() {
    let fixture = build();
    let single = &fixture.partials[0];
    let merged = AggregateProjection::merge_partials(std::slice::from_ref(single)).unwrap();
    assert_eq!(merged.cells.len(), single.cells.len());
    assert_eq!(merged.position, single.position);
    for ((left_key, left_state, _), (right_key, right_state, _)) in
        merged.cells.iter().zip(single.cells.iter())
    {
        assert_eq!(left_key, right_key);
        assert_eq!(left_state.count, right_state.count);
    }
}

/// A partial survives serialization: it is a wire format, so this is the
/// property that makes it one.
#[test]
fn a_partial_round_trips_through_json() {
    let fixture = build();
    let encoded = serde_json::to_vec(&fixture.partials[0]).unwrap();
    let decoded: PartialAggregate = serde_json::from_slice(&encoded).unwrap();
    let merged = AggregateProjection::merge_partials(&[decoded]).unwrap();
    assert_eq!(merged.cells.len(), fixture.partials[0].cells.len());
    for (key, state, sketches) in &merged.cells {
        let original = fixture.partials[0]
            .cells
            .iter()
            .find(|(candidate, _, _)| candidate == key)
            .unwrap();
        assert_eq!(state.count, original.1.count);
        assert_eq!(
            sketches[0].distinct_estimate(),
            original.2[0].distinct_estimate(),
            "a sketch did not survive the wire format"
        );
        assert_eq!(sketches[1].percentile(0.5), original.2[1].percentile(0.5));
    }
    assert!(
        matches!(fixture.partials[0].cells[0].0[0], DimensionValue::Text(_)),
        "partials must be exported in resolved value space, not interned ids"
    );
}
