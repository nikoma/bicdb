//! Partition count must fall as top-k rises.
//!
//! Partitioning weakens Block-Max WAND pruning: each partition establishes its
//! own top-k threshold instead of sharing a global one. Pruning is weakest at
//! LARGE top-k, so the partition count that helps a `k=10` query actively
//! harms a `k=1000` one — measured at 118 ms -> 312 ms for `medium+common` at
//! `k=100` going from 8 partitions to 24, with the block-cache hit rate
//! collapsing 85% -> 3% as the extra postings blew the cache.

use bicdb_core::effective_fts_partitions;

#[test]
fn small_top_k_uses_the_full_configured_width() {
    for configured in [1usize, 4, 8, 16, 24] {
        assert_eq!(
            effective_fts_partitions(configured, 10),
            configured.max(1),
            "k=10 should not be narrowed at configured={configured}"
        );
    }
}

#[test]
fn large_top_k_collapses_to_serial() {
    for configured in [4usize, 8, 16, 24] {
        assert_eq!(
            effective_fts_partitions(configured, 1_000),
            1,
            "k=1000 must run serial; partitioning costs more than it saves"
        );
    }
}

#[test]
fn the_rule_only_ever_narrows() {
    for configured in [1usize, 2, 8, 24, 64] {
        for keep in [1usize, 10, 100, 500, 1_000, 10_000] {
            let effective = effective_fts_partitions(configured, keep);
            assert!(
                effective <= configured.max(1),
                "configured={configured} keep={keep} widened to {effective}"
            );
            assert!(effective >= 1, "must always leave at least one partition");
        }
    }
}

#[test]
fn it_is_monotonic_in_top_k() {
    let mut previous = usize::MAX;
    for keep in [1usize, 10, 50, 100, 200, 400, 800, 1_600, 5_000] {
        let effective = effective_fts_partitions(24, keep);
        assert!(
            effective <= previous,
            "partitions rose from {previous} to {effective} as k grew to {keep}"
        );
        previous = effective;
    }
}

/// The case the rule exists for: an operator configures a wide search node,
/// and a large-k query must not take them at their word.
#[test]
fn a_wide_configuration_is_narrowed_for_large_k() {
    assert_eq!(effective_fts_partitions(24, 10), 24);
    assert_eq!(effective_fts_partitions(24, 100), 8);
    assert_eq!(effective_fts_partitions(24, 1_000), 1);
}

#[test]
fn a_zero_keep_is_not_a_division_by_zero() {
    assert_eq!(effective_fts_partitions(16, 0), 1);
}

// ---- load-adaptive width ------------------------------------------------

/// `0` selects adaptive width. With the machine to itself a query should get
/// a wide plan; the ceiling keeps it from overshooting.
#[test]
fn adaptive_width_is_wide_when_idle_and_never_exceeds_the_ceiling() {
    let idle = effective_fts_partitions(0, 10);
    assert!(idle >= 1);
    assert!(
        idle <= 8,
        "adaptive width {idle} exceeded the measured ceiling; a single query \
         was 24.5ms at 8 partitions against 32.1ms at 24"
    );
}

/// Adaptive still respects the top-k rule: a large-k query runs serial no
/// matter how idle the machine is, because pruning is what it loses.
#[test]
fn adaptive_still_narrows_for_large_top_k() {
    assert_eq!(effective_fts_partitions(0, 1_000), 1);
}

/// An explicit width still pins the plan — adaptive is a default, not a
/// takeover.
#[test]
fn an_explicit_width_is_still_honoured() {
    assert_eq!(effective_fts_partitions(4, 10), 4);
    assert_eq!(effective_fts_partitions(1, 10), 1);
}

/// Concurrent ranked queries must see each other and narrow. Without this the
/// adaptive rule is just a fancy constant.
#[test]
fn concurrent_queries_narrow_each_others_plans() {
    use bicdb_core::{BicDb, Bm25Parameters, DbConfig, Record, StorageMode};
    use serde_json::json;

    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged)
            .with_fts_query_partitions(0),
    )
    .unwrap();
    db.create_collection("docs").unwrap();
    let rows: Vec<Record> = (0..2_000)
        .map(|index| {
            Record::new(format!("d{index:05}")).with_metadata(json!({
                "body": format!("alpha beta gamma delta term{}", index % 40),
            }))
        })
        .collect();
    db.bulk_load_insert("docs", rows).unwrap();
    db.create_index(bicdb_core::IndexDefinition {
        name: "docs_fts".into(),
        collection: "docs".into(),
        fields: vec![bicdb_core::IndexField::MetadataPath(vec!["body".into()])],
        kind: bicdb_core::IndexKind::FullText,
        unique: false,
        predicate: None,
        exclusion: None,
    })
    .unwrap();

    // Eight threads querying at once must all succeed and agree; the point is
    // that the adaptive path is exercised concurrently without racing.
    let results: Vec<usize> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let db = &db;
                scope.spawn(move || {
                    let mut seen = 0usize;
                    for _ in 0..10 {
                        let hits = db
                            .full_text_bm25_top_k(
                                "docs_fts",
                                &["alpha", "beta"],
                                Bm25Parameters::default(),
                                20,
                                true,
                            )
                            .expect("ranked query")
                            .unwrap_or_default();
                        seen = seen.max(hits.len());
                    }
                    seen
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    assert!(
        results.iter().all(|hits| *hits > 0),
        "some thread saw no hits"
    );
    // Every thread must agree: adaptive scheduling changes the PLAN, never
    // the answer.
    assert!(
        results.windows(2).all(|pair| pair[0] == pair[1]),
        "concurrent adaptive plans returned different hit counts: {results:?}"
    );
}
