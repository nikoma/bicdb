//! One isolated test process: the cancellation trigger observes native posting
//! work, not a timer racing the query's initial admission check.
use bicdb_core::{
    full_text_query_instrumentation, BicDb, BicDbError, Bm25Parameters, CancellationToken,
    DbConfig, IndexDefinition, IndexField, IndexKind, Record, StorageMode,
};
use serde_json::json;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

#[test]
fn filtered_ranking_is_equivalent_and_running_scans_observe_cancellation() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        directory.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_sync_outbox(false)
            .with_storage_mode(StorageMode::ServerPaged)
            .with_fts_packed_segments(true),
    )
    .unwrap();
    db.create_collection("docs").unwrap();
    db.bulk_load_insert(
        "docs",
        (0..10_000)
            .map(|i| {
                Record::new(format!("d{i:05}")).with_metadata(
                    json!({"body": format!("yoga meditation health training sleep café uniq{i}")}),
                )
            })
            .collect::<Vec<_>>(),
    )
    .unwrap();
    db.create_index(IndexDefinition {
        name: "fts".into(),
        collection: "docs".into(),
        fields: vec![IndexField::MetadataPath(vec!["body".into()])],
        kind: IndexKind::FullText,
        unique: false,
        predicate: None,
        exclusion: None,
    })
    .unwrap();
    let parameters = Bm25Parameters::default();
    let active = CancellationToken::uncancelable();
    let filter = db
        .full_text_document_filter_from_primary_keys("fts", &["d00002".into(), "d00005".into()])
        .unwrap();
    // A shared owner supports concurrent independent read sessions. Every
    // thread must retain the same hits, scores and positions as serial reads.
    let expected = format!(
        "{:?}",
        db.full_text_bm25_top_k_filtered(
            "fts",
            &["yoga", "health"],
            parameters,
            20,
            true,
            Some(&filter)
        )
        .unwrap()
    );
    let barrier = std::sync::Barrier::new(4);
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let (db, filter, expected, barrier) = (&db, &filter, &expected, &barrier);
            scope.spawn(move || {
                let session = db.full_text_read_session("fts").unwrap();
                barrier.wait();
                for _ in 0..8 {
                    let hits = session
                        .block_max_bm25_and_top_k(&["yoga", "health"], parameters, 20, Some(filter))
                        .unwrap();
                    assert_eq!(&format!("{hits:?}"), expected);
                }
            });
        }
    });

    // Compare selective seeks against exhaustive results rather than against
    // another call to the same filtered path. Keep all hits so selection cannot
    // hide a missing document or change tie ordering.
    let sparse = db
        .full_text_document_filter_from_primary_keys("fts", &["d09000".into()])
        .unwrap();
    for terms in [vec!["yoga"], vec!["yoga", "health"], vec!["café", "health"]] {
        let all = db
            .full_text_bm25_top_k_filtered("fts", &terms, parameters, 10_000, true, None)
            .unwrap()
            .unwrap();
        let expected: Vec<_> = all
            .into_iter()
            .filter(|hit| hit.primary_key == "d09000")
            .collect();
        let selected = db
            .full_text_bm25_top_k_filtered("fts", &terms, parameters, 10_000, true, Some(&sparse))
            .unwrap()
            .unwrap();
        assert_eq!(format!("{selected:?}"), format!("{expected:?}"));
    }
    for terms in [
        vec!["yoga"],
        vec!["yoga", "meditation"],
        vec!["café", "health"],
    ] {
        for require_all in [true, false] {
            for filter in [None, Some(&filter)] {
                let legacy = db
                    .full_text_bm25_top_k_filtered(
                        "fts",
                        &terms,
                        parameters,
                        20,
                        require_all,
                        filter,
                    )
                    .unwrap();
                let cancellable = db
                    .full_text_bm25_top_k_filtered_cancellable(
                        "fts",
                        &terms,
                        parameters,
                        20,
                        require_all,
                        filter,
                        &active,
                    )
                    .unwrap();
                assert_eq!(format!("{legacy:?}"), format!("{cancellable:?}"));
            }
        }
    }
    let canceled = CancellationToken::uncancelable();
    canceled.cancel();
    assert!(matches!(
        db.full_text_bm25_top_k_filtered_cancellable(
            "fts",
            &["yoga"],
            parameters,
            20,
            true,
            None,
            &canceled
        ),
        Err(BicDbError::QueryCanceled)
    ));
    let expired = CancellationToken::new(Arc::new(AtomicBool::new(false)), Some(Instant::now()));
    let session = db
        .full_text_read_session("fts")
        .unwrap()
        .with_cancellation(expired);
    assert!(matches!(
        session.block_max_bm25_and_top_k(&["yoga"], parameters, 20, None),
        Err(BicDbError::QueryTimedOut)
    ));
    assert!(matches!(
        session.impact_ordered_bm25_and_top_k(&["yoga", "health"], parameters, 20, None),
        Err(BicDbError::QueryTimedOut)
    ));
    assert!(matches!(
        session.tail_merged_bm25_and_top_k(&["yoga", "health"], parameters, 20, None),
        Err(BicDbError::QueryTimedOut)
    ));
    drop(session);

    // OR uses the exhaustive fallback. Cancel only after native decoding has
    // started, and prove it didn't finish decoding the complete reference scan.
    let terms = ["yoga", "meditation", "health", "training", "sleep", "café"];
    let before = full_text_query_instrumentation().postings_decoded;
    db.full_text_bm25_top_k_filtered("fts", &terms, parameters, 10_000, false, None)
        .unwrap();
    let reference_decoded = full_text_query_instrumentation().postings_decoded - before;
    assert!(reference_decoded > 10_000);
    let token = CancellationToken::uncancelable();
    let finished = AtomicBool::new(false);
    let before = full_text_query_instrumentation().postings_decoded;
    let (result, canceled_at) = std::thread::scope(|scope| {
        let canceler = scope.spawn(|| {
            let limit = Instant::now() + Duration::from_secs(5);
            while full_text_query_instrumentation().postings_decoded == before {
                assert!(
                    !finished.load(Ordering::Acquire),
                    "query finished before cancellation trigger"
                );
                assert!(Instant::now() < limit, "query never began decoding");
                std::thread::yield_now();
            }
            let at = Instant::now();
            token.cancel();
            at
        });
        let result = db.full_text_bm25_top_k_filtered_cancellable(
            "fts", &terms, parameters, 10_000, false, None, &token,
        );
        finished.store(true, Ordering::Release);
        (result, canceler.join().unwrap())
    });
    assert!(
        matches!(result, Err(BicDbError::QueryCanceled)),
        "{result:?}"
    );
    assert!(canceled_at.elapsed() < Duration::from_secs(1));
    let decoded = full_text_query_instrumentation().postings_decoded - before;
    eprintln!(
        "cancellation grace {:?}; decoded {decoded} versus {reference_decoded} uncanceled",
        canceled_at.elapsed()
    );
    assert!(
        decoded < reference_decoded,
        "canceled scan decoded {decoded}, reference {reference_decoded}"
    );
    // Cancellation is request-local and releases the session/worker resources.
    assert!(!db
        .full_text_bm25_top_k("fts", &["yoga"], parameters, 20, true)
        .unwrap()
        .unwrap()
        .is_empty());
}
