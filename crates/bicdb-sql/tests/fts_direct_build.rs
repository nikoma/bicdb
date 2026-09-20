//! Direct-to-blocks index build (GC 2/3): CREATE INDEX over an existing
//! corpus writes v3/v5 posting blocks straight from a chunked pk-ordered
//! scan — no per-posting v1/v2 tail to fold and GC away afterwards.
//!
//! Contract under test:
//! - a direct-built index is BLOCKS-ONLY (empty tail, sentinel planted) and
//!   every query shape matches the tx-forced oracle after reopen;
//! - prefix (`term:*`) queries read the blocks — with an empty tail they
//!   would return nothing if the prefix path still scanned only v1 (the
//!   pre-0.9.58 bug this suite pins);
//! - post-create writes layer over the blocks and a fold re-absorbs them;
//! - DROP INDEX purges every block namespace, so a same-name re-create
//!   starts clean;
//! - an aborted backfill (crash window: blocks committed, no sentinel) must
//!   NOT serve read-through — the index falls back to the resident rebuild
//!   and stays correct until recreated.

use bicdb_core::{
    bm25_inverse_document_frequency, bm25_term_score, full_text_query_instrumentation, BicDb,
    Bm25Parameters, Bm25fParameters, DbConfig, IndexDefinition, IndexField, IndexKind, IndexValue,
    StorageMode, FTS_GENERATION_FORMAT_VERSION,
};
use bicdb_sql::{SqlSession, SqlValue};

const ROWS: usize = 900;

/// Env knobs are process-global; the tests that set them (and the tests
/// asserting exact instrumentation deltas) serialize here so a parallel
/// sibling can never observe a half-configured backfill. Poisoning is
/// deliberately forgiven: a panicking holder must produce ONE named test
/// failure, not a cascade that masks the root cause.
static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
}

/// Rows FIRST, index second — the direct-build trigger. Mixed term
/// frequencies, a shared broad term, and a couple of pre-index deletes and
/// updates so chain churn is part of the corpus.
fn seed_rows(sql: &mut SqlSession) {
    sql.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)")
        .unwrap();
    for chunk in (0..ROWS).collect::<Vec<_>>().chunks(150) {
        let values = chunk
            .iter()
            .map(|index| {
                let alphas = std::iter::repeat("ashwagandha")
                    .take(1 + index % 9)
                    .collect::<Vec<_>>()
                    .join(" pad ");
                format!(
                    "('k{index:05}', 'trial {index}: {alphas} anxiety phase {}')",
                    index % 5
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        sql.execute(&format!("INSERT INTO docs VALUES {values}"))
            .unwrap();
    }
    sql.execute("DELETE FROM docs WHERE id = 'k00007'").unwrap();
    sql.execute("UPDATE docs SET body = 'lone valerian trial' WHERE id = 'k00011'")
        .unwrap();
}

fn create_index(sql: &mut SqlSession) {
    sql.execute(
        "CREATE INDEX idx_docs_fts ON docs USING GIN (to_tsvector('english', COALESCE(body, '')))",
    )
    .unwrap();
}

const SHAPES: &[&str] = &[
    "SELECT count(*) FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha')",
    "SELECT count(*) FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwa:*')",
    "SELECT count(*) FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha & anxieti')",
    "SELECT count(*) FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'valerian | phase')",
    "SELECT count(*) FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'anxieti & !valerian')",
    "SELECT count(*) FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha <-> pad')",
    "SELECT id FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha & anxieti') \
     ORDER BY ts_rank(to_tsvector('english', COALESCE(body, '')), to_tsquery('english', 'ashwagandha & anxieti')) DESC LIMIT 7",
    "SELECT id FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ websearch_to_tsquery('english', 'ashwagandha anxiety') \
     ORDER BY ts_rank(to_tsvector('english', COALESCE(body, '')), websearch_to_tsquery('english', 'ashwagandha anxiety')) DESC LIMIT 7",
    "SELECT id FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha | valerian') \
     ORDER BY ts_rank(to_tsvector('english', COALESCE(body, '')), to_tsquery('english', 'ashwagandha | valerian')) DESC LIMIT 7",
    "SELECT id FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha') \
     ORDER BY ts_rank(to_tsvector('english', COALESCE(body, '')), to_tsquery('english', 'ashwagandha')) DESC, id LIMIT 7",
    "SELECT id FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwa:*') \
     ORDER BY ts_rank_cd(to_tsvector('english', COALESCE(body, '')), to_tsquery('english', 'ashwa:*')) DESC, id LIMIT 7",
];

/// Every shape, fast paths allowed vs tx-forced oracle (BEGIN disables the
/// streaming/index fast paths), compared row-for-row.
fn assert_oracle_parity(db: &mut BicDb) {
    let mut sql = SqlSession::new(db);
    for shape in SHAPES {
        let fast = sql.execute(shape).unwrap().rows;
        sql.execute("BEGIN").unwrap();
        let oracle = sql.execute(shape).unwrap().rows;
        sql.execute("COMMIT").unwrap();
        assert_eq!(fast, oracle, "shape diverged from oracle: {shape}");
    }
}

fn count(db: &mut BicDb, query: &str) -> i64 {
    let mut sql = SqlSession::new(db);
    match &sql.execute(query).unwrap().rows[0][0] {
        SqlValue::Int(value) => *value,
        other => panic!("expected integer count, got {other:?}"),
    }
}

#[test]
fn direct_build_is_blocks_only_and_matches_oracle_across_chunks() {
    let _env = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // Small chunks force the multi-chunk append path and the carry between
    // chunks; the tiny carry budget forces the freeze-partials path too.
    std::env::set_var("BICDB_FTS_BACKFILL_CHUNK_ROWS", "64");
    std::env::set_var("BICDB_FTS_BACKFILL_CARRY_BUDGET", "200");
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
        let mut sql = SqlSession::new(&mut db);
        seed_rows(&mut sql);
        create_index(&mut sql);
        db.close().unwrap();
    }
    std::env::remove_var("BICDB_FTS_BACKFILL_CHUNK_ROWS");
    std::env::remove_var("BICDB_FTS_BACKFILL_CARRY_BUDGET");

    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let open_metrics = db.full_text_open_metrics();
    assert_eq!(open_metrics.index_count, 1);
    assert_eq!(
        open_metrics.metadata_probes, 3,
        "a populated generation needs only statistics/sentinel/map probes"
    );
    assert_eq!(
        open_metrics.fallback_row_ids_registered, 0,
        "a generation-format FTS index must reopen without a corpus walk"
    );
    let fts_entries = db
        .index_entry_diagnostics()
        .into_iter()
        .find(|(name, _, _)| name == "idx_docs_fts")
        .expect("FTS index diagnostic");
    assert_eq!(fts_entries.1, 0);
    assert_eq!(fts_entries.2, 0);
    let (tail, blocks, impact_blocks, sentinel) =
        db.full_text_index_layout("idx_docs_fts").unwrap();
    assert_eq!(tail, 0, "direct build must leave no per-posting tail");
    assert!(blocks > 0, "direct build wrote no posting blocks");
    assert!(impact_blocks > 0, "direct build wrote no impact blocks");
    assert!(
        sentinel,
        "direct build must plant the completeness sentinel"
    );

    let corpus = db
        .full_text_collection_statistics("idx_docs_fts")
        .unwrap()
        .expect("new direct build must publish collection statistics");
    assert_eq!(corpus.format_version, FTS_GENERATION_FORMAT_VERSION);
    assert_eq!(corpus.document_count as usize, ROWS - 1);
    assert_eq!(
        corpus.next_document_id, corpus.document_count,
        "dense document ids must occupy exactly 0..document_count"
    );
    assert!(corpus.average_document_length > 0.0);
    assert!(corpus.term_count > 0);
    assert!(corpus.field_total_lengths[0] > 0);

    let term = db
        .full_text_term_statistics("idx_docs_fts", "ashwagandha")
        .unwrap()
        .expect("term dictionary entry");
    assert_eq!(term.document_frequency as usize, ROWS - 2);
    assert!(term.collection_frequency >= term.document_frequency);
    assert!(term.posting_block_count > 0);
    assert!(term.impact_block_count > 0);
    assert!(term.posting_bytes > 0);
    assert!(term.impact_bytes > 0);
    assert!(term.maximum_contribution > 0.0);
    assert_eq!(term.first_doc_id, Some(0));
    assert!(
        term.last_doc_id
            .is_some_and(|id| id < corpus.document_count),
        "posting dictionary bounds must be dense internal document ids"
    );
    let bm25 = db
        .full_text_bm25_top_k(
            "idx_docs_fts",
            &["ashwagandha"],
            Bm25Parameters::default(),
            7,
            true,
        )
        .unwrap()
        .expect("BM25 path");
    let bm25f = db
        .full_text_bm25f_top_k(
            "idx_docs_fts",
            &["ashwagandha"],
            Bm25fParameters::default(),
            7,
            true,
        )
        .unwrap()
        .expect("BM25F path");
    assert_eq!(bm25.len(), 7);
    assert_eq!(bm25f.len(), 7);
    assert!(bm25.windows(2).all(|pair| pair[0].score >= pair[1].score));
    assert!(bm25f.windows(2).all(|pair| pair[0].score >= pair[1].score));

    // Multi-term BM25 uses the document-ordered block-max threshold path. Compare it to
    // an exhaustive numeric intersection and score every matching document
    // independently so early termination cannot hide a ranking error.
    let terms = ["ashwagandha", "anxieti"];
    let parameters = Bm25Parameters::default();
    let block_max_ranked = db
        .full_text_bm25_top_k("idx_docs_fts", &terms, parameters, 25, true)
        .unwrap()
        .expect("block-max BM25 path");
    let candidates = db
        .full_text_numeric_conjunctive_postings("idx_docs_fts", &terms, &[0, 1])
        .unwrap()
        .expect("numeric BM25 oracle");
    let idfs = terms.map(|term| {
        let statistics = db
            .full_text_term_statistics("idx_docs_fts", term)
            .unwrap()
            .unwrap();
        bm25_inverse_document_frequency(corpus.document_count, statistics.document_frequency)
    });
    let mut oracle = candidates
        .into_iter()
        .map(|candidate| {
            let score = candidate
                .term_positions
                .iter()
                .enumerate()
                .map(|(slot, positions)| {
                    bm25_term_score(
                        positions.as_ref().unwrap().len().max(1) as u32,
                        idfs[slot],
                        candidate.document_length,
                        corpus.average_document_length,
                        parameters,
                    )
                })
                .sum::<f32>();
            (candidate.document_id, candidate.primary_key, score)
        })
        .collect::<Vec<_>>();
    oracle.sort_unstable_by(|left, right| {
        right
            .2
            .total_cmp(&left.2)
            .then_with(|| left.0.cmp(&right.0))
    });
    oracle.truncate(25);
    assert_eq!(
        block_max_ranked
            .iter()
            .map(|posting| (
                posting.document_id,
                posting.primary_key.as_str(),
                posting.score.to_bits()
            ))
            .collect::<Vec<_>>(),
        oracle
            .iter()
            .map(|(document_id, primary_key, score)| (
                *document_id,
                primary_key.as_str(),
                score.to_bits()
            ))
            .collect::<Vec<_>>()
    );

    // Planner cardinality is one dictionary lookup, never a walk/count over
    // the term's hundreds (or production millions) of posting keys.
    let before = full_text_query_instrumentation();
    assert_eq!(
        db.full_text_term_count_capped("idx_docs_fts", "ashwagandha", ROWS)
            .unwrap(),
        Some(ROWS - 2)
    );
    let after = full_text_query_instrumentation();
    assert_eq!(
        after.posting_keys_counted_for_planning,
        before.posting_keys_counted_for_planning
    );
    assert!(after.dictionary_lookups > before.dictionary_lookups);

    // The prefix shape is the regression pin: with tail == 0 it can only be
    // served from blocks.
    let prefix_hits = count(
        &mut db,
        "SELECT count(*) FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwa:*')",
    );
    assert_eq!(prefix_hits as usize, ROWS - 2, "prefix over blocks");
    let intersection_before = full_text_query_instrumentation();
    let conjunctive = db
        .full_text_numeric_conjunctive_postings(
            "idx_docs_fts",
            &["anxieti", "ashwagandha"],
            &[0, 1],
        )
        .unwrap()
        .expect("numeric conjunctive path");
    assert!(!conjunctive.is_empty());
    assert_oracle_parity(&mut db);
    let intersection_after = full_text_query_instrumentation();
    assert!(
        intersection_after.document_id_intersections
            > intersection_before.document_id_intersections,
        "conjunctive ranked retrieval must use numeric document-id intersection"
    );
    assert!(
        intersection_after.wand_candidates_scored > intersection_before.wand_candidates_scored,
        "ranked disjunction must use multi-term Block-Max WAND"
    );

    let cache_before = full_text_query_instrumentation();
    db.full_text_read_session("idx_docs_fts")
        .unwrap()
        .block_max_wand_top_k(&["phase", "trial"], [0.1, 0.2, 0.4, 1.0], 7, None)
        .unwrap()
        .unwrap();
    let cache_warmed = full_text_query_instrumentation();
    db.full_text_read_session("idx_docs_fts")
        .unwrap()
        .block_max_wand_top_k(&["phase", "trial"], [0.1, 0.2, 0.4, 1.0], 7, None)
        .unwrap()
        .unwrap();
    let cache_hot = full_text_query_instrumentation();
    assert!(cache_warmed.block_cache_misses > cache_before.block_cache_misses);
    assert!(cache_warmed.prefetched_blocks > cache_before.prefetched_blocks);
    assert!(cache_hot.block_cache_hits > cache_warmed.block_cache_hits);

    // Read workers own independent page-store snapshots and can rank against
    // one shared BicDb without an application-wide database mutex.
    let concurrent = std::thread::scope(|scope| {
        (0..8)
            .map(|_| {
                scope.spawn(|| {
                    let session = db.full_text_read_session("idx_docs_fts").unwrap();
                    let xid = session.transaction_id();
                    let hits = session
                        .block_max_wand_top_k(
                            &["ashwagandha", "valerian"],
                            [0.1, 0.2, 0.4, 1.0],
                            7,
                            None,
                        )
                        .unwrap()
                        .unwrap()
                        .into_iter()
                        .map(|posting| posting.primary_key)
                        .collect::<Vec<_>>();
                    (xid, hits)
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert!(
        concurrent.windows(2).all(|pair| pair[0].1 == pair[1].1),
        "concurrent FTS snapshots must return the same ranked result"
    );
    let mut snapshot_transactions = concurrent
        .iter()
        .map(|(transaction, _)| *transaction)
        .collect::<Vec<_>>();
    snapshot_transactions.sort_unstable();
    snapshot_transactions.dedup();
    assert_eq!(
        snapshot_transactions.len(),
        concurrent.len(),
        "each FTS read session must own an independent snapshot transaction"
    );

    // A native secondary-index predicate is converted once into a dense
    // generation bitset and enforced inside scoring, before result
    // materialization or application-level candidate overfetching.
    db.create_index(IndexDefinition {
        name: "idx_docs_id_filter".to_string(),
        collection: "docs".to_string(),
        fields: vec![IndexField::Id],
        unique: false,
        kind: IndexKind::BTree,
        predicate: None,
        exclusion: None,
    })
    .unwrap();
    let filtered_sql =
        "SELECT id FROM docs \
         WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha | valerian') \
           AND id = 'k00008' \
         ORDER BY ts_rank(to_tsvector('english', COALESCE(body, '')), to_tsquery('english', 'ashwagandha | valerian')) DESC \
         LIMIT 7";
    let fast_filtered = SqlSession::new(&mut db).execute(filtered_sql).unwrap().rows;
    {
        let mut oracle = SqlSession::new(&mut db);
        oracle.execute("BEGIN").unwrap();
        let expected = oracle.execute(filtered_sql).unwrap().rows;
        oracle.execute("COMMIT").unwrap();
        assert_eq!(fast_filtered, expected);
    }
    assert_eq!(
        fast_filtered,
        vec![vec![SqlValue::String("k00008".to_string())]]
    );
    let filter = db
        .full_text_document_filter_from_index(
            "idx_docs_fts",
            "idx_docs_id_filter",
            &[IndexValue::String("k00008".to_string())],
        )
        .unwrap();
    assert_eq!(filter.cardinality(), 1);
    let filter_before = full_text_query_instrumentation();
    let filtered = db
        .full_text_bm25_top_k_filtered(
            "idx_docs_fts",
            &["ashwagandha"],
            Bm25Parameters::default(),
            7,
            true,
            Some(&filter),
        )
        .unwrap()
        .expect("filtered BM25 path");
    assert_eq!(
        filtered
            .iter()
            .map(|posting| posting.primary_key.as_str())
            .collect::<Vec<_>>(),
        vec!["k00008"]
    );
    assert!(
        full_text_query_instrumentation().filter_candidates_rejected
            > filter_before.filter_candidates_rejected,
        "native filter must reject candidates inside ranked retrieval"
    );
}

#[test]
fn post_create_writes_layer_over_blocks_and_fold_reabsorbs() {
    let _env = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
        let mut sql = SqlSession::new(&mut db);
        seed_rows(&mut sql);
        create_index(&mut sql);
        db.close().unwrap();
    }
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute(
            "INSERT INTO docs VALUES ('zz001', 'fresh ashwagandha ashwagandha ashwagandha powder')",
        )
        .unwrap();
        sql.execute("UPDATE docs SET body = 'anxiety only now' WHERE id = 'k00002'")
            .unwrap();
        sql.execute("DELETE FROM docs WHERE id = 'k00003'").unwrap();
    }
    let (tail, ..) = db.full_text_index_layout("idx_docs_fts").unwrap();
    assert!(tail > 0, "post-create writes must land in the tail");
    assert_oracle_parity(&mut db);

    // k00002 lost `ashwagandha`, k00003 is gone, zz001 gained it.
    let hits = count(
        &mut db,
        "SELECT count(*) FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha')",
    );
    assert_eq!(hits as usize, ROWS - 2 - 2 + 1);

    db.compact_full_text_index("idx_docs_fts").unwrap();
    let (tail, _, _, sentinel) = db.full_text_index_layout("idx_docs_fts").unwrap();
    assert_eq!(tail, 0, "fold must re-absorb the tail");
    assert!(sentinel);
    assert_oracle_parity(&mut db);
    let hits_after_fold = count(
        &mut db,
        "SELECT count(*) FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha')",
    );
    assert_eq!(hits_after_fold, hits);
}

#[test]
fn drop_index_purges_every_block_namespace() {
    let _env = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    {
        let mut sql = SqlSession::new(&mut db);
        seed_rows(&mut sql);
        create_index(&mut sql);
    }
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute("DROP INDEX idx_docs_fts").unwrap();
    }
    let (tail, blocks, impact_blocks, sentinel) =
        db.full_text_index_layout("idx_docs_fts").unwrap();
    assert_eq!(
        (tail, blocks, impact_blocks, sentinel),
        (0, 0, 0, false),
        "DROP INDEX must clear v1/v2/v3/v4/v5 and the sentinel"
    );

    // Same-name re-create over the purged keyspace: counts must not double.
    {
        let mut sql = SqlSession::new(&mut db);
        create_index(&mut sql);
    }
    db.close().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    assert_oracle_parity(&mut db);
    let hits = count(
        &mut db,
        "SELECT count(*) FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha')",
    );
    assert_eq!(hits as usize, ROWS - 2);
}

#[test]
fn aborted_backfill_never_serves_partial_blocks() {
    let _env = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
        let mut sql = SqlSession::new(&mut db);
        seed_rows(&mut sql);
        drop(sql);
        // Small chunks so the first flush commits real blocks before the
        // injected abort fires — the widest possible crash window.
        // The abort knob matches by index name; a name unique to this test
        // keeps a parallel sibling's CREATE INDEX out of the blast radius.
        std::env::set_var("BICDB_FTS_BACKFILL_CHUNK_ROWS", "64");
        std::env::set_var("BICDB_TEST_FTS_BACKFILL_ABORT", "idx_docs_fts_abort");
        let mut sql = SqlSession::new(&mut db);
        let error = sql
            .execute(
                "CREATE INDEX idx_docs_fts_abort ON docs USING GIN (to_tsvector('english', COALESCE(body, '')))",
            )
            .unwrap_err();
        std::env::remove_var("BICDB_TEST_FTS_BACKFILL_ABORT");
        std::env::remove_var("BICDB_FTS_BACKFILL_CHUNK_ROWS");
        assert!(
            format!("{error}").contains("injected backfill abort"),
            "unexpected error: {error}"
        );
        db.close().unwrap();
    }

    // Reopen: partial blocks exist, no sentinel — the index must NOT serve
    // read-through from them.
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let (_, blocks, _, sentinel) = db.full_text_index_layout("idx_docs_fts_abort").unwrap();
    if std::env::var("BICDB_FTS_PACKED_SEGMENTS").as_deref() != Ok("0") {
        // Packed segments stream into unpublished files: an aborted build
        // leaves NOTHING visible, where the keyed format left committed
        // partial blocks the fallback then had to ignore. Strictly safer.
        assert_eq!(blocks, 0, "an aborted packed build must publish nothing");
    } else {
        assert!(blocks > 0, "the abort landed after a committed flush");
    }
    assert!(
        !sentinel,
        "the sentinel is only written by a COMPLETE build"
    );
    assert_oracle_parity(&mut db);
    let hits = count(
        &mut db,
        "SELECT count(*) FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha')",
    );
    assert_eq!(hits as usize, ROWS - 2, "resident fallback must stay exact");

    // The documented remedy: recreate. The rebuild must clear the stale
    // partial blocks before appending its own.
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute("DROP INDEX idx_docs_fts_abort").unwrap();
        create_index(&mut sql);
    }
    db.close().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let (tail, blocks, _, sentinel) = db.full_text_index_layout("idx_docs_fts").unwrap();
    assert_eq!(tail, 0);
    assert!(blocks > 0);
    assert!(sentinel);
    assert_oracle_parity(&mut db);
}

#[test]
fn interrupted_external_build_resumes_checkpoint_and_removes_partial_files() {
    let _env = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let tiny = DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_fts_build_memory_bytes(128 * 1024)
        .with_fts_build_workers(4);
    let mut db = BicDb::open_with_config(dir.path(), tiny.clone()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    seed_rows(&mut sql);

    // Crash #1: entering the merge — tokenization complete, every run on
    // disk, no merge work done.
    std::env::set_var("BICDB_FTS_BUILD_CRASH_AT", "mergepk");
    let error = sql
        .execute(
            "CREATE INDEX idx_docs_resume ON docs USING GIN \
             (to_tsvector('english', COALESCE(body, '')))",
        )
        .unwrap_err();
    std::env::remove_var("BICDB_FTS_BUILD_CRASH_AT");
    assert!(format!("{error}").contains("entering `mergepk`"));

    let builds = dir.path().join("fts-index-builds");
    let build_dir = std::fs::read_dir(&builds)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let completed_runs = std::fs::read_dir(&build_dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("run"))
        .count();
    assert!(completed_runs > 2, "tiny budget should spill several runs");
    std::fs::write(build_dir.join("torn-write.tmp"), b"partial").unwrap();

    // Abrupt process loss: do not close/checkpoint either handle. Reopen,
    // resume through the single-pass merge and crash AFTER it; the final
    // resume must go straight to atomic publication without re-tokenizing
    // or re-merging, and without application cleanup.
    drop(sql);
    drop(db);
    let mut db = BicDb::open_with_config(dir.path(), tiny.clone()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    std::env::set_var(
        "BICDB_TEST_FTS_BUILD_ABORT_PHASE",
        "idx_docs_resume:merge_pk",
    );
    let error = sql
        .execute(
            "CREATE INDEX idx_docs_resume ON docs USING GIN \
             (to_tsvector('english', COALESCE(body, '')))",
        )
        .unwrap_err();
    std::env::remove_var("BICDB_TEST_FTS_BUILD_ABORT_PHASE");
    assert!(format!("{error}").contains("after pk merge"));
    assert!(
        !build_dir.join("torn-write.tmp").exists(),
        "reopen must remove an uncheckpointed temporary run"
    );

    drop(sql);
    drop(db);
    let mut db = BicDb::open_with_config(dir.path(), tiny).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE INDEX idx_docs_resume ON docs USING GIN \
         (to_tsvector('english', COALESCE(body, '')))",
    )
    .unwrap();
    assert!(
        !build_dir.exists(),
        "published build must reclaim checkpoint, runs, and torn tmp files"
    );
    drop(sql);

    let (tail, blocks, impact, sentinel) = db.full_text_index_layout("idx_docs_resume").unwrap();
    assert_eq!(tail, 0);
    assert!(blocks > 0 && impact > 0 && sentinel);
    assert_oracle_parity(&mut db);
}

#[test]
fn resumed_external_build_skips_pathological_term_in_next_batch() {
    let _env = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let tiny = DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_fts_build_memory_bytes(64 * 1024)
        .with_fts_build_workers(2);
    let mut db = BicDb::open_with_config(dir.path(), tiny.clone()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE resume_terms (id TEXT PRIMARY KEY, body TEXT)")
        .unwrap();
    let pathological = "A".repeat(3_440);
    for start in (0..130).step_by(20) {
        let values = (start..(start + 20).min(130))
            .map(|index| {
                let body = if index == 64 {
                    format!("survivor {pathological}")
                } else {
                    "ordinary".to_string()
                };
                format!("('k{index:05}', '{body}')")
            })
            .collect::<Vec<_>>()
            .join(", ");
        sql.execute(&format!("INSERT INTO resume_terms VALUES {values}"))
            .unwrap();
    }

    const CREATE: &str = "CREATE INDEX idx_resume_terms ON resume_terms USING GIN \
        (to_tsvector('english', COALESCE(body, '')))";
    std::env::set_var("BICDB_TEST_FTS_BUILD_ABORT_AFTER_PK", "k00063");
    let first = sql.execute(CREATE);
    std::env::remove_var("BICDB_TEST_FTS_BUILD_ABORT_AFTER_PK");
    let error = first.unwrap_err();
    assert!(format!("{error}").contains("after pk `k00063`"));

    let build_dir = std::fs::read_dir(dir.path().join("fts-index-builds"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let checkpoint: serde_json::Value =
        serde_json::from_slice(&std::fs::read(build_dir.join("checkpoint.json")).unwrap()).unwrap();
    assert_eq!(checkpoint["last_pk"], "k00063");

    // Crash without close. Resume begins with k00064, whose 3,440-byte token
    // previously made every retry fail at the same checkpoint. It is now
    // skipped while the safe lexeme in that document remains searchable.
    drop(sql);
    drop(db);
    let mut db = BicDb::open_with_config(dir.path(), tiny).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(CREATE).unwrap();
    assert!(!build_dir.exists());
    drop(sql);
    assert_eq!(
        count(
            &mut db,
            "SELECT count(*) FROM resume_terms \
             WHERE to_tsvector('english', COALESCE(body, '')) \
             @@ to_tsquery('english', 'survivor')",
        ),
        1
    );
    assert_eq!(
        count(
            &mut db,
            "SELECT count(*) FROM resume_terms \
             WHERE to_tsvector('english', COALESCE(body, '')) \
             @@ to_tsquery('english', 'ordinari')",
        ),
        129
    );
}

#[test]
fn replacement_generation_does_not_hide_previous_valid_index() {
    let _env = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let durable = DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(StorageMode::ServerPaged);
    let mut db = BicDb::open_with_config(dir.path(), durable.clone()).unwrap();
    {
        let mut sql = SqlSession::new(&mut db);
        seed_rows(&mut sql);
        create_index(&mut sql);
    }
    let before = count(
        &mut db,
        "SELECT count(*) FROM docs WHERE to_tsvector('english', COALESCE(body, '')) \
         @@ to_tsquery('english', 'ashwagandha')",
    );

    db.prepare_full_text_build("idx_docs_fts", "docs", "replacement-test-v1")
        .unwrap();
    let blob = BicDb::encode_fts_doc_terms(1, 1, &[("replacement_only".to_string(), vec![1])]);
    db.append_full_text_build_batch("idx_docs_fts", &[("replacement-row".to_string(), blob)])
        .unwrap();
    db.finish_full_text_tokenization("idx_docs_fts").unwrap();
    std::env::set_var("BICDB_TEST_FTS_BUILD_ABORT_PHASE", "idx_docs_fts:merge_pk");
    let error = db
        .complete_prepared_full_text_build("idx_docs_fts")
        .unwrap_err();
    std::env::remove_var("BICDB_TEST_FTS_BUILD_ABORT_PHASE");
    assert!(format!("{error}").contains("after pk merge"));

    let during = count(
        &mut db,
        "SELECT count(*) FROM docs WHERE to_tsvector('english', COALESCE(body, '')) \
         @@ to_tsquery('english', 'ashwagandha')",
    );
    assert_eq!(
        during, before,
        "an unpublished replacement displaced the valid generation"
    );

    // Lose the process after the failed replacement merge. Reopen must still
    // resolve the logical name to the previous atomically published
    // generation; the unpublished physical generation remains discardable.
    drop(db);
    let mut db = BicDb::open_with_config(dir.path(), durable).unwrap();
    let after_restart = count(
        &mut db,
        "SELECT count(*) FROM docs WHERE to_tsvector('english', COALESCE(body, '')) \
         @@ to_tsquery('english', 'ashwagandha')",
    );
    assert_eq!(after_restart, before);
    assert!(db.discard_full_text_build("idx_docs_fts").unwrap());
    let after_discard = count(
        &mut db,
        "SELECT count(*) FROM docs WHERE to_tsvector('english', COALESCE(body, '')) \
         @@ to_tsquery('english', 'ashwagandha')",
    );
    assert_eq!(after_discard, before);
}

/// 0.9.63: bulk CREATE INDEX writes doc-terms blobs into the index keyspace
/// instead of rewriting every row with a JSON lexeme projection. Rows stay
/// lean; UPDATE/DELETE diffs resolve from the blob; a rewritten row becomes
/// authoritative again and retires its blob.
#[test]
fn bulk_create_leaves_rows_lean_and_diffs_from_blobs() {
    let _env = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
        let mut sql = SqlSession::new(&mut db);
        seed_rows(&mut sql);
        create_index(&mut sql);
        db.close().unwrap();
    }
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    // Rows must NOT carry the projection key.
    let record = db.get("docs", "k00004").unwrap().expect("row exists");
    assert!(
        record
            .metadata
            .as_object()
            .is_none_or(|map| !map.keys().any(|key| key.starts_with("$bicdb_fts"))),
        "bulk-indexed row still carries an in-row projection"
    );
    let blobs = db.full_text_doc_terms_count("idx_docs_fts").unwrap();
    assert_eq!(blobs, ROWS - 1, "one blob per surviving row");

    // UPDATE a bulk row: old postings must come from its blob (the term
    // disappears from the index), the row becomes authoritative, the blob
    // is retired.
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute("UPDATE docs SET body = 'no herbs at all' WHERE id = 'k00004'")
            .unwrap();
    }
    let hits = count(
        &mut db,
        "SELECT count(*) FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha')",
    );
    assert_eq!(
        hits as usize,
        ROWS - 3,
        "stale blob postings survived the update"
    );
    assert_eq!(
        db.full_text_doc_terms_count("idx_docs_fts").unwrap(),
        ROWS - 2,
        "the rewritten row must retire its blob"
    );
    let record = db.get("docs", "k00004").unwrap().expect("row exists");
    assert!(
        record
            .metadata
            .as_object()
            .is_some_and(|map| map.keys().any(|key| key.starts_with("$bicdb_fts"))),
        "post-create writes carry the in-row projection"
    );

    // DELETE a bulk row: postings and blob both go.
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute("DELETE FROM docs WHERE id = 'k00008'").unwrap();
    }
    let hits = count(
        &mut db,
        "SELECT count(*) FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha')",
    );
    assert_eq!(hits as usize, ROWS - 4);
    assert_eq!(
        db.full_text_doc_terms_count("idx_docs_fts").unwrap(),
        ROWS - 3
    );
    assert_oracle_parity(&mut db);

    // Fold re-absorbs the tail the two writes created; parity holds across
    // a reopen.
    db.compact_full_text_index("idx_docs_fts").unwrap();
    assert_oracle_parity(&mut db);
    db.close().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    assert_oracle_parity(&mut db);
}

/// Block-max WAND boundary (0.9.65): the u16 impact bucket is coarse enough
/// that tf=90 and tf=91 share a bucket while ranking differently. The
/// tf=91 winner is given the LARGEST pk so it lands in the bucket's LAST
/// block — a gate that over-skips (or terminates) on the bucket bound alone
/// drops the true top hit; the exact per-block max rank must keep that
/// block scanned.
#[test]
fn block_max_gate_scans_the_within_bucket_winner() {
    let _env = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // Find a REAL bucket collision under the exact f32 bucket function:
    // adjacent tf classes whose ranks differ but whose u16 impact buckets
    // tie. (Positions carry D weight; only the count matters for rank.)
    let bucket_of = |tf: usize| {
        let packed: Vec<u16> = (0..tf).map(|position| position as u16).collect();
        bicdb_core::fts_impact_bucket(&packed)
    };
    let (low_tf, high_tf) = (60..400)
        .map(|tf| (tf, tf + 1))
        .find(|(low, high)| bucket_of(*low) == bucket_of(*high))
        .expect("some adjacent tf classes must share a bucket");
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE tfdocs (id TEXT PRIMARY KEY, body TEXT)")
            .unwrap();
        let tf90 = std::iter::repeat("guggul")
            .take(low_tf)
            .collect::<Vec<_>>()
            .join(" ");
        let tf91 = std::iter::repeat("guggul")
            .take(high_tf)
            .collect::<Vec<_>>()
            .join(" ");
        for chunk in (0..130).collect::<Vec<_>>().chunks(65) {
            let values = chunk
                .iter()
                .map(|index| format!("('a{index:04}', '{tf90}')"))
                .collect::<Vec<_>>()
                .join(", ");
            sql.execute(&format!("INSERT INTO tfdocs VALUES {values}"))
                .unwrap();
        }
        sql.execute(&format!("INSERT INTO tfdocs VALUES ('zzwinner', '{tf91}')"))
            .unwrap();
        sql.execute(
            "CREATE INDEX idx_tfdocs_fts ON tfdocs USING GIN (to_tsvector('english', COALESCE(body, '')))",
        )
        .unwrap();
        db.close().unwrap();
    }
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    // Single ORDER BY key: the ranked fast path declines multi-key sorts,
    // and this test exists to exercise exactly that path.
    const QUERY: &str = "SELECT id FROM tfdocs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'guggul') \
         ORDER BY ts_rank(to_tsvector('english', COALESCE(body, '')), to_tsquery('english', 'guggul')) DESC LIMIT 5";
    let fast = sql.execute(QUERY).unwrap().rows;
    assert_eq!(
        fast[0][0],
        SqlValue::String("zzwinner".to_string()),
        "the within-bucket winner in the last block must rank first"
    );
    // Ties among the tf-90 rows may legitimately order differently across
    // paths; the winner and the member set must agree.
    let ids = |rows: &[Vec<SqlValue>]| {
        let mut ids: Vec<String> = rows
            .iter()
            .map(|row| match &row[0] {
                SqlValue::String(id) => id.clone(),
                other => panic!("expected id string, got {other:?}"),
            })
            .collect();
        ids.sort();
        ids
    };
    sql.execute("BEGIN").unwrap();
    let oracle = sql.execute(QUERY).unwrap().rows;
    sql.execute("COMMIT").unwrap();
    assert_eq!(
        oracle[0][0],
        SqlValue::String("zzwinner".to_string()),
        "oracle sanity: tf=91 outranks tf=90"
    );
    assert_eq!(ids(&fast), ids(&oracle), "gate diverged from the oracle");
}

#[test]
fn block_max_bm25_stops_before_low_impact_tail() {
    let _env = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)")
        .unwrap();
    for chunk in (0..1_024).collect::<Vec<_>>().chunks(128) {
        let values = chunk
            .iter()
            .map(|document_id| {
                let repetitions = if *document_id < 128 { 20 } else { 1 };
                let body = std::iter::repeat("alpha beta")
                    .take(repetitions)
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("('k{document_id:05}', '{body}')")
            })
            .collect::<Vec<_>>()
            .join(", ");
        sql.execute(&format!("INSERT INTO docs VALUES {values}"))
            .unwrap();
    }
    create_index(&mut sql);
    drop(sql);

    let before = full_text_query_instrumentation();
    // The public convenience query prefers impact-order sidecars for this
    // sealed single-field corpus. Call the document-order implementation
    // directly so this regression continues to pin its shallow secondary
    // cursor rather than accidentally testing a different, faster plan.
    let ranked = db
        .full_text_read_session("idx_docs_fts")
        .unwrap()
        .block_max_bm25_and_top_k(&["alpha", "beta"], Bm25Parameters::default(), 100, None)
        .unwrap()
        .expect("block-max BM25 path");
    let after = full_text_query_instrumentation();
    assert_eq!(
        ranked
            .iter()
            .map(|posting| posting.primary_key.clone())
            .collect::<Vec<_>>(),
        (0..100)
            .map(|document_id| format!("k{document_id:05}"))
            .collect::<Vec<_>>()
    );
    assert!(
        after.block_max_candidates_pruned > before.block_max_candidates_pruned,
        "the low-impact tail must be retired by the unread BM25 ceiling"
    );
    assert!(
        after.posting_block_boundaries_read > before.posting_block_boundaries_read,
        "secondary BM25 terms must shallow-seek through key-only block boundaries"
    );
    assert!(
        after.posting_blocks_read - before.posting_blocks_read < 64,
        "top-100 retrieval must not walk the low-impact posting tail"
    );
}

#[test]
fn boolean_count_serves_from_posting_blocks() {
    // Instrumentation deltas are asserted exactly; serialize against the
    // sibling counter-asserting test.
    let _env = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)")
        .unwrap();
    for chunk in (0..1_024).collect::<Vec<i64>>().chunks(128) {
        let values = chunk
            .iter()
            .map(|document_id| {
                let body = match document_id {
                    0..=199 => "alpha beta gamma",
                    200..=599 => "alpha beta",
                    _ => "alpha gamma",
                };
                format!("('k{document_id:05}', '{body}')")
            })
            .collect::<Vec<_>>()
            .join(", ");
        sql.execute(&format!("INSERT INTO docs VALUES {values}"))
            .unwrap();
    }
    create_index(&mut sql);

    let count = |sql: &mut SqlSession, tsquery: &str| -> i64 {
        let result = sql
            .execute(&format!(
                "SELECT count(*) FROM docs WHERE to_tsvector('english', COALESCE(body, '')) \
                 @@ to_tsquery('english', '{tsquery}')"
            ))
            .unwrap();
        match result.rows[0][0] {
            SqlValue::Int(count) => count,
            ref other => panic!("count returned {other:?}"),
        }
    };

    // Conjunctions of plain lexemes come straight from posting blocks.
    let before = full_text_query_instrumentation();
    assert_eq!(count(&mut sql, "alpha & beta"), 600);
    assert_eq!(count(&mut sql, "beta & gamma"), 200);
    assert_eq!(count(&mut sql, "alpha & beta & gamma"), 200);
    assert_eq!(count(&mut sql, "gamma"), 624);
    assert_eq!(count(&mut sql, "delta"), 0);
    let after = full_text_query_instrumentation();
    assert_eq!(
        after.boolean_block_counts - before.boolean_block_counts,
        5,
        "every conjunctive count must be answered from posting blocks"
    );

    // OR trees decline the block path and stay correct on the row scan.
    let before = full_text_query_instrumentation();
    assert_eq!(count(&mut sql, "beta | gamma"), 1_024);
    let after = full_text_query_instrumentation();
    assert_eq!(
        after.boolean_block_counts, before.boolean_block_counts,
        "OR counts take the fallback scan"
    );

    // Phrase counts: candidates from the lexeme conjunction, distance
    // decided by the positions recheck.
    let phrase_count = |sql: &mut SqlSession, phrase: &str| -> i64 {
        let result = sql
            .execute(&format!(
                "SELECT count(*) FROM docs WHERE to_tsvector('english', COALESCE(body, '')) \
                 @@ phraseto_tsquery('english', '{phrase}')"
            ))
            .unwrap();
        match result.rows[0][0] {
            SqlValue::Int(count) => count,
            ref other => panic!("phrase count returned {other:?}"),
        }
    };
    let before = full_text_query_instrumentation();
    assert_eq!(phrase_count(&mut sql, "alpha beta"), 600);
    assert_eq!(phrase_count(&mut sql, "beta gamma"), 200);
    assert_eq!(phrase_count(&mut sql, "alpha gamma"), 424);
    let after = full_text_query_instrumentation();
    assert_eq!(
        after.conjunctive_block_scans - before.conjunctive_block_scans,
        3,
        "phrase counts must run the conjunctive position scan"
    );

    // A transactional tail makes blocks non-authoritative: the count must
    // fall back and include the fresh row.
    sql.execute("INSERT INTO docs VALUES ('k99999', 'alpha beta fresh')")
        .unwrap();
    let before = full_text_query_instrumentation();
    assert_eq!(count(&mut sql, "alpha & beta"), 601);
    assert_eq!(phrase_count(&mut sql, "alpha beta"), 601);
    let after = full_text_query_instrumentation();
    assert_eq!(
        after.boolean_block_counts, before.boolean_block_counts,
        "a tailed term must decline the block count"
    );
    assert_eq!(
        after.conjunctive_block_scans, before.conjunctive_block_scans,
        "a tailed term must decline the position scan"
    );
}

#[test]
fn unranked_where_select_serves_from_posting_blocks() {
    // Instrumentation deltas are asserted exactly; serialize against the
    // sibling counter-asserting test.
    let _env = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)")
        .unwrap();
    for chunk in (0..1_024).collect::<Vec<i64>>().chunks(128) {
        let values = chunk
            .iter()
            .map(|document_id| {
                let body = match document_id {
                    0..=199 => "alpha beta gamma",
                    200..=599 => "alpha beta",
                    _ => "alpha gamma",
                };
                format!("('k{document_id:05}', '{body}')")
            })
            .collect::<Vec<_>>()
            .join(", ");
        sql.execute(&format!("INSERT INTO docs VALUES {values}"))
            .unwrap();
    }
    create_index(&mut sql);

    let ids = |sql: &mut SqlSession, predicate: &str, tail: &str| -> Vec<String> {
        let result = sql
            .execute(&format!(
                "SELECT id FROM docs WHERE to_tsvector('english', COALESCE(body, '')) \
                 @@ {predicate}{tail}"
            ))
            .unwrap();
        result
            .rows
            .iter()
            .map(|row| match &row[0] {
                SqlValue::String(id) => id.clone(),
                other => panic!("id returned {other:?}"),
            })
            .collect()
    };

    let before = full_text_query_instrumentation();
    assert_eq!(
        ids(
            &mut sql,
            "to_tsquery('english', 'alpha & gamma')",
            " LIMIT 5"
        ),
        (0..5)
            .map(|document_id| format!("k{document_id:05}"))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        ids(
            &mut sql,
            "to_tsquery('english', 'alpha & gamma')",
            " LIMIT 3 OFFSET 2"
        ),
        (2..5)
            .map(|document_id| format!("k{document_id:05}"))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        ids(
            &mut sql,
            "phraseto_tsquery('english', 'beta gamma')",
            " LIMIT 3"
        ),
        (0..3)
            .map(|document_id| format!("k{document_id:05}"))
            .collect::<Vec<_>>()
    );
    let unbounded = ids(&mut sql, "to_tsquery('english', 'alpha & gamma')", "");
    assert_eq!(unbounded.len(), 624);
    assert_eq!(unbounded[0], "k00000");
    assert_eq!(unbounded[200], "k00600");
    assert_eq!(unbounded[623], "k01023");
    let after = full_text_query_instrumentation();
    assert_eq!(
        after.conjunctive_block_scans - before.conjunctive_block_scans,
        4,
        "unranked selects must run the conjunctive position scan"
    );

    // Projections beyond the key column resolve through the fetched rows.
    let result = sql
        .execute(
            "SELECT body, id FROM docs WHERE to_tsvector('english', COALESCE(body, '')) \
             @@ to_tsquery('english', 'beta & gamma') LIMIT 1",
        )
        .unwrap();
    assert_eq!(result.columns, vec!["body".to_string(), "id".to_string()]);
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::String("alpha beta gamma".to_string()),
            SqlValue::String("k00000".to_string()),
        ]]
    );

    // OR trees and ORDER BY decline to the ordinary path and stay correct.
    let before = full_text_query_instrumentation();
    assert_eq!(
        ids(
            &mut sql,
            "to_tsquery('english', 'beta | gamma')",
            " LIMIT 2"
        ),
        vec!["k00000".to_string(), "k00001".to_string()]
    );
    let ordered = sql
        .execute(
            "SELECT id FROM docs WHERE to_tsvector('english', COALESCE(body, '')) \
             @@ to_tsquery('english', 'beta & gamma') ORDER BY id DESC LIMIT 1",
        )
        .unwrap();
    assert_eq!(
        ordered.rows,
        vec![vec![SqlValue::String("k00199".to_string())]]
    );
    let after = full_text_query_instrumentation();
    assert_eq!(
        after.conjunctive_block_scans, before.conjunctive_block_scans,
        "OR and ORDER BY shapes take the ordinary path"
    );

    // A transactional tail declines the block path; the fresh row appears.
    sql.execute("INSERT INTO docs VALUES ('k99999', 'beta gamma fresh')")
        .unwrap();
    let before = full_text_query_instrumentation();
    let with_tail = ids(&mut sql, "to_tsquery('english', 'beta & gamma')", "");
    assert_eq!(with_tail.len(), 201);
    assert_eq!(with_tail.last().unwrap(), "k99999");
    let after = full_text_query_instrumentation();
    assert_eq!(
        after.conjunctive_block_scans, before.conjunctive_block_scans,
        "a tailed term must decline the select scan"
    );
}

#[test]
fn fts_route_report_names_the_serving_path_and_unfolded_terms() {
    let _env = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)")
        .unwrap();
    sql.execute("INSERT INTO docs VALUES ('k1', 'alpha beta'), ('k2', 'alpha beta gamma')")
        .unwrap();
    create_index(&mut sql);

    let route = |sql: &mut SqlSession| -> String {
        let result = sql
            .execute("SELECT bicdb_fts_route('idx_docs_fts', 'alpha beta')")
            .unwrap();
        match &result.rows[0][0] {
            SqlValue::String(report) => report.clone(),
            other => panic!("route returned {other:?}"),
        }
    };

    let clean = route(&mut sql);
    assert!(clean.contains("block-max seeking conjunction"), "{clean}");
    assert!(clean.contains("unfolded_tail=false"), "{clean}");
    assert!(clean.contains("EXHAUSTIVE accumulator"), "{clean}");

    // A post-index write leaves an unfolded tail on its terms; the report
    // must call out the decline and name the terms.
    sql.execute("INSERT INTO docs VALUES ('k3', 'alpha fresh')")
        .unwrap();
    let tailed = route(&mut sql);
    assert!(tailed.contains("TAIL-MERGED block path"), "{tailed}");
    assert!(
        tailed.contains("unfolded writes or tombstones on: alpha"),
        "{tailed}"
    );
    assert!(tailed.contains("fold"), "{tailed}");
}

#[test]
fn fts_build_status_is_available_over_sql() {
    let _env = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)")
        .unwrap();
    sql.execute("INSERT INTO docs VALUES ('k1', 'alpha beta')")
        .unwrap();
    create_index(&mut sql);

    let result = sql
        .execute("SELECT bicdb_fts_build_status('idx_docs_fts')")
        .unwrap();
    let SqlValue::Json(status) = &result.rows[0][0] else {
        panic!("FTS build status did not return JSON: {:?}", result.rows);
    };
    assert_eq!(status["state"], "published");
    assert_eq!(status["reason_code"], "published_generation_active");
    assert_eq!(status["serving"], true);
    assert!(status["published"]["physical_index"].is_string());
}

#[test]
fn fts_fold_over_sql_reabsorbs_tails_and_reenables_block_paths() {
    let _env = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)")
        .unwrap();
    sql.execute("INSERT INTO docs VALUES ('k1', 'alpha beta'), ('k2', 'alpha beta gamma')")
        .unwrap();
    create_index(&mut sql);
    sql.execute("INSERT INTO docs VALUES ('k3', 'alpha fresh')")
        .unwrap();

    let route = |sql: &mut SqlSession| -> String {
        match &sql
            .execute("SELECT bicdb_fts_route('idx_docs_fts', 'alpha beta')")
            .unwrap()
            .rows[0][0]
        {
            SqlValue::String(report) => report.clone(),
            other => panic!("route returned {other:?}"),
        }
    };
    assert!(route(&mut sql).contains("TAIL-MERGED block path"));

    let folded = match &sql
        .execute("SELECT bicdb_fts_fold('idx_docs_fts')")
        .unwrap()
        .rows[0][0]
    {
        SqlValue::String(summary) => summary.clone(),
        other => panic!("fold returned {other:?}"),
    };
    assert!(folded.starts_with("folded "), "{folded}");

    let after = route(&mut sql);
    assert!(
        after.contains("block-max seeking conjunction"),
        "fold must re-enable the block paths: {after}"
    );
    assert!(after.contains("unfolded_tail=false"), "{after}");

    // The folded index still answers correctly, including the fresh row.
    let result = sql
        .execute(
            "SELECT count(*) FROM docs WHERE to_tsvector('english', COALESCE(body, '')) \
             @@ to_tsquery('english', 'alpha')",
        )
        .unwrap();
    assert_eq!(result.rows, vec![vec![SqlValue::Int(3)]]);
}

#[test]
fn tail_merged_bm25_layers_inserts_updates_and_deletes_over_blocks() {
    let _env = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)")
        .unwrap();
    // Sealed corpus: tf('alpha') descends with the pk so block ranking is
    // deterministic: k00000 ranks first, then k00001, ...
    for chunk in (0..512).collect::<Vec<i64>>().chunks(128) {
        let values = chunk
            .iter()
            .map(|document_id| {
                let repeats = 40usize.saturating_sub((*document_id as usize) / 16);
                let body = std::iter::repeat("alpha beta")
                    .take(repeats.max(1))
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("('k{document_id:05}', '{body}')")
            })
            .collect::<Vec<_>>()
            .join(", ");
        sql.execute(&format!("INSERT INTO docs VALUES {values}"))
            .unwrap();
    }
    create_index(&mut sql);

    let top = |db: &BicDb| -> Vec<String> {
        db.full_text_bm25_top_k(
            "idx_docs_fts",
            &["alpha", "beta"],
            Bm25Parameters::default(),
            5,
            true,
        )
        .unwrap()
        .expect("bm25 conjunction must not decline")
        .into_iter()
        .map(|posting| posting.primary_key)
        .collect()
    };

    // Sealed baseline: the five highest-tf documents.
    drop(sql);
    assert_eq!(
        top(&db),
        vec!["k00000", "k00001", "k00002", "k00003", "k00004"]
    );

    // A fresh insert with the highest tf must take first place; an update
    // that collapses k00001's tf must evict it; a delete removes k00002.
    let mut sql = SqlSession::new(&mut db);
    let champion_body = std::iter::repeat("alpha beta")
        .take(60)
        .collect::<Vec<_>>()
        .join(" ");
    sql.execute(&format!(
        "INSERT INTO docs VALUES ('k90000', '{champion_body}')"
    ))
    .unwrap();
    sql.execute("UPDATE docs SET body = 'alpha beta' WHERE id = 'k00001'")
        .unwrap();
    sql.execute("DELETE FROM docs WHERE id = 'k00002'").unwrap();
    drop(sql);

    let before = full_text_query_instrumentation();
    assert_eq!(
        top(&db),
        vec!["k90000", "k00000", "k00003", "k00004", "k00005"]
    );
    let after = full_text_query_instrumentation();
    assert_eq!(
        after.tail_merged_bm25_queries - before.tail_merged_bm25_queries,
        1,
        "the write layer must be served by the tail-merged block path"
    );

    // Folding absorbs the layer; results and route stay identical.
    let mut sql = SqlSession::new(&mut db);
    sql.execute("SELECT bicdb_fts_fold('idx_docs_fts')")
        .unwrap();
    drop(sql);
    let before = full_text_query_instrumentation();
    assert_eq!(
        top(&db),
        vec!["k90000", "k00000", "k00003", "k00004", "k00005"]
    );
    let after = full_text_query_instrumentation();
    assert_eq!(
        after.tail_merged_bm25_queries, before.tail_merged_bm25_queries,
        "a folded index serves from the sealed block path again"
    );
}

#[test]
fn advance_transaction_floor_is_callable_and_durable_over_sql() {
    let _env = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)")
            .unwrap();
        sql.execute("INSERT INTO docs VALUES ('k1', 'alpha')")
            .unwrap();
        let report = match &sql
            .execute("SELECT bicdb_advance_transaction_floor(500000)")
            .unwrap()
            .rows[0][0]
        {
            SqlValue::String(report) => report.clone(),
            other => panic!("floor advance returned {other:?}"),
        };
        let frozen: u64 = report
            .split("frozen_xid=")
            .nth(1)
            .and_then(|tail| tail.split_whitespace().next())
            .and_then(|value| value.parse().ok())
            .unwrap_or_else(|| panic!("unparseable report: {report}"));
        assert!(frozen >= 500_000, "{report}");
        // The store keeps working normally above the new floor.
        sql.execute("INSERT INTO docs VALUES ('k2', 'beta')")
            .unwrap();
        let count = sql.execute("SELECT count(*) FROM docs").unwrap();
        assert_eq!(count.rows, vec![vec![SqlValue::Int(2)]]);
        drop(sql);
        db.close().unwrap();
    }
    // Durable across reopen: a second advance to a lower target reports the
    // already-advanced watermarks.
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    let report = match &sql
        .execute("SELECT bicdb_advance_transaction_floor(1000)")
        .unwrap()
        .rows[0][0]
    {
        SqlValue::String(report) => report.clone(),
        other => panic!("floor advance returned {other:?}"),
    };
    let frozen: u64 = report
        .split("frozen_xid=")
        .nth(1)
        .and_then(|tail| tail.split_whitespace().next())
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| panic!("unparseable report: {report}"));
    assert!(frozen >= 500_000, "reopen lost the floor: {report}");
    sql.execute("INSERT INTO docs VALUES ('k3', 'gamma')")
        .unwrap();
    let count = sql.execute("SELECT count(*) FROM docs").unwrap();
    assert_eq!(count.rows, vec![vec![SqlValue::Int(3)]]);
}

#[test]
fn single_term_bm25_serves_from_blocks_with_pruning_and_tail_merge() {
    let _env = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)")
        .unwrap();
    // Descending tf with the pk: block ranking is deterministic and the
    // low-tf tail is prunable once the top-k fills.
    for chunk in (0..2_048i64).collect::<Vec<_>>().chunks(128) {
        let values = chunk
            .iter()
            .map(|document_id| {
                let repeats = 64usize.saturating_sub((*document_id as usize) / 32).max(1);
                let body = std::iter::repeat("alpha filler")
                    .take(repeats)
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("('k{document_id:05}', '{body}')")
            })
            .collect::<Vec<_>>()
            .join(", ");
        sql.execute(&format!("INSERT INTO docs VALUES {values}"))
            .unwrap();
    }
    create_index(&mut sql);
    drop(sql);

    let top = |db: &BicDb| -> Vec<String> {
        db.full_text_bm25_top_k(
            "idx_docs_fts",
            &["alpha"],
            Bm25Parameters::default(),
            5,
            true,
        )
        .unwrap()
        .expect("single-term bm25 must serve")
        .into_iter()
        .map(|posting| posting.primary_key)
        .collect()
    };

    let before = full_text_query_instrumentation();
    assert_eq!(
        top(&db),
        vec!["k00000", "k00001", "k00002", "k00003", "k00004"]
    );
    let after = full_text_query_instrumentation();
    assert!(
        after.block_max_candidates_pruned > before.block_max_candidates_pruned,
        "the low-tf tail must be retired by block bounds"
    );
    assert_eq!(
        after.tail_merged_bm25_queries, before.tail_merged_bm25_queries,
        "a sealed index serves single terms from the plain block path"
    );

    // Fresh writes: the tail-merged layer serves single terms too.
    let mut sql = SqlSession::new(&mut db);
    let champion = std::iter::repeat("alpha filler")
        .take(90)
        .collect::<Vec<_>>()
        .join(" ");
    sql.execute(&format!("INSERT INTO docs VALUES ('k90000', '{champion}')"))
        .unwrap();
    drop(sql);
    let before = full_text_query_instrumentation();
    assert_eq!(
        top(&db),
        vec!["k90000", "k00000", "k00001", "k00002", "k00003"]
    );
    let after = full_text_query_instrumentation();
    assert_eq!(
        after.tail_merged_bm25_queries - before.tail_merged_bm25_queries,
        1,
        "a tailed single term must serve through the write-layer merge"
    );
}

#[test]
fn parallel_seeking_conjunction_matches_construction_ranking() {
    let _env = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)")
        .unwrap();
    // 80k documents put the rarest operand over the parallel floor (65,536).
    // Constant length; 'alpha' tf descends with the pk so the expected
    // ranking is exact by construction; 'beta' rides along everywhere.
    for chunk in (0..80_000i64).collect::<Vec<_>>().chunks(1_000) {
        let values = chunk
            .iter()
            .map(|document_id| {
                let repeats = 16usize
                    .saturating_sub((*document_id as usize) / 5_000)
                    .max(1);
                let alpha = std::iter::repeat("alpha")
                    .take(repeats)
                    .chain(std::iter::repeat("filler").take(16 - repeats))
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("('k{document_id:06}', '{alpha} beta')")
            })
            .collect::<Vec<_>>()
            .join(", ");
        sql.execute(&format!("INSERT INTO docs VALUES {values}"))
            .unwrap();
    }
    create_index(&mut sql);
    drop(sql);

    let ranked = db
        .full_text_bm25_top_k(
            "idx_docs_fts",
            &["alpha", "beta"],
            Bm25Parameters::default(),
            10,
            true,
        )
        .unwrap()
        .expect("broad conjunction must serve")
        .into_iter()
        .map(|posting| posting.primary_key)
        .collect::<Vec<_>>();
    // Highest tf = ids 0..4999 (tf 16); ties break by document order.
    assert_eq!(
        ranked,
        (0..10)
            .map(|document_id| format!("k{document_id:06}"))
            .collect::<Vec<_>>()
    );

    // Determinism across repeats (worker scheduling must not leak into
    // results).
    for _ in 0..3 {
        let again = db
            .full_text_bm25_top_k(
                "idx_docs_fts",
                &["alpha", "beta"],
                Bm25Parameters::default(),
                10,
                true,
            )
            .unwrap()
            .unwrap()
            .into_iter()
            .map(|posting| posting.primary_key)
            .collect::<Vec<_>>();
        assert_eq!(again, ranked);
    }
}
