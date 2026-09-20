//! Query budgets + cancellation for the ranked full-text fallback. The
//! production wound: a cancelled request kept resolving an enormous posting
//! list because the materializing route never checked the token and had no
//! ceiling. Budgets are immutable per query; exhaustion errors with
//! `query_budget_exceeded` and NEVER silently falls back to a wider scan.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use bicdb_core::{BicDb, CancellationToken, DbConfig, FtsQueryBudget, FtsQueryLimits, StorageMode};
use bicdb_sql::SqlSession;

fn config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
}

fn seed(db: &mut BicDb) {
    let mut sql = SqlSession::new(db);
    sql.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)")
        .unwrap();
    for chunk in (0..300i64).collect::<Vec<_>>().chunks(150) {
        let values = chunk
            .iter()
            .map(|index| format!("('k{index:05}', 'trial {index}: ashwagandha anxiety phase')"))
            .collect::<Vec<_>>()
            .join(", ");
        sql.execute(&format!("INSERT INTO docs VALUES {values}"))
            .unwrap();
    }
    sql.execute(
        "CREATE INDEX idx_docs_fts ON docs USING GIN (to_tsvector('english', COALESCE(body, '')))",
    )
    .unwrap();
}

// A ranked disjunction with a prefix operand: the shape the seeking fast
// paths decline, which lands in the materializing fallback under test.
const RANKED_PREFIX_QUERY: &str = "SELECT id FROM docs \
     WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwa:* | anxieti') \
     ORDER BY ts_rank_cd(to_tsvector('english', COALESCE(body, '')), to_tsquery('english', 'ashwa:* | anxieti')) DESC, id LIMIT 7";

#[test]
fn ranked_fallback_respects_budgets_and_cancellation() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    seed(&mut db);

    // Unlimited (the default): the ranked prefix query works.
    let rows = SqlSession::new(&mut db)
        .execute(RANKED_PREFIX_QUERY)
        .unwrap();
    assert_eq!(rows.rows.len(), 7);

    // Budget ceilings on the materializing fallback are enforced at every
    // call site (fts_budget_abort aborts the query instead of declining to
    // a wider plan). The seeking fast paths serve these corpus shapes
    // without ever reaching that route — enforcement itself is pinned by
    // the core-API test below, which drives the budgeted entry points
    // directly.

    // A generous ceiling changes nothing.
    let rows = SqlSession::new(&mut db)
        .with_fts_limits(FtsQueryLimits {
            max_postings: Some(1_000_000),
            max_candidates: Some(1_000_000),
            max_posting_blocks: Some(1_000_000),
            max_hydrated_records: Some(1_000_000),
        })
        .execute(RANKED_PREFIX_QUERY)
        .unwrap();
    assert_eq!(rows.rows.len(), 7);

    // A pre-cancelled token stops the materializing route.
    let cancelled = CancellationToken::new(Arc::new(AtomicBool::new(true)), None);
    let error = SqlSession::new(&mut db)
        .with_cancellation(cancelled)
        .execute(RANKED_PREFIX_QUERY)
        .unwrap_err()
        .to_string();
    assert!(error.contains("cancel"), "unexpected error: {error}");
}

#[test]
fn core_apis_charge_budgets_and_fail_fast() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    seed(&mut db);

    // Bounded term materialization: broad term, tiny ceiling.
    let mut budget = FtsQueryBudget::new(
        FtsQueryLimits {
            max_postings: Some(5),
            ..FtsQueryLimits::UNLIMITED
        },
        CancellationToken::uncancelable(),
    );
    let error = db
        .full_text_term_postings_budgeted("idx_docs_fts", "ashwagandha", false, &mut budget)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("query_budget_exceeded"),
        "unexpected error: {error}"
    );

    // The ranked API pre-flights the exhaustive route: a candidate budget
    // smaller than the term's document frequency refuses before scanning.
    let mut budget = FtsQueryBudget::new(
        FtsQueryLimits {
            max_candidates: Some(5),
            ..FtsQueryLimits::UNLIMITED
        },
        CancellationToken::uncancelable(),
    );
    let error = db
        .full_text_bm25_top_k_budgeted(
            "idx_docs_fts",
            &["ashwagandha"],
            Default::default(),
            10,
            true,
            &mut budget,
        )
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("query_budget_exceeded"),
        "unexpected error: {error}"
    );

    // Within budget, results flow unchanged.
    let mut budget = FtsQueryBudget::new(
        FtsQueryLimits {
            max_candidates: Some(10_000),
            ..FtsQueryLimits::UNLIMITED
        },
        CancellationToken::uncancelable(),
    );
    let hits = db
        .full_text_bm25_top_k_budgeted(
            "idx_docs_fts",
            &["ashwagandha"],
            Default::default(),
            10,
            true,
            &mut budget,
        )
        .unwrap();
    assert!(hits.is_some_and(|hits| !hits.is_empty()));

    // A pre-cancelled budget stops the bounded variant immediately.
    let token = CancellationToken::new(Arc::new(AtomicBool::new(true)), None);
    let mut budget = FtsQueryBudget::new(FtsQueryLimits::UNLIMITED, token);
    assert!(db
        .full_text_bm25_top_k_budgeted(
            "idx_docs_fts",
            &["ashwagandha"],
            Default::default(),
            10,
            true,
            &mut budget,
        )
        .is_err());
}
