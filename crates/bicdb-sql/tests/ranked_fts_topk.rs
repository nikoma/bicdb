//! Rank-from-index (Slice 2): `WHERE tsv @@ q ORDER BY ts_rank[_cd](...) DESC
//! LIMIT k` scored entirely from durable postings. Oracle: the same statement
//! inside a transaction, which forces the ranked path to decline — both
//! answers come from the same rank code, one fed by sparse posting vectors,
//! one by re-parsed row text. Documents are built with strictly different
//! occurrence counts so ranks are distinct and orders comparable exactly.

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

const ROWS: usize = 1_200;

fn seeded() -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let config = || {
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged)
    };
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)")
            .unwrap();
        sql.execute(
            "CREATE INDEX idx_docs_fts ON docs \
             USING GIN (to_tsvector('english', COALESCE(body, '')))",
        )
        .unwrap();
        for chunk in (0..ROWS).collect::<Vec<_>>().chunks(200) {
            let values = chunk
                .iter()
                .map(|index| {
                    // Occurrence counts vary per row (1..=13 alphas), phrases
                    // appear on multiples of 7, valerian on multiples of 5,
                    // animals on multiples of 11 — enough shape for AND/OR/
                    // NOT/phrase/prefix queries with distinct ranks.
                    let alphas = std::iter::repeat("ashwagandha")
                        .take(1 + index % 13)
                        .collect::<Vec<_>>()
                        .join(" filler ");
                    let phrase = if index % 7 == 0 { " sleep quality" } else { " quality sleep aid" };
                    let valerian = if index % 5 == 0 { " valerian extract" } else { "" };
                    let animals = if index % 11 == 0 { " animals model" } else { "" };
                    format!(
                        "('k{index:05}', 'trial {index}: {alphas}{phrase}{valerian}{animals} anxiety')"
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            sql.execute(&format!("INSERT INTO docs VALUES {values}"))
                .unwrap();
        }
    }
    db.close().unwrap();
    let db = BicDb::open_with_config(dir.path(), config()).unwrap();
    (dir, db)
}

const QUERIES: &[&str] = &[
    // Plain single-term rank, both rank functions.
    "SELECT id FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha') \
     ORDER BY ts_rank(to_tsvector('english', COALESCE(body, '')), to_tsquery('english', 'ashwagandha')) DESC LIMIT 5",
    "SELECT id FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha') \
     ORDER BY ts_rank_cd(to_tsvector('english', COALESCE(body, '')), to_tsquery('english', 'ashwagandha')) DESC LIMIT 5",
    // AND + normalization flags.
    "SELECT id FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha & anxieti') \
     ORDER BY ts_rank(to_tsvector('english', COALESCE(body, '')), to_tsquery('english', 'ashwagandha & anxieti'), 1) DESC LIMIT 5",
    "SELECT id FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha') \
     ORDER BY ts_rank(to_tsvector('english', COALESCE(body, '')), to_tsquery('english', 'ashwagandha'), 2) DESC LIMIT 5",
    "SELECT id FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha') \
     ORDER BY ts_rank(to_tsvector('english', COALESCE(body, '')), to_tsquery('english', 'ashwagandha'), 8) DESC LIMIT 5",
    "SELECT id FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha') \
     ORDER BY ts_rank(to_tsvector('english', COALESCE(body, '')), to_tsquery('english', 'ashwagandha'), 16) DESC LIMIT 5",
    "SELECT id FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha') \
     ORDER BY ts_rank(to_tsvector('english', COALESCE(body, '')), to_tsquery('english', 'ashwagandha'), 32) DESC LIMIT 5",
    // OR and NOT in the WHERE query (NOT contributes to matching).
    "SELECT id FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'valerian | ashwagandha') \
     ORDER BY ts_rank(to_tsvector('english', COALESCE(body, '')), to_tsquery('english', 'valerian | ashwagandha')) DESC LIMIT 5",
    "SELECT id FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha & !anim') \
     ORDER BY ts_rank(to_tsvector('english', COALESCE(body, '')), to_tsquery('english', 'ashwagandha & !anim')) DESC LIMIT 5",
    // Phrase: the term-AND candidate is a superset; the sparse recheck must
    // use positions to keep only true phrase matches.
    "SELECT id FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'sleep <-> qualiti') \
     ORDER BY ts_rank(to_tsvector('english', COALESCE(body, '')), to_tsquery('english', 'sleep <-> qualiti')) DESC LIMIT 5",
    // Prefix operand: expands over the term keyspace.
    "SELECT id FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwa:*') \
     ORDER BY ts_rank(to_tsvector('english', COALESCE(body, '')), to_tsquery('english', 'ashwa:*')) DESC LIMIT 5",
    // Weights array argument.
    "SELECT id FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha') \
     ORDER BY ts_rank('{0.2, 0.3, 0.5, 0.9}'::float4[], to_tsvector('english', COALESCE(body, '')), to_tsquery('english', 'ashwagandha')) DESC LIMIT 5",
    // Pure conjunctive, normalization 0: the streaming-arena fast path.
    "SELECT id FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha & anxieti') \
     ORDER BY ts_rank(to_tsvector('english', COALESCE(body, '')), to_tsquery('english', 'ashwagandha & anxieti')) DESC LIMIT 5",
    // Three-term conjunction.
    "SELECT id FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha & anxieti & valerian') \
     ORDER BY ts_rank(to_tsvector('english', COALESCE(body, '')), to_tsquery('english', 'ashwagandha & anxieti & valerian')) DESC LIMIT 5",
    // LIMIT wider than the full-match count: partial matches (driver hits
    // missing another WHERE term) must NOT leak into the tail of the result.
    "SELECT id FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'valerian & anim') \
     ORDER BY ts_rank(to_tsvector('english', COALESCE(body, '')), to_tsquery('english', 'valerian & anim')) DESC LIMIT 30",
    // Multi-term WHERE ranked by a single different term.
    "SELECT id FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'valerian & anxieti') \
     ORDER BY ts_rank(to_tsvector('english', COALESCE(body, '')), to_tsquery('english', 'ashwagandha')) DESC, id LIMIT 5",
    // OFFSET participates in the keep bound.
    "SELECT id FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha') \
     ORDER BY ts_rank(to_tsvector('english', COALESCE(body, '')), to_tsquery('english', 'ashwagandha')) DESC LIMIT 4 OFFSET 3",
];

fn ids(result: &bicdb_sql::SqlResult) -> Vec<String> {
    result
        .rows
        .iter()
        .map(|row| match &row[0] {
            SqlValue::String(id) => id.clone(),
            other => panic!("unexpected {other:?}"),
        })
        .collect()
}

#[test]
fn ranked_topk_matches_the_text_ranked_oracle() {
    let (_dir, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    let mut streamed = Vec::new();
    for query in QUERIES {
        let result = sql
            .execute(query)
            .unwrap_or_else(|error| panic!("`{query}` failed: {error}"));
        streamed.push(ids(&result));
    }
    sql.execute("BEGIN").unwrap();
    for (query, streamed) in QUERIES.iter().zip(&streamed) {
        let oracle = sql
            .execute(query)
            .unwrap_or_else(|error| panic!("oracle `{query}` failed: {error}"));
        assert_eq!(
            &ids(&oracle),
            streamed,
            "rank-from-index diverged from text ranking for `{query}`"
        );
    }
    sql.execute("COMMIT").unwrap();
}

#[test]
fn ranked_query_right_after_create_index_still_answers_correctly() {
    // In the CREATE INDEX session the index serves residently (not
    // read-through); the ranked path must decline and the fallback answer.
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)")
        .unwrap();
    sql.execute(
        "INSERT INTO docs VALUES ('a', 'ashwagandha ashwagandha calm'), ('b', 'ashwagandha calm')",
    )
    .unwrap();
    sql.execute(
        "CREATE INDEX idx_docs_fts ON docs USING GIN (to_tsvector('english', COALESCE(body, '')))",
    )
    .unwrap();
    let result = sql
        .execute(
            "SELECT id FROM docs WHERE to_tsvector('english', COALESCE(body, '')) @@ to_tsquery('english', 'ashwagandha') \
             ORDER BY ts_rank(to_tsvector('english', COALESCE(body, '')), to_tsquery('english', 'ashwagandha')) DESC LIMIT 1",
        )
        .unwrap();
    assert_eq!(ids(&result), vec!["a".to_string()]);
}

/// The engine's impact scans score with `bicdb_core::fts_rank_single_term`,
/// a closed form of `rank_or` for one matching lexeme. If the two ever
/// drift, ranked fast-path results silently diverge from the oracle — pin
/// them together bit-for-bit across tf, weight labels, and the empty case.
#[test]
fn closed_form_single_term_rank_matches_ts_rank_exactly() {
    use bicdb_sql::ts_rank_with_scalars_for_tests as full_rank;
    let weights = [0.1f32, 0.2, 0.4, 1.0];
    let mut state = 0x9E37_79B9_u64;
    let mut next = move || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        state >> 33
    };
    for _ in 0..2_000 {
        let count = (next() % 24) as usize;
        let packed: Vec<u16> = (0..count)
            .map(|_| {
                let position = (next() % 0x3FFF) as u16;
                let weight = (next() % 4) as u16;
                position | (weight << 14)
            })
            .collect();
        let closed = bicdb_core::fts_rank_single_term(&packed, weights);
        let full = full_rank("term", &packed, weights);
        assert!(
            closed.to_bits() == full.to_bits(),
            "closed form diverged: packed={packed:?} closed={closed} full={full}"
        );
    }
}

/// Same lockstep pin for the conjunctive (And/Phrase) closed form: 2-4 term
/// documents with random positions, weight labels, absent terms, and the
/// present-but-position-less substitution case.
#[test]
fn closed_form_conjunctive_rank_matches_ts_rank_exactly() {
    use bicdb_sql::ts_rank_conjunctive_for_tests as full_rank;
    let weights = [0.1f32, 0.2, 0.4, 1.0];
    // MUST be in sorted-text order: fts sorts operands by text before
    // rank_and, and pair order changes the noisy-OR float accumulation —
    // the closed form's contract is "operands sorted by text".
    let names = ["alpha", "beta", "gamma", "zeta"];
    let mut state = 0xC0FF_EE00_u64;
    let mut next = move || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        state >> 33
    };
    for _ in 0..2_000 {
        let term_count = 2 + (next() % 3) as usize;
        let lists: Vec<Option<Vec<u16>>> = (0..term_count)
            .map(|_| match next() % 8 {
                0 => None,
                1 => Some(Vec::new()),
                _ => {
                    let count = 1 + (next() % 12) as usize;
                    Some(
                        (0..count)
                            .map(|_| {
                                let position = (next() % 200) as u16;
                                let weight = (next() % 4) as u16;
                                position | (weight << 14)
                            })
                            .collect(),
                    )
                }
            })
            .collect();
        let slices: Vec<Option<&[u16]>> = lists.iter().map(|list| list.as_deref()).collect();
        let closed = bicdb_core::fts_rank_conjunctive(&slices, weights);
        let with_names: Vec<(&str, Option<&[u16]>)> = names[..term_count]
            .iter()
            .zip(&slices)
            .map(|(name, positions)| (*name, *positions))
            .collect();
        let full = full_rank(&with_names, weights);
        assert!(
            closed.to_bits() == full.to_bits(),
            "conjunctive closed form diverged: lists={lists:?} closed={closed} full={full}"
        );
    }
}
