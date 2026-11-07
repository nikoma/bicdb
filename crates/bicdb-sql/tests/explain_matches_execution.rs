//! EXPLAIN must describe the access path that execution actually uses.
//!
//! For each single-table SELECT shape this compares the access path EXPLAIN
//! reports (first plan line) with the access path recorded while the same
//! statement really executes (opt-in `plan_diagnostics` trace), with and
//! without ANALYZE statistics, and inside a transaction with pending writes.

use bicdb_core::BicDb;
use bicdb_sql::plan_diagnostics::{with_plan_diagnostics, DecisionTrace, DiagnosticsOptions};
use bicdb_sql::SqlSession;

/// (kind, index) of the access path EXPLAIN claims, from its first line.
fn explained(session: &mut SqlSession<'_>, sql: &str) -> (String, Option<String>, Vec<String>) {
    let lines = session
        .execute(&format!("EXPLAIN {sql}"))
        .unwrap_or_else(|error| panic!("EXPLAIN {sql}: {error}"))
        .rows
        .into_iter()
        .map(|row| row[0].to_cell())
        .collect::<Vec<_>>();
    let first = lines[0].clone();
    let mut words = first.split_whitespace();
    let head = words.next().unwrap_or_default();
    let index = words.next().map(str::to_string);
    let kind = match head {
        "IndexScan" => "IndexLookup",
        "IndexRangeScan" => "IndexRange",
        "IndexPrefixOrderedScan" => "IndexPrefixOrderedScan",
        "OrderedIndexScan" => "OrderedIndexScan",
        "FullScan" | "SeqScan" => "FullScan",
        "PrimaryKeyInLookup" if first.contains(" 0 ids") => "Empty",
        "Empty" => "Empty",
        other => other,
    };
    let index = match kind {
        "FullScan" | "Empty" | "PrimaryKeyInLookup" | "PrimaryKeyLookup" => None,
        _ => index,
    };
    (kind.to_string(), index, lines)
}

/// (kind, index) of the access path that fetched rows during execution.
fn executed(decisions: &[DecisionTrace]) -> Option<(String, Option<String>)> {
    if decisions.iter().any(|decision| decision.contradiction) {
        return Some(("Empty".into(), None));
    }
    // The route that fetched rows executed; a declined rule-based chooser
    // only decides (a scan) when nothing fetched rows.
    let decision = decisions
        .iter()
        .rev()
        .find(|decision| decision.rows_fetched.is_some())
        .or_else(|| {
            decisions
                .iter()
                .rev()
                .find(|decision| decision.chooser != "planner")
        })?;
    let signature = &decision.candidates[decision.chosen].signature;
    let kind = match signature.kind {
        "PrimaryKeyInLookup" if signature.prefix_len == 0 => "Empty",
        other => other,
    };
    let index = match kind {
        "FullScan" | "Empty" => None,
        _ => signature.index.clone(),
    };
    Some((kind.to_string(), index))
}

const SHAPES: &[&str] = &[
    "SELECT id, c FROM t WHERE a = 3",
    "SELECT * FROM t WHERE a = 3",
    "SELECT id FROM t WHERE a = 3 AND b = 2",
    "SELECT id FROM t WHERE a = 0 AND d = 17",
    "SELECT id FROM t WHERE a = 0 AND b = 1 AND d = 17",
    "SELECT id FROM t WHERE a = 3 ORDER BY b LIMIT 5",
    "SELECT id FROM t WHERE a = 3 ORDER BY d LIMIT 5",
    "SELECT id FROM t WHERE a = 3 LIMIT 5",
    "SELECT count(*) FROM t WHERE a = 3",
    "SELECT b, count(*) FROM t WHERE a = 3 GROUP BY b",
    "SELECT DISTINCT b FROM t WHERE a = 3",
    "SELECT id FROM t WHERE c = 'x5'",
    "SELECT id FROM t WHERE a > 7",
    "SELECT id FROM t WHERE d BETWEEN 10 AND 20",
    "SELECT id FROM t WHERE a = 0",
    "SELECT id FROM t WHERE a > 60 AND a < 30",
    "SELECT id FROM t ORDER BY d LIMIT 5",
    "SELECT id FROM t WHERE id IN (1, 2, 3)",
    "SELECT id FROM t WHERE a = 3 OR d = 2",
    "SELECT id FROM t WHERE h = 5",
    "SELECT id FROM t WHERE h > 5",
    "SELECT id FROM t WHERE p << point(3, 0)",
    "SELECT id FROM t WHERE j @> '{\"k\": 3}'::jsonb",
    "SELECT id FROM t WHERE arr @> ARRAY[3]",
];

fn setup(db: &mut BicDb) {
    SqlSession::new(db)
        .execute(
            "CREATE TABLE t (id int4 PRIMARY KEY, a int4, b int4, c text, d int4, h int4, p point, j jsonb, arr int4[]);
             INSERT INTO t SELECT g, CASE WHEN g % 10 < 8 THEN 0 ELSE g % 10 END, g % 7, 'x' || g, g,
               g % 50, point(g % 13, g % 17), jsonb_build_object('k', g % 40), ARRAY[g % 30, g % 7]
               FROM generate_series(1, 6000) g;
             CREATE INDEX t_j ON t USING gin (j);
             CREATE INDEX t_arr ON t USING gin (arr);
             CREATE INDEX t_h ON t USING hash (h);
             CREATE INDEX t_p ON t USING gist (p);
             CREATE INDEX t_a ON t (a);
             CREATE INDEX t_ab ON t (a, b);
             CREATE INDEX t_d ON t (d);",
        )
        .unwrap();
}

fn check_all(db: &mut BicDb, condition: &str, in_write_tx: bool) -> Vec<String> {
    let mut mismatches = Vec::new();
    for sql in SHAPES {
        let mut session = SqlSession::new(db);
        if in_write_tx {
            session.execute("BEGIN").unwrap();
            session
                .execute("INSERT INTO t VALUES (100000, 3, 3, 'new', 100000, 5, point(1, 1), '{\"k\": 3}', ARRAY[3])")
                .unwrap();
        }
        let (kind, index, lines) = explained(&mut session, sql);
        let (result, decisions) =
            with_plan_diagnostics(DiagnosticsOptions::default(), || session.execute(sql));
        result.unwrap_or_else(|error| panic!("{sql}: {error}"));
        if in_write_tx {
            session.execute("ROLLBACK").unwrap();
        }
        match executed(&decisions) {
            Some(actual) if actual == (kind.clone(), index.clone()) => {}
            actual => mismatches.push(format!(
                "[{condition}] {sql}\n    EXPLAIN says {kind} {index:?} ({lines:?})\n    executed    {actual:?}\n    trace {decisions:?}"
            )),
        }
    }
    mismatches
}

#[test]
fn explain_reports_the_executed_access_path() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    setup(&mut db);
    let mut mismatches = check_all(&mut db, "no stats", false);
    SqlSession::new(&mut db).execute("ANALYZE t").unwrap();
    mismatches.extend(check_all(&mut db, "after ANALYZE", false));
    mismatches.extend(check_all(&mut db, "pending writes", true));
    assert!(
        mismatches.is_empty(),
        "{} EXPLAIN/execution mismatches:\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}
