//! Opt-in access-path diagnostics: tracing records the executed chooser's
//! legal candidates without values, and forcing any candidate preserves
//! results.

use bicdb_core::BicDb;
use bicdb_sql::plan_diagnostics::{
    with_plan_diagnostics, DecisionTrace, DiagnosticsOptions, PlanSignature,
};
use bicdb_sql::{SqlSession, SqlValue};

fn run(
    session: &mut SqlSession<'_>,
    sql: &str,
    force: Option<PlanSignature>,
) -> (Vec<Vec<String>>, Vec<DecisionTrace>) {
    let options = DiagnosticsOptions {
        force,
        ..DiagnosticsOptions::default()
    };
    let (result, decisions) = with_plan_diagnostics(options, || session.execute(sql));
    let mut rows = result
        .unwrap()
        .rows
        .into_iter()
        .map(|row| row.iter().map(SqlValue::to_cell).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    rows.sort();
    (rows, decisions)
}

#[test]
fn every_executed_candidate_can_be_forced_without_changing_results() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE t (id int4 PRIMARY KEY, a int4, b int4, c int4);
             INSERT INTO t SELECT g, g % 10, g % 7, g FROM generate_series(1, 3000) g;
             CREATE INDEX t_a ON t (a);
             CREATE INDEX t_ab ON t (a, b);
             CREATE INDEX t_c ON t (c);",
        )
        .unwrap();
    for sql in [
        "SELECT id FROM t WHERE a = 3",
        "SELECT id FROM t WHERE a = 3 AND b = 2",
        "SELECT id FROM t WHERE a = 3 AND c > 2000",
        "SELECT id FROM t WHERE c BETWEEN 10 AND 20 AND a = 3",
    ] {
        let (natural_rows, decisions) = run(&mut session, sql, None);
        let executed = decisions
            .iter()
            .rev()
            .find(|decision| decision.chooser == "locator")
            .unwrap_or_else(|| panic!("{sql}: no locator decision in {decisions:?}"))
            .clone();
        assert!(!executed.forced);
        let chosen = &executed.candidates[executed.chosen].signature;
        assert_ne!(chosen.kind, "FullScan", "{sql}: expected an index choice");
        assert!(executed
            .candidates
            .iter()
            .any(|candidate| candidate.signature == PlanSignature::full_scan()));
        for candidate in &executed.candidates {
            let (rows, forced_decisions) =
                run(&mut session, sql, Some(candidate.signature.clone()));
            assert_eq!(rows, natural_rows, "{sql} forced {:?}", candidate.signature);
            let locator = forced_decisions
                .iter()
                .filter(|decision| decision.chooser == "locator")
                .collect::<Vec<_>>();
            assert!(!locator.is_empty(), "{sql}: {forced_decisions:?}");
            assert!(
                locator.iter().all(|decision| decision.forced
                    && decision.candidates[decision.chosen].signature == candidate.signature),
                "{sql} forced {:?}: {locator:?}",
                candidate.signature
            );
            if candidate.signature.kind == "FullScan" {
                // Declined to locate; the row-set path then scanned the table.
                assert!(
                    locator
                        .iter()
                        .any(|decision| decision.rows_fetched == Some(3000)),
                    "{sql}: {locator:?}"
                );
            } else {
                assert!(locator
                    .iter()
                    .all(|decision| decision.rows_fetched.is_some()));
            }
        }
        // An unknown signature is ignored, not obeyed.
        let bogus = PlanSignature {
            kind: "IndexLookup",
            index: Some("no_such_index".into()),
            prefix_len: 1,
        };
        let (rows, decisions) = run(&mut session, sql, Some(bogus));
        assert_eq!(rows, natural_rows);
        assert!(decisions.iter().all(|decision| !decision.forced));
    }
}

#[test]
fn diagnostics_are_off_by_default() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE t (id int4 PRIMARY KEY, a int4);
             INSERT INTO t SELECT g, g % 10 FROM generate_series(1, 100) g;
             CREATE INDEX t_a ON t (a);",
        )
        .unwrap();
    // A scope on another thread must not affect this one.
    let handle = std::thread::spawn(|| {
        with_plan_diagnostics(
            DiagnosticsOptions {
                force: Some(PlanSignature::full_scan()),
                ..DiagnosticsOptions::default()
            },
            || std::thread::sleep(std::time::Duration::from_millis(50)),
        )
    });
    let result = session
        .execute("SELECT count(*) FROM t WHERE a = 3")
        .unwrap();
    assert_eq!(result.rows[0][0].to_cell(), "10");
    let (_, decisions) = handle.join().unwrap();
    assert!(decisions.is_empty());
    // After a scope ends, nothing is recorded or forced.
    let (_, decisions) = with_plan_diagnostics(DiagnosticsOptions::default(), || {
        session.execute("SELECT id FROM t WHERE a = 3").unwrap()
    });
    assert!(!decisions.is_empty());
    let (inner, _) = with_plan_diagnostics(DiagnosticsOptions::default(), || {
        with_plan_diagnostics(DiagnosticsOptions::default(), || {
            session.execute("SELECT id FROM t WHERE a = 4").unwrap()
        })
    });
    assert!(!inner.1.is_empty(), "inner scope records its own decisions");
}
