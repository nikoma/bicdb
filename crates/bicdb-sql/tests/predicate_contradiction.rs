//! Differential tests for deterministic WHERE contradiction detection: every
//! predicate must return exactly the rows it returns with detection disabled,
//! over data that includes NULLs and boundary values, and provable
//! contradictions must be answered without scanning the table.

use bicdb_core::BicDb;
use bicdb_sql::plan_diagnostics::{with_plan_diagnostics, DecisionTrace, DiagnosticsOptions};
use bicdb_sql::{SqlSession, SqlValue};

const SETUP: &str = "
    CREATE TABLE c (
        id int4 PRIMARY KEY,
        i int4,
        s int2,
        b int8,
        n numeric,
        nn numeric(6, 2),
        f float8,
        t text
    );
    INSERT INTO c VALUES
        (1, 2, 2, 2, 2.5, 2.50, 0.1, 'a'),
        (2, 3, 3, 3, 3, 3.00, 0.30000000000000004, 'b'),
        (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
        (4, 5, 5, 5, 5.0, 5.00, 5.0, 'c'),
        (5, -5, -5, -5, -5.25, -5.25, -0.0, 'd'),
        (6, 60, 60, 60, 60, 60.00, 60.5, 'e'),
        (7, 30, 30, 30, 30, 30.00, 30.0, 'f');
";

fn rows(
    session: &mut SqlSession<'_>,
    sql: &str,
    detect: bool,
) -> (Vec<Vec<String>>, Vec<DecisionTrace>) {
    let options = DiagnosticsOptions {
        disable_contradiction_detection: !detect,
        ..DiagnosticsOptions::default()
    };
    let (result, decisions) = with_plan_diagnostics(options, || session.execute(sql));
    let mut rows = result
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .into_iter()
        .map(|row| row.iter().map(SqlValue::to_cell).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    rows.sort();
    (rows, decisions)
}

fn detected(decisions: &[DecisionTrace]) -> bool {
    decisions.iter().any(|decision| decision.contradiction)
}

/// Runs `predicate` with and without detection; returns whether it was
/// detected. Asserts the results are identical and that a detected
/// contradiction returns no rows and fetches no rows.
fn check(session: &mut SqlSession<'_>, predicate: &str) -> bool {
    let sql = format!("SELECT id FROM c WHERE {predicate}");
    let (baseline, _) = rows(session, &sql, false);
    let (optimized, decisions) = rows(session, &sql, true);
    assert_eq!(optimized, baseline, "result changed for: {predicate}");
    let detected = detected(&decisions);
    if detected {
        assert!(
            optimized.is_empty(),
            "contradiction returned rows: {predicate}"
        );
        let fetched = decisions
            .iter()
            .filter_map(|decision| decision.rows_fetched)
            .sum::<usize>();
        assert_eq!(fetched, 0, "contradiction still fetched rows: {predicate}");
        // The executed chooser itself must have answered with the proof (a
        // declined locator would mean a full scan).
        let locator = decisions
            .iter()
            .filter(|decision| decision.chooser == "locator")
            .collect::<Vec<_>>();
        if locator.is_empty() {
            // Planner-executed path: the empty plan must have run.
            assert!(
                decisions
                    .iter()
                    .any(|decision| decision.contradiction && decision.rows_fetched == Some(0)),
                "executed path did not use the proof for {predicate}: {decisions:?}"
            );
        } else {
            assert!(
                locator.iter().all(|decision| decision.contradiction),
                "executed path did not use the proof for {predicate}: {decisions:?}"
            );
        }
    }
    detected
}

fn session_with_data(db: &mut BicDb) -> SqlSession<'_> {
    let mut session = SqlSession::new(db);
    session.execute(SETUP).unwrap();
    session
}

#[test]
fn provable_contradictions_are_detected_and_return_nothing() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = session_with_data(&mut db);
    for predicate in [
        "i > 60 AND i < 30",
        "i < 30 AND 60 < i",
        "c.i > 60 AND c.i < 30",
        "(i > 5) AND (t = 'a' AND i <= 5)",
        "i >= 5 AND i < 5",
        "i > 5 AND i <= 5",
        "i >= 5 AND i <= 5 AND i <> 5",
        "i BETWEEN 10 AND 1",
        "i = 2 AND i = 3",
        "i IN (2, 3) AND i > 3",
        "i IN (2, 3) AND i IN (5, 60)",
        "i >= 3.0 AND i < 3",
        "s > 100 AND s < -100",
        "b > 9007199254740993 AND b < 9007199254740992",
        "n > 2.5 AND n < 2.5",
        "n > 2.5000000000000000000001 AND n <= 2.5",
        "f > 1.5 AND f < 1.0",
        "i = NULL",
        "NULL = i",
        "i > NULL AND i < 100",
        "i BETWEEN NULL AND 100",
        "i IN (NULL)",
        "i IS NULL AND i > 5",
        "i IS NULL AND i IS NOT NULL",
        "n IS NULL AND n IN (2.5, 3)",
    ] {
        assert!(check(&mut session, predicate), "not detected: {predicate}");
    }
}

#[test]
fn satisfiable_or_unprovable_predicates_are_left_alone() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = session_with_data(&mut db);
    for predicate in [
        "i > 30 AND i < 61",
        "i >= 5 AND i <= 5",
        "i BETWEEN 5 AND 5",
        "i IN (2, 3) AND i >= 3",
        "i IN (NULL, 3) AND i >= 3",
        "i > 60 OR i < 30",
        "NOT (i > 60 AND i < 30)",
        "i NOT BETWEEN 10 AND 1",
        "i NOT IN (2, 3) AND i = 2",
        "i IS NULL",
        "i IS NOT NULL AND i > 5",
        "i IS NULL AND n IS NOT NULL",
        // Fractional constants on integer columns are left to the evaluator.
        "i > 2.5 AND i < 2.6",
        "i = 2.5 AND i = 3",
        "i > 2.5 AND i < 1",
        // Out of range for int2.
        "s > 40000 AND s < 0",
        // Typmod-constrained numeric is not reasoned about.
        "nn > 2.5 AND nn < 2.5",
        // Doubles: equal as doubles, ordered as decimals (either way).
        "f >= 0.10000000000000000001 AND f <= 0.1",
        "f > 0.1 AND f < 0.10000000000000000556",
        // Non-numeric and non-literal operands.
        "t > 'b' AND t < 'a'",
        "i > id AND i < id",
        "i > abs(-60) AND i < 30",
        "i > CAST(60 AS int4) AND i < 30",
    ] {
        assert!(
            !check(&mut session, predicate),
            "unexpectedly detected: {predicate}"
        );
    }
}

#[test]
fn contradiction_semantics_hold_for_aggregates_and_dml() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = session_with_data(&mut db);
    let (count, decisions) = rows(
        &mut session,
        "SELECT count(*) FROM c WHERE i > 60 AND i < 30",
        true,
    );
    assert_eq!(count, vec![vec!["0".to_string()]]);
    assert!(detected(&decisions));
    let (sum, _) = rows(
        &mut session,
        "SELECT sum(i) FROM c WHERE i > 60 AND i < 30",
        true,
    );
    assert_eq!(sum, vec![vec!["".to_string()]]);
    session
        .execute("UPDATE c SET t = 'changed' WHERE i > 60 AND i < 30")
        .unwrap();
    session
        .execute("DELETE FROM c WHERE i IS NULL AND i > 0")
        .unwrap();
    let (all, _) = rows(&mut session, "SELECT id, t FROM c", true);
    assert_eq!(all.len(), 7);
    assert!(all.iter().all(|row| row[1] != "changed"));
}

#[test]
fn bound_parameters_participate_only_when_bound_to_supported_values() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session.execute(SETUP).unwrap();
    }
    let cases: [(&str, Vec<SqlValue>, bool); 6] = [
        (
            "i > $1 AND i < $2",
            vec![SqlValue::Int(60), SqlValue::Int(30)],
            true,
        ),
        (
            "i > $1 AND i < $2",
            vec![SqlValue::Int(29), SqlValue::Int(61)],
            false,
        ),
        ("i = $1", vec![SqlValue::Null], true),
        (
            "f > $1 AND f < $2",
            vec![SqlValue::Float(1.5), SqlValue::Float(1.0)],
            true,
        ),
        // A double parameter on an integer column is left to the evaluator.
        (
            "i > $1 AND i < $2",
            vec![SqlValue::Float(60.0), SqlValue::Int(30)],
            false,
        ),
        // Text parameters are never reasoned about.
        (
            "i > $1 AND i < $2",
            vec![SqlValue::String("60".into()), SqlValue::Int(30)],
            false,
        ),
    ];
    for (predicate, parameters, expect_detected) in cases {
        let sql = format!("SELECT id FROM c WHERE {predicate}");
        let mut run = |detect: bool| {
            let mut session =
                SqlSession::new(&mut db).with_positional_parameters(parameters.clone());
            rows(&mut session, &sql, detect)
        };
        let (baseline, _) = run(false);
        let (optimized, decisions) = run(true);
        assert_eq!(
            optimized, baseline,
            "result changed for: {predicate} {parameters:?}"
        );
        assert_eq!(
            detected(&decisions),
            expect_detected,
            "detection mismatch for: {predicate} {parameters:?}; decisions: {decisions:?}"
        );
    }
}

#[test]
fn diagnostics_are_inert_without_a_scope_and_record_no_values() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = session_with_data(&mut db);
    // Without a scope nothing is recorded and plans are unchanged.
    session.execute("SELECT id FROM c WHERE i = 5").unwrap();
    let (_, decisions) = with_plan_diagnostics(DiagnosticsOptions::default(), || {
        session
            .execute("SELECT id FROM c WHERE i = 424242")
            .unwrap()
    });
    let rendered = format!("{decisions:?}");
    assert!(!decisions.is_empty());
    assert!(
        !rendered.contains("424242"),
        "trace leaked a literal: {rendered}"
    );
}
