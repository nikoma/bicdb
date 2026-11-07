//! Which access-path chooser executes which single-table SELECT shape.
//! Prints a routing table (run with --nocapture); asserts only that every
//! shape is answered correctly.

use bicdb_core::BicDb;
use bicdb_sql::plan_diagnostics::{with_plan_diagnostics, DiagnosticsOptions};
use bicdb_sql::{SqlEngine, SqlSession};

#[test]
fn routing_table() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    SqlSession::new(&mut db)
        .execute(
            "CREATE TABLE t (id int4 PRIMARY KEY, a int4, b int4, c text);
             INSERT INTO t SELECT g, g % 10, g % 7, 'x' || g FROM generate_series(1, 6000) g;
             CREATE INDEX t_a ON t (a);
             CREATE INDEX t_ab ON t (a, b);",
        )
        .unwrap();
    let shapes = [
        "SELECT id, c FROM t WHERE a = 3",
        "SELECT * FROM t WHERE a = 3",
        "SELECT id FROM t WHERE a = 3 AND b = 2",
        "SELECT id FROM t WHERE a = 3 ORDER BY b LIMIT 5",
        "SELECT id FROM t WHERE a = 3 LIMIT 5",
        "SELECT count(*) FROM t WHERE a = 3",
        "SELECT b, count(*) FROM t WHERE a = 3 GROUP BY b",
        "SELECT DISTINCT b FROM t WHERE a = 3",
        "SELECT id FROM t WHERE c = 'x5'",
        "SELECT id FROM t WHERE a > 7",
        "SELECT id FROM t WHERE a = 3 OR b = 2",
        "EXPLAIN SELECT id FROM t WHERE a = 3",
        "EXPLAIN ANALYZE SELECT id FROM t WHERE a = 3",
    ];
    println!("{:<52} | {:<10} | executed access path", "shape", "via");
    let describe = |decisions: &[bicdb_sql::plan_diagnostics::DecisionTrace]| {
        decisions
            .iter()
            .map(|d| {
                format!(
                    "{}:{}{}",
                    d.chooser,
                    d.candidates[d.chosen].signature.label(),
                    if d.rows_fetched.is_some() { "*" } else { "" }
                )
            })
            .collect::<Vec<_>>()
            .join(" ")
    };
    for sql in shapes {
        let mut session = SqlSession::new(&mut db);
        let (result, decisions) =
            with_plan_diagnostics(DiagnosticsOptions::default(), || session.execute(sql));
        result.unwrap();
        println!("{sql:<52} | session    | {}", describe(&decisions));
        drop(session);
        let engine = SqlEngine::new(&db);
        let (result, decisions) =
            with_plan_diagnostics(DiagnosticsOptions::default(), || engine.execute(sql));
        result.unwrap();
        println!("{sql:<52} | engine     | {}", describe(&decisions));
        let mut session = SqlSession::new(&mut db);
        session.execute("BEGIN").unwrap();
        session
            .execute("INSERT INTO t VALUES (100000, 3, 3, 'new')")
            .unwrap();
        let (result, decisions) =
            with_plan_diagnostics(DiagnosticsOptions::default(), || session.execute(sql));
        result.unwrap();
        session.execute("ROLLBACK").unwrap();
        println!("{sql:<52} | write-tx   | {}", describe(&decisions));
    }
}
