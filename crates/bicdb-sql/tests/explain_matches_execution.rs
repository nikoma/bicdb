//! EXPLAIN must describe the access path that execution actually uses.
//!
//! For each single-table SELECT shape this compares the access path EXPLAIN
//! reports (first plan line) with the access path recorded while the same
//! statement really executes (opt-in `plan_diagnostics` trace), with and
//! without ANALYZE statistics, and inside a transaction with pending writes.

use bicdb_core::{BicDb, DbConfig, StorageMode};
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
    let in_list = lines.iter().any(|line| line.starts_with("InListKeys"));
    let kind = match head {
        "IndexScan" if in_list => "IndexInLookup",
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
        "FullScan" | "Empty" | "PrimaryKeyInLookup" | "PrimaryKeyLookup" | "BitmapOr" => None,
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
    "SELECT id FROM t WHERE a IN (8, 9)",
    "SELECT id FROM t WHERE a = 8 OR id = 5",
    "SELECT id FROM t WHERE a = 3 AND b = 2 AND d > 4",
    "SELECT id FROM t WHERE id = 7",
    "SELECT id FROM t WHERE id IN (7, 8)",
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
            Some(actual) if family(&actual) == family(&(kind.clone(), index.clone())) => {}
            actual => mismatches.push(format!(
                "[{condition}] {sql}\n    EXPLAIN says {kind} {index:?} ({lines:?})\n    executed    {actual:?}\n    trace {decisions:?}"
            )),
        }
    }
    mismatches
}

/// Primary-key point, list and prefix reads are one access path (the
/// primary key) reached by different routes; other kinds compare exactly.
fn family(access: &(String, Option<String>)) -> (String, Option<String>) {
    match access.0.as_str() {
        "PrimaryKeyLookup"
        | "PrimaryKeyInLookup"
        | "PrimaryKeyLocator"
        | "PrimaryKeyPrefixLookup" => ("PrimaryKey".to_string(), None),
        _ => access.clone(),
    }
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

/// The scan nodes of a PostgreSQL-layout plan, as `(node, index)` pairs:
/// `Bitmap Heap Scan`, `Bitmap Index Scan t_a`, `Index Scan t_a`, ...
fn pg_layout_scans(session: &mut SqlSession<'_>, sql: &str) -> Vec<(String, Option<String>)> {
    session
        .execute(&format!("EXPLAIN (COSTS OFF) {sql}"))
        .unwrap_or_else(|error| panic!("EXPLAIN (COSTS OFF) {sql}: {error}"))
        .rows
        .into_iter()
        .filter_map(|row| {
            let line = row[0].to_cell();
            let line = line.trim().trim_start_matches("->").trim().to_string();
            for node in [
                "Bitmap Heap Scan",
                "Bitmap Index Scan",
                "BitmapOr",
                "Index Only Scan",
                "Index Scan Backward",
                "Index Scan",
                "Seq Scan",
                "Result",
            ] {
                if let Some(rest) = line.strip_prefix(node) {
                    let index = match node {
                        "Bitmap Index Scan" => rest.strip_prefix(" on ").map(str::to_string),
                        "Index Scan" | "Index Scan Backward" | "Index Only Scan" => rest
                            .strip_prefix(" using ")
                            .and_then(|rest| rest.split(' ').next())
                            .map(str::to_string),
                        _ => None,
                    };
                    return Some((node.to_string(), index));
                }
            }
            None
        })
        .collect()
}

/// The PostgreSQL scan nodes that truthfully describe an executed access
/// path. The row-set locator (and the pending-writes chooser) collect every
/// matching row id from the index before fetching rows in row-id order: a
/// Bitmap Heap Scan over a Bitmap Index Scan (a BitmapOr when it unions OR
/// branches). A full-key equality on a unique index fetches at most one row,
/// an Index Scan. The ordered routes read in index order: an Index Scan.
fn pg_nodes_for(
    decisions: &[DecisionTrace],
    unique: &[&str],
) -> Option<Vec<(String, Option<String>)>> {
    if decisions.iter().any(|decision| decision.contradiction) {
        return Some(vec![("Result".into(), None)]);
    }
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
    let collects = matches!(decision.chooser, "locator" | "pending-writes");
    let pkey = Some("t_pkey".to_string());
    let bitmap = |index: Option<String>| {
        vec![
            ("Bitmap Heap Scan".to_string(), None),
            ("Bitmap Index Scan".to_string(), index),
        ]
    };
    Some(match signature.kind {
        "FullScan" => vec![("Seq Scan".into(), None)],
        "PrimaryKeyInLookup" if signature.prefix_len == 0 && !collects => {
            vec![("Result".into(), None)]
        }
        "PrimaryKeyLookup"
        | "PrimaryKeyInLookup"
        | "PrimaryKeyLocator"
        | "PrimaryKeyPrefixLookup" => {
            vec![("Index Scan".into(), pkey)]
        }
        "IndexLookup"
            if collects
                && signature
                    .index
                    .as_deref()
                    .is_some_and(|index| unique.contains(&index)) =>
        {
            vec![("Index Scan".into(), signature.index.clone())]
        }
        "IndexLookup" | "IndexRange" if collects => bitmap(signature.index.clone()),
        "IndexInLookup" => bitmap(signature.index.clone()),
        "BitmapOr" => {
            let mut nodes = vec![
                ("Bitmap Heap Scan".to_string(), None),
                ("BitmapOr".to_string(), None),
            ];
            for part in &decision.parts {
                nodes.push((
                    "Bitmap Index Scan".to_string(),
                    match part.kind {
                        "PrimaryKeyLocator" | "PrimaryKeyInLookup" => pkey.clone(),
                        _ => part.index.clone(),
                    },
                ));
            }
            nodes
        }
        "IndexLookup" | "IndexRange" | "IndexPrefixOrderedScan" | "OrderedIndexScan" => {
            vec![("Index Scan".into(), signature.index.clone())]
        }
        "JsonbIndexScan" | "ArrayIndexScan" | "GeometricIndexScan" | "SpatialIndexScan"
        | "FullTextIndexScan" | "TrigramIndexScan" => bitmap(signature.index.clone()),
        other => vec![(other.to_string(), signature.index.clone())],
    })
}

fn check_pg_layout(db: &mut BicDb, condition: &str, in_write_tx: bool) -> Vec<String> {
    let mut mismatches = Vec::new();
    for sql in SHAPES {
        let mut session = SqlSession::new(db);
        if in_write_tx {
            session.execute("BEGIN").unwrap();
            session
                .execute("INSERT INTO t VALUES (100000, 3, 3, 'new', 100000, 5, point(1, 1), '{\"k\": 3}', ARRAY[3])")
                .unwrap();
        }
        let shown = pg_layout_scans(&mut session, sql);
        let (result, decisions) =
            with_plan_diagnostics(DiagnosticsOptions::default(), || session.execute(sql));
        result.unwrap_or_else(|error| panic!("{sql}: {error}"));
        if in_write_tx {
            session.execute("ROLLBACK").unwrap();
        }
        match pg_nodes_for(&decisions, &[]) {
            Some(expected) if expected == shown => {}
            expected => mismatches.push(format!(
                "[{condition}] {sql}\n    EXPLAIN (COSTS OFF) shows {shown:?}\n    executed means   {expected:?}\n    trace {decisions:?}"
            )),
        }
    }
    mismatches
}

/// The PostgreSQL layout's scan nodes are the executed access path, named
/// by what that path does (see [`pg_nodes_for`]).
#[test]
fn postgres_layout_names_the_executed_access_path() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    setup(&mut db);
    let mut mismatches = check_pg_layout(&mut db, "no stats", false);
    SqlSession::new(&mut db).execute("ANALYZE t").unwrap();
    mismatches.extend(check_pg_layout(&mut db, "after ANALYZE", false));
    mismatches.extend(check_pg_layout(&mut db, "pending writes", true));
    assert!(
        mismatches.is_empty(),
        "{} EXPLAIN/execution mismatches:\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

const PAGED_SHAPES: &[&str] = &[
    "SELECT id, c FROM t WHERE a = 3",
    "SELECT id FROM t WHERE a = 3 AND b = 2",
    "SELECT id FROM t WHERE a = 0",
    "SELECT id FROM t WHERE a >= 0",
    "SELECT id FROM t WHERE a > 7",
    "SELECT id FROM t WHERE d BETWEEN 10 AND 20",
    "SELECT id FROM t WHERE d > 10",
    "SELECT id FROM t WHERE c = 'x5'",
    "SELECT count(*) FROM t WHERE a = 0",
    "SELECT id FROM t WHERE a = 3 ORDER BY b LIMIT 5",
    "SELECT id FROM t WHERE a = 0 ORDER BY c",
    "SELECT id FROM t ORDER BY c",
    "SELECT id FROM t WHERE a = 3 OR d = 2",
];

#[test]
fn explain_reports_the_executed_access_path_in_paged_storage() {
    // Paged storage adds the streaming routes: when the cost-based planner
    // picks a full scan, a lazily paged collection streams record batches
    // instead of going through the row-set locator. Lazy paging needs a
    // fresh open and only durable index kinds (B-tree here).
    let root = tempfile::tempdir().unwrap();
    let config = || {
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged)
    };
    {
        let mut db = BicDb::open_with_config(root.path(), config()).unwrap();
        SqlSession::new(&mut db)
            .execute(
                "CREATE TABLE t (id int4 PRIMARY KEY, a int4, b int4, c text, d int4);
                 INSERT INTO t SELECT g, CASE WHEN g % 10 < 8 THEN 0 ELSE g % 10 END, g % 7, 'x' || g, g
                   FROM generate_series(1, 6000) g;
                 CREATE INDEX t_a ON t (a);
                 CREATE INDEX t_ab ON t (a, b);
                 CREATE INDEX t_d ON t (d);
                 ANALYZE t;",
            )
            .unwrap();
        db.close().unwrap();
    }
    let mut db = BicDb::open_with_config(root.path(), config()).unwrap();
    assert!(
        db.record_batch_streaming_possible("t"),
        "fixture must exercise the streaming routes"
    );
    let mut mismatches = Vec::new();
    for sql in PAGED_SHAPES {
        let mut session = SqlSession::new(&mut db);
        let (kind, index, lines) = explained(&mut session, sql);
        let (result, decisions) =
            with_plan_diagnostics(DiagnosticsOptions::default(), || session.execute(sql));
        result.unwrap_or_else(|error| panic!("{sql}: {error}"));
        println!(
            "{sql:<48} EXPLAIN {kind} {index:?} | trace {:?}",
            decisions
                .iter()
                .map(|d| format!(
                    "{}:{}{}",
                    d.chooser,
                    d.candidates[d.chosen].signature.label(),
                    if d.rows_fetched.is_some() { "*" } else { "" }
                ))
                .collect::<Vec<_>>()
        );
        match executed(&decisions) {
            Some(actual) if actual == (kind.clone(), index.clone()) => {}
            actual => mismatches.push(format!(
                "[paged] {sql}\n    EXPLAIN says {kind} {index:?} ({lines:?})\n    executed    {actual:?}\n    trace {decisions:?}"
            )),
        }
    }
    assert!(
        mismatches.is_empty(),
        "{} EXPLAIN/execution mismatches:\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

const JOIN_SHAPES: &[&str] = &[
    "SELECT t.id, u.v FROM t JOIN u ON u.tid = t.id WHERE t.a = 3",
    "SELECT t.id, u.v FROM t JOIN u ON u.id = t.d WHERE t.a = 3",
    "SELECT t.id, u.v FROM t JOIN u ON u.x = t.b WHERE t.a = 9",
    "SELECT t.id, u.v FROM t LEFT JOIN u ON u.x = t.b WHERE t.a = 8",
    "SELECT t.id, u.v FROM t LEFT JOIN u ON u.tid = t.id WHERE t.a = 8",
    "SELECT t.id, u.v FROM t, u WHERE u.tid = t.id AND t.a = 9",
    "SELECT t.id, u.v FROM t, u WHERE u.x = t.b AND t.a = 9",
    "SELECT t.id, u.v FROM t JOIN u ON u.x < t.b WHERE t.a = 9 AND u.id < 5",
    "SELECT t.id, u.v, w.id FROM t JOIN u ON u.tid = t.id JOIN t w ON w.id = u.x WHERE t.a = 9",
];

/// The join methods a PostgreSQL-layout plan shows, innermost (first
/// executed) join first.
fn pg_layout_joins(session: &mut SqlSession<'_>, sql: &str) -> Vec<String> {
    let lines = session
        .execute(&format!("EXPLAIN (COSTS OFF) {sql}"))
        .unwrap_or_else(|error| panic!("EXPLAIN (COSTS OFF) {sql}: {error}"))
        .rows
        .into_iter()
        .map(|row| row[0].to_cell())
        .collect::<Vec<_>>();
    let mut joins = Vec::new();
    for (position, line) in lines.iter().enumerate() {
        let text = line.trim().trim_start_matches("->").trim();
        let depth = line.len() - line.trim_start().len();
        if text.starts_with("Hash") && text.contains("Join") {
            joins.push("HashJoin".to_string());
        } else if text.starts_with("Nested Loop") {
            // An indexed nested loop's inner scan carries an Index Cond
            // referencing the outer side; it is this node's second child.
            let children = lines[position + 1..]
                .iter()
                .take_while(|child| child.len() - child.trim_start().len() > depth)
                .collect::<Vec<_>>();
            // A nested loop over stored inner rows shows them under a
            // Materialize; one that looks the inner rows up per outer row
            // has its index scan directly below.
            let materialized = children
                .iter()
                .any(|child| child.trim().trim_start_matches("->").trim() == "Materialize");
            joins.push(
                if materialized {
                    "NestedLoop"
                } else {
                    "IndexNestedLoop"
                }
                .to_string(),
            );
        }
    }
    joins.reverse();
    joins
}

/// The join node EXPLAIN prints for each join is the method the executor
/// used for it.
#[test]
fn postgres_layout_names_the_executed_join_method() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    setup(&mut db);
    SqlSession::new(&mut db)
        .execute(
            "CREATE TABLE u (id int4 PRIMARY KEY, tid int4, x int4, v text);
             INSERT INTO u SELECT g, g % 3000, g % 11, 'v' || g FROM generate_series(1, 4000) g;
             CREATE INDEX u_tid ON u (tid);",
        )
        .unwrap();
    let mut mismatches = Vec::new();
    for condition in ["no stats", "after ANALYZE"] {
        if condition == "after ANALYZE" {
            SqlSession::new(&mut db)
                .execute("ANALYZE t; ANALYZE u")
                .unwrap();
        }
        for sql in JOIN_SHAPES {
            let mut session = SqlSession::new(&mut db);
            let shown = pg_layout_joins(&mut session, sql);
            let (result, decisions) =
                with_plan_diagnostics(DiagnosticsOptions::default(), || session.execute(sql));
            result.unwrap_or_else(|error| panic!("{sql}: {error}"));
            let executed = decisions
                .iter()
                .filter(|decision| decision.chooser == "join")
                .map(|decision| {
                    decision.candidates[decision.chosen]
                        .signature
                        .kind
                        .to_string()
                })
                .collect::<Vec<_>>();
            if shown != executed {
                let trace = decisions
                    .iter()
                    .map(|decision| {
                        format!(
                            "{}:{}:{}",
                            decision.chooser,
                            decision.collection,
                            decision.candidates[decision.chosen].signature.label()
                        )
                    })
                    .collect::<Vec<_>>();
                mismatches.push(format!(
                    "[{condition}] {sql}\n    EXPLAIN shows {shown:?}\n    executed      {executed:?}\n    trace {trace:?}"
                ));
            }
        }
    }
    assert!(
        mismatches.is_empty(),
        "{} join mismatches:\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

const SUBPLAN_SHAPES: &[&str] = &[
    "SELECT id FROM t WHERE a IN (SELECT x FROM u WHERE id < 50)",
    "SELECT id FROM t WHERE a NOT IN (SELECT x FROM u WHERE id < 50) AND d < 40",
    "SELECT id FROM t WHERE c IN (SELECT v FROM u WHERE id < 10)",
    "SELECT id FROM t WHERE id IN (SELECT tid FROM u WHERE x = 3) AND a = 9",
    "SELECT id FROM t WHERE a IN (SELECT x FROM u WHERE u.tid = t.id) AND d < 40",
    "SELECT id FROM t WHERE a IN (SELECT x FROM u WHERE id < 50 LIMIT 3)",
    "SELECT id FROM t WHERE a IN (SELECT x + 1 FROM u WHERE id < 50)",
    "SELECT count(*) FROM t WHERE b IN (SELECT x FROM u)",
    "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.tid = t.id) AND a = 9",
    "SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.tid = t.id AND u.x = 3) AND a = 9",
    "SELECT count(*) FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.x = t.b) OR d < 3",
    "SELECT id FROM t WHERE EXISTS (SELECT u.v FROM u WHERE u.tid = t.id) AND a = 9",
    "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.tid = t.id LIMIT 1) AND a = 9",
    "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.tid = t.id AND random() < 2) AND a = 9",
];

/// EXPLAIN names a SubPlan `hashed` exactly when execution probed the
/// subquery's result through a hash table.
#[test]
fn postgres_layout_names_hashed_subplans_as_executed() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    setup(&mut db);
    SqlSession::new(&mut db)
        .execute(
            "CREATE TABLE u (id int4 PRIMARY KEY, tid int4, x int4, v text);
             INSERT INTO u SELECT g, g % 3000, g % 11, 'x' || g FROM generate_series(1, 4000) g;",
        )
        .unwrap();
    let mut mismatches = Vec::new();
    for sql in SUBPLAN_SHAPES {
        let mut session = SqlSession::new(&mut db);
        let plan = session
            .execute(&format!("EXPLAIN (COSTS OFF) {sql}"))
            .unwrap_or_else(|error| panic!("EXPLAIN {sql}: {error}"))
            .rows
            .into_iter()
            .map(|row| row[0].to_cell())
            .collect::<Vec<_>>();
        let shown = plan.iter().any(|line| {
            line.contains("hashed SubPlan")
                || line.contains("Hash Semi Join")
                || line.contains("Hash Anti Join")
        });
        let (result, decisions) =
            with_plan_diagnostics(DiagnosticsOptions::default(), || session.execute(sql));
        result.unwrap_or_else(|error| panic!("{sql}: {error}"));
        let hashed = decisions.iter().any(|decision| {
            decision.chooser == "subplan"
                && matches!(
                    decision.candidates[decision.chosen].signature.kind,
                    "HashedSubPlan" | "HashedExists"
                )
        });
        if shown != hashed {
            let trace = decisions
                .iter()
                .map(|decision| {
                    format!(
                        "{}:{}:{}",
                        decision.chooser,
                        decision.collection,
                        decision.candidates[decision.chosen].signature.label()
                    )
                })
                .collect::<Vec<_>>();
            mismatches.push(format!(
                "{sql}\n    EXPLAIN {plan:?}\n    hashed at execution: {hashed}\n    trace {trace:?}"
            ));
        }
    }
    assert!(
        mismatches.is_empty(),
        "{} subplan mismatches:\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}
