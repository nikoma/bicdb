//! Comma joins over catalog relations must join on their WHERE predicates.
//! Catalog relations have no stored `TableSchema`, so the WHERE-derived join
//! constraint and the comma-join ordering both skipped them: driver
//! foreign-key metadata (PgJDBC `getImportedKeys`/`getExportedKeys`) ran as a
//! Cartesian product over pg_namespace, pg_class, pg_attribute,
//! pg_constraint, generate_series, pg_class and pg_depend.
use super::*;

/// PgJDBC 42.7.8 `getImportedExportedKeys` (server >= 11), parameters inlined.
const DRIVER_KEYS_SQL: &str = "SELECT current_database() AS \"PKTABLE_CAT\", pkn.nspname AS \"PKTABLE_SCHEM\", pkc.relname AS \"PKTABLE_NAME\", pka.attname AS \"PKCOLUMN_NAME\", current_database() AS \"FKTABLE_CAT\", fkn.nspname AS \"FKTABLE_SCHEM\", fkc.relname AS \"FKTABLE_NAME\", fka.attname AS \"FKCOLUMN_NAME\", pos.n AS \"KEY_SEQ\", CASE con.confupdtype  WHEN 'c' THEN 0 WHEN 'n' THEN 2 WHEN 'd' THEN 4 WHEN 'r' THEN 1 WHEN 'p' THEN 1 WHEN 'a' THEN 3 ELSE NULL END AS \"UPDATE_RULE\", CASE con.confdeltype  WHEN 'c' THEN 0 WHEN 'n' THEN 2 WHEN 'd' THEN 4 WHEN 'r' THEN 1 WHEN 'p' THEN 1 WHEN 'a' THEN 3 ELSE NULL END AS \"DELETE_RULE\", con.conname AS \"FK_NAME\", pkic.relname AS \"PK_NAME\", CASE  WHEN con.condeferrable AND con.condeferred THEN 5 WHEN con.condeferrable THEN 6 ELSE 7 END AS \"DEFERRABILITY\"  FROM  pg_catalog.pg_namespace pkn, pg_catalog.pg_class pkc, pg_catalog.pg_attribute pka,  pg_catalog.pg_namespace fkn, pg_catalog.pg_class fkc, pg_catalog.pg_attribute fka,  pg_catalog.pg_constraint con,  pg_catalog.generate_series(1, 32) pos(n),  pg_catalog.pg_class pkic, pg_catalog.pg_depend dep  WHERE pkn.oid = pkc.relnamespace AND pkc.oid = pka.attrelid AND pka.attnum = con.confkey[pos.n] AND con.confrelid = pkc.oid  AND fkn.oid = fkc.relnamespace AND fkc.oid = fka.attrelid AND fka.attnum = con.conkey[pos.n] AND con.conrelid = fkc.oid  AND con.contype = 'f' AND (pkic.relkind = 'i' OR pkic.relkind = 'I') AND con.oid = dep.objid AND pkic.oid = dep.refobjid AND dep.classid = 'pg_constraint'::regclass::oid AND dep.refclassid = 'pg_class'::regclass::oid  AND pkic.oid = con.conindid ";

fn profiled(session: &mut SqlSession<'_>, sql: &str) -> (Vec<Vec<String>>, usize) {
    crate::SQL_PROFILE_STACK
        .with(|stack| stack.borrow_mut().push(crate::SqlProfileStats::default()));
    let result = session.execute(sql);
    let stats = crate::SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());
    let rows = result
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .into_iter()
        .map(|row| row.into_iter().map(|value| value.to_cell()).collect())
        .collect();
    (rows, stats.join_candidate_pairs)
}

fn count(session: &mut SqlSession<'_>, sql: &str) -> usize {
    session.execute(sql).unwrap().rows[0][0]
        .to_cell()
        .parse()
        .unwrap()
}

fn keyed_schema() -> (tempfile::TempDir, BicDb) {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path(),
        bicdb_core::DbConfig::default().with_fsync(false),
    )
    .unwrap();
    let mut session = SqlSession::new(&mut db);
    for table in 0..8 {
        session
            .execute(&format!(
                "CREATE TABLE p{table} (id INT PRIMARY KEY, uu TEXT UNIQUE, a INT, b INT)"
            ))
            .unwrap();
    }
    for table in 0..8 {
        let next = (table + 1) % 8;
        session
            .execute(&format!(
                "CREATE TABLE k{table} (id INT PRIMARY KEY, p_id INT, q_uu TEXT, x INT, \
                 CONSTRAINT k{table}_p FOREIGN KEY (p_id) REFERENCES p{table} (id), \
                 CONSTRAINT k{table}_q FOREIGN KEY (q_uu) REFERENCES p{next} (uu) \
                 ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED)"
            ))
            .unwrap();
    }
    drop(session);
    (root, db)
}

/// Without a WHERE-derived constraint every step was a cross product, so the
/// pairs examined grew with the product of the catalog sizes.
#[test]
fn catalog_comma_joins_use_where_predicates_as_join_keys() {
    let (_root, mut db) = keyed_schema();
    let mut session = SqlSession::new(&mut db);
    let classes = count(&mut session, "SELECT count(*) FROM pg_catalog.pg_class");
    let attributes = count(&mut session, "SELECT count(*) FROM pg_catalog.pg_attribute");
    let (rows, pairs) = profiled(
        &mut session,
        "SELECT c.relname, a.attname FROM pg_catalog.pg_class c, pg_catalog.pg_namespace n, \
         pg_catalog.pg_attribute a WHERE n.oid = c.relnamespace AND c.oid = a.attrelid \
         AND n.nspname = 'public' AND c.relname = 'k3' AND a.attnum > 0 ORDER BY a.attname",
    );
    assert_eq!(
        rows,
        [["k3", "id"], ["k3", "p_id"], ["k3", "q_uu"], ["k3", "x"]]
            .map(|row| row.map(str::to_string).to_vec())
            .to_vec()
    );
    // Joined on its keys each step examines at most one pair per row of the
    // relation it adds; unjoined, the classes met every attribute row.
    assert!(
        pairs <= classes + attributes,
        "catalog join examined {pairs} pairs ({classes} classes, {attributes} attributes)"
    );
}

/// The FK row names the parent's enforcing index and depends on it, as the
/// driver query joins through both.
#[test]
fn foreign_keys_record_the_referenced_unique_index() {
    let (_root, mut db) = keyed_schema();
    let mut session = SqlSession::new(&mut db);
    let (rows, _) = profiled(
        &mut session,
        "SELECT con.conname, i.relname, \
         (SELECT count(*) FROM pg_catalog.pg_depend d WHERE d.objid = con.oid \
          AND d.refobjid = con.conindid AND d.classid = 'pg_constraint'::regclass::oid \
          AND d.refclassid = 'pg_class'::regclass::oid) \
         FROM pg_catalog.pg_constraint con JOIN pg_catalog.pg_class i ON i.oid = con.conindid \
         WHERE con.conname IN ('k3_p', 'k3_q') ORDER BY con.conname",
    );
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert_eq!(rows[0][0], "k3_p");
    assert_eq!(rows[0][1], "p3_pkey");
    assert_eq!(rows[0][2], "1");
    assert_eq!(rows[1][0], "k3_q");
    assert_ne!(rows[1][1], "p4_pkey");
    assert_eq!(rows[1][2], "1");
}

#[test]
fn driver_imported_and_exported_keys_are_reported_without_a_cross_product() {
    let (_root, mut db) = keyed_schema();
    let mut session = SqlSession::new(&mut db);
    let classes = count(&mut session, "SELECT count(*) FROM pg_catalog.pg_class");
    let attributes = count(&mut session, "SELECT count(*) FROM pg_catalog.pg_attribute");
    // Cheap guard: the unfixed planner turns the driver query below into a
    // multi-billion-row product, so fail here instead of exhausting memory.
    let (_, pairs) = profiled(
        &mut session,
        "SELECT count(*) FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c, \
         pg_catalog.pg_attribute a \
         WHERE n.oid = c.relnamespace AND c.oid = a.attrelid AND n.nspname = 'public'",
    );
    assert!(
        pairs <= classes + attributes,
        "namespace/class/attribute join examined {pairs} pairs"
    );

    let columns = |rows: Vec<Vec<String>>| {
        rows.into_iter()
            // PKTABLE_NAME, PKCOLUMN_NAME, FKTABLE_NAME, FKCOLUMN_NAME,
            // KEY_SEQ, DELETE_RULE, FK_NAME, DEFERRABILITY
            .map(|row| {
                [2, 3, 6, 7, 8, 10, 11, 13]
                    .map(|at| row[at].clone())
                    .to_vec()
            })
            .collect::<Vec<_>>()
    };
    let (imported, imported_pairs) = profiled(
        &mut session,
        &format!(
            "{DRIVER_KEYS_SQL} AND fkn.nspname = 'public' AND fkc.relname = 'k3' \
             ORDER BY pkn.nspname,pkc.relname, con.conname,pos.n"
        ),
    );
    assert_eq!(
        columns(imported),
        vec![
            ["p3", "id", "k3", "p_id", "1", "3", "k3_p", "7"]
                .map(str::to_string)
                .to_vec(),
            ["p4", "uu", "k3", "q_uu", "1", "2", "k3_q", "5"]
                .map(str::to_string)
                .to_vec(),
        ]
    );
    let (exported, exported_pairs) = profiled(
        &mut session,
        &format!(
            "{DRIVER_KEYS_SQL} AND pkn.nspname = 'public' AND pkc.relname = 'p3' \
             ORDER BY fkn.nspname,fkc.relname,con.conname,pos.n"
        ),
    );
    assert_eq!(
        columns(exported),
        vec![
            ["p3", "uu", "k2", "q_uu", "1", "2", "k2_q", "5"]
                .map(str::to_string)
                .to_vec(),
            ["p3", "id", "k3", "p_id", "1", "3", "k3_p", "7"]
                .map(str::to_string)
                .to_vec(),
        ]
    );
    // Joined on its predicates the work is a few pairs per key column per
    // generate_series step; the Cartesian form was billions.
    for pairs in [imported_pairs, exported_pairs] {
        assert!(pairs < 100_000, "driver key query examined {pairs} pairs");
    }
}
