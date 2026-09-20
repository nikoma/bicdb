//! Proves the cell-row read path is the one actually taken for the TPC-C
//! statement shapes (point lookup, index-located set, procedure SELECT INTO,
//! UPDATE candidates' neighbours), not a silent fallback: rows are built from
//! cells and the per-statement record parse counter stays at zero.

use super::*;
use bicdb_core::DbConfig;

fn seeded_db(dir: &tempfile::TempDir) -> BicDb {
    let mut db =
        BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute(
            "CREATE TABLE stock (s_i_id INT NOT NULL, s_w_id INT NOT NULL, s_quantity INT, \
             s_dist_01 CHAR(24), s_ytd NUMERIC, s_price NUMERIC(5,2), s_since TIMESTAMP, \
             s_data VARCHAR(50), PRIMARY KEY (s_w_id, s_i_id))",
        )
        .unwrap();
        for i in 1..=6 {
            sql.execute(&format!(
                "INSERT INTO stock VALUES ({i}, 1, {}, 'dist{i}', {i}00.5, {i}.25, \
                 TIMESTAMP '2026-09-02 05:28:0{i}', 'd{i}')",
                10 + i
            ))
            .unwrap();
        }
        sql.execute("CREATE INDEX stock_qty ON stock (s_quantity)")
            .unwrap();
        sql.execute(
            "CREATE OR REPLACE PROCEDURE read_stock(w INT, i INT, INOUT price NUMERIC) \
             LANGUAGE plpgsql AS $$
             BEGIN
               SELECT s_price INTO price FROM stock WHERE s_w_id = w AND s_i_id = i;
             END $$",
        )
        .unwrap();
    }
    db
}

#[test]
fn point_lookups_and_index_sets_build_rows_from_cells() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = seeded_db(&dir);
    let mut sql = SqlSession::new(&mut db);

    reset_sql_cell_row_counts();
    reset_sql_record_storage_get_calls();
    let rows = sql
        .execute("SELECT s_price, s_since FROM stock WHERE s_w_id = 1 AND s_i_id = 4")
        .unwrap()
        .rows;
    assert_eq!(rows[0][0], SqlValue::String("4.25".to_string()));
    assert_eq!(
        rows[0][1],
        SqlValue::String("2026-09-02 05:28:04".to_string())
    );
    assert_eq!(
        sql_cell_row_counts(),
        (1, 0),
        "point lookup must come from cells"
    );
    assert_eq!(
        sql_record_storage_get_calls(),
        0,
        "no record parse for a point lookup"
    );

    reset_sql_cell_row_counts();
    reset_sql_record_storage_get_calls();
    let rows = sql
        .execute("SELECT s_i_id FROM stock WHERE s_quantity > 13 ORDER BY s_i_id")
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 3);
    // The index range may over-approximate (boundary candidates are re-checked
    // by the predicate): every candidate must still come from cells.
    let (fast, fallback) = sql_cell_row_counts();
    assert!(fast >= 3, "index-located rows must come from cells: {fast}");
    assert_eq!(fallback, 0);
    assert_eq!(sql_record_storage_get_calls(), 0);

    reset_sql_cell_row_counts();
    let rows = sql.execute("CALL read_stock(1, 2, NULL)").unwrap().rows;
    assert_eq!(rows[0][0], SqlValue::String("2.25".to_string()));
    assert_eq!(
        sql_cell_row_counts().0,
        1,
        "procedure SELECT INTO must come from cells"
    );

    // Join right-table fetches (prepared pk join) come from cells too.
    sql.execute("CREATE TABLE warehouse (w_id INT PRIMARY KEY, w_tax NUMERIC(4,4))")
        .unwrap();
    sql.execute("INSERT INTO warehouse VALUES (1, 0.0750)")
        .unwrap();
    reset_sql_cell_row_counts();
    reset_sql_record_storage_get_calls();
    let rows = sql
        .execute(
            "SELECT s.s_price, w.w_tax FROM stock s, warehouse w \
             WHERE w.w_id = 1 AND s.s_w_id = w.w_id AND s.s_i_id = 3",
        )
        .unwrap()
        .rows;
    assert_eq!(rows[0][1], SqlValue::String("0.0750".to_string()));
    let (fast, fallback) = sql_cell_row_counts();
    assert!(
        fast >= 2,
        "left and right rows must come from cells: {fast}"
    );
    assert_eq!(fallback, 0);
    assert_eq!(
        sql_record_storage_get_calls(),
        0,
        "no record parse in the join"
    );

    // A row this transaction rewrote is a stored-form pending write (typed
    // resident rows phase 3): it is served from its cells like its committed
    // neighbours, so nothing falls back to a parsed record.
    sql.execute("BEGIN").unwrap();
    sql.execute("UPDATE stock SET s_quantity = 1 WHERE s_w_id = 1 AND s_i_id = 5")
        .unwrap();
    reset_sql_cell_row_counts();
    let rows = sql
        .execute("SELECT s_i_id, s_quantity FROM stock WHERE s_quantity < 13 ORDER BY s_i_id")
        .unwrap()
        .rows;
    assert_eq!(
        rows.iter().map(|row| row[0].to_cell()).collect::<Vec<_>>(),
        vec!["1", "2", "5"]
    );
    let (fast, fallback) = sql_cell_row_counts();
    assert!(
        fast >= 3,
        "committed and rewritten candidates all come from cells: {fast}"
    );
    assert_eq!(
        fallback, 0,
        "the rewritten row is read from its stored form"
    );
    sql.execute("ROLLBACK").unwrap();
}

#[test]
fn secondary_index_beats_primary_key_prefix_without_probing_the_prefix() {
    // customer pk (w, d, id); customer_i2 (w, d, last, first). A lookup by
    // (w, d, last) must probe the secondary index only: the pk prefix (w, d)
    // used to be collected eagerly — every customer of the district — and
    // then discarded when the longer secondary prefix won.
    let dir = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE TABLE customer (c_id INT NOT NULL, c_d_id INT NOT NULL, c_w_id INT NOT NULL, \
         c_first VARCHAR(16), c_last VARCHAR(16), c_balance NUMERIC(12,2), \
         PRIMARY KEY (c_w_id, c_d_id, c_id))",
    )
    .unwrap();
    sql.execute("CREATE INDEX customer_i2 ON customer (c_w_id, c_d_id, c_last, c_first)")
        .unwrap();
    for i in 1..=60 {
        sql.execute(&format!(
            "INSERT INTO customer VALUES ({i}, 1, 1, 'F{i}', 'BAR{}', {i}.5)",
            i % 4
        ))
        .unwrap();
    }
    reset_sql_index_storage_lookup_calls();
    let rows = sql
        .execute(
            "SELECT c_id, c_first FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_last = 'BAR2' \
             ORDER BY c_first",
        )
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 15);
    assert_eq!(
        sql_index_storage_lookup_calls(),
        1,
        "exactly one index probe (the secondary index), no pk prefix collection"
    );
    // Prefix-only predicates still use the pk prefix (nothing better).
    reset_sql_index_storage_lookup_calls();
    let rows = sql
        .execute("SELECT count(*) FROM customer WHERE c_w_id = 1 AND c_d_id = 1")
        .unwrap()
        .rows;
    assert_eq!(rows[0][0], SqlValue::Int(60));
}

#[test]
fn routine_statements_bind_variables_to_frame_slots_without_snapshots() {
    // Embedded statements of a compiled routine bind variables to the frame's
    // slots: no per-statement snapshot of the variable map, and a value
    // assigned between statements is what the next statement reads. Dynamic
    // (non-slot) variable names keep the snapshot form.
    let dir = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut sql = SqlSession::new(&mut db);
    // `t` is driven by the routine, `t2` by the same statements replayed
    // outside it; both must evolve identically.
    for table in ["t", "t2"] {
        sql.execute(&format!(
            "CREATE TABLE {table} (id INT PRIMARY KEY, grp INT, amount NUMERIC(8,2), tag TEXT)"
        ))
        .unwrap();
        for i in 1..=30 {
            sql.execute(&format!(
                "INSERT INTO {table} VALUES ({i}, {}, {i}.25, 'tag{}')",
                i % 3,
                i % 5
            ))
            .unwrap();
        }
    }
    sql.execute(
        "CREATE PROCEDURE walk(g INT, INOUT total NUMERIC, INOUT hits INT, INOUT last_tag TEXT) \
         LANGUAGE plpgsql AS $$
         DECLARE i INT; lim INT; tg TEXT;
         BEGIN
           total := 0; hits := 0;
           FOR i IN 1 .. 5 LOOP
             lim := i * 4;
             tg := 'tag' || (i % 5);
             SELECT count(*), coalesce(sum(amount), 0) INTO hits, total \
               FROM t WHERE grp = g AND id <= lim AND tag <> tg;
             UPDATE t SET amount = amount + i WHERE grp = g AND id = lim;
           END LOOP;
           SELECT tag INTO last_tag FROM t WHERE id = lim;
         END $$",
    )
    .unwrap();
    let expect = |sql: &mut SqlSession, g: i64| -> Vec<SqlValue> {
        // Same loop, spelled out statement by statement.
        let mut hits = SqlValue::Null;
        let mut total = SqlValue::Null;
        for i in 1..=5 {
            let lim = i * 4;
            let tg = format!("tag{}", i % 5);
            let row = sql
                .execute(&format!(
                    "SELECT count(*), coalesce(sum(amount), 0) FROM t2 \
                     WHERE grp = {g} AND id <= {lim} AND tag <> '{tg}'"
                ))
                .unwrap()
                .rows
                .remove(0);
            hits = row[0].clone();
            total = row[1].clone();
            sql.execute(&format!(
                "UPDATE t2 SET amount = amount + {i} WHERE grp = {g} AND id = {lim}"
            ))
            .unwrap();
        }
        let tag = sql
            .execute("SELECT tag FROM t2 WHERE id = 20")
            .unwrap()
            .rows
            .remove(0)
            .remove(0);
        vec![total, hits, tag]
    };
    for round in 0..3 {
        for g in [0, 1, 2] {
            reset_sql_routine_var_snapshots();
            let rows = sql
                .execute(&format!("CALL walk({g}, NULL, NULL, NULL)"))
                .unwrap()
                .rows;
            assert_eq!(
                sql_routine_var_snapshots(),
                0,
                "round {round} g={g}: compiled routine statements snapshot no variables"
            );
            let expected = expect(&mut sql, g);
            assert_eq!(rows[0], expected, "round {round} g={g}");
            let (a, b) = (
                sql.execute("SELECT id, amount FROM t ORDER BY id")
                    .unwrap()
                    .rows,
                sql.execute("SELECT id, amount FROM t2 ORDER BY id")
                    .unwrap()
                    .rows,
            );
            assert_eq!(a, b, "round {round} g={g}: both tables evolve identically");
        }
    }
}

#[test]
fn record_id_locators_reuse_the_strategy_per_routine_node_and_follow_index_ddl() {
    // A routine SELECT after an UPDATE of the same table takes the record-id
    // locator path (pending writes). It used to re-derive the index analysis
    // on every call; the strategy is now memoized per IR node and refreshed
    // when the index catalog changes. Results must match the derived walk in
    // every case, including the pending-write merge.
    let dir = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE TABLE customer (c_id INT NOT NULL, c_d_id INT NOT NULL, c_w_id INT NOT NULL, \
         c_first VARCHAR(16), c_last VARCHAR(16), c_balance NUMERIC(12,2), \
         PRIMARY KEY (c_w_id, c_d_id, c_id))",
    )
    .unwrap();
    for i in 1..=40 {
        sql.execute(&format!(
            "INSERT INTO customer VALUES ({i}, 1, 1, 'F{i}', 'BAR{}', {i}.5)",
            i % 4
        ))
        .unwrap();
    }
    sql.execute(
        "CREATE PROCEDURE touch(w INT, d INT, last VARCHAR, INOUT n INT, INOUT total NUMERIC) \
         LANGUAGE plpgsql AS $$
         BEGIN
           UPDATE customer SET c_balance = c_balance + 1 WHERE c_w_id = w AND c_d_id = d AND c_id = 1;
           SELECT count(*), sum(c_balance) INTO n, total FROM customer \
             WHERE c_last = last AND c_d_id = d AND c_w_id = w;
         END $$",
    )
    .unwrap();
    let expect = |sql: &mut SqlSession, last: &str| -> Vec<SqlValue> {
        sql.execute(&format!(
            "SELECT count(*), sum(c_balance) FROM customer WHERE c_last = '{last}' AND c_d_id = 1 AND c_w_id = 1"
        ))
        .unwrap()
        .rows
        .remove(0)
    };
    for round in 0..3 {
        if round == 1 {
            // Round 0 derived every statement's strategy (the routine's
            // UPDATE and SELECT, the check query); later rounds derive none.
            reset_sql_locator_strategy_derivations();
        }
        for last in ["BAR1", "BAR2"] {
            let rows = sql
                .execute(&format!("CALL touch(1, 1, '{last}', NULL, NULL)"))
                .unwrap()
                .rows;
            // The UPDATE inside the call ran before the SELECT, so the
            // expectation is read afterwards (same committed state).
            assert_eq!(rows[0], expect(&mut sql, last), "round {round} {last}");
        }
    }
    assert_eq!(
        sql_locator_strategy_derivations(),
        0,
        "strategies are derived once per node, not per call"
    );
    // Index DDL refreshes the strategy: the new secondary index must be used
    // (fewer records located than the pk prefix) and the answer unchanged.
    sql.execute("CREATE INDEX customer_i2 ON customer (c_w_id, c_d_id, c_last, c_first)")
        .unwrap();
    reset_sql_locator_strategy_derivations();
    reset_sql_index_storage_lookup_calls();
    let rows = sql
        .execute("CALL touch(1, 1, 'BAR3', NULL, NULL)")
        .unwrap()
        .rows;
    assert_eq!(rows[0], expect(&mut sql, "BAR3"));
    assert!(
        sql_locator_strategy_derivations() >= 1,
        "index DDL must re-derive"
    );
    let rows = sql
        .execute("CALL touch(1, 1, 'BAR3', NULL, NULL)")
        .unwrap()
        .rows;
    assert_eq!(rows[0], expect(&mut sql, "BAR3"));
}

#[test]
fn routine_point_deletes_take_the_stored_path_and_match_the_generic_one() {
    // DELIVERY's `DELETE FROM new_order WHERE <full pk>` inside a routine is
    // served by id from the resident row: no Record parse, no catalog scan.
    // Tables with anything that needs the row's contents (row triggers,
    // referencing foreign keys) and statements with RETURNING keep the
    // generic path; both must delete exactly the same rows.
    let dir = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut sql = SqlSession::new(&mut db);
    for table in ["new_order", "audited", "parent"] {
        sql.execute(&format!(
            "CREATE TABLE {table} (no_o_id INT NOT NULL, no_d_id INT NOT NULL, no_w_id INT NOT NULL, \
             note TEXT, PRIMARY KEY (no_w_id, no_d_id, no_o_id))"
        ))
        .unwrap();
        for o in 1..=6 {
            sql.execute(&format!(
                "INSERT INTO {table} VALUES ({o}, 1, 1, 'n{o}'), ({o}, 2, 1, 'm{o}')"
            ))
            .unwrap();
        }
    }
    sql.execute("CREATE TABLE audit_log (seq SERIAL PRIMARY KEY, note TEXT)")
        .unwrap();
    sql.execute(
        "CREATE FUNCTION log_delete() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN INSERT INTO audit_log (note) VALUES (OLD.note); RETURN OLD; END $$",
    )
    .unwrap();
    sql.execute(
        "CREATE TRIGGER audited_delete AFTER DELETE ON audited FOR EACH ROW EXECUTE FUNCTION log_delete()",
    )
    .unwrap();
    sql.execute(
        "CREATE TABLE child (id INT PRIMARY KEY, p_o INT, p_d INT, p_w INT, \
         FOREIGN KEY (p_w, p_d, p_o) REFERENCES parent (no_w_id, no_d_id, no_o_id) ON DELETE CASCADE)",
    )
    .unwrap();
    sql.execute("INSERT INTO child VALUES (1, 1, 1, 1), (2, 2, 2, 1)")
        .unwrap();
    for table in ["new_order", "audited", "parent"] {
        sql.execute(&format!(
            "CREATE PROCEDURE del_{table}(o INT, d INT, w INT, INOUT n TEXT) LANGUAGE plpgsql AS $$
             BEGIN
               DELETE FROM {table} WHERE no_o_id = o AND no_d_id = d AND no_w_id = w;
               SELECT count(*)::text INTO n FROM {table} WHERE no_d_id = d AND no_w_id = w;
             END $$"
        ))
        .unwrap();
    }
    sql.execute(
        "CREATE PROCEDURE del_returning(o INT, d INT, w INT, INOUT n TEXT) LANGUAGE plpgsql AS $$
         DECLARE r TEXT;
         BEGIN
           DELETE FROM new_order WHERE no_o_id = o AND no_d_id = d AND no_w_id = w RETURNING note INTO r;
           n := coalesce(r, 'none');
         END $$",
    )
    .unwrap();
    let cells = |rows: &[Vec<SqlValue>]| -> Vec<Vec<String>> {
        rows.iter()
            .map(|row| row.iter().map(|value| value.to_cell()).collect())
            .collect()
    };
    let count = |sql: &mut SqlSession, table: &str, d: i64| -> Vec<SqlValue> {
        sql.execute(&format!(
            "SELECT count(*)::text FROM {table} WHERE no_d_id = {d} AND no_w_id = 1"
        ))
        .unwrap()
        .rows
        .remove(0)
    };
    // Stored path: the plain table, called repeatedly (first call builds the
    // point template, later calls hit it), including a missing row.
    reset_sql_stored_delete_hits();
    for (o, d) in [(1, 1), (2, 1), (2, 1), (9, 1), (3, 2)] {
        let rows = sql
            .execute(&format!("CALL del_new_order({o}, {d}, 1, NULL)"))
            .unwrap()
            .rows;
        assert_eq!(rows[0], count(&mut sql, "new_order", d), "o={o} d={d}");
    }
    assert!(
        sql_stored_delete_hits() >= 3,
        "routine point deletes take the stored path ({} hits)",
        sql_stored_delete_hits()
    );
    assert_eq!(
        count(&mut sql, "new_order", 1)[0],
        SqlValue::String("4".into())
    );
    assert_eq!(
        count(&mut sql, "new_order", 2)[0],
        SqlValue::String("5".into())
    );
    // Generic path: trigger table logs the deleted note; FK parent cascades.
    reset_sql_stored_delete_hits();
    let rows = sql.execute("CALL del_audited(1, 1, 1, NULL)").unwrap().rows;
    assert_eq!(rows[0], count(&mut sql, "audited", 1));
    assert_eq!(
        cells(&sql.execute("SELECT note FROM audit_log").unwrap().rows),
        vec![vec!["n1".to_string()]]
    );
    let rows = sql.execute("CALL del_parent(1, 1, 1, NULL)").unwrap().rows;
    assert_eq!(rows[0], count(&mut sql, "parent", 1));
    assert_eq!(
        cells(
            &sql.execute("SELECT id FROM child ORDER BY id")
                .unwrap()
                .rows
        ),
        vec![vec!["2".to_string()]]
    );
    let rows = sql
        .execute("CALL del_returning(4, 1, 1, NULL)")
        .unwrap()
        .rows;
    assert_eq!(cells(&rows)[0], vec!["n4".to_string()]);
    let rows = sql
        .execute("CALL del_returning(4, 1, 1, NULL)")
        .unwrap()
        .rows;
    assert_eq!(cells(&rows)[0], vec!["none".to_string()]);
    assert_eq!(
        sql_stored_delete_hits(),
        0,
        "trigger/FK/RETURNING deletes stay generic"
    );
    // A plain (non-routine) DELETE keeps working through the generic path.
    let rows = sql
        .execute("DELETE FROM new_order WHERE no_o_id = 5 AND no_d_id = 1 AND no_w_id = 1")
        .unwrap()
        .rows;
    assert!(rows.is_empty());
    assert_eq!(
        count(&mut sql, "new_order", 1)[0],
        SqlValue::String("2".into())
    );
}

#[test]
fn catalog_aware_expression_typing_is_memoized_inside_routines() {
    // CASE / ARRAY / cast-to-user-type / to_timestamp shapes are typed by the
    // catalog-aware typer, now memoized per IR node inside a routine. Repeated
    // calls must keep answering like the direct statements, and a user type
    // created after the first call must be seen (the scope covers the
    // user-type catalog generation).
    let dir = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE TABLE t (id INT PRIMARY KEY, amount NUMERIC(8,2), tag TEXT, seen TIMESTAMP)",
    )
    .unwrap();
    sql.execute("INSERT INTO t VALUES (1, 10.5, 'a', NULL), (2, 20, 'b', NULL)")
        .unwrap();
    sql.execute(
        "CREATE PROCEDURE p(stamp TEXT, INOUT a TEXT, INOUT b TEXT, INOUT c TEXT, INOUT d TEXT) \
         LANGUAGE plpgsql AS $$
         DECLARE ts TIMESTAMP;
         BEGIN
           ts := TO_TIMESTAMP(stamp, 'YYYYMMDDHH24MISS')::timestamp without time zone;
           UPDATE t SET seen = ts, tag = CASE WHEN amount > 15 THEN 'big' ELSE tag END WHERE id <= 2;
           SELECT (CASE WHEN amount > 15 THEN amount ELSE 0 END)::text INTO a FROM t WHERE id = 1;
           SELECT (ARRAY[amount, 1])[1]::text INTO b FROM t WHERE id = 2;
           SELECT tag || ':' || seen::text INTO c FROM t WHERE id = 2;
           d := ts::text;
         END $$",
    )
    .unwrap();
    for round in 0..3 {
        let rows = sql
            .execute("CALL p('20260903180000', NULL, NULL, NULL, NULL)")
            .unwrap()
            .rows;
        let expected = sql
            .execute(
                "SELECT (CASE WHEN amount > 15 THEN amount ELSE 0 END)::text, \
                        (SELECT (ARRAY[amount, 1])[1]::text FROM t WHERE id = 2), \
                        (SELECT tag || ':' || seen::text FROM t WHERE id = 2), \
                        TO_TIMESTAMP('20260903180000', 'YYYYMMDDHH24MISS')::timestamp without time zone::text \
                 FROM t WHERE id = 1",
            )
            .unwrap()
            .rows;
        assert_eq!(rows[0], expected[0], "round {round}");
    }
    // A user type created after the routine ran is typed correctly by a
    // routine cast that names it.
    sql.execute("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')")
        .unwrap();
    sql.execute(
        "CREATE PROCEDURE q(INOUT m TEXT) LANGUAGE plpgsql AS $$
         BEGIN m := ('ok'::mood)::text || '/' || (CASE WHEN true THEN 'happy'::mood END)::text; END $$",
    )
    .unwrap();
    let rows = sql.execute("CALL q(NULL)").unwrap().rows;
    assert_eq!(rows[0][0], SqlValue::String("ok/happy".into()));
    sql.execute("ALTER TYPE mood RENAME TO feeling").unwrap();
    sql.execute(
        "CREATE OR REPLACE PROCEDURE q(INOUT m TEXT) LANGUAGE plpgsql AS $$
         BEGIN m := ('ok'::feeling)::text; END $$",
    )
    .unwrap();
    let rows = sql.execute("CALL q(NULL)").unwrap().rows;
    assert_eq!(rows[0][0], SqlValue::String("ok".into()));
}

#[test]
fn routine_select_result_typing_is_memoized_per_node_and_invalidated_by_ddl() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut db = seeded_db(&dir);
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE PROCEDURE q(w INT, i INT, INOUT qty INT, INOUT dist TEXT) LANGUAGE plpgsql AS $$
         BEGIN SELECT s_quantity, s_dist_01 INTO qty, dist FROM stock \
         WHERE s_w_id = w AND s_i_id = i; END $$",
    )
    .unwrap();
    let hits = || crate::engine::materialize::SQL_RESULT_TYPING_MEMO_HITS.with(|h| *h.borrow());
    let before = hits();
    let first = sql.execute("CALL q(1, 1, NULL, NULL)").unwrap().rows;
    let after_first = hits();
    sql.execute("CALL q(1, 2, NULL, NULL)").unwrap();
    sql.execute("CALL q(1, 3, NULL, NULL)").unwrap();
    let after_three = hits();
    assert!(
        after_three >= after_first + 2,
        "repeat executions hit the typing memo"
    );
    // A catalog change bumps the generation: the next execution recomputes.
    sql.execute("ALTER TABLE stock ADD COLUMN s_note TEXT")
        .unwrap();
    let before_ddl_call = hits();
    let again = sql.execute("CALL q(1, 1, NULL, NULL)").unwrap().rows;
    assert_eq!(
        hits(),
        before_ddl_call,
        "first execution after DDL recomputes"
    );
    assert_eq!(first, again);
    sql.execute("CALL q(1, 1, NULL, NULL)").unwrap();
    assert!(hits() > before_ddl_call, "then the memo is warm again");
    let _ = before;
}

#[test]
fn joins_convert_only_referenced_right_columns_unless_using_or_natural() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut db =
        BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE TABLE cust (c_w_id INT, c_d_id INT, c_id INT, c_first VARCHAR(16), \
         c_last VARCHAR(16), c_credit CHAR(2), c_discount NUMERIC(4,4), c_balance NUMERIC(12,2), \
         c_data VARCHAR(500), c_phone CHAR(16), PRIMARY KEY (c_w_id, c_d_id, c_id))",
    )
    .unwrap();
    sql.execute("CREATE TABLE wh (w_id INT PRIMARY KEY, w_tax NUMERIC(4,4), w_name VARCHAR(10), w_city VARCHAR(20))")
        .unwrap();
    sql.execute("INSERT INTO wh VALUES (1, 0.0750, 'main', 'here')")
        .unwrap();
    for i in 1..=5 {
        sql.execute(&format!(
            "INSERT INTO cust VALUES (1, 1, {i}, 'F{i}', 'L{i}', 'GC', 0.05, {i}.5, 'data{i}', '555')"
        ))
        .unwrap();
    }
    let fields = || SQL_SLOT_ROW_FIELDS.with(|count| *count.borrow());
    let query = "SELECT c_discount, c_last, c_credit, w_tax FROM cust, wh \
                 WHERE wh.w_id = 1 AND cust.c_w_id = 1 AND cust.c_d_id = 1 AND cust.c_id = 3";
    let before = fields();
    let rows = sql.execute(query).unwrap().rows;
    let converted = fields() - before;
    assert_eq!(
        rows.iter()
            .map(|row| row.iter().map(SqlValue::to_cell).collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        vec![vec!["0.0500", "L3", "GC", "0.0750"]]
    );
    // cust has 10 columns and wh 4: with pushdown the right-hand rows carry
    // only the referenced ones (c_w_id, c_d_id, c_id, c_discount, c_last,
    // c_credit / w_id, w_tax), never all 14.
    assert!(
        converted > 0 && converted < 14,
        "converted {converted} fields"
    );
    // USING names columns outside any expression: every field is kept.
    sql.execute(
        "CREATE TABLE wh2 (c_w_id INT PRIMARY KEY, w_tax NUMERIC(4,4), w_name VARCHAR(10))",
    )
    .unwrap();
    sql.execute("INSERT INTO wh2 VALUES (1, 0.0800, 'two')")
        .unwrap();
    let before = fields();
    let rows = sql
        .execute("SELECT c_last, w_tax FROM cust JOIN wh2 USING (c_w_id) WHERE c_id = 2")
        .unwrap()
        .rows;
    assert_eq!(rows[0][0].to_cell(), "L2");
    assert_eq!(rows[0][1].to_cell(), "0.0800");
    let converted_using = fields() - before;
    assert!(
        converted_using >= 10 + 3 || converted_using == 0,
        "USING join keeps every field, converted {converted_using}"
    );
}

#[test]
fn routine_point_lookups_use_ir_keyed_plans_and_recompute_after_ddl() {
    // The SELECT plans are opt-in (`BICDB_IR_PLAN_CACHE`); this test exercises them.
    crate::ir_plan_cache::set_test_override(Some(true));
    let dir = tempfile::TempDir::new().unwrap();
    let mut db = seeded_db(&dir);
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE PROCEDURE q(w INT, i INT, INOUT qty INT, INOUT dist TEXT, INOUT n INT) \
         LANGUAGE plpgsql AS $$
         BEGIN
           SELECT s_quantity, s_dist_01 INTO qty, dist FROM stock WHERE s_w_id = w AND s_i_id = i;
           SELECT count(*) INTO n FROM stock WHERE s_w_id = w AND s_quantity > 0;
         END $$",
    )
    .unwrap();
    let hits = || crate::planner::SQL_IR_PLAN_HITS.with(|h| *h.borrow());
    let oracle = |sql: &mut SqlSession<'_>, i: i64| {
        sql.execute(&format!(
            "SELECT s_quantity, s_dist_01 FROM stock WHERE s_w_id = 1 AND s_i_id = {i}"
        ))
        .unwrap()
        .rows
    };
    let expected = oracle(&mut sql, 2);
    let before = hits();
    let first = sql.execute("CALL q(1, 2, NULL, NULL, NULL)").unwrap().rows;
    assert_eq!(first[0][0], expected[0][0]);
    assert_eq!(first[0][1], expected[0][1]);
    sql.execute("CALL q(1, 3, NULL, NULL, NULL)").unwrap();
    sql.execute("CALL q(1, 4, NULL, NULL, NULL)").unwrap();
    assert!(
        hits() >= before + 2,
        "repeat executions hit the IR-keyed plan: {} -> {}",
        before,
        hits()
    );
    // Values still track the key on hits.
    let expected4 = oracle(&mut sql, 4);
    let again = sql.execute("CALL q(1, 4, NULL, NULL, NULL)").unwrap().rows;
    assert_eq!(again[0][0], expected4[0][0]);
    assert_eq!(again[0][1], expected4[0][1]);
    // DDL moves the generation: the next execution replans (no hit), then hits again.
    sql.execute("ALTER TABLE stock ADD COLUMN s_note TEXT")
        .unwrap();
    let before_ddl = hits();
    let after_ddl = sql.execute("CALL q(1, 2, NULL, NULL, NULL)").unwrap().rows;
    assert_eq!(after_ddl[0][0], expected[0][0]);
    let replanned = hits();
    sql.execute("CALL q(1, 2, NULL, NULL, NULL)").unwrap();
    assert!(hits() > replanned, "warm again after the replan");
    let _ = before_ddl;
    // Recompiling the routine (new IR) replans too and stays correct.
    sql.execute(
        "CREATE OR REPLACE PROCEDURE q(w INT, i INT, INOUT qty INT, INOUT dist TEXT, INOUT n INT) \
         LANGUAGE plpgsql AS $$
         BEGIN
           SELECT s_quantity, s_dist_01 INTO qty, dist FROM stock WHERE s_w_id = w AND s_i_id = i;
           n := 7;
         END $$",
    )
    .unwrap();
    let rows = sql.execute("CALL q(1, 5, NULL, NULL, NULL)").unwrap().rows;
    let expected5 = oracle(&mut sql, 5);
    assert_eq!(rows[0][0], expected5[0][0]);
    assert_eq!(rows[0][2], SqlValue::Int(7));
    crate::ir_plan_cache::set_test_override(None);
}

#[test]
fn routine_primary_key_updates_use_ir_keyed_plans() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut db = seeded_db(&dir);
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE PROCEDURE bump(w INT, i INT, d INT, INOUT after INT) LANGUAGE plpgsql AS $$
         BEGIN
           UPDATE stock SET s_quantity = s_quantity + d WHERE s_i_id = i AND s_w_id = w;
           SELECT s_quantity INTO after FROM stock WHERE s_w_id = w AND s_i_id = i;
         END $$",
    )
    .unwrap();
    let hits = || crate::planner::SQL_IR_UPDATE_PLAN_HITS.with(|h| *h.borrow());
    let qty = |sql: &mut SqlSession<'_>, i: i64| -> SqlValue {
        sql.execute(&format!(
            "SELECT s_quantity FROM stock WHERE s_w_id = 1 AND s_i_id = {i}"
        ))
        .unwrap()
        .rows[0][0]
            .clone()
    };
    let start = qty(&mut sql, 2);
    let SqlValue::Int(start) = start else {
        panic!("int quantity")
    };
    let before = hits();
    let r1 = sql.execute("CALL bump(1, 2, 5, NULL)").unwrap().rows;
    assert_eq!(r1[0][0], SqlValue::Int(start + 5));
    let r2 = sql.execute("CALL bump(1, 2, -3, NULL)").unwrap().rows;
    assert_eq!(r2[0][0], SqlValue::Int(start + 2));
    assert_eq!(qty(&mut sql, 2), SqlValue::Int(start + 2));
    assert!(hits() > before, "the second execution hits the UPDATE plan");
    // A NULL key touches nothing (and the SELECT INTO finds no row).
    let r3 = sql.execute("CALL bump(1, NULL, 100, NULL)").unwrap().rows;
    assert_eq!(r3[0][0], SqlValue::Null);
    assert_eq!(qty(&mut sql, 2), SqlValue::Int(start + 2));
    // DDL replans; the update still lands.
    sql.execute("ALTER TABLE stock ADD COLUMN s_note2 TEXT")
        .unwrap();
    let r4 = sql.execute("CALL bump(1, 2, 1, NULL)").unwrap().rows;
    assert_eq!(r4[0][0], SqlValue::Int(start + 3));
    // A non-key predicate stays on the general path and is correct.
    sql.execute(
        "CREATE PROCEDURE bump_all(w INT, d INT) LANGUAGE plpgsql AS $$
         BEGIN UPDATE stock SET s_quantity = s_quantity + d WHERE s_w_id = w AND s_quantity > 0; \
         END $$",
    )
    .unwrap();
    sql.execute("CALL bump_all(1, 1)").unwrap();
    sql.execute("CALL bump_all(1, 1)").unwrap();
    assert_eq!(qty(&mut sql, 2), SqlValue::Int(start + 5));
}

#[test]
fn routine_batch_updates_from_unnest_use_ir_keyed_plans() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut db = seeded_db(&dir);
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE PROCEDURE take(w INT, ids INT[], qs INT[]) LANGUAGE plpgsql AS $$
         BEGIN
           UPDATE stock SET s_quantity = s_quantity - line.qty
           FROM UNNEST(ids, qs) AS line (item_id, qty)
           WHERE stock.s_i_id = line.item_id AND stock.s_w_id = w;
         END $$",
    )
    .unwrap();
    let hits = || crate::planner::SQL_IR_UPDATE_PLAN_HITS.with(|h| *h.borrow());
    let qty = |sql: &mut SqlSession<'_>, i: i64| -> i64 {
        match sql
            .execute(&format!(
                "SELECT s_quantity FROM stock WHERE s_w_id = 1 AND s_i_id = {i}"
            ))
            .unwrap()
            .rows[0][0]
        {
            SqlValue::Int(v) => v,
            ref other => panic!("int quantity, got {other:?}"),
        }
    };
    let (q2, q3, q5) = (qty(&mut sql, 2), qty(&mut sql, 3), qty(&mut sql, 5));
    let before = hits();
    sql.execute("CALL take(1, ARRAY[2, 5], ARRAY[1, 2])")
        .unwrap();
    sql.execute("CALL take(1, ARRAY[3, 2], ARRAY[4, 1])")
        .unwrap();
    assert!(
        hits() > before,
        "the second execution hits the batch UPDATE plan"
    );
    assert_eq!(qty(&mut sql, 2), q2 - 2);
    assert_eq!(qty(&mut sql, 3), q3 - 4);
    assert_eq!(qty(&mut sql, 5), q5 - 2);
    // A NULL key element updates nothing for that row; the others land.
    sql.execute("CALL take(1, ARRAY[NULL, 5], ARRAY[9, 1])")
        .unwrap();
    assert_eq!(qty(&mut sql, 2), q2 - 2);
    assert_eq!(qty(&mut sql, 5), q5 - 3);
    // An unknown key touches nothing.
    sql.execute("CALL take(1, ARRAY[999999], ARRAY[1])")
        .unwrap();
    assert_eq!(qty(&mut sql, 2), q2 - 2);
    // DDL replans and the arithmetic still lands.
    sql.execute("ALTER TABLE stock ADD COLUMN s_note3 TEXT")
        .unwrap();
    sql.execute("CALL take(1, ARRAY[2], ARRAY[1])").unwrap();
    assert_eq!(qty(&mut sql, 2), q2 - 3);
}

#[test]
fn routine_update_assignments_bound_per_node_match_the_generic_evaluator() {
    // Same procedures, same starting rows: the generic evaluator (gate off)
    // and the IR-bound assignments (gate on) must leave identical rows for
    // every assignment shape, including the shapes the binder declines.
    fn run(enabled: bool) -> Vec<Vec<String>> {
        crate::ir_update_plan::set_test_override(Some(enabled));
        let dir = tempfile::TempDir::new().unwrap();
        let mut db =
            BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
        let mut sql = SqlSession::new(&mut db);
        sql.execute(
            "CREATE TABLE acct (id INT PRIMARY KEY, bal NUMERIC(12,2), disc NUMERIC(4,4), \
             cnt INT, big BIGINT, f FLOAT8, tag TEXT, credit CHAR(2), seen TIMESTAMPTZ, \
             data VARCHAR(30))",
        )
        .unwrap();
        sql.execute(
            "INSERT INTO acct VALUES (1, 10.00, 0.0500, 3, 9000000000, 1.5, 'a', 'BC', \
             '2026-09-02 05:28:05+00', 'hello')",
        )
        .unwrap();
        sql.execute(
            "CREATE PROCEDURE p(i INT, amt NUMERIC, n INT, t TEXT) LANGUAGE plpgsql AS $$
             BEGIN
               UPDATE acct SET bal = bal + amt, cnt = cnt + n, big = big + n,
                 f = f * 2.5 - n, tag = tag || t || tag, disc = disc * 2,
                 data = CASE WHEN credit = 'BC' THEN substring(data || t FROM 1 FOR 8) ELSE data END,
                 seen = seen + interval '1 day'
               WHERE id = i;
               UPDATE acct SET bal = bal * (1 + disc) - amt / 4, cnt = -cnt + 7 WHERE id = i;
             END $$",
        )
        .unwrap();
        for k in 0..3 {
            sql.execute(&format!("CALL p(1, {k}.25, {k}, 'x{k}')"))
                .unwrap();
        }
        let rows = sql
            .execute("SELECT bal, disc, cnt, big, f, tag, credit, seen, data FROM acct")
            .unwrap()
            .rows;
        crate::ir_update_plan::set_test_override(None);
        rows.iter()
            .map(|row| row.iter().map(SqlValue::to_cell).collect())
            .collect()
    }
    let generic = run(false);
    let before = crate::planner::SQL_IR_BOUND_ASSIGNMENT_HITS.with(|h| *h.borrow());
    let bound = run(true);
    let hits = crate::planner::SQL_IR_BOUND_ASSIGNMENT_HITS.with(|h| *h.borrow());
    assert_eq!(generic, bound);
    assert!(
        hits > before,
        "bound assignments were used on repeat executions"
    );
    assert_eq!(generic[0][2], "3");
}

#[test]
fn routine_insert_values_templates_match_the_generic_path() {
    // Same procedures, same inputs: the generic INSERT path (gate off) and
    // the per-IR-node insert template (gate on) must produce identical rows
    // and identical errors — composite and omitted primary keys, a table
    // without a primary key, static defaults, JSON NULL absence, NOT NULL,
    // type, CHECK and unique violations, unbound expressions
    // (current_timestamp) and a BEFORE INSERT trigger that rewrites NEW.
    fn run(enabled: bool) -> (Vec<Vec<String>>, Vec<String>) {
        crate::ir_insert_plan::set_test_override(Some(enabled));
        let dir = tempfile::TempDir::new().unwrap();
        let mut db =
            BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
        let mut sql = SqlSession::new(&mut db);
        for ddl in [
            "CREATE TABLE ol (o INT, d INT, n INT, qty INT NOT NULL, amt NUMERIC(6,2), \
             info CHAR(24), PRIMARY KEY (o, d, n))",
            "CREATE TABLE hist (c INT, w INT, amount NUMERIC(6,2), at TIMESTAMPTZ, \
             data VARCHAR(24) DEFAULT 'none', extra JSONB)",
            "CREATE TABLE acct (id INT PRIMARY KEY, bal NUMERIC(12,2) CHECK (bal >= 0), \
             tag TEXT NOT NULL, seen TIMESTAMPTZ)",
            "CREATE TABLE trg (id INT PRIMARY KEY, v INT NOT NULL)",
            "CREATE FUNCTION trg_fn() RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN NEW.v := NEW.v * 10; RETURN NEW; END $$",
            "CREATE TRIGGER trg_before BEFORE INSERT ON trg FOR EACH ROW EXECUTE FUNCTION trg_fn()",
            "CREATE PROCEDURE ins_ol(o INT, d INT, n INT, q INT, a NUMERIC, i TEXT) \
             LANGUAGE plpgsql AS $$ BEGIN \
             INSERT INTO ol (o, d, n, qty, amt, info) VALUES (o, d, n, q, a, i); END $$",
            "CREATE PROCEDURE ins_hist(c INT, w INT, a NUMERIC, e JSONB) \
             LANGUAGE plpgsql AS $$ BEGIN \
             INSERT INTO hist (c, w, amount, at, extra) VALUES (c, w, a, current_timestamp, e); \
             END $$",
            "CREATE PROCEDURE ins_acct(i INT, b NUMERIC, t TEXT) LANGUAGE plpgsql AS $$ BEGIN \
             INSERT INTO acct (id, bal, tag) VALUES (i, b, t); END $$",
            "CREATE PROCEDURE ins_trg(i INT, v INT) LANGUAGE plpgsql AS $$ BEGIN \
             INSERT INTO trg VALUES (i, v); END $$",
            "CREATE PROCEDURE ins_two(i INT) LANGUAGE plpgsql AS $$ BEGIN \
             INSERT INTO acct (id, bal, tag) VALUES (i, 1, 'a'), (i + 1, 2, 'b'); END $$",
        ] {
            sql.execute(ddl).unwrap();
        }
        let mut errors = Vec::new();
        for call in [
            "CALL ins_ol(1, 2, 3, 5, 12.5, 'abc')",
            "CALL ins_ol(1, 2, 4, 6, 0.25, NULL)",
            "CALL ins_hist(1, 2, 3.5, '{\"k\": 1}')",
            "CALL ins_hist(2, 2, 4.5, NULL)",
            "CALL ins_acct(1, 10, 'x')",
            "CALL ins_acct(2, 20, 'y')",
            "CALL ins_trg(1, 4)",
            "CALL ins_two(10)",
            // errors: NOT NULL, CHECK, duplicate key, missing pk, bad type
            "CALL ins_ol(9, 9, 9, NULL, 1, 'z')",
            "CALL ins_acct(3, -1, 'neg')",
            "CALL ins_acct(1, 5, 'dup')",
            "CALL ins_acct(NULL, 5, 'nopk')",
            "CALL ins_acct(4, 5, NULL)",
            "CALL ins_trg(2, NULL)",
        ] {
            if let Err(error) = sql.execute(call) {
                errors.push(format!("{call}: {error}"));
            }
        }
        let mut rows = Vec::new();
        for query in [
            "SELECT o, d, n, qty, amt, info FROM ol ORDER BY o, d, n",
            "SELECT c, w, amount, data, extra, CASE WHEN at IS NULL THEN 0 ELSE 1 END FROM hist ORDER BY c",
            "SELECT id, bal, tag, seen FROM acct ORDER BY id",
            "SELECT id, v FROM trg ORDER BY id",
        ] {
            for row in sql.execute(query).unwrap().rows {
                rows.push(row.iter().map(SqlValue::to_cell).collect::<Vec<_>>());
            }
        }
        crate::ir_insert_plan::set_test_override(None);
        (rows, errors)
    }
    let generic = run(false);
    let before = crate::planner::SQL_IR_INSERT_PLAN_HITS.with(|h| *h.borrow());
    let templated = run(true);
    let hits = crate::planner::SQL_IR_INSERT_PLAN_HITS.with(|h| *h.borrow());
    assert_eq!(generic.0, templated.0);
    assert_eq!(generic.1, templated.1);
    assert!(
        hits > before,
        "insert templates were used on repeat executions"
    );
    assert_eq!(generic.1.len(), 6, "{:?}", generic.1);
    assert_eq!(generic.0.iter().filter(|row| row.len() == 2).count(), 1);
    assert_eq!(
        templated.0.last().unwrap()[0],
        "1",
        "the triggered table keeps its row"
    );
}

#[test]
fn routine_point_joins_use_ir_keyed_templates_and_match_the_generic_path() {
    // Same procedures, same rows: the generic join path (gate off) and the
    // point-join template (gate on) must return identical results for the
    // TPC-C customer x warehouse shape in both relation orders, comma and
    // JOIN ... ON forms, a NULL key, a missing row, and a query with a term
    // no lookup covers (which must decline the template and still be right).
    fn run(enabled: bool) -> Vec<Vec<String>> {
        crate::ir_join_plan::set_test_override(Some(enabled));
        let dir = tempfile::TempDir::new().unwrap();
        let mut db =
            BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
        let mut sql = SqlSession::new(&mut db);
        for ddl in [
            "CREATE TABLE warehouse (w_id INT PRIMARY KEY, w_tax NUMERIC(4,4), w_name VARCHAR(10))",
            "CREATE TABLE customer (c_w_id INT, c_d_id INT, c_id INT, c_discount NUMERIC(4,4), \
             c_last VARCHAR(16), c_credit CHAR(2), PRIMARY KEY (c_w_id, c_d_id, c_id))",
            "INSERT INTO warehouse VALUES (1, 0.1234, 'w1'), (2, 0.0500, 'w2')",
            "INSERT INTO customer VALUES (1, 2, 3, 0.0100, 'LAST3', 'GC'), \
             (2, 1, 1, 0.0200, 'LAST1', 'BC'), (1, 2, 4, 0.0300, 'LAST4', 'GC')",
            "CREATE FUNCTION f_comma(no_w_id INT, no_d_id INT, no_c_id INT) RETURNS TEXT \
             LANGUAGE plpgsql AS $$ DECLARE d NUMERIC; l TEXT; cr TEXT; t NUMERIC; BEGIN \
             SELECT c_discount, c_last, c_credit, w_tax INTO d, l, cr, t FROM customer, warehouse \
             WHERE warehouse.w_id = no_w_id AND customer.c_w_id = no_w_id \
             AND customer.c_d_id = no_d_id AND customer.c_id = no_c_id; \
             RETURN coalesce(d::text, '-') || '|' || coalesce(l, '-') || '|' || coalesce(cr, '-') \
             || '|' || coalesce(t::text, '-'); END $$",
            "CREATE FUNCTION f_on(no_w_id INT, no_d_id INT, no_c_id INT) RETURNS TEXT \
             LANGUAGE plpgsql AS $$ DECLARE r TEXT; BEGIN \
             SELECT w.w_name || ':' || c.c_last INTO r FROM warehouse w \
             JOIN customer c ON c.c_w_id = w.w_id \
             WHERE w.w_id = no_w_id AND c.c_d_id = no_d_id AND c.c_id = no_c_id; \
             RETURN coalesce(r, '-'); END $$",
            "CREATE FUNCTION f_residual(no_w_id INT, no_d_id INT, no_c_id INT) RETURNS TEXT \
             LANGUAGE plpgsql AS $$ DECLARE r TEXT; BEGIN \
             SELECT c_last INTO r FROM customer, warehouse WHERE w_id = no_w_id AND c_w_id = w_id \
             AND c_d_id = no_d_id AND c_id = no_c_id AND c_credit = 'GC'; \
             RETURN coalesce(r, '-'); END $$",
        ] {
            sql.execute(ddl).unwrap();
        }
        let mut rows = Vec::new();
        for query in [
            "SELECT f_comma(1, 2, 3)",
            "SELECT f_comma(1, 2, 3)",
            "SELECT f_comma(2, 1, 1)",
            "SELECT f_comma(1, 2, 9)",
            "SELECT f_comma(NULL, 2, 3)",
            "SELECT f_comma(3, 2, 3)",
            "SELECT f_on(1, 2, 4)",
            "SELECT f_on(1, 2, 4)",
            "SELECT f_on(2, 1, 1)",
            "SELECT f_on(2, 9, 9)",
            "SELECT f_residual(1, 2, 3)",
            "SELECT f_residual(2, 1, 1)",
        ] {
            for row in sql.execute(query).unwrap().rows {
                rows.push(row.iter().map(SqlValue::to_cell).collect::<Vec<_>>());
            }
        }
        crate::ir_join_plan::set_test_override(None);
        rows
    }
    let generic = run(false);
    let before = crate::planner::SQL_IR_JOIN_PLAN_HITS.with(|h| *h.borrow());
    let templated = run(true);
    let hits = crate::planner::SQL_IR_JOIN_PLAN_HITS.with(|h| *h.borrow());
    assert_eq!(generic, templated);
    assert!(
        hits > before,
        "point-join templates were used on repeat executions"
    );
    assert_eq!(generic[0][0], "0.0100|LAST3|GC|0.1234");
    assert_eq!(generic[3][0], "-|-|-|-");
    assert_eq!(generic[4][0], "-|-|-|-");
    assert_eq!(generic[6][0], "w1:LAST4");
    assert_eq!(generic[9][0], "-");
    assert_eq!(generic[10][0], "LAST3");
    assert_eq!(generic[11][0], "-");
}

#[test]
fn stored_update_assignment_shape_is_reused_and_invalidated_by_schema_changes() {
    use crate::session::txn_update::SQL_STORED_UPDATE_SHAPE_BUILDS;
    crate::stored_update::set_test_override(Some(true));
    let dir = tempfile::TempDir::new().unwrap();
    let mut db = seeded_db(&dir);
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE PROCEDURE bump_shape(n INT) LANGUAGE plpgsql AS $$ BEGIN \
         UPDATE stock SET s_quantity = s_quantity + n WHERE s_w_id = 1 AND s_i_id = 2; END $$",
    )
    .unwrap();
    sql.execute("BEGIN").unwrap();
    sql.execute("CALL bump_shape(2)").unwrap();
    // Initial catalog loading can replace the schema Arc without changing
    // its generation. Warm that up too; identity changes must rebuild safely.
    sql.execute("CALL bump_shape(2)").unwrap();
    let built = SQL_STORED_UPDATE_SHAPE_BUILDS.with(|builds| builds.get());
    assert!(built > 0);
    for _ in 0..6 {
        sql.execute("CALL bump_shape(2)").unwrap();
    }
    assert_eq!(
        SQL_STORED_UPDATE_SHAPE_BUILDS.with(|builds| builds.get()),
        built
    );
    sql.execute("COMMIT").unwrap();
    sql.execute("ALTER TABLE stock ADD COLUMN added TEXT")
        .unwrap();
    sql.execute("BEGIN").unwrap();
    sql.execute("CALL bump_shape(3)").unwrap();
    assert!(SQL_STORED_UPDATE_SHAPE_BUILDS.with(|builds| builds.get()) > built);
    sql.execute("COMMIT").unwrap();
    assert_eq!(
        sql.execute("SELECT s_quantity FROM stock WHERE s_w_id = 1 AND s_i_id = 2")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(31)]]
    );
    // Index eligibility is deliberately not part of the cached shape. A new
    // unique index must still reject a conflicting update of a warmed plan.
    sql.execute("CREATE UNIQUE INDEX stock_qty_unique ON stock (s_quantity)")
        .unwrap();
    sql.execute("BEGIN").unwrap();
    let error = sql.execute("CALL bump_shape(-18)").unwrap_err();
    assert_eq!(error.sqlstate(), "23505");
    sql.execute("ROLLBACK").unwrap();
    crate::stored_update::set_test_override(None);
}

#[test]
fn stored_returning_keeps_slot_rows_and_matches_generic_values_and_metadata() {
    fn run(enabled: bool) -> Vec<SqlResult> {
        crate::stored_update::set_test_override(Some(enabled));
        let dir = tempfile::TempDir::new().unwrap();
        let mut db = seeded_db(&dir);
        let mut sql = SqlSession::new(&mut db);
        sql.execute("BEGIN").unwrap();
        let mut results = Vec::new();
        for statement in [
            "UPDATE stock AS s SET s_quantity = s_quantity + 1 WHERE s.s_w_id = 1 AND s.s_i_id = 2 RETURNING s.s_quantity - 1 AS old_qty, s.s_data, s.s_price, s.s_since",
            "UPDATE stock SET s_quantity = s_quantity + 1 WHERE s_w_id = 1 AND s_i_id = 2 RETURNING *",
            "UPDATE stock AS s SET s_quantity = s_quantity + 1 WHERE s.s_w_id = 1 AND s.s_i_id = 2 RETURNING *",
            "UPDATE stock SET s_quantity = s_quantity + 1 WHERE s_w_id = 1 AND s_i_id = 3 RETURNING upper(s_data), s_quantity, s_dist_01",
            "UPDATE stock SET s_quantity = s_quantity + 1 WHERE s_w_id = 1 AND s_i_id = 99 RETURNING s_quantity, s_price",
        ] {
            reset_sql_slot_row_to_map_calls();
            let before = crate::session::txn_update::SQL_STORED_UPDATE_HITS.with(|h| *h.borrow());
            let result = sql.execute(statement).unwrap();
            if enabled {
                let hits = crate::session::txn_update::SQL_STORED_UPDATE_HITS.with(|h| *h.borrow());
                assert_eq!(hits, before + 1, "stored path: {statement}");
                assert_eq!(
                    sql_slot_row_to_map_calls(),
                    0,
                    "RETURNING must not rebuild maps: {statement}"
                );
            }
            results.push(result);
        }
        sql.execute("ROLLBACK").unwrap();
        crate::stored_update::set_test_override(None);
        results
    }
    assert_eq!(run(false), run(true));
}

#[test]
fn stored_form_updates_match_the_generic_path_and_survive_reopen() {
    // The stored-form UPDATE (gate on) splices the assigned columns into the
    // old row's JSON text; the generic path (gate off) parses and rebuilds
    // the record. Both must leave identical rows and raise identical errors:
    // UPDATE ... FROM (VALUES) with RETURNING, repairable no-FROM updates
    // (bicdb.update_repair), JSON NULL removal, nested JSON preserved, a NOT
    // NULL violation, a type mismatch, same-transaction visibility, and the
    // rows after commit, reopen and WAL replay.
    fn run(enabled: bool) -> (Vec<Vec<String>>, Vec<String>) {
        crate::stored_update::set_test_override(Some(enabled));
        let dir = tempfile::TempDir::new().unwrap();
        let mut errors = Vec::new();
        let mut rows = Vec::new();
        {
            let mut db =
                BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
            let mut sql = SqlSession::new(&mut db);
            for ddl in [
                "CREATE TABLE dist (w INT, d INT, next_o BIGINT NOT NULL, tax NUMERIC(4,4), \
                 ytd NUMERIC(12,2), name VARCHAR(10), seen TIMESTAMPTZ, extra JSONB, \
                 PRIMARY KEY (w, d))",
                "INSERT INTO dist VALUES (1, 1, 3001, 0.1000, 30000.00, 'd1', \
                 '2026-09-03 08:00:00+00', '{\"k\": {\"n\": [1, 2]}, \"s\": \"x\"}'), \
                 (1, 2, 3002, 0.0500, 10.50, 'd2', NULL, NULL)",
                "CREATE TABLE stock (w INT, i INT, qty INT NOT NULL, ytd NUMERIC(10,2), \
                 cnt INT, PRIMARY KEY (w, i))",
                "INSERT INTO stock VALUES (1, 10, 50, 0, 0), (1, 11, 5, 0, 0), (1, 12, 100, 0, 0)",
                "CREATE TABLE wh (w INT PRIMARY KEY, ytd NUMERIC(12,2) NOT NULL, next_o BIGINT,                  name VARCHAR(10), seen TIMESTAMPTZ, tax NUMERIC(4,4))",
                "INSERT INTO wh VALUES (1, 300000.00, 3001, 'w1', '2026-09-03 08:00:00+00', 0.1234)",
                "SET bicdb.update_repair = on",
                "CREATE TABLE wh2 (w INT PRIMARY KEY, ytd NUMERIC(12,2) NOT NULL, next_o BIGINT)",
                "INSERT INTO wh2 VALUES (1, 10.00, 5)",
                "CREATE PROCEDURE bump(p_w INT, amt NUMERIC) LANGUAGE plpgsql AS $$ BEGIN \
                 UPDATE wh2 SET ytd = ytd + amt, next_o = next_o + 1 WHERE w = p_w; END $$",
            ] {
                sql.execute(ddl).unwrap();
            }
            let statements = [
                "BEGIN",
                "UPDATE wh SET next_o = next_o + 1, ytd = ytd + 1.50 WHERE w = 1 RETURNING next_o, ytd",
                "UPDATE wh SET ytd = ytd + 2 WHERE w = 1 RETURNING name, tax",
                "UPDATE wh SET name = name || 'x', seen = '2026-09-03 09:30:00+00' WHERE w = 1",
                "SELECT w, ytd, next_o, name, seen, tax FROM wh",
                "UPDATE dist SET next_o = next_o + 1, ytd = ytd + 12.25 WHERE w = 1 AND d = 1 \
                 RETURNING next_o, ytd",
                "SELECT next_o, ytd FROM dist WHERE w = 1 AND d = 1",
                "UPDATE stock SET qty = CASE WHEN qty >= v.q + 10 THEN qty - v.q ELSE qty - v.q + 91 END, \
                 ytd = ytd + v.q, cnt = cnt + 1 FROM (VALUES (10, 7), (11, 9), (12, 1)) AS v(i, q) \
                 WHERE stock.w = 1 AND stock.i = v.i RETURNING stock.i, qty, cnt",
                "UPDATE dist SET name = name || '!', extra = NULL WHERE w = 1 AND d = 2",
                "UPDATE dist SET tax = tax + 0.0001, seen = seen + interval '1 hour' WHERE w = 1 AND d = 1",
                "COMMIT",
                "BEGIN",
                "UPDATE dist SET next_o = NULL WHERE w = 1 AND d = 1",
                "ROLLBACK",
                "BEGIN",
                "UPDATE stock SET qty = qty + 'x' FROM (VALUES (10)) AS v(i) WHERE stock.i = v.i AND stock.w = 1",
                "ROLLBACK",
                "BEGIN",
                "UPDATE dist SET ytd = ytd + 1 WHERE w = 1 AND d = 9",
                "COMMIT",
                "BEGIN",
                "CALL bump(1, 0.25)",
                "CALL bump(1, 0.75)",
                "SELECT ytd, next_o FROM wh2 WHERE w = 1",
                "COMMIT",
            ];
            for statement in statements {
                match sql.execute(statement) {
                    Ok(result) => {
                        for row in result.rows {
                            rows.push(row.iter().map(SqlValue::to_cell).collect::<Vec<_>>());
                        }
                    }
                    Err(error) => errors.push(format!("{statement}: {error}")),
                }
            }
        }
        // Reopen: the committed rows come back from the WAL / segments.
        let mut db =
            BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
        let mut sql = SqlSession::new(&mut db);
        for query in [
            "SELECT w, d, next_o, tax, ytd, name, seen, extra FROM dist ORDER BY w, d",
            "SELECT w, i, qty, ytd, cnt FROM stock ORDER BY w, i",
            "SELECT w, ytd, next_o, name, seen, tax FROM wh",
        ] {
            for row in sql.execute(query).unwrap().rows {
                rows.push(row.iter().map(SqlValue::to_cell).collect::<Vec<_>>());
            }
        }
        crate::stored_update::set_test_override(None);
        (rows, errors)
    }
    let generic = run(false);
    let before = crate::session::txn_update::SQL_STORED_UPDATE_HITS.with(|h| *h.borrow());
    let bound_before = crate::planner::SQL_IR_BOUND_ASSIGNMENT_HITS.with(|h| *h.borrow());
    let stored = run(true);
    let hits = crate::session::txn_update::SQL_STORED_UPDATE_HITS.with(|h| *h.borrow());
    let bound_hits = crate::planner::SQL_IR_BOUND_ASSIGNMENT_HITS.with(|h| *h.borrow());
    assert_eq!(generic.0, stored.0);
    assert_eq!(generic.1, stored.1);
    assert!(
        hits >= before + 5,
        "stored-form updates ran ({} hits)",
        hits - before
    );
    assert!(
        bound_hits > bound_before,
        "stored-form updates evaluate assignments through the per-node bound cache"
    );
    assert!(
        generic
            .0
            .contains(&vec!["11.00".to_string(), "7".to_string()]),
        "{:?}",
        generic.0
    );
    assert_eq!(generic.1.len(), 2, "{:?}", generic.1);
    assert_eq!(
        generic.0[0],
        vec!["3002".to_string(), "300001.50".to_string()]
    );
    assert_eq!(generic.0[1], vec!["w1".to_string(), "0.1234".to_string()]);
    assert_eq!(
        generic.0[2],
        vec![
            "1",
            "300003.50",
            "3002",
            "w1x",
            "2026-09-03 09:30:00+00",
            "0.1234"
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>(),
        "same-transaction read sees the stored-form updates"
    );
    assert_eq!(
        generic.0[3],
        vec!["3002".to_string(), "30012.25".to_string()]
    );
    assert_eq!(
        generic.0[4], generic.0[3],
        "same-transaction read sees the update"
    );
    let wh_after = generic
        .0
        .iter()
        .filter(|row| row.len() == 6)
        .last()
        .unwrap();
    assert_eq!(
        wh_after, &generic.0[2],
        "committed rows come back after reopen"
    );
    let dist_row = generic
        .0
        .iter()
        .find(|row| row.len() == 8 && row[1] == "1")
        .unwrap();
    assert_eq!(dist_row[2], "3002");
    assert_eq!(dist_row[3], "0.1001");
    assert_eq!(&dist_row[7][..4], "{\"k\"");
    let dist_row2 = generic
        .0
        .iter()
        .find(|row| row.len() == 8 && row[1] == "2")
        .unwrap();
    assert_eq!(dist_row2[5], "d2!");
    assert_eq!(dist_row2[7], "");
}

#[test]
fn stored_form_inserts_match_the_generic_path_and_survive_reopen() {
    // Routine INSERT ... VALUES inside a transaction: the stored-form path
    // (gate on) renders the row text from the template and buffers the
    // resident row; the generic path (gate off) builds records. Identical
    // rows and errors for composite, single and absent primary keys, a
    // duplicate key, NOT NULL, a missing key, a type error and a JSONB table
    // (which declines the template); same-transaction reads and reopen.
    fn run(enabled: bool) -> (Vec<Vec<String>>, Vec<String>) {
        crate::stored_update::set_test_override(Some(enabled));
        let dir = tempfile::TempDir::new().unwrap();
        let mut errors = Vec::new();
        let mut rows = Vec::new();
        {
            let mut db =
                BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
            let mut sql = SqlSession::new(&mut db);
            for ddl in [
                "CREATE TABLE ol (o INT, d INT, n INT, qty INT NOT NULL, amt NUMERIC(6,2), \
                 info CHAR(24), PRIMARY KEY (o, d, n))",
                "CREATE TABLE hist (c INT, w INT, amount NUMERIC(6,2), at TIMESTAMPTZ, \
                 data VARCHAR(24) DEFAULT 'none')",
                "CREATE TABLE acct (id INT PRIMARY KEY, bal NUMERIC(12,2), tag TEXT NOT NULL, \
                 extra JSONB)",
                "CREATE TABLE wh (w INT PRIMARY KEY, name VARCHAR(10), tax NUMERIC(4,4))",
                "CREATE PROCEDURE ins_ol(o INT, d INT, n INT, q INT, a NUMERIC, i TEXT) \
                 LANGUAGE plpgsql AS $$ BEGIN \
                 INSERT INTO ol (o, d, n, qty, amt, info) VALUES (o, d, n, q, a, i); END $$",
                "CREATE PROCEDURE ins_hist(c INT, w INT, a NUMERIC) \
                 LANGUAGE plpgsql AS $$ BEGIN \
                 INSERT INTO hist (c, w, amount, at) VALUES (c, w, a, current_timestamp); END $$",
                "CREATE PROCEDURE ins_acct(i INT, b NUMERIC, t TEXT) LANGUAGE plpgsql AS $$ BEGIN \
                 INSERT INTO acct (id, bal, tag) VALUES (i, b, t); END $$",
                "CREATE PROCEDURE ins_wh(w INT, n TEXT, t NUMERIC) LANGUAGE plpgsql AS $$ BEGIN \
                 INSERT INTO wh VALUES (w, n, t); END $$",
                "CREATE PROCEDURE ins_lines(o INT, d INT, ns INT[], qs INT[]) LANGUAGE plpgsql AS $$ BEGIN \
                 INSERT INTO ol (o, d, n, qty, amt, info) \
                 SELECT o, d, data.n, data.q, data.q * 1.5, 'line' || data.n \
                 FROM UNNEST(ns, qs) AS data(n, q); END $$",
            ] {
                sql.execute(ddl).unwrap();
            }
            let statements = [
                "BEGIN",
                "CALL ins_lines(7, 1, ARRAY[1, 2, 3], ARRAY[10, 20, 30])",
                "CALL ins_lines(7, 2, ARRAY[1], ARRAY[5])",
                "SELECT count(*) FROM ol WHERE o = 7",
                "CALL ins_ol(1, 2, 3, 5, 12.5, 'abc')",
                "CALL ins_ol(1, 2, 4, 6, 0.25, NULL)",
                "CALL ins_hist(1, 2, 3.5)",
                "CALL ins_acct(1, 10, 'x')",
                "CALL ins_wh(1, 'w1', 0.1234)",
                "CALL ins_wh(2, 'w2', 0.0500)",
                "SELECT w, name, tax FROM wh ORDER BY w",
                "SELECT o, d, n, qty FROM ol ORDER BY o, d, n",
                "COMMIT",
                "BEGIN",
                "CALL ins_wh(1, 'dup', 0.1)",
                "ROLLBACK",
                "BEGIN",
                "CALL ins_ol(9, 9, 9, NULL, 1, 'z')",
                "ROLLBACK",
                "BEGIN",
                "CALL ins_wh(NULL, 'nopk', 0.1)",
                "ROLLBACK",
                "BEGIN",
                "CALL ins_wh(3, 'w3', 'bad')",
                "ROLLBACK",
                "BEGIN",
                "CALL ins_wh(4, 'w4', 0.2)",
                "CALL ins_ol(1, 2, 3, 7, 1, 'dup')",
                "ROLLBACK",
                "BEGIN",
                "CALL ins_lines(7, 1, ARRAY[9, 1], ARRAY[1, 1])",
                "ROLLBACK",
            ];
            for statement in statements {
                match sql.execute(statement) {
                    Ok(result) => {
                        for row in result.rows {
                            rows.push(row.iter().map(SqlValue::to_cell).collect::<Vec<_>>());
                        }
                    }
                    Err(error) => errors.push(format!("{statement}: {error}")),
                }
            }
        }
        let mut db =
            BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
        let mut sql = SqlSession::new(&mut db);
        for query in [
            "SELECT o, d, n, qty, amt, info FROM ol ORDER BY o, d, n",
            "SELECT c, w, amount, data, CASE WHEN at IS NULL THEN 0 ELSE 1 END FROM hist ORDER BY c",
            "SELECT id, bal, tag, extra FROM acct ORDER BY id",
            "SELECT w, name, tax FROM wh ORDER BY w",
        ] {
            for row in sql.execute(query).unwrap().rows {
                rows.push(row.iter().map(SqlValue::to_cell).collect::<Vec<_>>());
            }
        }
        crate::stored_update::set_test_override(None);
        (rows, errors)
    }
    let generic = run(false);
    let before = crate::session::functions_insert::SQL_STORED_INSERT_HITS.with(|h| *h.borrow());
    let stored = run(true);
    let hits = crate::session::functions_insert::SQL_STORED_INSERT_HITS.with(|h| *h.borrow());
    assert_eq!(generic.0, stored.0);
    assert_eq!(generic.1, stored.1);
    assert!(
        hits >= before + 7,
        "stored-form inserts ran ({} hits)",
        hits - before
    );
    assert_eq!(generic.1.len(), 6, "{:?}", generic.1);
    assert_eq!(
        generic.0[0],
        vec!["4".to_string()],
        "four UNNEST-fed lines in the transaction"
    );
    assert!(
        generic.1[0].contains("dup") && generic.1[0].contains("wh"),
        "{}",
        generic.1[0]
    );
    assert_eq!(
        generic.0.iter().filter(|row| row.len() == 3).count(),
        4,
        "wh rows twice"
    );
    assert_eq!(generic.0.last().unwrap()[1], "w2");
}

#[test]
fn bound_row_filters_match_the_typed_evaluator_and_decline_inexact_shapes() {
    // The stock-level shape (range on the left, pk lookups on the right, a
    // residual comparison and COUNT(DISTINCT)) plus plain range scans: the
    // bound filter (gate on) must return exactly what the typed per-row
    // evaluator (gate off) returns, and shapes where the two evaluators can
    // differ (CHAR columns, arithmetic, enums, mixed text/int) must decline.
    fn run(enabled: bool) -> (Vec<Vec<String>>, usize) {
        crate::bound_row_filter::set_test_override(Some(enabled));
        let dir = tempfile::TempDir::new().unwrap();
        let mut db =
            BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
        let mut sql = SqlSession::new(&mut db);
        for ddl in [
            "CREATE TABLE order_line (ol_w_id INT, ol_d_id INT, ol_o_id INT, ol_number INT, \
             ol_i_id INT, ol_amount NUMERIC(6,2), ol_dist CHAR(24), \
             PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))",
            "CREATE TABLE stock (s_w_id INT, s_i_id INT, s_quantity INT, s_data VARCHAR(50), \
             PRIMARY KEY (s_w_id, s_i_id))",
            "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')",
            "CREATE TABLE moods (id INT PRIMARY KEY, m mood, small SMALLINT, note CHAR(4))",
            "INSERT INTO moods VALUES (1, 'happy', 32767, 'ab'), (2, 'sad', 1, 'ab  '), (3, 'ok', 5, 'abc')",
            "INSERT INTO stock SELECT 1, i, (i * 7) % 50, 'data' || i FROM generate_series(1, 40) AS g(i)",
            "INSERT INTO order_line SELECT 1, 1, o, n, ((o * 5 + n * 3) % 40) + 1, o + n, 'd' \
             FROM generate_series(1, 30) AS a(o), generate_series(1, 5) AS b(n)",
            "CREATE FUNCTION slev(w INT, d INT, next_o INT, threshold INT) RETURNS INT \
             LANGUAGE plpgsql AS $$ DECLARE n INT; BEGIN \
             SELECT COUNT(DISTINCT s_i_id) INTO n FROM order_line, stock \
             WHERE ol_w_id = w AND ol_d_id = d AND ol_o_id < next_o AND ol_o_id >= next_o - 20 \
             AND s_w_id = w AND s_i_id = ol_i_id AND s_quantity < threshold; \
             RETURN n; END $$",
            "CREATE FUNCTION over(w INT, o INT) RETURNS INT \
             LANGUAGE plpgsql AS $$ DECLARE n INT; BEGIN \
             SELECT count(*) INTO n FROM order_line WHERE ol_w_id = w AND ol_o_id >= o + 1; \
             RETURN n; END $$",
            "CREATE FUNCTION lines(w INT, d INT, lo INT, hi INT, amt NUMERIC) RETURNS INT \
             LANGUAGE plpgsql AS $$ DECLARE n INT; BEGIN \
             SELECT count(*) INTO n FROM order_line WHERE ol_w_id = w AND ol_d_id = d \
             AND ol_o_id >= lo AND ol_o_id <= hi AND ol_amount > amt AND ol_i_id <> 3; \
             RETURN n; END $$",
        ] {
            sql.execute(ddl).unwrap();
        }
        let mut rows = Vec::new();
        let hits = || crate::planner::SQL_BOUND_ROW_FILTER_HITS.with(|h| *h.borrow());
        let before_slev = hits();
        for row in sql.execute("SELECT slev(1, 1, 31, 20)").unwrap().rows {
            rows.push(row.iter().map(SqlValue::to_cell).collect::<Vec<_>>());
        }
        let slev_hits = hits() - before_slev;
        for query in [
            "SELECT slev(1, 1, 31, 20)",
            "SELECT slev(1, 1, 15, 45)",
            "SELECT slev(1, 1, 5, 0)",
            "SELECT slev(2, 1, 31, 20)",
            "SELECT lines(1, 1, 3, 9, 6.5)",
            "SELECT lines(1, 1, 3, 9, 6.5)",
            "SELECT lines(1, 1, 30, 40, 0)",
            "SELECT count(*) FROM order_line WHERE ol_w_id = 1 AND ol_i_id >= 10 AND ol_i_id < 20 AND ol_number IS NOT NULL",
            "SELECT id FROM moods WHERE m > 'sad' ORDER BY id",
            "SELECT id FROM moods WHERE note = 'ab' ORDER BY id",
            "SELECT id FROM moods WHERE small + 1 > 0 ORDER BY id",
            "SELECT id FROM moods WHERE id = 2 AND small < 3",
            "SELECT s_i_id FROM stock WHERE s_w_id = 1 AND s_data = 'data7'",
        ] {
            for row in sql.execute(query).unwrap().rows {
                rows.push(row.iter().map(SqlValue::to_cell).collect::<Vec<_>>());
            }
        }
        for row in sql
            .execute("SELECT over(1, 10), over(9, 2147483647)")
            .unwrap()
            .rows
        {
            rows.push(row.iter().map(SqlValue::to_cell).collect::<Vec<_>>());
        }
        for erroring in [
            "SELECT id FROM moods WHERE small + 1::int2 > 0",
            "SELECT over(1, 2147483647)",
        ] {
            let error = sql.execute(erroring).unwrap_err();
            rows.push(vec![error.to_string()]);
        }
        crate::bound_row_filter::set_test_override(None);
        (rows, slev_hits)
    }
    let (typed, typed_slev_hits) = run(false);
    let before = crate::planner::SQL_BOUND_ROW_FILTER_HITS.with(|h| *h.borrow());
    let (bound, bound_slev_hits) = run(true);
    let hits = crate::planner::SQL_BOUND_ROW_FILTER_HITS.with(|h| *h.borrow()) - before;
    assert_eq!(typed, bound);
    assert_eq!(typed_slev_hits, 0);
    assert_eq!(
        bound_slev_hits, 1,
        "the stock-level shape takes the bound filter"
    );
    assert!(
        (5..=14).contains(&hits),
        "bound filters ran for the exact shapes only ({hits} hits)"
    );
    assert_eq!(typed[typed.len() - 2][0], "smallint out of range");
    assert_eq!(
        typed[typed.len() - 3],
        vec!["100".to_string(), "0".to_string()]
    );
    assert_ne!(typed[0][0], "0", "stock-level counts something");
    assert_eq!(typed[3][0], "0");
    assert_eq!(typed[4][0], "0");
    assert_eq!(typed.last().unwrap()[0], "integer out of range");
}

#[test]
fn stock_level_join_binds_its_constraint_once_per_join() {
    // HammerDB's STOCK_LEVEL: a three-way comma join whose order_line range
    // comes from district's row and whose stock side is a primary-key
    // lookup. The join constraint must be bound once per join and only
    // evaluated per candidate pair; the generic per-pair path (merge the
    // column lists, build a scope, bind) is the slow path this guards.
    let dir = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE TABLE district (d_id INT, d_w_id INT, d_next_o_id INT, PRIMARY KEY (d_w_id, d_id))",
    )
    .unwrap();
    sql.execute(
        "CREATE TABLE order_line (ol_o_id INT, ol_d_id INT, ol_w_id INT, ol_number INT, ol_i_id INT, \
         ol_quantity INT, PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))",
    )
    .unwrap();
    sql.execute(
        "CREATE TABLE stock (s_i_id INT, s_w_id INT, s_quantity INT, s_data TEXT, PRIMARY KEY (s_w_id, s_i_id))",
    )
    .unwrap();
    sql.execute("INSERT INTO district VALUES (1, 1, 31), (2, 1, 5)")
        .unwrap();
    for o in 1..=30 {
        for n in 1..=3 {
            let item = (o * 3 + n) % 17 + 1;
            sql.execute(&format!(
                "INSERT INTO order_line VALUES ({o}, 1, 1, {n}, {item}, 5)"
            ))
            .unwrap();
        }
    }
    for i in 1..=20 {
        sql.execute(&format!(
            "INSERT INTO stock VALUES ({i}, 1, {}, 'd{i}')",
            if i % 2 == 0 { 5 } else { 25 }
        ))
        .unwrap();
    }
    sql.execute(
        "CREATE PROCEDURE slev (st_w_id IN INTEGER, st_d_id IN INTEGER, threshold IN INTEGER, \
         stock_count INOUT INTEGER) LANGUAGE plpgsql AS $$
         BEGIN
           SELECT COUNT(DISTINCT (s_i_id)) INTO stock_count
           FROM order_line, stock, district
           WHERE ol_w_id = st_w_id AND ol_d_id = st_d_id AND d_w_id = st_w_id AND d_id = st_d_id
             AND (ol_o_id < d_next_o_id) AND ol_o_id >= (d_next_o_id - 20)
             AND s_w_id = st_w_id AND s_i_id = ol_i_id AND s_quantity < threshold;
         END $$",
    )
    .unwrap();
    // Oracle: the same count from the flat query with the bounds inlined.
    let expected = sql
        .execute(
            "SELECT COUNT(DISTINCT (s_i_id))::text FROM order_line, stock \
             WHERE ol_w_id = 1 AND ol_d_id = 1 AND ol_o_id < 31 AND ol_o_id >= 11 \
             AND s_w_id = 1 AND s_i_id = ol_i_id AND s_quantity < 15",
        )
        .unwrap()
        .rows
        .remove(0);
    reset_sql_join_constraint_paths();
    for _ in 0..3 {
        SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
        let rows = sql.execute("CALL slev(1, 1, 15, NULL)").unwrap().rows;
        let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());
        assert_eq!(rows[0][0].to_cell(), expected[0].to_cell());
        // The order_line range comes from district's row and is applied at
        // the index: 60 of the 90 order lines are in range, each joined to
        // one stock row — never the district's whole order_line prefix.
        assert_eq!(stats.join_steps, 2);
        assert_eq!(
            stats.join_candidate_pairs, 120,
            "60 order_line rows, then 60 stock probes"
        );
    }
    let (bound_once, pair_fallbacks) = sql_join_constraint_paths();
    assert!(
        bound_once >= 3,
        "the join constraint is bound once per join ({bound_once} bindings)"
    );
    assert_eq!(
        pair_fallbacks, 0,
        "no candidate pair takes the generic bind-per-pair path"
    );
}

#[test]
fn money_avg_validation_still_fires_wherever_avg_appears() {
    // The validation only runs when the statement mentions a function named
    // avg (any case; a qualified `pg_catalog.avg` is matched by its last
    // part but is not otherwise supported here) — projection, HAVING,
    // ORDER BY — and is skipped, with its per-query aggregate collection,
    // otherwise.
    let dir = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE t (i INT, m MONEY)").unwrap();
    sql.execute("INSERT INTO t VALUES (1, '1.50'), (2, '2.50')")
        .unwrap();
    for bad in [
        "SELECT avg(m) FROM t",
        "SELECT AVG(m) FROM t",
        "SELECT i FROM t GROUP BY i HAVING avg(m) > '1'::money",
        "SELECT i FROM t GROUP BY i ORDER BY avg(m)",
    ] {
        let error = sql.execute(bad).unwrap_err().to_string();
        assert!(
            error.contains("function avg(money) does not exist"),
            "{bad}: {error}"
        );
    }
    for ok in [
        "SELECT sum(m) FROM t",
        "SELECT avg(i) FROM t",
        "SELECT avg(i), sum(m) FROM t",
        "SELECT count(*) FROM t WHERE m > '1'::money",
    ] {
        sql.execute(ok)
            .unwrap_or_else(|error| panic!("{ok}: {error}"));
    }
}

#[test]
fn call_arguments_are_typed_once_per_cached_statement() {
    // HammerDB's CALLs carry casts on their arguments; the cached statement
    // keeps those nodes at stable addresses, and evaluating them under the
    // callee's expression-type scope lets the per-node type memo serve every
    // call after the first instead of re-inferring each argument's type.
    let dir = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE audit (w INT, amount NUMERIC(12,2), at TIMESTAMP, note TEXT)")
        .unwrap();
    sql.execute(
        "CREATE PROCEDURE pay (p_w IN INTEGER, p_amount IN NUMERIC, p_at IN TIMESTAMP, p_note IN VARCHAR, \
         total INOUT NUMERIC) LANGUAGE plpgsql AS $$
         BEGIN
           INSERT INTO audit VALUES (p_w, p_amount, p_at, p_note);
           SELECT sum(amount) INTO total FROM audit WHERE w = p_w;
         END $$",
    )
    .unwrap();
    let call = |sql: &mut SqlSession, w: i64, amount: &str| {
        sql.execute(&format!(
            "CALL pay({w}, {amount}::numeric, TO_TIMESTAMP('20260902115830', 'YYYYMMDDHH24MISS')::timestamp without time zone, 'x'::varchar, NULL)"
        ))
        .unwrap()
        .rows
        .remove(0)
    };
    let first = call(&mut sql, 1, "10.50");
    assert_eq!(first[0].to_cell(), "10.50");
    let second = call(&mut sql, 1, "2.25");
    assert_eq!(second[0].to_cell(), "12.75");
    let uncached = crate::eval::SQL_EXPR_TYPE_UNCACHED_CALLS.with(|calls| calls.get());
    let third = call(&mut sql, 1, "0.25");
    assert_eq!(third[0].to_cell(), "13.00");
    assert_eq!(
        crate::eval::SQL_EXPR_TYPE_UNCACHED_CALLS.with(|calls| calls.get()),
        uncached,
        "a repeated CALL must not re-infer its argument types"
    );
}

#[test]
fn primary_key_access_plan_is_memoized_per_statement_and_stays_correct() {
    // The AND term binding each pk column is derived once per statement
    // node; repeated executions with different bound values (routine vars)
    // must locate the right rows, for a full key and for a key prefix.
    let dir = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE TABLE stock (s_w_id INT, s_i_id INT, s_quantity INT, PRIMARY KEY (s_w_id, s_i_id))",
    )
    .unwrap();
    for w in 1..=2 {
        for i in 1..=5 {
            sql.execute(&format!(
                "INSERT INTO stock VALUES ({w}, {i}, {})",
                100 * w + i
            ))
            .unwrap();
        }
    }
    sql.execute(
        "CREATE PROCEDURE take (p_w INT, p_i INT, p_qty INT, INOUT left_qty INT) LANGUAGE plpgsql AS $$
         BEGIN
           UPDATE stock SET s_quantity = s_quantity - p_qty WHERE s_i_id = p_i AND s_w_id = p_w;
           SELECT s_quantity INTO left_qty FROM stock WHERE s_w_id = p_w AND s_i_id = p_i;
         END $$",
    )
    .unwrap();
    for (w, i, qty, expect) in [
        (1, 3, 1, 102),
        (2, 3, 5, 198),
        (1, 3, 2, 100),
        (2, 5, 5, 200),
    ] {
        let rows = sql
            .execute(&format!("CALL take({w}, {i}, {qty}, NULL)"))
            .unwrap()
            .rows;
        assert_eq!(rows[0][0].to_cell(), expect.to_string(), "w={w} i={i}");
    }
    // Prefix access (first pk column only) through a repeated routine query.
    sql.execute(
        "CREATE PROCEDURE total (p_w INT, INOUT t BIGINT) LANGUAGE plpgsql AS $$
         BEGIN SELECT sum(s_quantity) INTO t FROM stock WHERE s_w_id = p_w; END $$",
    )
    .unwrap();
    let t1 = sql.execute("CALL total(1, NULL)").unwrap().rows.remove(0)[0].to_cell();
    let t2 = sql.execute("CALL total(2, NULL)").unwrap().rows.remove(0)[0].to_cell();
    let t1b = sql.execute("CALL total(1, NULL)").unwrap().rows.remove(0)[0].to_cell();
    assert_eq!(t1, (101 + 102 + 103 + 104 + 105 - 3).to_string());
    assert_eq!(t2, (201 + 202 + 203 + 204 + 205 - 10).to_string());
    assert_eq!(t1b, t1);
}
