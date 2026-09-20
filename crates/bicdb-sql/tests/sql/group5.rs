//! Test group split from the former monolithic tests/sql.rs.
use super::*;

#[test]
fn regular_same_value_clears_map_local_restore() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session.execute("SET app.scope = 'base'").unwrap();
    session.execute("SET app.unrelated = 'keep'").unwrap();
    session.execute("BEGIN").unwrap();
    session.execute("SET LOCAL app.scope = 'same'").unwrap();
    session
        .execute("SET LOCAL app.unrelated = 'temporary'")
        .unwrap();
    session.execute("SET app.scope = 'same'").unwrap();
    session.execute("COMMIT").unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT current_setting('app.scope', true), \
                        current_setting('app.unrelated', true)",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("same".to_string()),
            SqlValue::String("keep".to_string()),
        ]]
    );
}

#[test]
fn regular_same_value_clears_typed_local_restore() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session.execute("SET bicdb.ef_search = 10").unwrap();
    session.execute("SET bicdb.vector_search = 'ann'").unwrap();
    session.execute("BEGIN").unwrap();
    session.execute("SET LOCAL bicdb.ef_search = 20").unwrap();
    session
        .execute("SET LOCAL bicdb.vector_search = 'exact'")
        .unwrap();
    session.execute("SET bicdb.ef_search = 20").unwrap();
    session.execute("COMMIT").unwrap();

    assert_eq!(
        session.execute("SHOW bicdb.ef_search").unwrap().rows,
        vec![vec![SqlValue::Int(20)]]
    );
    assert_eq!(
        session.execute("SHOW bicdb.vector_search").unwrap().rows,
        vec![vec![SqlValue::String("ann".to_string())]]
    );
}

#[test]
fn reset_same_default_clears_typed_local_restore() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session.execute("SET bicdb.vector_search = 'ann'").unwrap();
    session.execute("BEGIN").unwrap();
    session
        .execute("SET LOCAL bicdb.vector_search = 'exact'")
        .unwrap();
    session.execute("RESET bicdb.vector_search").unwrap();
    session.execute("COMMIT").unwrap();

    assert_eq!(
        session.execute("SHOW bicdb.vector_search").unwrap().rows,
        vec![vec![SqlValue::String("exact".to_string())]]
    );
}

#[test]
fn sequence_currval_and_lastval_survive_rollback_and_savepoint() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE SEQUENCE session_seq").unwrap();
    session.execute("SELECT nextval('session_seq')").unwrap();

    session.execute("BEGIN").unwrap();
    session.execute("SELECT nextval('session_seq')").unwrap();
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session
            .execute("SELECT currval('session_seq'), lastval()")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(2), SqlValue::Int(2)]]
    );

    session.execute("BEGIN").unwrap();
    session.execute("SAVEPOINT before_nextval").unwrap();
    session.execute("SELECT nextval('session_seq')").unwrap();
    session
        .execute("ROLLBACK TO SAVEPOINT before_nextval")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT currval('session_seq'), lastval()")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(3), SqlValue::Int(3)]]
    );
    session.execute("ROLLBACK").unwrap();
}

#[test]
fn public_execution_entry_points_close_implicit_guc_boundaries() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE guc_copy_boundary (
                id INT PRIMARY KEY,
                owner TEXT DEFAULT set_config('app.user_id', 'copy-user', true)
            )",
        )
        .unwrap();

    assert_eq!(
        session
            .copy_insert_rows(
                "guc_copy_boundary",
                &["id".to_string()],
                vec![vec![Some("1".to_string())]]
            )
            .unwrap(),
        1
    );
    assert_eq!(
        session
            .execute("SELECT current_setting('app.user_id', true)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Null]]
    );
    session
        .copy_insert_rows(
            "guc_copy_boundary",
            &["id".to_string()],
            vec![vec![Some("1".to_string())]],
        )
        .unwrap_err();
    assert_eq!(
        session
            .execute("SELECT current_setting('app.user_id', true)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Null]]
    );

    let statement = Parser::parse_sql(
        &PostgreSqlDialect {},
        "SELECT set_config('app.user_id', 'ast-user', true)",
    )
    .unwrap()
    .remove(0);
    assert_eq!(
        session.execute_statement_ast(&statement).unwrap().rows,
        vec![vec![SqlValue::String("ast-user".to_string())]]
    );
    assert_eq!(
        session
            .execute("SELECT current_setting('app.user_id', true)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Null]]
    );

    session
        .execute(
            "CREATE PROCEDURE guc_boundary_proc(identity OUT TEXT)
             LANGUAGE 'plpgsql'
             AS $$
             BEGIN
                 identity := set_config('app.user_id', 'procedure-user', true);
             END;
             $$",
        )
        .unwrap();
    assert_eq!(
        session
            .call_procedure("guc_boundary_proc", &[])
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("procedure-user".to_string())]]
    );
    assert_eq!(
        session
            .execute("SELECT current_setting('app.user_id', true)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Null]]
    );
}

#[test]
fn session_guc_state_memory_estimate_includes_transactional_snapshots() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    let value = "x".repeat(32 * 1024);
    session
        .execute(&format!("SET app.large = '{value}'"))
        .unwrap();
    let committed = session.session_guc_state().memory_estimate();

    session.execute("BEGIN").unwrap();
    session.execute("SET LOCAL app.large = 'local'").unwrap();
    let transactional = session.session_guc_state().memory_estimate();

    assert!(committed >= value.len());
    assert!(transactional >= committed.saturating_add(value.len() / 2));

    session.execute("ROLLBACK").unwrap();
    session
        .execute("CREATE SEQUENCE session_memory_accounted_sequence_name")
        .unwrap();
    let before_currval = session.session_guc_state().memory_estimate();
    session
        .execute("SELECT nextval('session_memory_accounted_sequence_name')")
        .unwrap();
    let after_currval = session.session_guc_state().memory_estimate();
    assert!(after_currval > before_currval);
}

#[test]
fn rollback_clears_local_gucs_even_when_ddl_cleanup_fails() {
    let (_dir, mut db) = empty_test_db();
    let (tx, ddl_undo, guc_state) = {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE cleanup_target (id INT PRIMARY KEY, value TEXT)")
            .unwrap();
        session
            .execute("CREATE INDEX cleanup_target_value_idx ON cleanup_target (value)")
            .unwrap();
        session.execute("BEGIN").unwrap();
        session
            .execute("SET LOCAL app.identity = 'transaction-user'")
            .unwrap();
        session
            .execute("DROP INDEX cleanup_target_value_idx")
            .unwrap();
        let tx = session.take_pending_transaction().unwrap();
        let ddl_undo = session.take_ddl_undo_log();
        let guc_state = session.session_guc_state();
        (tx, ddl_undo, guc_state)
    };
    {
        let mut cleanup = SqlSession::new(&mut db);
        cleanup
            .execute("CREATE INDEX cleanup_target_value_idx ON cleanup_target (value)")
            .unwrap();
    }

    let mut session = SqlSession::new(&mut db)
        .with_pending_transaction(tx)
        .with_ddl_undo_log(ddl_undo)
        .with_session_guc_state(guc_state);
    session.execute("ROLLBACK").unwrap_err();
    assert!(!session.in_transaction());
    assert_eq!(
        session
            .execute("SELECT current_setting('app.identity', true)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Null]]
    );
}

#[test]
fn commit_failure_clears_local_gucs_even_when_ddl_cleanup_fails() {
    let (_dir, mut db) = empty_test_db();
    let (tx, ddl_undo, guc_state) = {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE cleanup_stock (id INT PRIMARY KEY, quantity INT)")
            .unwrap();
        session
            .execute("INSERT INTO cleanup_stock VALUES (1, 50)")
            .unwrap();
        session
            .execute("CREATE TABLE commit_cleanup_target (id INT PRIMARY KEY, value TEXT)")
            .unwrap();
        session
            .execute(
                "CREATE INDEX commit_cleanup_target_value_idx ON commit_cleanup_target (value)",
            )
            .unwrap();
        session.execute("BEGIN").unwrap();
        session
            .execute("SELECT quantity FROM cleanup_stock WHERE id = 1")
            .unwrap();
        session
            .execute("SET LOCAL app.identity = 'transaction-user'")
            .unwrap();
        session
            .execute("DROP INDEX commit_cleanup_target_value_idx")
            .unwrap();
        let tx = session.take_pending_transaction().unwrap();
        let ddl_undo = session.take_ddl_undo_log();
        let guc_state = session.session_guc_state();
        (tx, ddl_undo, guc_state)
    };
    {
        let mut competing = SqlSession::new(&mut db);
        competing
            .execute("UPDATE cleanup_stock SET quantity = 40 WHERE id = 1")
            .unwrap();
        competing
            .execute(
                "CREATE INDEX commit_cleanup_target_value_idx ON commit_cleanup_target (value)",
            )
            .unwrap();
    }

    let mut session = SqlSession::new(&mut db)
        .with_pending_transaction(tx)
        .with_ddl_undo_log(ddl_undo)
        .with_session_guc_state(guc_state);
    session
        .execute(
            "UPDATE cleanup_stock SET quantity = cleanup_stock.quantity - v.qty \
             FROM (SELECT 1 AS id, 5 AS qty) v WHERE cleanup_stock.id = v.id",
        )
        .unwrap();
    let error = session.execute("COMMIT").unwrap_err();
    assert!(matches!(
        error,
        SqlError::BicDb(BicDbError::TransactionConflict(_))
    ));
    assert!(!session.in_transaction());
    assert_eq!(
        session
            .execute("SELECT current_setting('app.identity', true)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Null]]
    );
}

#[test]
fn postgres_aggregate_default_labels_match_client_arrays() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE TABLE WAREHOUSE (W_ID INT PRIMARY KEY)")
        .unwrap();
    session
        .execute("CREATE TABLE DISTRICT (D_ID INT PRIMARY KEY, D_NEXT_O_ID INT)")
        .unwrap();
    session
        .execute("INSERT INTO warehouse (w_id) VALUES (1)")
        .unwrap();
    session
        .execute("INSERT INTO district (d_id, d_next_o_id) VALUES (10, 3001)")
        .unwrap();

    let result = session.execute("SELECT max(w_id) FROM warehouse").unwrap();
    assert_eq!(result.columns, ["max"]);
    assert_eq!(result.rows, vec![vec![SqlValue::Int(1)]]);

    let result = session
        .execute("SELECT sum(d_next_o_id) FROM district")
        .unwrap();
    assert_eq!(result.columns, ["sum"]);
    assert_eq!(result.rows, vec![vec![SqlValue::Int(3001)]]);
}

#[test]
fn postgres_composite_primary_key_preserves_distinct_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE district (
                d_w_id INT,
                d_id INT,
                d_next_o_id INT,
                PRIMARY KEY (d_w_id, d_id)
            )",
        )
        .unwrap();
    session
        .execute("INSERT INTO district (d_w_id, d_id, d_next_o_id) VALUES (1, 1, 3001)")
        .unwrap();
    session
        .execute("INSERT INTO district (d_w_id, d_id, d_next_o_id) VALUES (1, 2, 3001)")
        .unwrap();

    let result = session
        .execute("SELECT d_w_id, d_id, d_next_o_id FROM district ORDER BY d_id")
        .unwrap();
    assert_eq!(result.columns, ["d_w_id", "d_id", "d_next_o_id"]);
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Int(1), SqlValue::Int(1), SqlValue::Int(3001)],
            vec![SqlValue::Int(1), SqlValue::Int(2), SqlValue::Int(3001)],
        ]
    );

    let duplicate = session
        .execute("INSERT INTO district (d_w_id, d_id, d_next_o_id) VALUES (1, 1, 3002)")
        .unwrap_err();
    assert!(duplicate.to_string().contains("duplicate key value"));
}

#[test]
fn plpgsql_procedure_vm_uses_generic_symbols_and_declared_output_order() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            r#"CREATE TABLE ledger_entries (
                entry_id INT PRIMARY KEY,
                account_key INT,
                amount INT,
                customer_id INT,
                "c_id" TEXT,
                note TEXT
            )"#,
        )
        .unwrap();
    session
        .execute(
            r#"INSERT INTO ledger_entries
               (entry_id, account_key, amount, customer_id, "c_id", note)
               VALUES (1, 7, 10, 123, 'quoted c_id', 'before')"#,
        )
        .unwrap();

    session
        .execute(
            r#"
            CREATE PROCEDURE reconcile_anything(
                lookup_key IN INTEGER,
                delta_units IN INTEGER,
                c_id IN INTEGER,
                leading_total OUT INTEGER,
                trailing_note OUT TEXT,
                carried_value INOUT INTEGER
            )
            LANGUAGE 'plpgsql'
            AS $$
            DECLARE
                local_offset INTEGER := delta_units + 1;
                chosen_amount INTEGER;
            BEGIN
                SELECT chosen.amount + local_offset
                INTO chosen_amount
                FROM ledger_entries AS chosen
                WHERE chosen.account_key = lookup_key -- c_id customer_id in a comment
                  AND chosen.customer_id = (SELECT c_id)
                  AND chosen."c_id" = 'quoted c_id';

                leading_total := chosen_amount + carried_value;

                UPDATE ledger_entries AS chosen
                SET amount = amount + delta_units,
                    note = 'literal c_id customer_id -- c_id'
                WHERE chosen.account_key = lookup_key
                  AND chosen.customer_id = (SELECT c_id)
                  AND chosen."c_id" = 'quoted c_id'
                RETURNING note INTO trailing_note;

                carried_value := leading_total + delta_units;
            END;
            $$
            "#,
        )
        .unwrap();

    let result = session
        .execute("CALL reconcile_anything(7, 5, 123, 9)")
        .unwrap();
    assert_eq!(
        result.columns,
        ["leading_total", "trailing_note", "carried_value"]
    );
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Int(25),
            SqlValue::String("literal c_id customer_id -- c_id".to_string()),
            SqlValue::Int(30),
        ]]
    );
    assert_eq!(
        session
            .execute(
                r#"SELECT amount, customer_id, "c_id", note
                   FROM ledger_entries
                   WHERE entry_id = 1"#,
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(15),
            SqlValue::Int(123),
            SqlValue::String("quoted c_id".to_string()),
            SqlValue::String("literal c_id customer_id -- c_id".to_string()),
        ]]
    );
}

#[test]
fn plpgsql_function_vm_executes_select_into_if_else_and_return_generically() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE arbitrary_source (
                source_key INT PRIMARY KEY,
                source_value INT
            )",
        )
        .unwrap();
    session
        .execute("INSERT INTO arbitrary_source (source_key, source_value) VALUES (3, 11)")
        .unwrap();
    session
        .execute(
            r#"
            CREATE FUNCTION compute_arbitrary_result(input_key INTEGER, extra_value INTEGER)
            RETURNS INTEGER
            LANGUAGE 'plpgsql'
            AS $$
            DECLARE
                local_value INTEGER;
                final_value INTEGER;
            BEGIN
                SELECT src.source_value
                INTO local_value
                FROM arbitrary_source AS src
                WHERE src.source_key = input_key;

                IF (local_value > 10) THEN
                    final_value := local_value + extra_value;
                ELSE
                    final_value := extra_value;
                END IF;

                RETURN final_value;
            END;
            $$
            "#,
        )
        .unwrap();

    let result = session
        .execute("SELECT compute_arbitrary_result(3, 4)")
        .unwrap();
    assert_eq!(result.rows, vec![vec![SqlValue::Int(15)]]);
}

#[test]
fn plpgsql_if_elsif_else_executes_generically() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            r#"
            CREATE PROCEDURE arbitrary_branch_probe(branch_value IN INTEGER, branch_label OUT TEXT)
            LANGUAGE 'plpgsql'
            AS $$
            BEGIN
                IF (branch_value = 1) OR (branch_value = 11)
                THEN
                    branch_label := 'one';
                ELSIF (branch_value > 1) AND (branch_value < 3)
                THEN
                    branch_label := 'two';
                ELSIF branch_value = 3
                THEN
                    branch_label := 'three';
                ELSE
                    branch_label := 'other';
                END IF;
            END;
            $$
            "#,
        )
        .unwrap();

    assert_eq!(
        session
            .execute("CALL arbitrary_branch_probe(2)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("two".to_string())]]
    );
    assert_eq!(
        session
            .execute("CALL arbitrary_branch_probe(11)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("one".to_string())]]
    );
    assert_eq!(
        session
            .execute("CALL arbitrary_branch_probe(9)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("other".to_string())]]
    );
}

#[test]
fn plpgsql_elsif_branches_can_contain_cte_select_into_statements() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE arbitrary_branch_rows (
                row_id INT PRIMARY KEY,
                branch_key INT,
                amount INT
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO arbitrary_branch_rows (row_id, branch_key, amount)
             VALUES (1, 1, 10), (2, 2, 20), (3, 3, 30)",
        )
        .unwrap();
    session
        .execute(
            r#"
            CREATE PROCEDURE arbitrary_cte_branch_probe(branch_value IN INTEGER, branch_total OUT INTEGER)
            LANGUAGE 'plpgsql'
            AS $$
            BEGIN
                IF branch_value = 1
                THEN
                    WITH changed_rows AS (
                        UPDATE arbitrary_branch_rows
                        SET amount = amount + 1
                        WHERE branch_key = 1
                        RETURNING amount
                    )
                    SELECT max(amount) FROM changed_rows INTO branch_total;
                ELSIF branch_value = 2
                THEN
                    WITH changed_rows AS (
                        UPDATE arbitrary_branch_rows
                        SET amount = amount + 2
                        WHERE branch_key = 2
                        RETURNING amount
                    )
                    SELECT max(amount) FROM changed_rows INTO branch_total;
                ELSIF branch_value = 3
                THEN
                    WITH changed_rows AS (
                        UPDATE arbitrary_branch_rows
                        SET amount = amount + 3
                        WHERE branch_key = 3
                        RETURNING amount
                    )
                    SELECT max(amount) FROM changed_rows INTO branch_total;
                END IF;
            END;
            $$
            "#,
        )
        .unwrap();

    assert_eq!(
        session
            .execute("CALL arbitrary_cte_branch_probe(2)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(22)]]
    );
}

#[test]
fn plpgsql_if_splitter_ignores_case_expressions_inside_cte_branches() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE arbitrary_case_rows (
                row_id INT PRIMARY KEY,
                amount INT
            )",
        )
        .unwrap();
    session
        .execute("INSERT INTO arbitrary_case_rows (row_id, amount) VALUES (1, 10)")
        .unwrap();
    session
        .execute(
            r#"
            CREATE PROCEDURE arbitrary_case_branch_probe(branch_value IN INTEGER, branch_total OUT INTEGER)
            LANGUAGE 'plpgsql'
            AS $$
            DECLARE
                row_ids INT[];
                deltas INT[];
            BEGIN
                -- a leading comment must not hide the following block opener
                FOR loop_index IN 1 .. 1
                LOOP
                    -- a leading comment must not hide the following branch opener
                    IF loop_index = 1
                    THEN
                        row_ids[loop_index] := 1;
                        deltas[loop_index] := 2;
                    ELSE
                        row_ids[loop_index] := 3;
                        deltas[loop_index] := 4;
                    END IF;
                END LOOP;

                IF branch_value = 1
                THEN
                    WITH changed_rows AS (
                        UPDATE arbitrary_case_rows
                        SET amount = (
                            CASE
                                WHEN amount < payload.delta + 10
                                THEN amount + 91
                                ELSE amount
                            END
                        ) - payload.delta
                        FROM UNNEST(row_ids, deltas) AS payload(row_id, delta)
                        WHERE arbitrary_case_rows.row_id = payload.row_id
                        RETURNING amount
                    )
                    SELECT max(amount) FROM changed_rows INTO branch_total;
                ELSIF branch_value = 2
                THEN
                    SELECT 42 INTO branch_total;
                END IF;
            END;
            $$
            "#,
        )
        .unwrap();

    assert_eq!(
        session
            .execute("CALL arbitrary_case_branch_probe(2)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(42)]]
    );
}

#[test]
fn plpgsql_array_assignments_feed_unnest_queries_generically() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            r#"
            CREATE PROCEDURE arbitrary_array_probe(total_value OUT INTEGER, collected_values OUT INT[])
            LANGUAGE 'plpgsql'
            AS $$
            DECLARE
                local_numbers INT[];
            BEGIN
                local_numbers[1] := 4;
                local_numbers[2] := 5;

                SELECT sum(item_value), array_agg(item_value)
                FROM UNNEST(local_numbers) AS arbitrary_items(item_value)
                INTO total_value, collected_values;
            END;
            $$
            "#,
        )
        .unwrap();

    assert_eq!(
        session
            .execute("CALL arbitrary_array_probe()")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(9), SqlValue::Json(json!([4, 5])),]]
    );
}

#[test]
fn plpgsql_delete_using_cte_returning_feeds_select_into_generically() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE arbitrary_delete_rows (
                row_id INT PRIMARY KEY,
                payload TEXT
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO arbitrary_delete_rows (row_id, payload)
             VALUES (1, 'a'), (2, 'b'), (3, 'c')",
        )
        .unwrap();
    session
        .execute(
            r#"
            CREATE PROCEDURE arbitrary_delete_probe(removed_ids OUT INT[])
            LANGUAGE 'plpgsql'
            AS $$
            DECLARE
                wanted_ids INT[] := ARRAY[1, 3];
            BEGIN
                WITH removed_rows AS (
                    DELETE FROM arbitrary_delete_rows AS gone
                    USING UNNEST(wanted_ids) AS wanted(row_id)
                    WHERE gone.row_id = wanted.row_id
                    RETURNING gone.row_id
                )
                SELECT array_agg(row_id)
                FROM removed_rows
                INTO removed_ids;
            END;
            $$
            "#,
        )
        .unwrap();

    assert_eq!(
        session
            .execute("CALL arbitrary_delete_probe()")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Json(json!([1, 3]))]]
    );
    assert_eq!(
        session
            .execute("SELECT row_id FROM arbitrary_delete_rows ORDER BY row_id")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(2)]]
    );
}

#[test]
fn plpgsql_cursor_for_loop_fetches_rows_generically() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE arbitrary_roster (
                arbitrary_id INT PRIMARY KEY,
                bucket_key INT,
                member_code INT,
                display_name TEXT
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO arbitrary_roster (arbitrary_id, bucket_key, member_code, display_name)
             VALUES (1, 7, 42, 'bravo'), (2, 7, 84, 'alpha'), (3, 8, 126, 'charlie')",
        )
        .unwrap();
    session
        .execute(
            r#"
            CREATE PROCEDURE arbitrary_cursor_probe(
                lookup_bucket IN INTEGER,
                fetched_name OUT TEXT,
                fetched_key OUT INTEGER
            )
            LANGUAGE 'plpgsql'
            AS $$
            DECLARE
                arbitrary_total INTEGER;
                arbitrary_cursor CURSOR FOR
                    SELECT roster.member_code, roster.display_name
                    FROM arbitrary_roster AS roster
                    WHERE roster.bucket_key = lookup_bucket
                    ORDER BY roster.display_name;
            BEGIN
                SELECT count(arbitrary_id)
                INTO arbitrary_total
                FROM arbitrary_roster
                WHERE bucket_key = lookup_bucket;

                OPEN arbitrary_cursor;
                FOR local_loop_counter IN 1 .. CAST(arbitrary_total / 2 AS INT)
                LOOP
                    FETCH arbitrary_cursor INTO fetched_key, fetched_name;
                END LOOP;
                CLOSE arbitrary_cursor;

                EXCEPTION
                WHEN no_data_found
                THEN RETURN;
            END;
            $$
            "#,
        )
        .unwrap();

    let result = session.execute("CALL arbitrary_cursor_probe(7)").unwrap();
    assert_eq!(result.columns, ["fetched_name", "fetched_key"]);
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::String("alpha".to_string()),
            SqlValue::Int(84)
        ]]
    );

    let odd_count = session.execute("CALL arbitrary_cursor_probe(8)").unwrap();
    assert_eq!(odd_count.columns, ["fetched_name", "fetched_key"]);
    assert_eq!(odd_count.rows, vec![vec![SqlValue::Null, SqlValue::Null]]);
}

#[test]
fn plpgsql_exception_handler_rolls_back_block_writes() {
    let (_dir, mut db) = test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE arbitrary_exception_events (
                event_id INTEGER PRIMARY KEY,
                label TEXT
            )",
        )
        .unwrap();
    session
        .execute(
            r#"
            CREATE PROCEDURE arbitrary_exception_probe()
            LANGUAGE 'plpgsql'
            AS $$
            BEGIN
                INSERT INTO arbitrary_exception_events (event_id, label)
                VALUES (1, 'before-error');
                INSERT INTO arbitrary_exception_events (event_id, label)
                VALUES ('not-an-integer'::INTEGER, 'error-row');
            EXCEPTION
                WHEN others THEN
                    INSERT INTO arbitrary_exception_events (event_id, label)
                    VALUES (2, 'from-handler');
            END;
            $$
            "#,
        )
        .unwrap();

    session.execute("CALL arbitrary_exception_probe()").unwrap();
    let rows = session
        .execute("SELECT event_id, label FROM arbitrary_exception_events ORDER BY event_id")
        .unwrap()
        .rows;
    assert_eq!(
        rows,
        vec![vec![
            SqlValue::Int(2),
            SqlValue::String("from-handler".to_string())
        ]]
    );
}

#[test]
fn plpgsql_integer_for_loop_allows_loop_named_variables_generically() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            r#"
            CREATE PROCEDURE arbitrary_loop_probe(loop_limit IN INTEGER, loop_total OUT INTEGER)
            LANGUAGE 'plpgsql'
            AS $$
            BEGIN
                loop_total := 0;
                FOR loop_counter IN 1 .. loop_limit
                LOOP
                    loop_total := loop_total + loop_counter;
                END LOOP;
            END;
            $$
            "#,
        )
        .unwrap();

    assert_eq!(
        session
            .execute("CALL arbitrary_loop_probe(4)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(10)]]
    );
}

#[test]
fn plpgsql_query_for_loop_binds_record_fields_generically() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE arbitrary_loop_source (
                arbitrary_id INT PRIMARY KEY,
                arbitrary_bucket INT,
                arbitrary_code INT,
                arbitrary_amount INT
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO arbitrary_loop_source
             (arbitrary_id, arbitrary_bucket, arbitrary_code, arbitrary_amount)
             VALUES (1, 7, 42, 5), (2, 7, 84, 9), (3, 8, 126, 11)",
        )
        .unwrap();
    session
        .execute(
            r#"
            CREATE PROCEDURE arbitrary_query_loop_probe(
                lookup_bucket IN INTEGER,
                rendered_lines OUT TEXT
            )
            LANGUAGE 'plpgsql'
            AS $$
            DECLARE
                current_row RECORD;
                matching_rows INTEGER;
            BEGIN
                rendered_lines := '';
                FOR current_row IN
                    SELECT arbitrary_code AS code_alias,
                           arbitrary_amount AS amount_alias
                    FROM arbitrary_loop_source AS source_alias
                    WHERE source_alias.arbitrary_bucket = lookup_bucket
                    ORDER BY arbitrary_code
                LOOP
                    SELECT count(arbitrary_id) INTO matching_rows
                    FROM arbitrary_loop_source
                    WHERE arbitrary_code = current_row.code_alias;
                    rendered_lines := rendered_lines || ':' || current_row.code_alias || '=' || current_row.amount_alias || '/' || matching_rows;
                END LOOP;
            END;
            $$
            "#,
        )
        .unwrap();

    assert_eq!(
        session
            .execute("CALL arbitrary_query_loop_probe(7)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(":42=5/1:84=9/1".to_string())]]
    );
    assert_eq!(
        session
            .execute("CALL arbitrary_query_loop_probe(9)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(String::new())]]
    );
}

#[test]
fn current_setting_exposes_postgres_version_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute("SELECT current_setting('server_version')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("18.4".to_string())]]
    );
    assert_eq!(
        session
            .execute("SELECT current_setting('server_version_num')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("180004".to_string())]]
    );
}

#[test]
fn jit_guc_matches_postgres_session_local_and_missing_setting_behavior() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session.execute("SHOW jit").unwrap().rows,
        vec![vec![SqlValue::String("off".to_string())]]
    );
    assert_eq!(
        session
            .execute("SELECT current_setting('jit')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("off".to_string())]]
    );
    assert_eq!(
        session
            .execute("SELECT set_config('jit', 'on', false)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("on".to_string())]]
    );
    assert_eq!(
        session.execute("SHOW jit").unwrap().rows,
        vec![vec![SqlValue::String("on".to_string())]]
    );

    session.execute("BEGIN").unwrap();
    session
        .execute("SELECT set_config('jit', 'off', true)")
        .unwrap();
    assert_eq!(
        session.execute("SHOW jit").unwrap().rows,
        vec![vec![SqlValue::String("off".to_string())]]
    );
    session.execute("COMMIT").unwrap();
    assert_eq!(
        session.execute("SHOW jit").unwrap().rows,
        vec![vec![SqlValue::String("on".to_string())]]
    );

    session.execute("RESET jit").unwrap();
    assert_eq!(
        session.execute("SHOW jit").unwrap().rows,
        vec![vec![SqlValue::String("off".to_string())]]
    );
    assert_eq!(
        session
            .execute("SELECT current_setting('missing.setting', true)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Null]]
    );
    let error = session
        .execute("SELECT current_setting('missing.setting')")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42704");
    assert!(error
        .to_string()
        .contains("unrecognized configuration parameter"));
}

#[test]
fn postgres_stat_database_exposes_transaction_counters() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let result = session
        .execute("SELECT sum(xact_commit + xact_rollback) FROM pg_stat_database")
        .unwrap();
    assert_eq!(result.columns, ["sum"]);
    assert_eq!(result.rows, vec![vec![SqlValue::Int(0)]]);
    assert_eq!(
        session
            .execute("SELECT datname, xact_commit, xact_rollback FROM pg_catalog.pg_stat_database")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("bicdb".to_string()),
            SqlValue::Int(0),
            SqlValue::Int(0)
        ]]
    );
}

#[test]
fn sample_rls_session_gucs_enforce_tenant_roles_and_write_checks() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE patients (
                id TEXT PRIMARY KEY,
                org_id TEXT,
                name TEXT,
                deleted_at TEXT
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO patients (id, org_id, name, deleted_at) VALUES
                ('p1', 'org-a', 'Ada', NULL),
                ('p2', 'org-a', 'Deleted', '2026-01-01'),
                ('p4', 'org-b', 'Bea', NULL)",
        )
        .unwrap();
    session
        .execute("ALTER TABLE patients ENABLE ROW LEVEL SECURITY")
        .unwrap();
    session
        .execute("ALTER TABLE patients FORCE ROW LEVEL SECURITY")
        .unwrap();
    let tenant_expr = "coalesce(current_setting('carrier.current_tenant', true), '') <> '' AND org_id::text = current_setting('carrier.current_tenant', true)";
    let admin_role_expr = "position(',platform_admin,' in replace(',' || coalesce(current_setting('carrier.current_roles', true), '') || ',', ',,', ',')) > 0 OR position(',org_admin,' in replace(',' || coalesce(current_setting('carrier.current_roles', true), '') || ',', ',,', ',')) > 0";
    session
        .execute(&format!(
            "CREATE POLICY patients_select_policy ON patients FOR SELECT USING ({tenant_expr} AND (({admin_role_expr}) OR deleted_at IS NULL))"
        ))
        .unwrap();
    session
        .execute(&format!(
            "CREATE POLICY patients_insert_policy ON patients FOR INSERT WITH CHECK ({tenant_expr} AND ({admin_role_expr}))"
        ))
        .unwrap();
    session
        .execute(&format!(
            "CREATE POLICY patients_update_policy ON patients FOR UPDATE USING ({tenant_expr} AND deleted_at IS NULL) WITH CHECK ({tenant_expr} AND deleted_at IS NULL)"
        ))
        .unwrap();
    session
        .execute(&format!(
            "CREATE POLICY patients_delete_policy ON patients FOR DELETE USING ({tenant_expr} AND ({admin_role_expr}))"
        ))
        .unwrap();

    assert_eq!(
        session
            .execute("SELECT current_setting('carrier.current_tenant', true)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Null]]
    );
    assert_eq!(
        session.execute("SELECT id FROM patients").unwrap().rows,
        Vec::<Vec<SqlValue>>::new()
    );
    assert!(matches!(
        session.execute("INSERT INTO patients (id, org_id, name, deleted_at) VALUES ('p0', 'org-a', 'No role', NULL)"),
        Err(SqlError::BicDb(BicDbError::Authorization(_)))
    ));

    drop(session);
    let mut session = SqlSession::new_secure(
        &mut db,
        SecurityContext::new("user-1", "org-a").with_roles(["org_admin"]),
    );
    assert!(matches!(
        session.execute("INSERT INTO patients (id, org_id, name, deleted_at) VALUES ('p3', 'org-b', 'Wrong tenant', NULL)"),
        Err(SqlError::BicDb(BicDbError::Authorization(_)))
    ));
    assert_eq!(
        session
            .execute("SELECT id FROM patients ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("p1".to_string())],
            vec![SqlValue::String("p2".to_string())],
        ]
    );

    drop(session);
    let mut session = SqlSession::new_secure(
        &mut db,
        SecurityContext::new("clinician-1", "org-a").with_roles(["clinician"]),
    );
    assert_eq!(
        session
            .execute("SELECT id FROM patients ORDER BY id")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("p1".to_string())]]
    );
    assert_eq!(
        session
            .execute("UPDATE patients SET name = 'Skipped' WHERE id = 'p2'")
            .unwrap()
            .command_tag,
        Some("UPDATE 0".to_string())
    );
    assert!(matches!(
        session.execute("UPDATE patients SET org_id = 'org-b' WHERE id = 'p1'"),
        Err(SqlError::BicDb(BicDbError::Authorization(_)))
    ));

    drop(session);
    let mut session = SqlSession::new_secure(
        &mut db,
        SecurityContext::new("user-1", "org-a").with_roles(["org_admin"]),
    );
    assert_eq!(
        session
            .execute("UPDATE patients SET name = 'Ada Updated' WHERE id = 'p1'")
            .unwrap()
            .command_tag,
        Some("UPDATE 1".to_string())
    );
    assert_eq!(
        session
            .execute("DELETE FROM patients WHERE id = 'p1'")
            .unwrap()
            .command_tag,
        Some("DELETE 1".to_string())
    );
    assert_eq!(
        session
            .execute("SELECT id FROM patients ORDER BY id")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("p2".to_string())]]
    );
}

#[test]
fn equality_predicates_do_not_use_full_text_indexes_as_btrees() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    SqlSession::new(&mut db)
        .execute(
            "CREATE TABLE insurance_coverages (
                id TEXT PRIMARY KEY,
                org_id TEXT NOT NULL,
                patient_profile_id TEXT NOT NULL
            );
            INSERT INTO insurance_coverages VALUES
                ('coverage-1', 'clinic-cell-demo', 'patient-1')",
        )
        .unwrap();
    db.create_index(IndexDefinition {
        name: "InsuranceCoverage_search".to_string(),
        collection: "insurance_coverages".to_string(),
        fields: vec![IndexField::MetadataPath(vec!["org_id".to_string()])],
        unique: false,
        kind: IndexKind::FullText,
        predicate: None,
        exclusion: None,
    })
    .unwrap();
    db.create_index(IndexDefinition {
        name: "idx_insurance_coverages_org_id".to_string(),
        collection: "insurance_coverages".to_string(),
        fields: vec![IndexField::MetadataPath(vec!["org_id".to_string()])],
        unique: false,
        kind: IndexKind::BTree,
        predicate: None,
        exclusion: None,
    })
    .unwrap();

    let mut sql = SqlSession::new(&mut db);
    let explain = sql
        .execute(
            "EXPLAIN SELECT id FROM insurance_coverages
             WHERE org_id = 'clinic-cell-demo'",
        )
        .unwrap();
    assert!(!explain.rows.iter().any(|row| {
        row[0]
            .to_cell()
            .contains("IndexScan InsuranceCoverage_search")
    }));
    assert_eq!(
        sql.execute(
            "SELECT id FROM insurance_coverages
             WHERE org_id = 'clinic-cell-demo'",
        )
        .unwrap()
        .rows,
        vec![vec![SqlValue::String("coverage-1".to_string())]]
    );
}

#[test]
fn rls_policies_execute_stored_scalar_functions() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE SCHEMA carrier_private;
             CREATE TABLE carrier_private.request_contexts (
                 payload JSONB NOT NULL
             );
             INSERT INTO carrier_private.request_contexts (payload)
             VALUES ('{\"tenant\":\"org-a\",\"roles\":[\"owner\"]}'::JSONB);
             CREATE FUNCTION carrier_private.current_context()
             RETURNS JSONB LANGUAGE SQL STABLE SECURITY DEFINER
             AS $$ SELECT payload FROM carrier_private.request_contexts $$;
             CREATE FUNCTION carrier_private.current_tenant()
             RETURNS TEXT LANGUAGE SQL STABLE SECURITY DEFINER
             AS $$ SELECT carrier_private.current_context() ->> 'tenant' $$;
             CREATE FUNCTION carrier_private.current_roles()
             RETURNS JSONB LANGUAGE SQL STABLE SECURITY DEFINER
             AS $$ SELECT carrier_private.current_context() -> 'roles' $$;
             CREATE TABLE records (
                 id TEXT PRIMARY KEY,
                 org_id TEXT NOT NULL
             );
             ALTER TABLE records ENABLE ROW LEVEL SECURITY;
             ALTER TABLE records FORCE ROW LEVEL SECURITY;
             CREATE POLICY records_insert_policy ON records
             FOR INSERT
             WITH CHECK (
                 org_id::text =
                 NULLIF((SELECT carrier_private.current_tenant()), '')
                 AND (SELECT carrier_private.current_roles()) ?| ARRAY['owner']::TEXT[]
             );
             CREATE POLICY records_select_policy ON records
             FOR SELECT
             USING (
                 org_id::text =
                 NULLIF((SELECT carrier_private.current_tenant()), '')
                 AND (SELECT carrier_private.current_roles()) ?| ARRAY['owner']::TEXT[]
             )",
        )
        .unwrap();

    session
        .execute("INSERT INTO records (id, org_id) VALUES ('r1', 'org-a')")
        .unwrap();
    assert!(matches!(
        session.execute("INSERT INTO records (id, org_id) VALUES ('r2', 'org-b')"),
        Err(SqlError::BicDb(BicDbError::Authorization(_)))
    ));
    assert_eq!(
        session.execute("SELECT id FROM records").unwrap().rows,
        vec![vec![SqlValue::String("r1".to_string())]]
    );
}

#[test]
fn rls_policies_preserve_the_session_runtime_and_transaction() {
    #[derive(Debug)]
    struct TestRuntime;

    impl bicdb_sql::SqlSessionRuntime for TestRuntime {
        fn backend_pid(&self) -> i32 {
            4242
        }

        fn execute_advisory_lock(
            &self,
            _name: &str,
            _args: &[SqlValue],
            _cancellation: &CancellationToken,
        ) -> bicdb_sql::Result<Option<SqlValue>> {
            Ok(None)
        }

        fn advisory_lock_rows(
            &self,
            _database_oid: i64,
        ) -> Vec<std::collections::BTreeMap<String, SqlValue>> {
            Vec::new()
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db).with_runtime(Arc::new(TestRuntime));

    session
        .execute(
            "CREATE SCHEMA carrier_private;
             CREATE TABLE carrier_private.request_contexts (
                 backend_pid INT NOT NULL,
                 transaction_id BIGINT NOT NULL,
                 payload JSONB NOT NULL
             );
             CREATE FUNCTION carrier_private.current_context()
             RETURNS JSONB LANGUAGE SQL STABLE SECURITY DEFINER
             AS $$
                 SELECT payload
                 FROM carrier_private.request_contexts
                 WHERE backend_pid = pg_backend_pid()
                   AND transaction_id = txid_current()
             $$;
             CREATE TABLE records (
                 id TEXT PRIMARY KEY,
                 org_id TEXT NOT NULL
             );
             ALTER TABLE records ENABLE ROW LEVEL SECURITY;
             ALTER TABLE records FORCE ROW LEVEL SECURITY;
             CREATE POLICY records_insert_policy ON records
             FOR INSERT
             WITH CHECK (
                 org_id::text =
                 NULLIF((SELECT carrier_private.current_context() ->> 'tenant'), '')
                 AND (SELECT carrier_private.current_context() -> 'roles')
                     ?| ARRAY['owner']::TEXT[]
             )",
        )
        .unwrap();

    session.execute("BEGIN").unwrap();
    let SqlValue::Int(transaction_id) =
        session.execute("SELECT txid_current()").unwrap().rows[0][0].clone()
    else {
        panic!("txid_current() did not return an integer");
    };
    session
        .execute(&format!(
            "INSERT INTO carrier_private.request_contexts
                 (backend_pid, transaction_id, payload)
             VALUES (
                 4242,
                 {transaction_id},
                 '{{\"tenant\":\"org-a\",\"roles\":[\"owner\"]}}'::JSONB
             )"
        ))
        .unwrap();
    session
        .execute("INSERT INTO records (id, org_id) VALUES ('r1', 'org-a')")
        .unwrap();
    session.execute("ROLLBACK").unwrap();
}

#[test]
fn stored_sql_functions_evaluate_integer_bitwise_and() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE FUNCTION unsigned_int4(value BIGINT)
             RETURNS OID LANGUAGE SQL IMMUTABLE
             AS $$ SELECT (value & 4294967295)::OID $$",
        )
        .unwrap();

    assert_eq!(
        session
            .execute("SELECT unsigned_int4(-123456789)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(4_171_510_507)]]
    );
}

#[test]
fn alter_column_type_using_evaluates_expressions_and_is_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let s = |value: &str| SqlValue::String(value.to_string());

    session
        .execute("CREATE TABLE patients (id TEXT PRIMARY KEY, age TEXT)")
        .unwrap();
    session
        .execute("CREATE INDEX patients_age_idx ON patients(age)")
        .unwrap();
    session
        .execute("INSERT INTO patients (id, age) VALUES ('p1', '41'), ('p2', 'bad')")
        .unwrap();

    let error = session
        .execute("ALTER TABLE patients ALTER COLUMN age TYPE INT")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42804");

    let error = session
        .execute("ALTER TABLE patients ALTER COLUMN age TYPE INT USING age::int")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "22P02");
    assert_eq!(
        session
            .execute("SELECT age, pg_typeof(age)::text FROM patients ORDER BY id")
            .unwrap()
            .rows,
        vec![vec![s("41"), s("text")], vec![s("bad"), s("text")]]
    );

    session
        .execute("UPDATE patients SET age = '8' WHERE id = 'p2'")
        .unwrap();
    session
        .execute("ALTER TABLE patients ALTER COLUMN age TYPE INT USING age::int + 1")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT age FROM patients ORDER BY id")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(42)], vec![SqlValue::Int(9)]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT format_type(a.atttypid, a.atttypmod), i.indclass::text
                 FROM pg_attribute a
                 JOIN pg_class c ON c.oid = a.attrelid
                 JOIN pg_index i ON i.indrelid = c.oid
                 JOIN pg_class ic ON ic.oid = i.indexrelid
                 WHERE c.relname = 'patients' AND a.attname = 'age'
                   AND ic.relname = 'patients_age_idx'",
            )
            .unwrap()
            .rows,
        vec![vec![s("integer"), s("1978")]]
    );

    session.execute("BEGIN").unwrap();
    session
        .execute("ALTER TABLE patients ALTER COLUMN age TYPE BIGINT")
        .unwrap();
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT format_type(a.atttypid, a.atttypmod)
                 FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
                 WHERE c.relname = 'patients' AND a.attname = 'age'",
            )
            .unwrap()
            .rows,
        vec![vec![s("integer")]]
    );

    session
        .execute("CREATE TABLE alter_fk_parent (id TEXT PRIMARY KEY)")
        .unwrap();
    session
        .execute(
            "CREATE TABLE alter_fk_child (
                parent_id TEXT REFERENCES alter_fk_parent(id)
             )",
        )
        .unwrap();
    session
        .execute("INSERT INTO alter_fk_parent VALUES ('1')")
        .unwrap();
    session
        .execute("INSERT INTO alter_fk_child VALUES ('1')")
        .unwrap();
    let error = session
        .execute(
            "ALTER TABLE alter_fk_parent
             ALTER COLUMN id TYPE VARCHAR USING id || 'x'",
        )
        .unwrap_err();
    assert_eq!(error.sqlstate(), "23503", "{error}");
    assert_eq!(
        session
            .execute("SELECT id, pg_typeof(id)::text FROM alter_fk_parent")
            .unwrap()
            .rows,
        vec![vec![s("1"), s("text")]]
    );
}

#[test]
fn alter_column_type_propagates_partition_rows_and_rejects_dependencies() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    let s = |value: &str| SqlValue::String(value.to_string());

    session
        .execute(
            "CREATE TABLE measurements (bucket INT, raw TEXT, qty INT)
             PARTITION BY LIST (bucket)",
        )
        .unwrap();
    session
        .execute("CREATE TABLE measurements_1 PARTITION OF measurements FOR VALUES IN (1)")
        .unwrap();
    session
        .execute("INSERT INTO measurements_1 (bucket, raw, qty) VALUES (1, '12', 3)")
        .unwrap();
    session
        .execute("ALTER TABLE measurements ALTER COLUMN raw TYPE INT USING raw::int + qty")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT raw, pg_typeof(raw)::text FROM measurements_1")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(15), s("integer")]]
    );

    let error = session
        .execute("ALTER TABLE measurements ALTER COLUMN bucket TYPE BIGINT")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42P16");
    let error = session
        .execute("ALTER TABLE measurements_1 ALTER COLUMN raw TYPE BIGINT")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42P16");

    session
        .execute(
            "CREATE TABLE generated_measurements (
                id INT PRIMARY KEY,
                qty INT,
                doubled INT GENERATED ALWAYS AS (qty * 2) STORED
             )",
        )
        .unwrap();
    session
        .execute("INSERT INTO generated_measurements (id, qty) VALUES (1, 3)")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT qty, doubled FROM generated_measurements")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(3), SqlValue::Int(6)]]
    );
    session
        .execute("UPDATE generated_measurements SET qty = 4 WHERE id = 1")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT qty, doubled FROM generated_measurements",)
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(4), SqlValue::Int(8)]]
    );
    session
        .execute(
            "INSERT INTO generated_measurements (id, qty) VALUES (1, 6)
             ON CONFLICT (id) DO UPDATE SET qty = excluded.qty, doubled = DEFAULT",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT qty, doubled FROM generated_measurements")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(6), SqlValue::Int(12)]]
    );
    let error = session
        .execute("UPDATE generated_measurements SET doubled = 99 WHERE id = 1")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "428C9");
    assert_eq!(
        session
            .execute(
                "SELECT is_generated, generation_expression
                 FROM information_schema.columns
                 WHERE table_name = 'generated_measurements' AND column_name = 'doubled'",
            )
            .unwrap()
            .rows,
        vec![vec![s("ALWAYS"), s("qty * 2")]]
    );
    let error = session
        .execute("ALTER TABLE generated_measurements ALTER COLUMN qty TYPE BIGINT")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "0A000");

    session
        .execute("CREATE VIEW measurement_values AS SELECT raw FROM measurements")
        .unwrap();
    let error = session
        .execute("ALTER TABLE measurements ALTER COLUMN raw TYPE BIGINT")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "0A000");
}

#[test]
fn create_drop_index_and_explain_use_index_scan() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT, age INT)")
        .unwrap();
    session
        .execute("INSERT INTO patients (id, name, age) VALUES ('p1', 'Asha', 45), ('p2', 'Ben', 40), ('p3', 'Chen', 45)")
        .unwrap();

    session
        .execute("CREATE INDEX idx_patients_age ON patients(age)")
        .unwrap();
    let plan = session
        .execute("EXPLAIN SELECT id FROM patients WHERE age = 45")
        .unwrap();
    assert!(plan
        .rows
        .iter()
        .any(|row| row[0].to_cell().contains("IndexScan idx_patients_age")));
    assert_eq!(
        session
            .execute("SELECT id FROM patients WHERE age = 45 ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("p1".to_string())],
            vec![SqlValue::String("p3".to_string())],
        ]
    );

    session
        .execute("DROP INDEX CONCURRENTLY idx_patients_age")
        .unwrap();
    let plan = session
        .execute("EXPLAIN SELECT id FROM patients WHERE age = 45")
        .unwrap();
    assert!(plan
        .rows
        .iter()
        .any(|row| row[0].to_cell().contains("FullScan")));
    assert!(plan
        .rows
        .iter()
        .any(|row| row[0].to_cell().contains("Cost")));
}

#[test]
fn indexed_range_and_order_by_timestamp_queries_work() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE wearable (id TEXT PRIMARY KEY, metric TEXT, value FLOAT8, timestamp INT)",
        )
        .unwrap();
    session
        .execute("INSERT INTO wearable (id, metric, value, timestamp) VALUES ('w1', 'hrv', 10.0, 100), ('w2', 'hrv', 20.0, 200), ('w3', 'steps', 30.0, 300)")
        .unwrap();
    session
        .execute("CREATE INDEX idx_wearable_timestamp ON wearable(timestamp)")
        .unwrap();

    let range_plan = session
        .execute("EXPLAIN SELECT id FROM wearable WHERE timestamp >= 200")
        .unwrap();
    assert!(range_plan.rows.iter().any(|row| {
        row[0]
            .to_cell()
            .contains("IndexRangeScan idx_wearable_timestamp")
    }));
    assert_eq!(
        session
            .execute("SELECT id FROM wearable WHERE timestamp >= 200 ORDER BY timestamp")
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("w2".to_string())],
            vec![SqlValue::String("w3".to_string())],
        ]
    );

    let order_plan = session
        .execute("EXPLAIN SELECT id FROM wearable ORDER BY timestamp DESC LIMIT 2")
        .unwrap();
    assert!(order_plan.rows.iter().any(|row| {
        row[0]
            .to_cell()
            .contains("OrderedIndexScan idx_wearable_timestamp")
    }));
    assert_eq!(
        session
            .execute("SELECT id FROM wearable ORDER BY timestamp DESC LIMIT 2")
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("w3".to_string())],
            vec![SqlValue::String("w2".to_string())],
        ]
    );
}

#[test]
fn explain_includes_cost_and_prefers_lower_cardinality_index() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE patients (id TEXT PRIMARY KEY, clinic TEXT, age INT)")
        .unwrap();
    session
        .execute(
            "INSERT INTO patients (id, clinic, age) VALUES
             ('p1', 'rural-7', 45),
             ('p2', 'rural-7', 40),
             ('p3', 'urban-2', 45),
             ('p4', 'rural-7', 45)",
        )
        .unwrap();
    session
        .execute("CREATE INDEX idx_patients_age ON patients(age)")
        .unwrap();
    session
        .execute("CREATE INDEX idx_patients_clinic_age ON patients(clinic, age)")
        .unwrap();

    let plan = session
        .execute("EXPLAIN SELECT id FROM patients WHERE clinic = 'rural-7' AND age = 45")
        .unwrap();
    assert!(plan.rows.iter().any(|row| {
        row[0]
            .to_cell()
            .contains("IndexScan idx_patients_clinic_age")
    }));
    assert!(plan
        .rows
        .iter()
        .any(|row| row[0].to_cell() == "EstimatedRows 2"));
    assert!(plan
        .rows
        .iter()
        .any(|row| row[0].to_cell().starts_with("Cost ")));
}

#[test]
fn analyze_persists_stats_and_planner_uses_selectivity() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(dir.path()).unwrap();
        {
            let mut session = SqlSession::new(&mut db);
            session
                .execute("CREATE TABLE erp_lines (id TEXT PRIMARY KEY, status TEXT, posted_at INT)")
                .unwrap();
        }
        db.batch_insert(
            "erp_lines",
            (0..120).map(|idx| {
                let status = if idx < 2 { "rare" } else { "common" };
                Record::new(format!("l{idx:03}")).with_metadata(json!({
                    "status": status,
                    "posted_at": idx,
                }))
            }),
        )
        .unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE INDEX idx_erp_lines_status ON erp_lines(status)")
            .unwrap();
        session
            .execute("CREATE INDEX idx_erp_lines_posted ON erp_lines(posted_at)")
            .unwrap();
        assert_eq!(
            session
                .execute("ANALYZE TABLE erp_lines")
                .unwrap()
                .command_tag
                .as_deref(),
            Some("ANALYZE")
        );

        let rare = session
            .execute("EXPLAIN SELECT id FROM erp_lines WHERE status = 'rare'")
            .unwrap();
        assert!(rare
            .rows
            .iter()
            .any(|row| row[0].to_cell().contains("IndexScan idx_erp_lines_status")));

        let common = session
            .execute("EXPLAIN SELECT id FROM erp_lines WHERE status = 'common'")
            .unwrap();
        assert!(common
            .rows
            .iter()
            .any(|row| row[0].to_cell().contains("FullScan")));

        let analyzed = session
            .execute("EXPLAIN ANALYZE SELECT id FROM erp_lines WHERE status = 'rare'")
            .unwrap();
        assert!(analyzed
            .rows
            .iter()
            .any(|row| row[0].to_cell() == "EstimatedRows 2"));
        assert!(analyzed
            .rows
            .iter()
            .any(|row| row[0].to_cell() == "ActualRows 2"));
    }

    let db = BicDb::open(dir.path()).unwrap();
    assert_eq!(db.table_statistics("erp_lines").unwrap().row_count, 120);
    assert!(SqlEngine::new(&db)
        .execute("EXPLAIN SELECT id FROM erp_lines WHERE status = 'common'")
        .unwrap()
        .rows
        .iter()
        .any(|row| row[0].to_cell().contains("FullScan")));
}

#[test]
fn typed_statistics_and_operator_classes_drive_catalogs_and_validation() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TYPE work_state AS ENUM ('open', 'closed')")
        .unwrap();
    session
        .execute("CREATE DOMAIN money_amount AS numeric CHECK (VALUE >= 0)")
        .unwrap();
    session
        .execute(
            "CREATE TABLE typed_planner_matrix (
                id uuid PRIMARY KEY,
                amount numeric,
                domain_amount money_amount,
                owner_id uuid,
                tags text[],
                payload jsonb,
                span int4range,
                state work_state,
                xml_doc xml
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO typed_planner_matrix VALUES
             ('00000000-0000-0000-0000-000000000001', 0.10, 0.10,
              '10000000-0000-0000-0000-000000000001', ARRAY['a'], '{\"a\":1}', '[1,3)', 'open', '<a/>'),
             ('00000000-0000-0000-0000-000000000002', 9007199254740993.01, 1.25,
              '10000000-0000-0000-0000-000000000002', ARRAY['a','b'], '{\"a\":2}', '[3,5)', 'open', '<b/>'),
             ('00000000-0000-0000-0000-000000000003', 9007199254740993.01, 2.50,
              '10000000-0000-0000-0000-000000000003', ARRAY['c'], '{\"a\":3}', '[5,7)', 'closed', '<c/>')",
        )
        .unwrap();
    let additional_rows = (0..100)
        .map(|value| {
            format!(
                "('20000000-0000-0000-0000-{value:012x}', {}, {}, \
                 '30000000-0000-0000-0000-{value:012x}', ARRAY['bulk'], \
                 '{{\"bulk\":{value}}}', '[{}, {})', '{}', '<bulk/>')",
                value + 100,
                value + 100,
                value + 100,
                value + 101,
                if value % 2 == 0 { "open" } else { "closed" },
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    session
        .execute(&format!(
            "INSERT INTO typed_planner_matrix VALUES {additional_rows}"
        ))
        .unwrap();

    for statement in [
        "CREATE INDEX typed_amount_idx ON typed_planner_matrix (amount)",
        "CREATE INDEX typed_domain_idx ON typed_planner_matrix (domain_amount)",
        "CREATE INDEX typed_owner_hash_idx ON typed_planner_matrix USING hash (owner_id)",
        "CREATE INDEX typed_tags_gin_idx ON typed_planner_matrix USING gin (tags)",
        "CREATE INDEX typed_payload_gin_idx ON typed_planner_matrix USING gin (payload jsonb_path_ops)",
        "CREATE INDEX typed_span_gist_idx ON typed_planner_matrix USING gist (span)",
        "CREATE INDEX typed_state_idx ON typed_planner_matrix (state)",
    ] {
        session.execute(statement).unwrap();
    }

    assert_eq!(
        session
            .execute("CREATE INDEX typed_xml_idx ON typed_planner_matrix (xml_doc)")
            .unwrap_err()
            .sqlstate(),
        "42704"
    );
    assert_eq!(
        session
            .execute(
                "CREATE INDEX typed_wrong_class_idx ON typed_planner_matrix (owner_id numeric_ops)"
            )
            .unwrap_err()
            .sqlstate(),
        "42804"
    );

    session.execute("ANALYZE typed_planner_matrix").unwrap();
    let range_plan = session
        .execute(
            "EXPLAIN ANALYZE SELECT id FROM typed_planner_matrix
             WHERE amount >= 9007199254740993.01",
        )
        .unwrap();
    assert!(
        range_plan
            .rows
            .iter()
            .any(|row| row[0].to_cell().contains("IndexRangeScan typed_amount_idx")),
        "unexpected typed range plan: {:?}",
        range_plan.rows
    );
    assert!(range_plan
        .rows
        .iter()
        .any(|row| row[0].to_cell() == "EstimatedRows 2"));
    assert!(range_plan
        .rows
        .iter()
        .any(|row| row[0].to_cell() == "ActualRows 2"));
    let catalog = session
        .execute(
            "SELECT c.relname, i.indclass::text
             FROM pg_catalog.pg_index AS i
             JOIN pg_catalog.pg_class AS c ON c.oid = i.indexrelid
             WHERE c.relname LIKE 'typed_%_idx'
             ORDER BY c.relname",
        )
        .unwrap();
    assert!(catalog.rows.iter().any(|row| {
        row == &vec![
            SqlValue::String("typed_amount_idx".to_string()),
            SqlValue::String("3125".to_string()),
        ]
    }));
    assert!(catalog.rows.iter().any(|row| {
        row == &vec![
            SqlValue::String("typed_owner_hash_idx".to_string()),
            SqlValue::String("10066".to_string()),
        ]
    }));
    assert!(catalog.rows.iter().any(|row| {
        row == &vec![
            SqlValue::String("typed_payload_gin_idx".to_string()),
            SqlValue::String("10091".to_string()),
        ]
    }));
    drop(session);

    let stats = db.table_statistics("typed_planner_matrix").unwrap();
    for column in ["amount", "domain_amount", "owner_id", "tags", "state"] {
        let column_stats = stats
            .columns
            .values()
            .find(|stats| stats.field == IndexField::MetadataPath(vec![column.to_string()]))
            .unwrap();
        assert!(
            column_stats.typed.is_some(),
            "missing typed statistics for {column}"
        );
    }
}

#[test]
fn postgres_analyze_verbose_table_refreshes_statistics() {
    let (_dir, mut db) = empty_test_db();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE \"p_ci_build_needs\" (id BIGINT PRIMARY KEY)")
            .unwrap();
        session
            .execute("INSERT INTO \"p_ci_build_needs\" (id) VALUES (1)")
            .unwrap();
        assert_eq!(
            session
                .execute(
                    "ANALYZE VERBOSE \"p_ci_build_needs\"
                     /*application:web,line:/lib/gitlab/database/partitioning/list/convert_table.rb:253*/",
                )
                .unwrap()
                .command_tag
                .as_deref(),
            Some("ANALYZE")
        );
    }

    assert_eq!(
        db.table_statistics("p_ci_build_needs").unwrap().row_count,
        1
    );
}

#[test]
fn analyze_drives_deterministic_join_order() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE erp_headers (id TEXT PRIMARY KEY, tenant TEXT)")
            .unwrap();
        session
            .execute("CREATE TABLE erp_lines (id TEXT PRIMARY KEY, header_id TEXT)")
            .unwrap();
    }
    db.batch_insert(
        "erp_headers",
        (0..60).map(|idx| Record::new(format!("h{idx:03}")).with_metadata(json!({"tenant": "t1"}))),
    )
    .unwrap();
    db.batch_insert(
        "erp_lines",
        (0..5).map(|idx| {
            Record::new(format!("line{idx:03}"))
                .with_metadata(json!({"header_id": format!("h{idx:03}")}))
        }),
    )
    .unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("ANALYZE TABLE erp_headers").unwrap();
    session.execute("ANALYZE TABLE erp_lines").unwrap();

    let plan = session
        .execute(
            "EXPLAIN SELECT erp_headers.id, erp_lines.id \
             FROM erp_headers JOIN erp_lines ON erp_headers.id = erp_lines.header_id",
        )
        .unwrap();
    assert!(plan
        .rows
        .iter()
        .any(|row| row[0].to_cell() == "JoinOrder erp_lines -> erp_headers"));
}

#[test]
fn erp_report_plan_stays_stable_until_analyze_refreshes_skewed_stats() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE erp_report_lines (id TEXT PRIMARY KEY, cost_center TEXT)")
            .unwrap();
    }
    db.batch_insert(
        "erp_report_lines",
        (0..80).map(|idx| {
            let cost_center = if idx == 0 { "skunkworks" } else { "ops" };
            Record::new(format!("r{idx:03}")).with_metadata(json!({"cost_center": cost_center}))
        }),
    )
    .unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE INDEX idx_report_cost_center ON erp_report_lines(cost_center)")
        .unwrap();
    session.execute("ANALYZE TABLE erp_report_lines").unwrap();
    let before = session
        .execute("EXPLAIN SELECT id FROM erp_report_lines WHERE cost_center = 'skunkworks'")
        .unwrap();
    assert!(before.rows.iter().any(|row| {
        row[0]
            .to_cell()
            .contains("IndexScan idx_report_cost_center")
    }));

    drop(session);
    db.batch_insert(
        "erp_report_lines",
        (80..160).map(|idx| {
            Record::new(format!("r{idx:03}")).with_metadata(json!({"cost_center": "skunkworks"}))
        }),
    )
    .unwrap();
    let mut session = SqlSession::new(&mut db);
    let stale = session
        .execute("EXPLAIN SELECT id FROM erp_report_lines WHERE cost_center = 'skunkworks'")
        .unwrap();
    assert!(stale.rows.iter().any(|row| {
        row[0]
            .to_cell()
            .contains("IndexScan idx_report_cost_center")
    }));

    session.execute("ANALYZE TABLE erp_report_lines").unwrap();
    let refreshed = session
        .execute("EXPLAIN SELECT id FROM erp_report_lines WHERE cost_center = 'skunkworks'")
        .unwrap();
    assert!(refreshed
        .rows
        .iter()
        .any(|row| row[0].to_cell().contains("FullScan")));
}

#[test]
fn composite_and_json_metadata_indexes_work() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE wearable (id TEXT PRIMARY KEY, device_id TEXT, metric TEXT, value FLOAT8)")
        .unwrap();
    session
        .execute("INSERT INTO wearable (id, device_id, metric, value) VALUES ('w1', 'band-1', 'hrv', 10.0), ('w2', 'band-1', 'steps', 20.0), ('w3', 'band-2', 'hrv', 30.0)")
        .unwrap();
    session
        .execute("CREATE INDEX idx_wearable_device_metric ON wearable(device_id, metric)")
        .unwrap();
    let plan = session
        .execute("EXPLAIN SELECT id FROM wearable WHERE device_id = 'band-1' AND metric = 'hrv'")
        .unwrap();
    assert!(plan.rows.iter().any(|row| {
        row[0]
            .to_cell()
            .contains("IndexScan idx_wearable_device_metric")
    }));
    assert!(plan
        .rows
        .iter()
        .any(|row| row[0].to_cell().contains("PrefixKeys 2")));
    assert_eq!(
        session
            .execute("SELECT id FROM wearable WHERE device_id = 'band-1' AND metric = 'hrv'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("w1".to_string())]]
    );

    session
        .execute("CREATE TABLE patients (id TEXT PRIMARY KEY, metadata JSONB)")
        .unwrap();
    session
        .execute("INSERT INTO patients (id, metadata) VALUES ('p1', '{\"clinic\":\"rural-7\"}'::jsonb), ('p2', '{\"clinic\":\"urban-2\"}'::jsonb)")
        .unwrap();
    session
        .execute("CREATE INDEX idx_metadata_clinic ON patients((metadata->>'clinic'))")
        .unwrap();
    let plan = session
        .execute("EXPLAIN SELECT id FROM patients WHERE metadata->>'clinic' = 'rural-7'")
        .unwrap();
    assert!(plan
        .rows
        .iter()
        .any(|row| row[0].to_cell().contains("IndexScan idx_metadata_clinic")));
    assert_eq!(
        session
            .execute("SELECT id FROM patients WHERE metadata->>'clinic' = 'rural-7'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("p1".to_string())]]
    );

    session
        .execute(
            "CREATE TABLE vulnerability_occurrences (
                id TEXT PRIMARY KEY,
                project_id bigint,
                report_type integer,
                location jsonb
            )",
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                "CREATE INDEX i_vuln_occurrences_on_proj_report_loc_dep_pkg_ver_file_img
                 ON vulnerability_occurrences USING btree (
                   project_id,
                   report_type,
                   ((((location -> 'dependency'::text) -> 'package'::text) ->> 'name'::text)),
                   (((location -> 'dependency'::text) ->> 'version'::text)),
                   COALESCE((location ->> 'file'::text), (location ->> 'image'::text))
                 )
                 WHERE (report_type = ANY (ARRAY[2, 1]))"
            )
            .unwrap()
            .command_complete_tag(),
        "CREATE INDEX"
    );
    let expression_indexes = session
        .execute(
            "SELECT indexprs
             FROM pg_catalog.pg_index
             WHERE indexprs IS NOT NULL",
        )
        .unwrap();
    assert!(expression_indexes.rows.iter().any(|row| {
        row[0].to_cell().contains("location")
            && row[0].to_cell().contains("dependency")
            && row[0].to_cell().contains("COALESCE")
    }));
}

#[test]
fn unique_lower_expression_indexes_enforce_normalized_keys() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE custom_field_select_options (
                id TEXT PRIMARY KEY,
                custom_field_id bigint,
                value text
            )",
        )
        .unwrap();
    session
        .execute(
            "CREATE UNIQUE INDEX idx_custom_field_select_options_on_custom_field_id_lower_value
             ON custom_field_select_options USING btree (custom_field_id, lower(value))",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO custom_field_select_options (id, custom_field_id, value)
             VALUES ('one', 1, 'Release')",
        )
        .unwrap();

    let duplicate = session
        .execute(
            "INSERT INTO custom_field_select_options (id, custom_field_id, value)
             VALUES ('two', 1, 'release')",
        )
        .unwrap_err()
        .to_string();
    assert!(duplicate.contains(
        "duplicate key value violates unique constraint \"idx_custom_field_select_options_on_custom_field_id_lower_value\""
    ));

    session
        .execute(
            "INSERT INTO custom_field_select_options (id, custom_field_id, value)
             VALUES ('three', 2, 'release')",
        )
        .unwrap();
    let plan = session
        .execute(
            "EXPLAIN SELECT id FROM custom_field_select_options
             WHERE custom_field_id = 1 AND lower(value) = 'release'",
        )
        .unwrap();
    assert!(plan.rows.iter().any(|row| {
        row[0]
            .to_cell()
            .contains("IndexScan idx_custom_field_select_options_on_custom_field_id_lower_value")
    }));
    assert_eq!(
        session
            .execute(
                "SELECT id FROM custom_field_select_options
                 WHERE custom_field_id = 1 AND lower(value) = 'release'",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("one".to_string())]]
    );

    session
        .execute("CREATE TABLE work_item_types (id TEXT PRIMARY KEY, name text)")
        .unwrap();
    session
        .execute(
            "CREATE UNIQUE INDEX index_work_item_types_on_name_unique
             ON work_item_types USING btree (TRIM(BOTH FROM lower(name)))",
        )
        .unwrap();
    session
        .execute("INSERT INTO work_item_types (id, name) VALUES ('task', ' Task ')")
        .unwrap();
    let trim_duplicate = session
        .execute("INSERT INTO work_item_types (id, name) VALUES ('task-2', 'task')")
        .unwrap_err()
        .to_string();
    assert!(trim_duplicate.contains(
        "duplicate key value violates unique constraint \"index_work_item_types_on_name_unique\""
    ));
}

#[test]
fn unique_indexes_allow_multiple_null_values() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE batched_background_migrations (
                id TEXT PRIMARY KEY,
                queued_migration_version bigint
            )",
        )
        .unwrap();
    session
        .execute(
            "CREATE UNIQUE INDEX unique_batched_background_migrations_queued_migration_version
             ON batched_background_migrations (queued_migration_version)",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO batched_background_migrations (id, queued_migration_version)
             VALUES ('one', NULL), ('two', NULL)",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO batched_background_migrations (id, queued_migration_version)
             VALUES ('three', 20251014095615)",
        )
        .unwrap();

    let duplicate = session
        .execute(
            "INSERT INTO batched_background_migrations (id, queued_migration_version)
             VALUES ('four', 20251014095615)",
        )
        .unwrap_err()
        .to_string();
    assert!(duplicate.contains(
        "duplicate key value violates unique constraint \"unique_batched_background_migrations_queued_migration_version\""
    ));
}

#[test]
fn sample_weighted_fts_indexes_are_executable_and_maintained() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE orgs (
                id UUID PRIMARY KEY,
                org_id TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO orgs VALUES
                ('00000000-0000-0000-0000-000000000001', 'north-clinic', 'Sample Clinic'),
                ('00000000-0000-0000-0000-000000000002', 'south-clinic', 'South Clinic')",
        )
        .unwrap();
    session
        .execute(
            "CREATE INDEX IF NOT EXISTS idx_orgs_search_fts
             ON orgs USING GIN ((setweight(to_tsvector('english', COALESCE(org_id, '')), 'A') || setweight(to_tsvector('english', COALESCE(name, '')), 'B')))",
        )
        .unwrap();
    session
        .execute(
            "CREATE TABLE diagnosis_catalog_items (
                id UUID PRIMARY KEY,
                code TEXT NOT NULL,
                display_name TEXT NOT NULL,
                short_description TEXT,
                category TEXT
            )",
        )
        .unwrap();
    session
        .execute(
            "CREATE INDEX IF NOT EXISTS idx_diagnosis_catalog_items_search_fts
             ON diagnosis_catalog_items USING GIN ((setweight(to_tsvector('english', COALESCE(code, '')), 'A') || setweight(to_tsvector('english', COALESCE(display_name, '')), 'B') || setweight(to_tsvector('english', COALESCE(short_description, '')), 'C') || setweight(to_tsvector('english', COALESCE(category, '')), 'D')))",
        )
        .unwrap();

    let catalog = session
        .execute(
            "SELECT relname, amname
             FROM pg_catalog.pg_class c
             JOIN pg_catalog.pg_am am ON c.relam = am.oid
             WHERE relname = 'idx_orgs_search_fts'",
        )
        .unwrap();
    assert_eq!(
        catalog.rows,
        vec![vec![
            SqlValue::String("idx_orgs_search_fts".to_string()),
            SqlValue::String("gin".to_string()),
        ]]
    );

    let indexes = session
        .execute(
            "SELECT indkey, indexprs
             FROM pg_catalog.pg_index
             WHERE indexprs IS NOT NULL
             ORDER BY indexprs",
        )
        .unwrap();
    assert_eq!(indexes.rows.len(), 2);
    assert!(indexes.rows.iter().any(|row| {
        row[0] == SqlValue::String("0".to_string())
            && row[1].to_cell().contains("COALESCE(org_id, '')")
            && row[1].to_cell().contains("COALESCE(name, '')")
    }));
    assert!(indexes.rows.iter().any(|row| {
        row[0] == SqlValue::String("0".to_string())
            && row[1].to_cell().contains("COALESCE(display_name, '')")
            && row[1].to_cell().contains("COALESCE(category, '')")
    }));

    let weighted_expression = "setweight(to_tsvector('english', COALESCE(org_id, '')), 'A') || \
         setweight(to_tsvector('english', COALESCE(name, '')), 'B')";
    let explain = session
        .execute(&format!(
            "EXPLAIN SELECT org_id FROM orgs
             WHERE ({weighted_expression}) @@ to_tsquery('english', 'sample')"
        ))
        .unwrap();
    assert!(explain.rows.iter().any(|row| {
        row[0]
            .to_cell()
            .contains("FullTextIndexScan idx_orgs_search_fts")
    }));
    assert!(explain
        .rows
        .iter()
        .any(|row| row[0] == SqlValue::String("EstimatedRows 1".to_string())));
    assert_eq!(
        session
            .execute(&format!(
                "SELECT org_id FROM orgs
                 WHERE ({weighted_expression}) @@ websearch_to_tsquery('english', 'sample')
                 ORDER BY org_id"
            ))
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("north-clinic".to_string())]]
    );

    session
        .execute("UPDATE orgs SET name = 'Renamed Health' WHERE org_id = 'north-clinic'")
        .unwrap();
    assert!(session
        .execute(&format!(
            "SELECT org_id FROM orgs
                 WHERE ({weighted_expression}) @@ to_tsquery('english', 'sample')"
        ))
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(
        session
            .execute(&format!(
                "SELECT org_id FROM orgs
                 WHERE ({weighted_expression}) @@ to_tsquery('english', 'renam & health')"
            ))
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("north-clinic".to_string())]]
    );

    drop(session);
    assert!(db
        .index_definitions()
        .iter()
        .any(|index| { index.name == "idx_orgs_search_fts" && index.kind == IndexKind::FullText }));
    assert_eq!(
        db.lookup_full_text_term("idx_orgs_search_fts", "renam", false)
            .unwrap()
            .len(),
        1
    );

    drop(db);
    let mut reopened = BicDb::open(dir.path()).unwrap();
    assert_eq!(
        SqlSession::new(&mut reopened)
            .execute(&format!(
                "SELECT org_id FROM orgs
                 WHERE ({weighted_expression}) @@ to_tsquery('english', 'renam')"
            ))
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("north-clinic".to_string())]]
    );
}

#[test]
fn full_text_gin_and_gist_indexes_cover_query_shapes_statistics_and_transactions() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        session
            .execute("SELECT 'not a vector' @@ to_tsquery('simple', 'vector')")
            .unwrap_err()
            .sqlstate(),
        "42883"
    );
    session
        .execute("CREATE TABLE documents (id TEXT PRIMARY KEY, search TSVECTOR NOT NULL)")
        .unwrap();
    session
        .execute(
            "INSERT INTO documents VALUES
                ('a', to_tsvector('english', 'fat cat sat')),
                ('b', to_tsvector('english', 'dog chased cats')),
                ('c', to_tsvector('english', 'quiet bird'))",
        )
        .unwrap();
    session
        .execute("CREATE INDEX idx_documents_search ON documents USING GIN (search)")
        .unwrap();

    for (query, expected) in [
        ("to_tsquery('english', 'cat:*')", vec!["a", "b"]),
        ("to_tsquery('english', 'dog | bird')", vec!["b", "c"]),
        ("phraseto_tsquery('english', 'fat cat')", vec!["a"]),
        ("to_tsquery('english', 'cat & !dog')", vec!["a"]),
    ] {
        assert_eq!(
            session
                .execute(&format!(
                    "SELECT id FROM documents WHERE search @@ {query} ORDER BY id"
                ))
                .unwrap()
                .rows,
            expected
                .into_iter()
                .map(|id| vec![SqlValue::String(id.to_string())])
                .collect::<Vec<_>>()
        );
    }

    session.execute("BEGIN").unwrap();
    session
        .execute(
            "UPDATE documents SET search = to_tsvector('english', 'new phrase') WHERE id = 'a'",
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT id FROM documents
                 WHERE search @@ to_tsquery('english', 'new') ORDER BY id",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("a".to_string())]]
    );
    session.execute("COMMIT").unwrap();
    session.execute("ANALYZE documents").unwrap();

    session
        .execute("CREATE TABLE gist_documents (id TEXT PRIMARY KEY, search TSVECTOR NOT NULL)")
        .unwrap();
    session
        .execute("CREATE INDEX idx_gist_documents_search ON gist_documents USING GIST (search)")
        .unwrap();
    session
        .execute(
            "INSERT INTO gist_documents VALUES
                ('g1', to_tsvector('simple', 'alpha beta')),
                ('g2', to_tsvector('simple', 'gamma'))",
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT id FROM gist_documents
                 WHERE search @@ to_tsquery('simple', 'alpha')",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("g1".to_string())]]
    );

    drop(session);
    let stats = db.table_statistics("documents").unwrap();
    let index_stats = stats.indexes.get("idx_documents_search").unwrap();
    assert_eq!(index_stats.indexed_rows, 3);
    assert!(index_stats.distinct_keys >= 6);
    assert!(db.verify_index("idx_documents_search").unwrap().valid);
    assert!(db.verify_index("idx_gist_documents_search").unwrap().valid);
}

#[test]
fn indexes_survive_reopen_and_sql_rollback_keeps_them_clean() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE patients (id TEXT PRIMARY KEY, age INT)")
            .unwrap();
        session
            .execute("INSERT INTO patients (id, age) VALUES ('p1', 41)")
            .unwrap();
        session
            .execute("CREATE INDEX idx_patients_age ON patients(age)")
            .unwrap();
        session.execute("BEGIN").unwrap();
        session
            .execute("INSERT INTO patients (id, age) VALUES ('p2', 41)")
            .unwrap();
        session.execute("ROLLBACK").unwrap();
        assert_eq!(
            session
                .execute("SELECT id FROM patients WHERE age = 41")
                .unwrap()
                .rows,
            vec![vec![SqlValue::String("p1".to_string())]]
        );
    }

    let db = BicDb::open(dir.path()).unwrap();
    assert!(db.verify_index("idx_patients_age").unwrap().valid);
    assert_eq!(
        SqlEngine::new(&db)
            .execute("SELECT id FROM patients WHERE age = 41")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("p1".to_string())]]
    );
}

#[test]
fn create_table_if_not_exists_preserves_existing_index_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE IF NOT EXISTS background_jobs (id UUID PRIMARY KEY, org_id TEXT NOT NULL)")
        .unwrap();
    session
        .execute(
            "CREATE INDEX IF NOT EXISTS idx_background_jobs_org_id ON background_jobs (org_id)",
        )
        .unwrap();
    session
        .execute("CREATE TABLE IF NOT EXISTS background_jobs (id UUID PRIMARY KEY, org_id TEXT NOT NULL)")
        .unwrap();
    session
        .execute(
            "CREATE INDEX IF NOT EXISTS idx_background_jobs_org_id ON background_jobs (org_id)",
        )
        .unwrap();

    let indexes = session
        .execute(
            "SELECT relname
             FROM pg_catalog.pg_class
             WHERE relkind = 'i' AND relname = 'idx_background_jobs_org_id'",
        )
        .unwrap();
    assert_eq!(
        indexes.rows,
        vec![vec![SqlValue::String(
            "idx_background_jobs_org_id".to_string()
        )]]
    );
}

#[test]
fn local_transactions_commit_and_rollback_buffer_writes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT)")
        .unwrap();

    session.execute("BEGIN").unwrap();
    session
        .execute("INSERT INTO patients (id, name) VALUES ('p1', 'Rollback')")
        .unwrap();
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session
            .execute("SELECT COUNT(*) FROM patients")
            .unwrap()
            .rows[0][0],
        SqlValue::Int(0)
    );

    session.execute("BEGIN").unwrap();
    session
        .execute("INSERT INTO patients (id, name) VALUES ('p2', 'Commit')")
        .unwrap();
    session.execute("COMMIT").unwrap();
    assert_eq!(
        session
            .execute("SELECT name FROM patients WHERE id = 'p2'")
            .unwrap()
            .rows[0][0],
        SqlValue::String("Commit".to_string())
    );
}

#[test]
fn local_transactions_read_own_writes_through_key_and_index_paths() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE arbitrary_tx_overlay_rows (
                scope_key INT,
                item_key INT,
                lookup_key INT,
                payload TEXT,
                PRIMARY KEY (scope_key, item_key)
            )",
        )
        .unwrap();
    session
        .execute(
            "CREATE INDEX arbitrary_tx_overlay_lookup_idx
             ON arbitrary_tx_overlay_rows (lookup_key)",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO arbitrary_tx_overlay_rows
             (scope_key, item_key, lookup_key, payload)
             VALUES (7, 1, 10, 'original')",
        )
        .unwrap();

    session.execute("BEGIN").unwrap();
    session
        .execute(
            "INSERT INTO arbitrary_tx_overlay_rows
             (scope_key, item_key, lookup_key, payload)
             VALUES (7, 2, 20, 'inserted')",
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT payload
                 FROM arbitrary_tx_overlay_rows
                 WHERE scope_key = 7 AND item_key = 2"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("inserted".to_string())]]
    );
    assert_eq!(
        session
            .execute("SELECT COUNT(*) FROM arbitrary_tx_overlay_rows WHERE lookup_key = 20")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT MAX(item_key)
                 FROM arbitrary_tx_overlay_rows
                 WHERE scope_key = 7"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(2)]]
    );

    session
        .execute(
            "UPDATE arbitrary_tx_overlay_rows
             SET lookup_key = 30, payload = 'updated'
             WHERE scope_key = 7 AND item_key = 2",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT COUNT(*) FROM arbitrary_tx_overlay_rows WHERE lookup_key = 20")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(0)]]
    );
    assert_eq!(
        session
            .execute("SELECT payload FROM arbitrary_tx_overlay_rows WHERE lookup_key = 30")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("updated".to_string())]]
    );

    session
        .execute(
            "UPDATE arbitrary_tx_overlay_rows
             SET lookup_key = 40, payload = 'changed'
             WHERE scope_key = 7 AND item_key = 1",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT COUNT(*) FROM arbitrary_tx_overlay_rows WHERE lookup_key = 10")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(0)]]
    );
    assert_eq!(
        session
            .execute("SELECT payload FROM arbitrary_tx_overlay_rows WHERE lookup_key = 40")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("changed".to_string())]]
    );

    session
        .execute(
            "DELETE FROM arbitrary_tx_overlay_rows
             WHERE scope_key = 7 AND item_key = 2",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT COUNT(*) FROM arbitrary_tx_overlay_rows WHERE lookup_key = 30")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(0)]]
    );
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT item_key, lookup_key, payload
                 FROM arbitrary_tx_overlay_rows
                 ORDER BY item_key"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(1),
            SqlValue::Int(10),
            SqlValue::String("original".to_string()),
        ]]
    );
}

#[test]
fn secure_sessions_read_their_own_transaction_writes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut setup = SqlSession::new(&mut db);
        setup
            .execute(
                "CREATE TABLE secure_tx_overlay_rows (
                    row_key TEXT PRIMARY KEY,
                    lookup_key INT UNIQUE,
                    payload TEXT NOT NULL
                )",
            )
            .unwrap();
    }

    let context = SecurityContext::new("runtime-user", "tenant-a").with_roles(["runtime"]);
    let mut session = SqlSession::new_secure(&mut db, context);
    session.execute("BEGIN").unwrap();
    session
        .execute(
            "INSERT INTO secure_tx_overlay_rows (row_key, lookup_key, payload)
             VALUES ('context-key', 42, 'pending')",
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT EXISTS (
                    SELECT 1 FROM secure_tx_overlay_rows
                    WHERE row_key = 'context-key'
                )"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]]
    );
    assert_eq!(
        session
            .execute("SELECT COUNT(*) FROM secure_tx_overlay_rows WHERE lookup_key = 42")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );

    session
        .execute(
            "INSERT INTO secure_tx_overlay_rows (row_key, lookup_key, payload)
             VALUES ('context-key', 42, 'replaced')
             ON CONFLICT (row_key) DO UPDATE SET payload = EXCLUDED.payload",
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT payload FROM secure_tx_overlay_rows
                 WHERE row_key = 'context-key'"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("replaced".to_string())]]
    );

    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session
            .execute("SELECT COUNT(*) FROM secure_tx_overlay_rows")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(0)]]
    );
}

#[test]
fn mutating_plpgsql_call_reads_pending_writes_and_rolls_back_on_error() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE arbitrary_call_atomic_rows (
                row_key INT PRIMARY KEY,
                lookup_key INT,
                payload TEXT
            )",
        )
        .unwrap();
    session
        .execute(
            "CREATE INDEX arbitrary_call_atomic_lookup_idx
             ON arbitrary_call_atomic_rows (lookup_key)",
        )
        .unwrap();
    session
        .execute(
            "CREATE PROCEDURE arbitrary_call_atomic_probe(
                first_key IN INTEGER,
                second_key IN INTEGER,
                observed_count INOUT INTEGER
            )
            AS $$
            BEGIN
                INSERT INTO arbitrary_call_atomic_rows (row_key, lookup_key, payload)
                VALUES (first_key, 7, 'first');
                INSERT INTO arbitrary_call_atomic_rows (row_key, lookup_key, payload)
                VALUES (second_key, 7, 'second');
                SELECT COUNT(*)
                INTO observed_count
                FROM arbitrary_call_atomic_rows
                WHERE lookup_key = 7;
                UPDATE arbitrary_call_atomic_rows
                SET payload = 'updated'
                WHERE row_key = first_key;
            END;
            $$
            LANGUAGE 'plpgsql'",
        )
        .unwrap();
    session
        .execute(
            "CREATE PROCEDURE arbitrary_call_atomic_failure(failing_key IN INTEGER)
            AS $$
            BEGIN
                INSERT INTO arbitrary_call_atomic_rows (row_key, lookup_key, payload)
                VALUES (failing_key, 99, 'before-error');
                UPDATE arbitrary_call_atomic_missing_rows
                SET payload = 'missing-table-error'
                WHERE row_key = failing_key;
            END;
            $$
            LANGUAGE 'plpgsql'",
        )
        .unwrap();

    assert_eq!(
        session
            .execute("CALL arbitrary_call_atomic_probe(11, 12, 0)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(2)]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT row_key, lookup_key, payload
                 FROM arbitrary_call_atomic_rows
                 ORDER BY row_key"
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::Int(11),
                SqlValue::Int(7),
                SqlValue::String("updated".to_string()),
            ],
            vec![
                SqlValue::Int(12),
                SqlValue::Int(7),
                SqlValue::String("second".to_string()),
            ],
        ]
    );

    let error = session
        .execute("CALL arbitrary_call_atomic_failure(99)")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("arbitrary_call_atomic_missing_rows"));
    assert_eq!(
        session
            .execute("SELECT COUNT(*) FROM arbitrary_call_atomic_rows WHERE lookup_key = 99")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(0)]]
    );
}

#[test]
fn sql_procedure_executes_a_single_quoted_body() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE PROCEDURE carrier_touch(text)
             LANGUAGE sql AS 'SELECT lower($1)'",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("CALL carrier_touch('Touched')")
            .unwrap()
            .command_complete_tag(),
        "CALL"
    );
}
