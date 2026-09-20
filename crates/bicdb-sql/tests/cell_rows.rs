//! Cell-row read path: schema-typed rows are read as borrowed cells from the
//! resident JSON text instead of a parsed `Value` tree. The path is invisible
//! when it works, so these tests pin the value each storage shape must project
//! (NUMERIC/temporal envelopes, plain scalars, CHAR padding, NULLs, composite
//! primary keys) through every entry point it serves: cached point lookups,
//! index-located row sets, rows this transaction has already rewritten, table
//! aliases, and the per-row fallback for columns cells cannot represent.

use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::{SqlSession, SqlValue};
use tempfile::TempDir;

fn seeded() -> (TempDir, BicDb) {
    let dir = TempDir::new().unwrap();
    let mut db =
        BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute(
            "CREATE TABLE stock (s_i_id INT NOT NULL, s_w_id INT NOT NULL, s_quantity SMALLINT, \
             s_dist_01 CHAR(24), s_ytd NUMERIC, s_order_cnt INT, s_remote_cnt INT, \
             s_data VARCHAR(50), s_price NUMERIC(5,2), s_ratio FLOAT8, s_active BOOLEAN, \
             s_since TIMESTAMP, s_note TEXT, PRIMARY KEY (s_w_id, s_i_id))",
        )
        .unwrap();
        for i in 1..=8 {
            sql.execute(&format!(
                "INSERT INTO stock VALUES ({i}, 1, {}, 'dist{i}', {}.5, {i}, 0, 'data \"{i}\"', \
                 {i}.5, 0.25, {}, TIMESTAMP '2026-09-02 05:28:0{}', {})",
                10 + i,
                i * 100,
                if i % 2 == 0 { "TRUE" } else { "FALSE" },
                i % 10,
                if i == 3 {
                    "NULL".to_string()
                } else {
                    format!("'n{i}'")
                },
            ))
            .unwrap();
        }
        sql.execute("CREATE INDEX stock_qty ON stock (s_quantity)")
            .unwrap();
        sql.execute(
            "CREATE TABLE tagged (id INT PRIMARY KEY, name TEXT, attrs JSONB, price NUMERIC(6,2))",
        )
        .unwrap();
        sql.execute(
            "INSERT INTO tagged VALUES (1, 'one', '{\"k\": [1, 2]}', 10.5), (2, 'two', NULL, 20)",
        )
        .unwrap();
    }
    (dir, db)
}

fn run(db: &mut BicDb, sql: &str) -> Vec<Vec<SqlValue>> {
    SqlSession::new(db).execute(sql).unwrap().rows
}

fn cells(rows: &[Vec<SqlValue>]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| {
            row.iter()
                .map(|value| match value {
                    SqlValue::Null => "NULL".to_string(),
                    other => other.to_cell(),
                })
                .collect()
        })
        .collect()
}

#[test]
fn point_lookup_projects_every_storage_shape() {
    let (_dir, mut db) = seeded();
    let rows = run(
        &mut db,
        "SELECT s_i_id, s_w_id, s_quantity, s_dist_01, s_ytd, s_data, s_price, s_ratio, \
         s_active, s_since, s_note FROM stock WHERE s_w_id = 1 AND s_i_id = 3",
    );
    assert_eq!(
        cells(&rows),
        vec![vec![
            "3".to_string(),
            "1".to_string(),
            "13".to_string(),
            "dist3                   ".to_string(),
            "300.5".to_string(),
            "data \"3\"".to_string(),
            "3.50".to_string(),
            "0.25".to_string(),
            "false".to_string(),
            "2026-09-02 05:28:03".to_string(),
            "NULL".to_string(),
        ]]
    );
    assert!(matches!(rows[0][0], SqlValue::Int(3)));
    assert!(matches!(rows[0][7], SqlValue::Float(f) if f == 0.25));
    assert!(matches!(rows[0][8], SqlValue::Bool(false)));
}

#[test]
fn index_located_rows_and_aliases_match_the_record_path() {
    let (_dir, mut db) = seeded();
    let rows = run(
        &mut db,
        "SELECT st.s_i_id, st.s_price, st.s_since, s_quantity FROM stock AS st \
         WHERE st.s_quantity >= 15 ORDER BY st.s_i_id",
    );
    let expected: Vec<Vec<String>> = [
        ["5", "5.50", "2026-09-02 05:28:05", "15"],
        ["6", "6.50", "2026-09-02 05:28:06", "16"],
        ["7", "7.50", "2026-09-02 05:28:07", "17"],
        ["8", "8.50", "2026-09-02 05:28:08", "18"],
    ]
    .iter()
    .map(|row| row.iter().map(|cell| cell.to_string()).collect())
    .collect();
    assert_eq!(cells(&rows), expected);
    // Whole-row and system-column projections still work through the path.
    let rows = run(
        &mut db,
        "SELECT ctid IS NOT NULL, count(*) OVER () FROM stock WHERE s_quantity = 11",
    );
    assert_eq!(
        cells(&rows),
        vec![vec!["true".to_string(), "1".to_string()]]
    );
}

#[test]
fn rows_rewritten_in_the_same_transaction_are_read_back_updated() {
    let (_dir, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("BEGIN").unwrap();
    sql.execute(
        "UPDATE stock SET s_quantity = s_quantity - 5, s_ytd = s_ytd + 2.25, \
         s_price = 99.99 WHERE s_w_id = 1 AND s_i_id = 4",
    )
    .unwrap();
    let rows = sql
        .execute(
            "SELECT s_quantity, s_ytd, s_price, s_data FROM stock WHERE s_w_id = 1 AND s_i_id = 4",
        )
        .unwrap()
        .rows;
    assert_eq!(
        cells(&rows),
        vec![vec![
            "9".to_string(),
            "402.75".to_string(),
            "99.99".to_string(),
            "data \"4\"".to_string(),
        ]]
    );
    // Index-located set inside the same transaction sees the pending row too.
    let rows = sql
        .execute("SELECT s_i_id, s_quantity FROM stock WHERE s_quantity < 11 ORDER BY s_i_id")
        .unwrap()
        .rows;
    assert_eq!(cells(&rows), vec![vec!["4".to_string(), "9".to_string()]]);
    sql.execute("COMMIT").unwrap();
    let rows = run(
        &mut db,
        "SELECT s_quantity, s_price FROM stock WHERE s_w_id = 1 AND s_i_id = 4",
    );
    assert_eq!(
        cells(&rows),
        vec![vec!["9".to_string(), "99.99".to_string()]]
    );
}

#[test]
fn json_columns_fall_back_per_row_without_losing_typed_neighbours() {
    let (_dir, mut db) = seeded();
    let rows = run(
        &mut db,
        "SELECT id, name, attrs, price FROM tagged WHERE id IN (1, 2) ORDER BY id",
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1], SqlValue::String("one".to_string()));
    assert_eq!(rows[0][3], SqlValue::String("10.50".to_string()));
    assert!(matches!(&rows[0][2], SqlValue::Json(json) if json["k"][1] == 2));
    assert_eq!(rows[1][2], SqlValue::Null);
    assert_eq!(rows[1][3], SqlValue::String("20.00".to_string()));
    let rows = run(&mut db, "SELECT attrs->'k'->>0 FROM tagged WHERE id = 1");
    assert_eq!(cells(&rows), vec![vec!["1".to_string()]]);
}

#[test]
fn procedure_point_reads_see_typed_values() {
    let (_dir, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE OR REPLACE PROCEDURE take_stock(w INT, i INT, q INT, INOUT price NUMERIC, \
         INOUT dist CHAR(24)) LANGUAGE plpgsql AS $$
         DECLARE cur INT;
         BEGIN
           SELECT s_quantity, s_price, s_dist_01 INTO cur, price, dist FROM stock \
             WHERE s_w_id = w AND s_i_id = i;
           UPDATE stock SET s_quantity = cur - q WHERE s_w_id = w AND s_i_id = i;
           SELECT s_price INTO price FROM stock WHERE s_w_id = w AND s_i_id = i;
         END $$",
    )
    .unwrap();
    let rows = sql
        .execute("CALL take_stock(1, 2, 3, NULL, NULL)")
        .unwrap()
        .rows;
    assert_eq!(
        cells(&rows),
        vec![vec![
            "2.50".to_string(),
            "dist2                   ".to_string()
        ]]
    );
    let rows = sql
        .execute("SELECT s_quantity FROM stock WHERE s_w_id = 1 AND s_i_id = 2")
        .unwrap()
        .rows;
    assert_eq!(cells(&rows), vec![vec!["9".to_string()]]);
}

#[test]
fn joins_fetch_right_tables_through_cells() {
    let (_dir, mut db) = seeded();
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute(
            "CREATE TABLE warehouse (w_id INT PRIMARY KEY, w_tax NUMERIC(4,4), w_name VARCHAR(10))",
        )
        .unwrap();
        sql.execute("INSERT INTO warehouse VALUES (1, 0.0750, 'main')")
            .unwrap();
    }
    // Prepared primary-key right join (the procedure shape) and a generic
    // join with a residual constraint both project typed right-side cells.
    let rows = run(
        &mut db,
        "SELECT s.s_i_id, w.w_tax, w.w_name, s.s_price FROM stock s, warehouse w \
         WHERE w.w_id = 1 AND s.s_w_id = w.w_id AND s.s_i_id = 2",
    );
    assert_eq!(
        cells(&rows),
        vec![vec![
            "2".to_string(),
            "0.0750".to_string(),
            "main".to_string(),
            "2.50".to_string(),
        ]]
    );
    let rows = run(
        &mut db,
        "SELECT s.s_i_id, w.w_name FROM stock s JOIN warehouse w ON w.w_id = s.s_w_id \
         AND w.w_tax < s.s_price WHERE s.s_quantity >= 17 ORDER BY s.s_i_id",
    );
    assert_eq!(
        cells(&rows),
        vec![
            vec!["7".to_string(), "main".to_string()],
            vec!["8".to_string(), "main".to_string()],
        ]
    );
}

#[test]
fn routine_update_batch_preserves_missing_null_and_duplicate_source_positions() {
    let (_dir, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE PROCEDURE bump_stock_batch() LANGUAGE plpgsql AS $$ BEGIN
           UPDATE stock SET s_quantity = s_quantity + x.delta
           FROM UNNEST(ARRAY[NULL, 2, 999, 2, 3], ARRAY[100, 3, 7, 50, 4]) AS x(i, delta)
           WHERE stock.s_w_id = 1 AND stock.s_i_id = x.i;
         END $$",
    )
    .unwrap();
    // Exercise both cache construction and reuse. Each target is updated
    // once, using its first matching source row; missing and NULL keys do not
    // shift the source-to-target correspondence.
    sql.execute("CALL bump_stock_batch()").unwrap();
    sql.execute("CALL bump_stock_batch()").unwrap();
    let rows = sql.execute(
        "SELECT s_i_id, s_quantity, s_data FROM stock WHERE s_w_id = 1 AND s_i_id IN (2, 3) ORDER BY s_i_id"
    ).unwrap().rows;
    assert_eq!(
        cells(&rows),
        vec![
            vec!["2".to_string(), "18".to_string(), "data \"2\"".to_string()],
            vec!["3".to_string(), "21".to_string(), "data \"3\"".to_string()],
        ]
    );
}

#[test]
fn stored_update_projects_dependencies_without_changing_other_columns() {
    let (_dir, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    let before = sql
        .execute("SELECT * FROM stock WHERE s_w_id = 1 AND s_i_id = 2")
        .unwrap()
        .rows;
    sql.execute("BEGIN").unwrap();
    let rows = sql.execute(
        "UPDATE stock AS s SET s_quantity = CASE WHEN s_quantity < 20 THEN s_quantity + 3 ELSE 0 END \
         WHERE s.s_w_id = 1 AND s.s_i_id = 2 RETURNING s.s_quantity, s.s_data, s.s_price"
    ).unwrap().rows;
    assert_eq!(
        cells(&rows),
        vec![vec![
            "15".to_string(),
            "data \"2\"".to_string(),
            "2.50".to_string()
        ]]
    );
    // Wildcard and function projections must retain every required value.
    let rows = sql.execute(
        "UPDATE stock SET s_quantity = s_quantity + 1 WHERE s_w_id = 1 AND s_i_id = 2 RETURNING *"
    ).unwrap().rows;
    assert_eq!(rows[0].len(), before[0].len());
    for (idx, value) in rows[0].iter().enumerate() {
        if idx != 2 {
            assert_eq!(value, &before[0][idx], "column {idx} changed");
        }
    }
    let rows = sql.execute(
        "UPDATE stock SET s_quantity = s_quantity + 1 WHERE s_w_id = 1 AND s_i_id = 2 RETURNING upper(s_note)"
    ).unwrap().rows;
    assert_eq!(cells(&rows), vec![vec!["N2".to_string()]]);
    sql.execute("ROLLBACK").unwrap();
    assert_eq!(
        sql.execute("SELECT * FROM stock WHERE s_w_id = 1 AND s_i_id = 2")
            .unwrap()
            .rows,
        before
    );
}

#[test]
fn update_from_unnest_returning_projects_assigned_and_joined_columns() {
    let (_dir, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("BEGIN").unwrap();
    let rows = sql
        .execute(
            "WITH stock_update AS (
               UPDATE stock
               SET s_quantity = (CASE WHEN s_quantity < (item_stock.quantity + 10)
                                      THEN s_quantity + 91 ELSE s_quantity END) - item_stock.quantity,
                   s_price = item_stock.price
               FROM UNNEST(ARRAY[2, 5], ARRAY[1, 1], ARRAY[3, 20], ARRAY[9.9, 1.25])
                    AS item_stock (item_id, supply_wid, quantity, price)
               WHERE stock.s_i_id = item_stock.item_id AND stock.s_w_id = item_stock.supply_wid
               RETURNING stock.s_dist_01 AS s_dist, stock.s_quantity, stock.s_price,
                         (item_stock.quantity + item_stock.price) AS amount, stock.s_i_id
             )
             SELECT s_i_id, s_dist, s_quantity, s_price, amount FROM stock_update ORDER BY s_i_id",
        )
        .unwrap()
        .rows;
    // s_i_id 2: quantity 12 < 3 + 10 -> 12 + 91 - 3 = 100; price 9.9 -> NUMERIC(5,2) 9.90
    // s_i_id 5: quantity 15 < 20 + 10 -> 15 + 91 - 20 = 86; price 1.25
    let expected: Vec<Vec<String>> = [
        ["2", "dist2                   ", "100", "9.90", "12.9"],
        ["5", "dist5                   ", "86", "1.25", "21.25"],
    ]
    .iter()
    .map(|row| row.iter().map(|cell| cell.to_string()).collect())
    .collect();
    assert_eq!(cells(&rows), expected);
    let rows = sql
        .execute("SELECT s_quantity, s_price FROM stock WHERE s_w_id = 1 AND s_i_id = 2")
        .unwrap()
        .rows;
    assert_eq!(
        cells(&rows),
        vec![vec!["100".to_string(), "9.90".to_string()]]
    );
    sql.execute("ROLLBACK").unwrap();
}

#[test]
fn alternating_procedures_keep_their_own_operand_types() {
    // Regression: per-node type memos keyed by expression address were
    // poisoned when derived predicates (built per execution) of different
    // statements reused the same heap address — PAYMENT's by-last-name
    // lookup then cast a customer name to an integer. Alternate two
    // procedures with different derived-predicate shapes many times.
    let dir = TempDir::new().unwrap();
    let mut db =
        BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE TABLE warehouse (w_id INT PRIMARY KEY, w_tax NUMERIC(4,4), w_name VARCHAR(10))",
    )
    .unwrap();
    sql.execute(
        "CREATE TABLE customer (c_id INT NOT NULL, c_d_id INT NOT NULL, c_w_id INT NOT NULL, \
         c_first VARCHAR(16), c_last VARCHAR(16), c_credit CHAR(2), c_discount NUMERIC(4,4), \
         c_balance NUMERIC(12,2), PRIMARY KEY (c_w_id, c_d_id, c_id))",
    )
    .unwrap();
    sql.execute("CREATE INDEX customer_i2 ON customer (c_w_id, c_d_id, c_last, c_first)")
        .unwrap();
    sql.execute("INSERT INTO warehouse VALUES (1, 0.0750, 'main')")
        .unwrap();
    for i in 1..=30 {
        sql.execute(&format!(
            "INSERT INTO customer VALUES ({i}, 1, 1, 'F{i}', 'BAR{}', 'GC', 0.05, {i}.5)",
            i % 3
        ))
        .unwrap();
    }
    sql.execute(
        "CREATE OR REPLACE PROCEDURE by_name(w INT, d INT, last VARCHAR, INOUT cnt INT, \
         INOUT first VARCHAR) LANGUAGE plpgsql AS $$
         BEGIN
           SELECT count(c_id) INTO cnt FROM customer \
             WHERE c_last = last AND c_d_id = d AND c_w_id = w;
           SELECT c_first INTO first FROM customer \
             WHERE c_last = last AND c_d_id = d AND c_w_id = w ORDER BY c_first LIMIT 1;
         END $$",
    )
    .unwrap();
    sql.execute(
        "CREATE OR REPLACE PROCEDURE by_id(w INT, d INT, c INT, INOUT disc NUMERIC, \
         INOUT last VARCHAR, INOUT tax NUMERIC) LANGUAGE plpgsql AS $$
         BEGIN
           SELECT c_discount, c_last, w_tax INTO disc, last, tax FROM customer, warehouse \
             WHERE w_id = w AND c_w_id = w_id AND c_d_id = d AND c_id = c;
         END $$",
    )
    .unwrap();
    for i in 0..400 {
        let rows = sql
            .execute(&format!("CALL by_name(1, 1, 'BAR{}', NULL, NULL)", i % 3))
            .unwrap()
            .rows;
        assert_eq!(rows[0][0], SqlValue::Int(10), "iteration {i}");
        let rows = sql
            .execute(&format!(
                "CALL by_id(1, 1, {}, NULL, NULL, NULL)",
                1 + i % 30
            ))
            .unwrap()
            .rows;
        assert_eq!(
            rows[0][1],
            SqlValue::String(format!("BAR{}", (1 + i % 30) % 3)),
            "iteration {i}"
        );
        assert_eq!(rows[0][2], SqlValue::String("0.0750".to_string()));
    }
}

#[test]
fn range_join_with_record_level_constraint_counts_through_cells() {
    // STOCK_LEVEL shape: order_line is range-located (not a pk exact lookup)
    // and joined to stock with a residual quantity predicate; the generic
    // join evaluates the constraint on slot rows built from cells.
    let (_dir, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE TABLE order_line (ol_o_id INT NOT NULL, ol_d_id INT NOT NULL, ol_w_id INT NOT NULL, \
         ol_number INT NOT NULL, ol_i_id INT, ol_amount NUMERIC(6,2), \
         PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))",
    )
    .unwrap();
    for o in 1..=6 {
        for n in 1..=2 {
            sql.execute(&format!(
                "INSERT INTO order_line VALUES ({o}, 1, 1, {n}, {}, {o}.25)",
                (o + n) % 8 + 1
            ))
            .unwrap();
        }
    }
    let rows = sql
        .execute(
            "SELECT COUNT(DISTINCT s_i_id) FROM order_line, stock \
             WHERE ol_w_id = 1 AND ol_d_id = 1 AND ol_o_id < 5 AND ol_o_id >= 2 \
             AND s_w_id = 1 AND s_i_id = ol_i_id AND s_quantity < 16",
        )
        .unwrap()
        .rows;
    // orders 2..4 -> items {4,5,5,6,6,7}; quantities 10+i -> < 16 keeps items <= 5
    assert_eq!(rows[0][0], SqlValue::Int(2));
    let rows = sql
        .execute(
            "SELECT ol_o_id, ol_number, s_quantity FROM order_line JOIN stock \
             ON s_w_id = ol_w_id AND s_i_id = ol_i_id \
             WHERE ol_w_id = 1 AND ol_d_id = 1 AND ol_o_id >= 5 ORDER BY ol_o_id, ol_number",
        )
        .unwrap()
        .rows;
    let expected: Vec<Vec<String>> = [
        ["5", "1", "17"],
        ["5", "2", "18"],
        ["6", "1", "18"],
        ["6", "2", "11"],
    ]
    .iter()
    .map(|row| row.iter().map(|cell| cell.to_string()).collect())
    .collect();
    assert_eq!(cells(&rows), expected);
}

#[test]
fn routine_update_assignments_memoize_operand_types_per_ir_node() {
    // The UPDATE assignment operand-type memo is keyed by AST node address and
    // is only enabled for routine-IR-owned, subquery-free assignments. Mix:
    // two procedures whose assignment expressions have the same shape but
    // different operand types (NUMERIC vs TEXT vs INT), a dynamic EXECUTE
    // UPDATE (parsed per call, so its nodes are reused addresses), a cursor
    // (per-frame query clone) and an assignment with a scalar subquery.
    // Every path must keep exact semantics across many alternations.
    let dir = TempDir::new().unwrap();
    let mut db =
        BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE TABLE acct (id INT PRIMARY KEY, bal NUMERIC(12,2), tag TEXT, hits INT, \
         note TEXT)",
    )
    .unwrap();
    sql.execute("INSERT INTO acct VALUES (1, 10.00, 'a', 0, 'x'), (2, 20.00, 'b', 0, 'y')")
        .unwrap();
    sql.execute(
        "CREATE OR REPLACE PROCEDURE add_bal(i INT, amt NUMERIC) LANGUAGE plpgsql AS $$
         BEGIN UPDATE acct SET bal = bal + amt WHERE id = i; END $$",
    )
    .unwrap();
    sql.execute(
        "CREATE OR REPLACE PROCEDURE add_tag(i INT, t TEXT) LANGUAGE plpgsql AS $$
         BEGIN UPDATE acct SET tag = tag || t WHERE id = i; END $$",
    )
    .unwrap();
    sql.execute(
        "CREATE OR REPLACE PROCEDURE add_hit(i INT, n INT) LANGUAGE plpgsql AS $$
         BEGIN UPDATE acct SET hits = hits + n WHERE id = i; END $$",
    )
    .unwrap();
    sql.execute(
        "CREATE OR REPLACE PROCEDURE dyn_add(i INT, col TEXT, v TEXT) LANGUAGE plpgsql AS $$
         BEGIN EXECUTE 'UPDATE acct SET ' || col || ' = ' || col || ' + ' || v || \
             ' WHERE id = ' || i; END $$",
    )
    .unwrap();
    sql.execute(
        "CREATE OR REPLACE PROCEDURE sub_note(i INT) LANGUAGE plpgsql AS $$
         BEGIN UPDATE acct SET note = note || (SELECT tag FROM acct WHERE id = i) \
             WHERE id = i; END $$",
    )
    .unwrap();
    sql.execute(
        "CREATE OR REPLACE PROCEDURE cur_walk(INOUT total NUMERIC) LANGUAGE plpgsql AS $$
         DECLARE c CURSOR FOR SELECT bal FROM acct ORDER BY id; b NUMERIC; i INT;
         BEGIN total := 0; OPEN c;
             FOR i IN 1 .. 2 LOOP FETCH c INTO b; total := total + b; END LOOP;
             CLOSE c; END $$",
    )
    .unwrap();
    for round in 0..12 {
        sql.execute("CALL add_bal(1, 1.25)").unwrap();
        sql.execute("CALL add_tag(1, 'z')").unwrap();
        sql.execute("CALL add_hit(2, 3)").unwrap();
        sql.execute("CALL dyn_add(2, 'bal', '0.5')").unwrap();
        sql.execute("CALL dyn_add(2, 'hits', '1')").unwrap();
        sql.execute("CALL add_bal(2, -0.25)").unwrap();
        sql.execute("CALL sub_note(1)").unwrap();
        let rows = sql.execute("CALL cur_walk(NULL)").unwrap().rows;
        let expected_total = 10.0 + 1.25 * (round + 1) as f64 + 20.0 + 0.25 * (round + 1) as f64;
        let got: f64 = cells(&rows)[0][0].parse().unwrap();
        assert!(
            (got - expected_total).abs() < 1e-9,
            "round {round}: cursor total {got} != {expected_total}"
        );
    }
    let rows = sql
        .execute("SELECT id, bal, tag, hits, note FROM acct ORDER BY id")
        .unwrap()
        .rows;
    let text = cells(&rows);
    let mut tag = String::from("a");
    let mut note = String::from("x");
    for _ in 0..12 {
        tag.push('z');
        note.push_str(&tag);
    }
    assert_eq!(
        text[0],
        vec!["1", "25.00", tag.as_str(), "0", note.as_str()]
    );
    assert_eq!(text[1], vec!["2", "23.00", "b", "48", "y"]);
}

#[test]
fn return_only_functions_evaluate_without_a_frame_and_match_the_interpreter() {
    // A PL/pgSQL function whose body is exactly `RETURN <expr>` over its
    // parameters (HammerDB's DBMS_RANDOM shape, ~40 calls per NEW_ORDER)
    // evaluates the bound expression straight from the argument values. The
    // same body with a local variable takes the frame path; both must agree
    // on every value, NULL propagation and the return-type cast, from both the
    // hoisted-call path and the generic evaluator.
    let dir = TempDir::new().unwrap();
    let mut db =
        BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE FUNCTION fast_rand(INTEGER, INTEGER) RETURNS INTEGER AS $$
         DECLARE start_int ALIAS FOR $1; end_int ALIAS FOR $2;
         BEGIN RETURN trunc(0.5 * (end_int - start_int + 1) + start_int); END $$
         LANGUAGE 'plpgsql' STRICT",
    )
    .unwrap();
    sql.execute(
        "CREATE FUNCTION slow_rand(INTEGER, INTEGER) RETURNS INTEGER AS $$
         DECLARE start_int ALIAS FOR $1; end_int ALIAS FOR $2; v INT;
         BEGIN RETURN trunc(0.5 * (end_int - start_int + 1) + start_int); END $$
         LANGUAGE 'plpgsql' STRICT",
    )
    .unwrap();
    // Named parameters and a NUMERIC return cast.
    sql.execute(
        "CREATE FUNCTION fast_scale(x NUMERIC, factor INT) RETURNS NUMERIC(8,2) LANGUAGE plpgsql AS $$
         BEGIN RETURN x * factor / 3; END $$",
    )
    .unwrap();
    sql.execute(
        "CREATE FUNCTION slow_scale(x NUMERIC, factor INT) RETURNS NUMERIC(8,2) LANGUAGE plpgsql AS $$
         DECLARE v INT; BEGIN RETURN x * factor / 3; END $$",
    )
    .unwrap();
    sql.execute(
        "CREATE PROCEDURE p(lo INT, hi INT, INOUT a INT, INOUT b INT, INOUT c NUMERIC, INOUT d NUMERIC, INOUT e NUMERIC, INOUT f NUMERIC) \
         LANGUAGE plpgsql AS $$
         DECLARE i INT;
         BEGIN
           a := 0; b := 0;
           FOR i IN 1 .. 5 LOOP
             a := a + round(fast_rand(lo, hi));
             b := b + round(slow_rand(lo, hi));
           END LOOP;
           c := fast_scale(10.5, hi);
           d := slow_scale(10.5, hi);
           e := fast_scale(NULL, hi);
           f := slow_scale(NULL, hi);
         END $$",
    )
    .unwrap();
    // The body's own errors surface identically from both paths (today
    // `trunc(NULL)` is an error on the frame path as well).
    sql.execute(
        "CREATE PROCEDURE p_err_fast(INOUT x INT) LANGUAGE plpgsql AS $$ BEGIN x := fast_rand(NULL, 5); END $$",
    )
    .unwrap();
    sql.execute(
        "CREATE PROCEDURE p_err_slow(INOUT x INT) LANGUAGE plpgsql AS $$ BEGIN x := slow_rand(NULL, 5); END $$",
    )
    .unwrap();
    for _ in 0..2 {
        let fast = sql
            .execute("CALL p_err_fast(NULL)")
            .err()
            .map(|e| e.to_string());
        let slow = sql
            .execute("CALL p_err_slow(NULL)")
            .err()
            .map(|e| e.to_string());
        assert!(fast.is_some(), "frame path errors, inline path must too");
        assert_eq!(fast, slow);
    }
    #[cfg(debug_assertions)]
    let before = bicdb_sql::inline_return_fns::hits();
    for round in 0..3 {
        for (lo, hi) in [(1, 10), (5, 5), (-4, 7)] {
            let rows = sql
                .execute(&format!(
                    "CALL p({lo}, {hi}, NULL, NULL, NULL, NULL, NULL, NULL)"
                ))
                .unwrap()
                .rows;
            let row = &cells(&rows)[0];
            assert_eq!(row[0], row[1], "round {round} lo={lo} hi={hi}: {row:?}");
            assert_eq!(row[2], row[3], "round {round} lo={lo} hi={hi}: {row:?}");
            assert_eq!(row[4], row[5], "round {round} lo={lo} hi={hi}: {row:?}");
            assert_eq!(rows[0][4], SqlValue::Null, "null argument propagates");
        }
    }
    // SELECT-level calls run outside a routine scope and keep the frame path;
    // they must still agree.
    let rows = run(
        &mut db,
        "SELECT fast_rand(2, 9), slow_rand(2, 9), fast_scale(1.5, 4), slow_scale(1.5, 4)",
    );
    let row = &cells(&rows)[0];
    assert_eq!(row[0], "6");
    assert_eq!(row[0], row[1]);
    assert_eq!(row[2], row[3]);
    #[cfg(debug_assertions)]
    assert!(
        bicdb_sql::inline_return_fns::hits() >= before + 9 * 6,
        "return-only functions were not inlined: {} hits",
        bicdb_sql::inline_return_fns::hits() - before
    );
}

#[test]
fn hoisted_user_calls_keep_evaluation_order_short_circuit_and_builtin_precedence() {
    // Routine expressions that call user functions are bound with the calls
    // hoisted (evaluated before the bound tree, in PostgreSQL's order). The
    // first execution of each expression runs the generic evaluator (which
    // records the dispatch), later ones take the hoisted path: both must
    // produce the same values and the same side-effect sequence.
    let dir = TempDir::new().unwrap();
    let mut db =
        BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE log (seq SERIAL PRIMARY KEY, id INT, tag TEXT)")
        .unwrap();
    sql.execute(
        "CREATE FUNCTION f(i INT) RETURNS INT LANGUAGE plpgsql AS $$
         BEGIN INSERT INTO log (id, tag) VALUES (i, 'f'); RETURN i * 10; END $$",
    )
    .unwrap();
    sql.execute(
        "CREATE FUNCTION g(i INT, j INT) RETURNS INT STRICT LANGUAGE plpgsql AS $$
         BEGIN INSERT INTO log (id, tag) VALUES (i, 'g'); RETURN i + j; END $$",
    )
    .unwrap();
    // A user function shadowing a builtin the generic dispatch chain handles:
    // the builtin keeps winning, exactly as before.
    sql.execute(
        "CREATE FUNCTION upper(t TEXT) RETURNS TEXT LANGUAGE plpgsql AS $$
         BEGIN RETURN 'user'; END $$",
    )
    .unwrap();
    sql.execute(
        "CREATE PROCEDURE p(x INT, INOUT r INT, INOUT s INT, INOUT t INT, INOUT u TEXT) \
         LANGUAGE plpgsql AS $$
         DECLARE v INT;
         BEGIN
           r := round(f(1)) + f(2);
           v := g(f(3), 4);
           s := v;
           IF x > 1 AND f(5) > 0 THEN t := 1; ELSE t := 0; END IF;
           u := upper('a') || 'x';
         END $$",
    )
    .unwrap();
    for round in 0..4 {
        for x in [2, 0] {
            let rows = sql
                .execute(&format!("CALL p({x}, NULL, NULL, NULL, NULL)"))
                .unwrap()
                .rows;
            let expected_t = if x > 1 { "1" } else { "0" };
            assert_eq!(
                cells(&rows)[0],
                vec!["30", "34", expected_t, "Ax"],
                "round {round} x={x}"
            );
            let log = sql
                .execute("SELECT id, tag FROM log ORDER BY seq")
                .unwrap()
                .rows;
            let mut expected = vec![
                vec!["1".to_string(), "f".to_string()],
                vec!["2".to_string(), "f".to_string()],
                vec!["3".to_string(), "f".to_string()],
                vec!["30".to_string(), "g".to_string()],
            ];
            if x > 1 {
                expected.push(vec!["5".to_string(), "f".to_string()]);
            }
            assert_eq!(cells(&log), expected, "round {round} x={x}");
            sql.execute("DELETE FROM log").unwrap();
        }
    }
}

#[test]
fn bound_array_subscripts_match_postgres_semantics() {
    // `array_var[index]` binds for DECLAREd array variables (NEWORD's
    // amount_array[loop_counter]); the bound evaluator must give exactly what
    // the generic access-chain evaluator gives: 1-based, NULL out of range,
    // element type preserved.
    let dir = TempDir::new().unwrap();
    let mut db =
        BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE PROCEDURE p(i INT, INOUT a NUMERIC, INOUT b TEXT, INOUT c INT, INOUT d INT) \
         LANGUAGE plpgsql AS $$
         DECLARE
           amounts NUMERIC(5,2)[] := ARRAY[1.50, 2.25, 3.75];
           names VARCHAR(4)[] := ARRAY['ab', 'cd'];
           counts INT[];
           total NUMERIC := 0;
           n INT;
         BEGIN
           counts := ARRAY[10, 20, 30];
           a := amounts[i];
           b := names[i];
           c := counts[i] + counts[1];
           n := i + 1;
           d := counts[n];
           FOR k IN 1 .. 3 LOOP total := total + CAST(amounts[k] AS NUMERIC); END LOOP;
           IF total <> 7.50 THEN RAISE EXCEPTION 'bad total %', total; END IF;
         END $$",
    )
    .unwrap();
    for _ in 0..3 {
        let rows = sql
            .execute("CALL p(2, NULL, NULL, NULL, NULL)")
            .unwrap()
            .rows;
        assert_eq!(cells(&rows)[0], vec!["2.25", "cd", "30", "30"]);
        let rows = sql
            .execute("CALL p(3, NULL, NULL, NULL, NULL)")
            .unwrap()
            .rows;
        assert_eq!(cells(&rows)[0], vec!["3.75", "NULL", "40", "NULL"]);
        let rows = sql
            .execute("CALL p(0, NULL, NULL, NULL, NULL)")
            .unwrap()
            .rows;
        assert_eq!(cells(&rows)[0], vec!["NULL", "NULL", "NULL", "10"]);
    }
}

#[test]
fn routine_update_case_and_substring_keep_their_types_across_procedures() {
    // CASE branch-type validation and substring's bytea detection are
    // memoized per IR node for routine-owned UPDATE assignments; procedures
    // with identical shapes but different types must stay independent, and a
    // CASE with incompatible branches must still be rejected every time.
    let dir = TempDir::new().unwrap();
    let mut db =
        BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE TABLE cust (id INT PRIMARY KEY, credit CHAR(2), bal NUMERIC(12,2), \
         data VARCHAR(50), raw BYTEA)",
    )
    .unwrap();
    sql.execute("INSERT INTO cust VALUES (1, 'BC', 10.00, 'hello world', '\\x0102030405')")
        .unwrap();
    sql.execute(
        "CREATE PROCEDURE pay_num(i INT, amt NUMERIC) LANGUAGE plpgsql AS $$
         BEGIN UPDATE cust SET bal = CASE WHEN credit = 'BC' THEN bal - amt ELSE bal END \
         WHERE id = i; END $$",
    )
    .unwrap();
    sql.execute(
        "CREATE PROCEDURE pay_text(i INT, amt NUMERIC) LANGUAGE plpgsql AS $$
         BEGIN UPDATE cust SET data = CASE WHEN credit = 'BC' THEN substring(data || '|' || \
         amt::text FROM 1 FOR 20) ELSE data END WHERE id = i; END $$",
    )
    .unwrap();
    sql.execute(
        "CREATE PROCEDURE cut_raw(i INT) LANGUAGE plpgsql AS $$
         BEGIN UPDATE cust SET raw = substring(raw FROM 2 FOR 2) WHERE id = i; END $$",
    )
    .unwrap();
    sql.execute(
        "CREATE PROCEDURE bad_case(i INT) LANGUAGE plpgsql AS $$
         BEGIN UPDATE cust SET data = CASE WHEN credit = 'BC' THEN bal ELSE raw END \
         WHERE id = i; END $$",
    )
    .unwrap();
    for round in 0..4 {
        sql.execute("CALL pay_num(1, 1.50)").unwrap();
        sql.execute("CALL pay_text(1, 1.50)").unwrap();
        let bad = sql.execute("CALL bad_case(1)");
        assert!(
            bad.is_err(),
            "round {round}: incompatible CASE branches must error"
        );
    }
    sql.execute("CALL cut_raw(1)").unwrap();
    sql.execute("CALL cut_raw(1)").unwrap();
    let rows = sql
        .execute("SELECT bal, data, raw FROM cust WHERE id = 1")
        .unwrap()
        .rows;
    assert_eq!(
        cells(&rows)[0],
        vec!["4.00", "hello world|1.50|1.5", "\\x03"]
    );
}

#[test]
fn primary_key_joins_preserve_shared_right_rows_and_outer_residuals() {
    let dir = TempDir::new().unwrap();
    let mut db =
        BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE join_payload (id INT PRIMARY KEY, payload TEXT, amount INT)")
        .unwrap();
    sql.execute(
        "INSERT INTO join_payload VALUES (1, 'first payload', 10), (2, 'second payload', 20)",
    )
    .unwrap();
    let rows = sql.execute(
        "SELECT x.n, a.payload, b.payload FROM unnest(ARRAY[2,2,1,999,NULL]) WITH ORDINALITY AS x(id,n) \
         LEFT JOIN join_payload a ON a.id = x.id AND a.amount >= 20 \
         LEFT JOIN join_payload b ON b.id = x.id ORDER BY x.n",
    ).unwrap().rows;
    assert_eq!(
        cells(&rows),
        vec![
            vec!["1", "second payload", "second payload"],
            vec!["2", "second payload", "second payload"],
            vec!["3", "NULL", "first payload"],
            vec!["4", "NULL", "NULL"],
            vec!["5", "NULL", "NULL"],
        ]
    );
}
