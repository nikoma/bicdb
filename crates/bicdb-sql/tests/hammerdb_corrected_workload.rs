use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};
use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

const SCHEMA: &str = include_str!("../../../bench/tpcc/fixtures/neworder-v2/schema.sql");
const WORKLOAD: &str = include_str!("../../../bench/tpcc/corrected_workload_v2.sql");
const CALL: &str = "CALL neword(1,1,1,1,3,0,'','',0,0,0,CAST('2026-09-05 00:00:00' AS timestamp))";

fn script(session: &mut SqlSession<'_>, sql: &str) {
    for statement in Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap() {
        let sql = statement.to_string();
        session
            .execute(&sql)
            .unwrap_or_else(|error| panic!("{error}: {sql}"));
    }
}

fn workload(session: &mut SqlSession<'_>) {
    // The external AST parser does not accept OR REPLACE PROCEDURE, while
    // BicDB's raw statement entrypoint does. Send each complete definition
    // through the same entrypoint used to install the workload over pgwire.
    let mut offsets: Vec<_> = WORKLOAD
        .match_indices("CREATE OR REPLACE PROCEDURE")
        .map(|(offset, _)| offset)
        .collect();
    assert_eq!(offsets.len(), 3, "install Payment, Delivery, and NewOrder");
    offsets.push(WORKLOAD.len());
    for pair in offsets.windows(2) {
        let sql = WORKLOAD[pair[0]..pair[1]].trim();
        session
            .execute(sql)
            .unwrap_or_else(|error| panic!("{error}: {sql}"));
    }
}

fn duplicate_input(session: &mut SqlSession<'_>) {
    session.execute("CREATE OR REPLACE FUNCTION dbms_random(integer,integer) RETURNS double precision LANGUAGE sql AS $$SELECT 2.0$$").unwrap();
}

#[test]
fn invalid_item_rolls_back_and_duplicate_lines_keep_amounts_and_stock_counters() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    script(&mut session, SCHEMA);
    workload(&mut session);
    let invalid = session.execute(CALL).unwrap();
    assert_eq!(invalid.rows.len(), 1);
    assert_eq!(invalid.rows[0].last(), Some(&SqlValue::Int(-1)));
    assert_eq!(
        session.execute("SELECT count(*) FROM orders").unwrap().rows,
        vec![vec![SqlValue::Int(0)]]
    );
    assert_eq!(
        session
            .execute("SELECT d_next_o_id FROM district")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(3001)]]
    );
    duplicate_input(&mut session);
    session.execute(CALL).unwrap();
    assert_eq!(
        session
            .execute("SELECT s_quantity,s_ytd,s_order_cnt,s_remote_cnt FROM stock WHERE s_i_id=2")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(44),
            SqlValue::Int(6),
            SqlValue::Int(3),
            SqlValue::Int(0)
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT ol_number,ol_i_id,ol_quantity,ol_amount FROM order_line ORDER BY ol_number"
            )
            .unwrap()
            .rows,
        (1..=3)
            .map(|n| vec![
                SqlValue::Int(n),
                SqlValue::Int(2),
                SqlValue::Int(2),
                SqlValue::String("87.56".into())
            ])
            .collect::<Vec<_>>()
    );
    assert_eq!(
        session
            .execute("SELECT o_all_local FROM orders")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );
}

#[test]
fn missing_stock_aborts_the_order_and_district_increment() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    script(&mut session, SCHEMA);
    workload(&mut session);
    duplicate_input(&mut session);
    session.execute("DELETE FROM stock WHERE s_i_id=2").unwrap();
    let error = session.execute(CALL).unwrap_err();
    assert_eq!(error.sqlstate(), "P0001");
    assert_eq!(
        session
            .execute("SELECT d_next_o_id FROM district")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(3001)]]
    );
    for table in ["orders", "new_order", "order_line"] {
        assert_eq!(
            session
                .execute(&format!("SELECT count(*) FROM {table}"))
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(0)]],
            "{table}"
        );
    }
}

#[test]
fn delivery_updates_balance_and_counter_for_one_delivered_order() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    script(&mut session, SCHEMA);
    workload(&mut session);
    duplicate_input(&mut session);
    session.execute(CALL).unwrap();
    session
        .execute("CALL delivery(1,5,CAST('2026-09-05 00:00:00' AS timestamp))")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT c_balance,c_delivery_cnt FROM customer")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("262.68".into()), SqlValue::Int(1)]]
    );
    assert_eq!(
        session
            .execute("SELECT count(*) FROM new_order")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(0)]]
    );
    assert_eq!(
        session
            .execute("SELECT count(ol_delivery_d) FROM order_line")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(3)]]
    );
    assert_eq!(
        session
            .execute("SELECT o_carrier_id FROM orders")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(5)]]
    );
}
