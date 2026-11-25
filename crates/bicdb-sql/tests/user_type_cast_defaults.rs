//! Column defaults cast to enum, domain and composite types, the form a
//! schema dump writes (`DEFAULT 'new'::public.status`).
use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn text(value: &str) -> SqlValue {
    SqlValue::String(value.into())
}

#[test]
fn enum_cast_defaults_fill_inserts_and_survive_reopen() {
    let root = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(root.path()).unwrap();
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TYPE public.status AS ENUM ('new','paid'); CREATE TABLE public.orders(n INT, s public.status DEFAULT 'new'::public.status NOT NULL, t status DEFAULT 'paid'::status)").unwrap();
        sql.execute("INSERT INTO orders(n) VALUES (1)").unwrap();
        sql.execute("INSERT INTO orders DEFAULT VALUES").unwrap();
        assert_eq!(
            sql.execute("SELECT n, s::text, t::text FROM orders ORDER BY n NULLS LAST")
                .unwrap()
                .rows,
            vec![
                vec![SqlValue::Int(1), text("new"), text("paid")],
                vec![SqlValue::Null, text("new"), text("paid")],
            ]
        );
    }
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("INSERT INTO orders(n) VALUES (3)").unwrap();
    assert_eq!(
        sql.execute("SELECT s = 'new'::status FROM orders WHERE n = 3")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]]
    );
    let default = sql
        .execute("SELECT column_default FROM information_schema.columns WHERE table_name = 'orders' AND column_name = 't'")
        .unwrap()
        .rows;
    assert_eq!(default, vec![vec![text("'paid'::status")]]);
}

#[test]
fn domain_and_composite_cast_defaults_apply_their_rules_on_insert() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE DOMAIN pos AS INT CHECK (VALUE > 0); CREATE TYPE pair AS (a INT, b TEXT); CREATE TABLE dt(id INT, p pos DEFAULT 7::pos, q pair DEFAULT '(1,x)'::pair, bad pos DEFAULT (-1)::pos)").unwrap();
    // The domain check runs when the default is used, as in PostgreSQL.
    assert_eq!(
        sql.execute("INSERT INTO dt(id) VALUES (1)")
            .unwrap_err()
            .sqlstate(),
        "23514"
    );
    sql.execute("INSERT INTO dt(id, bad) VALUES (1, 2)")
        .unwrap();
    assert_eq!(
        sql.execute("SELECT p, (q).a, (q).b FROM dt").unwrap().rows,
        vec![vec![SqlValue::Int(7), SqlValue::Int(1), text("x")]]
    );
}

#[test]
fn invalid_enum_label_in_a_cast_default_is_rejected() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TYPE st AS ENUM ('a')").unwrap();
    let error = sql
        .execute("CREATE TABLE bad(s st DEFAULT 'nope'::st)")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "22P02", "{error}");
}
