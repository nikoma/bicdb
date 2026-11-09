//! Parser values are decoded. Rendering must preserve every quoted identifier
//! position, including DDL names that the expression visitor never sees.
use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::{
    sql_text::{sql_key, sql_text},
    SqlSession, SqlValue,
};
use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
#[test]
fn quoted_identifier_asts_round_trip_in_ddl_aliases_functions_and_types() {
    for name in [
        "ordinary",
        "one\"quote",
        "two\"\"quotes",
        "slash\\\"quote",
        "α\"\"β",
    ] {
        let n = quote(name);
        for sql in [
            format!("CREATE TABLE {n} ({n} NUMERIC)"),
            format!("SELECT {n}.{n} AS {n} FROM {n} AS {n}"),
            format!("CREATE VIEW {n} ({n}) AS SELECT 'a''''b'"),
            format!("SELECT {n}('a''''b')::{n}"),
        ] {
            let before = Parser::parse_sql(&PostgreSqlDialect {}, &sql)
                .unwrap()
                .remove(0);
            let rendered = sql_text(&before);
            let after = Parser::parse_sql(&PostgreSqlDialect {}, &rendered)
                .unwrap()
                .remove(0);
            assert_eq!(before, after, "{sql} rendered as {rendered}");
            assert_eq!(sql_key(&before), sql_key(&after));
        }
    }
}
#[test]
fn stored_views_preserve_adjacent_quotes_and_backslash_quote_column_names() {
    let root = tempfile::tempdir().unwrap();
    {
        let mut db =
            BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
        let mut s = SqlSession::new(&mut db);
        for (i, name) in ["two\"\"quotes", "slash\\\"quote"].iter().enumerate() {
            let n = quote(name);
            s.execute(&format!("CREATE TABLE base_{i} ({n} NUMERIC)"))
                .unwrap();
            s.execute(&format!("INSERT INTO base_{i} VALUES (2.00)"))
                .unwrap();
            s.execute(&format!("CREATE VIEW view_{i} AS SELECT {n} FROM base_{i}"))
                .unwrap();
        }
    }
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut s = SqlSession::new(&mut db);
    for (i, name) in ["two\"\"quotes", "slash\\\"quote"].iter().enumerate() {
        let n = quote(name);
        let result = s
            .execute(&format!("SELECT {n}=2.000 FROM view_{i}"))
            .unwrap();
        assert_eq!(result.rows, vec![vec![SqlValue::Bool(true)]]);
    }
}
