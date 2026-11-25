//! pgcrypto password hashing and digests through SQL.
use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn crypt_and_gen_salt_round_trip_and_match_reference_hashes() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE EXTENSION IF NOT EXISTS pgcrypto; CREATE TABLE accounts(name TEXT, hash TEXT)",
    )
    .unwrap();
    sql.execute("INSERT INTO accounts VALUES ('bf', crypt('s3cret', gen_salt('bf', 4))), ('md5', crypt('s3cret', gen_salt('md5'))), ('xdes', crypt('s3cret', gen_salt('xdes'))), ('des', crypt('s3cret', gen_salt('des'))), ('sha', crypt('s3cret', gen_salt('sha512crypt')))")
        .unwrap();
    let rows = sql
        .execute("SELECT name, hash = crypt('s3cret', hash), hash = crypt('wrong', hash) FROM accounts ORDER BY name")
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 5);
    for row in rows {
        assert_eq!(row[1], SqlValue::Bool(true), "{row:?}");
        assert_eq!(row[2], SqlValue::Bool(false), "{row:?}");
    }
    // A hash produced by PostgreSQL verifies here.
    assert_eq!(
        sql.execute("SELECT crypt('U*U', '$2a$05$CCCCCCCCCCCCCCCCCCCCC.E5YPO9kmyuRGyh0XouQYb4YMJKvyOeW') = '$2a$05$CCCCCCCCCCCCCCCCCCCCC.E5YPO9kmyuRGyh0XouQYb4YMJKvyOeW'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]]
    );
    assert_eq!(
        sql.execute("SELECT gen_salt('bf', 3)")
            .unwrap_err()
            .sqlstate(),
        "22023"
    );
    assert_eq!(
        sql.execute("SELECT encode(digest('abc', 'sha1'), 'hex'), encode(hmac('data', 'key', 'md5'), 'hex')")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("a9993e364706816aba3e25717850c26c9cd0d89d".into()),
            SqlValue::String("9d5c73ef85594d34ec4438b7c97e51d8".into())
        ]]
    );
}
