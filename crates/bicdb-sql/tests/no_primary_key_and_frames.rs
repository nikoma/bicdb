//! Tables without a declared primary key have no unique column (not even one
//! named `id`); `REFERENCES t` names t's primary key; window frame EXCLUDE;
//! ROWS FROM. Expected results were checked against PostgreSQL 18.4.
use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::SqlSession;

fn rows(session: &mut SqlSession<'_>, sql: &str) -> Vec<Vec<String>> {
    session
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .iter()
        .map(|row| row.iter().map(|value| value.to_cell()).collect())
        .collect()
}

fn grid(expected: &[&[&str]]) -> Vec<Vec<String>> {
    expected
        .iter()
        .map(|row| row.iter().map(|cell| cell.to_string()).collect())
        .collect()
}

fn with_session(test: impl FnOnce(&mut SqlSession<'_>)) {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut session = SqlSession::new(&mut db);
    test(&mut session);
}

#[test]
fn a_column_named_id_is_not_an_implicit_primary_key() {
    with_session(|session| {
        for sql in [
            "CREATE TABLE np(id INT, n INT)",
            "INSERT INTO np VALUES (1, 1)",
            "INSERT INTO np VALUES (1, 2)",
            "INSERT INTO np VALUES (NULL, 3)",
            "UPDATE np SET id = 7 WHERE n = 3",
            "DELETE FROM np WHERE n = 1",
        ] {
            session
                .execute(sql)
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        assert_eq!(
            rows(session, "SELECT id, n FROM np ORDER BY n"),
            grid(&[&["1", "2"], &["7", "3"]])
        );
        assert_eq!(
            rows(
                session,
                "SELECT count(*) FROM pg_constraint WHERE conrelid = 'np'::regclass"
            ),
            grid(&[&["0"]])
        );
        session.execute("INSERT INTO np DEFAULT VALUES").unwrap();
        assert_eq!(
            rows(
                session,
                "SELECT count(*) FROM np WHERE id IS NULL AND n IS NULL"
            ),
            grid(&[&["1"]])
        );
        let error = session
            .execute("INSERT INTO np VALUES (1, 9) ON CONFLICT (id) DO NOTHING")
            .unwrap_err();
        assert_eq!(error.sqlstate(), "42P10");
    });
}

#[test]
fn a_declared_id_primary_key_is_unchanged() {
    with_session(|session| {
        session
            .execute("CREATE TABLE dp(id INT PRIMARY KEY, n INT)")
            .unwrap();
        session.execute("INSERT INTO dp VALUES (1, 1)").unwrap();
        assert_eq!(
            session
                .execute("INSERT INTO dp VALUES (1, 2)")
                .unwrap_err()
                .sqlstate(),
            "23505"
        );
        session
            .execute("INSERT INTO dp VALUES (1, 3) ON CONFLICT (id) DO UPDATE SET n = excluded.n")
            .unwrap();
        assert_eq!(rows(session, "SELECT * FROM dp"), grid(&[&["1", "3"]]));
    });
}

#[test]
fn references_without_columns_names_the_primary_key() {
    with_session(|session| {
        for sql in [
            "CREATE TABLE parent_pk(code TEXT PRIMARY KEY)",
            "CREATE TABLE child_pk(p TEXT REFERENCES parent_pk)",
            "CREATE TABLE parent_none(id INT)",
            "INSERT INTO parent_pk VALUES ('a')",
            "INSERT INTO child_pk VALUES ('a')",
        ] {
            session
                .execute(sql)
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        assert_eq!(
            session
                .execute("INSERT INTO child_pk VALUES ('b')")
                .unwrap_err()
                .sqlstate(),
            "23503"
        );
        assert_eq!(
            session
                .execute("CREATE TABLE child_none(p INT REFERENCES parent_none)")
                .unwrap_err()
                .sqlstate(),
            "42704"
        );
        session
            .execute("CREATE TABLE tree(id INT PRIMARY KEY, up INT REFERENCES tree)")
            .unwrap();
        session
            .execute("INSERT INTO tree VALUES (1, NULL)")
            .unwrap();
        session.execute("INSERT INTO tree VALUES (2, 1)").unwrap();
        assert_eq!(
            session
                .execute("INSERT INTO tree VALUES (3, 9)")
                .unwrap_err()
                .sqlstate(),
            "23503"
        );
    });
}

#[test]
fn window_frame_exclusion() {
    with_session(|session| {
        for sql in [
            "CREATE TABLE wx(id INT, g INT, v INT)",
            "INSERT INTO wx VALUES (1,1,10),(2,1,20),(3,2,30),(4,2,40),(5,3,50)",
        ] {
            session.execute(sql).unwrap();
        }
        assert_eq!(
            rows(
                session,
                "SELECT id, \
                 sum(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW), \
                 sum(v) OVER (ORDER BY g ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP), \
                 sum(v) OVER (ORDER BY g ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES), \
                 count(*) OVER (ORDER BY g RANGE BETWEEN CURRENT ROW AND CURRENT ROW EXCLUDE CURRENT ROW), \
                 first_value(v) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING EXCLUDE CURRENT ROW), \
                 last_value(v) OVER w \
                 FROM wx WINDOW w AS (ORDER BY g GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE NO OTHERS) \
                 ORDER BY id"
            ),
            grid(&[
                &["1", "20", "120", "130", "1", "20", "40"],
                &["2", "40", "120", "140", "1", "30", "40"],
                &["3", "60", "80", "110", "1", "40", "50"],
                &["4", "80", "80", "120", "1", "50", "50"],
                &["5", "40", "100", "150", "0", "", "50"],
            ])
        );
        assert!(session
            .execute("SELECT avg(v) OVER (PARTITION BY g EXCLUDE TIES) FROM wx")
            .is_err());
    });
}

#[test]
fn rows_from_zips_set_returning_functions() {
    with_session(|session| {
        assert_eq!(
            rows(
                session,
                "SELECT * FROM ROWS FROM (generate_series(1,3), unnest(ARRAY['a','b'])) \
                 WITH ORDINALITY AS t(n, s, o) ORDER BY o"
            ),
            grid(&[&["1", "a", "1"], &["2", "b", "2"], &["3", "", "3"]])
        );
        assert_eq!(
            rows(
                session,
                "SELECT * FROM ROWS FROM (generate_series(1,2)) AS t"
            ),
            grid(&[&["1"], &["2"]])
        );
    });
}
