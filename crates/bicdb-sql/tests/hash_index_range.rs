//! Range predicates on network and other typed columns use the type's SQL
//! ordering: an untyped string-literal BETWEEN bound takes the column's type
//! (it compared as text), and a hash index never answers a range probe.

use bicdb_core::BicDb;
use bicdb_sql::SqlSession;

fn ids(session: &mut SqlSession<'_>, sql: &str) -> Vec<String> {
    let mut ids = session
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .into_iter()
        .map(|row| row[0].to_cell())
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

#[test]
fn range_predicates_on_hash_indexed_columns_match_unindexed_results() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let rows = "(1, '9.0.0.1', 'b', 9, '08:00:2b:01:02:03'),
                (2, '10.0.0.2', 'a', 10, '08:00:2b:01:02:04'),
                (3, '192.168.1.0/24', 'C', 100, '0a:00:2b:01:02:03'),
                (4, '2.2.2.2', 'aa', -5, '08:00:2b:01:02:05'),
                (5, NULL, NULL, NULL, NULL)";
    session
        .execute(&format!(
            "CREATE TABLE hashed (id int4 PRIMARY KEY, address inet, label text, n int4, mac macaddr);
             CREATE TABLE plain (id int4 PRIMARY KEY, address inet, label text, n int4, mac macaddr);
             INSERT INTO hashed VALUES {rows};
             INSERT INTO plain VALUES {rows};
             CREATE INDEX hashed_address ON hashed USING hash (address);
             CREATE INDEX hashed_label ON hashed USING hash (label);
             CREATE INDEX hashed_n ON hashed USING hash (n);
             CREATE INDEX hashed_mac ON hashed USING hash (mac);"
        ))
        .unwrap();
    for predicate in [
        "address > '9.0.0.1'",
        "address < '10.0.0.0'",
        "address >= '2.2.2.2' AND address <= '9.9.9.9'",
        "address BETWEEN '1.0.0.0' AND '9.255.255.255'",
        "label > 'B'",
        "label < 'aa'",
        "n > -6",
        "n < 10",
        "n BETWEEN 0 AND 50",
        "mac > '08:00:2b:01:02:03'",
        "address = '10.0.0.2'",
        "n = 10",
    ] {
        let expected = ids(
            &mut session,
            &format!("SELECT id FROM plain WHERE {predicate}"),
        );
        let actual = ids(
            &mut session,
            &format!("SELECT id FROM hashed WHERE {predicate}"),
        );
        assert_eq!(actual, expected, "{predicate}");
    }
    // Absolute answers in PostgreSQL's inet ordering (not text ordering).
    for table in ["plain", "hashed"] {
        for (predicate, expected) in [
            (
                "address BETWEEN '1.0.0.0' AND '9.255.255.255'",
                &["1", "4"][..],
            ),
            (
                "address NOT BETWEEN '1.0.0.0' AND '9.255.255.255'",
                &["2", "3"][..],
            ),
            ("address > '9.0.0.1'", &["2", "3"][..]),
            (
                "mac BETWEEN '08:00:2b:01:02:04' AND '08:00:2b:01:02:05'",
                &["2", "4"][..],
            ),
        ] {
            let sql = format!("SELECT id FROM {table} WHERE {predicate}");
            assert_eq!(ids(&mut session, &sql), expected, "{sql}");
        }
    }
}
