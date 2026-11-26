//! PostgreSQL 18 temporal keys: PRIMARY KEY / UNIQUE (..., period WITHOUT OVERLAPS).
use bicdb_core::BicDb;
use bicdb_sql::SqlSession;

fn run(db: &mut BicDb, sql: &str) {
    SqlSession::new(db)
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
}

fn cells(db: &mut BicDb, sql: &str) -> Vec<Vec<String>> {
    SqlSession::new(db)
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .iter()
        .map(|row| row.iter().map(|value| value.to_cell()).collect())
        .collect()
}

fn fails(db: &mut BicDb, sql: &str) -> (String, String) {
    let error = SqlSession::new(db).execute(sql).unwrap_err();
    (error.sqlstate().to_string(), error.to_string())
}

#[test]
fn temporal_primary_key_rejects_overlaps_nulls_and_empty_periods() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    run(
        &mut db,
        "CREATE TABLE temporal_rng (id int4range, valid_at daterange, \
         CONSTRAINT temporal_rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS))",
    );
    run(
        &mut db,
        "INSERT INTO temporal_rng VALUES ('[1,2)', '[2018-01-02,2018-02-03)'), \
         ('[1,2)', '[2018-03-03,2018-04-04)'), ('[2,3)', '[2018-01-01,2018-01-05)')",
    );
    assert_eq!(
        fails(
            &mut db,
            "INSERT INTO temporal_rng VALUES ('[1,2)', '[2018-01-01,2018-01-05)')"
        ),
        (
            "23P01".to_string(),
            "conflicting key value violates exclusion constraint \"temporal_rng_pk\"".to_string()
        )
    );
    assert_eq!(
        fails(
            &mut db,
            "INSERT INTO temporal_rng VALUES (NULL, '[2018-01-01,2018-01-05)')"
        )
        .0,
        "23502"
    );
    assert_eq!(
        fails(&mut db, "INSERT INTO temporal_rng VALUES ('[3,4)', 'empty')"),
        (
            "23514".to_string(),
            "empty WITHOUT OVERLAPS value found in column \"valid_at\" in relation \"temporal_rng\""
                .to_string()
        )
    );
    // An UPDATE into an overlap fails too; nothing changed.
    assert_eq!(
        fails(
            &mut db,
            "UPDATE temporal_rng SET valid_at = '[2018-01-01,2018-06-01)' \
             WHERE valid_at = '[2018-03-03,2018-04-04)'"
        )
        .0,
        "23P01"
    );
    assert_eq!(cells(&mut db, "SELECT count(*) FROM temporal_rng"), [["3"]]);
    assert_eq!(
        cells(
            &mut db,
            "SELECT contype, pg_get_constraintdef(oid) FROM pg_constraint \
             WHERE conname = 'temporal_rng_pk'"
        ),
        [["p", "PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)"]]
    );
}

#[test]
fn temporal_unique_allows_nulls_and_multiranges() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    run(
        &mut db,
        "CREATE TABLE temporal_mltrng3 (id int4range, valid_at datemultirange, \
         CONSTRAINT temporal_mltrng3_uq UNIQUE (id, valid_at WITHOUT OVERLAPS))",
    );
    run(
        &mut db,
        "INSERT INTO temporal_mltrng3 VALUES ('[1,2)', '{[2018-01-02,2018-02-03)}'), \
         (NULL, '{[2018-01-01,2018-01-05)}'), (NULL, '{[2018-01-01,2018-01-05)}')",
    );
    assert_eq!(
        fails(
            &mut db,
            "INSERT INTO temporal_mltrng3 VALUES ('[1,2)', '{[2018-01-01,2018-01-05)}')"
        )
        .0,
        "23P01"
    );
    assert_eq!(
        cells(
            &mut db,
            "SELECT contype FROM pg_constraint WHERE conname = 'temporal_mltrng3_uq'"
        ),
        [["u"]]
    );
}

#[test]
fn temporal_key_definitions_are_validated_like_postgresql() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    assert_eq!(
        fails(
            &mut db,
            "CREATE TABLE t1 (valid_at daterange, CONSTRAINT t1_pk PRIMARY KEY (valid_at WITHOUT OVERLAPS))"
        )
        .1,
        "constraint using WITHOUT OVERLAPS needs at least two columns"
    );
    assert_eq!(
        fails(
            &mut db,
            "CREATE TABLE t2 (id int, CONSTRAINT t2_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS))"
        )
        .1,
        "column \"valid_at\" named in key does not exist"
    );
    assert_eq!(
        fails(
            &mut db,
            "CREATE TABLE t3 (id int4range, valid_at text, CONSTRAINT t3_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS))"
        )
        .1,
        "column \"valid_at\" in WITHOUT OVERLAPS is not a range or multirange type"
    );
    // A failed definition leaves no table behind.
    for table in ["t1", "t2", "t3"] {
        assert_eq!(
            fails(&mut db, &format!("SELECT * FROM {table}")).0,
            "42P01",
            "{table}"
        );
    }
    // ALTER TABLE ADD over overlapping rows fails.
    run(
        &mut db,
        "CREATE TABLE t4 (id int4range, valid_at daterange)",
    );
    run(
        &mut db,
        "INSERT INTO t4 VALUES ('[1,2)', '[2018-01-01,2018-03-01)'), ('[1,2)', '[2018-02-01,2018-04-01)')",
    );
    assert_eq!(
        fails(
            &mut db,
            "ALTER TABLE t4 ADD CONSTRAINT t4_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)"
        ),
        (
            "23P01".to_string(),
            "could not create exclusion constraint \"t4_pk\"".to_string()
        )
    );
    run(
        &mut db,
        "DELETE FROM t4 WHERE valid_at = '[2018-02-01,2018-04-01)'",
    );
    run(
        &mut db,
        "ALTER TABLE t4 ADD CONSTRAINT t4_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)",
    );
    assert_eq!(
        fails(
            &mut db,
            "INSERT INTO t4 VALUES ('[1,2)', '[2018-02-15,2018-02-20)')"
        )
        .0,
        "23P01"
    );
}

#[test]
fn temporal_foreign_keys_parse_and_a_period_column_is_not_one() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    run(
        &mut db,
        "CREATE TABLE p (id int4range, valid_at daterange, \
         CONSTRAINT p_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS))",
    );
    // Supported now (see temporal_foreign_keys.rs).
    run(
        &mut db,
        "CREATE TABLE c (id int4range, parent_id int4range, valid_at daterange, \
         CONSTRAINT c_fk FOREIGN KEY (parent_id, PERIOD valid_at) REFERENCES p (id, PERIOD valid_at))",
    );
    // A column named "period" in an ordinary foreign key is fine.
    run(
        &mut db,
        "CREATE TABLE q (a int, period int, PRIMARY KEY (a, period))",
    );
    run(
        &mut db,
        "CREATE TABLE r (a int, period int, FOREIGN KEY (a, period) REFERENCES q (a, period))",
    );
}

#[test]
fn temporal_keys_hold_inside_the_partitions_of_a_partitioned_table() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    run(
        &mut db,
        "CREATE TABLE tp (id int4range, valid_at daterange, \
         CONSTRAINT tp_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)) PARTITION BY LIST (id)",
    );
    run(
        &mut db,
        "CREATE TABLE tp1 PARTITION OF tp FOR VALUES IN ('[1,2)')",
    );
    run(
        &mut db,
        "INSERT INTO tp VALUES ('[1,2)', '[2018-01-01,2018-02-01)')",
    );
    assert_eq!(
        fails(
            &mut db,
            "INSERT INTO tp VALUES ('[1,2)', '[2018-01-15,2018-03-01)')"
        )
        .0,
        "23P01"
    );
    assert_eq!(
        fails(
            &mut db,
            "INSERT INTO tp1 VALUES ('[1,2)', '[2018-01-20,2018-03-01)')"
        )
        .0,
        "23P01"
    );
}
