//! PostgreSQL's DETAIL lines of exclusion, temporal key and temporal foreign
//! key violations.
use bicdb_core::BicDb;
use bicdb_sql::{SqlError, SqlErrorField, SqlSession};

fn run(db: &mut BicDb, sql: &str) {
    SqlSession::new(db)
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
}

/// The SQLSTATE, message and DETAIL of a failing statement.
fn failure(db: &mut BicDb, sql: &str) -> (String, String, Option<String>) {
    let error: SqlError = SqlSession::new(db).execute(sql).unwrap_err();
    let detail = error.fields().into_iter().find_map(|field| match field {
        SqlErrorField::Detail(detail) => Some(detail),
        _ => None,
    });
    (error.sqlstate().to_string(), error.to_string(), detail)
}

fn expect(db: &mut BicDb, sql: &str, sqlstate: &str, message: &str, detail: &str) {
    assert_eq!(
        failure(db, sql),
        (
            sqlstate.to_string(),
            message.to_string(),
            Some(detail.to_string())
        ),
        "{sql}"
    );
}

#[test]
fn temporal_key_and_foreign_key_violations_show_the_keys() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    run(
        &mut db,
        "CREATE TABLE p (id int4range, valid_at daterange, \
         CONSTRAINT p_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS))",
    );
    run(
        &mut db,
        "INSERT INTO p VALUES ('[1,2)', '[2018-01-01,2018-02-01)'), \
         ('[1,2)', '[2018-02-01,2018-03-01)'), ('[2,3)', '[2018-01-01,2019-01-01)')",
    );
    expect(
        &mut db,
        "INSERT INTO p VALUES ('[1,2)', '[2018-01-15,2018-01-20)')",
        "23P01",
        "conflicting key value violates exclusion constraint \"p_pk\"",
        "Key (id, valid_at)=([1,2), [2018-01-15,2018-01-20)) conflicts with existing key \
         (id, valid_at)=([1,2), [2018-01-01,2018-02-01)).",
    );
    run(
        &mut db,
        "CREATE TABLE c (id int4range, parent_id int4range, valid_at daterange, \
         CONSTRAINT c_fk FOREIGN KEY (parent_id, PERIOD valid_at) REFERENCES p (id, PERIOD valid_at))",
    );
    run(
        &mut db,
        "INSERT INTO c VALUES ('[1,2)', '[1,2)', '[2018-01-05,2018-01-10)')",
    );
    expect(
        &mut db,
        "INSERT INTO c VALUES ('[4,5)', '[1,2)', '[2018-02-15,2018-04-01)')",
        "23503",
        "insert or update on table \"c\" violates foreign key constraint \"c_fk\"",
        "Key (parent_id, valid_at)=([1,2), [2018-02-15,2018-04-01)) is not present in table \"p\".",
    );
    expect(
        &mut db,
        "DELETE FROM p WHERE valid_at = '[2018-01-01,2018-02-01)'",
        "23503",
        "update or delete on table \"p\" violates foreign key constraint \"c_fk\" on table \"c\"",
        "Key (id, valid_at)=([1,2), [2018-01-01,2018-02-01)) is still referenced from table \"c\".",
    );
    // Adding a key over conflicting rows names the earlier row first.
    run(&mut db, "CREATE TABLE q (id int4range, valid_at daterange)");
    run(
        &mut db,
        "INSERT INTO q VALUES ('[1,2)', '[2018-01-01,2018-02-01)'), \
         ('[1,2)', '[2018-01-15,2018-03-01)')",
    );
    expect(
        &mut db,
        "ALTER TABLE q ADD CONSTRAINT q_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)",
        "23P01",
        "could not create exclusion constraint \"q_pk\"",
        "Key (id, valid_at)=([1,2), [2018-01-01,2018-02-01)) conflicts with key \
         (id, valid_at)=([1,2), [2018-01-15,2018-03-01)).",
    );
}

#[test]
fn exclusion_violations_show_the_keys_in_declared_order() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    run(&mut db, "CREATE TABLE r (room int, during int4range)");
    run(
        &mut db,
        "ALTER TABLE r ADD CONSTRAINT r_excl EXCLUDE USING gist (room WITH =, during WITH &&)",
    );
    run(&mut db, "CREATE TABLE r2 (room int, during int4range)");
    run(
        &mut db,
        "ALTER TABLE r2 ADD CONSTRAINT r2_excl EXCLUDE USING gist (during WITH &&, room WITH =)",
    );
    run(&mut db, "INSERT INTO r VALUES (1, '[1,5)')");
    run(&mut db, "INSERT INTO r2 VALUES (1, '[1,5)')");
    expect(
        &mut db,
        "INSERT INTO r VALUES (1, '[2,6)')",
        "23P01",
        "conflicting key value violates exclusion constraint \"r_excl\"",
        "Key (room, during)=(1, [2,6)) conflicts with existing key (room, during)=(1, [1,5)).",
    );
    expect(
        &mut db,
        "INSERT INTO r2 VALUES (1, '[2,6)')",
        "23P01",
        "conflicting key value violates exclusion constraint \"r2_excl\"",
        "Key (during, room)=([2,6), 1) conflicts with existing key (during, room)=([1,5), 1).",
    );
    run(&mut db, "CREATE TABLE r3 (room int, during int4range)");
    run(
        &mut db,
        "INSERT INTO r3 VALUES (1, '[1,5)'), (2, '[1,5)'), (1, '[3,8)')",
    );
    expect(
        &mut db,
        "ALTER TABLE r3 ADD CONSTRAINT r3_excl EXCLUDE USING gist (room WITH =, during WITH &&)",
        "23P01",
        "could not create exclusion constraint \"r3_excl\"",
        "Key (room, during)=(1, [1,5)) conflicts with key (room, during)=(1, [3,8)).",
    );
}

#[test]
fn exclusion_checks_see_the_transactions_own_writes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    run(
        &mut db,
        "CREATE TABLE p (id int4range, valid_at daterange, \
         CONSTRAINT p_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS))",
    );
    run(
        &mut db,
        "INSERT INTO p VALUES ('[1,2)', '[2018-01-01,2018-02-01)')",
    );
    let mut sql = SqlSession::new(&mut db);
    // A row inserted earlier in the transaction conflicts with a later one.
    sql.execute("BEGIN; INSERT INTO p VALUES ('[2,3)', '[2018-01-01,2018-02-01)')")
        .unwrap();
    assert_eq!(
        sql.execute("INSERT INTO p VALUES ('[2,3)', '[2018-01-15,2018-03-01)')")
            .unwrap_err()
            .sqlstate(),
        "23P01"
    );
    sql.execute("ROLLBACK").unwrap();
    // A row deleted earlier in the transaction no longer conflicts.
    sql.execute(
        "BEGIN; DELETE FROM p; \
         INSERT INTO p VALUES ('[1,2)', '[2018-01-15,2018-03-01)'); COMMIT",
    )
    .unwrap();
    drop(sql);
    assert_eq!(
        failure(&mut db, "INSERT INTO p VALUES ('[1,2)', '[2018-02-01,2018-02-02)')").0,
        "23P01"
    );
}
