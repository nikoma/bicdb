//! PostgreSQL 18 temporal foreign keys: FOREIGN KEY (a, PERIOD p) REFERENCES t (b, PERIOD q).
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

fn setup(range: &str, period: &str) -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    run(
        &mut db,
        &format!(
            "CREATE TABLE p (id int4range, valid_at {range}, \
             CONSTRAINT p_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS))"
        ),
    );
    run(
        &mut db,
        &format!(
            "INSERT INTO p VALUES ('[1,2)', {}), ('[1,2)', {}), ('[2,3)', {})",
            period.replace("{}", "[2018-01-01,2018-02-01)"),
            period.replace("{}", "[2018-02-01,2018-03-01)"),
            period.replace("{}", "[2018-01-01,2019-01-01)")
        ),
    );
    run(
        &mut db,
        &format!(
            "CREATE TABLE c (id int4range, parent_id int4range, valid_at {range}, \
             CONSTRAINT c_fk FOREIGN KEY (parent_id, PERIOD valid_at) REFERENCES p (id, PERIOD valid_at))"
        ),
    );
    (dir, db)
}

fn exercise(range: &str, period: &str) {
    let lit = |text: &str| period.replace("{}", text);
    let (_dir, mut db) = setup(range, period);
    // Covered by one parent row, and by two adjacent ones together.
    run(
        &mut db,
        &format!(
            "INSERT INTO c VALUES ('[1,2)', '[1,2)', {})",
            lit("[2018-01-05,2018-01-10)")
        ),
    );
    run(
        &mut db,
        &format!(
            "INSERT INTO c VALUES ('[2,3)', '[1,2)', {})",
            lit("[2018-01-15,2018-02-15)")
        ),
    );
    // NULL components exempt the row.
    run(&mut db, "INSERT INTO c VALUES ('[3,4)', '[1,2)', NULL)");
    assert_eq!(
        fails(
            &mut db,
            &format!(
                "INSERT INTO c VALUES ('[4,5)', '[1,2)', {})",
                lit("[2018-02-15,2018-04-01)")
            )
        ),
        (
            "23503".to_string(),
            "insert or update on table \"c\" violates foreign key constraint \"c_fk\"".to_string()
        )
    );
    assert_eq!(
        fails(
            &mut db,
            &format!(
                "INSERT INTO c VALUES ('[4,5)', '[9,10)', {})",
                lit("[2018-01-05,2018-01-10)")
            )
        )
        .0,
        "23503"
    );
    // Parent changes that would leave a referencing row uncovered are refused.
    let (state, message) = fails(
        &mut db,
        &format!(
            "DELETE FROM p WHERE id = '[1,2)' AND valid_at = {}",
            lit("[2018-02-01,2018-03-01)")
        ),
    );
    assert_eq!(state, "23503");
    assert_eq!(
        message,
        "update or delete on table \"p\" violates foreign key constraint \"c_fk\" on table \"c\""
    );
    assert_eq!(
        fails(
            &mut db,
            &format!(
                "UPDATE p SET valid_at = {} WHERE id = '[1,2)' AND valid_at = {}",
                lit("[2018-01-01,2018-01-07)"),
                lit("[2018-01-01,2018-02-01)")
            )
        )
        .0,
        "23503"
    );
    // Changes that keep every reference covered are fine.
    run(
        &mut db,
        &format!(
            "UPDATE p SET valid_at = {} WHERE id = '[1,2)' AND valid_at = {}",
            lit("[2017-01-01,2018-02-01)"),
            lit("[2018-01-01,2018-02-01)")
        ),
    );
    run(&mut db, "DELETE FROM p WHERE id = '[2,3)'");
    // The child's own updates are checked too.
    assert_eq!(
        fails(
            &mut db,
            &format!(
                "UPDATE c SET valid_at = {} WHERE id = '[1,2)'",
                lit("[2018-01-05,2018-05-01)")
            )
        )
        .0,
        "23503"
    );
}

#[test]
fn ranges_must_be_covered_by_the_referenced_periods() {
    exercise("daterange", "'{}'");
}

#[test]
fn multiranges_must_be_covered_by_the_referenced_periods() {
    exercise("datemultirange", "'{{}}'");
}

#[test]
fn temporal_foreign_key_definitions_follow_postgresql() {
    let (_dir, mut db) = setup("daterange", "'{}'");
    assert_eq!(
        fails(
            &mut db,
            "CREATE TABLE x1 (parent_id int4range, valid_at daterange, \
             FOREIGN KEY (parent_id, PERIOD valid_at) REFERENCES p (id, valid_at))"
        )
        .1,
        "foreign key uses PERIOD on the referencing table but not the referenced table"
    );
    assert_eq!(
        fails(
            &mut db,
            "CREATE TABLE x2 (parent_id int4range, valid_at daterange, \
             FOREIGN KEY (parent_id, valid_at) REFERENCES p (id, PERIOD valid_at))"
        )
        .1,
        "foreign key uses PERIOD on the referenced table but not the referencing table"
    );
    assert_eq!(
        fails(
            &mut db,
            "CREATE TABLE x3 (parent_id int4range, valid_at daterange, \
             FOREIGN KEY (parent_id, PERIOD valid_at) REFERENCES p (id, PERIOD valid_at) ON DELETE CASCADE)"
        ),
        (
            "0A000".to_string(),
            "unsupported ON DELETE action for foreign key constraint using PERIOD".to_string()
        )
    );
    // REFERENCES without a column list uses the referenced table's temporal primary key.
    run(
        &mut db,
        "CREATE TABLE x4 (parent_id int4range, valid_at daterange, \
         CONSTRAINT x4_fk FOREIGN KEY (parent_id, PERIOD valid_at) REFERENCES p)",
    );
    assert_eq!(
        fails(
            &mut db,
            "INSERT INTO x4 VALUES ('[1,2)', '[2020-01-01,2020-02-01)')"
        )
        .0,
        "23503"
    );
    // ALTER TABLE ADD over violating rows fails; DROP CONSTRAINT removes it.
    run(
        &mut db,
        "CREATE TABLE x5 (parent_id int4range, valid_at daterange)",
    );
    run(
        &mut db,
        "INSERT INTO x5 VALUES ('[1,2)', '[2020-01-01,2020-02-01)')",
    );
    assert_eq!(
        fails(
            &mut db,
            "ALTER TABLE x5 ADD CONSTRAINT x5_fk FOREIGN KEY (parent_id, PERIOD valid_at) REFERENCES p (id, PERIOD valid_at)"
        )
        .0,
        "23503"
    );
    run(&mut db, "ALTER TABLE x4 DROP CONSTRAINT x4_fk");
    run(
        &mut db,
        "INSERT INTO x4 VALUES ('[1,2)', '[2020-01-01,2020-02-01)')",
    );
    // The referenced table cannot be dropped or truncated from under the reference.
    assert_eq!(fails(&mut db, "DROP TABLE p").0, "2BP01");
    assert_eq!(fails(&mut db, "TRUNCATE p").0, "0A000");
    run(&mut db, "DROP TABLE p CASCADE");
    run(
        &mut db,
        "INSERT INTO c VALUES ('[9,10)', '[7,8)', '[2020-01-01,2020-02-01)')",
    );
}

#[test]
fn temporal_foreign_keys_cover_every_partition() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    for sql in [
        "CREATE TABLE p (id int4range, valid_at daterange, name text, \
         CONSTRAINT p_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)) PARTITION BY LIST (id)",
        "CREATE TABLE p1 PARTITION OF p FOR VALUES IN ('[1,2)', '[3,4)')",
        "CREATE TABLE p2 PARTITION OF p FOR VALUES IN ('[2,3)')",
        "INSERT INTO p VALUES ('[1,2)', '[2018-01-01,2018-02-01)'), \
         ('[2,3)', '[2018-01-01,2019-01-01)')",
        "CREATE TABLE c (id int4range, valid_at daterange, parent_id int4range, \
         CONSTRAINT c_fk FOREIGN KEY (parent_id, PERIOD valid_at) REFERENCES p) \
         PARTITION BY LIST (id)",
        "CREATE TABLE c1 PARTITION OF c FOR VALUES IN ('[1,2)')",
        "CREATE TABLE c2 PARTITION OF c FOR VALUES IN ('[2,3)')",
        "INSERT INTO c VALUES ('[1,2)', '[2018-01-05,2018-01-10)', '[1,2)'), \
         ('[2,3)', '[2018-03-01,2018-04-01)', '[2,3)')",
    ] {
        run(&mut db, sql);
    }
    // Child rows are checked whether written through the parent or the partition.
    let insert = fails(
        &mut db,
        "INSERT INTO c2 VALUES ('[2,3)', '[2019-03-01,2019-04-01)', '[2,3)')",
    );
    assert_eq!(insert.0, "23503");
    assert_eq!(
        insert.1,
        "insert or update on table \"c2\" violates foreign key constraint \"c_fk\""
    );
    let insert = fails(
        &mut db,
        "INSERT INTO c VALUES ('[1,2)', '[2018-03-01,2018-04-01)', '[1,2)')",
    );
    assert_eq!(
        insert,
        (
            "23503".to_string(),
            "insert or update on table \"c1\" violates foreign key constraint \"c_fk\"".to_string()
        )
    );
    assert_eq!(
        fails(
            &mut db,
            "UPDATE c SET valid_at = '[2019-01-01,2019-02-01)' WHERE id = '[2,3)'"
        )
        .0,
        "23503"
    );
    // Parent rows are protected in every partition, through either name.
    for sql in [
        "UPDATE p SET valid_at = '[2018-01-01,2018-01-07)' WHERE id = '[1,2)'",
        "DELETE FROM p1",
        "DELETE FROM p WHERE id = '[1,2)'",
        "UPDATE p SET id = '[3,4)' WHERE id = '[1,2)'",
    ] {
        let error = fails(&mut db, sql);
        assert_eq!(
            error,
            (
                "23503".to_string(),
                "update or delete on table \"p1\" violates foreign key constraint \"c_fk_1\" on table \"c\""
                    .to_string()
            ),
            "{sql}"
        );
    }
    for (sql, detail) in [("TRUNCATE p1", "p1"), ("TRUNCATE p", "p")] {
        let (state, message) = fails(&mut db, sql);
        assert_eq!(state, "0A000", "{sql}");
        assert_eq!(
            message, "cannot truncate a table referenced in a foreign key constraint",
            "{sql} ({detail})"
        );
    }
    assert_eq!(
        fails(&mut db, "ALTER TABLE p DETACH PARTITION p2"),
        (
            "23503".to_string(),
            "removing partition \"p2\" violates foreign key constraint \"c_fk_2\"".to_string()
        )
    );
    assert_eq!(fails(&mut db, "DROP TABLE p2").0, "2BP01");
    assert_eq!(cells(&mut db, "SELECT count(*) FROM p"), [["2"]]);
    // Uncovered rows refuse an ATTACH.
    run(
        &mut db,
        "CREATE TABLE c3 (id int4range, valid_at daterange, parent_id int4range)",
    );
    run(
        &mut db,
        "INSERT INTO c3 VALUES ('[3,4)', '[2020-01-01,2020-02-01)', '[1,2)')",
    );
    assert_eq!(
        fails(
            &mut db,
            "ALTER TABLE c ATTACH PARTITION c3 FOR VALUES IN ('[3,4)')"
        )
        .0,
        "23503"
    );
    // The catalog: the declared key, its rows per referenced partition, and the copies the
    // partitions inherit.
    assert_eq!(
        cells(
            &mut db,
            "SELECT conname, conrelid::regclass::text, confrelid::regclass::text, \
             conparentid <> 0, conislocal, coninhcount, pg_get_constraintdef(oid) \
             FROM pg_constraint WHERE contype = 'f' ORDER BY 2, 3, 1"
        ),
        [
            [
                "c_fk",
                "c",
                "p",
                "false",
                "true",
                "0",
                "FOREIGN KEY (parent_id, PERIOD valid_at) REFERENCES p(id, PERIOD valid_at)"
            ],
            [
                "c_fk_1",
                "c",
                "p1",
                "true",
                "false",
                "1",
                "FOREIGN KEY (parent_id, PERIOD valid_at) REFERENCES p1(id, PERIOD valid_at)"
            ],
            [
                "c_fk_2",
                "c",
                "p2",
                "true",
                "false",
                "1",
                "FOREIGN KEY (parent_id, PERIOD valid_at) REFERENCES p2(id, PERIOD valid_at)"
            ],
            [
                "c_fk",
                "c1",
                "p",
                "true",
                "false",
                "1",
                "FOREIGN KEY (parent_id, PERIOD valid_at) REFERENCES p(id, PERIOD valid_at)"
            ],
            [
                "c_fk",
                "c2",
                "p",
                "true",
                "false",
                "1",
                "FOREIGN KEY (parent_id, PERIOD valid_at) REFERENCES p(id, PERIOD valid_at)"
            ],
        ]
    );
    // A detached partition keeps the key as its own.
    run(&mut db, "ALTER TABLE c DETACH PARTITION c1");
    assert_eq!(
        fails(
            &mut db,
            "INSERT INTO c1 VALUES ('[1,2)', '[2018-03-01,2018-04-01)', '[1,2)')"
        )
        .0,
        "23503"
    );
    run(&mut db, "ALTER TABLE c1 DROP CONSTRAINT c_fk");
    run(
        &mut db,
        "INSERT INTO c1 VALUES ('[1,2)', '[2018-03-01,2018-04-01)', '[1,2)')",
    );
    // With no references left, the parent partition can go.
    run(&mut db, "DELETE FROM c2");
    run(&mut db, "ALTER TABLE p DETACH PARTITION p2");
}

#[test]
fn temporal_foreign_keys_are_listed_in_the_catalogs() {
    let (_dir, mut db) = setup("daterange", "'{}'");
    assert_eq!(
        cells(
            &mut db,
            "SELECT conname, contype, conperiod, confupdtype, confdeltype, \
             pg_get_constraintdef(oid) FROM pg_constraint \
             WHERE conrelid IN ('p'::regclass, 'c'::regclass) AND contype <> 'n' ORDER BY conname"
        ),
        [
            [
                "c_fk",
                "f",
                "true",
                "a",
                "a",
                "FOREIGN KEY (parent_id, PERIOD valid_at) REFERENCES p(id, PERIOD valid_at)"
            ],
            [
                "p_pk",
                "p",
                "true",
                " ",
                " ",
                "PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)"
            ],
        ]
    );
    assert_eq!(
        cells(
            &mut db,
            "SELECT conkey::text, confkey::text, confrelid::regclass::text = 'p' \
             FROM pg_constraint WHERE conname = 'c_fk'"
        ),
        [["{2,3}", "{1,2}", "true"]]
    );
    assert_eq!(
        cells(
            &mut db,
            "SELECT constraint_type FROM information_schema.table_constraints \
             WHERE table_name = 'c' AND constraint_name = 'c_fk'"
        ),
        [["FOREIGN KEY"]]
    );
    assert_eq!(
        cells(
            &mut db,
            "SELECT column_name, ordinal_position FROM information_schema.key_column_usage \
             WHERE constraint_name = 'c_fk' ORDER BY ordinal_position"
        ),
        [["parent_id", "1"], ["valid_at", "2"]]
    );
}

#[test]
fn deferrable_temporal_foreign_keys_are_checked_at_commit() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    run(
        &mut db,
        "CREATE TABLE p (id int4range, valid_at daterange, \
         CONSTRAINT p_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS))",
    );
    run(
        &mut db,
        "CREATE TABLE c (id int4range, parent_id int4range, valid_at daterange, \
         CONSTRAINT c_fk FOREIGN KEY (parent_id, PERIOD valid_at) REFERENCES p (id, PERIOD valid_at) \
         DEFERRABLE INITIALLY DEFERRED)",
    );
    assert_eq!(
        cells(
            &mut db,
            "SELECT condeferrable, condeferred, pg_get_constraintdef(oid) FROM pg_constraint \
             WHERE conname = 'c_fk'"
        ),
        [[
            "true",
            "true",
            "FOREIGN KEY (parent_id, PERIOD valid_at) REFERENCES p(id, PERIOD valid_at) \
             DEFERRABLE INITIALLY DEFERRED"
        ]]
    );
    let mut sql = SqlSession::new(&mut db);
    // The child may come first; the parent covers it by commit.
    sql.execute(
        "BEGIN; INSERT INTO c VALUES ('[1,2)', '[1,2)', '[2018-01-05,2018-01-10)'); \
         INSERT INTO p VALUES ('[1,2)', '[2018-01-01,2018-02-01)'); COMMIT",
    )
    .unwrap();
    // An uncovered child fails at COMMIT, and the transaction rolls back.
    sql.execute("BEGIN; INSERT INTO c VALUES ('[2,3)', '[1,2)', '[2018-01-20,2018-03-01)')")
        .unwrap();
    let error = sql.execute("COMMIT").unwrap_err();
    assert_eq!(error.sqlstate(), "23503");
    assert_eq!(
        error.to_string(),
        "insert or update on table \"c\" violates foreign key constraint \"c_fk\""
    );
    drop(sql);
    assert_eq!(cells(&mut db, "SELECT count(*) FROM c"), [["1"]]);
    let mut sql = SqlSession::new(&mut db);
    // Uncovering the child through the parent is deferred too: covering it again in time is fine.
    sql.execute(
        "BEGIN; UPDATE p SET valid_at = '[2018-01-20,2018-02-01)'; \
         UPDATE p SET valid_at = '[2018-01-01,2018-01-15)'; COMMIT",
    )
    .unwrap();
    sql.execute("BEGIN; DELETE FROM p").unwrap();
    let error = sql.execute("SET CONSTRAINTS c_fk IMMEDIATE").unwrap_err();
    assert_eq!(error.sqlstate(), "23503");
    sql.execute("ROLLBACK").unwrap();
    // NOT DEFERRABLE again: checked at once.
    sql.execute("ALTER TABLE c ALTER CONSTRAINT c_fk NOT DEFERRABLE")
        .unwrap();
    sql.execute("BEGIN").unwrap();
    assert_eq!(
        sql.execute("INSERT INTO c VALUES ('[3,4)', '[9,10)', '[2018-01-01,2018-01-02)')")
            .unwrap_err()
            .sqlstate(),
        "23503"
    );
    sql.execute("ROLLBACK").unwrap();
    drop(sql);
    assert_eq!(
        cells(
            &mut db,
            "SELECT condeferrable, condeferred FROM pg_constraint WHERE conname = 'c_fk'"
        ),
        [["false", "false"]]
    );
}
