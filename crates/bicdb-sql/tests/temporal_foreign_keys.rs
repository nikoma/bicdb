//! PostgreSQL 18 temporal foreign keys: FOREIGN KEY (a, PERIOD p) REFERENCES t (b, PERIOD q).
use bicdb_core::BicDb;
use bicdb_sql::SqlSession;

fn run(db: &mut BicDb, sql: &str) {
    SqlSession::new(db)
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
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
fn temporal_foreign_keys_on_partitioned_tables_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    run(
        &mut db,
        "CREATE TABLE pp (id int4range, valid_at daterange, \
         CONSTRAINT pp_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)) PARTITION BY LIST (id)",
    );
    run(
        &mut db,
        "CREATE TABLE pp1 PARTITION OF pp FOR VALUES IN ('[1,2)')",
    );
    assert_eq!(
        fails(
            &mut db,
            "CREATE TABLE cc (parent_id int4range, valid_at daterange, \
             FOREIGN KEY (parent_id, PERIOD valid_at) REFERENCES pp (id, PERIOD valid_at))"
        ),
        (
            "0A000".to_string(),
            "foreign keys using PERIOD on partitioned tables are not supported yet".to_string()
        )
    );
    // Nothing was left behind.
    assert_eq!(fails(&mut db, "SELECT * FROM cc").0, "42P01");
}
