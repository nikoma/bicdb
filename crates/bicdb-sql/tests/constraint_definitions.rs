//! Constraint catalog texts as PostgreSQL 18 has them: exclusion constraints declared inside
//! CREATE TABLE (named or not, elements in declared order) and NOT NULL constraints.
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

fn fails(db: &mut BicDb, sql: &str) -> String {
    SqlSession::new(db)
        .execute(sql)
        .unwrap_err()
        .sqlstate()
        .to_string()
}

#[test]
fn create_table_takes_exclusion_constraints_inline() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    run(
        &mut db,
        "CREATE TABLE r (room int, during int4range, \
         EXCLUDE USING gist (room WITH =, during WITH &&))",
    );
    run(
        &mut db,
        "CREATE TABLE r2 (room int, during int4range, \
         CONSTRAINT r2_no_overlap EXCLUDE USING gist (during WITH &&, room WITH =))",
    );
    run(
        &mut db,
        "CREATE TABLE r3 (room int, during int4range, EXCLUDE USING gist (during WITH &&))",
    );
    assert_eq!(
        cells(
            &mut db,
            "SELECT conname, contype, pg_get_constraintdef(oid) FROM pg_constraint \
             WHERE contype = 'x' ORDER BY conname"
        ),
        [
            [
                "r2_no_overlap",
                "x",
                "EXCLUDE USING gist (during WITH &&, room WITH =)"
            ],
            ["r3_during_excl", "x", "EXCLUDE USING gist (during WITH &&)"],
            [
                "r_room_during_excl",
                "x",
                "EXCLUDE USING gist (room WITH =, during WITH &&)"
            ],
        ]
    );
    run(&mut db, "INSERT INTO r VALUES (1, '[1,5)'), (2, '[1,5)')");
    assert_eq!(fails(&mut db, "INSERT INTO r VALUES (1, '[3,8)')"), "23P01");
    run(&mut db, "INSERT INTO r2 VALUES (1, '[1,5)')");
    assert_eq!(
        fails(&mut db, "INSERT INTO r2 VALUES (1, '[3,8)')"),
        "23P01"
    );
    // A column named like the keyword is a column.
    run(&mut db, "CREATE TABLE kw (exclude int)");
    // A constraint that cannot be created leaves no table behind.
    assert_eq!(
        fails(
            &mut db,
            "CREATE TABLE bad (room int, EXCLUDE USING gist (missing WITH &&))"
        ),
        "42703"
    );
    assert_eq!(fails(&mut db, "SELECT * FROM bad"), "42P01");
}

#[test]
fn not_null_constraint_definitions_name_the_column_after_the_keywords() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    run(&mut db, "CREATE TABLE n (id int NOT NULL, v text)");
    assert_eq!(
        cells(
            &mut db,
            "SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint \
             WHERE conrelid::regclass::text = 'n' AND contype = 'n'"
        ),
        [["n_id_not_null", "NOT NULL id"]]
    );
}
