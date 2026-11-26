//! INSERT ... ON CONFLICT against exclusion constraints and temporal keys, as PostgreSQL 18
//! treats them: DO NOTHING may use them as arbiters, DO UPDATE may not, and a column list
//! never infers them.
use bicdb_core::BicDb;
use bicdb_sql::SqlSession;

fn run(db: &mut BicDb, sql: &str) -> usize {
    SqlSession::new(db)
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .command_tag
        .and_then(|tag| tag.rsplit(' ').next()?.parse().ok())
        .unwrap_or(0)
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

const NO_MATCH: &str =
    "there is no unique or exclusion constraint matching the ON CONFLICT specification";
const NO_UPDATE: &str = "ON CONFLICT DO UPDATE not supported with exclusion constraints";

#[test]
fn temporal_keys_arbitrate_do_nothing_but_not_do_update() {
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
    let row = "INSERT INTO p VALUES ('[1,2)', '[2018-01-15,2018-03-01)')";
    assert_eq!(run(&mut db, &format!("{row} ON CONFLICT DO NOTHING")), 0);
    assert_eq!(
        run(
            &mut db,
            &format!("{row} ON CONFLICT ON CONSTRAINT p_pk DO NOTHING")
        ),
        0
    );
    assert_eq!(
        fails(
            &mut db,
            &format!("{row} ON CONFLICT (id, valid_at) DO NOTHING")
        ),
        ("42P10".to_string(), NO_MATCH.to_string())
    );
    assert_eq!(
        fails(
            &mut db,
            &format!("{row} ON CONFLICT (id, valid_at) DO UPDATE SET id = '[9,10)'")
        ),
        ("42P10".to_string(), NO_MATCH.to_string())
    );
    assert_eq!(
        fails(
            &mut db,
            &format!("{row} ON CONFLICT ON CONSTRAINT p_pk DO UPDATE SET id = '[9,10)'")
        ),
        ("42809".to_string(), NO_UPDATE.to_string())
    );
    // A row that conflicts with nothing is inserted.
    assert_eq!(
        run(
            &mut db,
            "INSERT INTO p VALUES ('[1,2)', '[2018-05-01,2018-06-01)') \
             ON CONFLICT ON CONSTRAINT p_pk DO NOTHING"
        ),
        1
    );
    assert_eq!(cells(&mut db, "SELECT count(*) FROM p"), [["2"]]);
}

#[test]
fn exclusion_constraints_arbitrate_do_nothing_but_not_do_update() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    run(&mut db, "CREATE TABLE r (room int, during int4range)");
    run(
        &mut db,
        "ALTER TABLE r ADD CONSTRAINT r_excl EXCLUDE USING gist (room WITH =, during WITH &&)",
    );
    run(&mut db, "INSERT INTO r VALUES (1, '[1,5)')");
    let row = "INSERT INTO r VALUES (1, '[2,6)')";
    assert_eq!(run(&mut db, &format!("{row} ON CONFLICT DO NOTHING")), 0);
    assert_eq!(
        run(
            &mut db,
            &format!("{row} ON CONFLICT ON CONSTRAINT r_excl DO NOTHING")
        ),
        0
    );
    assert_eq!(
        fails(
            &mut db,
            &format!("{row} ON CONFLICT ON CONSTRAINT r_excl DO UPDATE SET room = 2")
        ),
        ("42809".to_string(), NO_UPDATE.to_string())
    );
    assert_eq!(cells(&mut db, "SELECT count(*) FROM r"), [["1"]]);
}
