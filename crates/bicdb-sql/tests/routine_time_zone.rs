use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn routine_time_zone_operands_use_live_typed_parameters() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("SET TIME ZONE 'UTC'; CREATE FUNCTION local_clock(t timestamptz,z text) RETURNS timestamp LANGUAGE plpgsql AS $$ BEGIN RETURN t AT TIME ZONE z; END $$; CREATE FUNCTION absolute_clock(t timestamp,z text) RETURNS timestamptz LANGUAGE plpgsql AS $$ BEGIN RETURN t AT TIME ZONE z; END $$").unwrap();
    for query in [
        "SELECT local_clock('2026-07-01 12:00:00+00'::timestamptz,'America/New_York')=TIMESTAMP '2026-07-01 08:00:00'",
        "SELECT local_clock('2026-01-01 12:00:00+00'::timestamptz,'America/New_York')=TIMESTAMP '2026-01-01 07:00:00'",
        "SELECT absolute_clock(TIMESTAMP '2026-07-01 08:00:00','America/New_York')=TIMESTAMPTZ '2026-07-01 12:00:00+00'",
        "SELECT local_clock(NULL,'UTC') IS NULL",
        "SELECT local_clock(NOW(),NULL) IS NULL",
    ] {
        assert_eq!(session.execute(query).unwrap().rows[0][0], SqlValue::Bool(true), "{query}");
    }
    assert_eq!(
        session
            .execute("SELECT local_clock(NOW(),'invalid/zone')")
            .unwrap_err()
            .sqlstate(),
        "22023"
    );
}

#[test]
fn row_trigger_validates_the_actual_zone_and_preserves_invalid_zone_refusal() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("SET TIME ZONE 'UTC'; CREATE TABLE zone_items(id int PRIMARY KEY,zone text,stamp timestamptz,local_stamp timestamp);CREATE FUNCTION verify_zone() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM now() AT TIME ZONE NEW.zone;NEW.local_stamp:=NEW.stamp AT TIME ZONE NEW.zone;RETURN NEW;END $$;CREATE TRIGGER zone_guard BEFORE INSERT ON zone_items FOR EACH ROW EXECUTE FUNCTION verify_zone()").unwrap();
    session.execute("INSERT INTO zone_items(id,zone,stamp) VALUES(1,'America/New_York','2026-07-01 12:00:00+00')").unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT local_stamp=TIMESTAMP '2026-07-01 08:00:00' FROM zone_items WHERE id=1"
            )
            .unwrap()
            .rows[0][0],
        SqlValue::Bool(true)
    );
    assert_eq!(
        session
            .execute("INSERT INTO zone_items(id,zone,stamp) VALUES(2,'invalid/zone',NOW())")
            .unwrap_err()
            .sqlstate(),
        "22023"
    );
    assert_eq!(
        session
            .execute("SELECT count(*) FROM zone_items")
            .unwrap()
            .rows[0][0]
            .to_cell(),
        "1"
    );
}
