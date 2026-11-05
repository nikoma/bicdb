use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn nested_command_in_json_runs_once_after_dispatch_warms() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE command_counter(id int PRIMARY KEY,n int); INSERT INTO command_counter VALUES(1,0); CREATE FUNCTION next_command() RETURNS int LANGUAGE plpgsql AS $$ DECLARE value int; BEGIN UPDATE command_counter SET n=n+1 WHERE id=1 RETURNING n INTO value; RETURN value; END $$; CREATE FUNCTION command_envelope() RETURNS jsonb LANGUAGE plpgsql AS $$ BEGIN RETURN jsonb_build_object('record_id',next_command()); END $$").unwrap();
    for expected in 1..=6 {
        let result = session.execute("SELECT (r->>'record_id')::int AS record_id,r->>'error' AS error FROM (SELECT command_envelope() AS r) q").unwrap();
        assert_eq!(
            result.rows,
            vec![vec![SqlValue::Int(expected), SqlValue::Null]],
            "call {expected}: a fallback cannot repeat an already executed command"
        );
        assert_eq!(
            session
                .execute("SELECT n FROM command_counter WHERE id=1")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(expected)]]
        );
    }
}

#[test]
fn sql_out_parameters_form_a_single_row_source() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE FUNCTION caller(OUT tenant text,OUT email text,OUT clinical boolean) LANGUAGE sql STABLE AS $$ SELECT 'north'::text,'coach@example.test'::text,true $$; CREATE FUNCTION no_caller(OUT tenant text,OUT clinical boolean) LANGUAGE sql AS $$ SELECT 'north'::text,true WHERE false $$").unwrap();
    let result = session
        .execute("SELECT c.tenant,c.email,c.clinical FROM caller() c")
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::String("north".into()),
            SqlValue::String("coach@example.test".into()),
            SqlValue::Bool(true)
        ]]
    );
    assert_eq!(
        result.column_types,
        vec![
            Some("text".into()),
            Some("text".into()),
            Some("bool".into())
        ]
    );
    assert_eq!(
        session
            .execute(
                "SELECT c.account,c.address,c.allowed FROM caller() AS c(account,address,allowed)"
            )
            .unwrap()
            .rows,
        result.rows
    );
    assert_eq!(
        session
            .execute("SELECT tenant,clinical FROM no_caller()")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Null, SqlValue::Null]]
    );
    session.execute("CREATE FUNCTION scalar_out(OUT n int) LANGUAGE sql AS $$ SELECT 1 UNION ALL SELECT 2 $$; CREATE FUNCTION advance(INOUT n int) LANGUAGE sql AS $$ SELECT n+1 $$").unwrap();
    assert_eq!(
        session.execute("SELECT n FROM scalar_out()").unwrap().rows,
        vec![vec![SqlValue::Int(1)]]
    );
    assert_eq!(
        session.execute("SELECT n FROM advance(4)").unwrap().rows,
        vec![vec![SqlValue::Int(5)]]
    );
    session.execute("CREATE ROLE out_guest; REVOKE ALL ON FUNCTION caller() FROM PUBLIC; SET ROLE out_guest").unwrap();
    assert_eq!(
        session
            .execute("SELECT * FROM caller()")
            .unwrap_err()
            .sqlstate(),
        "42501"
    );
    session
        .execute("RESET ROLE; GRANT EXECUTE ON FUNCTION caller() TO out_guest; SET ROLE out_guest")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT clinical FROM caller()")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]]
    );
    session.execute("RESET ROLE").unwrap();
}

#[test]
fn date_parameter_arithmetic_retains_its_type_through_timestamp_casts() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE scheduled(id int PRIMARY KEY,due_at timestamptz); INSERT INTO scheduled VALUES(1,'2026-11-05T12:00:00Z'); CREATE FUNCTION scheduled_count(p_day date) RETURNS bigint LANGUAGE plpgsql AS $$ BEGIN RETURN (SELECT count(*) FROM scheduled WHERE due_at<((p_day+1)::timestamp AT TIME ZONE 'UTC')); END $$").unwrap();
    for (day, expected) in [("2026-11-05", 1), ("2026-11-04", 0), ("2026-11-06", 1)] {
        assert_eq!(
            session
                .execute(&format!("SELECT scheduled_count(DATE '{day}')"))
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(expected)]]
        );
    }
    assert_eq!(
        session
            .execute("SELECT (1+2)::timestamp")
            .unwrap_err()
            .sqlstate(),
        "42846"
    );
}
