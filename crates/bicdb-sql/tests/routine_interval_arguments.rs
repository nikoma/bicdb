use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn named_interval_arguments_use_live_routine_values() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE FUNCTION advance_day(n int) RETURNS timestamp LANGUAGE plpgsql AS $$ DECLARE step interval;BEGIN step:=make_interval(days=>n,hours=>2);RETURN TIMESTAMP '2026-11-06 00:00:00'+step;END $$").unwrap();
    assert_eq!(
        session
            .execute("SELECT advance_day(3),advance_day(1)")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("2026-11-09 02:00:00".into()),
            SqlValue::String("2026-11-07 02:00:00".into())
        ]]
    );
}

#[test]
fn recurrence_stops_at_horizon_and_emits_distinct_days() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE FUNCTION daily_rows(step_days int) RETURNS TABLE(n int,due timestamp) LANGUAGE plpgsql AS $$ DECLARE step interval;k int:=0;BEGIN step:=make_interval(days=>step_days);LOOP due:=TIMESTAMP '2026-11-06 09:00:00'+step*k;EXIT WHEN due>TIMESTAMP '2026-11-08 09:00:00' OR k>10;n:=k;RETURN NEXT;k:=k+1;END LOOP;END $$").unwrap();
    let result = session.execute("SELECT * FROM daily_rows(1)").unwrap();
    assert_eq!(
        result.rows,
        vec![
            vec![
                SqlValue::Int(0),
                SqlValue::String("2026-11-06 09:00:00".into())
            ],
            vec![
                SqlValue::Int(1),
                SqlValue::String("2026-11-07 09:00:00".into())
            ],
            vec![
                SqlValue::Int(2),
                SqlValue::String("2026-11-08 09:00:00".into())
            ]
        ]
    );
}
