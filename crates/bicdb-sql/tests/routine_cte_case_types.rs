use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};
use serde_json::json;

#[test]
fn routine_case_keeps_unresolved_cte_function_arguments_out_of_text_inference() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for function in ["LEAST", "GREATEST", "COALESCE"] {
        session
            .execute(&format!(
                "CREATE OR REPLACE FUNCTION cte_case() RETURNS jsonb LANGUAGE sql AS $$
             WITH t AS (SELECT 4::int AS n)
             SELECT jsonb_agg(jsonb_build_object('n',CASE WHEN true
                 THEN {function}(t.n,t.n) ELSE 0 END)) FROM t $$"
            ))
            .unwrap();
        for _ in 0..3 {
            assert_eq!(
                session.execute("SELECT cte_case()").unwrap().rows,
                vec![vec![SqlValue::Json(json!([{"n": 4}]).into())]],
                "{function} over a CTE column has no context-free text type"
            );
        }
    }
    // Literal-only unknowns still resolve to text; known incompatible CASE
    // branches still fail rather than bypassing conditional type validation.
    session.execute("CREATE FUNCTION unknown_literals() RETURNS text LANGUAGE sql AS $$ SELECT CASE WHEN true THEN COALESCE(NULL,'kept') ELSE 'other' END $$").unwrap();
    assert_eq!(
        session.execute("SELECT unknown_literals()").unwrap().rows,
        vec![vec![SqlValue::String("kept".into())]]
    );
    session.execute("CREATE FUNCTION incompatible_case() RETURNS jsonb LANGUAGE sql AS $$ WITH t AS (SELECT 4::int AS n) SELECT jsonb_agg(jsonb_build_object('n', CASE WHEN true THEN LEAST(t.n,t.n)::text ELSE 0 END)) FROM t $$").unwrap();
    assert_eq!(
        session
            .execute("SELECT incompatible_case()")
            .unwrap_err()
            .sqlstate(),
        "42804"
    );
}

#[test]
fn scheduled_adherence_case_preserves_dates_and_numeric_result_in_a_sql_routine() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE FUNCTION adherence(p_now timestamptz) RETURNS jsonb LANGUAGE sql AS $$
        WITH rx AS (SELECT 'daily'::text AS frequency,14::int AS duration_days,
            LEAST(p_now,TIMESTAMPTZ '2026-11-17 12:00:00+00') AS effective_end,
            TIMESTAMPTZ '2026-11-03 12:00:00+00' AS starts_at),
        s AS (SELECT GREATEST(0,(effective_end AT TIME ZONE 'UTC')::date
            -(starts_at AT TIME ZONE 'UTC')::date+1) AS days_elapsed,
            1::int AS weeks_elapsed FROM rx)
        SELECT jsonb_agg(jsonb_build_object('expected',CASE rx.frequency
            WHEN 'daily' THEN LEAST(s.days_elapsed,rx.duration_days)
            WHEN 'weekly' THEN s.weeks_elapsed ELSE s.weeks_elapsed*3 END))
        FROM rx,s $$",
        )
        .unwrap();
    for (date, expected) in [("2026-11-06", 4), ("2026-11-07", 5), ("2026-11-20", 14)] {
        let query = format!("SELECT adherence(TIMESTAMPTZ '{date} 12:00:00+00')");
        assert_eq!(
            session.execute(&query).unwrap().rows,
            vec![vec![SqlValue::Json(json!([{"expected": expected}]).into())]]
        );
    }
}

#[test]
fn therapeutic_content_status_reports_completion_and_adherence_with_typed_ctes() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(include_str!("fixtures/therapeutic_content_status.sql"))
        .unwrap();
    let query = "SELECT content_prescription_status(UUID '11111111-1111-4111-8111-111111111111',TIMESTAMPTZ '2026-11-06 12:00:00+00')";
    let result = session.execute(query).unwrap();
    let SqlValue::Json(status) = &result.rows[0][0] else {
        panic!("JSONB status required")
    };
    assert_eq!(status[0]["expected"], 4);
    assert_eq!(status[0]["completed"], 0);
    assert_eq!(status[0]["adherence_percent"].as_f64(), Some(0.0));
    assert_eq!(status[0]["due_now"], true);
    session.execute("INSERT INTO content_engagement_events(id,patient_profile_id,prescription_id,event_kind,occurred_at)
        VALUES ('55555555-5555-4555-8555-555555555555','11111111-1111-4111-8111-111111111111','44444444-4444-4444-8444-444444444444','completed','2026-11-04 12:00:00+00'),
        ('66666666-6666-4666-8666-666666666666','11111111-1111-4111-8111-111111111111','44444444-4444-4444-8444-444444444444','completed','2026-11-06 11:00:00+00')").unwrap();
    let result = session.execute(query).unwrap();
    let SqlValue::Json(status) = &result.rows[0][0] else {
        panic!("JSONB status required")
    };
    assert_eq!(status[0]["expected"], 4);
    assert_eq!(status[0]["completed"], 2);
    assert_eq!(status[0]["adherence_percent"].as_f64(), Some(50.0));
    assert_eq!(status[0]["done_today"], true);
    assert_eq!(status[0]["due_now"], false);
}
