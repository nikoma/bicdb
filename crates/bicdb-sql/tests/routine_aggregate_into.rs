use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};
#[test]
fn aggregate_cte_into_keeps_multiple_targets_and_array_selection() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(r#"CREATE FUNCTION aggregate_into(e jsonb) RETURNS jsonb LANGUAGE plpgsql AS $$
 DECLARE n int;ids jsonb;v numeric;night_start int:=18;night_end int:=8;
 BEGIN
 WITH r AS (SELECT '00000000-0000-0000-0000-000000000001'::uuid obs_id,2::numeric value,'2026-11-01 12:00:00+00'::timestamptz effective_at,'2026-11-01 12:00:00'::timestamp local_ts)
    SELECT count(*)::int,
      COALESCE(jsonb_agg(r.obs_id ORDER BY r.effective_at), '[]'::jsonb),
      CASE e->>'agg'
        WHEN 'count' THEN count(*)::numeric
        WHEN 'sum' THEN COALESCE(sum(r.value), 0)
        WHEN 'min' THEN min(r.value)
        WHEN 'max' THEN max(r.value)
        WHEN 'avg' THEN avg(r.value)
        WHEN 'latest' THEN (array_agg(r.value ORDER BY r.effective_at DESC))[1]
        WHEN 'first' THEN (array_agg(r.value ORDER BY r.effective_at ASC))[1]
        WHEN 'distinct_days' THEN count(DISTINCT r.local_ts::date)::numeric
        -- A night runs from start_hour to end_hour local time and belongs to the evening's date, so
        -- 23:30 and 02:00 the next morning are one night. Readings outside the night window are ignored.
        WHEN 'distinct_nights' THEN count(DISTINCT CASE
            WHEN extract(hour FROM r.local_ts) >= night_start THEN r.local_ts::date
            WHEN extract(hour FROM r.local_ts) < night_end THEN (r.local_ts - interval '1 day')::date END)::numeric
        WHEN 'slope_per_day' THEN CASE WHEN count(*) >= 2
            THEN regr_slope(r.value, extract(epoch FROM r.effective_at) / 86400.0)::numeric END
      END
    INTO n, ids, v
    FROM r;
 RETURN jsonb_build_object('n',n,'ids',ids,'v',v);END $$"#).unwrap();
    assert_eq!(sql.execute("SELECT (aggregate_into('{\"agg\":\"avg\"}'::jsonb)->>'n')::int,(aggregate_into('{\"agg\":\"latest\"}'::jsonb)->>'v')::int").unwrap().rows,vec![vec![SqlValue::Int(1),SqlValue::Int(2)]]);
    assert_eq!(
        sql.execute("SELECT jsonb_array_length(aggregate_into('{\"agg\":\"avg\"}'::jsonb)->'ids')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );
}

#[test]
fn plain_aggregate_query_keeps_existing_behavior() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(r#" WITH r AS (SELECT '00000000-0000-0000-0000-000000000001'::uuid obs_id,2::numeric value,'2026-11-01 12:00:00+00'::timestamptz effective_at,'2026-11-01 12:00:00'::timestamp local_ts)
    SELECT count(*)::int,
      COALESCE(jsonb_agg(r.obs_id ORDER BY r.effective_at), '[]'::jsonb),
      CASE 'latest'
        WHEN 'count' THEN count(*)::numeric
        WHEN 'sum' THEN COALESCE(sum(r.value), 0)
        WHEN 'min' THEN min(r.value)
        WHEN 'max' THEN max(r.value)
        WHEN 'avg' THEN avg(r.value)
        WHEN 'latest' THEN (array_agg(r.value ORDER BY r.effective_at DESC))[1]
        WHEN 'first' THEN (array_agg(r.value ORDER BY r.effective_at ASC))[1]
        WHEN 'distinct_days' THEN count(DISTINCT r.local_ts::date)::numeric
        -- A night runs from start_hour to end_hour local time and belongs to the evening's date, so
        -- 23:30 and 02:00 the next morning are one night. Readings outside the night window are ignored.
        WHEN 'distinct_nights' THEN count(DISTINCT CASE
            WHEN extract(hour FROM r.local_ts) >= 18 THEN r.local_ts::date
            WHEN extract(hour FROM r.local_ts) < 8 THEN (r.local_ts - interval '1 day')::date END)::numeric
        WHEN 'slope_per_day' THEN CASE WHEN count(*) >= 2
            THEN regr_slope(r.value, extract(epoch FROM r.effective_at) / 86400.0)::numeric END
      END
    FROM r;
"#).unwrap();
}

#[test]
fn aggregate_without_sql_comments_is_a_control() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(r#"CREATE FUNCTION aggregate_into(e jsonb) RETURNS jsonb LANGUAGE plpgsql AS $$
 DECLARE n int;ids jsonb;v numeric;night_start int:=18;night_end int:=8;
 BEGIN
 WITH r AS (SELECT '00000000-0000-0000-0000-000000000001'::uuid obs_id,2::numeric value,'2026-11-01 12:00:00+00'::timestamptz effective_at,'2026-11-01 12:00:00'::timestamp local_ts)
    SELECT count(*)::int,
      COALESCE(jsonb_agg(r.obs_id ORDER BY r.effective_at), '[]'::jsonb),
      CASE e->>'agg'
        WHEN 'count' THEN count(*)::numeric
        WHEN 'sum' THEN COALESCE(sum(r.value), 0)
        WHEN 'min' THEN min(r.value)
        WHEN 'max' THEN max(r.value)
        WHEN 'avg' THEN avg(r.value)
        WHEN 'latest' THEN (array_agg(r.value ORDER BY r.effective_at DESC))[1]
        WHEN 'first' THEN (array_agg(r.value ORDER BY r.effective_at ASC))[1]
        WHEN 'distinct_days' THEN count(DISTINCT r.local_ts::date)::numeric
        WHEN 'distinct_nights' THEN count(DISTINCT CASE
            WHEN extract(hour FROM r.local_ts) >= night_start THEN r.local_ts::date
            WHEN extract(hour FROM r.local_ts) < night_end THEN (r.local_ts - interval '1 day')::date END)::numeric
        WHEN 'slope_per_day' THEN CASE WHEN count(*) >= 2
            THEN regr_slope(r.value, extract(epoch FROM r.effective_at) / 86400.0)::numeric END
      END
    INTO n, ids, v
    FROM r;
 RETURN jsonb_build_object('n',n,'ids',ids,'v',v);END $$"#).unwrap();
    assert_eq!(sql.execute("SELECT (aggregate_into('{\"agg\":\"avg\"}'::jsonb)->>'n')::int,(aggregate_into('{\"agg\":\"latest\"}'::jsonb)->>'v')::int").unwrap().rows,vec![vec![SqlValue::Int(1),SqlValue::Int(2)]]);
}

#[test]
fn comments_and_dollar_literals_do_not_split_or_assign_statements() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        r#"CREATE FUNCTION lexical_boundaries() RETURNS text LANGUAGE plpgsql AS $body$
 DECLARE first text; second int;
 BEGIN
 SELECT $value$literal; := ( ' INTO FROM$value$,
 /* patient's ; := ( /* nested ; ' */ still comment */ 7
 INTO first,second;
 IF second<>7 THEN RAISE EXCEPTION 'second assignment lost';END IF;
 RETURN first;
 END $body$"#,
    )
    .unwrap();
    assert_eq!(
        sql.execute("SELECT lexical_boundaries()").unwrap().rows,
        vec![vec![SqlValue::String("literal; := ( ' INTO FROM".into())]]
    );
}

#[test]
fn dollar_characters_in_identifiers_are_not_literal_delimiters() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE FUNCTION dollar_identifiers() RETURNS int LANGUAGE plpgsql AS $$DECLARE with$dollar$name int:=7;BEGIN RETURN with$dollar$name;END $$").unwrap();
    assert_eq!(
        sql.execute("SELECT dollar_identifiers()").unwrap().rows,
        vec![vec![SqlValue::Int(7)]]
    );
}
