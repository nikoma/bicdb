use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn json_number(value: &serde_json::Value) -> f64 {
    value
        .as_f64()
        .unwrap_or_else(|| value.as_str().unwrap().parse().unwrap())
}

fn number(value: &SqlValue) -> f64 {
    match value {
        SqlValue::Float(n) => *n,
        other => panic!("expected float8, got {other:?}"),
    }
}

#[test]
fn regression_slope_handles_record_join_and_bound_group_paths() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE readings(id int PRIMARY KEY,side text,y numeric,x double precision);INSERT INTO readings VALUES(1,'before',10,1000000000000),(2,'before',12,1000000000001),(3,'before',14,1000000000002),(4,'after',8,1000000000000),(5,'after',7,1000000000001)").unwrap();
    for query in [
        "SELECT regr_slope(y,x) FROM readings WHERE side='before'",
        "SELECT pg_catalog.regr_slope(a.y,b.x) FROM readings a JOIN readings b ON a.id=b.id WHERE a.side='before'",
        "WITH r AS (SELECT side,y,x FROM readings) SELECT regr_slope(y,x) FROM r WHERE side='before'",
    ] { let r=sql.execute(query).unwrap(); assert_eq!(r.columns,vec!["regr_slope"]);assert_eq!(number(&r.rows[0][0]),2.0,"{query}"); }
    let r=sql.execute("SELECT side,round(regr_slope(y,x)::numeric,4) AS slope FROM readings GROUP BY side ORDER BY side").unwrap();
    assert_eq!(r.rows[0][0], SqlValue::String("after".into()));
    assert_eq!(r.rows[1][0], SqlValue::String("before".into()));
    assert_eq!(r.rows[0][1].to_cell().parse::<f64>().unwrap(), -1.0);
    assert_eq!(r.rows[1][1].to_cell().parse::<f64>().unwrap(), 2.0);
}

#[test]
fn regression_slope_ignores_incomplete_pairs_filters_and_distinct_pairs() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE readings(id int PRIMARY KEY,y double precision,x double precision);INSERT INTO readings VALUES(1,1,1),(2,2,2),(3,4,3),(4,4,3),(5,NULL,100),(6,100,NULL)").unwrap();
    let r=sql.execute("SELECT regr_slope(y,x),regr_slope(DISTINCT y,x),regr_slope(y,x) FILTER(WHERE id<=2) FROM readings").unwrap();
    assert!((number(&r.rows[0][0]) - 17.0 / 11.0).abs() < 1e-12);
    assert_eq!(number(&r.rows[0][1]), 1.5);
    assert_eq!(number(&r.rows[0][2]), 1.0);
    for condition in ["false", "id=1", "id=5", "id IN (1,6)"] {
        let r = sql
            .execute(&format!(
                "SELECT regr_slope(y,x) FROM readings WHERE {condition}"
            ))
            .unwrap();
        assert_eq!(r.rows, vec![vec![SqlValue::Null]]);
    }
    let r = sql
        .execute("SELECT regr_slope(y,1),regr_slope(5,x) FROM readings")
        .unwrap();
    assert_eq!(r.rows[0][0], SqlValue::Null);
    assert_eq!(number(&r.rows[0][1]), 0.0);
}

#[test]
fn intervention_grouped_stats_inside_sql_function_return_both_sides() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE observations(id int PRIMARY KEY,v numeric,t timestamptz);INSERT INTO observations VALUES(1,10,'2026-10-01T00:00:00Z'),(2,12,'2026-10-02T00:00:00Z'),(3,8,'2026-10-04T00:00:00Z'),(4,7,'2026-10-05T00:00:00Z');CREATE FUNCTION intervention(p_at timestamptz) RETURNS jsonb LANGUAGE sql STABLE AS $$ WITH sides AS (SELECT CASE WHEN t<p_at THEN 'before' ELSE 'after' END AS side,* FROM observations), stats AS (SELECT side,count(*) AS n,CASE WHEN count(*)>=2 THEN round(avg(v),3) END AS mean,CASE WHEN count(*)>=2 THEN round(regr_slope(v,extract(epoch FROM t)/86400.0)::numeric,4) END AS slope_per_day,jsonb_agg(jsonb_build_object('id',id,'t',t,'v',v) ORDER BY t) AS readings FROM sides GROUP BY side) SELECT jsonb_build_object('before',(SELECT to_jsonb(s)-'side' FROM stats s WHERE side='before'),'after',(SELECT to_jsonb(s)-'side' FROM stats s WHERE side='after'),'change',(SELECT a.mean-b.mean FROM stats a,stats b WHERE a.side='after' AND b.side='before')) $$").unwrap();
    let r = sql
        .execute("SELECT intervention('2026-10-03T00:00:00Z')")
        .unwrap();
    let value: serde_json::Value = serde_json::from_str(&r.rows[0][0].to_cell()).unwrap();
    assert_eq!(value["before"]["n"], 2);
    assert_eq!(value["after"]["n"], 2);
    assert_eq!(json_number(&value["before"]["slope_per_day"]), 2.0);
    assert_eq!(json_number(&value["after"]["slope_per_day"]), -1.0);
    assert_eq!(json_number(&value["change"]), -3.5);
}

#[test]
fn regression_slope_validates_types_arity_and_nonfinite_values() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE typed_values(id int PRIMARY KEY,label text);INSERT INTO typed_values VALUES(1,'5')").unwrap();
    for query in [
        "SELECT regr_slope(label,id) FROM typed_values",
        "SELECT regr_slope(label,id) FROM typed_values WHERE false",
        "SELECT regr_slope(id) FROM typed_values",
        "SELECT regr_slope(id,id,id) FROM typed_values",
    ] {
        assert!(sql.execute(query).is_err(), "{query}");
    }
    for query in [
        "SELECT regr_slope(y,x) FROM (VALUES('Infinity'::float8,1::float8),(2,2)) r(y,x)",
        "SELECT regr_slope(y,x) FROM (VALUES(1::float8,'Infinity'::float8),(2,2)) r(y,x)",
        "SELECT regr_slope(y,x) FROM (VALUES(1::float8,'Infinity'::float8)) r(y,x)",
        "SELECT regr_slope(y,x) FROM (VALUES('NaN'::float8,1::float8),(2,2)) r(y,x)",
    ] {
        let r = sql.execute(query).unwrap();
        assert!(number(&r.rows[0][0]).is_nan(), "{query}");
    }
    let r = sql
        .execute("SELECT regr_slope(y,x) FROM (VALUES('Infinity'::float8,1::float8)) r(y,x)")
        .unwrap();
    assert_eq!(r.rows, vec![vec![SqlValue::Null]]);
}
