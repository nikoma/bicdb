use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn comma_lateral_table_routines_match_explicit_cross_joins() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE inputs(id int PRIMARY KEY);INSERT INTO inputs VALUES(1),(2);CREATE FUNCTION scaled(n int) RETURNS TABLE(v int) LANGUAGE plpgsql STABLE AS $$ BEGIN RETURN QUERY SELECT n*10;END $$").unwrap();
    let explicit = sql
        .execute("SELECT i.id,r.v FROM inputs i CROSS JOIN LATERAL scaled(i.id) r ORDER BY i.id")
        .unwrap();
    assert_eq!(
        explicit.rows,
        vec![
            vec![SqlValue::Int(1), SqlValue::Int(10)],
            vec![SqlValue::Int(2), SqlValue::Int(20)]
        ]
    );
    assert_eq!(
        sql.execute("SELECT i.id,r.v FROM inputs i,LATERAL scaled(i.id) r ORDER BY i.id")
            .unwrap()
            .rows,
        explicit.rows
    );
    assert_eq!(
        sql.execute("SELECT i.id,r.v FROM inputs i,scaled(i.id) r ORDER BY i.id")
            .unwrap()
            .rows,
        explicit.rows
    );
    assert_eq!(sql.execute("WITH pairs AS (SELECT * FROM inputs) SELECT i.id,r.v FROM pairs i,LATERAL scaled(i.id) r ORDER BY i.id").unwrap().rows,explicit.rows);
}

#[test]
fn nested_comma_lateral_routine_keeps_arguments_and_execute_refusals() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE inputs(id int PRIMARY KEY);INSERT INTO inputs VALUES(1),(2);CREATE TABLE empty_inputs(id int PRIMARY KEY);CREATE FUNCTION scaled(n int) RETURNS TABLE(v int) LANGUAGE plpgsql STABLE AS $$ BEGIN RETURN QUERY SELECT n*10;END $$;CREATE FUNCTION combined(n int) RETURNS int LANGUAGE plpgsql STABLE AS $$ DECLARE answer int;BEGIN SELECT sum(r.v)::int INTO answer FROM jsonb_array_elements_text('[1,2]'::jsonb) item,LATERAL scaled(item::int+n) r;RETURN answer;END $$").unwrap();
    assert_eq!(
        sql.execute("SELECT combined(id) FROM inputs ORDER BY id")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(50)], vec![SqlValue::Int(70)]]
    );
    sql.execute("CREATE ROLE lateral_reader;GRANT SELECT ON inputs,empty_inputs TO lateral_reader;REVOKE ALL ON FUNCTION scaled(int) FROM PUBLIC;SET ROLE lateral_reader").unwrap();
    assert_eq!(
        sql.execute("SELECT i.id FROM empty_inputs i,LATERAL scaled(i.id) r")
            .unwrap_err()
            .sqlstate(),
        "42501"
    );
}
