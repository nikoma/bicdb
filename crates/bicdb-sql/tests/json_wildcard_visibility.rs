use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};
use serde_json::json;
fn json_value(value: serde_json::Value) -> SqlValue {
    SqlValue::Json(value.into())
}

#[test]
fn json_alias_wildcards_expose_only_physical_columns() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    for query in ["SELECT * FROM jsonb_array_elements('[{\"missed\":1},{\"other\":2}]'::jsonb) x WHERE x ? 'missed'","SELECT x.* FROM jsonb_array_elements('[{\"missed\":1}]'::jsonb) x"] {
  let result=sql.execute(query).unwrap();assert_eq!(result.columns,vec!["value"]);assert_eq!(result.column_types,vec![Some("jsonb".to_string())]);assert_eq!(result.rows,vec![vec![json_value(json!({"missed":1}))]]);
 }
    let empty = sql
        .execute("SELECT * FROM jsonb_array_elements('[]'::jsonb) x")
        .unwrap();
    assert_eq!(empty.columns, vec!["value"]);
    assert_eq!(empty.column_types, vec![Some("jsonb".to_string())]);
    assert!(empty.rows.is_empty());
}

#[test]
fn scalar_alias_expressions_keep_their_types_without_extra_wildcard_columns() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    let result=sql.execute("SELECT x,y FROM jsonb_array_elements('[1]'::jsonb) x CROSS JOIN jsonb_array_elements('[2]'::jsonb) y").unwrap();
    assert_eq!(
        result.column_types,
        vec![Some("jsonb".to_string()), Some("jsonb".to_string())]
    );
    assert_eq!(
        result.rows,
        vec![vec![json_value(json!(1)), json_value(json!(2))]]
    );
    let result=sql.execute("SELECT * FROM jsonb_array_elements('[1]'::jsonb) x CROSS JOIN jsonb_array_elements('[2]'::jsonb) y").unwrap();
    assert_eq!(result.columns, vec!["value", "value"]);
    assert_eq!(
        result.rows,
        vec![vec![json_value(json!(1)), json_value(json!(2))]]
    );
}

#[test]
fn explicit_alias_columns_and_ordinality_keep_their_visible_shape() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    let result = sql
        .execute("SELECT * FROM jsonb_array_elements('[1,2]'::jsonb) WITH ORDINALITY x(item,n)")
        .unwrap();
    assert_eq!(result.columns, vec!["item", "n"]);
    assert_eq!(
        result.column_types,
        vec![Some("jsonb".to_string()), Some("int8".to_string())]
    );
    assert_eq!(
        result.rows,
        vec![
            vec![json_value(json!(1)), SqlValue::Int(1)],
            vec![json_value(json!(2)), SqlValue::Int(2)]
        ]
    );
    let result = sql
        .execute("SELECT * FROM jsonb_array_elements('[1]'::jsonb) WITH ORDINALITY x")
        .unwrap();
    assert_eq!(result.columns, vec!["value", "ordinality"]);
    assert_eq!(
        result.rows,
        vec![vec![json_value(json!(1)), SqlValue::Int(1)]]
    );
    let result = sql
        .execute("SELECT * FROM jsonb_each('{\"k\":1}'::jsonb) x")
        .unwrap();
    assert_eq!(result.columns, vec!["key", "value"]);
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::String("k".into()), json_value(json!(1))]]
    );
}

#[test]
fn scalar_routine_loops_can_filter_aliased_json_wildcards() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE FUNCTION wildcard_loop(p jsonb) RETURNS jsonb LANGUAGE plpgsql AS $$DECLARE s jsonb;result jsonb:='[]'::jsonb;BEGIN FOR s IN SELECT * FROM jsonb_array_elements(p) x WHERE x ? 'missed' LOOP result:=result||jsonb_build_array(s);END LOOP;RETURN result;END $$").unwrap();
    let result = sql
        .execute("SELECT wildcard_loop('[{\"missed\":1},{\"other\":2}]'::jsonb)")
        .unwrap();
    assert_eq!(result.rows, vec![vec![json_value(json!([{"missed":1}]))]]);
}
