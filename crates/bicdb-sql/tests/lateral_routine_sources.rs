use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn lateral_sql_routines_bind_each_left_row_and_preserve_qualified_output_types() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE SCHEMA delivery; CREATE TABLE reminder_inputs(id int PRIMARY KEY,tenant text); INSERT INTO reminder_inputs VALUES(1,'north'),(2,'south'),(3,'missing'); CREATE TABLE delivery_values(tenant text PRIMARY KEY,value int); INSERT INTO delivery_values VALUES('north',10),('south',20); CREATE FUNCTION delivery.config(p_tenant text) RETURNS TABLE(payload_json jsonb,feature_flags_json jsonb) LANGUAGE sql STABLE SECURITY DEFINER AS $$ SELECT jsonb_build_object('value',v.value),jsonb_build_object('enabled',true) FROM delivery_values v WHERE v.tenant=p_tenant LIMIT 1 $$").unwrap();
    let result=sql.execute("WITH email_base AS (SELECT r.id,tc.payload_json FROM reminder_inputs r LEFT JOIN LATERAL delivery.config(r.tenant) tc ON true) SELECT id,COALESCE(payload_json,'{}'::jsonb) AS payload FROM email_base ORDER BY id").unwrap();
    assert_eq!(
        result.rows,
        vec![
            vec![
                SqlValue::Int(1),
                SqlValue::json(serde_json::json!({"value":10}))
            ],
            vec![
                SqlValue::Int(2),
                SqlValue::json(serde_json::json!({"value":20}))
            ],
            vec![SqlValue::Int(3), SqlValue::json(serde_json::json!({}))]
        ]
    );
    assert_eq!(
        result.column_types,
        vec![Some("int4".into()), Some("jsonb".into())]
    );
    let explicit=sql.execute("SELECT r.id,c.payload FROM reminder_inputs r CROSS JOIN LATERAL delivery.config(r.tenant) AS c(payload,flags) ORDER BY r.id").unwrap();
    let implicit=sql.execute("SELECT r.id,c.payload FROM reminder_inputs r, delivery.config(r.tenant) AS c(payload,flags) ORDER BY r.id").unwrap();
    assert_eq!(explicit.rows, result.rows[..2]);
    assert_eq!(implicit.rows, explicit.rows);
    assert_eq!(sql.execute("SELECT r.id FROM reminder_inputs r LEFT JOIN LATERAL delivery.config(r.tenant) c ON (c.payload_json->>'value')::int>10 ORDER BY r.id").unwrap().rows,vec![vec![SqlValue::Int(1)],vec![SqlValue::Int(2)],vec![SqlValue::Int(3)]]);
    assert_eq!(
        sql.execute("SELECT payload_json FROM LATERAL delivery.config('north')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::json(serde_json::json!({"value":10}))]]
    );
}

#[test]
fn lateral_sql_routines_keep_definer_identity_and_execute_refusals_for_empty_sources() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE SCHEMA delivery; CREATE TABLE public_inputs(id int PRIMARY KEY,tenant text); INSERT INTO public_inputs VALUES(1,'north'); CREATE TABLE empty_inputs(id int PRIMARY KEY,tenant text); CREATE TABLE protected_settings(tenant text PRIMARY KEY,value text); INSERT INTO protected_settings VALUES('north','bounded projection'); CREATE ROLE config_owner; CREATE ROLE config_reader; GRANT USAGE,CREATE ON SCHEMA delivery TO config_owner; GRANT USAGE ON SCHEMA delivery TO config_reader; GRANT SELECT ON protected_settings TO config_owner; GRANT SELECT ON public_inputs,empty_inputs TO config_reader; SET ROLE config_owner; CREATE FUNCTION delivery.actor(p_tenant text) RETURNS TABLE(setting text,actor text) LANGUAGE sql SECURITY DEFINER AS $$ SELECT value,current_user::text FROM protected_settings WHERE tenant=p_tenant $$; RESET ROLE; REVOKE ALL ON FUNCTION delivery.actor(text) FROM PUBLIC; GRANT EXECUTE ON FUNCTION delivery.actor(text) TO config_reader; SET ROLE config_reader").unwrap();
    assert_eq!(
        sql.execute("SELECT * FROM protected_settings")
            .unwrap_err()
            .sqlstate(),
        "42501"
    );
    assert_eq!(sql.execute("SELECT c.setting,c.actor FROM public_inputs p LEFT JOIN LATERAL delivery.actor(p.tenant) c ON true").unwrap().rows,vec![vec![SqlValue::String("bounded projection".into()),SqlValue::String("config_owner".into())]]);
    assert_eq!(
        sql.execute("SELECT current_user").unwrap().rows,
        vec![vec![SqlValue::String("config_reader".into())]]
    );
    assert_eq!(
        sql.execute("SELECT * FROM protected_settings")
            .unwrap_err()
            .sqlstate(),
        "42501"
    );
    let empty=sql.execute("SELECT p.id,c.setting FROM empty_inputs p LEFT JOIN LATERAL delivery.actor(p.tenant) c ON true").unwrap();
    assert!(empty.rows.is_empty());
    assert_eq!(
        empty.column_types,
        vec![Some("int4".into()), Some("text".into())]
    );
    sql.execute("RESET ROLE; REVOKE EXECUTE ON FUNCTION delivery.actor(text) FROM config_reader; SET ROLE config_reader").unwrap();
    assert_eq!(sql.execute("SELECT c.setting FROM public_inputs p LEFT JOIN LATERAL delivery.actor(p.tenant) c ON true").unwrap_err().sqlstate(),"42501");
    assert_eq!(sql.execute("SELECT c.setting FROM empty_inputs p LEFT JOIN LATERAL delivery.actor(p.tenant) c ON true").unwrap_err().sqlstate(),"42501");
    sql.execute("RESET ROLE").unwrap();
}
