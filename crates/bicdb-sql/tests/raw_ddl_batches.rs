use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn revoke_batch_does_not_parse_following_function_signatures_as_grantees() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE ROLE api_client;CREATE ROLE delivery_worker;
        CREATE FUNCTION claim_message(text,integer) RETURNS integer LANGUAGE sql AS $$ SELECT $2 $$;
        CREATE FUNCTION finish_message(uuid,uuid,boolean,jsonb,text,text) RETURNS boolean LANGUAGE sql AS $$ SELECT $3 $$").unwrap();
    sql.execute("REVOKE ALL ON FUNCTION claim_message(text,integer) FROM PUBLIC;
        REVOKE ALL ON FUNCTION finish_message(uuid,uuid,boolean,jsonb,text,text) FROM PUBLIC;
        REVOKE ALL ON FUNCTION claim_message(text,integer) FROM api_client;
        REVOKE ALL ON FUNCTION finish_message(uuid,uuid,boolean,jsonb,text,text) FROM api_client;
        GRANT EXECUTE ON FUNCTION claim_message(text,integer) TO delivery_worker;
        GRANT EXECUTE ON FUNCTION finish_message(uuid,uuid,boolean,jsonb,text,text) TO delivery_worker").unwrap();
    sql.execute("SET ROLE delivery_worker").unwrap();
    assert_eq!(
        sql.execute("SELECT claim_message('worker',60)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(60)]]
    );
    assert_eq!(
        sql.execute("SELECT finish_message(NULL,NULL,true,'{}',NULL,NULL)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]]
    );
    sql.execute("RESET ROLE;SET ROLE api_client").unwrap();
    for query in [
        "SELECT claim_message('worker',60)",
        "SELECT finish_message(NULL,NULL,true,'{}',NULL,NULL)",
    ] {
        assert_eq!(sql.execute(query).unwrap_err().sqlstate(), "42501");
    }
}

#[test]
fn anonymous_do_batch_preserves_body_delimiters_and_executes_following_statements() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE batch_receipts(id int PRIMARY KEY,label text)")
        .unwrap();
    let result = sql
        .execute(
            "-- A migration begins with an anonymous block.
        DO $migration$ BEGIN INSERT INTO batch_receipts VALUES(1,'inside;block'); END $migration$;
        INSERT INTO batch_receipts VALUES(2,'after');
        SELECT id,label FROM batch_receipts ORDER BY id",
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Int(1), SqlValue::String("inside;block".into())],
            vec![SqlValue::Int(2), SqlValue::String("after".into())]
        ]
    );
}

#[test]
fn a_function_row_source_accepts_a_field_from_the_callers_record_variable() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE TABLE delivery_candidates(org_id text PRIMARY KEY);
        INSERT INTO delivery_candidates VALUES('north'),('south');
        CREATE FUNCTION delivery_configuration(p_org text)
        RETURNS TABLE(config jsonb,flags jsonb) LANGUAGE sql AS $$
          SELECT jsonb_build_object('org',p_org),'{}'::jsonb $$;
        CREATE FUNCTION selected_delivery_org(p_org text) RETURNS text LANGUAGE plpgsql AS $$
          DECLARE candidate record; configuration jsonb;
          BEGIN
            SELECT c.* INTO candidate FROM delivery_candidates c WHERE c.org_id=p_org;
            SELECT d.config INTO configuration FROM delivery_configuration(candidate.org_id) d;
            RETURN configuration->>'org';
          END $$",
    )
    .unwrap();
    for org in ["north", "south", "north"] {
        assert_eq!(
            sql.execute(&format!("SELECT selected_delivery_org('{org}')"))
                .unwrap()
                .rows,
            vec![vec![SqlValue::String(org.into())]]
        );
    }
}
