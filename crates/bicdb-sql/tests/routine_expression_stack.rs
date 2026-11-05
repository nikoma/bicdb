use bicdb_core::BicDb;
use bicdb_sql::SqlSession;

#[test]
fn nested_identity_helpers_fit_an_ordinary_protocol_worker_stack() {
    std::thread::Builder::new()
        .name("ehr-routine-expression-stack".into())
        .stack_size(2 * 1024 * 1024)
        .spawn(|| {
            let dir = tempfile::tempdir().unwrap();
            let mut db = BicDb::open(dir.path()).unwrap();
            let mut session = SqlSession::new(&mut db);
            session
                .execute(
                    r#"
                    CREATE SCHEMA ehr_test;
                    CREATE FUNCTION ehr_test.claims() RETURNS jsonb LANGUAGE sql STABLE
                    AS $$ SELECT COALESCE(current_setting('ehr_test.claims',true),'{}')::jsonb $$;
                    CREATE FUNCTION ehr_test.roles() RETURNS jsonb LANGUAGE sql STABLE
                    AS $$ SELECT COALESCE(ehr_test.claims()->'roles','[]') $$;
                    CREATE FUNCTION ehr_test.self_profile() RETURNS uuid LANGUAGE plpgsql STABLE SECURITY DEFINER
                    AS $$ DECLARE claims jsonb:=ehr_test.claims(); BEGIN
                      IF NOT (ehr_test.roles() ? 'patient') THEN RETURN NULL; END IF;
                      RETURN (claims->>'patient')::uuid;
                    END $$;
                    CREATE FUNCTION ehr_test.scope(p_patient uuid) RETURNS text LANGUAGE plpgsql STABLE SECURITY DEFINER
                    AS $$ DECLARE tenant text:=COALESCE(ehr_test.claims()->>'tenant',''); BEGIN
                      IF p_patient IS NULL OR tenant='' OR NOT (ehr_test.roles() ? 'patient')
                         OR ehr_test.self_profile() IS DISTINCT FROM p_patient THEN
                        RAISE EXCEPTION 'patient access denied' USING ERRCODE='42501';
                      END IF;
                      RETURN tenant;
                    END $$;
                    CREATE FUNCTION ehr_test.invite(p_patient uuid,p_consent boolean) RETURNS uuid LANGUAGE plpgsql SECURITY DEFINER
                    AS $$ DECLARE tenant text:=ehr_test.scope(p_patient); BEGIN
                      IF p_consent IS DISTINCT FROM true OR NOT
                         (ehr_test.roles() ? 'patient' AND ehr_test.self_profile()=p_patient) THEN
                        RAISE EXCEPTION 'explicit consent required' USING ERRCODE='42501';
                      END IF;
                      RETURN p_patient;
                    END $$;
                    CREATE FUNCTION ehr_test.invite_result(p_patient uuid,p_consent boolean) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER
                    AS $$ BEGIN
                      RETURN jsonb_build_object('record_id',ehr_test.invite(p_patient,p_consent));
                      EXCEPTION WHEN insufficient_privilege THEN RETURN jsonb_build_object('error',SQLERRM);
                    END $$;
                    SET "ehr_test.claims"='{"roles":["patient"],"tenant":"alpha","patient":"00000000-0000-4000-8000-000000000001"}';
                    "#,
                )
                .unwrap();
            for _ in 0..4 {
                let result = session.execute("SELECT (r->>'record_id')::uuid FROM (SELECT ehr_test.invite_result('00000000-0000-4000-8000-000000000001'::uuid,true) AS r) q").unwrap();
                assert_eq!(result.rows[0][0].to_cell(), "00000000-0000-4000-8000-000000000001");
            }
            let denied = session.execute("SELECT r->>'error' FROM (SELECT ehr_test.invite_result('00000000-0000-4000-8000-000000000001'::uuid,false) AS r) q").unwrap();
            assert_eq!(denied.rows[0][0].to_cell(), "explicit consent required");
            session.execute("SET \"ehr_test.claims\"='{\"roles\":[\"patient\"],\"tenant\":\"alpha\",\"patient\":\"00000000-0000-4000-8000-000000000002\"}'").unwrap();
            let denied = session.execute("SELECT r->>'error' FROM (SELECT ehr_test.invite_result('00000000-0000-4000-8000-000000000001'::uuid,true) AS r) q").unwrap();
            assert_eq!(denied.rows[0][0].to_cell(), "patient access denied");
        })
        .unwrap()
        .join()
        .unwrap();
}
