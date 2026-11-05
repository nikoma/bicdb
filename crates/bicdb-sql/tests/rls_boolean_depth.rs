use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn deeply_nested_role_policies_preserve_tenant_scope_and_null_semantics() {
    std::thread::Builder::new()
        .name("ehr-policy-depth".into())
        .stack_size(2 * 1024 * 1024)
        .spawn(|| {
            let dir = tempfile::Builder::new().prefix("bicdb-ehr-policy-depth-").tempdir().unwrap();
            let mut db = BicDb::open(dir.path()).unwrap();
            let mut session = SqlSession::new(&mut db);
            session.execute("CREATE ROLE clinic_reader; CREATE TABLE programs(id int PRIMARY KEY,org_id text,published boolean,denominator int); INSERT INTO programs VALUES(1,'alpha',false,0),(2,'beta',false,0),(3,'beta',true,0),(4,NULL,false,0),(5,'alpha',NULL,0); ALTER TABLE programs ENABLE ROW LEVEL SECURITY; GRANT SELECT ON programs TO clinic_reader").unwrap();
            let roles = (0..40).map(|i| format!("current_setting('ehr_test.role',true)='other_{i}'")).chain(std::iter::once("current_setting('ehr_test.role',true)='reader'".into())).collect::<Vec<_>>().join(" OR ");
            session.execute(&format!("CREATE POLICY program_scope ON programs FOR SELECT USING ((org_id=current_setting('ehr_test.tenant',true) AND ({roles})) OR published)")).unwrap();
            session.execute("SET ROLE clinic_reader; SET \"ehr_test.role\"='reader'; SET \"ehr_test.tenant\"='alpha'").unwrap();
            let ids = |session: &mut SqlSession<'_>, query: &str| {
                session.execute(query).unwrap().rows.into_iter().map(|row| match row[0] { SqlValue::Int(id)=>id,_=>panic!("integer id required") }).collect::<Vec<_>>()
            };
            assert_eq!(ids(&mut session,"SELECT id FROM programs ORDER BY id"),vec![1,3,5]);
            session.execute("SET \"ehr_test.tenant\"='beta'").unwrap();
            assert_eq!(ids(&mut session,"SELECT id FROM programs ORDER BY id"),vec![2,3]);
            session.execute("SET \"ehr_test.tenant\"='alpha'; SET \"ehr_test.role\"='outsider'").unwrap();
            assert_eq!(ids(&mut session,"SELECT id FROM programs ORDER BY id"),vec![3]);
            session.execute("RESET \"ehr_test.role\"").unwrap();
            assert_eq!(ids(&mut session,"SELECT id FROM programs ORDER BY id"),vec![3]);
            session.execute("SET \"ehr_test.role\"='reader'; RESET \"ehr_test.tenant\"").unwrap();
            assert_eq!(ids(&mut session,"SELECT id FROM programs ORDER BY id"),vec![3]);
            session.execute("SET \"ehr_test.tenant\"='alpha'").unwrap();
            assert_eq!(ids(&mut session,"SELECT id FROM programs WHERE id=3 AND (published OR 1/denominator>0)"),vec![3]);
            assert!(ids(&mut session,"SELECT id FROM programs WHERE id=1 AND (published AND 1/denominator>0)").is_empty());
            assert_eq!(ids(&mut session,"SELECT id FROM programs WHERE NOT published ORDER BY id"),vec![1]);
            assert_eq!(ids(&mut session,"SELECT id FROM programs WHERE NULL OR published ORDER BY id"),vec![3]);
            assert!(ids(&mut session,"SELECT id FROM programs WHERE NULL AND published").is_empty());
            assert_eq!(ids(&mut session,"SELECT id FROM programs WHERE NOT NOT published"),vec![3]);
        })
        .unwrap()
        .join()
        .unwrap();
}
