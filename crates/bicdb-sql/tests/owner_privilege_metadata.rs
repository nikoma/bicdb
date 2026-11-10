//! Native PostgreSQL 16 privilege metadata and the original iDempiere routine.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn owner_reader_and_public_grant_visibility_matches_postgres() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/postgres/owner_grants_pg16.json")).unwrap();
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let open = || {
            BicDb::open_with_config(
                root.path(),
                DbConfig::default()
                    .with_fsync(false)
                    .with_storage_mode(mode.clone()),
            )
            .unwrap()
        };
        {
            let mut db = open();
            let mut s = SqlSession::new(&mut db);
            for sql in fixture["setup"].as_array().unwrap() {
                s.execute(sql.as_str().unwrap()).unwrap();
            }
        }
        // Reopen as well: ownership and grants are durable catalog state.
        let mut db = open();
        let mut s = SqlSession::new(&mut db);
        for case in fixture["cases"].as_array().unwrap() {
            s.execute("RESET ROLE").unwrap();
            s.execute(&format!("SET ROLE {}", case["role"].as_str().unwrap()))
                .unwrap();
            let expected = case["rows"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| {
                    r.as_array()
                        .unwrap()
                        .iter()
                        .map(|v| SqlValue::String(v.as_str().unwrap().to_string()))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            assert_eq!(
                s.execute(case["sql"].as_str().unwrap()).unwrap().rows,
                expected,
                "{mode:?}: {case}"
            );
        }
    }
}

#[test]
fn original_altercolumn_recreates_view_and_owner_grants() {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE SCHEMA adempiere; SET search_path=adempiere,public; CREATE TABLE privilege_probe(description varchar(20)); INSERT INTO privilege_probe VALUES('retained'); CREATE VIEW privilege_probe_v AS SELECT description FROM privilege_probe").unwrap();
    // Verbatim upstream source, including its privilege save/restore loop.
    s.execute(include_str!("fixtures/postgres/idempiere_altercolumn.sql"))
        .unwrap();
    s.execute("SELECT altercolumn('privilege_probe'::name,'description'::name,'varchar(40)'::name,NULL::varchar,NULL::varchar)").unwrap();
    assert_eq!(
        s.execute("SELECT description FROM privilege_probe_v")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("retained".into())]]
    );
    assert_eq!(s.execute("SELECT count(*) FROM information_schema.role_table_grants WHERE table_schema='adempiere' AND table_name='privilege_probe_v'").unwrap().rows,vec![vec![SqlValue::Int(7)]]);
}
