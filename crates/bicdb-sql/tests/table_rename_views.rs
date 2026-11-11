//! Stored views retain source identity across rename, rollback and reopen.
//! Native PG16 controls for these query scopes are retained independently.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

const CASES: &[(&str,&str)] = &[
    ("implicit", "SELECT source.id,source.payload FROM source"),
    ("qualified", "SELECT public.source.id,public.source.payload FROM public.source"),
    ("explicit_alias", "SELECT s.id,s.payload FROM source s"),
    ("cte_shadow", "WITH source AS (SELECT 99 AS id,'cte' AS payload) SELECT id,payload FROM source"),
    ("cte_self", "WITH source AS (SELECT id,payload FROM source) SELECT id,payload FROM source"),
    ("nested_shadow", "SELECT source.id,(WITH source AS (SELECT 99 AS id,'cte' AS payload) SELECT payload FROM source) AS nested FROM source"),
    ("qualified_correlated_shadow", "SELECT source.id,(SELECT public.source.payload FROM other source WHERE source.id=public.source.id) AS seen FROM public.source"),
    ("column_before_row_alias", "SELECT source AS result FROM source"),
    ("qualified_star", "SELECT source.* FROM source"),
    ("function_star", "SELECT count(source.*) FROM source"),
    ("union", "SELECT id,payload FROM source UNION ALL SELECT id,payload FROM source WHERE id>1"),
    ("literal", "SELECT 'source' AS literal,source.payload FROM source"),
    ("table_query", "TABLE public.source"),
];

#[test]
fn qualified_outer_reference_does_not_bind_a_shadowing_inner_alias() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db =
            BicDb::open_with_config(root.path(), DbConfig::default().with_storage_mode(mode))
                .unwrap();
        bicdb_sql::initialize_rename_journal_identities(&mut db).unwrap();
        let mut session = SqlSession::new(&mut db);
        session.execute("CREATE TABLE source(id INT PRIMARY KEY,payload TEXT); INSERT INTO source VALUES(1,'kept'),(2,'second'); CREATE TABLE other(id INT,payload TEXT); INSERT INTO other VALUES(1,'wrong'),(2,'also wrong')").unwrap();
        for query in [
            "SELECT source.id,(SELECT public.source.payload FROM other source WHERE source.id=public.source.id) FROM public.source ORDER BY source.id",
            "SELECT source.id,(WITH source AS (SELECT id,payload FROM other) SELECT public.source.payload FROM source WHERE source.id=public.source.id) FROM public.source ORDER BY source.id",
        ] {
            assert_eq!(session.execute(query).unwrap().rows, vec![vec![SqlValue::Int(1),SqlValue::String("kept".into())],vec![SqlValue::Int(2),SqlValue::String("second".into())]], "{query}");
        }
    }
}

#[test]
fn view_query_scopes_keep_their_rows_after_rename_and_reopen() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for (case, query) in CASES {
            let root = tempfile::tempdir().unwrap();
            let config = DbConfig::default().with_storage_mode(mode.clone());
            let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            bicdb_sql::initialize_rename_journal_identities(&mut db).unwrap();
            let expected = {
                let mut session = SqlSession::new(&mut db);
                for statement in [
                    "CREATE TABLE source(id INT PRIMARY KEY,payload TEXT,source INT)",
                    "INSERT INTO source VALUES(1,'kept',7)",
                    "CREATE TABLE other(id INT,payload TEXT)",
                    "INSERT INTO other VALUES(1,'other')",
                ] {
                    session.execute(statement).unwrap();
                }
                session
                    .execute(&format!("CREATE VIEW observed AS {query}"))
                    .unwrap_or_else(|e| panic!("{mode} {case} CREATE: {e}"));
                let expected = session.execute("SELECT * FROM observed").unwrap().rows;
                if *case == "qualified_correlated_shadow" {
                    // Native PG16 independently returns this row on both sides
                    // of the rename. Do not freeze an erroneous pre-rename
                    // binding to `other AS source` as the expected result.
                    assert_eq!(
                        expected,
                        vec![vec![SqlValue::Int(1), SqlValue::String("kept".into())]],
                        "{mode} before rename"
                    );
                }
                session
                    .execute("ALTER TABLE source RENAME TO renamed")
                    .unwrap();
                assert_eq!(
                    session
                        .execute("SELECT * FROM observed")
                        .unwrap_or_else(|e| panic!("{mode} {case} after rename: {e}"))
                        .rows,
                    expected,
                    "{mode} {case}"
                );
                expected
            };
            db.close().unwrap();
            let mut db = BicDb::open_with_config(root.path(), config).unwrap();
            assert_eq!(
                SqlSession::new(&mut db)
                    .execute("SELECT * FROM observed")
                    .unwrap()
                    .rows,
                expected,
                "{mode} {case} reopen"
            );
            db.close().unwrap();
        }
    }
}

#[test]
fn schema_and_quoted_names_are_distinct_source_identities() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db =
            BicDb::open_with_config(root.path(), DbConfig::default().with_storage_mode(mode))
                .unwrap();
        bicdb_sql::initialize_rename_journal_identities(&mut db).unwrap();
        let mut session = SqlSession::new(&mut db);
        for statement in ["CREATE SCHEMA isolated","CREATE TABLE public.source(id INT)","INSERT INTO public.source VALUES(99)","CREATE TABLE isolated.source(id INT)","INSERT INTO isolated.source VALUES(1)","CREATE VIEW isolated.observed AS SELECT a.id AS local_id,b.id AS public_id FROM isolated.source a CROSS JOIN public.source b"] {session.execute(statement).unwrap();}
        let expected = session
            .execute("SELECT * FROM isolated.observed")
            .unwrap()
            .rows;
        session
            .execute("ALTER TABLE isolated.source RENAME TO target")
            .unwrap();
        assert_eq!(
            session
                .execute("SELECT * FROM isolated.observed")
                .unwrap()
                .rows,
            expected
        );
        for statement in [
            r#"CREATE TABLE "SourceCase"("ValueCase" INT)"#,
            r#"INSERT INTO "SourceCase" VALUES(7)"#,
            r#"CREATE VIEW "CaseView" AS SELECT "SourceCase"."ValueCase" FROM "SourceCase""#,
        ] {
            session.execute(statement).unwrap();
        }
        let expected = session.execute(r#"SELECT * FROM "CaseView""#).unwrap().rows;
        session
            .execute(r#"ALTER TABLE "SourceCase" RENAME TO "TargetCase""#)
            .unwrap();
        assert_eq!(
            session.execute(r#"SELECT * FROM "CaseView""#).unwrap().rows,
            expected
        );
    }
}

#[test]
fn view_rebinding_rolls_back_with_the_rename_savepoint() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db =
            BicDb::open_with_config(root.path(), DbConfig::default().with_storage_mode(mode))
                .unwrap();
        bicdb_sql::initialize_rename_journal_identities(&mut db).unwrap();
        let mut session = SqlSession::new(&mut db);
        session.execute("CREATE TABLE source(id INT PRIMARY KEY); INSERT INTO source VALUES(1); CREATE VIEW observed AS SELECT source.id FROM source").unwrap();
        let expected = session.execute("SELECT * FROM observed").unwrap().rows;
        session
            .execute("BEGIN; SAVEPOINT before_rename; ALTER TABLE source RENAME TO renamed")
            .unwrap();
        assert_eq!(
            session.execute("SELECT * FROM observed").unwrap().rows,
            expected
        );
        session.execute("ROLLBACK TO before_rename").unwrap();
        assert_eq!(
            session.execute("SELECT * FROM observed").unwrap().rows,
            expected
        );
        session
            .execute("INSERT INTO source VALUES(2); COMMIT")
            .unwrap();
        assert_eq!(
            session
                .execute("SELECT * FROM observed")
                .unwrap()
                .rows
                .len(),
            2
        );
    }
}

#[test]
fn direct_and_indirect_views_keep_row_type_identity_after_rename_and_reopen() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let config = DbConfig::default().with_storage_mode(mode);
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        bicdb_sql::initialize_rename_journal_identities(&mut db).unwrap();
        {
            let mut session = SqlSession::new(&mut db);
            session.execute("CREATE TABLE source(id INT PRIMARY KEY,payload TEXT); INSERT INTO source VALUES(1,'acknowledged'); CREATE VIEW direct_row AS SELECT source AS item FROM source; CREATE VIEW indirect_row AS SELECT item FROM direct_row; CREATE MATERIALIZED VIEW stored_row AS SELECT item FROM direct_row").unwrap();
            for view in ["direct_row", "indirect_row", "stored_row"] {
                assert_eq!(
                    session
                        .execute(&format!(
                            "SELECT (item).payload,pg_typeof(item) FROM {view}"
                        ))
                        .unwrap()
                        .rows,
                    vec![vec![
                        SqlValue::String("acknowledged".into()),
                        SqlValue::String("source".into())
                    ]]
                );
            }
            session
                .execute("ALTER TABLE source RENAME TO renamed")
                .unwrap();
        }
        for reopen in [false, true] {
            if reopen {
                db.close().unwrap();
                db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            }
            let mut session = SqlSession::new(&mut db);
            for view in ["direct_row", "indirect_row", "stored_row"] {
                assert_eq!(
                    session
                        .execute(&format!(
                            "SELECT (item).payload,pg_typeof(item) FROM {view}"
                        ))
                        .unwrap()
                        .rows,
                    vec![vec![
                        SqlValue::String("acknowledged".into()),
                        SqlValue::String("renamed".into())
                    ]],
                    "{view} reopen={reopen}"
                );
            }
        }
        db.close().unwrap();
    }
}
