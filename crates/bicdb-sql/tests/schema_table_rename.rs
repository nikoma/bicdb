//! RENAME preserves a relation's namespace, including quoted identifiers and
//! a same-named relation in public. PostgreSQL and Oracle checks stay separate.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{ensure_primary_key_indexes, oracle, SqlResult, SqlSession, SqlValue};

fn execute(db: &mut BicDb, source: &str, oracle_profile: bool) -> bicdb_sql::Result<SqlResult> {
    let lowered = if oracle_profile {
        oracle::lower_session_sql(db, source, oracle::ORACLE_PREVIEW_PROFILE, None, "bicdb")?
    } else {
        source.to_string()
    };
    SqlSession::new(db).execute(&lowered)
}

fn text_rows(db: &mut BicDb, source: &str, oracle_profile: bool) -> Vec<Vec<String>> {
    execute(db, source, oracle_profile)
        .unwrap()
        .rows
        .into_iter()
        .map(|row| row.into_iter().map(|value| value.to_cell()).collect())
        .collect()
}

fn namespace_case(mode: StorageMode, oracle_profile: bool) {
    let root = tempfile::tempdir().unwrap();
    let config = DbConfig::default().with_storage_mode(mode);
    let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
    if oracle_profile {
        oracle::ensure_database_profile(&mut db, oracle::ORACLE_PREVIEW_PROFILE).unwrap();
    }
    let text_type = if oracle_profile {
        "VARCHAR2(32)"
    } else {
        "TEXT"
    };
    for statement in [
        "CREATE SCHEMA \"RenameSpace\"".to_string(),
        "CREATE SCHEMA \"OtherSpace\"".to_string(),
        format!("CREATE TABLE public.\"Target\"(\"rowkey\" INT PRIMARY KEY,\"payload\" {text_type})"),
        format!("CREATE TABLE \"RenameSpace\".\"Source\"(\"rowkey\" INT PRIMARY KEY,\"payload\" {text_type})"),
        "INSERT INTO public.\"Target\" VALUES(99,'outside')".to_string(),
        "INSERT INTO \"RenameSpace\".\"Source\" VALUES(1,'kept')".to_string(),
        "ALTER TABLE \"RenameSpace\".\"Source\" RENAME TO \"Target\"".to_string(),
    ] {
        execute(&mut db, &statement, oracle_profile).unwrap();
    }
    assert_eq!(
        text_rows(
            &mut db,
            "SELECT \"rowkey\",\"payload\" FROM \"RenameSpace\".\"Target\"",
            oracle_profile,
        ),
        vec![vec!["1", "kept"]]
    );
    assert_eq!(
        text_rows(
            &mut db,
            "SELECT \"rowkey\",\"payload\" FROM public.\"Target\"",
            oracle_profile,
        ),
        vec![vec!["99", "outside"]]
    );
    assert!(execute(
        &mut db,
        "SELECT * FROM \"OtherSpace\".\"Target\"",
        oracle_profile,
    )
    .is_err());
    execute(
        &mut db,
        "ALTER TABLE \"RenameSpace\".\"Target\" RENAME TO \"Dot.Target\"",
        oracle_profile,
    )
    .unwrap();
    drop(db);
    let mut reopened = BicDb::open_with_config(root.path(), config).unwrap();
    assert_eq!(
        text_rows(
            &mut reopened,
            "SELECT \"rowkey\",\"payload\" FROM \"RenameSpace\".\"Dot.Target\"",
            oracle_profile,
        ),
        vec![vec!["1", "kept"]]
    );
    assert_eq!(
        text_rows(
            &mut reopened,
            "SELECT \"rowkey\",\"payload\" FROM public.\"Target\"",
            oracle_profile,
        ),
        vec![vec!["99", "outside"]]
    );
}

#[test]
fn postgres_rename_preserves_schema_and_quoted_names_with_public_collision() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        namespace_case(mode, false);
    }
}

#[test]
fn oracle_rename_preserves_schema_and_quoted_names_with_public_collision() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        namespace_case(mode, true);
    }
}

#[test]
fn postgres_rename_checks_type_collision_in_the_owning_schema() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db =
            BicDb::open_with_config(root.path(), DbConfig::default().with_storage_mode(mode))
                .unwrap();
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE SCHEMA rename_types; CREATE TABLE rename_types.source(rowkey INT PRIMARY KEY); INSERT INTO rename_types.source VALUES(1); CREATE TYPE rename_types.taken AS (value INT)").unwrap();
        assert_eq!(
            sql.execute("ALTER TABLE rename_types.source RENAME TO taken")
                .unwrap_err()
                .sqlstate(),
            "42710"
        );
        assert_eq!(
            sql.execute("SELECT rowkey FROM rename_types.source")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "1"
        );
        assert_eq!(
            sql.execute("ALTER TABLE rename_types.source RENAME TO public.escaped")
                .unwrap_err()
                .sqlstate(),
            "42601"
        );
    }
}

#[test]
fn postgres_parent_rename_keeps_partition_links_after_name_reuse_and_reopen() {
    let query = "SELECT ns.nspname, parent.relname, child.relname
                 FROM pg_catalog.pg_inherits i
                 JOIN pg_catalog.pg_class parent ON parent.oid = i.inhparent
                 JOIN pg_catalog.pg_namespace ns ON ns.oid = parent.relnamespace
                 JOIN pg_catalog.pg_class child ON child.oid = i.inhrelid
                 WHERE child.relname IN ('part_child', 'shadow_child')
                 ORDER BY child.relname";
    let expected_links = |parent: &str| {
        vec![
            vec![
                SqlValue::String("rename_part".into()),
                SqlValue::String(parent.into()),
                SqlValue::String("part_child".into()),
            ],
            vec![
                SqlValue::String("public".into()),
                SqlValue::String("part_parent".into()),
                SqlValue::String("shadow_child".into()),
            ],
        ]
    };
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let config = DbConfig::default().with_storage_mode(mode);
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        {
            let mut sql = SqlSession::new(&mut db);
            for statement in [
                "CREATE SCHEMA rename_part",
                "CREATE TABLE rename_part.part_parent(id INT) PARTITION BY LIST (id)",
                "CREATE TABLE rename_part.replacement(id INT) PARTITION BY LIST (id)",
                "CREATE TABLE public.part_parent(id INT) PARTITION BY LIST (id)",
                "CREATE TABLE public.part_child PARTITION OF rename_part.part_parent FOR VALUES IN (1)",
                "CREATE TABLE public.shadow_child PARTITION OF public.part_parent FOR VALUES IN (1)",
            ] {
                sql.execute(statement).unwrap();
            }
            assert_eq!(
                sql.execute(query).unwrap().rows,
                expected_links("part_parent")
            );
            sql.execute("ALTER TABLE rename_part.part_parent RENAME TO renamed")
                .unwrap();
            assert_eq!(sql.execute(query).unwrap().rows, expected_links("renamed"));
            sql.execute("ALTER TABLE rename_part.replacement RENAME TO part_parent")
                .unwrap();
        }
        drop(db);
        let mut db = BicDb::open_with_config(root.path(), config).unwrap();
        let mut sql = SqlSession::new(&mut db);
        assert_eq!(sql.execute(query).unwrap().rows, expected_links("renamed"));
    }
}

#[test]
fn postgres_parent_rename_with_attached_partitions_rolls_back_with_its_transaction() {
    let query = "SELECT parent.relname, child.relname
                 FROM pg_catalog.pg_inherits i
                 JOIN pg_catalog.pg_class parent ON parent.oid = i.inhparent
                 JOIN pg_catalog.pg_class child ON child.oid = i.inhrelid
                 ORDER BY child.relname";
    let links = |parent: &str| {
        ["child_one", "child_two"]
            .into_iter()
            .map(|child| {
                vec![
                    SqlValue::String(parent.into()),
                    SqlValue::String(child.into()),
                ]
            })
            .collect::<Vec<_>>()
    };
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let config = DbConfig::default().with_storage_mode(mode);
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        {
            let mut sql = SqlSession::new(&mut db);
            for statement in [
                "CREATE TABLE tree_parent(id INT) PARTITION BY LIST (id)",
                "CREATE TABLE child_one PARTITION OF tree_parent FOR VALUES IN (1)",
                "CREATE TABLE child_two PARTITION OF tree_parent FOR VALUES IN (2)",
                "INSERT INTO tree_parent VALUES (1), (2)",
            ] {
                sql.execute(statement).unwrap();
            }
            sql.execute("BEGIN").unwrap();
            sql.execute("ALTER TABLE tree_parent RENAME TO tree_renamed")
                .unwrap();
            assert_eq!(sql.execute(query).unwrap().rows, links("tree_renamed"));
            sql.execute("ROLLBACK").unwrap();
            assert_eq!(sql.execute(query).unwrap().rows, links("tree_parent"));
            assert_eq!(
                sql.execute("SELECT count(*) FROM tree_parent")
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Int(2)]]
            );
            sql.execute("BEGIN").unwrap();
            sql.execute("ALTER TABLE tree_parent RENAME TO tree_renamed")
                .unwrap();
            sql.execute("COMMIT").unwrap();
        }
        drop(db);
        let mut db = BicDb::open_with_config(root.path(), config).unwrap();
        let mut sql = SqlSession::new(&mut db);
        assert_eq!(sql.execute(query).unwrap().rows, links("tree_renamed"));
        assert_eq!(
            sql.execute("SELECT count(*) FROM tree_renamed")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(2)]]
        );
    }
}

#[test]
fn postgres_reuses_renamed_table_name_without_reusing_the_old_primary_index() {
    let indexes = "SELECT tablename, indexname FROM pg_catalog.pg_indexes
                   WHERE schemaname = 'rename_pk' ORDER BY tablename";
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let config = DbConfig::default().with_storage_mode(mode);
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        {
            let mut sql = SqlSession::new(&mut db);
            for statement in [
                "CREATE SCHEMA rename_pk",
                "CREATE TABLE rename_pk.source(rowkey INT PRIMARY KEY)",
                "INSERT INTO rename_pk.source VALUES (1)",
                "ALTER TABLE rename_pk.source RENAME TO archived",
                "CREATE TABLE rename_pk.source(rowkey INT, PRIMARY KEY (rowkey))",
                "INSERT INTO rename_pk.source VALUES (1), (2)",
                "CREATE TABLE inferred_source(id INT)",
                "ALTER TABLE inferred_source RENAME TO inferred_archived",
                "CREATE TABLE inferred_source(id INT)",
            ] {
                sql.execute(statement).unwrap();
            }
        }
        drop(db);
        let mut db = BicDb::open_with_config(root.path(), config).unwrap();
        ensure_primary_key_indexes(&mut db).unwrap();
        assert_eq!(
            text_rows(&mut db, indexes, false),
            vec![
                vec!["archived", "source_pkey"],
                vec!["source", "source_pkey1"]
            ]
        );
        let mut sql = SqlSession::new(&mut db);
        assert_eq!(
            sql.execute("SELECT rowkey FROM rename_pk.archived")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(1)]]
        );
        assert_eq!(
            sql.execute("INSERT INTO rename_pk.source VALUES (2)")
                .unwrap_err()
                .sqlstate(),
            "23505"
        );
        sql.execute(
            "ALTER TABLE inferred_source ADD CONSTRAINT inferred_restored_pk PRIMARY KEY (id)",
        )
        .unwrap();
        drop(sql);
        ensure_primary_key_indexes(&mut db).unwrap();
        assert_eq!(
            text_rows(
                &mut db,
                "SELECT indexname FROM pg_catalog.pg_indexes WHERE tablename = 'inferred_source'",
                false,
            ),
            vec![vec!["inferred_restored_pk"]]
        );
        let mut sql = SqlSession::new(&mut db);
        sql.execute("ALTER TABLE inferred_source DROP CONSTRAINT inferred_restored_pk")
            .unwrap();
        sql.execute("ALTER TABLE inferred_source ADD PRIMARY KEY (id)")
            .unwrap();
        drop(sql);
        ensure_primary_key_indexes(&mut db).unwrap();
        assert_eq!(
            text_rows(
                &mut db,
                "SELECT indexname FROM pg_catalog.pg_indexes WHERE tablename = 'inferred_source'",
                false,
            ),
            vec![vec!["inferred_source_pkey1"]]
        );
    }
}
