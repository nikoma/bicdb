use bicdb_sql::{parse_database_ddl, DatabaseDdl};

#[test]
fn database_creation_accepts_sql_comments_as_whitespace() {
    for sql in [
        "CREATE DATABASE example ENCODING = 'unicode' /*trace='42'*/",
        "/* lead /* nested */ end */ CREATE/* gap */DATABASE example WITH /* option */ ENCODING 'unicode'; -- tail",
        "CREATE DATABASE example OWNER /* owner */ operator TEMPLATE /* template */ template0 /* tail */;",
    ] {
        let Some(DatabaseDdl::Create { name, .. }) = parse_database_ddl(sql).unwrap() else {
            panic!("creation not recognized: {sql}");
        };
        assert_eq!(name, "example");
    }
}

#[test]
fn database_comments_do_not_change_quoted_identifiers_or_settings() {
    let Some(DatabaseDdl::Create { name, owner, .. }) =
        parse_database_ddl("CREATE DATABASE \"a/*b*/--c\" OWNER \"quoted\"\"owner\" /*tail*/")
            .unwrap()
    else {
        panic!("creation not recognized");
    };
    assert_eq!(name, "a/*b*/--c");
    assert_eq!(owner.as_deref(), Some("quoted\"owner"));
    let Some(DatabaseDdl::SetSetting { value, .. }) = parse_database_ddl(
        "ALTER DATABASE example SET application_name = 'it''s /*literal*/ -- text' /* trace */",
    )
    .unwrap() else {
        panic!("setting not recognized");
    };
    assert_eq!(value.as_deref(), Some("it's /*literal*/ -- text"));
}

#[test]
fn database_drop_comments_do_not_hide_invalid_options() {
    assert!(matches!(
        parse_database_ddl("/* trace */ DROP DATABASE IF EXISTS example; /* tail */").unwrap(),
        Some(DatabaseDdl::Drop {
            if_exists: true,
            ..
        })
    ));
    assert!(parse_database_ddl("CREATE DATABASE example /* trace */ INVALID option").is_err());
    assert!(parse_database_ddl("DROP DATABASE example /* unfinished").is_err());
}

#[test]
fn extension_creation_accepts_comments_without_consuming_following_statements() {
    use bicdb_core::BicDb;
    use bicdb_sql::{SqlSession, SqlValue};
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    for statement in [
        "CREATE EXTENSION IF NOT EXISTS plpgsql /* trace='42' */",
        "/* lead */ CREATE/* gap */EXTENSION IF NOT EXISTS plpgsql /* nested /* comment */ end */; -- tail",
        "CREATE EXTENSION IF NOT EXISTS plpgsql /* tail */; SELECT 42",
    ] {
        sql.execute(statement).unwrap();
    }
    assert_eq!(
        sql.execute("SELECT 42").unwrap().rows,
        vec![vec![SqlValue::Int(42)]]
    );
    assert!(sql
        .execute("CREATE EXTENSION IF NOT EXISTS plpgsql /* tail */ INVALID option")
        .is_err());
}
