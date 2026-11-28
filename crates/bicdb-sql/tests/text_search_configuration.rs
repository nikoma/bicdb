use bicdb_core::BicDb;
use bicdb_sql::{render_oid_alias_value, SqlSession, SqlValue};

#[test]
fn copied_text_search_configurations_keep_their_oid_and_lexemes_after_reopen() {
    let root = tempfile::tempdir().unwrap();
    let values = {
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE SCHEMA search_copy;
                 CREATE TEXT SEARCH CONFIGURATION search_copy.\"English Copy\" (COPY = pg_catalog.english);
                 CREATE TABLE stored_configuration (id int PRIMARY KEY, cfg regconfig);
                 INSERT INTO stored_configuration VALUES (1, 'search_copy.\"English Copy\"');
                 ALTER TEXT SEARCH CONFIGURATION search_copy.\"English Copy\" RENAME TO \"Renamed\";",
            )
            .unwrap();
        session
            .execute(
                "SELECT cfg, to_tsvector(cfg, 'running and foxes')::text FROM stored_configuration",
            )
            .unwrap()
            .rows
    };

    let mut db = BicDb::open(root.path()).unwrap();
    assert_eq!(
        render_oid_alias_value(&db, "regconfig", &values[0][0]).unwrap(),
        "search_copy.\"Renamed\""
    );
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        session
            .execute(
                "SELECT cfg, to_tsvector(cfg, 'running and foxes')::text FROM stored_configuration"
            )
            .unwrap()
            .rows,
        values
    );
    assert_eq!(values[0][1], SqlValue::String("'fox':3 'run':1".into()));
}

#[test]
fn authored_text_search_objects_follow_search_path_and_savepoints() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE SCHEMA search_copy;
         CREATE TEXT SEARCH CONFIGURATION search_copy.english (COPY = pg_catalog.simple);
         SET search_path TO search_copy, pg_catalog, public;
         SET default_text_search_config TO 'english';",
        )
        .unwrap();

    let rows = session
        .execute(
            "SELECT to_tsvector('running and foxes')::text,
           to_tsvector('english'::regconfig, 'running and foxes')::text,
           to_tsvector('pg_catalog.english'::regconfig, 'running and foxes')::text",
        )
        .unwrap()
        .rows;
    assert_eq!(
        rows,
        vec![vec![
            SqlValue::String("'and':2 'foxes':3 'running':1".into()),
            SqlValue::String("'and':2 'foxes':3 'running':1".into()),
            SqlValue::String("'fox':3 'run':1".into()),
        ]]
    );
    assert_eq!(
        session
            .execute("SHOW default_text_search_config")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("search_copy.english".into())]]
    );

    session.execute("BEGIN").unwrap();
    session.execute("SAVEPOINT text_search_savepoint").unwrap();
    session
        .execute("ALTER TEXT SEARCH CONFIGURATION search_copy.english RENAME TO temporary_name")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT count(*) FROM pg_ts_config WHERE cfgname = 'temporary_name'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );
    session
        .execute("ROLLBACK TO SAVEPOINT text_search_savepoint")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT count(*) FROM pg_ts_config WHERE cfgname = 'temporary_name'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(0)]]
    );
    session.execute("COMMIT").unwrap();

    let error = session.execute("DROP SCHEMA search_copy").unwrap_err();
    assert_eq!(error.sqlstate(), "2BP01");
    session.execute("DROP SCHEMA search_copy CASCADE").unwrap();
    assert_eq!(
        session
            .execute("SELECT count(*) FROM pg_ts_config WHERE cfgname = 'english'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );
}

#[test]
fn authored_mappings_distinguish_unmapped_tokens_from_stop_words_and_copy_the_definition() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TEXT SEARCH CONFIGURATION authored_terms (PARSER = pg_catalog.default)")
        .unwrap();
    assert_eq!(
        sql.execute("SELECT to_tsvector('authored_terms', 'running foxes')::text")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(String::new())]]
    );
    sql.execute(
        "ALTER TEXT SEARCH CONFIGURATION authored_terms ADD MAPPING FOR asciiword WITH english_stem;
         CREATE TEXT SEARCH CONFIGURATION copied_terms (COPY = authored_terms);
         ALTER TEXT SEARCH CONFIGURATION authored_terms ALTER MAPPING FOR word WITH pg_catalog.simple;
         ALTER TEXT SEARCH CONFIGURATION authored_terms ALTER MAPPING REPLACE english_stem WITH simple;",
    ).unwrap();
    let result = sql
        .execute(
            "SELECT to_tsvector('authored_terms', 'running 42 the foxes café')::text,
                to_tsvector('copied_terms', 'running 42 the foxes café')::text,
                phraseto_tsquery('copied_terms', 'running 42 the foxes café')::text",
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::String("'café':4 'foxes':3 'running':1 'the':2".into()),
            SqlValue::String("'fox':3 'run':1".into()),
            SqlValue::String("'run' <2> 'fox'".into()),
        ]]
    );

    sql.execute("BEGIN").unwrap();
    sql.execute("SAVEPOINT before_drop").unwrap();
    sql.execute("ALTER TEXT SEARCH CONFIGURATION authored_terms DROP MAPPING FOR word, asciiword")
        .unwrap();
    assert_eq!(
        sql.execute("SELECT to_tsvector('authored_terms', 'running café')::text")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(String::new())]]
    );
    sql.execute("ROLLBACK TO SAVEPOINT before_drop").unwrap();
    sql.execute("COMMIT").unwrap();
    assert_eq!(
        sql.execute("SELECT to_tsvector('authored_terms', 'running café')::text")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("'café':2 'running':1".into())]]
    );

    let error = sql.execute(
        "ALTER TEXT SEARCH CONFIGURATION authored_terms ADD MAPPING FOR asciiword WITH english_stem",
    ).unwrap_err();
    assert_eq!(error.sqlstate(), "23505");
}

#[test]
fn authored_configuration_names_obey_schema_usage() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE ROLE config_reader;
         CREATE SCHEMA search_private;
         CREATE TEXT SEARCH CONFIGURATION search_private.secret (COPY = pg_catalog.english);
         SET ROLE config_reader;
         SET search_path TO search_private, public;",
    )
    .unwrap();

    for (query, state) in [
        ("SELECT 'search_private.secret'::regconfig", "42501"),
        (
            "SELECT to_tsvector('search_private.secret','running')",
            "42501",
        ),
        ("SELECT 'secret'::regconfig", "42704"),
    ] {
        assert_eq!(sql.execute(query).unwrap_err().sqlstate(), state, "{query}");
    }
    assert_eq!(
        sql.execute(
            "SELECT oid::regconfig::text, to_tsvector(oid::regconfig,'running')::text
             FROM pg_ts_config WHERE cfgname = 'secret'"
        )
        .unwrap()
        .rows,
        vec![vec![
            SqlValue::String("search_private.secret".into()),
            SqlValue::String("'run':1".into()),
        ]]
    );

    sql.execute(
        "RESET ROLE;
         GRANT USAGE ON SCHEMA search_private TO config_reader;
         SET ROLE config_reader;",
    )
    .unwrap();
    assert_eq!(
        sql.execute("SELECT 'secret'::regconfig::text")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("secret".into())]]
    );
}

#[test]
fn authored_dictionaries_run_in_mapping_order_and_track_dependencies() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE TEXT SEARCH DICTIONARY leading_dictionary (
             TEMPLATE = pg_catalog.simple, STOPWORDS = english, ACCEPT = false);
         CREATE TEXT SEARCH CONFIGURATION dictionary_terms (PARSER = default);
         ALTER TEXT SEARCH CONFIGURATION dictionary_terms
             ADD MAPPING FOR asciiword WITH leading_dictionary, english_stem;",
    )
    .unwrap();
    let initial = sql
        .execute(
            "SELECT ts_lexize('leading_dictionary','the')::text,
                ts_lexize('leading_dictionary','running') IS NULL,
                to_tsvector('dictionary_terms','running and foxes')::text",
        )
        .unwrap();
    assert_eq!(
        initial.rows,
        vec![vec![
            SqlValue::String("{}".into()),
            SqlValue::Bool(true),
            SqlValue::String("'fox':3 'run':1".into()),
        ]]
    );
    let error = sql
        .execute("DROP TEXT SEARCH DICTIONARY leading_dictionary")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "2BP01");
    let error = sql
        .execute("ALTER TEXT SEARCH DICTIONARY leading_dictionary (ACCEPT = yes)")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42601");

    sql.execute("ALTER TEXT SEARCH DICTIONARY leading_dictionary (ACCEPT = true)")
        .unwrap();
    assert_eq!(
        sql.execute("SELECT to_tsvector('dictionary_terms','running and foxes')::text")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("'foxes':3 'running':1".into())]]
    );
    sql.execute("BEGIN").unwrap();
    sql.execute("DROP TEXT SEARCH DICTIONARY leading_dictionary CASCADE")
        .unwrap();
    assert_eq!(
        sql.execute("SELECT count(*) FROM pg_ts_config WHERE cfgname = 'dictionary_terms'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(0)]]
    );
    sql.execute("ROLLBACK").unwrap();
    let restored = sql
        .execute(
            "SELECT 'leading_dictionary'::regdictionary::text,
                to_tsvector('dictionary_terms','running and foxes')::text",
        )
        .unwrap();
    assert_eq!(
        restored.rows,
        vec![vec![
            SqlValue::String("leading_dictionary".into()),
            SqlValue::String("'foxes':3 'running':1".into()),
        ]]
    );
}
