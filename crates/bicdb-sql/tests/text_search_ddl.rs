//! CREATE / ALTER / DROP TEXT SEARCH objects and their use by the text
//! search functions, against PostgreSQL 18's results.
use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn query(session: &mut SqlSession<'_>, sql: &str) -> Vec<Vec<SqlValue>> {
    session.execute(sql).unwrap().rows
}

fn text(session: &mut SqlSession<'_>, sql: &str) -> String {
    query(session, sql)[0][0].to_cell()
}

fn error(session: &mut SqlSession<'_>, sql: &str) -> String {
    session.execute(sql).unwrap_err().to_string()
}

#[test]
fn synonym_dictionary_in_a_copied_configuration() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TEXT SEARCH DICTIONARY syn (template = synonym, synonyms = synonym_sample)",
        )
        .unwrap();
    assert_eq!(
        text(&mut session, "SELECT ts_lexize('syn', 'PoStGrEs')::text"),
        "{pgsql}"
    );
    assert_eq!(
        query(&mut session, "SELECT ts_lexize('syn', 'other')")[0][0],
        SqlValue::Null
    );
    session
        .execute("CREATE TEXT SEARCH CONFIGURATION syn_tst (COPY = english)")
        .unwrap();
    session
        .execute(
            "ALTER TEXT SEARCH CONFIGURATION syn_tst ALTER MAPPING FOR asciiword, hword_asciipart, asciihword WITH syn, english_stem",
        )
        .unwrap();
    assert_eq!(
        text(
            &mut session,
            "SELECT to_tsvector('syn_tst', 'Postgresql is often called as postgres or pgsql and pronounced as postgre')"
        ),
        "'call':4 'often':3 'pgsql':1,6,8,12 'pronounc':10"
    );
    assert_eq!(
        text(
            &mut session,
            "SELECT to_tsquery('syn_tst', 'Index & indices')"
        ),
        "'index' & 'index':*"
    );
    // Unmapped tokens take no position.
    session
        .execute("ALTER TEXT SEARCH CONFIGURATION syn_tst DROP MAPPING FOR asciiword")
        .unwrap();
    assert_eq!(
        text(&mut session, "SELECT to_tsvector('syn_tst', 'foo bar 123')"),
        "'123':1"
    );
    assert_eq!(
        text(
            &mut session,
            "SELECT dictinitoption FROM pg_ts_dict WHERE dictname = 'syn'"
        ),
        "synonyms = 'synonym_sample'"
    );
    assert_eq!(text(&mut session, "SELECT 'syn_tst'::regconfig"), "syn_tst");
}

#[test]
fn dictionary_options_are_validated_by_their_template() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TEXT SEARCH DICTIONARY syn (template = synonym, synonyms = synonym_sample)",
        )
        .unwrap();
    assert!(error(
        &mut session,
        "ALTER TEXT SEARCH DICTIONARY syn (CaseSensitive = 2)"
    )
    .contains("casesensitive requires a Boolean value"));
    session
        .execute("ALTER TEXT SEARCH DICTIONARY syn (CaseSensitive = 1)")
        .unwrap();
    assert_eq!(
        text(
            &mut session,
            "SELECT dictinitoption FROM pg_ts_dict WHERE dictname = 'syn'"
        ),
        "synonyms = 'synonym_sample', casesensitive = 1"
    );
    assert!(error(
        &mut session,
        "CREATE TEXT SEARCH DICTIONARY d (template = simple, \"Accept\" = 1)"
    )
    .contains("unrecognized simple dictionary parameter: \"Accept\""));
    assert!(error(
        &mut session,
        "CREATE TEXT SEARCH DICTIONARY d (template = snowball, language = klingon)"
    )
    .contains("no Snowball stemmer available"));
}

#[test]
fn dictionaries_bicdb_cannot_execute_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE TEXT SEARCH DICTIONARY i (template = ispell, dictfile = ispell_sample, afffile = ispell_sample)",
        "CREATE TEXT SEARCH DICTIONARY t (template = thesaurus, dictfile = thesaurus_sample, dictionary = english_stem)",
    ] {
        let err = session.execute(sql).unwrap_err();
        assert_eq!(err.sqlstate(), "0A000", "{sql}: {err}");
    }
    assert_eq!(
        text(
            &mut session,
            "SELECT count(*) FROM pg_ts_dict WHERE dictname IN ('i', 't')"
        ),
        "0"
    );
}

#[test]
fn drop_follows_dependencies_and_rollback_restores() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TEXT SEARCH DICTIONARY d (template = snowball, language = english)")
        .unwrap();
    session
        .execute("CREATE TEXT SEARCH CONFIGURATION c (copy = english)")
        .unwrap();
    session
        .execute("ALTER TEXT SEARCH CONFIGURATION c ALTER MAPPING REPLACE english_stem WITH d")
        .unwrap();
    let err = session
        .execute("DROP TEXT SEARCH DICTIONARY d")
        .unwrap_err();
    assert_eq!(err.sqlstate(), "2BP01");
    assert!(error(&mut session, "DROP TEXT SEARCH DICTIONARY simple")
        .contains("required by the database system"));
    session.execute("BEGIN").unwrap();
    session
        .execute("DROP TEXT SEARCH DICTIONARY d CASCADE")
        .unwrap();
    assert_eq!(
        text(
            &mut session,
            "SELECT count(*) FROM pg_ts_config WHERE cfgname = 'c'"
        ),
        "0"
    );
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        text(&mut session, "SELECT to_tsvector('c', 'The running dogs')"),
        // The dictionary has no stop words.
        "'dog':3 'run':2 'the':1"
    );
}

#[test]
fn text_search_objects_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TEXT SEARCH CONFIGURATION keep_cfg (copy = simple)")
            .unwrap();
    }
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        text(&mut session, "SELECT to_tsvector('keep_cfg', 'The Dogs')"),
        "'dogs':2 'the':1"
    );
}
