use bicdb_core::{full_text_oversized_terms_skipped, BicDb, MAX_FULL_TEXT_TERM_BYTES};
use bicdb_sql::{PgTsQuery, SqlSession, SqlValue};

fn tsquery(value: &str) -> SqlValue {
    SqlValue::TsQuery(PgTsQuery::from_postgres_text(value).unwrap())
}

#[test]
fn tsquery_sql_value_serializes_as_postgres_text() {
    assert_eq!(
        serde_json::to_value(tsquery("a & b")).unwrap(),
        serde_json::json!("'a' & 'b'")
    );
}

#[test]
fn tsvector_storage_functions_and_comparison_match_postgres() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE search_vectors (
                    id text PRIMARY KEY,
                    document tsvector NOT NULL UNIQUE
                )",
            )
            .unwrap();
        session
            .execute("CREATE TABLE document_type_collision (id text PRIMARY KEY, document text)")
            .unwrap();
        session
            .execute(
                "INSERT INTO search_vectors VALUES
                    ('one', '''z'':2,1,1A,2B,4D,3C ''a b'':1A,2B,2C,3D'),
                    ('two', '''a'':2'),
                    ('three', '''a'':1A')",
            )
            .unwrap();

        let canonical = session
            .execute(
                "SELECT document, length(document), strip(document)
                 FROM search_vectors WHERE id = 'one'",
            )
            .unwrap();
        assert_eq!(
            canonical.rows,
            vec![vec![
                SqlValue::String("'a b':1A,2B,3 'z':1A,2B,3C,4".to_string()),
                SqlValue::Int(2),
                SqlValue::String("'a b' 'z'".to_string()),
            ]]
        );
        assert_eq!(
            canonical.column_types,
            vec![
                Some("tsvector".to_string()),
                Some("int4".to_string()),
                Some("tsvector".to_string()),
            ]
        );

        let concat = session
            .execute("SELECT '''a'':1A'::tsvector || '''b'':2B'::tsvector")
            .unwrap();
        assert_eq!(
            concat.rows,
            vec![vec![SqlValue::String("'a':1A 'b':3B".to_string())]]
        );
        assert_eq!(concat.column_types, vec![Some("tsvector".to_string())]);

        let ordered = session
            .execute("SELECT id FROM search_vectors WHERE id IN ('two', 'three') ORDER BY document")
            .unwrap();
        assert_eq!(
            ordered.rows,
            vec![
                vec![SqlValue::String("two".to_string())],
                vec![SqlValue::String("three".to_string())],
            ]
        );

        let invalid = session.execute("SELECT '''a'':0'::tsvector").unwrap_err();
        assert_eq!(invalid.sqlstate(), "22P02");
        let invalid_weight = session.execute("SELECT '''a'':1Z'::tsvector").unwrap_err();
        assert_eq!(invalid_weight.sqlstate(), "22P02");
    }

    let mut db = BicDb::open(dir.path()).unwrap();
    let reopened = SqlSession::new(&mut db)
        .execute("SELECT document FROM search_vectors WHERE id = 'one'")
        .unwrap();
    assert_eq!(
        reopened.rows,
        vec![vec![SqlValue::String(
            "'a b':1A,2B,3 'z':1A,2B,3C,4".to_string()
        )]]
    );
}

#[test]
fn tsquery_storage_operators_and_matching_match_postgres() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE search_queries (
                    id text PRIMARY KEY,
                    query tsquery NOT NULL
                )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO search_queries VALUES
                    ('canonical', '!(fat | rat) & cat:*BA'),
                    ('left-nested', '(a & b) & c'),
                    ('right-nested', 'a & (b & c)')",
            )
            .unwrap();

        let canonical = session
            .execute("SELECT query FROM search_queries WHERE id = 'canonical'")
            .unwrap();
        assert_eq!(
            canonical.rows,
            vec![vec![tsquery("!(fat | rat) & cat:*AB")]]
        );
        assert_eq!(canonical.column_types, vec![Some("tsquery".to_string())]);

        let operations = session
            .execute(
                "SELECT
                    'a'::tsquery && 'b'::tsquery,
                    'a'::tsquery || 'b'::tsquery,
                    !!'a'::tsquery",
            )
            .unwrap();
        assert_eq!(
            operations.rows,
            vec![vec![tsquery("a & b"), tsquery("a | b"), tsquery("!a"),]]
        );
        assert_eq!(
            operations.column_types,
            vec![Some("tsquery".to_string()); 3]
        );

        let matches = session
            .execute(
                "SELECT
                    '''a'':1A ''alpha'':2 ''b'':3B ''cat'':4C'::tsvector @@ 'a & b'::tsquery,
                    '''a'':1A ''alpha'':2 ''b'':3B ''cat'':4C'::tsvector @@ 'a <2> b'::tsquery,
                    'al:*'::tsquery @@ '''alpha'':2'::tsvector,
                    '''a'':1A'::tsvector @@ 'a:B'::tsquery,
                    '''a'''::tsvector @@ 'a:B'::tsquery,
                    '''a'''::tsvector @@ 'a <-> b'::tsquery",
            )
            .unwrap();
        assert_eq!(
            matches.rows,
            vec![vec![
                SqlValue::Bool(true),
                SqlValue::Bool(true),
                SqlValue::Bool(true),
                SqlValue::Bool(false),
                SqlValue::Bool(true),
                SqlValue::Bool(false),
            ]]
        );

        let comparison = session
            .execute(
                "SELECT
                    'a:A'::tsquery = 'a:B'::tsquery,
                    'a:*'::tsquery = 'a'::tsquery,
                    'a & (b & c)'::tsquery < '(a & b) & c'::tsquery",
            )
            .unwrap();
        assert_eq!(
            comparison.rows,
            vec![vec![
                SqlValue::Bool(true),
                SqlValue::Bool(true),
                SqlValue::Bool(true),
            ]]
        );

        assert_eq!(
            session
                .execute("SELECT 'a:Z'::tsquery")
                .unwrap_err()
                .sqlstate(),
            "22P02"
        );
        assert_eq!(
            session
                .execute("SELECT 'a <16385> b'::tsquery")
                .unwrap_err()
                .sqlstate(),
            "22023"
        );
    }

    let mut db = BicDb::open(dir.path()).unwrap();
    let reopened = SqlSession::new(&mut db)
        .execute("SELECT query FROM search_queries WHERE id = 'canonical'")
        .unwrap();
    assert_eq!(reopened.rows, vec![vec![tsquery("!(fat | rat) & cat:*AB")]]);
}

#[test]
fn text_search_arrays_copy_catalogs_and_reopen_preserve_type_identity() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE text_search_type_roundtrip (
                    id text PRIMARY KEY,
                    document tsvector NOT NULL,
                    query tsquery NOT NULL,
                    documents tsvector[],
                    queries tsquery[]
                )",
            )
            .unwrap();
        assert_eq!(
            session
                .copy_insert_rows(
                    "text_search_type_roundtrip",
                    &["id".into(), "document".into(), "query".into()],
                    vec![vec![
                        Some("copy".into()),
                        Some("'fat':1A 'rat':2".into()),
                        Some("fat & !rat".into()),
                    ]],
                )
                .unwrap(),
            1
        );
        session
            .execute(
                "INSERT INTO text_search_type_roundtrip
                    (id, document, query, documents, queries)
                 VALUES (
                    'arrays', '''alpha'':1'::tsvector, 'alpha'::tsquery,
                    ARRAY['''fat'':1A'::tsvector, '''rat'':2'::tsvector],
                    ARRAY['fat & rat'::tsquery, NULL]
                 )",
            )
            .unwrap();

        assert_eq!(
            session
                .execute(
                    "SELECT typname, oid, typarray
                     FROM pg_catalog.pg_type
                     WHERE typname IN ('tsvector', 'tsquery')
                     ORDER BY oid",
                )
                .unwrap()
                .rows,
            vec![
                vec![
                    SqlValue::String("tsvector".into()),
                    SqlValue::Int(3614),
                    SqlValue::Int(3643),
                ],
                vec![
                    SqlValue::String("tsquery".into()),
                    SqlValue::Int(3615),
                    SqlValue::Int(3645),
                ],
            ]
        );
        assert_eq!(
            session
                .execute(
                    "SELECT attname, atttypid, format_type(atttypid, atttypmod)
                     FROM pg_catalog.pg_attribute
                     WHERE attrelid = 'text_search_type_roundtrip'::regclass
                       AND attnum > 0
                     ORDER BY attnum",
                )
                .unwrap()
                .rows,
            vec![
                vec![
                    SqlValue::String("id".into()),
                    SqlValue::Int(25),
                    SqlValue::String("text".into()),
                ],
                vec![
                    SqlValue::String("document".into()),
                    SqlValue::Int(3614),
                    SqlValue::String("tsvector".into()),
                ],
                vec![
                    SqlValue::String("query".into()),
                    SqlValue::Int(3615),
                    SqlValue::String("tsquery".into()),
                ],
                vec![
                    SqlValue::String("documents".into()),
                    SqlValue::Int(3643),
                    SqlValue::String("tsvector[]".into()),
                ],
                vec![
                    SqlValue::String("queries".into()),
                    SqlValue::Int(3645),
                    SqlValue::String("tsquery[]".into()),
                ],
            ]
        );
    }

    let mut db = BicDb::open(dir.path()).unwrap();
    let result = SqlSession::new(&mut db)
        .execute(
            "SELECT id, document, query, documents, queries
             FROM text_search_type_roundtrip ORDER BY id",
        )
        .unwrap();
    assert_eq!(
        result.column_types,
        vec![
            Some("text".into()),
            Some("tsvector".into()),
            Some("tsquery".into()),
            Some("tsvector[]".into()),
            Some("tsquery[]".into()),
        ]
    );
    assert_eq!(result.rows.len(), 2);
    assert_eq!(
        result.rows[0],
        vec![
            SqlValue::String("arrays".into()),
            SqlValue::String("'alpha':1".into()),
            tsquery("alpha"),
            SqlValue::Json(serde_json::json!(["'fat':1A", "'rat':2"])),
            SqlValue::Json(serde_json::json!(["'fat' & 'rat'", null])),
        ]
    );
    assert_eq!(
        result.rows[1],
        vec![
            SqlValue::String("copy".into()),
            SqlValue::String("'fat':1A 'rat':2".into()),
            tsquery("fat & !rat"),
            SqlValue::Null,
            SqlValue::Null,
        ]
    );
}

#[test]
fn text_search_constructors_and_query_helpers_match_postgres() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let result = session
        .execute(
            "SELECT
                to_tsvector('The Fat Rats ate seven cheeses.'),
                to_tsvector('simple', 'The Fat Rats ate seven cheeses.'),
                to_tsquery('english', 'The & Fat & Rats'),
                plainto_tsquery('english', 'The Fat Rats'),
                phraseto_tsquery('english', 'fat the rat'),
                websearch_to_tsquery('english', '\"fat rat\" -cat OR dog'),
                get_current_ts_config(),
                numnode('a & !b'::tsquery),
                querytree('a & !b'::tsquery),
                tsquery_phrase('a'::tsquery, 'b'::tsquery, 3)",
        )
        .unwrap();

    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::String("'ate':4 'chees':6 'fat':2 'rat':3 'seven':5".into()),
            SqlValue::String("'ate':4 'cheeses':6 'fat':2 'rats':3 'seven':5 'the':1".into()),
            tsquery("fat & rat"),
            tsquery("fat & rat"),
            tsquery("fat <2> rat"),
            tsquery("(fat <-> rat) & !cat | dog"),
            SqlValue::String("english".into()),
            SqlValue::Int(4),
            SqlValue::String("'a'".into()),
            tsquery("a <3> b"),
        ]]
    );
    assert_eq!(
        result.column_types,
        vec![
            Some("tsvector".into()),
            Some("tsvector".into()),
            Some("tsquery".into()),
            Some("tsquery".into()),
            Some("tsquery".into()),
            Some("tsquery".into()),
            Some("regconfig".into()),
            Some("int4".into()),
            Some("text".into()),
            Some("tsquery".into()),
        ]
    );
}

#[test]
fn text_search_vector_transforms_and_ranking_match_postgres() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let result = session
        .execute(
            "SELECT
                setweight('''a'':1 ''b'':2B'::tsvector, 'A'),
                setweight('''a'':1 ''b'':2B'::tsvector, 'C', ARRAY['a']),
                ts_delete('''a'':1 ''b'':2'::tsvector, 'a'),
                ts_filter('''a'':1A,2B,3C,4 ''b'':5'::tsvector, ARRAY['A','C']::\"char\"[]),
                tsvector_to_array('''a'':1 ''b'':2'::tsvector),
                array_to_tsvector(ARRAY['foo','bar','foo']),
                ts_rank('''a'':1 ''b'':2 ''c'':8'::tsvector, 'a & b'::tsquery),
                ts_rank_cd('''a'':1 ''b'':2 ''c'':8'::tsvector, 'a & b'::tsquery),
                ts_rank('''a'':1A ''b'':2B'::tsvector, 'a | b'::tsquery),
                ts_rank_cd('''a'':1A ''b'':2B'::tsvector, 'a | b'::tsquery)",
        )
        .unwrap();

    let row = &result.rows[0];
    assert_eq!(row[0], SqlValue::String("'a':1A 'b':2A".into()));
    assert_eq!(row[1], SqlValue::String("'a':1C 'b':2B".into()));
    assert_eq!(row[2], SqlValue::String("'b':2".into()));
    assert_eq!(row[3], SqlValue::String("'a':1A,3C".into()));
    assert_eq!(row[4], SqlValue::Json(serde_json::json!(["a", "b"])));
    assert_eq!(row[5], SqlValue::String("'bar' 'foo'".into()));
    let ranks = row[6..]
        .iter()
        .map(|value| match value {
            SqlValue::Float(value) => *value,
            other => panic!("expected rank float, got {other:?}"),
        })
        .collect::<Vec<_>>();
    for (actual, expected) in ranks.iter().zip([0.09910322, 0.1, 0.42554897, 1.4]) {
        assert!(
            (actual - expected).abs() < 0.000001,
            "{actual} != {expected}"
        );
    }
}

#[test]
fn text_search_headline_rewrite_and_json_match_postgres() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let result = session
        .execute(
            "SELECT
                ts_headline(
                    'english',
                    'The fat rats ate seven cheeses. A cat watched the rats.',
                    'rat & chees'::tsquery
                ),
                ts_headline(
                    'english',
                    'The fat rats ate seven cheeses. A cat watched the rats.',
                    'rat & chees'::tsquery,
                    'StartSel=<b>, StopSel=</b>, MaxWords=8, MinWords=4'
                ),
                ts_rewrite('a & b'::tsquery, 'a'::tsquery, 'x | y'::tsquery),
                ts_rewrite('a & b'::tsquery, 'a & b'::tsquery, 'x'::tsquery),
                to_tsvector(
                    'english',
                    '{\"a\":\"The Fat Rats\",\"b\":[\"cheese\",null,12]}'::jsonb
                ),
                ts_headline(
                    'english',
                    '{\"a\":\"The Fat Rats\",\"b\":[\"cheese\",null,12]}'::jsonb,
                    'rat | chees'::tsquery
                )",
        )
        .unwrap();

    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::String(
                "The fat <b>rats</b> ate seven <b>cheeses</b>. A cat watched the <b>rats</b>."
                    .into()
            ),
            SqlValue::String("<b>rats</b> ate seven <b>cheeses</b>".into()),
            tsquery("b & (x | y)"),
            tsquery("x"),
            SqlValue::String("'chees':5 'fat':2 'rat':3".into()),
            SqlValue::Json(serde_json::json!({
                "a": "The Fat <b>Rats</b>",
                "b": ["<b>cheese</b>", null, 12]
            })),
        ]]
    );
}

#[test]
fn text_search_default_parser_handles_postgres_token_classes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute(
            "SELECT to_tsvector(
                'simple',
                'Email Foo.Bar+tag@example.com URL https://docs.example.com/v1/file-name.html version 2.3.4 host api.example.com file /tmp/report.pdf numbers 1,234.50 42 3.14 state-of-the-art'
            )",
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::String(
            "'/tmp/report.pdf':13 '/v1/file-name.html':7 '1':15 '2.3.4':9 '234.50':16 '3.14':18 '42':17 'api.example.com':11 'art':23 'docs.example.com':6 'docs.example.com/v1/file-name.html':5 'email':1 'file':12 'foo.bar':2 'host':10 'numbers':14 'of':21 'state':20 'state-of-the-art':19 'tag@example.com':3 'the':22 'url':4 'version':8"
                .into()
        )]]
    );
}

#[test]
fn text_search_skips_oversized_terms_consistently_without_losing_safe_identifiers() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let base64_like = "QUJD".repeat(900);
    let unicode = "界".repeat(MAX_FULL_TEXT_TERM_BYTES / 3 + 1);
    let url = format!(
        "https://medical.example/{}",
        "z".repeat(MAX_FULL_TEXT_TERM_BYTES + 1)
    );
    let chemical_left = "C".repeat(1_200);
    let chemical_right = "H".repeat(1_200);
    let chemical = format!("{chemical_left}-{chemical_right}");
    let max_identifier = "N".repeat(MAX_FULL_TEXT_TERM_BYTES);
    let before = full_text_oversized_terms_skipped();

    let result = session
        .execute(&format!(
            "SELECT
                to_tsvector(
                    'simple',
                    'keep {base64_like} {unicode} {url} {chemical} {max_identifier}'
                ),
                plainto_tsquery('simple', '{base64_like}'),
                to_tsquery('simple', '{base64_like}'),
                plainto_tsquery('simple', 'keep {base64_like}')"
        ))
        .unwrap();
    let SqlValue::String(vector) = &result.rows[0][0] else {
        panic!("expected tsvector text");
    };
    assert!(vector.contains("'keep':1"));
    assert!(vector.contains("'medical.example'"));
    assert!(vector.contains(&chemical_left.to_ascii_lowercase()));
    assert!(vector.contains(&chemical_right.to_ascii_lowercase()));
    assert!(vector.contains(&max_identifier.to_ascii_lowercase()));
    assert!(!vector.contains(&base64_like.to_ascii_lowercase()));
    assert!(!vector.contains(&unicode));
    assert_eq!(result.rows[0][1], tsquery(""));
    assert_eq!(result.rows[0][2], tsquery(""));
    assert_eq!(result.rows[0][3], tsquery("keep"));
    assert!(full_text_oversized_terms_skipped() > before);

    // Explicit type input retains PostgreSQL's representation error. The
    // lenient behavior applies to tokenizers/query constructors and indexes,
    // not to malformed serialized tsquery values.
    assert!(session
        .execute(&format!("SELECT '{base64_like}'::tsquery"))
        .is_err());
}

#[test]
fn table_driven_query_rewrite_matches_postgres() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE search_rewrites (
                id integer PRIMARY KEY,
                target tsquery NOT NULL,
                substitute tsquery NOT NULL
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO search_rewrites VALUES
                (1, 'a', 'x | y'),
                (2, 'b', 'z')",
        )
        .unwrap();
    let result = session
        .execute(
            "SELECT ts_rewrite(
                'a & b'::tsquery,
                'SELECT target, substitute FROM search_rewrites ORDER BY id'
            )",
        )
        .unwrap();
    assert_eq!(result.rows, vec![vec![tsquery("z & (y | x)")]]);
}
