//! Test group split from the former monolithic tests/sql.rs.
use super::*;

#[test]
fn session_execute_runs_semicolon_separated_statements_in_order() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    let result = session
        .execute(
            "CREATE TABLE migration_items (id TEXT PRIMARY KEY, label TEXT);
             INSERT INTO migration_items (id, label) VALUES ('one', 'first');
             SELECT label FROM migration_items WHERE id = 'one';",
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::String("first".to_string())]]
    );
}

#[test]
fn non_recursive_ctes_feed_insert_select_like_postgresql() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE carrier_audit_log (
                id TEXT PRIMARY KEY,
                action TEXT NOT NULL,
                previous_hash TEXT,
                row_hash TEXT NOT NULL
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO carrier_audit_log (id, action, previous_hash, row_hash)
             VALUES ('audit-1', 'seed', NULL, 'hash-1')",
        )
        .unwrap();

    session
        .execute(
            "WITH previous AS (
                 SELECT row_hash FROM carrier_audit_log ORDER BY id DESC LIMIT 1
             ), audit_payload AS (
                 SELECT COALESCE((SELECT row_hash FROM previous), '') AS previous_hash
             )
             INSERT INTO carrier_audit_log (id, action, previous_hash, row_hash)
             SELECT 'audit-2', 'application.operation.execute', previous_hash,
                    previous_hash || '-hash-2'
             FROM audit_payload",
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT action, previous_hash, row_hash
                 FROM carrier_audit_log WHERE id = 'audit-2'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("application.operation.execute".to_string()),
            SqlValue::String("hash-1".to_string()),
            SqlValue::String("hash-1-hash-2".to_string()),
        ]]
    );
}

#[test]
fn sql_memory_index_insert_process_and_similar_to_search() {
    let (_dir, mut db) = empty_test_db();
    db.register_embedding_model(ModelRegistryEntry::local_test("embeddinggemma-300m", 384))
        .unwrap();

    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE patient_notes (
                id TEXT PRIMARY KEY,
                text TEXT NOT NULL
            );",
        )
        .unwrap();
    session
        .execute(
            "CREATE MEMORY INDEX ON patient_notes(text)
             WITH (model = 'embeddinggemma-300m');",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO patient_notes (id, text) VALUES
             ('note-cough', 'Persistent cough after covid with fatigue and chest tightness'),
             ('note-back', 'Yoga plan for chronic low back pain and mobility');",
        )
        .unwrap();

    let process = session
        .execute("SELECT bicdb_process_memory_jobs();")
        .unwrap();
    assert_eq!(process.rows, vec![vec![SqlValue::Int(2)]]);

    let result = session
        .execute(
            "SELECT id
             FROM patient_notes
             ORDER BY SIMILAR_TO(text, 'persistent cough after covid')
             LIMIT 1;",
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::String("note-cough".to_string())]]
    );
}

#[test]
fn sql_memory_index_sync_mode_searches_without_processing_call() {
    let (_dir, mut db) = empty_test_db();
    db.register_embedding_model(ModelRegistryEntry::local_test("embeddinggemma-300m", 384))
        .unwrap();

    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE patient_notes (
                id TEXT PRIMARY KEY,
                text TEXT NOT NULL
            );",
        )
        .unwrap();
    session
        .execute(
            "CREATE MEMORY INDEX ON patient_notes(text)
             WITH (model = 'embeddinggemma-300m', mode = 'sync');",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO patient_notes (id, text) VALUES
             ('note-cough', 'Persistent cough after covid with fatigue and chest tightness'),
             ('note-back', 'Yoga plan for chronic low back pain and mobility');",
        )
        .unwrap();

    let result = session
        .execute(
            "SELECT id
             FROM patient_notes
             ORDER BY SIMILAR_TO(text, 'persistent cough after covid')
             LIMIT 1;",
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::String("note-cough".to_string())]]
    );
}

#[test]
fn sql_memory_index_supports_quoted_identifiers() {
    let (_dir, mut db) = empty_test_db();
    db.register_embedding_model(ModelRegistryEntry::local_test("embeddinggemma-300m", 384))
        .unwrap();

    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            r#"CREATE TABLE "patient_notes" (
                "id" TEXT PRIMARY KEY,
                "text" TEXT NOT NULL
            );"#,
        )
        .unwrap();
    session
        .execute(
            r#"CREATE MEMORY INDEX ON "patient_notes"("text")
             WITH (model = 'embeddinggemma-300m');"#,
        )
        .unwrap();
    session
        .execute(
            r#"INSERT INTO "patient_notes" ("id", "text") VALUES
             ('note-cough', 'Persistent cough after covid with fatigue and chest tightness'),
             ('note-back', 'Yoga plan for chronic low back pain and mobility');"#,
        )
        .unwrap();

    let process = session
        .execute("SELECT bicdb_process_memory_jobs();")
        .unwrap();
    assert_eq!(process.rows, vec![vec![SqlValue::Int(2)]]);

    let result = session
        .execute(
            r#"SELECT "id"
             FROM "patient_notes"
             ORDER BY SIMILAR_TO("text", 'persistent cough after covid')
             LIMIT 1;"#,
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::String("note-cough".to_string())]]
    );
}

#[test]
fn sql_memory_column_creates_default_memory_index() {
    let (_dir, mut db) = empty_test_db();
    db.register_embedding_model(ModelRegistryEntry::local_test("embeddinggemma-300m", 384))
        .unwrap();

    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE patient_notes (
                id TEXT PRIMARY KEY,
                text TEXT MEMORY
            );",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO patient_notes (id, text) VALUES
             ('note-cough', 'Persistent cough after covid with fatigue and chest tightness'),
             ('note-back', 'Yoga plan for chronic low back pain and mobility');",
        )
        .unwrap();

    let process = session
        .execute("SELECT bicdb_process_memory_jobs();")
        .unwrap();
    assert_eq!(process.rows, vec![vec![SqlValue::Int(2)]]);

    let result = session
        .execute(
            "SELECT id
             FROM patient_notes
             ORDER BY SIMILAR_TO(text, 'persistent cough after covid')
             LIMIT 1;",
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::String("note-cough".to_string())]]
    );
}

#[test]
fn postgres_text_array_columns_support_auth_style_queries() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE oauth_clients (
                id TEXT PRIMARY KEY,
                scopes TEXT[] NOT NULL DEFAULT '{}'
            );",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO oauth_clients (id, scopes) VALUES
             ('spa', ARRAY['openid', 'profile', 'email']::text[]),
             ('worker', ARRAY['system:integration']::text[]);",
        )
        .unwrap();

    let contains = session
        .execute(
            "SELECT id FROM oauth_clients
             WHERE scopes @> ARRAY['openid', 'email']::text[]
               AND 'profile' = ANY(scopes)
             ORDER BY id;",
        )
        .unwrap();
    assert_eq!(
        contains.rows,
        vec![vec![SqlValue::String("spa".to_string())]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT id FROM oauth_clients
                 WHERE ARRAY['openid', 'email']::text[] <@ scopes
                 ORDER BY id;",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("spa".to_string())]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT ARRAY['gitlab_main'] <@ ARRAY['gitlab_main', 'gitlab_ci'],
                        ARRAY['gitlab_main', 'unknown'] <@ ARRAY['gitlab_main', 'gitlab_ci'],
                        ARRAY[20, 30] <@ ARRAY[20, 30, 40, 50]"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Bool(true),
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT array_append(NULL::text[], 'x'),
                        array_prepend('x', NULL::text[]),
                        array_cat(NULL::text[], ARRAY['x']::text[]),
                        array_position(ARRAY['a', 'b']::text[], 'a', 0),
                        array_position(ARRAY['a', 'b']::text[], 'a', -5),
                        array_ndims('{}'::text[]),
                        array_dims('{}'::text[]),
                        array_length('{}'::text[], 1),
                        cardinality('{}'::text[])"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Json(json!(["x"])),
            SqlValue::Json(json!(["x"])),
            SqlValue::Json(json!(["x"])),
            SqlValue::Int(1),
            SqlValue::Int(1),
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Int(0),
        ]]
    );
    let null_start = session
        .execute("SELECT array_position(ARRAY['a']::text[], 'a', NULL)")
        .unwrap_err();
    assert_eq!(null_start.sqlstate(), "22004");
    assert_eq!(null_start.to_string(), "initial position must not be null");

    session
        .execute(
            "UPDATE oauth_clients
             SET scopes = array_remove(scopes, 'profile')
             WHERE id = 'spa';",
        )
        .unwrap();
    let removed = session
        .execute("SELECT scopes FROM oauth_clients WHERE id = 'spa';")
        .unwrap();
    assert_eq!(
        removed.rows,
        vec![vec![SqlValue::Json(json!(["openid", "email"]))]]
    );

    session
        .execute("CREATE TABLE varchar_array_rows (id TEXT PRIMARY KEY, labels character varying(255)[])")
        .unwrap();
    session
        .execute("INSERT INTO varchar_array_rows (id, labels) VALUES ('one', ARRAY['alpha', 'beta']::varchar[])")
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT udt_name FROM information_schema.columns
                 WHERE table_name = 'varchar_array_rows' AND column_name = 'labels'"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("_varchar".to_string())]]
    );
    assert_eq!(
        session
            .execute("SELECT labels FROM varchar_array_rows WHERE id = 'one'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Json(json!(["alpha", "beta"]))]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT labels[1], labels[2], labels[3] FROM varchar_array_rows WHERE id = 'one'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("alpha".to_string()),
            SqlValue::String("beta".to_string()),
            SqlValue::Null,
        ]]
    );
    session
        .execute(
            "DELETE FROM oauth_clients
             WHERE ARRAY['system:integration']::text[] <@ scopes",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT id FROM oauth_clients ORDER BY id")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("spa".to_string())]]
    );
    session
        .execute(
            "CREATE TABLE approval_rules (
                id bigint PRIMARY KEY,
                role_approvers integer[] NOT NULL,
                CONSTRAINT allowed_role_approvers CHECK (
                    role_approvers = '{}'::integer[]
                    OR role_approvers <@ ARRAY[20, 30, 40, 50, 60]
                )
            )",
        )
        .unwrap();
    session
        .execute("INSERT INTO approval_rules (id, role_approvers) VALUES (1, ARRAY[20, 40])")
        .unwrap();
    assert_eq!(
        session
            .execute("INSERT INTO approval_rules (id, role_approvers) VALUES (2, ARRAY[20, 70])")
            .unwrap_err()
            .sqlstate(),
        "23514"
    );
    assert_eq!(
        session
            .execute("SELECT (ARRAY[10, 20, 30])[2], (ARRAY[10, 20, 30])[9]")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(20), SqlValue::Null]]
    );
    assert_eq!(
        session
            .execute("SELECT 'text[]'::regtype::oid")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1009)]]
    );
    assert_eq!(
        session
            .execute("SELECT to_regtype('character varying[]')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("character varying[]".to_string())]]
    );
    assert_eq!(
        session
            .execute("SELECT oid, typname, typelem FROM pg_catalog.pg_type WHERE oid = 1009")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(1009),
            SqlValue::String("_text".to_string()),
            SqlValue::Int(25),
        ]]
    );
}

#[test]
fn postgres_polymorphic_array_functions_work_in_all_expression_contexts() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute(
                "SELECT
                    array_append(ARRAY['a', 'b']::text[], 'c'),
                    array_prepend('a', ARRAY['b', 'c']::text[]),
                    array_cat(ARRAY['a']::text[], ARRAY['b', 'c']::text[]),
                    array_remove(ARRAY['a', NULL, 'a']::text[], NULL),
                    array_replace(ARRAY['a', NULL, 'a']::text[], 'a', 'z'),
                    array_position(ARRAY['a', NULL, 'a']::text[], NULL),
                    array_position(ARRAY['a', 'b', 'a']::text[], 'a', 2),
                    array_positions(ARRAY['a', NULL, 'a']::text[], 'a'),
                    cardinality(ARRAY[[1, 2], [3, 4]]),
                    array_ndims(ARRAY[[1, 2], [3, 4]]),
                    array_dims(ARRAY[[1, 2], [3, 4]]),
                    array_length(ARRAY[[1, 2], [3, 4]], 2),
                    array_lower(ARRAY['a', 'b']::text[], 1),
                    array_upper(ARRAY['a', 'b']::text[], 1)"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Json(json!(["a", "b", "c"])),
            SqlValue::Json(json!(["a", "b", "c"])),
            SqlValue::Json(json!(["a", "b", "c"])),
            SqlValue::Json(json!(["a", "a"])),
            SqlValue::Json(json!(["z", null, "z"])),
            SqlValue::Int(2),
            SqlValue::Int(3),
            SqlValue::Json(json!([1, 3])),
            SqlValue::Int(4),
            SqlValue::Int(2),
            SqlValue::String("[1:2][1:2]".to_string()),
            SqlValue::Int(2),
            SqlValue::Int(1),
            SqlValue::Int(2),
        ]]
    );

    session
        .execute(
            "CREATE TABLE provenance_rows (
                id TEXT PRIMARY KEY,
                source_ids TEXT[] NOT NULL
                    DEFAULT array_append('{}'::text[], 'default-source')
            )",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("INSERT INTO provenance_rows (id) VALUES ('defaulted') RETURNING source_ids")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Json(json!(["default-source"]))]]
    );
    assert_eq!(
        session
            .execute(
                "UPDATE provenance_rows
                 SET source_ids = CASE
                     WHEN cardinality(source_ids) = 1
                     THEN array_append(source_ids, 'update-source')
                     ELSE source_ids
                 END
                 WHERE id = 'defaulted'
                 RETURNING source_ids"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Json(json!([
            "default-source",
            "update-source"
        ]))]]
    );

    session
        .execute(
            "INSERT INTO provenance_rows (id, source_ids)
             VALUES ('merged', ARRAY['first']::text[])
             ON CONFLICT (id) DO UPDATE
             SET source_ids = array_cat(provenance_rows.source_ids, excluded.source_ids)",
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                "INSERT INTO provenance_rows (id, source_ids)
                 VALUES ('merged', ARRAY['second']::text[])
                 ON CONFLICT (id) DO UPDATE
                 SET source_ids = array_cat(provenance_rows.source_ids, excluded.source_ids)
                 RETURNING source_ids"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Json(json!(["first", "second"]))]]
    );
}

#[test]
fn pg_type_catalog_matches_the_canonical_registry() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    for spec in PG_TYPE_SPECS {
        let scalar = session
            .execute(&format!(
                "SELECT oid, typlen, typbyval, typcategory, typalign, typstorage, \
                        typcollation, typarray, typtype \
                 FROM pg_catalog.pg_type WHERE typname = '{}'",
                spec.name
            ))
            .unwrap();
        assert_eq!(
            scalar.rows.len(),
            1,
            "missing pg_type row for {}",
            spec.name
        );
        assert_eq!(
            scalar.rows[0],
            vec![
                SqlValue::Int(i64::from(spec.oid)),
                SqlValue::Int(i64::from(spec.len)),
                SqlValue::Bool(spec.by_value),
                SqlValue::String(spec.category.to_string()),
                SqlValue::String(spec.align.to_string()),
                SqlValue::String(spec.storage.to_string()),
                SqlValue::Int(i64::from(spec.collation_oid())),
                SqlValue::Int(spec.array_oid.map(i64::from).unwrap_or(0)),
                SqlValue::String(spec.kind().to_string()),
            ],
            "catalog metadata differs for {}",
            spec.name
        );

        if let Some(array_oid) = spec.array_oid {
            let array = session
                .execute(&format!(
                    "SELECT oid, typlen, typbyval, typcategory, typalign, typstorage, \
                            typcollation, typelem, typarray \
                     FROM pg_catalog.pg_type WHERE typname = '_{}'",
                    spec.name
                ))
                .unwrap();
            assert_eq!(array.rows.len(), 1, "missing array row for {}", spec.name);
            assert_eq!(
                array.rows[0],
                vec![
                    SqlValue::Int(i64::from(array_oid)),
                    SqlValue::Int(-1),
                    SqlValue::Bool(false),
                    SqlValue::String(if spec.name == "record" { "P" } else { "A" }.to_string()),
                    SqlValue::String(spec.array_alignment().to_string()),
                    SqlValue::String("x".to_string()),
                    SqlValue::Int(i64::from(spec.collation_oid())),
                    SqlValue::Int(i64::from(spec.oid)),
                    SqlValue::Int(0),
                ],
                "array catalog metadata differs for {}",
                spec.name
            );
        }
    }
}

#[test]
fn ddl_uses_registered_types_and_never_lowers_unknown_types_to_text() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE registered_type_arrays (
                id UUID PRIMARY KEY,
                bytes BYTEA[],
                documents JSONB[],
                addresses INET[]
            )",
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT attname, atttypid FROM pg_catalog.pg_attribute a
                 JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
                 WHERE c.relname = 'registered_type_arrays' AND a.attnum > 0
                 ORDER BY a.attnum",
            )
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("id".to_string()), SqlValue::Int(2950)],
            vec![SqlValue::String("bytes".to_string()), SqlValue::Int(1001)],
            vec![
                SqlValue::String("documents".to_string()),
                SqlValue::Int(3807),
            ],
            vec![
                SqlValue::String("addresses".to_string()),
                SqlValue::Int(1041),
            ],
        ]
    );
    let error = session
        .execute("CREATE TABLE invalid_type_table (value definitely_not_a_pg_type)")
        .unwrap_err();
    assert!(matches!(
        error,
        SqlError::UndefinedType { ref name } if name == "definitely_not_a_pg_type"
    ));
    assert_eq!(error.sqlstate(), "42704");
}

#[test]
fn ddl_preserves_postgres_type_modifiers_in_catalogs_and_format_type() {
    let (dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    let s = |value: &str| SqlValue::String(value.to_string());
    session
        .execute(
            "CREATE TABLE typmod_values (
                amount NUMERIC(19,4),
                rounded NUMERIC(10,-2),
                label VARCHAR(12),
                code CHAR(3),
                happened TIMESTAMP(3),
                clock TIME(2),
                bits BIT(5),
                vbits VARBIT(7),
                labels VARCHAR(8)[],
                embedding VECTOR(3),
                span INTERVAL DAY TO SECOND(3)
            )",
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT attname, atttypid, atttypmod, format_type(atttypid, atttypmod)
                 FROM pg_catalog.pg_attribute a
                 JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
                 WHERE c.relname = 'typmod_values' AND a.attnum > 0
                 ORDER BY a.attnum",
            )
            .unwrap()
            .rows,
        vec![
            vec![
                s("amount"),
                SqlValue::Int(1700),
                SqlValue::Int(1_245_192),
                s("numeric(19,4)")
            ],
            vec![
                s("rounded"),
                SqlValue::Int(1700),
                SqlValue::Int(657_410),
                s("numeric(10,-2)")
            ],
            vec![
                s("label"),
                SqlValue::Int(1043),
                SqlValue::Int(16),
                s("character varying(12)")
            ],
            vec![
                s("code"),
                SqlValue::Int(1042),
                SqlValue::Int(7),
                s("character(3)")
            ],
            vec![
                s("happened"),
                SqlValue::Int(1114),
                SqlValue::Int(3),
                s("timestamp(3) without time zone")
            ],
            vec![
                s("clock"),
                SqlValue::Int(1083),
                SqlValue::Int(2),
                s("time(2) without time zone")
            ],
            vec![
                s("bits"),
                SqlValue::Int(1560),
                SqlValue::Int(5),
                s("bit(5)")
            ],
            vec![
                s("vbits"),
                SqlValue::Int(1562),
                SqlValue::Int(7),
                s("bit varying(7)")
            ],
            vec![
                s("labels"),
                SqlValue::Int(1015),
                SqlValue::Int(12),
                s("character varying(8)[]")
            ],
            vec![
                s("embedding"),
                SqlValue::Int(380_200),
                SqlValue::Int(3),
                s("vector(3)")
            ],
            vec![
                s("span"),
                SqlValue::Int(1186),
                SqlValue::Int(470_286_339),
                s("interval day to second(3)")
            ],
        ]
    );

    session
        .execute("ALTER TABLE typmod_values ALTER COLUMN label TYPE VARCHAR(24)")
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT atttypmod, format_type(atttypid, atttypmod)
                 FROM pg_catalog.pg_attribute a
                 JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
                 WHERE c.relname = 'typmod_values' AND a.attname = 'label'",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(28), s("character varying(24)")]]
    );

    session
        .execute("CREATE TABLE typmod_like (LIKE typmod_values INCLUDING ALL)")
        .unwrap();
    session
        .execute("CREATE VIEW typmod_view AS SELECT amount, label FROM typmod_values")
        .unwrap();
    session
        .execute(
            "CREATE TABLE typmod_parent (bucket integer, amount numeric(9,2))
             PARTITION BY LIST (bucket)",
        )
        .unwrap();
    session
        .execute(
            "CREATE TABLE typmod_child PARTITION OF typmod_parent
             FOR VALUES IN (1)",
        )
        .unwrap();
    for (relation, column, typmod, formatted) in [
        ("typmod_like", "amount", 1_245_192, "numeric(19,4)"),
        ("typmod_view", "label", 28, "character varying(24)"),
        ("typmod_child", "amount", 589_830, "numeric(9,2)"),
    ] {
        assert_eq!(
            session
                .execute(&format!(
                    "SELECT atttypmod, format_type(atttypid, atttypmod)
                     FROM pg_catalog.pg_attribute a
                     JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
                     WHERE c.relname = '{relation}' AND a.attname = '{column}'"
                ))
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(typmod), s(formatted)]],
            "{relation}.{column} lost its type modifier"
        );
    }

    drop(session);
    drop(db);
    let mut reopened = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut reopened);
    assert_eq!(
        session
            .execute(
                "SELECT atttypmod, format_type(atttypid, atttypmod)
                 FROM pg_catalog.pg_attribute a
                 JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
                 WHERE c.relname = 'typmod_values' AND a.attname = 'amount'",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1_245_192), s("numeric(19,4)")]]
    );
}

#[test]
fn character_declarations_preserve_postgresql_catalog_identity() {
    let (dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    let s = |value: &str| SqlValue::String(value.to_string());
    session
        .execute(
            "CREATE TABLE character_declarations (
                plain_text TEXT,
                plain_varchar VARCHAR,
                sized_varchar VARCHAR(7),
                sized_varying CHARACTER VARYING(8),
                default_char CHAR,
                sized_char CHAR(4),
                default_character CHARACTER,
                sized_character CHARACTER(5)
            )",
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT column_name, data_type, udt_name,
                        character_maximum_length, character_octet_length
                 FROM information_schema.columns
                 WHERE table_name = 'character_declarations'
                 ORDER BY ordinal_position",
            )
            .unwrap()
            .rows,
        vec![
            vec![
                s("plain_text"),
                s("text"),
                s("text"),
                SqlValue::Null,
                SqlValue::Int(1_073_741_824),
            ],
            vec![
                s("plain_varchar"),
                s("character varying"),
                s("varchar"),
                SqlValue::Null,
                SqlValue::Int(1_073_741_824),
            ],
            vec![
                s("sized_varchar"),
                s("character varying"),
                s("varchar"),
                SqlValue::Int(7),
                SqlValue::Int(28),
            ],
            vec![
                s("sized_varying"),
                s("character varying"),
                s("varchar"),
                SqlValue::Int(8),
                SqlValue::Int(32),
            ],
            vec![
                s("default_char"),
                s("character"),
                s("bpchar"),
                SqlValue::Int(1),
                SqlValue::Int(4),
            ],
            vec![
                s("sized_char"),
                s("character"),
                s("bpchar"),
                SqlValue::Int(4),
                SqlValue::Int(16),
            ],
            vec![
                s("default_character"),
                s("character"),
                s("bpchar"),
                SqlValue::Int(1),
                SqlValue::Int(4),
            ],
            vec![
                s("sized_character"),
                s("character"),
                s("bpchar"),
                SqlValue::Int(5),
                SqlValue::Int(20),
            ],
        ]
    );

    drop(session);
    drop(db);
    let mut reopened = BicDb::open(dir.path()).unwrap();
    assert_eq!(
        SqlSession::new(&mut reopened)
            .execute(
                "SELECT data_type, udt_name, character_maximum_length
                 FROM information_schema.columns
                 WHERE table_name = 'character_declarations'
                   AND column_name = 'sized_character'",
            )
            .unwrap()
            .rows,
        vec![vec![s("character"), s("bpchar"), SqlValue::Int(5)]]
    );
}

#[test]
fn character_typmods_enforce_assignment_and_explicit_cast_rules() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE character_lengths (
                id VARCHAR(3) PRIMARY KEY,
                varying VARCHAR(3),
                fixed CHAR(3)
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO character_lengths (id, varying, fixed)
             VALUES ('one', 'abc   ', 'abc   ')",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT id, varying, fixed FROM character_lengths")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("one".to_string()),
            SqlValue::String("abc".to_string()),
            SqlValue::String("abc".to_string()),
        ]]
    );
    session
        .execute("UPDATE character_lengths SET fixed = 'ab  ' WHERE id = 'one'")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT fixed::text, fixed::varchar FROM character_lengths")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("ab".to_string()),
            SqlValue::String("ab".to_string()),
        ]]
    );

    for sql in [
        "INSERT INTO character_lengths (id) VALUES ('toolong')",
        "INSERT INTO character_lengths (id, varying) VALUES ('two', 'abcd')",
        "INSERT INTO character_lengths (id, fixed) VALUES ('two', 'abcd')",
        "UPDATE character_lengths SET varying = 'abcd' WHERE id = 'one'",
    ] {
        assert_eq!(
            session.execute(sql).unwrap_err().sqlstate(),
            "22001",
            "{sql}"
        );
    }
    assert_eq!(
        session
            .copy_insert_rows(
                "character_lengths",
                &["id".to_string(), "varying".to_string()],
                vec![vec![Some("two".to_string()), Some("abcd".to_string())]],
            )
            .unwrap_err()
            .sqlstate(),
        "22001"
    );
    assert_eq!(
        session
            .execute(
                "SELECT 'abcdef'::VARCHAR(3), 'abcdef'::CHAR(3),
                        '😀éxy'::VARCHAR(3), '😀éxy'::CHAR(3)",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("abc".to_string()),
            SqlValue::String("abc".to_string()),
            SqlValue::String("😀éx".to_string()),
            SqlValue::String("😀éx".to_string()),
        ]]
    );

    session
        .execute("CREATE TABLE character_alter (value TEXT)")
        .unwrap();
    session
        .execute("INSERT INTO character_alter (value) VALUES ('abcdef')")
        .unwrap();
    assert_eq!(
        session
            .execute("ALTER TABLE character_alter ALTER COLUMN value TYPE VARCHAR(3)")
            .unwrap_err()
            .sqlstate(),
        "22001"
    );
    session
        .execute(
            "ALTER TABLE character_alter ALTER COLUMN value TYPE VARCHAR(3)
             USING value::VARCHAR(3)",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT value FROM character_alter")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("abc".to_string())]]
    );
}

#[test]
fn fixed_character_padding_comparison_and_unique_keys_match_postgresql() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE fixed_characters (
                id TEXT PRIMARY KEY,
                code CHAR(4) UNIQUE
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO fixed_characters (id, code)
             VALUES ('first', 'ab'), ('second', 'b')",
        )
        .unwrap();

    assert_eq!(
        session
            .execute("SELECT id, code FROM fixed_characters ORDER BY code")
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("first".to_string()),
                SqlValue::String("ab  ".to_string()),
            ],
            vec![
                SqlValue::String("second".to_string()),
                SqlValue::String("b   ".to_string()),
            ],
        ]
    );
    assert_eq!(
        session
            .execute(
                "SELECT id FROM fixed_characters
                 WHERE code = 'ab  '::CHAR(6)
                 ORDER BY id",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("first".to_string())]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT 'a'::CHAR(3) = 'a  '::CHAR(5),
                        'a'::CHAR(3) < 'b  '::CHAR(5)",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true), SqlValue::Bool(true)]]
    );
    assert_eq!(
        session
            .execute("INSERT INTO fixed_characters (id, code) VALUES ('duplicate', 'ab  ')")
            .unwrap_err()
            .sqlstate(),
        "23505"
    );
}

#[test]
fn builtin_collations_have_durable_column_and_catalog_identity() {
    let (dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE collation_columns (
                plain TEXT,
                c_text TEXT COLLATE \"C\",
                posix_text VARCHAR(8) COLLATE \"POSIX\"
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO collation_columns (plain, c_text, posix_text)
             VALUES ('z', 'z', 'z'), ('A', 'A', 'A'), ('a', 'a', 'a')",
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT c_text COLLATE \"C\" FROM collation_columns
                 ORDER BY c_text COLLATE \"C\"",
            )
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("A".to_string())],
            vec![SqlValue::String("a".to_string())],
            vec![SqlValue::String("z".to_string())],
        ]
    );
    session
        .execute(
            "CREATE INDEX collation_columns_c_idx
             ON collation_columns ((c_text COLLATE \"C\"))",
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT i.indcollation
                 FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
                 WHERE c.relname = 'collation_columns_c_idx'",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("950".to_string())]]
    );
    assert_eq!(
        session
            .execute("SELECT 'value' COLLATE \"missing_collation\"")
            .unwrap_err()
            .sqlstate(),
        "42704"
    );
    assert_eq!(
        session
            .execute("CREATE TABLE invalid_collation (value INTEGER COLLATE \"C\")")
            .unwrap_err()
            .sqlstate(),
        "42804"
    );
    assert_eq!(
        session
            .execute(
                "SELECT column_name, collation_schema, collation_name
                 FROM information_schema.columns
                 WHERE table_name = 'collation_columns'
                 ORDER BY ordinal_position",
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("plain".to_string()),
                SqlValue::Null,
                SqlValue::Null
            ],
            vec![
                SqlValue::String("c_text".to_string()),
                SqlValue::String("pg_catalog".to_string()),
                SqlValue::String("C".to_string()),
            ],
            vec![
                SqlValue::String("posix_text".to_string()),
                SqlValue::String("pg_catalog".to_string()),
                SqlValue::String("POSIX".to_string()),
            ],
        ]
    );
    assert_eq!(
        session
            .execute(
                "SELECT a.attname, a.attcollation
                 FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
                 WHERE c.relname = 'collation_columns' AND a.attnum > 0
                 ORDER BY a.attnum",
            )
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("plain".to_string()), SqlValue::Int(100)],
            vec![SqlValue::String("c_text".to_string()), SqlValue::Int(950)],
            vec![
                SqlValue::String("posix_text".to_string()),
                SqlValue::Int(951)
            ],
        ]
    );
    assert_eq!(
        session
            .execute(
                "SELECT oid, collname FROM pg_collation
                 WHERE collname IN ('default', 'C', 'POSIX', 'ucs_basic')
                 ORDER BY oid",
            )
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::Int(100), SqlValue::String("default".to_string())],
            vec![SqlValue::Int(950), SqlValue::String("C".to_string())],
            vec![SqlValue::Int(951), SqlValue::String("POSIX".to_string())],
            vec![
                SqlValue::Int(962),
                SqlValue::String("ucs_basic".to_string())
            ],
        ]
    );

    drop(session);
    drop(db);
    let mut reopened = BicDb::open(dir.path()).unwrap();
    assert_eq!(
        SqlSession::new(&mut reopened)
            .execute(
                "SELECT collation_name FROM information_schema.columns
                 WHERE table_name = 'collation_columns' AND column_name = 'c_text'",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("C".to_string())]]
    );
    assert_eq!(
        SqlSession::new(&mut reopened)
            .execute(
                "SELECT i.indcollation
                 FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
                 WHERE c.relname = 'collation_columns_c_idx'",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("950".to_string())]]
    );
}

#[test]
fn unicode_text_patterns_nul_and_locale_order_match_postgresql() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE unicode_values (
                id TEXT PRIMARY KEY,
                value TEXT COLLATE \"en-x-icu\"
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO unicode_values VALUES
             ('1', 'z'), ('2', 'ä'), ('3', 'a'),
             ('4', 'A'), ('5', 'é'), ('6', 'e')",
        )
        .unwrap();
    session
        .execute("CREATE UNIQUE INDEX unicode_values_locale_idx ON unicode_values (value)")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT value FROM unicode_values ORDER BY value",)
            .unwrap()
            .rows,
        ["a", "A", "ä", "e", "é", "z"]
            .into_iter()
            .map(|value| vec![SqlValue::String(value.to_string())])
            .collect::<Vec<_>>()
    );
    assert_eq!(
        session
            .execute(
                "SELECT 'ÄPFEL' ILIKE 'ä%',
                        'é' LIKE '_',
                        'a_b' LIKE 'a\\_b',
                        'a%b' LIKE 'a\\%b'",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT ('é' COLLATE \"C\") = ('é' COLLATE \"C\"),
                        ('é' COLLATE \"en-x-icu\") = ('é' COLLATE \"en-x-icu\"),
                        ('ä' COLLATE \"en-x-icu\") < ('z' COLLATE \"en-x-icu\"),
                        ('ä' COLLATE \"C\") < ('z' COLLATE \"C\")",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Bool(false),
            SqlValue::Bool(false),
            SqlValue::Bool(true),
            SqlValue::Bool(false),
        ]]
    );
    let nul_error = session.execute("SELECT chr(0)").unwrap_err();
    assert_eq!(nul_error.sqlstate(), "54000", "{nul_error:?}");
    assert_eq!(
        session
            .execute("SELECT 'a' LIKE 'a' ESCAPE 'xx'")
            .unwrap_err()
            .sqlstate(),
        "22025"
    );
    assert_eq!(
        session
            .execute("INSERT INTO unicode_values VALUES ('7', 'é')")
            .unwrap_err()
            .sqlstate(),
        "23505"
    );
}

#[test]
fn name_and_internal_char_match_postgresql_catalog_scalar_semantics() {
    let (dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    let s = |value: &str| SqlValue::String(value.to_string());
    let long_ascii = "a".repeat(63);
    let long_unicode = "é".repeat(40);
    session
        .execute(&format!(
            "CREATE TABLE catalog_scalars (
                id TEXT PRIMARY KEY,
                catalog_name NAME UNIQUE,
                kind \"char\"
            );
            CREATE INDEX catalog_scalars_kind_idx ON catalog_scalars (kind);
            INSERT INTO catalog_scalars VALUES
                ('ascii', '{}suffix', 'AB'),
                ('unicode', '{}', 'é'),
                ('empty', 'empty', '')",
            long_ascii, long_unicode,
        ))
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT id, length(catalog_name::text), octet_length(catalog_name::text),
                        kind::text, kind::int4
                 FROM catalog_scalars ORDER BY kind",
            )
            .unwrap()
            .rows,
        vec![
            vec![
                s("empty"),
                SqlValue::Int(5),
                SqlValue::Int(5),
                s(""),
                SqlValue::Int(0),
            ],
            vec![
                s("ascii"),
                SqlValue::Int(63),
                SqlValue::Int(63),
                s("A"),
                SqlValue::Int(65),
            ],
            vec![
                s("unicode"),
                SqlValue::Int(31),
                SqlValue::Int(62),
                s("\\303"),
                SqlValue::Int(-61),
            ],
        ]
    );
    assert_eq!(
        session
            .execute("SELECT (-128)::\"char\"::int4, 127::\"char\"::int4")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(-128), SqlValue::Int(127)]]
    );
    assert_eq!(
        session
            .execute("SELECT 128::\"char\"")
            .unwrap_err()
            .sqlstate(),
        "22003"
    );
    assert_eq!(
        session
            .execute(&format!(
                "INSERT INTO catalog_scalars VALUES ('duplicate', '{}other', 'D')",
                long_ascii,
            ))
            .unwrap_err()
            .sqlstate(),
        "23505"
    );

    drop(session);
    drop(db);
    let mut reopened = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut reopened);
    assert_eq!(
        session
            .execute(
                "SELECT catalog_name::text, kind::text, kind::int4
                 FROM catalog_scalars WHERE id = 'unicode'",
            )
            .unwrap()
            .rows,
        vec![vec![s(&long_unicode[..62]), s("\\303"), SqlValue::Int(-61)]]
    );
}

#[test]
fn insert_uses_non_sequence_column_defaults_when_columns_are_omitted() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE defaulted_rows (
                id TEXT PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                active BOOLEAN NOT NULL DEFAULT FALSE,
                tags TEXT[] NOT NULL DEFAULT '{}'
            );",
        )
        .unwrap();

    session
        .execute("INSERT INTO defaulted_rows (id) VALUES ('row-1');")
        .unwrap();
    let result = session
        .execute(
            "SELECT created_at, metadata, active, tags FROM defaulted_rows WHERE id = 'row-1';",
        )
        .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert!(!matches!(result.rows[0][0], SqlValue::Null));
    assert_eq!(
        result.rows[0][1..],
        [
            SqlValue::Json(json!({})),
            SqlValue::Bool(false),
            SqlValue::Json(json!([])),
        ]
    );
}

#[test]
fn insert_default_values_uses_sequence_and_column_defaults() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE signaling (id SERIAL PRIMARY KEY, date TIMESTAMP DEFAULT now())")
        .unwrap();

    assert_eq!(
        session
            .execute("INSERT INTO signaling DEFAULT VALUES")
            .unwrap()
            .command_complete_tag(),
        "INSERT 0 1"
    );

    let rows = session
        .execute("SELECT id, date FROM signaling ORDER BY id")
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0][0], SqlValue::Int(1));
    assert!(!matches!(rows.rows[0][1], SqlValue::Null));
}

#[test]
fn postgres_at_time_zone_defaults_and_values_are_accepted() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE utc_rows (
                id SERIAL PRIMARY KEY,
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT (now() at time zone 'UTC')
            )",
        )
        .unwrap();

    session
        .execute("INSERT INTO utc_rows DEFAULT VALUES")
        .unwrap();
    session
        .execute("INSERT INTO utc_rows (created_at) VALUES (now() at time zone 'UTC')")
        .unwrap();

    let rows = session
        .execute("SELECT id, created_at FROM utc_rows ORDER BY id")
        .unwrap();
    assert_eq!(rows.rows.len(), 2);
    assert_eq!(rows.rows[0][0], SqlValue::Int(1));
    assert_eq!(rows.rows[1][0], SqlValue::Int(2));
    assert!(!matches!(rows.rows[0][1], SqlValue::Null));
    assert!(!matches!(rows.rows[1][1], SqlValue::Null));
    assert!(matches!(
        &rows.rows[0][1],
        SqlValue::String(value)
            if value.len() >= 19 && value.as_bytes().get(4) == Some(&b'-')
    ));
}

#[test]
fn current_schema_regnamespace_cast_is_accepted_in_catalog_predicates() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE ir_module_module (id SERIAL PRIMARY KEY)")
        .unwrap();

    let result = session
        .execute(
            "SELECT c.relname
             FROM pg_class c
             WHERE c.relname IN ('ir_module_module')
               AND c.relkind IN ('r', 'v', 'm')
               AND c.relnamespace = current_schema::regnamespace",
        )
        .unwrap();

    assert_eq!(
        result.rows,
        vec![vec![SqlValue::String("ir_module_module".to_string())]]
    );
}

#[test]
fn odoo_auto_install_dependency_probe_uses_correlated_left_join() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE ir_module_module (
                id INTEGER PRIMARY KEY,
                name TEXT,
                auto_install BOOLEAN,
                state TEXT
            )",
        )
        .unwrap();
    session
        .execute(
            "CREATE TABLE ir_module_module_dependency (
                id INTEGER PRIMARY KEY,
                module_id INTEGER,
                name TEXT,
                auto_install_required BOOLEAN
            )",
        )
        .unwrap();
    session
        .execute(
            "CREATE INDEX ir_module_module_dependency_module_id_idx
             ON ir_module_module_dependency (module_id)",
        )
        .unwrap();
    session
        .execute("CREATE INDEX ir_module_module_name_idx ON ir_module_module (name)")
        .unwrap();

    session
        .execute(
            "INSERT INTO ir_module_module (id, name, auto_install, state) VALUES
                (1, 'base', FALSE, 'installed'),
                (2, 'good_optional', TRUE, 'installed'),
                (3, 'bad_missing', TRUE, 'installed'),
                (4, 'bad_state', TRUE, 'installed'),
                (5, 'installing_dep', FALSE, 'to install'),
                (6, 'good_required', TRUE, 'installed'),
                (7, 'already_to_install', TRUE, 'to install'),
                (8, 'uninstallable_mod', TRUE, 'uninstallable')",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO ir_module_module_dependency
                (id, module_id, name, auto_install_required) VALUES
                (1, 2, 'base', FALSE),
                (2, 3, 'missing_module', FALSE),
                (3, 4, 'base', TRUE),
                (4, 6, 'installing_dep', TRUE)",
        )
        .unwrap();

    let result = session
        .execute(
            "SELECT m.name FROM ir_module_module m
             WHERE m.auto_install
             AND state not in ('to install', 'uninstallable')
             AND NOT EXISTS (
                 SELECT 1 FROM ir_module_module_dependency d
                 LEFT JOIN ir_module_module mdep ON (d.name = mdep.name)
                 WHERE d.module_id = m.id
                   AND (
                       mdep.id IS NULL
                       OR (d.auto_install_required AND mdep.state != 'to install')
                   )
             )
             ORDER BY m.name",
        )
        .unwrap();

    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::String("good_optional".to_string())],
            vec![SqlValue::String("good_required".to_string())],
        ]
    );
}

#[test]
fn any_all_accept_postgres_text_array_literals() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    let result = session
        .execute(
            "SELECT
                'base' = any('{}') AS empty_any,
                'base' = any('{base,web}') AS text_any,
                'base' <> all('{}') AS empty_all",
        )
        .unwrap();

    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Bool(false),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
        ]]
    );
}

#[test]
fn odoo_parent_store_recursive_update_computes_parent_path() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE res_company (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER,
                parent_path VARCHAR
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO res_company (id, parent_id)
             VALUES (1, NULL), (2, 1), (3, 2)",
        )
        .unwrap();

    session
        .execute(
            "WITH RECURSIVE __parent_store_compute(id, parent_path) AS (
                SELECT row.id, concat(row.id, '/')
                FROM res_company row
                WHERE row.parent_id IS NULL
             UNION
                SELECT row.id, concat(comp.parent_path, row.id, '/')
                FROM res_company row, __parent_store_compute comp
                WHERE row.parent_id = comp.id
             )
             UPDATE res_company row SET parent_path = comp.parent_path
             FROM __parent_store_compute comp
             WHERE row.id = comp.id",
        )
        .unwrap();

    let rows = session
        .execute("SELECT id, parent_path FROM res_company ORDER BY id")
        .unwrap();
    assert_eq!(
        rows.rows,
        vec![
            vec![SqlValue::Int(1), SqlValue::String("1/".to_string())],
            vec![SqlValue::Int(2), SqlValue::String("1/2/".to_string())],
            vec![SqlValue::Int(3), SqlValue::String("1/2/3/".to_string())],
        ]
    );
}

#[test]
fn postgres_interval_literal_multiplication_supports_rate_limit_windows() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    let result = session
        .execute("SELECT 60::int * INTERVAL '1 second';")
        .unwrap();

    assert_eq!(
        result.rows,
        vec![vec![SqlValue::String("00:01:00".to_string())]]
    );
}

#[test]
fn postgres_jsonb_concat_merges_metadata_objects() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE schema_migrations (
                migration_name TEXT PRIMARY KEY,
                metadata JSONB NOT NULL DEFAULT '{}'::jsonb
            );",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO schema_migrations (migration_name, metadata)
             VALUES ('0001.sql', '{\"profile\":\"local\"}'::jsonb)
             ON CONFLICT (migration_name) DO UPDATE
             SET metadata = schema_migrations.metadata || EXCLUDED.metadata;",
        )
        .unwrap();

    session
        .execute(
            "INSERT INTO schema_migrations (migration_name, metadata)
             VALUES ('0001.sql', '{\"script\":\"apply\"}'::jsonb)
             ON CONFLICT (migration_name) DO UPDATE
             SET metadata = schema_migrations.metadata || EXCLUDED.metadata;",
        )
        .unwrap();

    assert_eq!(
        session
            .execute("SELECT metadata->>'profile', metadata->>'script' FROM schema_migrations")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("local".to_string()),
            SqlValue::String("apply".to_string()),
        ]]
    );
}

#[test]
fn postgres_jsonb_field_cast_to_text_supports_audit_queries() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE audit_log (
                id TEXT PRIMARY KEY,
                action TEXT NOT NULL,
                metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO audit_log (id, action, metadata)
             VALUES ('a1', 'auth.oauth.code.issue', '{\"client_id\":\"sample-demo-spa\"}'::jsonb);",
        )
        .unwrap();

    let result = session
        .execute(
            "SELECT action, metadata::text AS metadata
             FROM audit_log
             WHERE action IN ('auth.oauth.code.issue')
             ORDER BY created_at DESC
             LIMIT 20",
        )
        .unwrap();

    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::String("auth.oauth.code.issue".to_string()),
            SqlValue::String("{\"client_id\": \"sample-demo-spa\"}".to_string()),
        ]]
    );
}

#[test]
fn insert_on_conflict_update_returning_supports_auth_rate_limit_query() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE auth_rate_limits (
                bucket_key TEXT PRIMARY KEY,
                count INTEGER NOT NULL,
                reset_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            );",
        )
        .unwrap();

    let query = "
        INSERT INTO auth_rate_limits (bucket_key, count, reset_at, updated_at)
        VALUES ('metadata:127.0.0.1', 1, NOW() + (60::int * INTERVAL '1 second'), NOW())
        ON CONFLICT (bucket_key) DO UPDATE
        SET count = CASE
              WHEN auth_rate_limits.reset_at <= NOW() THEN 1
              ELSE auth_rate_limits.count + 1
            END,
            reset_at = CASE
              WHEN auth_rate_limits.reset_at <= NOW()
                THEN NOW() + (60::int * INTERVAL '1 second')
              ELSE auth_rate_limits.reset_at
            END,
            updated_at = NOW()
        RETURNING count, EXTRACT(EPOCH FROM (reset_at - NOW())) AS retry_after_seconds";

    let first = session.execute(query).unwrap();
    assert_eq!(first.columns, vec!["count", "retry_after_seconds"]);
    assert_eq!(first.rows.len(), 1);
    assert_eq!(first.rows[0][0], SqlValue::Int(1));
    assert!(first.rows[0][1]
        .to_cell()
        .parse::<f64>()
        .is_ok_and(|value| value >= 0.0));

    let second = session.execute(query).unwrap();
    assert_eq!(second.rows.len(), 1);
    assert_eq!(second.rows[0][0], SqlValue::Int(2));
    assert!(second.rows[0][1]
        .to_cell()
        .parse::<f64>()
        .is_ok_and(|value| value >= 0.0));
}

#[test]
fn insert_select_uses_query_rows_defaults_and_conflict_handling() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE ai_feature_settings (
                id BIGSERIAL PRIMARY KEY,
                feature INTEGER NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                ai_self_hosted_model_id BIGINT,
                provider TEXT NOT NULL
            )",
        )
        .unwrap();
    session
        .execute("CREATE UNIQUE INDEX index_ai_feature_settings_on_feature ON ai_feature_settings(feature)")
        .unwrap();
    session
        .execute(
            "INSERT INTO ai_feature_settings (
                feature,
                created_at,
                updated_at,
                ai_self_hosted_model_id,
                provider
            ) VALUES (16, NOW(), NOW(), 42, 'self_hosted')",
        )
        .unwrap();

    let copy_query = "
        INSERT INTO ai_feature_settings (
            created_at,
            updated_at,
            ai_self_hosted_model_id,
            feature,
            provider
        )
        SELECT
            NOW() AS created_at,
            NOW() AS updated_at,
            ai_self_hosted_model_id,
            17 AS feature,
            provider
        FROM ai_feature_settings
        WHERE feature = 16
        ON CONFLICT (feature) DO NOTHING";
    session.execute(copy_query).unwrap();
    session.execute(copy_query).unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT feature, ai_self_hosted_model_id, provider
                 FROM ai_feature_settings
                 ORDER BY feature"
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::Int(16),
                SqlValue::Int(42),
                SqlValue::String("self_hosted".to_string()),
            ],
            vec![
                SqlValue::Int(17),
                SqlValue::Int(42),
                SqlValue::String("self_hosted".to_string()),
            ],
        ]
    );

    session
        .execute(
            "CREATE TABLE insert_select_copies (
                id TEXT PRIMARY KEY,
                label TEXT NOT NULL,
                quantity INTEGER DEFAULT 7
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO insert_select_copies (id, label, quantity) VALUES ('seed', 'Seed', 3)",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO insert_select_copies
             SELECT 'copy', label, quantity
             FROM insert_select_copies
             WHERE id = 'seed'",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO insert_select_copies (id, label)
             SELECT 'defaulted', label
             FROM insert_select_copies
             WHERE id = 'seed'",
        )
        .unwrap();

    assert_eq!(
        session
            .execute("SELECT id, label, quantity FROM insert_select_copies ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("copy".to_string()),
                SqlValue::String("Seed".to_string()),
                SqlValue::Int(3),
            ],
            vec![
                SqlValue::String("defaulted".to_string()),
                SqlValue::String("Seed".to_string()),
                SqlValue::Int(7),
            ],
            vec![
                SqlValue::String("seed".to_string()),
                SqlValue::String("Seed".to_string()),
                SqlValue::Int(3),
            ],
        ]
    );

    session
        .execute(
            "CREATE TABLE nextcloud_migrations (
                app TEXT NOT NULL,
                version TEXT NOT NULL,
                CONSTRAINT nextcloud_migrations_unique UNIQUE (app, version)
            )",
        )
        .unwrap();
    let nextcloud_insert_if_missing = "
        INSERT INTO nextcloud_migrations (app, version)
        SELECT 'core', '35000Date20260527162338'
        FROM nextcloud_migrations
        WHERE app = 'core' AND version = '35000Date20260527162338'
        HAVING COUNT(*) = 0";
    assert_eq!(
        session
            .execute(nextcloud_insert_if_missing)
            .unwrap()
            .command_complete_tag(),
        "INSERT 0 1"
    );
    assert_eq!(
        session
            .execute(nextcloud_insert_if_missing)
            .unwrap()
            .command_complete_tag(),
        "INSERT 0 0"
    );
    assert_eq!(
        session
            .execute("SELECT app, version FROM nextcloud_migrations")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("core".to_string()),
            SqlValue::String("35000Date20260527162338".to_string()),
        ]]
    );
}

#[test]
fn non_id_primary_key_columns_project_and_filter_from_record_id() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE schema_migrations (migration_name TEXT PRIMARY KEY, dirty BOOLEAN)")
        .unwrap();
    session.execute("BEGIN").unwrap();
    session
        .execute(
            "INSERT INTO schema_migrations (migration_name, dirty)
             VALUES ('0001_initial.sql', FALSE);",
        )
        .unwrap();
    session.execute("COMMIT").unwrap();

    let result = session
        .execute(
            "SELECT migration_name, dirty
             FROM schema_migrations
             WHERE migration_name = '0001_initial.sql';",
        )
        .unwrap();

    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::String("0001_initial.sql".to_string()),
            SqlValue::Bool(false),
        ]]
    );

    let full_scan = session
        .execute("SELECT migration_name, dirty FROM schema_migrations;")
        .unwrap();
    assert_eq!(full_scan.rows, result.rows);
}

#[test]
fn declared_primary_key_columns_round_trip_through_query_and_cursor_paths() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE arbitrary_pk_values (
                pk_value INT PRIMARY KEY,
                payload TEXT
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO arbitrary_pk_values (pk_value, payload)
             VALUES (10, 'ten'), (2, 'two')",
        )
        .unwrap();

    assert_eq!(
        session
            .execute("SELECT pk_value FROM arbitrary_pk_values ORDER BY pk_value")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(2)], vec![SqlValue::Int(10)]]
    );
    assert_eq!(
        session
            .execute("SELECT pk_value FROM arbitrary_pk_values WHERE pk_value = 10")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(10)]]
    );
    assert_eq!(
        session
            .execute("SELECT MIN(pk_value), MAX(pk_value) FROM arbitrary_pk_values")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(2), SqlValue::Int(10)]]
    );

    session
        .execute(
            r#"
            CREATE PROCEDURE arbitrary_pk_cursor_probe(fetched_key OUT INTEGER)
            LANGUAGE 'plpgsql'
            AS $$
            DECLARE
                arbitrary_cursor CURSOR FOR
                    SELECT pk_value
                    FROM arbitrary_pk_values
                    ORDER BY pk_value;
            BEGIN
                OPEN arbitrary_cursor;
                FETCH arbitrary_cursor INTO fetched_key;
                CLOSE arbitrary_cursor;
            END;
            $$
            "#,
        )
        .unwrap();
    assert_eq!(
        session
            .execute("CALL arbitrary_pk_cursor_probe()")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(2)]]
    );
}

#[test]
fn erp_numeric_arithmetic_keeps_decimal_text_exact() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE ledger_lines (id TEXT PRIMARY KEY, debit NUMERIC, credit NUMERIC);")
        .unwrap();
    session
        .execute(
            "INSERT INTO ledger_lines (id, debit, credit) VALUES \
             ('l1', 0.10, 0.20), ('l2', '100.00'::numeric, '33.33'::numeric);",
        )
        .unwrap();

    let result = session
        .execute(
            "SELECT id, debit + credit AS total, debit * 3 AS tripled, debit - credit AS net \
             FROM ledger_lines ORDER BY id;",
        )
        .unwrap();

    assert_eq!(
        result.rows,
        vec![
            vec![
                SqlValue::String("l1".to_string()),
                SqlValue::String("0.30".to_string()),
                SqlValue::String("0.30".to_string()),
                SqlValue::String("-0.10".to_string()),
            ],
            vec![
                SqlValue::String("l2".to_string()),
                SqlValue::String("133.33".to_string()),
                SqlValue::String("300.00".to_string()),
                SqlValue::String("66.67".to_string()),
            ],
        ]
    );
}

#[test]
fn numeric_aggregates_accept_preserved_decimal_text_from_table_and_cte_rows() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE arbitrary_amounts (id INTEGER PRIMARY KEY, amount NUMERIC);")
        .unwrap();
    session
        .execute(
            "INSERT INTO arbitrary_amounts (id, amount) VALUES \
             (1, 0.0), (2, '1.25'::numeric), (3, '2.75'::numeric);",
        )
        .unwrap();

    assert_eq!(
        session
            .execute("SELECT SUM(amount) FROM arbitrary_amounts")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("4.00".to_string())]]
    );
    assert_eq!(
        session
            .execute("SELECT AVG(amount) FROM arbitrary_amounts")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("1.3333333333333333".to_string())]]
    );

    assert_eq!(
        session
            .execute(
                "WITH changed AS (
                    UPDATE arbitrary_amounts
                    SET amount = amount
                    WHERE id IN (1, 2)
                    RETURNING amount
                 )
                 SELECT SUM(amount) FROM changed",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("1.25".to_string())]]
    );
    assert_eq!(
        session
            .execute(
                "WITH changed AS (
                    SELECT amount
                    FROM arbitrary_amounts
                    WHERE id IN (1, 2)
                 )
                 SELECT SUM(amount) + '2.50'::numeric FROM changed",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("3.75".to_string())]]
    );
}

#[test]
fn integer_division_and_integer_casts_follow_postgres_numeric_semantics() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    let result = session
        .execute(
            "SELECT 1 / 2,
                    -1 / 2,
                    5 / 2,
                    CAST(1 / 2 AS INT),
                    CAST('0.5'::numeric AS INT),
                    CAST('1.5'::numeric AS INT),
                    CAST('-1.5'::numeric AS INT),
                    CAST(1.5::float8 AS INT),
                    CAST(-1.5::float8 AS INT)",
        )
        .unwrap();

    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Int(0),
            SqlValue::Int(0),
            SqlValue::Int(2),
            SqlValue::Int(0),
            SqlValue::Int(1),
            SqlValue::Int(2),
            SqlValue::Int(-2),
            SqlValue::Int(2),
            SqlValue::Int(-2),
        ]]
    );
}

#[test]
fn int2_and_int4_ranges_apply_to_casts_rows_defaults_arrays_and_arithmetic() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute("SELECT (-32768)::int2, 32767::int2, (-2147483648)::int4, 2147483647::int4")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(-32768),
            SqlValue::Int(32767),
            SqlValue::Int(-2147483648),
            SqlValue::Int(2147483647),
        ]]
    );

    for (sql, message) in [
        ("SELECT 32768::int2", "smallint out of range"),
        ("SELECT (-32769)::int2", "smallint out of range"),
        ("SELECT 2147483648::int4", "integer out of range"),
        ("SELECT (-2147483649)::int4", "integer out of range"),
        ("SELECT 32767::int2 + 1::int2", "smallint out of range"),
        ("SELECT 2147483647::int4 + 1::int4", "integer out of range"),
        (
            "SELECT 1 WHERE 32767::int2 + 1::int2 > 0",
            "smallint out of range",
        ),
        (
            "SELECT value FROM (VALUES (32767::int2)) AS limits(value)
             ORDER BY value + 1::int2",
            "smallint out of range",
        ),
        ("SELECT -((-32768)::int2)", "smallint out of range"),
        (
            "SELECT 1 WHERE -((-32768)::int2) > 0",
            "smallint out of range",
        ),
        ("SELECT '{1,32768}'::int2[]", "smallint out of range"),
    ] {
        let error = match session.execute(sql) {
            Ok(result) => panic!("expected integer range error for {sql}, got {result:?}"),
            Err(error) => error,
        };
        assert_eq!(error.sqlstate(), "22003", "{sql}: {error}");
        assert_eq!(error.to_string(), message, "{sql}");
    }

    session
        .execute(
            "CREATE TABLE invalid_integer_default (
                id TEXT PRIMARY KEY,
                small_value INT2 DEFAULT 32768
            )",
        )
        .unwrap();
    let default_error = session
        .execute("INSERT INTO invalid_integer_default (id) VALUES ('default')")
        .unwrap_err();
    assert_eq!(default_error.sqlstate(), "22003");
    assert_eq!(default_error.to_string(), "smallint out of range");

    session
        .execute(
            "CREATE TABLE integer_limits (
                id TEXT PRIMARY KEY,
                small_value INT2 DEFAULT 0,
                regular_value INT4
            )",
        )
        .unwrap();

    session
        .execute(
            "INSERT INTO integer_limits (id, small_value, regular_value)
             VALUES ('edge', 32767, 2147483647), ('min', -32768, 0)",
        )
        .unwrap();
    for sql in [
        "UPDATE integer_limits SET small_value = small_value + 1 WHERE id = 'edge'",
        "SELECT small_value + 1::int2 FROM integer_limits WHERE id = 'edge'",
        "SELECT id FROM integer_limits WHERE small_value + 1::int2 > 0",
        "SELECT id FROM integer_limits ORDER BY small_value + 1::int2",
        "SELECT id FROM integer_limits ORDER BY -small_value",
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), "22003", "{sql}: {error}");
        assert_eq!(error.to_string(), "smallint out of range");
    }

    let insert_error = session
        .execute(
            "INSERT INTO integer_limits (id, small_value, regular_value)
             VALUES ('overflow', 0, 2147483648)",
        )
        .unwrap_err();
    assert_eq!(insert_error.sqlstate(), "22003");
    assert_eq!(insert_error.to_string(), "integer out of range");
}

#[test]
fn int8_arithmetic_and_integer_aggregates_match_postgresql() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute(
                "SELECT (-9223372036854775808)::int8 % (-1)::int8,
                        7::int8 / (-3)::int8"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(0), SqlValue::Int(-2)]]
    );

    let promotions = session
        .execute(
            "SELECT 1::int2 + 1::int2,
                    1::int2 + 1::int4,
                    1::int4 + 1::int8,
                    1::int2 / 1::int2,
                    -1::int2",
        )
        .unwrap();
    assert_eq!(
        promotions.column_types,
        vec![
            Some("int2".to_string()),
            Some("int4".to_string()),
            Some("int8".to_string()),
            Some("int2".to_string()),
            Some("int2".to_string()),
        ]
    );
    for sql in [
        "SELECT 9223372036854775807::int8 + 1::int8",
        "SELECT (-9223372036854775808)::int8 - 1::int8",
        "SELECT (-9223372036854775808)::int8 / (-1)::int8",
        "SELECT -((-9223372036854775808)::int8)",
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), "22003", "{sql}: {error}");
        assert_eq!(error.to_string(), "bigint out of range", "{sql}");
    }

    let zero_error = session.execute("SELECT 1::int4 / 0::int4").unwrap_err();
    assert_eq!(zero_error.sqlstate(), "22012");
    assert_eq!(zero_error.to_string(), "division by zero");

    session
        .execute(
            "CREATE TABLE integer_aggregate_values (
                id INT4 PRIMARY KEY,
                small_value INT2,
                regular_value INT4,
                big_value INT8
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO integer_aggregate_values VALUES
                (1, 1, 1, 9223372036854775807),
                (2, 2, 2, 9223372036854775807)",
        )
        .unwrap();

    let aggregates = session
        .execute(
            "SELECT SUM(small_value), AVG(small_value),
                    SUM(regular_value), AVG(regular_value),
                    SUM(big_value), AVG(big_value)
             FROM integer_aggregate_values",
        )
        .unwrap();
    assert_eq!(
        aggregates.column_types,
        vec![
            Some("int8".to_string()),
            Some("numeric".to_string()),
            Some("int8".to_string()),
            Some("numeric".to_string()),
            Some("numeric".to_string()),
            Some("numeric".to_string()),
        ]
    );
    assert_eq!(
        aggregates.rows,
        vec![vec![
            SqlValue::Int(3),
            SqlValue::String("1.5000000000000000".to_string()),
            SqlValue::Int(3),
            SqlValue::String("1.5000000000000000".to_string()),
            SqlValue::String("18446744073709551614".to_string()),
            SqlValue::String("9223372036854775807".to_string()),
        ]]
    );

    assert_eq!(
        session
            .execute(
                "SELECT AVG(value)
                 FROM (VALUES (-1::int8), (0::int8)) AS values(value)"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(
            "-0.50000000000000000000".to_string()
        )]]
    );

    let rounded_averages = session
        .execute(
            "SELECT ROUND(AVG(regular_value)),
                    COALESCE(ROUND(AVG(big_value))::bigint, 0)
             FROM integer_aggregate_values
             WHERE id < 0",
        )
        .unwrap();
    assert_eq!(
        rounded_averages.rows,
        vec![vec![SqlValue::Null, SqlValue::Int(0)]]
    );

    assert_eq!(
        session
            .execute(
                "SELECT ROUND(NULL::numeric),
                        ROUND(NULL::double precision),
                        ROUND(NULL::numeric, 2)"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Null, SqlValue::Null, SqlValue::Null]]
    );
}

#[test]
fn floating_point_precision_special_values_and_errors_match_postgresql() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    let values = session
        .execute(
            "SELECT 0.1::float4, 0.1::float8,
                    0.1::float4 + 0.2::float4,
                    1::float4 + 1::int4",
        )
        .unwrap();
    assert_eq!(
        values.column_types,
        vec![
            Some("float4".to_string()),
            Some("float8".to_string()),
            Some("float4".to_string()),
            Some("float8".to_string()),
        ]
    );
    assert_eq!(
        values.rows,
        vec![vec![
            SqlValue::Float(f64::from(0.1_f32)),
            SqlValue::Float(0.1_f64),
            SqlValue::Float(f64::from(0.1_f32 + 0.2_f32)),
            SqlValue::Float(f64::from(1_f32) + 1_f64),
        ]]
    );

    assert_eq!(
        session
            .execute(
                "SELECT (1e6::float4)::text,
                        (1e-5::float8)::text,
                        (1e-4::float8)::text,
                        ('-0'::float8)::text,
                        ('Infinity'::float8)::text,
                        ('NaN'::float8)::text",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("1e+06".to_string()),
            SqlValue::String("1e-05".to_string()),
            SqlValue::String("0.0001".to_string()),
            SqlValue::String("-0".to_string()),
            SqlValue::String("Infinity".to_string()),
            SqlValue::String("NaN".to_string()),
        ]]
    );

    assert_eq!(
        session
            .execute(
                "SELECT 'NaN'::float8 = 'NaN'::float8,
                        'NaN'::float8 > 'Infinity'::float8,
                        'NaN'::float8 < 'Infinity'::float8,
                        '-0'::float8 = '0'::float8,
                        '-Infinity'::float8 < '-1'::float8"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
        ]]
    );

    session
        .execute(
            "CREATE TABLE float_order_values (
                label TEXT PRIMARY KEY,
                value FLOAT8,
                value_real FLOAT4
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO float_order_values VALUES
                ('negative_infinity', '-Infinity', '-Infinity'),
                ('negative_one', -1, -1),
                ('a_negative_zero', '-0', '-0'),
                ('b_positive_zero', '0', '0'),
                ('positive_one', 1, 1),
                ('positive_infinity', 'Infinity', 'Infinity'),
                ('nan_a', 'NaN', 'NaN'),
                ('nan_b', 'NaN', 'NaN')",
        )
        .unwrap();
    session
        .execute("CREATE INDEX float_order_value_idx ON float_order_values (value)")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT label FROM float_order_values ORDER BY value, label")
            .unwrap()
            .rows,
        [
            "negative_infinity",
            "negative_one",
            "a_negative_zero",
            "b_positive_zero",
            "positive_one",
            "positive_infinity",
            "nan_a",
            "nan_b",
        ]
        .into_iter()
        .map(|label| vec![SqlValue::String(label.to_string())])
        .collect::<Vec<_>>()
    );
    assert_eq!(
        session
            .execute("SELECT COUNT(DISTINCT value) FROM float_order_values")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(6)]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT label FROM float_order_values
                 WHERE value = 'NaN'::float8 ORDER BY label"
            )
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("nan_a".to_string())],
            vec![SqlValue::String("nan_b".to_string())],
        ]
    );
    assert_eq!(
        session
            .execute(
                "SELECT value_real::text, value::text
                 FROM float_order_values WHERE label = 'a_negative_zero'",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("-0".to_string()),
            SqlValue::String("-0".to_string()),
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT label FROM float_order_values
                 WHERE value > 'Infinity'::float8 ORDER BY label"
            )
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("nan_a".to_string())],
            vec![SqlValue::String("nan_b".to_string())],
        ]
    );

    for (sql, message) in [
        (
            "SELECT '3.4028236e38'::float4",
            "out of range for type real",
        ),
        ("SELECT '7e-46'::float4", "out of range for type real"),
        (
            "SELECT 3e38::float4 * 2::float4",
            "value out of range: overflow",
        ),
        (
            "SELECT 1e-30::float4 * 1e-20::float4",
            "value out of range: underflow",
        ),
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), "22003", "{sql}: {error}");
        assert!(error.to_string().contains(message), "{sql}: {error}");
    }
    let zero_error = session.execute("SELECT 1::float8 / 0::float8").unwrap_err();
    assert_eq!(zero_error.sqlstate(), "22012");
    assert_eq!(zero_error.to_string(), "division by zero");
}

#[test]
fn numeric_typmods_arithmetic_special_values_and_aggregates_match_postgresql() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute(
                "SELECT 1.235::numeric(4,2),
                        (-1.235)::numeric(4,2),
                        149::numeric(2,-1),
                        0.001234::numeric(2,4),
                        '1.2300e2'::numeric",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("1.24".to_string()),
            SqlValue::String("-1.24".to_string()),
            SqlValue::String("150".to_string()),
            SqlValue::String("0.0012".to_string()),
            SqlValue::String("123.00".to_string()),
        ]]
    );

    assert_eq!(
        session
            .execute(
                "SELECT 1.20::numeric + 2.3::numeric,
                        1.20::numeric - 2.3::numeric,
                        1.20::numeric * 2.30::numeric,
                        1.00::numeric / 3::numeric,
                        5.50::numeric % 2::numeric,
                        900719925474099301234567890.0100::numeric
                          + 0.0001::numeric",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("3.50".to_string()),
            SqlValue::String("-1.10".to_string()),
            SqlValue::String("2.7600".to_string()),
            SqlValue::String("0.33333333333333333333".to_string()),
            SqlValue::String("1.50".to_string()),
            SqlValue::String("900719925474099301234567890.0101".to_string()),
        ]]
    );

    assert_eq!(
        session
            .execute(
                "SELECT 'NaN'::numeric = 'NaN'::numeric,
                        'NaN'::numeric > 'Infinity'::numeric,
                        '-Infinity'::numeric < -1::numeric,
                        'Infinity'::numeric + '-Infinity'::numeric,
                        'Infinity'::numeric * 0::numeric,
                        2::numeric % 'Infinity'::numeric",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::String("NaN".to_string()),
            SqlValue::String("NaN".to_string()),
            SqlValue::String("2".to_string()),
        ]]
    );

    session
        .execute(
            "CREATE TABLE numeric_values (
                id TEXT PRIMARY KEY,
                constrained NUMERIC(6,2),
                rounded NUMERIC(2,-1),
                tiny NUMERIC(2,4),
                amount NUMERIC
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO numeric_values VALUES
                ('a', 12.345, 149, 0.001234, 1.20),
                ('b', -7.895, 151, 0.00949, 2.30)",
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT constrained, rounded, tiny
                 FROM numeric_values ORDER BY id",
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("12.35".to_string()),
                SqlValue::String("150".to_string()),
                SqlValue::String("0.0012".to_string()),
            ],
            vec![
                SqlValue::String("-7.90".to_string()),
                SqlValue::String("150".to_string()),
                SqlValue::String("0.0095".to_string()),
            ],
        ]
    );
    assert_eq!(
        session
            .execute(
                "SELECT SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM numeric_values"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("3.50".to_string()),
            SqlValue::String("1.7500000000000000".to_string()),
            SqlValue::String("1.20".to_string()),
            SqlValue::String("2.30".to_string()),
        ]]
    );

    for sql in [
        "SELECT 999::numeric(2,-1)",
        "SELECT 0.00999::numeric(2,4)",
        "SELECT 'Infinity'::numeric(4,2)",
        "INSERT INTO numeric_values VALUES ('overflow', 99999, 10, 0.001, 1)",
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), "22003", "{sql}: {error}");
        assert!(error.to_string().contains("numeric field overflow"));
    }
    let division_error = session
        .execute("SELECT 'Infinity'::numeric / 0::numeric")
        .unwrap_err();
    assert_eq!(division_error.sqlstate(), "22012");
}

#[test]
fn money_rounding_arithmetic_comparison_casts_and_overflow_match_postgresql() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute(
                "SELECT '$1,234.56'::money,
                        '(123.45)'::money,
                        '-$0.005'::money,
                        '$0.005'::money,
                        10.129::numeric::money,
                        (-0.005)::numeric::money"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("1234.56".into()),
            SqlValue::String("-123.45".into()),
            SqlValue::String("-0.01".into()),
            SqlValue::String("0.01".into()),
            SqlValue::String("10.13".into()),
            SqlValue::String("-0.01".into()),
        ]]
    );

    assert_eq!(
        session
            .execute(
                "SELECT 10::money + 2::money,
                        10::money - 2::money,
                        10::money * 2::int4,
                        2::numeric * 10::money,
                        10::money / 4::int4,
                        10::money / 4::money,
                        10::money::numeric,
                        10::money::text"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("12.00".into()),
            SqlValue::String("8.00".into()),
            SqlValue::String("20.00".into()),
            SqlValue::String("20.00".into()),
            SqlValue::String("2.50".into()),
            SqlValue::Float(2.5),
            SqlValue::String("10.00".into()),
            SqlValue::String("$10.00".into()),
        ]]
    );

    session
        .execute("CREATE TABLE money_values (id TEXT PRIMARY KEY, amount MONEY)")
        .unwrap();
    session
        .execute(
            "INSERT INTO money_values VALUES
             ('small', 2), ('large', 10), ('negative', -1)",
        )
        .unwrap();
    session
        .execute("CREATE INDEX money_values_amount_idx ON money_values (amount)")
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT id, amount, amount = 2::money, amount > 2::money
                 FROM money_values ORDER BY amount"
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("negative".into()),
                SqlValue::String("-1.00".into()),
                SqlValue::Bool(false),
                SqlValue::Bool(false),
            ],
            vec![
                SqlValue::String("small".into()),
                SqlValue::String("2.00".into()),
                SqlValue::Bool(true),
                SqlValue::Bool(false),
            ],
            vec![
                SqlValue::String("large".into()),
                SqlValue::String("10.00".into()),
                SqlValue::Bool(false),
                SqlValue::Bool(true),
            ],
        ]
    );

    assert_eq!(
        session
            .execute("SELECT id FROM money_values WHERE amount = 10::money")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("large".into())]]
    );
    assert_eq!(
        session
            .execute("SELECT SUM(amount), MIN(amount), MAX(amount) FROM money_values")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("11.00".into()),
            SqlValue::String("-1.00".into()),
            SqlValue::String("10.00".into()),
        ]]
    );
    assert_eq!(
        session
            .execute("SELECT AVG(amount) FROM money_values")
            .unwrap_err()
            .sqlstate(),
        "42883"
    );

    for sql in [
        "SELECT 92233720368547758.07::numeric::money + 0.01::money",
        "SELECT (-92233720368547758.08)::numeric::money - 0.01::money",
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), "22003");
        assert!(error.to_string().contains("money out of range"));
    }
    for sql in [
        "SELECT 1::money + 1::numeric",
        "SELECT 10::money % 3::money",
        "SELECT +1::money",
        "SELECT -(1::money)",
    ] {
        assert_eq!(session.execute(sql).unwrap_err().sqlstate(), "42883");
    }
    assert_eq!(
        session
            .execute("SELECT 10::money::int4")
            .unwrap_err()
            .sqlstate(),
        "42846"
    );
    assert_eq!(
        session
            .execute("SELECT 1::money / 0::int4")
            .unwrap_err()
            .sqlstate(),
        "22012"
    );
    session
        .execute("CREATE TABLE money_sum_overflow (id TEXT PRIMARY KEY, amount MONEY)")
        .unwrap();
    session
        .execute(
            "INSERT INTO money_sum_overflow VALUES
             ('max', 92233720368547758.07), ('cent', 0.01)",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT SUM(amount) FROM money_sum_overflow")
            .unwrap_err()
            .sqlstate(),
        "22003"
    );
}

#[test]
fn boolean_input_casts_logic_aggregates_and_indexes_match_postgresql() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute(
                "SELECT 'tr'::bool, 'ye'::bool, 'on'::bool, '1'::bool,
                        'fa'::bool, 'n'::bool, 'of'::bool, '0'::bool",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Bool(false),
            SqlValue::Bool(false),
            SqlValue::Bool(false),
        ]]
    );
    assert_eq!(
        session.execute("SELECT 'o'::bool").unwrap_err().sqlstate(),
        "22P02"
    );
    assert_eq!(
        session
            .execute(
                "SELECT true::int4, false::int4, 0::int4::bool,
                        2::int4::bool, (-1)::int4::bool, true::text",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(1),
            SqlValue::Int(0),
            SqlValue::Bool(false),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::String("true".into()),
        ]]
    );
    for sql in [
        "SELECT true::int2",
        "SELECT 1::int8::bool",
        "SELECT true::numeric",
    ] {
        assert_eq!(
            session.execute(sql).unwrap_err().sqlstate(),
            "42846",
            "{sql}"
        );
    }
    assert_eq!(
        session
            .execute(
                "SELECT false AND NULL, true AND NULL, false OR NULL, true OR NULL,
                        NOT NULL::bool, NULL::bool IS TRUE, NULL::bool IS FALSE,
                        NULL::bool IS UNKNOWN, NULL::bool IS NOT UNKNOWN",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Bool(false),
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Bool(true),
            SqlValue::Null,
            SqlValue::Bool(false),
            SqlValue::Bool(false),
            SqlValue::Bool(true),
            SqlValue::Bool(false),
        ]]
    );

    session
        .execute("CREATE TABLE boolean_values (id TEXT PRIMARY KEY, flag BOOLEAN)")
        .unwrap();
    session
        .execute(
            "INSERT INTO boolean_values VALUES ('false', 'of'), ('true', 'tr'), ('null', NULL)",
        )
        .unwrap();
    session
        .execute("CREATE INDEX boolean_values_flag_idx ON boolean_values (flag)")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT id FROM boolean_values WHERE flag = true ORDER BY id")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("true".into())]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT bool_and(flag), bool_or(flag), every(flag),
                        bool_and(DISTINCT flag), bool_or(DISTINCT flag)
                 FROM boolean_values",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Bool(false),
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Bool(false),
            SqlValue::Bool(true),
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT bool_and(flag), bool_or(flag), every(flag)
                 FROM boolean_values WHERE false",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Null, SqlValue::Null, SqlValue::Null]]
    );
}

#[test]
fn string_agg_orders_values_and_returns_null_for_empty_input() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE labels (id BIGINT PRIMARY KEY, label TEXT, separator TEXT)")
        .unwrap();
    session
        .execute(
            "INSERT INTO labels VALUES
             (1, 'bravo', ','),
             (2, NULL, ','),
             (3, 'alpha', NULL),
             (4, 'charlie', '|')",
        )
        .unwrap();

    let result = session
        .execute(
            "SELECT string_agg(label, separator ORDER BY label)
             FROM labels",
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::String("alpha,bravo|charlie".to_string())]]
    );
    assert_eq!(result.column_types, vec![Some("text".to_string())]);
    assert_eq!(
        session
            .execute("SELECT string_agg(label, ',') FROM labels WHERE false")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Null]]
    );
}

#[test]
fn substr_evaluates_in_scalar_and_update_assignment_contexts() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    let result = session
        .execute(
            "SELECT substr('abcdef', 1, 3),
                    substr('abcdef', 2, 3),
                    substr('abcdef', 0, 3),
                    substr('abcdef', -2, 4),
                    substr('abcdef', 4),
                    substr('abcdef', 2, 0)",
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::String("abc".to_string()),
            SqlValue::String("bcd".to_string()),
            SqlValue::String("ab".to_string()),
            SqlValue::String("a".to_string()),
            SqlValue::String("def".to_string()),
            SqlValue::String(String::new()),
        ]]
    );

    let error = session
        .execute("SELECT substr('abcdef', 2, -1)")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("negative substring length not allowed"));

    session
        .execute("CREATE TABLE arbitrary_notes (id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    session
        .execute("INSERT INTO arbitrary_notes (id, body) VALUES (1, 'tail')")
        .unwrap();
    session
        .execute(
            r#"
            CREATE PROCEDURE arbitrary_substr_update(
                arbitrary_prefix IN TEXT,
                updated_body OUT TEXT
            )
            LANGUAGE 'plpgsql'
            AS $$
            BEGIN
                UPDATE arbitrary_notes
                SET body = substr(arbitrary_prefix || body, 1, 5)
                WHERE id = 1
                RETURNING body INTO updated_body;
            END;
            $$
            "#,
        )
        .unwrap();

    assert_eq!(
        session
            .execute("CALL arbitrary_substr_update('abc')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("abcta".to_string())]]
    );
}

#[test]
fn to_char_evaluates_numeric_and_timestamp_values_in_update_assignments() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    let result = session
        .execute(
            "SELECT to_char(42.5::numeric, '9999.99'),
                    to_char(0.0::numeric, '9999.99'),
                    to_char(TIMESTAMP '2026-06-22 23:19:57', 'YYYYMMDDHH24MISS')",
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::String("   42.50".to_string()),
            SqlValue::String("     .00".to_string()),
            SqlValue::String("20260622231957".to_string()),
        ]]
    );

    session
        .execute("CREATE TABLE arbitrary_format_notes (id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    session
        .execute("INSERT INTO arbitrary_format_notes (id, body) VALUES (1, '')")
        .unwrap();
    session
        .execute(
            r#"
            CREATE PROCEDURE arbitrary_to_char_update(
                arbitrary_amount IN NUMERIC,
                arbitrary_ts IN TIMESTAMP,
                updated_body OUT TEXT
            )
            LANGUAGE 'plpgsql'
            AS $$
            BEGIN
                UPDATE arbitrary_format_notes
                SET body = to_char(arbitrary_amount, '9999.99') || ':' || to_char(arbitrary_ts, 'YYYYMMDDHH24MISS')
                WHERE id = 1
                RETURNING body INTO updated_body;
            END;
            $$
            "#,
        )
        .unwrap();

    assert_eq!(
        session
            .execute("CALL arbitrary_to_char_update(42.5, TIMESTAMP '2026-06-22 23:19:57')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(
            "   42.50:20260622231957".to_string()
        )]]
    );
}

#[test]
fn erp_date_interval_and_expression_subset() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE invoices (id TEXT PRIMARY KEY, due_on DATE, amount NUMERIC);")
        .unwrap();
    session
        .execute(
            "INSERT INTO invoices (id, due_on, amount) VALUES \
             ('i1', '2024-01-31'::date, 10.00), \
             ('i2', '2024-02-05'::date, 20.00), \
             ('i3', '2024-03-01'::date, 30.00);",
        )
        .unwrap();

    let result = session
        .execute(
            "SELECT id, due_on + '2 days'::interval AS shifted, NULLIF(amount, 20.00::numeric) AS kept \
             FROM invoices \
             WHERE due_on BETWEEN '2024-01-01'::date AND '2024-02-28'::date \
               AND amount = ANY(ARRAY[10.00::numeric, 20.00::numeric]) \
             ORDER BY id;",
        )
        .unwrap();

    assert_eq!(
        result.rows,
        vec![
            vec![
                SqlValue::String("i1".to_string()),
                SqlValue::String("2024-02-02".to_string()),
                SqlValue::String("10.00".to_string()),
            ],
            vec![
                SqlValue::String("i2".to_string()),
                SqlValue::String("2024-02-07".to_string()),
                SqlValue::Null,
            ],
        ]
    );

    let date_part = session
        .execute("SELECT date_part('month', '2024-02-07 03:04:05'::timestamp);")
        .unwrap();
    assert_eq!(date_part.rows, vec![vec![SqlValue::Int(2)]]);
}

#[test]
fn date_columns_accept_iso_midnight_roundtrips_and_return_canonical_dates() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE sync_cells (id TEXT PRIMARY KEY, due_date DATE)")
        .unwrap();
    session
        .execute("INSERT INTO sync_cells VALUES ('cell-a', '2026-08-01')")
        .unwrap();

    assert_eq!(
        session
            .execute("SELECT due_date FROM sync_cells WHERE id = 'cell-a'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("2026-08-01".to_string())]]
    );

    session
        .execute(
            "UPDATE sync_cells
             SET due_date = '2026-08-02T00:00:00.000Z'
             WHERE id = 'cell-a'",
        )
        .unwrap();

    assert_eq!(
        session
            .execute("SELECT due_date FROM sync_cells WHERE id = 'cell-a'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("2026-08-02".to_string())]]
    );
}

#[test]
fn failed_transactional_date_update_does_not_poison_row_until_restart() {
    let (_dir, mut db) = empty_test_db();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE poisoned_cells (id TEXT PRIMARY KEY, due_date DATE)")
            .unwrap();
        session
            .execute("INSERT INTO poisoned_cells VALUES ('cell-a', '2026-08-01')")
            .unwrap();
        session.execute("BEGIN").unwrap();
        assert!(session
            .execute(
                "UPDATE poisoned_cells
                 SET due_date = 'not-a-date'
                 WHERE id = 'cell-a'",
            )
            .is_err());
        session.execute("ROLLBACK").unwrap();
    }

    let mut retry = SqlSession::new(&mut db);
    retry
        .execute(
            "UPDATE poisoned_cells
             SET due_date = '2026-08-03'
             WHERE id = 'cell-a'",
        )
        .unwrap();
    assert_eq!(
        retry
            .execute("SELECT due_date FROM poisoned_cells WHERE id = 'cell-a'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("2026-08-03".to_string())]]
    );
}

#[test]
fn savepoint_rollback_clears_lock_from_failed_date_update() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE savepoint_cells (id TEXT PRIMARY KEY, due_date DATE)")
        .unwrap();
    session
        .execute("INSERT INTO savepoint_cells VALUES ('cell-a', '2026-08-01')")
        .unwrap();
    session.execute("BEGIN").unwrap();
    session.execute("SAVEPOINT before_bad_date").unwrap();
    assert!(session
        .execute(
            "UPDATE savepoint_cells
             SET due_date = 'not-a-date'
             WHERE id = 'cell-a'",
        )
        .is_err());
    session.execute("ROLLBACK TO before_bad_date").unwrap();
    session
        .execute(
            "UPDATE savepoint_cells
             SET due_date = '2026-08-05T00:00:00.000Z'
             WHERE id = 'cell-a'",
        )
        .unwrap();
    session.execute("COMMIT").unwrap();

    assert_eq!(
        session
            .execute("SELECT due_date FROM savepoint_cells WHERE id = 'cell-a'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("2026-08-05".to_string())]]
    );
}

#[test]
fn to_timestamp_parses_postgres_compact_timestamp_format() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    let timestamp = session
        .execute(
            "SELECT TO_TIMESTAMP('20260622174104','YYYYMMDDHH24MISS')::timestamp without time zone",
        )
        .unwrap();

    assert_eq!(
        timestamp.rows,
        vec![vec![SqlValue::String("2026-06-22 17:41:04".to_string())]]
    );
}

#[test]
fn to_timestamp_epoch_supports_auth_revocation_expiry_queries() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE revoked_access_tokens (
                jti TEXT PRIMARY KEY,
                expires_at TIMESTAMPTZ NOT NULL
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO revoked_access_tokens (jti, expires_at)
             VALUES ('token-1', TO_TIMESTAMP(4102444800))",
        )
        .unwrap();

    let result = session
        .execute(
            "SELECT 1
             FROM revoked_access_tokens
             WHERE jti = 'token-1' AND expires_at > NOW()
             LIMIT 1",
        )
        .unwrap();

    assert_eq!(result.rows, vec![vec![SqlValue::Int(1)]]);
}

#[test]
fn secure_sql_session_filters_reads_and_enforces_writes() {
    let (_dir, mut db) = empty_test_db();
    // An unrelated trigger must not move native-policy writes into the
    // explicit transaction path, where a signed mutation grant is required.
    SqlSession::new(&mut db).execute("CREATE TABLE unrelated_guarded (id INT PRIMARY KEY); CREATE FUNCTION unrelated_guard() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$; CREATE TRIGGER guard_row AFTER INSERT ON unrelated_guarded FOR EACH ROW EXECUTE FUNCTION unrelated_guard()").unwrap();
    db.create_collection_with_policy("patients", CollectionMode::Standard, tenant_policy())
        .unwrap();
    let ctx_a = tenant_ctx("tenant-a", &["reader", "writer"]);
    let ctx_b = tenant_ctx("tenant-b", &["reader", "writer"]);
    db.secure(&ctx_a)
        .insert(
            "patients",
            Record::new("p-a").with_metadata(json!({"tenant_id": "tenant-a", "name": "Ada"})),
        )
        .unwrap();
    db.secure(&ctx_b)
        .insert(
            "patients",
            Record::new("p-b").with_metadata(json!({"tenant_id": "tenant-b", "name": "Bea"})),
        )
        .unwrap();

    let mut raw = SqlSession::new(&mut db);
    assert!(matches!(
        raw.execute("SELECT id FROM patients"),
        Err(SqlError::BicDb(BicDbError::Authorization(_)))
    ));
    drop(raw);

    let mut session = SqlSession::new_secure(&mut db, ctx_a.clone());
    let result = session
        .execute("SELECT id FROM patients ORDER BY id")
        .unwrap();
    assert_eq!(result.rows, vec![vec![SqlValue::String("p-a".to_string())]]);
    session
        .execute("INSERT INTO patients (id, tenant_id, name) VALUES ('p-c', 'tenant-a', 'Cal')")
        .unwrap();
    assert!(matches!(
        session.execute("INSERT INTO patients (id, tenant_id) VALUES ('p-x', 'tenant-b')"),
        Err(SqlError::BicDb(BicDbError::Authorization(_)))
    ));
}

#[test]
fn regexp_replace_matches_postgres_scalar_behavior() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute(
                "SELECT
                    regexp_replace('Phone: +1 (206) 555-0123', '[^0-9+]', '', 'g'),
                    regexp_replace('one two two', 'two', 'three'),
                    regexp_replace('abc123', '([a-z]+)([0-9]+)', '\\2-\\1'),
                    regexp_replace(NULL, 'x', 'y', 'g')"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("+12065550123".to_string()),
            SqlValue::String("one three two".to_string()),
            SqlValue::String("123-abc".to_string()),
            SqlValue::Null,
        ]]
    );
}

#[test]
fn split_part_matches_postgres_scalar_behavior() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute(
                "SELECT
                    split_part('abc.def.ghi', '.', 1),
                    split_part('abc.def.ghi', '.', 2),
                    split_part('abc.def.ghi', '.', 4),
                    split_part('abc.def.ghi', '.', -1),
                    split_part('abc.def.ghi', '.', -4),
                    split_part('abc', '', 1),
                    split_part('abc', '', 2),
                    split_part('a::b::c', '::', 2),
                    split_part('a§b§c', '§', -2),
                    split_part(NULL, '.', 1),
                    split_part('abc', NULL, 1),
                    split_part('abc', '.', NULL)"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("abc".to_string()),
            SqlValue::String("def".to_string()),
            SqlValue::String(String::new()),
            SqlValue::String("ghi".to_string()),
            SqlValue::String(String::new()),
            SqlValue::String("abc".to_string()),
            SqlValue::String(String::new()),
            SqlValue::String("b".to_string()),
            SqlValue::String("b".to_string()),
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
        ]]
    );

    let error = session
        .execute("SELECT split_part('abc', '.', 0)")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "22023");
    assert!(error
        .to_string()
        .contains("field position must not be zero"));
}

#[test]
fn split_part_works_in_the_ehr_activity_feed_query_shape() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE carrier_audit_log (
                id TEXT PRIMARY KEY,
                action TEXT NOT NULL,
                \"current_user\" JSONB
            );
            INSERT INTO carrier_audit_log (id, action, \"current_user\")
            VALUES ('audit-1', 'break_glass.started', '{\"tenant_id\":\"tenant-a\"}');",
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT split_part(a.action, '.', 1), a.\"current_user\" ->> 'tenant_id'
                 FROM carrier_audit_log a"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("break_glass".to_string()),
            SqlValue::String("tenant-a".to_string()),
        ]]
    );

    // PostgreSQL also treats an unquoted qualified CURRENT_USER token as the
    // column belonging to the qualifier, rather than the session keyword.
    assert_eq!(
        session
            .execute(
                "SELECT a.current_user ->> 'tenant_id'
                 FROM carrier_audit_log a"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("tenant-a".to_string())]]
    );
}

#[test]
fn boolean_scalar_subquery_is_valid_as_a_where_predicate() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE audit_items (id BIGINT PRIMARY KEY);
             INSERT INTO audit_items VALUES (1), (2);",
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "WITH role_flags AS (SELECT true AS admin_access)
                 SELECT id
                 FROM audit_items
                 WHERE (SELECT admin_access FROM role_flags)
                 ORDER BY id"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(2)]]
    );
}
