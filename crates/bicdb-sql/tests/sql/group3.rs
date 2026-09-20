//! Test group split from the former monolithic tests/sql.rs.
use super::*;

#[test]
fn inner_join_with_on_predicate() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT)")
            .unwrap();
        session
            .execute(
                "CREATE TABLE appointments (id TEXT PRIMARY KEY, patient_id TEXT, doctor TEXT)",
            )
            .unwrap();
        session
            .execute("INSERT INTO patients (id, name) VALUES ('p1', 'John'), ('p2', 'Ada')")
            .unwrap();
        session
            .execute(
                "INSERT INTO appointments (id, patient_id, doctor) VALUES ('a1', 'p1', 'Dr. Rao'), ('a2', 'p2', 'Dr. Kim')",
            )
            .unwrap();
    }

    let result = SqlEngine::new(&db)
        .execute(
            "SELECT patients.name, appointments.doctor FROM patients JOIN appointments ON patients.id = appointments.patient_id WHERE appointments.doctor = 'Dr. Rao'",
        )
        .unwrap();

    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::String("John".to_string()),
            SqlValue::String("Dr. Rao".to_string()),
        ]]
    );
}

#[test]
fn qualified_wildcard_projection_expands_table_columns() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT)")
            .unwrap();
        session
            .execute(
                "CREATE TABLE appointments (id TEXT PRIMARY KEY, patient_id TEXT, doctor TEXT)",
            )
            .unwrap();
        session
            .execute("INSERT INTO patients (id, name) VALUES ('p1', 'John'), ('p2', 'Ada')")
            .unwrap();
        session
            .execute(
                "INSERT INTO appointments (id, patient_id, doctor) VALUES ('a1', 'p1', 'Dr. Rao'), ('a2', 'p2', 'Dr. Kim')",
            )
            .unwrap();
    }

    let direct = SqlEngine::new(&db)
        .execute("SELECT patients.* FROM patients WHERE patients.id = 'p1'")
        .unwrap();
    assert_eq!(direct.columns, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(
        direct.rows,
        vec![vec![
            SqlValue::String("p1".to_string()),
            SqlValue::String("John".to_string()),
        ]]
    );

    let joined = SqlEngine::new(&db)
        .execute(
            "SELECT p.*, appointments.doctor \
             FROM patients p JOIN appointments ON p.id = appointments.patient_id \
             WHERE appointments.doctor = 'Dr. Rao'",
        )
        .unwrap();
    assert_eq!(
        joined.columns,
        vec!["id".to_string(), "name".to_string(), "doctor".to_string()]
    );
    assert_eq!(
        joined.rows,
        vec![vec![
            SqlValue::String("p1".to_string()),
            SqlValue::String("John".to_string()),
            SqlValue::String("Dr. Rao".to_string()),
        ]]
    );
}

#[test]
fn outer_joins_null_extend_unmatched_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT)")
            .unwrap();
        session
            .execute(
                "CREATE TABLE appointments (id TEXT PRIMARY KEY, patient_id TEXT, doctor TEXT)",
            )
            .unwrap();
        session
            .execute("INSERT INTO patients (id, name) VALUES ('p1', 'John'), ('p2', 'Ada')")
            .unwrap();
        session
            .execute(
                "INSERT INTO appointments (id, patient_id, doctor) VALUES ('a1', 'p1', 'Dr. Rao'), ('a2', 'p3', 'Dr. Kim')",
            )
            .unwrap();
    }

    let left = SqlEngine::new(&db)
        .execute(
            "SELECT patients.id, patients.name, appointments.doctor FROM patients LEFT JOIN appointments ON patients.id = appointments.patient_id ORDER BY patients.id",
        )
        .unwrap();
    assert_eq!(left.columns, ["id", "name", "doctor"]);
    assert_eq!(
        left.rows,
        vec![
            vec![
                SqlValue::String("p1".to_string()),
                SqlValue::String("John".to_string()),
                SqlValue::String("Dr. Rao".to_string()),
            ],
            vec![
                SqlValue::String("p2".to_string()),
                SqlValue::String("Ada".to_string()),
                SqlValue::Null,
            ],
        ]
    );

    let right = SqlEngine::new(&db)
        .execute(
            "SELECT patients.name, appointments.id, appointments.doctor FROM patients RIGHT JOIN appointments ON patients.id = appointments.patient_id ORDER BY appointments.id",
        )
        .unwrap();
    assert_eq!(
        right.rows,
        vec![
            vec![
                SqlValue::String("John".to_string()),
                SqlValue::String("a1".to_string()),
                SqlValue::String("Dr. Rao".to_string()),
            ],
            vec![
                SqlValue::Null,
                SqlValue::String("a2".to_string()),
                SqlValue::String("Dr. Kim".to_string()),
            ],
        ]
    );

    let full = SqlEngine::new(&db)
        .execute(
            "SELECT patients.id, appointments.id, appointments.doctor FROM patients FULL OUTER JOIN appointments ON patients.id = appointments.patient_id ORDER BY appointments.id",
        )
        .unwrap();
    assert_eq!(
        full.rows,
        vec![
            vec![
                SqlValue::String("p1".to_string()),
                SqlValue::String("a1".to_string()),
                SqlValue::String("Dr. Rao".to_string()),
            ],
            vec![
                SqlValue::Null,
                SqlValue::String("a2".to_string()),
                SqlValue::String("Dr. Kim".to_string()),
            ],
            vec![
                SqlValue::String("p2".to_string()),
                SqlValue::Null,
                SqlValue::Null,
            ],
        ]
    );
}

#[test]
fn where_predicates_can_filter_nullable_outer_join_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT)")
            .unwrap();
        session
            .execute(
                "CREATE TABLE appointments (id TEXT PRIMARY KEY, patient_id TEXT, doctor TEXT)",
            )
            .unwrap();
        session
            .execute("INSERT INTO patients (id, name) VALUES ('p1', 'John'), ('p2', 'Ada')")
            .unwrap();
        session
            .execute(
                "INSERT INTO appointments (id, patient_id, doctor) VALUES ('a1', 'p1', 'Dr. Rao')",
            )
            .unwrap();
    }

    let result = SqlEngine::new(&db)
        .execute(
            "SELECT patients.id FROM patients LEFT JOIN appointments ON patients.id = appointments.patient_id WHERE appointments.id IS NULL",
        )
        .unwrap();

    assert_eq!(result.rows, vec![vec![SqlValue::String("p2".to_string())]]);
}

#[test]
fn right_join_null_filter_documents_current_null_extension_behavior() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT)")
            .unwrap();
        session
            .execute(
                "CREATE TABLE appointments (id TEXT PRIMARY KEY, patient_id TEXT, doctor TEXT)",
            )
            .unwrap();
        session
            .execute("INSERT INTO patients (id, name) VALUES ('p1', 'John'), ('p2', 'Ada')")
            .unwrap();
        session
            .execute(
                "INSERT INTO appointments (id, patient_id, doctor) VALUES ('a1', 'p1', 'Dr. Rao'), ('a2', 'missing', 'Dr. Null')",
            )
            .unwrap();
    }

    let result = SqlEngine::new(&db)
        .execute(
            "SELECT patients.name, appointments.doctor \
             FROM patients RIGHT JOIN appointments ON patients.id = appointments.patient_id \
             WHERE patients.id IS NULL ORDER BY appointments.id",
        )
        .unwrap();

    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Null, SqlValue::String("Dr. Rao".to_string())],
            vec![SqlValue::Null, SqlValue::String("Dr. Null".to_string())],
        ]
    );
}

#[test]
fn simple_in_and_scalar_subqueries() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT)")
            .unwrap();
        session
            .execute(
                "CREATE TABLE appointments (id TEXT PRIMARY KEY, patient_id TEXT, doctor TEXT)",
            )
            .unwrap();
        session
            .execute("INSERT INTO patients (id, name) VALUES ('p1', 'John'), ('p2', 'Ada')")
            .unwrap();
        session
            .execute(
                "INSERT INTO appointments (id, patient_id, doctor) VALUES ('a1', 'p1', 'Dr. Rao'), ('a2', 'p2', 'Dr. Kim')",
            )
            .unwrap();
    }

    let engine = SqlEngine::new(&db);
    let in_result = engine
        .execute(
            "SELECT id FROM patients WHERE id IN (SELECT patient_id FROM appointments WHERE doctor = 'Dr. Rao')",
        )
        .unwrap();
    assert_eq!(
        in_result.rows,
        vec![vec![SqlValue::String("p1".to_string())]]
    );

    let scalar_result = engine
        .execute(
            "SELECT name FROM patients WHERE id = (SELECT patient_id FROM appointments WHERE id = 'a2')",
        )
        .unwrap();
    assert_eq!(
        scalar_result.rows,
        vec![vec![SqlValue::String("Ada".to_string())]]
    );
}

#[test]
fn correlated_scalar_subquery_resolves_outer_columns_generically() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE arbitrary_outer_keys (
                outer_key INT PRIMARY KEY,
                label TEXT
            )",
        )
        .unwrap();
    session
        .execute(
            "CREATE TABLE arbitrary_inner_scores (
                inner_key INT PRIMARY KEY,
                outer_ref INT,
                score INT
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO arbitrary_outer_keys (outer_key, label)
             VALUES (1, 'first'), (2, 'second')",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO arbitrary_inner_scores (inner_key, outer_ref, score)
             VALUES (10, 1, 5), (11, 1, 3), (12, 2, 9)",
        )
        .unwrap();

    let result = session
        .execute(
            "SELECT parent.outer_key,
                    (SELECT MIN(child.score)
                     FROM arbitrary_inner_scores AS child
                     WHERE child.outer_ref = parent.outer_key) AS lowest_score
             FROM arbitrary_outer_keys AS parent
             ORDER BY parent.outer_key",
        )
        .unwrap();

    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Int(1), SqlValue::Int(3)],
            vec![SqlValue::Int(2), SqlValue::Int(9)],
        ]
    );
}

#[test]
fn no_from_coalesce_executes_scalar_subquery_for_feature_flags() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE hub_feature_flags (
                org_id TEXT,
                hub_id TEXT,
                feature TEXT,
                is_enabled BOOLEAN
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO hub_feature_flags (org_id, hub_id, feature, is_enabled)
             VALUES ('org-1', 'hub-1', 'apps', false)",
        )
        .unwrap();

    let engine = SqlEngine::new(&db);
    let disabled = engine
        .execute(
            "SELECT COALESCE((
                SELECT is_enabled
                FROM hub_feature_flags
                WHERE org_id = 'org-1'
                  AND hub_id = 'hub-1'
                  AND feature::text = 'apps'
                LIMIT 1
             ), true)",
        )
        .unwrap();
    assert_eq!(disabled.rows, vec![vec![SqlValue::Bool(false)]]);

    let missing = engine
        .execute(
            "SELECT COALESCE((
                SELECT is_enabled
                FROM hub_feature_flags
                WHERE org_id = 'org-1'
                  AND hub_id = 'hub-1'
                  AND feature::text = 'missing'
                LIMIT 1
             ), true)",
        )
        .unwrap();
    assert_eq!(missing.rows, vec![vec![SqlValue::Bool(true)]]);
}

#[test]
fn invalid_collection_errors() {
    let (_dir, db) = test_db();
    let error = SqlEngine::new(&db)
        .execute("SELECT * FROM missing")
        .unwrap_err();

    assert!(error.to_string().contains("collection not found"));
}

#[test]
fn invalid_sql_errors() {
    let (_dir, db) = test_db();
    let error = SqlEngine::new(&db).execute("SELECT FROM").unwrap_err();

    assert!(error.to_string().contains("invalid SQL"));
}

#[test]
fn empty_semicolon_only_sql_is_accepted() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(session.execute(";").unwrap(), SqlResult::empty(Vec::new()));
    assert_eq!(
        SqlEngine::new(&db).execute(" /* comment */ ; ").unwrap(),
        SqlResult::empty(Vec::new())
    );
}

#[test]
fn empty_result_keeps_columns() {
    let (_dir, db) = test_db();
    let result = SqlEngine::new(&db)
        .execute("SELECT id FROM patients WHERE id = 'missing'")
        .unwrap();

    assert_eq!(result.columns, ["id"]);
    assert!(result.rows.is_empty());
}

#[test]
fn postgres_metadata_helpers() {
    let (_dir, db) = test_db();
    let engine = SqlEngine::new(&db);

    assert_eq!(
        engine.execute("SELECT 1;").unwrap().rows,
        vec![vec![SqlValue::Int(1)]]
    );
    assert_eq!(
        engine.execute("SHOW server_version").unwrap().columns,
        ["server_version"]
    );
    assert_eq!(
        engine
            .execute("SELECT table_name FROM information_schema.tables")
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("bicdb_graph_edges".to_string())],
            vec![SqlValue::String("bicdb_graph_nodes".to_string())],
            vec![SqlValue::String("patients".to_string())],
            vec![SqlValue::String("wearable".to_string())],
        ]
    );
    assert_eq!(
        engine
            .execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'patients'"
            )
            .unwrap()
            .rows
            .len(),
        5
    );
}

#[test]
fn postgres_client_encoding_set_is_accepted() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute("SET client_encoding = 'UTF8'")
            .unwrap()
            .command_tag
            .as_deref(),
        Some("SET")
    );
    assert_eq!(
        session.execute("SHOW client_encoding").unwrap().rows,
        vec![vec![SqlValue::String("UTF8".to_string())]]
    );
}

#[test]
fn postgres_application_name_set_show_and_reset_are_supported() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("SET application_name = 'PostgreSQL JDBC Driver'")
        .unwrap();
    assert_eq!(
        session.execute("SHOW application_name").unwrap().rows,
        vec![vec![SqlValue::String("PostgreSQL JDBC Driver".to_string())]]
    );

    session.execute("RESET application_name").unwrap();
    assert_eq!(
        session.execute("SHOW application_name").unwrap().rows,
        vec![vec![SqlValue::String(String::new())]]
    );
}

#[test]
fn postgres_current_schemas_function_is_supported() {
    let (_dir, db) = empty_test_db();
    let engine = SqlEngine::new(&db);

    assert_eq!(
        engine.execute("SELECT current_schema").unwrap().rows,
        vec![vec![SqlValue::String("public".to_string())]]
    );
    assert_eq!(
        engine.execute("SELECT current_database").unwrap().rows,
        vec![vec![SqlValue::String("bicdb".to_string())]]
    );
    assert_eq!(
        engine
            .execute("SELECT current_schemas(false)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Json(json!(["public"]))]]
    );
    assert_eq!(
        engine.execute("SELECT current_schemas(true)").unwrap().rows,
        vec![vec![SqlValue::Json(json!(["pg_catalog", "public"]))]]
    );
}

#[test]
fn postgres_client_min_messages_set_is_accepted() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute("SET client_min_messages = 'warning'")
            .unwrap()
            .command_tag
            .as_deref(),
        Some("SET")
    );
    assert_eq!(
        session.execute("SHOW client_min_messages").unwrap().rows,
        vec![vec![SqlValue::String("warning".to_string())]]
    );
}

#[test]
fn postgres_standard_conforming_strings_set_is_accepted() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute("SET standard_conforming_strings = on")
            .unwrap()
            .command_tag
            .as_deref(),
        Some("SET")
    );
    assert_eq!(
        session
            .execute("SHOW standard_conforming_strings")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("on".to_string())]]
    );
}

#[test]
fn postgres_intervalstyle_set_is_accepted() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute("SET intervalstyle = iso_8601")
            .unwrap()
            .command_tag
            .as_deref(),
        Some("SET")
    );
    assert_eq!(
        session.execute("SHOW intervalstyle").unwrap().rows,
        vec![vec![SqlValue::String("iso_8601".to_string())]]
    );
}

#[test]
fn postgres_datestyle_set_is_accepted() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute("SET DATESTYLE = ISO")
            .unwrap()
            .command_tag
            .as_deref(),
        Some("SET")
    );
    assert_eq!(
        session.execute("SHOW datestyle").unwrap().rows,
        vec![vec![SqlValue::String("ISO, MDY".to_string())]]
    );

    session.execute("SET datestyle = 'SQL, DMY'").unwrap();
    assert_eq!(
        session.execute("SHOW datestyle").unwrap().rows,
        vec![vec![SqlValue::String("SQL, DMY".to_string())]]
    );

    assert_eq!(
        session
            .execute("RESET datestyle")
            .unwrap()
            .command_tag
            .as_deref(),
        Some("RESET")
    );
    assert_eq!(
        session.execute("SHOW datestyle").unwrap().rows,
        vec![vec![SqlValue::String("ISO, MDY".to_string())]]
    );
}

#[test]
fn postgres_pg_dump_session_settings_are_accepted() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "SET extra_float_digits TO 3;
             SET synchronize_seqscans TO off;
             SET row_security = off;
             SET default_tablespace = '';
             SET default_table_access_method = heap;
             SET search_path TO 'pg_catalog', 'pg_temp';
             SET check_function_bodies = false;
             SET xmloption = content;
             SET transaction_timeout = 0",
        )
        .unwrap();

    assert_eq!(
        session.execute("SHOW extra_float_digits").unwrap().rows,
        vec![vec![SqlValue::String("3".to_string())]]
    );
    assert_eq!(
        session.execute("SHOW synchronize_seqscans").unwrap().rows,
        vec![vec![SqlValue::String("off".to_string())]]
    );
    assert_eq!(
        session.execute("SHOW row_security").unwrap().rows,
        vec![vec![SqlValue::String("off".to_string())]]
    );
    assert_eq!(
        session.execute("SHOW search_path").unwrap().rows,
        vec![vec![SqlValue::String("pg_catalog, pg_temp".to_string())]]
    );
    assert_eq!(
        session.execute("SHOW default_tablespace").unwrap().rows,
        vec![vec![SqlValue::String(String::new())]]
    );
    assert_eq!(
        session
            .execute("SHOW default_table_access_method")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("heap".to_string())]]
    );
    assert_eq!(
        session.execute("SHOW transaction_timeout").unwrap().rows,
        vec![vec![SqlValue::String("0".to_string())]]
    );
    assert_eq!(
        session.execute("SHOW check_function_bodies").unwrap().rows,
        vec![vec![SqlValue::String("off".to_string())]]
    );
    assert_eq!(
        session.execute("SHOW xmloption").unwrap().rows,
        vec![vec![SqlValue::String("content".to_string())]]
    );

    session
        .execute(
            "RESET extra_float_digits; RESET synchronize_seqscans; RESET row_security; RESET search_path"
        )
        .unwrap();
    assert_eq!(
        session.execute("SHOW extra_float_digits").unwrap().rows,
        vec![vec![SqlValue::String("1".to_string())]]
    );
    assert_eq!(
        session.execute("SHOW synchronize_seqscans").unwrap().rows,
        vec![vec![SqlValue::String("on".to_string())]]
    );
    assert_eq!(
        session.execute("SHOW row_security").unwrap().rows,
        vec![vec![SqlValue::String("on".to_string())]]
    );
    assert_eq!(
        session.execute("SHOW search_path").unwrap().rows,
        vec![vec![SqlValue::String("\"$user\", public".to_string())]]
    );
}

#[test]
fn postgres_pg_settings_supports_pg_dump_restrict_relation_kind_probe() {
    let (_dir, db) = empty_test_db();
    let engine = SqlEngine::new(&db);

    assert_eq!(
        engine
            .execute(
                "SELECT name, setting
                 FROM pg_catalog.pg_settings
                 WHERE name = 'restrict_nonsystem_relation_kind'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("restrict_nonsystem_relation_kind".to_string()),
            SqlValue::String(String::new())
        ]]
    );
    assert_eq!(
        engine
            .execute(
                "SELECT set_config(name, 'view, foreign-table', false)
                 FROM pg_settings
                 WHERE name = 'restrict_nonsystem_relation_kind'"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("view, foreign-table".to_string())]]
    );
}

#[test]
fn postgres_qualified_operator_syntax_is_parse_compatible_for_pg_dump() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE SCHEMA gitlab_partitions_dynamic")
        .unwrap();
    session
        .execute("CREATE TABLE gitlab_partitions_dynamic.pg_dump_target (id bigint PRIMARY KEY)")
        .unwrap();

    let result = session
        .execute(
            "SELECT c.oid
             FROM pg_catalog.pg_class c
                  LEFT JOIN pg_catalog.pg_namespace n
                  ON n.oid OPERATOR(pg_catalog.=) c.relnamespace
             WHERE c.relkind OPERATOR(pg_catalog.=) ANY
                 (array['r', 'S', 'v', 'm', 'f', 'p'])
               AND n.nspname OPERATOR(pg_catalog.~) '^(gitlab_partitions_dynamic)$' COLLATE pg_catalog.default",
        )
        .unwrap();

    assert_eq!(result.rows.len(), 1);
}

#[test]
fn postgres_regex_match_operators_are_supported() {
    let (_dir, db) = empty_test_db();
    let engine = SqlEngine::new(&db);

    assert_eq!(
        engine
            .execute(
                "SELECT
                   'GitLab' ~ '^git' AS case_sensitive,
                   'GitLab' ~* '^git' AS case_insensitive,
                   'GitLab' !~ '^git' AS not_case_sensitive,
                   'GitLab' !~* '^git' AS not_case_insensitive"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Bool(false),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(false),
        ]]
    );
}

#[test]
fn postgres_acldefault_is_supported_for_pg_dump_namespace_query() {
    let (_dir, db) = empty_test_db();
    let engine = SqlEngine::new(&db);

    assert_eq!(
        engine
            .execute(
                "SELECT n.nspname, n.nspacl, acldefault('n', n.nspowner) AS acldefault
                 FROM pg_namespace n
                 WHERE n.nspname = 'public'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("public".to_string()),
            SqlValue::Null,
            SqlValue::String("{bicdb=UC/bicdb}".to_string()),
        ]]
    );
}

#[test]
fn alter_schema_owner_updates_catalog_and_rolls_back_transactionally() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE ROLE schema_owner").unwrap();
    session.execute("CREATE SCHEMA owned_probe").unwrap();

    let owner_query = "SELECT r.rolname
                       FROM pg_namespace n
                       JOIN pg_roles r ON r.oid = n.nspowner
                       WHERE n.nspname = 'owned_probe'";
    assert_eq!(
        session.execute(owner_query).unwrap().rows,
        vec![vec![SqlValue::String("bicdb".to_string())]]
    );

    session.execute("BEGIN").unwrap();
    session
        .execute("ALTER SCHEMA owned_probe OWNER TO schema_owner")
        .unwrap();
    assert_eq!(
        session.execute(owner_query).unwrap().rows,
        vec![vec![SqlValue::String("schema_owner".to_string())]]
    );
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session.execute(owner_query).unwrap().rows,
        vec![vec![SqlValue::String("bicdb".to_string())]]
    );
}

#[test]
fn postgres_namespace_rows_expose_catalog_tableoid_for_pg_dump_lookup() {
    let (_dir, db) = empty_test_db();
    let engine = SqlEngine::new(&db);

    assert_eq!(
        engine
            .execute(
                "SELECT n.tableoid, n.oid, n.nspname
                 FROM pg_namespace n
                 WHERE n.nspname = 'public'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(2615),
            SqlValue::Int(2200),
            SqlValue::String("public".to_string()),
        ]]
    );
}

#[test]
fn postgres_pg_dump_relation_inventory_query_is_supported() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE TABLE pg_dump_relation_inventory (id bigint PRIMARY KEY)")
        .unwrap();
    session
        .execute("CREATE SEQUENCE pg_dump_relation_inventory_seq")
        .unwrap();

    let result = session
        .execute(
            "SELECT c.tableoid, c.oid, c.relname, c.relnamespace, c.relkind, c.reltype, c.relowner,
                    c.relchecks, c.relhasindex, c.relhasrules, c.relpages, c.reltuples,
                    c.relallvisible, 0 AS relallfrozen, c.relhastriggers, c.relpersistence,
                    c.reloftype, c.relacl,
                    acldefault(CASE WHEN c.relkind = 'S' THEN 's'::\"char\" ELSE 'r'::\"char\" END, c.relowner) AS acldefault,
                    CASE WHEN c.relkind = 'f' THEN (SELECT ftserver FROM pg_catalog.pg_foreign_table WHERE ftrelid = c.oid) ELSE 0 END AS foreignserver,
                    c.relfrozenxid, tc.relfrozenxid AS tfrozenxid, tc.oid AS toid,
                    tc.relpages AS toastpages, tc.reloptions AS toast_reloptions,
                    d.refobjid AS owning_tab, d.refobjsubid AS owning_col,
                    tsp.spcname AS reltablespace, false AS relhasoids, c.relispopulated,
                    c.relreplident, c.relrowsecurity, c.relforcerowsecurity, c.relminmxid,
                    tc.relminmxid AS tminmxid,
                    array_remove(array_remove(c.reloptions,'check_option=local'),'check_option=cascaded') AS reloptions,
                    CASE WHEN 'check_option=local' = ANY (c.reloptions) THEN 'LOCAL'::text
                         WHEN 'check_option=cascaded' = ANY (c.reloptions) THEN 'CASCADED'::text
                         ELSE NULL END AS checkoption,
                    am.amname, (d.deptype = 'i') IS TRUE AS is_identity_sequence,
                    c.relispartition AS ispartition
             FROM pg_class c
             LEFT JOIN pg_depend d ON (c.relkind = 'S' AND d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.objsubid = 0 AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a', 'i'))
             LEFT JOIN pg_tablespace tsp ON (tsp.oid = c.reltablespace)
             LEFT JOIN pg_am am ON (c.relam = am.oid)
             LEFT JOIN pg_class tc ON (c.reltoastrelid = tc.oid AND tc.relkind = 't' AND c.relkind <> 'p')
             WHERE c.relkind IN ('r', 'S', 'v', 'c', 'm', 'f', 'p')
             ORDER BY c.oid",
        )
        .unwrap();

    let relname_idx = result
        .columns
        .iter()
        .position(|column| column == "relname")
        .unwrap();
    let acldefault_idx = result
        .columns
        .iter()
        .position(|column| column == "acldefault")
        .unwrap();
    let amname_idx = result
        .columns
        .iter()
        .position(|column| column == "amname")
        .unwrap();
    let identity_idx = result
        .columns
        .iter()
        .position(|column| column == "is_identity_sequence")
        .unwrap();

    let relation_rows = result
        .rows
        .iter()
        .filter(|row| {
            matches!(
                row.get(relname_idx),
                Some(SqlValue::String(name))
                    if name == "pg_dump_relation_inventory"
                        || name == "pg_dump_relation_inventory_seq"
            )
        })
        .collect::<Vec<_>>();

    assert!(!result.rows.iter().any(|row| {
        matches!(
            row.get(relname_idx),
            Some(SqlValue::String(name)) if name.starts_with("bicdb_graph_")
        )
    }));
    assert!(!result.rows.iter().any(|row| {
        matches!(
            row.get(relname_idx),
            Some(SqlValue::String(name)) if name.starts_with("__bicdb_")
        )
    }));
    assert_eq!(relation_rows.len(), 2);
    assert!(relation_rows.iter().any(|row| {
        row[relname_idx] == SqlValue::String("pg_dump_relation_inventory".to_string())
            && row[acldefault_idx] == SqlValue::String("{bicdb=arwdDxt/bicdb}".to_string())
            && row[amname_idx] == SqlValue::String("heap".to_string())
            && row[identity_idx] == SqlValue::Bool(false)
    }));
    assert!(relation_rows.iter().any(|row| {
        row[relname_idx] == SqlValue::String("pg_dump_relation_inventory_seq".to_string())
            && row[acldefault_idx] == SqlValue::String("{bicdb=rwU/bicdb}".to_string())
            && row[amname_idx] == SqlValue::String("heap".to_string())
            && row[identity_idx] == SqlValue::Bool(false)
    }));

    assert_eq!(
        session
            .execute(
                "SELECT i.indisprimary, i.indisreplident \
                 FROM pg_catalog.pg_index i \
                 WHERE i.indrelid = 'pg_dump_relation_inventory'::regclass",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true), SqlValue::Bool(false)]]
    );
}

#[test]
fn pg_get_constraintdef_returns_domain_check_definition_for_pg_dump() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE DOMAIN dump_positive AS numeric(12,2) CHECK (VALUE >= 0)")
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT pg_get_constraintdef(c.oid)
                 FROM pg_constraint c
                 JOIN pg_type t ON t.oid = c.contypid
                 WHERE t.typname = 'dump_positive' AND c.contype = 'c'",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("CHECK (VALUE >= 0)".to_string())]]
    );
}

#[test]
fn alter_sequence_owner_updates_catalog_and_rolls_back_transactionally() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE ROLE sequence_owner").unwrap();
    session
        .execute("CREATE SEQUENCE restore_owner_seq")
        .unwrap();
    let owner_query = "SELECT r.rolname
                       FROM pg_class c
                       JOIN pg_roles r ON r.oid = c.relowner
                       WHERE c.relname = 'restore_owner_seq'";

    session.execute("BEGIN").unwrap();
    session
        .execute("ALTER SEQUENCE restore_owner_seq OWNER TO sequence_owner")
        .unwrap();
    assert_eq!(
        session.execute(owner_query).unwrap().rows,
        vec![vec![SqlValue::String("sequence_owner".to_string())]]
    );
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session.execute(owner_query).unwrap().rows,
        vec![vec![SqlValue::String("bicdb".to_string())]]
    );
}

#[test]
fn alter_view_owner_updates_catalog_and_rolls_back_transactionally() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE ROLE view_owner").unwrap();
    session
        .execute("CREATE VIEW restore_owner_view AS SELECT 1 AS id")
        .unwrap();
    let owner_query = "SELECT r.rolname
                       FROM pg_class c
                       JOIN pg_roles r ON r.oid = c.relowner
                       WHERE c.relname = 'restore_owner_view'";

    session.execute("BEGIN").unwrap();
    session
        .execute("ALTER VIEW public.restore_owner_view OWNER TO view_owner")
        .unwrap();
    assert_eq!(
        session.execute(owner_query).unwrap().rows,
        vec![vec![SqlValue::String("view_owner".to_string())]]
    );
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session.execute(owner_query).unwrap().rows,
        vec![vec![SqlValue::String("bicdb".to_string())]]
    );
}

#[test]
fn postgres_pg_dump_large_object_metadata_catalog_is_empty() {
    let (_dir, db) = empty_test_db();
    let engine = SqlEngine::new(&db);

    let result = engine
        .execute(
            "SELECT tableoid, oid, lomowner, lomacl, \
                    acldefault('L', lomowner) AS acldefault \
             FROM pg_catalog.pg_largeobject_metadata \
             ORDER BY lomowner, lomacl::pg_catalog.text, oid",
        )
        .unwrap();

    assert_eq!(
        result.columns,
        vec!["tableoid", "oid", "lomowner", "lomacl", "acldefault"]
    );
    assert!(result.rows.is_empty());

    let result = engine
        .execute(
            "SELECT unnest(setconfig) \
             FROM pg_catalog.pg_db_role_setting \
             WHERE setrole = 0 AND setdatabase = '1'::oid",
        )
        .unwrap();
    assert_eq!(result.columns, vec!["unnest"]);
    assert!(result.rows.is_empty());
}

#[test]
fn postgres_restore_promotes_matching_implicit_id_primary_key_once() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE TABLE restored_rows (id uuid NOT NULL, value text)")
        .unwrap();
    session
        .execute(
            "INSERT INTO restored_rows (id, value) \
             VALUES ('018f3c7a-89ab-7def-8123-456789abcdef', 'restored')",
        )
        .unwrap();
    session
        .execute(
            "ALTER TABLE restored_rows \
             ADD CONSTRAINT restored_rows_pkey PRIMARY KEY (id)",
        )
        .unwrap();

    let duplicate = session
        .execute("ALTER TABLE restored_rows ADD PRIMARY KEY (id)")
        .unwrap_err();
    assert!(duplicate.to_string().contains("multiple primary keys"));
    assert_eq!(
        session
            .execute("SELECT id, value FROM restored_rows")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("018f3c7a-89ab-7def-8123-456789abcdef".to_string()),
            SqlValue::String("restored".to_string()),
        ]]
    );
}

#[test]
fn empty_unnest_inner_join_skips_right_side_materialization_and_preserves_columns() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    for idx in 0..24 {
        session
            .execute(&format!(
                "CREATE TABLE empty_join_noise_{idx} (id integer PRIMARY KEY, value text);
                 CREATE INDEX idx_empty_join_noise_{idx}_value ON empty_join_noise_{idx} (value)"
            ))
            .unwrap();
    }

    let explicit = session
        .execute(
            "SELECT t.oid AS index_oid
             FROM unnest('{}'::pg_catalog.oid[]) AS src(tbloid)
             JOIN pg_catalog.pg_index i ON (src.tbloid = i.indrelid)
             JOIN pg_catalog.pg_class t ON (t.oid = i.indexrelid)
             WHERE i.indisready",
        )
        .unwrap();

    assert_eq!(explicit.columns, vec!["index_oid"]);
    assert!(explicit.rows.is_empty());

    let wildcard = session
        .execute(
            "SELECT *
             FROM unnest('{}'::pg_catalog.oid[]) AS src(tbloid)
             JOIN pg_catalog.pg_index i ON (src.tbloid = i.indrelid)",
        )
        .unwrap();

    assert!(wildcard.rows.is_empty());
    assert!(wildcard.columns.contains(&"tbloid".to_string()));
    assert!(wildcard.columns.contains(&"indrelid".to_string()));
    assert!(wildcard.columns.contains(&"indexrelid".to_string()));
    assert!(wildcard.columns.contains(&"indisready".to_string()));
}

#[test]
fn postgres_pg_dump_proc_query_treats_auxiliary_catalogs_as_empty() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE FUNCTION pg_dump_proc_inventory(a integer)
             RETURNS integer
             LANGUAGE sql
             AS 'SELECT a'",
        )
        .unwrap();
    session
        .execute("CREATE TYPE pg_dump_internal_range AS RANGE (subtype = integer)")
        .unwrap();

    let result = session
        .execute(
            "SELECT p.tableoid, p.oid, p.proname, p.prolang, p.pronargs, p.proargtypes,
                    p.prorettype, p.proacl, acldefault('f', p.proowner) AS acldefault,
                    p.pronamespace, p.proowner
             FROM pg_proc p
             LEFT JOIN pg_init_privs pip
               ON (p.oid = pip.objoid AND pip.classoid = 'pg_proc'::regclass AND pip.objsubid = 0)
             WHERE p.prokind <> 'a'
               AND NOT EXISTS (
                 SELECT 1 FROM pg_depend
                 WHERE classid = 'pg_proc'::regclass AND objid = p.oid AND deptype = 'i'
               )
               AND (
                 pronamespace != (SELECT oid FROM pg_namespace WHERE nspname = 'pg_catalog')
                 OR EXISTS (
                   SELECT 1 FROM pg_cast
                   WHERE pg_cast.oid > 16383 AND p.oid = pg_cast.castfunc
                 )
                 OR EXISTS (
                   SELECT 1 FROM pg_transform
                   WHERE pg_transform.oid > 16383
                     AND (p.oid = pg_transform.trffromsql OR p.oid = pg_transform.trftosql)
                 )
                 OR p.proacl IS DISTINCT FROM pip.initprivs
               )",
        )
        .unwrap();

    assert_eq!(
        result.columns,
        vec![
            "tableoid",
            "oid",
            "proname",
            "prolang",
            "pronargs",
            "proargtypes",
            "prorettype",
            "proacl",
            "acldefault",
            "pronamespace",
            "proowner",
        ]
    );
    assert_eq!(result.rows.len(), 1);

    let row = &result.rows[0];
    assert_eq!(row[0], SqlValue::Int(1255));
    assert_eq!(
        row[2],
        SqlValue::String("pg_dump_proc_inventory".to_string())
    );
    assert_eq!(row[4], SqlValue::Int(1));
    assert_eq!(row[5], SqlValue::String("23".to_string()));
    assert_eq!(
        row[8],
        SqlValue::String("{=X/bicdb,bicdb=X/bicdb}".to_string())
    );
    assert_eq!(row[9], SqlValue::Int(2200));
    assert_eq!(
        session
            .execute(
                "SELECT count(*) FROM pg_depend d \
                 JOIN pg_cast c ON c.oid = d.objid \
                 JOIN pg_type source ON source.oid = c.castsource \
                 WHERE d.classid = 'pg_cast'::regclass \
                   AND d.deptype = 'i' \
                   AND source.typname = 'pg_dump_internal_range'",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(3)]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT tableoid, oid, castsource, casttarget, castfunc, castcontext, castmethod \
                 FROM pg_cast c \
                 WHERE NOT EXISTS ( \
                   SELECT 1 FROM pg_range r \
                   WHERE c.castsource = r.rngtypid \
                     AND c.casttarget = r.rngmultitypid \
                 ) \
                 ORDER BY 3,4",
            )
            .unwrap()
            .rows
            .len(),
        229
    );
}

#[test]
fn routine_signatures_preserve_registered_types_and_typmods_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE FUNCTION typed_signature(amount numeric(19,4), label varchar(12))
                 RETURNS timestamp(3)
                 LANGUAGE sql
                 AS 'SELECT NULL'",
            )
            .unwrap();
        session
            .execute(
                "CREATE PROCEDURE typed_procedure(amount numeric(12,2), label varchar(8))
                 LANGUAGE sql
                 AS 'SELECT 1'",
            )
            .unwrap();

        let result = session
            .execute(
                "SELECT pronargs, proargtypes,
                        pg_catalog.pg_get_function_arguments(oid),
                        pg_catalog.pg_get_function_result(oid)
                 FROM pg_catalog.pg_proc
                 WHERE proname = 'typed_signature'",
            )
            .unwrap();
        assert_eq!(
            result.rows,
            vec![vec![
                SqlValue::Int(2),
                SqlValue::String("1700 1043".to_string()),
                SqlValue::String("amount numeric(19,4), label varchar(12)".to_string()),
                SqlValue::String("timestamp(3) without time zone".to_string()),
            ]]
        );
        assert_eq!(
            session
                .execute(
                    "SELECT pronargs, proargtypes
                     FROM pg_catalog.pg_proc
                     WHERE proname = 'typed_procedure'",
                )
                .unwrap()
                .rows,
            vec![vec![
                SqlValue::Int(2),
                SqlValue::String("1700 1043".to_string()),
            ]]
        );
    }
    drop(db);

    let mut reopened = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut reopened);
    assert_eq!(
        session
            .execute(
                "SELECT proargtypes, pg_catalog.pg_get_function_result(oid)
                 FROM pg_catalog.pg_proc
                 WHERE proname = 'typed_signature'",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("1700 1043".to_string()),
            SqlValue::String("timestamp(3) without time zone".to_string()),
        ]]
    );
}

#[test]
fn postgres_pg_dump_aggregate_proc_query_returns_empty() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE FUNCTION pg_dump_proc_inventory_aggregate_noise()
             RETURNS integer
             LANGUAGE sql
             AS 'SELECT 1'",
        )
        .unwrap();

    let result = session
        .execute(
            "SELECT p.tableoid, p.oid, p.proname AS aggname, p.pronamespace AS aggnamespace,
                    p.pronargs, p.proargtypes, p.proowner, p.proacl AS aggacl,
                    acldefault('f', p.proowner) AS acldefault
             FROM pg_proc p
             LEFT JOIN pg_init_privs pip
               ON (p.oid = pip.objoid AND pip.classoid = 'pg_proc'::regclass AND pip.objsubid = 0)
             WHERE p.prokind = 'a'
               AND (
                 p.pronamespace != (SELECT oid FROM pg_namespace WHERE nspname = 'pg_catalog')
                 OR p.proacl IS DISTINCT FROM pip.initprivs
               )",
        )
        .unwrap();

    assert_eq!(
        result.columns,
        vec![
            "tableoid",
            "oid",
            "aggname",
            "aggnamespace",
            "pronargs",
            "proargtypes",
            "proowner",
            "aggacl",
            "acldefault",
        ]
    );
    assert!(result.rows.is_empty());
}

#[test]
fn postgres_pg_dump_function_metadata_helpers_project_from_pg_proc_rows() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE FUNCTION pg_dump_meta_fn(a integer, b text)
             RETURNS text
             LANGUAGE sql
             AS 'SELECT b'",
        )
        .unwrap();

    let user_function = session
        .execute(
            "SELECT pg_catalog.pg_get_function_arguments(p.oid) AS funcargs,
                    pg_catalog.pg_get_function_identity_arguments(p.oid) AS funciargs,
                    pg_catalog.pg_get_function_result(p.oid) AS funcresult,
                    pg_catalog.pg_get_function_sqlbody(p.oid) AS prosqlbody,
                    array_to_string(protrftypes, ' ') AS protrftypes
             FROM pg_catalog.pg_proc p, pg_catalog.pg_language l
             WHERE p.proname = 'pg_dump_meta_fn'
               AND l.oid = p.prolang",
        )
        .unwrap();

    assert_eq!(
        user_function.rows,
        vec![vec![
            SqlValue::String("a integer, b text".to_string()),
            SqlValue::String("a integer, b text".to_string()),
            SqlValue::String("text".to_string()),
            SqlValue::Null,
            SqlValue::Null,
        ]]
    );

    let builtin_function = session
        .execute(
            "SELECT pg_catalog.pg_get_function_arguments(p.oid) AS funcargs,
                    pg_catalog.pg_get_function_identity_arguments(p.oid) AS funciargs,
                    pg_catalog.pg_get_function_result(p.oid) AS funcresult,
                    pg_catalog.pg_get_function_sqlbody(p.oid) AS prosqlbody,
                    array_to_string(protrftypes, ' ') AS protrftypes
             FROM pg_catalog.pg_proc p, pg_catalog.pg_language l
             WHERE p.oid = 1000
               AND l.oid = p.prolang",
        )
        .unwrap();

    assert_eq!(
        builtin_function.rows,
        vec![vec![
            SqlValue::String(String::new()),
            SqlValue::String(String::new()),
            SqlValue::String("text".to_string()),
            SqlValue::Null,
            SqlValue::Null,
        ]]
    );
}

#[test]
fn postgres_pg_proc_assigns_distinct_oids_for_large_user_routine_sets() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE FUNCTION assign_p_ci_pipelines_id_value()
             RETURNS trigger
             LANGUAGE plpgsql
             AS 'BEGIN RETURN NEW; END'",
        )
        .unwrap();
    session
        .execute(
            "CREATE FUNCTION sync_packages_composer_with_packages()
             RETURNS trigger
             LANGUAGE plpgsql
             AS 'BEGIN RETURN NEW; END'",
        )
        .unwrap();

    let result = session
        .execute(
            "SELECT oid, proname
             FROM pg_catalog.pg_proc
             WHERE proname IN (
               'assign_p_ci_pipelines_id_value',
               'sync_packages_composer_with_packages'
             )
             ORDER BY proname",
        )
        .unwrap();

    assert_eq!(result.rows.len(), 2);
    assert_ne!(result.rows[0][0], result.rows[1][0]);
}

#[test]
fn postgres_pg_dump_type_query_supports_internal_char_casts() {
    let (_dir, db) = empty_test_db();
    let engine = SqlEngine::new(&db);

    let result = engine
        .execute(
            "SELECT tableoid, oid, typname, typnamespace, typacl,
                    acldefault('T', typowner) AS acldefault, typowner, typelem,
                    typrelid, typarray,
                    CASE WHEN typrelid = 0 THEN ' '::\"char\"
                         ELSE (SELECT relkind FROM pg_class WHERE oid = typrelid)
                    END AS typrelkind,
                    typtype, typisdefined,
                    typname[0] = '_' AND typelem != 0
                      AND (SELECT typarray FROM pg_type te WHERE oid = pg_type.typelem) = oid AS isarray
             FROM pg_type",
        )
        .unwrap();

    assert!(result
        .rows
        .iter()
        .any(|row| row.get(2) == Some(&SqlValue::String("char".to_string()))));
    assert!(result
        .rows
        .iter()
        .any(|row| row.get(10) == Some(&SqlValue::String(" ".to_string()))));
    assert!(result.rows.iter().any(|row| {
        row.get(2) == Some(&SqlValue::String("_text".to_string()))
            && row.get(13) == Some(&SqlValue::Bool(true))
    }));
    assert!(result.rows.iter().any(|row| {
        row.get(2) == Some(&SqlValue::String("text".to_string()))
            && row.get(13) == Some(&SqlValue::Bool(false))
    }));
}

#[test]
fn postgres_pg_dump_type_query_includes_enum_range_and_array_types() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TYPE dump_priority AS ENUM ('normal', 'urgent')")
        .unwrap();

    let result = session
        .execute(
            "SELECT tableoid, oid, typname, typnamespace, typacl,
                    acldefault('T', typowner) AS acldefault, typowner, typelem,
                    typrelid, typarray,
                    CASE WHEN typrelid = 0 THEN ' '::\"char\"
                         ELSE (SELECT relkind FROM pg_class WHERE oid = typrelid)
                    END AS typrelkind,
                    typtype, typisdefined,
                    typname[0] = '_' AND typelem != 0
                      AND (SELECT typarray FROM pg_type te WHERE oid = pg_type.typelem) = oid AS isarray
             FROM pg_type",
        )
        .unwrap();

    let enum_row = result
        .rows
        .iter()
        .find(|row| row[2] == SqlValue::String("dump_priority".to_string()))
        .expect("pg_dump inventory omitted enum type");
    let array_row = result
        .rows
        .iter()
        .find(|row| row[2] == SqlValue::String("_dump_priority".to_string()))
        .expect("pg_dump inventory omitted enum array type");
    assert_eq!(enum_row[11], SqlValue::String("e".to_string()));
    assert_eq!(enum_row[12], SqlValue::Bool(true));
    assert_eq!(enum_row[13], SqlValue::Bool(false));
    assert_eq!(array_row[7], enum_row[1]);
    assert_eq!(array_row[11], SqlValue::String("b".to_string()));
    assert_eq!(array_row[12], SqlValue::Bool(true));
    assert_eq!(array_row[13], SqlValue::Bool(true));

    for (base_name, array_name, kind) in [
        ("int4range", "_int4range", "r"),
        ("int4multirange", "_int4multirange", "m"),
    ] {
        let base_row = result
            .rows
            .iter()
            .find(|row| row[2] == SqlValue::String(base_name.to_string()))
            .unwrap_or_else(|| panic!("pg_dump inventory omitted {base_name}"));
        let array_row = result
            .rows
            .iter()
            .find(|row| row[2] == SqlValue::String(array_name.to_string()))
            .unwrap_or_else(|| panic!("pg_dump inventory omitted {array_name}"));
        assert_eq!(base_row[11], SqlValue::String(kind.to_string()));
        assert_eq!(base_row[13], SqlValue::Bool(false));
        assert_eq!(array_row[7], base_row[1]);
        assert_eq!(array_row[11], SqlValue::String("b".to_string()));
        assert_eq!(array_row[13], SqlValue::Bool(true));
    }
}

#[test]
fn postgres_pg_dump_language_query_supports_routine_languages() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE FUNCTION pg_dump_plpgsql_fn() RETURNS int LANGUAGE plpgsql AS 'BEGIN RETURN 1; END'",
        )
        .unwrap();

    let result = session
        .execute(
            "SELECT tableoid, oid, lanname, lanpltrusted, lanplcallfoid, laninline,
                    lanvalidator, lanacl, acldefault('l', lanowner) AS acldefault,
                    lanowner
             FROM pg_language
             WHERE lanispl
             ORDER BY oid",
        )
        .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], SqlValue::Int(2612));
    assert_eq!(result.rows[0][2], SqlValue::String("plpgsql".to_string()));
    assert_eq!(result.rows[0][3], SqlValue::Bool(true));
    assert_eq!(
        result.rows[0][8],
        SqlValue::String("{=U/bicdb,bicdb=U/bicdb}".to_string())
    );

    assert_eq!(
        session
            .execute(
                "SELECT classid, objid, refobjid
                 FROM pg_depend
                 WHERE refclassid = 'pg_extension'::regclass
                   AND refobjid = (SELECT oid FROM pg_extension WHERE extname = 'plpgsql')
                 ORDER BY classid, objid"
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::Int(1255),
                SqlValue::Int(14025),
                SqlValue::Int(14024),
            ],
            vec![
                SqlValue::Int(1255),
                SqlValue::Int(14026),
                SqlValue::Int(14024),
            ],
            vec![
                SqlValue::Int(1255),
                SqlValue::Int(14027),
                SqlValue::Int(14024),
            ],
            vec![
                SqlValue::Int(2612),
                SqlValue::Int(14028),
                SqlValue::Int(14024),
            ],
        ]
    );

    let prolang = session
        .execute("SELECT prolang FROM pg_proc WHERE proname = 'pg_dump_plpgsql_fn'")
        .unwrap();
    assert_eq!(prolang.rows, vec![vec![result.rows[0][1].clone()]]);
}

#[test]
fn postgres_pg_dump_operator_query_is_supported_without_user_defined_operators() {
    let (_dir, db) = empty_test_db();
    let engine = SqlEngine::new(&db);

    let result = engine
        .execute(
            "SELECT tableoid, oid, oprname, oprnamespace, oprowner, oprkind,
                    oprleft, oprright, oprcode::oid AS oprcode
             FROM pg_operator",
        )
        .unwrap();

    assert!(result.rows.is_empty());
}

#[test]
fn postgres_pg_dump_access_method_query_supports_regproc_casts() {
    let (_dir, db) = empty_test_db();
    let engine = SqlEngine::new(&db);

    let result = engine
        .execute(
            "SELECT tableoid, oid, amname, amtype, amhandler::pg_catalog.regproc AS amhandler
             FROM pg_am",
        )
        .unwrap();

    assert!(result.rows.iter().any(|row| {
        row.get(2) == Some(&SqlValue::String("btree".to_string()))
            && row.get(4) == Some(&SqlValue::Int(330))
    }));
    assert!(result.rows.iter().any(|row| {
        row.get(2) == Some(&SqlValue::String("heap".to_string()))
            && row.get(4) == Some(&SqlValue::Int(3))
    }));
}

#[test]
fn postgres_pg_dump_operator_class_family_queries_expose_builtin_entries() {
    let (_dir, db) = empty_test_db();
    let engine = SqlEngine::new(&db);

    assert_eq!(
        engine
            .execute(
                "SELECT tableoid, oid, opcmethod, opcname, opcnamespace, opcowner FROM pg_opclass"
            )
            .unwrap()
            .rows
            .len(),
        178 // PostgreSQL built-ins plus BicDB's supported trigram class.
    );
    assert_eq!(
        engine
            .execute(
                "SELECT tableoid, oid, opfmethod, opfname, opfnamespace, opfowner FROM pg_opfamily"
            )
            .unwrap()
            .rows
            .len(),
        147 // Includes the corresponding trigram operator family.
    );
    assert_eq!(
        engine.execute("SELECT c.opcname, f.opfname FROM pg_opclass c JOIN pg_opfamily f ON c.opcfamily = f.oid WHERE c.opcname = 'gin_trgm_ops'").unwrap().rows,
        vec![vec![SqlValue::String("gin_trgm_ops".into()), SqlValue::String("gin_trgm_ops".into())]]
    );
    assert_eq!(
        engine
            .execute(
                "SELECT amopstrategy, amopopr::pg_catalog.regoperator
             FROM pg_amop
             ORDER BY amopstrategy",
            )
            .unwrap()
            .rows
            .len(),
        945
    );
    assert_eq!(
        engine
            .execute(
                "SELECT amprocnum, amproc::pg_catalog.regprocedure
             FROM pg_amproc
             ORDER BY amprocnum",
            )
            .unwrap()
            .rows
            .len(),
        714
    );
}

#[test]
fn postgres_pg_dump_text_search_queries_are_supported_without_user_defined_entries() {
    let (_dir, db) = empty_test_db();
    let engine = SqlEngine::new(&db);

    assert!(engine
        .execute(
            "SELECT tableoid, oid, prsname, prsnamespace, prsstart::oid,
                    prstoken::oid, prsend::oid, prsheadline::oid, prslextype::oid
             FROM pg_ts_parser",
        )
        .unwrap()
        .rows
        .is_empty());
    assert!(engine
        .execute(
            "SELECT tableoid, oid, dictname, dictnamespace, dictowner,
                    dicttemplate, dictinitoption
             FROM pg_ts_dict",
        )
        .unwrap()
        .rows
        .is_empty());
    assert!(engine
        .execute(
            "SELECT tableoid, oid, tmplname, tmplnamespace,
                    tmplinit::oid, tmpllexize::oid
             FROM pg_ts_template",
        )
        .unwrap()
        .rows
        .is_empty());
    assert!(engine
        .execute(
            "SELECT tableoid, oid, cfgname, cfgnamespace, cfgowner, cfgparser
             FROM pg_ts_config",
        )
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn postgres_pg_dump_foreign_data_queries_are_supported_without_user_defined_entries() {
    let (_dir, db) = empty_test_db();
    let engine = SqlEngine::new(&db);

    assert!(engine
        .execute(
            "SELECT tableoid, oid, fdwname, fdwowner,
                    fdwhandler::pg_catalog.regproc, fdwvalidator::pg_catalog.regproc,
                    fdwacl, acldefault('F', fdwowner) AS acldefault
             FROM pg_foreign_data_wrapper",
        )
        .unwrap()
        .rows
        .is_empty());
    assert!(engine
        .execute(
            "SELECT tableoid, oid, srvname, srvowner, srvfdw, srvtype,
                    srvversion, srvacl, acldefault('S', srvowner) AS acldefault
             FROM pg_foreign_server",
        )
        .unwrap()
        .rows
        .is_empty());
    assert!(engine
        .execute("SELECT ftrelid, ftserver, ftoptions FROM pg_foreign_table")
        .unwrap()
        .rows
        .is_empty());
    assert!(engine
        .execute("SELECT usename, umoptions FROM pg_user_mappings WHERE srvid = 1 ORDER BY usename")
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn postgres_pg_dump_default_acl_query_is_supported_without_default_privileges() {
    let (_dir, db) = empty_test_db();
    let engine = SqlEngine::new(&db);

    let result = engine
        .execute(
            "SELECT oid, tableoid, defaclrole, defaclnamespace, defaclobjtype,
                    defaclacl,
                    CASE WHEN defaclnamespace = 0
                         THEN acldefault(CASE WHEN defaclobjtype = 'S' THEN 's'::\"char\"
                                             ELSE defaclobjtype END, defaclrole)
                         ELSE '{}' END AS acldefault
             FROM pg_default_acl",
        )
        .unwrap();

    assert!(result.rows.is_empty());
}

#[test]
fn postgres_pg_dump_conversion_query_is_supported_without_user_defined_conversions() {
    let (_dir, db) = empty_test_db();
    let engine = SqlEngine::new(&db);

    let result = engine
        .execute("SELECT tableoid, oid, conname, connamespace, conowner FROM pg_conversion")
        .unwrap();

    assert!(result.rows.is_empty());
}
