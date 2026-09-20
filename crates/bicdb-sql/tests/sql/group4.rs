//! Test group split from the former monolithic tests/sql.rs.
use super::*;

#[test]
fn postgres_pg_dump_event_trigger_query_is_supported_without_event_triggers() {
    let (_dir, db) = empty_test_db();
    let engine = SqlEngine::new(&db);

    let result = engine
        .execute(
            "SELECT e.tableoid, e.oid, evtname, evtenabled, evtevent, evtowner,
                    array_to_string(array(select quote_literal(x)
                                          from unnest(evttags) as t(x)), ', ') as evttags,
                    e.evtfoid::regproc as evtfname
             FROM pg_event_trigger e
             ORDER BY e.oid",
        )
        .unwrap();

    assert!(result.rows.is_empty());
}

#[test]
fn postgres_pg_dump_extended_statistics_queries_are_supported_without_statistics() {
    let (_dir, db) = empty_test_db();
    let engine = SqlEngine::new(&db);

    let result = engine
        .execute(
            "SELECT tableoid, oid, stxname, stxnamespace, stxowner, stxrelid, stxstattarget
             FROM pg_catalog.pg_statistic_ext",
        )
        .unwrap();
    assert!(result.rows.is_empty());

    let result = engine
        .execute(
            "SELECT stxoid, stxdinherit, stxdndistinct, stxddependencies, stxdmcv, stxdexpr
             FROM pg_catalog.pg_statistic_ext_data",
        )
        .unwrap();
    assert!(result.rows.is_empty());
}

#[test]
fn postgres_pg_dump_publication_queries_are_supported_without_publications() {
    let (_dir, db) = empty_test_db();
    let engine = SqlEngine::new(&db);

    let result = engine
        .execute(
            "SELECT p.tableoid, p.oid, p.pubname, p.pubowner, p.puballtables,
                    p.pubinsert, p.pubupdate, p.pubdelete, p.pubtruncate,
                    p.pubviaroot, 'n' AS pubgencols
             FROM pg_catalog.pg_publication p",
        )
        .unwrap();
    assert!(result.rows.is_empty());

    let result = engine
        .execute(
            "SELECT oid, prpubid, prrelid, prqual, prattrs
             FROM pg_catalog.pg_publication_rel",
        )
        .unwrap();
    assert!(result.rows.is_empty());

    let result = engine
        .execute(
            "SELECT oid, pnpubid, pnnspid
             FROM pg_catalog.pg_publication_namespace",
        )
        .unwrap();
    assert!(result.rows.is_empty());
}

#[test]
fn postgres_pg_dump_subscription_queries_are_supported_without_subscriptions() {
    let (_dir, db) = empty_test_db();
    let engine = SqlEngine::new(&db);

    let result = engine
        .execute(
            "SELECT count(*) FROM pg_catalog.pg_subscription
             WHERE subdbid = (SELECT oid FROM pg_catalog.pg_database
                              WHERE datname = current_database())",
        )
        .unwrap();
    assert_eq!(result.rows, vec![vec![SqlValue::Int(0)]]);

    let result = engine
        .execute(
            "SELECT srsubid, srrelid, srsubstate, srsublsn
             FROM pg_catalog.pg_subscription_rel",
        )
        .unwrap();
    assert!(result.rows.is_empty());
}

#[test]
fn postgres_pg_dump_security_label_queries_are_supported_without_security_labels() {
    let (_dir, db) = empty_test_db();
    let engine = SqlEngine::new(&db);

    let result = engine
        .execute(
            "SELECT label, provider, classoid, objoid, objsubid
             FROM pg_catalog.pg_seclabels
             ORDER BY classoid, objoid, objsubid",
        )
        .unwrap();
    assert!(result.rows.is_empty());

    let result = engine
        .execute(
            "SELECT objoid, classoid, objsubid, provider, label
             FROM pg_catalog.pg_seclabel",
        )
        .unwrap();
    assert!(result.rows.is_empty());

    let result = engine
        .execute(
            "SELECT objoid, classoid, provider, label
             FROM pg_catalog.pg_shseclabel",
        )
        .unwrap();
    assert!(result.rows.is_empty());
}

#[test]
fn postgres_pg_dump_column_info_query_supports_unnest_join_source() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TYPE pgdump_mood AS ENUM ('calm', 'busy');
             CREATE TABLE pgdump_unnest_source (
                 id integer PRIMARY KEY,
                 name text,
                 moods pgdump_mood[]
             )",
        )
        .unwrap();
    let oid = session
        .execute("SELECT oid FROM pg_class WHERE relname = 'pgdump_unnest_source'")
        .unwrap()
        .rows[0][0]
        .to_cell();

    let result = session
        .execute(&format!(
            "SELECT a.attrelid, a.attnum, a.attname, pg_catalog.format_type(t.oid, a.atttypmod) AS atttypname
             FROM unnest('{{{oid}}}'::pg_catalog.oid[]) AS src(tbloid)
             JOIN pg_catalog.pg_attribute a ON (src.tbloid = a.attrelid)
             LEFT JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)
             WHERE a.attnum > 0::pg_catalog.int2
             ORDER BY a.attrelid, a.attnum"
        ))
        .unwrap();

    assert_eq!(
        result.columns,
        vec![
            "attrelid".to_string(),
            "attnum".to_string(),
            "attname".to_string(),
            "atttypname".to_string(),
        ]
    );
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0][1], SqlValue::Int(1));
    assert_eq!(result.rows[0][2], SqlValue::String("id".to_string()));
    assert_eq!(result.rows[0][3], SqlValue::String("integer".to_string()));
    assert_eq!(result.rows[1][1], SqlValue::Int(2));
    assert_eq!(result.rows[1][2], SqlValue::String("name".to_string()));
    assert_eq!(result.rows[1][3], SqlValue::String("text".to_string()));
    assert_eq!(result.rows[2][2], SqlValue::String("moods".to_string()));
    assert_eq!(
        result.rows[2][3],
        SqlValue::String("pgdump_mood[]".to_string())
    );

    let full_result = session
        .execute(&format!(
            "SELECT
             a.attrelid,
             a.attnum,
             a.attname,
             a.attstattarget,
             a.attstorage,
             t.typstorage,
             a.atthasdef,
             a.attisdropped,
             a.attlen,
             a.attalign,
             a.attislocal,
             pg_catalog.format_type(t.oid, a.atttypmod) AS atttypname,
             array_to_string(a.attoptions, ', ') AS attoptions,
             CASE WHEN a.attcollation <> t.typcollation THEN a.attcollation ELSE 0 END AS attcollation,
             pg_catalog.array_to_string(ARRAY(SELECT pg_catalog.quote_ident(option_name) || ' ' || pg_catalog.quote_literal(option_value)
                                              FROM pg_catalog.pg_options_to_table(attfdwoptions)
                                              ORDER BY option_name), E',
    ') AS attfdwoptions,
             CASE WHEN a.attnotnull THEN '' ELSE NULL END AS notnull_name,
             NULL AS notnull_comment,
             NULL AS notnull_invalidoid,
             false AS notnull_noinherit,
             CASE WHEN a.attislocal THEN true
                  WHEN a.attnotnull AND NOT a.attislocal THEN true
                  ELSE false
             END AS notnull_islocal,
             a.attcompression AS attcompression,
             a.attidentity,
             CASE WHEN a.atthasmissing AND NOT a.attisdropped THEN a.attmissingval ELSE null END AS attmissingval,
             a.attgenerated
             FROM unnest('{{{oid}}}'::pg_catalog.oid[]) AS src(tbloid)
             JOIN pg_catalog.pg_attribute a ON (src.tbloid = a.attrelid)
             LEFT JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)
             WHERE a.attnum > 0::pg_catalog.int2
             ORDER BY a.attrelid, a.attnum"
        ))
        .unwrap();
    assert_eq!(full_result.columns.len(), 24);
    assert_eq!(full_result.rows.len(), 3);
    assert_eq!(full_result.rows[0][2], SqlValue::String("id".to_string()));
    assert_eq!(
        full_result.rows[0][11],
        SqlValue::String("integer".to_string())
    );
    assert_eq!(full_result.rows[0][15], SqlValue::String(String::new()));
    assert_eq!(full_result.rows[1][2], SqlValue::String("name".to_string()));
    assert_eq!(
        full_result.rows[1][11],
        SqlValue::String("text".to_string())
    );
    assert_eq!(full_result.rows[1][15], SqlValue::Null);
    assert_eq!(
        full_result.rows[2][2],
        SqlValue::String("moods".to_string())
    );
    assert_eq!(
        full_result.rows[2][11],
        SqlValue::String("pgdump_mood[]".to_string())
    );
}

#[test]
fn postgres_pg_dump_constraint_inventory_queries_support_unnest_join_source() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE pgdump_constraint_parent (id integer PRIMARY KEY);
             CREATE TABLE pgdump_constraint_child (
                 id integer PRIMARY KEY,
                 parent_id integer,
                 value integer,
                 CONSTRAINT pgdump_constraint_child_value_check CHECK (value > 0),
                 CONSTRAINT pgdump_constraint_child_parent_fk
                   FOREIGN KEY (parent_id) REFERENCES pgdump_constraint_parent(id)
             )",
        )
        .unwrap();
    let parent_oid = session
        .execute("SELECT oid FROM pg_class WHERE relname = 'pgdump_constraint_parent'")
        .unwrap()
        .rows[0][0]
        .to_cell();
    let child_oid = session
        .execute("SELECT oid FROM pg_class WHERE relname = 'pgdump_constraint_child'")
        .unwrap()
        .rows[0][0]
        .to_cell();

    let checks = session
        .execute(&format!(
            "SELECT c.tableoid, c.oid, conrelid, conname,
                    pg_catalog.pg_get_constraintdef(c.oid) AS consrc,
                    conislocal, convalidated
             FROM unnest('{{{parent_oid},{child_oid}}}'::pg_catalog.oid[]) AS src(tbloid)
             JOIN pg_catalog.pg_constraint c ON (src.tbloid = c.conrelid)
             WHERE contype = 'c'
             ORDER BY c.conrelid, c.conname"
        ))
        .unwrap();

    assert_eq!(
        checks.columns,
        vec![
            "tableoid".to_string(),
            "oid".to_string(),
            "conrelid".to_string(),
            "conname".to_string(),
            "consrc".to_string(),
            "conislocal".to_string(),
            "convalidated".to_string(),
        ]
    );
    assert_eq!(checks.rows.len(), 1);
    assert_eq!(
        checks.rows[0][3],
        SqlValue::String("pgdump_constraint_child_value_check".to_string())
    );
    assert_eq!(
        checks.rows[0][4],
        SqlValue::String("CHECK (value > 0)".to_string())
    );

    let foreign_keys = session
        .execute(&format!(
            "SELECT c.tableoid, c.oid, conrelid, conname, confrelid, conindid,
                    pg_catalog.pg_get_constraintdef(c.oid) AS condef
             FROM unnest('{{{parent_oid},{child_oid}}}'::pg_catalog.oid[]) AS src(tbloid)
             JOIN pg_catalog.pg_constraint c ON (src.tbloid = c.conrelid)
             WHERE contype = 'f' AND conparentid = 0
             ORDER BY conrelid, conname"
        ))
        .unwrap();

    assert_eq!(
        foreign_keys.columns,
        vec![
            "tableoid".to_string(),
            "oid".to_string(),
            "conrelid".to_string(),
            "conname".to_string(),
            "confrelid".to_string(),
            "conindid".to_string(),
            "condef".to_string(),
        ]
    );
    assert_eq!(foreign_keys.rows.len(), 1);
    assert_eq!(
        foreign_keys.rows[0][3],
        SqlValue::String("pgdump_constraint_child_parent_fk".to_string())
    );
    assert_eq!(
        foreign_keys.rows[0][4],
        SqlValue::Int(parent_oid.parse().unwrap())
    );
    assert_eq!(
        foreign_keys.rows[0][6],
        SqlValue::String(
            "FOREIGN KEY (parent_id) REFERENCES pgdump_constraint_parent(id)".to_string()
        )
    );

    let reflected = session
        .execute(
            "SELECT conname, pg_catalog.pg_get_constraintdef(oid, true)
             FROM pg_catalog.pg_constraint
             WHERE contype = 'f' AND conname = 'pgdump_constraint_child_parent_fk'",
        )
        .unwrap();
    assert_eq!(
        reflected.rows,
        vec![vec![
            SqlValue::String("pgdump_constraint_child_parent_fk".to_string()),
            SqlValue::String(
                "FOREIGN KEY (parent_id) REFERENCES pgdump_constraint_parent(id)".to_string(),
            ),
        ]]
    );
}

#[test]
fn postgres_timeout_settings_are_accepted() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute("SET statement_timeout = '30s'")
            .unwrap()
            .command_tag
            .as_deref(),
        Some("SET")
    );
    assert_eq!(
        session.execute("SHOW statement_timeout").unwrap().rows,
        vec![vec![SqlValue::String("30s".to_string())]]
    );
    session
        .execute("SET lock_timeout = '1s'; SET idle_in_transaction_session_timeout = '2s'")
        .unwrap();
    assert_eq!(
        session.execute("SHOW lock_timeout").unwrap().rows,
        vec![vec![SqlValue::String("1s".to_string())]]
    );
    assert_eq!(
        session
            .execute("SHOW idle_in_transaction_session_timeout")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("2s".to_string())]]
    );
    assert_eq!(
        session
            .execute(
                "RESET idle_in_transaction_session_timeout; RESET lock_timeout /*application:web,line:/lib/gitlab/database/with_lock_retries.rb:172:in `execute'*/"
            )
            .unwrap()
            .command_tag
            .as_deref(),
        Some("RESET")
    );
    assert_eq!(
        session
            .execute("SHOW idle_in_transaction_session_timeout")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("0".to_string())]]
    );
    assert_eq!(
        session.execute("SHOW lock_timeout").unwrap().rows,
        vec![vec![SqlValue::String("0".to_string())]]
    );
}

#[test]
fn sql_statement_splitter_ignores_standalone_comments() {
    assert_eq!(
        split_sql_statements(
            "CREATE TABLE gitlab_schema_probe (id text PRIMARY KEY);
             /*application:web,line:/db/migrate/20211202041233_init_schema.rb:8*/
             -- trailing marginalia"
        ),
        vec!["CREATE TABLE gitlab_schema_probe (id text PRIMARY KEY)".to_string()]
    );
}

#[test]
fn postgres_create_schema_is_catalog_visible() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute("CREATE SCHEMA gitlab_partitions_dynamic")
            .unwrap()
            .command_complete_tag(),
        "CREATE SCHEMA"
    );
    assert_eq!(
        session
            .execute("COMMENT ON SCHEMA gitlab_partitions_dynamic IS 'dynamic partitions'")
            .unwrap()
            .command_complete_tag(),
        "COMMENT"
    );
    assert_eq!(
        session
            .execute(
                "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname = 'gitlab_partitions_dynamic'"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(
            "gitlab_partitions_dynamic".to_string()
        )]]
    );
    assert_eq!(
        session
            .execute("CREATE EXTENSION IF NOT EXISTS btree_gist")
            .unwrap()
            .command_complete_tag(),
        "CREATE EXTENSION"
    );
    assert_eq!(
        session
            .execute("SELECT extname, extversion FROM pg_catalog.pg_extension WHERE extname = 'btree_gist'")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("btree_gist".to_string()),
            SqlValue::String("1.0".to_string())
        ]]
    );
    assert_eq!(
        session
            .execute("CREATE EXTENSION IF NOT EXISTS \"plpgsql\" SCHEMA pg_catalog")
            .unwrap()
            .command_complete_tag(),
        "CREATE EXTENSION"
    );
    assert_eq!(
        session
            .execute(
                "CREATE EXTENSION IF NOT EXISTS \"pg_trgm\" WITH SCHEMA pg_catalog VERSION '1.6'"
            )
            .unwrap()
            .command_complete_tag(),
        "CREATE EXTENSION"
    );
    assert_eq!(
        session
            .execute(
                "SELECT extname, extnamespace, extversion FROM pg_catalog.pg_extension WHERE extname = 'pg_trgm'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("pg_trgm".to_string()),
            SqlValue::Int(11),
            SqlValue::String("1.6".to_string())
        ]]
    );
}

#[test]
fn postgres_database_privileges_are_accepted_for_app_setup() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute("REVOKE ALL PRIVILEGES ON DATABASE bicdb FROM PUBLIC")
            .unwrap()
            .command_complete_tag(),
        "REVOKE"
    );
    assert_eq!(
        session
            .execute("GRANT CONNECT, CREATE, TEMPORARY ON DATABASE bicdb TO bicdb")
            .unwrap()
            .command_complete_tag(),
        "GRANT"
    );
}

#[test]
fn postgres_lastval_tracks_identity_insert_for_nextcloud_dav_setup() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE TABLE addressbooks (id BIGSERIAL PRIMARY KEY, principaluri TEXT)")
        .unwrap();
    session
        .execute("INSERT INTO addressbooks (principaluri) VALUES ('principals/system/system')")
        .unwrap();

    assert_eq!(
        session.execute("SELECT lastval()").unwrap().rows,
        vec![vec![SqlValue::Int(1)]]
    );
}

#[test]
fn alter_table_set_schema_updates_catalog_namespace_and_rolls_back() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE SCHEMA gitlab_partitions_dynamic")
        .unwrap();
    session
        .execute("CREATE TABLE ci_build_needs (id bigint PRIMARY KEY)")
        .unwrap();
    assert_eq!(
        session
            .execute("ALTER TABLE IF EXISTS ci_build_needs SET SCHEMA gitlab_partitions_dynamic")
            .unwrap()
            .command_complete_tag(),
        "ALTER TABLE"
    );
    assert_eq!(
        session
            .execute(
                "SELECT n.nspname
                 FROM pg_catalog.pg_class c
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                 WHERE c.relname = 'ci_build_needs'"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(
            "gitlab_partitions_dynamic".to_string()
        )]]
    );
    assert_eq!(
        session
            .execute("ALTER TABLE IF EXISTS missing_table SET SCHEMA gitlab_partitions_dynamic")
            .unwrap()
            .command_complete_tag(),
        "ALTER TABLE"
    );

    session.execute("BEGIN").unwrap();
    session
        .execute("ALTER TABLE IF EXISTS ci_build_needs SET SCHEMA public")
        .unwrap();
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT n.nspname
                 FROM pg_catalog.pg_class c
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                 WHERE c.relname = 'ci_build_needs'"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(
            "gitlab_partitions_dynamic".to_string()
        )]]
    );
}

#[test]
fn postgres_timezone_set_is_accepted() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute("SET timezone = 'UTC'")
            .unwrap()
            .command_tag
            .as_deref(),
        Some("SET")
    );
    assert_eq!(
        session.execute("SHOW timezone").unwrap().rows,
        vec![vec![SqlValue::String("UTC".to_string())]]
    );
    assert_eq!(
        session
            .execute("SET TIME ZONE 'UTC'")
            .unwrap()
            .command_tag
            .as_deref(),
        Some("SET")
    );
    assert_eq!(
        session.execute("SHOW timezone").unwrap().rows,
        vec![vec![SqlValue::String("UTC".to_string())]]
    );
}

#[test]
fn postgres_quote_helpers_work_without_from_for_dbal_introspection() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute(
                "SELECT quote_ident('simple_name'),
                        quote_ident('User'),
                        quote_ident('select'),
                        quote_ident('has\"quote'),
                        quote_literal('Ada''s file')"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("simple_name".to_string()),
            SqlValue::String("\"User\"".to_string()),
            SqlValue::String("\"select\"".to_string()),
            SqlValue::String("\"has\"\"quote\"".to_string()),
            SqlValue::String("'Ada''s file'".to_string()),
        ]]
    );
}

#[test]
fn postgres_max_identifier_length_is_reported() {
    let (_dir, db) = empty_test_db();
    let engine = SqlEngine::new(&db);

    assert_eq!(
        engine.execute("SHOW max_identifier_length").unwrap().rows,
        vec![vec![SqlValue::String("63".to_string())]]
    );
}

#[test]
fn postgres_backend_pid_and_advisory_lock_functions_are_supported() {
    let (_dir, db) = empty_test_db();
    let engine = SqlEngine::new(&db);

    assert_eq!(
        engine.execute("SELECT pg_backend_pid()").unwrap().rows,
        vec![vec![SqlValue::Int(1)]]
    );
    assert_eq!(
        engine
            .execute("SELECT pg_catalog.pg_is_in_recovery()")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(false)]]
    );
    assert_eq!(
        engine
            .execute("SELECT pg_try_advisory_lock(123)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]]
    );
    assert_eq!(
        engine
            .execute("SELECT pg_advisory_lock(hashtext('carrier_schema_lifecycle'))")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]]
    );
    assert_eq!(
        engine
            .execute("SELECT pg_advisory_unlock(123)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]]
    );
    assert_eq!(
        engine
            .execute("SELECT ('x' || substring('0123456789abcdef' FROM 1 FOR 8))::bit(32)::integer")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(0x0123_4567)]]
    );
}

#[test]
fn postgres_catalogs_expose_common_introspection_columns() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT, age INT, metadata JSONB)",
            )
            .unwrap();
        session
            .execute("CREATE INDEX idx_patients_age ON patients(age)")
            .unwrap();
    }

    let engine = SqlEngine::new(&db);
    let patients_oid = engine
        .execute("SELECT 'patients'::regclass::oid")
        .unwrap()
        .rows[0][0]
        .clone();
    assert_eq!(
        engine
            .execute("SELECT datname, datallowconn, datconnlimit FROM pg_catalog.pg_database")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("bicdb".to_string()),
            SqlValue::Bool(true),
            SqlValue::Int(-1),
        ]]
    );
    assert_eq!(
        engine
            .execute(
                "SELECT nspname, nspowner FROM pg_catalog.pg_namespace WHERE nspname = 'public'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("public".to_string()),
            SqlValue::Int(10)
        ]]
    );

    let class = engine
        .execute("SELECT relname, relkind, relnatts, relhasindex, relpersistence FROM pg_catalog.pg_class WHERE relname = 'patients'")
        .unwrap();
    assert_eq!(
        class.rows,
        vec![vec![
            SqlValue::String("patients".to_string()),
            SqlValue::String("r".to_string()),
            SqlValue::Int(4),
            SqlValue::Bool(true),
            SqlValue::String("p".to_string()),
        ]]
    );
    assert_eq!(
        engine
            .execute(
                "SELECT c.relname, s.schemaname, s.relname, s.n_live_tup
                 FROM pg_class c
                 LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
                 WHERE s.relid = to_regclass('patients')"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("patients".to_string()),
            SqlValue::String("public".to_string()),
            SqlValue::String("patients".to_string()),
            SqlValue::Int(0),
        ]]
    );

    let attr = engine
        .execute("SELECT attname, attnum, attnotnull, atttypid, attisdropped FROM pg_catalog.pg_attribute WHERE attname = 'id'")
        .unwrap();
    assert!(attr.rows.iter().any(|row| {
        row == &vec![
            SqlValue::String("id".to_string()),
            SqlValue::Int(1),
            SqlValue::Bool(true),
            SqlValue::Int(25),
            SqlValue::Bool(false),
        ]
    }));

    let indexes = engine
        .execute("SELECT indisprimary, indisunique, indisvalid, indkey FROM pg_catalog.pg_index")
        .unwrap();
    assert!(indexes
        .rows
        .iter()
        .any(|row| row[0] == SqlValue::Bool(true) && row[1] == SqlValue::Bool(true)));
    assert!(indexes
        .rows
        .iter()
        .any(|row| row[0] == SqlValue::Bool(false) && row[3] == SqlValue::String("3".to_string())));
    assert_eq!(
        engine
            .execute("SELECT indexname, tablename FROM pg_indexes WHERE tablename = 'patients' AND indexname = 'idx_patients_age'")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("idx_patients_age".to_string()),
            SqlValue::String("patients".to_string()),
        ]]
    );
    assert_eq!(
        engine
            .execute(
                "SELECT distinct i.relname, d.indisunique, d.indkey, pg_get_indexdef(d.indexrelid), t.oid,
                                pg_catalog.obj_description(i.oid, 'pg_class') AS comment, d.indisvalid
                 FROM pg_class t
                 INNER JOIN pg_index d ON t.oid = d.indrelid
                 INNER JOIN pg_class i ON d.indexrelid = i.oid
                 LEFT JOIN pg_namespace n ON n.oid = t.relnamespace
                 WHERE i.relkind IN ('i', 'I')
                   AND d.indisprimary = 'f'
                   AND t.relname = 'patients'
                   AND n.nspname = 'public'
                 ORDER BY i.relname"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("idx_patients_age".to_string()),
            SqlValue::Bool(false),
            SqlValue::String("3".to_string()),
            SqlValue::String(
                "CREATE INDEX idx_patients_age ON patients USING btree (age)".to_string()
            ),
            patients_oid,
            SqlValue::Null,
            SqlValue::Bool(true),
        ]]
    );
    assert_eq!(
        engine
            .execute(
                "SELECT i.indisvalid
                 FROM pg_class c
                 INNER JOIN pg_index i ON c.oid = i.indexrelid
                 INNER JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = 'public'
                   AND c.relname = 'idx_patients_age'"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]]
    );
    assert_eq!(
        engine
            .execute(
                "SELECT NOT i.indisvalid
                 FROM pg_class c
                 INNER JOIN pg_index i ON c.oid = i.indexrelid
                 INNER JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = 'public'
                   AND c.relname = 'idx_patients_age'"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(false)]]
    );

    let constraints = engine
        .execute("SELECT conname, contype, convalidated FROM pg_catalog.pg_constraint")
        .unwrap();
    assert!(constraints.rows.iter().any(|row| {
        row == &vec![
            SqlValue::String("patients_pkey".to_string()),
            SqlValue::String("p".to_string()),
            SqlValue::Bool(true),
        ]
    }));

    let types = engine
        .execute("SELECT typname, typnamespace, typtype, typinput FROM pg_catalog.pg_type WHERE typname = 'jsonb'")
        .unwrap();
    assert_eq!(
        types.rows,
        vec![vec![
            SqlValue::String("jsonb".to_string()),
            SqlValue::Int(11),
            SqlValue::String("b".to_string()),
            SqlValue::String("jsonb_in".to_string()),
        ]]
    );

    let locks = engine.execute("SELECT * FROM pg_locks").unwrap();
    assert!(locks.rows.is_empty());
    assert!(locks.columns.iter().any(|column| column == "relation"));
    assert!(locks.columns.iter().any(|column| column == "pid"));
    assert!(locks.columns.iter().any(|column| column == "mode"));

    let lock_check = engine
        .execute(
            "SELECT DISTINCT relation::regclass AS table_name
             FROM pg_locks
             JOIN pg_class ON pg_locks.relation = pg_class.oid
             WHERE relation IS NOT NULL
               AND pg_class.relkind IN ('r', 'p')
               AND pid = pg_backend_pid()
               AND relation::regclass::text NOT LIKE 'pg_%'
               AND relation::regclass::text NOT LIKE 'information_schema.%'
               AND relation::regclass::text NOT IN ('schema_migrations', 'ar_internal_metadata')
               AND mode NOT IN ('AccessShareLock', 'RowShareLock')",
        )
        .unwrap();
    assert_eq!(lock_check.columns, vec!["table_name".to_string()]);
    assert!(lock_check.rows.is_empty());
}

#[test]
fn postgres_catalog_filters_handle_targeted_columns_and_joined_constraints() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE catalog_alpha (
                    id INT PRIMARY KEY,
                    tenant_id INT,
                    CONSTRAINT catalog_alpha_tenant_positive CHECK (tenant_id > 0)
                );
                CREATE TABLE catalog_beta (
                    id INT PRIMARY KEY,
                    tenant_id INT,
                    CONSTRAINT catalog_beta_tenant_positive CHECK (tenant_id > 0)
                );
                CREATE INDEX idx_catalog_alpha_tenant_id ON catalog_alpha (tenant_id);",
            )
            .unwrap();
    }

    let engine = SqlEngine::new(&db);
    let nullable = engine
        .execute(
            "SELECT c.is_nullable
             FROM information_schema.columns c
             WHERE c.table_schema = 'public'
               AND c.table_name = 'catalog_alpha'
               AND c.column_name = 'tenant_id'",
        )
        .unwrap();
    assert_eq!(
        nullable.rows,
        vec![vec![SqlValue::String("YES".to_string())]]
    );

    let full_oid = engine
        .execute(
            "SELECT con.oid
             FROM pg_catalog.pg_constraint con
             WHERE con.conname = 'catalog_alpha_tenant_positive'",
        )
        .unwrap();
    let joined = engine
        .execute(
            "SELECT con.oid
             FROM pg_catalog.pg_constraint con
             INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
             INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = con.connamespace
             WHERE con.contype = 'c'
               AND con.conname = 'catalog_alpha_tenant_positive'
               AND nsp.nspname = 'public'
               AND rel.relname = 'catalog_alpha'",
        )
        .unwrap();
    assert_eq!(joined.rows, vec![vec![full_oid.rows[0][0].clone()]]);

    let wrong_relation = engine
        .execute(
            "SELECT COUNT(*)
             FROM pg_catalog.pg_constraint con
             INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
             WHERE con.contype = 'c'
               AND con.conname = 'catalog_alpha_tenant_positive'
               AND rel.relname = 'catalog_beta'",
        )
        .unwrap();
    assert_eq!(wrong_relation.rows, vec![vec![SqlValue::Int(0)]]);

    let index = engine
        .execute(
            "SELECT d.indisunique, d.indkey, pg_get_indexdef(d.indexrelid)
             FROM pg_catalog.pg_class t
             INNER JOIN pg_catalog.pg_index d ON t.oid = d.indrelid
             INNER JOIN pg_catalog.pg_class i ON d.indexrelid = i.oid
             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
             WHERE i.relkind IN ('i', 'I')
               AND d.indisprimary = false
               AND t.relname = 'catalog_alpha'
               AND i.relname = 'idx_catalog_alpha_tenant_id'
               AND n.nspname = 'public'",
        )
        .unwrap();
    assert_eq!(
        index.rows,
        vec![vec![
            SqlValue::Bool(false),
            SqlValue::String("2".to_string()),
            SqlValue::String(
                "CREATE INDEX idx_catalog_alpha_tenant_id ON catalog_alpha USING btree (tenant_id)"
                    .to_string()
            ),
        ]]
    );

    let indexes_for_table = engine
        .execute(
            "SELECT i.relname
             FROM pg_catalog.pg_class t
             INNER JOIN pg_catalog.pg_index d ON t.oid = d.indrelid
             INNER JOIN pg_catalog.pg_class i ON d.indexrelid = i.oid
             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
             WHERE i.relkind IN ('i', 'I')
               AND d.indisprimary = false
               AND t.relname = 'catalog_alpha'
               AND n.nspname = ANY (current_schemas(false))
             ORDER BY i.relname",
        )
        .unwrap();
    assert_eq!(
        indexes_for_table.rows,
        vec![vec![SqlValue::String(
            "idx_catalog_alpha_tenant_id".to_string()
        )]]
    );
}

#[test]
fn postgres_index_catalog_oids_do_not_collide_and_expression_defs_match_clients() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE alpha (id INT PRIMARY KEY)")
            .unwrap();
        session
            .execute("CREATE TABLE projects (id INT PRIMARY KEY, name TEXT)")
            .unwrap();
        session
            .execute("CREATE INDEX index_projects_on_lower_name ON projects (lower((name)::text))")
            .unwrap();
        session
            .execute("CREATE TABLE accounts (id INT PRIMARY KEY, username TEXT, domain TEXT)")
            .unwrap();
        session
            .execute(
                "CREATE UNIQUE INDEX index_accounts_on_username_and_domain_lower
                 ON accounts (lower((username)::text), COALESCE(lower((domain)::text), ''::text))",
            )
            .unwrap();
    }

    let engine = SqlEngine::new(&db);
    let index_classes = engine
        .execute(
            "SELECT oid, relname
             FROM pg_catalog.pg_class
             WHERE relkind IN ('i', 'I')
             ORDER BY oid, relname",
        )
        .unwrap();
    let mut seen_oids = Vec::new();
    for row in &index_classes.rows {
        assert!(
            !seen_oids.contains(&row[0]),
            "duplicate pg_class index oid {:?} in rows {:?}",
            row[0],
            index_classes.rows
        );
        seen_oids.push(row[0].clone());
    }

    let projects_oid = engine
        .execute("SELECT oid FROM pg_catalog.pg_class WHERE relname = 'projects'")
        .unwrap()
        .rows[0][0]
        .clone();
    assert_eq!(
        engine
            .execute(
                "SELECT distinct i.relname, d.indisunique, d.indkey, pg_get_indexdef(d.indexrelid), t.oid,
                                pg_catalog.obj_description(i.oid, 'pg_class') AS comment, d.indisvalid
                 FROM pg_class t
                 INNER JOIN pg_index d ON t.oid = d.indrelid
                 INNER JOIN pg_class i ON d.indexrelid = i.oid
                 LEFT JOIN pg_namespace n ON n.oid = t.relnamespace
                 WHERE i.relkind IN ('i', 'I')
                   AND d.indisprimary = 'f'
                   AND t.relname = 'projects'
                   AND n.nspname = 'public'
                 ORDER BY i.relname"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("index_projects_on_lower_name".to_string()),
            SqlValue::Bool(false),
            SqlValue::String("0".to_string()),
            SqlValue::String(
                "CREATE INDEX index_projects_on_lower_name ON projects USING btree (lower((name)::text))"
                    .to_string()
            ),
            projects_oid,
            SqlValue::Null,
            SqlValue::Bool(true),
        ]]
    );
    assert_eq!(
        engine
            .execute(
                "SELECT i.relname, d.indisunique, d.indkey, pg_get_indexdef(d.indexrelid)
                 FROM pg_class t
                 INNER JOIN pg_index d ON t.oid = d.indrelid
                 INNER JOIN pg_class i ON d.indexrelid = i.oid
                 WHERE t.relname = 'accounts'
                   AND d.indisprimary = false
                 ORDER BY i.relname"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("index_accounts_on_username_and_domain_lower".to_string()),
            SqlValue::Bool(true),
            SqlValue::String("0".to_string()),
            SqlValue::String(
                "CREATE UNIQUE INDEX index_accounts_on_username_and_domain_lower ON accounts USING btree (lower((username)::text), COALESCE(lower((domain)::text), ''::text))"
                    .to_string()
            ),
        ]]
    );
}

#[test]
fn postgres_catalogs_cover_client_introspection_failure_shapes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE invoices (id SERIAL PRIMARY KEY, label TEXT NOT NULL)")
            .unwrap();
        session
            .execute("CREATE INDEX idx_invoices_label ON invoices(label)")
            .unwrap();
    }

    let engine = SqlEngine::new(&db);
    assert_eq!(
        engine
            .execute(
                "SELECT relname, amname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_am am ON c.relam = am.oid WHERE c.relname = 'idx_invoices_label'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("idx_invoices_label".to_string()),
            SqlValue::String("btree".to_string()),
        ]]
    );

    assert_eq!(
        engine
            .execute(
                "SELECT attname, format_type(atttypid, atttypmod) AS formatted_type FROM pg_catalog.pg_attribute WHERE attrelid = 'invoices'::regclass AND attname = 'label'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("label".to_string()),
            SqlValue::String("text".to_string()),
        ]]
    );

    assert_eq!(
        engine
            .execute(
                "SELECT pg_get_expr(adbin, adrelid) AS default_expr FROM pg_catalog.pg_attrdef WHERE adrelid = 'invoices'::regclass AND adnum = 1"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(
            "nextval('invoices_id_seq'::regclass)".to_string()
        )]]
    );

    assert_eq!(
        engine
            .execute(
                "SELECT pg_table_is_visible(oid), pg_get_userbyid(relowner) FROM pg_catalog.pg_class WHERE relname = 'invoices' AND NOT pg_is_other_temp_schema(relnamespace)"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true), SqlValue::String("bicdb".to_string())]]
    );
    assert_eq!(
        engine
            .execute(
                "SELECT schemaname, tablename, tableowner, tablespace, hasindexes, hasrules, hastriggers, rowsecurity FROM pg_catalog.pg_tables WHERE tablename = 'invoices'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("public".to_string()),
            SqlValue::String("invoices".to_string()),
            SqlValue::String("bicdb".to_string()),
            SqlValue::Null,
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Bool(false),
            SqlValue::Bool(false),
        ]]
    );
    assert_eq!(
        engine
            .execute("SELECT tableowner FROM pg_tables WHERE tablename = 'invoices'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("bicdb".to_string())]]
    );

    assert_eq!(
        engine
            .execute("SELECT objoid, description FROM pg_catalog.pg_description WHERE objoid = 0")
            .unwrap()
            .rows,
        Vec::<Vec<SqlValue>>::new()
    );
    assert_eq!(
        engine
            .execute("SELECT inhrelid, inhparent FROM pg_catalog.pg_inherits")
            .unwrap()
            .rows,
        Vec::<Vec<SqlValue>>::new()
    );
    assert_eq!(
        engine
            .execute("SELECT enumtypid, enumlabel FROM pg_catalog.pg_enum")
            .unwrap()
            .rows,
        Vec::<Vec<SqlValue>>::new()
    );
    assert_eq!(
        engine
            .execute(
                "SELECT t.oid, t.typname, t.typelem, t.typdelim, t.typinput, r.rngsubtype, t.typtype, t.typbasetype FROM pg_type AS t LEFT JOIN pg_range AS r ON oid = rngtypid WHERE t.typname = 'text'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(25),
            SqlValue::String("text".to_string()),
            SqlValue::Int(0),
            SqlValue::String(",".to_string()),
            SqlValue::String("textin".to_string()),
            SqlValue::Null,
            SqlValue::String("b".to_string()),
            SqlValue::Int(0),
        ]]
    );
    assert_eq!(
        engine
            .execute(
                "SELECT a.attname, format_type(a.atttypid, a.atttypmod), pg_get_expr(d.adbin, d.adrelid), a.attnotnull, a.atttypid, a.atttypmod, c.collname, col_description(a.attrelid, a.attnum) AS comment FROM pg_attribute a LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum LEFT JOIN pg_type t ON a.atttypid = t.oid LEFT JOIN pg_collation c ON a.attcollation = c.oid AND a.attcollation <> t.typcollation WHERE a.attrelid = 'invoices'::regclass AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum"
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("id".to_string()),
                SqlValue::String("integer".to_string()),
                SqlValue::String("nextval('invoices_id_seq'::regclass)".to_string()),
                SqlValue::Bool(true),
                SqlValue::Int(23),
                SqlValue::Int(-1),
                SqlValue::Null,
                SqlValue::Null,
            ],
            vec![
                SqlValue::String("label".to_string()),
                SqlValue::String("text".to_string()),
                SqlValue::Null,
                SqlValue::Bool(true),
                SqlValue::Int(25),
                SqlValue::Int(-1),
                SqlValue::Null,
                SqlValue::Null,
            ],
        ]
    );
    assert_eq!(
        engine
            .execute(
                "SELECT a.attname, format_type(a.atttypid, a.atttypmod), pg_get_expr(d.adbin, d.adrelid), a.attnotnull, a.atttypid, a.atttypmod, c.collname, col_description(a.attrelid, a.attnum) AS comment, attidentity AS identity, attgenerated as attgenerated FROM pg_attribute a LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum LEFT JOIN pg_type t ON a.atttypid = t.oid LEFT JOIN pg_collation c ON a.attcollation = c.oid AND a.attcollation <> t.typcollation WHERE a.attrelid = '\"invoices\"'::regclass AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum"
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("id".to_string()),
                SqlValue::String("integer".to_string()),
                SqlValue::String("nextval('invoices_id_seq'::regclass)".to_string()),
                SqlValue::Bool(true),
                SqlValue::Int(23),
                SqlValue::Int(-1),
                SqlValue::Null,
                SqlValue::Null,
                SqlValue::String(String::new()),
                SqlValue::String(String::new()),
            ],
            vec![
                SqlValue::String("label".to_string()),
                SqlValue::String("text".to_string()),
                SqlValue::Null,
                SqlValue::Bool(true),
                SqlValue::Int(25),
                SqlValue::Int(-1),
                SqlValue::Null,
                SqlValue::Null,
                SqlValue::String(String::new()),
                SqlValue::String(String::new()),
            ],
        ]
    );
    assert_eq!(
        engine
            .execute(
                "SELECT a.attname
                 FROM pg_index i
                 JOIN pg_attribute a
                   ON a.attrelid = i.indrelid
                  AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = 'invoices'::regclass
                  AND i.indisprimary
                ORDER BY array_position(i.indkey, a.attnum)"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("id".to_string())]]
    );
    assert_eq!(
        engine
            .execute("SELECT system_identifier, current_database() FROM pg_control_system()")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(1_802_028_600_100),
            SqlValue::String("bicdb".to_string()),
        ]]
    );
}

#[test]
fn active_record_primary_key_lookup_returns_ordered_composite_columns() {
    let (_dir, mut db) = empty_test_db();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE composite_pk_refs (
                    tenant_id INT NOT NULL,
                    id INT NOT NULL,
                    label TEXT,
                    PRIMARY KEY (tenant_id, id)
                )",
            )
            .unwrap();
    }

    let engine = SqlEngine::new(&db);
    assert_eq!(
        engine
            .execute(
                "SELECT a.attname
                 FROM pg_index i
                 JOIN pg_attribute a
                   ON a.attrelid = i.indrelid
                  AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = '\"composite_pk_refs\"'::regclass
                  AND i.indisprimary
                ORDER BY array_position(i.indkey, a.attnum)"
            )
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("tenant_id".to_string())],
            vec![SqlValue::String("id".to_string())],
        ]
    );
}

#[test]
fn postgres_constraints_are_enforced_and_visible() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE departments (
                id TEXT PRIMARY KEY,
                code TEXT NOT NULL UNIQUE,
                name TEXT CHECK (name <> '')
            )",
        )
        .unwrap();
    session
        .execute(
            "CREATE TABLE employees (
                id TEXT PRIMARY KEY,
                email TEXT UNIQUE,
                age INT CHECK (age >= 18),
                dept_code TEXT,
                CONSTRAINT employees_dept_fkey
                    FOREIGN KEY (dept_code) REFERENCES departments(code)
                    ON UPDATE CASCADE ON DELETE SET NULL
            )",
        )
        .unwrap();
    session
        .execute(
            "CREATE TABLE projects (
                id TEXT PRIMARY KEY,
                dept_code TEXT,
                CONSTRAINT projects_dept_fkey
                    FOREIGN KEY (dept_code) REFERENCES departments(code)
                    ON UPDATE CASCADE ON DELETE CASCADE
            )",
        )
        .unwrap();

    session
        .execute("INSERT INTO departments (id, code, name) VALUES ('d1', 'ops', 'Ops')")
        .unwrap();
    session
        .execute(
            "INSERT INTO employees (id, email, age, dept_code) VALUES ('e1', 'a@example.com', 30, 'ops')",
        )
        .unwrap();
    session
        .execute("INSERT INTO projects (id, dept_code) VALUES ('p1', 'ops')")
        .unwrap();

    assert_sqlstate(
        session
            .execute("INSERT INTO departments (id, code, name) VALUES ('d2', NULL, 'Bad')")
            .unwrap_err(),
        "23502",
    );
    assert_sqlstate(
        session
            .execute("INSERT INTO departments (id, code, name) VALUES ('d3', 'ops', 'Dup')")
            .unwrap_err(),
        "23505",
    );
    assert_sqlstate(
        session
            .execute("INSERT INTO departments (id, code, name) VALUES ('d1', 'other', 'Dup id')")
            .unwrap_err(),
        "23505",
    );
    assert_sqlstate(
        session
            .execute("INSERT INTO employees (id, email, age, dept_code) VALUES ('e2', 'b@example.com', 17, 'ops')")
            .unwrap_err(),
        "23514",
    );
    assert_sqlstate(
        session
            .execute("INSERT INTO employees (id, email, age, dept_code) VALUES ('e3', 'c@example.com', 22, 'missing')")
            .unwrap_err(),
        "23503",
    );

    session
        .execute("UPDATE departments SET code = 'ops2' WHERE code = 'ops'")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT dept_code FROM employees WHERE id = 'e1'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("ops2".to_string())]]
    );

    session
        .execute("DELETE FROM departments WHERE code = 'ops2'")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT dept_code FROM employees WHERE id = 'e1'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Null]]
    );
    assert!(session
        .execute("SELECT id FROM projects WHERE id = 'p1'")
        .unwrap()
        .rows
        .is_empty());

    let table_constraints = session
        .execute(
            "SELECT constraint_name, constraint_type FROM information_schema.table_constraints WHERE table_name = 'employees'",
        )
        .unwrap();
    assert!(table_constraints.rows.iter().any(|row| {
        row == &vec![
            SqlValue::String("employees_dept_fkey".to_string()),
            SqlValue::String("FOREIGN KEY".to_string()),
        ]
    }));

    let key_usage = session
        .execute(
            "SELECT constraint_name, column_name FROM information_schema.key_column_usage WHERE constraint_name = 'employees_dept_fkey'",
        )
        .unwrap();
    assert_eq!(
        key_usage.rows,
        vec![vec![
            SqlValue::String("employees_dept_fkey".to_string()),
            SqlValue::String("dept_code".to_string()),
        ]]
    );
    let constraint_usage = session
        .execute(
            "SELECT constraint_name, column_name
             FROM information_schema.constraint_column_usage
             WHERE constraint_name = 'employees_dept_fkey'",
        )
        .unwrap();
    assert_eq!(
        constraint_usage.rows,
        vec![vec![
            SqlValue::String("employees_dept_fkey".to_string()),
            SqlValue::String("dept_code".to_string()),
        ]]
    );

    let pg_constraints = session
        .execute("SELECT conname, contype FROM pg_catalog.pg_constraint")
        .unwrap();
    assert!(pg_constraints.rows.iter().any(|row| {
        row == &vec![
            SqlValue::String("departments_code_key".to_string()),
            SqlValue::String("u".to_string()),
        ]
    }));
    assert!(pg_constraints.rows.iter().any(|row| {
        row == &vec![
            SqlValue::String("employees_dept_fkey".to_string()),
            SqlValue::String("f".to_string()),
        ]
    }));
    assert!(pg_constraints
        .rows
        .iter()
        .any(|row| row[1] == SqlValue::String("c".to_string())));
}

#[test]
fn check_constraints_evaluate_row_functions() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE batched_jobs (
                id TEXT PRIMARY KEY,
                min_value BIGINT,
                max_value BIGINT,
                min_cursor JSONB,
                max_cursor JSONB,
                CONSTRAINT cursor_shape CHECK (
                    jsonb_typeof(min_cursor) = 'array'
                    AND jsonb_typeof(max_cursor) = 'array'
                ),
                CONSTRAINT cursor_or_value_bounds CHECK (
                    num_nonnulls(min_value, max_value) = 2
                    OR num_nonnulls(min_cursor, max_cursor) = 2
                )
            )",
        )
        .unwrap();

    session
        .execute(
            "INSERT INTO batched_jobs (id, min_value, max_value)
             VALUES ('by_value', 1, 10)",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO batched_jobs (id, min_cursor, max_cursor)
             VALUES ('by_cursor', '[1]'::jsonb, '[10]'::jsonb)",
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT column_name
                 FROM information_schema.constraint_column_usage
                 WHERE constraint_name = 'cursor_or_value_bounds'
                 ORDER BY column_name"
            )
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("max_cursor".to_string())],
            vec![SqlValue::String("max_value".to_string())],
            vec![SqlValue::String("min_cursor".to_string())],
            vec![SqlValue::String("min_value".to_string())],
        ]
    );

    let err = session
        .execute("INSERT INTO batched_jobs (id) VALUES ('missing_bounds')")
        .unwrap_err();
    assert!(err.to_string().contains("cursor_or_value_bounds"));
}

#[test]
fn postgres_alter_table_add_composite_primary_key_reports_primary_catalogs() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE analytics_cycle_analytics_issue_stage_events (
                stage_event_hash_id bigint NOT NULL,
                issue_id bigint NOT NULL
            )",
        )
        .unwrap();
    session
        .execute(
            "ALTER TABLE ONLY analytics_cycle_analytics_issue_stage_events
             ADD CONSTRAINT analytics_cycle_analytics_issue_stage_events_pkey
             PRIMARY KEY (stage_event_hash_id, issue_id)",
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT constraint_name, constraint_type
                 FROM information_schema.table_constraints
                 WHERE table_name = 'analytics_cycle_analytics_issue_stage_events'
                   AND constraint_name = 'analytics_cycle_analytics_issue_stage_events_pkey'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("analytics_cycle_analytics_issue_stage_events_pkey".to_string()),
            SqlValue::String("PRIMARY KEY".to_string())
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT constraint_name, column_name, ordinal_position
                 FROM information_schema.key_column_usage
                 WHERE table_name = 'analytics_cycle_analytics_issue_stage_events'
                   AND constraint_name = 'analytics_cycle_analytics_issue_stage_events_pkey'
                 ORDER BY ordinal_position"
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("analytics_cycle_analytics_issue_stage_events_pkey".to_string()),
                SqlValue::String("stage_event_hash_id".to_string()),
                SqlValue::Int(1),
            ],
            vec![
                SqlValue::String("analytics_cycle_analytics_issue_stage_events_pkey".to_string()),
                SqlValue::String("issue_id".to_string()),
                SqlValue::Int(2),
            ],
        ]
    );
    assert_eq!(
        session
            .execute(
                "SELECT contype, conkey
                 FROM pg_catalog.pg_constraint
                 WHERE conrelid = 'analytics_cycle_analytics_issue_stage_events'::regclass
                   AND conname = 'analytics_cycle_analytics_issue_stage_events_pkey'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("p".to_string()),
            SqlValue::String("1 2".to_string()),
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT i.indisprimary, i.indisunique, i.indkey, pg_get_indexdef(i.indexrelid)
                 FROM pg_catalog.pg_class c
                 JOIN pg_catalog.pg_index i ON i.indrelid = c.oid
                 WHERE c.relname = 'analytics_cycle_analytics_issue_stage_events'
                   AND i.indisprimary"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::String("1 2".to_string()),
            SqlValue::String(
                "CREATE UNIQUE INDEX analytics_cycle_analytics_issue_stage_events_pkey ON analytics_cycle_analytics_issue_stage_events USING btree (stage_event_hash_id, issue_id)".to_string()
            ),
        ]]
    );
}

#[test]
fn postgres_primary_key_using_index_promotes_existing_unique_index() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE ci_build_needs (
                id bigint PRIMARY KEY,
                partition_id bigint NOT NULL,
                build_id bigint NOT NULL,
                name text NOT NULL
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO ci_build_needs (id, partition_id, build_id, name)
             VALUES (1, 100, 11, 'test'), (2, 100, 12, 'deploy')",
        )
        .unwrap();
    session
        .execute(
            "CREATE UNIQUE INDEX ci_build_needs_pkey_partitioning
             ON ci_build_needs (id, partition_id)",
        )
        .unwrap();
    session
        .execute("ALTER TABLE ci_build_needs DROP CONSTRAINT ci_build_needs_pkey CASCADE")
        .unwrap();
    session
        .execute(
            "ALTER TABLE ci_build_needs
             ADD CONSTRAINT ci_build_needs_pkey
             PRIMARY KEY USING INDEX ci_build_needs_pkey_partitioning",
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT contype, conkey
                 FROM pg_catalog.pg_constraint
                 WHERE conrelid = 'ci_build_needs'::regclass
                   AND conname = 'ci_build_needs_pkey'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("p".to_string()),
            SqlValue::String("1 2".to_string()),
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT i.indisprimary, i.indisunique, i.indkey, pg_get_indexdef(i.indexrelid)
                 FROM pg_catalog.pg_class c
                 JOIN pg_catalog.pg_index i ON i.indrelid = c.oid
                 WHERE c.relname = 'ci_build_needs'
                   AND i.indisprimary"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::String("1 2".to_string()),
            SqlValue::String(
                "CREATE UNIQUE INDEX ci_build_needs_pkey ON ci_build_needs USING btree (id, partition_id)"
                    .to_string()
            ),
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT indexname, indexdef
                 FROM pg_indexes
                 WHERE tablename = 'ci_build_needs'
                 ORDER BY indexname"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("ci_build_needs_pkey".to_string()),
            SqlValue::String(
                "CREATE UNIQUE INDEX ci_build_needs_pkey ON ci_build_needs USING btree (id, partition_id)"
                    .to_string()
            ),
        ]]
    );

    session
        .execute(
            "INSERT INTO ci_build_needs (id, partition_id, build_id, name)
             VALUES (1, 101, 13, 'next partition')",
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT id, partition_id, build_id, name
                 FROM ci_build_needs
                 ORDER BY id, partition_id"
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::Int(1),
                SqlValue::Int(100),
                SqlValue::Int(11),
                SqlValue::String("test".to_string()),
            ],
            vec![
                SqlValue::Int(1),
                SqlValue::Int(101),
                SqlValue::Int(13),
                SqlValue::String("next partition".to_string()),
            ],
            vec![
                SqlValue::Int(2),
                SqlValue::Int(100),
                SqlValue::Int(12),
                SqlValue::String("deploy".to_string()),
            ],
        ]
    );

    let duplicate = session
        .execute(
            "INSERT INTO ci_build_needs (id, partition_id, build_id, name)
             VALUES (1, 100, 13, 'duplicate')",
        )
        .unwrap_err()
        .to_string();
    assert!(duplicate
        .contains("duplicate key value violates unique constraint \"ci_build_needs_pkey\""));

    drop(session);
    let definitions = db.index_definitions();
    assert!(definitions
        .iter()
        .any(|index| index.name == "ci_build_needs_pkey" && index.unique));
    assert!(!definitions
        .iter()
        .any(|index| index.name == "ci_build_needs_pkey_partitioning"));
}

#[test]
fn legacy_unique_pkey_constraint_reports_as_primary_key() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE group_wiki_repositories (
                shard_id bigint NOT NULL,
                group_id bigint NOT NULL,
                disk_path text NOT NULL
            )",
        )
        .unwrap();
    session
        .execute(
            "ALTER TABLE group_wiki_repositories
             ADD CONSTRAINT group_wiki_repositories_pkey UNIQUE (group_id)",
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT constraint_type
                 FROM information_schema.table_constraints
                 WHERE table_name = 'group_wiki_repositories'
                   AND constraint_name = 'group_wiki_repositories_pkey'"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("PRIMARY KEY".to_string())]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT contype, conkey
                 FROM pg_catalog.pg_constraint
                 WHERE conrelid = 'group_wiki_repositories'::regclass
                   AND conname = 'group_wiki_repositories_pkey'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("p".to_string()),
            SqlValue::String("2".to_string()),
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT i.indisprimary, i.indisunique, i.indkey
                 FROM pg_catalog.pg_class c
                 JOIN pg_catalog.pg_index i ON i.indrelid = c.oid
                 WHERE c.relname = 'group_wiki_repositories'
                   AND i.indisprimary"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::String("2".to_string()),
        ]]
    );
}

#[test]
fn postgres_lock_table_modes_are_accepted_for_migrations() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE TABLE projects (id BIGINT PRIMARY KEY)")
        .unwrap();
    session
        .execute("CREATE TABLE project_push_rules (id BIGINT PRIMARY KEY)")
        .unwrap();

    assert_eq!(
        session
            .execute("LOCK TABLE projects, project_push_rules IN SHARE ROW EXCLUSIVE MODE")
            .unwrap()
            .command_tag,
        Some("LOCK TABLE".to_string())
    );
    assert_eq!(
        session
            .execute("LOCK TABLE ONLY projects IN ACCESS EXCLUSIVE MODE NOWAIT")
            .unwrap()
            .command_tag,
        Some("LOCK TABLE".to_string())
    );

    let err = session
        .execute("LOCK TABLE missing_table IN SHARE MODE")
        .unwrap_err();
    assert_eq!(err.sqlstate(), "42P01");
}

#[test]
fn gitlab_postgres_foreign_keys_view_uses_catalog_fast_path() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE TABLE projects (id BIGINT PRIMARY KEY)")
        .unwrap();
    session
        .execute(
            "CREATE TABLE project_push_rules (
                id BIGINT PRIMARY KEY,
                project_id BIGINT NOT NULL
            )",
        )
        .unwrap();
    session
        .execute(
            r#"
            CREATE VIEW postgres_foreign_keys AS
             SELECT pg_constraint.oid,
                pg_constraint.conname AS name,
                (((constrained_namespace.nspname)::text || '.'::text) || (constrained_table.relname)::text) AS constrained_table_identifier,
                (((referenced_namespace.nspname)::text || '.'::text) || (referenced_table.relname)::text) AS referenced_table_identifier,
                (constrained_table.relname)::text AS constrained_table_name,
                (referenced_table.relname)::text AS referenced_table_name,
                constrained_cols.constrained_columns,
                referenced_cols.referenced_columns,
                pg_constraint.confdeltype AS on_delete_action,
                pg_constraint.confupdtype AS on_update_action,
                (pg_constraint.coninhcount > 0) AS is_inherited,
                pg_constraint.convalidated AS is_valid,
                partitioned_parent_oids.parent_oid
               FROM (((((((pg_constraint
                 JOIN pg_class constrained_table ON ((constrained_table.oid = pg_constraint.conrelid)))
                 JOIN pg_class referenced_table ON ((referenced_table.oid = pg_constraint.confrelid)))
                 JOIN pg_namespace constrained_namespace ON ((constrained_table.relnamespace = constrained_namespace.oid)))
                 JOIN pg_namespace referenced_namespace ON ((referenced_table.relnamespace = referenced_namespace.oid)))
                 CROSS JOIN LATERAL ( SELECT array_agg(pg_attribute.attname ORDER BY conkey.idx) AS array_agg
                       FROM (unnest(pg_constraint.conkey) WITH ORDINALITY conkey(attnum, idx)
                         JOIN pg_attribute ON (((pg_attribute.attnum = conkey.attnum) AND (pg_attribute.attrelid = constrained_table.oid))))) constrained_cols(constrained_columns))
                 CROSS JOIN LATERAL ( SELECT array_agg(pg_attribute.attname ORDER BY confkey.idx) AS array_agg
                       FROM (unnest(pg_constraint.confkey) WITH ORDINALITY confkey(attnum, idx)
                         JOIN pg_attribute ON (((pg_attribute.attnum = confkey.attnum) AND (pg_attribute.attrelid = referenced_table.oid))))) referenced_cols(referenced_columns))
                 LEFT JOIN LATERAL ( SELECT pg_depend.refobjid AS parent_oid
                       FROM pg_depend
                      WHERE ((pg_depend.objid = pg_constraint.oid) AND (pg_depend.deptype = 'P'::"char") AND (pg_depend.refobjid IN ( SELECT pg_constraint_1.oid
                               FROM pg_constraint pg_constraint_1
                              WHERE (pg_constraint_1.contype = 'f'::"char"))))
                     LIMIT 1) partitioned_parent_oids(parent_oid) ON (true))
              WHERE (pg_constraint.contype = 'f'::"char")
            "#,
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT attname, format_type(atttypid, atttypmod), attndims
                 FROM pg_attribute
                 WHERE attrelid = '\"postgres_foreign_keys\"'::regclass
                   AND attname IN ('constrained_columns', 'referenced_columns')
                 ORDER BY attname",
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("constrained_columns".to_string()),
                SqlValue::String("text[]".to_string()),
                SqlValue::Int(1),
            ],
            vec![
                SqlValue::String("referenced_columns".to_string()),
                SqlValue::String("text[]".to_string()),
                SqlValue::Int(1),
            ],
        ]
    );

    let exists_sql = r#"
        SELECT 1 AS one FROM "postgres_foreign_keys"
        WHERE "postgres_foreign_keys"."constrained_table_name" = 'project_push_rules'
          AND "postgres_foreign_keys"."referenced_table_name" = 'projects'
          AND "postgres_foreign_keys"."name" = 'fk_9ed8a48c44'
          AND "postgres_foreign_keys"."constrained_columns" = ARRAY['project_id']
          AND "postgres_foreign_keys"."referenced_columns" = ARRAY['id']
          AND "postgres_foreign_keys"."on_delete_action" = 'c'
        LIMIT 1
    "#;
    assert_eq!(
        session.execute(exists_sql).unwrap().rows,
        Vec::<Vec<SqlValue>>::new()
    );

    session
        .execute(
            "ALTER TABLE project_push_rules
             ADD CONSTRAINT fk_9ed8a48c44
             FOREIGN KEY (project_id) REFERENCES projects(id)
             ON DELETE CASCADE
             NOT VALID",
        )
        .unwrap();

    assert_eq!(
        session.execute(exists_sql).unwrap().rows,
        vec![vec![SqlValue::Int(1)]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT name, constrained_table_identifier, referenced_table_identifier,
                        constrained_columns, referenced_columns, on_delete_action,
                        on_update_action, is_inherited, is_valid, parent_oid
                 FROM postgres_foreign_keys
                 WHERE name = 'fk_9ed8a48c44'",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("fk_9ed8a48c44".to_string()),
            SqlValue::String("public.project_push_rules".to_string()),
            SqlValue::String("public.projects".to_string()),
            SqlValue::Json(json!(["project_id"])),
            SqlValue::Json(json!(["id"])),
            SqlValue::String("c".to_string()),
            SqlValue::String("a".to_string()),
            SqlValue::Bool(false),
            SqlValue::Bool(false),
            SqlValue::Null,
        ]]
    );

    session
        .execute(
            "CREATE TABLE p_ci_builds (
                id BIGINT NOT NULL,
                partition_id BIGINT NOT NULL,
                PRIMARY KEY (partition_id, id)
            )",
        )
        .unwrap();
    session
        .execute(
            "CREATE TABLE p_ci_build_needs (
                id BIGINT PRIMARY KEY,
                partition_id BIGINT NOT NULL,
                build_id BIGINT NOT NULL
            )",
        )
        .unwrap();
    session
        .execute(
            "ALTER TABLE p_ci_build_needs
             ADD CONSTRAINT fk_rails_3cf221d4ed_p
             FOREIGN KEY (partition_id, build_id)
             REFERENCES p_ci_builds(partition_id, id)
             ON UPDATE CASCADE
             ON DELETE CASCADE",
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                r#"
                SELECT 1 AS one FROM postgres_foreign_keys
                WHERE constrained_table_name = 'p_ci_build_needs'
                  AND referenced_table_name = 'p_ci_builds'
                  AND name = 'fk_rails_3cf221d4ed_p'
                  AND constrained_columns = ARRAY['partition_id', 'build_id']
                  AND referenced_columns = ARRAY['partition_id', 'id']
                  AND on_delete_action = 'c'
                LIMIT 1
                "#,
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );
    assert_eq!(
        session
            .execute(
                r#"
                SELECT 1 AS one FROM postgres_foreign_keys
                WHERE constrained_table_name = 'p_ci_build_needs'
                  AND referenced_table_name = 'p_ci_builds'
                  AND name = 'fk_rails_3cf221d4ed_p'
                  AND constrained_columns = '{"partition_id","build_id"}'
                  AND referenced_columns = '{"partition_id","id"}'
                  AND on_delete_action = 'c'
                LIMIT 1
                "#,
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );
}

#[test]
fn gitlab_postgres_constraints_view_uses_catalog_fast_path_for_partition_checks() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE ci_build_needs (
                id BIGINT NOT NULL,
                partition_id BIGINT NOT NULL,
                build_id BIGINT NOT NULL,
                CONSTRAINT ci_build_needs_pkey PRIMARY KEY (id, partition_id),
                CONSTRAINT ci_build_needs_build_id_key UNIQUE (build_id)
            )",
        )
        .unwrap();
    session
        .execute(
            r#"
            CREATE VIEW postgres_constraints AS
             SELECT pg_constraint.oid,
                pg_constraint.conname AS name,
                pg_constraint.contype AS constraint_type,
                pg_constraint.convalidated AS constraint_valid,
                ( SELECT array_agg(pg_attribute.attname ORDER BY attnums.ordering) AS array_agg
                       FROM (unnest(pg_constraint.conkey) WITH ORDINALITY attnums(attnum, ordering)
                         JOIN pg_attribute ON (((pg_attribute.attnum = attnums.attnum) AND (pg_attribute.attrelid = pg_class.oid))))) AS column_names,
                (((pg_namespace.nspname)::text || '.'::text) || (pg_class.relname)::text) AS table_identifier,
                NULLIF(pg_constraint.conparentid, (0)::oid) AS parent_constraint_oid,
                pg_get_constraintdef(pg_constraint.oid) AS definition
               FROM ((pg_constraint
                 JOIN pg_class ON ((pg_constraint.conrelid = pg_class.oid)))
                 JOIN pg_namespace ON ((pg_class.relnamespace = pg_namespace.oid)))
            "#,
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT name, constraint_type, column_names, table_identifier, constraint_valid,
                        parent_constraint_oid, definition
                 FROM postgres_constraints
                 WHERE table_identifier = 'public.ci_build_needs'
                   AND constraint_type IN ('u', 'p')
                   AND NOT ('partition_id' = ANY(column_names))
                 ORDER BY name",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("ci_build_needs_build_id_key".to_string()),
            SqlValue::String("u".to_string()),
            SqlValue::Json(json!(["build_id"])),
            SqlValue::String("public.ci_build_needs".to_string()),
            SqlValue::Bool(true),
            SqlValue::Null,
            SqlValue::String("UNIQUE (build_id)".to_string()),
        ]]
    );

    assert_eq!(
        session
            .execute(
                "SELECT name, column_names, definition
                 FROM postgres_constraints
                 WHERE table_identifier = 'public.ci_build_needs'
                   AND constraint_type = 'p'
                   AND 'partition_id' = ANY(column_names)",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("ci_build_needs_pkey".to_string()),
            SqlValue::Json(json!(["id", "partition_id"])),
            SqlValue::String("PRIMARY KEY (id, partition_id)".to_string()),
        ]]
    );

    session
        .execute(
            "ALTER TABLE ci_build_needs
             ADD CONSTRAINT partitioning_constraint
             CHECK ( partition_id IN (100,101,102) )
             NOT VALID",
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT name, column_names, constraint_valid, definition
                 FROM postgres_constraints
                 WHERE table_identifier = 'public.ci_build_needs'
                   AND constraint_type = 'c'
                   AND 'partition_id' = ANY(column_names)",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("partitioning_constraint".to_string()),
            SqlValue::Json(json!(["partition_id"])),
            SqlValue::Bool(false),
            SqlValue::String("CHECK ((partition_id = ANY (ARRAY[100, 101, 102])))".to_string(),),
        ]]
    );
}

#[test]
fn rails_foreign_key_introspection_indexes_catalog_arrays() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE TABLE projects (id BIGINT PRIMARY KEY)")
        .unwrap();
    session
        .execute(
            "CREATE TABLE security_trainings (
                id BIGINT PRIMARY KEY,
                provider_id BIGINT NOT NULL,
                CONSTRAINT fk_security_trainings_provider
                    FOREIGN KEY (provider_id) REFERENCES projects(id)
            )",
        )
        .unwrap();

    let result = session
        .execute(
            "SELECT t2.oid::regclass::text AS to_table,
                    a1.attname AS column,
                    a2.attname AS primary_key,
                    c.conname AS name,
                    c.conkey,
                    c.confkey
             FROM pg_constraint c
             JOIN pg_class t1 ON c.conrelid = t1.oid
             JOIN pg_class t2 ON c.confrelid = t2.oid
             JOIN pg_attribute a1 ON a1.attnum = c.conkey[1] AND a1.attrelid = t1.oid
             JOIN pg_attribute a2 ON a2.attnum = c.confkey[1] AND a2.attrelid = t2.oid
             JOIN pg_namespace t3 ON c.connamespace = t3.oid
             WHERE c.contype = 'f'
               AND t1.relname = 'security_trainings'
               AND t3.nspname = 'public'
             ORDER BY c.conname",
        )
        .unwrap();

    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::String("projects".to_string()),
            SqlValue::String("provider_id".to_string()),
            SqlValue::String("id".to_string()),
            SqlValue::String("fk_security_trainings_provider".to_string()),
            SqlValue::String("2".to_string()),
            SqlValue::String("1".to_string()),
        ]]
    );
}

#[test]
fn alter_table_foreign_keys_persist_and_enforce_referential_actions() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut session = SqlSession::new(&mut db);

        session
            .execute("CREATE TABLE parents (id TEXT PRIMARY KEY, code TEXT UNIQUE)")
            .unwrap();
        session
            .execute("CREATE TABLE children (id TEXT PRIMARY KEY, parent_code TEXT)")
            .unwrap();
        session
            .execute("CREATE TABLE restricted_children (id TEXT PRIMARY KEY, parent_code TEXT)")
            .unwrap();
        session
            .execute("CREATE TABLE persisted_children (id TEXT PRIMARY KEY, parent_code TEXT)")
            .unwrap();
        session
            .execute("INSERT INTO parents (id, code) VALUES ('p1', 'one')")
            .unwrap();
        session
            .execute("INSERT INTO parents (id, code) VALUES ('p2', 'two')")
            .unwrap();
        session
            .execute("INSERT INTO children (id, parent_code) VALUES ('c1', 'one')")
            .unwrap();
        session
            .execute("INSERT INTO restricted_children (id, parent_code) VALUES ('r1', 'one')")
            .unwrap();
        session
            .execute("INSERT INTO persisted_children (id, parent_code) VALUES ('pc1', 'two')")
            .unwrap();

        session
            .execute(
                "ALTER TABLE children
                 ADD CONSTRAINT children_parent_fkey
                 FOREIGN KEY (parent_code) REFERENCES parents(code) ON DELETE CASCADE",
            )
            .unwrap();
        session
            .execute(
                "ALTER TABLE restricted_children
                 ADD CONSTRAINT restricted_children_parent_fkey
                 FOREIGN KEY (parent_code) REFERENCES parents(code)",
            )
            .unwrap();
        session
            .execute(
                "ALTER TABLE persisted_children
                 ADD CONSTRAINT persisted_children_parent_fkey
                 FOREIGN KEY (parent_code) REFERENCES parents(code)",
            )
            .unwrap();

        assert_eq!(
            session
                .execute(
                    "SELECT constraint_name, constraint_type
                     FROM information_schema.table_constraints
                     WHERE constraint_name = 'children_parent_fkey'",
                )
                .unwrap()
                .rows,
            vec![vec![
                SqlValue::String("children_parent_fkey".to_string()),
                SqlValue::String("FOREIGN KEY".to_string())
            ]]
        );

        assert_sqlstate(
            session
                .execute("INSERT INTO children (id, parent_code) VALUES ('bad', 'missing')")
                .unwrap_err(),
            "23503",
        );
        assert_sqlstate(
            session
                .execute("UPDATE children SET parent_code = 'missing' WHERE id = 'c1'")
                .unwrap_err(),
            "23503",
        );
        assert_sqlstate(
            session
                .execute("DELETE FROM parents WHERE code = 'one'")
                .unwrap_err(),
            "23503",
        );
        assert_eq!(
            session
                .execute("SELECT COUNT(*) FROM children WHERE id = 'c1'")
                .unwrap()
                .rows[0][0],
            SqlValue::Int(1)
        );

        session
            .execute("ALTER TABLE restricted_children DROP CONSTRAINT IF EXISTS restricted_children_parent_fkey")
            .unwrap();
        session
            .execute("DELETE FROM parents WHERE code = 'one'")
            .unwrap();
        assert!(session
            .execute("SELECT id FROM children WHERE id = 'c1'")
            .unwrap()
            .rows
            .is_empty());
        session
            .execute("ALTER TABLE children DROP CONSTRAINT IF EXISTS children_parent_fkey")
            .unwrap();
        session
            .execute("INSERT INTO children (id, parent_code) VALUES ('orphan', 'missing')")
            .unwrap();
    }

    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        session
            .execute(
                "SELECT conname, contype FROM pg_catalog.pg_constraint WHERE conname = 'persisted_children_parent_fkey'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("persisted_children_parent_fkey".to_string()),
            SqlValue::String("f".to_string())
        ]]
    );
    assert_sqlstate(
        session
            .execute("INSERT INTO persisted_children (id, parent_code) VALUES ('bad', 'missing')")
            .unwrap_err(),
        "23503",
    );
    assert!(session
        .execute(
            "SELECT conname FROM pg_catalog.pg_constraint WHERE conname = 'children_parent_fkey'"
        )
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(
        session
            .execute("SELECT parent_code FROM children WHERE id = 'orphan'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("missing".to_string())]]
    );
}

#[test]
fn inline_foreign_key_on_delete_cascade_rolls_back_with_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE TABLE parents (id TEXT PRIMARY KEY)")
        .unwrap();
    session
        .execute(
            "CREATE TABLE children (
                id TEXT PRIMARY KEY,
                parent_id TEXT REFERENCES parents(id) ON DELETE CASCADE
            )",
        )
        .unwrap();
    session
        .execute("INSERT INTO parents (id) VALUES ('p1')")
        .unwrap();
    session
        .execute("INSERT INTO children (id, parent_id) VALUES ('c1', 'p1')")
        .unwrap();

    session.execute("BEGIN").unwrap();
    session
        .execute("DELETE FROM parents WHERE id = 'p1'")
        .unwrap();
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session
            .execute("SELECT COUNT(*) FROM children")
            .unwrap()
            .rows[0][0],
        SqlValue::Int(1)
    );

    session.execute("BEGIN").unwrap();
    session
        .execute("DELETE FROM parents WHERE id = 'p1'")
        .unwrap();
    session.execute("COMMIT").unwrap();
    assert_eq!(
        session
            .execute("SELECT COUNT(*) FROM children")
            .unwrap()
            .rows[0][0],
        SqlValue::Int(0)
    );
}

#[test]
fn postgres_error_sqlstates_cover_common_client_failures() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    assert_sqlstate(session.execute("SELECT FROM").unwrap_err(), "42601");
    assert_sqlstate(
        session.execute("SELECT * FROM missing_table").unwrap_err(),
        "42P01",
    );

    session
        .execute("CREATE TABLE typed_errors (id TEXT PRIMARY KEY, age INT NOT NULL)")
        .unwrap();

    let missing_column = session
        .execute("SELECT missing_column FROM typed_errors")
        .unwrap_err();
    assert_eq!(missing_column.sqlstate(), "42703");

    let type_mismatch = session
        .execute("INSERT INTO typed_errors (id, age) VALUES ('p1', 'old')")
        .unwrap_err();
    assert_eq!(type_mismatch.sqlstate(), "22P02");

    let not_null = session
        .execute("INSERT INTO typed_errors (id, age) VALUES ('p2', NULL)")
        .unwrap_err();
    assert_eq!(not_null.sqlstate(), "23502");

    let unsupported = session
        .execute("SELECT MEDIAN(age) OVER (ORDER BY id) FROM typed_errors")
        .unwrap_err();
    assert_eq!(unsupported.sqlstate(), "0A000");
}

#[test]
fn graph_projection_tables_are_queryable_from_sql() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    db.create_collection("patients").unwrap();
    db.create_collection("doctors").unwrap();
    db.create_collection("appointments").unwrap();
    db.insert(
        "patients",
        Record::new("p1").with_metadata(json!({"name": "Asha"})),
    )
    .unwrap();
    db.insert(
        "doctors",
        Record::new("d7").with_metadata(json!({"name": "Dr. Rao"})),
    )
    .unwrap();
    db.insert(
        "appointments",
        Record::new("a1").with_metadata(json!({"patient_id": "p1", "doctor_id": "d7"})),
    )
    .unwrap();
    db.build_graph_projection(
        GraphProjection::new("entity_graph")
            .nodes_from("patients", "Patient")
            .nodes_from("doctors", "Doctor")
            .edge_from_field("appointments", "patient_id", "doctor_id", "VISITED"),
    )
    .unwrap();

    let engine = SqlEngine::new(&db);
    let nodes = engine
        .execute("SELECT id, label FROM bicdb_graph_nodes WHERE id = 'Patient:p1'")
        .unwrap();
    assert_eq!(
        nodes.rows,
        vec![vec![
            SqlValue::String("Patient:p1".to_string()),
            SqlValue::String("Patient".to_string())
        ]]
    );

    let edges = engine
        .execute("SELECT \"from\", \"to\", label FROM bicdb_graph_edges WHERE label = 'VISITED'")
        .unwrap();
    assert_eq!(
        edges.rows,
        vec![vec![
            SqlValue::String("Patient:p1".to_string()),
            SqlValue::String("Doctor:d7".to_string()),
            SqlValue::String("VISITED".to_string())
        ]]
    );

    let graph_columns = engine
        .execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'bicdb_graph_edges'")
        .unwrap();
    assert_eq!(graph_columns.rows.len(), 7);
}

#[test]
fn create_table_insert_update_delete_and_schema_introspection_work() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute(
                "CREATE TABLE patients (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    age INT,
                    metadata JSONB
                )"
            )
            .unwrap()
            .command_tag
            .as_deref(),
        Some("CREATE TABLE")
    );
    session
        .execute("INSERT INTO patients (id, name, age, metadata) VALUES ('p1', 'John', 45, '{\"clinic\":\"rural-7\"}'::jsonb)")
        .unwrap();

    let selected = session
        .execute("SELECT id, name, age, metadata.clinic FROM patients WHERE id = 'p1'")
        .unwrap();
    assert_eq!(
        selected.rows,
        vec![vec![
            SqlValue::String("p1".to_string()),
            SqlValue::String("John".to_string()),
            SqlValue::Int(45),
            SqlValue::String("rural-7".to_string())
        ]]
    );

    session
        .execute("UPDATE patients SET age = 46 WHERE id = 'p1'")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT age FROM patients WHERE id = 'p1'")
            .unwrap()
            .rows[0][0],
        SqlValue::Int(46)
    );

    let columns = session
        .execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'patients'")
        .unwrap()
        .rows;
    assert_eq!(
        columns,
        vec![
            vec![SqlValue::String("id".to_string())],
            vec![SqlValue::String("name".to_string())],
            vec![SqlValue::String("age".to_string())],
            vec![SqlValue::String("metadata".to_string())],
        ]
    );

    session
        .execute("DELETE FROM patients WHERE id = 'p1'")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT COUNT(*) FROM patients")
            .unwrap()
            .rows[0][0],
        SqlValue::Int(0)
    );
}

#[test]
fn alter_table_common_migration_operations_update_data_and_catalogs() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT, age INT)")
        .unwrap();
    session
        .execute("CREATE INDEX idx_patients_age ON patients(age)")
        .unwrap();
    session
        .execute("INSERT INTO patients (id, name, age) VALUES ('p1', 'Ada', 36)")
        .unwrap();
    session
        .execute(
            "ALTER TABLE patients ADD COLUMN active BOOLEAN DEFAULT true, ADD COLUMN profile JSONB DEFAULT '{\"tier\":\"gold\"}'::jsonb",
        )
        .unwrap();

    assert_eq!(
        session
            .execute("SELECT name, active, profile FROM patients WHERE id = 'p1'")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("Ada".to_string()),
            SqlValue::Bool(true),
            SqlValue::Json(json!({"tier": "gold"})),
        ]]
    );

    session
        .execute("ALTER TABLE patients RENAME COLUMN name TO full_name")
        .unwrap();
    session
        .execute("ALTER TABLE patients ALTER COLUMN age TYPE TEXT")
        .unwrap();
    session
        .execute("ALTER TABLE patients DROP COLUMN active")
        .unwrap();
    session
        .execute("ALTER TABLE patients RENAME TO people")
        .unwrap();

    assert_eq!(
        session
            .execute("SELECT id, full_name, age, profile FROM people WHERE id = 'p1'")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("p1".to_string()),
            SqlValue::String("Ada".to_string()),
            SqlValue::String("36".to_string()),
            SqlValue::Json(json!({"tier": "gold"})),
        ]]
    );

    assert_eq!(
        session
            .execute("SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_name = 'people' ORDER BY ordinal_position")
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("people".to_string()),
                SqlValue::String("id".to_string()),
                SqlValue::String("text".to_string()),
            ],
            vec![
                SqlValue::String("people".to_string()),
                SqlValue::String("full_name".to_string()),
                SqlValue::String("text".to_string()),
            ],
            vec![
                SqlValue::String("people".to_string()),
                SqlValue::String("age".to_string()),
                SqlValue::String("text".to_string()),
            ],
            vec![
                SqlValue::String("people".to_string()),
                SqlValue::String("profile".to_string()),
                SqlValue::String("jsonb".to_string()),
            ],
        ]
    );
    assert!(session
        .execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'people' AND column_name = 'active'")
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(
        session
            .execute("SELECT relname FROM pg_catalog.pg_class WHERE relname = 'people'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("people".to_string())]]
    );
    assert_eq!(
        session
            .execute("SELECT relname FROM pg_catalog.pg_class WHERE relname = 'idx_patients_age'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("idx_patients_age".to_string())]]
    );

    session
        .execute("INSERT INTO people (id, full_name, age) VALUES ('p2', 'Grace', 42)")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT age FROM people WHERE id = 'p2'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("42".to_string())]]
    );
}

#[test]
fn alter_table_rename_constraint_updates_primary_key_catalogs() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE TABLE patients (id TEXT PRIMARY KEY, email TEXT)")
        .unwrap();
    session
        .execute("ALTER TABLE patients ADD CONSTRAINT patients_email_key UNIQUE (email)")
        .unwrap();
    session
        .execute("ALTER TABLE patients RENAME TO people")
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT conname FROM pg_catalog.pg_constraint WHERE conrelid = 'people'::regclass AND contype = 'p'"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("patients_pkey".to_string())]]
    );

    session
        .execute("ALTER TABLE people RENAME CONSTRAINT patients_pkey TO people_pkey")
        .unwrap();
    session
        .execute("ALTER TABLE people RENAME CONSTRAINT patients_email_key TO people_email_key")
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT constraint_name, constraint_type FROM information_schema.table_constraints WHERE table_name = 'people' ORDER BY constraint_name"
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("people_email_key".to_string()),
                SqlValue::String("UNIQUE".to_string()),
            ],
            vec![
                SqlValue::String("people_id_not_null".to_string()),
                SqlValue::String("CHECK".to_string()),
            ],
            vec![
                SqlValue::String("people_pkey".to_string()),
                SqlValue::String("PRIMARY KEY".to_string()),
            ],
        ]
    );
    assert_eq!(
        session
            .execute(
                "SELECT conname FROM pg_catalog.pg_constraint WHERE conrelid = 'people'::regclass AND conname IN ('people_pkey', 'people_email_key') ORDER BY conname"
            )
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("people_email_key".to_string())],
            vec![SqlValue::String("people_pkey".to_string())],
        ]
    );
    session
        .execute("INSERT INTO people (id, email) VALUES ('p1', 'a@example.test')")
        .unwrap();
    session
        .execute(
            "INSERT INTO people (id, email) VALUES ('p1', 'b@example.test') ON CONFLICT ON CONSTRAINT people_pkey DO NOTHING",
        )
        .unwrap();
    assert_eq!(
        session.execute("SELECT COUNT(*) FROM people").unwrap().rows,
        vec![vec![SqlValue::Int(1)]]
    );
}

#[test]
fn rollback_restores_table_rename_after_later_ddl_error() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT)")
        .unwrap();
    session
        .execute("CREATE INDEX idx_patients_name ON patients(name)")
        .unwrap();
    session
        .execute("INSERT INTO patients (id, name) VALUES ('p1', 'Ada')")
        .unwrap();
    session.execute("BEGIN").unwrap();
    assert!(session
        .execute(
            "ALTER TABLE patients RENAME TO people;
             ALTER TABLE people RENAME CONSTRAINT missing_constraint TO people_pkey"
        )
        .is_err());
    session.execute("ROLLBACK").unwrap();

    assert_eq!(
        session
            .execute("SELECT id, name FROM patients")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("p1".to_string()),
            SqlValue::String("Ada".to_string()),
        ]]
    );
    assert!(session
        .execute("SELECT relname FROM pg_catalog.pg_class WHERE relname = 'people'")
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(
        session
            .execute("SELECT relname FROM pg_catalog.pg_class WHERE relname = 'idx_patients_name'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("idx_patients_name".to_string())]]
    );
}

#[test]
fn rls_policy_ddl_is_persisted_and_exposed_as_enforced_catalog_metadata() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut session = SqlSession::new(&mut db);

        session
            .execute(
                "CREATE TABLE memberships (
                    id TEXT PRIMARY KEY,
                    org_id TEXT,
                    deleted_at TIMESTAMPTZ
                )",
            )
            .unwrap();
        session
            .execute("ALTER TABLE memberships ENABLE ROW LEVEL SECURITY")
            .unwrap();
        session
            .execute("ALTER TABLE memberships FORCE ROW LEVEL SECURITY")
            .unwrap();
        session
            .execute("DROP POLICY IF EXISTS memberships_select_policy ON memberships")
            .unwrap();
        session
            .execute(
                "CREATE POLICY memberships_select_policy ON memberships FOR SELECT USING (coalesce(current_setting('carrier.current_tenant', true), '') <> '' AND org_id::text = current_setting('carrier.current_tenant', true) AND (position(',org_admin,' in replace(',' || coalesce(current_setting('carrier.current_roles', true), '') || ',', ',,', ',')) > 0 OR deleted_at IS NULL))",
            )
            .unwrap();
        session
            .execute(
                "CREATE POLICY memberships_update_policy ON memberships FOR UPDATE USING (deleted_at IS NULL) WITH CHECK (coalesce(current_setting('carrier.current_tenant', true), '') <> '' AND org_id::text = current_setting('carrier.current_tenant', true))",
            )
            .unwrap();

        assert_eq!(
            session
                .execute("SELECT relrowsecurity, relforcerowsecurity FROM pg_catalog.pg_class WHERE relname = 'memberships'")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Bool(true), SqlValue::Bool(true)]]
        );
        let policies = session
            .execute("SELECT polname, polcmd, polqual, polwithcheck, bicdb_enforced FROM pg_catalog.pg_policy ORDER BY polname")
            .unwrap();
        assert_eq!(policies.rows.len(), 2);
        assert_eq!(
            policies.rows[0][0],
            SqlValue::String("memberships_select_policy".to_string())
        );
        assert_eq!(policies.rows[0][1], SqlValue::String("r".to_string()));
        assert!(policies.rows[0][2]
            .to_cell()
            .contains("coalesce(current_setting('carrier.current_tenant', true), '')"));
        assert!(policies.rows[0][2]
            .to_cell()
            .contains("POSITION(',org_admin,'"));
        assert!(policies.rows[0][2].to_cell().contains("deleted_at IS NULL"));
        assert_eq!(policies.rows[0][3], SqlValue::Null);
        assert_eq!(policies.rows[0][4], SqlValue::Bool(true));
        assert_eq!(policies.rows[1][1], SqlValue::String("w".to_string()));
        assert!(policies.rows[1][3].to_cell().contains("org_id::TEXT"));
    }

    let mut reopened = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut reopened);
    assert_eq!(
        session
            .execute("SELECT tablename, policyname, cmd, qual, with_check, bicdb_enforced FROM pg_policies WHERE tablename = 'memberships' ORDER BY policyname")
            .unwrap()
            .rows
            .len(),
        2
    );
    session
        .execute("DROP POLICY memberships_select_policy ON memberships")
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT policyname FROM pg_policies WHERE policyname = 'memberships_select_policy'"
            )
            .unwrap()
            .rows,
        Vec::<Vec<SqlValue>>::new()
    );
}

#[test]
fn trusted_security_settings_are_neutral_host_bound_and_sql_immutable() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let context = SecurityContext::new("alice", "tenant-a")
        .with_client_id("application-web")
        .with_workspace_id("workspace-1")
        .with_roles(["editor", "staff"])
        .with_scopes(["records:read"])
        .with_authenticated_session("session-a", AuthenticationStrength::Jwt);
    let mut session = SqlSession::new_secure(&mut db, context);

    assert_eq!(
        session
            .execute(
                "SELECT current_trusted_tenant(),
                        current_setting('bicdb.current_workspace', true),
                        current_setting('bicdb.current_user', true),
                        current_setting('bicdb.current_roles', true),
                        current_setting('bicdb.current_scopes', true),
                        current_setting('carrier.current_tenant', true),
                        current_setting('carrier.current_user', true)"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("tenant-a".to_string()),
            SqlValue::String("workspace-1".to_string()),
            SqlValue::String("alice".to_string()),
            SqlValue::String("editor,staff".to_string()),
            SqlValue::String("records:read".to_string()),
            SqlValue::String("tenant-a".to_string()),
            SqlValue::String("alice".to_string()),
        ]]
    );

    session.execute("BEGIN").unwrap();

    for attack in [
        "SET bicdb.current_tenant = 'tenant-b'",
        "SET LOCAL bicdb.current_workspace = 'workspace-9'",
        "SELECT set_config('bicdb.current_user', 'mallory', false)",
        "RESET bicdb.current_scopes",
        "SET carrier.current_tenant = 'tenant-b'",
        "SET LOCAL carrier.current_tenant = 'other'",
        "SET LOCAL carrier.current_workspace = 'ward-9'",
        "SELECT set_config('carrier.current_tenant', 'other', true)",
        "SELECT set_config('carrier.current_user', 'mallory', false)",
        "SELECT pg_catalog.set_config('carrier.current_roles', 'platform_admin', true)",
        "RESET carrier.current_tenant",
        "RESET carrier.current_scopes",
    ] {
        let error = session.execute(attack).unwrap_err().to_string();
        assert!(
            error.contains("protected session attribute"),
            "{attack}: {error}"
        );
    }
    assert_eq!(
        session
            .execute("SELECT current_setting('bicdb.current_tenant', true), current_setting('bicdb.current_user', true), current_setting('carrier.current_tenant', true)")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("tenant-a".to_string()),
            SqlValue::String("alice".to_string()),
            SqlValue::String("tenant-a".to_string()),
        ]]
    );
    session.execute("ROLLBACK").unwrap();
    session.execute("RESET ALL").unwrap();
    session.execute("SET app.temporary = 'discard-me'").unwrap();
    session.execute("DISCARD ALL").unwrap();
    assert_eq!(
        session
            .execute("SELECT current_trusted_tenant(), current_setting('app.temporary', true)")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("tenant-a".to_string()),
            SqlValue::Null
        ]]
    );
}

#[test]
fn pooled_guc_state_cannot_carry_security_authority_between_contexts() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let state = {
        let session = SqlSession::new_secure(
            &mut db,
            SecurityContext::new("alice", "tenant-a").with_workspace_id("clinic-1"),
        );
        session.session_guc_state()
    };

    let mut session = SqlSession::new_secure(
        &mut db,
        SecurityContext::new("bob", "tenant-b").with_workspace_id("clinic-2"),
    )
    .with_session_guc_state(state);
    assert_eq!(
        session
            .execute(
                "SELECT current_setting('carrier.current_user', true),
                        current_setting('carrier.current_tenant', true),
                        current_setting('carrier.current_workspace', true)"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("bob".to_string()),
            SqlValue::String("tenant-b".to_string()),
            SqlValue::String("clinic-2".to_string()),
        ]]
    );
}

#[test]
fn guc_scopes_follow_transaction_and_savepoint_boundaries() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session.execute("SET app.user_id = 'session-user'").unwrap();
    session.execute("BEGIN").unwrap();
    session
        .execute("SET LOCAL app.user_id = 'local-user'")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT current_setting('app.user_id', true)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("local-user".to_string())]]
    );
    session.execute("COMMIT").unwrap();
    assert_eq!(
        session
            .execute("SELECT current_setting('app.user_id', true)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("session-user".to_string())]]
    );

    session.execute("BEGIN").unwrap();
    session.execute("SET app.user_id = 'rolled-back'").unwrap();
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session
            .execute("SELECT current_setting('app.user_id', true)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("session-user".to_string())]]
    );

    session.execute("BEGIN").unwrap();
    session.execute("SET app.user_id = 'committed'").unwrap();
    session.execute("SAVEPOINT before_local").unwrap();
    session
        .execute("SET LOCAL app.user_id = 'savepoint-local'")
        .unwrap();
    session
        .execute("ROLLBACK TO SAVEPOINT before_local")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT current_setting('app.user_id', true)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("committed".to_string())]]
    );
    session.execute("COMMIT").unwrap();
    assert_eq!(
        session
            .execute("SELECT current_setting('app.user_id', true)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("committed".to_string())]]
    );
}

#[test]
fn local_set_config_does_not_leak_rls_identity_after_commit() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE private_docs (id TEXT PRIMARY KEY, owner_id TEXT)")
        .unwrap();
    session
        .execute("INSERT INTO private_docs VALUES ('a', 'alice'), ('b', 'bob')")
        .unwrap();
    session
        .execute("ALTER TABLE private_docs ENABLE ROW LEVEL SECURITY")
        .unwrap();
    session
        .execute("ALTER TABLE private_docs FORCE ROW LEVEL SECURITY")
        .unwrap();
    session
        .execute(
            "CREATE POLICY own_docs ON private_docs USING (owner_id = current_setting('app.user_id', true))",
        )
        .unwrap();

    session.execute("BEGIN").unwrap();
    session
        .execute("SELECT set_config('app.user_id', 'alice', true)")
        .unwrap();
    assert_eq!(
        session.execute("SELECT id FROM private_docs").unwrap().rows,
        vec![vec![SqlValue::String("a".to_string())]]
    );
    session.execute("COMMIT").unwrap();
    assert!(session
        .execute("SELECT id FROM private_docs")
        .unwrap()
        .rows
        .is_empty());

    session
        .execute("SELECT set_config('app.user_id', 'bob', true)")
        .unwrap();
    assert!(session
        .execute("SELECT id FROM private_docs")
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn set_config_cannot_forge_reserved_session_identity() {
    let (_dir, mut db) = empty_test_db();
    {
        let mut setup = SqlSession::new(&mut db);
        setup.execute("CREATE ROLE app_user LOGIN").unwrap();
    }

    for (setting, expected_error) in [
        ("role", "permission denied to set role"),
        (
            "session_authorization",
            "permission denied to set session authorization",
        ),
        ("bicdb.initial_session_authorization", "cannot be changed"),
        ("bicdb.rls_check_as", "cannot be changed"),
    ] {
        let gucs = std::collections::HashMap::from([
            (
                "bicdb.initial_session_authorization".to_string(),
                "app_user".to_string(),
            ),
            ("session_authorization".to_string(), "app_user".to_string()),
        ]);
        let mut session = SqlSession::new(&mut db).with_session_gucs(gucs);
        let error = session
            .execute(&format!("SELECT set_config('{setting}', 'bicdb', false)"))
            .unwrap_err();
        assert!(
            error.to_string().contains(expected_error),
            "unexpected error for {setting}: {error}"
        );
        assert_eq!(
            session
                .execute("SELECT current_user, session_user")
                .unwrap()
                .rows,
            vec![vec![
                SqlValue::String("app_user".to_string()),
                SqlValue::String("app_user".to_string()),
            ]]
        );
    }

    let gucs = std::collections::HashMap::from([
        (
            "bicdb.initial_session_authorization".to_string(),
            "app_user".to_string(),
        ),
        ("session_authorization".to_string(), "app_user".to_string()),
    ]);
    let mut session = SqlSession::new(&mut db).with_session_gucs(gucs);
    session
        .execute("SELECT set_config('app.tenant', 'session-tenant', false)")
        .unwrap();
    session.execute("BEGIN").unwrap();
    session
        .execute("SELECT set_config('app.tenant', 'local-tenant', true)")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT current_setting('app.tenant', true)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("local-tenant".to_string())]]
    );
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session
            .execute("SELECT current_setting('app.tenant', true)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("session-tenant".to_string())]]
    );
    let error = session
        .execute("SELECT set_config('plain_name', 'value', false)")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("SET plain_name is not supported"));
}

#[test]
fn role_ddl_requires_createrole_and_reserves_privileged_attributes_for_superusers() {
    let (_dir, mut db) = empty_test_db();
    {
        let mut setup = SqlSession::new(&mut db);
        setup.execute("CREATE ROLE ordinary LOGIN").unwrap();
        setup
            .execute("CREATE ROLE manager LOGIN CREATEROLE")
            .unwrap();
        setup.execute("CREATE ROLE target LOGIN").unwrap();
        setup.execute("CREATE ROLE privileged BYPASSRLS").unwrap();
    }

    let identity = |role: &str| {
        std::collections::HashMap::from([
            (
                "bicdb.initial_session_authorization".to_string(),
                role.to_string(),
            ),
            ("session_authorization".to_string(), role.to_string()),
        ])
    };
    {
        let mut ordinary = SqlSession::new(&mut db).with_session_gucs(identity("ordinary"));
        for statement in [
            "CREATE ROLE attacker SUPERUSER",
            "CREATE USER attacker SUPERUSER PASSWORD 'secret'",
            "ALTER ROLE ordinary BYPASSRLS",
            "GRANT target TO ordinary",
            "REVOKE target FROM ordinary",
            "DROP ROLE target",
        ] {
            let error = ordinary.execute(statement).unwrap_err();
            assert_eq!(error.sqlstate(), "42501", "statement: {statement}; {error}");
        }
    }

    {
        let mut manager = SqlSession::new(&mut db).with_session_gucs(identity("manager"));
        manager.execute("CREATE ROLE worker LOGIN").unwrap();
        manager.execute("ALTER ROLE worker CREATEDB").unwrap();
        manager.execute("GRANT worker TO ordinary").unwrap();

        for statement in [
            "CREATE ROLE manager_child CREATEROLE",
            "CREATE ROLE bypass_child BYPASSRLS",
            "ALTER ROLE worker REPLICATION",
            "GRANT manager TO ordinary",
            "REVOKE privileged FROM ordinary",
            "DROP ROLE privileged",
        ] {
            let error = manager.execute(statement).unwrap_err();
            assert_eq!(error.sqlstate(), "42501", "statement: {statement}; {error}");
        }

        manager.execute("DROP ROLE worker").unwrap();
    }

    let engine = SqlEngine::new(&db);
    assert_eq!(
        engine
            .execute("SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = 'ordinary'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(false), SqlValue::Bool(false)]]
    );
    assert_eq!(
        engine
            .execute(
                "SELECT rolname FROM pg_roles WHERE rolname IN ('attacker', 'manager_child', 'bypass_child', 'worker')",
            )
            .unwrap()
            .rows,
        Vec::<Vec<SqlValue>>::new()
    );
}

#[test]
fn database_backed_bicdb_scalar_functions_require_superuser() {
    let (_dir, mut db) = empty_test_db();
    {
        let mut setup = SqlSession::new(&mut db);
        setup.execute("CREATE ROLE ordinary LOGIN").unwrap();
        setup
            .execute("CREATE ROLE administrator LOGIN SUPERUSER")
            .unwrap();
    }

    let identity = |role: &str| {
        std::collections::HashMap::from([
            (
                "bicdb.initial_session_authorization".to_string(),
                role.to_string(),
            ),
            ("session_authorization".to_string(), role.to_string()),
        ])
    };
    {
        let mut ordinary = SqlSession::new(&mut db).with_session_gucs(identity("ordinary"));
        for statement in [
            "SELECT bicdb_space_report()",
            "SELECT bicdb_advance_transaction_floor(100)",
            "SELECT bicdb_repair_row_xid('rows', 'pk', 'xmin', 1)",
            "SELECT bicdb_reverse_geocode('patients', 'home_geo', 0, 0, 1000)",
            "SELECT bicdb_record_asof('patients', 'pk', 1)",
            "SELECT bicdb_fts_fold('documents_fts')",
        ] {
            let error = ordinary.execute(statement).unwrap_err();
            assert_eq!(error.sqlstate(), "42501", "statement: {statement}; {error}");
        }
    }

    let mut administrator = SqlSession::new(&mut db).with_session_gucs(identity("administrator"));
    let report = administrator
        .execute("SELECT bicdb_space_report()")
        .unwrap();
    assert_eq!(report.rows.len(), 1);
}

#[test]
fn sql_settings_follow_transaction_and_savepoint_scopes() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session.execute("SET bicdb.vector_search = 'ann'").unwrap();
    session.execute("SET bicdb.ef_search = 10").unwrap();

    session.execute("BEGIN").unwrap();
    session
        .execute("SET bicdb.vector_search = 'exact'")
        .unwrap();
    session.execute("SET bicdb.ef_search = 20").unwrap();
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session.execute("SHOW bicdb.vector_search").unwrap().rows,
        vec![vec![SqlValue::String("ann".to_string())]]
    );
    assert_eq!(
        session.execute("SHOW bicdb.ef_search").unwrap().rows,
        vec![vec![SqlValue::Int(10)]]
    );

    session.execute("BEGIN").unwrap();
    session
        .execute("SET LOCAL bicdb.vector_search = 'exact'")
        .unwrap();
    session.execute("SET LOCAL bicdb.ef_search = 20").unwrap();
    session.execute("SAVEPOINT settings_mark").unwrap();
    session.execute("SET bicdb.vector_search = 'ann'").unwrap();
    session.execute("SET bicdb.ef_search = 30").unwrap();
    session
        .execute("ROLLBACK TO SAVEPOINT settings_mark")
        .unwrap();
    assert_eq!(
        session.execute("SHOW bicdb.vector_search").unwrap().rows,
        vec![vec![SqlValue::String("exact".to_string())]]
    );
    assert_eq!(
        session.execute("SHOW bicdb.ef_search").unwrap().rows,
        vec![vec![SqlValue::Int(20)]]
    );
    session.execute("COMMIT").unwrap();
    assert_eq!(
        session.execute("SHOW bicdb.vector_search").unwrap().rows,
        vec![vec![SqlValue::String("ann".to_string())]]
    );
    assert_eq!(
        session.execute("SHOW bicdb.ef_search").unwrap().rows,
        vec![vec![SqlValue::Int(10)]]
    );

    session.execute("BEGIN").unwrap();
    session.execute("SET LOCAL bicdb.ef_search = 40").unwrap();
    session.execute("SET bicdb.ef_search = 50").unwrap();
    session.execute("COMMIT").unwrap();
    assert_eq!(
        session.execute("SHOW bicdb.ef_search").unwrap().rows,
        vec![vec![SqlValue::Int(50)]]
    );

    session.execute("BEGIN").unwrap();
    session.execute("SET bicdb.ef_search = 60").unwrap();
    session.execute("SET LOCAL bicdb.ef_search = 70").unwrap();
    assert_eq!(
        session.execute("SHOW bicdb.ef_search").unwrap().rows,
        vec![vec![SqlValue::Int(70)]]
    );
    session.execute("COMMIT").unwrap();
    assert_eq!(
        session.execute("SHOW bicdb.ef_search").unwrap().rows,
        vec![vec![SqlValue::Int(60)]]
    );

    session.execute("BEGIN").unwrap();
    session
        .execute("SELECT set_config('bicdb.ef_search', '80', true)")
        .unwrap();
    assert_eq!(
        session.execute("SHOW bicdb.ef_search").unwrap().rows,
        vec![vec![SqlValue::Int(80)]]
    );
    session.execute("COMMIT").unwrap();
    assert_eq!(
        session.execute("SHOW bicdb.ef_search").unwrap().rows,
        vec![vec![SqlValue::Int(60)]]
    );
}

#[test]
fn regular_and_local_guc_ordering_and_reset_match_postgres() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session.execute("SET app.scope = 'base'").unwrap();
    session.execute("BEGIN").unwrap();
    session.execute("SET LOCAL app.scope = 'local'").unwrap();
    session.execute("SET app.scope = 'regular'").unwrap();
    session.execute("COMMIT").unwrap();
    assert_eq!(
        session
            .execute("SELECT current_setting('app.scope', true)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("regular".to_string())]]
    );

    session.execute("BEGIN").unwrap();
    session.execute("SET app.scope = 'regular-two'").unwrap();
    session
        .execute("SET LOCAL app.scope = 'local-two'")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT current_setting('app.scope', true)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("local-two".to_string())]]
    );
    session.execute("COMMIT").unwrap();
    assert_eq!(
        session
            .execute("SELECT current_setting('app.scope', true)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("regular-two".to_string())]]
    );

    session.execute("BEGIN").unwrap();
    session
        .execute("SET LOCAL app.scope = 'temporary'")
        .unwrap();
    session.execute("RESET app.scope").unwrap();
    session.execute("COMMIT").unwrap();
    assert_eq!(
        session
            .execute("SELECT current_setting('app.scope', true)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Null]]
    );
}
