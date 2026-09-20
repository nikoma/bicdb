//! Test group split from the former monolithic tests/sql.rs.
use super::*;

#[test]
fn casts_now_and_clear_unsupported_errors_work() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session.execute("SELECT '123'::int4").unwrap().rows[0][0],
        SqlValue::Int(123)
    );
    assert!(!session.execute("SELECT now()").unwrap().rows[0][0]
        .to_cell()
        .is_empty());
    session
        .execute("CREATE TABLE a (id TEXT PRIMARY KEY)")
        .unwrap();
    session
        .execute("CREATE TABLE b (id TEXT PRIMARY KEY)")
        .unwrap();
    let result = session
        .execute("SELECT a.id, b.id FROM a LEFT JOIN b ON a.id = b.id")
        .unwrap();
    assert!(result.rows.is_empty());
}

#[test]
fn type_breadth_supports_preserved_numeric_interval_and_catalogs() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE type_breadth (
                id TEXT PRIMARY KEY,
                amount NUMERIC,
                span INTERVAL,
                starts_at TIMESTAMP WITH TIME ZONE
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO type_breadth (id, amount, span, starts_at) VALUES \
             ('t1', '12.30'::numeric, '1 day'::interval, '2024-01-02 03:04:05+00'::timestamptz)",
        )
        .unwrap();

    assert_eq!(
        session
            .execute("SELECT amount, span, starts_at FROM type_breadth WHERE id = 't1'")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("12.30".to_string()),
            SqlValue::String("1 day".to_string()),
            SqlValue::String("2024-01-02 03:04:05+00".to_string()),
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT typname, oid FROM pg_catalog.pg_type \
                 WHERE typname IN ('numeric', 'interval') ORDER BY typname",
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("interval".to_string()),
                SqlValue::Int(1186)
            ],
            vec![SqlValue::String("numeric".to_string()), SqlValue::Int(1700)],
        ]
    );
    assert_eq!(
        session
            .execute(
                "SELECT attname, atttypid FROM pg_catalog.pg_attribute a \
                 JOIN pg_catalog.pg_class c ON a.attrelid = c.oid \
                 WHERE c.relname = 'type_breadth' AND attname IN ('amount', 'span') \
                 ORDER BY attname",
            )
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("amount".to_string()), SqlValue::Int(1700)],
            vec![SqlValue::String("span".to_string()), SqlValue::Int(1186)],
        ]
    );
    assert_eq!(
        session
            .execute("SELECT to_regtype('numeric'), to_regtype('hstore')")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("numeric".to_string()),
            SqlValue::Null
        ]]
    );
    assert_eq!(
        session
            .execute("SELECT 'bigint'::regtype::oid")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(20)]]
    );
    assert_eq!(
        session.execute("SELECT 23::regtype::text").unwrap().rows,
        vec![vec![SqlValue::String("integer".to_string())]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT oid, typname FROM pg_catalog.pg_type \
                 WHERE typname IN ('oid', 'regtype') ORDER BY oid",
            )
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::Int(26), SqlValue::String("oid".to_string())],
            vec![SqlValue::Int(2206), SqlValue::String("regtype".to_string())],
        ]
    );
}

#[test]
fn postgres_float_type_aliases_follow_precision_mapping() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE float_aliases (
                id TEXT PRIMARY KEY,
                bare FLOAT,
                narrow FLOAT(24),
                wide FLOAT(25),
                readings FLOAT[]
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO float_aliases (id, bare, narrow, wide, readings)
             VALUES ('row-1', 1.5, 2.5, 3.5, ARRAY[4.5, 5.5]::float[])",
        )
        .unwrap();

    assert_eq!(
        session
            .execute("SELECT bare, narrow, wide, readings[2] FROM float_aliases WHERE id = 'row-1'")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Float(1.5),
            SqlValue::Float(2.5),
            SqlValue::Float(3.5),
            SqlValue::Float(5.5),
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT a.attname, a.atttypid
                 FROM pg_catalog.pg_attribute a
                 JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
                 WHERE c.relname = 'float_aliases'
                   AND a.attname IN ('bare', 'narrow', 'wide', 'readings')
                 ORDER BY a.attname",
            )
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("bare".to_string()), SqlValue::Int(701)],
            vec![SqlValue::String("narrow".to_string()), SqlValue::Int(700)],
            vec![
                SqlValue::String("readings".to_string()),
                SqlValue::Int(1022)
            ],
            vec![SqlValue::String("wide".to_string()), SqlValue::Int(701)],
        ]
    );
    assert_eq!(
        session
            .execute("SELECT 'float'::regtype::oid, to_regtype('float')")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(701),
            SqlValue::String("double precision".to_string())
        ]]
    );
}

#[test]
fn postgres_create_table_like_copies_columns_and_honors_default_options() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE award_emoji (
                id BIGINT PRIMARY KEY,
                name TEXT NOT NULL DEFAULT 'thumbs',
                user_id BIGINT
            )",
        )
        .unwrap();
    session
        .execute(
            "CREATE TABLE award_emoji_archived (
                LIKE award_emoji
            )",
        )
        .unwrap();
    session
        .execute("ALTER TABLE award_emoji_archived ADD PRIMARY KEY (id)")
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT a.attname, a.attnotnull, pg_catalog.format_type(a.atttypid, a.atttypmod)
                 FROM pg_catalog.pg_attribute a
                 JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
                 WHERE c.relname = 'award_emoji_archived'
                   AND a.attname IN ('id', 'name', 'user_id')
                 ORDER BY a.attnum",
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("id".to_string()),
                SqlValue::Bool(true),
                SqlValue::String("bigint".to_string())
            ],
            vec![
                SqlValue::String("name".to_string()),
                SqlValue::Bool(true),
                SqlValue::String("text".to_string())
            ],
            vec![
                SqlValue::String("user_id".to_string()),
                SqlValue::Bool(false),
                SqlValue::String("bigint".to_string())
            ],
        ]
    );
    session
        .execute("INSERT INTO award_emoji_archived (id, name, user_id) VALUES (1, 'thumbs', 42)")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT id, name, user_id FROM award_emoji_archived WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(1),
            SqlValue::String("thumbs".to_string()),
            SqlValue::Int(42)
        ]]
    );

    let missing_default_error = session
        .execute("INSERT INTO award_emoji_archived (id, user_id) VALUES (2, 43)")
        .unwrap_err();
    assert_eq!(missing_default_error.sqlstate(), "23502");

    session
        .execute(
            "CREATE TABLE award_emoji_with_defaults (
                LIKE award_emoji INCLUDING DEFAULTS
            )",
        )
        .unwrap();
    session
        .execute("INSERT INTO award_emoji_with_defaults (id, user_id) VALUES (3, 44)")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT id, name, user_id FROM award_emoji_with_defaults WHERE id = 3")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(3),
            SqlValue::String("thumbs".to_string()),
            SqlValue::Int(44)
        ]]
    );
}

#[test]
fn postgres_create_table_like_including_all_partition_by_list_matches_gitlab_parent_table() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE ci_build_needs (
                id BIGINT NOT NULL,
                partition_id BIGINT NOT NULL,
                name TEXT,
                CONSTRAINT ci_build_needs_pkey PRIMARY KEY (id, partition_id)
            )",
        )
        .unwrap();
    session
        .execute(
            "ALTER TABLE ci_build_needs
             ADD CONSTRAINT partitioning_constraint
             CHECK ( partition_id IN (100,101) )
             NOT VALID",
        )
        .unwrap();

    session
        .execute(
            "CREATE TABLE IF NOT EXISTS \"p_ci_build_needs\" (
                LIKE \"ci_build_needs\" INCLUDING ALL
            ) PARTITION BY LIST(\"partition_id\")
            /*application:web,line:/lib/gitlab/database/partitioning/list/convert_table.rb:195*/",
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT partstrat, partattrs
                 FROM pg_partitioned_table p
                 JOIN pg_class c ON c.oid = p.partrelid
                 WHERE c.relname = 'p_ci_build_needs'",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("l".to_string()),
            SqlValue::String("2".to_string()),
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT conname, contype, conkey, convalidated
                 FROM pg_constraint con
                 JOIN pg_class c ON c.oid = con.conrelid
                 WHERE c.relname = 'p_ci_build_needs'
                   AND con.contype IN ('p', 'c')
                 ORDER BY con.contype DESC",
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("ci_build_needs_pkey".to_string()),
                SqlValue::String("p".to_string()),
                SqlValue::String("1 2".to_string()),
                SqlValue::Bool(true),
            ],
            vec![
                SqlValue::String("partitioning_constraint".to_string()),
                SqlValue::String("c".to_string()),
                SqlValue::String("2".to_string()),
                SqlValue::Bool(false),
            ],
        ]
    );
}

#[test]
fn remaining_unsupported_type_breadth_surfaces_clear_errors() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let numeric_error = session.execute("SELECT 'abc'::numeric").unwrap_err();
    assert_eq!(numeric_error.sqlstate(), "22P02");

    let float_scale_error = session
        .execute("CREATE TABLE invalid_float (id TEXT PRIMARY KEY, value FLOAT(8, 2))")
        .unwrap_err();
    assert_eq!(float_scale_error.sqlstate(), "0A000");
    assert!(float_scale_error
        .to_string()
        .contains("FLOAT with scale is not supported"));
}

#[test]
fn persistent_enum_catalog_matches_cognee_ddl_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let (pipeline_oid, pipeline_array_oid, sync_oid) = {
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        let missing_schema = session
            .execute("CREATE TYPE missing_namespace.status AS ENUM ('NO')")
            .unwrap_err();
        assert_eq!(missing_schema.sqlstate(), "3F000");
        session
            .execute("CREATE TYPE public.text AS ENUM ('CUSTOM')")
            .unwrap();
        session.execute("DROP TYPE public.text").unwrap();
        session
            .execute(
                "CREATE TYPE pipelinerunstatus AS ENUM (\
                 'DATASET_PROCESSING_INITIATED', \
                 'DATASET_PROCESSING_STARTED', \
                 'DATASET_PROCESSING_COMPLETED', \
                 'DATASET_PROCESSING_ERRORED')",
            )
            .unwrap();
        session
            .execute(
                "CREATE TYPE syncstatus AS ENUM (\
                 'STARTED', 'IN_PROGRESS', 'COMPLETED', 'FAILED', 'CANCELLED')",
            )
            .unwrap();

        let duplicate = session
            .execute("CREATE TYPE syncstatus AS ENUM ('OTHER')")
            .unwrap_err();
        assert_eq!(duplicate.sqlstate(), "42710");
        let duplicate_label = session
            .execute("CREATE TYPE invalid_status AS ENUM ('same', 'same')")
            .unwrap_err();
        assert_eq!(duplicate_label.sqlstate(), "42710");

        let type_rows = session
            .execute(
                "SELECT oid, typarray, typnamespace, typowner, typtype, typcategory, \
                 typinput, typoutput, typreceive, typsend \
                 FROM pg_type WHERE typname = 'pipelinerunstatus'",
            )
            .unwrap()
            .rows;
        assert_eq!(type_rows.len(), 1);
        assert_eq!(type_rows[0][2], SqlValue::Int(2200));
        assert_eq!(type_rows[0][3], SqlValue::Int(10));
        assert_eq!(type_rows[0][4], SqlValue::String("e".to_string()));
        assert_eq!(type_rows[0][5], SqlValue::String("E".to_string()));
        assert_eq!(type_rows[0][6], SqlValue::String("enum_in".to_string()));
        assert_eq!(type_rows[0][7], SqlValue::String("enum_out".to_string()));
        assert_eq!(type_rows[0][8], SqlValue::String("enum_recv".to_string()));
        assert_eq!(type_rows[0][9], SqlValue::String("enum_send".to_string()));
        let SqlValue::Int(pipeline_oid) = type_rows[0][0].clone() else {
            panic!("enum type oid must be an integer")
        };
        let SqlValue::Int(pipeline_array_oid) = type_rows[0][1].clone() else {
            panic!("enum array oid must be an integer")
        };
        assert_ne!(pipeline_oid, pipeline_array_oid);
        assert_eq!(
            session
                .execute(&format!("SELECT {pipeline_oid}::regtype::text"))
                .unwrap()
                .rows,
            vec![vec![SqlValue::String("pipelinerunstatus".to_string())]]
        );

        let array_row = session
            .execute(&format!(
                "SELECT typelem, typcategory FROM pg_type WHERE oid = {pipeline_array_oid}"
            ))
            .unwrap()
            .rows;
        assert_eq!(
            array_row,
            vec![vec![
                SqlValue::Int(pipeline_oid),
                SqlValue::String("A".to_string())
            ]]
        );

        session
            .execute(
                "ALTER TYPE pipelinerunstatus ADD VALUE 'DATASET_PROCESSING_QUEUED' \
                 BEFORE 'DATASET_PROCESSING_STARTED'",
            )
            .unwrap();
        session
            .execute(
                "ALTER TYPE pipelinerunstatus ADD VALUE IF NOT EXISTS \
                 'DATASET_PROCESSING_QUEUED'",
            )
            .unwrap();
        session
            .execute(
                "ALTER TYPE pipelinerunstatus RENAME VALUE \
                 'DATASET_PROCESSING_ERRORED' TO 'DATASET_PROCESSING_FAILED'",
            )
            .unwrap();
        let labels = session
            .execute(&format!(
                "SELECT enumlabel FROM pg_enum WHERE enumtypid = {pipeline_oid} \
                 ORDER BY enumsortorder"
            ))
            .unwrap()
            .rows;
        assert_eq!(
            labels,
            [
                "DATASET_PROCESSING_INITIATED",
                "DATASET_PROCESSING_QUEUED",
                "DATASET_PROCESSING_STARTED",
                "DATASET_PROCESSING_COMPLETED",
                "DATASET_PROCESSING_FAILED",
            ]
            .into_iter()
            .map(|label| vec![SqlValue::String(label.to_string())])
            .collect::<Vec<_>>()
        );

        let sync_oid_value = session
            .execute("SELECT oid FROM pg_type WHERE typname = 'syncstatus'")
            .unwrap()
            .rows[0][0]
            .clone();
        let SqlValue::Int(sync_oid) = sync_oid_value else {
            panic!("enum type oid must be an integer")
        };
        session.execute("BEGIN").unwrap();
        session.execute("DROP TYPE syncstatus").unwrap();
        assert!(session
            .execute("SELECT oid FROM pg_type WHERE typname = 'syncstatus'")
            .unwrap()
            .rows
            .is_empty());
        session.execute("ROLLBACK").unwrap();
        assert_eq!(
            session
                .execute("SELECT oid FROM pg_type WHERE typname = 'syncstatus'")
                .unwrap()
                .rows[0][0],
            SqlValue::Int(sync_oid)
        );

        session.execute("BEGIN").unwrap();
        session
            .execute("ALTER TYPE syncstatus ADD VALUE 'ROLLED_BACK'")
            .unwrap();
        session
            .execute("ALTER TYPE syncstatus RENAME TO renamed_syncstatus")
            .unwrap();
        session.execute("ROLLBACK").unwrap();
        assert_eq!(
            session
                .execute("SELECT oid FROM pg_type WHERE typname = 'syncstatus'")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(sync_oid)]]
        );
        assert!(session
            .execute("SELECT oid FROM pg_type WHERE typname = 'renamed_syncstatus'")
            .unwrap()
            .rows
            .is_empty());
        assert!(session
            .execute(&format!(
                "SELECT enumlabel FROM pg_enum WHERE enumtypid = {sync_oid} \
                 AND enumlabel = 'ROLLED_BACK'"
            ))
            .unwrap()
            .rows
            .is_empty());

        session.execute("BEGIN").unwrap();
        session
            .execute("CREATE TYPE rolled_back_status AS ENUM ('NO')")
            .unwrap();
        session.execute("ROLLBACK").unwrap();
        assert!(session
            .execute("SELECT oid FROM pg_type WHERE typname = 'rolled_back_status'")
            .unwrap()
            .rows
            .is_empty());
        (pipeline_oid, pipeline_array_oid, sync_oid)
    };

    let mut reopened = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut reopened);
    assert_eq!(
        session
            .execute("SELECT oid, typarray FROM pg_type WHERE typname = 'pipelinerunstatus'")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(pipeline_oid),
            SqlValue::Int(pipeline_array_oid)
        ]]
    );
    assert_eq!(
        session
            .execute("SELECT oid FROM pg_type WHERE typname = 'syncstatus'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(sync_oid)]]
    );
    session.execute("DROP TYPE syncstatus").unwrap();
    session.execute("DROP TYPE IF EXISTS syncstatus").unwrap();
    let missing = session.execute("DROP TYPE syncstatus").unwrap_err();
    assert_eq!(missing.sqlstate(), "42704");
}

#[test]
fn enum_columns_validate_order_rename_and_track_dependencies() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TYPE priority AS ENUM ('medium', 'low', 'high')")
        .unwrap();
    let type_oid = session
        .execute("SELECT oid FROM pg_type WHERE typname = 'priority'")
        .unwrap()
        .rows[0][0]
        .clone();
    session
        .execute(
            "CREATE TABLE enum_rows (\
             id TEXT PRIMARY KEY, status priority NOT NULL DEFAULT 'medium', \
             history priority[])",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO enum_rows (id, status, history) VALUES \
             ('high', 'high', '{medium,high}'), \
             ('medium', 'medium', '{medium}'), \
             ('low', 'low', '{low,NULL}')",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT id FROM enum_rows WHERE id <> 'defaulted' ORDER BY status")
            .unwrap()
            .rows,
        ["medium", "low", "high"]
            .into_iter()
            .map(|id| vec![SqlValue::String(id.to_string())])
            .collect::<Vec<_>>()
    );
    assert_eq!(
        session.execute("SELECT 'low'::priority").unwrap().rows,
        vec![vec![SqlValue::String("low".to_string())]]
    );
    session
        .execute("CREATE INDEX idx_enum_rows_status ON enum_rows (status)")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT id FROM enum_rows WHERE status = 'low'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("low".to_string())]]
    );
    assert_eq!(
        session
            .execute("SELECT id FROM enum_rows WHERE status > 'low' ORDER BY status")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("high".to_string())]]
    );
    session
        .execute("INSERT INTO enum_rows (id) VALUES ('defaulted')")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT status FROM enum_rows WHERE id = 'defaulted'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("medium".to_string())]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT status, COUNT(*) FROM enum_rows \
                 GROUP BY status ORDER BY status"
            )
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("medium".to_string()), SqlValue::Int(2)],
            vec![SqlValue::String("low".to_string()), SqlValue::Int(1)],
            vec![SqlValue::String("high".to_string()), SqlValue::Int(1)],
        ]
    );
    assert_eq!(
        session
            .execute(
                "SELECT atttypid FROM pg_attribute \
                 WHERE attrelid = 'enum_rows'::regclass AND attname = 'status'"
            )
            .unwrap()
            .rows,
        vec![vec![type_oid.clone()]]
    );
    for sql in [
        "INSERT INTO enum_rows (id, status) VALUES ('invalid', 'urgent')",
        "UPDATE enum_rows SET history = '{missing}' WHERE id = 'medium'",
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), "22P02", "{sql}: {error}");
    }

    session
        .execute("ALTER TYPE priority ADD VALUE 'urgent' BEFORE 'high'")
        .unwrap();
    session
        .execute("INSERT INTO enum_rows (id, status) VALUES ('urgent', 'urgent')")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT id FROM enum_rows WHERE id <> 'defaulted' ORDER BY status")
            .unwrap()
            .rows,
        ["medium", "low", "urgent", "high"]
            .into_iter()
            .map(|id| vec![SqlValue::String(id.to_string())])
            .collect::<Vec<_>>()
    );

    session
        .execute("ALTER TYPE priority RENAME VALUE 'low' TO 'normal'")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT status FROM enum_rows WHERE id = 'low'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("normal".to_string())]]
    );
    assert_eq!(
        session
            .execute("SELECT history FROM enum_rows WHERE id = 'low'")
            .unwrap()
            .rows[0][0]
            .to_cell(),
        "[\"normal\",null]"
    );

    let restricted = session.execute("DROP TYPE priority").unwrap_err();
    assert_eq!(restricted.sqlstate(), "2BP01");
    session.execute("BEGIN").unwrap();
    session
        .execute("ALTER TYPE priority RENAME VALUE 'normal' TO 'rolled_back'")
        .unwrap();
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session
            .execute("SELECT status FROM enum_rows WHERE id = 'low'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("normal".to_string())]]
    );

    session
        .execute("ALTER TYPE priority RENAME TO task_priority")
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT atttypid FROM pg_attribute \
                 WHERE attrelid = 'enum_rows'::regclass AND attname = 'status'"
            )
            .unwrap()
            .rows,
        vec![vec![type_oid]]
    );
    session
        .execute("INSERT INTO enum_rows (id, status) VALUES ('renamed', 'normal')")
        .unwrap();
    session.execute("DROP TYPE task_priority CASCADE").unwrap();
    let missing_status = session.execute("SELECT status FROM enum_rows").unwrap_err();
    assert_eq!(missing_status.sqlstate(), "42703");
}

#[test]
fn expression_operator_and_cast_baseline_matches_postgres_shapes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE exprs (id TEXT PRIMARY KEY, name TEXT, age INT, score FLOAT8, active BOOLEAN, joined DATE)",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO exprs (id, name, age, score, active, joined) VALUES \
             ('e1', 'Ada', 36, 9.5, true, '2024-01-02'::date), \
             ('e2', 'Bob', NULL, NULL, false, NULL), \
             ('e3', 'ALICE', 41, 7.0, NULL, '2023-12-31'::date)",
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT id, age + 4 AS age_plus, score * 2 AS doubled, name || '-x' AS label \
                 FROM exprs WHERE id = 'e1'",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("e1".to_string()),
            SqlValue::Int(40),
            SqlValue::Float(19.0),
            SqlValue::String("Ada-x".to_string()),
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT id, CASE WHEN age IS NULL THEN 'missing' WHEN age >= 40 THEN 'senior' ELSE 'adult' END AS band, COALESCE(age::text, 'n/a') AS age_text \
                 FROM exprs ORDER BY id",
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("e1".to_string()),
                SqlValue::String("adult".to_string()),
                SqlValue::String("36".to_string()),
            ],
            vec![
                SqlValue::String("e2".to_string()),
                SqlValue::String("missing".to_string()),
                SqlValue::String("n/a".to_string()),
            ],
            vec![
                SqlValue::String("e3".to_string()),
                SqlValue::String("senior".to_string()),
                SqlValue::String("41".to_string()),
            ],
        ]
    );
    assert_eq!(
        session
            .execute(
                "SELECT id FROM exprs WHERE (active OR age > 40) AND name ILIKE 'a%' ORDER BY id",
            )
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("e1".to_string())],
            vec![SqlValue::String("e3".to_string())],
        ]
    );
    assert_eq!(
        session
            .execute("SELECT id FROM exprs WHERE name LIKE 'A%' AND id IN ('e1', 'e3') ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("e1".to_string())],
            vec![SqlValue::String("e3".to_string())],
        ]
    );
    assert_eq!(
        session
            .execute("SELECT id FROM exprs WHERE age = NULL OR active = NULL ORDER BY id")
            .unwrap()
            .rows,
        Vec::<Vec<SqlValue>>::new()
    );
    assert_eq!(
        session
            .execute("SELECT '123'::int4, 7::text, '2024-01-02'::date")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(123),
            SqlValue::String("7".to_string()),
            SqlValue::String("2024-01-02".to_string()),
        ]]
    );
}

#[test]
fn unsupported_expressions_return_stable_errors() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE expr_unsupported (id TEXT PRIMARY KEY, age INT)")
        .unwrap();
    session
        .execute("INSERT INTO expr_unsupported (id, age) VALUES ('e1', 36)")
        .unwrap();

    let error = session
        .execute("SELECT id FROM expr_unsupported WHERE id SIMILAR TO 'e[0-9]'")
        .unwrap_err();
    assert!(matches!(error, SqlError::Unsupported(_)));
    assert_eq!(error.sqlstate(), "0A000");
}

#[test]
fn create_select_and_drop_view() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT, age INT)")
        .unwrap();
    session
        .execute(
            "INSERT INTO patients (id, name, age) VALUES ('p1', 'Ada', 36), ('p2', 'John', 45)",
        )
        .unwrap();

    assert_eq!(
        session
            .execute("CREATE VIEW patient_names AS SELECT id, name FROM patients")
            .unwrap()
            .command_tag
            .as_deref(),
        Some("CREATE VIEW")
    );
    assert_eq!(
        session
            .execute("SELECT * FROM patient_names ORDER BY id")
            .unwrap(),
        bicdb_sql::SqlResult::new(
            vec!["id".to_string(), "name".to_string()],
            vec![
                vec![
                    SqlValue::String("p1".to_string()),
                    SqlValue::String("Ada".to_string())
                ],
                vec![
                    SqlValue::String("p2".to_string()),
                    SqlValue::String("John".to_string())
                ],
            ],
        )
    );
    assert_eq!(
        session
            .execute("DROP VIEW patient_names")
            .unwrap()
            .command_tag
            .as_deref(),
        Some("DROP VIEW")
    );
    assert!(matches!(
        session.execute("SELECT id FROM patient_names"),
        Err(SqlError::InvalidCollection(name)) if name == "patient_names"
    ));
}

#[test]
fn create_view_does_not_execute_query_body() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE SEQUENCE view_side_effect_seq START WITH 5")
        .unwrap();

    session
        .execute("CREATE VIEW view_side_effects AS SELECT nextval('view_side_effect_seq') AS id")
        .unwrap();

    assert_eq!(
        session
            .execute("SELECT last_value, is_called FROM view_side_effect_seq")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(5), SqlValue::Bool(false)]]
    );
    assert_eq!(
        session
            .execute("SELECT nextval('view_side_effect_seq')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(5)]]
    );
    assert_eq!(
        session
            .execute("SELECT last_value, is_called FROM view_side_effect_seq")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(5), SqlValue::Bool(true)]]
    );
}

#[test]
fn view_over_join_can_be_selected() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT)")
        .unwrap();
    session
        .execute("CREATE TABLE appointments (id TEXT PRIMARY KEY, patient_id TEXT, doctor TEXT)")
        .unwrap();
    session
        .execute("INSERT INTO patients (id, name) VALUES ('p1', 'Ada'), ('p2', 'John')")
        .unwrap();
    session
        .execute("INSERT INTO appointments (id, patient_id, doctor) VALUES ('a1', 'p1', 'Dr. Rao'), ('a2', 'p2', 'Dr. Kim')")
        .unwrap();
    session
        .execute(
            "CREATE VIEW patient_appointments AS \
             SELECT patients.name AS patient_name, appointments.doctor AS doctor \
             FROM patients JOIN appointments ON patients.id = appointments.patient_id",
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT patient_name, doctor FROM patient_appointments WHERE doctor = 'Dr. Rao'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("Ada".to_string()),
            SqlValue::String("Dr. Rao".to_string())
        ]]
    );
}

#[test]
fn gitlab_postgres_partitions_view_query_materializes_catalog_join() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE duo_workflows_events (id bigint PRIMARY KEY)")
        .unwrap();
    session
        .execute(
            r#"
            CREATE VIEW postgres_partitions AS
             SELECT (((pg_namespace.nspname)::text || '.'::text) || (pg_class.relname)::text) AS identifier,
                pg_class.oid,
                pg_namespace.nspname AS schema,
                pg_class.relname AS name,
                (((parent_namespace.nspname)::text || '.'::text) || (parent_class.relname)::text) AS parent_identifier,
                pg_get_expr(pg_class.relpartbound, pg_inherits.inhrelid) AS condition
               FROM ((((pg_class
                 JOIN pg_namespace ON ((pg_namespace.oid = pg_class.relnamespace)))
                 JOIN pg_inherits ON ((pg_class.oid = pg_inherits.inhrelid)))
                 JOIN pg_class parent_class ON ((pg_inherits.inhparent = parent_class.oid)))
                 JOIN pg_namespace parent_namespace ON ((parent_class.relnamespace = parent_namespace.oid)))
              WHERE (pg_class.relispartition AND (pg_namespace.nspname = ANY (ARRAY["current_schema"(), 'gitlab_partitions_dynamic'::name, 'gitlab_partitions_static'::name])))
            "#,
        )
        .unwrap();

    assert_eq!(
        session
            .execute("SELECT concat(current_schema(), '.', 'duo_workflows_events')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(
            "public.duo_workflows_events".to_string()
        )]]
    );
    assert_eq!(
        session
            .execute(
                r#"SELECT 1 AS one FROM "postgres_partitions" WHERE (identifier = concat(current_schema(), '.', 'duo_workflows_events')) LIMIT 1"#
            )
            .unwrap(),
        bicdb_sql::SqlResult::new(vec!["one".to_string()], Vec::new())
            .with_column_types(vec![Some("int4".to_string())])
    );

    session
        .execute(
            r#"
            CREATE VIEW postgres_partitioned_tables AS
             SELECT (((pg_namespace.nspname)::text || '.'::text) || (pg_class.relname)::text) AS identifier,
                pg_class.oid,
                pg_namespace.nspname AS schema,
                pg_class.relname AS name,
                    CASE partitioned_tables.partstrat
                        WHEN 'l'::"char" THEN 'list'::text
                        WHEN 'r'::"char" THEN 'range'::text
                        WHEN 'h'::"char" THEN 'hash'::text
                        ELSE NULL::text
                    END AS strategy,
                array_agg(pg_attribute.attname) AS key_columns
               FROM (((( SELECT pg_partitioned_table.partrelid,
                        pg_partitioned_table.partstrat,
                        unnest(pg_partitioned_table.partattrs) AS column_position
                       FROM pg_partitioned_table) partitioned_tables
                 JOIN pg_class ON ((partitioned_tables.partrelid = pg_class.oid)))
                 JOIN pg_namespace ON ((pg_class.relnamespace = pg_namespace.oid)))
                 JOIN pg_attribute ON (((pg_attribute.attrelid = pg_class.oid) AND (pg_attribute.attnum = partitioned_tables.column_position))))
              WHERE (pg_namespace.nspname = "current_schema"())
              GROUP BY (((pg_namespace.nspname)::text || '.'::text) || (pg_class.relname)::text), pg_class.oid, pg_namespace.nspname, pg_class.relname,
                    CASE partitioned_tables.partstrat
                        WHEN 'l'::"char" THEN 'list'::text
                        WHEN 'r'::"char" THEN 'range'::text
                        WHEN 'h'::"char" THEN 'hash'::text
                        ELSE NULL::text
                    END
            "#,
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                r#"SELECT "postgres_partitioned_tables".* FROM "postgres_partitioned_tables" WHERE "postgres_partitioned_tables"."identifier" = 'public.audit_events' LIMIT 1"#
            )
            .unwrap(),
        bicdb_sql::SqlResult::new(
            vec![
                "identifier".to_string(),
                "oid".to_string(),
                "schema".to_string(),
                "name".to_string(),
                "strategy".to_string(),
                "key_columns".to_string(),
            ],
            Vec::new()
        )
        // The wildcard over the view now reports each column's declared type
        // (all text in this catalog view's schema) instead of leaving them
        // untyped for the wire layer to guess from values.
        .with_column_types(vec![Some("text".to_string()); 6])
    );
}

#[test]
fn generate_series_table_function_feeds_insert_select_with_conflict_handling() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE geo_ci_job_artifact_verification_summaries (
                bucket_number integer PRIMARY KEY,
                total_count integer NOT NULL,
                verified_count integer NOT NULL,
                failed_count integer NOT NULL,
                state integer NOT NULL,
                state_changed_at text NOT NULL,
                created_at text NOT NULL,
                updated_at text NOT NULL
            )",
        )
        .unwrap();

    let insert = "INSERT INTO geo_ci_job_artifact_verification_summaries
        (bucket_number, total_count, verified_count, failed_count, state, state_changed_at, created_at, updated_at)
        SELECT n, 0, 0, 0, 1, NOW(), NOW(), NOW()
        FROM generate_series(0, 3) AS s(n)
        ON CONFLICT (bucket_number) DO NOTHING";

    session.execute(insert).unwrap();
    session.execute(insert).unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT bucket_number, total_count, verified_count, failed_count, state
                 FROM geo_ci_job_artifact_verification_summaries
                 ORDER BY bucket_number"
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::Int(0),
                SqlValue::Int(0),
                SqlValue::Int(0),
                SqlValue::Int(0),
                SqlValue::Int(1),
            ],
            vec![
                SqlValue::Int(1),
                SqlValue::Int(0),
                SqlValue::Int(0),
                SqlValue::Int(0),
                SqlValue::Int(1),
            ],
            vec![
                SqlValue::Int(2),
                SqlValue::Int(0),
                SqlValue::Int(0),
                SqlValue::Int(0),
                SqlValue::Int(1),
            ],
            vec![
                SqlValue::Int(3),
                SqlValue::Int(0),
                SqlValue::Int(0),
                SqlValue::Int(0),
                SqlValue::Int(1),
            ],
        ]
    );
}

#[test]
fn generate_series_table_function_projects_alias_columns_and_descending_steps() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute("SELECT n FROM generate_series(5, 1, -2) AS s(n) ORDER BY n DESC")
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::Int(5)],
            vec![SqlValue::Int(3)],
            vec![SqlValue::Int(1)]
        ]
    );
}

#[test]
fn single_column_table_function_alias_is_visible_as_output_column() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute("SELECT scalar_item FROM UNNEST(ARRAY[4, 5]) AS scalar_item")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(4)], vec![SqlValue::Int(5)]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT scalar_item
                 FROM UNNEST('[0:1][5:6]={{1,2},{3,4}}'::int4[]) AS scalar_item",
            )
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::Int(1)],
            vec![SqlValue::Int(2)],
            vec![SqlValue::Int(3)],
            vec![SqlValue::Int(4)],
        ]
    );
    assert_eq!(
        session
            .execute("SELECT generated_value FROM generate_series(1, 2) AS generated_value")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(2)]]
    );
}

#[test]
fn transactional_insert_select_from_generate_series_commits_bulk_rows() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE TABLE tx_series_jobs (id integer PRIMARY KEY)")
        .unwrap();
    session.execute("BEGIN").unwrap();
    session
        .execute(
            "INSERT INTO tx_series_jobs (id)
             SELECT n FROM generate_series(0, 999) AS s(n)",
        )
        .unwrap();
    session.execute("COMMIT").unwrap();

    assert_eq!(
        session
            .execute("SELECT COUNT(*) FROM tx_series_jobs")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1_000)]]
    );
}

#[test]
fn transactional_generate_series_on_conflict_do_nothing_commits_bulk_rows() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE tx_conflict_series_jobs (
                id bigserial PRIMARY KEY,
                bucket_number integer NOT NULL,
                total_count integer NOT NULL,
                verified_count integer NOT NULL,
                failed_count integer NOT NULL,
                state integer NOT NULL,
                state_changed_at text NOT NULL,
                created_at text NOT NULL,
                updated_at text NOT NULL
            )",
        )
        .unwrap();
    session
        .execute(
            "CREATE UNIQUE INDEX idx_tx_conflict_series_jobs_on_bucket
             ON tx_conflict_series_jobs (bucket_number)",
        )
        .unwrap();

    let insert = "INSERT INTO tx_conflict_series_jobs
        (bucket_number, total_count, verified_count, failed_count, state, state_changed_at, created_at, updated_at)
        SELECT n, 0, 0, 0, 1, NOW(), NOW(), NOW()
        FROM generate_series(0, 99999) AS s(n)
        ON CONFLICT (bucket_number) DO NOTHING";

    session.execute("BEGIN").unwrap();
    session.execute(insert).unwrap();
    session.execute("COMMIT").unwrap();

    session.execute("BEGIN").unwrap();
    session.execute(insert).unwrap();
    session.execute("COMMIT").unwrap();

    assert_eq!(
        session
            .execute("SELECT COUNT(*) FROM tx_conflict_series_jobs")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(100_000)]]
    );
}

#[test]
fn pg_control_system_table_function_supports_gitlab_db_identifier_query() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE ar_internal_metadata (
                key text PRIMARY KEY,
                value text,
                created_at text
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO ar_internal_metadata (key, value, created_at)
             VALUES ('gitlab_db_config_name', 'main', '2026-06-22 00:00:00')",
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT system_identifier, current_database(), value AS db_config_name, created_at AS timestamp
                 FROM pg_control_system()
                 LEFT JOIN ar_internal_metadata ON ar_internal_metadata.key = 'gitlab_db_config_name'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(1_802_028_600_100),
            SqlValue::String("bicdb".to_string()),
            SqlValue::String("main".to_string()),
            SqlValue::String("2026-06-22 00:00:00".to_string())
        ]]
    );
}

#[test]
fn update_uses_non_id_primary_key_lookup_for_absent_rows() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE ar_internal_metadata (
                key text PRIMARY KEY,
                value text,
                created_at text,
                updated_at text
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO ar_internal_metadata (key, value, created_at, updated_at)
             VALUES ('gitlab_db_config_name', 'main', '2026-06-22 00:00:00', '2026-06-22 00:00:00')",
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "UPDATE ar_internal_metadata
                 SET value = 'ci', updated_at = '2026-06-22 01:00:00'
                 WHERE ar_internal_metadata.key = 'missing_db_config_name'",
            )
            .unwrap()
            .command_tag,
        Some("UPDATE 0".to_string())
    );
    assert_eq!(
        session
            .execute("SELECT value FROM ar_internal_metadata WHERE key = 'gitlab_db_config_name'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("main".to_string())]]
    );

    assert_eq!(
        session
            .execute(
                "UPDATE ar_internal_metadata AS metadata
                 SET value = 'ci', updated_at = '2026-06-22 01:00:00'
                 WHERE metadata.key = 'gitlab_db_config_name'",
            )
            .unwrap()
            .command_tag,
        Some("UPDATE 1".to_string())
    );
    assert_eq!(
        session
            .execute("SELECT value FROM ar_internal_metadata WHERE key = 'gitlab_db_config_name'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("ci".to_string())]]
    );
}

#[test]
fn complex_create_view_falls_back_to_projection_metadata() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE group_wiki_repositories (id bigint PRIMARY KEY, group_id bigint, shard_id bigint, disk_path text)")
        .unwrap();
    session
        .execute("CREATE TABLE routes (id bigint PRIMARY KEY, source_id bigint, source_type text, path text, name text)")
        .unwrap();
    session
        .execute("CREATE TABLE shards (id bigint PRIMARY KEY, name text)")
        .unwrap();

    session
        .execute(
            r#"
            CREATE VIEW group_wikis_routes_view AS
             SELECT gr.group_id,
                sh.name AS repository_storage,
                gr.disk_path,
                r.path AS path_with_namespace,
                r.name AS name_with_namespace
               FROM ((group_wiki_repositories gr
                 JOIN routes r ON (((r.source_id = gr.group_id) AND ((r.source_type)::text = 'Namespace'::text))))
                 JOIN shards sh ON ((gr.shard_id = sh.id)))
            "#,
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT column_name FROM information_schema.columns \
                 WHERE table_name = 'group_wikis_routes_view' \
                 ORDER BY ordinal_position"
            )
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("group_id".to_string())],
            vec![SqlValue::String("repository_storage".to_string())],
            vec![SqlValue::String("disk_path".to_string())],
            vec![SqlValue::String("path_with_namespace".to_string())],
            vec![SqlValue::String("name_with_namespace".to_string())],
        ]
    );

    session
        .execute(
            r#"
            CREATE VIEW postgres_autovacuum_activity AS
             WITH processes AS (
               SELECT activity.query,
                  activity.query_start,
                  regexp_matches(activity.query, '^autovacuum: VACUUM (\w+)\.(\w+)'::text) AS matches,
                  CASE
                    WHEN (activity.query ~~* '%wraparound)'::text) THEN true
                    ELSE false
                  END AS wraparound_prevention
                 FROM postgres_pg_stat_activity_autovacuum() activity(query, query_start)
             )
             SELECT ((matches[1] || '.'::text) || matches[2]) AS table_identifier,
                matches[1] AS schema,
                matches[2] AS "table",
                query_start AS vacuum_start,
                wraparound_prevention
               FROM processes
            "#,
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT column_name FROM information_schema.columns \
                 WHERE table_name = 'postgres_autovacuum_activity' \
                 ORDER BY ordinal_position"
            )
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("table_identifier".to_string())],
            vec![SqlValue::String("schema".to_string())],
            vec![SqlValue::String("table".to_string())],
            vec![SqlValue::String("vacuum_start".to_string())],
            vec![SqlValue::String("wraparound_prevention".to_string())],
        ]
    );
}

#[test]
fn views_are_exposed_through_information_schema_and_pg_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT)")
        .unwrap();
    session
        .execute(
            "CREATE VIEW patient_names (patient_id, patient_name) AS SELECT id, name FROM patients",
        )
        .unwrap();

    assert_eq!(
        session
            .execute("SELECT table_name, table_type FROM information_schema.tables WHERE table_name = 'patient_names'")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("patient_names".to_string()),
            SqlValue::String("VIEW".to_string())
        ]]
    );
    assert_eq!(
        session
            .execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'patient_names' ORDER BY ordinal_position")
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("patient_id".to_string())],
            vec![SqlValue::String("patient_name".to_string())],
        ]
    );
    assert_eq!(
        session
            .execute(
                "SELECT relname, relkind, relhasrules FROM pg_catalog.pg_class WHERE relname = 'patient_names'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("patient_names".to_string()),
            SqlValue::String("v".to_string()),
            SqlValue::Bool(true)
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT r.rulename, r.ev_type, r.is_instead, r.ev_enabled
                 FROM pg_catalog.pg_rewrite r
                 JOIN pg_catalog.pg_class c ON c.oid = r.ev_class
                 WHERE c.relname = 'patient_names'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("_RETURN".to_string()),
            SqlValue::String("1".to_string()),
            SqlValue::Bool(true),
            SqlValue::String("O".to_string())
        ]]
    );
}

#[test]
fn postgres_pg_get_viewdef_renders_stored_and_virtual_view_definitions() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT)")
        .unwrap();
    session
        .execute("CREATE VIEW patient_names AS SELECT id, name FROM patients")
        .unwrap();

    let patient_view_oid = match session
        .execute("SELECT oid FROM pg_catalog.pg_class WHERE relname = 'patient_names'")
        .unwrap()
        .rows[0][0]
    {
        SqlValue::Int(oid) => oid,
        ref other => panic!("expected view oid, got {other:?}"),
    };
    let graph_view_oid = match session
        .execute("SELECT oid FROM pg_catalog.pg_class WHERE relname = 'bicdb_graph_edges'")
        .unwrap()
        .rows[0][0]
    {
        SqlValue::Int(oid) => oid,
        ref other => panic!("expected graph view oid, got {other:?}"),
    };
    assert_eq!(
        session
            .execute(&format!(
                "SELECT pg_catalog.pg_get_viewdef('{patient_view_oid}'::pg_catalog.oid)"
            ))
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(
            "SELECT id, name FROM patients;".to_string()
        )]]
    );
    assert_eq!(
        session
            .execute(&format!(
                "SELECT pg_catalog.pg_get_viewdef('{graph_view_oid}'::pg_catalog.oid)"
            ))
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(
            "SELECT NULL::text AS projection, NULL::text AS id, NULL::text AS \"from\", NULL::text AS \"to\", NULL::text AS label, NULL::jsonb AS properties, NULL::bigint AS timestamp WHERE false;".to_string()
        )]]
    );
}

#[test]
fn unsupported_view_behaviors_return_clear_errors() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT)")
        .unwrap();
    session
        .execute("CREATE VIEW patient_names AS SELECT id, name FROM patients")
        .unwrap();

    assert_eq!(
        session
            .execute("CREATE MATERIALIZED VIEW patient_names_mat AS SELECT id FROM patients")
            .unwrap()
            .command_complete_tag(),
        "CREATE MATERIALIZED VIEW"
    );
    assert_eq!(
        session
            .execute(
                "SELECT relname, relkind, relhasrules
                 FROM pg_catalog.pg_class
                 WHERE relname = 'patient_names_mat'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("patient_names_mat".to_string()),
            SqlValue::String("m".to_string()),
            SqlValue::Bool(true),
        ]]
    );
    assert_eq!(
        session
            .execute(
                "CREATE INDEX idx_on_account_id_language_sensitive_250461e1eb
                 ON patient_names_mat (id)"
            )
            .unwrap()
            .command_complete_tag(),
        "CREATE INDEX"
    );
    assert_eq!(
        session
            .execute(
                "SELECT i.relname, d.indisprimary, d.indisunique, d.indkey, pg_get_indexdef(d.indexrelid)
                 FROM pg_catalog.pg_class t
                 INNER JOIN pg_catalog.pg_index d ON t.oid = d.indrelid
                 INNER JOIN pg_catalog.pg_class i ON d.indexrelid = i.oid
                 WHERE t.relname = 'patient_names_mat'
                 ORDER BY i.relname"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("idx_on_account_id_language_sensitive_250461e1eb".to_string()),
            SqlValue::Bool(false),
            SqlValue::Bool(false),
            SqlValue::String("0".to_string()),
            SqlValue::String(
                "CREATE INDEX idx_on_account_id_language_sensitive_250461e1eb ON patient_names_mat USING btree (id)"
                    .to_string()
            ),
        ]]
    );

    let err = session
        .execute("INSERT INTO patient_names (id, name) VALUES ('p1', 'Ada')")
        .unwrap_err();
    assert_eq!(err.sqlstate(), "0A000");
    assert!(err.to_string().contains("views"));

    let err = session
        .execute("UPDATE patient_names SET name = 'Ada'")
        .unwrap_err();
    assert_eq!(err.sqlstate(), "0A000");
    assert!(err.to_string().contains("views"));
}

#[test]
fn deferrable_constraints_are_accepted_and_enforced_immediately() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE parents (id TEXT PRIMARY KEY)")
        .unwrap();

    session
        .execute(
            "CREATE TABLE children (
                id TEXT PRIMARY KEY,
                parent_id TEXT,
                CONSTRAINT children_parent_fkey
                    FOREIGN KEY (parent_id) REFERENCES parents(id)
                    DEFERRABLE INITIALLY DEFERRED
            )",
        )
        .unwrap();

    let err = session
        .execute("INSERT INTO children (id, parent_id) VALUES ('c1', 'missing')")
        .unwrap_err();
    assert_eq!(err.sqlstate(), "23503");
}

#[test]
fn foreign_key_set_default_delete_and_rollback() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE parents (id TEXT PRIMARY KEY)")
        .unwrap();
    session
        .execute(
            "CREATE TABLE children (
                id TEXT PRIMARY KEY,
                parent_id TEXT DEFAULT 'fallback',
                CONSTRAINT children_parent_fkey
                    FOREIGN KEY (parent_id) REFERENCES parents(id)
                    ON DELETE SET DEFAULT
            )",
        )
        .unwrap();
    session
        .execute("INSERT INTO parents (id) VALUES ('fallback'), ('p1')")
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
            .execute("SELECT parent_id FROM children WHERE id = 'c1'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("p1".to_string())]]
    );

    session
        .execute("DELETE FROM parents WHERE id = 'p1'")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT parent_id FROM children WHERE id = 'c1'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("fallback".to_string())]]
    );
}

#[test]
fn not_valid_constraint_can_be_validated_after_backfill() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE parents (id TEXT PRIMARY KEY)")
        .unwrap();
    session
        .execute("CREATE TABLE children (id TEXT PRIMARY KEY, parent_id TEXT)")
        .unwrap();
    session
        .execute("INSERT INTO children (id, parent_id) VALUES ('c1', 'missing')")
        .unwrap();
    session
        .execute(
            "ALTER TABLE children
             ADD CONSTRAINT children_parent_fkey
             FOREIGN KEY (parent_id) REFERENCES parents(id) NOT VALID",
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT convalidated FROM pg_catalog.pg_constraint
                 WHERE conname = 'children_parent_fkey'"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(false)]]
    );
    let err = session
        .execute("INSERT INTO children (id, parent_id) VALUES ('c2', 'missing')")
        .unwrap_err();
    assert_eq!(err.sqlstate(), "23503");
    let err = session
        .execute("ALTER TABLE children VALIDATE CONSTRAINT children_parent_fkey")
        .unwrap_err();
    assert_eq!(err.sqlstate(), "23503");

    session
        .execute("INSERT INTO parents (id) VALUES ('missing')")
        .unwrap();
    session
        .execute("ALTER TABLE children VALIDATE CONSTRAINT children_parent_fkey")
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT convalidated FROM pg_catalog.pg_constraint
                 WHERE conname = 'children_parent_fkey'"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]]
    );
}

#[test]
fn sql_integrity_check_reports_invalid_foreign_key() {
    let (_dir, mut db) = empty_test_db();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE parents (id TEXT PRIMARY KEY)")
            .unwrap();
        session
            .execute("CREATE TABLE children (id TEXT PRIMARY KEY, parent_id TEXT)")
            .unwrap();
        session
            .execute("INSERT INTO children (id, parent_id) VALUES ('c1', 'missing')")
            .unwrap();
        session
            .execute(
                "ALTER TABLE children
                 ADD CONSTRAINT children_parent_fkey
                 FOREIGN KEY (parent_id) REFERENCES parents(id) NOT VALID",
            )
            .unwrap();
    }

    let report = integrity_check(&mut db).unwrap();
    assert!(!report.valid);
    assert!(report.violations.iter().any(|violation| {
        violation.check == "foreign_key"
            && violation.table.as_deref() == Some("children")
            && violation.object.as_deref() == Some("children_parent_fkey")
            && violation.record_id.as_deref() == Some("c1")
    }));
}

#[test]
fn prefix_ordered_index_scan_serves_latest_row_per_prefix_without_full_scan() {
    // The TPC-C "latest order for a customer" shape: an equality prefix on a
    // composite index plus ORDER BY the next index field with LIMIT 1. This must
    // use the bounded IndexPrefixOrderedScan (not fetch-all-prefix-then-sort) and
    // return the correct extreme.
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE orders (w INT, d INT, c INT, o INT, carrier INT, \
             PRIMARY KEY (w, d, o))",
        )
        .unwrap();
    session
        .execute("CREATE INDEX orders_cust ON orders (w, d, c, o)")
        .unwrap();
    for o in 1..=25 {
        let carrier = o % 2;
        session
            .execute(&format!(
                "INSERT INTO orders (w,d,c,o,carrier) VALUES (1,2,7,{o},{carrier})"
            ))
            .unwrap();
    }
    // A different customer's rows (distinct o, since the PK is (w,d,o)) must never
    // leak into customer 7's prefix result.
    for o in 101..=105 {
        session
            .execute(&format!(
                "INSERT INTO orders (w,d,c,o,carrier) VALUES (1,2,8,{o},0)"
            ))
            .unwrap();
    }

    // Plan selection: the bounded prefix-ordered scan is chosen.
    let plan = session
        .execute("EXPLAIN SELECT o FROM orders WHERE w=1 AND d=2 AND c=7 ORDER BY o DESC LIMIT 1")
        .unwrap();
    assert!(
        plan.rows.iter().any(|row| row[0]
            .to_cell()
            .contains("IndexPrefixOrderedScan orders_cust")),
        "expected IndexPrefixOrderedScan, got plan: {:?}",
        plan.rows
    );

    // Correctness: max for the customer.
    let latest = session
        .execute("SELECT o FROM orders WHERE w=1 AND d=2 AND c=7 ORDER BY o DESC LIMIT 1")
        .unwrap();
    assert_eq!(latest.rows, vec![vec![SqlValue::Int(25)]]);

    // Ascending (min) and a larger limit, same shape.
    let earliest = session
        .execute("SELECT o FROM orders WHERE w=1 AND d=2 AND c=7 ORDER BY o ASC LIMIT 1")
        .unwrap();
    assert_eq!(earliest.rows, vec![vec![SqlValue::Int(1)]]);
    let top3 = session
        .execute("SELECT o FROM orders WHERE w=1 AND d=2 AND c=7 ORDER BY o DESC LIMIT 3")
        .unwrap();
    assert_eq!(
        top3.rows,
        vec![
            vec![SqlValue::Int(25)],
            vec![SqlValue::Int(24)],
            vec![SqlValue::Int(23)],
        ]
    );

    // GUARD: a residual filter (carrier=0) is NOT part of the index prefix, so the
    // bounded plan must NOT be used (it could drop the true match) — and the result
    // must still be correct (greatest even o is 24).
    let plan_residual = session
        .execute(
            "EXPLAIN SELECT o FROM orders WHERE w=1 AND d=2 AND c=7 AND carrier=0 \
             ORDER BY o DESC LIMIT 1",
        )
        .unwrap();
    assert!(
        !plan_residual
            .rows
            .iter()
            .any(|row| row[0].to_cell().contains("IndexPrefixOrderedScan")),
        "residual filter must disable the bounded plan: {:?}",
        plan_residual.rows
    );
    let latest_even = session
        .execute(
            "SELECT o FROM orders WHERE w=1 AND d=2 AND c=7 AND carrier=0 ORDER BY o DESC LIMIT 1",
        )
        .unwrap();
    assert_eq!(latest_even.rows, vec![vec![SqlValue::Int(24)]]);
}

#[test]
fn postgres_unary_plus_numeric_constants_match_nextcloud_dav_smoke() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let selected = session.execute("SELECT +999999999999").unwrap();
    assert_eq!(selected.rows, vec![vec![SqlValue::Int(999999999999)]]);

    session
        .execute("CREATE TABLE dav_props (id INT PRIMARY KEY, value BIGINT)")
        .unwrap();
    session
        .execute("INSERT INTO dav_props (id, value) VALUES (1, 0)")
        .unwrap();
    session
        .execute("UPDATE dav_props SET value = +999999999999 WHERE id = 1")
        .unwrap();

    let updated = session
        .execute("SELECT value FROM dav_props WHERE id = 1")
        .unwrap();
    assert_eq!(updated.rows, vec![vec![SqlValue::Int(999999999999)]]);
}

#[test]
fn postgres_targetless_on_conflict_do_nothing_ignores_unique_index_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE TABLE file_locks (id BIGSERIAL PRIMARY KEY, key TEXT, ttl BIGINT)")
        .unwrap();
    session
        .execute("CREATE UNIQUE INDEX lock_key_index ON file_locks (key)")
        .unwrap();
    session
        .execute("INSERT INTO file_locks (key, ttl) VALUES ('files/admin', 1)")
        .unwrap();
    session
        .execute(
            "INSERT INTO file_locks (key, ttl) VALUES ('files/admin', 2)
             ON CONFLICT DO NOTHING",
        )
        .unwrap();

    let rows = session
        .execute("SELECT COUNT(*), MAX(ttl) FROM file_locks")
        .unwrap();
    assert_eq!(rows.rows, vec![vec![SqlValue::Int(1), SqlValue::Int(1)]]);
}

#[test]
fn bicdb_replication_virtual_views_expose_safe_status() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let status = session
        .execute(
            "SELECT current_commit_seq, last_applied_commit_seq, retained_commits
             FROM bicdb_replication_status",
        )
        .unwrap();
    assert_eq!(
        status.rows,
        vec![vec![SqlValue::Int(0), SqlValue::Int(0), SqlValue::Int(0)]]
    );

    let lag = session
        .execute(
            "SELECT source_commit_seq, last_applied_commit_seq, lag_commits
             FROM bicdb_replication_lag",
        )
        .unwrap();
    assert_eq!(
        lag.rows,
        vec![vec![SqlValue::Int(0), SqlValue::Int(0), SqlValue::Int(0)]]
    );

    let consensus = session
        .execute(
            "SELECT cluster_id, role, current_term, commit_index, voter_count
             FROM bicdb_consensus_status",
        )
        .unwrap();
    assert_eq!(
        consensus.rows,
        vec![vec![
            SqlValue::String("default".to_string()),
            SqlValue::String("Follower".to_string()),
            SqlValue::Int(0),
            SqlValue::Int(0),
            SqlValue::Int(0),
        ]]
    );
}

#[test]
fn concurrent_neword_payment_mix_never_duplicates_order_ids() {
    // Reproduces the TPC-C ORDERS_I1 duplicate-key race: neword-shaped
    // allocation (UPDATE ... RETURNING under the recheck protocol) interleaved
    // with payment-shaped repair updates on the same district row, driven the
    // way pgwire drives shared sessions (deferred commit + buffered commit +
    // bounded retry with a snapshot floor).
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut setup = SqlSession::new(&mut db);
        setup
            .execute(
                "CREATE TABLE district (d_w_id INT, d_id INT, d_next_o_id INT, d_ytd NUMERIC, \
                 d_name TEXT, PRIMARY KEY (d_w_id, d_id))",
            )
            .unwrap();
        setup
            .execute("INSERT INTO district VALUES (1, 1, 3001, 30000, 'dst')")
            .unwrap();
        setup
            .execute(
                "CREATE TABLE orders (o_id INT, o_d_id INT, o_w_id INT, o_c_id INT, \
                 PRIMARY KEY (o_id, o_d_id, o_w_id))",
            )
            .unwrap();
    }
    let db = &db;

    const NEWORD_THREADS: usize = 6;
    const NEWORD_ITERS: usize = 150;
    const PAYMENT_THREADS: usize = 4;
    const PAYMENT_ITERS: usize = 250;

    let successes = std::sync::atomic::AtomicUsize::new(0);
    let payment_successes = std::sync::atomic::AtomicUsize::new(0);
    let hard_errors: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

    std::thread::scope(|scope| {
        for _ in 0..NEWORD_THREADS {
            scope.spawn(|| {
                let mut last_commit = 0u64;
                for _ in 0..NEWORD_ITERS {
                    // pgwire-shaped bounded retry of the whole "call".
                    'attempts: for _attempt in 0..8 {
                        let mut session = SqlSession::new_shared(db)
                            .with_snapshot_floor(last_commit)
                            .with_deferred_commit();
                        session.execute("SET bicdb.update_repair = on").unwrap();
                        let run = (|| -> std::result::Result<i64, SqlError> {
                            session.execute("BEGIN")?;
                            let allocated = session.execute(
                                "UPDATE district SET d_next_o_id = d_next_o_id + 1 \
                                 WHERE d_w_id = 1 AND d_id = 1 RETURNING d_next_o_id - 1",
                            )?;
                            let SqlValue::Int(o_id) = allocated.rows[0][0] else {
                                panic!("unexpected RETURNING value");
                            };
                            session.execute(&format!(
                                "INSERT INTO orders VALUES ({o_id}, 1, 1, 42)"
                            ))?;
                            Ok(o_id)
                        })();
                        match run {
                            Ok(_o_id) => {
                                let Some(mut tx) = session.take_pending_transaction() else {
                                    break 'attempts;
                                };
                                match db.commit_buffered_transaction(&mut tx) {
                                    Ok(seq) => {
                                        last_commit = last_commit.max(seq);
                                        successes
                                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                        break 'attempts;
                                    }
                                    Err(BicDbError::TransactionConflict(_)) => continue 'attempts,
                                    Err(other) => {
                                        hard_errors.lock().unwrap().push(other.to_string());
                                        break 'attempts;
                                    }
                                }
                            }
                            Err(error) => {
                                // 40001-shaped errors are retried like the
                                // routine handler / driver would; anything
                                // else is the hard-kill class we assert on.
                                let text = error.to_string();
                                if text.contains("conflict") || text.contains("locked") {
                                    continue 'attempts;
                                }
                                hard_errors.lock().unwrap().push(text);
                                break 'attempts;
                            }
                        }
                    }
                }
            });
        }
        for _ in 0..PAYMENT_THREADS {
            scope.spawn(|| {
                let mut last_commit = 0u64;
                for _ in 0..PAYMENT_ITERS {
                    'attempts: for _attempt in 0..8 {
                        let mut session = SqlSession::new_shared(db)
                            .with_snapshot_floor(last_commit)
                            .with_deferred_commit();
                        session.execute("SET bicdb.update_repair = on").unwrap();
                        let run = (|| -> std::result::Result<(), SqlError> {
                            session.execute("BEGIN")?;
                            session.execute(
                                "UPDATE district SET d_ytd = d_ytd + 7.5 \
                                 WHERE d_w_id = 1 AND d_id = 1 RETURNING d_name",
                            )?;
                            Ok(())
                        })();
                        match run {
                            Ok(()) => {
                                let Some(mut tx) = session.take_pending_transaction() else {
                                    break 'attempts;
                                };
                                match db.commit_buffered_transaction(&mut tx) {
                                    Ok(seq) => {
                                        last_commit = last_commit.max(seq);
                                        payment_successes
                                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                        break 'attempts;
                                    }
                                    Err(BicDbError::TransactionConflict(_)) => continue 'attempts,
                                    Err(other) => {
                                        hard_errors.lock().unwrap().push(other.to_string());
                                        break 'attempts;
                                    }
                                }
                            }
                            Err(error) => {
                                let text = error.to_string();
                                if text.contains("conflict") || text.contains("locked") {
                                    continue 'attempts;
                                }
                                hard_errors.lock().unwrap().push(text);
                                break 'attempts;
                            }
                        }
                    }
                }
            });
        }
    });

    let errors = hard_errors.lock().unwrap();
    assert!(
        errors.is_empty(),
        "hard errors: {:?}",
        &errors[..errors.len().min(5)]
    );

    let mut check = SqlSession::new_shared(db);
    let orders = check
        .execute("SELECT count(*), count(DISTINCT o_id) FROM orders")
        .unwrap();
    assert_eq!(orders.rows[0][0], orders.rows[0][1], "duplicate order ids");
    let district = check
        .execute("SELECT d_next_o_id FROM district WHERE d_w_id = 1 AND d_id = 1")
        .unwrap();
    let successes = successes.load(std::sync::atomic::Ordering::Relaxed) as i64;
    let payment_successes = payment_successes.load(std::sync::atomic::Ordering::Relaxed);
    assert!(successes > 0, "the workload must actually commit NewOrders");
    assert!(
        payment_successes > 0,
        "the workload must actually commit Payments"
    );
    assert_eq!(
        district.rows[0][0],
        SqlValue::Int(3001 + successes),
        "district counter must equal successful newords"
    );
    assert_eq!(orders.rows[0][0], SqlValue::Int(successes));
    let payment_balance = check.execute(&format!(
        "SELECT d_ytd = 30000 + {payment_successes} * 7.5 FROM district WHERE d_w_id = 1 AND d_id = 1"
    )).unwrap();
    assert_eq!(
        payment_balance.rows,
        vec![vec![SqlValue::Bool(true)]],
        "district payment total must equal exactly the committed Payments"
    );
}

#[test]
fn first_counter_reservation_waits_and_returns_the_next_committed_value() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut setup = SqlSession::new(&mut db);
        setup
            .execute("CREATE TABLE counters (id INT PRIMARY KEY, next_id INT)")
            .unwrap();
        setup
            .execute("INSERT INTO counters VALUES (1, 10)")
            .unwrap();
    }
    let db = &db;
    let mut first = SqlSession::new_shared(db);
    first.execute("BEGIN").unwrap();
    let allocated = first
        .execute(
            "UPDATE counters SET next_id = next_id + 1 \
             WHERE id = 1 RETURNING next_id - 1",
        )
        .unwrap();
    assert_eq!(allocated.rows, vec![vec![SqlValue::Int(10)]]);

    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        scope.spawn(|| {
            let mut second = SqlSession::new_shared(db);
            second.execute("BEGIN").unwrap();
            started_tx.send(()).unwrap();
            let result = second.execute(
                "UPDATE counters SET next_id = next_id + 1 \
                 WHERE id = 1 RETURNING next_id - 1",
            );
            result_tx.send(result).unwrap();
            second.execute("COMMIT").unwrap();
        });
        started_rx.recv().unwrap();
        assert!(
            result_rx
                .recv_timeout(std::time::Duration::from_millis(20))
                .is_err(),
            "second reservation should wait for the first owner"
        );
        first.execute("COMMIT").unwrap();
        let second = result_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("second reservation should wake after commit")
            .unwrap();
        assert_eq!(second.rows, vec![vec![SqlValue::Int(11)]]);
    });

    let mut check = SqlSession::new_shared(db);
    assert_eq!(
        check
            .execute("SELECT next_id FROM counters WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(12)]]
    );
}

#[test]
fn concurrent_primary_key_inserts_arbitrate_at_commit_without_row_locking() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut setup = SqlSession::new(&mut db);
        setup
            .execute("CREATE TABLE insert_race (id INT PRIMARY KEY, value INT)")
            .unwrap();
        setup
            .execute(
                "CREATE PROCEDURE insert_race_row(p_id INT, p_value INT) \
                 LANGUAGE plpgsql AS $$ BEGIN \
                 INSERT INTO insert_race VALUES (p_id, p_value); END $$",
            )
            .unwrap();
    }

    let mut first = SqlSession::new_shared(&db);
    let mut second = SqlSession::new_shared(&db);
    first.execute("BEGIN").unwrap();
    second.execute("BEGIN").unwrap();
    first.execute("CALL insert_race_row(1, 10)").unwrap();
    // The uncommitted row is invisible. Both inserts may be buffered; the
    // conflict check/atomic unique-key claim must choose the commit winner
    // before WAL or resident apply.
    second.execute("CALL insert_race_row(1, 20)").unwrap();

    first.execute("COMMIT").unwrap();
    let error = second.execute("COMMIT").unwrap_err();
    assert_eq!(error.sqlstate(), "40001");

    let mut check = SqlSession::new_shared(&db);
    assert_eq!(
        check
            .execute("SELECT value FROM insert_race WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(10)]]
    );
}

#[test]
fn rls_restrictive_policies_are_anded_with_permissive_policies() {
    let (_dir, mut db) = empty_test_db();
    let mut session = rls_test_session(&mut db);
    setup_rls_docs(&mut session);
    // Restrictive-only: no permissive policy admits rows, so nothing is
    // visible even though the restrictive policy passes.
    session
        .execute("CREATE POLICY r_only ON docs AS RESTRICTIVE USING (true)")
        .unwrap();
    let rows = session.execute("SELECT id FROM docs").unwrap().rows;
    assert!(rows.is_empty(), "restrictive-only must deny: {rows:?}");
    // Adding a broad permissive policy admits rows, then a restrictive
    // policy narrows them.
    session
        .execute("CREATE POLICY p_all ON docs USING (true)")
        .unwrap();
    assert_eq!(
        session.execute("SELECT count(*) FROM docs").unwrap().rows[0][0],
        SqlValue::Int(2)
    );
    session.execute("DROP POLICY r_only ON docs").unwrap();
    session
        .execute("CREATE POLICY r_tenant ON docs AS RESTRICTIVE USING (tenant = 'alice')")
        .unwrap();
    let rows = session
        .execute("SELECT id FROM docs ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(rows, vec![vec![SqlValue::String("d1".to_string())]]);
    // Restrictive WITH CHECK failures name the failing policy.
    session.execute("DROP POLICY r_tenant ON docs").unwrap();
    session
        .execute(
            "CREATE POLICY r_check ON docs AS RESTRICTIVE USING (true) WITH CHECK (tenant = 'alice')",
        )
        .unwrap();
    let error = session
        .execute("INSERT INTO docs VALUES ('d3', 'bob', 'c')")
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("row-level security policy \"r_check\""),
        "unexpected error: {error}"
    );
}

#[test]
fn rls_owner_bypasses_unless_forced_and_flags_are_independent() {
    let (_dir, mut db) = empty_test_db();
    let mut session = rls_test_session(&mut db);
    session
        .execute("CREATE TABLE notes (id TEXT PRIMARY KEY, tenant TEXT)")
        .unwrap();
    session
        .execute("INSERT INTO notes VALUES ('n1', 'x')")
        .unwrap();
    session
        .execute("ALTER TABLE notes ENABLE ROW LEVEL SECURITY")
        .unwrap();
    session
        .execute("CREATE POLICY deny ON notes USING (false)")
        .unwrap();
    // The embedded session runs as the bootstrap role, which owns the table:
    // without FORCE, RLS does not apply to the owner.
    assert_eq!(
        session.execute("SELECT count(*) FROM notes").unwrap().rows[0][0],
        SqlValue::Int(1)
    );
    session
        .execute("ALTER TABLE notes FORCE ROW LEVEL SECURITY")
        .unwrap();
    assert_eq!(
        session.execute("SELECT count(*) FROM notes").unwrap().rows[0][0],
        SqlValue::Int(0)
    );
    session
        .execute("ALTER TABLE notes NO FORCE ROW LEVEL SECURITY")
        .unwrap();
    assert_eq!(
        session.execute("SELECT count(*) FROM notes").unwrap().rows[0][0],
        SqlValue::Int(1)
    );
    // DISABLE keeps the force flag, matching pg_class semantics.
    session
        .execute("ALTER TABLE notes FORCE ROW LEVEL SECURITY")
        .unwrap();
    session
        .execute("ALTER TABLE notes DISABLE ROW LEVEL SECURITY")
        .unwrap();
    let flags = session
        .execute("SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE relname = 'notes'")
        .unwrap()
        .rows;
    assert_eq!(
        flags,
        vec![vec![SqlValue::Bool(false), SqlValue::Bool(true)]]
    );
    assert_eq!(
        session.execute("SELECT count(*) FROM notes").unwrap().rows[0][0],
        SqlValue::Int(1)
    );
}

#[test]
fn rls_set_role_changes_identity_and_to_roles_filter_policies() {
    let (_dir, mut db) = empty_test_db();
    let mut session = rls_test_session(&mut db);
    session.execute("CREATE ROLE alice LOGIN").unwrap();
    session.execute("CREATE ROLE bob LOGIN").unwrap();
    session.execute("CREATE ROLE auditors").unwrap();
    session.execute("GRANT auditors TO bob").unwrap();
    setup_rls_docs(&mut session);
    session
        .execute("CREATE POLICY own_rows ON docs FOR SELECT TO alice USING (tenant = current_user)")
        .unwrap();
    session
        .execute("CREATE POLICY audit_all ON docs FOR SELECT TO auditors USING (true)")
        .unwrap();

    session.execute("SET ROLE alice").unwrap();
    assert_eq!(
        session.execute("SELECT current_user").unwrap().rows[0][0],
        SqlValue::String("alice".to_string())
    );
    let rows = session
        .execute("SELECT id FROM docs ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(rows, vec![vec![SqlValue::String("d1".to_string())]]);

    // bob matches the auditors policy through role membership.
    session.execute("SET ROLE bob").unwrap();
    assert_eq!(
        session.execute("SELECT count(*) FROM docs").unwrap().rows[0][0],
        SqlValue::Int(2)
    );

    session.execute("RESET ROLE").unwrap();
    assert_eq!(
        session.execute("SELECT current_user").unwrap().rows[0][0],
        SqlValue::String("bicdb".to_string())
    );
}

#[test]
fn quoted_relation_identity_is_preserved_for_owner_and_grants() {
    let (dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE ROLE cognee LOGIN").unwrap();
    session.execute("CREATE ROLE reader LOGIN").unwrap();
    session.execute("SET ROLE cognee").unwrap();
    session.execute(r#"CREATE SCHEMA "QuotedSchema""#).unwrap();
    session.execute("CREATE SCHEMA quotedschema").unwrap();
    session
        .execute(r#"CREATE TYPE "QuotedStatus" AS ENUM ('Ready', 'Done')"#)
        .unwrap();
    session
        .execute("CREATE TYPE quotedstatus AS ENUM ('lower')")
        .unwrap();
    session
        .execute(r#"CREATE SEQUENCE "QuotedSequence" START WITH 10"#)
        .unwrap();
    session
        .execute("CREATE SEQUENCE quotedsequence START WITH 100")
        .unwrap();
    session
        .execute(
            r#"CREATE TABLE "QuotedProbe" (
                "NodeId" integer DEFAULT nextval('"QuotedSequence"')::integer,
                nodeid integer NOT NULL DEFAULT 20,
                "DisplayName" text NOT NULL,
                displayname text NOT NULL DEFAULT 'lower',
                "State" "QuotedStatus" NOT NULL DEFAULT 'Ready',
                CONSTRAINT "QuotedProbePk" PRIMARY KEY ("NodeId"),
                CONSTRAINT "QuotedProbeNameKey" UNIQUE ("DisplayName"),
                CONSTRAINT quotedprobenamekey UNIQUE (displayname)
            )"#,
        )
        .unwrap();
    session
        .execute("CREATE TABLE quotedprobe (id integer PRIMARY KEY)")
        .unwrap();
    session
        .execute(r#"CREATE INDEX "QuotedProbeDisplayIdx" ON "QuotedProbe" ("DisplayName")"#)
        .unwrap();
    session
        .execute("CREATE INDEX quotedprobedisplayidx ON quotedprobe (id)")
        .unwrap();
    assert_eq!(
        session
            .execute(r#"SELECT nextval('"QuotedSequence"'), nextval('quotedsequence')"#)
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(10), SqlValue::Int(100)]]
    );
    session
        .execute(
            r#"INSERT INTO "QuotedProbe" ("NodeId", nodeid, "DisplayName", displayname)
               VALUES (11, 20, 'First', 'lower')"#,
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                r#"SELECT "NodeId", nodeid, "DisplayName", displayname, "State" FROM "QuotedProbe""#,
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(11),
            SqlValue::Int(20),
            SqlValue::String("First".to_string()),
            SqlValue::String("lower".to_string()),
            SqlValue::String("Ready".to_string())
        ]]
    );

    assert_eq!(
        session.execute("SELECT id FROM quotedprobe").unwrap().rows,
        Vec::<Vec<SqlValue>>::new()
    );
    assert_eq!(
        session
            .execute(r#"SELECT "NodeId", nodeid FROM "QuotedProbe""#)
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(11), SqlValue::Int(20)]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT column_name FROM information_schema.columns \
                 WHERE table_name = 'QuotedProbe' ORDER BY ordinal_position",
            )
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("NodeId".to_string())],
            vec![SqlValue::String("nodeid".to_string())],
            vec![SqlValue::String("DisplayName".to_string())],
            vec![SqlValue::String("displayname".to_string())],
            vec![SqlValue::String("State".to_string())],
        ]
    );
    assert_eq!(
        session
            .execute("SELECT nspname FROM pg_namespace WHERE nspname = 'QuotedSchema'",)
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("QuotedSchema".to_string())]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT nspname FROM pg_namespace \
                 WHERE nspname IN ('QuotedSchema', 'quotedschema') ORDER BY nspname",
            )
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("QuotedSchema".to_string())],
            vec![SqlValue::String("quotedschema".to_string())],
        ]
    );
    assert_eq!(
        session
            .execute("SELECT typname FROM pg_type WHERE typname = 'QuotedStatus'",)
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("QuotedStatus".to_string())]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT typname FROM pg_type \
                 WHERE typname IN ('QuotedStatus', 'quotedstatus') ORDER BY typname",
            )
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("QuotedStatus".to_string())],
            vec![SqlValue::String("quotedstatus".to_string())],
        ]
    );
    assert_eq!(
        session
            .execute(
                "SELECT relname FROM pg_class \
                 WHERE relname IN ('QuotedProbe', 'quotedprobe', 'QuotedSequence', \
                                   'quotedsequence', 'QuotedProbeDisplayIdx', \
                                   'quotedprobedisplayidx') \
                 ORDER BY relname",
            )
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("QuotedProbe".to_string())],
            vec![SqlValue::String("QuotedProbeDisplayIdx".to_string())],
            vec![SqlValue::String("QuotedSequence".to_string())],
            vec![SqlValue::String("quotedprobe".to_string())],
            vec![SqlValue::String("quotedprobedisplayidx".to_string())],
            vec![SqlValue::String("quotedsequence".to_string())],
        ]
    );
    assert_eq!(
        session
            .execute(
                "SELECT conname FROM pg_constraint \
                 WHERE conname IN ('QuotedProbePk', 'QuotedProbeNameKey', \
                                   'quotedprobenamekey') ORDER BY conname",
            )
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("QuotedProbeNameKey".to_string())],
            vec![SqlValue::String("QuotedProbePk".to_string())],
            vec![SqlValue::String("quotedprobenamekey".to_string())],
        ]
    );

    session.execute("RESET ROLE").unwrap();
    session
        .execute(r#"GRANT SELECT ON "QuotedProbe" TO reader"#)
        .unwrap();
    session.execute("SET ROLE reader").unwrap();
    assert_eq!(
        session
            .execute(r#"SELECT "NodeId" FROM "QuotedProbe""#)
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(11)]]
    );

    drop(session);
    drop(db);
    let mut reopened = BicDb::open(dir.path()).unwrap();
    let mut reopened_session = SqlSession::new(&mut reopened);
    reopened_session.execute("SET ROLE reader").unwrap();
    assert_eq!(
        reopened_session
            .execute(r#"SELECT "DisplayName" FROM "QuotedProbe""#)
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("First".to_string())]]
    );
}

#[test]
fn rls_bypassrls_and_superuser_roles_skip_policies() {
    let (_dir, mut db) = empty_test_db();
    let mut session = rls_test_session(&mut db);
    session
        .execute("CREATE ROLE app_reader LOGIN NOSUPERUSER NOBYPASSRLS")
        .unwrap();
    session
        .execute("CREATE ROLE dumper LOGIN BYPASSRLS")
        .unwrap();
    setup_rls_docs(&mut session);
    session
        .execute("CREATE POLICY deny ON docs USING (false)")
        .unwrap();
    session.execute("SET ROLE app_reader").unwrap();
    assert_eq!(
        session.execute("SELECT count(*) FROM docs").unwrap().rows[0][0],
        SqlValue::Int(0)
    );
    session.execute("SET ROLE dumper").unwrap();
    assert_eq!(
        session.execute("SELECT count(*) FROM docs").unwrap().rows[0][0],
        SqlValue::Int(2)
    );
}

#[test]
fn rls_row_security_off_errors_when_policies_would_apply() {
    let (_dir, mut db) = empty_test_db();
    let mut session = rls_test_session(&mut db);
    session.execute("CREATE ROLE reader LOGIN").unwrap();
    setup_rls_docs(&mut session);
    session
        .execute("CREATE POLICY p ON docs USING (true)")
        .unwrap();
    session.execute("SET ROLE reader").unwrap();
    session.execute("SET row_security = off").unwrap();
    let error = session.execute("SELECT count(*) FROM docs").unwrap_err();
    assert!(
        error
            .to_string()
            .contains("query would be affected by row-level security policy for table \"docs\""),
        "unexpected error: {error}"
    );
    session.execute("SET row_security = on").unwrap();
    assert_eq!(
        session.execute("SELECT count(*) FROM docs").unwrap().rows[0][0],
        SqlValue::Int(2)
    );
}

#[test]
fn rls_create_policy_validation_matches_postgres() {
    let (_dir, mut db) = empty_test_db();
    let mut session = rls_test_session(&mut db);
    session
        .execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
        .unwrap();
    let error = session
        .execute("CREATE POLICY bad ON t FOR INSERT USING (true)")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("only WITH CHECK expression allowed for INSERT"));
    let error = session
        .execute("CREATE POLICY bad ON t FOR SELECT WITH CHECK (true)")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("WITH CHECK cannot be applied to SELECT or DELETE"));
    let error = session
        .execute("CREATE POLICY bad ON t USING (count(*) > 0)")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("aggregate functions are not allowed in policy expressions"));
    let error = session
        .execute("CREATE POLICY bad ON t USING (nosuchcol = 1)")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("column \"nosuchcol\" does not exist"));
    session
        .execute("CREATE POLICY dup ON t USING (true)")
        .unwrap();
    let error = session
        .execute("CREATE POLICY dup ON t USING (true)")
        .unwrap_err();
    assert!(error.to_string().contains("already exists"));
    let error = session.execute("DROP POLICY nope ON t").unwrap_err();
    assert!(error
        .to_string()
        .contains("policy \"nope\" for table \"t\" does not exist"));
    // FOR defaults to ALL and unknown trailing clauses are rejected, not
    // silently discarded.
    session
        .execute("CREATE POLICY defaults_to_all ON t USING (true)")
        .unwrap();
    let cmd = session
        .execute("SELECT cmd FROM pg_policies WHERE policyname = 'defaults_to_all'")
        .unwrap()
        .rows;
    assert_eq!(cmd, vec![vec![SqlValue::String("ALL".to_string())]]);
}

#[test]
fn rls_alter_policy_rename_roles_and_expressions() {
    let (_dir, mut db) = empty_test_db();
    let mut session = rls_test_session(&mut db);
    session.execute("CREATE ROLE alice LOGIN").unwrap();
    setup_rls_docs(&mut session);
    session
        .execute("CREATE POLICY p ON docs FOR SELECT USING (true)")
        .unwrap();
    session
        .execute("ALTER POLICY p ON docs RENAME TO p2")
        .unwrap();
    session.execute("ALTER POLICY p2 ON docs TO alice").unwrap();
    session
        .execute("ALTER POLICY p2 ON docs USING (tenant = 'alice')")
        .unwrap();
    let rows = session
        .execute("SELECT policyname, roles, qual FROM pg_policies WHERE tablename = 'docs'")
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], SqlValue::String("p2".to_string()));
    assert_eq!(rows[0][1], SqlValue::String("{alice}".to_string()));
    assert!(rows[0][2].to_cell().contains("tenant = 'alice'"));
    let error = session
        .execute("ALTER POLICY missing ON docs RENAME TO x")
        .unwrap_err();
    assert!(error.to_string().contains("does not exist"));
}

#[test]
fn rls_security_invoker_views_and_definer_views_check_as_owner() {
    let (_dir, mut db) = empty_test_db();
    let mut session = rls_test_session(&mut db);
    session.execute("CREATE ROLE alice LOGIN").unwrap();
    setup_rls_docs(&mut session);
    session
        .execute("CREATE POLICY own_rows ON docs USING (tenant = current_user)")
        .unwrap();
    // Definer view owned by the bootstrap role: RLS is decided as the owner,
    // but FORCE keeps policies applied; expressions evaluate with the
    // invoker's identity (verified against the PG 18 oracle).
    session
        .execute("CREATE VIEW docs_definer AS SELECT id, tenant FROM docs")
        .unwrap();
    session
        .execute("CREATE VIEW docs_invoker WITH (security_invoker = true) AS SELECT id, tenant FROM docs")
        .unwrap();
    session
        .execute("GRANT SELECT ON docs_definer, docs_invoker TO PUBLIC")
        .unwrap();
    session.execute("SET ROLE alice").unwrap();
    let rows = session
        .execute("SELECT id FROM docs_definer ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(rows, vec![vec![SqlValue::String("d1".to_string())]]);
    let rows = session
        .execute("SELECT id FROM docs_invoker ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(rows, vec![vec![SqlValue::String("d1".to_string())]]);
    session.execute("RESET ROLE").unwrap();
}

#[test]
fn rls_least_privilege_role_end_to_end() {
    // The deploy-template flow: a NOSUPERUSER NOBYPASSRLS app role gets DML
    // grants and is fenced by FORCE ROW LEVEL SECURITY tenant policies.
    let (_dir, mut db) = empty_test_db();
    let mut session = rls_test_session(&mut db);
    session
        .execute("CREATE ROLE app_rw LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD 'app-secret'")
        .unwrap();
    session
        .execute("CREATE TABLE tenants_data (id TEXT PRIMARY KEY, tenant TEXT, body TEXT)")
        .unwrap();
    session
        .execute("GRANT SELECT, INSERT, UPDATE, DELETE ON tenants_data TO app_rw")
        .unwrap();
    session
        .execute("INSERT INTO tenants_data VALUES ('r1', 't-a', 'alpha'), ('r2', 't-b', 'beta')")
        .unwrap();
    session
        .execute("ALTER TABLE tenants_data ENABLE ROW LEVEL SECURITY")
        .unwrap();
    session
        .execute("ALTER TABLE tenants_data FORCE ROW LEVEL SECURITY")
        .unwrap();
    session
        .execute(
            "CREATE POLICY tenant_isolation ON tenants_data \
             USING (tenant = current_setting('app.tenant', true)) \
             WITH CHECK (tenant = current_setting('app.tenant', true))",
        )
        .unwrap();

    drop(session);
    let mut session = SqlSession::new_unprivileged(&mut db, "app_rw");
    assert_eq!(
        session.execute("SELECT session_user").unwrap().rows[0][0],
        SqlValue::String("app_rw".to_string())
    );
    // No tenant context: default deny.
    assert_eq!(
        session
            .execute("SELECT count(*) FROM tenants_data")
            .unwrap()
            .rows[0][0],
        SqlValue::Int(0)
    );
    session.execute("SET app.tenant = 't-a'").unwrap();
    let rows = session
        .execute("SELECT id FROM tenants_data ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(rows, vec![vec![SqlValue::String("r1".to_string())]]);
    // Cross-tenant writes are rejected.
    let error = session
        .execute("INSERT INTO tenants_data VALUES ('r3', 't-b', 'forged')")
        .unwrap_err();
    assert!(error.to_string().contains("row-level security"));
    // In-tenant writes succeed.
    session
        .execute("INSERT INTO tenants_data VALUES ('r4', 't-a', 'ok')")
        .unwrap();
    // The app role cannot escalate.
    let error = session
        .execute("SET SESSION AUTHORIZATION bicdb")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("permission denied to set session authorization"));
    let error = session
        .execute("SET bicdb.initial_session_authorization = 'bicdb'")
        .unwrap_err();
    assert!(error.to_string().contains("cannot be changed"));
}

#[test]
fn rls_on_conflict_do_update_errors_on_using_violation() {
    let (_dir, mut db) = empty_test_db();
    let mut session = rls_test_session(&mut db);
    session.execute("CREATE ROLE writer LOGIN").unwrap();
    setup_rls_docs(&mut session);
    session
        .execute("CREATE POLICY sel ON docs FOR SELECT USING (true)")
        .unwrap();
    session
        .execute("CREATE POLICY ins ON docs FOR INSERT WITH CHECK (true)")
        .unwrap();
    session
        .execute(
            "CREATE POLICY upd ON docs FOR UPDATE USING (tenant = current_user) WITH CHECK (true)",
        )
        .unwrap();
    session.execute("SET ROLE writer").unwrap();
    let error = session
        .execute(
            "INSERT INTO docs VALUES ('d2', 'writer', 'w') ON CONFLICT (id) DO UPDATE SET label = 'w'",
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("row-level security policy (USING expression) for table \"docs\""),
        "unexpected error: {error}"
    );
}

#[test]
fn rls_update_reading_columns_requires_select_visibility() {
    // Verified against the PG 18 oracle: an UPDATE whose WHERE clause reads
    // existing columns also needs SELECT-policy visibility, while an UPDATE
    // with no column reads only needs the UPDATE policy.
    let (_dir, mut db) = empty_test_db();
    let mut session = rls_test_session(&mut db);
    session.execute("CREATE ROLE writer LOGIN").unwrap();
    setup_rls_docs(&mut session);
    session
        .execute("CREATE POLICY upd ON docs FOR UPDATE USING (true) WITH CHECK (true)")
        .unwrap();
    session.execute("SET ROLE writer").unwrap();
    session
        .execute("UPDATE docs SET label = 'changed' WHERE tenant = 'alice'")
        .unwrap();
    session
        .execute("UPDATE docs SET label = 'blanket'")
        .unwrap();
    session.execute("RESET ROLE").unwrap();
    // Owner bypass (NO FORCE) so the verification read sees everything.
    session
        .execute("ALTER TABLE docs NO FORCE ROW LEVEL SECURITY")
        .unwrap();
    let rows = session
        .execute("SELECT label FROM docs GROUP BY label ORDER BY label")
        .unwrap()
        .rows;
    // The column-reading UPDATE matched nothing; the blanket UPDATE hit all rows.
    assert_eq!(rows, vec![vec![SqlValue::String("blanket".to_string())]]);
}

#[test]
fn pg_has_role_resolves_catalog_owner_oids_with_session_identity() {
    let (_dir, mut db) = empty_test_db();
    let mut session = rls_test_session(&mut db);
    session.execute("CREATE ROLE app LOGIN NOINHERIT").unwrap();
    session.execute("CREATE ROLE owner_role NOLOGIN").unwrap();
    session
        .execute("CREATE TABLE readiness_probe (id TEXT)")
        .unwrap();

    session.execute("SET SESSION AUTHORIZATION app").unwrap();
    let result = session
        .execute(
            "SELECT pg_get_userbyid(relowner), pg_has_role(current_user, relowner, 'MEMBER') \
             FROM pg_class WHERE oid = to_regclass('readiness_probe')",
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::String("bicdb".to_string()),
            SqlValue::Bool(false),
        ]]
    );
    assert_eq!(result.column_types[1], Some("bool".to_string()));

    session.execute("RESET SESSION AUTHORIZATION").unwrap();
    session.execute("GRANT owner_role TO app").unwrap();
    session.execute("SET SESSION AUTHORIZATION app").unwrap();
    let result = session
        .execute(
            "SELECT pg_has_role(current_user, oid, 'MEMBER'), \
             pg_has_role(current_user, oid, 'USAGE') \
             FROM pg_roles WHERE rolname = 'owner_role'",
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::Bool(true), SqlValue::Bool(false)]]
    );
    assert_eq!(
        result.column_types,
        vec![Some("bool".to_string()), Some("bool".to_string())]
    );

    let empty = session
        .execute(
            "SELECT pg_has_role(current_user, oid, 'MEMBER') \
             FROM pg_roles WHERE rolname = 'missing_role'",
        )
        .unwrap();
    assert!(empty.rows.is_empty());
    assert_eq!(empty.column_types, vec![Some("bool".to_string())]);

    let strict = session
        .execute("SELECT pg_has_role(NULL::name, 'MEMBER') FROM pg_roles LIMIT 1")
        .unwrap();
    assert_eq!(strict.rows, vec![vec![SqlValue::Null]]);
    assert_eq!(strict.column_types, vec![Some("bool".to_string())]);
}

#[test]
fn same_table_name_in_distinct_schemas_has_isolated_storage() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE SCHEMA tenant_a;
             CREATE SCHEMA tenant_b;
             CREATE TABLE tenant_a.users (id TEXT PRIMARY KEY, secret TEXT);
             CREATE TABLE tenant_b.users (id TEXT PRIMARY KEY, secret TEXT);
             INSERT INTO tenant_a.users VALUES ('same', 'alpha');
             INSERT INTO tenant_b.users VALUES ('same', 'beta')",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT secret FROM tenant_a.users WHERE id = 'same'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("alpha".to_string())]]
    );
    assert_eq!(
        session
            .execute("SELECT secret FROM tenant_b.users WHERE id = 'same'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("beta".to_string())]]
    );
    let reserved = session
        .execute("CREATE TABLE __bicdb_s_reserved (id TEXT)")
        .unwrap_err()
        .to_string();
    assert!(
        reserved.contains("reserved"),
        "unexpected error: {reserved}"
    );
}
