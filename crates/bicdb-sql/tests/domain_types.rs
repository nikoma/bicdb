use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn domains_enforce_base_typmods_defaults_checks_arrays_and_catalogs() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE DOMAIN positive_amount AS NUMERIC(8,2)
                 DEFAULT 1.20
                 NOT NULL
                 CONSTRAINT positive_amount_positive CHECK (VALUE > 0)
                 CHECK (VALUE < 1000000)",
            )
            .unwrap();

        let type_row = session
            .execute(
                "SELECT typtype, typcategory, typbasetype, typtypmod, typnotnull,
                        typinput, typoutput, typreceive, typsend, typdefault
                 FROM pg_type WHERE typname = 'positive_amount'",
            )
            .unwrap()
            .rows;
        assert_eq!(type_row.len(), 1);
        assert_eq!(type_row[0][0], SqlValue::String("d".to_string()));
        assert_eq!(type_row[0][1], SqlValue::String("N".to_string()));
        assert_eq!(type_row[0][2], SqlValue::Int(1700));
        assert_eq!(type_row[0][3], SqlValue::Int(524_294));
        assert_eq!(type_row[0][4], SqlValue::Bool(true));
        assert_eq!(type_row[0][5], SqlValue::String("domain_in".to_string()));
        assert_eq!(type_row[0][6], SqlValue::String("numeric_out".to_string()));
        assert_eq!(type_row[0][7], SqlValue::String("domain_recv".to_string()));
        assert_eq!(type_row[0][8], SqlValue::String("numeric_send".to_string()));
        assert_eq!(type_row[0][9], SqlValue::String("1.20".to_string()));
        assert_eq!(
            session
                .execute(
                    "SELECT conname, contype, conrelid, convalidated
                     FROM pg_constraint
                     WHERE contypid = (
                         SELECT oid FROM pg_type WHERE typname = 'positive_amount'
                     )
                     ORDER BY conname",
                )
                .unwrap()
                .rows,
            vec![
                vec![
                    SqlValue::String("positive_amount_check".to_string()),
                    SqlValue::String("c".to_string()),
                    SqlValue::Int(0),
                    SqlValue::Bool(true),
                ],
                vec![
                    SqlValue::String("positive_amount_not_null".to_string()),
                    SqlValue::String("n".to_string()),
                    SqlValue::Int(0),
                    SqlValue::Bool(true),
                ],
                vec![
                    SqlValue::String("positive_amount_positive".to_string()),
                    SqlValue::String("c".to_string()),
                    SqlValue::Int(0),
                    SqlValue::Bool(true),
                ],
            ],
        );

        session
            .execute(
                "CREATE TABLE domain_rows (
                    id TEXT PRIMARY KEY,
                    amount positive_amount,
                    history positive_amount[]
                )",
            )
            .unwrap();
        session
            .execute("INSERT INTO domain_rows (id) VALUES ('defaulted')")
            .unwrap();
        session
            .execute(
                "INSERT INTO domain_rows VALUES
                 ('explicit', 12.345, '[0:2]={1.234,2,9.999}')",
            )
            .unwrap();
        assert_eq!(
            session
                .execute("SELECT amount FROM domain_rows ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec![SqlValue::String("1.20".to_string())],
                vec![SqlValue::String("12.35".to_string())],
            ],
        );
        assert_eq!(
            session
                .execute(
                    "SELECT array_dims(history), history
                     FROM domain_rows WHERE id = 'explicit'",
                )
                .unwrap()
                .rows[0][0],
            SqlValue::String("[0:2]".to_string()),
        );
        for sql in [
            "INSERT INTO domain_rows VALUES ('negative', -1, NULL)",
            "UPDATE domain_rows SET history = '{1,-2}' WHERE id = 'explicit'",
        ] {
            let error = session.execute(sql).unwrap_err();
            assert_eq!(error.sqlstate(), "23514", "{sql}: {error}");
        }
        for sql in [
            "INSERT INTO domain_rows VALUES ('null-scalar', NULL, '{}')",
            "UPDATE domain_rows SET history = '{1,NULL}' WHERE id = 'explicit'",
        ] {
            let error = session.execute(sql).unwrap_err();
            assert_eq!(error.sqlstate(), "23502", "{sql}: {error}");
        }
        assert_eq!(
            session
                .execute("INSERT INTO domain_rows VALUES ('overflow', 1000000, NULL)")
                .unwrap_err()
                .sqlstate(),
            "22003",
        );

        session
            .execute("CREATE INDEX domain_rows_amount_idx ON domain_rows (amount)")
            .unwrap();
        assert_eq!(
            session
                .execute("SELECT id FROM domain_rows WHERE amount > 2 ORDER BY amount")
                .unwrap()
                .rows,
            vec![vec![SqlValue::String("explicit".to_string())]],
        );
        assert_eq!(
            session
                .execute("DROP DOMAIN positive_amount")
                .unwrap_err()
                .sqlstate(),
            "2BP01",
        );
    }
    db.close().unwrap();

    let mut reopened = BicDb::open(root.path()).unwrap();
    assert_eq!(
        SqlSession::new(&mut reopened)
            .execute("SELECT amount FROM domain_rows WHERE id = 'explicit'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("12.35".to_string())]],
    );
}

#[test]
fn domains_can_build_on_enums_and_other_domains() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TYPE priority AS ENUM ('low', 'normal', 'high')")
        .unwrap();
    session
        .execute(
            "CREATE DOMAIN actionable_priority AS priority
             CHECK (VALUE <> 'low')",
        )
        .unwrap();
    session
        .execute(
            "CREATE DOMAIN required_actionable AS actionable_priority
             CHECK (VALUE <> 'normal')",
        )
        .unwrap();
    session
        .execute("CREATE TABLE domain_enum_rows (id TEXT PRIMARY KEY, value required_actionable)")
        .unwrap();
    session
        .execute("INSERT INTO domain_enum_rows VALUES ('valid', 'high')")
        .unwrap();
    assert_eq!(
        session
            .execute("INSERT INTO domain_enum_rows VALUES ('invalid', 'normal')")
            .unwrap_err()
            .sqlstate(),
        "23514",
    );
    assert_eq!(
        session
            .execute("INSERT INTO domain_enum_rows VALUES ('base-invalid', 'low')")
            .unwrap_err()
            .sqlstate(),
        "23514",
    );
    for sql in ["DROP TYPE priority", "DROP DOMAIN actionable_priority"] {
        assert_eq!(
            session.execute(sql).unwrap_err().sqlstate(),
            "2BP01",
            "{sql}"
        );
    }
    session.execute("DROP TYPE priority CASCADE").unwrap();
    assert!(session
        .execute(
            "SELECT typname FROM pg_type
                 WHERE typname IN ('priority', 'actionable_priority', 'required_actionable')",
        )
        .unwrap()
        .rows
        .is_empty(),);
    assert_eq!(
        session
            .execute("SELECT value FROM domain_enum_rows")
            .unwrap_err()
            .sqlstate(),
        "42703",
    );
}

#[test]
fn alter_domain_manages_defaults_nullability_and_check_lifecycle() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE DOMAIN mutable_amount AS NUMERIC(5,1)")
        .unwrap();
    session
        .execute(
            "CREATE TABLE mutable_domain_rows (
                id TEXT PRIMARY KEY,
                amount mutable_amount
            )",
        )
        .unwrap();
    session
        .execute("INSERT INTO mutable_domain_rows VALUES ('negative', -1), ('null', NULL)")
        .unwrap();

    assert_eq!(
        session
            .execute("ALTER DOMAIN mutable_amount SET NOT NULL")
            .unwrap_err()
            .sqlstate(),
        "23502",
    );
    session
        .execute("DELETE FROM mutable_domain_rows WHERE id = 'null'")
        .unwrap();
    session
        .execute("ALTER DOMAIN mutable_amount SET NOT NULL")
        .unwrap();
    assert_eq!(
        session
            .execute("INSERT INTO mutable_domain_rows VALUES ('rejected-null', NULL)")
            .unwrap_err()
            .sqlstate(),
        "23502",
    );
    session
        .execute("ALTER DOMAIN mutable_amount DROP NOT NULL")
        .unwrap();

    session
        .execute("ALTER DOMAIN mutable_amount SET DEFAULT 2.34")
        .unwrap();
    session
        .execute("INSERT INTO mutable_domain_rows (id) VALUES ('defaulted')")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT amount FROM mutable_domain_rows WHERE id = 'defaulted'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("2.3".to_string())]],
    );

    session
        .execute(
            "ALTER DOMAIN mutable_amount
             ADD CONSTRAINT mutable_amount_positive CHECK (VALUE > 0) NOT VALID",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("INSERT INTO mutable_domain_rows VALUES ('rejected-negative', -2)")
            .unwrap_err()
            .sqlstate(),
        "23514",
    );
    assert_eq!(
        session
            .execute(
                "SELECT convalidated FROM pg_constraint
                 WHERE conname = 'mutable_amount_positive'",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(false)]],
    );
    assert_eq!(
        session
            .execute(
                "ALTER DOMAIN mutable_amount
                 VALIDATE CONSTRAINT mutable_amount_positive",
            )
            .unwrap_err()
            .sqlstate(),
        "23514",
    );
    session
        .execute("UPDATE mutable_domain_rows SET amount = 1 WHERE id = 'negative'")
        .unwrap();
    session
        .execute(
            "ALTER DOMAIN mutable_amount
             VALIDATE CONSTRAINT mutable_amount_positive",
        )
        .unwrap();
    session
        .execute(
            "ALTER DOMAIN mutable_amount
             RENAME CONSTRAINT mutable_amount_positive TO amount_positive",
        )
        .unwrap();
    session
        .execute("ALTER DOMAIN mutable_amount DROP CONSTRAINT amount_positive")
        .unwrap();

    session
        .execute("ALTER DOMAIN mutable_amount DROP DEFAULT")
        .unwrap();
    session
        .execute("INSERT INTO mutable_domain_rows (id) VALUES ('no-default')")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT amount IS NULL FROM mutable_domain_rows WHERE id = 'no-default'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]],
    );
}

#[test]
fn domain_postgres_syntax_collation_drop_tag_and_transaction_undo() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE DOMAIN optional_text TEXT COLLATE \"C\"
             DEFAULT 'fallback' NULL",
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT typcollation > 0, typnotnull, typdefault
                 FROM pg_type WHERE typname = 'optional_text'",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::String("'fallback'".to_string()),
        ]],
    );
    session
        .execute("CREATE TABLE optional_text_rows (id TEXT PRIMARY KEY, value optional_text)")
        .unwrap();

    session.execute("BEGIN").unwrap();
    session
        .execute("ALTER DOMAIN optional_text SET NOT NULL")
        .unwrap();
    session
        .execute("ALTER DOMAIN optional_text SET DEFAULT 'changed'")
        .unwrap();
    session
        .execute("ALTER DOMAIN optional_text ADD CHECK (VALUE <> '')")
        .unwrap();
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT typnotnull, typdefault
                 FROM pg_type WHERE typname = 'optional_text'",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Bool(false),
            SqlValue::String("'fallback'".to_string()),
        ]],
    );
    assert!(session
        .execute(
            "SELECT conname FROM pg_constraint
                 WHERE contypid = (SELECT oid FROM pg_type WHERE typname = 'optional_text')",
        )
        .unwrap()
        .rows
        .is_empty(),);
    session.execute("DROP TABLE optional_text_rows").unwrap();
    assert_eq!(
        session
            .execute("DROP DOMAIN optional_text")
            .unwrap()
            .command_tag
            .as_deref(),
        Some("DROP DOMAIN"),
    );
}

#[test]
fn named_domain_not_null_constraints_follow_postgres_lifecycle() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE DOMAIN named_required AS TEXT CONSTRAINT custom_required NOT NULL")
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT conname, contype FROM pg_constraint
                 WHERE contypid = (SELECT oid FROM pg_type WHERE typname = 'named_required')",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("custom_required".to_string()),
            SqlValue::String("n".to_string()),
        ]],
    );

    session
        .execute(
            "ALTER DOMAIN named_required RENAME CONSTRAINT custom_required TO renamed_required",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("ALTER DOMAIN named_required VALIDATE CONSTRAINT renamed_required")
            .unwrap_err()
            .sqlstate(),
        "42809",
    );
    session
        .execute("ALTER DOMAIN named_required DROP CONSTRAINT renamed_required")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT typnotnull FROM pg_type WHERE typname = 'named_required'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(false)]],
    );

    session
        .execute("ALTER DOMAIN named_required ADD CONSTRAINT \"Required Value\" NOT NULL")
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT conname, contype FROM pg_constraint
                 WHERE contypid = (SELECT oid FROM pg_type WHERE typname = 'named_required')",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("Required Value".to_string()),
            SqlValue::String("n".to_string()),
        ]],
    );
}
