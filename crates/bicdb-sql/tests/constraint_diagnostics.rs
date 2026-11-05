use bicdb_core::BicDb;
use bicdb_sql::{oracle, SqlErrorField, SqlSession};

fn unique_diagnostics(profile: &str) {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    oracle::ensure_database_profile(&mut db, profile).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE SCHEMA constraint_owner").unwrap();
    session.execute("CREATE SCHEMA other_owner").unwrap();
    session.execute("CREATE TABLE constraint_owner.\"Mixed.Rows\"(id INTEGER CONSTRAINT pk_rows PRIMARY KEY,a INTEGER CONSTRAINT \"Unique.Dot\" UNIQUE,b INTEGER)").unwrap();
    session
        .execute("CREATE UNIQUE INDEX \"Spaced Index\" ON constraint_owner.\"Mixed.Rows\"(b)")
        .unwrap();
    session
        .execute("INSERT INTO constraint_owner.\"Mixed.Rows\" VALUES(1,10,100),(2,20,200)")
        .unwrap();
    session
        .execute("SET search_path=other_owner,public")
        .unwrap();
    session.execute("BEGIN").unwrap();
    for (statement, constraint) in [
        (
            "INSERT INTO constraint_owner.\"Mixed.Rows\" VALUES(1,30,300)",
            "pk_rows",
        ),
        (
            "INSERT INTO constraint_owner.\"Mixed.Rows\" VALUES(3,10,300)",
            "Unique.Dot",
        ),
        (
            "INSERT INTO constraint_owner.\"Mixed.Rows\" VALUES(3,30,100)",
            "Spaced Index",
        ),
        (
            "UPDATE constraint_owner.\"Mixed.Rows\" SET id=1 WHERE id=2",
            "pk_rows",
        ),
        (
            "UPDATE constraint_owner.\"Mixed.Rows\" SET a=10 WHERE id=2",
            "Unique.Dot",
        ),
        (
            "UPDATE constraint_owner.\"Mixed.Rows\" SET b=100 WHERE id=2",
            "Spaced Index",
        ),
    ] {
        session.execute("SAVEPOINT attempt").unwrap();
        let error = session.execute(statement).unwrap_err();
        assert_eq!(error.sqlstate(), "23505", "{profile}: {statement}");
        let fields = error.fields();
        assert!(
            fields.contains(&SqlErrorField::Schema("constraint_owner".into())),
            "{profile}: {fields:?}"
        );
        assert!(
            fields.contains(&SqlErrorField::Table("Mixed.Rows".into())),
            "{profile}: {fields:?}"
        );
        assert!(
            fields.contains(&SqlErrorField::Constraint(constraint.into())),
            "{profile}: {fields:?}"
        );
        if oracle::is_oracle_profile(profile) {
            assert_eq!(
                error.to_string(),
                format!("ORA-00001: unique constraint (constraint_owner.{constraint}) violated")
            );
            let detail = fields
                .iter()
                .find_map(|field| match field {
                    SqlErrorField::Detail(value) => Some(value),
                    _ => None,
                })
                .unwrap();
            let detail: serde_json::Value = serde_json::from_str(detail).unwrap();
            assert_eq!(detail["oracle_error_stack"][0]["code"], -1);
        } else {
            assert_eq!(
                error.to_string(),
                format!("duplicate key value violates unique constraint \"{constraint}\"")
            );
            assert!(!fields
                .iter()
                .any(|field| matches!(field, SqlErrorField::Detail(_))));
        }
        session.execute("ROLLBACK TO SAVEPOINT attempt").unwrap();
        assert_eq!(
            session
                .execute("SELECT COUNT(*) FROM constraint_owner.\"Mixed.Rows\"")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "2"
        );
    }
    session.execute("ROLLBACK").unwrap();
    let names = session
        .execute("SELECT indexname FROM pg_indexes WHERE schemaname='constraint_owner'")
        .unwrap()
        .rows
        .into_iter()
        .map(|row| row[0].to_cell())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        names,
        ["pk_rows", "Unique.Dot", "Spaced Index"]
            .map(str::to_owned)
            .into_iter()
            .collect()
    );
    session
        .execute("SET search_path=constraint_owner,public")
        .unwrap();
    session.execute("DROP INDEX \"Spaced Index\"").unwrap();
    session
        .execute("INSERT INTO constraint_owner.\"Mixed.Rows\" VALUES(3,30,100)")
        .unwrap();
    session
        .execute("DELETE FROM constraint_owner.\"Mixed.Rows\" WHERE id=3")
        .unwrap();
    session.execute("BEGIN").unwrap();
    session.execute("ALTER TABLE constraint_owner.\"Mixed.Rows\" RENAME CONSTRAINT \"Unique.Dot\" TO \"Renamed Unique\"").unwrap();
    let error = session
        .execute("INSERT INTO constraint_owner.\"Mixed.Rows\" VALUES(3,10,300)")
        .unwrap_err();
    assert!(error
        .fields()
        .contains(&SqlErrorField::Constraint("Renamed Unique".into())));
    session.execute("ROLLBACK").unwrap();
    drop(session);
    drop(db);
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let error = session
        .execute("INSERT INTO constraint_owner.\"Mixed.Rows\" VALUES(3,10,300)")
        .unwrap_err();
    assert!(error
        .fields()
        .contains(&SqlErrorField::Constraint("Unique.Dot".into())));
}

#[test]
fn postgres_unique_diagnostics_preserve_standard_identity() {
    unique_diagnostics(oracle::POSTGRES_PROFILE);
}

#[test]
fn oracle_preview_unique_diagnostics_include_resolved_owner_and_name() {
    unique_diagnostics(oracle::ORACLE_PREVIEW_PROFILE);
}

fn validation_diagnostics(profile: &str) {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    oracle::ensure_database_profile(&mut db, profile).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE SCHEMA validation_owner").unwrap();
    session
        .execute("CREATE TABLE validation_owner.duplicates(key_value INTEGER,a INTEGER)")
        .unwrap();
    session
        .execute("INSERT INTO validation_owner.duplicates VALUES(1,10),(1,10)")
        .unwrap();
    for (statement, expected) in [
        (
            "ALTER TABLE validation_owner.duplicates ADD CONSTRAINT pk_duplicate PRIMARY KEY(key_value)",
            "ORA-02437: cannot validate (validation_owner.pk_duplicate) - primary key violated",
        ),
        (
            "ALTER TABLE validation_owner.duplicates ADD CONSTRAINT uq_duplicate UNIQUE(a)",
            "ORA-02299: cannot validate (validation_owner.uq_duplicate) - duplicate keys found",
        ),
        (
            "CREATE UNIQUE INDEX ix_duplicate ON validation_owner.duplicates(a)",
            "ORA-01452: cannot CREATE UNIQUE INDEX; duplicate keys found",
        ),
    ] {
        let error = session.execute(statement).unwrap_err();
        assert_eq!(error.sqlstate(), "23505", "{profile}: {error}");
        if oracle::is_oracle_profile(profile) {
            assert_eq!(error.to_string(), expected);
        } else {
            assert!(!error.to_string().starts_with("ORA-"));
        }
        assert_eq!(
            session
                .execute("SELECT COUNT(*) FROM validation_owner.duplicates")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "2"
        );
    }
    assert_eq!(
        session
            .execute("SELECT COUNT(*) FROM pg_indexes WHERE schemaname='validation_owner'")
            .unwrap()
            .rows[0][0]
            .to_cell(),
        "0"
    );
}

#[test]
fn postgres_unique_validation_errors_remain_postgresql_errors() {
    validation_diagnostics(oracle::POSTGRES_PROFILE);
}

#[test]
fn oracle_unique_validation_errors_keep_their_operation_identity() {
    validation_diagnostics(oracle::ORACLE_PREVIEW_PROFILE);
}

fn qualified_index_identity(profile: &str) {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    oracle::ensure_database_profile(&mut db, profile).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE SCHEMA index_owner").unwrap();
    session
        .execute("CREATE TABLE index_owner.rows(row_key INTEGER PRIMARY KEY,value INTEGER)")
        .unwrap();
    // Qualified index names used the schema-storage encoding in existing
    // catalogs. That key must never be reported as the SQL constraint name.
    session
        .execute("CREATE UNIQUE INDEX index_owner.\"Qualified.Index\" ON index_owner.rows(value)")
        .unwrap();
    session
        .execute("INSERT INTO index_owner.rows VALUES(1,10)")
        .unwrap();
    let error = session
        .execute("INSERT INTO index_owner.rows VALUES(2,10)")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "23505");
    assert!(
        error
            .fields()
            .contains(&SqlErrorField::Constraint("Qualified.Index".into())),
        "{profile}: {error:?}"
    );
    if oracle::is_oracle_profile(profile) {
        assert_eq!(
            error.to_string(),
            "ORA-00001: unique constraint (index_owner.Qualified.Index) violated"
        );
    }
}

#[test]
fn postgres_qualified_index_storage_names_do_not_leak_into_diagnostics() {
    qualified_index_identity(oracle::POSTGRES_PROFILE);
}

#[test]
fn oracle_qualified_index_storage_names_do_not_leak_into_diagnostics() {
    qualified_index_identity(oracle::ORACLE_PREVIEW_PROFILE);
}
