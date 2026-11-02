use super::*;

fn execute(db: &mut BicDb, sql: &str) -> SqlResult {
    let sql = lower_session_sql(db, sql, ORACLE_PREVIEW_PROFILE, None, "bicdb").unwrap();
    SqlSession::new(db)
        .execute(&sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
}

fn database(path: &std::path::Path) -> BicDb {
    let mut db =
        BicDb::open_with_config(path, bicdb_core::DbConfig::default().with_fsync(false)).unwrap();
    ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
    db
}

#[test]
fn temporal_columns_retain_source_types_storage_rules_and_anchors() {
    let root = tempfile::tempdir().unwrap();
    let mut db = database(root.path());
    execute(&mut db,"CREATE TABLE events(k NUMBER PRIMARY KEY,d DATE,t TIMESTAMP,t0 TIMESTAMP(0),t3 TIMESTAMP(3))");
    assert!(metadata_v2_enabled(&db).unwrap());
    execute(&mut db,"INSERT INTO events VALUES(1,TIMESTAMP '2024-02-29 23:59:59.999999',TIMESTAMP '2024-02-29 23:59:59.999999',TIMESTAMP '2024-02-29 23:59:59.999999',TIMESTAMP '2024-02-29 23:59:59.999999')");
    let result = execute(&mut db, "SELECT d,t,t0,t3 FROM events");
    assert_eq!(
        result.rows[0]
            .iter()
            .map(SqlValue::to_cell)
            .collect::<Vec<_>>(),
        vec![
            "2024-02-29 23:59:59",
            "2024-02-29 23:59:59.999999",
            "2024-03-01 00:00:00",
            "2024-03-01 00:00:00"
        ]
    );
    assert_eq!(result.column_types, vec![Some("timestamp".into()); 4]);
    for (column, expected) in [
        ("d", "2024-02-29 23:59:59"),
        ("t", "2024-02-29 23:59:59.999999"),
        ("t0", "2024-03-01 00:00:00"),
        ("t3", "2024-03-01 00:00:00"),
    ] {
        execute(&mut db,&format!("CREATE FUNCTION anchored_{column}(p_stamp TIMESTAMP) RETURN TIMESTAMP IS v events.{column}%TYPE; BEGIN v:=p_stamp; RETURN v; END;"));
        assert_eq!(
            execute(
                &mut db,
                &format!(
                    "SELECT anchored_{column}(TIMESTAMP '2024-02-29 23:59:59.999999') FROM DUAL"
                )
            )
            .rows[0][0]
                .to_cell(),
            expected
        );
    }
    assert_eq!(execute(&mut db,"SELECT bicdb_oracle_column_type('\"public\".\"EVENTS\"','D'),bicdb_oracle_column_type('\"public\".\"EVENTS\"','T0') FROM DUAL").rows[0],vec![SqlValue::String("DATE".into()),SqlValue::String("TIMESTAMP(0)".into())]);
}

#[test]
fn temporal_columns_trace_query_records_ctes_and_derived_tables() {
    let root = tempfile::tempdir().unwrap();
    let mut db = database(root.path());
    execute(
        &mut db,
        "CREATE TABLE events(k NUMBER PRIMARY KEY,happened DATE,stamp TIMESTAMP(0))",
    );
    execute(&mut db,"INSERT INTO events VALUES(1,TIMESTAMP '2024-02-29 12:00:00',TIMESTAMP '2024-02-29 12:00:00')");
    execute(&mut db,"CREATE FUNCTION record_delta RETURN NUMBER IS total NUMBER:=0; CURSOR c IS SELECT * FROM events; BEGIN FOR r IN c LOOP total:=total+(r.happened-DATE '2024-02-28'); END LOOP; RETURN total; END;");
    for sql in ["SELECT happened-DATE '2024-02-28' FROM events","SELECT e.happened-DATE '2024-02-28' FROM events e","WITH chosen AS (SELECT happened AS value FROM events) SELECT value-DATE '2024-02-28' FROM chosen","SELECT chosen.value-DATE '2024-02-28' FROM (SELECT happened AS value FROM events) chosen","SELECT record_delta() FROM DUAL"] {
        let result=execute(&mut db,sql);assert_eq!(parse_decimal(&result.rows[0][0].to_cell()).unwrap_or_else(|error| panic!("{sql}: {error}; types={:?}", result.column_types)).cmp(&parse_decimal("1.5").unwrap()), std::cmp::Ordering::Equal, "{sql}");assert_eq!(result.column_types,vec![Some("numeric".into())],"{sql}");
    }
    let result = execute(&mut db, "SELECT happened-stamp FROM events");
    assert_eq!(result.column_types, vec![Some("interval".into())]);
    assert_eq!(
        PgInterval::from_postgres_text(&result.rows[0][0].to_cell())
            .unwrap()
            .micros,
        0
    );
}

#[test]
fn temporal_source_metadata_survives_reopen_rename_and_native_backup() {
    let root = tempfile::tempdir().unwrap();
    let backup = tempfile::tempdir().unwrap();
    let restored = tempfile::tempdir().unwrap();
    {
        let mut db = database(root.path());
        execute(&mut db, "CREATE TABLE events(happened DATE)");
        execute(
            &mut db,
            "INSERT INTO events VALUES(TIMESTAMP '2024-02-29 12:34:56.999999')",
        );
        execute(
            &mut db,
            "ALTER TABLE events RENAME COLUMN happened TO occurred",
        );
        execute(&mut db, "ALTER TABLE events RENAME TO moved");
        db.create_consistent_backup(
            backup.path().join("temporal.bak"),
            bicdb_core::BackupCreateOptions {
                passphrase: "temporal-column-regression-only".into(),
                base_backup: None,
            },
        )
        .unwrap();
    }
    bicdb_core::restore_backup(
        backup.path().join("temporal.bak"),
        restored.path(),
        bicdb_core::BackupRestoreOptions {
            passphrase: "temporal-column-regression-only".into(),
            force: true,
        },
    )
    .unwrap();
    for path in [root.path(), restored.path()] {
        let mut db = BicDb::open(path).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        assert_eq!(
            execute(
                &mut db,
                "SELECT bicdb_oracle_column_type('\"public\".\"MOVED\"','OCCURRED') FROM DUAL"
            )
            .rows[0][0]
                .to_cell(),
            "DATE"
        );
        assert_eq!(
            execute(&mut db, "SELECT occurred FROM moved").rows[0][0].to_cell(),
            "2024-02-29 12:34:56"
        );
    }
}

#[test]
fn native_postgres_dates_and_temporal_rounding_remain_independent() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE events(d DATE,t TIMESTAMP(0))")
        .unwrap();
    session.execute("INSERT INTO events VALUES(TIMESTAMP '2024-02-29 23:59:59.999999',TIMESTAMP '2024-02-29 23:59:59.999999')").unwrap();
    let result = session.execute("SELECT d,t FROM events").unwrap();
    assert_eq!(
        result.rows[0]
            .iter()
            .map(SqlValue::to_cell)
            .collect::<Vec<_>>(),
        vec!["2024-02-29", "2024-03-01 00:00:00"]
    );
    assert_eq!(
        result.column_types,
        vec![Some("date".into()), Some("timestamp".into())]
    );
    assert!(!metadata_v2_enabled(session.db_ref()).unwrap());
    session
        .execute("CREATE TABLE quoted_types(\"Mixed.Stamp\" TIMESTAMP(0))")
        .unwrap();
    session
        .execute("INSERT INTO quoted_types VALUES(TIMESTAMP '2024-02-29 12:34:56')")
        .unwrap();
    session
        .execute("ALTER TABLE quoted_types ALTER COLUMN \"Mixed.Stamp\" TYPE TIMESTAMP(3)")
        .unwrap();
    let result = session
        .execute("SELECT \"Mixed.Stamp\" FROM quoted_types")
        .unwrap();
    assert_eq!(result.columns, vec!["Mixed.Stamp"]);
    assert_eq!(result.rows[0][0].to_cell(), "2024-02-29 12:34:56");
}

#[test]
fn date_index_lookups_keep_fractional_timestamp_comparison_values() {
    let root = tempfile::tempdir().unwrap();
    let mut db = database(root.path());
    execute(&mut db, "CREATE TABLE keyed(happened DATE PRIMARY KEY)");
    execute(
        &mut db,
        "INSERT INTO keyed VALUES(TIMESTAMP '2024-02-29 12:34:56.999999')",
    );
    for (sql, count) in [
        (
            "SELECT COUNT(*) FROM keyed WHERE happened=TIMESTAMP '2024-02-29 12:34:56.999999'",
            0,
        ),
        (
            "SELECT COUNT(*) FROM keyed WHERE happened=TIMESTAMP '2024-02-29 12:34:56'",
            1,
        ),
        (
            "SELECT COUNT(*) FROM keyed WHERE happened<TIMESTAMP '2024-02-29 12:34:56.5'",
            1,
        ),
    ] {
        assert_eq!(
            execute(&mut db, sql).rows[0][0].to_cell(),
            count.to_string(),
            "{sql}"
        );
    }
}

#[test]
fn composite_temporal_attributes_activate_the_same_reader_fence() {
    let root = tempfile::tempdir().unwrap();
    let mut db = database(root.path());
    assert!(!metadata_v2_enabled(&db).unwrap());
    SqlSession::new(&mut db)
        .execute("CREATE TYPE time_record AS (happened DATE)")
        .unwrap();
    assert!(metadata_v2_enabled(&db).unwrap());
    let stored = load_user_type(&db, "public", "time_record")
        .unwrap()
        .unwrap();
    let UserTypeKind::Composite { attributes, .. } = stored.kind else {
        panic!("expected a composite type")
    };
    assert!(matches!(
        attributes[0].type_modifier,
        Some(PgTypeModifier::OracleTemporalV1 {
            source: Source::Date,
            precision: 0
        })
    ));
}

#[test]
fn modify_legacy_temporal_column_requires_explicit_source_declaration() {
    let root = tempfile::tempdir().unwrap();
    let mut db = database(root.path());
    execute(&mut db, "CREATE TABLE old_events(happened TIMESTAMP(0))");
    execute(
        &mut db,
        "INSERT INTO old_events VALUES(TIMESTAMP '2024-02-29 12:34:56')",
    );
    let name = relation_name_from_parts(&["public".into(), "OLD_EVENTS".into()]).unwrap();
    let mut schema = load_schema(&db, &name).unwrap().unwrap();
    schema.columns[0].type_modifier = Some(PgTypeModifier::Temporal { precision: 0 });
    save_schema(&mut db, &schema).unwrap();
    let source="CREATE FUNCTION old_anchor RETURN DATE IS v old_events.happened%TYPE; BEGIN RETURN v; END;";
    assert!(lower_profile_sql(&db, source, ORACLE_PREVIEW_PROFILE)
        .unwrap_err()
        .to_string()
        .contains("source logical type provenance"));
    assert!(lower_profile_sql(
        &db,
        "SELECT happened-DATE '2024-02-28' FROM old_events",
        ORACLE_PREVIEW_PROFILE,
    )
    .unwrap_err()
    .to_string()
    .contains("source logical type provenance"));
    execute(
        &mut db,
        "CREATE VIEW event_view AS SELECT happened FROM old_events",
    );
    execute(&mut db, "ALTER TABLE old_events MODIFY (happened DATE)");
    let after = load_schema(&db, &name).unwrap().unwrap();
    assert_eq!(after.columns[0].name, "HAPPENED", "{after:?}");
    execute(&mut db, source);
    assert_eq!(
        execute(&mut db, "SELECT happened FROM old_events").rows[0][0].to_cell(),
        "2024-02-29 12:34:56"
    );
    assert_eq!(
        execute(
            &mut db,
            "SELECT bicdb_oracle_column_type('\"public\".\"OLD_EVENTS\"','HAPPENED') FROM DUAL"
        )
        .rows[0][0]
            .to_cell(),
        "DATE"
    );
    assert_eq!(
        execute(&mut db, "SELECT happened FROM event_view").rows[0][0].to_cell(),
        "2024-02-29 12:34:56"
    );
    // Once provenance exists, changing the logical type is a real type change
    // and must retain the ordinary dependent-view guard.
    let sql = lower_profile_sql(
        &db,
        "ALTER TABLE old_events MODIFY (happened TIMESTAMP(0))",
        ORACLE_PREVIEW_PROFILE,
    )
    .unwrap();
    assert!(SqlSession::new(&mut db).execute(&sql).is_err());
    assert_eq!(
        super::source(&load_schema(&db, &name).unwrap().unwrap().columns[0]),
        Some(Source::Date)
    );
}
