//! Durable Oracle BLOB declarations with native binary wire/storage codecs.
//!
//! This provenance is a prerequisite for locator semantics. It does not infer
//! Oracle identity for existing BYTEA columns or implement locator mutation.
use super::*;

pub(crate) fn modifier(db: &BicDb, data_type: &DataType) -> Result<Option<PgTypeModifier>> {
    if !preview_routine_language_enabled(db)? {
        return Ok(None);
    }
    match data_type {
        DataType::Blob(None) => Ok(Some(PgTypeModifier::OracleBlobV2)),
        DataType::Blob(Some(_)) => Err(unsupported(
            "Oracle BLOB declarations do not accept a length",
        )),
        _ => Ok(None),
    }
}

pub(crate) fn validate(db: &BicDb, schema: &TableSchema) -> Result<()> {
    for column in &schema.columns {
        if lobs::is_modifier(column.type_modifier.as_ref())
            && (column.pg_type != "bytea"
                || column.user_type.is_some()
                || column.array_ndims != 0
                || !metadata_v6_enabled(db)?
                || (matches!(column.type_modifier, Some(PgTypeModifier::OracleBlobV2))
                    && !metadata_v7_enabled(db)?))
        {
            return Err(unsupported(
                "invalid or unfenced Oracle BLOB column metadata",
            ));
        }
    }
    Ok(())
}

pub(crate) fn prepare_save(db: &mut BicDb, schema: &TableSchema) -> Result<()> {
    if schema
        .columns
        .iter()
        .any(|column| matches!(column.type_modifier, Some(PgTypeModifier::OracleBlobV2)))
    {
        enable_metadata_v7(db)?;
    } else if schema
        .columns
        .iter()
        .any(|column| lobs::is_modifier(column.type_modifier.as_ref()))
    {
        enable_metadata_v6(db)?;
    }
    validate(db, schema)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(db: &mut BicDb, sql: &str) -> Result<SqlResult> {
        let sql = lower_session_sql(db, sql, ORACLE_PREVIEW_PROFILE, None, "bicdb")?;
        SqlSession::new(db).execute(&sql)
    }

    #[test]
    fn blob_provenance_binary_payload_and_null_survive_reopen() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        run(
            &mut db,
            "CREATE TABLE binary_values(id NUMBER PRIMARY KEY, payload BLOB, native_bytes BYTEA)",
        )
        .unwrap();
        run(
            &mut db,
            "INSERT INTO binary_values VALUES(1,HEXTORAW('00FF'),HEXTORAW('AB'))",
        )
        .unwrap();
        run(&mut db, "INSERT INTO binary_values(id) VALUES(2)").unwrap();
        let expected = run(
            &mut db,
            "SELECT payload,native_bytes FROM binary_values ORDER BY id",
        )
        .unwrap()
        .rows;
        assert_eq!(
            expected[0]
                .iter()
                .map(SqlValue::to_cell)
                .collect::<Vec<_>>(),
            vec!["\\x00ff", "\\xab"]
        );
        assert_eq!(expected[1], vec![SqlValue::Null, SqlValue::Null]);
        let schema = load_schema(&db, "BINARY_VALUES").unwrap().unwrap();
        assert!(matches!(
            schema.columns[1].type_modifier,
            Some(PgTypeModifier::OracleBlobV2)
        ));
        assert!(schema.columns[2].type_modifier.is_none());
        assert_eq!(run(&mut db,"SELECT bicdb_oracle_column_type('\"BINARY_VALUES\"','PAYLOAD'),bicdb_oracle_column_type('\"BINARY_VALUES\"','NATIVE_BYTES') FROM DUAL").unwrap().rows[0],vec![SqlValue::String("BLOB".into()),SqlValue::Null]);
        assert!(metadata_v6_enabled(&db).unwrap());
        drop(db);
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        assert_eq!(
            run(
                &mut db,
                "SELECT payload,native_bytes FROM binary_values ORDER BY id"
            )
            .unwrap()
            .rows,
            expected
        );
    }

    #[test]
    fn blob_metadata_never_downgrades_when_other_features_are_saved() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        run(&mut db, "CREATE TABLE binary_values(payload BLOB)").unwrap();
        run(
            &mut db,
            "CREATE TABLE text_values(v VARCHAR2(8 CHAR),d DATE)",
        )
        .unwrap();
        enable_metadata_v5(&mut db).unwrap();
        prepare_hierarchy_save(&mut db, "SELECT 1 FROM DUAL CONNECT BY LEVEL < 2").unwrap();
        assert!(metadata_v6_enabled(&db).unwrap());
        assert!(metadata_v5_enabled(&db).unwrap());
        assert!(metadata_v3_enabled(&db).unwrap());
        assert!(metadata_v2_enabled(&db).unwrap());
    }

    #[test]
    fn explicit_blob_conversion_preserves_existing_binary_values() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        run(&mut db, "CREATE TABLE binary_values(payload BYTEA)").unwrap();
        run(
            &mut db,
            "INSERT INTO binary_values VALUES(HEXTORAW('00FF'))",
        )
        .unwrap();
        let before = run(&mut db, "SELECT payload FROM binary_values")
            .unwrap()
            .rows;
        run(&mut db, "ALTER TABLE binary_values MODIFY(payload BLOB)").unwrap();
        assert_eq!(
            run(&mut db, "SELECT payload FROM binary_values")
                .unwrap()
                .rows
                .iter()
                .map(|row| row.iter().map(SqlValue::to_cell).collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            before
                .iter()
                .map(|row| row.iter().map(SqlValue::to_cell).collect::<Vec<_>>())
                .collect::<Vec<_>>()
        );
        assert!(matches!(
            load_schema(&db, "BINARY_VALUES").unwrap().unwrap().columns[0].type_modifier,
            Some(PgTypeModifier::OracleBlobV2)
        ));
        run(&mut db, "ALTER TABLE binary_values ADD extra BLOB").unwrap();
        assert!(matches!(
            load_schema(&db, "BINARY_VALUES")
                .unwrap()
                .unwrap()
                .column("EXTRA")
                .unwrap()
                .type_modifier,
            Some(PgTypeModifier::OracleBlobV2)
        ));
        assert!(run(&mut db, "CREATE TABLE invalid_binary(v BLOB(10))").is_err());
    }

    #[test]
    fn default_postgres_binary_columns_remain_native() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE native_binary(v BYTEA)")
            .unwrap();
        session
            .execute("INSERT INTO native_binary VALUES('\\x00ff'::bytea)")
            .unwrap();
        assert_eq!(
            session.execute("SELECT v FROM native_binary").unwrap().rows[0],
            vec![SqlValue::String("\\x00ff".into())]
        );
        assert!(session
            .execute("CREATE TABLE unsupported_blob(v BLOB)")
            .is_err());
        assert!(!metadata_v6_enabled(&db).unwrap());
    }

    #[test]
    fn corrupt_or_unfenced_blob_metadata_is_rejected() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        run(&mut db, "CREATE TABLE binary_values(payload BLOB)").unwrap();
        let mut schema = load_schema(&db, "BINARY_VALUES").unwrap().unwrap();
        schema.columns[0].pg_type = "text".into();
        assert!(validate(&db, &schema).is_err());
        schema.columns[0].pg_type = "bytea".into();
        schema.columns[0].array_ndims = 1;
        assert!(validate(&db, &schema).is_err());
        schema.columns[0].array_ndims = 0;
        db.insert(
            PROFILE_COLLECTION,
            Record::new("profile").with_metadata(metadata_v5()),
        )
        .unwrap();
        assert!(validate(&db, &schema).is_err());
    }
}
