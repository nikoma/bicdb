//! AL32UTF8 character column provenance for the STANDARD preview contract.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Unit {
    Bytes,
    Characters,
}

pub(crate) fn modifier(db: &BicDb, data_type: &DataType) -> Result<Option<PgTypeModifier>> {
    if !preview_routine_language_enabled(db)? {
        return Ok(None);
    }
    let (fixed, length) = match data_type {
        DataType::Char(length) | DataType::Character(length) => (true, *length),
        DataType::Varchar(length)
        | DataType::CharVarying(length)
        | DataType::CharacterVarying(length) => (false, *length),
        _ => return Ok(None),
    };
    let (length, unit) = match length {
        None if fixed => (1, Unit::Bytes),
        Some(sqlparser::ast::CharacterLength::IntegerLength { length, unit }) => (
            length,
            match unit {
                Some(sqlparser::ast::CharLengthUnits::Characters) => Unit::Characters,
                _ => Unit::Bytes,
            },
        ),
        _ => {
            return Err(unsupported(
                "Oracle character column requires a scalar length",
            ))
        }
    };
    let maximum = if fixed { 2000 } else { 4000 };
    if !(1..=maximum).contains(&length) {
        return Err(unsupported(
            "Oracle character column length exceeds the STANDARD profile bound",
        ));
    }
    Ok(Some(PgTypeModifier::OracleCharacterV1 {
        fixed,
        length: length as u32,
        unit,
    }))
}

pub(crate) fn coerce(value: SqlValue, fixed: bool, length: u32, unit: Unit) -> Result<SqlValue> {
    let SqlValue::String(mut text) = value else {
        return Ok(value);
    };
    if text.is_empty() {
        return Ok(SqlValue::Null);
    }
    let actual = match unit {
        Unit::Bytes => text.len(),
        Unit::Characters => text.chars().count(),
    };
    let byte_cap = if fixed { 2000 } else { 4000 };
    if actual > length as usize || text.len() > byte_cap {
        return Err(errors::database_error(
            -12899,
            "22001",
            "value too large for Oracle character column",
        ));
    }
    if fixed {
        let padding = (length as usize - actual).min(byte_cap - text.len());
        text.extend(std::iter::repeat(' ').take(padding));
    }
    Ok(SqlValue::String(text))
}

pub(crate) fn validate(db: &BicDb, schema: &TableSchema) -> Result<()> {
    for column in &schema.columns {
        if let Some(PgTypeModifier::OracleCharacterV1 { fixed, length, .. }) = column.type_modifier
        {
            if column.pg_type != if fixed { "bpchar" } else { "varchar" }
                || column.user_type.is_some()
                || column.array_ndims != 0
                || length == 0
                || length > if fixed { 2000 } else { 4000 }
                || !metadata_v3_enabled(db)?
            {
                return Err(unsupported(
                    "invalid or unfenced Oracle character column metadata",
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn prepare_save(db: &mut BicDb, schema: &TableSchema) -> Result<()> {
    if schema.columns.iter().any(|c| {
        matches!(
            c.type_modifier,
            Some(PgTypeModifier::OracleCharacterV1 { .. })
        )
    }) {
        enable_metadata_v3(db)?;
    }
    validate(db, schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn run(db: &mut BicDb, sql: &str) -> Result<SqlResult> {
        let sql = lower_profile_sql(db, sql, ORACLE_PREVIEW_PROFILE)?;
        SqlSession::new(db).execute(&sql)
    }
    #[test]
    fn character_columns_preserve_units_padding_errors_and_reopen() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        run(&mut db,"CREATE TABLE char_units(id NUMBER PRIMARY KEY,b VARCHAR2(3 BYTE),c VARCHAR2(3 CHAR),fb CHAR(3 BYTE),fc CHAR(3 CHAR))").unwrap();
        run(&mut db, "INSERT INTO char_units VALUES(1,'é','😀','é','é')").unwrap();
        assert_eq!(
            run(&mut db, "SELECT b,c,fb,fc FROM char_units")
                .unwrap()
                .rows[0],
            vec![
                SqlValue::String("é".into()),
                SqlValue::String("😀".into()),
                SqlValue::String("é ".into()),
                SqlValue::String("é  ".into())
            ]
        );
        for sql in [
            "INSERT INTO char_units(id,b) VALUES(2,'😀')",
            "UPDATE char_units SET b='a   ' WHERE id=1",
        ] {
            let error = run(&mut db, sql).unwrap_err();
            assert_eq!(error.sqlstate(), "22001", "{sql}: {error}");
        }
        assert!(metadata_v3_enabled(&db).unwrap());
        assert_eq!(run(&mut db, r#"SELECT bicdb_oracle_column_type('"CHAR_UNITS"','B'),bicdb_oracle_column_type('"CHAR_UNITS"','C') FROM DUAL"#).unwrap().rows[0],vec![SqlValue::String("VARCHAR2(3 BYTE)".into()),SqlValue::String("VARCHAR2(3 CHAR)".into())]);
        let schema = load_schema(&db, "CHAR_UNITS").unwrap().unwrap();
        assert_eq!(schema.columns[1].character_octet_length(), Some(3));
        assert_eq!(schema.columns[2].character_octet_length(), Some(12));
        drop(db);
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        assert_eq!(
            run(&mut db, "UPDATE char_units SET b='😀' WHERE id=1")
                .unwrap_err()
                .sqlstate(),
            "22001"
        );
        assert!(metadata_v3_enabled(&db).unwrap());
    }
    #[test]
    fn changing_character_units_checks_existing_values_and_rolls_back() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        run(&mut db, "CREATE TABLE unit_change(v VARCHAR2(3 CHAR))").unwrap();
        run(&mut db, "INSERT INTO unit_change VALUES('😀')").unwrap();
        let error = SqlSession::new(&mut db)
            .execute("ALTER TABLE \"UNIT_CHANGE\" ALTER COLUMN \"V\" TYPE VARCHAR(3 OCTETS)")
            .unwrap_err();
        assert_eq!(error.sqlstate(), "22001");
        assert!(matches!(
            load_schema(&db, "UNIT_CHANGE").unwrap().unwrap().columns[0].type_modifier,
            Some(PgTypeModifier::OracleCharacterV1 {
                unit: Unit::Characters,
                ..
            })
        ));
        run(&mut db, "UPDATE unit_change SET v='é'").unwrap();
        SqlSession::new(&mut db)
            .execute("ALTER TABLE \"UNIT_CHANGE\" ALTER COLUMN \"V\" TYPE VARCHAR(3 OCTETS)")
            .unwrap();
        assert_eq!(
            run(&mut db, "UPDATE unit_change SET v='😀'")
                .unwrap_err()
                .sqlstate(),
            "22001"
        );
    }

    #[test]
    fn character_metadata_is_monotonic_and_missing_fence_is_rejected() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        run(&mut db, "CREATE TABLE fenced_char(v VARCHAR2(3 BYTE))").unwrap();
        run(&mut db, "CREATE TABLE after_char(d DATE)").unwrap();
        assert!(metadata_v3_enabled(&db).unwrap());
        db.insert(
            PROFILE_COLLECTION,
            Record::new("profile").with_metadata(metadata_v2()),
        )
        .unwrap();
        drop(db);
        let db = BicDb::open(root.path()).unwrap();
        assert_eq!(
            load_schema(&db, "FENCED_CHAR").unwrap_err().sqlstate(),
            "0A000"
        );
    }

    #[test]
    fn source_character_metadata_attaches_without_rewriting_rows_or_views() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        run(&mut db, "CREATE TABLE legacy_units(id NUMBER PRIMARY KEY,v VARCHAR2(3 CHAR),f CHAR(3 CHAR),CHECK(id>0),UNIQUE(v))").unwrap();
        run(&mut db, "INSERT INTO legacy_units VALUES(1,'é','A')").unwrap();
        let mut schema = load_schema(&db, "LEGACY_UNITS").unwrap().unwrap();
        for column in &mut schema.columns {
            if matches!(
                column.type_modifier,
                Some(PgTypeModifier::OracleCharacterV1 { .. })
            ) {
                column.type_modifier = Some(PgTypeModifier::Character { length: 3 });
            }
        }
        save_schema(&mut db, &schema).unwrap();
        run(
            &mut db,
            "CREATE VIEW unit_projection AS SELECT v,f FROM legacy_units",
        )
        .unwrap();
        let before = db.scan_collection("LEGACY_UNITS").unwrap();
        run(
            &mut db,
            "ALTER TABLE legacy_units MODIFY(v VARCHAR2(3 BYTE))",
        )
        .unwrap();
        run(&mut db, "ALTER TABLE legacy_units MODIFY(f CHAR(3 BYTE))").unwrap();
        assert_eq!(db.scan_collection("LEGACY_UNITS").unwrap(), before);
        assert_eq!(
            run(&mut db, "SELECT v,f FROM unit_projection")
                .unwrap()
                .rows,
            vec![vec![
                SqlValue::String("é".into()),
                SqlValue::String("A  ".into())
            ]]
        );
        // Once the source unit is known, a different unit remains a real type
        // change and retains the ordinary dependent-view guard.
        assert!(run(
            &mut db,
            "ALTER TABLE legacy_units MODIFY(v VARCHAR2(3 CHAR))"
        )
        .is_err());
        assert_eq!(
            run(&mut db, "UPDATE legacy_units SET v='😀' WHERE id=1")
                .unwrap_err()
                .sqlstate(),
            "22001"
        );
        assert_eq!(
            run(&mut db, "INSERT INTO legacy_units VALUES(2,'é','A')")
                .unwrap_err()
                .sqlstate(),
            "23505"
        );
    }

    #[test]
    fn standard_byte_ceiling_bounds_fixed_padding() {
        let value = coerce(SqlValue::String("é".into()), true, 2000, Unit::Characters)
            .unwrap()
            .to_cell();
        assert_eq!(value.len(), 2000);
        assert_eq!(value.chars().count(), 1999);
        assert!(coerce(
            SqlValue::String("é".repeat(2001)),
            false,
            4000,
            Unit::Characters
        )
        .is_err());
        assert_eq!(
            coerce(
                SqlValue::String("é".repeat(2000)),
                false,
                4000,
                Unit::Characters
            )
            .unwrap()
            .to_cell()
            .len(),
            4000
        );
    }

    #[test]
    fn native_character_assignment_keeps_postgres_trailing_space_rules() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        let mut s = SqlSession::new(&mut db);
        s.execute("CREATE TABLE native_chars(v VARCHAR(1))")
            .unwrap();
        s.execute("INSERT INTO native_chars VALUES('😀')").unwrap();
        s.execute("INSERT INTO native_chars VALUES('a  ')").unwrap();
        assert_eq!(
            s.execute("SELECT v FROM native_chars ORDER BY v")
                .unwrap()
                .rows,
            vec![
                vec![SqlValue::String("a".into())],
                vec![SqlValue::String("😀".into())]
            ]
        );
    }
}
