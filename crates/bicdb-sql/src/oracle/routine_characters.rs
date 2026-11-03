//! Versioned AL32UTF8 PL/SQL buffers, independent of SQL column byte limits.
use super::*;
use character_columns::Unit;

thread_local! { static ENABLED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

pub(crate) struct Scope(bool);
impl Scope {
    pub(crate) fn new(enabled: bool) -> Self {
        Self(ENABLED.with(|v| v.replace(enabled)))
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        ENABLED.with(|v| v.set(self.0));
    }
}

pub(crate) fn enabled() -> bool {
    ENABLED.with(std::cell::Cell::get)
}

pub(crate) fn routine_type(data_type: &DataType) -> Result<Option<RoutineTypeSchema>> {
    if !enabled() {
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
        None => (None, Unit::Bytes),
        Some(sqlparser::ast::CharacterLength::IntegerLength { length, unit })
            if (1..=32767).contains(&length) =>
        {
            (
                Some(length as u32),
                if unit == Some(sqlparser::ast::CharLengthUnits::Characters) {
                    Unit::Characters
                } else {
                    Unit::Bytes
                },
            )
        }
        _ => {
            return Err(unsupported(
                "PL/SQL character length must be between 1 and 32767",
            ))
        }
    };
    Ok(Some(RoutineTypeSchema {
        pg_type: if fixed { "bpchar" } else { "varchar" }.into(),
        type_modifier: Some(PgTypeModifier::OracleRoutineCharacterV1 {
            fixed,
            length,
            unit,
        }),
    }))
}

pub(crate) fn local_modifier(modifier: &mut Option<PgTypeModifier>) -> Result<()> {
    if let Some(PgTypeModifier::OracleRoutineCharacterV1 { fixed, length, .. }) = modifier {
        if length.is_none() {
            if *fixed {
                *length = Some(1);
            } else {
                return Err(unsupported(
                    "PL/SQL VARCHAR2 local declarations require a length",
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn coerce(
    value: SqlValue,
    fixed: bool,
    length: Option<u32>,
    unit: Unit,
) -> Result<SqlValue> {
    let mut text = match value {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::String(text) => text,
        _ => {
            return Err(unsupported(
                "PL/SQL character assignment requires a text value",
            ))
        }
    };
    if text.is_empty() {
        return Ok(SqlValue::Null);
    }
    let Some(length) = length.filter(|n| (1..=32767).contains(n)) else {
        return Err(unsupported(
            "PL/SQL character buffer requires a qualified length",
        ));
    };
    let actual = if unit == Unit::Bytes {
        text.len()
    } else {
        text.chars().count()
    };
    let padding = if fixed {
        (length as usize)
            .saturating_sub(actual)
            .min(32767usize.saturating_sub(text.len()))
    } else {
        0
    };
    if actual > length as usize || text.len().saturating_add(padding) > 32767 {
        return Err(errors::database_error(
            -6502,
            "22001",
            "PL/SQL: numeric or value error: character string buffer too small",
        ));
    }
    text.extend(std::iter::repeat(' ').take(padding));
    Ok(SqlValue::String(text))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(db: &mut BicDb, sql: &str) -> Result<SqlResult> {
        let sql = lower_profile_sql(db, sql, ORACLE_PREVIEW_PROFILE)?;
        SqlSession::new(db).execute(&sql)
    }

    #[test]
    fn byte_buffers_keep_units_defaults_anchors_and_reopen() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        run(
            &mut db,
            "CREATE TABLE byte_anchor(v VARCHAR2(3 BYTE),c VARCHAR2(3 CHAR))",
        )
        .unwrap();
        for (name, declaration) in [
            ("vb", "VARCHAR2(3 BYTE)"),
            ("vc", "VARCHAR2(3 CHAR)"),
            ("cb", "CHAR(3 BYTE)"),
            ("cc", "CHAR(3 CHAR)"),
            ("cd", "CHAR"),
            ("va", "byte_anchor.v%TYPE"),
            ("vca", "byte_anchor.c%TYPE"),
        ] {
            run(&mut db, &format!("CREATE FUNCTION {name}(p VARCHAR2) RETURN VARCHAR2 IS v {declaration}; BEGIN v:=p;RETURN v;END;")).unwrap();
        }
        for (name, expected) in [
            ("vb", "é"),
            ("vc", "é"),
            ("cb", "é "),
            ("cc", "é  "),
            ("va", "é"),
        ] {
            assert_eq!(
                run(&mut db, &format!("SELECT {name}('é') FROM DUAL"))
                    .unwrap()
                    .rows[0][0],
                SqlValue::String(expected.into())
            );
        }
        for name in ["vb", "cb", "cd", "va"] {
            let error = run(&mut db, &format!("SELECT {name}('😀') FROM DUAL")).unwrap_err();
            assert_eq!(error.sqlstate(), "22001", "{name}: {error}");
            assert!(format!("{error:?}").contains("6502"));
        }
        assert_eq!(
            run(
                &mut db,
                "SELECT vc('😀'),cc('😀'),cb(NULL),vb('') FROM DUAL"
            )
            .unwrap()
            .rows[0],
            vec![
                SqlValue::String("😀".into()),
                SqlValue::String("😀  ".into()),
                SqlValue::Null,
                SqlValue::Null
            ]
        );
        drop(db);
        let mut db = BicDb::open(root.path()).unwrap();
        assert_eq!(
            run(&mut db, "SELECT vca('😀') FROM DUAL").unwrap().rows[0][0],
            SqlValue::String("😀".into())
        );
        assert_eq!(
            run(&mut db, "SELECT va('😀') FROM DUAL")
                .unwrap_err()
                .sqlstate(),
            "22001"
        );
        assert_eq!(
            run(&mut db, "SELECT cb('é') FROM DUAL").unwrap().rows[0][0],
            SqlValue::String("é ".into())
        );
    }

    #[test]
    fn char_unit_padding_and_varchar_values_obey_the_plsql_byte_ceiling() {
        assert_eq!(
            coerce(
                SqlValue::String("é".repeat(16383)),
                false,
                Some(32767),
                Unit::Characters
            )
            .unwrap()
            .to_cell()
            .len(),
            32766
        );
        for (text, fixed) in [("é".repeat(16384), false), ("é".repeat(16384), true)] {
            let error =
                coerce(SqlValue::String(text), fixed, Some(32767), Unit::Characters).unwrap_err();
            assert_eq!(error.sqlstate(), "22001");
        }
        let padded = coerce(
            SqlValue::String("é".into()),
            true,
            Some(32767),
            Unit::Characters,
        )
        .unwrap()
        .to_cell();
        assert_eq!(padded.len(), 32767);
        assert_eq!(padded.chars().count(), 32766);
        assert_eq!(
            coerce(
                SqlValue::String("a".into()),
                true,
                Some(32767),
                Unit::Characters
            )
            .unwrap()
            .to_cell()
            .len(),
            32767
        );
        assert!(coerce(SqlValue::String("a   ".into()), false, Some(3), Unit::Bytes).is_err());
    }

    #[test]
    fn older_compilers_and_native_routines_retain_character_count_semantics() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let old = lower_profile_sql(&db, "CREATE FUNCTION old_buffer(p VARCHAR2) RETURN VARCHAR2 IS v VARCHAR2(3);BEGIN v:=p;RETURN v;END;", ORACLE_PREVIEW_PROFILE).unwrap().replace(PREVIEW_ROUTINE_LANGUAGE, PREVIEW_ROUTINE_LANGUAGE_V17);
        SqlSession::new(&mut db).execute(&old).unwrap();
        run(&mut db, "CREATE FUNCTION new_buffer(p VARCHAR2) RETURN VARCHAR2 IS v VARCHAR2(3);BEGIN v:=p;RETURN v;END;").unwrap();
        assert_eq!(
            run(&mut db, "SELECT old_buffer('😀') FROM DUAL")
                .unwrap()
                .rows[0][0],
            SqlValue::String("😀".into())
        );
        assert_eq!(
            run(&mut db, "SELECT new_buffer('😀') FROM DUAL")
                .unwrap_err()
                .sqlstate(),
            "22001"
        );
        drop(db);
        let mut db = BicDb::open(root.path()).unwrap();
        assert_eq!(
            run(&mut db, "SELECT old_buffer('😀') FROM DUAL")
                .unwrap()
                .rows[0][0],
            SqlValue::String("😀".into())
        );
        let native = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(native.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session.execute("CREATE FUNCTION native_buffer(p varchar) RETURNS varchar LANGUAGE plpgsql AS $$DECLARE v varchar(3);BEGIN v:=p;RETURN v;END$$").unwrap();
        assert_eq!(
            session
                .execute("SELECT native_buffer('😀'),native_buffer('a   ')")
                .unwrap()
                .rows[0],
            vec![
                SqlValue::String("😀".into()),
                SqlValue::String("a  ".into())
            ]
        );
    }
}
