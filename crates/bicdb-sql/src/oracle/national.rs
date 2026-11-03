//! Compiler-local AL16UTF16 routine declarations, with ordinary wire text codecs.
use super::*;
use unicode_categories::UnicodeCategories;
use unicode_normalization::UnicodeNormalization;

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

pub(crate) fn is_modifier(modifier: Option<&PgTypeModifier>) -> bool {
    matches!(modifier, Some(PgTypeModifier::OracleNationalV1 { .. }))
}

pub(crate) fn routine_type(data_type: &DataType) -> Result<Option<RoutineTypeSchema>> {
    if !ENABLED.with(std::cell::Cell::get) {
        return Ok(None);
    }
    let source = tokens(&data_type.to_string())?;
    let Some(first) = source.first() else {
        return Ok(None);
    };
    let type_word = |name: &str| matches!(first, Token::Word(w) if if w.quote_style.is_some() { w.value == name } else { w.value.eq_ignore_ascii_case(name) });
    if type_word("NVARCHAR") {
        return Err(unsupported(
            "NVARCHAR is not an Oracle routine type; use NVARCHAR2",
        ));
    }
    let fixed = if type_word("NCHAR") {
        true
    } else if type_word("NVARCHAR2") {
        false
    } else {
        return Ok(None);
    };
    let length = match &source[1..] {
        [] => None,
        [Token::LParen, Token::Number(n, _), Token::RParen] => {
            let n = n
                .parse::<u32>()
                .map_err(|_| unsupported("invalid national-character length"))?;
            if !(1..=32767).contains(&n) {
                return Err(unsupported(
                    "national-character routine length must be between 1 and 32767",
                ));
            }
            Some(n)
        }
        _ => {
            return Err(unsupported(
                "national-character declaration requires a scalar length",
            ))
        }
    };
    if fixed && length.is_some_and(|n| n > 16383) {
        return Err(unsupported(
            "NCHAR routine lengths above 16383 require separate qualification",
        ));
    }
    Ok(Some(RoutineTypeSchema {
        pg_type: "varchar".into(),
        type_modifier: Some(PgTypeModifier::OracleNationalV1 { fixed, length }),
    }))
}

pub(crate) fn local_modifier(modifier: &mut Option<PgTypeModifier>) -> Result<()> {
    if let Some(PgTypeModifier::OracleNationalV1 { fixed, length }) = modifier {
        if length.is_none() {
            if *fixed {
                *length = Some(1);
            } else {
                return Err(unsupported("NVARCHAR2 local declarations require a length"));
            }
        }
    }
    Ok(())
}

pub(crate) fn returns_national(routine: &RoutineSchema) -> Result<bool> {
    Ok(return_type(routine)?.is_some())
}

pub(crate) fn return_type(routine: &RoutineSchema) -> Result<Option<RoutineTypeSchema>> {
    if routine.kind == RoutineKind::Procedure
        || !preview_routine_language_version(&routine.language).is_some_and(|v| v >= 17)
    {
        return Ok(None);
    }
    let _scope = Scope::new(true);
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, &routine.definition)
        .map_err(|e| SqlError::InvalidSql(e.to_string()))?;
    let [Statement::CreateFunction(function)] = statements.as_slice() else {
        return Err(unsupported(
            "national return requires a function definition",
        ));
    };
    match &function.return_type {
        Some(FunctionReturnType::DataType(data_type) | FunctionReturnType::SetOf(data_type)) => {
            routine_type(data_type)
        }
        _ => Ok(None),
    }
}

pub(crate) fn coerce(value: SqlValue, fixed: bool, length: Option<u32>) -> Result<SqlValue> {
    let mut text = match value {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::String(text) => text,
        _ => {
            return Err(unsupported(
                "national-character assignment requires a text value",
            ))
        }
    };
    if text.is_empty() {
        return Ok(SqlValue::Null);
    }
    let units = text.encode_utf16().count();
    if units > 16383 || length.is_some_and(|n| units > n as usize) {
        return Err(errors::database_error(
            -6502,
            "22001",
            "PL/SQL: numeric or value error: national character string buffer too small",
        ));
    }
    if fixed {
        if let Some(length) = length {
            let padding = length as usize - units;
            if length > 16383 {
                return Err(errors::database_error(
                    -6502,
                    "22001",
                    "PL/SQL: numeric or value error: national character string buffer too small",
                ));
            }
            text.extend(std::iter::repeat(' ').take(padding));
        }
    }
    Ok(SqlValue::String(text))
}

pub(crate) fn length(args: &[SqlValue]) -> Result<SqlValue> {
    let [value, mode] = args else {
        return Err(SqlError::InvalidSql(
            "national length requires a value and a unit".into(),
        ));
    };
    if matches!(value, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let text = value.to_cell();
    let units = match mode.to_cell().as_str() {
        "utf16" => text.encode_utf16().count(),
        "bytes" => text.encode_utf16().count() * 2,
        "characters" => text.chars().count(),
        "complete" => text.nfc().filter(|c| !c.is_mark_nonspacing()).count(),
        _ => return Err(unsupported("unknown national-character length unit")),
    };
    Ok(SqlValue::Int(units as i64))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn run(db: &mut BicDb, sql: &str) -> Result<SqlResult> {
        let sql = lower_profile_sql(db, sql, ORACLE_PREVIEW_PROFILE)?;
        SqlSession::new(db).execute(&sql)
    }
    #[test]
    fn national_locals_preserve_utf16_lengths_and_padding() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        for (name, declaration) in [
            ("national_varying", "NVARCHAR2(3)"),
            ("national_fixed", "NCHAR(3)"),
        ] {
            run(&mut db, &format!("CREATE FUNCTION {name}(p VARCHAR2) RETURN VARCHAR2 IS v {declaration}; BEGIN v:=p; RETURN '['||v||']/'||LENGTH(v)||'/'||LENGTHC(v)||'/'||LENGTHB(v); END;")).unwrap();
        }
        assert_eq!(
            run(&mut db, "SELECT national_varying('😀') FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "[😀]/2/1/4"
        );
        assert_eq!(
            run(&mut db, "SELECT national_fixed('😀') FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "[😀 ]/3/2/6"
        );
        assert_eq!(
            run(&mut db, "SELECT national_varying('') FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "[]///"
        );
        assert_eq!(
            run(&mut db, "SELECT national_fixed('abcd') FROM DUAL")
                .unwrap_err()
                .sqlstate(),
            "22001"
        );
        drop(db);
        let mut db = BicDb::open(root.path()).unwrap();
        assert_eq!(
            run(&mut db, "SELECT national_fixed('é') FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "[é  ]/3/3/6"
        );
    }
    #[test]
    fn complete_character_lengths_keep_oracle_combining_rules() {
        // Independent Oracle observations distinguish complete characters from
        // scalar counts and extended emoji grapheme clusters.
        for (text, expected) in [
            ("Á", 1),
            ("́", 0),
            ("각", 1),
            ("👩‍👩‍👧‍👦", 7),
            ("🇺🇸", 2),
            ("👍🏽", 2),
            ("✈️", 1),
            ("का", 2),
            ("a⃝", 2),
            ("\r\n", 2),
        ] {
            assert_eq!(
                length(&[
                    SqlValue::String(text.into()),
                    SqlValue::String("complete".into())
                ])
                .unwrap(),
                SqlValue::Int(expected),
                "{text:?}"
            );
        }
    }
    #[test]
    fn national_metadata_is_versioned_and_native_text_stays_native() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        run(&mut db, "CREATE FUNCTION national_signature(p NVARCHAR2) RETURN NVARCHAR2 IS v NVARCHAR2(4);BEGIN v:=p;RETURN v;END;").unwrap();
        let stored = load_routine(&db, RoutineKind::Function, "NATIONAL_SIGNATURE")
            .unwrap()
            .unwrap();
        assert_eq!(stored.language, PREVIEW_ROUTINE_LANGUAGE);
        assert_eq!(stored.return_type, "varchar");
        assert!(stored.return_type_modifier.is_none());
        assert_eq!(stored.arg_types[0].pg_type, "varchar");
        assert!(stored.arg_types[0].type_modifier.is_none());
        SqlSession::new(&mut db).execute("CREATE FUNCTION legacy_national() RETURNS TEXT LANGUAGE bicdb_oracle_preview_v16 AS $$DECLARE v NVARCHAR2(2);BEGIN v:='x';RETURN v||'!';END$$").unwrap();
        assert_eq!(
            SqlSession::new(&mut db)
                .execute("SELECT legacy_national()")
                .unwrap_err()
                .sqlstate(),
            "0A000"
        );
        run(&mut db, "CREATE FUNCTION invalid_national_alias RETURN VARCHAR2 IS v NVARCHAR(2);BEGIN v:='A';RETURN v;END;").unwrap();
        assert_eq!(
            run(&mut db, "SELECT invalid_national_alias() FROM DUAL")
                .unwrap_err()
                .sqlstate(),
            "0A000"
        );
        let native_root = tempfile::tempdir().unwrap();
        let mut native = BicDb::open_with_config(
            native_root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        let mut session = SqlSession::new(&mut native);
        session.execute("CREATE FUNCTION native_character(p TEXT) RETURNS TEXT LANGUAGE plpgsql AS $$DECLARE v VARCHAR(1);BEGIN v:=p;RETURN v;END$$").unwrap();
        assert_eq!(
            session
                .execute("SELECT native_character('😀'),length(native_character('😀'))")
                .unwrap()
                .rows[0],
            vec![SqlValue::String("😀".into()), SqlValue::Int(1)]
        );
    }
    #[test]
    fn national_comparisons_and_dynamic_binds_preserve_logical_types() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        run(&mut db,"CREATE FUNCTION national_equal RETURN NUMBER IS v NCHAR(2):='A';w NVARCHAR2(1):='A';BEGIN IF v='A' THEN IF v=w THEN RETURN 99;END IF;RETURN 1;END IF;RETURN 0;END;").unwrap();
        assert_eq!(
            run(&mut db, "SELECT national_equal() FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "1"
        );
        run(&mut db,"CREATE FUNCTION national_dynamic RETURN NUMBER IS v NVARCHAR2(4):='😀';n NUMBER;BEGIN EXECUTE IMMEDIATE 'SELECT LENGTH(:1) FROM DUAL' INTO n USING v||'x';RETURN n;END;").unwrap();
        assert_eq!(
            run(&mut db, "SELECT national_dynamic() FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "3"
        );
        run(&mut db,"CREATE FUNCTION national_conditional RETURN NUMBER IS v NVARCHAR2(4):='😀';n NUMBER;BEGIN SELECT LENGTH(NVL(v,''))+LENGTH(DECODE(1,1,v,'')) INTO n FROM DUAL;RETURN n;END;").unwrap();
        assert_eq!(
            run(&mut db, "SELECT national_conditional() FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "4"
        );
        run(&mut db,"CREATE FUNCTION national_substring RETURN VARCHAR2 IS v NVARCHAR2(4):='😀';BEGIN RETURN SUBSTR(v,1,1);END;").unwrap();
        assert_eq!(
            run(&mut db, "SELECT national_substring() FROM DUAL")
                .unwrap_err()
                .sqlstate(),
            "0A000"
        );
    }
    #[test]
    fn national_overflow_is_not_silent_truncation() {
        assert_eq!(
            coerce(SqlValue::String("😀".into()), false, Some(1))
                .unwrap_err()
                .sqlstate(),
            "22001"
        );
        assert_eq!(
            coerce(SqlValue::String("A ".into()), false, Some(1))
                .unwrap_err()
                .sqlstate(),
            "22001"
        );
        assert_eq!(
            coerce(SqlValue::String("".into()), true, Some(4)).unwrap(),
            SqlValue::Null
        );
        assert_eq!(
            coerce(SqlValue::String("é".into()), true, Some(2))
                .unwrap()
                .to_cell(),
            "é "
        );
    }
}
