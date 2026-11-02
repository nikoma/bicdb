//! Versioned routine concatenation under the preview's fixed default numeric
//! characters and English DD-MON-RR DATE format. PostgreSQL operators are untouched.
use super::*;

fn number_text(value: &SqlValue, width: usize) -> Result<String> {
    let decimal = parse_decimal(&value.to_cell())?;
    if decimal.mantissa.is_zero() {
        return Ok("0".into());
    }
    let negative = decimal.mantissa.is_negative();
    let mut digits = decimal.mantissa.abs().to_string();
    let mut exponent = i32::try_from(digits.len())
        .map_err(|_| SqlError::numeric_value_out_of_range("Oracle NUMBER is out of range"))?
        - 1
        - decimal.scale as i32;
    if !(-130..=125).contains(&exponent) {
        return Err(SqlError::Unsupported(
            "implicit NUMBER concatenation outside Oracle's finite exponent range is not qualified"
                .into(),
        ));
    }
    let mut fixed = decimal.format();
    if fixed.contains('.') {
        fixed = fixed
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string();
    }
    if fixed.starts_with("0.") {
        fixed.remove(0);
    }
    if fixed.starts_with("-0.") {
        fixed.remove(1);
    }
    // Oracle's default implicit NUMBER conversion uses a context-specific
    // field. Scientific notation preserves that width, including sign/exponent.
    if fixed.len() <= width {
        return Ok(fixed);
    }
    loop {
        let exponent_width = exponent.unsigned_abs().to_string().len().max(2);
        let fractional = width - usize::from(negative) - 4 - exponent_width;
        let significant = fractional + 1;
        if digits.len() > significant {
            let rest = digits.as_bytes()[significant];
            let mut mantissa: BigInt = digits[..significant].parse().expect("decimal digits");
            if rest >= b'5' {
                mantissa += 1;
            }
            let rounded = mantissa.to_string();
            if rounded.len() > significant {
                exponent += 1;
                digits = rounded;
                continue;
            }
            digits = rounded;
        }
        digits.extend(std::iter::repeat_n(
            '0',
            significant.saturating_sub(digits.len()),
        ));
        return Ok(format!(
            "{}{}.{}E{}{:02}",
            if negative { "-" } else { "" },
            &digits[..1],
            &digits[1..],
            if exponent < 0 { "-" } else { "+" },
            exponent.unsigned_abs()
        ));
    }
}

fn operand(value: &SqlValue, kind: &str, width: usize) -> Result<String> {
    if matches!(value, SqlValue::Null) {
        return Ok(String::new());
    }
    match kind {
        "text" => match value {
            SqlValue::String(value) => Ok(value.clone()),
            _ => Err(SqlError::Unsupported(
                "character concatenation requires a character value".into(),
            )),
        },
        "number" => number_text(value, width),
        "date" => {
            let date = oracle_date_coerce(value.clone())?;
            let stamp = PgTimestamp::from_postgres_text(&date.to_cell(), false)
                .map_err(|error| postgres_timestamp_input_error(&date.to_cell(), error))?;
            let (year, month, day, ..) = stamp.components().expect("finite Oracle DATE");
            if year < 1 {
                return Err(SqlError::Unsupported(
                    "implicit BC DATE concatenation is not qualified".into(),
                ));
            }
            let months = [
                "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
            ];
            Ok(format!(
                "{day:02}-{}-{:02}",
                months[month as usize - 1],
                year % 100
            ))
        }
        _ => Err(SqlError::Unsupported(
            "unrecognized Oracle concatenation operand type".into(),
        )),
    }
}

pub(super) fn evaluate(args: &[SqlValue]) -> Result<SqlValue> {
    arity("bicdb_oracle_concat", args, 5..=5)?;
    let context = args[4].to_cell();
    let width = if context == "plsql" { 100 } else { 40 };
    let mut text = operand(&args[0], &args[2].to_cell(), width)?;
    text.push_str(&operand(&args[1], &args[3].to_cell(), width)?);
    let limit = match context.as_str() {
        "plsql" => 32767,
        "sql" => 4000,
        _ => {
            return Err(SqlError::Unsupported(
                "unknown Oracle concatenation context".into(),
            ))
        }
    };
    if text.len() > limit {
        return Err(SqlError::data_exception(
            "22001",
            "Oracle concatenation result exceeds the preview context's byte limit",
            Some("text".into()),
        ));
    }
    Ok(if text.is_empty() {
        SqlValue::Null
    } else {
        SqlValue::String(text)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oracle_function_character_limits_follow_sql_and_procedural_callers() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        oracle::ensure_database_profile(&mut db, oracle::ORACLE_PREVIEW_PROFILE).unwrap();
        for source in [
            "CREATE FUNCTION concat_large RETURN VARCHAR2 IS BEGIN RETURN RPAD('x',3000,'x')||RPAD('y',2000,'y'); END;",
            "CREATE FUNCTION concat_large_len RETURN NUMBER IS BEGIN RETURN LENGTH(concat_large()); END;",
            "CREATE FUNCTION concat_sql_len RETURN NUMBER IS result NUMBER; BEGIN SELECT LENGTH(concat_large()) INTO result FROM DUAL; RETURN result; END;",
            "CREATE FUNCTION concat_recover RETURN NUMBER IS result NUMBER; BEGIN BEGIN SELECT LENGTH(concat_large()) INTO result FROM DUAL; EXCEPTION WHEN OTHERS THEN result:=0; END; RETURN LENGTH(concat_large())+result; END;",
        ] {
            let sql=oracle::lower_profile_sql(&db,source,oracle::ORACLE_PREVIEW_PROFILE).unwrap();
            SqlSession::new(&mut db).execute(&sql).unwrap();
        }
        let mut session = SqlSession::new(&mut db);
        for _ in 0..2 {
            assert_eq!(
                session
                    .execute("SELECT concat_large()")
                    .unwrap_err()
                    .sqlstate(),
                "22001"
            );
            assert_eq!(
                session.execute("SELECT concat_large_len()").unwrap().rows[0][0].to_cell(),
                "5000"
            );
            assert_eq!(
                session
                    .execute("SELECT concat_sql_len()")
                    .unwrap_err()
                    .sqlstate(),
                "22001"
            );
            assert_eq!(
                session.execute("SELECT concat_recover()").unwrap().rows[0][0].to_cell(),
                "5000"
            );
        }
        for language in ["plpgsql", oracle::PREVIEW_ROUTINE_LANGUAGE_V6] {
            session.execute(&format!("CREATE OR REPLACE FUNCTION legacy_large() RETURNS VARCHAR LANGUAGE {language} AS $$BEGIN RETURN repeat('x',5000); END$$")).unwrap();
            assert_eq!(
                session.execute("SELECT legacy_large()").unwrap().rows[0][0]
                    .to_cell()
                    .len(),
                5000
            );
        }
    }

    #[test]
    fn versioned_routine_concatenation_preserves_native_and_legacy_null_rules() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        oracle::ensure_database_profile(&mut db, oracle::ORACLE_PREVIEW_PROFILE).unwrap();
        for source in [
            "CREATE FUNCTION concat_text(p_left VARCHAR2,p_right VARCHAR2) RETURN VARCHAR2 IS BEGIN RETURN p_left||p_right; END;",
            "CREATE FUNCTION concat_number(p_value NUMBER) RETURN VARCHAR2 IS BEGIN RETURN '['||p_value||']'; END;",
            "CREATE FUNCTION concat_date(p_value DATE) RETURN VARCHAR2 IS BEGIN RETURN '['||p_value||']'; END;",
        ] {
            let sql=oracle::lower_profile_sql(&db,source,oracle::ORACLE_PREVIEW_PROFILE).unwrap();
            SqlSession::new(&mut db).execute(&sql).unwrap();
        }
        let mut session = SqlSession::new(&mut db);
        for (sql, expected) in [
            ("SELECT concat_text(NULL,'x')", Some("x")),
            ("SELECT concat_text('x',NULL)", Some("x")),
            ("SELECT concat_text(NULL,NULL)", None),
            ("SELECT concat_number(CAST(0.5 AS NUMERIC))", Some("[.5]")),
            ("SELECT concat_number(CAST(-0.5 AS NUMERIC))", Some("[-.5]")),
            (
                "SELECT concat_date(TIMESTAMP '2024-02-29 12:34:56')",
                Some("[29-FEB-24]"),
            ),
        ] {
            assert_eq!(
                session.execute(sql).unwrap().rows[0][0],
                expected
                    .map(|value| SqlValue::String(value.into()))
                    .unwrap_or(SqlValue::Null),
                "{sql}"
            );
        }
        for language in ["plpgsql", oracle::PREVIEW_ROUTINE_LANGUAGE_V6] {
            session.execute(&format!("CREATE OR REPLACE FUNCTION legacy_concat(p_left TEXT,p_right TEXT) RETURNS TEXT LANGUAGE {language} AS $$BEGIN RETURN p_left||p_right; END$$")).unwrap();
            assert_eq!(
                session
                    .execute("SELECT legacy_concat(NULL,'x')")
                    .unwrap()
                    .rows[0][0],
                SqlValue::Null
            );
        }
        assert_eq!(
            session.execute("SELECT 'x'||NULL").unwrap().rows[0][0],
            SqlValue::Null
        );
    }

    #[test]
    fn implicit_number_conversion_preserves_exact_default_format_and_scientific_width() {
        for (input, expected) in [
            ("12.500", "12.5"),
            ("0.5", ".5"),
            ("-0.5", "-.5"),
            ("-0.000", "0"),
            ("1e-8", ".00000001"),
            (
                "123456789012345678901234567890",
                "123456789012345678901234567890",
            ),
            ("1e39", "1000000000000000000000000000000000000000"),
            ("-1e39", "-1.000000000000000000000000000000000E+39"),
            ("1e40", "1.0000000000000000000000000000000000E+40"),
            ("1e-100", "1.000000000000000000000000000000000E-100"),
        ] {
            assert_eq!(
                number_text(&SqlValue::String(input.into()), 40).unwrap(),
                expected,
                "{input}"
            );
        }
        assert_eq!(
            evaluate(&[
                SqlValue::Null,
                SqlValue::String("x  ".into()),
                SqlValue::String("text".into()),
                SqlValue::String("text".into()),
                SqlValue::String("plsql".into())
            ])
            .unwrap(),
            SqlValue::String("x  ".into())
        );
    }
}
