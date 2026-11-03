//! Oracle text-minimum number formats, kept separate from native TO_CHAR.
use super::*;
thread_local! { static ENABLED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
pub(crate) struct Scope(bool);
impl Scope {
    pub(crate) fn new(enabled: bool) -> Self {
        Self(ENABLED.with(|flag| flag.replace(enabled)))
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        ENABLED.with(|flag| flag.set(self.0));
    }
}
pub(crate) fn enabled() -> bool {
    ENABLED.with(std::cell::Cell::get)
}

fn arguments(expr: &Expr) -> Option<Vec<Expr>> {
    let Expr::Function(f) = expr else {
        return None;
    };
    let [part] = f.name.0.as_slice() else {
        return None;
    };
    if !part.as_ident().is_some_and(|name| {
        name.quote_style.is_none() && name.value.eq_ignore_ascii_case("to_char")
    }) || f.filter.is_some()
        || f.over.is_some()
        || !f.within_group.is_empty()
    {
        return None;
    }
    let FunctionArguments::List(args) = &f.args else {
        return None;
    };
    if !(2..=3).contains(&args.args.len())
        || args.duplicate_treatment.is_some()
        || !args.clauses.is_empty()
    {
        return None;
    }
    args.args
        .iter()
        .map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Some(expr.clone()),
            _ => None,
        })
        .collect()
}
fn intercepted(expr: &Expr) -> bool {
    match expr {
        Expr::Nested(inner) => intercepted(inner),
        Expr::Value(value) => match &value.value {
            Value::SingleQuotedString(text) => text.to_ascii_uppercase().contains("TM"),
            Value::Null => true,
            _ => true,
        },
        _ => true,
    }
}
pub(crate) fn input(expr: &Expr) -> Option<&Expr> {
    let Expr::Function(f) = expr else {
        return None;
    };
    let args = arguments(expr)?;
    if !intercepted(&args[1]) {
        return None;
    }
    let FunctionArguments::List(args) = &f.args else {
        return None;
    };
    let FunctionArg::Unnamed(FunctionArgExpr::Expr(input)) = &args.args[0] else {
        return None;
    };
    Some(input)
}
pub(crate) fn lower_expression(expr: &mut Expr, kind: &str) -> Result<()> {
    let Some(args) = arguments(expr) else {
        return Ok(());
    };
    if !intercepted(&args[1]) {
        return Ok(());
    }
    if kind == "unsupported" {
        return Err(unsupported("formatted TO_CHAR requires resolved NUMBER, character or datetime input; use an explicit conversion"));
    }
    let nls = args.get(2).map(render_sql).unwrap_or_else(|| "NULL".into());
    *expr = expression(&format!(
        "CAST(pg_catalog.bicdb_oracle_tm_to_char({}, {}, {nls}, '{kind}', {}) AS TEXT)",
        render_sql(&args[0]),
        render_sql(&args[1]),
        args.len() == 3
    ))?;
    Ok(())
}
fn error(code: i32, state: &'static str, message: &str) -> SqlError {
    errors::database_error(code, state, message)
}
fn decimal_character(value: &str) -> Result<char> {
    let Some((name, value)) = value.split_once('=') else {
        return Err(error(-12702, "22023", "invalid NLS parameter string"));
    };
    if !name.trim().eq_ignore_ascii_case("NLS_NUMERIC_CHARACTERS") {
        return Err(error(-12702, "22023", "invalid NLS parameter string"));
    }
    let Some(value) = value
        .trim()
        .strip_prefix('\'')
        .and_then(|value| value.strip_suffix('\''))
    else {
        return Err(error(-12702, "22023", "invalid NLS numeric characters"));
    };
    let value = value.replace("''", "'");
    let bytes = value.as_bytes();
    if bytes.len() != 2
        || !bytes.is_ascii()
        || bytes[0] == bytes[1]
        || bytes
            .iter()
            .any(|b| b.is_ascii_digit() || b.is_ascii_control() || b"+-<>".contains(b))
    {
        return Err(error(-12702, "22023", "invalid NLS numeric characters"));
    }
    Ok(bytes[0] as char)
}
fn numeric(value: &SqlValue, kind: &str) -> Result<Decimal> {
    let text = value.to_cell();
    if kind == "number" && matches!(text.as_str(), "NaN" | "Infinity" | "-Infinity") {
        return Err(unsupported(
            "text-minimum formatting of non-finite binary values is not qualified",
        ));
    }
    let decimal = parse_decimal(&text).map_err(|err| {
        if err.sqlstate() == "22003" {
            error(-1426, "22003", "numeric overflow")
        } else {
            error(-1722, "22P02", "invalid number")
        }
    })?;
    if !decimal.mantissa.is_zero() {
        let exponent =
            decimal.mantissa.abs().to_string().len() as i64 - 1 - i64::from(decimal.scale);
        if exponent > 125 {
            return Err(error(-1426, "22003", "numeric overflow"));
        }
        if exponent < -130 {
            return Ok(Decimal {
                mantissa: BigInt::from(0),
                scale: 0,
            });
        }
    }
    Ok(decimal)
}
fn minimum(decimal: &Decimal, scientific: bool, separator: char) -> String {
    if decimal.mantissa.is_zero() {
        return if scientific {
            "0E+00".into()
        } else {
            "0".into()
        };
    }
    let negative = decimal.mantissa.is_negative();
    let raw = decimal.mantissa.abs().to_string();
    let exponent = raw.len() as i64 - 1 - i64::from(decimal.scale);
    let mut fixed = decimal.clone().format();
    if fixed.contains('.') {
        fixed = fixed.trim_end_matches('0').trim_end_matches('.').into();
    }
    if fixed.starts_with("0.") {
        fixed.remove(0);
    } else if fixed.starts_with("-0.") {
        fixed.remove(1);
    }
    if !scientific && fixed.len() <= 64 {
        return fixed.replace('.', &separator.to_string());
    }
    let digits = raw.trim_end_matches('0');
    let tail = if digits.len() > 1 {
        format!("{separator}{}", &digits[1..])
    } else {
        String::new()
    };
    format!(
        "{}{}{tail}E{}{:02}",
        if negative { "-" } else { "" },
        &digits[..1],
        if exponent < 0 { "-" } else { "+" },
        exponent.unsigned_abs()
    )
}
pub(crate) fn evaluate(args: &[SqlValue]) -> Result<SqlValue> {
    let [value, format, nls, kind, has_nls] = args else {
        return Err(unsupported(
            "text-minimum TO_CHAR helper requires five arguments",
        ));
    };
    if matches!(value, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let kind = kind.to_cell();
    let decimal = match kind.as_str() {
        "number" | "text" => Some(numeric(value, &kind)?),
        "date" => None,
        _ => return Err(unsupported("unqualified TO_CHAR input kind")),
    };
    let SqlValue::Bool(has_nls) = has_nls else {
        return Err(unsupported("invalid TO_CHAR NLS presence flag"));
    };
    if matches!(format, SqlValue::Null) || *has_nls && matches!(nls, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let model = format.to_cell().to_ascii_uppercase();
    if !model.contains("TM") {
        if *has_nls {
            return Err(unsupported(
                "NLS overrides for other format models are not qualified",
            ));
        }
        return eval_to_char(&[value.clone(), format.clone()]);
    }
    if kind == "date" {
        return Err(error(-1821, "22007", "date format not recognized"));
    }
    let separator = if *has_nls {
        decimal_character(&nls.to_cell())?
    } else {
        '.'
    };
    let model = model.strip_prefix("FM").unwrap_or(&model);
    let scientific = match model {
        "TM" | "TM9" => false,
        "TME" => true,
        _ => return Err(error(-1481, "22023", "invalid number format model")),
    };
    Ok(SqlValue::String(minimum(
        decimal.as_ref().expect("numeric kind"),
        scientific,
        separator,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn text(s: &str) -> SqlValue {
        SqlValue::String(s.into())
    }
    fn call(value: &str, format: &str) -> Result<SqlValue> {
        evaluate(&[
            text(value),
            text(format),
            SqlValue::Null,
            text("number"),
            SqlValue::Bool(false),
        ])
    }
    fn run(db: &mut BicDb, source: &str) -> Result<SqlResult> {
        let sql = lower_session_sql(db, source, ORACLE_PREVIEW_PROFILE, None, "bicdb")?;
        SqlSession::new(db).execute(&sql)
    }
    fn code(error: SqlError) -> i64 {
        let detail: JsonValue = serde_json::from_str(&errors::transport_detail(&error)).unwrap();
        detail["oracle_error_stack"][0]["code"].as_i64().unwrap()
    }
    #[test]
    fn minimum_formats_preserve_precision_and_boundary_notation() {
        for (value, format, wanted) in [
            ("12.500", "TM9", "12.5"),
            (".5", "TM", ".5"),
            ("-.5", "FMTM9", "-.5"),
            ("0", "TME", "0E+00"),
            ("12.500", "tme", "1.25E+01"),
            ("1e64", "TM9", "1E+64"),
            ("-1e63", "TM9", "-1E+63"),
            ("1e-64", "TM9", "1E-64"),
            ("1e-131", "TM9", "0"),
            (
                "12345678901234567890123456789012345678",
                "TM9",
                "12345678901234567890123456789012345678",
            ),
        ] {
            assert_eq!(call(value, format).unwrap(), text(wanted));
        }
        assert_eq!(
            call("1e63", "TM9").unwrap().to_cell(),
            format!("1{}", "0".repeat(63))
        );
        assert_eq!(
            call("1e-63", "TM9").unwrap().to_cell(),
            format!(".{}1", "0".repeat(62))
        );
    }
    #[test]
    fn conversion_null_and_format_errors_have_source_precedence() {
        assert_eq!(
            evaluate(&[
                SqlValue::Null,
                text("TM9E"),
                text("invalid"),
                text("number"),
                SqlValue::Bool(true)
            ])
            .unwrap(),
            SqlValue::Null
        );
        assert_eq!(
            evaluate(&[
                text("12.5"),
                text("TM9"),
                SqlValue::Null,
                text("number"),
                SqlValue::Bool(true)
            ])
            .unwrap(),
            SqlValue::Null
        );
        assert_eq!(
            code(
                evaluate(&[
                    text("bad"),
                    SqlValue::Null,
                    SqlValue::Null,
                    text("text"),
                    SqlValue::Bool(false)
                ])
                .unwrap_err()
            ),
            -1722
        );
        assert_eq!(code(call("1e126", "TM9").unwrap_err()), -1426);
        for format in ["TM9E", " TM9", "TM9 ", "TM99", "TM9FM", "FMFMTM9"] {
            assert_eq!(code(call("12.5", format).unwrap_err()), -1481);
        }
    }
    #[test]
    fn numeric_characters_are_explicit_and_validated() {
        assert_eq!(
            evaluate(&[
                text(".5"),
                text("TM9"),
                text("NLS_NUMERIC_CHARACTERS=',.'"),
                text("number"),
                SqlValue::Bool(true)
            ])
            .unwrap(),
            text(",5")
        );
        assert_eq!(
            evaluate(&[
                text("12.500"),
                text("TME"),
                text("NLS_NUMERIC_CHARACTERS='!?'"),
                text("number"),
                SqlValue::Bool(true)
            ])
            .unwrap(),
            text("1!25E+01")
        );
        for nls in [
            "invalid",
            "NLS_NUMERIC_CHARACTERS='..'",
            "NLS_NUMERIC_CHARACTERS='12'",
            "NLS_NUMERIC_CHARACTERS='+-'",
            "NLS_DATE_LANGUAGE='English'",
        ] {
            assert_eq!(
                code(
                    evaluate(&[
                        text("12.5"),
                        text("TM9"),
                        text(nls),
                        text("number"),
                        SqlValue::Bool(true)
                    ])
                    .unwrap_err()
                ),
                -12702
            );
        }
    }
    #[test]
    fn sql_minimum_formats_and_date_errors_use_typed_input() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let result=run(&mut db,"SELECT TO_CHAR(12.500,'TM9'),TO_CHAR(.5,'TM9','NLS_NUMERIC_CHARACTERS='',.''') FROM DUAL").unwrap();
        assert_eq!(result.rows[0], vec![text("12.5"), text(",5")]);
        assert!(result
            .column_types
            .iter()
            .all(|t| t.as_deref() == Some("text")));
        assert_eq!(
            code(run(&mut db, "SELECT TO_CHAR(DATE '2020-01-01','TM9') FROM DUAL").unwrap_err()),
            -1821
        );
        assert_eq!(
            run(&mut db, "SELECT TO_CHAR(12.5,'FM99.9') FROM DUAL")
                .unwrap()
                .rows[0][0],
            text("12.5")
        );
    }
    #[test]
    fn old_compilers_and_native_routines_keep_their_formatter() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let source="CREATE FUNCTION minimum_value RETURN VARCHAR2 IS BEGIN RETURN TO_CHAR(12.5,'TM9');END;";
        let legacy = lower_session_sql(&db, source, ORACLE_PREVIEW_PROFILE, None, "bicdb")
            .unwrap()
            .replace(PREVIEW_ROUTINE_LANGUAGE, PREVIEW_ROUTINE_LANGUAGE_V23);
        SqlSession::new(&mut db).execute(&legacy).unwrap();
        assert!(run(&mut db, "SELECT minimum_value() FROM DUAL").is_err());
        run(&mut db, &source.replace("minimum_value", "minimum_current")).unwrap();
        assert_eq!(
            run(&mut db, "SELECT minimum_current() FROM DUAL")
                .unwrap()
                .rows[0][0],
            text("12.5")
        );
        SqlSession::new(&mut db).execute("CREATE FUNCTION native_minimum() RETURNS text LANGUAGE plpgsql AS $$BEGIN RETURN to_char(12.5,'TM9');END;$$").unwrap();
        run(&mut db,"CREATE FUNCTION calls_native RETURN VARCHAR2 IS BEGIN RETURN \"public\".native_minimum();END;").unwrap();
        assert!(run(&mut db, "SELECT calls_native() FROM DUAL").is_err());
    }
    #[test]
    fn qualified_user_calls_and_native_sql_remain_separate() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        assert!(SqlSession::new(&mut db)
            .execute("SELECT to_char(12.5,'TM9')")
            .is_err());
        SqlSession::new(&mut db)
            .execute("CREATE SCHEMA \"AUTHOR\"")
            .unwrap();
        SqlSession::new(&mut db).execute("CREATE FUNCTION \"AUTHOR\".to_char(n numeric,f text) RETURNS text LANGUAGE sql AS $$SELECT 'authored'$$").unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        assert_eq!(
            run(&mut db, "SELECT author.to_char(12.5,'TM9') FROM DUAL")
                .unwrap()
                .rows[0][0],
            text("authored")
        );
    }
}
