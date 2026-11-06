//! Explicit preview text rules; native PostgreSQL operators remain independent.
use super::*;

/// Preserve ESCAPE only where the expression parser identifies a LIKE suffix.
/// The same nonreserved word still folds as an ordinary Oracle identifier in
/// declarations, projections, pattern operands and qualified column names.
pub(super) fn escape_positions(source: &[Token]) -> BTreeSet<usize> {
    use sqlparser::dialect::{Dialect, Precedence};
    let mut positions = BTreeSet::new();
    if !source.iter().any(|token| word(token, "ESCAPE")) {
        return positions;
    }
    let dialect = OracleDialect {};
    for (index, token) in source.iter().enumerate() {
        if !word(token, "LIKE") || (index > 0 && source[index - 1] == Token::Period) {
            continue;
        }
        let mut parser = Parser::new(&dialect).with_tokens(source[index + 1..].to_vec());
        if parser
            .parse_subexpr(dialect.prec_value(Precedence::Like))
            .is_ok()
        {
            // sqlparser reports the last consumed token, not the next one.
            let suffix = index + 2 + parser.get_current_index();
            if source
                .get(suffix)
                .is_some_and(|token| word(token, "ESCAPE"))
            {
                positions.insert(suffix);
            }
        }
    }
    positions
}

pub(crate) fn is_pattern_function(function: &sqlparser::ast::Function) -> bool {
    matches!(function.name.0.as_slice(), [part] if part.as_ident().is_some_and(|name|
        name.quote_style.is_none() && matches!(name.value.to_ascii_lowercase().as_str(), "regexp_substr" | "regexp_replace")))
}

pub(crate) fn convert_text_arguments(
    expr: &mut Expr,
    mut convert: impl FnMut(&Expr) -> Result<Expr>,
) -> Result<()> {
    match expr {
        Expr::Trim {
            expr, trim_what, ..
        } => {
            **expr = convert(expr)?;
            if let Some(character) = trim_what {
                **character = convert(character)?;
            }
        }
        Expr::Function(function) => {
            let Ok(name) = object_name_lowercase(&function.name) else {
                return Ok(());
            };
            let indices: &[usize] = match name.as_ref() {
                "bicdb_oracle_trim" | "pg_catalog.bicdb_oracle_trim" => &[0, 1],
                "bicdb_oracle_like" | "pg_catalog.bicdb_oracle_like" => &[0, 1, 2],
                "bicdb_oracle_regexp_substr" | "pg_catalog.bicdb_oracle_regexp_substr" => {
                    &[0, 1, 4]
                }
                "bicdb_oracle_regexp_replace" | "pg_catalog.bicdb_oracle_regexp_replace" => {
                    &[0, 1, 2, 5]
                }
                "regexp_substr" if is_pattern_function(function) => &[0, 1, 4],
                "regexp_replace" if is_pattern_function(function) => &[0, 1, 2, 5],
                _ => return Ok(()),
            };
            if let FunctionArguments::List(arguments) = &mut function.args {
                for (index, argument) in arguments.args.iter_mut().enumerate() {
                    if indices.contains(&index) {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(value)) = argument {
                            *value = convert(value)?;
                        }
                    }
                }
            }
        }
        _ => {}
    }
    Ok(())
}

pub(crate) fn lower_expression(expr: &mut Expr) -> Result<()> {
    match expr {
        Expr::Like {
            expr: source,
            pattern,
            escape_char,
            negated,
            any,
            ..
        } => {
            if *any {
                return Err(unsupported(
                    "Oracle LIKE ANY requires a separate quantified-pattern contract",
                ));
            }
            // Preserve whether ESCAPE was specified. Runtime validation is
            // required because extended-query binds are not literals during
            // profile lowering. Two arguments mean no implicit escape.
            let escape = escape_char
                .as_ref()
                .map_or(String::new(), |value| format!(", {value}"));
            *expr = expression(&format!(
                "{}CAST(pg_catalog.bicdb_oracle_like({}, {}{}) AS BOOLEAN)",
                if *negated { "NOT " } else { "" },
                render_sql(source.as_ref()),
                render_sql(pattern.as_ref()),
                escape,
            ))?;
        }

        Expr::Trim {
            expr: source,
            trim_where,
            trim_what,
            trim_characters,
        } => {
            if trim_characters.is_some() {
                return Err(unsupported("Oracle TRIM requires the FROM grammar"));
            }
            let mode = trim_where
                .as_ref()
                .map_or("BOTH".into(), ToString::to_string);
            let character = trim_what
                .as_ref()
                .map_or("' '".into(), |value| render_sql(value.as_ref()));
            *expr = expression(&format!(
                "CAST(pg_catalog.bicdb_oracle_trim({}, {character}, '{mode}') AS VARCHAR)",
                render_sql(source.as_ref())
            ))?;
        }
        Expr::Function(function) => {
            let [part] = function.name.0.as_slice() else {
                return Ok(());
            };
            let Some(name) = part.as_ident().filter(|name| name.quote_style.is_none()) else {
                return Ok(());
            };
            let helper = match name.value.to_ascii_lowercase().as_str() {
                "regexp_substr" => "bicdb_oracle_regexp_substr",
                "regexp_replace" => "bicdb_oracle_regexp_replace",
                _ => return Ok(()),
            };
            if function.filter.is_some() || function.over.is_some() {
                return Err(unsupported(
                    "regular-expression functions require scalar calls",
                ));
            }
            let FunctionArguments::List(arguments) = &function.args else {
                return Err(unsupported(
                    "regular-expression functions require positional arguments",
                ));
            };
            if arguments.duplicate_treatment.is_some()
                || !arguments.clauses.is_empty()
                || arguments.args.iter().any(|argument| {
                    !matches!(argument, FunctionArg::Unnamed(FunctionArgExpr::Expr(_)))
                })
            {
                return Err(unsupported(
                    "regular-expression functions require positional scalar arguments",
                ));
            }
            function.name = ObjectName::from(vec![Ident::new("pg_catalog"), Ident::new(helper)]);
            *expr = expression(&format!("CAST({} AS VARCHAR)", render_sql(expr)))?;
        }
        _ => {}
    }
    Ok(())
}

/// Binary, case-sensitive preview LIKE. Validate ESCAPE before NULL operands;
/// the PostgreSQL evaluator retains its independent escape/strictness rules.
pub(crate) fn like(args: &[SqlValue]) -> Result<SqlValue> {
    if !(2..=3).contains(&args.len()) {
        return Err(unsupported(
            "internal LIKE helper requires two or three arguments",
        ));
    }
    let escape = if let Some(value) = args.get(2) {
        let text = value.to_cell();
        // The STANDARD AL32UTF8/BINARY reference rejects a multibyte
        // database-character escape. National-character inputs are declined
        // by typed lowering until their distinct UTF16 contract is provided.
        if matches!(value, SqlValue::Null) || text.len() != 1 {
            return Err(errors::database_error(
                -1425,
                "22025",
                "escape character must be character string of length 1",
            ));
        }
        text.chars().next()
    } else {
        None
    };
    if matches!(args[0], SqlValue::Null) || matches!(args[1], SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let source = args[0].to_cell();
    let pattern = args[1].to_cell();
    if source.is_empty() || pattern.is_empty() {
        return Ok(SqlValue::Null);
    }
    if let Some(escape) = escape {
        let mut chars = pattern.chars();
        while let Some(character) = chars.next() {
            if character == escape
                && !matches!(chars.next(), Some(next) if next == escape || next == '%' || next == '_')
            {
                return Err(errors::database_error(
                    -1424,
                    "22025",
                    "missing or illegal character following the escape character",
                ));
            }
        }
    }
    Ok(SqlValue::Bool(like_match_chars(
        &source.chars().collect::<Vec<_>>(),
        &pattern.chars().collect::<Vec<_>>(),
        escape,
    )))
}

pub(crate) fn trim(args: &[SqlValue]) -> Result<SqlValue> {
    let [source, character, mode] = args else {
        return Err(unsupported("internal TRIM helper requires three arguments"));
    };
    if matches!(source, SqlValue::Null) || matches!(character, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let source = source.to_cell();
    let character = character.to_cell();
    // The preview folds empty character strings to NULL. Check NULL input
    // before validating a multi-character trim expression, as Oracle does.
    if source.is_empty() || character.is_empty() {
        return Ok(SqlValue::Null);
    }
    let mut chars = character.chars();
    let character = chars.next().expect("nonempty trim character");
    if chars.next().is_some() {
        return Err(errors::database_error(
            -30001,
            "22023",
            "trim set should have only one character",
        ));
    }
    let value = match mode.to_cell().as_str() {
        "BOTH" => source.trim_matches(character),
        "LEADING" => source.trim_start_matches(character),
        "TRAILING" => source.trim_end_matches(character),
        _ => return Err(SqlError::invalid_parameter_value("invalid TRIM direction")),
    };
    Ok(if value.is_empty() {
        SqlValue::Null
    } else {
        SqlValue::String(value.into())
    })
}

fn integer(value: Option<&SqlValue>, default: i64, minimum: i64) -> Result<Option<i64>> {
    let Some(value) = value else {
        return Ok(Some(default));
    };
    if matches!(value, SqlValue::Null) {
        return Ok(None);
    }
    let number = parse_decimal(&value.to_cell())?;
    let number = (number.mantissa / pow10_bigint(number.scale)).to_i64();
    number
        .filter(|value| *value >= minimum)
        .map(Some)
        .ok_or_else(|| {
            errors::database_error(
                -1428,
                "22023",
                "regular-expression argument is out of range",
            )
        })
}

fn compile(pattern: &SqlValue, flags: Option<&SqlValue>) -> Result<Option<regex::Regex>> {
    let mut builder = regex::RegexBuilder::new(&pattern.to_cell());
    // The preview uses binary, case-sensitive matching by default. NLS-aware
    // linguistic comparison requires a separate collation contract.
    builder.size_limit(8 * 1024 * 1024);
    if let Some(flags) = flags.filter(|value| !matches!(value, SqlValue::Null)) {
        for flag in flags.to_cell().chars() {
            match flag {
                'i' => {
                    builder.case_insensitive(true);
                }
                'c' => {
                    builder.case_insensitive(false);
                }
                'm' => {
                    builder.multi_line(true);
                }
                'n' => {
                    builder.dot_matches_new_line(true);
                }
                'x' => {
                    builder.ignore_whitespace(true);
                }
                _ => {
                    return Err(errors::database_error(
                        -1760,
                        "22023",
                        "invalid regular-expression match parameter",
                    ))
                }
            }
        }
    }
    if matches!(pattern, SqlValue::Null) || pattern.to_cell().is_empty() {
        return Ok(None);
    }
    if pattern.to_cell().len() > 512 {
        return Err(errors::database_error(
            -12733,
            "2201B",
            "regular expression too long",
        ));
    }
    validate_dialect(&pattern.to_cell())?;
    builder.build().map(Some).map_err(|error| match error {
        regex::Error::CompiledTooBig(_) => SqlError::data_exception(
            "54000",
            "regular-expression compilation exceeds platform limits",
            None,
        ),
        regex::Error::Syntax(message) if message.contains("unclosed group") => {
            errors::database_error(
                -12725,
                "2201B",
                "unmatched parentheses in regular expression",
            )
        }
        regex::Error::Syntax(message) if message.contains("unclosed character class") => {
            errors::database_error(-12726, "2201B", "unmatched bracket in regular expression")
        }
        _ => unsupported("regular-expression syntax is outside the preview execution contract"),
    })
}

fn validate_dialect(pattern: &str) -> Result<()> {
    let unsupported = || {
        super::unsupported("regular-expression dialect construct is outside the preview contract")
    };
    let mut chars = pattern.chars().peekable();
    let mut class = false;
    let mut first_in_class = false;
    while let Some(character) = chars.next() {
        match character {
            '\\' => {
                if let Some(escaped) = chars.next() {
                    // Oracle does not give every Rust escape the same meaning:
                    // e.g. \b is a literal b, not a word-boundary assertion.
                    if escaped.is_alphanumeric()
                        && (class || !matches!(escaped, 'd' | 'D' | 'w' | 'W' | 's' | 'S' | 'A'))
                    {
                        return Err(unsupported());
                    }
                }
                first_in_class = false;
            }
            '(' if !class && chars.peek() == Some(&'?') => return Err(unsupported()),
            '[' if class => {
                if chars.peek() != Some(&':') {
                    return Err(unsupported());
                }
                // Preserve a POSIX class inside the surrounding bracket list.
                while let Some(character) = chars.next() {
                    if character == ':' && chars.peek() == Some(&']') {
                        chars.next();
                        break;
                    }
                }
                first_in_class = false;
            }
            '[' => {
                class = true;
                first_in_class = true;
            }
            ']' if class && !first_in_class => {
                class = false;
            }
            '^' if class && first_in_class => {}
            '&' | '-' | '~' if class && chars.peek() == Some(&character) => {
                return Err(unsupported())
            }
            _ => {
                first_in_class = false;
            }
        }
    }
    Ok(())
}

fn empty_to_null(value: String) -> SqlValue {
    if value.is_empty() {
        SqlValue::Null
    } else {
        SqlValue::String(value)
    }
}

fn start_offset(source: &str, position: i64) -> Option<usize> {
    let index = usize::try_from(position - 1).ok()?;
    source.char_indices().map(|(offset, _)| offset).nth(index)
}

fn captures<'a>(
    regex: &'a regex::Regex,
    source: &'a str,
    start: usize,
) -> impl Iterator<Item = regex::Captures<'a>> + 'a {
    let mut position = Some(start);
    std::iter::from_fn(move || {
        let capture = regex.captures_at(source, position?)?;
        let value = capture.get(0).expect("complete regex capture");
        // Search the complete input so anchors retain their original meaning.
        // Oracle counts an empty match immediately after a nonempty match.
        // Only an empty match advances by a character to ensure progress.
        position = if value.start() == value.end() {
            source[value.end()..]
                .chars()
                .next()
                .map(|ch| value.end() + ch.len_utf8())
        } else {
            Some(value.end())
        };
        Some(capture)
    })
}

pub(crate) fn regexp_substr(args: &[SqlValue]) -> Result<SqlValue> {
    if !(2..=6).contains(&args.len()) {
        return Err(unsupported(
            "REGEXP_SUBSTR requires two through six arguments",
        ));
    }
    let position = integer(args.get(2), 1, 1)?;
    let occurrence = integer(args.get(3), 1, 1)?;
    let group = integer(args.get(5), 0, 0)?;
    let regex = compile(&args[1], args.get(4))?;
    let (Some(position), Some(occurrence), Some(group), Some(regex)) =
        (position, occurrence, group, regex)
    else {
        return Ok(SqlValue::Null);
    };
    if matches!(args[0], SqlValue::Null) || group > 9 {
        return Ok(SqlValue::Null);
    }
    let source = args[0].to_cell();
    let Some(offset) = start_offset(&source, position) else {
        return Ok(SqlValue::Null);
    };
    let occurrence = usize::try_from(occurrence - 1).unwrap_or(usize::MAX);
    let result = captures(&regex, &source, offset)
        .nth(occurrence)
        .and_then(|capture| capture.get(group as usize))
        .map_or(SqlValue::Null, |value| empty_to_null(value.as_str().into()));
    Ok(result)
}

const MAX_REPLACEMENT_BYTES: usize = 8 * 1024 * 1024;

fn append(output: &mut String, value: &str) -> Result<()> {
    if output
        .len()
        .checked_add(value.len())
        .is_none_or(|length| length > MAX_REPLACEMENT_BYTES)
    {
        return Err(SqlError::data_exception(
            "54000",
            "regular-expression result exceeds platform byte limit",
            None,
        ));
    }
    output.try_reserve(value.len()).map_err(|_| {
        SqlError::data_exception("54000", "regular-expression result allocation failed", None)
    })?;
    output.push_str(value);
    Ok(())
}

fn replacement(output: &mut String, template: &str, capture: &regex::Captures<'_>) -> Result<()> {
    let mut chars = template.chars();
    while let Some(character) = chars.next() {
        if character == '\\' {
            if let Some(next) = chars.next() {
                if ('1'..='9').contains(&next) {
                    let group = next as usize - '0' as usize;
                    if group >= capture.len() {
                        append(output, "\\")?;
                        append(output, next.encode_utf8(&mut [0; 4]))?;
                    } else if let Some(value) = capture.get(group) {
                        append(output, value.as_str())?;
                    }
                } else {
                    append(output, "\\")?;
                    if next != '\\' {
                        append(output, next.encode_utf8(&mut [0; 4]))?;
                    }
                }
                continue;
            }
        }
        append(output, character.encode_utf8(&mut [0; 4]))?;
    }
    Ok(())
}

pub(crate) fn regexp_replace(args: &[SqlValue]) -> Result<SqlValue> {
    if !(2..=6).contains(&args.len()) {
        return Err(unsupported(
            "REGEXP_REPLACE requires two through six arguments",
        ));
    }
    let position = integer(args.get(3), 1, 1)?;
    let occurrence = integer(args.get(4), 0, 0)?;
    let regex = compile(&args[1], args.get(5))?;
    let (Some(position), Some(occurrence)) = (position, occurrence) else {
        return Ok(SqlValue::Null);
    };
    if matches!(args[0], SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let source = args[0].to_cell();
    let Some(regex) = regex else {
        return Ok(empty_to_null(source));
    };
    let Some(offset) = start_offset(&source, position) else {
        return Ok(empty_to_null(source));
    };
    let template = args
        .get(2)
        .filter(|value| !matches!(value, SqlValue::Null))
        .map(SqlValue::to_cell)
        .unwrap_or_default();
    let mut output = String::new();
    let mut previous = 0;
    for (index, capture) in captures(&regex, &source, offset).enumerate() {
        if occurrence != 0 && index as u64 != (occurrence - 1) as u64 {
            continue;
        }
        let value = capture.get(0).expect("complete regex capture");
        append(&mut output, &source[previous..value.start()])?;
        replacement(&mut output, &template, &capture)?;
        previous = value.end();
        if occurrence != 0 {
            break;
        }
    }
    append(&mut output, &source[previous..])?;
    Ok(empty_to_null(output))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oracle_code(error: SqlError) -> i64 {
        let detail: JsonValue = serde_json::from_str(&errors::transport_detail(&error)).unwrap();
        detail["oracle_error_stack"][0]["code"]
            .as_i64()
            .expect("Oracle error number")
    }

    fn run(db: &mut BicDb, sql: &str) -> Result<SqlResult> {
        let lowered = lower_profile_sql(db, sql, ORACLE_PREVIEW_PROFILE)?;
        SqlSession::new(db).execute(&lowered)
    }

    #[test]
    fn oracle_trim_uses_one_character_and_native_trim_keeps_character_sets() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        for (source, expected) in [
            ("TRIM(' a ')", "a"),
            ("TRIM('😀' FROM '😀a😀')", "a"),
            ("TRIM(LEADING 'x' FROM 'xxax')", "ax"),
            ("TRIM(TRAILING 'x' FROM 'xaxx')", "xa"),
        ] {
            assert_eq!(
                run(&mut db, &format!("SELECT {source} FROM DUAL"))
                    .unwrap()
                    .rows[0][0],
                SqlValue::String(expected.into())
            );
        }
        assert_eq!(
            oracle_code(run(&mut db, "SELECT TRIM('ab' FROM 'ababa') FROM DUAL").unwrap_err()),
            -30001
        );
        assert_eq!(
            run(
                &mut db,
                "SELECT TRIM('ab' FROM CAST(NULL AS VARCHAR2(8))) FROM DUAL"
            )
            .unwrap()
            .rows[0][0],
            SqlValue::Null
        );
        assert_eq!(
            SqlSession::new(&mut db)
                .execute("SELECT TRIM('ab' FROM 'ababa')")
                .unwrap()
                .rows[0][0],
            SqlValue::String(String::new())
        );
    }

    #[test]
    fn oracle_regex_defaults_captures_and_unicode_offsets_are_explicit() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        for (source, expected) in [
            ("REGEXP_REPLACE('a1b2c3','[0-9]','x')", "axbxcx"),
            ("REGEXP_REPLACE('a,b,c','[^,]*')", ",,"),
            ("REGEXP_REPLACE('abcabc','a','x',3)", "abcxbc"),
            (
                "REGEXP_REPLACE('abc123','([a-z]+)([0-9]+)','\\2-\\1')",
                "123-abc",
            ),
            ("REGEXP_SUBSTR('é😀x','.',2,1)", "😀"),
            ("REGEXP_SUBSTR('abc','.',1.9,1.9)", "a"),
            ("REGEXP_REPLACE('ab','a*','x')", "xxbx"),
            ("REGEXP_REPLACE('😀é','x*','-')", "-😀-é-"),
            ("REGEXP_REPLACE('abc','^','x',2)", "abc"),
            ("REGEXP_REPLACE('abc','$','x',4)", "abc"),
            ("REGEXP_SUBSTR('abc','..',2)", "bc"),
            ("TRIM(0.5)", ".5"),
            ("REGEXP_SUBSTR(0.5,'.')", "."),
            ("REGEXP_REPLACE('a','a',0.5)", ".5"),
            ("REGEXP_REPLACE('a','a',CHR(92)||'1x')", "\\1x"),
            ("REGEXP_SUBSTR('a,b','(\".*?\"|.*?)(,|$)',1,2,NULL,1)", "b"),
        ] {
            assert_eq!(
                run(&mut db, &format!("SELECT {source} FROM DUAL"))
                    .unwrap()
                    .rows[0][0],
                SqlValue::String(expected.into()),
                "{source}"
            );
        }
        assert_eq!(
            SqlSession::new(&mut db)
                .execute("SELECT regexp_replace('a1b2c3','[0-9]','x')")
                .unwrap()
                .rows[0][0],
            SqlValue::String("axb2c3".into())
        );
    }

    #[test]
    fn oracle_regex_null_options_and_argument_errors_do_not_use_strict_null_dispatch() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        for source in [
            "REGEXP_SUBSTR('abc','.',1,NULL)",
            "REGEXP_SUBSTR('abc','(a)',1,1,NULL,2)",
            "REGEXP_SUBSTR('abc','.',1,1,NULL,NULL)",
            "REGEXP_REPLACE('abc','a','x',1,NULL)",
        ] {
            assert_eq!(
                run(&mut db, &format!("SELECT {source} FROM DUAL"))
                    .unwrap()
                    .rows[0][0],
                SqlValue::Null,
                "{source}"
            );
        }
        for (source, expected) in [
            ("REGEXP_SUBSTR('abc','.',0)", -1428),
            ("REGEXP_SUBSTR(CAST(NULL AS VARCHAR2(8)),'.',0)", -1428),
            ("REGEXP_SUBSTR(CAST(NULL AS VARCHAR2(8)),'(')", -12725),
            ("REGEXP_SUBSTR('abc','[')", -12726),
            ("REGEXP_SUBSTR('abc','.',1,1,'z')", -1760),
        ] {
            assert_eq!(
                oracle_code(run(&mut db, &format!("SELECT {source} FROM DUAL")).unwrap_err()),
                expected,
                "{source}"
            );
        }
    }

    #[test]
    fn new_text_rules_are_fenced_by_the_saved_routine_language() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        run(&mut db, "CREATE FUNCTION new_regex RETURN VARCHAR2 IS v VARCHAR2(8):='a1b2';BEGIN RETURN REGEXP_REPLACE(v,'[0-9]','x');END;").unwrap();
        assert_eq!(
            run(&mut db, "SELECT new_regex() FROM DUAL").unwrap().rows[0][0],
            SqlValue::String("axbx".into())
        );
        let sql = lower_profile_sql(&db,"CREATE FUNCTION old_regex RETURN VARCHAR2 IS BEGIN RETURN regexp_replace('a1b2','[0-9]','x');END;", ORACLE_PREVIEW_PROFILE).unwrap().replace(PREVIEW_ROUTINE_LANGUAGE, PREVIEW_ROUTINE_LANGUAGE_V19);
        SqlSession::new(&mut db).execute(&sql).unwrap();
        assert_eq!(
            run(&mut db, "SELECT old_regex() FROM DUAL").unwrap().rows[0][0],
            SqlValue::String("axb2".into())
        );
        let sql = lower_profile_sql(&db,"CREATE FUNCTION old_query_regex RETURN VARCHAR2 IS v VARCHAR2(8);BEGIN SELECT regexp_replace('a1b2','[0-9]','x') INTO v FROM DUAL;RETURN v;END;", ORACLE_PREVIEW_PROFILE).unwrap().replace(PREVIEW_ROUTINE_LANGUAGE, PREVIEW_ROUTINE_LANGUAGE_V19);
        SqlSession::new(&mut db).execute(&sql).unwrap();
        assert_eq!(
            run(&mut db, "SELECT old_query_regex() FROM DUAL")
                .unwrap()
                .rows[0][0],
            SqlValue::String("axb2".into())
        );
    }

    #[test]
    fn national_text_is_refused_without_rewriting_qualified_or_quoted_functions() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        for sql in [
            "SELECT TRIM(N'😀' FROM N'😀a😀') FROM DUAL",
            "SELECT REGEXP_SUBSTR(N'é😀x','.',3) FROM DUAL",
        ] {
            assert_eq!(run(&mut db, sql).unwrap_err().sqlstate(), "0A000");
        }
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        run(&mut db, "CREATE FUNCTION national_regex RETURN NVARCHAR2 IS v NVARCHAR2(8):='é😀x';BEGIN RETURN REGEXP_SUBSTR(v,'.',3);END;").unwrap();
        assert_eq!(
            run(&mut db, "SELECT national_regex() FROM DUAL")
                .unwrap_err()
                .sqlstate(),
            "0A000"
        );
        for source in [
            "app.regexp_replace('x','x','y')",
            "\"regexp_replace\"('x','x','y')",
        ] {
            let mut expr = expression(source).unwrap();
            let original = expr.clone();
            lower_expression(&mut expr).unwrap();
            assert_eq!(expr, original);
        }
    }

    #[test]
    fn replacement_expansion_is_bounded_before_growing_the_result() {
        let value = regexp_replace(&[
            SqlValue::String("aaaaaaaaa".into()),
            SqlValue::String("a".into()),
            SqlValue::String("x".repeat(1024 * 1024)),
        ])
        .unwrap_err();
        assert_eq!(value.sqlstate(), "54000");
        assert_eq!(
            regexp_replace(&[
                SqlValue::String("a".into()),
                SqlValue::String("a".into()),
                SqlValue::String("\\1x".into()),
            ])
            .unwrap(),
            SqlValue::String("\\1x".into())
        );
        for pattern in ["(?i)a", "[a&&b]", "\\b", "\\p{Greek}"] {
            assert_eq!(
                regexp_substr(&[
                    SqlValue::String("abc".into()),
                    SqlValue::String(pattern.into())
                ])
                .unwrap_err()
                .sqlstate(),
                "0A000",
                "{pattern}"
            );
        }
    }

    #[test]
    fn oracle_schema_qualified_drop_uses_known_language_identity() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        run(&mut db, "CREATE SCHEMA \"UpperOwner\"").unwrap();
        run(&mut db, "CREATE SCHEMA \"OtherOwner\"").unwrap();
        run(
            &mut db,
            "CREATE FUNCTION \"UpperOwner\".\"Fn\" RETURN NUMBER IS BEGIN RETURN 7;END;",
        )
        .unwrap();
        assert!(run(&mut db, "DROP FUNCTION \"OtherOwner\".\"Fn\"").is_err());
        assert_eq!(
            run(&mut db, "SELECT \"UpperOwner\".\"Fn\"() FROM DUAL")
                .unwrap()
                .rows[0][0],
            SqlValue::String("7".into())
        );
        run(&mut db, "DROP FUNCTION \"UpperOwner\".\"Fn\"").unwrap();
        assert!(load_routine(&db, RoutineKind::Function, "Fn")
            .unwrap()
            .is_none());
    }

    #[test]
    fn native_schema_qualified_drop_retains_its_own_identity_checks() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        for sql in [
            "CREATE SCHEMA red",
            "CREATE SCHEMA blue",
            "CREATE FUNCTION red.native_fn() RETURNS INTEGER LANGUAGE sql AS $$SELECT 7$$",
        ] {
            SqlSession::new(&mut db).execute(sql).unwrap();
        }
        assert!(SqlSession::new(&mut db)
            .execute("DROP FUNCTION blue.native_fn")
            .is_err());
        assert_eq!(
            SqlSession::new(&mut db)
                .execute("SELECT red.native_fn()")
                .unwrap()
                .rows[0][0],
            SqlValue::Int(7)
        );
        SqlSession::new(&mut db)
            .execute("DROP FUNCTION red.native_fn")
            .unwrap();
        assert!(load_routine(&db, RoutineKind::Function, "native_fn")
            .unwrap()
            .is_none());
    }

    #[test]
    fn capability_checks_admit_only_registered_compiler_versions() {
        assert_eq!(
            preview_routine_language_version(PREVIEW_ROUTINE_LANGUAGE_V19),
            Some(19)
        );
        assert_eq!(
            preview_routine_language_version(PREVIEW_ROUTINE_LANGUAGE),
            Some(29)
        );
        assert_eq!(
            preview_routine_language_version(PREVIEW_ROUTINE_LANGUAGE_V25),
            Some(25)
        );
        assert_eq!(
            preview_routine_language_version(PREVIEW_ROUTINE_LANGUAGE_V24),
            Some(24)
        );
        for unknown in [
            "bicdb_oracle_preview_v30",
            "bicdb_oracle_preview_v200",
            "bicdb_oracle_preview_v0",
        ] {
            assert_eq!(preview_routine_language_version(unknown), None);
            assert!(!typed_preview_routine_language(unknown));
        }
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        assert!(SqlSession::new(&mut db).execute("CREATE FUNCTION future_compiler() RETURNS INTEGER LANGUAGE bicdb_oracle_preview_v30 AS $$BEGIN RETURN 7;END;$$").is_err());
    }
}

#[cfg(test)]
mod escape_grammar_tests {
    use super::*;

    #[test]
    fn escape_suffixes_are_contextual_and_keep_identifier_operands() {
        for profile in [ORACLE_HR_PROFILE, ORACLE_PREVIEW_PROFILE] {
            let root = tempfile::tempdir().unwrap();
            let mut db = BicDb::open(root.path()).unwrap();
            ensure_database_profile(&mut db, profile).unwrap();
            let mut session = SqlSession::new(&mut db);
            for statement in [
                "CREATE TABLE escape_words(escape VARCHAR2(30), label VARCHAR2(30))",
                "INSERT INTO escape_words VALUES('I|_%','I_name')",
                "INSERT INTO escape_words VALUES('T|_%','Text')",
            ] {
                let lowered =
                    lower_session_sql(session.db_ref(), statement, profile, None, "bicdb").unwrap();
                session.execute(&lowered).unwrap();
            }
            for (query, expected) in [
                ("SELECT label FROM escape_words WHERE label LIKE 'I|_%' ESCAPE '|'", "I_name"),
                ("SELECT label FROM escape_words WHERE label NOT LIKE 'I|_%' ESCAPE '|'", "Text"),
                ("SELECT label FROM escape_words WHERE label LIKE escape ESCAPE '|'", "I_name"),
                ("SELECT label FROM escape_words t WHERE t.label LIKE t.escape ESCAPE '|'", "I_name"),
                ("SELECT label FROM escape_words WHERE label LIKE (SUBSTR('I',1,1)||'|_%') ESCAPE '|'", "I_name"),
                ("SELECT escape FROM escape_words WHERE label='I_name'", "I|_%"),
            ] {
                let lowered = lower_session_sql(session.db_ref(), query, profile, None, "bicdb").unwrap();
                assert_eq!(session.execute(&lowered).unwrap().rows, vec![vec![SqlValue::String(expected.into())]], "{query}: {lowered}");
            }
        }
    }
}
