//! Preview current-time expressions, distinct from native transaction time.
use super::*;

const TIMESTAMP: &str = "bicdb_oracle_current_timestamp_v26";
const LOCAL: &str = "bicdb_oracle_localtimestamp_v26";
thread_local! { static CLOCK: std::cell::Cell<Option<i64>> = const { std::cell::Cell::new(None) }; }

pub(crate) struct StatementScope {
    previous: Option<i64>,
    installed: bool,
}
impl StatementScope {
    pub(crate) fn new() -> Self {
        Self {
            previous: CLOCK.with(|clock| clock.replace(Some(unix_now_micros()))),
            installed: true,
        }
    }
    pub(crate) fn if_absent() -> Self {
        if CLOCK.with(|clock| clock.get().is_none()) {
            Self::new()
        } else {
            Self {
                previous: None,
                installed: false,
            }
        }
    }
}
impl Drop for StatementScope {
    fn drop(&mut self) {
        if self.installed {
            CLOCK.with(|clock| clock.set(self.previous));
        }
    }
}

pub(crate) fn lower(source: &[Token]) -> Result<Option<(String, usize)>> {
    let Some(Token::Word(first)) = source.first() else {
        return Ok(None);
    };
    if first.quote_style.is_some() {
        return Ok(None);
    }
    let name = first.value.to_ascii_uppercase();
    if !matches!(
        name.as_str(),
        "CURRENT_TIMESTAMP" | "LOCALTIMESTAMP" | "CURRENT_DATE"
    ) {
        return Ok(None);
    }
    let mut consumed = 1;
    let mut precision = 6;
    if source.get(1) == Some(&Token::LParen) {
        if name == "CURRENT_DATE" {
            return Err(unsupported("CURRENT_DATE has no precision argument"));
        }
        let Some(Token::Number(value, false)) = source.get(2) else {
            return Err(unsupported(
                "current-time precision must be a literal integer",
            ));
        };
        if source.get(3) != Some(&Token::RParen) || !value.bytes().all(|b| b.is_ascii_digit()) {
            return Err(unsupported(
                "current-time precision must be a literal integer",
            ));
        }
        precision = value
            .parse::<u8>()
            .ok()
            .filter(|p| *p <= 6)
            .ok_or_else(|| {
                unsupported(
                    "current-time precision above six requires nanosecond transport qualification",
                )
            })?;
        consumed = 4;
    }
    let sql = match name.as_str() {
        "CURRENT_TIMESTAMP" => {
            format!("CAST({TIMESTAMP}() AS TIMESTAMP({precision}) WITH TIME ZONE)")
        }
        "LOCALTIMESTAMP" => format!("CAST({LOCAL}() AS TIMESTAMP({precision}))"),
        _ => format!("bicdb_oracle_date_coerce(CAST(date_trunc('second',{LOCAL}()) AS TIMESTAMP))"),
    };
    Ok(Some((sql, consumed)))
}

/// A clock keyword can also be an identifier. In particular, Oracle accepts
/// both explicit and implicit projection aliases with these names. Preserve
/// identifier positions before replacing a keyword with an expression.
pub(super) fn identifier_position(source: &[Token], index: usize) -> bool {
    if !source.get(index).is_some_and(|token| {
        ["CURRENT_TIMESTAMP", "LOCALTIMESTAMP", "CURRENT_DATE"]
            .iter()
            .any(|name| word(token, name))
    }) {
        return false;
    }
    if source.get(index + 1) == Some(&Token::Period) {
        return true;
    }
    let Some(previous) = index.checked_sub(1).and_then(|i| source.get(i)) else {
        return false;
    };
    if matches!(previous, Token::LParen | Token::Comma) && in_named_identifier_list(source, index) {
        return true;
    }
    match previous {
        Token::Period
        | Token::Number(_, _)
        | Token::SingleQuotedString(_)
        | Token::Placeholder(_)
        | Token::RParen => true,
        Token::Word(word) => {
            word.quote_style.is_some()
                || word.keyword == Keyword::NoKeyword
                || [
                    "AS",
                    "FROM",
                    "JOIN",
                    "INTO",
                    "UPDATE",
                    "TABLE",
                    "VIEW",
                    "SCHEMA",
                    "SEQUENCE",
                    "FUNCTION",
                    "PROCEDURE",
                    "COLUMN",
                    "ADD",
                    "CONSTRAINT",
                    "NULL",
                    "TRUE",
                    "FALSE",
                    "END",
                    "CURRENT_TIMESTAMP",
                    "LOCALTIMESTAMP",
                    "CURRENT_DATE",
                ]
                .iter()
                .any(|name| word.value.eq_ignore_ascii_case(name))
        }
        _ => false,
    }
}

fn in_named_identifier_list(source: &[Token], index: usize) -> bool {
    // Column declarations, insert targets, routine parameters and constraint
    // column lists contain identifiers even when spelled like clock keywords.
    // Nested DEFAULT/CHECK expressions are outside this list level.
    for start in 0..index {
        let direct = word(&source[start], "KEY") || word(&source[start], "UNIQUE");
        let named = [
            "TABLE",
            "INTO",
            "REFERENCES",
            "VIEW",
            "FUNCTION",
            "PROCEDURE",
        ]
        .iter()
        .any(|name| word(&source[start], name));
        if !direct && !named {
            continue;
        }
        if direct && !source[..start].iter().any(|token| word(token, "TABLE")) {
            continue;
        }
        let mut open = start + 1;
        if named {
            if !matches!(source.get(open), Some(Token::Word(_))) {
                continue;
            }
            open += 1;
            while source.get(open) == Some(&Token::Period)
                && matches!(source.get(open + 1), Some(Token::Word(_)))
            {
                open += 2;
            }
        }
        if source.get(open) != Some(&Token::LParen) || open >= index {
            continue;
        }
        let mut depth = 0;
        for token in &source[open..index] {
            match token {
                Token::LParen => depth += 1,
                Token::RParen => depth -= 1,
                _ => {}
            }
            if depth == 0 {
                break;
            }
        }
        if depth == 1 {
            return true;
        }
    }
    false
}

pub(crate) fn pg_type(name: &str) -> Option<&'static str> {
    match name {
        TIMESTAMP => Some("timestamptz"),
        LOCAL => Some("timestamp"),
        _ => None,
    }
}

pub(crate) fn evaluate(db: &BicDb, name: &str, args: &[SqlValue]) -> Result<SqlValue> {
    if !preview_routine_language_enabled(db)? || !args.is_empty() {
        return Err(unsupported(
            "current-time helper requires the preview profile and no arguments",
        ));
    }
    let micros = CLOCK
        .with(|clock| clock.get())
        .unwrap_or_else(unix_now_micros);
    let timestamp = pg_timestamp_from_unix_micros(micros);
    if name == TIMESTAMP {
        return Ok(SqlValue::String(render_timestamptz(timestamp)));
    }
    let offset = timezone_offset_at_timestamp(&current_timezone_name(), timestamp)
        .ok_or_else(|| unsupported("invalid session time zone"))?;
    let local = timestamp.finite_micros().unwrap() + i64::from(offset) * 1_000_000;
    Ok(SqlValue::String(
        PgTimestamp::Finite(local).to_iso_text(false),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(session: &mut SqlSession<'_>, sql: &str) -> SqlResult {
        let sql = lower_profile_sql(session.db_ref(), sql, ORACLE_PREVIEW_PROFILE).unwrap();
        session.execute(&sql).unwrap()
    }

    #[test]
    fn fixed_statement_clock_preserves_zone_rounding_and_date_time_of_day() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let _context = SqlTemporalScope::new(0, "Asia/Kolkata", "postgres");
        let _clock = StatementScope::new();
        CLOCK.with(|clock| clock.set(Some(1_709_251_199_765_432))); // 2024-02-29 23:59:59.765432 UTC
        let sql = lower_profile_sql(
            &db,
            "SELECT CURRENT_TIMESTAMP,CURRENT_TIMESTAMP(0),LOCALTIMESTAMP,CURRENT_DATE FROM DUAL",
            ORACLE_PREVIEW_PROFILE,
        )
        .unwrap();
        let result = SqlEngine::new(&db).execute(&sql).unwrap();
        let values: Vec<_> = result.rows[0].iter().map(SqlValue::to_cell).collect();
        assert_eq!(
            values,
            vec![
                "2024-03-01 05:29:59.765432+05:30",
                "2024-03-01 05:30:00+05:30",
                "2024-03-01 05:29:59.765432",
                "2024-03-01 05:29:59"
            ]
        );
        let types = infer_query_result_types(&db, &sql).unwrap().unwrap();
        assert_eq!(
            types,
            vec![
                Some("timestamptz".into()),
                Some("timestamptz".into()),
                Some("timestamp".into()),
                Some("timestamp".into())
            ]
        );
        // The native transaction timestamp is still the separately supplied
        // epoch; installing an Oracle statement clock does not replace it.
        assert!(transaction_timestamp_string().starts_with("1970-01-01"));
    }

    #[test]
    fn preview_statement_time_advances_in_one_transaction_and_native_time_stays_stable() {
        for preview in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut db = BicDb::open(dir.path()).unwrap();
            ensure_database_profile(
                &mut db,
                if preview {
                    ORACLE_PREVIEW_PROFILE
                } else {
                    POSTGRES_PROFILE
                },
            )
            .unwrap();
            let mut session = SqlSession::new(&mut db);
            session.execute("BEGIN").unwrap();
            let sql = "SELECT CURRENT_TIMESTAMP,CURRENT_TIMESTAMP";
            let before = if preview {
                run(&mut session, sql)
            } else {
                session.execute(sql).unwrap()
            };
            assert_eq!(
                before.column_types,
                vec![Some("timestamptz".into()), Some("timestamptz".into())],
                "preview={preview}"
            );
            assert!(!matches!(before.rows[0][0], SqlValue::Null));
            assert_eq!(before.rows[0][0], before.rows[0][1]);
            std::thread::sleep(std::time::Duration::from_millis(30));
            let after = if preview {
                run(&mut session, sql)
            } else {
                session.execute(sql).unwrap()
            };
            assert_eq!(after.rows[0][0], after.rows[0][1]);
            assert_eq!(before.rows[0][0] == after.rows[0][0], !preview);
            session.execute("ROLLBACK").unwrap();
        }
    }

    #[test]
    fn current_time_keywords_preserve_quoted_columns_literals_and_new_routine_fence() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        run(
            &mut session,
            "CREATE TABLE time_names(\"CURRENT_TIMESTAMP\" VARCHAR2(20))",
        );
        run(
            &mut session,
            "INSERT INTO time_names VALUES('stored value')",
        );
        assert_eq!(
            run(&mut session, "SELECT t.current_timestamp FROM time_names t").rows[0][0].to_cell(),
            "stored value"
        );
        assert_eq!(
            run(&mut session, "SELECT 'CURRENT_TIMESTAMP' FROM DUAL").rows[0][0].to_cell(),
            "CURRENT_TIMESTAMP"
        );
        for name in ["current_timestamp", "localtimestamp", "current_date"] {
            for expression in ["1", "(1)", "'value'"] {
                for separator in [" AS ", " "] {
                    let result = run(
                        &mut session,
                        &format!("SELECT {expression}{separator}{name} FROM DUAL"),
                    );
                    assert_eq!(result.columns, vec![name.to_ascii_uppercase()]);
                    assert!(!matches!(result.rows[0][0], SqlValue::Null));
                }
            }
        }
        run(&mut session,"CREATE TABLE keyword_columns(current_timestamp NUMBER,localtimestamp NUMBER,current_date NUMBER,PRIMARY KEY(current_timestamp))");
        run(&mut session,"INSERT INTO keyword_columns(current_timestamp,localtimestamp,current_date) VALUES(1,2,3)");
        let result = run(
            &mut session,
            "SELECT t.current_timestamp,t.localtimestamp,t.current_date FROM keyword_columns t",
        );
        assert_eq!(
            result.rows[0]
                .iter()
                .map(SqlValue::to_cell)
                .collect::<Vec<_>>(),
            vec!["1", "2", "3"]
        );
        run(&mut session,"CREATE FUNCTION current_time_fn RETURN TIMESTAMP WITH TIME ZONE IS BEGIN RETURN CURRENT_TIMESTAMP; END;");
        assert!(!matches!(
            run(&mut session, "SELECT current_time_fn() FROM DUAL").rows[0][0],
            SqlValue::Null
        ));
        assert_eq!(
            preview_routine_language_version(PREVIEW_ROUTINE_LANGUAGE),
            Some(28)
        );
        for sql in [
            "SELECT CURRENT_TIMESTAMP(9) FROM DUAL",
            "SELECT LOCALTIMESTAMP(-1) FROM DUAL",
            "SELECT CURRENT_DATE(2) FROM DUAL",
        ] {
            assert!(
                lower_profile_sql(session.db_ref(), sql, ORACLE_PREVIEW_PROFILE).is_err(),
                "{sql}"
            );
        }
    }
}
