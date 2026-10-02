//! Flashback DDL and FLASHBACK TABLE.
//!
//! * `ALTER TABLE t FLASHBACK ARCHIVE [RETENTION n {SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR}[S]]`
//! * `ALTER TABLE t NO FLASHBACK ARCHIVE`
//! * `FLASHBACK TABLE t [, u ...] TO {SCN | TIMESTAMP} expr`
use super::*;
use crate::engine::flashback::unix_micros_from_timestamp_text;
use bicdb_core::FlashbackPoint;
use sqlparser::keywords::Keyword;

fn is_word(token: &Token, word: &str) -> bool {
    matches!(token, Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(word))
}

fn contains_word_ci(sql: &str, needle: &str) -> bool {
    sql.as_bytes()
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle.as_bytes()))
}

struct ArchiveDdl {
    table: ObjectName,
    enable: bool,
    retention_secs: u64,
}

struct FlashbackTable {
    tables: Vec<ObjectName>,
    scn: bool,
    point: Expr,
}

fn retention_unit_seconds(unit: &str) -> Option<u64> {
    let unit = unit.to_ascii_lowercase();
    let unit = unit.strip_suffix('s').unwrap_or(&unit);
    Some(match unit {
        "second" => 1,
        "minute" => 60,
        "hour" => 3_600,
        "day" => 86_400,
        "week" => 7 * 86_400,
        "month" => 30 * 86_400,
        "year" => 365 * 86_400,
        _ => return None,
    })
}

fn expect_end(parser: &mut Parser<'_>, statement: &str) -> Result<()> {
    let _ = parser.consume_token(&Token::SemiColon);
    if parser.peek_token().token != Token::EOF {
        return Err(SqlError::InvalidSql(format!(
            "unexpected input after flashback statement: {statement}"
        )));
    }
    Ok(())
}

/// `None` when the statement is not flashback archive DDL.
fn parse_archive_ddl(statement: &str) -> Option<Result<ArchiveDdl>> {
    if !contains_word_ci(statement, "flashback") {
        return None;
    }
    let dialect = PostgreSqlDialect {};
    let mut parser = Parser::new(&dialect).try_with_sql(statement).ok()?;
    if !parser.parse_keywords(&[Keyword::ALTER, Keyword::TABLE]) {
        return None;
    }
    let _ = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
    let table = parser.parse_object_name(false).ok()?;
    let enable = !parser.parse_keyword(Keyword::NO);
    if !is_word(&parser.next_token().token, "FLASHBACK")
        || !is_word(&parser.next_token().token, "ARCHIVE")
    {
        return None;
    }
    let mut retention_secs = 0;
    if enable && is_word(&parser.peek_token().token, "RETENTION") {
        parser.next_token();
        let amount = match parser.next_token().token {
            Token::Number(value, _) => value.parse::<u64>().ok(),
            _ => None,
        };
        let unit = match parser.next_token().token {
            Token::Word(word) => retention_unit_seconds(&word.value),
            _ => None,
        };
        let (Some(amount), Some(unit)) = (amount, unit) else {
            return Some(Err(SqlError::InvalidSql(
                "FLASHBACK ARCHIVE RETENTION expects <n> SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR"
                    .to_string(),
            )));
        };
        retention_secs = amount.saturating_mul(unit);
        if retention_secs == 0 {
            return Some(Err(SqlError::InvalidSql(
                "FLASHBACK ARCHIVE RETENTION must be positive".to_string(),
            )));
        }
    } else if enable && matches!(parser.peek_token().token, Token::Word(_)) {
        return Some(Err(SqlError::Unsupported(
            "named flashback archives are not supported; use FLASHBACK ARCHIVE [RETENTION <n> <unit>]"
                .to_string(),
        )));
    }
    Some(expect_end(&mut parser, statement).map(|_| ArchiveDdl {
        table,
        enable,
        retention_secs,
    }))
}

/// `None` when the statement is not FLASHBACK TABLE.
fn parse_flashback_table(statement: &str) -> Option<Result<FlashbackTable>> {
    let dialect = PostgreSqlDialect {};
    let mut parser = Parser::new(&dialect).try_with_sql(statement).ok()?;
    if !is_word(&parser.next_token().token, "FLASHBACK") || !parser.parse_keyword(Keyword::TABLE) {
        return None;
    }
    let parsed = (|| -> Result<FlashbackTable> {
        let mut tables = vec![parser
            .parse_object_name(false)
            .map_err(|error| SqlError::InvalidSql(error.to_string()))?];
        while parser.consume_token(&Token::Comma) {
            tables.push(
                parser
                    .parse_object_name(false)
                    .map_err(|error| SqlError::InvalidSql(error.to_string()))?,
            );
        }
        if !parser.parse_keyword(Keyword::TO) {
            return Err(SqlError::InvalidSql(
                "FLASHBACK TABLE expects TO SCN <expr> or TO TIMESTAMP <expr>".to_string(),
            ));
        }
        let kind = parser.next_token().token;
        let scn = if is_word(&kind, "SCN") {
            true
        } else if is_word(&kind, "TIMESTAMP") {
            false
        } else if is_word(&kind, "BEFORE") {
            return Err(SqlError::Unsupported(
                "FLASHBACK TABLE ... TO BEFORE DROP is not supported yet".to_string(),
            ));
        } else {
            return Err(SqlError::InvalidSql(
                "FLASHBACK TABLE expects TO SCN <expr> or TO TIMESTAMP <expr>".to_string(),
            ));
        };
        let point = parser
            .parse_expr()
            .map_err(|error| SqlError::InvalidSql(error.to_string()))?;
        expect_end(&mut parser, statement)?;
        Ok(FlashbackTable { tables, scn, point })
    })();
    Some(parsed)
}

impl<'db> SqlSession<'db> {
    pub(crate) fn execute_raw_flashback(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let statements = split_sql_statements(sql);
        let [statement] = statements.as_slice() else {
            return Ok(None);
        };
        if let Some(ddl) = parse_archive_ddl(statement) {
            let ddl = ddl?;
            self.execute_flashback_archive_ddl(ddl)?;
            return Ok(Some(SqlResult::command("ALTER TABLE")));
        }
        if let Some(request) = parse_flashback_table(statement) {
            let request = request?;
            self.execute_flashback_table(request)?;
            return Ok(Some(SqlResult::command("FLASHBACK")));
        }
        Ok(None)
    }

    fn execute_flashback_archive_ddl(&mut self, ddl: ArchiveDdl) -> Result<()> {
        if self.in_transaction() {
            return Err(SqlError::Unsupported(
                "ALTER TABLE ... FLASHBACK ARCHIVE cannot run inside a transaction block"
                    .to_string(),
            ));
        }
        let table = resolve_session_relation_name(self.db_ref(), &relation_name(&ddl.table)?)?;
        self.require_table_ownership(&table, "ALTER TABLE")?;
        let db = self.db_mut()?;
        if ddl.enable {
            db.enable_flashback(&table, ddl.retention_secs)?;
        } else {
            db.disable_flashback(&table)?;
        }
        Ok(())
    }

    fn execute_flashback_table(&mut self, request: FlashbackTable) -> Result<()> {
        let point = {
            let engine = self.sql_engine();
            let value = engine.eval_flashback_bound(&request.point)?;
            if matches!(value, SqlValue::Null) {
                return Err(SqlError::InvalidSql(
                    "FLASHBACK TABLE target cannot be NULL".to_string(),
                ));
            }
            if request.scn {
                let scn = sql_value_i64(&value)
                    .and_then(|scn| u64::try_from(scn).ok())
                    .ok_or_else(|| {
                        SqlError::InvalidSql(format!("invalid SCN {}", value.to_cell()))
                    })?;
                FlashbackPoint::Scn(scn)
            } else {
                FlashbackPoint::TimestampMicros(unix_micros_from_timestamp_text(&value.to_cell())?)
            }
        };
        let mut tables = Vec::with_capacity(request.tables.len());
        for name in &request.tables {
            let table = resolve_session_relation_name(self.db_ref(), &relation_name(name)?)?;
            self.check_flashback_table_allowed(&table)?;
            tables.push(table);
        }
        let started_transaction = !self.in_transaction();
        if started_transaction {
            self.execute_inner("BEGIN")?;
        }
        let result = tables
            .iter()
            .try_for_each(|table| self.restore_flashback_table(table, point));
        match (result, started_transaction) {
            (Ok(()), true) => {
                self.execute_inner("COMMIT")?;
                Ok(())
            }
            (Ok(()), false) => Ok(()),
            (Err(error), true) => {
                let _ = self.execute_inner("ROLLBACK");
                Err(error)
            }
            (Err(error), false) => Err(error),
        }
    }

    /// FLASHBACK TABLE writes rows below the SQL statement layer, so it refuses
    /// every table whose writes that layer would otherwise police.
    fn check_flashback_table_allowed(&self, table: &str) -> Result<()> {
        {
            let engine = self.sql_engine();
            for privilege in ["SELECT", "INSERT", "UPDATE", "DELETE"] {
                engine.require_relation_privilege(table, privilege)?;
            }
        }
        let schema = load_schema(self.db_ref(), table)?.ok_or_else(|| {
            SqlError::InvalidSql(format!(
                "FLASHBACK TABLE requires a table, `{table}` is not one"
            ))
        })?;
        if schema.rls_enabled {
            return Err(SqlError::Unsupported(format!(
                "FLASHBACK TABLE is not supported for `{table}` because it has row-level security"
            )));
        }
        let has_foreign_key = |schema: &TableSchema| {
            schema
                .constraints
                .iter()
                .any(|constraint| matches!(constraint, ConstraintSchema::ForeignKey { .. }))
        };
        if has_foreign_key(&schema) {
            return Err(SqlError::Unsupported(format!(
                "FLASHBACK TABLE is not supported for `{table}` because it has foreign keys"
            )));
        }
        for other in list_schemas(self.db_ref())? {
            if other.constraints.iter().any(|constraint| {
                matches!(constraint, ConstraintSchema::ForeignKey { foreign_table, .. } if foreign_table == table)
            }) {
                return Err(SqlError::Unsupported(format!(
                    "FLASHBACK TABLE is not supported for `{table}` because `{}` references it",
                    other.name
                )));
            }
        }
        Ok(())
    }

    fn restore_flashback_table(&mut self, table: &str, point: FlashbackPoint) -> Result<()> {
        let past = match self.security_context.clone() {
            Some(ctx) => self
                .db_ref()
                .flashback_rows_with_context(&ctx, table, point),
            None => self.db_ref().flashback_rows(table, point),
        }?;
        let current = self.sql_engine().scan_records(table)?;
        let same = |a: &Record, b: &Record| {
            a.metadata == b.metadata
                && a.vector == b.vector
                && a.geometry == b.geometry
                && a.payload == b.payload
        };
        let past_by_id = past
            .iter()
            .map(|record| (record.id.as_str(), record))
            .collect::<HashMap<_, _>>();
        let current_by_id = current
            .iter()
            .map(|record| (record.id.as_str(), record.as_ref()))
            .collect::<HashMap<_, _>>();
        let deletes = current
            .iter()
            .filter(|record| !past_by_id.contains_key(record.id.as_str()))
            .map(|record| record.id.clone())
            .collect::<Vec<_>>();
        let upserts = past
            .iter()
            .filter(|record| {
                current_by_id
                    .get(record.id.as_str())
                    .is_none_or(|current| !same(record, current))
            })
            .cloned()
            .collect::<Vec<_>>();
        self.delete_session_records(table, &deletes)?;
        if !upserts.is_empty() {
            self.insert_session_records(table, upserts)?;
        }
        Ok(())
    }
}
