//! Query rewrite rules (`CREATE RULE`), for the unconditional `DO INSTEAD`
//! form: `DO INSTEAD NOTHING`, or `DO INSTEAD` one or more statements that run
//! once per affected row with `NEW` (INSERT) bound to the row's values.
//! Conditional (`WHERE`) and `DO ALSO` rules are rejected at creation.
use crate::*;
use std::sync::Arc;

pub(crate) const RULE_COLLECTION: &str = "__bicdb_pg_rules";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct RuleSchema {
    pub(crate) name: String,
    pub(crate) table: String,
    pub(crate) event: String,
    /// Empty for `DO INSTEAD NOTHING`.
    pub(crate) actions: Vec<String>,
    pub(crate) definition: String,
}

fn rule_key(table: &str, name: &str) -> String {
    format!("{table}:{name}")
}

thread_local! {
    static RULES: RefCell<Option<(u64, u64, Arc<Vec<RuleSchema>>)>> = const { RefCell::new(None) };
}

pub(crate) fn list_rules(db: &BicDb) -> Result<Arc<Vec<RuleSchema>>> {
    let database_id = db.instance_id();
    let generation = db.collection_generation(RULE_COLLECTION);
    if let Some(list) = RULES.with(|cache| {
        cache.borrow().as_ref().and_then(|(id, cached, list)| {
            (*id == database_id && *cached == generation).then(|| list.clone())
        })
    }) {
        return Ok(list);
    }
    let records = match db.scan_collection(RULE_COLLECTION) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => Vec::new(),
        Err(error) => return Err(error.into()),
    };
    let list = Arc::new(
        records
            .into_iter()
            .map(|record| serde_json::from_value(record.metadata).map_err(SqlError::from))
            .collect::<Result<Vec<RuleSchema>>>()?,
    );
    RULES.with(|cache| *cache.borrow_mut() = Some((database_id, generation, list.clone())));
    Ok(list)
}

pub(crate) fn is_rule_command(sql: &str) -> Result<bool> {
    Ok(parse_create(sql, None)?.is_some()
        || strip_prefix_ci(trim_sql_statement(sql), "DROP RULE ").is_some())
}

fn load_rule(db: &BicDb, key: &str) -> Result<Option<RuleSchema>> {
    match db.get(RULE_COLLECTION, key) {
        Ok(Some(record)) => Ok(Some(serde_json::from_value(record.metadata)?)),
        Ok(None) | Err(BicDbError::CollectionNotFound(_)) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn restore_rule(db: &mut BicDb, key: &str, previous: Option<RuleSchema>) -> Result<()> {
    if let Some(previous) = previous {
        db.create_collection(RULE_COLLECTION)?;
        db.insert(
            RULE_COLLECTION,
            Record::new(key).with_metadata(serde_json::to_value(previous)?),
        )?;
    } else {
        match db.delete(RULE_COLLECTION, key) {
            Ok(_) | Err(BicDbError::CollectionNotFound(_)) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

/// `CREATE [OR REPLACE] RULE name AS ON event TO table DO INSTEAD action`.
fn parse_create(sql: &str, db: Option<&BicDb>) -> Result<Option<(RuleSchema, bool)>> {
    let trimmed = trim_sql_statement(sql);
    let Some(mut rest) = keyword(trimmed, "CREATE") else {
        return Ok(None);
    };
    let or_replace = match keyword(rest, "OR").and_then(|after| keyword(after, "REPLACE")) {
        Some(after) => {
            rest = after;
            true
        }
        None => false,
    };
    let Some(rest) = keyword(rest, "RULE") else {
        return Ok(None);
    };
    let (name, rest) = parse_leading_sql_identifier(rest)?;
    let invalid =
        || SqlError::InvalidSql("CREATE RULE requires AS ON event TO table DO action".into());
    let rest = keyword(rest, "AS").ok_or_else(invalid)?;
    let rest = keyword(rest, "ON").ok_or_else(invalid)?;
    let rest = rest.trim_start();
    let end = rest.find(char::is_whitespace).ok_or_else(invalid)?;
    let event = rest[..end].to_ascii_lowercase();
    if !matches!(event.as_str(), "insert" | "update" | "delete") {
        return Err(SqlError::Unsupported(format!(
            "rules ON {event} are not supported"
        )));
    }
    let rest = keyword(&rest[end..], "TO").ok_or_else(invalid)?;
    let do_index = find_top_level_keyword(rest, "DO").ok_or_else(invalid)?;
    let target = rest[..do_index].trim();
    if find_top_level_keyword(target, "WHERE").is_some() {
        return Err(SqlError::Unsupported(
            "conditional rules are not supported".into(),
        ));
    }
    let mut object = Parser::new(&PostgreSqlDialect {})
        .try_with_sql(target)
        .and_then(|mut parser| parser.parse_object_name(false))
        .map_err(|error| SqlError::InvalidSql(error.to_string()))?;
    if let Some(db) = db {
        crate::search_path::bind_raw_relation(db, &mut object);
    }
    let table = relation_name(&object)?;
    let action = rest[do_index + 2..].trim_start();
    let Some(action) = keyword(action, "INSTEAD") else {
        return Err(SqlError::Unsupported(
            "only DO INSTEAD rules are supported".into(),
        ));
    };
    let action = action.trim();
    let actions = if action.eq_ignore_ascii_case("NOTHING") {
        Vec::new()
    } else {
        let body = action
            .strip_prefix('(')
            .and_then(|inner| inner.strip_suffix(')'))
            .unwrap_or(action);
        let statements = parse_statements(body)?;
        if statements.is_empty() {
            return Err(invalid());
        }
        // Keep the action as written: re-rendering parsed SQL is not a
        // faithful round trip for every literal.
        vec![body.trim().to_string()]
    };
    Ok(Some((
        RuleSchema {
            name: normalize_object_name(&name),
            table,
            event,
            actions,
            definition: trimmed.to_string(),
        },
        or_replace,
    )))
}

fn value_expr(value: &SqlValue) -> Expr {
    let value = match value {
        SqlValue::Null => Value::Null,
        SqlValue::Bool(value) => Value::Boolean(*value),
        SqlValue::Int(value) => Value::Number(value.to_string(), false),
        SqlValue::Float(value) => Value::Number(value.to_string(), false),
        other => Value::SingleQuotedString(other.to_cell()),
    };
    Expr::Value(ValueWithSpan {
        value,
        span: sqlparser::tokenizer::Span::empty(),
    })
}

/// Binds `NEW.column` references of a rule action to one row's values.
fn bind_new_row(statement: &mut Statement, columns: &[String], row: &[SqlValue]) {
    let _ = sqlparser::ast::visit_expressions_mut(statement, |expr| {
        if let Expr::CompoundIdentifier(parts) = expr {
            if let [qualifier, column] = parts.as_slice() {
                if qualifier.value.eq_ignore_ascii_case("new") {
                    let column = ident_value(column);
                    let value = columns
                        .iter()
                        .position(|name| name.eq_ignore_ascii_case(&column))
                        .and_then(|index| row.get(index))
                        .unwrap_or(&SqlValue::Null);
                    *expr = value_expr(value);
                }
            }
        }
        ControlFlow::<()>::Continue(())
    });
}

impl SqlSession<'_> {
    fn rules_unavailable(&self) -> Result<bool> {
        Ok(self
            .session_gucs
            .get("bicdb.sql_profile")
            .is_some_and(|profile| oracle::is_oracle_profile(profile))
            || oracle::preview_routine_language_enabled(self.db_ref())?)
    }

    pub(crate) fn execute_raw_create_rule(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        // Rules are a PostgreSQL feature; the Oracle profile keeps rejecting them.
        if self.rules_unavailable()? {
            return Ok(None);
        }
        let Some((rule, or_replace)) = parse_create(sql, Some(self.db_ref()))? else {
            return Ok(None);
        };
        if load_schema(self.db_ref(), &rule.table)?.is_none() {
            return Err(SqlError::InvalidCollection(rule.table));
        }
        self.require_table_ownership(&rule.table, "create a rule on it")?;
        let key = rule_key(&rule.table, &rule.name);
        let previous = load_rule(self.db_ref(), &key)?;
        if !or_replace && previous.is_some() {
            return Err(SqlError::InvalidSql(format!(
                "rule \"{}\" for relation already exists",
                rule.name
            )));
        }
        let db = self.db_mut()?;
        db.create_collection(RULE_COLLECTION)?;
        db.insert(
            RULE_COLLECTION,
            Record::new(key.clone()).with_metadata(serde_json::to_value(&rule)?),
        )?;
        if self.tx.is_some() {
            self.ddl_undo
                .push(crate::session::DdlUndo::RestoreRule { key, previous });
        }
        Ok(Some(SqlResult::command("CREATE RULE")))
    }

    pub(crate) fn execute_raw_drop_rule(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        // Rules are a PostgreSQL feature; the Oracle profile keeps rejecting them.
        if self.rules_unavailable()? {
            return Ok(None);
        }
        let trimmed = trim_sql_statement(sql);
        let Some(rest) = strip_prefix_ci(trimmed, "DROP RULE ") else {
            return Ok(None);
        };
        let (if_exists, rest) = match strip_prefix_ci(rest.trim_start(), "IF EXISTS ") {
            Some(rest) => (true, rest),
            None => (false, rest),
        };
        let (name, rest) = parse_leading_sql_identifier(rest)?;
        let rest = strip_prefix_ci(rest.trim_start(), "ON ")
            .ok_or_else(|| SqlError::InvalidSql("DROP RULE requires ON table".into()))?;
        let target = rest.trim();
        let target = strip_suffix_ci(target, " CASCADE")
            .or_else(|| strip_suffix_ci(target, " RESTRICT"))
            .unwrap_or(target);
        let mut object = Parser::new(&PostgreSqlDialect {})
            .try_with_sql(target)
            .and_then(|mut parser| parser.parse_object_name(false))
            .map_err(|error| SqlError::InvalidSql(error.to_string()))?;
        crate::search_path::bind_raw_relation(self.db_ref(), &mut object);
        let table = relation_name(&object)?;
        self.require_table_ownership(&table, "drop its rule")?;
        let key = rule_key(&table, &normalize_object_name(&name));
        let previous = load_rule(self.db_ref(), &key)?;
        if previous.is_none() && !if_exists {
            return Err(SqlError::InvalidSql(format!(
                "rule \"{name}\" does not exist"
            )));
        }
        if previous.is_some() {
            self.db_mut()?.delete(RULE_COLLECTION, &key)?;
            if self.tx.is_some() {
                self.ddl_undo
                    .push(crate::session::DdlUndo::RestoreRule { key, previous });
            }
        }
        Ok(Some(SqlResult::command("DROP RULE")))
    }

    /// Runs a DO INSTEAD rule of the statement's target in place of it.
    pub(crate) fn apply_instead_rule(
        &mut self,
        statement: &Statement,
    ) -> Result<Option<SqlResult>> {
        let (table, event) = match statement {
            Statement::Insert(insert) => match &insert.table {
                TableObject::TableName(name) => (name, "insert"),
                _ => return Ok(None),
            },
            Statement::Update(update) => match &update.table.relation {
                TableFactor::Table { name, .. } => (name, "update"),
                _ => return Ok(None),
            },
            Statement::Delete(delete) => match &delete.from {
                FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => {
                    match tables.as_slice() {
                        [TableWithJoins {
                            relation: TableFactor::Table { name, .. },
                            ..
                        }] => (name, "delete"),
                        _ => return Ok(None),
                    }
                }
            },
            _ => return Ok(None),
        };
        let rules = list_rules(self.db_ref())?;
        if rules.is_empty() {
            return Ok(None);
        }
        let table = relation_name(table)?;
        let Some(rule) = rules
            .iter()
            .find(|rule| rule.table == table && rule.event == event)
            .cloned()
        else {
            return Ok(None);
        };
        let tag = |count: usize| match event {
            "insert" => format!("INSERT 0 {count}"),
            "update" => format!("UPDATE {count}"),
            _ => format!("DELETE {count}"),
        };
        if rule.actions.is_empty() {
            return Ok(Some(SqlResult::command(tag(0))));
        }
        let Statement::Insert(insert) = statement else {
            return Err(SqlError::Unsupported(format!(
                "DO INSTEAD rule actions for {event} are not supported"
            )));
        };
        self.with_statement_transaction(|session| session.execute_insert_rule(insert, &rule))
            .map(Some)
    }

    fn execute_insert_rule(
        &mut self,
        insert: &sqlparser::ast::Insert,
        rule: &RuleSchema,
    ) -> Result<SqlResult> {
        let table = &rule.table;
        let source = insert
            .source
            .as_ref()
            .ok_or_else(|| SqlError::Unsupported("INSERT without rows under a rule".into()))?;
        let schema = load_schema(self.db_ref(), table)?
            .ok_or_else(|| SqlError::InvalidCollection(table.to_string()))?;
        let columns = if insert.columns.is_empty() {
            schema
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect()
        } else {
            insert
                .columns
                .iter()
                .map(|column| object_name(column).unwrap_or_default())
                .collect::<Vec<_>>()
        };
        let rows = self.execute_query(source)?.rows;
        let mut result = SqlResult::command("INSERT 0 0");
        let mut affected = 0usize;
        for row in &rows {
            let mut row_affected = 0usize;
            for action in &rule.actions {
                let mut parsed = parse_statements(action)?;
                for statement in &mut parsed {
                    bind_new_row(statement, &columns, row);
                    let produced = self.execute_statement(statement)?;
                    if matches!(statement, Statement::Insert(_)) {
                        row_affected =
                            crate::postgres_control::row_count(&produced).max(0) as usize;
                    }
                    if !produced.columns.is_empty() {
                        if result.columns.is_empty() {
                            result.columns = produced.columns.clone();
                            result.column_types = produced.column_types.clone();
                            result.column_metadata = produced.column_metadata.clone();
                        }
                        result.rows.extend(produced.rows);
                    }
                }
            }
            affected = affected.saturating_add(row_affected);
        }
        result.command_tag = Some(format!("INSERT 0 {affected}"));
        Ok(result)
    }
}

fn strip_suffix_ci<'a>(value: &'a str, suffix: &str) -> Option<&'a str> {
    let split = value.len().checked_sub(suffix.len())?;
    (value.is_char_boundary(split) && value[split..].eq_ignore_ascii_case(suffix))
        .then(|| &value[..split])
}

/// The text after a leading keyword followed by any whitespace.
fn keyword<'a>(text: &'a str, word: &str) -> Option<&'a str> {
    let text = text.trim_start();
    let head = text.get(..word.len())?;
    let tail = &text[word.len()..];
    (head.eq_ignore_ascii_case(word) && tail.starts_with(char::is_whitespace)).then_some(tail)
}

/// `pg_rules`: one row per user rule.
pub(crate) fn pg_rules_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    list_rules(db)?
        .iter()
        .map(|rule| {
            Ok(virtual_row([
                (
                    "schemaname",
                    SqlValue::String(relation_catalog_namespace(db, &rule.table)?),
                ),
                (
                    "tablename",
                    SqlValue::String(catalog_display_name(&rule.table)),
                ),
                ("rulename", SqlValue::String(rule.name.clone())),
                ("definition", SqlValue::String(rule.definition.clone())),
            ]))
        })
        .collect()
}
