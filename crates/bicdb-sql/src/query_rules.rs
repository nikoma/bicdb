//! Query rewrite rules (`CREATE RULE`), unconditional `DO INSTEAD` and
//! `DO ALSO` forms: `NOTHING`, or one or more statements that run once per
//! affected row with `NEW` (INSERT, UPDATE) and `OLD` (UPDATE, DELETE) bound
//! to the row's values. As in PostgreSQL, an INSERT runs before its rules'
//! actions and an UPDATE or DELETE after them; an INSTEAD rule suppresses
//! the original statement. Conditional (`WHERE`) rules are rejected.
use crate::*;
use std::sync::Arc;

pub(crate) const RULE_COLLECTION: &str = "__bicdb_pg_rules";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct RuleSchema {
    pub(crate) name: String,
    pub(crate) table: String,
    pub(crate) event: String,
    /// Empty for `DO [INSTEAD | ALSO] NOTHING`.
    pub(crate) actions: Vec<String>,
    /// `DO ALSO`: the original statement still runs.
    #[serde(default)]
    pub(crate) also: bool,
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
        Ok(Some(record)) => Ok(Some(serde_json::from_value(record.metadata.clone())?)),
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
    let (action, also) = if let Some(action) = keyword(action, "INSTEAD") {
        (action, false)
    } else if let Some(action) = keyword(action, "ALSO") {
        (action, true)
    } else {
        // PostgreSQL's default is ALSO.
        (action, true)
    };
    let action = action.trim();
    let actions = if action.eq_ignore_ascii_case("NOTHING") {
        Vec::new()
    } else {
        let body = action
            .strip_prefix('(')
            .and_then(|inner| inner.strip_suffix(')'))
            .unwrap_or(action);
        match parse_statements(body) {
            Ok(statements) if statements.is_empty() => return Err(invalid()),
            Ok(_) => {}
            // Utility actions such as NOTIFY run as text; they cannot use
            // NEW or OLD.
            Err(error) => {
                let lower = body.to_ascii_lowercase();
                if lower.contains("new.") || lower.contains("old.") {
                    return Err(error);
                }
            }
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
            also,
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

/// Binds `NEW.column` / `OLD.column` references of a rule action to one
/// row's values.
fn bind_rule_row(
    statement: &mut Statement,
    columns: &[String],
    new: Option<&[SqlValue]>,
    old: Option<&[SqlValue]>,
) {
    let _ = sqlparser::ast::visit_expressions_mut(statement, |expr| {
        if let Expr::CompoundIdentifier(parts) = expr {
            if let [qualifier, column] = parts.as_slice() {
                let row = if qualifier.value.eq_ignore_ascii_case("new") {
                    new
                } else if qualifier.value.eq_ignore_ascii_case("old") {
                    old
                } else {
                    return ControlFlow::Continue(());
                };
                let column = ident_value(column);
                let value = columns
                    .iter()
                    .position(|name| name.eq_ignore_ascii_case(&column))
                    .and_then(|index| row.and_then(|row| row.get(index)))
                    .unwrap_or(&SqlValue::Null);
                *expr = value_expr(value);
            }
        }
        ControlFlow::<()>::Continue(())
    });
}

thread_local! {
    static RULE_DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
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
        if load_schema(self.db_ref(), &rule.table)?.is_none()
            && load_view(self.db_ref(), &rule.table)?.is_none()
        {
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

    /// Applies the rules of the statement's target: `Some` when an INSTEAD
    /// rule replaced the statement or the statement ran here with its ALSO
    /// rules' actions.
    pub(crate) fn apply_instead_rule(
        &mut self,
        statement: &Statement,
    ) -> Result<Option<SqlResult>> {
        if take_rule_skip() {
            return Ok(None);
        }
        let (table, alias, event) = match statement {
            Statement::Insert(insert) => match &insert.table {
                TableObject::TableName(name) => (name, None, "insert"),
                _ => return Ok(None),
            },
            Statement::Update(update) => match &update.table.relation {
                TableFactor::Table { name, alias, .. } => (name, alias.as_ref(), "update"),
                _ => return Ok(None),
            },
            Statement::Delete(delete) => match &delete.from {
                FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => {
                    match tables.as_slice() {
                        [TableWithJoins {
                            relation: TableFactor::Table { name, alias, .. },
                            ..
                        }] => (name, alias.as_ref(), "delete"),
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
        let table_key = relation_name(table)?;
        let mut matching: Vec<RuleSchema> = rules
            .iter()
            .filter(|rule| rule.table == table_key && rule.event == event)
            .cloned()
            .collect();
        if matching.is_empty() {
            return Ok(None);
        }
        // PostgreSQL applies a relation's rules in name order.
        matching.sort_by(|left, right| left.name.cmp(&right.name));
        let depth = RULE_DEPTH.with(|depth| depth.get());
        if depth >= 16 {
            return Err(SqlError::RaisedException {
                sqlstate: "42P17".to_string(),
                message: format!(
                    "infinite recursion detected in rules for relation \"{}\"",
                    table_key.rsplit('.').next().unwrap_or(&table_key)
                ),
                detail: None,
            });
        }
        let table_name = table.clone();
        let alias = alias.map(|alias| ident_value(&alias.name));
        RULE_DEPTH.with(|cell| cell.set(depth + 1));
        let result = self.with_statement_transaction(|session| {
            session.execute_with_rules(statement, &table_key, &table_name, alias, event, &matching)
        });
        RULE_DEPTH.with(|cell| cell.set(depth));
        result.map(Some)
    }

    fn execute_with_rules(
        &mut self,
        statement: &Statement,
        table: &str,
        table_name: &ObjectName,
        alias: Option<String>,
        event: &str,
        rules: &[RuleSchema],
    ) -> Result<SqlResult> {
        let instead = rules.iter().any(|rule| !rule.also);
        let (columns, new_rows, old_rows) =
            self.rule_rows(statement, table, table_name, alias, event)?;
        let mut original = None;
        // An INSERT runs before its rules' actions; UPDATE and DELETE after.
        if !instead && event == "insert" {
            original = Some(self.execute_statement_without_rules(statement)?);
        }
        let mut last_same_kind = None;
        // Rows an INSTEAD rule's SELECT actions return are the statement's
        // result rows, under the original command's tag.
        let mut returned: Option<SqlResult> = None;
        for index in 0..new_rows.len().max(old_rows.len()) {
            let new = new_rows.get(index).map(Vec::as_slice);
            let old = old_rows.get(index).map(Vec::as_slice);
            for rule in rules {
                for action in &rule.actions {
                    if let Some((channel, payload)) = rule_notify(action) {
                        // Delivered on commit like a NOTIFY statement.
                        self.pending_notifications.push((channel, payload));
                        continue;
                    }
                    match parse_statements(action) {
                        Ok(mut parsed) => {
                            for statement in &mut parsed {
                                bind_rule_row(statement, &columns, new, old);
                                let produced = self.execute_statement(statement)?;
                                if !rule.also && statement_event(statement) == Some(event) {
                                    let count = crate::postgres_control::row_count(&produced).max(0)
                                        as usize;
                                    *last_same_kind.get_or_insert(0) += count;
                                }
                                if instead
                                    && matches!(statement, Statement::Query(_))
                                    && !produced.columns.is_empty()
                                {
                                    let result = returned.get_or_insert_with(|| {
                                        let mut result = SqlResult::empty(produced.columns.clone());
                                        result.column_types = produced.column_types.clone();
                                        result.column_metadata = produced.column_metadata.clone();
                                        result
                                    });
                                    result.rows.extend(produced.rows);
                                }
                            }
                        }
                        Err(_) => {
                            if let Some((channel, payload)) = rule_notify(action) {
                                // Delivered on commit like a NOTIFY statement.
                                self.pending_notifications.push((channel, payload));
                            } else {
                                self.execute(action)?;
                            }
                        }
                    }
                }
            }
        }
        if !instead && event != "insert" {
            original = Some(self.execute_statement_without_rules(statement)?);
        }
        if let Some(original) = original {
            return Ok(original);
        }
        let count = last_same_kind.unwrap_or(0);
        let tag = match event {
            "insert" => format!("INSERT 0 {count}"),
            "update" => format!("UPDATE {count}"),
            _ => format!("DELETE {count}"),
        };
        if let Some(mut result) = returned {
            result.command_tag = Some(tag);
            return Ok(result);
        }
        Ok(SqlResult::command(tag))
    }

    /// Runs the statement itself, past its target's rules.
    fn execute_statement_without_rules(&mut self, statement: &Statement) -> Result<SqlResult> {
        let depth = RULE_DEPTH.with(|depth| depth.get());
        SKIP_RULES.with(|skip| skip.set(Some(depth)));
        let result = self.execute_statement(statement);
        SKIP_RULES.with(|skip| skip.set(None));
        result
    }

    /// The target's visible columns and the NEW / OLD rows the statement
    /// affects.
    #[allow(clippy::type_complexity)]
    fn rule_rows(
        &mut self,
        statement: &Statement,
        table: &str,
        table_name: &ObjectName,
        alias: Option<String>,
        event: &str,
    ) -> Result<(Vec<String>, Vec<Vec<SqlValue>>, Vec<Vec<SqlValue>>)> {
        let schema = load_schema(self.db_ref(), table)?;
        let columns: Vec<String> = match &schema {
            Some(schema) => schema
                .columns
                .iter()
                .filter(|column| !column.hidden)
                .map(|column| column.name.clone())
                .collect(),
            None => load_view(self.db_ref(), table)?
                .map(|view| {
                    view.columns
                        .iter()
                        .map(|column| column.name.clone())
                        .collect()
                })
                .unwrap_or_default(),
        };
        // Without an alias the rows are qualified by the name the statement
        // wrote: `table` is the storage key, which for a table outside public
        // is the reserved schema-isolated name.
        let alias = alias.unwrap_or_else(|| {
            object_name_parts(table_name)
                .last()
                .cloned()
                .unwrap_or_else(|| table.rsplit('.').next().unwrap_or(table).to_string())
        });
        let target_rows = |session: &mut Self, extra: Vec<Expr>, selection: Option<&Expr>| {
            let mut query = parse_query(&format!(
                "SELECT \"{0}\".* FROM {1} AS \"{0}\"",
                alias.replace('"', "\"\""),
                table_name
            ))?;
            if let SetExpr::Select(select) = query.body.as_mut() {
                select
                    .projection
                    .extend(extra.into_iter().map(SelectItem::UnnamedExpr));
                select.selection = selection.cloned();
            }
            session.execute_query(&query).map(|result| result.rows)
        };
        match statement {
            Statement::Insert(insert) => {
                let Some(source) = insert.source.as_ref() else {
                    return Ok((columns, Vec::new(), Vec::new()));
                };
                let targets: Vec<String> = if insert.columns.is_empty() {
                    columns.clone()
                } else {
                    insert
                        .columns
                        .iter()
                        .map(|column| {
                            object_name_parts(column)
                                .last()
                                .cloned()
                                .unwrap_or_default()
                        })
                        .collect()
                };
                let rows = self.execute_query(source)?.rows;
                let mut new_rows = Vec::with_capacity(rows.len());
                for row in rows {
                    let mut values = Vec::with_capacity(columns.len());
                    for column in &columns {
                        match targets.iter().position(|target| target == column) {
                            Some(index) if index < row.len() => values.push(row[index].clone()),
                            _ => values.push(
                                self.default_value_for_column(schema.as_ref(), column)?
                                    .unwrap_or(SqlValue::Null),
                            ),
                        }
                    }
                    new_rows.push(values);
                }
                Ok((columns, new_rows, Vec::new()))
            }
            Statement::Update(update) => {
                if update.from.is_some() {
                    return Err(SqlError::Unsupported(
                        "UPDATE ... FROM on a relation with rules is not supported".into(),
                    ));
                }
                let mut targets = Vec::new();
                let mut values = Vec::new();
                for assignment in &update.assignments {
                    let AssignmentTarget::ColumnName(name) = &assignment.target else {
                        return Err(SqlError::Unsupported(
                            "row assignments on a relation with rules are not supported".into(),
                        ));
                    };
                    let column = object_name_parts(name).last().cloned().unwrap_or_default();
                    let index = columns
                        .iter()
                        .position(|candidate| candidate == &column)
                        .ok_or_else(|| SqlError::UndefinedColumn {
                            table: table.to_string(),
                            column: column.clone(),
                        })?;
                    targets.push(index);
                    values.push(assignment.value.clone());
                }
                let width = columns.len();
                let rows = target_rows(self, values, update.selection.as_ref())?;
                let mut old_rows = Vec::with_capacity(rows.len());
                let mut new_rows = Vec::with_capacity(rows.len());
                for row in rows {
                    let old = row[..width.min(row.len())].to_vec();
                    let mut new = old.clone();
                    for (index, value) in targets.iter().zip(row[width.min(row.len())..].iter()) {
                        new[*index] = value.clone();
                    }
                    old_rows.push(old);
                    new_rows.push(new);
                }
                Ok((columns, new_rows, old_rows))
            }
            Statement::Delete(delete) => {
                if delete.using.is_some() {
                    return Err(SqlError::Unsupported(
                        "DELETE ... USING on a relation with rules is not supported".into(),
                    ));
                }
                let rows = target_rows(self, Vec::new(), delete.selection.as_ref())?;
                Ok((columns, Vec::new(), rows))
            }
            _ => Ok((columns, Vec::new(), Vec::new())),
        }
    }
}

thread_local! {
    /// Set while a statement runs past its own rules (at this rule depth).
    static SKIP_RULES: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

/// Whether the statement now executing is the original of a rule
/// application, which must not apply its rules again.
pub(crate) fn take_rule_skip() -> bool {
    SKIP_RULES.with(|skip| skip.take().is_some())
}

/// `NOTIFY channel [, 'payload']` as a rule action.
fn rule_notify(action: &str) -> Option<(String, String)> {
    let rest = keyword(action.trim(), "NOTIFY")?
        .trim()
        .trim_end_matches(';')
        .trim();
    let (channel, payload) = match rest.split_once(',') {
        Some((channel, payload)) => {
            let payload = payload.trim();
            let inner = payload.strip_prefix('\'')?.strip_suffix('\'')?;
            (channel.trim(), inner.replace("''", "'"))
        }
        None => (rest, String::new()),
    };
    let channel = match channel.strip_prefix('"').and_then(|c| c.strip_suffix('"')) {
        Some(quoted) => quoted.to_string(),
        None => channel.to_ascii_lowercase(),
    };
    (!channel.is_empty() && !channel.contains(char::is_whitespace)).then_some((channel, payload))
}

fn statement_event(statement: &Statement) -> Option<&'static str> {
    match statement {
        Statement::Insert(_) => Some("insert"),
        Statement::Update(_) => Some("update"),
        Statement::Delete(_) => Some("delete"),
        _ => None,
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
