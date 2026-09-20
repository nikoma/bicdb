//! Row-level trigger execution.
//!
//! Before this module, `CREATE TRIGGER` was accepted and stored but nothing
//! ever fired except the `pg_notify` AFTER-INSERT special case. That is the
//! worst possible shape for stored enforcement — fail-open: a caller writes a
//! guard that raises on illegal writes, the DDL succeeds, and the guard
//! silently never runs. A double-entry ledger's balance invariant "held"
//! because nothing checked it.
//!
//! Semantics implemented here, matching PostgreSQL where BicDB application's generated
//! enforcement depends on it:
//!
//! * `BEFORE {INSERT|UPDATE} ... FOR EACH ROW`: the function sees `NEW` (and
//!   `OLD` on update); returning `NULL` suppresses the row, returning a row
//!   replaces `NEW`, and `RAISE EXCEPTION` aborts the statement.
//! * `BEFORE DELETE`: returning `NULL` suppresses the delete.
//! * `AFTER ...`: fired once the rows are written, same transaction; the
//!   return value is ignored.
//! * `CONSTRAINT TRIGGER ... DEFERRABLE INITIALLY DEFERRED`: queued on the
//!   open transaction and fired at `COMMIT`, so a multi-statement write (a
//!   journal entry and its lines) is checked as a whole. Outside an explicit
//!   transaction the queue drains at statement end, which is where an
//!   autocommit statement's transaction commits.
//!
//! Trigger functions run through the ordinary PLpgSQL interpreter with
//! `NEW`/`OLD` bound as JSON row values (dotted access resolves through the
//! same routine-variable path lookup every declared row variable uses) and
//! PostgreSQL's trigger metadata variables (`TG_OP`, `TG_TABLE_SCHEMA`,
//! `TG_TABLE_NAME`, `TG_RELID`, `TG_ARGV`, and peers).

use super::*;
use crate::schema_meta::ROUTINE_FOUND_VAR;

/// A constraint-trigger invocation queued to fire at COMMIT. Serialized onto
/// the core `Transaction` (not kept on the session): in server mode each
/// statement runs in its own short-lived session while the transaction travels
/// the whole BEGIN..COMMIT span, and rollback dropping the transaction is
/// exactly the discard semantics deferral requires.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DeferredTriggerCall {
    pub(crate) trigger_name: String,
    pub(crate) function_name: String,
    pub(crate) vars: Vec<(String, SqlValue)>,
}

/// Bound on trigger recursion (a trigger whose function writes a table that
/// itself carries triggers). PostgreSQL relies on stack depth; a fixed cap
/// keeps a self-inserting trigger from wedging the server.
const MAX_TRIGGER_DEPTH: usize = 64;

impl<'db> SqlSession<'db> {
    /// Inspect mutation targets (including writing CTEs) before executing any
    /// part of the statement. Ordinary trigger-free autocommit writes retain
    /// their native-policy path; triggered statements need transactional guards.
    pub(super) fn mutation_has_row_triggers(
        &self,
        ast: &impl sqlparser::ast::Visit,
    ) -> Result<bool> {
        use std::ops::ControlFlow;
        let triggers = crate::catalog_memo::triggers_shared(self.db_ref())?;
        if !triggers.iter().any(|trigger| trigger.enabled) {
            return Ok(false);
        }
        let result = sqlparser::ast::visit_statements(ast, |statement| {
            let table = match statement {
                Statement::Insert(insert) => match &insert.table {
                    TableObject::TableName(name) => relation_name(name),
                    _ => return ControlFlow::Continue(()),
                },
                Statement::Update(update) => {
                    table_with_joins_name_and_alias(&update.table).map(|(table, _)| table)
                }
                Statement::Delete(delete) => {
                    delete_from_table_and_alias(delete).map(|(table, _)| table)
                }
                _ => return ControlFlow::Continue(()),
            };
            let table = match table {
                Ok(table) => resolve_session_relation_name_if_exists(self.db_ref(), &table),
                Err(error) => return ControlFlow::Break(Err(error)),
            };
            if triggers.iter().any(|trigger| {
                trigger.enabled
                    && trigger.for_each.eq_ignore_ascii_case("row")
                    && trigger.table_name.eq_ignore_ascii_case(&table)
            }) {
                ControlFlow::Break(Ok(true))
            } else {
                ControlFlow::Continue(())
            }
        });
        match result {
            ControlFlow::Break(result) => result,
            ControlFlow::Continue(()) => Ok(false),
        }
    }

    /// The row as a trigger body sees it: a JSON object over the table's
    /// visible columns, so `NEW.column` resolves via routine-variable dotted
    /// lookup with the same values SQL over the table would produce.
    pub(crate) fn trigger_row_value(schema: &TableSchema, record: &Record) -> SqlValue {
        let mut object = serde_json::Map::new();
        for column in &schema.columns {
            if column.hidden {
                continue;
            }
            let value = record_column_value(record, schema, &column.name);
            object.insert(column.name.clone(), crate::jsonb::sql_value_to_json(&value));
        }
        SqlValue::Json(JsonValue::Object(object))
    }

    fn row_triggers_matching(
        &self,
        table: &str,
        timing: &str,
        op: &str,
    ) -> Result<Vec<TriggerSchema>> {
        let all = crate::catalog_memo::triggers_shared(self.db_ref())?;
        let mut triggers = all
            .iter()
            .filter(|trigger| {
                trigger.enabled
                    && trigger.table_name.eq_ignore_ascii_case(table)
                    && trigger.timing.eq_ignore_ascii_case(timing)
                    && trigger.for_each.eq_ignore_ascii_case("row")
                    && trigger.fires_on(op)
            })
            .cloned()
            .collect::<Vec<_>>();
        // PostgreSQL fires same-event triggers in name order.
        triggers.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(triggers)
    }

    /// Whether any row trigger exists for (table, timing, op) — a cheap guard
    /// so the unqualified write paths pay one catalog scan, not a frame build.
    pub(crate) fn has_row_triggers(&self, table: &str, timing: &str, op: &str) -> Result<bool> {
        Ok(!self.row_triggers_matching(table, timing, op)?.is_empty())
    }

    /// Run one trigger function with the given variables bound. Returns the
    /// function's RETURN value (None for a bare `RETURN;` or fall-through).
    fn call_trigger_function(
        &mut self,
        trigger_name: &str,
        function_name: &str,
        vars: &[(String, SqlValue)],
    ) -> Result<Option<SqlValue>> {
        if self.trigger_depth >= MAX_TRIGGER_DEPTH {
            return Err(SqlError::InvalidSql(format!(
                "trigger {trigger_name} exceeded the maximum trigger nesting depth of {MAX_TRIGGER_DEPTH}"
            )));
        }
        let Some(routine) =
            resolve_routine_cached(self.db_ref(), RoutineKind::Function, function_name)?
        else {
            return Err(SqlError::InvalidSql(format!(
                "trigger {trigger_name} references missing function {function_name}"
            )));
        };
        self.trigger_depth += 1;
        let result = self.execute_with_routine_security(&routine.schema, |session| {
            let mut frame =
                RoutineFrame::new_with_symbols(&routine.ir.params, &[], &routine.ir.symbol_names)?;
            // PostgreSQL exposes trigger variables before a PL/pgSQL block's
            // declarations are initialized. Generated trigger functions use
            // that ordering for defaults such as
            // `qualified_table TEXT := TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME`.
            for (name, value) in vars {
                frame.set(name, value.clone());
            }
            session.initialize_routine_frame(&mut frame, &routine.ir.declarations)?;
            frame.set(ROUTINE_FOUND_VAR, SqlValue::Bool(false));
            match session.execute_routine_block(
                &mut frame,
                &routine.ir.statements,
                &routine.ir.exception_handlers,
            )? {
                RoutineControl::NextIteration => {
                    Err(SqlError::InvalidSql("CONTINUE escaped a loop".into()))
                }
                RoutineControl::Return(value) => Ok(value),
                RoutineControl::Continue => Ok(None),
            }
        });
        self.trigger_depth -= 1;
        result
    }

    fn trigger_vars(
        trigger: &TriggerSchema,
        schema: &TableSchema,
        op: &str,
        old: Option<SqlValue>,
        new: Option<SqlValue>,
    ) -> Vec<(String, SqlValue)> {
        let table_name = logical_relation_name(&trigger.table_name)
            .unwrap_or_else(|| trigger.table_name.clone());
        // Unlike ordinary PostgreSQL arrays, TG_ARGV is zero-based.
        let arguments = array_value_with_lower_bounds(
            trigger
                .arguments
                .iter()
                .cloned()
                .map(SqlValue::String)
                .collect(),
            vec![0],
        );
        let mut vars = vec![
            (
                "tg_name".to_string(),
                SqlValue::String(trigger.name.clone()),
            ),
            (
                "tg_when".to_string(),
                SqlValue::String(trigger.timing.to_ascii_uppercase()),
            ),
            (
                "tg_level".to_string(),
                SqlValue::String(trigger.for_each.to_ascii_uppercase()),
            ),
            (
                "tg_op".to_string(),
                SqlValue::String(op.to_ascii_uppercase()),
            ),
            (
                "tg_table_name".to_string(),
                SqlValue::String(table_name.clone()),
            ),
            ("tg_relname".to_string(), SqlValue::String(table_name)),
            (
                "tg_table_schema".to_string(),
                SqlValue::String(schema.schema_name.clone()),
            ),
            (
                "tg_relid".to_string(),
                SqlValue::Int(table_relation_oid(schema)),
            ),
            (
                "tg_nargs".to_string(),
                SqlValue::Int(i64::try_from(trigger.arguments.len()).unwrap_or(i64::MAX)),
            ),
            ("tg_argv".to_string(), arguments),
            ("old".to_string(), old.unwrap_or(SqlValue::Null)),
            ("new".to_string(), new.unwrap_or(SqlValue::Null)),
        ];
        vars.shrink_to_fit();
        vars
    }

    /// Apply the row a BEFORE trigger returned onto the record being written.
    /// Only columns whose value actually changed are written back, so a body
    /// that does `RETURN NEW;` untouched costs nothing and cannot trip the
    /// primary-key update guard.
    fn apply_returned_row(
        schema: &TableSchema,
        record: &mut Record,
        original: &SqlValue,
        returned: &SqlValue,
    ) -> Result<()> {
        let (SqlValue::Json(JsonValue::Object(before)), SqlValue::Json(JsonValue::Object(after))) =
            (original, returned)
        else {
            return Ok(());
        };
        for (column, value) in after {
            if before.get(column) == Some(value) {
                continue;
            }
            set_record_column(record, Some(schema), column, json_to_sql_value(value))?;
        }
        Ok(())
    }

    /// BEFORE INSERT for a batch: each record runs through every matching
    /// trigger in name order; a NULL return drops the record, a row return
    /// replaces it. Returns the surviving records.
    pub(crate) fn apply_before_insert_row_triggers(
        &mut self,
        table: &str,
        schema: Option<&TableSchema>,
        records: Vec<Record>,
    ) -> Result<Vec<Record>> {
        self.apply_before_insert_row_triggers_reporting(table, schema, records)
            .map(|(records, _)| records)
    }

    /// `apply_before_insert_row_triggers` that also reports whether any
    /// trigger ran (a caller that verified the rows as built must re-verify
    /// rows a trigger may have changed).
    pub(crate) fn apply_before_insert_row_triggers_reporting(
        &mut self,
        table: &str,
        schema: Option<&TableSchema>,
        records: Vec<Record>,
    ) -> Result<(Vec<Record>, bool)> {
        let Some(schema) = schema else {
            return Ok((records, false));
        };
        let triggers = self.row_triggers_matching(table, "before", "insert")?;
        if triggers.is_empty() {
            return Ok((records, false));
        }
        let mut surviving = Vec::with_capacity(records.len());
        'record: for mut record in records {
            for trigger in &triggers {
                let new_value = Self::trigger_row_value(schema, &record);
                let vars =
                    Self::trigger_vars(trigger, schema, "insert", None, Some(new_value.clone()));
                match self.call_trigger_function(&trigger.name, &trigger.function_name, &vars)? {
                    Some(SqlValue::Null) => continue 'record,
                    Some(returned) => {
                        Self::apply_returned_row(schema, &mut record, &new_value, &returned)?;
                    }
                    None => {}
                }
            }
            surviving.push(record);
        }
        Ok((surviving, true))
    }

    /// BEFORE UPDATE for one row. `None` means a trigger suppressed the
    /// update; otherwise the (possibly modified) record to write.
    pub(crate) fn apply_before_update_row_triggers(
        &mut self,
        table: &str,
        schema: Option<&TableSchema>,
        old: &Record,
        mut record: Record,
    ) -> Result<Option<Record>> {
        let Some(schema) = schema else {
            return Ok(Some(record));
        };
        let triggers = self.row_triggers_matching(table, "before", "update")?;
        if triggers.is_empty() {
            return Ok(Some(record));
        }
        let old_value = Self::trigger_row_value(schema, old);
        for trigger in &triggers {
            let new_value = Self::trigger_row_value(schema, &record);
            let vars = Self::trigger_vars(
                trigger,
                schema,
                "update",
                Some(old_value.clone()),
                Some(new_value.clone()),
            );
            match self.call_trigger_function(&trigger.name, &trigger.function_name, &vars)? {
                Some(SqlValue::Null) => return Ok(None),
                Some(returned) => {
                    Self::apply_returned_row(schema, &mut record, &new_value, &returned)?;
                }
                None => {}
            }
        }
        Ok(Some(record))
    }

    /// BEFORE DELETE for one row. `false` means a trigger suppressed it.
    pub(crate) fn apply_before_delete_row_triggers(
        &mut self,
        table: &str,
        schema: Option<&TableSchema>,
        old: &Record,
    ) -> Result<bool> {
        let Some(schema) = schema else {
            return Ok(true);
        };
        let triggers = self.row_triggers_matching(table, "before", "delete")?;
        if triggers.is_empty() {
            return Ok(true);
        }
        let old_value = Self::trigger_row_value(schema, old);
        for trigger in &triggers {
            let vars = Self::trigger_vars(trigger, schema, "delete", Some(old_value.clone()), None);
            if let Some(SqlValue::Null) =
                self.call_trigger_function(&trigger.name, &trigger.function_name, &vars)?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// AFTER triggers for a batch of written rows. Immediate AFTER triggers
    /// run inline (same transaction); DEFERRABLE INITIALLY DEFERRED constraint
    /// triggers queue for COMMIT — unless there is no open transaction, in
    /// which case statement end IS commit and they run now.
    pub(crate) fn fire_after_row_triggers<'record>(
        &mut self,
        table: &str,
        schema: Option<&TableSchema>,
        op: &str,
        pairs: impl IntoIterator<Item = (Option<&'record Record>, Option<&'record Record>)>,
    ) -> Result<()> {
        let Some(schema) = schema else {
            return Ok(());
        };
        let triggers = self.row_triggers_matching(table, "after", op)?;
        if triggers.is_empty() {
            return Ok(());
        }
        for (old, new) in pairs {
            let old_value = old.map(|record| Self::trigger_row_value(schema, record));
            let new_value = new.map(|record| Self::trigger_row_value(schema, record));
            for trigger in &triggers {
                // The pg_notify special case predates general execution and
                // stays on its dedicated path in fire_after_insert_triggers.
                if trigger_function_is_pg_notify(self.db_ref(), &trigger.function_name)? {
                    continue;
                }
                let vars =
                    Self::trigger_vars(trigger, schema, op, old_value.clone(), new_value.clone());
                if trigger.is_constraint && trigger.initially_deferred && self.tx.is_some() {
                    let call = DeferredTriggerCall {
                        trigger_name: trigger.name.clone(),
                        function_name: trigger.function_name.clone(),
                        vars,
                    };
                    let hook = serde_json::to_value(&call)?;
                    self.tx
                        .as_mut()
                        .expect("checked is_some above")
                        .push_deferred_hook(hook);
                } else {
                    self.call_trigger_function(&trigger.name, &trigger.function_name, &vars)?;
                }
            }
        }
        Ok(())
    }

    /// Drain the deferred constraint-trigger queue riding the open
    /// transaction. Called with the transaction still open, immediately before
    /// it commits; an error here aborts the commit exactly as a failed
    /// statement would. Public because the pgwire server commits buffered
    /// transactions itself and must drain through a session first.
    pub fn fire_deferred_row_triggers(&mut self) -> Result<()> {
        loop {
            let hooks = match self.tx.as_mut() {
                Some(tx) => tx.take_deferred_hooks(),
                None => return Ok(()),
            };
            if hooks.is_empty() {
                return Ok(());
            }
            for hook in hooks {
                let call: DeferredTriggerCall = serde_json::from_value(hook).map_err(|error| {
                    SqlError::InvalidSql(format!("malformed deferred trigger hook: {error}"))
                })?;
                self.call_trigger_function(&call.trigger_name, &call.function_name, &call.vars)?;
            }
            // A deferred body may itself write rows that queue more deferred
            // work; loop until quiet (genuine recursion is bounded by
            // MAX_TRIGGER_DEPTH inside the calls themselves).
        }
    }
}

/// Whether a trigger's function body is the `pg_notify` pattern served by the
/// legacy notification path.
fn trigger_function_is_pg_notify(db: &BicDb, function_name: &str) -> Result<bool> {
    let Some(function) = load_routine(db, RoutineKind::Function, function_name)? else {
        return Ok(false);
    };
    Ok(pg_notify_trigger_call(&function.definition).is_some())
}
