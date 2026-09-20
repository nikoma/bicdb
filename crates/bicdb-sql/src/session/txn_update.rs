//! Split out of the parent module to keep files digestible; behavior
//! unchanged — a separate `impl` block on the same type.
use super::*;

/// Inferred types for one routine binary-operator node: the operand types the
/// range operators consult (`projected_expr_pg_type_with_db`) and the three
/// types the generic evaluator consults (`binary_expr_types`).
pub(crate) struct RoutineBinaryExprTypes {
    pub(crate) range_left: Option<String>,
    pub(crate) range_right: Option<String>,
    pub(crate) binary: BinaryExprTypes,
}

impl RoutineBinaryExprTypes {
    fn compute(db: &BicDb, left: &Expr, op: &BinaryOperator, right: &Expr) -> Self {
        Self {
            range_left: projected_expr_pg_type_with_db(db, left),
            range_right: projected_expr_pg_type_with_db(db, right),
            binary: binary_expr_types(left, op, right, None),
        }
    }
}

const ROUTINE_EXPR_TYPE_MEMO_MAX: usize = 65_536;

struct StoredUpdateReadPlan {
    schema: Arc<TableSchema>,
    fields: Vec<FieldRef>,
    columns: Option<Arc<Vec<String>>>,
    assignments: Option<StoredUpdateAssignmentPlan>,
}

struct StoredUpdateAssignmentPlan {
    /// Ordinals are valid only for the schema Arc retained by the read plan.
    columns: Vec<usize>,
    changed: Arc<[Box<str>]>,
}

impl StoredUpdateReadPlan {
    fn assignment_plan(
        update: &sqlparser::ast::Update,
        schema: &TableSchema,
    ) -> Result<Option<StoredUpdateAssignmentPlan>> {
        #[cfg(test)]
        SQL_STORED_UPDATE_SHAPE_BUILDS.with(|builds| builds.set(builds.get() + 1));
        let mut columns = Vec::with_capacity(update.assignments.len());
        for assignment in &update.assignments {
            let AssignmentTarget::ColumnName(column_name) = &assignment.target else {
                return Ok(None);
            };
            let column = relation_name(column_name)?;
            let Some(column_schema) = schema.column(&column) else {
                return Ok(None);
            };
            let ordinal = schema
                .columns
                .iter()
                .position(|candidate| std::ptr::eq(candidate, column_schema))
                .expect("column belongs to schema");
            if column_schema.primary_key
                || column_schema.hidden
                || column_schema.user_type.is_some()
                || column_schema.pg_type == "oid"
                || is_oid_alias_type(&column_schema.pg_type)
                || column_schema
                    .pg_type
                    .strip_suffix("[]")
                    .is_some_and(is_oid_alias_type)
                || is_vector_column(Some(schema), &column_schema.name)
                || column_schema.name.eq_ignore_ascii_case("timestamp")
                || column_schema.name.eq_ignore_ascii_case("geometry")
                || column_schema.name == "payload"
                || expr_is_default(&assignment.value)
                || expr_contains_subquery(&assignment.value)
                || columns.contains(&ordinal)
            {
                return Ok(None);
            }
            columns.push(ordinal);
        }
        let changed = columns
            .iter()
            .map(|ordinal| Box::<str>::from(schema.columns[*ordinal].name.as_str()))
            .collect();
        Ok(Some(StoredUpdateAssignmentPlan { columns, changed }))
    }

    fn build(
        update: &sqlparser::ast::Update,
        schema: &Arc<TableSchema>,
        table: &str,
        alias: &str,
    ) -> Result<Self> {
        let assignments = Self::assignment_plan(update, schema)?;
        let mut fields = FieldRef::wildcard(Some(schema));
        let mut references = Vec::new();
        // Deliberately decline function calls, subqueries, JSON paths and
        // other complex expressions. A missed dependency would change SQL
        // semantics; an unknown shape must retain full materialization.
        fn collect(expr: &Expr, refs: &mut Vec<Vec<String>>) -> bool {
            use sqlparser::ast::visit_expressions;
            use std::ops::ControlFlow;
            if visit_expressions(expr, |node| {
                if matches!(node, Expr::JsonAccess { .. }) {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                }
            })
            .is_break()
            {
                return false;
            }
            collect_predicate_column_references(expr, refs)
        }
        let complete = update
            .assignments
            .iter()
            .all(|a| collect(&a.value, &mut references))
            && update
                .selection
                .as_ref()
                .is_none_or(|expr| collect(expr, &mut references))
            && update.returning.as_ref().is_none_or(|items| {
                items.iter().all(|item| match item {
                    SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                        collect(expr, &mut references)
                    }
                    _ => false,
                })
            });
        let complete = complete
            && !references.iter().any(|reference| {
                reference.len() == 1
                    && (reference[0].eq_ignore_ascii_case(table)
                        || reference[0].eq_ignore_ascii_case(alias))
            });
        let columns = complete.then(|| {
            fields.retain(|field| {
                references.iter().any(|reference| {
                    reference
                        .last()
                        .is_some_and(|name| name.eq_ignore_ascii_case(&field.name()))
                })
            });
            // Carry the projected layout through recheck, assignment and
            // RETURNING, not just through the initial decode. No placeholder
            // cells, cloned NULL slots or unused names downstream.
            Arc::new(row_output_columns_from_fields(table, alias, &fields))
        });
        Ok(Self {
            schema: Arc::clone(schema),
            fields,
            columns,
            assignments,
        })
    }
}

thread_local! {
    static STORED_UPDATE_READ_MEMO: std::cell::RefCell<
        FxHashMap<(usize, u64, usize, usize), Rc<StoredUpdateReadPlan>>,
    > = std::cell::RefCell::new(FxHashMap::default());
    static ROUTINE_EXPR_TYPE_MEMO: std::cell::RefCell<
        rustc_hash::FxHashMap<(u64, usize, usize), std::rc::Rc<RoutineBinaryExprTypes>>,
    > = std::cell::RefCell::new(rustc_hash::FxHashMap::default());
}

thread_local! {
    /// Frame templates per compiled routine: (generation, IR id).
    static FRAME_TEMPLATE_MEMO: std::cell::RefCell<FxHashMap<(u64, usize), std::rc::Rc<RoutineFrameTemplate>>> =
        std::cell::RefCell::new(FxHashMap::default());
    /// Which dispatcher owns a function-call node inside a routine:
    /// (generation, IR id, node address) -> lowercase name + whether the
    /// stored-function path answered. A routine-embedded call used to walk
    /// twelve name-matching dispatchers (and an eager superuser lookup) on
    /// every evaluation before reaching the stored function.
    static FN_DISPATCH_MEMO: std::cell::RefCell<FxHashMap<(u64, usize, usize), std::rc::Rc<FnDispatch>>> =
        std::cell::RefCell::new(FxHashMap::default());
    /// Which stored functions evaluate without a frame (see
    /// `inline_return_fns`): (generation, IR id, argument count) -> for each
    /// symbol slot of the routine, the argument that fills it; `None` when the
    /// routine's shape disqualifies it.
    static INLINE_RETURN_MEMO: std::cell::RefCell<FxHashMap<(u64, usize, usize), Option<std::rc::Rc<[Option<usize>]>>>> =
        std::cell::RefCell::new(FxHashMap::default());
}

/// The argument slot map for a routine that is exactly `RETURN <bound expr>`
/// over its parameters: every symbol the body can name is a positional `$n`,
/// a named parameter or an `ALIAS FOR $n`, so the frame the interpreter would
/// build holds nothing but copies of the arguments. Anything else — locals,
/// OUT parameters, defaults, exception handlers, a SQL-language body, SECURITY
/// DEFINER, set returns, a body the binder declined or that hoists further
/// user calls — keeps the frame path.
fn inline_return_slots(
    schema: &RoutineSchema,
    ir: &RoutineIR,
    arg_count: usize,
) -> Option<Vec<Option<usize>>> {
    if !crate::routines::routine_language_is_plpgsql(schema)
        || schema.returns_set
        || schema.security_definer
        || !ir.exception_handlers.is_empty()
        || ir.params.len() != arg_count
    {
        return None;
    }
    match ir.statements.as_slice() {
        [RoutineStmt::Return(Some(expr))] if expr.bound.is_some() && expr.user_calls.is_empty() => {
        }
        _ => return None,
    }
    if ir.params.iter().enumerate().any(|(idx, param)| {
        param.index != idx || param.mode != RoutineArgMode::In || param.default_expr.is_some()
    }) {
        return None;
    }
    let mut slots = Vec::with_capacity(ir.symbol_names.len());
    for symbol in &ir.symbol_names {
        let arg = if let Some(position) = symbol.strip_prefix('$') {
            position
                .parse::<usize>()
                .ok()
                .filter(|position| (1..=arg_count).contains(position))
                .map(|position| position - 1)
        } else if let Some(param) = ir
            .params
            .iter()
            .find(|param| param.name.as_deref() == Some(symbol.as_str()))
        {
            Some(param.index)
        } else {
            ir.declarations.iter().find_map(|decl| match decl {
                RoutineDecl::Alias { name, position }
                    if name == symbol && (1..=arg_count).contains(position) =>
                {
                    Some(position - 1)
                }
                _ => None,
            })
        };
        slots.push(Some(arg?));
    }
    if ir
        .declarations
        .iter()
        .any(|decl| !matches!(decl, RoutineDecl::Alias { .. }))
    {
        return None;
    }
    Some(slots)
}

const ROUTINE_NODE_MEMO_MAX: usize = 65_536;

struct FnDispatch {
    name: String,
    stored: bool,
}

impl<'db> SqlSession<'db> {
    /// Index lookup caches shared across every statement of the open
    /// transaction (gated by `txn_lookup_cache_enabled`). Reuses the stored
    /// caches while they still belong to `tx` and the schema/index catalog is
    /// unchanged; otherwise installs fresh ones.
    pub(crate) fn txn_shared_lookup_caches(
        &self,
        tx: &Transaction,
    ) -> (IndexLookupCache, RowIdIndexLookupCache) {
        let db = self.db_ref();
        let schema_generation = db.collection_generation(SCHEMA_COLLECTION);
        let index_catalog_len = db.index_catalog_len();
        let mut slot = self.txn_lookup_caches.borrow_mut();
        if let Some(caches) = slot.as_ref() {
            if caches.tx_id == tx.id()
                && caches.schema_generation == schema_generation
                && caches.index_catalog_len == index_catalog_len
            {
                return (
                    caches.index_lookup_cache.clone(),
                    caches.rowid_index_lookup_cache.clone(),
                );
            }
        }
        let fresh = TxnLookupCaches {
            tx_id: tx.id(),
            schema_generation,
            index_catalog_len,
            index_lookup_cache: Rc::new(RefCell::new(FxHashMap::default())),
            rowid_index_lookup_cache: Rc::new(RefCell::new(FxHashMap::default())),
        };
        let result = (
            fresh.index_lookup_cache.clone(),
            fresh.rowid_index_lookup_cache.clone(),
        );
        *slot = Some(fresh);
        result
    }

    pub(crate) fn sql_engine(&self) -> SqlEngine<'_> {
        self.sql_engine_with_ctes(BTreeMap::new())
    }

    pub(crate) fn sql_engine_with_ctes(&self, ctes: BTreeMap<String, CteResult>) -> SqlEngine<'_> {
        let tx = self.tx.as_ref();
        let caches = tx
            .filter(|_| txn_lookup_cache_enabled())
            .map(|tx| self.txn_shared_lookup_caches(tx));
        let (index_lookup_cache, rowid_index_lookup_cache) = caches.unwrap_or_else(|| {
            (
                Rc::new(RefCell::new(FxHashMap::default())),
                Rc::new(RefCell::new(FxHashMap::default())),
            )
        });
        SqlEngine::from_session_parts(
            &*self.db_ref(),
            tx,
            self.settings,
            ctes,
            self.security_context.clone(),
            self.session_gucs.clone(),
            self.routine_vars.clone(),
            self.routine_slots.clone(),
            self.current_routine_ir,
            self.ir_owned_statement,
            self.bound_context_cache.clone(),
            index_lookup_cache,
            rowid_index_lookup_cache,
            self.runtime.clone(),
            self.cancellation.clone(),
            self.fts_limits,
        )
    }

    fn extension_event_relation(table: &str) -> String {
        if table.contains('.') {
            table.to_ascii_lowercase()
        } else {
            format!("public.{}", table.to_ascii_lowercase())
        }
    }

    fn extension_database_event_bindings(
        &self,
        relation: &str,
        operation: DatabaseOperation,
    ) -> Result<Vec<EventBindingDefinition>> {
        let bindings = crate::catalog_memo::event_bindings_shared(self.db_ref())?;
        Ok(bindings
            .iter()
            .filter(|binding| {
                binding.enabled
                    && matches!(
                        &binding.source,
                        EventSource::Database {
                            relation: source_relation,
                            operations,
                        } if source_relation.eq_ignore_ascii_case(relation)
                            && operations.contains(&operation)
                    )
            })
            .cloned()
            .collect())
    }

    /// Whether any enabled extension event binding listens to `operation` on
    /// `table`. A memo hit plus a filter over the (usually empty) list.
    pub(crate) fn has_extension_database_event_bindings(
        &self,
        table: &str,
        operation: DatabaseOperation,
    ) -> Result<bool> {
        let relation = Self::extension_event_relation(table);
        Ok(!self
            .extension_database_event_bindings(&relation, operation)?
            .is_empty())
    }

    pub(crate) fn publish_extension_database_events<'record>(
        &mut self,
        table: &str,
        operation: DatabaseOperation,
        records: impl IntoIterator<Item = (Option<&'record Record>, Option<&'record Record>)>,
    ) -> Result<()> {
        let relation = Self::extension_event_relation(table);
        let bindings = self.extension_database_event_bindings(&relation, operation)?;
        if bindings.is_empty() {
            return Ok(());
        }

        for (before, after) in records {
            for binding in &bindings {
                let event_id = uuid::Uuid::new_v4().to_string();
                let queue = binding
                    .delivery_queue
                    .as_deref()
                    .expect("validated database event binding has a delivery queue");
                let record_id = after
                    .or(before)
                    .map(|record| record.id.clone())
                    .unwrap_or_default();
                let payload = serde_json::json!({
                    "event_id": event_id,
                    "subscription": binding.name,
                    "extension": binding.extension,
                    "relation": relation,
                    "operation": operation,
                    "record_id": record_id,
                    "before": before,
                    "after": after,
                });
                let options = bicdb_core::PublishOptions {
                    headers: serde_json::json!({
                        "bicdb_extension": binding.extension,
                        "bicdb_subscription": binding.name,
                        "bicdb_operation": operation,
                    }),
                    idempotency_key: Some(event_id),
                    delay_ms: None,
                    max_attempts: Some(binding.max_attempts),
                };
                if let Some(transaction) = self.tx.as_ref() {
                    transaction.buffer_broker_publish(queue, payload, options)?;
                } else {
                    self.db_ref()
                        .with_broker(|broker| broker.publish_with(queue, payload, options))?;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn fire_after_insert_triggers(
        &mut self,
        table: &str,
        schema: Option<&TableSchema>,
        records: &[Record],
    ) -> Result<()> {
        self.publish_extension_database_events(
            table,
            DatabaseOperation::Insert,
            records.iter().map(|record| (None, Some(record))),
        )?;
        // The memoized catalog: this ran a trigger-collection scan (and a
        // JSON parse per trigger) on every INSERT statement.
        let triggers = crate::catalog_memo::triggers_shared(self.db_ref())?
            .iter()
            .filter(|trigger| {
                trigger.enabled
                    && trigger.table_name.eq_ignore_ascii_case(table)
                    && trigger.timing.eq_ignore_ascii_case("after")
                    && trigger.fires_on("insert")
                    && trigger.for_each.eq_ignore_ascii_case("row")
            })
            .cloned()
            .collect::<Vec<_>>();
        if triggers.is_empty() {
            return Ok(());
        }
        let Some(schema) = schema else {
            return Ok(());
        };
        // The pg_notify pattern keeps its dedicated notification path; every
        // other AFTER INSERT trigger executes for real (see
        // session/row_triggers.rs), deferring to COMMIT when declared so.
        let mut general = false;
        for trigger in triggers {
            let Some(function) =
                load_routine(self.db_ref(), RoutineKind::Function, &trigger.function_name)?
            else {
                continue;
            };
            let Some((channel, payload_column)) = pg_notify_trigger_call(&function.definition)
            else {
                general = true;
                continue;
            };
            for record in records {
                let payload = record_column_value(record, schema, &payload_column).to_cell();
                let notification_id = next_notification_id(self.db_ref())?;
                save_notification(
                    self.db_mut()?,
                    NotificationRecord {
                        id: notification_id,
                        channel: channel.clone(),
                        payload,
                        table_name: trigger.table_name.clone(),
                        trigger_name: trigger.name.clone(),
                        function_name: trigger.function_name.clone(),
                        created_at: unix_now(),
                    },
                )?;
            }
        }
        if general {
            self.fire_after_row_triggers(
                table,
                Some(schema),
                "insert",
                records.iter().map(|record| (None, Some(record))),
            )?;
        }
        Ok(())
    }

    pub(crate) fn execute_update(&mut self, update: &sqlparser::ast::Update) -> Result<SqlResult> {
        self.execute_update_with_ctes(update, BTreeMap::new())
    }

    /// Analyze an UPDATE for commit-time delta repair (see core `RepairPlan`).
    /// Some(plan-shape) only when EVERY assignment is `col = col +/- expr` with
    /// a row-independent numeric delta on an unindexed, unconstrained numeric
    /// column, on a plain (no FROM/RETURNING/RLS/trigger/partition) update
    /// inside a transaction. Anything else returns None and the statement uses
    /// the classic lock + recheck protocol.
    pub(crate) fn repairable_update_assignments(
        &self,
        update: &sqlparser::ast::Update,
        table: &str,
        target_alias: &str,
        schema: Option<&TableSchema>,
    ) -> Result<Option<Vec<RepairableAssignment>>> {
        if !self.update_repair_enabled() || self.tx.is_none() {
            return Ok(None);
        }
        let Some(schema) = schema else {
            return Ok(None);
        };
        if update.from.is_some() {
            return Ok(None);
        }
        // Delta repair does not rerun the WHERE predicate at commit. It is
        // therefore only safe when target-column predicates depend on the
        // primary key, whose movement is checked by commit validation. A
        // mutable condition such as balance >= debit or enabled = true must
        // retain the ordinary lock + READ COMMITTED recheck protocol.
        if let Some(selection) = &update.selection {
            let mut references = Vec::new();
            if !collect_predicate_column_references(selection, &mut references) {
                return Ok(None);
            }
            let primary_key = primary_key_columns_for_schema(schema);
            if references.iter().any(|reference| {
                reference.last().is_some_and(|name| {
                    schema.column(name).is_some()
                        && !primary_key.iter().any(|key| key.eq_ignore_ascii_case(name))
                })
            }) {
                return Ok(None);
            }
        }
        // RETURNING is allowed only when every item is a plain column that is
        // not itself repaired (checked against the repaired set below): a payment-style
        // Payment returns address/name fields alongside its ytd increments,
        // while NewOrder's RETURNING projects the repaired counter itself
        // (d_next_o_id - 1, a read dependency) and must stay disqualified.
        let mut returned_columns: Vec<String> = Vec::new();
        if let Some(returning) = &update.returning {
            for item in returning {
                let expr = match item {
                    SelectItem::UnnamedExpr(expr) => expr,
                    SelectItem::ExprWithAlias { expr, .. } => expr,
                    _ => return Ok(None),
                };
                let column = match expr {
                    Expr::Identifier(ident) => ident.value.clone(),
                    Expr::CompoundIdentifier(idents)
                        if idents.len() == 2
                            && (idents[0].value.eq_ignore_ascii_case(table)
                                || idents[0].value.eq_ignore_ascii_case(target_alias)) =>
                    {
                        idents[1].value.clone()
                    }
                    _ => return Ok(None),
                };
                returned_columns.push(column);
            }
        }
        if schema.rls_enabled
            || schema.rls_forced
            || schema.partitioning.is_some()
            || schema.partition_of.is_some()
        {
            return Ok(None);
        }
        // CHECK / exclusion constraints may span columns, and unique / FK
        // constraints on a repaired column would be validated against the
        // pre-repair record. Disqualify wholesale; per-column checks below.
        if schema.constraints.iter().any(|c| {
            matches!(
                c,
                ConstraintSchema::Check { .. } | ConstraintSchema::Exclusion { .. }
            )
        }) {
            return Ok(None);
        }
        let mut out = Vec::with_capacity(update.assignments.len());
        for assignment in &update.assignments {
            let AssignmentTarget::ColumnName(column_name) = &assignment.target else {
                return Ok(None);
            };
            let Ok(column) = relation_name(column_name) else {
                return Ok(None);
            };
            let Some(column_schema) = schema.column(&column) else {
                return Ok(None);
            };
            if column_schema.primary_key {
                return Ok(None);
            }
            let pg_type = column_schema.pg_type.to_ascii_lowercase();
            let numeric = pg_type.starts_with("int")
                || pg_type.starts_with("bigint")
                || pg_type.starts_with("smallint")
                || pg_type.starts_with("numeric")
                || pg_type.starts_with("decimal")
                || pg_type.starts_with("double")
                || pg_type.starts_with("real")
                || pg_type.starts_with("float");
            if !numeric {
                return Ok(None);
            }
            let storage_key = column_schema.name.clone();
            if storage_key.eq_ignore_ascii_case("timestamp")
                || is_vector_column(Some(schema), &storage_key)
            {
                return Ok(None);
            }
            // Column must not participate in any constraint or index (schema
            // level or executable core index).
            if schema.constraints.iter().any(|c| match c {
                ConstraintSchema::Unique { columns, .. } => {
                    columns.iter().any(|c| c.eq_ignore_ascii_case(&column))
                }
                ConstraintSchema::ForeignKey { columns, .. } => {
                    columns.iter().any(|c| c.eq_ignore_ascii_case(&column))
                }
                _ => false,
            }) {
                return Ok(None);
            }
            if schema.indexes.iter().any(|index| {
                index
                    .expression
                    .to_ascii_lowercase()
                    .contains(&column.to_ascii_lowercase())
            }) {
                return Ok(None);
            }
            let Some((delta_expr, negate)) =
                repair_delta_expr(&assignment.value, &column, table, target_alias)
            else {
                return Ok(None);
            };
            if expr_references_table_columns(&delta_expr, schema, table, target_alias) {
                return Ok(None);
            }
            out.push(RepairableAssignment {
                storage_key,
                delta_expr,
                negate,
            });
        }
        if out.is_empty() {
            return Ok(None);
        }
        // A RETURNING of a repaired column would project the stale-base value
        // rather than the final (latest + delta) row; disqualify.
        if returned_columns.iter().any(|returned| {
            out.iter()
                .any(|assignment| assignment.storage_key.eq_ignore_ascii_case(returned))
        }) {
            return Ok(None);
        }
        // Executable core indexes must not touch any repaired column, and any
        // enabled trigger on the table disqualifies. Both facts are memoized
        // per table (the uncached versions clone the index catalog and scan
        // the trigger records per statement).
        let db = self.db_ref();
        let (has_triggers, indexed_columns) = repair_table_info(db, table, || {
            let mut indexed_columns = BTreeSet::new();
            for definition in db.index_definitions() {
                if !definition.collection.eq_ignore_ascii_case(table) {
                    continue;
                }
                collect_index_column_heads(&definition.fields, &mut indexed_columns);
            }
            let has_triggers = list_triggers(db)?
                .iter()
                .any(|trigger| trigger.enabled && trigger.table_name.eq_ignore_ascii_case(table));
            Ok((has_triggers, indexed_columns))
        })?;
        if has_triggers {
            return Ok(None);
        }
        if out.iter().any(|assignment| {
            indexed_columns.contains(&assignment.storage_key.to_ascii_lowercase())
        }) {
            return Ok(None);
        }
        Ok(Some(out))
    }

    pub(crate) fn update_repair_enabled(&self) -> bool {
        static ENV: OnceLock<bool> = OnceLock::new();
        if *ENV.get_or_init(|| {
            std::env::var("BICDB_UPDATE_REPAIR")
                .map(|value| matches!(value.as_str(), "1" | "on" | "true" | "yes"))
                .unwrap_or(false)
        }) {
            return true;
        }
        self.session_gucs
            .get("bicdb.update_repair")
            .is_some_and(|value| matches!(value.as_str(), "1" | "on" | "true" | "yes"))
    }

    pub(crate) fn execute_update_with_ctes(
        &mut self,
        update: &sqlparser::ast::Update,
        ctes: BTreeMap<String, CteResult>,
    ) -> Result<SqlResult> {
        let (table, target_alias) = table_with_joins_name_and_alias(&update.table)?;
        let table = resolve_session_relation_name_if_exists(self.db_ref(), &table);
        self.require_update_privileges(&table, &update.assignments)?;
        // RETURNING projects the affected rows back to the caller, so it is a
        // read. Without this, an UPDATE-only grant was a full table read.
        if update.returning.is_some() {
            self.require_table_privilege(&table, "SELECT")?;
        }
        if load_view(self.db_ref(), &table)?.is_some() {
            return Err(SqlError::Unsupported(
                "UPDATE through views is not supported".to_string(),
            ));
        }
        let schema = load_schema_shared(self.db_ref(), &table)?;
        let from_rows = self.update_from_row_set(update.from.as_ref(), &ctes)?;
        let mut updated = 0usize;
        let mut updated_records = Vec::new();
        let mut updated_record_snapshots = Vec::new();
        let mut returning_rows = (update.returning.is_some() && from_rows.is_some()).then(Vec::new);
        let mut returning_columns: Option<Arc<Vec<String>>> = None;
        let target_columns = Arc::new(row_output_columns(&table, &target_alias, schema.as_deref()));
        // Simple updates of resident rows never parse the row: the assigned
        // columns are spliced into the old row's JSON text and the result is
        // written in its stored form (typed resident rows phase 3).
        if let Some(schema) = schema.as_ref() {
            if let Some(result) = self.try_execute_update_stored(
                update,
                &ctes,
                &table,
                &target_alias,
                schema,
                from_rows.as_ref(),
                &target_columns,
            )? {
                return Ok(result);
            }
        }
        let assignment_candidates = if let Some(from_rows) = from_rows.as_ref() {
            self.update_assignment_candidates_from_rows(
                &table,
                &target_alias,
                schema.as_deref(),
                update.selection.as_ref(),
                from_rows,
                target_columns.clone(),
                &ctes,
            )?
        } else {
            self.update_assignment_candidates_without_from(
                &table,
                &target_alias,
                schema.as_deref(),
                update.selection.as_ref(),
                target_columns.clone(),
                &ctes,
            )?
        };
        // When the statement reads existing row values, SELECT policies apply
        // to those reads in addition to the UPDATE policies (PostgreSQL
        // semantics: such rows are silently excluded).
        let reads_existing = update
            .selection
            .as_ref()
            .is_some_and(expr_references_stored_columns)
            || update
                .assignments
                .iter()
                .any(|assignment| expr_references_stored_columns(&assignment.value))
            || update
                .returning
                .as_deref()
                .is_some_and(select_items_reference_stored_columns)
            || update.from.is_some();
        let assignment_candidates = if reads_existing {
            self.retain_rls_select_visible(&table, schema.as_deref(), assignment_candidates, |c| {
                &c.record
            })?
        } else {
            assignment_candidates
        };
        let mut before_after_records = Vec::new();
        // Whether anything downstream reads the pre-update image of a row:
        // unique-key change validation, inbound foreign keys, AFTER UPDATE
        // row triggers, or extension event bindings. When none applies (the
        // common OLTP case) the per-row `before` deep clone and the
        // (before, after) pair clone are both skipped.
        let needs_before_image = match schema.as_deref() {
            None => true,
            Some(schema) => self.update_needs_before_image(update, &table, schema)?,
        };
        let repair_assignments =
            self.repairable_update_assignments(update, &table, &target_alias, schema.as_deref())?;
        let assignment_operand_memo = self.ir_owned_statement
            && self.current_routine_ir.is_some()
            && update
                .assignments
                .iter()
                .all(|assignment| !expr_contains_subquery(&assignment.value));
        let mut updated_record_repairs: Vec<Option<RepairPlan>> = Vec::new();
        // Bulk FROM-updates using UNNEST are NOT rechecked: the per-row
        // recheck (collection_state + lock-shard mutex + shard read, ten rows
        // per NewOrder) is contention-priced at high VU counts and profiled at
        // ~40% of exec; stock conflicts are the rare class (~0.03% of lock
        // failures) and stay on unlocked writes with commit-conflict retry.
        let can_read_committed_recheck =
            from_rows.is_none() && self.tx.is_some() && repair_assignments.is_none();
        // RETURNING rows for FROM-updates: when nothing but the assignments
        // can have changed the row (no BEFORE UPDATE row trigger — `before`
        // is None exactly then — and no generated column), only the assigned
        // columns are re-read from the new record and patched into the
        // candidate's slots; the whole-row decode is kept for the other cases.
        let returning_patch_columns: Option<Vec<String>> = if returning_rows.is_some()
            && !needs_before_image
            && schema
                .as_ref()
                .is_some_and(|schema| schema.columns.iter().all(|c| c.generated_expr.is_none()))
        {
            update
                .assignments
                .iter()
                .map(|assignment| match &assignment.target {
                    AssignmentTarget::ColumnName(column_name) => relation_name(column_name).ok(),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()
        } else {
            None
        };
        for (idx, mut candidate) in assignment_candidates.into_iter().enumerate() {
            if idx % 1024 == 0 {
                self.cancellation.check()?;
            }
            let mut statement_snapshot = 0;
            if can_read_committed_recheck {
                let candidate_id = candidate.record.id.clone();
                let signed_grant = self.signed_mutation_grant(&table);
                let refreshed = match self.tx.as_mut() {
                    Some(tx) => match signed_grant {
                        Some(grant) => tx
                            .read_committed_update_record_with_grant(grant, &table, &candidate_id)
                            .map_err(SqlError::from)?,
                        None => tx
                            .read_committed_update_record(&table, &candidate_id)
                            .map_err(SqlError::from)?,
                    },
                    None => None,
                };
                if let Some((latest, snapshot)) = refreshed {
                    let Some(latest) = latest else {
                        continue;
                    };
                    // Watermark-gap fast path: the recheck could not prove
                    // freshness cheaply (max_tx above the applied watermark),
                    // but the latest committed version equals the candidate we
                    // already materialized, so the read DID observe it. Stamp
                    // the snapshot and skip the slot-row rebuild and predicate
                    // re-evaluation.
                    if latest == candidate.record {
                        statement_snapshot = snapshot;
                    } else {
                        // PostgreSQL-style EvalPlanQual: the row changed after our
                        // snapshot, so refresh only the target-table slots (a FROM
                        // candidate row carries the joined FROM slots after them,
                        // which stay as scanned) and re-evaluate the predicate
                        // against the latest committed version.
                        let target_row = slot_row_from_record(
                            &table,
                            &target_alias,
                            schema.as_deref(),
                            &latest,
                        )?;
                        let mut refreshed_row = candidate.row.clone();
                        for (slot_idx, value) in target_row.into_iter().enumerate() {
                            if let Some(slot) = refreshed_row.get_mut(slot_idx) {
                                *slot = value;
                            }
                        }
                        if !self.update_row_matches(
                            &ctes,
                            candidate.columns.as_ref(),
                            &refreshed_row,
                            update.selection.as_ref(),
                        )? {
                            continue;
                        }
                        candidate.record = latest;
                        candidate.row = refreshed_row;
                        statement_snapshot = snapshot;
                    }
                }
            }
            let mut record = candidate.record;
            let before = needs_before_image.then(|| record.clone());
            // Every assignment (and repair delta) of this candidate is
            // evaluated through ONE row engine: building one per expression
            // re-cloned the CTE map and security context and rebuilt the
            // bound row context each time. Results are consumed below in the
            // original order, so error precedence is unchanged; assignments
            // to generated columns are not evaluated (they were never
            // evaluated before either).
            let (mut assignment_values, mut repair_delta_values) = {
                // Operand-type memo (keyed by AST node address) only for
                // assignment expressions that are routine-IR-owned and free
                // of subqueries: nothing derived per execution can reach the
                // memo through them. Repair deltas are per-execution clones
                // and evaluate with the memo off.
                let mut row_engine = self
                    .sql_engine_with_ctes(ctes.clone())
                    .with_operand_type_memo(assignment_operand_memo);
                let (scope, context) = row_engine.bound_row_context(candidate.columns.as_ref());
                let mut values = Vec::with_capacity(update.assignments.len());
                for assignment in &update.assignments {
                    let generated = match (&assignment.target, schema.as_deref()) {
                        (AssignmentTarget::ColumnName(column_name), Some(schema)) => {
                            relation_name(column_name)
                                .ok()
                                .and_then(|column| schema.column(&column))
                                .is_some_and(|column| column.generated_expr.is_some())
                        }
                        _ => false,
                    };
                    if generated {
                        values.push(None);
                        continue;
                    }
                    // Routine-owned assignment: the expression bound over
                    // this candidate layout is kept per IR node (the
                    // projection and aggregate paths evaluate bound
                    // expressions the same way); the binder's declines run
                    // the generic evaluator as before.
                    let bound = ir_update_plan::enabled()
                        .then(|| {
                            row_engine.ir_plan_node_key(&assignment.value as *const Expr as usize)
                        })
                        .flatten()
                        .and_then(|key| match sql_bound_assignment_cache_get(key) {
                            Some(entry) => entry,
                            None => {
                                let built = scope.bind(&assignment.value).map(|expr| {
                                    Rc::new(BoundAssignment {
                                        columns: candidate.columns.as_ref().clone(),
                                        expr,
                                    })
                                });
                                sql_bound_assignment_cache_set(key, built.clone());
                                built
                            }
                        })
                        .filter(|bound| bound.columns == *candidate.columns.as_ref());
                    values.push(Some(eval_row_or_bound_value(
                        &row_engine,
                        &candidate.row,
                        &assignment.value,
                        bound.as_deref().map(|bound| &bound.expr),
                        &context,
                    )));
                }
                row_engine.memo_operand_types = false;
                let deltas = repair_assignments.as_ref().map(|assignments| {
                    assignments
                        .iter()
                        .map(|assignment| {
                            row_engine.eval_slot_row_value(
                                &candidate.row,
                                &context,
                                &assignment.delta_expr,
                            )
                        })
                        .collect::<Vec<_>>()
                });
                (values.into_iter(), deltas.map(Vec::into_iter))
            };
            for assignment in &update.assignments {
                let evaluated = assignment_values.next().flatten();
                let AssignmentTarget::ColumnName(column_name) = &assignment.target else {
                    return Err(SqlError::Unsupported(
                        "UPDATE supports only single-column assignments".to_string(),
                    ));
                };
                let column = relation_name(column_name)?;
                if let Some(schema) = schema.as_deref() {
                    ensure_schema_column(&table, schema, &column)?;
                    if schema
                        .column(&column)
                        .is_some_and(|column| column.generated_expr.is_some())
                    {
                        if expr_is_default(&assignment.value) {
                            continue;
                        }
                        return Err(SqlError::generated_always_violation(format!(
                            "column \"{column}\" can only be updated to DEFAULT"
                        )));
                    }
                }
                let mut value = match evaluated {
                    Some(Ok(value)) => value,
                    Some(Err(SqlError::Unsupported(_))) => {
                        self.eval_session_expr(&assignment.value)?
                    }
                    Some(Err(error)) => return Err(error),
                    None => self.update_assignment_value(
                        &ctes,
                        candidate.columns.as_ref(),
                        &candidate.row,
                        &assignment.value,
                    )?,
                };
                if let Some(schema) = schema.as_deref() {
                    if schema
                        .column(&column)
                        .is_some_and(|column| column.pg_type == "oid")
                    {
                        if let Some(cast) =
                            explicit_oid_cast(value.clone(), &assignment.value, Some(schema))
                        {
                            value = cast?;
                        }
                    }
                }
                if let Some(schema) = schema.as_deref() {
                    value = resolve_oid_alias_column_value(self.db_ref(), schema, &column, value)?;
                }
                set_record_column(&mut record, schema.as_deref(), &column, value)?;
            }
            if let Some(schema) = schema.as_deref() {
                self.materialize_generated_columns(
                    &table,
                    schema,
                    std::slice::from_mut(&mut record),
                )?;
            }
            // BEFORE UPDATE row triggers see the fully assigned row and may
            // veto it (skip this candidate) or modify it. They run before the
            // repair/RETURNING bookkeeping so a suppressed row leaves no
            // trace in any of the parallel vectors.
            // `before` is None only when `update_needs_before_image` proved
            // there is no BEFORE UPDATE row trigger (among other consumers),
            // so skipping the trigger pass there is exact, not a shortcut.
            let record = match before.as_ref() {
                Some(before) => match self.apply_before_update_row_triggers(
                    &table,
                    schema.as_deref(),
                    before,
                    record,
                )? {
                    Some(record) => record,
                    None => continue,
                },
                None => record,
            };
            let mut record = record;
            let repair_plan = match repair_assignments.as_ref() {
                Some(assignments) => {
                    // The delta expressions are row-independent by analysis;
                    // evaluate them against this candidate's row context.
                    let mut deltas = Vec::with_capacity(assignments.len());
                    let mut repairable = true;
                    for assignment in assignments {
                        let value = match repair_delta_values.as_mut().and_then(Iterator::next) {
                            Some(Ok(value)) => value,
                            Some(Err(SqlError::Unsupported(_))) => {
                                self.eval_session_expr(&assignment.delta_expr)?
                            }
                            Some(Err(error)) => return Err(error),
                            None => self.update_assignment_value(
                                &ctes,
                                candidate.columns.as_ref(),
                                &candidate.row,
                                &assignment.delta_expr,
                            )?,
                        };
                        let delta = match value {
                            SqlValue::Int(value) => match assignment.negate {
                                true => match value.checked_neg() {
                                    Some(value) => RepairDelta::Int(value),
                                    None => {
                                        repairable = false;
                                        break;
                                    }
                                },
                                false => RepairDelta::Int(value),
                            },
                            SqlValue::Float(value) => {
                                RepairDelta::Float(if assignment.negate { -value } else { value })
                            }
                            // NUMERIC values travel as canonical decimal text.
                            SqlValue::String(ref value) => {
                                match bicdb_core::parse_repair_decimal(value) {
                                    Some((units, scale)) => RepairDelta::Decimal {
                                        units: if assignment.negate { -units } else { units },
                                        scale,
                                    },
                                    None => {
                                        repairable = false;
                                        break;
                                    }
                                }
                            }
                            _ => {
                                repairable = false;
                                break;
                            }
                        };
                        deltas.push((assignment.storage_key.clone(), delta));
                    }
                    repairable.then(|| RepairPlan { deltas })
                }
                None => None,
            };
            updated_record_repairs.push(repair_plan);
            if let Some(returning_rows) = returning_rows.as_mut() {
                candidate.row = match (returning_patch_columns.as_ref(), schema.as_deref()) {
                    (Some(assigned), Some(schema)) => update_returning_slot_row_patched(
                        &table,
                        &target_alias,
                        schema,
                        &record,
                        target_columns.as_ref(),
                        &candidate.row,
                        assigned,
                    ),
                    _ => update_returning_slot_row_from_assignment(
                        &table,
                        &target_alias,
                        schema.as_deref(),
                        &record,
                        target_columns.as_ref(),
                        candidate.columns.as_ref(),
                        &candidate.row,
                    )?,
                };
                returning_columns.get_or_insert_with(|| candidate.columns.clone());
                returning_rows.push(candidate.row.clone());
            }
            if let Some(before) = before {
                before_after_records.push((before, record.clone()));
            }
            updated_records.push(record);
            updated_record_snapshots.push(statement_snapshot);
            updated += 1;
        }
        if let Some(schema) = schema.as_deref() {
            validate_updated_records(
                self.db_ref(),
                self.tx.as_ref(),
                &table,
                schema,
                &updated_records,
                &before_after_records,
            )?;
            self.apply_foreign_key_parent_update_batch(&table, schema, &before_after_records)?;
        }
        self.enforce_rls_checks(
            &table,
            schema.as_deref(),
            PolicyAction::Update,
            &updated_records,
        )?;
        // The updated records are only read again for a RETURNING clause without
        // a FROM (the FROM case projects from slot rows, and a plain UPDATE never
        // reads them again). Project that result now, while the records are
        // still borrowed here, so the whole record vector can then MOVE into
        // the write instead of being deep-cloned for the projection.
        let returning_result = match update.returning.as_deref() {
            Some(returning) if from_rows.is_none() => {
                Some(self.project_returning_records_with_alias(
                    &table,
                    &target_alias,
                    schema.as_deref(),
                    returning,
                    &updated_records,
                )?)
            }
            _ => None,
        };
        if updated_record_repairs.iter().any(Option::is_some) {
            self.insert_session_records_with_repairs(
                &table,
                updated_records
                    .into_iter()
                    .zip(updated_record_snapshots.into_iter())
                    .zip(updated_record_repairs.into_iter())
                    .map(|((record, snapshot), repair)| (record, snapshot, repair)),
            )?;
        } else if updated_record_snapshots
            .iter()
            .any(|snapshot| *snapshot != 0)
        {
            self.insert_session_records_with_statement_snapshots(
                &table,
                updated_records
                    .into_iter()
                    .zip(updated_record_snapshots.into_iter()),
            )?;
        } else {
            self.insert_session_records(&table, updated_records)?;
        }
        self.publish_extension_database_events(
            &table,
            DatabaseOperation::Update,
            before_after_records
                .iter()
                .map(|(before, after)| (Some(before), Some(after))),
        )?;
        self.fire_after_row_triggers(
            &table,
            schema.as_deref(),
            "update",
            before_after_records
                .iter()
                .map(|(before, after)| (Some(before), Some(after))),
        )?;
        if let Some(returning) = &update.returning {
            if from_rows.is_some() {
                return self.project_update_from_returning_slot_rows(
                    &table,
                    schema.as_deref(),
                    returning,
                    returning_rows.as_deref().unwrap_or(&[]),
                    returning_columns
                        .as_deref()
                        .map(Vec::as_slice)
                        .unwrap_or(&[]),
                    target_columns.as_ref(),
                );
            }
            if let Some(result) = returning_result {
                return Ok(result);
            }
            return self.project_returning_records_with_alias(
                &table,
                &target_alias,
                schema.as_deref(),
                returning,
                &[],
            );
        }
        Ok(SqlResult::command(format!("UPDATE {updated}")))
    }

    /// Whether an UPDATE's consumers need each row's pre-update image: a
    /// unique key (including the primary key) that an assignment can change,
    /// an inbound foreign key referencing this table, a BEFORE/AFTER UPDATE
    /// row trigger, or an extension event binding on this relation. Conservative:
    /// anything not provably column-scoped counts as "needed".
    fn update_needs_before_image(
        &self,
        update: &sqlparser::ast::Update,
        table: &str,
        schema: &TableSchema,
    ) -> Result<bool> {
        let mut assigned = Vec::with_capacity(update.assignments.len());
        for assignment in &update.assignments {
            let AssignmentTarget::ColumnName(column_name) = &assignment.target else {
                return Ok(true);
            };
            assigned.push(relation_name(column_name)?);
        }
        let touches = |column: &str| assigned.iter().any(|a| a.eq_ignore_ascii_case(column));
        let primary_key_columns = primary_key_columns_for_schema(schema);
        fn index_field_touches(
            field: &IndexField,
            touches: &dyn Fn(&str) -> bool,
            primary_key_columns: &[String],
        ) -> bool {
            match field {
                IndexField::Id => primary_key_columns.iter().any(|c| touches(c)),
                IndexField::MetadataPath(path) => path.first().is_none_or(|head| touches(head)),
                IndexField::Lower(inner) | IndexField::Trim(inner) => {
                    index_field_touches(inner, touches, primary_key_columns)
                }
                IndexField::Timestamp | IndexField::Geometry => true,
            }
        }
        for arbiter in unique_arbiters_for_table(self.db_ref(), table, schema).iter() {
            let may_change = match &arbiter.key {
                UniqueKey::Columns(columns) => columns.iter().any(|c| touches(c)),
                UniqueKey::IndexFields(fields) => fields
                    .iter()
                    .any(|field| index_field_touches(field, &touches, &primary_key_columns)),
            };
            if may_change {
                return Ok(true);
            }
        }
        if !crate::catalog_memo::inbound_foreign_key_updates_shared(self.db_ref(), table)?
            .is_empty()
        {
            return Ok(true);
        }
        if self.has_row_triggers(table, "before", "update")?
            || self.has_row_triggers(table, "after", "update")?
        {
            return Ok(true);
        }
        self.has_extension_database_event_bindings(table, DatabaseOperation::Update)
    }

    pub(crate) fn update_assignment_candidates_from_rows(
        &self,
        table: &str,
        target_alias: &str,
        schema: Option<&TableSchema>,
        selection: Option<&Expr>,
        from_rows: &RowSet,
        target_columns: Arc<Vec<String>>,
        ctes: &BTreeMap<String, CteResult>,
    ) -> Result<Vec<UpdateAssignmentCandidate>> {
        let mut candidates = Vec::new();
        let mut seen_record_ids = BTreeSet::new();
        let mut fallback_records = None;
        let candidate_columns = Arc::new(merge_row_set_columns(
            target_columns.as_ref().clone(),
            from_rows.columns.clone(),
        ));
        // One engine for the whole FROM row set: building an engine per FROM
        // row re-cloned the CTE map and the security context and threw away
        // the engine's per-statement lookup and bound-context caches on
        // every row (ten times per TPC-C NEWORD stock update). Only the outer
        // slot row changes between iterations.
        let mut row_engine = self.sql_engine_with_ctes(ctes.clone());
        let cell_fields = schema
            .filter(|schema| {
                row_engine
                    .cell_rows_eligible(table, schema, PolicyAction::Update)
                    .unwrap_or(false)
            })
            .map(|schema| FieldRef::wildcard(Some(schema)));
        // Routine-owned full-primary-key UPDATE ... FROM: the plan (key columns
        // + key expressions bound over the FROM row layout and the routine
        // variables) is kept per IR node; each FROM row only evaluates the
        // key. The whole predicate is the key, so no per-row re-check.
        let mut from_point_template: Option<Rc<UpdatePointTemplate>> = None;
        if let (Some(fields_schema), Some(selection), true) =
            (schema, selection, cell_fields.is_some())
        {
            if let Some(key) = ir_update_plan::enabled()
                .then(|| row_engine.ir_plan_node_key(selection as *const Expr as usize))
                .flatten()
            {
                let template = match sql_update_plan_node_cache_get(key) {
                    Some(entry) => entry,
                    None => {
                        let built = row_engine
                            .build_update_point_template(
                                table,
                                target_alias,
                                fields_schema,
                                selection,
                                &from_rows.columns,
                            )?
                            .map(Rc::new);
                        sql_update_plan_node_cache_set(key, built.clone());
                        built
                    }
                };
                from_point_template =
                    template.filter(|template| template.outer_columns == from_rows.columns);
            }
        }
        for from_row in &from_rows.rows {
            row_engine.outer_row = Some(OuterSlotRow::from_slot(&from_rows.columns, from_row));
            let located = match (from_point_template.as_ref(), schema) {
                (Some(template), Some(fields_schema)) => Some(
                    row_engine
                        .update_point_record_id_for_row(template, table, fields_schema, from_row)?
                        .into_iter()
                        .collect::<Vec<String>>(),
                ),
                _ => row_engine.indexed_record_ids_for_table_selection(
                    table,
                    target_alias,
                    schema,
                    selection,
                )?,
            };
            let candidate_pairs: Vec<(SlotRow, Record)> = if let Some(ids) = located {
                sql_profile_index_lookup();
                match cell_fields.as_ref() {
                    // Cell path: no per-record policy pass (the statement-level
                    // verdict was Allow), slot rows from borrowed cells.
                    Some(fields) => {
                        let pairs = self.update_candidate_pairs_from_cells(
                            &row_engine,
                            table,
                            target_alias,
                            fields,
                            &ids,
                        )?;
                        sql_profile_sql_row_refs_materialized(pairs.iter().map(|(row, _)| row));
                        pairs
                    }
                    None => {
                        let records = self.session_records_for_action_ids_with_schema(
                            table,
                            PolicyAction::Update,
                            ids,
                            schema,
                        )?;
                        sql_profile_records_materialized(&records);
                        records
                            .into_iter()
                            .map(|record| {
                                Ok((
                                    slot_row_from_record(table, target_alias, schema, &record)?,
                                    record,
                                ))
                            })
                            .collect::<Result<Vec<_>>>()?
                    }
                }
            } else {
                if fallback_records.is_none() {
                    let records =
                        self.scan_session_records_for_action(table, PolicyAction::Update)?;
                    sql_profile_full_scan();
                    sql_profile_records_materialized(&records);
                    fallback_records = Some(records);
                }
                fallback_records
                    .clone()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|record| {
                        Ok((
                            slot_row_from_record(table, target_alias, schema, &record)?,
                            record,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?
            };
            for (idx, (target_row, record)) in candidate_pairs.into_iter().enumerate() {
                if idx % 1024 == 0 {
                    self.cancellation.check()?;
                }
                if seen_record_ids.contains(&record.id) {
                    continue;
                }
                let candidate_row =
                    merge_slot_rows(&target_row, &target_columns, from_row, &from_rows.columns);
                // The predicate recheck through the statement's engine (the
                // per-candidate engine it replaced had no outer row; clear it
                // for the evaluation so identifier resolution is identical).
                let matches = match selection {
                    None => true,
                    Some(_) if from_point_template.is_some() => true,
                    Some(selection) => {
                        let outer = row_engine.outer_row.take();
                        let (_scope, context) =
                            row_engine.bound_row_context(candidate_columns.as_ref());
                        let matches =
                            row_engine.eval_slot_row_predicate(&candidate_row, &context, selection);
                        row_engine.outer_row = outer;
                        matches?
                    }
                };
                if matches && seen_record_ids.insert(record.id.clone()) {
                    candidates.push(UpdateAssignmentCandidate {
                        record,
                        columns: candidate_columns.clone(),
                        row: candidate_row,
                    });
                }
            }
        }
        Ok(candidates)
    }

    pub(crate) fn update_assignment_candidates_without_from(
        &self,
        table: &str,
        target_alias: &str,
        schema: Option<&TableSchema>,
        selection: Option<&Expr>,
        target_columns: Arc<Vec<String>>,
        ctes: &BTreeMap<String, CteResult>,
    ) -> Result<Vec<UpdateAssignmentCandidate>> {
        let mut candidates = Vec::new();
        let predicate_covered_by_key = if let (Some(schema), Some(selection)) = (schema, selection)
        {
            self.sql_engine_with_ctes(ctes.clone())
                .exact_primary_key_selection_covers_predicate(
                    table,
                    target_alias,
                    schema,
                    selection,
                )?
        } else {
            false
        };
        let row_engine = self.sql_engine_with_ctes(ctes.clone());
        let cell_fields = schema
            .filter(|schema| {
                row_engine
                    .cell_rows_eligible(table, schema, PolicyAction::Update)
                    .unwrap_or(false)
            })
            .map(|schema| FieldRef::wildcard(Some(schema)));
        // Routine-owned full-primary-key UPDATE: the plan (key columns + bound
        // key expressions) is kept per IR node; this call only evaluates the
        // key. The whole predicate is the key, so no per-row re-check.
        let mut ir_point_ids: Option<Vec<String>> = None;
        if let (Some(fields_schema), Some(selection), true) =
            (schema, selection, cell_fields.is_some())
        {
            if let Some(key) = ir_update_plan::enabled()
                .then(|| row_engine.ir_plan_node_key(selection as *const Expr as usize))
                .flatten()
            {
                let template = match sql_update_plan_node_cache_get(key) {
                    Some(entry) => entry,
                    None => {
                        let built = row_engine
                            .build_update_point_template(
                                table,
                                target_alias,
                                fields_schema,
                                selection,
                                &[],
                            )?
                            .filter(|template| template.outer_columns.is_empty())
                            .map(Rc::new);
                        sql_update_plan_node_cache_set(key, built.clone());
                        built
                    }
                };
                if let Some(template) =
                    template.filter(|template| template.outer_columns.is_empty())
                {
                    ir_point_ids = Some(
                        row_engine
                            .update_point_record_id(&template, table, fields_schema)?
                            .into_iter()
                            .collect(),
                    );
                }
            }
        }
        let predicate_covered_by_key = predicate_covered_by_key || ir_point_ids.is_some();
        let candidate_pairs: Vec<(SlotRow, Record)> = match cell_fields.as_ref() {
            Some(fields) => match match ir_point_ids {
                Some(ids) => Some(ids),
                None => row_engine.indexed_record_ids_for_table_selection(
                    table,
                    target_alias,
                    schema,
                    selection,
                )?,
            } {
                Some(ids) => {
                    sql_profile_index_lookup();
                    let pairs = self.update_candidate_pairs_from_cells(
                        &row_engine,
                        table,
                        target_alias,
                        fields,
                        &ids,
                    )?;
                    sql_profile_sql_row_refs_materialized(pairs.iter().map(|(row, _)| row));
                    pairs
                }
                None => {
                    let records =
                        self.scan_session_records_for_action(table, PolicyAction::Update)?;
                    sql_profile_full_scan();
                    sql_profile_records_materialized(&records);
                    records
                        .into_iter()
                        .map(|record| {
                            Ok((
                                slot_row_from_record(table, target_alias, schema, &record)?,
                                record,
                            ))
                        })
                        .collect::<Result<Vec<_>>>()?
                }
            },
            None => self
                .update_candidate_records(table, target_alias, schema, selection, ctes)?
                .into_iter()
                .map(|record| {
                    Ok((
                        slot_row_from_record(table, target_alias, schema, &record)?,
                        record,
                    ))
                })
                .collect::<Result<Vec<_>>>()?,
        };
        for (idx, (target_row, record)) in candidate_pairs.into_iter().enumerate() {
            if idx % 1024 == 0 {
                self.cancellation.check()?;
            }
            let matches = predicate_covered_by_key
                || match selection {
                    None => true,
                    Some(selection) => {
                        let (_scope, context) =
                            row_engine.bound_row_context(target_columns.as_ref());
                        row_engine.eval_slot_row_predicate(&target_row, &context, selection)?
                    }
                };
            if matches {
                candidates.push(UpdateAssignmentCandidate {
                    record,
                    columns: target_columns.clone(),
                    row: target_row,
                });
            }
        }
        Ok(candidates)
    }

    /// Candidate target rows for `ids` through the cell path: the visible row
    /// (no JSON parse) builds the slot row from borrowed cells; the `Record`
    /// the assignments are applied to is materialized once per candidate.
    fn update_candidate_pairs_from_cells(
        &self,
        engine: &SqlEngine<'_>,
        table: &str,
        alias: &str,
        fields: &[FieldRef],
        ids: &[String],
    ) -> Result<Vec<(SlotRow, Record)>> {
        let visible = engine.visible_rows_for_pks(table, ids)?;
        let mut out = Vec::with_capacity(visible.len());
        let mut cells: bicdb_core::CellRow<'_> = Vec::with_capacity(32);
        let mut plan: Option<CellPlan> = None;
        for row in visible.iter().flatten() {
            let record = row.to_record().map_err(SqlError::from)?;
            let slot = match row {
                bicdb_core::VisibleRow::Stored(stored) => {
                    let fast = if stored.cells_into(&mut cells) {
                        slot_row_from_cells(table, alias, fields, stored, &cells, &mut plan)
                    } else {
                        None
                    };
                    match fast {
                        Some(slot) => slot,
                        None => slot_row_from_record_fields(table, alias, fields, &record)?,
                    }
                }
                bicdb_core::VisibleRow::Pending(_) => {
                    slot_row_from_record_fields(table, alias, fields, &record)?
                }
            };
            out.push((
                slot,
                Arc::try_unwrap(record).unwrap_or_else(|record| (*record).clone()),
            ));
        }
        Ok(out)
    }

    pub(crate) fn update_row_matches(
        &self,
        ctes: &BTreeMap<String, CteResult>,
        columns: &[String],
        row: &SlotRow,
        selection: Option<&Expr>,
    ) -> Result<bool> {
        let Some(selection) = selection else {
            return Ok(true);
        };
        let row_engine = self.sql_engine_with_ctes(ctes.clone());
        let (_scope, context) = row_engine.bound_row_context(columns);
        row_engine.eval_slot_row_predicate(row, &context, selection)
    }

    pub(crate) fn update_assignment_value(
        &mut self,
        ctes: &BTreeMap<String, CteResult>,
        columns: &[String],
        row: &SlotRow,
        expr: &Expr,
    ) -> Result<SqlValue> {
        let row_engine = self.sql_engine_with_ctes(ctes.clone());
        let (_scope, context) = row_engine.bound_row_context(columns);
        let row_value = row_engine.eval_slot_row_value(row, &context, expr);
        match row_value {
            Ok(value) => Ok(value),
            Err(SqlError::Unsupported(_)) => self.eval_session_expr(expr),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn update_from_row_set(
        &self,
        from: Option<&UpdateTableFromKind>,
        ctes: &BTreeMap<String, CteResult>,
    ) -> Result<Option<RowSet>> {
        let Some(from) = from else {
            return Ok(None);
        };
        let tables = match from {
            UpdateTableFromKind::BeforeSet(tables) | UpdateTableFromKind::AfterSet(tables) => {
                tables
            }
        };
        if tables.is_empty() {
            return Ok(None);
        }
        let engine = self.sql_engine_with_ctes(ctes.clone());
        let mut rows = vec![Vec::new()];
        let mut columns = Vec::new();
        for table in tables {
            let row_set = engine.row_set_from_table_with_joins(table)?;
            let mut merged_rows = Vec::new();
            for (idx, left) in rows.iter().enumerate() {
                if idx % 1024 == 0 {
                    self.cancellation.check()?;
                }
                for right in &row_set.rows {
                    merged_rows.push(merge_slot_rows(left, &columns, right, &row_set.columns));
                }
            }
            rows = merged_rows;
            columns = merge_row_set_columns(columns, row_set.columns);
            if rows.is_empty() {
                break;
            }
        }
        sql_profile_sql_rows_materialized(&rows);
        Ok(Some(RowSet { rows, columns }))
    }

    pub(crate) fn execute_delete(&mut self, delete: &Delete) -> Result<SqlResult> {
        self.execute_delete_with_ctes(delete, BTreeMap::new())
    }

    pub(crate) fn execute_delete_with_ctes(
        &mut self,
        delete: &Delete,
        ctes: BTreeMap<String, CteResult>,
    ) -> Result<SqlResult> {
        let (table, target_alias) = delete_from_table_and_alias(delete)?;
        let table = resolve_session_relation_name_if_exists(self.db_ref(), &table);
        self.require_table_privilege(&table, "DELETE")?;
        if delete.returning.is_some() {
            self.require_table_privilege(&table, "SELECT")?;
        }
        if load_view(self.db_ref(), &table)?.is_some() {
            return Err(SqlError::Unsupported(
                "DELETE from views is not supported".to_string(),
            ));
        }
        let schema = load_schema(self.db_ref(), &table)?;
        if let Some(schema_ref) = schema.as_ref() {
            if let Some(result) =
                self.try_execute_delete_stored(delete, &ctes, &table, &target_alias, schema_ref)?
            {
                return Ok(result);
            }
        }
        let using_rows = self.delete_using_row_set(delete.using.as_deref(), &ctes)?;
        let target_columns = Arc::new(row_output_columns(&table, &target_alias, schema.as_ref()));
        let records_to_delete = {
            let build_row_engine = |outer_row: Option<OuterSlotRow>| {
                let engine = self.sql_engine_with_ctes(ctes.clone());
                if let Some(outer_row) = outer_row {
                    engine.with_outer_slot_row(outer_row)
                } else {
                    engine
                }
            };
            let mut records = Vec::new();
            let mut seen_record_ids = BTreeSet::new();
            let mut fallback_records = None;
            if let Some(using_rows) = using_rows.as_ref() {
                let predicate_columns = Arc::new(merge_row_set_columns(
                    target_columns.as_ref().clone(),
                    using_rows.columns.clone(),
                ));
                for using_row in &using_rows.rows {
                    let using_outer_row = OuterSlotRow::from_slot(&using_rows.columns, using_row);
                    let row_engine = build_row_engine(Some(using_outer_row));
                    let candidate_records = if let Some(ids) = row_engine
                        .indexed_record_ids_for_table_selection(
                            &table,
                            &target_alias,
                            schema.as_ref(),
                            delete.selection.as_ref(),
                        )? {
                        sql_profile_index_lookup();
                        let records = self.session_records_for_action_ids_with_schema(
                            &table,
                            PolicyAction::Delete,
                            ids,
                            schema.as_ref(),
                        )?;
                        sql_profile_records_materialized(&records);
                        records
                    } else {
                        if fallback_records.is_none() {
                            let records =
                                self.scan_session_records_for_action(&table, PolicyAction::Delete)?;
                            sql_profile_full_scan();
                            sql_profile_records_materialized(&records);
                            fallback_records = Some(records);
                        }
                        fallback_records.clone().unwrap_or_default()
                    };
                    let predicate_covered_by_key = if let (Some(schema), Some(selection)) =
                        (schema.as_ref(), delete.selection.as_ref())
                    {
                        row_engine.exact_primary_key_selection_covers_predicate(
                            &table,
                            &target_alias,
                            schema,
                            selection,
                        )?
                    } else {
                        false
                    };
                    for (idx, record) in candidate_records.into_iter().enumerate() {
                        if idx % 1024 == 0 {
                            self.cancellation.check()?;
                        }
                        if seen_record_ids.contains(&record.id) {
                            continue;
                        }
                        let row_matches = if predicate_covered_by_key {
                            true
                        } else {
                            match delete.selection.as_ref() {
                                Some(selection) => {
                                    let target_row = slot_row_from_record(
                                        &table,
                                        &target_alias,
                                        schema.as_ref(),
                                        &record,
                                    )?;
                                    let row = merge_slot_rows(
                                        &target_row,
                                        target_columns.as_ref(),
                                        using_row,
                                        &using_rows.columns,
                                    );
                                    let (_scope, context) =
                                        row_engine.bound_row_context(predicate_columns.as_ref());
                                    row_engine.eval_slot_row_predicate(&row, &context, selection)?
                                }
                                None => true,
                            }
                        };
                        if row_matches && seen_record_ids.insert(record.id.clone()) {
                            let target_row = slot_row_from_record(
                                &table,
                                &target_alias,
                                schema.as_ref(),
                                &record,
                            )?;
                            let row = merge_slot_rows(
                                &target_row,
                                target_columns.as_ref(),
                                using_row,
                                &using_rows.columns,
                            );
                            records.push(DeleteCandidate {
                                record,
                                columns: predicate_columns.clone(),
                                row,
                            });
                        }
                    }
                }
            } else {
                let row_engine = build_row_engine(None);
                let candidate_records = if let Some(ids) = row_engine
                    .indexed_record_ids_for_table_selection(
                        &table,
                        &target_alias,
                        schema.as_ref(),
                        delete.selection.as_ref(),
                    )? {
                    sql_profile_index_lookup();
                    let records = self.session_records_for_action_ids_with_schema(
                        &table,
                        PolicyAction::Delete,
                        ids,
                        schema.as_ref(),
                    )?;
                    sql_profile_records_materialized(&records);
                    records
                } else {
                    let records =
                        self.scan_session_records_for_action(&table, PolicyAction::Delete)?;
                    sql_profile_full_scan();
                    sql_profile_records_materialized(&records);
                    records
                };
                let predicate_covered_by_key = if let (Some(schema), Some(selection)) =
                    (schema.as_ref(), delete.selection.as_ref())
                {
                    row_engine.exact_primary_key_selection_covers_predicate(
                        &table,
                        &target_alias,
                        schema,
                        selection,
                    )?
                } else {
                    false
                };
                for (idx, record) in candidate_records.into_iter().enumerate() {
                    if idx % 1024 == 0 {
                        self.cancellation.check()?;
                    }
                    let target_row =
                        slot_row_from_record(&table, &target_alias, schema.as_ref(), &record)?;
                    if !predicate_covered_by_key {
                        if let Some(selection) = &delete.selection {
                            let (_scope, context) =
                                row_engine.bound_row_context(target_columns.as_ref());
                            if !row_engine.eval_slot_row_predicate(
                                &target_row,
                                &context,
                                selection,
                            )? {
                                continue;
                            }
                        }
                    }
                    if seen_record_ids.insert(record.id.clone()) {
                        records.push(DeleteCandidate {
                            record,
                            columns: target_columns.clone(),
                            row: target_row,
                        });
                    }
                }
            }
            records
        };
        let mut deleted_records = Vec::new();
        // Like UPDATE, a DELETE that reads existing row values also has
        // SELECT policies applied to those reads.
        let reads_existing = delete
            .selection
            .as_ref()
            .is_some_and(expr_references_stored_columns)
            || delete
                .returning
                .as_deref()
                .is_some_and(select_items_reference_stored_columns)
            || using_rows.is_some();
        let records_to_delete = if reads_existing {
            self.retain_rls_select_visible(&table, schema.as_ref(), records_to_delete, |c| {
                &c.record
            })?
        } else {
            records_to_delete
        };
        let mut deleted_record_ids = Vec::new();
        let mut deleted_record_snapshots = Vec::new();
        let can_read_committed_recheck = self.tx.is_some();
        for (idx, mut candidate) in records_to_delete.into_iter().enumerate() {
            if idx % 1024 == 0 {
                self.cancellation.check()?;
            }
            let mut statement_snapshot = 0;
            if can_read_committed_recheck {
                let candidate_id = candidate.record.id.clone();
                let signed_grant = self.signed_mutation_grant(&table);
                let refreshed = match self.tx.as_mut() {
                    Some(tx) => match signed_grant {
                        Some(grant) => tx
                            .read_committed_delete_record_with_grant(grant, &table, &candidate_id)
                            .map_err(SqlError::from)?,
                        None => tx
                            .read_committed_delete_record(&table, &candidate_id)
                            .map_err(SqlError::from)?,
                    },
                    None => None,
                };
                if let Some((latest, snapshot)) = refreshed {
                    let Some(latest) = latest else {
                        continue;
                    };
                    let target_row =
                        slot_row_from_record(&table, &target_alias, schema.as_ref(), &latest)?;
                    let mut refreshed_row = candidate.row.clone();
                    for (idx, value) in target_row.into_iter().enumerate() {
                        if let Some(slot) = refreshed_row.get_mut(idx) {
                            *slot = value;
                        }
                    }
                    if let Some(selection) = delete.selection.as_ref() {
                        let row_engine = self.sql_engine_with_ctes(ctes.clone());
                        let (_scope, context) =
                            row_engine.bound_row_context(candidate.columns.as_ref());
                        if !row_engine.eval_slot_row_predicate(
                            &refreshed_row,
                            &context,
                            selection,
                        )? {
                            continue;
                        }
                    }
                    candidate.record = latest;
                    candidate.row = refreshed_row;
                    statement_snapshot = snapshot;
                }
            }
            let record = candidate.record;
            // BEFORE DELETE row triggers may veto this row.
            if !self.apply_before_delete_row_triggers(&table, schema.as_ref(), &record)? {
                continue;
            }
            deleted_record_ids.push(record.id.clone());
            deleted_record_snapshots.push(statement_snapshot);
            deleted_records.push(record);
        }
        if let Some(schema) = schema.as_ref() {
            self.apply_foreign_key_parent_delete_batch(&table, schema, &deleted_records)?;
        }
        let deleted = if deleted_record_snapshots
            .iter()
            .any(|snapshot| *snapshot != 0)
        {
            self.delete_session_records_with_statement_snapshots(
                &table,
                deleted_record_ids
                    .iter()
                    .cloned()
                    .zip(deleted_record_snapshots.into_iter()),
            )?
        } else {
            self.delete_session_records(&table, &deleted_record_ids)?
        };
        self.publish_extension_database_events(
            &table,
            DatabaseOperation::Delete,
            deleted_records.iter().map(|record| (Some(record), None)),
        )?;
        self.fire_after_row_triggers(
            &table,
            schema.as_ref(),
            "delete",
            deleted_records.iter().map(|record| (Some(record), None)),
        )?;
        if let Some(returning) = &delete.returning {
            return self.project_returning_records_with_alias(
                &table,
                &target_alias,
                schema.as_ref(),
                returning,
                &deleted_records,
            );
        }
        Ok(SqlResult::command(format!("DELETE {deleted}")))
    }

    pub(crate) fn delete_using_row_set(
        &self,
        using: Option<&[TableWithJoins]>,
        ctes: &BTreeMap<String, CteResult>,
    ) -> Result<Option<RowSet>> {
        let Some(tables) = using else {
            return Ok(None);
        };
        if tables.is_empty() {
            return Ok(None);
        }
        let engine = self.sql_engine_with_ctes(ctes.clone());
        let mut rows = vec![Vec::new()];
        let mut columns = Vec::new();
        for table in tables {
            let row_set = engine.row_set_from_table_with_joins(table)?;
            let mut merged_rows = Vec::new();
            for (idx, left) in rows.iter().enumerate() {
                if idx % 1024 == 0 {
                    self.cancellation.check()?;
                }
                for right in &row_set.rows {
                    merged_rows.push(merge_slot_rows(left, &columns, right, &row_set.columns));
                }
            }
            rows = merged_rows;
            columns = merge_row_set_columns(columns, row_set.columns);
            if rows.is_empty() {
                break;
            }
        }
        sql_profile_sql_rows_materialized(&rows);
        Ok(Some(RowSet { rows, columns }))
    }

    pub(crate) fn execute_truncate(&mut self, truncate: &Truncate) -> Result<SqlResult> {
        if truncate.partitions.is_some() {
            return Err(SqlError::Unsupported(
                "TRUNCATE PARTITION is not supported".to_string(),
            ));
        }
        if truncate.on_cluster.is_some() {
            return Err(SqlError::Unsupported(
                "TRUNCATE ON CLUSTER is not supported".to_string(),
            ));
        }
        let mut tables = Vec::new();
        for target in &truncate.table_names {
            tables.extend(truncate_target_tables(
                self.db_ref(),
                target,
                truncate.if_exists,
            )?);
        }
        // TRUNCATE destroys every row in a table and had NO authorization
        // check of any kind: any authenticated role could empty any
        // tenant's tables. PostgreSQL requires ownership or the TRUNCATE
        // privilege.
        for target in &tables {
            self.require_table_ownership(&target.name, "truncate it")?;
        }
        let cascade = matches!(truncate.cascade, Some(CascadeOption::Cascade));
        let schemas = list_schemas(self.db_ref())?;
        loop {
            let target_names = tables
                .iter()
                .map(|target| target.name.to_ascii_lowercase())
                .collect::<BTreeSet<_>>();
            let mut added = false;
            for child in &schemas {
                if target_names.contains(&child.name.to_ascii_lowercase()) {
                    continue;
                }
                let references_target = child.constraints.iter().any(|constraint| {
                    matches!(
                        constraint,
                        ConstraintSchema::ForeignKey { foreign_table, .. }
                            if target_names.contains(&foreign_table.to_ascii_lowercase())
                    )
                });
                if !references_target {
                    continue;
                }
                if !cascade {
                    return Err(SqlError::dependent_objects_still_exist(format!(
                        "cannot truncate a table referenced in a foreign key constraint; table \"{}\" references a target table",
                        child.name
                    )));
                }
                tables.push(TruncateTarget {
                    name: child.name.clone(),
                    schema: child.clone(),
                });
                added = true;
            }
            if !added {
                break;
            }
        }
        // ...and again on everything CASCADE pulled in. Checking only the
        // named targets was the whole bug: a caller who owns `bait` could
        // name it and have every table with a foreign key to it emptied,
        // whoever owned those. The children are truncated, so the children
        // must be authorized.
        for target in &tables {
            self.require_table_ownership(&target.name, "truncate it")?;
        }

        tables.sort_by(|left, right| {
            let left_references_right = left.schema.constraints.iter().any(|constraint| {
                matches!(
                    constraint,
                    ConstraintSchema::ForeignKey { foreign_table, .. }
                        if foreign_table.eq_ignore_ascii_case(&right.name)
                )
            });
            let right_references_left = right.schema.constraints.iter().any(|constraint| {
                matches!(
                    constraint,
                    ConstraintSchema::ForeignKey { foreign_table, .. }
                        if foreign_table.eq_ignore_ascii_case(&left.name)
                )
            });
            match (left_references_right, right_references_left) {
                (true, false) => std::cmp::Ordering::Less,
                (false, true) => std::cmp::Ordering::Greater,
                _ => left.name.cmp(&right.name),
            }
        });
        tables.dedup_by(|left, right| left.name.eq_ignore_ascii_case(&right.name));

        for target in tables {
            let records =
                self.scan_session_records_for_action(&target.name, PolicyAction::Delete)?;
            for (idx, record) in records.into_iter().enumerate() {
                if idx % 1024 == 0 {
                    self.cancellation.check()?;
                }
                self.delete_session_record(&target.name, &record.id)?;
            }
            if matches!(truncate.identity, Some(TruncateIdentityOption::Restart)) {
                restart_owned_sequences(self.db_mut()?, &target.schema)?;
            }
        }

        Ok(SqlResult::command("TRUNCATE TABLE"))
    }

    pub(crate) fn default_value_for_column(
        &mut self,
        schema: Option<&TableSchema>,
        column: &str,
    ) -> Result<Option<SqlValue>> {
        let Some(column_schema) = schema.and_then(|schema| schema.column(column)) else {
            return Ok(None);
        };
        if let Some(sequence) = column_schema.default_sequence.clone() {
            return Ok(Some(SqlValue::Int(self.nextval(&sequence)?)));
        }
        if let Some(default_value) = column_schema.default_value.clone() {
            return Ok(Some(default_value));
        }
        let Some(default_expr) = column_schema.effective_default_expr() else {
            return Ok(None);
        };
        self.eval_default_expr(&default_expr, &column_schema.pg_type)
            .map(Some)
    }

    pub(crate) fn eval_default_expr(
        &mut self,
        default_expr: &str,
        pg_type: &str,
    ) -> Result<SqlValue> {
        let expr = Self::parse_default_expr(default_expr)?;
        self.eval_parsed_default_expr(&expr, pg_type)
    }

    pub(crate) fn eval_parsed_default_expr(
        &mut self,
        expr: &Expr,
        pg_type: &str,
    ) -> Result<SqlValue> {
        let value = self.eval_session_expr(expr)?;
        if pg_type_spec(pg_type).is_some() {
            return cast_value_to_pg_type(value, pg_type);
        }
        if let Some(user_type) = list_user_types(self.db_ref())?
            .into_iter()
            .flat_map(|user_type| [user_type.column_type(false), user_type.column_type(true)])
            .find(|user_type| user_type.formatted_name() == pg_type)
        {
            cast_value_to_user_type(value, &user_type)
        } else {
            cast_value_to_pg_type(value, pg_type)
        }
    }

    pub(crate) fn parse_default_expr(default_expr: &str) -> Result<Expr> {
        let sql = format!("SELECT {default_expr}");
        let statements = parse_statements(&sql)?;
        let [Statement::Query(query)] = statements.as_slice() else {
            return Err(SqlError::InvalidSql(format!(
                "invalid column default expression {default_expr}"
            )));
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            return Err(SqlError::InvalidSql(format!(
                "invalid column default expression {default_expr}"
            )));
        };
        let [item] = select.projection.as_slice() else {
            return Err(SqlError::InvalidSql(format!(
                "invalid column default expression {default_expr}"
            )));
        };
        let (expr, _) = select_item_expr_and_alias(item)?;
        Ok(expr.clone())
    }

    pub(crate) fn eval_session_expr(&mut self, expr: &Expr) -> Result<SqlValue> {
        match expr {
            Expr::Value(value) => {
                if let Some(value) = routine_var_from_value(&self.routine_vars, value) {
                    return Ok(value);
                }
                literal_to_value(value)
            }
            Expr::TypedString(value) => typed_string_to_value_with_db(self.db_ref(), value),
            Expr::Identifier(ident) => {
                if let Some(value) = routine_var_from_ident(&self.routine_vars, ident) {
                    return Ok(value);
                }
                eval_constant_expr(expr)
            }
            Expr::CompoundIdentifier(idents) => {
                let parts = idents
                    .iter()
                    .map(|ident| ident.value.clone())
                    .collect::<Vec<_>>();
                routine_var_from_parts(&self.routine_vars, &parts)?.ok_or_else(|| {
                    SqlError::Unsupported(format!("unsupported value expression {expr}"))
                })
            }
            Expr::Function(function) if is_sequence_function(function)? => {
                self.eval_sequence_function(function)
            }
            Expr::Function(function) if is_set_config_function(function)? => {
                self.eval_set_config_function(function)
            }
            Expr::Function(function) => {
                let dispatch_key = crate::eval::expr_type_scope()
                    .map(|(generation, ir)| (generation, ir, function as *const Function as usize));
                let remembered = dispatch_key
                    .and_then(|key| FN_DISPATCH_MEMO.with(|memo| memo.borrow().get(&key).cloned()));
                if let Some(dispatch) = remembered.as_deref().filter(|d| d.stored) {
                    // Straight to the stored function: the earlier dispatchers
                    // are name-only and declined last time for this node.
                    let mut args = PooledArgs::take();
                    for arg in function_arg_list(function).iter() {
                        let value = self.eval_session_expr(arg)?;
                        args.push(value);
                    }
                    if let Some(value) = self.eval_stored_function_value(&dispatch.name, &args)? {
                        return Ok(value);
                    }
                }
                let name = match remembered.as_deref() {
                    Some(dispatch) => std::borrow::Cow::Borrowed(dispatch.name.as_str()),
                    None => object_name_lowercase(&function.name)?,
                };
                validate_common_type_expr(self.db_ref(), expr)?;
                let mut args = PooledArgs::take();
                let arg_exprs = function_arg_list(function);
                for arg in arg_exprs.iter() {
                    let value = self.eval_session_expr(arg)?;
                    args.push(value);
                }
                let arg_types = arg_exprs
                    .iter()
                    .map(|arg| projected_expr_pg_type_with_db(self.db_ref(), arg))
                    .collect::<Vec<_>>();
                if is_row_constructor(function) {
                    return Ok(anonymous_record_value(
                        std::mem::take(&mut *args),
                        arg_types,
                    ));
                }
                if matches!(name.as_ref(), "pg_typeof" | "pg_catalog.pg_typeof") {
                    return Ok(pg_typeof_result(
                        arg_types.first().and_then(Option::as_ref),
                        args.first(),
                    ));
                }
                if matches!(name.as_ref(), "txid_current" | "pg_catalog.txid_current") {
                    require_arg_count("txid_current", &args, 0)?;
                    let transaction_id = if let Some(tx) = &self.tx {
                        tx.id().0
                    } else {
                        // A standalone SELECT still has an implicit PostgreSQL
                        // transaction. Allocate its ID for this statement and
                        // release the read-only transaction immediately.
                        let tx = self.begin_session_transaction()?;
                        let transaction_id = tx.id().0;
                        tx.rollback()?;
                        transaction_id
                    };
                    return Ok(SqlValue::Int(transaction_id as i64));
                }
                if let Some(value) = eval_network_function_value(&name, &args, &arg_types)?
                    .or(eval_user_range_function_value(
                        self.db_ref(),
                        &name,
                        &args,
                        &arg_types,
                    )?)
                    .or(eval_range_function_value(&name, &args, &arg_types)?)
                {
                    return Ok(value);
                }
                if let Some(value) = self
                    .sql_engine()
                    .eval_runtime_function_value(&name, &args)?
                {
                    return Ok(value);
                }
                if let Some(value) = eval_session_function_value(&name, &args, &self.session_gucs)?
                {
                    return Ok(value);
                }
                if let Some(value) =
                    eval_privilege_function_value(self.db_ref(), &name, &args, &self.session_gucs)?
                {
                    return Ok(value);
                }
                if let Some(value) = eval_db_catalog_function_value(
                    self.db_ref(),
                    &name,
                    &args,
                    self.tx.as_ref().map(Transaction::visibility_watermark),
                    Some(&self.session_gucs),
                )? {
                    return Ok(value);
                }
                if let Some(value) = eval_broker_function_value(
                    self.db_ref(),
                    &name,
                    &args,
                    BrokerCaller::Sql {
                        context: self.security_context.as_ref(),
                        superuser: session_role_is_superuser(self.db_ref(), &self.session_gucs),
                    },
                    self.tx.as_ref(),
                )? {
                    return Ok(value);
                }
                if let Some(value) = eval_json_function_call_value(function, &args)? {
                    return Ok(value);
                }
                if let Some(value) = eval_catalog_function_value(&name, &args) {
                    return Ok(value);
                }
                if let Some(value) = eval_compatibility_function_value_with_db(
                    self.db_ref(),
                    &name,
                    &args,
                    Some(&arg_types),
                )? {
                    return Ok(value);
                }
                if let Some(value) = eval_spatial_function_value(&name, &args, &arg_types)? {
                    return Ok(value);
                }
                let stored = self.eval_stored_function_value(&name, &args)?;
                if let Some(key) = dispatch_key {
                    if remembered.is_none() {
                        let entry = std::rc::Rc::new(FnDispatch {
                            name: name.to_string(),
                            stored: stored.is_some(),
                        });
                        FN_DISPATCH_MEMO.with(|memo| {
                            let mut memo = memo.borrow_mut();
                            if memo.len() >= ROUTINE_NODE_MEMO_MAX {
                                memo.clear();
                            }
                            memo.insert(key, entry);
                        });
                    }
                }
                if let Some(value) = stored {
                    return Ok(value);
                }
                eval_builtin_function_value(function)
            }
            Expr::Cast {
                expr, data_type, ..
            } => {
                if let Some(source) = regclass_display_cast_source(expr, data_type) {
                    let value = self.eval_session_expr(source)?;
                    return regclass_text_value(self.db_ref(), value);
                }
                if let Some(source) = regtype_text_cast_source(expr, data_type)? {
                    let value = self.eval_session_expr(source)?;
                    return regtype_text_value(self.db_ref(), value);
                }
                if let Some(source) = regclass_text_cast_source(expr, data_type)? {
                    let value = self.eval_session_expr(source)?;
                    return regclass_text_value(self.db_ref(), value);
                }
                let value = self.eval_session_expr(expr)?;
                cast_expr_value_with_db(self.db_ref(), value, expr, data_type, None)
            }
            Expr::Position { expr, r#in } => eval_position_typed_value(
                self.eval_session_expr(expr)?,
                self.eval_session_expr(r#in)?,
                projected_expr_pg_type_with_db(self.db_ref(), expr).as_deref() == Some("bytea")
                    || projected_expr_pg_type_with_db(self.db_ref(), r#in).as_deref()
                        == Some("bytea"),
            ),
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                let bytea =
                    projected_expr_pg_type_with_db(self.db_ref(), expr).as_deref() == Some("bytea");
                eval_substring_expr(
                    expr,
                    substring_from.as_deref(),
                    substring_for.as_deref(),
                    |expr| self.eval_session_expr(expr),
                    bytea,
                )
            }
            Expr::Trim {
                trim_where,
                trim_what,
                expr,
                trim_characters,
            } => eval_trim_expr(
                trim_where.as_ref(),
                trim_what.as_deref(),
                expr,
                trim_characters.as_deref(),
                |expr| self.eval_session_expr(expr),
            ),
            Expr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => {
                let bytea =
                    projected_expr_pg_type_with_db(self.db_ref(), expr).as_deref() == Some("bytea");
                eval_overlay_expr(
                    expr,
                    overlay_what,
                    overlay_from,
                    overlay_for.as_deref(),
                    |expr| self.eval_session_expr(expr),
                    bytea,
                )
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => Ok(eval_in_list_truth(
                self.eval_session_expr(expr)?,
                list.iter()
                    .map(|candidate| self.eval_session_expr(candidate))
                    .collect::<Result<Vec<_>>>()?,
                *negated,
            )?
            .map(SqlValue::Bool)
            .unwrap_or(SqlValue::Null)),
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => Ok(eval_between_truth(
                self.eval_session_expr(expr)?,
                self.eval_session_expr(low)?,
                self.eval_session_expr(high)?,
                *negated,
            )?
            .map(SqlValue::Bool)
            .unwrap_or(SqlValue::Null)),
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                let left = sql_value_truth(self.eval_session_expr(left)?)?;
                if matches!(left, Some(false)) {
                    return Ok(SqlValue::Bool(false));
                }
                Ok(
                    sql_and(left, sql_value_truth(self.eval_session_expr(right)?)?)
                        .map(SqlValue::Bool)
                        .unwrap_or(SqlValue::Null),
                )
            }
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Or,
                right,
            } => {
                let left = sql_value_truth(self.eval_session_expr(left)?)?;
                if matches!(left, Some(true)) {
                    return Ok(SqlValue::Bool(true));
                }
                Ok(
                    sql_or(left, sql_value_truth(self.eval_session_expr(right)?)?)
                        .map(SqlValue::Bool)
                        .unwrap_or(SqlValue::Null),
                )
            }
            Expr::BinaryOp { left, op, right } => {
                let left_value = self.eval_session_expr(left)?;
                let right_value = self.eval_session_expr(right)?;
                let types = self.routine_binary_expr_types(left, op, right);
                if let Some(value) = eval_range_binary_value_with_db(
                    self.db_ref(),
                    left_value.clone(),
                    op,
                    right_value.clone(),
                    types.range_left.as_deref(),
                    types.range_right.as_deref(),
                )? {
                    Ok(value)
                } else {
                    eval_binary_expr_value_typed(
                        left,
                        op,
                        right,
                        left_value,
                        right_value,
                        None,
                        &types.binary,
                    )
                }
            }
            Expr::IsDistinctFrom(left, right) => {
                let mut eval = |expr: &Expr| self.eval_session_expr(expr);
                if let Some(not_distinct) = eval_tuple_not_distinct(left, right, &mut eval)? {
                    return Ok(SqlValue::Bool(!not_distinct));
                }
                Ok(SqlValue::Bool(!values_not_distinct(
                    &self.eval_session_expr(left)?,
                    &self.eval_session_expr(right)?,
                )))
            }
            Expr::IsNotDistinctFrom(left, right) => {
                let mut eval = |expr: &Expr| self.eval_session_expr(expr);
                if let Some(not_distinct) = eval_tuple_not_distinct(left, right, &mut eval)? {
                    return Ok(SqlValue::Bool(not_distinct));
                }
                Ok(SqlValue::Bool(values_not_distinct(
                    &self.eval_session_expr(left)?,
                    &self.eval_session_expr(right)?,
                )))
            }
            Expr::IsNull(expr) => Ok(SqlValue::Bool(value_is_null_predicate(
                &self.eval_session_expr(expr)?,
            ))),
            Expr::IsNotNull(expr) => Ok(SqlValue::Bool(value_is_not_null_predicate(
                &self.eval_session_expr(expr)?,
            ))),
            Expr::UnaryOp { op, expr } if op.to_string().eq_ignore_ascii_case("NOT") => {
                Ok(sql_not(sql_value_truth(self.eval_session_expr(expr)?)?)
                    .map(SqlValue::Bool)
                    .unwrap_or(SqlValue::Null))
            }
            Expr::UnaryOp { op, expr }
                if matches!(
                    op,
                    UnaryOperator::BitwiseNot | UnaryOperator::PGPrefixFactorial
                ) || is_geometric_unary_operator(op) =>
            {
                eval_unary_bit_not_expr_value(op, expr, self.eval_session_expr(expr)?, None)
            }
            Expr::Extract { field, expr, .. } => {
                eval_extract_value(field, self.eval_session_expr(expr)?)
            }
            Expr::Tuple(exprs) => {
                let values = exprs
                    .iter()
                    .map(|expr| self.eval_session_expr(expr))
                    .collect::<Result<Vec<_>>>()?;
                let pg_types = exprs
                    .iter()
                    .map(|expr| projected_expr_pg_type_with_db(self.db_ref(), expr))
                    .collect();
                Ok(anonymous_record_value(values, pg_types))
            }
            Expr::Array(array) => {
                validate_common_type_expr(self.db_ref(), expr)?;
                sql_array_value(
                    array
                        .elem
                        .iter()
                        .map(|expr| self.eval_session_expr(expr))
                        .collect::<Result<Vec<_>>>()?,
                )
            }
            Expr::CompoundFieldAccess { root, access_chain } => eval_access_chain_expr(
                root,
                access_chain,
                projected_expr_pg_type_with_db(self.db_ref(), root).as_deref() == Some("jsonb"),
                |expr| self.eval_session_expr(expr),
            ),
            Expr::Interval(interval) => {
                interval_literal_value(interval, |expr| self.eval_session_expr(expr))
            }
            Expr::Subquery(query) => self.execute_scalar_subquery(query),
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                if matches!(expr.as_ref(), Expr::Tuple(_)) {
                    return self.sql_engine().eval_row_value(
                        &SqlRow::default(),
                        &Expr::InSubquery {
                            expr: expr.clone(),
                            subquery: subquery.clone(),
                            negated: *negated,
                        },
                    );
                }
                let value = self.eval_session_expr(expr)?;
                let result = self.execute_query(subquery)?;
                if result.columns.len() != 1 {
                    return Err(SqlError::InvalidSql(
                        "IN subquery must return one column".into(),
                    ));
                }
                let mut unknown = false;
                for row in result.rows {
                    let candidate = &row[0];
                    if matches!(value, SqlValue::Null) || matches!(candidate, SqlValue::Null) {
                        unknown = true;
                    } else if values_equal(&value, candidate) {
                        return Ok(SqlValue::Bool(!*negated));
                    }
                }
                Ok(if unknown {
                    SqlValue::Null
                } else {
                    SqlValue::Bool(*negated)
                })
            }
            Expr::Exists { subquery, negated } => {
                let result = self.execute_query(subquery)?;
                Ok(SqlValue::Bool(result.rows.is_empty() == *negated))
            }
            Expr::Nested(expr) => self.eval_session_expr(expr),
            Expr::Collate { expr, collation } => {
                normalize_column_collation(collation)?;
                self.eval_session_expr(expr)
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                validate_common_type_expr(self.db_ref(), expr)?;
                let operand_value = operand
                    .as_deref()
                    .map(|operand| self.eval_session_expr(operand))
                    .transpose()?;
                for condition in conditions {
                    let matched = if let Some(operand_value) = &operand_value {
                        values_equal(
                            operand_value,
                            &self.eval_session_expr(&condition.condition)?,
                        )
                    } else {
                        sql_value_truth(self.eval_session_expr(&condition.condition)?)?
                            .unwrap_or(false)
                    };
                    if matched {
                        return self.eval_session_expr(&condition.result);
                    }
                }
                else_result
                    .as_deref()
                    .map(|result| self.eval_session_expr(result))
                    .unwrap_or(Ok(SqlValue::Null))
            }
            _ => eval_constant_expr(expr),
        }
    }

    pub(crate) fn execute_scalar_subquery(&mut self, query: &Query) -> Result<SqlValue> {
        sql_profile_scalar_subquery();
        let started = Instant::now();
        let result = self.execute_query(query);
        sql_profile_scalar_subquery_elapsed(started);
        let result = result?;
        if result.columns.len() != 1 {
            return Err(SqlError::InvalidSql(
                "scalar subquery must return one column".to_string(),
            ));
        }
        match result.rows.as_slice() {
            [] => Ok(SqlValue::Null),
            [row] => Ok(row.first().cloned().unwrap_or(SqlValue::Null)),
            _ => Err(SqlError::InvalidSql(
                "scalar subquery returned more than one row".to_string(),
            )),
        }
    }

    pub(crate) fn eval_set_config_function(&mut self, function: &Function) -> Result<SqlValue> {
        let args = function_args(function);
        if args.len() != 3 {
            return Err(SqlError::InvalidSql(format!(
                "set_config expects 3 arguments, got {}",
                args.len()
            )));
        }
        let SqlValue::String(setting) = self.eval_session_expr(&args[0])? else {
            return Err(SqlError::InvalidSql(
                "set_config name must be text".to_string(),
            ));
        };
        let value = self.eval_session_expr(&args[1])?.to_cell();
        let SqlValue::Bool(local) = self.eval_session_expr(&args[2])? else {
            return Err(SqlError::InvalidSql(
                "set_config is_local must be boolean".to_string(),
            ));
        };
        let setting = setting.to_ascii_lowercase();
        if is_protected_security_setting(&setting)
            && self.signed_application_security_guc_compatibility
        {
            return Ok(SqlValue::String(value));
        }
        let targets = GucAssignmentTargets::setting(&setting);
        self.apply_scoped_guc_change(local, targets, |session| {
            session.apply_setting_value(setting, SqlValue::String(value.clone()))
        })?;
        Ok(SqlValue::String(value))
    }

    pub(crate) fn eval_sequence_function(&mut self, function: &Function) -> Result<SqlValue> {
        let name = object_name(&function.name)?.to_ascii_lowercase();
        let args = function_args(function);
        match name.as_str() {
            "nextval" | "pg_catalog.nextval" => {
                let sequence =
                    normalize_sequence_name(&self.eval_sequence_arg(args.first(), "nextval")?);
                Ok(SqlValue::Int(self.nextval(&sequence)?))
            }
            "currval" | "pg_catalog.currval" => {
                let sequence =
                    normalize_sequence_name(&self.eval_sequence_arg(args.first(), "currval")?);
                let key = sequence.clone();
                self.currvals
                    .get(&key)
                    .copied()
                    .map(SqlValue::Int)
                    .ok_or_else(|| {
                        SqlError::InvalidSql(format!(
                            "currval of sequence \"{sequence}\" is not yet defined in this session"
                        ))
                    })
            }
            "lastval" | "pg_catalog.lastval" => {
                if !args.is_empty() {
                    return Err(SqlError::InvalidSql(
                        "lastval expects no arguments".to_string(),
                    ));
                }
                self.session_gucs
                    .get(LASTVAL_SESSION_KEY)
                    .and_then(|value| value.parse::<i64>().ok())
                    .map(SqlValue::Int)
                    .ok_or_else(|| {
                        SqlError::InvalidSql(
                            "lastval is not yet defined in this session".to_string(),
                        )
                    })
            }
            "setval" | "pg_catalog.setval" => {
                if args.len() != 2 && args.len() != 3 {
                    return Err(SqlError::InvalidSql(
                        "setval expects 2 or 3 arguments".to_string(),
                    ));
                }
                let sequence =
                    normalize_sequence_name(&self.eval_sequence_arg(args.first(), "setval")?);
                let value = self.eval_session_expr(&args[1])?;
                let Some(value) = sql_value_i64(&value) else {
                    return Err(SqlError::InvalidSql(
                        "setval value must be an integer".to_string(),
                    ));
                };
                let is_called = if let Some(expr) = args.get(2) {
                    let SqlValue::Bool(value) = self.eval_session_expr(expr)? else {
                        return Err(SqlError::InvalidSql(
                            "setval is_called must be boolean".to_string(),
                        ));
                    };
                    value
                } else {
                    true
                };
                self.setval(&sequence, value, is_called)?;
                Ok(SqlValue::Int(value))
            }
            _ => unreachable!(),
        }
    }

    pub(crate) fn eval_stored_function_value(
        &mut self,
        name: &str,
        args: &[SqlValue],
    ) -> Result<Option<SqlValue>> {
        let Some(routine) = resolve_routine_cached(self.db_ref(), RoutineKind::Function, name)?
        else {
            return Ok(None);
        };
        self.ensure_routine_execute_privilege(name)?;
        if let Some(value) = self.eval_inline_return_function(&routine, args)? {
            return Ok(Some(value));
        }
        self.execute_with_routine_security(&routine.schema, |session| {
            if routine.schema.language.eq_ignore_ascii_case("sql") {
                session.execute_sql_function_value(&routine.schema, &routine.ir, args)
            } else {
                session.execute_plpgsql_function_value(&routine.schema, &routine.ir, args)
            }
        })
        .map(Some)
    }

    /// `RETURN <expr>`-only functions evaluate the bound body directly over
    /// their arguments (see `inline_return_slots`). Only inside a routine's
    /// expression-type scope: the memo keys on its generation, and outside a
    /// routine the call is not on a hot path.
    fn eval_inline_return_function(
        &mut self,
        routine: &CachedRoutine,
        args: &[SqlValue],
    ) -> Result<Option<SqlValue>> {
        if !crate::inline_return_fns::enabled() {
            return Ok(None);
        }
        let Some((generation, _)) = crate::eval::expr_type_scope() else {
            return Ok(None);
        };
        let ir: &RoutineIR = &routine.ir;
        let key = (generation, ir as *const RoutineIR as usize, args.len());
        let slots = INLINE_RETURN_MEMO.with(|memo| {
            if let Some(hit) = memo.borrow().get(&key) {
                return hit.clone();
            }
            let built = inline_return_slots(&routine.schema, ir, args.len())
                .map(|slots| std::rc::Rc::from(slots.into_boxed_slice()));
            let mut memo = memo.borrow_mut();
            if memo.len() >= ROUTINE_NODE_MEMO_MAX {
                memo.clear();
            }
            memo.insert(key, built.clone());
            built
        });
        let Some(slots) = slots else {
            return Ok(None);
        };
        let RoutineStmt::Return(Some(expr)) = &ir.statements[0] else {
            return Ok(None);
        };
        let Some(bound) = &expr.bound else {
            return Ok(None);
        };
        // Small scalar helpers dominate procedural OLTP call counts (TPC-C's
        // DBMS_RANDOM alone is invoked dozens of times per NewOrder). Keep
        // their bound-variable frame inline instead of performing one heap
        // allocation for every otherwise-inlined function call. Unusually
        // wide helpers spill transparently.
        let vars: smallvec::SmallVec<[SqlValue; 16]> = slots
            .iter()
            .map(|slot| slot.map_or(SqlValue::Null, |arg| args[arg].clone()))
            .collect();
        let empty_row = SqlRow::default();
        let value = bound.eval(&BoundExprFrame {
            db: self.db_ref(),
            columns: BoundExprColumns::Row {
                row: &empty_row,
                column_keys: &[],
            },
            vars: &vars,
            user_calls: &[],
        })?;
        crate::inline_return_fns::record_hit();
        cast_routine_return_value(value, &routine.schema).map(Some)
    }

    pub(crate) fn execute_with_routine_security<T>(
        &mut self,
        routine: &RoutineSchema,
        execute: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        if !routine.security_definer {
            return execute(self);
        }
        // No recorded owner -> invoker semantics, so the body runs with the
        // caller's own authority rather than a superuser's.
        let Some(owner) = routine.definer_owner().map(str::to_string) else {
            return execute(self);
        };
        let previous =
            Arc::make_mut(&mut self.session_gucs).insert(CURRENT_ROLE_GUC.to_string(), owner);
        let result = execute(self);
        match previous {
            Some(role) => {
                Arc::make_mut(&mut self.session_gucs).insert(CURRENT_ROLE_GUC.to_string(), role);
            }
            None => {
                Arc::make_mut(&mut self.session_gucs).remove(CURRENT_ROLE_GUC);
            }
        }
        result
    }

    pub(crate) fn ensure_routine_execute_privilege(&self, name: &str) -> Result<()> {
        let role = current_user_from_gucs(&self.session_gucs);
        if role_can_execute_routine(self.db_ref(), &role, name)? {
            return Ok(());
        }
        Err(SqlError::BicDb(BicDbError::Authorization(format!(
            "permission denied for function {}",
            normalize_object_name(name)
        ))))
    }

    pub(crate) fn execute_plpgsql_procedure(
        &mut self,
        ir: &RoutineIR,
        args: &[SqlValue],
    ) -> Result<SqlResult> {
        if self.tx.is_none()
            && (routine_declarations_may_write(&ir.declarations)
                || routine_block_may_write(&ir.statements)
                || routine_handlers_may_write(&ir.exception_handlers))
        {
            return self.execute_plpgsql_procedure_in_auto_transaction(ir, args);
        }
        self.execute_plpgsql_procedure_ir(ir, args)
    }

    pub(crate) fn execute_plpgsql_procedure_in_auto_transaction(
        &mut self,
        ir: &RoutineIR,
        args: &[SqlValue],
    ) -> Result<SqlResult> {
        let ddl_undo_len = self.ddl_undo.len();
        let savepoint_len = self.savepoints.len();
        self.tx = Some(self.begin_session_transaction()?);
        let result = self
            .execute_plpgsql_procedure_ir(ir, args)
            // The auto-transaction commits below without passing through
            // `commit()`, so deferred constraint triggers queued inside the
            // procedure drain here — still inside the open transaction.
            .and_then(|result| {
                self.fire_deferred_row_triggers()?;
                Ok(result)
            });
        match result {
            Ok(result) => {
                if self.defer_commit {
                    // Leave the transaction pending in `self.tx`; the caller
                    // (holding the database write lock) applies it via
                    // commit_buffered_transaction. A shared session cannot have
                    // performed DDL (db_mut errors), so there is no DDL undo to
                    // reconcile here.
                    self.savepoints.truncate(savepoint_len);
                    return Ok(result);
                }
                if let Some(tx) = self.tx.take() {
                    let started = Instant::now();
                    let commit_result = tx.commit();
                    sql_profile_write_elapsed(started);
                    if let Err(error) = commit_result {
                        self.rollback_ddl_to_len(ddl_undo_len)?;
                        self.savepoints.truncate(savepoint_len);
                        return Err(error.into());
                    }
                }
                self.ddl_undo.truncate(ddl_undo_len);
                self.savepoints.truncate(savepoint_len);
                Ok(result)
            }
            Err(error) => {
                let rollback = self.tx.take().map(Transaction::rollback).transpose();
                let ddl_rollback = self.rollback_ddl_to_len(ddl_undo_len);
                self.savepoints.truncate(savepoint_len);
                rollback.map_err(SqlError::from)?;
                ddl_rollback?;
                Err(error)
            }
        }
    }

    /// Run a lowered procedure as WASM (Slice C). The embedded SQL re-enters
    /// this session via a host holding a raw `*mut self`; on an embedded error
    /// the run aborts and the recorded error is propagated (so an enclosing
    /// transaction rolls back exactly as the interpreter path would).
    #[cfg(feature = "adaptive-procs")]
    pub(crate) fn run_wasm_procedure(
        &mut self,
        int_args: &[i64],
        compiled: &adaptive::lower::CompiledProc,
    ) -> Result<SqlResult> {
        let sptr = std::ptr::addr_of_mut!(*self);
        let mut emb_err: Option<SqlError> = None;
        // SAFETY: `self` and `emb_err` live for this whole call and are not
        // touched again until `run` returns (control is in WASM until then).
        let host = unsafe { adaptive::lower::host_for(sptr, &mut emb_err, compiled) };
        match compiled.module.run(int_args, host) {
            Ok((out, _)) => Ok(adaptive::lower::build_result(&compiled.output_names, &out)),
            Err(_) => {
                if let Some(error) = emb_err.take() {
                    return Err(error);
                }
                // Codegen emits only total ops, so a trap without a recorded
                // embedded error is unexpected; surface it rather than risk a
                // double-applied re-run on the interpreter.
                Err(SqlError::InvalidSql(
                    "adaptive wasm procedure trapped unexpectedly".to_string(),
                ))
            }
        }
    }

    /// Auto-transaction wrapper for [`Self::run_wasm_procedure`], mirroring
    /// [`Self::execute_plpgsql_procedure_in_auto_transaction`].
    #[cfg(feature = "adaptive-procs")]
    pub(crate) fn run_wasm_procedure_in_auto_transaction(
        &mut self,
        int_args: &[i64],
        compiled: &adaptive::lower::CompiledProc,
    ) -> Result<SqlResult> {
        let ddl_undo_len = self.ddl_undo.len();
        let savepoint_len = self.savepoints.len();
        self.tx = Some(self.begin_session_transaction()?);
        let result = self.run_wasm_procedure(int_args, compiled);
        match result {
            Ok(result) => {
                if self.defer_commit {
                    self.savepoints.truncate(savepoint_len);
                    return Ok(result);
                }
                if let Some(tx) = self.tx.take() {
                    let started = Instant::now();
                    let commit_result = tx.commit();
                    sql_profile_write_elapsed(started);
                    if let Err(error) = commit_result {
                        self.rollback_ddl_to_len(ddl_undo_len)?;
                        self.savepoints.truncate(savepoint_len);
                        return Err(error.into());
                    }
                }
                self.ddl_undo.truncate(ddl_undo_len);
                self.savepoints.truncate(savepoint_len);
                Ok(result)
            }
            Err(error) => {
                let rollback = self.tx.take().map(Transaction::rollback).transpose();
                let ddl_rollback = self.rollback_ddl_to_len(ddl_undo_len);
                self.savepoints.truncate(savepoint_len);
                rollback.map_err(SqlError::from)?;
                ddl_rollback?;
                Err(error)
            }
        }
    }

    pub(crate) fn execute_plpgsql_procedure_ir(
        &mut self,
        ir: &RoutineIR,
        args: &[SqlValue],
    ) -> Result<SqlResult> {
        let previous_ir = self
            .current_routine_ir
            .replace(ir as *const RoutineIR as usize);
        let _type_scope = self.enter_routine_expr_type_scope(ir);
        let executed = self.execute_plpgsql_procedure_ir_inner(ir, args);
        self.current_routine_ir = previous_ir;
        executed
    }

    fn execute_plpgsql_procedure_ir_inner(
        &mut self,
        ir: &RoutineIR,
        args: &[SqlValue],
    ) -> Result<SqlResult> {
        let mut frame = self.routine_frame_for_call(ir, args)?;
        self.initialize_routine_frame(&mut frame, &ir.declarations)?;
        self.execute_routine_block(&mut frame, &ir.statements, &ir.exception_handlers)?;
        if frame.output_names.is_empty() {
            Ok(SqlResult::command("CALL"))
        } else {
            let row = frame
                .output_names
                .iter()
                .map(|name| frame.get(name))
                .collect::<Vec<_>>();
            Ok(SqlResult::new(frame.output_names.clone(), vec![row]))
        }
    }

    pub(crate) fn execute_plpgsql_function_value(
        &mut self,
        routine: &RoutineSchema,
        ir: &RoutineIR,
        args: &[SqlValue],
    ) -> Result<SqlValue> {
        let previous_ir = self
            .current_routine_ir
            .replace(ir as *const RoutineIR as usize);
        let _type_scope = self.enter_routine_expr_type_scope(ir);
        let executed = self.execute_plpgsql_function_value_inner(routine, ir, args);
        self.current_routine_ir = previous_ir;
        executed
    }

    /// A call frame from the routine's cached template (built once per
    /// compiled routine, keyed like the expression-type memo).
    pub(crate) fn routine_frame_for_call(
        &self,
        ir: &RoutineIR,
        args: &[SqlValue],
    ) -> Result<RoutineFrame> {
        let generation = crate::eval::expr_type_scope()
            .map(|(generation, _)| generation)
            .unwrap_or_else(|| self.db_ref().collection_generation(ROUTINE_COLLECTION));
        let key = (generation, ir as *const RoutineIR as usize);
        let template = FRAME_TEMPLATE_MEMO.with(|memo| {
            if let Some(hit) = memo.borrow().get(&key) {
                return std::rc::Rc::clone(hit);
            }
            let built = std::rc::Rc::new(RoutineFrameTemplate::build_with_declarations(
                &ir.params,
                &ir.symbol_names,
                &ir.declarations,
            ));
            let mut memo = memo.borrow_mut();
            if memo.len() >= ROUTINE_NODE_MEMO_MAX {
                memo.clear();
            }
            memo.insert(key, std::rc::Rc::clone(&built));
            built
        });
        RoutineFrame::from_template(&template, args)
    }

    /// One generation read per routine call covers both catalogs whose
    /// contents decide an expression's type: routines (recompiles) and the
    /// table schema (DDL). Per-row memos inside the call key on it.
    pub(crate) fn enter_routine_expr_type_scope(
        &self,
        ir: &RoutineIR,
    ) -> crate::eval::ExprTypeScopeGuard {
        let db = self.db_ref();
        // The user-type catalog joins the key: the catalog-aware typer
        // (`projected_expr_pg_type_with_db`) memoizes under it too.
        let generation = db.collection_generation(ROUTINE_COLLECTION)
            ^ db.collection_generation(SCHEMA_COLLECTION).rotate_left(32)
            ^ db.collection_generation(USER_TYPE_COLLECTION)
                .rotate_left(16);
        crate::eval::enter_expr_type_scope(generation, ir as *const RoutineIR as usize)
    }

    /// Per-node memo of the inferred types a routine expression's binary
    /// operator needs (see `binary_expr_types` and the range check in
    /// `eval_session_expr`). A PL/pgSQL body evaluates the same node on every
    /// loop iteration and every call; inferring types walks the whole
    /// subtree each time (and was 12% of TPC-C server CPU on dev9). Keyed by
    /// (routine generation, IR identity, node address) — see
    /// `Session::current_routine_ir` for why that is stable.
    pub(crate) fn routine_binary_expr_types(
        &self,
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
    ) -> std::rc::Rc<RoutineBinaryExprTypes> {
        let Some(ir) = self.current_routine_ir else {
            return std::rc::Rc::new(RoutineBinaryExprTypes::compute(
                self.db_ref(),
                left,
                op,
                right,
            ));
        };
        let generation = self.db_ref().collection_generation(ROUTINE_COLLECTION);
        let key = (generation, ir, left as *const Expr as usize);
        if let Some(hit) = ROUTINE_EXPR_TYPE_MEMO.with(|memo| memo.borrow().get(&key).cloned()) {
            return hit;
        }
        let types = std::rc::Rc::new(RoutineBinaryExprTypes::compute(
            self.db_ref(),
            left,
            op,
            right,
        ));
        ROUTINE_EXPR_TYPE_MEMO.with(|memo| {
            let mut memo = memo.borrow_mut();
            if memo.len() >= ROUTINE_EXPR_TYPE_MEMO_MAX {
                memo.clear();
            }
            memo.insert(key, std::rc::Rc::clone(&types));
        });
        types
    }

    fn execute_plpgsql_function_value_inner(
        &mut self,
        routine: &RoutineSchema,
        ir: &RoutineIR,
        args: &[SqlValue],
    ) -> Result<SqlValue> {
        if routine.returns_set {
            return Err(SqlError::UnsupportedPlpgsqlFeature(format!(
                "set-returning stored routine {} is not implemented",
                routine.name
            )));
        }
        let mut frame = self.routine_frame_for_call(ir, args)?;
        self.initialize_routine_frame(&mut frame, &ir.declarations)?;
        let value = match self.execute_routine_block(
            &mut frame,
            &ir.statements,
            &ir.exception_handlers,
        )? {
            RoutineControl::NextIteration => {
                Err(SqlError::InvalidSql("CONTINUE escaped a loop".into()))
            }
            RoutineControl::Return(Some(value)) => Ok(value),
            RoutineControl::Return(None) => Ok(SqlValue::Null),
            RoutineControl::Continue => {
                if frame.output_names.len() == 1 {
                    Ok(frame.get(&frame.output_names[0]))
                } else if frame.output_names.is_empty() && routine.return_type == "void" {
                    Ok(SqlValue::Null)
                } else if frame.output_names.is_empty() {
                    Err(SqlError::UnsupportedPlpgsqlFeature(format!(
                        "stored function {} completed without RETURN",
                        routine.name
                    )))
                } else {
                    Err(SqlError::UnsupportedPlpgsqlFeature(format!(
                        "stored function {} has multiple output values but scalar execution was requested",
                        routine.name
                    )))
                }
            }
        }?;
        cast_routine_return_value(value, routine)
    }

    pub(crate) fn execute_sql_function_value(
        &mut self,
        routine: &RoutineSchema,
        ir: &RoutineIR,
        args: &[SqlValue],
    ) -> Result<SqlValue> {
        if routine.returns_set {
            return Err(SqlError::UnsupportedPlpgsqlFeature(format!(
                "set-returning stored routine {} is not implemented",
                routine.name
            )));
        }
        let mut frame = self.routine_frame_for_call(ir, args)?;
        let mut result = None;
        for statement in &ir.statements {
            let RoutineStmt::Sql(statement) = statement else {
                return Err(SqlError::UnsupportedPlpgsqlFeature(format!(
                    "SQL function {} contains a non-SQL statement",
                    routine.name
                )));
            };
            result = Some(self.execute_routine_sql(&mut frame, statement)?);
        }
        let value = result
            .and_then(|result| result.rows.into_iter().next())
            .and_then(|row| row.into_iter().next())
            .unwrap_or(SqlValue::Null);
        cast_routine_return_value(value, routine)
    }

    pub(crate) fn initialize_routine_frame(
        &mut self,
        frame: &mut RoutineFrame,
        declarations: &[RoutineDecl],
    ) -> Result<()> {
        // PostgreSQL initializes FOUND to false on entry to every plpgsql
        // block; each SELECT INTO / PERFORM / DML statement then overwrites it.
        frame.set(ROUTINE_FOUND_VAR, SqlValue::Bool(false));
        for declaration in declarations {
            match declaration {
                RoutineDecl::Alias { name, position } => {
                    let value = frame
                        .positional
                        .get(position.saturating_sub(1))
                        .cloned()
                        .unwrap_or(SqlValue::Null);
                    frame.set(name, value);
                }
                RoutineDecl::Variable {
                    name, default_expr, ..
                } => {
                    let value = default_expr
                        .as_ref()
                        .map(|expr| self.eval_routine_expr(frame, expr))
                        .transpose()?
                        .unwrap_or(SqlValue::Null);
                    frame.assign(name, value)?;
                }
                RoutineDecl::Cursor { name, query } => {
                    frame.cursors.insert(
                        normalize_object_name(name),
                        RoutineCursor {
                            query: query.clone(),
                            rows: Vec::new(),
                            position: 0,
                            open: false,
                        },
                    );
                }
            }
        }
        Ok(())
    }

    pub(crate) fn execute_routine_block(
        &mut self,
        frame: &mut RoutineFrame,
        statements: &[RoutineStmt],
        handlers: &[RoutineExceptionHandler],
    ) -> Result<RoutineControl> {
        if handlers.is_empty() {
            return self.execute_routine_statements(frame, statements);
        }
        let transaction = self.tx.as_ref().map(Transaction::rollback_mark);
        let ddl_undo_len = self.ddl_undo.len();
        let savepoint_len = self.savepoints.len();
        match self.execute_routine_statements(frame, statements) {
            Ok(control) => Ok(control),
            Err(error) => self.execute_routine_exception_handler(
                frame,
                handlers,
                error,
                transaction,
                ddl_undo_len,
                savepoint_len,
            ),
        }
    }

    pub(crate) fn execute_routine_exception_handler(
        &mut self,
        frame: &mut RoutineFrame,
        handlers: &[RoutineExceptionHandler],
        error: SqlError,
        transaction: Option<bicdb_core::TransactionRollbackMark>,
        ddl_undo_len: usize,
        savepoint_len: usize,
    ) -> Result<RoutineControl> {
        let sqlstate = error.sqlstate().to_string();
        let Some(handler) = handlers
            .iter()
            .find(|handler| routine_exception_matches(handler, &sqlstate))
        else {
            return Err(error);
        };
        let rollback_started = routine_outcome_trace_enabled().then(Instant::now);
        let discarded_writes = if rollback_started.is_some() {
            self.tx.as_ref().map_or(0, |tx| {
                tx.write_len()
                    .saturating_sub(transaction.map_or(0, |mark| mark.write_len()))
            })
        } else {
            0
        };
        if let (Some(tx), Some(transaction)) = (self.tx.as_mut(), transaction) {
            tx.rollback_to_mark(transaction)?;
        }
        self.rollback_ddl_to_len(ddl_undo_len)?;
        self.savepoints.truncate(savepoint_len);
        if let Some(started) = rollback_started {
            crate::routine_outcome::record_routine_rollback(
                &sqlstate,
                discarded_writes,
                started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
            );
        }
        record_handled_routine_exception(&sqlstate);
        self.execute_routine_statements(frame, &handler.statements)
    }

    pub(crate) fn execute_routine_statements(
        &mut self,
        frame: &mut RoutineFrame,
        statements: &[RoutineStmt],
    ) -> Result<RoutineControl> {
        for statement in statements {
            match self.execute_routine_statement(frame, statement)? {
                RoutineControl::Continue => {}
                returned => return Ok(returned),
            }
        }
        Ok(RoutineControl::Continue)
    }

    pub(crate) fn execute_routine_statement(
        &mut self,
        frame: &mut RoutineFrame,
        statement: &RoutineStmt,
    ) -> Result<RoutineControl> {
        let mut profile = SqlProfileScope::new_routine_statement(statement);
        let result = self.execute_routine_statement_inner(frame, statement);
        profile.finish_control(&result);
        result
    }

    pub(crate) fn execute_routine_statement_inner(
        &mut self,
        frame: &mut RoutineFrame,
        statement: &RoutineStmt,
    ) -> Result<RoutineControl> {
        match statement {
            RoutineStmt::ContinueLoop => Ok(RoutineControl::NextIteration),
            RoutineStmt::Null => Ok(RoutineControl::Continue),
            RoutineStmt::Assignment { target, expr } => {
                let value = self.eval_routine_expr(frame, expr)?;
                self.assign_routine_value(frame, target, value)?;
                Ok(RoutineControl::Continue)
            }
            RoutineStmt::SelectInto {
                query,
                targets,
                strict,
            } => {
                let result = self.execute_routine_query(frame, query)?;
                if *strict {
                    if result.rows.is_empty() {
                        return Err(SqlError::NoDataFound);
                    }
                    if result.rows.len() > 1 {
                        return Err(SqlError::RaisedException {
                            sqlstate: "P0003".into(),
                            message: "query returned more than one row".into(),
                            detail: None,
                        });
                    }
                }
                let found = !result.rows.is_empty();
                assign_routine_targets_with_columns(
                    frame,
                    targets,
                    &result.columns,
                    result.rows.into_iter().next(),
                )?;
                frame.set(ROUTINE_FOUND_VAR, SqlValue::Bool(found));
                Ok(RoutineControl::Continue)
            }
            RoutineStmt::Perform { query } => {
                // PERFORM evaluates for effect and discards the rows; FOUND
                // still reports whether any row came back.
                let result = self.execute_routine_query(frame, query)?;
                frame.set(ROUTINE_FOUND_VAR, SqlValue::Bool(!result.rows.is_empty()));
                Ok(RoutineControl::Continue)
            }
            RoutineStmt::Sql(statement) => {
                let result = self.execute_routine_sql(frame, statement)?;
                frame.set(ROUTINE_FOUND_VAR, SqlValue::Bool(sql_result_found(&result)));
                Ok(RoutineControl::Continue)
            }
            RoutineStmt::SqlInto { statement, targets } => {
                let result = self.execute_routine_sql(frame, statement)?;
                let found = !result.rows.is_empty();
                assign_routine_targets_with_columns(
                    frame,
                    targets,
                    &result.columns,
                    result.rows.into_iter().next(),
                )?;
                frame.set(ROUTINE_FOUND_VAR, SqlValue::Bool(found));
                Ok(RoutineControl::Continue)
            }
            RoutineStmt::DynamicExecute(expr) => {
                let value = self.eval_routine_expr(frame, expr)?;
                let SqlValue::String(sql) = value else {
                    return Err(SqlError::InvalidSql(
                        "dynamic EXECUTE expression must evaluate to text".to_string(),
                    ));
                };
                // A dynamic statement runs with the same authority and in
                // the same transaction as any embedded routine SQL; there is
                // no reason to allow less than PostgreSQL does. GRANT/REVOKE
                // loops in provisioning scripts are the common shape.
                // Function grants have a raw compatibility path even when
                // sqlparser accepts their syntax. Use the same authorization
                // and grant-option checks as a directly submitted statement.
                if parse_raw_function_privilege_ddl(self.db_ref(), &sql)?.is_some() {
                    let previous = self.bind_routine_frame(frame);
                    let result = self.execute(&sql);
                    self.unbind_routine_frame(previous);
                    result?;
                    return Ok(RoutineControl::Continue);
                }
                match parse_single_statement(&sql) {
                    Ok(parsed) => {
                        // Parsed per execution: not an IR node.
                        let previous = self.bind_routine_frame(frame);
                        let owned = std::mem::replace(&mut self.ir_owned_statement, false);
                        let result = self.execute_parsed_statement(&parsed);
                        self.ir_owned_statement = owned;
                        self.unbind_routine_frame(previous);
                        result?;
                    }
                    // Raw-only DDL (GRANT EXECUTE ON FUNCTION and friends)
                    // never reaches the sqlparser AST; route it through the
                    // session's full raw-statement dispatch instead.
                    Err(_) => {
                        let previous = self.bind_routine_frame(frame);
                        let result = self.execute(&sql);
                        self.unbind_routine_frame(previous);
                        result?;
                    }
                }
                Ok(RoutineControl::Continue)
            }
            RoutineStmt::If {
                condition,
                then_body,
                else_body,
            } => {
                let condition =
                    sql_value_truth(self.eval_routine_expr(frame, condition)?)?.unwrap_or(false);
                if condition {
                    self.execute_routine_statements(frame, then_body)
                } else {
                    self.execute_routine_statements(frame, else_body)
                }
            }
            RoutineStmt::ForLoop {
                iterator,
                lower,
                upper,
                body,
            } => self.execute_routine_for_loop(frame, iterator, lower, upper, body),
            RoutineStmt::ForeachLoop {
                target,
                slice,
                array,
                body,
            } => self.execute_routine_foreach_loop(frame, target, *slice, array, body),
            RoutineStmt::QueryForLoop {
                target,
                query,
                body,
            } => self.execute_routine_query_for_loop(frame, target, query, body),
            RoutineStmt::OpenCursor { name } => {
                self.open_routine_cursor(frame, name)?;
                Ok(RoutineControl::Continue)
            }
            RoutineStmt::FetchCursor { name, targets } => {
                self.fetch_routine_cursor(frame, name, targets)?;
                Ok(RoutineControl::Continue)
            }
            RoutineStmt::CloseCursor { name } => {
                self.close_routine_cursor(frame, name)?;
                Ok(RoutineControl::Continue)
            }
            RoutineStmt::RaiseException {
                message,
                arguments,
                detail,
                sqlstate,
            } => {
                let arguments = arguments
                    .iter()
                    .map(|argument| self.eval_routine_expr(frame, argument))
                    .collect::<Result<Vec<_>>>()?;
                Err(SqlError::RaisedException {
                    sqlstate: sqlstate.clone(),
                    message: format_routine_raise_message(message, &arguments),
                    detail: detail
                        .as_ref()
                        .map(|expr| {
                            self.eval_routine_expr(frame, expr)
                                .map(|value| value.to_cell())
                        })
                        .transpose()?,
                })
            }
            RoutineStmt::ReturnQuery(query) => {
                if frame.returned_set.is_none() {
                    return Err(SqlError::InvalidSql(
                        "RETURN QUERY requires a set-returning function".into(),
                    ));
                }
                let result = self.execute_routine_query(frame, query)?;
                let target = frame.returned_set.as_mut().unwrap();
                if result.columns.len() != target.columns.len() {
                    return Err(SqlError::InvalidSql(
                        "RETURN QUERY column count does not match the declared result".into(),
                    ));
                }
                let found = !result.rows.is_empty();
                for row in result.rows {
                    let row = row
                        .into_iter()
                        .zip(&target.column_types)
                        .map(|(value, ty)| match ty {
                            Some(ty) => cast_value_to_pg_type(value, ty),
                            None => Ok(value),
                        })
                        .collect::<Result<Vec<_>>>()?;
                    target.rows.push(row);
                }
                frame.set("found", SqlValue::Bool(found));
                Ok(RoutineControl::Continue)
            }
            RoutineStmt::Return(expr) => {
                let value = expr
                    .as_ref()
                    .map(|expr| self.eval_routine_expr(frame, expr))
                    .transpose()?;
                Ok(RoutineControl::Return(value))
            }
        }
    }

    pub(crate) fn assign_routine_value(
        &mut self,
        frame: &mut RoutineFrame,
        target: &RoutineAssignmentTarget,
        value: SqlValue,
    ) -> Result<()> {
        match target {
            RoutineAssignmentTarget::Variable(name) => frame.assign(name, value)?,
            RoutineAssignmentTarget::ArrayElement { array_name, index } => {
                let index = eval_subscript_bound(self.eval_routine_expr(frame, index)?)?;
                if index < 1 {
                    return Err(SqlError::InvalidSql(format!(
                        "array assignment index for {array_name} must be positive"
                    )));
                }
                frame.set_array_element(array_name, (index - 1) as usize, value)?;
            }
        }
        Ok(())
    }

    pub(crate) fn execute_routine_for_loop(
        &mut self,
        frame: &mut RoutineFrame,
        iterator: &str,
        lower: &RoutineExpr,
        upper: &RoutineExpr,
        body: &[RoutineStmt],
    ) -> Result<RoutineControl> {
        let Some(lower) = sql_value_i64(&self.eval_routine_expr(frame, lower)?) else {
            return Err(SqlError::InvalidSql(format!(
                "FOR loop lower bound for {iterator} must be an integer"
            )));
        };
        let Some(upper) = sql_value_i64(&self.eval_routine_expr(frame, upper)?) else {
            return Err(SqlError::InvalidSql(format!(
                "FOR loop upper bound for {iterator} must be an integer"
            )));
        };
        if lower > upper {
            return Ok(RoutineControl::Continue);
        }
        for value in lower..=upper {
            self.cancellation.check()?;
            frame.set(iterator, SqlValue::Int(value));
            match self.execute_routine_statements(frame, body)? {
                RoutineControl::Continue | RoutineControl::NextIteration => {}
                returned => return Ok(returned),
            }
        }
        Ok(RoutineControl::Continue)
    }

    pub(crate) fn execute_routine_query_for_loop(
        &mut self,
        frame: &mut RoutineFrame,
        target: &str,
        query: &Query,
        body: &[RoutineStmt],
    ) -> Result<RoutineControl> {
        let mut result = self.execute_routine_query(frame, query)?;
        // A `regclass`-typed column carries the relation OID as its value —
        // the pgwire layer renders the name only at display time. A loop
        // variable is display-adjacent (`EXECUTE format('... %s', rec.name)`
        // is the canonical use), so resolve reg* columns to their names here,
        // exactly as the wire would print them.
        let reg_columns: Vec<usize> = result
            .column_types
            .iter()
            .enumerate()
            .filter(|(_, ty)| {
                ty.as_deref()
                    .is_some_and(|ty| matches!(ty, "regclass" | "regproc" | "regprocedure"))
            })
            .map(|(idx, _)| idx)
            .collect();
        if !reg_columns.is_empty() {
            for row in &mut result.rows {
                for &idx in &reg_columns {
                    if let Some(value @ SqlValue::Int(_)) = row.get(idx) {
                        let pg_type = result.column_types[idx].as_deref().unwrap();
                        row[idx] = SqlValue::String(render_oid_alias_value(
                            self.db_ref(),
                            pg_type,
                            value,
                        )?);
                    }
                }
            }
        }
        for row in result.rows {
            self.cancellation.check()?;
            frame.set(target, routine_record_value(&result.columns, &row));
            match self.execute_routine_statements(frame, body)? {
                RoutineControl::Continue | RoutineControl::NextIteration => {}
                returned => return Ok(returned),
            }
        }
        Ok(RoutineControl::Continue)
    }

    pub(crate) fn execute_routine_foreach_loop(
        &mut self,
        frame: &mut RoutineFrame,
        target: &str,
        slice: usize,
        array: &RoutineExpr,
        body: &[RoutineStmt],
    ) -> Result<RoutineControl> {
        let value = self.eval_routine_expr(frame, array)?;
        for value in foreach_array_values(&value, slice)? {
            self.cancellation.check()?;
            frame.assign(target, value)?;
            match self.execute_routine_statements(frame, body)? {
                RoutineControl::Continue | RoutineControl::NextIteration => {}
                returned => return Ok(returned),
            }
        }
        Ok(RoutineControl::Continue)
    }

    pub(crate) fn open_routine_cursor(
        &mut self,
        frame: &mut RoutineFrame,
        name: &str,
    ) -> Result<()> {
        let key = normalize_object_name(name);
        let query = frame
            .cursors
            .get(&key)
            .map(|cursor| cursor.query.clone())
            .ok_or_else(|| SqlError::InvalidSql(format!("cursor \"{name}\" does not exist")))?;
        let result = self.execute_routine_query_unowned(frame, &query)?;
        let cursor = frame
            .cursors
            .get_mut(&key)
            .ok_or_else(|| SqlError::InvalidSql(format!("cursor \"{name}\" does not exist")))?;
        cursor.rows = result.rows;
        cursor.position = 0;
        cursor.open = true;
        Ok(())
    }

    pub(crate) fn fetch_routine_cursor(
        &mut self,
        frame: &mut RoutineFrame,
        name: &str,
        targets: &[String],
    ) -> Result<()> {
        let key = normalize_object_name(name);
        let cursor = frame
            .cursors
            .get_mut(&key)
            .ok_or_else(|| SqlError::InvalidSql(format!("cursor \"{name}\" does not exist")))?;
        if !cursor.open {
            return Err(SqlError::InvalidSql(format!(
                "cursor \"{name}\" is not open"
            )));
        }
        let row = cursor.rows.get(cursor.position).cloned();
        if row.is_some() {
            cursor.position += 1;
        }
        assign_routine_targets_owned_nullable(frame, targets, row)?;
        Ok(())
    }

    pub(crate) fn close_routine_cursor(
        &mut self,
        frame: &mut RoutineFrame,
        name: &str,
    ) -> Result<()> {
        let key = normalize_object_name(name);
        let cursor = frame
            .cursors
            .get_mut(&key)
            .ok_or_else(|| SqlError::InvalidSql(format!("cursor \"{name}\" does not exist")))?;
        cursor.rows.clear();
        cursor.position = 0;
        cursor.open = false;
        Ok(())
    }

    pub(crate) fn eval_routine_expr(
        &mut self,
        frame: &mut RoutineFrame,
        expr: &RoutineExpr,
    ) -> Result<SqlValue> {
        if let Some(bound) = &expr.bound {
            let empty_row = SqlRow::default();
            if expr.user_calls.is_empty() {
                return bound.eval(&BoundExprFrame {
                    db: self.db_ref(),
                    columns: BoundExprColumns::Row {
                        row: &empty_row,
                        column_keys: &[],
                    },
                    vars: frame.slot_values(),
                    user_calls: &[],
                });
            }
            if let Some(results) = self.eval_hoisted_user_calls(frame, &expr.user_calls)? {
                return bound.eval(&BoundExprFrame {
                    db: self.db_ref(),
                    columns: BoundExprColumns::Row {
                        row: &empty_row,
                        column_keys: &[],
                    },
                    vars: frame.slot_values(),
                    user_calls: &results,
                });
            }
        }
        let previous = self.bind_routine_frame(frame);
        let result = self.eval_session_expr(&expr.expr);
        self.unbind_routine_frame(previous);
        result
    }

    /// Evaluate a routine expression's hoisted user calls in slot order.
    /// `None` when any call is not known (by the session's per-node dispatch
    /// memo) to resolve to a stored routine — the generic evaluator then runs
    /// the whole expression, exactly as before, and records the dispatch so
    /// the next evaluation takes this path. A builtin that the generic
    /// dispatch chain handles under the same name therefore keeps winning.
    fn eval_hoisted_user_calls(
        &mut self,
        frame: &RoutineFrame,
        calls: &[BoundUserCall],
    ) -> Result<Option<smallvec::SmallVec<[SqlValue; 4]>>> {
        let Some((generation, ir)) = crate::eval::expr_type_scope() else {
            return Ok(None);
        };
        let mut results = smallvec::SmallVec::<[SqlValue; 4]>::with_capacity(calls.len());
        let empty_row = SqlRow::default();
        for call in calls {
            let key = (generation, ir, call.node);
            let stored = FN_DISPATCH_MEMO
                .with(|memo| memo.borrow().get(&key).map(|dispatch| dispatch.stored));
            if stored != Some(true) {
                return Ok(None);
            }
            let mut args = smallvec::SmallVec::<[SqlValue; 8]>::with_capacity(call.args.len());
            {
                let arg_frame = BoundExprFrame {
                    db: self.db_ref(),
                    columns: BoundExprColumns::Row {
                        row: &empty_row,
                        column_keys: &[],
                    },
                    vars: frame.slot_values(),
                    user_calls: &results,
                };
                for arg in &call.args {
                    args.push(arg.eval(&arg_frame)?);
                }
            }
            match self.eval_stored_function_value(&call.name, &args)? {
                Some(value) => results.push(value),
                None => return Ok(None),
            }
        }
        Ok(Some(results))
    }

    /// Make `frame` the session's variable binding for one embedded statement:
    /// the string-keyed map for the generic evaluator and the frame's slots
    /// for bound expressions. Returns what `unbind_routine_frame` restores.
    pub(crate) fn bind_routine_frame(
        &mut self,
        frame: &mut RoutineFrame,
    ) -> (
        Arc<BTreeMap<String, SqlValue>>,
        Option<crate::engine::RoutineSlotBinding>,
    ) {
        let vars = std::mem::replace(&mut self.routine_vars, frame.materialized_values());
        let slots = std::mem::replace(&mut self.routine_slots, Some(frame.slot_binding()));
        (vars, slots)
    }

    pub(crate) fn unbind_routine_frame(
        &mut self,
        previous: (
            Arc<BTreeMap<String, SqlValue>>,
            Option<crate::engine::RoutineSlotBinding>,
        ),
    ) {
        self.routine_vars = previous.0;
        self.routine_slots = previous.1;
    }

    pub(crate) fn execute_routine_query(
        &mut self,
        frame: &mut RoutineFrame,
        query: &Query,
    ) -> Result<SqlResult> {
        let previous = self.bind_routine_frame(frame);
        let owned = std::mem::replace(&mut self.ir_owned_statement, true);
        let result = self.execute_query(query);
        self.ir_owned_statement = owned;
        self.unbind_routine_frame(previous);
        result
    }

    pub(crate) fn execute_routine_sql(
        &mut self,
        frame: &mut RoutineFrame,
        statement: &Statement,
    ) -> Result<SqlResult> {
        let previous = self.bind_routine_frame(frame);
        let owned = std::mem::replace(&mut self.ir_owned_statement, true);
        let result = self.execute_parsed_statement(statement);
        self.ir_owned_statement = owned;
        self.unbind_routine_frame(previous);
        result
    }

    /// `execute_routine_query` for a query that is NOT an IR node (a per-frame
    /// cursor clone): address-keyed memos stay off for it.
    fn execute_routine_query_unowned(
        &mut self,
        frame: &mut RoutineFrame,
        query: &Query,
    ) -> Result<SqlResult> {
        let previous = self.bind_routine_frame(frame);
        let owned = std::mem::replace(&mut self.ir_owned_statement, false);
        let result = self.execute_query(query);
        self.ir_owned_statement = owned;
        self.unbind_routine_frame(previous);
        result
    }

    pub(crate) fn eval_sequence_arg(
        &mut self,
        expr: Option<&Expr>,
        function: &str,
    ) -> Result<String> {
        let Some(expr) = expr else {
            return Err(SqlError::InvalidSql(format!(
                "{function} expects a sequence name"
            )));
        };
        match self.eval_session_expr(expr)? {
            SqlValue::String(value) => Ok(value),
            SqlValue::Int(oid) => resolve_regclass_name(self.db_ref(), oid)?.ok_or_else(|| {
                SqlError::InvalidSql(format!(
                    "{function} sequence oid {oid} does not name a relation"
                ))
            }),
            other => Err(SqlError::InvalidSql(format!(
                "{function} sequence name must be text, got {}",
                other.to_cell()
            ))),
        }
    }

    pub(crate) fn nextval(&mut self, sequence_name: &str) -> Result<i64> {
        let mut sequence = load_sequence_required(self.db_ref(), sequence_name)?;
        self.require_sequence_privilege(&sequence, &["USAGE", "UPDATE"])?;
        let value = advance_sequence(&mut sequence)?;
        save_sequence(self.db_mut()?, &sequence)?;
        self.currvals.insert(sequence.name.clone(), value);
        Arc::make_mut(&mut self.session_gucs)
            .insert(LASTVAL_SESSION_KEY.to_string(), value.to_string());
        Ok(value)
    }

    pub(crate) fn nextvals(&mut self, sequence_name: &str, count: usize) -> Result<Vec<i64>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let mut sequence = load_sequence_required(self.db_ref(), sequence_name)?;
        self.require_sequence_privilege(&sequence, &["USAGE", "UPDATE"])?;
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            match advance_sequence(&mut sequence) {
                Ok(value) => values.push(value),
                Err(error) => {
                    if !values.is_empty() {
                        save_sequence(self.db_mut()?, &sequence)?;
                    }
                    return Err(error);
                }
            }
        }
        save_sequence(self.db_mut()?, &sequence)?;
        if let Some(value) = values.last().copied() {
            self.currvals.insert(sequence.name.clone(), value);
            Arc::make_mut(&mut self.session_gucs)
                .insert(LASTVAL_SESSION_KEY.to_string(), value.to_string());
        }
        Ok(values)
    }

    pub(crate) fn setval(
        &mut self,
        sequence_name: &str,
        value: i64,
        is_called: bool,
    ) -> Result<()> {
        let mut sequence = load_sequence_required(self.db_ref(), sequence_name)?;
        self.require_sequence_privilege(&sequence, &["UPDATE"])?;
        if value < sequence.min_value || value > sequence.max_value {
            return Err(SqlError::numeric_value_out_of_range(format!(
                "setval: value {value} is out of bounds for sequence \"{}\" ({}..{})",
                sequence.name, sequence.min_value, sequence.max_value
            )));
        }
        sequence.last_value = value;
        sequence.is_called = is_called;
        save_sequence(self.db_mut()?, &sequence)?;
        self.currvals.insert(sequence.name.clone(), value);
        Arc::make_mut(&mut self.session_gucs)
            .insert(LASTVAL_SESSION_KEY.to_string(), value.to_string());
        Ok(())
    }

    pub(crate) fn require_sequence_privilege(
        &self,
        sequence: &SequenceSchema,
        privileges: &[&str],
    ) -> Result<()> {
        let role = current_user_from_gucs(&self.session_gucs);
        if role_can_use_sequence(self.db_ref(), &role, sequence, privileges)? {
            return Ok(());
        }
        Err(SqlError::BicDb(BicDbError::Authorization(format!(
            "permission denied for sequence {}",
            sequence.name
        ))))
    }

    pub(crate) fn require_update_privileges(
        &self,
        table: &str,
        assignments: &[Assignment],
    ) -> Result<()> {
        let role = current_user_from_gucs(&self.session_gucs);
        if role_has_table_privilege(self.db_ref(), &role, table, "UPDATE")? {
            return Ok(());
        }
        let inherited = role_privilege_closure(self.db_ref(), &role)?;
        let grants = list_privileges(self.db_ref())?;
        for assignment in assignments {
            let AssignmentTarget::ColumnName(name) = &assignment.target else {
                return self.require_table_privilege(table, "UPDATE");
            };
            let column = relation_name(name)?;
            if !grants.iter().any(|grant| {
                grant.object_type == PrivilegeObjectType::Table
                    && grant.object_name == table
                    && grant.column.as_deref() == Some(column.as_str())
                    && grant.privilege == "UPDATE"
                    && (grant.grantee == "public"
                        || inherited.contains(&normalize_role_name(&grant.grantee)))
            }) {
                return self.require_table_privilege(table, "UPDATE");
            }
        }
        if assignments.is_empty() {
            return self.require_table_privilege(table, "UPDATE");
        }
        Ok(())
    }

    pub(crate) fn change_column_privileges(
        &mut self,
        table: &str,
        old: &str,
        new: Option<&str>,
    ) -> Result<()> {
        for mut grant in list_privileges(self.db_ref())? {
            if grant.object_type == PrivilegeObjectType::Table
                && grant.object_name == table
                && grant.column.as_deref() == Some(old)
            {
                self.delete_session_privilege(&grant)?;
                if let Some(new) = new {
                    grant.column = Some(new.to_owned());
                    self.save_session_privilege(&grant)?;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn require_table_privilege(&self, table: &str, privilege: &str) -> Result<()> {
        let role = current_user_from_gucs(&self.session_gucs);
        if role_has_table_privilege(self.db_ref(), &role, table, privilege)? {
            return Ok(());
        }
        Err(SqlError::BicDb(BicDbError::Authorization(format!(
            "permission denied for table {}",
            unqualified_relation(table)
        ))))
    }

    pub(crate) fn apply_foreign_key_parent_delete_batch(
        &mut self,
        table: &str,
        schema: &TableSchema,
        records: &[Record],
    ) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        // Memoized per table (schema-generation validated): the catalog scan
        // that found these ran on every DELETE statement.
        let inbound = catalog_memo::inbound_foreign_key_deletes_shared(self.db_ref(), table)?;
        if inbound.is_empty() {
            return Ok(());
        }
        // Owned copies only on the rare path where inbound keys exist.
        let inbound_constraints = inbound
            .iter()
            .map(|constraint| {
                (
                    constraint.child_schema.clone(),
                    constraint.name.clone(),
                    constraint.columns.clone(),
                    constraint.referred_columns.clone(),
                    constraint.on_delete,
                )
            })
            .collect::<Vec<_>>();

        for (child_schema, name, columns, referred_columns, on_delete) in &inbound_constraints {
            if !matches!(
                on_delete,
                ForeignKeyAction::NoAction | ForeignKeyAction::Restrict
            ) {
                continue;
            }
            let parent_keys = records
                .iter()
                .map(|record| {
                    typed_column_key_label(
                        schema,
                        referred_columns,
                        &record_column_values(record, schema, referred_columns),
                    )
                })
                .collect::<Result<BTreeSet<_>>>()?;
            if parent_keys.is_empty() {
                continue;
            }

            let child_records = self
                .db_ref()
                .scan_collection_unchecked(&child_schema.name)?;
            sql_profile_foreign_key_child_scan();
            for child in child_records {
                let child_key = typed_column_key_label(
                    schema,
                    referred_columns,
                    &record_column_values(&child, child_schema, columns),
                )?;
                if parent_keys.contains(&child_key) {
                    return Err(foreign_key_violation(&child_schema.name, name));
                }
            }
        }

        for (child_schema, name, columns, referred_columns, on_delete) in inbound_constraints {
            if matches!(
                on_delete,
                ForeignKeyAction::NoAction | ForeignKeyAction::Restrict
            ) {
                continue;
            }
            let parent_keys = records
                .iter()
                .map(|record| {
                    typed_column_key_label(
                        schema,
                        &referred_columns,
                        &record_column_values(record, schema, &referred_columns),
                    )
                })
                .collect::<Result<BTreeSet<_>>>()?;
            if parent_keys.is_empty() {
                continue;
            }

            let child_records = self
                .db_ref()
                .scan_collection_unchecked(&child_schema.name)?;
            sql_profile_foreign_key_child_scan();
            let mut cascade_ids = Vec::new();
            for mut child in child_records {
                let child_key = typed_column_key_label(
                    schema,
                    &referred_columns,
                    &record_column_values(&child, &child_schema, &columns),
                )?;
                if !parent_keys.contains(&child_key) {
                    continue;
                }
                match on_delete {
                    ForeignKeyAction::Cascade => {
                        cascade_ids.push(child.id.clone());
                    }
                    ForeignKeyAction::SetNull => {
                        for column in &columns {
                            set_record_column(
                                &mut child,
                                Some(&child_schema),
                                column,
                                SqlValue::Null,
                            )?;
                        }
                        validate_record_local_constraints(
                            &child_schema.name,
                            &child_schema,
                            &child,
                        )?;
                        validate_unique_constraints(
                            self.db_ref(),
                            self.tx.as_ref(),
                            &child_schema.name,
                            &child_schema,
                            &[child.clone()],
                            true,
                        )?;
                        self.update_session_record(&child_schema.name, child)?;
                    }
                    ForeignKeyAction::SetDefault => {
                        for column in &columns {
                            let value = self
                                .default_value_for_column(Some(&child_schema), column)?
                                .unwrap_or(SqlValue::Null);
                            set_record_column(&mut child, Some(&child_schema), column, value)?;
                        }
                        let updated_key = typed_column_key_label(
                            schema,
                            &referred_columns,
                            &record_column_values(&child, &child_schema, &columns),
                        )?;
                        if parent_keys.contains(&updated_key) {
                            return Err(foreign_key_violation(&child_schema.name, &name));
                        }
                        validate_record_local_constraints(
                            &child_schema.name,
                            &child_schema,
                            &child,
                        )?;
                        validate_unique_constraints(
                            self.db_ref(),
                            self.tx.as_ref(),
                            &child_schema.name,
                            &child_schema,
                            &[child.clone()],
                            true,
                        )?;
                        self.update_session_record(&child_schema.name, child)?;
                    }
                    ForeignKeyAction::NoAction | ForeignKeyAction::Restrict => {
                        continue;
                    }
                }
            }
            if !cascade_ids.is_empty() {
                self.delete_session_records(&child_schema.name, &cascade_ids)?;
            }
        }
        Ok(())
    }

    pub(crate) fn apply_foreign_key_parent_update(
        &mut self,
        table: &str,
        schema: &TableSchema,
        before: &Record,
        after: &Record,
    ) -> Result<()> {
        self.apply_foreign_key_parent_update_batch(
            table,
            schema,
            &[(before.clone(), after.clone())],
        )
    }

    pub(crate) fn apply_foreign_key_parent_update_batch(
        &mut self,
        table: &str,
        schema: &TableSchema,
        before_after_records: &[(Record, Record)],
    ) -> Result<()> {
        if before_after_records.is_empty() {
            return Ok(());
        }
        let inbound_constraints =
            crate::catalog_memo::inbound_foreign_key_updates_shared(self.db_ref(), table)?;
        if inbound_constraints.is_empty() {
            return Ok(());
        }

        for constraint in inbound_constraints.iter() {
            if !matches!(
                constraint.on_update,
                ForeignKeyAction::NoAction | ForeignKeyAction::Restrict
            ) {
                continue;
            }
            let changed_keys = changed_parent_update_keys(
                schema,
                &constraint.referred_columns,
                before_after_records,
            )?;
            if changed_keys.is_empty() {
                continue;
            }
            sql_profile_foreign_key_child_scan();
            for child in self
                .db_ref()
                .scan_collection_unchecked(&constraint.child_schema.name)?
            {
                let child_key = typed_column_key_label(
                    schema,
                    &constraint.referred_columns,
                    &record_column_values(&child, &constraint.child_schema, &constraint.columns),
                )?;
                if changed_keys
                    .iter()
                    .any(|(old_key, _)| *old_key == child_key)
                {
                    return Err(foreign_key_violation(
                        &constraint.child_schema.name,
                        &constraint.name,
                    ));
                }
            }
        }

        for constraint in inbound_constraints.iter() {
            if matches!(
                constraint.on_update,
                ForeignKeyAction::NoAction | ForeignKeyAction::Restrict
            ) {
                continue;
            }
            let changed_keys = changed_parent_update_keys(
                schema,
                &constraint.referred_columns,
                before_after_records,
            )?;
            if changed_keys.is_empty() {
                continue;
            }
            sql_profile_foreign_key_child_scan();
            for mut child in self
                .db_ref()
                .scan_collection_unchecked(&constraint.child_schema.name)?
            {
                let child_key = typed_column_key_label(
                    schema,
                    &constraint.referred_columns,
                    &record_column_values(&child, &constraint.child_schema, &constraint.columns),
                )?;
                let Some((_, new_key)) = changed_keys
                    .iter()
                    .find(|(old_key, _)| *old_key == child_key)
                else {
                    continue;
                };
                match constraint.on_update {
                    ForeignKeyAction::Cascade => {
                        for (column, value) in constraint.columns.iter().zip(new_key.iter()) {
                            set_record_column(
                                &mut child,
                                Some(&constraint.child_schema),
                                column,
                                value.clone(),
                            )?;
                        }
                        validate_record_local_constraints(
                            &constraint.child_schema.name,
                            &constraint.child_schema,
                            &child,
                        )?;
                        validate_unique_constraints(
                            self.db_ref(),
                            self.tx.as_ref(),
                            &constraint.child_schema.name,
                            &constraint.child_schema,
                            &[child.clone()],
                            true,
                        )?;
                        self.update_session_record(&constraint.child_schema.name, child)?;
                    }
                    ForeignKeyAction::SetNull => {
                        for column in &constraint.columns {
                            set_record_column(
                                &mut child,
                                Some(&constraint.child_schema),
                                column,
                                SqlValue::Null,
                            )?;
                        }
                        validate_record_local_constraints(
                            &constraint.child_schema.name,
                            &constraint.child_schema,
                            &child,
                        )?;
                        validate_unique_constraints(
                            self.db_ref(),
                            self.tx.as_ref(),
                            &constraint.child_schema.name,
                            &constraint.child_schema,
                            &[child.clone()],
                            true,
                        )?;
                        self.update_session_record(&constraint.child_schema.name, child)?;
                    }
                    ForeignKeyAction::SetDefault => {
                        for column in &constraint.columns {
                            let value = self
                                .default_value_for_column(Some(&constraint.child_schema), column)?
                                .unwrap_or(SqlValue::Null);
                            set_record_column(
                                &mut child,
                                Some(&constraint.child_schema),
                                column,
                                value,
                            )?;
                        }
                        validate_record_local_constraints(
                            &constraint.child_schema.name,
                            &constraint.child_schema,
                            &child,
                        )?;
                        validate_unique_constraints(
                            self.db_ref(),
                            self.tx.as_ref(),
                            &constraint.child_schema.name,
                            &constraint.child_schema,
                            &[child.clone()],
                            true,
                        )?;
                        validate_foreign_keys(
                            self.db_ref(),
                            self.tx.as_ref(),
                            &constraint.child_schema.name,
                            &constraint.child_schema,
                            &child,
                        )?;
                        self.update_session_record(&constraint.child_schema.name, child)?;
                    }
                    ForeignKeyAction::NoAction | ForeignKeyAction::Restrict => {}
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
thread_local! {
    pub(crate) static SQL_STORED_UPDATE_HITS: std::cell::RefCell<usize> = const { std::cell::RefCell::new(0) };
    pub(crate) static SQL_STORED_UPDATE_SHAPE_BUILDS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// One assigned column of a stored-form UPDATE: the schema column, the
/// evaluated value cast to the column type, and its storage JSON text (`None`
/// removes the member: a JSON-typed column set to SQL NULL).
struct StoredAssignment {
    column: String,
    value: SqlValue,
    storage: Option<String>,
}

impl<'db> SqlSession<'db> {
    /// UPDATE of resident rows without parsing them (typed resident rows
    /// phase 3): `Some` only for the simple shape — a transaction, a table
    /// with no CHECK/FK/exclusion constraints, generated columns, RLS,
    /// full-text/JSONB/array index projections or protection policy; no
    /// trigger, unique-key, inbound-FK or extension-binding consumer of the
    /// pre-image; plain column assignments to non-key, non-special columns;
    /// and either a FROM row set or repairable assignments (the read-committed
    /// recheck of the generic path compares parsed records). Every candidate
    /// row must be resident with readable cells; anything else returns `None`
    /// before a write happens and the generic path runs unchanged.
    #[allow(clippy::too_many_arguments)]
    /// A routine-owned `DELETE ... WHERE <full primary key>` on a table with
    /// nothing that needs the row's contents (no triggers, no referencing
    /// foreign keys, no event bindings, no policies, no RETURNING/USING):
    /// locate the id through the IR-keyed point template, confirm the row is
    /// resident and current, and issue the delete by id. The generic path
    /// parsed the row twice (candidate + read-committed recheck) and scanned
    /// the schema catalog for inbound foreign keys on every statement.
    /// `None` = not eligible, take the generic path (nothing done yet).
    fn try_execute_delete_stored(
        &mut self,
        delete: &Delete,
        ctes: &BTreeMap<String, CteResult>,
        table: &str,
        target_alias: &str,
        schema: &TableSchema,
    ) -> Result<Option<SqlResult>> {
        if !stored_update::enabled()
            || !ir_update_plan::enabled()
            || self.tx.is_none()
            || !ctes.is_empty()
            || delete.returning.is_some()
            || delete.using.is_some()
        {
            return Ok(None);
        }
        let Some(selection) = delete.selection.as_ref() else {
            return Ok(None);
        };
        if schema.rls_enabled
            || !schema
                .constraints
                .iter()
                .all(|constraint| matches!(constraint, ConstraintSchema::Unique { .. }))
        {
            return Ok(None);
        }
        let row_engine = self.sql_engine_with_ctes(ctes.clone());
        let Some(key) = row_engine.ir_plan_node_key(selection as *const Expr as usize) else {
            return Ok(None);
        };
        let template = match sql_update_plan_node_cache_get(key) {
            Some(entry) => entry,
            None => {
                let built = row_engine
                    .build_update_point_template(table, target_alias, schema, selection, &[])?
                    .filter(|template| template.outer_columns.is_empty())
                    .map(Rc::new);
                sql_update_plan_node_cache_set(key, built.clone());
                built
            }
        };
        let Some(template) = template.filter(|template| template.outer_columns.is_empty()) else {
            return Ok(None);
        };
        if self.db_ref().collection_policy(table)?.is_some()
            || self
                .db_ref()
                .mutation_policy(table)?
                .is_some_and(|policy| policy.grants_required)
            || !row_engine.cell_rows_eligible(table, schema, PolicyAction::Delete)?
            || self.has_row_triggers(table, "before", "delete")?
            || self.has_row_triggers(table, "after", "delete")?
            || !catalog_memo::inbound_foreign_key_deletes_shared(self.db_ref(), table)?.is_empty()
            || !self
                .extension_database_event_bindings(
                    &Self::extension_event_relation(table),
                    DatabaseOperation::Delete,
                )?
                .is_empty()
        {
            return Ok(None);
        }
        let Some(id) = row_engine.update_point_record_id(&template, table, schema)? else {
            return Ok(Some(SqlResult::command("DELETE 0")));
        };
        // Resident and visible? A row this transaction already wrote takes the
        // generic path (its pending image is a Record).
        let visible = row_engine.visible_rows_for_pks(table, std::slice::from_ref(&id))?;
        drop(row_engine);
        match visible.first() {
            Some(Some(bicdb_core::VisibleRow::Stored(_))) => {}
            Some(None) | None => return Ok(Some(SqlResult::command("DELETE 0"))),
            Some(Some(_)) => return Ok(None),
        }
        // Read committed: the latest committed version decides. The predicate
        // is the (immutable) primary key, so a surviving row still matches.
        let refreshed = match self.tx.as_mut() {
            Some(tx) => tx
                .read_committed_update_stored(table, &id)
                .map_err(SqlError::from)?,
            None => None,
        };
        let snapshot = match refreshed {
            None => 0,
            Some((None, _)) => return Ok(Some(SqlResult::command("DELETE 0"))),
            Some((Some(_), snapshot)) => snapshot,
        };
        sql_profile_index_lookup();
        #[cfg(test)]
        SQL_STORED_DELETE_HITS.with(|hits| *hits.borrow_mut() += 1);
        let deleted = if snapshot != 0 {
            self.delete_session_records_with_statement_snapshots(
                table,
                std::iter::once((id, snapshot)),
            )?
        } else {
            self.delete_session_records(table, std::slice::from_ref(&id))?
        };
        Ok(Some(SqlResult::command(format!("DELETE {deleted}"))))
    }

    pub(crate) fn try_execute_update_stored(
        &mut self,
        update: &sqlparser::ast::Update,
        ctes: &BTreeMap<String, CteResult>,
        table: &str,
        target_alias: &str,
        schema: &Arc<TableSchema>,
        from_rows: Option<&RowSet>,
        target_columns: &Arc<Vec<String>>,
    ) -> Result<Option<SqlResult>> {
        if !stored_update::enabled() || self.tx.is_none() || !ctes.is_empty() {
            return Ok(None);
        }
        if schema.rls_enabled
            || !schema
                .constraints
                .iter()
                .all(|constraint| matches!(constraint, ConstraintSchema::Unique { .. }))
            || schema
                .columns
                .iter()
                .any(|column| column.generated_expr.is_some())
        {
            return Ok(None);
        }
        let row_engine = self.sql_engine_with_ctes(ctes.clone());
        let read_key = row_engine.ir_plan_node_key(update as *const _ as usize);
        let cached_plan = read_key.and_then(|key| {
            STORED_UPDATE_READ_MEMO.with(|memo| {
                memo.borrow()
                    .get(&key)
                    .filter(|plan| Arc::ptr_eq(&plan.schema, schema))
                    .cloned()
            })
        });
        let read_plan = match cached_plan {
            Some(plan) => plan,
            None => {
                let plan = Rc::new(StoredUpdateReadPlan::build(
                    update,
                    schema,
                    table,
                    target_alias,
                )?);
                if let Some(key) = read_key {
                    STORED_UPDATE_READ_MEMO.with(|memo| {
                        let mut memo = memo.borrow_mut();
                        if memo.len() >= ROUTINE_EXPR_TYPE_MEMO_MAX {
                            memo.clear();
                        }
                        memo.insert(key, Rc::clone(&plan));
                    });
                }
                plan
            }
        };
        let Some(assigned) = &read_plan.assignments else {
            return Ok(None);
        };
        // Only AST/schema-owned shape is cached. Authorization, indexes,
        // triggers and repair eligibility below must still be checked live.
        if self.update_needs_before_image(update, table, schema)? {
            return Ok(None);
        }
        let repair_assignments =
            self.repairable_update_assignments(update, table, target_alias, Some(schema))?;
        // Without FROM or repairs the generic path rechecks each row against
        // the latest committed version (read committed); done here on the
        // stored forms, below.
        let recheck = from_rows.is_none() && repair_assignments.is_none();
        // A counter increment whose RETURNING value feeds later statements is
        // a reservation, not an ordinary replace-in-place update. If it is the
        // transaction's first row lock, core can wait for the owner without a
        // possible lock cycle. This avoids throwing away the rest of an
        // order/invoice/ticket transaction merely because another allocator
        // held the counter for a few hundred microseconds.
        let reservation_recheck = recheck
            && update.assignments.len() == 1
            && update.returning.as_deref().is_some_and(|returning| {
                let AssignmentTarget::ColumnName(column_name) = &update.assignments[0].target
                else {
                    return false;
                };
                let Ok(column) = relation_name(column_name) else {
                    return false;
                };
                if repair_delta_expr(&update.assignments[0].value, &column, table, target_alias)
                    .is_none()
                {
                    return false;
                }
                returning.iter().any(|item| {
                    let expr = match item {
                        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                            expr
                        }
                        _ => return false,
                    };
                    let mut references = Vec::new();
                    collect_predicate_column_references(expr, &mut references)
                        && references.iter().any(|reference| {
                            reference
                                .last()
                                .is_some_and(|name| name.eq_ignore_ascii_case(&column))
                        })
                })
            });
        if self.db_ref().collection_policy(table)?.is_some()
            || self
                .db_ref()
                .mutation_policy(table)?
                .is_some_and(|policy| policy.grants_required)
        {
            return Ok(None);
        }
        if index_definitions_shared(self.db_ref()).iter().any(|index| {
            index.collection == table
                && matches!(
                    index.kind,
                    IndexKind::FullText | IndexKind::Jsonb | IndexKind::Array
                )
        }) {
            return Ok(None);
        }
        if !row_engine.cell_rows_eligible(table, schema, PolicyAction::Update)? {
            return Ok(None);
        }
        let fields = &read_plan.fields;
        let target_columns = read_plan.columns.as_ref().unwrap_or(target_columns);
        let selection = update.selection.as_ref();

        // Candidates: (candidate row, its stored row), every one resident.
        #[allow(unused_mut)]
        let mut candidates: Vec<(SlotRow, Arc<bicdb_core::StoredRecord>)> = Vec::new();
        let candidate_columns: Arc<Vec<String>> = match from_rows {
            Some(from_rows) => Arc::new(merge_row_set_columns(
                target_columns.as_ref().clone(),
                from_rows.columns.clone(),
            )),
            None => target_columns.clone(),
        };
        let mut row_engine = row_engine;
        match from_rows {
            Some(from_rows) => {
                // The IR-keyed point plan (key expressions bound over the FROM
                // row layout) locates each target with no per-row planning,
                // exactly as the generic path does.
                let mut from_point_template: Option<Rc<UpdatePointTemplate>> = None;
                if let Some(selection) = selection {
                    if let Some(key) = ir_update_plan::enabled()
                        .then(|| row_engine.ir_plan_node_key(selection as *const Expr as usize))
                        .flatten()
                    {
                        let template = match sql_update_plan_node_cache_get(key) {
                            Some(entry) => entry,
                            None => {
                                let built = row_engine
                                    .build_update_point_template(
                                        table,
                                        target_alias,
                                        schema,
                                        selection,
                                        &from_rows.columns,
                                    )?
                                    .map(Rc::new);
                                sql_update_plan_node_cache_set(key, built.clone());
                                built
                            }
                        };
                        from_point_template =
                            template.filter(|template| template.outer_columns == from_rows.columns);
                    }
                }
                let mut seen = BTreeSet::new();
                if let Some(template) = from_point_template.as_ref() {
                    // A prepared point lookup covers the complete predicate.
                    // Resolve its keys first, then fetch all targets together:
                    // one visibility probe and one reusable cell plan for the
                    // FROM batch, with no cloned outer-row contexts.
                    let mut ids = Vec::with_capacity(from_rows.rows.len());
                    let mut source_positions = Vec::with_capacity(from_rows.rows.len());
                    for (position, from_row) in from_rows.rows.iter().enumerate() {
                        if position % 1024 == 0 {
                            self.cancellation.check()?;
                        }
                        if let Some(id) = row_engine
                            .update_point_record_id_for_row(template, table, schema, from_row)?
                        {
                            ids.push(id);
                            source_positions.push(position);
                        }
                    }
                    sql_profile_index_lookup();
                    let Some(pairs) = self.stored_update_candidates(
                        &row_engine,
                        table,
                        target_alias,
                        fields,
                        &ids,
                    )?
                    else {
                        return Ok(None);
                    };
                    for (position, target_row, stored) in pairs {
                        if !seen.insert(stored.id.clone()) {
                            continue;
                        }
                        let from_row = &from_rows.rows[source_positions[position]];
                        candidates.push((
                            merge_slot_rows(
                                &target_row,
                                target_columns,
                                from_row,
                                &from_rows.columns,
                            ),
                            stored,
                        ));
                    }
                } else {
                    for from_row in &from_rows.rows {
                        row_engine.outer_row =
                            Some(OuterSlotRow::from_slot(&from_rows.columns, from_row));
                        let ids = match from_point_template.as_ref() {
                            Some(template) => row_engine
                                .update_point_record_id_for_row(template, table, schema, from_row)?
                                .into_iter()
                                .collect::<Vec<String>>(),
                            None => {
                                let Some(ids) = row_engine.indexed_record_ids_for_table_selection(
                                    table,
                                    target_alias,
                                    Some(schema),
                                    selection,
                                )?
                                else {
                                    return Ok(None);
                                };
                                ids
                            }
                        };
                        sql_profile_index_lookup();
                        let Some(pairs) = self.stored_update_candidates(
                            &row_engine,
                            table,
                            target_alias,
                            &fields,
                            &ids,
                        )?
                        else {
                            return Ok(None);
                        };
                        for (_, target_row, stored) in pairs {
                            if seen.contains(&stored.id) {
                                continue;
                            }
                            let candidate_row = merge_slot_rows(
                                &target_row,
                                target_columns,
                                from_row,
                                &from_rows.columns,
                            );
                            // A point-plan hit is the whole predicate: no re-check.
                            let matches = match selection {
                                None => true,
                                Some(_) if from_point_template.is_some() => true,
                                Some(selection) => {
                                    let outer = row_engine.outer_row.take();
                                    let (_scope, context) =
                                        row_engine.bound_row_context(candidate_columns.as_ref());
                                    let matches = row_engine.eval_slot_row_predicate(
                                        &candidate_row,
                                        &context,
                                        selection,
                                    );
                                    row_engine.outer_row = outer;
                                    matches?
                                }
                            };
                            if matches && seen.insert(stored.id.clone()) {
                                candidates.push((candidate_row, stored));
                            }
                        }
                    }
                }
                row_engine.outer_row = None;
            }
            None => {
                // The IR-keyed point plan first (its key is the whole
                // predicate), the generic locator otherwise.
                let mut ir_point_ids: Option<Vec<String>> = None;
                if let Some(selection) = selection {
                    if let Some(key) = ir_update_plan::enabled()
                        .then(|| row_engine.ir_plan_node_key(selection as *const Expr as usize))
                        .flatten()
                    {
                        let template = match sql_update_plan_node_cache_get(key) {
                            Some(entry) => entry,
                            None => {
                                let built = row_engine
                                    .build_update_point_template(
                                        table,
                                        target_alias,
                                        schema,
                                        selection,
                                        &[],
                                    )?
                                    .filter(|template| template.outer_columns.is_empty())
                                    .map(Rc::new);
                                sql_update_plan_node_cache_set(key, built.clone());
                                built
                            }
                        };
                        if let Some(template) =
                            template.filter(|template| template.outer_columns.is_empty())
                        {
                            ir_point_ids = Some(
                                row_engine
                                    .update_point_record_id(&template, table, schema)?
                                    .into_iter()
                                    .collect(),
                            );
                        }
                    }
                }
                let predicate_covered_by_key = ir_point_ids.is_some()
                    || match selection {
                        Some(selection) => row_engine
                            .exact_primary_key_selection_covers_predicate(
                                table,
                                target_alias,
                                schema,
                                selection,
                            )?,
                        None => false,
                    };
                let ids = match ir_point_ids {
                    Some(ids) => ids,
                    None => {
                        let Some(ids) = row_engine.indexed_record_ids_for_table_selection(
                            table,
                            target_alias,
                            Some(schema),
                            selection,
                        )?
                        else {
                            return Ok(None);
                        };
                        ids
                    }
                };
                sql_profile_index_lookup();
                let Some(pairs) =
                    self.stored_update_candidates(&row_engine, table, target_alias, &fields, &ids)?
                else {
                    return Ok(None);
                };
                for (_, target_row, stored) in pairs {
                    let matches = predicate_covered_by_key
                        || match selection {
                            None => true,
                            Some(selection) => {
                                let (_scope, context) =
                                    row_engine.bound_row_context(target_columns.as_ref());
                                row_engine.eval_slot_row_predicate(
                                    &target_row,
                                    &context,
                                    selection,
                                )?
                            }
                        };
                    if matches {
                        candidates.push((target_row, stored));
                    }
                }
            }
        }
        sql_profile_sql_row_refs_materialized(candidates.iter().map(|(row, _)| row));
        drop(row_engine);

        // Read-committed recheck on the stored forms: a row rewritten by a
        // committed transaction newer than this snapshot is re-read, its
        // candidate row rebuilt from the latest cells, and the WHERE
        // re-evaluated; an equal latest row only records the snapshot.
        let mut snapshots = vec![0u64; candidates.len()];
        let mut recheck_predicate = vec![false; candidates.len()];
        if recheck {
            let mut plan: Option<CellPlan> = None;
            let mut kept = Vec::with_capacity(candidates.len());
            for (row, stored) in candidates {
                let refreshed = match self.tx.as_mut() {
                    Some(tx) if reservation_recheck => tx
                        .read_committed_update_stored_reservation(table, &stored.id)
                        .map_err(SqlError::from)?,
                    Some(tx) => tx
                        .read_committed_update_stored(table, &stored.id)
                        .map_err(SqlError::from)?,
                    None => None,
                };
                let Some((latest, snapshot)) = refreshed else {
                    kept.push((row, stored, 0u64, false));
                    continue;
                };
                let Some(latest) = latest else {
                    continue;
                };
                if Arc::ptr_eq(&latest, &stored) || latest.metadata.get() == stored.metadata.get() {
                    kept.push((row, stored, snapshot, false));
                    continue;
                }
                let target_row = {
                    let mut cells: bicdb_core::CellRow<'_> = Vec::with_capacity(32);
                    if !latest.cells_into(&mut cells) {
                        return Ok(None);
                    }
                    let Some(target_row) = slot_row_from_cells(
                        table,
                        target_alias,
                        &fields,
                        &latest,
                        &cells,
                        &mut plan,
                    ) else {
                        return Ok(None);
                    };
                    target_row
                };
                let mut refreshed_row = row;
                for (slot_idx, value) in target_row.into_iter().enumerate() {
                    if let Some(slot) = refreshed_row.get_mut(slot_idx) {
                        *slot = value;
                    }
                }
                kept.push((refreshed_row, latest, snapshot, selection.is_some()));
            }
            candidates = Vec::with_capacity(kept.len());
            snapshots.clear();
            recheck_predicate.clear();
            for (row, stored, snapshot, recheck_row) in kept {
                candidates.push((row, stored));
                snapshots.push(snapshot);
                recheck_predicate.push(recheck_row);
            }
        }

        // Assign, splice, buffer.
        let row_engine = self.sql_engine_with_ctes(ctes.clone());
        let memo = self.ir_owned_statement
            && self.current_routine_ir.is_some()
            && update
                .assignments
                .iter()
                .all(|assignment| !expr_contains_subquery(&assignment.value));
        let mut row_engine = row_engine.with_operand_type_memo(memo);
        let (scope, context) = row_engine.bound_row_context(candidate_columns.as_ref());
        // Routine-owned assignments bind once per IR node over this candidate
        // layout (the same cache the generic UPDATE path keeps); the binder's
        // declines and non-routine statements evaluate the expression as
        // before.
        let bound_assignments: Vec<Option<Rc<BoundAssignment>>> = update
            .assignments
            .iter()
            .map(|assignment| {
                ir_update_plan::enabled()
                    .then(|| row_engine.ir_plan_node_key(&assignment.value as *const Expr as usize))
                    .flatten()
                    .and_then(|key| match sql_bound_assignment_cache_get(key) {
                        Some(entry) => entry,
                        None => {
                            let built = scope
                                .bind_with_case_validator(
                                    &assignment.value,
                                    &|conditions, else_result| {
                                        let mut types = Vec::with_capacity(conditions.len() + 1);
                                        for result in else_result
                                            .into_iter()
                                            .chain(conditions.iter().map(|branch| &branch.result))
                                        {
                                            let Some(pg_type) = row_engine
                                                .infer_slot_row_expr_type(
                                                    candidate_columns.as_ref(),
                                                    result,
                                                )
                                            else {
                                                return false;
                                            };
                                            types.push(Some(pg_type));
                                        }
                                        select_common_pg_type(self.db_ref(), &types, "CASE").is_ok()
                                    },
                                )
                                .map(|expr| {
                                    Rc::new(BoundAssignment {
                                        columns: candidate_columns.as_ref().clone(),
                                        expr,
                                    })
                                });
                            sql_bound_assignment_cache_set(key, built.clone());
                            built
                        }
                    })
                    .filter(|bound| bound.columns == *candidate_columns.as_ref())
            })
            .collect();
        let mut rows = Vec::with_capacity(candidates.len());
        let mut returning_rows: Vec<SlotRow> = Vec::new();
        for (idx, (candidate_row, stored)) in candidates.into_iter().enumerate() {
            if idx % 1024 == 0 {
                self.cancellation.check()?;
            }
            if recheck_predicate[idx] {
                if let Some(selection) = selection {
                    if !row_engine.eval_slot_row_predicate(&candidate_row, &context, selection)? {
                        continue;
                    }
                }
            }
            let statement_snapshot = snapshots[idx];
            let mut assignments = Vec::with_capacity(update.assignments.len());
            for ((assignment, column), bound) in update
                .assignments
                .iter()
                .zip(&assigned.columns)
                .zip(&bound_assignments)
            {
                let value = match eval_row_or_bound_value(
                    &row_engine,
                    &candidate_row,
                    &assignment.value,
                    bound.as_deref().map(|bound| &bound.expr),
                    &context,
                ) {
                    Ok(value) => value,
                    Err(SqlError::Unsupported(_)) => return Ok(None),
                    Err(error) => return Err(error),
                };
                let column_schema = &schema.columns[*column];
                let value =
                    cast_value_to_column_type(value, column_schema).map_err(
                        |error| match error {
                            error @ (SqlError::ConstraintViolation { .. }
                            | SqlError::DataException { .. }) => error,
                            error => SqlError::TypeMismatch {
                                table: schema.name.clone(),
                                column: column_schema.name.clone(),
                                expected: column_schema.pg_type.clone(),
                                message: format!(
                                    "column \"{}\" of relation \"{}\" expects type {}: {error}",
                                    column_schema.name, schema.name, column_schema.pg_type
                                ),
                            },
                        },
                    )?;
                if matches!(value, SqlValue::Null)
                    && !column_schema.hidden
                    && (!column_schema.nullable || column_schema.primary_key)
                {
                    return Err(constraint_violation(
                        "23502",
                        format!(
                            "null value in column \"{}\" of relation \"{}\" violates not-null constraint",
                            column_schema.name, table
                        ),
                        Some(table.to_string()),
                        Some(column_schema.name.clone()),
                        Some(not_null_constraint_name(table, &column_schema.name)),
                    ));
                }
                let storage =
                    if is_json_pg_type(&column_schema.pg_type) && matches!(value, SqlValue::Null) {
                        None
                    } else {
                        Some({
                            let mut out = Vec::new();
                            sql_value_write_column_storage(
                                &mut out,
                                value.clone(),
                                Some(column_schema),
                            )?;
                            String::from_utf8(out).expect("JSON rendering is UTF-8")
                        })
                    };
                assignments.push(StoredAssignment {
                    column: column_schema.name.clone(),
                    value,
                    storage,
                });
            }
            // Repair deltas exactly as the generic path derives them.
            let repair_plan = match repair_assignments.as_ref() {
                Some(repairable) => {
                    let mut deltas = Vec::with_capacity(repairable.len());
                    let mut repairable_row = true;
                    for assignment in repairable {
                        let value = match row_engine.eval_slot_row_value(
                            &candidate_row,
                            &context,
                            &assignment.delta_expr,
                        ) {
                            Ok(value) => value,
                            Err(SqlError::Unsupported(_)) => return Ok(None),
                            Err(error) => return Err(error),
                        };
                        let delta = match value {
                            SqlValue::Int(value) => match assignment.negate {
                                true => match value.checked_neg() {
                                    Some(value) => RepairDelta::Int(value),
                                    None => {
                                        repairable_row = false;
                                        break;
                                    }
                                },
                                false => RepairDelta::Int(value),
                            },
                            SqlValue::Float(value) => {
                                RepairDelta::Float(if assignment.negate { -value } else { value })
                            }
                            SqlValue::String(ref value) => {
                                match bicdb_core::parse_repair_decimal(value) {
                                    Some((units, scale)) => RepairDelta::Decimal {
                                        units: if assignment.negate { -units } else { units },
                                        scale,
                                    },
                                    None => {
                                        repairable_row = false;
                                        break;
                                    }
                                }
                            }
                            _ => {
                                repairable_row = false;
                                break;
                            }
                        };
                        deltas.push((assignment.storage_key.clone(), delta));
                    }
                    repairable_row.then(|| RepairPlan { deltas })
                }
                None => None,
            };
            let mut patches = assignments
                .iter()
                .map(|assignment| (assignment.column.as_str(), assignment.storage.as_deref()))
                .collect::<Vec<_>>();
            patches.sort_by(|a, b| a.0.cmp(b.0));
            // Spliced from the row's own bytes when its text is compact
            // (every resident row): no parse to patch, no parse to validate.
            let new_stored =
                match bicdb_core::splice_json_object_text(stored.metadata.get(), &patches) {
                    Some(text) => Arc::new(
                        stored
                            .with_metadata_text_spliced(text)
                            .map_err(SqlError::from)?,
                    ),
                    None => {
                        let Some(text) =
                            bicdb_core::patch_json_object_text(stored.metadata.get(), &patches)
                        else {
                            return Ok(None);
                        };
                        Arc::new(stored.with_metadata_text(text).map_err(SqlError::from)?)
                    }
                };
            if update.returning.is_some() {
                // Assignment and delta evaluation have finished borrowing the
                // candidate. RETURNING owns it now; copying its string payloads
                // here only to discard the original is unnecessary.
                let mut row = candidate_row;
                for assignment in &assignments {
                    for (slot_idx, target) in target_columns.iter().enumerate() {
                        let Some((relation, name)) = target.rsplit_once('.') else {
                            continue;
                        };
                        if name == assignment.column
                            && (relation == target_alias || relation == table)
                        {
                            if let Some(slot) = row.get_mut(slot_idx) {
                                *slot = assignment.value.clone();
                            }
                        }
                    }
                }
                returning_rows.push(row);
            }
            rows.push((stored, new_stored, statement_snapshot, repair_plan));
        }
        let updated = rows.len();
        // The rows were spliced from their pre-images on exactly these keys:
        // commit skips every index that reads none of them.
        self.update_session_stored_records(table, rows, Arc::clone(&assigned.changed))?;
        #[cfg(test)]
        SQL_STORED_UPDATE_HITS.with(|hits| *hits.borrow_mut() += 1);
        if let Some(returning) = &update.returning {
            // The no-FROM case has exactly the same slot layout contract as
            // UPDATE FROM. Keep it columnar instead of rebuilding a name map
            // with qualified and unqualified copies of every cell.
            // An alias adds a second set of table-qualified lookup slots, not
            // a second set of columns to an unqualified RETURNING *.
            let wildcard_columns = if from_rows.is_none() {
                &target_columns[..fields.len()]
            } else {
                target_columns.as_ref()
            };
            return self
                .project_update_from_returning_slot_rows(
                    table,
                    Some(schema),
                    returning,
                    &returning_rows,
                    candidate_columns.as_ref(),
                    wildcard_columns,
                )
                .map(Some);
        }
        Ok(Some(SqlResult::command(format!("UPDATE {updated}"))))
    }

    /// The candidate rows for a stored-form UPDATE: `None` unless every id
    /// resolves to a resident row whose cells read through the cell plan.
    fn stored_update_candidates(
        &self,
        engine: &SqlEngine<'_>,
        table: &str,
        alias: &str,
        fields: &[FieldRef],
        ids: &[String],
    ) -> Result<Option<Vec<(usize, SlotRow, Arc<bicdb_core::StoredRecord>)>>> {
        let visible = engine.visible_rows_for_pks(table, ids)?;
        let mut out = Vec::with_capacity(visible.len());
        let mut cells: bicdb_core::CellRow<'_> = Vec::with_capacity(32);
        let mut plan: Option<CellPlan> = None;
        for (position, row) in visible.iter().enumerate() {
            let Some(row) = row else {
                continue;
            };
            let bicdb_core::VisibleRow::Stored(stored) = row else {
                return Ok(None);
            };
            if !stored.cells_into(&mut cells) {
                return Ok(None);
            }
            let Some(slot) = slot_row_from_cells(table, alias, fields, stored, &cells, &mut plan)
            else {
                return Ok(None);
            };
            out.push((position, slot, Arc::clone(stored)));
        }
        Ok(Some(out))
    }
}
