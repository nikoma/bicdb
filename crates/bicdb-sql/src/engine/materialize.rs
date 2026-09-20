//! Split out of the parent module to keep files digestible; behavior
//! unchanged — a separate `impl` block on the same type.
use super::*;
#[allow(unused_imports)]
use crate::*;

/// Row sets smaller than this keep the typed per-row WHERE evaluator: the
/// exactness check, the variable fold and the bind cost about as much as a
/// handful of typed row evaluations.
const BOUND_ROW_FILTER_MIN_ROWS: usize = 8;

impl<'db> SqlEngine<'db> {
    pub(crate) fn execute_projection_array_set_functions(
        &self,
        select: &Select,
    ) -> Result<Option<SqlResult>> {
        let calls = array_projection_set_returning_calls(&select.projection)?;
        if calls.is_empty() {
            return Ok(None);
        }
        let include_rows = match &select.selection {
            Some(selection) => {
                sql_value_truth(self.eval_select_constant_expr(selection)?)?.unwrap_or(false)
            }
            None => true,
        };
        let mut columns = Vec::with_capacity(select.projection.len());
        let mut column_types = Vec::with_capacity(select.projection.len());
        let mut base_row = Vec::with_capacity(select.projection.len());
        let mut expanded_sets = Vec::with_capacity(calls.len());
        let mut output_indices = Vec::with_capacity(calls.len());
        for (projection_index, item) in select.projection.iter().enumerate() {
            let (expr, alias) = match item {
                SelectItem::UnnamedExpr(expr) => (expr, None),
                SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
                other => {
                    return Err(SqlError::Unsupported(format!(
                        "unsupported SELECT without FROM expression {other}"
                    )));
                }
            };
            columns.push(alias.unwrap_or_else(|| select_expr_column_name(expr)));
            if let Some(call) = calls
                .iter()
                .find(|call| call.projection_index == projection_index)
            {
                let arguments = call
                    .arguments
                    .iter()
                    .map(|argument| self.eval_select_constant_expr(argument))
                    .collect::<Result<Vec<_>>>()?;
                expanded_sets.push(array_projection_set_values(call.function, arguments)?);
                output_indices.push(projection_index);
                column_types.push(match call.function {
                    ArrayProjectionSetReturningFunction::GenerateSubscripts => {
                        Some("int4".to_string())
                    }
                    ArrayProjectionSetReturningFunction::PgSnapshotXip => Some("xid8".to_string()),
                    ArrayProjectionSetReturningFunction::TxidSnapshotXip => {
                        Some("int8".to_string())
                    }
                    ArrayProjectionSetReturningFunction::Unnest => None,
                });
                base_row.push(SqlValue::Null);
            } else {
                column_types.push(projected_expr_pg_type(expr, None));
                base_row.push(self.eval_select_constant_expr(expr)?);
            }
        }
        let row_count = if include_rows {
            expanded_sets.iter().map(Vec::len).max().unwrap_or_default()
        } else {
            0
        };
        let mut rows = Vec::with_capacity(row_count);
        for expanded_index in 0..row_count {
            let mut row = base_row.clone();
            for (set_index, output_index) in output_indices.iter().copied().enumerate() {
                row[output_index] = expanded_sets[set_index]
                    .get(expanded_index)
                    .cloned()
                    .unwrap_or(SqlValue::Null);
            }
            rows.push(row);
        }
        Ok(Some(
            SqlResult::new(columns, rows).with_column_types(column_types),
        ))
    }

    pub(crate) fn execute_window_query_without_from(
        &self,
        select: &Select,
        query: &Query,
    ) -> Result<SqlResult> {
        let mut rows = vec![SlotRow::new()];
        let mut columns = Vec::new();
        self.apply_row_windows(select, query, &mut rows, &mut columns)?;
        self.apply_row_order_by(
            &mut rows,
            query.order_by.as_ref(),
            &columns,
            &[],
            order_by_keep_bound(query)?,
        )?;
        apply_row_limit(&mut rows, query)?;
        let result =
            self.project_slot_row_select_with_wildcard(&select.projection, &rows, &columns, &[])?;
        self.typed_row_result(result, select, None)
    }

    /// PostgreSQL permits set-returning functions in the SELECT list. Expand one
    /// top-level JSON SRF and repeat any scalar siblings for every emitted value.
    pub(crate) fn execute_projection_json_set_function(
        &self,
        select: &Select,
    ) -> Result<Option<SqlResult>> {
        let Some(call) = json_projection_set_returning_call(&select.projection)? else {
            return Ok(None);
        };
        let arguments = call
            .arguments
            .iter()
            .map(|argument| self.eval_select_constant_expr(argument))
            .collect::<Result<Vec<_>>>()?;
        let expanded = json_set_function_values_from_args(call.function, &arguments)?;
        let mut columns = Vec::with_capacity(select.projection.len());
        let mut column_types = Vec::with_capacity(select.projection.len());
        let mut base_row = Vec::with_capacity(select.projection.len());
        for (index, item) in select.projection.iter().enumerate() {
            let (expr, alias) = match item {
                SelectItem::UnnamedExpr(expr) => (expr, None),
                SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
                other => {
                    return Err(SqlError::Unsupported(format!(
                        "unsupported SELECT without FROM expression {other}"
                    )));
                }
            };
            columns.push(alias.unwrap_or_else(|| select_expr_column_name(expr)));
            if index == call.projection_index {
                column_types.push(Some(call.function.pg_type().to_string()));
                base_row.push(SqlValue::Null);
            } else {
                column_types.push(projected_expr_pg_type(expr, None));
                base_row.push(self.eval_select_constant_expr(expr)?);
            }
        }
        let rows = expanded
            .into_iter()
            .map(|value| {
                let mut row = base_row.clone();
                row[call.projection_index] = value;
                row
            })
            .collect();
        Ok(Some(
            SqlResult::new(columns, rows).with_column_types(column_types),
        ))
    }

    pub(crate) fn eval_select_constant_expr(&self, expr: &Expr) -> Result<SqlValue> {
        if let Expr::Identifier(ident) = expr {
            if ident.value.eq_ignore_ascii_case("current_date") {
                return Ok(SqlValue::String(unix_now_date_string()));
            }
            if ident.quote_style.is_none() {
                if let Some(value) = self.session_identity_value(&ident.value) {
                    return Ok(value);
                }
            }
        }
        if self.outer_row.is_some() {
            // The inherited row is correlated, not a local FROM row. Keeping
            // the local context empty preserves routine-variable precedence.
            let (_scope, context) = self.bound_row_context(&[]);
            return self.eval_slot_row_value(&SlotRow::new(), &context, expr);
        }
        match expr {
            Expr::Identifier(ident)
                if ident.quote_style.is_none()
                    && self.session_identity_value(&ident.value).is_some() =>
            {
                Ok(self
                    .session_identity_value(&ident.value)
                    .expect("identity value checked above"))
            }
            Expr::Value(value) => routine_var_from_value(&self.routine_vars, value)
                .map(Ok)
                .unwrap_or_else(|| literal_to_value(value)),
            Expr::Identifier(ident) => routine_var_from_ident(&self.routine_vars, ident)
                .ok_or_else(|| {
                    SqlError::Unsupported(format!("unsupported SELECT expression {expr}"))
                }),
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                let mut branch_types = Vec::with_capacity(conditions.len() + 1);
                branch_types.push(
                    else_result
                        .as_deref()
                        .and_then(|result| projected_expr_pg_type_with_db(self.db_ref(), result)),
                );
                branch_types.extend(conditions.iter().map(|condition| {
                    projected_expr_pg_type_with_db(self.db_ref(), &condition.result)
                }));
                select_common_pg_type(self.db_ref(), &branch_types, "CASE")?;
                let operand_value = operand
                    .as_deref()
                    .map(|operand| self.eval_select_constant_expr(operand))
                    .transpose()?;
                for condition in conditions {
                    let matched = if let Some(operand_value) = &operand_value {
                        values_equal(
                            operand_value,
                            &self.eval_select_constant_expr(&condition.condition)?,
                        )
                    } else {
                        sql_value_truth(self.eval_select_constant_expr(&condition.condition)?)?
                            .unwrap_or(false)
                    };
                    if matched {
                        return self.eval_select_constant_expr(&condition.result);
                    }
                }
                else_result
                    .as_deref()
                    .map(|result| self.eval_select_constant_expr(result))
                    .unwrap_or(Ok(SqlValue::Null))
            }
            Expr::Function(function) => {
                let name = object_name(&function.name)?.to_ascii_lowercase();
                let arg_exprs = function_args(function);
                let arg_types = arg_exprs
                    .iter()
                    .map(|arg| projected_expr_pg_type_with_db(self.db_ref(), arg))
                    .collect::<Vec<_>>();
                let bare_name = name.strip_prefix("pg_catalog.").unwrap_or(&name);
                if matches!(
                    bare_name,
                    "coalesce" | "nullif" | "greatest" | "least" | "ifnull"
                ) {
                    select_common_pg_type(
                        self.db_ref(),
                        &arg_types,
                        &bare_name.to_ascii_uppercase(),
                    )?;
                }
                if matches!(
                    bare_name,
                    "array_append"
                        | "array_prepend"
                        | "array_remove"
                        | "array_replace"
                        | "array_cat"
                ) {
                    projected_expr_pg_type_with_db(self.db_ref(), expr).ok_or_else(|| {
                        SqlError::data_exception_public(
                            "42804",
                            format!("could not determine polymorphic type for {bare_name}"),
                            None,
                        )
                    })?;
                }
                let args = arg_exprs
                    .iter()
                    .map(|arg| self.eval_select_constant_expr(arg))
                    .collect::<Result<Vec<_>>>()?;
                if is_row_constructor(function) {
                    return Ok(anonymous_record_value(args, arg_types));
                }
                if matches!(name.as_str(), "pg_typeof" | "pg_catalog.pg_typeof") {
                    return Ok(pg_typeof_result(
                        arg_types.first().and_then(Option::as_ref),
                        args.first(),
                    ));
                }
                if let Some(value) = self.eval_runtime_function_value(&name, &args)? {
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
                    self.tx.map(Transaction::visibility_watermark),
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
                    self.tx,
                )? {
                    return Ok(value);
                }
                if let Some(value) = eval_json_function_call_value(function, &args)? {
                    return Ok(value);
                }
                if let Some(value) = crate::eval_xml_function_value(&name, &args, Some(&arg_types))?
                {
                    return Ok(value);
                }
                if let Some(value) = self.eval_routing_function_value_authorized(&name, &args)? {
                    return Ok(value);
                }
                if let Some(value) = eval_fts_function_value(&name, &args, Some(&arg_types))? {
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
                eval_builtin_function_value(function)
            }
            Expr::Cast {
                expr, data_type, ..
            } => {
                if let Some(source) = regclass_display_cast_source(expr, data_type) {
                    let value = self.eval_select_constant_expr(source)?;
                    return regclass_text_value(self.db_ref(), value);
                }
                if let Some(source) = regtype_text_cast_source(expr, data_type)? {
                    let value = self.eval_select_constant_expr(source)?;
                    return regtype_text_value(self.db_ref(), value);
                }
                if let Some(source) = regclass_text_cast_source(expr, data_type)? {
                    let value = self.eval_select_constant_expr(source)?;
                    return regclass_text_value(self.db_ref(), value);
                }
                cast_expr_value_with_db(
                    self.db_ref(),
                    self.eval_select_constant_expr(expr)?,
                    expr,
                    data_type,
                    None,
                )
            }
            Expr::Position { expr, r#in } => eval_position_typed_value(
                self.eval_select_constant_expr(expr)?,
                self.eval_select_constant_expr(r#in)?,
                projected_expr_pg_type_with_db(self.db_ref(), expr).as_deref() == Some("bytea")
                    || projected_expr_pg_type_with_db(self.db_ref(), r#in).as_deref()
                        == Some("bytea"),
            ),
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
                |expr| self.eval_select_constant_expr(expr),
            ),
            Expr::Nested(expr) => self.eval_select_constant_expr(expr),
            Expr::Array(array) => sql_array_value({
                let types = array
                    .elem
                    .iter()
                    .map(|expr| projected_expr_pg_type_with_db(self.db_ref(), expr))
                    .collect::<Vec<_>>();
                select_common_pg_type(self.db_ref(), &types, "ARRAY")?;
                array
                    .elem
                    .iter()
                    .map(|expr| self.eval_select_constant_expr(expr))
                    .collect::<Result<Vec<_>>>()?
            }),
            Expr::Tuple(exprs) => Ok(anonymous_record_value(
                exprs
                    .iter()
                    .map(|expr| self.eval_select_constant_expr(expr))
                    .collect::<Result<Vec<_>>>()?,
                exprs
                    .iter()
                    .map(|expr| projected_expr_pg_type_with_db(self.db_ref(), expr))
                    .collect(),
            )),
            Expr::CompoundFieldAccess { root, access_chain } => eval_access_chain_expr(
                root,
                access_chain,
                projected_expr_pg_type_with_db(self.db_ref(), root).as_deref() == Some("jsonb"),
                |expr| self.eval_select_constant_expr(expr),
            ),
            Expr::Exists { subquery, negated } => {
                let result = self.execute_query(subquery)?;
                Ok(SqlValue::Bool(result.rows.is_empty() == *negated))
            }
            Expr::Subquery(query) => self.execute_scalar_subquery(query),
            _ => eval_constant_expr(expr),
        }
    }

    pub(crate) fn execute_virtual_table(
        &self,
        select: &Select,
        query: &Query,
        table: &str,
    ) -> Result<SqlResult> {
        let alias = table.rsplit('.').next().unwrap_or(table);
        let mut rows =
            self.session_virtual_rows_with_selection(table, alias, select.selection.as_ref())?;
        if let Some(selection) = &select.selection {
            let mut filtered = Vec::new();
            for (idx, row) in rows.into_iter().enumerate() {
                if idx % 1024 == 0 {
                    self.check_cancellation()?;
                }
                if eval_virtual_predicate(&row, selection).unwrap_or(false) {
                    filtered.push(row);
                }
            }
            rows = filtered;
        }

        if has_aggregates(&select.projection) {
            let row_columns = aliased_virtual_columns(table, alias);
            let row_set = rows
                .into_iter()
                .map(|row| {
                    let row = row_from_virtual_row(table, alias, row);
                    slot_row_from_sql_row(&row_columns, &row)
                })
                .collect::<Vec<_>>();
            return self.execute_row_aggregates(&select.projection, &row_set, &row_columns);
        }

        self.check_cancellation()?;
        apply_virtual_order_by(&mut rows, query.order_by.as_ref())?;
        self.check_cancellation()?;
        apply_virtual_limit(&mut rows, query)?;
        let fallback_columns = virtual_table_columns(table).unwrap_or_default();
        project_virtual_rows(&select.projection, rows, &fallback_columns)
    }

    /// Load a projection, follow the stream to current, and return its cells
    /// as rows. Catching up on read means a query never silently serves state
    /// older than the events already committed — and `bicdb_projections`
    /// reports the lag if it is behind.
    pub(crate) fn projection_relation_rows(
        &self,
        name: &str,
    ) -> Result<Vec<BTreeMap<String, SqlValue>>> {
        let db = self.db_ref();
        let directory = db
            .data_path()
            .join(bicdb_core::aggregate_projection::PROJECTIONS_DIR);
        let mut projection =
            bicdb_core::aggregate_projection::AggregateProjection::load(&directory, name)
                .map_err(SqlError::from)?
                .ok_or_else(|| {
                    SqlError::InvalidCollection(format!(
                        "{}{name}",
                        crate::select_exec::PROJECTION_RELATION_PREFIX
                    ))
                })?;
        // A cube is a materialized aggregate OVER a base table; reading it
        // discloses that table's dimension values and measures. Authorization
        // must match a direct read of the base table — the cube dispatches as
        // a virtual relation, so without this it bypassed the table gate and
        // RLS entirely. Resolve and check the base collection before emitting
        // any cell.
        self.require_relation_privilege(projection.collection_name(), "SELECT")?;
        projection.catch_up(db).map_err(SqlError::from)?;
        let mut rows = Vec::new();
        for cells in projection.relation_rows() {
            let mut row = BTreeMap::new();
            for (column, number, text) in cells {
                let value = match (number, text) {
                    (Some(number), _) => {
                        // Counts are integers — including distinct-counts,
                        // which are approximate but still counts. The ROLLUP
                        // and MERGE surfaces type them the same way, so the
                        // same measure never appears as `15` in one place and
                        // `15.0068` in another.
                        if column == "count" || column.starts_with("distinct_") {
                            SqlValue::Int(number as i64)
                        } else {
                            SqlValue::Float(number)
                        }
                    }
                    (None, Some(text)) => SqlValue::String(text),
                    (None, None) => SqlValue::Null,
                };
                row.insert(column, value);
            }
            rows.push(row);
        }
        Ok(rows)
    }

    /// One row per durable projection: freshness and footprint, so an
    /// operator can see whether a projection is current before trusting it.
    pub(crate) fn projection_status_rows(&self) -> Result<Vec<BTreeMap<String, SqlValue>>> {
        let db = self.db_ref();
        let directory = db
            .data_path()
            .join(bicdb_core::aggregate_projection::PROJECTIONS_DIR);
        let source_position =
            bicdb_core::aggregate_projection::AggregateProjection::source_position(db);
        let mut rows = Vec::new();
        for name in bicdb_core::aggregate_projection::list_projections(db.data_path()) {
            let Some(projection) =
                bicdb_core::aggregate_projection::AggregateProjection::load(&directory, &name)
                    .map_err(SqlError::from)?
            else {
                continue;
            };
            // Only surface cubes whose base table the role may read: the status
            // row discloses the base collection name, its dimensions and its
            // measures. Skipping (rather than erroring) matches how catalog
            // views hide objects a role has no privilege on.
            if self
                .require_relation_privilege(projection.collection_name(), "SELECT")
                .is_err()
            {
                continue;
            }
            let residency = projection.residency();
            let mut row = BTreeMap::new();
            row.insert("name".to_string(), SqlValue::String(name));
            row.insert(
                "collection".to_string(),
                SqlValue::String(projection.collection_name().to_string()),
            );
            row.insert(
                "dimensions".to_string(),
                SqlValue::String(projection.dimension_names().join(", ")),
            );
            row.insert(
                "measures".to_string(),
                SqlValue::String(projection.measure_names().join(", ")),
            );
            row.insert(
                "projection_position".to_string(),
                SqlValue::Int(projection.projection_position() as i64),
            );
            row.insert(
                "source_position".to_string(),
                SqlValue::Int(source_position as i64),
            );
            row.insert(
                "lag_events".to_string(),
                SqlValue::Int(projection.lag_events(source_position) as i64),
            );
            row.insert("cells".to_string(), SqlValue::Int(residency.cells as i64));
            row.insert(
                "source_rows".to_string(),
                SqlValue::Int(residency.input_records as i64),
            );
            row.insert(
                "resident_bytes".to_string(),
                SqlValue::Int(residency.total_bytes() as i64),
            );
            row.insert(
                "input_logical_bytes".to_string(),
                SqlValue::Int(residency.input_logical_bytes as i64),
            );
            rows.push(row);
        }
        Ok(rows)
    }

    pub(crate) fn session_virtual_rows_with_selection(
        &self,
        table: &str,
        alias: &str,
        selection: Option<&Expr>,
    ) -> Result<Vec<BTreeMap<String, SqlValue>>> {
        // Durable aggregate projections, exposed as ordinary relations so the
        // full SELECT machinery (WHERE, ORDER BY, GROUP BY, LIMIT) applies
        // without any cube-specific syntax.
        if let Some(name) = table.strip_prefix(crate::select_exec::PROJECTION_RELATION_PREFIX) {
            return self.projection_relation_rows(name);
        }
        if table == "bicdb_projections" {
            return self.projection_status_rows();
        }
        let stripped = table.strip_prefix("pg_catalog.").unwrap_or(table);
        if stripped == "pg_locks" {
            if let Some(runtime) = &self.runtime {
                let database = current_database_from_gucs(&self.session_gucs);
                let database_oid = pg_database_rows(self.db_ref())?
                    .into_iter()
                    .find(|row| {
                        sql_value_text(&virtual_cell(row, "datname"))
                            .is_some_and(|name| name == database)
                    })
                    .and_then(|row| sql_value_i64(&virtual_cell(&row, "oid")))
                    .ok_or_else(|| {
                        SqlError::InvalidSql(format!(
                            "current database {database} is missing from pg_database"
                        ))
                    })?;
                return Ok(runtime.advisory_lock_rows(database_oid));
            }
        }
        let rows = virtual_rows_with_selection(self.db_ref(), table, alias, selection)?;
        self.filter_virtual_rows_for_role(table, rows)
    }

    /// Per-role visibility for virtual catalogs whose rows carry user data.
    /// Applied at the session seam (the row builders take only a `&BicDb` and
    /// have no identity), so every session read of these catalogs passes
    /// through it.
    pub(crate) fn filter_virtual_rows_for_role(
        &self,
        table: &str,
        rows: Vec<BTreeMap<String, SqlValue>>,
    ) -> Result<Vec<BTreeMap<String, SqlValue>>> {
        let role = rls_check_user_from_gucs(&self.session_gucs);
        if let Some((key, columns)) = virtual_catalog_redactions(table) {
            return redact_catalog_rows_for_role(self.db_ref(), &role, &key, columns, rows);
        }
        let Some(key) = virtual_catalog_relation_key(table) else {
            return Ok(rows);
        };
        filter_catalog_rows_for_role(self.db_ref(), &role, &key, rows)
    }

    pub(crate) fn execute_sequence_relation(
        &self,
        select: &Select,
        query: &Query,
        sequence: SequenceSchema,
    ) -> Result<SqlResult> {
        let mut rows = vec![virtual_row([
            ("last_value", SqlValue::Int(sequence.last_value)),
            ("log_cnt", SqlValue::Int(0)),
            ("is_called", SqlValue::Bool(sequence.is_called)),
        ])];
        if let Some(selection) = &select.selection {
            let mut filtered = Vec::new();
            for (idx, row) in rows.into_iter().enumerate() {
                if idx % 1024 == 0 {
                    self.check_cancellation()?;
                }
                if eval_virtual_predicate(&row, selection).unwrap_or(false) {
                    filtered.push(row);
                }
            }
            rows = filtered;
        }
        self.check_cancellation()?;
        apply_virtual_order_by(&mut rows, query.order_by.as_ref())?;
        self.check_cancellation()?;
        apply_virtual_limit(&mut rows, query)?;
        project_virtual_rows(&select.projection, rows, &[])
    }

    pub(crate) fn execute_row_query(&self, select: &Select, query: &Query) -> Result<SqlResult> {
        self.check_cancellation()?;
        let normalized_from;
        let from = match select.from.as_slice() {
            [] => {
                return Err(SqlError::Unsupported(
                    "row query execution requires at least one FROM item".to_string(),
                ));
            }
            [from] => from,
            from_items => {
                normalized_from = Self::comma_from_items_to_cross_join(from_items);
                &normalized_from
            }
        };
        // Routine-owned two-relation primary-key joins: two point lookups
        // through the per-IR-node template, no join planning per call.
        if ir_join_plan::enabled() && self.ir_owned_statement {
            if let Some(result) = self.try_cached_point_join_select(from, select, query)? {
                return Ok(result);
            }
        }
        if let TableFactor::Table { name, .. } = &from.relation {
            self.reject_encrypted_predicates(&relation_name(name)?, select.selection.as_ref())?;
        }
        if let Some(result) = self.odoo_auto_install_dependency_query(select, query, from)? {
            return Ok(result);
        }
        if let Some(result) = self.active_record_table_name_query(select, query, from)? {
            return Ok(result);
        }
        if let Some(result) = self.pg_dump_relation_inventory_query(select, query, from)? {
            return Ok(result);
        }
        if let Some(result) = self.pg_dump_column_info_query(select, query, from)? {
            return Ok(result);
        }
        if let Some(result) = self.pg_dump_constraint_inventory_query(select, query, from)? {
            return Ok(result);
        }
        if let Some(result) = self.pg_dump_extension_fk_dependency_query(select, query, from)? {
            return Ok(result);
        }
        if let Some(result) = self.pg_dump_proc_inventory_query(select, query, from)? {
            return Ok(result);
        }
        if let Some(result) = self.pg_dump_type_query(select, query, from)? {
            return Ok(result);
        }
        if let Some(result) = self.pg_dump_cast_inventory_query(select, query, from)? {
            return Ok(result);
        }
        // An aggregate over a WHOLE UNFILTERED TABLE materializes every row here
        // (below) to emit a single row: collecting aggregates (string_agg /
        // array_agg / json_agg) cannot stream. With no WHERE and no GROUP BY the
        // input is the entire table, an O(table) allocation any role can trigger.
        // Refuse before the allocation when the estimate exceeds the ceiling;
        // filtered, grouped, and streaming aggregates never reach this shape.
        if select.selection.is_none()
            && !has_group_by(select)?
            && has_aggregates(&select.projection)
        {
            let cap = crate::engine::max_aggregate_scan_rows();
            if cap != 0 {
                if let TableFactor::Table { name, args, .. } = &from.relation {
                    if let Ok(collection) = relation_name(name) {
                        // Estimate the input size: a real table from its record
                        // count, or a `generate_series(...)` from its bounds (its
                        // rows drive string_agg/array_agg exactly as a table's do,
                        // and it can name an arbitrarily large range).
                        let estimate = match args {
                            None => self
                                .db_ref()
                                .estimated_record_count(&collection)
                                .unwrap_or(0),
                            Some(table_args)
                                if crate::virtual_tables::is_generate_series_table_function(
                                    &collection,
                                ) =>
                            {
                                crate::virtual_tables::estimate_generate_series_rows(table_args)
                                    .unwrap_or(0)
                            }
                            Some(_) => 0,
                        };
                        if estimate > cap {
                            let schema = load_schema(self.db_ref(), &collection)?;
                            let functions = query_group_aggregate_functions(select, query)?;
                            if any_collecting_aggregate(&functions, schema.as_ref()) {
                                return Err(SqlError::resource_limit(
                                    "53400",
                                    format!(
                                        "aggregate over an estimated {estimate} rows of `{collection}` exceeds the non-streaming materialization limit of {cap}; add a WHERE filter or GROUP BY, use a streaming aggregate (count/sum/min/max), or raise BICDB_MAX_AGGREGATE_ROWS"
                                    ),
                                ));
                            }
                        }
                    }
                }
            }
        }
        let projection_json_srf = json_projection_set_returning_call(&select.projection)?;
        let projection_array_srfs = array_projection_set_returning_calls(&select.projection)?;
        let bounded_row_set = if projection_json_srf.is_none() && projection_array_srfs.is_empty() {
            self.try_bounded_ordered_index_row_set(from, select, query)?
        } else {
            None
        };
        let row_set = match bounded_row_set {
            Some(row_set) => row_set,
            None => {
                let needed_columns = self.referenced_column_names_cached(select, query);
                self.row_set_from_table_with_joins_with_selection(
                    from,
                    select.selection.as_ref(),
                    Some(&select.projection),
                    query.order_by.as_ref(),
                    needed_columns.as_deref(),
                )?
            }
        };
        self.execute_materialized_row_query(select, query, from, row_set)
    }

    pub(crate) fn execute_materialized_row_query(
        &self,
        select: &Select,
        query: &Query,
        from: &TableWithJoins,
        row_set: RowSet,
    ) -> Result<SqlResult> {
        self.execute_materialized_row_query_inner(select, query, from, row_set, None)
    }

    pub(crate) fn execute_materialized_row_query_with_locks(
        &self,
        select: &Select,
        query: &Query,
        from: &TableWithJoins,
        row_set: RowSet,
        targets: &[super::row_locks::RowLockTarget],
    ) -> Result<SqlResult> {
        self.execute_materialized_row_query_inner(select, query, from, row_set, Some(targets))
    }

    fn execute_materialized_row_query_inner(
        &self,
        select: &Select,
        query: &Query,
        from: &TableWithJoins,
        mut row_set: RowSet,
        locks: Option<&[super::row_locks::RowLockTarget]>,
    ) -> Result<SqlResult> {
        if locks.is_some() {
            let limit = match query.limit_clause.as_ref() {
                Some(LimitClause::LimitOffset { limit, .. }) => limit.as_ref(),
                Some(LimitClause::OffsetCommaLimit { limit, .. }) => Some(limit),
                None => None,
            };
            if limit.map(optional_count_expr).transpose()?.flatten() == Some(0) {
                row_set.rows.clear();
            }
        }
        let projection_json_srf = json_projection_set_returning_call(&select.projection)?;
        let projection_array_srfs = array_projection_set_returning_calls(&select.projection)?;
        if let Some(selection) = &select.selection {
            // The WHERE re-check over the materialized rows: bound once per
            // statement when the binder accepts the expression (the same
            // evaluator the IR point lookups filter with), else the typed
            // per-row evaluator. The join and index stages consume their
            // key terms exactly, so only the residual terms actually cost
            // anything here, but every term is still applied.
            let predicate_env = self.from_relation_columns(&select.from).unwrap_or_default();
            let (scope, context) = self.bound_row_context(&row_set.columns);
            // Fold the variable-only arithmetic first (`next_o - 20`), with the
            // typed evaluator's integer range check, so the bound tree compares
            // columns against plain values; an empty row set skips the filter
            // the way the per-row evaluator never runs on zero rows.
            // Binding costs about as much as one typed row evaluation, so the
            // bound path is only worth taking over a row set of some size.
            let folded = (crate::bound_row_filter::enabled()
                && row_set.rows.len() >= BOUND_ROW_FILTER_MIN_ROWS
                && self.bound_row_filter_is_exact(selection, &predicate_env))
            .then(|| self.fold_variable_operands(selection, &predicate_env, &context))
            .transpose()?
            .flatten();
            match folded.as_ref().and_then(|folded| scope.bind(folded)) {
                Some(bound) => {
                    let mut filtered = Vec::with_capacity(row_set.rows.len());
                    for (idx, row) in row_set.rows.into_iter().enumerate() {
                        if idx % 1024 == 0 {
                            self.check_cancellation()?;
                        }
                        if bound
                            .eval_truth(&BoundExprFrame {
                                user_calls: &[],
                                db: self.db_ref(),
                                columns: BoundExprColumns::Values(&row),
                                vars: &context.var_values,
                            })?
                            .unwrap_or(false)
                        {
                            filtered.push(row);
                        }
                    }
                    #[cfg(test)]
                    SQL_BOUND_ROW_FILTER_HITS.with(|hits| *hits.borrow_mut() += 1);
                    row_set.rows = filtered;
                }
                None => {
                    row_set.rows = self.filter_row_predicate_typed(
                        row_set.rows,
                        &row_set.columns,
                        selection,
                        &predicate_env,
                    )?;
                }
            }
        }

        // Resolve the (single) base-table schema, if any, so result columns can be
        // typed from the schema/expression rather than from the runtime value
        // width. `None` for joins/CTEs/views/virtual tables — those fall back to
        // the wire layer's value-width heuristic.
        let schema = self.single_relation_schema(from);
        let schema_ref = schema.as_deref();

        let group_exprs = group_by_exprs(select)?;
        if !group_exprs.is_empty() {
            let group_env = self.from_relation_columns(&select.from).unwrap_or_default();
            let group_types = group_exprs
                .iter()
                .map(|expr| self.infer_env_expr_type(expr, &group_env))
                .collect::<Vec<_>>();
            if let Some(pg_type) = group_types
                .iter()
                .find_map(|pg_type| type_without_comparison_operators(pg_type.as_deref()))
            {
                return Err(SqlError::undefined_function(format!(
                    "could not identify an equality operator for type {pg_type}"
                )));
            }
            if select_has_window_functions(select, query)? || select.having.is_some() {
                let result = self.execute_grouped_window_rows(
                    select,
                    &group_exprs,
                    query,
                    &row_set.columns,
                    row_set.rows,
                    &group_types,
                )?;
                return self.typed_row_result(result, select, schema_ref);
            }
            let result = self.execute_grouped_rows(
                &select.projection,
                &group_exprs,
                query,
                &row_set.columns,
                row_set.rows,
                &group_types,
            )?;
            return self.typed_row_result(result, select, schema_ref);
        }
        if let Some(having) = &select.having {
            self.check_cancellation()?;
            let mut result =
                self.execute_row_aggregates(&select.projection, &row_set.rows, &row_set.columns)?;
            if !self
                .eval_row_aggregate_truth(&row_set.rows, &row_set.columns, having)?
                .unwrap_or(false)
            {
                result.rows.clear();
            }
            return self.typed_row_result(result, select, schema_ref);
        }
        if has_aggregates(&select.projection) {
            self.check_cancellation()?;
            if select_has_window_functions(select, query)? {
                let result = self.execute_grouped_window_rows(
                    select,
                    &[],
                    query,
                    &row_set.columns,
                    row_set.rows,
                    &[],
                )?;
                return self.typed_row_result(result, select, schema_ref);
            }
            let mut result =
                self.execute_row_aggregates(&select.projection, &row_set.rows, &row_set.columns)?;
            // As in `execute_query`: LIMIT/OFFSET apply to the aggregate's own
            // result row, and returning here skipped `apply_limit`.
            apply_limit(&mut result.rows, query)?;
            return self.typed_row_result(result, select, schema_ref);
        }

        let wildcard_columns = row_set
            .columns
            .iter()
            .filter(|column| !is_postgres_system_column(column))
            .cloned()
            .collect::<Vec<_>>();
        self.apply_row_windows(select, query, &mut row_set.rows, &mut row_set.columns)?;
        self.check_cancellation()?;
        let mut resolved_order_by =
            resolve_order_by_projection_aliases(query.order_by.as_ref(), &select.projection);
        if locks.is_some() {
            super::row_locks::resolve_locking_order_positions(
                &mut resolved_order_by,
                &select.projection,
                &row_set.columns,
                &wildcard_columns,
            )?;
        }
        let order_env = self.from_relation_columns(&select.from).unwrap_or_default();
        let order_types = resolved_order_by
            .as_ref()
            .and_then(|order_by| match &order_by.kind {
                OrderByKind::Expressions(expressions) => Some(
                    expressions
                        .iter()
                        .map(|order| {
                            self.infer_env_expr_type(&order.expr, &order_env)
                                .or_else(|| projected_expr_pg_type(&order.expr, schema_ref))
                                .or_else(|| {
                                    row_columns_expr_pg_type(
                                        self.db_ref(),
                                        &order.expr,
                                        row_set.columns.iter().map(String::as_str),
                                    )
                                })
                        })
                        .collect::<Vec<_>>(),
                ),
                _ => None,
            })
            .unwrap_or_default();
        if let Some(pg_type) = order_types
            .iter()
            .find_map(|pg_type| type_without_comparison_operators(pg_type.as_deref()))
        {
            return Err(SqlError::undefined_function(format!(
                "could not identify an ordering operator for type {pg_type}"
            )));
        }
        // The keep bound is safe only when apply_row_limit is the NEXT
        // row-count operation: SRF expansion multiplies rows and DISTINCT
        // dedups BEFORE the limit in the branches below, so truncating at the
        // sort would starve them.
        let keep = if locks.is_none()
            && projection_array_srfs.is_empty()
            && projection_json_srf.is_none()
            && matches!(
                select.distinct.as_ref(),
                None | Some(sqlparser::ast::Distinct::All)
            ) {
            order_by_keep_bound(query)?
        } else {
            None
        };
        let locking_projection = if locks.is_some() {
            Some(self.cache_locking_order_projections(
                &select.projection,
                &mut resolved_order_by,
                &mut row_set,
                &wildcard_columns,
            )?)
        } else {
            None
        };
        self.apply_row_order_by(
            &mut row_set.rows,
            resolved_order_by.as_ref(),
            &row_set.columns,
            &order_types,
            keep,
        )?;
        self.check_cancellation()?;
        if let Some(targets) = locks {
            row_set.rows =
                self.lock_materialized_rows(query, &row_set.columns, row_set.rows, targets)?;
        }
        if !projection_array_srfs.is_empty() {
            let mut result = self.project_row_select_with_array_set_functions(
                select,
                &row_set.rows,
                &row_set.columns,
                &wildcard_columns,
                &projection_array_srfs,
            )?;
            result = self.typed_row_result(result, select, schema_ref)?;
            self.apply_select_distinct(select, &mut result)?;
            apply_row_limit(&mut result.rows, query)?;
            return Ok(result);
        }
        if let Some(call) = projection_json_srf.as_ref() {
            let mut result = self.project_row_select_with_json_set_function(
                select,
                &row_set.rows,
                &row_set.columns,
                &wildcard_columns,
                call,
            )?;
            result = self.typed_row_result(result, select, schema_ref)?;
            self.apply_select_distinct(select, &mut result)?;
            apply_row_limit(&mut result.rows, query)?;
            return Ok(result);
        }
        let result = self.project_slot_row_select_with_wildcard(
            locking_projection.as_deref().unwrap_or(&select.projection),
            &row_set.rows,
            &row_set.columns,
            &wildcard_columns,
        )?;
        let mut result = self.typed_row_result(result, select, schema_ref)?;
        self.apply_select_distinct(select, &mut result)?;
        if locks.is_none() {
            apply_row_limit(&mut result.rows, query)?;
        }
        Ok(result)
    }

    pub(crate) fn validate_money_aggregate_signatures(
        &self,
        select: &Select,
        query: &Query,
    ) -> Result<()> {
        // Only `avg` can name `money`: when no expression of the statement
        // calls a function named avg there is nothing to validate, and the
        // aggregate/window collection below (which clones every call) is
        // skipped.
        if !query_mentions_function(select, query, "avg") {
            return Ok(());
        }
        let mut functions = query_group_aggregate_functions(select, query)?;
        functions.extend(
            query_window_functions(select, query)?
                .into_iter()
                .filter(|function| is_aggregate_function(function)),
        );
        // The relation column environment (a schema load per relation) is only
        // needed to type an `avg` argument; every other statement skips it.
        let mut env: Option<Vec<RelationColumns>> = None;
        for function in functions {
            let raw_name = object_name(&function.name)?.to_ascii_lowercase();
            let name = raw_name.strip_prefix("pg_catalog.").unwrap_or(&raw_name);
            if name != "avg" {
                continue;
            }
            let argument = single_function_expr(&function)?;
            let env = env.get_or_insert_with(|| {
                self.from_relation_columns(&select.from).unwrap_or_default()
            });
            if self
                .infer_env_expr_type(&argument, env.as_slice())
                .as_deref()
                == Some("money")
            {
                return Err(SqlError::undefined_function(
                    "function avg(money) does not exist",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn apply_select_distinct(
        &self,
        select: &Select,
        result: &mut SqlResult,
    ) -> Result<()> {
        match select.distinct.as_ref() {
            None | Some(sqlparser::ast::Distinct::All) => Ok(()),
            Some(sqlparser::ast::Distinct::Distinct) => {
                if let Some(pg_type) = result
                    .column_types
                    .iter()
                    .find_map(|pg_type| type_without_comparison_operators(pg_type.as_deref()))
                {
                    return Err(SqlError::undefined_function(format!(
                        "could not identify an equality operator for type {pg_type}"
                    )));
                }
                result.rows = deduplicate_sql_rows_typed(
                    std::mem::take(&mut result.rows),
                    &result.column_types,
                )?;
                Ok(())
            }
            Some(sqlparser::ast::Distinct::On(on_exprs)) => {
                // DISTINCT ON keeps the first row per key in the sorted
                // result; the surrounding branches apply ORDER BY before
                // this dedup runs, exactly as PostgreSQL evaluates it. Keys
                // resolve to projected columns by expression or alias.
                let mut key_indices = Vec::with_capacity(on_exprs.len());
                for on_expr in on_exprs {
                    let rendered = on_expr.to_string();
                    let index = select
                        .projection
                        .iter()
                        .position(|item| match item {
                            SelectItem::UnnamedExpr(expr) => expr.to_string() == rendered,
                            SelectItem::ExprWithAlias { expr, alias } => {
                                expr.to_string() == rendered || alias.value == rendered
                            }
                            _ => false,
                        })
                        .ok_or_else(|| {
                            SqlError::Unsupported(format!(
                                "DISTINCT ON expression {rendered} must appear in the select list"
                            ))
                        })?;
                    key_indices.push(index);
                }
                let mut seen = std::collections::BTreeSet::new();
                result.rows.retain(|row| {
                    let key = key_indices
                        .iter()
                        .map(|index| row.get(*index).map(SqlValue::to_cell))
                        .collect::<Vec<_>>();
                    seen.insert(key)
                });
                Ok(())
            }
        }
    }

    /// The schema of the single base table referenced by `from`, or `None` when
    /// `from` is a join, CTE, view, sequence, or virtual table — cases where a
    /// projected column can't be unambiguously typed from one table schema.
    pub(crate) fn single_relation_schema(&self, from: &TableWithJoins) -> Option<Arc<TableSchema>> {
        if !from.joins.is_empty() {
            return None;
        }
        let TableFactor::Table {
            name, args: None, ..
        } = &from.relation
        else {
            return None;
        };
        let collection = relation_name(name).ok()?;
        if self.cte(&collection).is_some() || is_virtual_table(&collection) {
            return None;
        }
        if load_view(self.db_ref(), &collection)
            .ok()
            .flatten()
            .is_some()
            || load_sequence(self.db_ref(), &collection)
                .ok()
                .flatten()
                .is_some()
        {
            return None;
        }
        let collection = resolve_session_relation_name(self.db_ref(), &collection).ok()?;
        load_schema_shared(self.db_ref(), &collection)
            .ok()
            .flatten()
    }

    /// Resolve the logical PostgreSQL type of every output column of a row-path
    /// SELECT (joins, CTE/derived references, multi-relation FROM, expressions),
    /// returning one entry per output column in projection order (wildcards
    /// expanded). `None` for the whole select when the FROM shape can't be
    /// enumerated for typing (e.g. a virtual/catalog table, table function, or a
    /// schemaless collection), so the wire layer falls back to its
    /// value-independent default. This complements the single-base-table record
    /// path (`with_projection_column_types`) by tracing each column to its source
    /// relation's schema/CTE/subquery type.
    pub(crate) fn infer_row_select_column_types(
        &self,
        select: &Select,
    ) -> Option<Vec<Option<String>>> {
        Some(
            self.select_output_columns(select)?
                .into_iter()
                .map(|(_, ty)| ty)
                .collect(),
        )
    }

    /// Combine the existing single-relation typing with the richer row-path
    /// resolver: if a single base-table schema already typed the projection use
    /// that; otherwise trace columns through joins/CTEs/subqueries. Attaches by
    /// index, guarded on a 1:1 length match with the produced columns.
    pub(crate) fn typed_row_result(
        &self,
        result: SqlResult,
        select: &Select,
        schema: Option<&TableSchema>,
    ) -> Result<SqlResult> {
        let mut result = with_projection_column_types(result, &select.projection, schema);
        // The inferred column types and metadata depend only on the SELECT
        // node and the catalogs (no CTE, no routine variable in the
        // projection): for routine-IR-owned statements they are computed once
        // per catalog generation.
        let typing_key = if self.ir_owned_statement
            && self.ctes.is_empty()
            && !self.projection_references_routine_vars(select)
        {
            self.routine_ir.map(|ir| {
                let generation = crate::eval::expr_type_scope()
                    .map(|(generation, _)| generation)
                    .unwrap_or_else(|| self.db_ref().collection_generation(ROUTINE_COLLECTION));
                (generation, ir, select as *const Select as usize)
            })
        } else {
            None
        };
        let remembered = typing_key
            .and_then(|key| RESULT_TYPING_MEMO.with(|memo| memo.borrow().get(&key).cloned()));
        #[cfg(test)]
        if remembered.is_some() {
            SQL_RESULT_TYPING_MEMO_HITS.with(|hits| *hits.borrow_mut() += 1);
        }
        let (inferred_types, metadata) = match remembered {
            Some(remembered) => (remembered.0.clone(), remembered.1.clone()),
            None => {
                let computed = (
                    self.infer_row_select_column_types(select),
                    self.select_column_metadata(select),
                );
                if let Some(key) = typing_key {
                    RESULT_TYPING_MEMO.with(|memo| {
                        let mut memo = memo.borrow_mut();
                        if memo.len() >= RESULT_TYPING_MEMO_MAX {
                            memo.clear();
                        }
                        memo.insert(key, std::rc::Rc::new(computed.clone()));
                    });
                }
                computed
            }
        };
        if let Some(types) = inferred_types {
            if types.len() == result.columns.len() {
                let mut merged = result.column_types.clone();
                merged.resize(result.columns.len(), None);
                for (slot, inferred) in merged.iter_mut().zip(types) {
                    if inferred.is_some() {
                        *slot = inferred;
                    }
                }
                result = result.with_column_types(merged);
            }
        }
        if let Some(metadata) = metadata {
            if metadata.len() == result.columns.len() {
                result = result.with_column_metadata(metadata);
            }
        }
        let mut result = validate_integer_result_types(result)?;
        for (column_index, pg_type) in result.column_types.iter().enumerate() {
            if pg_type.as_deref() != Some("money") {
                continue;
            }
            for row in &mut result.rows {
                let Some(value) = row.get_mut(column_index) else {
                    continue;
                };
                if matches!(value, SqlValue::Null) {
                    continue;
                }
                let cents =
                    pg_money_cents_from_text(&value.to_cell()).map_err(|error| match error {
                        PgCanonicalValueError::NumericOverflow => SqlError::money_out_of_range(),
                        _ => SqlError::InvalidTextRepresentation(format!(
                            "invalid input syntax for type money: \"{}\"",
                            value.to_cell()
                        )),
                    })?;
                *value = SqlValue::String(pg_money_text_from_cents(cents));
            }
        }
        Ok(result)
    }

    /// Whether any projected expression names a routine variable (whose
    /// runtime value could steer inference).
    fn projection_references_routine_vars(&self, select: &Select) -> bool {
        if self.routine_vars.is_empty() {
            return false;
        }
        use sqlparser::ast::visit_expressions;
        use std::ops::ControlFlow;
        select.projection.iter().any(|item| {
            let expr = match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
                _ => return false,
            };
            visit_expressions(expr, |e| match e {
                Expr::Identifier(ident)
                    if routine_var_from_ident(&self.routine_vars, ident).is_some() =>
                {
                    ControlFlow::Break(())
                }
                Expr::Value(value)
                    if routine_var_from_value(&self.routine_vars, value).is_some() =>
                {
                    ControlFlow::Break(())
                }
                Expr::CompoundIdentifier(idents)
                    if routine_var_from_parts(
                        &self.routine_vars,
                        &idents.iter().map(|i| i.value.clone()).collect::<Vec<_>>(),
                    )
                    .ok()
                    .flatten()
                    .is_some() =>
                {
                    ControlFlow::Break(())
                }
                _ => ControlFlow::Continue(()),
            })
            .is_break()
        })
    }

    pub(crate) fn select_column_metadata(&self, select: &Select) -> Option<Vec<SqlColumnMetadata>> {
        let mut relations = Vec::new();
        for table in &select.from {
            self.collect_relation_column_metadata(&table.relation, &mut relations)?;
            for join in &table.joins {
                self.collect_relation_column_metadata(&join.relation, &mut relations)?;
            }
        }

        let direct_column = |expr: &Expr| -> Option<SqlColumnMetadata> {
            let (qualifier, column) = match expr {
                Expr::Identifier(identifier) => (None, identifier.value.to_ascii_lowercase()),
                Expr::CompoundIdentifier(parts) => (
                    parts
                        .get(parts.len().checked_sub(2)?)
                        .map(|part| part.value.to_ascii_lowercase()),
                    parts.last()?.value.to_ascii_lowercase(),
                ),
                Expr::Nested(inner) => match inner.as_ref() {
                    Expr::Identifier(identifier) => (None, identifier.value.to_ascii_lowercase()),
                    Expr::CompoundIdentifier(parts) => (
                        parts
                            .get(parts.len().checked_sub(2)?)
                            .map(|part| part.value.to_ascii_lowercase()),
                        parts.last()?.value.to_ascii_lowercase(),
                    ),
                    _ => return None,
                },
                _ => return None,
            };
            if let Some(qualifier) = qualifier {
                return relations
                    .iter()
                    .find(|relation: &&RelationColumnMetadata| relation.alias == qualifier)?
                    .columns
                    .iter()
                    .find(|(name, _)| name == &column)
                    .map(|(_, metadata)| metadata.clone());
            }
            let mut found = None;
            for relation in &relations {
                for (name, metadata) in &relation.columns {
                    if name == &column {
                        if found.is_some() {
                            return None;
                        }
                        found = Some(metadata.clone());
                    }
                }
            }
            found
        };

        let mut metadata = Vec::new();
        for item in &select.projection {
            match item {
                SelectItem::Wildcard(_) => {
                    for relation in &relations {
                        metadata.extend(
                            relation
                                .columns
                                .iter()
                                .map(|(_, metadata)| metadata.clone()),
                        );
                    }
                }
                SelectItem::QualifiedWildcard(qualifier, _) => {
                    let SelectItemQualifiedWildcardKind::ObjectName(qualifier) = qualifier else {
                        return None;
                    };
                    let alias =
                        unqualified_relation(&object_name(qualifier).ok()?).to_ascii_lowercase();
                    let relation = relations.iter().find(|relation| relation.alias == alias)?;
                    metadata.extend(
                        relation
                            .columns
                            .iter()
                            .map(|(_, metadata)| metadata.clone()),
                    );
                }
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    metadata.push(direct_column(expr).unwrap_or_default());
                }
                _ => return None,
            }
        }
        Some(metadata)
    }

    pub(crate) fn collect_relation_column_metadata(
        &self,
        factor: &TableFactor,
        relations: &mut Vec<RelationColumnMetadata>,
    ) -> Option<()> {
        match factor {
            TableFactor::NestedJoin {
                table_with_joins,
                alias: None,
            } => {
                self.collect_relation_column_metadata(&table_with_joins.relation, relations)?;
                for join in &table_with_joins.joins {
                    self.collect_relation_column_metadata(&join.relation, relations)?;
                }
                Some(())
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let alias_name = alias
                    .as_ref()
                    .map(|alias| alias.name.value.clone())
                    .unwrap_or_else(|| "subquery".to_string());
                let columns = self.query_output_columns(subquery)?;
                let alias_columns = alias
                    .as_ref()
                    .map(|alias| alias.columns.as_slice())
                    .unwrap_or(&[]);
                let columns = rename_relation_columns(columns, alias_columns)?
                    .into_iter()
                    .map(|(name, _)| (name, SqlColumnMetadata::default()))
                    .collect();
                relations.push(RelationColumnMetadata {
                    alias: alias_name.to_ascii_lowercase(),
                    columns,
                });
                Some(())
            }
            TableFactor::Table {
                name,
                alias,
                args: None,
                ..
            } => {
                let table = relation_name(name).ok()?;
                let short = table.rsplit('.').next().unwrap_or(&table).to_string();
                let alias_name = alias
                    .as_ref()
                    .map(|alias| alias.name.value.clone())
                    .unwrap_or_else(|| short.clone());
                let alias_columns = alias
                    .as_ref()
                    .map(|alias| alias.columns.as_slice())
                    .unwrap_or(&[]);
                let (typed_columns, is_base_table) =
                    self.relation_table_columns(&table, alias_columns)?;
                let mut columns = if is_base_table {
                    let resolved = resolve_session_relation_name(self.db_ref(), &table).ok()?;
                    let schema = load_schema_shared(self.db_ref(), &resolved)
                        .ok()
                        .flatten()?;
                    let table_oid = table_oids(self.db_ref()).get(&resolved).copied()? as i32;
                    typed_columns
                        .into_iter()
                        .map(|(name, _)| {
                            let column = schema.column(&name)?;
                            let attribute_number = schema
                                .columns
                                .iter()
                                .position(|candidate| {
                                    candidate.name.eq_ignore_ascii_case(&column.name)
                                })
                                .and_then(|index| i16::try_from(index + 1).ok())?;
                            Some((
                                name,
                                SqlColumnMetadata {
                                    table_oid,
                                    attribute_number,
                                    type_modifier: column.catalog_typmod() as i32,
                                },
                            ))
                        })
                        .collect::<Option<Vec<_>>>()?
                } else {
                    typed_columns
                        .into_iter()
                        .map(|(name, _)| (name, SqlColumnMetadata::default()))
                        .collect()
                };
                if is_base_table && !alias_name.eq_ignore_ascii_case(&short) {
                    columns.extend(columns.clone());
                }
                relations.push(RelationColumnMetadata {
                    alias: alias_name.to_ascii_lowercase(),
                    columns,
                });
                Some(())
            }
            _ => None,
        }
    }

    pub(crate) fn odoo_auto_install_dependency_query(
        &self,
        select: &Select,
        query: &Query,
        from: &TableWithJoins,
    ) -> Result<Option<SqlResult>> {
        if !odoo_auto_install_dependency_query_matches(select, from)? {
            return Ok(None);
        }

        let module_table = resolve_session_relation_name(self.db_ref(), "ir_module_module")?;
        let dependency_table =
            resolve_session_relation_name(self.db_ref(), "ir_module_module_dependency")?;
        let Some(module_schema) = load_schema(self.db_ref(), &module_table)? else {
            return Ok(None);
        };
        let Some(dependency_schema) = load_schema(self.db_ref(), &dependency_table)? else {
            return Ok(None);
        };

        #[derive(Clone)]
        struct ModuleRow {
            id: i64,
            name: String,
            auto_install: bool,
            state: String,
        }

        struct DependencyRow {
            module_id: i64,
            name: String,
            auto_install_required: bool,
        }

        let mut modules = Vec::new();
        let mut modules_by_name = FxHashMap::default();
        for (idx, record) in self.scan_records(&module_table)?.into_iter().enumerate() {
            if idx % 1024 == 0 {
                self.check_cancellation()?;
            }
            let Some(id) = sql_value_i64(&record_column_value(&record, &module_schema, "id"))
            else {
                continue;
            };
            let Some(name) = sql_value_text(&record_column_value(&record, &module_schema, "name"))
            else {
                continue;
            };
            let auto_install = sql_value_bool(&record_column_value(
                &record,
                &module_schema,
                "auto_install",
            ))
            .unwrap_or(false);
            let state = sql_value_text(&record_column_value(&record, &module_schema, "state"))
                .unwrap_or_default();
            let module = ModuleRow {
                id,
                name: name.clone(),
                auto_install,
                state,
            };
            modules_by_name.insert(name, module.clone());
            modules.push(module);
        }

        let mut dependencies_by_module = FxHashMap::<i64, Vec<DependencyRow>>::default();
        for (idx, record) in self
            .scan_records(&dependency_table)?
            .into_iter()
            .enumerate()
        {
            if idx % 1024 == 0 {
                self.check_cancellation()?;
            }
            let Some(module_id) = sql_value_i64(&record_column_value(
                &record,
                &dependency_schema,
                "module_id",
            )) else {
                continue;
            };
            let Some(name) =
                sql_value_text(&record_column_value(&record, &dependency_schema, "name"))
            else {
                continue;
            };
            let auto_install_required = sql_value_bool(&record_column_value(
                &record,
                &dependency_schema,
                "auto_install_required",
            ))
            .unwrap_or(false);
            dependencies_by_module
                .entry(module_id)
                .or_default()
                .push(DependencyRow {
                    module_id,
                    name,
                    auto_install_required,
                });
        }

        let mut rows = Vec::new();
        for (idx, module) in modules.iter().enumerate() {
            if idx % 1024 == 0 {
                self.check_cancellation()?;
            }
            if !module.auto_install
                || module.state.eq_ignore_ascii_case("to install")
                || module.state.eq_ignore_ascii_case("uninstallable")
            {
                continue;
            }
            let has_blocking_dependency = dependencies_by_module
                .get(&module.id)
                .into_iter()
                .flatten()
                .any(|dependency| {
                    let _ = dependency.module_id;
                    match modules_by_name.get(&dependency.name) {
                        None => true,
                        Some(dep_module) => {
                            dependency.auto_install_required
                                && !dep_module.state.eq_ignore_ascii_case("to install")
                        }
                    }
                });
            if !has_blocking_dependency {
                rows.push(vec![SqlValue::String(module.name.clone())]);
            }
        }

        if query.order_by.is_some() {
            rows.sort_by(|left, right| {
                let left = left.first().and_then(sql_value_text).unwrap_or_default();
                let right = right.first().and_then(sql_value_text).unwrap_or_default();
                left.cmp(&right)
            });
        }
        apply_row_limit(&mut rows, query)?;
        Ok(Some(SqlResult::new(vec!["name".to_string()], rows)))
    }

    /// Output columns (name + logical type) of a SELECT, used both for the
    /// top-level attach and recursively for derived tables / subqueries. Names are
    /// lowercased; they are not load-bearing for the top-level attach (which uses
    /// position), but matter when this SELECT is a derived table whose columns are
    /// looked up by name.
    pub(crate) fn select_output_columns(
        &self,
        select: &Select,
    ) -> Option<Vec<(String, Option<String>)>> {
        if select.from.is_empty() {
            let env: Vec<RelationColumns> = Vec::new();
            return select
                .projection
                .iter()
                .map(|item| match item {
                    SelectItem::UnnamedExpr(expr) => Some((
                        select_expr_column_name(expr).to_ascii_lowercase(),
                        self.infer_env_expr_type(expr, &env),
                    )),
                    SelectItem::ExprWithAlias { expr, alias } => Some((
                        alias.value.to_ascii_lowercase(),
                        self.infer_env_expr_type(expr, &env),
                    )),
                    _ => None,
                })
                .collect();
        }
        let env = self.from_relation_columns(&select.from)?;
        let mut out: Vec<(String, Option<String>)> = Vec::new();
        for item in &select.projection {
            match item {
                SelectItem::Wildcard(_) => {
                    for relation in &env {
                        for (name, ty) in &relation.columns {
                            out.push((name.clone(), ty.clone()));
                        }
                    }
                }
                SelectItem::QualifiedWildcard(qualifier, _) => {
                    let relation = self.qualified_wildcard_relation(qualifier, &env)?;
                    for (name, ty) in &relation.columns {
                        out.push((name.clone(), ty.clone()));
                    }
                }
                SelectItem::UnnamedExpr(expr) => out.push((
                    row_expr_column_name(expr).to_ascii_lowercase(),
                    self.infer_env_expr_type(expr, &env),
                )),
                SelectItem::ExprWithAlias { expr, alias } => out.push((
                    alias.value.to_ascii_lowercase(),
                    self.infer_env_expr_type(expr, &env),
                )),
                _ => return None,
            }
        }
        Some(out)
    }

    /// Find the FROM relation a `qualifier.*` wildcard refers to (by binding
    /// alias). `None` (bail) when it can't be matched to exactly one relation.
    pub(crate) fn qualified_wildcard_relation<'e>(
        &self,
        qualifier: &SelectItemQualifiedWildcardKind,
        env: &'e [RelationColumns],
    ) -> Option<&'e RelationColumns> {
        let SelectItemQualifiedWildcardKind::ObjectName(qualifier) = qualifier else {
            return None;
        };
        let object = object_name(qualifier).ok()?;
        let target = unqualified_relation(&object).to_ascii_lowercase();
        env.iter().find(|relation| relation.alias == target)
    }

    /// Output columns (name + type) of a Query body, resolving set operations to
    /// the first (left) branch per the deterministic UNION rule.
    pub(crate) fn query_output_columns(
        &self,
        query: &Query,
    ) -> Option<Vec<(String, Option<String>)>> {
        if query.with.is_some() {
            // A subquery that introduces its own CTEs is not materialized at type
            // resolution time; bail to the value-independent fallback rather than
            // risk a wrong type. (Rare in practice.)
            return None;
        }
        self.set_expr_output_columns(query.body.as_ref())
    }

    pub(crate) fn set_expr_output_columns(
        &self,
        expr: &SetExpr,
    ) -> Option<Vec<(String, Option<String>)>> {
        match expr {
            SetExpr::Select(select) => self.select_output_columns(select),
            SetExpr::Query(query) => self.query_output_columns(query),
            SetExpr::SetOperation {
                left, right, op, ..
            } => {
                let left = self.set_expr_output_columns(left)?;
                let right = self.set_expr_output_columns(right)?;
                if left.len() != right.len() {
                    return None;
                }
                Some(
                    left.into_iter()
                        .zip(right)
                        .map(|((name, left_type), (_, right_type))| {
                            let pg_type = select_common_pg_type(
                                self.db_ref(),
                                &[left_type, right_type],
                                &op.to_string(),
                            )
                            .ok();
                            (name, pg_type)
                        })
                        .collect(),
                )
            }
            SetExpr::Values(values) => {
                let width = values.rows.first()?.len();
                Some(
                    (0..width)
                        .map(|index| {
                            let types = values
                                .rows
                                .iter()
                                .map(|row| {
                                    row.get(index)
                                        .and_then(|expr| self.infer_env_expr_type(expr, &[]))
                                })
                                .collect::<Vec<_>>();
                            (
                                format!("column{}", index + 1),
                                select_common_pg_type(self.db_ref(), &types, "VALUES").ok(),
                            )
                        })
                        .collect(),
                )
            }
            _ => None,
        }
    }

    /// Build the ordered, typed column environment for a FROM clause: one
    /// [`RelationColumns`] per base relation (left-to-right, joins flattened),
    /// each carrying that relation's visible columns in wildcard order. Returns
    /// `None` if any relation can't be typed (so wildcard expansion never
    /// misaligns).
    pub(crate) fn from_relation_columns(
        &self,
        from: &[TableWithJoins],
    ) -> Option<Vec<RelationColumns>> {
        let mut env = Vec::new();
        for table in from {
            self.collect_relation_columns(&table.relation, &mut env)?;
            for join in &table.joins {
                self.collect_relation_columns(&join.relation, &mut env)?;
            }
        }
        Some(env)
    }

    pub(crate) fn collect_relation_columns(
        &self,
        factor: &TableFactor,
        env: &mut Vec<RelationColumns>,
    ) -> Option<()> {
        if let Some(call) = json_set_returning_call(factor).ok().flatten() {
            let (alias, columns) = json_set_function_columns(&call).ok()?;
            let output_types = json_set_function_output_pg_types(&call).ok()?;
            let columns = columns
                .into_iter()
                .zip(output_types)
                .map(|(column, pg_type)| (column.to_ascii_lowercase(), Some(pg_type)))
                .collect::<Vec<_>>();
            env.push(RelationColumns {
                alias: alias.to_ascii_lowercase(),
                row_type: None,
                columns,
            });
            return Some(());
        }
        match factor {
            TableFactor::UNNEST {
                alias,
                array_exprs,
                with_offset,
                with_offset_alias,
                with_ordinality,
            } => {
                let alias_name = alias
                    .as_ref()
                    .map(|alias| alias.name.value.clone())
                    .unwrap_or_else(|| "unnest".to_string());
                let source_columns = unnest_source_columns(
                    array_exprs.len(),
                    *with_offset,
                    with_offset_alias.as_ref(),
                    *with_ordinality,
                );
                let mut columns = array_exprs
                    .iter()
                    .enumerate()
                    .map(|(index, expr)| {
                        (
                            source_columns[index].to_ascii_lowercase(),
                            self.infer_env_expr_type(expr, &[]).and_then(|pg_type| {
                                pg_type.strip_suffix("[]").map(str::to_string).or_else(|| {
                                    match pg_type.as_str() {
                                        "int2vector" => Some("int2".to_string()),
                                        "oidvector" => Some("oid".to_string()),
                                        _ => None,
                                    }
                                })
                            }),
                        )
                    })
                    .collect::<Vec<_>>();
                columns.extend(
                    source_columns
                        .iter()
                        .skip(columns.len())
                        .map(|name| (name.to_ascii_lowercase(), Some("int8".to_string()))),
                );
                let alias_columns = alias
                    .as_ref()
                    .map(|alias| alias.columns.as_slice())
                    .unwrap_or(&[]);
                env.push(RelationColumns {
                    alias: alias_name.to_ascii_lowercase(),
                    row_type: None,
                    columns: rename_relation_columns(columns, alias_columns)?,
                });
                Some(())
            }
            TableFactor::Table {
                name,
                alias,
                args: Some(args),
                with_ordinality,
                ..
            } if relation_name(name)
                .ok()
                .is_some_and(|name| name.eq_ignore_ascii_case("unnest")) =>
            {
                let array_exprs = table_function_expr_args(args).ok()?;
                let alias_name = alias
                    .as_ref()
                    .map(|alias| alias.name.value.clone())
                    .unwrap_or_else(|| "unnest".to_string());
                let source_columns =
                    unnest_source_columns(array_exprs.len(), false, None, *with_ordinality);
                let mut columns = array_exprs
                    .iter()
                    .enumerate()
                    .map(|(index, expr)| {
                        (
                            source_columns[index].to_ascii_lowercase(),
                            self.infer_env_expr_type(expr, &[]).and_then(|pg_type| {
                                pg_type.strip_suffix("[]").map(str::to_string).or_else(|| {
                                    match pg_type.as_str() {
                                        "int2vector" => Some("int2".to_string()),
                                        "oidvector" => Some("oid".to_string()),
                                        _ => None,
                                    }
                                })
                            }),
                        )
                    })
                    .collect::<Vec<_>>();
                columns.extend(
                    source_columns
                        .iter()
                        .skip(columns.len())
                        .map(|name| (name.to_ascii_lowercase(), Some("int8".to_string()))),
                );
                let alias_columns = alias
                    .as_ref()
                    .map(|alias| alias.columns.as_slice())
                    .unwrap_or(&[]);
                env.push(RelationColumns {
                    alias: alias_name.to_ascii_lowercase(),
                    row_type: None,
                    columns: rename_relation_columns(columns, alias_columns)?,
                });
                Some(())
            }
            TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => {
                if alias.is_some() {
                    return None;
                }
                let nested = self.from_relation_columns(std::slice::from_ref(table_with_joins))?;
                env.extend(nested);
                Some(())
            }
            TableFactor::Derived {
                lateral,
                subquery,
                alias,
                ..
            } => {
                let alias_name = alias
                    .as_ref()
                    .map(|alias| alias.name.value.clone())
                    .unwrap_or_else(|| "subquery".to_string());
                let columns = if *lateral {
                    // A lateral subquery may reference the outer row and define
                    // local CTEs. Its body still supplies stable output names,
                    // which lets the enclosing projection infer types from
                    // expressions such as COALESCE(catalog.value, '{}'::jsonb).
                    self.set_expr_output_columns(subquery.body.as_ref())?
                } else {
                    self.query_output_columns(subquery)?
                };
                let alias_cols = alias
                    .as_ref()
                    .map(|alias| alias.columns.as_slice())
                    .unwrap_or(&[]);
                let columns = rename_relation_columns(columns, alias_cols)?;
                env.push(RelationColumns {
                    alias: alias_name.to_ascii_lowercase(),
                    row_type: None,
                    columns,
                });
                Some(())
            }
            TableFactor::Table {
                name,
                alias,
                args: None,
                ..
            } => {
                let table = relation_name(name).ok()?;
                let short = table.rsplit('.').next().unwrap_or(&table).to_string();
                let alias_name = alias
                    .as_ref()
                    .map(|alias| alias.name.value.clone())
                    .unwrap_or_else(|| short.clone());
                let alias_cols = alias
                    .as_ref()
                    .map(|alias| alias.columns.as_slice())
                    .unwrap_or(&[]);
                let (mut columns, is_base_table) =
                    self.relation_table_columns(&table, alias_cols)?;
                // A base table whose binding alias differs from its name is
                // materialized by the executor with BOTH `alias.col` and
                // `table.col` entries (see `row_output_columns`), so a wildcard
                // over it yields each column twice. Mirror that here so the typed
                // column count matches the produced columns exactly.
                if is_base_table && !alias_name.eq_ignore_ascii_case(&short) {
                    let duplicated = columns.clone();
                    columns.extend(duplicated);
                }
                env.push(RelationColumns {
                    alias: alias_name.to_ascii_lowercase(),
                    row_type: is_base_table.then(|| short.to_ascii_lowercase()),
                    columns,
                });
                Some(())
            }
            _ => None,
        }
    }

    /// Typed visible columns (unqualified, lowercased names) of a named relation,
    /// resolving CTEs, views and base tables in the same precedence the executor
    /// uses. `None` for virtual/catalog tables, sequences and schemaless
    /// collections — those fall back to the value-independent default.
    ///
    /// The returned bool is `true` for a base table (whose wildcard materialization
    /// duplicates columns when aliased — see [`Self::collect_relation_columns`]).
    pub(crate) fn relation_table_columns(
        &self,
        table: &str,
        alias_cols: &[TableAliasColumnDef],
    ) -> Option<(Vec<(String, Option<String>)>, bool)> {
        if let Some(cte) = self.cte(table) {
            let columns = cte
                .columns
                .iter()
                .enumerate()
                .map(|(idx, name)| (name.to_ascii_lowercase(), cte.column_type(idx)))
                .collect();
            return Some((rename_relation_columns(columns, alias_cols)?, false));
        }
        if let Some(view) = load_view(self.db_ref(), table).ok().flatten() {
            let columns = view
                .columns
                .iter()
                .filter(|column| !column.hidden)
                .map(|column| {
                    (
                        column.name.to_ascii_lowercase(),
                        Some(column.pg_type.clone()),
                    )
                })
                .collect();
            return Some((rename_relation_columns(columns, alias_cols)?, false));
        }
        if is_virtual_table(table) {
            let columns = virtual_table_column_types(table)?;
            return Some((rename_relation_columns(columns, alias_cols)?, false));
        }
        if load_sequence(self.db_ref(), table).ok().flatten().is_some() {
            return None;
        }
        let table = resolve_session_relation_name(self.db_ref(), table).ok()?;
        let schema = load_schema_shared(self.db_ref(), &table).ok().flatten()?;
        // The base-table scan path keys columns off the schema field names and
        // ignores any relation column-alias list, so we do too.
        let columns = schema
            .columns
            .iter()
            .filter(|column| !column.hidden)
            .map(|column| {
                (
                    column.name.to_ascii_lowercase(),
                    Some(column.pg_type.clone()),
                )
            })
            .collect();
        Some((columns, true))
    }

    /// Infer the logical PostgreSQL type of a projected expression against a
    /// row-path column environment (handles column references, casts, common
    /// operators, CASE/COALESCE/NULLIF, string/comparison functions and
    /// aggregates). `None` when the type can't be determined — the wire layer then
    /// applies its value-independent default.
    /// Whether the bound evaluator gives exactly the typed evaluator's answer
    /// for `expr` over rows of `env`: a tree of AND/OR/NOT over comparisons
    /// whose operands are plain column references, routine variables or
    /// literals of a type where `pg_typed_compare` is the plain value
    /// comparison (integers, floats, booleans; text only for equality), plus
    /// IS [NOT] NULL on such operands. Arithmetic (integer range enforcement),
    /// casts, functions, enums, character types, temporal and network types
    /// all stay on the typed evaluator.
    pub(crate) fn bound_row_filter_is_exact(&self, expr: &Expr, env: &[RelationColumns]) -> bool {
        fn plain_operand(expr: &Expr) -> bool {
            match expr {
                Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Value(_) => true,
                Expr::Nested(inner) => plain_operand(inner),
                _ => false,
            }
        }
        // Arithmetic over routine variables and literals only (`next_o - 20`):
        // no column reference, so the typed evaluator infers no type for it
        // and enforces no integer range on its value; both evaluators then
        // compare the same value against the column.
        fn var_arithmetic(engine: &SqlEngine, expr: &Expr) -> bool {
            match expr {
                Expr::Value(_) => true,
                Expr::Identifier(ident) => {
                    routine_var_from_ident(&engine.routine_vars, ident).is_some()
                }
                Expr::CompoundIdentifier(idents) => {
                    let parts = idents
                        .iter()
                        .map(|ident| ident.value.clone())
                        .collect::<Vec<_>>();
                    routine_var_from_parts(&engine.routine_vars, &parts)
                        .ok()
                        .flatten()
                        .is_some()
                }
                Expr::Nested(inner)
                | Expr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: inner,
                } => var_arithmetic(engine, inner),
                Expr::BinaryOp { left, op, right }
                    if matches!(
                        op,
                        BinaryOperator::Plus | BinaryOperator::Minus | BinaryOperator::Multiply
                    ) =>
                {
                    var_arithmetic(engine, left) && var_arithmetic(engine, right)
                }
                _ => false,
            }
        }
        let operand_ok = |expr: &Expr| {
            plain_operand(expr)
                || (var_arithmetic(self, expr)
                    && self
                        .infer_env_expr_type(expr, env)
                        .is_none_or(|pg_type| exact_type(&pg_type, false)))
        };
        fn exact_type(pg_type: &str, equality_only: bool) -> bool {
            match pg_type {
                "int2" | "int4" | "int8" | "smallint" | "integer" | "int" | "bigint" | "float4"
                | "float8" | "real" | "double precision" | "bool" | "boolean" => true,
                "text" | "varchar" | "character varying" => equality_only,
                _ => false,
            }
        }
        match expr {
            Expr::Nested(inner) => self.bound_row_filter_is_exact(inner, env),
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: inner,
            } => self.bound_row_filter_is_exact(inner, env),
            Expr::BinaryOp { left, op, right }
                if matches!(op, BinaryOperator::And | BinaryOperator::Or) =>
            {
                self.bound_row_filter_is_exact(left, env)
                    && self.bound_row_filter_is_exact(right, env)
            }
            Expr::BinaryOp { left, op, right }
                if matches!(
                    op,
                    BinaryOperator::Eq
                        | BinaryOperator::NotEq
                        | BinaryOperator::Gt
                        | BinaryOperator::GtEq
                        | BinaryOperator::Lt
                        | BinaryOperator::LtEq
                ) =>
            {
                if !operand_ok(left) || !operand_ok(right) {
                    return false;
                }
                let equality_only = matches!(op, BinaryOperator::Eq | BinaryOperator::NotEq);
                let left_type = self.infer_env_expr_type(left, env);
                let right_type = self.infer_env_expr_type(right, env);
                let (Some(left_type), Some(right_type)) = (left_type, right_type) else {
                    // An operand of unknown type (a routine variable) is fine
                    // only next to an exact-typed column; two unknowns go the
                    // typed way, which decides what an undefined comparison is.
                    return match (
                        self.infer_env_expr_type(left, env),
                        self.infer_env_expr_type(right, env),
                    ) {
                        (Some(t), None) | (None, Some(t)) => exact_type(&t, equality_only),
                        _ => false,
                    };
                };
                exact_type(&left_type, equality_only)
                    && exact_type(&right_type, equality_only)
                    && (left_type == right_type
                        || (!matches!(
                            left_type.as_str(),
                            "text" | "varchar" | "character varying" | "bool" | "boolean"
                        ) && !matches!(
                            right_type.as_str(),
                            "text" | "varchar" | "character varying" | "bool" | "boolean"
                        )))
            }
            Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
                plain_operand(inner)
                    && self
                        .infer_env_expr_type(inner, env)
                        .is_some_and(|pg_type| exact_type(&pg_type, true))
            }
            _ => false,
        }
    }

    /// `expr` with every comparison operand that is arithmetic over routine
    /// variables and literals replaced by its value, evaluated once (it does
    /// not depend on the row) through the same evaluator and integer range
    /// check the typed per-row path applies. `None` when a folded value has
    /// no literal form (the typed path then runs unchanged). Only called on
    /// expressions `bound_row_filter_is_exact` accepted.
    fn fold_variable_operands(
        &self,
        expr: &Expr,
        env: &[RelationColumns],
        context: &BoundRowContext,
    ) -> Result<Option<Expr>> {
        fn literal(value: SqlValue) -> Option<Expr> {
            let value = match value {
                SqlValue::Null => Value::Null,
                SqlValue::Int(v) => Value::Number(v.to_string(), false),
                SqlValue::Float(v) if v.is_finite() => Value::Number(format!("{v:?}"), false),
                SqlValue::Bool(v) => Value::Boolean(v),
                _ => return None,
            };
            Some(Expr::Value(value.into()))
        }
        let mut out = expr.clone();
        let mut ok = true;
        fn walk(
            engine: &SqlEngine,
            expr: &mut Expr,
            env: &[RelationColumns],
            context: &BoundRowContext,
            ok: &mut bool,
        ) -> Result<()> {
            match expr {
                Expr::Nested(inner) | Expr::UnaryOp { expr: inner, .. } => {
                    walk(engine, inner, env, context, ok)
                }
                Expr::BinaryOp { left, op, right }
                    if matches!(op, BinaryOperator::And | BinaryOperator::Or) =>
                {
                    walk(engine, left, env, context, ok)?;
                    walk(engine, right, env, context, ok)
                }
                Expr::BinaryOp { left, right, .. } => {
                    for side in [left, right] {
                        if matches!(
                            side.as_ref(),
                            Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Value(_)
                        ) {
                            continue;
                        }
                        let pg_type = engine.infer_env_expr_type(side, env);
                        let value = enforce_integer_value_type(
                            engine.eval_slot_row_value(&Vec::new(), context, side)?,
                            pg_type.as_deref(),
                        )?;
                        match literal(value) {
                            Some(folded) => **side = folded,
                            None => *ok = false,
                        }
                    }
                    Ok(())
                }
                _ => Ok(()),
            }
        }
        walk(self, &mut out, env, context, &mut ok)?;
        Ok(ok.then_some(out))
    }

    pub(crate) fn infer_env_expr_type(
        &self,
        expr: &Expr,
        env: &[RelationColumns],
    ) -> Option<String> {
        match expr {
            Expr::Value(_) => literal_expr_type(expr),
            Expr::Nested(inner) | Expr::Collate { expr: inner, .. } => {
                self.infer_env_expr_type(inner, env)
            }
            Expr::Cast { data_type, .. } => {
                user_type_column_from_data_type(self.db_ref(), data_type)
                    .ok()
                    .flatten()
                    .map(|user_type| user_type.formatted_name())
                    .or_else(|| pg_type_from_data_type(data_type).ok().map(|(name, _)| name))
            }
            Expr::TypedString(value) => {
                user_type_column_from_data_type(self.db_ref(), &value.data_type)
                    .ok()
                    .flatten()
                    .map(|user_type| user_type.formatted_name())
                    .or_else(|| {
                        pg_type_from_data_type(&value.data_type)
                            .ok()
                            .map(|(name, _)| name)
                    })
            }
            Expr::Interval(_) => Some("interval".to_string()),
            Expr::Array(array) => {
                let types = array
                    .elem
                    .iter()
                    .map(|element| self.infer_env_expr_type(element, env))
                    .collect::<Vec<_>>();
                select_common_pg_type(self.db_ref(), &types, "ARRAY")
                    .ok()
                    .map(|element_type| {
                        if element_type.ends_with("[]") {
                            element_type
                        } else {
                            format!("{element_type}[]")
                        }
                    })
            }
            Expr::Extract { .. } => Some("numeric".to_string()),
            Expr::AtTimeZone { timestamp, .. } => {
                match self.infer_env_expr_type(timestamp, env).as_deref() {
                    Some("timestamptz") => Some("timestamp".to_string()),
                    Some("timestamp") => Some("timestamptz".to_string()),
                    _ => None,
                }
            }
            Expr::Identifier(ident)
                if ident.quote_style.is_none()
                    && matches!(
                        ident.value.to_ascii_lowercase().as_str(),
                        "current_user" | "current_role" | "session_user" | "user"
                    ) =>
            {
                Some("name".to_string())
            }
            Expr::Identifier(ident) => env
                .iter()
                .find(|relation| relation.alias.eq_ignore_ascii_case(&ident.value))
                .and_then(|relation| relation.row_type.clone())
                .or_else(|| env_lookup_unqualified(env, &ident.value)),
            Expr::CompoundIdentifier(parts) => {
                let column = parts.last()?.value.to_ascii_lowercase();
                let qualifier = parts
                    .get(parts.len().checked_sub(2)?)?
                    .value
                    .to_ascii_lowercase();
                env_lookup_qualified(env, &qualifier, &column)
            }
            Expr::Function(function) => self.infer_env_function_type(function, env),
            Expr::BinaryOp { left, op, right } => self.infer_env_binary_type(op, left, right, env),
            Expr::Substring { expr, .. } | Expr::Overlay { expr, .. } => {
                match self.infer_env_expr_type(expr, env).as_deref() {
                    Some("bit" | "varbit") => Some("bit".to_string()),
                    _ => self.infer_env_expr_type(expr, env),
                }
            }
            Expr::UnaryOp { op, expr: inner } => {
                if let Some(pg_type) = geometric_unary_result_pg_type(
                    op,
                    self.infer_env_expr_type(inner, env).as_deref(),
                ) {
                    return Some(pg_type);
                }
                if matches!(op, UnaryOperator::BitwiseNot) {
                    match self.infer_env_expr_type(inner, env).as_deref() {
                        Some("bit" | "varbit") => Some("bit".to_string()),
                        Some("inet" | "cidr") => Some("inet".to_string()),
                        Some(pg_type @ ("macaddr" | "macaddr8")) => Some(pg_type.to_string()),
                        _ => None,
                    }
                } else if matches!(op, UnaryOperator::PGPrefixFactorial)
                    && self.infer_env_expr_type(inner, env).as_deref() == Some("tsquery")
                {
                    Some("tsquery".to_string())
                } else if op.to_string().eq_ignore_ascii_case("NOT") {
                    Some("bool".to_string())
                } else if matches!(op.to_string().as_str(), "-" | "+") {
                    self.infer_env_expr_type(inner, env)
                } else {
                    None
                }
            }
            Expr::Like { .. }
            | Expr::ILike { .. }
            | Expr::SimilarTo { .. }
            | Expr::InList { .. }
            | Expr::InSubquery { .. }
            | Expr::Between { .. }
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::IsTrue(_)
            | Expr::IsNotTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsNotFalse(_)
            | Expr::IsDistinctFrom(_, _)
            | Expr::IsNotDistinctFrom(_, _)
            | Expr::Exists { .. }
            | Expr::AnyOp { .. }
            | Expr::AllOp { .. } => Some("bool".to_string()),
            Expr::Case {
                conditions,
                else_result,
                ..
            } => {
                let mut branches: Vec<&Expr> = conditions.iter().map(|when| &when.result).collect();
                if let Some(else_result) = else_result {
                    branches.push(else_result);
                }
                self.common_branch_type(&branches, env)
            }
            _ => None,
        }
    }

    /// Result type of a function call. Aggregates follow PostgreSQL rules
    /// (`COUNT`->int8, `MIN`/`MAX`->arg type, `SUM`/`AVG` by operand class);
    /// `COALESCE`/`NULLIF`/`GREATEST`/`LEAST` take the common type of their
    /// arguments; common string functions return text.
    pub(crate) fn infer_env_function_type(
        &self,
        function: &Function,
        env: &[RelationColumns],
    ) -> Option<String> {
        let name = object_name(&function.name).ok()?.to_ascii_lowercase();
        let bare_name = name.strip_prefix("pg_catalog.").unwrap_or(&name);
        let args = function_args(function);
        // PostgreSQL parses these unqualified, zero-argument spellings as
        // session-identity constructs. A same-named user routine in the search
        // path must not change either their value or their `name` type.
        if matches!(
            bare_name,
            "current_user" | "current_role" | "session_user" | "user"
        ) && args.is_empty()
        {
            return Some("name".to_string());
        }
        if bare_name == "pg_backend_pid" && args.is_empty() {
            return Some("int4".to_string());
        }
        if bare_name == "current_trusted_tenant" && args.is_empty() {
            return Some("text".to_string());
        }
        if matches!(
            bare_name,
            "pg_try_advisory_lock"
                | "pg_try_advisory_lock_shared"
                | "pg_try_advisory_xact_lock"
                | "pg_try_advisory_xact_lock_shared"
                | "pg_advisory_unlock"
                | "pg_advisory_unlock_shared"
        ) {
            return Some("bool".to_string());
        }
        if matches!(
            bare_name,
            "has_schema_privilege"
                | "has_table_privilege"
                | "has_function_privilege"
                | "has_type_privilege"
                | "has_column_privilege"
                | "has_any_column_privilege"
                | "has_sequence_privilege"
                | "has_database_privilege"
                | "has_language_privilege"
                | "has_tablespace_privilege"
                | "has_parameter_privilege"
        ) {
            return Some("bool".to_string());
        }
        if matches!(
            bare_name,
            "pg_advisory_lock"
                | "pg_advisory_lock_shared"
                | "pg_advisory_xact_lock"
                | "pg_advisory_xact_lock_shared"
                | "pg_advisory_unlock_all"
        ) {
            return Some("void".to_string());
        }
        let arg_types = args
            .iter()
            .map(|arg| self.infer_env_expr_type(arg, env))
            .collect::<Vec<_>>();
        if let Some(pg_type) = geometric_function_pg_type(&name, &arg_types) {
            return Some(pg_type);
        }
        if let Some(pg_type) = network_function_pg_type(&name, &arg_types) {
            return Some(pg_type);
        }
        if let Some(pg_type) = json_function_call_pg_type(function) {
            return Some(pg_type);
        }
        if is_builtin_range_type(bare_name) {
            return Some(bare_name.to_string());
        }
        if is_builtin_multirange_type(bare_name) {
            return Some(bare_name.to_string());
        }
        let load_named_user_type = |pg_type: &str| {
            let (schema_name, type_name) = pg_type
                .rsplit_once('.')
                .map(|(schema, name)| (schema, name))
                .unwrap_or(("public", pg_type));
            load_user_type(self.db_ref(), schema_name, type_name)
                .ok()
                .flatten()
        };
        if let Some(user_type) = load_named_user_type(&name) {
            if matches!(
                user_type.kind,
                UserTypeKind::Range { .. } | UserTypeKind::Multirange { .. }
            ) {
                return Some(user_type.column_type(false).formatted_name());
            }
        }
        if matches!(
            bare_name,
            "isempty" | "lower_inc" | "upper_inc" | "lower_inf" | "upper_inf"
        ) && self
            .infer_env_expr_type(args.first()?, env)
            .is_some_and(|pg_type| {
                is_builtin_range_type(&pg_type)
                    || matches!(
                        pg_type.as_str(),
                        "int4multirange"
                            | "int8multirange"
                            | "nummultirange"
                            | "datemultirange"
                            | "tsmultirange"
                            | "tstzmultirange"
                    )
                    || load_named_user_type(&pg_type).is_some_and(|user_type| {
                        matches!(
                            user_type.kind,
                            UserTypeKind::Range { .. } | UserTypeKind::Multirange { .. }
                        )
                    })
            })
        {
            return Some("bool".to_string());
        }
        if matches!(bare_name, "lower" | "upper") {
            let range_type = self.infer_env_expr_type(args.first()?, env)?;
            return match range_type.as_str() {
                "int4range" => Some("int4".to_string()),
                "int8range" => Some("int8".to_string()),
                "numrange" => Some("numeric".to_string()),
                "daterange" => Some("date".to_string()),
                "tsrange" => Some("timestamp".to_string()),
                "tstzrange" => Some("timestamptz".to_string()),
                _ => load_named_user_type(&range_type).and_then(|user_type| match user_type.kind {
                    UserTypeKind::Range { value, .. } => Some(value.subtype),
                    _ => None,
                }),
            };
        }
        if bare_name == "range_merge" {
            let arg_type = self.infer_env_expr_type(args.first()?, env)?;
            if is_builtin_range_type(&arg_type) {
                return Some(arg_type);
            }
            if let Some(user_type) = load_named_user_type(&arg_type) {
                return match user_type.kind {
                    UserTypeKind::Range { .. } => {
                        Some(user_type.column_type(false).formatted_name())
                    }
                    UserTypeKind::Multirange {
                        range_schema_name,
                        range_name,
                        ..
                    } => Some(if range_schema_name == "public" {
                        range_name
                    } else {
                        format!("{range_schema_name}.{range_name}")
                    }),
                    _ => None,
                };
            }
            return match arg_type.as_str() {
                "int4multirange" => Some("int4range".to_string()),
                "int8multirange" => Some("int8range".to_string()),
                "nummultirange" => Some("numrange".to_string()),
                "datemultirange" => Some("daterange".to_string()),
                "tsmultirange" => Some("tsrange".to_string()),
                "tstzmultirange" => Some("tstzrange".to_string()),
                _ => None,
            };
        }
        match bare_name {
            "pg_current_snapshot" => return Some("pg_snapshot".to_string()),
            "txid_current_snapshot" => return Some("txid_snapshot".to_string()),
            "txid_current" => return Some("int8".to_string()),
            "pg_snapshot_xmin" | "pg_snapshot_xmax" | "pg_snapshot_xip" => {
                return Some("xid8".to_string());
            }
            "txid_snapshot_xmin" | "txid_snapshot_xmax" | "txid_snapshot_xip" => {
                return Some("int8".to_string());
            }
            "pg_visible_in_snapshot" | "txid_visible_in_snapshot" => {
                return Some("bool".to_string());
            }
            "range_agg" | "range_intersect_agg" => {
                let input_type = self.infer_env_expr_type(args.first()?, env)?;
                if bare_name == "range_intersect_agg" && is_builtin_range_type(&input_type) {
                    return Some(input_type);
                }
                return match input_type.as_str() {
                    "int4range" | "int4multirange" => Some("int4multirange".to_string()),
                    "int8range" | "int8multirange" => Some("int8multirange".to_string()),
                    "numrange" | "nummultirange" => Some("nummultirange".to_string()),
                    "daterange" | "datemultirange" => Some("datemultirange".to_string()),
                    "tsrange" | "tsmultirange" => Some("tsmultirange".to_string()),
                    "tstzrange" | "tstzmultirange" => Some("tstzmultirange".to_string()),
                    _ => None,
                };
            }
            "array_agg" => {
                let element_type = self.infer_env_expr_type(args.first()?, env)?;
                return Some(if element_type.ends_with("[]") {
                    element_type
                } else {
                    format!("{element_type}[]")
                });
            }
            "string_agg" => return Some("text".to_string()),
            "xmlagg" => return Some("xml".to_string()),
            "pg_typeof" => return Some("regtype".to_string()),
            "unnest" => {
                let array_type = self.infer_env_expr_type(args.first()?, env)?;
                return array_type.strip_suffix("[]").map(str::to_string).or_else(
                    || match array_type.as_str() {
                        "int2vector" => Some("int2".to_string()),
                        "oidvector" => Some("oid".to_string()),
                        _ => None,
                    },
                );
            }
            "generate_subscripts" => return Some("int4".to_string()),
            "array_append" | "array_remove" | "array_replace" | "array_cat"
            | "bicdb_array_assign" => {
                return self.infer_env_expr_type(args.first()?, env);
            }
            "array_prepend" => return self.infer_env_expr_type(args.get(1)?, env),
            "trim_array" | "array_sample" | "array_shuffle" => {
                return self.infer_env_expr_type(args.first()?, env);
            }
            "array_fill" => {
                let element_type = self.infer_env_expr_type(args.first()?, env)?;
                return Some(if element_type.ends_with("[]") {
                    element_type
                } else {
                    format!("{element_type}[]")
                });
            }
            "string_to_array" => return Some("text[]".to_string()),
            "array_to_string" => return Some("text".to_string()),
            "array_position" | "cardinality" | "array_ndims" | "array_length" | "array_lower"
            | "array_upper" => return Some("int4".to_string()),
            "array_positions" => return Some("int4[]".to_string()),
            "array_dims" => return Some("text".to_string()),
            _ => {}
        }
        match name.as_str() {
            "uuidv4"
            | "pg_catalog.uuidv4"
            | "gen_random_uuid"
            | "pg_catalog.gen_random_uuid"
            | "uuid_generate_v4"
            | "public.uuid_generate_v4"
            | "uuidv7"
            | "pg_catalog.uuidv7" => Some("uuid".to_string()),
            "uuid_extract_version" | "pg_catalog.uuid_extract_version" => Some("int2".to_string()),
            "uuid_extract_timestamp" | "pg_catalog.uuid_extract_timestamp" => {
                Some("timestamptz".to_string())
            }
            "date_part" | "pg_catalog.date_part" => Some("float8".to_string()),
            "date_trunc" | "pg_catalog.date_trunc" => self.infer_env_expr_type(args.get(1)?, env),
            "justify_hours"
            | "pg_catalog.justify_hours"
            | "justify_days"
            | "pg_catalog.justify_days"
            | "justify_interval"
            | "pg_catalog.justify_interval" => Some("interval".to_string()),
            "count" => Some("int8".to_string()),
            "row_number" | "rank" | "dense_rank" | "ntile" => Some("int8".to_string()),
            "percent_rank" | "cume_dist" => Some("float8".to_string()),
            "min" | "max" => self.infer_env_expr_type(args.first()?, env),
            "bool_and"
            | "pg_catalog.bool_and"
            | "bool_or"
            | "pg_catalog.bool_or"
            | "every"
            | "pg_catalog.every"
            | "pg_has_role"
            | "pg_catalog.pg_has_role" => Some("bool".to_string()),
            "sum" => aggregate_pg_type_for_sum(self.infer_env_expr_type(args.first()?, env)?),
            "avg" => aggregate_pg_type_for_avg(self.infer_env_expr_type(args.first()?, env)?),
            "lag" | "lead" | "first_value" | "last_value" | "nth_value" => {
                self.infer_env_expr_type(args.first()?, env)
            }
            "coalesce" | "nullif" | "greatest" | "least" | "ifnull" => {
                let branches: Vec<&Expr> = args.iter().collect();
                self.common_branch_type(&branches, env)
            }
            "lower"
            | "upper"
            | "trim"
            | "ltrim"
            | "rtrim"
            | "concat"
            | "concat_ws"
            | "replace"
            | "split_part"
            | "initcap"
            | "md5"
            | "to_char"
            | "format"
            | "left"
            | "right"
            | "repeat"
            | "reverse"
            | "bicdb_variadic_call" => Some("text".to_string()),
            "substr" | "substring" => match self.infer_env_expr_type(args.first()?, env).as_deref()
            {
                Some("bit" | "varbit") => Some("bit".to_string()),
                Some("bytea") => Some("bytea".to_string()),
                _ => Some("text".to_string()),
            },
            "length" | "char_length" | "character_length" | "octet_length" | "bit_length"
            | "position" | "strpos" | "ascii" => Some("int4".to_string()),
            "bit_count" | "crc32" | "crc32c" => Some("int8".to_string()),
            "decode" | "set_byte" | "sha224" | "sha256" | "sha384" | "sha512" => {
                Some("bytea".to_string())
            }
            "set_bit" => match self.infer_env_expr_type(args.first()?, env).as_deref() {
                Some("bit" | "varbit") => Some("bit".to_string()),
                _ => Some("bytea".to_string()),
            },
            "json_typeof"
            | "pg_catalog.json_typeof"
            | "jsonb_typeof"
            | "pg_catalog.jsonb_typeof" => Some("text".to_string()),
            "pg_get_expr"
            | "pg_catalog.pg_get_expr"
            | "pg_get_indexdef"
            | "pg_catalog.pg_get_indexdef" => Some("text".to_string()),
            "abs" | "ceil" | "ceiling" | "floor" | "round" | "mod" | "power" | "sqrt" => {
                self.infer_env_expr_type(args.first()?, env)
            }
            _ => load_routine(self.db_ref(), RoutineKind::Function, &name)
                .ok()
                .flatten()
                .map(|routine| routine.return_type),
        }
    }

    /// Result type of a binary operator: comparisons / logical operators yield
    /// bool; `||` yields text; arithmetic combines operand types per PostgreSQL's
    /// numeric promotion rules (by operand type, never by value).
    pub(crate) fn infer_env_binary_type(
        &self,
        op: &BinaryOperator,
        left: &Expr,
        right: &Expr,
        env: &[RelationColumns],
    ) -> Option<String> {
        let left_type = self.infer_env_expr_type(left, env);
        let right_type = self.infer_env_expr_type(right, env);
        if let Some(pg_type) =
            geometric_binary_result_pg_type(op, left_type.as_deref(), right_type.as_deref())
        {
            return Some(pg_type);
        }
        if let Some(pg_type) =
            network_binary_result_pg_type(op, left_type.as_deref(), right_type.as_deref())
        {
            return Some(pg_type);
        }
        if let Some(pg_type) =
            pg_lsn_binary_result_pg_type(op, left_type.as_deref(), right_type.as_deref())
        {
            return Some(pg_type.to_string());
        }
        if let Some(pg_type) =
            range_binary_result_pg_type(op, left_type.as_deref(), right_type.as_deref())
        {
            return Some(pg_type);
        }
        match op {
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::And
            | BinaryOperator::Or
            | BinaryOperator::AtArrow
            | BinaryOperator::ArrowAt
            | BinaryOperator::AtAt
            | BinaryOperator::AtQuestion
            | BinaryOperator::Question
            | BinaryOperator::QuestionAnd
            | BinaryOperator::QuestionPipe => Some("bool".to_string()),
            BinaryOperator::PGOverlap => {
                let left = self.infer_env_expr_type(left, env);
                let right = self.infer_env_expr_type(right, env);
                if left.as_deref() == Some("tsquery") && right.as_deref() == Some("tsquery") {
                    Some("tsquery".to_string())
                } else {
                    Some("bool".to_string())
                }
            }
            BinaryOperator::Arrow | BinaryOperator::HashArrow | BinaryOperator::HashMinus => self
                .infer_env_expr_type(left, env)
                .filter(|pg_type| matches!(pg_type.as_str(), "json" | "jsonb")),
            BinaryOperator::LongArrow | BinaryOperator::HashLongArrow => Some("text".to_string()),
            BinaryOperator::StringConcat => match (left_type, right_type) {
                (Some(left), _) if left.ends_with("[]") => Some(left),
                (_, Some(right)) if right.ends_with("[]") => Some(right),
                (Some(left), _)
                    if matches!(
                        left.as_str(),
                        "json" | "jsonb" | "bytea" | "tsvector" | "tsquery"
                    ) =>
                {
                    Some(left)
                }
                (Some(left), _) if matches!(left.as_str(), "bit" | "varbit") => {
                    Some("varbit".to_string())
                }
                _ => Some("text".to_string()),
            },
            BinaryOperator::Minus
                if self
                    .infer_env_expr_type(left, env)
                    .is_some_and(|pg_type| matches!(pg_type.as_str(), "json" | "jsonb")) =>
            {
                self.infer_env_expr_type(left, env)
            }
            BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Modulo => {
                let left = self.infer_env_expr_type(left, env);
                let right = self.infer_env_expr_type(right, env);
                let left_interval = left.as_deref() == Some("interval");
                let right_interval = right.as_deref() == Some("interval");
                let left_scalar = left.as_deref().is_some_and(is_interval_scalar_type);
                let right_scalar = right.as_deref().is_some_and(is_interval_scalar_type);
                if ((left_interval && right_interval)
                    && matches!(op, BinaryOperator::Plus | BinaryOperator::Minus))
                    || ((left_interval && right_scalar) || (left_scalar && right_interval))
                        && matches!(op, BinaryOperator::Multiply)
                {
                    return Some("interval".to_string());
                }
                if matches!(op, BinaryOperator::Minus)
                    && left.as_deref() == Some("time")
                    && right.as_deref() == Some("time")
                {
                    return Some("interval".to_string());
                }
                if (((left.as_deref() == Some("time") && right.as_deref() == Some("interval"))
                    || (left.as_deref() == Some("interval") && right.as_deref() == Some("time")))
                    && matches!(op, BinaryOperator::Plus))
                    || (left.as_deref() == Some("time")
                        && right.as_deref() == Some("interval")
                        && matches!(op, BinaryOperator::Minus))
                {
                    return Some("time".to_string());
                }
                if (((left.as_deref() == Some("timetz") && right.as_deref() == Some("interval"))
                    || (left.as_deref() == Some("interval") && right.as_deref() == Some("timetz")))
                    && matches!(op, BinaryOperator::Plus))
                    || (left.as_deref() == Some("timetz")
                        && right.as_deref() == Some("interval")
                        && matches!(op, BinaryOperator::Minus))
                {
                    return Some("timetz".to_string());
                }
                let timestamp_interval_plus = matches!(op, BinaryOperator::Plus)
                    && ((left.as_deref() == Some("timestamp")
                        && right.as_deref() == Some("interval"))
                        || (left.as_deref() == Some("interval")
                            && right.as_deref() == Some("timestamp")));
                let timestamp_interval_minus = matches!(op, BinaryOperator::Minus)
                    && left.as_deref() == Some("timestamp")
                    && right.as_deref() == Some("interval");
                if timestamp_interval_plus || timestamp_interval_minus {
                    return Some("timestamp".to_string());
                }
                if left.as_deref() == Some("timestamp")
                    && right.as_deref() == Some("timestamp")
                    && matches!(op, BinaryOperator::Minus)
                {
                    return Some("interval".to_string());
                }
                let timestamptz_interval_plus = matches!(op, BinaryOperator::Plus)
                    && ((left.as_deref() == Some("timestamptz")
                        && right.as_deref() == Some("interval"))
                        || (left.as_deref() == Some("interval")
                            && right.as_deref() == Some("timestamptz")));
                let timestamptz_interval_minus = matches!(op, BinaryOperator::Minus)
                    && left.as_deref() == Some("timestamptz")
                    && right.as_deref() == Some("interval");
                if timestamptz_interval_plus || timestamptz_interval_minus {
                    return Some("timestamptz".to_string());
                }
                if left.as_deref() == Some("timestamptz")
                    && right.as_deref() == Some("timestamptz")
                    && matches!(op, BinaryOperator::Minus)
                {
                    return Some("interval".to_string());
                }
                if left.as_deref() == Some("money") || right.as_deref() == Some("money") {
                    return money_arithmetic_pg_type(left.as_deref(), op, right.as_deref());
                }
                numeric_combine_pg_type(left.as_deref(), right.as_deref())
            }
            BinaryOperator::Divide => {
                let left = self.infer_env_expr_type(left, env);
                let right = self.infer_env_expr_type(right, env);
                if left.as_deref() == Some("interval")
                    && right.as_deref().is_some_and(is_interval_scalar_type)
                {
                    return Some("interval".to_string());
                }
                if left.as_deref() == Some("money") || right.as_deref() == Some("money") {
                    return money_arithmetic_pg_type(left.as_deref(), op, right.as_deref());
                }
                numeric_combine_pg_type(left.as_deref(), right.as_deref())
            }
            BinaryOperator::BitwiseAnd
            | BinaryOperator::BitwiseOr
            | BinaryOperator::PGBitwiseXor
            | BinaryOperator::PGBitwiseShiftLeft
            | BinaryOperator::PGBitwiseShiftRight => self
                .infer_env_expr_type(left, env)
                .filter(|pg_type| matches!(pg_type.as_str(), "bit" | "varbit"))
                .map(|_| "bit".to_string()),
            _ => None,
        }
    }

    /// Common type of a set of CASE/COALESCE branches: the first branch type that
    /// resolves, unless a later branch is wider (numeric promotion). Conservative
    /// — returns `None` if no branch types or they conflict irreconcilably.
    pub(crate) fn common_branch_type(
        &self,
        branches: &[&Expr],
        env: &[RelationColumns],
    ) -> Option<String> {
        let types = branches
            .iter()
            .map(|branch| self.infer_env_expr_type(branch, env))
            .collect::<Vec<_>>();
        select_common_pg_type(self.db_ref(), &types, "CASE").ok()
    }

    pub(crate) fn active_record_table_name_query(
        &self,
        select: &Select,
        query: &Query,
        from: &TableWithJoins,
    ) -> Result<Option<SqlResult>> {
        let Some((class_alias, namespace_alias)) = active_record_table_name_aliases(from)? else {
            return Ok(None);
        };
        if !active_record_table_name_projection_matches(&select.projection, &class_alias) {
            return Ok(None);
        }
        if !active_record_table_name_order_by_matches(query.order_by.as_ref(), &class_alias) {
            return Ok(None);
        }
        if !pg_class_namespace_summary_selection_covered(
            select.selection.as_ref(),
            &class_alias,
            &namespace_alias,
        )? {
            return Ok(None);
        }
        let relnames = string_filter_values_from_selection(
            select.selection.as_ref(),
            &class_alias,
            "pg_class",
            &["relname"],
        )?;
        let relkinds = string_filter_values_from_selection(
            select.selection.as_ref(),
            &class_alias,
            "pg_class",
            &["relkind"],
        )?;
        let schema_names = string_filter_values_from_selection(
            select.selection.as_ref(),
            &namespace_alias,
            "pg_namespace",
            &["nspname"],
        )?;
        let mut rows = active_record_table_name_rows(
            self.db_ref(),
            relnames.as_ref(),
            relkinds.as_ref(),
            schema_names.as_ref(),
        )?;
        apply_row_limit(&mut rows, query)?;
        Ok(Some(SqlResult::new(vec!["relname".to_string()], rows)))
    }

    pub(crate) fn comma_from_items_to_cross_join(from_items: &[TableWithJoins]) -> TableWithJoins {
        let mut normalized = from_items[0].clone();
        for from in &from_items[1..] {
            let relation = if from.joins.is_empty() {
                from.relation.clone()
            } else {
                TableFactor::NestedJoin {
                    table_with_joins: Box::new(from.clone()),
                    alias: None,
                }
            };
            normalized.joins.push(Join {
                relation,
                global: false,
                join_operator: JoinOperator::CrossJoin(JoinConstraint::None),
            });
        }
        normalized
    }

    pub(crate) fn row_set_from_table_with_joins(&self, from: &TableWithJoins) -> Result<RowSet> {
        self.row_set_from_table_with_joins_with_selection(from, None, None, None, None)
    }

    pub(crate) fn row_set_from_table_with_joins_with_selection(
        &self,
        from: &TableWithJoins,
        selection: Option<&Expr>,
        projection: Option<&[SelectItem]>,
        order_by: Option<&OrderBy>,
        needed_columns: Option<&ReferencedColumns>,
    ) -> Result<RowSet> {
        if let Some(row_set) = self.pg_class_index_join_row_set(from, selection)? {
            return Ok(row_set);
        }
        if let Some(row_set) = self.pg_class_indexrelid_join_row_set(from, selection)? {
            return Ok(row_set);
        }
        if let Some(row_set) =
            self.pg_class_namespace_join_row_set(from, selection, projection, order_by)?
        {
            return Ok(row_set);
        }
        if let Some(row_set) = self.pg_constraint_foreign_key_join_row_set(from, selection)? {
            return Ok(row_set);
        }
        if let Some(row_set) = self.pg_constraint_class_namespace_join_row_set(from, selection)? {
            return Ok(row_set);
        }
        if let Some(row_set) = self.pg_locks_class_join_row_set(from)? {
            return Ok(row_set);
        }

        let (relation, joins) = self.plan_row_join_order(from, selection)?;
        let can_push_join_selection = joins.iter().all(is_reorderable_inner_join);
        let base_pushdown_selection = if can_push_join_selection {
            selection_with_join_transitive_predicates(selection, &relation, &joins)?
        } else {
            self.base_table_selection_with_outer(&relation, selection)?
        };
        let materialize_selection = if can_push_join_selection {
            base_pushdown_selection.as_ref().or(selection)
        } else {
            base_pushdown_selection.as_ref()
        };
        // Projection pushdown runs through joins whose constraints are ON or
        // absent: every column they need appears in an expression the
        // referenced-column scan visited (projection, WHERE, ON, ORDER BY,
        // subqueries). USING/NATURAL joins keep every field.
        let base_needed_columns = if joins.iter().all(join_allows_projection_pushdown) {
            needed_columns
        } else {
            None
        };
        let mut row_set = self.row_set_from_table_factor_with_selection(
            &relation,
            materialize_selection,
            base_needed_columns,
        )?;
        self.apply_pushable_base_predicates(&mut row_set, materialize_selection)?;
        let mut cumulative_inner_selection = can_push_join_selection
            .then(|| selection.cloned())
            .flatten();
        for join in &joins {
            self.check_cancellation()?;
            row_set = self.apply_row_join(
                row_set,
                join,
                selection,
                cumulative_inner_selection.as_ref(),
                base_needed_columns,
            )?;
            if is_reorderable_inner_join(join) {
                if let Some(constraint) = join_operator_constraint(&join.join_operator) {
                    cumulative_inner_selection = selection_with_join_constraint(
                        cumulative_inner_selection.as_ref(),
                        constraint,
                    );
                }
            }
        }
        Ok(row_set)
    }

    pub(crate) fn pg_dump_relation_inventory_query(
        &self,
        select: &Select,
        query: &Query,
        from: &TableWithJoins,
    ) -> Result<Option<SqlResult>> {
        let Some(aliases) = pg_dump_relation_inventory_aliases(from)? else {
            return Ok(None);
        };
        if !pg_dump_relation_inventory_projection_matches(&select.projection) {
            return Ok(None);
        }
        if !pg_dump_relation_inventory_order_by_matches(query.order_by.as_ref(), &aliases.class) {
            return Ok(None);
        }
        let Some(relkinds) = string_filter_values_from_selection(
            select.selection.as_ref(),
            &aliases.class,
            "pg_class",
            &["relkind"],
        )?
        else {
            return Ok(None);
        };

        let am_rows_by_oid = virtual_rows_by_oid(pg_am_rows());
        let tablespace_rows_by_oid = virtual_rows_by_oid(pg_tablespace_rows());
        let selection_covered_by_relkind = pg_dump_relation_inventory_selection_covered_by_relkind(
            select.selection.as_ref(),
            &aliases.class,
        )?;
        if selection_covered_by_relkind
            && !relkinds
                .iter()
                .any(|relkind| relkind.eq_ignore_ascii_case("i"))
        {
            let mut result_rows =
                pg_dump_relation_inventory_result_rows_direct(self.db_ref(), &relkinds)?;
            apply_row_limit(&mut result_rows, query)?;
            return Ok(Some(SqlResult::new(
                pg_dump_relation_inventory_columns(),
                result_rows,
            )));
        }
        let mut materialized_rows = Vec::new();
        let mut result_rows = Vec::new();
        let mut class_rows = pg_class_rows_filtered(self.db_ref(), None, Some(&relkinds), None)?;
        class_rows.sort_by(|left, right| {
            sql_value_i64(&virtual_cell(left, "oid"))
                .unwrap_or_default()
                .cmp(&sql_value_i64(&virtual_cell(right, "oid")).unwrap_or_default())
        });

        for (idx, class_row) in class_rows.into_iter().enumerate() {
            if idx % 1024 == 0 {
                self.check_cancellation()?;
            }
            if !selection_covered_by_relkind {
                let row = pg_dump_relation_inventory_context_row(
                    &aliases,
                    &class_row,
                    &am_rows_by_oid,
                    &tablespace_rows_by_oid,
                );
                if let Some(selection) = &select.selection {
                    if !self.eval_row_predicate(&row, selection)? {
                        continue;
                    }
                }
                materialized_rows.push(row);
            }
            result_rows.push(pg_dump_relation_inventory_result_row(
                &class_row,
                &am_rows_by_oid,
                &tablespace_rows_by_oid,
            ));
        }
        sql_profile_sql_rows_materialized(&materialized_rows);
        apply_row_limit(&mut result_rows, query)?;
        Ok(Some(SqlResult::new(
            pg_dump_relation_inventory_columns(),
            result_rows,
        )))
    }

    pub(crate) fn pg_dump_column_info_query(
        &self,
        select: &Select,
        query: &Query,
        from: &TableWithJoins,
    ) -> Result<Option<SqlResult>> {
        let Some(aliases) = pg_dump_column_info_aliases(from)? else {
            return Ok(None);
        };
        if !pg_dump_column_info_projection_matches(&select.projection) {
            return Ok(None);
        }
        if !pg_dump_column_info_selection_matches(select.selection.as_ref(), &aliases.attribute) {
            return Ok(None);
        }
        if !pg_dump_column_info_order_by_matches(query.order_by.as_ref(), &aliases.attribute) {
            return Ok(None);
        }

        let rows = pg_dump_column_info_result_rows(self.db_ref(), &aliases.attrelids)?;
        sql_profile_rows_materialized(rows.len(), sql_result_rows_memory_estimate(&rows));
        Ok(Some(SqlResult::new(pg_dump_column_info_columns(), rows)))
    }

    pub(crate) fn pg_dump_constraint_inventory_query(
        &self,
        select: &Select,
        query: &Query,
        from: &TableWithJoins,
    ) -> Result<Option<SqlResult>> {
        let Some(aliases) = pg_dump_constraint_inventory_aliases(from)? else {
            return Ok(None);
        };
        let Some(projection_kind) =
            pg_dump_constraint_inventory_projection_kind(&select.projection)
        else {
            return Ok(None);
        };
        let Some(selection_kind) = pg_dump_constraint_inventory_selection_kind(
            select.selection.as_ref(),
            &aliases.constraint,
        ) else {
            return Ok(None);
        };
        if projection_kind != selection_kind
            || !pg_dump_constraint_inventory_order_by_matches(
                query.order_by.as_ref(),
                &aliases.constraint,
            )
            || !group_by_exprs(select)?.is_empty()
            || has_aggregates(&select.projection)
        {
            return Ok(None);
        }

        let table_oids = table_oids(self.db_ref());
        let schemas = if projection_kind == PgDumpConstraintInventoryKind::ForeignKey {
            list_schemas(self.db_ref())?
        } else {
            load_schemas_for_relation_oid_filter(
                self.db_ref(),
                &table_oids,
                Some(&aliases.conrelids),
            )?
        };
        let mut result_rows = pg_dump_constraint_inventory_result_rows(
            projection_kind,
            &schemas,
            &table_oids,
            &aliases.conrelids,
        );
        apply_row_limit(&mut result_rows, query)?;
        Ok(Some(SqlResult::new(
            pg_dump_constraint_inventory_columns(projection_kind),
            result_rows,
        )))
    }
}

const RESULT_TYPING_MEMO_MAX: usize = 65_536;

thread_local! {
    /// Inferred result column types + metadata per routine-IR-owned SELECT
    /// node: (catalog generation, routine IR, node address).
    static RESULT_TYPING_MEMO: std::cell::RefCell<
        rustc_hash::FxHashMap<
            (u64, usize, usize),
            std::rc::Rc<(Option<Vec<Option<String>>>, Option<Vec<SqlColumnMetadata>>)>,
        >,
    > = std::cell::RefCell::new(rustc_hash::FxHashMap::default());
}

#[cfg(test)]
thread_local! {
    pub(crate) static SQL_RESULT_TYPING_MEMO_HITS: std::cell::RefCell<usize> =
        const { std::cell::RefCell::new(0) };
}

/// Whether any expression of the query (projection, HAVING, ORDER BY, WHERE,
/// window specs — everything `visit_expressions` reaches) calls a function
/// whose unqualified name is `name` (ASCII case-insensitive).
pub(crate) fn query_mentions_function(select: &Select, query: &Query, name: &str) -> bool {
    use sqlparser::ast::visit_expressions;
    use std::ops::ControlFlow;
    let _ = select;
    let flow: ControlFlow<()> = visit_expressions(query, |expr| {
        if let Expr::Function(function) = expr {
            if function
                .name
                .0
                .last()
                .and_then(|part| part.as_ident())
                .is_some_and(|ident| ident.value.eq_ignore_ascii_case(name))
            {
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    });
    flow.is_break()
}
