//! SELECT FOR UPDATE uses transaction row locks shared with ordinary DML.
use super::*;
use sqlparser::ast::NonBlock;

/// Message prefix of the conflict a READ COMMITTED `FOR UPDATE` returns after
/// it waited for a row another transaction changed. The session refreshes its
/// snapshot and re-runs the statement; anywhere else it stays a retryable 40001.
pub(crate) const READ_COMMITTED_LOCK_REFRESH: &str = "row changed while waiting for its lock: ";

pub(crate) fn is_read_committed_lock_refresh(error: &SqlError) -> bool {
    matches!(error, SqlError::BicDb(BicDbError::TransactionConflict(message)) if message.starts_with(READ_COMMITTED_LOCK_REFRESH))
}

pub(crate) type RowLockTarget = (String, String, TableSchema, Vec<String>, Option<NonBlock>);

impl SqlEngine<'_> {
    pub(crate) fn execute_locking_query(&self, query: &Query) -> Result<SqlResult> {
        self.tx.ok_or_else(|| {
            SqlError::Unsupported("locking SELECT requires a transaction-backed SQL session".into())
        })?;
        let SetExpr::Select(select) = query.body.as_ref() else {
            return Err(SqlError::Unsupported(
                "row locking requires a SELECT of base tables".into(),
            ));
        };
        if select.distinct.is_some()
            || !matches!(&select.group_by, GroupByExpr::Expressions(expressions, modifiers) if expressions.is_empty() && modifiers.is_empty())
            || select.having.is_some()
            || select_has_window_functions(select, query)?
            || !query_group_aggregate_functions(select, query)?.is_empty()
            || query.fetch.is_some()
            || json_projection_set_returning_call(&select.projection)?.is_some()
            || !array_projection_set_returning_calls(&select.projection)?.is_empty()
        {
            return Err(SqlError::Unsupported(
                "row locking does not support DISTINCT, grouping, window functions, projection set-returning functions, or FETCH"
                    .into(),
            ));
        }
        let mut relations = Vec::new();
        for from in &select.from {
            let start = relations.len();
            relations.push((&from.relation, false));
            for join in &from.joins {
                // OF may lock a preserved base table even when another input
                // is a nullable outer/lateral join. Track null-extension per
                // input; a later RIGHT/FULL join also makes earlier inputs
                // nullable. Never attempt to lock an absent outer-join row.
                let nullable = match join.join_operator {
                    JoinOperator::Inner(_) | JoinOperator::Join(_) | JoinOperator::CrossJoin(_) => {
                        false
                    }
                    JoinOperator::Left(_) | JoinOperator::LeftOuter(_) => true,
                    JoinOperator::Right(_) | JoinOperator::RightOuter(_) => {
                        for (_, nullable) in &mut relations[start..] {
                            *nullable = true;
                        }
                        false
                    }
                    JoinOperator::FullOuter(_) => {
                        for (_, nullable) in &mut relations[start..] {
                            *nullable = true;
                        }
                        true
                    }
                    _ => {
                        return Err(SqlError::Unsupported(
                            "row locking does not support this join type".into(),
                        ))
                    }
                };
                relations.push((&join.relation, nullable));
            }
        }
        let mut targets = Vec::new();
        // FOR SHARE takes the same exclusive row lock as FOR UPDATE: never
        // weaker than PostgreSQL, but two sharers of one row conflict (40001).
        for lock in &query.locks {
            let requested = lock.of.as_ref().map(object_name).transpose()?;
            let before = targets.len();
            for (relation, nullable) in &relations {
                let TableFactor::Table {
                    name,
                    alias,
                    args: None,
                    ..
                } = relation
                else {
                    // A FROM function contributes values, not lockable tuples.
                    // Unqualified FOR UPDATE still locks every base input;
                    // explicitly naming the function itself remains an error.
                    let function_names = match relation {
                        TableFactor::Table {
                            name,
                            alias,
                            args: Some(_),
                            ..
                        }
                        | TableFactor::Function { name, alias, .. } => Some((
                            alias.as_ref().map(|alias| alias.name.value.clone()),
                            object_name(name)?,
                        )),
                        TableFactor::UNNEST { alias, .. } => Some((
                            alias.as_ref().map(|alias| alias.name.value.clone()),
                            "unnest".to_string(),
                        )),
                        TableFactor::TableFunction { alias, .. } => Some((
                            alias.as_ref().map(|alias| alias.name.value.clone()),
                            String::new(),
                        )),
                        _ => None,
                    };
                    if let Some((alias, name)) = function_names {
                        if requested
                            .as_ref()
                            .is_some_and(|requested| alias.as_ref().unwrap_or(&name) == requested)
                        {
                            return Err(SqlError::Unsupported(
                                "FOR UPDATE/SHARE cannot be applied to a function".into(),
                            ));
                        }
                        continue;
                    }
                    if requested.is_none() {
                        return Err(SqlError::Unsupported(
                            "row locking targets must be base tables".into(),
                        ));
                    }
                    continue;
                };
                let physical_name = relation_name(name)?;
                let logical_alias = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| object_name_parts(name).last().cloned().unwrap_or_default());
                let name = object_name(name)?;
                if requested
                    .as_ref()
                    .is_some_and(|requested| requested != &logical_alias && requested != &name)
                {
                    continue;
                }
                if *nullable {
                    return Err(SqlError::Unsupported(
                        "FOR UPDATE/SHARE cannot be applied to the nullable side of an outer join"
                            .into(),
                    ));
                }
                let table = resolve_session_relation_name(self.db_ref(), &physical_name)?;
                // Unaliased row sets use the encoded physical relation as
                // their column prefix. OF resolves a logical SQL table name,
                // but lock-key extraction must address that actual row set.
                let row_alias = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| {
                        physical_name
                            .rsplit('.')
                            .next()
                            .unwrap_or(&physical_name)
                            .to_string()
                    });
                self.require_relation_privilege(&table, "SELECT")?;
                self.require_relation_privilege(&table, "UPDATE")?;
                let schema = load_schema(self.db_ref(), &table)?.ok_or_else(|| {
                    SqlError::Unsupported("row locking targets must be base tables".into())
                })?;
                let keys = primary_key_columns_for_schema(&schema);
                if keys.is_empty() {
                    return Err(SqlError::Unsupported(
                        "row locking currently requires a primary key".into(),
                    ));
                }
                targets.push((table, row_alias, schema, keys, lock.nonblock));
            }
            if targets.len() == before {
                return Err(SqlError::InvalidSql(
                    "FOR UPDATE/SHARE OF target is not in the FROM clause".into(),
                ));
            }
        }
        if targets.is_empty() {
            return Err(SqlError::InvalidSql(
                "FOR UPDATE requires a base table".into(),
            ));
        }
        // The normalized FROM and cached ordering expressions are temporary,
        // so pointer-keyed routine-IR memos must not outlive these AST nodes.
        let engine = self
            .inherit_transaction(SqlEngine::with_ctes_and_context(
                self.db_ref(),
                self.settings,
                self.ctes.clone(),
                self.security_context.clone(),
                self.session_gucs.clone(),
            ))
            .with_shared_routine_vars(self.routine_vars.clone())
            .with_routine_slots(self.routine_slots.clone())
            .with_cancellation(self.cancellation.clone())
            .with_fts_limits(self.fts_limits);
        let from = Self::comma_from_items_to_cross_join(&select.from);
        let row_set = engine.row_set_from_table_with_joins_with_selection(
            &from,
            select.selection.as_ref(),
            None,
            query.order_by.as_ref(),
            None,
        )?;
        engine.execute_materialized_row_query_with_locks(select, query, &from, row_set, &targets)
    }

    pub(crate) fn lock_materialized_rows(
        &self,
        query: &Query,
        columns: &[String],
        rows: Vec<SlotRow>,
        targets: &[RowLockTarget],
    ) -> Result<Vec<SlotRow>> {
        let tx = self
            .tx
            .ok_or_else(|| SqlError::Unsupported("locking SELECT requires a transaction".into()))?;
        let wait_duration = oracle::lock_waits::duration(self.db_ref(), query)?;
        let key_slots = targets
            .iter()
            .map(|(_, alias, _, keys, _)| {
                keys.iter()
                    .map(|key| {
                        slot_row_column_index(columns, &format!("{alias}.{key}"))
                            .or_else(|| slot_row_column_index(columns, key))
                            .ok_or_else(|| SqlError::UndefinedColumn {
                                table: alias.clone(),
                                column: key.clone(),
                            })
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        let keep = order_by_keep_bound(query)?;
        let mut locked = Vec::new();
        for row in rows {
            if keep.is_some_and(|keep| locked.len() >= keep) {
                break;
            }
            self.check_cancellation()?;
            let mut skip = false;
            let mut ids = Vec::with_capacity(targets.len());
            // UPDATE USING policies are additional to SELECT visibility for
            // a locking read. Check every target before reserving any row.
            for ((table, _, schema, keys, _), slots) in targets.iter().zip(&key_slots) {
                let id = record_id_from_column_values(
                    table,
                    schema,
                    keys,
                    &slots
                        .iter()
                        .map(|slot| row[*slot].clone())
                        .collect::<Vec<_>>(),
                )?;
                let Some(record) = self.get_record(table, &id)? else {
                    skip = true;
                    break;
                };
                if !rls_allows_record_with_schema(
                    self,
                    table,
                    PolicyAction::Update,
                    &record,
                    Some(schema),
                )? {
                    skip = true;
                    break;
                }
                ids.push(id);
            }
            if skip {
                continue;
            }
            let candidate_mark = tx.rollback_mark();
            for ((table, _, _, _, nonblock), id) in targets.iter().zip(ids) {
                if nonblock.is_none() {
                    self.check_cancellation()?;
                    // Oracle's WAIT budget starts anew for each contended
                    // row. A child timer cannot extend statement cancellation
                    // or leave a deadline on subsequent session operations.
                    let local = wait_duration
                        .map(|duration| {
                            std::time::Instant::now()
                                .checked_add(duration)
                                .map(|deadline| self.cancellation.child_with_deadline(deadline))
                                .ok_or_else(|| {
                                    SqlError::InvalidSql(
                                        "lock-wait deadline exceeds the supported range".into(),
                                    )
                                })
                        })
                        .transpose()?;
                    let result = tx.lock_visible_record_cancellable(
                        self.security_context.as_ref(),
                        table,
                        &id,
                        local.as_ref().unwrap_or(&self.cancellation),
                    );
                    let acquired = match result {
                        Err(BicDbError::QueryTimedOut) if local.is_some() => {
                            // An earlier statement deadline or explicit cancel
                            // retains its own error instead of being relabeled
                            // as a row-lock timeout.
                            self.check_cancellation()?;
                            return Err(SqlError::RaisedException {
                                sqlstate: "55P03".into(),
                                message: format!(
                                    "row-lock wait deadline expired for relation {table}"
                                ),
                                detail: None,
                            });
                        }
                        result => result?,
                    };
                    if !acquired {
                        return Err(SqlError::BicDb(BicDbError::TransactionConflict(format!(
                            "{READ_COMMITTED_LOCK_REFRESH}{table}:{id}"
                        ))));
                    }
                    continue;
                }
                if !tx.try_lock_visible_record(self.security_context.as_ref(), table, &id)? {
                    match nonblock {
                        Some(NonBlock::SkipLocked) => {
                            tx.release_read_locks_since(candidate_mark)?;
                            skip = true;
                        }
                        Some(NonBlock::Nowait) => {
                            return Err(SqlError::RaisedException {
                                sqlstate: "55P03".into(),
                                message: format!(
                                    "could not obtain lock on row in relation {table}"
                                ),
                                detail: None,
                            })
                        }
                        None => {
                            self.check_cancellation()?;
                            // Do not wait while holding the engine's shared
                            // database latch: a concurrent commit needs it.
                            // Like contended optimistic writes, retry the TX.
                            return Err(SqlError::BicDb(BicDbError::TransactionConflict(format!(
                                "row is locked: {table}:{id}"
                            ))));
                        }
                    }
                }
                if skip {
                    break;
                }
            }
            if !skip {
                locked.push(row);
            }
        }
        apply_row_limit(&mut locked, query)?;
        Ok(locked)
    }
    /// Sorting may need an output expression before locks/limit. Cache that
    /// exact expression once per candidate and reuse its value for output.
    pub(crate) fn cache_locking_order_projections(
        &self,
        projection: &[SelectItem],
        order_by: &mut Option<OrderBy>,
        row_set: &mut RowSet,
        wildcard_columns: &[String],
    ) -> Result<Vec<SelectItem>> {
        let mut projected = projection.to_vec();
        let Some(OrderBy {
            kind: OrderByKind::Expressions(orders),
            ..
        }) = order_by
        else {
            return Ok(projected);
        };
        let mut cached = BTreeMap::<String, Ident>::new();
        for order in orders {
            let key = order.expr.to_string();
            if !projection.iter().any(|item| match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    expr.to_string() == key
                }
                _ => false,
            }) {
                continue;
            }
            let column = if let Some(column) = cached.get(&key) {
                column.clone()
            } else {
                let mut suffix = row_set.columns.len();
                let name = loop {
                    let name = format!("__bicdb_lock_order_{suffix}");
                    if !row_set
                        .columns
                        .iter()
                        .any(|column| column.eq_ignore_ascii_case(&name))
                    {
                        break name;
                    }
                    suffix += 1;
                };
                let values = self.project_slot_row_select_with_wildcard(
                    &[SelectItem::UnnamedExpr(order.expr.clone())],
                    &row_set.rows,
                    &row_set.columns,
                    wildcard_columns,
                )?;
                for (row, mut value) in row_set.rows.iter_mut().zip(values.rows) {
                    row.push(value.remove(0));
                }
                row_set.columns.push(name.clone());
                let column = Ident::with_quote('"', name);
                cached.insert(key.clone(), column.clone());
                column
            };
            order.expr = Expr::Identifier(column.clone());
            for (original, output) in projection.iter().zip(&mut projected) {
                match original {
                    SelectItem::UnnamedExpr(expr) if expr.to_string() == key => {
                        *output = SelectItem::ExprWithAlias {
                            expr: Expr::Identifier(column.clone()),
                            alias: Ident::with_quote('"', row_expr_column_name(expr)),
                        };
                    }
                    SelectItem::ExprWithAlias { expr, alias } if expr.to_string() == key => {
                        *output = SelectItem::ExprWithAlias {
                            expr: Expr::Identifier(column.clone()),
                            alias: alias.clone(),
                        };
                    }
                    _ => {}
                }
            }
        }
        Ok(projected)
    }
}

/// Positional ORDER BY refers to the original visible output, including wildcard
/// expansion, rather than physical input, binding-alias or lock-key columns.
#[derive(Clone, Copy)]
pub(crate) struct MaterializedOrderColumn {
    pub(crate) source_slot: usize,
    pub(crate) output_position: usize,
}

pub(crate) fn resolve_materialized_order_positions(
    order_by: &mut Option<OrderBy>,
    projection: &[SelectItem],
    columns: &[String],
    wildcard_columns: &[String],
) -> Result<Vec<Option<MaterializedOrderColumn>>> {
    let Some(OrderBy {
        kind: OrderByKind::Expressions(orders),
        ..
    }) = order_by
    else {
        return Ok(Vec::new());
    };
    let mut outputs = Vec::new();
    for item in projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                outputs.push((expr.clone(), None))
            }
            SelectItem::Wildcard(_) => outputs.extend(
                wildcard_columns
                    .iter()
                    .zip(wildcard_column_slots(columns, wildcard_columns)?)
                    .map(|(column, slot)| {
                        (Expr::Identifier(Ident::with_quote('"', column)), Some(slot))
                    }),
            ),
            SelectItem::QualifiedWildcard(qualifier, _) => {
                outputs.extend(
                    qualified_wildcard_slots(qualifier, columns, columns)?
                        .into_iter()
                        .map(|(slot, source, _)| {
                            (Expr::Identifier(Ident::with_quote('"', source)), Some(slot))
                        }),
                );
            }
            _ => {}
        }
    }
    let mut bindings = Vec::with_capacity(orders.len());
    for order in orders {
        let mut binding = None;
        if let Expr::Value(value) = &order.expr {
            if let Value::Number(number, _) = &value.value {
                if let Ok(position) = number.parse::<usize>() {
                    let output_position = position
                        .checked_sub(1)
                        .filter(|index| *index < outputs.len())
                        .ok_or_else(|| {
                            SqlError::InvalidSql(
                                "ORDER BY position is not in the select list".into(),
                            )
                        })?;
                    let (expression, source_slot) = &outputs[output_position];
                    order.expr = expression.clone();
                    binding = source_slot.map(|source_slot| MaterializedOrderColumn {
                        source_slot,
                        output_position,
                    });
                }
            }
        }
        bindings.push(binding);
    }
    Ok(bindings)
}
