//! Page unique suffix ranges with residual filtering and a pinned index snapshot.
use super::*;

impl<'db> SqlEngine<'db> {
    pub(crate) fn try_bounded_range_row_set(
        &self,
        from: &TableWithJoins,
        select: &Select,
        query: &Query,
    ) -> Result<Option<RowSet>> {
        if crate::plan_diagnostics::forced_signature().is_some()
            || !self.db_ref().is_server_paged()
            || self.security_context.is_some()
            || !session_role_is_superuser(self.db_ref(), &self.session_gucs)
            || self.tx.is_some_and(|tx| {
                tx.write_len() != 0
                    || tx.isolation() != bicdb_core::TransactionIsolation::ReadCommitted
            })
            || !from.joins.is_empty()
            || select.distinct.is_some()
            || select.prewhere.is_some()
            || select.having.is_some()
            || select.qualify.is_some()
            || select.into.is_some()
            || !select.connect_by.is_empty()
            || has_group_by(select)?
            || has_aggregates(&select.projection)
            || select_has_window_functions(select, query)?
            || query_has_row_locks(query)
            || !query.pipe_operators.is_empty()
        {
            return Ok(None);
        }
        let Some(selection) = select.selection.as_ref() else {
            return Ok(None);
        };
        if !super::bounded_fts::repeatable_predicate(selection) {
            return Ok(None);
        }
        let Some(LimitClause::LimitOffset {
            offset: None,
            limit_by,
            ..
        }) = &query.limit_clause
        else {
            return Ok(None);
        };
        if !limit_by.is_empty() {
            return Ok(None);
        }
        let Some(limit) = limit_for_plan(query)? else {
            return Ok(None);
        };
        if limit > 1000 {
            return Ok(None);
        }
        let TableFactor::Table {
            name,
            alias,
            args: None,
            ..
        } = &from.relation
        else {
            return Ok(None);
        };
        let relation = relation_name(name)?;
        if self.cte(&relation).is_some()
            || is_virtual_table(&relation)
            || load_view(self.db_ref(), &relation)?.is_some()
            || load_sequence(self.db_ref(), &relation)?.is_some()
        {
            return Ok(None);
        }
        let table = resolve_session_relation_name(self.db_ref(), &relation)?;
        if self.db_ref().collection_has_policy(&table)? {
            return Ok(None);
        }
        let Some(schema) = load_schema(self.db_ref(), &table)? else {
            return Ok(None);
        };
        if schema.rls_enabled || schema.rls_forced {
            return Ok(None);
        }
        let alias = alias
            .as_ref()
            .map(|a| a.name.value.as_str())
            .unwrap_or_else(|| table.rsplit('.').next().unwrap_or(&table));
        let Some(order) = query.order_by.as_ref() else {
            return Ok(None);
        };
        let OrderByKind::Expressions(terms) = &order.kind else {
            return Ok(None);
        };
        let [order] = terms.as_slice() else {
            return Ok(None);
        };
        if order.options.asc == Some(false)
            || !matches!(
                order.expr,
                Expr::Identifier(_) | Expr::CompoundIdentifier(_)
            )
        {
            return Ok(None);
        }
        self.reject_encrypted_predicates(&table, Some(selection))?;
        for index in sql_index_definitions_for_collection(self.db_ref(), &table) {
            if index.kind != IndexKind::BTree
                || !index.unique
                || index.predicate.is_some()
                || index.fields.len() < 2
                || !index_supports_ordering(&schema, &index.name)
            {
                continue;
            }
            let mut columns = Vec::new();
            let mut usable = true;
            for field in &index.fields {
                let IndexField::MetadataPath(path) = field else {
                    usable = false;
                    break;
                };
                if path.len() != 1 {
                    usable = false;
                    break;
                }
                let Some(column) = schema.column(&path[0]) else {
                    usable = false;
                    break;
                };
                if column.collation.is_some()
                    || column.type_modifier.is_some()
                    || column.user_type.is_some()
                    || !matches!(column.pg_type.as_str(), "text" | "int2" | "int4" | "int8")
                {
                    usable = false;
                    break;
                }
                columns.push((path[0].clone(), column.pg_type.clone()));
            }
            if !usable {
                continue;
            }
            let (suffix, _) = columns.last().unwrap();
            if !self.expr_matches_table_column(&order.expr, &table, alias, Some(&schema), suffix)? {
                continue;
            }
            let mut comparisons = Vec::new();
            for term in and_terms(selection) {
                let Expr::BinaryOp { left, op, right } = term else {
                    continue;
                };
                if !matches!(
                    left.as_ref(),
                    Expr::Identifier(_) | Expr::CompoundIdentifier(_)
                ) || !matches!(right.as_ref(), Expr::Value(_))
                    || !matches!(
                        op,
                        BinaryOperator::Eq
                            | BinaryOperator::Gt
                            | BinaryOperator::GtEq
                            | BinaryOperator::Lt
                            | BinaryOperator::LtEq
                    )
                {
                    continue;
                }
                for (n, (column, ty)) in columns.iter().enumerate() {
                    if self.expr_matches_table_column(left, &table, alias, Some(&schema), column)? {
                        let value = self.eval_dynamic_bound_expr(right)?;
                        let exact = match (&value, ty.as_str()) {
                            (SqlValue::String(_), "text") => true,
                            (SqlValue::Int(_), "int2" | "int4" | "int8") => true,
                            _ => false,
                        };
                        if exact {
                            comparisons.push((n, op.clone(), index_value_from_sql(value)?));
                        }
                    }
                }
            }
            let last = columns.len() - 1;
            let mut prefix = Vec::new();
            for n in 0..last {
                if let Some((_, _, value)) = comparisons
                    .iter()
                    .find(|(i, op, _)| *i == n && *op == BinaryOperator::Eq)
                {
                    prefix.push(value.clone());
                }
            }
            if prefix.len() != last {
                continue;
            }
            let mut lower: Option<(IndexValue, bool)> = None;
            let mut upper: Option<(IndexValue, bool)> = None;
            for (n, op, value) in comparisons {
                if n != last {
                    continue;
                }
                let (slot, inclusive) = match op {
                    BinaryOperator::Gt => (&mut lower, false),
                    BinaryOperator::GtEq => (&mut lower, true),
                    BinaryOperator::Lt => (&mut upper, false),
                    BinaryOperator::LtEq => (&mut upper, true),
                    _ => continue,
                };
                let is_lower = matches!(op, BinaryOperator::Gt | BinaryOperator::GtEq);
                if slot.as_ref().is_none_or(|(old, inc)| {
                    (is_lower && &value > old)
                        || (!is_lower && &value < old)
                        || (&value == old && *inc && !inclusive)
                }) {
                    *slot = Some((value, inclusive));
                }
            }
            let Some(lower) = lower else {
                continue;
            };
            let mut cursor =
                self.db_ref()
                    .unique_index_range_cursor(&index.name, &prefix, lower, upper)?;
            let fields = row_fields_for_records(Some(&schema), &[]);
            let columns = row_output_columns_from_fields(&table, alias, &fields);
            let (_scope, context) = self.bound_row_context(&columns);
            let predicate_env = self.from_relation_columns(&select.from).unwrap_or_default();
            let mut rows = Vec::with_capacity(limit);
            let mut fetched = 0;
            while rows.len() < limit {
                self.check_cancellation()?;
                let ids = cursor.next_batch(128.min(limit - rows.len()))?;
                if ids.is_empty() {
                    break;
                }
                for id in ids {
                    self.check_cancellation()?;
                    if let Some(record) = self.get_record_with_schema(&table, &id, Some(&schema))? {
                        fetched += 1;
                        let row = slot_row_from_record_fields(&table, alias, &fields, &record)?;
                        if self
                            .eval_slot_row_truth_typed(&row, &context, selection, &predicate_env)?
                            .unwrap_or(false)
                        {
                            rows.push(row);
                        }
                    }
                }
            }
            if crate::plan_diagnostics::active() {
                let signature = crate::plan_diagnostics::PlanSignature {
                    kind: "IndexRangeScan",
                    index: Some(index.name),
                    prefix_len: prefix.len() + 1,
                };
                crate::plan_diagnostics::record_decision_for(
                    "bounded-range",
                    &table,
                    vec![signature.clone()],
                    &signature,
                    Some(fetched),
                );
            }
            return Ok(Some(RowSet { rows, columns }));
        }
        Ok(None)
    }
}
