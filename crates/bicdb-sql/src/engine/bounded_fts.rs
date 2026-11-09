//! Hydrate ordered FTS candidates only until a page has enough exact matches.
use super::*;

// The ordinary row executor rechecks WHERE. Restrict this early check to a
// small, side-effect-free expression language so it cannot evaluate a user
// routine, subquery or volatile predicate twice.
pub(super) fn repeatable_predicate(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Value(_) => true,
        Expr::Nested(e) | Expr::IsNull(e) | Expr::IsNotNull(e) => repeatable_predicate(e),
        Expr::BinaryOp { left, op, right } => {
            matches!(op, BinaryOperator::And | BinaryOperator::Or | BinaryOperator::Eq
                | BinaryOperator::NotEq | BinaryOperator::Gt | BinaryOperator::GtEq
                | BinaryOperator::Lt | BinaryOperator::LtEq | BinaryOperator::AtAt)
                && repeatable_predicate(left) && repeatable_predicate(right)
        }
        Expr::Function(f) => {
            matches!(object_name(&f.name).ok().as_deref(),
                Some("to_tsvector" | "to_tsquery" | "plainto_tsquery" | "coalesce"))
                && f.filter.is_none() && f.over.is_none() && f.within_group.is_empty()
                && match &f.args {
                    FunctionArguments::List(args) => args.clauses.is_empty()
                        && args.duplicate_treatment.is_none()
                        && args.args.iter().all(|arg| matches!(arg,
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) if repeatable_predicate(e))),
                    _ => false,
                }
        }
        _ => false,
    }
}

impl<'db> SqlEngine<'db> {
    pub(crate) fn try_bounded_fts_row_set(
        &self,
        from: &TableWithJoins,
        select: &Select,
        query: &Query,
    ) -> Result<Option<RowSet>> {
        if crate::plan_diagnostics::forced_signature().is_some()
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
        if !repeatable_predicate(selection) {
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
        let keys = primary_key_columns_for_schema(&schema);
        if keys.len() != 1
            || schema.column(&keys[0]).is_none_or(|c| {
                c.pg_type != "text"
                    || c.collation.is_some()
                    || c.user_type.is_some()
                    || c.type_modifier.is_some()
            })
        {
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
        let [term] = terms.as_slice() else {
            return Ok(None);
        };
        if self.primary_key_expr_index(&term.expr, &table, alias, Some(&schema), &keys)? != Some(0)
        {
            return Ok(None);
        }
        self.reject_encrypted_predicates(&table, Some(selection))?;
        let Some((index, candidate)) =
            self.full_text_index_candidate(&table, alias, Some(&schema), selection)?
        else {
            return Ok(None);
        };
        if !self.db_ref().full_text_index_is_ready(&index)? {
            return Ok(None);
        }
        let ids = self.full_text_candidate_ids(&index, &candidate)?;
        // Apply simple primary-key cursor bounds before fetching payloads. A
        // later page must not rehydrate the candidates from every earlier page.
        let mut bounds = Vec::new();
        for term in and_terms(selection) {
            if let Expr::BinaryOp { left, op, right } = term {
                if matches!(
                    op,
                    BinaryOperator::Eq
                        | BinaryOperator::NotEq
                        | BinaryOperator::Gt
                        | BinaryOperator::GtEq
                        | BinaryOperator::Lt
                        | BinaryOperator::LtEq
                ) && matches!(right.as_ref(), Expr::Value(_))
                    && self.primary_key_expr_index(left, &table, alias, Some(&schema), &keys)?
                        == Some(0)
                {
                    match self.eval_dynamic_bound_expr(right)? {
                        SqlValue::String(value) => bounds.push((op.clone(), value)),
                        SqlValue::Null => {
                            return Ok(Some(RowSet {
                                rows: Vec::new(),
                                columns: row_output_columns_from_fields(
                                    &table,
                                    alias,
                                    &row_fields_for_records(Some(&schema), &[]),
                                ),
                            }))
                        }
                        _ => {}
                    }
                }
            }
        }
        let mut ordered = Vec::with_capacity(ids.len().min(
            self.db_ref().query_work_memory_bytes() / std::mem::size_of::<(String, String)>(),
        ));
        let mut ordered_bytes = 0usize;
        for (n, id) in ids.into_iter().enumerate() {
            if n % 1024 == 0 {
                self.check_cancellation()?;
            }
            let Some(values) = primary_key_values_from_record_id(&schema, &keys, &id)? else {
                return Ok(None);
            };
            let [SqlValue::String(key)] = values.as_slice() else {
                return Ok(None);
            };
            if !bounds.iter().all(|(op, value)| match op {
                BinaryOperator::Eq => key == value,
                BinaryOperator::NotEq => key != value,
                BinaryOperator::Gt => key > value,
                BinaryOperator::GtEq => key >= value,
                BinaryOperator::Lt => key < value,
                BinaryOperator::LtEq => key <= value,
                _ => unreachable!(),
            }) {
                continue;
            }
            ordered_bytes = ordered_bytes
                .saturating_add(std::mem::size_of::<(String, String)>().saturating_add(key.len()));
            if ordered_bytes > self.db_ref().query_work_memory_bytes() {
                return Err(SqlError::resource_limit(
                    "53400",
                    "ordered full-text candidate keys exceed query_work_memory_bytes",
                ));
            }
            ordered.push((key.clone(), id));
        }
        ordered.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        if term.options.asc == Some(false) {
            ordered.reverse();
        }
        let fields = row_fields_for_records(Some(&schema), &[]);
        let columns = row_output_columns_from_fields(&table, alias, &fields);
        let (_scope, context) = self.bound_row_context(&columns);
        let predicate_env = self.from_relation_columns(&select.from).unwrap_or_default();
        let mut rows = Vec::with_capacity(limit);
        let mut fetched = 0;
        for (_, id) in ordered {
            if rows.len() == limit {
                break;
            }
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
        if crate::plan_diagnostics::active() {
            let signature = crate::plan_diagnostics::PlanSignature {
                kind: "FullTextIndexScan",
                index: Some(index),
                prefix_len: 0,
            };
            crate::plan_diagnostics::record_decision_for(
                "bounded-fts",
                &table,
                vec![signature.clone()],
                &signature,
                Some(fetched),
            );
        }
        Ok(Some(RowSet { rows, columns }))
    }
}
