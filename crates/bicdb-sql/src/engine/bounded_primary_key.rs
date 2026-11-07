//! Bound simple administrative UUID inventory reads before loading row payloads.
use super::*;

fn literal_inventory_bound(expr: &Expr) -> bool {
    match expr {
        Expr::Nested(inner) | Expr::Cast { expr: inner, .. } => literal_inventory_bound(inner),
        Expr::Value(_) => true,
        _ => false,
    }
}

impl<'db> SqlEngine<'db> {
    pub(crate) fn try_bounded_uuid_primary_key_row_set(
        &self,
        from: &TableWithJoins,
        select: &Select,
        query: &Query,
    ) -> Result<Option<RowSet>> {
        // Other principals retain the full policy-filtered execution path.
        // A pending mutation also needs the ordinary read-your-writes path.
        if !session_role_is_superuser(self.db_ref(), &self.session_gucs)
            || self.tx.is_some_and(|tx| tx.write_len() != 0)
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
            || json_projection_set_returning_call(&select.projection)?.is_some()
            || !array_projection_set_returning_calls(&select.projection)?.is_empty()
            || query_has_row_locks(query)
            || !query.pipe_operators.is_empty()
        {
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
        let collection = resolve_session_relation_name(self.db_ref(), &relation)?;
        if self.db_ref().collection_has_policy(&collection)? {
            return Ok(None);
        }
        let Some(schema) = load_schema(self.db_ref(), &collection)? else {
            return Ok(None);
        };
        self.reject_encrypted_predicates(&collection, select.selection.as_ref())?;
        let keys = primary_key_columns_for_schema(&schema);
        if keys.len() != 1
            || schema
                .column(&keys[0])
                .is_none_or(|column| column.pg_type != "uuid")
        {
            return Ok(None);
        }
        let alias_name = alias
            .as_ref()
            .map(|alias| alias.name.value.as_str())
            .unwrap_or_else(|| collection.rsplit('.').next().unwrap_or(&collection));
        let Some(order) = query.order_by.as_ref() else {
            return Ok(None);
        };
        let OrderByKind::Expressions(terms) = &order.kind else {
            return Ok(None);
        };
        let [term] = terms.as_slice() else {
            return Ok(None);
        };
        if self.primary_key_expr_index(&term.expr, &collection, alias_name, Some(&schema), &keys)?
            != Some(0)
        {
            return Ok(None);
        }
        let descending = term.options.asc == Some(false);
        let bound = if let Some(mut predicate) = select.selection.as_ref() {
            while let Expr::Nested(inner) = predicate {
                predicate = inner;
            }
            let Expr::BinaryOp { left, op, right } = predicate else {
                return Ok(None);
            };
            if !matches!(
                op,
                BinaryOperator::Eq
                    | BinaryOperator::Gt
                    | BinaryOperator::GtEq
                    | BinaryOperator::Lt
                    | BinaryOperator::LtEq
            ) || self.primary_key_expr_index(
                left,
                &collection,
                alias_name,
                Some(&schema),
                &keys,
            )? != Some(0)
                || !literal_inventory_bound(right)
            {
                return Ok(None);
            }
            let value = self.eval_dynamic_bound_expr(right)?;
            if matches!(value, SqlValue::Null) {
                Some((op.clone(), None))
            } else {
                let Ok(SqlValue::String(value)) = cast_value_to_pg_type(value, "uuid") else {
                    return Ok(None);
                };
                Some((op.clone(), Some(value)))
            }
        } else {
            None
        };
        self.check_cancellation()?;
        let mut ordered = Vec::new();
        if limit != 0 && !matches!(bound, Some((_, None))) {
            let ids = match self.tx {
                Some(tx) => {
                    tx.scan_collection_record_key_candidates_with_prefix_unchecked(&collection, "")?
                }
                None => self
                    .db_ref()
                    .scan_collection_record_key_candidates_with_prefix_unchecked(&collection, "")?,
            };
            for (index, id) in ids.into_iter().enumerate() {
                if index % 1024 == 0 {
                    self.check_cancellation()?;
                }
                let Some(values) = primary_key_values_from_record_id(&schema, &keys, &id)? else {
                    return Ok(None);
                };
                let [SqlValue::String(key)] = values.as_slice() else {
                    return Ok(None);
                };
                let include = match &bound {
                    None => true,
                    Some((op, Some(value))) => match op {
                        BinaryOperator::Eq => key == value,
                        BinaryOperator::Gt => key > value,
                        BinaryOperator::GtEq => key >= value,
                        BinaryOperator::Lt => key < value,
                        BinaryOperator::LtEq => key <= value,
                        _ => unreachable!(),
                    },
                    Some((_, None)) => false,
                };
                if include {
                    ordered.push((key.clone(), id));
                }
            }
            ordered.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            if descending {
                ordered.reverse();
            }
        }
        self.check_cancellation()?;
        let mut records = Vec::with_capacity(limit.min(ordered.len()));
        for (_, id) in ordered {
            if records.len() == limit {
                break;
            }
            self.check_cancellation()?;
            // Resolve visibility before counting towards LIMIT. Deleted or
            // snapshot-invisible keys must not hide later visible rows.
            if let Some(record) = self.get_record_with_schema(&collection, &id, Some(&schema))? {
                records.push(record);
            }
        }
        let fields = row_fields_for_records(Some(&schema), &records);
        let columns = row_output_columns_from_fields(&collection, alias_name, &fields);
        let rows = records
            .iter()
            .map(|record| slot_row_from_record_fields(&collection, alias_name, &fields, record))
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(RowSet { rows, columns }))
    }
}
