//! Split out of the parent module to keep files digestible; behavior
//! unchanged — a separate `impl` block on the same type.
use super::*;
use crate::engine::predicates_locators::{
    nth_and_term, primary_key_term_plan_get, primary_key_term_plan_set,
};
#[allow(unused_imports)]
use crate::*;

impl<'db> SqlEngine<'db> {
    pub(crate) fn execute_set_query(&self, query: &Query) -> Result<SqlResult> {
        let mut result = self.execute_set_expr(query.body.as_ref())?;
        self.apply_result_order_by(
            &mut result,
            query.order_by.as_ref(),
            order_by_keep_bound(query)?,
        )?;
        apply_row_limit(&mut result.rows, query)?;
        Ok(result)
    }

    pub(crate) fn execute_set_expr(&self, expr: &SetExpr) -> Result<SqlResult> {
        match expr {
            SetExpr::Select(select) => {
                let query = query_from_body(SetExpr::Select(select.clone()));
                self.execute_query(&query)
            }
            SetExpr::Query(query) => self.execute_query(query),
            SetExpr::Values(values) => self.execute_values(values),
            SetExpr::SetOperation {
                left,
                op,
                set_quantifier,
                right,
            } => {
                let left = self.execute_set_expr(left)?;
                let right = self.execute_set_expr(right)?;
                let column_types = left
                    .column_types
                    .iter()
                    .zip(&right.column_types)
                    .map(|(left, right)| {
                        select_common_pg_type(
                            self.db_ref(),
                            &[left.clone(), right.clone()],
                            &op.to_string(),
                        )
                        .map(Some)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let mut result = combine_set_results(left, *op, *set_quantifier, right)?;
                result.column_types = column_types;
                Ok(result)
            }
            other => Err(SqlError::Unsupported(format!(
                "unsupported set query expression {other}"
            ))),
        }
    }

    pub(crate) fn execute_values(&self, values: &sqlparser::ast::Values) -> Result<SqlResult> {
        let width = values.rows.first().map(|row| row.len()).unwrap_or_default();
        let columns = (1..=width)
            .map(|index| format!("column{index}"))
            .collect::<Vec<_>>();
        let column_types = (0..width)
            .map(|column| {
                let types = values
                    .rows
                    .iter()
                    .map(|row| {
                        row.get(column)
                            .and_then(|expr| projected_expr_pg_type_with_db(self.db_ref(), expr))
                    })
                    .collect::<Vec<_>>();
                select_common_pg_type(self.db_ref(), &types, "VALUES").map(Some)
            })
            .collect::<Result<Vec<_>>>()?;
        let mut rows = Vec::with_capacity(values.rows.len());
        for (index, row) in values.rows.iter().enumerate() {
            if index % 1024 == 0 {
                self.check_cancellation()?;
            }
            if row.len() != width {
                return Err(SqlError::InvalidSql(
                    "VALUES lists must all be the same length".to_string(),
                ));
            }
            rows.push(
                row.iter()
                    .map(|expr| self.eval_select_constant_expr(expr))
                    .collect::<Result<Vec<_>>>()?,
            );
        }
        Ok(SqlResult::new(columns, rows).with_column_types(column_types))
    }

    /// `keep`: when `Some(k)`, only the k best-ranked rows are needed (a
    /// constant LIMIT+OFFSET follows) — the sort selects the top k in O(n)
    /// and ranks only those, instead of sorting the full input.
    pub(crate) fn apply_result_order_by(
        &self,
        result: &mut SqlResult,
        order_by: Option<&OrderBy>,
        keep: Option<usize>,
    ) -> Result<()> {
        self.check_cancellation()?;
        let Some(order_by) = order_by else {
            return Ok(());
        };
        let OrderByKind::Expressions(expressions) = &order_by.kind else {
            return Err(SqlError::Unsupported(
                "ORDER BY ALL is not supported".to_string(),
            ));
        };
        if expressions.is_empty() {
            return Ok(());
        }
        let mut rows = result
            .rows
            .drain(..)
            .map(|values| result_row_map(&result.columns, &values).map(|row| (row, values)))
            .collect::<Result<Vec<_>>>()?;
        let order_types = expressions
            .iter()
            .map(|order| {
                let column = match &order.expr {
                    Expr::Identifier(identifier) => Some(identifier.value.as_str()),
                    Expr::CompoundIdentifier(parts) => parts.last().map(|part| part.value.as_str()),
                    _ => None,
                };
                column
                    .and_then(|column| {
                        result
                            .columns
                            .iter()
                            .position(|candidate| candidate.eq_ignore_ascii_case(column))
                    })
                    .and_then(|index| result.column_types.get(index).cloned().flatten())
                    .or_else(|| projected_expr_pg_type(&order.expr, None))
            })
            .collect::<Vec<_>>();
        if let Some(pg_type) = order_types
            .iter()
            .find_map(|pg_type| type_without_comparison_operators(pg_type.as_deref()))
        {
            return Err(SqlError::undefined_function(format!(
                "could not identify an ordering operator for type {pg_type}"
            )));
        }
        let mut keyed_rows = rows
            .drain(..)
            .map(|(row, values)| {
                let keys = expressions
                    .iter()
                    .enumerate()
                    .map(|(index, order)| {
                        let pg_type = order_types.get(index).and_then(Option::as_deref);
                        let value = match self.eval_row_value(&row, &order.expr) {
                            Ok(value) => enforce_integer_value_type(value, pg_type)?,
                            Err(error @ SqlError::DataException { .. }) => return Err(error),
                            Err(_) => SqlValue::Null,
                        };
                        let typed_key = pg_type
                            .filter(|_| !matches!(value, SqlValue::Null))
                            .map(|pg_type| {
                                pg_typed_index_key_for_db(self.db_ref(), pg_type, &value)
                            })
                            .transpose()?;
                        Ok((value, typed_key))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok((keys, values))
            })
            .collect::<Result<Vec<_>>>()?;
        let compare =
            |(left, _): &(Vec<(SqlValue, Option<Vec<u8>>)>, Vec<SqlValue>),
             (right, _): &(Vec<(SqlValue, Option<Vec<u8>>)>, Vec<SqlValue>)| {
                expressions
                    .iter()
                    .zip(left.iter().zip(right))
                    .map(|(order, (left, right))| {
                        typed_order_expr_value_ordering(
                            &order.expr,
                            &left.0,
                            left.1.as_deref(),
                            &right.0,
                            right.1.as_deref(),
                            &order.options,
                        )
                    })
                    .find(|ordering| *ordering != Ordering::Equal)
                    .unwrap_or(Ordering::Equal)
            };
        match keep {
            // Top-K with original-index tiebreak: bit-identical to the stable
            // full sort + truncate it replaces (see apply_row_order_by).
            Some(keep) if keep < keyed_rows.len() => {
                if keep == 0 {
                    keyed_rows.clear();
                } else {
                    let mut indexed: Vec<(
                        usize,
                        (Vec<(SqlValue, Option<Vec<u8>>)>, Vec<SqlValue>),
                    )> = keyed_rows.into_iter().enumerate().collect();
                    let indexed_compare = |left: &(
                        usize,
                        (Vec<(SqlValue, Option<Vec<u8>>)>, Vec<SqlValue>),
                    ),
                                           right: &(
                        usize,
                        (Vec<(SqlValue, Option<Vec<u8>>)>, Vec<SqlValue>),
                    )| {
                        compare(&left.1, &right.1).then(left.0.cmp(&right.0))
                    };
                    indexed.select_nth_unstable_by(keep - 1, indexed_compare);
                    indexed.truncate(keep);
                    indexed.sort_by(indexed_compare);
                    keyed_rows = indexed.into_iter().map(|(_, entry)| entry).collect();
                }
            }
            _ => keyed_rows.sort_by(compare),
        }
        result.rows = keyed_rows.into_iter().map(|(_, values)| values).collect();
        self.check_cancellation()?;
        Ok(())
    }

    pub(crate) fn execute_query_with_ctes(&self, query: &Query, with: &With) -> Result<SqlResult> {
        if with.recursive {
            return self.execute_query_with_recursive_ctes(query, with);
        }
        match query.body.as_ref() {
            SetExpr::Insert(_) => {
                return Err(SqlError::Unsupported(
                    "CTEs feeding INSERT are not supported".to_string(),
                ));
            }
            SetExpr::Update(_) => {
                return Err(SqlError::Unsupported(
                    "CTEs feeding UPDATE are not supported".to_string(),
                ));
            }
            _ => {}
        }

        let mut ctes = self.ctes.clone();
        for cte in &with.cte_tables {
            let name = cte.alias.name.value.clone();
            let key = cte_key(&name);
            let engine = self
                .inherit_transaction(SqlEngine::with_ctes_and_context(
                    self.db_ref(),
                    self.settings,
                    ctes.clone(),
                    self.security_context.clone(),
                    self.session_gucs.clone(),
                ))
                .with_shared_routine_vars(self.routine_vars.clone())
                .with_cancellation(self.cancellation.clone());
            let result = engine.execute_query(&cte.query)?;
            let columns = cte_columns(&name, &cte.alias.columns, &result.columns)?;
            let column_types = result.column_types.clone();
            ctes.insert(
                key,
                CteResult::new(name, columns, result.rows).with_column_types(column_types),
            );
        }

        let mut body = query.clone();
        body.with = None;
        self.inherit_transaction(SqlEngine::with_ctes_and_context(
            self.db_ref(),
            self.settings,
            ctes,
            self.security_context.clone(),
            self.session_gucs.clone(),
        ))
        .with_shared_routine_vars(self.routine_vars.clone())
        .with_cancellation(self.cancellation.clone())
        .execute_query(&body)
    }

    pub(crate) fn execute_query_with_recursive_ctes(
        &self,
        query: &Query,
        with: &With,
    ) -> Result<SqlResult> {
        match query.body.as_ref() {
            SetExpr::Insert(_) => {
                return Err(SqlError::Unsupported(
                    "CTEs feeding INSERT are not supported".to_string(),
                ));
            }
            SetExpr::Update(_) => {
                return Err(SqlError::Unsupported(
                    "CTEs feeding UPDATE are not supported".to_string(),
                ));
            }
            _ => {}
        }

        let mut ctes = self.ctes.clone();
        for cte in &with.cte_tables {
            let result = self.materialize_recursive_cte(cte, &ctes)?;
            ctes.insert(cte_key(&result.name), result);
        }

        let mut body = query.clone();
        body.with = None;
        self.inherit_transaction(SqlEngine::with_ctes_and_context(
            self.db_ref(),
            self.settings,
            ctes,
            self.security_context.clone(),
            self.session_gucs.clone(),
        ))
        .with_shared_routine_vars(self.routine_vars.clone())
        .with_cancellation(self.cancellation.clone())
        .execute_query(&body)
    }

    pub(crate) fn materialize_recursive_cte(
        &self,
        cte: &sqlparser::ast::Cte,
        outer_ctes: &BTreeMap<String, CteResult>,
    ) -> Result<CteResult> {
        let name = cte.alias.name.value.clone();
        let key = cte_key(&name);
        let SetExpr::SetOperation {
            left,
            op: SetOperator::Union,
            set_quantifier,
            right,
        } = cte.query.body.as_ref()
        else {
            let engine = self
                .inherit_transaction(SqlEngine::with_ctes_and_context(
                    self.db_ref(),
                    self.settings,
                    outer_ctes.clone(),
                    self.security_context.clone(),
                    self.session_gucs.clone(),
                ))
                .with_shared_routine_vars(self.routine_vars.clone())
                .with_cancellation(self.cancellation.clone());
            let result = engine.execute_query(&cte.query)?;
            let columns = cte_columns(&name, &cte.alias.columns, &result.columns)?;
            return Ok(
                CteResult::new(name, columns, result.rows).with_column_types(result.column_types)
            );
        };
        match set_quantifier {
            SetQuantifier::None | SetQuantifier::Distinct | SetQuantifier::All => {}
            _ => {
                return Err(SqlError::Unsupported(format!(
                    "recursive UNION {set_quantifier} is not supported"
                )));
            }
        }

        let anchor_engine = self
            .inherit_transaction(SqlEngine::with_ctes_and_context(
                self.db_ref(),
                self.settings,
                outer_ctes.clone(),
                self.security_context.clone(),
                self.session_gucs.clone(),
            ))
            .with_shared_routine_vars(self.routine_vars.clone())
            .with_cancellation(self.cancellation.clone());
        let anchor = anchor_engine.execute_set_expr(left)?;
        let columns = cte_columns(&name, &cte.alias.columns, &anchor.columns)?;
        let column_types = anchor.column_types;
        let mut rows = anchor.rows;
        let mut working_rows = rows.clone();
        let distinct = !matches!(set_quantifier, SetQuantifier::All);

        for iteration in 0..MAX_RECURSIVE_CTE_ITERATIONS {
            self.check_cancellation()?;
            if working_rows.is_empty() {
                break;
            }
            let mut ctes = outer_ctes.clone();
            ctes.insert(
                key.clone(),
                CteResult::new(name.clone(), columns.clone(), working_rows.clone())
                    .with_column_types(column_types.clone()),
            );
            let recursive_engine = self
                .inherit_transaction(SqlEngine::with_ctes_and_context(
                    self.db_ref(),
                    self.settings,
                    ctes,
                    self.security_context.clone(),
                    self.session_gucs.clone(),
                ))
                .with_shared_routine_vars(self.routine_vars.clone())
                .with_cancellation(self.cancellation.clone());
            let recursive = recursive_engine.execute_set_expr(right)?;
            if recursive.columns.len() != columns.len() {
                return Err(SqlError::InvalidSql(format!(
                    "recursive CTE \"{name}\" returned {} columns, expected {}",
                    recursive.columns.len(),
                    columns.len()
                )));
            }
            let mut next_rows = recursive.rows;
            if distinct {
                let mut retained = Vec::new();
                for row in next_rows {
                    let mut duplicate = false;
                    for candidate in &rows {
                        if sql_rows_not_distinct_typed(candidate, &row, &column_types)? {
                            duplicate = true;
                            break;
                        }
                    }
                    if !duplicate {
                        retained.push(row);
                    }
                }
                next_rows = deduplicate_sql_rows_typed(retained, &column_types)?;
            }
            if next_rows.is_empty() {
                break;
            }
            rows.extend(next_rows.clone());
            working_rows = next_rows;
            if iteration + 1 == MAX_RECURSIVE_CTE_ITERATIONS {
                return Err(SqlError::Unsupported(format!(
                    "recursive CTE \"{name}\" exceeded {MAX_RECURSIVE_CTE_ITERATIONS} iterations"
                )));
            }
        }

        Ok(CteResult::new(name, columns, rows).with_column_types(column_types))
    }

    pub(crate) fn cte(&self, name: &str) -> Option<&CteResult> {
        self.ctes.get(&cte_key(name))
    }

    pub(crate) fn get_record(&self, collection: &str, id: &str) -> Result<Option<Arc<Record>>> {
        let schema = load_schema_shared(self.db_ref(), collection)?;
        self.get_record_with_schema(collection, id, schema.as_deref())
    }

    pub(crate) fn raw_record_cache_key(&self, collection: &str, id: &str) -> RecordLookupCacheKey {
        RecordLookupCacheKey {
            collection: collection.to_string(),
            id: id.to_string(),
            tx_write_len: self.tx.map(Transaction::write_len),
        }
    }

    pub(crate) fn get_raw_record_cached(
        &self,
        collection: &str,
        id: &str,
    ) -> Result<Option<Arc<Record>>> {
        let key = self.raw_record_cache_key(collection, id);
        if let Some(record) = self.record_lookup_cache.borrow().get(&key).cloned() {
            return Ok(record);
        }
        #[cfg(test)]
        SQL_RECORD_STORAGE_GET_CALLS.with(|calls| *calls.borrow_mut() += 1);
        let record = match self.security_context.as_ref() {
            Some(ctx) => match self.tx {
                Some(tx) => tx
                    .get_with_context(ctx, collection, id)
                    .map_err(SqlError::from),
                None => self
                    .db_ref()
                    .get_with_context(ctx, collection, id)
                    .map_err(SqlError::from),
            },
            None => match self.tx {
                Some(tx) => tx.get(collection, id).map_err(SqlError::from),
                None => self.db_ref().get(collection, id).map_err(SqlError::from),
            },
        }?;
        self.record_lookup_cache
            .borrow_mut()
            .insert(key, record.clone());
        Ok(record)
    }

    pub(crate) fn get_record_with_schema(
        &self,
        collection: &str,
        id: &str,
        schema: Option<&TableSchema>,
    ) -> Result<Option<Arc<Record>>> {
        let record = self.get_raw_record_cached(collection, id)?;
        let Some(record) = record else {
            return Ok(None);
        };
        if rls_allows_record_with_schema(self, collection, PolicyAction::Select, &record, schema)? {
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn scan_records(&self, collection: &str) -> Result<Vec<Arc<Record>>> {
        let records = match self.security_context.as_ref() {
            Some(ctx) => match self.tx {
                Some(tx) => tx
                    .scan_collection_with_context(ctx, collection)
                    .map_err(SqlError::from),
                None => self
                    .db_ref()
                    .scan_collection_with_context(ctx, collection)
                    .map_err(SqlError::from),
            },
            None => match self.tx {
                Some(tx) => tx.scan_collection(collection).map_err(SqlError::from),
                None => self
                    .db_ref()
                    .scan_collection(collection)
                    .map_err(SqlError::from),
            },
        }?;
        let records: Vec<Arc<Record>> = records.into_iter().map(Arc::new).collect();
        if self
            .security_context
            .as_ref()
            .is_some_and(|ctx| ctx.bypass_policy.is_some())
        {
            return Ok(records);
        }
        let schema = load_schema(self.db_ref(), collection)?;
        if schema.as_ref().is_none_or(|schema| !schema.rls_enabled) {
            return Ok(records);
        }
        let mut filtered = Vec::with_capacity(records.len());
        for record in records {
            if rls_allows_record_with_schema(
                self,
                collection,
                PolicyAction::Select,
                &record,
                schema.as_ref(),
            )? {
                filtered.push(record);
            }
        }
        Ok(filtered)
    }

    pub(crate) fn record_ids_with_pending_candidates(
        &self,
        collection: &str,
        ids: Rc<[String]>,
    ) -> Rc<[String]> {
        let Some(tx) = self.tx else {
            return ids;
        };
        let pending_ids = tx.pending_record_ids_for_collection(collection);
        if pending_ids.is_empty() {
            return ids;
        }
        #[cfg(test)]
        SQL_PENDING_ID_MERGES.with(|merges| *merges.borrow_mut() += 1);
        let mut merged = ids.iter().cloned().collect::<BTreeSet<String>>();
        merged.extend(pending_ids);
        Rc::from(merged.into_iter().collect::<Vec<String>>())
    }

    pub(crate) fn record_ids_with_filtered_pending_candidates<F>(
        &self,
        collection: &str,
        ids: Rc<[String]>,
        mut predicate: F,
    ) -> Result<Rc<[String]>>
    where
        F: FnMut(&Record) -> Result<bool>,
    {
        let Some(tx) = self.tx else {
            return Ok(ids);
        };
        let pending_ids = tx.pending_record_ids_for_collection(collection);
        if pending_ids.is_empty() {
            return Ok(ids);
        }
        #[cfg(test)]
        SQL_PENDING_ID_MERGES.with(|merges| *merges.borrow_mut() += 1);
        let mut merged = ids.iter().cloned().collect::<BTreeSet<String>>();
        for id in pending_ids {
            merged.remove(&id);
            if let Some(record) = tx
                .get_for_integrity_check(collection, &id)
                .map_err(SqlError::from)?
            {
                if predicate(&record)? {
                    merged.insert(id);
                }
            }
        }
        Ok(Rc::from(merged.into_iter().collect::<Vec<String>>()))
    }

    pub(crate) fn record_ids_with_pending_index_lookup_candidates(
        &self,
        collection: &str,
        index: &IndexDefinition,
        prefix: &[IndexValue],
        ids: Rc<[String]>,
    ) -> Result<Rc<[String]>> {
        self.record_ids_with_filtered_pending_candidates(collection, ids, |record| {
            let key = record_index_key_from_sql_record(record, &index.fields)?;
            Ok(key.starts_with(prefix))
        })
    }

    pub(crate) fn lookup_index_rowids_cached(
        &self,
        index_name: &str,
        prefix: &[IndexValue],
    ) -> Result<Rc<[RowId]>> {
        if let Some(rowids) = self
            .rowid_index_lookup_cache
            .borrow()
            .get(index_name)
            .and_then(|per_index| per_index.get(prefix))
            .cloned()
        {
            return Ok(rowids);
        }
        #[cfg(test)]
        SQL_INDEX_STORAGE_LOOKUP_CALLS.with(|calls| *calls.borrow_mut() += 1);
        let rowids: Rc<[RowId]> = Rc::from(self.db_ref().lookup_index_rowids(index_name, prefix)?);
        self.rowid_index_lookup_cache
            .borrow_mut()
            .entry(index_name.to_string())
            .or_default()
            .insert(prefix.to_vec(), Rc::clone(&rowids));
        Ok(rowids)
    }

    pub(crate) fn lookup_index_cached(
        &self,
        index_name: &str,
        prefix: &[IndexValue],
    ) -> Result<Rc<[String]>> {
        if let Some(ids) = self
            .index_lookup_cache
            .borrow()
            .get(index_name)
            .and_then(|per_index| per_index.get(prefix))
            .cloned()
        {
            return Ok(ids);
        }
        #[cfg(test)]
        SQL_INDEX_STORAGE_LOOKUP_CALLS.with(|calls| *calls.borrow_mut() += 1);
        let ids: Rc<[String]> = Rc::from(self.db_ref().lookup_index(index_name, prefix)?);
        self.index_lookup_cache
            .borrow_mut()
            .entry(index_name.to_string())
            .or_default()
            .insert(prefix.to_vec(), Rc::clone(&ids));
        Ok(ids)
    }

    pub(crate) fn record_ids_with_pending_index_range_candidates(
        &self,
        collection: &str,
        index: &IndexDefinition,
        prefix: &[IndexValue],
        lower: Option<&IndexValue>,
        upper: Option<&IndexValue>,
        filters: &[(usize, IndexValue)],
        ids: Rc<[String]>,
    ) -> Result<Rc<[String]>> {
        self.record_ids_with_filtered_pending_candidates(collection, ids, |record| {
            let key = record_index_key_from_sql_record(record, &index.fields)?;
            Ok(index_key_matches_prefix_range_and_filters(
                &key, prefix, lower, upper, filters,
            ))
        })
    }

    pub(crate) fn index_definition_for_scan(
        &self,
        collection: &str,
        index_name: &str,
    ) -> Option<IndexDefinition> {
        sql_index_definitions_for_collection(self.db_ref(), collection)
            .into_iter()
            .find(|index| index.name == index_name || index.name.eq_ignore_ascii_case(index_name))
    }

    pub(crate) fn scan_record_ids_with_prefix(
        &self,
        collection: &str,
        prefix: &str,
    ) -> Result<Vec<String>> {
        match (self.security_context.as_ref(), self.tx) {
            (None, Some(tx)) => tx
                .scan_collection_record_ids_with_prefix_unchecked(collection, prefix)
                .map_err(SqlError::from),
            _ => self
                .db_ref()
                .scan_collection_record_ids_with_prefix_unchecked(collection, prefix)
                .map_err(SqlError::from),
        }
    }

    pub(crate) fn load_ann_records_if_enabled(
        &self,
        collection: &str,
        select: &Select,
        query: &Query,
    ) -> Result<Option<Vec<Record>>> {
        if self.settings.vector_search != VectorSearchMode::Ann
            || select.selection.is_some()
            || has_aggregates(&select.projection)
            || !self.db_ref().has_vector_index(collection)
        {
            return Ok(None);
        }
        let Some((vector_order, limit)) = ann_vector_order(query)? else {
            return Ok(None);
        };
        let Some(metric) = vector_order.metric.vector_metric() else {
            return Ok(None);
        };
        if self.security_context.is_some() {
            return Ok(None);
        }
        if load_schema(self.db_ref(), collection)?.is_some_and(|schema| schema.rls_enabled) {
            return Ok(None);
        }
        self.check_cancellation()?;
        let results = self.db_ref().search_vector_ann_with_metric_cancellable(
            collection,
            &vector_order.query,
            limit,
            self.settings.ef_search,
            metric,
            &self.cancellation,
        )?;
        self.check_cancellation()?;
        let records = results
            .into_iter()
            .map(|result| result.record)
            .collect::<Vec<_>>();
        sql_profile_index_lookup();
        sql_profile_records_materialized(&records);
        Ok(Some(records))
    }

    pub(crate) fn try_single_table_indexed_extreme_aggregate(
        &self,
        from: &TableWithJoins,
        select: &Select,
    ) -> Result<Option<SqlResult>> {
        let TableFactor::Table {
            name,
            alias,
            args: None,
            ..
        } = &from.relation
        else {
            return Ok(None);
        };
        let collection = relation_name(name)?;
        if self.cte(&collection).is_some()
            || is_virtual_table(&collection)
            || load_view(self.db_ref(), &collection)?.is_some()
            || load_sequence(self.db_ref(), &collection)?.is_some()
        {
            return Ok(None);
        }
        let alias_name = alias
            .as_ref()
            .map(|alias| alias.name.value.clone())
            .unwrap_or_else(|| {
                collection
                    .rsplit('.')
                    .next()
                    .unwrap_or(&collection)
                    .to_string()
            });
        let collection = resolve_session_relation_name(self.db_ref(), &collection)?;
        let schema = load_schema_shared(self.db_ref(), &collection)?;
        self.execute_indexed_extreme_aggregate(&collection, &alias_name, select, schema.as_deref())
    }

    pub(crate) fn execute_indexed_extreme_aggregate(
        &self,
        collection: &str,
        alias: &str,
        select: &Select,
        schema: Option<&TableSchema>,
    ) -> Result<Option<SqlResult>> {
        let Some(schema) = schema else {
            return Ok(None);
        };
        if self.security_context.is_some() || schema.rls_enabled {
            return Ok(None);
        }
        let [item] = select.projection.as_slice() else {
            return Ok(None);
        };
        let (expr, alias_name) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            _ => return Ok(None),
        };
        let Ok((aggregate, cast)) = aggregate_from_expr(expr, Some(schema)) else {
            return Ok(None);
        };
        if cast.is_some() {
            return Ok(None);
        }
        let (field, greatest) = match &aggregate {
            Aggregate::Min(field, _) => (field, false),
            Aggregate::Max(field, _) => (field, true),
            _ => return Ok(None),
        };
        let Some(column) = primary_key_column_for_field(schema, collection, alias, field) else {
            return Ok(None);
        };
        let primary_key_columns = primary_key_columns_for_schema(schema);
        let Some(extreme_idx) = primary_key_columns
            .iter()
            .position(|candidate| candidate.eq_ignore_ascii_case(&column))
        else {
            return Ok(None);
        };
        let Some(bindings) = self.primary_key_equality_bindings(
            select.selection.as_ref(),
            collection,
            alias,
            schema,
            &primary_key_columns,
        )?
        else {
            return Ok(None);
        };
        if bindings
            .iter()
            .take(extreme_idx)
            .any(|binding| binding.is_none())
        {
            return Ok(None);
        }
        if bindings[extreme_idx].is_some() {
            return Ok(None);
        }

        // This fast path reads real data, so it needs the same SELECT
        // privilege every other read path takes. It sits BEFORE the check
        // at the ordinary dispatch site, so without this an unprivileged
        // role could read min/max of any table's primary key — and with a
        // composite key, bind the leading columns and use it as an
        // existence and range oracle over another tenant's records. The
        // sibling call site of this same function is already after the
        // gate; only this one was not.
        self.require_relation_privilege(collection, "SELECT")?;

        let prefix_values = bindings
            .iter()
            .take(extreme_idx)
            .flatten()
            .cloned()
            .collect::<Vec<_>>();
        let committed_primary_key_extreme_is_safe = self.tx.is_none_or(|tx| tx.write_len() == 0);
        if committed_primary_key_extreme_is_safe && !primary_key_requires_typed_identity(schema) {
            if let Some(index) = executable_primary_key_index_for_schema(self.db_ref(), schema) {
                let prefix = prefix_values
                    .iter()
                    .cloned()
                    .map(index_value_from_sql)
                    .collect::<Result<Vec<_>>>()?;
                let filters = bindings
                    .iter()
                    .enumerate()
                    .skip(extreme_idx + 1)
                    .filter_map(|(idx, value)| value.clone().map(|value| (idx, value)))
                    .map(|(idx, value)| Ok((idx, index_value_from_sql(value)?)))
                    .collect::<Result<Vec<_>>>()?;
                sql_profile_index_lookup();
                if let Some(best) = self
                    .db_ref()
                    .lookup_index_extreme_with_filters(
                        &index.name,
                        prefix.as_slice(),
                        greatest,
                        filters.as_slice(),
                    )?
                    .and_then(|(key, _)| key.get(extreme_idx).map(sql_value_from_index_value))
                {
                    if !matches!(best, SqlValue::Null) {
                        let column_name = alias_name.unwrap_or_else(|| aggregate.column_name());
                        let column_types = vec![aggregate_result_pg_type(&aggregate, Some(schema))];
                        return Ok(Some(
                            SqlResult::new(vec![column_name], vec![vec![best]])
                                .with_column_types(column_types),
                        ));
                    }
                }
            }
        }
        sql_profile_index_lookup();
        let id_prefix = if prefix_values.is_empty() {
            String::new()
        } else {
            composite_record_id_prefix_from_values(schema, &primary_key_columns, &prefix_values)?
        };
        let ids =
            self.primary_key_prefix_record_ids(collection, schema, &prefix_values, &id_prefix)?;
        let mut best = SqlValue::Null;
        for id in ids.iter() {
            let Some(values) = primary_key_values_from_record_id(schema, &primary_key_columns, id)?
            else {
                return Ok(None);
            };
            if !primary_key_values_match_bindings(&values, &bindings) {
                continue;
            }
            let value = values.get(extreme_idx).cloned().unwrap_or(SqlValue::Null);
            if matches!(value, SqlValue::Null) {
                continue;
            }
            if matches!(best, SqlValue::Null)
                || value_ordering(&value, &best).is_some_and(|ordering| {
                    if greatest {
                        ordering == Ordering::Greater
                    } else {
                        ordering == Ordering::Less
                    }
                })
            {
                best = value;
            }
        }
        let column_name = alias_name.unwrap_or_else(|| aggregate.column_name());
        let column_types = vec![aggregate_result_pg_type(&aggregate, Some(schema))];
        Ok(Some(
            SqlResult::new(vec![column_name], vec![vec![best]]).with_column_types(column_types),
        ))
    }

    pub(crate) fn primary_key_equality_bindings(
        &self,
        selection: Option<&Expr>,
        table: &str,
        alias: &str,
        schema: &TableSchema,
        primary_key_columns: &[String],
    ) -> Result<Option<Vec<Option<SqlValue>>>> {
        let mut bindings = vec![None; primary_key_columns.len()];
        let Some(selection) = selection else {
            return Ok(Some(bindings));
        };
        for term in and_terms(selection) {
            let Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } = term
            else {
                return Ok(None);
            };
            let binding = if let Some(idx) =
                self.primary_key_expr_index(left, table, alias, Some(schema), primary_key_columns)?
            {
                if self.expr_references_table(right, table, alias, Some(schema))? {
                    return Ok(None);
                }
                Some((idx, self.eval_dynamic_bound_expr(right)?))
            } else if let Some(idx) =
                self.primary_key_expr_index(right, table, alias, Some(schema), primary_key_columns)?
            {
                if self.expr_references_table(left, table, alias, Some(schema))? {
                    return Ok(None);
                }
                Some((idx, self.eval_dynamic_bound_expr(left)?))
            } else {
                None
            };
            let Some((idx, value)) = binding else {
                return Ok(None);
            };
            if bindings[idx]
                .as_ref()
                .is_some_and(|existing| !values_equal(existing, &value))
            {
                return Ok(None);
            }
            bindings[idx] = Some(value);
        }
        Ok(Some(bindings))
    }

    pub(crate) fn primary_key_prefix_record_ids(
        &self,
        table: &str,
        schema: &TableSchema,
        prefix_values: &[SqlValue],
        record_id_prefix: &str,
    ) -> Result<Rc<[String]>> {
        if self.security_context.is_none() && !primary_key_requires_typed_identity(schema) {
            if let Some(index) = executable_primary_key_index_for_schema(self.db_ref(), schema) {
                let prefix = prefix_values
                    .iter()
                    .cloned()
                    .map(index_value_from_sql)
                    .collect::<Result<Vec<_>>>()?;
                if !prefix.is_empty() {
                    let ids = self
                        .db_ref()
                        .lookup_index(&index.name, prefix.as_slice())
                        .map_err(SqlError::from)?;
                    return self.record_ids_with_pending_index_lookup_candidates(
                        table,
                        &index,
                        prefix.as_slice(),
                        Rc::from(ids),
                    );
                }
            }
        }
        sql_profile_record_id_prefix_scan();
        let typed_prefix = primary_key_prefix_requires_typed_identity(schema, prefix_values.len());
        let physical_prefix = if typed_prefix { "" } else { record_id_prefix };
        let ids = self.scan_record_ids_with_prefix(table, physical_prefix)?;
        if !typed_prefix {
            return Ok(Rc::from(ids));
        }

        let primary_key_columns = primary_key_columns_for_schema(schema);
        let prefix_columns = &primary_key_columns[..prefix_values.len()];
        let mut matched = Vec::new();
        for id in ids {
            // Locator planning: the ids that match resolve to rows only
            // through the policy-filtered record paths.
            let Some(record) = self.db_ref().get_unchecked(table, &id)? else {
                continue;
            };
            let values = record_column_values(&record, schema, prefix_columns);
            if typed_column_values_not_distinct(schema, prefix_columns, &values, prefix_values)? {
                matched.push(id);
            }
        }
        Ok(Rc::from(matched))
    }

    pub(crate) fn primary_key_expr_index(
        &self,
        expr: &Expr,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        primary_key_columns: &[String],
    ) -> Result<Option<usize>> {
        let Some(Expr::Identifier(ident)) = normalize_table_field_expr(expr, table, alias, schema)
        else {
            return Ok(None);
        };
        Ok(primary_key_columns
            .iter()
            .position(|column| column.eq_ignore_ascii_case(&ident.value)))
    }

    /// Require SELECT on every base table an EXPLAIN'd query references, so
    /// EXPLAIN (and EXPLAIN ANALYZE) cannot leak plan shape, row estimates, or
    /// actual counts for a relation the role has no privilege to read.
    pub(crate) fn require_explained_relations_readable(&self, select: &Select) -> Result<()> {
        for from in &select.from {
            for factor in
                std::iter::once(&from.relation).chain(from.joins.iter().map(|join| &join.relation))
            {
                if let TableFactor::Table { name, .. } = factor {
                    let collection = relation_name(name)?;
                    if !is_virtual_table(&collection) {
                        self.require_relation_privilege(&collection, "SELECT")?;
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn execute_explain(
        &self,
        statement: &Statement,
        analyze: bool,
    ) -> Result<SqlResult> {
        let Statement::Query(query) = statement else {
            return Err(SqlError::Unsupported(
                "EXPLAIN supports only SELECT statements".to_string(),
            ));
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            return Err(SqlError::Unsupported(
                "EXPLAIN supports only simple SELECT statements".to_string(),
            ));
        };
        // EXPLAIN requires the underlying statement's privileges, exactly as
        // PostgreSQL does. Without this, the plan shape and `EstimatedRows`
        // are an existence/cardinality oracle on a table the role cannot read,
        // and `EXPLAIN ANALYZE` goes further — it executes the query and
        // reports `ActualRows`, a real matching-row count. Gate every base
        // table in the FROM (relation and joins) up front; virtual/system
        // tables carry their own gates and are checked when scanned.
        self.require_explained_relations_readable(select)?;
        if select.from.is_empty() {
            return Ok(SqlResult::new(
                vec!["QUERY PLAN".to_string()],
                vec![vec![SqlValue::String("Result".to_string())]],
            ));
        }
        if select.from.len() != 1 {
            return Err(SqlError::Unsupported(
                "EXPLAIN supports one FROM collection".to_string(),
            ));
        }
        if !select.from[0].joins.is_empty() {
            let lines = self.explain_row_join(select, analyze)?;
            return Ok(SqlResult::new(
                vec!["QUERY PLAN".to_string()],
                lines
                    .into_iter()
                    .map(|line| vec![SqlValue::String(line)])
                    .collect(),
            ));
        }
        let TableFactor::Table { name, alias, .. } = &select.from[0].relation else {
            return Err(SqlError::Unsupported(
                "EXPLAIN supports table SELECT statements".to_string(),
            ));
        };
        let collection = relation_name(name)?;
        let alias_name = alias
            .as_ref()
            .map(|alias| alias.name.value.clone())
            .unwrap_or_else(|| {
                collection
                    .rsplit('.')
                    .next()
                    .unwrap_or(&collection)
                    .to_string()
            });
        let plan = if is_virtual_table(&collection) {
            QueryPlan::full_scan()
        } else {
            self.plan_query(&collection, &alias_name, select, query)?
        };
        let mut lines = plan.explain_lines();
        if analyze {
            let started = Instant::now();
            let mut records = self.load_records_for_plan(&collection, &plan)?;
            if let Some(selection) = &select.selection {
                let schema = load_schema(self.db_ref(), &collection)?;
                let predicate_env = self.from_relation_columns(&select.from).unwrap_or_default();
                let mut filtered = Vec::new();
                for (idx, record) in records.into_iter().enumerate() {
                    if idx % 1024 == 0 {
                        self.check_cancellation()?;
                    }
                    let row = row_from_record(&collection, &collection, schema.as_ref(), &record)?;
                    if self
                        .eval_row_truth_typed(&row, selection, &predicate_env)?
                        .unwrap_or(false)
                    {
                        filtered.push(record);
                    }
                }
                records = filtered;
            }
            lines.push(format!("ActualRows {}", records.len()));
            lines.push(format!(
                "ActualTimeMs {:.3}",
                started.elapsed().as_secs_f64() * 1000.0
            ));
        }
        Ok(SqlResult::new(
            vec!["QUERY PLAN".to_string()],
            lines
                .into_iter()
                .map(|line| vec![SqlValue::String(line)])
                .collect(),
        ))
    }

    pub(crate) fn explain_row_join(&self, select: &Select, analyze: bool) -> Result<Vec<String>> {
        let (relation, joins) =
            self.plan_row_join_order(&select.from[0], select.selection.as_ref())?;
        let mut order = vec![table_factor_explain_name(&relation)?];
        order.extend(
            joins
                .iter()
                .map(|join| table_factor_explain_name(&join.relation))
                .collect::<Result<Vec<_>>>()?,
        );
        let estimated_rows = order
            .iter()
            .filter_map(|name| {
                self.db_ref()
                    .table_statistics(name)
                    .map(|stats| stats.row_count)
            })
            .min()
            .unwrap_or(0);
        let mut lines = vec![
            "NestedLoopJoin".to_string(),
            format!("JoinOrder {}", order.join(" -> ")),
            format!("EstimatedRows {estimated_rows}"),
            format!(
                "Cost {:.3}",
                estimated_rows as f64 + order.len() as f64 * 20.0
            ),
        ];
        if analyze {
            let started = Instant::now();
            let row_set = self.row_set_from_table_with_joins(&select.from[0])?;
            lines.push(format!("ActualRows {}", row_set.rows.len()));
            lines.push(format!(
                "ActualTimeMs {:.3}",
                started.elapsed().as_secs_f64() * 1000.0
            ));
        }
        Ok(lines)
    }

    pub(crate) fn plan_query(
        &self,
        collection: &str,
        alias: &str,
        select: &Select,
        query: &Query,
    ) -> Result<QueryPlan> {
        let stats = self.db_ref().table_statistics(collection);
        // Row-count estimate for costing. Without ANALYZE stats (the common case —
        // e.g. some external workloads never run ANALYZE), fall back to the collection's cheap
        // O(shards) live record count, NOT a full table scan: the old
        // `scan_records().len()` materialized every row on EVERY query, so query
        // cost grew linearly with table size — an in-run decay on every SELECT
        // against a growing table (orders/order_line/history). Only if even the
        // cheap count is unavailable (e.g. a virtual/system table) do we scan.
        let total_rows = stats.map(|stats| stats.row_count).unwrap_or_else(|| {
            self.db_ref()
                .estimated_record_count(collection)
                .or_else(|_| self.scan_records(collection).map(|records| records.len()))
                .unwrap_or(0)
        });
        let schema = load_schema(self.db_ref(), collection)?;
        let full_scan_rows = match (&select.selection, schema.as_ref(), stats) {
            (Some(selection), Some(schema), Some(stats)) => self
                .estimate_range_predicate_rows(collection, alias, schema, selection, stats)?
                .unwrap_or(total_rows),
            (Some(selection), Some(schema), None) => self
                .estimate_network_default_predicate_rows(
                    collection, alias, schema, selection, total_rows,
                )?
                .unwrap_or(total_rows),
            _ => total_rows,
        };
        let mut full_scan = QueryPlan::full_scan_with_estimate(full_scan_rows);
        full_scan.estimated_cost = total_rows as f64;
        let mut candidates = vec![full_scan];

        if let Some(selection) = &select.selection {
            if let Some(IndexValue::String(record_id)) = equality_value(selection, &IndexField::Id)?
            {
                candidates.push(QueryPlan {
                    kind: PlanKind::PrimaryKeyLookup { record_id },
                    estimated_rows: 1,
                    estimated_cost: 0.5,
                });
            }
            if let Some(schema) = schema.as_ref() {
                if let Some(candidate) =
                    self.primary_key_in_list_candidate(collection, alias, schema, selection)?
                {
                    candidates.push(candidate);
                }
            }
            if let Some(schema) = schema.as_ref() {
                if let Some(candidate) = self.primary_key_filter_candidate(
                    collection, alias, schema, selection, total_rows,
                )? {
                    candidates.push(candidate);
                }
            }
        }

        let unusable_primary_key_index = schema
            .as_ref()
            .filter(|schema| primary_key_requires_typed_identity(schema))
            .map(TableSchema::primary_key_constraint_name);
        let indexes = sql_index_definitions_for_collection(self.db_ref(), collection)
            .into_iter()
            .filter(|index| {
                unusable_primary_key_index
                    .as_ref()
                    .is_none_or(|name| !index.name.eq_ignore_ascii_case(name))
            })
            .collect::<Vec<_>>();

        if let Some(selection) = &select.selection {
            self.add_index_filter_candidates(
                collection,
                selection,
                &indexes,
                stats,
                total_rows,
                &mut candidates,
            )?;
            self.add_spatial_index_candidates(selection, &indexes, &mut candidates)?;
            if let Some(schema) = schema.as_ref() {
                self.add_geometric_index_candidates(
                    collection,
                    alias,
                    schema,
                    selection,
                    &indexes,
                    &mut candidates,
                )?;
            }
            if self.tx.is_none_or(|tx| tx.write_len() == 0) {
                if let Some((index_name, candidate)) =
                    self.array_index_candidate(collection, alias, schema.as_ref(), selection)?
                {
                    let estimated_rows = self.array_candidate_ids(&index_name, &candidate)?.len();
                    candidates.push(QueryPlan {
                        kind: PlanKind::ArrayIndexScan {
                            index_name,
                            candidate,
                        },
                        estimated_rows,
                        estimated_cost: 1.0 + estimated_rows as f64 * 0.05,
                    });
                }
                if let Some((index_name, candidate)) =
                    self.jsonb_index_candidate(collection, alias, schema.as_ref(), selection)?
                {
                    let estimated_rows = self.jsonb_candidate_ids(&index_name, &candidate)?.len();
                    candidates.push(QueryPlan {
                        kind: PlanKind::JsonbIndexScan {
                            index_name,
                            candidate,
                        },
                        estimated_rows,
                        estimated_cost: 1.0 + estimated_rows as f64 * 0.05,
                    });
                }
                if let Some((index_name, candidate)) =
                    self.full_text_index_candidate(collection, alias, schema.as_ref(), selection)?
                {
                    let ids =
                        std::rc::Rc::new(self.full_text_candidate_ids(&index_name, &candidate)?);
                    let estimated_rows = ids.len();
                    candidates.push(QueryPlan {
                        kind: PlanKind::FullTextIndexScan { index_name, ids },
                        estimated_rows,
                        estimated_cost: 1.0 + estimated_rows as f64 * 0.05,
                    });
                }
            }
        }

        if let Some(schema) = schema.as_ref() {
            self.add_geometric_knn_index_candidate(
                collection,
                alias,
                schema,
                select,
                query,
                &indexes,
                total_rows,
                &mut candidates,
            )?;
        }

        let ordered_index_is_transaction_safe = self.tx.is_none_or(|tx| tx.write_len() == 0);
        if ordered_index_is_transaction_safe {
            if let Some((field, descending)) = first_order_field(query)? {
                if let Some(index) = indexes.iter().find(|index| {
                    index.kind == IndexKind::BTree
                        && index.fields.len() == 1
                        && index_field_matches(&index.fields[0], &field)
                        && schema
                            .as_ref()
                            .is_none_or(|schema| index_supports_ordering(schema, &index.name))
                }) {
                    let limit = limit_for_plan(query)?;
                    let estimated_rows = limit.unwrap_or(total_rows).min(total_rows);
                    candidates.push(QueryPlan {
                        kind: PlanKind::OrderedIndexScan {
                            index_name: index.name.clone(),
                            descending,
                            limit,
                        },
                        estimated_rows,
                        estimated_cost: 1.0 + estimated_rows as f64 * 0.25,
                    });
                }
            }
            if let Some(selection) = &select.selection {
                let ordered_indexes = indexes
                    .iter()
                    .filter(|index| {
                        index.kind == IndexKind::BTree
                            && schema
                                .as_ref()
                                .is_none_or(|schema| index_supports_ordering(schema, &index.name))
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                self.add_prefix_ordered_index_candidate(
                    selection,
                    query,
                    &ordered_indexes,
                    &mut candidates,
                )?;
            }
        }

        candidates.sort_by(|left, right| {
            left.estimated_cost
                .partial_cmp(&right.estimated_cost)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.estimated_rows.cmp(&right.estimated_rows))
        });
        Ok(candidates
            .into_iter()
            .next()
            .unwrap_or_else(QueryPlan::full_scan))
    }

    pub(crate) fn estimate_range_predicate_rows(
        &self,
        table: &str,
        alias: &str,
        schema: &TableSchema,
        selection: &Expr,
        stats: &TableStatistics,
    ) -> Result<Option<usize>> {
        let mut estimate = None::<usize>;
        for term in and_terms(selection) {
            let Expr::BinaryOp { left, op, right } = term else {
                continue;
            };
            if matches!(
                op.to_string().as_str(),
                "=" | "<>" | "<" | "<=" | ">" | ">=" | "<<" | "<<=" | ">>" | ">>=" | "&&"
            ) {
                for column in schema
                    .columns
                    .iter()
                    .filter(|column| matches!(column.pg_type.as_str(), "inet" | "cidr"))
                {
                    let field = IndexField::MetadataPath(vec![column.name.clone()]);
                    let Some(column_stats) = table_column_stats(stats, &field) else {
                        continue;
                    };
                    let Some(network_stats) = column_stats.network.as_ref() else {
                        continue;
                    };
                    if network_stats.samples.is_empty() {
                        estimate = Some(0);
                        continue;
                    }
                    let (bound, effective_op) = if self.expr_matches_table_column(
                        left,
                        table,
                        alias,
                        Some(schema),
                        &column.name,
                    )? {
                        (right.as_ref(), op.clone())
                    } else if self.expr_matches_table_column(
                        right,
                        table,
                        alias,
                        Some(schema),
                        &column.name,
                    )? {
                        let Some(reversed) = reverse_network_statistics_operator(op) else {
                            continue;
                        };
                        (left.as_ref(), reversed)
                    } else {
                        continue;
                    };
                    if self.expr_references_table(bound, table, alias, Some(schema))?
                        || !self.expr_is_bound_without_table_row(bound)?
                    {
                        continue;
                    }
                    let bound_value = self.eval_dynamic_bound_expr(bound)?;
                    let bound_type = projected_expr_pg_type(bound, Some(schema))
                        .unwrap_or_else(|| column.pg_type.clone());
                    if let Some(selectivity) = network_histogram_selectivity(
                        &network_stats.samples,
                        &bound_value,
                        &effective_op,
                        &column.pg_type,
                        &bound_type,
                    )? {
                        let non_null = column_stats
                            .row_count
                            .saturating_sub(column_stats.null_count);
                        let rows = (selectivity * non_null as f64).round() as usize;
                        estimate = Some(estimate.map_or(rows, |current| current.min(rows)));
                        continue;
                    }
                    let mut matched = 0usize;
                    for sample in &network_stats.samples {
                        let sample = SqlValue::String(sample.clone());
                        let matches = if let Some(value) = eval_network_binary_value(
                            &sample,
                            &effective_op,
                            &bound_value,
                            Some(&column.pg_type),
                            Some(&bound_type),
                        )? {
                            matches!(value, SqlValue::Bool(true))
                        } else {
                            let ordering = pg_typed_compare("inet", &sample, &bound_value)?;
                            comparison_from_ordering(&effective_op, ordering)
                        };
                        matched += usize::from(matches);
                    }
                    let non_null = column_stats
                        .row_count
                        .saturating_sub(column_stats.null_count);
                    let sampled = network_stats.samples.len();
                    let rows = matched
                        .saturating_mul(non_null)
                        .saturating_add(sampled / 2)
                        .saturating_div(sampled);
                    estimate = Some(estimate.map_or(rows, |current| current.min(rows)));
                }
            }
            if !matches!(
                op,
                BinaryOperator::Eq
                    | BinaryOperator::NotEq
                    | BinaryOperator::AtArrow
                    | BinaryOperator::ArrowAt
                    | BinaryOperator::PGOverlap
                    | BinaryOperator::PGBitwiseShiftLeft
                    | BinaryOperator::PGBitwiseShiftRight
                    | BinaryOperator::AndLt
                    | BinaryOperator::AndGt
            ) && !matches!(op, BinaryOperator::Custom(operator) if operator == "-|-" )
            {
                continue;
            }
            for column in schema.columns.iter().filter(|column| {
                is_builtin_range_type(&column.pg_type)
                    || is_builtin_multirange_type(&column.pg_type)
            }) {
                let field = IndexField::MetadataPath(vec![column.name.clone()]);
                let Some(column_stats) = table_column_stats(stats, &field) else {
                    continue;
                };
                let Some(range_stats) = column_stats.range.as_ref() else {
                    continue;
                };
                if range_stats.samples.is_empty() {
                    estimate = Some(0);
                    continue;
                }
                let (bound, effective_op) = if self.expr_matches_table_column(
                    left,
                    table,
                    alias,
                    Some(schema),
                    &column.name,
                )? {
                    (right.as_ref(), op.clone())
                } else if self.expr_matches_table_column(
                    right,
                    table,
                    alias,
                    Some(schema),
                    &column.name,
                )? {
                    let Some(reversed) = reverse_range_statistics_operator(op) else {
                        continue;
                    };
                    (left.as_ref(), reversed)
                } else {
                    continue;
                };
                if self.expr_references_table(bound, table, alias, Some(schema))?
                    || !self.expr_is_bound_without_table_row(bound)?
                {
                    continue;
                }
                let bound_value = self.eval_dynamic_bound_expr(bound)?;
                let bound_type = projected_expr_pg_type(bound, Some(schema));
                let mut matched = 0usize;
                for sample in &range_stats.samples {
                    if matches!(
                        eval_range_binary_value_with_db(
                            self.db_ref(),
                            SqlValue::String(sample.clone()),
                            &effective_op,
                            bound_value.clone(),
                            Some(&column.pg_type),
                            bound_type.as_deref(),
                        )?,
                        Some(SqlValue::Bool(true))
                    ) {
                        matched += 1;
                    }
                }
                let non_null = column_stats
                    .row_count
                    .saturating_sub(column_stats.null_count);
                let sampled = range_stats.samples.len();
                let rows = matched
                    .saturating_mul(non_null)
                    .saturating_add(sampled / 2)
                    .saturating_div(sampled);
                estimate = Some(estimate.map_or(rows, |current| current.min(rows)));
            }
        }
        Ok(estimate)
    }

    pub(crate) fn estimate_network_default_predicate_rows(
        &self,
        table: &str,
        alias: &str,
        schema: &TableSchema,
        selection: &Expr,
        total_rows: usize,
    ) -> Result<Option<usize>> {
        let mut estimate = None;
        for term in and_terms(selection) {
            let Expr::BinaryOp { left, op, right } = term else {
                continue;
            };
            let Some(selectivity) = network_default_selectivity(op) else {
                continue;
            };
            for column in schema
                .columns
                .iter()
                .filter(|column| matches!(column.pg_type.as_str(), "inet" | "cidr"))
            {
                let bound = if self.expr_matches_table_column(
                    left,
                    table,
                    alias,
                    Some(schema),
                    &column.name,
                )? {
                    right.as_ref()
                } else if self.expr_matches_table_column(
                    right,
                    table,
                    alias,
                    Some(schema),
                    &column.name,
                )? {
                    left.as_ref()
                } else {
                    continue;
                };
                if self.expr_references_table(bound, table, alias, Some(schema))?
                    || !self.expr_is_bound_without_table_row(bound)?
                {
                    continue;
                }
                let rows = ((total_rows as f64 * selectivity).round() as usize).max(1);
                estimate = Some(estimate.map_or(rows, |current: usize| current.min(rows)));
            }
        }
        Ok(estimate)
    }

    pub(crate) fn add_index_filter_candidates(
        &self,
        collection: &str,
        selection: &Expr,
        indexes: &[IndexDefinition],
        stats: Option<&TableStatistics>,
        total_rows: usize,
        candidates: &mut Vec<QueryPlan>,
    ) -> Result<()> {
        // Below this many rows an exact index probe for cardinality estimation is
        // cheap; above it, estimate instead of scanning (a per-query full prefix
        // scan during planning is the in-run decay on growing tables). ANALYZE
        // stats, when present, are always preferred over both.
        const SMALL_TABLE_EXACT_PROBE_ROWS: usize = 4096;
        let schema = load_schema(self.db_ref(), collection)?;
        for index in indexes
            .iter()
            .filter(|index| index.kind == IndexKind::BTree)
        {
            let mut prefix = Vec::new();
            for field in &index.fields {
                let Some(mut value) = equality_value(selection, field)? else {
                    break;
                };
                if let Some(schema) = schema.as_ref() {
                    value = typed_index_predicate_value(schema, field, value)?;
                }
                prefix.push(value);
            }
            if !prefix.is_empty() {
                let estimated_rows = estimate_index_lookup_rows(index, &prefix, stats)
                    .unwrap_or_else(|| {
                        if total_rows <= SMALL_TABLE_EXACT_PROBE_ROWS {
                            // Small table: an exact probe is cheap and its accurate
                            // cardinality lets the planner pick the more selective
                            // index.
                            self.db_ref()
                                .lookup_index(&index.name, prefix.as_slice())
                                .map(|ids| ids.len())
                                .unwrap_or(total_rows)
                        } else {
                            // Large table, no ANALYZE stats: estimate (each equality
                            // field ~10x selectivity) rather than probe. Probing here
                            // is a full prefix scan, so doing it per plan made
                            // planning O(prefix matches) on every query — the in-run
                            // decay on growing tables.
                            total_rows
                                .saturating_div(10usize.saturating_pow(prefix.len() as u32))
                                .max(1)
                        }
                    })
                    .min(total_rows);
                candidates.push(QueryPlan {
                    kind: PlanKind::IndexLookup {
                        index_name: index.name.clone(),
                        prefix,
                    },
                    estimated_rows,
                    estimated_cost: if stats.is_some() {
                        20.0 + estimated_rows as f64 * 2.0
                    } else {
                        1.0 + estimated_rows as f64 * 0.10
                    },
                });
            }
        }

        for index in indexes
            .iter()
            .filter(|index| index.kind == IndexKind::BTree)
        {
            if schema
                .as_ref()
                .is_some_and(|schema| !index_supports_ordering(schema, &index.name))
            {
                continue;
            }
            let [field] = index.fields.as_slice() else {
                continue;
            };
            let Some((mut lower, mut upper)) = range_bounds(selection, field)? else {
                continue;
            };
            if let Some(schema) = schema.as_ref() {
                lower = lower
                    .map(|value| typed_index_predicate_value(schema, field, value))
                    .transpose()?;
                upper = upper
                    .map(|value| typed_index_predicate_value(schema, field, value))
                    .transpose()?;
            }
            let estimated_rows =
                estimate_index_range_rows(field, lower.as_ref(), upper.as_ref(), stats)
                    .unwrap_or_else(|| {
                        if total_rows <= SMALL_TABLE_EXACT_PROBE_ROWS {
                            self.db_ref()
                                .range_index(&index.name, lower.as_ref(), upper.as_ref())
                                .map(|ids| ids.len())
                                .unwrap_or(total_rows)
                        } else {
                            // Large table: assume a range selects ~1/3 of rows
                            // rather than probing (`range_index` is a full range
                            // scan — the same planning-time decay as the prefix
                            // case above).
                            (total_rows / 3).max(1)
                        }
                    })
                    .min(total_rows);
            candidates.push(QueryPlan {
                kind: PlanKind::IndexRange {
                    index_name: index.name.clone(),
                    lower,
                    upper,
                },
                estimated_rows,
                estimated_cost: if stats.is_some() {
                    20.0 + estimated_rows as f64 * 2.0
                } else {
                    1.0 + estimated_rows as f64 * 0.25
                },
            });
        }
        Ok(())
    }

    /// Candidate for `WHERE <equality prefix> ORDER BY <next index field> LIMIT k`:
    /// returns only the first `k` ids in key order, instead of `IndexLookup`'s
    /// fetch-all-prefix-matches-then-sort. Drives a "latest order per
    /// customer" read, whose cost otherwise grows linearly with a customer's
    /// accumulated orders (the in-run decay). Gated so the WHERE is EXACTLY the
    /// equality prefix (every AND term an equality on a leading index field) — then
    /// the downstream re-filter removes nothing and the bounded result is exact.
    pub(crate) fn add_prefix_ordered_index_candidate(
        &self,
        selection: &Expr,
        query: &Query,
        indexes: &[IndexDefinition],
        candidates: &mut Vec<QueryPlan>,
    ) -> Result<()> {
        if let Some((index_name, prefix, descending, limit)) =
            match_prefix_ordered_index(selection, query, indexes)?
        {
            candidates.push(QueryPlan {
                kind: PlanKind::IndexPrefixOrderedScan {
                    index_name,
                    prefix,
                    descending,
                    limit,
                },
                estimated_rows: limit,
                // Cheaper than IndexLookup (which is `~prefix_match_count`): this
                // reads at most `limit` entries from a single shard.
                estimated_cost: 0.5 + limit as f64 * 0.25,
            });
        }
        Ok(())
    }

    /// Row-path equivalent of [`PlanKind::IndexPrefixOrderedScan`]: when the
    /// row-evaluator SELECT (`execute_row_query`) is a single-table `WHERE
    /// <equality prefix> ORDER BY <next index field> LIMIT k`, fetch only the first
    /// `k` records via a bounded index scan instead of materializing every
    /// prefix match and sorting. Returns the bounded `RowSet`; the caller's normal
    /// filter/order/limit/projection tail then runs over just those rows. `None`
    /// (fall back to the full materialization) unless the shape matches exactly.
    ///
    /// Most real queries take the row path (any `AND`/comparison WHERE forces it),
    /// so without this the "customer's latest order" read cost grew linearly with a
    /// customer's accumulated orders — the dominant in-run workload decay.
    pub(crate) fn try_bounded_ordered_index_row_set(
        &self,
        from: &TableWithJoins,
        select: &Select,
        query: &Query,
    ) -> Result<Option<RowSet>> {
        if !from.joins.is_empty()
            || select.distinct.is_some()
            || has_group_by(select)?
            || has_aggregates(&select.projection)
        {
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
        let Some(selection) = select.selection.as_ref() else {
            return Ok(None);
        };
        // Bounded committed-index reads are only safe with no pending writes (the
        // tx's own uncommitted rows are invisible to the index).
        if !self.tx.is_none_or(|tx| tx.write_len() == 0) {
            return Ok(None);
        }
        let collection = relation_name(name)?;
        if self.cte(&collection).is_some() || is_virtual_table(&collection) {
            return Ok(None);
        }
        let collection = resolve_session_relation_name(self.db_ref(), &collection)?;
        if load_view(self.db_ref(), &collection)?.is_some()
            || load_sequence(self.db_ref(), &collection)?.is_some()
        {
            return Ok(None);
        }
        let schema = load_schema(self.db_ref(), &collection)?;
        let indexes = sql_index_definitions_for_collection(self.db_ref(), &collection)
            .into_iter()
            .filter(|index| {
                index.kind == IndexKind::BTree
                    && schema
                        .as_ref()
                        .is_none_or(|schema| index_supports_ordering(schema, &index.name))
            })
            .collect::<Vec<_>>();
        let Some((index_name, prefix, descending, limit)) =
            match_prefix_ordered_index(selection, query, &indexes)?
        else {
            return Ok(None);
        };
        let ids = self.db_ref().prefix_ordered_index_records(
            &index_name,
            prefix.as_slice(),
            descending,
            limit,
        )?;
        let records = self.records_for_ids_with_schema(&collection, &ids, schema.as_ref())?;
        let alias_name = alias
            .as_ref()
            .map(|alias| alias.name.value.clone())
            .unwrap_or_else(|| {
                collection
                    .rsplit('.')
                    .next()
                    .unwrap_or(&collection)
                    .to_string()
            });
        let fields = row_fields_for_records(schema.as_ref(), &records);
        let columns = row_output_columns_from_fields(&collection, &alias_name, &fields);
        let mut rows = Vec::with_capacity(records.len());
        for record in &records {
            rows.push(slot_row_from_record_fields(
                &collection,
                &alias_name,
                &fields,
                record,
            )?);
        }
        Ok(Some(RowSet { rows, columns }))
    }

    pub(crate) fn primary_key_filter_candidate(
        &self,
        table: &str,
        alias: &str,
        schema: &TableSchema,
        selection: &Expr,
        total_rows: usize,
    ) -> Result<Option<QueryPlan>> {
        let Some(access) =
            self.primary_key_access_for_selection(table, alias, schema, selection)?
        else {
            return Ok(None);
        };
        Ok(Some(match access {
            PrimaryKeyAccess::Exact { record_id, values } => {
                if primary_key_requires_typed_identity(schema) {
                    QueryPlan {
                        kind: PlanKind::PrimaryKeyPrefixLookup {
                            prefix: record_id,
                            prefix_values: values,
                        },
                        estimated_rows: 1,
                        estimated_cost: 0.5,
                    }
                } else {
                    QueryPlan {
                        kind: PlanKind::PrimaryKeyLookup { record_id },
                        estimated_rows: 1,
                        estimated_cost: 0.5,
                    }
                }
            }
            PrimaryKeyAccess::Prefix {
                prefix,
                prefix_values,
                matched_columns,
            } => {
                if self.security_context.is_some() {
                    return Ok(None);
                }
                let estimated_rows = total_rows
                    .saturating_div(10usize.saturating_pow(matched_columns as u32))
                    .max(1)
                    .min(total_rows.max(1));
                if !primary_key_requires_typed_identity(schema) {
                    if let Some(index) =
                        executable_primary_key_index_for_schema(self.db_ref(), schema)
                    {
                        let index_prefix = prefix_values
                            .iter()
                            .cloned()
                            .map(index_value_from_sql)
                            .collect::<Result<Vec<_>>>()?;
                        if !index_prefix.is_empty() {
                            return Ok(Some(QueryPlan {
                                kind: PlanKind::IndexLookup {
                                    index_name: index.name.clone(),
                                    prefix: index_prefix,
                                },
                                estimated_rows,
                                estimated_cost: 0.75 + estimated_rows as f64 * 0.10,
                            }));
                        }
                    }
                }
                QueryPlan {
                    kind: PlanKind::PrimaryKeyPrefixLookup {
                        prefix,
                        prefix_values,
                    },
                    estimated_rows,
                    estimated_cost: 1.0 + estimated_rows as f64 * 0.25,
                }
            }
        }))
    }

    /// A non-negated `pk IN (constant list)` conjunct over a single-column,
    /// untyped primary key becomes one point lookup per literal — the
    /// production alternative was a full scan re-evaluating the list against
    /// every row (observed at 295k rows per search). Typed identities,
    /// composite keys, dynamic elements and NOT IN all decline to the
    /// ordinary planner.
    pub(crate) fn primary_key_in_list_candidate(
        &self,
        table: &str,
        alias: &str,
        schema: &TableSchema,
        selection: &Expr,
    ) -> Result<Option<QueryPlan>> {
        const MAX_IN_LIST_LOOKUPS: usize = 16_384;
        if primary_key_requires_typed_identity(schema) {
            return Ok(None);
        }
        let columns = primary_key_columns_for_schema(schema);
        let [column] = columns.as_slice() else {
            return Ok(None);
        };
        for term in and_terms(selection) {
            let Expr::InList {
                expr,
                list,
                negated: false,
            } = term
            else {
                continue;
            };
            if !self.expr_matches_table_column(expr, table, alias, Some(schema), column)? {
                continue;
            }
            if list.is_empty() || list.len() > MAX_IN_LIST_LOOKUPS {
                continue;
            }
            let mut record_ids = Vec::with_capacity(list.len());
            let mut constant = true;
            for element in list {
                match eval_constant_expr(element) {
                    Ok(value) if !matches!(value, SqlValue::Null) => {
                        record_ids.push(record_id_from_column_values(
                            table,
                            schema,
                            &columns,
                            &[value],
                        )?);
                    }
                    _ => {
                        constant = false;
                        break;
                    }
                }
            }
            if !constant {
                continue;
            }
            record_ids.sort_unstable();
            record_ids.dedup();
            let estimated_rows = record_ids.len();
            return Ok(Some(QueryPlan {
                kind: PlanKind::PrimaryKeyInLookup { record_ids },
                estimated_rows,
                estimated_cost: 0.5 * estimated_rows as f64,
            }));
        }
        Ok(None)
    }

    pub(crate) fn primary_key_access_for_selection(
        &self,
        table: &str,
        alias: &str,
        schema: &TableSchema,
        selection: &Expr,
    ) -> Result<Option<PrimaryKeyAccess>> {
        let primary_key_columns = primary_key_columns_for_schema(schema);
        if primary_key_columns.is_empty() {
            return Ok(None);
        }
        // Which AND term binds each pk column is fixed per statement node
        // (the schema generation is part of the key); only the bound
        // expressions are evaluated per call. This used to walk every term
        // for every pk column on every execution.
        let node_key = self.ir_plan_node_key(selection as *const Expr as usize);
        let plan = match node_key.and_then(|key| primary_key_term_plan_get(key, table, alias)) {
            Some(plan) => plan,
            None => {
                let mut plan = Vec::with_capacity(primary_key_columns.len());
                for column in &primary_key_columns {
                    plan.push(self.dynamic_equality_term_plan(
                        selection,
                        table,
                        alias,
                        Some(schema),
                        column,
                    )?);
                }
                let plan = Rc::new(plan);
                if let Some(key) = node_key {
                    primary_key_term_plan_set(key, table, alias, Rc::clone(&plan));
                }
                plan
            }
        };
        let mut bound_values = Vec::with_capacity(primary_key_columns.len());
        for entry in plan.iter() {
            let value = match entry {
                Some((term_idx, field_on_left)) => match nth_and_term(selection, *term_idx) {
                    Some(Expr::BinaryOp { left, right, .. }) => {
                        let bound = if *field_on_left { right } else { left };
                        Some(self.eval_dynamic_bound_expr(bound)?)
                    }
                    _ => None,
                },
                None => None,
            };
            bound_values.push(value);
        }
        if bound_values
            .iter()
            .flatten()
            .any(|value| matches!(value, SqlValue::Null))
        {
            return Ok(None);
        }
        if bound_values.iter().all(Option::is_some) {
            let values = bound_values
                .into_iter()
                .map(Option::unwrap)
                .collect::<Vec<_>>();
            let record_id =
                record_id_from_column_values(table, schema, &primary_key_columns, &values)?;
            return Ok(Some(PrimaryKeyAccess::Exact { record_id, values }));
        }

        if primary_key_columns.len() <= 1 {
            return Ok(None);
        }
        let prefix_values = bound_values
            .into_iter()
            .take_while(Option::is_some)
            .map(Option::unwrap)
            .collect::<Vec<_>>();
        if prefix_values.is_empty() {
            return Ok(None);
        }
        let matched_columns = prefix_values.len();
        Ok(Some(PrimaryKeyAccess::Prefix {
            prefix: composite_record_id_prefix_from_values(
                schema,
                &primary_key_columns,
                &prefix_values,
            )?,
            prefix_values,
            matched_columns,
        }))
    }

    pub(crate) fn reject_encrypted_predicates(
        &self,
        collection: &str,
        selection: Option<&Expr>,
    ) -> Result<()> {
        let Some(selection) = selection else {
            return Ok(());
        };
        let policy = match self.db_ref().collection_policy(collection) {
            Ok(policy) => policy,
            Err(BicDbError::CollectionNotFound(_)) => return Ok(()),
            Err(error) => return Err(SqlError::from(error)),
        };
        let Some(policy) = policy else {
            return Ok(());
        };
        for path in expression_metadata_paths(selection) {
            if policy
                .columns
                .iter()
                .any(|(field, security)| security.encrypted && path.len() == 1 && path[0] == *field)
            {
                return Err(SqlError::BicDb(BicDbError::Authorization(format!(
                    "encrypted field `{}` cannot be filtered directly; use an approved blind-index lookup",
                    path.join(".")
                ))));
            }
        }
        Ok(())
    }

    pub(crate) fn add_spatial_index_candidates(
        &self,
        selection: &Expr,
        indexes: &[IndexDefinition],
        candidates: &mut Vec<QueryPlan>,
    ) -> Result<()> {
        for index in indexes
            .iter()
            .filter(|index| index.kind == IndexKind::Spatial)
        {
            let [field] = index.fields.as_slice() else {
                continue;
            };
            let Some(predicate) = spatial_index_predicate(selection, field)? else {
                continue;
            };
            let estimated_rows = match &predicate {
                SpatialIndexPredicate::DWithin { lon, lat, meters } => self
                    .db_ref()
                    .spatial_radius_index(&index.name, *lon, *lat, *meters)?
                    .len(),
                SpatialIndexPredicate::IntersectsEnvelope {
                    min_lon,
                    min_lat,
                    max_lon,
                    max_lat,
                } => self
                    .db_ref()
                    .spatial_intersects_index(&index.name, *min_lon, *min_lat, *max_lon, *max_lat)?
                    .len(),
            };
            candidates.push(QueryPlan {
                kind: PlanKind::SpatialIndexScan {
                    index_name: index.name.clone(),
                    predicate,
                },
                estimated_rows,
                estimated_cost: 1.0 + estimated_rows as f64 * 0.05,
            });
        }
        Ok(())
    }

    pub(crate) fn add_geometric_index_candidates(
        &self,
        table: &str,
        alias: &str,
        schema: &TableSchema,
        selection: &Expr,
        indexes: &[IndexDefinition],
        candidates: &mut Vec<QueryPlan>,
    ) -> Result<()> {
        for definition in indexes.iter().filter(|definition| {
            definition.kind == IndexKind::Spatial
                && definition.fields.first().is_some_and(|field| {
                    matches!(field, IndexField::MetadataPath(path) if path.first().is_some_and(|name| name.starts_with("$bicdb_geometric_")))
                })
        }) {
            let Some((index, position)) = schema.indexes.iter().find_map(|index| {
                index
                    .internal_index_names
                    .iter()
                    .position(|name| name.eq_ignore_ascii_case(&definition.name))
                    .map(|position| (index, position))
            })
            else {
                continue;
            };
            let Some(source_expression_text) = index
                .source_expressions
                .get(position)
                .map(String::as_str)
            else {
                continue;
            };
            let source_expression = parse_routine_expr(source_expression_text)?;
            let Some(indexed_type) = projected_expr_pg_type(&source_expression, Some(schema)) else {
                continue;
            };
            for term in and_terms(selection) {
                let Expr::BinaryOp { left, op, right } = term else {
                    continue;
                };
                let operator = op.to_string();
                let (bound, operator) =
                    if full_text_index_expression_matches(source_expression_text, left) {
                    (right.as_ref(), operator)
                } else if full_text_index_expression_matches(source_expression_text, right) {
                    let Some(operator) = reverse_geometric_index_operator(&operator) else {
                        continue;
                    };
                    (left.as_ref(), operator.to_string())
                } else {
                    continue;
                };
                if !self.expr_references_table(
                    &source_expression,
                    table,
                    alias,
                    Some(schema),
                )? || self.expr_references_table(bound, table, alias, Some(schema))?
                    || !self.expr_is_bound_without_table_row(bound)?
                {
                    continue;
                }
                let other_type = projected_expr_pg_type(bound, Some(schema))
                    .unwrap_or_else(|| indexed_type.clone());
                if !geometric_index_operator_supported(
                    &index.access_method,
                    &indexed_type,
                    &operator,
                    &other_type,
                ) {
                    continue;
                }
                let value = self.eval_dynamic_bound_expr(bound)?;
                let Some(bounds) = geometric_index_bounds(&value, &other_type)? else {
                    continue;
                };
                let Some(envelope) = geometric_candidate_envelope(&operator, bounds) else {
                    continue;
                };
                let estimated_rows = self
                    .db_ref()
                    .spatial_intersects_index(
                        &definition.name,
                        envelope[0],
                        envelope[1],
                        envelope[2],
                        envelope[3],
                    )?
                    .len();
                candidates.push(QueryPlan {
                    kind: PlanKind::GeometricIndexScan {
                        index_name: definition.name.clone(),
                        display_name: index.name.clone(),
                        operator,
                        envelope,
                    },
                    estimated_rows,
                    estimated_cost: 1.0 + estimated_rows as f64 * 0.05,
                });
                break;
            }
        }
        Ok(())
    }
}

/// Collect, iteratively (never recursively — a hostile query can nest deeply),
/// every CTE name and every FROM/join TableFactor::Table reference in a query
/// tree. Used by the centralized relation-authorization pass so that no
/// physical execution path can read a base table the caller was never
/// authorized for — the invariant broken by the indexed-equi-join fast path.
fn collect_query_relations(
    root: &sqlparser::ast::Query,
    cte_names: &mut rustc_hash::FxHashSet<String>,
    table_refs: &mut Vec<(String, bool)>,
) {
    use sqlparser::ast::{Query, SetExpr, TableFactor};

    fn visit_factor<'a>(
        factor: &'a TableFactor,
        table_refs: &mut Vec<(String, bool)>,
        queries: &mut Vec<&'a Query>,
    ) {
        match factor {
            TableFactor::Table { name, args, .. } => {
                if let Some(last) = name.0.last() {
                    table_refs.push((last.to_string(), args.is_some()));
                }
            }
            TableFactor::Derived { subquery, .. } => queries.push(subquery),
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                visit_factor(&table_with_joins.relation, table_refs, queries);
                for join in &table_with_joins.joins {
                    visit_factor(&join.relation, table_refs, queries);
                }
            }
            _ => {}
        }
    }

    let mut queries: Vec<&Query> = vec![root];
    while let Some(query) = queries.pop() {
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                cte_names.insert(cte.alias.name.value.clone());
                queries.push(&cte.query);
            }
        }
        let mut bodies: Vec<&SetExpr> = vec![query.body.as_ref()];
        while let Some(body) = bodies.pop() {
            match body {
                SetExpr::Select(select) => {
                    for from in &select.from {
                        visit_factor(&from.relation, table_refs, &mut queries);
                        for join in &from.joins {
                            visit_factor(&join.relation, table_refs, &mut queries);
                        }
                    }
                }
                SetExpr::Query(inner) => queries.push(inner),
                SetExpr::SetOperation { left, right, .. } => {
                    bodies.push(left);
                    bodies.push(right);
                }
                _ => {}
            }
        }
    }
}

impl SqlEngine<'_> {
    /// Authorize SELECT on every base table a query reads, BEFORE any physical
    /// path is chosen — a plan referencing an unauthorized base table cannot
    /// execute. Defense in depth alongside the per-path gates; the per-path
    /// checks remain for views, CTEs, table functions, and virtual tables,
    /// which this pass deliberately skips (they are gated where they resolve).
    pub(crate) fn authorize_read_relations(&self, query: &sqlparser::ast::Query) -> Result<()> {
        let mut cte_names = rustc_hash::FxHashSet::default();
        let mut table_refs = Vec::new();
        collect_query_relations(query, &mut cte_names, &mut table_refs);
        for (name, has_args) in table_refs {
            if has_args || cte_names.contains(&name) || is_virtual_table(&name) {
                continue;
            }
            let resolved = resolve_session_relation_name(self.db_ref(), &name)
                .unwrap_or_else(|_| name.clone());
            // Only real BASE TABLES here; views/functions/absent names are left
            // to the paths that resolve them (and this pass must not overreach).
            if load_schema(self.db_ref(), &resolved)?.is_some() {
                self.require_relation_privilege(&resolved, "SELECT")?;
            }
        }
        Ok(())
    }
}
