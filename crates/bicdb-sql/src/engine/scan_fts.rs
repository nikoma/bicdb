//! Split out of the parent module to keep files digestible; behavior
//! unchanged — a separate `impl` block on the same type.
use super::*;
#[allow(unused_imports)]
use crate::*;

impl<'db> SqlEngine<'db> {
    /// The query's `LIMIT`+`OFFSET` as one static candidate bound, when both
    /// are plain integer literals. Anything dynamic (placeholders,
    /// expressions, `FETCH … WITH TIES`, `LIMIT BY`) returns `None` and the
    /// k-NN scan stays exhaustive.
    pub(crate) fn static_knn_row_bound(query: &Query) -> Option<usize> {
        if query.fetch.is_some() {
            return None;
        }
        let literal = |expr: &Expr| -> Option<usize> {
            let Expr::Value(value) = expr else {
                return None;
            };
            let Value::Number(number, _) = &value.value else {
                return None;
            };
            number.parse::<usize>().ok()
        };
        let (limit, offset) = match query.limit_clause.as_ref()? {
            LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            } => {
                if !limit_by.is_empty() {
                    return None;
                }
                (
                    literal(limit.as_ref()?)?,
                    match offset {
                        None => 0,
                        Some(offset) => literal(&offset.value)?,
                    },
                )
            }
            LimitClause::OffsetCommaLimit { offset, limit } => (literal(limit)?, literal(offset)?),
        };
        limit.checked_add(offset)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn add_geometric_knn_index_candidate(
        &self,
        table: &str,
        alias: &str,
        schema: &TableSchema,
        select: &Select,
        query: &Query,
        indexes: &[IndexDefinition],
        total_rows: usize,
        candidates: &mut Vec<QueryPlan>,
    ) -> Result<()> {
        let Some(order_by) = &query.order_by else {
            return Ok(());
        };
        let OrderByKind::Expressions(expressions) = &order_by.kind else {
            return Ok(());
        };
        let [order] = expressions.as_slice() else {
            return Ok(());
        };
        if order.options.asc == Some(false) {
            return Ok(());
        }
        let Expr::BinaryOp { left, op, right } = &order.expr else {
            return Ok(());
        };
        if op.to_string() != "<->" {
            return Ok(());
        }
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
            let bound = if full_text_index_expression_matches(source_expression_text, left) {
                right.as_ref()
            } else if full_text_index_expression_matches(source_expression_text, right) {
                left.as_ref()
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
                || !geometric_index_operator_supported(
                    &index.access_method,
                    &indexed_type,
                    "<->",
                    "point",
                )
            {
                continue;
            }
            let value = self.eval_dynamic_bound_expr(bound)?;
            let bound_type = projected_expr_pg_type(bound, Some(schema))
                .unwrap_or_else(|| "point".to_string());
            let Some(point) = geometric_index_point(&value, &bound_type)? else {
                continue;
            };
            // B10 RESOLVED: the index scan returns true nearest neighbors in
            // distance order for any k (bounded best-first k-NN), and the
            // row path evaluates `<->` order keys for real, so a STATIC
            // LIMIT(+OFFSET) can bound the scan. The bound is only sound
            // when nothing between the scan and the final ORDER BY/LIMIT
            // can drop or need more rows:
            // - point-typed index only — for box/polygon/circle the index
            //   orders by MBR distance, which is a lower bound, not the
            //   shape distance `<->` sorts by;
            // - no WHERE / RLS / pending transaction writes (each filters
            //   or augments candidates after the scan);
            // - no DISTINCT / GROUP BY / HAVING / window functions (each
            //   consumes the full row set before LIMIT applies).
            let bounded = indexed_type == "point"
                && select.selection.is_none()
                && select.distinct.is_none()
                && select.having.is_none()
                && !has_group_by(select)?
                && !select_has_window_functions(select, query)?
                && self.security_context.is_none()
                && self.tx.is_none_or(|tx| tx.write_len() == 0);
            let limit = if bounded {
                Self::static_knn_row_bound(query)
                    .map_or(total_rows, |bound| bound.min(total_rows))
            } else {
                total_rows
            };
            candidates.push(QueryPlan {
                kind: PlanKind::GeometricKnnIndexScan {
                    index_name: definition.name.clone(),
                    display_name: index.name.clone(),
                    point,
                    limit,
                },
                estimated_rows: limit,
                estimated_cost: 1.0 + limit as f64 * 0.05,
            });
            break;
        }
        Ok(())
    }

    /// `SELECT COUNT(*) FROM t` with nothing that could change which rows
    /// count, answered from the collection's row count instead of by reading
    /// rows.
    ///
    /// This is the query an operator actually runs to check an import, and it
    /// was the most expensive one in the system: on a 2,000,000-row paged
    /// table it materialized every row (4096 MiB, 16.6 s) to produce a single
    /// integer.
    ///
    /// Every guard below removes a way the count could differ from "live rows
    /// in the collection". They are deliberately conservative — a WHERE, a
    /// GROUP BY or RLS needs the ordinary counting path. Resident transaction
    /// counts use version visibility and a pending-write overlay, not today's
    /// collection length, so a pinned snapshot remains authoritative.
    pub(crate) fn try_count_star_fast_path(
        &self,
        from: &TableWithJoins,
        select: &Select,
        query: &Query,
    ) -> Result<Option<SqlResult>> {
        if !from.joins.is_empty()
            || select.selection.is_some()
            || select.having.is_some()
            || select.distinct.is_some()
            || has_group_by(select)?
            || query.order_by.is_some()
            || select_has_window_functions(select, query)?
        {
            return Ok(None);
        }
        // LIMIT/OFFSET apply to the one-row result, not to what is counted, so
        // an OFFSET could legitimately drop it. Rare enough to decline.
        if query.limit_clause.is_some() {
            return Ok(None);
        }
        // A security context can filter rows independently of SQL RLS.
        if self.security_context.is_some() {
            return Ok(None);
        }
        let [item] = select.projection.as_slice() else {
            return Ok(None);
        };
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            _ => return Ok(None),
        };
        let Expr::Function(function) = expr else {
            return Ok(None);
        };
        if function.filter.is_some() || function.over.is_some() {
            return Ok(None);
        }
        if !object_name(&function.name)?.eq_ignore_ascii_case("count") || !is_count_star(function) {
            return Ok(None);
        }
        let TableFactor::Table {
            name, args: None, ..
        } = &from.relation
        else {
            return Ok(None);
        };
        let collection = relation_name(name)?;
        // Views, CTEs, virtual and system tables are not plain collections.
        if is_virtual_table(&collection)
            || self.cte(&collection).is_some()
            || load_view(self.db_ref(), &collection)?.is_some()
        {
            return Ok(None);
        }
        let schema = load_schema(self.db_ref(), &collection)?;
        if schema.as_ref().is_some_and(|schema| schema.rls_enabled) {
            return Ok(None);
        }
        self.require_relation_privilege(&collection, "SELECT")?;
        let count = match self.tx {
            Some(tx) => {
                let Some(count) =
                    tx.try_count_collection_visible_cancellable(&collection, &self.cancellation)?
                else {
                    return Ok(None);
                };
                count
            }
            None => match self
                .db_ref()
                .collection_record_count_cancellable(&collection, &self.cancellation)
            {
                Ok(count) => count,
                // An unknown collection must raise the ordinary error, not zero.
                Err(_) => return Ok(None),
            },
        };
        let column = alias.unwrap_or_else(|| aggregate_column_name(expr, schema.as_ref()));
        sql_profile_index_lookup();
        Ok(Some(
            SqlResult::new(vec![column], vec![vec![SqlValue::Int(count as i64)]])
                .with_column_types(aggregate_projection_column_types(
                    &select.projection,
                    schema.as_ref(),
                )),
        ))
    }

    /// `SELECT count(*) FROM t WHERE tsv @@ q` answered by intersecting the
    /// dense document-id posting blocks of the matching full-text index. The
    /// tsquery must be a constant conjunction of plain lexemes (no prefix,
    /// weight, OR, NOT or phrase operators) — the one shape where lexeme
    /// presence alone decides `@@`, so no positions and no rows are read.
    /// Every other shape, and any index with a transactional tail or
    /// tombstones, declines to the ordinary scan.
    pub(crate) fn try_fts_count_fast_path(
        &self,
        from: &TableWithJoins,
        select: &Select,
        query: &Query,
    ) -> Result<Option<SqlResult>> {
        if !from.joins.is_empty()
            || select.having.is_some()
            || select.distinct.is_some()
            || has_group_by(select)?
            || query.order_by.is_some()
            || select_has_window_functions(select, query)?
        {
            return Ok(None);
        }
        // LIMIT/OFFSET apply to the one-row result, not to what is counted —
        // same decline as `try_count_star_fast_path`.
        if query.limit_clause.is_some() {
            return Ok(None);
        }
        // RLS filters rows and a transaction's pending writes change the
        // count; postings also cannot see either.
        if self.security_context.is_some() || self.tx.is_some() {
            return Ok(None);
        }
        let [item] = select.projection.as_slice() else {
            return Ok(None);
        };
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            _ => return Ok(None),
        };
        let Expr::Function(function) = expr else {
            return Ok(None);
        };
        if function.filter.is_some() || function.over.is_some() {
            return Ok(None);
        }
        if !object_name(&function.name)?.eq_ignore_ascii_case("count") || !is_count_star(function) {
            return Ok(None);
        }
        let TableFactor::Table {
            name,
            alias: table_alias,
            args: None,
            ..
        } = &from.relation
        else {
            return Ok(None);
        };
        let collection = relation_name(name)?;
        let alias_name = table_alias
            .as_ref()
            .map(|alias| alias.name.value.clone())
            .unwrap_or_else(|| {
                collection
                    .rsplit('.')
                    .next()
                    .unwrap_or(&collection)
                    .to_string()
            });
        if is_virtual_table(&collection)
            || self.cte(&collection).is_some()
            || load_view(self.db_ref(), &collection)?.is_some()
        {
            return Ok(None);
        }
        let schema = load_schema(self.db_ref(), &collection)?;
        if schema
            .as_ref()
            .is_some_and(|schema| schema.rls_enabled || schema.rls_forced)
        {
            return Ok(None);
        }
        let Some(selection) = &select.selection else {
            return Ok(None);
        };
        self.require_relation_privilege(&collection, "SELECT")?;
        self.reject_encrypted_predicates(&collection, Some(selection))?;
        // The WHERE must be exactly the one `@@` predicate; extra conjuncts
        // would need row evaluation the postings cannot provide.
        if and_terms(selection).len() != 1 {
            return Ok(None);
        }
        let Some((index_name, _candidate)) =
            self.full_text_index_candidate(&collection, &alias_name, schema.as_ref(), selection)?
        else {
            return Ok(None);
        };
        let Expr::BinaryOp {
            left,
            op: BinaryOperator::AtAt,
            right,
        } = selection
        else {
            return Ok(None);
        };
        let query_expr =
            if projected_expr_pg_type(left, schema.as_ref()).as_deref() == Some("tsvector") {
                right.as_ref()
            } else {
                left.as_ref()
            };
        let where_query =
            crate::fts::sql_value_tsquery(&self.eval_dynamic_bound_expr(query_expr)?)?;
        let count = if let Some(lexemes) = crate::fts::conjunctive_plain_lexemes(&where_query) {
            let terms: Vec<&str> = lexemes.iter().map(String::as_str).collect();
            let Some(count) = self
                .db_ref()
                .full_text_numeric_conjunctive_count(&index_name, &terms)?
            else {
                return Ok(None);
            };
            count
        } else if let Some((lexemes, _)) = crate::fts::phrase_conjunctive_lexemes(&where_query) {
            // Phrase trees: candidates are the conjunction of every lexeme;
            // distance semantics live in `matches()` over each candidate's
            // packed positions — the same recheck the ranked path uses.
            let terms: Vec<&str> = lexemes.iter().map(String::as_str).collect();
            let mut matched = 0u64;
            let mut fts_budget =
                bicdb_core::FtsQueryBudget::new(self.fts_limits, self.cancellation.child());
            let applied = self.db_ref().full_text_numeric_conjunctive_position_scan(
                &index_name,
                &terms,
                &mut fts_budget,
                &mut |_document_id, positions| {
                    let sparse = crate::fts::sparse_candidate_tsvector(&terms, positions);
                    if where_query.matches(&sparse) {
                        matched += 1;
                    }
                    Ok(true)
                },
            )?;
            if applied.is_none() {
                return Ok(None);
            }
            matched
        } else {
            return Ok(None);
        };
        let column = alias.unwrap_or_else(|| aggregate_column_name(expr, schema.as_ref()));
        sql_profile_index_lookup();
        Ok(Some(
            SqlResult::new(vec![column], vec![vec![SqlValue::Int(count as i64)]])
                .with_column_types(aggregate_projection_column_types(
                    &select.projection,
                    schema.as_ref(),
                )),
        ))
    }

    /// Unranked `SELECT <plain fields> FROM t WHERE tsv @@ q [LIMIT n]`
    /// served candidate-first from posting blocks: the conjunction of the
    /// tsquery's lexemes enumerates candidates (with a positions recheck
    /// when a phrase node participates), which resolve to primary keys and
    /// point row fetches — no table scan, no per-row re-tokenization. Rows
    /// come back in document order, which is as arbitrary as the scan order
    /// an ORDER-BY-less SELECT already had. LIMIT/OFFSET bound the scan.
    /// Every other shape declines exactly like `try_fts_count_fast_path`.
    pub(crate) fn try_fts_select_fast_path(
        &self,
        from: &TableWithJoins,
        select: &Select,
        query: &Query,
    ) -> Result<Option<SqlResult>> {
        if !from.joins.is_empty()
            || select.having.is_some()
            || select.distinct.is_some()
            || has_group_by(select)?
            || query.order_by.is_some()
            || has_aggregates(&select.projection)
            || select_has_window_functions(select, query)?
            || select
                .projection
                .iter()
                .any(select_item_needs_row_evaluator)
            || select
                .projection
                .iter()
                .any(|item| select_item_is_whole_row_reference(item, &from.relation))
        {
            return Ok(None);
        }
        if self.security_context.is_some() || self.tx.is_some() {
            return Ok(None);
        }
        let TableFactor::Table {
            name,
            alias: table_alias,
            args: None,
            ..
        } = &from.relation
        else {
            return Ok(None);
        };
        let collection = relation_name(name)?;
        let alias_name = table_alias
            .as_ref()
            .map(|alias| alias.name.value.clone())
            .unwrap_or_else(|| {
                collection
                    .rsplit('.')
                    .next()
                    .unwrap_or(&collection)
                    .to_string()
            });
        if is_virtual_table(&collection)
            || self.cte(&collection).is_some()
            || load_view(self.db_ref(), &collection)?.is_some()
        {
            return Ok(None);
        }
        let schema = load_schema(self.db_ref(), &collection)?;
        if schema
            .as_ref()
            .is_some_and(|schema| schema.rls_enabled || schema.rls_forced)
        {
            return Ok(None);
        }
        let Some(selection) = &select.selection else {
            return Ok(None);
        };
        self.require_relation_privilege(&collection, "SELECT")?;
        self.reject_encrypted_predicates(&collection, Some(selection))?;
        if and_terms(selection).len() != 1 {
            return Ok(None);
        }
        let Some((index_name, _candidate)) =
            self.full_text_index_candidate(&collection, &alias_name, schema.as_ref(), selection)?
        else {
            return Ok(None);
        };
        let Expr::BinaryOp {
            left,
            op: BinaryOperator::AtAt,
            right,
        } = selection
        else {
            return Ok(None);
        };
        let query_expr =
            if projected_expr_pg_type(left, schema.as_ref()).as_deref() == Some("tsvector") {
                right.as_ref()
            } else {
                left.as_ref()
            };
        let where_query =
            crate::fts::sql_value_tsquery(&self.eval_dynamic_bound_expr(query_expr)?)?;
        let Some((lexemes, has_phrase)) = crate::fts::phrase_conjunctive_lexemes(&where_query)
        else {
            return Ok(None);
        };
        let terms: Vec<&str> = lexemes.iter().map(String::as_str).collect();
        let keep = order_by_keep_bound(query)?;
        let mut matched: Vec<u64> = Vec::new();
        let mut fts_budget =
            bicdb_core::FtsQueryBudget::new(self.fts_limits, self.cancellation.child());
        let applied = self.db_ref().full_text_numeric_conjunctive_position_scan(
            &index_name,
            &terms,
            &mut fts_budget,
            &mut |document_id, positions| {
                if has_phrase {
                    let sparse = crate::fts::sparse_candidate_tsvector(&terms, positions);
                    if !where_query.matches(&sparse) {
                        return Ok(true);
                    }
                }
                matched.push(document_id);
                Ok(keep.is_none_or(|keep| matched.len() < keep))
            },
        )?;
        if applied.is_none() {
            return Ok(None);
        }
        let primary_keys = self
            .db_ref()
            .full_text_primary_keys_for_document_ids(&index_name, &matched)?;
        let projection = Projection::from_select_items(&select.projection, schema.as_ref())?;
        let Some(rows) =
            self.ranked_fts_project_primary_keys(&collection, &projection, &primary_keys)?
        else {
            return Ok(None);
        };
        sql_profile_index_lookup();
        let column_types = projection.column_types();
        let column_metadata =
            projection.column_metadata(self.db_ref(), &collection, schema.as_ref());
        let mut result = SqlResult::new(projection.columns, rows)
            .with_column_types(column_types)
            .with_column_metadata(column_metadata);
        apply_limit(&mut result.rows, query)?;
        Ok(Some(result))
    }

    /// Streaming attempt for a query the routing above would send to
    /// `execute_row_query`. Derives the collection, schema and plan itself, and
    /// Streaming DISTINCT (Phase 5c): stream the scan, key each projected row
    /// by the concatenation of its columns' normalized typed index keys,
    /// external-sort, and emit one row per distinct key during the merge.
    /// NULLs compare equal (one NULL row survives), matching DISTINCT.
    /// Declines when any output column lacks a declared pg_type (key
    /// fidelity unprovable), and for every shape the other streamers decline.
    pub(crate) fn try_streaming_distinct(
        &self,
        from: &TableWithJoins,
        select: &Select,
        query: &Query,
    ) -> Result<Option<SqlResult>> {
        if !matches!(
            select.distinct.as_ref(),
            Some(sqlparser::ast::Distinct::Distinct)
        ) {
            return Ok(None);
        }
        if !from.joins.is_empty()
            || has_aggregates(&select.projection)
            || has_group_by(select)?
            || select.having.is_some()
            || query.order_by.is_some()
            || select_has_window_functions(select, query)?
            || select
                .projection
                .iter()
                .any(select_item_needs_row_evaluator)
            || select
                .projection
                .iter()
                .any(|item| select_item_is_whole_row_reference(item, &from.relation))
        {
            return Ok(None);
        }
        if self.security_context.is_some() || self.tx.is_some() {
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
        if self.cte(&collection).is_some()
            || is_virtual_table(&collection)
            || load_view(self.db_ref(), &collection)?.is_some()
        {
            return Ok(None);
        }
        let schema = load_schema(self.db_ref(), &collection)?;
        if schema
            .as_ref()
            .is_some_and(|schema| schema.rls_enabled || schema.rls_forced)
        {
            return Ok(None);
        }
        let plan = self.plan_query(&collection, &alias_name, select, query)?;
        if !matches!(plan.kind, PlanKind::FullScan) {
            return Ok(None);
        }
        let projection = Projection::from_select_items(&select.projection, schema.as_ref())?;
        let column_types = projection.column_types();
        // Every column needs a typed key encoding, or distinctness-by-key is
        // not provably the engine's distinctness.
        let Some(key_types): Option<Vec<String>> = column_types.iter().cloned().collect() else {
            return Ok(None);
        };

        let predicate_env = self.from_relation_columns(&select.from).unwrap_or_default();
        let mut sorter =
            crate::external_sort::ExternalSorter::for_db(self.db_ref(), self.cancellation.clone());
        let mut sql_error: Option<SqlError> = None;
        let mut seen = 0usize;
        let streamed = self.db_ref().for_each_record_batch_cancellable(
            &collection,
            STREAMING_SCAN_BATCH,
            &self.cancellation,
            |batch| {
                for record in batch {
                    seen += 1;
                    if seen % 1024 == 0 {
                        if let Err(error) = self.check_cancellation() {
                            sql_error = Some(error);
                            return Ok(false);
                        }
                    }
                    let record = Arc::new(record);
                    if let Some(selection) = &select.selection {
                        let keep =
                            row_from_record(&collection, &alias_name, schema.as_ref(), &record)
                                .and_then(|row| {
                                    self.eval_row_truth_typed(&row, selection, &predicate_env)
                                });
                        match keep {
                            Ok(verdict) => {
                                if !verdict.unwrap_or(false) {
                                    continue;
                                }
                            }
                            Err(error) => {
                                sql_error = Some(error);
                                return Ok(false);
                            }
                        }
                    }
                    let pushed = (|| -> Result<()> {
                        let row = projection.row(&record)?;
                        let mut key = Vec::new();
                        for (value, pg_type) in row.iter().zip(&key_types) {
                            let bytes = if matches!(value, SqlValue::Null) {
                                None
                            } else {
                                Some(pg_typed_index_key_for_db(self.db_ref(), pg_type, value)?)
                            };
                            crate::external_sort::push_normalized_component(
                                &mut key,
                                bytes.as_deref(),
                                false,
                                false,
                            );
                        }
                        sorter.push(key, row)
                    })();
                    if let Err(error) = pushed {
                        sql_error = Some(error);
                        return Ok(false);
                    }
                }
                Ok(true)
            },
        )?;
        if let Some(error) = sql_error {
            if error.is_resource_limit() || error.is_query_interruption() {
                return Err(error);
            }
            return Ok(None);
        }
        if !streamed {
            return Ok(None);
        }
        let mut rows: Vec<Vec<SqlValue>> = Vec::new();
        let mut last_key: Option<Vec<u8>> = None;
        let merged = sorter.finish_each(|key, row| {
            if last_key.as_deref() != Some(key) {
                last_key = Some(key.to_vec());
                rows.push(row);
            }
            Ok(())
        });
        if let Err(error) = merged {
            if error.is_resource_limit() || error.is_query_interruption() {
                return Err(error);
            }
            return Ok(None);
        }
        sql_profile_full_scan();
        let column_metadata =
            projection.column_metadata(self.db_ref(), &collection, schema.as_ref());
        let mut result = SqlResult::new(projection.columns, rows)
            .with_column_types(column_types)
            .with_column_metadata(column_metadata);
        apply_limit(&mut result.rows, query)?;
        Ok(Some(result))
    }

    /// Streaming GROUP BY (Phase 5c): stream the scan, spill (group key,
    /// group values + per-aggregate input values) through the external
    /// sorter, and fold ADJACENT same-key entries during the merge — memory
    /// is bounded by the spill budget plus ONE ROW PER GROUP of output, never
    /// one per input row. Aggregate folds use the same value-level primitives
    /// as the materializing path. Declines: HAVING, ORDER BY, DISTINCT,
    /// window functions, group keys without typed encodings, projection items
    /// that are neither a group expression nor a foldable aggregate.
    pub(crate) fn try_streaming_group_by(
        &self,
        from: &TableWithJoins,
        select: &Select,
        query: &Query,
    ) -> Result<Option<SqlResult>> {
        if !has_group_by(select)? {
            return Ok(None);
        }
        if !from.joins.is_empty()
            || select.having.is_some()
            || select.distinct.is_some()
            || query.order_by.is_some()
            || select_has_window_functions(select, query)?
        {
            return Ok(None);
        }
        if self.security_context.is_some() || self.tx.is_some() {
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
        if self.cte(&collection).is_some()
            || is_virtual_table(&collection)
            || load_view(self.db_ref(), &collection)?.is_some()
        {
            return Ok(None);
        }
        let schema = load_schema(self.db_ref(), &collection)?;
        if schema
            .as_ref()
            .is_some_and(|schema| schema.rls_enabled || schema.rls_forced)
        {
            return Ok(None);
        }
        let plan = self.plan_query(&collection, &alias_name, select, query)?;
        if !matches!(plan.kind, PlanKind::FullScan) {
            return Ok(None);
        }

        // Group expressions: plain fields with typed key encodings.
        let group_exprs = group_by_exprs(select)?;
        if group_exprs.is_empty() {
            return Ok(None);
        }
        let mut group_fields = Vec::with_capacity(group_exprs.len());
        for expr in &group_exprs {
            let Ok(field_ref) = FieldRef::from_expr(expr) else {
                return Ok(None);
            };
            let field = schema_projected_field(field_ref, schema.as_ref());
            let Some(pg_type) = projected_expr_pg_type(expr, schema.as_ref()) else {
                return Ok(None);
            };
            // "default" is the byte order pg_typed_index_key encodes; only a
            // NON-default collation makes the key ordering unfaithful.
            if crate::eval::expr_collation(expr, schema.as_ref())?
                .is_some_and(|collation| !collation.eq_ignore_ascii_case("default"))
            {
                return Ok(None);
            }
            group_fields.push((field, pg_type));
        }

        // Projection items: a group expression (by position) or a foldable
        // aggregate. Spill layout: group values first, aggregate inputs after.
        enum Item {
            Group(usize),
            Fold {
                slot: usize,
                fold: StreamingFoldKind,
                field: FieldRef,
                pg_type: Option<String>,
            },
        }
        enum StreamingFoldKind {
            CountAll,
            CountField,
            Sum { money: bool },
            Min,
            Max,
            BoolAnd,
            BoolOr,
        }
        let mut items = Vec::new();
        let mut fold_slots = 0usize;
        let mut columns = Vec::new();
        for item in &select.projection {
            let (expr, item_alias) = match item {
                SelectItem::UnnamedExpr(expr) => (expr, None),
                SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
                _ => return Ok(None),
            };
            columns
                .push(item_alias.unwrap_or_else(|| aggregate_column_name(expr, schema.as_ref())));
            // Compare by rendered SQL: sqlparser's Expr equality can include
            // source spans, so the projected `dept` and GROUP BY's `dept`
            // compare unequal as ASTs even when they are the same expression.
            let rendered = expr.to_string();
            if let Some(position) = group_exprs
                .iter()
                .position(|group| group.to_string().eq_ignore_ascii_case(&rendered))
            {
                items.push(Item::Group(position));
                continue;
            }
            let Expr::Function(function) = expr else {
                return Ok(None);
            };
            if function.filter.is_some()
                || function.over.is_some()
                || !is_aggregate_function(function)
            {
                return Ok(None);
            }
            let (fold, field, pg_type) =
                match crate::eval::Aggregate::from_function(function, schema.as_ref())? {
                    crate::eval::Aggregate::CountAll => {
                        (StreamingFoldKind::CountAll, FieldRef::Id, None)
                    }
                    crate::eval::Aggregate::CountField(field, false) => {
                        (StreamingFoldKind::CountField, field, None)
                    }
                    crate::eval::Aggregate::Sum(field, false, money) => {
                        (StreamingFoldKind::Sum { money }, field, None)
                    }
                    crate::eval::Aggregate::Min(field, pg_type) => {
                        (StreamingFoldKind::Min, field, pg_type)
                    }
                    crate::eval::Aggregate::Max(field, pg_type) => {
                        (StreamingFoldKind::Max, field, pg_type)
                    }
                    crate::eval::Aggregate::BoolAnd(field, false) => {
                        (StreamingFoldKind::BoolAnd, field, None)
                    }
                    crate::eval::Aggregate::BoolOr(field, false) => {
                        (StreamingFoldKind::BoolOr, field, None)
                    }
                    _ => return Ok(None),
                };
            items.push(Item::Fold {
                slot: fold_slots,
                fold,
                field,
                pg_type,
            });
            fold_slots += 1;
        }

        let predicate_env = self.from_relation_columns(&select.from).unwrap_or_default();
        let mut sorter =
            crate::external_sort::ExternalSorter::for_db(self.db_ref(), self.cancellation.clone());
        let mut sql_error: Option<SqlError> = None;
        let mut seen = 0usize;
        let streamed = self.db_ref().for_each_record_batch_cancellable(
            &collection,
            STREAMING_SCAN_BATCH,
            &self.cancellation,
            |batch| {
                for record in batch {
                    seen += 1;
                    if seen % 1024 == 0 {
                        if let Err(error) = self.check_cancellation() {
                            sql_error = Some(error);
                            return Ok(false);
                        }
                    }
                    let record = Arc::new(record);
                    if let Some(selection) = &select.selection {
                        let keep =
                            row_from_record(&collection, &alias_name, schema.as_ref(), &record)
                                .and_then(|row| {
                                    self.eval_row_truth_typed(&row, selection, &predicate_env)
                                });
                        match keep {
                            Ok(verdict) => {
                                if !verdict.unwrap_or(false) {
                                    continue;
                                }
                            }
                            Err(error) => {
                                sql_error = Some(error);
                                return Ok(false);
                            }
                        }
                    }
                    let pushed = (|| -> Result<()> {
                        let mut key = Vec::new();
                        let mut spilled = Vec::with_capacity(group_fields.len() + fold_slots);
                        for (field, pg_type) in &group_fields {
                            let value = field.value(&record)?;
                            let bytes = if matches!(value, SqlValue::Null) {
                                None
                            } else {
                                Some(pg_typed_index_key_for_db(self.db_ref(), pg_type, &value)?)
                            };
                            crate::external_sort::push_normalized_component(
                                &mut key,
                                bytes.as_deref(),
                                false,
                                false,
                            );
                            spilled.push(value);
                        }
                        for item in &items {
                            if let Item::Fold { fold, field, .. } = item {
                                spilled.push(match fold {
                                    StreamingFoldKind::CountAll => SqlValue::Int(1),
                                    _ => field.value(&record)?,
                                });
                            }
                        }
                        sorter.push(key, spilled)
                    })();
                    if let Err(error) = pushed {
                        sql_error = Some(error);
                        return Ok(false);
                    }
                }
                Ok(true)
            },
        )?;
        if let Some(error) = sql_error {
            if error.is_resource_limit() || error.is_query_interruption() {
                return Err(error);
            }
            return Ok(None);
        }
        if !streamed {
            return Ok(None);
        }

        // Per-group accumulators, using the SAME value-level primitives as
        // the materializing aggregates.
        enum Accumulator {
            Count(i64),
            Sum {
                money: bool,
                partial: Option<SqlValue>,
            },
            Extreme {
                greatest: bool,
                pg_type: Option<String>,
                best: SqlValue,
            },
            Bool {
                and: bool,
                state: Option<bool>,
            },
        }
        impl Accumulator {
            fn fold(&mut self, value: SqlValue) -> Result<()> {
                match self {
                    Self::Count(count) => {
                        if !matches!(value, SqlValue::Null) {
                            *count += 1;
                        }
                    }
                    Self::Sum { money, partial } => {
                        if !matches!(value, SqlValue::Null) {
                            let chained = partial.take().into_iter().chain(std::iter::once(value));
                            let sum = if *money {
                                crate::eval::sum_money_aggregate_values(
                                    chained.collect::<Vec<_>>(),
                                )?
                            } else {
                                crate::eval::sum_aggregate_values(chained)?
                            };
                            if !matches!(sum, SqlValue::Null) {
                                *partial = Some(sum);
                            }
                        }
                    }
                    Self::Extreme {
                        greatest,
                        pg_type,
                        best,
                    } => {
                        let running = std::mem::replace(best, SqlValue::Null);
                        *best = crate::eval::combine_extreme_value(
                            running,
                            value,
                            pg_type.as_deref(),
                            *greatest,
                        )?;
                    }
                    Self::Bool { and, state } => {
                        if let SqlValue::Bool(value) =
                            crate::eval::bool_aggregate_values(vec![value], *and)?
                        {
                            *state = Some(match state {
                                Some(previous) => {
                                    if *and {
                                        *previous && value
                                    } else {
                                        *previous || value
                                    }
                                }
                                None => value,
                            });
                        }
                    }
                }
                Ok(())
            }
            fn finalize(self) -> SqlValue {
                match self {
                    Self::Count(count) => SqlValue::Int(count),
                    Self::Sum { partial, .. } => partial.unwrap_or(SqlValue::Null),
                    Self::Extreme { best, .. } => best,
                    Self::Bool { state, .. } => state.map(SqlValue::Bool).unwrap_or(SqlValue::Null),
                }
            }
        }
        let fresh_accumulators = |items: &[Item]| -> Vec<Accumulator> {
            items
                .iter()
                .filter_map(|item| match item {
                    Item::Fold { fold, pg_type, .. } => Some(match fold {
                        StreamingFoldKind::CountAll | StreamingFoldKind::CountField => {
                            Accumulator::Count(0)
                        }
                        StreamingFoldKind::Sum { money } => Accumulator::Sum {
                            money: *money,
                            partial: None,
                        },
                        StreamingFoldKind::Min => Accumulator::Extreme {
                            greatest: false,
                            pg_type: pg_type.clone(),
                            best: SqlValue::Null,
                        },
                        StreamingFoldKind::Max => Accumulator::Extreme {
                            greatest: true,
                            pg_type: pg_type.clone(),
                            best: SqlValue::Null,
                        },
                        StreamingFoldKind::BoolAnd => Accumulator::Bool {
                            and: true,
                            state: None,
                        },
                        StreamingFoldKind::BoolOr => Accumulator::Bool {
                            and: false,
                            state: None,
                        },
                    }),
                    Item::Group(_) => None,
                })
                .collect()
        };
        let group_count = group_fields.len();
        let mut rows: Vec<Vec<SqlValue>> = Vec::new();
        let mut current: Option<(Vec<u8>, Vec<SqlValue>, Vec<Accumulator>)> = None;
        let emit = |group_values: Vec<SqlValue>,
                    accumulators: Vec<Accumulator>,
                    rows: &mut Vec<Vec<SqlValue>>| {
            let mut finals = accumulators
                .into_iter()
                .map(Accumulator::finalize)
                .collect::<Vec<_>>();
            // Consume in slot order.
            let mut finals_iter = finals.drain(..);
            let row = items
                .iter()
                .map(|item| match item {
                    Item::Group(position) => group_values[*position].clone(),
                    Item::Fold { .. } => finals_iter.next().unwrap_or(SqlValue::Null),
                })
                .collect::<Vec<_>>();
            rows.push(row);
        };
        let merged = sorter.finish_each(|key, mut spilled| {
            let fold_values = spilled.split_off(group_count);
            let group_values = spilled;
            match &mut current {
                Some((current_key, _, accumulators)) if current_key.as_slice() == key => {
                    for (accumulator, value) in accumulators.iter_mut().zip(fold_values) {
                        accumulator.fold(value)?;
                    }
                }
                _ => {
                    if let Some((_, group_values, accumulators)) = current.take() {
                        emit(group_values, accumulators, &mut rows);
                    }
                    let mut accumulators = fresh_accumulators(&items);
                    for (accumulator, value) in accumulators.iter_mut().zip(fold_values) {
                        accumulator.fold(value)?;
                    }
                    current = Some((key.to_vec(), group_values, accumulators));
                }
            }
            Ok(())
        });
        if let Err(error) = merged {
            if error.is_resource_limit() || error.is_query_interruption() {
                return Err(error);
            }
            return Ok(None);
        }
        if let Some((_, group_values, accumulators)) = current.take() {
            emit(group_values, accumulators, &mut rows);
        }
        sql_profile_full_scan();
        let column_types = aggregate_projection_column_types(&select.projection, schema.as_ref());
        let mut result = validate_integer_result_types(
            SqlResult::new(columns, rows).with_column_types(column_types),
        )?;
        apply_limit(&mut result.rows, query)?;
        Ok(Some(result))
    }

    /// The single positive, unweighted, non-prefix term when BOTH the WHERE
    /// and rank tsqueries are exactly that one operand — the shape the
    /// impact-ordered early termination covers.
    #[allow(clippy::wrong_self_convention)]
    pub(crate) fn single_positive_term_of(
        query: &crate::fts::PgTsQuery,
    ) -> Option<&crate::fts::PgTsQueryOperand> {
        match query.root.as_ref()? {
            crate::fts::PgTsQueryNode::Operand(operand)
                if !operand.prefix && operand.weights == 0 =>
            {
                Some(operand)
            }
            _ => None,
        }
    }

    /// The plain positional argument expressions of a function call, or
    /// `None` when any argument is named/wildcard/qualified — shapes the
    /// ranked top-k path does not model.
    #[allow(clippy::wrong_self_convention)]
    pub(crate) fn plain_rank_function_args(
        function: &sqlparser::ast::Function,
    ) -> Option<Vec<Expr>> {
        let sqlparser::ast::FunctionArguments::List(list) = &function.args else {
            return None;
        };
        if !list.clauses.is_empty() {
            return None;
        }
        let mut args = Vec::with_capacity(list.args.len());
        for arg in &list.args {
            match arg {
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(
                    expr,
                )) => args.push(expr.clone()),
                _ => return None,
            }
        }
        Some(args)
    }

    /// `SELECT <plain fields> FROM t WHERE tsv @@ q ORDER BY ts_rank[_cd](tsv, q2) DESC
    /// LIMIT k` answered ENTIRELY from index postings (Slice 2 of
    /// ranked-search-without-tantivy): candidate documents come from the
    /// durable term postings, the `@@` recheck runs against a SPARSE tsvector
    /// reconstructed from those postings (positions and weights included, so
    /// phrases, weight restrictions, and negations recheck exactly), and the
    /// rank is computed by the SAME ts_rank/ts_rank_cd code over the sparse
    /// vector with the document scalars stored in the payload — bit-identical
    /// to ranking the full document text, without reading a single row until
    /// the top k are known.
    ///
    /// Declines to the correct (materializing) fallback whenever exactness is
    /// not provable: non-read-through indexes, legacy postings without
    /// payloads, WHERE shapes beyond a single `@@`, rank arguments that do
    /// not match the indexed expression, dynamic LIMIT, ASC ordering,
    /// projections needing the row evaluator, RLS, transactions.
    /// Is this tsquery tree a conjunction of plain positive operands —
    /// only And/Phrase/Operand nodes, every operand unweighted and
    /// non-prefix? Returns `(eligible, contains_phrase)`.
    pub(crate) fn conjunctive_tree_shape(node: &crate::fts::PgTsQueryNode) -> (bool, bool) {
        use crate::fts::PgTsQueryNode as Node;
        match node {
            Node::Operand(operand) => (operand.weights == 0 && !operand.prefix, false),
            Node::And(left, right) => {
                let (left_ok, left_phrase) = Self::conjunctive_tree_shape(left);
                let (right_ok, right_phrase) = Self::conjunctive_tree_shape(right);
                (left_ok && right_ok, left_phrase || right_phrase)
            }
            Node::Phrase { left, right, .. } => {
                let (left_ok, left_phrase) = Self::conjunctive_tree_shape(left);
                let (right_ok, right_phrase) = Self::conjunctive_tree_shape(right);
                let _ = (left_phrase, right_phrase);
                (left_ok && right_ok, true)
            }
            Node::Not(_) | Node::Or(..) => (false, false),
        }
    }

    pub(crate) fn positive_or_terms<'a>(
        node: &'a crate::fts::PgTsQueryNode,
        terms: &mut Vec<&'a str>,
    ) -> bool {
        use crate::fts::PgTsQueryNode as Node;
        match node {
            Node::Operand(operand) if operand.weights == 0 && !operand.prefix => {
                terms.push(operand.text.as_str());
                true
            }
            Node::Or(left, right) => {
                Self::positive_or_terms(left, terms) && Self::positive_or_terms(right, terms)
            }
            _ => false,
        }
    }

    /// Turn every non-FTS equality conjunct backed by a native B-tree into
    /// one dense filter for the current FTS generation. `None` means a
    /// conjunct cannot be represented exactly and the ranked fast path must
    /// decline to the general executor.
    pub(crate) fn ranked_fts_native_filter(
        &self,
        collection: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        selection: &Expr,
        full_text_predicate: &Expr,
        full_text_index: &str,
    ) -> Result<Option<Option<bicdb_core::FullTextDocumentFilter>>> {
        let definitions = sql_index_definitions_for_collection(self.db_ref(), collection);
        let mut combined: Option<bicdb_core::FullTextDocumentFilter> = None;
        for predicate in and_terms(selection) {
            if std::ptr::eq(predicate, full_text_predicate) {
                continue;
            }
            if !matches!(
                predicate,
                Expr::BinaryOp {
                    op: BinaryOperator::Eq,
                    ..
                }
            ) {
                return Ok(None);
            }
            let mut matched = None;
            for definition in &definitions {
                if definition.kind != IndexKind::BTree || definition.fields.is_empty() {
                    continue;
                }
                let Some(value) = self.dynamic_equality_value(
                    predicate,
                    collection,
                    alias,
                    schema,
                    &definition.fields[0],
                )?
                else {
                    continue;
                };
                matched = Some((definition.name.as_str(), value));
                break;
            }
            let Some((filter_index, value)) = matched else {
                return Ok(None);
            };
            let filter = self
                .db_ref()
                .full_text_document_filter_from_index(
                    full_text_index,
                    filter_index,
                    std::slice::from_ref(&value),
                )
                .map_err(SqlError::from)?;
            combined = Some(match combined {
                Some(current) => current.intersect(&filter),
                None => filter,
            });
        }
        Ok(Some(combined))
    }

    /// Materialize an already-ranked winner set through BicDB's bounded batch
    /// read path. The ranked fast paths have excluded transactions, RLS, and
    /// row-dependent projections before reaching this helper, so one ordered
    /// batch is equivalent to the former serial `get_record` loop.
    pub(crate) fn ranked_fts_project_primary_keys(
        &self,
        collection: &str,
        projection: &Projection,
        primary_keys: &[String],
    ) -> Result<Option<Vec<Vec<SqlValue>>>> {
        let records = self
            .db_ref()
            .get_records_by_pks(collection, primary_keys)
            .map_err(SqlError::from)?;
        let mut rows = Vec::with_capacity(records.len());
        for record in records {
            let Some(record) = record else {
                return Ok(None);
            };
            rows.push(projection.row(&record)?);
        }
        Ok(Some(rows))
    }

    /// STREAMING CONJUNCTIVE TOP-K: the allocation-free fast path for ranked
    /// multi-term AND / phrase queries over a read-through FTS index.
    ///
    /// The driver term's postings stream in pk order into a flat candidate
    /// arena (one Vec<u16> of packed positions, (start, end) ranges per
    /// candidate per term); every other operand term is answered by ONE
    /// lockstep walk of its posting blocks against the sorted candidate pks.
    /// Candidates missing a positive WHERE term are conjunctively dead and
    /// skipped; phrase distance is verified by `matches()` only when the tree
    /// actually has a phrase node; ranking is the closed-form `rank_and` /
    /// single-operand `ts_rank` (property-pinned bit-exact) straight off the
    /// arena. Replaces: per-posting payload round-trips, a BTreeMap-of-Vecs
    /// per candidate, a rebuilt sparse tsvector per candidate, and a full
    /// query-tree walk per candidate.
    ///
    /// Returns Ok(None) when the shape is outside its validity domain —
    /// the caller falls through to the general docs-map path.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_conjunctive_stream_topk(
        &self,
        index_name: &str,
        collection: &str,
        select: &Select,
        query: &Query,
        schema: Option<&TableSchema>,
        where_query: &crate::fts::PgTsQuery,
        rank_query: &crate::fts::PgTsQuery,
        rank_name: &str,
        weights: [f32; 4],
        normalization: i64,
        operands: &[crate::fts::PgTsQueryOperand],
        driver_term: &str,
        keep: usize,
        native_filter: Option<&bicdb_core::FullTextDocumentFilter>,
    ) -> Result<Option<SqlResult>> {
        use crate::fts::PgTsQueryNode as Node;
        if rank_name != "ts_rank" || normalization != 0 {
            return Ok(None);
        }
        let Some(where_root) = &where_query.root else {
            return Ok(None);
        };
        let Some(rank_root) = &rank_query.root else {
            return Ok(None);
        };
        let (where_ok, where_has_phrase) = Self::conjunctive_tree_shape(where_root);
        let (rank_ok, _) = Self::conjunctive_tree_shape(rank_root);
        if !where_ok || !rank_ok {
            return Ok(None);
        }
        // Distinct terms across BOTH queries, sorted — rank_and's operand
        // order, and the arena's term-slot order.
        let mut terms: Vec<&str> = operands
            .iter()
            .map(|operand| operand.text.as_str())
            .collect();
        terms.sort_unstable();
        terms.dedup();
        let n_terms = terms.len();
        let Some(driver_slot) = terms.iter().position(|term| *term == driver_term) else {
            return Ok(None);
        };
        let mut where_terms: Vec<&str> = Vec::new();
        {
            let mut stack = vec![where_root];
            while let Some(node) = stack.pop() {
                match node {
                    Node::Operand(operand) => where_terms.push(operand.text.as_str()),
                    Node::And(left, right) | Node::Phrase { left, right, .. } => {
                        stack.push(left);
                        stack.push(right);
                    }
                    _ => return Ok(None),
                }
            }
        }
        where_terms.sort_unstable();
        where_terms.dedup();
        let where_slots: Vec<usize> = where_terms
            .iter()
            .filter_map(|term| terms.iter().position(|t| t == term))
            .collect();
        let mut rank_terms: Vec<&str> = Vec::new();
        {
            let mut stack = vec![rank_root];
            while let Some(node) = stack.pop() {
                match node {
                    Node::Operand(operand) => rank_terms.push(operand.text.as_str()),
                    Node::And(left, right) | Node::Phrase { left, right, .. } => {
                        stack.push(left);
                        stack.push(right);
                    }
                    _ => return Ok(None),
                }
            }
        }
        rank_terms.sort_unstable();
        rank_terms.dedup();
        let rank_slots: Vec<usize> = rank_terms
            .iter()
            .filter_map(|term| terms.iter().position(|t| t == term))
            .collect();
        let rank_is_and =
            matches!(rank_root, Node::And(..) | Node::Phrase { .. }) && rank_terms.len() >= 2;

        // Block-max ranked AND wins when the rarest operand is selective
        // (its cursor drives large skips) or when three or more operands
        // multiply the noisy-OR pair bound into real pruning. A broad
        // two-term conjunction defeats both: the bound saturates near 1.0
        // and the walk degenerates into a full decode of every posting
        // list — slower than intersecting document ids outright and decoding
        // positions only for intersection members (the arena path below).
        const WAND_RAREST_DF_CEILING: u64 = 16_384;
        let mut broad_pair = false;
        if rank_is_and && !where_has_phrase && where_slots.len() == terms.len() && n_terms == 2 {
            broad_pair = terms.iter().all(|term| {
                self.db_ref()
                    .full_text_term_statistics(index_name, term)
                    .ok()
                    .flatten()
                    .map(|statistics| statistics.document_frequency)
                    .unwrap_or(0)
                    >= WAND_RAREST_DF_CEILING
            });
        }
        if rank_is_and
            && !where_has_phrase
            && where_query == rank_query
            && where_slots.len() == terms.len()
            && !broad_pair
        {
            if let Some(best) = self
                .db_ref()
                .full_text_block_max_ranked_and_top_k_filtered(
                    index_name,
                    &terms,
                    weights,
                    keep,
                    native_filter,
                )
                .map_err(SqlError::from)?
            {
                let projection = Projection::from_select_items(&select.projection, schema)?;
                let primary_keys = best
                    .into_iter()
                    .map(|posting| posting.primary_key)
                    .collect::<Vec<_>>();
                let Some(rows) =
                    self.ranked_fts_project_primary_keys(collection, &projection, &primary_keys)?
                else {
                    return Ok(None);
                };
                sql_profile_index_lookup();
                let column_types = projection.column_types();
                let column_metadata = projection.column_metadata(self.db_ref(), collection, schema);
                let mut result = SqlResult::new(projection.columns, rows)
                    .with_column_types(column_types)
                    .with_column_metadata(column_metadata);
                apply_limit(&mut result.rows, query)?;
                return Ok(Some(result));
            }
        }
        if native_filter.is_some() {
            return Ok(None);
        }

        // ---- Stream the driver term into the candidate arena. ----
        const ABSENT: (u32, u32) = (u32::MAX, u32::MAX);
        let mut cand_pks: Vec<String> = Vec::new();
        let mut cand_doc_ids: Vec<u64> = Vec::new();
        let mut arena: Vec<u16> = Vec::new();
        let mut ranges: Vec<(u32, u32)> = Vec::new();
        // Numeric generations rank on document ids and resolve a primary key
        // only for candidates that actually contend for the top-k. Resolving
        // one per candidate before ranking made ranked phrase latency linear
        // in the match count — the dominant term at corpus scale. The
        // position scan is a strict conjunction, so a rank-only optional
        // term keeps the legacy paths below.
        let mut numeric_ids = false;
        if where_slots.len() == terms.len() {
            let mut fts_budget =
                bicdb_core::FtsQueryBudget::new(self.fts_limits, self.cancellation.child());
            numeric_ids = self
                .db_ref()
                .full_text_numeric_conjunctive_position_scan(
                    index_name,
                    &terms,
                    &mut fts_budget,
                    &mut |document_id, positions| {
                        cand_doc_ids.push(document_id);
                        let base = ranges.len();
                        ranges.resize(base + n_terms, ABSENT);
                        for (slot, packed) in positions.iter().enumerate() {
                            let Some(packed) = packed else {
                                continue;
                            };
                            let start = arena.len() as u32;
                            arena.extend_from_slice(packed);
                            ranges[base + slot] = (start, arena.len() as u32);
                        }
                        Ok(true)
                    },
                )
                .map_err(SqlError::from)?
                .is_some();
        }
        let numeric = if numeric_ids {
            None
        } else {
            self.db_ref()
                .full_text_numeric_conjunctive_postings(index_name, &terms, &where_slots)
                .map_err(SqlError::from)?
        };
        if numeric_ids {
        } else if let Some(matches) = numeric {
            for posting in matches {
                cand_pks.push(posting.primary_key);
                let base = ranges.len();
                ranges.resize(base + n_terms, ABSENT);
                for (slot, packed) in posting.term_positions.into_iter().enumerate() {
                    let Some(packed) = packed else {
                        continue;
                    };
                    let start = arena.len() as u32;
                    arena.extend_from_slice(&packed);
                    ranges[base + slot] = (start, arena.len() as u32);
                }
            }
        } else {
            let covered = self
                .db_ref()
                .full_text_term_postings_stream(index_name, driver_term, |pk, _, _, packed| {
                    let start = arena.len() as u32;
                    arena.extend_from_slice(packed);
                    cand_pks.push(pk.to_string());
                    let base = ranges.len();
                    ranges.resize(base + n_terms, ABSENT);
                    ranges[base + driver_slot] = (start, arena.len() as u32);
                    Ok(true)
                })
                .map_err(SqlError::from)?;
            if !covered {
                return Ok(None);
            }
            // ---- One lockstep block walk per remaining term. ----
            let pk_refs: Vec<&str> = cand_pks.iter().map(String::as_str).collect();
            for (slot, term) in terms.iter().enumerate() {
                if slot == driver_slot {
                    continue;
                }
                self.db_ref()
                    .full_text_posting_probe_many_stream(
                        index_name,
                        term,
                        &pk_refs,
                        |candidate, _, _, packed| {
                            let start = arena.len() as u32;
                            arena.extend_from_slice(packed);
                            ranges[candidate * n_terms + slot] = (start, arena.len() as u32);
                            Ok(())
                        },
                    )
                    .map_err(SqlError::from)?;
            }
        }

        // ---- Score. ----
        let slice_of = |candidate: usize, slot: usize| -> Option<&[u16]> {
            let (start, end) = ranges[candidate * n_terms + slot];
            if (start, end) == ABSENT {
                None
            } else {
                Some(&arena[start as usize..end as usize])
            }
        };
        // Winners carry (rank, candidate, primary key); the pk resolves
        // lazily on first contention so noncompetitive candidates never pay
        // a document-id mapping lookup.
        let mut best: Vec<(f32, usize, String)> = Vec::new();
        let mut rank_slices: Vec<Option<&[u16]>> = Vec::with_capacity(rank_slots.len());
        let candidate_count = if numeric_ids {
            cand_doc_ids.len()
        } else {
            cand_pks.len()
        };
        for candidate in 0..candidate_count {
            // Conjunctive: every WHERE term must be present.
            if where_slots
                .iter()
                .any(|slot| slice_of(candidate, *slot).is_none())
            {
                continue;
            }
            if where_has_phrase {
                // Distance semantics live in matches(); assemble a sparse
                // vector for just this candidate (phrase candidate sets are
                // post-intersection small).
                let sparse = crate::fts::PgTsVector {
                    lexemes: terms
                        .iter()
                        .enumerate()
                        .filter_map(|(slot, term)| {
                            slice_of(candidate, slot).map(|packed| crate::fts::PgTsLexeme {
                                text: (*term).to_string(),
                                positions: packed
                                    .iter()
                                    .map(|p| crate::fts::unpack_ts_position(*p))
                                    .collect(),
                            })
                        })
                        .collect(),
                };
                if !where_query.matches(&sparse) {
                    continue;
                }
            }
            let rank = if rank_is_and {
                rank_slices.clear();
                rank_slices.extend(rank_slots.iter().map(|slot| slice_of(candidate, *slot)));
                bicdb_core::fts_rank_conjunctive(&rank_slices, weights)
            } else {
                match slice_of(candidate, rank_slots[0]) {
                    Some(packed) => bicdb_core::fts_rank_single_term(packed, weights),
                    // Absent term: rank_or finds no matching lexeme -> 0.
                    None => 0.0,
                }
            };
            // The kth rank alone rejects most candidates; only ties and
            // improvements need the pk (tie-break order is by pk, exactly
            // as before).
            if best.len() >= keep {
                let (kth_rank, _, _) = best.last().expect("non-empty");
                if rank < *kth_rank {
                    continue;
                }
            }
            let pk = if numeric_ids {
                self.db_ref()
                    .full_text_primary_key_for_document_id(index_name, cand_doc_ids[candidate])
                    .map_err(SqlError::from)?
                    .ok_or_else(|| {
                        SqlError::Unsupported(format!(
                            "full-text document id {} has no primary-key mapping",
                            cand_doc_ids[candidate]
                        ))
                    })?
            } else {
                cand_pks[candidate].clone()
            };
            if best.len() >= keep {
                let (kth_rank, _, kth_pk) = best.last().expect("non-empty");
                if rank == *kth_rank && pk.as_str() > kth_pk.as_str() {
                    continue;
                }
            }
            let position = best
                .binary_search_by(|(existing_rank, _, existing_pk)| {
                    rank.partial_cmp(existing_rank)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then_with(|| existing_pk.as_str().cmp(pk.as_str()))
                })
                .unwrap_or_else(|index| index);
            if position < keep {
                best.insert(position, (rank, candidate, pk));
                best.truncate(keep);
            }
        }

        // ---- Materialize the winners. ----
        let projection = Projection::from_select_items(&select.projection, schema)?;
        let primary_keys = best.iter().map(|(_, _, pk)| pk.clone()).collect::<Vec<_>>();
        let Some(rows) =
            self.ranked_fts_project_primary_keys(collection, &projection, &primary_keys)?
        else {
            return Ok(None);
        };
        sql_profile_index_lookup();
        let column_types = projection.column_types();
        let column_metadata = projection.column_metadata(self.db_ref(), collection, schema);
        let mut result = SqlResult::new(projection.columns, rows)
            .with_column_types(column_types)
            .with_column_metadata(column_metadata);
        apply_limit(&mut result.rows, query)?;
        Ok(Some(result))
    }

    pub(crate) fn try_ranked_fts_topk(
        &self,
        from: &TableWithJoins,
        select: &Select,
        query: &Query,
    ) -> Result<Option<SqlResult>> {
        if !from.joins.is_empty()
            || select.distinct.is_some()
            || select.having.is_some()
            || has_group_by(select)?
            || has_aggregates(&select.projection)
            || select_has_window_functions(select, query)?
            || select
                .projection
                .iter()
                .any(select_item_needs_row_evaluator)
            || select
                .projection
                .iter()
                .any(|item| select_item_is_whole_row_reference(item, &from.relation))
        {
            return Ok(None);
        }
        if self.security_context.is_some() || self.tx.is_some() {
            return Ok(None);
        }
        let Some(keep) = order_by_keep_bound(query)? else {
            return Ok(None);
        };
        let Some(order_by) = &query.order_by else {
            return Ok(None);
        };
        let OrderByKind::Expressions(expressions) = &order_by.kind else {
            return Ok(None);
        };
        let [order] = expressions.as_slice() else {
            return Ok(None);
        };
        if order.options.asc != Some(false) {
            return Ok(None);
        }
        let Expr::Function(function) = &order.expr else {
            return Ok(None);
        };
        let rank_name = object_name(&function.name)?.to_ascii_lowercase();
        let rank_name = rank_name.strip_prefix("pg_catalog.").unwrap_or(&rank_name);
        if !matches!(rank_name, "ts_rank" | "ts_rank_cd") {
            return Ok(None);
        }
        if function.filter.is_some() || function.over.is_some() {
            return Ok(None);
        }
        let Some(rank_args) = Self::plain_rank_function_args(function) else {
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
        if self.cte(&collection).is_some()
            || is_virtual_table(&collection)
            || load_view(self.db_ref(), &collection)?.is_some()
        {
            return Ok(None);
        }
        let schema = load_schema(self.db_ref(), &collection)?;
        if schema
            .as_ref()
            .is_some_and(|schema| schema.rls_enabled || schema.rls_forced)
        {
            return Ok(None);
        }

        // Find the one durable `tsv @@ q` predicate. Remaining equality
        // conjuncts may be pushed down through native B-tree bitsets.
        let Some(selection) = &select.selection else {
            return Ok(None);
        };
        self.require_relation_privilege(&collection, "SELECT")?;
        self.reject_encrypted_predicates(&collection, Some(selection))?;
        let Some((index_name, _candidate)) =
            self.full_text_index_candidate(&collection, &alias_name, schema.as_ref(), selection)?
        else {
            return Ok(None);
        };
        let mut full_text_predicate = None;
        for predicate in and_terms(selection) {
            if self
                .full_text_index_candidate(&collection, &alias_name, schema.as_ref(), predicate)?
                .is_some_and(|(candidate_index, _)| {
                    candidate_index.eq_ignore_ascii_case(&index_name)
                })
            {
                if full_text_predicate.is_some() {
                    return Ok(None);
                }
                full_text_predicate = Some(predicate);
            }
        }
        let Some(full_text_predicate) = full_text_predicate else {
            return Ok(None);
        };
        let Some(native_filter) = self.ranked_fts_native_filter(
            &collection,
            &alias_name,
            schema.as_ref(),
            selection,
            full_text_predicate,
            &index_name,
        )?
        else {
            return Ok(None);
        };
        let Expr::BinaryOp {
            left,
            op: BinaryOperator::AtAt,
            right,
        } = full_text_predicate
        else {
            return Ok(None);
        };
        let (where_vector_expr, where_query_expr) =
            if projected_expr_pg_type(left, schema.as_ref()).as_deref() == Some("tsvector") {
                (left.as_ref(), right.as_ref())
            } else {
                (right.as_ref(), left.as_ref())
            };
        let where_query =
            crate::fts::sql_value_tsquery(&self.eval_dynamic_bound_expr(where_query_expr)?)?;

        // Rank arguments: [weights,] vector_expr, query_expr [, normalization].
        // The vector expression must be the SAME indexed expression as the
        // WHERE side; weights/query/normalization must be row-independent.
        let (weights, rank_vector_expr, rank_query_expr, normalization) = match rank_args.len() {
            2 => (None, &rank_args[0], &rank_args[1], None),
            3 => {
                // Either (weights, v, q) or (v, q, normalization):
                // disambiguate by the tsvector side.
                if projected_expr_pg_type(&rank_args[1], schema.as_ref()).as_deref()
                    == Some("tsvector")
                {
                    (Some(&rank_args[0]), &rank_args[1], &rank_args[2], None)
                } else {
                    (None, &rank_args[0], &rank_args[1], Some(&rank_args[2]))
                }
            }
            4 => (
                Some(&rank_args[0]),
                &rank_args[1],
                &rank_args[2],
                Some(&rank_args[3]),
            ),
            _ => return Ok(None),
        };
        // Same indexed expression on both sides — compared as rendered SQL
        // (sqlparser Expr equality includes source spans).
        if !where_vector_expr
            .to_string()
            .eq_ignore_ascii_case(&rank_vector_expr.to_string())
        {
            return Ok(None);
        }
        if !self.expr_is_bound_without_table_row(rank_query_expr)? {
            return Ok(None);
        }
        let rank_query =
            crate::fts::sql_value_tsquery(&self.eval_dynamic_bound_expr(rank_query_expr)?)?;
        let weights = match weights {
            None => [0.1f32, 0.2, 0.4, 1.0],
            Some(expr) => {
                if !self.expr_is_bound_without_table_row(expr)? {
                    return Ok(None);
                }
                match crate::fts::rank_weights_from_value(&self.eval_dynamic_bound_expr(expr)?) {
                    Ok(weights) => weights,
                    Err(_) => return Ok(None),
                }
            }
        };
        let normalization = match normalization {
            None => 0i64,
            Some(expr) => {
                if !self.expr_is_bound_without_table_row(expr)? {
                    return Ok(None);
                }
                match self.eval_dynamic_bound_expr(expr)? {
                    SqlValue::Int(value) => value,
                    _ => return Ok(None),
                }
            }
        };

        // Fetch postings for every operand term of BOTH queries (negated
        // operands included — they contribute to matches() and to the rank);
        // prefix operands expand over the term keyspace.
        let mut operands = Vec::new();
        for tsquery in [&where_query, &rank_query] {
            if let Some(root) = &tsquery.root {
                crate::fts::collect_query_operands(root, &mut operands);
            }
        }
        if operands.is_empty() {
            return Ok(None);
        }
        struct DocPostings {
            lexemes: std::collections::BTreeMap<String, Vec<crate::fts::PgTsPosition>>,
            doc_length: u32,
            doc_distinct: u32,
        }
        // MULTI-TERM BLOCK-MAX WAND / MAXSCORE: exact additive ts_rank
        // contributions for a plain positive OR query. The core keeps one
        // lazy cursor per numeric posting list, chooses MaxScore pivots, and
        // applies per-block maxima before scoring candidates.
        if rank_name == "ts_rank"
            && normalization == 0
            && weights == [0.1f32, 0.2, 0.4, 1.0]
            && where_query == rank_query
        {
            let mut terms = Vec::new();
            if where_query
                .root
                .as_ref()
                .is_some_and(|root| Self::positive_or_terms(root, &mut terms))
            {
                terms.sort_unstable();
                terms.dedup();
                if terms.len() >= 2 {
                    if let Some(best) = self
                        .db_ref()
                        .full_text_block_max_wand_top_k_filtered(
                            &index_name,
                            &terms,
                            weights,
                            keep,
                            native_filter.as_ref(),
                        )
                        .map_err(SqlError::from)?
                    {
                        let projection =
                            Projection::from_select_items(&select.projection, schema.as_ref())?;
                        let primary_keys = best
                            .into_iter()
                            .map(|posting| posting.primary_key)
                            .collect::<Vec<_>>();
                        let Some(rows) = self.ranked_fts_project_primary_keys(
                            &collection,
                            &projection,
                            &primary_keys,
                        )?
                        else {
                            return Ok(None);
                        };
                        sql_profile_index_lookup();
                        let column_types = projection.column_types();
                        let column_metadata =
                            projection.column_metadata(self.db_ref(), &collection, schema.as_ref());
                        let mut result = SqlResult::new(projection.columns, rows)
                            .with_column_types(column_types)
                            .with_column_metadata(column_metadata);
                        apply_limit(&mut result.rows, query)?;
                        return Ok(Some(result));
                    }
                }
            }
        }
        // IMPACT-ORDERED EARLY TERMINATION: the canonical search shape —
        // one positive unweighted term, ts_rank, default weights, no
        // normalization — walks the v2 impact ordering (descending bucket)
        // and STOPS once the top-k cannot change: every remaining posting's
        // raw score is strictly below its bucket's upper edge
        // ((bucket+1)/155), so `kth >= edge` proves no remaining document
        // can displace the kth (ties lose strictly: remaining < edge <= kth).
        let single_positive_term = match (
            Self::single_positive_term_of(&where_query),
            Self::single_positive_term_of(&rank_query),
        ) {
            (Some(where_op), Some(rank_op)) if where_op.text == rank_op.text => {
                Some(where_op.text.clone())
            }
            _ => None,
        };
        if let (None, Some(term)) = (native_filter.as_ref(), single_positive_term) {
            if rank_name == "ts_rank" && normalization == 0 && weights == [0.1f32, 0.2, 0.4, 1.0] {
                let mut best: Vec<(f32, String)> = Vec::new();
                let mut failed: Option<SqlError> = None;
                let mut scanned = 0usize;
                let mut first_bucket: Option<u16> = None;
                let mut fts_budget =
                    bicdb_core::FtsQueryBudget::new(self.fts_limits, self.cancellation.child());
                let covered = self
                    .db_ref()
                    .full_text_impact_scan(
                        &index_name,
                        &term,
                        &mut fts_budget,
                        |bucket, pk, _doc_length, _doc_distinct, packed| {
                            scanned += 1;
                            // Tie-plateau bail: sound termination must finish the
                            // current bucket, so a top bucket the size of the
                            // corpus (uniform tf) would degrade into probing
                            // everything. Once a SECOND bucket appears the
                            // ordering discriminates and termination is near —
                            // never bail then. Inside the first bucket, give up
                            // after a bounded number of probes and let the bulk
                            // path rank once.
                            let discriminates = match first_bucket {
                                None => {
                                    first_bucket = Some(bucket);
                                    false
                                }
                                Some(first) => bucket != first,
                            };
                            if !discriminates && scanned > (keep * 4).max(8_192) {
                                failed =
                                    Some(SqlError::InvalidSql("impact scan plateaued".to_string()));
                                return Ok(false);
                            }
                            if best.len() >= keep {
                                let kth = best
                                    .last()
                                    .map(|(rank, _)| *rank)
                                    .unwrap_or(f32::NEG_INFINITY);
                                if kth >= bicdb_core::fts_impact_bucket_upper_edge(bucket) {
                                    return Ok(false);
                                }
                            }
                            // Closed-form single-operand ts_rank straight from the
                            // packed positions (guarded above: default weights,
                            // normalization 0) — no sparse tsvector, no query
                            // walk, no allocation for postings that do not place.
                            let rank = bicdb_core::fts_rank_single_term(packed, weights);
                            // One compare rejects almost every posting before the
                            // top-k binary search: it loses to the kth on rank,
                            // or ties and loses on pk.
                            if best.len() >= keep {
                                let (kth_rank, kth_pk) = best.last().expect("non-empty");
                                if rank < *kth_rank || (rank == *kth_rank && pk > kth_pk.as_str()) {
                                    return Ok(true);
                                }
                            }
                            let position = best
                                .binary_search_by(|(existing_rank, existing_pk)| {
                                    rank.partial_cmp(existing_rank)
                                        .unwrap_or(std::cmp::Ordering::Equal)
                                        .then_with(|| existing_pk.as_str().cmp(pk))
                                })
                                .unwrap_or_else(|index| index);
                            if position < keep {
                                best.insert(position, (rank, pk.to_string()));
                                best.truncate(keep);
                            }
                            Ok(true)
                        },
                    )
                    .map_err(SqlError::from)?;
                if std::env::var_os("BICDB_IMPACT_TRACE").is_some() {
                    eprintln!(
                        "IMPACT covered={covered} failed={:?} best={} scanned={scanned}",
                        failed.as_ref().map(|error| error.to_string()),
                        best.len()
                    );
                }
                let mut covered = covered;
                let block_scan_started = std::time::Instant::now();
                if !covered {
                    // Folded index: same termination at BLOCK granularity —
                    // blocks arrive in descending max-impact order and the
                    // kth-vs-upper-edge bound applies to the block max.
                    best.clear();
                    scanned = 0;
                    first_bucket = None;
                    failed = None;
                    let mut blocks_skipped = 0usize;
                    // The gate and the visitor cannot both borrow `best`;
                    // the kth rank crosses between them through a Cell,
                    // updated by the visitor whenever the top-k changes.
                    let kth_cell = std::cell::Cell::new(None::<f32>);
                    covered = self
                        .db_ref()
                        .full_text_block_impact_scan(
                            &index_name,
                            &term,
                            &mut fts_budget,
                            |bucket, max_rank| {
                                let Some(kth) = kth_cell.get() else {
                                    return bicdb_core::BlockGate::Scan;
                                };
                                // Bucket bound: blocks arrive in descending
                                // max-impact order, so once the kth reaches a
                                // bucket's upper edge nothing later can win.
                                if kth >= bicdb_core::fts_impact_bucket_upper_edge(bucket) {
                                    return bicdb_core::BlockGate::Stop;
                                }
                                // BLOCK-MAX: the exact best rank in THIS block
                                // cannot beat the kth — and an exact tie loses
                                // (scan order within a bucket is pk-ascending,
                                // so a remaining equal-rank posting has a
                                // larger pk than every equal-rank member of
                                // the top-k). Exact maxima are not monotonic
                                // within a bucket, so this skips rather than
                                // stops. This is what retires a tie plateau
                                // (uniform tf) at header cost instead of
                                // decoding the whole posting list.
                                if max_rank.is_some_and(|max_rank| kth >= max_rank) {
                                    blocks_skipped += 1;
                                    return bicdb_core::BlockGate::Skip;
                                }
                                bicdb_core::BlockGate::Scan
                            },
                            |bucket, pk, _doc_length, _doc_distinct, packed| {
                                scanned += 1;
                                if best.len() >= keep {
                                    let kth = best
                                        .last()
                                        .map(|(rank, _)| *rank)
                                        .unwrap_or(f32::NEG_INFINITY);
                                    if bucket != u16::MAX
                                        && kth >= bicdb_core::fts_impact_bucket_upper_edge(bucket)
                                    {
                                        return Ok(false);
                                    }
                                }
                                let rank = bicdb_core::fts_rank_single_term(packed, weights);
                                if best.len() >= keep {
                                    let (kth_rank, kth_pk) = best.last().expect("non-empty");
                                    if rank < *kth_rank
                                        || (rank == *kth_rank && pk > kth_pk.as_str())
                                    {
                                        return Ok(true);
                                    }
                                }
                                let position = best
                                    .binary_search_by(|(existing_rank, existing_pk)| {
                                        rank.partial_cmp(existing_rank)
                                            .unwrap_or(std::cmp::Ordering::Equal)
                                            .then_with(|| existing_pk.as_str().cmp(pk))
                                    })
                                    .unwrap_or_else(|index| index);
                                if position < keep {
                                    best.insert(position, (rank, pk.to_string()));
                                    best.truncate(keep);
                                    if best.len() >= keep {
                                        kth_cell.set(best.last().map(|(rank, _)| *rank));
                                    }
                                }
                                Ok(true)
                            },
                        )
                        .map_err(SqlError::from)?;
                    if std::env::var_os("BICDB_IMPACT_TRACE").is_some() {
                        eprintln!(
                            "BLOCK-IMPACT covered={covered} failed={:?} best={} scanned={scanned} skipped={blocks_skipped} scan_ms={:.1}",
                            failed.as_ref().map(|error| error.to_string()),
                            best.len(),
                            block_scan_started.elapsed().as_secs_f64() * 1e3
                        );
                    }
                }
                if covered && failed.is_none() {
                    let projection =
                        Projection::from_select_items(&select.projection, schema.as_ref())?;
                    let primary_keys = best
                        .iter()
                        .map(|(_, primary_key)| primary_key.clone())
                        .collect::<Vec<_>>();
                    let Some(rows) = self.ranked_fts_project_primary_keys(
                        &collection,
                        &projection,
                        &primary_keys,
                    )?
                    else {
                        return Ok(None);
                    };
                    sql_profile_index_lookup();
                    let column_types = projection.column_types();
                    let column_metadata =
                        projection.column_metadata(self.db_ref(), &collection, schema.as_ref());
                    let mut result = SqlResult::new(projection.columns, rows)
                        .with_column_types(column_types)
                        .with_column_metadata(column_metadata);
                    apply_limit(&mut result.rows, query)?;
                    return Ok(Some(result));
                }
            }
        }

        let mut docs: FxHashMap<String, DocPostings> = FxHashMap::default();
        fn absorb_posting(
            docs: &mut FxHashMap<String, DocPostings>,
            term: String,
            pk: String,
            payload: &[u8],
        ) -> bool {
            let Some((doc_length, doc_distinct, packed)) =
                bicdb_core::decode_fts_posting_payload(payload)
            else {
                // Legacy posting without positions: exact ranking from the
                // index is impossible — the caller falls back to row text.
                return false;
            };
            let doc = docs.entry(pk).or_insert_with(|| DocPostings {
                lexemes: std::collections::BTreeMap::new(),
                doc_length,
                doc_distinct,
            });
            doc.lexemes.entry(term).or_insert_with(|| {
                packed
                    .iter()
                    .map(|packed| crate::fts::unpack_ts_position(*packed))
                    .collect()
            });
            true
        }

        // PROBE-DRIVEN AND INTERSECTION: for a pure conjunctive WHERE query
        // (no OR, no prefix operands), pick the RAREST positive operand as
        // the driver, fetch only its postings, and resolve every other term
        // per driver document with a single-descent point probe. An AND of a
        // rare and a common term costs |rare| probes instead of a walk of
        // the common term's whole posting list.
        // First look for a genuinely rare driver with a cheap capped scan.
        // Only when every operand exceeds that cap do the larger-corpus pass:
        // this keeps entity-led queries from counting a million postings for
        // adjacent common terms, while still finding a useful driver for
        // conjunctions whose rarest term is common at web-corpus scale.
        const RARE_DRIVER_CAP: usize = 8_192;
        const DRIVER_CAP: usize = 1_000_000;
        let mut probed = false;
        let conjunctive = where_query
            .root
            .as_ref()
            .is_some_and(|root| !crate::fts::tsquery_has_or(root))
            && rank_query
                .root
                .as_ref()
                .is_none_or(|root| !crate::fts::tsquery_has_or(root))
            && !operands.iter().any(|operand| operand.prefix);
        let distinct_terms: std::collections::BTreeSet<&str> = operands
            .iter()
            .map(|operand| operand.text.as_str())
            .collect();
        // A single-term query has nothing to intersect: probing would only
        // add a counting pass over the same posting list.
        if conjunctive && distinct_terms.len() >= 2 {
            let mut positive = Vec::new();
            if let Some(root) = &where_query.root {
                crate::fts::collect_positive_operands(root, &mut positive);
            }
            let mut seen = std::collections::HashSet::new();
            positive.retain(|operand| seen.insert(operand.text.clone()));
            let mut driver: Option<(String, usize)> = None;
            for operand in &positive {
                match self.db_ref().full_text_term_count_capped(
                    &index_name,
                    &operand.text,
                    RARE_DRIVER_CAP,
                ) {
                    Ok(Some(count)) => {
                        if driver.as_ref().is_none_or(|(_, best)| count < *best) {
                            driver = Some((operand.text.clone(), count));
                        }
                    }
                    Ok(None) => {}
                    Err(_) => return Ok(None),
                }
            }
            if driver.is_none() {
                for operand in &positive {
                    match self.db_ref().full_text_term_count_capped(
                        &index_name,
                        &operand.text,
                        DRIVER_CAP,
                    ) {
                        Ok(Some(count)) => {
                            if driver.as_ref().is_none_or(|(_, best)| count < *best) {
                                driver = Some((operand.text.clone(), count));
                            }
                        }
                        Ok(None) => {}
                        Err(_) => return Ok(None),
                    }
                }
            }
            if let Some((driver_term, _)) = driver {
                if let Some(result) = self.try_conjunctive_stream_topk(
                    &index_name,
                    &collection,
                    select,
                    query,
                    schema.as_ref(),
                    &where_query,
                    &rank_query,
                    rank_name,
                    weights,
                    normalization,
                    &operands,
                    &driver_term,
                    keep,
                    native_filter.as_ref(),
                )? {
                    return Ok(Some(result));
                }
                if native_filter.is_some() {
                    return Ok(None);
                }
                // Budgeted + cancellable: this is the materializing route a
                // dropped connection previously could not stop. Budget and
                // cancellation failures abort the QUERY; any other error
                // keeps the legacy decline-to-other-plans behavior.
                let mut fts_budget =
                    bicdb_core::FtsQueryBudget::new(self.fts_limits, self.cancellation.child());
                let driver_postings = match self.db_ref().full_text_term_postings_budgeted(
                    &index_name,
                    &driver_term,
                    false,
                    &mut fts_budget,
                ) {
                    Ok(postings) => postings,
                    Err(error) if fts_budget_abort(&error) => return Err(error.into()),
                    Err(_) => return Ok(None),
                };
                for (term, pk, payload) in driver_postings {
                    fts_budget.charge_candidates(1).map_err(SqlError::from)?;
                    if !absorb_posting(&mut docs, term, pk, &payload) {
                        return Ok(None);
                    }
                }
                let mut need_terms: std::collections::BTreeSet<String> = operands
                    .iter()
                    .map(|operand| operand.text.clone())
                    .collect();
                need_terms.remove(&driver_term);
                let mut pks: Vec<String> = docs.keys().cloned().collect();
                pks.sort_unstable();
                for term in &need_terms {
                    match self.db_ref().full_text_posting_probe_many_budgeted(
                        &index_name,
                        term,
                        &pks,
                        &mut fts_budget,
                    ) {
                        Ok(hits) => {
                            for (pk, payload) in hits {
                                if !absorb_posting(&mut docs, term.clone(), pk, &payload) {
                                    return Ok(None);
                                }
                            }
                        }
                        Err(error) if fts_budget_abort(&error) => return Err(error.into()),
                        Err(_) => return Ok(None),
                    }
                }
                probed = true;
            }
        }
        if native_filter.is_some() {
            return Ok(None);
        }
        if !probed {
            let mut fetched_terms: std::collections::BTreeSet<(String, bool)> =
                std::collections::BTreeSet::new();
            let mut fts_budget =
                bicdb_core::FtsQueryBudget::new(self.fts_limits, self.cancellation.child());
            for operand in &operands {
                if !fetched_terms.insert((operand.text.clone(), operand.prefix)) {
                    continue;
                }
                let postings = match self.db_ref().full_text_term_postings_budgeted(
                    &index_name,
                    &operand.text,
                    operand.prefix,
                    &mut fts_budget,
                ) {
                    Ok(postings) => postings,
                    Err(error) if fts_budget_abort(&error) => return Err(error.into()),
                    // Not read-through (embedded / pre-upgrade): fall back.
                    Err(_) => return Ok(None),
                };
                for (term, pk, payload) in postings {
                    fts_budget.charge_candidates(1).map_err(SqlError::from)?;
                    if !absorb_posting(&mut docs, term, pk, &payload) {
                        return Ok(None);
                    }
                }
            }
        }

        // Recheck + rank per candidate document, keeping the top `keep`.
        let mut ranked: Vec<(f32, String)> = Vec::new();
        for (pk, doc) in &docs {
            self.check_cancellation()?;
            let sparse = crate::fts::PgTsVector {
                lexemes: doc
                    .lexemes
                    .iter()
                    .map(|(text, positions)| crate::fts::PgTsLexeme {
                        text: text.clone(),
                        positions: positions.clone(),
                    })
                    .collect(),
            };
            if !where_query.matches(&sparse) {
                continue;
            }
            let scalars = Some(crate::fts::FtsDocumentScalars {
                length: doc.doc_length,
                distinct: doc.doc_distinct,
            });
            let rank = if rank_name == "ts_rank" {
                crate::fts::ts_rank_with_scalars(
                    &sparse,
                    &rank_query,
                    weights,
                    normalization,
                    scalars,
                )
            } else {
                crate::fts::ts_rank_cd_with_scalars(
                    &sparse,
                    &rank_query,
                    weights,
                    normalization,
                    scalars,
                )
            };
            ranked.push((rank, pk.clone()));
        }
        // Deterministic: rank descending, pk ascending on ties.
        ranked.sort_by(|left, right| {
            right
                .0
                .partial_cmp(&left.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.1.cmp(&right.1))
        });
        ranked.truncate(keep);

        let projection = Projection::from_select_items(&select.projection, schema.as_ref())?;
        let primary_keys = ranked
            .iter()
            .map(|(_, primary_key)| primary_key.clone())
            .collect::<Vec<_>>();
        // The posting says each row exists; a miss means we raced a write —
        // abandon rather than answer with a hole.
        let Some(rows) =
            self.ranked_fts_project_primary_keys(&collection, &projection, &primary_keys)?
        else {
            return Ok(None);
        };
        sql_profile_index_lookup();
        let column_types = projection.column_types();
        let column_metadata =
            projection.column_metadata(self.db_ref(), &collection, schema.as_ref());
        let mut result = SqlResult::new(projection.columns, rows)
            .with_column_types(column_types)
            .with_column_metadata(column_metadata);
        apply_limit(&mut result.rows, query)?;
        Ok(Some(result))
    }

    /// [`Self::try_streaming_external_sort`] behind the same relation
    /// extraction as [`Self::try_streaming_row_query`], for call sites that
    /// have not resolved the collection/plan yet.
    pub(crate) fn try_streaming_external_sort_query(
        &self,
        from: &TableWithJoins,
        select: &Select,
        query: &Query,
    ) -> Result<Option<SqlResult>> {
        if !from.joins.is_empty() {
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
        if self.cte(&collection).is_some()
            || is_virtual_table(&collection)
            || load_view(self.db_ref(), &collection)?.is_some()
        {
            return Ok(None);
        }
        let schema = load_schema(self.db_ref(), &collection)?;
        let plan = self.plan_query(&collection, &alias_name, select, query)?;
        self.try_streaming_external_sort(
            &collection,
            &alias_name,
            select,
            query,
            &plan,
            schema.as_ref(),
        )
    }

    /// Full-table ORDER BY as an external merge sort (Phase 5b): stream the
    /// scan, normalize each row's ORDER BY values into one memcmp-comparable
    /// key (escape-encoded components, byte-inverted for DESC, null markers
    /// per NULLS FIRST/LAST), push into [`crate::external_sort::ExternalSorter`]
    /// — which spills sorted runs past its budget — and merge. Sort memory is
    /// bounded by the spill budget; only the query's OUTPUT materializes.
    ///
    /// Declines (falls back to the materializing sort) whenever fidelity is
    /// not provable: expressions that are not plain projected fields, types
    /// without a normalized key encoding, collated or user-typed columns,
    /// row-evaluator projections, and every shape the other streaming paths
    /// decline (RLS, transactions, virtual tables, non-full-scan plans).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_streaming_external_sort(
        &self,
        collection: &str,
        alias: &str,
        select: &Select,
        query: &Query,
        plan: &QueryPlan,
        schema: Option<&TableSchema>,
    ) -> Result<Option<SqlResult>> {
        if !matches!(plan.kind, PlanKind::FullScan) {
            return Ok(None);
        }
        let Some(order_by) = &query.order_by else {
            return Ok(None);
        };
        let OrderByKind::Expressions(expressions) = &order_by.kind else {
            return Ok(None);
        };
        if expressions.is_empty() {
            return Ok(None);
        }
        if has_aggregates(&select.projection)
            || select.distinct.is_some()
            || has_group_by(select)?
            || select.having.is_some()
            || select
                .projection
                .iter()
                .any(select_item_needs_row_evaluator)
            || select
                .projection
                .iter()
                .any(|item| select_item_is_whole_row_reference(item, &select.from[0].relation))
        {
            return Ok(None);
        }
        if self.security_context.is_some() || self.tx.is_some() {
            return Ok(None);
        }
        if schema
            .as_ref()
            .is_some_and(|schema| schema.rls_enabled || schema.rls_forced)
        {
            return Ok(None);
        }

        // One normalizer per ORDER BY expression, or bail.
        struct KeyDriver {
            field: FieldRef,
            pg_type: String,
            descending: bool,
            nulls_first: bool,
        }
        let mut drivers = Vec::with_capacity(expressions.len());
        for order in expressions {
            let Ok(field_ref) = FieldRef::from_expr(&order.expr) else {
                return Ok(None);
            };
            let field = schema_projected_field(field_ref, schema);
            let Some(pg_type) = projected_expr_pg_type(&order.expr, schema) else {
                return Ok(None);
            };
            // "default" is the byte order pg_typed_index_key encodes; only a
            // NON-default collation makes the key ordering unfaithful.
            if crate::eval::expr_collation(&order.expr, schema)?
                .is_some_and(|collation| !collation.eq_ignore_ascii_case("default"))
            {
                return Ok(None);
            }
            // User-typed (enum/domain) columns order by their catalog order,
            // which `pg_typed_index_key` does not encode.
            if let Some(schema) = schema {
                if let Ok(column_name) = partition_key_column_name(&order.expr) {
                    if schema
                        .column(&column_name)
                        .is_some_and(|column| column.user_type.is_some())
                    {
                        return Ok(None);
                    }
                }
            }
            let descending = order.options.asc == Some(false);
            drivers.push(KeyDriver {
                field,
                pg_type,
                descending,
                // PostgreSQL defaults: ASC = NULLS LAST, DESC = NULLS FIRST.
                nulls_first: order.options.nulls_first.unwrap_or(descending),
            });
        }

        let projection = Projection::from_select_items(&select.projection, schema)?;
        let predicate_env = self.from_relation_columns(&select.from).unwrap_or_default();
        let mut sorter =
            crate::external_sort::ExternalSorter::for_db(self.db_ref(), self.cancellation.clone());
        let mut sql_error: Option<SqlError> = None;
        let mut seen = 0usize;
        let streamed = self.db_ref().for_each_record_batch_cancellable(
            collection,
            STREAMING_SCAN_BATCH,
            &self.cancellation,
            |batch| {
                for record in batch {
                    seen += 1;
                    if seen % 1024 == 0 {
                        if let Err(error) = self.check_cancellation() {
                            sql_error = Some(error);
                            return Ok(false);
                        }
                    }
                    let record = Arc::new(record);
                    if let Some(selection) = &select.selection {
                        let keep =
                            row_from_record(collection, alias, schema, &record).and_then(|row| {
                                self.eval_row_truth_typed(&row, selection, &predicate_env)
                            });
                        match keep {
                            Ok(verdict) => {
                                if !verdict.unwrap_or(false) {
                                    continue;
                                }
                            }
                            Err(error) => {
                                sql_error = Some(error);
                                return Ok(false);
                            }
                        }
                    }
                    let pushed = (|| -> Result<()> {
                        let mut key = Vec::new();
                        for driver in &drivers {
                            let value = driver.field.value(&record)?;
                            let bytes = if matches!(value, SqlValue::Null) {
                                None
                            } else {
                                Some(pg_typed_index_key_for_db(
                                    self.db_ref(),
                                    &driver.pg_type,
                                    &value,
                                )?)
                            };
                            crate::external_sort::push_normalized_component(
                                &mut key,
                                bytes.as_deref(),
                                driver.descending,
                                driver.nulls_first,
                            );
                        }
                        sorter.push(key, projection.row(&record)?)
                    })();
                    if let Err(error) = pushed {
                        sql_error = Some(error);
                        return Ok(false);
                    }
                }
                Ok(true)
            },
        )?;
        if let Some(error) = sql_error {
            if error.is_resource_limit() || error.is_query_interruption() {
                return Err(error);
            }
            return Ok(None);
        }
        if !streamed {
            return Ok(None);
        }
        let rows = match sorter.finish() {
            Ok(rows) => rows,
            // A spill-codec decline mid-stream abandons the attempt.
            Err(error) if error.is_resource_limit() || error.is_query_interruption() => {
                return Err(error);
            }
            Err(_) => return Ok(None),
        };
        sql_profile_full_scan();
        let column_types = projection.column_types();
        let column_metadata = projection.column_metadata(self.db_ref(), collection, schema);
        let mut result = SqlResult::new(projection.columns, rows)
            .with_column_types(column_types)
            .with_column_metadata(column_metadata);
        apply_limit(&mut result.rows, query)?;
        Ok(Some(result))
    }

    /// Simple aggregates over one plain table, folded per batch while the
    /// scan streams: `SELECT COUNT(*)/COUNT(x)/SUM(x)/MIN(x)/MAX(x)/
    /// BOOL_AND(x)/BOOL_OR(x) ... [WHERE ...]` with no GROUP BY. The batch
    /// fold reuses the SAME per-slice aggregate primitives as the
    /// materializing path (`record_aggregate_values`, `sum_aggregate_values`,
    /// `combine_extreme_value`, ...), so the two paths cannot drift; partials
    /// combine losslessly (a SUM partial re-enters the next batch's sum, the
    /// running extreme folds through `combine_extreme_value`). AVG is
    /// deliberately absent: its final division has type rules the partial
    /// pair (sum, count) does not reproduce exactly, so it falls back.
    ///
    /// Returning `None` always falls back to a path that is already correct.
    pub(crate) fn try_streaming_simple_aggregates(
        &self,
        from: &TableWithJoins,
        select: &Select,
        query: &Query,
    ) -> Result<Option<SqlResult>> {
        if !from.joins.is_empty()
            || select.having.is_some()
            || select.distinct.is_some()
            || has_group_by(select)?
            || query.order_by.is_some()
            || select_has_window_functions(select, query)?
            || !has_aggregates(&select.projection)
        {
            return Ok(None);
        }
        if self.security_context.is_some() || self.tx.is_some() {
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
        if self.cte(&collection).is_some()
            || is_virtual_table(&collection)
            || load_view(self.db_ref(), &collection)?.is_some()
        {
            return Ok(None);
        }
        let schema = load_schema(self.db_ref(), &collection)?;
        if schema
            .as_ref()
            .is_some_and(|schema| schema.rls_enabled || schema.rls_forced)
        {
            return Ok(None);
        }
        // Only a full scan streams; index plans materialize just their
        // matches, which is already bounded by selectivity.
        let plan = self.plan_query(&collection, &alias_name, select, query)?;
        if !matches!(plan.kind, PlanKind::FullScan) {
            return Ok(None);
        }

        // Each projection item must be a bare aggregate this fold supports.
        enum Fold {
            CountAll(i64),
            CountField(FieldRef, i64),
            Sum {
                field: FieldRef,
                money: bool,
                partial: Option<SqlValue>,
            },
            Extreme {
                field: FieldRef,
                pg_type: Option<String>,
                greatest: bool,
                best: SqlValue,
            },
            Bool {
                field: FieldRef,
                and: bool,
                state: Option<bool>,
            },
            Avg {
                field: FieldRef,
                state: crate::eval::AverageState,
            },
        }
        let mut folds: Vec<(Fold, Option<String>, &Expr)> = Vec::new();
        for item in &select.projection {
            let (expr, item_alias) = match item {
                SelectItem::UnnamedExpr(expr) => (expr, None),
                SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
                _ => return Ok(None),
            };
            let Expr::Function(function) = expr else {
                return Ok(None);
            };
            if function.filter.is_some() || function.over.is_some() {
                return Ok(None);
            }
            if !is_aggregate_function(function) {
                return Ok(None);
            }
            let fold = match crate::eval::Aggregate::from_function(function, schema.as_ref())? {
                crate::eval::Aggregate::CountAll => Fold::CountAll(0),
                crate::eval::Aggregate::CountField(field, false) => Fold::CountField(field, 0),
                crate::eval::Aggregate::Sum(field, false, money) => Fold::Sum {
                    field,
                    money,
                    partial: None,
                },
                crate::eval::Aggregate::Min(field, pg_type) => Fold::Extreme {
                    field,
                    pg_type,
                    greatest: false,
                    best: SqlValue::Null,
                },
                crate::eval::Aggregate::Max(field, pg_type) => Fold::Extreme {
                    field,
                    pg_type,
                    greatest: true,
                    best: SqlValue::Null,
                },
                crate::eval::Aggregate::BoolAnd(field, false) => Fold::Bool {
                    field,
                    and: true,
                    state: None,
                },
                crate::eval::Aggregate::BoolOr(field, false) => Fold::Bool {
                    field,
                    and: false,
                    state: None,
                },
                crate::eval::Aggregate::Avg(field, false) => Fold::Avg {
                    field,
                    state: crate::eval::AverageState::new(),
                },
                _ => return Ok(None),
            };
            folds.push((fold, item_alias, expr));
        }
        if folds.is_empty() {
            return Ok(None);
        }

        let predicate_env = self.from_relation_columns(&select.from).unwrap_or_default();
        let mut sql_error: Option<SqlError> = None;
        let mut seen = 0usize;
        let streamed = self.db_ref().for_each_record_batch_cancellable(
            &collection,
            STREAMING_SCAN_BATCH,
            &self.cancellation,
            |batch| {
                let mut matching: Vec<Record> = Vec::new();
                for record in batch {
                    seen += 1;
                    if seen % 1024 == 0 {
                        if let Err(error) = self.check_cancellation() {
                            sql_error = Some(error);
                            return Ok(false);
                        }
                    }
                    if let Some(selection) = &select.selection {
                        let record = Arc::new(record);
                        let keep =
                            row_from_record(&collection, &alias_name, schema.as_ref(), &record)
                                .and_then(|row| {
                                    self.eval_row_truth_typed(&row, selection, &predicate_env)
                                });
                        match keep {
                            Ok(verdict) => {
                                if !verdict.unwrap_or(false) {
                                    continue;
                                }
                            }
                            Err(error) => {
                                sql_error = Some(error);
                                return Ok(false);
                            }
                        }
                        matching.push(
                            Arc::try_unwrap(record).unwrap_or_else(|record| (*record).clone()),
                        );
                    } else {
                        matching.push(record);
                    }
                }
                if matching.is_empty() {
                    return Ok(true);
                }
                for (fold, _, _) in &mut folds {
                    let folded: Result<()> = (|| {
                        match fold {
                            Fold::CountAll(count) => *count += matching.len() as i64,
                            Fold::CountField(field, count) => {
                                *count +=
                                    crate::eval::record_aggregate_count(&matching, field, false)?
                                        as i64;
                            }
                            Fold::Sum {
                                field,
                                money,
                                partial,
                            } => {
                                let values = crate::eval::record_aggregate_values(
                                    &matching, field, false, false,
                                )?;
                                let chained = partial.take().into_iter().chain(values.into_iter());
                                let sum = if *money {
                                    crate::eval::sum_money_aggregate_values(
                                        chained.collect::<Vec<_>>(),
                                    )?
                                } else {
                                    crate::eval::sum_aggregate_values(chained)?
                                };
                                if !matches!(sum, SqlValue::Null) {
                                    *partial = Some(sum);
                                }
                            }
                            Fold::Extreme {
                                field,
                                pg_type,
                                greatest,
                                best,
                            } => {
                                let batch_best = crate::eval::extreme_record_value(
                                    &matching,
                                    field,
                                    pg_type.as_deref(),
                                    *greatest,
                                )?;
                                let running = std::mem::replace(best, SqlValue::Null);
                                *best = crate::eval::combine_extreme_value(
                                    running,
                                    batch_best,
                                    pg_type.as_deref(),
                                    *greatest,
                                )?;
                            }
                            Fold::Bool { field, and, state } => {
                                let batch = crate::eval::bool_aggregate_values(
                                    crate::eval::record_aggregate_values(
                                        &matching, field, false, false,
                                    )?,
                                    *and,
                                )?;
                                if let SqlValue::Bool(batch) = batch {
                                    *state = Some(match state {
                                        Some(previous) => {
                                            if *and {
                                                *previous && batch
                                            } else {
                                                *previous || batch
                                            }
                                        }
                                        None => batch,
                                    });
                                }
                            }
                            Fold::Avg { field, state } => {
                                let values = crate::eval::record_aggregate_values(
                                    &matching, field, false, false,
                                )?;
                                for value in values {
                                    state.fold_value(value)?;
                                }
                            }
                        }
                        Ok(())
                    })();
                    if let Err(error) = folded {
                        sql_error = Some(error);
                        return Ok(false);
                    }
                }
                Ok(true)
            },
        )?;
        if sql_error.is_some() || !streamed {
            return Ok(None);
        }
        sql_profile_full_scan();

        let mut columns = Vec::new();
        let mut row = Vec::new();
        for (fold, item_alias, expr) in folds {
            columns
                .push(item_alias.unwrap_or_else(|| aggregate_column_name(expr, schema.as_ref())));
            row.push(match fold {
                Fold::CountAll(count) | Fold::CountField(_, count) => SqlValue::Int(count),
                Fold::Sum { partial, .. } => partial.unwrap_or(SqlValue::Null),
                Fold::Extreme { best, .. } => best,
                Fold::Bool { state, .. } => state.map(SqlValue::Bool).unwrap_or(SqlValue::Null),
                Fold::Avg { state, .. } => state.finish()?,
            });
        }
        let column_types = aggregate_projection_column_types(&select.projection, schema.as_ref());
        let mut result = validate_integer_result_types(
            SqlResult::new(columns, vec![row]).with_column_types(column_types),
        )?;
        apply_limit(&mut result.rows, query)?;
        Ok(Some(result))
    }

    /// declines anything [`Self::try_streaming_projection`] cannot serve.
    ///
    /// Returning `None` always falls back to a path that is already correct, so
    /// a shape this does not understand can never lose rows.
    pub(crate) fn try_streaming_row_query(
        &self,
        from: &TableWithJoins,
        select: &Select,
        query: &Query,
    ) -> Result<Option<SqlResult>> {
        // Everything the streaming projection genuinely cannot do. A
        // row-evaluator *selection* is deliberately absent: that is the case
        // this exists to rescue.
        if !from.joins.is_empty()
            || has_group_by(select)?
            || select
                .projection
                .iter()
                .any(select_item_needs_row_evaluator)
            || select
                .projection
                .iter()
                .any(|item| select_item_is_whole_row_reference(item, &from.relation))
            || query.order_by.is_some()
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
        // A virtual/system table has no paged collection to stream.
        if is_virtual_table(&collection) || load_view(self.db_ref(), &collection)?.is_some() {
            return Ok(None);
        }
        let schema = load_schema(self.db_ref(), &collection)?;
        let plan = self.plan_query(&collection, &alias_name, select, query)?;
        self.try_streaming_projection(
            &collection,
            &alias_name,
            select,
            query,
            &plan,
            schema.as_ref(),
        )
    }

    /// One-pass projection over a streamed full scan, or `None` when the query
    /// shape or storage mode cannot support it (the caller then materializes).
    ///
    /// Returning `None` rather than a partial result is deliberate: every
    /// bail-out here must fall back to a path that is already correct, so a
    /// shape this does not understand can never silently lose rows.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_streaming_projection(
        &self,
        collection: &str,
        alias: &str,
        select: &Select,
        query: &Query,
        plan: &QueryPlan,
        schema: Option<&TableSchema>,
    ) -> Result<Option<SqlResult>> {
        if !matches!(plan.kind, PlanKind::FullScan) {
            return Ok(None);
        }
        // Shapes that need the whole input before emitting anything.
        if query.order_by.is_some()
            || has_aggregates(&select.projection)
            || select.distinct.is_some()
            || has_group_by(select)?
            || select.having.is_some()
        {
            return Ok(None);
        }
        // Row-level security and a transaction's own pending writes both need
        // the paths the materializing route already takes. RLS must be checked
        // on the SCHEMA, not just the session: `scan_records` filters through
        // the table's policies even when no security context is set, so a
        // streaming path gated only on the context would return unfiltered
        // rows exactly when the table asked for filtering (B2 in
        // IMPORTANT-TODO.md).
        if self.security_context.is_some() || self.tx.is_some() {
            return Ok(None);
        }
        if schema
            .as_ref()
            .is_some_and(|schema| schema.rls_enabled || schema.rls_forced)
        {
            return Ok(None);
        }
        let (offset, limit) = limit_offset_bounds(query)?;
        let projection = Projection::from_select_items(&select.projection, schema)?;
        let predicate_env = self.from_relation_columns(&select.from).unwrap_or_default();
        let mut rows: Vec<Vec<SqlValue>> = Vec::new();
        let mut skipped = 0usize;
        let mut seen = 0usize;
        // The storage callback speaks `bicdb_core`'s error type, so SQL errors
        // travel out of band and are re-raised below rather than being
        // flattened into a storage error.
        let mut sql_error: Option<SqlError> = None;
        let streamed = self.db_ref().for_each_record_batch_cancellable(
            collection,
            STREAMING_SCAN_BATCH,
            &self.cancellation,
            |batch| {
                for record in batch {
                    seen += 1;
                    if seen % 1024 == 0 {
                        if let Err(error) = self.check_cancellation() {
                            sql_error = Some(error);
                            return Ok(false);
                        }
                    }
                    let record = Arc::new(record);
                    if let Some(selection) = &select.selection {
                        let keep =
                            row_from_record(collection, alias, schema, &record).and_then(|row| {
                                self.eval_row_truth_typed(&row, selection, &predicate_env)
                            });
                        match keep {
                            Ok(verdict) => {
                                if !verdict.unwrap_or(false) {
                                    continue;
                                }
                            }
                            Err(error) => {
                                sql_error = Some(error);
                                return Ok(false);
                            }
                        }
                    }
                    if skipped < offset {
                        skipped += 1;
                        continue;
                    }
                    match projection.row(&record) {
                        Ok(row) => rows.push(row),
                        Err(error) => {
                            sql_error = Some(error);
                            return Ok(false);
                        }
                    }
                    if limit.is_some_and(|limit| rows.len() >= limit) {
                        return Ok(false);
                    }
                }
                Ok(true)
            },
        )?;
        // An error here means this path could not evaluate the query — most
        // often a predicate the row evaluator handles and this one does not
        // (subqueries, correlated outer references, LATERAL). Abandon the
        // streamed result and let the caller materialize rather than
        // propagating: the general path either answers correctly or raises the
        // same error itself.
        //
        // Detecting incapability beats enumerating it. The first attempt at
        // this guarded on a list of shapes I believed the streaming path could
        // not serve, and the list was wrong in thirteen ways — every one a
        // query that silently took the streaming path and failed. A capability
        // that reports itself cannot be out of date.
        //
        // Safe because nothing has been emitted yet and predicate evaluation
        // has no side effects; the cost is a partial scan repeated.
        if sql_error.is_some() || !streamed {
            return Ok(None);
        }
        sql_profile_full_scan();
        let column_types = projection.column_types();
        let column_metadata = projection.column_metadata(self.db_ref(), collection, schema);
        Ok(Some(
            SqlResult::new(projection.columns, rows)
                .with_column_types(column_types)
                .with_column_metadata(column_metadata),
        ))
    }

    pub(crate) fn load_records_for_plan(
        &self,
        collection: &str,
        plan: &QueryPlan,
    ) -> Result<Vec<Arc<Record>>> {
        self.check_cancellation()?;
        let records = match &plan.kind {
            PlanKind::PrimaryKeyLookup { record_id } => {
                sql_profile_index_lookup();
                Ok(self
                    .get_record(collection, record_id)?
                    .into_iter()
                    .collect::<Vec<_>>())
            }
            PlanKind::PrimaryKeyInLookup { record_ids } => {
                sql_profile_index_lookup();
                self.records_for_ids(collection, record_ids)
            }
            PlanKind::PrimaryKeyPrefixLookup {
                prefix,
                prefix_values,
            } => {
                sql_profile_index_lookup();
                let schema = load_schema(self.db_ref(), collection)?;
                let Some(schema) = schema.as_ref() else {
                    sql_profile_record_id_prefix_scan();
                    let ids = self.scan_record_ids_with_prefix(collection, prefix)?;
                    return self.records_for_ids(collection, &ids);
                };
                let ids =
                    self.primary_key_prefix_record_ids(collection, schema, prefix_values, prefix)?;
                self.records_for_ids(collection, &ids)
            }
            PlanKind::IndexLookup { index_name, prefix } => {
                sql_profile_index_lookup();
                let ids = self.lookup_index_cached(index_name, prefix.as_slice())?;
                let ids =
                    if let Some(index) = self.index_definition_for_scan(collection, index_name) {
                        self.record_ids_with_pending_index_lookup_candidates(
                            collection,
                            &index,
                            prefix.as_slice(),
                            ids,
                        )?
                    } else {
                        self.record_ids_with_pending_candidates(collection, ids)
                    };
                self.records_for_ids(collection, &ids)
            }
            PlanKind::IndexRange {
                index_name,
                lower,
                upper,
            } => {
                sql_profile_index_lookup();
                let ids: Rc<[String]> = Rc::from(self.db_ref().range_index(
                    index_name,
                    lower.as_ref(),
                    upper.as_ref(),
                )?);
                let ids =
                    if let Some(index) = self.index_definition_for_scan(collection, index_name) {
                        self.record_ids_with_pending_index_range_candidates(
                            collection,
                            &index,
                            &[],
                            lower.as_ref(),
                            upper.as_ref(),
                            &[],
                            ids,
                        )?
                    } else {
                        self.record_ids_with_pending_candidates(collection, ids)
                    };
                self.records_for_ids(collection, &ids)
            }
            PlanKind::OrderedIndexScan {
                index_name,
                descending,
                limit,
            } => {
                sql_profile_index_lookup();
                if self.tx.is_some_and(|tx| tx.write_len() > 0) {
                    sql_profile_full_scan();
                    return self.scan_records(collection);
                }
                let ids = self
                    .db_ref()
                    .ordered_index_records(index_name, *descending, *limit)?;
                self.records_for_ids(collection, &ids)
            }
            PlanKind::IndexPrefixOrderedScan {
                index_name,
                prefix,
                descending,
                limit,
            } => {
                sql_profile_index_lookup();
                if self.tx.is_some_and(|tx| tx.write_len() > 0) {
                    sql_profile_full_scan();
                    return self.scan_records(collection);
                }
                let ids = self.db_ref().prefix_ordered_index_records(
                    index_name,
                    prefix.as_slice(),
                    *descending,
                    *limit,
                )?;
                self.records_for_ids(collection, &ids)
            }
            PlanKind::SpatialIndexScan {
                index_name,
                predicate,
            } => {
                sql_profile_index_lookup();
                let ids = match predicate {
                    SpatialIndexPredicate::DWithin { lon, lat, meters } => self
                        .db_ref()
                        .spatial_radius_index(index_name, *lon, *lat, *meters)?,
                    SpatialIndexPredicate::IntersectsEnvelope {
                        min_lon,
                        min_lat,
                        max_lon,
                        max_lat,
                    } => self.db_ref().spatial_intersects_index(
                        index_name, *min_lon, *min_lat, *max_lon, *max_lat,
                    )?,
                };
                let ids = self.record_ids_with_pending_candidates(collection, Rc::from(ids));
                self.records_for_ids(collection, &ids)
            }
            PlanKind::GeometricIndexScan {
                index_name,
                envelope,
                ..
            } => {
                sql_profile_index_lookup();
                let ids = self.db_ref().spatial_intersects_index(
                    index_name,
                    envelope[0],
                    envelope[1],
                    envelope[2],
                    envelope[3],
                )?;
                let ids = self.record_ids_with_pending_candidates(collection, Rc::from(ids));
                self.records_for_ids(collection, &ids)
            }
            PlanKind::GeometricKnnIndexScan {
                index_name,
                point,
                limit,
                ..
            } => {
                sql_profile_index_lookup();
                let ids = self
                    .db_ref()
                    .spatial_nearest_envelope_index(index_name, point.0, point.1, *limit)?;
                let ids = self.record_ids_with_pending_candidates(collection, Rc::from(ids));
                self.records_for_ids(collection, &ids)
            }
            PlanKind::FullTextIndexScan { ids, .. } => {
                sql_profile_index_lookup();
                self.records_for_ids(collection, &ids.iter().cloned().collect::<Vec<_>>())
            }
            PlanKind::JsonbIndexScan {
                index_name,
                candidate,
            } => {
                sql_profile_index_lookup();
                let ids = self.jsonb_candidate_ids(index_name, candidate)?;
                self.records_for_ids(collection, &ids.into_iter().collect::<Vec<_>>())
            }
            PlanKind::ArrayIndexScan {
                index_name,
                candidate,
            } => {
                sql_profile_index_lookup();
                let ids = self.array_candidate_ids(index_name, candidate)?;
                self.records_for_ids(collection, &ids.into_iter().collect::<Vec<_>>())
            }
            PlanKind::FullScan => {
                sql_profile_full_scan();
                self.scan_records(collection)
            }
        }?;
        sql_profile_records_materialized(&records);
        Ok(records)
    }

    pub(crate) fn records_for_ids(
        &self,
        collection: &str,
        ids: &[String],
    ) -> Result<Vec<Arc<Record>>> {
        let schema = load_schema(self.db_ref(), collection)?;
        self.records_for_ids_with_schema(collection, ids, schema.as_ref())
    }

    pub(crate) fn records_for_ids_with_schema(
        &self,
        collection: &str,
        ids: &[String],
        schema: Option<&TableSchema>,
    ) -> Result<Vec<Arc<Record>>> {
        #[cfg(test)]
        SQL_RECORDS_FOR_IDS_CALLS.with(|calls| *calls.borrow_mut() += 1);

        let mut records = Vec::new();
        for (idx, id) in ids.iter().enumerate() {
            if idx % 1024 == 0 {
                self.check_cancellation()?;
            }
            if let Some(record) = self.get_record_with_schema(collection, id, schema)? {
                records.push(record);
            }
        }
        Ok(records)
    }

    pub(crate) fn execute_select_without_from(&self, select: &Select) -> Result<SqlResult> {
        if let Some(result) = self.execute_projection_array_set_functions(select)? {
            return Ok(result);
        }
        if let Some(result) = self.execute_projection_json_set_function(select)? {
            return Ok(result);
        }
        let include_row = match &select.selection {
            Some(selection) => {
                sql_value_truth(self.eval_select_constant_expr(selection)?)?.unwrap_or(false)
            }
            None => true,
        };
        let mut columns = Vec::new();
        let mut column_types = Vec::new();
        let mut row = Vec::new();
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    columns.push(select_expr_column_name(expr));
                    column_types.push(projected_expr_pg_type_with_db(self.db_ref(), expr));
                    if include_row {
                        row.push(self.eval_select_constant_expr(expr)?);
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    columns.push(alias.value.clone());
                    column_types.push(projected_expr_pg_type_with_db(self.db_ref(), expr));
                    if include_row {
                        row.push(self.eval_select_constant_expr(expr)?);
                    }
                }
                other => {
                    return Err(SqlError::Unsupported(format!(
                        "unsupported SELECT without FROM expression {other}"
                    )));
                }
            }
        }
        let rows = if include_row { vec![row] } else { Vec::new() };
        validate_integer_result_types(SqlResult::new(columns, rows).with_column_types(column_types))
    }
}
