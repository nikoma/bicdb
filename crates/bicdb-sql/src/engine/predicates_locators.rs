//! Split out of the parent module to keep files digestible; behavior
//! unchanged — a separate `impl` block on the same type.
use super::*;
#[allow(unused_imports)]
use crate::*;
use bicdb_core::VisibleRow;

impl<'db> SqlEngine<'db> {
    pub(crate) fn predicate_references_only_table_or_outer(
        &self,
        expr: &Expr,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
    ) -> Result<bool> {
        let mut references = Vec::new();
        if !collect_predicate_column_references(expr, &mut references) {
            return Ok(false);
        }
        let mut saw_table_reference = false;
        for reference in references {
            if target_record_field_from_parts(table, alias, schema, &reference).is_some() {
                saw_table_reference = true;
                continue;
            }
            if self
                .outer_row
                .as_ref()
                .is_some_and(|row| row.value_from_parts(&reference).is_some())
            {
                continue;
            }
            return Ok(false);
        }
        Ok(saw_table_reference)
    }

    pub(crate) fn plan_row_join_order(
        &self,
        from: &TableWithJoins,
        selection: Option<&Expr>,
    ) -> Result<(TableFactor, Vec<Join>)> {
        // Join ordering is a pure reordering of the statement's own AST nodes:
        // a stale entry can only cost performance, never correctness, so the
        // memo needs no catalog invalidation — only key equality. Routine IR
        // re-executes the same embedded statements millions of times per run
        // in procedure-heavy workloads, and the greedy cross-join planner re-walks the WHERE clause
        // and index catalog per relation per pass, which dominated profiles at
        // ~8% before this memo.
        if let Some(plan) = join_plan_memo_get(from, selection) {
            return Ok(plan);
        }
        let plan = self.plan_row_join_order_uncached(from, selection)?;
        join_plan_memo_insert(from, selection, &plan);
        Ok(plan)
    }

    pub(crate) fn plan_row_join_order_uncached(
        &self,
        from: &TableWithJoins,
        selection: Option<&Expr>,
    ) -> Result<(TableFactor, Vec<Join>)> {
        if let Some(plan) = self.plan_cross_join_order(from, selection)? {
            return Ok(plan);
        }
        if from.joins.len() != 1 || !is_reorderable_inner_join(&from.joins[0]) {
            return Ok((from.relation.clone(), from.joins.clone()));
        }
        if json_set_returning_call(&from.relation)?.is_some()
            || json_set_returning_call(&from.joins[0].relation)?.is_some()
        {
            return Ok((from.relation.clone(), from.joins.clone()));
        }

        let left_rows = self.estimated_table_factor_rows(&from.relation)?;
        let right_rows = self.estimated_table_factor_rows(&from.joins[0].relation)?;
        if right_rows < left_rows {
            let mut join = from.joins[0].clone();
            join.relation = from.relation.clone();
            return Ok((from.joins[0].relation.clone(), vec![join]));
        }
        Ok((from.relation.clone(), from.joins.clone()))
    }

    pub(crate) fn plan_cross_join_order(
        &self,
        from: &TableWithJoins,
        selection: Option<&Expr>,
    ) -> Result<Option<(TableFactor, Vec<Join>)>> {
        // (memoized by plan_row_join_order)
        let Some(selection) = selection else {
            return Ok(None);
        };
        if from.joins.is_empty()
            || !from.joins.iter().all(|join| {
                matches!(
                    join.join_operator,
                    JoinOperator::CrossJoin(JoinConstraint::None)
                )
            })
        {
            return Ok(None);
        }
        let mut remaining = Vec::with_capacity(from.joins.len() + 1);
        remaining.push(from.relation.clone());
        remaining.extend(from.joins.iter().map(|join| join.relation.clone()));
        if !remaining.iter().all(simple_reorderable_table_factor) {
            return Ok(None);
        }
        let initial_available_columns = BTreeSet::new();
        let has_bound_start = remaining.iter().try_fold(false, |found, relation| {
            let bound_terms = self.bound_where_term_count_for_relation(
                selection,
                relation,
                &initial_available_columns,
            )?;
            let index_bound_fields = self.usable_index_bound_count_for_relation(
                selection,
                relation,
                &initial_available_columns,
            )?;
            Ok::<bool, SqlError>(found || bound_terms > 0 || index_bound_fields > 0)
        })?;
        if !has_bound_start {
            return Ok(None);
        }

        let mut available_columns = BTreeSet::new();
        let mut ordered = Vec::with_capacity(remaining.len());
        while !remaining.is_empty() {
            let mut best_idx = 0usize;
            let mut best_index_bound_fields = 0usize;
            let mut best_bound_terms = 0usize;
            let mut best_rows = usize::MAX;
            for (idx, relation) in remaining.iter().enumerate() {
                let index_bound_fields = self.usable_index_bound_count_for_relation(
                    selection,
                    relation,
                    &available_columns,
                )?;
                let bound_terms = self.bound_where_term_count_for_relation(
                    selection,
                    relation,
                    &available_columns,
                )?;
                let estimated_rows = self.estimated_table_factor_rows(relation)?;
                if sql_trace_flags().plan {
                    eprintln!(
                        "bicdb_trace_plan join_candidate relation={} index_bound_fields={} bound_terms={} estimated_rows={}",
                        table_factor_trace_name(relation),
                        index_bound_fields,
                        bound_terms,
                        estimated_rows
                    );
                }
                if index_bound_fields > best_index_bound_fields
                    || (index_bound_fields == best_index_bound_fields
                        && (bound_terms > best_bound_terms
                            || (bound_terms == best_bound_terms && estimated_rows < best_rows)))
                {
                    best_idx = idx;
                    best_index_bound_fields = index_bound_fields;
                    best_bound_terms = bound_terms;
                    best_rows = estimated_rows;
                }
            }
            let relation = remaining.remove(best_idx);
            if sql_trace_flags().plan {
                eprintln!(
                    "bicdb_trace_plan join_select position={} relation={} index_bound_fields={} bound_terms={} estimated_rows={}",
                    ordered.len(),
                    table_factor_trace_name(&relation),
                    best_index_bound_fields,
                    best_bound_terms,
                    best_rows
                );
            }
            if let Some(columns) = self.row_set_columns_from_table_factor_without_rows(&relation)? {
                available_columns.extend(available_join_columns(&columns));
            }
            ordered.push(relation);
        }

        let relation = ordered.remove(0);
        let joins = ordered
            .into_iter()
            .map(|relation| Join {
                relation,
                global: false,
                join_operator: JoinOperator::CrossJoin(JoinConstraint::None),
            })
            .collect();
        Ok(Some((relation, joins)))
    }

    pub(crate) fn bound_where_term_count_for_relation(
        &self,
        selection: &Expr,
        relation: &TableFactor,
        available_columns: &BTreeSet<String>,
    ) -> Result<usize> {
        let TableFactor::Table {
            name,
            alias,
            args: None,
            ..
        } = relation
        else {
            return Ok(0);
        };
        let table = relation_name(name)?;
        let alias_name = alias
            .as_ref()
            .map(|alias| alias.name.value.clone())
            .unwrap_or_else(|| table.rsplit('.').next().unwrap_or(&table).to_string());
        let table = resolve_session_relation_name_if_exists(self.db_ref(), &table);
        let schema = load_schema_shared(self.db_ref(), &table)?;
        let mut count = 0usize;
        for term in and_terms(selection) {
            let Expr::BinaryOp {
                op: BinaryOperator::Eq,
                ..
            } = unwrap_nested_expr(term)
            else {
                continue;
            };
            if self.where_term_applies_to_right_table(
                term,
                &table,
                &alias_name,
                schema.as_deref(),
                available_columns,
            )? {
                count += 1;
            }
        }
        Ok(count)
    }

    pub(crate) fn usable_index_bound_count_for_relation(
        &self,
        selection: &Expr,
        relation: &TableFactor,
        available_columns: &BTreeSet<String>,
    ) -> Result<usize> {
        let TableFactor::Table {
            name,
            alias,
            args: None,
            ..
        } = relation
        else {
            return Ok(0);
        };
        let table = relation_name(name)?;
        let alias_name = alias
            .as_ref()
            .map(|alias| alias.name.value.clone())
            .unwrap_or_else(|| table.rsplit('.').next().unwrap_or(&table).to_string());
        let table = resolve_session_relation_name_if_exists(self.db_ref(), &table);
        let schema = load_schema_shared(self.db_ref(), &table)?;
        let mut best = 0usize;
        if let Some(schema) = schema.as_deref() {
            let primary_key_fields = primary_key_columns_for_schema(schema)
                .into_iter()
                .map(|column| IndexField::MetadataPath(vec![column]))
                .collect::<Vec<_>>();
            best = best.max(self.usable_index_field_prefix_count(
                selection,
                &table,
                &alias_name,
                Some(schema),
                &primary_key_fields,
                available_columns,
                true,
            )?);
        }
        for index in sql_index_definitions_for_collection(self.db_ref(), &table) {
            if index.kind != IndexKind::BTree {
                continue;
            }
            best = best.max(self.usable_index_field_prefix_count(
                selection,
                &table,
                &alias_name,
                schema.as_deref(),
                &index.fields,
                available_columns,
                true,
            )?);
        }
        Ok(best)
    }

    pub(crate) fn usable_index_field_prefix_count(
        &self,
        selection: &Expr,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        fields: &[IndexField],
        available_columns: &BTreeSet<String>,
        allow_range_bound: bool,
    ) -> Result<usize> {
        let mut bound_fields = 0usize;
        for field in fields {
            if self.index_field_has_available_bound(
                selection,
                table,
                alias,
                schema,
                field,
                available_columns,
                IndexBoundMatch::Equality,
            )? {
                bound_fields += 1;
                continue;
            }
            if allow_range_bound
                && self.index_field_has_available_bound(
                    selection,
                    table,
                    alias,
                    schema,
                    field,
                    available_columns,
                    IndexBoundMatch::Range,
                )?
            {
                bound_fields += 1;
            }
            break;
        }
        Ok(bound_fields)
    }

    pub(crate) fn index_field_has_available_bound(
        &self,
        selection: &Expr,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        field: &IndexField,
        available_columns: &BTreeSet<String>,
        bound_match: IndexBoundMatch,
    ) -> Result<bool> {
        for term in and_terms(selection) {
            let Expr::BinaryOp { left, op, right } = term else {
                continue;
            };
            if !bound_match.matches(op) {
                continue;
            }
            if self.expr_matches_table_index_field(left, table, alias, schema, field)?
                && !self.expr_references_table(right, table, alias, schema)?
                && self.expr_is_available_from_join_inputs(
                    right,
                    table,
                    alias,
                    schema,
                    available_columns,
                )?
            {
                return Ok(true);
            }
            if self.expr_matches_table_index_field(right, table, alias, schema, field)?
                && !self.expr_references_table(left, table, alias, schema)?
                && self.expr_is_available_from_join_inputs(
                    left,
                    table,
                    alias,
                    schema,
                    available_columns,
                )?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(crate) fn estimated_table_factor_rows(&self, relation: &TableFactor) -> Result<usize> {
        if json_set_returning_call(relation)?.is_some() {
            // The cardinality depends on the argument and may be correlated.
            // Keep it above ordinary table estimates so join planning preserves
            // the function's position after its input relation.
            return Ok(usize::MAX / 2);
        }
        if let TableFactor::NestedJoin {
            table_with_joins, ..
        } = relation
        {
            let left_rows = self.estimated_table_factor_rows(&table_with_joins.relation)?;
            let join_rows = table_with_joins
                .joins
                .iter()
                .map(|join| self.estimated_table_factor_rows(&join.relation))
                .collect::<Result<Vec<_>>>()?;
            return Ok(join_rows
                .into_iter()
                .fold(left_rows, |estimate, rows| estimate.min(rows)));
        }
        if matches!(relation, TableFactor::Derived { .. }) {
            return Ok(usize::MAX / 2);
        }
        if let TableFactor::UNNEST { array_exprs, .. } = relation {
            return self.estimate_unnest_rows(array_exprs);
        }
        let TableFactor::Table { name, args, .. } = relation else {
            return Ok(usize::MAX);
        };
        let table = relation_name(name)?;
        if let Some(args) = args {
            if is_generate_series_table_function(&table) {
                return estimate_generate_series_rows(args);
            }
            if is_one_row_catalog_table_function(&table) {
                validate_zero_arg_table_function(&table, args)?;
                return Ok(1);
            }
            return Ok(usize::MAX / 2);
        }
        let table = resolve_session_relation_name_if_exists(self.db_ref(), &table);
        if let Some(stats) = self.db_ref().table_statistics(&table) {
            return Ok(stats.row_count);
        }
        if self.cte(&table).is_some() || load_view(self.db_ref(), &table)?.is_some() {
            return Ok(usize::MAX / 2);
        }
        if is_virtual_table(&table) || load_sequence(self.db_ref(), &table)?.is_some() {
            return Ok(1);
        }
        Ok(self
            .db_ref()
            .collection_record_count_cancellable(&table, &self.cancellation)?)
    }

    pub(crate) fn row_set_from_table_factor_with_selection(
        &self,
        relation: &TableFactor,
        selection: Option<&Expr>,
        needed_columns: Option<&ReferencedColumns>,
    ) -> Result<RowSet> {
        if let Some(call) = json_set_returning_call(relation)? {
            return self.json_set_function_row_set(&call);
        }
        if let TableFactor::NestedJoin {
            table_with_joins,
            alias,
        } = relation
        {
            if alias.is_some() {
                return Err(SqlError::Unsupported(
                    "aliases on nested joins are not supported".to_string(),
                ));
            }
            return self.row_set_from_table_with_joins(table_with_joins);
        }
        if let TableFactor::Derived {
            lateral,
            subquery,
            alias,
            ..
        } = relation
        {
            if *lateral {
                return Err(SqlError::Unsupported(
                    "LATERAL derived tables are not supported".to_string(),
                ));
            }
            let alias_name = alias
                .as_ref()
                .map(|alias| alias.name.value.clone())
                .unwrap_or_else(|| "subquery".to_string());
            let result = self.execute_query(subquery)?;
            let columns = table_alias_columns(
                &alias_name,
                &alias_name,
                alias
                    .as_ref()
                    .map(|alias| alias.columns.as_slice())
                    .unwrap_or(&[]),
                &result.columns,
            )?;
            let rows = result
                .rows
                .iter()
                .enumerate()
                .map(|(idx, row)| {
                    if idx % 1024 == 0 {
                        self.check_cancellation()?;
                    }
                    Ok(slot_row_from_values(&columns, row))
                })
                .collect::<Result<Vec<_>>>()?;
            sql_profile_sql_rows_materialized(&rows);
            return Ok(RowSet {
                rows,
                columns: aliased_row_output_columns(&alias_name, &columns),
            });
        }
        if let TableFactor::UNNEST {
            alias,
            array_exprs,
            with_offset,
            with_offset_alias,
            with_ordinality,
        } = relation
        {
            return self.unnest_row_set(
                alias.as_ref(),
                array_exprs,
                *with_offset,
                with_offset_alias.as_ref(),
                *with_ordinality,
            );
        }
        let TableFactor::Table {
            name,
            alias,
            args,
            with_ordinality,
            ..
        } = relation
        else {
            return Err(SqlError::Unsupported(
                "joins and grouped queries support only table references".to_string(),
            ));
        };
        // A TableFactor carrying arguments is a function reference, not a
        // relation reference. Routine catalog names remain logically schema
        // qualified; relation_name() would instead encode a non-public schema
        // as an internal __bicdb_s_* collection name.
        let table = if args.is_some() {
            normalize_object_name(&object_name(name)?)
        } else {
            relation_name(name)?
        };
        let table_alias = alias.as_ref();
        let alias_name = table_alias
            .as_ref()
            .map(|alias| alias.name.value.clone())
            .unwrap_or_else(|| table.rsplit('.').next().unwrap_or(&table).to_string());

        if let Some(args) = args {
            if is_generate_series_table_function(&table) {
                return self.generate_series_row_set(&table, table_alias, args, *with_ordinality);
            }
            if is_regexp_split_to_table_function(&table) {
                return self.regexp_split_to_table_row_set(
                    &table,
                    table_alias,
                    args,
                    *with_ordinality,
                );
            }
            if is_one_row_catalog_table_function(&table) {
                return self.catalog_table_function_row_set(
                    &table,
                    table_alias,
                    args,
                    *with_ordinality,
                );
            }
            if let Some(row_set) = self.stored_routine_table_function_row_set(
                &table,
                table_alias,
                args,
                *with_ordinality,
            )? {
                return Ok(row_set);
            }
            return Err(SqlError::Unsupported(format!(
                "table function {table} is not supported"
            )));
        }

        if let Some(cte) = self.cte(&table) {
            let columns = table_alias_columns(
                &table,
                &alias_name,
                table_alias
                    .map(|alias| alias.columns.as_slice())
                    .unwrap_or(&[]),
                &cte.columns,
            )?;
            let rows = cte
                .rows
                .iter()
                .enumerate()
                .map(|(idx, row)| {
                    if idx % 1024 == 0 {
                        self.check_cancellation()?;
                    }
                    Ok(slot_row_from_values(&columns, row))
                })
                .collect::<Result<Vec<_>>>()?;
            sql_profile_sql_rows_materialized(&rows);
            let output_columns = aliased_row_output_columns(&alias_name, &columns);
            return Ok(RowSet {
                rows,
                columns: output_columns,
            });
        }

        if let Some(view) = load_view(self.db_ref(), &table)? {
            self.require_relation_privilege(&table, "SELECT")?;
            let cte = self.materialize_view_with_selection(&view, &alias_name, selection)?;
            let columns = table_alias_columns(
                &table,
                &alias_name,
                table_alias
                    .map(|alias| alias.columns.as_slice())
                    .unwrap_or(&[]),
                &cte.columns,
            )?;
            let rows = cte
                .rows
                .iter()
                .enumerate()
                .map(|(idx, row)| {
                    if idx % 1024 == 0 {
                        self.check_cancellation()?;
                    }
                    Ok(slot_row_from_values(&columns, row))
                })
                .collect::<Result<Vec<_>>>()?;
            sql_profile_sql_rows_materialized(&rows);
            let output_columns = aliased_row_output_columns(&alias_name, &columns);
            return Ok(RowSet {
                rows,
                columns: output_columns,
            });
        }

        if is_virtual_table(&table) {
            let source_rows =
                self.session_virtual_rows_with_selection(&table, &alias_name, selection)?;
            let mut source_columns = virtual_table_columns(&table).unwrap_or_else(|| {
                let mut columns = source_rows
                    .iter()
                    .flat_map(|row| row.keys().cloned())
                    .collect::<std::collections::BTreeSet<_>>();
                if virtual_catalog_table_oid(&table).is_some() {
                    columns.insert("tableoid".to_string());
                }
                columns.into_iter().collect()
            });
            add_virtual_tableoid_column(&table, &mut source_columns);
            let columns = source_columns
                .iter()
                .map(|column| format!("{alias_name}.{column}"))
                .collect::<Vec<_>>();
            let rows = source_rows
                .into_iter()
                .enumerate()
                .map(|(idx, source)| {
                    if idx % 1024 == 0 {
                        self.check_cancellation()?;
                    }
                    Ok(source_columns
                        .iter()
                        .map(|column| virtual_cell(&source, column))
                        .collect::<Vec<_>>())
                })
                .collect::<Result<Vec<_>>>()?;
            sql_profile_sql_rows_materialized(&rows);
            return Ok(RowSet { rows, columns });
        }

        if let Some(sequence) = load_sequence(self.db_ref(), &table)? {
            let role = current_user_from_gucs(&self.session_gucs);
            if !role_can_use_sequence(self.db_ref(), &role, &sequence, &["SELECT"])? {
                return Err(SqlError::BicDb(BicDbError::Authorization(format!(
                    "permission denied for sequence {}",
                    sequence.name
                ))));
            }
            let source = virtual_row([
                ("last_value", SqlValue::Int(sequence.last_value)),
                ("log_cnt", SqlValue::Int(0)),
                ("is_called", SqlValue::Bool(sequence.is_called)),
            ]);
            let source_columns = source.keys().cloned().collect::<Vec<_>>();
            let columns = source_columns
                .iter()
                .map(|column| format!("{alias_name}.{column}"))
                .collect::<Vec<_>>();
            let rows = vec![source_columns
                .iter()
                .map(|column| virtual_cell(&source, column))
                .collect::<Vec<_>>()];
            sql_profile_sql_rows_materialized(&rows);
            return Ok(RowSet { rows, columns });
        }

        let table = resolve_session_relation_name(self.db_ref(), &table)?;
        self.require_relation_privilege(&table, "SELECT")?;

        // Cell-row path: schema table, no RLS filter/policy, and the selection
        // resolves through an index — read each visible row's cells straight
        // from its resident JSON text (no `Value` tree, nothing cached), one
        // row at a time falling back to the `Record` form where a field cannot
        // be read from cells.
        if cell_rows_enabled() {
            if let Some(schema) = load_schema_shared(self.db_ref(), &table)?.as_deref() {
                if let Some(row_set) = self.cell_row_set_for_selection(
                    &table,
                    &alias_name,
                    schema,
                    selection,
                    needed_columns,
                )? {
                    return Ok(row_set);
                }
            }
        }

        // Typed-row fast path: schema table, rowid-eligible session state, no
        // RLS, and the selection resolves to rowid locators — build the slot
        // rows straight from each resident record's cached flat cells, never
        // materializing the metadata `Value` tree. Any record whose cells
        // cannot represent a field falls back to the `Record` form alone.
        if typed_rows_enabled() && self.rowid_fast_path_eligible(&table) {
            let schema = load_schema_shared(self.db_ref(), &table)?;
            if schema
                .as_ref()
                .is_some_and(|schema| !schema.rls_enabled && !schema.rls_forced)
            {
                if let Some(RecordLocators::Rowids(rowids)) = self
                    .indexed_record_locators_for_table_selection(
                        &table,
                        &alias_name,
                        schema.as_deref(),
                        selection,
                    )?
                {
                    let stored = match (self.security_context.as_ref(), self.tx) {
                        (Some(ctx), Some(tx)) => tx
                            .get_stored_by_rowids_with_context(ctx, &table, &rowids)
                            .map_err(SqlError::from)?,
                        (None, Some(tx)) => tx
                            .get_stored_by_rowids(&table, &rowids)
                            .map_err(SqlError::from)?,
                        (_, None) => self
                            .db_ref()
                            .get_stored_by_rowids(&table, &rowids)
                            .map_err(SqlError::from)?,
                    };
                    let mut fields = FieldRef::wildcard(schema.as_deref());
                    if stored
                        .iter()
                        .flatten()
                        .any(|record| record.geometry.is_some())
                        && !fields
                            .iter()
                            .any(|field| field.name().eq_ignore_ascii_case("geometry"))
                    {
                        fields.push(FieldRef::Geometry);
                    }
                    retain_referenced_fields(&mut fields, needed_columns, &table, &alias_name);
                    let mut columns = row_output_columns_from_fields(&table, &alias_name, &fields);
                    columns.extend(postgres_system_output_columns(&table, &alias_name));
                    let duplicate = alias_name != table;
                    // Metadata keys serialize in sorted order (serde_json's
                    // BTreeMap-backed Map), so a field's cell INDEX is stable
                    // across a table's records: resolve once from the first
                    // row, then use direct indexing with a cheap key check per
                    // row (falling back to the scanning resolver on mismatch).
                    // The per-row O(fields x cells) case-insensitive scans were
                    // what regressed the first typed-rows attempt.
                    let record_ids = stored
                        .iter()
                        .flatten()
                        .map(|entry| entry.id.clone())
                        .collect::<Vec<_>>();
                    let system_metadata =
                        self.postgres_system_metadata_batch(&table, &record_ids)?;
                    let mut cell_plan: Option<Vec<Option<usize>>> = None;
                    let mut rows = Vec::with_capacity(stored.len());
                    for (idx, (entry, metadata)) in stored
                        .into_iter()
                        .flatten()
                        .zip(system_metadata)
                        .enumerate()
                    {
                        if idx % 1024 == 0 {
                            self.check_cancellation()?;
                        }
                        let row = entry.typed_row().and_then(|cells| {
                            let plan = cell_plan.get_or_insert_with(|| {
                                fields
                                    .iter()
                                    .map(|field| {
                                        let name = match field {
                                            FieldRef::PrimaryKey { name, .. } => name.as_str(),
                                            FieldRef::Column(name)
                                            | FieldRef::JsonColumn(name)
                                            | FieldRef::TypedColumn { name, .. } => name.as_str(),
                                            _ => return None,
                                        };
                                        cells.iter().position(|(key, _)| {
                                            key.as_ref() == name || key.eq_ignore_ascii_case(name)
                                        })
                                    })
                                    .collect()
                            });
                            let mut row =
                                Vec::with_capacity(fields.len() * if duplicate { 2 } else { 1 });
                            for (field, planned) in fields.iter().zip(plan.iter()) {
                                let fast = planned.and_then(|cell_idx| {
                                    let name = match field {
                                        FieldRef::PrimaryKey { name, .. } => name.as_str(),
                                        FieldRef::Column(name)
                                        | FieldRef::JsonColumn(name)
                                        | FieldRef::TypedColumn { name, .. } => name.as_str(),
                                        _ => return None,
                                    };
                                    cells.get(cell_idx).and_then(|(key, cell)| {
                                        (key.as_ref() == name || key.eq_ignore_ascii_case(name))
                                            .then_some(cell)
                                    })
                                });
                                match fast {
                                    Some(cell) => match if matches!(field, FieldRef::JsonColumn(_))
                                        || matches!(
                                            field,
                                            FieldRef::PrimaryKey { pg_type, .. }
                                                if is_json_pg_type(pg_type)
                                        ) {
                                        typed_json_cell_to_sql_value(cell)
                                    } else {
                                        typed_cell_to_sql_value(cell)
                                    } {
                                        Some(SqlValue::Json(json)) => {
                                            if let FieldRef::TypedColumn { pg_type, .. } = field {
                                                row.push(storage_json_to_sql_value(&json, pg_type));
                                            } else {
                                                row.push(SqlValue::Json(json));
                                            }
                                        }
                                        Some(value) => row.push(value),
                                        None => return None,
                                    },
                                    None => match field.value_from_stored(&entry, &cells) {
                                        Some(Ok(value)) => row.push(value),
                                        Some(Err(_)) | None => return None,
                                    },
                                }
                            }
                            Some(row)
                        });
                        let mut row = match row {
                            Some(row) => row,
                            None => {
                                // Fallback: materialize this record only.
                                let record = entry.to_record().map_err(SqlError::from)?;
                                let mut row = Vec::with_capacity(
                                    fields.len() * if duplicate { 2 } else { 1 },
                                );
                                for field in &fields {
                                    row.push(field.value(&record)?);
                                }
                                row
                            }
                        };
                        if duplicate {
                            for idx in 0..fields.len() {
                                row.push(row[idx].clone());
                            }
                        }
                        row.extend(self.postgres_system_values(
                            &table,
                            &alias_name,
                            schema.as_deref(),
                            metadata,
                        ));
                        rows.push(row);
                    }
                    sql_profile_sql_rows_materialized(&rows);
                    return Ok(RowSet { rows, columns });
                }
            }
        }

        let source_records = if let Some(records) = self.indexed_records_for_table_selection(
            &table,
            &alias_name,
            load_schema_shared(self.db_ref(), &table)?.as_deref(),
            selection,
        )? {
            records
        } else {
            sql_profile_full_scan();
            self.scan_records(&table)?
        };
        sql_profile_records_materialized(&source_records);
        let schema = load_schema_shared(self.db_ref(), &table)?;
        let mut fields = row_fields_for_records(schema.as_deref(), &source_records);
        retain_referenced_fields(&mut fields, needed_columns, &table, &alias_name);
        let mut columns = row_output_columns_from_fields(&table, &alias_name, &fields);
        columns.extend(postgres_system_output_columns(&table, &alias_name));
        let record_ids = source_records
            .iter()
            .map(|record| record.id.clone())
            .collect::<Vec<_>>();
        let system_metadata = self.postgres_system_metadata_batch(&table, &record_ids)?;
        let mut rows = Vec::with_capacity(source_records.len());
        for (idx, (record, metadata)) in source_records.iter().zip(system_metadata).enumerate() {
            if idx % 1024 == 0 {
                self.check_cancellation()?;
            }
            let mut row = slot_row_from_record_fields(&table, &alias_name, &fields, record)?;
            row.extend(self.postgres_system_values(
                &table,
                &alias_name,
                schema.as_deref(),
                metadata,
            ));
            rows.push(row);
        }
        Ok(RowSet { rows, columns })
    }

    // The record ids come from rows this statement just materialized, so the
    // materialized variants apply: they stamp system columns without the
    // per-row page-store presence probe that dominates row output on lazy
    // paged collections.
    pub(crate) fn postgres_system_metadata_batch(
        &self,
        table: &str,
        record_ids: &[String],
    ) -> Result<Vec<Option<RecordSystemMetadata>>> {
        match self.tx {
            Some(tx) => tx
                .record_system_metadata_batch_for_materialized(table, record_ids)
                .map_err(Into::into),
            None => self
                .db_ref()
                .record_system_metadata_batch_for_materialized(table, record_ids)
                .map_err(Into::into),
        }
    }

    pub(crate) fn postgres_system_values(
        &self,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        metadata: Option<RecordSystemMetadata>,
    ) -> Vec<SqlValue> {
        let tableoid = schema
            .map(table_relation_oid)
            .unwrap_or_else(|| named_relation_oid(table));
        let values = match metadata {
            Some(metadata) => vec![
                SqlValue::Int(tableoid),
                SqlValue::Int(i64::from(metadata.xmin.0 as u32)),
                SqlValue::Int(i64::from(metadata.xmax.map_or(0, |xmax| xmax.0 as u32))),
                SqlValue::Int(0),
                SqlValue::Int(0),
                SqlValue::String(
                    PgTupleId {
                        block: metadata.tid_block,
                        offset: metadata.tid_offset,
                    }
                    .to_postgres_text(),
                ),
            ],
            None => vec![
                SqlValue::Int(tableoid),
                SqlValue::Int(0),
                SqlValue::Int(0),
                SqlValue::Int(0),
                SqlValue::Int(0),
                SqlValue::String("(0,0)".to_string()),
            ],
        };
        if alias == table {
            return values;
        }
        let mut duplicated = values.clone();
        duplicated.extend(values);
        duplicated
    }

    /// Authorize a routing call, then evaluate it.
    ///
    /// `shortest_path`/`route_distance`/`optimize_route` take a graph NAME and
    /// scan `{graph}_nodes` and `{graph}_edges` through the raw store, so the
    /// tables they read never appear in the query and no relation gate ever
    /// fired: an unprivileged role could read node ids, coordinates and the
    /// graph topology out of tables `SELECT` denies it. The sibling
    /// `travel_time`/`bicdb_*` functions were gated by the earlier
    /// prefix-based check; these three were missed because they carry no
    /// prefix and dispatch through a different evaluator.
    ///
    /// Gated on SELECT over the two backing collections rather than on
    /// superuser, so legitimate graph users keep working.
    pub(crate) fn eval_routing_function_value_authorized(
        &self,
        name: &str,
        args: &[SqlValue],
    ) -> Result<Option<SqlValue>> {
        if let Some(graph) = routing_function_graph(name, args) {
            // Authorize only the backing collections that EXIST. A relation
            // that is not there holds no data to leak, and demanding a
            // privilege on it denies calls that never read it —
            // `optimize_route` orders its stops geometrically and succeeds on
            // a graph with no stored nodes or edges. This also matters because
            // `role_has_table_privilege` answers `relation_exists` for a
            // superuser, so an unconditional check refuses even the bootstrap
            // role on a missing collection. Same convention as
            // `require_table_ownership`: a nonexistent relation is the
            // caller's error to report, not an authorization decision.
            for collection in [format!("{graph}_nodes"), format!("{graph}_edges")] {
                if relation_exists(self.db_ref(), &collection) {
                    self.require_relation_privilege(&collection, "SELECT")?;
                }
            }
        }
        eval_routing_function_value(self.db_ref(), name, args)
    }

    pub(crate) fn require_relation_privilege(&self, table: &str, privilege: &str) -> Result<()> {
        let role = rls_check_user_from_gucs(&self.session_gucs);
        if role_has_table_privilege(self.db_ref(), &role, table, privilege)? {
            return Ok(());
        }
        Err(SqlError::BicDb(BicDbError::Authorization(format!(
            "permission denied for table {}",
            unqualified_relation(table)
        ))))
    }

    pub(crate) fn indexed_records_for_table_selection(
        &self,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        selection: Option<&Expr>,
    ) -> Result<Option<Vec<Arc<Record>>>> {
        // Fast path: when no security context is active and the open transaction has
        // no buffered writes for this table, index lookups and record fetches can stay
        // in RowId space (Copy u64) end-to-end — no String record-id clone, no pk
        // re-resolution. This is the common read shape and where the PG-gap win lands.
        if self.rowid_fast_path_eligible(table) {
            let Some(locators) =
                self.indexed_record_locators_for_table_selection(table, alias, schema, selection)?
            else {
                return Ok(None);
            };
            sql_profile_index_lookup();
            return match locators {
                RecordLocators::Pks(pks) => self
                    .records_for_pks_with_schema(table, &pks, schema)
                    .map(Some),
                RecordLocators::Rowids(rowids) => self
                    .records_for_rowids_with_schema(table, &rowids, schema)
                    .map(Some),
            };
        }
        let Some(ids) =
            self.indexed_record_ids_for_table_selection(table, alias, schema, selection)?
        else {
            return Ok(None);
        };
        sql_profile_index_lookup();
        self.records_for_ids_with_schema(table, &ids, schema)
            .map(Some)
    }

    /// Whether the RowId read fast path can serve this table for the current session
    /// state. Requires no security context (the rowid fetch bypasses the secure
    /// `get_with_context` path) and, if a transaction is open, no buffered writes for
    /// the table (so the committed index + snapshot record reads are authoritative —
    /// no pending-candidate merge needed).
    pub(crate) fn rowid_fast_path_eligible(&self, table: &str) -> bool {
        if !rowid_fast_path_enabled() {
            return false;
        }
        if self.security_context.is_some() {
            return false;
        }
        match self.tx {
            None => true,
            Some(tx) => !tx.has_pending_writes(table),
        }
    }

    /// Twin of [`Self::indexed_record_ids_for_table_selection`] for the fast path.
    /// Primary-key matches stay as pk Strings; secondary-index matches stay as rowids.
    /// Content-hash statement plans for locator analysis
    /// (`BICDB_LOCATOR_PLANS=1`). The analysis walks the WHERE AST once per
    /// pk column and 1-3 times per catalog index per EXECUTION to locate
    /// which equality/range terms bind which fields; the located expressions
    /// depend only on statement structure + catalogs, so they are derived
    /// once and cached by content hash (full-equality verified on hit —
    /// pointer keys proved unsound). Execution evaluates the cached
    /// expressions and reproduces the existing candidate/tie-break logic
    /// exactly, so behavior is identical to the ungated path.
    pub(crate) fn locator_strategy_for_table_selection(
        &self,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        selection: &Expr,
    ) -> Result<Option<RecordLocators>> {
        // NULL equality depends on the current outer row (for example, a
        // correlated VALUES expression), so it must never enter the
        // statement-structure memo below.
        if self.selection_has_null_equality_for_table(table, alias, schema, selection)? {
            return Ok(Some(RecordLocators::Rowids(Vec::new())));
        }
        let strategy = self.locator_strategy_cached(table, alias, schema, selection)?;
        self.execute_locator_strategy(table, alias, schema, selection, &strategy)
    }

    /// The statement's locator strategy: by routine IR node when the
    /// statement is IR-owned (no hash, no content verify per execution), else
    /// by the content-keyed statement memo.
    fn locator_strategy_cached(
        &self,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        selection: &Expr,
    ) -> Result<Rc<LocatorStrategy>> {
        let node_key = self.ir_plan_node_key(selection as *const Expr as usize);
        let index_len = self.db_ref().index_catalog_len();
        if let Some(key) = node_key {
            if let Some(strategy) = crate::engine::locator_strategy_node_get(key, index_len) {
                return Ok(strategy);
            }
        }
        let strategy = locator_strategy_memo(self.db_ref(), selection, table, alias, || {
            self.derive_locator_strategy(table, alias, schema, selection)
        })?;
        if let Some(key) = node_key {
            crate::engine::locator_strategy_node_set(key, index_len, Rc::clone(&strategy));
        }
        Ok(strategy)
    }

    pub(crate) fn derive_locator_strategy(
        &self,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        selection: &Expr,
    ) -> Result<LocatorStrategy> {
        #[cfg(test)]
        SQL_LOCATOR_STRATEGY_DERIVATIONS.with(|calls| *calls.borrow_mut() += 1);
        let pk_exprs = match schema {
            Some(schema) => {
                let pk_columns = primary_key_columns_for_schema(schema);
                pk_columns
                    .iter()
                    .map(|column| {
                        Ok(self
                            .dynamic_equality_value_expr(
                                selection,
                                table,
                                alias,
                                Some(schema),
                                column,
                            )?
                            .cloned())
                    })
                    .collect::<Result<Vec<Option<Expr>>>>()?
            }
            None => Vec::new(),
        };
        let mut indexes = Vec::new();
        let mut has_non_btree_indexes = false;
        for index in sql_index_definitions_for_collection(self.db_ref(), table) {
            if index.kind != IndexKind::BTree {
                has_non_btree_indexes = true;
                continue;
            }
            if is_unusable_typed_primary_key_index(schema, &index) {
                continue;
            }
            let mut prefix_exprs = Vec::new();
            for field in &index.fields {
                let Some(expr) =
                    self.dynamic_equality_field_expr(selection, table, alias, schema, field)?
                else {
                    break;
                };
                prefix_exprs.push(expr.clone());
            }
            let mut range_bounds = Vec::new();
            let mut filters = Vec::new();
            if let Some(field) = index.fields.get(prefix_exprs.len()) {
                range_bounds =
                    self.dynamic_range_bound_exprs(selection, table, alias, schema, field)?;
                if !range_bounds.is_empty() {
                    for (idx, filter_field) in
                        index.fields.iter().enumerate().skip(prefix_exprs.len() + 1)
                    {
                        if let Some(expr) = self.dynamic_equality_field_expr(
                            selection,
                            table,
                            alias,
                            schema,
                            filter_field,
                        )? {
                            filters.push((idx, expr.clone()));
                        }
                    }
                }
            }
            if prefix_exprs.is_empty() && range_bounds.is_empty() {
                continue;
            }
            indexes.push(LocatorIndexPlan {
                name: index.name.clone(),
                fields: index.fields.clone(),
                prefix_exprs,
                range_bounds,
                filters,
                definition: index,
            });
        }
        Ok(LocatorStrategy {
            pk_exprs,
            indexes,
            has_non_btree_indexes,
        })
    }

    /// How many leading pk columns the pk path would match for `selection`
    /// (`usize::MAX` = exact), without running any index probe. Mirrors the
    /// decisions of `primary_key_record_locators_for_table_selection`: exact,
    /// else a range on the column after the bound prefix, else the prefix.
    fn primary_key_locator_matched_columns(
        &self,
        table: &str,
        alias: &str,
        schema: &TableSchema,
        selection: &Expr,
    ) -> Result<Option<usize>> {
        let access = self.primary_key_access_for_selection(table, alias, schema, selection)?;
        if matches!(access, Some(PrimaryKeyAccess::Exact { .. })) {
            return Ok(Some(usize::MAX));
        }
        if self.security_context.is_none() {
            let primary_key_columns = primary_key_columns_for_schema(schema);
            let mut prefix_len = 0usize;
            for column in &primary_key_columns {
                if self
                    .dynamic_equality_sql_value(selection, table, alias, Some(schema), column)?
                    .is_none()
                {
                    break;
                }
                prefix_len += 1;
            }
            if let Some(range_column) = primary_key_columns.get(prefix_len) {
                if !primary_key_requires_typed_identity(schema)
                    && executable_primary_key_index_for_schema(self.db_ref(), schema).is_some()
                {
                    let field = IndexField::MetadataPath(vec![range_column.clone()]);
                    if self
                        .dynamic_range_bounds(selection, table, alias, Some(schema), &field)?
                        .is_some()
                    {
                        return Ok(Some(prefix_len + 1));
                    }
                }
            }
        }
        Ok(match access {
            Some(PrimaryKeyAccess::Prefix {
                matched_columns, ..
            }) => {
                if self.security_context.is_some() {
                    None
                } else {
                    Some(matched_columns)
                }
            }
            _ => None,
        })
    }

    pub(crate) fn execute_locator_strategy(
        &self,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        selection: &Expr,
        strategy: &LocatorStrategy,
    ) -> Result<Option<RecordLocators>> {
        // Plan first, probe later: every candidate's matched-column count is
        // known without touching an index. Only the candidates tied at the
        // maximum are executed (in the original order, fewest rowids wins the
        // tie), so a pk PREFIX that a longer secondary index beats no longer
        // collects its whole prefix — TPC-C's customer-by-last-name and
        // orders-by-customer lookups paid a 3,000-rowid pk scan per call for a
        // result the secondary index answered with a handful of rows.
        struct SecondaryCandidate<'a> {
            plan: &'a LocatorIndexPlan,
            prefix: Vec<IndexValue>,
            range: Option<(Option<IndexValue>, Option<IndexValue>, bool)>,
        }
        let mut pk_matched: Option<usize> = None;
        // Executed early only when the pre-check says the pk is exact (the
        // common point-lookup case): its result is then final.
        let mut pk_result: Option<(usize, RecordLocators)> = None;
        if let Some(schema) = schema {
            pk_matched =
                self.primary_key_locator_matched_columns(table, alias, schema, selection)?;
            if pk_matched == Some(usize::MAX) {
                match self.primary_key_record_locators_for_table_selection(
                    table, alias, schema, selection,
                )? {
                    Some((matched_columns, locators)) => {
                        if matched_columns == usize::MAX {
                            return Ok(Some(locators));
                        }
                        // The pre-check disagreed with the full analysis:
                        // keep this result and compete it below.
                        pk_matched = Some(matched_columns);
                        pk_result = Some((matched_columns, locators));
                    }
                    None => pk_matched = None,
                }
            }
        }
        let mut candidates: Vec<(usize, SecondaryCandidate<'_>)> = Vec::new();
        for plan in &strategy.indexes {
            let mut prefix = Vec::with_capacity(plan.prefix_exprs.len());
            let mut prefix_complete = true;
            for (field_idx, expr) in plan.prefix_exprs.iter().enumerate() {
                let value = self.eval_dynamic_bound_expr(expr)?;
                // An unjoined input currently evaluates to NULL. It cannot
                // narrow this table to an index's NULL keys: defer the
                // equality to the joined row, just as primary-key planning
                // does. A genuinely bound NULL is handled by the earlier
                // NULL-equality shortcut.
                if matches!(value, SqlValue::Null) {
                    prefix_complete = false;
                    break;
                }
                match index_value_from_sql(value) {
                    Ok(value) => {
                        let value = if let (Some(schema), Some(field)) =
                            (schema, plan.fields.get(field_idx))
                        {
                            typed_index_predicate_value(schema, field, value)?
                        } else {
                            value
                        };
                        prefix.push(value);
                    }
                    Err(_) => {
                        prefix_complete = false;
                        break;
                    }
                }
            }
            if !prefix_complete {
                continue;
            }
            if !plan.range_bounds.is_empty() {
                let mut lower = None;
                let mut upper = None;
                let mut has_null_bound = false;
                let range_field = plan.fields.get(prefix.len());
                for (op, expr) in &plan.range_bounds {
                    let value = index_value_from_sql(self.eval_dynamic_bound_expr(expr)?)?;
                    let value = if let (Some(schema), Some(field)) = (schema, range_field) {
                        typed_index_predicate_value(schema, field, value)?
                    } else {
                        value
                    };
                    update_dynamic_range_bound(
                        op.clone(),
                        sql_value_from_index_value(&value),
                        &mut lower,
                        &mut upper,
                        &mut has_null_bound,
                    )?;
                }
                let matched_columns = prefix.len() + 1;
                candidates.push((
                    matched_columns,
                    SecondaryCandidate {
                        plan,
                        prefix,
                        range: Some((lower, upper, has_null_bound)),
                    },
                ));
                continue;
            }
            if !prefix.is_empty() {
                candidates.push((
                    prefix.len(),
                    SecondaryCandidate {
                        plan,
                        prefix,
                        range: None,
                    },
                ));
            }
        }
        let max_matched = candidates
            .iter()
            .map(|(matched, _)| *matched)
            .chain(pk_matched)
            .max();
        let Some(max_matched) = max_matched else {
            return Ok(None);
        };
        let mut best: Option<(usize, RecordLocators)> = None;
        if let (Some(schema), Some(pk)) = (schema, pk_matched) {
            if pk >= max_matched {
                match pk_result.take() {
                    Some(result) => best = Some(result),
                    None => {
                        if let Some((matched_columns, locators)) = self
                            .primary_key_record_locators_for_table_selection(
                                table, alias, schema, selection,
                            )?
                        {
                            if matched_columns == usize::MAX {
                                return Ok(Some(locators));
                            }
                            best = Some((matched_columns, locators));
                        }
                    }
                }
            }
        }
        for (matched_columns, candidate) in candidates {
            if matched_columns < max_matched {
                continue;
            }
            let rowids = match candidate.range {
                Some((lower, upper, has_null_bound)) => {
                    if has_null_bound {
                        Vec::new()
                    } else {
                        let mut filters = Vec::with_capacity(candidate.plan.filters.len());
                        for (idx, expr) in &candidate.plan.filters {
                            let value = self.eval_dynamic_bound_expr(expr)?;
                            if matches!(value, SqlValue::Null) {
                                continue;
                            }
                            if let Ok(mut value) = index_value_from_sql(value) {
                                if let (Some(schema), Some(field)) =
                                    (schema, candidate.plan.fields.get(*idx))
                                {
                                    value = typed_index_predicate_value(schema, field, value)?;
                                }
                                filters.push((*idx, value));
                            }
                        }
                        self.db_ref().range_index_with_prefix_filters_rowids(
                            &candidate.plan.name,
                            candidate.prefix.as_slice(),
                            lower.as_ref(),
                            upper.as_ref(),
                            filters.as_slice(),
                        )?
                    }
                }
                None => self
                    .lookup_index_rowids_cached(&candidate.plan.name, candidate.prefix.as_slice())?
                    .to_vec(),
            };
            let replace = best.as_ref().is_none_or(|(best_prefix_len, best_loc)| {
                matched_columns > *best_prefix_len
                    || (matched_columns == *best_prefix_len && rowids.len() < best_loc.len())
            });
            if replace {
                best = Some((matched_columns, RecordLocators::Rowids(rowids)));
            }
        }
        Ok(best.map(|(_, locators)| locators))
    }

    /// Locate-only twin of [`Self::dynamic_equality_value`].
    pub(crate) fn dynamic_equality_field_expr<'sel>(
        &self,
        selection: &'sel Expr,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        field: &IndexField,
    ) -> Result<Option<&'sel Expr>> {
        for term in and_terms(selection) {
            let Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } = term
            else {
                continue;
            };
            if self.expr_matches_table_index_field(left, table, alias, schema, field)? {
                if self.expr_references_table(right, table, alias, schema)? {
                    continue;
                }
                return Ok(Some(right));
            }
            if self.expr_matches_table_index_field(right, table, alias, schema, field)? {
                if self.expr_references_table(left, table, alias, schema)? {
                    continue;
                }
                return Ok(Some(left));
            }
        }
        Ok(None)
    }

    /// Locate-only twin of [`Self::dynamic_range_bounds`]: the (normalized
    /// operator, value expression) pairs binding `field`. Left/right operand
    /// order is normalized here so execution applies operators verbatim.
    pub(crate) fn dynamic_range_bound_exprs(
        &self,
        selection: &Expr,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        field: &IndexField,
    ) -> Result<Vec<(BinaryOperator, Expr)>> {
        let mut bounds = Vec::new();
        for term in and_terms(selection) {
            let Expr::BinaryOp { left, op, right } = term else {
                continue;
            };
            if !matches!(
                op,
                BinaryOperator::Gt
                    | BinaryOperator::GtEq
                    | BinaryOperator::Lt
                    | BinaryOperator::LtEq
            ) {
                continue;
            }
            if self.expr_matches_table_index_field(left, table, alias, schema, field)? {
                if self.expr_references_table(right, table, alias, schema)?
                    || !self.expr_is_bound_without_table_row(right)?
                {
                    continue;
                }
                bounds.push((op.clone(), (**right).clone()));
                continue;
            }
            if self.expr_matches_table_index_field(right, table, alias, schema, field)? {
                if self.expr_references_table(left, table, alias, schema)?
                    || !self.expr_is_bound_without_table_row(left)?
                {
                    continue;
                }
                bounds.push((reverse_comparison(op), (**left).clone()));
            }
        }
        Ok(bounds)
    }

    pub(crate) fn indexed_record_locators_for_table_selection(
        &self,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        selection: Option<&Expr>,
    ) -> Result<Option<RecordLocators>> {
        let Some(selection) = selection else {
            return Ok(None);
        };
        if let Some(schema) = schema {
            if let Some((index_name, candidate)) =
                self.trigram_index_candidate(table, alias, schema, selection)?
            {
                if self.tx.is_none_or(|tx| !tx.has_pending_writes(table)) {
                    return Ok(Some(RecordLocators::Pks(
                        self.array_candidate_ids(&index_name, &candidate)?
                            .into_iter()
                            .collect(),
                    )));
                }
            }
        }
        #[cfg(test)]
        SQL_INDEXED_SELECTION_PLAN_CALLS.with(|calls| *calls.borrow_mut() += 1);
        if locator_plans_enabled() {
            return self.locator_strategy_for_table_selection(table, alias, schema, selection);
        }
        if self.selection_has_null_equality_for_table(table, alias, schema, selection)? {
            return Ok(Some(RecordLocators::Rowids(Vec::new())));
        }
        let mut best: Option<(usize, RecordLocators)> = None;
        if let Some(schema) = schema {
            if let Some((matched_columns, locators)) = self
                .primary_key_record_locators_for_table_selection(table, alias, schema, selection)?
            {
                if matched_columns == usize::MAX {
                    return Ok(Some(locators));
                }
                best = Some((matched_columns, locators));
            }
        }
        for index in sql_index_definitions_for_collection(self.db_ref(), table) {
            if index.kind != IndexKind::BTree || is_unusable_typed_primary_key_index(schema, &index)
            {
                continue;
            }
            sql_profile_index_catalog_entry_considered();
            let mut prefix = Vec::new();
            for field in &index.fields {
                let Some(value) =
                    self.dynamic_equality_value(selection, table, alias, schema, field)?
                else {
                    break;
                };
                prefix.push(value);
            }
            if let Some(field) = index.fields.get(prefix.len()) {
                if let Some((lower, upper, has_null_bound)) =
                    self.dynamic_range_bounds(selection, table, alias, schema, field)?
                {
                    let matched_columns = prefix.len() + 1;
                    let rowids = if has_null_bound {
                        Vec::new()
                    } else {
                        let filters = self.dynamic_equality_filters_for_index_fields(
                            selection,
                            table,
                            alias,
                            schema,
                            &index.fields,
                            prefix.len() + 1,
                        )?;
                        self.db_ref().range_index_with_prefix_filters_rowids(
                            &index.name,
                            prefix.as_slice(),
                            lower.as_ref(),
                            upper.as_ref(),
                            filters.as_slice(),
                        )?
                    };
                    let replace = best.as_ref().is_none_or(|(best_prefix_len, best_loc)| {
                        matched_columns > *best_prefix_len
                            || (matched_columns == *best_prefix_len
                                && rowids.len() < best_loc.len())
                    });
                    if replace {
                        best = Some((matched_columns, RecordLocators::Rowids(rowids)));
                    }
                    continue;
                }
            }
            if !prefix.is_empty() {
                let rowids = self
                    .lookup_index_rowids_cached(&index.name, prefix.as_slice())?
                    .to_vec();
                let replace = best.as_ref().is_none_or(|(best_prefix_len, best_loc)| {
                    prefix.len() > *best_prefix_len
                        || (prefix.len() == *best_prefix_len && rowids.len() < best_loc.len())
                });
                if replace {
                    best = Some((prefix.len(), RecordLocators::Rowids(rowids)));
                }
            }
        }
        let Some((_, locators)) = best else {
            return Ok(None);
        };
        Ok(Some(locators))
    }

    /// RowId-native twin of [`Self::records_for_ids_with_schema`]: fetch by physical
    /// locator under one collection read lock and apply row-level security.
    pub(crate) fn records_for_rowids_with_schema(
        &self,
        collection: &str,
        rowids: &[RowId],
        schema: Option<&TableSchema>,
    ) -> Result<Vec<Arc<Record>>> {
        #[cfg(test)]
        SQL_RECORDS_FOR_IDS_CALLS.with(|calls| *calls.borrow_mut() += 1);
        self.check_cancellation()?;
        let fetched = match (self.security_context.as_ref(), self.tx) {
            (Some(ctx), Some(tx)) => tx
                .get_records_by_rowids_with_context(ctx, collection, rowids)
                .map_err(SqlError::from)?,
            (None, Some(tx)) => tx
                .get_records_by_rowids(collection, rowids)
                .map_err(SqlError::from)?,
            (_, None) => self
                .db_ref()
                .get_records_by_rowids(collection, rowids)
                .map_err(SqlError::from)?,
        };
        self.filter_fetched_records(collection, fetched, schema)
    }

    /// Primary-key twin of [`Self::records_for_rowids_with_schema`].
    pub(crate) fn records_for_pks_with_schema(
        &self,
        collection: &str,
        pks: &[String],
        schema: Option<&TableSchema>,
    ) -> Result<Vec<Arc<Record>>> {
        #[cfg(test)]
        SQL_RECORDS_FOR_IDS_CALLS.with(|calls| *calls.borrow_mut() += 1);
        self.check_cancellation()?;
        let fetched = match (self.security_context.as_ref(), self.tx) {
            (Some(ctx), Some(tx)) => tx
                .get_records_by_pks_with_context(ctx, collection, pks)
                .map_err(SqlError::from)?,
            (None, Some(tx)) => tx
                .get_records_by_pks(collection, pks)
                .map_err(SqlError::from)?,
            (_, None) => self
                .db_ref()
                .get_records_by_pks(collection, pks)
                .map_err(SqlError::from)?,
        };
        self.filter_fetched_records(collection, fetched, schema)
    }

    pub(crate) fn filter_fetched_records(
        &self,
        collection: &str,
        fetched: Vec<Option<Arc<Record>>>,
        schema: Option<&TableSchema>,
    ) -> Result<Vec<Arc<Record>>> {
        let mut records = Vec::with_capacity(fetched.len());
        for record in fetched.into_iter().flatten() {
            if rls_allows_record_with_schema(
                self,
                collection,
                PolicyAction::Select,
                &record,
                schema,
            )? {
                records.push(record);
            }
        }
        Ok(records)
    }

    pub(crate) fn indexed_record_ids_for_table_selection(
        &self,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        selection: Option<&Expr>,
    ) -> Result<Option<Vec<String>>> {
        let Some(selection) = selection else {
            return Ok(None);
        };
        #[cfg(test)]
        SQL_INDEXED_SELECTION_PLAN_CALLS.with(|calls| *calls.borrow_mut() += 1);
        if self.selection_has_null_equality_for_table(table, alias, schema, selection)? {
            return Ok(Some(Vec::new()));
        }
        // The record-id path serves statements with pending writes on the
        // table (every routine SELECT after an UPDATE of the same table) and
        // every UPDATE/DELETE: it re-derived the index analysis per
        // execution. The memoized strategy the locator path already uses
        // fixes the B-tree candidates once per statement.
        let strategy = if locator_plans_enabled() {
            Some(self.locator_strategy_cached(table, alias, schema, selection)?)
        } else {
            None
        };
        if strategy
            .as_ref()
            .is_none_or(|strategy| strategy.has_non_btree_indexes)
        {
            if let Some(ids) = self.array_index_record_ids(table, alias, schema, selection)? {
                return Ok(Some(ids));
            }
            if let Some(ids) = self.jsonb_index_record_ids(table, alias, schema, selection)? {
                return Ok(Some(ids));
            }
            if let Some(ids) = self.full_text_index_record_ids(table, alias, schema, selection)? {
                return Ok(Some(ids));
            }
        }
        let mut best: Option<(usize, Vec<String>)> = None;
        if let Some(schema) = schema {
            if let Some((matched_columns, ids)) =
                self.primary_key_record_ids_for_table_selection(table, alias, schema, selection)?
            {
                if matched_columns == usize::MAX {
                    return Ok(Some(ids));
                }
                best = Some((matched_columns, ids));
            }
        }
        if let Some(strategy) = strategy {
            return self.record_ids_from_index_plans(table, schema, &strategy, best);
        }
        for index in sql_index_definitions_for_collection(self.db_ref(), table) {
            if index.kind != IndexKind::BTree || is_unusable_typed_primary_key_index(schema, &index)
            {
                continue;
            }
            sql_profile_index_catalog_entry_considered();
            let mut prefix = Vec::new();
            for field in &index.fields {
                let Some(value) =
                    self.dynamic_equality_value(selection, table, alias, schema, field)?
                else {
                    break;
                };
                prefix.push(value);
            }
            if let Some(field) = index.fields.get(prefix.len()) {
                if let Some((lower, upper, has_null_bound)) =
                    self.dynamic_range_bounds(selection, table, alias, schema, field)?
                {
                    let matched_columns = prefix.len() + 1;
                    let ids = if has_null_bound {
                        Vec::new()
                    } else {
                        let filters = self.dynamic_equality_filters_for_index_fields(
                            selection,
                            table,
                            alias,
                            schema,
                            &index.fields,
                            prefix.len() + 1,
                        )?;
                        let ids = self.db_ref().range_index_with_prefix_filters(
                            &index.name,
                            prefix.as_slice(),
                            lower.as_ref(),
                            upper.as_ref(),
                            filters.as_slice(),
                        )?;
                        self.record_ids_with_pending_index_range_candidates(
                            table,
                            &index,
                            prefix.as_slice(),
                            lower.as_ref(),
                            upper.as_ref(),
                            filters.as_slice(),
                            Rc::from(ids),
                        )?
                        .to_vec()
                    };
                    let replace = best.as_ref().is_none_or(|(best_prefix_len, best_ids)| {
                        matched_columns > *best_prefix_len
                            || (matched_columns == *best_prefix_len && ids.len() < best_ids.len())
                    });
                    if replace {
                        best = Some((matched_columns, ids));
                    }
                    continue;
                }
            }
            if !prefix.is_empty() {
                let ids = self
                    .record_ids_with_pending_index_lookup_candidates(
                        table,
                        &index,
                        prefix.as_slice(),
                        self.lookup_index_cached(&index.name, prefix.as_slice())?,
                    )?
                    .to_vec();
                let replace = best.as_ref().is_none_or(|(best_prefix_len, best_ids)| {
                    prefix.len() > *best_prefix_len
                        || (prefix.len() == *best_prefix_len && ids.len() < best_ids.len())
                });
                if replace {
                    best = Some((prefix.len(), ids));
                }
            }
        }
        let Some((_, ids)) = best else {
            return Ok(None);
        };
        Ok(Some(ids))
    }

    /// The B-tree half of `indexed_record_ids_for_table_selection` over a
    /// memoized strategy: the same candidates, values, probes (with this
    /// transaction's pending writes merged) and tie-break as the derived
    /// walk, evaluating the strategy's expressions instead of re-walking the
    /// WHERE clause per index field.
    fn record_ids_from_index_plans(
        &self,
        table: &str,
        schema: Option<&TableSchema>,
        strategy: &LocatorStrategy,
        mut best: Option<(usize, Vec<String>)>,
    ) -> Result<Option<Vec<String>>> {
        'plans: for plan in &strategy.indexes {
            sql_profile_index_catalog_entry_considered();
            let mut prefix = Vec::with_capacity(plan.prefix_exprs.len());
            for (field_idx, expr) in plan.prefix_exprs.iter().enumerate() {
                let value = self.eval_dynamic_bound_expr(expr)?;
                if matches!(value, SqlValue::Null) {
                    continue 'plans;
                }
                let value = index_value_from_sql(value)?;
                let value = match (schema, plan.fields.get(field_idx)) {
                    (Some(schema), Some(field)) => {
                        typed_index_predicate_value(schema, field, value)?
                    }
                    _ => value,
                };
                prefix.push(value);
            }
            if !plan.range_bounds.is_empty() {
                let mut lower = None;
                let mut upper = None;
                let mut has_null_bound = false;
                let range_field = plan.fields.get(prefix.len());
                for (op, expr) in &plan.range_bounds {
                    let value = index_value_from_sql(self.eval_dynamic_bound_expr(expr)?)?;
                    let value = match (schema, range_field) {
                        (Some(schema), Some(field)) => {
                            typed_index_predicate_value(schema, field, value)?
                        }
                        _ => value,
                    };
                    update_dynamic_range_bound(
                        op.clone(),
                        sql_value_from_index_value(&value),
                        &mut lower,
                        &mut upper,
                        &mut has_null_bound,
                    )?;
                }
                let matched_columns = prefix.len() + 1;
                let ids = if has_null_bound {
                    Vec::new()
                } else {
                    let mut filters = Vec::with_capacity(plan.filters.len());
                    for (idx, expr) in &plan.filters {
                        let value = self.eval_dynamic_bound_expr(expr)?;
                        if matches!(value, SqlValue::Null) {
                            continue;
                        }
                        let value = index_value_from_sql(value)?;
                        let value = match (schema, plan.fields.get(*idx)) {
                            (Some(schema), Some(field)) => {
                                typed_index_predicate_value(schema, field, value)?
                            }
                            _ => value,
                        };
                        filters.push((*idx, value));
                    }
                    let ids = self.db_ref().range_index_with_prefix_filters(
                        &plan.name,
                        prefix.as_slice(),
                        lower.as_ref(),
                        upper.as_ref(),
                        filters.as_slice(),
                    )?;
                    self.record_ids_with_pending_index_range_candidates(
                        table,
                        &plan.definition,
                        prefix.as_slice(),
                        lower.as_ref(),
                        upper.as_ref(),
                        filters.as_slice(),
                        Rc::from(ids),
                    )?
                    .to_vec()
                };
                let replace = best.as_ref().is_none_or(|(best_prefix_len, best_ids)| {
                    matched_columns > *best_prefix_len
                        || (matched_columns == *best_prefix_len && ids.len() < best_ids.len())
                });
                if replace {
                    best = Some((matched_columns, ids));
                }
                continue;
            }
            if !prefix.is_empty() {
                let ids = self
                    .record_ids_with_pending_index_lookup_candidates(
                        table,
                        &plan.definition,
                        prefix.as_slice(),
                        self.lookup_index_cached(&plan.name, prefix.as_slice())?,
                    )?
                    .to_vec();
                let replace = best.as_ref().is_none_or(|(best_prefix_len, best_ids)| {
                    prefix.len() > *best_prefix_len
                        || (prefix.len() == *best_prefix_len && ids.len() < best_ids.len())
                });
                if replace {
                    best = Some((prefix.len(), ids));
                }
            }
        }
        Ok(best.map(|(_, ids)| ids))
    }

    pub(crate) fn full_text_index_record_ids(
        &self,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        selection: &Expr,
    ) -> Result<Option<Vec<String>>> {
        if self.tx.is_some_and(|tx| tx.has_pending_writes(table)) {
            return Ok(None);
        }
        let Some((index_name, candidate)) =
            self.full_text_index_candidate(table, alias, schema, selection)?
        else {
            return Ok(None);
        };
        let ids = self.full_text_candidate_ids(&index_name, &candidate)?;
        sql_profile_index_lookup();
        Ok(Some(ids.into_iter().collect()))
    }

    pub(crate) fn array_index_record_ids(
        &self,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        selection: &Expr,
    ) -> Result<Option<Vec<String>>> {
        if self.tx.is_some_and(|tx| tx.has_pending_writes(table)) {
            return Ok(None);
        }
        let Some((index_name, candidate)) =
            self.array_index_candidate(table, alias, schema, selection)?
        else {
            return Ok(None);
        };
        let ids = self.array_candidate_ids(&index_name, &candidate)?;
        sql_profile_index_lookup();
        Ok(Some(ids.into_iter().collect()))
    }

    pub(crate) fn jsonb_index_record_ids(
        &self,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        selection: &Expr,
    ) -> Result<Option<Vec<String>>> {
        if self.tx.is_some_and(|tx| tx.has_pending_writes(table)) {
            return Ok(None);
        }
        let Some((index_name, candidate)) =
            self.jsonb_index_candidate(table, alias, schema, selection)?
        else {
            return Ok(None);
        };
        let ids = self.jsonb_candidate_ids(&index_name, &candidate)?;
        sql_profile_index_lookup();
        Ok(Some(ids.into_iter().collect()))
    }

    pub(crate) fn jsonb_index_candidate(
        &self,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        selection: &Expr,
    ) -> Result<Option<(String, JsonbIndexCandidate)>> {
        let Some(schema) = schema else {
            return Ok(None);
        };
        let definitions = sql_index_definitions_for_collection(self.db_ref(), table);
        for term in and_terms(selection) {
            let Expr::BinaryOp { left, op, right } = term else {
                continue;
            };
            if !matches!(
                op,
                BinaryOperator::AtArrow
                    | BinaryOperator::AtQuestion
                    | BinaryOperator::AtAt
                    | BinaryOperator::Question
                    | BinaryOperator::QuestionAnd
                    | BinaryOperator::QuestionPipe
            ) || projected_expr_pg_type(left, Some(schema)).as_deref() != Some("jsonb")
                || !self.expr_references_table(left, table, alias, Some(schema))?
                || self.expr_references_table(right, table, alias, Some(schema))?
                || !self.expr_is_bound_without_table_row(right)?
            {
                continue;
            }
            let Some(index_schema) = schema.indexes.iter().find(|index| {
                !index.metadata_only
                    && index.access_method == "gin"
                    && full_text_index_expression_matches(&index.expression, left)
            }) else {
                continue;
            };
            if !definitions.iter().any(|definition| {
                definition.kind == IndexKind::Jsonb
                    && definition.name.eq_ignore_ascii_case(&index_schema.name)
            }) {
                continue;
            }
            let value = self.eval_dynamic_bound_expr(right)?;
            let candidate = match op {
                BinaryOperator::AtArrow => {
                    let terms = jsonb_index_terms(&value)?;
                    if terms.is_empty() {
                        continue;
                    }
                    JsonbIndexCandidate::All(terms)
                }
                BinaryOperator::Question => {
                    let SqlValue::String(key) = value else {
                        continue;
                    };
                    JsonbIndexCandidate::All(vec![jsonb_index_key_token(&key)])
                }
                BinaryOperator::QuestionAnd | BinaryOperator::QuestionPipe => {
                    let terms = json_text_array_path(&value)?
                        .into_iter()
                        .flatten()
                        .map(|key| jsonb_index_key_token(&key))
                        .collect::<Vec<_>>();
                    if terms.is_empty() {
                        if matches!(op, BinaryOperator::QuestionPipe) {
                            return Ok(Some((
                                index_schema.name.clone(),
                                JsonbIndexCandidate::Any(Vec::new()),
                            )));
                        }
                        continue;
                    }
                    if matches!(op, BinaryOperator::QuestionAnd) {
                        JsonbIndexCandidate::All(terms)
                    } else {
                        JsonbIndexCandidate::Any(terms)
                    }
                }
                BinaryOperator::AtQuestion | BinaryOperator::AtAt => {
                    let terms = jsonpath_index_terms(&value)?;
                    if terms.is_empty() {
                        continue;
                    }
                    JsonbIndexCandidate::All(terms)
                }
                _ => unreachable!(),
            };
            return Ok(Some((index_schema.name.clone(), candidate)));
        }
        Ok(None)
    }

    pub(crate) fn array_index_candidate(
        &self,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        selection: &Expr,
    ) -> Result<Option<(String, JsonbIndexCandidate)>> {
        let Some(schema) = schema else {
            return Ok(None);
        };
        if let Some(candidate) = self.trigram_index_candidate(table, alias, schema, selection)? {
            return Ok(Some(candidate));
        }
        let definitions = sql_index_definitions_for_collection(self.db_ref(), table);
        for term in and_terms(selection) {
            let Expr::BinaryOp { left, op, right } = term else {
                continue;
            };
            if !matches!(op, BinaryOperator::AtArrow | BinaryOperator::PGOverlap)
                || !projected_expr_pg_type(left, Some(schema))
                    .is_some_and(|pg_type| pg_type.ends_with("[]"))
                || !self.expr_references_table(left, table, alias, Some(schema))?
                || self.expr_references_table(right, table, alias, Some(schema))?
                || !self.expr_is_bound_without_table_row(right)?
            {
                continue;
            }
            let Some(index_schema) = schema.indexes.iter().find(|index| {
                !index.metadata_only
                    && index.access_method == "gin"
                    && full_text_index_expression_matches(&index.expression, left)
            }) else {
                continue;
            };
            if !definitions.iter().any(|definition| {
                definition.kind == IndexKind::Array
                    && definition.name.eq_ignore_ascii_case(&index_schema.name)
            }) {
                continue;
            }
            let value = self.eval_dynamic_bound_expr(right)?;
            if matches!(value, SqlValue::Null) {
                return Ok(Some((
                    index_schema.name.clone(),
                    JsonbIndexCandidate::Any(Vec::new()),
                )));
            }
            let terms = array_index_terms(&value)?;
            if terms.is_empty() && matches!(op, BinaryOperator::AtArrow) {
                continue;
            }
            let candidate = if matches!(op, BinaryOperator::AtArrow) {
                JsonbIndexCandidate::All(terms)
            } else {
                JsonbIndexCandidate::Any(terms)
            };
            return Ok(Some((index_schema.name.clone(), candidate)));
        }
        Ok(None)
    }

    pub(crate) fn jsonb_candidate_ids(
        &self,
        index_name: &str,
        candidate: &JsonbIndexCandidate,
    ) -> Result<BTreeSet<String>> {
        let (terms, require_all) = match candidate {
            JsonbIndexCandidate::All(terms) => (terms, true),
            JsonbIndexCandidate::Any(terms) => (terms, false),
        };
        let mut result: Option<BTreeSet<String>> = None;
        for term in terms {
            let ids = self
                .db_ref()
                .lookup_jsonb_term(index_name, term)?
                .into_iter()
                .collect::<BTreeSet<_>>();
            result = Some(match result {
                None => ids,
                Some(result) if require_all => result.intersection(&ids).cloned().collect(),
                Some(result) => result.union(&ids).cloned().collect(),
            });
        }
        Ok(result.unwrap_or_default())
    }

    pub(crate) fn array_candidate_ids(
        &self,
        index_name: &str,
        candidate: &JsonbIndexCandidate,
    ) -> Result<BTreeSet<String>> {
        let (terms, require_all) = match candidate {
            JsonbIndexCandidate::All(terms) => (terms, true),
            JsonbIndexCandidate::Any(terms) => (terms, false),
        };
        let mut result: Option<BTreeSet<String>> = None;
        for term in terms {
            let ids = self
                .db_ref()
                .lookup_array_term(index_name, term)?
                .into_iter()
                .collect::<BTreeSet<_>>();
            result = Some(match result {
                None => ids,
                Some(result) if require_all => result.intersection(&ids).cloned().collect(),
                Some(result) => result.union(&ids).cloned().collect(),
            });
        }
        Ok(result.unwrap_or_default())
    }

    pub(crate) fn full_text_index_candidate(
        &self,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        selection: &Expr,
    ) -> Result<Option<(String, FtsIndexCandidate)>> {
        let Some(schema) = schema else {
            return Ok(None);
        };
        let definitions = sql_index_definitions_for_collection(self.db_ref(), table);
        for term in and_terms(selection) {
            let Expr::BinaryOp {
                left,
                op: BinaryOperator::AtAt,
                right,
            } = term
            else {
                continue;
            };
            let (vector_expr, query_expr) = if projected_expr_pg_type(left, Some(schema)).as_deref()
                == Some("tsvector")
            {
                (left.as_ref(), right.as_ref())
            } else if projected_expr_pg_type(right, Some(schema)).as_deref() == Some("tsvector") {
                (right.as_ref(), left.as_ref())
            } else {
                continue;
            };
            if !self.expr_references_table(vector_expr, table, alias, Some(schema))?
                || self.expr_references_table(query_expr, table, alias, Some(schema))?
                || !self.expr_is_bound_without_table_row(query_expr)?
            {
                continue;
            }
            let Some(index_schema) = schema.indexes.iter().find(|index| {
                !index.metadata_only
                    && matches!(index.access_method.as_str(), "gin" | "gist")
                    && full_text_index_expression_matches(&index.expression, vector_expr)
            }) else {
                continue;
            };
            if !definitions.iter().any(|definition| {
                definition.kind == IndexKind::FullText
                    && definition.name.eq_ignore_ascii_case(&index_schema.name)
            }) {
                continue;
            }
            let query = tsquery_from_sql_value(&self.eval_dynamic_bound_expr(query_expr)?)?;
            let Some(candidate) = query.index_candidate() else {
                continue;
            };
            return Ok(Some((index_schema.name.clone(), candidate)));
        }
        Ok(None)
    }

    pub(crate) fn full_text_candidate_ids(
        &self,
        index_name: &str,
        candidate: &FtsIndexCandidate,
    ) -> Result<BTreeSet<String>> {
        let mut budget =
            bicdb_core::FtsQueryBudget::new(self.fts_limits, self.cancellation.child());
        self.full_text_candidate_ids_budgeted(index_name, candidate, &mut budget)
    }

    pub(crate) fn full_text_candidate_ids_budgeted(
        &self,
        index_name: &str,
        candidate: &FtsIndexCandidate,
        budget: &mut bicdb_core::FtsQueryBudget,
    ) -> Result<BTreeSet<String>> {
        match candidate {
            FtsIndexCandidate::Term { text, prefix } => {
                let postings = self
                    .db_ref()
                    .lookup_full_text_term_budgeted(index_name, text, *prefix, budget)
                    .map_err(SqlError::from)?;
                Ok(postings.into_iter().collect())
            }
            FtsIndexCandidate::And(left, right) => {
                let left = self.full_text_candidate_ids_budgeted(index_name, left, budget)?;
                let right = self.full_text_candidate_ids_budgeted(index_name, right, budget)?;
                Ok(left.intersection(&right).cloned().collect())
            }
            FtsIndexCandidate::Or(left, right) => {
                let left = self.full_text_candidate_ids_budgeted(index_name, left, budget)?;
                let right = self.full_text_candidate_ids_budgeted(index_name, right, budget)?;
                Ok(left.union(&right).cloned().collect())
            }
        }
    }

    pub(crate) fn selection_has_null_equality_for_table(
        &self,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        selection: &Expr,
    ) -> Result<bool> {
        // The structural half — which AND terms compare a column of this
        // table with something that does not read its row — is fixed per
        // statement node (the schema generation is part of the key). Only the
        // binding check and the NULL test, which depend on the routine
        // variables and the outer row, run per call.
        let key = self.ir_plan_node_key(selection as *const Expr as usize);
        let candidates = match key.and_then(|key| null_equality_candidates_get(key, table, alias)) {
            Some(candidates) => candidates,
            None => {
                let candidates =
                    Rc::new(self.null_equality_candidates(table, alias, schema, selection)?);
                if let Some(key) = key {
                    null_equality_candidates_set(key, table, alias, Rc::clone(&candidates));
                }
                candidates
            }
        };
        for &(index, field_on_left) in candidates.iter() {
            let Some(Expr::BinaryOp { left, right, .. }) = nth_and_term(selection, index) else {
                continue;
            };
            let bound = if field_on_left { right } else { left };
            if self.expr_is_bound_without_table_row(bound)?
                && matches!(self.eval_dynamic_bound_expr(bound)?, SqlValue::Null)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// `(and-term index, column on the left)` for every equality term of
    /// `selection` that compares a column of `table` with an expression not
    /// referencing that table — the per-call NULL test's candidates.
    fn null_equality_candidates(
        &self,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        selection: &Expr,
    ) -> Result<Vec<(usize, bool)>> {
        let mut candidates = Vec::new();
        for (index, term) in and_terms(selection).into_iter().enumerate() {
            let Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } = term
            else {
                continue;
            };
            if normalize_table_field_expr(left, table, alias, schema).is_some()
                && !self.expr_references_table(right, table, alias, schema)?
            {
                candidates.push((index, true));
            }
            if normalize_table_field_expr(right, table, alias, schema).is_some()
                && !self.expr_references_table(left, table, alias, schema)?
            {
                candidates.push((index, false));
            }
        }
        Ok(candidates)
    }

    /// [`referenced_column_names`] memoized per IR-owned query node (it reads
    /// identifiers only, which literal rebinding never touches).
    pub(crate) fn referenced_column_names_cached(
        &self,
        select: &Select,
        query: &Query,
    ) -> Option<Rc<ReferencedColumns>> {
        let Some(key) = self.ir_plan_node_key(query as *const Query as usize) else {
            return referenced_column_names(select, query).map(Rc::new);
        };
        if let Some(cached) =
            REFERENCED_COLUMNS_NODES.with(|cache| cache.borrow().get(&key).cloned())
        {
            return cached;
        }
        let computed = referenced_column_names(select, query).map(Rc::new);
        REFERENCED_COLUMNS_NODES.with(|cache| {
            let mut cache = cache.borrow_mut();
            if cache.len() >= STATEMENT_NODE_MEMO_MAX {
                cache.clear();
            }
            cache.insert(key, computed.clone());
        });
        computed
    }

    pub(crate) fn expr_is_bound_without_table_row(&self, expr: &Expr) -> Result<bool> {
        Ok(match expr {
            Expr::Value(_) | Expr::TypedString(_) | Expr::Interval(_) => true,
            Expr::Identifier(ident) => {
                routine_var_from_ident(&self.routine_vars, ident).is_some()
                    || self.outer_row.as_ref().is_some_and(|row| {
                        row.value_from_parts(std::slice::from_ref(&ident.value))
                            .is_some()
                    })
            }
            Expr::CompoundIdentifier(idents) => {
                let parts = idents
                    .iter()
                    .map(|ident| ident.value.clone())
                    .collect::<Vec<_>>();
                routine_var_from_parts(&self.routine_vars, &parts)?.is_some()
                    || self
                        .outer_row
                        .as_ref()
                        .is_some_and(|row| row.value_from_parts(&parts).is_some())
            }
            Expr::Nested(expr)
            | Expr::Cast { expr, .. }
            | Expr::UnaryOp { expr, .. }
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::IsTrue(expr)
            | Expr::IsNotTrue(expr)
            | Expr::IsFalse(expr)
            | Expr::IsNotFalse(expr)
            | Expr::IsUnknown(expr)
            | Expr::IsNotUnknown(expr)
            | Expr::Collate { expr, .. } => self.expr_is_bound_without_table_row(expr)?,
            Expr::BinaryOp { left, right, .. }
            | Expr::AnyOp { left, right, .. }
            | Expr::AllOp { left, right, .. } => {
                self.expr_is_bound_without_table_row(left)?
                    && self.expr_is_bound_without_table_row(right)?
            }
            Expr::InList { expr, list, .. } => {
                self.expr_is_bound_without_table_row(expr)?
                    && list
                        .iter()
                        .all(|item| self.expr_is_bound_without_table_row(item).unwrap_or(false))
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.expr_is_bound_without_table_row(expr)?
                    && self.expr_is_bound_without_table_row(low)?
                    && self.expr_is_bound_without_table_row(high)?
            }
            Expr::Function(function) => function_args(function)
                .iter()
                .all(|arg| self.expr_is_bound_without_table_row(arg).unwrap_or(false)),
            Expr::Array(array) => array
                .elem
                .iter()
                .all(|item| self.expr_is_bound_without_table_row(item).unwrap_or(false)),
            Expr::Position { expr, r#in } => {
                self.expr_is_bound_without_table_row(expr)?
                    && self.expr_is_bound_without_table_row(r#in)?
            }
            Expr::Extract { expr, .. } => self.expr_is_bound_without_table_row(expr)?,
            Expr::Trim {
                expr,
                trim_what,
                trim_characters,
                ..
            } => {
                self.expr_is_bound_without_table_row(expr)?
                    && trim_what.as_ref().is_none_or(|expr| {
                        self.expr_is_bound_without_table_row(expr).unwrap_or(false)
                    })
                    && trim_characters.as_ref().is_none_or(|exprs| {
                        exprs
                            .iter()
                            .all(|expr| self.expr_is_bound_without_table_row(expr).unwrap_or(false))
                    })
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                operand
                    .as_ref()
                    .is_none_or(|expr| self.expr_is_bound_without_table_row(expr).unwrap_or(false))
                    && conditions.iter().all(|condition| {
                        self.expr_is_bound_without_table_row(&condition.condition)
                            .unwrap_or(false)
                            && self
                                .expr_is_bound_without_table_row(&condition.result)
                                .unwrap_or(false)
                    })
                    && else_result.as_ref().is_none_or(|expr| {
                        self.expr_is_bound_without_table_row(expr).unwrap_or(false)
                    })
            }
            _ => false,
        })
    }

    pub(crate) fn primary_key_record_ids_for_table_selection(
        &self,
        table: &str,
        alias: &str,
        schema: &TableSchema,
        selection: &Expr,
    ) -> Result<Option<(usize, Vec<String>)>> {
        let access = self.primary_key_access_for_selection(table, alias, schema, selection)?;
        if let Some(PrimaryKeyAccess::Exact { record_id, values }) = access.as_ref() {
            if primary_key_requires_typed_identity(schema) {
                // Locator planning: probes id existence only; the rows those
                // ids resolve to are fetched through the policy-filtered
                // record paths.
                if self.db_ref().get_unchecked(table, record_id)?.is_some() {
                    return Ok(Some((usize::MAX, vec![record_id.clone()])));
                }
                return Ok(Some((
                    usize::MAX,
                    self.primary_key_prefix_record_ids(table, schema, values, record_id)?
                        .to_vec(),
                )));
            }
            return Ok(Some((usize::MAX, vec![record_id.clone()])));
        }
        if let Some(candidate) =
            self.primary_key_range_record_ids_for_table_selection(table, alias, schema, selection)?
        {
            return Ok(Some(candidate));
        }
        let Some(access) = access else {
            return Ok(None);
        };
        match access {
            PrimaryKeyAccess::Exact { record_id, .. } => Ok(Some((usize::MAX, vec![record_id]))),
            PrimaryKeyAccess::Prefix {
                prefix,
                prefix_values,
                matched_columns,
            } => {
                if self.security_context.is_some() {
                    return Ok(None);
                }
                Ok(Some((
                    matched_columns,
                    self.primary_key_prefix_record_ids(table, schema, &prefix_values, &prefix)?
                        .to_vec(),
                )))
            }
        }
    }

    /// Fast-path twin of [`Self::primary_key_record_ids_for_table_selection`]:
    /// the PREFIX case (partial pk equality, e.g. `WHERE w_id=? AND d_id=?`)
    /// resolves through the pk index in RowId space with no String pk
    /// materialization and no pending-candidate merge — callers guarantee
    /// `rowid_fast_path_eligible` (no security context, no buffered writes for
    /// the table). Exact matches and pk ranges keep the String forms.
    pub(crate) fn primary_key_record_locators_for_table_selection(
        &self,
        table: &str,
        alias: &str,
        schema: &TableSchema,
        selection: &Expr,
    ) -> Result<Option<(usize, RecordLocators)>> {
        let access = self.primary_key_access_for_selection(table, alias, schema, selection)?;
        if let Some(PrimaryKeyAccess::Exact { record_id, values }) = access.as_ref() {
            if primary_key_requires_typed_identity(schema) {
                // Locator planning: probes id existence only; the rows those
                // ids resolve to are fetched through the policy-filtered
                // record paths.
                if self.db_ref().get_unchecked(table, record_id)?.is_some() {
                    return Ok(Some((
                        usize::MAX,
                        RecordLocators::Pks(vec![record_id.clone()]),
                    )));
                }
                return Ok(Some((
                    usize::MAX,
                    RecordLocators::Pks(
                        self.primary_key_prefix_record_ids(table, schema, values, record_id)?
                            .to_vec(),
                    ),
                )));
            }
            return Ok(Some((
                usize::MAX,
                RecordLocators::Pks(vec![record_id.clone()]),
            )));
        }
        if let Some((matched_columns, ids)) =
            self.primary_key_range_record_ids_for_table_selection(table, alias, schema, selection)?
        {
            return Ok(Some((matched_columns, RecordLocators::Pks(ids))));
        }
        let Some(access) = access else {
            return Ok(None);
        };
        match access {
            PrimaryKeyAccess::Exact { record_id, .. } => {
                Ok(Some((usize::MAX, RecordLocators::Pks(vec![record_id]))))
            }
            PrimaryKeyAccess::Prefix {
                prefix,
                prefix_values,
                matched_columns,
            } => {
                if self.security_context.is_some() {
                    return Ok(None);
                }
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
                            let rowids = self
                                .lookup_index_rowids_cached(&index.name, index_prefix.as_slice())?;
                            return Ok(Some((
                                matched_columns,
                                RecordLocators::Rowids(rowids.to_vec()),
                            )));
                        }
                    }
                }
                Ok(Some((
                    matched_columns,
                    RecordLocators::Pks(
                        self.primary_key_prefix_record_ids(table, schema, &prefix_values, &prefix)?
                            .to_vec(),
                    ),
                )))
            }
        }
    }

    pub(crate) fn exact_primary_key_selection_covers_predicate(
        &self,
        table: &str,
        alias: &str,
        schema: &TableSchema,
        selection: &Expr,
    ) -> Result<bool> {
        let primary_key_columns = primary_key_columns_for_schema(schema);
        if primary_key_columns.is_empty() {
            return Ok(false);
        }
        let mut matched_columns = BTreeSet::new();
        for term in and_terms(selection) {
            let Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } = term
            else {
                return Ok(false);
            };
            let matched = if let Some(idx) =
                self.primary_key_expr_index(left, table, alias, Some(schema), &primary_key_columns)?
            {
                if self.expr_references_table(right, table, alias, Some(schema))?
                    || expr_contains_volatile_function(right)
                {
                    return Ok(false);
                }
                Some(idx)
            } else if let Some(idx) = self.primary_key_expr_index(
                right,
                table,
                alias,
                Some(schema),
                &primary_key_columns,
            )? {
                if self.expr_references_table(left, table, alias, Some(schema))?
                    || expr_contains_volatile_function(left)
                {
                    return Ok(false);
                }
                Some(idx)
            } else {
                None
            };
            let Some(idx) = matched else {
                return Ok(false);
            };
            matched_columns.insert(idx);
        }
        Ok(matched_columns.len() == primary_key_columns.len())
    }

    pub(crate) fn primary_key_range_record_ids_for_table_selection(
        &self,
        table: &str,
        alias: &str,
        schema: &TableSchema,
        selection: &Expr,
    ) -> Result<Option<(usize, Vec<String>)>> {
        if self.security_context.is_some() {
            return Ok(None);
        }
        let primary_key_columns = primary_key_columns_for_schema(schema);
        if primary_key_columns.is_empty() {
            return Ok(None);
        }
        let mut bound_values = Vec::with_capacity(primary_key_columns.len());
        for column in &primary_key_columns {
            bound_values.push(self.dynamic_equality_sql_value(
                selection,
                table,
                alias,
                Some(schema),
                column,
            )?);
        }
        if bound_values
            .iter()
            .flatten()
            .any(|value| matches!(value, SqlValue::Null))
        {
            return Ok(Some((usize::MAX, Vec::new())));
        }
        let prefix_values = bound_values
            .into_iter()
            .take_while(Option::is_some)
            .map(Option::unwrap)
            .collect::<Vec<_>>();
        let range_idx = prefix_values.len();
        let Some(range_column) = primary_key_columns.get(range_idx) else {
            return Ok(None);
        };
        let field = IndexField::MetadataPath(vec![range_column.clone()]);
        if !primary_key_requires_typed_identity(schema) {
            if let Some(index) = executable_primary_key_index_for_schema(self.db_ref(), schema) {
                let Some((lower, upper, has_null_bound)) =
                    self.dynamic_range_bounds(selection, table, alias, Some(schema), &field)?
                else {
                    return Ok(None);
                };
                if has_null_bound {
                    return Ok(Some((range_idx + 1, Vec::new())));
                }
                let prefix = prefix_values
                    .iter()
                    .cloned()
                    .map(index_value_from_sql)
                    .collect::<Result<Vec<_>>>()?;
                let primary_key_fields = primary_key_index_fields_for_columns(&primary_key_columns);
                let filters = self.dynamic_equality_filters_for_index_fields(
                    selection,
                    table,
                    alias,
                    Some(schema),
                    &primary_key_fields,
                    range_idx + 1,
                )?;
                let ids = self.db_ref().range_index_with_prefix_filters(
                    &index.name,
                    prefix.as_slice(),
                    lower.as_ref(),
                    upper.as_ref(),
                    filters.as_slice(),
                )?;
                let ids = self
                    .record_ids_with_pending_index_range_candidates(
                        table,
                        &index,
                        prefix.as_slice(),
                        lower.as_ref(),
                        upper.as_ref(),
                        filters.as_slice(),
                        Rc::from(ids),
                    )?
                    .to_vec();
                if sql_trace_flags().plan {
                    eprintln!(
                        "bicdb_trace_plan primary_key_range source=btree table={} index={} matched_columns={} ids={}",
                        table,
                        index.name,
                        range_idx + 1,
                        ids.len()
                    );
                }
                return Ok(Some((range_idx + 1, ids)));
            }
        }
        let Some((lower, upper, has_null_bound)) =
            self.dynamic_sql_range_bounds(selection, table, alias, Some(schema), &field)?
        else {
            return Ok(None);
        };
        if has_null_bound {
            return Ok(Some((range_idx + 1, Vec::new())));
        }
        let id_prefix = if prefix_values.is_empty() {
            String::new()
        } else {
            composite_record_id_prefix_from_values(schema, &primary_key_columns, &prefix_values)?
        };
        sql_profile_record_id_prefix_scan();
        let ids = self.scan_record_ids_with_prefix(table, &id_prefix)?;
        let mut matched_ids = Vec::new();
        for id in ids {
            let Some(values) =
                primary_key_values_from_record_id(schema, &primary_key_columns, &id)?
            else {
                return Ok(None);
            };
            if !primary_key_values_match_prefix(&values, &prefix_values) {
                continue;
            }
            let value = values.get(range_idx).cloned().unwrap_or(SqlValue::Null);
            if primary_key_value_outside_range(&value, lower.as_ref(), upper.as_ref()) {
                continue;
            }
            matched_ids.push(id);
        }
        if sql_trace_flags().plan {
            eprintln!(
                "bicdb_trace_plan primary_key_range source=record_ids table={} matched_columns={} ids={}",
                table,
                range_idx + 1,
                matched_ids.len()
            );
        }
        Ok(Some((range_idx + 1, matched_ids)))
    }

    pub(crate) fn dynamic_equality_value(
        &self,
        selection: &Expr,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        field: &IndexField,
    ) -> Result<Option<IndexValue>> {
        for term in and_terms(selection) {
            let Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } = term
            else {
                continue;
            };
            if self.expr_matches_table_index_field(left, table, alias, schema, field)? {
                if self.expr_references_table(right, table, alias, schema)? {
                    continue;
                }
                let value = self.eval_dynamic_bound_expr(right)?;
                if matches!(value, SqlValue::Null) {
                    continue;
                }
                let value = index_value_from_sql(value)?;
                let value = if let Some(schema) = schema {
                    typed_index_predicate_value(schema, field, value)?
                } else {
                    value
                };
                return Ok(Some(value));
            }
            if self.expr_matches_table_index_field(right, table, alias, schema, field)? {
                if self.expr_references_table(left, table, alias, schema)? {
                    continue;
                }
                let value = self.eval_dynamic_bound_expr(left)?;
                if matches!(value, SqlValue::Null) {
                    continue;
                }
                let value = index_value_from_sql(value)?;
                let value = if let Some(schema) = schema {
                    typed_index_predicate_value(schema, field, value)?
                } else {
                    value
                };
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    pub(crate) fn dynamic_range_bounds(
        &self,
        selection: &Expr,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        field: &IndexField,
    ) -> Result<Option<(Option<IndexValue>, Option<IndexValue>, bool)>> {
        let mut lower = None;
        let mut upper = None;
        let mut matched = false;
        let mut has_null_bound = false;
        for term in and_terms(selection) {
            let Expr::BinaryOp { left, op, right } = term else {
                continue;
            };
            if !matches!(
                op,
                BinaryOperator::Gt
                    | BinaryOperator::GtEq
                    | BinaryOperator::Lt
                    | BinaryOperator::LtEq
            ) {
                continue;
            }
            if self.expr_matches_table_index_field(left, table, alias, schema, field)? {
                if self.expr_references_table(right, table, alias, schema)?
                    || !self.expr_is_bound_without_table_row(right)?
                {
                    continue;
                }
                let value = self.eval_dynamic_bound_expr(right)?;
                let value = if let Some(schema) = schema {
                    let value =
                        typed_index_predicate_value(schema, field, index_value_from_sql(value)?)?;
                    sql_value_from_index_value(&value)
                } else {
                    value
                };
                update_dynamic_range_bound(
                    op.clone(),
                    value,
                    &mut lower,
                    &mut upper,
                    &mut has_null_bound,
                )?;
                matched = true;
                continue;
            }
            if self.expr_matches_table_index_field(right, table, alias, schema, field)? {
                if self.expr_references_table(left, table, alias, schema)?
                    || !self.expr_is_bound_without_table_row(left)?
                {
                    continue;
                }
                let value = self.eval_dynamic_bound_expr(left)?;
                let value = if let Some(schema) = schema {
                    let value =
                        typed_index_predicate_value(schema, field, index_value_from_sql(value)?)?;
                    sql_value_from_index_value(&value)
                } else {
                    value
                };
                update_dynamic_range_bound(
                    reverse_comparison(op),
                    value,
                    &mut lower,
                    &mut upper,
                    &mut has_null_bound,
                )?;
                matched = true;
            }
        }
        Ok(matched.then_some((lower, upper, has_null_bound)))
    }

    pub(crate) fn dynamic_equality_filters_for_index_fields(
        &self,
        selection: &Expr,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        fields: &[IndexField],
        start_idx: usize,
    ) -> Result<Vec<(usize, IndexValue)>> {
        let mut filters = Vec::new();
        for (idx, field) in fields.iter().enumerate().skip(start_idx) {
            let Some(value) =
                self.dynamic_equality_value(selection, table, alias, schema, field)?
            else {
                continue;
            };
            filters.push((idx, value));
        }
        Ok(filters)
    }

    pub(crate) fn dynamic_sql_range_bounds(
        &self,
        selection: &Expr,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        field: &IndexField,
    ) -> Result<Option<(Option<SqlValue>, Option<SqlValue>, bool)>> {
        let mut lower = None;
        let mut upper = None;
        let mut matched = false;
        let mut has_null_bound = false;
        for term in and_terms(selection) {
            let Expr::BinaryOp { left, op, right } = term else {
                continue;
            };
            if !matches!(
                op,
                BinaryOperator::Gt
                    | BinaryOperator::GtEq
                    | BinaryOperator::Lt
                    | BinaryOperator::LtEq
            ) {
                continue;
            }
            if self.expr_matches_table_index_field(left, table, alias, schema, field)? {
                if self.expr_references_table(right, table, alias, schema)?
                    || !self.expr_is_bound_without_table_row(right)?
                {
                    continue;
                }
                update_dynamic_sql_range_bound(
                    op.clone(),
                    self.eval_dynamic_bound_expr(right)?,
                    &mut lower,
                    &mut upper,
                    &mut has_null_bound,
                );
                matched = true;
                continue;
            }
            if self.expr_matches_table_index_field(right, table, alias, schema, field)? {
                if self.expr_references_table(left, table, alias, schema)?
                    || !self.expr_is_bound_without_table_row(left)?
                {
                    continue;
                }
                update_dynamic_sql_range_bound(
                    reverse_comparison(op),
                    self.eval_dynamic_bound_expr(left)?,
                    &mut lower,
                    &mut upper,
                    &mut has_null_bound,
                );
                matched = true;
            }
        }
        Ok(matched.then_some((lower, upper, has_null_bound)))
    }

    pub(crate) fn dynamic_equality_sql_value(
        &self,
        selection: &Expr,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        column: &str,
    ) -> Result<Option<SqlValue>> {
        match self.dynamic_equality_value_expr(selection, table, alias, schema, column)? {
            Some(expr) => self.eval_dynamic_bound_expr(expr).map(Some),
            None => Ok(None),
        }
    }

    /// Locate-only half of [`Self::dynamic_equality_sql_value`]: find the
    /// value expression bound to `column` by an equality term, without
    /// evaluating it. The location depends only on the statement structure,
    /// so per-statement plans cache it and re-evaluate only the expression.
    /// Structural half of [`Self::dynamic_equality_value_expr`]: the index
    /// of the first AND term that equates `column` with an expression not
    /// referencing the table, and whether the column sits on the left. A
    /// pure function of the statement structure and the schema, so
    /// per-statement plans memoize it and re-evaluate only the expression.
    pub(crate) fn dynamic_equality_term_plan(
        &self,
        selection: &Expr,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        column: &str,
    ) -> Result<Option<(usize, bool)>> {
        for (term_idx, term) in and_terms(selection).into_iter().enumerate() {
            let Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } = term
            else {
                continue;
            };
            if self.expr_matches_table_column(left, table, alias, schema, column)? {
                if self.expr_references_table(right, table, alias, schema)? {
                    continue;
                }
                return Ok(Some((term_idx, true)));
            }
            if self.expr_matches_table_column(right, table, alias, schema, column)? {
                if self.expr_references_table(left, table, alias, schema)? {
                    continue;
                }
                return Ok(Some((term_idx, false)));
            }
        }
        Ok(None)
    }

    pub(crate) fn dynamic_equality_value_expr<'sel>(
        &self,
        selection: &'sel Expr,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        column: &str,
    ) -> Result<Option<&'sel Expr>> {
        for term in and_terms(selection) {
            let Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } = term
            else {
                continue;
            };
            if self.expr_matches_table_column(left, table, alias, schema, column)? {
                if self.expr_references_table(right, table, alias, schema)? {
                    continue;
                }
                return Ok(Some(right));
            }
            if self.expr_matches_table_column(right, table, alias, schema, column)? {
                if self.expr_references_table(left, table, alias, schema)? {
                    continue;
                }
                return Ok(Some(left));
            }
        }
        Ok(None)
    }

    pub(crate) fn expr_matches_table_index_field(
        &self,
        expr: &Expr,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        field: &IndexField,
    ) -> Result<bool> {
        let Some(normalized) = normalize_table_field_expr(expr, table, alias, schema) else {
            return Ok(false);
        };
        match schema {
            Some(schema) => index_field_from_expr_for_schema(schema, &normalized),
            None => index_field_from_expr(&normalized),
        }
        .map(|candidate| index_field_matches(&candidate, field))
    }

    pub(crate) fn expr_matches_table_column(
        &self,
        expr: &Expr,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        column: &str,
    ) -> Result<bool> {
        let Some(normalized) = normalize_table_field_expr(expr, table, alias, schema) else {
            return Ok(false);
        };
        Ok(match normalized {
            Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case(column),
            _ => false,
        })
    }

    pub(crate) fn expr_references_table(
        &self,
        expr: &Expr,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
    ) -> Result<bool> {
        if normalize_table_field_expr(expr, table, alias, schema).is_some() {
            return Ok(true);
        }
        Ok(match expr {
            Expr::Nested(expr)
            | Expr::Cast { expr, .. }
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::IsTrue(expr)
            | Expr::IsNotTrue(expr)
            | Expr::IsFalse(expr)
            | Expr::IsNotFalse(expr)
            | Expr::IsUnknown(expr)
            | Expr::IsNotUnknown(expr) => self.expr_references_table(expr, table, alias, schema)?,
            Expr::BinaryOp { left, right, .. } => {
                self.expr_references_table(left, table, alias, schema)?
                    || self.expr_references_table(right, table, alias, schema)?
            }
            Expr::UnaryOp { expr, .. } => self.expr_references_table(expr, table, alias, schema)?,
            Expr::InList { expr, list, .. } => {
                if self.expr_references_table(expr, table, alias, schema)? {
                    return Ok(true);
                }
                for item in list {
                    if self.expr_references_table(item, table, alias, schema)? {
                        return Ok(true);
                    }
                }
                false
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.expr_references_table(expr, table, alias, schema)?
                    || self.expr_references_table(low, table, alias, schema)?
                    || self.expr_references_table(high, table, alias, schema)?
            }
            Expr::Function(function) => {
                for arg in function_args(function) {
                    if self.expr_references_table(&arg, table, alias, schema)? {
                        return Ok(true);
                    }
                }
                false
            }
            Expr::Array(array) => {
                for arg in &array.elem {
                    if self.expr_references_table(arg, table, alias, schema)? {
                        return Ok(true);
                    }
                }
                false
            }
            _ => false,
        })
    }

    pub(crate) fn eval_dynamic_bound_expr(&self, expr: &Expr) -> Result<SqlValue> {
        if let Some(outer_row) = &self.outer_row {
            let (_scope, context) = self.bound_row_context(outer_row.columns());
            return self.eval_slot_row_value(&outer_row.values, &context, expr);
        }
        self.eval_row_value(&SqlRow::default(), expr)
    }

    pub(crate) fn json_set_function_row_set(&self, call: &JsonSetReturningCall) -> Result<RowSet> {
        if !call.function.accepts_argument_count(call.args.len()) {
            return Err(json_set_function_argument_count_error(
                call.function,
                call.args.len(),
            ));
        }
        if matches!(
            call.function,
            JsonSetReturningFunction::JsonbPathQuery | JsonSetReturningFunction::JsonbPathQueryTz
        ) {
            let arguments = call
                .args
                .iter()
                .map(|argument| self.eval_dynamic_bound_expr(argument))
                .collect::<Result<Vec<_>>>()?;
            let values = eval_jsonpath_query_values(call.function.name(), &arguments)?;
            return self.json_set_function_row_set_from_values(
                call,
                values.into_iter().map(|value| vec![value]).collect(),
            );
        }
        let base = (call.function.json_argument_index() == 1)
            .then(|| self.eval_dynamic_bound_expr(&call.args[0]))
            .transpose()?;
        let value =
            self.eval_dynamic_bound_expr(&call.args[call.function.json_argument_index()])?;
        let flag = call
            .args
            .get(2)
            .map(|expr| self.eval_dynamic_bound_expr(expr))
            .transpose()?;
        validate_json_populate_legacy_flag(call, flag.as_ref())?;
        self.json_set_function_row_set_from_value(call, base, value)
    }

    pub(crate) fn json_set_function_row_set_from_value(
        &self,
        call: &JsonSetReturningCall,
        base: Option<SqlValue>,
        value: SqlValue,
    ) -> Result<RowSet> {
        let values = if call.function.returns_record() {
            json_record_function_rows(call, base, value)?
        } else {
            json_set_function_rows(call.function, value)?
        };
        self.json_set_function_row_set_from_values(call, values)
    }

    pub(crate) fn json_set_function_row_set_from_values(
        &self,
        call: &JsonSetReturningCall,
        values: Vec<Vec<SqlValue>>,
    ) -> Result<RowSet> {
        let (alias_name, columns) = json_set_function_columns(call)?;
        let mut rows = Vec::with_capacity(values.len());
        for (idx, mut source) in values.into_iter().enumerate() {
            if idx % 1024 == 0 {
                self.check_cancellation()?;
            }
            if call.with_ordinality {
                source.push(SqlValue::Int(idx as i64 + 1));
            }
            rows.push(slot_row_from_values(&columns, &source));
        }
        sql_profile_sql_rows_materialized(&rows);
        Ok(RowSet {
            rows,
            columns: aliased_row_output_columns(&alias_name, &columns),
        })
    }

    pub(crate) fn generate_series_row_set(
        &self,
        table: &str,
        table_alias: Option<&TableAlias>,
        args: &TableFunctionArgs,
        with_ordinality: bool,
    ) -> Result<RowSet> {
        if with_ordinality {
            return Err(SqlError::Unsupported(
                "generate_series WITH ORDINALITY is not supported".to_string(),
            ));
        }
        let alias_name = table_alias
            .map(|alias| alias.name.value.clone())
            .unwrap_or_else(|| table.rsplit('.').next().unwrap_or(table).to_string());
        let source_columns = table_function_source_columns_for_alias(
            table_alias,
            &alias_name,
            vec!["generate_series".to_string()],
        );
        let columns = table_alias_columns(
            table,
            &alias_name,
            table_alias
                .map(|alias| alias.columns.as_slice())
                .unwrap_or(&[]),
            &source_columns,
        )?;
        let Some((start, _stop, step, row_count)) = generate_series_bounds(args)? else {
            let output_columns = aliased_row_output_columns(&alias_name, &columns);
            return Ok(RowSet {
                rows: Vec::new(),
                columns: output_columns,
            });
        };
        let mut rows = Vec::new();
        rows.try_reserve(row_count).map_err(|_| {
            SqlError::Unsupported(format!(
                "generate_series result has {row_count} rows and cannot be materialized"
            ))
        })?;
        let mut value = start;
        for idx in 0..row_count {
            if idx % 1024 == 0 {
                self.check_cancellation()?;
            }
            let source = [SqlValue::Int(value)];
            rows.push(slot_row_from_values(&columns, &source));
            if idx + 1 < row_count {
                value = value.checked_add(step).ok_or_else(|| {
                    SqlError::InvalidSql("generate_series integer overflow".to_string())
                })?;
            }
        }
        sql_profile_sql_rows_materialized(&rows);
        let output_columns = aliased_row_output_columns(&alias_name, &columns);
        Ok(RowSet {
            rows,
            columns: output_columns,
        })
    }

    pub(crate) fn regexp_split_to_table_row_set(
        &self,
        table: &str,
        table_alias: Option<&TableAlias>,
        args: &TableFunctionArgs,
        with_ordinality: bool,
    ) -> Result<RowSet> {
        let alias_name = table_alias
            .map(|alias| alias.name.value.clone())
            .unwrap_or_else(|| table.rsplit('.').next().unwrap_or(table).to_string());
        let mut source_columns = table_function_source_columns_for_alias(
            table_alias,
            &alias_name,
            vec!["regexp_split_to_table".to_string()],
        );
        if with_ordinality {
            source_columns.push("ordinality".to_string());
        }
        let columns = table_alias_columns(
            table,
            &alias_name,
            table_alias
                .map(|alias| alias.columns.as_slice())
                .unwrap_or(&[]),
            &source_columns,
        )?;
        let values = table_function_expr_args(args)?
            .iter()
            .map(|expr| self.eval_select_constant_expr(expr))
            .collect::<Result<Vec<_>>>()?;
        let split_values = regexp_split_to_table_values(&values)?;
        let mut rows = Vec::with_capacity(split_values.len());
        for (idx, value) in split_values.into_iter().enumerate() {
            if idx % 1024 == 0 {
                self.check_cancellation()?;
            }
            let mut source = vec![value];
            if with_ordinality {
                source.push(SqlValue::Int(idx as i64 + 1));
            }
            rows.push(slot_row_from_values(&columns, &source));
        }
        sql_profile_sql_rows_materialized(&rows);
        Ok(RowSet {
            rows,
            columns: aliased_row_output_columns(&alias_name, &columns),
        })
    }

    pub(crate) fn unnest_row_set(
        &self,
        table_alias: Option<&TableAlias>,
        array_exprs: &[Expr],
        with_offset: bool,
        with_offset_alias: Option<&Ident>,
        with_ordinality: bool,
    ) -> Result<RowSet> {
        if array_exprs.is_empty() {
            return Err(SqlError::InvalidSql(
                "UNNEST expects at least one array argument".to_string(),
            ));
        }
        let alias_name = table_alias
            .map(|alias| alias.name.value.clone())
            .unwrap_or_else(|| "unnest".to_string());
        let source_columns = table_function_source_columns_for_alias(
            table_alias,
            &alias_name,
            unnest_source_columns(
                array_exprs.len(),
                with_offset,
                with_offset_alias,
                with_ordinality,
            ),
        );
        let columns = table_alias_columns(
            "unnest",
            &alias_name,
            table_alias
                .map(|alias| alias.columns.as_slice())
                .unwrap_or(&[]),
            &source_columns,
        )?;
        let value_sets = array_exprs
            .iter()
            .map(|expr| {
                self.eval_select_constant_expr(expr)
                    .and_then(unnest_values_from_sql)
            })
            .collect::<Result<Vec<_>>>()?;
        let row_count = value_sets.iter().map(Vec::len).max().unwrap_or_default();
        let mut rows = Vec::with_capacity(row_count);
        for idx in 0..row_count {
            if idx % 1024 == 0 {
                self.check_cancellation()?;
            }
            let mut values = value_sets
                .iter()
                .map(|values| values.get(idx).cloned().unwrap_or(SqlValue::Null))
                .collect::<Vec<_>>();
            if with_offset {
                values.push(SqlValue::Int(idx as i64));
            }
            if with_ordinality {
                values.push(SqlValue::Int(idx as i64 + 1));
            }
            rows.push(slot_row_from_values(&columns, &values));
        }
        sql_profile_sql_rows_materialized(&rows);
        Ok(RowSet {
            rows,
            columns: aliased_row_output_columns(&alias_name, &columns),
        })
    }

    pub(crate) fn catalog_table_function_row_set(
        &self,
        table: &str,
        table_alias: Option<&TableAlias>,
        args: &TableFunctionArgs,
        with_ordinality: bool,
    ) -> Result<RowSet> {
        if with_ordinality {
            return Err(SqlError::Unsupported(format!(
                "{table} WITH ORDINALITY is not supported"
            )));
        }
        validate_zero_arg_table_function(table, args)?;
        let alias_name = table_alias
            .map(|alias| alias.name.value.clone())
            .unwrap_or_else(|| table.rsplit('.').next().unwrap_or(table).to_string());
        let source_rows =
            self.filter_virtual_rows_for_role(table, virtual_rows(self.db_ref(), table)?)?;
        let mut source_columns = virtual_table_columns(table).unwrap_or_else(|| {
            source_rows
                .iter()
                .flat_map(|row| row.keys().cloned())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect()
        });
        add_virtual_tableoid_column(table, &mut source_columns);
        let columns = table_alias_columns(
            table,
            &alias_name,
            table_alias
                .map(|alias| alias.columns.as_slice())
                .unwrap_or(&[]),
            &source_columns,
        )?;
        let rows = source_rows
            .iter()
            .enumerate()
            .map(|(idx, row)| {
                if idx % 1024 == 0 {
                    self.check_cancellation()?;
                }
                let values = source_columns
                    .iter()
                    .map(|column| virtual_cell(row, column))
                    .collect::<Vec<_>>();
                Ok(slot_row_from_values(&columns, &values))
            })
            .collect::<Result<Vec<_>>>()?;
        sql_profile_sql_rows_materialized(&rows);
        Ok(RowSet {
            rows,
            columns: aliased_row_output_columns(&alias_name, &columns),
        })
    }

    pub(crate) fn stored_routine_table_function_row_set(
        &self,
        table: &str,
        table_alias: Option<&TableAlias>,
        args: &TableFunctionArgs,
        with_ordinality: bool,
    ) -> Result<Option<RowSet>> {
        if with_ordinality {
            return Err(SqlError::Unsupported(format!(
                "{table} WITH ORDINALITY is not supported"
            )));
        }
        let Some(routine) = resolve_routine_cached(self.db_ref(), RoutineKind::Function, table)?
        else {
            return Ok(None);
        };
        if !routine.schema.returns_set {
            return Err(SqlError::UnsupportedPlpgsqlFeature(format!(
                "stored routine {} is not set-returning",
                routine.schema.name
            )));
        }
        if !routine.schema.language.eq_ignore_ascii_case("sql") {
            return Err(SqlError::UnsupportedPlpgsqlFeature(format!(
                "set-returning {} routine {} is not implemented",
                routine.schema.language, routine.schema.name
            )));
        }
        let role = current_user_from_gucs(&self.session_gucs);
        if !role_can_execute_routine(self.db_ref(), &role, table)? {
            return Err(SqlError::BicDb(BicDbError::Authorization(format!(
                "permission denied for function {}",
                normalize_object_name(table)
            ))));
        }
        let arguments = table_function_expr_args(args)?
            .iter()
            .map(|argument| self.eval_select_constant_expr(argument))
            .collect::<Result<Vec<_>>>()?;
        let mut frame = RoutineFrame::new_with_symbols(
            &routine.ir.params,
            &arguments,
            &routine.ir.symbol_names,
        )?;
        let mut session_gucs = (*self.session_gucs).clone();
        if routine.schema.security_definer {
            // No recorded owner -> invoker semantics, never a superuser.
            if let Some(owner) = routine.schema.definer_owner() {
                session_gucs.insert(CURRENT_ROLE_GUC.to_string(), owner.to_string());
            }
        }
        let routine_engine = self.inherit_transaction(
            SqlEngine::new_with_settings_and_context(
                self.db_ref(),
                self.settings,
                self.security_context.clone(),
            )
            .with_session_gucs(Arc::new(session_gucs))
            .with_shared_routine_vars(frame.materialized_values())
            .with_cancellation(self.cancellation.clone()),
        );
        let mut result = None;
        for statement in &routine.ir.statements {
            let RoutineStmt::Sql(Statement::Query(query)) = statement else {
                return Err(SqlError::UnsupportedPlpgsqlFeature(format!(
                    "SQL function {} contains a non-query statement",
                    routine.schema.name
                )));
            };
            result = Some(routine_engine.execute_query(query)?);
        }
        let result = result.unwrap_or_else(|| SqlResult::new(Vec::new(), Vec::new()));
        let alias_name = table_alias
            .map(|alias| alias.name.value.clone())
            .unwrap_or_else(|| table.rsplit('.').next().unwrap_or(table).to_string());
        let columns = table_alias_columns(
            table,
            &alias_name,
            table_alias
                .map(|alias| alias.columns.as_slice())
                .unwrap_or(&[]),
            &result.columns,
        )?;
        let rows = result
            .rows
            .iter()
            .enumerate()
            .map(|(index, row)| {
                if index % 1024 == 0 {
                    self.check_cancellation()?;
                }
                Ok(slot_row_from_values(&columns, row))
            })
            .collect::<Result<Vec<_>>>()?;
        sql_profile_sql_rows_materialized(&rows);
        Ok(Some(RowSet {
            rows,
            columns: aliased_row_output_columns(&alias_name, &columns),
        }))
    }

    pub(crate) fn apply_row_join(
        &self,
        mut left: RowSet,
        join: &Join,
        selection: Option<&Expr>,
        prior_inner_selection: Option<&Expr>,
        needed_columns: Option<&ReferencedColumns>,
    ) -> Result<RowSet> {
        let (kind, constraint) = match &join.join_operator {
            JoinOperator::Join(constraint)
            | JoinOperator::Inner(constraint)
            | JoinOperator::CrossJoin(constraint) => (RowJoinKind::Inner, constraint),
            JoinOperator::Left(constraint) | JoinOperator::LeftOuter(constraint) => {
                (RowJoinKind::Left, constraint)
            }
            JoinOperator::Right(constraint) | JoinOperator::RightOuter(constraint) => {
                (RowJoinKind::Right, constraint)
            }
            JoinOperator::FullOuter(constraint) => (RowJoinKind::FullOuter, constraint),
            other => {
                return Err(SqlError::Unsupported(format!(
                    "join type {other:?} is not supported"
                )));
            }
        };
        let derived_constraint =
            if matches!(kind, RowJoinKind::Inner) && matches!(constraint, JoinConstraint::None) {
                self.derive_where_join_constraint(&left, &join.relation, selection)?
            } else {
                None
            };
        let effective_constraint = derived_constraint.as_ref().unwrap_or(constraint);
        if matches!(kind, RowJoinKind::Inner | RowJoinKind::Left) {
            if let Some(call) = json_set_returning_call(&join.relation)? {
                if call.permits_correlation {
                    return self.apply_lateral_json_set_function_join(
                        left,
                        &call,
                        kind,
                        effective_constraint,
                    );
                }
            }
            if let TableFactor::Derived {
                lateral: true,
                subquery,
                alias,
                ..
            } = &join.relation
            {
                return self.apply_lateral_derived_table_join(
                    left,
                    subquery,
                    alias.as_ref(),
                    kind,
                    effective_constraint,
                );
            }
        }
        if left.rows.is_empty() && matches!(kind, RowJoinKind::Inner | RowJoinKind::Left) {
            if let Some(right_columns) =
                self.row_set_columns_from_table_factor_without_rows(&join.relation)?
            {
                let columns = merge_row_set_columns(left.columns, right_columns);
                sql_profile_join_rows::<SlotRow>(&[], 0);
                return Ok(RowSet {
                    rows: Vec::new(),
                    columns,
                });
            }
        }
        let right_seed_selection = if matches!(kind, RowJoinKind::Inner) {
            prior_inner_selection.or(selection)
        } else {
            None
        };
        if matches!(kind, RowJoinKind::Inner | RowJoinKind::Left) {
            if let Some(row_set) = self.apply_indexed_right_table_join(
                &mut left,
                &join.relation,
                kind,
                effective_constraint,
                needed_columns,
            )? {
                return Ok(row_set);
            }
        }
        // The indexed path has already consumed the join predicate. Construct
        // the cloned combined AST only when the scan fallback actually needs it.
        let combined_inner_selection = matches!(kind, RowJoinKind::Inner)
            .then(|| selection_with_join_constraint(right_seed_selection, effective_constraint))
            .flatten();
        let mut right = self.row_set_from_table_factor_with_selection(
            &join.relation,
            combined_inner_selection.as_ref().or(right_seed_selection),
            needed_columns,
        )?;
        if matches!(kind, RowJoinKind::Inner) {
            self.apply_pushable_base_predicates(&mut right, selection)?;
        }
        if let Some(row_set) =
            self.apply_spatial_tree_join(&left, &right, kind, effective_constraint)?
        {
            return Ok(row_set);
        }
        if let (RowJoinKind::Inner, Some((left_key, right_key))) = (
            kind,
            equi_join_key_exprs(effective_constraint, &left.columns, &right.columns),
        ) {
            let mut right_type_env = Vec::new();
            let right_key_type = self
                .collect_relation_columns(&join.relation, &mut right_type_env)
                .and_then(|_| self.infer_env_expr_type(right_key, &right_type_env));
            // The existing single-equality path has its typed key contract.
            // For a newly extracted conjunct, only plain integer columns are
            // admitted: textual/numeric/boolean coercions can compare equal
            // while producing different untyped labels. Keep those shapes on
            // the ordinary evaluator until their common key type is known.
            let simple_equality = matches!(
                effective_constraint,
                JoinConstraint::On(expr)
                    if matches!(unwrap_nested_expr(expr), Expr::BinaryOp { op: BinaryOperator::Eq, .. })
            );
            fn integer_column_key(expr: &Expr, set: &RowSet) -> bool {
                let parts = match unwrap_nested_expr(expr) {
                    Expr::Identifier(ident) => vec![ident.value.clone()],
                    Expr::CompoundIdentifier(parts) => {
                        parts.iter().map(|ident| ident.value.clone()).collect()
                    }
                    _ => return false,
                };
                set.rows.iter().all(|row| {
                    matches!(
                        slot_row_value_from_parts(&set.columns, row, &parts),
                        SqlValue::Int(_) | SqlValue::Null
                    )
                })
            }
            if simple_equality
                || (integer_column_key(left_key, &left) && integer_column_key(right_key, &right))
            {
                return self.apply_inner_hash_join(
                    left,
                    right,
                    effective_constraint,
                    left_key,
                    right_key,
                    right_key_type.as_deref(),
                );
            }
        }
        let candidate_pairs = left.rows.len().saturating_mul(right.rows.len());
        let pair_ceiling = max_nested_join_pairs();
        if pair_ceiling != 0 && candidate_pairs > pair_ceiling {
            return Err(SqlError::Unsupported(format!(
                "join would enumerate {candidate_pairs} row pairs, exceeding the                  nested-loop limit of {pair_ceiling} (add a join condition, or raise                  BICDB_MAX_NESTED_JOIN_PAIRS)"
            )));
        }

        let mut rows = Vec::new();
        let mut matched_right = vec![false; right.rows.len()];
        let null_left = null_slot_row_for_columns(&left.columns);
        let null_right = null_slot_row_for_columns(&right.columns);

        for (left_idx, left_row) in left.rows.iter().enumerate() {
            if left_idx % 128 == 0 {
                self.check_cancellation()?;
            }
            let mut matched_left = false;
            for (right_idx, right_row) in right.rows.iter().enumerate() {
                if right_idx % 1024 == 0 {
                    self.check_cancellation()?;
                }
                if self.join_constraint_matches(
                    left_row,
                    &left.columns,
                    right_row,
                    &right.columns,
                    effective_constraint,
                )? {
                    matched_left = true;
                    matched_right[right_idx] = true;
                    rows.push(merge_slot_rows(
                        left_row,
                        &left.columns,
                        right_row,
                        &right.columns,
                    ));
                }
            }
            if !matched_left && matches!(kind, RowJoinKind::Left | RowJoinKind::FullOuter) {
                rows.push(merge_slot_rows(
                    left_row,
                    &left.columns,
                    &null_right,
                    &right.columns,
                ));
            }
        }

        if matches!(kind, RowJoinKind::Right | RowJoinKind::FullOuter) {
            for (right_idx, right_row) in right.rows.iter().enumerate() {
                if right_idx % 1024 == 0 {
                    self.check_cancellation()?;
                }
                if !matched_right[right_idx] {
                    rows.push(merge_slot_rows(
                        &null_left,
                        &left.columns,
                        right_row,
                        &right.columns,
                    ));
                }
            }
        }

        let mut columns = left.columns;
        for column in right.columns {
            if !columns.iter().any(|existing| existing == &column) {
                columns.push(column);
            }
        }
        sql_profile_join_rows(&rows, candidate_pairs);
        Ok(RowSet { rows, columns })
    }

    pub(crate) fn apply_lateral_json_set_function_join(
        &self,
        left: RowSet,
        call: &JsonSetReturningCall,
        kind: RowJoinKind,
        constraint: &JoinConstraint,
    ) -> Result<RowSet> {
        if !call.function.accepts_argument_count(call.args.len()) {
            return Err(json_set_function_argument_count_error(
                call.function,
                call.args.len(),
            ));
        }
        let (alias_name, source_columns) = json_set_function_columns(call)?;
        let right_columns = aliased_row_output_columns(&alias_name, &source_columns);
        let null_right = null_slot_row_for_columns(&right_columns);
        let (scope, context) = self.bound_row_context(&left.columns);
        let bound_args = call
            .args
            .iter()
            .map(|arg| scope.bind(arg))
            .collect::<Vec<_>>();
        let mut rows = Vec::new();
        let mut candidate_pairs = 0usize;

        for (left_idx, left_row) in left.rows.iter().enumerate() {
            if left_idx % 128 == 0 {
                self.check_cancellation()?;
            }
            let mut arguments = Vec::with_capacity(call.args.len());
            for (arg, bound) in call.args.iter().zip(&bound_args) {
                arguments.push(match bound {
                    Some(bound) => bound.eval(&BoundExprFrame {
                        user_calls: &[],
                        db: self.db_ref(),
                        columns: BoundExprColumns::Values(left_row),
                        vars: &context.var_values,
                    })?,
                    None => self.eval_slot_row_value(left_row, &context, arg)?,
                });
            }
            if matches!(
                call.function,
                JsonSetReturningFunction::JsonbPathQuery
                    | JsonSetReturningFunction::JsonbPathQueryTz
            ) {
                let values = eval_jsonpath_query_values(call.function.name(), &arguments)?;
                let right = self.json_set_function_row_set_from_values(
                    call,
                    values.into_iter().map(|value| vec![value]).collect(),
                )?;
                candidate_pairs = candidate_pairs.saturating_add(right.rows.len());
                let mut matched_left = false;
                for right_row in &right.rows {
                    if self.join_constraint_matches(
                        left_row,
                        &left.columns,
                        right_row,
                        &right_columns,
                        constraint,
                    )? {
                        matched_left = true;
                        rows.push(merge_slot_rows(
                            left_row,
                            &left.columns,
                            right_row,
                            &right_columns,
                        ));
                    }
                }
                if !matched_left && matches!(kind, RowJoinKind::Left) {
                    rows.push(merge_slot_rows(
                        left_row,
                        &left.columns,
                        &null_right,
                        &right_columns,
                    ));
                }
                continue;
            }
            let json_index = call.function.json_argument_index();
            validate_json_populate_legacy_flag(call, arguments.get(2))?;
            let base = (json_index == 1).then(|| arguments[0].clone());
            let right = self.json_set_function_row_set_from_value(
                call,
                base,
                arguments[json_index].clone(),
            )?;
            candidate_pairs = candidate_pairs.saturating_add(right.rows.len());
            let mut matched_left = false;
            for (right_idx, right_row) in right.rows.iter().enumerate() {
                if right_idx % 1024 == 0 {
                    self.check_cancellation()?;
                }
                if self.join_constraint_matches(
                    left_row,
                    &left.columns,
                    right_row,
                    &right_columns,
                    constraint,
                )? {
                    matched_left = true;
                    rows.push(merge_slot_rows(
                        left_row,
                        &left.columns,
                        right_row,
                        &right_columns,
                    ));
                }
            }
            if !matched_left && matches!(kind, RowJoinKind::Left) {
                rows.push(merge_slot_rows(
                    left_row,
                    &left.columns,
                    &null_right,
                    &right_columns,
                ));
            }
        }

        let columns = merge_row_set_columns(left.columns, right_columns);
        sql_profile_join_rows(&rows, candidate_pairs);
        Ok(RowSet { rows, columns })
    }

    pub(crate) fn apply_lateral_derived_table_join(
        &self,
        left: RowSet,
        subquery: &Query,
        alias: Option<&TableAlias>,
        kind: RowJoinKind,
        constraint: &JoinConstraint,
    ) -> Result<RowSet> {
        debug_assert!(matches!(kind, RowJoinKind::Inner | RowJoinKind::Left));
        let alias_name = alias
            .map(|alias| alias.name.value.clone())
            .unwrap_or_else(|| "subquery".to_string());
        let alias_columns = alias.map(|alias| alias.columns.as_slice()).unwrap_or(&[]);
        let mut right_columns = self
            .set_expr_output_columns(subquery.body.as_ref())
            .map(|columns| {
                columns
                    .into_iter()
                    .map(|(name, _)| name)
                    .collect::<Vec<_>>()
            })
            .map(|columns| {
                table_alias_columns(&alias_name, &alias_name, alias_columns, &columns)
                    .map(|columns| aliased_row_output_columns(&alias_name, &columns))
            })
            .transpose()?;

        let mut rows = Vec::new();
        let mut candidate_pairs = 0usize;
        for (left_idx, left_row) in left.rows.iter().enumerate() {
            if left_idx % 128 == 0 {
                self.check_cancellation()?;
            }
            let outer_row =
                extend_outer_slot_context(self.outer_row.as_ref(), &left.columns, left_row);
            let result = self
                .inherit_transaction(SqlEngine::with_ctes_and_context(
                    self.db_ref(),
                    self.settings,
                    self.ctes.clone(),
                    self.security_context.clone(),
                    self.session_gucs.clone(),
                ))
                .with_shared_routine_vars(self.routine_vars.clone())
                .with_outer_slot_row(outer_row)
                .with_cancellation(self.cancellation.clone())
                .execute_query(subquery)?;
            let source_columns =
                table_alias_columns(&alias_name, &alias_name, alias_columns, &result.columns)?;
            let actual_right_columns = aliased_row_output_columns(&alias_name, &source_columns);
            if let Some(columns) = &right_columns {
                if columns != &actual_right_columns {
                    return Err(SqlError::InvalidSql(
                        "LATERAL derived table returned inconsistent columns".to_string(),
                    ));
                }
            } else {
                right_columns = Some(actual_right_columns);
            }
            let right_columns_ref = right_columns
                .as_ref()
                .expect("actual lateral result defines columns");
            let null_right = null_slot_row_for_columns(right_columns_ref);
            let mut matched_left = false;
            candidate_pairs = candidate_pairs.saturating_add(result.rows.len());
            for (right_idx, right_row) in result.rows.iter().enumerate() {
                if right_idx % 1024 == 0 {
                    self.check_cancellation()?;
                }
                let right_row = slot_row_from_values(&source_columns, right_row);
                if self.join_constraint_matches(
                    left_row,
                    &left.columns,
                    &right_row,
                    right_columns_ref,
                    constraint,
                )? {
                    matched_left = true;
                    rows.push(merge_slot_rows(
                        left_row,
                        &left.columns,
                        &right_row,
                        right_columns_ref,
                    ));
                }
            }
            if !matched_left && matches!(kind, RowJoinKind::Left) {
                rows.push(merge_slot_rows(
                    left_row,
                    &left.columns,
                    &null_right,
                    right_columns_ref,
                ));
            }
        }

        let columns = merge_row_set_columns(left.columns, right_columns.unwrap_or_default());
        sql_profile_join_rows(&rows, candidate_pairs);
        Ok(RowSet { rows, columns })
    }

    pub(crate) fn derive_where_join_constraint(
        &self,
        left: &RowSet,
        right_relation: &TableFactor,
        selection: Option<&Expr>,
    ) -> Result<Option<JoinConstraint>> {
        let Some(selection) = selection else {
            return Ok(None);
        };
        let TableFactor::Table {
            name,
            alias,
            args: None,
            ..
        } = right_relation
        else {
            return Ok(None);
        };
        let table = relation_name(name)?;
        if self.cte(&table).is_some()
            || is_virtual_table(&table)
            || load_view(self.db_ref(), &table)?.is_some()
            || load_sequence(self.db_ref(), &table)?.is_some()
        {
            return Ok(None);
        }
        let alias_name = alias
            .as_ref()
            .map(|alias| alias.name.value.clone())
            .unwrap_or_else(|| table.rsplit('.').next().unwrap_or(&table).to_string());
        let table = resolve_session_relation_name(self.db_ref(), &table)?;
        let schema = load_schema(self.db_ref(), &table)?;
        let available_columns = available_join_columns(&left.columns);
        let mut derived = None;
        for term in and_terms(selection) {
            if !self.where_term_applies_to_right_table(
                term,
                &table,
                &alias_name,
                schema.as_ref(),
                &available_columns,
            )? {
                continue;
            }
            derived = Some(match derived {
                Some(existing) => and_expr(existing, term.clone()),
                None => term.clone(),
            });
        }
        Ok(derived.map(JoinConstraint::On))
    }

    pub(crate) fn where_term_applies_to_right_table(
        &self,
        term: &Expr,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        available_columns: &BTreeSet<String>,
    ) -> Result<bool> {
        if !self.expr_references_table(term, table, alias, schema)? {
            return Ok(false);
        }
        self.expr_is_available_from_join_inputs(term, table, alias, schema, available_columns)
    }

    pub(crate) fn expr_is_available_from_join_inputs(
        &self,
        expr: &Expr,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        available_columns: &BTreeSet<String>,
    ) -> Result<bool> {
        Ok(match expr {
            Expr::Value(_) | Expr::TypedString(_) | Expr::Interval(_) => true,
            Expr::Identifier(ident) => {
                normalize_table_field_expr(expr, table, alias, schema).is_some()
                    || routine_var_from_ident(&self.routine_vars, ident).is_some()
                    || join_column_reference_available(
                        std::slice::from_ref(&ident.value),
                        available_columns,
                    )
            }
            Expr::CompoundIdentifier(idents) => {
                let parts = idents
                    .iter()
                    .map(|ident| ident.value.clone())
                    .collect::<Vec<_>>();
                normalize_table_field_expr(expr, table, alias, schema).is_some()
                    || routine_var_from_parts(&self.routine_vars, &parts)?.is_some()
                    || join_column_reference_available(&parts, available_columns)
            }
            Expr::Nested(expr)
            | Expr::Cast { expr, .. }
            | Expr::UnaryOp { expr, .. }
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::IsTrue(expr)
            | Expr::IsNotTrue(expr)
            | Expr::IsFalse(expr)
            | Expr::IsNotFalse(expr)
            | Expr::IsUnknown(expr)
            | Expr::IsNotUnknown(expr)
            | Expr::Collate { expr, .. } => self.expr_is_available_from_join_inputs(
                expr,
                table,
                alias,
                schema,
                available_columns,
            )?,
            Expr::BinaryOp { left, right, .. }
            | Expr::AnyOp { left, right, .. }
            | Expr::AllOp { left, right, .. } => {
                self.expr_is_available_from_join_inputs(
                    left,
                    table,
                    alias,
                    schema,
                    available_columns,
                )? && self.expr_is_available_from_join_inputs(
                    right,
                    table,
                    alias,
                    schema,
                    available_columns,
                )?
            }
            Expr::Function(function) => self.exprs_are_available_from_join_inputs(
                &function_args(function),
                table,
                alias,
                schema,
                available_columns,
            )?,
            Expr::Array(array) => self.exprs_are_available_from_join_inputs(
                &array.elem,
                table,
                alias,
                schema,
                available_columns,
            )?,
            Expr::Position { expr, r#in } => {
                self.expr_is_available_from_join_inputs(
                    expr,
                    table,
                    alias,
                    schema,
                    available_columns,
                )? && self.expr_is_available_from_join_inputs(
                    r#in,
                    table,
                    alias,
                    schema,
                    available_columns,
                )?
            }
            Expr::Extract { expr, .. } => self.expr_is_available_from_join_inputs(
                expr,
                table,
                alias,
                schema,
                available_columns,
            )?,
            _ => false,
        })
    }

    pub(crate) fn exprs_are_available_from_join_inputs(
        &self,
        exprs: &[Expr],
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        available_columns: &BTreeSet<String>,
    ) -> Result<bool> {
        for expr in exprs {
            if !self.expr_is_available_from_join_inputs(
                expr,
                table,
                alias,
                schema,
                available_columns,
            )? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(crate) fn row_set_columns_from_table_factor_without_rows(
        &self,
        relation: &TableFactor,
    ) -> Result<Option<Vec<String>>> {
        if let Some(call) = json_set_returning_call(relation)? {
            let (alias_name, columns) = json_set_function_columns(&call)?;
            return Ok(Some(aliased_row_output_columns(&alias_name, &columns)));
        }
        if let TableFactor::UNNEST {
            alias,
            array_exprs,
            with_offset,
            with_offset_alias,
            with_ordinality,
        } = relation
        {
            if array_exprs.is_empty() {
                return Err(SqlError::InvalidSql(
                    "UNNEST expects at least one array argument".to_string(),
                ));
            }
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
            let columns = table_alias_columns(
                "unnest",
                &alias_name,
                alias
                    .as_ref()
                    .map(|alias| alias.columns.as_slice())
                    .unwrap_or(&[]),
                &source_columns,
            )?;
            return Ok(Some(aliased_row_output_columns(&alias_name, &columns)));
        }

        let TableFactor::Table {
            name,
            alias,
            args,
            with_ordinality,
            ..
        } = relation
        else {
            return Ok(None);
        };
        // Keep schema-qualified function names in their logical catalog form.
        // Physical schema isolation applies to relations, never routines.
        let table = if args.is_some() {
            normalize_object_name(&object_name(name)?)
        } else {
            relation_name(name)?
        };
        let table_alias = alias.as_ref();
        let alias_name = table_alias
            .map(|alias| alias.name.value.clone())
            .unwrap_or_else(|| table.rsplit('.').next().unwrap_or(&table).to_string());

        if let Some(args) = args {
            if is_generate_series_table_function(&table) {
                if *with_ordinality {
                    return Err(SqlError::Unsupported(
                        "generate_series WITH ORDINALITY is not supported".to_string(),
                    ));
                }
                let source_columns = vec!["generate_series".to_string()];
                let columns = table_alias_columns(
                    &table,
                    &alias_name,
                    table_alias
                        .map(|alias| alias.columns.as_slice())
                        .unwrap_or(&[]),
                    &source_columns,
                )?;
                return Ok(Some(aliased_row_output_columns(&alias_name, &columns)));
            }
            if is_regexp_split_to_table_function(&table) {
                let mut source_columns = table_function_source_columns_for_alias(
                    table_alias,
                    &alias_name,
                    vec!["regexp_split_to_table".to_string()],
                );
                if *with_ordinality {
                    source_columns.push("ordinality".to_string());
                }
                let columns = table_alias_columns(
                    &table,
                    &alias_name,
                    table_alias
                        .map(|alias| alias.columns.as_slice())
                        .unwrap_or(&[]),
                    &source_columns,
                )?;
                return Ok(Some(aliased_row_output_columns(&alias_name, &columns)));
            }
            if is_one_row_catalog_table_function(&table) {
                if *with_ordinality {
                    return Err(SqlError::Unsupported(format!(
                        "{table} WITH ORDINALITY is not supported"
                    )));
                }
                validate_zero_arg_table_function(&table, args)?;
                if let Some(source_columns) = virtual_table_columns(&table) {
                    let columns = table_alias_columns(
                        &table,
                        &alias_name,
                        table_alias
                            .map(|alias| alias.columns.as_slice())
                            .unwrap_or(&[]),
                        &source_columns,
                    )?;
                    return Ok(Some(aliased_row_output_columns(&alias_name, &columns)));
                }
                return Ok(None);
            }
            if let Some(routine) =
                resolve_routine_cached(self.db_ref(), RoutineKind::Function, &table)?
            {
                if !routine.schema.returns_set {
                    return Err(SqlError::UnsupportedPlpgsqlFeature(format!(
                        "stored routine {} is not set-returning",
                        routine.schema.name
                    )));
                }
                if !routine.schema.language.eq_ignore_ascii_case("sql") {
                    return Err(SqlError::UnsupportedPlpgsqlFeature(format!(
                        "set-returning {} routine {} is not implemented",
                        routine.schema.language, routine.schema.name
                    )));
                }
                let source_columns = if routine.schema.return_type.eq_ignore_ascii_case("record") {
                    table_alias
                        .map(|alias| {
                            alias
                                .columns
                                .iter()
                                .map(|column| column.name.value.clone())
                                .collect::<Vec<_>>()
                        })
                        .filter(|columns| !columns.is_empty())
                        .unwrap_or_else(|| vec![alias_name.clone()])
                } else {
                    vec![table.rsplit('.').next().unwrap_or(&table).to_string()]
                };
                let columns = table_alias_columns(
                    &table,
                    &alias_name,
                    table_alias
                        .map(|alias| alias.columns.as_slice())
                        .unwrap_or(&[]),
                    &source_columns,
                )?;
                return Ok(Some(aliased_row_output_columns(&alias_name, &columns)));
            }
            return Err(SqlError::Unsupported(format!(
                "table function {table} is not supported"
            )));
        }

        if let Some(cte) = self.cte(&table) {
            let columns = table_alias_columns(
                &table,
                &alias_name,
                table_alias
                    .map(|alias| alias.columns.as_slice())
                    .unwrap_or(&[]),
                &cte.columns,
            )?;
            return Ok(Some(aliased_row_output_columns(&alias_name, &columns)));
        }

        if let Some(view) = load_view(self.db_ref(), &table)? {
            let source_columns = catalog_view_columns(&view)
                .into_iter()
                .map(|column| column.name)
                .collect::<Vec<_>>();
            let columns = table_alias_columns(
                &table,
                &alias_name,
                table_alias
                    .map(|alias| alias.columns.as_slice())
                    .unwrap_or(&[]),
                &source_columns,
            )?;
            return Ok(Some(aliased_row_output_columns(&alias_name, &columns)));
        }

        if is_virtual_table(&table) {
            let Some(source_columns) = virtual_table_columns_for_empty_join(&table) else {
                return Ok(None);
            };
            let columns = table_alias_columns(
                &table,
                &alias_name,
                table_alias
                    .map(|alias| alias.columns.as_slice())
                    .unwrap_or(&[]),
                &source_columns,
            )?;
            return Ok(Some(aliased_row_output_columns(&alias_name, &columns)));
        }

        if load_sequence(self.db_ref(), &table)?.is_some() {
            let source_columns = vec![
                "last_value".to_string(),
                "log_cnt".to_string(),
                "is_called".to_string(),
            ];
            let columns = table_alias_columns(
                &table,
                &alias_name,
                table_alias
                    .map(|alias| alias.columns.as_slice())
                    .unwrap_or(&[]),
                &source_columns,
            )?;
            return Ok(Some(aliased_row_output_columns(&alias_name, &columns)));
        }

        let table = resolve_session_relation_name(self.db_ref(), &table)?;

        let schema = load_schema_shared(self.db_ref(), &table)?;
        Ok(Some(row_output_columns(
            &table,
            &alias_name,
            schema.as_deref(),
        )))
    }

    pub(crate) fn estimate_unnest_rows(&self, array_exprs: &[Expr]) -> Result<usize> {
        let mut estimate = 0;
        for expr in array_exprs {
            match self
                .eval_select_constant_expr(expr)
                .and_then(unnest_values_from_sql)
            {
                Ok(values) => estimate = estimate.max(values.len()),
                Err(SqlError::Unsupported(_)) => return Ok(usize::MAX / 2),
                Err(error) => return Err(error),
            }
        }
        Ok(estimate)
    }

    pub(crate) fn apply_inner_hash_join(
        &self,
        left: RowSet,
        right: RowSet,
        constraint: &JoinConstraint,
        left_key: &Expr,
        right_key: &Expr,
        pg_type: Option<&str>,
    ) -> Result<RowSet> {
        let mut right_by_key = BTreeMap::<String, Vec<usize>>::new();
        let (right_scope, right_context) = self.bound_row_context(&right.columns);
        let right_key_bound = right_scope.bind(right_key);
        for (right_idx, right_row) in right.rows.iter().enumerate() {
            if right_idx % 1024 == 0 {
                self.check_cancellation()?;
            }
            let value = match &right_key_bound {
                Some(bound) => bound.eval(&BoundExprFrame {
                    user_calls: &[],
                    db: self.db_ref(),
                    columns: BoundExprColumns::Values(right_row),
                    vars: &right_context.var_values,
                })?,
                None => self.eval_slot_row_value(right_row, &right_context, right_key)?,
            };
            let key = match (pg_type, value) {
                (_, SqlValue::Null) => None,
                (Some(pg_type), value) => {
                    Some(pg_typed_index_label_for_db(self.db_ref(), pg_type, &value)?)
                }
                (None, value) => join_key_value(value),
            };
            if let Some(key) = key {
                right_by_key.entry(key).or_default().push(right_idx);
            }
        }

        let mut rows = Vec::new();
        let mut candidate_pairs = 0_usize;
        let (left_scope, left_context) = self.bound_row_context(&left.columns);
        let left_key_bound = left_scope.bind(left_key);
        for (left_idx, left_row) in left.rows.iter().enumerate() {
            if left_idx % 128 == 0 {
                self.check_cancellation()?;
            }
            let value = match &left_key_bound {
                Some(bound) => bound.eval(&BoundExprFrame {
                    user_calls: &[],
                    db: self.db_ref(),
                    columns: BoundExprColumns::Values(left_row),
                    vars: &left_context.var_values,
                })?,
                None => self.eval_slot_row_value(left_row, &left_context, left_key)?,
            };
            let key = match (pg_type, value) {
                (_, SqlValue::Null) => None,
                (Some(pg_type), value) => {
                    Some(pg_typed_index_label_for_db(self.db_ref(), pg_type, &value)?)
                }
                (None, value) => join_key_value(value),
            };
            let Some(key) = key else {
                continue;
            };
            let Some(matches) = right_by_key.get(&key) else {
                continue;
            };
            candidate_pairs = candidate_pairs.saturating_add(matches.len());
            for right_idx in matches {
                let right_row = &right.rows[*right_idx];
                let typed_equality_is_complete = pg_type.is_some()
                    && matches!(
                        constraint,
                        JoinConstraint::On(Expr::BinaryOp {
                            op: BinaryOperator::Eq,
                            ..
                        })
                    );
                if typed_equality_is_complete
                    || self.join_constraint_matches(
                        left_row,
                        &left.columns,
                        right_row,
                        &right.columns,
                        constraint,
                    )?
                {
                    rows.push(merge_slot_rows(
                        left_row,
                        &left.columns,
                        right_row,
                        &right.columns,
                    ));
                }
            }
        }

        let mut columns = left.columns;
        for column in right.columns {
            if !columns.iter().any(|existing| existing == &column) {
                columns.push(column);
            }
        }
        sql_profile_join_rows(&rows, candidate_pairs);
        Ok(RowSet { rows, columns })
    }

    pub(crate) fn prepare_dynamic_record_lookup(
        &self,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        selection: &Expr,
        available_columns: &[String],
    ) -> Result<Option<PreparedDynamicRecordLookup>> {
        let (scope, context) = self.bound_row_context(available_columns);
        // Typed primary keys can have legacy physical record IDs. The regular
        // PK path performs the compatibility scan; the prepared point path
        // intentionally falls back rather than probing only the new identity.
        if let Some(schema) = schema.filter(|schema| !primary_key_requires_typed_identity(schema)) {
            let primary_key_columns = primary_key_columns_for_schema(schema);
            if !primary_key_columns.is_empty() {
                let primary_key_fields = primary_key_columns
                    .iter()
                    .cloned()
                    .map(|column| IndexField::MetadataPath(vec![column]))
                    .collect::<Vec<_>>();
                if let Some(values) = self.prepare_dynamic_index_prefix(
                    selection,
                    table,
                    alias,
                    Some(schema),
                    &primary_key_fields,
                    &scope,
                )? {
                    if values.len() == primary_key_columns.len() {
                        let covered_terms = lookup_covered_terms(&values);
                        return Ok(Some(PreparedDynamicRecordLookup::PrimaryKeyExact {
                            schema: schema.clone(),
                            columns: primary_key_columns,
                            values,
                            covered_terms,
                            context,
                        }));
                    }
                }
            }
        }

        let primary_key_index_name = schema.map(TableSchema::primary_key_constraint_name);
        let mut best: Option<PreparedDynamicRecordLookup> = None;
        for index in sql_index_definitions_for_collection(self.db_ref(), table)
            .into_iter()
            .filter(|index| {
                index.kind == IndexKind::BTree
                    && primary_key_index_name
                        .as_ref()
                        .is_none_or(|name| !index.name.eq_ignore_ascii_case(name))
            })
        {
            let Some(prefix) = self.prepare_dynamic_index_prefix(
                selection,
                table,
                alias,
                schema,
                &index.fields,
                &scope,
            )?
            else {
                continue;
            };
            if prefix.is_empty() {
                continue;
            }
            if let Some(next_field) = index.fields.get(prefix.len()) {
                if self.dynamic_range_bound_expr_available(
                    selection, table, alias, schema, next_field, &scope,
                )? {
                    continue;
                }
            }
            let replace = best
                .as_ref()
                .is_none_or(|existing| prefix.len() > existing.prefix_len());
            if replace {
                let covered_terms = lookup_covered_terms(&prefix);
                best = Some(PreparedDynamicRecordLookup::IndexPrefix {
                    index,
                    prefix,
                    covered_terms,
                    context: context.clone(),
                });
            }
        }
        Ok(best)
    }

    pub(crate) fn prepare_dynamic_index_prefix(
        &self,
        selection: &Expr,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        fields: &[IndexField],
        scope: &BoundExprScope,
    ) -> Result<Option<Vec<PreparedDynamicBoundExpr>>> {
        let mut prefix = Vec::new();
        for field in fields {
            let Some(bound) =
                self.dynamic_equality_bound_expr(selection, table, alias, schema, field, scope)?
            else {
                break;
            };
            prefix.push(bound);
        }
        Ok((!prefix.is_empty()).then_some(prefix))
    }

    pub(crate) fn dynamic_equality_bound_expr(
        &self,
        selection: &Expr,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        field: &IndexField,
        scope: &BoundExprScope,
    ) -> Result<Option<PreparedDynamicBoundExpr>> {
        for (term_idx, term) in and_terms(selection).into_iter().enumerate() {
            let Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } = term
            else {
                continue;
            };
            if self.expr_matches_table_index_field(left, table, alias, schema, field)?
                && !self.expr_references_table(right, table, alias, schema)?
            {
                if let Some(bound) = scope.bind(right) {
                    return Ok(Some(PreparedDynamicBoundExpr {
                        expr: bound,
                        term_idx,
                    }));
                }
            }
            if self.expr_matches_table_index_field(right, table, alias, schema, field)?
                && !self.expr_references_table(left, table, alias, schema)?
            {
                if let Some(bound) = scope.bind(left) {
                    return Ok(Some(PreparedDynamicBoundExpr {
                        expr: bound,
                        term_idx,
                    }));
                }
            }
        }
        Ok(None)
    }

    pub(crate) fn dynamic_range_bound_expr_available(
        &self,
        selection: &Expr,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        field: &IndexField,
        scope: &BoundExprScope,
    ) -> Result<bool> {
        for term in and_terms(selection) {
            let Expr::BinaryOp { left, op, right } = term else {
                continue;
            };
            if !matches!(
                op,
                BinaryOperator::Gt
                    | BinaryOperator::GtEq
                    | BinaryOperator::Lt
                    | BinaryOperator::LtEq
            ) {
                continue;
            }
            if self.expr_matches_table_index_field(left, table, alias, schema, field)?
                && !self.expr_references_table(right, table, alias, schema)?
                && scope.bind(right).is_some()
            {
                return Ok(true);
            }
            if self.expr_matches_table_index_field(right, table, alias, schema, field)?
                && !self.expr_references_table(left, table, alias, schema)?
                && scope.bind(left).is_some()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(crate) fn prepared_dynamic_record_ids(
        &self,
        table: &str,
        lookup: &PreparedDynamicRecordLookup,
        row: &SlotRow,
    ) -> Result<Rc<[String]>> {
        match lookup {
            PreparedDynamicRecordLookup::PrimaryKeyExact { .. } => Ok(Rc::from(
                self.prepared_dynamic_primary_key_record_id(table, lookup, row)?
                    .into_iter()
                    .collect::<Vec<String>>(),
            )),
            PreparedDynamicRecordLookup::IndexPrefix {
                index,
                prefix,
                context,
                ..
            } => {
                let mut values = Vec::with_capacity(prefix.len());
                for expr in prefix {
                    let value = expr.expr.eval(&BoundExprFrame {
                        user_calls: &[],
                        db: self.db_ref(),
                        columns: BoundExprColumns::Values(row),
                        vars: &context.var_values,
                    })?;
                    if matches!(value, SqlValue::Null) {
                        return Ok(Rc::from(Vec::<String>::new()));
                    }
                    values.push(value);
                }
                let prefix = values
                    .into_iter()
                    .map(index_value_from_sql)
                    .collect::<Result<Vec<_>>>()?;
                if sql_trace_flags().plan {
                    eprintln!(
                        "bicdb_trace_plan prepared_index_lookup table={} index={} prefix={:?}",
                        table, index.name, prefix
                    );
                }
                let ids = self.lookup_index_cached(&index.name, prefix.as_slice())?;
                self.record_ids_with_pending_index_lookup_candidates(
                    table,
                    index,
                    prefix.as_slice(),
                    ids,
                )
            }
        }
    }

    pub(crate) fn prepared_dynamic_primary_key_record_id(
        &self,
        table: &str,
        lookup: &PreparedDynamicRecordLookup,
        row: &SlotRow,
    ) -> Result<Option<String>> {
        let PreparedDynamicRecordLookup::PrimaryKeyExact {
            schema,
            columns,
            values,
            context,
            ..
        } = lookup
        else {
            return Ok(None);
        };
        let mut sql_values = Vec::with_capacity(values.len());
        for expr in values {
            let value = expr.expr.eval(&BoundExprFrame {
                user_calls: &[],
                db: self.db_ref(),
                columns: BoundExprColumns::Values(row),
                vars: &context.var_values,
            })?;
            if matches!(value, SqlValue::Null) {
                return Ok(None);
            }
            sql_values.push(value);
        }
        let record_id = record_id_from_column_values(table, schema, columns, &sql_values)?;
        if sql_trace_flags().plan {
            eprintln!(
                "bicdb_trace_plan prepared_primary_key_lookup table={} matched_columns=all",
                table
            );
        }
        Ok(Some(record_id))
    }

    /// Bound-plan cache fast path for single-table full-primary-key point
    /// lookups (`SELECT cols FROM t WHERE pk = <expr> [AND ...]`). Returns
    /// `Some(result)` only when the cached/prepared lookup reproduces the fused
    /// `execute_row_query` path exactly; otherwise returns `None` so the caller
    /// falls through to the unchanged path. Only ever called when the
    /// `BICDB_PLAN_CACHE` gate is on.
    ///
    /// Scope (deliberately narrow — every excluded shape falls back to the
    /// fused oracle, never to a guessed result):
    /// * single table, no joins, no correlated outer row;
    /// * none of DISTINCT/TOP/INTO/LATERAL/PREWHERE/CLUSTER/DISTRIBUTE/SORT/
    ///   HAVING/WINDOW/QUALIFY/VALUE-TABLE/CONNECT BY, no GROUP BY, no aggregates;
    /// * a real user table with a schema (not a CTE/view/virtual/sequence);
    /// * the predicate is exactly the conjunction of equalities binding every
    ///   primary-key column (`PrimaryKeyExact` covering all AND-terms). Secondary
    ///   / composite *index* prefixes are intentionally excluded: their validity
    ///   would depend on the index catalog, which (unlike the table schema) does
    ///   not bump `SCHEMA_COLLECTION`'s generation, so they cannot be safely
    ///   invalidated here.
    pub(crate) fn try_cached_point_lookup_select(
        &self,
        from: &TableWithJoins,
        select: &Select,
        query: &Query,
    ) -> Result<Option<SqlResult>> {
        // Cheap AST shape gate. These rejections are not cached (the checks are
        // trivial) and simply fall through to the existing path.
        if !from.joins.is_empty()
            || self.outer_row.is_some()
            || select.distinct.is_some()
            || select.top.is_some()
            || select.into.is_some()
            || select.exclude.is_some()
            || select.select_modifiers.is_some()
            || !select.optimizer_hints.is_empty()
            || !matches!(select.flavor, SelectFlavor::Standard)
            || !select.lateral_views.is_empty()
            || select.prewhere.is_some()
            || !select.cluster_by.is_empty()
            || !select.distribute_by.is_empty()
            || !select.sort_by.is_empty()
            || select.having.is_some()
            || !select.named_window.is_empty()
            || select.qualify.is_some()
            || select.value_table_mode.is_some()
            || !select.connect_by.is_empty()
            || has_group_by(select)?
            || has_aggregates(&select.projection)
        {
            return Ok(None);
        }
        let Some(selection) = select.selection.as_ref() else {
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

        let db = self.db_ref();

        // Routine-IR-owned node: the plan is keyed by the node itself. The
        // IR fixes both the statement text and the variable name shape, and
        // the generation covers recompiles and DDL.
        if ir_plan_cache::enabled() && self.ir_owned_statement && self.ctes.is_empty() {
            if let Some(ir) = self.routine_ir {
                let generation = crate::eval::expr_type_scope()
                    .map(|(generation, _)| generation)
                    .unwrap_or_else(|| {
                        db.collection_generation(ROUTINE_COLLECTION)
                            ^ db.collection_generation(SCHEMA_COLLECTION).rotate_left(32)
                    });
                let key = (
                    db as *const BicDb as usize,
                    generation,
                    ir,
                    query as *const Query as usize,
                );
                if let Some(entry) = sql_plan_node_cache_get(key) {
                    return match entry.plan.as_ref() {
                        Some(template) => self
                            .execute_cached_point_lookup(template, select, query)
                            .map(Some),
                        None => Ok(None),
                    };
                }
                let entry =
                    self.build_point_lookup_entry(name, alias.as_ref(), selection, select, query)?;
                let result = match entry.plan.as_ref() {
                    Some(template) => {
                        Some(self.execute_cached_point_lookup(template, select, query)?)
                    }
                    None => None,
                };
                sql_plan_node_cache_set(key, entry);
                return Ok(result);
            }
        }
        if !plan_cache::enabled() {
            return Ok(None);
        }
        let sql = query.to_string();

        // Cache probe, validated against the current schema generation (handled in
        // `sql_plan_cache_get`) and the current routine-variable name shape.
        if let Some(entry) = sql_plan_cache_get(db, &sql) {
            if self.routine_var_names_match(&entry.var_names) {
                return match entry.plan.as_ref() {
                    Some(template) => self
                        .execute_cached_point_lookup(template, select, query)
                        .map(Some),
                    None => Ok(None),
                };
            }
        }

        // Miss (or stale var shape): plan once, classify, cache the verdict.
        let entry =
            self.build_point_lookup_entry(name, alias.as_ref(), selection, select, query)?;
        let result = match entry.plan.as_ref() {
            Some(template) => Some(self.execute_cached_point_lookup(template, select, query)?),
            None => None,
        };
        sql_plan_cache_set(db, &sql, entry);
        Ok(result)
    }

    /// Whether the current routine variables present the same ordered raw-name
    /// list a cached plan was built against (so its `VarId`s still bind to the
    /// same variables). Compared by raw name in `BTreeMap` order, which is exactly
    /// what determines the `VarId` assignment.
    pub(crate) fn routine_var_names_match(&self, names: &[String]) -> bool {
        self.routine_vars.len() == names.len()
            && self
                .routine_vars
                .keys()
                .zip(names)
                .all(|(key, name)| key == name)
    }

    /// The IR-node key for a routine-owned statement node (`None` when the
    /// statement is not IR-owned, a CTE is in scope, or no routine runs).
    pub(crate) fn ir_plan_node_key(&self, node: usize) -> Option<(usize, u64, usize, usize)> {
        if !(self.ir_owned_statement && self.ctes.is_empty()) {
            return None;
        }
        let ir = self.routine_ir?;
        let db = self.db_ref();
        let generation = crate::eval::expr_type_scope()
            .map(|(generation, _)| generation)
            .unwrap_or_else(|| {
                db.collection_generation(ROUTINE_COLLECTION)
                    ^ db.collection_generation(SCHEMA_COLLECTION).rotate_left(32)
            });
        Some((db as *const BicDb as usize, generation, ir, node))
    }

    /// The UPDATE analogue of `build_point_lookup_entry`: `Some` only when the
    /// WHERE is exactly a full primary-key equality the prepared lookup
    /// covers with no residual term, so the located row set is the fused
    /// path's and the per-row predicate re-check is redundant.
    pub(crate) fn build_update_point_template(
        &self,
        table: &str,
        alias: &str,
        schema: &TableSchema,
        selection: &Expr,
        outer_columns: &[String],
    ) -> Result<Option<UpdatePointTemplate>> {
        let prepared = self.prepare_dynamic_record_lookup(
            table,
            alias,
            Some(schema),
            selection,
            outer_columns,
        )?;
        let Some(PreparedDynamicRecordLookup::PrimaryKeyExact {
            columns,
            values,
            covered_terms,
            ..
        }) = prepared
        else {
            return Ok(None);
        };
        if covered_terms.len() != and_terms(selection).len() {
            return Ok(None);
        }
        Ok(Some(UpdatePointTemplate {
            pk_columns: columns,
            key_exprs: values.into_iter().map(|value| value.expr).collect(),
            outer_columns: outer_columns.to_vec(),
        }))
    }

    /// The record id an UPDATE point template addresses with this call's
    /// variable values; `None` when a key value is NULL (matches no row).
    pub(crate) fn update_point_record_id(
        &self,
        template: &UpdatePointTemplate,
        table: &str,
        schema: &TableSchema,
    ) -> Result<Option<String>> {
        let empty_row: SlotRow = Vec::new();
        self.update_point_record_id_for_row(template, table, schema, &empty_row)
    }

    /// `update_point_record_id` with an outer (FROM) row supplying the column
    /// references; the caller has checked the row's layout is the template's.
    pub(crate) fn update_point_record_id_for_row(
        &self,
        template: &UpdatePointTemplate,
        table: &str,
        schema: &TableSchema,
        outer_row: &SlotRow,
    ) -> Result<Option<String>> {
        let (_, context) = self.bound_row_context(&template.outer_columns);
        let mut key_values = Vec::with_capacity(template.key_exprs.len());
        for expr in &template.key_exprs {
            let value = expr.eval(&BoundExprFrame {
                user_calls: &[],
                db: self.db_ref(),
                columns: BoundExprColumns::Values(outer_row),
                vars: &context.var_values,
            })?;
            if matches!(value, SqlValue::Null) {
                return Ok(None);
            }
            key_values.push(value);
        }
        record_id_from_column_values(table, schema, &template.pk_columns, &key_values).map(Some)
    }

    /// Plan and classify a single-table point lookup for the bound-plan cache.
    /// Returns a `CachedPointLookup` whose `plan` is `Some` only for the exact
    /// full-primary-key shape the fused path reproduces, else `None`.
    pub(crate) fn build_point_lookup_entry(
        &self,
        name: &ObjectName,
        alias: Option<&TableAlias>,
        selection: &Expr,
        select: &Select,
        query: &Query,
    ) -> Result<CachedPointLookup> {
        let var_names: Rc<[String]> = self.routine_vars.keys().cloned().collect::<Vec<_>>().into();
        let not_cacheable = CachedPointLookup {
            var_names: var_names.clone(),
            plan: None,
        };

        let collection = relation_name(name)?;
        // Relation kinds the fused single-table path handles specially: defer to
        // it (a CTE may even shadow a real table of the same name).
        if self.cte(&collection).is_some()
            || is_virtual_table(&collection)
            || load_view(self.db_ref(), &collection)?.is_some()
            || load_sequence(self.db_ref(), &collection)?.is_some()
        {
            return Ok(not_cacheable);
        }
        // Mirror `execute_row_query`'s encrypted-predicate rejection so error
        // behavior is identical for the shapes we take over.
        self.reject_encrypted_predicates(&collection, Some(selection))?;

        let alias_name = alias
            .map(|alias| alias.name.value.clone())
            .unwrap_or_else(|| {
                collection
                    .rsplit('.')
                    .next()
                    .unwrap_or(&collection)
                    .to_string()
            });
        let table = resolve_session_relation_name(self.db_ref(), &collection)?;
        let Some(schema) = load_schema(self.db_ref(), &table)? else {
            return Ok(not_cacheable);
        };

        // Plan with no outer-row columns available: the predicate's value side may
        // reference only routine variables / literals (correlated/outer-row cases
        // were already excluded above). A non-bindable side yields `None` here and
        // falls back.
        let prepared =
            self.prepare_dynamic_record_lookup(&table, &alias_name, Some(&schema), selection, &[])?;
        let Some(PreparedDynamicRecordLookup::PrimaryKeyExact {
            columns,
            values,
            covered_terms,
            ..
        }) = prepared
        else {
            return Ok(not_cacheable);
        };
        // The primary-key equalities must cover the WHOLE predicate, so the single
        // fetched record is exactly the fused path's result set (no residual
        // predicate the prepared lookup would not apply). `covered_terms` are
        // distinct indices in `0..len`, so an equal count means full coverage.
        if covered_terms.len() != and_terms(selection).len() {
            return Ok(not_cacheable);
        }

        // Convert only the referenced fields (fixed per plan: the statement
        // node or text is the key); the column list comes from the same list
        // so slots and names align.
        let mut fields = FieldRef::wildcard(Some(&schema));
        let needed_columns = referenced_column_names(select, query);
        retain_referenced_fields(&mut fields, needed_columns.as_ref(), &table, &alias_name);
        let output_columns = row_output_columns_from_fields(&table, &alias_name, &fields);
        let template = PointLookupTemplate {
            table,
            alias: alias_name,
            pk_columns: columns,
            key_exprs: values.into_iter().map(|value| value.expr).collect(),
            schema,
            fields,
            output_columns,
        };
        Ok(CachedPointLookup {
            var_names,
            plan: Some(Rc::new(template)),
        })
    }

    /// Execute a cached point-lookup plan with this call's variable values,
    /// reproducing the fused `execute_row_query` tail (fetch -> materialize ->
    /// filter -> order -> limit -> project) exactly.
    /// Statement-level eligibility for the cell-row read path on `table`:
    /// enabled, no RLS filter for `action`, and no collection policy while a
    /// security context is active (policies filter and project parsed records).
    pub(crate) fn cell_rows_eligible(
        &self,
        table: &str,
        schema: &TableSchema,
        action: PolicyAction,
    ) -> Result<bool> {
        if !cell_rows_enabled() {
            return Ok(false);
        }
        if self.security_context.is_some() && self.db_ref().collection_has_policy(table)? {
            return Ok(false);
        }
        Ok(matches!(
            prepare_rls_with_schema(
                self.db_ref(),
                table,
                action,
                Some(schema),
                &self.session_gucs,
                self.security_context.as_ref(),
                false,
            )?,
            PreparedRls::Allow
        ))
    }

    /// Visible rows by primary key without parsing their JSON (pending writes
    /// overlaid), transactional or not.
    pub(crate) fn visible_rows_for_pks(
        &self,
        table: &str,
        pks: &[String],
    ) -> Result<Vec<Option<VisibleRow>>> {
        match self.tx {
            Some(tx) => tx.get_stored_by_pks(table, pks).map_err(SqlError::from),
            None => pks
                .iter()
                .map(|pk| self.db_ref().get_stored(table, pk).map_err(SqlError::from))
                .collect(),
        }
    }

    /// Slot rows for visible rows: from borrowed cells when every field can be
    /// read that way, through the `Record` form for that row otherwise. Also
    /// returns the ids of the present rows, in order (for system columns).
    pub(crate) fn slot_rows_from_visible(
        &self,
        table: &str,
        alias: &str,
        fields: &[FieldRef],
        visible: &[Option<VisibleRow>],
        collect_ids: bool,
    ) -> Result<(Vec<SlotRow>, Vec<String>)> {
        let mut rows = Vec::with_capacity(visible.len());
        let mut ids = Vec::with_capacity(if collect_ids { visible.len() } else { 0 });
        let mut cells: bicdb_core::CellRow<'_> = Vec::with_capacity(32);
        let mut plan: Option<CellPlan> = None;
        for (idx, row) in visible.iter().enumerate() {
            if idx % 1024 == 0 {
                self.check_cancellation()?;
            }
            let Some(row) = row else {
                continue;
            };
            if collect_ids {
                ids.push(row.id().to_string());
            }
            #[cfg(test)]
            SQL_SLOT_ROW_FIELDS.with(|count| *count.borrow_mut() += fields.len());
            let slot = match row {
                VisibleRow::Stored(stored) => {
                    let fast = if stored.cells_into(&mut cells) {
                        slot_row_from_cells(table, alias, fields, stored, &cells, &mut plan)
                    } else {
                        None
                    };
                    match fast {
                        Some(slot) => {
                            #[cfg(test)]
                            SQL_CELL_ROWS_FAST.with(|calls| *calls.borrow_mut() += 1);
                            slot
                        }
                        None => {
                            #[cfg(test)]
                            SQL_CELL_ROWS_FALLBACK.with(|calls| *calls.borrow_mut() += 1);
                            let record = stored.to_record().map_err(SqlError::from)?;
                            slot_row_from_record_fields(table, alias, fields, &record)?
                        }
                    }
                }
                VisibleRow::Pending(record) => {
                    #[cfg(test)]
                    SQL_CELL_ROWS_FALLBACK.with(|calls| *calls.borrow_mut() += 1);
                    slot_row_from_record_fields(table, alias, fields, record)?
                }
            };
            rows.push(slot);
        }
        Ok((rows, ids))
    }

    /// The index-located row set of `table` for `selection` through the cell
    /// path; `None` when the statement does not qualify or needs a full scan.
    fn cell_row_set_for_selection(
        &self,
        table: &str,
        alias: &str,
        schema: &TableSchema,
        selection: Option<&Expr>,
        needed_columns: Option<&ReferencedColumns>,
    ) -> Result<Option<RowSet>> {
        if !self.cell_rows_eligible(table, schema, PolicyAction::Select)? {
            return Ok(None);
        }
        let has_pending = self.tx.is_some_and(|tx| tx.has_pending_writes(table));
        let visible: Vec<Option<VisibleRow>> = if has_pending {
            // Pending writes: the id resolver merges this transaction's own
            // candidates; the pk fetch overlays them per key.
            let Some(ids) =
                self.indexed_record_ids_for_table_selection(table, alias, Some(schema), selection)?
            else {
                return Ok(None);
            };
            sql_profile_index_lookup();
            self.visible_rows_for_pks(table, &ids)?
        } else {
            let Some(locators) = self.indexed_record_locators_for_table_selection(
                table,
                alias,
                Some(schema),
                selection,
            )?
            else {
                return Ok(None);
            };
            sql_profile_index_lookup();
            match locators {
                RecordLocators::Pks(pks) => self.visible_rows_for_pks(table, &pks)?,
                RecordLocators::Rowids(rowids) => {
                    let stored = match self.tx {
                        Some(tx) => tx.get_stored_by_rowids(table, &rowids),
                        None => self.db_ref().get_stored_by_rowids(table, &rowids),
                    }
                    .map_err(SqlError::from)?;
                    stored
                        .into_iter()
                        .map(|stored| stored.map(VisibleRow::Stored))
                        .collect()
                }
            }
        };
        let mut fields = FieldRef::wildcard(Some(schema));
        retain_referenced_fields(&mut fields, needed_columns, table, alias);
        let mut columns = row_output_columns_from_fields(table, alias, &fields);
        columns.extend(postgres_system_output_columns(table, alias));
        let (mut rows, ids) = self.slot_rows_from_visible(table, alias, &fields, &visible, true)?;
        let system_metadata = self.postgres_system_metadata_batch(table, &ids)?;
        for (row, metadata) in rows.iter_mut().zip(system_metadata) {
            row.extend(self.postgres_system_values(table, alias, Some(schema), metadata));
        }
        sql_profile_sql_rows_materialized(&rows);
        Ok(Some(RowSet { rows, columns }))
    }

    /// Routine-owned two-relation SELECT through the per-IR-node point-join
    /// template: `None` when the shape or the tables decline it (the generic
    /// join path runs). `from` is the normalized single `TableWithJoins`.
    pub(crate) fn try_cached_point_join_select(
        &self,
        from: &TableWithJoins,
        select: &Select,
        query: &Query,
    ) -> Result<Option<SqlResult>> {
        if from.joins.len() != 1
            || self.outer_row.is_some()
            || select.distinct.is_some()
            || select.top.is_some()
            || select.into.is_some()
            || select.exclude.is_some()
            || select.select_modifiers.is_some()
            || !select.optimizer_hints.is_empty()
            || !matches!(select.flavor, SelectFlavor::Standard)
            || !select.lateral_views.is_empty()
            || select.prewhere.is_some()
            || !select.cluster_by.is_empty()
            || !select.distribute_by.is_empty()
            || !select.sort_by.is_empty()
            || select.having.is_some()
            || !select.named_window.is_empty()
            || select.qualify.is_some()
            || select.value_table_mode.is_some()
            || !select.connect_by.is_empty()
            || has_group_by(select)?
            || has_aggregates(&select.projection)
        {
            return Ok(None);
        }
        let Some(key) = self.ir_plan_node_key(query as *const Query as usize) else {
            return Ok(None);
        };
        let template = match sql_join_plan_node_cache_get(key) {
            Some(entry) => entry,
            None => {
                let built = self
                    .build_point_join_template(from, select, query)?
                    .map(Rc::new);
                sql_join_plan_node_cache_set(key, built.clone());
                built
            }
        };
        match template {
            Some(template) => self
                .execute_cached_point_join(&template, select, query)
                .map(Some),
            None => Ok(None),
        }
    }

    /// The rows of one point-lookup template for this call: key expressions
    /// evaluated over `outer_row` (the left row of a point join, empty for a
    /// single-table lookup) and the routine variables, then the visible row
    /// as a slot row over the template's fields. Zero or one row.
    pub(crate) fn point_lookup_rows(
        &self,
        template: &PointLookupTemplate,
        outer_row: &SlotRow,
        var_values: &[SqlValue],
    ) -> Result<Vec<SlotRow>> {
        let mut key_values = Vec::with_capacity(template.key_exprs.len());
        let mut any_null = false;
        for expr in &template.key_exprs {
            let value = expr.eval(&BoundExprFrame {
                user_calls: &[],
                db: self.db_ref(),
                columns: BoundExprColumns::Values(outer_row),
                vars: var_values,
            })?;
            if matches!(value, SqlValue::Null) {
                // A NULL key matches no row (the fused PK path returns 0 rows too).
                any_null = true;
                break;
            }
            key_values.push(value);
        }
        let ids: Vec<String> = if any_null {
            Vec::new()
        } else {
            vec![record_id_from_column_values(
                &template.table,
                &template.schema,
                &template.pk_columns,
                &key_values,
            )?]
        };
        // Mirror the fused `indexed_records_for_table_selection` profiling signal
        // so introspection/tests observe an index lookup, not a full scan.
        sql_profile_index_lookup();
        let fields = &template.fields;
        if self.cell_rows_eligible(&template.table, &template.schema, PolicyAction::Select)? {
            // Cell path: the visible row without its JSON parsed, cells
            // borrowed from the text, slot row built directly.
            let visible = self.visible_rows_for_pks(&template.table, &ids)?;
            let (rows, _) = self.slot_rows_from_visible(
                &template.table,
                &template.alias,
                fields,
                &visible,
                false,
            )?;
            sql_profile_sql_rows_materialized(&rows);
            Ok(rows)
        } else {
            let records =
                self.records_for_ids_with_schema(&template.table, &ids, Some(&template.schema))?;
            sql_profile_records_materialized(&records);
            let mut rows = Vec::with_capacity(records.len());
            for (idx, record) in records.iter().enumerate() {
                if idx % 1024 == 0 {
                    self.check_cancellation()?;
                }
                rows.push(slot_row_from_record_fields(
                    &template.table,
                    &template.alias,
                    fields,
                    record,
                )?);
            }
            Ok(rows)
        }
    }

    /// One relation of a point join as a lookup template, or `None` when the
    /// relation is not a plain table or the terms do not pin its whole
    /// primary key (given `available_columns`, the left row's layout).
    fn point_join_relation_template(
        &self,
        relation: &TableFactor,
        combined: &Expr,
        available_columns: &[String],
        select: &Select,
        query: &Query,
    ) -> Result<Option<(PointLookupTemplate, BTreeSet<usize>)>> {
        let TableFactor::Table {
            name,
            alias,
            args: None,
            ..
        } = relation
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
        self.reject_encrypted_predicates(&collection, Some(combined))?;
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
        let table = resolve_session_relation_name(self.db_ref(), &collection)?;
        let Some(schema) = load_schema(self.db_ref(), &table)? else {
            return Ok(None);
        };
        let prepared = self.prepare_dynamic_record_lookup(
            &table,
            &alias_name,
            Some(&schema),
            combined,
            available_columns,
        )?;
        let Some(PreparedDynamicRecordLookup::PrimaryKeyExact {
            columns,
            values,
            covered_terms,
            ..
        }) = prepared
        else {
            return Ok(None);
        };
        let mut fields = FieldRef::wildcard(Some(&schema));
        let needed_columns = referenced_column_names(select, query);
        retain_referenced_fields(&mut fields, needed_columns.as_ref(), &table, &alias_name);
        let output_columns = row_output_columns_from_fields(&table, &alias_name, &fields);
        Ok(Some((
            PointLookupTemplate {
                table,
                alias: alias_name,
                pk_columns: columns,
                key_exprs: values.into_iter().map(|value| value.expr).collect(),
                schema,
                fields,
                output_columns,
            },
            covered_terms,
        )))
    }

    /// Plan a two-relation inner join (comma or JOIN ... ON) as two point
    /// lookups: `Some` only when, in one of the two orders, the first
    /// relation's primary key is pinned by the routine variables and the
    /// second's by the variables and the first row, and the two lookups
    /// together consume every WHERE/ON term (no residual predicate).
    pub(crate) fn build_point_join_template(
        &self,
        from: &TableWithJoins,
        select: &Select,
        query: &Query,
    ) -> Result<Option<PointJoinTemplate>> {
        let [join] = from.joins.as_slice() else {
            return Ok(None);
        };
        let on_terms = match &join.join_operator {
            JoinOperator::Join(constraint)
            | JoinOperator::Inner(constraint)
            | JoinOperator::CrossJoin(constraint) => match constraint {
                JoinConstraint::None => None,
                JoinConstraint::On(expr) => Some(expr),
                JoinConstraint::Using(_) | JoinConstraint::Natural => return Ok(None),
            },
            _ => return Ok(None),
        };
        let combined = match (select.selection.as_ref(), on_terms) {
            (Some(selection), Some(on)) => and_expr(selection.clone(), on.clone()),
            (Some(selection), None) => selection.clone(),
            (None, Some(on)) => on.clone(),
            (None, None) => return Ok(None),
        };
        let term_count = and_terms(&combined).len();
        let relations = [&from.relation, &join.relation];
        for (first, second) in [(0usize, 1usize), (1, 0)] {
            let Some((left, left_terms)) =
                self.point_join_relation_template(relations[first], &combined, &[], select, query)?
            else {
                continue;
            };
            let Some((right, right_terms)) = self.point_join_relation_template(
                relations[second],
                &combined,
                &left.output_columns,
                select,
                query,
            )?
            else {
                continue;
            };
            if left.alias == right.alias {
                return Ok(None);
            }
            let covered = left_terms.union(&right_terms).count();
            if covered != term_count {
                continue;
            }
            let output_columns =
                merge_row_set_columns(left.output_columns.clone(), right.output_columns.clone());
            return Ok(Some(PointJoinTemplate {
                left,
                right,
                output_columns,
            }));
        }
        Ok(None)
    }

    /// Execute a point-join template: the left lookup from the variables,
    /// the right lookup from the variables and the left row, one merged row
    /// (or none), then the fused tail (order -> limit -> project -> type).
    /// Every WHERE/ON term is a key equality one of the lookups applied, so
    /// there is no predicate left to re-check on the merged row.
    pub(crate) fn execute_cached_point_join(
        &self,
        template: &PointJoinTemplate,
        select: &Select,
        query: &Query,
    ) -> Result<SqlResult> {
        self.check_cancellation()?;
        let (_, context) = self.bound_row_context(&[]);
        let empty_row: SlotRow = Vec::new();
        let left_rows = self.point_lookup_rows(&template.left, &empty_row, &context.var_values)?;
        let mut rows = Vec::with_capacity(1);
        if let Some(left_row) = left_rows.first() {
            let right_rows =
                self.point_lookup_rows(&template.right, left_row, &context.var_values)?;
            if let Some(right_row) = right_rows.first() {
                rows.push(merge_slot_rows(
                    left_row,
                    &template.left.output_columns,
                    right_row,
                    &template.right.output_columns,
                ));
            }
        }
        sql_profile_join_rows(&rows, rows.len());
        self.check_cancellation()?;
        let resolved_order_by =
            resolve_order_by_projection_aliases(query.order_by.as_ref(), &select.projection);
        self.apply_row_order_by(
            &mut rows,
            resolved_order_by.as_ref(),
            &template.output_columns,
            &[],
            order_by_keep_bound(query)?,
        )?;
        self.check_cancellation()?;
        apply_row_limit(&mut rows, query)?;
        let result =
            self.project_row_select(&select.projection, &rows, &template.output_columns)?;
        self.typed_row_result(result, select, None)
    }

    pub(crate) fn execute_cached_point_lookup(
        &self,
        template: &PointLookupTemplate,
        select: &Select,
        query: &Query,
    ) -> Result<SqlResult> {
        self.check_cancellation()?;
        // Fresh routine-variable values for this call; the plan is value-
        // independent (variables are referenced by `VarId`).
        let (_, context) = self.bound_row_context(&[]);
        let empty_row: SlotRow = Vec::new();
        let mut rows = self.point_lookup_rows(template, &empty_row, &context.var_values)?;
        if let Some(selection) = &select.selection {
            rows = self.filter_bound_row_predicate(rows, &template.output_columns, selection)?;
        }
        self.check_cancellation()?;
        let resolved_order_by =
            resolve_order_by_projection_aliases(query.order_by.as_ref(), &select.projection);
        self.apply_row_order_by(
            &mut rows,
            resolved_order_by.as_ref(),
            &template.output_columns,
            &[],
            order_by_keep_bound(query)?,
        )?;
        self.check_cancellation()?;
        apply_row_limit(&mut rows, query)?;
        let result =
            self.project_row_select(&select.projection, &rows, &template.output_columns)?;
        // The fused path types its result (column types + metadata, integer
        // and money checks) before returning; routine FOR loops read the
        // types (regclass rendering) and the wire layer the metadata.
        self.typed_row_result(result, select, Some(&template.schema))
    }
}

type StatementNodeKey = (usize, u64, usize, usize);

const STATEMENT_NODE_MEMO_MAX: usize = 4096;

thread_local! {
    static REFERENCED_COLUMNS_NODES: RefCell<FxHashMap<StatementNodeKey, Option<Rc<ReferencedColumns>>>> =
        RefCell::new(FxHashMap::default());
    static NULL_EQUALITY_NODES: RefCell<FxHashMap<StatementNodeKey, Vec<(String, String, Rc<Vec<(usize, bool)>>)>>> =
        RefCell::new(FxHashMap::default());
}

/// Per pk column: the equality term that binds it, or `None`.
pub(crate) type PrimaryKeyTermPlan = Rc<Vec<Option<(usize, bool)>>>;

thread_local! {
    static PRIMARY_KEY_TERM_PLANS: RefCell<FxHashMap<StatementNodeKey, Vec<(String, String, PrimaryKeyTermPlan)>>> =
        RefCell::new(FxHashMap::default());
}

pub(crate) fn primary_key_term_plan_get(
    key: StatementNodeKey,
    table: &str,
    alias: &str,
) -> Option<PrimaryKeyTermPlan> {
    PRIMARY_KEY_TERM_PLANS.with(|cache| {
        cache.borrow().get(&key).and_then(|entries| {
            entries
                .iter()
                .find(|(t, a, _)| t == table && a == alias)
                .map(|(_, _, plan)| Rc::clone(plan))
        })
    })
}

pub(crate) fn primary_key_term_plan_set(
    key: StatementNodeKey,
    table: &str,
    alias: &str,
    plan: PrimaryKeyTermPlan,
) {
    PRIMARY_KEY_TERM_PLANS.with(|cache| {
        let mut cache = cache.borrow_mut();
        if cache.len() >= STATEMENT_NODE_MEMO_MAX {
            cache.clear();
        }
        cache
            .entry(key)
            .or_default()
            .push((table.to_string(), alias.to_string(), plan));
    });
}

fn null_equality_candidates_get(
    key: StatementNodeKey,
    table: &str,
    alias: &str,
) -> Option<Rc<Vec<(usize, bool)>>> {
    NULL_EQUALITY_NODES.with(|cache| {
        cache.borrow().get(&key).and_then(|entries| {
            entries
                .iter()
                .find(|(t, a, _)| t == table && a == alias)
                .map(|(_, _, candidates)| Rc::clone(candidates))
        })
    })
}

fn null_equality_candidates_set(
    key: StatementNodeKey,
    table: &str,
    alias: &str,
    candidates: Rc<Vec<(usize, bool)>>,
) {
    NULL_EQUALITY_NODES.with(|cache| {
        let mut cache = cache.borrow_mut();
        if cache.len() >= STATEMENT_NODE_MEMO_MAX {
            cache.clear();
        }
        cache
            .entry(key)
            .or_default()
            .push((table.to_string(), alias.to_string(), candidates));
    });
}

/// The `index`-th term of `expr`'s top-level AND chain, in
/// [`and_terms`] order, without materializing the list.
pub(crate) fn nth_and_term(expr: &Expr, index: usize) -> Option<&Expr> {
    fn walk<'a>(expr: &'a Expr, remaining: &mut usize) -> Option<&'a Expr> {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => walk(left, remaining).or_else(|| walk(right, remaining)),
            Expr::Nested(inner) => walk(inner, remaining),
            term => {
                if *remaining == 0 {
                    Some(term)
                } else {
                    *remaining -= 1;
                    None
                }
            }
        }
    }
    let mut remaining = index;
    walk(expr, &mut remaining)
}

#[cfg(test)]
mod statement_scan_memo_tests {
    use super::*;

    #[test]
    fn nth_and_term_walks_exactly_like_and_terms() {
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        for sql in [
            "a = 1",
            "a = 1 AND b = $1",
            "(a = 1 AND (b = 2 OR c = 3)) AND (d = NULL) AND NOT e",
            "w_id = p_w_id AND d_id = p_d_id AND c_id = p_c_id",
            "((a = 1))",
        ] {
            let expr = sqlparser::parser::Parser::new(&dialect)
                .try_with_sql(sql)
                .unwrap()
                .parse_expr()
                .unwrap();
            let terms = and_terms(&expr);
            for (index, term) in terms.iter().enumerate() {
                assert!(
                    std::ptr::eq(nth_and_term(&expr, index).unwrap(), *term),
                    "{sql}: term {index}"
                );
            }
            assert!(
                nth_and_term(&expr, terms.len()).is_none(),
                "{sql}: past the end"
            );
        }
    }
}
