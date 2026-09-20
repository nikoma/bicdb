//! Split out of the parent module to keep files digestible; behavior
//! unchanged — a separate `impl` block on the same type.
use super::*;

#[cfg(test)]
mod order_row_ownership_tests {
    use super::*;

    fn order(sql: &str) -> OrderBy {
        let Statement::Query(query) = crate::parse_statements(sql).unwrap().remove(0) else {
            panic!("expected query");
        };
        query.order_by.unwrap()
    }

    fn fixture() -> Vec<SlotRow> {
        [2, 1, 2, 0, 1, 3]
            .into_iter()
            .enumerate()
            .map(|(index, key)| {
                vec![
                    SqlValue::Int(key),
                    SqlValue::String(format!("payload-{index}-").repeat(64)),
                ]
            })
            .collect()
    }

    #[test]
    fn indexed_join_borrows_existing_slot_row_payloads() {
        let rows = fixture();
        let borrowed = RightRowSource::Row(&rows[0])
            .slot_row("t", "t", &[])
            .unwrap();
        assert!(matches!(borrowed, std::borrow::Cow::Borrowed(_)));
        assert_eq!(borrowed.as_ptr(), rows[0].as_ptr());
        assert_eq!(borrowed.as_ref(), &rows[0]);
    }

    #[test]
    fn order_by_moves_payloads_and_preserves_stable_topk_ties() {
        let dir = tempfile::tempdir().unwrap();
        let db = BicDb::open(dir.path()).unwrap();
        let engine = SqlEngine::new(&db);
        let order = order("SELECT key, payload FROM t ORDER BY key");
        let columns = vec!["key".to_owned(), "payload".to_owned()];
        for keep in [None, Some(0), Some(1), Some(3), Some(5), Some(6), Some(9)] {
            let mut rows = fixture();
            let pointers = rows
                .iter()
                .map(|row| match &row[1] {
                    SqlValue::String(text) => text.as_ptr(),
                    _ => unreachable!(),
                })
                .collect::<Vec<_>>();
            engine
                .apply_row_order_by(
                    &mut rows,
                    Some(&order),
                    &columns,
                    &[Some("int8".to_owned())],
                    keep,
                )
                .unwrap();
            let expected = [3, 1, 4, 0, 2, 5];
            assert_eq!(rows.len(), keep.unwrap_or(6).min(6));
            for (row, index) in rows.iter().zip(expected) {
                let SqlValue::String(text) = &row[1] else {
                    unreachable!()
                };
                assert_eq!(text, &format!("payload-{index}-").repeat(64));
                assert_eq!(
                    text.as_ptr(),
                    pointers[index],
                    "ORDER BY must move, not clone, row payloads"
                );
            }
        }
    }

    #[test]
    fn order_key_error_leaves_original_rows_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let db = BicDb::open(dir.path()).unwrap();
        let engine = SqlEngine::new(&db);
        let order = order("SELECT key, payload FROM t ORDER BY 1 / key");
        let columns = vec!["key".to_owned(), "payload".to_owned()];
        let mut rows = fixture();
        let original = rows.clone();
        let allocation = rows.as_ptr();
        assert!(engine
            .apply_row_order_by(
                &mut rows,
                Some(&order),
                &columns,
                &[Some("int8".to_owned())],
                Some(3),
            )
            .is_err());
        assert_eq!(rows, original);
        assert_eq!(rows.as_ptr(), allocation);
    }
}
#[allow(unused_imports)]
use crate::*;
use rustc_hash::FxHashSet;

/// One right-side candidate of a row join: a slot row already built from cells,
/// or a parsed record when the join constraint must be evaluated against it.
enum RightRowSource<'a> {
    Row(&'a SlotRow),
    Record(&'a Arc<Record>),
}

impl<'a> RightRowSource<'a> {
    fn slot_row(
        self,
        table: &str,
        alias: &str,
        fields: &[FieldRef],
    ) -> Result<std::borrow::Cow<'a, SlotRow>> {
        match self {
            Self::Row(row) => Ok(std::borrow::Cow::Borrowed(row)),
            Self::Record(record) => slot_row_from_record_fields(table, alias, fields, record)
                .map(std::borrow::Cow::Owned),
        }
    }
}

impl<'db> SqlEngine<'db> {
    /// Executor-level spatial join: when the ON condition is exactly
    /// `ST_Intersects(a.geom, b.geom)` or `ST_DWithin(a.geom, b.geom, m)`
    /// over columns of the two sides, build an R-tree over the right rows
    /// once and probe it per left row — O((N+M) log M) with exact
    /// verification, instead of N x M pairwise predicate evaluations.
    pub(crate) fn apply_spatial_tree_join(
        &self,
        left: &RowSet,
        right: &RowSet,
        kind: RowJoinKind,
        constraint: &JoinConstraint,
    ) -> Result<Option<RowSet>> {
        use rstar::{primitives::GeomWithData, RTree, AABB};
        if !matches!(kind, RowJoinKind::Inner | RowJoinKind::Left) {
            return Ok(None);
        }
        let JoinConstraint::On(selection) = constraint else {
            return Ok(None);
        };
        let terms = and_terms(selection);
        let [term] = terms.as_slice() else {
            return Ok(None);
        };
        let Expr::Function(function) = term else {
            return Ok(None);
        };
        let name = object_name(&function.name)?.to_ascii_lowercase();
        let name = name
            .strip_prefix("public.")
            .or_else(|| name.strip_prefix("pg_catalog."))
            .unwrap_or(&name);
        let args = function_args(function);
        let (first, second, op) = match name {
            "st_intersects" if args.len() == 2 => (&args[0], &args[1], SpatialJoinOp::Intersects),
            "st_dwithin" if args.len() == 3 => {
                let meters = crate::records::spatial_number(
                    "ST_DWithin",
                    &eval_constant_expr(&args[2])?,
                    "meters",
                )?;
                if meters < 0.0 {
                    return Ok(None);
                }
                (&args[0], &args[1], SpatialJoinOp::DWithin(meters))
            }
            _ => return Ok(None),
        };
        // Resolve which argument binds to which side (either order).
        let binding = match (
            row_set_column_index(&left.columns, first),
            row_set_column_index(&right.columns, second),
            row_set_column_index(&left.columns, second),
            row_set_column_index(&right.columns, first),
        ) {
            (Some(left_idx), Some(right_idx), _, _) => Some((left_idx, right_idx)),
            (_, _, Some(left_idx), Some(right_idx)) => Some((left_idx, right_idx)),
            _ => None,
        };
        let Some((left_idx, right_idx)) = binding else {
            return Ok(None);
        };

        // Build side: parse every right geometry once and index its bbox.
        let mut right_geometries: Vec<Option<Geometry>> = Vec::with_capacity(right.rows.len());
        let mut entries = Vec::new();
        for (row_index, row) in right.rows.iter().enumerate() {
            let geometry = slot_geometry(row.get(right_idx));
            if let Some(geometry) = &geometry {
                if let Some((min, max)) = geometry.coordinate_bounds() {
                    entries.push(GeomWithData::new(
                        rstar::primitives::Rectangle::from_corners(min, max),
                        row_index,
                    ));
                }
            }
            right_geometries.push(geometry);
        }
        let tree: RTree<GeomWithData<rstar::primitives::Rectangle<[f64; 2]>, usize>> =
            RTree::bulk_load(entries);

        let null_pad = vec![SqlValue::Null; right.columns.len()];
        let mut rows: Vec<SlotRow> = Vec::new();
        for left_row in &left.rows {
            self.check_cancellation()?;
            let mut matched = false;
            if let Some(left_geometry) = slot_geometry(left_row.get(left_idx)) {
                if let Some((mut min, mut max)) = left_geometry.coordinate_bounds() {
                    if let SpatialJoinOp::DWithin(meters) = op {
                        let mid_lat = ((min[1] + max[1]) / 2.0).to_radians();
                        let dlat = meters / 111_320.0;
                        let dlon = meters / (111_320.0 * mid_lat.cos().abs().max(0.01));
                        min = [min[0] - dlon, min[1] - dlat];
                        max = [max[0] + dlon, max[1] + dlat];
                    }
                    let envelope = AABB::from_corners(min, max);
                    let mut candidates: Vec<usize> = tree
                        .locate_in_envelope_intersecting(&envelope)
                        .map(|entry| entry.data)
                        .collect();
                    candidates.sort_unstable();
                    for row_index in candidates {
                        let Some(right_geometry) = &right_geometries[row_index] else {
                            continue;
                        };
                        let hit = match op {
                            SpatialJoinOp::Intersects => {
                                crate::records::spatial_intersects(&left_geometry, right_geometry)
                            }
                            SpatialJoinOp::DWithin(meters) => {
                                crate::records::spatial_distance_meters(
                                    &left_geometry,
                                    right_geometry,
                                )? <= meters
                            }
                        };
                        if hit {
                            matched = true;
                            let mut row = left_row.clone();
                            row.extend(right.rows[row_index].iter().cloned());
                            rows.push(row);
                        }
                    }
                }
            }
            if !matched && matches!(kind, RowJoinKind::Left) {
                let mut row = left_row.clone();
                row.extend(null_pad.iter().cloned());
                rows.push(row);
            }
        }
        let columns = merge_row_set_columns(left.columns.clone(), right.columns.clone());
        Ok(Some(RowSet { rows, columns }))
    }

    pub(crate) fn apply_indexed_right_table_join(
        &self,
        left: &mut RowSet,
        right_relation: &TableFactor,
        kind: RowJoinKind,
        constraint: &JoinConstraint,
        needed_columns: Option<&ReferencedColumns>,
    ) -> Result<Option<RowSet>> {
        let JoinConstraint::On(selection) = constraint else {
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
        // Authorize the right table exactly as the non-indexed join path does
        // (`row_set_from_table_factor_with_selection`). Without this, an indexed
        // equi-join such as `generate_series(1,2) g JOIN secrets s ON s.id = g`
        // read `secrets` through the index-lookup fast path with no privilege
        // check — a cross-tenant read of any table by any authenticated role.
        self.require_relation_privilege(&table, "SELECT")?;
        let schema = load_schema(self.db_ref(), &table)?;
        // Column list and rows come from the same (possibly pruned) field
        // list: `row_output_columns` is exactly this over the wildcard.
        let mut right_fields = FieldRef::wildcard(schema.as_ref());
        retain_referenced_fields(&mut right_fields, needed_columns, &table, &alias_name);
        let right_columns = row_output_columns_from_fields(&table, &alias_name, &right_fields);
        let can_eval_constraint_with_outer_row =
            join_constraint_can_eval_with_outer_row(constraint, &left.columns, &right_columns);
        let prepared_lookup = if can_eval_constraint_with_outer_row {
            self.prepare_dynamic_record_lookup(
                &table,
                &alias_name,
                schema.as_ref(),
                selection,
                &left.columns,
            )?
        } else {
            None
        };
        let prepared_residual_constraint = prepared_lookup
            .as_ref()
            .map(|lookup| residual_join_constraint_after_lookup(selection, lookup));
        if let Some(lookup @ PreparedDynamicRecordLookup::PrimaryKeyExact { .. }) =
            prepared_lookup.as_ref()
        {
            let prepared_join = PreparedPrimaryKeyRightJoin {
                table: &table,
                alias_name: &alias_name,
                schema: schema.as_ref(),
                right_columns,
                right_fields,
                kind,
                lookup,
                residual_constraint: prepared_residual_constraint
                    .as_ref()
                    .unwrap_or(&JoinConstraint::None),
            };
            return self
                .apply_prepared_primary_key_right_table_join(left, prepared_join)
                .map(Some);
        }
        let null_right = null_slot_row_for_columns(&right_columns);
        let mut rows = Vec::new();
        let mut candidate_pairs = 0usize;
        let mut row_engine = self
            .inherit_transaction(SqlEngine::with_ctes_and_context(
                self.db_ref(),
                self.settings,
                self.ctes.clone(),
                self.security_context.clone(),
                self.session_gucs.clone(),
            ))
            .with_shared_routine_vars(self.routine_vars.clone())
            .with_cancellation(self.cancellation.clone());
        // The constraint (or, after a prepared lookup, its residual) is bound
        // once per join over the merged layout; `join_constraint_matches`
        // re-merged the column lists, rebuilt the scope and re-bound the
        // expression for every candidate pair — STOCK_LEVEL's order_line
        // range join paid that ~200 times per call. The outer row is not set
        // yet here, so identifiers bind to the merged columns exactly as the
        // per-pair path binds them; an expression the binder declines keeps
        // the per-pair path.
        let bind_once = |expr: &Expr| {
            let merged_columns = merge_row_set_columns(left.columns.clone(), right_columns.clone());
            let (scope, context) = row_engine.bound_row_context(&merged_columns);
            let bound = scope.bind(expr);
            #[cfg(test)]
            if bound.is_some() {
                SQL_JOIN_CONSTRAINT_BOUND_ONCE.with(|calls| *calls.borrow_mut() += 1);
            }
            bound.map(|bound| {
                (
                    bound,
                    context,
                    merged_slot_sources(&left.columns, &right_columns),
                )
            })
        };
        let bound_constraint = match constraint {
            JoinConstraint::On(expr) => bind_once(expr),
            _ => None,
        };
        let bound_residual = match prepared_residual_constraint.as_ref() {
            Some(JoinConstraint::On(expr)) => bind_once(expr),
            _ => None,
        };
        let eval_bound = |bound: &BoundExpr,
                          context: &BoundRowContext,
                          sources: &[JoinedSlotSource],
                          left_row: &SlotRow,
                          right_row: &SlotRow|
         -> Result<bool> {
            #[cfg(test)]
            SQL_JOIN_CONSTRAINT_EVAL_CALLS.with(|calls| *calls.borrow_mut() += 1);
            Ok(bound
                .eval_truth(&BoundExprFrame {
                    user_calls: &[],
                    db: self.db_ref(),
                    columns: BoundExprColumns::Joined {
                        left: left_row,
                        right: right_row,
                        sources,
                    },
                    vars: &context.var_values,
                })?
                .unwrap_or(false))
        };

        for (left_idx, left_row) in left.rows.iter().enumerate() {
            if left_idx % 128 == 0 {
                self.check_cancellation()?;
            }
            let left_outer_row = if prepared_lookup.is_none() {
                let row = OuterSlotRow::from_slot(&left.columns, left_row);
                row_engine.outer_row = Some(row.clone());
                Some(row)
            } else {
                row_engine.outer_row = None;
                None
            };
            let ids = if let Some(lookup) = &prepared_lookup {
                self.prepared_dynamic_record_ids(&table, lookup, left_row)?
            } else {
                let Some(ids) = row_engine.indexed_record_ids_for_table_selection(
                    &table,
                    &alias_name,
                    schema.as_ref(),
                    Some(selection),
                )?
                else {
                    return Ok(None);
                };
                Rc::from(ids)
            };
            sql_profile_index_lookup();
            // The record itself is only consulted when the constraint is
            // evaluated against it (no prepared lookup); otherwise the right
            // rows come from cells.
            // With cells the right slot rows are cheap, so the constraint is
            // evaluated on slot rows (join_constraint_matches) instead of
            // fetching parsed records for the direct record-level check —
            // STOCK_LEVEL's order_line range join fetched ~200 parsed rows
            // per call through that check.
            let cell_schema = schema.as_ref().filter(|schema| {
                self.cell_rows_eligible(&table, schema, PolicyAction::Select)
                    .unwrap_or(false)
            });
            let needs_records = prepared_lookup.is_none()
                && can_eval_constraint_with_outer_row
                && cell_schema.is_none();
            let cell_right_rows = match cell_schema {
                Some(_) => {
                    let visible = self.visible_rows_for_pks(&table, &ids)?;
                    let (rows, _) = self.slot_rows_from_visible(
                        &table,
                        &alias_name,
                        &right_fields,
                        &visible,
                        false,
                    )?;
                    sql_profile_sql_rows_materialized(&rows);
                    Some(rows)
                }
                None => None,
            };
            let records = if cell_right_rows.is_some() {
                Vec::new()
            } else {
                let records = self.records_for_ids_with_schema(&table, &ids, schema.as_ref())?;
                sql_profile_records_materialized(&records);
                records
            };
            let candidate_count = cell_right_rows.as_ref().map_or(records.len(), Vec::len);
            candidate_pairs = candidate_pairs.saturating_add(candidate_count);
            let mut matched_left = false;
            let right_row_sources = cell_right_rows
                .iter()
                .flat_map(|rows| rows.iter().map(RightRowSource::Row))
                .chain(records.iter().map(RightRowSource::Record));
            for (record_idx, source) in right_row_sources.enumerate() {
                if record_idx % 1024 == 0 {
                    self.check_cancellation()?;
                }
                let direct_constraint_matches =
                    if let (RightRowSource::Record(record), true) = (&source, needs_records) {
                        let left_outer_row = left_outer_row
                            .as_ref()
                            .expect("left outer row is available without prepared lookup");
                        match constraint {
                            JoinConstraint::On(expr) => self.eval_record_outer_predicate(
                                &table,
                                &alias_name,
                                schema.as_ref(),
                                record,
                                left_outer_row,
                                expr,
                            )?,
                            _ => None,
                        }
                    } else {
                        None
                    };
                if matches!(direct_constraint_matches, Some(false)) {
                    continue;
                }
                // The join borrows resident slot rows for predicate checking;
                // only accepted output pairs need their values copied by merge.
                let right_row = source.slot_row(&table, &alias_name, &right_fields)?;
                let constraint_matches = if let Some(matches) = direct_constraint_matches {
                    matches
                } else if let Some(residual_constraint) = prepared_residual_constraint.as_ref() {
                    match (residual_constraint, &bound_residual) {
                        (JoinConstraint::None, _) => true,
                        (_, Some((bound, context, sources))) => {
                            eval_bound(bound, context, sources, left_row, &right_row)?
                        }
                        _ => {
                            #[cfg(test)]
                            SQL_JOIN_CONSTRAINT_PAIR_FALLBACKS
                                .with(|calls| *calls.borrow_mut() += 1);
                            row_engine.join_constraint_matches(
                                left_row,
                                &left.columns,
                                &right_row,
                                &right_columns,
                                residual_constraint,
                            )?
                        }
                    }
                } else {
                    match &bound_constraint {
                        Some((bound, context, sources)) => {
                            eval_bound(bound, context, sources, left_row, &right_row)?
                        }
                        None => {
                            #[cfg(test)]
                            SQL_JOIN_CONSTRAINT_PAIR_FALLBACKS
                                .with(|calls| *calls.borrow_mut() += 1);
                            row_engine.join_constraint_matches(
                                left_row,
                                &left.columns,
                                &right_row,
                                &right_columns,
                                constraint,
                            )?
                        }
                    }
                };
                if constraint_matches {
                    matched_left = true;
                    rows.push(merge_slot_rows(
                        left_row,
                        &left.columns,
                        &right_row,
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

        let columns = merge_row_set_columns(left.columns.clone(), right_columns);
        sql_profile_join_rows(&rows, candidate_pairs);
        Ok(Some(RowSet { rows, columns }))
    }

    pub(crate) fn apply_prepared_primary_key_right_table_join(
        &self,
        left: &mut RowSet,
        prepared: PreparedPrimaryKeyRightJoin<'_>,
    ) -> Result<RowSet> {
        // Each left row resolves to the position of its right record id in
        // `ids` (unique, first-seen order); the fetched right rows are kept
        // in that same order, so the pairing below is an index, not a string
        // lookup. This used to keep the id strings three times over (per
        // left row, a BTreeSet, a BTreeMap keyed by id) and compare strings
        // for every pair.
        let mut left_slots: Vec<Option<usize>> = Vec::with_capacity(left.rows.len());
        let mut ids: Vec<String> = Vec::new();
        let mut slot_of_id: FxHashMap<String, usize> = FxHashMap::default();
        for (left_idx, left_row) in left.rows.iter().enumerate() {
            if left_idx % 128 == 0 {
                self.check_cancellation()?;
            }
            let record_id = self.prepared_dynamic_primary_key_record_id(
                prepared.table,
                prepared.lookup,
                left_row,
            )?;
            let slot = record_id.map(|record_id| match slot_of_id.get(&record_id) {
                Some(slot) => *slot,
                None => {
                    let slot = ids.len();
                    ids.push(record_id.clone());
                    slot_of_id.insert(record_id, slot);
                    slot
                }
            });
            left_slots.push(slot);
        }

        let prepared_fields = &prepared.right_fields;
        // Right rows by slot (`None` = not visible / absent).
        let mut right_rows: Vec<Option<SlotRow>> = Vec::new();
        if !ids.is_empty() {
            sql_profile_index_lookup();
            let cell_schema = prepared.schema.filter(|schema| {
                self.cell_rows_eligible(prepared.table, schema, PolicyAction::Select)
                    .unwrap_or(false)
            });
            right_rows.resize_with(ids.len(), || None);
            if cell_schema.is_some() {
                // Right rows straight from cells: the join only needs their
                // slot rows, never the parsed record. `slot_rows_from_visible`
                // returns the present rows in `visible` order, so they are
                // placed back by walking `visible` once.
                let visible = self.visible_rows_for_pks(prepared.table, &ids)?;
                let (rows, _) = self.slot_rows_from_visible(
                    prepared.table,
                    prepared.alias_name,
                    &prepared_fields,
                    &visible,
                    false,
                )?;
                sql_profile_sql_rows_materialized(&rows);
                let mut rows = rows.into_iter();
                for (slot, present) in visible.iter().enumerate() {
                    if present.is_some() {
                        right_rows[slot] = rows.next();
                    }
                }
            } else {
                let records =
                    self.records_for_ids_with_schema(prepared.table, &ids, prepared.schema)?;
                sql_profile_records_materialized(&records);
                for (record_idx, record) in records.into_iter().enumerate() {
                    if record_idx % 1024 == 0 {
                        self.check_cancellation()?;
                    }
                    if let Some(slot) = slot_of_id.get(&record.id) {
                        right_rows[*slot] = Some(slot_row_from_record_fields(
                            prepared.table,
                            prepared.alias_name,
                            &prepared_fields,
                            &record,
                        )?);
                    }
                }
            }
        }

        let row_engine =
            (!matches!(prepared.residual_constraint, JoinConstraint::None)).then(|| {
                self.inherit_transaction(SqlEngine::with_ctes_and_context(
                    self.db_ref(),
                    self.settings,
                    self.ctes.clone(),
                    self.security_context.clone(),
                    self.session_gucs.clone(),
                ))
                .with_shared_routine_vars(self.routine_vars.clone())
                .with_cancellation(self.cancellation.clone())
            });
        // The residual constraint is bound once per join over the merged
        // layout; it used to be re-bound (merged column list, scope, slot
        // sources) for every candidate pair.
        let bound_residual = match prepared.residual_constraint {
            JoinConstraint::On(expr) => {
                let merged_columns =
                    merge_row_set_columns(left.columns.clone(), prepared.right_columns.clone());
                let (scope, context) = row_engine
                    .as_ref()
                    .expect("ON constraint engine")
                    .bound_row_context(&merged_columns);
                scope.bind(expr).map(|bound| {
                    (
                        bound,
                        context,
                        merged_slot_sources(&left.columns, &prepared.right_columns),
                    )
                })
            }
            _ => None,
        };
        // A PK lookup produces at most one output per left row. Transfer that
        // row through the pipeline; the previous implementation cloned every
        // upstream value at every join. Shared right rows stay reusable, while
        // uniquely referenced rows can transfer their values as well.
        let novel = merge_novel_right_ordinals(&left.columns, &prepared.right_columns);
        let mut right_uses = vec![0usize; right_rows.len()];
        for slot in left_slots.iter().flatten() {
            right_uses[*slot] += 1;
        }
        let left_rows = std::mem::take(&mut left.rows);
        let mut rows = Vec::with_capacity(left_rows.len());
        let mut candidate_pairs = 0usize;
        for (left_idx, mut left_row) in left_rows.into_iter().enumerate() {
            if left_idx % 128 == 0 {
                self.check_cancellation()?;
            }
            let mut matched_left = false;
            if let Some(slot) = left_slots[left_idx] {
                if let Some(right_row) = right_rows[slot].as_ref() {
                    candidate_pairs = candidate_pairs.saturating_add(1);
                    let constraint_matches = match (&prepared.residual_constraint, &bound_residual)
                    {
                        (JoinConstraint::None, _) => true,
                        (_, Some((bound, context, sources))) => {
                            #[cfg(test)]
                            SQL_JOIN_CONSTRAINT_EVAL_CALLS.with(|calls| *calls.borrow_mut() += 1);
                            bound
                                .eval_truth(&BoundExprFrame {
                                    user_calls: &[],
                                    db: self.db_ref(),
                                    columns: BoundExprColumns::Joined {
                                        left: &left_row,
                                        right: right_row,
                                        sources,
                                    },
                                    vars: &context.var_values,
                                })?
                                .unwrap_or(false)
                        }
                        _ => row_engine
                            .as_ref()
                            .expect("residual constraint engine")
                            .join_constraint_matches(
                                &left_row,
                                &left.columns,
                                right_row,
                                &prepared.right_columns,
                                prepared.residual_constraint,
                            )?,
                    };
                    if constraint_matches {
                        matched_left = true;
                        left_row.reserve(novel.len());
                        if right_uses[slot] == 1 {
                            let mut right_row = right_rows[slot].take().expect("visible right row");
                            for &index in novel.iter() {
                                left_row.push(
                                    right_row
                                        .get_mut(index)
                                        .map(|value| std::mem::replace(value, SqlValue::Null))
                                        .unwrap_or(SqlValue::Null),
                                );
                            }
                        } else {
                            for &index in novel.iter() {
                                left_row
                                    .push(right_row.get(index).cloned().unwrap_or(SqlValue::Null));
                            }
                        }
                    }
                }
                right_uses[slot] -= 1;
            }
            if !matched_left && matches!(prepared.kind, RowJoinKind::Left) {
                left_row.extend(novel.iter().map(|_| SqlValue::Null));
            }
            if matched_left || matches!(prepared.kind, RowJoinKind::Left) {
                rows.push(left_row);
            }
        }

        let columns =
            merge_row_set_columns(std::mem::take(&mut left.columns), prepared.right_columns);
        sql_profile_join_rows(&rows, candidate_pairs);
        Ok(RowSet { rows, columns })
    }

    pub(crate) fn join_constraint_matches(
        &self,
        left: &SlotRow,
        left_columns: &[String],
        right: &SlotRow,
        right_columns: &[String],
        constraint: &JoinConstraint,
    ) -> Result<bool> {
        #[cfg(test)]
        SQL_JOIN_CONSTRAINT_EVAL_CALLS.with(|calls| *calls.borrow_mut() += 1);
        match constraint {
            JoinConstraint::None => Ok(true),
            JoinConstraint::On(expr) => {
                let columns = merge_row_set_columns(left_columns.to_vec(), right_columns.to_vec());
                let (scope, context) = self.bound_row_context(&columns);
                if let Some(bound) = scope.bind(expr) {
                    let sources = merged_slot_sources(left_columns, right_columns);
                    return Ok(bound
                        .eval_truth(&BoundExprFrame {
                            user_calls: &[],
                            db: self.db_ref(),
                            columns: BoundExprColumns::Joined {
                                left,
                                right,
                                sources: &sources,
                            },
                            vars: &context.var_values,
                        })?
                        .unwrap_or(false));
                }

                sql_profile_join_predicate_row_merge();
                let merged = merge_slot_rows(left, left_columns, right, right_columns);
                self.eval_slot_row_predicate(&merged, &context, expr)
            }
            JoinConstraint::Using(columns) => {
                for column in columns {
                    let name = object_name(column)?;
                    let field = name.rsplit('.').next().unwrap_or(&name).to_string();
                    let left_value =
                        slot_row_value_from_parts(left_columns, left, std::slice::from_ref(&field));
                    let right_value = slot_row_value_from_parts(
                        right_columns,
                        right,
                        std::slice::from_ref(&field),
                    );
                    if !values_equal(&left_value, &right_value) {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            JoinConstraint::Natural => Err(SqlError::Unsupported(
                "NATURAL JOIN is not supported".to_string(),
            )),
        }
    }

    pub(crate) fn project_row_select(
        &self,
        projection: &[SelectItem],
        rows: &[SlotRow],
        wildcard_columns: &[String],
    ) -> Result<SqlResult> {
        self.project_slot_row_select_with_wildcard(
            projection,
            rows,
            wildcard_columns,
            wildcard_columns,
        )
    }

    pub(crate) fn project_row_select_with_array_set_functions(
        &self,
        select: &Select,
        rows: &[SlotRow],
        row_columns: &[String],
        wildcard_columns: &[String],
        calls: &[ArrayProjectionSetReturningCall],
    ) -> Result<SqlResult> {
        enum RowProjectionAction<'a> {
            Column(usize),
            Expr {
                expr: &'a Expr,
                bound: Option<BoundExpr>,
            },
            ArraySetFunction {
                call_index: usize,
                bounds: Vec<Option<BoundExpr>>,
            },
        }

        let (scope, context) = self.bound_row_context(row_columns);
        let mut columns = Vec::new();
        let mut actions = Vec::new();
        let mut set_function_output_indices = Vec::new();

        for (projection_index, item) in select.projection.iter().enumerate() {
            self.check_cancellation()?;
            if let Some(call_index) = calls
                .iter()
                .position(|call| call.projection_index == projection_index)
            {
                let (expr, alias) = match item {
                    SelectItem::UnnamedExpr(expr) => (expr, None),
                    SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
                    _ => unreachable!("projection SRFs are expression select items"),
                };
                columns.push(alias.unwrap_or_else(|| row_expr_column_name(expr)));
                set_function_output_indices.push(actions.len());
                actions.push(RowProjectionAction::ArraySetFunction {
                    call_index,
                    bounds: calls[call_index]
                        .arguments
                        .iter()
                        .map(|argument| scope.bind(argument))
                        .collect(),
                });
                continue;
            }
            match item {
                SelectItem::Wildcard(_) => {
                    for (idx, column) in wildcard_columns.iter().enumerate() {
                        columns.push(row_wildcard_output_column(column));
                        actions.push(RowProjectionAction::Column(idx));
                    }
                }
                SelectItem::QualifiedWildcard(qualifier, _) => {
                    for (source_column, output_column) in
                        qualified_wildcard_columns(qualifier, row_columns)?
                    {
                        let Some(idx) = slot_row_column_index(row_columns, &source_column) else {
                            return Err(SqlError::UndefinedColumn {
                                table: qualifier.to_string(),
                                column: source_column,
                            });
                        };
                        columns.push(output_column);
                        actions.push(RowProjectionAction::Column(idx));
                    }
                }
                SelectItem::UnnamedExpr(expr) => {
                    columns.push(row_expr_column_name(expr));
                    actions.push(RowProjectionAction::Expr {
                        expr,
                        bound: scope.bind(expr),
                    });
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    columns.push(alias.value.clone());
                    actions.push(RowProjectionAction::Expr {
                        expr,
                        bound: scope.bind(expr),
                    });
                }
                other => {
                    return Err(SqlError::Unsupported(format!(
                        "unsupported row projection {other}"
                    )));
                }
            }
        }

        let mut result_rows = Vec::new();
        for (row_index, row) in rows.iter().enumerate() {
            if row_index % 1024 == 0 {
                self.check_cancellation()?;
            }
            let mut base_row = Vec::with_capacity(actions.len());
            let mut expanded_sets = Vec::with_capacity(calls.len());
            for action in &actions {
                match action {
                    RowProjectionAction::Column(idx) => {
                        base_row.push(row.get(*idx).cloned().unwrap_or(SqlValue::Null));
                    }
                    RowProjectionAction::Expr { expr, bound } => {
                        let value = match bound {
                            Some(bound) => bound.eval(&BoundExprFrame {
                                user_calls: &[],
                                db: self.db_ref(),
                                columns: BoundExprColumns::Values(row),
                                vars: &context.var_values,
                            })?,
                            None => self.eval_slot_row_value(row, &context, expr)?,
                        };
                        base_row.push(value);
                    }
                    RowProjectionAction::ArraySetFunction { call_index, bounds } => {
                        let call = &calls[*call_index];
                        let arguments = call
                            .arguments
                            .iter()
                            .zip(bounds)
                            .map(|(argument, bound)| match bound {
                                Some(bound) => bound.eval(&BoundExprFrame {
                                    user_calls: &[],
                                    db: self.db_ref(),
                                    columns: BoundExprColumns::Values(row),
                                    vars: &context.var_values,
                                }),
                                None => self.eval_slot_row_value(row, &context, argument),
                            })
                            .collect::<Result<Vec<_>>>()?;
                        expanded_sets.push(array_projection_set_values(call.function, arguments)?);
                        base_row.push(SqlValue::Null);
                    }
                }
            }
            let row_count = expanded_sets.iter().map(Vec::len).max().unwrap_or_default();
            for expanded_index in 0..row_count {
                if expanded_index % 1024 == 0 {
                    self.check_cancellation()?;
                }
                let mut result_row = base_row.clone();
                for (set_index, output_index) in
                    set_function_output_indices.iter().copied().enumerate()
                {
                    result_row[output_index] = expanded_sets[set_index]
                        .get(expanded_index)
                        .cloned()
                        .unwrap_or(SqlValue::Null);
                }
                result_rows.push(result_row);
            }
        }

        let mut column_types = self
            .infer_row_select_column_types(select)
            .filter(|types| types.len() == columns.len())
            .unwrap_or_else(|| vec![None; columns.len()]);
        for (set_index, output_index) in set_function_output_indices.into_iter().enumerate() {
            column_types[output_index] = match calls[set_index].function {
                ArrayProjectionSetReturningFunction::GenerateSubscripts => Some("int4".to_string()),
                ArrayProjectionSetReturningFunction::PgSnapshotXip => Some("xid8".to_string()),
                ArrayProjectionSetReturningFunction::TxidSnapshotXip => Some("int8".to_string()),
                ArrayProjectionSetReturningFunction::Unnest => column_types[output_index].clone(),
            };
        }
        Ok(SqlResult::new(columns, result_rows).with_column_types(column_types))
    }

    pub(crate) fn project_row_select_with_json_set_function(
        &self,
        select: &Select,
        rows: &[SlotRow],
        row_columns: &[String],
        wildcard_columns: &[String],
        call: &JsonProjectionSetReturningCall,
    ) -> Result<SqlResult> {
        enum RowProjectionAction<'a> {
            Column(usize),
            Expr {
                expr: &'a Expr,
                bound: Option<BoundExpr>,
            },
            JsonSetFunction {
                bounds: Vec<Option<BoundExpr>>,
            },
        }

        let (scope, context) = self.bound_row_context(row_columns);
        let mut columns = Vec::new();
        let mut actions = Vec::new();
        let mut set_function_output_index = None;

        for (projection_index, item) in select.projection.iter().enumerate() {
            self.check_cancellation()?;
            if projection_index == call.projection_index {
                let (expr, alias) = match item {
                    SelectItem::UnnamedExpr(expr) => (expr, None),
                    SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
                    _ => unreachable!("JSON projection SRFs are expression select items"),
                };
                columns.push(alias.unwrap_or_else(|| row_expr_column_name(expr)));
                set_function_output_index = Some(actions.len());
                actions.push(RowProjectionAction::JsonSetFunction {
                    bounds: call
                        .arguments
                        .iter()
                        .map(|argument| scope.bind(argument))
                        .collect(),
                });
                continue;
            }
            match item {
                SelectItem::Wildcard(_) => {
                    for (idx, column) in wildcard_columns.iter().enumerate() {
                        columns.push(row_wildcard_output_column(column));
                        actions.push(RowProjectionAction::Column(idx));
                    }
                }
                SelectItem::QualifiedWildcard(qualifier, _) => {
                    for (source_column, output_column) in
                        qualified_wildcard_columns(qualifier, wildcard_columns)?
                    {
                        let Some(idx) = slot_row_column_index(row_columns, &source_column) else {
                            return Err(SqlError::UndefinedColumn {
                                table: qualifier.to_string(),
                                column: source_column,
                            });
                        };
                        columns.push(output_column);
                        actions.push(RowProjectionAction::Column(idx));
                    }
                }
                SelectItem::UnnamedExpr(expr) => {
                    columns.push(row_expr_column_name(expr));
                    actions.push(RowProjectionAction::Expr {
                        expr,
                        bound: scope.bind(expr),
                    });
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    columns.push(alias.value.clone());
                    actions.push(RowProjectionAction::Expr {
                        expr,
                        bound: scope.bind(expr),
                    });
                }
                other => {
                    return Err(SqlError::Unsupported(format!(
                        "unsupported row projection {other}"
                    )));
                }
            }
        }

        let set_function_output_index =
            set_function_output_index.expect("projection call has one output action");
        let mut result_rows = Vec::new();
        for (row_index, row) in rows.iter().enumerate() {
            if row_index % 1024 == 0 {
                self.check_cancellation()?;
            }
            let mut base_row = Vec::with_capacity(actions.len());
            let mut expanded = None;
            for action in &actions {
                match action {
                    RowProjectionAction::Column(idx) => {
                        base_row.push(row.get(*idx).cloned().unwrap_or(SqlValue::Null));
                    }
                    RowProjectionAction::Expr { expr, bound } => {
                        let value = match bound {
                            Some(bound) => bound.eval(&BoundExprFrame {
                                user_calls: &[],
                                db: self.db_ref(),
                                columns: BoundExprColumns::Values(row),
                                vars: &context.var_values,
                            })?,
                            None => self.eval_slot_row_value(row, &context, expr)?,
                        };
                        base_row.push(value);
                    }
                    RowProjectionAction::JsonSetFunction { bounds } => {
                        let arguments = call
                            .arguments
                            .iter()
                            .zip(bounds)
                            .map(|(argument, bound)| match bound {
                                Some(bound) => bound.eval(&BoundExprFrame {
                                    user_calls: &[],
                                    db: self.db_ref(),
                                    columns: BoundExprColumns::Values(row),
                                    vars: &context.var_values,
                                }),
                                None => self.eval_slot_row_value(row, &context, argument),
                            })
                            .collect::<Result<Vec<_>>>()?;
                        expanded = Some(json_set_function_values_from_args(
                            call.function,
                            &arguments,
                        )?);
                        base_row.push(SqlValue::Null);
                    }
                }
            }
            for (expanded_index, value) in expanded
                .expect("projection call has one evaluated output")
                .into_iter()
                .enumerate()
            {
                if expanded_index % 1024 == 0 {
                    self.check_cancellation()?;
                }
                let mut result_row = base_row.clone();
                result_row[set_function_output_index] = value;
                result_rows.push(result_row);
            }
        }

        let mut column_types = self
            .infer_row_select_column_types(select)
            .filter(|types| types.len() == columns.len())
            .unwrap_or_else(|| vec![None; columns.len()]);
        column_types[set_function_output_index] = Some(call.function.pg_type().to_string());
        Ok(SqlResult::new(columns, result_rows).with_column_types(column_types))
    }

    pub(crate) fn project_slot_row_select_with_wildcard(
        &self,
        projection: &[SelectItem],
        rows: &[SlotRow],
        row_columns: &[String],
        wildcard_columns: &[String],
    ) -> Result<SqlResult> {
        enum RowProjectionAction<'a> {
            Column(usize),
            Expr {
                expr: &'a Expr,
                bound: Option<BoundExpr>,
            },
        }

        let (scope, context) = self.bound_row_context(row_columns);
        let mut columns = Vec::new();
        let mut actions = Vec::new();

        for item in projection {
            self.check_cancellation()?;
            match item {
                SelectItem::Wildcard(_) => {
                    for (idx, column) in wildcard_columns.iter().enumerate() {
                        columns.push(row_wildcard_output_column(column));
                        actions.push(RowProjectionAction::Column(idx));
                    }
                }
                SelectItem::QualifiedWildcard(qualifier, _) => {
                    for (source_column, output_column) in
                        qualified_wildcard_columns(qualifier, row_columns)?
                    {
                        let Some(idx) = slot_row_column_index(row_columns, &source_column) else {
                            return Err(SqlError::UndefinedColumn {
                                table: qualifier.to_string(),
                                column: source_column,
                            });
                        };
                        columns.push(output_column);
                        actions.push(RowProjectionAction::Column(idx));
                    }
                }
                SelectItem::UnnamedExpr(expr) => {
                    columns.push(row_expr_column_name(expr));
                    actions.push(RowProjectionAction::Expr {
                        expr,
                        bound: scope.bind(expr),
                    });
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    columns.push(alias.value.clone());
                    actions.push(RowProjectionAction::Expr {
                        expr,
                        bound: scope.bind(expr),
                    });
                }
                other => {
                    return Err(SqlError::Unsupported(format!(
                        "unsupported row projection {other}"
                    )));
                }
            }
        }

        let mut result_rows = Vec::with_capacity(rows.len());
        for (idx, row) in rows.iter().enumerate() {
            if idx % 1024 == 0 {
                self.check_cancellation()?;
            }
            let mut result_row = Vec::with_capacity(actions.len());
            for action in &actions {
                match action {
                    RowProjectionAction::Column(idx) => {
                        result_row.push(row.get(*idx).cloned().unwrap_or(SqlValue::Null));
                    }
                    RowProjectionAction::Expr { expr, bound } => {
                        let value = match bound {
                            Some(bound) => bound.eval(&BoundExprFrame {
                                user_calls: &[],
                                db: self.db_ref(),
                                columns: BoundExprColumns::Values(row),
                                vars: &context.var_values,
                            })?,
                            None => self.eval_slot_row_value(row, &context, expr)?,
                        };
                        result_row.push(value);
                    }
                }
            }
            result_rows.push(result_row);
        }

        Ok(SqlResult::new(columns, result_rows))
    }

    pub(crate) fn project_sql_row_select(
        &self,
        projection: &[SelectItem],
        rows: &[SqlRow],
        wildcard_columns: &[String],
    ) -> Result<SqlResult> {
        enum RowProjectionAction<'a> {
            Column(String),
            Expr {
                expr: &'a Expr,
                bound: Option<BoundExpr>,
            },
        }

        let (scope, context) = self.bound_row_context(wildcard_columns);
        let mut columns = Vec::new();
        let mut actions = Vec::new();

        for item in projection {
            self.check_cancellation()?;
            match item {
                SelectItem::Wildcard(_) => {
                    for column in wildcard_columns {
                        columns.push(row_wildcard_output_column(column));
                        actions.push(RowProjectionAction::Column(column.clone()));
                    }
                }
                SelectItem::QualifiedWildcard(qualifier, _) => {
                    for (source_column, output_column) in
                        qualified_wildcard_columns(qualifier, wildcard_columns)?
                    {
                        columns.push(output_column);
                        actions.push(RowProjectionAction::Column(source_column));
                    }
                }
                SelectItem::UnnamedExpr(expr) => {
                    columns.push(row_expr_column_name(expr));
                    actions.push(RowProjectionAction::Expr {
                        expr,
                        bound: scope.bind(expr),
                    });
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    columns.push(alias.value.clone());
                    actions.push(RowProjectionAction::Expr {
                        expr,
                        bound: scope.bind(expr),
                    });
                }
                other => {
                    return Err(SqlError::Unsupported(format!(
                        "unsupported row projection {other}"
                    )));
                }
            }
        }

        let mut result_rows = Vec::with_capacity(rows.len());
        for (idx, row) in rows.iter().enumerate() {
            if idx % 1024 == 0 {
                self.check_cancellation()?;
            }
            let mut result_row = Vec::with_capacity(actions.len());
            for action in &actions {
                match action {
                    RowProjectionAction::Column(column) => {
                        result_row.push(row.get(column).cloned().unwrap_or(SqlValue::Null));
                    }
                    RowProjectionAction::Expr { expr, bound } => {
                        let value = match bound {
                            Some(bound) => bound.eval(&BoundExprFrame {
                                user_calls: &[],
                                db: self.db_ref(),
                                columns: BoundExprColumns::Row {
                                    row,
                                    column_keys: context.column_keys(),
                                },
                                vars: &context.var_values,
                            })?,
                            None => self.eval_row_value(row, expr)?,
                        };
                        result_row.push(value);
                    }
                }
            }
            result_rows.push(result_row);
        }

        Ok(SqlResult::new(columns, result_rows))
    }

    pub(crate) fn execute_row_aggregates(
        &self,
        projection: &[SelectItem],
        rows: &[SlotRow],
        columns: &[String],
    ) -> Result<SqlResult> {
        let (scope, context) = self.bound_row_context(columns);
        let mut columns = Vec::new();
        let mut values = Vec::new();
        for item in projection {
            self.check_cancellation()?;
            let (expr, alias) = select_item_expr_and_alias(item)?;
            columns.push(alias.unwrap_or_else(|| row_aggregate_column_name(expr)));
            values.push(self.eval_bound_row_aggregate_expr(rows, expr, &scope, &context)?);
        }
        Ok(SqlResult::new(columns, vec![values]))
    }

    pub(crate) fn eval_bound_row_aggregate_expr(
        &self,
        rows: &[SlotRow],
        expr: &Expr,
        scope: &BoundExprScope,
        context: &BoundRowContext,
    ) -> Result<SqlValue> {
        if let Ok((aggregate, cast)) = row_aggregate_from_expr(expr) {
            let filtered_rows;
            let rows = if let Some(filter) = aggregate_filter_from_expr(expr) {
                filtered_rows =
                    self.filter_bound_row_predicate(rows.to_vec(), context.column_keys(), filter)?;
                filtered_rows.as_slice()
            } else {
                rows
            };
            let value = aggregate.evaluate_bound(self, rows, scope, context)?;
            return match cast {
                Some(data_type) => cast_value(value, data_type),
                None => Ok(value),
            };
        }
        self.eval_row_aggregate_expr(rows, context.column_keys(), expr)
    }

    /// Replaces column references outside aggregate calls with their value
    /// from the group's representative row. Aggregate arguments are left
    /// untouched — they range over the whole group.
    fn inline_group_scalars(
        &self,
        expr: &mut Expr,
        first: &SlotRow,
        context: &BoundRowContext,
    ) -> Result<()> {
        match expr {
            Expr::Function(function) if is_aggregate_function(function) => Ok(()),
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
                let value = self.eval_slot_row_value(first, context, expr)?;
                *expr = sql_value_to_literal_expr(&value);
                Ok(())
            }
            Expr::Function(function) => {
                if let sqlparser::ast::FunctionArguments::List(list) = &mut function.args {
                    for arg in &mut list.args {
                        if let sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(inner),
                        ) = arg
                        {
                            self.inline_group_scalars(inner, first, context)?;
                        }
                    }
                }
                Ok(())
            }
            Expr::BinaryOp { left, right, .. } => {
                self.inline_group_scalars(left, first, context)?;
                self.inline_group_scalars(right, first, context)
            }
            Expr::UnaryOp { expr: inner, .. }
            | Expr::Nested(inner)
            | Expr::Cast { expr: inner, .. }
            | Expr::IsNull(inner)
            | Expr::IsNotNull(inner) => self.inline_group_scalars(inner, first, context),
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(operand) = operand {
                    self.inline_group_scalars(operand, first, context)?;
                }
                for condition in conditions {
                    self.inline_group_scalars(&mut condition.condition, first, context)?;
                    self.inline_group_scalars(&mut condition.result, first, context)?;
                }
                if let Some(else_result) = else_result {
                    self.inline_group_scalars(else_result, first, context)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub(crate) fn eval_row_aggregate_expr(
        &self,
        rows: &[SlotRow],
        columns: &[String],
        expr: &Expr,
    ) -> Result<SqlValue> {
        match expr {
            Expr::Function(function) if is_aggregate_function(function) => {
                // A nested aggregate keeps its FILTER clause
                // (`COALESCE(SUM(x) FILTER (WHERE ...), 0)`).
                if let Some(filter) = function.filter.as_deref() {
                    let filtered =
                        self.filter_bound_row_predicate(rows.to_vec(), columns, filter)?;
                    return RowAggregate::from_function(function)?
                        .evaluate(self, &filtered, columns);
                }
                RowAggregate::from_function(function)?.evaluate(self, rows, columns)
            }
            Expr::Function(function) => {
                self.eval_row_aggregate_function_value(rows, columns, function)
            }
            Expr::Value(value) => routine_var_from_value(&self.routine_vars, value)
                .map(Ok)
                .unwrap_or_else(|| literal_to_value(value)),
            Expr::BinaryOp { left, op, right } => eval_binary_expr_value(
                left,
                op,
                right,
                self.eval_row_aggregate_expr(rows, columns, left)?,
                self.eval_row_aggregate_expr(rows, columns, right)?,
                None,
            ),
            Expr::Cast {
                expr, data_type, ..
            } => {
                if let Some(source) = regclass_display_cast_source(expr, data_type) {
                    let value = self.eval_row_aggregate_expr(rows, columns, source)?;
                    return regclass_text_value(self.db_ref(), value);
                }
                if let Some(source) = regtype_text_cast_source(expr, data_type)? {
                    let value = self.eval_row_aggregate_expr(rows, columns, source)?;
                    return regtype_text_value(self.db_ref(), value);
                }
                if let Some(source) = regclass_text_cast_source(expr, data_type)? {
                    let value = self.eval_row_aggregate_expr(rows, columns, source)?;
                    return regclass_text_value(self.db_ref(), value);
                }
                let value = self.eval_row_aggregate_expr(rows, columns, expr)?;
                if pg_type_from_data_type(data_type).is_ok_and(|(pg_type, _)| {
                    matches!(pg_type.as_str(), "text" | "varchar" | "bpchar" | "name")
                }) {
                    let source_type = rows
                        .first()
                        .and_then(|_| self.infer_slot_row_expr_type(columns, expr))
                        .or_else(|| projected_expr_pg_type_with_db(self.db_ref(), expr));
                    if source_type.as_deref() == Some("bpchar") {
                        return Ok(match value {
                            SqlValue::String(value) => {
                                SqlValue::String(value.trim_end_matches(' ').to_string())
                            }
                            value => value,
                        });
                    }
                    if source_type.as_deref().is_some_and(is_oid_alias_type) {
                        return render_oid_alias_value(
                            self.db_ref(),
                            source_type.as_deref().expect("alias source type checked"),
                            &value,
                        )
                        .map(SqlValue::String);
                    }
                    if let Some(element_type) = source_type
                        .as_deref()
                        .and_then(|pg_type| pg_type.strip_suffix("[]"))
                    {
                        return postgres_array_text_value(
                            &value,
                            pg_type_delimiter_with_db(self.db_ref(), element_type)?,
                        )
                        .map(SqlValue::String);
                    }
                }
                cast_expr_value_with_db(self.db_ref(), value, expr, data_type, None)
            }
            Expr::Position { expr, r#in } => eval_position_typed_value(
                self.eval_row_aggregate_expr(rows, columns, expr)?,
                self.eval_row_aggregate_expr(rows, columns, r#in)?,
                projected_expr_pg_type_with_db(self.db_ref(), expr).as_deref() == Some("bytea")
                    || projected_expr_pg_type_with_db(self.db_ref(), r#in).as_deref()
                        == Some("bytea"),
            ),
            Expr::Extract { field, expr, .. } => {
                eval_extract_value(field, self.eval_row_aggregate_expr(rows, columns, expr)?)
            }
            Expr::Array(array) => sql_array_value(
                array
                    .elem
                    .iter()
                    .map(|expr| self.eval_row_aggregate_expr(rows, columns, expr))
                    .collect::<Result<Vec<_>>>()?,
            ),
            Expr::Tuple(exprs) => Ok(anonymous_record_value(
                exprs
                    .iter()
                    .map(|expr| self.eval_row_aggregate_expr(rows, columns, expr))
                    .collect::<Result<Vec<_>>>()?,
                exprs
                    .iter()
                    .map(|expr| projected_expr_pg_type_with_db(self.db_ref(), expr))
                    .collect(),
            )),
            Expr::CompoundFieldAccess { root, access_chain } => eval_access_chain_expr(
                root,
                access_chain,
                row_columns_expr_pg_type(self.db_ref(), root, columns.iter().map(String::as_str))
                    .as_deref()
                    == Some("jsonb"),
                |expr| self.eval_row_aggregate_expr(rows, columns, expr),
            ),
            Expr::Interval(interval) => interval_literal_value(interval, |expr| {
                self.eval_row_aggregate_expr(rows, columns, expr)
            }),
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
                |expr| self.eval_row_aggregate_expr(rows, columns, expr),
            ),
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => self.eval_row_aggregate_case(
                rows,
                columns,
                operand.as_deref(),
                conditions,
                else_result.as_deref(),
            ),
            Expr::Like { .. }
            | Expr::ILike { .. }
            | Expr::InList { .. }
            | Expr::Between { .. }
            | Expr::AnyOp { .. }
            | Expr::AllOp { .. }
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::IsTrue(_)
            | Expr::IsNotTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsNotFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotUnknown(_) => self
                .eval_row_aggregate_truth(rows, columns, expr)
                .map(|value| value.map(SqlValue::Bool).unwrap_or(SqlValue::Null)),
            Expr::Subquery(query) => self.execute_scalar_subquery(query),
            Expr::Nested(expr) => self.eval_row_aggregate_expr(rows, columns, expr),
            Expr::Collate { expr, .. } => self.eval_row_aggregate_expr(rows, columns, expr),
            Expr::UnaryOp { op, expr } if op.to_string().eq_ignore_ascii_case("NOT") => self
                .eval_row_aggregate_truth(rows, columns, expr)
                .map(sql_not)
                .map(|value| value.map(SqlValue::Bool).unwrap_or(SqlValue::Null)),
            Expr::UnaryOp { op, expr } if op.to_string() == "-" => eval_unary_minus_expr_value(
                expr,
                self.eval_row_aggregate_expr(rows, columns, expr)?,
                None,
            ),
            Expr::UnaryOp { op, expr } if op.to_string() == "+" => eval_unary_plus_expr_value(
                expr,
                self.eval_row_aggregate_expr(rows, columns, expr)?,
                None,
            ),
            Expr::UnaryOp { op, expr }
                if matches!(
                    op,
                    UnaryOperator::BitwiseNot | UnaryOperator::PGPrefixFactorial
                ) || is_geometric_unary_operator(op) =>
            {
                eval_unary_bit_not_expr_value(
                    op,
                    expr,
                    self.eval_row_aggregate_expr(rows, columns, expr)?,
                    None,
                )
            }
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => Err(SqlError::Unsupported(
                "aggregate SELECT cannot mix raw fields and aggregates without GROUP BY"
                    .to_string(),
            )),
            other => Err(SqlError::Unsupported(format!(
                "unsupported aggregate expression {other}"
            ))),
        }
    }

    pub(crate) fn eval_row_aggregate_function_value(
        &self,
        rows: &[SlotRow],
        columns: &[String],
        function: &Function,
    ) -> Result<SqlValue> {
        let name = object_name(&function.name)?.to_ascii_lowercase();
        let args = function_args(function)
            .iter()
            .map(|arg| self.eval_row_aggregate_expr(rows, columns, arg))
            .collect::<Result<Vec<_>>>()?;
        let arg_types = function_args(function)
            .iter()
            .map(|arg| {
                row_columns_expr_pg_type(self.db_ref(), arg, columns.iter().map(String::as_str))
            })
            .collect::<Vec<_>>();
        if matches!(name.as_str(), "pg_typeof" | "pg_catalog.pg_typeof") {
            return Ok(pg_typeof_result(
                arg_types.first().and_then(Option::as_ref),
                args.first(),
            ));
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
        if let Some(value) = self.eval_runtime_function_value(&name, &args)? {
            return Ok(value);
        }
        if let Some(value) = eval_session_function_value(&name, &args, &self.session_gucs)? {
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
        if let Some(value) = crate::eval_xml_function_value(&name, &args, Some(&arg_types))? {
            return Ok(value);
        }
        if let Some(value) = eval_catalog_function_value(&name, &args) {
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
        if let Some(value) = self.eval_routing_function_value_authorized(&name, &args)? {
            return Ok(value);
        }
        if let Some(value) = eval_spatial_function_value(&name, &args, &arg_types)? {
            return Ok(value);
        }
        Err(SqlError::Unsupported(format!(
            "function {} is not supported in aggregate projection",
            function.name
        )))
    }

    pub(crate) fn eval_row_aggregate_case(
        &self,
        rows: &[SlotRow],
        columns: &[String],
        operand: Option<&Expr>,
        conditions: &[sqlparser::ast::CaseWhen],
        else_result: Option<&Expr>,
    ) -> Result<SqlValue> {
        let operand_value = operand
            .map(|expr| self.eval_row_aggregate_expr(rows, columns, expr))
            .transpose()?;
        for condition in conditions {
            let matched = if let Some(operand_value) = &operand_value {
                values_equal(
                    operand_value,
                    &self.eval_row_aggregate_expr(rows, columns, &condition.condition)?,
                )
            } else {
                self.eval_row_aggregate_truth(rows, columns, &condition.condition)?
                    .unwrap_or(false)
            };
            if matched {
                return self.eval_row_aggregate_expr(rows, columns, &condition.result);
            }
        }
        else_result
            .map(|expr| self.eval_row_aggregate_expr(rows, columns, expr))
            .unwrap_or(Ok(SqlValue::Null))
    }

    pub(crate) fn eval_row_aggregate_truth(
        &self,
        rows: &[SlotRow],
        columns: &[String],
        expr: &Expr,
    ) -> Result<Option<bool>> {
        match expr {
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::And => Ok(sql_and(
                    self.eval_row_aggregate_truth(rows, columns, left)?,
                    self.eval_row_aggregate_truth(rows, columns, right)?,
                )),
                BinaryOperator::Or => Ok(sql_or(
                    self.eval_row_aggregate_truth(rows, columns, left)?,
                    self.eval_row_aggregate_truth(rows, columns, right)?,
                )),
                BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq => {
                    let mut eval = |expr: &Expr| self.eval_row_aggregate_expr(rows, columns, expr);
                    if let Some(truth) = eval_tuple_comparison(left, op, right, &mut eval)? {
                        return Ok(truth);
                    }
                    compare_values(
                        &self.eval_row_aggregate_expr(rows, columns, left)?,
                        op,
                        &self.eval_row_aggregate_expr(rows, columns, right)?,
                    )
                }
                BinaryOperator::PGLikeMatch
                | BinaryOperator::PGILikeMatch
                | BinaryOperator::PGNotLikeMatch
                | BinaryOperator::PGNotILikeMatch
                | BinaryOperator::PGRegexMatch
                | BinaryOperator::PGRegexIMatch
                | BinaryOperator::PGRegexNotMatch
                | BinaryOperator::PGRegexNotIMatch => eval_pg_pattern_operator(
                    &self.eval_row_aggregate_expr(rows, columns, left)?,
                    op,
                    &self.eval_row_aggregate_expr(rows, columns, right)?,
                ),
                BinaryOperator::AtArrow | BinaryOperator::ArrowAt => eval_containment_truth(
                    self.eval_row_aggregate_expr(rows, columns, left)?,
                    op,
                    self.eval_row_aggregate_expr(rows, columns, right)?,
                ),
                _ => self
                    .eval_row_aggregate_expr(rows, columns, expr)
                    .and_then(sql_value_truth),
            },
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let mut eval = |expr: &Expr| self.eval_row_aggregate_expr(rows, columns, expr);
                if let Some(truth) = eval_tuple_in_list_truth(expr, list, *negated, &mut eval)? {
                    return Ok(truth);
                }
                eval_in_list_truth(
                    self.eval_row_aggregate_expr(rows, columns, expr)?,
                    list.iter()
                        .map(|expr| self.eval_row_aggregate_expr(rows, columns, expr))
                        .collect::<Result<Vec<_>>>()?,
                    *negated,
                )
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let mut eval = |expr: &Expr| self.eval_row_aggregate_expr(rows, columns, expr);
                if let Some(truth) = eval_tuple_between_truth(expr, low, high, *negated, &mut eval)?
                {
                    return Ok(truth);
                }
                eval_between_truth(
                    self.eval_row_aggregate_expr(rows, columns, expr)?,
                    self.eval_row_aggregate_expr(rows, columns, low)?,
                    self.eval_row_aggregate_expr(rows, columns, high)?,
                    *negated,
                )
            }
            Expr::AnyOp {
                left,
                compare_op,
                right,
                ..
            } => eval_quantified_truth(
                self.eval_row_aggregate_expr(rows, columns, left)?,
                compare_op,
                self.eval_row_aggregate_expr(rows, columns, right)?,
                false,
            ),
            Expr::AllOp {
                left,
                compare_op,
                right,
            } => eval_quantified_truth(
                self.eval_row_aggregate_expr(rows, columns, left)?,
                compare_op,
                self.eval_row_aggregate_expr(rows, columns, right)?,
                true,
            ),
            Expr::IsNull(expr) => Ok(Some(value_is_null_predicate(
                &self.eval_row_aggregate_expr(rows, columns, expr)?,
            ))),
            Expr::IsNotNull(expr) => Ok(Some(value_is_not_null_predicate(
                &self.eval_row_aggregate_expr(rows, columns, expr)?,
            ))),
            Expr::IsDistinctFrom(left, right) => {
                let mut eval = |expr: &Expr| self.eval_row_aggregate_expr(rows, columns, expr);
                if let Some(not_distinct) = eval_tuple_not_distinct(left, right, &mut eval)? {
                    return Ok(Some(!not_distinct));
                }
                Ok(Some(!values_not_distinct(
                    &self.eval_row_aggregate_expr(rows, columns, left)?,
                    &self.eval_row_aggregate_expr(rows, columns, right)?,
                )))
            }
            Expr::IsNotDistinctFrom(left, right) => {
                let mut eval = |expr: &Expr| self.eval_row_aggregate_expr(rows, columns, expr);
                if let Some(not_distinct) = eval_tuple_not_distinct(left, right, &mut eval)? {
                    return Ok(Some(not_distinct));
                }
                Ok(Some(values_not_distinct(
                    &self.eval_row_aggregate_expr(rows, columns, left)?,
                    &self.eval_row_aggregate_expr(rows, columns, right)?,
                )))
            }
            Expr::Like {
                negated,
                any,
                expr,
                pattern,
                escape_char,
            } => {
                if *any {
                    return Err(SqlError::Unsupported(
                        "LIKE ANY is not supported".to_string(),
                    ));
                }
                eval_like_values(
                    self.eval_row_aggregate_expr(rows, columns, expr)?,
                    self.eval_row_aggregate_expr(rows, columns, pattern)?,
                    *negated,
                    false,
                    escape_char.as_ref(),
                )
            }
            Expr::ILike {
                negated,
                any,
                expr,
                pattern,
                escape_char,
            } => {
                if *any {
                    return Err(SqlError::Unsupported(
                        "ILIKE ANY is not supported".to_string(),
                    ));
                }
                eval_like_values(
                    self.eval_row_aggregate_expr(rows, columns, expr)?,
                    self.eval_row_aggregate_expr(rows, columns, pattern)?,
                    *negated,
                    true,
                    escape_char.as_ref(),
                )
            }
            Expr::SimilarTo { .. } => Err(SqlError::Unsupported(
                "SIMILAR TO is not supported".to_string(),
            )),
            Expr::IsTrue(expr) => Ok(Some(matches!(
                self.eval_row_aggregate_truth(rows, columns, expr)?,
                Some(true)
            ))),
            Expr::IsNotTrue(expr) => Ok(Some(!matches!(
                self.eval_row_aggregate_truth(rows, columns, expr)?,
                Some(true)
            ))),
            Expr::IsFalse(expr) => Ok(Some(matches!(
                self.eval_row_aggregate_truth(rows, columns, expr)?,
                Some(false)
            ))),
            Expr::IsNotFalse(expr) => Ok(Some(!matches!(
                self.eval_row_aggregate_truth(rows, columns, expr)?,
                Some(false)
            ))),
            Expr::IsUnknown(expr) => Ok(Some(
                self.eval_row_aggregate_truth(rows, columns, expr)?
                    .is_none(),
            )),
            Expr::IsNotUnknown(expr) => Ok(Some(
                self.eval_row_aggregate_truth(rows, columns, expr)?
                    .is_some(),
            )),
            Expr::UnaryOp { op, expr } if op.to_string().eq_ignore_ascii_case("NOT") => {
                Ok(sql_not(self.eval_row_aggregate_truth(rows, columns, expr)?))
            }
            Expr::Nested(expr) => self.eval_row_aggregate_truth(rows, columns, expr),
            _ => self
                .eval_row_aggregate_expr(rows, columns, expr)
                .and_then(sql_value_truth),
        }
    }

    pub(crate) fn execute_grouped_rows(
        &self,
        projection: &[SelectItem],
        group_exprs: &[Expr],
        query: &Query,
        row_columns: &[String],
        rows: Vec<SlotRow>,
        group_types: &[Option<String>],
    ) -> Result<SqlResult> {
        let mut groups = BTreeMap::<Vec<String>, Vec<SlotRow>>::new();
        let (group_scope, group_context) = self.bound_row_context(row_columns);
        let group_exprs = group_exprs
            .iter()
            .map(|expr| (expr, group_scope.bind(expr)))
            .collect::<Vec<_>>();
        for (idx, row) in rows.into_iter().enumerate() {
            if idx % 1024 == 0 {
                self.check_cancellation()?;
            }
            let values = group_exprs
                .iter()
                .map(|(expr, bound)| {
                    let value = match bound {
                        Some(bound) => bound.eval(&BoundExprFrame {
                            user_calls: &[],
                            db: self.db_ref(),
                            columns: BoundExprColumns::Values(&row),
                            vars: &group_context.var_values,
                        })?,
                        None => self.eval_slot_row_value(&row, &group_context, expr)?,
                    };
                    Ok(value)
                })
                .collect::<Result<Vec<_>>>()?;
            let key = values
                .iter()
                .enumerate()
                .map(
                    |(index, value)| match group_types.get(index).and_then(Option::as_deref) {
                        Some(pg_type) => pg_typed_index_label_for_db(self.db_ref(), pg_type, value),
                        None => Ok(value.to_cell()),
                    },
                )
                .collect::<Result<Vec<_>>>()?;
            groups.entry(key).or_default().push(row);
        }
        let mut groups = groups.into_values().collect::<Vec<_>>();
        self.check_cancellation()?;
        self.apply_group_order_by(&mut groups, query.order_by.as_ref(), row_columns)?;
        self.check_cancellation()?;
        apply_row_limit(&mut groups, query)?;

        let mut columns = Vec::new();
        for item in projection {
            self.check_cancellation()?;
            let (expr, alias) = select_item_expr_and_alias(item)?;
            let column = if let Some(alias) = alias {
                alias
            } else if let Ok((aggregate, _)) = row_aggregate_from_expr(expr) {
                aggregate.column_name()
            } else {
                row_expr_column_name(expr)
            };
            columns.push(column);
        }

        let mut result_rows = Vec::new();
        for (group_idx, group) in groups.iter().enumerate() {
            if group_idx % 1024 == 0 {
                self.check_cancellation()?;
            }
            let first = group.first().ok_or_else(|| {
                SqlError::InvalidSql("GROUP BY produced an empty group".to_string())
            })?;
            let mut result_row = Vec::new();
            for item in projection {
                self.check_cancellation()?;
                let (expr, _) = select_item_expr_and_alias(item)?;
                if let Ok((aggregate, cast)) = row_aggregate_from_expr(expr) {
                    // FILTER restricts the aggregate to the group's matching
                    // rows (`COUNT(*) FILTER (WHERE ...)`).
                    let filtered_group;
                    let group = if let Some(filter) = aggregate_filter_from_expr(expr) {
                        filtered_group =
                            self.filter_bound_row_predicate(group.to_vec(), row_columns, filter)?;
                        filtered_group.as_slice()
                    } else {
                        group.as_slice()
                    };
                    let value = aggregate.evaluate(self, group, row_columns)?;
                    result_row.push(match cast {
                        Some(data_type) => cast_value(value, data_type)?,
                        None => value,
                    });
                } else if expr_contains_aggregate(expr) {
                    // An aggregate nested inside a scalar expression
                    // (`COALESCE(SUM(x), 0)`, `SUM(a) <= g.key`) evaluates
                    // over the whole group. Group-key columns referenced
                    // outside the aggregates are constant within the group,
                    // so they inline as literals from its first row before
                    // the aggregate evaluator runs.
                    let (scope, context) = self.bound_row_context(row_columns);
                    let mut expr = expr.clone();
                    self.inline_group_scalars(&mut expr, first, &context)?;
                    result_row
                        .push(self.eval_bound_row_aggregate_expr(group, &expr, &scope, &context)?);
                } else {
                    let (scope, context) = self.bound_row_context(row_columns);
                    let value = match scope.bind(expr) {
                        Some(bound) => bound.eval(&BoundExprFrame {
                            user_calls: &[],
                            db: self.db_ref(),
                            columns: BoundExprColumns::Values(first),
                            vars: &context.var_values,
                        })?,
                        None => self.eval_slot_row_value(first, &context, expr)?,
                    };
                    result_row.push(value);
                }
            }
            result_rows.push(result_row);
        }

        Ok(SqlResult::new(columns, result_rows))
    }

    pub(crate) fn execute_grouped_window_rows(
        &self,
        select: &Select,
        group_exprs: &[Expr],
        query: &Query,
        row_columns: &[String],
        rows: Vec<SlotRow>,
        group_types: &[Option<String>],
    ) -> Result<SqlResult> {
        let mut groups = BTreeMap::<Vec<String>, Vec<SlotRow>>::new();
        let (scope, context) = self.bound_row_context(row_columns);
        let group_exprs = group_exprs
            .iter()
            .map(|expr| (expr, scope.bind(expr)))
            .collect::<Vec<_>>();
        for row in rows {
            let values = group_exprs
                .iter()
                .map(|(expr, bound)| {
                    let value = match bound {
                        Some(bound) => bound.eval(&BoundExprFrame {
                            user_calls: &[],
                            db: self.db_ref(),
                            columns: BoundExprColumns::Values(&row),
                            vars: &context.var_values,
                        })?,
                        None => self.eval_slot_row_value(&row, &context, expr)?,
                    };
                    Ok(value)
                })
                .collect::<Result<Vec<_>>>()?;
            let key = values
                .iter()
                .enumerate()
                .map(
                    |(index, value)| match group_types.get(index).and_then(Option::as_deref) {
                        Some(pg_type) => pg_typed_index_label_for_db(self.db_ref(), pg_type, value),
                        None => Ok(value.to_cell()),
                    },
                )
                .collect::<Result<Vec<_>>>()?;
            groups.entry(key).or_default().push(row);
        }
        if group_exprs.is_empty() && groups.is_empty() {
            groups.insert(Vec::new(), Vec::new());
        }

        let aggregates = query_group_aggregate_functions(select, query)?;
        let mut grouped_columns = row_columns.to_vec();
        grouped_columns.extend(aggregates.iter().map(group_aggregate_column_name));
        let mut grouped_rows = Vec::with_capacity(groups.len());
        for group in groups.into_values() {
            self.check_cancellation()?;
            let mut row = group
                .first()
                .cloned()
                .unwrap_or_else(|| vec![SqlValue::Null; row_columns.len()]);
            for function in &aggregates {
                let mut aggregate_rows = group.clone();
                if let Some(filter) = &function.filter {
                    aggregate_rows =
                        self.filter_bound_row_predicate(aggregate_rows, row_columns, filter)?;
                }
                row.push(RowAggregate::from_function(function)?.evaluate(
                    self,
                    &aggregate_rows,
                    row_columns,
                )?);
            }
            grouped_rows.push(row);
        }
        if let Some(having) = &select.having {
            grouped_rows =
                self.filter_bound_row_predicate(grouped_rows, &grouped_columns, having)?;
        }

        self.apply_row_windows(select, query, &mut grouped_rows, &mut grouped_columns)?;
        self.apply_row_order_by(
            &mut grouped_rows,
            query.order_by.as_ref(),
            &grouped_columns,
            &[],
            order_by_keep_bound(query)?,
        )?;
        apply_row_limit(&mut grouped_rows, query)?;
        self.project_slot_row_select_with_wildcard(
            &select.projection,
            &grouped_rows,
            &grouped_columns,
            row_columns,
        )
    }

    /// `keep`: when `Some(k)`, only the k best-ranked rows survive — a
    /// constant LIMIT+OFFSET truncation follows immediately, so the sort
    /// selects the top k in O(n), ranks only those, and truncates `rows`
    /// before downstream projection ever sees the rest. Callers whose
    /// pipeline can still reshape rows (SRFs, DISTINCT) must pass `None`.
    pub(crate) fn apply_row_order_by(
        &self,
        rows: &mut Vec<SlotRow>,
        order_by: Option<&OrderBy>,
        columns: &[String],
        order_types: &[Option<String>],
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
        self.check_cancellation()?;
        let (scope, context) = self.bound_row_context(columns);
        let bound = expressions
            .iter()
            .map(|order| scope.bind(&order.expr))
            .collect::<Vec<_>>();
        let mut keyed_rows = rows
            .iter()
            .map(|row| {
                let keys = expressions
                    .iter()
                    .zip(&bound)
                    .enumerate()
                    .map(|(index, (order, bound))| {
                        let evaluated = match bound {
                            Some(bound) => bound.eval(&BoundExprFrame {
                                user_calls: &[],
                                db: self.db_ref(),
                                columns: BoundExprColumns::Values(&row),
                                vars: &context.var_values,
                            }),
                            None => self.eval_slot_row_value(&row, &context, &order.expr),
                        };
                        let value = match evaluated {
                            Ok(value) => value,
                            Err(error @ SqlError::DataException { .. }) => return Err(error),
                            Err(_) => SqlValue::Null,
                        };
                        let pg_type = order_types.get(index).and_then(Option::as_deref);
                        let value = enforce_integer_value_type(value, pg_type)?;
                        let typed_key = pg_type
                            .filter(|_| !matches!(value, SqlValue::Null))
                            .map(|pg_type| {
                                pg_typed_index_key_for_db(self.db_ref(), pg_type, &value)
                            })
                            .transpose()?;
                        Ok((value, typed_key))
                    })
                    .collect::<Result<Vec<_>>>()?;
                // Keep ownership in `rows` until every key has evaluated:
                // an error must leave the input untouched. Empty row slots
                // need no allocation and are filled by moving below.
                Ok((keys, SlotRow::new()))
            })
            .collect::<Result<Vec<_>>>()?;
        for ((_, owned), row) in keyed_rows.iter_mut().zip(rows.drain(..)) {
            *owned = row;
        }
        let key_compare =
            |(left_values, _): &(Vec<(SqlValue, Option<Vec<u8>>)>, SlotRow),
             (right_values, _): &(Vec<(SqlValue, Option<Vec<u8>>)>, SlotRow)| {
                expressions
                    .iter()
                    .zip(left_values.iter().zip(right_values))
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
            // Top-K: partition the k best to the front in O(n) and rank only
            // those. The original row index breaks ties, which makes this
            // BIT-IDENTICAL to the stable full sort + truncate it replaces —
            // load-bearing, because ORDER BY keys the evaluator cannot score
            // degrade to Null (all-equal), and an unstable partition would
            // turn that silent degradation into nondeterministic results.
            Some(keep) if keep < keyed_rows.len() => {
                if keep == 0 {
                    keyed_rows.clear();
                } else {
                    let mut indexed: Vec<(usize, (Vec<(SqlValue, Option<Vec<u8>>)>, SlotRow))> =
                        keyed_rows.into_iter().enumerate().collect();
                    let indexed_compare = |left: &(
                        usize,
                        (Vec<(SqlValue, Option<Vec<u8>>)>, SlotRow),
                    ),
                                           right: &(
                        usize,
                        (Vec<(SqlValue, Option<Vec<u8>>)>, SlotRow),
                    )| {
                        key_compare(&left.1, &right.1).then(left.0.cmp(&right.0))
                    };
                    indexed.select_nth_unstable_by(keep - 1, indexed_compare);
                    indexed.truncate(keep);
                    indexed.sort_by(indexed_compare);
                    keyed_rows = indexed.into_iter().map(|(_, entry)| entry).collect();
                }
            }
            _ => keyed_rows.sort_by(key_compare),
        }
        rows.clear();
        rows.extend(keyed_rows.into_iter().map(|(_, row)| row));
        Ok(())
    }

    pub(crate) fn apply_row_windows(
        &self,
        select: &Select,
        query: &Query,
        rows: &mut [SlotRow],
        columns: &mut Vec<String>,
    ) -> Result<()> {
        for function in query_window_functions(select, query)? {
            self.check_cancellation()?;
            let values = self.evaluate_row_window(&function, select, rows, columns)?;
            for (row, value) in rows.iter_mut().zip(values) {
                row.push(value);
            }
            columns.push(window_column_name(&function));
        }
        Ok(())
    }

    pub(crate) fn evaluate_row_window(
        &self,
        function: &Function,
        select: &Select,
        rows: &[SlotRow],
        columns: &[String],
    ) -> Result<Vec<SqlValue>> {
        let spec = resolve_window_spec(function, select)?;
        let (scope, context) = self.bound_row_context(columns);
        let eval = |row: &SlotRow, expr: &Expr| -> Result<SqlValue> {
            match scope.bind(expr) {
                Some(bound) => bound.eval(&BoundExprFrame {
                    user_calls: &[],
                    db: self.db_ref(),
                    columns: BoundExprColumns::Values(row),
                    vars: &context.var_values,
                }),
                None => self.eval_slot_row_value(row, &context, expr),
            }
        };

        let mut partition_keys = Vec::with_capacity(rows.len());
        let mut order_keys = Vec::with_capacity(rows.len());
        for (index, row) in rows.iter().enumerate() {
            if index % 1024 == 0 {
                self.check_cancellation()?;
            }
            partition_keys.push(
                spec.partition_by
                    .iter()
                    .map(|expr| eval(row, expr))
                    .collect::<Result<Vec<_>>>()?,
            );
            order_keys.push(
                spec.order_by
                    .iter()
                    .map(|order| eval(row, &order.expr))
                    .collect::<Result<Vec<_>>>()?,
            );
        }

        let mut indices = (0..rows.len()).collect::<Vec<_>>();
        indices.sort_by(|left, right| {
            let partition_ordering = partition_keys[*left]
                .iter()
                .zip(&partition_keys[*right])
                .map(|(left, right)| order_value_ordering(left, right, &OrderByOptions::default()))
                .find(|ordering| *ordering != Ordering::Equal)
                .unwrap_or(Ordering::Equal);
            if partition_ordering != Ordering::Equal {
                return partition_ordering;
            }
            spec.order_by
                .iter()
                .zip(order_keys[*left].iter().zip(&order_keys[*right]))
                .map(|(order, (left, right))| {
                    typed_order_expr_value_ordering(
                        &order.expr,
                        left,
                        None,
                        right,
                        None,
                        &order.options,
                    )
                })
                .find(|ordering| *ordering != Ordering::Equal)
                .unwrap_or(Ordering::Equal)
        });

        let mut result = vec![SqlValue::Null; rows.len()];
        let mut start = 0;
        while start < indices.len() {
            let mut end = start + 1;
            while end < indices.len()
                && window_keys_are_peers(
                    &partition_keys[indices[start]],
                    &partition_keys[indices[end]],
                )
            {
                end += 1;
            }
            self.evaluate_row_window_partition(
                function,
                &spec,
                rows,
                columns,
                &context,
                &indices[start..end],
                &order_keys,
                &mut result,
            )?;
            start = end;
        }
        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn evaluate_row_window_partition(
        &self,
        function: &Function,
        spec: &WindowSpec,
        rows: &[SlotRow],
        columns: &[String],
        context: &BoundRowContext,
        partition: &[usize],
        order_keys: &[Vec<SqlValue>],
        result: &mut [SqlValue],
    ) -> Result<()> {
        let raw_name = object_name(&function.name)?.to_ascii_lowercase();
        let name = raw_name.strip_prefix("pg_catalog.").unwrap_or(&raw_name);
        let args = function_args(function);
        let aggregate = matches!(
            name,
            "count" | "sum" | "avg" | "min" | "max" | "bool_and" | "bool_or" | "every"
        )
        .then(|| RowAggregate::from_function(function))
        .transpose()?;
        let ignore_nulls = matches!(function.null_treatment, Some(NullTreatment::IgnoreNulls));
        let mut rank = 1usize;
        let mut dense_rank = 1usize;

        for position in 0..partition.len() {
            if position % 1024 == 0 {
                self.check_cancellation()?;
            }
            let row_index = partition[position];
            let row = &rows[row_index];
            let peer = position > 0
                && window_keys_are_peers(
                    &order_keys[partition[position - 1]],
                    &order_keys[row_index],
                );
            if position > 0 && !peer {
                rank = position + 1;
                dense_rank += 1;
            }

            let value = match name {
                "row_number" => SqlValue::Int((position + 1) as i64),
                "rank" => SqlValue::Int(rank as i64),
                "dense_rank" => SqlValue::Int(dense_rank as i64),
                "percent_rank" => {
                    if partition.len() <= 1 {
                        SqlValue::Float(0.0)
                    } else {
                        SqlValue::Float((rank - 1) as f64 / (partition.len() - 1) as f64)
                    }
                }
                "cume_dist" => SqlValue::Float(
                    (window_peer_end(partition, order_keys, position) + 1) as f64
                        / partition.len() as f64,
                ),
                "ntile" => {
                    let [buckets_expr] = args.as_slice() else {
                        return Err(SqlError::InvalidSql(
                            "NTILE expects exactly one argument".to_string(),
                        ));
                    };
                    let buckets =
                        sql_value_i64(&self.eval_slot_row_value(row, context, buckets_expr)?)
                            .ok_or_else(|| {
                                SqlError::InvalidSql(
                                    "NTILE argument must be an integer".to_string(),
                                )
                            })?;
                    if buckets <= 0 {
                        return Err(SqlError::InvalidSql(
                            "NTILE argument must be greater than zero".to_string(),
                        ));
                    }
                    SqlValue::Int(window_ntile(position, partition.len(), buckets as usize) as i64)
                }
                "lag" | "lead" => {
                    if args.is_empty() || args.len() > 3 {
                        return Err(SqlError::InvalidSql(format!(
                            "{} expects one to three arguments",
                            name.to_ascii_uppercase()
                        )));
                    }
                    let offset = match args.get(1) {
                        Some(expr) => sql_value_i64(&self.eval_slot_row_value(row, context, expr)?)
                            .ok_or_else(|| {
                                SqlError::InvalidSql("window offset must be an integer".to_string())
                            })?,
                        None => 1,
                    };
                    let direction = if name == "lag" { -1i64 } else { 1i64 };
                    if ignore_nulls && offset != 0 {
                        let direction = direction * offset.signum();
                        let mut remaining = offset.unsigned_abs();
                        let mut target = position as i64;
                        let mut found = None;
                        while remaining > 0 {
                            target += direction;
                            if target < 0 || target as usize >= partition.len() {
                                break;
                            }
                            let candidate = self.eval_slot_row_value(
                                &rows[partition[target as usize]],
                                context,
                                &args[0],
                            )?;
                            if !matches!(candidate, SqlValue::Null) {
                                remaining -= 1;
                                if remaining == 0 {
                                    found = Some(candidate);
                                }
                            }
                        }
                        if let Some(value) = found {
                            value
                        } else if let Some(default) = args.get(2) {
                            self.eval_slot_row_value(row, context, default)?
                        } else {
                            SqlValue::Null
                        }
                    } else if let Some(default) = args.get(2) {
                        let target = position as i64 + direction * offset;
                        if target >= 0 && (target as usize) < partition.len() {
                            self.eval_slot_row_value(
                                &rows[partition[target as usize]],
                                context,
                                &args[0],
                            )?
                        } else {
                            self.eval_slot_row_value(row, context, default)?
                        }
                    } else {
                        let target = position as i64 + direction * offset;
                        if target >= 0 && (target as usize) < partition.len() {
                            self.eval_slot_row_value(
                                &rows[partition[target as usize]],
                                context,
                                &args[0],
                            )?
                        } else {
                            SqlValue::Null
                        }
                    }
                }
                "first_value" | "last_value" | "nth_value" => {
                    let expr = args.first().ok_or_else(|| {
                        SqlError::InvalidSql(format!(
                            "{} expects a value argument",
                            name.to_ascii_uppercase()
                        ))
                    })?;
                    let frame = self
                        .window_frame_range(spec, rows, context, partition, order_keys, position)?;
                    let nth = if name == "nth_value" {
                        let nth = sql_value_i64(&self.eval_slot_row_value(row, context, &args[1])?)
                            .ok_or_else(|| {
                                SqlError::InvalidSql(
                                    "NTH_VALUE position must be an integer".to_string(),
                                )
                            })?;
                        if nth <= 0 {
                            return Err(SqlError::InvalidSql(
                                "NTH_VALUE position must be greater than zero".to_string(),
                            ));
                        }
                        nth as usize
                    } else {
                        1
                    };
                    if let Some((frame_start, frame_end)) = frame {
                        let positions: Box<dyn Iterator<Item = usize>> = if name == "last_value" {
                            Box::new((frame_start..=frame_end).rev())
                        } else {
                            Box::new(frame_start..=frame_end)
                        };
                        let mut seen = 0usize;
                        let mut value = SqlValue::Null;
                        for target in positions {
                            let candidate =
                                self.eval_slot_row_value(&rows[partition[target]], context, expr)?;
                            if ignore_nulls && matches!(candidate, SqlValue::Null) {
                                continue;
                            }
                            seen += 1;
                            if seen == nth {
                                value = candidate;
                                break;
                            }
                        }
                        value
                    } else {
                        SqlValue::Null
                    }
                }
                "count" | "sum" | "avg" | "min" | "max" | "bool_and" | "bool_or" | "every" => {
                    let frame = self
                        .window_frame_range(spec, rows, context, partition, order_keys, position)?;
                    if let Some((frame_start, frame_end)) = frame {
                        let mut frame_rows = partition[frame_start..=frame_end]
                            .iter()
                            .map(|index| rows[*index].clone())
                            .collect::<Vec<_>>();
                        if let Some(filter) = &function.filter {
                            frame_rows =
                                self.filter_bound_row_predicate(frame_rows, columns, filter)?;
                        }
                        aggregate
                            .as_ref()
                            .expect("aggregate window has aggregate driver")
                            .evaluate(self, &frame_rows, columns)?
                    } else if name == "count" {
                        SqlValue::Int(0)
                    } else {
                        SqlValue::Null
                    }
                }
                _ => unreachable!("validated window function"),
            };
            result[row_index] = value;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn window_frame_range(
        &self,
        spec: &WindowSpec,
        rows: &[SlotRow],
        context: &BoundRowContext,
        partition: &[usize],
        order_keys: &[Vec<SqlValue>],
        position: usize,
    ) -> Result<Option<(usize, usize)>> {
        let Some(frame) = &spec.window_frame else {
            if spec.order_by.is_empty() {
                return Ok((!partition.is_empty()).then_some((0, partition.len() - 1)));
            }
            let mut peer_end = position;
            while peer_end + 1 < partition.len()
                && window_keys_are_peers(
                    &order_keys[partition[position]],
                    &order_keys[partition[peer_end + 1]],
                )
            {
                peer_end += 1;
            }
            return Ok(Some((0, peer_end)));
        };
        let end_bound = frame
            .end_bound
            .as_ref()
            .unwrap_or(&WindowFrameBound::CurrentRow);
        match frame.units {
            WindowFrameUnits::Rows => {
                let start = self.window_rows_bound_position(
                    &frame.start_bound,
                    true,
                    rows,
                    context,
                    partition,
                    position,
                )?;
                let end = self.window_rows_bound_position(
                    end_bound, false, rows, context, partition, position,
                )?;
                window_clamped_frame(start, end, partition.len())
            }
            WindowFrameUnits::Groups => self.window_groups_frame_range(
                &frame.start_bound,
                end_bound,
                rows,
                context,
                partition,
                order_keys,
                position,
            ),
            WindowFrameUnits::Range => self.window_range_frame_range(
                spec,
                &frame.start_bound,
                end_bound,
                rows,
                context,
                partition,
                order_keys,
                position,
            ),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn window_rows_bound_position(
        &self,
        bound: &WindowFrameBound,
        is_start: bool,
        rows: &[SlotRow],
        context: &BoundRowContext,
        partition: &[usize],
        position: usize,
    ) -> Result<i64> {
        let offset = |expr: &Expr| -> Result<i64> {
            let value = self.eval_slot_row_value(&rows[partition[position]], context, expr)?;
            let value = sql_value_i64(&value).ok_or_else(|| {
                SqlError::InvalidSql("window frame offset must be an integer".to_string())
            })?;
            if value < 0 {
                return Err(SqlError::InvalidSql(
                    "window frame offset must not be negative".to_string(),
                ));
            }
            Ok(value)
        };
        match bound {
            WindowFrameBound::CurrentRow => Ok(position as i64),
            WindowFrameBound::Preceding(None) if is_start => Ok(0),
            WindowFrameBound::Following(None) if !is_start => Ok(partition.len() as i64 - 1),
            WindowFrameBound::Preceding(None) => Err(SqlError::InvalidSql(
                "window frame end cannot be UNBOUNDED PRECEDING".to_string(),
            )),
            WindowFrameBound::Following(None) => Err(SqlError::InvalidSql(
                "window frame start cannot be UNBOUNDED FOLLOWING".to_string(),
            )),
            WindowFrameBound::Preceding(Some(offset_expr)) => {
                Ok(position as i64 - offset(offset_expr)?)
            }
            WindowFrameBound::Following(Some(offset_expr)) => {
                Ok(position as i64 + offset(offset_expr)?)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn window_groups_frame_range(
        &self,
        start_bound: &WindowFrameBound,
        end_bound: &WindowFrameBound,
        rows: &[SlotRow],
        context: &BoundRowContext,
        partition: &[usize],
        order_keys: &[Vec<SqlValue>],
        position: usize,
    ) -> Result<Option<(usize, usize)>> {
        let groups = window_peer_groups(partition, order_keys);
        let current_group = groups
            .iter()
            .position(|(start, end)| (*start..=*end).contains(&position))
            .expect("partition position belongs to a peer group");
        let bound_group = |bound: &WindowFrameBound, is_start: bool| -> Result<i64> {
            let offset = |expr: &Expr| -> Result<i64> {
                self.window_nonnegative_integer_offset(expr, &rows[partition[position]], context)
            };
            match bound {
                WindowFrameBound::CurrentRow => Ok(current_group as i64),
                WindowFrameBound::Preceding(None) if is_start => Ok(0),
                WindowFrameBound::Following(None) if !is_start => Ok(groups.len() as i64 - 1),
                WindowFrameBound::Preceding(None) => Err(SqlError::InvalidSql(
                    "window frame end cannot be UNBOUNDED PRECEDING".to_string(),
                )),
                WindowFrameBound::Following(None) => Err(SqlError::InvalidSql(
                    "window frame start cannot be UNBOUNDED FOLLOWING".to_string(),
                )),
                WindowFrameBound::Preceding(Some(expr)) => Ok(current_group as i64 - offset(expr)?),
                WindowFrameBound::Following(Some(expr)) => Ok(current_group as i64 + offset(expr)?),
            }
        };
        let start_group = bound_group(start_bound, true)?.clamp(0, groups.len() as i64);
        let end_group = bound_group(end_bound, false)?.clamp(-1, groups.len() as i64 - 1);
        if start_group > end_group {
            return Ok(None);
        }
        Ok(Some((
            groups[start_group as usize].0,
            groups[end_group as usize].1,
        )))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn window_range_frame_range(
        &self,
        spec: &WindowSpec,
        start_bound: &WindowFrameBound,
        end_bound: &WindowFrameBound,
        rows: &[SlotRow],
        context: &BoundRowContext,
        partition: &[usize],
        order_keys: &[Vec<SqlValue>],
        position: usize,
    ) -> Result<Option<(usize, usize)>> {
        let bound_position = |bound: &WindowFrameBound, is_start: bool| -> Result<i64> {
            match bound {
                WindowFrameBound::CurrentRow => Ok(if is_start {
                    window_peer_start(partition, order_keys, position) as i64
                } else {
                    window_peer_end(partition, order_keys, position) as i64
                }),
                WindowFrameBound::Preceding(None) if is_start => Ok(0),
                WindowFrameBound::Following(None) if !is_start => Ok(partition.len() as i64 - 1),
                WindowFrameBound::Preceding(None) => Err(SqlError::InvalidSql(
                    "window frame end cannot be UNBOUNDED PRECEDING".to_string(),
                )),
                WindowFrameBound::Following(None) => Err(SqlError::InvalidSql(
                    "window frame start cannot be UNBOUNDED FOLLOWING".to_string(),
                )),
                WindowFrameBound::Preceding(Some(expr)) => self.window_range_offset_position(
                    spec, expr, true, is_start, rows, context, partition, order_keys, position,
                ),
                WindowFrameBound::Following(Some(expr)) => self.window_range_offset_position(
                    spec, expr, false, is_start, rows, context, partition, order_keys, position,
                ),
            }
        };
        let start = bound_position(start_bound, true)?;
        let end = bound_position(end_bound, false)?;
        window_clamped_frame(start, end, partition.len())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn window_range_offset_position(
        &self,
        spec: &WindowSpec,
        offset_expr: &Expr,
        preceding: bool,
        is_start: bool,
        rows: &[SlotRow],
        context: &BoundRowContext,
        partition: &[usize],
        order_keys: &[Vec<SqlValue>],
        position: usize,
    ) -> Result<i64> {
        let [order] = spec.order_by.as_slice() else {
            return Err(SqlError::InvalidSql(
                "RANGE with an offset requires exactly one ORDER BY expression".to_string(),
            ));
        };
        let offset_value =
            self.eval_slot_row_value(&rows[partition[position]], context, offset_expr)?;
        let numeric_offset = offset_value.as_f64();
        let interval_offset = match &offset_value {
            SqlValue::String(value) => parse_interval_seconds(value),
            _ => None,
        };
        if numeric_offset.is_none() && interval_offset.is_none() {
            return Err(SqlError::InvalidSql(
                "RANGE frame offset must be numeric or an interval".to_string(),
            ));
        }
        if numeric_offset.is_some_and(|offset| offset < 0.0)
            || interval_offset.is_some_and(|offset| offset < 0)
        {
            return Err(SqlError::InvalidSql(
                "window frame offset must not be negative".to_string(),
            ));
        }
        let current = &order_keys[partition[position]][0];
        if matches!(current, SqlValue::Null) {
            return Ok(if is_start {
                window_peer_start(partition, order_keys, position) as i64
            } else {
                window_peer_end(partition, order_keys, position) as i64
            });
        }
        let ascending = order.options.asc != Some(false);
        let operator = match (preceding, ascending) {
            (true, true) | (false, false) => BinaryOperator::Minus,
            (false, true) | (true, false) => BinaryOperator::Plus,
        };
        if interval_offset.is_some()
            && !matches!(current, SqlValue::String(value) if parse_temporal_seconds(value).is_some())
        {
            return Err(SqlError::InvalidSql(
                "an interval RANGE offset requires a date or timestamp ORDER BY expression"
                    .to_string(),
            ));
        }
        if numeric_offset.is_some() && current.as_f64().is_none() {
            return Err(SqlError::InvalidSql(
                "a numeric RANGE offset requires a numeric ORDER BY expression".to_string(),
            ));
        }
        let target = eval_binary_value(current.clone(), &operator, offset_value)?;
        if is_start {
            Ok(partition
                .iter()
                .position(|row_index| {
                    let key = &order_keys[*row_index][0];
                    !matches!(key, SqlValue::Null)
                        && typed_order_expr_value_ordering(
                            &order.expr,
                            key,
                            None,
                            &target,
                            None,
                            &order.options,
                        ) != Ordering::Less
                })
                .unwrap_or(partition.len()) as i64)
        } else {
            Ok(partition
                .iter()
                .rposition(|row_index| {
                    let key = &order_keys[*row_index][0];
                    !matches!(key, SqlValue::Null)
                        && typed_order_expr_value_ordering(
                            &order.expr,
                            key,
                            None,
                            &target,
                            None,
                            &order.options,
                        ) != Ordering::Greater
                })
                .map(|position| position as i64)
                .unwrap_or(-1))
        }
    }

    pub(crate) fn window_nonnegative_integer_offset(
        &self,
        expr: &Expr,
        row: &SlotRow,
        context: &BoundRowContext,
    ) -> Result<i64> {
        let value = self.eval_slot_row_value(row, context, expr)?;
        let value = sql_value_i64(&value).ok_or_else(|| {
            SqlError::InvalidSql("window frame offset must be an integer".to_string())
        })?;
        if value < 0 {
            return Err(SqlError::InvalidSql(
                "window frame offset must not be negative".to_string(),
            ));
        }
        Ok(value)
    }

    pub(crate) fn filter_bound_row_predicate(
        &self,
        rows: Vec<SlotRow>,
        columns: &[String],
        expr: &Expr,
    ) -> Result<Vec<SlotRow>> {
        let (scope, context) = self.bound_row_context(columns);
        let Some(bound) = scope.bind(expr) else {
            let mut filtered = Vec::new();
            for (idx, row) in rows.into_iter().enumerate() {
                if idx % 1024 == 0 {
                    self.check_cancellation()?;
                }
                if self.eval_slot_row_predicate(&row, &context, expr)? {
                    filtered.push(row);
                }
            }
            return Ok(filtered);
        };

        let mut filtered = Vec::new();
        for (idx, row) in rows.into_iter().enumerate() {
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
        Ok(filtered)
    }

    pub(crate) fn bound_row_context(
        &self,
        columns: &[String],
    ) -> (BoundExprScope, BoundRowContext) {
        // Inside a compiled routine the frame's slots are the binding: no
        // value snapshot, no name map rebuild — unless the string-keyed map
        // carries names the slot layout does not (dynamic `set` fallbacks),
        // which only the snapshot form can resolve.
        let slots = self
            .routine_slots
            .as_ref()
            .filter(|slots| self.routine_vars.len() <= slots.ids.len());
        {
            let cache = self.bound_context_cache.borrow();
            if let Some(entry) = cache.get(columns) {
                let values = match (&entry.vars, slots) {
                    (BoundContextVars::Slots(cached), Some(slots)) => cached
                        .upgrade()
                        .filter(|cached| Rc::ptr_eq(cached, &slots.values)),
                    (BoundContextVars::Snapshot { binding, values }, None) => binding
                        .upgrade()
                        .filter(|binding| Arc::ptr_eq(binding, &self.routine_vars))
                        .map(|_| Rc::clone(values)),
                    _ => None,
                };
                if let Some(var_values) = values {
                    return (
                        entry.scope.clone(),
                        BoundRowContext {
                            row_lookup: entry.row_lookup.clone(),
                            var_values,
                        },
                    );
                }
            }
        }
        let cols = cached_column_lookups(columns);
        let (exact_vars, var_values, vars) = match slots {
            Some(slots) => (
                Arc::clone(&slots.ids),
                Rc::clone(&slots.values),
                BoundContextVars::Slots(Rc::downgrade(&slots.values)),
            ),
            None => {
                #[cfg(test)]
                SQL_ROUTINE_VAR_SNAPSHOTS.with(|snapshots| *snapshots.borrow_mut() += 1);
                let values = Rc::new(self.routine_vars.values().cloned().collect::<Vec<_>>());
                (
                    self.cached_routine_var_ids(),
                    Rc::clone(&values),
                    BoundContextVars::Snapshot {
                        binding: Arc::downgrade(&self.routine_vars),
                        values,
                    },
                )
            }
        };
        let scope = BoundExprScope {
            cols: cols.clone(),
            exact_vars,
            array_vars: Arc::new(FxHashSet::default()),
        };
        let row_lookup = SlotRowLookup { cols };
        let context = BoundRowContext {
            row_lookup: row_lookup.clone(),
            var_values,
        };
        let mut cache = self.bound_context_cache.borrow_mut();
        let entry = BoundContextEntry {
            scope: scope.clone(),
            row_lookup,
            vars,
        };
        match cache.get_mut(columns) {
            Some(slot) => *slot = entry,
            None => {
                cache.insert(columns.to_vec(), entry);
            }
        }
        (scope, context)
    }

    /// Routine-variable name->id map for the current binding. Cached by the
    /// `Arc` identity of `routine_vars` so the repeated `bound_row_context`
    /// calls within one statement (and across statements that keep the same
    /// variable binding) avoid re-normalizing every variable name.
    pub(crate) fn cached_routine_var_ids(&self) -> Arc<FxHashMap<String, VarId>> {
        {
            // A `Weak` key validates identity without keeping the variable map
            // alive, so `materialized_values`/`Arc::make_mut` on the frame is not
            // forced to deep-clone the map just because the cache references it.
            let cache = self.bound_var_cache.borrow();
            if let Some((cached_vars, cached)) = cache.as_ref() {
                if let Some(strong) = cached_vars.upgrade() {
                    if Arc::ptr_eq(&strong, &self.routine_vars) {
                        return cached.clone();
                    }
                }
            }
        }
        let mut exact_vars =
            FxHashMap::with_capacity_and_hasher(self.routine_vars.len(), Default::default());
        for (idx, name) in self.routine_vars.keys().enumerate() {
            exact_vars.insert(normalize_object_name(name), VarId(idx));
        }
        let exact_vars = Arc::new(exact_vars);
        *self.bound_var_cache.borrow_mut() =
            Some((Arc::downgrade(&self.routine_vars), exact_vars.clone()));
        exact_vars
    }

    pub(crate) fn apply_group_order_by(
        &self,
        groups: &mut [Vec<SlotRow>],
        order_by: Option<&OrderBy>,
        columns: &[String],
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
        self.check_cancellation()?;
        let (scope, context) = self.bound_row_context(columns);
        let bounds = expressions
            .iter()
            .map(|order| scope.bind(&order.expr))
            .collect::<Vec<_>>();
        let order_types = expressions
            .iter()
            .map(|order| {
                row_columns_expr_pg_type(
                    self.db_ref(),
                    &order.expr,
                    columns.iter().map(String::as_str),
                )
                .or_else(|| projected_expr_pg_type_with_db(self.db_ref(), &order.expr))
            })
            .collect::<Vec<_>>();
        groups.sort_by(|left, right| {
            expressions
                .iter()
                .zip(&bounds)
                .zip(&order_types)
                .map(|((order, bound), pg_type)| {
                    let eval = |group: &[SlotRow]| {
                        group
                            .first()
                            .and_then(|row| match bound {
                                Some(bound) => bound
                                    .eval(&BoundExprFrame {
                                        user_calls: &[],
                                        db: self.db_ref(),
                                        columns: BoundExprColumns::Values(row),
                                        vars: &context.var_values,
                                    })
                                    .ok(),
                                None => self.eval_slot_row_value(row, &context, &order.expr).ok(),
                            })
                            .unwrap_or(SqlValue::Null)
                    };
                    let left = eval(left);
                    let right = eval(right);
                    let left_key = pg_type.as_deref().and_then(|pg_type| {
                        pg_typed_index_key_for_db(self.db_ref(), pg_type, &left).ok()
                    });
                    let right_key = pg_type.as_deref().and_then(|pg_type| {
                        pg_typed_index_key_for_db(self.db_ref(), pg_type, &right).ok()
                    });
                    typed_order_expr_value_ordering(
                        &order.expr,
                        &left,
                        left_key.as_deref(),
                        &right,
                        right_key.as_deref(),
                        &order.options,
                    )
                })
                .find(|ordering| *ordering != Ordering::Equal)
                .unwrap_or(Ordering::Equal)
        });
        self.check_cancellation()?;
        Ok(())
    }
}
