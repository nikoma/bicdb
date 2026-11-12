use crate::*;

pub(super) type PartitionRowEvent = (TableSchema, Option<Record>, Option<Record>);

pub(super) enum PartitionUpdateResult {
    Skipped,
    Deleted,
    Updated(Record),
}

impl SqlSession<'_> {
    pub(crate) fn execute_partition_insert(
        &mut self,
        parent: &TableSchema,
        records: Vec<Record>,
        insert: &Insert,
    ) -> Result<SqlResult> {
        let inserted = self.insert_partition_rows(
            parent,
            records,
            super::functions_insert::insert_reads_target(insert),
        )?;
        self.insert_result(
            &parent.name,
            Some(parent),
            insert.returning.as_deref(),
            &inserted,
            inserted.len(),
        )
    }

    /// Shared INSERT/COPY physical semantics: route first, run the destination
    /// leaf's BEFORE triggers, check the named parent's policies, then stage
    /// each actual leaf write before the next input row's triggers.
    pub(super) fn insert_partition_rows(
        &mut self,
        parent: &TableSchema,
        records: Vec<Record>,
        reads_target: bool,
    ) -> Result<Vec<Record>> {
        let mut inserted = Vec::with_capacity(records.len());
        for record in records {
            self.cancellation.check()?;
            let (rows, _) = self.before_partition_insert_triggers(parent, vec![record])?;
            for record in rows {
                self.enforce_rls_write_checks(
                    &parent.name,
                    Some(parent),
                    PolicyAction::Insert,
                    reads_target,
                    std::slice::from_ref(&record),
                )?;
                validate_records_for_write(
                    self.db_ref(),
                    self.read_tx(),
                    &parent.name,
                    parent,
                    std::slice::from_ref(&record),
                    false,
                    Some(&self.sql_engine()),
                )?;
                // Each next BEFORE trigger sees the writes of earlier input
                // rows. The outer statement transaction still makes the
                // whole statement atomic if any later row fails.
                self.insert_partition_records(parent, std::slice::from_ref(&record))?;
                inserted.push(record);
            }
        }
        self.publish_extension_database_events(
            &parent.name,
            DatabaseOperation::Insert,
            inserted.iter().map(|record| (None, Some(record))),
        )?;
        self.after_partition_insert_triggers(parent, &inserted)?;
        Ok(inserted)
    }

    /// Routing precedes BEFORE INSERT. A leaf trigger may modify the row but
    /// cannot redirect it into another leaf; PostgreSQL reports 23514 there.
    /// Trigger effects remain in the outer statement transaction.
    pub(crate) fn before_partition_insert_triggers(
        &mut self,
        parent: &TableSchema,
        records: Vec<Record>,
    ) -> Result<(Vec<Record>, bool)> {
        let mut surviving = Vec::with_capacity(records.len());
        let mut triggered = false;
        for mut record in records {
            partition::materialize_primary_key_fields(parent, &mut record);
            let target = partition::route_record(self.db_ref(), parent, &record)?;
            let (rows, fired) = self.apply_before_insert_row_triggers_with_identity(
                &target.name,
                Some(&target),
                vec![record],
                Some(parent),
            )?;
            triggered |= fired;
            for mut row in rows {
                self.materialize_generated_columns(
                    &target.name,
                    &target,
                    std::slice::from_mut(&mut row),
                )?;
                self.refresh_partition_update_identity(&target, &mut row)?;
                partition::validate_record(self.db_ref(), &target, &row)?;
                surviving.push(row);
            }
        }
        Ok((surviving, triggered))
    }

    pub(crate) fn after_partition_insert_triggers(
        &mut self,
        parent: &TableSchema,
        records: &[Record],
    ) -> Result<()> {
        // Retain input row order, rather than grouping events by leaf. This
        // runs after every BEFORE invocation and every physical write.
        for record in records {
            let target = partition::route_record(self.db_ref(), parent, record)?;
            self.fire_after_insert_triggers(
                &target.name,
                Some(&target),
                std::slice::from_ref(record),
            )?;
        }
        Ok(())
    }

    pub(super) fn stage_partition_update_record(
        &mut self,
        parent: &TableSchema,
        source: &str,
        before: &Record,
        mut record: Record,
        snapshot: u64,
        assigned_columns: &[String],
        reads_existing: bool,
        after_events: &mut Vec<PartitionRowEvent>,
    ) -> Result<PartitionUpdateResult> {
        let target = partition::route_record(self.db_ref(), parent, &record)?;
        let source_schema = load_schema(self.db_ref(), source)?.ok_or_else(|| {
            SqlError::InvalidSql(format!("partition source {source} disappeared"))
        })?;
        self.refresh_partition_update_identity(&target, &mut record)?;
        self.enforce_rls_write_checks(
            &parent.name,
            Some(parent),
            PolicyAction::Update,
            reads_existing,
            std::slice::from_ref(&record),
        )?;
        if source != target.name {
            if !self.apply_before_delete_row_triggers(source, Some(&source_schema), before)? {
                return Ok(PartitionUpdateResult::Skipped);
            }
            self.delete_session_records_with_statement_snapshots(
                source,
                std::iter::once((before.id.clone(), snapshot)),
            )?;
            after_events.push((source_schema, Some(before.clone()), None));
            // PostgreSQL deletes the source before invoking destination BEFORE
            // INSERT. A NULL destination result suppresses the insertion; it
            // does not undo that deletion and contributes no UPDATE row.
            let (mut rows, _) = self.apply_before_insert_row_triggers_with_identity(
                &target.name,
                Some(&target),
                vec![record],
                Some(parent),
            )?;
            let Some(next) = rows.pop() else {
                return Ok(PartitionUpdateResult::Deleted);
            };
            record = next;
            self.materialize_generated_columns(
                &target.name,
                &target,
                std::slice::from_mut(&mut record),
            )?;
            self.refresh_partition_update_identity(&target, &mut record)?;
            partition::validate_record(self.db_ref(), &target, &record)?;
            self.enforce_rls_write_checks(
                &parent.name,
                Some(parent),
                PolicyAction::Update,
                reads_existing,
                std::slice::from_ref(&record),
            )?;
            validate_updated_records(
                self.db_ref(),
                self.read_tx(),
                &parent.name,
                parent,
                std::slice::from_ref(&record),
                &[(before.clone(), record.clone())],
                Some(&self.sql_engine()),
                assigned_columns,
            )?;
            if self
                .sql_engine()
                .get_raw_record_cached(&target.name, &record.id)?
                .is_some()
            {
                return Err(unique_violation(
                    &target,
                    &target.primary_key_constraint_name(),
                ));
            }
            validate_records_for_write(
                self.db_ref(),
                self.read_tx(),
                &target.name,
                &target,
                std::slice::from_ref(&record),
                false,
                Some(&self.sql_engine()),
            )?;
            self.insert_session_new_records(&target.name, Some(&target), vec![record.clone()])?;
            after_events.push((target, None, Some(record.clone())));
        } else {
            validate_updated_records(
                self.db_ref(),
                self.read_tx(),
                &parent.name,
                parent,
                std::slice::from_ref(&record),
                &[(before.clone(), record.clone())],
                Some(&self.sql_engine()),
                assigned_columns,
            )?;
            self.update_partition_records(
                parent,
                &[(before.clone(), record.clone())],
                &[Some(source.to_string())],
                &[snapshot],
                assigned_columns,
            )?;
            after_events.push((source_schema, Some(before.clone()), Some(record.clone())));
        }
        Ok(PartitionUpdateResult::Updated(record))
    }

    pub(super) fn fire_partition_after_events(
        &mut self,
        events: Vec<PartitionRowEvent>,
    ) -> Result<()> {
        for (schema, before, after) in events {
            match (before.as_ref(), after.as_ref()) {
                (None, Some(record)) => self.fire_after_insert_triggers(
                    &schema.name,
                    Some(&schema),
                    std::slice::from_ref(record),
                )?,
                (Some(before), after) => {
                    let (op, operation) = if after.is_some() {
                        ("update", DatabaseOperation::Update)
                    } else {
                        ("delete", DatabaseOperation::Delete)
                    };
                    self.publish_extension_database_events(
                        &schema.name,
                        operation,
                        std::iter::once((Some(before), after)),
                    )?;
                    self.fire_after_row_triggers(
                        &schema.name,
                        Some(&schema),
                        op,
                        std::iter::once((Some(before), after)),
                    )?;
                }
                _ => return Err(SqlError::InvalidSql("empty partition trigger event".into())),
            }
        }
        Ok(())
    }

    pub(crate) fn partition_update_candidates(
        &self,
        update: &sqlparser::ast::Update,
        ctes: &BTreeMap<String, CteResult>,
        schema: &TableSchema,
        alias: &str,
        from_rows: Option<&RowSet>,
        target_columns: Arc<Vec<String>>,
        match_policy: UpdateMatchPolicy,
    ) -> Result<Vec<UpdateAssignmentCandidate>> {
        let engine = self.sql_engine_with_ctes(ctes.clone());
        let mut candidates = Vec::new();
        let columns = from_rows
            .map(|from| {
                Arc::new(merge_row_set_columns(
                    target_columns.as_ref().clone(),
                    from.columns.clone(),
                ))
            })
            .unwrap_or_else(|| target_columns.clone());
        for physical in partition::physical_tables(self.db_ref(), schema)? {
            let different_keys =
                primary_key_columns_for_schema(&physical) != primary_key_columns_for_schema(schema);
            for record in engine.scan_raw_records(&physical.name)? {
                let record = if different_keys {
                    let mut record = (*record).clone();
                    partition::materialize_primary_key_fields(&physical, &mut record);
                    Arc::new(record)
                } else {
                    record
                };
                self.cancellation.check()?;
                if !rls_allows_record_with_schema(
                    &engine,
                    &schema.name,
                    PolicyAction::Update,
                    &record,
                    Some(schema),
                )? {
                    continue;
                }
                let target = slot_row_from_record(&schema.name, alias, Some(schema), &record)?;
                let mut consider = |row: SlotRow| -> Result<bool> {
                    let (_scope, context) = engine.bound_row_context(columns.as_ref());
                    if update
                        .selection
                        .as_ref()
                        .map(|selection| engine.eval_slot_row_predicate(&row, &context, selection))
                        .transpose()?
                        .unwrap_or(true)
                    {
                        candidates.push(UpdateAssignmentCandidate {
                            physical_table: Some(physical.name.clone()),
                            record: (*record).clone(),
                            columns: columns.clone(),
                            row,
                        });
                        return Ok(true);
                    }
                    Ok(false)
                };
                if let Some(from) = from_rows {
                    for from_row in &from.rows {
                        let row = materialize_join_row(
                            &target,
                            target_columns.as_ref(),
                            from_row,
                            &from.columns,
                        )?;
                        if consider(row)? && match_policy == UpdateMatchPolicy::First {
                            break;
                        }
                    }
                } else {
                    consider(target)?;
                }
            }
        }
        Ok(candidates)
    }

    pub(crate) fn refresh_partition_update_identity(
        &self,
        schema: &TableSchema,
        record: &mut Record,
    ) -> Result<()> {
        if schema.has_hidden_primary_key() {
            return Ok(());
        }
        let keys = primary_key_columns_for_schema(schema);
        let mut fields = BTreeMap::new();
        for key in keys {
            let value = record_column_value(record, schema, &key);
            if matches!(value, SqlValue::Null) {
                return Err(constraint_violation(
                    "23502",
                    format!("null value in column \"{key}\" violates not-null constraint"),
                    Some(schema.name.clone()),
                    Some(key.clone()),
                    Some(not_null_constraint_name(&schema.name, &key)),
                ));
            }
            fields.insert(key, value);
        }
        if !fields.is_empty() {
            record.id = record_id_from_fields(&schema.name, Some(schema), &fields)?;
        }
        Ok(())
    }

    /// Apply PostgreSQL's immediate constraints in row order inside the one
    /// outer statement transaction. Deleting the old physical row is staged;
    /// any later refusal rolls back the entire statement, including that row.
    pub(crate) fn update_partition_records(
        &mut self,
        schema: &TableSchema,
        pairs: &[(Record, Record)],
        sources: &[Option<String>],
        snapshots: &[u64],
        assigned_columns: &[String],
    ) -> Result<()> {
        if self.tx.is_none() {
            return Err(SqlError::BicDb(BicDbError::TransactionNotPending));
        }
        if pairs.len() != sources.len() || pairs.len() != snapshots.len() {
            return Err(SqlError::InvalidSql(
                "partition update lost physical row provenance".into(),
            ));
        }
        // Refuse an unroutable late row before staging the first mutation.
        let targets = pairs
            .iter()
            .map(|(_, after)| partition::route_record(self.db_ref(), schema, after))
            .collect::<Result<Vec<_>>>()?;
        for ((((before, after), source), snapshot), target) in
            pairs.iter().zip(sources).zip(snapshots).zip(targets)
        {
            let source = source.as_ref().ok_or_else(|| {
                SqlError::InvalidSql("partition update has no physical source".into())
            })?;
            if source == &target.name && before.id == after.id {
                validate_updated_records(
                    self.db_ref(),
                    self.read_tx(),
                    &target.name,
                    &target,
                    std::slice::from_ref(after),
                    std::slice::from_ref(&(before.clone(), after.clone())),
                    Some(&self.sql_engine()),
                    assigned_columns,
                )?;
                self.insert_session_records_with_statement_snapshots(
                    source,
                    std::iter::once((after.clone(), *snapshot)),
                )?;
            } else {
                self.delete_session_records_with_statement_snapshots(
                    source,
                    std::iter::once((before.id.clone(), *snapshot)),
                )?;
                // A routed insert may never upsert an unrelated row that has
                // the same Native ID in the destination heap.
                if self
                    .sql_engine()
                    .get_raw_record_cached(&target.name, &after.id)?
                    .is_some()
                {
                    return Err(unique_violation(
                        &target,
                        &target.primary_key_constraint_name(),
                    ));
                }
                validate_records_for_write(
                    self.db_ref(),
                    self.read_tx(),
                    &target.name,
                    &target,
                    std::slice::from_ref(after),
                    false,
                    Some(&self.sql_engine()),
                )?;
                self.insert_session_new_records(&target.name, Some(&target), vec![after.clone()])?;
            }
        }
        Ok(())
    }

    pub(crate) fn partition_delete_candidates(
        &self,
        delete: &Delete,
        ctes: &BTreeMap<String, CteResult>,
        schema: &TableSchema,
        alias: &str,
        using_rows: Option<&RowSet>,
        target_columns: Arc<Vec<String>>,
    ) -> Result<Vec<DeleteCandidate>> {
        let engine = self.sql_engine_with_ctes(ctes.clone());
        let mut candidates = Vec::new();
        let mut seen = BTreeSet::new();
        let columns = using_rows
            .map(|using| {
                Arc::new(merge_row_set_columns(
                    target_columns.as_ref().clone(),
                    using.columns.clone(),
                ))
            })
            .unwrap_or_else(|| target_columns.clone());
        for physical in partition::physical_tables(self.db_ref(), schema)? {
            let different_keys =
                primary_key_columns_for_schema(&physical) != primary_key_columns_for_schema(schema);
            for record in engine.scan_raw_records(&physical.name)? {
                let record = if different_keys {
                    let mut record = (*record).clone();
                    partition::materialize_primary_key_fields(&physical, &mut record);
                    Arc::new(record)
                } else {
                    record
                };
                self.cancellation.check()?;
                if !rls_allows_record_with_schema(
                    &engine,
                    &schema.name,
                    PolicyAction::Delete,
                    &record,
                    Some(schema),
                )? {
                    continue;
                }
                let target_row = slot_row_from_record(&schema.name, alias, Some(schema), &record)?;
                let predicate_matches = |row: &SlotRow| -> Result<bool> {
                    let (_scope, context) = engine.bound_row_context(columns.as_ref());
                    match delete.selection.as_ref() {
                        Some(selection) => engine.eval_slot_row_predicate(row, &context, selection),
                        None => Ok(true),
                    }
                };
                let matched_row = if let Some(using) = using_rows {
                    let mut found = None;
                    for using_row in &using.rows {
                        let row = materialize_join_row(
                            &target_row,
                            target_columns.as_ref(),
                            using_row,
                            &using.columns,
                        )?;
                        if predicate_matches(&row)? {
                            found = Some(row);
                            break;
                        }
                    }
                    found
                } else if predicate_matches(&target_row)? {
                    Some(target_row)
                } else {
                    None
                };
                if let Some(row) = matched_row {
                    if seen.insert((physical.name.clone(), record.id.clone())) {
                        candidates.push(DeleteCandidate {
                            physical_table: Some(physical.name.clone()),
                            record: (*record).clone(),
                            columns: columns.clone(),
                            row,
                        });
                    }
                }
            }
        }
        Ok(candidates)
    }

    /// Route the complete statement before staging any physical write. The
    /// outer statement transaction owns every leaf write and its rollback.
    pub(crate) fn insert_partition_records(
        &mut self,
        schema: &TableSchema,
        records: &[Record],
    ) -> Result<()> {
        let mut groups: BTreeMap<String, (TableSchema, Vec<Record>)> = BTreeMap::new();
        for record in records {
            let target = partition::route_record(self.db_ref(), schema, record)?;
            groups
                .entry(target.name.clone())
                .or_insert_with(|| (target, Vec::new()))
                .1
                .push(record.clone());
        }
        // Physical unique/foreign-key constraints must be checked against the
        // destination's actual heap, never just the empty parent collection.
        for (target, records) in groups.values() {
            validate_records_for_write(
                self.db_ref(),
                self.read_tx(),
                &target.name,
                target,
                records,
                false,
                Some(&self.sql_engine()),
            )?;
        }
        for (name, (target, records)) in groups {
            self.insert_session_new_records(&name, Some(&target), records)?;
        }
        Ok(())
    }
}
