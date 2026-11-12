use crate::*;

impl SqlSession<'_> {
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
            for record in engine.scan_raw_records(&physical.name)? {
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
