//! Publish a partition schema transition and its physical rows together.
use crate::*;

#[derive(Clone, Debug)]
pub(crate) struct TableState {
    schema: Record,
    records: Vec<Record>,
}

impl SqlSession<'_> {
    pub(super) fn alter_partition_add_column(
        &mut self,
        schema: &mut TableSchema,
        definition: &ColumnDef,
        if_not_exists: bool,
    ) -> Result<bool> {
        if serial_type(&definition.data_type).is_some() || identity_options(definition)?.is_some() {
            return Ok(false);
        }
        let mut column = column_schema_from_def(self.db_ref(), definition)?;
        self.ensure_user_type_usage(column.user_type.as_ref())?;
        if schema.column(&column.name).is_some() {
            if if_not_exists {
                return Ok(true);
            }
            return Err(SqlError::InvalidSql(format!(
                "column \"{}\" of relation \"{}\" already exists",
                column.name, schema.name
            )));
        }
        column.default_value = column_default_value(definition)?;
        let constraints = column_constraints_from_def(
            &schema.name,
            definition,
            &column,
            oracle::default_unique_null_policy(self.db_ref())?,
        )?;
        self.require_reference_privilege(&schema.name, &constraints)?;
        let mut parent = schema.clone();
        parent.add_column(column.clone());
        parent.constraints.extend(constraints.clone());
        partition::validate_definition(&parent)?;
        for constraint in &constraints {
            validate_constraint_columns(self.db_ref(), &parent.name, &parent, constraint)?;
        }
        let mut changes = vec![(schema.clone(), parent.clone())];
        let candidates = list_schemas_raw(self.db_ref())?;
        let mut cursor = 0;
        while cursor < changes.len() {
            let parent_name = changes[cursor].0.name.clone();
            for child in candidates.iter().filter(|child| {
                child
                    .partition_of
                    .as_ref()
                    .is_some_and(|link| link.parent_table == parent_name)
            }) {
                let mut updated = child.clone();
                updated.add_column(column.clone());
                updated.constraints.extend(constraints.clone());
                changes.push((child.clone(), updated));
            }
            cursor += 1;
        }
        let mut planned = Vec::with_capacity(changes.len());
        for (previous, updated) in changes {
            let mut records = self.db_ref().scan_collection(&previous.name)?;
            for record in &mut records {
                let value = if let Some(expression) = &column.generated_expr {
                    let expression = Self::parse_default_expr(expression)?;
                    self.eval_target_record_expr(
                        &updated.name,
                        &updated.name,
                        &updated,
                        record,
                        &expression,
                    )?
                    .ok_or_else(|| {
                        SqlError::Unsupported("unsupported generated column expression".into())
                    })?
                } else {
                    self.default_value_for_column(Some(&updated), &column.name)?
                        .unwrap_or(SqlValue::Null)
                };
                let value = cast_value_to_column_type(value, &column)?;
                set_record_column(record, Some(&updated), &column.name, value)?;
            }
            self.validate_planned_alter_record_constraints(&updated, &records)?;
            planned.push((previous, updated, records));
        }
        self.validate_planned_alter_foreign_keys(&planned)?;
        self.publish_partition_schema_change(&planned)?;
        *schema = parent;
        Ok(true)
    }

    pub(super) fn publish_partition_schema_change(
        &mut self,
        planned: &[(TableSchema, TableSchema, Vec<Record>)],
    ) -> Result<()> {
        // Prepare every descriptor while the old, certified tree is intact.
        // Preparing the next child after publishing its parent would observe
        // an incompatible half-transition and correctly refuse admission.
        let mut states = Vec::with_capacity(planned.len());
        for (_, schema, records) in planned {
            states.push(TableState {
                schema: prepare_schema_record(self.db_mut()?, schema)?,
                records: records.clone(),
            });
        }
        self.replace_partition_table_states(states, true)
    }

    pub(super) fn restore_partition_table_states(&mut self, states: Vec<TableState>) -> Result<()> {
        self.replace_partition_table_states(states, false)
    }

    fn replace_partition_table_states(
        &mut self,
        states: Vec<TableState>,
        capture_undo: bool,
    ) -> Result<()> {
        self.db_mut()?;
        let mut schemas = Vec::with_capacity(states.len());
        let mut changes = Vec::with_capacity(states.len());
        let mut previous = Vec::with_capacity(states.len());
        let mut names = BTreeSet::new();
        for state in &states {
            let schema: TableSchema = serde_json::from_value(state.schema.metadata.clone())?;
            if schema.name != state.schema.id
                || !names.insert(schema.name.clone())
                || (schema.partitioning.is_none() && schema.partition_of.is_none())
            {
                return Err(SqlError::InvalidSql(
                    "invalid partition table transition identity".into(),
                ));
            }
            let raw = self
                .db_ref()
                .get(SCHEMA_COLLECTION, &schema.name)?
                .ok_or_else(|| SqlError::InvalidCollection(schema.name.clone()))?;
            previous.push(TableState {
                schema: (*raw).clone(),
                records: self.db_ref().scan_collection(&schema.name)?,
            });
            changes.push((
                SCHEMA_COLLECTION,
                schema.name.clone(),
                Some(state.schema.clone()),
            ));
            schemas.push(schema);
        }
        let certificate =
            super::partition_admission::certificate_for_catalog_changes(self.db_ref(), &changes)?;
        let mut tx = self.db_ref().begin_transaction()?;
        for (state, old) in states.iter().zip(&previous) {
            let ids = state
                .records
                .iter()
                .map(|record| record.id.as_str())
                .collect::<BTreeSet<_>>();
            if ids.len() != state.records.len() {
                return Err(SqlError::InvalidSql(
                    "partition table transition contains duplicate physical identities".into(),
                ));
            }
            for record in &old.records {
                if !ids.contains(record.id.as_str()) {
                    tx.delete(&state.schema.id, &record.id)?;
                }
            }
            if !state.records.is_empty() {
                tx.batch_insert(&state.schema.id, state.records.clone())?;
            }
            tx.batch_insert(SCHEMA_COLLECTION, vec![state.schema.clone()])?;
        }
        if let Some(certificate) = certificate {
            tx.batch_insert(
                super::partition_admission::CERTIFICATE_COLLECTION,
                vec![certificate],
            )?;
        }
        // Keep one undo entry for the entire forest. Rolling back children one
        // at a time would expose the same incompatible intermediate catalog.
        if capture_undo && self.tx.is_some() {
            self.ddl_undo
                .push(DdlUndo::RestorePartitionTableStates { states: previous });
        }
        tx.commit()?;
        sql_schema_list_cache_invalidate(self.db_ref());
        for schema in schemas {
            cache_saved_schema(self.db_ref(), &schema);
        }
        Ok(())
    }
}
