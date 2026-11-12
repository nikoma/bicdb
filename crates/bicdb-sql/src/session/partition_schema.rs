//! Publish a partition schema transition and its physical rows together.
use crate::*;

#[derive(Clone, Debug)]
pub(crate) struct TableState {
    schema: Record,
    records: Vec<Record>,
}

impl SqlSession<'_> {
    pub(super) fn publish_partition_type_change(
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
