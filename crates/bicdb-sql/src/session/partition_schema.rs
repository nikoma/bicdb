//! Publish a partition schema transition and its physical rows together.
use crate::*;

#[derive(Clone, Debug)]
pub(crate) struct TableState {
    schema: Record,
    records: Vec<Record>,
}

#[derive(Clone, Debug)]
pub(crate) struct AuxiliaryState {
    collection: &'static str,
    id: String,
    record: Option<Record>,
}

impl SqlSession<'_> {
    pub(super) fn alter_partition_column_default(
        &mut self,
        schema: &mut TableSchema,
        column: &str,
        operation: &AlterColumnOperation,
    ) -> Result<()> {
        let mut parent = schema.clone();
        alter_column_default(self.db_ref(), &mut parent, column, operation)?;
        let candidates = list_schemas_raw(self.db_ref())?;
        let mut schemas = vec![parent.clone()];
        let mut cursor = 0;
        while cursor < schemas.len() {
            let name = schemas[cursor].name.clone();
            for child in candidates.iter().filter(|child| {
                child
                    .partition_of
                    .as_ref()
                    .is_some_and(|link| link.parent_table == name)
            }) {
                let mut child = child.clone();
                alter_column_default(self.db_ref(), &mut child, column, operation)?;
                schemas.push(child);
            }
            cursor += 1;
        }
        let mut changes = Vec::with_capacity(schemas.len());
        for schema in schemas {
            let record = prepare_schema_record(self.db_mut()?, &schema)?;
            changes.push((SCHEMA_COLLECTION, schema.name, Some(record)));
        }
        self.publish_partition_catalog(changes, true)?;
        *schema = parent;
        Ok(())
    }

    pub(super) fn alter_partition_add_column(
        &mut self,
        schema: &mut TableSchema,
        definition: &ColumnDef,
        if_not_exists: bool,
    ) -> Result<bool> {
        let serial = serial_type(&definition.data_type);
        let identity = identity_options(definition)?;
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
        let mut pending_sequence = if serial.is_some() || identity.is_some() {
            if identity.is_some() && !matches!(column.pg_type.as_str(), "int2" | "int4" | "int8") {
                return Err(SqlError::invalid_parameter_value(
                    "identity column type must be smallint, integer, or bigint",
                ));
            }
            let name = sequence_name_for_column(&schema.name, &column.name);
            require_user_relation_name(&name)?;
            if load_sequence(self.db_ref(), &name)?.is_some()
                || relation_exists(self.db_ref(), &name)
            {
                return Err(SqlError::InvalidSql(format!(
                    "relation \"{name}\" already exists"
                )));
            }
            require_unused_relation_oid(self.db_ref(), sequence_oid(&name))?;
            let mut sequence = if let Some(data_type) = serial {
                SequenceSchema::new_typed(name.clone(), data_type, 1)
            } else {
                sequence_from_options(
                    name.clone(),
                    &column.pg_type,
                    identity.as_ref().expect("identity checked").1,
                )?
            };
            sequence.owner = current_user_from_gucs(&self.session_gucs);
            sequence.owned_by_table = Some(schema.name.clone());
            sequence.owned_by_column = Some(column.name.clone());
            column.nullable = false;
            column.default_sequence = Some(name);
            column.default_value = None;
            column.default_expr = None;
            column.identity = identity.map(|(kind, _)| kind);
            Some(sequence)
        } else {
            None
        };
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
                let value = if let Some(sequence) = &mut pending_sequence {
                    // A newly owned generator is private until publication.
                    // Validation failure must not leave a sequence or consume
                    // values in an independently committed catalog write.
                    SqlValue::Int(advance_sequence(sequence)?)
                } else if let Some(expression) = &column.generated_expr {
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
        let mut auxiliary = Vec::new();
        if let Some(sequence) = &pending_sequence {
            auxiliary.push(AuxiliaryState {
                collection: SEQUENCE_COLLECTION,
                id: sequence.name.clone(),
                record: Some(
                    Record::new(sequence.name.clone())
                        .with_metadata(serde_json::to_value(sequence)?),
                ),
            });
            let mut grants = BTreeMap::new();
            for grant in self.default_privileges_for_object(
                &sequence.owner,
                &schema.schema_name,
                PrivilegeObjectType::Sequence,
                &sequence.name,
            )? {
                grants.insert(privilege_key(&grant), grant);
            }
            for (id, grant) in grants {
                auxiliary.push(AuxiliaryState {
                    collection: PRIVILEGE_COLLECTION,
                    id: id.clone(),
                    record: Some(Record::new(id).with_metadata(serde_json::to_value(grant)?)),
                });
            }
        }
        self.publish_partition_schema_change_with_auxiliary(&planned, auxiliary)?;
        if let Some(sequence) = pending_sequence.filter(|sequence| sequence.is_called) {
            self.currvals.insert(sequence.name, sequence.last_value);
            Arc::make_mut(&mut self.session_gucs).insert(
                LASTVAL_SESSION_KEY.to_string(),
                sequence.last_value.to_string(),
            );
        }
        *schema = parent;
        Ok(true)
    }

    pub(super) fn publish_partition_schema_change(
        &mut self,
        planned: &[(TableSchema, TableSchema, Vec<Record>)],
    ) -> Result<()> {
        self.publish_partition_schema_change_with_auxiliary(planned, Vec::new())
    }

    fn publish_partition_schema_change_with_auxiliary(
        &mut self,
        planned: &[(TableSchema, TableSchema, Vec<Record>)],
        auxiliary: Vec<AuxiliaryState>,
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
        self.replace_partition_table_states(states, auxiliary, true)
    }

    pub(super) fn restore_partition_table_states(
        &mut self,
        states: Vec<TableState>,
        auxiliary: Vec<AuxiliaryState>,
    ) -> Result<()> {
        self.replace_partition_table_states(states, auxiliary, false)
    }

    fn replace_partition_table_states(
        &mut self,
        states: Vec<TableState>,
        auxiliary: Vec<AuxiliaryState>,
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
        let mut previous_auxiliary = Vec::with_capacity(auxiliary.len());
        let mut auxiliary_ids = BTreeSet::new();
        for entry in &auxiliary {
            if !auxiliary_ids.insert((entry.collection, entry.id.as_str()))
                || entry
                    .record
                    .as_ref()
                    .is_some_and(|record| record.id != entry.id)
            {
                return Err(SqlError::InvalidSql(
                    "invalid partition auxiliary catalog identity".into(),
                ));
            }
            match entry.collection {
                SEQUENCE_COLLECTION => {
                    if let Some(record) = &entry.record {
                        let sequence: SequenceSchema =
                            serde_json::from_value(record.metadata.clone())?;
                        if sequence.name != entry.id {
                            return Err(SqlError::InvalidSql(
                                "partition sequence catalog identity mismatch".into(),
                            ));
                        }
                        validate_sequence_schema(&sequence)?;
                    }
                    ensure_sequence_collection(self.db_mut()?)?;
                }
                PRIVILEGE_COLLECTION => {
                    if let Some(record) = &entry.record {
                        let grant: PrivilegeGrant =
                            serde_json::from_value(record.metadata.clone())?;
                        if privilege_key(&grant) != entry.id {
                            return Err(SqlError::InvalidSql(
                                "partition privilege catalog identity mismatch".into(),
                            ));
                        }
                    }
                    ensure_privilege_collection(self.db_mut()?)?;
                }
                _ => {
                    return Err(SqlError::InvalidSql(
                        "invalid partition auxiliary catalog target".into(),
                    ))
                }
            }
            previous_auxiliary.push(AuxiliaryState {
                collection: entry.collection,
                id: entry.id.clone(),
                record: self
                    .db_ref()
                    .get(entry.collection, &entry.id)?
                    .map(|record| (*record).clone()),
            });
        }
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
        for entry in auxiliary {
            if let Some(record) = entry.record {
                tx.batch_insert(entry.collection, vec![record])?;
            } else {
                tx.delete(entry.collection, &entry.id)?;
            }
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
            self.ddl_undo.push(DdlUndo::RestorePartitionTableStates {
                states: previous,
                auxiliary: previous_auxiliary,
            });
        }
        tx.commit()?;
        sql_schema_list_cache_invalidate(self.db_ref());
        for schema in schemas {
            cache_saved_schema(self.db_ref(), &schema);
        }
        Ok(())
    }
}
