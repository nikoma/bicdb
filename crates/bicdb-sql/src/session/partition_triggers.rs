//! Partition trigger lineage and atomic schema/trigger catalog publication.
use crate::*;

#[derive(Clone, Debug)]
pub(crate) struct CatalogUndo {
    collection: &'static str,
    id: String,
    previous: Option<Record>,
}

type Change = (&'static str, String, Option<Record>);

fn trigger_record(trigger: &TriggerSchema) -> Result<Record> {
    Ok(Record::new(trigger_key(&trigger.table_name, &trigger.name))
        .with_metadata(serde_json::to_value(trigger)?))
}

fn follows(child: &TriggerSchema, parent: &TriggerSchema) -> bool {
    child.partition_parent.as_ref().is_some_and(|link| {
        link.table_name == parent.table_name && link.trigger_name == parent.name
    })
}

fn clone_for_partition(parent: &TriggerSchema, child: &TableSchema) -> Result<TriggerSchema> {
    let mut clone = parent.clone();
    clone.partition_parent = Some(PartitionTriggerParent {
        table_name: parent.table_name.clone(),
        trigger_name: parent.name.clone(),
    });
    clone.table_name = child.name.clone();
    let on = find_top_level_keyword(&parent.definition, "ON").ok_or_else(|| {
        SqlError::InvalidSql("stored trigger definition has no target relation".into())
    })?;
    let (_, after) =
        parse_leading_qualified_sql_identifier(parent.definition[on + 2..].trim_start())?;
    let logical = logical_relation_name(&child.name).unwrap_or_else(|| child.name.clone());
    let target = format!(
        "\"{}\".\"{}\"",
        child.schema_name.replace('"', "\"\""),
        logical.replace('"', "\"\"")
    );
    clone.definition = format!(
        "{} {} {}",
        &parent.definition[..on + 2],
        target,
        after.trim_start()
    );
    Ok(clone)
}

fn trigger_descendants(all: &[TriggerSchema], root: &TriggerSchema) -> Result<Vec<TriggerSchema>> {
    let mut out = vec![root.clone()];
    let mut seen = BTreeSet::from([trigger_key(&root.table_name, &root.name)]);
    let mut index = 0;
    while index < out.len() {
        let parent = out[index].clone();
        for child in all.iter().filter(|child| follows(child, &parent)) {
            if !seen.insert(trigger_key(&child.table_name, &child.name)) {
                return Err(SqlError::InvalidSql(
                    "cyclic or ambiguous partition trigger lineage".into(),
                ));
            }
            out.push(child.clone());
        }
        index += 1;
    }
    Ok(out)
}

impl SqlSession<'_> {
    /// All changes, including a partition link and its trigger clones, become
    /// visible in one Native transaction. Prepare/cache-decode before commit;
    /// no fallible derived catalog write follows publication.
    fn publish_partition_catalog(
        &mut self,
        changes: Vec<Change>,
        capture_undo: bool,
    ) -> Result<()> {
        if changes.is_empty() {
            return Ok(());
        }
        let mut schema_cache = Vec::new();
        for (collection, id, record) in &changes {
            match *collection {
                SCHEMA_COLLECTION => {
                    let schema = record
                        .as_ref()
                        .map(|record| {
                            serde_json::from_value::<TableSchema>(record.metadata.clone())
                        })
                        .transpose()?;
                    if schema.as_ref().is_some_and(|schema| schema.name != *id) {
                        return Err(SqlError::InvalidSql(
                            "partition schema catalog identity mismatch".into(),
                        ));
                    }
                    schema_cache.push((id.clone(), schema));
                }
                TRIGGER_COLLECTION => {
                    if let Some(record) = record {
                        let trigger: TriggerSchema =
                            serde_json::from_value(record.metadata.clone())?;
                        if trigger_key(&trigger.table_name, &trigger.name) != *id {
                            return Err(SqlError::InvalidSql(
                                "partition trigger catalog identity mismatch".into(),
                            ));
                        }
                    }
                }
                _ => {
                    return Err(SqlError::InvalidSql(
                        "invalid partition catalog target".into(),
                    ))
                }
            }
        }
        // Match ordinary DDL's exclusive-session boundary and Native catalog
        // authorization; do not publish from a shared SQL read latch.
        self.db_mut()?;
        if changes
            .iter()
            .any(|(collection, _, _)| *collection == TRIGGER_COLLECTION)
        {
            ensure_trigger_collection(self.db_mut()?)?;
        }
        if capture_undo && self.tx.is_some() {
            let entries = changes
                .iter()
                .map(|(collection, id, _)| {
                    let previous = match self.db_ref().get(collection, id) {
                        Ok(record) => record.map(|record| (*record).clone()),
                        Err(BicDbError::CollectionNotFound(_)) => None,
                        Err(error) => return Err(SqlError::from(error)),
                    };
                    Ok(CatalogUndo {
                        collection: *collection,
                        id: id.clone(),
                        previous,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            self.ddl_undo
                .push(DdlUndo::RestorePartitionCatalog { entries });
        }
        let mut tx = self.db_ref().begin_transaction()?;
        for (collection, id, record) in changes {
            match record {
                Some(record) => tx.batch_insert(collection, vec![record])?,
                None => {
                    tx.delete(collection, &id)?;
                }
            }
        }
        tx.commit()?;
        if !schema_cache.is_empty() {
            sql_schema_list_cache_invalidate(self.db_ref());
        }
        for (id, schema) in schema_cache {
            sql_schema_cache_set(self.db_ref(), &id, schema.map(Arc::new));
        }
        Ok(())
    }

    pub(crate) fn restore_partition_catalog(&mut self, entries: Vec<CatalogUndo>) -> Result<()> {
        self.publish_partition_catalog(
            entries
                .into_iter()
                .map(|entry| (entry.collection, entry.id, entry.previous))
                .collect(),
            false,
        )
    }

    fn plan_trigger_subtree(
        &self,
        root: TriggerSchema,
        table: &TableSchema,
        replace: bool,
        changes: &mut Vec<Change>,
    ) -> Result<()> {
        let key = trigger_key(&root.table_name, &root.name);
        if changes
            .iter()
            .any(|(collection, id, _)| *collection == TRIGGER_COLLECTION && *id == key)
        {
            return Err(SqlError::InvalidSql(
                "cyclic or aliased partition trigger plan".into(),
            ));
        }
        if let Some(existing) = load_trigger(self.db_ref(), &root.table_name, &root.name)? {
            if !replace {
                return Err(SqlError::data_exception(
                    "42710",
                    format!(
                        "trigger \"{}\" already exists for relation \"{}\"",
                        root.name, root.table_name
                    ),
                    None,
                ));
            }
            if existing.table_name != root.table_name || existing.name != root.name {
                return Err(SqlError::InvalidSql(
                    "trigger catalog key aliases a different authored identity".into(),
                ));
            }
        }
        changes.push((
            TRIGGER_COLLECTION,
            trigger_key(&root.table_name, &root.name),
            Some(trigger_record(&root)?),
        ));
        if table.partitioning.is_some() {
            for child in partition::children(self.db_ref(), table)? {
                let clone = clone_for_partition(&root, &child)?;
                self.plan_trigger_subtree(clone, &child, replace, changes)?;
            }
        }
        Ok(())
    }

    pub(crate) fn create_partition_trigger_tree(
        &mut self,
        trigger: TriggerSchema,
        replace: bool,
    ) -> Result<()> {
        let table = load_schema(self.db_ref(), &trigger.table_name)?
            .ok_or_else(|| SqlError::InvalidCollection(trigger.table_name.clone()))?;
        let mut changes = Vec::new();
        self.plan_trigger_subtree(trigger, &table, replace, &mut changes)?;
        self.publish_partition_catalog(changes, true)
    }

    pub(crate) fn validate_partition_trigger_attachment(
        &self,
        schema: &TableSchema,
        parent: &TableSchema,
    ) -> Result<()> {
        let mut changes = Vec::new();
        for trigger in list_triggers(self.db_ref())?
            .into_iter()
            .filter(|trigger| trigger.table_name == parent.name && trigger.for_each == "row")
        {
            self.plan_trigger_subtree(
                clone_for_partition(&trigger, schema)?,
                schema,
                false,
                &mut changes,
            )?;
        }
        Ok(())
    }

    pub(crate) fn has_partition_trigger_descendants(
        &self,
        trigger: &TriggerSchema,
    ) -> Result<bool> {
        Ok(list_triggers(self.db_ref())?
            .iter()
            .any(|child| follows(child, trigger)))
    }

    pub(crate) fn save_partition_trigger_mode_tree(
        &mut self,
        trigger: &TriggerSchema,
    ) -> Result<()> {
        let mut tree = trigger_descendants(&list_triggers(self.db_ref())?, trigger)?;
        let mut changes = Vec::new();
        for item in &mut tree {
            item.set_enabled_mode(trigger.enabled_mode);
            changes.push((
                TRIGGER_COLLECTION,
                trigger_key(&item.table_name, &item.name),
                Some(trigger_record(item)?),
            ));
        }
        self.publish_partition_catalog(changes, true)
    }

    pub(crate) fn drop_partition_trigger_tree(&mut self, trigger: &TriggerSchema) -> Result<bool> {
        let changes = trigger_descendants(&list_triggers(self.db_ref())?, trigger)?
            .into_iter()
            .map(|item| {
                (
                    TRIGGER_COLLECTION,
                    trigger_key(&item.table_name, &item.name),
                    None,
                )
            })
            .collect();
        self.publish_partition_catalog(changes, true)?;
        Ok(true)
    }

    /// Attach/create clones or detach only the inherited lineage. Local
    /// triggers and their own descendant clones retain their authored identity.
    pub(crate) fn save_partition_schema_and_triggers(
        &mut self,
        schema: &TableSchema,
        parent: Option<&TableSchema>,
    ) -> Result<()> {
        let mut changes = Vec::new();
        match parent {
            Some(parent) => {
                for trigger in list_triggers(self.db_ref())?.into_iter().filter(|trigger| {
                    trigger.table_name == parent.name && trigger.for_each == "row"
                }) {
                    let clone = clone_for_partition(&trigger, schema)?;
                    self.plan_trigger_subtree(clone, schema, false, &mut changes)?;
                }
            }
            None => {
                let all = list_triggers(self.db_ref())?;
                for root in all.iter().filter(|trigger| {
                    trigger.table_name == schema.name && trigger.partition_parent.is_some()
                }) {
                    for trigger in trigger_descendants(&all, root)? {
                        changes.push((
                            TRIGGER_COLLECTION,
                            trigger_key(&trigger.table_name, &trigger.name),
                            None,
                        ));
                    }
                }
            }
        }
        let record = prepare_schema_record(self.db_mut()?, schema)?;
        changes.push((SCHEMA_COLLECTION, schema.name.clone(), Some(record)));
        self.publish_partition_catalog(changes, true)
    }
}
