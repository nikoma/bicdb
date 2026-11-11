//! Recycle bin: `DROP TABLE` keeps the table restorable.
//!
//! * `DROP TABLE t` moves `t` into the recycle bin (unless `PURGE` is given,
//!   the bin is disabled with `SET bicdb.recyclebin = off`, or the table
//!   cannot be moved; those drops are permanent as before).
//! * `FLASHBACK TABLE t TO BEFORE DROP [RENAME TO u]` restores the most
//!   recently dropped `t` (or a specific bin entry named by its bin name).
//! * `PURGE TABLE t`, `PURGE RECYCLEBIN` and `SHOW RECYCLEBIN` manage it.
//!
//! Moving a table is catalog-only: its collection is renamed to a bin name
//! (rows, indexes, policy and flashback history stay where they are), its
//! indexes take bin-scoped names, and its schema, grants, triggers and owned
//! sequences move into a bin record so the original name is free at once.
//! Restoring reverses each step. Inside a transaction block a `ROLLBACK`
//! restores the table.
use super::*;

pub const RECYCLE_BIN_COLLECTION: &str = "__bicdb_recyclebin";
const BIN_NAME_PREFIX: &str = "__bicdb_bin_";
const RECYCLE_BIN_GUC: &str = "bicdb.recyclebin";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct RecycleBinEntry {
    pub(crate) bin_name: String,
    /// Physical relation name the table had when dropped.
    pub(crate) table: String,
    pub(crate) owner: String,
    pub(crate) dropped_at_micros: i64,
    pub(super) schema: TableSchema,
    pub(super) grants: Vec<PrivilegeGrant>,
    pub(super) triggers: Vec<TriggerSchema>,
    pub(super) sequences: Vec<SequenceSchema>,
    /// (bin-scoped index name, original index name)
    pub(super) indexes: Vec<(String, String)>,
}

impl RecycleBinEntry {
    /// The table's name as users wrote it (`schema.table` outside `public`).
    pub(crate) fn display_name(&self) -> String {
        logical_relation_name(&self.table).unwrap_or_else(|| self.table.clone())
    }
}

/// True for the collection name of a table held in the recycle bin.
pub(crate) fn is_recycle_bin_name(name: &str) -> bool {
    name.starts_with(BIN_NAME_PREFIX)
}

/// Roll back the unacknowledged collection-rename part of a DROP that never
/// published its recycle-bin entry or removed the original SQL schema.
/// Core WAL/catalog recovery must run first. Completed drops are excluded by
/// their durable bin entry; ambiguous states are preserved for diagnosis.
pub fn recover_interrupted_recycle_bin_renames(db: &mut BicDb) -> Result<usize> {
    db.check_recovery_state()?;
    let result = recover_interrupted_recycle_bin_renames_inner(db);
    // A concurrent/late fence wins even over an empty result or a structural
    // ambiguity diagnostic: startup must not continue with an unsafe store.
    db.check_recovery_state()?;
    result
}

/// Finish only durably decided purges while the host owns exclusive catalog
/// access. Unlike startup recovery, this never reverses a live move intent.
/// Native committers call it after their transaction is durable and released.
/// Also reclaims one bounded batch of decided rename images; live intents are
/// retained. The return value remains the number of completed recycle purges.
pub fn finish_committed_recycle_bin_purges(db: &mut BicDb) -> Result<usize> {
    db.check_recovery_state()?;
    let result = super::recycle_bin_journal::recover_purges(db);
    db.check_recovery_state()?;
    if result.is_ok() {
        super::rename_journal::collect_committed_images_batch(db, 128)?;
    }
    result
}

fn recover_interrupted_recycle_bin_renames_inner(db: &mut BicDb) -> Result<usize> {
    db.check_catalog_recovery_quiescent()?;
    let journal_repairs = super::recycle_bin_journal::recover(db)?;
    let completed = list_recycle_bin(db)?
        .into_iter()
        .map(|entry| entry.bin_name)
        .collect::<BTreeSet<_>>();
    let collections = db.collections();
    let schemas = list_schemas(db)?;
    let indexes = db.index_definitions();
    let mut repairs = Vec::new();
    for bin in collections
        .iter()
        .filter(|meta| is_recycle_bin_name(&meta.name))
    {
        if completed.contains(&bin.name)
            || uuid::Uuid::parse_str(&bin.name[BIN_NAME_PREFIX.len()..]).is_err()
        {
            continue;
        }
        let candidates = schemas
            .iter()
            .filter(|schema| {
                schema.name != bin.name
                    && (bin.renamed_from.as_deref() == Some(schema.name.as_str())
                        || bin.storage() == schema.name
                        || primary_key_index_definition_for_schema(schema).is_some_and(
                            |mut expected| {
                                expected.collection = bin.name.clone();
                                indexes.iter().any(|index| {
                                    index.name == expected.name
                                        && unique_constraint_index_definition_matches(
                                            index, &expected,
                                        )
                                })
                            },
                        ))
            })
            .collect::<Vec<_>>();
        let [schema] = candidates.as_slice() else {
            if candidates.is_empty() {
                continue;
            }
            return Err(SqlError::InvalidSql(format!(
                "ambiguous interrupted recycle-bin rename {}: multiple SQL schemas match",
                bin.name
            )));
        };
        if collections.iter().any(|meta| meta.name == schema.name) {
            return Err(SqlError::InvalidSql(format!(
                "ambiguous interrupted recycle-bin rename {}: original collection {} also exists",
                bin.name, schema.name
            )));
        }
        // Later SQL-stage index renames need their durable mapping. Never
        // infer one or remove another object's index merely to permit open.
        if indexes.iter().any(|index| {
            index.collection == bin.name && index.name.starts_with(&format!("{}_i", bin.name))
        }) {
            return Err(SqlError::InvalidSql(format!(
                "interrupted recycle-bin rename {} has unrecorded index-name changes",
                bin.name
            )));
        }
        repairs.push((bin.name.clone(), schema.name.clone()));
    }
    for (bin, original) in &repairs {
        db.rename_collection(bin, original)?;
    }
    if !repairs.is_empty() {
        sql_index_definitions_cache_clear();
    }
    Ok(journal_repairs + repairs.len())
}

fn unix_micros_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_micros() as i64)
        .unwrap_or_default()
}

pub(crate) fn list_recycle_bin(db: &BicDb) -> Result<Vec<RecycleBinEntry>> {
    let records = match db.scan_collection(RECYCLE_BIN_COLLECTION) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut entries = records
        .into_iter()
        .map(|record| serde_json::from_value::<RecycleBinEntry>(record.metadata))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| SqlError::InvalidSql(format!("corrupt recycle bin entry: {error}")))?;
    let mut visible = Vec::new();
    for entry in entries.drain(..) {
        if !super::recycle_bin_journal::purge_is_decided(db, &entry.bin_name)? {
            visible.push(entry);
        }
    }
    visible.sort_by_key(|entry| std::cmp::Reverse(entry.dropped_at_micros));
    Ok(visible)
}

pub(super) fn save_recycle_bin_entry(db: &mut BicDb, entry: &RecycleBinEntry) -> Result<()> {
    db.create_collection(RECYCLE_BIN_COLLECTION)?;
    let metadata = serde_json::to_value(entry)
        .map_err(|error| SqlError::InvalidSql(format!("recycle bin entry: {error}")))?;
    db.insert(
        RECYCLE_BIN_COLLECTION,
        Record::new(entry.bin_name.clone()).with_metadata(metadata),
    )?;
    Ok(())
}

pub(super) fn delete_recycle_bin_entry(db: &mut BicDb, bin_name: &str) -> Result<()> {
    let mut tx = db.begin_transaction()?;
    tx.delete(RECYCLE_BIN_COLLECTION, bin_name)?;
    tx.commit()?;
    Ok(())
}

fn index_name_taken(db: &BicDb, name: &str) -> bool {
    db.index_definitions()
        .iter()
        .any(|index| index.name.eq_ignore_ascii_case(name))
}

impl<'db> SqlSession<'db> {
    pub(super) fn rename_recycle_collection(&mut self, old: &str, new: &str) -> Result<()> {
        if let Some(mut tx) = self.tx.take() {
            let result = self.db_mut().and_then(|db| {
                db.rename_collection_preserving_pending_writes(&mut tx, old, new)
                    .map_err(SqlError::from)
            });
            self.tx = Some(tx);
            result
        } else {
            self.db_mut()?
                .rename_collection(old, new)
                .map_err(SqlError::from)
        }
    }

    pub(super) fn recycle_bin_enabled(&self) -> bool {
        let setting = self
            .session_gucs
            .get(RECYCLE_BIN_GUC)
            .cloned()
            .or_else(|| std::env::var("BICDB_RECYCLEBIN").ok());
        !matches!(
            setting.as_deref().map(str::to_ascii_lowercase).as_deref(),
            Some("off" | "false" | "0" | "no")
        )
    }

    /// Moves `table` into the recycle bin. `Ok(false)` means the table cannot
    /// be kept (bin disabled, partitioned, or its storage cannot be renamed)
    /// and the caller drops it permanently.
    pub(crate) fn move_table_to_recycle_bin(
        &mut self,
        table: &str,
        schema: &TableSchema,
        deferred_purge: bool,
    ) -> Result<bool> {
        if (!deferred_purge && !self.recycle_bin_enabled())
            || schema.partitioning.is_some()
            || schema.partition_of.is_some()
            || table.starts_with("__bicdb_")
        {
            return Ok(false);
        }
        let bin_name = format!("{BIN_NAME_PREFIX}{}", uuid::Uuid::new_v4().simple());
        let grants = list_privileges(self.db_ref())?
            .into_iter()
            .filter(|grant| {
                grant.object_type == PrivilegeObjectType::Table && grant.object_name == table
            })
            .collect::<Vec<_>>();
        let triggers = list_triggers(self.db_ref())?
            .into_iter()
            .filter(|trigger| trigger.table_name == table)
            .collect::<Vec<_>>();
        let sequences = list_sequences(self.db_ref())?
            .into_iter()
            .filter(|sequence| sequence.owned_by_table.as_deref() == Some(table))
            .collect::<Vec<_>>();
        let index_names = self
            .db_ref()
            .index_definitions()
            .into_iter()
            .filter(|index| index.collection == table)
            .map(|index| index.name)
            .collect::<Vec<_>>();

        let entry = RecycleBinEntry {
            bin_name: bin_name.clone(),
            table: table.to_string(),
            owner: schema
                .owner
                .clone()
                .unwrap_or_else(|| BOOTSTRAP_ROLE_NAME.to_string()),
            dropped_at_micros: unix_micros_now(),
            schema: schema.clone(),
            grants: grants.clone(),
            triggers: triggers.clone(),
            sequences: sequences.clone(),
            indexes: index_names
                .iter()
                .enumerate()
                .map(|(position, original)| (format!("{bin_name}_i{position}"), original.clone()))
                .collect(),
        };
        let intent_sequence = super::recycle_bin_journal::prepare(self.db_mut()?, &entry)?;
        super::recycle_bin_journal::stage("intent");
        let renamed = self.rename_recycle_collection(table, &bin_name);
        match renamed {
            Ok(()) => {}
            Err(SqlError::BicDb(BicDbError::Unsupported(_))) => {
                super::recycle_bin_journal::clear(self.db_mut()?, &bin_name)?;
                return Ok(false);
            }
            Err(error) => return Err(error),
        }
        super::recycle_bin_journal::stage("collection");
        let mut indexes = Vec::with_capacity(index_names.len());
        for (position, original) in index_names.into_iter().enumerate() {
            let scoped = format!("{bin_name}_i{position}");
            // An index the storage layer cannot rename keeps its name.
            let renamed = match self.db_mut()?.rename_index(&original, &scoped) {
                Ok(true) => true,
                Err(BicDbError::Unsupported(_)) => false,
                Ok(false) => {
                    return Err(SqlError::InvalidSql(format!(
                        "index {original} disappeared during recycle-bin move"
                    )))
                }
                Err(error) => return Err(error.into()),
            };
            indexes.push((if renamed { scoped } else { original.clone() }, original));
            super::recycle_bin_journal::stage("index");
        }
        let db = self.db_mut()?;
        delete_schema(db, table)?;
        super::recycle_bin_journal::stage("schema");
        for grant in &grants {
            delete_privilege(db, grant)?;
        }
        for trigger in &triggers {
            delete_trigger(db, &trigger.name, Some(table))?;
        }
        for sequence in &sequences {
            delete_sequence(db, &sequence.name)?;
        }
        super::recycle_bin_journal::stage("dependents");
        let mut entry = entry;
        entry.indexes = indexes;
        save_recycle_bin_entry(db, &entry)?;
        super::recycle_bin_journal::stage("entry");
        super::recycle_bin_journal::decide(self, &bin_name, intent_sequence)?;
        if deferred_purge {
            super::recycle_bin_journal::schedule_purge(self, &entry)?;
        }
        super::recycle_bin_journal::stage("decision");
        sql_index_definitions_cache_clear();
        if self.tx.is_some() {
            self.ddl_undo.push(DdlUndo::RestoreFromRecycleBin {
                bin_name,
                target: table.to_string(),
            });
        }
        Ok(true)
    }

    fn find_recycle_bin_entry(&self, name: &ObjectName) -> Result<RecycleBinEntry> {
        let entries = list_recycle_bin(self.db_ref())?;
        let written = object_name(name)?;
        if let Some(entry) = entries
            .iter()
            .find(|entry| entry.bin_name.eq_ignore_ascii_case(&written))
        {
            return Ok(entry.clone());
        }
        let table = relation_name(name)?;
        entries
            .into_iter()
            .find(|entry| entry.table == table)
            .ok_or_else(|| {
                SqlError::InvalidSql(format!("object \"{written}\" is not in the recycle bin"))
            })
    }

    fn require_recycle_bin_owner(&self, entry: &RecycleBinEntry, action: &str) -> Result<()> {
        if self.current_user_is_superuser()? {
            return Ok(());
        }
        let role = rls_check_user_from_gucs(&self.session_gucs);
        if role == entry.owner {
            return Ok(());
        }
        Err(SqlError::BicDb(BicDbError::Authorization(format!(
            "must be owner of table {} to {action}",
            entry.display_name()
        ))))
    }

    /// `FLASHBACK TABLE name TO BEFORE DROP [RENAME TO target]`.
    pub(crate) fn execute_flashback_before_drop(
        &mut self,
        name: &ObjectName,
        rename_to: Option<&ObjectName>,
    ) -> Result<()> {
        let entry = self.find_recycle_bin_entry(name)?;
        self.require_recycle_bin_owner(&entry, "restore it")?;
        let target = match rename_to {
            Some(target) => relation_name(target)?,
            None => entry.table.clone(),
        };
        let intent_id = self.restore_from_recycle_bin(&entry, &target)?;
        if self.tx.is_some() {
            self.ddl_undo
                .push(DdlUndo::ReturnToRecycleBin { intent_id });
        }
        Ok(())
    }

    pub(crate) fn restore_from_recycle_bin(
        &mut self,
        entry: &RecycleBinEntry,
        target: &str,
    ) -> Result<String> {
        Ok(self
            .restore_from_recycle_bin_impl(entry, target, false)?
            .expect("authored restore has an intent"))
    }
    pub(crate) fn restore_from_recycle_bin_undo(
        &mut self,
        entry: &RecycleBinEntry,
        target: &str,
    ) -> Result<()> {
        self.restore_from_recycle_bin_impl(entry, target, true)
            .map(|_| ())
    }
    fn restore_from_recycle_bin_impl(
        &mut self,
        entry: &RecycleBinEntry,
        target: &str,
        undo: bool,
    ) -> Result<Option<String>> {
        require_user_relation_name(target)?;
        // Check every conflict before changing anything.
        if load_schema(self.db_ref(), target)?.is_some()
            || self
                .db_ref()
                .collections()
                .iter()
                .any(|collection| collection.name == target)
        {
            return Err(SqlError::InvalidSql(format!(
                "cannot restore {}: relation \"{}\" already exists; use RENAME TO",
                entry.display_name(),
                logical_relation_name(target).unwrap_or_else(|| target.to_string())
            )));
        }
        // Owned sequences whose names were reused while the table was in the
        // bin (a re-created SERIAL column does exactly that) take a derived
        // name; the column defaults below follow it.
        let mut sequences = Vec::with_capacity(entry.sequences.len());
        for sequence in &entry.sequences {
            let mut restored = sequence.clone();
            if load_sequence(self.db_ref(), &sequence.name)?.is_some() {
                let base = format!(
                    "{}_{}",
                    target.rsplit('.').next().unwrap_or(target),
                    sequence.name
                );
                let mut candidate = base.clone();
                let mut suffix = 1;
                while load_sequence(self.db_ref(), &candidate)?.is_some()
                    || sequences
                        .iter()
                        .any(|taken: &SequenceSchema| taken.name == candidate)
                {
                    suffix += 1;
                    candidate = format!("{base}{suffix}");
                }
                restored.name = candidate;
            }
            restored.owned_by_table = Some(target.to_string());
            sequences.push(restored);
        }

        let mut schema = entry.schema.clone();
        schema.name = target.to_string();
        for (original, restored) in entry.sequences.iter().zip(&sequences) {
            if original.name == restored.name {
                continue;
            }
            let quoted_old = format!("'{}'", original.name);
            let quoted_new = format!("'{}'", restored.name);
            for column in &mut schema.columns {
                if column.default_sequence.as_deref() == Some(original.name.as_str()) {
                    column.default_sequence = Some(restored.name.clone());
                    if let Some(expr) = column.default_expr.as_mut() {
                        *expr = expr.replace(&quoted_old, &quoted_new);
                    }
                }
            }
        }
        let mut index_renames = Vec::new();
        let mut reserved = self
            .db_ref()
            .index_definitions()
            .into_iter()
            .map(|index| index.name)
            .collect::<BTreeSet<_>>();
        for (scoped, original) in &entry.indexes {
            let mut name = original.clone();
            if scoped != original {
                if index_name_taken(self.db_ref(), original) {
                    // The original name was reused while the table was in the
                    // bin: take a derived name, and rename the matching SQL
                    // index and constraint with it.
                    let base = format!(
                        "{}_{original}",
                        schema.name.rsplit('.').next().unwrap_or(target)
                    );
                    let mut candidate = base.clone();
                    let mut suffix = 1;
                    while reserved.contains(&candidate) {
                        suffix += 1;
                        candidate = format!("{base}{suffix}");
                    }
                    name = candidate;
                }
                reserved.insert(name.clone());
            }
            index_renames.push((scoped.clone(), name.clone()));
            if &name != original {
                for index in &mut schema.indexes {
                    if index.name == *original {
                        index.name = name.clone();
                    }
                    for internal in &mut index.internal_index_names {
                        if internal == original {
                            *internal = name.clone();
                        }
                    }
                }
                for constraint in &mut schema.constraints {
                    if let ConstraintSchema::Unique { name: unique, .. }
                    | ConstraintSchema::UniqueWithNullPolicy { name: unique, .. } = constraint
                    {
                        if unique == original {
                            *unique = name.clone();
                        }
                    }
                }
            }
        }
        let journal = if undo {
            None
        } else {
            let id = format!("restore_{}", uuid::Uuid::new_v4().simple());
            let plan = super::recycle_bin_journal::RestorePlan {
                id: id.clone(),
                target: target.to_string(),
                schema: schema.clone(),
                sequences: sequences.clone(),
                indexes: index_renames.clone(),
            };
            let sequence =
                super::recycle_bin_journal::prepare_restore(self.db_mut()?, entry, plan)?;
            super::recycle_bin_journal::stage("restore_intent");
            Some((id, sequence))
        };
        self.rename_recycle_collection(&entry.bin_name, target)?;
        super::recycle_bin_journal::stage("restore_collection");
        for (from, to) in &index_renames {
            if from != to && !self.db_mut()?.rename_index(from, to)? {
                return Err(SqlError::InvalidSql(format!(
                    "restore index {from} disappeared"
                )));
            }
            super::recycle_bin_journal::stage("restore_index");
        }
        let db = self.db_mut()?;
        save_schema(db, &schema)?;
        super::recycle_bin_journal::stage("restore_schema");
        for grant in &entry.grants {
            let mut grant = grant.clone();
            grant.object_name = target.to_string();
            save_privilege(db, &grant)?;
        }
        for trigger in &entry.triggers {
            let mut trigger = trigger.clone();
            trigger.table_name = target.to_string();
            save_trigger(db, &trigger)?;
        }
        for sequence in &sequences {
            save_sequence(db, sequence)?;
        }
        super::recycle_bin_journal::stage("restore_dependents");
        delete_recycle_bin_entry(db, &entry.bin_name)?;
        super::recycle_bin_journal::stage("restore_entry");
        if let Some((id, sequence)) = &journal {
            super::recycle_bin_journal::decide(self, id, *sequence)?;
            super::recycle_bin_journal::stage("restore_decision");
        }
        if undo {
            // The caller already rolled back the outer transaction/mark and
            // released its pending intent-delete lock before DDL restoration.
            super::recycle_bin_journal::clear_if_present(self.db_mut()?, &entry.bin_name)?;
        }
        sql_index_definitions_cache_clear();
        Ok(journal.map(|(id, _)| id))
    }

    fn purge_recycle_bin_entry(&mut self, entry: &RecycleBinEntry) -> Result<()> {
        let db = self.db_mut()?;
        super::recycle_bin_journal::prepare_purge(db, entry)?;
        super::recycle_bin_journal::stage("purge_intent");
        super::recycle_bin_journal::finish_purge(db, entry)?;
        sql_index_definitions_cache_clear();
        Ok(())
    }

    /// `PURGE TABLE name`: permanently removes one bin entry.
    pub(crate) fn execute_purge_table(&mut self, name: &ObjectName) -> Result<()> {
        if self.in_transaction() {
            return Err(SqlError::Unsupported(
                "PURGE cannot run inside a transaction block".to_string(),
            ));
        }
        let entry = self.find_recycle_bin_entry(name)?;
        self.require_recycle_bin_owner(&entry, "purge it")?;
        self.purge_recycle_bin_entry(&entry)
    }

    /// `PURGE RECYCLEBIN`: the caller's entries (every entry for a superuser).
    pub(crate) fn execute_purge_recycle_bin(&mut self) -> Result<usize> {
        if self.in_transaction() {
            return Err(SqlError::Unsupported(
                "PURGE cannot run inside a transaction block".to_string(),
            ));
        }
        let superuser = self.current_user_is_superuser()?;
        let role = rls_check_user_from_gucs(&self.session_gucs);
        let entries = list_recycle_bin(self.db_ref())?
            .into_iter()
            .filter(|entry| superuser || entry.owner == role)
            .collect::<Vec<_>>();
        for entry in &entries {
            self.purge_recycle_bin_entry(entry)?;
        }
        Ok(entries.len())
    }

    /// Purges every entry dropped before `cutoff_micros` (retention).
    pub fn purge_recycle_bin_older_than(&mut self, cutoff_micros: i64) -> Result<usize> {
        if self.in_transaction() {
            return Err(SqlError::Unsupported(
                "PURGE cannot run inside a transaction block".into(),
            ));
        }
        let entries = list_recycle_bin(self.db_ref())?
            .into_iter()
            .filter(|entry| entry.dropped_at_micros < cutoff_micros)
            .collect::<Vec<_>>();
        for entry in &entries {
            self.purge_recycle_bin_entry(entry)?;
        }
        Ok(entries.len())
    }

    /// `SHOW RECYCLEBIN`: the caller's entries (every entry for a superuser).
    pub(crate) fn execute_show_recycle_bin(&self) -> Result<SqlResult> {
        let superuser = self.current_user_is_superuser()?;
        let role = rls_check_user_from_gucs(&self.session_gucs);
        let rows = list_recycle_bin(self.db_ref())?
            .into_iter()
            .filter(|entry| superuser || entry.owner == role)
            .map(|entry| {
                vec![
                    SqlValue::String(entry.display_name()),
                    SqlValue::String(entry.bin_name.clone()),
                    SqlValue::String(crate::engine::flashback::timestamp_text_from_unix_micros(
                        entry.dropped_at_micros,
                    )),
                    SqlValue::String(entry.owner.clone()),
                ]
            })
            .collect::<Vec<_>>();
        let mut result = SqlResult::empty(vec![
            "original_name".to_string(),
            "recyclebin_name".to_string(),
            "dropped_at".to_string(),
            "owner".to_string(),
        ]);
        result.rows = rows;
        result.column_types = vec![
            Some("text".to_string()),
            Some("text".to_string()),
            Some("timestamptz".to_string()),
            Some("text".to_string()),
        ];
        Ok(result)
    }
}
