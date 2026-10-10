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
    schema: TableSchema,
    grants: Vec<PrivilegeGrant>,
    triggers: Vec<TriggerSchema>,
    sequences: Vec<SequenceSchema>,
    /// (bin-scoped index name, original index name)
    indexes: Vec<(String, String)>,
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
    entries.sort_by_key(|entry| std::cmp::Reverse(entry.dropped_at_micros));
    Ok(entries)
}

fn save_recycle_bin_entry(db: &mut BicDb, entry: &RecycleBinEntry) -> Result<()> {
    db.create_collection(RECYCLE_BIN_COLLECTION)?;
    let metadata = serde_json::to_value(entry)
        .map_err(|error| SqlError::InvalidSql(format!("recycle bin entry: {error}")))?;
    db.insert(
        RECYCLE_BIN_COLLECTION,
        Record::new(entry.bin_name.clone()).with_metadata(metadata),
    )?;
    Ok(())
}

fn delete_recycle_bin_entry(db: &mut BicDb, bin_name: &str) -> Result<()> {
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
    fn recycle_bin_enabled(&self) -> bool {
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
    ) -> Result<bool> {
        if !self.recycle_bin_enabled()
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

        match self.db_mut()?.rename_collection(table, &bin_name) {
            Ok(()) => {}
            Err(BicDbError::Unsupported(_)) => return Ok(false),
            Err(error) => return Err(error.into()),
        }
        let mut indexes = Vec::with_capacity(index_names.len());
        for (position, original) in index_names.into_iter().enumerate() {
            let scoped = format!("{bin_name}_i{position}");
            // An index the storage layer cannot rename keeps its name.
            let renamed = self
                .db_mut()?
                .rename_index(&original, &scoped)
                .unwrap_or(false);
            indexes.push((if renamed { scoped } else { original.clone() }, original));
        }
        let db = self.db_mut()?;
        delete_schema(db, table)?;
        for grant in &grants {
            delete_privilege(db, grant)?;
        }
        for trigger in &triggers {
            delete_trigger(db, &trigger.name, Some(table))?;
        }
        for sequence in &sequences {
            delete_sequence(db, &sequence.name)?;
        }
        let entry = RecycleBinEntry {
            bin_name: bin_name.clone(),
            table: table.to_string(),
            owner: schema
                .owner
                .clone()
                .unwrap_or_else(|| BOOTSTRAP_ROLE_NAME.to_string()),
            dropped_at_micros: unix_micros_now(),
            schema: schema.clone(),
            grants,
            triggers,
            sequences,
            indexes,
        };
        save_recycle_bin_entry(db, &entry)?;
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
        self.restore_from_recycle_bin(&entry, &target)?;
        if self.tx.is_some() {
            self.ddl_undo.push(DdlUndo::ReturnToRecycleBin {
                entry: Box::new(entry),
                target,
            });
        }
        Ok(())
    }

    pub(crate) fn restore_from_recycle_bin(
        &mut self,
        entry: &RecycleBinEntry,
        target: &str,
    ) -> Result<()> {
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

        self.db_mut()?.rename_collection(&entry.bin_name, target)?;
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
                    while index_name_taken(self.db_ref(), &candidate) {
                        suffix += 1;
                        candidate = format!("{base}{suffix}");
                    }
                    name = candidate;
                }
                self.db_mut()?.rename_index(scoped, &name)?;
            }
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
                    if let ConstraintSchema::Unique { name: unique, .. } = constraint {
                        if unique == original {
                            *unique = name.clone();
                        }
                    }
                }
            }
        }
        let db = self.db_mut()?;
        save_schema(db, &schema)?;
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
        delete_recycle_bin_entry(db, &entry.bin_name)?;
        sql_index_definitions_cache_clear();
        Ok(())
    }

    fn purge_recycle_bin_entry(&mut self, entry: &RecycleBinEntry) -> Result<()> {
        let db = self.db_mut()?;
        db.drop_collection(&entry.bin_name)?;
        delete_recycle_bin_entry(db, &entry.bin_name)?;
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
