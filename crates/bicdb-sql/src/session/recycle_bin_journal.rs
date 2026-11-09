//! Durable intent for recycle moves. Decisions are ordinary writes in the
//! caller's transaction, so its row changes and DDL decision share one WAL commit.
use super::recycle_bin::*;
use super::*;
const INTENTS: &str = "__bicdb_recycle_intents";
const DECISIONS: &str = "__bicdb_recycle_decisions";
#[derive(Clone, Serialize, Deserialize)]
struct MoveIntent {
    entry: RecycleBinEntry,
    definitions: Vec<bicdb_core::IndexDefinition>,
}
pub(super) fn prepare(db: &mut BicDb, entry: &RecycleBinEntry) -> Result<()> {
    db.create_collection(INTENTS)?;
    db.create_collection(DECISIONS)?;
    let intent = MoveIntent {
        entry: entry.clone(),
        definitions: db
            .index_definitions()
            .into_iter()
            .filter(|index| index.collection == entry.table)
            .collect(),
    };
    db.insert(
        INTENTS,
        Record::new(entry.bin_name.clone()).with_metadata(serde_json::to_value(intent)?),
    )?;
    Ok(())
}
pub(super) fn decide(session: &mut SqlSession<'_>, bin: &str) -> Result<()> {
    let record = Record::new(bin);
    match session.tx.as_mut() {
        Some(tx) => tx.insert(DECISIONS, record)?,
        None => {
            session.db_mut()?.insert(DECISIONS, record)?;
        }
    }
    Ok(())
}
pub(super) fn clear(db: &mut BicDb, bin: &str) -> Result<()> {
    // Remove intent first: a leftover decision without an intent is inert.
    // Both deletes are one decision, avoiding unbounded abandoned markers.
    let mut tx = db.begin_transaction()?;
    tx.delete(INTENTS, bin)?;
    tx.delete(DECISIONS, bin)?;
    tx.commit()?;
    Ok(())
}
pub(super) fn clear_if_present(db: &mut BicDb, bin: &str) -> Result<()> {
    match db.get(INTENTS, bin) {
        Ok(Some(_)) => clear(db, bin),
        Ok(None) | Err(BicDbError::CollectionNotFound(_)) => Ok(()),
        Err(error) => Err(error.into()),
    }
}
pub(super) fn recover(db: &mut BicDb) -> Result<usize> {
    let records = match db.scan_collection(INTENTS) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => return Ok(0),
        Err(error) => return Err(error.into()),
    };
    let mut repaired = 0;
    for record in records {
        let intent: MoveIntent = serde_json::from_value(record.metadata)?;
        let entry = &intent.entry;
        if record.id != entry.bin_name {
            return Err(SqlError::InvalidSql(
                "recycle intent identity mismatch".into(),
            ));
        }
        let committed = db.get(DECISIONS, &entry.bin_name)?.is_some();
        let collections = db.collections();
        let original = collections.iter().any(|c| c.name == entry.table);
        let bin = collections.iter().any(|c| c.name == entry.bin_name);
        if committed {
            // Every catalog stage was durable before this marker was staged.
            // Inconsistent proof is an error, never a reason to prune data.
            if !bin
                || original
                || load_schema(db, &entry.table)?.is_some()
                || !list_recycle_bin(db)?
                    .iter()
                    .any(|e| e.bin_name == entry.bin_name)
            {
                return Err(SqlError::InvalidSql(format!(
                    "committed recycle intent {} has inconsistent catalog state",
                    entry.bin_name
                )));
            }
            clear(db, &entry.bin_name)?;
            continue;
        }
        if original == bin {
            return Err(SqlError::InvalidSql(format!(
                "ambiguous pending recycle intent {}: expected exactly one backing collection",
                entry.bin_name
            )));
        }
        // Validate all index identities before changing names. An original
        // namespace reused by another object must survive for diagnosis.
        let indexes = db.index_definitions();
        let mut renames = Vec::new();
        for definition in &intent.definitions {
            let scoped = entry
                .indexes
                .iter()
                .find(|(_, name)| name == &definition.name)
                .map(|(name, _)| name)
                .ok_or_else(|| {
                    SqlError::InvalidSql("recycle intent missing index mapping".into())
                })?;
            let matches = indexes
                .iter()
                .filter(|index| index.name == definition.name || index.name == *scoped)
                .collect::<Vec<_>>();
            let [current] = matches.as_slice() else {
                return Err(SqlError::InvalidSql(format!(
                    "ambiguous pending recycle index {}",
                    definition.name
                )));
            };
            let mut expected = definition.clone();
            expected.name = current.name.clone();
            expected.collection = if bin {
                entry.bin_name.clone()
            } else {
                entry.table.clone()
            };
            if **current != expected {
                return Err(SqlError::InvalidSql(format!(
                    "pending recycle index {} has conflicting definition",
                    current.name
                )));
            }
            if current.name != definition.name {
                renames.push((current.name.clone(), definition.name.clone()));
            }
        }
        if bin {
            db.rename_collection(&entry.bin_name, &entry.table)?;
        }
        for (from, to) in renames {
            if !db.rename_index(&from, &to)? {
                return Err(SqlError::InvalidSql(format!(
                    "pending recycle index {from} disappeared"
                )));
            }
        }
        save_schema(db, &entry.schema)?;
        for grant in &entry.grants {
            save_privilege(db, grant)?;
        }
        for trigger in &entry.triggers {
            save_trigger(db, trigger)?;
        }
        for sequence in &entry.sequences {
            save_sequence(db, sequence)?;
        }
        if list_recycle_bin(db)?
            .iter()
            .any(|e| e.bin_name == entry.bin_name)
        {
            delete_recycle_bin_entry(db, &entry.bin_name)?;
        }
        clear(db, &entry.bin_name)?;
        repaired += 1;
    }
    Ok(repaired)
}
