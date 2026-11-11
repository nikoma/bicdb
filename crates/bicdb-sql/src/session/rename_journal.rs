//! Durable undo building blocks for generic rename. Not yet connected to SQL
//! dispatch or startup: integration must reserve affected relations and merge
//! these intents into the recycle journal's reverse transaction ordering.
use super::*;

const HEADERS: &str = "__bicdb_rename_intents";
const IMAGES: &str = "__bicdb_rename_images";

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    id: String,
    order: u64,
    count: u64,
    /// Persist each completed undo step. A crash between applying an effect
    /// and advancing this cursor replays only that idempotent effect.
    remaining: Option<u64>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum Effect {
    Row {
        storage: String,
        id: String,
        previous: Option<Record>,
    },
    Rename {
        storage: String,
        old: String,
        new: String,
    },
}

pub(super) struct PreparedRename {
    header: Header,
    sequence: u64,
}

fn invalid(message: &str) -> SqlError {
    SqlError::InvalidSql(format!("rename undo journal: {message}"))
}

fn backing_name(db: &BicDb, storage: &str) -> Result<String> {
    let names = db
        .collections()
        .into_iter()
        .filter(|meta| meta.storage() == storage)
        .map(|meta| meta.name)
        .collect::<Vec<_>>();
    match names.as_slice() {
        [name] => Ok(name.clone()),
        _ => Err(invalid("missing or ambiguous physical backing")),
    }
}

fn storage_for(db: &BicDb, name: &str) -> Result<String> {
    if [HEADERS, IMAGES].contains(&name) {
        return Err(invalid("cannot journal the journal itself"));
    }
    db.collections()
        .into_iter()
        .find(|meta| meta.name == name)
        .map(|meta| meta.storage().to_string())
        .ok_or_else(|| invalid("capture collection does not exist"))
}

fn image_id(header: &Header, index: u64) -> String {
    format!("{}:{index:020}", header.id)
}

pub(super) fn prepare(db: &mut BicDb) -> Result<PreparedRename> {
    db.enable_sql_rename_intents()?;
    db.create_collection(HEADERS)?;
    db.create_collection(IMAGES)?;
    let mut tx = db.begin_transaction()?;
    let header = Header {
        id: uuid::Uuid::new_v4().simple().to_string(),
        order: tx.id().0,
        count: 0,
        remaining: None,
    };
    tx.insert(
        HEADERS,
        Record::new(&header.id).with_metadata(serde_json::to_value(&header)?),
    )?;
    let sequence = tx.commit_with_sequence()?;
    Ok(PreparedRename { header, sequence })
}

fn append(db: &BicDb, prepared: &mut PreparedRename, effect: Effect) -> Result<()> {
    if prepared.header.remaining.is_some() {
        return Err(invalid("cannot append after rollback starts"));
    }
    let mut next = prepared.header.clone();
    next.count = next
        .count
        .checked_add(1)
        .ok_or_else(|| invalid("image count overflow"))?;
    // The image and its manifest count share one WAL decision. Keep each
    // original row in its own record instead of repeatedly serializing an
    // ever-growing before-image vector into a single giant manifest.
    let mut tx = db.begin_transaction()?;
    tx.insert(
        IMAGES,
        Record::new(image_id(&prepared.header, prepared.header.count))
            .with_metadata(serde_json::to_value(effect)?),
    )?;
    tx.update(
        HEADERS,
        Record::new(&next.id).with_metadata(serde_json::to_value(&next)?),
    )?;
    let sequence = tx.commit_with_sequence()?;
    prepared.header = next;
    prepared.sequence = sequence;
    Ok(())
}

pub(super) fn capture_row(
    db: &BicDb,
    prepared: &mut PreparedRename,
    collection: &str,
    id: &str,
) -> Result<()> {
    let storage = storage_for(db, collection)?;
    let previous = db.get(collection, id)?.map(|row| row.as_ref().clone());
    append(
        db,
        prepared,
        Effect::Row {
            storage,
            id: id.into(),
            previous,
        },
    )
}

pub(super) fn capture_rename(
    db: &BicDb,
    prepared: &mut PreparedRename,
    old: &str,
    new: &str,
) -> Result<()> {
    require_user_relation_name(old)?;
    require_user_relation_name(new)?;
    if old == new || db.collections().iter().any(|meta| meta.name == new) {
        return Err(invalid("rename target is occupied or unchanged"));
    }
    let storage = storage_for(db, old)?;
    append(
        db,
        prepared,
        Effect::Rename {
            storage,
            old: old.into(),
            new: new.into(),
        },
    )
}

/// Remove only the manifest in the caller's business transaction. Committed
/// image records become harmless garbage, cleaned after catalog recovery.
pub(super) fn decide(prepared: &PreparedRename, tx: &mut Transaction) -> Result<()> {
    if prepared.header.remaining.is_some() {
        return Err(invalid("cannot commit a rolling-back intent"));
    }
    tx.delete_many_with_statement_snapshots(HEADERS, [(&prepared.header.id, prepared.sequence)])?;
    Ok(())
}

fn load(db: &BicDb, id: &str) -> Result<Option<Header>> {
    let Some(record) = db.get(HEADERS, id)? else {
        return Ok(None);
    };
    let header: Header = serde_json::from_value(record.metadata.clone())?;
    if header.id != id
        || uuid::Uuid::parse_str(id).is_err()
        || header
            .remaining
            .is_some_and(|remaining| remaining > header.count)
    {
        return Err(invalid("invalid manifest identity or rollback cursor"));
    }
    Ok(Some(header))
}

/// Startup-only undo. The host must merge intents with other DDL journals in
/// reverse `order`; it must never run this against live SQL transactions.
pub(super) fn rollback(db: &mut BicDb, id: &str) -> Result<()> {
    db.check_catalog_recovery_quiescent()?;
    let Some(mut header) = load(db, id)? else {
        return Ok(());
    };
    let remaining = header.remaining.unwrap_or(header.count);
    // Validate the complete remaining image inventory before any repair. Its
    // size does not control allocation: read one row at a time, twice.
    let mut names = db
        .collections()
        .into_iter()
        .map(|meta| (meta.name.clone(), meta.storage().to_string()))
        .collect::<BTreeMap<_, _>>();
    for index in (0..remaining).rev() {
        let record = db
            .get(IMAGES, &image_id(&header, index))?
            .ok_or_else(|| invalid("missing before-image"))?;
        let effect: Effect = serde_json::from_value(record.metadata.clone())?;
        match effect {
            Effect::Row {
                storage,
                id,
                previous,
            } => {
                if previous.as_ref().is_some_and(|row| row.id != id) {
                    return Err(invalid("before-image row identity mismatch"));
                }
                let name = backing_name(db, &storage)?;
                storage_for(db, &name)?;
            }
            Effect::Rename { storage, old, new } => {
                require_user_relation_name(&old)?;
                require_user_relation_name(&new)?;
                if old == new {
                    return Err(invalid("invalid rename effect"));
                }
                let matches = names
                    .iter()
                    .filter(|(_, backing)| **backing == storage)
                    .map(|(name, _)| name.clone())
                    .collect::<Vec<_>>();
                let [name] = matches.as_slice() else {
                    return Err(invalid("missing or ambiguous planned backing"));
                };
                if name == &new {
                    if names.contains_key(&old) {
                        return Err(invalid("rollback target was reused"));
                    }
                    names.remove(&new);
                    names.insert(old, storage);
                } else if name != &old {
                    return Err(invalid("backing has an unexpected planned name"));
                }
            }
        }
    }
    header.remaining = Some(remaining);
    persist_header(db, &header)?;
    while header.remaining.unwrap() > 0 {
        let index = header.remaining.unwrap() - 1;
        let record = db
            .get(IMAGES, &image_id(&header, index))?
            .ok_or_else(|| invalid("missing before-image"))?;
        let effect: Effect = serde_json::from_value(record.metadata.clone())?;
        match effect {
            Effect::Row {
                storage,
                id,
                previous,
            } => {
                let name = backing_name(db, &storage)?;
                let current = db.get(&name, &id)?;
                if serde_json::to_value(current.as_deref())? != serde_json::to_value(&previous)? {
                    match previous {
                        Some(row) => {
                            db.insert(&name, row)?;
                        }
                        None => {
                            db.delete(&name, &id)?;
                        }
                    }
                }
            }
            Effect::Rename { storage, old, new } => {
                let name = backing_name(db, &storage)?;
                if name == new {
                    if db.collections().iter().any(|meta| meta.name == old) {
                        return Err(invalid("rollback target was reused"));
                    }
                    db.rename_collection(&new, &old)?;
                } else if name != old {
                    return Err(invalid("backing has an unexpected logical name"));
                }
            }
        }
        header.remaining = Some(index);
        persist_header(db, &header)?;
    }
    db.delete(HEADERS, &header.id)?;
    Ok(())
}

fn persist_header(db: &BicDb, header: &Header) -> Result<()> {
    db.insert(
        HEADERS,
        Record::new(&header.id).with_metadata(serde_json::to_value(header)?),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bicdb_core::{DbConfig, StorageMode};

    fn row(id: &str, value: i64) -> Record {
        Record::new(id).with_metadata(serde_json::json!({"value":value}))
    }

    #[test]
    fn uncommitted_journal_restores_backing_and_scoped_rows_after_reopen() {
        for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
            let root = tempfile::tempdir().unwrap();
            let config = DbConfig::default().with_storage_mode(mode);
            let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            db.create_collection("original").unwrap();
            db.create_collection("dependent").unwrap();
            db.insert("original", row("seed", 1)).unwrap();
            db.insert("dependent", row("existing", 2)).unwrap();
            db.insert("dependent", row("unrelated", 99)).unwrap();
            let mut plan = prepare(&mut db).unwrap();
            capture_rename(&db, &mut plan, "original", "renamed").unwrap();
            db.rename_collection("original", "renamed").unwrap();
            capture_row(&db, &mut plan, "dependent", "existing").unwrap();
            db.insert("dependent", row("existing", 20)).unwrap();
            capture_row(&db, &mut plan, "dependent", "created").unwrap();
            db.insert("dependent", row("created", 30)).unwrap();
            db.close().unwrap();
            let mut db = BicDb::open_with_config(root.path(), config).unwrap();
            rollback(&mut db, &plan.header.id).unwrap();
            rollback(&mut db, &plan.header.id).unwrap();
            assert_eq!(
                db.get("original", "seed").unwrap().unwrap().metadata["value"],
                1
            );
            assert!(!db.collections().iter().any(|meta| meta.name == "renamed"));
            assert_eq!(
                db.get("dependent", "existing").unwrap().unwrap().metadata["value"],
                2
            );
            assert_eq!(
                db.get("dependent", "unrelated").unwrap().unwrap().metadata["value"],
                99
            );
            assert!(db.get("dependent", "created").unwrap().is_none());
            db.close().unwrap();
        }
    }

    #[test]
    fn decision_and_business_rows_share_the_caller_commit() {
        for commit in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let mut db = BicDb::open(root.path()).unwrap();
            db.create_collection("metadata").unwrap();
            db.create_collection("business").unwrap();
            db.insert("metadata", row("type", 1)).unwrap();
            let mut plan = prepare(&mut db).unwrap();
            capture_row(&db, &mut plan, "metadata", "type").unwrap();
            db.insert("metadata", row("type", 2)).unwrap();
            let mut tx = db.begin_transaction().unwrap();
            tx.insert("business", row("ack", 7)).unwrap();
            decide(&plan, &mut tx).unwrap();
            if commit {
                tx.commit().unwrap();
            } else {
                tx.rollback().unwrap();
            }
            db.close().unwrap();
            let mut db = BicDb::open(root.path()).unwrap();
            rollback(&mut db, &plan.header.id).unwrap();
            assert_eq!(db.get("business", "ack").unwrap().is_some(), commit);
            assert_eq!(
                db.get("metadata", "type").unwrap().unwrap().metadata["value"],
                if commit { 2 } else { 1 }
            );
            db.close().unwrap();
        }
    }

    #[test]
    fn incomplete_images_and_reused_names_refuse_before_restoring_rows() {
        for missing in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let mut db = BicDb::open(root.path()).unwrap();
            db.create_collection("original").unwrap();
            db.create_collection("dependent").unwrap();
            db.insert("dependent", row("value", 1)).unwrap();
            let mut plan = prepare(&mut db).unwrap();
            capture_rename(&db, &mut plan, "original", "renamed").unwrap();
            db.rename_collection("original", "renamed").unwrap();
            capture_row(&db, &mut plan, "dependent", "value").unwrap();
            db.insert("dependent", row("value", 2)).unwrap();
            if missing {
                db.delete(IMAGES, &image_id(&plan.header, 0)).unwrap();
            } else {
                db.create_collection("original").unwrap();
            }
            assert!(rollback(&mut db, &plan.header.id).is_err());
            assert_eq!(
                db.get("dependent", "value").unwrap().unwrap().metadata["value"],
                2
            );
            assert!(load(&db, &plan.header.id)
                .unwrap()
                .unwrap()
                .remaining
                .is_none());
            db.close().unwrap();
        }
    }
}
