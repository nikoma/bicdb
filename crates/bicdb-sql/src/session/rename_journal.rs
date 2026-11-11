//! Durable undo for generic rename, connected to ordered startup DDL recovery.
//! SQL dispatch captures its dependency closure before mutation and stages the
//! decision with its business transaction. Scoped concurrent admission and
//! replication qualification remain gates for the unmerged prototype.
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

pub(super) struct PendingRename {
    pub(super) id: String,
    pub(super) order: u64,
}

pub(super) fn is_collection(name: &str) -> bool {
    [HEADERS, IMAGES].contains(&name)
}

/// Gather identities only. Recovery of their physical backings must run after
/// later recycle/creation intents have been reversed, in the shared timeline.
pub(super) fn pending(db: &BicDb) -> Result<Vec<PendingRename>> {
    let records = match db.scan_collection(HEADERS) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    records
        .into_iter()
        .map(|record| {
            let header = load(db, &record.id)?.ok_or_else(|| invalid("manifest disappeared"))?;
            Ok(PendingRename {
                id: header.id,
                order: header.order,
            })
        })
        .collect()
}

pub(super) struct PreparedRename {
    header: Header,
    sequence: u64,
}

impl PreparedRename {
    pub(super) fn id(&self) -> &str {
        &self.header.id
    }
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

/// Capture failed before any domain mutator ran. Resolve the manifest now,
/// independently of the caller's transaction: leaving a live undo intent could
/// otherwise reverse a later acknowledged write during the next startup.
pub(super) fn discard_unapplied_capture(db: &BicDb, prepared: &PreparedRename) -> Result<()> {
    let mut tx = db.begin_transaction()?;
    decide(prepared, &mut tx)?;
    tx.commit()?;
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
    rollback_inner(db, id, None)
}

/// The caller restores its write/lock savepoint first. Earlier buffered writes
/// survive and are rebound by the core rename API during catalog undo.
pub(super) fn rollback_owned(db: &mut BicDb, id: &str, owner: &mut Transaction) -> Result<()> {
    rollback_inner(db, id, Some(owner))
}

pub(super) fn undo_session(session: &mut SqlSession<'_>, id: &str) -> Result<()> {
    let mut owner = session.tx.take();
    let result = session.db_mut().and_then(|db| match owner.as_mut() {
        Some(tx) => rollback_owned(db, id, tx),
        None => rollback(db, id),
    });
    // Even refused undo must retain the transaction and its rollback state.
    session.tx = owner;
    result
}

fn rollback_inner(db: &mut BicDb, id: &str, mut owner: Option<&mut Transaction>) -> Result<()> {
    match owner.as_deref() {
        Some(tx) => db.check_catalog_undo_owner(tx)?,
        None => db.check_catalog_recovery_quiescent()?,
    }
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
                if name == SCHEMA_COLLECTION {
                    if let Some(record) = previous {
                        let schema: TableSchema = serde_json::from_value(record.metadata)?;
                        if schema.name != id {
                            return Err(invalid("schema image identity mismatch"));
                        }
                    }
                }
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
                if name == SCHEMA_COLLECTION {
                    sql_schema_list_cache_invalidate(db);
                    let restored = db.get(&name, &id)?;
                    let schema = restored
                        .map(|record| {
                            serde_json::from_value::<TableSchema>(record.metadata.clone())
                        })
                        .transpose()?;
                    sql_schema_cache_set(db, &id, schema.map(std::sync::Arc::new));
                }
            }
            Effect::Rename { storage, old, new } => {
                let name = backing_name(db, &storage)?;
                if name == new {
                    if db.collections().iter().any(|meta| meta.name == old) {
                        return Err(invalid("rollback target was reused"));
                    }
                    match owner.as_deref_mut() {
                        Some(tx) => {
                            db.rename_collection_preserving_pending_writes(tx, &new, &old)?
                        }
                        None => db.rename_collection(&new, &old)?,
                    }
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

/// Only a deleted manifest proves that all its images are obsolete. Never
/// prune an active intent's images, including steps already undone by cursor.
pub(super) fn collect_orphans(db: &mut BicDb) -> Result<usize> {
    db.check_catalog_recovery_quiescent()?;
    let images = match db.scan_collection(IMAGES) {
        Ok(images) => images,
        Err(BicDbError::CollectionNotFound(_)) => return Ok(0),
        Err(error) => return Err(error.into()),
    };
    let mut obsolete = Vec::new();
    for record in images {
        let (id, index) = record
            .id
            .split_once(':')
            .ok_or_else(|| invalid("invalid image identity"))?;
        let uuid = uuid::Uuid::parse_str(id).map_err(|_| invalid("invalid image owner"))?;
        let index = index
            .parse::<u64>()
            .map_err(|_| invalid("invalid image ordinal"))?;
        if uuid.simple().to_string() != id || record.id != format!("{id}:{index:020}") {
            return Err(invalid("noncanonical image identity"));
        }
        let _: Effect = serde_json::from_value(record.metadata)?;
        if load(db, id)?.is_none() {
            obsolete.push(record.id);
        }
    }
    for id in &obsolete {
        db.delete(IMAGES, id)?;
    }
    Ok(obsolete.len())
}

fn persist_header(db: &mut BicDb, header: &Header) -> Result<()> {
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
    #[test]
    fn recovery_composes_rename_recycle_and_reused_name_in_reverse_order() {
        for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
            let root = tempfile::tempdir().unwrap();
            let config = DbConfig::default().with_storage_mode(mode);
            let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            SqlSession::new(&mut db).execute("CREATE TABLE original(rowkey INT PRIMARY KEY,payload TEXT); INSERT INTO original VALUES(1,'acknowledged')").unwrap();
            let schema = load_schema(&db, "original").unwrap().unwrap();
            let mut renamed_schema = schema.clone();
            renamed_schema.primary_key_name = Some(schema.primary_key_constraint_name());
            renamed_schema.name = "renamed".into();
            let mut plan = prepare(&mut db).unwrap();
            capture_rename(&db, &mut plan, "original", "renamed").unwrap();
            capture_row(&db, &mut plan, SCHEMA_COLLECTION, "original").unwrap();
            capture_row(&db, &mut plan, SCHEMA_COLLECTION, "renamed").unwrap();
            db.rename_collection("original", "renamed").unwrap();
            delete_schema(&mut db, "original").unwrap();
            save_schema(&mut db, &renamed_schema).unwrap();
            let bin = format!("__bicdb_bin_{}", uuid::Uuid::new_v4().simple());
            let entry = recycle_bin::RecycleBinEntry {
                bin_name: bin.clone(),
                table: "renamed".into(),
                owner: BOOTSTRAP_ROLE_NAME.into(),
                dropped_at_micros: 0,
                schema: renamed_schema,
                grants: vec![],
                triggers: vec![],
                sequences: vec![],
                indexes: db
                    .index_definitions()
                    .into_iter()
                    .filter(|i| i.collection == "renamed")
                    .map(|i| (i.name.clone(), i.name))
                    .collect(),
            };
            recycle_bin_journal::prepare(&mut db, &entry).unwrap();
            db.rename_collection("renamed", &bin).unwrap();
            delete_schema(&mut db, "renamed").unwrap();
            // Reuse the logical name with a separately identified backing.
            // The existing creation journal must undo this BEFORE the later
            // recycle move, which must itself precede the generic rename.
            let created = format!("__bicdb_created_{}", uuid::Uuid::new_v4().simple());
            db.create_collection("__bicdb_recycle_creations").unwrap();
            let mut tx = db.begin_transaction().unwrap();
            let order = tx.id().0;
            tx.insert(
                "__bicdb_recycle_creations",
                Record::new(&created).with_metadata(
                    serde_json::json!({"table":"renamed","storage":created,"order":order}),
                ),
            )
            .unwrap();
            tx.commit().unwrap();
            db.create_collection(&created).unwrap();
            db.rename_collection(&created, "renamed").unwrap();
            db.insert("renamed", row("replacement", 99)).unwrap();
            db.close().unwrap();
            let mut db = BicDb::open_with_config(root.path(), config).unwrap();
            assert_eq!(
                recycle_bin::recover_interrupted_recycle_bin_renames(&mut db).unwrap(),
                3
            );
            assert_eq!(
                recycle_bin::recover_interrupted_recycle_bin_renames(&mut db).unwrap(),
                0
            );
            assert!(load_schema(&db, "renamed").unwrap().is_none());
            assert!(!db
                .collections()
                .iter()
                .any(|m| m.name == "renamed" || m.name == bin));
            assert_eq!(
                SqlSession::new(&mut db)
                    .execute("SELECT rowkey,payload FROM original")
                    .unwrap()
                    .rows,
                vec![vec![
                    SqlValue::Int(1),
                    SqlValue::String("acknowledged".into())
                ]]
            );
            assert!(db
                .verify_indexes_consistently()
                .unwrap()
                .iter()
                .all(|i| i.valid));
            db.close().unwrap();
        }
    }
    #[test]
    fn orphan_collection_keeps_undecided_images_and_business_data() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        db.create_collection("business").unwrap();
        db.insert("business", row("value", 1)).unwrap();
        let mut committed = prepare(&mut db).unwrap();
        capture_row(&db, &mut committed, "business", "value").unwrap();
        let mut pending = prepare(&mut db).unwrap();
        capture_row(&db, &mut pending, "business", "other").unwrap();
        let mut tx = db.begin_transaction().unwrap();
        tx.update("business", row("value", 2)).unwrap();
        decide(&committed, &mut tx).unwrap();
        tx.commit().unwrap();
        assert_eq!(collect_orphans(&mut db).unwrap(), 1);
        assert!(db
            .get(IMAGES, &image_id(&pending.header, 0))
            .unwrap()
            .is_some());
        assert_eq!(
            db.get("business", "value").unwrap().unwrap().metadata["value"],
            2
        );
        rollback(&mut db, &pending.header.id).unwrap();
        assert_eq!(collect_orphans(&mut db).unwrap(), 1);
        assert_eq!(collect_orphans(&mut db).unwrap(), 0);
        assert_eq!(
            db.get("business", "value").unwrap().unwrap().metadata["value"],
            2
        );
        db.close().unwrap();
    }
    #[test]
    fn live_undo_preserves_rows_buffered_before_the_rename_savepoint() {
        for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
            let root = tempfile::tempdir().unwrap();
            let mut db =
                BicDb::open_with_config(root.path(), DbConfig::default().with_storage_mode(mode))
                    .unwrap();
            db.create_collection("original").unwrap();
            db.insert("original", row("seed", 1)).unwrap();
            let mut owner = db.begin_transaction().unwrap();
            owner.insert("original", row("pending", 7)).unwrap();
            let mark = owner.rollback_mark();
            let mut plan = prepare(&mut db).unwrap();
            capture_rename(&db, &mut plan, "original", "renamed").unwrap();
            db.rename_collection_preserving_pending_writes(&mut owner, "original", "renamed")
                .unwrap();
            owner.update("renamed", row("pending", 8)).unwrap();
            decide(&plan, &mut owner).unwrap();
            owner.rollback_to_mark(mark).unwrap();
            {
                let mut session = SqlSession::new(&mut db);
                session.tx = Some(owner);
                session.ddl_undo.push(DdlUndo::RestoreRenameJournal {
                    id: plan.header.id.clone(),
                    old_table: "original".into(),
                });
                session.rollback_ddl_to_len(0).unwrap();
                owner = session.tx.take().unwrap();
            }
            assert_eq!(
                owner.get("original", "pending").unwrap().unwrap().metadata["value"],
                7
            );
            owner.commit().unwrap();
            assert_eq!(
                db.get("original", "seed").unwrap().unwrap().metadata["value"],
                1
            );
            assert_eq!(
                db.get("original", "pending").unwrap().unwrap().metadata["value"],
                7
            );
            db.close().unwrap();
        }
    }

    #[test]
    fn live_undo_refuses_other_owners_before_changing_the_backing() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        db.create_collection("original").unwrap();
        let mut owner = db.begin_transaction().unwrap();
        let mut plan = prepare(&mut db).unwrap();
        capture_rename(&db, &mut plan, "original", "renamed").unwrap();
        db.rename_collection_preserving_pending_writes(&mut owner, "original", "renamed")
            .unwrap();
        let mut other = db.begin_transaction().unwrap();
        let owner_id = owner.id();
        {
            let mut session = SqlSession::new(&mut db);
            session.tx = Some(owner);
            assert!(undo_session(&mut session, &plan.header.id).is_err());
            assert_eq!(session.tx.as_ref().unwrap().id(), owner_id);
            owner = session.tx.take().unwrap();
        }
        assert!(db.collections().iter().any(|m| m.name == "renamed"));
        assert!(load(&db, &plan.header.id)
            .unwrap()
            .unwrap()
            .remaining
            .is_none());
        other.rollback().unwrap();
        let foreign_root = tempfile::tempdir().unwrap();
        let foreign_db = BicDb::open(foreign_root.path()).unwrap();
        let mut foreign = foreign_db.begin_transaction().unwrap();
        assert!(rollback_owned(&mut db, &plan.header.id, &mut foreign).is_err());
        foreign.rollback().unwrap();
        rollback_owned(&mut db, &plan.header.id, &mut owner).unwrap();
        owner.rollback().unwrap();
        db.close().unwrap();
    }
}
