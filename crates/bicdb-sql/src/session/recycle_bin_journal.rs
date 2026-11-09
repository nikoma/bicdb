//! Durable intent for recycle moves. Decisions are ordinary writes in the
//! caller's transaction, so its row changes and DDL decision share one WAL commit.
use super::recycle_bin::*;
use super::*;
const INTENTS: &str = "__bicdb_recycle_intents";
const PURGES: &str = "__bicdb_recycle_purges";
const CREATIONS: &str = "__bicdb_recycle_creations";
const CREATED_PREFIX: &str = "__bicdb_created_";
#[derive(Clone, Serialize, Deserialize)]
struct CreationIntent {
    table: String,
    storage: String,
    order: u64,
}

// A reused logical name alone cannot prove that startup owns its backing.
// Allocate an unreachable, unique backing only after its identity is durable.
// The outer transaction removes this intent in the same decision as its rows.
pub(super) fn create_replacement_collection(
    session: &mut SqlSession<'_>,
    name: &str,
) -> Result<bool> {
    if session.tx.is_none()
        || !session.ddl_undo.iter().any(
            |undo| matches!(undo, DdlUndo::RestoreFromRecycleBin { target, .. } if target == name),
        )
    {
        return Ok(false);
    }
    let storage = format!("{CREATED_PREFIX}{}", uuid::Uuid::new_v4().simple());
    let sequence = {
        let db = session.db_mut()?;
        db.enable_sql_catalog_intents()?;
        db.create_collection(CREATIONS)?;
        let mut tx = db.begin_transaction()?;
        let intent = CreationIntent {
            table: name.to_string(),
            storage: storage.clone(),
            order: tx.id().0,
        };
        tx.insert(
            CREATIONS,
            Record::new(&storage).with_metadata(serde_json::to_value(intent)?),
        )?;
        tx.commit_with_sequence()?
    };
    session.ddl_undo.push(DdlUndo::DropCollection {
        name: name.to_string(),
        creation_intent: Some(storage.clone()),
    });
    stage("recreate_intent");
    session.db_mut()?.create_collection(&storage)?;
    stage("recreate_backing");
    session.db_mut()?.rename_collection(&storage, name)?;
    stage("recreate_name");
    session
        .tx
        .as_mut()
        .expect("explicit replacement transaction")
        .delete_many_with_statement_snapshots(CREATIONS, [(&storage, sequence)])?;
    stage("recreate_decision");
    Ok(true)
}

fn validate_creation(intent: &CreationIntent, id: &str) -> Result<()> {
    require_user_relation_name(&intent.table)?;
    if intent.storage != id
        || !id.starts_with(CREATED_PREFIX)
        || uuid::Uuid::parse_str(&id[CREATED_PREFIX.len()..]).is_err()
    {
        return Err(SqlError::InvalidSql(
            "invalid replacement creation intent identity".into(),
        ));
    }
    Ok(())
}

fn rollback_creation(db: &mut BicDb, intent: &CreationIntent) -> Result<()> {
    let collections = db.collections();
    let target = collections.iter().find(|meta| meta.name == intent.table);
    let staged = collections.iter().find(|meta| meta.name == intent.storage);
    if target.is_some() && staged.is_some()
        || target.is_some_and(|meta| meta.storage() != intent.storage)
        || staged.is_some_and(|meta| meta.storage() != intent.storage)
    {
        return Err(SqlError::InvalidSql(format!(
            "ambiguous replacement backing for {}",
            intent.table
        )));
    }
    if target.is_some() {
        // Every removed object belongs to the independently proven backing;
        // never prune a collection merely because its logical name was reused.
        delete_schema(db, &intent.table)?;
        for grant in list_privileges(db)? {
            if grant.object_type == PrivilegeObjectType::Table && grant.object_name == intent.table
            {
                delete_privilege(db, &grant)?;
            }
        }
        for trigger in list_triggers(db)? {
            if trigger.table_name == intent.table {
                delete_trigger(db, &trigger.name, Some(&intent.table))?;
            }
        }
        for sequence in list_sequences(db)? {
            if sequence.owned_by_table.as_deref() == Some(&intent.table) {
                delete_sequence(db, &sequence.name)?;
            }
        }
        db.drop_collection(&intent.table)?;
    } else if staged.is_some() {
        db.drop_collection(&intent.storage)?;
    }
    db.delete(CREATIONS, &intent.storage)?;
    Ok(())
}

pub(super) fn undo_creation(db: &mut BicDb, id: &str) -> Result<()> {
    let record = db
        .get(CREATIONS, id)?
        .ok_or_else(|| SqlError::InvalidSql("missing replacement creation intent".into()))?;
    let intent: CreationIntent = serde_json::from_value(record.metadata.clone())?;
    validate_creation(&intent, id)?;
    rollback_creation(db, &intent)
}
#[derive(Clone, Serialize, Deserialize)]
struct MoveIntent {
    entry: RecycleBinEntry,
    definitions: Vec<bicdb_core::IndexDefinition>,
    #[serde(default)]
    order: u64,
    #[serde(default)]
    restore: Option<RestorePlan>,
}
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct RestorePlan {
    pub(super) id: String,
    pub(super) target: String,
    pub(super) schema: TableSchema,
    pub(super) sequences: Vec<SequenceSchema>,
    pub(super) indexes: Vec<(String, String)>,
}
pub(super) fn prepare(db: &mut BicDb, entry: &RecycleBinEntry) -> Result<u64> {
    db.enable_sql_catalog_intents()?;
    db.create_collection(INTENTS)?;
    prepare_inner(db, entry, None)
}
pub(super) fn prepare_restore(
    db: &mut BicDb,
    entry: &RecycleBinEntry,
    plan: RestorePlan,
) -> Result<u64> {
    db.enable_sql_catalog_intents()?;
    db.create_collection(INTENTS)?;
    prepare_inner(db, entry, Some(plan))
}
fn prepare_inner(
    db: &mut BicDb,
    entry: &RecycleBinEntry,
    restore: Option<RestorePlan>,
) -> Result<u64> {
    let source = if restore.is_some() {
        &entry.bin_name
    } else {
        &entry.table
    };
    let definitions = db
        .index_definitions()
        .into_iter()
        .filter(|index| &index.collection == source)
        .collect();
    let mut tx = db.begin_transaction()?;
    let intent = MoveIntent {
        entry: entry.clone(),
        definitions,
        order: tx.id().0,
        restore,
    };
    let id = intent
        .restore
        .as_ref()
        .map(|plan| plan.id.as_str())
        .unwrap_or(&entry.bin_name);
    tx.insert(
        INTENTS,
        Record::new(id).with_metadata(serde_json::to_value(&intent)?),
    )?;
    Ok(tx.commit_with_sequence()?)
}
pub(super) fn decide(session: &mut SqlSession<'_>, bin: &str, sequence: u64) -> Result<()> {
    match session.tx.as_mut() {
        Some(tx) => tx.delete_many_with_statement_snapshots(INTENTS, [(bin, sequence)])?,
        None => {
            session.db_mut()?.delete(INTENTS, bin)?;
        }
    }
    Ok(())
}
pub(super) fn clear(db: &mut BicDb, bin: &str) -> Result<()> {
    // Called after metadata restoration, so repeated recovery stays safe.
    let mut tx = db.begin_transaction()?;
    tx.delete(INTENTS, bin)?;
    tx.commit()?;
    Ok(())
}
#[inline]
pub(super) fn stage(_name: &str) {
    #[cfg(test)]
    if std::env::var("BICDB_TEST_RECYCLE_CUT").ok().as_deref() == Some(_name) {
        std::process::exit(99);
    }
}
pub(super) fn clear_if_present(db: &mut BicDb, bin: &str) -> Result<()> {
    match db.get(INTENTS, bin) {
        Ok(Some(_)) => clear(db, bin),
        Ok(None) | Err(BicDbError::CollectionNotFound(_)) => Ok(()),
        Err(error) => Err(error.into()),
    }
}
pub(super) fn prepare_purge(db: &mut BicDb, entry: &RecycleBinEntry) -> Result<()> {
    db.enable_sql_catalog_intents()?;
    db.create_collection(PURGES)?;
    db.insert(
        PURGES,
        Record::new(entry.bin_name.clone()).with_metadata(serde_json::to_value(entry)?),
    )?;
    Ok(())
}
pub(super) fn schedule_purge(session: &mut SqlSession<'_>, entry: &RecycleBinEntry) -> Result<()> {
    session.db_mut()?.create_collection(PURGES)?;
    session.tx.as_mut().expect("transactional purge").insert(
        PURGES,
        Record::new(&entry.bin_name).with_metadata(serde_json::to_value(entry)?),
    )?;
    Ok(())
}
pub(super) fn purge_is_decided(db: &BicDb, bin: &str) -> Result<bool> {
    match db.get(PURGES, bin) {
        Ok(record) => Ok(record.is_some()),
        Err(BicDbError::CollectionNotFound(_)) => Ok(false),
        Err(error) => Err(error.into()),
    }
}
pub(super) fn finish_purge(db: &mut BicDb, entry: &RecycleBinEntry) -> Result<()> {
    if db
        .collections()
        .iter()
        .any(|meta| meta.name == entry.bin_name)
    {
        db.drop_collection(&entry.bin_name)?;
    }
    stage("purge_collection");
    // Decided purges are already hidden from the public recycle-bin view.
    if db.get(RECYCLE_BIN_COLLECTION, &entry.bin_name)?.is_some() {
        delete_recycle_bin_entry(db, &entry.bin_name)?;
    }
    stage("purge_entry");
    clear_if_present(db, &entry.bin_name)?;
    db.delete(PURGES, &entry.bin_name)?;
    Ok(())
}
pub(super) fn recover_purges(db: &mut BicDb) -> Result<usize> {
    let records = match db.scan_collection(PURGES) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => return Ok(0),
        Err(error) => return Err(error.into()),
    };
    let count = records.len();
    for record in records {
        let entry: RecycleBinEntry = serde_json::from_value(record.metadata)?;
        if record.id != entry.bin_name
            || !entry.bin_name.starts_with("__bicdb_bin_")
            || uuid::Uuid::parse_str(&entry.bin_name["__bicdb_bin_".len()..]).is_err()
        {
            return Err(SqlError::InvalidSql(
                "invalid recycle purge intent identity".into(),
            ));
        }
        finish_purge(db, &entry)?;
    }
    Ok(count)
}
pub(super) fn recover(db: &mut BicDb) -> Result<usize> {
    let purge_repairs = recover_purges(db)?;
    let records = match db.scan_collection(INTENTS) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => Vec::new(),
        Err(error) => return Err(error.into()),
    };
    let mut intents = records
        .into_iter()
        .map(|record| {
            let intent: MoveIntent = serde_json::from_value(record.metadata)?;
            let id = intent
                .restore
                .as_ref()
                .map(|plan| plan.id.as_str())
                .unwrap_or(&intent.entry.bin_name);
            if record.id != id {
                return Err(SqlError::InvalidSql(
                    "recycle intent identity mismatch".into(),
                ));
            }
            Ok(intent)
        })
        .collect::<Result<Vec<_>>>()?;
    let creations = match db.scan_collection(CREATIONS) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => Vec::new(),
        Err(error) => return Err(error.into()),
    };
    enum PendingIntent {
        Move(MoveIntent),
        Creation(CreationIntent),
    }
    let mut pending = intents
        .drain(..)
        .map(PendingIntent::Move)
        .collect::<Vec<_>>();
    for record in creations {
        let creation: CreationIntent = serde_json::from_value(record.metadata)?;
        validate_creation(&creation, &record.id)?;
        pending.push(PendingIntent::Creation(creation));
    }
    pending.sort_by_key(|intent| {
        std::cmp::Reverse(match intent {
            PendingIntent::Move(intent) => intent.order,
            PendingIntent::Creation(intent) => intent.order,
        })
    });
    let mut repaired = purge_repairs;
    for intent in pending {
        let intent = match intent {
            PendingIntent::Move(intent) => intent,
            PendingIntent::Creation(intent) => {
                rollback_creation(db, &intent)?;
                repaired += 1;
                continue;
            }
        };
        if intent.restore.is_some() {
            rollback_restore(db, &intent)?;
            repaired += 1;
            continue;
        }
        let entry = &intent.entry;
        let collections = db.collections();
        let original = collections.iter().any(|c| c.name == entry.table);
        let bin = collections.iter().any(|c| c.name == entry.bin_name);
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

fn restore_backing_state(db: &BicDb, intent: &MoveIntent) -> Result<bool> {
    let plan = intent.restore.as_ref().expect("restore intent");
    let collections = db.collections();
    let target = collections.iter().any(|meta| meta.name == plan.target);
    let bin = collections
        .iter()
        .any(|meta| meta.name == intent.entry.bin_name);
    if target == bin {
        return Err(SqlError::InvalidSql(format!(
            "ambiguous pending restore intent {}",
            plan.id
        )));
    }
    let indexes = db.index_definitions();
    for definition in &intent.definitions {
        let restored = plan
            .indexes
            .iter()
            .find(|(from, _)| from == &definition.name)
            .map(|(_, to)| to)
            .ok_or_else(|| SqlError::InvalidSql("restore intent missing index mapping".into()))?;
        let matching = indexes
            .iter()
            .filter(|index| index.name == definition.name || index.name == *restored)
            .collect::<Vec<_>>();
        let [current] = matching.as_slice() else {
            return Err(SqlError::InvalidSql(format!(
                "ambiguous pending restore index {}",
                definition.name
            )));
        };
        let mut expected = definition.clone();
        expected.name = current.name.clone();
        expected.collection = if target {
            plan.target.clone()
        } else {
            intent.entry.bin_name.clone()
        };
        if **current != expected {
            return Err(SqlError::InvalidSql(format!(
                "conflicting pending restore index {}",
                current.name
            )));
        }
    }
    Ok(target)
}
fn rollback_restore(db: &mut BicDb, intent: &MoveIntent) -> Result<()> {
    let plan = intent.restore.as_ref().expect("restore intent");
    if restore_backing_state(db, intent)? {
        db.rename_collection(&plan.target, &intent.entry.bin_name)?;
    }
    let indexes = db.index_definitions();
    for (scoped, restored) in &plan.indexes {
        if scoped != restored && indexes.iter().any(|index| index.name == *restored) {
            if !db.rename_index(restored, scoped)? {
                return Err(SqlError::InvalidSql(format!(
                    "pending restore index {restored} disappeared"
                )));
            }
        }
    }
    delete_schema(db, &plan.target)?;
    for grant in &intent.entry.grants {
        let mut grant = grant.clone();
        grant.object_name = plan.target.clone();
        delete_privilege(db, &grant)?;
    }
    for trigger in &intent.entry.triggers {
        delete_trigger(db, &trigger.name, Some(&plan.target))?;
    }
    for sequence in &plan.sequences {
        delete_sequence(db, &sequence.name)?;
    }
    save_recycle_bin_entry(db, &intent.entry)?;
    clear(db, &plan.id)?;
    sql_index_definitions_cache_clear();
    Ok(())
}
pub(super) fn undo_restore(session: &mut SqlSession<'_>, id: &str) -> Result<()> {
    let record = session
        .db_ref()
        .get(INTENTS, id)?
        .ok_or_else(|| SqlError::InvalidSql("missing restore undo intent".into()))?;
    let intent: MoveIntent = serde_json::from_value(record.metadata.clone())?;
    let plan = intent
        .restore
        .as_ref()
        .ok_or_else(|| SqlError::InvalidSql("invalid restore undo intent".into()))?;
    if restore_backing_state(session.db_ref(), &intent)? {
        session.rename_recycle_collection(&plan.target, &intent.entry.bin_name)?;
    }
    rollback_restore(session.db_mut()?, &intent)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bicdb_core::{DbConfig, StorageMode};
    use std::process::Command;
    fn config(paged: bool) -> DbConfig {
        DbConfig::default().with_storage_mode(if paged {
            StorageMode::ServerPaged
        } else {
            StorageMode::EmbeddedMemory
        })
    }
    fn seed_crash_source(db: &mut BicDb) {
        let mut sql = SqlSession::new(db);
        for statement in [
            "CREATE ROLE cut_reader LOGIN NOSUPERUSER",
            "CREATE TABLE crash_source(k int PRIMARY KEY,v text,seq_id serial)",
            "CREATE INDEX crash_source_v ON crash_source(v)",
            "GRANT SELECT ON crash_source TO cut_reader",
            "CREATE FUNCTION cut_trigger_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$",
            "CREATE TRIGGER cut_trigger BEFORE UPDATE ON crash_source FOR EACH ROW EXECUTE FUNCTION cut_trigger_fn()",
            "INSERT INTO crash_source(k,v) VALUES(1,'acknowledged')",
        ] {sql.execute(statement).unwrap_or_else(|error|panic!("{statement}: {error}"));}
    }
    fn assert_restored_metadata(db: &BicDb) {
        assert!(list_privileges(db)
            .unwrap()
            .iter()
            .any(|grant| grant.object_name == "crash_source" && grant.grantee == "cut_reader"));
        assert!(list_triggers(db)
            .unwrap()
            .iter()
            .any(|trigger| trigger.table_name == "crash_source" && trigger.name == "cut_trigger"));
        assert!(list_sequences(db).unwrap().iter().any(|sequence| sequence
            .owned_by_table
            .as_deref()
            == Some("crash_source")
            && sequence.owned_by_column.as_deref() == Some("seq_id")));
        assert!(load_schema(db, "crash_source")
            .unwrap()
            .unwrap()
            .columns
            .iter()
            .any(|column| column.name == "seq_id" && column.default_sequence.is_some()));
    }
    #[test]
    fn crash_cut_child() {
        let Ok(path) = std::env::var("BICDB_TEST_RECYCLE_PATH") else {
            return;
        };
        let paged = std::env::var("BICDB_TEST_RECYCLE_PAGED").unwrap() == "true";
        let explicit = std::env::var("BICDB_TEST_RECYCLE_EXPLICIT").unwrap() == "true";
        let mut db = BicDb::open_with_config(path, config(paged)).unwrap();
        let mut session = SqlSession::new(&mut db);
        let cut = std::env::var("BICDB_TEST_RECYCLE_CUT").unwrap();
        if cut.starts_with("recreate_") {
            session.execute("BEGIN").unwrap();
            session.execute("SAVEPOINT before_recreate").unwrap();
            session.execute("DROP TABLE crash_source").unwrap();
            session
                .execute("CREATE TABLE crash_source(k int PRIMARY KEY,v text)")
                .unwrap();
            session
                .execute("INSERT INTO crash_source VALUES(2,'replacement')")
                .unwrap();
            if cut.starts_with("recreate_repeat_") {
                session.execute("DROP TABLE crash_source").unwrap();
                session
                    .execute("CREATE TABLE crash_source(k int PRIMARY KEY,v text)")
                    .unwrap();
                session
                    .execute("INSERT INTO crash_source VALUES(3,'second replacement')")
                    .unwrap();
            }
            if cut.ends_with("_commit") {
                session.execute("COMMIT").unwrap();
            } else if cut.ends_with("_rollback") {
                session.execute("ROLLBACK TO before_recreate").unwrap();
            }
            std::process::exit(99);
        }
        if cut.starts_with("restore_") {
            session.execute("DROP TABLE crash_source").unwrap();
            if explicit {
                session.execute("BEGIN").unwrap();
                session.execute("SAVEPOINT before_restore").unwrap();
            }
            session
                .execute("FLASHBACK TABLE crash_source TO BEFORE DROP")
                .unwrap();
            if cut == "restore_commit" {
                session.execute("COMMIT").unwrap();
            }
            if cut == "restore_rollback" {
                session.execute("ROLLBACK TO before_restore").unwrap();
            }
            std::process::exit(99);
        }
        if cut.starts_with("cycle_") {
            session.execute("BEGIN").unwrap();
            session.execute("SAVEPOINT before_cycle").unwrap();
            session.execute("DROP TABLE crash_source").unwrap();
            session
                .execute("FLASHBACK TABLE crash_source TO BEFORE DROP")
                .unwrap();
            session.execute("DROP TABLE crash_source").unwrap();
            if cut == "cycle_commit" {
                session.execute("COMMIT").unwrap();
            }
            if cut == "cycle_rollback" {
                session.execute("ROLLBACK TO before_cycle").unwrap();
            }
            std::process::exit(99);
        }
        if explicit {
            session.execute("BEGIN").unwrap();
            session.execute("SAVEPOINT before_drop").unwrap();
            session
                .execute("UPDATE crash_source SET v='pending' WHERE k=1")
                .unwrap();
        }
        session.execute("DROP TABLE crash_source").unwrap();
        let stage = std::env::var("BICDB_TEST_RECYCLE_CUT").unwrap();
        if stage.starts_with("purge_") {
            session.execute("PURGE RECYCLEBIN").unwrap();
        }
        if stage == "commit" {
            session.execute("COMMIT").unwrap();
        }
        if stage == "rollback" {
            session.execute("ROLLBACK TO before_drop").unwrap();
        }
        std::process::exit(99);
    }
    #[test]
    fn process_crash_recreated_name_restores_only_its_proven_backing() {
        for paged in [false, true] {
            for stage in [
                "recreate_intent",
                "recreate_backing",
                "recreate_name",
                "recreate_decision",
                "recreate_pending",
                "recreate_commit",
                "recreate_rollback",
                "recreate_repeat_pending",
                "recreate_repeat_commit",
                "recreate_repeat_rollback",
            ] {
                let root = tempfile::tempdir().unwrap();
                {
                    let mut db = BicDb::open_with_config(root.path(), config(paged)).unwrap();
                    seed_crash_source(&mut db);
                }
                let output = Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "session::recycle_bin_journal::tests::crash_cut_child",
                        "--nocapture",
                    ])
                    .env("BICDB_TEST_RECYCLE_PATH", root.path())
                    .env("BICDB_TEST_RECYCLE_PAGED", paged.to_string())
                    .env("BICDB_TEST_RECYCLE_EXPLICIT", "true")
                    .env("BICDB_TEST_RECYCLE_CUT", stage)
                    .output()
                    .unwrap();
                assert_eq!(
                    output.status.code(),
                    Some(99),
                    "{paged}/{stage}: {} {}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
                for reopen in 0..2 {
                    let mut db = BicDb::open_with_config(root.path(), config(paged)).unwrap();
                    super::super::recycle_bin::recover_interrupted_recycle_bin_renames(&mut db)
                        .unwrap_or_else(|error| panic!("{paged}/{stage}/{reopen}: {error}"));
                    ensure_primary_key_indexes(&mut db).unwrap();
                    let mut sql = SqlSession::new(&mut db);
                    let (id, value) = if stage == "recreate_repeat_commit" {
                        (3, "second replacement")
                    } else if stage == "recreate_commit" {
                        (2, "replacement")
                    } else {
                        (1, "acknowledged")
                    };
                    assert_eq!(
                        sql.execute("SELECT k,v FROM crash_source").unwrap().rows,
                        vec![vec![SqlValue::Int(id), SqlValue::String(value.into())]],
                        "{paged}/{stage}/{reopen}"
                    );
                    if !stage.ends_with("_commit") {
                        assert_restored_metadata(sql.db_ref());
                    }
                    assert_eq!(
                        sql.execute(&format!(
                            "INSERT INTO crash_source(k,v) VALUES({id},'duplicate')"
                        ))
                        .unwrap_err()
                        .sqlstate(),
                        "23505"
                    );
                }
            }
        }
    }

    #[test]
    fn process_crash_every_move_stage_and_transaction_decision() {
        for paged in [false, true] {
            for explicit in [false, true] {
                let mut stages = vec![
                    "intent",
                    "collection",
                    "index",
                    "schema",
                    "dependents",
                    "entry",
                    "decision",
                ];
                if explicit {
                    stages.extend(["commit", "rollback"]);
                }
                for stage in stages {
                    let root = tempfile::tempdir().unwrap();
                    {
                        let mut db = BicDb::open_with_config(root.path(), config(paged)).unwrap();
                        seed_crash_source(&mut db);
                    }
                    let output = Command::new(std::env::current_exe().unwrap())
                        .args([
                            "--exact",
                            "session::recycle_bin_journal::tests::crash_cut_child",
                            "--nocapture",
                        ])
                        .env("BICDB_TEST_RECYCLE_PATH", root.path())
                        .env("BICDB_TEST_RECYCLE_PAGED", paged.to_string())
                        .env("BICDB_TEST_RECYCLE_EXPLICIT", explicit.to_string())
                        .env("BICDB_TEST_RECYCLE_CUT", stage)
                        .output()
                        .unwrap();
                    assert_eq!(
                        output.status.code(),
                        Some(99),
                        "{paged}/{explicit}/{stage}: {} {}",
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr)
                    );
                    let committed = stage == "commit" || (!explicit && stage == "decision");
                    for reopen in 0..2 {
                        let mut db = BicDb::open_with_config(root.path(), config(paged)).unwrap();
                        super::super::recycle_bin::recover_interrupted_recycle_bin_renames(&mut db)
                            .unwrap_or_else(|e| panic!("{paged}/{explicit}/{stage}/{reopen}: {e}"));
                        ensure_primary_key_indexes(&mut db).unwrap();
                        let mut sql = SqlSession::new(&mut db);
                        if committed && reopen == 0 {
                            assert!(sql.execute("SELECT * FROM crash_source").is_err());
                            sql.execute("FLASHBACK TABLE crash_source TO BEFORE DROP")
                                .unwrap();
                        }
                        let rows = sql.execute("SELECT k,v FROM crash_source").unwrap().rows;
                        assert_eq!(
                            rows,
                            vec![vec![
                                SqlValue::Int(1),
                                SqlValue::String(
                                    if committed && explicit {
                                        "pending"
                                    } else {
                                        "acknowledged"
                                    }
                                    .into()
                                )
                            ]],
                            "{paged}/{explicit}/{stage}/{reopen}"
                        );
                        assert_restored_metadata(sql.db_ref());
                        assert_eq!(
                            sql.execute("INSERT INTO crash_source(k,v) VALUES(1,'duplicate')")
                                .unwrap_err()
                                .sqlstate(),
                            "23505"
                        );
                    }
                }
            }
        }
    }
    #[test]
    fn process_crash_purge_intent_finishes_without_startup_blocker() {
        for paged in [false, true] {
            for stage in ["purge_intent", "purge_collection", "purge_entry"] {
                let root = tempfile::tempdir().unwrap();
                {
                    let mut db = BicDb::open_with_config(root.path(), config(paged)).unwrap();
                    seed_crash_source(&mut db);
                }
                let output = Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "session::recycle_bin_journal::tests::crash_cut_child",
                        "--nocapture",
                    ])
                    .env("BICDB_TEST_RECYCLE_PATH", root.path())
                    .env("BICDB_TEST_RECYCLE_PAGED", paged.to_string())
                    .env("BICDB_TEST_RECYCLE_EXPLICIT", "false")
                    .env("BICDB_TEST_RECYCLE_CUT", stage)
                    .output()
                    .unwrap();
                assert_eq!(
                    output.status.code(),
                    Some(99),
                    "{paged}/{stage}: {} {}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
                for _ in 0..2 {
                    let mut db = BicDb::open_with_config(root.path(), config(paged)).unwrap();
                    super::super::recycle_bin::recover_interrupted_recycle_bin_renames(&mut db)
                        .unwrap();
                    ensure_primary_key_indexes(&mut db).unwrap();
                    assert!(list_recycle_bin(&db).unwrap().is_empty());
                    assert!(!db
                        .collections()
                        .iter()
                        .any(|meta| meta.name == "crash_source"
                            || meta.name.starts_with("__bicdb_bin_")));
                    assert!(SqlSession::new(&mut db)
                        .execute("SELECT * FROM crash_source")
                        .is_err());
                }
            }
        }
    }
    #[test]
    fn process_crash_every_restore_stage_and_repeated_ddl() {
        for paged in [false, true] {
            for explicit in [false, true] {
                let mut stages = vec![
                    "restore_intent",
                    "restore_collection",
                    "restore_index",
                    "restore_schema",
                    "restore_dependents",
                    "restore_entry",
                    "restore_decision",
                ];
                if explicit {
                    stages.extend([
                        "restore_commit",
                        "restore_rollback",
                        "cycle_pending",
                        "cycle_commit",
                        "cycle_rollback",
                    ]);
                }
                for stage in stages {
                    let root = tempfile::tempdir().unwrap();
                    {
                        let mut db = BicDb::open_with_config(root.path(), config(paged)).unwrap();
                        seed_crash_source(&mut db);
                    }
                    let output = Command::new(std::env::current_exe().unwrap())
                        .args([
                            "--exact",
                            "session::recycle_bin_journal::tests::crash_cut_child",
                            "--nocapture",
                        ])
                        .env("BICDB_TEST_RECYCLE_PATH", root.path())
                        .env("BICDB_TEST_RECYCLE_PAGED", paged.to_string())
                        .env("BICDB_TEST_RECYCLE_EXPLICIT", explicit.to_string())
                        .env("BICDB_TEST_RECYCLE_CUT", stage)
                        .output()
                        .unwrap();
                    assert_eq!(
                        output.status.code(),
                        Some(99),
                        "{paged}/{explicit}/{stage}: {} {}",
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr)
                    );
                    let restored = stage == "restore_commit"
                        || (!explicit && stage == "restore_decision")
                        || stage == "cycle_pending"
                        || stage == "cycle_rollback";
                    for reopen in 0..2 {
                        let mut db = BicDb::open_with_config(root.path(), config(paged)).unwrap();
                        super::super::recycle_bin::recover_interrupted_recycle_bin_renames(&mut db)
                            .unwrap_or_else(|error| {
                                panic!("{paged}/{explicit}/{stage}/{reopen}: {error}")
                            });
                        ensure_primary_key_indexes(&mut db).unwrap();
                        let mut sql = SqlSession::new(&mut db);
                        if !restored && reopen == 0 {
                            assert!(
                                sql.execute("SELECT * FROM crash_source").is_err(),
                                "{paged}/{explicit}/{stage}"
                            );
                            sql.execute("FLASHBACK TABLE crash_source TO BEFORE DROP")
                                .unwrap();
                        }
                        assert_eq!(
                            sql.execute("SELECT k,v FROM crash_source").unwrap().rows,
                            vec![vec![
                                SqlValue::Int(1),
                                SqlValue::String("acknowledged".into())
                            ]],
                            "{paged}/{explicit}/{stage}/{reopen}"
                        );
                        assert_restored_metadata(sql.db_ref());
                        assert_eq!(
                            sql.execute("INSERT INTO crash_source(k,v) VALUES(1,'duplicate')")
                                .unwrap_err()
                                .sqlstate(),
                            "23505"
                        );
                    }
                }
            }
        }
    }
}
