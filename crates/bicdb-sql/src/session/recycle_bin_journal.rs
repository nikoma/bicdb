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
pub(super) fn clear_restored(session: &mut SqlSession<'_>, bin: &str) -> Result<()> {
    if session.tx.is_some() {
        match session.db_ref().get(INTENTS, bin) {
            Ok(Some(_)) => {}
            Ok(None) | Err(BicDbError::CollectionNotFound(_)) => return Ok(()),
            Err(error) => return Err(error.into()),
        }
        // The caller may own a pending decision record; never acquire its
        // lock from an independent cleanup transaction.
        session.tx.as_mut().unwrap().delete(DECISIONS, bin)?;
        session.db_mut()?.delete(INTENTS, bin)?;
        Ok(())
    } else {
        clear_if_present(session.db_mut()?, bin)
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    fn config(paged: bool) -> DbConfig {
        DbConfig::default().with_storage_mode(if paged {
            StorageMode::ServerPaged
        } else {
            StorageMode::EmbeddedMemory
        })
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
        if explicit {
            session.execute("BEGIN").unwrap();
            session.execute("SAVEPOINT before_drop").unwrap();
            session
                .execute("UPDATE crash_source SET v='pending' WHERE k=1")
                .unwrap();
        }
        session.execute("DROP TABLE crash_source").unwrap();
        let stage = std::env::var("BICDB_TEST_RECYCLE_CUT").unwrap();
        if stage == "commit" {
            session.execute("COMMIT").unwrap();
        }
        if stage == "rollback" {
            session.execute("ROLLBACK TO before_drop").unwrap();
        }
        std::process::exit(99);
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
                        let mut sql = SqlSession::new(&mut db);
                        for statement in [
                            "CREATE TABLE crash_source(k int PRIMARY KEY,v text)",
                            "CREATE INDEX crash_source_v ON crash_source(v)",
                            "INSERT INTO crash_source VALUES(1,'acknowledged')",
                        ] {
                            sql.execute(statement).unwrap();
                        }
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
                        assert_eq!(
                            sql.execute("INSERT INTO crash_source VALUES(1,'duplicate')")
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
