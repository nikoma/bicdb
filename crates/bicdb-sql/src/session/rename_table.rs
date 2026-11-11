//! SQL rename producer for the durable before-image journal. This prototype
//! retains conservative native quiescence checks. Scoped admission, concurrent
//! transaction visibility and bounded value mutation remain qualification gates.
use super::*;

#[inline]
pub(super) fn stage(_name: &str) {
    #[cfg(test)]
    if std::env::var("BICDB_TEST_RENAME_CUT").ok().as_deref() == Some(_name) {
        std::process::exit(99);
    }
}

fn move_source(session: &mut SqlSession<'_>, old: &str, new: &str) -> Result<()> {
    let mut owner = session.tx.take();
    let result = session.db_mut().and_then(|db| match owner.as_mut() {
        Some(tx) => db
            .rename_collection_preserving_pending_writes(tx, old, new)
            .map_err(Into::into),
        None => db.rename_collection(old, new).map_err(Into::into),
    });
    // Refused moves must retain the caller's original transaction too.
    session.tx = owner;
    result
}

pub(super) fn apply(
    session: &mut SqlSession<'_>,
    old: &str,
    new: &str,
    schema: &mut TableSchema,
) -> Result<()> {
    require_user_relation_name(new)?;
    if user_collection_names(session.db_ref())
        .iter()
        .any(|name| name == new)
        || load_schema(session.db_ref(), new)?.is_some()
        || load_view(session.db_ref(), new)?.is_some()
        || load_sequence(session.db_ref(), new)?.is_some()
    {
        return Err(SqlError::InvalidSql(format!(
            "relation \"{new}\" already exists"
        )));
    }
    // Fail shared-session fallback before publishing a journal or format flag.
    session.db_mut()?;
    match session.tx.as_ref() {
        Some(tx) => session.db_ref().check_catalog_undo_owner(tx)?,
        None => session.db_ref().check_catalog_recovery_quiescent()?,
    }
    let previous = schema.clone();
    let mut updated = previous.clone();
    // Legacy descriptors derive these identities from the original name.
    // Persist them before changing that spelling, including the array type.
    updated.row_type_oid = Some(previous.row_type_oid());
    updated.row_array_type_oid = Some(previous.row_array_type_oid());
    updated.name = new.into();
    if updated.primary_key_column().is_some() {
        if updated.primary_key_name.is_none() {
            updated.primary_key_name = Some(previous.primary_key_constraint_name());
        }
    } else if updated.implicit_primary_key_name.is_none() {
        updated.implicit_primary_key_name = Some(previous.primary_key_constraint_name());
    }
    // Name/type binding must still see the source's original spelling.
    let views = rename_views::plan_queries(session.db_ref(), old, new)?;
    let dependencies =
        rename_dependencies::RenameDependencies::plan(session.db_ref(), &previous, &updated)?;
    let prepared = dependencies.capture(session.db_mut()?, old, new)?;
    stage("captured");
    let explicit = session.tx.is_some();
    if explicit {
        session.ddl_undo.push(DdlUndo::RestoreRenameJournal {
            id: prepared.id().into(),
            old_table: old.into(),
        });
    }
    let result = (|| -> Result<()> {
        move_source(session, old, new)?;
        stage("backing");
        delete_schema(session.db_mut()?, old)?;
        stage("old_schema_removed");
        *schema = updated;
        session.save_session_schema(schema)?;
        stage("new_schema_saved");
        for mut sequence in list_sequences(session.db_ref())? {
            if sequence.owned_by_table.as_deref() == Some(old) {
                sequence.owned_by_table = Some(new.into());
                session.save_session_sequence(&sequence)?;
            }
        }
        stage("sequences");
        for mut dependent in list_schemas(session.db_ref())? {
            let mut changed = false;
            for constraint in &mut dependent.constraints {
                if let ConstraintSchema::ForeignKey { foreign_table, .. } = constraint {
                    if foreign_table == old {
                        *foreign_table = new.into();
                        changed = true;
                    }
                }
            }
            if let Some(partition) = &mut dependent.partition_of {
                if partition.parent_table == old {
                    partition.parent_table = new.into();
                    changed = true;
                }
            }
            if changed {
                session.save_session_schema(&dependent)?;
                if dependent.name == new {
                    *schema = dependent;
                }
            }
        }
        stage("dependencies");
        for mut grant in list_privileges(session.db_ref())? {
            if grant.object_type == PrivilegeObjectType::Table && grant.object_name == old {
                session.delete_session_privilege(&grant)?;
                grant.object_name = new.into();
                session.save_session_privilege(&grant)?;
            }
        }
        for mut trigger in list_triggers(session.db_ref())? {
            if trigger.table_name == old {
                session.delete_session_trigger(&trigger.name, Some(old))?;
                trigger.table_name = new.into();
                session.save_session_trigger(&trigger)?;
            }
        }
        for view in views {
            session.save_session_view(&view)?;
        }
        stage("views");
        let name = table_row_type_schema(schema)
            .column_type(false)
            .formatted_name();
        session.apply_table_row_type_change(
            &previous,
            schema,
            &CompositeAttributeMutation::RenameType { name },
        )?;
        stage("values_complete");
        sql_index_definitions_cache_clear();
        Ok(())
    })();
    if let Err(error) = result {
        if !explicit {
            // If durability is fenced, this refuses and retains the manifest
            // for startup recovery. Never erase uncertain durable evidence.
            rename_journal::rollback(session.db_mut()?, prepared.id())?;
        }
        return Err(error);
    }
    // The manifest decision belongs to the business transaction. A commit
    // error is propagated directly, never reinterpreted as a rollback.
    match session.tx.as_mut() {
        Some(tx) => rename_journal::decide(&prepared, tx)?,
        None => {
            let mut tx = session.db_ref().begin_transaction()?;
            rename_journal::decide(&prepared, &mut tx)?;
            tx.commit()?;
        }
    }
    stage(if explicit {
        "decision_staged"
    } else {
        "decision_committed"
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bicdb_core::{DbConfig, StorageMode};

    fn config(paged: bool) -> DbConfig {
        DbConfig::default().with_storage_mode(if paged {
            StorageMode::ServerPaged
        } else {
            StorageMode::EmbeddedMemory
        })
    }

    #[test]
    fn sql_rename_cut_child() {
        let Ok(path) = std::env::var("BICDB_TEST_RENAME_PATH") else {
            return;
        };
        let paged = std::env::var("BICDB_TEST_RENAME_PAGED").unwrap() == "true";
        let explicit = std::env::var("BICDB_TEST_RENAME_EXPLICIT").unwrap() == "true";
        let cut = std::env::var("BICDB_TEST_RENAME_CUT").unwrap();
        let mut db = BicDb::open_with_config(&path, config(paged)).unwrap();
        oracle::initialize_existing_database_profile(&mut db).unwrap();
        let mut session = SqlSession::new(&mut db);
        if explicit {
            session
                .execute(
                    "BEGIN; UPDATE original SET payload='pending-business-write' WHERE rowkey=1; SAVEPOINT before_rename",
                )
                .unwrap();
        }
        session
            .execute("ALTER TABLE original RENAME TO renamed")
            .unwrap();
        if cut.starts_with("reuse_") {
            assert!(explicit);
            session.execute("CREATE TABLE original(rowkey INT PRIMARY KEY,payload TEXT); INSERT INTO original VALUES(2,'replacement')").unwrap();
            if cut == "reuse_rollback" {
                session
                    .execute("ROLLBACK TO before_rename; COMMIT")
                    .unwrap();
            } else if cut == "reuse_commit" {
                session.execute("COMMIT").unwrap();
            }
            if cut != "reuse_pending" {
                let mut acknowledged = BTreeMap::new();
                for name in if cut == "reuse_commit" {
                    vec!["original", "renamed"]
                } else {
                    vec!["original"]
                } {
                    let rows = session
                        .execute(&format!(
                            "SELECT rowkey,payload FROM {name} ORDER BY rowkey"
                        ))
                        .unwrap()
                        .rows;
                    acknowledged.insert(
                        name,
                        rows.into_iter()
                            .map(|row| {
                                row.into_iter()
                                    .map(|value| value.to_cell())
                                    .collect::<Vec<_>>()
                            })
                            .collect::<Vec<_>>(),
                    );
                }
                let ack = std::path::Path::new(&path).join(if cut == "reuse_commit" {
                    "client-rename-ack"
                } else {
                    "client-business-ack"
                });
                std::fs::write(&ack, serde_json::to_vec(&acknowledged).unwrap()).unwrap();
                std::fs::File::open(&ack).unwrap().sync_all().unwrap();
                std::fs::File::open(&path).unwrap().sync_all().unwrap();
            }
            stage(&cut);
            panic!("reuse cut was not reached");
        }
        if explicit {
            session.execute("COMMIT").unwrap();
        }
        // A separate fsynced client ledger distinguishes acknowledged rename
        // from the committed-but-not-yet-acknowledged decision cut.
        let ack = std::path::Path::new(&path).join("client-rename-ack");
        std::fs::write(&ack, b"rename-ack\n").unwrap();
        std::fs::File::open(&ack).unwrap().sync_all().unwrap();
        std::fs::File::open(&path).unwrap().sync_all().unwrap();
        stage("client_ack");
        panic!("requested cut was never reached");
    }

    fn contains_old_type_name(value: &JsonValue) -> bool {
        match value {
            JsonValue::Object(fields) => fields.iter().any(|(key, value)| {
                (key == "type_name" && value.as_str() == Some("original"))
                    || contains_old_type_name(value)
            }),
            JsonValue::Array(values) => values.iter().any(contains_old_type_name),
            _ => false,
        }
    }

    #[test]
    fn sql_rename_process_cuts_restore_complete_catalog_and_acknowledged_rows() {
        for paged in [false, true] {
            for explicit in [false, true] {
                let mut cuts = vec![
                    "captured",
                    "backing",
                    "old_schema_removed",
                    "new_schema_saved",
                    "sequences",
                    "dependencies",
                    "views",
                    "value_row",
                    "values_complete",
                    "client_ack",
                ];
                cuts.push(if explicit {
                    "decision_staged"
                } else {
                    "decision_committed"
                });
                if explicit {
                    cuts.extend(["reuse_pending", "reuse_commit", "reuse_rollback"]);
                }
                for cut in cuts {
                    let root = tempfile::tempdir().unwrap();
                    let mut db = BicDb::open_with_config(root.path(), config(paged)).unwrap();
                    {
                        let mut session = SqlSession::new(&mut db);
                        for sql in [
                            "CREATE ROLE rename_reader LOGIN NOSUPERUSER",
                            "CREATE TABLE original(rowkey INT PRIMARY KEY,payload TEXT,seqid SERIAL)",
                            "INSERT INTO original(rowkey,payload) VALUES(1,'acknowledged')",
                            "CREATE TABLE child(id INT PRIMARY KEY,parent INT REFERENCES original(rowkey))",
                            "INSERT INTO child VALUES(1,1)",
                            "CREATE TABLE holder(id INT PRIMARY KEY,item original,items original[])",
                            "CREATE VIEW direct_row AS SELECT original AS item FROM original",
                            "CREATE VIEW indirect_row AS SELECT item FROM direct_row",
                            "CREATE MATERIALIZED VIEW stored_row AS SELECT item FROM direct_row",
                            "GRANT SELECT ON original TO rename_reader",
                            "CREATE FUNCTION rename_trigger_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$",
                            "CREATE TRIGGER rename_guard BEFORE UPDATE ON original FOR EACH ROW EXECUTE FUNCTION rename_trigger_fn()",
                        ] { session.execute(sql).unwrap_or_else(|error| panic!("{sql}: {error}")); }
                        for id in 1..=8 {
                            session.execute(&format!("INSERT INTO holder VALUES({id},ROW(1,'item_{id}',1)::original,ARRAY[ROW(1,'array_{id}',1)::original])")).unwrap();
                        }
                    }
                    let identity = load_schema(&db, "original")
                        .unwrap()
                        .unwrap()
                        .row_type_oid();
                    let before = db
                        .scan_collection("holder")
                        .unwrap()
                        .into_iter()
                        .map(|row| (row.id, row.metadata))
                        .collect::<BTreeMap<_, _>>();
                    db.close().unwrap();
                    let output = std::process::Command::new(std::env::current_exe().unwrap())
                        .args([
                            "--exact",
                            "session::rename_table::tests::sql_rename_cut_child",
                            "--nocapture",
                        ])
                        .env("BICDB_TEST_RENAME_PATH", root.path())
                        .env("BICDB_TEST_RENAME_PAGED", paged.to_string())
                        .env("BICDB_TEST_RENAME_EXPLICIT", explicit.to_string())
                        .env("BICDB_TEST_RENAME_CUT", cut)
                        .output()
                        .unwrap();
                    assert_eq!(
                        output.status.code(),
                        Some(99),
                        "paged={paged} explicit={explicit} cut={cut}\n{}\n{}",
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr)
                    );
                    let committed =
                        matches!(cut, "decision_committed" | "client_ack" | "reuse_commit");
                    let table = if committed { "renamed" } else { "original" };
                    let absent = if committed { "original" } else { "renamed" };
                    let payload = if explicit && (committed || cut == "reuse_rollback") {
                        "pending-business-write"
                    } else {
                        "acknowledged"
                    };
                    assert_eq!(
                        root.path().join("client-rename-ack").exists(),
                        matches!(cut, "client_ack" | "reuse_commit")
                    );
                    assert_eq!(
                        root.path().join("client-business-ack").exists(),
                        cut == "reuse_rollback"
                    );
                    if cut == "reuse_commit" || cut == "reuse_rollback" {
                        let ledger = root.path().join(if cut == "reuse_commit" {
                            "client-rename-ack"
                        } else {
                            "client-business-ack"
                        });
                        let ledger: JsonValue =
                            serde_json::from_slice(&std::fs::read(ledger).unwrap()).unwrap();
                        let expected = if cut == "reuse_commit" {
                            serde_json::json!({"original":[["2","replacement"]],"renamed":[["1","pending-business-write"]]})
                        } else {
                            serde_json::json!({"original":[["1","pending-business-write"]]})
                        };
                        assert_eq!(ledger, expected);
                    }
                    for reopen in 0..2 {
                        let mut db = BicDb::open_with_config(root.path(), config(paged)).unwrap();
                        oracle::initialize_existing_database_profile(&mut db).unwrap();
                        {
                            let mut session = SqlSession::new(&mut db);
                            assert_eq!(
                                session
                                    .execute(&format!("SELECT rowkey,payload FROM {table}"))
                                    .unwrap()
                                    .rows,
                                vec![vec![SqlValue::Int(1), SqlValue::String(payload.into())]],
                                "paged={paged} explicit={explicit} cut={cut} reopen={reopen}"
                            );
                            if cut == "reuse_commit" {
                                assert_eq!(
                                    session
                                        .execute("SELECT rowkey,payload FROM original")
                                        .unwrap()
                                        .rows,
                                    vec![vec![
                                        SqlValue::Int(2),
                                        SqlValue::String("replacement".into())
                                    ]]
                                );
                            } else {
                                assert!(session
                                    .execute(&format!("SELECT rowkey FROM {absent}"))
                                    .is_err());
                            }
                            let expected = (1..=8)
                                .map(|id| {
                                    vec![
                                        SqlValue::String(table.into()),
                                        SqlValue::String(format!("item_{id}")),
                                        SqlValue::String(format!("array_{id}")),
                                    ]
                                })
                                .collect::<Vec<_>>();
                            assert_eq!(session.execute("SELECT pg_typeof(item),(item).payload,(items[1]).payload FROM holder ORDER BY id").unwrap().rows,expected);
                            for view in ["direct_row", "indirect_row", "stored_row"] {
                                assert_eq!(
                                    session
                                        .execute(&format!(
                                            "SELECT (item).payload,pg_typeof(item) FROM {view}"
                                        ))
                                        .unwrap()
                                        .rows,
                                    vec![vec![
                                        SqlValue::String(payload.into()),
                                        SqlValue::String(table.into())
                                    ]]
                                );
                            }
                            assert_eq!(
                                session
                                    .execute(&format!(
                                        "INSERT INTO child VALUES({},999)",
                                        100 + reopen
                                    ))
                                    .unwrap_err()
                                    .sqlstate(),
                                "23503"
                            );
                            session
                                .execute(&format!("INSERT INTO child VALUES({},1)", 10 + reopen))
                                .unwrap();
                        }
                        assert_eq!(
                            load_schema(&db, table).unwrap().unwrap().row_type_oid(),
                            identity
                        );
                        let rows = db
                            .scan_collection("holder")
                            .unwrap()
                            .into_iter()
                            .map(|row| (row.id, row.metadata))
                            .collect::<BTreeMap<_, _>>();
                        if committed {
                            assert!(rows.values().all(|row| !contains_old_type_name(row)));
                        } else {
                            assert_eq!(
                                rows, before,
                                "complete original typed row images must survive"
                            );
                        }
                        assert!(list_privileges(&db)
                            .unwrap()
                            .iter()
                            .any(|g| g.object_name == table && g.grantee == "rename_reader"));
                        assert!(list_triggers(&db)
                            .unwrap()
                            .iter()
                            .any(|t| t.table_name == table && t.name == "rename_guard"));
                        assert!(list_sequences(&db)
                            .unwrap()
                            .iter()
                            .any(|s| s.owned_by_table.as_deref() == Some(table)));
                        assert!(
                            rename_journal::pending(&db).unwrap().is_empty(),
                            "paged={paged} explicit={explicit} cut={cut} reopen={reopen}"
                        );
                        assert!(db
                            .verify_indexes_consistently()
                            .unwrap()
                            .iter()
                            .all(|index| index.valid));
                        db.close().unwrap();
                    }
                }
            }
        }
    }

    #[test]
    fn legacy_name_derived_row_types_keep_their_oid_when_the_old_name_is_reused() {
        for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
            let root = tempfile::tempdir().unwrap();
            let config = DbConfig::default().with_storage_mode(mode);
            let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            SqlSession::new(&mut db).execute(
                "CREATE TABLE source(id INT PRIMARY KEY,payload TEXT); INSERT INTO source VALUES(1,'kept')").unwrap();
            let mut legacy = load_schema(&db, "source").unwrap().unwrap();
            legacy.row_type_oid = None;
            legacy.row_array_type_oid = None;
            let identity = (legacy.row_type_oid(), legacy.row_array_type_oid());
            save_schema(&mut db, &legacy).unwrap();
            {
                let mut session = SqlSession::new(&mut db);
                session.execute("CREATE VIEW observed AS SELECT source AS item FROM source; ALTER TABLE source RENAME TO renamed; CREATE TABLE source(id INT PRIMARY KEY,payload TEXT)").unwrap();
                assert_eq!(
                    session
                        .execute("SELECT (item).payload,pg_typeof(item) FROM observed")
                        .unwrap()
                        .rows,
                    vec![vec![
                        SqlValue::String("kept".into()),
                        SqlValue::String("renamed".into())
                    ]]
                );
            }
            let renamed = load_schema(&db, "renamed").unwrap().unwrap();
            assert_eq!(
                (renamed.row_type_oid(), renamed.row_array_type_oid()),
                identity
            );
            assert_ne!(
                load_schema(&db, "source").unwrap().unwrap().row_type_oid(),
                identity.0
            );
            db.close().unwrap();
            let mut db = BicDb::open_with_config(root.path(), config).unwrap();
            assert_eq!(
                SqlSession::new(&mut db)
                    .execute("SELECT (item).payload FROM observed")
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::String("kept".into())]]
            );
            db.close().unwrap();
        }
    }
}
