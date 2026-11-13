//! SQL rename producer for the durable before-image journal. Reserve the
//! dependency closure before capture and retain it until a durable decision
//! or complete undo. Replica and final application qualification remain gates.
use super::*;

#[cfg(test)]
mod partition_owner_tests;

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
    let previous = schema.clone();
    let mut updated = previous.clone();
    // Legacy descriptors derive these identities from the original name.
    // Persist them before changing that spelling, including the array type.
    updated.row_type_oid = Some(previous.row_type_oid());
    updated.row_array_type_oid = Some(previous.row_array_type_oid());
    updated.name = new.into();
    if updated.primary_key_name.is_none() {
        if updated.has_hidden_primary_key() {
            updated.implicit_primary_key_name = Some(previous.primary_key_constraint_name());
        } else if updated.primary_key_column().is_some() {
            updated.primary_key_name = Some(previous.primary_key_constraint_name());
        }
    }
    // Name/type binding must still see the source's original spelling.
    let views = rename_views::plan_queries(session.db_ref(), old, new)?;
    let dependencies =
        rename_dependencies::RenameDependencies::plan(session.db_ref(), &previous, &updated)?;
    // Namespace activation is a trusted initialization operation, never a
    // retroactive assignment to a live/undecided before-image.
    if !session.db_ref().local_namespace_identity_feature_active() {
        // A database no host initialized may meet its first rename inside
        // this session's own transaction; that owner alone may be pending.
        let owner = session.tx.take();
        let initialized = session.db_mut().and_then(|db| match &owner {
            Some(owner) => {
                rename_journal::initialize_rename_journal_identities_for_owner(db, owner)
            }
            None => rename_journal::initialize_rename_journal_identities(db),
        });
        session.tx = owner;
        initialized?;
    }
    let explicit = session.tx.is_some();
    let mut implicit_owner = if explicit {
        None
    } else {
        Some(session.begin_session_transaction()?)
    };
    let owner = session.tx.as_ref().or(implicit_owner.as_ref()).unwrap();
    let scope = dependencies.reserve(session.db_ref(), owner)?;
    let access = match session.db_ref().enter_catalog_scope(&scope) {
        Ok(access) => access,
        Err(error) => {
            let _ = owner.release_catalog_scope(&scope);
            return Err(error.into());
        }
    };
    let prepared = match dependencies.capture(session.db_mut()?, old, new) {
        Ok(prepared) => prepared,
        Err(error) => {
            drop(access);
            let owner = session.tx.as_ref().or(implicit_owner.as_ref()).unwrap();
            let _ = owner.release_catalog_scope(&scope);
            return Err(error);
        }
    };
    let owner = session.tx.as_ref().or(implicit_owner.as_ref()).unwrap();
    if let Err(error) = owner.retain_catalog_scope(&scope, prepared.id()) {
        rename_journal::discard_unapplied_capture(session.db_ref(), &prepared)?;
        drop(access);
        let _ = owner.release_catalog_scope(&scope);
        return Err(error.into());
    }
    stage("captured");
    let undo_len = session.ddl_undo.len();
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
        // The relation keeps its identity and ACL. Creation defaults may
        // have changed since it was created; applying them to this new name
        // would grant access that the original relation never had. Its exact
        // old/new catalog rows are already covered by the durable journal.
        save_schema(session.db_mut()?, schema)?;
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
        session.apply_journaled_table_row_type_rename(&previous, schema)?;
        stage("values_complete");
        sql_index_definitions_cache_clear();
        Ok(())
    })();
    if explicit {
        // The durable closure contains every effect above. Individual helper
        // undo entries would run without the journal's scope and duplicate
        // repair; retain exactly its one retryable journal entry instead.
        session.ddl_undo.truncate(undo_len + 1);
    }
    drop(access);
    if let Err(error) = result {
        if !explicit {
            // If durability is fenced, this refuses and retains the manifest
            // for startup recovery. Never erase uncertain durable evidence.
            rename_journal::rollback_owned(
                session.db_mut()?,
                prepared.id(),
                implicit_owner.as_mut().unwrap(),
            )?;
            implicit_owner.take().unwrap().rollback()?;
        }
        return Err(error);
    }
    // The manifest decision belongs to the business transaction. A commit
    // error is propagated directly, never reinterpreted as a rollback.
    match session.tx.as_mut() {
        Some(tx) => rename_journal::decide(&prepared, tx)?,
        None => {
            rename_journal::decide(&prepared, implicit_owner.as_mut().unwrap())?;
            implicit_owner.take().unwrap().commit()?;
            rename_journal::finish_committed(session.db_ref(), prepared.id())?;
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

    fn replica_config(paged: bool, role: bicdb_core::ReplicationMode) -> DbConfig {
        config(paged)
            .with_fsync(true)
            .with_replication(bicdb_core::ReplicationConfig {
                enabled: true,
                mode: role,
                cluster_id: "sql-startup".into(),
                listen_addr: Some("127.0.0.1:0".into()),
                tls: Some(bicdb_core::ReplicationTlsConfig {
                    cert_path: Default::default(),
                    key_path: Default::default(),
                    ca_path: Default::default(),
                    require_client_cert: true,
                    dev_localhost_plaintext: true,
                }),
                ..Default::default()
            })
    }

    fn ship(primary: &BicDb, standby: &mut BicDb) {
        let frames = primary
            .export_replication_frames_since(standby.last_applied_commit_seq(), 10_000)
            .unwrap();
        assert!(!frames.is_empty());
        standby.apply_replication_batch(&frames).unwrap();
    }

    fn journal_images(db: &BicDb) -> BTreeMap<String, JsonValue> {
        db.scan_collection("__bicdb_rename_images")
            .unwrap()
            .into_iter()
            .map(|record| (record.id, record.metadata))
            .collect()
    }

    fn bindings(db: &BicDb) -> BTreeMap<String, String> {
        db.collections()
            .into_iter()
            .map(|meta| (meta.name.clone(), meta.storage().to_string()))
            .collect()
    }

    fn concurrent_rename_with_native_writer(ending: &str) {
        let autocommit = matches!(ending, "autocommit" | "call");
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let mut db = BicDb::open_with_config(root.path(), config(paged)).unwrap();
            SqlSession::new(&mut db)
                .execute("CREATE TABLE source(id INT PRIMARY KEY,payload INT); INSERT INTO source VALUES(1,10); CREATE VIEW dependent AS SELECT source AS item FROM source")
                .unwrap();
            if ending == "call" {
                SqlSession::new(&mut db).execute("CREATE PROCEDURE rename_source() LANGUAGE plpgsql AS $$ DECLARE n INT; BEGIN SELECT count(*) INTO n FROM source; EXECUTE 'ALTER TABLE source RENAME TO renamed'; END; $$").unwrap();
            }
            db.create_collection("native_unrelated").unwrap();
            db.insert(
                "native_unrelated",
                Record::new("ack").with_metadata(serde_json::json!({"value": 10})),
            )
            .unwrap();
            rename_journal::initialize_rename_journal_identities(&mut db).unwrap();
            let mut unrelated = db.begin_transaction().unwrap();
            unrelated
                .update(
                    "native_unrelated",
                    Record::new("ack").with_metadata(serde_json::json!({"value": 99})),
                )
                .unwrap();
            {
                let mut sql = SqlSession::new(&mut db);
                if !autocommit {
                    sql.execute(
                        "BEGIN; UPDATE source SET payload=20 WHERE id=1; SAVEPOINT before_rename",
                    )
                    .unwrap();
                }
                sql.execute(if ending == "call" { "CALL rename_source()" } else { "ALTER TABLE source RENAME TO renamed" })
                    .unwrap_or_else(|error| {
                        panic!("paged={paged} ending={ending}: unrelated writer must not prevent rename: {error}")
                    });
                assert_eq!(
                    sql.execute("SELECT (item).payload FROM dependent")
                        .unwrap()
                        .rows,
                    vec![vec![SqlValue::Int(if autocommit { 10 } else { 20 })]]
                );
                // This actual native write/row lock predates the rename and
                // commits while the SQL owner still holds its catalog scope.
                unrelated.commit().unwrap();
                let later = sql.db_ref().begin_transaction().unwrap();
                assert_eq!(
                    later
                        .get("native_unrelated", "ack")
                        .unwrap()
                        .unwrap()
                        .metadata,
                    serde_json::json!({"value": 99})
                );
                if !autocommit {
                    assert!(later.scan_collection("renamed").is_err());
                    assert!(later.scan_collection("source").is_err());
                }
                later.rollback().unwrap();
                match ending {
                    "commit" => sql.execute("COMMIT").unwrap(),
                    "rollback" => sql.execute("ROLLBACK").unwrap(),
                    "savepoint" => sql.execute("ROLLBACK TO before_rename; COMMIT").unwrap(),
                    "autocommit" | "call" => SqlResult::command("SELECT"),
                    _ => unreachable!(),
                };
                let table = if matches!(ending, "commit" | "autocommit" | "call") {
                    "renamed"
                } else {
                    "source"
                };
                let expected = if matches!(ending, "commit" | "savepoint") {
                    20
                } else {
                    10
                };
                assert_eq!(
                    sql.execute(&format!("SELECT payload FROM {table}"))
                        .unwrap()
                        .rows,
                    vec![vec![SqlValue::Int(expected)]]
                );
                assert_eq!(
                    sql.execute("SELECT pg_typeof(item) FROM dependent")
                        .unwrap()
                        .rows,
                    vec![vec![SqlValue::String(table.into())]]
                );
            }
            // Finalization must release both names and dependent metadata.
            let observer = db.begin_transaction().unwrap();
            assert_eq!(
                observer.scan_collection("native_unrelated").unwrap().len(),
                1
            );
            observer.rollback().unwrap();
            db.close().unwrap();
        }
    }

    #[test]
    fn concurrent_rename_commits_with_unrelated_pending_native_writer() {
        concurrent_rename_with_native_writer("commit");
    }

    #[test]
    fn concurrent_rename_rolls_back_with_unrelated_pending_native_writer() {
        concurrent_rename_with_native_writer("rollback");
    }

    #[test]
    fn concurrent_rename_savepoint_preserves_prior_writes_and_unrelated_writer() {
        concurrent_rename_with_native_writer("savepoint");
    }

    #[test]
    fn concurrent_rename_autocommit_preserves_unrelated_pending_native_writer() {
        concurrent_rename_with_native_writer("autocommit");
    }

    #[test]
    fn concurrent_rename_call_keeps_its_preflight_owner_and_unrelated_writer() {
        concurrent_rename_with_native_writer("call");
    }

    #[cfg(feature = "adaptive-procs")]
    #[test]
    fn concurrent_rename_compiled_wrapper_releases_only_durably_decided_scope() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let mut db = BicDb::open_with_config(root.path(), config(paged)).unwrap();
            SqlSession::new(&mut db).execute("CREATE TABLE source(id INT PRIMARY KEY,payload INT); INSERT INTO source VALUES(1,10); CREATE VIEW dependent AS SELECT source AS item FROM source; CREATE PROCEDURE wasm_rename() LANGUAGE plpgsql AS $$ BEGIN ALTER TABLE source RENAME TO renamed; END; $$").unwrap();
            db.create_collection("native_unrelated").unwrap();
            rename_journal::initialize_rename_journal_identities(&mut db).unwrap();
            let routine = list_routines(&db)
                .unwrap()
                .into_iter()
                .find(|routine| routine.name == "wasm_rename")
                .unwrap();
            let mut ir = compile_routine_ir(&routine).unwrap();
            assert!(ir.params.is_empty() && ir.declarations.is_empty());
            // Exercise the existing integer prototype's host wrapper directly.
            // A spare unused slot makes a no-argument statement body lowerable;
            // declared-type guards and live adaptive selection are unchanged.
            ir.symbol_names.push("unused_test_slot".into());
            let compiled = adaptive::lower::get_or_build(
                adaptive::routine_signature("scope_wasm_rename", &[]),
                &ir,
            )
            .expect("statement-only fixture must compile");
            let mut unrelated = db.begin_transaction().unwrap();
            unrelated
                .insert("native_unrelated", Record::new("acknowledged"))
                .unwrap();
            {
                let mut sql = SqlSession::new(&mut db);
                sql.run_wasm_procedure_in_auto_transaction(&[], &compiled)
                    .unwrap();
                assert!(sql.take_pending_transaction().is_none());
                assert_eq!(
                    sql.execute("SELECT (item).payload FROM dependent")
                        .unwrap()
                        .rows,
                    vec![vec![SqlValue::Int(10)]]
                );
                unrelated.commit().unwrap();
            }
            assert!(db
                .get("native_unrelated", "acknowledged")
                .unwrap()
                .is_some());
            assert!(rename_journal::pending(&db).unwrap().is_empty());
            db.close().unwrap();
        }
    }

    #[test]
    fn standby_initialization_preserves_an_undecided_primary_rename_and_follows_its_decision() {
        use bicdb_core::ReplicationMode;
        for paged in [false, true] {
            for profile in [oracle::POSTGRES_PROFILE, oracle::ORACLE_HR_PROFILE] {
                for commit in [false, true] {
                    let root = tempfile::tempdir().unwrap();
                    let mut primary = BicDb::open_with_config(
                        root.path().join("primary"),
                        replica_config(paged, ReplicationMode::Primary),
                    )
                    .unwrap();
                    // This control follows a retained WAL prefix. Register a
                    // real bootstrap hold before schema/index checkpoints;
                    // the hold is not a durability acknowledgement.
                    primary.sync_quorum().hold("sql-startup-control", 0);
                    primary.persist_standby_slots().unwrap();
                    oracle::ensure_database_profile(&mut primary, profile).unwrap();
                    let standby_path = root.path().join("standby");
                    let mut standby = BicDb::open_with_config(
                        &standby_path,
                        replica_config(paged, ReplicationMode::Standby),
                    )
                    .unwrap();
                    {
                        let mut sql = SqlSession::new(&mut primary);
                        sql.execute("CREATE TABLE source(id INT PRIMARY KEY,payload TEXT); INSERT INTO source VALUES(1,'acknowledged'); BEGIN").unwrap();
                        // Exercise the host operation's undecided durable
                        // prefix under each persisted profile, independently
                        // of Oracle's separately qualified implicit-DDL rules.
                        let mut schema = load_schema(sql.db_ref(), "source").unwrap().unwrap();
                        apply(&mut sql, "source", "renamed", &mut schema).unwrap();
                        ship(sql.db_ref(), &mut standby);
                        let before = journal_images(&standby);
                        let before_bindings = bindings(&standby);
                        assert_eq!(rename_journal::pending(&standby).unwrap().len(), 1);
                        for reopen in [false, true] {
                            if reopen {
                                standby.close().unwrap();
                                standby = BicDb::open_with_config(
                                    &standby_path,
                                    replica_config(paged, ReplicationMode::Standby),
                                )
                                .unwrap();
                            }
                            oracle::initialize_existing_database_profile(&mut standby).unwrap_or_else(|error|panic!("paged={paged} {profile} commit={commit} reopen={reopen}: standby must follow a live intent without local undo: {error}"));
                            oracle::ensure_database_profile(&mut standby, profile).unwrap();
                            assert_eq!(journal_images(&standby), before);
                            assert_eq!(bindings(&standby), before_bindings);
                            assert_eq!(rename_journal::pending(&standby).unwrap().len(), 1);
                            assert!(standby.get("renamed", "1").unwrap().is_some());
                            let mut client = SqlSession::new_shared(&standby);
                            let error = client.execute("SELECT payload FROM renamed").unwrap_err();
                            assert_eq!(
                                error.sqlstate(),
                                "55000",
                                "do not expose intermediate source catalog state"
                            );
                            client.execute("BEGIN; ROLLBACK").unwrap();
                        }
                        sql.execute(if commit { "COMMIT" } else { "ROLLBACK" })
                            .unwrap();
                        ship(sql.db_ref(), &mut standby);
                    }
                    oracle::initialize_existing_database_profile(&mut standby).unwrap();
                    assert!(rename_journal::pending(&standby).unwrap().is_empty());
                    assert_eq!(
                        SqlSession::new_shared(&standby)
                            .execute(if commit {
                                "SELECT payload FROM renamed"
                            } else {
                                "SELECT payload FROM source"
                            })
                            .unwrap()
                            .rows,
                        vec![vec![SqlValue::String("acknowledged".into())]]
                    );
                    standby.close().unwrap();
                    primary.close().unwrap();
                }
            }
        }
    }

    #[test]
    fn promoted_primary_recovers_an_undecided_replicated_rename_before_sql_writes() {
        use bicdb_core::ReplicationMode;
        for paged in [false, true] {
            for profile in [oracle::POSTGRES_PROFILE, oracle::ORACLE_HR_PROFILE] {
                let root = tempfile::tempdir().unwrap();
                let mut primary = BicDb::open_with_config(
                    root.path().join("primary"),
                    replica_config(paged, ReplicationMode::Primary),
                )
                .unwrap();
                primary.sync_quorum().hold("sql-promotion-control", 0);
                primary.persist_standby_slots().unwrap();
                oracle::ensure_database_profile(&mut primary, profile).unwrap();
                let standby_path = root.path().join("standby");
                let mut standby = BicDb::open_with_config(
                    &standby_path,
                    replica_config(paged, ReplicationMode::Standby),
                )
                .unwrap();
                {
                    let mut sql = SqlSession::new(&mut primary);
                    sql.execute("CREATE TABLE source(id INT PRIMARY KEY,payload TEXT); INSERT INTO source VALUES(1,'acknowledged'); BEGIN; UPDATE source SET payload='uncommitted' WHERE id=1").unwrap();
                    let mut schema = load_schema(sql.db_ref(), "source").unwrap().unwrap();
                    // Persist and replicate the host operation's prefix, not
                    // an Oracle client statement with implicit-DDL commits.
                    apply(&mut sql, "source", "renamed", &mut schema).unwrap();
                    ship(sql.db_ref(), &mut standby);
                    // Stop the producer without publishing a SQL decision.
                    // Discarding its native owner does not undo catalog WAL.
                    sql.take_pending_transaction().unwrap().rollback().unwrap();
                }
                primary.close().unwrap();
                assert_eq!(rename_journal::pending(&standby).unwrap().len(), 1);
                assert_eq!(
                    SqlSession::new_shared(&standby)
                        .execute("SELECT payload FROM renamed")
                        .unwrap_err()
                        .sqlstate(),
                    "55000"
                );
                // This is the real persisted promotion/restart API, but a
                // local control, not a network election or VM-fault proof.
                standby
                    .promote_by_election(1, "sql-successor", None)
                    .unwrap();
                drop(standby);
                for reopen in [false, true] {
                    let mut promoted = BicDb::open_with_config(
                        &standby_path,
                        replica_config(paged, ReplicationMode::Primary),
                    )
                    .unwrap();
                    assert!(!promoted.is_standby_role());
                    assert_eq!(promoted.replication_epoch(), 1);
                    if reopen {
                        oracle::initialize_existing_database_profile(&mut promoted).unwrap();
                    } else {
                        oracle::ensure_database_profile(&mut promoted, profile).unwrap();
                    }
                    assert!(rename_journal::pending(&promoted).unwrap().is_empty());
                    let mut sql = SqlSession::new(&mut promoted);
                    assert_eq!(
                        sql.execute("SELECT payload FROM source WHERE id=1")
                            .unwrap()
                            .rows,
                        vec![vec![SqlValue::String("acknowledged".into())]],
                        "paged={paged} {profile} reopen={reopen}"
                    );
                    assert!(load_schema(sql.db_ref(), "renamed").unwrap().is_none());
                    if !reopen {
                        sql.execute("INSERT INTO source VALUES(2,'after-promotion')")
                            .unwrap();
                    }
                    assert_eq!(
                        sql.execute("SELECT payload FROM source WHERE id=2")
                            .unwrap()
                            .rows,
                        vec![vec![SqlValue::String("after-promotion".into())]]
                    );
                    drop(sql);
                    promoted.close().unwrap();
                }
            }
        }
    }

    #[test]
    fn configured_profile_initialization_recovers_an_undecided_primary_rename() {
        for paged in [false, true] {
            for profile in [oracle::POSTGRES_PROFILE, oracle::ORACLE_HR_PROFILE] {
                let root = tempfile::tempdir().unwrap();
                let mut db = BicDb::open_with_config(root.path(), config(paged)).unwrap();
                oracle::ensure_database_profile(&mut db, profile).unwrap();
                {
                    let mut sql = SqlSession::new(&mut db);
                    sql.execute("CREATE TABLE source(id INT PRIMARY KEY,payload TEXT); INSERT INTO source VALUES(1,'acknowledged'); BEGIN").unwrap();
                    let mut schema = load_schema(sql.db_ref(), "source").unwrap().unwrap();
                    apply(&mut sql, "source", "renamed", &mut schema).unwrap();
                    // Freeze the undecided durable prefix: discard only the
                    // native owner, without performing the host's DDL undo.
                    sql.take_pending_transaction().unwrap().rollback().unwrap();
                }
                assert_eq!(rename_journal::pending(&db).unwrap().len(), 1);
                db.close().unwrap();
                let mut db = BicDb::open_with_config(root.path(), config(paged)).unwrap();
                let before = bindings(&db);
                let wrong = if profile == oracle::POSTGRES_PROFILE {
                    oracle::ORACLE_HR_PROFILE
                } else {
                    oracle::POSTGRES_PROFILE
                };
                assert!(oracle::ensure_database_profile(&mut db, wrong).is_err());
                assert_eq!(
                    bindings(&db),
                    before,
                    "wrong serve profile must not repair the store"
                );
                assert_eq!(rename_journal::pending(&db).unwrap().len(), 1);
                oracle::ensure_database_profile(&mut db, profile).unwrap();
                assert!(
                    rename_journal::pending(&db).unwrap().is_empty(),
                    "paged={paged} {profile}: configured-profile startup skipped recovery"
                );
                assert_eq!(
                    SqlSession::new(&mut db)
                        .execute("SELECT payload FROM source")
                        .unwrap()
                        .rows,
                    vec![vec![SqlValue::String("acknowledged".into())]]
                );
                db.close().unwrap();
            }
        }
    }

    #[test]
    fn clean_postgres_profile_check_preserves_an_unrelated_live_native_transaction() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        db.create_collection("business").unwrap();
        SqlSession::new(&mut db).execute("CREATE TABLE history(id INT PRIMARY KEY); INSERT INTO history VALUES(1); ALTER TABLE history RENAME TO renamed_history").unwrap();
        let mut native = db.begin_transaction().unwrap();
        native.insert("business", Record::new("pending")).unwrap();
        oracle::ensure_database_profile(&mut db, oracle::POSTGRES_PROFILE).unwrap();
        assert!(db.get("renamed_history", "1").unwrap().is_some());
        native.commit().unwrap();
        assert!(db.get("business", "pending").unwrap().is_some());
        db.close().unwrap();
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
                    "routine_types",
                    "procedure_type_row",
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
                            "CREATE FUNCTION rename_row_fn(arg original) RETURNS original LANGUAGE sql AS 'SELECT arg'",
                            "CREATE FUNCTION rename_array_fn(arg original[]) RETURNS original[] LANGUAGE sql AS 'SELECT arg'",
                            "CREATE PROCEDURE rename_proc_row(arg original) LANGUAGE sql AS 'SELECT 1'",
                            "CREATE PROCEDURE rename_proc_array(arg original[]) LANGUAGE sql AS 'SELECT 1'",
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
                    let before_routines = db
                        .scan_collection(ROUTINE_COLLECTION)
                        .unwrap()
                        .into_iter()
                        .map(|row| (row.id, row.metadata))
                        .collect::<BTreeMap<_, _>>();
                    const ROUTINE_SIGNATURES: &str = "SELECT proargtypes,prorettype,pg_get_function_arguments(oid),pg_get_function_result(oid) FROM pg_proc WHERE proname IN ('rename_row_fn','rename_array_fn') ORDER BY proname";
                    let before_signatures = SqlSession::new(&mut db)
                        .execute(ROUTINE_SIGNATURES)
                        .unwrap()
                        .rows;
                    assert_eq!(before_signatures.len(), 2);
                    const PROCEDURE_SIGNATURES: &str = "SELECT proargtypes,pg_get_function_arguments(oid),pg_get_functiondef(oid) FROM pg_proc WHERE proname IN ('rename_proc_row','rename_proc_array') ORDER BY proname";
                    let before_procedures = SqlSession::new(&mut db)
                        .execute(PROCEDURE_SIGNATURES)
                        .unwrap()
                        .rows;
                    assert_eq!(before_procedures.len(), 2);
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
                    let mut expected_signatures = before_signatures.clone();
                    let mut expected_procedures = before_procedures.clone();
                    if committed {
                        for row in &mut expected_signatures {
                            for value in &mut row[2..] {
                                let SqlValue::String(name) = value else {
                                    panic!("expected routine declaration text");
                                };
                                *name = name.replace("original", "renamed");
                            }
                        }
                        for row in &mut expected_procedures {
                            for value in &mut row[1..] {
                                let SqlValue::String(text) = value else {
                                    panic!("expected procedure declaration text");
                                };
                                *text = text.replace("original", "renamed");
                            }
                        }
                    }
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
                                session.execute(ROUTINE_SIGNATURES).unwrap().rows,
                                expected_signatures,
                                "routine identity paged={paged} explicit={explicit} cut={cut} reopen={reopen}"
                            );
                            assert_eq!(
                                session.execute(PROCEDURE_SIGNATURES).unwrap().rows,
                                expected_procedures,
                                "procedure identity paged={paged} explicit={explicit} cut={cut} reopen={reopen}"
                            );
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
                            assert_eq!(
                                db.scan_collection(ROUTINE_COLLECTION)
                                    .unwrap()
                                    .into_iter()
                                    .map(|row| (row.id, row.metadata))
                                    .collect::<BTreeMap<_, _>>(),
                                before_routines,
                                "complete original routine rows must survive"
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
