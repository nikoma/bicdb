//! Physical routing and computed key changes must retain Native authority.
use bicdb_core::{
    BicDb, DbConfig, MutationActor, MutationGrantSpec, MutationOperation, MutationPolicy,
    StorageMode,
};
use bicdb_sql::{SqlSession, SqlValue};
use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

fn config(mode: StorageMode) -> DbConfig {
    DbConfig::default()
        .with_storage_mode(mode)
        .with_fsync(true)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
}
fn actor() -> MutationActor {
    MutationActor {
        actor_id: "partition-authority-proof".into(),
        roles: BTreeSet::from(["writer".into()]),
        scopes: BTreeSet::new(),
        tenant_id: None,
        workspace_id: None,
        originating_plugin: "native-test-host".into(),
        originating_resource: None,
        originating_action: None,
        trace_id: "one-logical-update".into(),
        deadline_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 120000,
    }
}
fn grant(relation: &str) -> MutationGrantSpec {
    MutationGrantSpec {
        relation: relation.into(),
        operation: MutationOperation::Update,
        record_id: None,
        record_id_prefix: None,
        expected_version: None,
        version_field: None,
        // Explicitly authorize the Native address as well as the SQL key fields.
        allowed_columns: BTreeSet::from(["id".into(), "k".into(), "b".into()]),
        bulk: false,
        maximum_affected_rows: 1,
        cascade_relations: BTreeSet::new(),
        statement_budget: 1,
        tenant_field: None,
        workspace_field: None,
        audit_metadata: BTreeMap::from([("proof".into(), "one authorized UPDATE".into())]),
    }
}
fn partition_setup(db: &mut BicDb) {
    SqlSession::new(db)
        .execute("CREATE TABLE p(k INT PRIMARY KEY,b INT) PARTITION BY RANGE(k); CREATE TABLE low PARTITION OF p FOR VALUES FROM(0) TO(10); CREATE TABLE high PARTITION OF p FOR VALUES FROM(10) TO(20); INSERT INTO p VALUES(1,10),(11,20)")
        .unwrap();
    for name in ["low", "high"] {
        db.set_mutation_policy(name, MutationPolicy::grants_required().with_audit())
            .unwrap();
    }
}
fn hashes(db: &BicDb) -> BTreeMap<(String, String), String> {
    ["low", "high"]
        .into_iter()
        .flat_map(|name| {
            db.scan_collection(name)
                .unwrap()
                .into_iter()
                .map(move |r| ((name.into(), r.id.clone()), r.content_hash().unwrap()))
        })
        .collect()
}
#[test]
fn routed_mutations_without_native_grants_preserve_every_ack() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(dir.path(), cfg.clone()).unwrap();
        partition_setup(&mut db);
        let before = hashes(&db);
        for query in [
            "INSERT INTO p VALUES(2,30)",
            "UPDATE p SET b=99 WHERE k=1",
            "UPDATE p SET k=12 WHERE k=1",
            "DELETE FROM p WHERE k=1",
            "INSERT INTO p VALUES(1,40) ON CONFLICT(k) DO UPDATE SET b=excluded.b",
        ] {
            let sequence = db.last_commit_seq();
            let error = SqlSession::new(&mut db).execute(query).unwrap_err();
            assert_eq!(error.sqlstate(), "42501", "{query}: {error}");
            assert_eq!(hashes(&db), before, "{query} changed an ACK");
            assert_eq!(db.last_commit_seq(), sequence, "{query} committed");
        }
        let error = SqlSession::new(&mut db)
            .copy_insert_rows(
                "p",
                &["k".into(), "b".into()],
                vec![vec![Some("2".into()), Some("30".into())]],
            )
            .unwrap_err();
        assert_eq!(error.sqlstate(), "42501", "{error}");
        assert_eq!(hashes(&db), before);
        db.close().unwrap();
        for _ in 0..2 {
            let db = BicDb::open_with_config(dir.path(), cfg.clone()).unwrap();
            assert_eq!(hashes(&db), before);
            assert!(db
                .verify_indexes_consistently()
                .unwrap()
                .iter()
                .all(|r| r.valid));
            db.close().unwrap();
        }
    }
}
#[test]
fn physical_leaf_update_uses_one_actual_update_grant_and_audit() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(dir.path(), config(mode)).unwrap();
        partition_setup(&mut db);
        let mut tx = db.begin_application_transaction(actor()).unwrap();
        let authority = tx.issue_mutation_grant(grant("low")).unwrap();
        let mut sql = SqlSession::new_shared(&db)
            .with_pending_transaction(tx)
            .with_signed_mutation_grants(BTreeMap::from([("low".into(), authority)]));
        assert_eq!(
            sql.execute("UPDATE p SET b=11 WHERE k=1 RETURNING k,b")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(1), SqlValue::Int(11)]]
        );
        let tx = sql.take_pending_transaction().unwrap();
        assert_eq!(
            tx.broker_publish_len(),
            1,
            "one logical UPDATE must emit one audit"
        );
        drop(sql);
        tx.commit().unwrap();
        assert_eq!(db.get("low", "1").unwrap().unwrap().metadata["b"], 11);
        db.close().unwrap();
    }
}
#[test]
fn computed_key_update_uses_update_authority_without_delete_or_insert_grants() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(dir.path(), cfg.clone()).unwrap();
        SqlSession::new(&mut db).execute("CREATE TABLE computed(k INT GENERATED ALWAYS AS(b*2) STORED PRIMARY KEY,b INT); INSERT INTO computed(b) VALUES(1)").unwrap();
        db.set_mutation_policy("computed", MutationPolicy::grants_required().with_audit())
            .unwrap();
        let mut tx = db.begin_application_transaction(actor()).unwrap();
        let authority = tx.issue_mutation_grant(grant("computed")).unwrap();
        let mut sql = SqlSession::new_shared(&db)
            .with_pending_transaction(tx)
            .with_signed_mutation_grants(BTreeMap::from([("computed".into(), authority)]));
        assert_eq!(
            sql.execute("UPDATE computed SET b=2 WHERE k=2 RETURNING k,b")
                .expect("a generated key change is one authorized logical UPDATE")
                .rows,
            vec![vec![SqlValue::Int(4), SqlValue::Int(2)]]
        );
        let tx = sql.take_pending_transaction().unwrap();
        assert_eq!(
            tx.broker_publish_len(),
            1,
            "physical key replacement must not consume two logical actions"
        );
        drop(sql);
        tx.commit().unwrap();
        assert!(db.get("computed", "2").unwrap().is_none());
        assert_eq!(db.get("computed", "4").unwrap().unwrap().metadata["b"], 2);
        db.close().unwrap();
        for _ in 0..2 {
            let db = BicDb::open_with_config(dir.path(), cfg.clone()).unwrap();
            assert!(db.get("computed", "2").unwrap().is_none());
            assert_eq!(db.get("computed", "4").unwrap().unwrap().metadata["b"], 2);
            assert!(db
                .verify_indexes_consistently()
                .unwrap()
                .iter()
                .all(|r| r.valid));
            db.close().unwrap();
        }
    }
}

#[test]
fn late_sql_constraint_failure_rolls_back_authorized_key_replacement_and_audit() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(dir.path(), cfg.clone()).unwrap();
        SqlSession::new(&mut db).execute("CREATE TABLE computed(k INT GENERATED ALWAYS AS(b*2) STORED PRIMARY KEY,b INT CHECK(b<10)); INSERT INTO computed(b) VALUES(1)").unwrap();
        let original = db
            .get("computed", "2")
            .unwrap()
            .unwrap()
            .content_hash()
            .unwrap();
        db.set_mutation_policy("computed", MutationPolicy::grants_required().with_audit())
            .unwrap();
        let mut tx = db.begin_application_transaction(actor()).unwrap();
        let authority = tx.issue_mutation_grant(grant("computed")).unwrap();
        let mut sql = SqlSession::new_shared(&db)
            .with_pending_transaction(tx)
            .with_signed_mutation_grants(BTreeMap::from([("computed".into(), authority)]));
        let error = sql
            .execute("UPDATE computed SET b=11 WHERE k=2")
            .unwrap_err();
        assert_eq!(error.sqlstate(), "23514", "{error}");
        let tx = sql.take_pending_transaction().unwrap();
        assert_eq!(
            tx.write_len(),
            0,
            "failed statement retained its physical move"
        );
        assert_eq!(
            tx.broker_publish_len(),
            0,
            "failed SQL check retained an UPDATE audit"
        );
        drop(sql);
        tx.commit().unwrap();
        assert_eq!(
            db.get("computed", "2")
                .unwrap()
                .unwrap()
                .content_hash()
                .unwrap(),
            original
        );
        assert!(db.get("computed", "22").unwrap().is_none());
        db.close().unwrap();
        for _ in 0..2 {
            let db = BicDb::open_with_config(dir.path(), cfg.clone()).unwrap();
            assert_eq!(
                db.get("computed", "2")
                    .unwrap()
                    .unwrap()
                    .content_hash()
                    .unwrap(),
                original
            );
            assert!(db.get("computed", "22").unwrap().is_none());
            assert!(db
                .verify_indexes_consistently()
                .unwrap()
                .iter()
                .all(|r| r.valid));
            db.close().unwrap();
        }
    }
}
