use super::*;

fn row(value: i64) -> Record {
    Record::new("one").with_metadata(serde_json::json!({"value": value}))
}

#[test]
fn catalog_publication_resources_are_temporary_and_recovery_keeps_the_actual_parent() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = DbConfig::default().with_fsync(true).with_storage_mode(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        for name in ["base", "derived", "independent"] {
            db.create_collection(name).unwrap();
            db.insert(name, row(1)).unwrap();
        }
        let independent = db
            .get("independent", "one")
            .unwrap()
            .unwrap()
            .content_hash()
            .unwrap();
        let owner = db.begin_transaction().unwrap();
        let parent = owner
            .reserve_catalog_resources(&[], &[("base".into(), "one".into())])
            .unwrap();
        owner
            .retain_catalog_scope(&parent, "actual-parent-fixture")
            .unwrap();
        {
            let access = db
                .enter_catalog_scope_with_publication_resources(&parent, &["derived".into()])
                .unwrap();
            let mut child = db.begin_transaction().unwrap();
            child.insert("base", row(2)).unwrap();
            child.insert("derived", row(2)).unwrap();
            child.commit().unwrap();
            drop(access);
        }
        assert_eq!(db.catalog_admission.state.read().scopes.len(), 1);
        assert_eq!(
            db.get("derived", "one").unwrap().unwrap().metadata["value"],
            2
        );
        assert!(db.get("base", "one").is_err());
        assert_eq!(
            db.get("independent", "one")
                .unwrap()
                .unwrap()
                .content_hash()
                .unwrap(),
            independent
        );
        owner.rollback().unwrap();
        db.resume_catalog_scope_for_recovery(&parent).unwrap();
        {
            let _access = db
                .enter_catalog_scope_with_publication_resources(&parent, &["derived".into()])
                .unwrap();
            let mut repair = db.begin_transaction().unwrap();
            repair.insert("base", row(1)).unwrap();
            repair.insert("derived", row(1)).unwrap();
            repair.commit().unwrap();
        }
        db.finish_catalog_scope(&parent).unwrap();
        assert!(db.catalog_admission.state.read().scopes.is_empty());
        db.close().unwrap();
        for _ in 0..2 {
            let db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            for name in ["base", "derived"] {
                assert_eq!(db.get(name, "one").unwrap().unwrap().metadata["value"], 1);
            }
            assert_eq!(
                db.get("independent", "one")
                    .unwrap()
                    .unwrap()
                    .content_hash()
                    .unwrap(),
                independent
            );
            db.close().unwrap();
        }
    }
}

#[test]
fn catalog_publication_resources_preserve_foreign_observation_and_owner_fences() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    for name in ["base", "derived"] {
        db.create_collection(name).unwrap();
        db.insert(name, row(1)).unwrap();
    }
    let owner = db.begin_transaction().unwrap();
    let parent = owner.reserve_catalog_collections(&["base".into()]).unwrap();
    let foreign = db.begin_transaction().unwrap();
    foreign.get("derived", "one").unwrap();
    assert!(db
        .enter_catalog_scope_with_publication_resources(&parent, &["derived".into()])
        .is_err());
    assert_eq!(db.catalog_admission.state.read().scopes.len(), 1);
    foreign.rollback().unwrap();
    let foreign = db.begin_transaction().unwrap();
    let other = foreign
        .reserve_catalog_collections(&["derived".into()])
        .unwrap();
    assert!(db
        .enter_catalog_scope_with_publication_resources(&parent, &["derived".into()])
        .is_err());
    foreign.release_catalog_scope(&other).unwrap();
    foreign.rollback().unwrap();
    let elsewhere = tempfile::tempdir().unwrap();
    let other_db = BicDb::open(elsewhere.path()).unwrap();
    assert!(other_db
        .enter_catalog_scope_with_publication_resources(&parent, &["derived".into()])
        .is_err());
    owner.release_catalog_scope(&parent).unwrap();
    assert!(db
        .enter_catalog_scope_with_publication_resources(&parent, &["derived".into()])
        .is_err());
    owner.rollback().unwrap();
}

#[test]
fn catalog_publication_scope_drop_refuses_a_late_child_commit() {
    let root = tempfile::tempdir().unwrap();
    let cfg = DbConfig::default().with_fsync(true);
    let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
    for name in ["base", "derived"] {
        db.create_collection(name).unwrap();
        db.insert(name, row(1)).unwrap();
    }
    let before = db
        .get("derived", "one")
        .unwrap()
        .unwrap()
        .content_hash()
        .unwrap();
    let owner = db.begin_transaction().unwrap();
    let parent = owner.reserve_catalog_collections(&["base".into()]).unwrap();
    let access = db
        .enter_catalog_scope_with_publication_resources(&parent, &["derived".into()])
        .unwrap();
    let mut child = db.begin_transaction().unwrap();
    child.insert("derived", row(9)).unwrap();
    drop(access);
    assert!(child.commit().is_err());
    owner.rollback().unwrap();
    db.close().unwrap();
    let db = BicDb::open_with_config(root.path(), cfg).unwrap();
    assert_eq!(
        db.get("derived", "one")
            .unwrap()
            .unwrap()
            .content_hash()
            .unwrap(),
        before
    );
    db.close().unwrap();
}
