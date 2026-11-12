use bicdb_core::{
    BicDb, DbConfig, IndexDefinition, IndexExclusion, IndexExclusionElement, IndexPredicate,
    StorageMode,
};
use bicdb_sql::{ensure_primary_key_indexes, SqlEngine, SqlSession, SqlValue};

fn config(mode: StorageMode) -> DbConfig {
    DbConfig::default()
        .with_storage_mode(mode)
        .with_fsync(true)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
}

fn setup(db: &mut BicDb, partitioned: bool) -> &'static str {
    let mut sql = SqlSession::new(db);
    if partitioned {
        sql.execute(
            "CREATE TABLE p(k INT,seq INT,payload TEXT,PRIMARY KEY(k,seq)) PARTITION BY RANGE(k)",
        )
        .unwrap();
        sql.execute("CREATE TABLE guarded PARTITION OF p FOR VALUES FROM(0) TO(10)")
            .unwrap();
        sql.execute("INSERT INTO p VALUES(2,1,'ack'),(3,1,'second')")
            .unwrap();
        "p"
    } else {
        sql.execute("CREATE TABLE guarded(k INT,seq INT,payload TEXT,PRIMARY KEY(k,seq))")
            .unwrap();
        sql.execute("INSERT INTO guarded VALUES(2,1,'ack'),(3,1,'second')")
            .unwrap();
        "guarded"
    }
}

fn expected_rows() -> Vec<Vec<SqlValue>> {
    vec![
        vec![SqlValue::Int(2), SqlValue::String("ack".into())],
        vec![SqlValue::Int(3), SqlValue::String("second".into())],
    ]
}

fn acknowledged_rows(db: &BicDb) -> serde_json::Value {
    let mut rows = db.scan_collection("guarded").unwrap();
    rows.sort_by(|a, b| a.id.cmp(&b.id));
    serde_json::to_value(rows).unwrap()
}

fn incompatible_backing_refuses_without_repair(change: impl Fn(&mut IndexDefinition)) {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for partitioned in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let cfg = config(mode.clone());
            let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            let relation = setup(&mut db, partitioned);
            ensure_primary_key_indexes(&mut db).unwrap();
            let expected = db
                .index_definitions()
                .into_iter()
                .find(|index| index.collection == "guarded")
                .unwrap();
            let acknowledged = acknowledged_rows(&db);
            let mut incompatible = expected.clone();
            change(&mut incompatible);
            assert!(db.drop_index(&expected.name).unwrap());
            db.create_index(incompatible.clone()).unwrap();
            for _ in 0..2 {
                db.close().unwrap();
                db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
                let sequence = db.last_commit_seq();
                let indexes = db.index_definitions();
                let error = ensure_primary_key_indexes(&mut db).expect_err(
                    "an incompatible Native index cannot stand in for a complete SQL primary key",
                );
                assert!(error.to_string().contains("does not match primary key"));
                assert_eq!(db.last_commit_seq(), sequence);
                assert_eq!(db.index_definitions(), indexes);
                assert_eq!(
                    acknowledged_rows(&db),
                    acknowledged,
                    "{mode}: partitioned={partitioned}: refusal changed acknowledged rows"
                );
            }
            // A trusted Native repair restores the original exact definition;
            // SQL backfill may then admit it without issuing another commit.
            assert!(db.drop_index(&expected.name).unwrap());
            db.create_index(expected).unwrap();
            let sequence = db.last_commit_seq();
            ensure_primary_key_indexes(&mut db).unwrap();
            assert_eq!(db.last_commit_seq(), sequence);
            assert_eq!(
                SqlEngine::new(&db)
                    .execute(&format!("SELECT k,payload FROM {relation} ORDER BY k"))
                    .unwrap()
                    .rows,
                expected_rows()
            );
            assert_eq!(
                SqlSession::new(&mut db)
                    .execute(&format!("INSERT INTO {relation} VALUES(2,1,'must refuse')"))
                    .unwrap_err()
                    .sqlstate(),
                "23505"
            );
            db.close().unwrap();
        }
    }
}

#[test]
fn partial_primary_key_backing_refuses_without_touching_acknowledged_rows() {
    incompatible_backing_refuses_without_repair(|index| {
        index.predicate = Some(IndexPredicate::Literal {
            value: serde_json::json!(false),
        });
    });
}

#[test]
fn different_null_policy_primary_key_backing_requires_an_explicit_native_repair() {
    incompatible_backing_refuses_without_repair(|index| index.nulls_not_distinct = true);
}

#[test]
fn exclusion_primary_key_backing_requires_an_explicit_native_repair() {
    incompatible_backing_refuses_without_repair(|index| {
        // Native exclusion definitions are necessarily non-unique. They must
        // remain inadmissible even when their equality fields match the PK.
        index.unique = false;
        index.exclusion = Some(IndexExclusion {
            elements: index
                .fields
                .iter()
                .cloned()
                .map(|field| IndexExclusionElement::Equal { field })
                .collect(),
        });
    });
}

#[test]
fn partial_primary_key_backing_cannot_omit_rows_from_read_only_sql() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for partitioned in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let cfg = config(mode.clone());
            let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            let relation = setup(&mut db, partitioned);
            ensure_primary_key_indexes(&mut db).unwrap();
            let mut index = db
                .index_definitions()
                .into_iter()
                .find(|index| index.collection == "guarded")
                .unwrap();
            assert!(db.drop_index(&index.name).unwrap());
            index.predicate = Some(IndexPredicate::Literal {
                value: serde_json::json!(false),
            });
            db.create_index(index).unwrap();
            db.close().unwrap();
            let db = BicDb::open_with_config(root.path(), cfg).unwrap();
            let sequence = db.last_commit_seq();
            for sql in [
                format!("SELECT k,payload FROM {relation} WHERE k>=2 AND k<4 ORDER BY k"),
                format!("SELECT k,payload FROM {relation} WHERE k=2 AND seq=1"),
            ] {
                let expected = if sql.contains("k=2") {
                    expected_rows()[..1].to_vec()
                } else {
                    expected_rows()
                };
                assert_eq!(SqlEngine::new(&db).execute(&sql).unwrap().rows, expected);
                assert_eq!(db.last_commit_seq(), sequence);
            }
            db.close().unwrap();
        }
    }
}
