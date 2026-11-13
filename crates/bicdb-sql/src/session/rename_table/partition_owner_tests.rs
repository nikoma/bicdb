//! A retained rename owner may reuse its old name for a real partition tree.
//! The derived certificate must keep its own Native admission and atomicity.
use crate::{oracle, SqlSession, SqlValue};
use bicdb_core::{BicDb, DbConfig, StorageMode};

#[test]
fn retained_rename_owner_creates_partition_tree_without_blocking_unrelated_acknowledged_work() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for commit in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let cfg = DbConfig::default()
                .with_fsync(true)
                .with_storage_mode(mode.clone())
                .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
            let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            oracle::ensure_database_profile(&mut db, oracle::POSTGRES_PROFILE).unwrap();
            SqlSession::new(&mut db).execute("CREATE TABLE source(item_key INT,payload TEXT); INSERT INTO source VALUES(1,'source-ack'); CREATE TABLE independent(item_key INT,payload TEXT); INSERT INTO independent VALUES(7,'independent-ack')").unwrap();
            let source_ack = db.scan_collection("source").unwrap()[0]
                .content_hash()
                .unwrap();
            let independent_ack = db.scan_collection("independent").unwrap()[0]
                .content_hash()
                .unwrap();
            {
                let mut owner = SqlSession::new(&mut db);
                owner
                    .execute("BEGIN; ALTER TABLE source RENAME TO fallback")
                    .unwrap();
                let mut unrelated = SqlSession::new_shared(owner.db_ref());
                assert_eq!(
                    unrelated
                        .execute("SELECT payload FROM independent WHERE item_key=7")
                        .unwrap()
                        .rows,
                    vec![vec![SqlValue::String("independent-ack".into())]]
                );
                drop(unrelated);
                owner.execute("CREATE TABLE source(item_key INT,payload TEXT) PARTITION BY LIST(item_key)")
                    .unwrap_or_else(|error| panic!("{mode} commit={commit}: partition publication under the actual retained owner: {error}"));
                owner
                    .execute("ALTER TABLE source ATTACH PARTITION fallback DEFAULT")
                    .unwrap();
                let mut unrelated = SqlSession::new_shared(owner.db_ref());
                assert_eq!(
                    unrelated
                        .execute("SELECT payload FROM independent WHERE item_key=7")
                        .unwrap()
                        .rows,
                    vec![vec![SqlValue::String("independent-ack".into())]]
                );
                drop(unrelated);
                owner
                    .execute(if commit { "COMMIT" } else { "ROLLBACK" })
                    .unwrap();
            }
            for reopen in 0..3 {
                oracle::initialize_existing_database_profile(&mut db).unwrap();
                assert_eq!(
                    SqlSession::new(&mut db)
                        .execute("SELECT item_key,payload FROM source")
                        .unwrap()
                        .rows,
                    vec![vec![
                        SqlValue::Int(1),
                        SqlValue::String("source-ack".into())
                    ]],
                    "{mode} commit={commit} reopen={reopen}"
                );
                let backing = if commit { "fallback" } else { "source" };
                assert_eq!(
                    db.scan_collection(backing).unwrap()[0]
                        .content_hash()
                        .unwrap(),
                    source_ack
                );
                assert_eq!(
                    db.scan_collection("independent").unwrap()[0]
                        .content_hash()
                        .unwrap(),
                    independent_ack
                );
                assert_eq!(db.has_collection("fallback"), commit);
                if reopen < 2 {
                    db.close().unwrap();
                    db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
                }
            }
            db.close().unwrap();
        }
    }
}
