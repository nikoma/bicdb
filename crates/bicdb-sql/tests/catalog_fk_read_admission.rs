//! Cached FK discovery must admit its real descriptors without observing
//! unrelated schemas. Exercise the ordinary UPDATE/DELETE paths and row state.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn foreign_key_reads(warm: bool, delete: bool) {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            DbConfig::default().with_fsync(true).with_storage_mode(mode),
        )
        .unwrap();
        SqlSession::new(&mut db).execute("CREATE TABLE parent(id INT PRIMARY KEY,payload INT); INSERT INTO parent VALUES(1,10); CREATE TABLE child(id INT PRIMARY KEY,parent_id INT REFERENCES parent(id)); INSERT INTO child VALUES(1,1); CREATE TABLE detached(id INT PRIMARY KEY,payload INT); INSERT INTO detached VALUES(1,10)").unwrap();
        let statement = if delete {
            "DELETE FROM parent WHERE id=1"
        } else {
            "UPDATE parent SET payload=20 WHERE id=1"
        };
        if warm {
            let mut sql = SqlSession::new(&mut db);
            if delete {
                assert_eq!(sql.execute(statement).unwrap_err().sqlstate(), "23503");
            } else {
                sql.execute("UPDATE parent SET payload=10 WHERE id=1")
                    .unwrap();
            }
        }
        let owner = db.begin_transaction().unwrap();
        let scope = owner
            .reserve_catalog_resources(
                &["child".into(), "renamed_child".into()],
                &[
                    ("__bicdb_pg_schema".into(), "child".into()),
                    ("__bicdb_pg_schema".into(), "renamed_child".into()),
                ],
            )
            .unwrap();
        // Empty inbound-FK discovery on this unrelated relation remains usable
        // even while another relation's actual child descriptor is reserved.
        SqlSession::new(&mut db)
            .execute("UPDATE detached SET payload=99 WHERE id=1")
            .unwrap();
        let error = SqlSession::new(&mut db).execute(statement).unwrap_err();
        assert_eq!(
            error.sqlstate(),
            "40001",
            "warm={warm} delete={delete}: {error}"
        );
        owner.release_catalog_scope(&scope).unwrap();
        owner.rollback().unwrap();
        let mut sql = SqlSession::new(&mut db);
        assert_eq!(
            sql.execute("SELECT payload FROM parent WHERE id=1")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(10)]]
        );
        assert_eq!(
            sql.execute("SELECT parent_id FROM child WHERE id=1")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(1)]]
        );
        assert_eq!(
            sql.execute("SELECT payload FROM detached WHERE id=1")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(99)]]
        );
        if delete {
            assert_eq!(sql.execute(statement).unwrap_err().sqlstate(), "23503");
        } else {
            sql.execute(statement).unwrap();
        }
        drop(sql);
        db.close().unwrap();
    }
}

#[test]
fn cold_update_reads_actual_foreign_key_descriptors() {
    foreign_key_reads(false, false);
}

#[test]
fn cached_update_reads_actual_foreign_key_descriptors() {
    foreign_key_reads(true, false);
}

#[test]
fn cold_delete_reads_actual_foreign_key_descriptors() {
    foreign_key_reads(false, true);
}

#[test]
fn cached_delete_reads_actual_foreign_key_descriptors() {
    foreign_key_reads(true, true);
}
