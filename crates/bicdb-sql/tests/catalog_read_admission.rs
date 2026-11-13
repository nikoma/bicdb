//! SQL catalog readers must observe native catalog reservations before caches.
use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use bicdb_sql::{infer_query_result_columns_for_describe, SqlSession, SqlValue};

#[derive(Clone, Copy)]
enum Reader {
    Point,
    Catalog,
    Describe,
}

fn probe(db: &mut BicDb, reader: Reader) -> bicdb_sql::Result<()> {
    match reader {
        Reader::Point => {
            assert_eq!(
                SqlSession::new(db)
                    .execute("SELECT id FROM source")
                    .map(|r| r.rows)?,
                vec![vec![SqlValue::Int(1)]]
            );
        }
        Reader::Catalog => {
            let result = SqlSession::new(db)
                .execute("SELECT relname FROM pg_class WHERE relname='source'")?;
            assert_eq!(result.rows, vec![vec![SqlValue::String("source".into())]]);
        }
        Reader::Describe => {
            let columns =
                infer_query_result_columns_for_describe(db, "SELECT id,payload FROM source")?;
            assert_eq!(
                columns.unwrap(),
                vec![
                    ("id".into(), Some("int4".into())),
                    ("payload".into(), Some("text".into()))
                ]
            );
        }
    }
    Ok(())
}

fn metadata_reservation(reader: Reader, warm: bool) {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            DbConfig::default()
                .with_fsync(true)
                .with_storage_mode(mode.clone()),
        )
        .unwrap();
        SqlSession::new(&mut db).execute("CREATE TABLE source(id INT PRIMARY KEY,payload TEXT); INSERT INTO source VALUES(1,'kept')").unwrap();
        db.create_collection("unrelated_native").unwrap();
        // Verify query validity before reserving, including the desired warm
        // cache path. The cold case still starts with initialized SQL catalogs.
        if warm {
            probe(&mut db, reader).unwrap();
        }
        let owner = db.begin_transaction().unwrap();
        let token = owner
            .reserve_catalog_collections(&["__bicdb_pg_schema".into()])
            .unwrap();
        let mut unrelated = db.begin_transaction().unwrap();
        unrelated
            .insert("unrelated_native", Record::new("acknowledged"))
            .unwrap();
        unrelated.commit().unwrap();
        assert_eq!(
            SqlSession::new(&mut db).execute("SELECT 1").unwrap().rows,
            vec![vec![SqlValue::Int(1)]]
        );
        let error = probe(&mut db, reader).expect_err(
            "ordinary SQL must not expose a reserved catalog, including cached metadata",
        );
        assert_eq!(error.sqlstate(), "40001", "{mode} warm={warm}: {error}");
        owner.release_catalog_scope(&token).unwrap();
        probe(&mut db, reader).unwrap();
        owner.rollback().unwrap();
        assert!(db
            .get("unrelated_native", "acknowledged")
            .unwrap()
            .is_some());
        db.close().unwrap();
    }
}

#[test]
fn cold_point_metadata_read_obeys_catalog_reservation() {
    metadata_reservation(Reader::Point, false);
}

#[test]
fn warm_point_metadata_read_obeys_catalog_reservation() {
    metadata_reservation(Reader::Point, true);
}

#[test]
fn cold_catalog_scan_obeys_catalog_reservation() {
    metadata_reservation(Reader::Catalog, false);
}

#[test]
fn warm_catalog_scan_obeys_catalog_reservation() {
    metadata_reservation(Reader::Catalog, true);
}

#[test]
fn cold_describe_obeys_catalog_reservation() {
    metadata_reservation(Reader::Describe, false);
}

#[test]
fn warm_describe_obeys_catalog_reservation() {
    metadata_reservation(Reader::Describe, true);
}
