use super::*;

fn fixture(profile: &str) -> (tempfile::TempDir, BicDb) {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    oracle::ensure_database_profile(&mut db, profile).unwrap();
    SqlSession::new(&mut db).execute("CREATE DOMAIN positive_oid AS INT NOT NULL CHECK(VALUE>0); CREATE TABLE p(id INT PRIMARY KEY); CREATE TABLE c(pid INT NOT NULL,val INT,CONSTRAINT c_u UNIQUE(val),CONSTRAINT c_ck CHECK(val>=0),CONSTRAINT c_fk FOREIGN KEY(pid) REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED); CREATE TABLE n(id INT PRIMARY KEY,u INT,CONSTRAINT n_u UNIQUE NULLS NOT DISTINCT(u)); CREATE TABLE spans(id TEXT PRIMARY KEY,account NUMERIC NOT NULL,occupied INT4RANGE NOT NULL); ALTER TABLE spans ADD CONSTRAINT no_overlap EXCLUDE USING gist (account WITH =,occupied WITH &&)").unwrap();
    (root, db)
}

#[test]
fn cold_oid_validation_does_not_materialize_table_catalog_rows() {
    for profile in [oracle::POSTGRES_PROFILE, oracle::ORACLE_PREVIEW_PROFILE] {
        let (_root, db) = fixture(profile);
        CONSTRAINT_OID_VALIDATION.with(|memo| memo.borrow_mut().remove(&db.instance_id()));
        CONSTRAINT_TABLE_ROW_BUILDS.with(|count| count.set(0));
        validate_constraint_catalog_oids(&db).unwrap();
        assert_eq!(
            CONSTRAINT_TABLE_ROW_BUILDS.with(|count| count.get()),
            0,
            "{profile}"
        );
    }
}

#[test]
#[ignore = "requires BICDB_OID_BENCH_DATABASE pointing to a disposable database copy"]
fn constraint_oid_snapshot_benchmark() {
    let path =
        std::env::var("BICDB_OID_BENCH_DATABASE").expect("disposable benchmark database path");
    let db = BicDb::open(path).unwrap();
    let schemas = list_schemas(&db).unwrap();
    let mut times = Vec::new();
    let mut rows = Vec::new();
    for _ in 0..3 {
        CONSTRAINT_OID_VALIDATION.with(|memo| memo.borrow_mut().remove(&db.instance_id()));
        CONSTRAINT_TABLE_ROW_BUILDS.with(|count| count.set(0));
        let started = std::time::Instant::now();
        validate_constraint_catalog_oids(&db).unwrap();
        times.push(started.elapsed().as_secs_f64());
        rows.push(CONSTRAINT_TABLE_ROW_BUILDS.with(|count| count.get()));
    }
    println!(
        "OID_BENCH {}",
        serde_json::json!({"schema_count":schemas.len(),"seconds":times,"table_rows_materialized":rows})
    );
}

#[test]
fn direct_oid_slots_match_actual_catalog_rows_for_every_constraint_kind() {
    for profile in [oracle::POSTGRES_PROFILE, oracle::ORACLE_PREVIEW_PROFILE] {
        let (_root, db) = fixture(profile);
        let schemas = list_schemas(&db).unwrap();
        let relations = table_oids(&db);
        let actual =
            pg_constraint_rows_from_schemas(&db, &schemas, &relations, None, None, None).unwrap();
        let expected = actual
            .iter()
            .map(|row| sql_value_i64(&virtual_cell(row, "oid")).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            table_constraint_oids(&schemas, &relations).collect::<Vec<_>>(),
            expected,
            "{profile}"
        );
        let domains = pg_domain_constraint_rows(&db, None, None).unwrap();
        assert_eq!(domains.len(), 2);
        validate_constraint_oid_values(
            expected.into_iter().chain(
                domains
                    .iter()
                    .map(|row| sql_value_i64(&virtual_cell(row, "oid")).unwrap()),
            ),
        )
        .unwrap();
    }
}

#[test]
fn identifier_fencing_rejects_collisions_and_invalid_values() {
    for invalid in [vec![0], vec![-1], vec![u32::MAX as i64 + 1], vec![1, 2, 1]] {
        assert!(validate_constraint_oid_values(invalid).is_err());
    }
    let (_root, db) = fixture(oracle::POSTGRES_PROFILE);
    let schemas = list_schemas(&db).unwrap();
    // Deliberately aliased relations must still trip the global fence.
    let aliased = schemas
        .iter()
        .map(|schema| (schema.name.clone(), 42))
        .collect();
    assert!(validate_constraint_oid_values(table_constraint_oids(&schemas, &aliased)).is_err());
}
