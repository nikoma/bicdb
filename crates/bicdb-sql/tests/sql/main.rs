use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bicdb_core::{
    AuthenticationStrength, BicDb, BicDbError, CancellationToken, CollectionMode, CollectionPolicy,
    ColumnSecurity, Geometry, GraphProjection, HnswIndexConfig, IndexDefinition, IndexField,
    IndexKind, ModelRegistryEntry, Record, RedactionPolicy, SecurityContext,
};
use bicdb_sql::{
    infer_parameter_types, infer_query_result_types, integrity_check, split_sql_statements,
    PgJsonText, SqlEngine, SqlError, SqlResult, SqlSession, SqlValue, PG_TYPE_SPECS,
};
use serde_json::json;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

fn test_db() -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    db.create_collection("patients").unwrap();
    db.create_timeseries_collection("wearable").unwrap();

    db.batch_insert(
        "patients",
        [
            Record::new("patient-a")
                .with_timestamp(1_710_000_010)
                .with_metadata(json!({"clinic": "rural-7", "risk": 0.8})),
            Record::new("patient-b")
                .with_timestamp(1_710_000_020)
                .with_metadata(json!({"clinic": "urban-2", "risk": 0.2})),
            Record::new("patient-c")
                .with_timestamp(1_710_000_030)
                .with_metadata(json!({"clinic": "rural-7", "risk": 0.4})),
        ],
    )
    .unwrap();

    db.batch_insert(
        "wearable",
        [
            Record::new("w-1")
                .with_timestamp(1_710_000_000)
                .with_metadata(json!({"metric": "hrv", "value": 10.0})),
            Record::new("w-2")
                .with_timestamp(1_710_000_001)
                .with_metadata(json!({"metric": "hrv", "value": 20.0})),
            Record::new("w-3")
                .with_timestamp(1_710_000_002)
                .with_metadata(json!({"metric": "steps", "value": 1000.0})),
        ],
    )
    .unwrap();

    (dir, db)
}

fn expired_cancellation() -> CancellationToken {
    CancellationToken::new(
        Arc::new(AtomicBool::new(false)),
        Some(Instant::now() - Duration::from_millis(1)),
    )
}

fn empty_test_db() -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open(dir.path()).unwrap();
    (dir, db)
}

fn assert_float_close(value: &SqlValue, expected: f64, tolerance: f64) {
    let SqlValue::Float(actual) = value else {
        panic!("expected float, got {value:?}");
    };
    assert!(
        (actual - expected).abs() <= tolerance,
        "expected {actual} to be within {tolerance} of {expected}"
    );
}

fn expected_role_oid(name: &str) -> i64 {
    let name = name.trim_matches('"').to_ascii_lowercase();
    if name == "bicdb" {
        return 10;
    }
    let mut hash = 30_000_i64;
    for byte in name.bytes() {
        hash = (hash * 31 + i64::from(byte)) % 1_000_000;
    }
    hash
}

fn tenant_policy() -> CollectionPolicy {
    CollectionPolicy::tenant_field("tenant_id")
        .with_read_roles(["reader", "writer"])
        .with_write_roles(["writer"])
        .with_delete_roles(["writer"])
}

fn tenant_ctx(tenant: &str, roles: &[&str]) -> SecurityContext {
    SecurityContext::new("sql-user", tenant).with_roles(roles.iter().copied())
}

fn insert_toy_roads(db: &mut BicDb) {
    db.create_collection("roads_nodes").unwrap();
    db.create_collection("roads_edges").unwrap();
    db.batch_insert(
        "roads_nodes",
        [
            Record::new("a").with_geometry(Geometry::point(0.0, 0.0).unwrap()),
            Record::new("b").with_geometry(Geometry::point(0.001, 0.0).unwrap()),
            Record::new("c").with_geometry(Geometry::point(0.002, 0.0).unwrap()),
        ],
    )
    .unwrap();
    db.batch_insert(
        "roads_edges",
        [
            Record::new("ab").with_metadata(json!({
                "from": "a", "to": "b", "distance_m": 100.0, "duration_s": 10.0,
                "road_class": "local"
            })),
            Record::new("bc").with_metadata(json!({
                "from": "b", "to": "c", "distance_m": 100.0, "duration_s": 10.0,
                "road_class": "local"
            })),
            Record::new("ac").with_metadata(json!({
                "from": "a", "to": "c", "distance_m": 300.0, "duration_s": 30.0,
                "road_class": "arterial"
            })),
        ],
    )
    .unwrap();
}

fn assert_sqlstate(error: SqlError, sqlstate: &str) {
    assert_eq!(error.sqlstate(), sqlstate, "{error}");
}

fn rls_test_session(db: &mut BicDb) -> SqlSession<'_> {
    SqlSession::new(db)
}

fn setup_rls_docs(session: &mut SqlSession<'_>) {
    session
        .execute("CREATE TABLE docs (id TEXT PRIMARY KEY, tenant TEXT, label TEXT)")
        .unwrap();
    session
        .execute("INSERT INTO docs VALUES ('d1', 'alice', 'a'), ('d2', 'bob', 'b')")
        .unwrap();
    session.execute("GRANT ALL ON docs TO PUBLIC").unwrap();
    session
        .execute("ALTER TABLE docs ENABLE ROW LEVEL SECURITY")
        .unwrap();
    session
        .execute("ALTER TABLE docs FORCE ROW LEVEL SECURITY")
        .unwrap();
}

mod group1;
mod group2;
mod group3;
mod group4;
mod group5;
mod group6;
mod group7;
mod group8;
