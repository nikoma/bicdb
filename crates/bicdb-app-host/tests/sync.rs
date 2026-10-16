use base64::Engine;
use bicdb_app_host::{
    database::{Database, Query, QueryBatch},
    manifest::{Manifest, Release, Resources, Runtime, TableSync},
    sync,
};
use bicdb_core::SyncVector;
use serde_json::json;
use std::collections::BTreeMap;

fn release() -> Release {
    Release {
        manifest: Manifest {
            bindings: Default::default(),
            format: 1,
            name: "notes".into(),
            version: "1".into(),
            runtime: Runtime::Node,
            isolation: Default::default(),
            placement: None,
            entrypoint: "app.mjs".into(),
            files: vec!["app.mjs".into(), "001.sql".into()],
            migrations: vec!["001.sql".into()],
            resources: Resources::default(),
            sync: BTreeMap::from([("notes".into(), TableSync::AppendOnly)]),
        },
        files: BTreeMap::from([
            (
                "app.mjs".into(),
                base64::engine::general_purpose::STANDARD
                    .encode("export default {fetch(){return new Response('ok')}}"),
            ),
            (
                "001.sql".into(),
                base64::engine::general_purpose::STANDARD
                    .encode("CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT)"),
            ),
        ]),
    }
}
fn query(database: &mut Database, sql: &str) -> anyhow::Result<Vec<bicdb_sql::SqlResult>> {
    database.query(&QueryBatch {
        statements: vec![Query {
            sql: sql.into(),
            params: vec![],
        }],
        idempotency_key: None,
        jobs: vec![],
    })
}
fn connect(left: &mut Database, right: &mut Database) {
    left.db
        .write()
        .pin_node_key(
            &right.db.read().node_id(),
            &right.db.read().mesh_verifying_key().unwrap(),
        )
        .unwrap();
    right
        .db
        .write()
        .pin_node_key(
            &left.db.read().node_id(),
            &left.db.read().mesh_verifying_key().unwrap(),
        )
        .unwrap();
}

#[test]
fn offline_changes_converge_with_signatures_without_exporting_jobs_or_platform_metadata() {
    let directory = tempfile::tempdir().unwrap();
    let release = release();
    let digest = release.digest().unwrap();
    let mut left = Database::open(&directory.path().join("left")).unwrap();
    let mut right = Database::open(&directory.path().join("right")).unwrap();
    left.prepare(&release).unwrap();
    right.prepare(&release).unwrap();
    connect(&mut left, &mut right);
    query(&mut left, "INSERT INTO notes VALUES ('a', 'left offline')").unwrap();
    query(
        &mut right,
        "INSERT INTO notes VALUES ('b', 'right offline')",
    )
    .unwrap();
    left.enqueue(json!({"private":"left job"}), Some("left".into()))
        .unwrap();
    let outgoing = sync::export(&mut left, &release, &digest, &SyncVector::default()).unwrap();
    assert_eq!(outgoing.bundle.event_count, 1);
    assert!(outgoing
        .bundle
        .events
        .iter()
        .all(|event| event.envelope.signature.is_some()));
    let mut forged = outgoing.clone();
    forged.bundle.events[0].event.payload["record"]["metadata"] = json!({"body":"tampered"});
    assert!(sync::import(&mut right, &release, forged).is_err());
    sync::import(&mut right, &release, outgoing.clone()).unwrap();
    sync::import(&mut right, &release, outgoing).unwrap();
    let vector = left.db.read().sync_vector().unwrap();
    let incoming = sync::export(&mut right, &release, &digest, &vector).unwrap();
    sync::import(&mut left, &release, incoming).unwrap();
    assert_eq!(
        query(&mut left, "SELECT * FROM notes ORDER BY id").unwrap(),
        query(&mut right, "SELECT * FROM notes ORDER BY id").unwrap()
    );
    assert_eq!(
        left.db.read().sync_vector().unwrap(),
        right.db.read().sync_vector().unwrap()
    );
    assert!(
        right.consume("worker", 1000).unwrap().is_none(),
        "jobs are local to their application instance"
    );
    drop(left);
    drop(right);
    let mut left = Database::open(&directory.path().join("left")).unwrap();
    let mut right = Database::open(&directory.path().join("right")).unwrap();
    left.prepare(&release).unwrap();
    right.prepare(&release).unwrap();
    let vector = right.db.read().sync_vector().unwrap();
    assert_eq!(
        sync::export(&mut left, &release, &digest, &vector)
            .unwrap()
            .bundle
            .event_count,
        0
    );
}

#[test]
fn append_only_and_single_writer_policies_refuse_unauthorized_mutations() {
    let directory = tempfile::tempdir().unwrap();
    let mut left = Database::open(&directory.path().join("left")).unwrap();
    let mut right = Database::open(&directory.path().join("right")).unwrap();
    let mut release = release();
    left.prepare(&release).unwrap();
    right.prepare(&release).unwrap();
    for sql in [
        "UPDATE notes SET body = 'bad'",
        "UPDATE notes AS n SET body = 'bad'",
        "DELETE FROM notes",
        "DELETE FROM public.\"notes\"",
        "INSERT INTO notes VALUES ('x', 'bad') ON CONFLICT (id) DO UPDATE SET body = 'bad'",
    ] {
        assert!(query(&mut right, sql).is_err(), "must reject {sql}");
    }
    release.manifest.sync.insert(
        "notes".into(),
        TableSync::SingleWriter {
            node: left.db.read().node_id(),
        },
    );
    left.prepare(&release).unwrap();
    right.prepare(&release).unwrap();
    connect(&mut left, &mut right);
    query(&mut left, "INSERT INTO notes VALUES ('a', 'owner')").unwrap();
    assert!(query(
        &mut right,
        "INSERT INTO \"public\".\"notes\" VALUES ('b', 'bad')"
    )
    .is_err());
    let outgoing = sync::export(
        &mut left,
        &release,
        &release.digest().unwrap(),
        &SyncVector::default(),
    )
    .unwrap();
    sync::import(&mut right, &release, outgoing).unwrap();
    assert_eq!(
        query(&mut right, "SELECT * FROM notes").unwrap()[0]
            .rows
            .len(),
        1
    );
}

#[test]
fn concurrent_append_key_collisions_are_visible_on_both_replicas() {
    let directory = tempfile::tempdir().unwrap();
    let release = release();
    let digest = release.digest().unwrap();
    let mut left = Database::open(&directory.path().join("left")).unwrap();
    let mut right = Database::open(&directory.path().join("right")).unwrap();
    left.prepare(&release).unwrap();
    right.prepare(&release).unwrap();
    connect(&mut left, &mut right);
    query(&mut left, "INSERT INTO notes VALUES ('shared', 'left')").unwrap();
    query(&mut right, "INSERT INTO notes VALUES ('shared', 'right')").unwrap();
    let outgoing = sync::export(&mut left, &release, &digest, &SyncVector::default()).unwrap();
    sync::import(&mut right, &release, outgoing).unwrap();
    let incoming = sync::export(
        &mut right,
        &release,
        &digest,
        &left.db.read().sync_vector().unwrap(),
    )
    .unwrap();
    sync::import(&mut left, &release, incoming).unwrap();
    assert_eq!(
        left.conflicts("notes").unwrap().as_array().unwrap().len(),
        1
    );
    assert_eq!(
        right.conflicts("notes").unwrap().as_array().unwrap().len(),
        1
    );
    assert_eq!(
        query(&mut left, "SELECT * FROM notes").unwrap(),
        query(&mut right, "SELECT * FROM notes").unwrap()
    );
    assert!(right.conflicts("__bic_platform_receipts").is_err());
}
