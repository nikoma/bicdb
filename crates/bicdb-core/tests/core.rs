use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

use bicdb_core::replication_transport;
use bicdb_core::{
    cosine_similarity, dot_product, l2_distance, redact_bind_parameters, redact_query_text,
    AppendResponse, BicDb, BicDbError, CollectionMode, CollectionPolicy, CompressionConfig,
    ConsensusConfig, ConsensusPeer, ConsensusRole, DbConfig, Event, Geometry, GraphProjection,
    HnswIndexConfig, IndexDefinition, IndexField, IndexKind, IndexValue, JsonFilter,
    MemoryIndexMode, MemoryJobStatus, ModelRegistryEntry, OperationalMetrics, OsmImportBbox,
    Record, RedactionConfig, ReplicationConfig, ReplicationFrame, ReplicationMode,
    ReplicationTlsConfig, SecurityContext, SegmentReadMode, SlowQueryLogEntry, StorageMode,
    SyncCheckpoint, TimeSeriesFilter, VectorMetric, CURRENT_FORMAT_VERSION,
    DEFAULT_FORMAT_METADATA, DEFAULT_TRANSACTION_LOG, FORMAT_MIGRATION_STATE_FILE,
    SPATIAL_AUDIT_STREAM,
};
use osmpbfreader::{fileformat, osmformat};
use protobuf::Message;
use serde_json::json;
use tempfile::TempDir;

fn open_temp() -> (TempDir, BicDb) {
    let temp = tempfile::tempdir().expect("tempdir");
    let db = BicDb::open_with_config(temp.path(), DbConfig::default()).expect("open db");
    (temp, db)
}

fn localhost_standby_replication_config() -> ReplicationConfig {
    ReplicationConfig {
        enabled: true,
        mode: ReplicationMode::Standby,
        listen_addr: Some("127.0.0.1:0".to_string()),
        tls: Some(ReplicationTlsConfig {
            cert_path: Default::default(),
            key_path: Default::default(),
            ca_path: Default::default(),
            require_client_cert: true,
            dev_localhost_plaintext: true,
        }),
        ..ReplicationConfig::default()
    }
}

// Primaries in replication tests DECLARE replication: on server_paged a
// primary without `replication.enabled` logs materialized commit markers
// (no full frames), and `export_replication_frames_since` refuses to serve
// from it. Declared replication keeps full-frame logging in both modes.
fn localhost_primary_replication_config() -> ReplicationConfig {
    ReplicationConfig {
        enabled: true,
        mode: ReplicationMode::Primary,
        listen_addr: Some("127.0.0.1:0".to_string()),
        tls: Some(ReplicationTlsConfig {
            cert_path: Default::default(),
            key_path: Default::default(),
            ca_path: Default::default(),
            require_client_cert: true,
            dev_localhost_plaintext: true,
        }),
        ..ReplicationConfig::default()
    }
}

fn test_consensus_config(node_id: &str) -> ConsensusConfig {
    ConsensusConfig {
        enabled: true,
        cluster_id: "cluster-a".to_string(),
        node_id: node_id.to_string(),
        peers: vec![
            ConsensusPeer::voting("n1", "127.0.0.1:9101"),
            ConsensusPeer::voting("n2", "127.0.0.1:9102"),
            ConsensusPeer::voting("n3", "127.0.0.1:9103"),
        ],
        ..ConsensusConfig::default()
    }
}

fn record_segment_path(root: &Path, collection: &str) -> std::path::PathBuf {
    root.join("segments").join(format!("{collection}.seg"))
}

fn event_segment_path(root: &Path) -> std::path::PathBuf {
    root.join("events").join("events.seg")
}

fn append_partial_frame(path: &Path) {
    OpenOptions::new()
        .append(true)
        .open(path)
        .unwrap()
        .write_all(b"BIC")
        .unwrap();
}

#[test]
fn memory_index_async_insert_process_and_searches_text() {
    let (_temp, mut db) = open_temp();
    db.create_collection("patient_notes").unwrap();
    db.register_embedding_model(ModelRegistryEntry::local_test("embeddinggemma-300m", 384))
        .unwrap();
    db.create_memory_index(
        "idx_patient_notes_text_memory",
        "patient_notes",
        "text",
        "embeddinggemma-300m",
        MemoryIndexMode::Async,
    )
    .unwrap();

    db.insert(
        "patient_notes",
        Record::new("note-cough").with_metadata(json!({
            "text": "Persistent cough after covid with fatigue and chest tightness"
        })),
    )
    .unwrap();
    db.insert(
        "patient_notes",
        Record::new("note-back").with_metadata(json!({
            "text": "Yoga plan for chronic low back pain and mobility"
        })),
    )
    .unwrap();

    let pending = db.memory_index_jobs();
    assert_eq!(pending.len(), 2);
    assert!(pending
        .iter()
        .all(|job| job.status == MemoryJobStatus::Pending));
    assert!(db
        .get("patient_notes", "note-cough")
        .unwrap()
        .unwrap()
        .vector
        .is_none());

    let report = db.process_memory_index_jobs(10).unwrap();
    assert_eq!(report.processed, 2);
    assert_eq!(report.failed, 0);
    assert!(db
        .get("patient_notes", "note-cough")
        .unwrap()
        .unwrap()
        .vector
        .as_ref()
        .is_some_and(|vector| vector.len() == 384));

    let hits = db
        .search_memory_index("patient_notes", "text", "persistent cough after covid", 2)
        .unwrap();
    assert_eq!(hits[0].record.id, "note-cough");
}

#[test]
fn memory_index_transaction_commit_enqueues_async_jobs() {
    let (_temp, mut db) = open_temp();
    db.create_collection("patient_notes").unwrap();
    db.register_embedding_model(ModelRegistryEntry::local_test("embeddinggemma-300m", 384))
        .unwrap();
    db.create_memory_index(
        "idx_patient_notes_text_memory",
        "patient_notes",
        "text",
        "embeddinggemma-300m",
        MemoryIndexMode::Async,
    )
    .unwrap();

    let mut tx = db.begin_transaction().unwrap();
    tx.insert(
        "patient_notes",
        Record::new("note-cough").with_metadata(json!({
            "text": "Persistent cough after covid with fatigue and chest tightness"
        })),
    )
    .unwrap();
    tx.commit().unwrap();

    let jobs = db.memory_index_jobs();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].status, MemoryJobStatus::Pending);
}

#[test]
fn memory_index_buffered_commit_enqueues_after_durable_finalize() {
    let (_temp, mut db) = open_temp();
    db.create_collection("patient_notes").unwrap();
    db.register_embedding_model(ModelRegistryEntry::local_test("embeddinggemma-300m", 384))
        .unwrap();
    db.create_memory_index(
        "idx_patient_notes_text_memory",
        "patient_notes",
        "text",
        "embeddinggemma-300m",
        MemoryIndexMode::Async,
    )
    .unwrap();

    let mut tx = db.begin_transaction().unwrap();
    tx.insert(
        "patient_notes",
        Record::new("note-cough").with_metadata(json!({
            "text": "Persistent cough after covid with fatigue and chest tightness"
        })),
    )
    .unwrap();
    tx.prepare_wal_payloads();
    let commit_seq = db.commit_buffered_transaction(&mut tx).unwrap();
    db.tx_log_handle().write_durable(commit_seq).unwrap();
    tx.finalize_committed_memory_jobs().unwrap();

    let jobs = db.memory_index_jobs();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].status, MemoryJobStatus::Pending);
}

#[test]
fn memory_index_secure_insert_enqueues_async_jobs() {
    let (_temp, mut db) = open_temp();
    db.create_collection_with_policy("patient_notes", CollectionMode::Standard, tenant_policy())
        .unwrap();
    db.register_embedding_model(ModelRegistryEntry::local_test("embeddinggemma-300m", 384))
        .unwrap();
    db.create_memory_index(
        "idx_patient_notes_text_memory",
        "patient_notes",
        "text",
        "embeddinggemma-300m",
        MemoryIndexMode::Async,
    )
    .unwrap();

    let writer = tenant_ctx("tenant-a", &["writer", "reader"]);
    db.secure(&writer)
        .insert(
            "patient_notes",
            Record::new("note-cough").with_metadata(json!({
                "tenant_id": "tenant-a",
                "text": "Persistent cough after covid with fatigue and chest tightness"
            })),
        )
        .unwrap();

    let jobs = db.memory_index_jobs();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].status, MemoryJobStatus::Pending);
}

#[test]
fn memory_index_sync_insert_indexes_before_returning() {
    let (_temp, mut db) = open_temp();
    db.create_collection("patient_notes").unwrap();
    db.register_embedding_model(ModelRegistryEntry::local_test("embeddinggemma-300m", 384))
        .unwrap();
    db.create_memory_index(
        "idx_patient_notes_text_memory",
        "patient_notes",
        "text",
        "embeddinggemma-300m",
        MemoryIndexMode::Sync,
    )
    .unwrap();

    db.insert(
        "patient_notes",
        Record::new("note-cough").with_metadata(json!({
            "text": "Persistent cough after covid with fatigue and chest tightness"
        })),
    )
    .unwrap();

    let stored = db.get("patient_notes", "note-cough").unwrap().unwrap();
    assert!(stored
        .vector
        .as_ref()
        .is_some_and(|vector| vector.len() == 384));
    assert_eq!(db.memory_index_jobs()[0].status, MemoryJobStatus::Indexed);

    let hits = db
        .search_memory_index("patient_notes", "text", "persistent cough after covid", 1)
        .unwrap();
    assert_eq!(hits[0].record.id, "note-cough");
}

#[test]
// Opt-in: `cargo test -- --ignored memory_index_local_onnx`.
//
// Needs BOTH a ~200 MB EmbeddingGemma model and a working ONNX Runtime shared
// library. Neither belongs in a default test run, and when the runtime is
// missing this test used to HANG the entire bicdb-core suite rather than fail:
// `ort` deadlocks in its own error path when the dylib cannot be loaded (see
// `ensure_onnx_runtime_available`). The pre-flight check now turns that into an
// error, but the test still requires real inference, so it stays opt-in.
#[ignore = "requires the EmbeddingGemma model and a local ONNX Runtime library"]
fn memory_index_local_onnx_model_indexes_and_searches_when_available() {
    let model_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .unwrap()
        .join("models")
        .join("embeddinggemma-300m-ONNX");
    if !model_dir.join("onnx/model_q4.onnx").is_file()
        || !model_dir.join("onnx/model_q4.onnx_data").is_file()
        || !model_dir.join("tokenizer.json").is_file()
    {
        return;
    }

    let (_temp, mut db) = open_temp();
    db.create_collection("patient_notes").unwrap();
    db.register_local_onnx_embedding_model("embeddinggemma-300m", &model_dir, 768)
        .unwrap();
    db.create_memory_index(
        "idx_patient_notes_text_memory",
        "patient_notes",
        "text",
        "embeddinggemma-300m",
        MemoryIndexMode::Sync,
    )
    .unwrap();

    db.insert(
        "patient_notes",
        Record::new("note-cough").with_metadata(json!({
            "text": "Persistent cough after covid with fatigue and chest tightness"
        })),
    )
    .unwrap();

    let stored = db.get("patient_notes", "note-cough").unwrap().unwrap();
    assert!(stored
        .vector
        .as_ref()
        .is_some_and(|vector| vector.len() == 768));

    let hits = db
        .search_memory_index("patient_notes", "text", "persistent cough after covid", 1)
        .unwrap();
    assert_eq!(hits[0].record.id, "note-cough");
}

#[test]
fn memory_index_catalog_and_jobs_persist_across_reopen() {
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(temp.path()).unwrap();
        db.create_collection("notes").unwrap();
        db.register_embedding_model(ModelRegistryEntry::local_test("embeddinggemma-300m", 384))
            .unwrap();
        db.create_memory_index(
            "idx_notes_text_memory",
            "notes",
            "text",
            "embeddinggemma-300m",
            MemoryIndexMode::Async,
        )
        .unwrap();
        db.insert(
            "notes",
            Record::new("n1").with_metadata(json!({"text": "tuberculosis case follow up"})),
        )
        .unwrap();
    }

    let reopened = BicDb::open(temp.path()).unwrap();
    assert_eq!(reopened.memory_indexes().len(), 1);
    assert_eq!(reopened.memory_index_jobs().len(), 1);

    let report = reopened.process_memory_index_jobs(10).unwrap();
    assert_eq!(report.processed, 1);
    let hits = reopened
        .search_memory_index("notes", "text", "similar tuberculosis cases", 1)
        .unwrap();
    assert_eq!(hits[0].record.id, "n1");
}

#[test]
fn native_replication_stream_replays_and_resumes_to_standby() {
    let primary_dir = tempfile::tempdir().unwrap();
    let standby_dir = tempfile::tempdir().unwrap();

    let mut primary = BicDb::open_with_config(
        primary_dir.path(),
        DbConfig::default().with_replication(localhost_primary_replication_config()),
    )
    .unwrap();
    primary.create_collection("items").unwrap();
    {
        let mut tx = primary.begin_transaction().unwrap();
        tx.insert(
            "items",
            Record::new("a").with_metadata(json!({"value": 1, "kind": "first"})),
        )
        .unwrap();
        tx.commit().unwrap();
    }

    let standby_config =
        DbConfig::default().with_replication(localhost_standby_replication_config());
    let mut standby = BicDb::open_with_config(standby_dir.path(), standby_config).unwrap();

    assert!(matches!(
        standby.insert("items", Record::new("ordinary-write")),
        Err(BicDbError::ReadOnlyStandby(_))
    ));

    let first_batch = primary.export_replication_frames_since(0, 100).unwrap();
    assert_eq!(first_batch.len(), 1);
    let report = standby.apply_replication_batch(&first_batch).unwrap();
    assert_eq!(report.applied, 1);
    assert_eq!(report.duplicates, 0);
    assert_eq!(standby.replication_lag(primary.current_commit_seq()), 0);
    assert_eq!(
        standby.get("items", "a").unwrap().unwrap().metadata["value"],
        json!(1)
    );

    let duplicate = standby.apply_replication_frame(&first_batch[0]).unwrap();
    assert_eq!(duplicate.applied, 0);
    assert_eq!(duplicate.duplicates, 1);

    {
        let mut tx = primary.begin_transaction().unwrap();
        tx.insert(
            "items",
            Record::new("b").with_metadata(json!({"value": 2, "kind": "resume"})),
        )
        .unwrap();
        tx.commit().unwrap();
    }
    let catch_up = primary
        .export_replication_frames_since(standby.last_applied_commit_seq(), 100)
        .unwrap();
    assert_eq!(catch_up.len(), 1);
    standby.apply_replication_batch(&catch_up).unwrap();
    assert_eq!(
        standby.get("items", "b").unwrap().unwrap().metadata["kind"],
        json!("resume")
    );
    assert_eq!(standby.replication_lag(primary.current_commit_seq()), 0);
}

#[test]
fn native_replication_apply_state_persists_across_reopen() {
    let primary_dir = tempfile::tempdir().unwrap();
    let standby_dir = tempfile::tempdir().unwrap();

    let primary_config = DbConfig::default().with_replication(ReplicationConfig {
        cluster_id: "cluster-a".to_string(),
        node_id: "primary-a".to_string(),
        ..localhost_primary_replication_config()
    });
    let mut primary = BicDb::open_with_config(primary_dir.path(), primary_config).unwrap();
    primary.create_collection("items").unwrap();
    primary
        .insert("items", Record::new("a").with_metadata(json!({"value": 1})))
        .unwrap();

    let mut standby_replication = localhost_standby_replication_config();
    standby_replication.cluster_id = "cluster-a".to_string();
    standby_replication.node_id = "standby-a".to_string();
    let standby_config = DbConfig::default().with_replication(standby_replication.clone());
    {
        let mut standby =
            BicDb::open_with_config(standby_dir.path(), standby_config.clone()).unwrap();
        let frames = primary.export_replication_frames_since(0, 10).unwrap();
        standby.apply_replication_batch(&frames).unwrap();

        let state = standby.replication_apply_state();
        assert_eq!(state.cluster_id, "cluster-a");
        assert_eq!(state.source_node_id.as_deref(), Some("primary-a"));
        assert_eq!(state.stream_id.as_deref(), Some("default"));
        assert_eq!(state.last_applied_commit_seq, 1);
        assert!(state.last_applied_at.is_some());
        assert!(state.last_error.is_none());
    }

    let mut reopened = BicDb::open_with_config(standby_dir.path(), standby_config).unwrap();
    let state = reopened.replication_apply_state();
    assert_eq!(state.source_node_id.as_deref(), Some("primary-a"));
    assert_eq!(state.last_applied_commit_seq, 1);

    primary
        .insert("items", Record::new("b").with_metadata(json!({"value": 2})))
        .unwrap();
    let catch_up = primary
        .export_replication_frames_since(state.last_applied_commit_seq, 10)
        .unwrap();
    assert_eq!(catch_up.len(), 1);
    reopened.apply_replication_batch(&catch_up).unwrap();
    assert_eq!(
        reopened.get("items", "b").unwrap().unwrap().metadata["value"],
        json!(2)
    );
    assert_eq!(
        reopened.replication_apply_state().last_applied_commit_seq,
        2
    );
}

#[test]
fn native_replication_bad_frame_records_error_without_advancing_apply_state() {
    let primary_dir = tempfile::tempdir().unwrap();
    let standby_dir = tempfile::tempdir().unwrap();

    let primary_config = DbConfig::default().with_replication(ReplicationConfig {
        cluster_id: "cluster-a".to_string(),
        node_id: "primary-a".to_string(),
        ..localhost_primary_replication_config()
    });
    let mut primary = BicDb::open_with_config(primary_dir.path(), primary_config).unwrap();
    primary.create_collection("items").unwrap();
    primary
        .insert("items", Record::new("a").with_metadata(json!({"value": 1})))
        .unwrap();
    let mut frame = primary
        .export_replication_frames_since(0, 10)
        .unwrap()
        .remove(0);
    frame.cluster_id = "wrong-cluster".to_string();
    frame.checksum = frame.calculate_checksum();

    let mut standby_replication = localhost_standby_replication_config();
    standby_replication.cluster_id = "cluster-a".to_string();
    let standby_config = DbConfig::default().with_replication(standby_replication);
    let mut standby = BicDb::open_with_config(standby_dir.path(), standby_config.clone()).unwrap();
    assert!(standby.apply_replication_frame(&frame).is_err());

    let state = standby.replication_apply_state();
    assert_eq!(state.last_applied_commit_seq, 0);
    assert!(state.last_applied_at.is_none());
    assert!(state
        .last_error
        .as_deref()
        .unwrap_or_default()
        .contains("cluster_id mismatch"));

    drop(standby);
    let reopened = BicDb::open_with_config(standby_dir.path(), standby_config).unwrap();
    assert_eq!(
        reopened.replication_apply_state().last_applied_commit_seq,
        0
    );
    assert!(reopened.replication_apply_state().last_error.is_some());
}

#[test]
fn native_replication_exports_public_direct_writes() {
    let primary_dir = tempfile::tempdir().unwrap();
    let mut primary = BicDb::open_with_config(
        primary_dir.path(),
        DbConfig::default().with_replication(localhost_primary_replication_config()),
    )
    .unwrap();
    primary.create_collection("items").unwrap();

    primary
        .insert(
            "items",
            Record::new("direct").with_metadata(json!({"value": 10})),
        )
        .unwrap();
    assert_eq!(primary.current_commit_seq(), 1);
    let frames = primary.export_replication_frames_since(0, 10).unwrap();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].commit_seq, 1);
    assert_eq!(frames[0].writes[0].record_id, "direct");

    primary.delete("items", "direct").unwrap();
    let frames = primary.export_replication_frames_since(1, 10).unwrap();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].commit_seq, 2);
    assert_eq!(frames[0].writes[0].record_id, "direct");
}

#[test]
fn native_replication_preserves_collection_metadata() {
    let primary_dir = tempfile::tempdir().unwrap();
    let standby_dir = tempfile::tempdir().unwrap();

    let mut primary = BicDb::open_with_config(
        primary_dir.path(),
        DbConfig::default().with_replication(localhost_primary_replication_config()),
    )
    .unwrap();
    let policy = CollectionPolicy::tenant_field("tenant_id");
    primary
        .create_collection_with_mode("secure_events", CollectionMode::TimeSeries)
        .unwrap();
    primary
        .insert(
            "secure_events",
            Record::new("e1")
                .with_metadata(json!({"tenant_id": "t1", "value": 42}))
                .with_timestamp(100),
        )
        .unwrap();
    primary
        .set_collection_policy("secure_events", policy.clone())
        .unwrap();

    let frames = primary.export_replication_frames_since(0, 10).unwrap();
    assert_eq!(frames.len(), 1);
    assert_eq!(
        frames[0].writes[0].collection_meta.as_ref().unwrap().mode,
        CollectionMode::TimeSeries
    );
    assert_eq!(
        frames[0].writes[0].collection_meta.as_ref().unwrap().policy,
        Some(policy.clone())
    );

    let standby_config =
        DbConfig::default().with_replication(localhost_standby_replication_config());
    let mut standby = BicDb::open_with_config(standby_dir.path(), standby_config).unwrap();
    standby.apply_replication_batch(&frames).unwrap();

    let meta = standby
        .collections()
        .into_iter()
        .find(|collection| collection.name == "secure_events")
        .unwrap();
    assert_eq!(meta.mode, CollectionMode::TimeSeries);
    assert_eq!(meta.policy, Some(policy));
    assert_eq!(standby.collection_record_count("secure_events").unwrap(), 1);
    assert!(matches!(
        standby.scan_time_range("secure_events", 0, 200),
        Err(BicDbError::Authorization(_))
    ));
}

#[test]
fn consensus_config_requires_local_voter() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = test_consensus_config("missing");
    config.peers.retain(|peer| peer.node_id != "missing");
    let result = BicDb::open_with_config(temp.path(), DbConfig::default().with_consensus(config));
    assert!(matches!(result, Err(BicDbError::Consensus(_))));
}

#[test]
fn consensus_election_state_persists_across_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let config = test_consensus_config("n1");
    {
        let mut db = BicDb::open_with_config(
            temp.path(),
            DbConfig::default().with_consensus(config.clone()),
        )
        .unwrap();
        let vote = db.consensus_start_election().unwrap();
        assert_eq!(vote.term, 1);
        assert_eq!(vote.candidate_id, "n1");
        assert_eq!(db.consensus_status().role, ConsensusRole::Candidate);
    }
    let db =
        BicDb::open_with_config(temp.path(), DbConfig::default().with_consensus(config)).unwrap();
    let status = db.consensus_status();
    assert_eq!(status.current_term, 1);
    assert_eq!(status.voted_for.as_deref(), Some("n1"));
    assert_eq!(status.role, ConsensusRole::Candidate);
}

#[test]
fn consensus_leader_commits_after_quorum_ack() {
    let temp = tempfile::tempdir().unwrap();
    let config = test_consensus_config("n1");
    let mut db =
        BicDb::open_with_config(temp.path(), DbConfig::default().with_consensus(config)).unwrap();
    db.consensus_start_election().unwrap();
    db.consensus_become_leader().unwrap();

    let frame = bicdb_core::CommitFrame::new("cluster-a", "n1", "default", 1, 1, 123, Vec::new());
    let entry = db.consensus_append_local_commit(frame).unwrap();
    assert_eq!(db.consensus_status().commit_index, 0);
    let response = AppendResponse {
        cluster_id: "cluster-a".to_string(),
        term: db.consensus_status().current_term,
        node_id: "n2".to_string(),
        success: true,
        match_index: entry.index,
    };
    assert_eq!(
        db.consensus_record_append_response("n2", &response)
            .unwrap(),
        entry.index
    );
    assert_eq!(db.consensus_status().commit_index, entry.index);
}

#[test]
fn native_replication_transport_streams_commit_frames_over_loopback() {
    let primary_dir = tempfile::tempdir().unwrap();
    let standby_dir = tempfile::tempdir().unwrap();
    let mut primary = BicDb::open_with_config(
        primary_dir.path(),
        DbConfig::default().with_replication(localhost_primary_replication_config()),
    )
    .unwrap();
    primary.create_collection("items").unwrap();
    primary
        .insert(
            "items",
            Record::new("wire").with_metadata(json!({"value": 42})),
        )
        .unwrap();
    let frames = primary.export_replication_frames_since(0, 10).unwrap();

    let addr = replication_transport::serve_localhost_once("127.0.0.1:0", move |mut stream| {
        replication_transport::send_commit_batch(&mut stream, &frames, 1024 * 1024)
    })
    .unwrap();

    let mut client =
        replication_transport::connect_localhost(addr, std::time::Duration::from_secs(2)).unwrap();
    let mut received = Vec::new();
    let frame = replication_transport::recv_frame(&mut client, 1024 * 1024).unwrap();
    match frame {
        ReplicationFrame::Commit(commit) => received.push(commit),
        other => panic!("unexpected replication frame: {other:?}"),
    }

    let standby_config =
        DbConfig::default().with_replication(localhost_standby_replication_config());
    let mut standby = BicDb::open_with_config(standby_dir.path(), standby_config).unwrap();
    standby.apply_replication_batch(&received).unwrap();
    assert_eq!(
        standby.get("items", "wire").unwrap().unwrap().metadata["value"],
        json!(42)
    );
}

#[test]
fn native_replication_rejects_corrupt_out_of_order_and_wrong_cluster_frames() {
    let primary_dir = tempfile::tempdir().unwrap();
    let standby_dir = tempfile::tempdir().unwrap();

    let mut primary = BicDb::open_with_config(
        primary_dir.path(),
        DbConfig::default().with_replication(localhost_primary_replication_config()),
    )
    .unwrap();
    primary.create_collection("items").unwrap();
    {
        let mut tx = primary.begin_transaction().unwrap();
        tx.insert("items", Record::new("a").with_metadata(json!({"value": 1})))
            .unwrap();
        tx.commit().unwrap();
    }

    let standby_config =
        DbConfig::default().with_replication(localhost_standby_replication_config());
    let mut standby = BicDb::open_with_config(standby_dir.path(), standby_config).unwrap();
    let frame = primary
        .export_replication_frames_since(0, 1)
        .unwrap()
        .remove(0);

    let mut corrupt = frame.clone();
    corrupt.writes[0].payload.push(0xff);
    assert!(standby.apply_replication_frame(&corrupt).is_err());

    let mut wrong_cluster = frame.clone();
    wrong_cluster.cluster_id = "other-cluster".to_string();
    wrong_cluster.checksum = wrong_cluster.calculate_checksum();
    assert!(standby.apply_replication_frame(&wrong_cluster).is_err());

    let mut out_of_order = frame.clone();
    out_of_order.commit_seq = 2;
    out_of_order.previous_commit_seq = 1;
    out_of_order.checksum = out_of_order.calculate_checksum();
    assert!(standby.apply_replication_frame(&out_of_order).is_err());
}

fn corrupt_first_byte(path: &Path) {
    use std::io::{Seek, SeekFrom};

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&[0xff]).unwrap();
}

fn sorted_dir_entries(path: &Path) -> Vec<String> {
    let mut entries = fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    entries.sort();
    entries
}

fn tenant_policy() -> CollectionPolicy {
    CollectionPolicy::tenant_field("tenant_id")
        .with_read_roles(["reader", "writer", "deleter"])
        .with_write_roles(["writer"])
        .with_delete_roles(["deleter"])
}

fn tenant_ctx(tenant: &str, roles: &[&str]) -> SecurityContext {
    SecurityContext::new("user-1", tenant).with_roles(roles.iter().copied())
}

#[test]
fn protected_collections_fail_closed_and_secure_api_enforces_tenant_roles() {
    let (_temp, mut db) = open_temp();
    db.create_collection_with_policy("patients", CollectionMode::Standard, tenant_policy())
        .unwrap();

    let writer_a = tenant_ctx("tenant-a", &["writer", "reader", "deleter"]);
    let writer_b = tenant_ctx("tenant-b", &["writer", "reader", "deleter"]);
    db.secure(&writer_a)
        .insert(
            "patients",
            Record::new("p-a")
                .with_vector(vec![1.0, 0.0])
                .with_metadata(json!({"tenant_id": "tenant-a", "name": "Ada"})),
        )
        .unwrap();
    db.secure(&writer_b)
        .insert(
            "patients",
            Record::new("p-b")
                .with_vector(vec![0.0, 1.0])
                .with_metadata(json!({"tenant_id": "tenant-b", "name": "Bea"})),
        )
        .unwrap();

    assert!(matches!(
        db.scan_collection("patients"),
        Err(BicDbError::Authorization(_))
    ));
    assert_eq!(
        db.secure(&writer_a)
            .scan_collection("patients")
            .unwrap()
            .iter()
            .map(|record| record.id.as_str())
            .collect::<Vec<_>>(),
        vec!["p-a"]
    );
    assert_eq!(db.secure(&writer_a).get("patients", "p-b").unwrap(), None);
    assert_eq!(
        db.secure(&writer_a)
            .search_vector("patients", &[1.0, 0.0], 10, None)
            .unwrap()
            .iter()
            .map(|result| result.record.id.as_str())
            .collect::<Vec<_>>(),
        vec!["p-a"]
    );
    assert!(matches!(
        db.secure(&tenant_ctx("tenant-a", &["reader"])).insert(
            "patients",
            Record::new("denied").with_metadata(json!({"tenant_id": "tenant-a"})),
        ),
        Err(BicDbError::Authorization(_))
    ));
    assert!(matches!(
        db.secure(&writer_a).insert(
            "patients",
            Record::new("wrong").with_metadata(json!({"tenant_id": "tenant-b"})),
        ),
        Err(BicDbError::Authorization(_))
    ));
    assert!(matches!(
        db.secure(&writer_a)
            .insert("patients", Record::new("missing")),
        Err(BicDbError::Authorization(_))
    ));
    assert!(db.secure(&writer_a).delete("patients", "p-b").is_err());
    assert!(db.secure(&writer_a).delete("patients", "p-a").unwrap());
}

#[test]
fn admin_bypass_requires_reason_and_records_audit_event() {
    let (_temp, mut db) = open_temp();
    db.create_collection_with_policy("patients", CollectionMode::Standard, tenant_policy())
        .unwrap();
    let ctx = tenant_ctx("tenant-a", &["writer", "reader", "deleter"]);
    db.secure(&ctx)
        .insert(
            "patients",
            Record::new("p-a").with_metadata(json!({"tenant_id": "tenant-a"})),
        )
        .unwrap();

    let missing_reason = SecurityContext::new("admin", "tenant-admin").with_bypass_reason("");
    assert!(matches!(
        db.secure(&missing_reason).scan_collection("patients"),
        Err(BicDbError::Authorization(_))
    ));

    let bypass = SecurityContext::new("admin", "tenant-admin").with_bypass_reason("break glass");
    assert_eq!(
        db.secure(&bypass)
            .scan_collection("patients")
            .unwrap()
            .len(),
        1
    );
    let events = db.events().read("bicdb.security");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event.payload["user_id"], "admin");
    assert_eq!(events[0].event.payload["collection"], "patients");
    assert_eq!(events[0].event.payload["reason"], "break glass");
}

#[test]
fn secure_graph_projection_filters_protected_source_records() {
    let (_temp, mut db) = open_temp();
    db.create_collection_with_policy("patients", CollectionMode::Standard, tenant_policy())
        .unwrap();
    let ctx_a = tenant_ctx("tenant-a", &["writer", "reader"]);
    let ctx_b = tenant_ctx("tenant-b", &["writer", "reader"]);
    db.secure(&ctx_a)
        .insert(
            "patients",
            Record::new("p-a").with_metadata(json!({"tenant_id": "tenant-a"})),
        )
        .unwrap();
    db.secure(&ctx_b)
        .insert(
            "patients",
            Record::new("p-b").with_metadata(json!({"tenant_id": "tenant-b"})),
        )
        .unwrap();
    let projection = GraphProjection::new("tenant_graph").nodes_from("patients", "Patient");
    assert!(matches!(
        db.build_graph_projection(projection.clone()),
        Err(BicDbError::Authorization(_))
    ));
    let graph = db
        .secure(&ctx_a)
        .build_graph_projection(projection)
        .unwrap();
    assert_eq!(graph.nodes.len(), 1);
    assert!(graph.nodes.contains_key("Patient:p-a"));
}

#[test]
fn insert_get_round_trip() {
    let (_temp, mut db) = open_temp();
    db.create_collection("patients").unwrap();

    let record = Record::new("patient-1")
        .with_vector(vec![1.0, 0.0, 0.0])
        .with_metadata(json!({"name": "Asha", "clinic": "rural-7"}))
        .with_timestamp(1710000000)
        .with_payload(vec![1, 2, 3]);

    db.insert("patients", record.clone()).unwrap();

    assert_eq!(
        db.get("patients", "patient-1").unwrap(),
        Some(record.into())
    );
    assert_eq!(db.pending_sync_ops().len(), 1);
}

#[test]
fn legacy_fixture_without_format_metadata_upgrades_to_current_format() {
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(
            temp.path(),
            DbConfig::default()
                .with_storage_mode(StorageMode::EmbeddedMemory)
                .with_fsync(false),
        )
        .unwrap();
        db.create_collection("patients").unwrap();
        db.insert(
            "patients",
            Record::new("p-1").with_metadata(json!({"fixture": "legacy-no-format"})),
        )
        .unwrap();
        db.close().unwrap();
    }
    fs::remove_file(temp.path().join(DEFAULT_FORMAT_METADATA)).unwrap();

    let plan = BicDb::plan_format_migration(temp.path()).unwrap();
    assert_eq!(plan.from_version, 1);
    assert_eq!(plan.to_version, CURRENT_FORMAT_VERSION);
    assert!(plan.dry_run);
    assert!(plan.backup_recommended);

    let db = BicDb::open_with_config(
        temp.path(),
        DbConfig::default()
            .with_storage_mode(StorageMode::EmbeddedMemory)
            .with_fsync(false),
    )
    .unwrap();
    assert_eq!(
        db.format_metadata().unwrap().format_version,
        CURRENT_FORMAT_VERSION
    );
    assert_eq!(
        db.get("patients", "p-1").unwrap().unwrap().metadata["fixture"],
        "legacy-no-format"
    );
}

#[test]
fn historical_fixture_with_v1_metadata_upgrades_to_current_format() {
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(
            temp.path(),
            DbConfig::default()
                .with_storage_mode(StorageMode::EmbeddedMemory)
                .with_fsync(false),
        )
        .unwrap();
        db.create_collection("patients").unwrap();
        db.insert(
            "patients",
            Record::new("p-2").with_metadata(json!({"fixture": "format-v1"})),
        )
        .unwrap();
        db.close().unwrap();
    }
    fs::write(
        temp.path().join(DEFAULT_FORMAT_METADATA),
        br#"{"format_version":1,"min_reader_version":1,"min_writer_version":1,"feature_flags":[]}"#,
    )
    .unwrap();

    let plan = BicDb::plan_format_migration(temp.path()).unwrap();
    assert_eq!(plan.from_version, 1);
    assert!(plan.backup_recommended);

    let db = BicDb::open_with_config(
        temp.path(),
        DbConfig::default()
            .with_storage_mode(StorageMode::EmbeddedMemory)
            .with_fsync(false),
    )
    .unwrap();
    assert_eq!(
        db.format_metadata().unwrap().format_version,
        CURRENT_FORMAT_VERSION
    );
    assert_eq!(
        db.get("patients", "p-2").unwrap().unwrap().metadata["fixture"],
        "format-v1"
    );
}

#[test]
fn interrupted_format_migration_recovers_safely_on_open() {
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(
            temp.path(),
            DbConfig::default()
                .with_storage_mode(StorageMode::EmbeddedMemory)
                .with_fsync(false),
        )
        .unwrap();
        db.create_collection("patients").unwrap();
        db.insert("patients", Record::new("p-1")).unwrap();
        db.close().unwrap();
    }
    fs::remove_file(temp.path().join(DEFAULT_FORMAT_METADATA)).unwrap();
    fs::write(
        temp.path().join(FORMAT_MIGRATION_STATE_FILE),
        br#"{"from_version":1,"to_version":2,"steps_total":1,"steps_completed":0}"#,
    )
    .unwrap();

    let db = BicDb::open_with_config(
        temp.path(),
        DbConfig::default()
            .with_storage_mode(StorageMode::EmbeddedMemory)
            .with_fsync(false),
    )
    .unwrap();
    assert_eq!(
        db.format_metadata().unwrap().format_version,
        CURRENT_FORMAT_VERSION
    );
    assert!(db.get("patients", "p-1").unwrap().is_some());
    assert!(!temp.path().join(FORMAT_MIGRATION_STATE_FILE).exists());
}

#[test]
fn unsupported_future_format_fails_without_mutating_data() {
    let temp = tempfile::tempdir().unwrap();
    fs::write(
        temp.path().join(DEFAULT_FORMAT_METADATA),
        br#"{"format_version":999,"min_reader_version":999,"min_writer_version":999,"feature_flags":["future_flag"]}"#,
    )
    .unwrap();
    let before = sorted_dir_entries(temp.path());

    let error = BicDb::open_with_config(temp.path(), DbConfig::default().with_fsync(false))
        .unwrap_err()
        .to_string();
    assert!(error.contains("format compatibility"));
    assert_eq!(sorted_dir_entries(temp.path()), before);
}

#[test]
fn ha_shipping_converges_standby_and_promote_preserves_state() {
    let root = tempfile::tempdir().unwrap();
    let primary_path = root.path().join("primary");
    let standby_path = root.path().join("standby");
    {
        let mut primary = BicDb::open_with_config(
            &primary_path,
            DbConfig::default()
                .with_fsync(false)
                .with_audit_events(true),
        )
        .unwrap();
        primary
            .create_collection_with_policy("patients", CollectionMode::Standard, tenant_policy())
            .unwrap();
        primary
            .secure(&tenant_ctx("tenant-a", &["writer"]))
            .insert(
                "patients",
                Record::new("p-1").with_metadata(json!({"tenant_id": "tenant-a", "name": "Ada"})),
            )
            .unwrap();
        primary
            .create_index(IndexDefinition {
                name: "patients_name".to_string(),
                collection: "patients".to_string(),
                fields: vec![IndexField::MetadataPath(vec!["name".to_string()])],
                unique: false,
                kind: IndexKind::BTree,
                predicate: None,
                exclusion: None,
            })
            .unwrap();
        primary.close().unwrap();
    }

    let report = BicDb::configure_standby_from(&standby_path, &primary_path).unwrap();
    assert!(report.files_copied > 0);

    let mut standby = BicDb::open(&standby_path).unwrap();
    let status = standby.ha_status().unwrap();
    assert_eq!(status.role, bicdb_core::HaRole::Standby);
    // The cheap in-memory accessor must agree with the full status walk.
    assert_eq!(standby.ha_role(), status.role);
    assert!(status.read_only);
    assert_eq!(status.lag_bytes, 0);
    assert_eq!(
        standby
            .secure(&tenant_ctx("tenant-a", &["reader"]))
            .scan_collection("patients")
            .unwrap()
            .len(),
        1
    );
    assert!(standby.verify_index("patients_name").unwrap().valid);
    assert_eq!(standby.events().read(SPATIAL_AUDIT_STREAM).len(), 0);
    assert!(matches!(
        standby.insert("patients", Record::new("p-2")),
        Err(BicDbError::ReadOnlyStandby(_))
    ));
    drop(standby);

    let promoted = BicDb::promote_standby(&standby_path, false).unwrap();
    assert_eq!(promoted.role, bicdb_core::HaRole::Primary);
    let mut promoted_db = BicDb::open(&standby_path).unwrap();
    assert_eq!(promoted_db.ha_role(), bicdb_core::HaRole::Primary);
    promoted_db
        .secure(&tenant_ctx("tenant-a", &["writer"]))
        .insert(
            "patients",
            Record::new("p-2").with_metadata(json!({"tenant_id": "tenant-a"})),
        )
        .unwrap();
    assert_eq!(
        promoted_db
            .secure(&tenant_ctx("tenant-a", &["reader"]))
            .scan_collection("patients")
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn ha_shipping_overwrites_interrupted_apply_artifact_on_restart() {
    let root = tempfile::tempdir().unwrap();
    let primary_path = root.path().join("primary");
    let standby_path = root.path().join("standby");
    {
        let mut primary =
            BicDb::open_with_config(&primary_path, DbConfig::default().with_fsync(false)).unwrap();
        primary.create_collection("patients").unwrap();
        primary.insert("patients", Record::new("p-1")).unwrap();
        primary.close().unwrap();
    }

    std::fs::create_dir_all(standby_path.join("segments")).unwrap();
    std::fs::write(
        standby_path.join("segments").join("patients.ha-tmp"),
        b"partial",
    )
    .unwrap();
    BicDb::configure_standby_from(&standby_path, &primary_path).unwrap();

    let standby = BicDb::open(&standby_path).unwrap();
    assert_eq!(standby.scan_collection("patients").unwrap().len(), 1);
    assert_eq!(standby.ha_status().unwrap().lag_bytes, 0);
    assert!(!standby_path
        .join("segments")
        .join("patients.ha-tmp")
        .exists());
}

#[test]
fn geometry_wkt_parse_and_render_supported_shapes() {
    let cases = [
        "POINT (-122.4 37.8)",
        "LINESTRING (-122.4 37.8, -122.3 37.9)",
        "POLYGON ((-122.4 37.8, -122.3 37.8, -122.3 37.9, -122.4 37.8))",
    ];

    for wkt in cases {
        let geometry = Geometry::from_wkt(wkt).unwrap();
        assert_eq!(Geometry::from_wkt(&geometry.to_wkt()).unwrap(), geometry);
    }

    let envelope = Geometry::from_wkt("BBOX (-122.5 37.7, -122.3 37.9)").unwrap();
    assert_eq!(envelope.to_wkt(), "BBOX (-122.5 37.7, -122.3 37.9)");
    assert_eq!(Geometry::from_wkt(&envelope.to_wkt()).unwrap(), envelope);
    assert_eq!(
        Geometry::from_wkt("ENVELOPE (-122.5 37.7, -122.3 37.9)").unwrap(),
        envelope
    );
}

#[test]
fn geometry_geojson_round_trips_supported_shapes() {
    let cases = [
        Geometry::from_wkt("POINT (-122.4 37.8)").unwrap(),
        Geometry::from_wkt("LINESTRING (-122.4 37.8, -122.3 37.9)").unwrap(),
        Geometry::from_wkt("POLYGON ((-122.4 37.8, -122.3 37.8, -122.3 37.9, -122.4 37.8))")
            .unwrap(),
        Geometry::envelope(-122.5, 37.7, -122.3, 37.9).unwrap(),
    ];

    for geometry in cases {
        let value = geometry.to_geojson_value();
        assert_eq!(
            Geometry::from_geojson_value(value.clone()).unwrap(),
            geometry
        );
        assert_eq!(
            Geometry::from_geojson_str(&value.to_string()).unwrap(),
            geometry
        );
    }
}

#[test]
fn geometry_binary_frames_are_versioned_and_round_trip() {
    let cases = [
        Geometry::from_wkt("POINT (-122.4 37.8)").unwrap(),
        Geometry::from_wkt("LINESTRING (-122.4 37.8, -122.3 37.9)").unwrap(),
        Geometry::from_wkt("POLYGON ((-122.4 37.8, -122.3 37.8, -122.3 37.9, -122.4 37.8))")
            .unwrap(),
        Geometry::envelope(-122.5, 37.7, -122.3, 37.9).unwrap(),
    ];

    for geometry in cases {
        let frame = geometry.to_bicdb_frame();
        assert_eq!(&frame[..4], b"BICG");
        assert_eq!(frame[4], 1);
        assert_eq!(Geometry::from_bicdb_frame(&frame).unwrap(), geometry);
    }
}

#[test]
fn point_geometry_storage_survives_close_reopen_and_readback() {
    let temp = tempfile::tempdir().unwrap();
    let point = Geometry::point(-122.4, 37.8).unwrap();
    {
        let mut db = BicDb::open(temp.path()).unwrap();
        db.create_collection("places").unwrap();
        db.insert(
            "places",
            Record::new("place-1")
                .with_geometry(point.clone())
                .with_metadata(json!({
                    "name": "clinic",
                    "geojson_copy": point.to_geojson_value(),
                })),
        )
        .unwrap();
    }

    let db = BicDb::open(temp.path()).unwrap();
    let record = db.get("places", "place-1").unwrap().unwrap();
    assert_eq!(record.geometry, Some(point));
    assert_eq!(record.metadata["name"], "clinic");
    assert!(record.metadata["geojson_copy"].is_object());
}

#[test]
fn invalid_wkt_geojson_and_geometry_frames_return_clear_errors() {
    let wkt_error = Geometry::from_wkt("POINT (1)").unwrap_err().to_string();
    assert!(wkt_error.contains("geometry error"));
    assert!(wkt_error.contains("invalid WKT"));

    // MULTIPOINT and the other Multi* types were unsupported when this test
    // was written, and it asserted that. The geo campaign added them, so the
    // assertion is now the wrong way round: these must PARSE.
    assert!(matches!(
        Geometry::from_wkt("MULTIPOINT ((1 2), (3 4))").unwrap(),
        Geometry::MultiPoint(_)
    ));
    assert!(matches!(
        Geometry::from_geojson_str(
            r#"{"type":"MultiPoint","coordinates":[[-122.4,37.8],[-122.3,37.9]]}"#,
        )
        .unwrap(),
        Geometry::MultiPoint(_)
    ));

    // A GeoJSON `type` that is not an OGC geometry still reports clearly.
    // Every type WKT can express is now supported, so the remaining negative
    // case lives on the GeoJSON side.
    let geojson_error =
        Geometry::from_geojson_str(r#"{"type":"Circle","coordinates":[-122.4,37.8]}"#)
            .unwrap_err()
            .to_string();
    assert!(
        geojson_error.contains("unsupported GeoJSON geometry type Circle"),
        "unexpected error: {geojson_error}"
    );

    let frame_error = Geometry::from_bicdb_frame(b"BICG\x02\x01")
        .unwrap_err()
        .to_string();
    assert!(frame_error.contains("unsupported geometry frame version 2"));
}

#[test]
fn durable_collection_and_index_names_are_length_bounded() {
    let (_dir, mut db) = open_temp();
    let oversized = "a".repeat(256);
    let error = db.create_collection(&oversized).unwrap_err().to_string();
    assert!(
        error.contains("at most 255 bytes"),
        "unexpected error: {error}"
    );

    db.create_collection("records").unwrap();
    let error = db
        .create_index(IndexDefinition {
            name: oversized,
            collection: "records".to_string(),
            fields: vec![IndexField::Id],
            unique: false,
            kind: IndexKind::BTree,
            predicate: None,
            exclusion: None,
        })
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("at most 255 bytes"),
        "unexpected error: {error}"
    );
}

#[test]
fn planted_index_catalog_paths_are_rejected_on_open() {
    let root = tempfile::tempdir().unwrap();
    let db_path = root.path().join("db");
    {
        let mut db = BicDb::open(&db_path).unwrap();
        db.create_collection("places").unwrap();
        db.create_index(IndexDefinition {
            name: "places_geom".to_string(),
            collection: "places".to_string(),
            fields: vec![IndexField::Geometry],
            unique: false,
            kind: IndexKind::Spatial,
            predicate: None,
            exclusion: None,
        })
        .unwrap();
        db.close().unwrap();
    }
    let catalog_path = db_path.join("indexes.json");
    let mut catalog: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&catalog_path).unwrap()).unwrap();
    catalog["indexes"][0]["name"] = serde_json::Value::String("../../victim".to_string());
    std::fs::write(&catalog_path, serde_json::to_vec_pretty(&catalog).unwrap()).unwrap();
    let victim = root.path().join("victim");
    std::fs::create_dir(&victim).unwrap();
    std::fs::write(victim.join("keep"), b"do not delete").unwrap();

    let error = match BicDb::open(&db_path) {
        Ok(_) => panic!("malicious index catalog unexpectedly opened"),
        Err(error) => error.to_string(),
    };
    assert!(
        error.contains("invalid collection name"),
        "unexpected error: {error}"
    );
    assert!(victim.join("keep").is_file());
}

#[test]
fn recursive_geometry_inputs_are_depth_limited() {
    let mut wkb = Geometry::point(1.0, 2.0).unwrap().to_wkb();
    for _ in 0..33 {
        let mut outer = Vec::with_capacity(wkb.len() + 9);
        outer.push(1);
        outer.extend_from_slice(&7_u32.to_le_bytes());
        outer.extend_from_slice(&1_u32.to_le_bytes());
        outer.extend_from_slice(&wkb);
        wkb = outer;
    }
    let error = Geometry::from_wkb(&wkb).unwrap_err().to_string();
    assert!(error.contains("maximum depth"), "unexpected error: {error}");

    let mut frame = Geometry::point(1.0, 2.0).unwrap().to_bicdb_frame();
    for _ in 0..33 {
        let mut outer = b"BICG\x01\x08".to_vec();
        outer.extend_from_slice(&1_u32.to_le_bytes());
        outer.extend_from_slice(&(frame.len() as u32).to_le_bytes());
        outer.extend_from_slice(&frame);
        frame = outer;
    }
    let error = Geometry::from_bicdb_frame(&frame).unwrap_err().to_string();
    assert!(error.contains("maximum depth"), "unexpected error: {error}");

    let mut wkt = "POINT (1 2)".to_string();
    for _ in 0..33 {
        wkt = format!("GEOMETRYCOLLECTION ({wkt})");
    }
    let error = Geometry::from_wkt(&wkt).unwrap_err().to_string();
    assert!(error.contains("maximum depth"), "unexpected error: {error}");
}

#[test]
fn drop_collection_removes_catalog_and_records() {
    let (_dir, mut db) = open_temp();
    db.create_collection("scratch").unwrap();
    db.insert("scratch", Record::new("one")).unwrap();

    assert!(db.drop_collection("scratch").unwrap());
    assert!(!db.drop_collection("scratch").unwrap());
    assert!(db.get("scratch", "one").is_err());
    assert!(!db
        .collections()
        .iter()
        .any(|collection| collection.name == "scratch"));
}

#[test]
fn drop_collection_purges_server_paged_rows_before_name_reuse() {
    let temp = tempfile::tempdir().unwrap();
    let config = DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged);
    {
        let mut db = BicDb::open_with_config(temp.path(), config.clone()).unwrap();
        db.create_collection("tenant_data").unwrap();
        db.insert("tenant_data", Record::new("secret")).unwrap();
        assert!(db.drop_collection("tenant_data").unwrap());
        db.create_collection("tenant_data").unwrap();
        assert!(db.scan_collection("tenant_data").unwrap().is_empty());
        db.close().unwrap();
    }
    let db = BicDb::open_with_config(temp.path(), config).unwrap();
    assert!(db.scan_collection("tenant_data").unwrap().is_empty());
}

#[test]
fn secondary_index_survives_reopen_and_verifies() {
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(temp.path()).unwrap();
        db.create_collection("patients").unwrap();
        db.batch_insert(
            "patients",
            [
                Record::new("p1").with_metadata(json!({"clinic": "rural-7"})),
                Record::new("p2").with_metadata(json!({"clinic": "urban-2"})),
                Record::new("p3").with_metadata(json!({"clinic": "rural-7"})),
            ],
        )
        .unwrap();
        db.create_index(IndexDefinition {
            name: "idx_patients_clinic".to_string(),
            collection: "patients".to_string(),
            fields: vec![IndexField::MetadataPath(vec!["clinic".to_string()])],
            unique: false,
            kind: IndexKind::BTree,
            predicate: None,
            exclusion: None,
        })
        .unwrap();

        assert_eq!(
            db.lookup_index("idx_patients_clinic", &[IndexValue::from("rural-7")])
                .unwrap(),
            vec!["p1".to_string(), "p3".to_string()]
        );
        assert!(db.verify_index("idx_patients_clinic").unwrap().valid);
    }

    let db = BicDb::open(temp.path()).unwrap();
    assert_eq!(db.index_definitions().len(), 1);
    assert_eq!(
        db.lookup_index("idx_patients_clinic", &[IndexValue::from("rural-7")])
            .unwrap(),
        vec!["p1".to_string(), "p3".to_string()]
    );
}

#[test]
fn secondary_index_tracks_update_delete_and_rollback() {
    let (_temp, mut db) = open_temp();
    db.create_collection("patients").unwrap();
    db.insert(
        "patients",
        Record::new("p1").with_metadata(json!({"age": 40})),
    )
    .unwrap();
    db.create_index(IndexDefinition {
        name: "idx_patients_age".to_string(),
        collection: "patients".to_string(),
        fields: vec![IndexField::MetadataPath(vec!["age".to_string()])],
        unique: false,
        kind: IndexKind::BTree,
        predicate: None,
        exclusion: None,
    })
    .unwrap();

    assert_eq!(
        db.lookup_index("idx_patients_age", &[IndexValue::from(40)])
            .unwrap(),
        vec!["p1".to_string()]
    );

    db.insert(
        "patients",
        Record::new("p1").with_metadata(json!({"age": 41})),
    )
    .unwrap();
    assert!(db
        .lookup_index("idx_patients_age", &[IndexValue::from(40)])
        .unwrap()
        .is_empty());
    assert_eq!(
        db.lookup_index("idx_patients_age", &[IndexValue::from(41)])
            .unwrap(),
        vec!["p1".to_string()]
    );

    let mut tx = db.begin_transaction().unwrap();
    tx.insert(
        "patients",
        Record::new("p2").with_metadata(json!({"age": 41})),
    )
    .unwrap();
    tx.rollback().unwrap();
    assert_eq!(
        db.lookup_index("idx_patients_age", &[IndexValue::from(41)])
            .unwrap(),
        vec!["p1".to_string()]
    );

    assert!(db.delete("patients", "p1").unwrap());
    assert!(db
        .lookup_index("idx_patients_age", &[IndexValue::from(41)])
        .unwrap()
        .is_empty());
    assert!(db.verify_index("idx_patients_age").unwrap().valid);
}

#[test]
fn spatial_index_reopens_and_returns_radius_and_nearest_candidates() {
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(temp.path()).unwrap();
        db.create_collection("places").unwrap();
        db.batch_insert(
            "places",
            [
                Record::new("sf").with_geometry(Geometry::point(-122.4194, 37.7749).unwrap()),
                Record::new("oak").with_geometry(Geometry::point(-122.2711, 37.8044).unwrap()),
                Record::new("la").with_geometry(Geometry::point(-118.2437, 34.0522).unwrap()),
            ],
        )
        .unwrap();
        db.create_index(IndexDefinition {
            name: "idx_places_geometry".to_string(),
            collection: "places".to_string(),
            fields: vec![IndexField::Geometry],
            unique: false,
            kind: IndexKind::Spatial,
            predicate: None,
            exclusion: None,
        })
        .unwrap();
        assert!(db.verify_index("idx_places_geometry").unwrap().valid);
    }

    let db = BicDb::open(temp.path()).unwrap();
    assert_eq!(db.index_definitions()[0].kind, IndexKind::Spatial);
    assert_eq!(
        db.spatial_radius_index("idx_places_geometry", -122.4194, 37.7749, 20_000.0)
            .unwrap(),
        vec!["oak".to_string(), "sf".to_string()]
    );
    assert_eq!(
        db.spatial_nearest_index("idx_places_geometry", -122.4194, 37.7749, 2)
            .unwrap(),
        vec!["sf".to_string(), "oak".to_string()]
    );
}

#[test]
fn spatial_api_nearest_and_within_radius_use_collection_and_field() {
    let (_temp, mut db) = open_temp();
    db.create_collection("places").unwrap();
    db.batch_insert(
        "places",
        [
            Record::new("sf")
                .with_geometry(Geometry::point(-122.4194, 37.7749).unwrap())
                .with_metadata(json!({"name": "San Francisco"})),
            Record::new("oak")
                .with_geometry(Geometry::point(-122.2711, 37.8044).unwrap())
                .with_metadata(json!({"name": "Oakland"})),
            Record::new("la")
                .with_geometry(Geometry::point(-118.2437, 34.0522).unwrap())
                .with_metadata(json!({"name": "Los Angeles"})),
        ],
    )
    .unwrap();
    db.create_spatial_index("places", "geometry").unwrap();

    let nearest = db
        .nearest("places", "geometry", -122.4194, 37.7749, 2)
        .unwrap();
    assert_eq!(
        nearest
            .iter()
            .map(|result| result.record.id.as_str())
            .collect::<Vec<_>>(),
        vec!["sf", "oak"]
    );
    assert_eq!(nearest[0].distance_meters, 0.0);

    let nearby = db
        .within_radius("places", "geometry", -122.4194, 37.7749, 20_000.0)
        .unwrap();
    assert_eq!(
        nearby
            .iter()
            .map(|result| result.record.id.as_str())
            .collect::<Vec<_>>(),
        vec!["sf", "oak"]
    );
}

#[test]
fn spatial_api_falls_back_to_exact_scan_without_index() {
    let (_temp, mut db) = open_temp();
    db.create_collection("places").unwrap();
    db.batch_insert(
        "places",
        [
            Record::new("near").with_metadata(json!({
                "location": "POINT (-122.4194 37.7749)"
            })),
            Record::new("far").with_metadata(json!({
                "location": "POINT (-118.2437 34.0522)"
            })),
        ],
    )
    .unwrap();

    let nearest = db
        .nearest("places", "location", -122.4194, 37.7749, 1)
        .unwrap();
    assert_eq!(nearest[0].record.id, "near");

    let nearby = db
        .within_radius("places", "location", -122.4194, 37.7749, 1_000.0)
        .unwrap();
    assert_eq!(nearby.len(), 1);
    assert_eq!(nearby[0].record.id, "near");
}

#[test]
fn spatial_api_errors_for_missing_collection_and_invalid_field() {
    let (_temp, mut db) = open_temp();
    db.create_collection("places").unwrap();
    db.insert(
        "places",
        Record::new("sf").with_geometry(Geometry::point(-122.4194, 37.7749).unwrap()),
    )
    .unwrap();

    let missing_collection = db
        .nearest("missing", "geometry", -122.4194, 37.7749, 1)
        .unwrap_err()
        .to_string();
    assert!(missing_collection.contains("collection not found"));

    let missing_field = db
        .nearest("places", "location", -122.4194, 37.7749, 1)
        .unwrap_err()
        .to_string();
    assert!(missing_field.contains("spatial field `location` not found"));

    let invalid_field = db
        .within_radius("places", "metadata..location", -122.4194, 37.7749, 1_000.0)
        .unwrap_err()
        .to_string();
    assert!(invalid_field.contains("empty metadata path segment"));
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
            Record::new("d").with_geometry(Geometry::point(1.0, 1.0).unwrap()),
        ],
    )
    .unwrap();
    db.batch_insert(
        "roads_edges",
        [
            Record::new("ab").with_metadata(json!({
                "from": "a", "to": "b", "distance_m": 100.0, "duration_s": 10.0,
                "road_class": "local", "metadata": {"name": "A-B"}
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

fn write_tiny_osm_pbf(path: &Path) {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .unwrap();

    let mut header = osmformat::HeaderBlock::new();
    header.required_features.push("OsmSchema-V0.6".to_string());
    header.required_features.push("DenseNodes".to_string());
    write_pbf_block(&mut file, "OSMHeader", &header);

    let mut strings = osmformat::StringTable::new();
    strings.s = ["", "highway", "residential", "name", "Fixture Road"]
        .into_iter()
        .map(|value| value.as_bytes().to_vec())
        .collect();

    let raw_nodes = [
        (1_i64, -122.4194_f64, 37.7749_f64),
        (2_i64, -122.4184_f64, 37.7749_f64),
        (3_i64, -122.4174_f64, 37.7749_f64),
    ];
    let mut dense = osmformat::DenseNodes::new();
    let mut last_id = 0_i64;
    let mut last_lat = 0_i64;
    let mut last_lon = 0_i64;
    for (id, lon, lat) in raw_nodes {
        let lat_raw = (lat * 10_000_000.0).round() as i64;
        let lon_raw = (lon * 10_000_000.0).round() as i64;
        dense.id.push(id - last_id);
        dense.lat.push(lat_raw - last_lat);
        dense.lon.push(lon_raw - last_lon);
        dense.keys_vals.push(0);
        last_id = id;
        last_lat = lat_raw;
        last_lon = lon_raw;
    }

    let mut node_group = osmformat::PrimitiveGroup::new();
    node_group.dense = protobuf::MessageField::some(dense);

    let mut way = osmformat::Way::new();
    way.set_id(10);
    way.keys = vec![1, 3];
    way.vals = vec![2, 4];
    way.refs = vec![1, 1, 1];
    let mut way_group = osmformat::PrimitiveGroup::new();
    way_group.ways.push(way);

    let mut block = osmformat::PrimitiveBlock::new();
    block.stringtable = protobuf::MessageField::some(strings);
    block.primitivegroup = vec![node_group, way_group];
    block.set_granularity(100);
    write_pbf_block(&mut file, "OSMData", &block);
}

fn write_pbf_block<M: Message>(file: &mut std::fs::File, block_type: &str, message: &M) {
    let payload = message.write_to_bytes().unwrap();
    let mut blob = fileformat::Blob::new();
    blob.set_raw(payload);
    let blob_bytes = blob.write_to_bytes().unwrap();

    let mut header = fileformat::BlobHeader::new();
    header.set_type(block_type.to_string());
    header.set_datasize(blob_bytes.len() as i32);
    let header_bytes = header.write_to_bytes().unwrap();

    file.write_all(&(header_bytes.len() as i32).to_be_bytes())
        .unwrap();
    file.write_all(&header_bytes).unwrap();
    file.write_all(&blob_bytes).unwrap();
}

fn route_fixture_stops(count: usize) -> Vec<Geometry> {
    (0..count)
        .map(|idx| {
            let lon = ((idx * 37) % count) as f64 * 0.001;
            let lat = ((idx * 19) % 7) as f64 * 0.001;
            Geometry::point(lon, lat).unwrap()
        })
        .collect()
}

fn haversine_test_meters(left: &Geometry, right: &Geometry) -> f64 {
    let Geometry::Point(left) = left else {
        panic!("expected point");
    };
    let Geometry::Point(right) = right else {
        panic!("expected point");
    };
    let radius_meters = 6_371_000.0_f64;
    let d_lat = (right.y() - left.y()).to_radians();
    let d_lon = (right.x() - left.x()).to_radians();
    let left_lat = left.y().to_radians();
    let right_lat = right.y().to_radians();
    let a = (d_lat / 2.0).sin().powi(2)
        + left_lat.cos() * right_lat.cos() * (d_lon / 2.0).sin().powi(2);
    2.0 * radius_meters * a.sqrt().asin()
}

fn naive_route_distance(stops: &[Geometry]) -> f64 {
    stops
        .windows(2)
        .map(|pair| haversine_test_meters(&pair[0], &pair[1]))
        .sum()
}

fn insert_complete_test_roads(db: &mut BicDb, count: usize) {
    db.create_collection("roads_nodes").unwrap();
    db.create_collection("roads_edges").unwrap();
    db.batch_insert(
        "roads_nodes",
        (0..count).map(|idx| {
            Record::new(format!("n{idx}")).with_geometry(Geometry::point(idx as f64, 0.0).unwrap())
        }),
    )
    .unwrap();
    db.batch_insert(
        "roads_edges",
        (0..count).flat_map(|from| {
            (0..count).filter(move |to| *to != from).map(move |to| {
                Record::new(format!("n{from}_n{to}")).with_metadata(json!({
                    "from": format!("n{from}"),
                    "to": format!("n{to}"),
                    "distance_m": ((from as i64 - to as i64).abs() as f64) * 10.0,
                    "duration_s": ((from as i64 - to as i64).abs() as f64) * 2.0,
                    "road_class": "test"
                }))
            })
        }),
    )
    .unwrap();
}

#[test]
fn routing_shortest_path_and_route_distance_on_toy_graph() {
    let (_temp, mut db) = open_temp();
    insert_toy_roads(&mut db);

    let start = Geometry::point(0.0, 0.0).unwrap();
    let end = Geometry::point(0.002, 0.0).unwrap();
    let path = db.shortest_path("roads", &start, &end).unwrap();

    assert_eq!(path.node_ids, vec!["a", "b", "c"]);
    assert_eq!(path.distance_m, 200.0);
    assert_eq!(db.route_distance("roads", &start, &end).unwrap(), 200.0);

    let astar_path = db.shortest_path_astar("roads", &start, &end).unwrap();
    assert_eq!(astar_path.node_ids, vec!["a", "b", "c"]);
    assert_eq!(astar_path.distance_m, 200.0);
}

#[test]
fn osm_pbf_import_builds_routeable_graph() {
    let (_temp, mut db) = open_temp();
    let pbf_dir = tempfile::tempdir().unwrap();
    let pbf_path = pbf_dir.path().join("tiny.osm.pbf");
    write_tiny_osm_pbf(&pbf_path);

    let report = db
        .import_osm_pbf(
            &pbf_path,
            OsmImportBbox {
                min_lon: -122.42,
                min_lat: 37.77,
                max_lon: -122.41,
                max_lat: 37.78,
            },
        )
        .unwrap();

    assert_eq!(report.graph, "roads");
    assert_eq!(report.road_ways, 1);
    assert_eq!(report.nodes, 3);
    assert_eq!(report.edges, 4);

    let path = db
        .shortest_path(
            "roads",
            &Geometry::point(-122.4194, 37.7749).unwrap(),
            &Geometry::point(-122.4174, 37.7749).unwrap(),
        )
        .unwrap();
    assert_eq!(
        path.node_ids,
        vec!["osm-node-1", "osm-node-2", "osm-node-3"]
    );
    assert!(path.distance_m > 170.0);
}

#[test]
fn route_optimization_handles_5_10_25_and_50_haversine_stops() {
    let (_temp, db) = open_temp();

    for count in [5, 10, 25, 50] {
        let stops = route_fixture_stops(count);
        let route = db.optimize_route("roads", &stops).unwrap();

        assert_eq!(route.distance_mode, "haversine");
        assert_eq!(route.heuristic, "nearest_neighbor_2opt");
        assert_eq!(route.ordered_stops.len(), count);
        assert_eq!(route.ordered_stops[0].input_index, 0);
        assert!(route.estimated_distance_m.is_finite());
    }
}

#[test]
fn route_optimization_is_no_worse_than_naive_input_order() {
    let (_temp, db) = open_temp();
    let stops = [0.0, 4.0, 1.0, 3.0, 2.0]
        .into_iter()
        .map(|lon| Geometry::point(lon, 0.0).unwrap())
        .collect::<Vec<_>>();

    let route = db.optimize_route("roads", &stops).unwrap();

    assert!(route.estimated_distance_m <= naive_route_distance(&stops) + 1e-9);
    assert_eq!(
        route
            .ordered_stops
            .iter()
            .map(|stop| stop.input_index)
            .collect::<Vec<_>>(),
        vec![0, 2, 4, 3, 1]
    );
}

#[test]
fn route_optimization_uses_route_graph_when_available() {
    let (_temp, mut db) = open_temp();
    insert_complete_test_roads(&mut db, 5);
    let stops = (0..5)
        .rev()
        .map(|idx| Geometry::point(idx as f64, 0.0).unwrap())
        .collect::<Vec<_>>();

    let route = db.optimize_route("roads", &stops).unwrap();

    assert_eq!(route.distance_mode, "route_graph");
    assert_eq!(route.estimated_distance_m, 40.0);
    assert_eq!(route.ordered_stops[0].input_index, 0);
}

#[test]
fn routing_disconnected_graph_returns_clear_no_route_error() {
    let (_temp, mut db) = open_temp();
    insert_toy_roads(&mut db);

    let error = db
        .shortest_path(
            "roads",
            &Geometry::point(0.0, 0.0).unwrap(),
            &Geometry::point(1.0, 1.0).unwrap(),
        )
        .unwrap_err()
        .to_string();

    assert!(error.contains("no route in road graph `roads`"));
}

#[test]
fn routing_graph_rebuilds_after_reopen() {
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(temp.path()).unwrap();
        insert_toy_roads(&mut db);
    }

    let db = BicDb::open(temp.path()).unwrap();
    let distance = db
        .route_distance(
            "roads",
            &Geometry::point(0.0, 0.0).unwrap(),
            &Geometry::point(0.002, 0.0).unwrap(),
        )
        .unwrap();
    assert_eq!(distance, 200.0);
}

#[test]
fn spatial_v0_1_vertical_slice_survives_reopen_and_uses_event_path() {
    let temp = tempfile::tempdir().unwrap();
    let pbf_dir = tempfile::tempdir().unwrap();
    let pbf_path = pbf_dir.path().join("tiny.osm.pbf");
    write_tiny_osm_pbf(&pbf_path);

    {
        let mut db = BicDb::open_with_config(
            temp.path(),
            DbConfig::default()
                .with_fsync(false)
                .with_audit_events(true),
        )
        .unwrap();
        db.create_collection("clinics").unwrap();
        db.batch_insert(
            "clinics",
            [
                Record::new("sf")
                    .with_geometry(Geometry::point(-122.4194, 37.7749).unwrap())
                    .with_vector(vec![1.0, 0.0])
                    .with_metadata(json!({"name": "San Francisco"})),
                Record::new("oak")
                    .with_geometry(Geometry::point(-122.2711, 37.8044).unwrap())
                    .with_vector(vec![0.8, 0.2])
                    .with_metadata(json!({"name": "Oakland"})),
                Record::new("la")
                    .with_geometry(Geometry::point(-118.2437, 34.0522).unwrap())
                    .with_vector(vec![0.0, 1.0])
                    .with_metadata(json!({"name": "Los Angeles"})),
            ],
        )
        .unwrap();
        db.create_spatial_index("clinics", "geometry").unwrap();

        assert_eq!(
            db.nearest("clinics", "geometry", -122.4194, 37.7749, 2)
                .unwrap()
                .into_iter()
                .map(|result| result.record.id)
                .collect::<Vec<_>>(),
            vec!["sf", "oak"]
        );
        assert_eq!(
            db.within_radius("clinics", "geometry", -122.4194, 37.7749, 20_000.0)
                .unwrap()
                .into_iter()
                .map(|result| result.record.id)
                .collect::<Vec<_>>(),
            vec!["sf", "oak"]
        );

        let import = db
            .import_osm_pbf(
                &pbf_path,
                OsmImportBbox {
                    min_lon: -122.42,
                    min_lat: 37.77,
                    max_lon: -122.41,
                    max_lat: 37.78,
                },
            )
            .unwrap();
        assert_eq!(import.graph, "roads");
        assert_eq!(import.nodes, 3);
        assert_eq!(import.edges, 4);
        db.close().unwrap();
    }

    let mut db = BicDb::open_with_config(
        temp.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_audit_events(true),
    )
    .unwrap();
    assert!(
        db.verify_index("idx_clinics_geometry_spatial")
            .unwrap()
            .valid
    );
    assert_eq!(
        db.nearest("clinics", "geometry", -122.4194, 37.7749, 1)
            .unwrap()[0]
            .record
            .id,
        "sf"
    );

    let path = db
        .shortest_path_with_events(
            "roads",
            &Geometry::point(-122.4194, 37.7749).unwrap(),
            &Geometry::point(-122.4174, 37.7749).unwrap(),
        )
        .unwrap();
    assert_eq!(
        path.node_ids,
        vec!["osm-node-1", "osm-node-2", "osm-node-3"]
    );

    let optimized = db
        .optimize_route_with_events(
            "roads",
            &[
                Geometry::point(-122.4194, 37.7749).unwrap(),
                Geometry::point(-122.4174, 37.7749).unwrap(),
                Geometry::point(-122.4184, 37.7749).unwrap(),
                Geometry::point(-122.4190, 37.7749).unwrap(),
                Geometry::point(-122.4178, 37.7749).unwrap(),
            ],
        )
        .unwrap();
    assert_eq!(optimized.distance_mode, "route_graph");
    assert_eq!(optimized.ordered_stops.len(), 5);

    let event_types = db
        .export_events_since(0)
        .into_iter()
        .filter(|stored| stored.event.stream == SPATIAL_AUDIT_STREAM)
        .map(|stored| stored.event.event_type)
        .collect::<Vec<_>>();
    assert!(event_types
        .iter()
        .any(|event| event == "SpatialRecordInserted"));
    assert!(event_types
        .iter()
        .any(|event| event == "SpatialIndexUpdated"));
    assert_eq!(
        event_types
            .iter()
            .filter(|event| event.as_str() == "RouteComputed")
            .count(),
        2
    );
}

#[test]
// Pinned to embedded_memory: asserts segment/dirty-set/residency mechanics
// that do not exist in paged mode (segments carry no rows there). The paged
// sweep (BICDB_STORAGE_MODE=server_paged) covers that engine's own contract.
fn compaction_rewrites_live_records_and_preserves_indexes_vectors_and_reopen() {
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(
            temp.path(),
            DbConfig::default()
                .with_storage_mode(StorageMode::EmbeddedMemory)
                .with_fsync(false),
        )
        .unwrap();
        db.create_collection("patients").unwrap();
        let large = "rural clinic note ".repeat(256);

        for round in 0..5 {
            db.batch_insert(
                "patients",
                (0..40)
                    .map(|idx| {
                        Record::new(format!("p-{idx:03}"))
                            .with_vector(vec![idx as f32 + round as f32, 1.0])
                            .with_metadata(json!({
                                "clinic": if idx % 2 == 0 { "rural-7" } else { "urban-2" },
                                "round": round,
                                "body": large,
                            }))
                    })
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        }
        db.compact().unwrap();
        for idx in 0..10 {
            assert!(db.delete("patients", &format!("p-{idx:03}")).unwrap());
        }
        db.create_index(IndexDefinition {
            name: "idx_patients_clinic".to_string(),
            collection: "patients".to_string(),
            fields: vec![IndexField::MetadataPath(vec!["clinic".to_string()])],
            unique: false,
            kind: IndexKind::BTree,
            predicate: None,
            exclusion: None,
        })
        .unwrap();

        let before = db
            .stats()
            .unwrap()
            .collections
            .iter()
            .find(|collection| collection.name == "patients")
            .unwrap()
            .segment_bytes;
        let report = db.compact_collection("patients").unwrap();
        assert_eq!(report.collections.len(), 1);
        assert_eq!(report.collections[0].live_records, 30);
        assert!(report.bytes_reclaimed > 0);

        let after = db
            .stats()
            .unwrap()
            .collections
            .iter()
            .find(|collection| collection.name == "patients")
            .unwrap()
            .segment_bytes;
        assert!(after < before, "before={before}, after={after}");
        assert_eq!(db.scan_collection("patients").unwrap().len(), 30);
        assert!(db.get("patients", "p-000").unwrap().is_none());
        assert_eq!(
            db.lookup_index("idx_patients_clinic", &[IndexValue::from("rural-7")])
                .unwrap()
                .len(),
            15
        );
        assert_eq!(
            db.search_vector("patients", &[39.0 + 4.0, 1.0], 1, None)
                .unwrap()[0]
                .record
                .id,
            "p-039"
        );
    }

    let db = BicDb::open_with_config(
        temp.path(),
        DbConfig::default().with_storage_mode(StorageMode::EmbeddedMemory),
    )
    .unwrap();
    assert_eq!(db.scan_collection("patients").unwrap().len(), 30);
    assert!(db.get("patients", "p-000").unwrap().is_none());
    assert!(db.verify_index("idx_patients_clinic").unwrap().valid);
}

#[test]
fn compaction_checkpoints_committed_transaction_log_frames() {
    let (temp, mut db) = open_temp();
    db.create_collection("patients").unwrap();
    let mut tx = db.begin_transaction().unwrap();
    tx.insert(
        "patients",
        Record::new("p1").with_metadata(json!({"name": "Asha"})),
    )
    .unwrap();
    tx.commit().unwrap();

    let tx_log = temp.path().join(DEFAULT_TRANSACTION_LOG);
    assert!(fs::metadata(&tx_log).unwrap().len() > 0);

    let report = db.compact().unwrap();
    assert!(report.checkpoint_id.is_some());
    assert!(report.transaction_log_bytes_before > 0);
    assert_eq!(report.transaction_log_bytes_after, 0);
    assert_eq!(fs::metadata(&tx_log).unwrap().len(), 0);
    assert_eq!(
        db.get("patients", "p1").unwrap().unwrap().metadata["name"],
        "Asha"
    );

    drop(db);
    let reopened = BicDb::open(temp.path()).unwrap();
    assert_eq!(
        reopened.get("patients", "p1").unwrap().unwrap().metadata["name"],
        "Asha"
    );
}

#[test]
fn compaction_checkpoints_event_and_sync_logs_consistently() {
    let (temp, mut db) = open_temp();
    db.create_collection("patients").unwrap();
    db.insert(
        "patients",
        Record::new("p1").with_metadata(json!({"name": "Asha"})),
    )
    .unwrap();
    db.events_mut()
        .append(Event::new(
            "application.audit",
            "InvoicePosted",
            json!({"invoice": "i1"}),
        ))
        .unwrap();
    let pending_before = db.pending_sync_ops().len();
    assert!(pending_before > 0);

    let event_log = temp.path().join("events").join("events.seg");
    let sync_log = temp.path().join("sync.log");
    assert!(fs::metadata(&event_log).unwrap().len() > 0);
    assert!(fs::metadata(&sync_log).unwrap().len() > 0);

    let report = db.compact().unwrap();
    assert!(report.event_log_bytes_before >= report.event_log_bytes_after);
    assert!(report.sync_log_bytes_before >= report.sync_log_bytes_after);
    assert_eq!(db.events().read("application.audit").len(), 1);
    assert_eq!(db.pending_sync_ops().len(), pending_before);

    drop(db);
    let reopened = BicDb::open(temp.path()).unwrap();
    assert_eq!(reopened.events().read("application.audit").len(), 1);
    assert_eq!(reopened.pending_sync_ops().len(), pending_before);
}

#[test]
fn transaction_commit_persists() {
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(temp.path()).unwrap();
        db.create_collection("patients").unwrap();
        let mut tx = db.begin_transaction().unwrap();
        tx.insert(
            "patients",
            Record::new("p1").with_metadata(json!({"name": "Asha"})),
        )
        .unwrap();
        tx.commit().unwrap();
        assert_eq!(
            db.get("patients", "p1").unwrap().unwrap().metadata["name"],
            "Asha"
        );
    }

    let db = BicDb::open(temp.path()).unwrap();
    assert!(db.get("patients", "p1").unwrap().is_some());
}

#[test]
fn transaction_rollback_discards_writes() {
    let (_temp, mut db) = open_temp();
    db.create_collection("patients").unwrap();
    let mut tx = db.begin_transaction().unwrap();
    tx.insert("patients", Record::new("p1")).unwrap();
    assert!(db.get("patients", "p1").unwrap().is_none());
    tx.rollback().unwrap();
    assert!(db.get("patients", "p1").unwrap().is_none());
}

#[test]
fn recovery_ignores_transaction_writes_without_commit() {
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(temp.path()).unwrap();
        db.create_collection("patients").unwrap();
        let mut tx = db.begin_transaction().unwrap();
        tx.insert("patients", Record::new("p1")).unwrap();
        drop(tx);
        drop(db);
    }

    let db = BicDb::open(temp.path()).unwrap();
    assert!(db.get("patients", "p1").unwrap().is_none());
}

#[test]
fn recovery_applies_committed_transaction_without_close() {
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(temp.path()).unwrap();
        db.create_collection("patients").unwrap();
        let mut tx = db.begin_transaction().unwrap();
        tx.insert("patients", Record::new("p1")).unwrap();
        tx.commit().unwrap();
        drop(db);
    }

    let db = BicDb::open(temp.path()).unwrap();
    assert!(db.get("patients", "p1").unwrap().is_some());
}

#[test]
fn recovery_does_not_replay_transaction_over_later_segment_delete() {
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db =
            BicDb::open_with_config(temp.path(), DbConfig::default().with_fsync(false)).unwrap();
        db.create_collection("jobs").unwrap();
        db.create_index(IndexDefinition {
            name: "idx_jobs_queued_migration_version".to_string(),
            collection: "jobs".to_string(),
            fields: vec![IndexField::MetadataPath(vec![
                "queued_migration_version".to_string()
            ])],
            unique: true,
            kind: IndexKind::BTree,
            predicate: None,
            exclusion: None,
        })
        .unwrap();

        let mut tx = db.begin_transaction().unwrap();
        tx.insert(
            "jobs",
            Record::new("job-1").with_metadata(json!({"queued_migration_version": "v1"})),
        )
        .unwrap();
        tx.commit().unwrap();
        db.delete("jobs", "job-1").unwrap();
        db.insert(
            "jobs",
            Record::new("job-2").with_metadata(json!({"queued_migration_version": "v1"})),
        )
        .unwrap();
        assert!(
            fs::metadata(temp.path().join(DEFAULT_TRANSACTION_LOG))
                .unwrap()
                .len()
                > 0
        );
    }

    let db = BicDb::open_with_config(temp.path(), DbConfig::default().with_fsync(false)).unwrap();
    assert!(db.get("jobs", "job-1").unwrap().is_none());
    assert_eq!(
        db.get("jobs", "job-2").unwrap().unwrap().metadata["queued_migration_version"],
        "v1"
    );
    assert!(
        db.verify_index("idx_jobs_queued_migration_version")
            .unwrap()
            .valid
    );
}

#[test]
// Pinned to embedded_memory: asserts the transaction log's exact length
// after recovery. In paged mode open truncates the spent log entirely
// (its contents live in the page store), so the embedded arithmetic
// does not apply.
fn partial_transaction_frame_is_truncated_on_recovery() {
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(
            temp.path(),
            DbConfig::default().with_storage_mode(StorageMode::EmbeddedMemory),
        )
        .unwrap();
        db.create_collection("patients").unwrap();
        let mut tx = db.begin_transaction().unwrap();
        tx.insert("patients", Record::new("p1")).unwrap();
        tx.commit().unwrap();
    }

    let tx_log = temp.path().join(DEFAULT_TRANSACTION_LOG);
    let before_len = std::fs::metadata(&tx_log).unwrap().len();
    let mut file = OpenOptions::new().append(true).open(&tx_log).unwrap();
    file.write_all(b"partial-tx-frame").unwrap();
    drop(file);
    assert!(std::fs::metadata(&tx_log).unwrap().len() > before_len);

    let db = BicDb::open_with_config(
        temp.path(),
        DbConfig::default().with_storage_mode(StorageMode::EmbeddedMemory),
    )
    .unwrap();
    assert!(db.get("patients", "p1").unwrap().is_some());
    assert_eq!(std::fs::metadata(&tx_log).unwrap().len(), before_len);
}

#[test]
fn crash_boundary_recovery_handles_small_and_large_transactions() {
    for record_count in [1_usize, 250] {
        let temp = tempfile::tempdir().unwrap();
        {
            let mut db = BicDb::open(temp.path()).unwrap();
            db.create_collection("patients").unwrap();
            let mut tx = db.begin_transaction().unwrap();
            for index in 0..record_count {
                tx.insert(
                    "patients",
                    Record::new(format!("pending-{index:04}"))
                        .with_metadata(json!({"batch": "pending", "index": index})),
                )
                .unwrap();
            }
            drop(tx);
            drop(db);
        }
        // Scoped, so the handle is closed before the next open. Holding several
        // writable handles to one database at once is not a supported pattern —
        // the paged engine refuses it outright, and the in-memory engine only
        // tolerates it by luck.
        {
            let db = BicDb::open(temp.path()).unwrap();
            assert_eq!(db.scan_collection("patients").unwrap().len(), 0);
        }

        {
            let db = BicDb::open(temp.path()).unwrap();
            let mut tx = db.begin_transaction().unwrap();
            for index in 0..record_count {
                tx.insert(
                    "patients",
                    Record::new(format!("committed-{index:04}"))
                        .with_metadata(json!({"batch": "committed", "index": index})),
                )
                .unwrap();
            }
            tx.commit().unwrap();
            drop(db);
        }
        let db = BicDb::open(temp.path()).unwrap();
        assert_eq!(db.scan_collection("patients").unwrap().len(), record_count);
        assert!(db
            .get("patients", &format!("committed-{:04}", record_count - 1))
            .unwrap()
            .is_some());
    }
}

#[test]
// Pinned to embedded_memory: asserts segment/dirty-set/residency mechanics
// that do not exist in paged mode (segments carry no rows there). The paged
// sweep (BICDB_STORAGE_MODE=server_paged) covers that engine's own contract.
fn partial_trailing_frames_are_rejected_by_verify_and_truncated_on_open() {
    for file_name in ["record", "event", "sync", "transaction"] {
        let temp = tempfile::tempdir().unwrap();
        {
            let mut db = BicDb::open_with_config(
                temp.path(),
                DbConfig::default()
                    .with_storage_mode(StorageMode::EmbeddedMemory)
                    .with_fsync(false)
                    .with_audit_events(true),
            )
            .unwrap();
            db.create_collection("patients").unwrap();
            db.insert("patients", Record::new("p1")).unwrap();
            let mut tx = db.begin_transaction().unwrap();
            tx.insert("patients", Record::new("p2")).unwrap();
            tx.commit().unwrap();
            db.close().unwrap();
        }

        let path = match file_name {
            "record" => record_segment_path(temp.path(), "patients"),
            "event" => event_segment_path(temp.path()),
            "sync" => temp.path().join("sync.log"),
            "transaction" => temp.path().join(DEFAULT_TRANSACTION_LOG),
            _ => unreachable!(),
        };
        let clean_len = fs::metadata(&path).unwrap().len();
        append_partial_frame(&path);

        assert!(
            BicDb::verify_path(
                temp.path(),
                DbConfig::default().with_storage_mode(StorageMode::EmbeddedMemory),
                None
            )
            .is_err(),
            "{file_name} partial frame should fail strict verification"
        );

        let db = BicDb::open_with_config(
            temp.path(),
            DbConfig::default().with_storage_mode(StorageMode::EmbeddedMemory),
        )
        .unwrap();
        assert!(db.get("patients", "p1").unwrap().is_some());
        assert!(db.get("patients", "p2").unwrap().is_some());
        assert_eq!(fs::metadata(&path).unwrap().len(), clean_len);
        BicDb::verify_path(
            temp.path(),
            DbConfig::default().with_storage_mode(StorageMode::EmbeddedMemory),
            None,
        )
        .unwrap();
    }
}

#[test]
// Pinned to embedded_memory: asserts segment/dirty-set/residency mechanics
// that do not exist in paged mode (segments carry no rows there). The paged
// sweep (BICDB_STORAGE_MODE=server_paged) covers that engine's own contract.
fn intentional_frame_corruption_is_detected_and_not_silently_repaired() {
    for file_name in ["record", "event", "sync", "transaction"] {
        let temp = tempfile::tempdir().unwrap();
        {
            let mut db = BicDb::open_with_config(
                temp.path(),
                DbConfig::default()
                    .with_storage_mode(StorageMode::EmbeddedMemory)
                    .with_fsync(false)
                    .with_audit_events(true),
            )
            .unwrap();
            db.create_collection("patients").unwrap();
            db.insert("patients", Record::new("p1")).unwrap();
            let mut tx = db.begin_transaction().unwrap();
            tx.insert("patients", Record::new("p2")).unwrap();
            tx.commit().unwrap();
            db.close().unwrap();
        }

        let path = match file_name {
            "record" => record_segment_path(temp.path(), "patients"),
            "event" => event_segment_path(temp.path()),
            "sync" => temp.path().join("sync.log"),
            "transaction" => temp.path().join(DEFAULT_TRANSACTION_LOG),
            _ => unreachable!(),
        };
        corrupt_first_byte(&path);

        assert!(
            BicDb::verify_path(
                temp.path(),
                DbConfig::default().with_storage_mode(StorageMode::EmbeddedMemory),
                None
            )
            .is_err(),
            "{file_name} corruption should fail strict verification"
        );
        let reopened = BicDb::open_with_config(
            temp.path(),
            DbConfig::default().with_storage_mode(StorageMode::EmbeddedMemory),
        )
        .unwrap_or_else(|error| {
            panic!("{file_name} corruption at offset 0 must be recoverable: {error}")
        });
        assert_eq!(fs::metadata(&path).unwrap().len(), 0);
        drop(reopened);
        BicDb::verify_path(
            temp.path(),
            DbConfig::default().with_storage_mode(StorageMode::EmbeddedMemory),
            None,
        )
        .unwrap();
    }
}

#[test]
fn snapshot_keeps_old_version_after_committed_update() {
    let (_temp, mut db) = open_temp();
    db.create_collection("patients").unwrap();
    db.insert(
        "patients",
        Record::new("p1").with_metadata(json!({"name": "old"})),
    )
    .unwrap();
    let snapshot = db.snapshot().unwrap();

    let mut tx = db.begin_transaction().unwrap();
    tx.update(
        "patients",
        Record::new("p1").with_metadata(json!({"name": "new"})),
    )
    .unwrap();
    tx.commit().unwrap();

    assert_eq!(
        snapshot.get("patients", "p1").unwrap().metadata["name"],
        "old"
    );
    assert_eq!(
        db.snapshot()
            .unwrap()
            .get("patients", "p1")
            .unwrap()
            .metadata["name"],
        "new"
    );
}

#[test]
fn uncommitted_write_is_invisible_to_readers() {
    let (_temp, mut db) = open_temp();
    db.create_collection("patients").unwrap();
    let mut tx = db.begin_transaction().unwrap();
    tx.insert("patients", Record::new("p1")).unwrap();

    assert!(db.get("patients", "p1").unwrap().is_none());
    assert!(db.snapshot().unwrap().get("patients", "p1").is_none());

    tx.commit().unwrap();
    assert!(db.get("patients", "p1").unwrap().is_some());
}

#[test]
fn collection_generations_track_committed_record_mutations() {
    let (_temp, mut db) = open_temp();
    db.create_collection("patients").unwrap();
    assert_eq!(db.collection_generation("patients"), 0);

    db.insert("patients", Record::new("p1")).unwrap();
    assert_eq!(db.collection_generation("patients"), 1);

    db.insert(
        "patients",
        Record::new("p1").with_metadata(json!({"name": "new"})),
    )
    .unwrap();
    assert_eq!(db.collection_generation("patients"), 2);

    db.delete("patients", "p1").unwrap();
    assert_eq!(db.collection_generation("patients"), 3);

    let mut tx = db.begin_transaction().unwrap();
    tx.insert("patients", Record::new("p2")).unwrap();
    assert_eq!(db.collection_generation("patients"), 3);
    tx.commit().unwrap();
    assert_eq!(db.collection_generation("patients"), 4);
}

#[test]
fn concurrent_write_conflict_is_reported() {
    let (_temp, mut db) = open_temp();
    db.create_collection("patients").unwrap();
    let mut tx1 = db.begin_transaction().unwrap();
    tx1.insert("patients", Record::new("p1")).unwrap();

    let mut tx2 = db.begin_transaction().unwrap();
    let error = tx2.insert("patients", Record::new("p1")).unwrap_err();
    assert!(matches!(error, BicDbError::TransactionConflict(_)));

    tx1.rollback().unwrap();
    tx2.rollback().unwrap();
}

#[test]
fn read_committed_recheck_skips_fresh_rows_but_still_locks_them() {
    let (_temp, mut db) = open_temp();
    db.create_collection("patients").unwrap();
    db.insert(
        "patients",
        Record::new("p1").with_metadata(json!({"version": 0})),
    )
    .unwrap();

    let mut tx = db.begin_transaction().unwrap();
    // Nothing committed past the snapshot: the recheck reports "fresh" (None)
    // without materializing the record, and the caller keeps its candidate.
    assert!(tx
        .read_committed_update_record("patients", "p1")
        .unwrap()
        .is_none());

    // The recheck must still have taken the row lock, so a competing writer
    // fails instead of sneaking a commit between recheck and commit.
    let mut competing = db.begin_transaction().unwrap();
    let error = competing
        .insert(
            "patients",
            Record::new("p1").with_metadata(json!({"version": 9})),
        )
        .unwrap_err();
    assert!(matches!(error, BicDbError::TransactionConflict(_)));
    competing.rollback().unwrap();

    tx.write_upserts_with_statement_snapshots(
        "patients",
        [(Record::new("p1").with_metadata(json!({"version": 1})), 0)],
    )
    .unwrap();
    tx.commit().unwrap();
    assert_eq!(
        db.get("patients", "p1").unwrap().unwrap().metadata["version"],
        1
    );
}

#[test]
fn rollback_releases_read_committed_locks_without_buffered_writes() {
    let (_temp, mut db) = open_temp();
    db.create_collection("cells").unwrap();
    db.insert(
        "cells",
        Record::new("cell-a").with_metadata(json!({"due_date": "2026-08-01"})),
    )
    .unwrap();
    db.insert(
        "cells",
        Record::new("cell-b").with_metadata(json!({"due_date": "2026-08-02"})),
    )
    .unwrap();

    let mut aborted = db.begin_transaction().unwrap();
    assert!(aborted
        .read_committed_update_record("cells", "cell-a")
        .unwrap()
        .is_none());
    assert!(aborted
        .read_committed_update_record("cells", "cell-b")
        .unwrap()
        .is_none());
    assert_eq!(aborted.write_len(), 0);
    aborted.rollback().unwrap();

    let mut retry = db.begin_transaction().unwrap();
    retry
        .update(
            "cells",
            Record::new("cell-a").with_metadata(json!({"due_date": "2026-08-03"})),
        )
        .unwrap();
    retry
        .update(
            "cells",
            Record::new("cell-b").with_metadata(json!({"due_date": "2026-08-04"})),
        )
        .unwrap();
    retry.commit().unwrap();
}

#[test]
fn dropping_aborted_transaction_releases_prewrite_row_lock() {
    let (_temp, mut db) = open_temp();
    db.create_collection("cells").unwrap();
    db.insert("cells", Record::new("cell-a")).unwrap();

    {
        let mut aborted = db.begin_transaction().unwrap();
        assert!(aborted
            .read_committed_update_record("cells", "cell-a")
            .unwrap()
            .is_none());
        assert_eq!(aborted.write_len(), 0);
    }

    let mut retry = db.begin_transaction().unwrap();
    retry
        .update(
            "cells",
            Record::new("cell-a").with_metadata(json!({"status": "recovered"})),
        )
        .unwrap();
    retry.commit().unwrap();
}

#[test]
fn savepoint_rollback_releases_only_prewrite_locks_acquired_after_mark() {
    let (_temp, mut db) = open_temp();
    db.create_collection("cells").unwrap();
    db.insert("cells", Record::new("kept-lock")).unwrap();
    db.insert("cells", Record::new("released-lock")).unwrap();

    let mut transaction = db.begin_transaction().unwrap();
    transaction
        .read_committed_update_record("cells", "kept-lock")
        .unwrap();
    let write_len = transaction.write_len();
    let lock_len = transaction.lock_len();
    transaction
        .read_committed_update_record("cells", "released-lock")
        .unwrap();
    transaction
        .truncate_writes_and_locks(write_len, lock_len)
        .unwrap();

    let mut competing = db.begin_transaction().unwrap();
    competing
        .update(
            "cells",
            Record::new("released-lock").with_metadata(json!({"updated": true})),
        )
        .unwrap();
    let kept_error = competing
        .update(
            "cells",
            Record::new("kept-lock").with_metadata(json!({"updated": true})),
        )
        .unwrap_err();
    assert!(matches!(kept_error, BicDbError::TransactionConflict(_)));

    transaction.rollback().unwrap();
    competing.commit().unwrap();
}

#[test]
fn commit_time_delta_repair_merges_concurrent_counter_updates() {
    use bicdb_core::{RepairDelta, RepairPlan};
    let (_temp, mut db) = open_temp();
    db.create_collection("warehouse").unwrap();
    db.insert(
        "warehouse",
        Record::new("w1").with_metadata(json!({"w_ytd": 100.0, "w_name": "alpha"})),
    )
    .unwrap();

    // Both transactions read the same snapshot (w_ytd = 100) and buffer
    // absolute records computed from it, carrying their deltas. Neither takes
    // the statement-time row lock.
    let mut tx1 = db.begin_transaction().unwrap();
    let mut tx2 = db.begin_transaction().unwrap();
    tx1.write_upserts_with_repairs(
        "warehouse",
        [(
            Record::new("w1").with_metadata(json!({"w_ytd": 110.0, "w_name": "alpha"})),
            0,
            Some(RepairPlan {
                deltas: vec![("w_ytd".to_string(), RepairDelta::Float(10.0))],
            }),
        )],
    )
    .unwrap();
    tx2.write_upserts_with_repairs(
        "warehouse",
        [(
            Record::new("w1").with_metadata(json!({"w_ytd": 105.0, "w_name": "alpha"})),
            0,
            Some(RepairPlan {
                deltas: vec![("w_ytd".to_string(), RepairDelta::Float(5.0))],
            }),
        )],
    )
    .unwrap();

    tx1.commit().unwrap();
    // Without repair this second commit is a serialization failure; with it,
    // the record is rebuilt as latest (110) + delta (5).
    tx2.commit().unwrap();

    let final_record = db.get("warehouse", "w1").unwrap().unwrap();
    assert_eq!(final_record.metadata["w_ytd"], 115.0);
    assert_eq!(final_record.metadata["w_name"], "alpha");
}

#[test]
fn delta_repair_rebuilds_even_when_floored_snapshot_covers_the_conflict() {
    use bicdb_core::{RepairDelta, RepairPlan};
    let (_temp, mut db) = open_temp();
    db.create_collection("district").unwrap();
    db.insert(
        "district",
        Record::new("d1").with_metadata(json!({"next_o_id": 100, "ytd": 50.0})),
    )
    .unwrap();

    // Simulate the read-your-writes floor raising the snapshot ABOVE the
    // applied watermark: the floored snapshot will cover the concurrent commit
    // below, but this transaction's (hypothetical) read physically predates
    // it. The dev9 signature of this bug: a payment "fresh"-pathed its stale
    // absolute district record and regressed d_next_o_id, minting duplicate
    // order ids.
    let mut floored = db.begin_transaction_after(u64::MAX / 2).unwrap();

    // Concurrent committer bumps the counter (commit seq is far below the floor).
    db.insert(
        "district",
        Record::new("d1").with_metadata(json!({"next_o_id": 101, "ytd": 50.0})),
    )
    .unwrap();

    // The floored transaction buffers an absolute record computed from a STALE
    // read (next_o_id still 100) plus a ytd delta.
    floored
        .write_upserts_with_repairs(
            "district",
            [(
                Record::new("d1").with_metadata(json!({"next_o_id": 100, "ytd": 57.5})),
                0,
                Some(RepairPlan {
                    deltas: vec![("ytd".to_string(), RepairDelta::Float(7.5))],
                }),
            )],
        )
        .unwrap();
    floored.commit().unwrap();

    let final_record = db.get("district", "d1").unwrap().unwrap();
    // The repair must rebuild from the latest committed version: the counter
    // bump survives, the delta applies on top of it.
    assert_eq!(final_record.metadata["next_o_id"], 101);
    assert_eq!(final_record.metadata["ytd"], 57.5);
}

#[test]
fn floored_snapshot_cannot_mask_a_point_read_lost_update() {
    let (_temp, mut db) = open_temp();
    db.create_collection("counters").unwrap();
    db.insert(
        "counters",
        Record::new("shared").with_metadata(json!({"value": 100})),
    )
    .unwrap();

    // The floor deliberately covers the winner's future commit sequence, but
    // the point read below physically observes only value=100.
    let mut stale = db.begin_transaction_after(u64::MAX / 2).unwrap();
    let stale_value = stale.get("counters", "shared").unwrap().unwrap().metadata["value"]
        .as_i64()
        .unwrap();

    db.insert(
        "counters",
        Record::new("shared").with_metadata(json!({"value": stale_value + 1})),
    )
    .unwrap();

    stale
        .update(
            "counters",
            Record::new("shared").with_metadata(json!({"value": stale_value - 1})),
        )
        .unwrap();
    assert!(matches!(
        stale.commit(),
        Err(BicDbError::TransactionConflict(_))
    ));
    assert_eq!(
        db.get("counters", "shared").unwrap().unwrap().metadata["value"],
        101,
        "the winner must survive a stale write hidden beneath a raised snapshot floor"
    );
}

#[test]
fn floored_snapshot_cannot_mask_a_batch_point_read_lost_update() {
    let (_temp, mut db) = open_temp();
    db.create_collection("stock").unwrap();
    db.insert(
        "stock",
        Record::new("shared").with_metadata(json!({"quantity": 44, "orders": 4})),
    )
    .unwrap();
    let mut stale = db.begin_transaction_after(u64::MAX / 2).unwrap();
    let rows = stale
        .get_stored_by_pks("stock", &["shared".to_string()])
        .unwrap();
    let bicdb_core::VisibleRow::Stored(previous) = rows[0].as_ref().unwrap() else {
        panic!("expected a resident stock row");
    };
    assert_eq!(previous.to_record().unwrap().metadata["quantity"], 44);
    db.insert(
        "stock",
        Record::new("shared").with_metadata(json!({"quantity": 38, "orders": 5})),
    )
    .unwrap();
    stale
        .write_stored_updates_with_changes(
            "stock",
            [(
                Some(std::sync::Arc::clone(previous)),
                std::sync::Arc::new(
                    bicdb_core::StoredRecord::from_record(
                        &Record::new("shared").with_metadata(json!({"quantity": 39, "orders": 5})),
                    )
                    .unwrap(),
                ),
                0,
                None,
                Some(std::sync::Arc::from([
                    Box::<str>::from("quantity"),
                    Box::<str>::from("orders"),
                ])),
            )],
        )
        .unwrap();
    assert!(matches!(
        stale.commit(),
        Err(BicDbError::TransactionConflict(_))
    ));
    assert_eq!(
        db.get("stock", "shared").unwrap().unwrap().metadata,
        json!({"quantity": 38, "orders": 5})
    );
}

#[test]
fn batch_point_reread_retains_earlier_observation() {
    let (_temp, mut db) = open_temp();
    db.create_collection("rows").unwrap();
    db.insert("rows", Record::new("r").with_metadata(json!({"v": 1})))
        .unwrap();
    let mut stale = db.begin_transaction_after(u64::MAX / 2).unwrap();
    let keys = ["r".to_string(), "r".to_string()];
    let first = stale.get_stored_by_pks("rows", &keys).unwrap();
    assert_eq!(first.len(), 2);
    assert_eq!(
        first[0].as_ref().unwrap().to_record().unwrap().metadata["v"],
        1
    );
    db.insert("rows", Record::new("r").with_metadata(json!({"v": 2})))
        .unwrap();
    let later = stale.get_stored_by_pks("rows", &keys).unwrap();
    assert_eq!(
        later[1].as_ref().unwrap().to_record().unwrap().metadata["v"],
        2
    );
    stale
        .update("rows", Record::new("r").with_metadata(json!({"v": 3})))
        .unwrap();
    assert!(matches!(
        stale.commit(),
        Err(BicDbError::TransactionConflict(_))
    ));
    assert_eq!(db.get("rows", "r").unwrap().unwrap().metadata["v"], 2);
}

#[test]
fn batch_point_absence_cannot_be_hidden_by_a_snapshot_floor() {
    let (_temp, mut db) = open_temp();
    db.create_collection("rows").unwrap();
    let mut stale = db.begin_transaction_after(u64::MAX / 2).unwrap();
    let rows = stale.get_stored_by_pks("rows", &["r".to_string()]).unwrap();
    assert!(rows[0].is_none());
    db.insert("rows", Record::new("r").with_metadata(json!({"v": 1})))
        .unwrap();
    stale
        .insert("rows", Record::new("r").with_metadata(json!({"v": 2})))
        .unwrap();
    assert!(matches!(
        stale.commit(),
        Err(BicDbError::TransactionConflict(_))
    ));
    assert_eq!(db.get("rows", "r").unwrap().unwrap().metadata["v"], 1);
}

#[test]
fn read_committed_refresh_allows_repeated_same_tx_writes_to_commit() {
    let (_temp, mut db) = open_temp();
    db.create_collection("patients").unwrap();
    db.insert(
        "patients",
        Record::new("p1").with_metadata(json!({"version": 0})),
    )
    .unwrap();

    let mut long_tx = db.begin_transaction().unwrap();
    db.insert(
        "patients",
        Record::new("p1").with_metadata(json!({"version": 1})),
    )
    .unwrap();

    let (latest, statement_snapshot) = long_tx
        .read_committed_update_record("patients", "p1")
        .unwrap()
        .expect("row should be refreshed after snapshot");
    assert_eq!(latest.unwrap().metadata["version"], 1);
    assert!(statement_snapshot > 0);

    long_tx
        .write_upserts_with_statement_snapshots(
            "patients",
            [(
                Record::new("p1").with_metadata(json!({"version": 2})),
                statement_snapshot,
            )],
        )
        .unwrap();
    // A later write to the same record skips the recheck (the record already
    // has a pending write) and carries snapshot 0; commit must judge the record
    // by the refreshed sibling snapshot instead of false-conflicting.
    long_tx
        .write_upserts_with_statement_snapshots(
            "patients",
            [(Record::new("p1").with_metadata(json!({"version": 3})), 0)],
        )
        .unwrap();
    long_tx.commit().unwrap();

    assert_eq!(
        db.get("patients", "p1").unwrap().unwrap().metadata["version"],
        3
    );
}

#[test]
fn savepoint_truncate_releases_rolled_back_write_locks() {
    let (_temp, mut db) = open_temp();
    db.create_collection("patients").unwrap();

    let mut tx1 = db.begin_transaction().unwrap();
    let before_patient = tx1.write_len();
    tx1.insert("patients", Record::new("p1")).unwrap();
    tx1.truncate_writes(before_patient).unwrap();

    let mut tx2 = db.begin_transaction().unwrap();
    tx2.insert("patients", Record::new("p1")).unwrap();

    tx1.rollback().unwrap();
    tx2.commit().unwrap();
    assert!(db.get("patients", "p1").unwrap().is_some());
}

#[test]
fn recovery_honors_savepoint_truncated_transaction_writes() {
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(temp.path(), DbConfig::default()).unwrap();
        db.create_collection("patients").unwrap();

        let mut tx = db.begin_transaction().unwrap();
        tx.insert("patients", Record::new("kept")).unwrap();
        let before_rolled_back = tx.write_len();
        tx.insert("patients", Record::new("rolled-back")).unwrap();
        tx.truncate_writes(before_rolled_back).unwrap();
        tx.commit().unwrap();
    }

    let db = BicDb::open(temp.path()).unwrap();
    assert!(db.get("patients", "kept").unwrap().is_some());
    assert!(db.get("patients", "rolled-back").unwrap().is_none());
}

#[test]
fn erp_inventory_transfer_conflict_prevents_double_spend() {
    let (_temp, mut db) = open_temp();
    db.create_collection("inventory").unwrap();
    db.insert(
        "inventory",
        Record::new("tenant-a:sku-1:main").with_metadata(json!({"qty": 10})),
    )
    .unwrap();

    let mut transfer_a = db.begin_transaction().unwrap();
    transfer_a
        .update(
            "inventory",
            Record::new("tenant-a:sku-1:main").with_metadata(json!({"qty": 4})),
        )
        .unwrap();

    let mut transfer_b = db.begin_transaction().unwrap();
    let conflict = transfer_b
        .update(
            "inventory",
            Record::new("tenant-a:sku-1:main").with_metadata(json!({"qty": 3})),
        )
        .unwrap_err();

    assert!(matches!(conflict, BicDbError::TransactionConflict(_)));
    transfer_a.commit().unwrap();
    transfer_b.rollback().unwrap();

    let inventory = db.get("inventory", "tenant-a:sku-1:main").unwrap().unwrap();
    assert_eq!(inventory.metadata["qty"], 4);
}

#[test]
fn reopen_recovers_records() {
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(temp.path(), DbConfig::default()).unwrap();
        db.create_collection("memories").unwrap();
        db.insert(
            "memories",
            Record::new("note-1")
                .with_vector(vec![0.2, 0.4])
                .with_metadata(json!({"kind": "note"})),
        )
        .unwrap();
        db.close().unwrap();
    }

    let db = BicDb::open(temp.path()).unwrap();
    let recovered = db.get("memories", "note-1").unwrap().unwrap();
    assert_eq!(recovered.id, "note-1");
    assert_eq!(recovered.vector, Some(vec![0.2, 0.4]));
    assert_eq!(db.pending_sync_ops().len(), 1);
}

#[test]
// Pinned to embedded_memory: asserts segment/dirty-set/residency mechanics
// that do not exist in paged mode (segments carry no rows there). The paged
// sweep (BICDB_STORAGE_MODE=server_paged) covers that engine's own contract.
fn corrupted_trailing_frame_is_ignored_and_truncated() {
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(
            temp.path(),
            DbConfig::default().with_storage_mode(StorageMode::EmbeddedMemory),
        )
        .unwrap();
        db.create_collection("patients").unwrap();
        db.insert(
            "patients",
            Record::new("ok").with_metadata(json!({"status": "valid"})),
        )
        .unwrap();
        db.compact().unwrap();
        db.close().unwrap();
    }

    let segment = temp.path().join("segments").join("patients.seg");
    let before_len = std::fs::metadata(&segment).unwrap().len();
    let mut file = OpenOptions::new().append(true).open(&segment).unwrap();
    file.write_all(b"corrupt-trailing-frame").unwrap();
    drop(file);
    assert!(std::fs::metadata(&segment).unwrap().len() > before_len);

    let db = BicDb::open_with_config(
        temp.path(),
        DbConfig::default().with_storage_mode(StorageMode::EmbeddedMemory),
    )
    .unwrap();
    assert!(db.get("patients", "ok").unwrap().is_some());
    assert_eq!(std::fs::metadata(&segment).unwrap().len(), before_len);
}

#[test]
fn vector_search_returns_best_matches_and_supports_filters() {
    let (_temp, mut db) = open_temp();
    db.create_collection("memories").unwrap();
    db.batch_insert(
        "memories",
        vec![
            Record::new("east")
                .with_vector(vec![1.0, 0.0])
                .with_metadata(json!({"kind": "note"})),
            Record::new("north")
                .with_vector(vec![0.0, 1.0])
                .with_metadata(json!({"kind": "note"})),
            Record::new("other")
                .with_vector(vec![0.9, 0.1])
                .with_metadata(json!({"kind": "task"})),
        ],
    )
    .unwrap();

    let results = db.search_vector("memories", &[1.0, 0.0], 2, None).unwrap();
    assert_eq!(results[0].record.id, "east");
    assert_eq!(results[1].record.id, "other");

    let filter = JsonFilter::new().eq("kind", "task");
    let filtered = db
        .search_vector("memories", &[1.0, 0.0], 2, Some(&filter))
        .unwrap();
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].record.id, "other");
}

#[test]
fn vector_profile_reports_phase_timings() {
    let (_temp, mut db) = open_temp();
    db.create_collection("vectors").unwrap();
    db.batch_insert(
        "vectors",
        vec![
            Record::new("a").with_vector(vec![1.0, 0.0]),
            Record::new("b").with_vector(vec![0.0, 1.0]),
            Record::new("c").with_vector(vec![0.8, 0.2]),
        ],
    )
    .unwrap();

    let profiled = db
        .profile_vector_search("vectors", &[1.0, 0.0], 2, None)
        .unwrap();

    assert_eq!(profiled.results.len(), 2);
    assert_eq!(profiled.profile.candidate_records, 3);
    assert_eq!(profiled.profile.vectors_read, 3);
    assert_eq!(profiled.profile.dim, 2);
    assert_eq!(profiled.profile.top_k, 2);
    assert!(profiled.profile.total >= profiled.profile.similarity);
}

#[test]
fn vector_store_rebuilds_after_upsert() {
    let (_temp, mut db) = open_temp();
    db.create_collection("vectors").unwrap();
    db.batch_insert(
        "vectors",
        vec![
            Record::new("old").with_vector(vec![1.0, 0.0]),
            Record::new("moved").with_vector(vec![0.0, 1.0]),
        ],
    )
    .unwrap();

    db.insert(
        "vectors",
        Record::new("moved").with_vector(vec![0.99, 0.01]),
    )
    .unwrap();

    let results = db.search_vector("vectors", &[1.0, 0.0], 2, None).unwrap();
    assert_eq!(results[0].record.id, "old");
    assert_eq!(results[1].record.id, "moved");
}

#[test]
fn hnsw_vector_index_searches_and_reports_recall_against_exact() {
    let (_temp, mut db) = open_temp();
    db.create_collection("memories").unwrap();
    db.batch_insert(
        "memories",
        (0..64)
            .map(|idx| {
                Record::new(format!("memory-{idx:03}"))
                    .with_vector(vec![idx as f32 / 64.0, 1.0 - (idx as f32 / 64.0)])
            })
            .collect::<Vec<_>>(),
    )
    .unwrap();

    let report = db
        .create_vector_index(
            "memories",
            HnswIndexConfig {
                m: 8,
                ef_construction: 24,
                ef_search: 24,
                distance: VectorMetric::Cosine,
            },
        )
        .unwrap();
    assert!(report.valid);
    assert_eq!(report.live_vectors, 64);

    let query = [0.5, 0.5];
    let exact = db.search_vector_exact("memories", &query, 10).unwrap();
    let ann = db.search_vector_ann("memories", &query, 10, 32).unwrap();
    let recall = recall_at(&ann, &exact);

    assert_eq!(ann.len(), 10);
    assert!(recall >= 0.8, "recall@10 was {recall}");
}

#[test]
fn hnsw_vector_index_persists_and_excludes_tombstoned_deletes() {
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(temp.path()).unwrap();
        db.create_collection("memories").unwrap();
        db.batch_insert(
            "memories",
            [
                Record::new("keep").with_vector(vec![1.0, 0.0]),
                Record::new("delete").with_vector(vec![0.99, 0.01]),
                Record::new("far").with_vector(vec![0.0, 1.0]),
            ],
        )
        .unwrap();
        db.create_vector_index("memories", HnswIndexConfig::default())
            .unwrap();
        assert!(db.delete("memories", "delete").unwrap());
        let report = db.verify_vector_index("memories").unwrap();
        assert!(report.valid);
        assert_eq!(report.live_vectors, 2);
        assert_eq!(report.tombstoned_vectors, 1);
    }

    let db = BicDb::open(temp.path()).unwrap();
    assert!(db.verify_vector_index("memories").unwrap().valid);
    let results = db
        .search_vector_ann("memories", &[1.0, 0.0], 3, 16)
        .unwrap();
    assert_eq!(results[0].record.id, "keep");
    assert!(!results.iter().any(|result| result.record.id == "delete"));
}

#[test]
fn hnsw_rebuild_fixes_index_after_record_updates() {
    let (_temp, mut db) = open_temp();
    db.create_collection("memories").unwrap();
    db.batch_insert(
        "memories",
        [
            Record::new("a").with_vector(vec![1.0, 0.0]),
            Record::new("b").with_vector(vec![0.0, 1.0]),
        ],
    )
    .unwrap();
    db.create_vector_index("memories", HnswIndexConfig::default())
        .unwrap();

    db.insert("memories", Record::new("b").with_vector(vec![0.99, 0.01]))
        .unwrap();
    let results = db
        .search_vector_ann("memories", &[1.0, 0.0], 2, 16)
        .unwrap();
    assert_eq!(results[0].record.id, "a");
    assert_eq!(results[1].record.id, "b");

    let report = db.rebuild_vector_index("memories").unwrap();
    assert!(report.valid);
    assert_eq!(report.live_vectors, 2);
}

#[test]
fn hnsw_dimension_and_metric_mismatches_return_clear_errors() {
    let (_temp, mut db) = open_temp();
    db.create_collection("memories").unwrap();
    db.insert("memories", Record::new("a").with_vector(vec![1.0, 0.0]))
        .unwrap();
    db.create_vector_index(
        "memories",
        HnswIndexConfig {
            distance: VectorMetric::L2,
            ..HnswIndexConfig::default()
        },
    )
    .unwrap();

    let dimension_error = db
        .search_vector_ann("memories", &[1.0, 0.0, 0.0], 1, 16)
        .unwrap_err();
    assert!(matches!(
        dimension_error,
        BicDbError::DimensionMismatch { .. }
    ));

    let metric_error = db
        .search_vector_ann_with_metric("memories", &[1.0, 0.0], 1, 16, VectorMetric::Cosine)
        .unwrap_err();
    assert!(matches!(metric_error, BicDbError::Index(_)));
}

#[test]
fn graph_projection_builds_nodes_edges_and_traverses() {
    let (_temp, mut db) = open_temp();
    seed_graph_records(&mut db);

    let projection = GraphProjection::new("entity_graph")
        .nodes_from("patients", "Patient")
        .nodes_from("doctors", "Doctor")
        .nodes_from("devices", "Device")
        .edge_from_field("appointments", "patient_id", "doctor_id", "VISITED")
        .edge_from_field("measurements", "patient_id", "device_id", "MEASURED_BY");

    let graph = db.build_graph_projection(projection.clone()).unwrap();

    assert!(graph.nodes.contains_key("Patient:p1"));
    assert!(graph.nodes.contains_key("Doctor:d7"));
    assert!(graph.nodes.contains_key("Device:band-1"));
    assert_eq!(graph.edges.len(), 2);
    assert_eq!(
        graph.neighbors("Patient:p1")[0].id,
        "Device:band-1".to_string()
    );
    assert_eq!(
        graph
            .path("Patient:p1", "Doctor:d7", 3)
            .unwrap()
            .nodes
            .as_slice(),
        ["Patient:p1", "Doctor:d7"]
    );
    assert_eq!(
        graph.traverse("Patient:p1", "VISITED", 1)[0].id,
        "Doctor:d7"
    );
    assert!(db.verify_graph_projection(&projection).unwrap().valid);
}

#[test]
fn graph_projection_persists_rebuilds_and_reflects_deleted_sources() {
    let temp = tempfile::tempdir().unwrap();
    let projection = GraphProjection::new("entity_graph")
        .nodes_from("patients", "Patient")
        .nodes_from("doctors", "Doctor")
        .edge_from_field("appointments", "patient_id", "doctor_id", "VISITED");

    {
        let mut db = BicDb::open(temp.path()).unwrap();
        seed_graph_records(&mut db);
        db.build_graph_projection(projection.clone()).unwrap();
        db.close().unwrap();
    }

    let mut db = BicDb::open(temp.path()).unwrap();
    assert_eq!(
        db.graph_projection("entity_graph")
            .unwrap()
            .unwrap()
            .neighbors("Patient:p1")
            .len(),
        1
    );

    assert!(db.delete("appointments", "a1").unwrap());
    let graph = db.graph_projection("entity_graph").unwrap().unwrap();
    assert!(graph.edges("Patient:p1").is_empty());
    assert!(db.verify_graph_projection(&projection).unwrap().valid);
}

#[test]
fn graph_projection_can_derive_nodes_from_event_streams() {
    let (_temp, mut db) = open_temp();
    db.events_mut()
        .append(Event::new(
            "application-events",
            "PatientCreated",
            json!({"patient_id": "p1", "name": "Asha"}),
        ))
        .unwrap();

    let graph = db
        .build_graph_projection(GraphProjection::new("event_graph").nodes_from_events(
            "application-events",
            "PatientCreated",
            "Patient",
            "patient_id",
        ))
        .unwrap();

    assert_eq!(graph.nodes["Patient:p1"].properties["name"], "Asha");
}

#[test]
fn sync_import_refreshes_existing_graph_projection() {
    let left_dir = tempfile::tempdir().unwrap();
    let right_dir = tempfile::tempdir().unwrap();
    let projection = GraphProjection::new("entity_graph")
        .nodes_from("patients", "Patient")
        .nodes_from("doctors", "Doctor")
        .edge_from_field("appointments", "patient_id", "doctor_id", "VISITED");

    let mut left =
        BicDb::open_with_config(left_dir.path(), DbConfig::default().with_audit_events(true))
            .unwrap();
    seed_graph_records(&mut left);
    // Mesh replication is opt-in per collection, and imports are refused from
    // unpinned origins. Authorize both the way a deployment would rather than
    // turning the checks off.
    for collection in ["patients", "doctors", "appointments"] {
        left.set_collection_mesh_sync_enabled(collection, true)
            .unwrap();
    }

    let mut right = BicDb::open_with_config(
        right_dir.path(),
        DbConfig::default().with_audit_events(true),
    )
    .unwrap();
    // The importing side must know and authorize the target collections too:
    // a bundle cannot conjure a collection into existence on a peer.
    for collection in ["patients", "doctors", "appointments"] {
        right.create_collection(collection).unwrap();
        right
            .set_collection_mesh_sync_enabled(collection, true)
            .unwrap();
    }
    right
        .build_graph_projection(projection.clone())
        .expect("empty projection can be built before sync");
    if let Some(key) = left.mesh_verifying_key() {
        right.pin_node_key(&left.node_id(), &key).unwrap();
    }

    let bundle = left
        .export_sync_bundle_since(SyncCheckpoint::new(0))
        .unwrap();
    right.import_sync_bundle(bundle).unwrap();

    let graph = right.graph_projection("entity_graph").unwrap().unwrap();
    assert!(graph.path("Patient:p1", "Doctor:d7", 2).is_some());
}

fn seed_graph_records(db: &mut BicDb) {
    db.create_collection("patients").unwrap();
    db.create_collection("doctors").unwrap();
    db.create_collection("devices").unwrap();
    db.create_collection("appointments").unwrap();
    db.create_collection("measurements").unwrap();
    db.insert(
        "patients",
        Record::new("p1").with_metadata(json!({"name": "Asha"})),
    )
    .unwrap();
    db.insert(
        "doctors",
        Record::new("d7").with_metadata(json!({"name": "Dr. Rao"})),
    )
    .unwrap();
    db.insert(
        "devices",
        Record::new("band-1").with_metadata(json!({"kind": "wearable"})),
    )
    .unwrap();
    db.insert(
        "appointments",
        Record::new("a1")
            .with_metadata(json!({"patient_id": "p1", "doctor_id": "d7"}))
            .with_timestamp(10),
    )
    .unwrap();
    db.insert(
        "measurements",
        Record::new("m1")
            .with_metadata(json!({"patient_id": "p1", "device_id": "band-1"}))
            .with_timestamp(11),
    )
    .unwrap();
}

#[test]
fn time_range_scan_and_summary_work() {
    let (_temp, mut db) = open_temp();
    db.create_timeseries_collection("wearable").unwrap();
    db.batch_insert(
        "wearable",
        vec![
            wearable_record("r1", "band-1", "hrv", 50.0, 100),
            wearable_record("r2", "band-1", "hrv", 55.0, 200),
            wearable_record("r3", "band-1", "steps", 20.0, 250),
            wearable_record("r4", "band-2", "hrv", 70.0, 300),
        ],
    )
    .unwrap();

    let records = db.scan_time_range("wearable", 150, 260).unwrap();
    assert_eq!(
        records
            .iter()
            .map(|record| record.id.as_str())
            .collect::<Vec<_>>(),
        vec!["r2", "r3"]
    );

    let latest = db
        .latest_value_per_device("wearable", "band-1", "hrv")
        .unwrap()
        .unwrap();
    assert_eq!(latest.id, "r2");

    let summary = db
        .time_series_summary(
            "wearable",
            &TimeSeriesFilter::new(0, 300)
                .device_id("band-1")
                .metric("hrv"),
        )
        .unwrap();
    assert_eq!(summary.count, 2);
    assert_eq!(summary.min, Some(50.0));
    assert_eq!(summary.max, Some(55.0));
    assert_eq!(summary.avg, Some(52.5));
}

fn recall_at(
    ann: &[bicdb_core::VectorSearchResult],
    exact: &[bicdb_core::VectorSearchResult],
) -> f64 {
    if exact.is_empty() {
        return 1.0;
    }
    let hits = ann
        .iter()
        .filter(|ann| exact.iter().any(|exact| exact.record.id == ann.record.id))
        .count();
    hits as f64 / exact.len() as f64
}

#[test]
fn sync_log_pending_and_mark_synced() {
    let (_temp, mut db) = open_temp();
    db.create_collection("docs").unwrap();
    db.insert("docs", Record::new("doc-1")).unwrap();
    db.insert("docs", Record::new("doc-2")).unwrap();

    let pending = db.pending_sync_ops();
    assert_eq!(pending.len(), 2);
    let first_id = pending[0].op_id;

    db.mark_synced(&[first_id]).unwrap();
    let pending = db.pending_sync_ops();
    assert_eq!(pending.len(), 1);
    assert_ne!(pending[0].op_id, first_id);
}

#[test]
fn batch_ingestion_persists_all_records() {
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db =
            BicDb::open_with_config(temp.path(), DbConfig::default().with_fsync(false)).unwrap();
        db.create_collection("batch").unwrap();
        let records = (0..1_000)
            .map(|idx| Record::new(format!("r-{idx}")).with_metadata(json!({"idx": idx})))
            .collect::<Vec<_>>();
        db.batch_insert("batch", records).unwrap();
        db.flush().unwrap();
    }

    let db = BicDb::open(temp.path()).unwrap();
    assert_eq!(db.stats().unwrap().record_count, 1_000);
    assert!(db.get("batch", "r-999").unwrap().is_some());
}

#[test]
fn vector_math_matches_scalar_expectations() {
    let left = (0..257)
        .map(|idx| (idx as f32 * 0.25) - 17.0)
        .collect::<Vec<_>>();
    let right = (0..257)
        .map(|idx| 4.0 - (idx as f32 * 0.125))
        .collect::<Vec<_>>();

    let expected_dot = left
        .iter()
        .zip(right.iter())
        .map(|(left, right)| left * right)
        .sum::<f32>();
    let expected_l2 = left
        .iter()
        .zip(right.iter())
        .map(|(left, right)| {
            let diff = left - right;
            diff * diff
        })
        .sum::<f32>()
        .sqrt();

    assert!((dot_product(&left, &right).unwrap() - expected_dot).abs() < 0.01);
    assert!((l2_distance(&left, &right).unwrap() - expected_l2).abs() < 0.01);
    assert!((cosine_similarity(&[1.0, 0.0], &[0.0, 1.0]).unwrap()).abs() < f32::EPSILON);
}

#[test]
fn vectorized_summary_over_contiguous_values() {
    let values = (0..10_001)
        .map(|idx| (idx % 97) as f64 + 0.5)
        .collect::<Vec<_>>();

    let summary = bicdb_core::query_exec::summarize_values(&values);

    assert_eq!(summary.count, values.len());
    assert_eq!(summary.min, Some(0.5));
    assert_eq!(summary.max, Some(96.5));
    let expected_avg = values.iter().sum::<f64>() / values.len() as f64;
    assert!((summary.avg.unwrap() - expected_avg).abs() < 0.000_001);
}

#[test]
fn compressed_records_reopen_with_mmap_reads() {
    let temp = tempfile::tempdir().unwrap();
    let large_metadata = "offline-first ".repeat(8_000);

    {
        let mut db = BicDb::open_with_config(
            temp.path(),
            DbConfig {
                fsync: true,
                read_mode: SegmentReadMode::Buffered,
                compression: CompressionConfig {
                    enabled: true,
                    level: 3,
                    min_bytes: 256,
                },
                audit_events: false,
                paged_value_compression: DbConfig::default().paged_value_compression,
                mesh_signing: DbConfig::default().mesh_signing,
                require_signed_imports: DbConfig::default().require_signed_imports,
                allow_unsafe_legacy_mesh_collections: DbConfig::default()
                    .allow_unsafe_legacy_mesh_collections,
                sync_outbox: true,
                snapshot: false,
                replication: Default::default(),
                consensus: Default::default(),
                require_commit_admission: false,
                load_secondary_indexes: DbConfig::default().load_secondary_indexes,
                storage_mode: Default::default(),
                paged_page_size: DbConfig::default().paged_page_size,
                paged_buffer_pool_bytes: DbConfig::default().paged_buffer_pool_bytes,
                paged_read_ahead_queue_pages: DbConfig::default().paged_read_ahead_queue_pages,
                paged_wal_max_bytes: DbConfig::default().paged_wal_max_bytes,
                paged_accept_legacy_meta: DbConfig::default().paged_accept_legacy_meta,
                paged_rowid_registry: DbConfig::default().paged_rowid_registry,
                paged_extent_bytes: 0,
                fts_build_memory_bytes: DbConfig::default().fts_build_memory_bytes,
                fts_build_workers: DbConfig::default().fts_build_workers,
                btree_build_batch_bytes: DbConfig::default().btree_build_batch_bytes,
                btree_build_batch_rows: DbConfig::default().btree_build_batch_rows,
                fts_block_cache_bytes: DbConfig::default().fts_block_cache_bytes,
                fts_prefetch_blocks: DbConfig::default().fts_prefetch_blocks,
                query_work_memory_bytes: DbConfig::default().query_work_memory_bytes,
                query_temp_space_bytes: DbConfig::default().query_temp_space_bytes,
                query_server_temp_space_bytes: DbConfig::default().query_server_temp_space_bytes,
                query_merge_fan_in: DbConfig::default().query_merge_fan_in,
                paged_wal_segment_bytes: DbConfig::default().paged_wal_segment_bytes,
                fts_query_partitions: DbConfig::default().fts_query_partitions,
                fts_packed_segments: DbConfig::default().fts_packed_segments,
                fts_progressive: DbConfig::default().fts_progressive,
                fts_progressive_interval_docs: DbConfig::default().fts_progressive_interval_docs,
            },
        )
        .unwrap();
        db.create_collection("compressed").unwrap();
        db.insert(
            "compressed",
            Record::new("doc-1").with_metadata(json!({
                "body": large_metadata,
                "kind": "note",
            })),
        )
        .unwrap();
        db.compact().unwrap();
        db.close().unwrap();
    }

    let compressed_segment_size =
        std::fs::metadata(temp.path().join("segments").join("compressed.seg"))
            .unwrap()
            .len();
    assert!(compressed_segment_size < 20_000);

    let db = BicDb::open_with_config(
        temp.path(),
        DbConfig {
            fsync: true,
            read_mode: SegmentReadMode::Mmap,
            compression: CompressionConfig::default(),
            audit_events: false,
            paged_value_compression: DbConfig::default().paged_value_compression,
            mesh_signing: DbConfig::default().mesh_signing,
            require_signed_imports: DbConfig::default().require_signed_imports,
            fts_progressive: DbConfig::default().fts_progressive,
            fts_progressive_interval_docs: DbConfig::default().fts_progressive_interval_docs,
            allow_unsafe_legacy_mesh_collections: DbConfig::default()
                .allow_unsafe_legacy_mesh_collections,
            sync_outbox: true,
            snapshot: false,
            replication: Default::default(),
            consensus: Default::default(),
            require_commit_admission: false,
            load_secondary_indexes: DbConfig::default().load_secondary_indexes,
            storage_mode: Default::default(),
            paged_page_size: DbConfig::default().paged_page_size,
            paged_buffer_pool_bytes: DbConfig::default().paged_buffer_pool_bytes,
            paged_read_ahead_queue_pages: DbConfig::default().paged_read_ahead_queue_pages,
            paged_wal_max_bytes: DbConfig::default().paged_wal_max_bytes,
            paged_accept_legacy_meta: DbConfig::default().paged_accept_legacy_meta,
            paged_rowid_registry: DbConfig::default().paged_rowid_registry,
            paged_extent_bytes: 0,
            fts_build_memory_bytes: DbConfig::default().fts_build_memory_bytes,
            fts_build_workers: DbConfig::default().fts_build_workers,
            btree_build_batch_bytes: DbConfig::default().btree_build_batch_bytes,
            btree_build_batch_rows: DbConfig::default().btree_build_batch_rows,
            fts_block_cache_bytes: DbConfig::default().fts_block_cache_bytes,
            fts_prefetch_blocks: DbConfig::default().fts_prefetch_blocks,
            query_work_memory_bytes: DbConfig::default().query_work_memory_bytes,
            query_temp_space_bytes: DbConfig::default().query_temp_space_bytes,
            query_server_temp_space_bytes: DbConfig::default().query_server_temp_space_bytes,
            query_merge_fan_in: DbConfig::default().query_merge_fan_in,
            paged_wal_segment_bytes: DbConfig::default().paged_wal_segment_bytes,
            fts_query_partitions: DbConfig::default().fts_query_partitions,
            fts_packed_segments: DbConfig::default().fts_packed_segments,
        },
    )
    .unwrap();
    let recovered = db.get("compressed", "doc-1").unwrap().unwrap();
    assert_eq!(recovered.metadata["kind"], "note");
    assert!(recovered.metadata["body"].as_str().unwrap().len() > 50_000);
}

#[test]
// Pinned to embedded_memory: asserts segment_bytes > 0, and in paged mode
// segments carry no rows, so zero is the correct answer there.
fn collection_stats_report_payload_and_overhead_sizes() {
    let _temp = tempfile::tempdir().expect("tempdir");
    let mut db = BicDb::open_with_config(
        _temp.path(),
        DbConfig::default().with_storage_mode(StorageMode::EmbeddedMemory),
    )
    .expect("open db");
    db.create_collection("stats").unwrap();
    db.insert(
        "stats",
        Record::new("record-1")
            .with_metadata(json!({"value": 1}))
            .with_payload(vec![1, 2, 3, 4]),
    )
    .unwrap();
    db.compact().unwrap();

    let stats = db.stats().unwrap();
    let collection = stats
        .collections
        .iter()
        .find(|collection| collection.name == "stats")
        .unwrap();

    assert!(collection.segment_bytes > 0);
    assert!(collection.logical_record_bytes > 0);
    assert!(collection.storage_overhead_bytes <= collection.segment_bytes);
}

#[test]
fn operational_metrics_cover_storage_backup_compaction_and_replication() {
    let (temp, mut db) = open_temp();
    db.create_collection("patients").unwrap();
    db.insert(
        "patients",
        Record::new("p-1").with_metadata(json!({"name": "Asha"})),
    )
    .unwrap();
    std::fs::write(temp.path().with_extension("bicbackup"), b"backup").unwrap();
    db.compact().unwrap();

    let metrics = OperationalMetrics::from_db(&db).unwrap();
    assert_eq!(metrics.server["up"], 1);
    assert_eq!(metrics.storage["collections"], 1);
    assert_eq!(metrics.storage["records"], 1);
    assert!(metrics.backup["artifacts_found"] >= 1);
    assert!(metrics.compaction.contains_key("checkpoint_present"));
    assert_eq!(metrics.replication["ready"], 1);
    assert!(metrics.to_prometheus().contains("bicdb_storage_records 1"));
}

#[test]
fn slow_query_redaction_masks_sensitive_fields_and_bind_parameters() {
    let config = RedactionConfig {
        sensitive_fields: vec!["ssn".to_string(), "password".to_string()],
        ..RedactionConfig::default()
    };
    let redacted = redact_query_text(
        "UPDATE patients SET ssn = '123-45-6789', note = 'ok' WHERE password = 'secret';",
        &config,
    );
    // Redaction is now by SYNTAX, not by configured field name: every
    // literal goes, so a value is protected whether or not someone
    // remembered to list its column. The statement shape survives.
    assert!(redacted.contains("ssn = "), "shape lost: {redacted}");
    assert!(redacted.contains("password = "), "shape lost: {redacted}");
    assert!(
        redacted.contains("UPDATE patients"),
        "shape lost: {redacted}"
    );
    assert!(!redacted.contains("123-45-6789"));
    assert!(!redacted.contains("secret"));
    // The unlisted column's value is redacted too — the old behaviour
    // logged it in full.
    assert!(
        !redacted.contains("'ok'"),
        "unlisted literal leaked: {redacted}"
    );

    let params = redact_bind_parameters(&["abc".to_string(), "def".to_string()], &config);
    assert_eq!(params, vec!["[REDACTED]", "[REDACTED]"]);

    let entry = SlowQueryLogEntry::new(
        25,
        1,
        "SELECT * FROM patients WHERE ssn = '123-45-6789'",
        &["123-45-6789".to_string()],
        &config,
    );
    assert!(!entry.query.contains("123-45-6789"));
    assert_eq!(entry.bind_parameters, vec!["[REDACTED]"]);
}

fn wearable_record(id: &str, device_id: &str, metric: &str, value: f64, timestamp: i64) -> Record {
    Record::new(id)
        .with_timestamp(timestamp)
        .with_metadata(json!({
            "device_id": device_id,
            "metric": metric,
            "value": value,
        }))
}

#[test]
fn commit_frames_carry_monotonic_sequence_and_survive_recovery() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(dir.path()).unwrap();
        db.create_collection("widgets").unwrap();
        for i in 0..3 {
            let mut tx = db.begin_transaction().unwrap();
            tx.insert("widgets", Record::new(&format!("w-{i}")))
                .unwrap();
            tx.commit().unwrap();
        }
        // Three transactional commits must have been assigned strictly
        // increasing, gap-free commit sequence numbers.
        assert_eq!(db.last_commit_seq(), 3);
    }
    // Reopen: recovery must restore the sequence high-water and the data.
    let db = BicDb::open(dir.path()).unwrap();
    assert_eq!(db.last_commit_seq(), 3);
    assert_eq!(db.scan_collection("widgets").unwrap().len(), 3);
}

#[test]
fn commit_critical_section_preserves_ordering_under_serial_commits() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    db.create_collection("ledger").unwrap();
    let mut seqs = Vec::new();
    for i in 0..5 {
        let mut tx = db.begin_transaction().unwrap();
        tx.insert("ledger", Record::new(&format!("e-{i}"))).unwrap();
        tx.commit().unwrap();
        seqs.push(db.last_commit_seq());
    }
    // The dedicated commit critical section must keep sequence assignment
    // gap-free and monotonic across serial commits.
    assert_eq!(seqs, vec![1, 2, 3, 4, 5]);
}

#[test]
fn concurrent_unique_inserts_cannot_half_apply_a_commit() {
    // Two committers carrying the SAME unique key race through the
    // validate->apply window (widened by BICDB_TEST_COMMIT_PAUSE_MS, a
    // debug-only hook). Exactly one must win; the loser must abort CLEANLY —
    // before the WAL/apply section — leaving the index with one entry and the
    // visibility watermark advancing (a frozen watermark makes every later
    // commit invisible, which is how the unfixed race manifests).
    use bicdb_core::IndexDefinition;
    use bicdb_core::IndexField;
    use bicdb_core::IndexKind;
    std::env::set_var("BICDB_TEST_COMMIT_PAUSE_MS", "120");
    let (_temp, mut db) = open_temp();
    db.create_collection("accounts").unwrap();
    db.create_index(IndexDefinition {
        name: "accounts_email_unique".to_string(),
        collection: "accounts".to_string(),
        fields: vec![IndexField::MetadataPath(vec!["email".to_string()])],
        unique: true,
        kind: IndexKind::BTree,
        predicate: None,
        exclusion: None,
    })
    .unwrap();

    let mut tx1 = db.begin_transaction().unwrap();
    tx1.insert(
        "accounts",
        Record::new("a1").with_metadata(json!({"email": "dup@x"})),
    )
    .unwrap();
    let mut tx2 = db.begin_transaction().unwrap();
    tx2.insert(
        "accounts",
        Record::new("a2").with_metadata(json!({"email": "dup@x"})),
    )
    .unwrap();

    let db_ref: &BicDb = &db;
    let (r1, r2) = std::thread::scope(|scope| {
        let h1 = scope.spawn(|| db_ref.commit_buffered_transaction(&mut tx1));
        let h2 = scope.spawn(|| db_ref.commit_buffered_transaction(&mut tx2));
        (h1.join().unwrap(), h2.join().unwrap())
    });
    std::env::remove_var("BICDB_TEST_COMMIT_PAUSE_MS");

    eprintln!("race outcome: r1={r1:?} r2={r2:?}");
    let ok_count = [r1.is_ok(), r2.is_ok()].iter().filter(|ok| **ok).count();
    assert_eq!(
        ok_count, 1,
        "exactly one committer must win: {r1:?} / {r2:?}"
    );

    // Index/heap agreement: one record under the key.
    let ids = db
        .lookup_index(
            "accounts_email_unique",
            &[bicdb_core::IndexValue::String("dup@x".to_string())],
        )
        .unwrap();
    assert_eq!(ids.len(), 1, "unique key must hold exactly one record");

    // Watermark must not be frozen: a later commit must become visible.
    let mut tx3 = db.begin_transaction().unwrap();
    tx3.insert(
        "accounts",
        Record::new("a3").with_metadata(json!({"email": "other@x"})),
    )
    .unwrap();
    tx3.commit().unwrap();
    // Snapshot-gated read: BicDb::get reads the latest resident record and
    // bypasses the watermark, so probe through a fresh transaction snapshot.
    let probe = db.begin_transaction().unwrap();
    assert!(
        probe.get("accounts", "a3").unwrap().is_some(),
        "watermark frozen: later commits never became visible to new snapshots"
    );
    drop(probe);
}
