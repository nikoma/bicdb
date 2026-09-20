use std::fs::OpenOptions;
use std::io::Write;

use bicdb_core::{
    create_backup, restore_backup, BackupCreateOptions, BackupRestoreOptions, BicDb, DbConfig,
    NodeId, ReplicationConfig, ReplicationMode, ReplicationTlsConfig, SyncCheckpoint,
    DEFAULT_TRANSACTION_LOG, RECORD_AUDIT_STREAM,
};
use bicdb_sql::{SqlResult, SqlSession, SqlValue};
use serde_json::json;
use uuid::Uuid;

const BACKUP_KEY: &str = "typed-value-durability-test";
const CREATE_TYPED_TABLE: &str = r#"
    CREATE TABLE durable_types (
        id UUID PRIMARY KEY,
        i2 SMALLINT NOT NULL,
        i4 INTEGER NOT NULL,
        i8 BIGINT NOT NULL,
        f4 REAL NOT NULL,
        f8 DOUBLE PRECISION NOT NULL,
        exact NUMERIC(30,6) NOT NULL,
        label VARCHAR(24) NOT NULL,
        active BOOLEAN NOT NULL,
        bytes BYTEA NOT NULL,
        fixed_bits BIT(5) NOT NULL,
        flexible_bits VARBIT NOT NULL,
        day DATE NOT NULL,
        clock TIME(3) NOT NULL,
        zoned_clock TIME(3) WITH TIME ZONE NOT NULL,
        local_ts TIMESTAMP(3) NOT NULL,
        instant TIMESTAMPTZ(3) NOT NULL,
        span INTERVAL NOT NULL,
        doc JSONB NOT NULL,
        tags TEXT[] NOT NULL,
        ids UUID[] NOT NULL,
        amounts NUMERIC[] NOT NULL
    )
"#;
const INSERT_TYPED_ROW: &str = r#"
    INSERT INTO durable_types VALUES (
        '018f22d0-4510-7cc8-9a21-3e78ea2b9c44'::uuid,
        (-32768)::smallint,
        2147483647::integer,
        9007199254740993::bigint,
        1.5::real,
        2.25::double precision,
        '9007199254740993.010000'::numeric(30,6),
        'durable',
        TRUE,
        '\x00ff10'::bytea,
        '10101'::bit(5),
        '001001'::varbit,
        '2024-02-29'::date,
        '23:59:58.123'::time(3),
        '23:59:58.123+02'::timetz(3),
        '2024-02-29 23:59:58.123'::timestamp(3),
        '2024-02-29 23:59:58.123+00'::timestamptz(3),
        '2 days'::interval,
        '{"nested":{"amount":"12.30"},"ok":true}'::jsonb,
        ARRAY['alpha', 'beta']::text[],
        ARRAY['018f22d0-4510-7cc8-9a21-3e78ea2b9c44'::uuid]::uuid[],
        ARRAY['0.10'::numeric, '9007199254740993.01'::numeric]::numeric[]
    )
"#;
const SELECT_TYPED_ROW: &str = r#"
    SELECT id, i2, i4, i8, f4, f8, exact, label, active, bytes, fixed_bits,
           flexible_bits, day, clock, zoned_clock, local_ts, instant, span, doc,
           tags, ids, amounts
    FROM durable_types
"#;

fn node(value: u128) -> NodeId {
    NodeId(Uuid::from_u128(value))
}

fn sync_config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_audit_events(true)
}

fn standby_config() -> DbConfig {
    DbConfig::default().with_replication(ReplicationConfig {
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
        cluster_id: "typed-values".to_string(),
        node_id: "standby".to_string(),
        ..ReplicationConfig::default()
    })
}

fn create_schema(db: &mut BicDb) {
    {
        let mut session = SqlSession::new(db);
        session.execute(CREATE_TYPED_TABLE).unwrap();
    }
    // Record events only leave a node for collections explicitly opted into
    // mesh sync; without this the exported bundle carries no events at all.
    db.set_collection_mesh_sync_enabled("durable_types", true)
        .unwrap();
    let mut session = SqlSession::new(db);
    assert_eq!(
        session
            .execute(
                "SELECT a.atttypid FROM pg_catalog.pg_attribute a
                 JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
                 WHERE c.relname = 'durable_types' AND a.attname = 'instant'",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1184)]]
    );
}

fn insert_row(db: &mut BicDb) {
    SqlSession::new(db).execute(INSERT_TYPED_ROW).unwrap();
}

fn read_row(db: &mut BicDb) -> SqlResult {
    SqlSession::new(db).execute(SELECT_TYPED_ROW).unwrap()
}

fn assert_canonical_row(result: &SqlResult) {
    assert_eq!(
        result.column_types,
        vec![
            Some("uuid".to_string()),
            Some("int2".to_string()),
            Some("int4".to_string()),
            Some("int8".to_string()),
            Some("float4".to_string()),
            Some("float8".to_string()),
            Some("numeric".to_string()),
            Some("varchar".to_string()),
            Some("bool".to_string()),
            Some("bytea".to_string()),
            Some("bit".to_string()),
            Some("varbit".to_string()),
            Some("date".to_string()),
            Some("time".to_string()),
            Some("timetz".to_string()),
            Some("timestamp".to_string()),
            Some("timestamptz".to_string()),
            Some("interval".to_string()),
            Some("jsonb".to_string()),
            Some("text[]".to_string()),
            Some("uuid[]".to_string()),
            Some("numeric[]".to_string()),
        ]
    );
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0],
        vec![
            SqlValue::String("018f22d0-4510-7cc8-9a21-3e78ea2b9c44".to_string()),
            SqlValue::Int(-32768),
            SqlValue::Int(2147483647),
            SqlValue::Int(9007199254740993),
            SqlValue::Float(1.5),
            SqlValue::Float(2.25),
            SqlValue::String("9007199254740993.010000".to_string()),
            SqlValue::String("durable".to_string()),
            SqlValue::Bool(true),
            SqlValue::String("\\x00ff10".to_string()),
            SqlValue::String("10101".to_string()),
            SqlValue::String("001001".to_string()),
            SqlValue::String("2024-02-29".to_string()),
            SqlValue::String("23:59:58.123".to_string()),
            SqlValue::String("23:59:58.123+02".to_string()),
            SqlValue::String("2024-02-29 23:59:58.123".to_string()),
            SqlValue::String("2024-02-29 23:59:58.123+00".to_string()),
            SqlValue::String("2 days".to_string()),
            SqlValue::Json(json!({"nested": {"amount": "12.30"}, "ok": true})),
            SqlValue::Json(json!(["alpha", "beta"])),
            SqlValue::Json(json!(["018f22d0-4510-7cc8-9a21-3e78ea2b9c44"])),
            SqlValue::Json(json!(["0.10", "9007199254740993.01"])),
        ]
    );
}

fn assert_durable_enum(db: &mut BicDb, type_oid: i64, array_oid: i64) {
    let mut session = SqlSession::new(db);
    assert_eq!(
        session
            .execute("SELECT oid, typarray FROM pg_type WHERE typname = 'durable_priority'",)
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(type_oid), SqlValue::Int(array_oid)]]
    );
    let result = session
        .execute("SELECT status, history FROM durable_enums ORDER BY status")
        .unwrap();
    assert_eq!(
        result.column_types,
        vec![
            Some("durable_priority".to_string()),
            Some("durable_priority[]".to_string()),
        ]
    );
    assert_eq!(
        result.rows,
        vec![
            vec![
                SqlValue::String("medium".to_string()),
                SqlValue::Json(json!(["medium"])),
            ],
            vec![
                SqlValue::String("low".to_string()),
                SqlValue::Json(json!(["low", null])),
            ],
            vec![
                SqlValue::String("high".to_string()),
                SqlValue::Json(json!(["medium", "high"])),
            ],
        ]
    );
    assert_eq!(
        session
            .execute("SELECT id FROM durable_enums WHERE status > 'medium' ORDER BY status",)
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("low".to_string())],
            vec![SqlValue::String("high".to_string())],
        ]
    );
}

fn assert_durable_domain(db: &mut BicDb, type_oid: i64, array_oid: i64) {
    let mut session = SqlSession::new(db);
    assert_eq!(
        session
            .execute(
                "SELECT oid, typarray, typbasetype, typtypmod, typnotnull, typdefault
                 FROM pg_type WHERE typname = 'durable_amount'",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(type_oid),
            SqlValue::Int(array_oid),
            SqlValue::Int(1700),
            SqlValue::Int(786_439),
            SqlValue::Bool(true),
            SqlValue::String("1.200".to_string()),
        ]],
    );
    let result = session
        .execute(
            "SELECT amount, array_dims(history), history
             FROM durable_domains WHERE id = 'explicit'",
        )
        .unwrap();
    assert_eq!(
        result.column_types,
        vec![
            Some("durable_amount".to_string()),
            Some("text".to_string()),
            Some("durable_amount[]".to_string()),
        ],
    );
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::String("12.346".to_string()),
            SqlValue::String("[0:1]".to_string()),
            SqlValue::Json(json!({
                "$bicdb_array_input": {
                    "lower_bounds": [0],
                    "value": ["1.235", "2.000"]
                }
            })),
        ]],
    );
    assert_eq!(
        session
            .execute("INSERT INTO durable_domains VALUES ('invalid', -1, NULL)")
            .unwrap_err()
            .sqlstate(),
        "23514",
    );
    assert_eq!(
        session
            .execute("SELECT id FROM durable_domains WHERE amount > 10 ORDER BY amount")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("explicit".to_string())]],
    );
}

#[test]
fn enum_catalog_values_and_indexes_survive_reopen_and_backup_restore() {
    let source = tempfile::tempdir().unwrap();
    let backup_dir = tempfile::tempdir().unwrap();
    let restored = tempfile::tempdir().unwrap();
    let backup_path = backup_dir.path().join("enum-types.bicbackup");
    let (type_oid, array_oid) = {
        let mut db = BicDb::open(source.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TYPE durable_priority AS ENUM ('medium', 'high')")
            .unwrap();
        let type_row = session
            .execute("SELECT oid, typarray FROM pg_type WHERE typname = 'durable_priority'")
            .unwrap()
            .rows
            .remove(0);
        session
            .execute(
                "CREATE TABLE durable_enums (id TEXT PRIMARY KEY, status durable_priority, \
                 history durable_priority[])",
            )
            .unwrap();
        session
            .execute("CREATE INDEX durable_enums_status_idx ON durable_enums (status)")
            .unwrap();
        session
            .execute(
                "INSERT INTO durable_enums VALUES \
                 ('high', 'high', '{medium,high}'), \
                 ('medium', 'medium', '{medium}'), \
                 ('low', 'medium', '{medium,NULL}')",
            )
            .unwrap();
        session
            .execute("ALTER TYPE durable_priority ADD VALUE 'low' BEFORE 'high'")
            .unwrap();
        session
            .execute(
                "UPDATE durable_enums SET status = 'low', history = '{low,NULL}' WHERE id = 'low'",
            )
            .unwrap();
        let [SqlValue::Int(type_oid), SqlValue::Int(array_oid)] = type_row.as_slice() else {
            panic!("enum catalog returned non-integer OIDs: {type_row:?}");
        };
        (*type_oid, *array_oid)
    };

    {
        let mut reopened = BicDb::open(source.path()).unwrap();
        assert_durable_enum(&mut reopened, type_oid, array_oid);
        reopened.close().unwrap();
    }

    create_backup(
        source.path(),
        &backup_path,
        BackupCreateOptions {
            passphrase: BACKUP_KEY.to_string(),
            base_backup: None,
        },
    )
    .unwrap();
    restore_backup(
        &backup_path,
        restored.path(),
        BackupRestoreOptions {
            passphrase: BACKUP_KEY.to_string(),
            force: true,
        },
    )
    .unwrap();

    let mut restored_db = BicDb::open(restored.path()).unwrap();
    assert_durable_enum(&mut restored_db, type_oid, array_oid);
}

#[test]
fn domain_catalog_constraints_arrays_and_indexes_survive_reopen_and_backup_restore() {
    let source = tempfile::tempdir().unwrap();
    let backup_dir = tempfile::tempdir().unwrap();
    let restored = tempfile::tempdir().unwrap();
    let backup_path = backup_dir.path().join("domain-types.bicbackup");
    let (type_oid, array_oid) = {
        let mut db = BicDb::open(source.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE DOMAIN durable_amount AS NUMERIC(12,3)
                 DEFAULT 1.200 NOT NULL CHECK (VALUE > 0)",
            )
            .unwrap();
        let type_row = session
            .execute("SELECT oid, typarray FROM pg_type WHERE typname = 'durable_amount'")
            .unwrap()
            .rows
            .remove(0);
        session
            .execute(
                "CREATE TABLE durable_domains (
                    id TEXT PRIMARY KEY,
                    amount durable_amount,
                    history durable_amount[]
                )",
            )
            .unwrap();
        session
            .execute("CREATE INDEX durable_domains_amount_idx ON durable_domains (amount)")
            .unwrap();
        session
            .execute(
                "INSERT INTO durable_domains
                 VALUES ('explicit', 12.3456, '[0:1]={1.2345,2}')",
            )
            .unwrap();
        let [SqlValue::Int(type_oid), SqlValue::Int(array_oid)] = type_row.as_slice() else {
            panic!("domain catalog returned non-integer OIDs: {type_row:?}");
        };
        (*type_oid, *array_oid)
    };

    {
        let mut reopened = BicDb::open(source.path()).unwrap();
        assert_durable_domain(&mut reopened, type_oid, array_oid);
        reopened.close().unwrap();
    }

    create_backup(
        source.path(),
        &backup_path,
        BackupCreateOptions {
            passphrase: BACKUP_KEY.to_string(),
            base_backup: None,
        },
    )
    .unwrap();
    restore_backup(
        &backup_path,
        restored.path(),
        BackupRestoreOptions {
            passphrase: BACKUP_KEY.to_string(),
            force: true,
        },
    )
    .unwrap();

    let mut restored_db = BicDb::open(restored.path()).unwrap();
    assert_durable_domain(&mut restored_db, type_oid, array_oid);
}

#[test]
fn typed_values_survive_restart_and_torn_wal_recovery() {
    let root = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(root.path()).unwrap();
        create_schema(&mut db);
        insert_row(&mut db);
        assert_canonical_row(&read_row(&mut db));
    }

    let wal_path = root.path().join(DEFAULT_TRANSACTION_LOG);
    let valid_len = std::fs::metadata(&wal_path).unwrap().len();
    OpenOptions::new()
        .append(true)
        .open(&wal_path)
        .unwrap()
        .write_all(b"partial-typed-value-frame")
        .unwrap();

    let mut reopened = BicDb::open(root.path()).unwrap();
    // What recovery leaves in the log is mode-specific; what matters in both
    // modes is that the torn suffix is gone and the data survived. Embedded
    // truncates back to the last valid frame; paged truncates the whole log,
    // because its contents are already durable in the page store.
    let recovered_len = std::fs::metadata(wal_path).unwrap().len();
    match bicdb_core::storage_mode(root.path()).unwrap() {
        bicdb_core::StorageMode::ServerPaged => assert_eq!(recovered_len, 0),
        _ => assert_eq!(recovered_len, valid_len),
    }
    assert_canonical_row(&read_row(&mut reopened));
}

#[test]
fn typed_values_survive_encrypted_backup_and_restore() {
    let source = tempfile::tempdir().unwrap();
    let backup_dir = tempfile::tempdir().unwrap();
    let restored = tempfile::tempdir().unwrap();
    let backup_path = backup_dir.path().join("typed-values.bicbackup");
    {
        let mut db = BicDb::open(source.path()).unwrap();
        create_schema(&mut db);
        insert_row(&mut db);
        db.close().unwrap();
    }

    create_backup(
        source.path(),
        &backup_path,
        BackupCreateOptions {
            passphrase: BACKUP_KEY.to_string(),
            base_backup: None,
        },
    )
    .unwrap();
    restore_backup(
        &backup_path,
        restored.path(),
        BackupRestoreOptions {
            passphrase: BACKUP_KEY.to_string(),
            force: true,
        },
    )
    .unwrap();

    let mut db = BicDb::open(restored.path()).unwrap();
    assert_canonical_row(&read_row(&mut db));
}

#[test]
fn typed_values_survive_native_replication() {
    let primary_root = tempfile::tempdir().unwrap();
    let standby_root = tempfile::tempdir().unwrap();
    let primary_config = DbConfig::default().with_replication(ReplicationConfig {
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
        cluster_id: "typed-values".to_string(),
        node_id: "primary".to_string(),
        ..ReplicationConfig::default()
    });
    let mut primary = BicDb::open_with_config(primary_root.path(), primary_config).unwrap();
    create_schema(&mut primary);
    {
        let mut standby = BicDb::open(standby_root.path()).unwrap();
        create_schema(&mut standby);
    }
    insert_row(&mut primary);
    let frames = primary.export_replication_frames_since(0, 100).unwrap();

    let mut standby = BicDb::open_with_config(standby_root.path(), standby_config()).unwrap();
    standby.apply_replication_batch(&frames).unwrap();
    assert_canonical_row(&read_row(&mut standby));
}

#[test]
fn typed_values_survive_offline_sync() {
    let source_root = tempfile::tempdir().unwrap();
    let target_root = tempfile::tempdir().unwrap();
    let mut source = BicDb::open_with_node_id(source_root.path(), sync_config(), node(1)).unwrap();
    let mut target = BicDb::open_with_node_id(target_root.path(), sync_config(), node(2)).unwrap();
    create_schema(&mut source);
    create_schema(&mut target);
    let checkpoint = source
        .export_sync_bundle_since(SyncCheckpoint::default())
        .unwrap()
        .next_checkpoint;

    insert_row(&mut source);
    let bundle = source.export_sync_bundle_since(checkpoint).unwrap();
    assert!(bundle.event_count > 0);
    // Signed imports come only from a pinned origin; pin it the way a real
    // first contact would rather than turning the check off.
    if let Some(key) = source.mesh_verifying_key() {
        target.pin_node_key(&source.node_id(), &key).unwrap();
    }
    target.import_sync_bundle(bundle).unwrap();
    assert_canonical_row(&read_row(&mut target));
}

#[test]
fn typed_values_survive_record_audit_events_and_reopen() {
    let root = tempfile::tempdir().unwrap();
    let expected_metadata = {
        let mut db = BicDb::open_with_config(root.path(), sync_config()).unwrap();
        create_schema(&mut db);
        insert_row(&mut db);
        let records = db.scan_collection("durable_types").unwrap();
        assert_eq!(records.len(), 1);
        let metadata = records[0].metadata.clone();
        let events = db.events().read(RECORD_AUDIT_STREAM);
        let created = events
            .iter()
            .find(|event| {
                event.event.event_type == "RecordCreated"
                    && event.event.payload["collection"] == "durable_types"
            })
            .unwrap();
        assert_eq!(created.event.payload["record"]["metadata"], metadata);
        metadata
    };

    let reopened = BicDb::open_with_config(root.path(), sync_config()).unwrap();
    let events = reopened.events().read(RECORD_AUDIT_STREAM);
    let created = events
        .iter()
        .find(|event| {
            event.event.event_type == "RecordCreated"
                && event.event.payload["collection"] == "durable_types"
        })
        .unwrap();
    assert_eq!(
        created.event.payload["record"]["metadata"],
        expected_metadata
    );
}
