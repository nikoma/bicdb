use bicdb_core::{
    create_backup, restore_backup, BackupCreateOptions, BackupRestoreOptions, BicDb, DbConfig,
    Record, ReplicationConfig, ReplicationMode, ReplicationTlsConfig,
};
use bicdb_sql::{SqlSession, SqlValue};
use serde_json::json;

const OID_COLLECTION: &str = "__bicdb_pg_user_type_oids";
const BACKUP_KEY: &str = "user-type-oid-durability-test";

fn catalog_oids(session: &mut SqlSession<'_>, names: &[&str]) -> Vec<i64> {
    let mut oids = Vec::new();
    for name in names {
        let type_rows = session
            .execute(&format!(
                "SELECT oid, typarray FROM pg_type WHERE typname = '{name}'"
            ))
            .unwrap()
            .rows;
        assert_eq!(type_rows.len(), 1, "missing pg_type row for {name}");
        oids.extend(type_rows[0].iter().map(integer_value));
        oids.extend(
            session
                .execute(&format!(
                    "SELECT e.oid FROM pg_enum e JOIN pg_type t ON t.oid = e.enumtypid \
                     WHERE t.typname = '{name}' ORDER BY e.oid"
                ))
                .unwrap()
                .rows
                .iter()
                .map(|row| integer_value(&row[0])),
        );
    }
    oids
}

fn integer_value(value: &SqlValue) -> i64 {
    match value {
        SqlValue::Int(value) => *value,
        other => panic!("expected integer OID, got {other:?}"),
    }
}

fn standby_config() -> DbConfig {
    replication_config(ReplicationMode::Standby, "standby")
}

fn primary_config() -> DbConfig {
    replication_config(ReplicationMode::Primary, "primary")
}

fn replication_config(mode: ReplicationMode, node_id: &str) -> DbConfig {
    DbConfig::default().with_replication(ReplicationConfig {
        enabled: true,
        mode,
        listen_addr: Some("127.0.0.1:0".to_string()),
        tls: Some(ReplicationTlsConfig {
            cert_path: Default::default(),
            key_path: Default::default(),
            ca_path: Default::default(),
            require_client_cert: true,
            dev_localhost_plaintext: true,
        }),
        cluster_id: "user-type-oids".to_string(),
        node_id: node_id.to_string(),
        ..ReplicationConfig::default()
    })
}

#[test]
fn user_type_oids_are_not_reused_after_drop_stale_metadata_restart_or_restore() {
    let source = tempfile::tempdir().unwrap();
    let backup_dir = tempfile::tempdir().unwrap();
    let restored = tempfile::tempdir().unwrap();
    let independent = tempfile::tempdir().unwrap();
    let backup_path = backup_dir.path().join("user-type-oids.bicbackup");

    let retired_max = {
        let mut db = BicDb::open(source.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TYPE retired_state AS ENUM ('queued', 'done')")
            .unwrap();
        let max = *catalog_oids(&mut session, &["retired_state"])
            .iter()
            .max()
            .unwrap();
        session.execute("DROP TYPE retired_state").unwrap();
        max
    };

    let recovered_max = {
        let mut db = BicDb::open(source.path()).unwrap();
        db.delete(OID_COLLECTION, "allocator").unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TYPE recovered_state AS ENUM ('active')")
            .unwrap();
        let oids = catalog_oids(&mut session, &["recovered_state"]);
        assert!(oids.iter().all(|oid| *oid > retired_max));
        *oids.iter().max().unwrap()
    };

    let durable_max = {
        let mut db = BicDb::open(source.path()).unwrap();
        db.insert(
            OID_COLLECTION,
            Record::new("allocator").with_metadata(json!({ "next_oid": 800_000 })),
        )
        .unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE DOMAIN durable_code AS TEXT")
            .unwrap();
        let oids = catalog_oids(&mut session, &["durable_code"]);
        assert!(oids.iter().all(|oid| *oid > recovered_max));
        *oids.iter().max().unwrap()
    };

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
    let mut restored_session = SqlSession::new(&mut restored_db);
    restored_session
        .execute("CREATE TYPE restored_span AS RANGE (SUBTYPE = integer)")
        .unwrap();
    let restored_oids = catalog_oids(
        &mut restored_session,
        &["restored_span", "restored_span_multirange"],
    );
    assert!(restored_oids.iter().all(|oid| *oid > durable_max));

    let mut independent_db = BicDb::open(independent.path()).unwrap();
    let mut independent_session = SqlSession::new(&mut independent_db);
    independent_session
        .execute("CREATE DOMAIN independent_code AS TEXT")
        .unwrap();
    assert_eq!(
        *catalog_oids(&mut independent_session, &["independent_code"])
            .iter()
            .min()
            .unwrap(),
        800_000,
        "PostgreSQL type OID namespaces are isolated per database",
    );
}

#[test]
fn replicated_oid_reservations_prevent_standby_collisions_after_promotion() {
    let primary_root = tempfile::tempdir().unwrap();
    let standby_root = tempfile::tempdir().unwrap();
    let mut primary = BicDb::open_with_config(primary_root.path(), primary_config()).unwrap();
    let primary_max = {
        let mut session = SqlSession::new(&mut primary);
        session
            .execute("CREATE TYPE replicated_state AS ENUM ('queued', 'running', 'done')")
            .unwrap();
        *catalog_oids(&mut session, &["replicated_state"])
            .iter()
            .max()
            .unwrap()
    };
    let frames = primary.export_replication_frames_since(0, 100).unwrap();

    {
        let mut standby = BicDb::open_with_config(standby_root.path(), standby_config()).unwrap();
        standby.apply_replication_batch(&frames).unwrap();
    }

    let mut promoted = BicDb::open(standby_root.path()).unwrap();
    let mut session = SqlSession::new(&mut promoted);
    assert_eq!(
        catalog_oids(&mut session, &["replicated_state"])
            .into_iter()
            .max(),
        Some(primary_max),
    );
    session
        .execute("CREATE DOMAIN promoted_code AS TEXT")
        .unwrap();
    assert!(catalog_oids(&mut session, &["promoted_code"])
        .iter()
        .all(|oid| *oid > primary_max));
}
