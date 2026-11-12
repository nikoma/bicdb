//! Catalog reservations must fence affected wire metadata and preserve unrelated SQL.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_pgwire::{PgWireConfig, PgWireServer};
use bicdb_sql::SqlSession;
use parking_lot::RwLock;
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::Arc,
    thread,
    time::Duration,
};

fn send(stream: &mut TcpStream, tag: u8, payload: &[u8]) {
    stream.write_all(&[tag]).unwrap();
    stream
        .write_all(&((payload.len() + 4) as i32).to_be_bytes())
        .unwrap();
    stream.write_all(payload).unwrap();
}

fn ready(stream: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
    let mut messages = Vec::new();
    loop {
        let mut tag = [0];
        stream.read_exact(&mut tag).unwrap();
        let mut length = [0; 4];
        stream.read_exact(&mut length).unwrap();
        let length = i32::from_be_bytes(length);
        assert!((4..1_048_576).contains(&length));
        let mut payload = vec![0; length as usize - 4];
        stream.read_exact(&mut payload).unwrap();
        messages.push((tag[0], payload));
        if tag[0] == b'Z' {
            return messages;
        }
    }
}

fn query(stream: &mut TcpStream, sql: &str) -> Vec<(u8, Vec<u8>)> {
    let mut payload = sql.as_bytes().to_vec();
    payload.push(0);
    send(stream, b'Q', &payload);
    ready(stream)
}

fn describe(stream: &mut TcpStream, statement: &str) -> Vec<(u8, Vec<u8>)> {
    let mut payload = vec![b'S'];
    payload.extend_from_slice(statement.as_bytes());
    payload.push(0);
    send(stream, b'D', &payload);
    send(stream, b'S', &[]);
    ready(stream)
}

fn prepare(stream: &mut TcpStream, statement: &str, sql: &str) {
    let mut payload = statement.as_bytes().to_vec();
    payload.push(0);
    payload.extend_from_slice(sql.as_bytes());
    payload.push(0);
    payload.extend_from_slice(&0i16.to_be_bytes());
    send(stream, b'P', &payload);
    send(stream, b'S', &[]);
    assert!(ready(stream).iter().all(|(tag, _)| *tag != b'E'));
    assert!(describe(stream, statement)
        .iter()
        .any(|(tag, _)| *tag == b'T'));
}

fn codes(messages: &[(u8, Vec<u8>)]) -> Vec<String> {
    messages
        .iter()
        .filter(|(tag, _)| *tag == b'E')
        .flat_map(|(_, payload)| payload.split(|byte| *byte == 0))
        .filter(|field| field.first() == Some(&b'C'))
        .map(|field| String::from_utf8_lossy(&field[1..]).into_owned())
        .collect()
}

fn value(messages: &[(u8, Vec<u8>)]) -> Option<String> {
    let payload = &messages.iter().find(|(tag, _)| *tag == b'D')?.1;
    assert_eq!(i16::from_be_bytes(payload[..2].try_into().unwrap()), 1);
    let size = i32::from_be_bytes(payload[2..6].try_into().unwrap());
    assert!(size >= 0);
    Some(String::from_utf8(payload[6..6 + size as usize].to_vec()).unwrap())
}

fn connect(address: std::net::SocketAddr) -> TcpStream {
    let mut client = TcpStream::connect(address).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut startup = 196608i32.to_be_bytes().to_vec();
    startup.extend_from_slice(b"user\0bicdb\0database\0bicdb\0\0");
    client
        .write_all(&((startup.len() + 4) as i32).to_be_bytes())
        .unwrap();
    client.write_all(&startup).unwrap();
    assert!(ready(&mut client).iter().all(|(tag, _)| *tag != b'E'));
    client
}

fn concurrent_sql_rename(ending: &str) {
    concurrent_sql_rename_with_isolation(ending, "READ COMMITTED");
}

fn concurrent_sql_rename_with_isolation(ending: &str, isolation: &str) {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            DbConfig::default().with_fsync(true).with_storage_mode(mode),
        )
        .unwrap();
        SqlSession::new(&mut db).execute("CREATE TABLE source(id INT PRIMARY KEY,payload INT); INSERT INTO source VALUES(1,10); CREATE TABLE unrelated(id INT PRIMARY KEY,payload INT); INSERT INTO unrelated VALUES(2,2); CREATE VIEW dependent AS SELECT source AS item FROM source").unwrap();
        bicdb_sql::initialize_rename_journal_identities(&mut db).unwrap();
        let shared = Arc::new(RwLock::new(db));
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = PgWireServer::open_with_shared(
            root.path(),
            PgWireConfig {
                fsync: true,
                automatic_paged_checkpoint: false,
                ..Default::default()
            },
            Arc::clone(&shared),
        )
        .unwrap();
        let serving = Arc::clone(&server);
        let task = thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
        let mut owner = connect(address);
        let mut writer = connect(address);
        let mut observer = connect(address);
        // Always stop our serving thread, including a failed semantic assertion.
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let response = query(
                &mut owner,
                &format!("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {isolation}"),
            );
            assert!(codes(&response).is_empty(), "{response:?}");
            let response = query(
                &mut writer,
                "BEGIN; UPDATE unrelated SET payload=99 WHERE id=2",
            );
            assert!(codes(&response).is_empty(), "{response:?}");
            if ending != "autocommit" {
                let response = query(
                    &mut owner,
                    "BEGIN; UPDATE source SET payload=20 WHERE id=1; SAVEPOINT before_rename",
                );
                assert!(codes(&response).is_empty(), "{response:?}");
            }
            let response = query(&mut owner, "ALTER TABLE source RENAME TO renamed");
            assert!(codes(&response).is_empty(), "ending={ending}: {response:?}");
            assert_eq!(
                value(&query(&mut owner, "SELECT (item).payload FROM dependent")),
                Some(if ending == "autocommit" { "10" } else { "20" }.into())
            );
            assert_eq!(
                value(&query(
                    &mut writer,
                    "SELECT payload FROM unrelated WHERE id=2"
                )),
                Some("99".into())
            );
            assert_eq!(
                value(&query(
                    &mut observer,
                    "SELECT payload FROM unrelated WHERE id=2"
                )),
                Some("2".into())
            );
            if ending != "autocommit" {
                assert_eq!(
                    codes(&query(&mut observer, "SELECT payload FROM renamed")),
                    vec!["40001"]
                );
            }
            let response = query(&mut writer, "COMMIT");
            assert!(codes(&response).is_empty(), "{response:?}");
            assert_eq!(
                value(&query(
                    &mut observer,
                    "SELECT payload FROM unrelated WHERE id=2"
                )),
                Some("99".into())
            );
            if ending != "autocommit" {
                let response = query(
                    &mut owner,
                    match ending {
                        "commit" => "COMMIT",
                        "rollback" => "ROLLBACK",
                        "savepoint" => "ROLLBACK TO before_rename; COMMIT",
                        _ => unreachable!(),
                    },
                );
                assert!(codes(&response).is_empty(), "ending={ending}: {response:?}");
            }
            let table = if matches!(ending, "commit" | "autocommit") {
                "renamed"
            } else {
                "source"
            };
            let expected = if matches!(ending, "commit" | "savepoint") {
                "20"
            } else {
                "10"
            };
            assert_eq!(
                value(&query(
                    &mut observer,
                    &format!("SELECT payload FROM {table}")
                )),
                Some(expected.into())
            );
            assert_eq!(
                value(&query(
                    &mut observer,
                    "SELECT pg_typeof(item) FROM dependent"
                )),
                Some(table.into())
            );
        }));
        for client in [owner, writer, observer] {
            let _ = client.shutdown(std::net::Shutdown::Both);
        }
        server.request_shutdown();
        task.join().unwrap().unwrap();
        if let Err(error) = outcome {
            std::panic::resume_unwind(error);
        }
    }
}

#[test]
fn concurrent_wire_rename_commits_without_blocking_an_unrelated_sql_writer() {
    concurrent_sql_rename("commit");
}

#[test]
fn concurrent_wire_rename_rollback_keeps_the_unrelated_commit() {
    concurrent_sql_rename("rollback");
}

#[test]
fn concurrent_wire_rename_savepoint_keeps_prior_writes_and_unrelated_commit() {
    concurrent_sql_rename("savepoint");
}

#[test]
fn concurrent_wire_rename_autocommit_keeps_the_unrelated_commit() {
    concurrent_sql_rename("autocommit");
}

#[test]
fn concurrent_wire_rename_retains_repeatable_read_across_each_decision_path() {
    for ending in ["commit", "rollback", "savepoint", "autocommit"] {
        concurrent_sql_rename_with_isolation(ending, "REPEATABLE READ");
    }
}

#[test]
fn concurrent_wire_rename_retains_serializable_across_each_decision_path() {
    for ending in ["commit", "rollback", "savepoint", "autocommit"] {
        concurrent_sql_rename_with_isolation(ending, "SERIALIZABLE");
    }
}

#[derive(Clone, Copy)]
enum Case {
    Simple(bool),
    CachedUnrelated,
    CachedSource,
    Typed(&'static str, &'static str),
}

fn exercise(case: Case) {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            DbConfig::default().with_fsync(true).with_storage_mode(mode),
        )
        .unwrap();
        SqlSession::new(&mut db).execute("CREATE TABLE source(id INT PRIMARY KEY); CREATE TABLE unrelated(id INT PRIMARY KEY); INSERT INTO source VALUES(1); INSERT INTO unrelated VALUES(2)").unwrap();
        let mut resources = vec![
            ("__bicdb_pg_schema".into(), "source".into()),
            ("__bicdb_pg_schema".into(), "renamed".into()),
        ];
        if matches!(case, Case::Typed(..)) {
            SqlSession::new(&mut db).execute("CREATE TYPE independent_color AS ENUM('red'); CREATE TYPE independent_pair AS (v INT); CREATE TYPE affected_wrapper AS (item source); CREATE TABLE \"independent.dot\"(id INT); CREATE TABLE typed_unrelated(id INT PRIMARY KEY,e independent_color,p independent_pair); INSERT INTO typed_unrelated VALUES(1,'red',ROW(2)::independent_pair)").unwrap();
            let collection = "__bicdb_pg_user_types";
            let affected = db
                .scan_collection(collection)
                .unwrap()
                .into_iter()
                .find(|record| record.metadata["name"] == "affected_wrapper")
                .expect("the dependent standalone type has its own metadata record");
            resources.push((collection.into(), affected.id.clone()));
        }
        let shared = Arc::new(RwLock::new(db));
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = PgWireServer::open_with_shared(
            root.path(),
            PgWireConfig {
                fsync: true,
                automatic_paged_checkpoint: false,
                ..Default::default()
            },
            Arc::clone(&shared),
        )
        .unwrap();
        let serving = Arc::clone(&server);
        let task = thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
        let mut client = TcpStream::connect(address).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut startup = 196608i32.to_be_bytes().to_vec();
        startup.extend_from_slice(b"user\0bicdb\0database\0bicdb\0\0");
        client
            .write_all(&((startup.len() + 4) as i32).to_be_bytes())
            .unwrap();
        client.write_all(&startup).unwrap();
        assert!(ready(&mut client).iter().all(|(tag, _)| *tag != b'E'));
        match case {
            Case::Simple(true) => assert_eq!(
                value(&query(&mut client, "SELECT id FROM unrelated")),
                Some("2".into())
            ),
            Case::CachedUnrelated => prepare(&mut client, "cached", "SELECT id FROM unrelated"),
            Case::CachedSource => prepare(&mut client, "cached", "SELECT id FROM source"),
            Case::Simple(false) | Case::Typed(..) => {}
        }
        let owner = shared.read().begin_transaction().unwrap();
        let scope = owner
            .reserve_catalog_resources(&["source".into(), "renamed".into()], &resources)
            .unwrap();
        let responses = match case {
            Case::Simple(_) => vec![
                query(&mut client, "SELECT 1"),
                query(&mut client, "SELECT id FROM unrelated"),
                query(&mut client, "SELECT id FROM source"),
            ],
            Case::CachedUnrelated | Case::CachedSource => vec![describe(&mut client, "cached")],
            Case::Typed(sql, _) => vec![query(&mut client, sql)],
        };
        // Cleanup precedes assertions so even a negative control shuts down
        // its own connection/listener and never leaves a serving thread live.
        {
            let _db = shared.read();
            owner.release_catalog_scope(&scope).unwrap();
            owner.rollback().unwrap();
        }
        let restored = query(&mut client, "SELECT id FROM source");
        send(&mut client, b'X', &[]);
        drop(client);
        server.request_shutdown();
        task.join().unwrap().unwrap();
        assert_eq!(value(&restored), Some("1".into()));
        match case {
            Case::Simple(_) => {
                assert!(
                    codes(&responses[0]).is_empty(),
                    "literal SQL must remain available: {:?}",
                    codes(&responses[0])
                );
                assert_eq!(value(&responses[0]), Some("1".into()));
                assert!(
                    codes(&responses[1]).is_empty(),
                    "unrelated SQL must remain available: {:?}",
                    codes(&responses[1])
                );
                assert_eq!(value(&responses[1]), Some("2".into()));
                assert_eq!(codes(&responses[2]), vec!["40001"]);
            }
            Case::CachedUnrelated => {
                assert!(
                    codes(&responses[0]).is_empty(),
                    "cached unrelated Describe must remain available: {:?}",
                    codes(&responses[0])
                );
                assert!(responses[0].iter().any(|(tag, _)| *tag == b'T'));
            }
            Case::CachedSource => {
                assert_eq!(codes(&responses[0]), vec!["40001"]);
                assert!(
                    !responses[0].iter().any(|(tag, _)| *tag == b'T'),
                    "cached metadata must not precede its reservation refusal"
                );
            }
            Case::Typed(_, expected) => {
                assert!(
                    codes(&responses[0]).is_empty(),
                    "an unrelated declared type must remain available: {:?}",
                    codes(&responses[0])
                );
                assert_eq!(value(&responses[0]), Some(expected.into()));
            }
        }
    }
}

#[test]
fn cold_builtin_wire_queries_leave_unrelated_catalog_resources_available() {
    exercise(Case::Simple(false));
}

#[test]
fn warm_builtin_wire_queries_leave_unrelated_catalog_resources_available() {
    exercise(Case::Simple(true));
}

#[test]
fn cached_unrelated_statement_describe_remains_available() {
    exercise(Case::CachedUnrelated);
}

#[test]
fn cached_affected_statement_describe_obeys_catalog_reservation() {
    exercise(Case::CachedSource);
}

#[test]
fn an_unrelated_enum_survives_another_schema_and_type_reservation() {
    exercise(Case::Typed("SELECT e FROM typed_unrelated", "red"));
}

#[test]
fn an_unrelated_composite_survives_another_schema_and_type_reservation() {
    exercise(Case::Typed("SELECT p FROM typed_unrelated", "(2)"));
}

#[test]
fn an_unrelated_table_row_survives_another_schema_and_type_reservation() {
    exercise(Case::Typed("SELECT ROW(2)::unrelated", "(2)"));
}

#[test]
fn oid_cache_hits_keep_affected_and_unknown_identities_fenced() {
    use bicdb_sql::{pg_is_table_row_type_oid, pg_is_user_type_oid, pg_user_type_oid_by_name};
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            DbConfig::default().with_fsync(true).with_storage_mode(mode),
        )
        .unwrap();
        SqlSession::new(&mut db).execute("CREATE TABLE source(id INT); CREATE TABLE unrelated(id INT); CREATE TYPE independent_color AS ENUM('red'); CREATE TYPE affected_wrapper AS (item source)").unwrap();
        let source = pg_user_type_oid_by_name(&db, "source").unwrap().unwrap();
        let unrelated = pg_user_type_oid_by_name(&db, "unrelated").unwrap().unwrap();
        let color = pg_user_type_oid_by_name(&db, "independent_color")
            .unwrap()
            .unwrap();
        let wrapper = pg_user_type_oid_by_name(&db, "affected_wrapper")
            .unwrap()
            .unwrap();
        let metadata = db
            .scan_collection("__bicdb_pg_user_types")
            .unwrap()
            .into_iter()
            .find(|record| record.metadata["name"] == "affected_wrapper")
            .unwrap();
        assert!(!pg_is_user_type_oid(&db, 1_234_567_890).unwrap());
        let owner = db.begin_transaction().unwrap();
        let scope = owner
            .reserve_catalog_resources(
                &["source".into(), "renamed".into()],
                &[
                    ("__bicdb_pg_schema".into(), "source".into()),
                    ("__bicdb_pg_schema".into(), "renamed".into()),
                    ("__bicdb_pg_user_types".into(), metadata.id),
                ],
            )
            .unwrap();
        assert!(!pg_is_user_type_oid(&db, 23).unwrap());
        assert!(!pg_is_table_row_type_oid(&db, 23).unwrap());
        assert!(pg_is_table_row_type_oid(&db, unrelated).unwrap());
        assert!(pg_is_user_type_oid(&db, color).unwrap());
        for blocked in [
            pg_is_table_row_type_oid(&db, source).map(|_| ()),
            pg_is_user_type_oid(&db, wrapper).map(|_| ()),
            pg_is_table_row_type_oid(&db, wrapper).map(|_| ()),
            pg_is_user_type_oid(&db, 1_234_567_890).map(|_| ()),
            pg_user_type_oid_by_name(&db, "source").map(|_| ()),
            pg_user_type_oid_by_name(&db, "affected_wrapper").map(|_| ()),
        ] {
            assert_eq!(blocked.unwrap_err().sqlstate(), "40001");
        }
        owner.release_catalog_scope(&scope).unwrap();
        owner.rollback().unwrap();
        assert!(pg_is_table_row_type_oid(&db, source).unwrap());
        assert!(pg_is_user_type_oid(&db, wrapper).unwrap());
        assert!(!pg_is_user_type_oid(&db, 1_234_567_890).unwrap());
    }
}

#[test]
fn a_quoted_table_row_type_keeps_its_exact_identity_during_another_reservation() {
    exercise(Case::Typed("SELECT ROW(2)::\"independent.dot\"", "(2)"));
}

#[test]
fn created_replacement_receipts_survive_wire_messages_and_repeated_savepoint_undo() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for ending in ["commit", "savepoint", "rollback"] {
            let root = tempfile::tempdir().unwrap();
            let config = DbConfig::default()
                .with_fsync(true)
                .with_storage_mode(mode.clone());
            let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            bicdb_sql::initialize_rename_journal_identities(&mut db).unwrap();
            SqlSession::new(&mut db).execute("CREATE TABLE source(id INT PRIMARY KEY,payload INT); INSERT INTO source VALUES(1,10); CREATE TABLE unrelated(id INT PRIMARY KEY,payload INT); INSERT INTO unrelated VALUES(2,2)").unwrap();
            let shared = Arc::new(RwLock::new(db));
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = PgWireServer::open_with_shared(
                root.path(),
                PgWireConfig {
                    fsync: true,
                    automatic_paged_checkpoint: false,
                    ..Default::default()
                },
                Arc::clone(&shared),
            )
            .unwrap();
            let serving = Arc::clone(&server);
            let task =
                thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
            let mut owner = connect(address);
            let mut writer = connect(address);
            let mut observer = connect(address);
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let response = query(
                    &mut writer,
                    "BEGIN; UPDATE unrelated SET payload=99 WHERE id=2",
                );
                assert!(codes(&response).is_empty(), "{response:?}");
                for command in [
                    "BEGIN",
                    "SAVEPOINT before_rename",
                    "UPDATE source SET payload=20",
                    "ALTER TABLE source RENAME TO renamed",
                    "SAVEPOINT before_replacement",
                ] {
                    let response = query(&mut owner, command);
                    assert!(
                        codes(&response).is_empty(),
                        "{mode} {ending} {command}: {response:?}"
                    );
                }
                for _ in 0..3 {
                    for command in [
                        "CREATE TABLE source(id INT PRIMARY KEY,payload INT)",
                        "INSERT INTO source VALUES(2,30)",
                        "ROLLBACK TO before_replacement",
                    ] {
                        let response = query(&mut owner, command);
                        assert!(
                            codes(&response).is_empty(),
                            "{mode} {ending} {command}: {response:?}"
                        );
                    }
                }
                for command in [
                    "CREATE TABLE source(id INT PRIMARY KEY,payload INT)",
                    "INSERT INTO source VALUES(2,30)",
                ] {
                    let response = query(&mut owner, command);
                    assert!(
                        codes(&response).is_empty(),
                        "{mode} {ending} {command}: {response:?}"
                    );
                }
                assert!(codes(&query(&mut writer, "COMMIT")).is_empty());
                assert_eq!(
                    value(&query(
                        &mut observer,
                        "SELECT payload FROM unrelated WHERE id=2"
                    )),
                    Some("99".into())
                );
                assert_eq!(
                    codes(&query(&mut observer, "SELECT payload FROM source")),
                    vec!["40001"]
                );
                let response = query(
                    &mut owner,
                    match ending {
                        "commit" => "COMMIT",
                        "savepoint" => "ROLLBACK TO before_rename; COMMIT",
                        _ => "ROLLBACK",
                    },
                );
                assert!(codes(&response).is_empty(), "{mode} {ending}: {response:?}");
                assert_eq!(
                    value(&query(&mut observer, "SELECT payload FROM source")),
                    Some(if ending == "commit" { "30" } else { "10" }.into())
                );
                if ending == "commit" {
                    assert_eq!(
                        value(&query(&mut observer, "SELECT payload FROM renamed")),
                        Some("20".into())
                    );
                }
                assert_eq!(
                    value(&query(
                        &mut observer,
                        "SELECT payload FROM unrelated WHERE id=2"
                    )),
                    Some("99".into())
                );
            }));
            for stream in [&mut owner, &mut writer, &mut observer] {
                send(stream, b'X', &[]);
            }
            drop(owner);
            drop(writer);
            drop(observer);
            server.request_shutdown();
            task.join().unwrap().unwrap();
            drop(server);
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while Arc::strong_count(&shared) > 1 && std::time::Instant::now() < deadline {
                thread::sleep(Duration::from_millis(10));
            }
            Arc::try_unwrap(shared)
                .expect("wire handlers must release the owned database before reopen")
                .into_inner()
                .close()
                .unwrap();
            if let Err(error) = outcome {
                std::panic::resume_unwind(error);
            }
            for _ in 0..2 {
                let mut reopened = BicDb::open_with_config(root.path(), config.clone()).unwrap();
                bicdb_sql::oracle::initialize_existing_database_profile(&mut reopened).unwrap();
                let mut session = SqlSession::new(&mut reopened);
                assert_eq!(
                    session.execute("SELECT payload FROM source").unwrap().rows[0][0],
                    bicdb_sql::SqlValue::Int(if ending == "commit" { 30 } else { 10 })
                );
                assert_eq!(
                    session
                        .execute("SELECT payload FROM unrelated WHERE id=2")
                        .unwrap()
                        .rows[0][0],
                    bicdb_sql::SqlValue::Int(99)
                );
                drop(session);
                reopened.close().unwrap();
            }
        }
    }
}

#[test]
fn prepared_information_schema_columns_keep_the_live_rename_owner_in_every_phase() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            DbConfig::default()
                .with_fsync(true)
                .with_storage_mode(mode.clone()),
        )
        .unwrap();
        SqlSession::new(&mut db)
            .execute("CREATE TABLE source(id INT PRIMARY KEY,payload TEXT)")
            .unwrap();
        let source_row_oid = bicdb_sql::pg_user_type_oid_by_name(&db, "source")
            .unwrap()
            .unwrap();
        bicdb_sql::initialize_rename_journal_identities(&mut db).unwrap();
        let shared = Arc::new(RwLock::new(db));
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = PgWireServer::open_with_shared(
            root.path(),
            PgWireConfig {
                fsync: true,
                automatic_paged_checkpoint: false,
                ..Default::default()
            },
            Arc::clone(&shared),
        )
        .unwrap();
        let serving = Arc::clone(&server);
        let task = thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
        let mut owner = connect(address);
        let mut other = connect(address);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            assert!(codes(&query(
                &mut owner,
                "BEGIN; ALTER TABLE source RENAME TO renamed"
            ))
            .is_empty());
            prepare(
                &mut owner,
                "all_column_descriptors",
                "SELECT * FROM information_schema.columns WHERE 1=0",
            );
            let all = describe(&mut owner, "all_column_descriptors");
            let header = &all.iter().find(|(tag, _)| *tag == b'T').unwrap().1;
            assert_eq!(i16::from_be_bytes(header[..2].try_into().unwrap()), 44);
            // A user row type is protected by the owner's rename scope too;
            // fixing the built-in descriptor table alone cannot authorize it.
            prepare(
                &mut owner,
                "owned_row_descriptor",
                "SELECT NULL::renamed AS item WHERE 1=0",
            );
            let owned = describe(&mut owner, "owned_row_descriptor");
            assert!(codes(&owned).is_empty());
            let header = &owned.iter().find(|(tag, _)| *tag == b'T').unwrap().1;
            let end_name = header[2..].iter().position(|byte| *byte == 0).unwrap() + 2;
            let oid_offset = end_name + 1 + 4 + 2;
            assert_eq!(
                u32::from_be_bytes(header[oid_offset..oid_offset + 4].try_into().unwrap()),
                source_row_oid as u32
            );
            // The metadata query is intentionally fully parameterized, with a
            // LOWER call and the real public descriptor types. Parse and both
            // Describe variants must retain the same live owner's identity.
            let sql="SELECT table_schema,column_name,data_type,character_maximum_length,numeric_precision,numeric_scale,is_nullable,column_default FROM information_schema.columns WHERE table_name=LOWER($1) ORDER BY ordinal_position";
            let mut parse = b"owner_metadata\0".to_vec();
            parse.extend_from_slice(sql.as_bytes());
            parse.push(0);
            parse.extend_from_slice(&0i16.to_be_bytes());
            send(&mut owner, b'P', &parse);
            send(&mut owner, b'S', &[]);
            let response = ready(&mut owner);
            assert!(codes(&response).is_empty(), "{mode} Parse: {response:?}");
            let response = describe(&mut owner, "owner_metadata");
            assert!(
                codes(&response).is_empty(),
                "{mode} statement Describe: {response:?}"
            );
            assert!(response.iter().any(|(tag, _)| *tag == b'T'));
            let mut bind = b"owner_portal\0owner_metadata\0".to_vec();
            bind.extend_from_slice(&0i16.to_be_bytes());
            bind.extend_from_slice(&1i16.to_be_bytes());
            bind.extend_from_slice(&7i32.to_be_bytes());
            bind.extend_from_slice(b"RENAMED");
            bind.extend_from_slice(&0i16.to_be_bytes());
            send(&mut owner, b'B', &bind);
            send(&mut owner, b'S', &[]);
            let response = ready(&mut owner);
            assert!(codes(&response).is_empty(), "{mode} Bind: {response:?}");
            send(&mut owner, b'D', b"Powner_portal\0");
            send(&mut owner, b'S', &[]);
            let response = ready(&mut owner);
            assert!(
                codes(&response).is_empty(),
                "{mode} portal Describe: {response:?}"
            );
            let mut execute = b"owner_portal\0".to_vec();
            execute.extend_from_slice(&0i32.to_be_bytes());
            send(&mut owner, b'E', &execute);
            send(&mut owner, b'S', &[]);
            let response = ready(&mut owner);
            assert!(codes(&response).is_empty(), "{mode} Execute: {response:?}");
            assert_eq!(response.iter().filter(|(tag, _)| *tag == b'D').count(), 2);
            assert_eq!(
                codes(&query(
                    &mut other,
                    "SELECT column_name FROM information_schema.columns WHERE table_name='renamed'"
                )),
                vec!["40001"]
            );
            assert!(codes(&query(&mut owner, "ROLLBACK")).is_empty());
            assert_eq!(value(&query(&mut other,"SELECT column_name FROM information_schema.columns WHERE table_name='source' ORDER BY ordinal_position LIMIT 1")),Some("id".into()));
        }));
        for stream in [&mut owner, &mut other] {
            send(stream, b'X', &[]);
        }
        drop(owner);
        drop(other);
        server.request_shutdown();
        task.join().unwrap().unwrap();
        drop(server);
        drop(shared);
        if let Err(error) = outcome {
            std::panic::resume_unwind(error);
        }
    }
}

#[test]
fn cold_prepared_origins_observe_only_the_referenced_table_during_a_foreign_rename() {
    // Each case has a new database and connection so a warmed global OID
    // directory cannot hide an overly broad catalog observation at Describe.
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for sql in [
            "SELECT id FROM unrelated WHERE id=$1",
            "SELECT COUNT(*) FROM unrelated",
            "SELECT payload FROM unrelated",
            "SELECT ROW(id,payload)::unrelated FROM unrelated",
        ] {
            let root = tempfile::tempdir().unwrap();
            let config = DbConfig::default()
                .with_fsync(true)
                .with_storage_mode(mode.clone());
            let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            SqlSession::new(&mut db)
                .execute("CREATE TABLE source(id INT PRIMARY KEY,payload INT); CREATE TABLE unrelated(id INT PRIMARY KEY,payload INT); INSERT INTO source VALUES(1,10); INSERT INTO unrelated VALUES(2,20)")
                .unwrap();
            bicdb_sql::initialize_rename_journal_identities(&mut db).unwrap();
            let shared = Arc::new(RwLock::new(db));
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = PgWireServer::open_with_shared(
                root.path(),
                PgWireConfig {
                    fsync: true,
                    automatic_paged_checkpoint: false,
                    ..Default::default()
                },
                Arc::clone(&shared),
            )
            .unwrap();
            let serving = Arc::clone(&server);
            let task =
                thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
            let mut owner = connect(address);
            let mut preparer = connect(address);
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                assert!(codes(&query(
                    &mut preparer,
                    "BEGIN; UPDATE unrelated SET payload=99 WHERE id=2"
                ))
                .is_empty());
                prepare(&mut preparer, "cold_unrelated", sql);
                let response = query(&mut owner, "BEGIN; ALTER TABLE source RENAME TO renamed");
                assert!(
                    codes(&response).is_empty(),
                    "{mode} after preparing {sql}: {response:?}"
                );
                // The independent writer may still use its own table and ACK
                // while the other transaction keeps the rename undecided.
                assert_eq!(
                    value(&query(
                        &mut preparer,
                        "SELECT payload FROM unrelated WHERE id=2"
                    )),
                    Some("99".into())
                );
                assert!(codes(&query(&mut preparer, "COMMIT")).is_empty());
                assert_eq!(
                    codes(&query(&mut preparer, "SELECT id FROM renamed")),
                    vec!["40001"]
                );
                assert!(codes(&query(&mut owner, "ROLLBACK")).is_empty());
            }));
            for stream in [&mut owner, &mut preparer] {
                send(stream, b'X', &[]);
            }
            drop(owner);
            drop(preparer);
            server.request_shutdown();
            task.join().unwrap().unwrap();
            drop(server);
            Arc::try_unwrap(shared)
                .expect("wire handlers must release the database before reopen")
                .into_inner()
                .close()
                .unwrap();
            if let Err(error) = outcome {
                std::panic::resume_unwind(error);
            }
            for _ in 0..2 {
                let mut reopened = BicDb::open_with_config(root.path(), config.clone()).unwrap();
                bicdb_sql::oracle::initialize_existing_database_profile(&mut reopened).unwrap();
                let mut session = SqlSession::new(&mut reopened);
                assert_eq!(
                    session
                        .execute("SELECT payload FROM source WHERE id=1")
                        .unwrap()
                        .rows[0][0],
                    bicdb_sql::SqlValue::Int(10)
                );
                assert_eq!(
                    session
                        .execute("SELECT payload FROM unrelated WHERE id=2")
                        .unwrap()
                        .rows[0][0],
                    bicdb_sql::SqlValue::Int(99)
                );
                drop(session);
                reopened.close().unwrap();
            }
        }
    }
}

#[test]
fn prepared_regclass_metadata_tracks_the_live_path_and_created_partition_parent() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let config = DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(mode.clone());
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        // Keep the user key distinct from the native implicit id convention.
        SqlSession::new(&mut db)
            .execute("CREATE SCHEMA tenant_a; CREATE SCHEMA tenant_b; CREATE TABLE tenant_a.source(item_key INT PRIMARY KEY,payload INT); CREATE TABLE tenant_b.source(item_key INT PRIMARY KEY,payload INT); INSERT INTO tenant_a.source VALUES(1,10); INSERT INTO tenant_b.source VALUES(2,20)")
            .unwrap();
        bicdb_sql::initialize_rename_journal_identities(&mut db).unwrap();
        let shared = Arc::new(RwLock::new(db));
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = PgWireServer::open_with_shared(
            root.path(),
            PgWireConfig {
                fsync: true,
                automatic_paged_checkpoint: false,
                ..Default::default()
            },
            Arc::clone(&shared),
        )
        .unwrap();
        let serving = Arc::clone(&server);
        let task = thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
        let mut owner = connect(address);
        let mut writer = connect(address);
        let mut observer = connect(address);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            assert!(codes(&query(&mut owner, "SET search_path=tenant_a,tenant_b")).is_empty());
            prepare(
                &mut owner,
                "partition_metadata",
                "SELECT COUNT(*) FROM pg_constraint c WHERE conrelid=LOWER($1)::regclass AND contype='p'",
            );
            let count = |owner: &mut TcpStream| {
                let mut bind = b"metadata_portal\0partition_metadata\0".to_vec();
                bind.extend_from_slice(&0i16.to_be_bytes());
                bind.extend_from_slice(&1i16.to_be_bytes());
                bind.extend_from_slice(&6i32.to_be_bytes());
                bind.extend_from_slice(b"SOURCE");
                bind.extend_from_slice(&0i16.to_be_bytes());
                send(owner, b'B', &bind);
                send(owner, b'D', b"Pmetadata_portal\0");
                let mut execute = b"metadata_portal\0".to_vec();
                execute.extend_from_slice(&0i32.to_be_bytes());
                send(owner, b'E', &execute);
                send(owner, b'S', &[]);
                let response = ready(owner);
                assert!(codes(&response).is_empty(), "{mode}: {response:?}");
                value(&response).unwrap()
            };
            assert_eq!(count(&mut owner), "1");
            assert!(codes(&query(
                &mut writer,
                "BEGIN; UPDATE tenant_b.source SET payload=99 WHERE item_key=2"
            ))
            .is_empty());
            for command in [
                "BEGIN",
                "ALTER TABLE source RENAME TO source_default_partition",
                "CREATE TABLE source(item_key INT,tenant_id INT) PARTITION BY LIST(tenant_id)",
            ] {
                let response = query(&mut owner, command);
                assert!(
                    codes(&response).is_empty(),
                    "{mode} {command}: {response:?}"
                );
            }
            assert_eq!(count(&mut owner), "0");
            for query_text in [
                "SELECT to_regclass('tenant_a.source')::oid",
                "SELECT 'tenant_a.source_default_partition'::regclass::oid",
            ] {
                assert_eq!(codes(&query(&mut observer, query_text)), vec!["40001"]);
            }
            assert!(codes(&query(&mut writer, "COMMIT")).is_empty());
            assert!(codes(&query(&mut owner, "SAVEPOINT before_path")).is_empty());
            assert!(codes(&query(
                &mut owner,
                "SET LOCAL search_path=tenant_b,tenant_a"
            ))
            .is_empty());
            assert_eq!(count(&mut owner), "1");
            assert!(codes(&query(&mut owner, "ROLLBACK TO before_path")).is_empty());
            assert_eq!(count(&mut owner), "0");
            assert!(codes(&query(&mut owner, "ROLLBACK")).is_empty());
            assert_eq!(count(&mut owner), "1");
        }));
        for stream in [&mut owner, &mut writer, &mut observer] {
            send(stream, b'X', &[]);
        }
        drop(owner);
        drop(writer);
        drop(observer);
        server.request_shutdown();
        task.join().unwrap().unwrap();
        drop(server);
        Arc::try_unwrap(shared)
            .expect("wire handlers must release the database before reopen")
            .into_inner()
            .close()
            .unwrap();
        if let Err(error) = outcome {
            std::panic::resume_unwind(error);
        }
        for _ in 0..2 {
            let mut reopened = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            bicdb_sql::oracle::initialize_existing_database_profile(&mut reopened).unwrap();
            let mut sql = SqlSession::new(&mut reopened);
            assert_eq!(
                sql.execute("SELECT payload FROM tenant_a.source WHERE item_key=1")
                    .unwrap()
                    .rows[0][0],
                bicdb_sql::SqlValue::Int(10)
            );
            assert_eq!(
                sql.execute("SELECT payload FROM tenant_b.source WHERE item_key=2")
                    .unwrap()
                    .rows[0][0],
                bicdb_sql::SqlValue::Int(99)
            );
            drop(sql);
            reopened.close().unwrap();
        }
    }
}
