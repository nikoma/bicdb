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

#[derive(Clone, Copy)]
enum Case {
    Simple(bool),
    CachedUnrelated,
    CachedSource,
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
            Case::Simple(false) => {}
        }
        let owner = shared.read().begin_transaction().unwrap();
        let scope = owner
            .reserve_catalog_resources(
                &["source".into(), "renamed".into()],
                &[
                    ("__bicdb_pg_schema".into(), "source".into()),
                    ("__bicdb_pg_schema".into(), "renamed".into()),
                ],
            )
            .unwrap();
        let responses = match case {
            Case::Simple(_) => vec![
                query(&mut client, "SELECT 1"),
                query(&mut client, "SELECT id FROM unrelated"),
                query(&mut client, "SELECT id FROM source"),
            ],
            Case::CachedUnrelated | Case::CachedSource => vec![describe(&mut client, "cached")],
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
