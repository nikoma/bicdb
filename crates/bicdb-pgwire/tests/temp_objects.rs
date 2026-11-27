//! Temporary objects: views, sequences and DDL over temporary tables, found
//! through the temporary schema and dropped with the session.
use bicdb_pgwire::{PgWireConfig, PgWireServer, ServerCopyConfig};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn send_message(stream: &mut TcpStream, tag: u8, payload: &[u8]) {
    stream.write_all(&[tag]).unwrap();
    stream
        .write_all(&((payload.len() as i32) + 4).to_be_bytes())
        .unwrap();
    stream.write_all(payload).unwrap();
}

fn read_message(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut tag = [0_u8; 1];
    stream.read_exact(&mut tag).unwrap();
    let mut len = [0_u8; 4];
    stream.read_exact(&mut len).unwrap();
    let len = i32::from_be_bytes(len);
    let mut payload = vec![0_u8; (len - 4) as usize];
    stream.read_exact(&mut payload).unwrap();
    (tag[0], payload)
}

fn read_until_ready(stream: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
    let mut messages = Vec::new();
    loop {
        let (tag, payload) = read_message(stream);
        if tag == b'Z' {
            return messages;
        }
        messages.push((tag, payload));
    }
}

/// Startup as `bicdb`; returns the BackendKeyData (pid, secret).
fn startup(stream: &mut TcpStream) -> (i32, i32) {
    let mut payload = Vec::new();
    payload.extend_from_slice(&196_608_i32.to_be_bytes());
    for part in ["user", "bicdb"] {
        payload.extend_from_slice(part.as_bytes());
        payload.push(0);
    }
    payload.push(0);
    stream
        .write_all(&((payload.len() as i32) + 4).to_be_bytes())
        .unwrap();
    stream.write_all(&payload).unwrap();
    let mut key = (0, 0);
    for (tag, payload) in read_until_ready(stream) {
        if tag == b'K' {
            key = (
                i32::from_be_bytes(payload[0..4].try_into().unwrap()),
                i32::from_be_bytes(payload[4..8].try_into().unwrap()),
            );
        }
    }
    key
}

/// One query: (error fields, data rows as text, command tags).
struct Outcome {
    error: Option<String>,
    rows: Vec<Vec<Option<String>>>,
    tags: Vec<String>,
}

impl Outcome {
    fn ok(&self) -> &Self {
        assert!(self.error.is_none(), "unexpected error: {:?}", self.error);
        self
    }
    fn sqlstate(&self) -> String {
        let error = self.error.as_deref().expect("an error");
        error
            .split('\0')
            .find_map(|field| field.strip_prefix('C'))
            .unwrap_or_default()
            .to_string()
    }
    fn field(&self, code: char) -> String {
        let error = self.error.as_deref().expect("an error");
        error
            .split('\0')
            .find_map(|field| field.strip_prefix(code))
            .unwrap_or_default()
            .to_string()
    }
}

fn query(stream: &mut TcpStream, sql: &str) -> Outcome {
    let mut payload = sql.as_bytes().to_vec();
    payload.push(0);
    send_message(stream, b'Q', &payload);
    let mut outcome = Outcome {
        error: None,
        rows: Vec::new(),
        tags: Vec::new(),
    };
    for (tag, payload) in read_until_ready(stream) {
        match tag {
            b'E' => outcome.error = Some(String::from_utf8_lossy(&payload).to_string()),
            b'C' => outcome
                .tags
                .push(String::from_utf8_lossy(&payload[..payload.len() - 1]).to_string()),
            b'D' => {
                let count = i16::from_be_bytes([payload[0], payload[1]]) as usize;
                let mut idx = 2;
                let mut row = Vec::new();
                for _ in 0..count {
                    let len = i32::from_be_bytes(payload[idx..idx + 4].try_into().unwrap());
                    idx += 4;
                    if len < 0 {
                        row.push(None);
                    } else {
                        let len = len as usize;
                        row.push(Some(
                            String::from_utf8_lossy(&payload[idx..idx + len]).to_string(),
                        ));
                        idx += len;
                    }
                }
                outcome.rows.push(row);
            }
            _ => {}
        }
    }
    outcome
}

struct Served {
    server: Arc<PgWireServer>,
    thread: Option<thread::JoinHandle<()>>,
    address: std::net::SocketAddr,
    _dir: tempfile::TempDir,
}

impl Served {
    fn start(server_copy: ServerCopyConfig) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = PgWireServer::open(
            dir.path(),
            PgWireConfig {
                server_copy,
                ..PgWireConfig::default()
            },
        )
        .unwrap();
        let for_thread = server.clone();
        let thread = thread::spawn(move || {
            let _ = bicdb_pgwire::serve_existing_listener(for_thread, listener);
        });
        Self {
            server,
            thread: Some(thread),
            address,
            _dir: dir,
        }
    }

    fn connect(&self) -> (TcpStream, (i32, i32)) {
        let mut client = TcpStream::connect(self.address).unwrap();
        let key = startup(&mut client);
        (client, key)
    }
}

impl Drop for Served {
    fn drop(&mut self) {
        self.server.request_shutdown();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn notices_of(stream: &mut TcpStream, sql: &str) -> (Vec<String>, Outcome) {
    let mut payload = sql.as_bytes().to_vec();
    payload.push(0);
    send_message(stream, b'Q', &payload);
    let mut notices = Vec::new();
    let mut outcome = Outcome {
        error: None,
        rows: Vec::new(),
        tags: Vec::new(),
    };
    for (tag, payload) in read_until_ready(stream) {
        let text = String::from_utf8_lossy(&payload).to_string();
        match tag {
            b'N' => notices.extend(
                text.split('\0')
                    .find_map(|field| field.strip_prefix('M').map(str::to_string)),
            ),
            b'E' => outcome.error = Some(text),
            _ => {}
        }
    }
    (notices, outcome)
}

fn one(stream: &mut TcpStream, sql: &str) -> Vec<Vec<Option<String>>> {
    let outcome = query(stream, sql);
    assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
    outcome.rows
}

fn text(value: &str) -> Vec<Vec<Option<String>>> {
    vec![vec![Some(value.to_string())]]
}

#[test]
fn temporary_views_and_sequences_live_in_the_session() {
    let served = Served::start(ServerCopyConfig::default());
    let (mut first, _) = served.connect();
    one(&mut first, "CREATE TEMP VIEW tv AS SELECT 1 AS a");
    assert_eq!(one(&mut first, "SELECT a FROM tv"), text("1"));
    one(
        &mut first,
        "CREATE OR REPLACE TEMP VIEW tv AS SELECT 2 AS a",
    );
    assert_eq!(one(&mut first, "SELECT a FROM tv"), text("2"));
    one(&mut first, "CREATE TEMP SEQUENCE ts");
    assert_eq!(one(&mut first, "SELECT nextval('ts')"), text("1"));
    // A view over a temporary table becomes temporary, with the NOTICE.
    one(&mut first, "CREATE TEMP TABLE tt(a int)");
    one(&mut first, "INSERT INTO tt VALUES (5)");
    let (notices, outcome) = notices_of(&mut first, "CREATE VIEW over_temp AS SELECT * FROM tt");
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(notices, vec!["view \"over_temp\" will be a temporary view"]);
    assert_eq!(
        one(
            &mut first,
            "SELECT n.nspname LIKE 'pg_temp%' FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = 'over_temp'"
        ),
        text("t")
    );
    // Another session sees none of them.
    let (mut second, _) = served.connect();
    for sql in [
        "SELECT * FROM tv",
        "SELECT * FROM over_temp",
        "SELECT nextval('ts')",
    ] {
        assert!(query(&mut second, sql).error.is_some(), "{sql}");
    }
    // They go with the session; nothing it left behind breaks the catalog.
    first.write_all(b"X\0\0\0\x04").unwrap();
    drop(first);
    thread::sleep(Duration::from_millis(500));
    assert_eq!(
        one(
            &mut second,
            "SELECT count(*) FROM pg_class WHERE relname IN ('tv', 'over_temp', 'ts', 'tt')"
        ),
        text("0")
    );
    one(&mut second, "CREATE TABLE after_session(a int)");
    assert_eq!(
        one(&mut second, "SELECT count(*) FROM after_session"),
        text("0")
    );
}

#[test]
fn ddl_finds_temporary_tables_and_refuses_permanent_dependents() {
    let served = Served::start(ServerCopyConfig::default());
    let (mut client, _) = served.connect();
    one(
        &mut client,
        "CREATE TEMP TABLE src(a int, b text DEFAULT 'x' NOT NULL)",
    );
    one(
        &mut client,
        "CREATE TABLE copy_of (LIKE src INCLUDING DEFAULTS)",
    );
    one(&mut client, "INSERT INTO copy_of(a) VALUES (1)");
    assert_eq!(one(&mut client, "SELECT b FROM copy_of"), text("x"));
    one(&mut client, "CREATE TEMP TABLE child(c int) INHERITS (src)");
    one(&mut client, "INSERT INTO child VALUES (1, 'y', 2)");
    assert_eq!(one(&mut client, "SELECT count(*) FROM src"), text("1"));
    one(&mut client, "COMMENT ON TABLE src IS 'temporary'");
    assert_eq!(
        one(
            &mut client,
            "SELECT obj_description('src'::regclass, 'pg_class')"
        ),
        text("temporary")
    );
    assert_eq!(
        one(&mut client, "SELECT 'src'::regclass::text"),
        text("src")
    );
    for (sql, state, message) in [
        (
            "CREATE TABLE perm_child(c int) INHERITS (src)",
            "42809",
            "cannot inherit from temporary relation \"src\"",
        ),
        (
            "CREATE TABLE perm_ref(a int REFERENCES child_key)",
            "42P16",
            "constraints on permanent tables may reference only permanent tables",
        ),
        (
            "CREATE VIEW public.perm_view AS SELECT * FROM src",
            "42P16",
            "cannot create temporary relation in non-temporary schema",
        ),
        (
            "CREATE MATERIALIZED VIEW mv AS SELECT * FROM src",
            "0A000",
            "materialized views must not use temporary tables or views",
        ),
    ] {
        if sql.contains("child_key") {
            one(
                &mut client,
                "CREATE TEMP TABLE child_key(a int PRIMARY KEY)",
            );
        }
        let outcome = query(&mut client, sql);
        assert_eq!(outcome.sqlstate(), state, "{sql}: {:?}", outcome.error);
        assert_eq!(outcome.field('M'), message, "{sql}");
    }
    one(&mut client, "CREATE TEMP TABLE uq(a int UNIQUE)");
    one(&mut client, "INSERT INTO uq VALUES (1)");
    let outcome = query(&mut client, "INSERT INTO uq VALUES (1)");
    assert_eq!(
        outcome.field('M'),
        "duplicate key value violates unique constraint \"uq_a_key\""
    );
    one(
        &mut client,
        "CREATE TEMP TABLE parted(a int) PARTITION BY RANGE (a)",
    );
    one(
        &mut client,
        "CREATE TEMP TABLE parted_1 PARTITION OF parted FOR VALUES FROM (0) TO (10)",
    );
    one(&mut client, "INSERT INTO parted VALUES (5)");
    assert_eq!(one(&mut client, "SELECT count(*) FROM parted_1"), text("1"));
}
