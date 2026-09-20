//! End-to-end tests: a real server on an ephemeral port, raw RESP over TCP.

use std::io::{BufReader, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::time::Duration;

use bicdb_resp::{EvictionPolicy, RespConfig, RespServerHandle};

fn test_config() -> RespConfig {
    RespConfig {
        port: 0,
        ..RespConfig::default()
    }
}

fn start_server(path: &Path, config: RespConfig) -> RespServerHandle {
    bicdb_resp::start(path, config).expect("server starts")
}

#[derive(Clone, Debug, PartialEq)]
enum Reply {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Vec<u8>),
    Nil,
    Array(Vec<Reply>),
}

impl Reply {
    fn bulk_str(&self) -> &str {
        match self {
            Reply::Bulk(bytes) => std::str::from_utf8(bytes).expect("utf8 bulk"),
            other => panic!("expected bulk, got {other:?}"),
        }
    }

    fn int(&self) -> i64 {
        match self {
            Reply::Int(value) => *value,
            other => panic!("expected int, got {other:?}"),
        }
    }

    fn array(&self) -> &[Reply] {
        match self {
            Reply::Array(items) => items,
            other => panic!("expected array, got {other:?}"),
        }
    }
}

struct Client {
    writer: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Client {
    fn connect(handle: &RespServerHandle) -> Self {
        let stream = TcpStream::connect(handle.local_addr()).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        Self {
            writer: stream.try_clone().unwrap(),
            reader: BufReader::new(stream),
        }
    }

    fn cmd(&mut self, args: &[&[u8]]) -> Reply {
        let mut request = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            request.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            request.extend_from_slice(arg);
            request.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&request).expect("send");
        self.read_reply()
    }

    fn cmd_str(&mut self, args: &[&str]) -> Reply {
        let raw: Vec<&[u8]> = args.iter().map(|arg| arg.as_bytes()).collect();
        self.cmd(&raw)
    }

    fn read_reply(&mut self) -> Reply {
        let mut kind = [0u8; 1];
        self.reader.read_exact(&mut kind).expect("reply type byte");
        let line = self.read_line();
        match kind[0] {
            b'+' => Reply::Simple(line),
            b'-' => Reply::Error(line),
            b':' => Reply::Int(line.parse().expect("int reply")),
            b'$' => {
                let len: i64 = line.parse().expect("bulk length");
                if len < 0 {
                    return Reply::Nil;
                }
                let mut body = vec![0u8; len as usize + 2];
                self.reader.read_exact(&mut body).expect("bulk body");
                body.truncate(len as usize);
                Reply::Bulk(body)
            }
            b'*' => {
                let len: i64 = line.parse().expect("array length");
                if len < 0 {
                    return Reply::Nil;
                }
                let items = (0..len).map(|_| self.read_reply()).collect();
                Reply::Array(items)
            }
            other => panic!("unexpected reply type byte: {other}"),
        }
    }

    fn read_line(&mut self) -> String {
        let mut line = Vec::new();
        loop {
            let mut byte = [0u8; 1];
            self.reader.read_exact(&mut byte).expect("line byte");
            if byte[0] == b'\n' {
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                return String::from_utf8(line).expect("utf8 line");
            }
            line.push(byte[0]);
        }
    }
}

fn assert_ok(reply: Reply) {
    assert_eq!(reply, Reply::Simple("OK".to_string()));
}

#[test]
fn ping_echo_and_connection_plumbing() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(dir.path(), test_config());
    let mut client = Client::connect(&server);

    assert_eq!(client.cmd_str(&["PING"]), Reply::Simple("PONG".into()));
    assert_eq!(
        client.cmd_str(&["PING", "hello"]),
        Reply::Bulk(b"hello".to_vec())
    );
    assert_eq!(
        client.cmd_str(&["ECHO", "payload"]),
        Reply::Bulk(b"payload".to_vec())
    );
    assert_ok(client.cmd_str(&["CLIENT", "SETNAME", "test-app"]));
    assert_eq!(
        client.cmd_str(&["CLIENT", "GETNAME"]),
        Reply::Bulk(b"test-app".to_vec())
    );
    assert_eq!(client.cmd_str(&["COMMAND"]), Reply::Array(vec![]));
    match client.cmd_str(&["INFO"]) {
        Reply::Bulk(info) => {
            let text = String::from_utf8(info).unwrap();
            assert!(
                text.contains("redis_version:"),
                "INFO missing version: {text}"
            );
        }
        other => panic!("INFO returned {other:?}"),
    }

    drop(client);
    server.shutdown();
}

#[test]
fn set_get_del_exists_type_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(dir.path(), test_config());
    let mut client = Client::connect(&server);

    assert_ok(client.cmd_str(&["SET", "greeting", "hello world"]));
    assert_eq!(
        client.cmd_str(&["GET", "greeting"]),
        Reply::Bulk(b"hello world".to_vec())
    );
    assert_eq!(client.cmd_str(&["EXISTS", "greeting"]).int(), 1);
    assert_eq!(client.cmd_str(&["EXISTS", "missing"]).int(), 0);
    assert_eq!(
        client
            .cmd_str(&["EXISTS", "greeting", "greeting", "missing"])
            .int(),
        2
    );
    assert_eq!(
        client.cmd_str(&["TYPE", "greeting"]),
        Reply::Simple("string".into())
    );
    assert_eq!(
        client.cmd_str(&["TYPE", "missing"]),
        Reply::Simple("none".into())
    );
    assert_eq!(client.cmd_str(&["DEL", "greeting", "missing"]).int(), 1);
    assert_eq!(client.cmd_str(&["GET", "greeting"]), Reply::Nil);
    assert_eq!(client.cmd_str(&["DBSIZE"]).int(), 0);

    drop(client);
    server.shutdown();
}

#[test]
fn binary_safe_keys_and_values() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(dir.path(), test_config());
    let mut client = Client::connect(&server);

    let value = b"line1\r\nline2\0binary\xff".to_vec();
    assert_ok(client.cmd(&[b"SET", b"bin-value", &value]));
    assert_eq!(client.cmd(&[b"GET", b"bin-value"]), Reply::Bulk(value));

    let key: &[u8] = &[0xff, 0xfe, 0x00, 0x01];
    assert_ok(client.cmd(&[b"SET", key, b"non-utf8 key"]));
    assert_eq!(
        client.cmd(&[b"GET", key]),
        Reply::Bulk(b"non-utf8 key".to_vec())
    );
    assert_eq!(client.cmd(&[b"DEL", key]).int(), 1);

    drop(client);
    server.shutdown();
}

#[test]
fn set_options_nx_xx_get_keepttl() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(dir.path(), test_config());
    let mut client = Client::connect(&server);

    assert_ok(client.cmd_str(&["SET", "k", "v1", "NX"]));
    assert_eq!(client.cmd_str(&["SET", "k", "v2", "NX"]), Reply::Nil);
    assert_eq!(client.cmd_str(&["GET", "k"]), Reply::Bulk(b"v1".to_vec()));

    assert_ok(client.cmd_str(&["SET", "k", "v2", "XX"]));
    assert_eq!(client.cmd_str(&["SET", "absent", "x", "XX"]), Reply::Nil);

    assert_eq!(
        client.cmd_str(&["SET", "k", "v3", "GET"]),
        Reply::Bulk(b"v2".to_vec())
    );
    assert_eq!(client.cmd_str(&["SET", "fresh", "x", "GET"]), Reply::Nil);

    // KEEPTTL: overwrite must not clear the TTL.
    assert_ok(client.cmd_str(&["SET", "ttlkey", "v1", "EX", "100"]));
    assert_ok(client.cmd_str(&["SET", "ttlkey", "v2", "KEEPTTL"]));
    assert!(client.cmd_str(&["TTL", "ttlkey"]).int() > 90);
    // Plain overwrite clears it.
    assert_ok(client.cmd_str(&["SET", "ttlkey", "v3"]));
    assert_eq!(client.cmd_str(&["TTL", "ttlkey"]).int(), -1);

    drop(client);
    server.shutdown();
}

#[test]
fn ttl_expiry_lazy_and_swept() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(dir.path(), test_config());
    let mut client = Client::connect(&server);

    assert_ok(client.cmd_str(&["SET", "short", "gone soon", "PX", "80"]));
    assert_ok(client.cmd_str(&["SET", "long", "stays", "EX", "100"]));
    assert!(client.cmd_str(&["PTTL", "short"]).int() > 0);
    assert!(client.cmd_str(&["TTL", "long"]).int() >= 99);
    assert_eq!(client.cmd_str(&["TTL", "missing"]).int(), -2);

    std::thread::sleep(Duration::from_millis(200));
    // Lazy: reads see it as gone; sweeper physically removes it.
    assert_eq!(client.cmd_str(&["GET", "short"]), Reply::Nil);
    assert_eq!(client.cmd_str(&["TTL", "short"]).int(), -2);
    assert_eq!(client.cmd_str(&["EXISTS", "short"]).int(), 0);
    assert_eq!(
        client.cmd_str(&["GET", "long"]),
        Reply::Bulk(b"stays".to_vec())
    );
    assert_eq!(client.cmd_str(&["DBSIZE"]).int(), 1);

    // EXPIRE / PERSIST on a live key.
    assert_eq!(client.cmd_str(&["EXPIRE", "long", "500"]).int(), 1);
    assert!(client.cmd_str(&["TTL", "long"]).int() > 400);
    assert_eq!(client.cmd_str(&["PERSIST", "long"]).int(), 1);
    assert_eq!(client.cmd_str(&["TTL", "long"]).int(), -1);
    assert_eq!(client.cmd_str(&["PERSIST", "long"]).int(), 0);
    assert_eq!(client.cmd_str(&["EXPIRE", "missing", "10"]).int(), 0);

    // SETEX and GETEX.
    assert_ok(client.cmd_str(&["SETEX", "sess", "50", "data"]));
    assert!(client.cmd_str(&["TTL", "sess"]).int() > 45);
    assert_eq!(
        client.cmd_str(&["GETEX", "sess", "PERSIST"]),
        Reply::Bulk(b"data".to_vec())
    );
    assert_eq!(client.cmd_str(&["TTL", "sess"]).int(), -1);

    drop(client);
    server.shutdown();
}

#[test]
fn counters_and_append() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(dir.path(), test_config());
    let mut client = Client::connect(&server);

    assert_eq!(client.cmd_str(&["INCR", "hits"]).int(), 1);
    assert_eq!(client.cmd_str(&["INCR", "hits"]).int(), 2);
    assert_eq!(client.cmd_str(&["INCRBY", "hits", "40"]).int(), 42);
    assert_eq!(client.cmd_str(&["DECR", "hits"]).int(), 41);
    assert_eq!(client.cmd_str(&["DECRBY", "hits", "40"]).int(), 1);

    assert_ok(client.cmd_str(&["SET", "text", "not a number"]));
    match client.cmd_str(&["INCR", "text"]) {
        Reply::Error(message) => assert!(message.contains("not an integer"), "{message}"),
        other => panic!("expected error, got {other:?}"),
    }

    assert_eq!(
        client.cmd_str(&["INCRBYFLOAT", "price", "10.5"]),
        Reply::Bulk(b"10.5".to_vec())
    );
    assert_eq!(
        client.cmd_str(&["INCRBYFLOAT", "price", "0.1"]),
        Reply::Bulk(b"10.6".to_vec())
    );

    assert_eq!(client.cmd_str(&["APPEND", "log", "abc"]).int(), 3);
    assert_eq!(client.cmd_str(&["APPEND", "log", "def"]).int(), 6);
    assert_eq!(
        client.cmd_str(&["GET", "log"]),
        Reply::Bulk(b"abcdef".to_vec())
    );
    assert_eq!(client.cmd_str(&["STRLEN", "log"]).int(), 6);
    assert_eq!(client.cmd_str(&["STRLEN", "missing"]).int(), 0);

    drop(client);
    server.shutdown();
}

#[test]
fn multi_key_commands() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(dir.path(), test_config());
    let mut client = Client::connect(&server);

    assert_ok(client.cmd_str(&["MSET", "a", "1", "b", "2", "c", "3"]));
    let replies = client.cmd_str(&["MGET", "a", "b", "missing", "c"]);
    assert_eq!(
        replies.array(),
        &[
            Reply::Bulk(b"1".to_vec()),
            Reply::Bulk(b"2".to_vec()),
            Reply::Nil,
            Reply::Bulk(b"3".to_vec()),
        ]
    );

    assert_eq!(client.cmd_str(&["MSETNX", "x", "1", "y", "2"]).int(), 1);
    // One key exists -> nothing is written.
    assert_eq!(client.cmd_str(&["MSETNX", "y", "9", "z", "3"]).int(), 0);
    assert_eq!(client.cmd_str(&["GET", "y"]), Reply::Bulk(b"2".to_vec()));
    assert_eq!(client.cmd_str(&["GET", "z"]), Reply::Nil);

    assert_eq!(client.cmd_str(&["SETNX", "a", "other"]).int(), 0);
    assert_eq!(client.cmd_str(&["SETNX", "brand-new", "v"]).int(), 1);

    assert_eq!(
        client.cmd_str(&["GETSET", "a", "10"]),
        Reply::Bulk(b"1".to_vec())
    );
    assert_eq!(
        client.cmd_str(&["GETDEL", "a"]),
        Reply::Bulk(b"10".to_vec())
    );
    assert_eq!(client.cmd_str(&["GET", "a"]), Reply::Nil);

    assert_ok(client.cmd_str(&["RENAME", "b", "b2"]));
    assert_eq!(client.cmd_str(&["GET", "b2"]), Reply::Bulk(b"2".to_vec()));
    match client.cmd_str(&["RENAME", "nope", "dst"]) {
        Reply::Error(message) => assert!(message.contains("no such key"), "{message}"),
        other => panic!("expected error, got {other:?}"),
    }

    drop(client);
    server.shutdown();
}

#[test]
fn keys_scan_and_glob_matching() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(dir.path(), test_config());
    let mut client = Client::connect(&server);

    for index in 0..5 {
        assert_ok(client.cmd_str(&["SET", &format!("user:{index}"), "u"]));
    }
    assert_ok(client.cmd_str(&["SET", "session:9", "s"]));

    let keys = client.cmd_str(&["KEYS", "user:*"]);
    assert_eq!(keys.array().len(), 5);
    let keys = client.cmd_str(&["KEYS", "*"]);
    assert_eq!(keys.array().len(), 6);
    let keys = client.cmd_str(&["KEYS", "user:[0-2]"]);
    assert_eq!(keys.array().len(), 3);

    // Full SCAN loop must visit every key exactly once.
    let mut cursor = "0".to_string();
    let mut seen = Vec::new();
    loop {
        let reply = client.cmd_str(&["SCAN", &cursor, "COUNT", "2"]);
        let parts = reply.array();
        cursor = parts[0].bulk_str().to_string();
        for key in parts[1].array() {
            seen.push(key.bulk_str().to_string());
        }
        if cursor == "0" {
            break;
        }
    }
    seen.sort();
    assert_eq!(seen.len(), 6);
    assert!(seen.contains(&"session:9".to_string()));

    let reply = client.cmd_str(&["SCAN", "0", "MATCH", "session:*", "COUNT", "100"]);
    assert_eq!(reply.array()[1].array().len(), 1);

    drop(client);
    server.shutdown();
}

#[test]
fn select_databases_and_flush() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(dir.path(), test_config());
    let mut client = Client::connect(&server);

    assert_ok(client.cmd_str(&["SET", "shared-name", "db0-value"]));
    assert_ok(client.cmd_str(&["SELECT", "1"]));
    assert_eq!(client.cmd_str(&["GET", "shared-name"]), Reply::Nil);
    assert_ok(client.cmd_str(&["SET", "shared-name", "db1-value"]));
    assert_eq!(client.cmd_str(&["DBSIZE"]).int(), 1);

    assert_ok(client.cmd_str(&["FLUSHDB"]));
    assert_eq!(client.cmd_str(&["DBSIZE"]).int(), 0);
    assert_ok(client.cmd_str(&["SELECT", "0"]));
    assert_eq!(
        client.cmd_str(&["GET", "shared-name"]),
        Reply::Bulk(b"db0-value".to_vec())
    );

    assert_ok(client.cmd_str(&["FLUSHALL"]));
    assert_eq!(client.cmd_str(&["DBSIZE"]).int(), 0);

    match client.cmd_str(&["SELECT", "99"]) {
        Reply::Error(message) => assert!(message.contains("out of range"), "{message}"),
        other => panic!("expected error, got {other:?}"),
    }

    drop(client);
    server.shutdown();
}

#[test]
fn values_and_ttls_survive_restart() {
    let dir = tempfile::tempdir().unwrap();

    let server = start_server(dir.path(), test_config());
    let mut client = Client::connect(&server);
    assert_ok(client.cmd_str(&["SET", "durable", "kept across restart"]));
    assert_ok(client.cmd_str(&["SET", "with-ttl", "v", "EX", "100"]));
    assert_ok(client.cmd_str(&["SET", "short-ttl", "v", "PX", "50"]));
    drop(client);
    server.shutdown();

    std::thread::sleep(Duration::from_millis(80));

    let server = start_server(dir.path(), test_config());
    let mut client = Client::connect(&server);
    assert_eq!(
        client.cmd_str(&["GET", "durable"]),
        Reply::Bulk(b"kept across restart".to_vec())
    );
    let remaining = client.cmd_str(&["TTL", "with-ttl"]).int();
    assert!((1..=100).contains(&remaining), "ttl was {remaining}");
    // Expired while the server was down: must be gone after restart.
    assert_eq!(client.cmd_str(&["GET", "short-ttl"]), Reply::Nil);

    drop(client);
    server.shutdown();
}

#[test]
fn auth_gate() {
    let dir = tempfile::tempdir().unwrap();
    let config = RespConfig {
        password: Some("sesame".to_string()),
        ..test_config()
    };
    let server = start_server(dir.path(), config);
    let mut client = Client::connect(&server);

    match client.cmd_str(&["GET", "k"]) {
        Reply::Error(message) => assert!(message.starts_with("NOAUTH"), "{message}"),
        other => panic!("expected NOAUTH, got {other:?}"),
    }
    match client.cmd_str(&["AUTH", "wrong"]) {
        Reply::Error(message) => assert!(message.starts_with("WRONGPASS"), "{message}"),
        other => panic!("expected WRONGPASS, got {other:?}"),
    }
    assert_ok(client.cmd_str(&["AUTH", "sesame"]));
    assert_ok(client.cmd_str(&["SET", "k", "v"]));

    // Two-argument form with the default user.
    let mut second = Client::connect(&server);
    assert_ok(second.cmd_str(&["AUTH", "default", "sesame"]));
    assert_eq!(second.cmd_str(&["GET", "k"]), Reply::Bulk(b"v".to_vec()));

    drop(client);
    drop(second);
    server.shutdown();
}

#[test]
fn hello_handshake() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(dir.path(), test_config());
    let mut client = Client::connect(&server);

    // RESP3 is refused so clients fall back to RESP2.
    match client.cmd_str(&["HELLO", "3"]) {
        Reply::Error(message) => assert!(message.starts_with("NOPROTO"), "{message}"),
        other => panic!("expected NOPROTO, got {other:?}"),
    }
    let handshake = client.cmd_str(&["HELLO"]);
    let fields = handshake.array();
    assert!(fields.len() >= 6);
    assert_eq!(fields[0], Reply::Bulk(b"server".to_vec()));

    drop(client);
    server.shutdown();
}

#[test]
fn unsupported_commands_get_a_clear_error() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(dir.path(), test_config());
    let mut client = Client::connect(&server);

    match client.cmd_str(&["LPUSH", "queue", "job"]) {
        Reply::Error(message) => {
            assert!(message.contains("unsupported command"), "{message}")
        }
        other => panic!("expected error, got {other:?}"),
    }
    match client.cmd_str(&["TOTALLYUNKNOWN"]) {
        Reply::Error(message) => assert!(message.contains("unknown command"), "{message}"),
        other => panic!("expected error, got {other:?}"),
    }

    drop(client);
    server.shutdown();
}

#[test]
fn max_keys_eviction_policies() {
    // noeviction: the 4th key is rejected.
    let dir = tempfile::tempdir().unwrap();
    let config = RespConfig {
        max_keys: Some(3),
        eviction: EvictionPolicy::NoEviction,
        ..test_config()
    };
    let server = start_server(dir.path(), config);
    let mut client = Client::connect(&server);
    for index in 0..3 {
        assert_ok(client.cmd_str(&["SET", &format!("k{index}"), "v"]));
    }
    match client.cmd_str(&["SET", "k3", "v"]) {
        Reply::Error(message) => assert!(message.starts_with("OOM"), "{message}"),
        other => panic!("expected OOM, got {other:?}"),
    }
    // Overwriting an existing key is still allowed at the cap.
    assert_ok(client.cmd_str(&["SET", "k0", "v2"]));
    drop(client);
    server.shutdown();

    // allkeys-random: the 4th key evicts one of the others.
    let dir = tempfile::tempdir().unwrap();
    let config = RespConfig {
        max_keys: Some(3),
        eviction: EvictionPolicy::AllKeysRandom,
        ..test_config()
    };
    let server = start_server(dir.path(), config);
    let mut client = Client::connect(&server);
    for index in 0..4 {
        assert_ok(client.cmd_str(&["SET", &format!("k{index}"), "v"]));
    }
    assert_eq!(client.cmd_str(&["GET", "k3"]), Reply::Bulk(b"v".to_vec()));
    assert_eq!(client.cmd_str(&["DBSIZE"]).int(), 3);
    drop(client);
    server.shutdown();
}

#[test]
fn ephemeral_mode_full_semantics_but_cold_restart() {
    let dir = tempfile::tempdir().unwrap();
    let config = RespConfig {
        ephemeral: true,
        max_keys: Some(3),
        eviction: EvictionPolicy::AllKeysRandom,
        ..test_config()
    };
    let server = start_server(dir.path(), config.clone());
    let mut client = Client::connect(&server);

    // Same semantics as durable mode: conditions, counters, TTLs, eviction.
    assert_ok(client.cmd_str(&["SET", "k", "v1", "EX", "100"]));
    assert_eq!(client.cmd_str(&["SET", "k", "x", "NX"]), Reply::Nil);
    assert!(client.cmd_str(&["TTL", "k"]).int() > 90);
    assert_ok(client.cmd_str(&["SET", "k", "v2", "KEEPTTL"]));
    assert!(client.cmd_str(&["TTL", "k"]).int() > 90);
    assert_eq!(client.cmd_str(&["INCR", "n"]).int(), 1);
    assert_eq!(client.cmd_str(&["APPEND", "log", "ab"]).int(), 2);
    assert_ok(client.cmd_str(&["SET", "short", "gone", "PX", "60"]));
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(client.cmd_str(&["GET", "short"]), Reply::Nil);
    // Sweeper reclaimed the expired key; eviction keeps us at the cap.
    assert_ok(client.cmd_str(&["SET", "extra", "v"]));
    assert!(client.cmd_str(&["DBSIZE"]).int() <= 3);

    drop(client);
    server.shutdown();

    // Restart: ephemeral entries are gone by design.
    let server = start_server(dir.path(), config);
    let mut client = Client::connect(&server);
    assert_eq!(client.cmd_str(&["GET", "k"]), Reply::Nil);
    assert_eq!(client.cmd_str(&["DBSIZE"]).int(), 0);
    drop(client);
    server.shutdown();
}

#[test]
fn pipelined_commands_in_one_write() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(dir.path(), test_config());
    let mut client = Client::connect(&server);

    let pipeline = b"*3\r\n$3\r\nSET\r\n$1\r\np\r\n$1\r\n1\r\n*2\r\n$3\r\nGET\r\n$1\r\np\r\n*1\r\n$4\r\nPING\r\n";
    client.writer.write_all(pipeline).unwrap();
    assert_eq!(client.read_reply(), Reply::Simple("OK".into()));
    assert_eq!(client.read_reply(), Reply::Bulk(b"1".to_vec()));
    assert_eq!(client.read_reply(), Reply::Simple("PONG".into()));

    drop(client);
    server.shutdown();
}
