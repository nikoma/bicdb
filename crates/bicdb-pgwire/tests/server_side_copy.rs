//! Server-side COPY to and from files and programs: the SQL role gate, the
//! operator gates (off by default, PROGRAM separately), path confinement, and
//! the formats round-tripping through files.
use bicdb_pgwire::{PgWireConfig, PgWireServer, ServerCopyConfig};
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

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

/// A scratch tree: `allowed/` (the operator's directory) and `outside/`.
fn tree() -> (tempfile::TempDir, PathBuf, PathBuf) {
    let base = tempfile::tempdir().unwrap();
    let allowed = base.path().join("allowed");
    let outside = base.path().join("outside");
    fs::create_dir_all(&allowed).unwrap();
    fs::create_dir_all(&outside).unwrap();
    (base, allowed, outside)
}

fn quoted(path: &Path) -> String {
    format!("'{}'", path.display().to_string().replace('\'', "''"))
}

#[test]
fn server_side_copy_is_off_by_default() {
    let (_base, allowed, _) = tree();
    fs::write(allowed.join("d.txt"), "1\n").unwrap();
    let served = Served::start(ServerCopyConfig::default());
    let (mut client, _) = served.connect();
    query(&mut client, "CREATE TABLE t(a int)").ok();
    for sql in [
        format!("COPY t FROM {}", quoted(&allowed.join("d.txt"))),
        format!("COPY t TO {}", quoted(&allowed.join("o.txt"))),
        "COPY t FROM PROGRAM 'echo 1'".to_string(),
        "COPY t TO PROGRAM 'cat'".to_string(),
    ] {
        let outcome = query(&mut client, &sql);
        assert_eq!(outcome.sqlstate(), "0A000", "{sql}: {:?}", outcome.error);
        assert!(
            outcome
                .field('M')
                .contains("is not allowed in this environment"),
            "{:?}",
            outcome.error
        );
    }
    assert!(!allowed.join("o.txt").exists());
    // The session is usable afterwards and COPY STDOUT is unaffected.
    assert_eq!(
        query(&mut client, "SELECT count(*) FROM t").rows,
        vec![vec![Some("0".to_string())]]
    );
}

#[test]
fn copy_program_is_gated_separately_from_files() {
    let (_base, allowed, _) = tree();
    fs::write(allowed.join("d.txt"), "1\n").unwrap();
    // Files enabled: PROGRAM stays off.
    {
        let served = Served::start(ServerCopyConfig::new(&[allowed.clone()], false).unwrap());
        let (mut client, _) = served.connect();
        query(&mut client, "CREATE TABLE t(a int)").ok();
        let outcome = query(
            &mut client,
            &format!("COPY t FROM {}", quoted(&allowed.join("d.txt"))),
        );
        assert_eq!(outcome.ok().tags, vec!["COPY 1"]);
        let outcome = query(&mut client, "COPY t FROM PROGRAM 'echo 2'");
        assert_eq!(outcome.sqlstate(), "0A000", "{:?}", outcome.error);
        assert!(outcome.field('M').contains("external program"));
    }
    // PROGRAM enabled: files stay off.
    {
        let served = Served::start(ServerCopyConfig::new(&[], true).unwrap());
        let (mut client, _) = served.connect();
        query(&mut client, "CREATE TABLE t(a int)").ok();
        let outcome = query(&mut client, "COPY t FROM PROGRAM 'echo 2'");
        assert_eq!(outcome.ok().tags, vec!["COPY 1"]);
        let outcome = query(
            &mut client,
            &format!("COPY t FROM {}", quoted(&allowed.join("d.txt"))),
        );
        assert_eq!(outcome.sqlstate(), "0A000", "{:?}", outcome.error);
    }
}

#[test]
fn server_side_copy_requires_the_predefined_roles() {
    let (_base, allowed, _) = tree();
    fs::write(allowed.join("d.txt"), "1\n").unwrap();
    let served = Served::start(ServerCopyConfig::new(&[allowed.clone()], true).unwrap());
    let (mut client, _) = served.connect();
    query(&mut client, "CREATE TABLE t(a int)").ok();
    query(&mut client, "CREATE ROLE copier").ok();
    query(&mut client, "GRANT ALL ON t TO copier").ok();
    query(&mut client, "SET ROLE copier").ok();
    let from = format!("COPY t FROM {}", quoted(&allowed.join("d.txt")));
    let to = format!("COPY t TO {}", quoted(&allowed.join("o.txt")));
    let outcome = query(&mut client, &from);
    assert_eq!(outcome.sqlstate(), "42501");
    assert_eq!(outcome.field('M'), "permission denied to COPY from a file");
    assert_eq!(
        outcome.field('D'),
        "Only roles with privileges of the \"pg_read_server_files\" role may COPY from a file."
    );
    assert!(outcome.field('H').starts_with("Anyone can COPY to stdout"));
    let outcome = query(&mut client, &to);
    assert_eq!(outcome.field('M'), "permission denied to COPY to a file");
    let outcome = query(&mut client, "COPY t FROM PROGRAM 'echo 1'");
    assert_eq!(
        outcome.field('M'),
        "permission denied to COPY to or from an external program"
    );
    query(&mut client, "RESET ROLE").ok();

    // Each predefined role grants exactly its own direction.
    query(&mut client, "GRANT pg_read_server_files TO copier").ok();
    query(&mut client, "SET ROLE copier").ok();
    assert_eq!(query(&mut client, &from).ok().tags, vec!["COPY 1"]);
    assert_eq!(query(&mut client, &to).sqlstate(), "42501");
    assert_eq!(
        query(&mut client, "COPY t FROM PROGRAM 'echo 1'").sqlstate(),
        "42501"
    );
    query(&mut client, "RESET ROLE").ok();
    query(&mut client, "GRANT pg_write_server_files TO copier").ok();
    query(&mut client, "SET ROLE copier").ok();
    assert_eq!(query(&mut client, &to).ok().tags, vec!["COPY 1"]);
    assert_eq!(
        query(&mut client, "COPY t TO PROGRAM 'cat >/dev/null'").sqlstate(),
        "42501"
    );
    query(&mut client, "RESET ROLE").ok();
    query(&mut client, "GRANT pg_execute_server_program TO copier").ok();
    query(&mut client, "SET ROLE copier").ok();
    assert_eq!(
        query(&mut client, "COPY t FROM PROGRAM 'echo 7'").ok().tags,
        vec!["COPY 1"]
    );
    // SET ROLE drops a superuser's privilege too.
    query(&mut client, "RESET ROLE").ok();
    query(&mut client, "CREATE ROLE plain").ok();
    query(&mut client, "GRANT ALL ON t TO plain").ok();
    query(&mut client, "SET ROLE plain").ok();
    assert_eq!(query(&mut client, &from).sqlstate(), "42501");
}

#[test]
fn server_side_copy_refuses_escapes() {
    let (_base, allowed, outside) = tree();
    fs::write(outside.join("secret"), "42\n").unwrap();
    fs::create_dir_all(allowed.join("sub")).unwrap();
    std::os::unix::fs::symlink(outside.join("secret"), allowed.join("link")).unwrap();
    std::os::unix::fs::symlink(&outside, allowed.join("dirlink")).unwrap();
    std::os::unix::fs::symlink(outside.join("new"), allowed.join("dangling")).unwrap();
    let served = Served::start(ServerCopyConfig::new(&[allowed.clone()], false).unwrap());
    let (mut client, _) = served.connect();
    query(&mut client, "CREATE TABLE t(a int)").ok();
    query(&mut client, "INSERT INTO t VALUES (1)").ok();
    let allowed_text = allowed.display().to_string();
    for target in [
        format!("{allowed_text}/../outside/secret"),
        format!("{allowed_text}/sub/../../outside/secret"),
        outside.join("secret").display().to_string(),
        "/etc/passwd".to_string(),
        format!("{allowed_text}/link"),
        format!("{allowed_text}/dirlink/secret"),
        "relative/secret".to_string(),
    ] {
        let outcome = query(&mut client, &format!("COPY t FROM '{target}'"));
        assert_eq!(outcome.sqlstate(), "42501", "{target}: {:?}", outcome.error);
    }
    for target in [
        format!("{allowed_text}/../outside/secret"),
        outside.join("secret").display().to_string(),
        format!("{allowed_text}/link"),
        format!("{allowed_text}/dirlink/secret"),
        format!("{allowed_text}/dangling"),
    ] {
        let outcome = query(&mut client, &format!("COPY t TO '{target}'"));
        assert_eq!(outcome.sqlstate(), "42501", "{target}: {:?}", outcome.error);
    }
    assert_eq!(fs::read_to_string(outside.join("secret")).unwrap(), "42\n");
    assert!(!outside.join("new").exists());
    assert_eq!(
        query(&mut client, "SELECT count(*) FROM t").rows,
        vec![vec![Some("1".to_string())]]
    );
    // PostgreSQL's own path errors.
    let outcome = query(&mut client, "COPY t TO 'relative.txt'");
    assert_eq!(outcome.sqlstate(), "42602");
    assert_eq!(
        outcome.field('M'),
        "relative path not allowed for COPY to file"
    );
    let outcome = query(
        &mut client,
        &format!("COPY t FROM '{allowed_text}/missing'"),
    );
    assert_eq!(outcome.sqlstate(), "58P01");
    assert_eq!(
        outcome.field('M'),
        format!(
            "could not open file \"{allowed_text}/missing\" for reading: No such file or directory"
        )
    );
    let outcome = query(&mut client, &format!("COPY t FROM '{allowed_text}/sub'"));
    assert_eq!(outcome.sqlstate(), "42809");
    assert_eq!(
        outcome.field('M'),
        format!("\"{allowed_text}/sub\" is a directory")
    );
}

#[test]
fn server_side_copy_round_trips_text_csv_and_binary() {
    let (_base, allowed, _) = tree();
    let served = Served::start(ServerCopyConfig::new(&[allowed.clone()], false).unwrap());
    let (mut client, _) = served.connect();
    query(
        &mut client,
        "CREATE TABLE src(id int, name text, score float8, ok boolean)",
    )
    .ok();
    query(
        &mut client,
        "INSERT INTO src VALUES (1, 'a,b', 1.5, true), (2, NULL, -2, false), (3, E'tab\\there', 0, NULL)",
    )
    .ok();
    for (name, options) in [
        ("text.txt", ""),
        ("csv.csv", " WITH (FORMAT csv, HEADER)"),
        ("bin.dat", " WITH (FORMAT binary)"),
    ] {
        let path = quoted(&allowed.join(name));
        let outcome = query(&mut client, &format!("COPY src TO {path}{options}"));
        assert_eq!(outcome.ok().tags, vec!["COPY 3"], "{name}");
        query(&mut client, "DROP TABLE IF EXISTS dst").ok();
        query(
            &mut client,
            "CREATE TABLE dst(id int, name text, score float8, ok boolean)",
        )
        .ok();
        let outcome = query(&mut client, &format!("COPY dst FROM {path}{options}"));
        assert_eq!(outcome.ok().tags, vec!["COPY 3"], "{name}");
        let diff = query(
            &mut client,
            "SELECT count(*) FROM (SELECT * FROM src EXCEPT SELECT * FROM dst) d",
        );
        assert_eq!(diff.rows, vec![vec![Some("0".to_string())]], "{name}");
        let count = query(&mut client, "SELECT count(*) FROM dst");
        assert_eq!(count.rows, vec![vec![Some("3".to_string())]], "{name}");
    }
    assert_eq!(
        fs::read_to_string(allowed.join("csv.csv")).unwrap(),
        "id,name,score,ok\n1,\"a,b\",1.5,t\n2,,-2,f\n3,tab\there,0,\n"
    );
    assert!(fs::read(allowed.join("bin.dat"))
        .unwrap()
        .starts_with(b"PGCOPY\n\xff\r\n\0"));
    // A query, a column list, and COPY inside a transaction that rolls back.
    let path = quoted(&allowed.join("q.txt"));
    query(
        &mut client,
        &format!("COPY (SELECT id, name FROM src WHERE id < 3 ORDER BY id) TO {path}"),
    )
    .ok();
    assert_eq!(
        fs::read_to_string(allowed.join("q.txt")).unwrap(),
        "1\ta,b\n2\t\\N\n"
    );
    query(&mut client, "TRUNCATE dst").ok();
    query(&mut client, "BEGIN").ok();
    query(&mut client, &format!("COPY dst(id, name) FROM {path}")).ok();
    query(&mut client, "ROLLBACK").ok();
    assert_eq!(
        query(&mut client, "SELECT count(*) FROM dst").rows,
        vec![vec![Some("0".to_string())]]
    );
    // Several statements in one message, a server COPY in the middle.
    let outcome = query(
        &mut client,
        &format!("COPY dst(id, name) FROM {path}; SELECT count(*) FROM dst"),
    );
    assert_eq!(outcome.ok().rows, vec![vec![Some("2".to_string())]]);
    // A bad row reports PostgreSQL's error and loads nothing.
    fs::write(allowed.join("bad.txt"), "1\tx\nnot-a-number\ty\n").unwrap();
    query(&mut client, "TRUNCATE dst").ok();
    let outcome = query(
        &mut client,
        &format!(
            "COPY dst(id, name) FROM {}",
            quoted(&allowed.join("bad.txt"))
        ),
    );
    assert_eq!(outcome.sqlstate(), "22P02", "{:?}", outcome.error);
    assert_eq!(
        query(&mut client, "SELECT count(*) FROM dst").rows,
        vec![vec![Some("0".to_string())]]
    );
}

#[test]
fn copy_program_runs_a_shell_command_and_reports_its_exit() {
    let (_base, allowed, _) = tree();
    let served = Served::start(ServerCopyConfig::new(&[], true).unwrap());
    let (mut client, _) = served.connect();
    query(&mut client, "CREATE TABLE t(a int, b text)").ok();
    let outcome = query(
        &mut client,
        "COPY t FROM PROGRAM 'printf \"1\\tone\\n2\\ttwo\\n\"'",
    );
    assert_eq!(outcome.ok().tags, vec!["COPY 2"]);
    let out = allowed.join("out.csv");
    let outcome = query(
        &mut client,
        &format!(
            "COPY t TO PROGRAM {} WITH (FORMAT csv)",
            quoted(Path::new(&format!("cat > {}", out.display())))
        ),
    );
    assert_eq!(outcome.ok().tags, vec!["COPY 2"]);
    assert_eq!(fs::read_to_string(&out).unwrap(), "1,one\n2,two\n");

    let outcome = query(&mut client, "COPY t FROM PROGRAM 'exit 3'");
    assert_eq!(outcome.sqlstate(), "38000");
    assert_eq!(outcome.field('M'), "program \"exit 3\" failed");
    assert_eq!(outcome.field('D'), "child process exited with exit code 3");
    let outcome = query(&mut client, "COPY t TO PROGRAM 'cat >/dev/null; exit 4'");
    assert_eq!(
        outcome.field('M'),
        "program \"cat >/dev/null; exit 4\" failed"
    );
    assert_eq!(outcome.field('D'), "child process exited with exit code 4");
    let outcome = query(&mut client, "COPY t FROM PROGRAM 'no-such-command-bicdb'");
    assert_eq!(outcome.field('D'), "command not found");
    // Rows read before a failing exit are not kept.
    let outcome = query(
        &mut client,
        "COPY t FROM PROGRAM 'printf \"9\\tx\\n\"; exit 1'",
    );
    assert_eq!(outcome.sqlstate(), "38000");
    assert_eq!(
        query(&mut client, "SELECT count(*) FROM t").rows,
        vec![vec![Some("2".to_string())]]
    );
    // The child does not inherit the server's environment.
    let outcome = query(
        &mut client,
        "COPY t(b) FROM PROGRAM 'env | grep -v -e ^PATH= -e ^PWD= -e ^SHLVL= -e ^_= | wc -l | tr -d \" \"'",
    );
    assert_eq!(outcome.ok().tags, vec!["COPY 1"]);
    assert_eq!(
        query(&mut client, "SELECT b FROM t WHERE a IS NULL").rows,
        vec![vec![Some("0".to_string())]]
    );
}

#[test]
fn copy_program_is_cancelled_with_its_query() {
    let served = Served::start(ServerCopyConfig::new(&[], true).unwrap());
    let (mut client, key) = served.connect();
    query(&mut client, "CREATE TABLE t(a int)").ok();
    let started = Instant::now();
    let server = served.server.clone();
    let canceller = thread::spawn(move || {
        thread::sleep(Duration::from_millis(500));
        server.request_cancel_for_test(key.0, key.1);
    });
    // `sleep` holds the pipe open; only killing its process group ends it.
    let outcome = query(&mut client, "COPY t FROM PROGRAM 'sleep 60; echo 1'");
    canceller.join().unwrap();
    assert_eq!(outcome.sqlstate(), "57014", "{:?}", outcome.error);
    assert!(started.elapsed() < Duration::from_secs(20));
    // A program that never reads its input nor exits: the TO side.
    let server = served.server.clone();
    let canceller = thread::spawn(move || {
        thread::sleep(Duration::from_millis(500));
        server.request_cancel_for_test(key.0, key.1);
    });
    let started = Instant::now();
    let outcome = query(&mut client, "COPY t TO PROGRAM 'sleep 60'");
    canceller.join().unwrap();
    assert_eq!(outcome.sqlstate(), "57014", "{:?}", outcome.error);
    assert!(started.elapsed() < Duration::from_secs(20));
    assert_eq!(
        query(&mut client, "SELECT count(*) FROM t").rows,
        vec![vec![Some("0".to_string())]]
    );
}

#[test]
fn routines_cannot_reach_server_side_copy() {
    let (_base, allowed, _) = tree();
    fs::write(allowed.join("d.txt"), "1\n").unwrap();
    let served = Served::start(ServerCopyConfig::new(&[allowed.clone()], true).unwrap());
    let (mut client, _) = served.connect();
    query(&mut client, "CREATE TABLE t(a int)").ok();
    let path = allowed.join("d.txt").display().to_string();
    let outcome = query(
        &mut client,
        &format!(
            "CREATE FUNCTION load_it() RETURNS void LANGUAGE plpgsql AS $$ \
             BEGIN EXECUTE 'COPY t FROM ''{path}'''; END $$"
        ),
    );
    outcome.ok();
    assert!(query(&mut client, "SELECT load_it()").error.is_some());
    assert!(query(
        &mut client,
        "DO $$ BEGIN EXECUTE 'COPY t FROM PROGRAM ''echo 5'''; END $$"
    )
    .error
    .is_some());
    assert_eq!(
        query(&mut client, "SELECT count(*) FROM t").rows,
        vec![vec![Some("0".to_string())]]
    );
}

#[test]
fn csv_quoted_fields_may_span_lines_from_files_and_stdin() {
    let (_base, allowed, _) = tree();
    fs::write(
        allowed.join("nl.csv"),
        "1,\"a\nb\"\n2,\"x\"\"y\r\nz\"\n3,plain\n",
    )
    .unwrap();
    let served = Served::start(ServerCopyConfig::new(&[allowed.clone()], false).unwrap());
    let (mut client, _) = served.connect();
    query(&mut client, "CREATE TABLE t(a int, b text)").ok();
    let outcome = query(
        &mut client,
        &format!(
            "COPY t FROM {} (FORMAT csv)",
            quoted(&allowed.join("nl.csv"))
        ),
    );
    assert_eq!(outcome.ok().tags, vec!["COPY 3"]);
    assert_eq!(
        query(&mut client, "SELECT a, b FROM t ORDER BY a").rows,
        vec![
            vec![Some("1".into()), Some("a\nb".into())],
            vec![Some("2".into()), Some("x\"y\r\nz".into())],
            vec![Some("3".into()), Some("plain".into())],
        ]
    );
    // STDIN, with the record split across CopyData messages inside the quotes.
    query(&mut client, "TRUNCATE t").ok();
    let mut payload = b"COPY t FROM STDIN (FORMAT csv)".to_vec();
    payload.push(0);
    send_message(&mut client, b'Q', &payload);
    assert_eq!(read_message(&mut client).0, b'G');
    send_message(&mut client, b'd', b"7,\"multi\nli");
    send_message(&mut client, b'd', b"ne\"\n8,\"q\"\"");
    send_message(&mut client, b'd', b"\"\n");
    send_message(&mut client, b'c', &[]);
    let messages = read_until_ready(&mut client);
    assert!(messages.iter().all(|(tag, _)| *tag != b'E'), "{messages:?}");
    assert_eq!(
        query(&mut client, "SELECT a, b FROM t ORDER BY a").rows,
        vec![
            vec![Some("7".into()), Some("multi\nline".into())],
            vec![Some("8".into()), Some("q\"".into())],
        ]
    );
    // An unterminated quote is PostgreSQL's error, at the end of the input.
    fs::write(allowed.join("unterm.csv"), "1,\"abc\n").unwrap();
    let outcome = query(
        &mut client,
        &format!(
            "COPY t FROM {} (FORMAT csv)",
            quoted(&allowed.join("unterm.csv"))
        ),
    );
    assert_eq!(outcome.sqlstate(), "22P04");
    assert_eq!(outcome.field('M'), "unterminated CSV quoted field");
    assert_eq!(outcome.field('W'), "COPY t, line 1: \"1,\"abc\n\"");
}

#[test]
fn bad_copy_rows_report_postgresql_errors_and_context() {
    let (_base, allowed, _) = tree();
    let served = Served::start(ServerCopyConfig::new(&[allowed.clone()], false).unwrap());
    let (mut client, _) = served.connect();
    query(&mut client, "CREATE TABLE qq(a int, b text)").ok();
    let mut copy_file = |name: &str, contents: &str, options: &str| {
        fs::write(allowed.join(name), contents).unwrap();
        query(
            &mut client,
            &format!("COPY qq FROM {}{options}", quoted(&allowed.join(name))),
        )
    };
    let outcome = copy_file("bad.txt", "1\tx\nzz\ty\n", "");
    assert_eq!(outcome.sqlstate(), "22P02");
    assert_eq!(
        outcome.field('M'),
        "invalid input syntax for type integer: \"zz\""
    );
    assert_eq!(outcome.field('W'), "COPY qq, line 2, column a: \"zz\"");
    let outcome = copy_file("badh.csv", "a,b\n1,x\nzz,y\n", " (FORMAT csv, HEADER)");
    assert_eq!(outcome.field('W'), "COPY qq, line 3, column a: \"zz\"");
    // A line end inside quotes counts once the input's line ends are known.
    let outcome = copy_file("nl.csv", "1,\"a\nb\"\nzz,\"c\nd\"\n", " (FORMAT csv)");
    assert_eq!(outcome.field('W'), "COPY qq, line 3, column a: \"zz\"");
    let outcome = copy_file("range.txt", "99999999999\tx\n", "");
    assert_eq!(outcome.sqlstate(), "22003");
    assert_eq!(
        outcome.field('W'),
        "COPY qq, line 1, column a: \"99999999999\""
    );
    let outcome = copy_file("miss.txt", "1\n", "");
    assert_eq!(outcome.sqlstate(), "22P04");
    assert_eq!(outcome.field('M'), "missing data for column \"b\"");
    assert_eq!(outcome.field('W'), "COPY qq, line 1: \"1\"");
    let outcome = copy_file("extra.txt", "1\tx\ty\n", "");
    assert_eq!(outcome.field('M'), "extra data after last expected column");
    assert_eq!(outcome.field('W'), "COPY qq, line 1: \"1\tx\ty\"");
    // A long value is cut at 100 bytes, as PostgreSQL prints it.
    let long = "z".repeat(150);
    let outcome = copy_file("long.txt", &format!("{long}\tx\n"), "");
    assert_eq!(
        outcome.field('W'),
        format!("COPY qq, line 1, column a: \"{}...\"", "z".repeat(100))
    );
    assert_eq!(
        query(&mut client, "SELECT count(*) FROM qq").rows,
        vec![vec![Some("0".to_string())]]
    );
}
