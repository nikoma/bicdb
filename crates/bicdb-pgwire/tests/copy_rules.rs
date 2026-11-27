//! COPY FROM rules: ON_ERROR IGNORE (REJECT_LIMIT, LOG_VERBOSITY), DEFAULT
//! markers, WHERE conditions and FREEZE.
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

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

fn startup(stream: &mut TcpStream) {
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
    read_until_ready(stream);
}

fn cstr(text: &str) -> Vec<u8> {
    let mut bytes = text.as_bytes().to_vec();
    bytes.push(0);
    bytes
}

fn send_query(stream: &mut TcpStream, query: &str) {
    send_message(stream, b'Q', &cstr(query));
}

fn simple(stream: &mut TcpStream, query: &str) -> Vec<(u8, Vec<u8>)> {
    send_query(stream, query);
    read_until_ready(stream)
}

fn error_field(messages: &[(u8, Vec<u8>)], code: char) -> Option<String> {
    let (_, payload) = messages.iter().find(|(tag, _)| *tag == b'E')?;
    String::from_utf8_lossy(payload)
        .split('\0')
        .find_map(|field| field.strip_prefix(code).map(str::to_string))
}

fn copy_out(messages: &[(u8, Vec<u8>)]) -> String {
    messages
        .iter()
        .filter(|(tag, _)| *tag == b'd')
        .map(|(_, payload)| String::from_utf8_lossy(payload).to_string())
        .collect()
}

fn rows(messages: &[(u8, Vec<u8>)]) -> Vec<Vec<Option<String>>> {
    messages
        .iter()
        .filter(|(tag, _)| *tag == b'D')
        .map(|(_, payload)| {
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
            row
        })
        .collect()
}

fn serve() -> (TcpStream, thread::JoinHandle<()>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });
    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    (client, server, dir)
}

fn finish(mut client: TcpStream, server: thread::JoinHandle<()>) {
    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

fn assert_alive(client: &mut TcpStream) {
    assert_eq!(
        rows(&simple(client, "SELECT 41 + 1")),
        vec![vec![Some("42".to_string())]]
    );
}

fn copy_in(client: &mut TcpStream, sql: &str, data: &[u8]) -> Vec<(u8, Vec<u8>)> {
    send_query(client, sql);
    let (tag, payload) = read_message(client);
    if tag != b'G' {
        let mut messages = vec![(tag, payload)];
        messages.extend(read_until_ready(client));
        return messages;
    }
    send_message(client, b'd', data);
    send_message(client, b'c', &[]);
    read_until_ready(client)
}

fn copy_bytes(messages: &[(u8, Vec<u8>)]) -> Vec<u8> {
    messages
        .iter()
        .filter(|(tag, _)| *tag == b'd')
        .flat_map(|(_, payload)| payload.clone())
        .collect()
}

fn notices(messages: &[(u8, Vec<u8>)]) -> Vec<String> {
    messages
        .iter()
        .filter(|(tag, _)| *tag == b'N')
        .filter_map(|(_, payload)| {
            String::from_utf8_lossy(payload)
                .split('\0')
                .find_map(|field| field.strip_prefix('M').map(str::to_string))
        })
        .collect()
}

fn command_tag(messages: &[(u8, Vec<u8>)]) -> Option<String> {
    messages
        .iter()
        .find(|(tag, _)| *tag == b'C')
        .map(|(_, payload)| String::from_utf8_lossy(&payload[..payload.len() - 1]).to_string())
}

const BAD_ROWS: &[u8] = b"1\t{1}\t1\na\t{2}\t2\n3\t{3}\t3333333333\n4\t{a, 4}\t4\n\t{5}\t5\n6\t{6}\t6\n";

#[test]
fn on_error_ignore_skips_rows_with_bad_values() {
    let (mut client, server, _dir) = serve();
    simple(&mut client, "CREATE TABLE e(n int, m int[], k int)");
    let messages = copy_in(
        &mut client,
        "COPY e FROM STDIN WITH (on_error ignore, log_verbosity verbose)",
        BAD_ROWS,
    );
    assert!(error_field(&messages, 'M').is_none(), "{messages:?}");
    assert_eq!(
        notices(&messages),
        vec![
            "skipping row due to data type incompatibility at line 2 for column \"n\": \"a\"",
            "skipping row due to data type incompatibility at line 3 for column \"k\": \"3333333333\"",
            "skipping row due to data type incompatibility at line 4 for column \"m\": \"{a, 4}\"",
            "skipping row due to data type incompatibility at line 5 for column \"n\": \"\"",
            "4 rows were skipped due to data type incompatibility",
        ]
    );
    assert_eq!(command_tag(&messages).as_deref(), Some("COPY 2"));
    assert_eq!(
        rows(&simple(&mut client, "SELECT n, k FROM e ORDER BY n")),
        vec![
            vec![Some("1".into()), Some("1".into())],
            vec![Some("6".into()), Some("6".into())],
        ]
    );
    // The default verbosity reports only the count; silent reports nothing.
    let messages = copy_in(&mut client, "COPY e FROM STDIN (on_error ignore)", BAD_ROWS);
    assert_eq!(
        notices(&messages),
        vec!["4 rows were skipped due to data type incompatibility"]
    );
    let messages = copy_in(
        &mut client,
        "COPY e FROM STDIN (on_error ignore, log_verbosity silent)",
        BAD_ROWS,
    );
    assert!(notices(&messages).is_empty());
    let messages = copy_in(
        &mut client,
        "COPY e FROM STDIN (on_error ignore)",
        b"x\t{1}\t1\n",
    );
    assert_eq!(
        notices(&messages),
        vec!["1 row was skipped due to data type incompatibility"]
    );
    // REJECT_LIMIT: more skipped rows than allowed fail the COPY.
    simple(&mut client, "TRUNCATE e");
    let messages = copy_in(
        &mut client,
        "COPY e FROM STDIN (on_error ignore, reject_limit 3)",
        BAD_ROWS,
    );
    assert_eq!(error_field(&messages, 'C').as_deref(), Some("22P02"));
    assert_eq!(
        error_field(&messages, 'M').as_deref(),
        Some("skipped more than REJECT_LIMIT (3) rows due to data type incompatibility")
    );
    let messages = copy_in(
        &mut client,
        "COPY e FROM STDIN (on_error ignore, reject_limit 4)",
        BAD_ROWS,
    );
    assert!(error_field(&messages, 'M').is_none(), "{messages:?}");
    // Missing data and constraint violations are not data type errors.
    let messages = copy_in(
        &mut client,
        "COPY e FROM STDIN (on_error ignore)",
        b"1\t{1}\n",
    );
    assert_eq!(
        error_field(&messages, 'M').as_deref(),
        Some("missing data for column \"k\"")
    );
    simple(&mut client, "CREATE TABLE c(n int CHECK (n > 0))");
    let messages = copy_in(
        &mut client,
        "COPY c FROM STDIN (on_error ignore)",
        b"x\n-1\n",
    );
    assert_eq!(error_field(&messages, 'C').as_deref(), Some("23514"));
    assert_eq!(
        rows(&simple(&mut client, "SELECT count(*) FROM c")),
        vec![vec![Some("0".into())]]
    );
    finish(client, server);
}

#[test]
fn default_marker_takes_the_column_default() {
    let (mut client, server, _dir) = serve();
    simple(
        &mut client,
        "CREATE TABLE d(id int, t text DEFAULT 'dflt', n serial, s text)",
    );
    let messages = copy_in(
        &mut client,
        "COPY d FROM STDIN (DEFAULT '\\D')",
        b"1\t\\D\t\\D\tx\n2\tgiven\t50\t\\D2\n",
    );
    assert!(error_field(&messages, 'M').is_none(), "{messages:?}");
    assert_eq!(
        rows(&simple(
            &mut client,
            "SELECT id, t, n, s FROM d ORDER BY id"
        )),
        vec![
            vec![
                Some("1".into()),
                Some("dflt".into()),
                Some("1".into()),
                Some("x".into())
            ],
            vec![
                Some("2".into()),
                Some("given".into()),
                Some("50".into()),
                Some("D2".into())
            ],
        ]
    );
    let messages = copy_in(
        &mut client,
        "COPY d FROM STDIN (FORMAT csv, DEFAULT 'DEF')",
        b"3,DEF,DEF,\"DEF\"\n",
    );
    assert!(error_field(&messages, 'M').is_none(), "{messages:?}");
    assert_eq!(
        rows(&simple(&mut client, "SELECT t, n, s FROM d WHERE id = 3")),
        vec![vec![
            Some("dflt".into()),
            Some("2".into()),
            Some("DEF".into())
        ]]
    );
    // A column without a default.
    let messages = copy_in(
        &mut client,
        "COPY d FROM STDIN (DEFAULT '\\D')",
        b"\\D\tx\t1\ty\n",
    );
    assert_eq!(error_field(&messages, 'C').as_deref(), Some("22P04"));
    assert_eq!(
        error_field(&messages, 'M').as_deref(),
        Some("unexpected default marker in COPY data")
    );
    assert_eq!(
        error_field(&messages, 'D').as_deref(),
        Some("Column \"id\" has no default value.")
    );
    assert_eq!(
        error_field(&messages, 'W').as_deref(),
        Some("COPY d, line 1: \"\\D\tx\t1\ty\"")
    );
    finish(client, server);
}

#[test]
fn copy_from_where_filters_rows() {
    let (mut client, server, _dir) = serve();
    simple(&mut client, "CREATE TABLE w(a int, b text)");
    let messages = copy_in(
        &mut client,
        "COPY w FROM STDIN WHERE a % 2 = 1 AND b <> 'skip'",
        b"1\tx\n2\ty\n3\tskip\n5\tz\n",
    );
    assert!(error_field(&messages, 'M').is_none(), "{messages:?}");
    assert_eq!(command_tag(&messages).as_deref(), Some("COPY 2"));
    assert_eq!(
        rows(&simple(&mut client, "SELECT a FROM w ORDER BY a")),
        vec![vec![Some("1".into())], vec![Some("5".into())]]
    );
    for (sql, state) in [
        ("COPY w FROM STDIN WHERE a IN (SELECT 1)", "0A000"),
        ("COPY w FROM STDIN WHERE a = row_number() OVER ()", "42P20"),
        ("COPY w FROM STDIN WHERE max(a) > 1", "42803"),
        ("COPY w FROM STDIN WHERE generate_series(1, a) > 1", "0A000"),
    ] {
        let messages = simple(&mut client, sql);
        assert!(messages.iter().all(|(tag, _)| *tag != b'G'), "{sql}");
        assert_eq!(error_field(&messages, 'C').as_deref(), Some(state), "{sql}");
    }
    finish(client, server);
}

#[test]
fn copy_freeze_needs_a_table_fresh_in_this_subtransaction() {
    let (mut client, server, _dir) = serve();
    simple(&mut client, "CREATE TABLE f(a text)");
    let refused = |messages: &[(u8, Vec<u8>)]| {
        assert!(messages.iter().all(|(tag, _)| *tag != b'G'));
        assert_eq!(error_field(messages, 'C').as_deref(), Some("55000"));
        assert_eq!(
            error_field(messages, 'M').as_deref(),
            Some(
                "cannot perform COPY FREEZE because the table was not created or truncated \
                 in the current subtransaction"
            )
        );
    };
    refused(&simple(&mut client, "COPY f FROM STDIN (FREEZE)"));
    simple(&mut client, "BEGIN");
    simple(&mut client, "TRUNCATE f");
    let messages = copy_in(&mut client, "COPY f FROM STDIN CSV FREEZE", b"x\ny\n");
    assert!(error_field(&messages, 'M').is_none(), "{messages:?}");
    simple(&mut client, "SAVEPOINT s1");
    refused(&simple(&mut client, "COPY f FROM STDIN (FREEZE)"));
    simple(&mut client, "ROLLBACK");
    simple(&mut client, "BEGIN");
    simple(&mut client, "SAVEPOINT s1");
    simple(&mut client, "TRUNCATE f");
    simple(&mut client, "ROLLBACK TO SAVEPOINT s1");
    refused(&simple(&mut client, "COPY f FROM STDIN (FREEZE)"));
    simple(&mut client, "ROLLBACK");
    simple(&mut client, "BEGIN");
    simple(&mut client, "CREATE TABLE g(a int)");
    let messages = copy_in(&mut client, "COPY g FROM STDIN (FREEZE)", b"1\n");
    assert!(error_field(&messages, 'M').is_none(), "{messages:?}");
    simple(&mut client, "COMMIT");
    assert_eq!(
        rows(&simple(&mut client, "SELECT count(*) FROM g")),
        vec![vec![Some("1".into())]]
    );
    simple(&mut client, "CREATE TABLE p(a int) PARTITION BY RANGE (a)");
    simple(&mut client, "BEGIN");
    let messages = simple(&mut client, "COPY p FROM STDIN (FREEZE)");
    assert_eq!(
        error_field(&messages, 'M').as_deref(),
        Some("cannot perform COPY FREEZE on a partitioned table")
    );
    simple(&mut client, "ROLLBACK");
    finish(client, server);
}
