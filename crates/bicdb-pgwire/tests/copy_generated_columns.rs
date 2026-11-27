//! COPY and generated columns (stored and virtual), and the session state
//! after a COPY is refused or fails mid-stream.
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

const TABLE: &str = "CREATE TABLE g(a int, b int GENERATED ALWAYS AS (a * 2) STORED, \
                     c int GENERATED ALWAYS AS (a * 3) VIRTUAL, d text)";

fn assert_alive(client: &mut TcpStream) {
    assert_eq!(
        rows(&simple(client, "SELECT 41 + 1")),
        vec![vec![Some("42".to_string())]]
    );
}

#[test]
fn copy_from_leaves_generated_columns_to_be_computed() {
    let (mut client, server, _dir) = serve();
    simple(&mut client, TABLE);
    send_query(&mut client, "COPY g FROM STDIN");
    assert_eq!(read_message(&mut client).0, b'G');
    send_message(&mut client, b'd', b"1\tx\n2\ty\n");
    send_message(&mut client, b'c', &[]);
    let messages = read_until_ready(&mut client);
    assert!(error_field(&messages, 'M').is_none(), "{messages:?}");
    assert_eq!(
        rows(&simple(&mut client, "SELECT a, b, c, d FROM g ORDER BY a")),
        vec![
            vec![
                Some("1".into()),
                Some("2".into()),
                Some("3".into()),
                Some("x".into())
            ],
            vec![
                Some("2".into()),
                Some("4".into()),
                Some("6".into()),
                Some("y".into())
            ],
        ]
    );
    // HEADER MATCH checks the default list, which has no generated column.
    send_query(&mut client, "COPY g FROM STDIN (FORMAT csv, HEADER MATCH)");
    assert_eq!(read_message(&mut client).0, b'G');
    send_message(&mut client, b'd', b"a,d\n3,z\n");
    send_message(&mut client, b'c', &[]);
    let messages = read_until_ready(&mut client);
    assert!(error_field(&messages, 'M').is_none(), "{messages:?}");
    // COPY TO leaves stored and virtual generated columns out.
    assert_eq!(
        copy_out(&simple(&mut client, "COPY g TO STDOUT")),
        "1\tx\n2\ty\n3\tz\n"
    );
    assert_eq!(
        copy_out(&simple(&mut client, "COPY g TO STDOUT (HEADER)")),
        "a\td\n1\tx\n2\ty\n3\tz\n"
    );
    // Binary in both directions uses the same list.
    let messages = simple(&mut client, "COPY g TO STDOUT (FORMAT binary)");
    let data: Vec<u8> = messages
        .iter()
        .filter(|(tag, _)| *tag == b'd')
        .flat_map(|(_, payload)| payload.clone())
        .collect();
    simple(&mut client, "TRUNCATE g");
    send_query(&mut client, "COPY g FROM STDIN (FORMAT binary)");
    assert_eq!(read_message(&mut client).0, b'G');
    send_message(&mut client, b'd', &data);
    send_message(&mut client, b'c', &[]);
    let messages = read_until_ready(&mut client);
    assert!(error_field(&messages, 'M').is_none(), "{messages:?}");
    assert_eq!(
        rows(&simple(
            &mut client,
            "SELECT count(*), sum(b), sum(c) FROM g"
        )),
        vec![vec![Some("3".into()), Some("12".into()), Some("18".into())]]
    );
    finish(client, server);
}

#[test]
fn a_named_generated_column_is_refused_before_copy_mode() {
    let (mut client, server, _dir) = serve();
    simple(&mut client, TABLE);
    for sql in [
        "COPY g (a, b) FROM STDIN",
        "COPY g (c) FROM STDIN",
        "COPY g (a, b) TO STDOUT",
        "COPY g (c) TO STDOUT",
    ] {
        let messages = simple(&mut client, sql);
        assert!(
            messages.iter().all(|(tag, _)| *tag != b'G' && *tag != b'H'),
            "{sql} entered COPY mode: {messages:?}"
        );
        assert_eq!(
            error_field(&messages, 'C').as_deref(),
            Some("42P10"),
            "{sql}"
        );
        let column = if sql.contains("(c)") { "c" } else { "b" };
        assert_eq!(
            error_field(&messages, 'M'),
            Some(format!("column \"{column}\" is a generated column"))
        );
        assert_eq!(
            error_field(&messages, 'D').as_deref(),
            Some("Generated columns cannot be used in COPY.")
        );
        assert_alive(&mut client);
    }
    finish(client, server);
}

#[test]
fn copy_from_where_is_refused_before_copy_mode() {
    let (mut client, server, _dir) = serve();
    simple(&mut client, TABLE);
    for (sql, state, message) in [
        (
            "COPY g FROM STDIN WHERE b <> 10",
            "42P10",
            "generated columns are not supported in COPY FROM WHERE conditions",
        ),
        (
            "COPY g FROM STDIN WHERE g IS NULL",
            "42P10",
            "generated columns are not supported in COPY FROM WHERE conditions",
        ),
        (
            "COPY g FROM STDIN WHERE a > 1",
            "0A000",
            "COPY FROM with a WHERE condition is not supported",
        ),
        (
            "COPY g TO STDOUT WHERE a > 1",
            "42601",
            "WHERE clause not allowed with COPY TO",
        ),
    ] {
        let messages = simple(&mut client, sql);
        assert!(messages.iter().all(|(tag, _)| *tag != b'G'), "{sql}");
        assert_eq!(error_field(&messages, 'C').as_deref(), Some(state), "{sql}");
        assert_eq!(
            error_field(&messages, 'M').as_deref(),
            Some(message),
            "{sql}"
        );
        if state == "42P10" {
            assert_eq!(
                error_field(&messages, 'D').as_deref(),
                Some("Column \"b\" is a generated column.")
            );
        }
        assert_alive(&mut client);
    }
    finish(client, server);
}

#[test]
fn the_session_keeps_working_after_copy_data_fails() {
    let (mut client, server, _dir) = serve();
    simple(&mut client, TABLE);
    // A bad row in the middle of the stream: one error, one ReadyForQuery
    // after the client's CopyDone, and the rest of the data is dropped.
    send_query(&mut client, "COPY g FROM STDIN");
    assert_eq!(read_message(&mut client).0, b'G');
    send_message(&mut client, b'd', b"1\tx\textra\n");
    send_message(&mut client, b'd', b"2\ty\n");
    send_message(&mut client, b'c', &[]);
    let messages = read_until_ready(&mut client);
    assert_eq!(messages.iter().filter(|(tag, _)| *tag == b'E').count(), 1);
    assert_eq!(error_field(&messages, 'C').as_deref(), Some("22P04"));
    assert_alive(&mut client);
    // CopyFail from the client.
    send_query(&mut client, "COPY g FROM STDIN");
    assert_eq!(read_message(&mut client).0, b'G');
    send_message(&mut client, b'd', b"3\tz\n");
    send_message(&mut client, b'f', &cstr("client gave up"));
    let messages = read_until_ready(&mut client);
    assert_eq!(messages.iter().filter(|(tag, _)| *tag == b'E').count(), 1);
    assert_alive(&mut client);
    assert_eq!(
        rows(&simple(&mut client, "SELECT count(*) FROM g")),
        vec![vec![Some("0".to_string())]]
    );

    // Extended protocol: a refused COPY never enters COPY mode, and after a
    // mid-stream failure the server waits for Sync.
    let extended = |client: &mut TcpStream, sql: &str| {
        let mut parse = cstr("");
        parse.extend(cstr(sql));
        parse.extend(0_i16.to_be_bytes());
        send_message(client, b'P', &parse);
        let mut bind = cstr("");
        bind.extend(cstr(""));
        bind.extend(0_i16.to_be_bytes());
        bind.extend(0_i16.to_be_bytes());
        bind.extend(0_i16.to_be_bytes());
        send_message(client, b'B', &bind);
        let mut execute = cstr("");
        execute.extend(0_i32.to_be_bytes());
        send_message(client, b'E', &execute);
    };
    extended(&mut client, "COPY g (a, b) FROM STDIN");
    send_message(&mut client, b'S', &[]);
    let messages = read_until_ready(&mut client);
    assert!(messages.iter().all(|(tag, _)| *tag != b'G'), "{messages:?}");
    assert_eq!(error_field(&messages, 'C').as_deref(), Some("42P10"));
    assert_alive(&mut client);

    extended(&mut client, "COPY g FROM STDIN");
    // ParseComplete, BindComplete, CopyInResponse.
    loop {
        let (tag, _) = read_message(&mut client);
        if tag == b'G' {
            break;
        }
        assert!(matches!(tag, b'1' | b'2'), "unexpected {}", tag as char);
    }
    send_message(&mut client, b'd', b"not-a-number\tx\n");
    send_message(&mut client, b'd', b"5\tv\n");
    send_message(&mut client, b'c', &[]);
    send_message(&mut client, b'S', &[]);
    let messages = read_until_ready(&mut client);
    assert_eq!(messages.iter().filter(|(tag, _)| *tag == b'E').count(), 1);
    assert_eq!(error_field(&messages, 'C').as_deref(), Some("22P02"));
    assert_alive(&mut client);
    assert_eq!(
        rows(&simple(&mut client, "SELECT count(*) FROM g")),
        vec![vec![Some("0".to_string())]]
    );
    finish(client, server);
}

#[test]
fn copy_finds_temporary_tables() {
    let (mut client, server, _dir) = serve();
    simple(
        &mut client,
        "CREATE TEMP TABLE tt(a int, b int GENERATED ALWAYS AS (a * 2) STORED, c text)",
    );
    send_query(&mut client, "COPY tt FROM STDIN");
    let (tag, payload) = read_message(&mut client);
    assert_eq!(tag, b'G', "{}", String::from_utf8_lossy(&payload));
    send_message(
        &mut client,
        b'd',
        b"1	x
",
    );
    send_message(&mut client, b'c', &[]);
    let messages = read_until_ready(&mut client);
    assert!(error_field(&messages, 'M').is_none(), "{messages:?}");
    assert_eq!(
        copy_out(&simple(&mut client, "COPY tt TO STDOUT")),
        "1\tx\n"
    );
    let messages = simple(&mut client, "COPY tt (a, b) FROM STDIN");
    assert_eq!(error_field(&messages, 'C').as_deref(), Some("42P10"));
    assert_alive(&mut client);
    finish(client, server);
}
