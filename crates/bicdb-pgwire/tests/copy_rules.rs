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

#[test]
fn text_copy_end_marker_requires_its_own_line() {
    let (mut client, server, _dir) = serve();
    simple(&mut client, "CREATE TABLE markers (value text)");
    for data in [
        b"first\nsecond\nfoo\\.\n".as_slice(),
        b"first\nsecond\n\\.foo\n",
        b"first\nsecond\n\\.",
    ] {
        let messages = copy_in(&mut client, "COPY markers FROM STDIN", data);
        assert_eq!(error_field(&messages, 'C').as_deref(), Some("22P04"));
        assert_eq!(
            error_field(&messages, 'M').as_deref(),
            Some("end-of-copy marker is not alone on its line")
        );
        assert_eq!(
            error_field(&messages, 'W').as_deref(),
            Some("COPY markers, line 3")
        );
        assert!(rows(&simple(&mut client, "SELECT value FROM markers")).is_empty());
    }

    send_query(&mut client, "COPY markers FROM STDIN");
    assert_eq!(read_message(&mut client).0, b'G');
    send_message(&mut client, b'd', b"foo\\\\.\r\n\\");
    send_message(&mut client, b'd', b".\r\n\xff");
    send_message(&mut client, b'd', b"ignored\r\n");
    send_message(&mut client, b'c', &[]);
    assert!(error_field(&read_until_ready(&mut client), 'C').is_none());
    let messages = copy_in(&mut client, "COPY markers FROM STDIN CSV", b"\\.\n");
    assert!(error_field(&messages, 'C').is_none());
    assert_eq!(
        rows(&simple(
            &mut client,
            "SELECT value FROM markers ORDER BY value"
        )),
        vec![vec![Some("\\.".into())], vec![Some("foo\\.".into())]]
    );
    finish(client, server);
}

#[test]
fn copy_checks_used_constant_defaults_before_entering_copy_mode() {
    let (mut client, server, _dir) = serve();
    let setup = simple(
        &mut client,
        "CREATE TABLE oversized_defaults (a varchar(5) DEFAULT 'overlong', b varchar(5))",
    );
    assert!(error_field(&setup, 'C').is_none());
    for sql in [
        "COPY oversized_defaults (b) FROM STDIN",
        "COPY oversized_defaults FROM STDIN (DEFAULT '!')",
        "COPY oversized_defaults (b) FROM STDIN (ON_ERROR IGNORE)",
    ] {
        let messages = copy_in(&mut client, sql, b"");
        assert_eq!(
            messages.first().map(|message| message.0),
            Some(b'E'),
            "{sql}"
        );
        assert_eq!(error_field(&messages, 'C').as_deref(), Some("22001"));
        assert_eq!(
            error_field(&messages, 'M').as_deref(),
            Some("value too long for type character varying(5)")
        );
        assert!(error_field(&messages, 'W').is_none());
    }
    assert!(error_field(
        &copy_in(
            &mut client,
            "COPY oversized_defaults FROM STDIN",
            b"hello\tx\n"
        ),
        'C'
    )
    .is_none());
    assert_eq!(
        rows(&simple(&mut client, "SELECT a,b FROM oversized_defaults")),
        vec![vec![Some("hello".into()), Some("x".into())]]
    );
    let insert = simple(
        &mut client,
        "INSERT INTO oversized_defaults (b) VALUES ('y')",
    );
    assert_eq!(error_field(&insert, 'C').as_deref(), Some("22001"));

    let setup = simple(
        &mut client,
        "CREATE TABLE explicitly_cast_default (a varchar(5) DEFAULT 'overlong'::varchar(5))",
    );
    assert!(error_field(&setup, 'C').is_none());
    let copied = copy_in(
        &mut client,
        "COPY explicitly_cast_default FROM STDIN (DEFAULT '!')",
        b"!\n",
    );
    assert!(error_field(&copied, 'C').is_none(), "{copied:?}");
    assert_eq!(
        rows(&simple(
            &mut client,
            "SELECT a FROM explicitly_cast_default"
        )),
        vec![vec![Some("overl".into())]],
    );

    simple(
        &mut client,
        "CREATE SEQUENCE copy_unconsumed;
         CREATE TABLE expression_defaults (a bigint DEFAULT nextval('copy_unconsumed'), b int)",
    );
    assert!(error_field(
        &copy_in(&mut client, "COPY expression_defaults (b) FROM STDIN", b""),
        'C'
    )
    .is_none());
    assert_eq!(
        rows(&simple(&mut client, "SELECT nextval('copy_unconsumed')")),
        vec![vec![Some("1".into())]]
    );
    finish(client, server);
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

const BAD_ROWS: &[u8] =
    b"1\t{1}\t1\na\t{2}\t2\n3\t{3}\t3333333333\n4\t{a, 4}\t4\n\t{5}\t5\n6\t{6}\t6\n";

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
    // Errors come in input order: a bad value on line 2 is reported, not
    // the short line 5 after it.
    let messages = copy_in(
        &mut client,
        "COPY e FROM STDIN WITH (on_error stop)",
        b"1\t{1}\t1\na\t{2}\t2\n3\t{3}\t3\n4\t{4}\t4\n\n",
    );
    assert_eq!(
        error_field(&messages, 'M').as_deref(),
        Some("invalid input syntax for type integer: \"a\"")
    );
    assert_eq!(
        error_field(&messages, 'W').as_deref(),
        Some("COPY e, line 2, column n: \"a\"")
    );
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
        ("COPY w FROM STDIN WHERE f > 1", "42703"),
        ("COPY w FROM STDIN WHERE w.f > 1", "42703"),
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
