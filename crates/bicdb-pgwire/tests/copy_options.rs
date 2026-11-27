//! COPY options: implemented as PostgreSQL does, or refused before COPY mode.
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

#[test]
fn unimplemented_options_are_refused_before_copy_mode() {
    let (mut client, server, _dir) = serve();
    simple(&mut client, "CREATE TABLE t(a int, b text)");
    for (sql, message) in [
        (
            "COPY t FROM STDIN (ENCODING 'EUC_JP')",
            "COPY ENCODING other than UTF8 and LATIN1 is not supported",
        ),
        (
            "COPY t TO STDOUT (ENCODING 'WIN1252')",
            "COPY ENCODING other than UTF8 and LATIN1 is not supported",
        ),
    ] {
        let messages = simple(&mut client, sql);
        assert!(
            messages.iter().all(|(tag, _)| *tag != b'G' && *tag != b'H'),
            "{sql} entered COPY mode"
        );
        assert_eq!(
            error_field(&messages, 'C').as_deref(),
            Some("0A000"),
            "{sql}"
        );
        assert_eq!(
            error_field(&messages, 'M').as_deref(),
            Some(message),
            "{sql}"
        );
        assert_alive(&mut client);
    }
    // PostgreSQL's own validation comes first.
    let messages = simple(&mut client, "COPY t TO STDOUT (FREEZE)");
    assert_eq!(
        error_field(&messages, 'M').as_deref(),
        Some("COPY FREEZE cannot be used with COPY TO")
    );
    let messages = simple(&mut client, "COPY t FROM STDIN (DELIMITER '||')");
    assert_eq!(
        error_field(&messages, 'M').as_deref(),
        Some("COPY delimiter must be a single one-byte character")
    );
    let messages = simple(&mut client, "COPY t TO STDOUT (FORMAT xml)");
    assert_eq!(error_field(&messages, 'P').as_deref(), Some("19"));
    // Options PostgreSQL accepts with no effect here are accepted.
    let messages = copy_in(
        &mut client,
        "COPY t FROM STDIN (FREEZE false, ON_ERROR stop, LOG_VERBOSITY verbose, ENCODING 'utf-8')",
        b"1\tx\n",
    );
    assert!(error_field(&messages, 'M').is_none(), "{messages:?}");
    finish(client, server);
}

#[test]
fn text_and_csv_options_shape_the_data() {
    let (mut client, server, _dir) = serve();
    simple(&mut client, "CREATE TABLE t(a int, b text, c text)");
    let messages = copy_in(
        &mut client,
        "COPY t FROM STDIN WITH DELIMITER '|' NULL AS 'nil'",
        b"1|x\\|y|nil\n2|\\x41\\101\\b|\n",
    );
    assert!(error_field(&messages, 'M').is_none(), "{messages:?}");
    assert_eq!(
        rows(&simple(&mut client, "SELECT a, b, c FROM t ORDER BY a")),
        vec![
            vec![Some("1".into()), Some("x|y".into()), None],
            vec![
                Some("2".into()),
                Some("AA\u{8}".into()),
                Some(String::new())
            ],
        ]
    );
    assert_eq!(
        String::from_utf8(copy_bytes(&simple(
            &mut client,
            "COPY t TO STDOUT (DELIMITER '|', NULL 'nil')"
        )))
        .unwrap(),
        "1|x\\|y|nil\n2|AA\\b|\n"
    );
    simple(&mut client, "TRUNCATE t");
    let messages = copy_in(
        &mut client,
        "COPY t FROM STDIN (FORMAT csv, DELIMITER ';', QUOTE '''', ESCAPE '\\', \
         FORCE_NOT_NULL (b), FORCE_NULL (c))",
        b"1;;''\n2;'it\\'s';'a;\nb'\n",
    );
    assert!(error_field(&messages, 'M').is_none(), "{messages:?}");
    assert_eq!(
        rows(&simple(&mut client, "SELECT a, b, c FROM t ORDER BY a")),
        vec![
            vec![Some("1".into()), Some(String::new()), None],
            vec![Some("2".into()), Some("it's".into()), Some("a;\nb".into())],
        ]
    );
    assert_eq!(
        String::from_utf8(copy_bytes(&simple(
            &mut client,
            "COPY t TO STDOUT (FORMAT csv, QUOTE '''', ESCAPE '\\', FORCE_QUOTE (a), NULL 'N')"
        )))
        .unwrap(),
        "'1',,N\n'2','it\\'s','a;\nb'\n"
    );
    // FORCE_QUOTE must name a column COPY writes.
    let messages = simple(
        &mut client,
        "COPY t (a) TO STDOUT (FORMAT csv, FORCE_QUOTE (b))",
    );
    assert_eq!(error_field(&messages, 'C').as_deref(), Some("42P10"));
    assert_eq!(
        error_field(&messages, 'M').as_deref(),
        Some("FORCE_QUOTE column \"b\" not referenced by COPY")
    );
    let messages = simple(
        &mut client,
        "COPY t FROM STDIN (FORMAT csv, FORCE_NULL (zz))",
    );
    assert!(messages.iter().all(|(tag, _)| *tag != b'G'));
    assert_eq!(error_field(&messages, 'C').as_deref(), Some("42703"));
    assert_alive(&mut client);
    finish(client, server);
}

#[test]
fn latin1_encoding_converts_both_ways() {
    let (mut client, server, _dir) = serve();
    simple(&mut client, "CREATE TABLE t(s text)");
    let messages = copy_in(
        &mut client,
        "COPY t FROM STDIN (ENCODING 'LATIN1')",
        b"caf\xe9\n",
    );
    assert!(error_field(&messages, 'M').is_none(), "{messages:?}");
    assert_eq!(
        rows(&simple(&mut client, "SELECT s FROM t")),
        vec![vec![Some("café".into())]]
    );
    assert_eq!(
        copy_bytes(&simple(&mut client, "COPY t TO STDOUT (ENCODING 'latin1')")),
        b"caf\xe9\n".to_vec()
    );
    simple(&mut client, "INSERT INTO t VALUES ('€')");
    let messages = simple(&mut client, "COPY t TO STDOUT (ENCODING 'LATIN1')");
    assert_eq!(error_field(&messages, 'C').as_deref(), Some("22P05"));
    assert_eq!(
        error_field(&messages, 'M').as_deref(),
        Some(
            "character with byte sequence 0xe2 0x82 0xac in encoding \"UTF8\" \
             has no equivalent in encoding \"LATIN1\""
        )
    );
    assert_alive(&mut client);
    // UTF8 input that is not UTF-8.
    let messages = copy_in(&mut client, "COPY t FROM STDIN", b"caf\xe9\n");
    assert_eq!(error_field(&messages, 'C').as_deref(), Some("22021"));
    assert_alive(&mut client);
    finish(client, server);
}
