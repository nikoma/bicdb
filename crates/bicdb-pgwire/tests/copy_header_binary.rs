//! COPY HEADER (TO and FROM, with MATCH) and binary COPY in both directions.
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

fn send_query(stream: &mut TcpStream, query: &str) {
    let mut payload = query.as_bytes().to_vec();
    payload.push(0);
    send_message(stream, b'Q', &payload);
}

/// Every message up to ReadyForQuery.
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

fn copy_data(messages: &[(u8, Vec<u8>)]) -> Vec<u8> {
    messages
        .iter()
        .filter(|(tag, _)| *tag == b'd')
        .flat_map(|(_, payload)| payload.clone())
        .collect()
}

fn error_text(messages: &[(u8, Vec<u8>)]) -> Option<String> {
    messages
        .iter()
        .find(|(tag, _)| *tag == b'E')
        .map(|(_, payload)| String::from_utf8_lossy(payload).to_string())
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

#[test]
fn copy_to_writes_the_header_line() {
    let (mut client, server, _dir) = serve();
    send_query(&mut client, "CREATE TABLE h(a int, b text)");
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "INSERT INTO h VALUES (1, 'x'), (2, NULL), (3, '')",
    );
    read_until_ready(&mut client);
    send_query(&mut client, "COPY h TO STDOUT WITH (FORMAT csv, HEADER)");
    let out = copy_data(&read_until_ready(&mut client));
    assert_eq!(String::from_utf8(out).unwrap(), "a,b\n1,x\n2,\n3,\"\"\n");
    send_query(
        &mut client,
        "COPY (SELECT 1 AS \"one two\") TO STDOUT WITH (HEADER)",
    );
    let out = copy_data(&read_until_ready(&mut client));
    assert_eq!(String::from_utf8(out).unwrap(), "one two\n1\n");
    finish(client, server);
}

#[test]
fn copy_from_header_match_checks_the_column_names() {
    let (mut client, server, _dir) = serve();
    send_query(&mut client, "CREATE TABLE m(a int, b text)");
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "COPY m FROM STDIN WITH (FORMAT csv, HEADER MATCH)",
    );
    let (tag, _) = read_message(&mut client);
    assert_eq!(tag, b'G');
    send_message(&mut client, b'd', b"a,b\n1,x\n");
    send_message(&mut client, b'c', &[]);
    let messages = read_until_ready(&mut client);
    assert!(error_text(&messages).is_none(), "{messages:?}");

    send_query(
        &mut client,
        "COPY m FROM STDIN WITH (FORMAT csv, HEADER MATCH)",
    );
    let (tag, _) = read_message(&mut client);
    assert_eq!(tag, b'G');
    send_message(&mut client, b'd', b"a,wrong\n2,y\n");
    send_message(&mut client, b'c', &[]);
    let error = error_text(&read_until_ready(&mut client)).expect("header mismatch");
    assert!(error.contains("C22P04"), "{error}");
    assert!(
        error
            .contains("column name mismatch in header line field 2: got \"wrong\", expected \"b\""),
        "{error}"
    );
    assert!(error.contains("WCOPY m, line 1: \"a,wrong\""), "{error}");

    // A plain HEADER skips the line whatever it says.
    send_query(&mut client, "COPY m FROM STDIN WITH (FORMAT csv, HEADER)");
    let (tag, _) = read_message(&mut client);
    assert_eq!(tag, b'G');
    send_message(&mut client, b'd', b"anything\n3,z\n");
    send_message(&mut client, b'c', &[]);
    assert!(error_text(&read_until_ready(&mut client)).is_none());
    send_query(&mut client, "SELECT count(*) FROM m");
    let messages = read_until_ready(&mut client);
    let row = messages.iter().find(|(tag, _)| *tag == b'D').unwrap();
    assert!(row.1.ends_with(b"2"), "{:?}", row.1);
    finish(client, server);
}

#[test]
fn binary_copy_round_trips() {
    let (mut client, server, _dir) = serve();
    send_query(
        &mut client,
        "CREATE TABLE b(a int, t text, n int8); INSERT INTO b VALUES (1, 'x', 5), (NULL, NULL, NULL)",
    );
    read_until_ready(&mut client);
    send_query(&mut client, "COPY b TO STDOUT WITH (FORMAT binary)");
    let messages = read_until_ready(&mut client);
    let (_, response) = messages.iter().find(|(tag, _)| *tag == b'H').unwrap();
    assert_eq!(response[0], 1, "overall binary format");
    let out = copy_data(&messages);
    let mut expected = b"PGCOPY\n\xff\r\n\0".to_vec();
    expected.extend_from_slice(&0i32.to_be_bytes());
    expected.extend_from_slice(&0i32.to_be_bytes());
    expected.extend_from_slice(&3i16.to_be_bytes());
    expected.extend_from_slice(&4i32.to_be_bytes());
    expected.extend_from_slice(&1i32.to_be_bytes());
    expected.extend_from_slice(&1i32.to_be_bytes());
    expected.extend_from_slice(b"x");
    expected.extend_from_slice(&8i32.to_be_bytes());
    expected.extend_from_slice(&5i64.to_be_bytes());
    expected.extend_from_slice(&3i16.to_be_bytes());
    for _ in 0..3 {
        expected.extend_from_slice(&(-1i32).to_be_bytes());
    }
    expected.extend_from_slice(&(-1i16).to_be_bytes());
    assert_eq!(out, expected);

    send_query(&mut client, "CREATE TABLE b2(a int, t text, n int8)");
    read_until_ready(&mut client);
    send_query(&mut client, "COPY b2 FROM STDIN (FORMAT binary)");
    let (tag, response) = read_message(&mut client);
    assert_eq!(tag, b'G');
    assert_eq!(response[0], 1);
    // Split the stream mid-tuple: a partial tuple waits for the next chunk.
    let (first, second) = expected.split_at(25);
    send_message(&mut client, b'd', first);
    send_message(&mut client, b'd', second);
    send_message(&mut client, b'c', &[]);
    let messages = read_until_ready(&mut client);
    assert!(error_text(&messages).is_none(), "{messages:?}");
    send_query(
        &mut client,
        "SELECT count(*), max(t), sum(n), count(a) FROM b2",
    );
    let messages = read_until_ready(&mut client);
    let row = messages.iter().find(|(tag, _)| *tag == b'D').unwrap();
    let text = String::from_utf8_lossy(&row.1).to_string();
    assert!(text.contains('x') && text.contains('5'), "{text}");

    send_query(&mut client, "COPY b2 FROM STDIN (FORMAT binary)");
    let (tag, _) = read_message(&mut client);
    assert_eq!(tag, b'G');
    send_message(&mut client, b'd', b"not a copy file at all");
    send_message(&mut client, b'c', &[]);
    let error = error_text(&read_until_ready(&mut client)).expect("bad signature");
    assert!(
        error.contains("COPY file signature not recognized"),
        "{error}"
    );
    finish(client, server);
}

/// (name, type oid, typmod) of each RowDescription field.
fn row_description(messages: &[(u8, Vec<u8>)]) -> Vec<(String, i32, i32)> {
    let (_, payload) = messages.iter().find(|(tag, _)| *tag == b'T').unwrap();
    let count = i16::from_be_bytes([payload[0], payload[1]]) as usize;
    let mut at = 2;
    let mut fields = Vec::new();
    for _ in 0..count {
        let end = at + payload[at..].iter().position(|byte| *byte == 0).unwrap();
        let name = String::from_utf8(payload[at..end].to_vec()).unwrap();
        at = end + 1 + 4 + 2;
        let oid = i32::from_be_bytes(payload[at..at + 4].try_into().unwrap());
        at += 4 + 2;
        let typmod = i32::from_be_bytes(payload[at..at + 4].try_into().unwrap());
        at += 4 + 2;
        fields.push((name, oid, typmod));
    }
    fields
}

#[test]
fn row_descriptions_carry_postgresql_types_and_modifiers() {
    let (mut client, server, _dir) = serve();
    send_query(
        &mut client,
        "SELECT 'x'::varchar(3), 1.5::numeric(5,2), date_trunc('day', '2024-01-02'::date), \
         ceil(1.5), sqrt(4)",
    );
    let fields = row_description(&read_until_ready(&mut client));
    assert_eq!(
        fields,
        vec![
            ("varchar".to_string(), 1043, 7),
            ("numeric".to_string(), 1700, (5 << 16 | 2) + 4),
            ("date_trunc".to_string(), 1184, -1),
            ("ceil".to_string(), 1700, -1),
            ("sqrt".to_string(), 701, -1),
        ]
    );
    send_query(
        &mut client,
        "CREATE TABLE one(x text); INSERT INTO one VALUES ('a')",
    );
    read_until_ready(&mut client);
    send_query(&mut client, "SELECT x::varchar(4), x FROM one");
    let fields = row_description(&read_until_ready(&mut client));
    assert_eq!(fields[0].1, 1043);
    assert_eq!(fields[0].2, 8);
    assert_eq!(fields[1].2, -1);
    finish(client, server);
}
