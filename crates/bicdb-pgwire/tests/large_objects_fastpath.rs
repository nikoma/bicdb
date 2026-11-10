//! Actual frontend F/V cycles through SQL admission, transactions and codecs.
use bicdb_pgwire::{PgWireConfig, PgWireServer};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
    time::Duration,
};
fn send(s: &mut TcpStream, tag: u8, data: &[u8]) {
    s.write_all(&[tag]).unwrap();
    s.write_all(&((data.len() + 4) as i32).to_be_bytes())
        .unwrap();
    s.write_all(data).unwrap();
}
fn cycle(s: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
    let mut out = Vec::new();
    loop {
        let mut h = [0; 5];
        s.read_exact(&mut h).unwrap();
        let n = i32::from_be_bytes(h[1..].try_into().unwrap());
        assert!((4..1024 * 1024).contains(&n));
        let mut p = vec![0; n as usize - 4];
        s.read_exact(&mut p).unwrap();
        out.push((h[0], p));
        if h[0] == b'Z' {
            return out;
        }
    }
}
fn query(s: &mut TcpStream, sql: &str) -> Vec<(u8, Vec<u8>)> {
    let mut data = sql.as_bytes().to_vec();
    data.push(0);
    send(s, b'Q', &data);
    cycle(s)
}
fn ok_query(s: &mut TcpStream, sql: &str) -> Vec<(u8, Vec<u8>)> {
    let result = query(s, sql);
    assert!(!result.iter().any(|(t, _)| *t == b'E'), "{sql}: {result:?}");
    result
}
fn first_text(rows: &[(u8, Vec<u8>)]) -> String {
    let data = &rows.iter().find(|(t, _)| *t == b'D').unwrap().1;
    assert_eq!(&data[..2], &1i16.to_be_bytes());
    let n = i32::from_be_bytes(data[2..6].try_into().unwrap()) as usize;
    String::from_utf8(data[6..6 + n].to_vec()).unwrap()
}
fn frame(oid: u32, formats: &[i16], args: &[Option<&[u8]>], result: i16) -> Vec<u8> {
    let mut p = oid.to_be_bytes().to_vec();
    p.extend_from_slice(&(formats.len() as i16).to_be_bytes());
    for f in formats {
        p.extend_from_slice(&f.to_be_bytes());
    }
    p.extend_from_slice(&(args.len() as i16).to_be_bytes());
    for arg in args {
        match arg {
            None => p.extend_from_slice(&(-1i32).to_be_bytes()),
            Some(arg) => {
                p.extend_from_slice(&(arg.len() as i32).to_be_bytes());
                p.extend_from_slice(arg);
            }
        }
    }
    p.extend_from_slice(&result.to_be_bytes());
    p
}
fn call(s: &mut TcpStream, oid: u32, args: &[Option<&[u8]>]) -> Vec<(u8, Vec<u8>)> {
    send(s, b'F', &frame(oid, &[1], args, 1));
    cycle(s)
}
fn value(rows: &[(u8, Vec<u8>)]) -> Option<Vec<u8>> {
    assert!(!rows.iter().any(|(t, _)| *t == b'E'), "{rows:?}");
    assert!(
        !rows
            .iter()
            .any(|(t, _)| *t == b'C' || *t == b'T' || *t == b'D'),
        "FunctionCall must only return a scalar V response"
    );
    let v = &rows.iter().find(|(t, _)| *t == b'V').unwrap().1;
    let n = i32::from_be_bytes(v[..4].try_into().unwrap());
    if n == -1 {
        assert_eq!(v.len(), 4);
        None
    } else {
        assert_eq!(v.len(), 4 + n as usize);
        Some(v[4..].to_vec())
    }
}
fn number(rows: &[(u8, Vec<u8>)]) -> i32 {
    i32::from_be_bytes(value(rows).unwrap().try_into().unwrap())
}
fn error(rows: &[(u8, Vec<u8>)], code: &str, status: u8) {
    let e = &rows
        .iter()
        .find(|(t, _)| *t == b'E')
        .expect("ErrorResponse")
        .1;
    assert!(
        e.split(|b| *b == 0)
            .any(|s| s.first() == Some(&b'C') && &s[1..] == code.as_bytes()),
        "{rows:?}"
    );
    assert_eq!(rows.last().unwrap(), &(b'Z', vec![status]));
    assert!(!rows.iter().any(|(t, _)| *t == b'V'));
}
#[test]
fn native_large_object_fastpath_cycles_preserve_bytes_transactions_and_error_recovery() {
    for mode in [
        bicdb_core::StorageMode::EmbeddedMemory,
        bicdb_core::StorageMode::ServerPaged,
    ] {
        let root = tempfile::tempdir().unwrap();
        drop(
            bicdb_core::BicDb::open_with_config(
                root.path(),
                bicdb_core::DbConfig::default()
                    .with_fsync(false)
                    .with_storage_mode(mode),
            )
            .unwrap(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = PgWireServer::open(root.path(), PgWireConfig::default()).unwrap();
        let serving = server.clone();
        let task = thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
        let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut startup = 196608i32.to_be_bytes().to_vec();
        startup.extend_from_slice(b"user\0bicdb\0database\0bicdb\0\0");
        s.write_all(&((startup.len() + 4) as i32).to_be_bytes())
            .unwrap();
        s.write_all(&startup).unwrap();
        cycle(&mut s);
        ok_query(&mut s, "BEGIN");
        let oid = 51001u32.to_be_bytes();
        let fd = 0i32.to_be_bytes();
        assert_eq!(number(&call(&mut s, 715, &[Some(&oid)])), 51001);
        assert_eq!(
            number(&call(
                &mut s,
                952,
                &[Some(&oid), Some(&393216i32.to_be_bytes())]
            )),
            0
        );
        let bytes = b"\0\xff';--data";
        assert_eq!(
            number(&call(&mut s, 955, &[Some(&fd), Some(bytes)])),
            bytes.len() as i32
        );
        assert_eq!(
            i64::from_be_bytes(
                value(&call(&mut s, 3171, &[Some(&fd)]))
                    .unwrap()
                    .try_into()
                    .unwrap()
            ),
            bytes.len() as i64
        );
        assert_eq!(
            number(&call(
                &mut s,
                956,
                &[
                    Some(&fd),
                    Some(&0i32.to_be_bytes()),
                    Some(&0i32.to_be_bytes())
                ]
            )),
            0
        );
        assert_eq!(
            value(&call(
                &mut s,
                954,
                &[Some(&fd), Some(&999i32.to_be_bytes())]
            ))
            .unwrap(),
            bytes
        );
        ok_query(&mut s, "SAVEPOINT keep");
        assert_eq!(
            number(&call(&mut s, 3172, &[Some(&fd), Some(&1i64.to_be_bytes())])),
            0
        );
        assert_eq!(number(&call(&mut s, 958, &[Some(&fd)])), bytes.len() as i32);
        ok_query(&mut s, "ROLLBACK TO keep");
        assert_eq!(value(&call(&mut s, 3458, &[Some(&oid)])).unwrap(), bytes);
        error(
            &call(&mut s, 952, &[Some(&oid), Some(&0i32.to_be_bytes())]),
            "22023",
            b'E',
        );
        error(&call(&mut s, 958, &[Some(&fd)]), "25P02", b'E');
        ok_query(&mut s, "ROLLBACK TO keep");
        send(&mut s, b'F', &frame(955, &[], &[Some(b"0"), None], 0));
        assert_eq!(value(&cycle(&mut s)), None);
        ok_query(&mut s, "COMMIT");
        error(&call(&mut s, 958, &[Some(&fd)]), "42704", b'I');
        let hex = bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
        assert_eq!(
            first_text(&ok_query(&mut s, "SELECT encode(lo_get(51001),'hex')")),
            hex
        );
        ok_query(&mut s, "BEGIN READ ONLY");
        error(
            &call(&mut s, 715, &[Some(&51002u32.to_be_bytes())]),
            "25006",
            b'E',
        );
        ok_query(&mut s, "ROLLBACK");
        ok_query(&mut s, "BEGIN");
        let mut malformed = frame(715, &[1], &[Some(&51002u32.to_be_bytes())], 1);
        malformed.push(0);
        send(&mut s, b'F', &malformed);
        error(&cycle(&mut s), "08P01", b'E');
        ok_query(&mut s, "ROLLBACK");
        error(&query(&mut s, "SELECT lo_get(51002)"), "42704", b'I');
        ok_query(&mut s,"CREATE FUNCTION fastpath_custom(v integer) RETURNS integer LANGUAGE sql AS $$ SELECT v+1 $$");
        let proc_oid = first_text(&ok_query(
            &mut s,
            "SELECT oid::text FROM pg_proc WHERE proname='fastpath_custom'",
        ))
        .parse::<u32>()
        .unwrap();
        assert_eq!(
            number(&call(&mut s, proc_oid, &[Some(&7i32.to_be_bytes())])),
            8
        );
        assert_eq!(value(&call(&mut s, proc_oid, &[None])), None);
        send(
            &mut s,
            b'F',
            &frame(
                proc_oid,
                &[],
                &[Some(b"1); CREATE TABLE injected(n int);--")],
                1,
            ),
        );
        error(&cycle(&mut s), "22P02", b'I');
        for input in [b"2147483648".as_slice(), b"9223372036854775808".as_slice()] {
            send(&mut s, b'F', &frame(proc_oid, &[], &[Some(input)], 1));
            error(&cycle(&mut s), "22003", b'I');
        }
        assert_eq!(
            first_text(&ok_query(&mut s, "SELECT fastpath_custom(1)")),
            "2"
        );
        ok_query(&mut s, "SELECT lo_unlink(51001)");
        send(&mut s, b'X', &[]);
        drop(s);
        server.request_shutdown();
        task.join().unwrap().unwrap();
    }
}
