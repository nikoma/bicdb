//! Describe a rule-driven INSERT without executing it or advertising action rows.
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
fn receive(s: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut h = [0u8; 5];
    s.read_exact(&mut h).unwrap();
    let n = i32::from_be_bytes(h[1..].try_into().unwrap()) as usize;
    assert!((4..1024 * 1024).contains(&n));
    let mut p = vec![0; n - 4];
    s.read_exact(&mut p).unwrap();
    (h[0], p)
}
#[tokio::test]
async fn portal_describe_keeps_rule_actions_deferred_until_execute() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    let serving = server.clone();
    let task = thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=bicdb dbname=bicdb"),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    let connection_task = tokio::spawn(async move {
        let _ = connection.await;
    });
    client.batch_execute("CREATE TABLE portal_request(v integer); CREATE TABLE portal_effect(v integer); CREATE FUNCTION portal_apply(n integer) RETURNS integer LANGUAGE plpgsql AS $$ BEGIN INSERT INTO portal_effect VALUES(n); RETURN n; END $$; CREATE RULE r AS ON INSERT TO portal_request DO INSTEAD SELECT portal_apply(new.v)").await.unwrap();
    let mut raw = TcpStream::connect(("127.0.0.1", port)).unwrap();
    raw.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut startup = 196608i32.to_be_bytes().to_vec();
    startup.extend_from_slice(b"user\0bicdb\0database\0bicdb\0\0");
    raw.write_all(&((startup.len() + 4) as i32).to_be_bytes())
        .unwrap();
    raw.write_all(&startup).unwrap();
    while receive(&mut raw).0 != b'Z' {}
    let mut parse = b"rule_insert\0INSERT INTO portal_request VALUES(7)\0".to_vec();
    parse.extend_from_slice(&0i16.to_be_bytes());
    send(&mut raw, b'P', &parse);
    let mut bind = b"rule_portal\0rule_insert\0".to_vec();
    for _ in 0..3 {
        bind.extend_from_slice(&0i16.to_be_bytes());
    }
    send(&mut raw, b'B', &bind);
    send(&mut raw, b'D', b"Prule_portal\0");
    send(&mut raw, b'H', &[]);
    assert_eq!(receive(&mut raw).0, b'1');
    assert_eq!(receive(&mut raw).0, b'2');
    assert_eq!(
        receive(&mut raw).0,
        b'n',
        "INSERT without RETURNING describes as NoData even when its rule returns rows"
    );
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM portal_effect", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0,
        "Describe must not run the rule"
    );
    let mut execute = b"rule_portal\0".to_vec();
    execute.extend_from_slice(&0i32.to_be_bytes());
    send(&mut raw, b'E', &execute);
    send(&mut raw, b'S', &[]);
    let mut rows = 0;
    let mut command = None;
    loop {
        let (t, p) = receive(&mut raw);
        match t {
            b'D' => rows += 1,
            b'C' => command = Some(String::from_utf8(p[..p.len() - 1].to_vec()).unwrap()),
            b'Z' => break,
            b'N' => {}
            other => panic!("unexpected response {other}: {p:?}"),
        }
    }
    assert_eq!(rows, 1);
    assert_eq!(command.as_deref(), Some("INSERT 0 0"));
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM portal_effect", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    drop(raw);
    drop(client);
    connection_task.abort();
    server.request_shutdown();
    task.join().unwrap().unwrap();
}
