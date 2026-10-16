//! `SUBSCRIBE` live queries through a running server: streamed changes over
//! the extended protocol and over COPY, cancellation, and authorization.
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::pin::pin;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use bicdb_pgwire::{PgWireConfig, PgWireServer};
use futures_util::StreamExt;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

struct Running {
    address: SocketAddr,
    server: Arc<PgWireServer>,
    thread: thread::JoinHandle<bicdb_pgwire::Result<()>>,
}

impl Running {
    fn start(path: &Path) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = PgWireServer::open(path, PgWireConfig::default()).unwrap();
        let serving = server.clone();
        let thread =
            thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
        Self {
            address,
            server,
            thread,
        }
    }

    async fn client(&self, user: &str) -> Client {
        let config = format!(
            "host=127.0.0.1 port={} user={user} dbname=bicdb",
            self.address.port()
        );
        let (client, connection) = tokio_postgres::connect(&config, NoTls).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }

    fn stop(self) {
        self.server.request_shutdown();
        self.thread.join().unwrap().unwrap();
    }
}

async fn text_rows(client: &Client, sql: &str) -> Vec<Vec<Option<String>>> {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|index| row.get(index).map(str::to_string))
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

/// A social feed: alice follows bob and carol.
async fn seed_feed(client: &Client) {
    for sql in [
        "CREATE TABLE posts (id INT PRIMARY KEY, author TEXT, body TEXT)",
        "CREATE TABLE follows (follower TEXT, followee TEXT, PRIMARY KEY (follower, followee))",
        "INSERT INTO follows VALUES ('alice', 'bob'), ('alice', 'carol')",
        "INSERT INTO posts VALUES (1, 'bob', 'hello'), (2, 'dave', 'unfollowed'), (3, 'carol', 'hi')",
    ] {
        client.batch_execute(sql).await.unwrap();
    }
}

const FEED: &str = "SUBSCRIBE SELECT p.id, p.author, p.body FROM posts p \
                    JOIN follows f ON f.followee = p.author \
                    WHERE f.follower = $1 ORDER BY p.id DESC LIMIT 3";

/// (change, id, author, body) of a streamed row.
fn change_row(row: &tokio_postgres::Row) -> (String, i32, String, String) {
    (row.get(1), row.get(2), row.get(3), row.get(4))
}

fn change(sign: &str, id: i32, author: &str, body: &str) -> (String, i32, String, String) {
    (sign.to_string(), id, author.to_string(), body.to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_feed_subscription_streams_changes_and_stops_on_cancel() {
    let dir = tempfile::tempdir().unwrap();
    let running = Running::start(dir.path());
    let writer = running.client("bicdb").await;
    seed_feed(&writer).await;
    let subscriber = running.client("bicdb").await;
    let cancel = subscriber.cancel_token();

    // Prepared with a parameter, as drivers do.
    let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![&"alice"];
    let stream = subscriber.query_raw(FEED, params).await.unwrap();
    let mut stream = pin!(stream);
    let mut next = async || {
        tokio::time::timeout(Duration::from_secs(10), stream.next())
            .await
            .expect("a change arrived in time")
            .expect("the stream is open")
            .map(|row| change_row(&row))
            .unwrap()
    };
    // The initial result: alice's feed, newest first.
    assert_eq!(next().await, change("+", 1, "bob", "hello"));
    assert_eq!(next().await, change("+", 3, "carol", "hi"));

    writer
        .batch_execute("INSERT INTO posts VALUES (4, 'bob', 'second')")
        .await
        .unwrap();
    assert_eq!(next().await, change("+", 4, "bob", "second"));
    // A post by someone alice does not follow changes nothing; an edit is a
    // removal and an addition.
    writer
        .batch_execute("INSERT INTO posts VALUES (5, 'dave', 'still unfollowed')")
        .await
        .unwrap();
    writer
        .batch_execute("UPDATE posts SET body = 'edited' WHERE id = 3")
        .await
        .unwrap();
    assert_eq!(next().await, change("-", 3, "carol", "hi"));
    assert_eq!(next().await, change("+", 3, "carol", "edited"));
    // Following dave brings his newest post into the top three and pushes
    // the oldest out.
    writer
        .batch_execute("INSERT INTO follows VALUES ('alice', 'dave')")
        .await
        .unwrap();
    assert_eq!(next().await, change("-", 1, "bob", "hello"));
    assert_eq!(next().await, change("+", 5, "dave", "still unfollowed"));
    writer
        .batch_execute("DELETE FROM posts WHERE id = 4")
        .await
        .unwrap();
    assert_eq!(next().await, change("-", 4, "bob", "second"));
    // Dave's older post is now the third newest.
    assert_eq!(next().await, change("+", 2, "dave", "unfollowed"));

    // Cancelling ends the subscription with SQLSTATE 57014; the connection
    // stays usable.
    cancel.cancel_query(NoTls).await.unwrap();
    let ended = loop {
        match tokio::time::timeout(Duration::from_secs(10), stream.next())
            .await
            .expect("the cancel took effect")
        {
            Some(Ok(_)) => continue,
            Some(Err(error)) => break Some(error),
            None => break None,
        }
    };
    let error = ended.expect("cancellation is reported as an error");
    assert_eq!(
        error.code().map(|code| code.code()),
        Some("57014"),
        "{error}"
    );
    drop(stream);
    assert_eq!(
        text_rows(&subscriber, "SELECT count(*) FROM posts").await,
        vec![vec![Some("4".to_string())]]
    );
    // A join is re-run whole on every change to its tables.
    let (_, full, incremental) = running.server.live_query_stats();
    assert!(
        full >= 5 && incremental == 0,
        "full={full} incremental={incremental}"
    );
    running.stop();
}

// A minimal simple-query client, for the COPY form `psql` uses.
fn cstr(payload: &mut Vec<u8>, value: &str) {
    payload.extend_from_slice(value.as_bytes());
    payload.push(0);
}

fn raw_connect(address: SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect(address).unwrap();
    let mut payload = Vec::new();
    payload.extend_from_slice(&196_608_i32.to_be_bytes());
    cstr(&mut payload, "user");
    cstr(&mut payload, "bicdb");
    cstr(&mut payload, "database");
    cstr(&mut payload, "bicdb");
    payload.push(0);
    stream
        .write_all(&((payload.len() as i32) + 4).to_be_bytes())
        .unwrap();
    stream.write_all(&payload).unwrap();
    // Startup answers end with ReadyForQuery.
    while raw_read(&mut stream).0 != b'Z' {}
    stream
}

fn raw_query(stream: &mut TcpStream, sql: &str) {
    let mut payload = sql.as_bytes().to_vec();
    payload.push(0);
    stream.write_all(b"Q").unwrap();
    stream
        .write_all(&((payload.len() as i32) + 4).to_be_bytes())
        .unwrap();
    stream.write_all(&payload).unwrap();
}

fn raw_read(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut tag = [0_u8; 1];
    stream.read_exact(&mut tag).unwrap();
    let mut len = [0_u8; 4];
    stream.read_exact(&mut len).unwrap();
    let mut payload = vec![0_u8; (i32::from_be_bytes(len) - 4) as usize];
    stream.read_exact(&mut payload).unwrap();
    (tag[0], payload)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copy_subscribe_streams_text_rows_until_the_client_leaves() {
    let dir = tempfile::tempdir().unwrap();
    let running = Running::start(dir.path());
    let writer = running.client("bicdb").await;
    writer
        .batch_execute(
            "CREATE TABLE counters (name TEXT PRIMARY KEY, n INT); \
             INSERT INTO counters VALUES ('a', 1)",
        )
        .await
        .unwrap();
    let address = running.address;
    let subscriber = tokio::task::spawn_blocking(move || {
        let mut stream = raw_connect(address);
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        raw_query(
            &mut stream,
            "COPY (SUBSCRIBE SELECT name, sum(n) AS total FROM counters GROUP BY name) TO STDOUT",
        );
        let (tag, _) = raw_read(&mut stream);
        assert_eq!(tag, b'H', "CopyOutResponse first");
        let (tag, payload) = raw_read(&mut stream);
        assert_eq!(tag, b'd', "CopyData");
        (stream, String::from_utf8(payload).unwrap())
    })
    .await
    .unwrap();
    let (mut stream, initial) = subscriber;
    assert!(initial.ends_with("\t+\ta\t1\n"), "{initial:?}");
    writer
        .batch_execute("UPDATE counters SET n = 5 WHERE name = 'a'")
        .await
        .unwrap();
    let more = tokio::task::spawn_blocking(move || {
        let removed = {
            let (tag, payload) = raw_read(&mut stream);
            assert_eq!(tag, b'd');
            String::from_utf8(payload).unwrap()
        };
        let added = {
            let (tag, payload) = raw_read(&mut stream);
            assert_eq!(tag, b'd');
            String::from_utf8(payload).unwrap()
        };
        // The subscriber disconnects mid-stream: the server notices and the
        // statement ends without affecting anyone else.
        drop(stream);
        (removed, added)
    })
    .await
    .unwrap();
    assert!(more.0.ends_with("\t-\ta\t1\n"), "{:?}", more.0);
    assert!(more.1.ends_with("\t+\ta\t5\n"), "{:?}", more.1);
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(
        text_rows(&writer, "SELECT n FROM counters").await,
        vec![vec![Some("5".to_string())]]
    );
    // A grouped aggregate re-aggregates the affected groups only.
    let (_, full, incremental) = running.server.live_query_stats();
    assert!(
        full == 0 && incremental >= 1,
        "full={full} incremental={incremental}"
    );
    running.stop();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriptions_run_with_the_subscribers_permissions() {
    let dir = tempfile::tempdir().unwrap();
    let running = Running::start(dir.path());
    let admin = running.client("bicdb").await;
    for sql in [
        "CREATE TABLE secrets (id INT PRIMARY KEY, body TEXT)",
        "INSERT INTO secrets VALUES (1, 'x')",
        "CREATE ROLE reader LOGIN",
        "CREATE ROLE outsider LOGIN",
        "GRANT SELECT ON secrets TO reader",
    ] {
        admin
            .batch_execute(sql)
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    let outsider = running.client("outsider").await;
    let refused = outsider
        .simple_query("SUBSCRIBE SELECT * FROM secrets")
        .await
        .unwrap_err();
    assert_eq!(
        refused.code().map(|code| code.code()),
        Some("42501"),
        "{refused}"
    );
    let inside_transaction = admin
        .batch_execute("BEGIN; SUBSCRIBE SELECT * FROM secrets")
        .await
        .unwrap_err();
    let message = inside_transaction
        .as_db_error()
        .map(|db| db.message().to_string())
        .unwrap_or_else(|| inside_transaction.to_string());
    assert!(message.contains("inside a transaction block"), "{message}");
    let describe = |error: tokio_postgres::Error| {
        format!("{error}: {:?}", error.as_db_error().map(|db| db.message()))
    };
    admin
        .batch_execute("ROLLBACK")
        .await
        .unwrap_or_else(|error| panic!("rollback: {}", describe(error)));

    let reader = running.client("reader").await;
    let cancel = reader.cancel_token();
    let stream = reader
        .query_raw("SUBSCRIBE SELECT id FROM secrets", Vec::<i32>::new())
        .await
        .unwrap_or_else(|error| panic!("reader subscribe: {}", describe(error)));
    let mut stream = pin!(stream);
    let first = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap_or_else(|error| panic!("reader first row: {}", describe(error)));
    assert_eq!(first.get::<_, String>(1), "+");
    assert_eq!(first.get::<_, i32>(2), 1);
    // Changes to other rows are evaluated for those rows only; a row that
    // leaves the filter is a removal.
    admin
        .batch_execute(
            "INSERT INTO secrets VALUES (2, 'y'); UPDATE secrets SET body = 'z' WHERE id = 1",
        )
        .await
        .unwrap();
    let mut seen = Vec::new();
    while seen.len() < 1 {
        let row = tokio::time::timeout(Duration::from_secs(10), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        seen.push((row.get::<_, String>(1), row.get::<_, i32>(2)));
    }
    assert_eq!(seen, vec![("+".to_string(), 2)]);
    let (_, full, incremental) = running.server.live_query_stats();
    assert!(
        full == 0 && incremental >= 1,
        "full={full} incremental={incremental}"
    );
    cancel.cancel_query(NoTls).await.unwrap();
    while let Some(Ok(_)) = stream.next().await {}
    running.stop();
}

/// Drives a tokio-postgres connection and collects its notifications.
fn notifying_client(
    config: String,
) -> impl std::future::Future<
    Output = (
        Client,
        tokio::sync::mpsc::UnboundedReceiver<tokio_postgres::Notification>,
    ),
> {
    async move {
        let (client, mut connection) = tokio_postgres::connect(&config, NoTls).await.unwrap();
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut messages =
                futures_util::stream::poll_fn(move |context| connection.poll_message(context));
            while let Some(message) = messages.next().await {
                match message {
                    Ok(tokio_postgres::AsyncMessage::Notification(notification)) => {
                        let _ = sender.send(notification);
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        });
        (client, receiver)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registered_subscriptions_share_a_connection_and_notify() {
    let dir = tempfile::tempdir().unwrap();
    let running = Running::start(dir.path());
    let writer = running.client("bicdb").await;
    seed_feed(&writer).await;
    let config = format!(
        "host=127.0.0.1 port={} user=bicdb dbname=bicdb",
        running.address.port()
    );
    let (api, mut notifications) = notifying_client(config).await;

    // One connection, one subscription per user; each returns its initial
    // result like the streaming form.
    let bob = api
        .query(
            "SUBSCRIBE feed_bob AS SELECT id, body FROM posts WHERE author = $1",
            &[&"bob"],
        )
        .await
        .unwrap();
    assert_eq!(bob.len(), 1);
    assert_eq!(bob[0].get::<_, String>(1), "+");
    assert_eq!(bob[0].get::<_, i32>(2), 1);
    let carol = api
        .query(
            "SUBSCRIBE feed_carol AS SELECT id, body FROM posts WHERE author = 'carol'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(carol.len(), 1);
    let (open, _, _) = running.server.live_query_stats();
    assert_eq!(open, 2);

    // A post by bob notifies feed_bob only; dave notifies nobody.
    writer
        .batch_execute("INSERT INTO posts VALUES (4, 'bob', 'second'); INSERT INTO posts VALUES (5, 'dave', 'nobody')")
        .await
        .unwrap();
    let notification = tokio::time::timeout(Duration::from_secs(10), notifications.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(notification.channel(), "feed_bob");
    let payload: serde_json::Value = serde_json::from_str(notification.payload()).unwrap();
    assert_eq!(payload["change"], "+");
    assert_eq!(payload["row"]["id"], "4");
    assert_eq!(payload["row"]["body"], "second");
    assert!(payload["commit_seq"].is_number());
    // An edit on carol's post: a removal and an addition on feed_carol.
    writer
        .batch_execute("UPDATE posts SET body = 'edited' WHERE id = 3")
        .await
        .unwrap();
    let mut changes = Vec::new();
    for _ in 0..2 {
        let notification = tokio::time::timeout(Duration::from_secs(10), notifications.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(notification.channel(), "feed_carol");
        let payload: serde_json::Value = serde_json::from_str(notification.payload()).unwrap();
        changes.push((
            payload["change"].as_str().unwrap().to_string(),
            payload["row"]["body"].as_str().unwrap().to_string(),
        ));
    }
    assert_eq!(
        changes,
        vec![
            ("-".to_string(), "hi".to_string()),
            ("+".to_string(), "edited".to_string())
        ]
    );
    // Nothing arrived for dave's post.
    assert!(
        tokio::time::timeout(Duration::from_millis(800), notifications.recv())
            .await
            .is_err()
    );

    // UNSUBSCRIBE stops one; dropping the connection drops the rest.
    api.batch_execute("UNSUBSCRIBE feed_bob").await.unwrap();
    assert_eq!(running.server.live_query_stats().0, 1);
    let refused = api.batch_execute("UNSUBSCRIBE feed_bob").await.unwrap_err();
    assert!(
        refused
            .as_db_error()
            .map(|db| db.message().contains("no subscription named feed_bob"))
            .unwrap_or(false),
        "{refused}"
    );
    drop(api);
    let cleared = tokio::time::timeout(Duration::from_secs(10), async {
        while running.server.live_query_stats().0 != 0 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(
        cleared.is_ok(),
        "subscriptions of a closed connection linger"
    );
    running.stop();
}
