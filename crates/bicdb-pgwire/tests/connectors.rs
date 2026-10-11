//! Connectors to external messaging systems, end to end through a running
//! server. The outbox test always runs; the Redis and AMQP tests run when
//! `BICDB_TEST_REDIS_URL` / `BICDB_TEST_AMQP_URL` name a server (CI provides
//! both) and are skipped otherwise.
use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use bicdb_pgwire::{ConnectorsConfig, PgWireConfig, PgWireServer};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

struct Running {
    address: SocketAddr,
    server: Arc<PgWireServer>,
    thread: thread::JoinHandle<bicdb_pgwire::Result<()>>,
}

impl Running {
    fn start(path: &Path, connectors: Option<ConnectorsConfig>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let config = PgWireConfig {
            connectors,
            ..PgWireConfig::default()
        };
        let server = PgWireServer::open(path, config).unwrap();
        let serving = server.clone();
        let thread =
            thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
        Self {
            address,
            server,
            thread,
        }
    }

    async fn client(&self) -> Client {
        let config = format!(
            "host=127.0.0.1 port={} user=bicdb dbname=bicdb",
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

async fn rows(client: &Client, sql: &str) -> Vec<Vec<Option<String>>> {
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

async fn scalar(client: &Client, sql: &str) -> String {
    rows(client, sql).await[0][0].clone().unwrap_or_default()
}

async fn eventually(timeout: Duration, mut check: impl AsyncFnMut() -> bool) -> bool {
    let end = Instant::now() + timeout;
    while Instant::now() < end {
        if check().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

/// A trigger that publishes each new order with broker_publish_on_commit.
async fn create_orders_outbox(client: &Client) {
    for sql in [
        "CREATE TABLE orders (id INT PRIMARY KEY, amount INT)",
        "CREATE FUNCTION orders_outbox() RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN PERFORM broker_publish_on_commit('orders', \
         jsonb_build_object('id', NEW.id, 'amount', NEW.amount), NULL); RETURN NEW; END $$",
        "CREATE TRIGGER orders_outbox AFTER INSERT ON orders \
         FOR EACH ROW EXECUTE FUNCTION orders_outbox()",
    ] {
        client.batch_execute(sql).await.unwrap();
    }
}

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_on_commit_reaches_the_queue_through_every_server_commit_path() {
    let dir = tempfile::tempdir().unwrap();
    let running = Running::start(dir.path(), None);
    let client = running.client().await;
    create_orders_outbox(&client).await;

    client
        .simple_query(
            "SELECT broker_publish_on_commit('orders', '{\"via\": \"autocommit select\"}', NULL)",
        )
        .await
        .unwrap();
    client
        .batch_execute(
            "BEGIN; SELECT broker_publish_on_commit('orders', '{\"via\": \"explicit\"}', NULL); COMMIT;",
        )
        .await
        .unwrap();
    client
        .batch_execute("INSERT INTO orders VALUES (1, 10)")
        .await
        .unwrap();
    client
        .batch_execute("BEGIN; INSERT INTO orders VALUES (2, 20); COMMIT;")
        .await
        .unwrap();
    client
        .batch_execute("BEGIN; INSERT INTO orders VALUES (3, 30); ROLLBACK;")
        .await
        .unwrap();

    let peeked: serde_json::Value =
        serde_json::from_str(&scalar(&client, "SELECT broker_peek('orders', 0, 100)").await)
            .unwrap();
    let payloads = peeked
        .as_array()
        .unwrap()
        .iter()
        .map(|message| message["payload"].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        payloads,
        vec![
            serde_json::json!({"via": "autocommit select"}),
            serde_json::json!({"via": "explicit"}),
            serde_json::json!({"id": 1, "amount": 10}),
            serde_json::json!({"id": 2, "amount": 20}),
        ]
    );
    drop(client);
    running.stop();
}

fn connectors(value: serde_json::Value) -> ConnectorsConfig {
    let config: ConnectorsConfig = serde_json::from_value(value).unwrap();
    config.validate().unwrap();
    config
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_connectors_move_messages_both_ways() {
    let Ok(url) = std::env::var("BICDB_TEST_REDIS_URL") else {
        eprintln!("skipped: set BICDB_TEST_REDIS_URL to run against a Redis server");
        return;
    };
    let (out_stream, in_stream) = (unique("orders-out"), unique("clicks-in"));
    let dir = tempfile::tempdir().unwrap();
    let running = Running::start(
        dir.path(),
        Some(connectors(serde_json::json!({
            "sinks": [{"name": "orders-out", "queue": "orders",
                       "target": {"redis": {"url": url, "stream": out_stream}}}],
            "sources": [{"name": "clicks-in",
                         "from": {"redis": {"url": url, "stream": in_stream}},
                         "into": {"table": {"table": "clicks", "key": "id", "payload_column": "raw"}},
                         "batch": 50, "flush_ms": 50}]
        }))),
    );
    let client = running.client().await;
    create_orders_outbox(&client).await;
    client
        .batch_execute("CREATE TABLE clicks (id TEXT PRIMARY KEY, page TEXT, raw JSONB)")
        .await
        .unwrap();
    client
        .batch_execute("INSERT INTO orders VALUES (1, 10), (2, 20)")
        .await
        .unwrap();
    client
        .batch_execute("BEGIN; INSERT INTO orders VALUES (3, 30); ROLLBACK;")
        .await
        .unwrap();

    let redis = redis::Client::open(url.as_str()).unwrap();
    let mut connection = redis.get_connection().unwrap();
    // Outbox -> stream: the committed rows only.
    let delivered = eventually(Duration::from_secs(15), async || {
        redis::cmd("XLEN")
            .arg(&out_stream)
            .query::<i64>(&mut connection)
            .unwrap()
            == 2
    })
    .await;
    assert!(delivered, "the sink did not deliver both orders");
    let entries: Vec<(String, Vec<(String, String)>)> = redis::cmd("XRANGE")
        .arg(&out_stream)
        .arg("-")
        .arg("+")
        .query(&mut connection)
        .unwrap();
    let payloads = entries
        .iter()
        .map(|(_, fields)| {
            let payload = fields
                .iter()
                .find(|(field, _)| field == "payload")
                .map(|(_, value)| value.clone())
                .unwrap();
            serde_json::from_str::<serde_json::Value>(&payload).unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        payloads,
        vec![
            serde_json::json!({"id": 1, "amount": 10}),
            serde_json::json!({"id": 2, "amount": 20})
        ]
    );

    // Stream -> table, with a redelivered duplicate written once.
    for index in 0..120 {
        redis::cmd("XADD")
            .arg(&in_stream)
            .arg("*")
            .arg("id")
            .arg(format!("c{index}"))
            .arg("page")
            .arg(format!("/p{}", index % 3))
            .query::<String>(&mut connection)
            .unwrap();
    }
    redis::cmd("XADD")
        .arg(&in_stream)
        .arg("*")
        .arg("id")
        .arg("c7")
        .arg("page")
        .arg("/duplicate")
        .query::<String>(&mut connection)
        .unwrap();
    let drained = eventually(Duration::from_secs(20), async || {
        scalar(&client, "SELECT count(*) FROM clicks").await == "120"
            && redis::cmd("XPENDING")
                .arg(&in_stream)
                .arg("bicdb")
                .query::<redis::Value>(&mut connection)
                .map(|pending| match pending {
                    redis::Value::Array(parts) => {
                        matches!(parts.first(), Some(redis::Value::Int(0)))
                    }
                    _ => false,
                })
                .unwrap_or(false)
    })
    .await;
    assert!(drained, "the source did not drain the stream");
    assert_eq!(
        rows(
            &client,
            "SELECT page, raw->>'page' FROM clicks WHERE id = 'c7'"
        )
        .await,
        vec![vec![Some("/p1".to_string()), Some("/p1".to_string())]]
    );
    let status = rows(&client, "SELECT * FROM bicdb_connectors").await;
    assert_eq!(status.len(), 2, "{status:?}");
    drop(client);
    running.stop();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn amqp_connectors_move_messages_both_ways() {
    use lapin::options::{BasicGetOptions, BasicPublishOptions, QueueDeclareOptions};
    use lapin::types::FieldTable;
    use lapin::{BasicProperties, Connection, ConnectionProperties};

    let Ok(url) = std::env::var("BICDB_TEST_AMQP_URL") else {
        eprintln!("skipped: set BICDB_TEST_AMQP_URL to run against an AMQP broker");
        return;
    };
    let (out_queue, in_queue, late_queue) =
        (unique("orders-out"), unique("events-in"), unique("late"));
    let amqp = Connection::connect(&url, ConnectionProperties::default())
        .await
        .unwrap();
    let channel = amqp.create_channel().await.unwrap();
    for queue in [&out_queue, &in_queue] {
        channel
            .queue_declare(
                queue.clone().into(),
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();
    }

    let dir = tempfile::tempdir().unwrap();
    let running = Running::start(
        dir.path(),
        Some(connectors(serde_json::json!({
            "sinks": [
                {"name": "orders-out", "queue": "orders",
                 "target": {"amqp": {"url": url, "exchange": "", "routing_key": out_queue}}},
                // Nothing is bound to this routing key until later: deliveries
                // fail (unroutable) and are retried, not dropped.
                {"name": "late", "queue": "late", "retry_delay_ms": 200,
                 "target": {"amqp": {"url": url, "exchange": "", "routing_key": late_queue}}}
            ],
            "sources": [{"name": "events-in", "from": {"amqp": {"url": url, "queue": in_queue}},
                         "into": {"table": {"table": "events", "key": "id"}},
                         "batch": 25, "flush_ms": 50}]
        }))),
    );
    let client = running.client().await;
    create_orders_outbox(&client).await;
    client
        .batch_execute("CREATE TABLE events (id TEXT PRIMARY KEY, kind TEXT, n INT)")
        .await
        .unwrap();
    client
        .batch_execute("INSERT INTO orders VALUES (1, 10), (2, 20)")
        .await
        .unwrap();
    client
        .simple_query("SELECT broker_publish('late', '{\"late\": true}', NULL)")
        .await
        .unwrap();

    // Outbox -> AMQP queue, with the broker message id as message id.
    let mut received = Vec::new();
    let delivered = eventually(Duration::from_secs(15), async || {
        while let Some(message) = channel
            .basic_get(out_queue.clone().into(), BasicGetOptions { no_ack: true })
            .await
            .unwrap()
        {
            assert!(message.delivery.properties.message_id().is_some());
            received
                .push(serde_json::from_slice::<serde_json::Value>(&message.delivery.data).unwrap());
        }
        received.len() == 2
    })
    .await;
    assert!(delivered, "received {received:?}");
    assert_eq!(received[1], serde_json::json!({"id": 2, "amount": 20}));

    // AMQP queue -> table; a redelivered message id is written once.
    for index in 0..60 {
        let body = serde_json::json!({"kind": if index % 2 == 0 {"a"} else {"b"}, "n": index});
        channel
            .basic_publish(
                "".into(),
                in_queue.clone().into(),
                BasicPublishOptions::default(),
                body.to_string().as_bytes(),
                BasicProperties::default().with_message_id(format!("m{index}").into()),
            )
            .await
            .unwrap()
            .await
            .unwrap();
    }
    channel
        .basic_publish(
            "".into(),
            in_queue.clone().into(),
            BasicPublishOptions::default(),
            br#"{"kind": "duplicate", "n": 9999}"#,
            BasicProperties::default().with_message_id("m5".into()),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    let drained = eventually(Duration::from_secs(20), async || {
        scalar(&client, "SELECT count(*) FROM events").await == "60"
    })
    .await;
    assert!(drained, "the source did not drain the queue");
    assert_eq!(
        scalar(&client, "SELECT n FROM events WHERE id = 'm5'").await,
        "5"
    );

    // The unroutable sink is retrying; once a queue is bound, it delivers.
    let retrying = eventually(Duration::from_secs(10), async || {
        rows(&client, "SELECT * FROM bicdb_connectors")
            .await
            .iter()
            .any(|row| row[0].as_deref() == Some("late") && row[5].as_deref() != Some("0"))
    })
    .await;
    assert!(retrying, "the unroutable delivery was not reported");
    channel
        .queue_declare(
            late_queue.clone().into(),
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let late = eventually(Duration::from_secs(15), async || {
        channel
            .basic_get(late_queue.clone().into(), BasicGetOptions { no_ack: true })
            .await
            .unwrap()
            .is_some()
    })
    .await;
    assert!(late, "the retried delivery never arrived");
    drop(client);
    running.stop();
}
