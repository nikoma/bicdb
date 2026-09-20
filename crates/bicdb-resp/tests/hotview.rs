//! End-to-end HotView tests: SQL over RESP, materialized cache keys, and
//! commit-driven refresh/invalidation — all through a real TCP connection.

use std::io::{BufReader, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::time::Duration;

use bicdb_resp::{RespConfig, RespServerHandle};

fn start_server(path: &Path) -> RespServerHandle {
    bicdb_resp::start(
        path,
        RespConfig {
            port: 0,
            hotview: true,
            ..RespConfig::default()
        },
    )
    .expect("server starts")
}

#[derive(Clone, Debug, PartialEq)]
enum Reply {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Vec<u8>),
    Nil,
    Array(Vec<Reply>),
}

impl Reply {
    fn bulk_json(&self) -> serde_json::Value {
        match self {
            Reply::Bulk(bytes) => serde_json::from_slice(bytes).expect("valid JSON bulk"),
            other => panic!("expected bulk, got {other:?}"),
        }
    }

    fn int(&self) -> i64 {
        match self {
            Reply::Int(value) => *value,
            other => panic!("expected int, got {other:?}"),
        }
    }

    fn array(&self) -> &[Reply] {
        match self {
            Reply::Array(items) => items,
            other => panic!("expected array, got {other:?}"),
        }
    }
}

struct Client {
    writer: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Client {
    fn connect(handle: &RespServerHandle) -> Self {
        let stream = TcpStream::connect(handle.local_addr()).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        Self {
            writer: stream.try_clone().unwrap(),
            reader: BufReader::new(stream),
        }
    }

    fn cmd(&mut self, args: &[&str]) -> Reply {
        let mut request = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            request.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            request.extend_from_slice(arg.as_bytes());
            request.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&request).expect("send");
        self.read_reply()
    }

    fn read_reply(&mut self) -> Reply {
        let mut kind = [0u8; 1];
        self.reader.read_exact(&mut kind).expect("reply type byte");
        let line = self.read_line();
        match kind[0] {
            b'+' => Reply::Simple(line),
            b'-' => Reply::Error(line),
            b':' => Reply::Int(line.parse().expect("int reply")),
            b'$' => {
                let len: i64 = line.parse().expect("bulk length");
                if len < 0 {
                    return Reply::Nil;
                }
                let mut body = vec![0u8; len as usize + 2];
                self.reader.read_exact(&mut body).expect("bulk body");
                body.truncate(len as usize);
                Reply::Bulk(body)
            }
            b'*' => {
                let len: i64 = line.parse().expect("array length");
                if len < 0 {
                    return Reply::Nil;
                }
                Reply::Array((0..len).map(|_| self.read_reply()).collect())
            }
            other => panic!("unexpected reply type byte: {other}"),
        }
    }

    fn read_line(&mut self) -> String {
        let mut line = Vec::new();
        loop {
            let mut byte = [0u8; 1];
            self.reader.read_exact(&mut byte).expect("line byte");
            if byte[0] == b'\n' {
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                return String::from_utf8(line).expect("utf8 line");
            }
            line.push(byte[0]);
        }
    }

    /// GET a key and parse the value as JSON.
    fn get_json(&mut self, key: &str) -> serde_json::Value {
        self.cmd(&["GET", key]).bulk_json()
    }
}

fn seed_orders(client: &mut Client) {
    client.cmd(&[
        "SQL",
        "CREATE TABLE orders (id TEXT PRIMARY KEY, customer TEXT, total BIGINT, status TEXT)",
    ]);
    client.cmd(&[
        "SQL",
        "INSERT INTO orders (id, customer, total, status) VALUES \
         ('o1', 'acme', 100, 'open'), ('o2', 'acme', 250, 'open'), ('o3', 'globex', 40, 'shipped')",
    ]);
}

#[test]
fn sql_over_resp_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(dir.path());
    let mut client = Client::connect(&server);

    seed_orders(&mut client);
    let reply = client
        .cmd(&[
            "SQL",
            "SELECT id, total FROM orders WHERE customer = 'acme' ORDER BY id",
        ])
        .bulk_json();
    assert_eq!(reply["columns"], serde_json::json!(["id", "total"]));
    assert_eq!(reply["rows"], serde_json::json!([["o1", 100], ["o2", 250]]));

    match client.cmd(&["SQL", "SELECT nope FROM missing_table"]) {
        Reply::Error(message) => assert!(message.contains("SQL error"), "{message}"),
        other => panic!("expected SQL error, got {other:?}"),
    }

    drop(client);
    server.shutdown();
}

#[test]
fn hotview_refreshes_before_the_write_reply() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(dir.path());
    let mut client = Client::connect(&server);
    seed_orders(&mut client);

    let generation = client
        .cmd(&[
            "HOTVIEW.CREATE",
            "dash:acme",
            "SELECT COUNT(*) AS orders, SUM(total) AS revenue FROM orders WHERE customer = 'acme'",
        ])
        .int();
    assert_eq!(generation, 1);

    // The materialized value is a plain cache key any Redis client can GET.
    let dashboard = client.get_json("dash:acme");
    assert_eq!(
        dashboard,
        serde_json::json!([{"orders": 2, "revenue": 350}])
    );

    // A write through SQL refreshes the view before the write's own reply.
    let write = client
        .cmd(&[
            "SQL",
            "INSERT INTO orders (id, customer, total, status) VALUES ('o4', 'acme', 50, 'open')",
        ])
        .bulk_json();
    assert_eq!(write["hotviews_refreshed"], serde_json::json!(1));
    let dashboard = client.get_json("dash:acme");
    assert_eq!(
        dashboard,
        serde_json::json!([{"orders": 3, "revenue": 400}])
    );

    // UPDATE and DELETE cascade the same way.
    client.cmd(&["SQL", "UPDATE orders SET total = 200 WHERE id = 'o1'"]);
    assert_eq!(
        client.get_json("dash:acme"),
        serde_json::json!([{"orders": 3, "revenue": 500}])
    );
    client.cmd(&["SQL", "DELETE FROM orders WHERE id = 'o4'"]);
    assert_eq!(
        client.get_json("dash:acme"),
        serde_json::json!([{"orders": 2, "revenue": 450}])
    );

    // Writes to unrelated tables do not touch the view.
    let unrelated = client
        .cmd(&[
            "SQL",
            "CREATE TABLE audit_log (id TEXT PRIMARY KEY, note TEXT)",
        ])
        .bulk_json();
    assert_eq!(unrelated["hotviews_refreshed"], serde_json::json!(0));
    let unrelated = client
        .cmd(&["SQL", "INSERT INTO audit_log (id, note) VALUES ('a1', 'x')"])
        .bulk_json();
    assert_eq!(unrelated["hotviews_refreshed"], serde_json::json!(0));

    drop(client);
    server.shutdown();
}

#[test]
fn cascading_invalidation_multiple_views_and_joins() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(dir.path());
    let mut client = Client::connect(&server);

    client.cmd(&[
        "SQL",
        "CREATE TABLE products (id TEXT PRIMARY KEY, name TEXT, price BIGINT, category TEXT)",
    ]);
    client.cmd(&[
        "SQL",
        "CREATE TABLE cart_items (id TEXT PRIMARY KEY, product_id TEXT, qty BIGINT)",
    ]);
    client.cmd(&[
        "SQL",
        "INSERT INTO products (id, name, price, category) VALUES \
         ('p1', 'widget', 10, 'tools'), ('p2', 'gadget', 30, 'tools')",
    ]);
    client.cmd(&[
        "SQL",
        "INSERT INTO cart_items (id, product_id, qty) VALUES ('c1', 'p1', 3)",
    ]);

    // Three views over the same underlying data: product page, category page,
    // and a JOIN (cart totals) spanning both tables.
    client.cmd(&[
        "HOTVIEW.CREATE",
        "page:product:p1",
        "SELECT name, price FROM products WHERE id = 'p1'",
    ]);
    client.cmd(&[
        "HOTVIEW.CREATE",
        "page:category:tools",
        "SELECT COUNT(*) AS n, SUM(price) AS total FROM products WHERE category = 'tools'",
    ]);
    client.cmd(&[
        "HOTVIEW.CREATE",
        "cart:totals",
        "SELECT SUM(products.price * cart_items.qty) AS amount \
         FROM cart_items JOIN products ON products.id = cart_items.product_id",
    ]);

    assert_eq!(
        client.get_json("cart:totals"),
        serde_json::json!([{"amount": 30}])
    );

    // One price change cascades into all three cached pages at once.
    let write = client
        .cmd(&["SQL", "UPDATE products SET price = 20 WHERE id = 'p1'"])
        .bulk_json();
    assert_eq!(write["hotviews_refreshed"], serde_json::json!(3));
    assert_eq!(
        client.get_json("page:product:p1"),
        serde_json::json!([{"name": "widget", "price": 20}])
    );
    assert_eq!(
        client.get_json("page:category:tools"),
        serde_json::json!([{"n": 2, "total": 50}])
    );
    assert_eq!(
        client.get_json("cart:totals"),
        serde_json::json!([{"amount": 60}])
    );

    // The JOIN view also tracks its second table.
    client.cmd(&["SQL", "UPDATE cart_items SET qty = 1 WHERE id = 'c1'"]);
    assert_eq!(
        client.get_json("cart:totals"),
        serde_json::json!([{"amount": 20}])
    );

    drop(client);
    server.shutdown();
}

#[test]
fn invalidate_mode_deletes_until_manual_refresh() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(dir.path());
    let mut client = Client::connect(&server);
    seed_orders(&mut client);

    client.cmd(&[
        "HOTVIEW.CREATE",
        "report:open",
        "SELECT COUNT(*) AS open_orders FROM orders WHERE status = 'open'",
        "MODE",
        "invalidate",
    ]);
    assert_eq!(
        client.get_json("report:open"),
        serde_json::json!([{"open_orders": 2}])
    );

    let write = client
        .cmd(&[
            "SQL",
            "UPDATE orders SET status = 'shipped' WHERE id = 'o1'",
        ])
        .bulk_json();
    assert_eq!(write["hotviews_invalidated"], serde_json::json!(1));
    // Invalidate mode: the stale value is gone, not silently served.
    assert_eq!(client.cmd(&["GET", "report:open"]), Reply::Nil);

    let generation = client.cmd(&["HOTVIEW.REFRESH", "report:open"]).int();
    assert!(generation >= 2);
    assert_eq!(
        client.get_json("report:open"),
        serde_json::json!([{"open_orders": 1}])
    );

    drop(client);
    server.shutdown();
}

#[test]
fn hotview_lifecycle_list_status_drop() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(dir.path());
    let mut client = Client::connect(&server);
    seed_orders(&mut client);

    client.cmd(&["HOTVIEW.CREATE", "v1", "SELECT COUNT(*) AS n FROM orders"]);
    client.cmd(&[
        "HOTVIEW.CREATE",
        "v2",
        "SELECT COUNT(*) AS n FROM orders WHERE status = 'open'",
    ]);

    let listed = client.cmd(&["HOTVIEW.LIST"]);
    let names: Vec<String> = listed
        .array()
        .iter()
        .map(|reply| match reply {
            Reply::Bulk(bytes) => String::from_utf8(bytes.clone()).unwrap(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(names, vec!["v1".to_string(), "v2".to_string()]);

    let status = client.cmd(&["HOTVIEW.STATUS", "v1"]);
    let fields = status.array();
    let mut map = std::collections::HashMap::new();
    for pair in fields.chunks(2) {
        if let [Reply::Bulk(field), Reply::Bulk(value)] = pair {
            map.insert(
                String::from_utf8(field.clone()).unwrap(),
                String::from_utf8(value.clone()).unwrap(),
            );
        }
    }
    assert_eq!(map["deps"], "orders");
    assert_eq!(map["mode"], "refresh");
    assert_eq!(map["generation"], "1");
    assert_eq!(map["stale"], "false");

    assert_eq!(client.cmd(&["HOTVIEW.DROP", "v2"]).int(), 1);
    assert_eq!(client.cmd(&["HOTVIEW.DROP", "v2"]).int(), 0);
    assert_eq!(client.cmd(&["GET", "v2"]), Reply::Nil);
    // Dropped views no longer refresh on writes.
    let write = client
        .cmd(&[
            "SQL",
            "INSERT INTO orders (id, customer, total, status) VALUES ('o9', 'x', 1, 'open')",
        ])
        .bulk_json();
    assert_eq!(write["hotviews_refreshed"], serde_json::json!(1));

    // Validation errors.
    match client.cmd(&["HOTVIEW.CREATE", "bad", "DELETE FROM orders"]) {
        Reply::Error(message) => assert!(message.contains("SELECT"), "{message}"),
        other => panic!("expected error, got {other:?}"),
    }
    match client.cmd(&["HOTVIEW.STATUS", "missing"]) {
        Reply::Error(message) => assert!(message.contains("no such hotview"), "{message}"),
        other => panic!("expected error, got {other:?}"),
    }

    drop(client);
    server.shutdown();
}

#[test]
fn disabled_by_default_rejects_hotview_and_sql() {
    let dir = tempfile::tempdir().unwrap();
    // Default config: hotview off.
    let server = bicdb_resp::start(
        dir.path(),
        RespConfig {
            port: 0,
            ..RespConfig::default()
        },
    )
    .expect("server starts");
    let mut client = Client::connect(&server);

    for request in [
        vec!["SQL", "SELECT 1"],
        vec!["HOTVIEW.CREATE", "k", "SELECT 1 FROM t"],
        vec!["HOTVIEW.LIST"],
        vec!["HOTVIEW.STATUS", "k"],
        vec!["HOTVIEW.REFRESH", "k"],
        vec!["HOTVIEW.DROP", "k"],
    ] {
        match client.cmd(&request) {
            Reply::Error(message) => {
                assert!(message.contains("disabled"), "{request:?}: {message}")
            }
            other => panic!("{request:?} should be rejected, got {other:?}"),
        }
    }
    // The plain cache surface is unaffected.
    assert_eq!(client.cmd(&["SET", "k", "v"]), Reply::Simple("OK".into()));
    assert_eq!(client.cmd(&["GET", "k"]), Reply::Bulk(b"v".to_vec()));

    drop(client);
    server.shutdown();
}

#[test]
fn definitions_stay_dormant_while_disabled_and_revive_on_enable() {
    let dir = tempfile::tempdir().unwrap();

    // Create a view with hotview on.
    let server = start_server(dir.path());
    let mut client = Client::connect(&server);
    seed_orders(&mut client);
    client.cmd(&["HOTVIEW.CREATE", "dash", "SELECT COUNT(*) AS n FROM orders"]);
    assert_eq!(client.get_json("dash"), serde_json::json!([{"n": 3}]));
    drop(client);
    server.shutdown();

    // Restart with hotview off: the materialized value is still a plain,
    // readable key, but nothing recomputes and the definition lies dormant.
    let server = bicdb_resp::start(
        dir.path(),
        RespConfig {
            port: 0,
            ..RespConfig::default()
        },
    )
    .expect("server starts");
    let mut client = Client::connect(&server);
    assert_eq!(client.get_json("dash"), serde_json::json!([{"n": 3}]));
    match client.cmd(&["HOTVIEW.STATUS", "dash"]) {
        Reply::Error(message) => assert!(message.contains("disabled"), "{message}"),
        other => panic!("expected disabled error, got {other:?}"),
    }
    drop(client);
    server.shutdown();

    // Re-enable: the definition revives, recomputes at startup, and the
    // dependency graph fires again.
    let server = start_server(dir.path());
    let mut client = Client::connect(&server);
    client.cmd(&["SQL", "DELETE FROM orders WHERE id = 'o1'"]);
    assert_eq!(client.get_json("dash"), serde_json::json!([{"n": 2}]));
    drop(client);
    server.shutdown();
}

#[test]
fn ephemeral_cache_still_restart_hot_via_hotview() {
    let dir = tempfile::tempdir().unwrap();
    let config = RespConfig {
        port: 0,
        hotview: true,
        ephemeral: true,
        ..RespConfig::default()
    };

    let server = bicdb_resp::start(dir.path(), config.clone()).expect("server starts");
    let mut client = Client::connect(&server);
    seed_orders(&mut client);
    client.cmd(&["SET", "plain", "memory-only"]);
    client.cmd(&["HOTVIEW.CREATE", "dash", "SELECT COUNT(*) AS n FROM orders"]);
    assert_eq!(client.get_json("dash"), serde_json::json!([{"n": 3}]));
    drop(client);
    server.shutdown();

    let server = bicdb_resp::start(dir.path(), config).expect("server starts");
    let mut client = Client::connect(&server);
    // The plain key died with the process; the derived key is back and hot,
    // recomputed from the durable SQL tables at startup.
    assert_eq!(client.cmd(&["GET", "plain"]), Reply::Nil);
    assert_eq!(client.get_json("dash"), serde_json::json!([{"n": 3}]));
    // And the dependency graph still fires.
    client.cmd(&["SQL", "DELETE FROM orders WHERE id = 'o1'"]);
    assert_eq!(client.get_json("dash"), serde_json::json!([{"n": 2}]));
    drop(client);
    server.shutdown();
}

#[test]
fn hotviews_survive_restart_and_come_up_hot() {
    let dir = tempfile::tempdir().unwrap();

    let server = start_server(dir.path());
    let mut client = Client::connect(&server);
    seed_orders(&mut client);
    client.cmd(&["HOTVIEW.CREATE", "dash", "SELECT COUNT(*) AS n FROM orders"]);
    assert_eq!(client.get_json("dash"), serde_json::json!([{"n": 3}]));
    drop(client);
    server.shutdown();

    let server = start_server(dir.path());
    let mut client = Client::connect(&server);
    // Recomputed at startup: still served, still correct.
    assert_eq!(client.get_json("dash"), serde_json::json!([{"n": 3}]));
    // And the dependency graph still fires after restart.
    client.cmd(&["SQL", "DELETE FROM orders WHERE id = 'o3'"]);
    assert_eq!(client.get_json("dash"), serde_json::json!([{"n": 2}]));
    let status = client.cmd(&["HOTVIEW.STATUS", "dash"]);
    assert!(!status.array().is_empty());

    drop(client);
    server.shutdown();
}
