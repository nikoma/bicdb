//! Routing functions read tables that never appear in the query.
//!
//! `shortest_path`, `route_distance` and `optimize_route` take a graph NAME
//! and resolve it to `{graph}_nodes` / `{graph}_edges`, which they scan
//! through the raw store. Because the relation is derived from an argument
//! rather than named in a FROM clause, no relation gate ever ran — and the
//! functions return the rows themselves, not just a derived number: node ids,
//! coordinates, and the graph topology, from tables SELECT denies.
//!
//! Their siblings `travel_time` and the `bicdb_*` family were caught by an
//! earlier prefix-based check. These three carry no prefix and dispatch
//! through a different evaluator, so the prefix check never saw them.

use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use bicdb_sql::SqlSession;
use serde_json::json;

fn seeded() -> (tempfile::TempDir, BicDb) {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        directory.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    db.create_collection("roads_nodes").unwrap();
    db.create_collection("roads_edges").unwrap();
    for (id, lon) in [("a", 13.40), ("b", 13.41), ("c", 13.42)] {
        db.insert(
            "roads_nodes",
            Record::new(id).with_metadata(json!({"lon": lon, "lat": 52.52})),
        )
        .unwrap();
    }
    let mut edge = |id: &str, from: &str, to: &str| {
        db.insert(
            "roads_edges",
            Record::new(id).with_metadata(json!({
                "from": from,
                "to": to,
                "distance_m": 1000.0,
                "duration_s": 60.0,
                "road_class": "residential",
            })),
        )
        .unwrap();
    };
    for (index, pair) in [("a", "b"), ("b", "c")].iter().enumerate() {
        edge(&format!("e{index}f"), pair.0, pair.1);
        edge(&format!("e{index}r"), pair.1, pair.0);
    }
    {
        let mut owner = SqlSession::new(&mut db);
        owner
            .execute("CREATE ROLE mallory LOGIN NOSUPERUSER")
            .unwrap();
    }
    (directory, db)
}

const ROUTE: &str = "SELECT route_distance('roads', ST_Point(13.40,52.52), ST_Point(13.42,52.52)) \
     FROM generate_series(1,1)";
const PATH: &str = "SELECT shortest_path('roads', ST_Point(13.40,52.52), ST_Point(13.42,52.52)) \
     FROM generate_series(1,1)";

#[test]
fn routing_functions_require_select_on_the_graph() {
    let (_directory, mut db) = seeded();
    let mut mallory = SqlSession::new_unprivileged(&mut db, "mallory");
    mallory
        .execute("SELECT * FROM roads_nodes")
        .expect_err("the control: mallory cannot read the graph tables");
    for sql in [ROUTE, PATH] {
        match mallory.execute(sql) {
            Ok(rows) => panic!("{sql} must be refused, returned {rows:?}"),
            Err(error) => {
                let rendered = format!("{error}");
                assert!(
                    rendered.contains("permission") || rendered.contains("owner"),
                    "wrong refusal for {sql}: {rendered}"
                );
            }
        }
    }
}

/// The leak was not merely "a route was computed" — the result carries the
/// source rows verbatim.
#[test]
fn shortest_path_does_not_return_node_rows_to_an_unprivileged_role() {
    let (_directory, mut db) = seeded();
    let mut mallory = SqlSession::new_unprivileged(&mut db, "mallory");
    if let Ok(rows) = mallory.execute(PATH) {
        let rendered = format!("{rows:?}");
        for leaked in ["13.4", "52.52", "node_ids"] {
            assert!(
                !rendered.contains(leaked),
                "shortest_path leaked {leaked} from an unreadable table: {rendered}"
            );
        }
    }
}

/// A granted role must still be able to route — the gate is SELECT on the
/// graph, not a superuser lockout like the `bicdb_*` family has.
#[test]
fn a_granted_role_can_still_route() {
    let (_directory, mut db) = seeded();
    {
        let mut owner = SqlSession::new(&mut db);
        owner
            .execute("GRANT SELECT ON roads_nodes TO mallory")
            .unwrap();
        owner
            .execute("GRANT SELECT ON roads_edges TO mallory")
            .unwrap();
    }
    let mut mallory = SqlSession::new_unprivileged(&mut db, "mallory");
    let rows = mallory
        .execute(ROUTE)
        .expect("a role granted SELECT on the graph must still be able to route");
    assert!(
        format!("{rows:?}").contains("2000"),
        "the route must still compute: {rows:?}"
    );
}

/// The owner is unaffected.
#[test]
fn the_owner_can_still_route() {
    let (_directory, mut db) = seeded();
    let mut owner = SqlSession::new(&mut db);
    owner
        .execute(ROUTE)
        .expect("the owner must still be able to route");
    owner
        .execute(PATH)
        .expect("the owner must still be able to route");
}
