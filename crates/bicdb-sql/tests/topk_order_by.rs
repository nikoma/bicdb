//! Bounded top-K ordering: `ORDER BY <ranked expr> LIMIT k` selects the k
//! best rows instead of ranking the whole input, and the KNN plan carries the
//! query's real LIMIT when nothing downstream can drop rows.

use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn ids(result: &bicdb_sql::SqlResult) -> Vec<String> {
    result
        .rows
        .iter()
        .map(|row| match &row[0] {
            SqlValue::String(id) => id.clone(),
            other => panic!("unexpected {other:?}"),
        })
        .collect()
}

#[test]
fn ts_rank_order_with_limit_returns_the_true_top_k_in_rank_order() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)")
        .unwrap();
    // Distinct ranks: r5 mentions the term 5 times, r4 four times, ...
    for count in 1..=5 {
        let body = std::iter::repeat("ashwagandha")
            .take(count)
            .collect::<Vec<_>>()
            .join(" trial ");
        sql.execute(&format!("INSERT INTO docs VALUES ('r{count}', '{body}')"))
            .unwrap();
    }
    sql.execute("INSERT INTO docs VALUES ('none', 'valerian only')")
        .unwrap();

    let top = sql
        .execute(
            "SELECT id FROM docs \
             WHERE to_tsvector('english', body) @@ to_tsquery('english', 'ashwagandha') \
             ORDER BY ts_rank(to_tsvector('english', body), to_tsquery('english', 'ashwagandha')) DESC \
             LIMIT 3",
        )
        .unwrap();
    assert_eq!(ids(&top), ["r5", "r4", "r3"]);

    // OFFSET participates in the keep bound: rows ranked 3rd and 4th.
    let paged = sql
        .execute(
            "SELECT id FROM docs \
             WHERE to_tsvector('english', body) @@ to_tsquery('english', 'ashwagandha') \
             ORDER BY ts_rank(to_tsvector('english', body), to_tsquery('english', 'ashwagandha')) DESC \
             LIMIT 2 OFFSET 2",
        )
        .unwrap();
    assert_eq!(ids(&paged), ["r3", "r2"]);

    // LIMIT 0 is a valid keep bound.
    let empty = sql
        .execute(
            "SELECT id FROM docs \
             ORDER BY ts_rank(to_tsvector('english', body), to_tsquery('english', 'ashwagandha')) DESC \
             LIMIT 0",
        )
        .unwrap();
    assert!(empty.rows.is_empty());

    // No LIMIT: the full ranking still comes back complete and ordered.
    let all = sql
        .execute(
            "SELECT id FROM docs \
             WHERE to_tsvector('english', body) @@ to_tsquery('english', 'ashwagandha') \
             ORDER BY ts_rank(to_tsvector('english', body), to_tsquery('english', 'ashwagandha')) DESC",
        )
        .unwrap();
    assert_eq!(ids(&all), ["r5", "r4", "r3", "r2", "r1"]);
}

#[test]
fn knn_query_returns_the_true_nearest_in_distance_order() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE places (id TEXT PRIMARY KEY, spot POINT, kind TEXT)")
        .unwrap();
    for index in 0..20 {
        sql.execute(&format!(
            "INSERT INTO places VALUES ('p{index:02}', point({index}, {index}), 'cafe')"
        ))
        .unwrap();
    }
    sql.execute("CREATE INDEX idx_places_spot ON places USING GIST (spot)")
        .unwrap();

    // Semantics regardless of plan: 4 nearest, in distance order.
    let rows = sql
        .execute("SELECT id FROM places ORDER BY spot <-> point(0,0) LIMIT 4")
        .unwrap();
    assert_eq!(ids(&rows), ["p00", "p01", "p02", "p03"]);
}
