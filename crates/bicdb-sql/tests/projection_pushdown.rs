//! Projection pushdown: a single-relation SELECT converts only the stored
//! fields it references into SQL values, instead of every column of the row.
//!
//! The pushdown is invisible when it works, so these tests pin the shapes
//! that must still see every column (wildcards, whole-row references, locks)
//! and the shapes that reference columns outside the projection (WHERE,
//! ORDER BY, aliases, qualified names, JSON paths) — each asserting the value
//! a dropped column would have made wrong or unresolvable.

use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};
use serde_json::json;
use tempfile::TempDir;

fn seeded(mode: &StorageMode) -> (TempDir, BicDb) {
    let dir = TempDir::new().unwrap();
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(mode.clone()),
    )
    .unwrap_or_else(|error| panic!("open in {mode} failed: {error}"));
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute(
            "CREATE TABLE wide (id INT PRIMARY KEY, a TEXT, b INT, c INT, d TEXT, e INT, \
             \"MixedCase\" INT, payload JSONB)",
        )
        .unwrap();
        for i in 1..=20 {
            sql.execute(&format!(
                "INSERT INTO wide VALUES ({i}, 'a{i}', {}, {}, 'd{i}', {}, {}, '{{\"k\": {i}, \"s\": \"s{i}\"}}')",
                i * 10,
                i * 100,
                i % 3,
                i * 7
            ))
            .unwrap();
        }
    }
    (dir, db)
}

fn run(db: &mut BicDb, sql: &str) -> Vec<Vec<SqlValue>> {
    SqlSession::new(db)
        .execute(sql)
        .unwrap_or_else(|error| panic!("`{sql}` failed: {error}"))
        .rows
}

fn int(value: &SqlValue) -> i64 {
    match value {
        SqlValue::Int(v) => *v,
        other => panic!("{other:?} is not an integer"),
    }
}

fn text(value: &SqlValue) -> String {
    match value {
        SqlValue::String(v) => v.to_string(),
        other => panic!("{other:?} is not text"),
    }
}

fn modes() -> Vec<StorageMode> {
    vec![StorageMode::default(), StorageMode::ServerPaged]
}

#[test]
fn projected_columns_only_still_returns_the_right_values() {
    for mode in modes() {
        let (_dir, mut db) = seeded(&mode);
        let rows = run(&mut db, "SELECT a, c FROM wide WHERE id = 7");
        assert_eq!(rows.len(), 1);
        assert_eq!(text(&rows[0][0]), "a7");
        assert_eq!(int(&rows[0][1]), 700);
    }
}

#[test]
fn where_and_order_by_columns_outside_the_projection_resolve() {
    for mode in modes() {
        let (_dir, mut db) = seeded(&mode);
        // b and e are not projected; the filter and the sort still see them.
        let rows = run(
            &mut db,
            "SELECT a FROM wide WHERE b > 150 AND e = 1 ORDER BY b DESC LIMIT 2",
        );
        let got: Vec<String> = rows.iter().map(|r| text(&r[0])).collect();
        assert_eq!(got, vec!["a19", "a16"]);
    }
}

#[test]
fn qualified_aliased_and_mixed_case_references_resolve() {
    for mode in modes() {
        let (_dir, mut db) = seeded(&mode);
        let rows = run(
            &mut db,
            "SELECT w.d AS label, w.\"MixedCase\" + 1 AS m FROM wide AS w WHERE w.id = 3",
        );
        assert_eq!(text(&rows[0][0]), "d3");
        assert_eq!(int(&rows[0][1]), 22);
        let rows = run(&mut db, "SELECT wide.c FROM wide WHERE wide.a = 'a4'");
        assert_eq!(int(&rows[0][0]), 400);
    }
}

#[test]
fn json_paths_reference_their_base_column() {
    for mode in modes() {
        let (_dir, mut db) = seeded(&mode);
        let rows = run(
            &mut db,
            "SELECT (payload->>'k')::int FROM wide WHERE (payload->>'k')::int = 12",
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(int(&rows[0][0]), 12);
    }
}

#[test]
fn wildcards_keep_every_column() {
    for mode in modes() {
        let (_dir, mut db) = seeded(&mode);
        let rows = run(&mut db, "SELECT * FROM wide WHERE id = 2");
        assert_eq!(rows[0].len(), 8, "SELECT * must return every column");
        let rows = run(&mut db, "SELECT w.* FROM wide w WHERE id = 2");
        assert_eq!(
            rows[0].len(),
            8,
            "qualified wildcard must return every column"
        );
        let rows = run(&mut db, "SELECT count(*) FROM wide WHERE e = 0");
        assert_eq!(int(&rows[0][0]), 6);
    }
}

#[test]
fn whole_row_references_keep_every_column() {
    for mode in modes() {
        let (_dir, mut db) = seeded(&mode);
        let rows = run(&mut db, "SELECT row_to_json(w) FROM wide w WHERE id = 5");
        let json = match &rows[0][0] {
            SqlValue::Json(v) => v.to_string(),
            SqlValue::String(v) => v.to_string(),
            other => format!("{other:?}"),
        };
        for column in ["\"a\"", "\"b\"", "\"c\"", "\"d\"", "\"e\""] {
            assert!(json.contains(column), "row_to_json lost {column}: {json}");
        }
    }
}

#[test]
fn projection_alias_reused_in_order_by_and_subquery_outer_reference() {
    for mode in modes() {
        let (_dir, mut db) = seeded(&mode);
        let rows = run(
            &mut db,
            "SELECT c AS hundreds FROM wide WHERE id <= 3 ORDER BY hundreds DESC",
        );
        let got: Vec<i64> = rows.iter().map(|r| int(&r[0])).collect();
        assert_eq!(got, vec![300, 200, 100]);
        let rows = run(
            &mut db,
            "SELECT a FROM wide o WHERE EXISTS (SELECT 1 FROM wide i WHERE i.b = o.c / 10 AND i.id = 9)",
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(text(&rows[0][0]), "a9");
    }
}

#[test]
fn for_update_keeps_every_column() {
    for mode in modes() {
        let (_dir, mut db) = seeded(&mode);
        let mut sql = SqlSession::new(&mut db);
        sql.execute("BEGIN").unwrap();
        let rows = sql
            .execute("SELECT a FROM wide WHERE id = 8 FOR UPDATE")
            .unwrap()
            .rows;
        assert_eq!(text(&rows[0][0]), "a8");
        sql.execute("UPDATE wide SET b = b + 1 WHERE id = 8")
            .unwrap();
        sql.execute("COMMIT").unwrap();
        let rows = sql.execute("SELECT b FROM wide WHERE id = 8").unwrap().rows;
        assert_eq!(int(&rows[0][0]), 81);
    }
}

#[test]
fn dotted_json_key_access_keeps_the_base_column() {
    // `metadata.clinic` on a schemaless collection is field `metadata` plus a
    // JSON key, not a qualified column named `clinic`: the collector must keep
    // the first part of the compound identifier, or the filter sees NULLs.
    let dir = TempDir::new().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    db.create_collection("patients").unwrap();
    db.batch_insert(
        "patients",
        [
            Record::new("patient-a").with_metadata(json!({"clinic": "rural-7", "risk": 0.8})),
            Record::new("patient-b").with_metadata(json!({"clinic": "urban-2", "risk": 0.2})),
            Record::new("patient-c").with_metadata(json!({"clinic": "rural-7", "risk": 0.4})),
        ],
    )
    .unwrap();
    let rows = run(
        &mut db,
        "SELECT id FROM patients WHERE metadata.clinic = 'rural-7' ORDER BY id",
    );
    let got: Vec<String> = rows.iter().map(|r| text(&r[0])).collect();
    assert_eq!(got, vec!["patient-a", "patient-c"]);
    let rows = run(
        &mut db,
        "SELECT id, metadata.clinic FROM patients WHERE id = 'patient-b'",
    );
    assert_eq!(text(&rows[0][1]), "urban-2");
    let rows = run(
        &mut db,
        "SELECT count(*), max(metadata.risk) FROM patients WHERE metadata.clinic = 'rural-7'",
    );
    assert_eq!(int(&rows[0][0]), 2);
    assert_ne!(rows[0][1], SqlValue::Null);
}
