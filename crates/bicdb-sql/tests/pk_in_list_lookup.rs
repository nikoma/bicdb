//! `WHERE id IN (...)` must plan as point lookups, not a full scan — the
//! production regression: a 295k-row override table was fully scanned on
//! every search because the IN list had no index route.

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
}

#[test]
fn id_in_list_plans_point_lookups_and_matches_scan_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE overrides (document_id TEXT PRIMARY KEY, state TEXT)")
        .unwrap();
    for chunk in (0..5_000i64).collect::<Vec<_>>().chunks(500) {
        let values = chunk
            .iter()
            .map(|index| format!("('doc{index:06}', 'state{}')", index % 7))
            .collect::<Vec<_>>()
            .join(", ");
        sql.execute(&format!("INSERT INTO overrides VALUES {values}"))
            .unwrap();
    }

    // The production shape: a large IN list over the primary key.
    let ids = (0..100)
        .map(|index| format!("'doc{:06}'", index * 37))
        .collect::<Vec<_>>()
        .join(", ");
    let plan = sql
        .execute(&format!(
            "EXPLAIN SELECT document_id FROM overrides WHERE document_id IN ({ids})"
        ))
        .unwrap();
    let plan_text = plan
        .rows
        .iter()
        .flat_map(|row| row.iter())
        .map(|value| value.to_cell())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan_text.contains("PrimaryKeyInLookup 100 ids"),
        "IN list must plan point lookups, got:\n{plan_text}"
    );

    let result = sql
        .execute(&format!(
            "SELECT document_id FROM overrides WHERE document_id IN ({ids}) ORDER BY document_id"
        ))
        .unwrap();
    assert_eq!(result.rows.len(), 100);
    assert_eq!(result.rows[0][0], SqlValue::String("doc000000".into()));
    assert_eq!(result.rows[99][0], SqlValue::String("doc003663".into()));

    // Missing ids simply match nothing; duplicates do not duplicate rows.
    let result = sql
        .execute(
            "SELECT document_id FROM overrides \
             WHERE document_id IN ('doc000037', 'doc000037', 'missing') ",
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);

    // Extra conjuncts recheck on the fetched rows.
    let result = sql
        .execute(&format!(
            "SELECT count(*) FROM overrides WHERE document_id IN ({ids}) AND state = 'state0'"
        ))
        .unwrap();
    let SqlValue::Int(filtered) = result.rows[0][0] else {
        panic!("count returned {:?}", result.rows[0][0]);
    };
    assert!(filtered > 0 && filtered < 100);

    // NOT IN must not take the point-lookup plan.
    let plan = sql
        .execute(
            "EXPLAIN SELECT document_id FROM overrides \
             WHERE document_id NOT IN ('doc000000', 'doc000001')",
        )
        .unwrap();
    let plan_text = plan
        .rows
        .iter()
        .flat_map(|row| row.iter())
        .map(|value| value.to_cell())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        !plan_text.contains("PrimaryKeyInLookup"),
        "negated IN must not point-look, got:\n{plan_text}"
    );
    let result = sql
        .execute(
            "SELECT count(*) FROM overrides \
             WHERE document_id NOT IN ('doc000000', 'doc000001')",
        )
        .unwrap();
    assert_eq!(result.rows[0][0], SqlValue::Int(4_998));
}
