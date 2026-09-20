//! ANALYZE examines a bounded sample of each table.
//!
//! Before this, every ANALYZE pass materialized the whole table (several times
//! over for typed columns); on the 16-warehouse TPC-C seed that took the
//! loader from 13 GB to over 50 GB resident and the kernel killed it. Tables
//! at or under the sample limit are still analyzed exactly; larger ones get
//! PostgreSQL-style scaled estimates, deterministic across repeated runs.

use bicdb_core::{analyze_sample_limit, BicDb, DbConfig, IndexField, StorageMode};
use bicdb_sql::SqlSession;
use tempfile::TempDir;

const ROWS: usize = 35_000;
const GROUPS: usize = 10;

fn open(dir: &TempDir, mode: &StorageMode) -> BicDb {
    BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(mode.clone()),
    )
    .unwrap_or_else(|error| panic!("open in {mode} failed: {error}"))
}

fn seeded(mode: &StorageMode) -> (TempDir, BicDb) {
    let dir = TempDir::new().unwrap();
    let mut db = open(&dir, mode);
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE t (id INT PRIMARY KEY, grp TEXT, n INT, price NUMERIC(12,2))")
            .unwrap();
        for chunk in (0..ROWS).collect::<Vec<_>>().chunks(1000) {
            let values = chunk
                .iter()
                .map(|index| {
                    let n = if index % 4 == 0 {
                        "NULL".to_string()
                    } else {
                        (index % 100).to_string()
                    };
                    format!("({index}, 'g{}', {n}, {}.25)", index % GROUPS, index % 500)
                })
                .collect::<Vec<_>>()
                .join(", ");
            sql.execute(&format!("INSERT INTO t VALUES {values}"))
                .unwrap();
        }
    }
    (dir, db)
}

fn column<'a>(
    stats: &'a bicdb_core::TableStatistics,
    name: &str,
) -> &'a bicdb_core::ColumnStatistics {
    let field = IndexField::MetadataPath(vec![name.to_string()]);
    stats
        .columns
        .values()
        .find(|column| column.field == field || (name == "id" && column.field == IndexField::Id))
        .unwrap_or_else(|| panic!("no statistics for column {name}: {stats:?}"))
}

fn within(actual: usize, expected: usize, tolerance: f64, what: &str) {
    let delta = (actual as f64 - expected as f64).abs() / expected as f64;
    assert!(
        delta <= tolerance,
        "{what}: {actual} is not within {tolerance:.0}% of {expected}",
        tolerance = tolerance * 100.0
    );
}

#[test]
fn sampled_analyze_estimates_scale_to_the_table_and_repeat_exactly() {
    assert!(
        ROWS > analyze_sample_limit(),
        "the table must exceed the sample"
    );
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let (_dir, mut db) = seeded(&mode);
        SqlSession::new(&mut db).execute("ANALYZE t").unwrap();
        let stats = db.table_statistics("t").cloned().unwrap();
        assert_eq!(
            stats.row_count, ROWS,
            "{mode}: row count is the whole table"
        );

        let id = column(&stats, "id");
        assert_eq!(id.row_count, ROWS);
        within(
            id.distinct_count,
            ROWS,
            0.02,
            &format!("{mode}: unique id distinct"),
        );
        assert_eq!(id.null_count, 0);

        let grp = column(&stats, "grp");
        assert_eq!(
            grp.distinct_count, GROUPS,
            "{mode}: low-cardinality distinct"
        );
        let common_total: usize = grp.most_common.iter().map(|entry| entry.count).sum();
        within(
            common_total,
            ROWS,
            0.02,
            &format!("{mode}: most-common counts cover the table"),
        );

        let n = column(&stats, "n");
        within(
            n.null_count,
            ROWS / 4,
            0.05,
            &format!("{mode}: scaled null count"),
        );
        // index % 100 with every index % 4 == 0 null: residues divisible by 4
        // never appear, leaving 75 distinct non-null values.
        within(n.distinct_count, 75, 0.02, &format!("{mode}: n distinct"));

        let price = column(&stats, "price");
        let typed = price
            .typed
            .as_ref()
            .unwrap_or_else(|| panic!("{mode}: NUMERIC column has typed statistics"));
        assert_eq!(typed.pg_type, "numeric");
        assert!(!typed.histogram_values.is_empty());
        within(
            price.distinct_count,
            500,
            0.05,
            &format!("{mode}: numeric distinct"),
        );
        let typed_total: usize = typed.most_common.iter().map(|entry| entry.count).sum();
        assert!(
            typed_total <= ROWS,
            "{mode}: scaled typed counts stay within the table"
        );

        SqlSession::new(&mut db).execute("ANALYZE t").unwrap();
        let mut again = db.table_statistics("t").cloned().unwrap();
        again.analyzed_at_unix_ms = stats.analyzed_at_unix_ms;
        assert_eq!(
            again, stats,
            "{mode}: repeated ANALYZE over unchanged data is identical"
        );
    }
}

#[test]
fn small_tables_are_analyzed_exactly() {
    let dir = TempDir::new().unwrap();
    let mut db = open(&dir, &StorageMode::EmbeddedMemory);
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE s (id INT PRIMARY KEY, grp TEXT, n INT)")
            .unwrap();
        sql.execute("INSERT INTO s VALUES (1,'a',NULL),(2,'a',1),(3,'b',2),(4,'b',NULL),(5,'c',3)")
            .unwrap();
        sql.execute("ANALYZE s").unwrap();
    }
    let stats = db.table_statistics("s").cloned().unwrap();
    assert_eq!(stats.row_count, 5);
    assert_eq!(column(&stats, "grp").distinct_count, 3);
    assert_eq!(column(&stats, "n").null_count, 2);
    assert_eq!(column(&stats, "n").distinct_count, 3);
    assert_eq!(column(&stats, "id").distinct_count, 5);
}
