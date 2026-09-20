//! L3: interior hole punching. After churn + VACUUM, physically allocated
//! bytes drop while file lengths stay put — the store goes sparse.
//!
//! Hole punching and `st_blocks` accounting are Unix filesystem features.
#![cfg(unix)]

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

/// Bytes actually allocated on disk under `root`, from `st_blocks`.
///
/// `bicdb_space_report()` rate-limits its directory walk behind a five second
/// cache, so two calls inside one test return the same numbers no matter what
/// happened in between -- which is why the reclaim asserted below was
/// invisible. Measure the filesystem directly instead.
fn allocated_bytes(root: &std::path::Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    fn walk(dir: &std::path::Path, total: &mut u64) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let Ok(meta) = entry.metadata() else { continue };
            if meta.is_dir() {
                walk(&entry.path(), total);
            } else {
                *total += meta.blocks() * 512;
            }
        }
    }
    let mut total = 0;
    walk(root, &mut total);
    total
}

fn space(sql: &mut SqlSession) -> serde_json::Value {
    let SqlValue::String(report) =
        sql.execute("SELECT bicdb_space_report()").unwrap().rows[0][0].clone()
    else {
        panic!("expected JSON");
    };
    serde_json::from_str(&report).unwrap()
}

#[test]
fn vacuum_punches_interior_free_space_back_to_the_filesystem() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged)
            // Punching keeps the first and last filesystem block of a page --
            // the header and the checksum trailer -- so there is only an
            // interior to return when a page is larger than two blocks. At the
            // default 8 KiB there is nothing to punch and the pass is a no-op.
            .with_paged_page_size(32_768),
    )
    .unwrap();

    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE blobs (id TEXT PRIMARY KEY, body TEXT)")
        .unwrap();
    // Enough payload to occupy many pages, then delete most of it so the
    // freed pages are interior (live catalog/index pages sit around them).
    let filler = "x".repeat(2_000);
    for chunk in (0..400i64).collect::<Vec<_>>().chunks(50) {
        let values: Vec<String> = chunk
            .iter()
            .map(|index| format!("('k{index:04}', '{filler}')"))
            .collect();
        sql.execute(&format!("INSERT INTO blobs VALUES {}", values.join(", ")))
            .unwrap();
    }
    sql.execute("DELETE FROM blobs WHERE id < 'k0380'").unwrap();

    // Checkpoint before measuring, and again after VACUUM. Punching returns
    // page-store blocks, but `allocated_file_bytes` counts the WAL too, and
    // the WAL here is several times the size of the page store: VACUUM's own
    // writes grow it by more than the punch reclaims, which hid the reclaim
    // entirely.
    drop(sql);
    db.checkpoint_for_resume().unwrap();
    let allocated_before = allocated_bytes(dir.path());
    let mut sql = SqlSession::new(&mut db);

    // VACUUM reclaims the dead versions into free pages and punches them.
    sql.execute("VACUUM").unwrap();

    drop(sql);
    db.checkpoint_for_resume().unwrap();
    let allocated_after = allocated_bytes(dir.path());
    let mut sql = SqlSession::new(&mut db);
    let after = space(&mut sql);
    let free_pages = after["paged"]["free_pages"].as_u64().unwrap();
    assert!(
        free_pages > 0,
        "vacuum must free whole pages from the churn"
    );
    assert!(
        allocated_after < allocated_before,
        "hole punching must return real blocks: {allocated_before} -> {allocated_after}"
    );

    // Explicit re-punch is an idempotent no-op-or-better.
    let report = db.punch_free_space(u64::MAX).unwrap();
    assert!(
        report.supported,
        "test filesystems (tmpfs/ext4) support punching"
    );

    // The store still works after punching: reuse the freed pages.
    let mut sql = SqlSession::new(&mut db);
    let filler2 = "y".repeat(2_000);
    for chunk in (0..200i64).collect::<Vec<_>>().chunks(50) {
        let values: Vec<String> = chunk
            .iter()
            .map(|index| format!("('r{index:04}', '{filler2}')"))
            .collect();
        sql.execute(&format!("INSERT INTO blobs VALUES {}", values.join(", ")))
            .unwrap();
    }
    let rows = sql.execute("SELECT count(*) FROM blobs").unwrap();
    assert_eq!(rows.rows[0][0], SqlValue::Int(220));
}
