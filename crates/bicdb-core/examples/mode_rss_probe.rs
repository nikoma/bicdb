//! Measures resident memory for the SAME dataset under both storage engines.
use bicdb_core::storage_mode_migration::{migrate_storage_mode, DEFAULT_MIGRATION_BATCH};
use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use serde_json::json;

fn rss_mib() -> f64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: f64 = rest
                .trim()
                .trim_end_matches(" kB")
                .trim()
                .parse()
                .unwrap_or(0.0);
            return kb / 1024.0;
        }
    }
    0.0
}

fn dir_mib(path: &std::path::Path) -> f64 {
    fn walk(p: &std::path::Path) -> u64 {
        let mut total = 0;
        if let Ok(entries) = std::fs::read_dir(p) {
            for e in entries.flatten() {
                let path = e.path();
                if path.is_dir() {
                    total += walk(&path);
                } else if let Ok(m) = e.metadata() {
                    total += m.len();
                }
            }
        }
        total
    }
    walk(path) as f64 / (1024.0 * 1024.0)
}

fn main() {
    let rows: usize = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(20_000);
    let payload_kib: usize = std::env::args()
        .nth(2)
        .and_then(|a| a.parse().ok())
        .unwrap_or(10);
    let base = tempfile::tempdir().unwrap();
    let src = base.path().join("embedded");
    let dst = base.path().join("paged");
    let body = "x".repeat(payload_kib * 1024);

    println!("rows={rows} payload={payload_kib}KiB");
    println!("baseline RSS            {:>8.0} MiB", rss_mib());

    {
        let mut db = BicDb::open_with_config(
            &src,
            DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(StorageMode::EmbeddedMemory),
        )
        .unwrap();
        db.create_collection("landings").unwrap();
        db.bulk_load_insert(
            "landings",
            (0..rows).map(|i| {
                Record::new(format!("id-{i:08}")).with_metadata(json!({
                    "source_message_id": format!("src-{i:08}"),
                    "payload": {"body": body, "n": i},
                }))
            }),
        )
        .unwrap();
        db.close().unwrap();
    }
    println!("on-disk (embedded)      {:>8.0} MiB", dir_mib(&src));

    let embedded_open = {
        let db = BicDb::open_with_config(
            &src,
            DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(StorageMode::EmbeddedMemory),
        )
        .unwrap();
        let rss = rss_mib();
        drop(db);
        rss
    };
    println!(
        "RSS: embedded OPEN      {:>8.0} MiB   <-- all rows resident",
        embedded_open
    );

    let report = migrate_storage_mode(
        &src,
        &dst,
        StorageMode::ServerPaged,
        DEFAULT_MIGRATION_BATCH,
    )
    .unwrap();
    println!("migrated {} records", report.records);
    println!("on-disk (paged)         {:>8.0} MiB", dir_mib(&dst));

    // Fresh process-like measurement: the migration itself touched memory, so
    // report the delta of opening the paged store from the post-migration floor.
    let floor = rss_mib();
    let paged_open = {
        let db = BicDb::open_with_config(
            &dst,
            DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(StorageMode::ServerPaged),
        )
        .unwrap();
        let rss = rss_mib();
        let n = db.scan_collection("landings").map(|r| r.len()).unwrap_or(0);
        println!("(paged scan sees {n} rows)");
        drop(db);
        rss
    };
    println!("RSS floor after migrate {:>8.0} MiB", floor);
    println!("RSS: paged OPEN         {:>8.0} MiB", paged_open);
}
