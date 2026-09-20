//! Reopen an existing paged database and report RSS + open time — the
//! bootstrap residency per row, with no write-path residue in the number.
use bicdb_core::{BicDb, DbConfig, StorageMode};

fn rss_mib() -> f64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            return rest
                .trim()
                .trim_end_matches("kB")
                .trim()
                .parse::<f64>()
                .unwrap_or(0.0)
                / 1024.0;
        }
    }
    0.0
}

fn main() {
    let dir = std::env::args()
        .nth(1)
        .expect("usage: paged_reopen_probe <dir>");
    println!("rss_before_open={:.1}MiB", rss_mib());
    let started = std::time::Instant::now();
    let db = BicDb::open_with_config(
        &dir,
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .expect("open");
    println!(
        "open_secs={:.2} rss_after_open={:.1}MiB",
        started.elapsed().as_secs_f64(),
        rss_mib()
    );
    let got = db.get("articles", "pmid-000499999").expect("get");
    println!("point_read={} rss={:.1}MiB", got.is_some(), rss_mib());
    let report = db.residency_report().expect("residency");
    println!(
        "rows={} versions={} rows_bytes={} chains_bytes={} pk_maps_bytes={} idx_bytes={} accounted={}",
        report.record_count,
        report.version_count,
        report.rows_bytes,
        report.version_chains_bytes,
        report.primary_key_maps_bytes,
        report.secondary_indexes_bytes,
        report.accounted_bytes,
    );
    for index in &report.indexes {
        println!("index {} entries={}", index.name, index.entry_count);
    }
}
