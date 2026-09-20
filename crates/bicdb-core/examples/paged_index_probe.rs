//! Measures what an index costs an indexed paged collection at open.
//!
//! Usage: paged_index_probe <dir> <records>
//!
//! Ingests, creates an index, then reopens twice, timing each open. The
//! second reopen is the steady state: stubs from identity-only decode,
//! index loaded from durable entries.

use bicdb_core::{BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, Record, StorageMode};
use serde_json::json;

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

fn config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
}

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args
        .next()
        .expect("usage: paged_index_probe <dir> <records>");
    let total: usize = args.next().expect("records").parse().expect("number");

    {
        let mut db = BicDb::open_with_config(&dir, config()).expect("open");
        db.create_collection("articles").expect("create");
        let started = std::time::Instant::now();
        let mut ingested = 0usize;
        while ingested < total {
            let count = 2_000.min(total - ingested);
            let records: Vec<Record> = (ingested..ingested + count)
                .map(|i| {
                    Record::new(format!("pmid-{i:09}")).with_metadata(json!({
                        "title": format!("Study {i}"),
                        "abstract": "Background and methods. ".repeat(20),
                        "year": 1990 + (i % 36),
                    }))
                })
                .collect();
            db.batch_insert("articles", records).expect("insert");
            ingested += count;
        }
        println!(
            "ingested {total} in {:.1}s",
            started.elapsed().as_secs_f64()
        );
        let started = std::time::Instant::now();
        db.create_index(IndexDefinition {
            name: "articles_year".to_string(),
            collection: "articles".to_string(),
            fields: vec![IndexField::MetadataPath(vec!["year".to_string()])],
            unique: false,
            kind: IndexKind::BTree,
            predicate: None,
            exclusion: None,
        })
        .expect("create index");
        println!(
            "create_index (materialize + backfill): {:.1}s rss={:.0}MiB",
            started.elapsed().as_secs_f64(),
            rss_mib()
        );
        db.close().expect("close");
    }

    for round in 1..=2 {
        let started = std::time::Instant::now();
        let db = BicDb::open_with_config(&dir, config()).expect("reopen");
        println!(
            "reopen #{round}: {:.2}s rss={:.0}MiB",
            started.elapsed().as_secs_f64(),
            rss_mib()
        );
        let got = db
            .get("articles", &format!("pmid-{:09}", total - 1))
            .expect("get");
        assert!(got.is_some(), "read-back failed");
        db.close().expect("close");
    }
}
