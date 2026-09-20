//! Measure what durable FTS postings buy on a `server_paged` database:
//! reopen time and RSS for an FTS-indexed corpus (registry mode + entry load
//! vs the old stub materialization + full from-rows rebuild), and point/prefix
//! lookup latency against the loaded store.
//!
//! Usage: paged_fts_probe <dir> <rows>

use bicdb_core::{BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, Record, StorageMode};
use serde_json::json;

fn rss_mib() -> f64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    status
        .lines()
        .find(|line| line.starts_with("VmRSS:"))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse::<f64>().ok())
        .map(|kib| kib / 1024.0)
        .unwrap_or(0.0)
}

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args.next().expect("dir");
    let rows: usize = args.next().expect("rows").parse().unwrap();
    let path = std::path::Path::new(&dir);
    let config = || {
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged)
    };
    // vocab size and terms/doc are tunable so the probe can model a REAL
    // corpus (tens of thousands of distinct stems, dozens per document) —
    // a tiny vocabulary hides the resident-postings cost in a handful of
    // giant posting lists.
    let vocab: usize = std::env::args()
        .nth(3)
        .and_then(|value| value.parse().ok())
        .unwrap_or(15);
    let terms_per_doc: usize = std::env::args()
        .nth(4)
        .and_then(|value| value.parse().ok())
        .unwrap_or(4);
    let vocabulary: Vec<String> = (0..vocab)
        .map(|index| {
            [
                "anxiety",
                "ashwagandha",
                "sleep",
                "stress",
                "yoga",
                "valerian",
                "chamomile",
                "calm",
                "insomnia",
                "cortisol",
                "adaptogen",
                "placebo",
                "randomized",
                "trial",
                "cohort",
            ]
            .get(index)
            .map(|term| term.to_string())
            .unwrap_or_else(|| format!("term{index:06}"))
        })
        .collect();

    if !path.join("paged").exists() {
        let started = std::time::Instant::now();
        let mut db = BicDb::open_with_config(path, config()).unwrap();
        db.create_collection("articles").unwrap();
        db.create_index(IndexDefinition {
            name: "articles_fts".to_string(),
            collection: "articles".to_string(),
            fields: vec![IndexField::MetadataPath(vec!["terms".to_string()])],
            unique: false,
            kind: IndexKind::FullText,
            predicate: None,
            exclusion: None,
        })
        .unwrap();
        let mut batch = Vec::with_capacity(5_000);
        for index in 0..rows {
            let terms: Vec<&str> = (0..terms_per_doc)
                .map(|offset| vocabulary[(index * 7 + offset * 131) % vocabulary.len()].as_str())
                .collect();
            batch.push(
                Record::new(format!("r{index:08}"))
                    .with_metadata(json!({ "terms": terms, "title": format!("Study {index}") })),
            );
            if batch.len() == 5_000 {
                db.batch_insert("articles", std::mem::take(&mut batch))
                    .unwrap();
            }
        }
        if !batch.is_empty() {
            db.batch_insert("articles", batch).unwrap();
        }
        println!(
            "ingest: {rows} rows in {:.1}s rss={:.0}MiB",
            started.elapsed().as_secs_f64(),
            rss_mib()
        );
        db.close().unwrap();
        return;
    }

    let started = std::time::Instant::now();
    let db = BicDb::open_with_config(path, config()).unwrap();
    println!(
        "reopen: {:.2}s rss={:.0}MiB",
        started.elapsed().as_secs_f64(),
        rss_mib()
    );
    let started = std::time::Instant::now();
    let hits = db
        .lookup_full_text_term("articles_fts", "ashwagandha", false)
        .unwrap();
    println!(
        "point lookup: {} pks in {:.1}ms",
        hits.len(),
        started.elapsed().as_secs_f64() * 1e3
    );
    let started = std::time::Instant::now();
    let hits = db.lookup_full_text_term("articles_fts", "a", true).unwrap();
    println!(
        "prefix lookup: {} pks in {:.1}ms rss={:.0}MiB",
        hits.len(),
        started.elapsed().as_secs_f64() * 1e3,
        rss_mib()
    );
    let report = db.verify_index("articles_fts").unwrap();
    println!("verify: valid={}", report.valid);
}
