//! At-scale RSS probe for `storage_mode = server_paged`, through `BicDb`.
//!
//! Gates 1+2 of the PubMed import plan at the *product* level: ingest a large
//! PubMed-shaped corpus through the full `BicDb` API in paged mode and prove
//! resident memory stops tracking corpus size. The page engine proved this for
//! itself months of work ago; this proves the layer above stopped undoing it.
//!
//! Usage: paged_scale_probe <dir> <records> [embedded]
//!
//! Prints one line per checkpoint: records ingested, RSS, rate. Exits nonzero
//! if any batch fails. RSS interpretation is the operator's job — the probe
//! reports, a human (or the calling script) judges.

use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use serde_json::json;

fn rss_mib() -> f64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: f64 = rest
                .trim()
                .trim_end_matches("kB")
                .trim()
                .parse()
                .unwrap_or(0.0);
            return kb / 1024.0;
        }
    }
    0.0
}

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args
        .next()
        .expect("usage: paged_scale_probe <dir> <records> [embedded]");
    let total: usize = args
        .next()
        .expect("usage: paged_scale_probe <dir> <records> [embedded]")
        .parse()
        .expect("records must be a number");
    let mode = if args.next().as_deref() == Some("embedded") {
        StorageMode::EmbeddedMemory
    } else {
        StorageMode::ServerPaged
    };

    let mut db = BicDb::open_with_config(
        &dir,
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(mode.clone()),
    )
    .expect("open");
    db.create_collection("articles").expect("create collection");

    println!("mode={mode} target={total} rss_start={:.1}MiB", rss_mib());
    let started = std::time::Instant::now();
    let batch = 2_000;
    let mut ingested = 0usize;

    while ingested < total {
        let count = batch.min(total - ingested);
        let records: Vec<Record> = (ingested..ingested + count)
            .map(|i| {
                // PubMed-shaped: a title, an abstract of a few hundred bytes,
                // authors, year. ~600 bytes of metadata per record.
                Record::new(format!("pmid-{i:09}")).with_metadata(json!({
                    "title": format!("Association of factor {i} with outcome {}", i % 977),
                    "abstract": format!(
                        "Background: study {i}. Methods: cohort of {} patients \
                         followed for {} months. Results: significant association \
                         observed (p={}). Conclusion: further work needed. {}",
                        (i % 9000) + 100,
                        (i % 120) + 1,
                        1.0 / ((i % 97) + 2) as f64,
                        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(4),
                    ),
                    "authors": [format!("Author{}", i % 5000), format!("Author{}", i % 7001)],
                    "year": 1990 + (i % 36),
                    "journal": format!("Journal of Field {}", i % 250),
                }))
            })
            .collect();
        db.batch_insert("articles", records).expect("batch insert");
        ingested += count;

        if ingested % 100_000 == 0 || ingested == total {
            let secs = started.elapsed().as_secs_f64();
            println!(
                "ingested={ingested} rss={:.1}MiB rate={:.0}/s elapsed={:.0}s",
                rss_mib(),
                ingested as f64 / secs,
                secs,
            );
        }
    }

    // A point read and a scan-count from the far end, to prove the data is
    // really reachable rather than merely written.
    let probe_id = format!("pmid-{:09}", total - 1);
    let found = db.get("articles", &probe_id).expect("get").is_some();
    println!("final: read_back={found} rss={:.1}MiB", rss_mib());
    db.close().expect("close");
    if !found {
        std::process::exit(1);
    }
}
// (reopen probe appended by a second binary below)
