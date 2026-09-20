//! FTS build-pipeline probe: seeds a corpus of realistic text rows, then
//! times CREATE INDEX (the external block build) so the pipelined scan can
//! be A/B'd against the serial one.
//!
//! ```text
//! cargo run --release -p bicdb-core --example fts_build_probe -- <dir> seed 100000
//! BICDB_FTS_BUILD_SERIAL=1 cargo run --release -p bicdb-core --example fts_build_probe -- <dir> build
//! cargo run --release -p bicdb-core --example fts_build_probe -- <dir> build
//! ```
//!
//! Seed and build run as separate invocations so the build starts from a
//! fresh process with a cold buffer pool, like a real post-import CREATE
//! INDEX does.

use bicdb_core::{BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, Record, StorageMode};
use serde_json::json;
use std::time::Instant;

const WORDS: &[&str] = &[
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
    "baseline",
    "outcome",
    "significant",
    "patients",
    "treatment",
    "control",
    "measured",
    "weeks",
    "dosage",
    "response",
    "domain",
    "double",
    "blind",
    "serum",
    "levels",
];

fn config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_sync_outbox(false)
}

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args
        .next()
        .expect("usage: fts_build_probe <dir> seed|build [rows]");
    let mode = args
        .next()
        .expect("usage: fts_build_probe <dir> seed|build [rows]");
    let path = std::path::Path::new(&dir);

    match mode.as_str() {
        "seed" => {
            let rows: usize = args
                .next()
                .and_then(|value| value.parse().ok())
                .unwrap_or(100_000);
            let mut db = BicDb::open_with_config(path, config()).expect("open");
            db.create_collection("articles").expect("collection");
            let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
            let started = Instant::now();
            for chunk_start in (0..rows).step_by(10_000) {
                let chunk = (chunk_start..rows.min(chunk_start + 10_000))
                    .map(|index| {
                        // ~450 words (~3 KB) of zipf-ish text per document.
                        let mut body = String::with_capacity(3_200);
                        for _ in 0..450 {
                            state = state
                                .wrapping_mul(6364136223846793005)
                                .wrapping_add(1442695040888963407);
                            let pick = ((state >> 33) as usize
                                % (WORDS.len() * (WORDS.len() + 1) / 2))
                                .min(WORDS.len() * (WORDS.len() + 1) / 2 - 1);
                            // Triangular skew: earlier words dominate.
                            let mut word = 0usize;
                            let mut acc = 0usize;
                            for (rank, _) in WORDS.iter().enumerate() {
                                acc += WORDS.len() - rank;
                                if pick < acc {
                                    word = rank;
                                    break;
                                }
                            }
                            body.push_str(WORDS[word]);
                            body.push(' ');
                        }
                        Record::new(format!("doc-{index:08}"))
                            .with_metadata(json!({ "title": format!("t-{index}"), "body": body }))
                    })
                    .collect::<Vec<_>>();
                db.bulk_load_insert("articles", chunk).expect("bulk load");
            }
            db.close().expect("close");
            println!("seeded {rows} rows in {:?}", started.elapsed());
        }
        "build" => {
            let mut db = BicDb::open_with_config(path, config()).expect("open");
            let serial = std::env::var("BICDB_FTS_BUILD_SERIAL").as_deref() == Ok("1");
            let started = Instant::now();
            db.create_index(IndexDefinition {
                name: "articles_fts".to_string(),
                collection: "articles".to_string(),
                fields: vec![
                    IndexField::MetadataPath(vec!["title".to_string()]),
                    IndexField::MetadataPath(vec!["body".to_string()]),
                ],
                unique: false,
                kind: IndexKind::FullText,
                predicate: None,
                exclusion: None,
            })
            .expect("create index");
            println!(
                "build mode={} took {:?}",
                if serial { "serial" } else { "pipelined" },
                started.elapsed()
            );
            let hits = db
                .lookup_full_text_term("articles_fts", "ashwagandha", false)
                .expect("lookup");
            println!("sanity: {} docs match `ashwagandha`", hits.len());
        }
        other => panic!("unknown mode `{other}`"),
    }
}
