//! Search-node workload harness: cache-class isolation and ranked-query
//! parallelism, measured together on one corpus.
//!
//! Two questions, one harness, because they share a corpus and interact:
//!
//! 1. **Does cache-class isolation hold under a real workload?** Popular
//!    queries, then a large document-text scan, then the same queries again.
//!    If phase 3 is materially slower than phase 1, a document scan is still
//!    evicting the search working set.
//!
//! 2. **Where do the ranked-parallelism gates bind?** A matrix of query
//!    shapes, top-k sizes and partition counts, recording workers *wanted*
//!    against workers *granted* so permit starvation is visible rather than
//!    inferred.
//!
//! ```text
//! cargo run --release -p bicdb-core --example fts_workload -- <dir> seed 200000
//! cargo run --release -p bicdb-core --example fts_workload -- <dir> seed-wet <wet-dir>
//! cargo run --release -p bicdb-core --example fts_workload -- <dir> build
//! cargo run --release -p bicdb-core --example fts_workload -- <dir> phases
//! cargo run --release -p bicdb-core --example fts_workload -- <dir> matrix
//! cargo run --release -p bicdb-core --example fts_workload -- <dir> queryset <out.json>
//! cargo run --release -p bicdb-core --example fts_workload -- <dir> bench <queryset.json>
//! cargo run --release -p bicdb-core --example fts_workload -- <dir> concurrent <clients> <secs>
//! ```
//!
//! `seed-wet` ingests a directory of extracted-text files (one document per
//! file, or WET-style records split on blank lines) so the same measurements
//! can be run against a real corpus rather than a synthetic one. Each phase
//! runs in its own process invocation so the buffer pool starts cold, which
//! is the state a restarted search node is actually in.

use bicdb_core::{
    full_text_query_instrumentation, BicDb, Bm25Parameters, DbConfig, IndexDefinition, IndexField,
    IndexKind, Record, StorageMode,
};
use serde_json::json;
use std::time::Instant;

/// Zipf-ish vocabulary. Rank 0 is the most common term; the tail is rare.
/// Query shapes below are chosen against these ranks on purpose, because
/// "common + rare" and "common + common" exercise completely different paths.
const WORDS: &[&str] = &[
    "the",
    "and",
    "for",
    "with",
    "from",
    "that",
    "this",
    "have",
    "more",
    "will",
    "about",
    "which",
    "their",
    "would",
    "there",
    "other",
    "these",
    "when",
    "search",
    "index",
    "content",
    "website",
    "service",
    "product",
    "company",
    "research",
    "analysis",
    "framework",
    "distributed",
    "throughput",
    "provenance",
    "quantisation",
    "isochrone",
    "polyfill",
    "adaptogen",
    "bathymetry",
    "hydrology",
    "xenolith",
    "chalcedony",
    "zeolite",
];

const COMMON: &[&str] = &["the", "and", "for", "with"];
const MEDIUM: &[&str] = &["search", "index", "content", "service"];
const RARE: &[&str] = &["xenolith", "chalcedony", "zeolite", "bathymetry"];

/// First index of `needle` in `haystack`.
fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Value of a WARC header line, case-sensitive as the format specifies.
fn header_value<'a>(headers: &'a [u8], name: &[u8]) -> Option<&'a str> {
    let at = find(headers, name)?;
    let rest = &headers[at + name.len()..];
    let end = find(rest, b"\r\n").unwrap_or(rest.len());
    std::str::from_utf8(&rest[..end]).ok()
}

fn config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_sync_outbox(false)
        // Off by default; halves page bytes on text and is the largest single
        // lever at corpus scale, so the benchmark must be able to measure it.
        .with_paged_value_compression(
            std::env::var("BICDB_WORKLOAD_COMPRESS").as_deref() == Ok("1"),
        )
        // Deliberately small relative to the corpus: a search node never holds
        // its corpus, and a pool that fits everything measures nothing.
        .with_paged_buffer_pool_bytes(
            std::env::var("BICDB_WORKLOAD_POOL_MB")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(96)
                * 1024
                * 1024,
        )
        // The knob that actually governs ranked-query latency. It is SEPARATE
        // from the buffer pool: posting blocks live here, document pages live
        // there, so a document scan cannot evict postings however large it is.
        // Default is 64 MiB, which is nothing against a corpus-scale index.
        .with_fts_block_cache_bytes(
            std::env::var("BICDB_WORKLOAD_FTS_CACHE_MB")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(64)
                * 1024
                * 1024,
        )
        .with_fts_query_partitions(
            std::env::var("BICDB_WORKLOAD_PARTITIONS")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(8),
        )
}

struct Sample {
    wanted: u64,
    granted: u64,
    posting_bytes: u64,
    cache_hits: u64,
    cache_misses: u64,
}

fn sample() -> Sample {
    let instrumentation = full_text_query_instrumentation();
    Sample {
        wanted: instrumentation.ranked_workers_wanted,
        granted: instrumentation.ranked_workers_granted,
        posting_bytes: instrumentation.posting_bytes_read,
        cache_hits: instrumentation.block_cache_hits,
        cache_misses: instrumentation.block_cache_misses,
    }
}

fn delta(before: &Sample, after: &Sample) -> (u64, u64, u64, f64) {
    let hits = after.cache_hits.saturating_sub(before.cache_hits);
    let misses = after.cache_misses.saturating_sub(before.cache_misses);
    let rate = if hits + misses == 0 {
        0.0
    } else {
        hits as f64 * 100.0 / (hits + misses) as f64
    };
    (
        after.wanted.saturating_sub(before.wanted),
        after.granted.saturating_sub(before.granted),
        after.posting_bytes.saturating_sub(before.posting_bytes),
        rate,
    )
}

/// The popular-query set, run repeatedly. Deliberately a mix of shapes so one
/// pathological shape cannot dominate the phase timing.
fn popular_queries() -> Vec<Vec<&'static str>> {
    vec![
        vec![COMMON[0], COMMON[1]],
        vec![COMMON[0], MEDIUM[0]],
        vec![MEDIUM[0], MEDIUM[1]],
        vec![COMMON[2], MEDIUM[2]],
        vec![COMMON[0], COMMON[1], MEDIUM[0]],
    ]
}

fn run_queries(db: &BicDb, queries: &[Vec<&str>], keep: usize, rounds: usize) -> Vec<f64> {
    let mut latencies = Vec::with_capacity(queries.len() * rounds);
    for _ in 0..rounds {
        for terms in queries {
            let started = Instant::now();
            let _ = db
                .full_text_bm25_top_k("articles_fts", terms, Bm25Parameters::default(), keep, true)
                .expect("ranked query");
            latencies.push(started.elapsed().as_secs_f64() * 1000.0);
        }
    }
    latencies
}

fn percentile(sorted: &[f64], fraction: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = ((fraction * sorted.len() as f64).ceil() as usize).saturating_sub(1);
    sorted[rank.min(sorted.len() - 1)]
}

fn summarize(mut latencies: Vec<f64>) -> (f64, f64, f64) {
    latencies.sort_by(f64::total_cmp);
    (
        percentile(&latencies, 0.5),
        percentile(&latencies, 0.95),
        latencies.iter().sum::<f64>() / latencies.len().max(1) as f64,
    )
}

fn synthetic_body(state: &mut u64, words: usize) -> String {
    let mut body = String::with_capacity(words * 7);
    for _ in 0..words {
        *state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        // Triangular skew toward rank 0: a realistic term-frequency curve is
        // what makes "common + rare" and "common + common" behave differently.
        let span = WORDS.len() * (WORDS.len() + 1) / 2;
        let pick = ((*state >> 33) as usize) % span;
        let mut acc = 0usize;
        let mut word = WORDS.len() - 1;
        for rank in 0..WORDS.len() {
            acc += WORDS.len() - rank;
            if pick < acc {
                word = rank;
                break;
            }
        }
        body.push_str(WORDS[word]);
        body.push(' ');
    }
    body
}

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args
        .next()
        .expect("usage: fts_workload <dir> seed|seed-wet|build|phases|matrix [arg]");
    let mode = args
        .next()
        .expect("usage: fts_workload <dir> seed|seed-wet|build|phases|matrix [arg]");
    let path = std::path::Path::new(&dir);

    match mode.as_str() {
        "seed" => {
            let rows: usize = args
                .next()
                .and_then(|value| value.parse().ok())
                .unwrap_or(200_000);
            let mut db = BicDb::open_with_config(path, config()).expect("open");
            db.create_collection("articles").expect("collection");
            let mut state = 0x9E37_79B9_7F4A_7C15u64;
            let started = Instant::now();
            for chunk_start in (0..rows).step_by(10_000) {
                let chunk = (chunk_start..rows.min(chunk_start + 10_000))
                    .map(|index| {
                        // ~1,100 words is roughly the 8 KB/page ratio observed
                        // on extracted web text.
                        let body = synthetic_body(&mut state, 1_100);
                        Record::new(format!("doc-{index:08}")).with_metadata(
                            json!({ "url": format!("https://example/{index}"), "body": body }),
                        )
                    })
                    .collect::<Vec<_>>();
                db.bulk_load_insert("articles", chunk).expect("bulk load");
            }
            db.close().expect("close");
            println!(
                "seeded {rows} synthetic documents in {:?}",
                started.elapsed()
            );
        }
        // Proper WARC/WET parsing. Records are `WARC/1.0`, headers until a
        // blank line, then exactly Content-Length bytes of extracted text.
        // Splitting on blank lines (the first version of this) shreds
        // documents at every paragraph break and inflates the document count
        // by an order of magnitude, which would quietly invalidate every
        // per-document statistic derived from the corpus.
        "seed-wet" => {
            let source = args
                .next()
                .expect("usage: ... seed-wet <dir-of-wet-files> [max]");
            let max_documents: usize = args
                .next()
                .and_then(|value| value.parse().ok())
                .unwrap_or(usize::MAX);
            let mut db = BicDb::open_with_config(path, config()).expect("open");
            db.create_collection("articles").expect("collection");
            let started = Instant::now();

            let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(&source)
                .expect("read source dir")
                .flatten()
                .map(|entry| entry.path())
                .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("wet"))
                .collect();
            files.sort();
            assert!(!files.is_empty(), "no .wet files in {source}");

            let mut index = 0usize;
            let mut text_bytes = 0u64;
            let mut skipped_short = 0usize;
            let mut chunk: Vec<Record> = Vec::with_capacity(2_000);
            'files: for file in &files {
                let raw = std::fs::read(file).expect("read wet file");
                let mut at = 0usize;
                while at < raw.len() {
                    // Find the next record header.
                    let Some(start) = find(&raw[at..], b"WARC/1.0") else {
                        break;
                    };
                    let header_start = at + start;
                    let Some(gap) = find(&raw[header_start..], b"\r\n\r\n") else {
                        break;
                    };
                    let body_start = header_start + gap + 4;
                    let headers = &raw[header_start..header_start + gap];
                    let length = header_value(headers, b"Content-Length:")
                        .and_then(|value| value.trim().parse::<usize>().ok())
                        .unwrap_or(0);
                    let body_end = (body_start + length).min(raw.len());
                    let is_conversion = header_value(headers, b"WARC-Type:")
                        .map(|value| value.trim() == "conversion")
                        .unwrap_or(false);
                    if is_conversion && length > 0 {
                        let url = header_value(headers, b"WARC-Target-URI:")
                            .map(|value| value.trim().to_string())
                            .unwrap_or_default();
                        let body = String::from_utf8_lossy(&raw[body_start..body_end]);
                        // Real WET output includes a long tail of near-empty
                        // extractions. Keeping them would skew every
                        // per-document statistic downward.
                        if body.trim().len() >= 200 {
                            text_bytes += body.len() as u64;
                            chunk.push(Record::new(format!("doc-{index:08}")).with_metadata(
                                json!({
                                    "url": url,
                                    "body": body,
                                }),
                            ));
                            index += 1;
                            if chunk.len() >= 2_000 {
                                db.bulk_load_insert("articles", std::mem::take(&mut chunk))
                                    .expect("bulk load");
                                chunk = Vec::with_capacity(2_000);
                                if index % 100_000 == 0 {
                                    println!("  ingested {index} documents...");
                                }
                            }
                            if index >= max_documents {
                                break 'files;
                            }
                        } else {
                            skipped_short += 1;
                        }
                    }
                    at = body_end.max(header_start + 8);
                }
            }
            if !chunk.is_empty() {
                db.bulk_load_insert("articles", chunk).expect("bulk load");
            }
            db.close().expect("close");
            println!(
                "ingested {index} documents from {} WET files ({:.2} GiB of text, \
                 {:.0} B/doc mean); skipped {skipped_short} near-empty extractions; \
                 {:?}",
                files.len(),
                text_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
                text_bytes as f64 / index.max(1) as f64,
                started.elapsed()
            );
        }
        "build" => {
            let mut db = BicDb::open_with_config(path, config()).expect("open");
            let started = Instant::now();
            db.create_index(IndexDefinition {
                name: "articles_fts".to_string(),
                collection: "articles".to_string(),
                fields: vec![IndexField::MetadataPath(vec!["body".to_string()])],
                kind: IndexKind::FullText,
                unique: false,
                predicate: None,
                exclusion: None,
            })
            .expect("create index");
            db.close().expect("close");
            println!("built FTS index in {:?}", started.elapsed());
        }
        // ---- the cache-isolation acceptance workload -------------------
        "phases" => {
            let db = BicDb::open_with_config(path, config()).expect("open");
            let queries = popular_queries();

            println!("phase 1: popular queries against a cold pool");
            let before = sample();
            let warm = run_queries(&db, &queries, 100, 20);
            let after = sample();
            let (wanted, granted, posting_bytes, hit_rate) = delta(&before, &after);
            let (p50_1, p95_1, mean_1) = summarize(warm);
            println!(
                "  p50 {p50_1:.2}ms  p95 {p95_1:.2}ms  mean {mean_1:.2}ms  \
                 cache {hit_rate:.1}%  postings {posting_bytes} B  workers {granted}/{wanted}"
            );

            println!("phase 2: streaming document text");
            let before = sample();
            let scan_started = Instant::now();
            let mut scanned = 0u64;
            let mut scanned_bytes = 0u64;
            for record in db.scan_collection("articles").expect("scan") {
                // Read-then-snippet: the access pattern that defeats a plain
                // 2Q, and the one cache classes exist to contain.
                if let Some(body) = record.metadata.get("body").and_then(|v| v.as_str()) {
                    scanned_bytes += body.len() as u64;
                    let _ = body.len();
                    let _ = body.as_bytes().first();
                }
                scanned += 1;
            }
            let after = sample();
            let (_, _, scan_posting_bytes, scan_hit_rate) = delta(&before, &after);
            println!(
                "  scanned {scanned} documents ({:.2} GiB) in {:?}  \
                 cache {scan_hit_rate:.1}%  postings touched {scan_posting_bytes} B",
                scanned_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
                scan_started.elapsed()
            );

            println!("phase 3: the same popular queries again");
            let before = sample();
            let again = run_queries(&db, &queries, 100, 20);
            let after = sample();
            let (wanted, granted, posting_bytes, hit_rate) = delta(&before, &after);
            let (p50_3, p95_3, mean_3) = summarize(again);
            println!(
                "  p50 {p50_3:.2}ms  p95 {p95_3:.2}ms  mean {mean_3:.2}ms  \
                 cache {hit_rate:.1}%  postings {posting_bytes} B  workers {granted}/{wanted}"
            );

            let ratio = if mean_1 > 0.0 { mean_3 / mean_1 } else { 0.0 };
            println!();
            println!("VERDICT  phase3/phase1 mean = {ratio:.2}x  (p50 {p50_1:.2} -> {p50_3:.2}ms)");
            println!(
                "  {}",
                if ratio <= 1.25 {
                    "PASS: the document scan did not materially degrade hot-query latency"
                } else {
                    "FAIL: hot-query latency degraded after the scan - the search \
                     working set is still being evicted"
                }
            );
        }
        // ---- the ranked-parallelism matrix -----------------------------
        // ---- where do the index bytes actually go? ---------------------
        "storage" => {
            let db = BicDb::open_with_config(path, config()).expect("open");
            let acct = db
                .full_text_storage_accounting("articles_fts")
                .expect("storage accounting");
            let rows: Vec<(&str, bicdb_core::StorageNamespaceAccounting)> = vec![
                ("row data", acct.row_data),
                ("document terms", acct.document_terms),
                ("term dictionary", acct.term_dictionary),
                ("document ids", acct.document_ids),
                ("postings", acct.postings),
                ("impact metadata", acct.impact_metadata),
                ("document statistics", acct.document_statistics),
                ("stored text", acct.stored_text),
            ];
            let total = acct.total_bytes().max(1);
            println!(
                "{:<22} {:>14} {:>12} {:>14} {:>8}",
                "namespace", "entries", "keys MiB", "values MiB", "share"
            );
            for (label, ns) in &rows {
                let bytes = ns.total_bytes();
                println!(
                    "{label:<22} {:>14} {:>12.1} {:>14.1} {:>7.1}%",
                    ns.entries,
                    ns.key_bytes as f64 / (1024.0 * 1024.0),
                    ns.value_bytes as f64 / (1024.0 * 1024.0),
                    bytes as f64 * 100.0 / total as f64
                );
            }
            println!();
            println!(
                "logical total {:.2} GiB across {} entries",
                total as f64 / (1024.0 * 1024.0 * 1024.0),
                rows.iter().map(|(_, ns)| ns.entries).sum::<u64>()
            );
            // Keys are the part a search engine would not pay at all: every
            // entry in a paged keyspace carries its own key, where a packed
            // posting block carries none.
            let key_total: u64 = rows.iter().map(|(_, ns)| ns.key_bytes).sum();
            println!(
                "of which KEYS {:.2} GiB ({:.1}%) — the per-entry cost a packed \
                 posting block does not pay",
                key_total as f64 / (1024.0 * 1024.0 * 1024.0),
                key_total as f64 * 100.0 / total as f64
            );
        }
        // ---- forensics: what is every key byte actually FOR? ------------
        "keys" => {
            let db = BicDb::open_with_config(path, config()).expect("open");
            let parts = db
                .full_text_key_forensics("articles_fts")
                .expect("key forensics");
            let mib = |bytes: u64| bytes as f64 / (1024.0 * 1024.0);
            println!(
                "{:<26} {:>11} {:>9} {:>9} {:>9} {:>9} {:>9} {:>8}",
                "namespace", "entries", "total", "ns+len", "idx name", "framing", "term", "id"
            );
            let mut total = bicdb_core::KeyForensics::default();
            for (label, forensics) in &parts {
                total.merge(*forensics);
                println!(
                    "{label:<26} {:>11} {:>8.1}M {:>8.1}M {:>8.1}M {:>8.1}M {:>8.1}M {:>7.1}M",
                    forensics.entries,
                    mib(forensics.total_bytes),
                    mib(forensics.namespace_bytes + forensics.length_prefix_bytes),
                    mib(forensics.index_name_bytes),
                    mib(forensics.framing_bytes),
                    mib(forensics.term_bytes),
                    mib(forensics.suffix_bytes),
                );
            }
            println!();
            println!(
                "TOTAL key bytes {:.2} GiB across {} entries ({:.1} B/key)",
                total.total_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
                total.entries,
                total.total_bytes as f64 / total.entries.max(1) as f64
            );
            println!();
            println!(
                "  REQUIRED   term + id      {:>8.1} MiB  ({:.1}%)",
                mib(total.required_bytes()),
                total.required_bytes() as f64 * 100.0 / total.total_bytes.max(1) as f64
            );
            println!(
                "  IMPLIED    ns+len+name+framing {:>4.1} MiB  ({:.1}%)",
                mib(total.implied_bytes()),
                total.implied_bytes() as f64 * 100.0 / total.total_bytes.max(1) as f64
            );
            println!(
                "    of which index name  {:>8.1} MiB",
                mib(total.index_name_bytes)
            );
            println!(
                "    of which ns + length {:>8.1} MiB",
                mib(total.namespace_bytes + total.length_prefix_bytes)
            );
            println!(
                "    of which framing     {:>8.1} MiB",
                mib(total.framing_bytes)
            );
            println!(
                "  escapes inside terms   {:>8.1} MiB",
                mib(total.escape_bytes)
            );
            println!();
            println!("  Every IMPLIED byte is already known from the subtree being read.");
            println!("  Term bytes become an ordinal once a dictionary maps term -> id.");
        }
        // ---- build a stratified query set FROM the corpus --------------
        //
        // The strata have to be derived from the corpus, not written down in
        // advance: "common" means whatever is common in THIS text. A query set
        // hand-written against a 40-word synthetic vocabulary tells you
        // nothing about Common Crawl, where the head is enormous stopwords and
        // the tail is HTML garbage and every language on earth.
        //
        // The result is written to a file and meant to be KEPT. A benchmark
        // whose queries drift is a benchmark whose numbers cannot be compared
        // across releases.
        "queryset" => {
            let out = args.next().unwrap_or_else(|| "queryset.json".to_string());
            let sample: usize = args
                .next()
                .and_then(|value| value.parse().ok())
                .unwrap_or(20_000);
            let db = BicDb::open_with_config(path, config()).expect("open");

            let mut frequency: std::collections::HashMap<String, usize> =
                std::collections::HashMap::new();
            let mut sampled = 0usize;
            let mut lengths: Vec<usize> = Vec::new();
            for record in db.scan_collection("articles").expect("scan") {
                let Some(body) = record.metadata.get("body").and_then(|v| v.as_str()) else {
                    continue;
                };
                lengths.push(body.len());
                // Per-document term SET, so this is document frequency rather
                // than raw term frequency — df is what drives BM25 pruning.
                let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
                // Match the indexer exactly. An earlier version split on a
                // narrower class and shredded every script whose words carry
                // combining marks, which silently produced strata of fragments
                // rather than terms.
                for token in body.split(|c: char| {
                    !(c.is_alphanumeric()
                        || c == '_'
                        || unicode_categories::UnicodeCategories::is_mark(c))
                }) {
                    if token.len() < 3 || token.len() > 32 {
                        continue;
                    }
                    seen.insert(token);
                }
                for token in seen {
                    *frequency.entry(token.to_ascii_lowercase()).or_insert(0) += 1;
                }
                sampled += 1;
                if sampled >= sample {
                    break;
                }
            }
            assert!(sampled > 0, "the corpus is empty");

            let mut ranked: Vec<(String, usize)> = frequency.into_iter().collect();
            ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            // Drop the df<3 tail the way a real index would: it is most of the
            // vocabulary and none of the traffic.
            ranked.retain(|(_, df)| *df >= 3);
            assert!(ranked.len() >= 30, "too few terms survived df>=3");

            // Strata by DF PERCENTILE, not by stepping through rank slices.
            // Stepping put `mastercard` in "common" on real web text, because
            // the top 1% of a 487,735-term vocabulary is still 4,877 terms and
            // its tail is nothing like its head.
            let total = ranked.len();
            let at = |percentile: f64, n: usize| -> Vec<(String, usize)> {
                // `ranked` is descending by df, so percentile 100 = most common.
                let index = (((100.0 - percentile) / 100.0) * total as f64) as usize;
                ranked
                    .iter()
                    .skip(index.min(total.saturating_sub(1)))
                    .take(n)
                    .cloned()
                    .collect()
            };
            let named = |terms: &[(String, usize)]| -> Vec<String> {
                terms.iter().map(|(term, _)| term.clone()).collect()
            };
            // The stratum that punishes everything: posting lists that dwarf
            // anything a synthetic corpus produces.
            let extreme_terms = at(100.0, 6);
            let common_terms = at(97.0, 6);
            let medium_terms = at(50.0, 6);
            let rare_terms = at(3.0, 6);
            let extreme = named(&extreme_terms);
            let common = named(&common_terms);
            let medium = named(&medium_terms);
            let rare = named(&rare_terms);
            let df_of = |terms: &[(String, usize)]| -> Vec<usize> {
                terms.iter().map(|(_, df)| *df).collect()
            };

            lengths.sort_unstable();
            let median_len = lengths.get(lengths.len() / 2).copied().unwrap_or(0);
            let p99_len = lengths.get(lengths.len() * 99 / 100).copied().unwrap_or(0);

            let set = serde_json::json!({
                "documents_sampled": sampled,
                "terms_with_df_at_least_3": ranked.len(),
                "document_length_median": median_len,
                "document_length_p99": p99_len,
                "strata": {
                    "extreme": extreme, "common": common,
                    "medium": medium, "rare": rare,
                },
                "document_frequency": {
                    "extreme": df_of(&extreme_terms), "common": df_of(&common_terms),
                    "medium": df_of(&medium_terms), "rare": df_of(&rare_terms),
                },
                "top_k": [10, 100, 1000],
                "shapes": [
                    "rare", "medium", "common", "extreme",
                    "rare+common", "medium+common", "common+common",
                    "rare+rare+common", "extreme+common", "extreme+rare",
                    "three_term", "five_term",
                ],
            });
            std::fs::write(&out, serde_json::to_vec_pretty(&set).unwrap()).expect("write");
            println!(
                "sampled {sampled} documents, {} terms with df>=3\n\
                 document length median {median_len} B, p99 {p99_len} B\n\
                 extreme {extreme:?} df={:?}\n common {common:?} df={:?}\n\
                 medium {medium:?} df={:?}\n   rare {rare:?} df={:?}\n\
                 written to {out}",
                ranked.len(),
                df_of(&extreme_terms),
                df_of(&common_terms),
                df_of(&medium_terms),
                df_of(&rare_terms),
            );
        }
        // ---- run the retained query set against the corpus -------------
        "bench" => {
            let set_path = args.next().unwrap_or_else(|| "queryset.json".to_string());
            let raw = std::fs::read(&set_path).expect("read query set");
            let set: serde_json::Value = serde_json::from_slice(&raw).expect("parse query set");
            let stratum = |name: &str| -> Vec<String> {
                set["strata"][name]
                    .as_array()
                    .map(|values| {
                        values
                            .iter()
                            .filter_map(|v| v.as_str().map(str::to_string))
                            .collect()
                    })
                    .unwrap_or_default()
            };
            let extreme = stratum("extreme");
            let common = stratum("common");
            let medium = stratum("medium");
            let rare = stratum("rare");
            assert!(
                !common.is_empty() && !medium.is_empty() && !rare.is_empty(),
                "query set has empty strata"
            );
            // Older query sets predate the extreme tier; fall back so a
            // retained file from before it still runs.
            let extreme = if extreme.is_empty() {
                common.clone()
            } else {
                extreme
            };

            let shapes: Vec<(&str, Vec<String>)> = vec![
                ("rare", vec![rare[0].clone(), rare[1].clone()]),
                ("medium", vec![medium[0].clone(), medium[1].clone()]),
                ("common", vec![common[0].clone(), common[1].clone()]),
                ("rare+common", vec![rare[0].clone(), common[0].clone()]),
                ("medium+common", vec![medium[0].clone(), common[0].clone()]),
                ("common+common", vec![common[0].clone(), common[1].clone()]),
                (
                    "three_term",
                    vec![common[0].clone(), medium[0].clone(), rare[0].clone()],
                ),
                ("extreme", vec![extreme[0].clone(), extreme[1].clone()]),
                (
                    "extreme+common",
                    vec![extreme[0].clone(), common[0].clone()],
                ),
                ("extreme+rare", vec![extreme[0].clone(), rare[0].clone()]),
                (
                    "rare+rare+common",
                    vec![rare[0].clone(), rare[1].clone(), common[0].clone()],
                ),
                (
                    "five_term",
                    vec![
                        common[0].clone(),
                        common[1].clone(),
                        medium[0].clone(),
                        medium[1].clone(),
                        rare[0].clone(),
                    ],
                ),
            ];

            let db = BicDb::open_with_config(path, config()).expect("open");
            println!(
                "{:<16} {:>6} {:>10} {:>8} {:>9} {:>8} {:>12}",
                "shape", "top-k", "wall ms", "hits", "workers", "cache%", "postings B"
            );
            for (label, terms) in &shapes {
                for keep in [10usize, 100, 1_000] {
                    let borrowed: Vec<&str> = terms.iter().map(String::as_str).collect();
                    let before = sample();
                    let started = Instant::now();
                    let rounds = 5;
                    let mut hits = 0usize;
                    for _ in 0..rounds {
                        hits = db
                            .full_text_bm25_top_k(
                                "articles_fts",
                                &borrowed,
                                Bm25Parameters::default(),
                                keep,
                                true,
                            )
                            .expect("ranked query")
                            .map(|found| found.len())
                            .unwrap_or(0);
                    }
                    let wall = started.elapsed().as_secs_f64() * 1000.0 / rounds as f64;
                    let after = sample();
                    let (wanted, granted, posting_bytes, hit_rate) = delta(&before, &after);
                    println!(
                        "{label:<16} {keep:>6} {wall:>10.2} {hits:>8} {:>9} {hit_rate:>7.1}% {:>12}",
                        format!("{granted}/{wanted}"),
                        posting_bytes / rounds as u64
                    );
                }
            }
            println!();
            println!(
                "  Query set: {} documents sampled, {} terms with df>=3, \
                 median doc {} B.",
                set["documents_sampled"],
                set["terms_with_df_at_least_3"],
                set["document_length_median"]
            );
            println!("  Keep this file. Numbers are only comparable against the same queries.");
        }
        // ---- concurrent clients: where the permit pool binds ----------
        "concurrent" => {
            let clients: usize = args
                .next()
                .and_then(|value| value.parse().ok())
                .unwrap_or(8);
            let seconds: u64 = args
                .next()
                .and_then(|value| value.parse().ok())
                .unwrap_or(10);
            let db = BicDb::open_with_config(path, config()).expect("open");
            let queries = popular_queries();

            println!(
                "{clients} concurrent clients for {seconds}s, \
                 fts_query_partitions={}",
                std::env::var("BICDB_WORKLOAD_PARTITIONS").unwrap_or_else(|_| "default".into())
            );
            let before = sample();
            let deadline = Instant::now() + std::time::Duration::from_secs(seconds);
            let latencies: Vec<Vec<f64>> = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..clients)
                    .map(|client| {
                        let db = &db;
                        let queries = &queries;
                        scope.spawn(move || {
                            let mut mine = Vec::new();
                            let mut index = client;
                            while Instant::now() < deadline {
                                let terms = &queries[index % queries.len()];
                                index += 1;
                                let started = Instant::now();
                                let _ = db
                                    .full_text_bm25_top_k(
                                        "articles_fts",
                                        terms,
                                        Bm25Parameters::default(),
                                        50,
                                        true,
                                    )
                                    .expect("ranked query");
                                mine.push(started.elapsed().as_secs_f64() * 1000.0);
                            }
                            mine
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|handle| handle.join().unwrap_or_default())
                    .collect()
            });
            let after = sample();
            let (wanted, granted, posting_bytes, hit_rate) = delta(&before, &after);

            let total: usize = latencies.iter().map(|l| l.len()).sum();
            let flat: Vec<f64> = latencies.into_iter().flatten().collect();
            let (p50, p95, mean) = summarize(flat);
            let starvation = if wanted == 0 {
                0.0
            } else {
                100.0 - (granted as f64 * 100.0 / wanted as f64)
            };
            println!(
                "  {total} queries  {:.0} q/s  p50 {p50:.2}ms  p95 {p95:.2}ms  mean {mean:.2}ms",
                total as f64 / seconds as f64
            );
            println!("  cache {hit_rate:.1}%   postings {posting_bytes} B");
            println!(
                "  workers granted/wanted {granted}/{wanted}  \
                 STARVATION {starvation:.1}%"
            );
            println!();
            println!(
                "  Extra workers come from a GLOBAL permit pool. A large \
                 starvation figure is contention for that pool, not slow code \
                 — the queries are getting fewer partitions than their plan \
                 asked for because other queries hold the permits."
            );
        }
        "matrix" => {
            let db = BicDb::open_with_config(path, config()).expect("open");
            let shapes: Vec<(&str, Vec<&str>)> = vec![
                ("rare+common", vec![RARE[0], COMMON[0]]),
                ("medium+common", vec![MEDIUM[0], COMMON[0]]),
                ("common+common", vec![COMMON[0], COMMON[1]]),
                ("common x4", COMMON.to_vec()),
                (
                    "common x6",
                    vec![
                        COMMON[0], COMMON[1], COMMON[2], COMMON[3], MEDIUM[0], MEDIUM[1],
                    ],
                ),
            ];
            println!(
                "{:<16} {:>6} {:>10} {:>10} {:>9} {:>8} {:>12}",
                "shape", "top-k", "wall ms", "cpu ms", "workers", "cache%", "postings B"
            );
            for (label, terms) in &shapes {
                for keep in [10usize, 100, 1_000] {
                    let before = sample();
                    let cpu_before = std::time::Instant::now();
                    let started = Instant::now();
                    let rounds = 5;
                    for _ in 0..rounds {
                        let _ = db
                            .full_text_bm25_top_k(
                                "articles_fts",
                                terms,
                                Bm25Parameters::default(),
                                keep,
                                true,
                            )
                            .expect("ranked query");
                    }
                    let wall = started.elapsed().as_secs_f64() * 1000.0 / rounds as f64;
                    let cpu = cpu_before.elapsed().as_secs_f64() * 1000.0 / rounds as f64;
                    let after = sample();
                    let (wanted, granted, posting_bytes, hit_rate) = delta(&before, &after);
                    println!(
                        "{label:<16} {keep:>6} {wall:>10.2} {cpu:>10.2} {:>9} {hit_rate:>7.1}% {:>12}",
                        format!("{granted}/{wanted}"),
                        posting_bytes / rounds as u64
                    );
                }
            }
            println!();
            println!(
                "workers shown as granted/wanted. A large gap is permit contention, \
                 not slow code."
            );
            println!(
                "re-run with BICDB_WORKLOAD_PARTITIONS=1|4|8|16|24 to sweep the \
                 partition count."
            );
        }
        other => panic!("unknown mode `{other}`"),
    }
}
