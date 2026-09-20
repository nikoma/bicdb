//! G5.3: several real-shaped projections over one ~1M-row business corpus,
//! subjected to recrawl mutations, reconciled at MULTIPLE checkpoints.
//!
//! The corpus here is synthetic but shaped like the crawl corpus (skewed
//! categories, a dominant hosting provider, realistic missing-value rates,
//! H3-like cells, CMS/ecommerce flags, score bands). Running against the real
//! India corpus additionally requires access to that database — see the
//! campaign notes; nothing about this harness changes for it, only the loader.
//!
//! Usage: `cargo run --release --example projection_corpus -- [rows] [rounds]`

use std::time::Instant;

use bicdb_core::aggregate_projection::AggregateProjection;
use bicdb_core::{BicDb, DbConfig, Record};
use serde_json::json;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound.max(1) as u64) as usize
    }
    /// Squaring concentrates mass on low indices — a few categories and one
    /// hosting provider dominate, as in a real directory.
    fn skewed(&mut self, bound: usize) -> usize {
        let unit = (self.next() % 1_000_000) as f64 / 1_000_000.0;
        ((unit * unit) * bound as f64) as usize % bound.max(1)
    }
}

const STATES: usize = 36;
const CATEGORIES: usize = 500;
const HOSTS: usize = 50;
const CMS: usize = 12;
const H3_CELLS: usize = 20_000;

fn rss_mib() -> f64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                line.strip_prefix("VmRSS:").and_then(|rest| {
                    rest.trim()
                        .trim_end_matches(" kB")
                        .trim()
                        .parse::<f64>()
                        .ok()
                        .map(|kb| kb / 1024.0)
                })
            })
        })
        .unwrap_or(0.0)
}

fn business(rng: &mut Rng, index: usize) -> Record {
    let score = rng.below(100);
    Record::new(format!("biz-{index:08}")).with_metadata(json!({
        "state": format!("state-{:02}", rng.below(STATES)),
        "category": format!("cat-{:03}", rng.skewed(CATEGORIES)),
        // ~22% of businesses have no detectable hosting provider.
        "host": if rng.below(100) < 22 {
            serde_json::Value::Null
        } else {
            json!(format!("host-{:03}", rng.skewed(HOSTS)))
        },
        "cms": if rng.below(100) < 35 {
            serde_json::Value::Null
        } else {
            json!(format!("cms-{:02}", rng.skewed(CMS)))
        },
        "ecommerce": rng.below(100) < 18,
        "h3": format!("h3-{:05}", rng.skewed(H3_CELLS)),
        "score_band": format!("band-{}", score / 20),
        "score": score as f64,
        "lcp": (rng.below(4000) + 500) as f64,
    }))
}

fn projections() -> Vec<AggregateProjection> {
    vec![
        AggregateProjection::new(
            "state_category",
            "businesses",
            vec!["state".into(), "category".into()],
            vec!["score".into()],
        )
        .unwrap(),
        AggregateProjection::new(
            "state_category_host",
            "businesses",
            vec!["state".into(), "category".into(), "host".into()],
            vec!["score".into()],
        )
        .unwrap(),
        AggregateProjection::new(
            "h3_category",
            "businesses",
            vec!["h3".into(), "category".into()],
            vec!["score".into(), "lcp".into()],
        )
        .unwrap(),
        AggregateProjection::new(
            "cms_ecommerce_band",
            "businesses",
            vec!["cms".into(), "ecommerce".into(), "score_band".into()],
            vec!["score".into(), "lcp".into()],
        )
        .unwrap(),
    ]
}

fn main() {
    let mut args = std::env::args().skip(1);
    let rows: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(200_000);
    let rounds: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(4);

    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_audit_events(true),
    )
    .unwrap();
    db.create_collection("businesses").unwrap();
    let baseline = rss_mib();

    println!("corpus: {rows} rows, 4 projections, {rounds} recrawl rounds");
    let started = Instant::now();
    let mut rng = Rng(0x00c0_ffee_1234_5678);
    let records: Vec<Record> = (0..rows).map(|index| business(&mut rng, index)).collect();
    db.bulk_load_insert("businesses", records).unwrap();
    println!(
        "load                {:>7.1}s",
        started.elapsed().as_secs_f64()
    );

    let mut projections = projections();
    println!();
    println!(
        "{:<22} {:>9} {:>9} {:>8} {:>9} {:>10} {:>9}",
        "projection", "cells", "cells/row", "B/cell", "B/row", "rebuild", "resident"
    );
    for projection in &mut projections {
        let started = Instant::now();
        projection.rebuild_from_base(&db).unwrap();
        let rebuild = started.elapsed().as_secs_f64();
        let residency = projection.residency();
        println!(
            "{:<22} {:>9} {:>9.2} {:>8.0} {:>9.0} {:>9.1}s {:>8.0}M",
            projection.name,
            residency.cells,
            residency.cells as f64 / rows as f64,
            residency.cell_bytes as f64 / residency.cells.max(1) as f64,
            residency.input_bytes as f64 / rows as f64,
            rebuild,
            residency.total_bytes() as f64 / 1048576.0,
        );
        let drift = projection.reconcile(&db).unwrap();
        assert!(
            drift.is_clean(),
            "{} initial drift: {drift:?}",
            projection.name
        );
    }

    // Recrawl rounds: dimension moves, score changes, deletes. Reconcile
    // EVERY round, not only at the end.
    println!();
    for round in 1..=rounds {
        let mutations = rows / 20;
        let started = Instant::now();
        for step in 0..mutations {
            let index = rng.below(rows);
            let id = format!("biz-{index:08}");
            if step % 23 == 0 {
                db.delete("businesses", &id).unwrap();
                continue;
            }
            let Some(existing) = db.get("businesses", &id).unwrap() else {
                continue;
            };
            let mut metadata = existing.metadata.clone();
            let score = rng.below(100);
            metadata["score"] = json!(score as f64);
            metadata["score_band"] = json!(format!("band-{}", score / 20));
            metadata["lcp"] = json!((rng.below(4000) + 500) as f64);
            if step % 3 == 0 {
                // The hard path: move the row between cells.
                metadata["host"] = json!(format!("host-{:03}", rng.skewed(HOSTS)));
            }
            if step % 7 == 0 {
                metadata["cms"] = json!(format!("cms-{:02}", rng.skewed(CMS)));
            }
            db.insert("businesses", Record::new(id).with_metadata(metadata))
                .unwrap();
        }
        let churn = started.elapsed().as_secs_f64();

        let mut catch_total = 0.0;
        let mut reconcile_total = 0.0;
        let mut events = 0usize;
        for projection in &mut projections {
            let started = Instant::now();
            events = projection.catch_up(&db).unwrap();
            catch_total += started.elapsed().as_secs_f64();
            let started = Instant::now();
            let drift = projection.reconcile(&db).unwrap();
            reconcile_total += started.elapsed().as_secs_f64();
            assert!(
                drift.is_clean(),
                "round {round}: {} drifted: {drift:?}",
                projection.name
            );
        }
        println!(
            "round {round}: churn {mutations} in {churn:.1}s | catch_up {events} events \
             {catch_total:.2}s ({:.0} ev/s) | reconcile x4 {reconcile_total:.1}s | ALL CLEAN",
            events as f64 / catch_total.max(1e-6)
        );
    }

    // Independent check against the base table, per projection.
    let live = db.scan_collection("businesses").unwrap();
    println!();
    for projection in &projections {
        let total: i64 = projection.cells().map(|(_, cell)| cell.count).sum();
        assert_eq!(
            total as usize,
            live.len(),
            "{}: cell counts must sum to COUNT(*)",
            projection.name
        );
    }
    let residency_total: u64 = projections
        .iter()
        .map(|projection| projection.residency().total_bytes())
        .sum();
    println!("live rows                {}", live.len());
    println!(
        "all 4 projections        {:.0} MiB resident ({:.0} B/row combined)",
        residency_total as f64 / 1048576.0,
        residency_total as f64 / live.len().max(1) as f64
    );
    println!(
        "process RSS              {:.0} MiB (baseline {baseline:.0})",
        rss_mib()
    );
    println!();
    println!("incremental == authoritative at every checkpoint: CLEAN");
}
