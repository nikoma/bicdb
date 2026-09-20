//! Tier-1 scale harness for incremental aggregate projections.
//!
//! Answers the two questions that decide whether the design in
//! `docs/olap-cubes.md` survives the real corpus, using NO production data:
//!
//!   1. does `incremental == authoritative` hold at ~1M rows after a recrawl
//!      storm that moves rows between cells, and
//!   2. what does the per-record input state actually COST — the number §12
//!      says a projection's grain must be justified against.
//!
//! The corpus is value-free by design: only cardinality and skew matter to
//! the projection, so dimension values are synthetic. Distributions are
//! deliberately uneven (Zipf-ish categories, a dominant hosting provider, a
//! realistic missing-value rate) because uniform data hides the cells that
//! actually stress retract.
//!
//! Usage: `cargo run --release --example projection_scale -- [rows] [rounds]`

use std::time::Instant;

use bicdb_core::aggregate_projection::AggregateProjection;
use bicdb_core::{BicDb, DbConfig, Record};
use serde_json::json;

/// Deterministic xorshift: the same corpus every run, no rand dependency.
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

    /// Skewed index: squaring the uniform draw concentrates mass on the low
    /// indices, so a few categories dominate exactly as they do in a real
    /// business directory.
    fn skewed(&mut self, bound: usize) -> usize {
        let unit = (self.next() % 1_000_000) as f64 / 1_000_000.0;
        ((unit * unit) * bound as f64) as usize % bound.max(1)
    }
}

const STATES: usize = 36;
const CATEGORIES: usize = 500;
const HOSTS: usize = 50;

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
    let host = rng.skewed(HOSTS);
    // ~22% of businesses have no detectable hosting provider. Missing values
    // must still land in a cell, or cell counts stop summing to COUNT(*).
    let host_value = if rng.below(100) < 22 {
        serde_json::Value::Null
    } else {
        json!(format!("host-{host:03}"))
    };
    Record::new(format!("biz-{index:08}")).with_metadata(json!({
        "state": format!("state-{:02}", rng.below(STATES)),
        "category": format!("cat-{:03}", rng.skewed(CATEGORIES)),
        "host": host_value,
        "score": rng.below(100) as f64,
    }))
}

fn main() {
    let mut args = std::env::args().skip(1);
    let rows: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(200_000);
    let rounds: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(3);

    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_audit_events(true),
    )
    .unwrap();
    db.create_collection("businesses").unwrap();

    println!(
        "corpus: {rows} rows, {STATES} states x {CATEGORIES} categories x {HOSTS} hosts (skewed)"
    );
    let baseline_rss = rss_mib();

    let started = Instant::now();
    let mut rng = Rng(0x5eed_1234_9876_abcd);
    let records: Vec<Record> = (0..rows).map(|index| business(&mut rng, index)).collect();
    db.bulk_load_insert("businesses", records).unwrap();
    println!(
        "load                    {:>8.1}s",
        started.elapsed().as_secs_f64()
    );

    let mut projection = AggregateProjection::new(
        "website_market",
        "businesses",
        vec!["state".into(), "category".into(), "host".into()],
        vec!["score".into()],
    )
    .unwrap();

    // A bulk load emits no audit events, so the projection is created the way
    // it would be over existing data: rebuild from base, then follow the
    // stream.
    let started = Instant::now();
    let built = projection.rebuild_from_base(&db).unwrap();
    let build_secs = started.elapsed().as_secs_f64();
    println!(
        "rebuild_from_base       {:>8.1}s  ({built} rows, {} cells)",
        build_secs,
        projection.cell_count()
    );

    let residency = projection.residency();
    let per_row = residency.input_records.max(1) as f64;
    println!(
        "projection residency    {:>8.1} MiB  (cells {:.1} MiB / input state {:.1} MiB for {} records)",
        residency.total_bytes() as f64 / 1048576.0,
        residency.cell_bytes as f64 / 1048576.0,
        residency.input_bytes as f64 / 1048576.0,
        residency.input_records
    );
    println!(
        "  cell layout             {:>8} B/cell packed ({:.0} B/cell resident, {} cells)",
        projection.layout().cell_width(),
        residency.cell_bytes as f64 / residency.cells.max(1) as f64,
        residency.cells,
    );
    // Reported separately on purpose: once the state itself is compact, the
    // CONTAINER becomes the next enemy, and one blended number would hide it.
    println!(
        "  layout                  {:>8} B/row logical (identity {} + state {})",
        projection.layout().logical_bytes_per_row(),
        bicdb_core::aggregate_projection::ProjectionLayout::IDENTITY_WIDTH,
        projection.layout().state_width(),
    );
    println!(
        "  input state per row     {:>8.0} B logical | {:>4.0} B slab | {:>4.0} B resident (container overhead {:.0} B)",
        residency.input_logical_bytes as f64 / per_row,
        residency.slab_bytes as f64 / per_row,
        residency.input_bytes as f64 / per_row,
        (residency.input_bytes as f64 - residency.input_logical_bytes as f64) / per_row,
    );

    let started = Instant::now();
    let drift = projection.reconcile(&db).unwrap();
    println!(
        "reconcile (initial)     {:>8.1}s  clean={} compared={}",
        started.elapsed().as_secs_f64(),
        drift.is_clean(),
        drift.cells_compared
    );
    assert!(
        drift.is_clean(),
        "initial rebuild disagreed with base: {drift:?}"
    );

    // Recrawl storm: the churn that actually stresses retract. Every third
    // mutation MOVES the row between cells (new host) rather than only
    // changing the measure, because a score-only change never exercises the
    // hard path.
    for round in 1..=rounds {
        let mutations = rows / 10;
        let started = Instant::now();
        let mut moved = 0usize;
        let mut deleted = 0usize;
        for step in 0..mutations {
            let index = rng.below(rows);
            let id = format!("biz-{index:08}");
            if step % 17 == 0 {
                if db.delete("businesses", &id).unwrap() {
                    deleted += 1;
                }
                continue;
            }
            let Some(existing) = db.get("businesses", &id).unwrap() else {
                continue;
            };
            let mut metadata = existing.metadata.clone();
            metadata["score"] = json!(rng.below(100) as f64);
            if step % 3 == 0 {
                metadata["host"] = json!(format!("host-{:03}", rng.skewed(HOSTS)));
                moved += 1;
            }
            db.insert("businesses", Record::new(id).with_metadata(metadata))
                .unwrap();
        }
        let churn_secs = started.elapsed().as_secs_f64();

        let started = Instant::now();
        let applied = projection.catch_up(&db).unwrap();
        let catch_secs = started.elapsed().as_secs_f64();

        let started = Instant::now();
        let drift = projection.reconcile(&db).unwrap();
        let reconcile_secs = started.elapsed().as_secs_f64();

        println!(
            "round {round}: churn {mutations} ({moved} moved, {deleted} deleted) {churn_secs:.1}s | \
             catch_up {applied} events {catch_secs:.1}s | reconcile {reconcile_secs:.1}s clean={}",
            drift.is_clean()
        );
        assert!(drift.is_clean(), "round {round} drifted: {drift:?}");
    }

    // Replay the whole stream again: an at-least-once consumer restarting must
    // change nothing.
    let before = projection.residency();
    projection.catch_up(&db).unwrap();
    let drift = projection.reconcile(&db).unwrap();
    assert!(drift.is_clean(), "replay drifted: {drift:?}");
    assert_eq!(
        before.cells,
        projection.residency().cells,
        "replay changed cells"
    );

    // Independent check against the base table, not just cell-vs-cell.
    let live = db.scan_collection("businesses").unwrap();
    let total: i64 = projection.cells().map(|(_, cell)| cell.count).sum();
    assert_eq!(
        total as usize,
        live.len(),
        "cell counts must sum to COUNT(*)"
    );
    let cell_sum: f64 = projection.cells().map(|(_, cell)| cell.sum(0)).sum();
    let base_sum: f64 = live
        .iter()
        .filter_map(|record| record.metadata.get("score").and_then(|v| v.as_f64()))
        .sum();
    assert!(
        (cell_sum - base_sum).abs() < 1e-6,
        "summed measures drifted"
    );

    let residency = projection.residency();
    println!("---");
    println!(
        "final: {} live rows, {} cells",
        live.len(),
        projection.cell_count()
    );
    println!(
        "projection residency    {:>8.1} MiB  ({:.0} B/row logical, {:.0} B/row resident)",
        residency.total_bytes() as f64 / 1048576.0,
        residency.input_logical_bytes as f64 / residency.input_records.max(1) as f64,
        residency.input_bytes as f64 / residency.input_records.max(1) as f64
    );
    println!(
        "process RSS             {:>8.0} MiB  (baseline {baseline_rss:.0} MiB)",
        rss_mib()
    );
    println!("incremental == authoritative: CLEAN");
}
