//! CPU cost of the experimental page-delta path at the page layer, with fsync
//! OFF so the disk does not hide it: commits of a few small puts (an insert
//! and a hot-row update) into a fresh store, timed. Run once per arm:
//!
//!   page_delta_cpu_bench <fresh-dir> <commits> normal|disposable [fsync]
//!
//! With `fsync` the log is synced at every commit, as in production, and the
//! commit latency percentiles show what the disk adds.
//!
//! Deltas engage only for `disposable` with `BICDB_PAGE_WAL_DELTAS=1`.
//! Prints one JSON line. See `docs/page-delta-redo-design.md`.

use std::time::Instant;

use bicdb_page::{PagedStore, PagedStoreOptions};

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = std::path::PathBuf::from(args.next().expect("fresh dir"));
    let commits: u64 = args.next().expect("commits").parse().unwrap();
    let disposable = args.next().as_deref() == Some("disposable");
    let fsync = args.next().as_deref() == Some("fsync");
    let options = PagedStoreOptions::default()
        .with_buffer_pool_bytes(256 * 1024 * 1024)
        .with_fsync(fsync)
        .with_experimental_page_deltas_disposable(disposable);
    let (store, _) = PagedStore::open(&dir, options).unwrap();
    let started = Instant::now();
    let before = store.wal().snapshot();
    let mut latencies = Vec::with_capacity(commits as usize);
    for i in 0..commits {
        let commit_started = Instant::now();
        let tx = store.begin();
        let value = format!(
            "{{\"owner\":{},\"payload\":\"{}\"}}",
            i % 97,
            "p".repeat(120)
        );
        store
            .put(tx, format!("rows/{i:010}").as_bytes(), value.as_bytes())
            .unwrap();
        store
            .put(
                tx,
                format!("idx/owner/{:04}/{i:010}", i % 97).as_bytes(),
                b"",
            )
            .unwrap();
        store
            .put(
                tx,
                format!("hot/{:02}", i % 8).as_bytes(),
                format!("{i}").as_bytes(),
            )
            .unwrap();
        store.commit(tx).unwrap();
        latencies.push(commit_started.elapsed().as_micros() as u64);
    }
    latencies.sort_unstable();
    let pct = |p: f64| latencies[((latencies.len() - 1) as f64 * p) as usize];
    let elapsed = started.elapsed().as_secs_f64();
    let after = store.wal().snapshot();
    let (engaged, stats) = store.page_deltas();
    println!(
        "{{\"disposable\":{disposable},\"deltas_engaged\":{engaged},\"commits\":{commits},\"seconds\":{elapsed:.3},\"us_per_commit\":{:.1},\"wal_bytes_per_commit\":{:.0},\"checkpoints\":{},\"delta_records\":{},\"full_images\":{},\"p50_us\":{},\"p95_us\":{},\"p99_us\":{},\"max_us\":{}}}",
        elapsed * 1e6 / commits as f64,
        (after.bytes_appended - before.bytes_appended) as f64 / commits as f64,
        after.checkpoints - before.checkpoints,
        stats.delta_records,
        stats.full_images,
        pct(0.50),
        pct(0.95),
        pct(0.99),
        pct(1.0)
    );
}
