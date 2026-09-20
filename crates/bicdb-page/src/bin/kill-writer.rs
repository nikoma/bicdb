//! Writer process for the kill-recovery test. Not part of the product.
//!
//! A dedicated binary rather than a re-exec of the test harness. The first
//! version of this test re-executed its own test binary with an env var set,
//! which meant every test in the binary spawned another full test binary, each
//! of which spawned more: an unbounded process explosion. It hung rather than
//! exploding, but the design was wrong.
//!
//! This process can only write. It has no test harness, spawns nothing, and
//! stops on its own after a bounded number of rows so an orphan cannot run
//! forever if the parent dies first.

use std::io::Write;

use bicdb_page::{PagedStore, PagedStoreOptions};

/// Hard stop, so an orphaned writer cannot fill a disk.
const MAX_ROWS: usize = 5_000_000;
const BATCH: usize = 200;

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args.next().expect("usage: kill-writer <dir> [page_size]");
    let page_size: u32 = args
        .next()
        .and_then(|value| value.parse().ok())
        .unwrap_or(4096);

    let options = PagedStoreOptions::default()
        .with_page_size(page_size)
        .with_buffer_pool_bytes(4 * 1024 * 1024)
        // fsync ON: a kill test without it proves nothing, because the kernel
        // may still hold writes the process believed were durable.
        .with_fsync(true)
        .with_wal_max_bytes(2 * 1024 * 1024);

    let (store, _) = PagedStore::open(&dir, options).expect("child could not open store");

    let mut written = 0usize;
    while written < MAX_ROWS {
        let transaction = store.begin();
        for _ in 0..BATCH {
            let key = format!("k{written:07}");
            let value = format!("value-{written}-{}", "w".repeat(200));
            store
                .put(transaction, key.as_bytes(), value.as_bytes())
                .expect("child write failed");
            written += 1;
        }
        store.commit(transaction).expect("child commit failed");
        // Announced only AFTER commit returns, so anything the parent reads
        // here is durable by the store's own contract.
        println!("committed {written}");
        std::io::stdout().flush().ok();
    }
}
