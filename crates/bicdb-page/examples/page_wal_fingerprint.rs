//! Deterministic page-store workload that prints a SHA-256 of the resulting
//! `store.wal` (and `store.pages`), so two engine builds can be compared byte
//! for byte. Used to qualify that the experimental page-delta code changes no
//! WAL byte while its gate is closed (`docs/page-delta-redo-design.md`, Q0).
//!
//! Usage: page_wal_fingerprint <fresh-dir> [pool-pages]

use bicdb_page::{PagedStore, PagedStoreOptions};
use sha2::{Digest, Sha256};

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = std::path::PathBuf::from(args.next().expect("fresh dir"));
    let pool_pages: u64 = args.next().map(|v| v.parse().unwrap()).unwrap_or(4096);
    let options = PagedStoreOptions::default()
        .with_page_size(4096)
        .with_buffer_pool_bytes(pool_pages * 4096)
        .with_fsync(false)
        .with_wal_max_bytes(1 << 30);
    {
        let (store, _) = PagedStore::open(&dir, options).unwrap();
        for round in 0..400u64 {
            let tx = store.begin();
            for i in 0..8u64 {
                let k = (round * 17 + i * 31) % 3000;
                let value = format!("v{round}-{}", "x".repeat(((k * 7 + round) % 200) as usize));
                store
                    .put(tx, format!("key-{k:06}").as_bytes(), value.as_bytes())
                    .unwrap();
            }
            if round % 5 == 2 {
                store
                    .delete(tx, format!("key-{:06}", (round * 3) % 3000).as_bytes())
                    .unwrap();
            }
            store.commit(tx).unwrap();
            if round % 9 == 4 {
                let tx = store.begin();
                store.put(tx, b"aborted", b"x").unwrap();
                store.abort(tx).unwrap();
            }
        }
        let (engaged, _) = store.page_deltas();
        eprintln!("deltas_engaged={engaged}");
    }
    for file in ["store.wal", "store.pages"] {
        let bytes = std::fs::read(dir.join(file)).unwrap_or_default();
        println!("{file} {} {:x}", bytes.len(), Sha256::digest(&bytes));
    }
}
