//! Heap placement and the free-space map, end to end through the paged store.
//!
//! 1. **No unchanged page is imaged.** Every page image a commit appends to
//!    the page WAL must differ from the previous image of the same page. An
//!    identical image adds nothing recovery needs; it used to come from heap
//!    inserts that latched candidate pages for writing and then did not use
//!    them.
//! 2. **No committed row lost, and space reused, under churn.** A churn of
//!    updates, deletes, re-inserts and vacuum over a constant live set must
//!    keep every committed row readable and must not grow the file without
//!    bound. Before forgetting a page removed every free-space record naming
//!    it, and before inserts checked the page type, vacuum could free a page
//!    while a drifted record still advertised it; a later insert placed a
//!    committed row on the freed page, and the row was lost when the page was
//!    next allocated. This churn lost row 191 that way.

use std::collections::HashMap;
use std::path::Path;

use bicdb_page::{PagedStore, PagedStoreOptions, RecordKind, Wal};
use tempfile::TempDir;

fn options(page_size: u32, pool_bytes: u64) -> PagedStoreOptions {
    PagedStoreOptions::default()
        .with_page_size(page_size)
        .with_buffer_pool_bytes(pool_bytes)
        .with_fsync(false)
        // No automatic checkpoint: the whole run stays in one WAL generation
        // so every image can be compared with its predecessor.
        .with_wal_max_bytes(1 << 34)
}

fn key(index: usize) -> Vec<u8> {
    format!("key-{index:08}").into_bytes()
}

fn value(index: usize, round: usize, len: usize) -> Vec<u8> {
    let mut out = format!("v{index}-{round}-").into_bytes();
    out.resize(len.max(out.len()), b'a' + (index % 26) as u8);
    out
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }
}

/// Page images in the WAL that are byte-identical to the previous image of the
/// same page, and the total number of images.
fn unchanged_images(wal_path: &Path) -> (usize, usize) {
    let wal = Wal::open(wal_path, false).unwrap();
    let (records, _) = wal.read_all().unwrap();
    let mut last: HashMap<u64, Vec<u8>> = HashMap::new();
    let mut unchanged = 0;
    let mut images = 0;
    for record in records {
        if record.kind != RecordKind::PageImage {
            continue;
        }
        images += 1;
        if last.get(&record.page_id) == Some(&record.payload) {
            unchanged += 1;
        }
        last.insert(record.page_id, record.payload);
    }
    (unchanged, images)
}

fn run_mixed_workload(store: &PagedStore, rng: &mut Rng) {
    let mut next_key = 0usize;
    // One-row commits plus a hot-row update, like an OLTP order line.
    for round in 0..300 {
        let xid = store.begin();
        store
            .put(
                xid,
                &key(next_key),
                &value(next_key, round, 60 + rng.below(200)),
            )
            .unwrap();
        next_key += 1;
        store
            .put(xid, b"hot-counter", &value(0, round, 40))
            .unwrap();
        store.commit(xid).unwrap();
    }
    // Twenty-row commits.
    for round in 0..40 {
        let xid = store.begin();
        for _ in 0..20 {
            store
                .put(
                    xid,
                    &key(next_key),
                    &value(next_key, round, 40 + rng.below(400)),
                )
                .unwrap();
            next_key += 1;
        }
        store.commit(xid).unwrap();
    }
    // Update-only commits of existing rows, growing and shrinking.
    for round in 0..300 {
        let xid = store.begin();
        let target = rng.below(next_key);
        store
            .put(
                xid,
                &key(target),
                &value(target, round, 20 + rng.below(500)),
            )
            .unwrap();
        store.commit(xid).unwrap();
    }
    // Delete commits.
    for _ in 0..200 {
        let xid = store.begin();
        store.delete(xid, &key(rng.below(next_key))).unwrap();
        store.commit(xid).unwrap();
    }
}

#[test]
fn commits_never_image_an_unchanged_page() {
    for (page_size, pool_bytes) in [(4096u32, 8u64 << 20), (4096, 96 * 4096), (1024, 64 * 1024)] {
        let dir = TempDir::new().unwrap();
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ u64::from(page_size) ^ pool_bytes);
        {
            let (store, _) = PagedStore::open(dir.path(), options(page_size, pool_bytes)).unwrap();
            run_mixed_workload(&store, &mut rng);
            // Dropped without a checkpoint: the WAL keeps every image.
        }
        let (unchanged, images) = unchanged_images(&dir.path().join("store.wal"));
        eprintln!(
            "page_size={page_size} pool={pool_bytes}: {images} page images, {unchanged} unchanged"
        );
        assert!(images > 0);
        assert_eq!(
            unchanged, 0,
            "{unchanged} of {images} page images repeat the page's previous image \
             (page_size {page_size}, pool {pool_bytes})"
        );

        // And the store recovers from exactly that WAL.
        let (store, _) = PagedStore::open(dir.path(), options(page_size, pool_bytes)).unwrap();
        assert!(store.get(&key(0)).is_ok());
    }
}

#[test]
fn churn_reuses_space_instead_of_growing_the_file() {
    let dir = TempDir::new().unwrap();
    let (store, _) = PagedStore::open(dir.path(), options(4096, 4 << 20)).unwrap();
    let mut rng = Rng(0xD1B5_4A32_D192_ED03);
    let live = 3_000usize;
    let mut sizes = vec![0usize; live];
    for chunk in (0..live).collect::<Vec<_>>().chunks(50) {
        let xid = store.begin();
        for index in chunk {
            sizes[*index] = 40 + rng.below(360);
            store
                .put(xid, &key(*index), &value(*index, 0, sizes[*index]))
                .unwrap();
        }
        store.commit(xid).unwrap();
    }
    let loaded = store.snapshot().unwrap();

    for round in 1..=60 {
        // Each round rewrites about a fifth of the live set (updates of new
        // sizes, and delete + re-insert), then vacuums.
        let xid = store.begin();
        for _ in 0..live / 10 {
            let target = rng.below(live);
            sizes[target] = 40 + rng.below(360);
            store
                .put(xid, &key(target), &value(target, round, sizes[target]))
                .unwrap();
        }
        store.commit(xid).unwrap();
        let xid = store.begin();
        let doomed: Vec<usize> = (0..live / 10).map(|_| rng.below(live)).collect();
        for target in &doomed {
            store.delete(xid, &key(*target)).unwrap();
        }
        store.commit(xid).unwrap();
        let xid = store.begin();
        for target in &doomed {
            sizes[*target] = 40 + rng.below(360);
            store
                .put(xid, &key(*target), &value(*target, round, sizes[*target]))
                .unwrap();
        }
        store.commit(xid).unwrap();
        while store.vacuum(1 << 20).unwrap().versions_reclaimed > 0 {}
    }
    store.checkpoint().unwrap();
    let after = store.snapshot().unwrap();
    let live_bytes: usize = sizes.iter().sum();
    eprintln!(
        "CHURN live_rows={live} live_value_bytes={live_bytes} loaded_used_pages={} \
         final_used_pages={} final_page_count={} free_pages={}",
        loaded.used_data_pages, after.used_data_pages, after.page_count, after.free_pages
    );
    for index in 0..live {
        assert_eq!(
            store.get(&key(index)).unwrap().map(|v| v.len()),
            Some(sizes[index]),
            "row {index}"
        );
    }
    // Sixty rounds rewrite the live set about twelve times over. With space
    // reuse working the file stays within a small multiple of its loaded
    // size; without it, it grows with every round.
    assert!(
        after.used_data_pages <= loaded.used_data_pages * 3,
        "churn grew the used pages from {} to {}",
        loaded.used_data_pages,
        after.used_data_pages
    );
}
