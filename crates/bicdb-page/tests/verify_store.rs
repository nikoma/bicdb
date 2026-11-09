//! The read-only store verifier against deliberately damaged stores.
//!
//! Every verification in this file goes through [`verify`], which hashes
//! every file of the store directory (SHA-256) before and after the run and
//! fails unless they are byte-identical, the directory listing is unchanged,
//! and no lock file appeared. So every scenario below, including the crash
//! states that a real open would rewrite, doubles as evidence that the
//! verifier never modifies a store.
//!
//! Damaged states come from three sources:
//! - the churn reproducer of the vacuum row-loss bug (the free-space
//!   placement fix's own test), run on code without that fix;
//! - crafted page edits that put a store into exactly the on-disk state the
//!   bug leaves, independent of whether the fix is present;
//! - the GAP-1 and GAP-2 crash reproducers from the recovery-semantics work,
//!   ported here as test-only helpers.
//!
//! Ground truth for each scenario is taken independently of the verifier:
//! reads through a normally opened copy of the store, compared with a model of
//! what was committed.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use bicdb_page::{
    verify_store_read_only, FindingClass, FindingSeverity, LogResetShape, PageError, PagedStore,
    PagedStoreOptions, StoreVerdict, StoreVerifyOptions, StoreVerifyReport, TupleLocator,
};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Harness.

type Snapshot = BTreeMap<PathBuf, (u64, String, std::time::SystemTime)>;

fn snapshot_tree(dir: &Path) -> Snapshot {
    let mut out = BTreeMap::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(current) = pending.pop() {
        for entry in std::fs::read_dir(&current).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let meta = std::fs::symlink_metadata(&path).unwrap();
            if meta.is_dir() {
                pending.push(path);
                continue;
            }
            let bytes = std::fs::read(&path).unwrap();
            let digest = hex::encode(Sha256::digest(&bytes));
            out.insert(
                path.strip_prefix(dir).unwrap().to_path_buf(),
                (meta.len(), digest, meta.modified().unwrap()),
            );
        }
    }
    out
}

/// Verify `dir` read-only and prove the run changed nothing.
fn verify_with(dir: &Path, options: &StoreVerifyOptions) -> StoreVerifyReport {
    let before = snapshot_tree(dir);
    let report = verify_store_read_only(dir, options).unwrap();
    let after = snapshot_tree(dir);
    assert_eq!(
        before, after,
        "the verifier changed the store (sha256, size, mtime or listing)"
    );
    assert!(!report.read_only.recovery_ran);
    report
}

fn verify(dir: &Path) -> StoreVerifyReport {
    verify_with(dir, &StoreVerifyOptions::default())
}

fn copy_dir(from: &Path) -> TempDir {
    let to = TempDir::new().unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_file() {
            std::fs::copy(entry.path(), to.path().join(entry.file_name())).unwrap();
        }
    }
    to
}

fn keys_of(report: &StoreVerifyReport, class: FindingClass) -> BTreeSet<Vec<u8>> {
    report
        .classes
        .iter()
        .filter(|summary| summary.class == class)
        .flat_map(|summary| summary.samples.iter())
        .filter_map(|finding| finding.key.clone())
        .collect()
}

fn severities(report: &StoreVerifyReport, severity: FindingSeverity) -> Vec<FindingClass> {
    report
        .classes
        .iter()
        .filter(|summary| summary.severity == severity)
        .map(|summary| summary.class)
        .collect()
}

fn summary_line(report: &StoreVerifyReport) -> String {
    let classes: Vec<String> = report
        .classes
        .iter()
        .map(|summary| format!("{:?}={}", summary.class, summary.count))
        .collect();
    format!(
        "verdict={:?} complete={} entries={} visible={} deleted={} heap_versions={} reachable={} \
         passes={} log_images={} classes=[{}]",
        report.verdict,
        report.complete,
        report.counters.index_entries,
        report.counters.visible_rows,
        report.counters.deleted_rows,
        report.counters.heap_versions_swept,
        report.counters.reachable_versions_swept,
        report.counters.key_tree_passes,
        report.log.pending_page_images,
        classes.join(" ")
    )
}

fn many_samples() -> StoreVerifyOptions {
    StoreVerifyOptions {
        max_samples_per_class: 100_000,
        ..StoreVerifyOptions::default()
    }
}

fn options(page_size: u32, pool_bytes: u64) -> PagedStoreOptions {
    PagedStoreOptions::default()
        .with_page_size(page_size)
        .with_buffer_pool_bytes(pool_bytes)
        .with_fsync(false)
        .with_wal_max_bytes(1 << 34)
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

// Raw page access for crafted damage (test-only; monolithic layout).

fn raw_read(dir: &Path, page_size: u32, page_id: u64) -> Vec<u8> {
    use std::os::unix::fs::FileExt;
    let file = std::fs::File::open(dir.join("store.pages")).unwrap();
    let mut buffer = vec![0u8; page_size as usize];
    file.read_exact_at(&mut buffer, page_id * u64::from(page_size))
        .unwrap();
    buffer
}

fn raw_write(dir: &Path, page_size: u32, page_id: u64, mut page: Vec<u8>) {
    use std::os::unix::fs::FileExt;
    let generation = u64::from_le_bytes(page[16..24].try_into().unwrap());
    bicdb_page::page::finalize(&mut page, generation);
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(dir.join("store.pages"))
        .unwrap();
    file.write_all_at(&page, page_id * u64::from(page_size))
        .unwrap();
}

fn raw_pages(dir: &Path, page_size: u32) -> u64 {
    std::fs::metadata(dir.join("store.pages")).unwrap().len() / u64::from(page_size)
}

fn raw_free_pages(dir: &Path, page_size: u32) -> Vec<u64> {
    (1..raw_pages(dir, page_size))
        .filter(|page| raw_read(dir, page_size, *page)[6] == 6)
        .collect()
}

fn u16_at(page: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([page[at], page[at + 1]])
}

fn put_u16(page: &mut [u8], at: usize, value: u16) {
    page[at..at + 2].copy_from_slice(&value.to_le_bytes());
}

/// Rewrite the 14-byte locator stored for `key` in whichever key-tree leaf
/// holds it.
fn raw_repoint(dir: &Path, page_size: u32, key: &[u8], from: TupleLocator, to: TupleLocator) {
    let mut needle = key.to_vec();
    needle.extend_from_slice(&from.encode());
    for page_id in 1..raw_pages(dir, page_size) {
        let mut page = raw_read(dir, page_size, page_id);
        if page[6] != 5 {
            continue;
        }
        if let Some(at) = page
            .windows(needle.len())
            .position(|window| window == needle.as_slice())
        {
            let at = at + key.len();
            page[at..at + 14].copy_from_slice(&to.encode());
            raw_write(dir, page_size, page_id, page);
            return;
        }
    }
    panic!("no key-tree leaf holds the key with that locator");
}

/// Put a store into state A exactly as the vacuum row-loss bug leaves it,
/// by editing pages: the live row `key` (whose current version is at `at`)
/// is moved onto the Free page `free`, slot 0 generation 1 (the free page's
/// bytes 52..56 are zero, so an insert onto it starts there), the page keeps
/// its Free type and free-list link, and the key's head is repointed.
fn craft_state_a(dir: &Path, page_size: u32, key: &[u8], at: TupleLocator, free: u64) {
    let mut heap = raw_read(dir, page_size, at.page_id);
    let entry = 64 + at.slot as usize * 8;
    let offset = u16_at(&heap, entry) as usize;
    let len = u16_at(&heap, entry + 2) as usize;
    let tuple = heap[offset..offset + len].to_vec();
    // Retire the original slot so only the misplaced copy remains.
    put_u16(&mut heap, entry, 0);
    raw_write(dir, page_size, at.page_id, heap);

    let mut page = raw_read(dir, page_size, free);
    assert_eq!(page[6], 6, "page {free} is not Free");
    let tuple_at = page_size as usize - 8 - len;
    page[tuple_at..tuple_at + len].copy_from_slice(&tuple);
    put_u16(&mut page, 36, 64 + 8); // free_start
    put_u16(&mut page, 38, tuple_at as u16); // free_end
    put_u16(&mut page, 40, 1); // slot_count
    put_u16(&mut page, 64, tuple_at as u16);
    put_u16(&mut page, 66, len as u16);
    page[68..72].copy_from_slice(&1u32.to_le_bytes());
    raw_write(dir, page_size, free, page);

    raw_repoint(dir, page_size, key, at, TupleLocator::new(free, 0, 1));
}

/// Model-tracked keys and the locator of each key's current version.
struct Model {
    live: BTreeMap<Vec<u8>, Vec<u8>>,
    deleted: BTreeSet<Vec<u8>>,
}

/// Keys whose committed value no longer reads back, through a normal open of
/// a copy (the original stays untouched for the verifier).
fn lost_keys(dir: &Path, page_size: u32, model: &Model) -> BTreeMap<Vec<u8>, String> {
    let copy = copy_dir(dir);
    let (store, _) = PagedStore::open(copy.path(), options(page_size, 64 << 20)).unwrap();
    let mut lost = BTreeMap::new();
    for (key, value) in &model.live {
        match store.get(key) {
            Ok(Some(found)) if &found == value => {}
            Ok(Some(_)) => {
                lost.insert(key.clone(), "reads another value".to_string());
            }
            Ok(None) => {
                lost.insert(key.clone(), "reads absent".to_string());
            }
            Err(error) => {
                lost.insert(key.clone(), format!("read fails: {error}"));
            }
        }
    }
    lost
}

const MISSING_ROW: [FindingClass; 4] = [
    FindingClass::MissingRowPageReused,
    FindingClass::MissingRowOnFreePage,
    FindingClass::MissingRowStaleHeapSlot,
    FindingClass::MissingRowBeyondPageCount,
];

// ---------------------------------------------------------------------------
// Clean stores: no false positives.

#[test]
fn a_clean_checkpointed_store_is_clean_and_counts_every_row() {
    let dir = TempDir::new().unwrap();
    {
        let (store, _) = PagedStore::open(dir.path(), options(4096, 8 << 20)).unwrap();
        for chunk in (0..2_000usize).collect::<Vec<_>>().chunks(100) {
            let xid = store.begin();
            for index in chunk {
                let value = if index % 97 == 0 {
                    vec![b'w'; 20_000] // overflow chains
                } else {
                    format!("value-{index}").into_bytes()
                };
                store
                    .put(xid, format!("key-{index:06}").as_bytes(), &value)
                    .unwrap();
            }
            store.commit(xid).unwrap();
        }
        store.checkpoint().unwrap();
    }
    let report = verify(dir.path());
    eprintln!("CLEAN {}", summary_line(&report));
    assert_eq!(report.verdict, StoreVerdict::Clean, "{report:#?}");
    assert!(report.complete);
    assert_eq!(report.counters.index_entries, 2_000);
    assert_eq!(report.counters.visible_rows, 2_000);
    assert_eq!(report.counters.reachable_versions_swept, 2_000);
    assert_eq!(report.counters.heap_versions_swept, 2_000);
    assert!(report.classes.is_empty(), "{report:#?}");
}

#[test]
fn a_crashed_store_is_verified_as_recovery_would_leave_it() {
    let dir = TempDir::new().unwrap();
    let mut model = BTreeMap::new();
    {
        let (store, _) = PagedStore::open(dir.path(), options(4096, 8 << 20)).unwrap();
        let xid = store.begin();
        for index in 0..300usize {
            store
                .put(xid, format!("base-{index:05}").as_bytes(), b"base")
                .unwrap();
        }
        store.commit(xid).unwrap();
        store.checkpoint().unwrap();
        // Committed after the checkpoint: their pages exist only as log
        // images and in the pool, and the process "crashes".
        for round in 0..20usize {
            let xid = store.begin();
            for index in 0..60usize {
                let key = format!("after-{round:02}-{index:03}").into_bytes();
                let value = format!("v{round}").into_bytes();
                store.put(xid, &key, &value).unwrap();
                model.insert(key, value);
            }
            store.commit(xid).unwrap();
        }
        let open = store.begin();
        store.put(open, b"in-flight", b"never committed").unwrap();
    }
    let crashed = verify(dir.path());
    eprintln!("CRASHED {}", summary_line(&crashed));
    assert!(crashed.log.pending_page_images > 0);
    assert_eq!(crashed.log.replay_scope, "full_chain");
    assert!(
        severities(&crashed, FindingSeverity::Inconsistent).is_empty()
            && severities(&crashed, FindingSeverity::NeedsExternalCrossCheck).is_empty(),
        "{crashed:#?}"
    );
    assert_eq!(crashed.counters.visible_rows, 300 + model.len() as u64);

    // The same store after a real recovery and checkpoint, in a copy.
    let copy = copy_dir(dir.path());
    {
        let (store, _) = PagedStore::open(copy.path(), options(4096, 8 << 20)).unwrap();
        for (key, value) in &model {
            assert_eq!(store.get(key).unwrap().as_ref(), Some(value));
        }
        store.checkpoint().unwrap();
    }
    let recovered = verify(copy.path());
    eprintln!("RECOVERED {}", summary_line(&recovered));
    assert_eq!(recovered.log.pending_page_images, 0);
    assert_eq!(
        recovered.counters.visible_rows,
        crashed.counters.visible_rows
    );
    assert_eq!(
        recovered.counters.index_entries,
        crashed.counters.index_entries
    );
    assert_eq!(
        recovered.counters.versions_examined,
        crashed.counters.versions_examined
    );
}

/// Heavy churn with deletes that stay deleted, vacuum and checkpoints, at
/// 8 KiB pages, then verification with the default budget and with a budget
/// so small the key tree is walked once per heap window. Clean means: no
/// finding the ground truth contradicts. Deleted-and-vacuumed keys leave
/// dangling index entries (autovacuum never sweeps them), which the verifier
/// must list as needing a cross-check, and only those.
#[test]
fn a_large_store_after_heavy_churn_vacuum_and_checkpoints_has_no_false_positives() {
    const PAGE: u32 = 8192;
    let dir = TempDir::new().unwrap();
    let mut model = Model {
        live: BTreeMap::new(),
        deleted: BTreeSet::new(),
    };
    let key = |index: usize| format!("row-{index:07}").into_bytes();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let rows = 60_000usize;
    {
        let (store, _) = PagedStore::open(dir.path(), options(PAGE, 16 << 20)).unwrap();
        for chunk in (0..rows).collect::<Vec<_>>().chunks(1_000) {
            let xid = store.begin();
            for index in chunk {
                let len = if index % 501 == 0 {
                    30_000
                } else {
                    30 + rng.below(300)
                };
                let value = vec![b'a' + (index % 26) as u8; len];
                store.put(xid, &key(*index), &value).unwrap();
                model.live.insert(key(*index), value);
            }
            store.commit(xid).unwrap();
        }
        store.checkpoint().unwrap();
        for round in 0..12usize {
            let xid = store.begin();
            for _ in 0..rows / 8 {
                let target = rng.below(rows);
                if model.deleted.contains(&key(target)) {
                    continue;
                }
                let value = format!("r{round}-{}", "u".repeat(rng.below(400))).into_bytes();
                store.put(xid, &key(target), &value).unwrap();
                model.live.insert(key(target), value);
            }
            store.commit(xid).unwrap();
            let xid = store.begin();
            for _ in 0..rows / 40 {
                let target = rng.below(rows);
                if store.delete(xid, &key(target)).unwrap() {
                    model.live.remove(&key(target));
                    model.deleted.insert(key(target));
                }
            }
            store.commit(xid).unwrap();
            while store.vacuum(1 << 20).unwrap().versions_reclaimed > 0 {}
            if round % 3 == 2 {
                store.checkpoint().unwrap();
            }
        }
        store.checkpoint().unwrap();
    }
    let lost = lost_keys(dir.path(), PAGE, &model);
    let report = verify_with(dir.path(), &many_samples());
    eprintln!(
        "LARGE-CHURN lost_by_ground_truth={} {}",
        lost.len(),
        summary_line(&report)
    );
    assert!(report.complete);
    assert_eq!(
        report.count(FindingClass::UnreachableCommittedLiveVersion),
        0
    );
    for class in severities(&report, FindingSeverity::Inconsistent) {
        assert_eq!(
            class,
            FindingClass::RowOnFreePage,
            "only a real misplaced row may be inconsistent here: {report:#?}"
        );
    }
    // Every key the verifier calls possibly lost is either deleted (the
    // cross-check would clear it) or really lost.
    for class in MISSING_ROW {
        for key in keys_of(&report, class) {
            assert!(
                model.deleted.contains(&key) || lost.contains_key(&key),
                "{class:?} flags {} which the model holds live and which reads back",
                String::from_utf8_lossy(&key)
            );
        }
    }
    // Every really lost key is flagged.
    let flagged: BTreeSet<Vec<u8>> = MISSING_ROW
        .iter()
        .chain(
            [
                FindingClass::RowOnFreePage,
                FindingClass::ReferencedPageUnreadable,
            ]
            .iter(),
        )
        .flat_map(|class| keys_of(&report, *class))
        .collect();
    for key in lost.keys() {
        assert!(
            flagged.contains(key),
            "lost {} not flagged",
            String::from_utf8_lossy(key)
        );
    }
    // Live rows are counted exactly, minus the lost ones.
    assert_eq!(
        report.counters.visible_rows,
        (model.live.len() - lost.len()) as u64,
        "{}",
        summary_line(&report)
    );

    // The same verdict with the key tree walked once per small heap window.
    let small = verify_with(
        dir.path(),
        &StoreVerifyOptions {
            max_memory_bytes: 1024 * 1024,
            page_cache_pages: 16,
            heap_window_pages: Some(64),
            max_samples_per_class: 100_000,
            ..StoreVerifyOptions::default()
        },
    );
    eprintln!("LARGE-CHURN-WINDOWED {}", summary_line(&small));
    assert!(
        small.counters.key_tree_passes > 1,
        "{}",
        summary_line(&small)
    );
    assert_eq!(small.verdict, report.verdict);
    for summary in &report.classes {
        assert_eq!(
            small.count(summary.class),
            summary.count,
            "{:?}",
            summary.class
        );
    }
    assert_eq!(
        small.counters.heap_versions_swept,
        report.counters.heap_versions_swept
    );
    assert_eq!(
        small.counters.reachable_versions_swept,
        report.counters.reachable_versions_swept
    );

    // Sweeping the dangling entries of deleted keys removes exactly the
    // findings the cross-check would have cleared.
    let swept = copy_dir(dir.path());
    {
        let (store, _) = PagedStore::open(swept.path(), options(PAGE, 16 << 20)).unwrap();
        store.sweep_dead_index_entries(usize::MAX).unwrap();
        store.checkpoint().unwrap();
    }
    let after_sweep = verify_with(swept.path(), &many_samples());
    eprintln!("LARGE-CHURN-SWEPT {}", summary_line(&after_sweep));
    for class in MISSING_ROW {
        for key in keys_of(&after_sweep, class) {
            assert!(
                lost.contains_key(&key),
                "{class:?} still flags deleted key after the sweep"
            );
        }
    }
    if lost.is_empty() && report.count(FindingClass::RowOnFreePage) == 0 {
        assert!(
            matches!(
                after_sweep.verdict,
                StoreVerdict::Clean | StoreVerdict::AtRisk
            ),
            "{after_sweep:#?}"
        );
    }
}

/// Inserts, updates and checkpoints on a segmented store, no deletes and no
/// vacuum: nothing can be freed, so the verdict must be exactly clean.
#[test]
fn a_segmented_store_without_deletes_is_exactly_clean() {
    let dir = TempDir::new().unwrap();
    let store_options = options(4096, 4 << 20)
        .with_extent_bytes(1024 * 1024)
        .with_wal_segment_bytes(256 * 1024);
    let mut model = BTreeMap::new();
    {
        let (store, _) = PagedStore::open(dir.path(), store_options.clone()).unwrap();
        let mut rng = Rng(7);
        for round in 0..30usize {
            let xid = store.begin();
            for _ in 0..400 {
                let index = rng.below(5_000);
                let value = format!("{round}-{}", "x".repeat(rng.below(200))).into_bytes();
                store
                    .put(xid, format!("k{index:05}").as_bytes(), &value)
                    .unwrap();
                model.insert(index, value);
            }
            store.commit(xid).unwrap();
            if round % 7 == 6 {
                store.checkpoint().unwrap();
            }
        }
    }
    let report = verify(dir.path());
    eprintln!("SEGMENTED {}", summary_line(&report));
    assert_eq!(report.store.layout, "segmented");
    assert!(report.store.page_files > 1);
    assert!(report.log.sealed_segments > 0 || report.log.active_bytes > 0);
    assert_eq!(report.verdict, StoreVerdict::Clean, "{report:#?}");
    assert_eq!(report.counters.visible_rows, model.len() as u64);
    assert_eq!(
        report.count(FindingClass::UnreachableCommittedLiveVersion),
        0
    );
}

// ---------------------------------------------------------------------------
// Vacuum row loss.

/// Fill a store, delete every row on some pages, vacuum so those pages are
/// freed, checkpoint, and close. Returns the store directory, the live keys
/// with their current locators, and the freed pages.
fn store_with_freed_pages(page_size: u32) -> (TempDir, BTreeMap<Vec<u8>, TupleLocator>, Vec<u64>) {
    store_with_freed_pages_swept(page_size, true)
}

fn store_with_freed_pages_swept(
    page_size: u32,
    sweep: bool,
) -> (TempDir, BTreeMap<Vec<u8>, TupleLocator>, Vec<u64>) {
    let dir = TempDir::new().unwrap();
    let mut live = BTreeMap::new();
    {
        let (store, _) = PagedStore::open(dir.path(), options(page_size, 8 << 20)).unwrap();
        let xid = store.begin();
        for index in 0..200usize {
            store
                .put(xid, format!("doomed-{index:04}").as_bytes(), &[b'd'; 120])
                .unwrap();
        }
        store.commit(xid).unwrap();
        let xid = store.begin();
        for index in 0..200usize {
            let key = format!("kept-{index:04}").into_bytes();
            let locator = store
                .put_returning_locator(xid, &key, format!("kept value {index}").as_bytes())
                .unwrap();
            live.insert(key, locator);
        }
        store.commit(xid).unwrap();
        let xid = store.begin();
        for index in 0..200usize {
            store
                .delete(xid, format!("doomed-{index:04}").as_bytes())
                .unwrap();
        }
        store.commit(xid).unwrap();
        while store.vacuum(1 << 20).unwrap().versions_reclaimed > 0 {}
        // Index entries of the deleted keys are swept so the only dangling
        // entries in these tests are the ones the test creates.
        if sweep {
            store.sweep_dead_index_entries(usize::MAX).unwrap();
        }
        store.checkpoint().unwrap();
    }
    let free = raw_free_pages(dir.path(), page_size);
    assert!(!free.is_empty(), "vacuum freed no page");
    (dir, live, free)
}

#[test]
fn a_freshly_freed_store_is_clean() {
    let (dir, live, _) = store_with_freed_pages(4096);
    let report = verify(dir.path());
    eprintln!("FREED {}", summary_line(&report));
    assert_eq!(report.counters.visible_rows, live.len() as u64);
    assert!(
        matches!(report.verdict, StoreVerdict::Clean | StoreVerdict::AtRisk),
        "{report:#?}"
    );
    for class in severities(&report, FindingSeverity::AtRisk) {
        assert_eq!(class, FindingClass::StaleFreeSpaceRecord);
    }
}

/// The bug's precondition: a free-space record that advertises a page
/// vacuum freed. Created here the way the delete path drifts records
/// (`update_free_space(page, 0, after)`).
#[test]
fn a_stale_free_space_record_for_a_freed_page_is_reported_at_risk() {
    let (dir, _, free) = store_with_freed_pages(4096);
    {
        let (store, _) = PagedStore::open(dir.path(), options(4096, 8 << 20)).unwrap();
        store.catalog().update_free_space(free[0], 0, 4000).unwrap();
        store.checkpoint().unwrap();
    }
    let report = verify_with(dir.path(), &many_samples());
    eprintln!("STALE-RECORD {}", summary_line(&report));
    let pages: Vec<u64> = report
        .classes
        .iter()
        .filter(|summary| summary.class == FindingClass::StaleFreeSpaceRecord)
        .flat_map(|summary| summary.samples.iter())
        .filter_map(|finding| finding.page_id)
        .collect();
    assert!(pages.contains(&free[0]), "{report:#?}");
    assert!(severities(&report, FindingSeverity::Inconsistent).is_empty());
}

/// State A by crafted page edit, then state B by letting the engine
/// allocate the freed page again. Independent of the placement fix.
#[test]
fn state_a_and_then_state_b_are_detected_from_crafted_pages() {
    const PAGE: u32 = 4096;
    let (dir, live, free) = store_with_freed_pages(PAGE);
    let (victim, at) = live.iter().next().map(|(k, l)| (k.clone(), *l)).unwrap();
    let victim_value = b"kept value 0".to_vec();
    craft_state_a(dir.path(), PAGE, &victim, at, free[0]);

    // State A: readable, on a Free page.
    let report = verify_with(dir.path(), &many_samples());
    eprintln!("STATE-A {}", summary_line(&report));
    assert_eq!(report.verdict, StoreVerdict::Inconsistent);
    assert_eq!(report.count(FindingClass::RowOnFreePage), 1, "{report:#?}");
    assert_eq!(
        keys_of(&report, FindingClass::RowOnFreePage),
        BTreeSet::from([victim.clone()])
    );
    let finding = &report
        .classes
        .iter()
        .find(|summary| summary.class == FindingClass::RowOnFreePage)
        .unwrap()
        .samples[0];
    assert_eq!(finding.page_id, Some(free[0]));
    assert_eq!(finding.chain_depth, Some(0));
    assert!(finding.detail.contains("readers see"), "{}", finding.detail);
    for class in MISSING_ROW {
        assert_eq!(report.count(class), 0, "{class:?}");
    }
    assert_eq!(
        report.count(FindingClass::UnreachableCommittedLiveVersion),
        0
    );
    {
        // Ground truth: the row reads back through a normal open.
        let copy = copy_dir(dir.path());
        let (store, _) = PagedStore::open(copy.path(), options(PAGE, 8 << 20)).unwrap();
        assert_eq!(store.get(&victim).unwrap(), Some(victim_value.clone()));
    }

    // State B: the engine allocates the freed pages again (overflow chains
    // take whole pages from the free list).
    {
        let (store, _) = PagedStore::open(dir.path(), options(PAGE, 8 << 20)).unwrap();
        let xid = store.begin();
        for index in 0..(free.len() + 2) {
            store
                .put(
                    xid,
                    format!("wide-{index}").as_bytes(),
                    &vec![b'w'; 3 * PAGE as usize],
                )
                .unwrap();
        }
        store.commit(xid).unwrap();
        store.checkpoint().unwrap();
    }
    assert_ne!(
        raw_read(dir.path(), PAGE, free[0])[6],
        6,
        "the freed page was not reused"
    );
    let report = verify_with(dir.path(), &many_samples());
    eprintln!("STATE-B {}", summary_line(&report));
    // Ground truth for every key. On an engine without the placement fix the
    // wide inserts above can themselves land on a freed page through a stale
    // free-space record and be lost the same way; those are real losses too.
    let mut model = Model {
        live: live
            .keys()
            .enumerate()
            .map(|(index, key)| (key.clone(), format!("kept value {index}").into_bytes()))
            .collect(),
        deleted: BTreeSet::new(),
    };
    for index in 0..(free.len() + 2) {
        model.live.insert(
            format!("wide-{index}").into_bytes(),
            vec![b'w'; 3 * PAGE as usize],
        );
    }
    let lost = lost_keys(dir.path(), PAGE, &model);
    eprintln!(
        "STATE-B lost_by_ground_truth={} ({:?})",
        lost.len(),
        lost.iter()
            .map(|(key, how)| format!("{} {how}", String::from_utf8_lossy(key)))
            .collect::<Vec<_>>()
    );
    assert!(lost.contains_key(&victim));
    let missing: BTreeSet<Vec<u8>> = MISSING_ROW
        .iter()
        .flat_map(|class| keys_of(&report, *class))
        .collect();
    let on_foreign = keys_of(&report, FindingClass::RowOnNonHeapPage);
    // The victim is missing (its page was reused). A key flagged missing is
    // really lost. A row written into a page another structure owns may
    // still read back, but whichever structure lost the overwrite is lost.
    assert!(missing.contains(&victim), "{report:#?}");
    for key in &missing {
        assert!(
            lost.contains_key(key),
            "{} flagged missing but reads back",
            String::from_utf8_lossy(key)
        );
    }
    for key in lost.keys() {
        assert!(
            missing.contains(key) || on_foreign.contains(key),
            "lost {} not flagged",
            String::from_utf8_lossy(key)
        );
    }
    assert_eq!(report.count(FindingClass::RowOnFreePage), 0);
    assert_ne!(report.verdict, StoreVerdict::Clean);
    assert!(!report.external_evidence.is_empty());
}

/// Why a dangling head on a reused non-heap page is not proof of loss: a key
/// that was deleted, vacuumed and never swept (automatic vacuum does not
/// sweep index entries) leaves exactly that signature once its page is
/// allocated again. The verifier must list it as needing a cross-check.
#[test]
fn a_deleted_key_whose_page_was_reused_is_ambiguous_not_proof_of_loss() {
    const PAGE: u32 = 4096;
    let (dir, live, free) = store_with_freed_pages_swept(PAGE, false);
    {
        let (store, _) = PagedStore::open(dir.path(), options(PAGE, 8 << 20)).unwrap();
        let xid = store.begin();
        for index in 0..(free.len() + 2) {
            store
                .put(
                    xid,
                    format!("wide-{index}").as_bytes(),
                    &vec![b'w'; 3 * PAGE as usize],
                )
                .unwrap();
        }
        store.commit(xid).unwrap();
        store.checkpoint().unwrap();
    }
    let report = verify_with(dir.path(), &many_samples());
    eprintln!("DELETED-REUSED {}", summary_line(&report));
    let doomed: BTreeSet<Vec<u8>> = (0..200usize)
        .map(|index| format!("doomed-{index:04}").into_bytes())
        .collect();
    let reused = keys_of(&report, FindingClass::MissingRowPageReused);
    let deleted_on_reused: Vec<&Vec<u8>> =
        reused.iter().filter(|key| doomed.contains(*key)).collect();
    eprintln!(
        "DELETED-REUSED deleted_keys_on_reused_pages={} of {} reused-page findings",
        deleted_on_reused.len(),
        reused.len()
    );
    assert!(
        !deleted_on_reused.is_empty(),
        "expected deleted keys whose freed page now holds another structure"
    );
    assert_eq!(
        FindingClass::MissingRowPageReused.severity(),
        FindingSeverity::NeedsExternalCrossCheck
    );
    // None of the kept rows is flagged unless it really reads wrong.
    let mut model = Model {
        live: live
            .keys()
            .enumerate()
            .map(|(index, key)| (key.clone(), format!("kept value {index}").into_bytes()))
            .collect(),
        deleted: doomed.clone(),
    };
    for index in 0..(free.len() + 2) {
        model.live.insert(
            format!("wide-{index}").into_bytes(),
            vec![b'w'; 3 * PAGE as usize],
        );
    }
    let lost = lost_keys(dir.path(), PAGE, &model);
    for class in MISSING_ROW {
        for key in keys_of(&report, class) {
            assert!(
                doomed.contains(&key) || lost.contains_key(&key),
                "{class:?} flags {} which reads back",
                String::from_utf8_lossy(&key)
            );
        }
    }
    for key in lost.keys() {
        let flagged = MISSING_ROW
            .iter()
            .chain([FindingClass::RowOnNonHeapPage].iter())
            .any(|class| keys_of(&report, *class).contains(key));
        assert!(flagged, "lost {} not flagged", String::from_utf8_lossy(key));
    }
}

/// The free-space placement fix's churn reproducer, run on this tree. On code
/// without the fix it loses committed rows (row 191 at the time of the fix);
/// the verifier must flag every lost key and nothing that reads back. The
/// churn re-inserts every deleted key in the same round, so at the end every
/// key is live and no dangling entry is legitimate.
#[test]
fn the_churn_reproducer_of_the_vacuum_row_loss_bug_is_detected() {
    let dir = TempDir::new().unwrap();
    let key = |index: usize| format!("key-{index:08}").into_bytes();
    let value = |index: usize, round: usize, len: usize| {
        let mut out = format!("v{index}-{round}-").into_bytes();
        out.resize(len.max(out.len()), b'a' + (index % 26) as u8);
        out
    };
    let mut model = Model {
        live: BTreeMap::new(),
        deleted: BTreeSet::new(),
    };
    {
        let (store, _) = PagedStore::open(dir.path(), options(4096, 4 << 20)).unwrap();
        let mut rng = Rng(0xD1B5_4A32_D192_ED03);
        let live = 3_000usize;
        for chunk in (0..live).collect::<Vec<_>>().chunks(50) {
            let xid = store.begin();
            for index in chunk {
                let len = 40 + rng.below(360);
                store
                    .put(xid, &key(*index), &value(*index, 0, len))
                    .unwrap();
                model.live.insert(key(*index), value(*index, 0, len));
            }
            store.commit(xid).unwrap();
        }
        for round in 1..=60 {
            let xid = store.begin();
            for _ in 0..live / 10 {
                let target = rng.below(live);
                let len = 40 + rng.below(360);
                store
                    .put(xid, &key(target), &value(target, round, len))
                    .unwrap();
                model.live.insert(key(target), value(target, round, len));
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
                let len = 40 + rng.below(360);
                store
                    .put(xid, &key(*target), &value(*target, round, len))
                    .unwrap();
                model.live.insert(key(*target), value(*target, round, len));
            }
            store.commit(xid).unwrap();
            while store.vacuum(1 << 20).unwrap().versions_reclaimed > 0 {}
        }
        store.checkpoint().unwrap();
    }
    let lost = lost_keys(dir.path(), 4096, &model);
    let report = verify_with(dir.path(), &many_samples());
    eprintln!(
        "CHURN lost_by_ground_truth={} {}",
        lost.len(),
        summary_line(&report)
    );
    for (key, how) in lost.iter().take(5) {
        eprintln!("  lost {} ({how})", String::from_utf8_lossy(key));
    }
    let missing: BTreeSet<Vec<u8>> = MISSING_ROW
        .iter()
        .chain([FindingClass::ReferencedPageUnreadable].iter())
        .flat_map(|class| keys_of(&report, *class))
        .collect();
    let on_foreign = keys_of(&report, FindingClass::RowOnNonHeapPage);
    // Every key flagged missing is lost, and every lost key is flagged
    // (missing, or written into a page another structure owns). Keys still
    // readable on a Free page are state A.
    for key in &missing {
        assert!(
            lost.contains_key(key),
            "{} flagged but reads back",
            String::from_utf8_lossy(key)
        );
    }
    for key in lost.keys() {
        assert!(
            missing.contains(key) || on_foreign.contains(key),
            "lost {} not flagged",
            String::from_utf8_lossy(key)
        );
    }
    for key in keys_of(&report, FindingClass::RowOnFreePage) {
        assert!(!lost.contains_key(&key));
    }
    if lost.is_empty() && report.count(FindingClass::RowOnFreePage) == 0 {
        eprintln!("CHURN: no row was misplaced on this tree (placement fix present?)");
        assert!(severities(&report, FindingSeverity::Inconsistent).is_empty());
    } else {
        assert_ne!(report.verdict, StoreVerdict::Clean);
    }
}

// ---------------------------------------------------------------------------
// GAP-1: uncommitted structure that reached the page file.

/// The page a read error names, if any.
fn error_page(error: &PageError) -> Option<u64> {
    match error {
        PageError::ChecksumMismatch { page_id, .. }
        | PageError::TornPage { page_id, .. }
        | PageError::OutOfBounds { page_id, .. }
        | PageError::ShortRead { page_id, .. }
        | PageError::UnexpectedPageType { page_id, .. } => Some(*page_id),
        _ => None,
    }
}

/// Pages named by inconsistent findings.
fn flagged_pages(report: &StoreVerifyReport) -> BTreeSet<u64> {
    report
        .classes
        .iter()
        .filter(|summary| summary.severity == FindingSeverity::Inconsistent)
        .flat_map(|summary| summary.samples.iter())
        .flat_map(|finding| {
            let mut pages = Vec::new();
            pages.extend(finding.page_id);
            pages.extend(finding.locator.map(|locator| locator.page_id));
            pages
        })
        .collect()
}

/// Reader ground truth captured from the historical writer, independently of
/// the verifier. Reopening these corrupt bytes with a future recovery engine
/// would change the witness and would no longer test the original failure.
#[derive(serde::Deserialize)]
struct GapTruth {
    committed_lost: usize,
    committed_erroring: Vec<(Vec<u8>, Option<u64>)>,
    uncommitted_erroring: Vec<(Vec<u8>, Option<u64>)>,
    integrity_valid: bool,
}

fn historical_gap(name: &str) -> (TempDir, serde_json::Value) {
    use std::io::Read;
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/gap1")
        .join(format!("{name}.json.gz"));
    let compressed = std::fs::read(&path).unwrap();
    let manifest: serde_json::Value = serde_json::from_slice(
        &std::fs::read(path.parent().unwrap().join("manifest.json")).unwrap(),
    )
    .unwrap();
    let entries = manifest["fixtures"].as_array().unwrap();
    assert_eq!(
        entries.len(),
        24,
        "retain every captured historical witness"
    );
    let entry = entries
        .iter()
        .find(|entry| entry["name"].as_str() == path.file_name().unwrap().to_str())
        .unwrap();
    assert_eq!(
        hex::encode(Sha256::digest(&compressed)),
        entry["sha256"].as_str().unwrap()
    );
    assert_eq!(compressed.len() as u64, entry["bytes"].as_u64().unwrap());
    let mut json = Vec::new();
    flate2::read::GzDecoder::new(compressed.as_slice())
        .read_to_end(&mut json)
        .unwrap();
    let fixture: serde_json::Value = serde_json::from_slice(&json).unwrap();
    assert_eq!(fixture["format"], 1);
    assert_eq!(
        fixture["writer_commit"],
        "0af1b55e3d586d4a6933098419e7ee46a7ae5a79"
    );
    assert_eq!(fixture["case"], name);
    let dir = TempDir::new().unwrap();
    for file in fixture["files"].as_array().unwrap() {
        let name = file["name"].as_str().unwrap();
        assert_eq!(Path::new(name).components().count(), 1);
        assert!(matches!(
            Path::new(name).components().next(),
            Some(std::path::Component::Normal(_))
        ));
        let bytes = hex::decode(file["bytes_hex"].as_str().unwrap()).unwrap();
        assert_eq!(bytes.len() as u64, file["size"].as_u64().unwrap());
        assert_eq!(
            hex::encode(Sha256::digest(&bytes)),
            file["sha256"].as_str().unwrap()
        );
        std::fs::write(dir.path().join(name), bytes).unwrap();
    }
    (dir, fixture["truth"].clone())
}

/// Every failing read names a page the verifier flagged.
fn assert_failing_reads_flagged(report: &StoreVerifyReport, failing: &[(Vec<u8>, Option<u64>)]) {
    let pages = flagged_pages(report);
    for (key, page) in failing {
        let page =
            page.unwrap_or_else(|| panic!("{} fails without a page", String::from_utf8_lossy(key)));
        assert!(
            pages.contains(&page),
            "{} fails on page {page}, which no inconsistent finding names: {report:#?}",
            String::from_utf8_lossy(key)
        );
    }
}

#[test]
fn gap1_an_uncommitted_root_split_that_loses_committed_keys_is_detected() {
    let mut detected_any = false;
    for committed in [2usize, 4, 6, 8] {
        let (dir, truth) = historical_gap(&format!("root-{committed}"));
        let report = verify(dir.path());
        let truth: GapTruth = serde_json::from_value(truth).unwrap();
        let lost = truth.committed_lost;
        eprintln!(
            "GAP1-ROOT committed={committed} committed_lost={lost} committed_erroring={} \
             uncommitted_erroring={} verify_integrity_valid={} {}",
            truth.committed_erroring.len(),
            truth.uncommitted_erroring.len(),
            truth.integrity_valid,
            summary_line(&report)
        );
        assert_failing_reads_flagged(&report, &truth.committed_erroring);
        assert_failing_reads_flagged(&report, &truth.uncommitted_erroring);
        assert_eq!(
            report.count(FindingClass::UnreachableCommittedLiveVersion),
            (lost + truth.committed_erroring.len()) as u64,
            "every committed key a reader cannot reach is one unreachable committed version"
        );
        if lost > 0 {
            detected_any = true;
            assert_eq!(report.verdict, StoreVerdict::Inconsistent);
        }
        // Replay the exact state checkpointed by the historical writer too.
        let (copy, recovered_truth) = historical_gap(&format!("root-{committed}-recovered"));
        assert_eq!(
            serde_json::to_value(lost).unwrap(),
            recovered_truth["committed_lost"]
        );
        let recovered = verify(copy.path());
        assert_eq!(
            recovered.count(FindingClass::UnreachableCommittedLiveVersion),
            (lost + truth.committed_erroring.len()) as u64
        );
    }
    assert!(
        detected_any,
        "historical fixtures must retain a damaged witness"
    );
    eprintln!("GAP1-ROOT detected_any={detected_any}");
}

#[test]
fn gap1_a_partially_drained_uncommitted_leaf_split_is_detected() {
    let mut fixtures: Vec<String> =
        std::fs::read_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/gap1"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .filter_map(|name| {
                name.strip_suffix(".json.gz")
                    .filter(|name| name.starts_with("drain-"))
                    .map(str::to_owned)
            })
            .collect();
    fixtures.sort();
    let steps = fixtures.len();
    assert_eq!(steps, 15, "retain all historical partial-drain steps");
    let mut damaged = 0;
    for (index, name) in fixtures.iter().enumerate() {
        let written = index + 1;
        assert_eq!(name, &format!("drain-{written:03}-of-{steps:03}"));
        let (dir, truth) = historical_gap(name);
        let report = verify(dir.path());
        let truth: GapTruth = serde_json::from_value(truth).unwrap();
        eprintln!(
            "GAP1-DRAIN {written}/{steps} committed_lost={} committed_erroring={} \
             uncommitted_erroring={} verify_integrity_valid={} {}",
            truth.committed_lost,
            truth.committed_erroring.len(),
            truth.uncommitted_erroring.len(),
            truth.integrity_valid,
            summary_line(&report)
        );
        assert_failing_reads_flagged(&report, &truth.committed_erroring);
        assert_failing_reads_flagged(&report, &truth.uncommitted_erroring);
        assert_eq!(
            report.count(FindingClass::UnreachableCommittedLiveVersion),
            (truth.committed_lost + truth.committed_erroring.len()) as u64,
            "every committed key a reader cannot reach is one unreachable committed version"
        );
        if truth.committed_lost > 0
            || !truth.committed_erroring.is_empty()
            || !truth.uncommitted_erroring.is_empty()
        {
            damaged += 1;
            assert_eq!(report.verdict, StoreVerdict::Inconsistent, "{report:#?}");
        } else {
            assert!(
                severities(&report, FindingSeverity::Inconsistent).is_empty(),
                "a state that reads back correctly reported inconsistent: {report:#?}"
            );
        }
    }
    assert!(
        damaged > 0,
        "historical fixtures must retain damaged partial drains"
    );
    eprintln!("GAP1-DRAIN damaged_states={damaged} of {steps}");
}

#[test]
fn gap1_an_interleaved_open_transaction_leaves_unreadable_referenced_pages() {
    let (dir, truth) = historical_gap("interleaved");
    let report = verify_with(dir.path(), &many_samples());
    let erroring: Vec<(Vec<u8>, Option<u64>)> =
        serde_json::from_value(truth["erroring"].clone()).unwrap();
    let committed_wrong = truth["committed_wrong"].as_u64().unwrap();
    let integrity_valid = truth["integrity_valid"].as_bool().unwrap();
    let failing_pages: BTreeSet<Option<u64>> = erroring.iter().map(|(_, page)| *page).collect();
    eprintln!(
        "GAP1-INTERLEAVED erroring={} on pages {failing_pages:?} committed_wrong={committed_wrong} \
         verify_integrity_valid={integrity_valid} {}",
        erroring.len(),
        summary_line(&report)
    );
    assert_eq!(erroring.len(), 77);
    assert_eq!(committed_wrong, 0);
    assert!(
        !erroring.is_empty(),
        "the reproducer no longer produces failing reads"
    );
    assert_failing_reads_flagged(&report, &erroring);
    assert_eq!(report.verdict, StoreVerdict::Inconsistent);
}

// ---------------------------------------------------------------------------
// GAP-2: interrupted page-log reset.

fn segmented_options() -> PagedStoreOptions {
    PagedStoreOptions::default()
        .with_page_size(4096)
        .with_buffer_pool_bytes(4 * 1024 * 1024)
        .with_fsync(false)
        .with_wal_max_bytes(1 << 30)
        .with_extent_bytes(1024 * 1024)
        .with_wal_segment_bytes(64 * 1024)
}

fn sealed_segments(dir: &Path) -> Vec<PathBuf> {
    let mut sealed: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.strip_prefix("store.wal."))
                .is_some_and(|suffix| suffix.parse::<u64>().is_ok())
        })
        .collect();
    sealed.sort_by_key(|path| {
        path.extension()
            .and_then(|ext| ext.to_str())
            .and_then(|ext| ext.parse::<u64>().ok())
    });
    sealed
}

#[test]
fn gap2_interrupted_log_reset_shapes_are_classified_without_opening() {
    let dir = TempDir::new().unwrap();
    let hot = b"hot".to_vec();
    let filler = format!("filler-{}", "f".repeat(600));
    let keep = TempDir::new().unwrap();
    let final_value;
    let kept: Vec<(PathBuf, PathBuf)>;
    {
        let (store, _) = PagedStore::open(dir.path(), segmented_options()).unwrap();
        let mut generation = 0u64;
        while sealed_segments(dir.path()).len() < 2 {
            generation += 1;
            let xid = store.begin();
            store
                .put(xid, &hot, format!("v{generation}").as_bytes())
                .unwrap();
            store
                .put(
                    xid,
                    format!("fill-{generation}").as_bytes(),
                    filler.as_bytes(),
                )
                .unwrap();
            store.commit(xid).unwrap();
        }
        generation += 1;
        let xid = store.begin();
        store
            .put(xid, &hot, format!("v{generation}").as_bytes())
            .unwrap();
        store.commit(xid).unwrap();
        final_value = format!("v{generation}").into_bytes();
        kept = sealed_segments(dir.path())
            .into_iter()
            .map(|path| {
                let target = keep.path().join(path.file_name().unwrap());
                std::fs::copy(&path, &target).unwrap();
                (target, path)
            })
            .collect();
        store.checkpoint().unwrap();
        assert!(sealed_segments(dir.path()).is_empty());
    }
    let restore = || {
        for (kept, original) in &kept {
            std::fs::copy(kept, original).unwrap();
        }
    };
    // The crash state: active truncated, sealed segments not yet deleted.
    restore();
    let report = verify(dir.path());
    eprintln!(
        "GAP2-INTERRUPTED {} shape={:?}",
        summary_line(&report),
        report.log.reset_shape
    );
    assert!(
        matches!(
            report.log.reset_shape,
            LogResetShape::InterruptedReset { stale_segments, stale_images_replayed_by_unfixed_open, .. }
                if stale_segments == kept.len() as u64 && stale_images_replayed_by_unfixed_open > 0
        ),
        "{:?}",
        report.log.reset_shape
    );
    assert_eq!(report.log.replay_scope, "none");
    assert_eq!(report.count(FindingClass::LogInterruptedReset), 1);
    assert_eq!(report.verdict, StoreVerdict::AtRisk, "{report:#?}");
    assert_eq!(
        report
            .log
            .stale_records_discarded
            .segments_a_fixed_open_would_discard,
        kept.len() as u64
    );
    // Verified as the page file holds it: the newest value, no damage.
    assert_eq!(report.counters.visible_rows, report.counters.index_entries);

    // An engine opens it and commits; whatever that engine does with the
    // stale segments, put them back (as an engine without the reset fix
    // leaves them) to reach the after-the-fact shape.
    let regressed;
    {
        let (store, _) = PagedStore::open(dir.path(), segmented_options()).unwrap();
        regressed = store.get(&hot).unwrap() != Some(final_value.clone());
        let xid = store.begin();
        store.put(xid, b"after-reopen", b"committed").unwrap();
        store.commit(xid).unwrap();
    }
    restore();
    let report = verify(dir.path());
    eprintln!(
        "GAP2-REPLAYED hot_key_regressed_on_this_engine={regressed} {} shape={:?}",
        summary_line(&report),
        report.log.reset_shape
    );
    assert!(
        matches!(
            report.log.reset_shape,
            LogResetShape::StaleSealedBeforeActive { .. }
        ),
        "{:?}",
        report.log.reset_shape
    );
    assert_eq!(report.log.replay_scope, "active_only");
    assert_eq!(report.count(FindingClass::LogStaleChainAlreadyReplayed), 1);
    assert_eq!(report.verdict, StoreVerdict::NeedsExternalCrossCheck);
}

// ---------------------------------------------------------------------------
// Read-only guarantees and limits.

#[test]
fn verification_refuses_a_store_another_handle_holds_and_creates_no_lock_file() {
    let dir = TempDir::new().unwrap();
    {
        let (store, _) = PagedStore::open(dir.path(), options(4096, 4 << 20)).unwrap();
        let xid = store.begin();
        store.put(xid, b"k", b"v").unwrap();
        store.commit(xid).unwrap();
        store.checkpoint().unwrap();
        match verify_store_read_only(dir.path(), &StoreVerifyOptions::default()) {
            Err(PageError::AlreadyOpen { .. }) => {}
            other => panic!("a live store must be refused, got {other:?}"),
        }
    }
    // The engine leaves its lock file; remove it to show the verifier never
    // creates one.
    std::fs::remove_file(dir.path().join("store.lock")).unwrap();
    let report = verify(dir.path());
    assert!(!dir.path().join("store.lock").exists());
    assert_eq!(report.verdict, StoreVerdict::Clean);
    assert!(report
        .read_only
        .files_opened_read_only
        .iter()
        .all(|path| path.starts_with(dir.path())));
}

#[test]
fn a_time_limit_yields_an_incomplete_report() {
    let dir = TempDir::new().unwrap();
    {
        let (store, _) = PagedStore::open(dir.path(), options(4096, 4 << 20)).unwrap();
        let xid = store.begin();
        for index in 0..5_000usize {
            store
                .put(xid, format!("k{index:06}").as_bytes(), b"value")
                .unwrap();
        }
        store.commit(xid).unwrap();
        store.checkpoint().unwrap();
    }
    let report = verify_with(
        dir.path(),
        &StoreVerifyOptions {
            max_duration: Some(std::time::Duration::ZERO),
            ..StoreVerifyOptions::default()
        },
    );
    assert!(!report.complete);
    assert_eq!(report.stop_reason.as_deref(), Some("time limit"));
}

#[test]
fn verifier_limits_reject_huge_metadata_before_allocating_bitmaps() {
    let dir = TempDir::new().unwrap();
    {
        let (store, _) = PagedStore::open(dir.path(), options(4096, 4 << 20)).unwrap();
        store.checkpoint().unwrap();
    }
    let mut superblock = raw_read(dir.path(), 4096, 0);
    superblock[64 + 12..64 + 20].copy_from_slice(&u64::MAX.to_le_bytes());
    raw_write(dir.path(), 4096, 0, superblock);
    let report = verify(dir.path());
    assert!(!report.complete);
    assert_eq!(report.stop_reason.as_deref(), Some("memory limit"));
    assert_ne!(report.verdict, StoreVerdict::Clean);
}

#[test]
fn verifier_limits_cover_wal_setup_and_small_memory_cache() {
    let dir = TempDir::new().unwrap();
    {
        let (store, _) = PagedStore::open(dir.path(), options(4096, 4 << 20)).unwrap();
        let tx = store.begin();
        store.put(tx, b"retained", b"value").unwrap();
        store.commit(tx).unwrap();
        store.checkpoint().unwrap();
    }
    let small = StoreVerifyOptions {
        max_memory_bytes: 1 << 20,
        // Neither a huge requested window nor the default cache may overrun it.
        heap_window_pages: Some(u64::MAX),
        max_duration: Some(std::time::Duration::MAX),
        ..Default::default()
    };
    let clean = verify_with(dir.path(), &small);
    assert!(clean.complete);
    assert_eq!(clean.verdict, StoreVerdict::Clean);
    {
        let wal = bicdb_page::wal::Wal::open(dir.path().join("store.wal"), false).unwrap();
        for tx in 10_000..40_000 {
            wal.commit(tx).unwrap();
        }
    }
    let stopped = verify_with(dir.path(), &small);
    assert!(!stopped.complete);
    assert_eq!(stopped.stop_reason.as_deref(), Some("memory limit"));
    assert!(
        stopped.log.open_would_refuse.is_none(),
        "limits are not disk corruption"
    );
    let timed = verify_with(
        dir.path(),
        &StoreVerifyOptions {
            max_duration: Some(std::time::Duration::ZERO),
            ..Default::default()
        },
    );
    assert!(!timed.complete);
    assert_eq!(timed.stop_reason.as_deref(), Some("time limit"));
    assert_eq!(timed.log.records, 0, "expired setup must not scan the WAL");
}

#[test]
fn verifier_limits_detect_overflow_cycles_and_impossible_lengths() {
    let dir = TempDir::new().unwrap();
    let locator = {
        let (store, _) = PagedStore::open(dir.path(), options(4096, 4 << 20)).unwrap();
        let tx = store.begin();
        let locator = store
            .put_returning_locator(tx, b"wide", &vec![b'x'; 20_000])
            .unwrap();
        store.commit(tx).unwrap();
        store.checkpoint().unwrap();
        locator
    };
    let heap = raw_read(dir.path(), 4096, locator.page_id);
    let offset = u16_at(&heap, 64 + locator.slot as usize * 8) as usize;
    assert_eq!(heap[offset], 1, "expected an overflow reference");
    let head = u64::from_le_bytes(heap[offset + 1..offset + 9].try_into().unwrap());
    let mut overflow = raw_read(dir.path(), 4096, head);
    overflow[44..52].copy_from_slice(&head.to_le_bytes());
    raw_write(dir.path(), 4096, head, overflow);
    let cycle = verify(dir.path());
    assert!(cycle.complete);
    assert!(cycle.count(FindingClass::OverflowChainBroken) > 0);
    let mut forged = heap;
    forged[offset + 9..offset + 17].copy_from_slice(&u64::MAX.to_le_bytes());
    raw_write(dir.path(), 4096, locator.page_id, forged);
    let huge = verify(dir.path());
    assert!(huge.complete);
    assert!(huge.count(FindingClass::OverflowChainBroken) > 0);
}

#[test]
fn verifier_limits_stop_long_version_walks_without_calling_them_corrupt() {
    let dir = TempDir::new().unwrap();
    {
        let (store, _) = PagedStore::open(dir.path(), options(4096, 4 << 20)).unwrap();
        // Keep a real old snapshot while accumulating a valid version chain.
        let old = store.begin();
        for _ in 0..12_000 {
            let tx = store.begin();
            store.put(tx, b"history", b"value").unwrap();
            store.commit(tx).unwrap();
        }
        store.checkpoint().unwrap();
        store.abort(old).unwrap();
    }
    let report = verify_with(
        dir.path(),
        &StoreVerifyOptions {
            max_memory_bytes: 1 << 20,
            ..Default::default()
        },
    );
    assert!(!report.complete);
    assert_eq!(report.stop_reason.as_deref(), Some("memory limit"));
    assert_eq!(report.count(FindingClass::ChainTooLong), 0);
    assert_eq!(report.count(FindingClass::MalformedVersion), 0);
}

#[cfg(unix)]
#[test]
fn verifier_limits_refuse_fifo_inputs_without_blocking() {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    for special in ["store.lock", "store.wal"] {
        let dir = TempDir::new().unwrap();
        {
            let (store, _) = PagedStore::open(dir.path(), options(4096, 4 << 20)).unwrap();
            store.checkpoint().unwrap();
        }
        let path = dir.path().join(special);
        if path.exists() {
            std::fs::remove_file(&path).unwrap();
        }
        let c_path = CString::new(path.as_os_str().as_bytes()).unwrap();
        // SAFETY: c_path is a valid NUL-terminated path in this test's tempdir.
        assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);
        let start = std::time::Instant::now();
        assert!(verify_store_read_only(dir.path(), &StoreVerifyOptions::default()).is_err());
        assert!(start.elapsed() < std::time::Duration::from_secs(5));
    }
}
