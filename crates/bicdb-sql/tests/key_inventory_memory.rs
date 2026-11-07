use bicdb_core::{BicDb, DbConfig, SecurityContext, StorageMode};
use bicdb_sql::SqlSession;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
thread_local! {static ALLOCATED:Cell<Option<usize>>=const{Cell::new(None)};}
struct MeasuredSystem;
fn note(bytes: usize) {
    let _ = ALLOCATED.try_with(|count| {
        if let Some(value) = count.get() {
            count.set(Some(value.saturating_add(bytes)));
        }
    });
}
unsafe impl GlobalAlloc for MeasuredSystem {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        note(size);
        unsafe { System.realloc(ptr, layout, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: MeasuredSystem = MeasuredSystem;
#[test]
fn key_inventory_pages_have_a_bounded_allocation_budget() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        directory.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE inventory(id uuid PRIMARY KEY,payload text)")
        .unwrap();
    let payload = "x".repeat(64 * 1024);
    for page in 0..4 {
        let values = (page * 32 + 1..=page * 32 + 32)
            .map(|n| format!("('00000000-0000-0000-0000-{n:012x}','{payload}')"))
            .collect::<Vec<_>>()
            .join(",");
        sql.execute(&format!("INSERT INTO inventory VALUES {values}"))
            .unwrap();
    }
    let cases=[
 ("text-ordered","SELECT id::text FROM inventory ORDER BY id LIMIT 4",true),
 ("text-seek","SELECT id::text FROM inventory WHERE id>'00000000-0000-0000-0000-000000000000'::uuid ORDER BY id LIMIT 4",true),
 ("bare-ordered","SELECT id FROM inventory ORDER BY id LIMIT 4",false),
 ("bare-seek","SELECT id FROM inventory WHERE id>'00000000-0000-0000-0000-000000000000'::uuid ORDER BY id LIMIT 4",false),
 ("text-unordered","SELECT id::text FROM inventory LIMIT 4",false),
 ("bare-unordered","SELECT id FROM inventory LIMIT 4",false),
 ("outer-cast","SELECT id::text FROM (SELECT id FROM inventory ORDER BY id LIMIT 4) keys",false),
 ];
    let mut required = Vec::new();
    for authenticated in [false, true] {
        sql = sql.with_security_context(if authenticated {
            Some(SecurityContext::new("inventory-owner", ""))
        } else {
            None
        });
        for transaction in [false, true] {
            if transaction {
                sql.execute("BEGIN READ ONLY").unwrap();
            }
            for (label, query, enforce) in cases {
                ALLOCATED.with(|c| c.set(Some(0)));
                let result = sql.execute(query);
                let allocated = ALLOCATED.with(|c| c.replace(None).unwrap());
                let result = result.unwrap();
                assert_eq!(result.rows.len(), 4);
                eprintln!("synthetic key inventory {label}: {allocated} allocated bytes");
                if enforce {
                    required.push((label, authenticated, transaction, allocated));
                }
            }
            if transaction {
                sql.execute("ROLLBACK").unwrap();
            }
        }
    }
    assert!(
        required
            .iter()
            .all(|(_, _, _, bytes)| *bytes < 8 * 1024 * 1024),
        "four keys must not clone the eight-MiB payload corpus: {required:?}"
    );
}
