//! Page-WAL bytes per commit for the PR #40 workloads, with and without the
//! EXPERIMENTAL page-delta records. The workload code is the
//! `wal_amplification_probe` of branch `wal-amplification-study` (PR #40),
//! with its trace removed: bytes come from the WAL's own counters.
//!
//! Usage:
//!   page_delta_bytes_probe <fresh-db-dir> <workload> <commits> [threads]
//!
//! `PROBE_DISPOSABLE=1` creates the database as a disposable experimental
//! page-delta store first (`create_disposable_page_delta_store`); deltas are
//! then written only when `BICDB_PAGE_WAL_DELTAS=1` too. Prints one JSON
//! object. See `docs/page-delta-redo-design.md`.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use bicdb_core::{BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, Record, StorageMode};
use serde_json::{json, Value};

const PRELOAD_ROWS: u64 = 20_000;

fn config() -> DbConfig {
    DbConfig::default()
        .with_fsync(true)
        .with_sync_outbox(false)
        .with_storage_mode(StorageMode::ServerPaged)
}

fn index(collection: &str, name: &str, fields: &[&str], unique: bool) -> IndexDefinition {
    IndexDefinition {
        name: name.into(),
        collection: collection.into(),
        fields: fields
            .iter()
            .map(|field| IndexField::MetadataPath(vec![(*field).into()]))
            .collect(),
        unique,
        nulls_not_distinct: false,
        kind: IndexKind::BTree,
        predicate: None,
        exclusion: None,
    }
}

/// Deterministic per-thread pseudo-random numbers (xorshift).
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn setup_rows(db: &mut BicDb, threads: usize) {
    db.create_collection("rows").expect("create");
    db.create_index(index("rows", "rows_owner", &["owner"], false))
        .expect("index");
    db.create_index(index("rows", "rows_uk", &["uk"], true))
        .expect("unique");
    for thread in 0..threads {
        let mut tx = db.begin_transaction().expect("begin");
        tx.insert(
            "rows",
            Record::new(format!("hot-{thread}"))
                .with_metadata(json!({"owner": thread, "seq": 0, "uk": format!("h-{thread}")})),
        )
        .expect("seed");
        tx.commit().expect("seed commit");
    }
}

fn preload_rows(db: &BicDb) {
    let mut next = 0u64;
    while next < PRELOAD_ROWS {
        let mut tx = db.begin_transaction().expect("begin");
        for _ in 0..500 {
            tx.insert(
                "rows",
                Record::new(format!("p-{next:08}")).with_metadata(json!({
                    "owner": next % 64, "seq": 0, "uk": format!("pu-{next:08}"),
                    "note": "preloaded row with a modest payload",
                })),
            )
            .expect("preload");
            next += 1;
        }
        tx.commit().expect("preload commit");
    }
}

fn setup_erp(db: &mut BicDb) {
    db.create_collection("c_order").expect("create");
    db.create_collection("c_orderline").expect("create");
    db.create_collection("ad_sequence").expect("create");
    for (name, fields, unique) in [
        ("c_order_docno", &["ad_client_id", "documentno"][..], true),
        ("c_order_bpartner", &["c_bpartner_id"][..], false),
        ("c_order_dateordered", &["dateordered"][..], false),
        ("c_order_docstatus", &["docstatus"][..], false),
        ("c_order_org", &["ad_org_id", "c_doctype_id"][..], false),
    ] {
        db.create_index(index("c_order", name, fields, unique))
            .expect("order index");
    }
    for (name, fields, unique) in [
        ("c_orderline_order", &["c_order_id"][..], false),
        ("c_orderline_product", &["m_product_id"][..], false),
        ("c_orderline_order_line", &["c_order_id", "line"][..], true),
    ] {
        db.create_index(index("c_orderline", name, fields, unique))
            .expect("line index");
    }
    db.create_index(index("ad_sequence", "ad_sequence_name", &["name"], true))
        .expect("sequence index");
    let mut tx = db.begin_transaction().expect("begin");
    for seq in 0..8 {
        tx.insert(
            "ad_sequence",
            Record::new(format!("SEQ{seq:04}")).with_metadata(json!({
                "name": format!("DocumentNo_C_Order_{seq}"), "currentnext": 1_000_000,
                "incrementno": 1, "ad_client_id": "1000000",
            })),
        )
        .expect("sequence");
    }
    tx.commit().expect("seed commit");
}

fn erp_commit(db: &BicDb, thread: usize, i: u64, rng: &mut Rng) {
    let order_id = format!("C_Order-{thread:02}-{i:010}");
    let documentno = format!("SO-{thread:02}{i:08}");
    let bpartner = format!("C_BPartner-{:06}", rng.next() % 5_000);
    let header = |docstatus: &str, total: u64| {
        json!({
            "ad_client_id": "1000000", "ad_org_id": format!("{}", 1_000_000 + rng_org(i)),
            "documentno": documentno, "c_doctype_id": "1000030", "docstatus": docstatus,
            "c_bpartner_id": bpartner, "dateordered": format!("2026-11-{:02}", 1 + i % 28),
            "description": "Standard sales order created by the probe",
            "grandtotal": total, "isactive": "Y", "issotrx": "Y", "salesrep_id": "1000001",
            "m_warehouse_id": "1000000", "m_pricelist_id": "1000001", "c_currency_id": "100",
        })
    };
    let mut tx = db.begin_transaction().expect("begin");
    // The sequence row every document takes its number from.
    let seq = format!("SEQ{:04}", thread % 8);
    tx.update(
        "ad_sequence",
        Record::new(seq.clone()).with_metadata(json!({
            "name": format!("DocumentNo_C_Order_{}", thread % 8),
            "currentnext": 1_000_000 + i, "incrementno": 1, "ad_client_id": "1000000",
        })),
    )
    .expect("sequence");
    tx.insert(
        "c_order",
        Record::new(order_id.clone()).with_metadata(header("DR", 0)),
    )
    .expect("order");
    let mut total = 0;
    for line in 1..=5u64 {
        let amount = 10 + rng.next() % 500;
        total += amount;
        tx.insert(
            "c_orderline",
            Record::new(format!("{order_id}-L{line}")).with_metadata(json!({
                "c_order_id": order_id, "line": line * 10,
                "m_product_id": format!("M_Product-{:05}", rng.next() % 20_000),
                "qtyordered": 1 + rng.next() % 9, "priceactual": amount,
                "linenetamt": amount, "ad_client_id": "1000000", "isactive": "Y",
                "description": "order line",
            })),
        )
        .expect("line");
    }
    // Complete the document: totals and status (moves the docstatus key).
    tx.update(
        "c_order",
        Record::new(order_id).with_metadata(header("CO", total)),
    )
    .expect("complete");
    tx.commit().expect("commit");
}

fn rng_org(i: u64) -> u64 {
    i % 4
}

fn workload_commit(db: &BicDb, workload: &str, thread: usize, i: u64, rng: &mut Rng) {
    match workload {
        "one-row" | "twenty-row" => {
            let rows = if workload == "one-row" { 1 } else { 20 };
            let mut tx = db.begin_transaction().expect("begin");
            for row in 0..rows {
                tx.insert(
                    "rows",
                    Record::new(format!("r-{thread}-{i}-{row}")).with_metadata(json!({
                        "owner": thread, "seq": i, "uk": format!("u-{thread}-{i}-{row}"),
                    })),
                )
                .expect("insert");
            }
            tx.update(
                "rows",
                Record::new(format!("hot-{thread}"))
                    .with_metadata(json!({"owner": thread, "seq": i, "uk": format!("h-{thread}")})),
            )
            .expect("update");
            tx.commit().expect("commit");
        }
        "erp" => erp_commit(db, thread, i, rng),
        "update" | "update-indexed" => {
            let row = rng.next() % PRELOAD_ROWS;
            let owner = if workload == "update" {
                row % 64
            } else {
                64 + (rng.next() % 64)
            };
            let mut tx = db.begin_transaction().expect("begin");
            tx.update(
                "rows",
                Record::new(format!("p-{row:08}")).with_metadata(json!({
                    "owner": owner, "seq": i, "uk": format!("pu-{row:08}"),
                    "note": "preloaded row with a modest payload",
                })),
            )
            .expect("update");
            tx.commit().expect("commit");
        }
        "delete" => {
            // Each thread deletes from its own stride so no row is deleted twice.
            let row = (i * 7919 + thread as u64 * 104_729) % PRELOAD_ROWS;
            let mut tx = db.begin_transaction().expect("begin");
            tx.delete("rows", &format!("p-{row:08}")).expect("delete");
            tx.commit().expect("commit");
        }
        other => panic!("unknown workload {other}"),
    }
}

fn per_commit(value: u64, commits: u64) -> f64 {
    if commits == 0 {
        0.0
    } else {
        value as f64 / commits as f64
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 3 {
        eprintln!("usage: page_delta_bytes_probe <db-dir> <workload> <commits> [threads]");
        std::process::exit(2);
    }
    let db_dir = Path::new(&args[0]);
    let workload = args[1].as_str();
    let commits: u64 = args[2].parse().expect("commits");
    let threads: usize = args
        .get(3)
        .map(|t| t.parse().expect("threads"))
        .unwrap_or(1);

    if std::env::var("PROBE_DISPOSABLE").is_ok_and(|v| v == "1") {
        bicdb_core::page_deltas_experimental::create_disposable_page_delta_store(db_dir, &config())
            .expect("create disposable store");
    }
    let mut db = BicDb::open_with_config(db_dir, config()).expect("open");
    match workload {
        "erp" => setup_erp(&mut db),
        _ => setup_rows(&mut db, threads),
    }
    if matches!(workload, "update" | "update-indexed" | "delete") {
        preload_rows(&db);
    }
    let store = db
        .paged_records_handle()
        .expect("server_paged")
        .store()
        .clone();
    store.checkpoint().expect("checkpoint");
    let wal_before = store.wal().snapshot();
    let (engaged, deltas_before) = store.page_deltas();
    let txlog_before = std::fs::metadata(db_dir.join("transactions.log"))
        .map(|m| m.len())
        .unwrap_or(0);

    let db = Arc::new(db);
    let next = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    std::thread::scope(|scope| {
        for thread in 0..threads {
            let db = db.clone();
            let next = next.clone();
            scope.spawn(move || {
                let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ (thread as u64 + 1));
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= commits {
                        break;
                    }
                    workload_commit(&db, workload, thread, i + 1, &mut rng);
                }
            });
        }
    });
    let elapsed = started.elapsed().as_secs_f64();
    let wal_after = store.wal().snapshot();
    let (_, deltas_after) = store.page_deltas();
    let txlog_after = std::fs::metadata(db_dir.join("transactions.log"))
        .map(|m| m.len())
        .unwrap_or(0);
    let n = commits.max(1);
    let wal_bytes = wal_after.bytes_appended - wal_before.bytes_appended;
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "workload": workload,
            "threads": threads,
            "commits": commits,
            "deltas_engaged": engaged,
            "disposable_store": store.is_disposable_page_delta_store(),
            "seconds": elapsed,
            "commits_per_second": commits as f64 / elapsed,
            "wal_bytes_appended": wal_bytes,
            "wal_records_appended": wal_after.records_appended - wal_before.records_appended,
            "checkpoints": wal_after.checkpoints - wal_before.checkpoints,
            "per_commit": {
                "wal_bytes": per_commit(wal_bytes, n),
                "transaction_log_bytes": per_commit(txlog_after.saturating_sub(txlog_before), n),
                "delta_records": per_commit(deltas_after.delta_records - deltas_before.delta_records, n),
                "delta_bytes": per_commit(deltas_after.delta_bytes - deltas_before.delta_bytes, n),
                "full_images": per_commit(deltas_after.full_images - deltas_before.full_images, n),
                "full_image_bytes": per_commit(deltas_after.full_image_bytes - deltas_before.full_image_bytes, n),
                "delta_refused": per_commit(deltas_after.delta_refused - deltas_before.delta_refused, n),
            },
        }))
        .unwrap()
    );
    let db = Arc::try_unwrap(db).ok().expect("sole owner");
    db.close().expect("close");
}
