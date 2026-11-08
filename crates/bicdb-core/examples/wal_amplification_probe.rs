//! Page-WAL write amplification by page class and operation, per workload,
//! in `server_paged` with fsync on. Measurement only: it enables the
//! `bicdb_page::wal_amplification` trace (off by default) for this process.
//! See `docs/page-wal-write-amplification.md`.
//!
//! Usage:
//!   wal_amplification_probe <db-dir> <workload> <commits> [threads]
//!
//! Workloads:
//! - `one-row`: one insert (unique + B-tree index) plus an update of the
//!   writer's hot row, exactly the shape of `commit_durability_bench`;
//! - `twenty-row`: twenty such inserts plus the hot-row update;
//! - `erp`: a document header with varchar keys and five secondary indexes,
//!   five document lines with three indexes, an update of a small hot
//!   sequence table, and a status update of the header that moves one of its
//!   index keys;
//! - `update`: an update of one random preloaded row, non-indexed field;
//! - `update-indexed`: the same, but the update moves one indexed key;
//! - `delete`: a delete of one random preloaded row.
//!
//! Preloads (for `update*`/`delete`) run before a checkpoint and a reset of
//! the trace, so the measured phase starts from a clean redo point. Prints
//! one JSON object.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use bicdb_core::{BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, Record, StorageMode};
use bicdb_page::wal_amplification;
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
                Record::new(format!("hot-{thread}")).with_metadata(
                    json!({"owner": thread, "seq": i, "uk": format!("h-{thread}")}),
                ),
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
        eprintln!("usage: wal_amplification_probe <db-dir> <workload> <commits> [threads]");
        std::process::exit(2);
    }
    let db_dir = Path::new(&args[0]);
    let workload = args[1].as_str();
    let commits: u64 = args[2].parse().expect("commits");
    let threads: usize = args.get(3).map(|t| t.parse().expect("threads")).unwrap_or(1);

    wal_amplification::enable();
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
    wal_amplification::reset();
    let wal_before = store.wal().snapshot();
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
    let txlog_after = std::fs::metadata(db_dir.join("transactions.log"))
        .map(|m| m.len())
        .unwrap_or(0);
    let report = wal_amplification::report();
    let n = report.commits.max(1);
    let t = &report.totals;
    let rows: Vec<Value> = report
        .rows
        .iter()
        .map(|row| {
            let c = &row.counters;
            json!({
                "site": row.site, "page_class": row.page_class, "operations": row.operations,
                "images_per_commit": per_commit(c.images, n),
                "bytes_per_commit": per_commit(c.record_bytes, n),
                "changed_bytes_per_commit": per_commit(c.changed_bytes, n),
                "delta_bytes_per_commit": per_commit(c.delta_record_bytes, n),
                "unchanged_images": c.unchanged_images,
                "repeat_since_redo": c.repeat_since_redo,
                "consecutive_commit_images": c.consecutive_commit_images,
                "raw": c,
            })
        })
        .collect();
    let fpw_bytes = t.record_bytes - t.repeat_since_redo_bytes + t.repeat_since_redo_delta_bytes;
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "workload": workload,
            "threads": threads,
            "commits": report.commits,
            "seconds": elapsed,
            "commits_per_second": report.commits as f64 / elapsed,
            "wal_bytes_appended": wal_after.bytes_appended - wal_before.bytes_appended,
            "wal_records_appended": wal_after.records_appended - wal_before.records_appended,
            "checkpoints": wal_after.checkpoints - wal_before.checkpoints,
            "transaction_log_bytes": txlog_after.saturating_sub(txlog_before),
            "per_commit": {
                "wal_bytes": per_commit(wal_after.bytes_appended - wal_before.bytes_appended, n),
                "transaction_log_bytes": per_commit(txlog_after.saturating_sub(txlog_before), n),
                "images": per_commit(t.images, n),
                "image_bytes": per_commit(t.record_bytes, n),
                "changed_bytes": per_commit(t.changed_bytes, n),
                "delta_bytes_all_images": per_commit(t.delta_record_bytes, n),
                "full_page_write_rule_bytes": per_commit(fpw_bytes, n),
                "unchanged_images": per_commit(t.unchanged_images, n),
                "repeat_since_redo_images": per_commit(t.repeat_since_redo, n),
                "consecutive_commit_images": per_commit(t.consecutive_commit_images, n),
            },
            "lock_hold_us_per_commit": {
                "apply": report.lock_hold.apply_nanos as f64 / 1000.0 / report.lock_hold.commits.max(1) as f64,
                "log_images": report.lock_hold.log_images_nanos as f64 / 1000.0 / n as f64,
                "commit_record_and_sync": report.lock_hold.commit_record_and_sync_nanos as f64 / 1000.0 / n as f64,
            },
            "totals": t,
            "rows": rows,
        }))
        .unwrap()
    );
    let db = Arc::try_unwrap(db).ok().expect("sole owner");
    db.close().expect("close");
}
