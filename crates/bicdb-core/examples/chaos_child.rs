//! G5.4 chaos child: opens a database, mutates source rows, keeps a durable
//! projection caught up, and checkpoints — forever, until something kills it.
//!
//! It deliberately does **not** know when it will die. The parent murders it
//! at an arbitrary instant, which is the whole point: controlled failpoints
//! test the failure modes you thought of, `SIGKILL` tests the ones you didn't.
//!
//! Usage: `chaos_child <db-dir> <state-dir> <rows> [seed]`

use bicdb_core::aggregate_projection::AggregateProjection;
use bicdb_core::{BicDb, DbConfig, Record};
use serde_json::json;

fn projection() -> AggregateProjection {
    AggregateProjection::new(
        "chaos",
        "businesses",
        vec!["state".into(), "category".into(), "host".into()],
        vec!["score".into()],
    )
    .unwrap()
}

fn main() {
    let mut args = std::env::args().skip(1);
    let db_dir = args.next().expect("db dir");
    let state_dir = args.next().expect("state dir");
    let rows: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(2_000);
    let mut seed: u64 = args
        .next()
        .and_then(|a| a.parse().ok())
        .unwrap_or(0x9e37_79b9_7f4a_7c15);
    let mut next = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };

    let mut db = BicDb::open_with_config(
        &db_dir,
        DbConfig::default()
            .with_fsync(false)
            .with_audit_events(true),
    )
    .expect("open db");
    let _ = db.create_collection("businesses");

    // Seed once; later invocations just keep mutating what is there.
    if db
        .scan_collection("businesses")
        .map(|r| r.is_empty())
        .unwrap_or(true)
    {
        let records: Vec<Record> = (0..rows)
            .map(|index| {
                Record::new(format!("biz-{index:06}")).with_metadata(json!({
                    "state": format!("state-{}", index % 12),
                    "category": format!("cat-{}", index % 30),
                    "host": format!("host-{}", index % 7),
                    "score": (index % 100) as f64,
                }))
            })
            .collect();
        db.bulk_load_insert("businesses", records).expect("seed");
    }

    let mut projection =
        AggregateProjection::open(&state_dir, "chaos", &db, || Ok(projection())).expect("open");

    // Mutate and checkpoint until murdered. Hosts are drawn from a growing
    // space so brand-new dictionary values keep being minted mid-flight —
    // the case where a crash could strand a slab id whose mapping was never
    // published.
    let mut round: u64 = 0;
    loop {
        round += 1;
        for step in 0..25 {
            let index = (next() as usize) % rows;
            let id = format!("biz-{index:06}");
            if step % 19 == 0 {
                let _ = db.delete("businesses", &id);
                continue;
            }
            let Ok(Some(existing)) = db.get("businesses", &id) else {
                continue;
            };
            let mut metadata = existing.metadata.clone();
            metadata["score"] = json!((next() % 100) as f64);
            if step % 3 == 0 {
                // Brand-new dictionary values, forever.
                metadata["host"] = json!(format!("host-{}", next() % 4096));
            }
            let _ = db.insert("businesses", Record::new(id).with_metadata(metadata));
        }
        projection.catch_up(&db).expect("catch up");
        // Checkpoint often, so kills land inside and around publication.
        if round % 2 == 0 {
            projection.save(&state_dir, true).expect("save");
        }
    }
}
