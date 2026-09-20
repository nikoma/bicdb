//! Temporary diagnostic: times each advance of the restartable paged B-tree
//! build to locate the quadratic term in create_index backfill.
//! Usage: build_advance_timing <dir> <records>

use bicdb_core::{
    BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, PagedBTreeBuildAdvance, Record,
    ResourceDemand, ResourceGovernor, StorageMode,
};
use serde_json::json;

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args
        .next()
        .expect("usage: build_advance_timing <dir> <records>");
    let total: usize = args.next().expect("records").parse().expect("number");
    let config = DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged);
    let mut db = BicDb::open_with_config(&dir, config).expect("open");
    db.create_collection("articles").expect("create");
    let mut ingested = 0usize;
    while ingested < total {
        let count = 2_000.min(total - ingested);
        let records: Vec<Record> = (ingested..ingested + count)
            .map(|i| {
                Record::new(format!("pmid-{i:09}")).with_metadata(json!({
                    "title": format!("Study {i}"),
                    "abstract": "Background and methods. ".repeat(20),
                    "year": 1990 + (i % 36),
                }))
            })
            .collect();
        db.batch_insert("articles", records).expect("insert");
        ingested += count;
    }
    eprintln!("ingested {total}");

    let definition = IndexDefinition {
        name: "articles_year".to_string(),
        collection: "articles".to_string(),
        fields: vec![IndexField::MetadataPath(vec!["year".to_string()])],
        unique: false,
        kind: IndexKind::BTree,
        predicate: None,
        exclusion: None,
    };
    let limits = bicdb_core::PagedBTreeBuildLimits {
        max_batch_rows: 4_096,
        max_batch_bytes: 64 * 1024 * 1024,
        max_state_bytes: 1024 * 1024,
    };
    let mut now = 1_000u64;
    let governor = ResourceGovernor::new(Default::default(), now).expect("governor");
    let demand = ResourceDemand {
        memory_bytes: limits.max_batch_bytes as u64,
        io_bytes: limits.max_batch_bytes as u64,
        cpu_slots: 1,
        io_charge_bytes: limits.max_batch_bytes as u64,
    };
    db.begin_paged_btree_build(definition.clone(), limits, now)
        .expect("begin");
    let mut batch = 0usize;
    loop {
        now += 1_000;
        let started = std::time::Instant::now();
        let advance = db
            .advance_paged_btree_build_governed(&definition.name, limits, &governor, demand, now)
            .expect("advance");
        let elapsed = started.elapsed().as_secs_f64();
        batch += 1;
        match advance {
            PagedBTreeBuildAdvance::Progress(state) => {
                eprintln!(
                    "batch {batch:4} {elapsed:7.3}s phase={:?} resume={:?} entries={}",
                    state.phase,
                    state.resume_after_id.as_deref().unwrap_or("-"),
                    state.expected_entries,
                );
            }
            PagedBTreeBuildAdvance::Complete(state) => {
                eprintln!(
                    "batch {batch:4} {elapsed:7.3}s COMPLETE entries={}",
                    state.expected_entries
                );
                break;
            }
        }
    }
}
