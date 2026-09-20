//! Bounded-nearest probe: seeds a packed spatial index and times `nearest`
//! (best-first k-NN) against a corpus-draining radius query at the same
//! scale, so the bounded-vs-O(corpus) gap is measured, not asserted.
//!
//! ```text
//! cargo run --release -p bicdb-core --example nearest_probe -- [rows]
//! ```

use bicdb_core::{BicDb, DbConfig, Geometry, Record, StorageMode};
use serde_json::json;
use std::time::Instant;

fn main() {
    let rows: usize = std::env::args()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(200_000);
    let dir = tempfile::tempdir().expect("tempdir");
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged)
            .with_sync_outbox(false),
    )
    .expect("open");
    db.create_collection("places").expect("collection");

    // Deterministic scatter over a continent-sized box.
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut records = Vec::with_capacity(rows);
    for index in 0..rows {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let lon = -30.0 + ((state >> 20) & 0xFFFFF) as f64 / 1048.576 / 16.666;
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let lat = -30.0 + ((state >> 20) & 0xFFFFF) as f64 / 1048.576 / 16.666;
        records.push(
            Record::new(format!("place-{index:08}"))
                .with_metadata(json!({ "n": index }))
                .with_geometry(Geometry::point(lon, lat).expect("point")),
        );
    }
    let seed_started = Instant::now();
    db.bulk_load_insert("places", records).expect("bulk load");
    db.create_spatial_index("places", "geometry")
        .expect("index");
    println!("seeded {rows} rows in {:?}", seed_started.elapsed());

    let pack_started = Instant::now();
    let report = db
        .pack_spatial_index("idx_places_geometry_spatial")
        .expect("pack");
    println!(
        "packed: nodes={} height={} in {:?}",
        report.node_count,
        report.height,
        pack_started.elapsed()
    );

    let queries = [(0.0, 0.0), (-25.0, -25.0), (0.31, 0.17), (14.9, -22.3)];

    for k in [1usize, 10, 100] {
        let started = Instant::now();
        let mut total_hits = 0usize;
        let repeats = 50;
        for round in 0..repeats {
            for (lon, lat) in queries {
                let ids = db
                    .nearest("places", "geometry", lon + round as f64 * 0.001, lat, k)
                    .expect("nearest");
                total_hits += ids.len();
            }
        }
        let per_query = started.elapsed() / (repeats * queries.len()) as u32;
        println!("nearest k={k}: {per_query:?}/query ({total_hits} hits)");
    }

    // The O(corpus) reference: a radius wide enough to cover every row forces
    // the same full walk the old nearest() always paid.
    let started = Instant::now();
    let all = db
        .within_radius("places", "geometry", 0.0, 0.0, 20_000_000.0)
        .expect("radius");
    println!(
        "corpus-draining radius query (old nearest cost floor): {:?} for {} rows",
        started.elapsed(),
        all.len()
    );
}
