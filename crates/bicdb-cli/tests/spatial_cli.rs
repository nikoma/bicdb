use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::process::Command;

use bicdb_core::{BicDb, DbConfig, Geometry, OsmImportBbox, Record, StorageMode};
use osmpbfreader::{fileformat, osmformat};
use protobuf::Message;
use serde_json::json;

fn bicdb_bin() -> &'static str {
    env!("CARGO_BIN_EXE_bicdb")
}

fn spatial_fixture() -> tempfile::TempDir {
    let temp = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(temp.path()).unwrap();
    db.create_collection("places").unwrap();
    db.batch_insert(
        "places",
        [
            Record::new("sf")
                .with_geometry(Geometry::point(-122.4194, 37.7749).unwrap())
                .with_metadata(json!({"name": "San Francisco"})),
            Record::new("oak")
                .with_geometry(Geometry::point(-122.2711, 37.8044).unwrap())
                .with_metadata(json!({"name": "Oakland"})),
            Record::new("la")
                .with_geometry(Geometry::point(-118.2437, 34.0522).unwrap())
                .with_metadata(json!({"name": "Los Angeles"})),
        ],
    )
    .unwrap();
    db.create_spatial_index("places", "geometry").unwrap();
    temp
}

fn write_tiny_osm_pbf(path: &Path) {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .unwrap();

    let mut header = osmformat::HeaderBlock::new();
    header.required_features.push("OsmSchema-V0.6".to_string());
    header.required_features.push("DenseNodes".to_string());
    write_pbf_block(&mut file, "OSMHeader", &header);

    let mut strings = osmformat::StringTable::new();
    strings.s = ["", "highway", "residential", "name", "Fixture Road"]
        .into_iter()
        .map(|value| value.as_bytes().to_vec())
        .collect();

    let raw_nodes = [
        (1_i64, -122.4194_f64, 37.7749_f64),
        (2_i64, -122.4184_f64, 37.7749_f64),
        (3_i64, -122.4174_f64, 37.7749_f64),
    ];
    let mut dense = osmformat::DenseNodes::new();
    let mut last_id = 0_i64;
    let mut last_lat = 0_i64;
    let mut last_lon = 0_i64;
    for (id, lon, lat) in raw_nodes {
        let lat_raw = (lat * 10_000_000.0).round() as i64;
        let lon_raw = (lon * 10_000_000.0).round() as i64;
        dense.id.push(id - last_id);
        dense.lat.push(lat_raw - last_lat);
        dense.lon.push(lon_raw - last_lon);
        dense.keys_vals.push(0);
        last_id = id;
        last_lat = lat_raw;
        last_lon = lon_raw;
    }

    let mut node_group = osmformat::PrimitiveGroup::new();
    node_group.dense = protobuf::MessageField::some(dense);

    let mut way = osmformat::Way::new();
    way.set_id(10);
    way.keys = vec![1, 3];
    way.vals = vec![2, 4];
    way.refs = vec![1, 1, 1];
    let mut way_group = osmformat::PrimitiveGroup::new();
    way_group.ways.push(way);

    let mut block = osmformat::PrimitiveBlock::new();
    block.stringtable = protobuf::MessageField::some(strings);
    block.primitivegroup = vec![node_group, way_group];
    block.set_granularity(100);
    write_pbf_block(&mut file, "OSMData", &block);
}

fn write_pbf_block<M: Message>(file: &mut std::fs::File, block_type: &str, message: &M) {
    let payload = message.write_to_bytes().unwrap();
    let mut blob = fileformat::Blob::new();
    blob.set_raw(payload);
    let blob_bytes = blob.write_to_bytes().unwrap();

    let mut header = fileformat::BlobHeader::new();
    header.set_type(block_type.to_string());
    header.set_datasize(blob_bytes.len() as i32);
    let header_bytes = header.write_to_bytes().unwrap();

    file.write_all(&(header_bytes.len() as i32).to_be_bytes())
        .unwrap();
    file.write_all(&header_bytes).unwrap();
    file.write_all(&blob_bytes).unwrap();
}

#[test]
fn cli_spatial_nearest_outputs_json() {
    let temp = spatial_fixture();
    let output = Command::new(bicdb_bin())
        .args([
            "spatial",
            "nearest",
            temp.path().to_str().unwrap(),
            "places",
            "--point",
            "-122.4194,37.7749",
            "--limit",
            "2",
            "--json",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let rows: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(rows[0]["id"], "sf");
    assert_eq!(rows[1]["id"], "oak");
}

#[test]
fn cli_spatial_within_radius_outputs_table() {
    let temp = spatial_fixture();
    let output = Command::new(bicdb_bin())
        .args([
            "spatial",
            "within-radius",
            temp.path().to_str().unwrap(),
            "places",
            "--point",
            "-122.4194,37.7749",
            "--meters",
            "20000",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("distance_meters"));
    assert!(stdout.contains("sf"));
    assert!(stdout.contains("oak"));
    assert!(!stdout.contains("Los Angeles"));
}

#[test]
fn cli_spatial_errors_for_missing_collection_and_invalid_point() {
    let temp = spatial_fixture();
    let missing = Command::new(bicdb_bin())
        .args([
            "spatial",
            "nearest",
            temp.path().to_str().unwrap(),
            "missing",
            "--point",
            "-122.4194,37.7749",
        ])
        .output()
        .unwrap();
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("collection not found"));

    let missing_field = Command::new(bicdb_bin())
        .args([
            "spatial",
            "nearest",
            temp.path().to_str().unwrap(),
            "places",
            "--field",
            "location",
            "--point",
            "-122.4194,37.7749",
        ])
        .output()
        .unwrap();
    assert!(!missing_field.status.success());
    assert!(String::from_utf8_lossy(&missing_field.stderr).contains("spatial field `location`"));

    let invalid_point = Command::new(bicdb_bin())
        .args([
            "spatial",
            "within-radius",
            temp.path().to_str().unwrap(),
            "places",
            "--point",
            "-122.4194",
            "--meters",
            "20000",
        ])
        .output()
        .unwrap();
    assert!(!invalid_point.status.success());
    assert!(String::from_utf8_lossy(&invalid_point.stderr).contains("expected lon,lat"));
}

#[test]
fn cli_spatial_import_osm_and_route_fixture() {
    let temp = tempfile::tempdir().unwrap();
    let pbf = temp.path().join("tiny.osm.pbf");
    write_tiny_osm_pbf(&pbf);

    let import = Command::new(bicdb_bin())
        .args([
            "spatial",
            "import-osm",
            pbf.to_str().unwrap(),
            "--path",
            temp.path().to_str().unwrap(),
            "--bbox",
            "-122.42,37.77,-122.41,37.78",
            "--json",
        ])
        .output()
        .unwrap();

    assert!(
        import.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&import.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&import.stdout).unwrap();
    assert_eq!(report["graph"], "roads");
    assert_eq!(report["road_ways"], 1);
    assert_eq!(report["nodes"], 3);
    assert_eq!(report["edges"], 4);

    let route = Command::new(bicdb_bin())
        .args([
            "spatial",
            "route",
            temp.path().to_str().unwrap(),
            "--from",
            "-122.4194,37.7749",
            "--to",
            "-122.4174,37.7749",
            "--json",
        ])
        .output()
        .unwrap();

    assert!(
        route.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&route.stderr)
    );
    let route: serde_json::Value = serde_json::from_slice(&route.stdout).unwrap();
    assert_eq!(route["graph"], "roads");
    assert_eq!(route["node_ids"][0], "osm-node-1");
    assert_eq!(route["node_ids"][2], "osm-node-3");
    assert!(route["distance_m"].as_f64().unwrap() > 170.0);
}

#[test]
fn cli_spatial_import_osm_rejects_invalid_bbox() {
    let temp = tempfile::tempdir().unwrap();
    let pbf = temp.path().join("tiny.osm.pbf");
    write_tiny_osm_pbf(&pbf);

    let output = Command::new(bicdb_bin())
        .args([
            "spatial",
            "import-osm",
            pbf.to_str().unwrap(),
            "--path",
            temp.path().to_str().unwrap(),
            "--bbox",
            "-122.42,37.77",
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("expected minLon,minLat,maxLon,maxLat")
    );

    let mut db = BicDb::open(temp.path()).unwrap();
    let error = db
        .import_osm_pbf(
            &pbf,
            OsmImportBbox {
                min_lon: -122.41,
                min_lat: 37.77,
                max_lon: -122.42,
                max_lat: 37.78,
            },
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("min values must not exceed max values"));
}

#[test]
fn index_pack_packs_a_paged_spatial_index() {
    let temp = tempfile::tempdir().unwrap();
    let config = || {
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged)
    };
    {
        let mut db = BicDb::open_with_config(temp.path(), config()).unwrap();
        db.create_collection("places").unwrap();
        db.close().unwrap();
        let mut db = BicDb::open_with_config(temp.path(), config()).unwrap();
        db.bulk_load_insert(
            "places",
            (0..50u32)
                .map(|index| {
                    Record::new(format!("p-{index:02}"))
                        .with_geometry(Geometry::point(0.001 * f64::from(index), 0.0).unwrap())
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();
        db.create_spatial_index("places", "geometry").unwrap();
        db.close().unwrap();
    }

    let output = Command::new(bicdb_bin())
        .args([
            "index",
            "pack",
            temp.path().to_str().unwrap(),
            "idx_places_geometry_spatial",
            "--strategy",
            "str",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "index pack failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["index_name"], json!("idx_places_geometry_spatial"));
    assert_eq!(report["strategy"], json!("str"));
    assert_eq!(report["generation"], json!(1));
    assert_eq!(report["entry_count"], json!(50));

    // The packed index answers after reopen.
    let db = BicDb::open_with_config(temp.path(), config()).unwrap();
    let hits = db
        .within_radius("places", "geometry", 0.0, 0.0, 20_000.0)
        .unwrap();
    assert_eq!(hits.len(), 50);

    // An unknown strategy is refused before touching the database.
    let output = Command::new(bicdb_bin())
        .args([
            "index",
            "pack",
            temp.path().to_str().unwrap(),
            "idx_places_geometry_spatial",
            "--strategy",
            "quadtree",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("unknown pack strategy"));
}

#[test]
fn index_pack_create_builds_a_packed_index_from_scratch() {
    let temp = tempfile::tempdir().unwrap();
    let config = || {
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged)
    };
    {
        let mut db = BicDb::open_with_config(temp.path(), config()).unwrap();
        db.create_collection("shops").unwrap();
        db.close().unwrap();
        let mut db = BicDb::open_with_config(temp.path(), config()).unwrap();
        db.bulk_load_insert(
            "shops",
            (0..40u32)
                .map(|index| {
                    Record::new(format!("s-{index:02}"))
                        .with_geometry(Geometry::point(0.001 * f64::from(index), 1.0).unwrap())
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();
        db.close().unwrap();
    }

    // No index exists; --create registers it packed, never building the
    // resident tree.
    let output = Command::new(bicdb_bin())
        .args([
            "index",
            "pack",
            temp.path().to_str().unwrap(),
            "idx_shops_geometry_spatial",
            "--create",
            "shops",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "index pack --create failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["entry_count"], json!(40));
    assert_eq!(report["generation"], json!(1));

    // Mismatched derived name is refused before touching the database.
    let output = Command::new(bicdb_bin())
        .args([
            "index",
            "pack",
            temp.path().to_str().unwrap(),
            "idx_wrong_name",
            "--create",
            "shops",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("does not match"));

    let db = BicDb::open_with_config(temp.path(), config()).unwrap();
    let hits = db
        .within_radius("shops", "geometry", 0.0, 1.0, 20_000.0)
        .unwrap();
    assert_eq!(hits.len(), 40);
}

#[test]
fn index_verify_accepts_a_single_index_name() {
    let temp = spatial_fixture();
    let output = Command::new(bicdb_bin())
        .args([
            "index",
            "verify",
            temp.path().to_str().unwrap(),
            "idx_places_geometry_spatial",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "single-index verify failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["index_name"], json!("idx_places_geometry_spatial"));
    assert_eq!(report["valid"], json!(true));

    let output = Command::new(bicdb_bin())
        .args([
            "index",
            "verify",
            temp.path().to_str().unwrap(),
            "no_such_index",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("not found"));
}
