# BicDB Spatial v0.1

BicDB Spatial v0.1 provides a small durable geometry representation, a focused
SQL function subset, and rebuildable R-tree spatial indexes for supported local
predicates. It is not a PostGIS-compatible surface.

The goal is practical offline GIS for applications that still need location
features when a network, hosted map service, or central database is unavailable:
clinic finders, field routing, local asset search, embedded agents, rural
operations, mobile sync, and edge devices. BicDB keeps canonical records in the
database and treats spatial indexes, routing graphs, and derived structures as
rebuildable acceleration layers.

BicDB Spatial does not render maps. Frontends can read geometry or route results
from BicDB and render them with browser-native mapping libraries such as
Leaflet or MapLibre.

## v0.1 Integration Status

The v0.1 integration gate is complete for the documented vertical slice:
canonical spatial records can be stored, indexed, queried by nearest and
within-radius APIs/CLI, and recovered after reopen; spatial SQL filters can
combine with exact vector ordering; bounded OSM PBF fixtures can build a small
road graph for shortest-path routing; route optimization is verified for the
documented small stop counts; and spatial audit events use the existing
EventStream/sync export path.

Known limitations are listed in [Limitations and Roadmap](#limitations-and-roadmap).
Deferred GIS work should be tracked explicitly in follow-up issues rather than
treated as hidden compatibility.

## Geometry Types

The Rust API exports `bicdb_core::Geometry` with these v0.1 variants:

- `Point`: longitude/latitude coordinate pair.
- `LineString`: ordered coordinate sequence with at least two points.
- `Polygon`: closed exterior ring plus optional closed interior rings.
- `Envelope`: bounding box stored as `min_lon`, `min_lat`, `max_lon`,
  `max_lat`.

Coordinates are `f64` and must be finite. Unsupported GIS types return explicit
errors rather than being coerced.

## SQL Type Boundary

BicDB has two intentionally separate geometric surfaces. They share coordinate
shapes but not SQL types, units, indexes, or compatibility guarantees.

| Surface | Values | Semantics | Index syntax |
| --- | --- | --- | --- |
| PostgreSQL planar geometry | `point`, `line`, `lseg`, `box`, `path`, `polygon`, `circle` columns | Coordinate-neutral Cartesian operations in the units supplied by the application | PostgreSQL-compatible B-tree where defined and GiST/SP-GiST/BRIN operator classes |
| BicDB Spatial | The intrinsic `Record.geometry` field exposed as the virtual SQL field `geometry` | Longitude/latitude by convention; point distance and radius functions return meters | `CREATE SPATIAL INDEX ... ON table(geometry)` |
| PostGIS | Not installed | No `geometry`/`geography` SQL types, SRIDs, reprojection, or PostGIS operator contract | Not available |

The constructors are distinct: `point(x, y)` returns PostgreSQL `point`, while
`ST_Point(lon, lat)` returns a BicDB Spatial value. There are no implicit casts
between them. Passing a planar value to `ST_*`, casting a Spatial value to a
planar type, or mixing their index syntax returns an error rather than silently
changing units or semantics.

A declared SQL column always wins over the intrinsic field. For example,
`geometry point` is an ordinary PostgreSQL planar `point` column despite its
name. To use the intrinsic Spatial field, do not declare a SQL column named
`geometry`; write `Record.geometry` through the Rust API or the virtual
`geometry` field and use `CREATE SPATIAL INDEX`.

`CREATE EXTENSION postgis` and PostGIS companion extensions fail with SQLSTATE
`0A000`. Creating a column of type `geometry` or `geography` fails as an unknown
type. BicDB must only accept those declarations if a future optional PostGIS
compatibility module implements the corresponding types and contracts.

## WKT

`Geometry::from_wkt` parses v0.1 WKT values for `POINT`, `LINESTRING`, and
`POLYGON` using the `wkt` and `geo-types` crates. BicDB envelope values use a
small canonical extension:

```text
BBOX (-122.5 37.7, -122.3 37.9)
```

`ENVELOPE (...)` is accepted as an input alias. `Geometry::to_wkt` renders
envelopes as `BBOX (...)`.

## GeoJSON

`Geometry::from_geojson_str` and `Geometry::from_geojson_value` accept GeoJSON
Geometry objects and single Feature objects for the v0.1 geometry types.
`Geometry::to_geojson_value` emits a GeoJSON Geometry object.

Envelope values serialize as a polygon ring with a four-value `bbox`. The same
shape deserializes back to `Geometry::Envelope`; ordinary polygons remain
`Geometry::Polygon`.

## Binary Geometry Frame

`Geometry::to_bicdb_frame` and `Geometry::from_bicdb_frame` define the BicDB
geometry value frame. The current frame starts with:

```text
BICG | version=1 | kind
```

The remaining payload is little-endian `f64` coordinate data with little-endian
`u32` counts for variable-length line and polygon coordinate sequences. The
version byte is part of the format so future geometry expansion can reject or
migrate unknown frames cleanly.

## Record Storage

`Record` has an optional typed `geometry` field and a builder:

```rust
use bicdb_core::{Geometry, Record};
use serde_json::json;

let record = Record::new("clinic-1")
    .with_geometry(Geometry::point(-122.4, 37.8)?)
    .with_metadata(json!({"name": "clinic"}));

# Ok::<(), bicdb_core::BicDbError>(())
```

Record metadata remains untyped `serde_json::Value`, so existing JSON metadata
use cases continue to work. Applications may also keep GeoJSON copies or
domain-specific spatial metadata under `metadata` when they need that shape.

## Spatial Events and Sync

Spatial audit events use the normal BicDB `EventStream`; there is no separate
spatial event log or networking layer. Enable them with the same optional
`DbConfig::with_audit_events(true)` switch used by record audit events, then
read or sync them from the `bicdb.spatial` stream.

Current spatial event types:

- `SpatialRecordInserted` is emitted for spatial record upserts the spatial
  subsystem can observe, including typed `record.geometry` writes and metadata
  fields covered by an existing spatial index.
- `SpatialIndexUpdated` is emitted when a spatial index is created, explicitly
  rebuilt, or refreshed after indexed collection mutations.
- `RouteComputed` is emitted by opt-in route APIs such as
  `shortest_path_with_events`, `route_distance_with_events`, and
  `optimize_route_with_events`. Route event payloads include metadata such as
  graph, operation, algorithm or heuristic, distance, and counts; they do not
  duplicate full route geometry or node path payloads.
- `GeoFenceTriggered` and `DeviceLocationUpdated` can be appended through the
  `emit_geofence_triggered` and `emit_device_location_updated` hooks.

Because these are ordinary BicDB events, `export_events_since` and sync bundles
preserve them along with other local audit records.

## SQL Function Subset

The SQL layer exposes a small PostGIS-inspired function set. Geometry arguments
can be BicDB geometry values returned by these functions, WKT text, or GeoJSON
Geometry/Feature objects represented as JSON values or JSON text.

```sql
SELECT ST_AsText(ST_Point(-122.4194, 37.7749));
SELECT ST_AsGeoJSON(ST_Point(-122.4194, 37.7749));
SELECT ST_DWithin(
  ST_Point(-122.4194, 37.7749),
  ST_Point(-118.2437, 34.0522),
  560000
);
SELECT ST_Contains(
  'POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))',
  ST_Point(1, 1)
);
SELECT ST_AsText(ST_Envelope('LINESTRING (0 1, 3 4)'));
```

Supported functions:

- `ST_Point(lon, lat)` returns a BicDB geometry point.
- `ST_AsText(geom)` returns v0.1 WKT text. Envelopes render as `BBOX (...)`.
- `ST_AsGeoJSON(geom)` returns GeoJSON Geometry text.
- `ST_Distance(a, b)` supports point-to-point distance only. Coordinates are
  interpreted as longitude/latitude degrees and the result is meters using a
  spherical Haversine model.
- `ST_DWithin(a, b, meters)` uses the same point-to-point Haversine distance
  model as `ST_Distance`.
- `ST_Contains(poly, point)` supports polygon/envelope contains point.
- `ST_Intersects(a, b)` supports the v0.1 geometry variants (`Point`,
  `LineString`, `Polygon`, and `Envelope`) through the `geo` crate's planar
  intersection predicates.
- `ST_Envelope(geom)` returns the geometry bounding box as a BicDB envelope.
- `shortest_path(graph, start_point, end_point)` snaps the input points to the
  nearest road graph nodes and returns a JSON route object.
- `route_distance(graph, start_point, end_point)` returns the shortest route
  distance in meters.
- `optimize_route(graph, ARRAY[point1, point2, ...])` returns a JSON object
  with `ordered_stops` and `estimated_distance_m` for a heuristic field route.

Unsupported geometry/function combinations return explicit unsupported-feature
errors. For example, `ST_Distance` on non-point geometries is rejected rather
than returning planar degrees or an arbitrary approximation.

## Offline Routing Graphs

BicDB Spatial supports small deterministic road graphs for offline shortest-path
queries. A graph named `roads` is stored in two ordinary authoritative
collections:

- `roads_nodes`: one record per `RoadNode { id, lon, lat }`. The record id is
  the node id. Coordinates may be stored as the canonical `geometry` POINT or
  as numeric metadata fields `lon` and `lat`.
- `roads_edges`: one record per directed `RoadEdge { from, to, distance_m,
  duration_s, road_class, metadata }`. Edge fields are stored in record
  metadata. `distance_m` and `duration_s` must be non-negative finite numbers.

Routing structures are rebuilt from these collections for each query, so the
canonical records remain authoritative and reopen recovery is safe. This is not
a full navigation engine: there are no turn restrictions, map rendering,
traffic models, or PostGIS routing compatibility claims.

```rust
use bicdb_core::{BicDb, Geometry};

let db = BicDb::open("./clinicdb")?;
let route = db.shortest_path(
    "roads",
    &Geometry::point(-122.4194, 37.7749)?,
    &Geometry::point(-122.2711, 37.8044)?,
)?;
let distance_m = db.route_distance(
    "roads",
    &Geometry::point(-122.4194, 37.7749)?,
    &Geometry::point(-122.2711, 37.8044)?,
)?;

# Ok::<(), bicdb_core::BicDbError>(())
```

`shortest_path` uses Dijkstra over a `petgraph` directed graph.
`shortest_path_astar` is also available and uses a Haversine distance heuristic
between road nodes. Both APIs snap start and end points to the nearest node and
return snapped endpoint details in `RoutePath`.

```sql
SELECT shortest_path('roads', ST_Point(-122.4194, 37.7749), ST_Point(-122.2711, 37.8044));
SELECT route_distance('roads', ST_Point(-122.4194, 37.7749), ST_Point(-122.2711, 37.8044));
SELECT optimize_route(
  'roads',
  ARRAY[
    ST_Point(-122.4194, 37.7749),
    ST_Point(-122.2711, 37.8044),
    ST_Point(-122.3321, 47.6062)
  ]
);
```

Disconnected graphs return a clear no-route error. Missing node/edge
collections, malformed route records, non-point inputs, and edges that reference
missing nodes are rejected rather than silently producing partial routes.

`optimize_route` is a practical heuristic, not an exact TSP solver. It keeps the
first stop fixed, builds a nearest-neighbor baseline, runs open-route 2-opt
improvement, and keeps the shorter of that result and 2-opt over the input
order. When both `<graph>_nodes` and `<graph>_edges` collections exist, leg
estimates use the same route-graph distance model as `route_distance`; otherwise
they fall back to direct point-to-point Haversine distance. The result is useful
for small offline field routes such as 5, 10, 25, or 50 stops, but it does not
prove global optimality and does not imply PostGIS or pgRouting compatibility.

## Offline OSM Import

`bicdb spatial import-osm` imports a bounded OpenStreetMap `.osm.pbf` extract
into the route graph storage described above:

```sh
bicdb spatial import-osm ./region.osm.pbf --path ./clinicdb --bbox -122.42,37.77,-122.41,37.78
```

The default graph name is `roads`, which writes `roads_nodes` and `roads_edges`.
Use `--graph <name>` to write `<name>_nodes` and `<name>_edges` instead.
`--bbox` is required and uses `minLon,minLat,maxLon,maxLat`; invalid numeric
values, reversed bounds, and coordinates outside longitude `[-180,180]` or
latitude `[-90,90]` return errors before import.

For v0.1 BicDB uses the `osmpbfreader` crate for offline PBF parsing. It is
practical for the current bounded-region importer and works with the workspace's
Rust 1.80 floor. `fast-osmpbf` was considered, but its current crate metadata
requires a newer Rust compiler than this workspace. This importer is
intentionally limited to routing graph extraction; it does not claim full OSM
model coverage or PostGIS compatibility.

Imported roads are OpenStreetMap ways with routable `highway` classes such as
`residential`, `service`, `primary`, `secondary`, and related link classes.
Nodes are imported only when their coordinates are inside the bbox. Edges are
created between adjacent imported nodes on selected ways. Oneway tags `yes`,
`true`, and `1` create forward edges; `-1` creates reverse edges; other values
create bidirectional edges. Edge distance uses BicDB's Haversine meter model,
and duration is a simple class-based estimate. Turn restrictions, access rules,
traffic, lane metadata, map rendering, and relation processing are out of scope
for this v0.1 importer.

Import replaces the selected graph's `*_nodes` and `*_edges` collections before
writing the bounded extract, so rerunning it cannot leave stale road records
from an older bbox. Record ids are derived from OSM ids. The canonical BicDB
records remain authoritative, so routing structures continue to be rebuilt from
`*_nodes` and `*_edges`.

After import, route with:

```sh
bicdb spatial route ./clinicdb --from -122.4194,37.7749 --to -122.4174,37.7749
bicdb spatial route ./clinicdb --from -122.4194,37.7749 --to -122.4174,37.7749 --json
```

`route` snaps each input point to the nearest road graph node and returns the
graph name, route distance in meters, route node ids, and snapped endpoint
details. Use `--graph <name>` to query a non-default imported graph.

## Spatial Indexes

Spatial indexes use in-memory `rstar` R-trees and persist only their metadata in
`indexes.json`. Canonical BicDB records remain authoritative; spatial index
contents are rebuilt from records on reopen and after writes that affect the
indexed collection.

```sql
CREATE SPATIAL INDEX idx_places_geometry ON places(geometry);

EXPLAIN SELECT id
FROM places
WHERE ST_DWithin(geometry, ST_Point(-122.4194, 37.7749), 20000);
```

For supported predicates, `EXPLAIN` includes `SpatialIndexScan idx_name`.
BicDB v0.1 supports spatial index planning for:

- `WHERE ST_DWithin(field, ST_Point(lon, lat), meters)` on point geometries.
- `WHERE ST_Intersects(field, ST_Envelope(...))` for envelope candidates.

The original predicate is always re-evaluated after loading R-tree candidates,
so envelope false positives do not change query results. Unsupported geometry
types or non-geometry indexed fields return clear errors.

### Packed Spatial Indexes (`server_paged`)

A spatial corpus that arrives as a release/snapshot doesn't need a dynamic
tree. `pack_spatial_index` (Rust API) bulk-packs the index into an immutable
node tree stored in the durable index keyspace:

1. Stream every `(pk, rect)` from the page store (identity scan, no rows
   materialized).
2. Order spatially — `SpatialPackStrategy::Hilbert` (default) or
   `SpatialPackStrategy::Str`; benchmark both, the builder treats them as
   equals.
3. Pack leaves to capacity, build parent MBR levels bottom-up, write each
   node once as an immutable value under an unpublished generation.
4. Publish with one atomic meta swap; the retired generation is
   garbage-collected afterwards.

Packing is available on three surfaces:

```sql
-- SQL (exclusive session; refuses to run inside a transaction).
PACK SPATIAL INDEX idx_places_geometry;              -- Hilbert (default)
PACK SPATIAL INDEX idx_places_geometry USING STR;    -- Sort-Tile-Recursive
-- Returns one row: index_name, strategy, generation, entry_count,
-- node_count, height.
```

```bash
# CLI (server_paged databases).
bicdb index pack <path> idx_places_geometry_spatial --strategy hilbert --json
```

```rust
// Rust API.
db.pack_spatial_index("idx_places_geometry_spatial")?;
db.pack_spatial_index_with_strategy(name, SpatialPackStrategy::Str)?;
// Create + pack in one step, never building the resident tree — the only
// index-creation path whose peak memory does not scale with the corpus:
db.create_packed_spatial_index("places", "geometry", SpatialPackStrategy::Hilbert)?;
```

**Bounded memory and resumability (Hilbert).** Hilbert packs run through an
external-sort pipeline: the corpus streams into sorted run files (default
1M entries per run, `BICDB_SPATIAL_PACK_RUN_ENTRIES` to override) under
`<db>/spatial_pack/<index>/`, checkpointing the scan cursor per run, then a
k-way merge feeds the streaming node builder — peak memory is one run buffer
plus one level of node summaries, regardless of corpus size. A crash- or
OOM-interrupted pack resumes from its checkpoint when re-run (`bicdb index
pack` again, or the same API call; the report's `resumed` flag says which
happened). Writes landing between interruption and resume are safe: durable
delta rows mask every touched pk against whatever the runs captured, and the
publish folds only delta rows that are byte-identical to their capture. On
reopen mid-build the index loads EMPTY (queries incomplete until the pack
completes) rather than streaming the corpus into a resident tree. Hilbert
keys use fixed geographic bounds (lon −180..180, lat −90..90); planar data
outside that window clamps to edge cells — ordering quality degrades there,
correctness does not. STR remains the in-memory, non-resumable benchmark
alternative.

For a brand-new index on a large corpus, prefer:

```bash
bicdb index pack <path> idx_places_geometry_spatial --create places --json
```

which registers the index and packs it in one step — plain `CREATE SPATIAL
INDEX` on N rows first builds an in-memory R-tree of N entries (~14 GiB at
74M), which is exactly what `--create` skips.

After packing, queries combine the packed base (a recursive page-pruning
descent through durable nodes) with a small resident **delta** R-tree.
Writes tombstone the touched pk in the base at the id level and land in the
delta, and each commit also writes a durable delta row — so reopen loads the
meta plus the delta tail instead of rescanning the corpus. Re-packing (or
`rebuild_index`, which re-packs automatically when a packed base exists)
folds the delta into a fresh base. `DROP INDEX` purges the whole namespace.

## Hybrid Spatial + Vector SQL

BicDB supports SQL queries that filter spatial candidates and then rank the
remaining records by vector distance:

```sql
CREATE SPATIAL INDEX idx_patients_geometry ON patients(geometry);

SELECT id
FROM patients
WHERE risk_score >= 80
  AND ST_DWithin(geometry, ST_Point(-122.4194, 37.7749), 20000)
ORDER BY embedding <=> '[1,0,0]'
LIMIT 10;
```

For sensitive-domain memory retrieval this supports shapes such as high-risk
patients within 20km ranked by semantic embedding, or clinics within 20km
ranked by a specialty embedding:

```sql
SELECT id
FROM clinics
WHERE ST_DWithin(geometry, ST_Point(-122.4194, 37.7749), 20000)
ORDER BY embedding <=> '[0,1,0]'
LIMIT 5;
```

When a matching spatial index exists, `ST_DWithin` supplies the index-backed
candidate set. BicDB then reloads those records from canonical storage,
re-evaluates the full `WHERE` predicate, and applies `ORDER BY embedding <=>`
as an exact rerank before `LIMIT`. The vector HNSW fast path is currently used
only for pure vector `ORDER BY ... LIMIT` queries without `WHERE`; hybrid
spatial + vector queries fall back safely to spatial candidates plus exact
vector sorting, including when `bicdb.vector_search = 'ann'`.

If no matching spatial index exists, BicDB uses an exact scan and still applies
the same distance filter and exact vector ordering. Unsupported spatial
predicate shapes are not planned as spatial index scans; unsupported geometry
types still return clear errors instead of silently changing results.

## Rust API Queries

For common point lookups, the Rust API provides collection-and-field methods so
applications do not need to hand-write SQL:

```rust
use bicdb_core::{BicDb, Geometry, Record};

let mut db = BicDb::open("./clinicdb")?;
db.create_collection("clinics")?;
db.insert(
    "clinics",
    Record::new("clinic-1").with_geometry(Geometry::point(-122.4194, 37.7749)?),
)?;

db.create_spatial_index("clinics", "geometry")?;

let nearest = db.nearest("clinics", "geometry", -122.4, 37.8, 5)?;
let nearby = db.within_radius("clinics", "geometry", -122.4, 37.8, 20_000.0)?;

# Ok::<(), bicdb_core::BicDbError>(())
```

Results are returned as `SpatialQueryResult { record, distance_meters }` sorted
by distance and then record id. `field` may be `geometry` for the canonical
typed geometry field or a metadata path such as `location` or
`address.point`. Metadata spatial values must be WKT strings or GeoJSON
objects.

If a matching spatial index exists, `nearest` and `within_radius` use it for
candidate lookup. If no matching index exists, they fall back to an exact scan
over the collection and still return exact Haversine distances. These APIs
support point geometries only; missing fields and non-point values return clear
index errors.

## CLI Queries

The CLI exposes the same common point lookups. The default field is
`geometry`; use `--field` for WKT or GeoJSON metadata paths.

```sh
bicdb spatial import-osm ./region.osm.pbf --path ./clinicdb --bbox -122.42,37.77,-122.41,37.78
bicdb spatial route ./clinicdb --from -122.4194,37.7749 --to -122.4174,37.7749
bicdb spatial nearest ./clinicdb clinics --point -122.4,37.8 --limit 5
bicdb spatial within-radius ./clinicdb clinics --point -122.4,37.8 --meters 20000
bicdb spatial nearest ./clinicdb clinics --field location --point -122.4,37.8 --json
```

Table output is the default and includes `id`, `distance_meters`, and
`metadata`. `--json` emits an array of row objects, and `--csv` emits the same
columns as CSV. Invalid points must use the exact `lon,lat` form and return a
clear CLI error.

## Frontend Integration

BicDB core owns storage, SQL, indexing, routing, OSM import, and data export. It
intentionally does not include a map renderer, tile server, browser widget, or
style system. Keep rendering in the application layer and pass only stable data
across the boundary: GeoJSON geometry, route node/stop JSON, record metadata,
and explicit distances.

For Leaflet, use BicDB SQL or Rust APIs to return GeoJSON strings or JSON values
and add them to a Leaflet layer in the frontend:

```sql
SELECT id, ST_AsGeoJSON(geometry) AS geometry
FROM clinics
WHERE ST_DWithin(geometry, ST_Point(-122.4194, 37.7749), 20000);
```

```js
const feature = {
  type: "Feature",
  geometry: JSON.parse(row.geometry),
  properties: { id: row.id },
};
L.geoJSON(feature).addTo(map);
```

For MapLibre GL JS, build a GeoJSON `FeatureCollection` from the same query
results and attach it as a source:

```js
map.addSource("clinics", {
  type: "geojson",
  data: { type: "FeatureCollection", features },
});
map.addLayer({
  id: "clinics-circle",
  type: "circle",
  source: "clinics",
  paint: { "circle-radius": 5, "circle-color": "#1f7a8c" },
});
```

Route results currently return graph name, route distance, route node ids, and
snapped endpoint details. If a frontend needs a drawn route polyline, resolve
the returned node ids from `<graph>_nodes` and construct a GeoJSON
`LineString` in the application. BicDB core should not grow Leaflet, MapLibre,
WebGL, tile, or style dependencies for that purpose.

## Benchmarks

The spatial benchmark suite uses synthetic but deterministic fixtures so runs
are reproducible and do not require map downloads:

```sh
cargo run -p bicdb-cli --features bench -- bench spatial --points 100000 \
  --json-out target/spatial.json --csv-out target/spatial.csv
cargo run -p bicdb-cli --features bench -- bench spatial-nearest --points 100000 --queries 10000 \
  --json-out target/spatial-nearest.json --csv-out target/spatial-nearest.csv
cargo run -p bicdb-cli --features bench -- bench route --nodes 10000 --edges 30000 \
  --json-out target/route.json --csv-out target/route.csv
```

`bench spatial` reports point insert throughput, spatial index build time,
index catalog size, database size, and collection storage sizes.
`bench spatial-nearest` builds the same point fixture and reports nearest and
radius query latency as p50/p95/p99, plus result counts and database size.
`bench route` builds a connected synthetic road graph in canonical BicDB
storage and reports route query p50/p95/p99 and database size.

CI and local smoke checks can use reduced fixtures, for example:

```sh
cargo run -p bicdb-cli --features bench -- bench spatial --points 10000
cargo run -p bicdb-cli --features bench -- bench spatial-nearest --points 10000 --queries 100
cargo run -p bicdb-cli --features bench -- bench route --nodes 1000 --edges 3000
```

Checked-in sample reports from a local debug run are available in
`reports/spatial-10k.json`, `reports/spatial-nearest-10k.json`, and
`reports/route-1k.json`, with matching CSV files.

## Limitations and Roadmap

Current v0.1 limitations are intentional:

- No full PostGIS compatibility claim. Function names are PostGIS-inspired, but
  behavior is limited to the documented subset.
- No SRID system, coordinate reprojection, geography/geometry dual type model,
  3D coordinates, measures, curves, rasters, topology, or spatial joins beyond
  the supported SQL predicates.
- `ST_Distance` and `ST_DWithin` support point-to-point Haversine distance only.
  Other geometry combinations return explicit errors.
- Spatial nearest and radius lookups support point geometries only.
- R-tree contents are in-memory and rebuild from canonical records; only index
  metadata is persisted.
- OSM import is bounded road-graph extraction from `.osm.pbf`, not a complete
  OSM model import. Relations, turn restrictions, access rules, traffic, lanes,
  and rendering data are out of scope.
- Route optimization is a nearest-neighbor plus 2-opt heuristic, not an exact
  TSP solver.
- Hybrid spatial/vector SQL uses spatial candidate filtering plus exact vector
  reranking; the vector HNSW fast path is reserved for pure vector nearest
  queries in v0.1.

Likely future work includes broader geometry predicates, richer route geometry
export, optional projection support, more OSM metadata handling, persisted or
incrementally maintained spatial sidecars, and tighter SQL planning for mixed
spatial/vector workloads. Those additions should preserve the core rule that
canonical BicDB records remain authoritative and derived spatial structures are
recoverable.
