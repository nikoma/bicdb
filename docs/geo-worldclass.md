# BicDB Geo — the location-intelligence campaign

Strategic rule: **do not rebuild PostGIS.** Build the compatibility floor
that unlocks the existing ecosystem, then push hard into what PostGIS is
not designed to be. The end state is not "a database with GIS" — it is the
engine for **location intelligence** (Wewobo: 74M businesses).

## Five pillars

1. **PostGIS-compatible enough to disappear into existing tooling.**
   WKB/EWKB, Multi* geometries, SRID handling, the common ST_* set, QGIS
   opening BicDB over pgwire and treating it as a normal spatial database.
2. **Geo + FTS as one query engine** — the most important differentiator.
   "Cardiologists mentioning PCOS within 30 minutes of here" is ONE native
   query, never a stitch across systems.
3. **Geospatial analytics, not just geometry.** H3/S2 cells, spatial
   joins, heatmaps, clustering, drive-time catchments, nearest-N at huge
   scale, density overlays, vector tiles directly from BicDB.
4. **Routing as a first-class database primitive.** Matrices, isochrones,
   profiles, turn restrictions, multimodal, map matching, continent-scale
   preprocessing. "People/businesses reachable within 10 minutes" is the
   intelligence layer's workhorse.
5. **Streaming/offline geo.** Event streams + mesh replication: moving
   assets, enter/exit/dwell, field collection, offline maps, peer-to-peer
   convergence.

## The headline abstraction: `travel_time`

```sql
SELECT * FROM businesses
WHERE category = 'dentist'
  AND travel_time(location, :origin, 'driving') <= interval '15 minutes'
ORDER BY wewobo_score DESC;
```

Reachability as a WHERE-clause primitive (isochrone-backed, index-driven,
budgeted), composable with FTS rank and analytics:
"areas with population > 75k, dentists reachable in 15 min < 4, average
competitor score < 55, rent proxy below median."

## Disproportionate-value additions

- MVT generation **with tile clipping + generalization** as a first-class op.
- Address normalization/geocoding: aliases, locality hierarchy,
  multilingual names (FTS-fused).
- Administrative boundaries with containment hierarchy:
  country → state → county → city → neighborhood.
- **Antimeridian/polar correctness from day one** (every new op is tested
  across the antimeridian and near poles before merge).
- Elevation/raster support, minimal first: terrain, slope, population and
  climate grids.
- **Temporal geo** on the append/event model: "where was this
  business/road/boundary six months ago?"
- Batch spatial jobs: millions of points × millions of polygons/routes
  without pretending everything is interactive SQL (budgeted, resumable,
  checkpointed — the pack-build pattern).

## Benchmark discipline

The 74M-business corpus is THE benchmark, not synthetic data. Publish:
"74M businesses, nearest 25 in ~140µs; global text+geo ranked search in X
ms; 15-minute drive-time catchment across Y million roads in Z ms."
Differential-test the ST_* subset against PostGIS as the oracle.

## TODO — work the list in order, one PR per item (bump 0.0.1 each)

- [x] **G1. WKB/EWKB + Multi\*/GeometryCollection + SRID plumbing** (PR #497) —
      encode/decode (little+big endian), `ST_GeomFromText/WKB/GeoJSON`,
      `ST_AsBinary/AsEWKB`, Multi* in Geometry enum + index paths; pgwire
      binary geometry columns. Antimeridian tests from day one.
- [x] **G2. Predicate + constructive layer** (PR #498) (geo crate adoption):
      DE-9IM (`ST_Relate`, touches/crosses/overlaps/covers), `ST_Buffer`
      (geodesic meters), Union/Intersection/Difference, Simplify,
      ConvexHull, Centroid, geodesic Area/Length, IsValid/MakeValid.
- [x] **G3. Spatial join (R-tree executor)** (PR #499) — bbox nested-loop over the
      packed index as a planner route (`JOIN ... ON ST_Intersects/DWithin`),
      budgeted; batch-job variant for the millions×millions case.
- [x] **G4. H3/grid analytics** (PR #500) — cell functions, grid aggregation
      (heatmaps, density overlays), H3 as mesh/shard partition key.
- [x] **G5. Vector tiles** (PR #501) — `ST_AsMVT` + tile clipping/generalization
      from the packed index; `bicdb tiles serve`; PMTiles export.
- [x] **G6. Geocoder** (PR #502) — OSM names × FTS: forward + reverse, aliases,
      locality hierarchy, multilingual; admin-boundary containment
      hierarchy loaded from OSM.
- [x] **G7. Route matrices + isochrones + travel_time** (PR #503) — many-to-many matrices,
      isochrone polygons, `travel_time()` SQL primitive backed by them;
      profiles + turn restrictions; contraction hierarchies for scale.
- [x] **G8. Raster/elevation (minimal)** (PR #504) — grid tiles as blobs + sampling
      functions (elevation/slope/population lookup at point/polygon).
- [x] **G9. Streaming/offline geo** (PR #505) — enter/exit/dwell geofence engine
      over the broker; reverse-containment (point-in-many-fences) packed
      index; mesh-replicated layers + offline tile bundles.
- [x] **G10. Temporal geo** (PR #506) — as-of queries over the audit/event model
      for geometries and boundaries.
- [x] **G11. Hardening** (PR #507; the 74M-corpus benchmark run is an ops
      follow-up on benchmark-primary once the business dataset is loaded) — differential fuzzing vs PostGIS oracle on the
      supported subset; antimeridian/polar suite; validity repair; 74M
      benchmark publication.

Progress log: mark items done here with PR numbers as they merge.

**Campaign G1-G11 engineering COMPLETE** (PRs #497-#507, 1.0.157 →
1.0.167-beta, 2026-08-12). Remaining follow-ups tracked above in context:
74M benchmark publication (ops, benchmark-primary), contraction hierarchies + turn
restrictions (G7 scale), line/polygon ST_Buffer offsetting, full OGC
validity + split-box antimeridian topology, binary raster tiles, tile
server + PMTiles product surface, OSM place/boundary import recipes.
