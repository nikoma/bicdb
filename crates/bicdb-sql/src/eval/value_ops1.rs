//! Split out of the parent module to keep files digestible; behavior
//! unchanged. Items are re-exported from the parent via `pub(crate) use`.
use super::*;
#[allow(unused_imports)]
use crate::*;

pub(crate) fn eval_privilege_function_value(
    db: &BicDb,
    name: &str,
    args: &[SqlValue],
    session_gucs: &HashMap<String, String>,
) -> Result<Option<SqlValue>> {
    match name {
        "has_schema_privilege" | "pg_catalog.has_schema_privilege" => {
            Ok(Some(SqlValue::Bool(has_schema_privilege(db, args)?)))
        }
        "has_table_privilege" | "pg_catalog.has_table_privilege" => {
            Ok(Some(SqlValue::Bool(has_table_privilege(db, args)?)))
        }
        "has_function_privilege" | "pg_catalog.has_function_privilege" => {
            if args.iter().any(|value| matches!(value, SqlValue::Null)) {
                return Ok(Some(SqlValue::Null));
            }
            Ok(Some(SqlValue::Bool(has_function_privilege(db, args)?)))
        }
        "has_type_privilege" | "pg_catalog.has_type_privilege" => {
            if args.iter().any(|value| matches!(value, SqlValue::Null)) {
                return Ok(Some(SqlValue::Null));
            }
            Ok(Some(SqlValue::Bool(has_type_privilege(
                db,
                args,
                &current_user_from_gucs(session_gucs),
            )?)))
        }
        "obj_description" | "pg_catalog.obj_description"
            if args.len() >= 2
                && sql_value_text(&args[1])
                    .is_some_and(|catalog| catalog.eq_ignore_ascii_case("pg_type")) =>
        {
            Ok(Some(
                args.first()
                    .and_then(sql_value_i64)
                    .map(|oid| user_type_comment_by_oid(db, oid))
                    .transpose()?
                    .flatten()
                    .map(SqlValue::String)
                    .unwrap_or(SqlValue::Null),
            ))
        }
        "pg_get_userbyid" | "pg_catalog.pg_get_userbyid" => {
            let [value] = args else {
                return Err(SqlError::InvalidSql(
                    "pg_get_userbyid expects one OID".into(),
                ));
            };
            if matches!(value, SqlValue::Null) {
                return Ok(Some(SqlValue::Null));
            }
            let oid = sql_value_i64(value)
                .ok_or_else(|| SqlError::InvalidSql("pg_get_userbyid requires an OID".into()))?;
            let name = list_roles(db)?
                .into_iter()
                .find(|role| role_oid(&role.name) == oid)
                .map(|role| role.name)
                .unwrap_or_else(|| format!("unknown (OID={oid})"));
            Ok(Some(SqlValue::String(name)))
        }
        "pg_has_role" | "pg_catalog.pg_has_role" => {
            if args.iter().any(|value| matches!(value, SqlValue::Null)) {
                return Ok(Some(SqlValue::Null));
            }
            Ok(Some(SqlValue::Bool(pg_has_role(
                db,
                args,
                &current_user_from_gucs(session_gucs),
            )?)))
        }
        _ => Ok(None),
    }
}

pub(crate) fn eval_db_catalog_function_value(
    db: &BicDb,
    name: &str,
    args: &[SqlValue],
    snapshot_watermark: Option<u64>,
    session_gucs: Option<&HashMap<String, String>>,
) -> Result<Option<SqlValue>> {
    let bare_name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    if bare_name.starts_with("bicdb_") || bare_name == "travel_time" {
        // Authorization keys on the EFFECTIVE role, never `session_user`:
        // `SET ROLE` must drop privilege here exactly as it does at the table
        // gate. Keying on `session_user` left every bicdb_* admin function —
        // including state-mutating ones like bicdb_advance_transaction_floor —
        // callable for the life of a superuser-authenticated pooled connection
        // that had lowered its role per tenant. See the doctrine on
        // `SqlSession::current_user_is_superuser`.
        let role = session_gucs
            .map(current_user_from_gucs)
            .unwrap_or_else(current_role_name);
        let superuser = role == BOOTSTRAP_ROLE_NAME
            || load_role_schema(db, &role)?.is_some_and(|role| role.superuser);
        if !superuser {
            return Err(SqlError::BicDb(BicDbError::Authorization(format!(
                "permission denied for function {bare_name}: superuser is required"
            ))));
        }
    }
    if let Some(value) = eval_fts_db_function_value(db, name, args, session_gucs)? {
        return Ok(Some(value));
    }
    match name {
        // REPAIR: advance the transaction floor past a corrupt future xid
        // (permanently-unwritable-row corruption). Online and durable: the
        // store checkpoints before this returns.
        // SURGICAL repair: replace one row's corrupt header stamp with the
        // structural committed marker; expected-value guarded, idempotent.
        "bicdb_repair_row_xid" => {
            require_arg_count(name, args, 4)?;
            if args.iter().any(|arg| matches!(arg, SqlValue::Null)) {
                return Ok(Some(SqlValue::Null));
            }
            let text = |value: &SqlValue| -> Result<String> {
                match value {
                    SqlValue::String(text) => Ok(text.clone()),
                    other => Err(SqlError::InvalidSql(format!(
                        "bicdb_repair_row_xid expects text arguments, got {other:?}"
                    ))),
                }
            };
            let collection = text(&args[0])?;
            let pk = text(&args[1])?;
            let field = text(&args[2])?;
            let expected = match &args[3] {
                SqlValue::Int(value) if *value >= 0 => *value as u64,
                other => {
                    return Err(SqlError::InvalidSql(format!(
                        "bicdb_repair_row_xid expects a non-negative integer stamp, got {other:?}"
                    )));
                }
            };
            let report = db
                .repair_row_transaction_stamp(&collection, &pk, &field, expected)
                .map_err(SqlError::from)?;
            return Ok(Some(SqlValue::String(report)));
        }
        // Mesh conflict review: unresolved concurrent writes surfaced by the
        // causal resolver, as JSON. One-arg form lists a collection's
        // conflicts; two-arg form inspects a single record (NULL when none).
        // The space observability surface: physical bytes by category plus
        // the paged store's logical accounting — what autovacuum, tail
        // reclaim, and hole punching act on. You can't keep lean what you
        // can't see.
        // Memory attribution: where the resident set actually goes. The
        // engine already computed this per collection and per index; it had
        // no caller, which is why an incident could only guess. Refreshes the
        // index-size sidecar on the way through, since that number is
        // otherwise only written at index build time.
        "bicdb_memory_report" => {
            require_arg_count(name, args, 0)?;
            let _ = db.refresh_index_maintenance_sizes();
            let report = db.residency_report().map_err(SqlError::from)?;
            let mut collections: Vec<serde_json::Value> = report
                .collections
                .iter()
                .map(|collection| {
                    serde_json::json!({
                        "name": collection.name,
                        "record_count": collection.record_count,
                        "version_count": collection.version_count,
                        "rows_bytes": collection.rows_bytes,
                        "version_chains_bytes": collection.version_chains_bytes,
                        "primary_key_maps_bytes": collection.primary_key_maps_bytes,
                        "exact_vectors_bytes": collection.exact_vectors_bytes,
                    })
                })
                .collect();
            // Biggest first: an operator reads the top of this list.
            collections
                .sort_by_key(|value| std::cmp::Reverse(value["rows_bytes"].as_u64().unwrap_or(0)));
            let mut indexes: Vec<serde_json::Value> = report
                .indexes
                .iter()
                .map(|index| {
                    serde_json::json!({
                        "name": index.name,
                        "collection": index.collection,
                        "entry_count": index.entry_count,
                        "store_bytes": index.store_bytes,
                        "spatial_bytes": index.spatial_bytes,
                    })
                })
                .collect();
            indexes
                .sort_by_key(|value| std::cmp::Reverse(value["store_bytes"].as_u64().unwrap_or(0)));
            return Ok(Some(SqlValue::String(
                serde_json::json!({
                    "accounted_bytes": report.accounted_bytes,
                    "process_resident_bytes": report.process_resident_bytes,
                    "unaccounted_bytes": report.unaccounted_bytes,
                    "rows_bytes": report.rows_bytes,
                    "version_chains_bytes": report.version_chains_bytes,
                    "primary_key_maps_bytes": report.primary_key_maps_bytes,
                    "secondary_indexes_bytes": report.secondary_indexes_bytes,
                    "collections": collections,
                    "indexes": indexes,
                })
                .to_string(),
            )));
        }
        "bicdb_space_report" => {
            require_arg_count(name, args, 0)?;
            let root = db.data_path().to_path_buf();
            // The directory walk is rate-limited and cached; this function is
            // also superuser-only at the SQL dispatch boundary.
            let (total, allocated, categories) = cached_space_usage(&root);
            let paged = db
                .paged_storage_snapshot()
                .map_err(SqlError::from)?
                .map(|snapshot| {
                    serde_json::json!({
                        "page_size": snapshot.page_size,
                        "page_count": snapshot.page_count,
                        "used_data_pages": snapshot.used_data_pages,
                        "free_pages": snapshot.free_pages,
                        "free_bytes": snapshot.free_bytes,
                        "logical_page_bytes": snapshot.logical_page_bytes,
                        "page_file_bytes": snapshot.page_file_bytes,
                        "wal_bytes": snapshot.wal_bytes,
                        "wal_max_bytes": snapshot.wal_max_bytes,
                        // What vacuum + tail reclaim + hole punching can return:
                        // interior free pages plus tail slack beyond the logical
                        // page span.
                        "reclaimable_estimate_bytes": snapshot.free_bytes.saturating_add(
                            snapshot.page_file_bytes.saturating_sub(snapshot.logical_page_bytes),
                        ),
                    })
                });
            let report = serde_json::json!({
                "total_file_bytes": total,
                "allocated_file_bytes": allocated,
                "categories": categories,
                "paged": paged,
                "event_log_events": db.sync_status().total_events,
            });
            return Ok(Some(SqlValue::String(report.to_string())));
        }
        // Temporal geo: what did this record/boundary look like at time T?
        // Replays the audit stream (requires audit events on the writing
        // side). bicdb_record_asof -> JSON record or NULL;
        // bicdb_geometry_asof -> the intrinsic geometry value or NULL.
        "bicdb_record_asof" | "bicdb_geometry_asof" => {
            require_arg_count(name, args, 3)?;
            let (SqlValue::String(collection), SqlValue::String(record_id), SqlValue::Int(at)) =
                (&args[0], &args[1], &args[2])
            else {
                return Err(SqlError::InvalidSql(format!(
                    "{name} expects (collection, record_id, unix_seconds)"
                )));
            };
            let snapshot = db.snapshot_at(*at).map_err(SqlError::from)?;
            let Some(record) = snapshot.get(collection, record_id) else {
                return Ok(Some(SqlValue::Null));
            };
            if name == "bicdb_geometry_asof" {
                return Ok(Some(match &record.geometry {
                    Some(geometry) => SqlValue::Geometry(geometry.clone()),
                    None => SqlValue::Null,
                }));
            }
            return Ok(Some(SqlValue::String(
                serde_json::json!({
                    "id": record.id,
                    "metadata": record.metadata,
                    "geometry": record.geometry.as_ref().map(|g| g.to_wkt()),
                })
                .to_string(),
            )));
        }
        // Minimal raster support: grids stored as records with
        // {min_lon,min_lat,max_lon,max_lat,width,height,values[row-major,
        // row 0 = north]}. Sampling is bilinear; slope uses central
        // differences in meters; zonal mean iterates cell centers.
        "bicdb_raster_sample" | "bicdb_raster_slope" => {
            require_arg_count(name, args, 3)?;
            let SqlValue::String(table) = &args[0] else {
                return Err(SqlError::InvalidSql(format!(
                    "{name} expects a raster table name first"
                )));
            };
            let lon = sql_value_f64(&args[1])
                .ok_or_else(|| SqlError::InvalidSql(format!("{name} lon must be a number")))?;
            let lat = sql_value_f64(&args[2])
                .ok_or_else(|| SqlError::InvalidSql(format!("{name} lat must be a number")))?;
            let Some(raster) = raster_covering(db, table, lon, lat)? else {
                return Ok(Some(SqlValue::Null));
            };
            let value = if name == "bicdb_raster_sample" {
                raster.sample(lon, lat)
            } else {
                raster.slope_degrees(lon, lat)
            };
            return Ok(Some(match value {
                Some(value) => SqlValue::Float(value),
                None => SqlValue::Null,
            }));
        }
        "bicdb_raster_zonal_mean" => {
            require_arg_count(name, args, 2)?;
            let (SqlValue::String(table), SqlValue::String(zone_wkt)) = (&args[0], &args[1]) else {
                return Err(SqlError::InvalidSql(
                    "bicdb_raster_zonal_mean expects (table, polygon_wkt)".to_string(),
                ));
            };
            let zone = bicdb_core::Geometry::from_wkt(zone_wkt).map_err(SqlError::from)?;
            let polygons: Vec<geo::Polygon<f64>> = match &zone {
                bicdb_core::Geometry::Polygon(polygon) => vec![polygon.clone()],
                bicdb_core::Geometry::MultiPolygon(polygons) => polygons.0.clone(),
                _ => {
                    return Err(SqlError::InvalidSql(
                        "bicdb_raster_zonal_mean zone must be areal".to_string(),
                    ));
                }
            };
            const RASTER_ZONAL_WORK_CAP: u128 = 10_000_000;
            let zone_vertices = polygons
                .iter()
                .map(|polygon| {
                    polygon.exterior().0.len()
                        + polygon
                            .interiors()
                            .iter()
                            .map(|ring| ring.0.len())
                            .sum::<usize>()
                })
                .sum::<usize>()
                .max(1) as u128;
            let records = db.scan_collection(table).map_err(SqlError::from)?;
            let mut sum = 0.0_f64;
            let mut count = 0_u64;
            let mut estimated_work = 0_u128;
            for record in &records {
                let Ok(raster) = RasterGrid::from_record(record) else {
                    continue;
                };
                let raster_work = (raster.width as u128)
                    .saturating_mul(raster.height as u128)
                    .saturating_mul(zone_vertices);
                estimated_work = estimated_work.saturating_add(raster_work);
                if estimated_work > RASTER_ZONAL_WORK_CAP {
                    return Err(SqlError::InvalidSql(format!(
                        "bicdb_raster_zonal_mean exceeds the {RASTER_ZONAL_WORK_CAP}-operation work cap; use a smaller raster or simpler zone"
                    )));
                }
                for row in 0..raster.height {
                    for col in 0..raster.width {
                        let (lon, lat) = raster.cell_center(col, row);
                        let point = geo::Point::new(lon, lat);
                        if polygons
                            .iter()
                            .any(|polygon| geo::Contains::contains(polygon, &point))
                        {
                            sum += raster.values[row * raster.width + col];
                            count += 1;
                        }
                    }
                }
            }
            return Ok(Some(if count == 0 {
                SqlValue::Null
            } else {
                SqlValue::Float(sum / count as f64)
            }));
        }
        // The reachability headline: travel_time over a road graph.
        // bicdb_travel_time('graph', from_lon, from_lat, to_lon, to_lat, 'profile') -> seconds
        "bicdb_travel_time" | "travel_time" => {
            require_arg_count("bicdb_travel_time", args, 6)?;
            let SqlValue::String(graph) = &args[0] else {
                return Err(SqlError::InvalidSql(
                    "travel_time expects a road graph name first".to_string(),
                ));
            };
            let number = |value: &SqlValue, what: &str| -> Result<f64> {
                sql_value_f64(value).ok_or_else(|| {
                    SqlError::InvalidSql(format!("travel_time {what} must be a number"))
                })
            };
            let from = bicdb_core::Geometry::point(
                number(&args[1], "from_lon")?,
                number(&args[2], "from_lat")?,
            )
            .map_err(SqlError::from)?;
            let to = bicdb_core::Geometry::point(
                number(&args[3], "to_lon")?,
                number(&args[4], "to_lat")?,
            )
            .map_err(SqlError::from)?;
            let SqlValue::String(profile) = &args[5] else {
                return Err(SqlError::InvalidSql(
                    "travel_time expects a profile name last".to_string(),
                ));
            };
            let profile: bicdb_core::RouteProfile = profile.parse().map_err(SqlError::from)?;
            let seconds = db
                .travel_time_seconds(graph, &from, &to, profile)
                .map_err(SqlError::from)?;
            return Ok(Some(SqlValue::Float(seconds)));
        }
        // bicdb_travel_matrix('graph', '[[lon,lat],...]', '[[lon,lat],...]', 'profile')
        // -> JSON matrix of seconds (null = unreachable).
        "bicdb_travel_matrix" => {
            require_arg_count(name, args, 4)?;
            let (
                SqlValue::String(graph),
                SqlValue::String(origins),
                SqlValue::String(dests),
                SqlValue::String(profile),
            ) = (&args[0], &args[1], &args[2], &args[3])
            else {
                return Err(SqlError::InvalidSql(
                    "bicdb_travel_matrix expects (graph, origins_json, destinations_json, profile)"
                        .to_string(),
                ));
            };
            let parse_points = |text: &str, what: &str| -> Result<Vec<bicdb_core::Geometry>> {
                let raw: Vec<[f64; 2]> = serde_json::from_str(text).map_err(|error| {
                    SqlError::InvalidSql(format!(
                        "bicdb_travel_matrix {what} must be [[lon,lat],...]: {error}"
                    ))
                })?;
                if raw.is_empty() || raw.len() > 1_000 {
                    return Err(SqlError::InvalidSql(format!(
                        "bicdb_travel_matrix {what} must contain 1-1000 points"
                    )));
                }
                raw.into_iter()
                    .map(|[lon, lat]| bicdb_core::Geometry::point(lon, lat).map_err(SqlError::from))
                    .collect()
            };
            let origins = parse_points(origins, "origins")?;
            let dests = parse_points(dests, "destinations")?;
            let profile: bicdb_core::RouteProfile = profile.parse().map_err(SqlError::from)?;
            let matrix = db
                .travel_time_matrix(graph, &origins, &dests, profile)
                .map_err(SqlError::from)?;
            return Ok(Some(SqlValue::String(
                serde_json::to_string(&matrix).map_err(|error| {
                    SqlError::InvalidSql(format!("matrix serialization failed: {error}"))
                })?,
            )));
        }
        // bicdb_isochrone('graph', lon, lat, seconds, 'profile') -> POLYGON
        "bicdb_isochrone" => {
            require_arg_count(name, args, 5)?;
            let SqlValue::String(graph) = &args[0] else {
                return Err(SqlError::InvalidSql(
                    "bicdb_isochrone expects a road graph name first".to_string(),
                ));
            };
            let number = |value: &SqlValue, what: &str| -> Result<f64> {
                sql_value_f64(value).ok_or_else(|| {
                    SqlError::InvalidSql(format!("bicdb_isochrone {what} must be a number"))
                })
            };
            let origin =
                bicdb_core::Geometry::point(number(&args[1], "lon")?, number(&args[2], "lat")?)
                    .map_err(SqlError::from)?;
            let seconds = number(&args[3], "seconds")?;
            let SqlValue::String(profile) = &args[4] else {
                return Err(SqlError::InvalidSql(
                    "bicdb_isochrone expects a profile name last".to_string(),
                ));
            };
            let profile: bicdb_core::RouteProfile = profile.parse().map_err(SqlError::from)?;
            let polygon = db
                .isochrone(graph, &origin, seconds, profile)
                .map_err(SqlError::from)?;
            return Ok(Some(SqlValue::Geometry(polygon)));
        }
        // Forward geocode: FTS-ranked search over a names table.
        // bicdb_geocode('fts_index_name', 'query', limit) -> JSON results.
        "bicdb_geocode" => {
            require_arg_count(name, args, 3)?;
            let (SqlValue::String(index_name), SqlValue::String(query), SqlValue::Int(limit)) =
                (&args[0], &args[1], &args[2])
            else {
                return Err(SqlError::InvalidSql(
                    "bicdb_geocode expects (index_name, query, limit)".to_string(),
                ));
            };
            let limit = usize::try_from(*limit)
                .ok()
                .filter(|limit| (1..=1_000).contains(limit))
                .ok_or_else(|| {
                    SqlError::InvalidSql("bicdb_geocode limit must be 1-1000".to_string())
                })?;
            let terms = crate::fts::fts_index_terms(&SqlValue::String(query.clone()))?;
            if terms.is_empty() {
                return Ok(Some(SqlValue::String("[]".to_string())));
            }
            let term_refs: Vec<&str> = terms.iter().map(String::as_str).collect();
            let collection = db
                .index_definitions()
                .into_iter()
                .find(|definition| definition.name == *index_name)
                .ok_or_else(|| {
                    SqlError::InvalidSql(format!("bicdb_geocode: index `{index_name}` not found"))
                })?
                .collection;
            let hits = db
                .full_text_bm25_top_k(index_name, &term_refs, Default::default(), limit, false)
                .map_err(SqlError::from)?
                .unwrap_or_default();
            let mut results = Vec::new();
            for hit in hits {
                let Some(record) = db
                    .get(&collection, &hit.primary_key)
                    .map_err(SqlError::from)?
                else {
                    continue;
                };
                results.push(serde_json::json!({
                    "id": record.id,
                    "score": hit.score,
                    "metadata": record.metadata,
                }));
            }
            return Ok(Some(SqlValue::String(
                serde_json::Value::Array(results).to_string(),
            )));
        }
        // Reverse geocode: nearest named features by geodesic distance.
        // bicdb_reverse_geocode('collection', 'geom_field', lon, lat, limit)
        "bicdb_reverse_geocode" => {
            require_arg_count(name, args, 5)?;
            let (SqlValue::String(collection), SqlValue::String(field)) = (&args[0], &args[1])
            else {
                return Err(SqlError::InvalidSql(
                    "bicdb_reverse_geocode expects (collection, geom_field, lon, lat, limit)"
                        .to_string(),
                ));
            };
            let lon = sql_value_f64(&args[2]).ok_or_else(|| {
                SqlError::InvalidSql("bicdb_reverse_geocode lon must be a number".to_string())
            })?;
            let lat = sql_value_f64(&args[3]).ok_or_else(|| {
                SqlError::InvalidSql("bicdb_reverse_geocode lat must be a number".to_string())
            })?;
            let SqlValue::Int(limit) = args[4] else {
                return Err(SqlError::InvalidSql(
                    "bicdb_reverse_geocode limit must be an integer".to_string(),
                ));
            };
            let limit = usize::try_from(limit)
                .ok()
                .filter(|limit| (1..=1_000).contains(limit))
                .ok_or_else(|| {
                    SqlError::InvalidSql("bicdb_reverse_geocode limit must be 1-1000".to_string())
                })?;
            let hits = db
                .nearest(collection, field, lon, lat, limit)
                .map_err(SqlError::from)?;
            let results: Vec<serde_json::Value> = hits
                .into_iter()
                .map(|hit| {
                    serde_json::json!({
                        "id": hit.record.id,
                        "distance_meters": hit.distance_meters,
                        "metadata": hit.record.metadata,
                    })
                })
                .collect();
            return Ok(Some(SqlValue::String(
                serde_json::Value::Array(results).to_string(),
            )));
        }
        // Administrative containment hierarchy:
        // bicdb_admin_hierarchy('collection', 'geom_field', 'level_field', lon, lat)
        // -> JSON array of containing boundaries ordered by level (country
        // first, neighborhood last).
        "bicdb_admin_hierarchy" => {
            require_arg_count(name, args, 5)?;
            let (
                SqlValue::String(collection),
                SqlValue::String(geom_field),
                SqlValue::String(level_field),
            ) = (&args[0], &args[1], &args[2])
            else {
                return Err(SqlError::InvalidSql(
                    "bicdb_admin_hierarchy expects (collection, geom_field, level_field, lon, lat)"
                        .to_string(),
                ));
            };
            let lon = sql_value_f64(&args[3]).ok_or_else(|| {
                SqlError::InvalidSql("bicdb_admin_hierarchy lon must be a number".to_string())
            })?;
            let lat = sql_value_f64(&args[4]).ok_or_else(|| {
                SqlError::InvalidSql("bicdb_admin_hierarchy lat must be a number".to_string())
            })?;
            let point = geo::Point::new(lon, lat);
            let records = db.scan_collection(collection).map_err(SqlError::from)?;
            let mut containing = Vec::new();
            for record in records {
                let Some(geom_value) = record.metadata.get(geom_field) else {
                    continue;
                };
                let geometry = match geom_value {
                    serde_json::Value::String(text) => {
                        let trimmed = text.trim_start();
                        if trimmed.starts_with('{') {
                            bicdb_core::Geometry::from_geojson_str(text)
                        } else {
                            bicdb_core::Geometry::from_wkt(text)
                        }
                    }
                    other @ serde_json::Value::Object(_) => {
                        bicdb_core::Geometry::from_geojson_value(other.clone())
                    }
                    _ => continue,
                };
                let Ok(geometry) = geometry else { continue };
                let contains = match &geometry {
                    bicdb_core::Geometry::Polygon(polygon) => {
                        geo::Contains::contains(polygon, &point)
                    }
                    bicdb_core::Geometry::MultiPolygon(polygons) => {
                        geo::Contains::contains(polygons, &point)
                    }
                    bicdb_core::Geometry::Envelope(rect) => {
                        geo::Contains::contains(&rect.to_polygon(), &point)
                    }
                    _ => false,
                };
                if !contains {
                    continue;
                }
                let level = record
                    .metadata
                    .get(level_field)
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(i64::MAX);
                containing.push((level, record));
            }
            containing.sort_by_key(|(level, record)| (*level, record.id.clone()));
            let results: Vec<serde_json::Value> = containing
                .into_iter()
                .map(|(level, record)| {
                    serde_json::json!({
                        "level": if level == i64::MAX { serde_json::Value::Null } else { serde_json::json!(level) },
                        "id": record.id,
                        "metadata": record.metadata,
                    })
                })
                .collect();
            return Ok(Some(SqlValue::String(
                serde_json::Value::Array(results).to_string(),
            )));
        }
        // MVT tile from a table: bicdb_tile_mvt('table','geom_col',z,x,y,'layer')
        // -> hex-encoded Mapbox Vector Tile. Non-geometry columns become
        // feature tags. Buffered clipping + integer-snap generalization.
        "bicdb_tile_mvt" => {
            require_arg_count(name, args, 6)?;
            let text = |value: &SqlValue, what: &str| -> Result<String> {
                match value {
                    SqlValue::String(text) => Ok(text.clone()),
                    other => Err(SqlError::InvalidSql(format!(
                        "bicdb_tile_mvt expects text {what}, got {other:?}"
                    ))),
                }
            };
            let integer = |value: &SqlValue, what: &str| -> Result<u32> {
                match value {
                    SqlValue::Int(number) if *number >= 0 => u32::try_from(*number).map_err(|_| {
                        SqlError::InvalidSql(format!("bicdb_tile_mvt {what} out of range"))
                    }),
                    other => Err(SqlError::InvalidSql(format!(
                        "bicdb_tile_mvt expects a non-negative integer {what}, got {other:?}"
                    ))),
                }
            };
            let table = text(&args[0], "table name")?;
            let geom_column = text(&args[1], "geometry column")?;
            let z = integer(&args[2], "zoom")?;
            if z > 24 {
                return Err(SqlError::InvalidSql(
                    "bicdb_tile_mvt zoom is capped at 24".to_string(),
                ));
            }
            let x = integer(&args[3], "x")?;
            let y = integer(&args[4], "y")?;
            let layer = text(&args[5], "layer name")?;
            let records = db.scan_collection(&table).map_err(SqlError::from)?;
            let mut features = Vec::new();
            for (index, record) in records.iter().enumerate() {
                let Some(geom_value) = record.metadata.get(&geom_column) else {
                    continue;
                };
                let geometry = match geom_value {
                    serde_json::Value::String(text) => {
                        let trimmed = text.trim_start();
                        if trimmed.starts_with('{') {
                            bicdb_core::Geometry::from_geojson_str(text)
                        } else {
                            bicdb_core::Geometry::from_wkt(text)
                        }
                    }
                    other @ serde_json::Value::Object(_) => {
                        bicdb_core::Geometry::from_geojson_value(other.clone())
                    }
                    _ => continue,
                };
                let Ok(geometry) = geometry else { continue };
                let mut tags = vec![(
                    "id".to_string(),
                    crate::mvt::TileValue::Text(record.id.clone()),
                )];
                if let serde_json::Value::Object(fields) = &record.metadata {
                    for (key, value) in fields {
                        if key == &geom_column {
                            continue;
                        }
                        let tag_value = match value {
                            serde_json::Value::String(text) => {
                                crate::mvt::TileValue::Text(text.clone())
                            }
                            serde_json::Value::Bool(flag) => crate::mvt::TileValue::Bool(*flag),
                            serde_json::Value::Number(number) => {
                                if let Some(int) = number.as_i64() {
                                    crate::mvt::TileValue::Int(int)
                                } else if let Some(float) = number.as_f64() {
                                    crate::mvt::TileValue::Float(float)
                                } else {
                                    continue;
                                }
                            }
                            _ => continue,
                        };
                        tags.push((key.clone(), tag_value));
                    }
                }
                if let Some(feature) =
                    crate::mvt::tile_feature(index as u64 + 1, &geometry, z, x, y, tags)
                {
                    features.push(feature);
                }
            }
            let tile = crate::mvt::encode_tile(&layer, &features);
            return Ok(Some(SqlValue::String(format!("\\x{}", hex::encode(tile)))));
        }
        "bicdb_record_conflicts" => {
            require_arg_count(name, args, 1)?;
            let SqlValue::String(collection) = &args[0] else {
                if matches!(args[0], SqlValue::Null) {
                    return Ok(Some(SqlValue::Null));
                }
                return Err(SqlError::InvalidSql(format!(
                    "bicdb_record_conflicts expects a collection name, got {:?}",
                    args[0]
                )));
            };
            let conflicts = db
                .list_record_conflicts(collection)
                .map_err(SqlError::from)?;
            let json = serde_json::to_string(&conflicts).map_err(|error| {
                SqlError::InvalidSql(format!("conflict serialization failed: {error}"))
            })?;
            return Ok(Some(SqlValue::String(json)));
        }
        "bicdb_record_conflict" => {
            require_arg_count(name, args, 2)?;
            if args.iter().any(|arg| matches!(arg, SqlValue::Null)) {
                return Ok(Some(SqlValue::Null));
            }
            let (SqlValue::String(collection), SqlValue::String(record_id)) = (&args[0], &args[1])
            else {
                return Err(SqlError::InvalidSql(format!(
                    "bicdb_record_conflict expects (collection, record_id) text arguments, got {args:?}"
                )));
            };
            let conflict = db
                .record_conflict(collection, record_id)
                .map_err(SqlError::from)?;
            return Ok(Some(match conflict {
                Some(conflict) => {
                    SqlValue::String(serde_json::to_string(&conflict).map_err(|error| {
                        SqlError::InvalidSql(format!("conflict serialization failed: {error}"))
                    })?)
                }
                None => SqlValue::Null,
            }));
        }
        "bicdb_advance_transaction_floor" => {
            require_arg_count(name, args, 1)?;
            if matches!(args[0], SqlValue::Null) {
                return Ok(Some(SqlValue::Null));
            }
            let beyond = match &args[0] {
                SqlValue::Int(value) if *value >= 0 => *value as u64,
                other => {
                    return Err(SqlError::InvalidSql(format!(
                        "bicdb_advance_transaction_floor expects a non-negative integer, got {other:?}"
                    )));
                }
            };
            let Some((frozen, next)) = db
                .advance_transaction_floor(beyond)
                .map_err(SqlError::from)?
            else {
                return Err(SqlError::Unsupported(
                    "bicdb_advance_transaction_floor requires a paged store".to_string(),
                ));
            };
            return Ok(Some(SqlValue::String(format!(
                "transaction floor advanced: frozen_xid={frozen} next_xid={next} (checkpointed)"
            ))));
        }
        "pg_current_snapshot"
        | "pg_catalog.pg_current_snapshot"
        | "txid_current_snapshot"
        | "pg_catalog.txid_current_snapshot" => {
            require_arg_count(name, args, 0)?;
            let watermark = snapshot_watermark.unwrap_or_else(|| db.current_visibility_watermark());
            let boundary = watermark.saturating_add(1).max(1);
            Ok(Some(SqlValue::String(
                PgSnapshot::new(boundary, boundary, Vec::new())
                    .expect("a visibility watermark always forms a valid snapshot")
                    .to_postgres_text(),
            )))
        }
        "pg_snapshot_xmin"
        | "pg_catalog.pg_snapshot_xmin"
        | "pg_snapshot_xmax"
        | "pg_catalog.pg_snapshot_xmax"
        | "txid_snapshot_xmin"
        | "pg_catalog.txid_snapshot_xmin"
        | "txid_snapshot_xmax"
        | "pg_catalog.txid_snapshot_xmax" => {
            require_arg_count(name, args, 1)?;
            let Some(snapshot) = pg_snapshot_argument(&args[0], name)? else {
                return Ok(Some(SqlValue::Null));
            };
            let value = if name.contains("xmin") {
                snapshot.xmin
            } else {
                snapshot.xmax
            };
            Ok(Some(snapshot_xid_result(name, value)?))
        }
        "pg_visible_in_snapshot"
        | "pg_catalog.pg_visible_in_snapshot"
        | "txid_visible_in_snapshot"
        | "pg_catalog.txid_visible_in_snapshot" => {
            require_arg_count(name, args, 2)?;
            if args.iter().any(|value| matches!(value, SqlValue::Null)) {
                return Ok(Some(SqlValue::Null));
            }
            let xid = snapshot_xid_argument(name, &args[0])?;
            let snapshot =
                pg_snapshot_argument(&args[1], name)?.expect("NULL snapshot was handled above");
            let visible = xid < snapshot.xmin
                || (xid < snapshot.xmax && snapshot.in_progress.binary_search(&xid).is_err());
            Ok(Some(SqlValue::Bool(visible)))
        }
        "pg_current_wal_lsn" | "pg_catalog.pg_current_wal_lsn" => {
            require_arg_count("pg_current_wal_lsn", args, 0)?;
            if db.is_replication_standby() {
                return Err(SqlError::object_not_in_prerequisite_state(
                    "recovery is in progress",
                ));
            }
            Ok(Some(SqlValue::String(format_pg_lsn(
                db.current_wal_write_lsn(),
            ))))
        }
        "pg_current_wal_insert_lsn" | "pg_catalog.pg_current_wal_insert_lsn" => {
            require_arg_count("pg_current_wal_insert_lsn", args, 0)?;
            if db.is_replication_standby() {
                return Err(SqlError::object_not_in_prerequisite_state(
                    "recovery is in progress",
                ));
            }
            Ok(Some(SqlValue::String(format_pg_lsn(
                db.current_wal_insert_lsn(),
            ))))
        }
        "pg_current_wal_flush_lsn" | "pg_catalog.pg_current_wal_flush_lsn" => {
            require_arg_count("pg_current_wal_flush_lsn", args, 0)?;
            if db.is_replication_standby() {
                return Err(SqlError::object_not_in_prerequisite_state(
                    "recovery is in progress",
                ));
            }
            Ok(Some(SqlValue::String(format_pg_lsn(
                db.current_wal_flush_lsn(),
            ))))
        }
        "pg_last_wal_receive_lsn"
        | "pg_catalog.pg_last_wal_receive_lsn"
        | "pg_last_wal_replay_lsn"
        | "pg_catalog.pg_last_wal_replay_lsn" => {
            require_arg_count(name, args, 0)?;
            Ok(Some(if db.is_replication_standby() {
                SqlValue::String(format_pg_lsn(db.last_applied_commit_seq()))
            } else {
                SqlValue::Null
            }))
        }
        "pg_wal_lsn_diff" | "pg_catalog.pg_wal_lsn_diff" => {
            require_arg_count("pg_wal_lsn_diff", args, 2)?;
            if args.iter().any(|value| matches!(value, SqlValue::Null)) {
                return Ok(Some(SqlValue::Null));
            }
            let left = pg_lsn_argument(&args[0])?;
            let right = pg_lsn_argument(&args[1])?;
            Ok(Some(SqlValue::String(
                (BigInt::from(left) - BigInt::from(right)).to_string(),
            )))
        }
        "format_type" | "pg_catalog.format_type" => {
            require_arg_count("format_type", args, 2)?;
            if matches!(args.first(), Some(SqlValue::Null) | None) {
                return Ok(Some(SqlValue::Null));
            }
            let oid = args.first().and_then(sql_value_i64).unwrap_or_default();
            let typmod = args.get(1).and_then(sql_value_i64).unwrap_or(-1) as i32;
            if oid == 0 {
                return Ok(Some(SqlValue::String("-".to_string())));
            }
            let formatted = i32::try_from(oid)
                .ok()
                .and_then(|oid| pg_format_type(oid, typmod));
            let formatted = match formatted {
                Some(formatted) => formatted,
                None => list_user_types(db)?
                    .into_iter()
                    .find_map(|user_type| {
                        if user_type.oid == oid {
                            Some(user_type.column_type(false).formatted_name())
                        } else if user_type.array_oid == oid {
                            Some(user_type.column_type(true).formatted_name())
                        } else {
                            None
                        }
                    })
                    .unwrap_or_else(|| "???".to_string()),
            };
            Ok(Some(SqlValue::String(formatted)))
        }
        "to_regtype" | "pg_catalog.to_regtype" => {
            require_arg_count("to_regtype", args, 1)?;
            if matches!(args.first(), Some(SqlValue::Null) | None) {
                return Ok(Some(SqlValue::Null));
            }
            let value = args[0].to_cell();
            let Some(oid) = resolve_regtype_oid(db, &value)? else {
                return Ok(Some(SqlValue::Null));
            };
            let formatted = eval_db_catalog_function_value(
                db,
                "format_type",
                &[SqlValue::Int(i64::from(oid)), SqlValue::Int(-1)],
                snapshot_watermark,
                session_gucs,
            )?
            .unwrap_or(SqlValue::Null);
            Ok(Some(formatted))
        }
        "pg_get_constraintdef" | "pg_catalog.pg_get_constraintdef" => {
            if !(1..=2).contains(&args.len()) {
                return Err(SqlError::InvalidSql(format!(
                    "pg_get_constraintdef expects one or two arguments, got {}",
                    args.len()
                )));
            }
            let definition = args
                .first()
                .and_then(sql_value_i64)
                .map(|oid| pg_constraint_definition_for_oid(db, oid))
                .transpose()?
                .flatten()
                .map(SqlValue::String)
                .unwrap_or(SqlValue::Null);
            Ok(Some(definition))
        }
        "pg_get_indexdef" | "pg_catalog.pg_get_indexdef" => {
            let definition = args
                .first()
                .and_then(sql_value_i64)
                .map(|indexrelid| pg_indexdef_for_oid(db, indexrelid))
                .transpose()?
                .flatten()
                .map(SqlValue::String)
                .unwrap_or(SqlValue::Null);
            Ok(Some(definition))
        }
        "to_regproc"
        | "pg_catalog.to_regproc"
        | "to_regprocedure"
        | "pg_catalog.to_regprocedure" => {
            require_arg_count(name, args, 1)?;
            let Some(value) = sql_value_text(&args[0]) else {
                return Ok(Some(SqlValue::Null));
            };
            let kind = if name.ends_with("procedure") {
                "regprocedure"
            } else {
                "regproc"
            };
            Ok(Some(
                resolve_regprocedure_oid(db, kind, &value)?
                    .map(|oid| SqlValue::Int(i64::from(oid)))
                    .unwrap_or(SqlValue::Null),
            ))
        }
        "pg_get_functiondef" | "pg_catalog.pg_get_functiondef" => {
            require_arg_count(name, args, 1)?;
            let Some(oid) = args.first().and_then(sql_value_i64) else {
                return Ok(Some(SqlValue::Null));
            };
            let definition = routine_for_oid(db, oid)?.map(|routine| {
                let sql = routine.definition.trim();
                if let Some(rest) = strip_prefix_ci(sql, "CREATE FUNCTION ") {
                    format!("CREATE OR REPLACE FUNCTION {rest}")
                } else if let Some(rest) = strip_prefix_ci(sql, "CREATE PROCEDURE ") {
                    format!("CREATE OR REPLACE PROCEDURE {rest}")
                } else {
                    sql.to_owned()
                }
            });
            Ok(Some(
                definition.map(SqlValue::String).unwrap_or(SqlValue::Null),
            ))
        }
        "to_regclass" | "pg_catalog.to_regclass" => {
            let Some(SqlValue::String(name)) = args.first() else {
                return Ok(Some(SqlValue::Null));
            };
            Ok(Some(
                resolve_regclass_oid(db, name)
                    .map(SqlValue::Int)
                    .unwrap_or(SqlValue::Null),
            ))
        }
        "pg_get_serial_sequence" | "pg_catalog.pg_get_serial_sequence" => {
            Ok(Some(pg_get_serial_sequence(db, args)?))
        }
        "pg_get_partkeydef" | "pg_catalog.pg_get_partkeydef" => {
            Ok(Some(pg_get_partkeydef(db, args)?))
        }
        "pg_get_viewdef" | "pg_catalog.pg_get_viewdef" => Ok(Some(pg_get_viewdef(db, args)?)),
        "pg_get_function_arguments" | "pg_catalog.pg_get_function_arguments" => {
            Ok(Some(pg_get_function_arguments(db, args, true)?))
        }
        "pg_get_function_identity_arguments" | "pg_catalog.pg_get_function_identity_arguments" => {
            Ok(Some(pg_get_function_arguments(db, args, false)?))
        }
        "pg_get_function_result" | "pg_catalog.pg_get_function_result" => {
            Ok(Some(pg_get_function_result(db, args)?))
        }
        "pg_get_function_sqlbody" | "pg_catalog.pg_get_function_sqlbody" => {
            require_arg_count("pg_get_function_sqlbody", args, 1)?;
            Ok(Some(SqlValue::Null))
        }
        _ => Ok(None),
    }
}

pub(crate) fn pg_snapshot_argument(value: &SqlValue, function: &str) -> Result<Option<PgSnapshot>> {
    if matches!(value, SqlValue::Null) {
        return Ok(None);
    }
    let pg_type = if function.contains("txid_") {
        "txid_snapshot"
    } else {
        "pg_snapshot"
    };
    let text = value.to_cell();
    match parse_pg_canonical_special(pg_type, &text) {
        Ok(Some(PgCanonicalValue::Snapshot(snapshot))) => Ok(Some(snapshot)),
        _ => Err(SqlError::invalid_text_representation(
            pg_type,
            format!("invalid input syntax for type {pg_type}: \"{text}\""),
        )),
    }
}

pub(crate) fn snapshot_xid_argument(function: &str, value: &SqlValue) -> Result<u64> {
    let text = value.to_cell();
    if function.contains("txid_") {
        return text
            .parse::<u64>()
            .map_err(|_| SqlError::numeric_value_out_of_range("transaction ID is out of range"));
    }
    parse_pg_xid8(&text).map_err(|_| {
        SqlError::invalid_text_representation(
            "xid8",
            format!("invalid input syntax for type xid8: \"{text}\""),
        )
    })
}

pub(crate) fn snapshot_xid_result(function: &str, value: u64) -> Result<SqlValue> {
    if function.contains("txid_") {
        return i64::try_from(value)
            .map(SqlValue::Int)
            .map_err(|_| SqlError::numeric_value_out_of_range("bigint out of range"));
    }
    Ok(SqlValue::String(value.to_string()))
}

pub(crate) fn pg_snapshot_xip_values(function: &str, value: &SqlValue) -> Result<Vec<SqlValue>> {
    let Some(snapshot) = pg_snapshot_argument(value, function)? else {
        return Ok(Vec::new());
    };
    snapshot
        .in_progress
        .into_iter()
        .map(|xid| snapshot_xid_result(function, xid))
        .collect()
}

pub(crate) fn eval_pg_proc_row_function_value(
    db: &BicDb,
    row: &SqlRow,
    name: &str,
    args: &[SqlValue],
) -> Result<Option<SqlValue>> {
    match name {
        "pg_get_function_arguments" | "pg_catalog.pg_get_function_arguments" => Ok(Some(
            pg_get_function_arguments_from_pg_proc_row(db, row, args, true)?,
        )),
        "pg_get_function_identity_arguments" | "pg_catalog.pg_get_function_identity_arguments" => {
            Ok(Some(pg_get_function_arguments_from_pg_proc_row(
                db, row, args, false,
            )?))
        }
        "pg_get_function_result" | "pg_catalog.pg_get_function_result" => Ok(Some(
            pg_get_function_result_from_pg_proc_row(db, row, args)?,
        )),
        "pg_get_function_sqlbody" | "pg_catalog.pg_get_function_sqlbody" => {
            require_arg_count("pg_get_function_sqlbody", args, 1)?;
            Ok(Some(SqlValue::Null))
        }
        _ => Ok(None),
    }
}

pub(crate) fn pg_get_function_arguments_from_pg_proc_row(
    db: &BicDb,
    row: &SqlRow,
    args: &[SqlValue],
    include_defaults: bool,
) -> Result<SqlValue> {
    require_arg_count("pg_get_function_arguments", args, 1)?;
    if args.first().and_then(sql_value_i64).is_none() {
        return Ok(SqlValue::Null);
    }
    if let Some(routine) = routine_from_pg_proc_row(db, row)? {
        return Ok(SqlValue::String(routine_argument_signature(
            &routine.args,
            include_defaults,
        )));
    }
    if pg_proc_row_text(row, "proname").is_some() {
        return Ok(SqlValue::String(String::new()));
    }
    Ok(SqlValue::Null)
}

pub(crate) fn pg_get_function_result_from_pg_proc_row(
    db: &BicDb,
    row: &SqlRow,
    args: &[SqlValue],
) -> Result<SqlValue> {
    require_arg_count("pg_get_function_result", args, 1)?;
    if args.first().and_then(sql_value_i64).is_none() {
        return Ok(SqlValue::Null);
    }
    if let Some(routine) = routine_from_pg_proc_row(db, row)? {
        let mut rendered = routine.formatted_return_type();
        if routine.returns_set {
            rendered = format!("SETOF {rendered}");
        }
        return Ok(SqlValue::String(rendered));
    }
    let Some(return_type_oid) =
        pg_proc_row_value(row, "prorettype").and_then(|value| sql_value_i64(&value))
    else {
        return Ok(SqlValue::Null);
    };
    let mut rendered = pg_type_name_from_oid(return_type_oid)
        .unwrap_or("unknown")
        .to_string();
    if pg_proc_row_value(row, "proretset")
        .and_then(|value| sql_value_bool(&value))
        .unwrap_or(false)
    {
        rendered = format!("SETOF {rendered}");
    }
    Ok(SqlValue::String(rendered))
}

pub(crate) fn routine_from_pg_proc_row(db: &BicDb, row: &SqlRow) -> Result<Option<RoutineSchema>> {
    let Some(name) = pg_proc_row_text(row, "proname") else {
        return Ok(None);
    };
    let kind = match pg_proc_row_text(row, "prokind").as_deref() {
        Some("p") => RoutineKind::Procedure,
        _ => RoutineKind::Function,
    };
    load_routine(db, kind, &name)
}

pub(crate) fn pg_proc_row_text(row: &SqlRow, field: &str) -> Option<String> {
    pg_proc_row_value(row, field).and_then(|value| sql_value_text(&value))
}

pub(crate) fn pg_proc_row_value(row: &SqlRow, field: &str) -> Option<SqlValue> {
    let parts = vec!["pg_proc".to_string(), field.to_string()];
    let value = row_value_from_parts(row, &parts);
    if !matches!(value, SqlValue::Null) {
        return Some(value);
    }
    let parts = vec!["p".to_string(), field.to_string()];
    let value = row_value_from_parts(row, &parts);
    if !matches!(value, SqlValue::Null) {
        return Some(value);
    }
    let parts = vec![field.to_string()];
    let value = row_value_from_parts(row, &parts);
    (!matches!(value, SqlValue::Null)).then_some(value)
}

pub(crate) fn pg_get_partkeydef(db: &BicDb, args: &[SqlValue]) -> Result<SqlValue> {
    require_arg_count("pg_get_partkeydef", args, 1)?;
    let Some(oid) = args.first().and_then(sql_value_i64) else {
        return Ok(SqlValue::Null);
    };
    let Some(relation) = catalog_relation_name_for_table_oid(db, oid)? else {
        return Ok(SqlValue::Null);
    };
    let Some(schema) = load_schema(db, &relation)? else {
        return Ok(SqlValue::Null);
    };
    let Some(partitioning) = schema.partitioning.as_ref() else {
        return Ok(SqlValue::Null);
    };
    let keys = partitioning
        .key_columns
        .iter()
        .map(|column| pg_partition_key_identifier(column))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(SqlValue::String(format!(
        "{} ({keys})",
        partitioning.strategy.to_ascii_uppercase()
    )))
}

pub(crate) fn pg_get_viewdef(db: &BicDb, args: &[SqlValue]) -> Result<SqlValue> {
    if args.is_empty() || args.len() > 2 {
        return Err(SqlError::InvalidSql(format!(
            "pg_get_viewdef expects 1 or 2 arguments, got {}",
            args.len()
        )));
    }
    let Some(oid) = args.first().and_then(sql_value_i64) else {
        return Ok(SqlValue::Null);
    };
    let Some(relation) = catalog_relation_name_for_table_oid(db, oid)? else {
        return Ok(SqlValue::Null);
    };
    if let Some(view) = load_view(db, &relation)? {
        return Ok(SqlValue::String(postgres_view_definition(&view.query_sql)));
    }
    for table in graph_virtual_table_names() {
        if table.eq_ignore_ascii_case(&relation) {
            return Ok(SqlValue::String(postgres_view_definition(
                &graph_virtual_view_definition(table),
            )));
        }
    }
    Ok(SqlValue::Null)
}

pub(crate) fn postgres_view_definition(definition: &str) -> String {
    let definition = definition.trim_end();
    if definition.ends_with(';') {
        definition.to_string()
    } else {
        format!("{definition};")
    }
}

pub(crate) fn catalog_relation_name_for_table_oid(db: &BicDb, oid: i64) -> Result<Option<String>> {
    Ok((*table_oids(db))
        .clone()
        .into_iter()
        .find_map(|(relation, relation_oid)| (relation_oid == oid).then_some(relation)))
}

pub(crate) fn graph_virtual_view_definition(table: &str) -> String {
    let columns = graph_virtual_table_columns(table)
        .into_iter()
        .map(|column| {
            format!(
                "NULL::{} AS {}",
                pg_function_result_type(&column.pg_type),
                pg_partition_key_identifier(&column.name)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT {columns} WHERE false")
}

pub(crate) fn pg_partition_key_identifier(column: &str) -> String {
    if is_unquoted_pg_identifier(column) {
        column.to_string()
    } else {
        format!("\"{}\"", column.replace('"', "\"\""))
    }
}

pub(crate) fn is_unquoted_pg_identifier(value: &str) -> bool {
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    matches!(first, b'a'..=b'z' | b'_')
        && bytes.all(|byte| matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'_' | b'$'))
        && !is_pg_reserved_identifier(value)
}

pub(crate) fn is_pg_reserved_identifier(value: &str) -> bool {
    matches!(
        value,
        "all"
            | "analyse"
            | "analyze"
            | "and"
            | "any"
            | "array"
            | "as"
            | "asc"
            | "asymmetric"
            | "both"
            | "case"
            | "cast"
            | "check"
            | "collate"
            | "column"
            | "constraint"
            | "create"
            | "current_catalog"
            | "current_date"
            | "current_role"
            | "current_time"
            | "current_timestamp"
            | "current_user"
            | "default"
            | "deferrable"
            | "desc"
            | "distinct"
            | "do"
            | "else"
            | "end"
            | "except"
            | "false"
            | "fetch"
            | "for"
            | "foreign"
            | "from"
            | "grant"
            | "group"
            | "having"
            | "in"
            | "initially"
            | "intersect"
            | "into"
            | "lateral"
            | "leading"
            | "limit"
            | "localtime"
            | "localtimestamp"
            | "not"
            | "null"
            | "offset"
            | "on"
            | "only"
            | "or"
            | "order"
            | "placing"
            | "primary"
            | "references"
            | "returning"
            | "select"
            | "session_user"
            | "some"
            | "symmetric"
            | "table"
            | "then"
            | "to"
            | "trailing"
            | "true"
            | "union"
            | "unique"
            | "user"
            | "using"
            | "variadic"
            | "when"
            | "where"
            | "window"
            | "with"
    )
}

pub(crate) fn pg_get_function_arguments(
    db: &BicDb,
    args: &[SqlValue],
    include_defaults: bool,
) -> Result<SqlValue> {
    require_arg_count("pg_get_function_arguments", args, 1)?;
    let Some(oid) = args.first().and_then(sql_value_i64) else {
        return Ok(SqlValue::Null);
    };
    if let Some(routine) = routine_for_oid(db, oid)? {
        return Ok(SqlValue::String(routine_argument_signature(
            &routine.args,
            include_defaults,
        )));
    }
    if pg_builtin_proc_return_type_oid(oid).is_some() {
        return Ok(SqlValue::String(String::new()));
    }
    Ok(SqlValue::Null)
}

pub(crate) fn pg_get_function_result(db: &BicDb, args: &[SqlValue]) -> Result<SqlValue> {
    require_arg_count("pg_get_function_result", args, 1)?;
    let Some(oid) = args.first().and_then(sql_value_i64) else {
        return Ok(SqlValue::Null);
    };
    if let Some(routine) = routine_for_oid(db, oid)? {
        return Ok(SqlValue::String(routine.formatted_return_type()));
    }
    if let Some(return_type_oid) = pg_builtin_proc_return_type_oid(oid) {
        return Ok(SqlValue::String(
            pg_type_name_from_oid(return_type_oid)
                .unwrap_or("unknown")
                .to_string(),
        ));
    }
    Ok(SqlValue::Null)
}

pub(crate) fn routine_for_oid(db: &BicDb, oid: i64) -> Result<Option<RoutineSchema>> {
    Ok(list_routines(db)?
        .into_iter()
        .find(|routine| routine_oid(routine.kind, &routine.name) == oid))
}

pub(crate) fn pg_builtin_proc_return_type_oid(oid: i64) -> Option<i64> {
    PG_BUILTIN_PROC_ROWS
        .iter()
        .find_map(|&(proc_oid, _, return_type, _)| (proc_oid == oid).then_some(return_type))
        .or_else(|| {
            PG_PLPGSQL_PROC_ROWS
                .iter()
                .find_map(|&(proc_oid, _, return_type, _, _, _)| {
                    (proc_oid == oid).then_some(return_type)
                })
        })
}

pub(crate) fn routine_argument_signature(args: &[String], include_defaults: bool) -> String {
    args.iter()
        .map(|arg| routine_argument(arg, include_defaults))
        .filter(|arg| !arg.is_empty())
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) fn routine_argument(arg: &str, include_defaults: bool) -> String {
    let arg = collapse_sql_whitespace(arg.trim());
    let (signature, default_clause) = split_routine_argument_default(&arg);
    let signature = normalize_routine_argument_signature(signature);
    if include_defaults {
        match default_clause {
            Some(default_clause) => format!("{signature}{default_clause}"),
            None => signature,
        }
    } else {
        signature
    }
}

pub(crate) fn split_routine_argument_default(arg: &str) -> (&str, Option<&str>) {
    let lower = arg.to_ascii_lowercase();
    match lower.find(" default ").or_else(|| lower.find(" = ")) {
        Some(idx) => (arg[..idx].trim(), Some(&arg[idx..])),
        None => (arg, None),
    }
}

pub(crate) fn normalize_routine_argument_signature(signature: &str) -> String {
    let tokens = signature.split_whitespace().collect::<Vec<_>>();
    if tokens.is_empty() {
        return String::new();
    }
    let mut output = Vec::new();
    let mut type_start = 0;
    if routine_argument_mode(tokens[0]).is_some() {
        output.push(tokens[0].to_ascii_lowercase());
        type_start = 1;
    }
    if tokens.len().saturating_sub(type_start) > 1 {
        output.push(normalize_routine_identifier_token(tokens[type_start]));
        type_start += 1;
    }
    output.push(normalize_routine_type_tokens(&tokens[type_start..]));
    output
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

pub(crate) fn routine_argument_mode(token: &str) -> Option<&'static str> {
    match token.to_ascii_lowercase().as_str() {
        "in" => Some("in"),
        "out" => Some("out"),
        "inout" => Some("inout"),
        "variadic" => Some("variadic"),
        _ => None,
    }
}

pub(crate) fn normalize_routine_identifier_token(token: &str) -> String {
    if token.starts_with('"') {
        token.to_string()
    } else {
        token.to_ascii_lowercase()
    }
}

pub(crate) fn normalize_routine_type_tokens(tokens: &[&str]) -> String {
    let rendered = tokens.join(" ");
    if rendered.is_empty() {
        return rendered;
    }
    let lower = rendered.to_ascii_lowercase();
    pg_type_regtype_name(&lower)
        .map(|pg_type| pg_function_result_type(&pg_type))
        .unwrap_or(lower)
}

pub(crate) fn collapse_sql_whitespace(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub(crate) fn pg_function_result_type(return_type: &str) -> String {
    let trimmed = collapse_sql_whitespace(return_type.trim());
    if let Some(pg_type) = pg_type_regtype_name(&trimmed) {
        if let Some(name) = pg_type_name_from_oid(pg_type_oid(&pg_type)) {
            return name.to_string();
        }
        return pg_type;
    }
    trimmed
}

pub(crate) fn pg_get_serial_sequence(db: &BicDb, args: &[SqlValue]) -> Result<SqlValue> {
    if args.len() != 2 {
        return Err(SqlError::InvalidSql(format!(
            "pg_get_serial_sequence expects 2 arguments, got {}",
            args.len()
        )));
    }
    let Some(table) = sql_value_text(&args[0]) else {
        return Ok(SqlValue::Null);
    };
    let Some(column) = sql_value_text(&args[1]) else {
        return Ok(SqlValue::Null);
    };
    let table = normalize_object_name(&table);
    let column = normalize_object_name(&column);
    Ok(list_sequences(db)?
        .into_iter()
        .find(|sequence| {
            sequence
                .owned_by_table
                .as_deref()
                .is_some_and(|owned_table| normalize_object_name(owned_table) == table)
                && sequence
                    .owned_by_column
                    .as_deref()
                    .is_some_and(|owned_column| normalize_object_name(owned_column) == column)
        })
        .map(|sequence| SqlValue::String(sequence.name))
        .unwrap_or(SqlValue::Null))
}

pub(crate) fn eval_catalog_function_value(name: &str, args: &[SqlValue]) -> Option<SqlValue> {
    match name {
        "current_database" | "pg_catalog.current_database" => {
            Some(SqlValue::String("bicdb".to_string()))
        }
        "current_schema" | "pg_catalog.current_schema" => {
            Some(SqlValue::String("public".to_string()))
        }
        "current_date" | "pg_catalog.current_date" => {
            Some(SqlValue::String(unix_now_date_string()))
        }
        "now"
        | "pg_catalog.now"
        | "current_timestamp"
        | "pg_catalog.current_timestamp"
        | "transaction_timestamp"
        | "pg_catalog.transaction_timestamp" => {
            Some(SqlValue::String(transaction_timestamp_string()))
        }
        "clock_timestamp" | "pg_catalog.clock_timestamp" => {
            Some(SqlValue::String(unix_now_timestamp_string()))
        }
        "set_config" | "pg_catalog.set_config" if args.len() == 3 => args.get(1).cloned(),
        "concat" | "pg_catalog.concat" => Some(SqlValue::String(
            args.iter()
                .filter(|value| !matches!(value, SqlValue::Null))
                .map(SqlValue::to_cell)
                .collect::<String>(),
        )),
        "hashtextextended" | "pg_catalog.hashtextextended" if args.len() == 2 => {
            if args.iter().any(|arg| matches!(arg, SqlValue::Null)) {
                Some(SqlValue::Null)
            } else {
                sql_value_i64(&args[1]).map(|seed| {
                    SqlValue::Int(crate::pg_hash::hash_text_extended(&args[0].to_cell(), seed))
                })
            }
        }
        "hashtext" | "pg_catalog.hashtext" if args.len() == 1 => {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            args[0].to_cell().hash(&mut hasher);
            Some(SqlValue::Int(hasher.finish() as i32 as i64))
        }
        "quote_ident" | "pg_catalog.quote_ident" if args.len() == 1 => {
            sql_value_text(&args[0]).map(|value| SqlValue::String(pg_quote_ident(&value)))
        }
        "quote_literal" | "pg_catalog.quote_literal" if args.len() == 1 => {
            if matches!(args[0], SqlValue::Null) {
                Some(SqlValue::Null)
            } else {
                Some(SqlValue::String(pg_quote_literal(&args[0].to_cell())))
            }
        }
        "pg_advisory_xact_lock" | "pg_catalog.pg_advisory_xact_lock" if args.len() == 1 => {
            Some(SqlValue::Null)
        }
        "format_type" | "pg_catalog.format_type" => {
            args.first().and_then(sql_value_i64).map(|oid| {
                let typmod = args.get(1).and_then(sql_value_i64).unwrap_or(-1) as i32;
                SqlValue::String(
                    i32::try_from(oid)
                        .ok()
                        .and_then(|oid| pg_format_type(oid, typmod))
                        .unwrap_or_else(|| "???".to_string()),
                )
            })
        }
        "array_to_string" | "pg_catalog.array_to_string" => eval_array_to_string(args),
        "to_regtype" | "pg_catalog.to_regtype" => args
            .first()
            .and_then(|arg| pg_type_regtype_name(&arg.to_cell()))
            .map(SqlValue::String)
            .or(Some(SqlValue::Null)),
        "pg_get_expr" | "pg_catalog.pg_get_expr" => args.first().cloned(),
        "acldefault" | "pg_catalog.acldefault" => eval_acldefault(args),
        "pg_table_is_visible"
        | "pg_catalog.pg_table_is_visible"
        | "pg_type_is_visible"
        | "pg_catalog.pg_type_is_visible"
        | "pg_function_is_visible"
        | "pg_catalog.pg_function_is_visible"
        | "has_column_privilege"
        | "pg_catalog.has_column_privilege" => Some(SqlValue::Bool(true)),
        "pg_is_other_temp_schema" | "pg_catalog.pg_is_other_temp_schema" => {
            Some(SqlValue::Bool(false))
        }
        // Projected by psql meta-commands over the catalog tables.
        "pg_encoding_to_char" | "pg_catalog.pg_encoding_to_char" => {
            Some(SqlValue::String(match args.first() {
                Some(SqlValue::Int(0)) => "SQL_ASCII".to_string(),
                _ => "UTF8".to_string(),
            }))
        }
        "pg_char_to_encoding" | "pg_catalog.pg_char_to_encoding" => Some(SqlValue::Int(6)),
        "obj_description"
        | "pg_catalog.obj_description"
        | "shobj_description"
        | "pg_catalog.shobj_description"
        | "col_description"
        | "pg_catalog.col_description" => Some(SqlValue::Null),
        "pg_tablespace_location" | "pg_catalog.pg_tablespace_location" => {
            Some(SqlValue::String(String::new()))
        }
        "pg_size_pretty" | "pg_catalog.pg_size_pretty" => {
            let bytes = match args.first() {
                Some(SqlValue::Int(value)) => *value,
                Some(SqlValue::Float(value)) => *value as i64,
                _ => 0,
            };
            let scaled = bytes as f64;
            Some(SqlValue::String(if scaled >= 1024.0 * 1024.0 * 1024.0 {
                format!("{:.0} GB", scaled / (1024.0 * 1024.0 * 1024.0))
            } else if scaled >= 1024.0 * 1024.0 {
                format!("{:.0} MB", scaled / (1024.0 * 1024.0))
            } else if scaled >= 1024.0 {
                format!("{:.0} kB", scaled / 1024.0)
            } else {
                format!("{bytes} bytes")
            }))
        }
        "num_nonnulls" | "pg_catalog.num_nonnulls" => Some(SqlValue::Int(
            args.iter()
                .filter(|value| !matches!(value, SqlValue::Null))
                .count() as i64,
        )),
        "json_typeof" | "pg_catalog.json_typeof" | "jsonb_typeof" | "pg_catalog.jsonb_typeof" => {
            args.first().map(json_typeof_value)
        }
        _ => None,
    }
}

pub(crate) fn pg_quote_ident(value: &str) -> String {
    if is_unquoted_pg_identifier(value) {
        value.to_string()
    } else {
        format!("\"{}\"", value.replace('"', "\"\""))
    }
}

pub(crate) fn pg_quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub(crate) fn eval_array_to_string(args: &[SqlValue]) -> Option<SqlValue> {
    if !(args.len() == 2 || args.len() == 3) {
        return None;
    }
    if matches!(args.first(), Some(SqlValue::Null)) || matches!(args.get(1), Some(SqlValue::Null)) {
        return Some(SqlValue::Null);
    }
    let delimiter = sql_value_text(args.get(1)?)?;
    let null_string = args.get(2).and_then(sql_value_text);
    let values = array_like_values(args.first()?)?;
    let mut rendered = Vec::new();
    for value in values {
        match value {
            SqlValue::Null => {
                if let Some(null_string) = &null_string {
                    rendered.push(null_string.clone());
                }
            }
            value => rendered.push(value.to_cell()),
        }
    }
    Some(SqlValue::String(rendered.join(&delimiter)))
}

pub(crate) fn eval_acldefault(args: &[SqlValue]) -> Option<SqlValue> {
    if args.len() != 2 || args.iter().any(|arg| matches!(arg, SqlValue::Null)) {
        return Some(SqlValue::Null);
    }
    let object_kind = sql_value_text(&args[0])?.chars().next()?;
    let owner = acl_owner_name(&args[1]);
    let (owner_privileges, public_privileges) = match object_kind {
        'd' => ("CTc", Some("Tc")),
        'f' => ("X", Some("X")),
        'n' => ("UC", None),
        'F' | 'S' => ("U", None),
        'r' | 'p' | 'v' | 'm' => ("arwdDxt", None),
        's' => ("rwU", None),
        'T' | 'l' => ("U", Some("U")),
        _ => return Some(SqlValue::Null),
    };
    let mut entries = Vec::new();
    if let Some(public_privileges) = public_privileges {
        entries.push(format!("={public_privileges}/{owner}"));
    }
    entries.push(format!("{owner}={owner_privileges}/{owner}"));
    Some(SqlValue::String(format!("{{{}}}", entries.join(","))))
}

pub(crate) fn acl_owner_name(value: &SqlValue) -> String {
    match sql_value_i64(value) {
        Some(10) => "bicdb".to_string(),
        Some(oid) => oid.to_string(),
        None => value.to_cell(),
    }
}

pub(crate) fn json_typeof_value(value: &SqlValue) -> SqlValue {
    let value_type = match value {
        SqlValue::Null => return SqlValue::Null,
        SqlValue::JsonText(value) => match value.parsed() {
            JsonValue::Null => "null",
            JsonValue::Bool(_) => "boolean",
            JsonValue::Number(_) => "number",
            JsonValue::String(_) => "string",
            JsonValue::Array(_) => "array",
            JsonValue::Object(_) => "object",
        },
        SqlValue::Json(JsonValue::Null) => "null",
        SqlValue::Json(JsonValue::Bool(_)) => "boolean",
        SqlValue::Json(JsonValue::Number(_)) => "number",
        SqlValue::Json(JsonValue::String(_)) => "string",
        SqlValue::Json(JsonValue::Array(_)) => "array",
        SqlValue::Json(JsonValue::Object(_)) => "object",
        _ => return SqlValue::Null,
    };
    SqlValue::String(value_type.to_string())
}

fn trusted_identity_guc<'a>(
    session_gucs: &'a HashMap<String, String>,
    suffix: &str,
) -> Option<&'a str> {
    session_gucs
        .get(&format!("bicdb.{suffix}"))
        .or_else(|| session_gucs.get(&format!("carrier.{suffix}")))
        .map(String::as_str)
        .filter(|value| !value.is_empty())
}

/// Whether this session carries a host-bound trusted identity. The protected
/// `bicdb.current_user` setting exists only when a `SecurityContext` was bound
/// to the connection; SQL cannot set it.
pub(crate) fn has_trusted_identity(session_gucs: &HashMap<String, String>) -> bool {
    trusted_identity_guc(session_gucs, "current_user").is_some()
}

/// The Carrier policy-context payload derived from the trusted identity, in
/// the shape `carrier_private.current_tenant()`, `current_roles()`, and
/// `"current_user"()` read (`tenant`, `roles`, `user`).
pub(crate) fn trusted_identity_context(
    session_gucs: &HashMap<String, String>,
) -> Option<JsonValue> {
    let user_id = trusted_identity_guc(session_gucs, "current_user")?;
    let list = |suffix: &str| -> JsonValue {
        JsonValue::Array(
            trusted_identity_guc(session_gucs, suffix)
                .map(|value| {
                    value
                        .split(',')
                        .filter(|entry| !entry.is_empty())
                        .map(|entry| JsonValue::String(entry.to_string()))
                        .collect()
                })
                .unwrap_or_default(),
        )
    };
    let text = |suffix: &str| -> JsonValue {
        trusted_identity_guc(session_gucs, suffix)
            .map(|value| JsonValue::String(value.to_string()))
            .unwrap_or(JsonValue::Null)
    };
    let mut user = serde_json::Map::new();
    user.insert("id".to_string(), JsonValue::String(user_id.to_string()));
    user.insert("email".to_string(), text("current_email"));
    user.insert("name".to_string(), text("current_name"));
    user.insert("roles".to_string(), list("current_roles"));
    user.insert("scopes".to_string(), list("current_scopes"));
    user.insert("tenant_id".to_string(), text("current_tenant"));
    user.insert("workspace_id".to_string(), text("current_workspace"));
    let mut payload = serde_json::Map::new();
    payload.insert(
        "source".to_string(),
        JsonValue::String("bicdb_trusted_identity".to_string()),
    );
    payload.insert(
        "subject".to_string(),
        JsonValue::String(user_id.to_string()),
    );
    payload.insert("tenant".to_string(), text("current_tenant"));
    payload.insert("hub".to_string(), text("current_workspace"));
    payload.insert("client".to_string(), text("current_client"));
    payload.insert("session".to_string(), text("current_session"));
    payload.insert(
        "authentication_strength".to_string(),
        text("authentication_strength"),
    );
    payload.insert("roles".to_string(), list("current_roles"));
    payload.insert("scopes".to_string(), list("current_scopes"));
    payload.insert("user".to_string(), JsonValue::Object(user));
    Some(JsonValue::Object(payload))
}

pub(crate) fn eval_session_function_value(
    name: &str,
    args: &[SqlValue],
    session_gucs: &HashMap<String, String>,
) -> Result<Option<SqlValue>> {
    match name {
        "version" | "pg_catalog.version" if args.is_empty() => {
            Ok(Some(SqlValue::String(sql_version_banner(session_gucs))))
        }
        "bicdb_version" | "pg_catalog.bicdb_version" if args.is_empty() => {
            Ok(Some(SqlValue::String(BICDB_VERSION.to_string())))
        }
        "current_database" | "pg_catalog.current_database" if args.is_empty() => Ok(Some(
            SqlValue::String(current_database_from_gucs(session_gucs)),
        )),
        // CURRENT_USER / CURRENT_ROLE / USER / SESSION_USER parse as
        // zero-argument keyword functions.
        "current_user" | "pg_catalog.current_user" | "current_role" | "user" if args.is_empty() => {
            Ok(Some(SqlValue::String(current_user_from_gucs(session_gucs))))
        }
        "session_user" | "pg_catalog.session_user" if args.is_empty() => {
            Ok(Some(SqlValue::String(session_user_from_gucs(session_gucs))))
        }
        "current_trusted_tenant" if args.is_empty() => Ok(Some(
            session_gucs
                .get("bicdb.current_tenant")
                .or_else(|| session_gucs.get("carrier.current_tenant"))
                .filter(|tenant| !tenant.is_empty())
                .cloned()
                .map(SqlValue::String)
                .unwrap_or(SqlValue::Null),
        )),
        // Identity comes from the authenticated login or verified transaction
        // delegation. Synthesize the identity payload from protected host
        // settings. Mutation authorization still requires its separately
        // verified Carrier operation envelope; this does not grant writes.
        // Unbound logins fall through to the authored function.
        "carrier_private.current_context" if args.is_empty() => {
            Ok(trusted_identity_context(session_gucs).map(SqlValue::Json))
        }
        "current_trusted_tenant" => Err(SqlError::InvalidSql(format!(
            "current_trusted_tenant expects no arguments, got {}",
            args.len()
        ))),
        "current_setting" | "pg_catalog.current_setting" => {
            if args.is_empty() || args.len() > 2 {
                return Err(SqlError::InvalidSql(format!(
                    "current_setting expects 1 or 2 argument(s), got {}",
                    args.len()
                )));
            }
            let SqlValue::String(setting) = &args[0] else {
                return Err(SqlError::InvalidSql(
                    "current_setting name must be text".to_string(),
                ));
            };
            let key = setting.to_ascii_lowercase();
            if let Some(value) = session_gucs.get(&key) {
                return Ok(Some(SqlValue::String(value.clone())));
            }
            if let Some(value) = default_session_guc(&key) {
                return Ok(Some(SqlValue::String(value.to_string())));
            }
            let missing_ok = args
                .get(1)
                .and_then(|value| match value {
                    SqlValue::Bool(value) => Some(*value),
                    SqlValue::String(value) => value.parse().ok(),
                    _ => None,
                })
                .unwrap_or(false);
            if missing_ok {
                Ok(Some(SqlValue::Null))
            } else {
                Err(SqlError::undefined_object(format!(
                    "unrecognized configuration parameter \"{setting}\""
                )))
            }
        }
        "replace" | "pg_catalog.replace" => {
            require_arg_count("replace", args, 3)?;
            if args.iter().any(|value| matches!(value, SqlValue::Null)) {
                return Ok(Some(SqlValue::Null));
            }
            Ok(Some(SqlValue::String(args[0].to_cell().replace(
                args[1].to_cell().as_str(),
                args[2].to_cell().as_str(),
            ))))
        }
        "split_part" | "pg_catalog.split_part" => {
            require_arg_count("split_part", args, 3)?;
            if args.iter().any(|value| matches!(value, SqlValue::Null)) {
                return Ok(Some(SqlValue::Null));
            }

            let source = args[0].to_cell();
            let delimiter = args[1].to_cell();
            let field = match &args[2] {
                SqlValue::Int(value) => *value,
                value => value.to_cell().parse::<i64>().map_err(|_| {
                    SqlError::invalid_parameter_value("field position must be an integer")
                })?,
            };
            if field == 0 {
                return Err(SqlError::invalid_parameter_value(
                    "field position must not be zero",
                ));
            }

            let parts = if delimiter.is_empty() {
                vec![source.as_str()]
            } else {
                source.split(delimiter.as_str()).collect::<Vec<_>>()
            };
            let index = if field > 0 {
                i128::from(field) - 1
            } else {
                parts.len() as i128 + i128::from(field)
            };
            let part = usize::try_from(index)
                .ok()
                .and_then(|index| parts.get(index))
                .copied()
                .unwrap_or_default();
            Ok(Some(SqlValue::String(part.to_string())))
        }
        "regexp_replace" | "pg_catalog.regexp_replace" => {
            if !(3..=4).contains(&args.len()) {
                return Err(SqlError::InvalidSql(format!(
                    "regexp_replace expects 3 or 4 arguments, got {}",
                    args.len()
                )));
            }
            if args.iter().any(|value| matches!(value, SqlValue::Null)) {
                return Ok(Some(SqlValue::Null));
            }

            let source = args[0].to_cell();
            let mut pattern = args[1].to_cell();
            let replacement = postgres_regex_replacement(&args[2].to_cell());
            let flags = args.get(3).map(SqlValue::to_cell).unwrap_or_default();
            let mut global = false;
            let mut case_insensitive = false;
            let mut multi_line = false;
            let mut dot_matches_new_line = true;
            let mut ignore_whitespace = false;
            for flag in flags.chars() {
                match flag {
                    'g' => global = true,
                    'i' => case_insensitive = true,
                    'm' | 'n' => {
                        multi_line = true;
                        dot_matches_new_line = false;
                    }
                    'p' => dot_matches_new_line = false,
                    's' => {
                        multi_line = false;
                        dot_matches_new_line = true;
                    }
                    'w' => {
                        multi_line = true;
                        dot_matches_new_line = true;
                    }
                    'x' => ignore_whitespace = true,
                    'q' => pattern = regex::escape(&pattern),
                    other => {
                        return Err(SqlError::invalid_parameter_value(format!(
                            "invalid regular expression option: {other}"
                        )));
                    }
                }
            }
            let regex = RegexBuilder::new(&pattern)
                .case_insensitive(case_insensitive)
                .multi_line(multi_line)
                .dot_matches_new_line(dot_matches_new_line)
                .ignore_whitespace(ignore_whitespace)
                .build()
                .map_err(|error| {
                    SqlError::invalid_parameter_value(format!(
                        "invalid regular expression: {error}"
                    ))
                })?;
            let value = if global {
                regex.replace_all(&source, replacement.as_str())
            } else {
                regex.replace(&source, replacement.as_str())
            };
            Ok(Some(SqlValue::String(value.into_owned())))
        }
        "to_timestamp" | "pg_catalog.to_timestamp" => Ok(Some(eval_to_timestamp(args)?)),
        "lastval" | "pg_catalog.lastval" => {
            if !args.is_empty() {
                return Err(SqlError::InvalidSql(
                    "lastval expects no arguments".to_string(),
                ));
            }
            Ok(Some(
                session_gucs
                    .get(LASTVAL_SESSION_KEY)
                    .and_then(|value| value.parse::<i64>().ok())
                    .map(SqlValue::Int)
                    .ok_or_else(|| {
                        SqlError::InvalidSql(
                            "lastval is not yet defined in this session".to_string(),
                        )
                    })?,
            ))
        }
        _ => Ok(None),
    }
}

pub(crate) fn postgres_regex_replacement(value: &str) -> String {
    let mut replacement = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        match ch {
            '$' => replacement.push_str("$$"),
            '\\' => match chars.next() {
                Some('&') => replacement.push_str("$0"),
                Some(group @ '1'..='9') => {
                    replacement.push_str("${");
                    replacement.push(group);
                    replacement.push('}');
                }
                Some('\\') => replacement.push('\\'),
                Some(other) => {
                    replacement.push('\\');
                    replacement.push(other);
                }
                None => replacement.push('\\'),
            },
            other => replacement.push(other),
        }
    }
    replacement
}

pub(crate) fn current_database_from_gucs(session_gucs: &HashMap<String, String>) -> String {
    session_gucs
        .get("database")
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .unwrap_or_else(|| "bicdb".to_string())
}

#[derive(Default)]
pub(crate) struct TimestampTemplateParts {
    pub(crate) year: Option<i32>,
    pub(crate) month: Option<u32>,
    pub(crate) day: Option<u32>,
    pub(crate) hour: Option<u32>,
    pub(crate) minute: Option<u32>,
    pub(crate) second: Option<u32>,
}

pub(crate) fn eval_to_timestamp(args: &[SqlValue]) -> Result<SqlValue> {
    if args.len() != 1 && args.len() != 2 {
        return Err(SqlError::InvalidSql(format!(
            "to_timestamp expects 1 or 2 argument(s), got {}",
            args.len()
        )));
    }
    if args.iter().any(|value| matches!(value, SqlValue::Null)) {
        return Ok(SqlValue::Null);
    }
    if args.len() == 1 {
        let epoch_seconds = sql_value_f64(&args[0]).ok_or_else(|| {
            SqlError::InvalidSql("to_timestamp first argument must be numeric".to_string())
        })?;
        return Ok(SqlValue::String(unix_seconds_to_utc_timestamp(
            epoch_seconds.floor() as i64,
        )));
    }
    let input = sql_value_text(&args[0]).ok_or_else(|| {
        SqlError::InvalidSql("to_timestamp first argument must be text".to_string())
    })?;
    let format = sql_value_text(&args[1]).ok_or_else(|| {
        SqlError::InvalidSql("to_timestamp second argument must be text".to_string())
    })?;
    let parts = parse_timestamp_template(&input, &format)?;
    let year = parts.year.ok_or_else(|| {
        SqlError::InvalidSql("to_timestamp format must include YYYY or YY".to_string())
    })?;
    let month = parts
        .month
        .ok_or_else(|| SqlError::InvalidSql("to_timestamp format must include MM".to_string()))?;
    let day = parts
        .day
        .ok_or_else(|| SqlError::InvalidSql("to_timestamp format must include DD".to_string()))?;
    let hour = parts.hour.unwrap_or(0);
    let minute = parts.minute.unwrap_or(0);
    let second = parts.second.unwrap_or(0);
    validate_timestamp_parts(year, month, day, hour, minute, second)?;
    Ok(SqlValue::String(format!(
        "{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}"
    )))
}

pub(crate) fn parse_timestamp_template(
    input: &str,
    format: &str,
) -> Result<TimestampTemplateParts> {
    let input = input.trim();
    let format = format.trim().to_ascii_uppercase();
    let input_bytes = input.as_bytes();
    let format_bytes = format.as_bytes();
    let mut input_pos = 0;
    let mut format_pos = 0;
    let mut parts = TimestampTemplateParts::default();

    while format_pos < format_bytes.len() {
        if format[format_pos..].starts_with("YYYY") {
            parts.year = Some(consume_timestamp_digits(input, &mut input_pos, 4, "YYYY")? as i32);
            format_pos += 4;
        } else if format[format_pos..].starts_with("HH24") {
            parts.hour = Some(consume_timestamp_digits(input, &mut input_pos, 2, "HH24")?);
            format_pos += 4;
        } else if format[format_pos..].starts_with("YY") {
            let year = consume_timestamp_digits(input, &mut input_pos, 2, "YY")? as i32;
            parts.year = Some(if year >= 70 { 1900 + year } else { 2000 + year });
            format_pos += 2;
        } else if format[format_pos..].starts_with("MM") {
            parts.month = Some(consume_timestamp_digits(input, &mut input_pos, 2, "MM")?);
            format_pos += 2;
        } else if format[format_pos..].starts_with("DD") {
            parts.day = Some(consume_timestamp_digits(input, &mut input_pos, 2, "DD")?);
            format_pos += 2;
        } else if format[format_pos..].starts_with("MI") {
            parts.minute = Some(consume_timestamp_digits(input, &mut input_pos, 2, "MI")?);
            format_pos += 2;
        } else if format[format_pos..].starts_with("SS") {
            parts.second = Some(consume_timestamp_digits(input, &mut input_pos, 2, "SS")?);
            format_pos += 2;
        } else {
            let expected = format_bytes[format_pos];
            if expected.is_ascii_whitespace() {
                while format_pos < format_bytes.len()
                    && format_bytes[format_pos].is_ascii_whitespace()
                {
                    format_pos += 1;
                }
                while input_pos < input_bytes.len() && input_bytes[input_pos].is_ascii_whitespace()
                {
                    input_pos += 1;
                }
            } else if expected.is_ascii_alphabetic() {
                return Err(SqlError::Unsupported(format!(
                    "to_timestamp format token {} is not supported",
                    format[format_pos..]
                        .chars()
                        .take_while(|ch| ch.is_ascii_alphabetic() || ch.is_ascii_digit())
                        .collect::<String>()
                )));
            } else {
                let Some(actual) = input_bytes.get(input_pos).copied() else {
                    return Err(SqlError::InvalidSql(format!(
                        "to_timestamp input ended before literal '{}'",
                        expected as char
                    )));
                };
                if !actual.eq_ignore_ascii_case(&expected) {
                    return Err(SqlError::InvalidSql(format!(
                        "to_timestamp expected literal '{}' at input byte {}",
                        expected as char, input_pos
                    )));
                }
                input_pos += 1;
                format_pos += 1;
            }
        }
    }

    while input_pos < input_bytes.len() && input_bytes[input_pos].is_ascii_whitespace() {
        input_pos += 1;
    }
    if input_pos != input_bytes.len() {
        return Err(SqlError::InvalidSql(format!(
            "to_timestamp input has trailing text at byte {input_pos}"
        )));
    }
    Ok(parts)
}

pub(crate) fn consume_timestamp_digits(
    input: &str,
    input_pos: &mut usize,
    digits: usize,
    field: &str,
) -> Result<u32> {
    let bytes = input.as_bytes();
    let end = input_pos.saturating_add(digits);
    if end > bytes.len() || !bytes[*input_pos..end].iter().all(u8::is_ascii_digit) {
        return Err(SqlError::InvalidSql(format!(
            "to_timestamp expected {digits} digit(s) for {field} at input byte {}",
            *input_pos
        )));
    }
    let value = input[*input_pos..end]
        .parse::<u32>()
        .map_err(|error| SqlError::InvalidSql(error.to_string()))?;
    *input_pos = end;
    Ok(value)
}

pub(crate) fn validate_timestamp_parts(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
) -> Result<()> {
    if !(1..=12).contains(&month) {
        return Err(SqlError::InvalidSql(format!(
            "to_timestamp month {month} is out of range"
        )));
    }
    let max_day = days_in_month(year, month);
    if day == 0 || day > max_day {
        return Err(SqlError::InvalidSql(format!(
            "to_timestamp day {day} is out of range for month {month}"
        )));
    }
    if hour > 23 {
        return Err(SqlError::InvalidSql(format!(
            "to_timestamp hour {hour} is out of range"
        )));
    }
    if minute > 59 {
        return Err(SqlError::InvalidSql(format!(
            "to_timestamp minute {minute} is out of range"
        )));
    }
    if second > 59 {
        return Err(SqlError::InvalidSql(format!(
            "to_timestamp second {second} is out of range"
        )));
    }
    Ok(())
}

pub(crate) fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

pub(crate) fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

pub(crate) fn default_session_guc(name: &str) -> Option<&'static str> {
    match name {
        "bicdb_version" => Some(BICDB_VERSION),
        "server_version" => Some(POSTGRES_COMPATIBILITY_VERSION),
        "server_version_num" => Some(POSTGRES_COMPATIBILITY_VERSION_NUM),
        "application_name" => Some("bicdb"),
        "client_encoding" => Some("UTF8"),
        "client_min_messages" => Some("notice"),
        "bytea_output" => Some("hex"),
        "intervalstyle" => Some("postgres"),
        "jit" => Some("off"),
        "search_path" => Some("public"),
        _ => None,
    }
}

pub(crate) fn bytea_argument(value: &SqlValue) -> Result<Option<Vec<u8>>> {
    if matches!(value, SqlValue::Null) {
        return Ok(None);
    }
    let text = value.to_cell();
    parse_bytea_text(&text)
        .map(Some)
        .map_err(|error| match error {
            PgCanonicalValueError::InvalidByteaHex => SqlError::invalid_parameter_value(format!(
                "invalid hexadecimal digit in bytea value: {text}"
            )),
            _ => SqlError::invalid_text_representation(
                "bytea",
                format!("invalid input syntax for type bytea: \"{text}\""),
            ),
        })
}

pub(crate) fn bit_argument(value: &SqlValue) -> Result<Option<PgBitString>> {
    if matches!(value, SqlValue::Null) {
        return Ok(None);
    }
    let text = value.to_cell();
    PgBitString::from_bit_text(&text).map(Some).map_err(|_| {
        SqlError::invalid_text_representation(
            "bit",
            format!("invalid input syntax for type bit: \"{text}\""),
        )
    })
}

pub(crate) fn bytea_index(args: &[SqlValue], index: usize) -> Result<usize> {
    sql_value_i64(&args[index])
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| SqlError::data_exception("2202E", "index out of valid range", None))
}

pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0x82f6_3b78 & (0_u32.wrapping_sub(crc & 1)));
        }
    }
    !crc
}

pub(crate) fn format_base64(bytes: &[u8]) -> String {
    let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
    encoded
        .as_bytes()
        .chunks(76)
        .map(|chunk| std::str::from_utf8(chunk).expect("base64 is ASCII"))
        .collect::<Vec<_>>()
        .join("\n")
}

pub(crate) fn normalized_encoding_name(value: &SqlValue) -> Result<&'static str> {
    match value
        .to_cell()
        .replace(['-', '_'], "")
        .to_ascii_uppercase()
        .as_str()
    {
        "UTF8" | "UNICODE" => Ok("UTF8"),
        "LATIN1" | "ISO88591" => Ok("LATIN1"),
        name => Err(SqlError::invalid_parameter_value(format!(
            "invalid encoding name: \"{name}\""
        ))),
    }
}

pub(crate) fn decode_server_text(bytes: &[u8], encoding: &str) -> Result<String> {
    match encoding {
        "UTF8" => std::str::from_utf8(bytes).map(str::to_string).map_err(|_| {
            SqlError::data_exception("22021", "invalid byte sequence for encoding \"UTF8\"", None)
        }),
        "LATIN1" => Ok(bytes.iter().map(|byte| char::from(*byte)).collect()),
        _ => unreachable!("encoding names are normalized"),
    }
}

pub(crate) fn encode_server_text(value: &str, encoding: &str) -> Result<Vec<u8>> {
    match encoding {
        "UTF8" => Ok(value.as_bytes().to_vec()),
        "LATIN1" => value
            .chars()
            .map(|character| {
                u8::try_from(u32::from(character)).map_err(|_| {
                    SqlError::data_exception(
                        "22P05",
                        "character has no equivalent in encoding \"LATIN1\"",
                        None,
                    )
                })
            })
            .collect(),
        _ => unreachable!("encoding names are normalized"),
    }
}

pub(crate) fn trim_bytea(
    mut bytes: &[u8],
    characters: &[u8],
    leading: bool,
    trailing: bool,
) -> Vec<u8> {
    if leading {
        while bytes.first().is_some_and(|byte| characters.contains(byte)) {
            bytes = &bytes[1..];
        }
    }
    if trailing {
        while bytes.last().is_some_and(|byte| characters.contains(byte)) {
            bytes = &bytes[..bytes.len() - 1];
        }
    }
    bytes.to_vec()
}

pub(crate) fn eval_bit_function_value(
    name: &str,
    args: &[SqlValue],
    arg_types: Option<&[Option<String>]>,
) -> Result<Option<SqlValue>> {
    let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    let first_type = arg_types
        .and_then(|types| types.first())
        .and_then(Option::as_deref);
    if !matches!(first_type, Some("bit" | "varbit")) {
        return Ok(None);
    }
    if args.iter().any(|value| matches!(value, SqlValue::Null)) {
        return Ok(Some(SqlValue::Null));
    }
    let bits = bit_argument(&args[0])?.unwrap();
    match name {
        "length" | "bit_length" => {
            require_arg_count(name, args, 1)?;
            Ok(Some(SqlValue::Int(bits.bit_len() as i64)))
        }
        "octet_length" => {
            require_arg_count(name, args, 1)?;
            Ok(Some(SqlValue::Int(bits.bit_len().div_ceil(8) as i64)))
        }
        "bit_count" => {
            require_arg_count(name, args, 1)?;
            let count = bits
                .bytes()
                .iter()
                .map(|byte| i64::from(byte.count_ones()))
                .sum();
            Ok(Some(SqlValue::Int(count)))
        }
        "get_bit" => {
            require_arg_count(name, args, 2)?;
            let index = bytea_index(args, 1)?;
            if index >= bits.bit_len() {
                return Err(SqlError::data_exception(
                    "2202E",
                    format!(
                        "bit index {index} out of valid range (0..{})",
                        bits.bit_len().saturating_sub(1)
                    ),
                    None,
                ));
            }
            Ok(Some(SqlValue::Int(i64::from(
                (bits.bytes()[index / 8] >> (7 - index % 8)) & 1,
            ))))
        }
        "set_bit" => {
            require_arg_count(name, args, 3)?;
            let index = bytea_index(args, 1)?;
            if index >= bits.bit_len() {
                return Err(SqlError::data_exception(
                    "2202E",
                    format!(
                        "bit index {index} out of valid range (0..{})",
                        bits.bit_len().saturating_sub(1)
                    ),
                    None,
                ));
            }
            let replacement = sql_value_i64(&args[2])
                .ok_or_else(|| SqlError::invalid_parameter_value("new bit must be 0 or 1"))?;
            if !matches!(replacement, 0 | 1) {
                return Err(SqlError::invalid_parameter_value("new bit must be 0 or 1"));
            }
            let mut text = bits.to_bit_text().into_bytes();
            text[index] = if replacement == 0 { b'0' } else { b'1' };
            Ok(Some(SqlValue::String(
                String::from_utf8(text).expect("bit text is ASCII"),
            )))
        }
        _ => Ok(None),
    }
}

pub(crate) fn eval_bytea_function_value(
    name: &str,
    args: &[SqlValue],
    arg_types: Option<&[Option<String>]>,
) -> Result<Option<SqlValue>> {
    let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    let name = name.strip_prefix("public.").unwrap_or(name);
    let first_is_bytea = arg_types
        .and_then(|types| types.first())
        .and_then(Option::as_deref)
        == Some("bytea");
    let bytea_overload = first_is_bytea
        || matches!(
            name,
            "get_byte"
                | "set_byte"
                | "get_bit"
                | "set_bit"
                | "encode"
                | "decode"
                | "convert"
                | "convert_from"
                | "convert_to"
                | "crc32"
                | "crc32c"
                | "digest"
                | "hmac"
                | "sha224"
                | "sha256"
                | "sha384"
                | "sha512"
        );
    if !bytea_overload {
        return Ok(None);
    }
    if args.iter().any(|value| matches!(value, SqlValue::Null)) {
        return Ok(Some(SqlValue::Null));
    }

    match name {
        "length" | "octet_length" => {
            require_arg_count(name, args, 1)?;
            Ok(Some(SqlValue::Int(
                bytea_argument(&args[0])?.unwrap().len() as i64,
            )))
        }
        "bit_length" => {
            require_arg_count(name, args, 1)?;
            let bits = bytea_argument(&args[0])?
                .unwrap()
                .len()
                .checked_mul(8)
                .ok_or_else(|| {
                    SqlError::numeric_value_out_of_range("bit length exceeds bigint range")
                })?;
            Ok(Some(SqlValue::Int(bits as i64)))
        }
        "bit_count" => {
            require_arg_count(name, args, 1)?;
            let bits = bytea_argument(&args[0])?
                .unwrap()
                .iter()
                .map(|byte| i64::from(byte.count_ones()))
                .sum();
            Ok(Some(SqlValue::Int(bits)))
        }
        "get_byte" | "get_bit" => {
            require_arg_count(name, args, 2)?;
            let bytes = bytea_argument(&args[0])?.unwrap();
            let index = bytea_index(args, 1)?;
            let value = if name == "get_byte" {
                i64::from(*bytes.get(index).ok_or_else(|| {
                    SqlError::data_exception("2202E", "index out of valid range", None)
                })?)
            } else {
                let byte = bytes.get(index / 8).ok_or_else(|| {
                    SqlError::data_exception("2202E", "index out of valid range", None)
                })?;
                i64::from((byte >> (index % 8)) & 1)
            };
            Ok(Some(SqlValue::Int(value)))
        }
        "set_byte" | "set_bit" => {
            require_arg_count(name, args, 3)?;
            let mut bytes = bytea_argument(&args[0])?.unwrap();
            let index = bytea_index(args, 1)?;
            let replacement = sql_value_i64(&args[2])
                .ok_or_else(|| SqlError::invalid_parameter_value("new value is not an integer"))?;
            if name == "set_byte" {
                let target = bytes.get_mut(index).ok_or_else(|| {
                    SqlError::data_exception("2202E", "index out of valid range", None)
                })?;
                *target = replacement as u8;
            } else {
                if !matches!(replacement, 0 | 1) {
                    return Err(SqlError::invalid_parameter_value("new bit must be 0 or 1"));
                }
                let target = bytes.get_mut(index / 8).ok_or_else(|| {
                    SqlError::data_exception("2202E", "index out of valid range", None)
                })?;
                let mask = 1_u8 << (index % 8);
                *target = (*target & !mask) | ((replacement as u8) * mask);
            }
            Ok(Some(SqlValue::String(format_bytea_hex(&bytes))))
        }
        "encode" => {
            require_arg_count(name, args, 2)?;
            let bytes = bytea_argument(&args[0])?.unwrap();
            let format = args[1].to_cell().to_ascii_lowercase();
            let encoded = match format.as_str() {
                "hex" => format_bytea_hex(&bytes)
                    .trim_start_matches("\\x")
                    .to_string(),
                "base64" => format_base64(&bytes),
                "escape" => format_bytea_escape(&bytes),
                _ => {
                    return Err(SqlError::invalid_parameter_value(format!(
                        "unrecognized encoding: {format}"
                    )));
                }
            };
            Ok(Some(SqlValue::String(encoded)))
        }
        "decode" => {
            require_arg_count(name, args, 2)?;
            let input = args[0].to_cell();
            let format = args[1].to_cell().to_ascii_lowercase();
            let bytes = match format.as_str() {
                "hex" => parse_bytea_text(&format!("\\x{input}")),
                "escape" => parse_bytea_text(&input),
                "base64" => base64::engine::general_purpose::STANDARD
                    .decode(
                        input
                            .bytes()
                            .filter(|byte| !byte.is_ascii_whitespace())
                            .collect::<Vec<_>>(),
                    )
                    .map_err(|_| PgCanonicalValueError::InvalidBytea),
                _ => {
                    return Err(SqlError::invalid_parameter_value(format!(
                        "unrecognized encoding: {format}"
                    )));
                }
            }
            .map_err(|error| match (format.as_str(), error) {
                ("escape", _) => SqlError::invalid_text_representation(
                    "bytea",
                    "invalid input syntax for decoding",
                ),
                _ => SqlError::invalid_parameter_value("invalid input syntax for decoding"),
            })?;
            Ok(Some(SqlValue::String(format_bytea_hex(&bytes))))
        }
        "substr" | "substring" => {
            if !(2..=3).contains(&args.len()) {
                return Err(SqlError::InvalidSql(format!(
                    "{name} expects 2 or 3 arguments"
                )));
            }
            let bytes = bytea_argument(&args[0])?.unwrap();
            let start = sql_value_i64(&args[1]).unwrap_or(1);
            let start_offset = i128::from(start) - 1;
            let skip = usize::try_from(start_offset.max(0)).unwrap_or(usize::MAX);
            let length = args.get(2).map(|value| sql_value_i64(value).unwrap_or(0));
            if length.is_some_and(|length| length < 0) {
                return Err(SqlError::data_exception(
                    "22011",
                    "negative substring length not allowed",
                    None,
                ));
            }
            let take = length
                .map(|length| (i128::from(length) + start_offset.min(0)).max(0))
                .map(|length| usize::try_from(length).unwrap_or(usize::MAX))
                .unwrap_or(usize::MAX);
            let slice = bytes
                .get(skip.min(bytes.len())..)
                .unwrap_or_default()
                .get(..take.min(bytes.len().saturating_sub(skip)))
                .unwrap_or_default();
            Ok(Some(SqlValue::String(format_bytea_hex(slice))))
        }
        "trim" | "btrim" | "ltrim" | "rtrim" => {
            if !(1..=2).contains(&args.len()) {
                return Err(SqlError::InvalidSql(format!(
                    "{name} expects 1 or 2 arguments"
                )));
            }
            let bytes = bytea_argument(&args[0])?.unwrap();
            let characters = args
                .get(1)
                .map(bytea_argument)
                .transpose()?
                .flatten()
                .unwrap_or_else(|| vec![b' ']);
            let (leading, trailing) = match name {
                "ltrim" => (true, false),
                "rtrim" => (false, true),
                _ => (true, true),
            };
            Ok(Some(SqlValue::String(format_bytea_hex(&trim_bytea(
                &bytes,
                &characters,
                leading,
                trailing,
            )))))
        }
        "convert_from" => {
            require_arg_count(name, args, 2)?;
            let bytes = bytea_argument(&args[0])?.unwrap();
            let source = normalized_encoding_name(&args[1])?;
            Ok(Some(SqlValue::String(decode_server_text(&bytes, source)?)))
        }
        "convert_to" => {
            require_arg_count(name, args, 2)?;
            let destination = normalized_encoding_name(&args[1])?;
            let bytes = encode_server_text(&args[0].to_cell(), destination)?;
            Ok(Some(SqlValue::String(format_bytea_hex(&bytes))))
        }
        "convert" => {
            require_arg_count(name, args, 3)?;
            let bytes = bytea_argument(&args[0])?.unwrap();
            let source = normalized_encoding_name(&args[1])?;
            let destination = normalized_encoding_name(&args[2])?;
            let text = decode_server_text(&bytes, source)?;
            Ok(Some(SqlValue::String(format_bytea_hex(
                &encode_server_text(&text, destination)?,
            ))))
        }
        "digest" => {
            require_arg_count(name, args, 2)?;
            let bytes = bytea_argument(&args[0])?.unwrap();
            let algorithm = args[1].to_cell().to_ascii_lowercase();
            let digest = match algorithm.as_str() {
                "md5" => Md5::digest(bytes).to_vec(),
                "sha224" => Sha224::digest(bytes).to_vec(),
                "sha256" => Sha256::digest(bytes).to_vec(),
                "sha384" => Sha384::digest(bytes).to_vec(),
                "sha512" => Sha512::digest(bytes).to_vec(),
                _ => {
                    return Err(SqlError::invalid_parameter_value(format!(
                        "Cannot use \"{algorithm}\": No such hash algorithm"
                    )));
                }
            };
            Ok(Some(SqlValue::String(format_bytea_hex(&digest))))
        }
        "hmac" => {
            require_arg_count(name, args, 3)?;
            let bytes = bytea_argument(&args[0])?.unwrap();
            let key = bytea_argument(&args[1])?.unwrap();
            let algorithm = args[2].to_cell().to_ascii_lowercase();
            let digest = match algorithm.as_str() {
                "sha256" => {
                    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&key).map_err(|_| {
                        SqlError::invalid_parameter_value("invalid HMAC key length")
                    })?;
                    mac.update(&bytes);
                    mac.finalize().into_bytes().to_vec()
                }
                _ => {
                    return Err(SqlError::invalid_parameter_value(format!(
                        "Cannot use \"{algorithm}\": No such hash algorithm"
                    )));
                }
            };
            Ok(Some(SqlValue::String(format_bytea_hex(&digest))))
        }
        "reverse" => {
            require_arg_count(name, args, 1)?;
            let mut bytes = bytea_argument(&args[0])?.unwrap();
            bytes.reverse();
            Ok(Some(SqlValue::String(format_bytea_hex(&bytes))))
        }
        "md5" | "sha224" | "sha256" | "sha384" | "sha512" => {
            require_arg_count(name, args, 1)?;
            let bytes = bytea_argument(&args[0])?.unwrap();
            let digest = match name {
                "md5" => format!("{:x}", Md5::digest(bytes)),
                "sha224" => format_bytea_hex(&Sha224::digest(bytes)),
                "sha256" => format_bytea_hex(&Sha256::digest(bytes)),
                "sha384" => format_bytea_hex(&Sha384::digest(bytes)),
                "sha512" => format_bytea_hex(&Sha512::digest(bytes)),
                _ => unreachable!(),
            };
            Ok(Some(SqlValue::String(digest)))
        }
        "crc32" => {
            require_arg_count(name, args, 1)?;
            let bytes = bytea_argument(&args[0])?.unwrap();
            Ok(Some(SqlValue::Int(i64::from(crc32fast::hash(&bytes)))))
        }
        "crc32c" => {
            require_arg_count(name, args, 1)?;
            let bytes = bytea_argument(&args[0])?.unwrap();
            Ok(Some(SqlValue::Int(i64::from(crc32c(&bytes)))))
        }
        _ => Ok(None),
    }
}

pub(crate) fn eval_compatibility_function_value_with_db(
    db: &BicDb,
    name: &str,
    args: &[SqlValue],
    arg_types: Option<&[Option<String>]>,
) -> Result<Option<SqlValue>> {
    if matches!(name, "format" | "pg_catalog.format") {
        let mut rendered = args.to_vec();
        if let Some(types) = arg_types {
            for (value, pg_type) in rendered.iter_mut().zip(types) {
                if let Some(pg_type) = pg_type.as_deref().filter(|name| is_oid_alias_type(name)) {
                    if !matches!(value, SqlValue::Null) {
                        *value = SqlValue::String(render_oid_alias_value(db, pg_type, value)?);
                    }
                }
            }
        }
        return Ok(Some(eval_pg_format(&rendered)?));
    }
    eval_compatibility_function_value(name, args, arg_types)
}

pub(crate) fn eval_compatibility_function_value(
    name: &str,
    args: &[SqlValue],
    arg_types: Option<&[Option<String>]>,
) -> Result<Option<SqlValue>> {
    if arg_types.is_some_and(|arg_types| geometric_function_pg_type(name, arg_types).is_some()) {
        return Ok(None);
    }
    if name == "bicdb_variadic_call" {
        if args.len() < 2 {
            return Err(SqlError::InvalidSql(
                "VARIADIC call requires a function name and array argument".to_string(),
            ));
        }
        let target = args[0].to_cell().to_ascii_lowercase();
        if matches!(args.last(), Some(SqlValue::Null)) {
            return Ok(Some(SqlValue::Null));
        }
        let (array, _) = array_json_parts(args.last().unwrap(), "VARIADIC")?.ok_or_else(|| {
            SqlError::InvalidSql("VARIADIC argument must be an array".to_string())
        })?;
        let mut flattened = Vec::new();
        flatten_array_json(array, &mut flattened);
        let mut expanded = args[1..args.len() - 1].to_vec();
        expanded.extend(flattened.into_iter().map(json_to_sql_value));
        return eval_compatibility_function_value(&target, &expanded, None);
    }
    if let Some(value) = eval_bit_function_value(name, args, arg_types)? {
        return Ok(Some(value));
    }
    if let Some(value) = eval_bytea_function_value(name, args, arg_types)? {
        return Ok(Some(value));
    }
    if let Some(value) = eval_polymorphic_array_function_value(name, args)? {
        return Ok(Some(value));
    }
    match name {
        "uuidv4"
        | "pg_catalog.uuidv4"
        | "gen_random_uuid"
        | "pg_catalog.gen_random_uuid"
        | "uuid_generate_v4"
        | "public.uuid_generate_v4" => {
            require_arg_count(name, args, 0)?;
            Ok(Some(SqlValue::String(uuid::Uuid::new_v4().to_string())))
        }
        "uuidv7" | "pg_catalog.uuidv7" => eval_uuid_v7(args).map(Some),
        "uuid_extract_version" | "pg_catalog.uuid_extract_version" => {
            require_arg_count("uuid_extract_version", args, 1)?;
            if matches!(args[0], SqlValue::Null) {
                return Ok(Some(SqlValue::Null));
            }
            let bytes = parse_postgres_uuid(&args[0].to_cell()).map_err(|_| {
                SqlError::invalid_text_representation("uuid", format!("\"{}\"", args[0].to_cell()))
            })?;
            Ok(Some(if uuid_has_rfc_variant(&bytes) {
                SqlValue::Int(i64::from(bytes[6] >> 4))
            } else {
                SqlValue::Null
            }))
        }
        "uuid_extract_timestamp" | "pg_catalog.uuid_extract_timestamp" => {
            require_arg_count("uuid_extract_timestamp", args, 1)?;
            if matches!(args[0], SqlValue::Null) {
                return Ok(Some(SqlValue::Null));
            }
            let bytes = parse_postgres_uuid(&args[0].to_cell()).map_err(|_| {
                SqlError::invalid_text_representation("uuid", format!("\"{}\"", args[0].to_cell()))
            })?;
            Ok(Some(
                uuid_timestamp(&bytes)
                    .map(render_timestamptz)
                    .map(SqlValue::String)
                    .unwrap_or(SqlValue::Null),
            ))
        }
        "abs" | "pg_catalog.abs" => {
            require_arg_count("abs", args, 1)?;
            if matches!(args[0], SqlValue::Null) {
                return Ok(Some(SqlValue::Null));
            }
            let pg_type = arg_types
                .and_then(|types| types.first())
                .and_then(Option::as_deref);
            let value = match pg_type {
                Some("numeric") => {
                    let absolute = match pg_numeric_from_sql_value(args[0].clone())? {
                        PgNumeric::Finite {
                            coefficient,
                            display_scale,
                            ..
                        } => PgNumeric::finite(false, coefficient, display_scale)
                            .expect("existing PgNumeric values remain canonical"),
                        PgNumeric::NegativeInfinity => PgNumeric::PositiveInfinity,
                        value => value,
                    };
                    SqlValue::String(absolute.to_decimal_text())
                }
                Some(pg_type @ ("int2" | "int4" | "int8")) => {
                    let value = sql_value_i64(&args[0]).ok_or_else(|| {
                        SqlError::InvalidSql(format!(
                            "abs expects numeric, got {}",
                            args[0].to_cell()
                        ))
                    })?;
                    let absolute = value
                        .checked_abs()
                        .map(SqlValue::Int)
                        .ok_or_else(|| integer_out_of_range(pg_type))?;
                    enforce_integer_value_type(absolute, Some(pg_type))?
                }
                Some("float4" | "float8") => {
                    let value = sql_value_f64(&args[0]).ok_or_else(|| {
                        SqlError::InvalidSql(format!(
                            "abs expects numeric, got {}",
                            args[0].to_cell()
                        ))
                    })?;
                    SqlValue::Float(value.abs())
                }
                _ => match &args[0] {
                    SqlValue::Int(value) => value
                        .checked_abs()
                        .map(SqlValue::Int)
                        .ok_or_else(|| integer_out_of_range("int8"))?,
                    SqlValue::Float(value) => SqlValue::Float(value.abs()),
                    SqlValue::String(value) if is_valid_numeric_text(value) => {
                        let absolute = match pg_numeric_from_sql_value(args[0].clone())? {
                            PgNumeric::Finite {
                                coefficient,
                                display_scale,
                                ..
                            } => PgNumeric::finite(false, coefficient, display_scale)
                                .expect("existing PgNumeric values remain canonical"),
                            PgNumeric::NegativeInfinity => PgNumeric::PositiveInfinity,
                            value => value,
                        };
                        SqlValue::String(absolute.to_decimal_text())
                    }
                    value => {
                        return Err(SqlError::InvalidSql(format!(
                            "abs expects numeric, got {}",
                            value.to_cell()
                        )));
                    }
                },
            };
            Ok(Some(value))
        }
        "random" | "pg_catalog.random" => {
            require_arg_count("random", args, 0)?;
            Ok(Some(SqlValue::Float(sql_random_value())))
        }
        "trunc" | "pg_catalog.trunc" => {
            require_arg_count("trunc", args, 1)?;
            let value = sql_value_f64(&args[0]).ok_or_else(|| {
                SqlError::InvalidSql(format!("trunc expects numeric, got {}", args[0].to_cell()))
            })?;
            Ok(Some(SqlValue::Float(value.trunc())))
        }
        "round" | "pg_catalog.round" => {
            // round(numeric) and round(numeric, scale). The two-argument form
            // is exact numeric rounding, half away from zero, like PostgreSQL.
            if args.len() == 2 {
                if matches!(args[0], SqlValue::Null) || matches!(args[1], SqlValue::Null) {
                    return Ok(Some(SqlValue::Null));
                }
                let scale = sql_value_i64(&args[1]).ok_or_else(|| {
                    SqlError::InvalidSql(format!(
                        "round expects an integer scale, got {}",
                        args[1].to_cell()
                    ))
                })?;
                let value = crate::eval::operators_compare::decimal_value(&args[0]).ok_or_else(
                    || {
                        SqlError::InvalidSql(format!(
                            "round expects numeric, got {}",
                            args[0].to_cell()
                        ))
                    },
                )??;
                return Ok(Some(SqlValue::String(round_decimal_half_away(
                    value, scale,
                ))));
            }
            require_arg_count("round", args, 1)?;
            if matches!(args[0], SqlValue::Null) {
                return Ok(Some(SqlValue::Null));
            }
            let value = sql_value_f64(&args[0]).ok_or_else(|| {
                SqlError::InvalidSql(format!("round expects numeric, got {}", args[0].to_cell()))
            })?;
            Ok(Some(SqlValue::Float(value.round())))
        }
        "md5" | "pg_catalog.md5" => {
            require_arg_count("md5", args, 1)?;
            Ok(Some(if matches!(args[0], SqlValue::Null) {
                SqlValue::Null
            } else {
                SqlValue::String(format!("{:x}", Md5::digest(args[0].to_cell().as_bytes())))
            }))
        }
        "mod" | "pg_catalog.mod" => {
            require_arg_count("mod", args, 2)?;
            let left = sql_value_f64(&args[0]).ok_or_else(|| {
                SqlError::InvalidSql(format!("mod expects numeric, got {}", args[0].to_cell()))
            })?;
            let right = sql_value_f64(&args[1]).ok_or_else(|| {
                SqlError::InvalidSql(format!("mod expects numeric, got {}", args[1].to_cell()))
            })?;
            if right == 0.0 {
                return Err(SqlError::InvalidSql("division by zero".to_string()));
            }
            Ok(Some(
                if matches!((&args[0], &args[1]), (SqlValue::Int(_), SqlValue::Int(_))) {
                    SqlValue::Int(
                        sql_value_i64(&args[0]).unwrap() % sql_value_i64(&args[1]).unwrap(),
                    )
                } else {
                    SqlValue::Float(left % right)
                },
            ))
        }
        "concat" | "pg_catalog.concat" => Ok(Some(SqlValue::String(
            args.iter()
                .filter(|value| !matches!(value, SqlValue::Null))
                .map(SqlValue::to_cell)
                .collect::<String>(),
        ))),
        "concat_ws" | "pg_catalog.concat_ws" => {
            let Some(separator) = args.first() else {
                return Err(SqlError::InvalidSql(
                    "concat_ws expects a separator argument".to_string(),
                ));
            };
            if matches!(separator, SqlValue::Null) {
                return Ok(Some(SqlValue::Null));
            }
            Ok(Some(SqlValue::String(
                args[1..]
                    .iter()
                    .filter(|value| !matches!(value, SqlValue::Null))
                    .map(SqlValue::to_cell)
                    .collect::<Vec<_>>()
                    .join(&separator.to_cell()),
            )))
        }
        "chr" | "pg_catalog.chr" => {
            require_arg_count("chr", args, 1)?;
            let code = sql_value_i64(&args[0]).ok_or_else(|| {
                SqlError::invalid_parameter_value(
                    "requested character is not valid for encoding UTF8",
                )
            })?;
            if code == 0 {
                return Err(SqlError::data_exception(
                    "54000",
                    "null character not permitted",
                    Some("text".to_string()),
                ));
            }
            let character = u32::try_from(code)
                .ok()
                .and_then(char::from_u32)
                .ok_or_else(|| {
                    SqlError::invalid_parameter_value(
                        "requested character is not valid for encoding UTF8",
                    )
                })?;
            Ok(Some(SqlValue::String(character.to_string())))
        }
        "format" | "pg_catalog.format" => Ok(Some(eval_pg_format(args)?)),
        "to_char" | "pg_catalog.to_char" => Ok(Some(eval_to_char(args)?)),
        "char_length"
        | "pg_catalog.char_length"
        | "character_length"
        | "pg_catalog.character_length"
        | "length"
        | "pg_catalog.length" => {
            require_arg_count("char_length", args, 1)?;
            Ok(Some(text_length_value(&args[0], false)))
        }
        "octet_length" | "pg_catalog.octet_length" => {
            require_arg_count("octet_length", args, 1)?;
            Ok(Some(text_length_value(&args[0], true)))
        }
        "substr" | "pg_catalog.substr" | "substring" | "pg_catalog.substring" => {
            Ok(Some(substr_value(args)?))
        }
        "coalesce" | "pg_catalog.coalesce" => Ok(Some(
            args.iter()
                .find(|value| !matches!(value, SqlValue::Null))
                .cloned()
                .unwrap_or(SqlValue::Null),
        )),
        "greatest" | "pg_catalog.greatest" => Ok(Some(extreme_value(args, true)?)),
        "least" | "pg_catalog.least" => Ok(Some(extreme_value(args, false)?)),
        "trim" | "pg_catalog.trim" | "btrim" | "pg_catalog.btrim" => {
            if args.is_empty() || args.len() > 2 {
                return Err(SqlError::InvalidSql(format!(
                    "trim expects 1 or 2 argument(s), got {}",
                    args.len()
                )));
            }
            if matches!(args[0], SqlValue::Null) {
                return Ok(Some(SqlValue::Null));
            }
            let value = args[0].to_cell();
            let trimmed = if let Some(characters) = args.get(1) {
                value
                    .trim_matches(|ch| characters.to_cell().contains(ch))
                    .to_string()
            } else {
                value.trim().to_string()
            };
            Ok(Some(SqlValue::String(trimmed)))
        }
        "lower" | "pg_catalog.lower" => {
            require_arg_count("lower", args, 1)?;
            if matches!(args[0], SqlValue::Null) {
                return Ok(Some(SqlValue::Null));
            }
            Ok(Some(SqlValue::String(
                args[0].to_cell().to_ascii_lowercase(),
            )))
        }
        "upper" | "pg_catalog.upper" => {
            require_arg_count("upper", args, 1)?;
            if matches!(args[0], SqlValue::Null) {
                return Ok(Some(SqlValue::Null));
            }
            Ok(Some(SqlValue::String(
                args[0].to_cell().to_ascii_uppercase(),
            )))
        }
        "initcap" | "pg_catalog.initcap" => {
            require_arg_count("initcap", args, 1)?;
            if matches!(args[0], SqlValue::Null) {
                return Ok(Some(SqlValue::Null));
            }
            let value = args[0].to_cell();
            let mut result = String::with_capacity(value.len());
            let mut inside_word = false;
            for character in value.chars() {
                if character.is_alphanumeric() {
                    if inside_word {
                        result.extend(character.to_lowercase());
                    } else {
                        result.extend(character.to_uppercase());
                        inside_word = true;
                    }
                } else {
                    result.push(character);
                    inside_word = false;
                }
            }
            Ok(Some(SqlValue::String(result)))
        }
        "nullif" | "pg_catalog.nullif" => {
            require_arg_count("nullif", args, 2)?;
            Ok(Some(if values_equal(&args[0], &args[1]) {
                SqlValue::Null
            } else {
                args[0].clone()
            }))
        }
        "justify_hours"
        | "pg_catalog.justify_hours"
        | "justify_days"
        | "pg_catalog.justify_days"
        | "justify_interval"
        | "pg_catalog.justify_interval" => {
            require_arg_count(name, args, 1)?;
            if matches!(args[0], SqlValue::Null) {
                return Ok(Some(SqlValue::Null));
            }
            let text = args[0].to_cell();
            let interval = PgInterval::from_postgres_text(&text)
                .map_err(|error| postgres_interval_input_error(&text, error))?;
            let interval = match name.strip_prefix("pg_catalog.").unwrap_or(name) {
                "justify_hours" => interval.justify_hours(),
                "justify_days" => interval.justify_days(),
                _ => interval.justify_interval(),
            }
            .map_err(|error| postgres_interval_input_error(&text, error))?;
            Ok(Some(SqlValue::String(render_interval(interval))))
        }
        "date_trunc" | "pg_catalog.date_trunc" => {
            require_arg_count("date_trunc", args, 2)?;
            if args.iter().any(|value| matches!(value, SqlValue::Null)) {
                return Ok(Some(SqlValue::Null));
            }
            let field = args[0].to_cell();
            let timestamp_text = args[1].to_cell();
            if arg_types
                .and_then(|types| types.get(1))
                .and_then(Option::as_deref)
                == Some("timestamptz")
            {
                let timestamp = parse_timestamptz(&timestamp_text)
                    .map_err(|error| postgres_timestamptz_input_error(&timestamp_text, error))?;
                let Some(micros) = timestamp.finite_micros() else {
                    return Ok(Some(SqlValue::String(render_timestamptz(timestamp))));
                };
                let timezone = current_timezone_name();
                let offset =
                    timezone_offset_at_timestamp(&timezone, timestamp).ok_or_else(|| {
                        SqlError::invalid_parameter_value(format!(
                            "time zone \"{timezone}\" not recognized"
                        ))
                    })?;
                let local = PgTimestamp::Finite(
                    micros
                        .checked_add(i64::from(offset) * 1_000_000)
                        .ok_or_else(|| {
                            SqlError::data_exception("22008", "timestamptz out of range", None)
                        })?,
                );
                let truncated = local.truncate(&field).map_err(|_| {
                    SqlError::data_exception(
                        "22023",
                        format!(
                            "unit \"{field}\" not recognized for type timestamp with time zone"
                        ),
                        Some("timestamptz".to_string()),
                    )
                })?;
                let instant = parse_timestamptz_in_zone(&truncated.to_iso_text(false), &timezone)
                    .map_err(|error| {
                    postgres_timestamptz_input_error(&timestamp_text, error)
                })?;
                return Ok(Some(SqlValue::String(render_timestamptz(instant))));
            }
            let timestamp = PgTimestamp::from_postgres_text(&timestamp_text, false)
                .map_err(|error| postgres_timestamp_input_error(&timestamp_text, error))?;
            let truncated = timestamp.truncate(&field).map_err(|_| {
                SqlError::data_exception(
                    "22023",
                    format!("unit \"{field}\" not recognized for type timestamp"),
                    Some("timestamp".to_string()),
                )
            })?;
            Ok(Some(SqlValue::String(truncated.to_iso_text(false))))
        }
        "date_part" | "pg_catalog.date_part" | "extract" | "pg_catalog.extract" => {
            require_arg_count("date_part", args, 2)?;
            let field = args[0].to_cell().to_ascii_lowercase();
            let value = args[1].to_cell();
            if arg_types
                .and_then(|types| types.get(1))
                .and_then(Option::as_deref)
                == Some("interval")
            {
                let interval = PgInterval::from_postgres_text(&value)
                    .map_err(|error| postgres_interval_input_error(&value, error))?;
                let part = interval.extract_field(&field).ok_or_else(|| {
                    SqlError::Unsupported(format!("date_part field {field} is not supported"))
                })?;
                return part
                    .parse::<f64>()
                    .map(SqlValue::Float)
                    .map(Some)
                    .map_err(|_| {
                        SqlError::invalid_datetime_format(format!(
                            "cannot extract {field} from interval"
                        ))
                    });
            }
            if field == "epoch"
                && arg_types
                    .and_then(|types| types.get(1))
                    .and_then(Option::as_deref)
                    == Some("timestamptz")
            {
                let timestamp = parse_timestamptz(&value)
                    .map_err(|error| postgres_timestamptz_input_error(&value, error))?;
                let part = timestamp.extract_field(&field).ok_or_else(|| {
                    SqlError::Unsupported(format!("date_part field {field} is not supported"))
                })?;
                return Ok(Some(part.parse::<i64>().map(SqlValue::Int).unwrap_or_else(
                    |_| SqlValue::Float(part.parse().unwrap_or(f64::NAN)),
                )));
            }
            if let Ok(timestamp) = PgTimestamp::from_postgres_text(&value, false) {
                let part = timestamp.extract_field(&field).ok_or_else(|| {
                    SqlError::Unsupported(format!("date_part field {field} is not supported"))
                })?;
                let value = part.parse::<i64>().map(SqlValue::Int).unwrap_or_else(|_| {
                    part.parse::<f64>()
                        .map(SqlValue::Float)
                        .unwrap_or_else(|_| SqlValue::String(part))
                });
                return Ok(Some(value));
            }
            let part = match field.as_str() {
                "year" => parse_date_prefix(&value).map(|(year, _, _)| year as i64),
                "month" => parse_date_prefix(&value).map(|(_, month, _)| month as i64),
                "day" => parse_date_prefix(&value)
                    .map(|(_, _, day)| day as i64)
                    .or_else(|| parse_interval_seconds(&value).map(|seconds| seconds / 86_400)),
                "hour" => parse_time_prefix(&value)
                    .map(|(hour, _, _)| hour as i64)
                    .or_else(|| {
                        parse_interval_seconds(&value).map(|seconds| (seconds % 86_400) / 3_600)
                    }),
                "minute" => parse_time_prefix(&value)
                    .map(|(_, minute, _)| minute as i64)
                    .or_else(|| {
                        parse_interval_seconds(&value).map(|seconds| (seconds % 3_600) / 60)
                    }),
                "second" => parse_time_prefix(&value)
                    .map(|(_, _, second)| second as i64)
                    .or_else(|| parse_interval_seconds(&value).map(|seconds| seconds % 60)),
                _ => {
                    return Err(SqlError::Unsupported(format!(
                        "date_part field {field} is not supported"
                    )));
                }
            };
            Ok(Some(part.map(SqlValue::Int).unwrap_or(SqlValue::Null)))
        }
        _ => Ok(None),
    }
}

pub(crate) fn eval_uuid_v7(args: &[SqlValue]) -> Result<SqlValue> {
    if args.len() > 1 {
        return Err(SqlError::InvalidSql(format!(
            "uuidv7 expects 0 or 1 arguments, got {}",
            args.len()
        )));
    }
    if args
        .first()
        .is_some_and(|value| matches!(value, SqlValue::Null))
    {
        return Ok(SqlValue::Null);
    }
    if args.is_empty() {
        return Ok(SqlValue::String(uuid::Uuid::now_v7().to_string()));
    }

    let interval_text = args[0].to_cell();
    let interval = PgInterval::from_postgres_text(&interval_text)
        .map_err(|error| postgres_interval_input_error(&interval_text, error))?;
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SqlError::InvalidSql("UUIDv7 timestamp is too small".to_string()))?;
    let unix_micros = i64::try_from(duration.as_micros())
        .map_err(|_| SqlError::InvalidSql("UUIDv7 timestamp is too large".to_string()))?;
    let shifted = PgTimestamp::Finite(unix_micros - 946_684_800_000_000)
        .checked_add_interval(interval)
        .map_err(|_| SqlError::InvalidSql("UUIDv7 timestamp is out of range".to_string()))?;
    let shifted_unix_micros = shifted
        .finite_micros()
        .and_then(|micros| micros.checked_add(946_684_800_000_000))
        .filter(|micros| *micros >= 0)
        .ok_or_else(|| SqlError::InvalidSql("UUIDv7 timestamp is too small".to_string()))?;
    let seconds = u64::try_from(shifted_unix_micros / 1_000_000)
        .map_err(|_| SqlError::InvalidSql("UUIDv7 timestamp is out of range".to_string()))?;
    let nanos = u32::try_from(shifted_unix_micros % 1_000_000).unwrap_or_default() * 1_000;
    let id = SQL_UUID_V7_CONTEXT
        .with(|context| uuid::Uuid::new_v7(uuid::Timestamp::from_unix(context, seconds, nanos)));
    Ok(SqlValue::String(id.to_string()))
}

pub(crate) fn uuid_has_rfc_variant(bytes: &[u8; 16]) -> bool {
    bytes[8] & 0xc0 == 0x80
}

pub(crate) fn uuid_timestamp(bytes: &[u8; 16]) -> Option<PgTimestamp> {
    if !uuid_has_rfc_variant(bytes) {
        return None;
    }
    let unix_micros = match bytes[6] >> 4 {
        1 => {
            let time_low = u64::from(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]));
            let time_mid = u64::from(u16::from_be_bytes([bytes[4], bytes[5]]));
            let time_high = u64::from(u16::from_be_bytes([bytes[6], bytes[7]]) & 0x0fff);
            let ticks = time_low | (time_mid << 32) | (time_high << 48);
            let unix_ticks = i128::from(ticks) - 0x01b2_1dd2_1381_4000_i128;
            i64::try_from(unix_ticks.div_euclid(10)).ok()?
        }
        7 => {
            let milliseconds = u64::from_be_bytes([
                0, 0, bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5],
            ]);
            i64::try_from(milliseconds).ok()?.checked_mul(1_000)?
        }
        _ => return None,
    };
    Some(PgTimestamp::Finite(
        unix_micros.checked_sub(946_684_800_000_000)?,
    ))
}

pub(crate) fn eval_pg_format(args: &[SqlValue]) -> Result<SqlValue> {
    let Some(template) = args.first().and_then(sql_value_text) else {
        return Ok(SqlValue::Null);
    };
    let mut rendered = String::with_capacity(template.len());
    let mut values = args[1..].iter();
    let mut chars = template.chars();
    while let Some(ch) = chars.next() {
        if ch != '%' {
            rendered.push(ch);
            continue;
        }
        let specifier = chars.next().ok_or_else(|| {
            SqlError::InvalidSql("unterminated format() conversion specifier".to_string())
        })?;
        if specifier == '%' {
            rendered.push('%');
            continue;
        }
        let value = values
            .next()
            .ok_or_else(|| SqlError::InvalidSql("too few arguments for format()".to_string()))?;
        match specifier {
            'I' => {
                let value = sql_value_text(value).ok_or_else(|| {
                    SqlError::InvalidSql(
                        "null values cannot be formatted as an SQL identifier".to_string(),
                    )
                })?;
                rendered.push_str(&pg_quote_ident(&value));
            }
            'L' => {
                if matches!(value, SqlValue::Null) {
                    rendered.push_str("NULL");
                } else {
                    rendered.push_str(&pg_quote_literal(&value.to_cell()));
                }
            }
            's' => {
                if !matches!(value, SqlValue::Null) {
                    rendered.push_str(&value.to_cell());
                }
            }
            other => {
                return Err(SqlError::Unsupported(format!(
                    "format() conversion %{other} is not supported"
                )));
            }
        }
    }
    Ok(SqlValue::String(rendered))
}

pub(crate) fn extreme_value(args: &[SqlValue], greatest: bool) -> Result<SqlValue> {
    if args.is_empty() {
        return Err(SqlError::InvalidSql(format!(
            "{} expects at least one argument",
            if greatest { "greatest" } else { "least" }
        )));
    }
    let mut selected: Option<SqlValue> = None;
    for value in args.iter().filter(|value| !matches!(value, SqlValue::Null)) {
        let Some(current) = selected.as_ref() else {
            selected = Some(value.clone());
            continue;
        };
        let ordering = value_ordering(value, current).unwrap_or(Ordering::Equal);
        if (greatest && ordering == Ordering::Greater) || (!greatest && ordering == Ordering::Less)
        {
            selected = Some(value.clone());
        }
    }
    Ok(selected.unwrap_or(SqlValue::Null))
}

pub(crate) fn text_length_value(value: &SqlValue, octets: bool) -> SqlValue {
    if matches!(value, SqlValue::Null) {
        return SqlValue::Null;
    }
    if octets {
        let len = match value {
            SqlValue::Json(JsonValue::Array(values)) => values.len(),
            other => other.to_cell().len(),
        };
        SqlValue::Int(len as i64)
    } else {
        SqlValue::Int(value.to_cell().chars().count() as i64)
    }
}

pub(crate) fn eval_to_char(args: &[SqlValue]) -> Result<SqlValue> {
    require_arg_count("to_char", args, 2)?;
    if args.iter().any(|value| matches!(value, SqlValue::Null)) {
        return Ok(SqlValue::Null);
    }
    let format = sql_value_text(&args[1])
        .ok_or_else(|| SqlError::InvalidSql("to_char format must be text".to_string()))?;
    let input = args[0].to_cell();
    if parse_date_prefix(&input).is_some() {
        return format_timestamp_to_char(&input, &format).map(SqlValue::String);
    }
    let numeric = sql_value_f64(&args[0]).ok_or_else(|| {
        SqlError::Unsupported(format!("to_char does not support {}", args[0].to_cell()))
    })?;
    format_numeric_to_char(numeric, &format).map(SqlValue::String)
}

pub(crate) fn format_timestamp_to_char(value: &str, format: &str) -> Result<String> {
    let (year, month, day) = parse_date_prefix(value)
        .ok_or_else(|| SqlError::InvalidSql(format!("to_char cannot parse timestamp {}", value)))?;
    let (hour, minute, second) = parse_time_prefix(value).unwrap_or((0, 0, 0));
    let format = format.to_ascii_uppercase();
    let mut output = String::new();
    let mut idx = 0;
    while idx < format.len() {
        if format[idx..].starts_with("YYYY") {
            output.push_str(&format!("{year:04}"));
            idx += 4;
        } else if format[idx..].starts_with("HH24") {
            output.push_str(&format!("{hour:02}"));
            idx += 4;
        } else if format[idx..].starts_with("YY") {
            output.push_str(&format!("{:02}", year.rem_euclid(100)));
            idx += 2;
        } else if format[idx..].starts_with("MM") {
            output.push_str(&format!("{month:02}"));
            idx += 2;
        } else if format[idx..].starts_with("DD") {
            output.push_str(&format!("{day:02}"));
            idx += 2;
        } else if format[idx..].starts_with("MI") {
            output.push_str(&format!("{minute:02}"));
            idx += 2;
        } else if format[idx..].starts_with("SS") {
            output.push_str(&format!("{second:02}"));
            idx += 2;
        } else {
            let ch = format[idx..].chars().next().unwrap();
            if ch.is_ascii_alphabetic() {
                return Err(SqlError::Unsupported(format!(
                    "to_char timestamp format token {} is not supported",
                    format[idx..]
                        .chars()
                        .take_while(|ch| ch.is_ascii_alphabetic() || ch.is_ascii_digit())
                        .collect::<String>()
                )));
            }
            output.push(ch);
            idx += ch.len_utf8();
        }
    }
    Ok(output)
}

pub(crate) fn format_numeric_to_char(value: f64, format: &str) -> Result<String> {
    if !value.is_finite() {
        return Err(SqlError::InvalidSql(
            "to_char numeric value is not finite".to_string(),
        ));
    }
    let (left, right) = format.split_once('.').unwrap_or((format, ""));
    if !left.chars().all(|ch| ch == '9') || !right.chars().all(|ch| ch == '9') {
        return Err(SqlError::Unsupported(format!(
            "to_char numeric format {format} is not supported"
        )));
    }
    let left_width = left.chars().count();
    let right_width = right.chars().count();
    let rendered = format!("{:.*}", right_width, value.abs());
    let (integer, fraction) = rendered.split_once('.').unwrap_or((rendered.as_str(), ""));
    let integer = if integer == "0" { "" } else { integer };
    let integer = if value.is_sign_negative() && !integer.is_empty() {
        format!("-{integer}")
    } else {
        integer.to_string()
    };
    let integer_width = left_width + 1;
    if integer.chars().count() > integer_width {
        let overflow_len = integer_width + usize::from(right_width > 0) + right_width;
        return Ok("#".repeat(overflow_len));
    }
    let mut output = format!("{integer:>integer_width$}");
    if right_width > 0 {
        output.push('.');
        output.push_str(fraction);
    }
    Ok(output)
}
