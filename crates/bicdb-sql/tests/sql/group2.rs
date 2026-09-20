//! Test group split from the former monolithic tests/sql.rs.
use super::*;

#[test]
fn positional_parameters_work_in_trimmed_scalars_and_aggregate_projections() {
    let (_dir, mut db) = empty_test_db();
    let scalar = SqlSession::new(&mut db)
        .with_positional_parameters(vec![SqlValue::String("  User@Example.COM  ".to_string())])
        .execute("SELECT lower(trim($1::text))")
        .unwrap();
    assert_eq!(
        scalar.rows,
        vec![vec![SqlValue::String("user@example.com".to_string())]]
    );

    SqlSession::new(&mut db)
        .execute(
            "CREATE TABLE positional_revenue (
                id TEXT PRIMARY KEY,
                amount_cents BIGINT,
                entry_type TEXT
            )",
        )
        .unwrap();
    let aggregate = SqlSession::new(&mut db)
        .with_positional_parameters(vec![
            SqlValue::String("tenant-a".to_string()),
            SqlValue::String("usd".to_string()),
        ])
        .execute(
            "SELECT $1 AS org_id,
                    upper($2) AS currency,
                    COALESCE(SUM(amount_cents) FILTER (WHERE entry_type = 'charge'), 0)::bigint AS gross
             FROM positional_revenue",
        )
        .unwrap();
    assert_eq!(
        aggregate.rows,
        vec![vec![
            SqlValue::String("tenant-a".to_string()),
            SqlValue::String("USD".to_string()),
            SqlValue::Int(0),
        ]]
    );
}

#[test]
fn initcap_matches_postgres_for_constants_and_table_projections() {
    let (_dir, mut db) = empty_test_db();
    let constants = SqlSession::new(&mut db)
        .execute(
            "SELECT initcap('hi THOMAS'),
                    initcap('foo-bar BAZ_qux'),
                    initcap('123abc ABC123'),
                    initcap('élAN ÜBER'),
                    initcap(NULL::text)",
        )
        .unwrap();
    assert_eq!(
        constants.rows,
        vec![vec![
            SqlValue::String("Hi Thomas".to_string()),
            SqlValue::String("Foo-Bar Baz_Qux".to_string()),
            SqlValue::String("123abc Abc123".to_string()),
            SqlValue::String("Élan Über".to_string()),
            SqlValue::Null,
        ]]
    );

    SqlSession::new(&mut db)
        .execute(
            "CREATE TABLE appointments (
                id TEXT PRIMARY KEY,
                appointment_type TEXT NOT NULL,
                service_line TEXT
            );
            INSERT INTO appointments (id, appointment_type, service_line)
            VALUES ('appointment-1', 'FOLLOW_UP', 'Functional Medicine')",
        )
        .unwrap();
    let projection = SqlSession::new(&mut db)
        .execute(
            "SELECT INITCAP(REPLACE(a.appointment_type::text, '_', ' '))
                    || COALESCE(' · ' || NULLIF(a.service_line, ''), '') AS title
             FROM appointments a",
        )
        .unwrap();
    assert_eq!(
        projection.rows,
        vec![vec![SqlValue::String(
            "Follow Up · Functional Medicine".to_string()
        )]]
    );
}

#[test]
fn positional_parameters_work_in_case_predicates() {
    let (_dir, mut db) = empty_test_db();
    let result = SqlSession::new(&mut db)
        .with_positional_parameters(vec![
            SqlValue::String("checked_in".to_string()),
            SqlValue::String("ready".to_string()),
        ])
        .execute(
            "SELECT CASE
                 WHEN $1 = $2 THEN true
                 WHEN $2 IN ('cancelled', 'no_show', 'entered_in_error')
                   AND $1 NOT IN ('completed', 'cancelled', 'no_show', 'entered_in_error')
                 THEN true
                 WHEN $1 = 'checked_in' AND $2 IN ('triage', 'ready') THEN true
                 ELSE false
             END",
        )
        .unwrap();
    assert_eq!(result.rows, vec![vec![SqlValue::Bool(true)]]);
}

#[test]
fn positional_timestamp_parameter_uses_case_update_column_context() {
    let (_dir, mut db) = empty_test_db();
    SqlSession::new(&mut db)
        .execute(
            "CREATE TABLE clinic_queue_entries (
                id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                ready_at TIMESTAMPTZ
            )",
        )
        .unwrap();
    SqlSession::new(&mut db)
        .execute("INSERT INTO clinic_queue_entries (id, status) VALUES ('queue-1', 'checked_in')")
        .unwrap();

    SqlSession::new(&mut db)
        .with_positional_parameters(vec![
            SqlValue::String("queue-1".to_string()),
            SqlValue::String("ready".to_string()),
            SqlValue::String("2026-08-28T21:14:54.083Z".to_string()),
        ])
        .execute(
            "UPDATE clinic_queue_entries q
             SET status = $2,
                 ready_at = CASE
                   WHEN $2 = 'ready' THEN COALESCE(q.ready_at, $3, NOW())
                   ELSE q.ready_at
                 END
             WHERE q.id = $1",
        )
        .unwrap();

    let selected = SqlSession::new(&mut db)
        .execute("SELECT status, ready_at FROM clinic_queue_entries WHERE id = 'queue-1'")
        .unwrap();
    assert_eq!(selected.rows[0][0], SqlValue::String("ready".to_string()));
    assert!(!matches!(selected.rows[0][1], SqlValue::Null));
}

#[test]
fn null_timestamp_parameter_uses_unqualified_column_context_in_nested_case_coalesce() {
    let (_dir, mut db) = empty_test_db();
    SqlSession::new(&mut db)
        .execute(
            "CREATE TABLE patient_medications (
                id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                end_at TIMESTAMPTZ
            );
            INSERT INTO patient_medications (id, status)
            VALUES ('medication-1', 'active')",
        )
        .unwrap();

    let sql = "UPDATE patient_medications
               SET status = $1,
                   end_at = CASE
                     WHEN $1::text IN ('completed', 'stopped', 'not_taken', 'entered_in_error')
                       THEN COALESCE($2, end_at, NOW())
                     WHEN $1::text IN ('active', 'intended') THEN $2
                     ELSE COALESCE($2, end_at)
                   END
               WHERE id = $3";
    assert_eq!(
        infer_parameter_types(&db, sql).unwrap(),
        vec![
            Some("text".to_string()),
            Some("timestamptz".to_string()),
            Some("text".to_string()),
        ]
    );

    SqlSession::new(&mut db)
        .with_positional_parameters(vec![
            SqlValue::String("on_hold".to_string()),
            SqlValue::Null,
            SqlValue::String("medication-1".to_string()),
        ])
        .execute(sql)
        .unwrap();

    let selected = SqlSession::new(&mut db)
        .execute("SELECT status, end_at FROM patient_medications WHERE id = 'medication-1'")
        .unwrap();
    assert_eq!(
        selected.rows,
        vec![vec![
            SqlValue::String("on_hold".to_string()),
            SqlValue::Null,
        ]]
    );
}

#[test]
fn secure_sql_insert_enforces_unique_constraints_on_protected_tables() {
    let (_dir, mut db) = empty_test_db();
    SqlSession::new(&mut db)
        .execute(
            "CREATE TABLE protected_orgs (id UUID PRIMARY KEY, tenant_id TEXT NOT NULL, code TEXT UNIQUE)",
        )
        .unwrap();
    db.set_collection_policy("protected_orgs", tenant_policy())
        .unwrap();

    let mut session = SqlSession::new_secure(&mut db, tenant_ctx("tenant-a", &["writer"]));
    session
        .execute(
            "INSERT INTO protected_orgs (id, tenant_id, code) VALUES ('00000000-0000-0000-0000-000000000001', 'tenant-a', 'clinic-a')",
        )
        .unwrap();
    let duplicate = session
        .execute(
            "INSERT INTO protected_orgs (id, tenant_id, code) VALUES ('00000000-0000-0000-0000-000000000002', 'tenant-a', 'clinic-a')",
        )
        .unwrap_err();
    assert_eq!(duplicate.sqlstate(), "23505");
}

#[test]
fn signed_application_security_guc_setup_cannot_replace_host_authority() {
    let (_dir, mut db) = empty_test_db();
    let context = tenant_ctx("tenant-a", &["writer"]);
    let rejected = SqlSession::new_secure(&mut db, context.clone())
        .execute("SELECT set_config('carrier.current_roles', 'admin', true)")
        .unwrap_err();
    assert_eq!(rejected.sqlstate(), "42501");

    let mut session = SqlSession::new_secure(&mut db, context)
        .with_signed_application_security_guc_compatibility();
    let accepted = session
        .execute("SELECT set_config('carrier.current_roles', 'admin', true)")
        .unwrap();
    assert_eq!(
        accepted.rows,
        vec![vec![SqlValue::String("admin".to_string())]]
    );
    let retained = session
        .execute("SELECT current_setting('carrier.current_roles', true)")
        .unwrap();
    assert_eq!(
        retained.rows,
        vec![vec![SqlValue::String("writer".to_string())]]
    );
}

#[test]
fn secure_sql_rejects_plaintext_protected_data_filters_and_allows_blind_index_lookup() {
    let (_dir, mut db) = empty_test_db();
    let policy = CollectionPolicy::tenant_field("tenant_id")
        .with_read_roles(["reader"])
        .with_write_roles(["writer"])
        .with_column_security(
            "email",
            ColumnSecurity::encrypted("patient-email")
                .with_key_ref("phi-field:v1")
                .with_blind_index("patient-email:v1:")
                .with_redaction(RedactionPolicy::Null)
                .with_decrypt_roles(["phi-reader"]),
        );
    db.create_collection_with_policy("patients", CollectionMode::Standard, policy)
        .unwrap();

    std::env::set_var("PHI_FIELD_ENCRYPTION_KEY", "44".repeat(32));
    std::env::set_var("PHI_LOOKUP_HMAC_KEY", "55".repeat(32));
    let writer = tenant_ctx("tenant-a", &["writer"]);
    db.secure(&writer)
        .insert(
            "patients",
            Record::new("p-a").with_metadata(json!({
                "tenant_id": "tenant-a",
                "email": "sql-phi@example.test"
            })),
        )
        .unwrap();

    let blind = db
        .blind_index_filter(
            &tenant_ctx("tenant-a", &["reader"]),
            "patients",
            "email",
            json!("sql-phi@example.test"),
        )
        .unwrap()
        .equals_value("__bicdb_blind_index__email")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    let mut session =
        SqlSession::new_secure(&mut db, tenant_ctx("tenant-a", &["reader", "phi-reader"]));
    assert!(matches!(
        session.execute("SELECT id FROM patients WHERE email = 'sql-phi@example.test'"), // security-approved: negative test expects rejection
        Err(SqlError::BicDb(BicDbError::Authorization(_)))
    ));
    let result = session
        .execute(&format!(
            "SELECT id FROM patients WHERE __bicdb_blind_index__email = '{blind}'"
        ))
        .unwrap();
    assert_eq!(result.rows, vec![vec![SqlValue::String("p-a".to_string())]]);
}

#[test]
fn spatial_sql_astext_and_asgeojson() {
    let (_dir, db) = empty_test_db();
    let result = SqlEngine::new(&db)
        .execute("SELECT ST_AsText(ST_Point(-122.4194, 37.7749)), ST_AsGeoJSON(ST_Point(-122.4194, 37.7749))")
        .unwrap();

    assert_eq!(result.rows.len(), 1);
    let SqlValue::String(wkt) = &result.rows[0][0] else {
        panic!("expected WKT text");
    };
    assert!(wkt.starts_with("POINT"));
    assert!(wkt.contains("-122.4194"));
    assert!(wkt.contains("37.7749"));

    let SqlValue::String(geojson_text) = &result.rows[0][1] else {
        panic!("expected GeoJSON text");
    };
    let geojson: serde_json::Value = serde_json::from_str(geojson_text).unwrap();
    assert_eq!(geojson["type"], "Point");
    assert_eq!(geojson["coordinates"], json!([-122.4194, 37.7749]));
}

#[test]
fn routing_sql_shortest_path_and_route_distance_smoke() {
    let (_dir, mut db) = empty_test_db();
    insert_toy_roads(&mut db);

    let result = SqlEngine::new(&db)
        .execute(
            "SELECT shortest_path('roads', ST_Point(0, 0), ST_Point(0.002, 0)), \
             route_distance('roads', ST_Point(0, 0), ST_Point(0.002, 0))",
        )
        .unwrap();

    let SqlValue::Json(path) = &result.rows[0][0] else {
        panic!("expected JSON route path");
    };
    assert_eq!(path["node_ids"], json!(["a", "b", "c"]));
    assert_eq!(result.rows[0][1], SqlValue::Float(200.0));
}

#[test]
fn routing_sql_optimize_route_array_smoke() {
    let (_dir, db) = empty_test_db();

    let result = SqlEngine::new(&db)
        .execute(
            "SELECT optimize_route('roads', ARRAY[
                ST_Point(0, 0),
                ST_Point(4, 0),
                ST_Point(1, 0),
                ST_Point(3, 0),
                ST_Point(2, 0)
            ])",
        )
        .unwrap();

    let SqlValue::Json(route) = &result.rows[0][0] else {
        panic!("expected JSON optimized route");
    };
    assert_eq!(route["distance_mode"], json!("haversine"));
    assert_eq!(route["heuristic"], json!("nearest_neighbor_2opt"));
    assert_eq!(route["ordered_stops"].as_array().unwrap().len(), 5);
    assert!(route["estimated_distance_m"].as_f64().unwrap() > 0.0);
}

#[test]
fn spatial_sql_distance_uses_haversine_meters_for_lon_lat_points() {
    let (_dir, db) = empty_test_db();
    let result = SqlEngine::new(&db)
        .execute("SELECT ST_Distance(ST_Point(-122.4194, 37.7749), ST_Point(-118.2437, 34.0522))")
        .unwrap();

    assert_float_close(&result.rows[0][0], 559_000.0, 1_000.0);
}

#[test]
fn spatial_sql_dwithin_thresholds() {
    let (_dir, db) = empty_test_db();
    let result = SqlEngine::new(&db)
        .execute("SELECT ST_DWithin(ST_Point(-122.4194, 37.7749), ST_Point(-118.2437, 34.0522), 560000), ST_DWithin(ST_Point(-122.4194, 37.7749), ST_Point(-118.2437, 34.0522), 550000)")
        .unwrap();

    assert_eq!(
        result.rows,
        vec![vec![SqlValue::Bool(true), SqlValue::Bool(false)]]
    );
}

#[test]
fn spatial_sql_polygon_contains_point() {
    let (_dir, db) = empty_test_db();
    let result = SqlEngine::new(&db)
        .execute("SELECT ST_Contains('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))', ST_Point(1, 1)), ST_Contains('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))', ST_Point(3, 3))")
        .unwrap();

    assert_eq!(
        result.rows,
        vec![vec![SqlValue::Bool(true), SqlValue::Bool(false)]]
    );
}

#[test]
fn spatial_sql_envelope_and_intersects() {
    let (_dir, db) = empty_test_db();
    let result = SqlEngine::new(&db)
        .execute("SELECT ST_AsText(ST_Envelope('LINESTRING (0 1, 3 4)')), ST_Intersects(ST_Envelope('LINESTRING (0 1, 3 4)'), ST_Point(2, 2)), ST_Intersects('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))', 'LINESTRING (3 3, 4 4)')")
        .unwrap();

    assert_eq!(
        result.rows[0][0],
        SqlValue::String("BBOX (0 1, 3 4)".to_string())
    );
    assert_eq!(result.rows[0][1], SqlValue::Bool(true));
    assert_eq!(result.rows[0][2], SqlValue::Bool(false));
}

#[test]
fn spatial_sql_reads_canonical_record_geometry() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    db.create_collection("places").unwrap();
    db.batch_insert(
        "places",
        [
            Record::new("sf").with_geometry(Geometry::point(-122.4194, 37.7749).unwrap()),
            Record::new("la").with_geometry(Geometry::point(-118.2437, 34.0522).unwrap()),
        ],
    )
    .unwrap();

    let result = SqlEngine::new(&db)
        .execute(
            "SELECT id, ST_AsText(geometry) FROM places WHERE ST_DWithin(geometry, ST_Point(-122.4194, 37.7749), 1000)",
        )
        .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], SqlValue::String("sf".to_string()));
    let SqlValue::String(wkt) = &result.rows[0][1] else {
        panic!("expected WKT text");
    };
    assert!(wkt.starts_with("POINT"));
    assert!(wkt.contains("-122.4194"));
}

#[test]
fn spatial_index_create_reopen_radius_and_explain() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(dir.path()).unwrap();
        SqlSession::new(&mut db)
            .execute("CREATE TABLE places (id TEXT PRIMARY KEY)")
            .unwrap();
        db.batch_insert(
            "places",
            [
                Record::new("sf").with_geometry(Geometry::point(-122.4194, 37.7749).unwrap()),
                Record::new("oak").with_geometry(Geometry::point(-122.2711, 37.8044).unwrap()),
                Record::new("la").with_geometry(Geometry::point(-118.2437, 34.0522).unwrap()),
            ],
        )
        .unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE SPATIAL INDEX idx_places_geometry ON places(geometry)")
            .unwrap();

        let explain = session
            .execute(
                "EXPLAIN SELECT id FROM places WHERE ST_DWithin(geometry, ST_Point(-122.4194, 37.7749), 20000)",
            )
            .unwrap();
        assert!(explain.rows.iter().any(|row| {
            row[0]
                .to_cell()
                .contains("SpatialIndexScan idx_places_geometry")
        }));
    }

    let db = BicDb::open(dir.path()).unwrap();
    let result = SqlEngine::new(&db)
        .execute(
            "SELECT id FROM places WHERE ST_DWithin(geometry, ST_Point(-122.4194, 37.7749), 20000) ORDER BY id",
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::String("oak".to_string())],
            vec![SqlValue::String("sf".to_string())],
        ]
    );
}

#[test]
fn spatial_index_intersects_envelope_explain_and_rows() {
    let (_dir, mut db) = empty_test_db();
    SqlSession::new(&mut db)
        .execute("CREATE TABLE regions (id TEXT PRIMARY KEY)")
        .unwrap();
    db.batch_insert(
        "regions",
        [
            Record::new("bay")
                .with_geometry(Geometry::envelope(-123.0, 37.0, -121.0, 38.0).unwrap()),
            Record::new("south")
                .with_geometry(Geometry::envelope(-119.0, 33.0, -117.0, 35.0).unwrap()),
        ],
    )
    .unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE SPATIAL INDEX idx_regions_geometry ON regions(geometry)")
        .unwrap();

    let explain = session
        .execute(
            "EXPLAIN SELECT id FROM regions WHERE ST_Intersects(geometry, ST_Envelope('BBOX (-122.5 37.2, -121.5 37.8)'))",
        )
        .unwrap();
    assert!(explain.rows.iter().any(|row| {
        row[0]
            .to_cell()
            .contains("SpatialIndexScan idx_regions_geometry")
    }));

    let result = session
        .execute(
            "SELECT id FROM regions WHERE ST_Intersects(geometry, ST_Envelope('BBOX (-122.5 37.2, -121.5 37.8)'))",
        )
        .unwrap();
    assert_eq!(result.rows, vec![vec![SqlValue::String("bay".to_string())]]);
}

#[test]
fn spatial_index_wrong_field_and_wrong_geometry_type_errors() {
    let (_dir, mut db) = empty_test_db();
    SqlSession::new(&mut db)
        .execute("CREATE TABLE places (id TEXT PRIMARY KEY, name TEXT)")
        .unwrap();
    db.insert(
        "places",
        Record::new("sf")
            .with_metadata(json!({"name": "not geometry"}))
            .with_geometry(Geometry::point(-122.4194, 37.7749).unwrap()),
    )
    .unwrap();
    let mut session = SqlSession::new(&mut db);
    let wrong_field = session
        .execute("CREATE SPATIAL INDEX idx_places_name ON places(name)")
        .unwrap_err();
    assert!(wrong_field
        .to_string()
        .contains("invalid spatial index geometry"));

    session
        .execute("CREATE TABLE paths (id TEXT PRIMARY KEY)")
        .unwrap();
    drop(session);
    db.insert(
        "paths",
        Record::new("path-1").with_geometry(Geometry::from_wkt("LINESTRING (0 0, 1 1)").unwrap()),
    )
    .unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE SPATIAL INDEX idx_paths_geometry ON paths(geometry)")
        .unwrap();
    let wrong_type = session
        .execute("SELECT id FROM paths WHERE ST_DWithin(geometry, ST_Point(0, 0), 1000)")
        .unwrap_err();
    assert!(wrong_type
        .to_string()
        .contains("supports only point geometries"));
}

#[test]
fn spatial_vector_hybrid_patients_filters_distance_then_orders_by_embedding() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE patients (
                id TEXT PRIMARY KEY,
                name TEXT,
                risk_score INT,
                embedding VECTOR(3)
            )",
        )
        .unwrap();
    drop(session);
    db.batch_insert(
        "patients",
        [
            Record::new("p-near-best")
                .with_metadata(json!({"name": "Nia", "risk_score": 92}))
                .with_geometry(Geometry::point(-122.4194, 37.7749).unwrap())
                .with_vector(vec![0.95, 0.05, 0.0]),
            Record::new("p-near-second")
                .with_metadata(json!({"name": "Omar", "risk_score": 86}))
                .with_geometry(Geometry::point(-122.2711, 37.8044).unwrap())
                .with_vector(vec![0.55, 0.45, 0.0]),
            Record::new("p-near-low-risk")
                .with_metadata(json!({"name": "Mina", "risk_score": 30}))
                .with_geometry(Geometry::point(-122.4313, 37.7739).unwrap())
                .with_vector(vec![0.99, 0.01, 0.0]),
            Record::new("p-far-best")
                .with_metadata(json!({"name": "Lee", "risk_score": 99}))
                .with_geometry(Geometry::point(-121.8863, 37.3382).unwrap())
                .with_vector(vec![1.0, 0.0, 0.0]),
        ],
    )
    .unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE SPATIAL INDEX idx_patients_geometry ON patients(geometry)")
        .unwrap();

    let explain = session
        .execute(
            "EXPLAIN SELECT id FROM patients
             WHERE risk_score >= 80
               AND ST_DWithin(geometry, ST_Point(-122.4194, 37.7749), 20000)
             ORDER BY embedding <=> '[1,0,0]'
             LIMIT 2",
        )
        .unwrap();
    assert!(explain.rows.iter().any(|row| {
        row[0]
            .to_cell()
            .contains("SpatialIndexScan idx_patients_geometry")
    }));

    let result = session
        .execute(
            "SELECT id FROM patients
             WHERE risk_score >= 80
               AND ST_DWithin(geometry, ST_Point(-122.4194, 37.7749), 20000)
             ORDER BY embedding <=> '[1,0,0]'
             LIMIT 2",
        )
        .unwrap();

    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::String("p-near-best".to_string())],
            vec![SqlValue::String("p-near-second".to_string())],
        ]
    );
}

#[test]
fn cognee_pgvector_contract_works_across_queries_catalogs_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE EXTENSION IF NOT EXISTS vector")
            .unwrap();
        session
            .execute(
                "CREATE TABLE \"Entity_name\" (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    embedding VECTOR(3)
                 );
                 INSERT INTO \"Entity_name\" (id, name, embedding) VALUES
                    ('x', 'exact', '[1,0,0]'),
                    ('y', 'near', '[0.8,0.2,0]'),
                    ('z', 'far', '[0,1,0]');",
            )
            .unwrap();

        let projected = session
            .execute(
                "SELECT id,
                        embedding <=> '[1,0,0]'::vector AS cosine,
                        embedding <-> '[1,0,0]'::vector AS l2,
                        embedding <#> '[1,0,0]'::vector AS negative_inner,
                        embedding <+> '[1,0,0]'::vector AS l1
                 FROM \"Entity_name\"
                 ORDER BY cosine, id",
            )
            .unwrap();
        assert_eq!(
            projected.column_types,
            vec![
                Some("text".to_string()),
                Some("float8".to_string()),
                Some("float8".to_string()),
                Some("float8".to_string()),
                Some("float8".to_string()),
            ]
        );
        assert_eq!(
            projected
                .rows
                .iter()
                .map(|row| row[0].to_cell())
                .collect::<Vec<_>>(),
            vec!["x", "y", "z"]
        );
        assert_float_close(&projected.rows[0][1], 0.0, 1e-7);
        assert_float_close(&projected.rows[0][2], 0.0, 1e-7);
        assert_float_close(&projected.rows[0][3], -1.0, 1e-7);
        assert_float_close(&projected.rows[0][4], 0.0, 1e-7);
        assert_float_close(&projected.rows[1][1], 0.02985753301900307, 1e-14);
        assert_float_close(&projected.rows[1][2], 0.28284270931360533, 1e-14);
        assert_float_close(&projected.rows[1][3], -0.800000011920929, 1e-14);
        assert_float_close(&projected.rows[1][4], 0.3999999761581421, 1e-14);

        assert_eq!(
            session
                .execute(
                    "WITH ranked AS (
                        SELECT id, embedding <=> '[1,0,0]'::vector AS distance
                        FROM \"Entity_name\"
                    )
                    SELECT entity.name, ranked.distance
                    FROM ranked
                    JOIN \"Entity_name\" entity ON entity.id = ranked.id
                    ORDER BY ranked.distance, entity.id
                    LIMIT 2"
                )
                .unwrap()
                .rows
                .iter()
                .map(|row| row[0].to_cell())
                .collect::<Vec<_>>(),
            vec!["exact", "near"]
        );

        assert_eq!(
            session
                .execute("SELECT NULL::vector <=> '[1,0,0]'::vector")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Null]]
        );
        let dimensions = session
            .execute(
                "INSERT INTO \"Entity_name\" (id, name, embedding)
                 VALUES ('bad', 'bad', '[1,2]')",
            )
            .unwrap_err();
        assert_eq!(dimensions.sqlstate(), "22000");
        assert_eq!(dimensions.to_string(), "expected 3 dimensions, not 2");
        let operator_dimensions = session
            .execute("SELECT '[1,2,3]'::vector <-> '[1,2]'::vector")
            .unwrap_err();
        assert_eq!(operator_dimensions.sqlstate(), "22000");
        assert_eq!(
            operator_dimensions.to_string(),
            "different vector dimensions 3 and 2"
        );
        assert!(matches!(
            session
                .execute("SELECT '[0,0,0]'::vector <=> '[1,0,0]'::vector")
                .unwrap()
                .rows[0][0],
            SqlValue::Float(value) if value.is_nan()
        ));

        assert_eq!(
            session
                .execute(
                    "SELECT a.atttypid, a.atttypmod, format_type(a.atttypid, a.atttypmod)
                     FROM pg_attribute a
                     JOIN pg_class c ON c.oid = a.attrelid
                     WHERE c.relname = 'Entity_name' AND a.attname = 'embedding'"
                )
                .unwrap()
                .rows,
            vec![vec![
                SqlValue::Int(380_200),
                SqlValue::Int(3),
                SqlValue::String("vector(3)".to_string()),
            ]]
        );
        assert_eq!(
            session
                .execute(
                    "SELECT t.oid, n.nspname, e.extname
                     FROM pg_type t
                     JOIN pg_namespace n ON n.oid = t.typnamespace
                     JOIN pg_extension e ON e.extnamespace = n.oid
                     WHERE t.typname = 'vector' AND e.extname = 'vector'"
                )
                .unwrap()
                .rows,
            vec![vec![
                SqlValue::Int(380_200),
                SqlValue::String("public".to_string()),
                SqlValue::String("vector".to_string()),
            ]]
        );
    }
    drop(db);

    let mut reopened = BicDb::open(dir.path()).unwrap();
    let persisted = SqlSession::new(&mut reopened)
        .execute(
            "SELECT id, embedding
             FROM \"Entity_name\"
             ORDER BY embedding <=> '[1,0,0]'::vector, id",
        )
        .unwrap();
    assert_eq!(
        persisted
            .rows
            .iter()
            .map(|row| row[0].to_cell())
            .collect::<Vec<_>>(),
        vec!["x", "y", "z"]
    );
    assert_eq!(
        SqlSession::new(&mut reopened)
            .execute(
                "SELECT a.atttypid, a.atttypmod, format_type(a.atttypid, a.atttypmod)
                 FROM pg_attribute a
                 JOIN pg_class c ON c.oid = a.attrelid
                 WHERE c.relname = 'Entity_name' AND a.attname = 'embedding'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(380_200),
            SqlValue::Int(3),
            SqlValue::String("vector(3)".to_string()),
        ]]
    );
}

#[test]
fn vector_distance_order_by_projection_alias_uses_computed_values() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE EXTENSION IF NOT EXISTS vector")
        .unwrap();
    session
        .execute(
            "CREATE TABLE vector_alias_order (
                 id TEXT PRIMARY KEY,
                 embedding VECTOR(3) NOT NULL
             );
             INSERT INTO vector_alias_order (id, embedding) VALUES
                 ('a-far', '[0,1,0]'),
                 ('b-near', '[0.8,0.2,0]'),
                 ('c-exact', '[1,0,0]');",
        )
        .unwrap();

    for order_by in ["similarity, id", "embedding <=> '[1,0,0]'::vector, id"] {
        let result = session
            .execute(&format!(
                "SELECT id, embedding <=> '[1,0,0]'::vector AS similarity
                 FROM vector_alias_order
                 ORDER BY {order_by}"
            ))
            .unwrap();
        assert_eq!(
            result
                .rows
                .iter()
                .map(|row| row[0].to_cell())
                .collect::<Vec<_>>(),
            vec!["c-exact", "b-near", "a-far"]
        );
    }
}

#[test]
fn projection_array_srfs_expand_in_postgresql_lockstep() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute(
            "SELECT expanded.value, expanded.ordinal
             FROM (
                 SELECT unnest(ARRAY[10, 20]) AS value,
                        generate_subscripts(ARRAY[10], 1) AS ordinal
             ) expanded
             ORDER BY expanded.value",
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Int(10), SqlValue::Int(1)],
            vec![SqlValue::Int(20), SqlValue::Null],
        ]
    );
}

#[test]
fn spatial_vector_hybrid_clinics_filters_distance_then_orders_by_specialty_embedding() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE clinics (
                id TEXT PRIMARY KEY,
                name TEXT,
                specialty TEXT,
                embedding VECTOR(3)
            )",
        )
        .unwrap();
    drop(session);
    db.batch_insert(
        "clinics",
        [
            Record::new("clinic-cardiac-north")
                .with_metadata(json!({"name": "Bay Heart North", "specialty": "cardiology"}))
                .with_geometry(Geometry::point(-122.2711, 37.8044).unwrap())
                .with_vector(vec![0.02, 0.98, 0.0]),
            Record::new("clinic-general-sf")
                .with_metadata(json!({"name": "Market Street Care", "specialty": "primary care"}))
                .with_geometry(Geometry::point(-122.4194, 37.7749).unwrap())
                .with_vector(vec![0.35, 0.65, 0.0]),
            Record::new("clinic-cardiac-far")
                .with_metadata(json!({"name": "Valley Heart", "specialty": "cardiology"}))
                .with_geometry(Geometry::point(-121.8863, 37.3382).unwrap())
                .with_vector(vec![0.0, 1.0, 0.0]),
        ],
    )
    .unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE SPATIAL INDEX idx_clinics_geometry ON clinics(geometry)")
        .unwrap();

    let result = session
        .execute(
            "SELECT id FROM clinics
             WHERE ST_DWithin(geometry, ST_Point(-122.4194, 37.7749), 20000)
             ORDER BY embedding <=> '[0,1,0]'
             LIMIT 2",
        )
        .unwrap();

    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::String("clinic-cardiac-north".to_string())],
            vec![SqlValue::String("clinic-general-sf".to_string())],
        ]
    );
}

#[test]
fn spatial_vector_hybrid_ann_mode_falls_back_to_spatial_candidates_and_exact_rerank() {
    let (_dir, mut db) = empty_test_db();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE clinics (
                    id TEXT PRIMARY KEY,
                    embedding VECTOR(3)
                )",
            )
            .unwrap();
    }
    db.batch_insert(
        "clinics",
        [
            Record::new("near-second")
                .with_geometry(Geometry::point(-122.4194, 37.7749).unwrap())
                .with_vector(vec![0.8, 0.2, 0.0]),
            Record::new("near-best")
                .with_geometry(Geometry::point(-122.2711, 37.8044).unwrap())
                .with_vector(vec![0.9, 0.1, 0.0]),
            Record::new("far-ann-best")
                .with_geometry(Geometry::point(-121.8863, 37.3382).unwrap())
                .with_vector(vec![1.0, 0.0, 0.0]),
        ],
    )
    .unwrap();
    SqlSession::new(&mut db)
        .execute("CREATE SPATIAL INDEX idx_clinics_geometry ON clinics(geometry)")
        .unwrap();
    db.create_vector_index("clinics", HnswIndexConfig::default())
        .unwrap();

    let mut session = SqlSession::new(&mut db);
    session.execute("SET bicdb.vector_search = 'ann'").unwrap();
    let explain = session
        .execute(
            "EXPLAIN SELECT id FROM clinics
             WHERE ST_DWithin(geometry, ST_Point(-122.4194, 37.7749), 20000)
             ORDER BY embedding <=> '[1,0,0]'
             LIMIT 2",
        )
        .unwrap();
    assert!(explain.rows.iter().any(|row| {
        row[0]
            .to_cell()
            .contains("SpatialIndexScan idx_clinics_geometry")
    }));

    let result = session
        .execute(
            "SELECT id FROM clinics
             WHERE ST_DWithin(geometry, ST_Point(-122.4194, 37.7749), 20000)
             ORDER BY embedding <=> '[1,0,0]'
             LIMIT 2",
        )
        .unwrap();

    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::String("near-best".to_string())],
            vec![SqlValue::String("near-second".to_string())],
        ]
    );
}

#[test]
fn spatial_sql_invalid_geometry_and_unsupported_combinations_are_errors() {
    let (_dir, db) = empty_test_db();
    let engine = SqlEngine::new(&db);

    let unsupported = engine
        .execute("SELECT ST_Distance('LINESTRING (0 0, 1 1)', ST_Point(0, 0))")
        .unwrap_err();
    assert_eq!(unsupported.sqlstate(), "0A000");
    assert!(unsupported
        .to_string()
        .contains("ST_Distance supports point-set geometries"));

    // MULTIPOINT became a first-class geometry in the G1 campaign item.
    let multipoint = engine
        .execute("SELECT ST_AsText('MULTIPOINT ((0 0))')")
        .unwrap();
    assert_eq!(
        multipoint.rows[0][0],
        SqlValue::String("MULTIPOINT((0 0))".to_string())
    );

    let invalid = engine
        .execute("SELECT ST_AsText('MULTIPOINT ((0 0)')")
        .unwrap_err();
    assert!(invalid.to_string().contains("invalid WKT"), "{invalid}");
}

#[test]
fn select_star_returns_record_shape() {
    let (_dir, db) = test_db();
    let result = SqlEngine::new(&db)
        .execute("SELECT * FROM patients LIMIT 1")
        .unwrap();

    assert_eq!(
        result.columns,
        ["id", "metadata", "timestamp", "payload", "vector"]
    );
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], SqlValue::String("patient-a".to_string()));
}

#[test]
fn roles_grants_and_privilege_functions_reflect_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT)")
            .unwrap();
        session
            .execute("CREATE ROLE analyst LOGIN CREATEDB")
            .unwrap();
        session
            .execute("CREATE USER app_user PASSWORD 'secret'")
            .unwrap();
        session
            .execute("GRANT USAGE ON SCHEMA public TO analyst")
            .unwrap();
        session
            .execute("GRANT SELECT ON TABLE patients TO analyst")
            .unwrap();
    }

    let engine = SqlEngine::new(&db);
    let users_in_session = engine.execute("SELECT current_user, session_user").unwrap();
    assert_eq!(
        users_in_session.rows,
        vec![vec![
            SqlValue::String("bicdb".to_string()),
            SqlValue::String("bicdb".to_string()),
        ]]
    );

    let current_role = engine
        .execute(
            "SELECT rolname, rolsuper, rolbypassrls
             FROM pg_roles
             WHERE rolname = current_user",
        )
        .unwrap();
    assert_eq!(
        current_role.rows,
        vec![vec![
            SqlValue::String("bicdb".to_string()),
            SqlValue::Bool(false),
            SqlValue::Bool(false),
        ]]
    );
    let other_privileged_roles = engine
        .execute(
            "SELECT rolname
             FROM pg_roles
             WHERE rolname <> current_user
               AND (rolsuper OR rolbypassrls OR rolcreaterole OR rolcreatedb)
             ORDER BY rolname",
        )
        .unwrap();
    assert_eq!(
        other_privileged_roles.rows,
        vec![vec![SqlValue::String("analyst".to_string())]]
    );

    let roles = engine
        .execute(
            "SELECT rolname, rolcanlogin, rolcreatedb FROM pg_catalog.pg_roles WHERE rolname = 'analyst'",
        )
        .unwrap();
    assert_eq!(
        roles.rows,
        vec![vec![
            SqlValue::String("analyst".to_string()),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
        ]]
    );

    let users = engine
        .execute("SELECT usename FROM pg_catalog.pg_user WHERE usename = 'app_user'")
        .unwrap();
    assert_eq!(
        users.rows,
        vec![vec![SqlValue::String("app_user".to_string())]]
    );

    let privileges = engine
        .execute(
            "SELECT has_schema_privilege('analyst', 'public', 'USAGE'), has_table_privilege('analyst', 'patients', 'SELECT'), has_table_privilege('analyst', 'patients', 'INSERT')",
        )
        .unwrap();
    assert_eq!(
        privileges.rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(false),
        ]]
    );

    let grants = engine
        .execute("SELECT grantee, table_name, privilege_type FROM information_schema.role_table_grants WHERE grantee = 'analyst'")
        .unwrap();
    assert_eq!(
        grants.rows,
        vec![vec![
            SqlValue::String("analyst".to_string()),
            SqlValue::String("patients".to_string()),
            SqlValue::String("SELECT".to_string()),
        ]]
    );

    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("REVOKE SELECT ON TABLE patients FROM analyst")
            .unwrap();
    }

    let revoked = SqlEngine::new(&db)
        .execute("SELECT has_table_privilege('analyst', 'patients', 'SELECT')")
        .unwrap();
    assert_eq!(revoked.rows, vec![vec![SqlValue::Bool(false)]]);
}

#[test]
fn session_identity_keeps_name_type_when_auth_schema_defines_current_user_function() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    SqlSession::new(&mut db)
        .execute(
            "CREATE SCHEMA carrier_private;
             CREATE FUNCTION carrier_private.current_user()
             RETURNS JSONB LANGUAGE SQL
             AS $$ SELECT NULL::JSONB $$",
        )
        .unwrap();

    let scalar_identity = SqlEngine::new(&db).execute("SELECT current_user").unwrap();
    assert_eq!(
        scalar_identity.rows,
        vec![vec![SqlValue::String("bicdb".to_string())]]
    );
    assert_eq!(scalar_identity.column_types, vec![Some("name".to_string())]);
    let identity = SqlEngine::new(&db)
        .execute(
            "SELECT current_user
             FROM pg_roles
             WHERE rolname = current_user",
        )
        .unwrap();
    assert_eq!(identity.column_types, vec![Some("name".to_string())]);
    assert_eq!(
        identity.rows,
        vec![vec![SqlValue::String("bicdb".to_string())]]
    );
    assert_eq!(
        SqlEngine::new(&db)
            .execute(
                "SELECT rolname
                 FROM pg_roles
                 WHERE rolname = current_user",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("bicdb".to_string())]]
    );
    assert_eq!(
        SqlEngine::new(&db)
            .execute(
                "SELECT COALESCE(string_agg(rolname, ', ' ORDER BY rolname), '')
                 FROM pg_roles
                 WHERE rolname <> current_user
                   AND (rolsuper OR rolbypassrls OR rolcreaterole OR rolcreatedb)
                   AND pg_has_role(current_user, oid, 'MEMBER')",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(String::new())]]
    );
}

#[test]
fn postgres_role_membership_grants_reflect_pg_auth_members() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session.execute("CREATE ROLE parent_role").unwrap();
        session
            .execute("CREATE USER child_user PASSWORD 'secret'")
            .unwrap();
        assert_eq!(
            session
                .execute(r#"GRANT "parent_role" TO "child_user" WITH ADMIN OPTION"#)
                .unwrap()
                .command_tag
                .as_deref(),
            Some("GRANT ROLE")
        );
    }

    let engine = SqlEngine::new(&db);
    let memberships = engine
        .execute(
            "SELECT roleid, member, grantor, admin_option, inherit_option, set_option \
             FROM pg_catalog.pg_auth_members",
        )
        .unwrap();
    assert_eq!(
        memberships.rows,
        vec![vec![
            SqlValue::Int(expected_role_oid("parent_role")),
            SqlValue::Int(expected_role_oid("child_user")),
            SqlValue::Int(expected_role_oid("bicdb")),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
        ]]
    );

    {
        let mut session = SqlSession::new(&mut db);
        assert_eq!(
            session
                .execute("REVOKE parent_role FROM child_user")
                .unwrap()
                .command_tag
                .as_deref(),
            Some("REVOKE ROLE")
        );
    }
    let revoked = SqlEngine::new(&db)
        .execute("SELECT roleid, member FROM pg_auth_members")
        .unwrap();
    assert!(revoked.rows.is_empty());
}

#[test]
fn postgres_database_catalog_tracks_create_and_owner_changes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session.execute("CREATE ROLE db_owner").unwrap();
        session
            .execute(r#"CREATE DATABASE "appdb" OWNER "db_owner""#)
            .unwrap();
        session
            .execute(r#"ALTER DATABASE "bicdb" OWNER TO "db_owner""#)
            .unwrap();
    }

    let databases = SqlEngine::new(&db)
        .execute("SELECT datname, datdba FROM pg_database ORDER BY datname")
        .unwrap();
    assert_eq!(
        databases.rows,
        vec![
            vec![
                SqlValue::String("appdb".to_string()),
                SqlValue::Int(expected_role_oid("db_owner")),
            ],
            vec![
                SqlValue::String("bicdb".to_string()),
                SqlValue::Int(expected_role_oid("db_owner")),
            ],
        ]
    );
}

#[test]
fn projection_of_fields() {
    let (_dir, db) = test_db();
    let result = SqlEngine::new(&db)
        .execute("SELECT id, metadata FROM patients LIMIT 1")
        .unwrap();

    assert_eq!(result.columns, ["id", "metadata"]);
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn table_qualified_projection_resolves_schema_columns() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE schema_migrations (version varchar NOT NULL)")
            .unwrap();
        session
            .execute(
                "INSERT INTO schema_migrations (version)
                 VALUES ('20251013071613'), ('20211202041233')",
            )
            .unwrap();
    }

    let result = SqlEngine::new(&db)
        .execute(
            r#"SELECT "schema_migrations"."version" FROM "schema_migrations" ORDER BY "schema_migrations"."version" ASC"#,
        )
        .unwrap();

    assert_eq!(result.columns, ["version"]);
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::String("20211202041233".to_string())],
            vec![SqlValue::String("20251013071613".to_string())],
        ]
    );
}

#[test]
fn derived_table_from_limited_ordered_subquery_can_be_reordered() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE project_authorizations (
                user_id bigint NOT NULL,
                project_id bigint NOT NULL,
                access_level integer NOT NULL,
                PRIMARY KEY (user_id, project_id, access_level)
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO project_authorizations (user_id, project_id, access_level)
             VALUES (1, 10, 20), (2, 20, 30), (3, 30, 40)",
        )
        .unwrap();

    let tuple_range = session
        .execute(
            "SELECT user_id
             FROM project_authorizations
             WHERE (user_id, project_id, access_level) > (1, 10, 20)
               AND (user_id, project_id, access_level) <= (3, 30, 40)
             ORDER BY user_id ASC",
        )
        .unwrap();
    assert_eq!(
        tuple_range.rows,
        vec![vec![SqlValue::Int(2)], vec![SqlValue::Int(3)]]
    );

    let tuple_list = session
        .execute(
            "SELECT user_id
             FROM project_authorizations
             WHERE (user_id, project_id, access_level) IN ((1, 10, 20), (3, 30, 40))
             ORDER BY user_id ASC",
        )
        .unwrap();
    assert_eq!(
        tuple_list.rows,
        vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(3)]]
    );

    let tuple_subquery = session
        .execute(
            "SELECT user_id
             FROM project_authorizations
             WHERE (user_id, project_id, access_level) IN (
               SELECT user_id, project_id, access_level
               FROM project_authorizations
               WHERE user_id = 2
             )",
        )
        .unwrap();
    assert_eq!(tuple_subquery.rows, vec![vec![SqlValue::Int(2)]]);

    let result = session
        .execute(
            "SELECT batch.user_id, batch.project_id, batch.access_level
             FROM (
               SELECT user_id, project_id, access_level
               FROM project_authorizations
               WHERE (user_id, project_id, access_level) >= (0, 0, 0)
               ORDER BY user_id ASC, project_id ASC, access_level ASC
               LIMIT 2
             ) batch
             ORDER BY batch.user_id DESC, batch.project_id DESC, batch.access_level DESC
             LIMIT 1",
        )
        .unwrap();

    assert_eq!(
        result.rows,
        vec![vec![SqlValue::Int(2), SqlValue::Int(20), SqlValue::Int(30),]]
    );
}

#[test]
fn union_query_can_feed_derived_table_filters_and_limits() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE batched_background_migration_jobs (
                id bigint PRIMARY KEY,
                batched_background_migration_id bigint NOT NULL,
                status integer NOT NULL,
                attempts integer NOT NULL,
                updated_at text NOT NULL
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO batched_background_migration_jobs
             (id, batched_background_migration_id, status, attempts, updated_at)
             VALUES
             (1, 67, 2, 1, '2026-06-22 01:00:00'),
             (2, 67, 1, 0, '2026-06-22 01:00:00'),
             (3, 68, 2, 1, '2026-06-22 01:00:00')",
        )
        .unwrap();

    let result = session
        .execute(
            "SELECT batched_background_migration_jobs.*
             FROM (
               (SELECT batched_background_migration_jobs.*
                FROM batched_background_migration_jobs
                WHERE batched_background_migration_jobs.batched_background_migration_id = 67
                  AND batched_background_migration_jobs.status = 2
                  AND (attempts < 3))
               UNION
               (SELECT batched_background_migration_jobs.*
                FROM batched_background_migration_jobs
                WHERE batched_background_migration_jobs.batched_background_migration_id = 67
                  AND batched_background_migration_jobs.status IN (0, 1)
                  AND (updated_at <= '2026-06-22 01:39:27'))
             ) batched_background_migration_jobs
             WHERE batched_background_migration_jobs.batched_background_migration_id = 67
             ORDER BY batched_background_migration_jobs.id ASC
             LIMIT 1",
        )
        .unwrap();

    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Int(1),
            SqlValue::Int(67),
            SqlValue::Int(2),
            SqlValue::Int(1),
            SqlValue::String("2026-06-22 01:00:00".to_string()),
        ]]
    );

    let distinct = session
        .execute(
            "SELECT id
             FROM (
               SELECT id FROM batched_background_migration_jobs WHERE id IN (1, 2)
               UNION
               SELECT id FROM batched_background_migration_jobs WHERE id IN (2, 3)
             ) unioned
             ORDER BY id ASC",
        )
        .unwrap();
    assert_eq!(
        distinct.rows,
        vec![
            vec![SqlValue::Int(1)],
            vec![SqlValue::Int(2)],
            vec![SqlValue::Int(3)],
        ]
    );
}

#[test]
fn comma_from_items_are_evaluated_as_cross_joins() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE source_users (id text PRIMARY KEY, name text)")
        .unwrap();
    session
        .execute("CREATE TABLE source_projects (id text PRIMARY KEY, path text)")
        .unwrap();
    session
        .execute("INSERT INTO source_users (id, name) VALUES ('u1', 'Ada'), ('u2', 'Linus')")
        .unwrap();
    session
        .execute("INSERT INTO source_projects (id, path) VALUES ('p1', 'bicdb'), ('p2', 'gitlab')")
        .unwrap();

    let result = session
        .execute(
            "SELECT u.id, p.id
             FROM source_users u, source_projects p
             WHERE u.id = 'u1'
             ORDER BY p.id",
        )
        .unwrap();

    assert_eq!(
        result.rows,
        vec![
            vec![
                SqlValue::String("u1".to_string()),
                SqlValue::String("p1".to_string())
            ],
            vec![
                SqlValue::String("u1".to_string()),
                SqlValue::String("p2".to_string())
            ],
        ]
    );
}

#[test]
fn postgres_pg_dump_dependency_union_query_supports_comma_catalog_joins() {
    let (_dir, db) = empty_test_db();
    let engine = SqlEngine::new(&db);

    let result = engine
        .execute(
            "SELECT classid, objid, refclassid, refobjid, deptype
             FROM pg_depend
             WHERE deptype != 'p' AND deptype != 'e'
             UNION ALL
             SELECT 'pg_opfamily'::regclass AS classid, amopfamily AS objid,
                    refclassid, refobjid, deptype
             FROM pg_depend d, pg_amop o
             WHERE deptype NOT IN ('p', 'e', 'i')
               AND classid = 'pg_amop'::regclass
               AND objid = o.oid
               AND NOT (refclassid = 'pg_opfamily'::regclass AND amopfamily = refobjid)
             UNION ALL
             SELECT 'pg_opfamily'::regclass AS classid, amprocfamily AS objid,
                    refclassid, refobjid, deptype
             FROM pg_depend d, pg_amproc p
             WHERE deptype NOT IN ('p', 'e', 'i')
               AND classid = 'pg_amproc'::regclass
               AND objid = p.oid
               AND NOT (refclassid = 'pg_opfamily'::regclass AND amprocfamily = refobjid)
             ORDER BY 1,2",
        )
        .unwrap();

    assert_eq!(
        result.columns,
        vec![
            "classid".to_string(),
            "objid".to_string(),
            "refclassid".to_string(),
            "refobjid".to_string(),
            "deptype".to_string(),
        ]
    );
    assert!(result.rows.is_empty());
}

#[test]
fn postgres_pg_dump_extension_fk_dependency_query_returns_empty_without_scanning_constraints() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE pg_dump_ext_fk_parents (id bigint PRIMARY KEY);
             CREATE TABLE pg_dump_ext_fk_children (
                 id bigint PRIMARY KEY,
                 parent_id bigint REFERENCES pg_dump_ext_fk_parents(id)
             );",
        )
        .unwrap();

    let result = session
        .execute(
            "SELECT conrelid, confrelid
             FROM pg_constraint
             JOIN pg_depend ON (objid = confrelid)
             WHERE contype = 'f'
               AND refclassid = 'pg_extension'::regclass
               AND classid = 'pg_class'::regclass;",
        )
        .unwrap();

    assert_eq!(
        result.columns,
        vec!["conrelid".to_string(), "confrelid".to_string()]
    );
    assert!(result.rows.is_empty());
}

#[test]
fn literal_projection_supports_exists_queries() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE geo_nodes (id bigint PRIMARY KEY)")
            .unwrap();
    }

    let empty = SqlEngine::new(&db)
        .execute(r#"SELECT 1 AS one FROM "geo_nodes" LIMIT 1"#)
        .unwrap();
    assert_eq!(empty.columns, ["one"]);
    assert!(empty.rows.is_empty());

    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("INSERT INTO geo_nodes (id) VALUES (1)")
            .unwrap();
    }

    let exists = SqlEngine::new(&db)
        .execute(r#"SELECT 1 AS one FROM "geo_nodes" LIMIT 1"#)
        .unwrap();
    assert_eq!(exists.columns, ["one"]);
    assert_eq!(exists.rows, vec![vec![SqlValue::Int(1)]]);
}

#[test]
fn scalar_numeric_functions_work_without_from_and_inside_stored_functions() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let result = session
        .execute("SELECT random() AS value, trunc(12.9) AS truncated, round(12.4) AS rounded, mod(11, 4) AS remainder, current_timestamp AS captured_at")
        .unwrap();
    assert_eq!(
        result.columns,
        vec![
            "value".to_string(),
            "truncated".to_string(),
            "rounded".to_string(),
            "remainder".to_string(),
            "captured_at".to_string(),
        ]
    );
    let [random_value, truncated, rounded, remainder, captured_at] = result.rows[0].as_slice()
    else {
        panic!("expected one scalar projection row");
    };
    let SqlValue::Float(random_value) = random_value else {
        panic!("random() should return a float");
    };
    assert!((0.0..1.0).contains(random_value));
    assert_eq!(truncated, &SqlValue::Float(12.0));
    assert_eq!(rounded, &SqlValue::Float(12.0));
    assert_eq!(remainder, &SqlValue::Int(3));
    assert!(!captured_at.to_cell().is_empty());

    session
        .execute(
            r#"
            CREATE FUNCTION arbitrary_integer_pick(lower_bound IN INTEGER, upper_bound IN INTEGER)
            RETURNS INTEGER
            LANGUAGE 'plpgsql'
            AS $$
            DECLARE
                picked INTEGER;
            BEGIN
                picked := trunc(random() * (upper_bound - lower_bound + 1) + lower_bound);
                RETURN picked;
            END;
            $$
            "#,
        )
        .unwrap();
    let picked = session
        .execute("SELECT arbitrary_integer_pick(5, 7) AS picked")
        .unwrap();
    let SqlValue::Int(value) = &picked.rows[0][0] else {
        panic!("stored function should return an integer");
    };
    assert!((5..=7).contains(value));
}

#[test]
fn abs_preserves_numeric_types_in_row_predicates() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let result = session
        .execute(
            r#"
            SELECT
                abs(-17::integer),
                pg_typeof(abs(-17::integer))::text,
                abs(-17.4::numeric),
                pg_typeof(abs(-17.4::numeric))::text,
                abs(-17.4::double precision),
                pg_typeof(abs(-17.4::double precision))::text,
                EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements('[{"x": -2.5}]'::jsonb) element
                    WHERE abs((element->>'x')::double precision) > 2
                )
            "#,
        )
        .unwrap();

    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Int(17),
            SqlValue::String("integer".to_string()),
            SqlValue::String("17.4".to_string()),
            SqlValue::String("numeric".to_string()),
            SqlValue::Float(17.4),
            SqlValue::String("double precision".to_string()),
            SqlValue::Bool(true),
        ]]
    );
}

#[test]
fn current_date_matches_postgresql_value_and_type_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE TABLE calendar_rows (id bigint PRIMARY KEY)")
        .unwrap();
    session
        .execute("INSERT INTO calendar_rows (id) VALUES (1)")
        .unwrap();

    for sql in [
        "SELECT CURRENT_DATE AS today",
        "SELECT CURRENT_DATE AS today FROM calendar_rows",
    ] {
        let result = session.execute(sql).unwrap();
        assert_eq!(result.columns, ["today"]);
        assert_eq!(result.column_types, [Some("date".to_string())]);
        let value = result.rows[0][0].to_cell();
        assert_eq!(value.len(), 10);
        assert_eq!(&value[4..5], "-");
        assert_eq!(&value[7..8], "-");
    }

    let comparison = session
        .execute("SELECT CURRENT_DATE = CURRENT_DATE AS stable")
        .unwrap();
    assert_eq!(comparison.rows, vec![vec![SqlValue::Bool(true)]]);
}

#[test]
fn uuidv7_is_typed_volatile_cataloged_and_valid_in_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let generated = session
        .execute("SELECT uuidv7() AS first_id, pg_catalog.uuidv7() AS second_id")
        .unwrap();
    assert_eq!(
        generated.column_types,
        vec![Some("uuid".to_string()), Some("uuid".to_string())]
    );
    let ids = generated.rows[0]
        .iter()
        .map(|value| uuid::Uuid::parse_str(&value.to_cell()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(ids[0].get_version_num(), 7);
    assert_eq!(ids[1].get_version_num(), 7);
    assert_ne!(ids[0], ids[1]);

    session
        .execute(
            "CREATE TABLE uuidv7_items (id uuid PRIMARY KEY DEFAULT uuidv7(), title text NOT NULL)",
        )
        .unwrap();
    let inserted = session
        .execute("INSERT INTO uuidv7_items (title) VALUES ('first'), ('second') RETURNING id")
        .unwrap();
    assert_eq!(inserted.column_types, vec![Some("uuid".to_string())]);
    let inserted_ids = inserted
        .rows
        .iter()
        .map(|row| uuid::Uuid::parse_str(&row[0].to_cell()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(inserted_ids.len(), 2);
    assert!(inserted_ids.iter().all(|id| id.get_version_num() == 7));
    assert_ne!(inserted_ids[0], inserted_ids[1]);

    let catalog = session
        .execute(
            "SELECT oid, proname, pronargs, prorettype, provolatile, proisstrict
             FROM pg_catalog.pg_proc
             WHERE proname IN ('uuidv4', 'uuidv7', 'uuid_extract_timestamp', 'uuid_extract_version')
             ORDER BY oid",
        )
        .unwrap();
    assert_eq!(
        catalog.rows,
        vec![
            vec![
                SqlValue::Int(6342),
                SqlValue::String("uuid_extract_timestamp".to_string()),
                SqlValue::Int(1),
                SqlValue::Int(1184),
                SqlValue::String("i".to_string()),
                SqlValue::Bool(true)
            ],
            vec![
                SqlValue::Int(6343),
                SqlValue::String("uuid_extract_version".to_string()),
                SqlValue::Int(1),
                SqlValue::Int(21),
                SqlValue::String("i".to_string()),
                SqlValue::Bool(true)
            ],
            vec![
                SqlValue::Int(6428),
                SqlValue::String("uuidv4".to_string()),
                SqlValue::Int(0),
                SqlValue::Int(2950),
                SqlValue::String("v".to_string()),
                SqlValue::Bool(true)
            ],
            vec![
                SqlValue::Int(6429),
                SqlValue::String("uuidv7".to_string()),
                SqlValue::Int(0),
                SqlValue::Int(2950),
                SqlValue::String("v".to_string()),
                SqlValue::Bool(true)
            ],
            vec![
                SqlValue::Int(6430),
                SqlValue::String("uuidv7".to_string()),
                SqlValue::Int(1),
                SqlValue::Int(2950),
                SqlValue::String("v".to_string()),
                SqlValue::Bool(true)
            ],
        ]
    );

    let shifted = session
        .execute("SELECT uuidv7(interval '1 second')")
        .unwrap();
    let shifted = uuid::Uuid::parse_str(&shifted.rows[0][0].to_cell()).unwrap();
    assert_eq!(shifted.get_version_num(), 7);

    let invalid = session
        .execute("SELECT uuidv7(interval '1 second', interval '2 seconds')")
        .unwrap_err();
    assert!(invalid
        .to_string()
        .contains("uuidv7 expects 0 or 1 arguments, got 2"));
}

#[test]
fn where_id_filters() {
    let (_dir, db) = test_db();
    let result = SqlEngine::new(&db)
        .execute("SELECT id FROM patients WHERE id = 'patient-b'")
        .unwrap();

    assert_eq!(
        result.rows,
        vec![vec![SqlValue::String("patient-b".to_string())]]
    );
}

#[test]
fn row_predicate_and_short_circuits_false_left_side() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE TABLE arbitrary_short_circuit_rows (row_id INT PRIMARY KEY)")
        .unwrap();
    session
        .execute("INSERT INTO arbitrary_short_circuit_rows (row_id) VALUES (1)")
        .unwrap();

    let result = session
        .execute(
            "SELECT row_id
             FROM arbitrary_short_circuit_rows
             WHERE row_id = 2 AND unsupported_right_side(row_id) = 1",
        )
        .unwrap();
    assert!(result.rows.is_empty());
}

#[test]
fn timestamp_range_filters() {
    let (_dir, db) = test_db();
    let result = SqlEngine::new(&db)
        .execute("SELECT id FROM patients WHERE timestamp >= 1710000020")
        .unwrap();

    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::String("patient-b".to_string())],
            vec![SqlValue::String("patient-c".to_string())],
        ]
    );
}

#[test]
fn metadata_equality_filters() {
    let (_dir, db) = test_db();
    let result = SqlEngine::new(&db)
        .execute("SELECT id FROM patients WHERE metadata.clinic = 'rural-7'")
        .unwrap();

    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::String("patient-a".to_string())],
            vec![SqlValue::String("patient-c".to_string())],
        ]
    );
}

#[test]
fn order_by_timestamp_desc_and_limit() {
    let (_dir, db) = test_db();
    let result = SqlEngine::new(&db)
        .execute("SELECT id FROM patients ORDER BY timestamp DESC LIMIT 2")
        .unwrap();

    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::String("patient-c".to_string())],
            vec![SqlValue::String("patient-b".to_string())],
        ]
    );
}

#[test]
fn count_aggregate() {
    let (_dir, db) = test_db();
    let result = SqlEngine::new(&db)
        .execute("SELECT COUNT(*) FROM wearable WHERE metadata.metric = 'hrv'")
        .unwrap();

    assert_eq!(result.columns, ["COUNT(*)"]);
    assert_eq!(result.rows, vec![vec![SqlValue::Int(2)]]);
}

#[test]
fn count_aggregate_empty_input_returns_one_zero_row() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE t_count (
                id UUID PRIMARY KEY,
                token_hash TEXT,
                deleted_at TIMESTAMPTZ
            )",
        )
        .unwrap();

    let result = session
        .execute(
            "SELECT COUNT(*)::bigint
             FROM t_count
             WHERE token_hash = 'missing'
               AND deleted_at IS NULL",
        )
        .unwrap();

    assert_eq!(result.rows, vec![vec![SqlValue::Int(0)]]);
}

#[test]
fn casted_count_star_returns_matching_count() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE t_count (
                id UUID PRIMARY KEY,
                token_hash TEXT,
                deleted_at TIMESTAMPTZ
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO t_count (id, token_hash, deleted_at)
             VALUES ('00000000-0000-0000-0000-000000000001', 'present', NULL)",
        )
        .unwrap();

    let result = session
        .execute(
            "SELECT COUNT(*)::bigint
             FROM t_count
             WHERE token_hash = 'present'
               AND deleted_at IS NULL",
        )
        .unwrap();

    assert_eq!(result.rows, vec![vec![SqlValue::Int(1)]]);
}

#[test]
fn count_column_empty_input_returns_one_zero_row() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE t_count_column (id TEXT PRIMARY KEY, token_hash TEXT)")
        .unwrap();

    let result = session
        .execute("SELECT COUNT(token_hash) FROM t_count_column WHERE token_hash = 'missing'")
        .unwrap();

    assert_eq!(result.rows, vec![vec![SqlValue::Int(0)]]);
}

#[test]
fn grouped_count_empty_input_returns_no_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE t_count_grouped (id TEXT PRIMARY KEY, token_hash TEXT)")
        .unwrap();

    let result = session
        .execute(
            "SELECT token_hash, COUNT(*)
             FROM t_count_grouped
             WHERE token_hash = 'missing'
             GROUP BY token_hash",
        )
        .unwrap();

    assert!(result.rows.is_empty());
}

#[test]
fn avg_min_max_aggregates() {
    let (_dir, db) = test_db();
    let result = SqlEngine::new(&db)
        .execute(
            "SELECT AVG(metadata.value), MIN(metadata.value), MAX(metadata.value) FROM wearable WHERE metadata.metric = 'hrv'",
        )
        .unwrap();

    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Float(15.0),
            SqlValue::Float(10.0),
            SqlValue::Float(20.0),
        ]]
    );
}

#[test]
fn group_by_with_aggregates() {
    let (_dir, db) = test_db();
    let result = SqlEngine::new(&db)
        .execute(
            "SELECT metric, COUNT(*), AVG(value), MIN(value), MAX(value) FROM wearable GROUP BY metric ORDER BY metric",
        )
        .unwrap();

    assert_eq!(result.columns, ["metric", "COUNT(*)", "avg", "min", "max"]);
    assert_eq!(
        result.rows,
        vec![
            vec![
                SqlValue::String("hrv".to_string()),
                SqlValue::Int(2),
                SqlValue::Float(15.0),
                SqlValue::Float(10.0),
                SqlValue::Float(20.0),
            ],
            vec![
                SqlValue::String("steps".to_string()),
                SqlValue::Int(1),
                SqlValue::Float(1000.0),
                SqlValue::Float(1000.0),
                SqlValue::Float(1000.0),
            ],
        ]
    );
}

#[test]
fn cognee_grouped_having_supports_derived_rows_and_reused_ctes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE cognee_neighbors (
                edge_id TEXT PRIMARY KEY,
                primary_id TEXT NOT NULL,
                nbr_id TEXT NOT NULL
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO cognee_neighbors VALUES
                ('e1', 'p1', 'n1'),
                ('e2', 'p2', 'n1'),
                ('e3', 'p1', 'n2'),
                ('e4', 'p2', 'n2'),
                ('e5', 'p1', 'n3'),
                ('e6', 'p1', 'n1')",
        )
        .unwrap();

    let query = |required: i64| {
        format!(
            "WITH matching_neighbors AS (
                SELECT nbr_id AS id
                FROM (
                    SELECT primary_id, nbr_id
                    FROM cognee_neighbors
                    WHERE primary_id IN ('p1', 'p2')
                ) sub
                GROUP BY nbr_id
                HAVING COUNT(DISTINCT lower(primary_id)) = {required}
            )
            SELECT lhs.id
            FROM matching_neighbors lhs
            JOIN matching_neighbors rhs ON rhs.id = lhs.id
            ORDER BY lhs.id"
        )
    };

    assert_eq!(
        session.execute(&query(2)).unwrap().rows,
        vec![
            vec![SqlValue::String("n1".to_string())],
            vec![SqlValue::String("n2".to_string())],
        ]
    );
    assert_eq!(
        session.execute(&query(1)).unwrap().rows,
        vec![vec![SqlValue::String("n3".to_string())]]
    );
    assert!(session.execute(&query(0)).unwrap().rows.is_empty());
}
