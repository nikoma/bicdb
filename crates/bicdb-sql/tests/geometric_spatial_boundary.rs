use bicdb_core::{BicDb, Geometry, Record};
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn declared_planar_geometry_column_wins_over_intrinsic_spatial_field() {
    let root = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE boundary_geometry (
                    id text PRIMARY KEY,
                    geometry point NOT NULL,
                    bounds box NOT NULL
                 );
                 INSERT INTO boundary_geometry VALUES
                    ('row', point(1, 2), '(0,0),(4,5)');",
            )
            .unwrap();

        let result = session
            .execute(
                "SELECT geometry, pg_typeof(geometry), bounds, pg_typeof(bounds)
                 FROM boundary_geometry",
            )
            .unwrap();
        assert_eq!(
            result.rows,
            vec![vec![
                SqlValue::String("(1,2)".to_string()),
                SqlValue::String("point".to_string()),
                SqlValue::String("(4,5),(0,0)".to_string()),
                SqlValue::String("box".to_string()),
            ]],
        );
        assert_eq!(result.column_types[0], Some("point".to_string()));
        assert_eq!(result.column_types[2], Some("box".to_string()));

        let spatial_index = session
            .execute(
                "CREATE SPATIAL INDEX boundary_geometry_spatial
                 ON boundary_geometry(geometry)",
            )
            .unwrap_err();
        assert_eq!(spatial_index.sqlstate(), "0A000");
        assert!(spatial_index
            .to_string()
            .contains("intrinsic geometry field"));

        session
            .execute(
                "CREATE INDEX boundary_geometry_point_gist
                 ON boundary_geometry USING gist (geometry)",
            )
            .unwrap();
        session
            .execute("UPDATE boundary_geometry SET geometry = point(3, 4) WHERE id = 'row'")
            .unwrap();

        let stored = db.get("boundary_geometry", "row").unwrap().unwrap();
        assert!(stored.geometry.is_none());
        assert_eq!(
            stored.metadata["geometry"]["$bicdb_typed"]["pg_type"],
            "point",
        );
    }

    let mut db = BicDb::open(root.path()).unwrap();
    assert_eq!(
        SqlSession::new(&mut db)
            .execute("SELECT geometry FROM boundary_geometry")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("(3,4)".to_string())]],
    );
}

#[test]
fn bicdb_spatial_and_postgis_boundaries_fail_closed() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    SqlSession::new(&mut db)
        .execute("CREATE TABLE spatial_records (id text PRIMARY KEY)")
        .unwrap();
    db.insert(
        "spatial_records",
        Record::new("location").with_geometry(Geometry::point(1.0, 2.0).unwrap()),
    )
    .unwrap();

    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        session
            .execute("SELECT ST_AsText(geometry) FROM spatial_records")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("POINT(1 2)".to_string())]],
    );

    let planar_as_spatial = session
        .execute("SELECT ST_AsText(point(1, 2))")
        .unwrap_err();
    assert!(planar_as_spatial.to_string().contains("invalid WKT"));
    assert!(session.execute("SELECT ST_Point(1, 2)::point").is_err());

    for extension in ["postgis", "postgis_topology"] {
        let error = session
            .execute(&format!("CREATE EXTENSION IF NOT EXISTS {extension}"))
            .unwrap_err();
        assert_eq!(error.sqlstate(), "0A000");
        assert!(error.to_string().contains("PostGIS is not installed"));
    }
    session
        .execute("CREATE EXTENSION IF NOT EXISTS vector")
        .unwrap();

    let geometry_type = session
        .execute("CREATE TABLE postgis_shape (id text PRIMARY KEY, shape geometry)")
        .unwrap_err();
    assert_eq!(geometry_type.sqlstate(), "42704");
}
