use std::convert::TryFrom;
use std::str::FromStr;

use geo_types::{
    Coord, Geometry as GeoGeometry, LineString, MultiLineString, MultiPoint, MultiPolygon, Point,
    Polygon, Rect,
};
use geojson::{GeoJson, Geometry as GeoJsonGeometry, GeometryValue, Position};
use serde::{Deserialize, Deserializer, Serialize};
use wkt::{ToWkt, Wkt};

use crate::error::{BicDbError, Result};

const GEOMETRY_FRAME_MAGIC: &[u8; 4] = b"BICG";
const GEOMETRY_FRAME_VERSION: u8 = 1;
/// Recursive geometry containers are attacker-controlled on SQL and storage
/// decode paths. Keep this comfortably above practical GIS nesting while
/// preventing stack exhaustion in our decoders and the WKT dependency.
const MAX_GEOMETRY_NESTING_DEPTH: usize = 32;
const KIND_POINT: u8 = 1;
const KIND_LINESTRING: u8 = 2;
const KIND_POLYGON: u8 = 3;
const KIND_ENVELOPE: u8 = 4;
const KIND_MULTI_POINT: u8 = 5;
const KIND_MULTI_LINESTRING: u8 = 6;
const KIND_MULTI_POLYGON: u8 = 7;
const KIND_GEOMETRY_COLLECTION: u8 = 8;

/// EWKB flag marking an embedded SRID.
const EWKB_SRID_FLAG: u32 = 0x2000_0000;
/// The only SRID BicDB speaks: geographic WGS84. Geometry is
/// geography-first by design; other reference systems are rejected at the
/// codec boundary rather than silently misinterpreted.
pub const WGS84_SRID: u32 = 4326;

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(tag = "type", content = "coordinates", rename_all = "snake_case")]
pub enum Geometry {
    Point(Point<f64>),
    LineString(LineString<f64>),
    Polygon(Polygon<f64>),
    Envelope(Rect<f64>),
    MultiPoint(MultiPoint<f64>),
    MultiLineString(MultiLineString<f64>),
    MultiPolygon(MultiPolygon<f64>),
    GeometryCollection(Vec<Geometry>),
}

impl<'de> Deserialize<'de> for Geometry {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        geometry_from_storage_value(&value).map_err(serde::de::Error::custom)
    }
}

impl crate::residency::ResidentBytes for Geometry {
    fn heap_bytes(&self) -> u64 {
        fn ring_bytes(ring: &LineString<f64>) -> u64 {
            crate::residency::vec_bytes::<Coord<f64>>(ring.0.capacity())
        }
        match self {
            // Points and envelopes are fixed-size and fully inline.
            Geometry::Point(_) | Geometry::Envelope(_) => 0,
            Geometry::LineString(line) => ring_bytes(line),
            Geometry::Polygon(polygon) => {
                ring_bytes(polygon.exterior())
                    + crate::residency::vec_bytes::<LineString<f64>>(polygon.interiors().len())
                    + polygon.interiors().iter().map(ring_bytes).sum::<u64>()
            }
            Geometry::MultiPoint(points) => {
                crate::residency::vec_bytes::<Point<f64>>(points.0.capacity())
            }
            Geometry::MultiLineString(lines) => {
                crate::residency::vec_bytes::<LineString<f64>>(lines.0.capacity())
                    + lines.iter().map(ring_bytes).sum::<u64>()
            }
            Geometry::MultiPolygon(polygons) => {
                crate::residency::vec_bytes::<Polygon<f64>>(polygons.0.capacity())
                    + polygons
                        .iter()
                        .map(|polygon| {
                            ring_bytes(polygon.exterior())
                                + crate::residency::vec_bytes::<LineString<f64>>(
                                    polygon.interiors().len(),
                                )
                                + polygon.interiors().iter().map(ring_bytes).sum::<u64>()
                        })
                        .sum::<u64>()
            }
            Geometry::GeometryCollection(members) => {
                crate::residency::vec_bytes::<Geometry>(members.capacity())
                    + members
                        .iter()
                        .map(|member| member.heap_bytes())
                        .sum::<u64>()
            }
        }
    }
}

impl Geometry {
    pub fn point(lon: f64, lat: f64) -> Result<Self> {
        validate_coord(lon, lat)?;
        Ok(Self::Point(Point::new(lon, lat)))
    }

    pub fn envelope(min_lon: f64, min_lat: f64, max_lon: f64, max_lat: f64) -> Result<Self> {
        validate_coord(min_lon, min_lat)?;
        validate_coord(max_lon, max_lat)?;
        if min_lon > max_lon || min_lat > max_lat {
            return Err(geometry_error(
                "invalid envelope: min values must not exceed max values",
            ));
        }
        Ok(Self::Envelope(Rect::new(
            Coord {
                x: min_lon,
                y: min_lat,
            },
            Coord {
                x: max_lon,
                y: max_lat,
            },
        )))
    }

    pub fn from_wkt(input: &str) -> Result<Self> {
        if let Some(envelope) = parse_envelope_wkt(input)? {
            return Ok(envelope);
        }

        let mut depth = 0_usize;
        for byte in input.bytes() {
            match byte {
                b'(' => {
                    depth = depth.saturating_add(1);
                    if depth > MAX_GEOMETRY_NESTING_DEPTH {
                        return Err(geometry_error(format!(
                            "WKT nesting exceeds maximum depth {MAX_GEOMETRY_NESTING_DEPTH}"
                        )));
                    }
                }
                b')' => depth = depth.saturating_sub(1),
                _ => {}
            }
        }

        let wkt = Wkt::<f64>::from_str(input)
            .map_err(|error| geometry_error(format!("invalid WKT: {error}")))?;
        let geo = GeoGeometry::try_from(wkt)
            .map_err(|error| geometry_error(format!("invalid WKT geometry: {error}")))?;
        Self::try_from_geo(geo)
    }

    pub fn to_wkt(&self) -> String {
        match self {
            Self::Point(point) => point.to_wkt().to_string(),
            Self::LineString(line) => line.to_wkt().to_string(),
            Self::Polygon(polygon) => polygon.to_wkt().to_string(),
            Self::Envelope(rect) => format!(
                "BBOX ({} {}, {} {})",
                rect.min().x,
                rect.min().y,
                rect.max().x,
                rect.max().y
            ),
            Self::MultiPoint(points) => points.to_wkt().to_string(),
            Self::MultiLineString(lines) => lines.to_wkt().to_string(),
            Self::MultiPolygon(polygons) => polygons.to_wkt().to_string(),
            Self::GeometryCollection(members) => {
                if members.is_empty() {
                    return "GEOMETRYCOLLECTION EMPTY".to_string();
                }
                let parts = members
                    .iter()
                    .map(|member| match member {
                        // WKT has no bbox type; envelopes print as polygons.
                        Self::Envelope(rect) => envelope_to_polygon(rect).to_wkt().to_string(),
                        other => other.to_wkt(),
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("GEOMETRYCOLLECTION ({parts})")
            }
        }
    }

    pub fn from_geojson_str(input: &str) -> Result<Self> {
        let value = serde_json::from_str(input)
            .map_err(|error| geometry_error(format!("invalid GeoJSON: {error}")))?;
        Self::from_geojson_value(value)
    }

    pub fn from_geojson_value(value: serde_json::Value) -> Result<Self> {
        let object = value
            .as_object()
            .ok_or_else(|| geometry_error("invalid GeoJSON: expected an object"))?;
        let kind = object
            .get("type")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| geometry_error("invalid GeoJSON: missing string field type"))?;
        match kind {
            "Feature" => {
                let geometry = object.get("geometry").ok_or_else(|| {
                    geometry_error("invalid GeoJSON: Feature is missing geometry")
                })?;
                if geometry.is_null() {
                    return Err(geometry_error(
                        "invalid GeoJSON: Feature is missing geometry",
                    ));
                }
                Self::from_geojson_value(geometry.clone())
            }
            "FeatureCollection" => Err(geometry_error(
                "unsupported GeoJSON: FeatureCollection is not a v0.1 geometry value",
            )),
            _ => Self::from_geojson_geometry(&geojson_geometry_from_value(object)?),
        }
    }

    pub fn to_geojson(&self) -> GeoJson {
        GeoJson::Geometry(self.to_geojson_geometry())
    }

    pub fn to_geojson_value(&self) -> serde_json::Value {
        serde_json::to_value(self.to_geojson()).expect("GeoJSON serialization cannot fail")
    }

    pub fn to_bicdb_frame(&self) -> Vec<u8> {
        let mut frame = Vec::new();
        frame.extend_from_slice(GEOMETRY_FRAME_MAGIC);
        frame.push(GEOMETRY_FRAME_VERSION);
        match self {
            Self::Point(point) => {
                frame.push(KIND_POINT);
                push_f64(&mut frame, point.x());
                push_f64(&mut frame, point.y());
            }
            Self::LineString(line) => {
                frame.push(KIND_LINESTRING);
                push_coords(
                    &mut frame,
                    line.points().map(|point| (point.x(), point.y())),
                );
            }
            Self::Polygon(polygon) => {
                frame.push(KIND_POLYGON);
                push_coords(
                    &mut frame,
                    polygon
                        .exterior()
                        .points()
                        .map(|point| (point.x(), point.y())),
                );
                push_u32(&mut frame, polygon.interiors().len() as u32);
                for ring in polygon.interiors() {
                    push_coords(
                        &mut frame,
                        ring.points().map(|point| (point.x(), point.y())),
                    );
                }
            }
            Self::Envelope(rect) => {
                frame.push(KIND_ENVELOPE);
                push_f64(&mut frame, rect.min().x);
                push_f64(&mut frame, rect.min().y);
                push_f64(&mut frame, rect.max().x);
                push_f64(&mut frame, rect.max().y);
            }
            Self::MultiPoint(points) => {
                frame.push(KIND_MULTI_POINT);
                push_coords(
                    &mut frame,
                    points.iter().map(|point| (point.x(), point.y())),
                );
            }
            Self::MultiLineString(lines) => {
                frame.push(KIND_MULTI_LINESTRING);
                push_u32(&mut frame, lines.0.len() as u32);
                for line in lines {
                    push_coords(
                        &mut frame,
                        line.points().map(|point| (point.x(), point.y())),
                    );
                }
            }
            Self::MultiPolygon(polygons) => {
                frame.push(KIND_MULTI_POLYGON);
                push_u32(&mut frame, polygons.0.len() as u32);
                for polygon in polygons {
                    push_coords(
                        &mut frame,
                        polygon
                            .exterior()
                            .points()
                            .map(|point| (point.x(), point.y())),
                    );
                    push_u32(&mut frame, polygon.interiors().len() as u32);
                    for ring in polygon.interiors() {
                        push_coords(
                            &mut frame,
                            ring.points().map(|point| (point.x(), point.y())),
                        );
                    }
                }
            }
            Self::GeometryCollection(members) => {
                frame.push(KIND_GEOMETRY_COLLECTION);
                push_u32(&mut frame, members.len() as u32);
                for member in members {
                    let inner = member.to_bicdb_frame();
                    push_u32(&mut frame, inner.len() as u32);
                    frame.extend_from_slice(&inner);
                }
            }
        }
        frame
    }

    pub fn from_bicdb_frame(frame: &[u8]) -> Result<Self> {
        Self::from_bicdb_frame_at_depth(frame, 0)
    }

    fn from_bicdb_frame_at_depth(frame: &[u8], depth: usize) -> Result<Self> {
        if depth > MAX_GEOMETRY_NESTING_DEPTH {
            return Err(geometry_error(format!(
                "geometry frame nesting exceeds maximum depth {MAX_GEOMETRY_NESTING_DEPTH}"
            )));
        }
        if frame.len() < 6 || &frame[..4] != GEOMETRY_FRAME_MAGIC {
            return Err(geometry_error(
                "invalid geometry frame: missing BICG header",
            ));
        }
        if frame[4] != GEOMETRY_FRAME_VERSION {
            return Err(geometry_error(format!(
                "unsupported geometry frame version {}",
                frame[4]
            )));
        }

        let mut cursor = FrameCursor::new(&frame[6..]);
        let geometry = match frame[5] {
            KIND_POINT => Self::point(cursor.f64()?, cursor.f64()?)?,
            KIND_LINESTRING => Self::LineString(coords_to_linestring(cursor.coords()?)?),
            KIND_POLYGON => {
                let exterior = coords_to_linestring(cursor.coords()?)?;
                let interior_count = cursor.bounded_count(4, "polygon interior ring")?;
                let mut interiors = Vec::with_capacity(interior_count);
                for _ in 0..interior_count {
                    interiors.push(coords_to_linestring(cursor.coords()?)?);
                }
                validate_polygon(&exterior, &interiors)?;
                Self::Polygon(Polygon::new(exterior, interiors))
            }
            KIND_ENVELOPE => {
                Self::envelope(cursor.f64()?, cursor.f64()?, cursor.f64()?, cursor.f64()?)?
            }
            KIND_MULTI_POINT => Self::MultiPoint(MultiPoint::new(
                cursor
                    .coords()?
                    .into_iter()
                    .map(|(x, y)| Point::new(x, y))
                    .collect(),
            )),
            KIND_MULTI_LINESTRING => {
                let count = cursor.bounded_count(4, "multilinestring member")?;
                let mut lines = Vec::with_capacity(count);
                for _ in 0..count {
                    lines.push(coords_to_linestring(cursor.coords()?)?);
                }
                Self::MultiLineString(MultiLineString::new(lines))
            }
            KIND_MULTI_POLYGON => {
                let count = cursor.bounded_count(8, "multipolygon member")?;
                let mut polygons = Vec::with_capacity(count);
                for _ in 0..count {
                    let exterior = coords_to_linestring(cursor.coords()?)?;
                    let interior_count = cursor.bounded_count(4, "polygon interior ring")?;
                    let mut interiors = Vec::with_capacity(interior_count);
                    for _ in 0..interior_count {
                        interiors.push(coords_to_linestring(cursor.coords()?)?);
                    }
                    validate_polygon(&exterior, &interiors)?;
                    polygons.push(Polygon::new(exterior, interiors));
                }
                Self::MultiPolygon(MultiPolygon::new(polygons))
            }
            KIND_GEOMETRY_COLLECTION => {
                let count = cursor.bounded_count(4, "geometry collection member")?;
                let mut members = Vec::with_capacity(count);
                for _ in 0..count {
                    let length = cursor.u32()? as usize;
                    members.push(Self::from_bicdb_frame_at_depth(
                        cursor.bytes(length)?,
                        depth.saturating_add(1),
                    )?);
                }
                Self::GeometryCollection(members)
            }
            kind => {
                return Err(geometry_error(format!(
                    "unsupported geometry frame kind {kind}"
                )));
            }
        };

        cursor.finish()?;
        Ok(geometry)
    }

    fn from_geojson_geometry(geometry: &GeoJsonGeometry) -> Result<Self> {
        match &geometry.value {
            GeometryValue::Point { coordinates } => {
                let (x, y) = position_xy(coordinates)?;
                Self::point(x, y)
            }
            GeometryValue::LineString { coordinates } => {
                Ok(Self::LineString(coords_to_linestring(
                    coordinates
                        .iter()
                        .map(position_xy)
                        .collect::<Result<Vec<_>>>()?,
                )?))
            }
            GeometryValue::Polygon { coordinates } => {
                if let Some(bbox) = geometry.bbox.as_deref() {
                    if let Some(envelope) = envelope_from_geojson_bbox_polygon(bbox, coordinates)? {
                        return Ok(envelope);
                    }
                }
                let (exterior, interiors) = polygon_rings_from_positions(coordinates)?;
                validate_polygon(&exterior, &interiors)?;
                Ok(Self::Polygon(Polygon::new(exterior, interiors)))
            }
            GeometryValue::MultiPoint { coordinates } => Ok(Self::MultiPoint(MultiPoint::new(
                coordinates
                    .iter()
                    .map(|position| {
                        let (x, y) = position_xy(position)?;
                        validate_coord(x, y)?;
                        Ok(Point::new(x, y))
                    })
                    .collect::<Result<Vec<_>>>()?,
            ))),
            GeometryValue::MultiLineString { coordinates } => {
                Ok(Self::MultiLineString(MultiLineString::new(
                    coordinates
                        .iter()
                        .map(|line| {
                            coords_to_linestring(
                                line.iter().map(position_xy).collect::<Result<Vec<_>>>()?,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?,
                )))
            }
            GeometryValue::MultiPolygon { coordinates } => {
                let mut polygons = Vec::with_capacity(coordinates.len());
                for rings in coordinates {
                    let (exterior, interiors) = polygon_rings_from_positions(rings)?;
                    validate_polygon(&exterior, &interiors)?;
                    polygons.push(Polygon::new(exterior, interiors));
                }
                Ok(Self::MultiPolygon(MultiPolygon::new(polygons)))
            }
            GeometryValue::GeometryCollection { geometries } => Ok(Self::GeometryCollection(
                geometries
                    .iter()
                    .map(Self::from_geojson_geometry)
                    .collect::<Result<Vec<_>>>()?,
            )),
        }
    }

    fn to_geojson_geometry(&self) -> GeoJsonGeometry {
        match self {
            Self::Point(point) => {
                GeoJsonGeometry::new(GeometryValue::new_point([point.x(), point.y()]))
            }
            Self::LineString(line) => GeoJsonGeometry::new(GeometryValue::new_line_string(
                line.points().map(|point| [point.x(), point.y()]),
            )),
            Self::Polygon(polygon) => {
                let mut rings = Vec::with_capacity(1 + polygon.interiors().len());
                rings.push(
                    polygon
                        .exterior()
                        .points()
                        .map(|point| [point.x(), point.y()].into())
                        .collect(),
                );
                for ring in polygon.interiors() {
                    rings.push(
                        ring.points()
                            .map(|point| [point.x(), point.y()].into())
                            .collect(),
                    );
                }
                GeoJsonGeometry::new(GeometryValue::Polygon { coordinates: rings })
            }
            Self::Envelope(rect) => {
                let mut geometry = GeoJsonGeometry::new(GeometryValue::Polygon {
                    coordinates: vec![vec![
                        [rect.min().x, rect.min().y].into(),
                        [rect.max().x, rect.min().y].into(),
                        [rect.max().x, rect.max().y].into(),
                        [rect.min().x, rect.max().y].into(),
                        [rect.min().x, rect.min().y].into(),
                    ]],
                });
                geometry.bbox = Some(vec![rect.min().x, rect.min().y, rect.max().x, rect.max().y]);
                geometry
            }
            Self::MultiPoint(points) => GeoJsonGeometry::new(GeometryValue::MultiPoint {
                coordinates: points
                    .iter()
                    .map(|point| vec![point.x(), point.y()].into())
                    .collect(),
            }),
            Self::MultiLineString(lines) => GeoJsonGeometry::new(GeometryValue::MultiLineString {
                coordinates: lines
                    .iter()
                    .map(|line| {
                        line.points()
                            .map(|point| vec![point.x(), point.y()].into())
                            .collect()
                    })
                    .collect(),
            }),
            Self::MultiPolygon(polygons) => GeoJsonGeometry::new(GeometryValue::MultiPolygon {
                coordinates: polygons
                    .iter()
                    .map(|polygon| {
                        let mut rings = Vec::with_capacity(1 + polygon.interiors().len());
                        rings.push(
                            polygon
                                .exterior()
                                .points()
                                .map(|point| vec![point.x(), point.y()].into())
                                .collect(),
                        );
                        for ring in polygon.interiors() {
                            rings.push(
                                ring.points()
                                    .map(|point| vec![point.x(), point.y()].into())
                                    .collect(),
                            );
                        }
                        rings
                    })
                    .collect(),
            }),
            Self::GeometryCollection(members) => {
                GeoJsonGeometry::new(GeometryValue::GeometryCollection {
                    geometries: members
                        .iter()
                        .map(|member| member.to_geojson_geometry())
                        .collect(),
                })
            }
        }
    }

    fn try_from_geo(geometry: GeoGeometry<f64>) -> Result<Self> {
        match geometry {
            GeoGeometry::Point(point) => {
                validate_coord(point.x(), point.y())?;
                Ok(Self::Point(point))
            }
            GeoGeometry::LineString(line) => {
                validate_linestring(&line)?;
                Ok(Self::LineString(line))
            }
            GeoGeometry::Polygon(polygon) => {
                validate_polygon(polygon.exterior(), polygon.interiors())?;
                Ok(Self::Polygon(polygon))
            }
            GeoGeometry::Rect(rect) => {
                Self::envelope(rect.min().x, rect.min().y, rect.max().x, rect.max().y)
            }
            GeoGeometry::MultiPoint(points) => {
                for point in &points {
                    validate_coord(point.x(), point.y())?;
                }
                Ok(Self::MultiPoint(points))
            }
            GeoGeometry::MultiLineString(lines) => {
                for line in &lines {
                    validate_linestring(line)?;
                }
                Ok(Self::MultiLineString(lines))
            }
            GeoGeometry::MultiPolygon(polygons) => {
                for polygon in &polygons {
                    validate_polygon(polygon.exterior(), polygon.interiors())?;
                }
                Ok(Self::MultiPolygon(polygons))
            }
            GeoGeometry::GeometryCollection(collection) => Ok(Self::GeometryCollection(
                collection
                    .into_iter()
                    .map(Self::try_from_geo)
                    .collect::<Result<Vec<_>>>()?,
            )),
            other => Err(geometry_error(format!(
                "unsupported WKT geometry type {:?} for BicDB Spatial v0.1",
                other
            ))),
        }
    }
}

fn geometry_from_storage_value(value: &serde_json::Value) -> Result<Geometry> {
    let object = value
        .as_object()
        .ok_or_else(|| geometry_error("stored geometry must be an object"))?;
    let kind = object
        .get("type")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| geometry_error("stored geometry is missing type"))?;
    let coordinates = object
        .get("coordinates")
        .ok_or_else(|| geometry_error("stored geometry is missing coordinates"))?;
    match kind {
        "point" => {
            let coord = storage_coord(coordinates)?;
            Geometry::point(coord.x, coord.y)
        }
        "line_string" => {
            let line = storage_line_string(coordinates)?;
            validate_linestring(&line)?;
            Ok(Geometry::LineString(line))
        }
        "polygon" => {
            let polygon = coordinates
                .as_object()
                .ok_or_else(|| geometry_error("stored polygon coordinates must be an object"))?;
            let exterior = storage_line_string(
                polygon
                    .get("exterior")
                    .ok_or_else(|| geometry_error("stored polygon is missing exterior"))?,
            )?;
            let interiors = polygon
                .get("interiors")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| geometry_error("stored polygon is missing interiors"))?
                .iter()
                .map(storage_line_string)
                .collect::<Result<Vec<_>>>()?;
            validate_polygon(&exterior, &interiors)?;
            Ok(Geometry::Polygon(Polygon::new(exterior, interiors)))
        }
        "envelope" => {
            let envelope = coordinates
                .as_object()
                .ok_or_else(|| geometry_error("stored envelope coordinates must be an object"))?;
            let min = storage_coord(
                envelope
                    .get("min")
                    .ok_or_else(|| geometry_error("stored envelope is missing min"))?,
            )?;
            let max = storage_coord(
                envelope
                    .get("max")
                    .ok_or_else(|| geometry_error("stored envelope is missing max"))?,
            )?;
            Geometry::envelope(min.x, min.y, max.x, max.y)
        }
        "multi_point" => {
            let points = coordinates
                .as_array()
                .ok_or_else(|| geometry_error("stored multi_point coordinates must be an array"))?
                .iter()
                .map(|value| storage_coord(value).map(|coord| Point::new(coord.x, coord.y)))
                .collect::<Result<Vec<_>>>()?;
            Ok(Geometry::MultiPoint(MultiPoint::new(points)))
        }
        "multi_line_string" => {
            let lines = coordinates
                .as_array()
                .ok_or_else(|| {
                    geometry_error("stored multi_line_string coordinates must be an array")
                })?
                .iter()
                .map(|value| {
                    let line = storage_line_string(value)?;
                    validate_linestring(&line)?;
                    Ok(line)
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Geometry::MultiLineString(MultiLineString::new(lines)))
        }
        "multi_polygon" => {
            let polygons = coordinates
                .as_array()
                .ok_or_else(|| geometry_error("stored multi_polygon coordinates must be an array"))?
                .iter()
                .map(storage_polygon)
                .collect::<Result<Vec<_>>>()?;
            Ok(Geometry::MultiPolygon(MultiPolygon::new(polygons)))
        }
        "geometry_collection" => Ok(Geometry::GeometryCollection(
            coordinates
                .as_array()
                .ok_or_else(|| {
                    geometry_error("stored geometry_collection coordinates must be an array")
                })?
                .iter()
                .map(geometry_from_storage_value)
                .collect::<Result<Vec<_>>>()?,
        )),
        other => Err(geometry_error(format!(
            "unsupported stored geometry type {other}"
        ))),
    }
}

fn storage_polygon(value: &serde_json::Value) -> Result<Polygon<f64>> {
    let polygon = value
        .as_object()
        .ok_or_else(|| geometry_error("stored polygon coordinates must be an object"))?;
    let exterior = storage_line_string(
        polygon
            .get("exterior")
            .ok_or_else(|| geometry_error("stored polygon is missing exterior"))?,
    )?;
    let interiors = polygon
        .get("interiors")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| geometry_error("stored polygon is missing interiors"))?
        .iter()
        .map(storage_line_string)
        .collect::<Result<Vec<_>>>()?;
    validate_polygon(&exterior, &interiors)?;
    Ok(Polygon::new(exterior, interiors))
}

/// WKT/WKB have no bbox type; envelopes are emitted as their closed-ring
/// polygon.
fn envelope_to_polygon(rect: &Rect<f64>) -> Polygon<f64> {
    Polygon::new(
        LineString::new(vec![
            Coord {
                x: rect.min().x,
                y: rect.min().y,
            },
            Coord {
                x: rect.max().x,
                y: rect.min().y,
            },
            Coord {
                x: rect.max().x,
                y: rect.max().y,
            },
            Coord {
                x: rect.min().x,
                y: rect.max().y,
            },
            Coord {
                x: rect.min().x,
                y: rect.min().y,
            },
        ]),
        Vec::new(),
    )
}

fn storage_coord(value: &serde_json::Value) -> Result<Coord<f64>> {
    let object = value
        .as_object()
        .ok_or_else(|| geometry_error("stored coordinate must be an object"))?;
    let x = json_number(
        object
            .get("x")
            .ok_or_else(|| geometry_error("stored coordinate is missing x"))?,
        "stored coordinate x",
    )?;
    let y = json_number(
        object
            .get("y")
            .ok_or_else(|| geometry_error("stored coordinate is missing y"))?,
        "stored coordinate y",
    )?;
    validate_coord(x, y)?;
    Ok(Coord { x, y })
}

fn storage_line_string(value: &serde_json::Value) -> Result<LineString<f64>> {
    let coordinates = value
        .as_array()
        .ok_or_else(|| geometry_error("stored linestring coordinates must be an array"))?
        .iter()
        .map(storage_coord)
        .collect::<Result<Vec<_>>>()?;
    Ok(LineString::new(coordinates))
}

fn geojson_geometry_from_value(
    object: &serde_json::Map<String, serde_json::Value>,
) -> Result<GeoJsonGeometry> {
    let kind = object
        .get("type")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| geometry_error("invalid GeoJSON: missing string field type"))?;
    let coordinates = object.get("coordinates");
    let value = match kind {
        "Point" => GeometryValue::Point {
            coordinates: geojson_position(
                coordinates
                    .ok_or_else(|| geometry_error("invalid GeoJSON Point: missing coordinates"))?,
            )?,
        },
        "LineString" => GeometryValue::LineString {
            coordinates: geojson_positions(coordinates.ok_or_else(|| {
                geometry_error("invalid GeoJSON LineString: missing coordinates")
            })?)?,
        },
        "Polygon" => {
            GeometryValue::Polygon {
                coordinates: geojson_rings(coordinates.ok_or_else(|| {
                    geometry_error("invalid GeoJSON Polygon: missing coordinates")
                })?)?,
            }
        }
        "MultiPoint" => GeometryValue::MultiPoint {
            coordinates: geojson_positions(coordinates.ok_or_else(|| {
                geometry_error("invalid GeoJSON MultiPoint: missing coordinates")
            })?)?,
        },
        "MultiLineString" => GeometryValue::MultiLineString {
            coordinates: geojson_rings(coordinates.ok_or_else(|| {
                geometry_error("invalid GeoJSON MultiLineString: missing coordinates")
            })?)?,
        },
        "MultiPolygon" => GeometryValue::MultiPolygon {
            coordinates: coordinates
                .ok_or_else(|| geometry_error("invalid GeoJSON MultiPolygon: missing coordinates"))?
                .as_array()
                .ok_or_else(|| geometry_error("GeoJSON MultiPolygon coordinates must be an array"))?
                .iter()
                .map(geojson_rings)
                .collect::<Result<Vec<_>>>()?,
        },
        "GeometryCollection" => GeometryValue::GeometryCollection {
            geometries: object
                .get("geometries")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| {
                    geometry_error("invalid GeoJSON GeometryCollection: missing geometries")
                })?
                .iter()
                .map(|member| {
                    let object = member.as_object().ok_or_else(|| {
                        geometry_error("GeoJSON collection member must be an object")
                    })?;
                    geojson_geometry_from_value(object)
                })
                .collect::<Result<Vec<_>>>()?,
        },
        other => {
            return Err(geometry_error(format!(
                "unsupported GeoJSON geometry type {other} for BicDB Spatial v0.1"
            )));
        }
    };
    let mut geometry = GeoJsonGeometry::new(value);
    if let Some(bbox) = object.get("bbox") {
        geometry.bbox = Some(json_number_array(bbox, "GeoJSON bbox")?);
    }
    Ok(geometry)
}

fn geojson_position(value: &serde_json::Value) -> Result<Position> {
    let position = json_number_array(value, "GeoJSON position")?;
    if position.len() < 2 {
        return Err(geometry_error(
            "GeoJSON position requires lon and lat values",
        ));
    }
    validate_coord(position[0], position[1])?;
    Ok(position.into())
}

fn geojson_positions(value: &serde_json::Value) -> Result<Vec<Position>> {
    value
        .as_array()
        .ok_or_else(|| geometry_error("GeoJSON coordinates must be an array"))?
        .iter()
        .map(geojson_position)
        .collect()
}

fn geojson_rings(value: &serde_json::Value) -> Result<Vec<Vec<Position>>> {
    value
        .as_array()
        .ok_or_else(|| geometry_error("GeoJSON polygon coordinates must be an array"))?
        .iter()
        .map(geojson_positions)
        .collect()
}

fn json_number_array(value: &serde_json::Value, name: &str) -> Result<Vec<f64>> {
    value
        .as_array()
        .ok_or_else(|| geometry_error(format!("{name} must be an array")))?
        .iter()
        .map(|value| json_number(value, name))
        .collect()
}

fn json_number(value: &serde_json::Value, name: &str) -> Result<f64> {
    let number = value
        .as_f64()
        .ok_or_else(|| geometry_error(format!("{name} must contain finite numbers")))?;
    if !number.is_finite() {
        return Err(geometry_error(format!(
            "{name} must contain finite numbers"
        )));
    }
    Ok(number)
}

fn validate_coord(x: f64, y: f64) -> Result<()> {
    if !x.is_finite() || !y.is_finite() {
        return Err(geometry_error(
            "geometry coordinates must be finite numbers",
        ));
    }
    Ok(())
}

fn parse_envelope_wkt(input: &str) -> Result<Option<Geometry>> {
    let trimmed = input.trim();
    let upper = trimmed.to_ascii_uppercase();
    let Some(prefix_len) = ["BBOX", "ENVELOPE"]
        .iter()
        .find_map(|prefix| upper.starts_with(prefix).then_some(prefix.len()))
    else {
        return Ok(None);
    };

    let rest = trimmed[prefix_len..].trim();
    let Some(body) = rest
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
    else {
        return Err(geometry_error(
            "invalid envelope WKT: expected BBOX (min_lon min_lat, max_lon max_lat)",
        ));
    };
    let values = body
        .split(|ch: char| ch == ',' || ch.is_ascii_whitespace())
        .filter(|value| !value.is_empty())
        .map(|value| {
            value.parse::<f64>().map_err(|error| {
                geometry_error(format!("invalid envelope WKT coordinate: {error}"))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if values.len() != 4 {
        return Err(geometry_error(
            "invalid envelope WKT: expected four coordinates",
        ));
    }
    Geometry::envelope(values[0], values[1], values[2], values[3]).map(Some)
}

fn envelope_from_geojson_bbox_polygon(
    bbox: &[f64],
    coordinates: &[Vec<Position>],
) -> Result<Option<Geometry>> {
    if bbox.len() != 4 || coordinates.len() != 1 || coordinates[0].len() != 5 {
        return Ok(None);
    }
    let envelope = Geometry::envelope(bbox[0], bbox[1], bbox[2], bbox[3])?;
    let Geometry::Envelope(rect) = &envelope else {
        return Err(geometry_error("internal envelope conversion failed"));
    };
    let expected = [
        (rect.min().x, rect.min().y),
        (rect.max().x, rect.min().y),
        (rect.max().x, rect.max().y),
        (rect.min().x, rect.max().y),
        (rect.min().x, rect.min().y),
    ];
    let actual = coordinates[0]
        .iter()
        .map(position_xy)
        .collect::<Result<Vec<_>>>()?;
    if actual == expected {
        Ok(Some(envelope))
    } else {
        Ok(None)
    }
}

fn validate_linestring(line: &LineString<f64>) -> Result<()> {
    if line.0.len() < 2 {
        return Err(geometry_error(
            "LINESTRING requires at least two coordinates",
        ));
    }
    for coord in &line.0 {
        validate_coord(coord.x, coord.y)?;
    }
    Ok(())
}

fn validate_polygon(exterior: &LineString<f64>, interiors: &[LineString<f64>]) -> Result<()> {
    validate_ring("POLYGON exterior ring", exterior)?;
    for ring in interiors {
        validate_ring("POLYGON interior ring", ring)?;
    }
    Ok(())
}

fn validate_ring(name: &str, ring: &LineString<f64>) -> Result<()> {
    if ring.0.len() < 4 {
        return Err(geometry_error(format!(
            "{name} requires at least four coordinates"
        )));
    }
    for coord in &ring.0 {
        validate_coord(coord.x, coord.y)?;
    }
    if ring.0.first() != ring.0.last() {
        return Err(geometry_error(format!("{name} must be closed")));
    }
    Ok(())
}

fn coords_to_linestring(coords: Vec<(f64, f64)>) -> Result<LineString<f64>> {
    let line = LineString::new(coords.into_iter().map(|(x, y)| Coord { x, y }).collect());
    validate_linestring(&line)?;
    Ok(line)
}

fn polygon_rings_from_positions(
    rings: &[Vec<Position>],
) -> Result<(LineString<f64>, Vec<LineString<f64>>)> {
    let exterior = rings
        .first()
        .ok_or_else(|| geometry_error("POLYGON requires an exterior ring"))?;
    let exterior = LineString::new(
        exterior
            .iter()
            .map(position_xy)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|(x, y)| Coord { x, y })
            .collect(),
    );
    let interiors = rings
        .iter()
        .skip(1)
        .map(|ring| {
            Ok(LineString::new(
                ring.iter()
                    .map(position_xy)
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .map(|(x, y)| Coord { x, y })
                    .collect(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((exterior, interiors))
}

fn position_xy(position: &Position) -> Result<(f64, f64)> {
    if position.len() < 2 {
        return Err(geometry_error(
            "GeoJSON position requires lon and lat values",
        ));
    }
    validate_coord(position[0], position[1])?;
    Ok((position[0], position[1]))
}

fn push_coords(frame: &mut Vec<u8>, coords: impl IntoIterator<Item = (f64, f64)>) {
    let coords = coords.into_iter().collect::<Vec<_>>();
    push_u32(frame, coords.len() as u32);
    for (x, y) in coords {
        push_f64(frame, x);
        push_f64(frame, y);
    }
}

fn push_u32(frame: &mut Vec<u8>, value: u32) {
    frame.extend_from_slice(&value.to_le_bytes());
}

fn push_f64(frame: &mut Vec<u8>, value: f64) {
    frame.extend_from_slice(&value.to_le_bytes());
}

struct FrameCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> FrameCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn u32(&mut self) -> Result<u32> {
        let bytes = self.take(4)?;
        let bytes: [u8; 4] = bytes
            .try_into()
            .map_err(|_| geometry_error("truncated geometry frame u32"))?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn f64(&mut self) -> Result<f64> {
        let bytes = self.take(8)?;
        let bytes: [u8; 8] = bytes
            .try_into()
            .map_err(|_| geometry_error("truncated geometry frame f64"))?;
        let value = f64::from_le_bytes(bytes);
        if !value.is_finite() {
            return Err(geometry_error(
                "geometry frame contains non-finite coordinate",
            ));
        }
        Ok(value)
    }

    fn coords(&mut self) -> Result<Vec<(f64, f64)>> {
        let len = self.bounded_count(16, "coordinate")?;
        let mut coords = Vec::with_capacity(len);
        for _ in 0..len {
            coords.push((self.f64()?, self.f64()?));
        }
        Ok(coords)
    }

    fn bounded_count(&mut self, minimum_item_bytes: usize, kind: &str) -> Result<usize> {
        let count = self.u32()? as usize;
        let remaining = self.bytes.len().saturating_sub(self.offset);
        if count > remaining / minimum_item_bytes.max(1) {
            return Err(geometry_error(format!(
                "geometry frame {kind} count exceeds remaining payload"
            )));
        }
        Ok(count)
    }

    fn bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        self.take(len)
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self.offset.saturating_add(len);
        if end > self.bytes.len() {
            return Err(geometry_error("truncated geometry frame"));
        }
        let bytes = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }

    fn finish(&self) -> Result<()> {
        if self.offset != self.bytes.len() {
            return Err(geometry_error("geometry frame has trailing bytes"));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// WKB / EWKB codec (OGC well-known binary; PostGIS-compatible EWKB SRID).
// Geography-first: only SRID 4326 (or 0 = unspecified) is accepted on read.
// Envelopes encode as their closed-ring polygon — WKB has no bbox type.
// ---------------------------------------------------------------------------

const WKB_POINT: u32 = 1;
const WKB_LINESTRING: u32 = 2;
const WKB_POLYGON: u32 = 3;
const WKB_MULTI_POINT: u32 = 4;
const WKB_MULTI_LINESTRING: u32 = 5;
const WKB_MULTI_POLYGON: u32 = 6;
const WKB_GEOMETRY_COLLECTION: u32 = 7;

impl Geometry {
    /// The axis-aligned lon/lat bounds over every coordinate in the
    /// geometry, `None` when it contains no coordinates. Purely coordinate
    /// arithmetic: an antimeridian-crossing geometry yields the wide box —
    /// callers that need split boxes handle that at query level.
    pub fn coordinate_bounds(&self) -> Option<([f64; 2], [f64; 2])> {
        let mut bounds: Option<([f64; 2], [f64; 2])> = None;
        let mut fold = |x: f64, y: f64| match &mut bounds {
            Some((min, max)) => {
                min[0] = min[0].min(x);
                min[1] = min[1].min(y);
                max[0] = max[0].max(x);
                max[1] = max[1].max(y);
            }
            None => bounds = Some(([x, y], [x, y])),
        };
        fn fold_ring(ring: &LineString<f64>, fold: &mut impl FnMut(f64, f64)) {
            for coord in &ring.0 {
                fold(coord.x, coord.y);
            }
        }
        fn fold_polygon(polygon: &Polygon<f64>, fold: &mut impl FnMut(f64, f64)) {
            fold_ring(polygon.exterior(), fold);
            for ring in polygon.interiors() {
                fold_ring(ring, fold);
            }
        }
        match self {
            Self::Point(point) => fold(point.x(), point.y()),
            Self::Envelope(rect) => {
                fold(rect.min().x, rect.min().y);
                fold(rect.max().x, rect.max().y);
            }
            Self::LineString(line) => fold_ring(line, &mut fold),
            Self::Polygon(polygon) => fold_polygon(polygon, &mut fold),
            Self::MultiPoint(points) => {
                for point in points {
                    fold(point.x(), point.y());
                }
            }
            Self::MultiLineString(lines) => {
                for line in lines {
                    fold_ring(line, &mut fold);
                }
            }
            Self::MultiPolygon(polygons) => {
                for polygon in polygons {
                    fold_polygon(polygon, &mut fold);
                }
            }
            Self::GeometryCollection(members) => {
                for member in members {
                    if let Some((min, max)) = member.coordinate_bounds() {
                        fold(min[0], min[1]);
                        fold(max[0], max[1]);
                    }
                }
            }
        }
        bounds
    }

    /// Little-endian OGC WKB without an SRID.
    pub fn to_wkb(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(64);
        write_wkb(self, None, &mut out);
        out
    }

    /// PostGIS-style EWKB, little-endian, stamped with WGS84 (4326).
    pub fn to_ewkb(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(64);
        write_wkb(self, Some(WGS84_SRID), &mut out);
        out
    }

    /// Parses WKB or EWKB, either endianness. EWKB SRIDs other than 4326
    /// (or 0) are rejected: BicDB geometry is geography-first and refusing
    /// beats silently misreading projected coordinates as degrees.
    pub fn from_wkb(bytes: &[u8]) -> Result<Self> {
        let mut cursor = WkbCursor { bytes, offset: 0 };
        let geometry = read_wkb(&mut cursor, 0)?;
        if cursor.offset != cursor.bytes.len() {
            return Err(geometry_error("WKB has trailing bytes"));
        }
        Ok(geometry)
    }

    /// Parses a hex-encoded (E)WKB string — the text-protocol shape PostGIS
    /// clients produce (with or without a leading `\x`).
    pub fn from_wkb_hex(hex_text: &str) -> Result<Self> {
        let trimmed = hex_text.trim();
        let trimmed = trimmed.strip_prefix("\\x").unwrap_or(trimmed);
        let bytes = hex::decode(trimmed)
            .map_err(|error| geometry_error(format!("invalid WKB hex: {error}")))?;
        Self::from_wkb(&bytes)
    }
}

struct WkbCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl WkbCursor<'_> {
    fn take(&mut self, len: usize) -> Result<&[u8]> {
        let end = self.offset.saturating_add(len);
        if end > self.bytes.len() {
            return Err(geometry_error("truncated WKB"));
        }
        let bytes = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self, big_endian: bool) -> Result<u32> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| geometry_error("truncated WKB u32"))?;
        Ok(if big_endian {
            u32::from_be_bytes(bytes)
        } else {
            u32::from_le_bytes(bytes)
        })
    }

    fn f64(&mut self, big_endian: bool) -> Result<f64> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| geometry_error("truncated WKB f64"))?;
        let value = if big_endian {
            f64::from_be_bytes(bytes)
        } else {
            f64::from_le_bytes(bytes)
        };
        if !value.is_finite() {
            return Err(geometry_error("WKB contains non-finite coordinate"));
        }
        Ok(value)
    }

    fn count(&mut self, big_endian: bool) -> Result<usize> {
        let count = self.u32(big_endian)? as usize;
        // 16 bytes per coordinate pair is the smallest element footprint;
        // reject counts the buffer cannot possibly hold instead of
        // allocating from attacker-controlled lengths.
        if count > self.bytes.len() / 16 + 1 {
            return Err(geometry_error("WKB element count exceeds buffer"));
        }
        Ok(count)
    }
}

fn write_wkb(geometry: &Geometry, srid: Option<u32>, out: &mut Vec<u8>) {
    out.push(1); // little endian
    let type_code = match geometry {
        Geometry::Point(_) => WKB_POINT,
        Geometry::LineString(_) => WKB_LINESTRING,
        Geometry::Polygon(_) | Geometry::Envelope(_) => WKB_POLYGON,
        Geometry::MultiPoint(_) => WKB_MULTI_POINT,
        Geometry::MultiLineString(_) => WKB_MULTI_LINESTRING,
        Geometry::MultiPolygon(_) => WKB_MULTI_POLYGON,
        Geometry::GeometryCollection(_) => WKB_GEOMETRY_COLLECTION,
    };
    let flagged = type_code | if srid.is_some() { EWKB_SRID_FLAG } else { 0 };
    out.extend_from_slice(&flagged.to_le_bytes());
    if let Some(srid) = srid {
        out.extend_from_slice(&srid.to_le_bytes());
    }
    let write_coord = |out: &mut Vec<u8>, x: f64, y: f64| {
        out.extend_from_slice(&x.to_le_bytes());
        out.extend_from_slice(&y.to_le_bytes());
    };
    let write_ring = |out: &mut Vec<u8>, ring: &LineString<f64>| {
        out.extend_from_slice(&(ring.0.len() as u32).to_le_bytes());
        for coord in &ring.0 {
            write_coord(out, coord.x, coord.y);
        }
    };
    let write_polygon = |out: &mut Vec<u8>, polygon: &Polygon<f64>| {
        out.extend_from_slice(&((1 + polygon.interiors().len()) as u32).to_le_bytes());
        write_ring(out, polygon.exterior());
        for ring in polygon.interiors() {
            write_ring(out, ring);
        }
    };
    match geometry {
        Geometry::Point(point) => write_coord(out, point.x(), point.y()),
        Geometry::LineString(line) => write_ring(out, line),
        Geometry::Polygon(polygon) => write_polygon(out, polygon),
        Geometry::Envelope(rect) => write_polygon(out, &envelope_to_polygon(rect)),
        Geometry::MultiPoint(points) => {
            out.extend_from_slice(&(points.0.len() as u32).to_le_bytes());
            for point in points {
                write_wkb(&Geometry::Point(*point), None, out);
            }
        }
        Geometry::MultiLineString(lines) => {
            out.extend_from_slice(&(lines.0.len() as u32).to_le_bytes());
            for line in lines {
                write_wkb(&Geometry::LineString(line.clone()), None, out);
            }
        }
        Geometry::MultiPolygon(polygons) => {
            out.extend_from_slice(&(polygons.0.len() as u32).to_le_bytes());
            for polygon in polygons {
                write_wkb(&Geometry::Polygon(polygon.clone()), None, out);
            }
        }
        Geometry::GeometryCollection(members) => {
            out.extend_from_slice(&(members.len() as u32).to_le_bytes());
            for member in members {
                write_wkb(member, None, out);
            }
        }
    }
}

fn read_wkb(cursor: &mut WkbCursor<'_>, depth: usize) -> Result<Geometry> {
    if depth > MAX_GEOMETRY_NESTING_DEPTH {
        return Err(geometry_error(format!(
            "WKB nesting exceeds maximum depth {MAX_GEOMETRY_NESTING_DEPTH}"
        )));
    }
    let big_endian = match cursor.u8()? {
        0 => true,
        1 => false,
        other => {
            return Err(geometry_error(format!(
                "invalid WKB byte order marker {other}"
            )));
        }
    };
    let raw_type = cursor.u32(big_endian)?;
    if raw_type & EWKB_SRID_FLAG != 0 {
        if depth != 0 {
            return Err(geometry_error("nested WKB member carries an SRID"));
        }
        let srid = cursor.u32(big_endian)?;
        if srid != 0 && srid != WGS84_SRID {
            return Err(geometry_error(format!(
                "unsupported SRID {srid}: BicDB geometry is geographic WGS84 (4326)"
            )));
        }
    }
    // Mask EWKB dimension flags too (Z/M unsupported; their coordinate
    // layout differs, so refuse rather than misparse).
    let type_code = raw_type & 0x0000_FFFF;
    if raw_type & 0xC000_0000 & !EWKB_SRID_FLAG != 0 || (1000..=3007).contains(&type_code) {
        return Err(geometry_error(
            "WKB with Z/M dimensions is not supported (2D geographic only)",
        ));
    }
    let read_ring = |cursor: &mut WkbCursor<'_>| -> Result<LineString<f64>> {
        let count = cursor.count(big_endian)?;
        let mut coords = Vec::with_capacity(count);
        for _ in 0..count {
            let x = cursor.f64(big_endian)?;
            let y = cursor.f64(big_endian)?;
            validate_coord(x, y)?;
            coords.push(Coord { x, y });
        }
        Ok(LineString::new(coords))
    };
    let read_polygon = |cursor: &mut WkbCursor<'_>| -> Result<Polygon<f64>> {
        let ring_count = cursor.count(big_endian)?;
        if ring_count == 0 {
            return Err(geometry_error("WKB polygon has no rings"));
        }
        let exterior = read_ring(cursor)?;
        let mut interiors = Vec::with_capacity(ring_count - 1);
        for _ in 1..ring_count {
            interiors.push(read_ring(cursor)?);
        }
        validate_polygon(&exterior, &interiors)?;
        Ok(Polygon::new(exterior, interiors))
    };
    match type_code {
        WKB_POINT => {
            let x = cursor.f64(big_endian)?;
            let y = cursor.f64(big_endian)?;
            Geometry::point(x, y)
        }
        WKB_LINESTRING => {
            let line = read_ring(cursor)?;
            validate_linestring(&line)?;
            Ok(Geometry::LineString(line))
        }
        WKB_POLYGON => Ok(Geometry::Polygon(read_polygon(cursor)?)),
        WKB_MULTI_POINT => {
            let count = cursor.count(big_endian)?;
            let mut points = Vec::with_capacity(count);
            for _ in 0..count {
                match read_wkb(cursor, depth.saturating_add(1))? {
                    Geometry::Point(point) => points.push(point),
                    _ => return Err(geometry_error("WKB MultiPoint member is not a point")),
                }
            }
            Ok(Geometry::MultiPoint(MultiPoint::new(points)))
        }
        WKB_MULTI_LINESTRING => {
            let count = cursor.count(big_endian)?;
            let mut lines = Vec::with_capacity(count);
            for _ in 0..count {
                match read_wkb(cursor, depth.saturating_add(1))? {
                    Geometry::LineString(line) => lines.push(line),
                    _ => {
                        return Err(geometry_error(
                            "WKB MultiLineString member is not a linestring",
                        ));
                    }
                }
            }
            Ok(Geometry::MultiLineString(MultiLineString::new(lines)))
        }
        WKB_MULTI_POLYGON => {
            let count = cursor.count(big_endian)?;
            let mut polygons = Vec::with_capacity(count);
            for _ in 0..count {
                match read_wkb(cursor, depth.saturating_add(1))? {
                    Geometry::Polygon(polygon) => polygons.push(polygon),
                    _ => return Err(geometry_error("WKB MultiPolygon member is not a polygon")),
                }
            }
            Ok(Geometry::MultiPolygon(MultiPolygon::new(polygons)))
        }
        WKB_GEOMETRY_COLLECTION => {
            let count = cursor.count(big_endian)?;
            let mut members = Vec::with_capacity(count);
            for _ in 0..count {
                members.push(read_wkb(cursor, depth.saturating_add(1))?);
            }
            Ok(Geometry::GeometryCollection(members))
        }
        other => Err(geometry_error(format!("unsupported WKB type {other}"))),
    }
}

fn geometry_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Geometry(message.into())
}
