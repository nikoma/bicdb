//! Mapbox Vector Tile (MVT v2.1) generation: Web Mercator projection,
//! buffered tile clipping (Sutherland–Hodgman for rings, segment clipping
//! for lines), integer-snap generalization, and a hand-rolled protobuf
//! writer — the format is small enough that a dependency would cost more
//! than these ~200 lines.

use bicdb_core::Geometry;

pub(crate) const MVT_EXTENT: i64 = 4096;
pub(crate) const MVT_BUFFER: i64 = 64;

/// A feature's tile-local geometry after projection + clipping.
enum TileGeometry {
    Points(Vec<(i64, i64)>),
    Lines(Vec<Vec<(i64, i64)>>),
    Polygons(Vec<Vec<(i64, i64)>>),
}

pub(crate) struct TileFeature {
    pub id: u64,
    pub tags: Vec<(String, TileValue)>,
    geometry: TileGeometry,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum TileValue {
    Text(String),
    Float(f64),
    Int(i64),
    Bool(bool),
}

/// lon/lat → tile-local integer coordinates for z/x/y (top-left origin).
fn project(lon: f64, lat: f64, z: u32, x: u32, y: u32) -> (f64, f64) {
    let scale = f64::from(1_u32 << z.min(31));
    let mercator_x = (lon + 180.0) / 360.0;
    let lat_rad = lat.to_radians().clamp(-1.484_422, 1.484_422); // ±85.05°
    let mercator_y =
        (1.0 - ((lat_rad.tan() + 1.0 / lat_rad.cos()).ln()) / std::f64::consts::PI) / 2.0;
    (
        (mercator_x * scale - f64::from(x)) * MVT_EXTENT as f64,
        (mercator_y * scale - f64::from(y)) * MVT_EXTENT as f64,
    )
}

fn clip_range() -> (f64, f64) {
    (-(MVT_BUFFER as f64), (MVT_EXTENT + MVT_BUFFER) as f64)
}

/// Sutherland–Hodgman against the buffered tile square.
fn clip_ring(ring: &[(f64, f64)]) -> Vec<(f64, f64)> {
    let (low, high) = clip_range();
    let mut output: Vec<(f64, f64)> = ring.to_vec();
    for edge in 0..4 {
        if output.is_empty() {
            return output;
        }
        let input = std::mem::take(&mut output);
        let inside = |point: (f64, f64)| match edge {
            0 => point.0 >= low,
            1 => point.0 <= high,
            2 => point.1 >= low,
            _ => point.1 <= high,
        };
        let intersect = |a: (f64, f64), b: (f64, f64)| -> (f64, f64) {
            let (boundary, vertical) = match edge {
                0 => (low, true),
                1 => (high, true),
                2 => (low, false),
                _ => (high, false),
            };
            if vertical {
                let t = (boundary - a.0) / (b.0 - a.0);
                (boundary, a.1 + t * (b.1 - a.1))
            } else {
                let t = (boundary - a.1) / (b.1 - a.1);
                (a.0 + t * (b.0 - a.0), boundary)
            }
        };
        for index in 0..input.len() {
            let current = input[index];
            let previous = input[(index + input.len() - 1) % input.len()];
            match (inside(previous), inside(current)) {
                (true, true) => output.push(current),
                (true, false) => output.push(intersect(previous, current)),
                (false, true) => {
                    output.push(intersect(previous, current));
                    output.push(current);
                }
                (false, false) => {}
            }
        }
    }
    output
}

/// Clips an open line to the buffered square, splitting where it exits.
fn clip_line(line: &[(f64, f64)]) -> Vec<Vec<(f64, f64)>> {
    let (low, high) = clip_range();
    let inside =
        |point: (f64, f64)| point.0 >= low && point.0 <= high && point.1 >= low && point.1 <= high;
    let mut parts: Vec<Vec<(f64, f64)>> = Vec::new();
    let mut current: Vec<(f64, f64)> = Vec::new();
    for window in line.windows(2) {
        let (a, b) = (window[0], window[1]);
        // Liang–Barsky parametric clip of segment a→b.
        let (dx, dy) = (b.0 - a.0, b.1 - a.1);
        let mut t0 = 0.0_f64;
        let mut t1 = 1.0_f64;
        let mut accept = true;
        for (p, q) in [
            (-dx, a.0 - low),
            (dx, high - a.0),
            (-dy, a.1 - low),
            (dy, high - a.1),
        ] {
            if p == 0.0 {
                if q < 0.0 {
                    accept = false;
                    break;
                }
            } else {
                let r = q / p;
                if p < 0.0 {
                    t0 = t0.max(r);
                } else {
                    t1 = t1.min(r);
                }
                if t0 > t1 {
                    accept = false;
                    break;
                }
            }
        }
        if !accept {
            if !current.is_empty() {
                parts.push(std::mem::take(&mut current));
            }
            continue;
        }
        let start = (a.0 + t0 * dx, a.1 + t0 * dy);
        let end = (a.0 + t1 * dx, a.1 + t1 * dy);
        if current.is_empty() || !inside(a) || t0 > 0.0 {
            if !current.is_empty() {
                parts.push(std::mem::take(&mut current));
            }
            current.push(start);
        }
        current.push(end);
    }
    if !current.is_empty() {
        parts.push(current);
    }
    parts
}

/// Integer snap + consecutive-duplicate drop: the generalization step that
/// makes coarse-zoom tiles small.
fn snap(points: &[(f64, f64)], closed: bool) -> Vec<(i64, i64)> {
    let mut snapped: Vec<(i64, i64)> = Vec::with_capacity(points.len());
    for point in points {
        let candidate = (point.0.round() as i64, point.1.round() as i64);
        if snapped.last() != Some(&candidate) {
            snapped.push(candidate);
        }
    }
    if closed && snapped.len() > 1 && snapped.first() == snapped.last() {
        snapped.pop();
    }
    snapped
}

pub(crate) fn tile_feature(
    id: u64,
    geometry: &Geometry,
    z: u32,
    x: u32,
    y: u32,
    tags: Vec<(String, TileValue)>,
) -> Option<TileFeature> {
    let proj = |lon: f64, lat: f64| project(lon, lat, z, x, y);
    let line_parts = |line: &geo::LineString<f64>| -> Vec<Vec<(i64, i64)>> {
        let projected: Vec<(f64, f64)> = line.0.iter().map(|c| proj(c.x, c.y)).collect();
        clip_line(&projected)
            .iter()
            .map(|part| snap(part, false))
            .filter(|part| part.len() >= 2)
            .collect()
    };
    let polygon_rings = |polygon: &geo::Polygon<f64>| -> Vec<Vec<(i64, i64)>> {
        std::iter::once(polygon.exterior())
            .chain(polygon.interiors())
            .filter_map(|ring| {
                let projected: Vec<(f64, f64)> = ring.0.iter().map(|c| proj(c.x, c.y)).collect();
                let clipped = clip_ring(&projected);
                let snapped = snap(&clipped, true);
                (snapped.len() >= 3).then_some(snapped)
            })
            .collect()
    };
    let geometry = match geometry {
        Geometry::Point(point) => {
            let projected = proj(point.x(), point.y());
            let snapped = snap(&[projected], false);
            let (low, high) = clip_range();
            if projected.0 < low || projected.0 > high || projected.1 < low || projected.1 > high {
                return None;
            }
            TileGeometry::Points(snapped)
        }
        Geometry::MultiPoint(points) => {
            let (low, high) = clip_range();
            let kept: Vec<(i64, i64)> = points
                .iter()
                .map(|point| proj(point.x(), point.y()))
                .filter(|p| p.0 >= low && p.0 <= high && p.1 >= low && p.1 <= high)
                .map(|p| (p.0.round() as i64, p.1.round() as i64))
                .collect();
            if kept.is_empty() {
                return None;
            }
            TileGeometry::Points(kept)
        }
        Geometry::LineString(line) => TileGeometry::Lines(line_parts(line)),
        Geometry::MultiLineString(lines) => {
            TileGeometry::Lines(lines.iter().flat_map(|l| line_parts(l)).collect())
        }
        Geometry::Polygon(polygon) => TileGeometry::Polygons(polygon_rings(polygon)),
        Geometry::MultiPolygon(polygons) => {
            TileGeometry::Polygons(polygons.iter().flat_map(|p| polygon_rings(p)).collect())
        }
        Geometry::Envelope(rect) => TileGeometry::Polygons(polygon_rings(&rect.to_polygon())),
        Geometry::GeometryCollection(_) => return None,
    };
    let empty = match &geometry {
        TileGeometry::Points(points) => points.is_empty(),
        TileGeometry::Lines(lines) => lines.is_empty(),
        TileGeometry::Polygons(rings) => rings.is_empty(),
    };
    (!empty).then_some(TileFeature { id, tags, geometry })
}

// --------------------------- protobuf writing ---------------------------

fn varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
}

fn zigzag(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

fn tag(out: &mut Vec<u8>, field: u32, wire: u8) {
    varint(out, u64::from(field << 3 | u32::from(wire)));
}

fn bytes_field(out: &mut Vec<u8>, field: u32, bytes: &[u8]) {
    tag(out, field, 2);
    varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

fn geometry_commands(geometry: &TileGeometry) -> (u64, Vec<u8>) {
    let mut commands: Vec<u8> = Vec::new();
    let mut cursor = (0_i64, 0_i64);
    let mut emit_moves =
        |commands: &mut Vec<u8>, points: &[(i64, i64)], cursor: &mut (i64, i64)| {
            varint(commands, (1 | (points.len() as u64) << 3) as u64);
            for point in points {
                varint(commands, zigzag(point.0 - cursor.0));
                varint(commands, zigzag(point.1 - cursor.1));
                *cursor = *point;
            }
        };
    match geometry {
        TileGeometry::Points(points) => {
            emit_moves(&mut commands, points, &mut cursor);
            (1, commands)
        }
        TileGeometry::Lines(lines) => {
            for line in lines {
                emit_moves(&mut commands, &line[..1], &mut cursor);
                varint(&mut commands, 2 | ((line.len() - 1) as u64) << 3);
                for point in &line[1..] {
                    varint(&mut commands, zigzag(point.0 - cursor.0));
                    varint(&mut commands, zigzag(point.1 - cursor.1));
                    cursor = *point;
                }
            }
            (2, commands)
        }
        TileGeometry::Polygons(rings) => {
            for ring in rings {
                emit_moves(&mut commands, &ring[..1], &mut cursor);
                varint(&mut commands, 2 | ((ring.len() - 1) as u64) << 3);
                for point in &ring[1..] {
                    varint(&mut commands, zigzag(point.0 - cursor.0));
                    varint(&mut commands, zigzag(point.1 - cursor.1));
                    cursor = *point;
                }
                varint(&mut commands, 7 | 1 << 3); // ClosePath
            }
            (3, commands)
        }
    }
}

/// Encodes one layer into a complete tile.
pub(crate) fn encode_tile(layer_name: &str, features: &[TileFeature]) -> Vec<u8> {
    let mut keys: Vec<String> = Vec::new();
    let mut values: Vec<TileValue> = Vec::new();
    let mut layer: Vec<u8> = Vec::new();

    // version = 2 (field 15), name (1), extent (5)
    tag(&mut layer, 15, 0);
    varint(&mut layer, 2);
    bytes_field(&mut layer, 1, layer_name.as_bytes());

    for feature in features {
        let mut body: Vec<u8> = Vec::new();
        tag(&mut body, 1, 0);
        varint(&mut body, feature.id);
        // tags: packed key/value index pairs
        let mut packed: Vec<u8> = Vec::new();
        for (key, value) in &feature.tags {
            let key_index = keys.iter().position(|k| k == key).unwrap_or_else(|| {
                keys.push(key.clone());
                keys.len() - 1
            });
            let value_index = values.iter().position(|v| v == value).unwrap_or_else(|| {
                values.push(value.clone());
                values.len() - 1
            });
            varint(&mut packed, key_index as u64);
            varint(&mut packed, value_index as u64);
        }
        if !packed.is_empty() {
            bytes_field(&mut body, 2, &packed);
        }
        let (geometry_type, commands) = geometry_commands(&feature.geometry);
        tag(&mut body, 3, 0);
        varint(&mut body, geometry_type);
        bytes_field(&mut body, 4, &commands);
        bytes_field(&mut layer, 2, &body);
    }

    for key in &keys {
        bytes_field(&mut layer, 3, key.as_bytes());
    }
    for value in &values {
        let mut body: Vec<u8> = Vec::new();
        match value {
            TileValue::Text(text) => bytes_field(&mut body, 1, text.as_bytes()),
            TileValue::Float(number) => {
                tag(&mut body, 3, 1);
                body.extend_from_slice(&number.to_le_bytes());
            }
            TileValue::Int(number) => {
                tag(&mut body, 4, 0);
                varint(&mut body, *number as u64);
            }
            TileValue::Bool(flag) => {
                tag(&mut body, 7, 0);
                varint(&mut body, u64::from(*flag));
            }
        }
        bytes_field(&mut layer, 4, &body);
    }

    let mut tile: Vec<u8> = Vec::new();
    bytes_field(&mut tile, 3, &layer);
    tile
}
