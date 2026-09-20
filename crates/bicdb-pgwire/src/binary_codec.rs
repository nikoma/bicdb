//! Split out of the parent module to keep files digestible; behavior
//! unchanged. Items are re-exported from the parent via `pub(crate) use`.
use super::*;
#[allow(unused_imports)]
use crate::*;

pub(crate) fn decode_binary_catalog_vector_parameter(
    bytes: &[u8],
    oid: i32,
    db: Option<&BicDb>,
) -> Result<String> {
    let element_oid = if oid == 22 { 21 } else { 26 };
    let literal = decode_binary_array_parameter_inner(bytes, element_oid, db)?;
    let raw = unquote_binary_sql_literal(&literal);
    let body = raw.rsplit_once('=').map_or(raw.as_str(), |(_, body)| body);
    let body = body
        .strip_prefix('{')
        .and_then(|body| body.strip_suffix('}'))
        .ok_or_else(|| PgWireError::Protocol(format!("invalid binary catalog vector oid {oid}")))?;
    if body.is_empty() {
        return Ok(quote_sql_string(""));
    }
    let values = body
        .split(',')
        .map(|value| {
            let value = value.trim().trim_matches('"');
            if value.eq_ignore_ascii_case("NULL") {
                return Err(PgWireError::Protocol(
                    "catalog vectors cannot contain null elements".to_string(),
                ));
            }
            Ok(value)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(quote_sql_string(&values.join(" ")))
}

pub(crate) fn decode_binary_float_parameter_text(bytes: &[u8], oid: i32) -> Result<String> {
    match oid {
        700 => {
            expect_binary_len(bytes, oid, 4)?;
            let value = f32::from_be_bytes(bytes.try_into().unwrap());
            Ok(bicdb_sql::postgres_float_text(f64::from(value), "float4"))
        }
        701 => {
            expect_binary_len(bytes, oid, 8)?;
            let value = f64::from_be_bytes(bytes.try_into().unwrap());
            Ok(bicdb_sql::postgres_float_text(value, "float8"))
        }
        _ => Err(PgWireError::Protocol(format!(
            "binary float decoder does not support oid {oid}"
        ))),
    }
}

pub(crate) fn decode_binary_text_parameter(bytes: &[u8]) -> Result<String> {
    let value =
        std::str::from_utf8(bytes).map_err(|error| PgWireError::Protocol(error.to_string()))?;
    Ok(quote_sql_string(value))
}

pub(crate) fn decode_binary_vector_parameter(bytes: &[u8]) -> Result<String> {
    if bytes.len() < 4 {
        return Err(PgWireError::Protocol(
            "vector binary parameter is shorter than its header".to_string(),
        ));
    }
    let dimensions = i16::from_be_bytes(bytes[0..2].try_into().unwrap());
    let reserved = i16::from_be_bytes(bytes[2..4].try_into().unwrap());
    if !(1..=16_000).contains(&dimensions) {
        return Err(PgWireError::Protocol(format!(
            "invalid vector binary dimension {dimensions}"
        )));
    }
    if reserved != 0 {
        return Err(PgWireError::Protocol(format!(
            "unsupported vector binary reserved value {reserved}"
        )));
    }
    let expected = 4 + dimensions as usize * 4;
    if bytes.len() != expected {
        return Err(PgWireError::Protocol(format!(
            "vector binary parameter length {} does not match dimension {dimensions}",
            bytes.len()
        )));
    }
    let mut values = Vec::with_capacity(dimensions as usize);
    for chunk in bytes[4..].chunks_exact(4) {
        let value = f32::from_bits(u32::from_be_bytes(chunk.try_into().unwrap()));
        if !value.is_finite() {
            return Err(PgWireError::Protocol(
                "vector binary parameter contains a non-finite value".to_string(),
            ));
        }
        values.push(bicdb_sql::postgres_float_text(f64::from(value), "float4"));
    }
    Ok(quote_sql_string(&format!("[{}]", values.join(","))))
}

pub(crate) fn decode_versioned_text_parameter(
    bytes: &[u8],
    oid: i32,
    label: &str,
) -> Result<String> {
    let (version, payload) = bytes.split_first().ok_or_else(|| {
        PgWireError::Protocol(format!("{label} binary parameter is missing version byte"))
    })?;
    if *version != 1 {
        return Err(PgWireError::Protocol(format!(
            "unsupported {label} binary version {version} for oid {oid}"
        )));
    }
    decode_binary_text_parameter(payload)
}

pub(crate) fn decode_binary_bit_parameter(bytes: &[u8], oid: i32) -> Result<String> {
    if bytes.len() < 4 {
        return Err(PgWireError::Protocol(format!(
            "binary bit parameter oid {oid} requires a four-byte bit length"
        )));
    }
    let bit_len = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
    if bit_len < 0 {
        return Err(PgWireError::Protocol(format!(
            "binary bit parameter oid {oid} has negative bit length {bit_len}"
        )));
    }
    let bit_len = bit_len as usize;
    let byte_len = bit_len.div_ceil(8);
    let expected = 4_usize.checked_add(byte_len).ok_or_else(|| {
        PgWireError::Protocol(format!("binary bit parameter oid {oid} length overflow"))
    })?;
    expect_binary_len(bytes, oid, expected)?;

    let mut packed = bytes[4..].to_vec();
    let unused = byte_len.saturating_mul(8).saturating_sub(bit_len);
    if unused > 0 {
        if let Some(last) = packed.last_mut() {
            *last &= u8::MAX << unused;
        }
    }
    let value = PgBitString::new(packed, bit_len)
        .map_err(|error| PgWireError::Protocol(error.to_string()))?;
    Ok(quote_sql_string(&value.to_bit_text()))
}

pub(crate) fn decode_binary_network_parameter(bytes: &[u8], oid: i32) -> Result<String> {
    if bytes.len() < 4 {
        return Err(PgWireError::Protocol(format!(
            "binary network parameter oid {oid} requires a four-byte header"
        )));
    }
    let (address, expected_len, max_prefix) = match bytes[0] {
        2 => {
            let octets: [u8; 4] = bytes
                .get(4..8)
                .ok_or_else(|| PgWireError::Protocol("truncated IPv4 payload".to_string()))?
                .try_into()
                .unwrap();
            (PgIpAddress::V4(octets), 4_usize, 32_u8)
        }
        3 => {
            let octets: [u8; 16] = bytes
                .get(4..20)
                .ok_or_else(|| PgWireError::Protocol("truncated IPv6 payload".to_string()))?
                .try_into()
                .unwrap();
            (PgIpAddress::V6(octets), 16_usize, 128_u8)
        }
        family => {
            return Err(PgWireError::Protocol(format!(
                "invalid network address family {family} for oid {oid}"
            )));
        }
    };
    if bytes[1] > max_prefix || usize::from(bytes[3]) != expected_len {
        return Err(PgWireError::Protocol(format!(
            "invalid binary network prefix or address length for oid {oid}"
        )));
    }
    expect_binary_len(bytes, oid, 4 + expected_len)?;
    let kind = if oid == 650 {
        PgNetworkKind::Cidr
    } else {
        PgNetworkKind::Inet
    };
    let network = PgNetwork::new(kind, address, bytes[1])
        .map_err(|error| PgWireError::Protocol(error.to_string()))?;
    let text = network.to_postgres_text();
    PgNetwork::from_postgres_text(&text, kind)
        .map_err(|error| PgWireError::Protocol(error.to_string()))?;
    Ok(quote_sql_string(&text))
}

pub(crate) fn decode_binary_mac_parameter(bytes: &[u8], oid: i32) -> Result<String> {
    let address = match oid {
        829 => {
            expect_binary_len(bytes, oid, 6)?;
            PgMacAddress::Mac48(bytes.try_into().unwrap())
        }
        774 if bytes.len() == 6 => PgMacAddress::Mac64([
            bytes[0], bytes[1], bytes[2], 0xff, 0xfe, bytes[3], bytes[4], bytes[5],
        ]),
        774 => {
            expect_binary_len(bytes, oid, 8)?;
            PgMacAddress::Mac64(bytes.try_into().unwrap())
        }
        _ => unreachable!(),
    };
    Ok(quote_sql_string(&address.to_postgres_text()))
}

pub(crate) fn read_binary_geometric_float(
    bytes: &[u8],
    index: &mut usize,
    oid: i32,
) -> Result<PgFloat8> {
    let value = bytes.get(*index..*index + 8).ok_or_else(|| {
        PgWireError::Protocol(format!("truncated geometric payload for oid {oid}"))
    })?;
    *index += 8;
    Ok(PgFloat8::from_value(f64::from_be_bytes(
        value.try_into().unwrap(),
    )))
}

pub(crate) fn read_binary_geometric_point(
    bytes: &[u8],
    index: &mut usize,
    oid: i32,
) -> Result<PgPoint> {
    Ok(PgPoint {
        x: read_binary_geometric_float(bytes, index, oid)?,
        y: read_binary_geometric_float(bytes, index, oid)?,
    })
}

pub(crate) fn bounded_binary_count(
    count: i32,
    remaining_bytes: usize,
    minimum_item_bytes: usize,
    label: &str,
) -> Result<usize> {
    let count = usize::try_from(count)
        .map_err(|_| PgWireError::Protocol(format!("{label} count must not be negative")))?;
    let maximum = remaining_bytes / minimum_item_bytes;
    if count > maximum {
        return Err(PgWireError::Protocol(format!(
            "{label} count {count} exceeds the {maximum} items that fit in the remaining payload"
        )));
    }
    Ok(count)
}

pub(crate) fn decode_binary_geometric_parameter(bytes: &[u8], oid: i32) -> Result<String> {
    let mut index = 0;
    let value = match oid {
        600 => PgGeometric::Point(read_binary_geometric_point(bytes, &mut index, oid)?),
        601 => PgGeometric::LineSegment {
            start: read_binary_geometric_point(bytes, &mut index, oid)?,
            end: read_binary_geometric_point(bytes, &mut index, oid)?,
        },
        603 => {
            let first = read_binary_geometric_point(bytes, &mut index, oid)?;
            let second = read_binary_geometric_point(bytes, &mut index, oid)?;
            PgGeometric::Box {
                high: PgPoint {
                    x: PgFloat8::from_value(first.x.to_value().max(second.x.to_value())),
                    y: PgFloat8::from_value(first.y.to_value().max(second.y.to_value())),
                },
                low: PgPoint {
                    x: PgFloat8::from_value(first.x.to_value().min(second.x.to_value())),
                    y: PgFloat8::from_value(first.y.to_value().min(second.y.to_value())),
                },
            }
        }
        628 => {
            let a = read_binary_geometric_float(bytes, &mut index, oid)?;
            let b = read_binary_geometric_float(bytes, &mut index, oid)?;
            let c = read_binary_geometric_float(bytes, &mut index, oid)?;
            if a.to_value() == 0.0 && b.to_value() == 0.0 {
                return Err(PgWireError::Protocol(
                    "invalid line coefficients: A and B cannot both be zero".to_string(),
                ));
            }
            PgGeometric::Line { a, b, c }
        }
        602 | 604 => {
            let closed = if oid == 602 {
                let value = *bytes
                    .first()
                    .ok_or_else(|| PgWireError::Protocol("truncated path payload".to_string()))?;
                index = 1;
                value != 0
            } else {
                true
            };
            let count = read_i32(bytes, &mut index)?;
            if count <= 0 {
                return Err(PgWireError::Protocol(format!(
                    "geometric oid {oid} requires at least one point"
                )));
            }
            let count = bounded_binary_count(
                count,
                bytes.len().saturating_sub(index),
                16,
                "geometric point",
            )?;
            let mut points = Vec::with_capacity(count);
            for _ in 0..count {
                points.push(read_binary_geometric_point(bytes, &mut index, oid)?);
            }
            if oid == 602 {
                PgGeometric::Path { closed, points }
            } else {
                PgGeometric::Polygon { points }
            }
        }
        718 => {
            let center = read_binary_geometric_point(bytes, &mut index, oid)?;
            let radius = read_binary_geometric_float(bytes, &mut index, oid)?;
            if radius.to_value() < 0.0 {
                return Err(PgWireError::Protocol(
                    "circle radius cannot be negative".to_string(),
                ));
            }
            PgGeometric::Circle { center, radius }
        }
        _ => unreachable!(),
    };
    if index != bytes.len() {
        return Err(PgWireError::Protocol(format!(
            "geometric oid {oid} has {} trailing bytes",
            bytes.len() - index
        )));
    }
    Ok(quote_sql_string(&value.to_postgres_text()))
}

pub(crate) fn decode_binary_snapshot_parameter(bytes: &[u8], oid: i32) -> Result<String> {
    if bytes.len() < 20 {
        return Err(PgWireError::Protocol(format!(
            "snapshot binary parameter oid {oid} is shorter than 20 bytes"
        )));
    }
    let count = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
    if count < 0 {
        return Err(PgWireError::Protocol(
            "snapshot transaction count cannot be negative".to_string(),
        ));
    }
    let expected = 20_usize
        .checked_add((count as usize).checked_mul(8).ok_or_else(|| {
            PgWireError::Protocol("snapshot transaction count overflow".to_string())
        })?)
        .ok_or_else(|| PgWireError::Protocol("snapshot payload length overflow".to_string()))?;
    expect_binary_len(bytes, oid, expected)?;
    let xmin = u64::from_be_bytes(bytes[4..12].try_into().unwrap());
    let xmax = u64::from_be_bytes(bytes[12..20].try_into().unwrap());
    let in_progress = bytes[20..]
        .chunks_exact(8)
        .map(|value| u64::from_be_bytes(value.try_into().unwrap()))
        .collect::<Vec<_>>();
    let snapshot = PgSnapshot::new(xmin, xmax, in_progress)
        .map_err(|error| PgWireError::Protocol(error.to_string()))?;
    Ok(quote_sql_string(&snapshot.to_postgres_text()))
}

pub(crate) fn is_oid_alias_oid(oid: i32) -> bool {
    matches!(
        oid,
        24 | 26 | 2202 | 2203 | 2204 | 2205 | 2206 | 3734 | 3769 | 4089 | 4096 | 4191
    )
}

pub(crate) const RANGE_EMPTY: u8 = 0x01;
pub(crate) const RANGE_LB_INC: u8 = 0x02;
pub(crate) const RANGE_UB_INC: u8 = 0x04;
pub(crate) const RANGE_LB_INF: u8 = 0x08;
pub(crate) const RANGE_UB_INF: u8 = 0x10;

pub(crate) fn decode_binary_range_parameter(bytes: &[u8], range_oid: i32) -> Result<String> {
    let subtype_oid = range_subtype_oid_for_range_oid(range_oid).ok_or_else(|| {
        PgWireError::Protocol(format!("range oid {range_oid} has no registered subtype"))
    })?;
    decode_binary_range_parameter_inner(bytes, range_oid, subtype_oid, None)
}

pub(crate) fn decode_binary_range_parameter_inner(
    bytes: &[u8],
    range_oid: i32,
    subtype_oid: i32,
    db: Option<&BicDb>,
) -> Result<String> {
    let mut index = 0;
    let flags = *bytes
        .first()
        .ok_or_else(|| PgWireError::Protocol("range payload is missing flags".to_string()))?;
    let allowed = RANGE_EMPTY | RANGE_LB_INC | RANGE_UB_INC | RANGE_LB_INF | RANGE_UB_INF;
    if flags & !allowed != 0
        || (flags & RANGE_EMPTY != 0 && flags != RANGE_EMPTY)
        || (flags & RANGE_LB_INF != 0 && flags & RANGE_LB_INC != 0)
        || (flags & RANGE_UB_INF != 0 && flags & RANGE_UB_INC != 0)
    {
        return Err(PgWireError::Protocol(format!(
            "range oid {range_oid} has invalid flags 0x{flags:02x}"
        )));
    }
    index += 1;
    if flags & RANGE_EMPTY != 0 {
        expect_binary_len(bytes, range_oid, 1)?;
        return Ok("empty".to_string());
    }
    let lower = if flags & RANGE_LB_INF != 0 {
        String::new()
    } else {
        decode_binary_range_bound(bytes, &mut index, subtype_oid, db)?
    };
    let upper = if flags & RANGE_UB_INF != 0 {
        String::new()
    } else {
        decode_binary_range_bound(bytes, &mut index, subtype_oid, db)?
    };
    if index != bytes.len() {
        return Err(PgWireError::Protocol(format!(
            "range oid {range_oid} has {} trailing bytes",
            bytes.len() - index
        )));
    }
    let lower_marker = if flags & RANGE_LB_INC != 0 { '[' } else { '(' };
    let upper_marker = if flags & RANGE_UB_INC != 0 { ']' } else { ')' };
    let lower = quote_range_wire_bound(&lower);
    let upper = quote_range_wire_bound(&upper);
    Ok(format!("{lower_marker}{lower},{upper}{upper_marker}"))
}

pub(crate) fn decode_binary_range_bound(
    bytes: &[u8],
    index: &mut usize,
    subtype_oid: i32,
    db: Option<&BicDb>,
) -> Result<String> {
    let length = read_i32(bytes, index)?;
    if length < 0 {
        return Err(PgWireError::Protocol(
            "range bounds cannot use null binary values".to_string(),
        ));
    }
    let length = length as usize;
    let end = index
        .checked_add(length)
        .ok_or_else(|| PgWireError::Protocol("range bound length overflow".to_string()))?;
    let payload = bytes
        .get(*index..end)
        .ok_or_else(|| PgWireError::Protocol("truncated range bound payload".to_string()))?;
    *index = end;
    let literal = match db {
        Some(db) => decode_binary_parameter_with_db(db, payload, subtype_oid)?,
        None => decode_binary_parameter(payload, subtype_oid)?,
    };
    Ok(unquote_binary_sql_literal(&literal))
}

pub(crate) fn unquote_binary_sql_literal(value: &str) -> String {
    value
        .strip_prefix('\'')
        .and_then(|value| value.strip_suffix('\''))
        .map(|value| value.replace("''", "'"))
        .unwrap_or_else(|| value.to_string())
}

pub(crate) fn quote_range_wire_bound(value: &str) -> String {
    if value.is_empty() {
        String::new()
    } else if value
        .chars()
        .any(|ch| ch.is_whitespace() || matches!(ch, '"' | '\\' | ',' | '(' | ')' | '[' | ']'))
    {
        format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
    } else {
        value.to_string()
    }
}

pub(crate) fn decode_binary_multirange_parameter(
    bytes: &[u8],
    multirange_oid: i32,
) -> Result<String> {
    let range_oid = range_oid_for_multirange_oid(multirange_oid).ok_or_else(|| {
        PgWireError::Protocol(format!(
            "multirange oid {multirange_oid} has no registered range type"
        ))
    })?;
    decode_binary_multirange_parameter_inner(
        bytes,
        multirange_oid,
        range_oid,
        range_subtype_oid_for_range_oid(range_oid).ok_or_else(|| {
            PgWireError::Protocol(format!("range oid {range_oid} has no registered subtype"))
        })?,
        None,
    )
}

pub(crate) fn decode_binary_multirange_parameter_inner(
    bytes: &[u8],
    multirange_oid: i32,
    range_oid: i32,
    subtype_oid: i32,
    db: Option<&BicDb>,
) -> Result<String> {
    let mut index = 0;
    let count = read_i32(bytes, &mut index)?;
    if count < 0 {
        return Err(PgWireError::Protocol(
            "multirange range count cannot be negative".to_string(),
        ));
    }
    let count = bounded_binary_count(
        count,
        bytes.len().saturating_sub(index),
        4,
        "multirange range",
    )?;
    let mut ranges = Vec::with_capacity(count);
    for _ in 0..count {
        let length = read_i32(bytes, &mut index)?;
        if length < 0 {
            return Err(PgWireError::Protocol(
                "multirange entries cannot be null".to_string(),
            ));
        }
        let end = index
            .checked_add(length as usize)
            .ok_or_else(|| PgWireError::Protocol("multirange length overflow".to_string()))?;
        let payload = bytes
            .get(index..end)
            .ok_or_else(|| PgWireError::Protocol("truncated multirange payload".to_string()))?;
        index = end;
        ranges.push(decode_binary_range_parameter_inner(
            payload,
            range_oid,
            subtype_oid,
            db,
        )?);
    }
    if index != bytes.len() {
        return Err(PgWireError::Protocol(format!(
            "multirange oid {multirange_oid} has {} trailing bytes",
            bytes.len() - index
        )));
    }
    Ok(format!("{{{}}}", ranges.join(",")))
}

pub(crate) fn range_subtype_oid_for_range_oid(oid: i32) -> Option<i32> {
    bicdb_sql::pg_type_spec_by_oid(oid).and_then(|spec| spec.range_subtype_oid())
}

pub(crate) fn range_oid_for_multirange_oid(oid: i32) -> Option<i32> {
    Some(match oid {
        4451 => 3904,
        4532 => 3906,
        4533 => 3908,
        4534 => 3910,
        4535 => 3912,
        4536 => 3926,
        _ => return None,
    })
}

pub(crate) fn decode_binary_array_parameter(
    bytes: &[u8],
    expected_element_oid: i32,
) -> Result<String> {
    decode_binary_array_parameter_inner(bytes, expected_element_oid, None)
}

pub(crate) fn decode_binary_array_parameter_inner(
    bytes: &[u8],
    expected_element_oid: i32,
    db: Option<&BicDb>,
) -> Result<String> {
    let mut idx = 0;
    let rank = read_i32(bytes, &mut idx)?;
    let has_nulls = read_i32(bytes, &mut idx)?;
    let element_oid = read_i32(bytes, &mut idx)?;
    if !(0..=6).contains(&rank) {
        return Err(PgWireError::Protocol(format!(
            "binary array rank {rank} is outside PostgreSQL's 0..=6 range"
        )));
    }
    if !matches!(has_nulls, 0 | 1) {
        return Err(PgWireError::Protocol(format!(
            "binary array null flag must be 0 or 1, got {has_nulls}"
        )));
    }
    if element_oid != 0 && element_oid != expected_element_oid {
        return Err(PgWireError::Protocol(format!(
            "binary array element oid {element_oid} does not match expected oid {expected_element_oid}"
        )));
    }

    let mut dimensions = Vec::with_capacity(rank as usize);
    let mut element_count = if rank == 0 { 0_usize } else { 1_usize };
    for _ in 0..rank {
        let length = read_i32(bytes, &mut idx)?;
        let lower_bound = read_i32(bytes, &mut idx)?;
        if length < 0 {
            return Err(PgWireError::Protocol(
                "binary array dimension length must not be negative".to_string(),
            ));
        }
        let length = length as usize;
        element_count = element_count.checked_mul(length).ok_or_else(|| {
            PgWireError::Protocol("binary array element count overflow".to_string())
        })?;
        dimensions.push((length, lower_bound));
    }
    if rank == 0 {
        if idx != bytes.len() {
            return Err(PgWireError::Protocol(
                "zero-dimensional binary array has trailing bytes".to_string(),
            ));
        }
        return Ok(quote_sql_string("{}"));
    }
    if element_count > bytes.len().saturating_sub(idx) / 4 {
        return Err(PgWireError::Protocol(format!(
            "binary array dimensions require {element_count} elements but the payload is too short"
        )));
    }

    let mut values = Vec::with_capacity(element_count);
    for _ in 0..element_count {
        let len = read_i32(bytes, &mut idx)?;
        if len == -1 {
            values.push(None);
            continue;
        }
        if len < -1 {
            return Err(PgWireError::Protocol(format!(
                "binary array element has invalid length {len}"
            )));
        }
        let len = len as usize;
        let end = idx.checked_add(len).ok_or_else(|| {
            PgWireError::Protocol("binary array element length overflow".to_string())
        })?;
        let payload = bytes.get(idx..end).ok_or_else(|| {
            PgWireError::Protocol("binary array element length exceeds payload".to_string())
        })?;
        let value = if expected_element_oid == 18 {
            expect_binary_len(payload, expected_element_oid, 1)?;
            i8::from_ne_bytes([payload[0]]).to_string()
        } else if matches!(expected_element_oid, 700 | 701) {
            decode_binary_float_parameter_text(payload, expected_element_oid)?
        } else if let Some(db) = db.filter(|db| {
            bicdb_sql::pg_is_table_row_type_oid(db, expected_element_oid).unwrap_or(false)
        }) {
            decode_binary_composite_array_element(db, payload, expected_element_oid)?
        } else {
            let literal = match db {
                Some(db) => decode_binary_parameter_with_db(db, payload, expected_element_oid)?,
                None => decode_binary_parameter(payload, expected_element_oid)?,
            };
            unquote_binary_sql_literal(&literal)
        };
        values.push(Some(value));
        idx = end;
    }
    if idx != bytes.len() {
        return Err(PgWireError::Protocol(format!(
            "binary array has {} trailing bytes",
            bytes.len() - idx
        )));
    }
    if element_count == 0 {
        return Ok(quote_sql_string("{}"));
    }
    if expected_element_oid == 18 {
        let mut offset = 0;
        let value = render_binary_char_array_json(&dimensions, &values, 0, &mut offset)?;
        if offset != values.len() {
            return Err(PgWireError::Protocol(
                "binary char array has too many elements".to_string(),
            ));
        }
        let lower_bounds = dimensions
            .iter()
            .map(|(_, lower_bound)| *lower_bound)
            .collect::<Vec<_>>();
        let envelope = serde_json::json!({
            "$bicdb_array_input": {
                "lower_bounds": lower_bounds,
                "value": value,
            }
        });
        return Ok(format!(
            "{}::jsonb",
            quote_sql_string(&envelope.to_string())
        ));
    }

    let mut offset = 0;
    let delimiter = match db {
        Some(db) => array_element_delimiter_with_db(db, expected_element_oid)?,
        None => array_element_delimiter(expected_element_oid),
    };
    let body = render_binary_array_text(&dimensions, &values, 0, &mut offset, delimiter)?;
    if offset != values.len() {
        return Err(PgWireError::Protocol(
            "binary array has too many elements".to_string(),
        ));
    }
    let bounds = dimensions
        .iter()
        .map(|(length, lower)| {
            let upper = i64::from(*lower) + *length as i64 - 1;
            i32::try_from(upper)
                .map(|upper| format!("[{lower}:{upper}]"))
                .map_err(|_| PgWireError::Protocol("binary array bounds overflow".to_string()))
        })
        .collect::<Result<Vec<_>>>()?
        .join("");
    Ok(quote_sql_string(&format!("{bounds}={body}")))
}

pub(crate) fn decode_binary_composite_array_element(
    db: &BicDb,
    bytes: &[u8],
    oid: i32,
) -> Result<String> {
    let mut index = 0usize;
    let field_count = read_i32(bytes, &mut index)?;
    if field_count < 0 {
        return Err(PgWireError::Protocol(
            "binary composite field count must not be negative".to_string(),
        ));
    }
    let field_count = bounded_binary_count(
        field_count,
        bytes.len().saturating_sub(index),
        8,
        "binary composite field",
    )?;
    if let Some((_, fields)) = bicdb_sql::pg_table_row_type_definition(db, oid)? {
        if fields.len() != field_count {
            return Err(PgWireError::Protocol(format!(
                "binary composite has {field_count} fields but type OID {oid} requires {}",
                fields.len()
            )));
        }
    }
    let mut values = Vec::with_capacity(field_count);
    for _ in 0..field_count {
        let field_oid = read_i32(bytes, &mut index)?;
        let length = read_i32(bytes, &mut index)?;
        if length == -1 {
            values.push(None);
            continue;
        }
        let length = usize::try_from(length).map_err(|_| {
            PgWireError::Protocol("binary composite field length must not be negative".to_string())
        })?;
        let end = index.checked_add(length).ok_or_else(|| {
            PgWireError::Protocol("binary composite field length overflow".to_string())
        })?;
        let field = bytes
            .get(index..end)
            .ok_or_else(|| PgWireError::Protocol("truncated binary composite field".to_string()))?;
        index = end;
        let literal = decode_binary_parameter_with_db(db, field, field_oid)?;
        values.push(Some(unquote_binary_sql_literal(&literal)));
    }
    if index != bytes.len() {
        return Err(PgWireError::Protocol(
            "binary composite has trailing bytes".to_string(),
        ));
    }
    Ok(format!(
        "({})",
        values
            .into_iter()
            .map(|value| value.map_or_else(String::new, composite_array_field_text))
            .collect::<Vec<_>>()
            .join(",")
    ))
}

pub(crate) fn composite_array_field_text(value: String) -> String {
    let quoted = value.is_empty()
        || value.chars().any(|character| {
            character.is_whitespace() || matches!(character, ',' | '(' | ')' | '"' | '\\')
        });
    if !quoted {
        return value;
    }
    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('"');
    for character in value.chars() {
        if matches!(character, '"' | '\\') {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    escaped.push('"');
    escaped
}

pub(crate) fn render_binary_char_array_json(
    dimensions: &[(usize, i32)],
    values: &[Option<String>],
    dimension: usize,
    offset: &mut usize,
) -> Result<JsonValue> {
    let (length, _) = dimensions.get(dimension).ok_or_else(|| {
        PgWireError::Protocol("binary char array rendering exceeded its rank".to_string())
    })?;
    let mut rendered = Vec::with_capacity(*length);
    for _ in 0..*length {
        if dimension + 1 == dimensions.len() {
            let value = values.get(*offset).ok_or_else(|| {
                PgWireError::Protocol("binary char array has too few elements".to_string())
            })?;
            *offset += 1;
            rendered.push(match value {
                None => JsonValue::Null,
                Some(value) => JsonValue::from(value.parse::<i8>().map_err(|_| {
                    PgWireError::Protocol("binary char array contains an invalid byte".to_string())
                })?),
            });
        } else {
            rendered.push(render_binary_char_array_json(
                dimensions,
                values,
                dimension + 1,
                offset,
            )?);
        }
    }
    Ok(JsonValue::Array(rendered))
}

pub(crate) fn render_binary_array_text(
    dimensions: &[(usize, i32)],
    values: &[Option<String>],
    dimension: usize,
    offset: &mut usize,
    delimiter: char,
) -> Result<String> {
    let (length, _) = dimensions.get(dimension).ok_or_else(|| {
        PgWireError::Protocol("binary array rendering exceeded its rank".to_string())
    })?;
    let mut rendered = Vec::with_capacity(*length);
    for _ in 0..*length {
        if dimension + 1 == dimensions.len() {
            let value = values.get(*offset).ok_or_else(|| {
                PgWireError::Protocol("binary array has too few elements".to_string())
            })?;
            *offset += 1;
            rendered.push(match value {
                None => "NULL".to_string(),
                Some(value) => format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\"")),
            });
        } else {
            rendered.push(render_binary_array_text(
                dimensions,
                values,
                dimension + 1,
                offset,
                delimiter,
            )?);
        }
    }
    Ok(format!("{{{}}}", rendered.join(&delimiter.to_string())))
}

pub(crate) fn expect_binary_len(bytes: &[u8], oid: i32, expected: usize) -> Result<()> {
    if bytes.len() == expected {
        Ok(())
    } else {
        Err(PgWireError::Protocol(format!(
            "binary parameter oid {oid} expected {expected} bytes, got {}",
            bytes.len()
        )))
    }
}

pub(crate) fn temporal_binary_error(oid: i32, error: impl std::fmt::Display) -> PgWireError {
    PgWireError::Protocol(format!(
        "invalid binary value for temporal oid {oid}: {error}"
    ))
}

pub(crate) const NUMERIC_POS: u16 = 0x0000;
pub(crate) const NUMERIC_NEG: u16 = 0x4000;
pub(crate) const NUMERIC_NAN: u16 = 0xC000;
pub(crate) const NUMERIC_PINF: u16 = 0xD000;
pub(crate) const NUMERIC_NINF: u16 = 0xF000;
pub(crate) const NUMERIC_DSCALE_MASK: u16 = 0x3FFF;

pub(crate) fn decode_binary_numeric_parameter(bytes: &[u8]) -> Result<String> {
    if bytes.len() < 8 {
        return Err(PgWireError::Protocol(format!(
            "numeric binary parameter requires at least 8 bytes, got {}",
            bytes.len()
        )));
    }
    let ndigits = usize::from(u16::from_be_bytes([bytes[0], bytes[1]]));
    let weight = i16::from_be_bytes([bytes[2], bytes[3]]);
    let sign = u16::from_be_bytes([bytes[4], bytes[5]]);
    let dscale = u16::from_be_bytes([bytes[6], bytes[7]]);
    let expected = 8_usize
        .checked_add(ndigits.checked_mul(2).ok_or_else(|| {
            PgWireError::Protocol("numeric binary digit count overflow".to_string())
        })?)
        .ok_or_else(|| PgWireError::Protocol("numeric binary length overflow".to_string()))?;
    if bytes.len() != expected {
        return Err(PgWireError::Protocol(format!(
            "numeric binary parameter expected {expected} bytes for {ndigits} digits, got {}",
            bytes.len()
        )));
    }
    match sign {
        NUMERIC_NAN => return Ok("NaN".to_string()),
        NUMERIC_PINF => return Ok("Infinity".to_string()),
        NUMERIC_NINF => return Ok("-Infinity".to_string()),
        NUMERIC_POS | NUMERIC_NEG => {}
        _ => {
            return Err(PgWireError::Protocol(format!(
                "numeric binary parameter has invalid sign 0x{sign:04x}"
            )));
        }
    }
    if dscale & !NUMERIC_DSCALE_MASK != 0 {
        return Err(PgWireError::Protocol(format!(
            "numeric binary parameter has invalid display scale {dscale}"
        )));
    }
    let digits = bytes[8..]
        .chunks_exact(2)
        .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
        .collect::<Vec<_>>();
    if let Some(invalid) = digits.iter().find(|digit| **digit > 9999) {
        return Err(PgWireError::Protocol(format!(
            "numeric binary parameter has invalid base-10000 digit {invalid}"
        )));
    }

    let mut whole = String::new();
    if weight >= 0 {
        for position in (0..=i32::from(weight)).rev() {
            let digit_index = i32::from(weight) - position;
            let digit = usize::try_from(digit_index)
                .ok()
                .and_then(|index| digits.get(index))
                .copied()
                .unwrap_or(0);
            if whole.is_empty() {
                whole.push_str(&digit.to_string());
            } else {
                whole.push_str(&format!("{digit:04}"));
            }
        }
    } else {
        whole.push('0');
    }

    let fractional_groups = usize::from(dscale).div_ceil(4);
    let mut fraction = String::with_capacity(fractional_groups.saturating_mul(4));
    for fractional_index in 1..=fractional_groups {
        let digit_index = i32::from(weight) + i32::try_from(fractional_index).unwrap_or(i32::MAX);
        let digit = usize::try_from(digit_index)
            .ok()
            .and_then(|index| digits.get(index))
            .copied()
            .unwrap_or(0);
        fraction.push_str(&format!("{digit:04}"));
    }
    fraction.truncate(usize::from(dscale));
    let mut value = if dscale == 0 {
        whole
    } else {
        format!("{whole}.{fraction}")
    };
    if sign == NUMERIC_NEG && digits.iter().any(|digit| *digit != 0) {
        value.insert(0, '-');
    }
    Ok(value)
}

pub(crate) fn encode_binary_numeric_result(value: &SqlValue) -> Result<Vec<u8>> {
    let numeric = PgNumeric::from_postgres_text(&value.to_cell()).map_err(|_| {
        PgWireError::Protocol(format!(
            "numeric result is not a valid PostgreSQL numeric: {}",
            value.to_cell()
        ))
    })?;
    let (sign, text) = match numeric {
        PgNumeric::NaN => return Ok(numeric_binary_header(0, 0, NUMERIC_NAN, 0)),
        PgNumeric::PositiveInfinity => return Ok(numeric_binary_header(0, 0, NUMERIC_PINF, 0)),
        PgNumeric::NegativeInfinity => return Ok(numeric_binary_header(0, 0, NUMERIC_NINF, 0)),
        PgNumeric::Finite { negative, .. } => (
            if negative { NUMERIC_NEG } else { NUMERIC_POS },
            numeric.to_decimal_text(),
        ),
    };
    let unsigned = text.strip_prefix('-').unwrap_or(&text);
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let dscale = u16::try_from(fraction.len()).map_err(|_| {
        PgWireError::Protocol("numeric display scale exceeds protocol range".to_string())
    })?;
    if dscale & !NUMERIC_DSCALE_MASK != 0 {
        return Err(PgWireError::Protocol(format!(
            "numeric display scale {dscale} exceeds PostgreSQL's limit"
        )));
    }

    let whole = whole.trim_start_matches('0');
    let whole = if whole.is_empty() { "0" } else { whole };
    let whole_groups = whole.len().div_ceil(4);
    let mut grouped = String::with_capacity((whole_groups + fraction.len().div_ceil(4)) * 4);
    grouped.push_str(&"0".repeat(whole_groups * 4 - whole.len()));
    grouped.push_str(whole);
    grouped.push_str(fraction);
    grouped.push_str(&"0".repeat((4 - fraction.len() % 4) % 4));
    let mut digits = grouped
        .as_bytes()
        .chunks_exact(4)
        .map(|chunk| {
            std::str::from_utf8(chunk)
                .ok()
                .and_then(|value| value.parse::<u16>().ok())
                .expect("numeric text contains four ASCII digits")
        })
        .collect::<Vec<_>>();
    let mut weight = i32::try_from(whole_groups).unwrap_or(i32::MAX) - 1;
    let leading_zeroes = digits.iter().take_while(|digit| **digit == 0).count();
    digits.drain(..leading_zeroes);
    weight -= i32::try_from(leading_zeroes).unwrap_or(i32::MAX);
    while digits.last() == Some(&0) {
        digits.pop();
    }
    if digits.is_empty() {
        weight = 0;
    }
    let weight = i16::try_from(weight).map_err(|_| {
        PgWireError::Protocol("numeric weight exceeds PostgreSQL's binary range".to_string())
    })?;
    let ndigits = u16::try_from(digits.len()).map_err(|_| {
        PgWireError::Protocol("numeric digit count exceeds PostgreSQL's binary range".to_string())
    })?;
    let mut output = numeric_binary_header(ndigits, weight, sign, dscale);
    for digit in digits {
        output.extend_from_slice(&digit.to_be_bytes());
    }
    Ok(output)
}

pub(crate) fn numeric_binary_header(ndigits: u16, weight: i16, sign: u16, dscale: u16) -> Vec<u8> {
    let mut output = Vec::with_capacity(8 + usize::from(ndigits) * 2);
    output.extend_from_slice(&ndigits.to_be_bytes());
    output.extend_from_slice(&weight.to_be_bytes());
    output.extend_from_slice(&sign.to_be_bytes());
    output.extend_from_slice(&dscale.to_be_bytes());
    output
}

/// Render one text-format bind parameter as a SQL literal.
///
/// **A bound parameter is data, never SQL.** Prepared statements are the
/// defence applications rely on against injection, and this function is the
/// single point where a parameter becomes query text — so no branch here
/// may emit client bytes unquoted. Numeric and boolean parameters used to
/// be passed through verbatim on the theory that a number cannot be
/// dangerous; nothing checked that the value WAS a number, so
/// `Bind $1::int4 = "1); DROP TABLE t; --"` became executable SQL.
///
/// Every branch now either quotes the value (making it inert) or emits text
/// regenerated from a PARSED value, so the output cannot contain anything
/// the parser did not accept as that type. Values that are not valid for
/// the declared type are refused here, exactly as PostgreSQL refuses them.
pub(crate) fn sql_literal_for_text_parameter(value: &str, oid: i32) -> Result<String> {
    if let Some(element_oid) = array_element_oid(oid) {
        return sql_array_literal_for_text_parameter(value, element_oid);
    }

    match oid {
        16 => sql_bool_parameter_literal(value),
        20 | 21 | 23 => sql_integer_parameter_literal(value, oid),
        700 | 701 => Ok(sql_float_parameter_literal(value, oid)),
        114 | 1700 | 3802 => Ok(quote_sql_string(value)),
        // Unknown/undeclared types: a value that parses as a number is
        // re-emitted FROM THE PARSE so the literal is numeric by
        // construction; everything else is quoted.
        _ => match value.trim().parse::<i64>() {
            Ok(parsed) => Ok(parsed.to_string()),
            Err(_) => match value.trim().parse::<f64>() {
                Ok(parsed) if parsed.is_finite() => Ok(format!(
                    "CAST({} AS float8)",
                    quote_sql_string(&parsed.to_string())
                )),
                _ => Ok(quote_sql_string(value)),
            },
        },
    }
}

/// A boolean parameter, rendered from the parsed value.
pub(crate) fn sql_bool_parameter_literal(value: &str) -> Result<String> {
    let trimmed = value.trim();
    let parsed = if trimmed.eq_ignore_ascii_case("t")
        || trimmed.eq_ignore_ascii_case("true")
        || trimmed.eq_ignore_ascii_case("yes")
        || trimmed.eq_ignore_ascii_case("on")
        || trimmed == "1"
    {
        true
    } else if trimmed.eq_ignore_ascii_case("f")
        || trimmed.eq_ignore_ascii_case("false")
        || trimmed.eq_ignore_ascii_case("no")
        || trimmed.eq_ignore_ascii_case("off")
        || trimmed == "0"
    {
        false
    } else {
        return Err(PgWireError::Protocol(format!(
            "invalid input syntax for type boolean: \"{value}\""
        )));
    };
    Ok(if parsed { "true" } else { "false" }.to_string())
}

/// An integer parameter, rendered from the parsed value and range-checked
/// against the declared width.
pub(crate) fn sql_integer_parameter_literal(value: &str, oid: i32) -> Result<String> {
    let pg_type = match oid {
        20 => "bigint",
        21 => "smallint",
        _ => "integer",
    };
    let parsed: i64 = value.trim().parse().map_err(|_| {
        PgWireError::Protocol(format!(
            "invalid input syntax for type {pg_type}: \"{value}\""
        ))
    })?;
    let in_range = match oid {
        21 => i16::try_from(parsed).is_ok(),
        23 => i32::try_from(parsed).is_ok(),
        _ => true,
    };
    if !in_range {
        return Err(PgWireError::Protocol(format!(
            "value \"{value}\" is out of range for type {pg_type}"
        )));
    }
    Ok(parsed.to_string())
}

pub(crate) fn sql_literal_for_text_parameter_with_db(
    db: &BicDb,
    value: &str,
    oid: i32,
) -> Result<String> {
    if let Some(element_oid) = array_element_oid_with_db(db, oid)? {
        return sql_array_literal_for_text_parameter_with_db(db, value, element_oid);
    }
    if bicdb_sql::pg_is_user_type_oid(db, oid)? {
        return Ok(quote_sql_string(value));
    }
    sql_literal_for_text_parameter(value, oid)
}

pub(crate) fn sql_array_literal_for_text_parameter_with_db(
    db: &BicDb,
    value: &str,
    element_oid: i32,
) -> Result<String> {
    let elements = parse_pg_text_array(value, array_element_delimiter_with_db(db, element_oid)?)?;
    let mut literals = Vec::with_capacity(elements.len());
    for element in elements {
        match element {
            Some(value) => literals.push(sql_literal_for_text_parameter_with_db(
                db,
                &value,
                element_oid,
            )?),
            None => literals.push("NULL".to_string()),
        }
    }
    Ok(format!("ARRAY[{}]", literals.join(", ")))
}

pub(crate) fn sql_float_parameter_literal(value: &str, oid: i32) -> String {
    let pg_type = if oid == 700 { "float4" } else { "float8" };
    format!("CAST({} AS {pg_type})", quote_sql_string(value))
}

pub(crate) fn sql_array_literal_for_text_parameter(
    value: &str,
    element_oid: i32,
) -> Result<String> {
    let elements = parse_pg_text_array(value, array_element_delimiter(element_oid))?;
    let mut literals = Vec::with_capacity(elements.len());
    for element in elements {
        match element {
            Some(value) => literals.push(sql_literal_for_text_parameter(&value, element_oid)?),
            None => literals.push("NULL".to_string()),
        }
    }
    Ok(format!("ARRAY[{}]", literals.join(", ")))
}

pub(crate) fn parse_pg_text_array(value: &str, delimiter: char) -> Result<Vec<Option<String>>> {
    if !delimiter.is_ascii() || matches!(delimiter, '{' | '}' | '"' | '\\') {
        return Err(PgWireError::Protocol(
            "array delimiter must be a safe single-byte character".to_string(),
        ));
    }
    let delimiter = delimiter as u8;
    let trimmed = value.trim();
    let start = trimmed.find('{').ok_or_else(|| {
        PgWireError::Protocol("text array parameter is missing opening brace".to_string())
    })?;
    let end = trimmed.rfind('}').ok_or_else(|| {
        PgWireError::Protocol("text array parameter is missing closing brace".to_string())
    })?;
    if end < start || !trimmed[end + 1..].trim().is_empty() {
        return Err(PgWireError::Protocol(
            "invalid text array parameter".to_string(),
        ));
    }

    let body = &trimmed[start + 1..end];
    if body.trim().is_empty() {
        return Ok(Vec::new());
    }

    let bytes = body.as_bytes();
    let mut idx = 0;
    let mut elements = Vec::new();
    loop {
        while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }
        if idx >= bytes.len() {
            break;
        }
        if bytes[idx] == b'{' || bytes[idx] == b'}' {
            return Err(PgWireError::Protocol(
                "multidimensional text array parameters are not supported".to_string(),
            ));
        }

        let (element, next_idx) = if bytes[idx] == b'"' {
            parse_pg_quoted_array_element(body, idx)?
        } else {
            parse_pg_unquoted_array_element(body, idx, delimiter)
        };
        elements.push(element);
        idx = next_idx;

        while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }
        if idx >= bytes.len() {
            break;
        }
        if bytes[idx] != delimiter {
            return Err(PgWireError::Protocol(
                "text array parameter expected its type delimiter".to_string(),
            ));
        }
        idx += 1;
    }

    Ok(elements)
}

pub(crate) fn parse_pg_quoted_array_element(
    body: &str,
    mut idx: usize,
) -> Result<(Option<String>, usize)> {
    let bytes = body.as_bytes();
    idx += 1;
    let mut value = String::new();
    while idx < bytes.len() {
        match bytes[idx] {
            b'\\' => {
                idx += 1;
                let Some(byte) = bytes.get(idx) else {
                    return Err(PgWireError::Protocol(
                        "text array parameter has trailing escape".to_string(),
                    ));
                };
                value.push(*byte as char);
                idx += 1;
            }
            b'"' => return Ok((Some(value), idx + 1)),
            byte => {
                value.push(byte as char);
                idx += 1;
            }
        }
    }
    Err(PgWireError::Protocol(
        "text array parameter has unterminated quoted element".to_string(),
    ))
}

pub(crate) fn parse_pg_unquoted_array_element(
    body: &str,
    idx: usize,
    delimiter: u8,
) -> (Option<String>, usize) {
    let bytes = body.as_bytes();
    let mut end = idx;
    while end < bytes.len() && bytes[end] != delimiter {
        end += 1;
    }
    let value = body[idx..end].trim();
    if value.eq_ignore_ascii_case("NULL") {
        (None, end)
    } else {
        (Some(value.to_string()), end)
    }
}

pub(crate) fn array_element_oid(oid: i32) -> Option<i32> {
    bicdb_sql::pg_array_element_oid(oid)
}

pub(crate) fn array_element_oid_with_db(db: &BicDb, oid: i32) -> Result<Option<i32>> {
    match array_element_oid(oid) {
        Some(element_oid) => Ok(Some(element_oid)),
        None => match bicdb_sql::pg_user_type_array_element_oid(db, oid)? {
            Some(element_oid) => Ok(Some(element_oid)),
            None => bicdb_sql::pg_table_row_array_element_oid(db, oid).map_err(PgWireError::Sql),
        },
    }
}

pub(crate) fn array_element_delimiter(element_oid: i32) -> char {
    bicdb_sql::pg_type_delimiter_by_oid(element_oid).unwrap_or(',')
}

pub(crate) fn array_element_delimiter_with_db(db: &BicDb, element_oid: i32) -> Result<char> {
    if let Some(delimiter) = bicdb_sql::pg_type_delimiter_by_oid(element_oid) {
        return Ok(delimiter);
    }
    Ok(bicdb_sql::pg_user_type_delimiter(db, element_oid)?.unwrap_or(','))
}

pub(crate) fn quote_sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub(crate) fn substitute_parameters(sql: &str, params: &[String]) -> Result<String> {
    let mut output = String::with_capacity(sql.len() + params.len() * 8);
    let bytes = sql.as_bytes();
    let mut idx = 0;
    let mut positional_idx = 0usize;
    let question_operators = bytes
        .contains(&b'?')
        .then(|| question_mark_operator_flags(sql))
        .flatten();
    let mut question_idx = 0usize;
    while idx < bytes.len() {
        let plain_start = idx;
        while idx < bytes.len() && !matches!(bytes[idx], b'\'' | b'"' | b'-' | b'/' | b'$' | b'?') {
            idx += 1;
        }
        if plain_start != idx {
            output.push_str(&sql[plain_start..idx]);
            if idx >= bytes.len() {
                break;
            }
        }

        match bytes[idx] {
            b'\'' => {
                let end = single_quoted_sql_end(sql, idx);
                output.push_str(&sql[idx..end]);
                idx = end;
            }
            b'"' => {
                let end = double_quoted_sql_end(sql, idx);
                output.push_str(&sql[idx..end]);
                idx = end;
            }
            b'-' if bytes.get(idx + 1) == Some(&b'-') => {
                let end = line_comment_end(sql, idx);
                output.push_str(&sql[idx..end]);
                idx = end;
            }
            b'/' if bytes.get(idx + 1) == Some(&b'*') => {
                let end = block_comment_end(sql, idx);
                output.push_str(&sql[idx..end]);
                idx = end;
            }
            b'$' if dollar_quote_delimiter_len(sql, idx).is_some() => {
                let end = dollar_quoted_sql_end(sql, idx);
                output.push_str(&sql[idx..end]);
                idx = end;
            }
            b'$' => {
                let start = idx + 1;
                let mut end = start;
                while end < bytes.len() && bytes[end].is_ascii_digit() {
                    end += 1;
                }
                if end > start {
                    let param_number = std::str::from_utf8(&bytes[start..end])
                        .map_err(|error| PgWireError::Protocol(error.to_string()))?
                        .parse::<usize>()
                        .map_err(|error| PgWireError::Protocol(error.to_string()))?;
                    let param = params.get(param_number.saturating_sub(1)).ok_or_else(|| {
                        PgWireError::Protocol(format!("missing bind parameter ${param_number}"))
                    })?;
                    output.push_str(param);
                    idx = end;
                    continue;
                }
                output.push('$');
                idx += 1;
            }
            b'?' => {
                let is_operator = question_operators
                    .as_ref()
                    .and_then(|operators| operators.get(question_idx))
                    .copied()
                    .unwrap_or_else(|| question_mark_is_json_operator_fallback(sql, idx));
                question_idx += 1;
                if is_operator {
                    output.push('?');
                    idx += 1;
                    continue;
                }
                let param = params.get(positional_idx).ok_or_else(|| {
                    PgWireError::Protocol(format!("missing bind parameter ?{}", positional_idx + 1))
                })?;
                output.push_str(param);
                positional_idx += 1;
                idx += 1;
            }
            other => {
                output.push(other as char);
                idx += 1;
            }
        }
    }
    Ok(output)
}

pub(crate) fn question_mark_operator_flags(sql: &str) -> Option<Vec<bool>> {
    // PostgreSQL tokenizes `?` as an operator, while several clients use it as
    // an anonymous bind marker. Ignore whitespace/comments and decide from the
    // surrounding expression boundary without disturbing quoted SQL text.
    let dialect = PostgreSqlDialect {};
    let tokens = Tokenizer::new(&dialect, sql).tokenize().ok()?;
    let significant = tokens
        .iter()
        .enumerate()
        .filter(|(_, token)| !matches!(token, Token::Whitespace(_)))
        .collect::<Vec<_>>();
    let mut operators: Vec<bool> = Vec::new();

    for (position, (_, token)) in significant.iter().enumerate() {
        match token {
            Token::Question => {
                let previous = position.checked_sub(1).and_then(|idx| significant.get(idx));
                let next = significant.get(position + 1);
                let starts_distinct_on_projection =
                    previous.is_some_and(|(previous_idx, token)| {
                        matches!(token, Token::RParen)
                            && closes_distinct_on_clause(&tokens, *previous_idx)
                    });
                let previous_can_end_expression = previous.is_some_and(|(_, token)| match token {
                    // A substituted anonymous parameter is an expression atom;
                    // a preserved `?` is an operator waiting for its right side.
                    Token::Question => operators.last().is_some_and(|operator| !*operator),
                    token => token_can_end_expression(token),
                });
                operators.push(
                    !starts_distinct_on_projection
                        && previous_can_end_expression
                        && next.is_some_and(|(_, token)| token_can_start_expression(token)),
                );
            }
            Token::QuestionAnd
            | Token::QuestionPipe
            | Token::AtQuestion
            | Token::QuestionMarkDash
            | Token::QuestionMarkSharp
            | Token::QuestionMarkDashVerticalBar
            | Token::QuestionMarkDoubleVerticalBar => operators.push(true),
            _ => {}
        }
    }
    Some(operators)
}

pub(crate) fn closes_distinct_on_clause(tokens: &[Token], closing_idx: usize) -> bool {
    let mut depth = 0usize;
    for idx in (0..closing_idx).rev() {
        match &tokens[idx] {
            Token::Whitespace(_) => {}
            Token::RParen => depth += 1,
            Token::LParen if depth > 0 => depth -= 1,
            Token::LParen => {
                let mut previous = tokens[..idx]
                    .iter()
                    .rev()
                    .filter(|token| !matches!(token, Token::Whitespace(_)));
                return token_is_word(previous.next(), "on")
                    && token_is_word(previous.next(), "distinct");
            }
            _ => {}
        }
    }
    false
}

pub(crate) fn token_is_word(token: Option<&Token>, expected: &str) -> bool {
    matches!(token, Some(Token::Word(word)) if word.value.eq_ignore_ascii_case(expected))
}

pub(crate) fn token_can_end_expression(token: &Token) -> bool {
    match token {
        Token::Word(word) if word.quote_style.is_some() => true,
        Token::Word(word) => !word_requires_following_expression(&word.value),
        _ => {
            token_is_expression_atom(token)
                || matches!(token, Token::RParen | Token::RBracket | Token::RBrace)
        }
    }
}

pub(crate) fn token_can_start_expression(token: &Token) -> bool {
    matches!(token, Token::Word(_))
        || token_is_expression_atom(token)
        || matches!(
            token,
            Token::LParen
                | Token::LBracket
                | Token::Plus
                | Token::Minus
                | Token::Tilde
                | Token::AtSign
                | Token::Question
        )
}

pub(crate) fn token_is_expression_atom(token: &Token) -> bool {
    matches!(
        token,
        Token::Number(_, _)
            | Token::SingleQuotedString(_)
            | Token::DoubleQuotedString(_)
            | Token::TripleSingleQuotedString(_)
            | Token::TripleDoubleQuotedString(_)
            | Token::DollarQuotedString(_)
            | Token::SingleQuotedByteStringLiteral(_)
            | Token::DoubleQuotedByteStringLiteral(_)
            | Token::TripleSingleQuotedByteStringLiteral(_)
            | Token::TripleDoubleQuotedByteStringLiteral(_)
            | Token::SingleQuotedRawStringLiteral(_)
            | Token::DoubleQuotedRawStringLiteral(_)
            | Token::TripleSingleQuotedRawStringLiteral(_)
            | Token::TripleDoubleQuotedRawStringLiteral(_)
            | Token::NationalStringLiteral(_)
            | Token::QuoteDelimitedStringLiteral(_)
            | Token::NationalQuoteDelimitedStringLiteral(_)
            | Token::EscapedStringLiteral(_)
            | Token::UnicodeStringLiteral(_)
            | Token::HexStringLiteral(_)
            | Token::Placeholder(_)
    )
}

pub(crate) fn word_requires_following_expression(word: &str) -> bool {
    matches!(
        word.to_ascii_lowercase().as_str(),
        "all"
            | "and"
            | "as"
            | "between"
            | "by"
            | "case"
            | "distinct"
            | "distinctrow"
            | "else"
            | "first"
            | "having"
            | "ilike"
            | "in"
            | "is"
            | "like"
            | "limit"
            | "not"
            | "next"
            | "offset"
            | "on"
            | "or"
            | "returning"
            | "select"
            | "set"
            | "then"
            | "values"
            | "when"
            | "where"
    )
}

pub(crate) fn question_mark_is_json_operator_fallback(sql: &str, offset: usize) -> bool {
    let bytes = sql.as_bytes();
    if matches!(bytes.get(offset + 1), Some(b'|' | b'&')) {
        return true;
    }

    let Some(previous) = bytes[..offset]
        .iter()
        .rposition(|byte| !byte.is_ascii_whitespace())
    else {
        return false;
    };
    let Some(next) = bytes[offset + 1..]
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .map(|relative| offset + 1 + relative)
    else {
        return false;
    };
    if matches!(bytes[next], b',' | b')' | b']' | b';') {
        return false;
    }

    match bytes[previous] {
        b'\'' | b'"' | b')' | b']' | b'0'..=b'9' => true,
        byte if byte.is_ascii_alphabetic() || matches!(byte, b'_' | b'$') => {
            let mut start = previous;
            while start > 0
                && (bytes[start - 1].is_ascii_alphanumeric()
                    || matches!(bytes[start - 1], b'_' | b'$'))
            {
                start -= 1;
            }
            let word = sql[start..=previous].to_ascii_lowercase();
            !matches!(
                word.as_str(),
                "select"
                    | "all"
                    | "case"
                    | "distinct"
                    | "distinctrow"
                    | "where"
                    | "and"
                    | "or"
                    | "when"
                    | "then"
                    | "else"
                    | "on"
                    | "by"
                    | "set"
                    | "values"
                    | "returning"
                    | "as"
                    | "in"
                    | "is"
                    | "not"
                    | "like"
                    | "ilike"
                    | "limit"
                    | "offset"
                    | "having"
            )
        }
        _ => false,
    }
}

pub(crate) fn single_quoted_sql_end(sql: &str, mut idx: usize) -> usize {
    let bytes = sql.as_bytes();
    idx += 1;
    while idx < bytes.len() {
        if bytes[idx] == b'\'' {
            if bytes.get(idx + 1) == Some(&b'\'') {
                idx += 2;
            } else {
                return idx + 1;
            }
        } else {
            idx += 1;
        }
    }
    bytes.len()
}

pub(crate) fn double_quoted_sql_end(sql: &str, mut idx: usize) -> usize {
    let bytes = sql.as_bytes();
    idx += 1;
    while idx < bytes.len() {
        if bytes[idx] == b'"' {
            if bytes.get(idx + 1) == Some(&b'"') {
                idx += 2;
            } else {
                return idx + 1;
            }
        } else {
            idx += 1;
        }
    }
    bytes.len()
}

pub(crate) fn line_comment_end(sql: &str, idx: usize) -> usize {
    let bytes = sql.as_bytes();
    bytes[idx..]
        .iter()
        .position(|byte| *byte == b'\n')
        .map(|offset| idx + offset + 1)
        .unwrap_or(bytes.len())
}

pub(crate) fn block_comment_end(sql: &str, mut idx: usize) -> usize {
    let bytes = sql.as_bytes();
    let mut depth = 1_usize;
    idx += 2;
    while idx + 1 < bytes.len() {
        match (bytes[idx], bytes[idx + 1]) {
            (b'/', b'*') => {
                depth += 1;
                idx += 2;
            }
            (b'*', b'/') => {
                depth -= 1;
                idx += 2;
                if depth == 0 {
                    return idx;
                }
            }
            _ => idx += 1,
        }
    }
    bytes.len()
}

pub(crate) fn dollar_quoted_sql_end(sql: &str, idx: usize) -> usize {
    let Some(delimiter_len) = dollar_quote_delimiter_len(sql, idx) else {
        return idx + 1;
    };
    let delimiter = &sql[idx..idx + delimiter_len];
    sql[idx + delimiter_len..]
        .find(delimiter)
        .map(|offset| idx + delimiter_len + offset + delimiter_len)
        .unwrap_or(sql.len())
}

pub(crate) fn dollar_quote_delimiter_len(sql: &str, idx: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    if bytes.get(idx) != Some(&b'$') {
        return None;
    }
    let mut end = idx + 1;
    match bytes.get(end).copied() {
        Some(b'$') => return Some(2),
        Some(byte) if byte.is_ascii_alphabetic() || byte == b'_' => end += 1,
        _ => return None,
    }
    while let Some(byte) = bytes.get(end).copied() {
        if byte == b'$' {
            return Some(end - idx + 1);
        }
        if byte.is_ascii_alphanumeric() || byte == b'_' {
            end += 1;
        } else {
            return None;
        }
    }
    None
}

pub(crate) fn read_cstr(payload: &[u8], idx: &mut usize) -> Result<String> {
    let start = *idx;
    let end = payload[start..]
        .iter()
        .position(|byte| *byte == 0)
        .map(|offset| start + offset)
        .ok_or_else(|| PgWireError::Protocol("missing null-terminated string".to_string()))?;
    *idx = end + 1;
    std::str::from_utf8(&payload[start..end])
        .map(str::to_string)
        .map_err(|error| PgWireError::Protocol(error.to_string()))
}

pub(crate) fn read_i16(payload: &[u8], idx: &mut usize) -> Result<i16> {
    if *idx + 2 > payload.len() {
        return Err(PgWireError::Protocol(
            "message truncated reading i16".to_string(),
        ));
    }
    let value = i16::from_be_bytes(payload[*idx..*idx + 2].try_into().unwrap());
    *idx += 2;
    Ok(value)
}

pub(crate) fn read_nonnegative_i16_count(
    payload: &[u8],
    idx: &mut usize,
    kind: &str,
) -> Result<usize> {
    let count = read_i16(payload, idx)?;
    usize::try_from(count)
        .map_err(|_| PgWireError::Protocol(format!("{kind} count must not be negative")))
}

pub(crate) fn read_i32(payload: &[u8], idx: &mut usize) -> Result<i32> {
    if *idx + 4 > payload.len() {
        return Err(PgWireError::Protocol(
            "message truncated reading i32".to_string(),
        ));
    }
    let value = i32::from_be_bytes(payload[*idx..*idx + 4].try_into().unwrap());
    *idx += 4;
    Ok(value)
}

#[derive(Clone, Debug)]
pub(crate) struct StartupState {
    pub(crate) user: String,
    pub(crate) database: String,
    pub(crate) session_gucs: HashMap<String, String>,
    pub(crate) security_context: Option<SecurityContext>,
}

pub(crate) fn startup(
    stream: &mut ClientStream,
    server: &PgWireServer,
    process_id: i32,
    secret_key: i32,
) -> Result<Option<StartupState>> {
    loop {
        let payload = match read_startup_payload(stream, server.config.max_request_bytes) {
            Ok(payload) => payload,
            Err(error @ PgWireError::Protocol(_)) => {
                let _ = startup_protocol_error_response(stream, &error.to_string());
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        if payload.len() < 4 {
            let error = PgWireError::Protocol("startup message missing protocol code".to_string());
            let _ = startup_protocol_error_response(stream, &error.to_string());
            return Err(error);
        }

        let code = i32::from_be_bytes(payload[0..4].try_into().unwrap());
        match code {
            SSL_REQUEST => {
                if let Some(config) = server.tls_config.clone() {
                    stream.write_all(b"S")?;
                    stream.flush()?;
                    stream.upgrade_tls(config.server.clone())?;
                } else {
                    stream.write_all(b"N")?;
                    stream.flush()?;
                }
            }
            GSSENC_REQUEST => {
                stream.write_all(b"N")?;
                stream.flush()?;
            }
            CANCEL_REQUEST => {
                if payload.len() >= 12 {
                    let cancel_process_id = i32::from_be_bytes(payload[4..8].try_into().unwrap());
                    let cancel_secret_key = i32::from_be_bytes(payload[8..12].try_into().unwrap());
                    server.request_cancel(cancel_process_id, cancel_secret_key);
                }
                return Ok(None);
            }
            requested_protocol if protocol_major(requested_protocol) == PROTOCOL_MAJOR_V3 => {
                if server.config.require_tls && !stream.is_tls() {
                    let message = "TLS is required for this BicDB server; reconnect with PostgreSQL SSL enabled";
                    let _ = error_response_with_fields(stream, "FATAL", "28000", message, &[]);
                    return Err(PgWireError::Server(message.to_string()));
                }
                let params = match startup_params(&payload[4..]) {
                    Ok(params) => params,
                    Err(error @ PgWireError::Protocol(_)) => {
                        let _ = startup_protocol_error_response(stream, &error.to_string());
                        return Err(error);
                    }
                    Err(error) => return Err(error),
                };
                let negotiated_protocol = supported_protocol_version(requested_protocol);
                let unsupported_protocol_options = params
                    .keys()
                    .filter(|key| key.starts_with("_pq_."))
                    .cloned()
                    .collect::<Vec<_>>();
                if negotiated_protocol != requested_protocol
                    || !unsupported_protocol_options.is_empty()
                {
                    negotiate_protocol_version(
                        stream,
                        negotiated_protocol,
                        &unsupported_protocol_options,
                    )?;
                }
                let user = params
                    .get("user")
                    .cloned()
                    .unwrap_or_else(|| "bicdb".to_string());
                let database =
                    if let Some(cluster) = server.cluster.as_ref().and_then(Weak::upgrade) {
                        params
                            .get("database")
                            .filter(|value| !value.trim().is_empty())
                            .cloned()
                            .unwrap_or_else(|| cluster.default_database())
                    } else {
                        "bicdb".to_string()
                    };
                let session_gucs = startup_session_gucs(&params);
                let authentication_snapshot = if server.config.require_auth {
                    load_user_catalog(&server.auth_path)?
                        .users
                        .get(&user)
                        .map(user_record_digest)
                        .transpose()?
                } else {
                    None
                };
                if server.config.require_auth {
                    match server.config.auth_method {
                        AuthMethod::Cleartext => {
                            if !stream.is_tls() {
                                let error = PgWireError::Authentication;
                                error_response_for_error(stream, &error)?;
                                return Err(PgWireError::Server(
                                    "cleartext password authentication requires TLS".to_string(),
                                ));
                            }
                            authentication_cleartext_password(stream)?;
                            let password =
                                read_password_message(stream, server.config.max_request_bytes)?;
                            if !verify_user_password(&server.auth_path, &user, &password)? {
                                let error = PgWireError::Authentication;
                                error_response_for_error(stream, &error)?;
                                return Err(error);
                            }
                        }
                        AuthMethod::ScramSha256 => {
                            if let Err(error) = authenticate_scram_sha256(stream, server, &user) {
                                let auth_error = PgWireError::Authentication;
                                error_response_for_error(stream, &auth_error)?;
                                return Err(error);
                            }
                        }
                    }
                }
                if let Some(cluster) = server.cluster.as_ref().and_then(Weak::upgrade) {
                    if let Err(error) = cluster.server_for_database(&database) {
                        error_response_for_error(stream, &error)?;
                        return Err(error);
                    }
                }
                // A credential/identity change during authentication must not
                // combine an old proof with a newly assigned principal, including
                // revocation followed by reuse of the same login name.
                let security_context =
                    if server.config.require_auth && authentication_snapshot.is_none() {
                        Err(PgWireError::Authentication)
                    } else {
                        connection_security_context(
                            server,
                            &user,
                            process_id as u64,
                            authentication_snapshot.as_ref(),
                        )
                    };
                let security_context = match security_context {
                    Ok(context) => context,
                    Err(error) => {
                        error_response_for_error(stream, &error)?;
                        return Err(error);
                    }
                };
                authentication_ok(stream)?;
                parameter_status(
                    stream,
                    "server_version",
                    &server.config.postgres_server_version,
                )?;
                parameter_status(stream, "server_encoding", "UTF8")?;
                parameter_status(stream, "client_encoding", "UTF8")?;
                parameter_status(stream, "DateStyle", "ISO, MDY")?;
                parameter_status(stream, "integer_datetimes", "on")?;
                parameter_status(stream, "standard_conforming_strings", "on")?;
                backend_key_data(stream, process_id, secret_key)?;
                ready_for_query(stream)?;
                return Ok(Some(StartupState {
                    user,
                    database,
                    session_gucs,
                    security_context,
                }));
            }
            other => {
                let error =
                    PgWireError::Protocol(format!("unsupported protocol request code {other}"));
                let _ = startup_protocol_error_response(stream, &error.to_string());
                return Err(error);
            }
        }
    }
}

pub(crate) fn startup_session_gucs(params: &HashMap<String, String>) -> HashMap<String, String> {
    let mut gucs = HashMap::new();
    let Some(options) = params.get("options") else {
        return gucs;
    };

    let mut parts = options.split_whitespace().peekable();
    while let Some(part) = parts.next() {
        if let Some(value) = part.strip_prefix("-csearch_path=") {
            gucs.insert("search_path".to_string(), value.replace("%2C", ","));
            continue;
        }
        if part == "-c" {
            if let Some(value) = parts
                .next()
                .and_then(|part| part.strip_prefix("search_path="))
            {
                gucs.insert("search_path".to_string(), value.replace("%2C", ","));
            }
        }
    }
    gucs
}

pub(crate) fn protocol_major(protocol_version: i32) -> i32 {
    (protocol_version >> 16) & 0xffff
}

pub(crate) fn protocol_minor(protocol_version: i32) -> i32 {
    protocol_version & 0xffff
}

pub(crate) fn supported_protocol_version(requested_protocol: i32) -> i32 {
    if protocol_minor(requested_protocol) >= protocol_minor(PROTOCOL_V3_2) {
        PROTOCOL_V3_2
    } else {
        PROTOCOL_V3
    }
}

pub(crate) fn read_startup_payload(
    stream: &mut ClientStream,
    max_request_bytes: usize,
) -> Result<Vec<u8>> {
    let mut len_bytes = [0_u8; 4];
    stream.read_exact(&mut len_bytes)?;
    let len = i32::from_be_bytes(len_bytes);
    if len < 4 {
        return Err(PgWireError::Protocol(format!(
            "invalid startup length {len}"
        )));
    }
    if (len as usize) > max_request_bytes {
        return Err(PgWireError::Protocol(format!(
            "startup message exceeds max_request_bytes {max_request_bytes}"
        )));
    }
    let mut payload = vec![0_u8; (len - 4) as usize];
    stream.read_exact(&mut payload)?;
    Ok(payload)
}

pub(crate) fn startup_params(payload: &[u8]) -> Result<HashMap<String, String>> {
    let mut idx = 0;
    let mut params = HashMap::new();
    while idx < payload.len() && payload[idx] != 0 {
        let key = read_cstr(payload, &mut idx)?;
        if idx >= payload.len() || payload[idx] == 0 {
            return Err(PgWireError::Protocol(
                "startup parameter missing value".to_string(),
            ));
        }
        let value = read_cstr(payload, &mut idx)?;
        params.insert(key, value);
    }
    if idx >= payload.len() {
        return Err(PgWireError::Protocol(
            "startup message missing terminator".to_string(),
        ));
    }
    Ok(params)
}

pub(crate) enum FrontendMessageRead {
    Message(u8, Vec<u8>),
    Eof,
    Timeout,
}

/// How long a message that has started arriving may stall between segments
/// before the peer is treated as dead. The connection's socket read timeout
/// is only the 250 ms idle poll used to interleave NOTIFY delivery; without
/// this distinction a large multi-row INSERT from a slow or CPU-starved
/// client hit that poll timeout mid-payload and the connection was dropped
/// with "Resource temporarily unavailable" (seen: HammerDB loaders under a
/// concurrent compile on the same host).
pub(crate) const PARTIAL_MESSAGE_BUDGET: Duration = Duration::from_secs(30);

/// `read_exact` that treats the poll timeout as "not yet" while a message is
/// in flight, bounded by PARTIAL_MESSAGE_BUDGET, and still honors an expired
/// startup/authentication deadline immediately.
fn read_exact_in_flight(stream: &mut ClientStream, buf: &mut [u8]) -> io::Result<()> {
    let started = Instant::now();
    let mut filled = 0;
    while filled < buf.len() {
        match stream.read(&mut buf[filled..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed mid-message",
                ))
            }
            Ok(n) => filled += n,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                if stream.read_deadline_expired() || started.elapsed() >= PARTIAL_MESSAGE_BUDGET {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "timed out reading partial frontend message",
                    ));
                }
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

pub(crate) fn read_frontend_message(
    stream: &mut ClientStream,
    max_request_bytes: usize,
) -> Result<FrontendMessageRead> {
    let mut tag = [0_u8; 1];
    match stream.read_exact(&mut tag) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
            return Ok(FrontendMessageRead::Eof);
        }
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
            ) =>
        {
            return Ok(FrontendMessageRead::Timeout);
        }
        Err(error) => return Err(error.into()),
    }

    let mut len_bytes = [0_u8; 4];
    read_exact_in_flight(stream, &mut len_bytes)?;
    let len = i32::from_be_bytes(len_bytes);
    if len < 4 {
        return Err(PgWireError::Protocol(format!(
            "invalid frontend message length {len}"
        )));
    }
    if (len as usize) > max_request_bytes {
        return Err(PgWireError::Protocol(format!(
            "frontend message exceeds max_request_bytes {max_request_bytes}"
        )));
    }
    let mut payload = vec![0_u8; (len - 4) as usize];
    read_exact_in_flight(stream, &mut payload)?;
    Ok(FrontendMessageRead::Message(tag[0], payload))
}

pub(crate) fn write_query_result(
    stream: &mut ClientStream,
    server: &PgWireServer,
    result: &SqlResult,
    result_formats: &[i16],
    source_sql: Option<&str>,
    include_row_description: bool,
    include_command_complete: bool,
) -> Result<StreamWriteStats> {
    let mut stats = StreamWriteStats::default();
    if !result.columns.is_empty() {
        let db = server.read_db()?;
        validate_format_arity(result_formats, result.columns.len(), "result")?;
        let column_types = result_column_types_with_db(&db, result, source_sql)?;
        // Element-oid resolution is constant per column but consults the type
        // and schema catalogs on every non-builtin miss; memoize it across the
        // row loop so a large result resolves each column once, not per cell.
        let mut element_oid_memo = vec![None; column_types.len()];
        let row_payloads = result
            .rows
            .iter()
            .map(|row| {
                data_row_payload_with_memo(
                    &db,
                    row,
                    &column_types,
                    result_formats,
                    &mut element_oid_memo,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        if include_row_description {
            row_description_with_db(stream, &db, result, &column_types, result_formats)?;
        }
        for payload in row_payloads {
            stats.rows = stats.rows.saturating_add(1);
            stats.bytes = stats.bytes.saturating_add(payload.len() as u64);
            write_message(stream, b'D', &payload)?;
        }
    }
    if include_command_complete {
        command_complete(stream, &result.command_complete_tag())?;
    }
    Ok(stats)
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct StreamWriteStats {
    pub(crate) rows: u64,
    pub(crate) bytes: u64,
}

pub(crate) fn describe_result(
    stream: &mut ClientStream,
    server: &PgWireServer,
    result: &SqlResult,
    result_formats: &[i16],
    source_sql: Option<&str>,
) -> Result<()> {
    if result.columns.is_empty() {
        no_data(stream)
    } else {
        let db = server.read_db()?;
        validate_format_arity(result_formats, result.columns.len(), "result")?;
        let column_types = result_column_types_with_db(&db, result, source_sql)?;
        row_description_with_db(stream, &db, result, &column_types, result_formats)
    }
}

pub(crate) fn row_description_with_db(
    stream: &mut ClientStream,
    db: &BicDb,
    result: &SqlResult,
    column_types: &[i32],
    result_formats: &[i16],
) -> Result<()> {
    let payload = row_description_payload_inner(Some(db), result, column_types, result_formats)?;
    write_message(stream, b'T', &payload)
}

#[cfg(test)]
pub(crate) fn row_description_payload(
    result: &SqlResult,
    column_types: &[i32],
    result_formats: &[i16],
) -> Result<Vec<u8>> {
    row_description_payload_inner(None, result, column_types, result_formats)
}

pub(crate) fn row_description_payload_inner(
    db: Option<&BicDb>,
    result: &SqlResult,
    column_types: &[i32],
    result_formats: &[i16],
) -> Result<Vec<u8>> {
    let mut payload = Vec::new();
    put_i16(&mut payload, result.columns.len() as i16);
    for (idx, column) in result.columns.iter().enumerate() {
        cstr(&mut payload, column);
        let metadata = result.column_metadata.get(idx).cloned().unwrap_or_default();
        put_i32(&mut payload, metadata.table_oid);
        put_i16(&mut payload, metadata.attribute_number);
        let oid = column_types
            .get(idx)
            .copied()
            .unwrap_or_else(|| type_oid(result, idx));
        put_i32(&mut payload, oid);
        let type_size = match db {
            Some(db) => type_size_for_oid_with_db(db, result, idx, oid)?,
            None => type_size_for_oid(result, idx, oid),
        };
        put_i16(&mut payload, type_size);
        put_i32(&mut payload, metadata.type_modifier);
        put_i16(&mut payload, format_for_column(result_formats, idx)?);
    }
    Ok(payload)
}

// `element_oid_memo` carries one slot per column: `None` until that column's
// `array_element_oid_with_db` resolution runs, then `Some(resolution)` so
// later rows of the same result reuse it instead of re-scanning the type and
// schema catalogs. Callers streaming a whole result pass one memo across all
// rows; the memo is only valid for a fixed `column_types`.
pub(crate) fn data_row_payload_with_memo(
    db: &BicDb,
    row: &[SqlValue],
    column_types: &[i32],
    result_formats: &[i16],
    element_oid_memo: &mut [Option<Option<i32>>],
) -> Result<Vec<u8>> {
    let mut payload = Vec::new();
    put_i16(&mut payload, row.len() as i16);
    for (idx, value) in row.iter().enumerate() {
        if matches!(value, SqlValue::Null) {
            put_i32(&mut payload, -1);
            continue;
        }
        let oid = column_types.get(idx).copied().unwrap_or(25);
        let format = format_for_column(result_formats, idx)?;
        let mut unmemoized = None;
        let memo_slot = element_oid_memo.get_mut(idx).unwrap_or(&mut unmemoized);
        let rendered = encode_result_value_with_memo(db, value, oid, format, memo_slot)?;
        put_i32(&mut payload, rendered.len() as i32);
        payload.extend_from_slice(&rendered);
    }
    Ok(payload)
}

pub(crate) fn memoized_element_oid(
    db: &BicDb,
    oid: i32,
    memo_slot: &mut Option<Option<i32>>,
) -> Result<Option<i32>> {
    match memo_slot {
        Some(cached) => Ok(*cached),
        None => {
            let resolved = array_element_oid_with_db(db, oid)?;
            *memo_slot = Some(resolved);
            Ok(resolved)
        }
    }
}

/// Handles LISTEN / UNLISTEN / NOTIFY when the query is a single such
/// statement; returns the CommandComplete tag. Channel names follow the
/// stream-name charset (letters, digits, '_', '-', '.', ':'), optionally
/// double-quoted; NOTIFY takes an optional single-quoted payload.
pub(crate) fn try_listen_notify_statement(
    server: &PgWireServer,
    state: &ConnectionState,
    query: &str,
) -> Result<Option<&'static str>> {
    // Only LISTEN / UNLISTEN / NOTIFY statements are handled here; decide
    // that from the first word before splitting and upper-casing the text.
    let first_word_len = query
        .trim_start()
        .bytes()
        .take_while(|byte| byte.is_ascii_alphabetic())
        .count();
    let first_word = &query.trim_start()[..first_word_len];
    if !(first_word.eq_ignore_ascii_case("listen")
        || first_word.eq_ignore_ascii_case("unlisten")
        || first_word.eq_ignore_ascii_case("notify"))
    {
        return Ok(None);
    }
    let statements = split_sql_statements(query);
    let [statement] = statements.as_slice() else {
        return Ok(None);
    };
    let trimmed = statement.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    let parse_channel = |raw: &str| -> Result<String> {
        let raw = raw.trim();
        let name = raw
            .strip_prefix('"')
            .and_then(|rest| rest.strip_suffix('"'))
            .unwrap_or(raw);
        if name.is_empty()
            || !name.bytes().all(|byte| {
                byte.is_ascii_alphanumeric()
                    || byte == b'_'
                    || byte == b'-'
                    || byte == b'.'
                    || byte == b':'
            })
        {
            return Err(PgWireError::Protocol(format!(
                "invalid notification channel name `{raw}`"
            )));
        }
        Ok(name.to_string())
    };
    if let Some(rest) = upper
        .strip_prefix("LISTEN ")
        .map(|_| trimmed["LISTEN ".len()..].trim())
    {
        let channel = parse_channel(rest)?;
        if let Ok(mut bus) = server.notifications.lock() {
            bus.listen(state.connection_id, &channel);
        }
        return Ok(Some("LISTEN"));
    }
    if upper == "UNLISTEN *" {
        if let Ok(mut bus) = server.notifications.lock() {
            bus.unlisten(state.connection_id, None);
        }
        return Ok(Some("UNLISTEN"));
    }
    if let Some(rest) = upper
        .strip_prefix("UNLISTEN ")
        .map(|_| trimmed["UNLISTEN ".len()..].trim())
    {
        let channel = parse_channel(rest)?;
        if let Ok(mut bus) = server.notifications.lock() {
            bus.unlisten(state.connection_id, Some(&channel));
        }
        return Ok(Some("UNLISTEN"));
    }
    if upper.starts_with("NOTIFY ") {
        let rest = trimmed["NOTIFY ".len()..].trim();
        let (channel_raw, payload) = match rest.split_once(',') {
            Some((channel, payload)) => {
                let payload = payload.trim();
                let payload = payload
                    .strip_prefix('\'')
                    .and_then(|inner| inner.strip_suffix('\''))
                    .ok_or_else(|| {
                        PgWireError::Protocol(
                            "NOTIFY payload must be a single-quoted string".to_string(),
                        )
                    })?;
                (channel, payload.replace("''", "'"))
            }
            None => (rest, String::new()),
        };
        let channel = parse_channel(channel_raw)?;
        if let Ok(mut bus) = server.notifications.lock() {
            bus.notify(&channel, &payload, state.connection_id as i32);
        }
        return Ok(Some("NOTIFY"));
    }
    Ok(None)
}

/// Writes any queued NotificationResponse ('A') messages for the connection.
pub(crate) fn flush_notifications(
    stream: &mut ClientStream,
    server: &PgWireServer,
    connection_id: u64,
) -> Result<()> {
    let pending = match server.notifications.lock() {
        Ok(mut bus) => bus.drain(connection_id),
        Err(_) => return Ok(()),
    };
    if pending.is_empty() {
        return Ok(());
    }
    for (channel, payload, sender_pid) in pending {
        let mut body = Vec::with_capacity(8 + channel.len() + payload.len() + 2);
        body.extend_from_slice(&sender_pid.to_be_bytes());
        body.extend_from_slice(channel.as_bytes());
        body.push(0);
        body.extend_from_slice(payload.as_bytes());
        body.push(0);
        write_message(stream, b'A', &body)?;
    }
    stream.flush()?;
    Ok(())
}
