//! PostgreSQL's seeded per-type hash and ordered hash_combine64 partition key.
//! The byte mixer is shared with the existing PostgreSQL hashtextextended
//! implementation and its license/attribution in pg_hash.rs.
//!
//! Reference: PostgreSQL REL_16_STABLE catalog/partition.h, partbounds.c,
//! access/hash/hashfunc.c and common/hashfn.h. Canonical SQL equality keys
//! and Rust's DefaultHasher are not PostgreSQL partition hashes.
use crate::*;

const SEED: u64 = 0x7a5b22367996dcfd;

fn scalar_hash(column: &ColumnSchema, value: &SqlValue) -> Result<u64> {
    // Domains, enums, composites and arrays require their actual PostgreSQL
    // operator-class support functions; never hash their display strings.
    if column.user_type.is_some() {
        return Err(SqlError::Unsupported(format!(
            "hash partition support function for {} is not implemented",
            column.pg_type
        )));
    }
    let invalid = || {
        SqlError::InvalidTextRepresentation(format!(
            "invalid hash partition key for {}",
            column.pg_type
        ))
    };
    match column.pg_type.as_str() {
        "int2" | "int4" | "int8" => Ok(crate::pg_hash::hash_int8_extended(
            sql_value_i64(value).ok_or_else(invalid)?,
            SEED,
        )),
        "oid" => {
            let value = sql_value_i64(value).ok_or_else(invalid)? as u32;
            Ok(crate::pg_hash::hash_bytes_extended(
                &value.to_le_bytes(),
                SEED,
            ))
        }
        "bool" => match value {
            SqlValue::Bool(value) => Ok(crate::pg_hash::hash_bytes_extended(
                &u32::from(*value).to_le_bytes(),
                SEED,
            )),
            _ => Err(invalid()),
        },
        "text" | "varchar" | "bpchar" | "name" => {
            let SqlValue::String(text) = value else {
                return Err(invalid());
            };
            let text = if column.pg_type == "bpchar" {
                text.trim_end_matches(' ')
            } else {
                text
            };
            Ok(crate::pg_hash::hash_bytes_extended(text.as_bytes(), SEED))
        }
        "float4" | "float8" => {
            let mut value = value.as_f64().ok_or_else(invalid)?;
            if column.pg_type == "float4" {
                value = f64::from(value as f32);
            }
            if value == 0.0 {
                return Ok(SEED);
            }
            let bits = if value.is_nan() {
                0x7ff8000000000000u64
            } else {
                value.to_bits()
            };
            Ok(crate::pg_hash::hash_bytes_extended(
                &bits.to_le_bytes(),
                SEED,
            ))
        }
        _ => Err(SqlError::Unsupported(format!(
            "hash partition support function for {} is not implemented",
            column.pg_type
        ))),
    }
}

pub(super) fn row_hash(columns: &[&ColumnSchema], values: &[SqlValue]) -> Result<u64> {
    let mut hash = 0u64;
    for (column, value) in columns.iter().zip(values) {
        if matches!(value, SqlValue::Null) {
            continue;
        }
        let next = scalar_hash(column, value)?;
        hash ^= next
            .wrapping_add(0x49a0f4dd15e5a8e3)
            .wrapping_add(hash << 54)
            .wrapping_add(hash >> 7);
    }
    Ok(hash)
}
