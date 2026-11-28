//! PostgreSQL scalar support functions for built-in hash operator classes.
use crate::*;

struct HashInput {
    pg_type: &'static str,
    oid: i64,
    base: (i64, &'static str),
    extended: (i64, &'static str),
}

const INPUTS: &[HashInput] = &[
    HashInput {
        pg_type: "int2",
        oid: 21,
        base: (449, "hashint2"),
        extended: (441, "hashint2extended"),
    },
    HashInput {
        pg_type: "int4",
        oid: 23,
        base: (450, "hashint4"),
        extended: (425, "hashint4extended"),
    },
    HashInput {
        pg_type: "int8",
        oid: 20,
        base: (949, "hashint8"),
        extended: (442, "hashint8extended"),
    },
    HashInput {
        pg_type: "float4",
        oid: 700,
        base: (451, "hashfloat4"),
        extended: (443, "hashfloat4extended"),
    },
    HashInput {
        pg_type: "float8",
        oid: 701,
        base: (452, "hashfloat8"),
        extended: (444, "hashfloat8extended"),
    },
    HashInput {
        pg_type: "oid",
        oid: 26,
        base: (453, "hashoid"),
        extended: (445, "hashoidextended"),
    },
    HashInput {
        pg_type: "char",
        oid: 18,
        base: (454, "hashchar"),
        extended: (446, "hashcharextended"),
    },
    HashInput {
        pg_type: "name",
        oid: 19,
        base: (455, "hashname"),
        extended: (447, "hashnameextended"),
    },
    HashInput {
        pg_type: "oidvector",
        oid: 30,
        base: (457, "hashoidvector"),
        extended: (776, "hashoidvectorextended"),
    },
];

fn scalar_hash_input(name: &str) -> Option<(&'static HashInput, bool)> {
    let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    INPUTS.iter().find_map(|input| {
        if input.base.1 == name {
            Some((input, false))
        } else if input.extended.1 == name {
            Some((input, true))
        } else {
            None
        }
    })
}

pub(crate) fn scalar_hash_result_type(name: &str, arity: usize) -> Option<&'static str> {
    let (_, extended) = scalar_hash_input(name)?;
    (arity == 1 + usize::from(extended)).then_some(if extended { "int8" } else { "int4" })
}

pub(crate) fn scalar_hash_procedures() -> impl Iterator<Item = (i64, &'static str, i64, i64, bool)>
{
    INPUTS.iter().flat_map(|input| {
        [
            (input.base.0, input.base.1, input.oid, 23, false),
            (input.extended.0, input.extended.1, input.oid, 20, true),
        ]
    })
}

pub(crate) fn eval_scalar_hash(
    name: &str,
    args: &[SqlValue],
    arg_types: Option<&[Option<String>]>,
) -> Result<Option<SqlValue>> {
    let Some((input, extended)) = scalar_hash_input(name) else {
        return Ok(None);
    };
    let expected = [input.pg_type, "int8"];
    let compatible = args.len() == 1 + usize::from(extended)
        && arg_types.is_none_or(|types| {
            types.iter().zip(expected).all(|(actual, expected)| {
                actual.as_deref().is_none_or(|actual| {
                    actual == "unknown" || pg_cast_allows(actual, expected, PgCastContext::Implicit)
                })
            })
        });
    if !compatible {
        let parameters = (0..args.len())
            .map(|index| {
                arg_types
                    .and_then(|types| types.get(index))
                    .and_then(Option::as_deref)
                    .map(|pg_type| {
                        pg_type_spec(pg_type)
                            .map_or_else(|| pg_cast_display_type(pg_type), |spec| spec.display_name)
                    })
                    .unwrap_or("unknown")
            })
            .collect::<Vec<_>>()
            .join(", ");
        return Err(SqlError::coded(
            "42883",
            format!("function {name}({parameters}) does not exist"),
            None,
            Some("No function matches the given name and argument types. You might need to add explicit type casts.".to_string()),
        ));
    }
    if args.iter().any(|arg| matches!(arg, SqlValue::Null)) {
        return Ok(Some(SqlValue::Null));
    }
    let value = cast_value_to_pg_type(args[0].clone(), input.pg_type)?;
    let seed = if extended {
        let SqlValue::Int(seed) = cast_value_to_pg_type(args[1].clone(), "int8")? else {
            unreachable!("cast to int8 returns an integer");
        };
        seed as u64
    } else {
        0
    };
    let hash = scalar_hash(value, input.pg_type, seed)?;
    let result = if extended {
        hash as i64
    } else {
        i64::from(hash as i32)
    };
    Ok(Some(SqlValue::Int(result)))
}

fn scalar_hash(value: SqlValue, pg_type: &str, seed: u64) -> Result<u64> {
    use super::{hash_bytes_extended, hash_int8_extended};

    Ok(match (pg_type, value) {
        ("int2" | "int4" | "int8", SqlValue::Int(value)) => hash_int8_extended(value, seed),
        ("oid", SqlValue::Int(value)) => hash_bytes_extended(&(value as u32).to_le_bytes(), seed),
        ("char", value) => {
            let byte = pg_internal_char_byte(&value).expect("cast to char returns a char");
            hash_int8_extended(i64::from(byte as i8), seed)
        }
        ("name", SqlValue::String(value)) => hash_bytes_extended(value.as_bytes(), seed),
        ("float4" | "float8", SqlValue::Float(value)) => {
            if value == 0.0 {
                return Ok(seed);
            }
            let value = if value.is_nan() { f64::NAN } else { value };
            hash_bytes_extended(&value.to_le_bytes(), seed)
        }
        ("oidvector", SqlValue::String(value)) => {
            let PgCanonicalValue::Array(vector) =
                crate::type_codec::canonical_catalog_vector("oidvector", &value)?
            else {
                unreachable!("a catalog vector is a canonical array");
            };
            let mut bytes = Vec::with_capacity(vector.elements.len() * size_of::<u32>());
            for element in vector.elements {
                let PgCanonicalValue::Oid(oid) = element else {
                    unreachable!("a canonical oidvector contains OIDs");
                };
                bytes.extend_from_slice(&oid.to_le_bytes());
            }
            hash_bytes_extended(&bytes, seed)
        }
        _ => unreachable!("the scalar hash input is explicitly cast to its declared type"),
    })
}
