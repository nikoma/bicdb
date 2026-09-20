use std::cmp::Ordering;

use serde_json::Value as JsonValue;

use crate::{
    canonical_array_from_sql, canonical_json_value_key, cast_value_to_pg_type, format_bytea_hex,
    format_pg_lsn, format_pg_multirange, load_user_type, parse_bytea_text,
    parse_pg_canonical_special, parse_postgres_uuid, parse_timestamptz,
    pg_array_element_spec_by_oid, pg_internal_char_byte, pg_internal_char_text,
    pg_type_spec_by_oid, sql_value_to_vector, BicDb, ColumnSchema, EnumLabelSchema, PgArray,
    PgArrayDimension, PgBitString, PgCanonicalValue, PgDate, PgFloat4, PgFloat8, PgInterval,
    PgIpAddress, PgNumeric, PgOidAlias, PgRange, PgRangeBound, PgTime, PgTimeTz, PgTimestamp,
    PgTsQuery, PgTsVector, PgTypeSpec, Result, SqlError, SqlValue, UserTypeKind,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PgCodecContext {
    pub oid: i32,
    pub typmod: i32,
}

impl PgCodecContext {
    pub const fn new(oid: i32, typmod: i32) -> Self {
        Self { oid, typmod }
    }
}

pub trait PgScalarCodec: Send + Sync {
    fn parse_text(&self, input: &str, context: PgCodecContext) -> Result<SqlValue>;
    fn canonical_text(&self, value: &SqlValue, context: PgCodecContext) -> Result<String>;
    fn decode_binary(&self, input: &[u8], context: PgCodecContext) -> Result<SqlValue>;
    fn encode_binary(&self, value: &SqlValue, context: PgCodecContext) -> Result<Vec<u8>>;
    fn cast(
        &self,
        value: &SqlValue,
        source: PgCodecContext,
        target: PgCodecContext,
    ) -> Result<SqlValue>;
    fn compare(
        &self,
        left: &SqlValue,
        right: &SqlValue,
        context: PgCodecContext,
    ) -> Result<Ordering>;
    fn hash_key(&self, value: &SqlValue, context: PgCodecContext) -> Result<Vec<u8>>;
    fn index_key(&self, value: &SqlValue, context: PgCodecContext) -> Result<Vec<u8>>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PgCanonicalTextCodec;

impl PgScalarCodec for PgCanonicalTextCodec {
    fn parse_text(&self, input: &str, context: PgCodecContext) -> Result<SqlValue> {
        let codec_type = codec_type(context)?;
        reject_pseudo(codec_type.spec)?;
        let pg_type = codec_type.pg_type();
        let value = cast_value_to_pg_type(SqlValue::String(input.to_string()), &pg_type)?;
        canonical_value_for_type(codec_type, &value)?;
        Ok(value)
    }

    fn canonical_text(&self, value: &SqlValue, context: PgCodecContext) -> Result<String> {
        let codec_type = codec_type(context)?;
        let canonical = canonical_value_for_type(codec_type, value)?;
        canonical_text_for_value(codec_type.spec, value, &canonical)
    }

    fn decode_binary(&self, input: &[u8], context: PgCodecContext) -> Result<SqlValue> {
        let input = std::str::from_utf8(input).map_err(|_| {
            SqlError::invalid_text_representation(
                format!("oid {}", context.oid),
                "binary value is not valid UTF-8",
            )
        })?;
        self.parse_text(input, context)
    }

    fn encode_binary(&self, value: &SqlValue, context: PgCodecContext) -> Result<Vec<u8>> {
        Ok(self.canonical_text(value, context)?.into_bytes())
    }

    fn cast(
        &self,
        value: &SqlValue,
        _source: PgCodecContext,
        target: PgCodecContext,
    ) -> Result<SqlValue> {
        let codec_type = codec_type(target)?;
        reject_pseudo(codec_type.spec)?;
        cast_value_to_pg_type(value.clone(), &codec_type.pg_type())
    }

    fn compare(
        &self,
        left: &SqlValue,
        right: &SqlValue,
        context: PgCodecContext,
    ) -> Result<Ordering> {
        let codec_type = codec_type(context)?;
        let left = canonical_value_for_type(codec_type, left)?;
        let right = canonical_value_for_type(codec_type, right)?;
        Ok(canonical_index_key(codec_type.spec, &left)?
            .cmp(&canonical_index_key(codec_type.spec, &right)?))
    }

    fn hash_key(&self, value: &SqlValue, context: PgCodecContext) -> Result<Vec<u8>> {
        let codec_type = codec_type(context)?;
        let canonical = canonical_value_for_type(codec_type, value)?;
        let mut key = Vec::with_capacity(8);
        key.extend_from_slice(&context.oid.to_be_bytes());
        key.extend(canonical_equality_key(codec_type.spec, &canonical)?);
        Ok(key)
    }

    fn index_key(&self, value: &SqlValue, context: PgCodecContext) -> Result<Vec<u8>> {
        let codec_type = codec_type(context)?;
        canonical_index_key(
            codec_type.spec,
            &canonical_value_for_type(codec_type, value)?,
        )
    }
}

#[derive(Clone, Copy)]
struct CodecType {
    spec: &'static PgTypeSpec,
    array: bool,
}

impl CodecType {
    fn pg_type(self) -> String {
        if self.array {
            format!("{}[]", self.spec.name)
        } else {
            self.spec.name.to_string()
        }
    }
}

fn codec_type(context: PgCodecContext) -> Result<CodecType> {
    pg_type_spec_by_oid(context.oid)
        .map(|spec| CodecType { spec, array: false })
        .or_else(|| {
            pg_array_element_spec_by_oid(context.oid).map(|spec| CodecType { spec, array: true })
        })
        .ok_or_else(|| {
            SqlError::Unsupported(format!(
                "PostgreSQL type OID {} has no registered codec",
                context.oid
            ))
        })
}

fn reject_pseudo(spec: &PgTypeSpec) -> Result<()> {
    if spec.pseudo {
        return Err(SqlError::Unsupported(format!(
            "pseudo-type {} cannot encode a scalar value",
            spec.name
        )));
    }
    Ok(())
}

fn invalid_value(spec: &PgTypeSpec, value: &SqlValue) -> SqlError {
    SqlError::InvalidTextRepresentation(format!(
        "invalid input syntax for type {}: \"{}\"",
        spec.name,
        value.to_cell()
    ))
}

fn canonical_value_for_type(codec_type: CodecType, value: &SqlValue) -> Result<PgCanonicalValue> {
    if !codec_type.array {
        return canonical_value(codec_type.spec, value);
    }
    let pg_type = codec_type.pg_type();
    let value = match value {
        SqlValue::String(_) => cast_value_to_pg_type(value.clone(), &pg_type)?,
        _ => value.clone(),
    };
    let (_, array) = canonical_array_from_sql(value, &pg_type)?;
    Ok(PgCanonicalValue::Array(array))
}

pub(crate) fn canonical_value(spec: &PgTypeSpec, value: &SqlValue) -> Result<PgCanonicalValue> {
    reject_pseudo(spec)?;
    if matches!(value, SqlValue::Null) {
        return Ok(PgCanonicalValue::Null);
    }
    let text = value.to_cell();
    let invalid = || invalid_value(spec, value);
    Ok(match spec.name {
        "bool" => PgCanonicalValue::Bool(match value {
            SqlValue::Bool(value) => *value,
            _ => match cast_value_to_pg_type(value.clone(), "bool")? {
                SqlValue::Bool(value) => value,
                _ => return Err(invalid()),
            },
        }),
        "int2" => PgCanonicalValue::Int2(
            i16::try_from(canonical_i64(value, spec)?).map_err(|_| invalid())?,
        ),
        "int2vector" | "oidvector" => canonical_catalog_vector(spec.name, &text)?,
        "int4" => PgCanonicalValue::Int4(
            i32::try_from(canonical_i64(value, spec)?).map_err(|_| invalid())?,
        ),
        "int8" => PgCanonicalValue::Int8(canonical_i64(value, spec)?),
        "oid" => PgCanonicalValue::Oid(
            u32::try_from(canonical_i64(value, spec)?).map_err(|_| invalid())?,
        ),
        "xid" | "xid8" | "cid" | "tid" => parse_pg_canonical_special(spec.name, &text)
            .map_err(|_| invalid())?
            .ok_or_else(invalid)?,
        "float4" => {
            PgCanonicalValue::Float4(PgFloat4::from_value(canonical_f64(value, spec)? as f32))
        }
        "float8" => PgCanonicalValue::Float8(PgFloat8::from_value(canonical_f64(value, spec)?)),
        "numeric" => PgCanonicalValue::Numeric(
            PgNumeric::from_postgres_text(
                &cast_value_to_pg_type(value.clone(), "numeric")?.to_cell(),
            )
            .map_err(|_| invalid())?,
        ),
        "money" => PgCanonicalValue::Money(
            crate::pg_money_cents_from_text(
                &cast_value_to_pg_type(value.clone(), "money")?.to_cell(),
            )
            .map_err(|_| invalid())?,
        ),
        "bytea" => PgCanonicalValue::Bytes(parse_bytea_text(&text).map_err(|_| invalid())?),
        "bit" | "varbit" => {
            PgCanonicalValue::BitString(PgBitString::from_bit_text(&text).map_err(|_| invalid())?)
        }
        "date" => PgCanonicalValue::Date(PgDate::from_iso_text(&text).map_err(|_| invalid())?),
        "time" => PgCanonicalValue::Time(PgTime::from_postgres_text(&text).map_err(|_| invalid())?),
        "timetz" => {
            PgCanonicalValue::TimeTz(PgTimeTz::from_postgres_text(&text, 0).map_err(|_| invalid())?)
        }
        "timestamp" => PgCanonicalValue::Timestamp(
            PgTimestamp::from_postgres_text(&text, false).map_err(|_| invalid())?,
        ),
        "timestamptz" => {
            PgCanonicalValue::TimestampTz(parse_timestamptz(&text).map_err(|_| invalid())?)
        }
        "interval" => PgCanonicalValue::Interval(
            PgInterval::from_postgres_text(&text).map_err(|_| invalid())?,
        ),
        "uuid" => PgCanonicalValue::Uuid(parse_postgres_uuid(&text).map_err(|_| invalid())?),
        "json" => {
            serde_json::from_str::<serde_json::Value>(&text).map_err(|_| invalid())?;
            PgCanonicalValue::JsonText(text)
        }
        "jsonb" => PgCanonicalValue::Json(match value {
            SqlValue::JsonText(value) => value.parsed().clone(),
            SqlValue::Json(value) => value.clone(),
            _ => serde_json::from_str(&text).map_err(|_| invalid())?,
        }),
        "xml" => PgCanonicalValue::Xml(crate::normalize_xml(&text)?),
        "jsonpath" => {
            PgCanonicalValue::JsonPath(crate::normalize_jsonpath(&text).map_err(|_| invalid())?)
        }
        "vector" => PgCanonicalValue::Vector(
            sql_value_to_vector(value)?
                .into_iter()
                .map(PgFloat4::from_value)
                .collect(),
        ),
        "char" => PgCanonicalValue::Bytes(vec![pg_internal_char_byte(value)
            .or_else(|| match value {
                SqlValue::String(value) => Some(value.as_bytes().first().copied().unwrap_or(0)),
                _ => None,
            })
            .ok_or_else(invalid)?]),
        "bpchar" => PgCanonicalValue::Text(text.trim_end_matches(' ').to_string()),
        "tsvector" => PgCanonicalValue::TsVector(PgTsVector::from_postgres_text(&text)?),
        "tsquery" => PgCanonicalValue::TsQuery(match value {
            SqlValue::TsQuery(query) => query.clone(),
            _ => PgTsQuery::from_postgres_text(&text)?,
        }),
        "name" | "text" | "varchar" | "refcursor" | "pg_node_tree" | "pg_ndistinct"
        | "pg_dependencies" | "pg_mcv_list" => PgCanonicalValue::Text(text),
        "regproc" | "regprocedure" | "regoper" | "regoperator" | "regclass" | "regcollation"
        | "regtype" | "regrole" | "regnamespace" | "regconfig" | "regdictionary" => {
            let alias = if spec.name == "regtype" {
                crate::pg_type_regtype_name(&text)
                    .and_then(|name| crate::pg_type_oid_by_name(&name))
                    .and_then(|oid| u32::try_from(oid).ok())
                    .map(|oid| PgOidAlias {
                        oid: Some(oid),
                        symbolic_name: None,
                    })
            } else {
                None
            };
            PgCanonicalValue::OidAlias(
                alias.unwrap_or(PgOidAlias::from_postgres_text(&text).map_err(|_| invalid())?),
            )
        }
        "cidr" | "inet" | "macaddr" | "macaddr8" | "point" | "line" | "lseg" | "box" | "path"
        | "polygon" | "circle" | "int4range" | "numrange" | "tsrange" | "tstzrange"
        | "daterange" | "int8range" | "int4multirange" | "nummultirange" | "tsmultirange"
        | "tstzmultirange" | "datemultirange" | "int8multirange" | "pg_lsn" | "pg_snapshot"
        | "txid_snapshot" => parse_pg_canonical_special(spec.name, &text)
            .map_err(|_| invalid())?
            .ok_or_else(invalid)?,
        _ => {
            return Err(SqlError::Unsupported(format!(
                "registered PostgreSQL type {} has no canonical value encoder",
                spec.name
            )))
        }
    })
}

fn canonical_i64(value: &SqlValue, spec: &PgTypeSpec) -> Result<i64> {
    match cast_value_to_pg_type(value.clone(), spec.name)? {
        SqlValue::Int(value) => Ok(value),
        other => Err(invalid_value(spec, &other)),
    }
}

fn canonical_f64(value: &SqlValue, spec: &PgTypeSpec) -> Result<f64> {
    match cast_value_to_pg_type(value.clone(), spec.name)? {
        SqlValue::Float(value) => Ok(value),
        other => Err(invalid_value(spec, &other)),
    }
}

fn canonical_text_for_value(
    spec: &PgTypeSpec,
    original: &SqlValue,
    canonical: &PgCanonicalValue,
) -> Result<String> {
    Ok(match canonical {
        PgCanonicalValue::Null => String::new(),
        PgCanonicalValue::Bool(value) => value.to_string(),
        PgCanonicalValue::Int2(value) => value.to_string(),
        PgCanonicalValue::Int4(value) => value.to_string(),
        PgCanonicalValue::Int8(value) => value.to_string(),
        PgCanonicalValue::Oid(value) => value.to_string(),
        PgCanonicalValue::TransactionId32(value) => value.to_string(),
        PgCanonicalValue::TransactionId64(value) => value.to_string(),
        PgCanonicalValue::CommandId(value) => value.to_string(),
        PgCanonicalValue::TupleId(value) => value.to_postgres_text(),
        PgCanonicalValue::Float4(value) => value.to_value().to_string(),
        PgCanonicalValue::Float8(value) => value.to_value().to_string(),
        PgCanonicalValue::Numeric(value) => value.to_decimal_text(),
        PgCanonicalValue::Money(value) => crate::pg_money_text_from_cents(*value),
        PgCanonicalValue::Bytes(value) if spec.name == "char" && value.len() == 1 => {
            pg_internal_char_text(value[0])
        }
        PgCanonicalValue::Bytes(value) => format_bytea_hex(value),
        PgCanonicalValue::BitString(value) => value.to_bit_text(),
        PgCanonicalValue::JsonText(value) => value.clone(),
        PgCanonicalValue::Json(value) => canonical_json_value_key(value),
        PgCanonicalValue::Xml(value) | PgCanonicalValue::JsonPath(value) => value.clone(),
        PgCanonicalValue::TsVector(value) => value.to_postgres_text(),
        PgCanonicalValue::TsQuery(value) => value.to_postgres_text(),
        PgCanonicalValue::Text(value) => value.clone(),
        PgCanonicalValue::Network(value) => value.to_postgres_text(),
        PgCanonicalValue::MacAddress(value) => value.to_postgres_text(),
        PgCanonicalValue::Geometric(value) => value.to_postgres_text(),
        PgCanonicalValue::Range(value) => value.to_postgres_text(),
        PgCanonicalValue::Multirange(value) => format_pg_multirange(value),
        PgCanonicalValue::Lsn(value) => format_pg_lsn(*value),
        PgCanonicalValue::Snapshot(value) => value.to_postgres_text(),
        PgCanonicalValue::Array(value) if matches!(spec.name, "int2vector" | "oidvector") => value
            .elements
            .iter()
            .map(|element| match element {
                PgCanonicalValue::Int2(value) => Ok(value.to_string()),
                PgCanonicalValue::Oid(value) => Ok(value.to_string()),
                _ => Err(invalid_value(spec, original)),
            })
            .collect::<Result<Vec<_>>>()?
            .join(" "),
        _ => cast_value_to_pg_type(original.clone(), spec.name)?.to_cell(),
    })
}

pub(crate) fn canonical_catalog_vector(pg_type: &str, input: &str) -> Result<PgCanonicalValue> {
    let (element_type, elements) = match pg_type {
        "int2vector" => {
            let values = input
                .split_whitespace()
                .map(|value| {
                    let parsed = value.parse::<i64>().map_err(|_| {
                        SqlError::invalid_text_representation("smallint", format!("\"{value}\""))
                    })?;
                    i16::try_from(parsed)
                        .map(PgCanonicalValue::Int2)
                        .map_err(|_| {
                            SqlError::numeric_value_out_of_range(format!(
                                "value \"{value}\" is out of range for type smallint"
                            ))
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            ("int2", values)
        }
        "oidvector" => {
            let values = input
                .split_whitespace()
                .map(|value| {
                    let parsed = value.parse::<i64>().map_err(|_| {
                        SqlError::invalid_text_representation("oid", format!("\"{value}\""))
                    })?;
                    let wrapped = if parsed < 0 {
                        i128::from(parsed) + (1_i128 << 32)
                    } else {
                        i128::from(parsed)
                    };
                    u32::try_from(wrapped)
                        .map(PgCanonicalValue::Oid)
                        .map_err(|_| SqlError::numeric_value_out_of_range("oid out of range"))
                })
                .collect::<Result<Vec<_>>>()?;
            ("oid", values)
        }
        _ => {
            return Err(SqlError::Unsupported(format!(
                "{pg_type} is not a PostgreSQL catalog vector"
            )))
        }
    };
    let length = elements.len();
    PgArray::new(
        element_type,
        vec![PgArrayDimension {
            lower_bound: 0,
            length,
        }],
        elements,
    )
    .map(PgCanonicalValue::Array)
    .map_err(|error| SqlError::InvalidTextRepresentation(error.to_string()))
}

fn canonical_equality_key(spec: &PgTypeSpec, value: &PgCanonicalValue) -> Result<Vec<u8>> {
    canonical_index_key(spec, value)
}

pub(crate) fn canonical_index_key(spec: &PgTypeSpec, value: &PgCanonicalValue) -> Result<Vec<u8>> {
    if matches!(value, PgCanonicalValue::Null) {
        return Ok(vec![0]);
    }
    let mut key = vec![1];
    match value {
        PgCanonicalValue::Bool(value) => key.push(u8::from(*value)),
        PgCanonicalValue::Int2(value) => key.extend(order_i16(*value)),
        PgCanonicalValue::Int4(value) => key.extend(order_i32(*value)),
        PgCanonicalValue::Int8(value) => key.extend(order_i64(*value)),
        PgCanonicalValue::Oid(value) => key.extend(value.to_be_bytes()),
        PgCanonicalValue::TransactionId32(value) => key.extend(value.to_be_bytes()),
        PgCanonicalValue::TransactionId64(value) => key.extend(value.to_be_bytes()),
        PgCanonicalValue::CommandId(value) => key.extend(value.to_be_bytes()),
        PgCanonicalValue::TupleId(value) => {
            key.extend(value.block.to_be_bytes());
            key.extend(value.offset.to_be_bytes());
        }
        PgCanonicalValue::Float4(value) => key.extend(order_f32(value.to_value()).to_be_bytes()),
        PgCanonicalValue::Float8(value) => key.extend(order_f64(value.to_value()).to_be_bytes()),
        PgCanonicalValue::Numeric(value) => key.extend(numeric_order_key(value)?),
        PgCanonicalValue::Money(value) => key.extend(order_i64(*value)),
        PgCanonicalValue::Text(value) => key.extend(value.as_bytes()),
        PgCanonicalValue::TsVector(value) => key.extend(value.index_key()),
        PgCanonicalValue::TsQuery(value) => key.extend(value.index_key()),
        PgCanonicalValue::Bytes(value) => key.extend(value),
        PgCanonicalValue::BitString(value) if spec.name == "varbit" => {
            key.extend(varbit_order_key(value));
        }
        PgCanonicalValue::BitString(value) => {
            key.extend(value.bytes());
            key.push(0);
            key.extend((value.bit_len() as u64).to_be_bytes());
        }
        PgCanonicalValue::Date(value) => match value {
            PgDate::NegativeInfinity => key.push(0),
            PgDate::Finite(value) => {
                key.push(1);
                key.extend(order_i32(*value));
            }
            PgDate::PositiveInfinity => key.push(2),
        },
        PgCanonicalValue::Time(value) => key.extend(order_i64(value.micros_since_midnight())),
        PgCanonicalValue::TimeTz(value) => {
            let utc = value.time.micros_since_midnight()
                - i64::from(value.utc_offset_seconds) * 1_000_000;
            key.extend(order_i64(utc));
            key.extend(order_i32(-value.utc_offset_seconds));
        }
        PgCanonicalValue::Timestamp(value) | PgCanonicalValue::TimestampTz(value) => match value {
            PgTimestamp::NegativeInfinity => key.push(0),
            PgTimestamp::Finite(value) => {
                key.push(1);
                key.extend(order_i64(*value));
            }
            PgTimestamp::PositiveInfinity => key.push(2),
        },
        PgCanonicalValue::Interval(value) => {
            let micros = i128::from(value.months) * 30 * 86_400_000_000_i128
                + i128::from(value.days) * 86_400_000_000_i128
                + i128::from(value.micros);
            key.extend(order_i128(micros));
        }
        PgCanonicalValue::Uuid(value) => key.extend(value),
        PgCanonicalValue::JsonText(value) => key.extend(value.as_bytes()),
        PgCanonicalValue::Json(value) => key.extend(canonical_json_value_key(value).as_bytes()),
        PgCanonicalValue::Xml(value) | PgCanonicalValue::JsonPath(value) => {
            key.extend(value.as_bytes())
        }
        PgCanonicalValue::Network(value) => {
            key.push(match value.address {
                PgIpAddress::V4(_) => 0,
                PgIpAddress::V6(_) => 1,
            });
            let address = match value.address {
                PgIpAddress::V4(address) => address.to_vec(),
                PgIpAddress::V6(address) => address.to_vec(),
            };
            for bit in 0..value.prefix {
                let byte = address[usize::from(bit / 8)];
                key.push(if byte & (0x80 >> (bit % 8)) == 0 {
                    1
                } else {
                    2
                });
            }
            // A network sorts before every more-specific network sharing its
            // prefix. At equal mask lengths PostgreSQL compares the full host
            // address. inet and cidr deliberately share comparison identity.
            key.push(0);
            key.extend(address);
        }
        PgCanonicalValue::MacAddress(value) => match value {
            crate::PgMacAddress::Mac48(value) => key.extend(value),
            crate::PgMacAddress::Mac64(value) => key.extend(value),
        },
        PgCanonicalValue::Geometric(value) => key.extend(value.to_postgres_text().as_bytes()),
        PgCanonicalValue::Range(value) => key.extend(range_order_key(value)?),
        PgCanonicalValue::Multirange(ranges) => {
            for range in ranges {
                push_ordered_bytes(&mut key, &range_order_key(range)?);
            }
            key.extend([0, 0]);
        }
        PgCanonicalValue::Array(value) => {
            let element = crate::pg_type_spec(&value.element_type).ok_or_else(|| {
                SqlError::Unsupported(format!(
                    "unregistered array element type {}",
                    value.element_type
                ))
            })?;
            for element_value in &value.elements {
                let element_key = canonical_index_key(element, element_value)?;
                push_ordered_bytes(&mut key, &element_key);
            }
            key.extend([0, 0]);
            key.extend((value.dimensions.len() as u64).to_be_bytes());
            for dimension in &value.dimensions {
                key.extend((dimension.length as u64).to_be_bytes());
                key.extend(order_i32(dimension.lower_bound));
            }
        }
        PgCanonicalValue::OidAlias(value) => match (value.oid, &value.symbolic_name) {
            (Some(oid), _) => {
                key.push(0);
                key.extend(oid.to_be_bytes());
            }
            (None, Some(name)) => {
                key.push(1);
                key.extend(name.as_bytes());
            }
            (None, None) => return Err(invalid_value(spec, &SqlValue::String(String::new()))),
        },
        PgCanonicalValue::Lsn(value) => key.extend(value.to_be_bytes()),
        PgCanonicalValue::Snapshot(value) => {
            key.extend(value.xmin.to_be_bytes());
            key.extend(value.xmax.to_be_bytes());
            for xid in &value.in_progress {
                key.extend(xid.to_be_bytes());
            }
        }
        PgCanonicalValue::Vector(values) => {
            for value in values {
                key.extend(order_f32(value.to_value()).to_be_bytes());
            }
        }
        unsupported => {
            return Err(SqlError::Unsupported(format!(
                "PostgreSQL type {} canonical key is not implemented for {unsupported:?}",
                spec.name
            )))
        }
    }
    Ok(key)
}

fn varbit_order_key(value: &PgBitString) -> Vec<u8> {
    // Encode five base-3 symbols per byte: 1 for bit 0, 2 for bit 1, and a
    // trailing 0 terminator. Numeric byte order then exactly matches
    // PostgreSQL's bitwise lexicographic order, including prefix values.
    let mut key = Vec::with_capacity(value.bit_len().div_ceil(5) + 1);
    let mut encoded = 0_u8;
    let mut width = 0_u8;
    for symbol in value
        .to_bit_text()
        .bytes()
        .map(|bit| if bit == b'0' { 1 } else { 2 })
        .chain(std::iter::once(0))
    {
        encoded = encoded * 3 + symbol;
        width += 1;
        if width == 5 {
            key.push(encoded);
            encoded = 0;
            width = 0;
        }
    }
    if width != 0 {
        key.push(encoded * 3_u8.pow(u32::from(5 - width)));
    }
    key
}

fn push_ordered_bytes(target: &mut Vec<u8>, value: &[u8]) {
    for byte in value {
        if *byte == 0 {
            target.extend([0, 255]);
        } else {
            target.push(*byte);
        }
    }
    target.extend([0, 0]);
}

fn range_order_key(range: &PgRange) -> Result<Vec<u8>> {
    if range.empty {
        // PostgreSQL defines the canonical empty range as less than every
        // non-empty range.
        return Ok(vec![0]);
    }
    let mut key = vec![1];
    key.extend(range_bound_key(&range.lower, true)?);
    key.extend(range_bound_key(&range.upper, false)?);
    Ok(key)
}

fn range_bound_key(bound: &PgRangeBound, lower: bool) -> Result<Vec<u8>> {
    let (marker, value) = match bound {
        PgRangeBound::Unbounded => return Ok(vec![if lower { 0 } else { 3 }]),
        PgRangeBound::Inclusive(value) => (if lower { 1 } else { 2 }, value),
        PgRangeBound::Exclusive(value) => (if lower { 2 } else { 1 }, value),
    };
    let subtype = match value.as_ref() {
        PgCanonicalValue::Int4(_) => "int4",
        PgCanonicalValue::Int8(_) => "int8",
        PgCanonicalValue::Numeric(_) => "numeric",
        PgCanonicalValue::Date(_) => "date",
        PgCanonicalValue::Timestamp(_) => "timestamp",
        PgCanonicalValue::TimestampTz(_) => "timestamptz",
        _ => {
            return Err(SqlError::Unsupported(
                "unsupported range bound type".to_string(),
            ))
        }
    };
    let spec = crate::pg_type_spec(subtype)
        .ok_or_else(|| SqlError::Unsupported(format!("unregistered range subtype {subtype}")))?;
    let mut key = vec![marker];
    key.extend(canonical_index_key(spec, value)?);
    Ok(key)
}

pub(crate) use bicdb_core::{canonical_numeric_parts, numeric_index_key_from_canonical_text};

fn numeric_order_key(value: &PgNumeric) -> Result<Vec<u8>> {
    match value {
        PgNumeric::NegativeInfinity => Ok(vec![0]),
        PgNumeric::Finite {
            negative,
            coefficient,
            display_scale,
        } => {
            let mut digits = coefficient.trim_end_matches('0').to_string();
            if digits.is_empty() {
                digits.push('0');
            }
            if digits == "0" {
                return Ok(vec![2]);
            }
            let exponent = i64::try_from(coefficient.len())
                .ok()
                .and_then(|length| length.checked_sub(i64::from(*display_scale)))
                .ok_or_else(|| {
                    SqlError::InvalidSql("numeric exponent exceeds key range".to_string())
                })?;
            let mut magnitude = Vec::with_capacity(8 + digits.len() + 1);
            magnitude.extend(order_i64(exponent));
            magnitude.extend(digits.bytes());
            magnitude.push(0);
            if *negative {
                for byte in &mut magnitude {
                    *byte = !*byte;
                }
                let mut key = vec![1];
                key.extend(magnitude);
                Ok(key)
            } else {
                let mut key = vec![3];
                key.extend(magnitude);
                Ok(key)
            }
        }
        PgNumeric::PositiveInfinity => Ok(vec![4]),
        PgNumeric::NaN => Ok(vec![5]),
    }
}

fn order_i16(value: i16) -> [u8; 2] {
    (value as u16 ^ (1 << 15)).to_be_bytes()
}

fn order_i32(value: i32) -> [u8; 4] {
    (value as u32 ^ (1 << 31)).to_be_bytes()
}

fn order_i64(value: i64) -> [u8; 8] {
    (value as u64 ^ (1 << 63)).to_be_bytes()
}

fn order_i128(value: i128) -> [u8; 16] {
    (value as u128 ^ (1 << 127)).to_be_bytes()
}

fn order_f32(value: f32) -> u32 {
    let value = if value == 0.0 { 0.0 } else { value };
    if value.is_nan() {
        return u32::MAX;
    }
    let bits = value.to_bits();
    if bits & (1 << 31) == 0 {
        bits ^ (1 << 31)
    } else {
        !bits
    }
}

fn order_f64(value: f64) -> u64 {
    let value = if value == 0.0 { 0.0 } else { value };
    if value.is_nan() {
        return u64::MAX;
    }
    let bits = value.to_bits();
    if bits & (1 << 63) == 0 {
        bits ^ (1 << 63)
    } else {
        !bits
    }
}

static CANONICAL_TEXT_CODEC: PgCanonicalTextCodec = PgCanonicalTextCodec;

pub fn pg_scalar_codec(_spec: &PgTypeSpec) -> &'static dyn PgScalarCodec {
    &CANONICAL_TEXT_CODEC
}

pub fn pg_typed_compare(pg_type: &str, left: &SqlValue, right: &SqlValue) -> Result<Ordering> {
    let oid = crate::pg_type_oid_by_name(pg_type).ok_or_else(|| {
        SqlError::Unsupported(format!("PostgreSQL type {pg_type} has no registered codec"))
    })?;
    CANONICAL_TEXT_CODEC.compare(left, right, PgCodecContext::new(oid, -1))
}

pub(crate) fn column_typed_compare(
    column: &ColumnSchema,
    left: &SqlValue,
    right: &SqlValue,
) -> Result<Ordering> {
    let Some(user_type) = &column.user_type else {
        return pg_typed_compare(&column.pg_type, left, right);
    };
    match &user_type.kind {
        UserTypeKind::Shell => Err(SqlError::undefined_type(format!(
            "{} is only a shell",
            user_type.formatted_name()
        ))),
        UserTypeKind::Base { .. } => Err(SqlError::undefined_function(format!(
            "type {} has no registered comparison operator",
            user_type.formatted_name()
        ))),
        UserTypeKind::Enum { labels } if user_type.array => {
            enum_array_compare(left, right, labels, &user_type.formatted_name())
        }
        UserTypeKind::Enum { labels } => {
            enum_scalar_compare(left, right, labels, &user_type.formatted_name())
        }
        UserTypeKind::Composite { .. } => {
            if crate::eval::compare_values(left, &sqlparser::ast::BinaryOperator::Eq, right)?
                == Some(true)
            {
                Ok(Ordering::Equal)
            } else if crate::eval::compare_values(left, &sqlparser::ast::BinaryOperator::Lt, right)?
                == Some(true)
            {
                Ok(Ordering::Less)
            } else {
                Ok(Ordering::Greater)
            }
        }
        UserTypeKind::Domain { .. } => {
            column_typed_compare(&domain_base_runtime_column(user_type), left, right)
        }
        UserTypeKind::Range { value, .. } => {
            Ok(user_range_order_key(user_type, value, false, left)?
                .cmp(&user_range_order_key(user_type, value, false, right)?))
        }
        UserTypeKind::Multirange { value, .. } => {
            Ok(user_range_order_key(user_type, value, true, left)?
                .cmp(&user_range_order_key(user_type, value, true, right)?))
        }
    }
}

pub(crate) fn pg_typed_compare_for_db(
    db: &BicDb,
    pg_type: &str,
    left: &SqlValue,
    right: &SqlValue,
) -> Result<Ordering> {
    let Some(user_type) = user_type_column_for_name(db, pg_type)? else {
        return pg_typed_compare(pg_type, left, right);
    };
    let column = enum_runtime_column(user_type);
    column_typed_compare(&column, left, right)
}

pub(crate) fn pg_typed_index_key_for_db(
    db: &BicDb,
    pg_type: &str,
    value: &SqlValue,
) -> Result<Vec<u8>> {
    let Some(user_type) = user_type_column_for_name(db, pg_type)? else {
        return pg_typed_index_key(pg_type, value);
    };
    column_typed_index_key(&enum_runtime_column(user_type), value)
}

pub(crate) fn pg_typed_index_label_for_db(
    db: &BicDb,
    pg_type: &str,
    value: &SqlValue,
) -> Result<String> {
    let Some(user_type) = user_type_column_for_name(db, pg_type)? else {
        return pg_typed_index_label(pg_type, value);
    };
    column_typed_index_label(&enum_runtime_column(user_type), value)
}

fn user_type_column_for_name(
    db: &BicDb,
    pg_type: &str,
) -> Result<Option<crate::UserTypeColumnSchema>> {
    let (name, array) = pg_type
        .strip_suffix("[]")
        .map(|name| (name, true))
        .unwrap_or((pg_type, false));
    let (schema_name, name) = name
        .rsplit_once('.')
        .map(|(schema_name, name)| (schema_name, name))
        .unwrap_or(("public", name));
    Ok(load_user_type(db, schema_name, name)?.map(|user_type| user_type.column_type(array)))
}

fn enum_runtime_column(user_type: crate::UserTypeColumnSchema) -> ColumnSchema {
    ColumnSchema {
        name: String::new(),
        pg_type: user_type.formatted_name(),
        user_type: Some(user_type),
        collation: None,
        type_modifier: None,
        array_ndims: 0,
        compression: None,
        primary_key: false,
        hidden: false,
        nullable: true,
        vector_dim: None,
        default_sequence: None,
        default_value: None,
        default_expr: None,
        generated_expr: None,
        identity: None,
    }
}

fn domain_base_runtime_column(user_type: &crate::UserTypeColumnSchema) -> ColumnSchema {
    let UserTypeKind::Domain {
        base_type,
        base_user_type,
        type_modifier,
        collation,
        ..
    } = &user_type.kind
    else {
        unreachable!("domain base column requires a domain type")
    };
    if let Some(base_user_type) = base_user_type {
        let mut base_user_type = (**base_user_type).clone();
        base_user_type.array |= user_type.array;
        return ColumnSchema {
            name: String::new(),
            pg_type: base_user_type.formatted_name(),
            user_type: Some(base_user_type),
            collation: collation.clone(),
            type_modifier: None,
            array_ndims: usize::from(user_type.array),
            compression: None,
            primary_key: false,
            hidden: false,
            nullable: true,
            vector_dim: None,
            default_sequence: None,
            default_value: None,
            default_expr: None,
            generated_expr: None,
            identity: None,
        };
    }
    let pg_type = if user_type.array && !base_type.ends_with("[]") {
        format!("{base_type}[]")
    } else {
        base_type.clone()
    };
    ColumnSchema {
        name: String::new(),
        pg_type,
        user_type: None,
        collation: collation.clone(),
        type_modifier: (!user_type.array).then(|| type_modifier.clone()).flatten(),
        array_ndims: usize::from(user_type.array),
        compression: None,
        primary_key: false,
        hidden: false,
        nullable: true,
        vector_dim: None,
        default_sequence: None,
        default_value: None,
        default_expr: None,
        generated_expr: None,
        identity: None,
    }
}

fn enum_scalar_compare(
    left: &SqlValue,
    right: &SqlValue,
    labels: &[EnumLabelSchema],
    type_name: &str,
) -> Result<Ordering> {
    match (left, right) {
        (SqlValue::Null, SqlValue::Null) => Ok(Ordering::Equal),
        (SqlValue::Null, _) => Ok(Ordering::Greater),
        (_, SqlValue::Null) => Ok(Ordering::Less),
        _ => {
            let left = enum_label_position(labels, &left.to_cell(), type_name)?;
            let right = enum_label_position(labels, &right.to_cell(), type_name)?;
            Ok(left.cmp(&right))
        }
    }
}

fn enum_label_position(labels: &[EnumLabelSchema], label: &str, type_name: &str) -> Result<usize> {
    labels
        .iter()
        .position(|candidate| candidate.label == label)
        .ok_or_else(|| SqlError::invalid_text_representation(type_name, format!("\"{label}\"")))
}

fn enum_array_compare(
    left: &SqlValue,
    right: &SqlValue,
    labels: &[EnumLabelSchema],
    type_name: &str,
) -> Result<Ordering> {
    let left = enum_array_elements(left, type_name)?;
    let right = enum_array_elements(right, type_name)?;
    for (left, right) in left.iter().zip(&right) {
        let ordering = match (left, right) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
            (Some(left), Some(right)) => enum_label_position(labels, left, type_name)?
                .cmp(&enum_label_position(labels, right, type_name)?),
        };
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(left.len().cmp(&right.len()))
}

fn enum_array_elements(value: &SqlValue, type_name: &str) -> Result<Vec<Option<String>>> {
    fn flatten(value: &JsonValue, output: &mut Vec<Option<String>>) -> Result<()> {
        match value {
            JsonValue::Null => output.push(None),
            JsonValue::String(value) => output.push(Some(value.clone())),
            JsonValue::Array(values) => {
                for value in values {
                    flatten(value, output)?;
                }
            }
            _ => {
                return Err(SqlError::InvalidTextRepresentation(
                    "invalid enum array".to_string(),
                ))
            }
        }
        Ok(())
    }

    let SqlValue::Json(value) = value else {
        return Err(SqlError::invalid_text_representation(
            type_name,
            format!("\"{}\"", value.to_cell()),
        ));
    };
    let value = value
        .as_object()
        .and_then(|object| object.get("$bicdb_array_input"))
        .and_then(JsonValue::as_object)
        .and_then(|input| input.get("value"))
        .unwrap_or(value);
    let mut output = Vec::new();
    flatten(value, &mut output)?;
    Ok(output)
}

pub(crate) fn column_typed_not_distinct(
    column: &ColumnSchema,
    left: &SqlValue,
    right: &SqlValue,
) -> Result<bool> {
    match (left, right) {
        (SqlValue::Null, SqlValue::Null) => Ok(true),
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(false),
        _ => Ok(column_typed_compare(column, left, right)? == Ordering::Equal),
    }
}

pub(crate) fn column_typed_index_key(column: &ColumnSchema, value: &SqlValue) -> Result<Vec<u8>> {
    let Some(user_type) = &column.user_type else {
        return pg_typed_index_key(&column.pg_type, value);
    };
    let mut key = user_type.type_oid().to_be_bytes().to_vec();
    match &user_type.kind {
        UserTypeKind::Shell => {
            return Err(SqlError::undefined_type(format!(
                "{} is only a shell",
                user_type.formatted_name()
            )))
        }
        UserTypeKind::Base { .. } => {
            return Err(SqlError::undefined_function(format!(
                "could not identify an ordering operator for type {}",
                user_type.formatted_name()
            )))
        }
        UserTypeKind::Enum { labels } if user_type.array => {
            for value in enum_array_elements(value, &user_type.formatted_name())? {
                let rank = value
                    .as_deref()
                    .map(|value| enum_label_position(labels, value, &user_type.formatted_name()))
                    .transpose()?
                    .map(|rank| rank as u64)
                    .unwrap_or(u64::MAX);
                key.extend_from_slice(&rank.to_be_bytes());
            }
        }
        UserTypeKind::Enum { labels } => {
            let rank = enum_label_position(labels, &value.to_cell(), &user_type.formatted_name())?;
            key.extend_from_slice(&(rank as u64).to_be_bytes());
        }
        UserTypeKind::Composite { .. } => {
            key.extend_from_slice(value.to_cell().as_bytes());
        }
        UserTypeKind::Domain { .. } => {
            key.extend_from_slice(&column_typed_index_key(
                &domain_base_runtime_column(user_type),
                value,
            )?);
        }
        UserTypeKind::Range {
            value: definition, ..
        } => key.extend(user_range_order_key(user_type, definition, false, value)?),
        UserTypeKind::Multirange {
            value: definition, ..
        } => key.extend(user_range_order_key(user_type, definition, true, value)?),
    }
    Ok(key)
}

fn user_range_order_key(
    user_type: &crate::UserTypeColumnSchema,
    definition: &crate::UserRangeValueSchema,
    multirange: bool,
    value: &SqlValue,
) -> Result<Vec<u8>> {
    if matches!(value, SqlValue::Null) {
        return Ok(vec![u8::MAX]);
    }
    if user_type.array {
        let mut key = Vec::new();
        let scalar = crate::UserTypeColumnSchema {
            array: false,
            ..user_type.clone()
        };
        for value in enum_array_elements(value, &user_type.formatted_name())? {
            match value {
                None => push_ordered_bytes(&mut key, &[u8::MAX]),
                Some(value) => push_ordered_bytes(
                    &mut key,
                    &user_range_order_key(
                        &scalar,
                        definition,
                        multirange,
                        &SqlValue::String(value),
                    )?,
                ),
            }
        }
        return Ok(key);
    }
    let text = crate::cast_value_to_user_type(value.clone(), user_type)?.to_cell();
    if multirange {
        let ranges = crate::parse_pg_multirange_with_policy(
            &text,
            &user_type.formatted_name(),
            &definition.subtype,
            definition.canonical_discrete,
        )
        .map_err(|error| SqlError::InvalidTextRepresentation(error.to_string()))?;
        let mut key = Vec::new();
        for range in ranges {
            push_ordered_bytes(&mut key, &range_order_key(&range)?);
        }
        return Ok(key);
    }
    let range = PgRange::from_postgres_text_with_policy(
        &text,
        &user_type.formatted_name(),
        &definition.subtype,
        definition.canonical_discrete,
    )
    .map_err(|error| SqlError::InvalidTextRepresentation(error.to_string()))?;
    range_order_key(&range)
}

pub(crate) fn column_typed_storage_key(column: &ColumnSchema, value: &SqlValue) -> Result<Vec<u8>> {
    let Some(user_type) = &column.user_type else {
        return pg_typed_index_key(&column.pg_type, value);
    };
    if let UserTypeKind::Base { codec_type, .. } = &user_type.kind {
        let codec_type = if user_type.array {
            format!("{codec_type}[]")
        } else {
            codec_type.clone()
        };
        let mut key = user_type.type_oid().to_be_bytes().to_vec();
        key.extend_from_slice(&pg_typed_index_key(&codec_type, value)?);
        return Ok(key);
    }
    column_typed_index_key(column, value)
}

pub(crate) fn column_typed_index_label(column: &ColumnSchema, value: &SqlValue) -> Result<String> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let key = column_typed_index_key(column, value)?;
    let mut label = String::with_capacity(13 + key.len() * 2);
    label.push_str("\0bicdb:typed:");
    for byte in key {
        label.push(HEX[(byte >> 4) as usize] as char);
        label.push(HEX[(byte & 0x0f) as usize] as char);
    }
    Ok(label)
}

pub fn pg_typed_not_distinct(pg_type: &str, left: &SqlValue, right: &SqlValue) -> Result<bool> {
    match (left, right) {
        (SqlValue::Null, SqlValue::Null) => Ok(true),
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(false),
        _ => Ok(pg_typed_compare(pg_type, left, right)? == Ordering::Equal),
    }
}

pub fn pg_typed_hash_key(pg_type: &str, value: &SqlValue) -> Result<Vec<u8>> {
    let oid = crate::pg_type_oid_by_name(pg_type).ok_or_else(|| {
        SqlError::Unsupported(format!("PostgreSQL type {pg_type} has no registered codec"))
    })?;
    CANONICAL_TEXT_CODEC.hash_key(value, PgCodecContext::new(oid, -1))
}

pub fn pg_typed_index_key(pg_type: &str, value: &SqlValue) -> Result<Vec<u8>> {
    let oid = crate::pg_type_oid_by_name(pg_type).ok_or_else(|| {
        SqlError::Unsupported(format!("PostgreSQL type {pg_type} has no registered codec"))
    })?;
    CANONICAL_TEXT_CODEC.index_key(value, PgCodecContext::new(oid, -1))
}

/// `pg_typed_index_key` for a value already parsed to its canonical form —
/// the key the codec derives after its own parse, minus that parse. Only
/// exact where the caller's parse is the codec's (`canonical_value`) parse
/// for the type.
pub(crate) fn pg_typed_index_key_for_canonical(
    pg_type: &str,
    canonical: &PgCanonicalValue,
) -> Result<Vec<u8>> {
    let oid = crate::pg_type_oid_by_name(pg_type).ok_or_else(|| {
        SqlError::Unsupported(format!("PostgreSQL type {pg_type} has no registered codec"))
    })?;
    let spec = pg_type_spec_by_oid(oid).ok_or_else(|| {
        SqlError::Unsupported(format!("PostgreSQL type OID {oid} has no registered codec"))
    })?;
    canonical_index_key(spec, canonical)
}

pub(crate) fn pg_typed_index_label(pg_type: &str, value: &SqlValue) -> Result<String> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let key = pg_typed_index_key(pg_type, value)?;
    let mut label = String::with_capacity(13 + key.len() * 2);
    label.push_str("\0bicdb:typed:");
    for byte in key {
        label.push(HEX[(byte >> 4) as usize] as char);
        label.push(HEX[(byte & 0x0f) as usize] as char);
    }
    Ok(label)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{pg_type_spec, PG_TYPE_SPECS};
    use serde_json::json;

    fn context(pg_type: &str) -> PgCodecContext {
        PgCodecContext::new(pg_type_spec(pg_type).unwrap().oid, -1)
    }

    #[test]
    fn canonical_codec_covers_the_complete_scalar_contract() {
        let codec = pg_scalar_codec(pg_type_spec("text").unwrap());
        let context = context("text");
        let value = codec.parse_text("alpha", context).unwrap();
        assert_eq!(codec.canonical_text(&value, context).unwrap(), "alpha");
        assert_eq!(codec.encode_binary(&value, context).unwrap(), b"alpha");
        assert_eq!(
            codec.decode_binary(b"alpha", context).unwrap(),
            SqlValue::String("alpha".to_string())
        );
        assert_eq!(
            codec
                .compare(&value, &SqlValue::String("beta".to_string()), context)
                .unwrap(),
            Ordering::Less
        );
        assert_eq!(
            codec.hash_key(&value, context).unwrap()[..4],
            25_i32.to_be_bytes()
        );
        assert_eq!(
            codec.index_key(&value, context).unwrap(),
            vec![1, b'a', b'l', b'p', b'h', b'a']
        );
        assert_eq!(codec.cast(&value, context, context).unwrap(), value);
    }

    #[test]
    fn equality_and_order_keys_follow_postgres_numeric_and_float_semantics() {
        let codec = pg_scalar_codec(pg_type_spec("numeric").unwrap());
        let numeric = context("numeric");
        for equal in [("1", "1.0"), ("-0.00", "0"), ("12.300", "12.3")] {
            assert_eq!(
                codec
                    .hash_key(&SqlValue::String(equal.0.into()), numeric)
                    .unwrap(),
                codec
                    .hash_key(&SqlValue::String(equal.1.into()), numeric)
                    .unwrap()
            );
        }
        for ordered in [("-100", "-2"), ("-0.1", "0"), ("1.11", "1.2"), ("9", "10")] {
            assert_eq!(
                codec
                    .compare(
                        &SqlValue::String(ordered.0.into()),
                        &SqlValue::String(ordered.1.into()),
                        numeric,
                    )
                    .unwrap(),
                Ordering::Less
            );
        }

        let float = context("float8");
        assert_eq!(
            codec.hash_key(&SqlValue::Float(-0.0), float).unwrap(),
            codec.hash_key(&SqlValue::Float(0.0), float).unwrap()
        );
        assert_eq!(
            codec
                .compare(
                    &SqlValue::Float(f64::NAN),
                    &SqlValue::Float(f64::INFINITY),
                    float
                )
                .unwrap(),
            Ordering::Greater
        );

        let varbit = context("varbit");
        for (left, right) in [
            ("", "0"),
            ("0", "00"),
            ("001", "01"),
            ("1", "100000000"),
            ("100000000", "11"),
        ] {
            let left = SqlValue::String(left.to_string());
            let right = SqlValue::String(right.to_string());
            assert_eq!(
                codec.compare(&left, &right, varbit).unwrap(),
                Ordering::Less
            );
            assert!(
                codec.index_key(&left, varbit).unwrap() < codec.index_key(&right, varbit).unwrap()
            );
        }
    }

    #[test]
    fn schema_identity_separates_hashes_and_special_keys_are_typed() {
        let codec = pg_scalar_codec(pg_type_spec("text").unwrap());
        let text = codec
            .hash_key(&SqlValue::String("1".into()), context("text"))
            .unwrap();
        let int = codec.hash_key(&SqlValue::Int(1), context("int4")).unwrap();
        assert_ne!(text, int);
        assert_eq!(
            codec
                .compare(
                    &SqlValue::String("192.0.2.1".into()),
                    &SqlValue::String("2001:db8::1".into()),
                    context("inet"),
                )
                .unwrap(),
            Ordering::Less
        );
        assert_eq!(
            codec
                .compare(
                    &SqlValue::String("[1,3)".into()),
                    &SqlValue::String("[2,3)".into()),
                    context("int4range"),
                )
                .unwrap(),
            Ordering::Less
        );
    }

    #[test]
    fn network_btree_and_hash_keys_follow_postgresql_identity() {
        let ordered = [
            "192.168.0.255/32",
            "192.168.1.1/24",
            "192.168.1.200/24",
            "192.168.1.0/25",
        ]
        .map(|value| pg_typed_index_key("inet", &SqlValue::String(value.to_string())).unwrap());
        assert!(ordered.windows(2).all(|pair| pair[0] < pair[1]));

        assert_eq!(
            pg_typed_index_key("inet", &SqlValue::String("10.0.0.0/8".to_string())).unwrap(),
            pg_typed_index_key("cidr", &SqlValue::String("10/8".to_string())).unwrap(),
        );
        assert_eq!(
            pg_typed_hash_key("inet", &SqlValue::String("192.0.2.1".to_string())).unwrap(),
            pg_typed_hash_key("inet", &SqlValue::String("192.0.2.1/32".to_string())).unwrap(),
        );
        assert_eq!(
            pg_typed_hash_key("macaddr", &SqlValue::String("08002b:010203".to_string()),).unwrap(),
            pg_typed_hash_key(
                "macaddr",
                &SqlValue::String("08:00:2b:01:02:03".to_string()),
            )
            .unwrap(),
        );
    }

    #[test]
    fn every_registered_storable_type_has_deterministic_keys() {
        let fixtures = [
            ("bool", SqlValue::Bool(true)),
            ("bytea", SqlValue::String("\\x00ff".into())),
            ("char", SqlValue::String("x".into())),
            ("name", SqlValue::String("name".into())),
            ("int8", SqlValue::Int(8)),
            ("int2", SqlValue::Int(2)),
            ("int2vector", SqlValue::String("1 2 -3".into())),
            ("int4", SqlValue::Int(4)),
            ("regproc", SqlValue::String("now".into())),
            ("text", SqlValue::String("text".into())),
            ("oid", SqlValue::Int(26)),
            ("oidvector", SqlValue::String("1 2 4294967295".into())),
            ("json", SqlValue::Json(json!({"b": 2, "a": 1}))),
            ("xml", SqlValue::String("<value/>".into())),
            ("cidr", SqlValue::String("192.0.2.0/24".into())),
            ("float4", SqlValue::Float(4.5)),
            ("float8", SqlValue::Float(8.5)),
            (
                "macaddr8",
                SqlValue::String("08:00:2b:01:02:03:04:05".into()),
            ),
            ("macaddr", SqlValue::String("08:00:2b:01:02:03".into())),
            ("inet", SqlValue::String("192.0.2.1/24".into())),
            ("varchar", SqlValue::String("varchar".into())),
            ("bpchar", SqlValue::String("fixed   ".into())),
            ("date", SqlValue::String("2024-02-29".into())),
            ("time", SqlValue::String("12:34:56".into())),
            ("timestamp", SqlValue::String("2024-02-29 12:34:56".into())),
            (
                "timestamptz",
                SqlValue::String("2024-02-29 12:34:56+00".into()),
            ),
            ("interval", SqlValue::String("2 days".into())),
            ("timetz", SqlValue::String("12:34:56+02".into())),
            ("numeric", SqlValue::String("123.4500".into())),
            ("money", SqlValue::String("123.45".into())),
            ("bit", SqlValue::String("101".into())),
            ("varbit", SqlValue::String("00101".into())),
            ("regprocedure", SqlValue::String("now()".into())),
            ("regclass", SqlValue::String("public.items".into())),
            ("regtype", SqlValue::String("integer".into())),
            (
                "uuid",
                SqlValue::String("018f22d0-4510-7cc8-9a21-3e78ea2b9c44".into()),
            ),
            ("tsvector", SqlValue::String("'alpha':1".into())),
            ("tsquery", SqlValue::String("alpha & beta".into())),
            ("regconfig", SqlValue::String("english".into())),
            ("regdictionary", SqlValue::String("simple".into())),
            ("jsonb", SqlValue::Json(json!({"exact": "1.20"}))),
            ("jsonpath", SqlValue::String("$.value ? (@ > 2)".into())),
            ("int4range", SqlValue::String("[1,3)".into())),
            ("numrange", SqlValue::String("[1.20,3.40)".into())),
            (
                "tsrange",
                SqlValue::String("[2024-01-01,2024-02-01)".into()),
            ),
            (
                "tstzrange",
                SqlValue::String("[\"2024-01-01 00:00:00+00\",\"2024-02-01 00:00:00+00\")".into()),
            ),
            (
                "daterange",
                SqlValue::String("[2024-01-01,2024-02-01)".into()),
            ),
            ("int8range", SqlValue::String("[1,9007199254740993)".into())),
            ("regnamespace", SqlValue::Int(2200)),
            ("tid", SqlValue::String("(42,7)".into())),
            ("xid", SqlValue::String("4294967295".into())),
            ("cid", SqlValue::String("42".into())),
            ("xid8", SqlValue::String("18446744073709551615".into())),
            ("point", SqlValue::String("(1,2)".into())),
            ("lseg", SqlValue::String("[(1,2),(3,4)]".into())),
            ("path", SqlValue::String("[(1,2),(3,4)]".into())),
            ("box", SqlValue::String("(3,4),(1,2)".into())),
            ("polygon", SqlValue::String("((1,2),(3,4),(5,6))".into())),
            ("line", SqlValue::String("{1,2,3}".into())),
            ("circle", SqlValue::String("<(1,2),3>".into())),
            ("regoper", SqlValue::Int(96)),
            ("regoperator", SqlValue::Int(96)),
            ("refcursor", SqlValue::String("portal name".into())),
            ("txid_snapshot", SqlValue::String("10:20:12,15".into())),
            ("pg_lsn", SqlValue::String("16/B374D848".into())),
            ("regrole", SqlValue::Int(10)),
            ("regcollation", SqlValue::Int(100)),
            ("int4multirange", SqlValue::String("{[1,3),[5,8)}".into())),
            (
                "nummultirange",
                SqlValue::String("{[1.20,3.40),[5.00,8.00)}".into()),
            ),
            (
                "tsmultirange",
                SqlValue::String("{[2024-01-01,2024-02-01)}".into()),
            ),
            (
                "tstzmultirange",
                SqlValue::String(
                    "{[\"2024-01-01 00:00:00+00\",\"2024-02-01 00:00:00+00\")}".into(),
                ),
            ),
            (
                "datemultirange",
                SqlValue::String("{[2024-01-01,2024-02-01)}".into()),
            ),
            (
                "int8multirange",
                SqlValue::String("{[1,9007199254740993)}".into()),
            ),
            ("pg_snapshot", SqlValue::String("10:20:12,15".into())),
            (
                "pg_node_tree",
                SqlValue::String("{CONST :consttype 23}".into()),
            ),
            ("pg_ndistinct", SqlValue::String("{\"1, 2\": 10}".into())),
            (
                "pg_dependencies",
                SqlValue::String("{\"1 => 2\": 1.0}".into()),
            ),
            ("pg_mcv_list", SqlValue::String("opaque-mcv-payload".into())),
            ("vector", SqlValue::Json(json!([1.0, 2.0, 3.0]))),
        ];
        let codec = pg_scalar_codec(pg_type_spec("text").unwrap());
        for spec in PG_TYPE_SPECS {
            let fixture = fixtures.iter().find(|(name, _)| *name == spec.name);
            if spec.pseudo {
                assert!(codec
                    .index_key(
                        &SqlValue::String("value".into()),
                        PgCodecContext::new(spec.oid, -1)
                    )
                    .is_err());
                continue;
            }
            let (_, value) =
                fixture.unwrap_or_else(|| panic!("missing key fixture for {}", spec.name));
            let context = PgCodecContext::new(spec.oid, -1);
            let first = codec.index_key(value, context).unwrap();
            assert_eq!(
                first,
                codec.index_key(value, context).unwrap(),
                "{}",
                spec.name
            );
            assert_eq!(
                codec.hash_key(value, context).unwrap(),
                codec.hash_key(value, context).unwrap(),
                "{}",
                spec.name
            );

            if let Some(array_oid) = spec.array_oid {
                let array_context = PgCodecContext::new(array_oid, -1);
                if spec.pseudo {
                    assert!(codec
                        .index_key(&SqlValue::Json(json!([])), array_context)
                        .is_err());
                    continue;
                }
                let element = match (spec.name, value) {
                    ("vector", _) => json!("[1,2,3]"),
                    (_, SqlValue::Null) => serde_json::Value::Null,
                    (_, SqlValue::Bool(value)) => json!(value),
                    (_, SqlValue::Int(value)) => json!(value),
                    (_, SqlValue::Float(value)) => json!(value),
                    (_, SqlValue::String(value)) => json!(value),
                    (_, SqlValue::TsQuery(value)) => json!(value.to_postgres_text()),
                    (_, SqlValue::JsonText(value)) => value.parsed().clone(),
                    (_, SqlValue::Json(value)) => value.clone(),
                    (_, SqlValue::Geometry(_)) => unreachable!("no geometry key fixtures"),
                    (_, SqlValue::Composite(_)) => unreachable!("no composite key fixtures"),
                };
                let array = SqlValue::Json(json!([element]));
                let first = codec.index_key(&array, array_context).unwrap();
                assert_eq!(
                    first,
                    codec.index_key(&array, array_context).unwrap(),
                    "{}[]",
                    spec.name
                );
            }
        }
    }

    #[test]
    fn array_keys_include_rank_dimensions_lower_bounds_and_typed_elements() {
        let codec = pg_scalar_codec(pg_type_spec("int4").unwrap());
        let context = PgCodecContext::new(pg_type_spec("int4").unwrap().array_oid.unwrap(), -1);
        let one_dimensional = SqlValue::String("{1,2,3,4}".into());
        let matrix = SqlValue::String("{{1,2},{3,4}}".into());
        let shifted = SqlValue::String("[0:1][1:2]={{1,2},{3,4}}".into());
        assert_ne!(
            codec.hash_key(&one_dimensional, context).unwrap(),
            codec.hash_key(&matrix, context).unwrap()
        );
        assert_ne!(
            codec.hash_key(&matrix, context).unwrap(),
            codec.hash_key(&shifted, context).unwrap()
        );
        assert_eq!(
            codec
                .compare(
                    &SqlValue::String("{1,2}".into()),
                    &SqlValue::String("{1,3}".into()),
                    context,
                )
                .unwrap(),
            Ordering::Less
        );
    }

    #[test]
    fn canonical_codec_reports_invalid_binary_text_with_postgres_sqlstate() {
        let codec = pg_scalar_codec(pg_type_spec("text").unwrap());
        let error = codec.decode_binary(&[0xff], context("text")).unwrap_err();
        assert_eq!(error.sqlstate(), "22P02");
    }
}

#[cfg(test)]
mod numeric_fast_path_tests {
    use super::*;

    fn texts() -> Vec<String> {
        let mut out = vec![
            "0",
            "0.0",
            "0.00",
            "1",
            "-1",
            "10",
            "100",
            "0.5",
            "0.50",
            "0.05",
            "-0.05",
            "12.50",
            "-12.50",
            "123456789012.34",
            "-123456789012.34",
            "999999.99",
            "1.000",
            "10.10",
            "0.000001",
            "-0.000001",
            "7",
            "-7",
            "3000.00",
            "42.1",
            "12345678901234567890.123456",
        ]
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();
        // A sweep of values around the scales TPC-C uses.
        for whole in [0i64, 1, 9, 10, 99, 100, 4999, 12345, 999999, 3000001] {
            for frac in ["", "0", "5", "00", "05", "50", "99", "123", "007"] {
                for sign in ["", "-"] {
                    let text = if frac.is_empty() {
                        format!("{sign}{whole}")
                    } else {
                        format!("{sign}{whole}.{frac}")
                    };
                    out.push(text);
                }
            }
        }
        out
    }

    #[test]
    fn canonical_text_index_key_matches_the_parsed_path() {
        for text in texts() {
            let fast = numeric_index_key_from_canonical_text(&text);
            let parsed = PgNumeric::from_postgres_text(&text).unwrap();
            let canonical = parsed.to_decimal_text() == text;
            let slow =
                pg_typed_index_key_for_canonical("numeric", &PgCanonicalValue::Numeric(parsed))
                    .unwrap();
            match fast {
                Some(key) => {
                    assert!(canonical, "{text}: fast path accepted non-canonical text");
                    assert_eq!(key, slow, "{text}");
                }
                None => assert!(
                    !canonical,
                    "{text}: canonical text declined by the fast path"
                ),
            }
        }
        for bad in [
            "00.5", ".5", "1.", "-0", "-0.00", "1e3", "NaN", "Infinity", "+1", " 1", "1 ",
        ] {
            assert!(
                numeric_index_key_from_canonical_text(bad).is_none(),
                "{bad}"
            );
        }
    }

    #[test]
    fn canonical_parts_split_exactly() {
        assert_eq!(canonical_numeric_parts("12.50"), Some((false, "12", "50")));
        assert_eq!(canonical_numeric_parts("-0.05"), Some((true, "0", "05")));
        assert_eq!(canonical_numeric_parts("7"), Some((false, "7", "")));
        assert_eq!(canonical_numeric_parts("0"), Some((false, "0", "")));
        assert_eq!(canonical_numeric_parts("-0.00"), None);
        assert_eq!(canonical_numeric_parts("007"), None);
    }
}
