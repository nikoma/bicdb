//! Split out of the parent module to keep files digestible; behavior
//! unchanged. Items are re-exported from the parent via `pub(crate) use`.
use super::*;
#[allow(unused_imports)]
use crate::*;

pub(crate) fn oid_alias_proc_arg_types(row: &BTreeMap<String, SqlValue>) -> Vec<String> {
    let oid = row.get("oid").and_then(sql_value_i64).unwrap_or_default();
    if let Some(types) = builtin_proc_arg_types(oid) {
        return types.iter().map(|value| (*value).to_string()).collect();
    }
    row.get("proargtypes")
        .map(SqlValue::to_cell)
        .unwrap_or_default()
        .split_whitespace()
        .filter_map(|oid| oid.parse::<i64>().ok())
        .filter_map(pg_type_name_from_oid)
        .map(str::to_string)
        .collect()
}

pub(crate) fn resolve_regprocedure_oid(
    db: &BicDb,
    pg_type: &str,
    value: &str,
) -> Result<Option<u32>> {
    let (name, signature) = split_alias_signature(value);
    if pg_type == "regprocedure" && signature.is_none() {
        return Ok(None);
    }
    let (schema, name) = alias_schema_and_name(name);
    let requested_namespace = schema.as_ref().map(|schema| namespace_oid(schema));
    let mut matches = pg_proc_rows(db)?
        .into_iter()
        .filter(|row| {
            row.get("proname")
                .is_some_and(|candidate| candidate.to_cell() == name)
        })
        .filter(|row| {
            requested_namespace.is_none_or(|namespace| {
                row.get("pronamespace").and_then(sql_value_i64) == Some(namespace)
            })
        })
        .filter(|row| {
            signature.as_ref().is_none_or(|types| {
                let actual = oid_alias_proc_arg_types(row)
                    .into_iter()
                    .map(|name| pg_type_regtype_name(&name).unwrap_or(name))
                    .collect::<Vec<_>>();
                actual == *types
            })
        })
        .filter_map(|row| row.get("oid").and_then(sql_value_i64))
        .filter_map(|oid| u32::try_from(oid).ok());
    let first = matches.next();
    if first.is_some() && matches.next().is_some() {
        return Ok(None);
    }
    Ok(first)
}

pub(crate) fn resolve_regprocedure_name(
    db: &BicDb,
    pg_type: &str,
    oid: i64,
) -> Result<Option<String>> {
    let row = pg_proc_rows(db)?
        .into_iter()
        .find(|row| row.get("oid").and_then(sql_value_i64) == Some(oid));
    let Some(row) = row else {
        return Ok(None);
    };
    let name = row
        .get("proname")
        .map(SqlValue::to_cell)
        .unwrap_or_default();
    let namespace = row
        .get("pronamespace")
        .and_then(sql_value_i64)
        .map(|oid| resolve_regnamespace_name(db, oid))
        .transpose()?
        .flatten();
    let name = match namespace.as_deref() {
        Some(schema) if !matches!(schema, "public" | "pg_catalog") => {
            format!("{}.{}", pg_quote_ident(schema), pg_quote_ident(&name))
        }
        _ => pg_quote_ident(&name),
    };
    if pg_type == "regproc" {
        return Ok(Some(name));
    }
    let args = oid_alias_proc_arg_types(&row).join(",");
    Ok(Some(format!("{name}({args})")))
}

pub(crate) fn split_alias_signature(value: &str) -> (&str, Option<Vec<String>>) {
    let value = value.trim();
    let Some(open) = value.find('(') else {
        return (value, None);
    };
    let Some(args) = value.strip_suffix(')').map(|value| &value[open + 1..]) else {
        return (value, None);
    };
    let types = if args.trim().is_empty() {
        Vec::new()
    } else {
        args.split(',')
            .map(|name| pg_type_regtype_name(name).unwrap_or_else(|| name.trim().to_owned()))
            .collect::<Vec<_>>()
    };
    (&value[..open], Some(types))
}

pub(crate) fn resolve_regoperator_oid(pg_type: &str, value: &str) -> Result<Option<u32>> {
    let (name, signature) = split_alias_signature(value);
    if pg_type == "regoperator" && signature.is_none() {
        return Ok(None);
    }
    let (schema, name) = alias_schema_and_name(name);
    if !alias_schema_is(schema.as_deref(), "pg_catalog") {
        return Ok(None);
    }
    let mut matches = REGOPERATOR_OBJECTS
        .iter()
        .filter(|(_, candidate, left, right)| {
            *candidate == name
                && signature.as_ref().is_none_or(|types| {
                    types.as_slice() == [(*left).to_string(), (*right).to_string()]
                })
        });
    let first = matches.next().map(|value| value.0);
    if first.is_some() && matches.next().is_some() {
        return Ok(None);
    }
    Ok(first)
}

pub(crate) fn resolve_regoperator_name(pg_type: &str, oid: i64) -> Option<String> {
    let (_, name, left, right) = REGOPERATOR_OBJECTS
        .iter()
        .find(|(candidate, _, _, _)| i64::from(*candidate) == oid)?;
    Some(if pg_type == "regoper" {
        (*name).to_string()
    } else {
        format!("{name}({left},{right})")
    })
}

pub(crate) fn undefined_oid_alias(pg_type: &str, value: &str) -> SqlError {
    let (state, object) = match pg_type {
        "regclass" => ("42P01", "relation"),
        "regnamespace" => ("3F000", "schema"),
        "regproc" | "regprocedure" => ("42883", "function"),
        "regoper" | "regoperator" => ("42883", "operator"),
        "regrole" => ("42704", "role"),
        "regcollation" => ("42704", "collation"),
        "regconfig" => ("42704", "text search configuration"),
        "regdictionary" => ("42704", "text search dictionary"),
        _ => ("42704", "type"),
    };
    SqlError::data_exception(
        state,
        format!("{object} \"{value}\" does not exist"),
        Some(value.to_string()),
    )
}

pub(crate) fn regtype_text_value(db: &BicDb, value: SqlValue) -> Result<SqlValue> {
    if matches!(value, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let value_text = value.to_cell();
    let oid = sql_value_i64(&value)
        .or_else(|| {
            value_text
                .parse::<i64>()
                .ok()
                .or_else(|| pg_type_regtype_name(&value_text).map(|pg_type| pg_type_oid(&pg_type)))
        })
        .or_else(|| {
            pg_user_type_oid_by_name(db, &value_text)
                .ok()
                .flatten()
                .map(i64::from)
        });
    let Some(oid) = oid else {
        return Err(SqlError::InvalidSql(format!(
            "cannot cast {} to regtype text",
            value.to_cell()
        )));
    };
    let formatted = eval_db_catalog_function_value(
        db,
        "format_type",
        &[SqlValue::Int(oid), SqlValue::Int(-1)],
        None,
        None,
    )?
    .unwrap_or_else(|| SqlValue::String(oid.to_string()));
    if formatted == SqlValue::String("???".to_string()) {
        Ok(SqlValue::String(oid.to_string()))
    } else {
        Ok(formatted)
    }
}

pub(crate) fn regclass_text_cast_source<'a>(
    expr: &'a Expr,
    data_type: &DataType,
) -> Result<Option<&'a Expr>> {
    let Ok((pg_type, _)) = pg_type_from_data_type(data_type) else {
        return Ok(None);
    };
    if !matches!(pg_type.as_str(), "text" | "varchar") {
        return Ok(None);
    }
    let Expr::Cast {
        expr, data_type, ..
    } = expr
    else {
        return Ok(None);
    };
    if matches!(data_type, DataType::Regclass) {
        Ok(Some(expr))
    } else {
        Ok(None)
    }
}

pub(crate) fn regtype_text_cast_source<'a>(
    expr: &'a Expr,
    data_type: &DataType,
) -> Result<Option<&'a Expr>> {
    let Ok((pg_type, _)) = pg_type_from_data_type(data_type) else {
        return Ok(None);
    };
    if !matches!(pg_type.as_str(), "text" | "varchar" | "bpchar" | "name") {
        return Ok(None);
    }
    let Expr::Cast {
        expr, data_type, ..
    } = expr
    else {
        return Ok(None);
    };
    Ok(pg_type_from_data_type(data_type)
        .is_ok_and(|(pg_type, _)| pg_type == "regtype")
        .then_some(expr))
}

pub(crate) fn regclass_display_cast_source<'a>(
    expr: &'a Expr,
    data_type: &DataType,
) -> Option<&'a Expr> {
    if !matches!(data_type, DataType::Regclass) {
        return None;
    }
    match expr {
        Expr::Function(function) if function_name_is(function, "pg_get_serial_sequence") => {
            Some(expr)
        }
        _ => None,
    }
}

pub(crate) fn regclass_text_value(db: &BicDb, value: SqlValue) -> Result<SqlValue> {
    if matches!(value, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    if let Some(oid) = sql_value_i64(&value) {
        return Ok(resolve_regclass_name(db, oid)?
            .map(SqlValue::String)
            .unwrap_or_else(|| SqlValue::String(oid.to_string())));
    }
    if let SqlValue::String(name) = value {
        if let Some(oid) = resolve_regclass_oid(db, &name) {
            return Ok(resolve_regclass_name(db, oid)?
                .map(SqlValue::String)
                .unwrap_or_else(|| SqlValue::String(regclass_relation_name(&name))));
        }
        return Ok(SqlValue::String(regclass_relation_name(&name)));
    }
    Err(SqlError::InvalidSql(format!(
        "cannot cast {} to regclass",
        value.to_cell()
    )))
}

pub(crate) fn resolve_regclass_oid(db: &BicDb, name: &str) -> Option<i64> {
    let (schema_name, relation) = alias_schema_and_name(name);
    if alias_schema_is(schema_name.as_deref(), "pg_catalog") {
        if let Some(oid) = virtual_catalog_table_oid(&relation) {
            return Some(oid);
        }
    }
    let schemas = list_schemas(db).ok()?;
    if let Some(schema) = schemas.iter().find(|schema| {
        // A table outside `public` is stored under a schema-isolated physical
        // name, so match the logical name it encodes as well as the raw one.
        let logical = crate::eval::rewrite_helpers::logical_relation_name(&schema.name);
        (schema.name.eq_ignore_ascii_case(&relation)
            || logical
                .as_deref()
                .is_some_and(|name| name.eq_ignore_ascii_case(&relation)))
            && schema_name.as_deref().map_or_else(
                || schema.schema_name.eq_ignore_ascii_case("public"),
                |requested| schema.schema_name.eq_ignore_ascii_case(requested),
            )
    }) {
        return Some(table_relation_oid(schema));
    }
    // Sequences are stored under their bare name (see
    // `normalize_sequence_name`), so a schema-qualified `'<schema>.<seq>'`
    // — exactly what pg_dump emits for SERIAL defaults and setval() — has
    // to be matched on the relation part. Checked before the
    // non-public-schema bail-out below, which would otherwise reject every
    // qualified sequence reference outright.
    if let Some(oid) = list_sequences(db)
        .ok()
        .into_iter()
        .flatten()
        .find(|sequence| sequence.name.eq_ignore_ascii_case(&relation))
        .map(|sequence| sequence_oid(&sequence.name))
    {
        return Some(oid);
    }
    if schema_name
        .as_deref()
        .is_some_and(|schema| !schema.eq_ignore_ascii_case("public"))
    {
        return None;
    }
    (*table_oids(db))
        .clone()
        .into_iter()
        .find(|(table, _)| {
            table.eq_ignore_ascii_case(&relation)
                && !schemas
                    .iter()
                    .any(|schema| schema.name.eq_ignore_ascii_case(table))
        })
        .map(|(_, oid)| oid)
        .or_else(|| {
            list_sequences(db)
                .ok()?
                .into_iter()
                .find(|sequence| sequence.name.eq_ignore_ascii_case(&relation))
                .map(|sequence| sequence_oid(&sequence.name))
        })
}

pub(crate) fn resolve_regclass_name(db: &BicDb, oid: i64) -> Result<Option<String>> {
    if let Some(name) = virtual_catalog_name_for_oid(oid) {
        return Ok(Some(name.to_string()));
    }
    if let Some(name) = list_schemas(db)?.into_iter().find_map(|schema| {
        (table_relation_oid(&schema) == oid).then(|| {
            // Outside `public` the stored name is the schema-isolated physical
            // one; render the relation the way it was written.
            let display = crate::eval::rewrite_helpers::logical_relation_name(&schema.name)
                .unwrap_or_else(|| schema.name.clone());
            if schema.schema_name.eq_ignore_ascii_case("public") {
                display
            } else {
                format!("{}.{}", schema.schema_name, display)
            }
        })
    }) {
        return Ok(Some(name));
    }
    if let Some((relation, _)) = (*table_oids(db))
        .clone()
        .into_iter()
        .find(|(_, candidate)| *candidate == oid)
    {
        return Ok(Some(relation));
    }
    Ok(list_sequences(db)?
        .into_iter()
        .find(|sequence| sequence_oid(&sequence.name) == oid)
        .map(|sequence| sequence.name))
}

pub(crate) fn virtual_catalog_name_for_oid(oid: i64) -> Option<&'static str> {
    match oid {
        1213 => Some("pg_tablespace"),
        826 => Some("pg_default_acl"),
        1247 => Some("pg_type"),
        1249 => Some("pg_attribute"),
        PG_PROC_CATALOG_OID => Some("pg_proc"),
        PG_CLASS_CATALOG_OID => Some("pg_class"),
        1260 => Some("pg_authid"),
        1262 => Some("pg_database"),
        1417 => Some("pg_foreign_server"),
        1418 => Some("pg_user_mapping"),
        2328 => Some("pg_foreign_data_wrapper"),
        2601 => Some("pg_am"),
        2602 => Some("pg_amop"),
        2603 => Some("pg_amproc"),
        2604 => Some("pg_attrdef"),
        2605 => Some("pg_cast"),
        PG_CONSTRAINT_CATALOG_OID => Some("pg_constraint"),
        2607 => Some("pg_conversion"),
        2608 => Some("pg_depend"),
        2964 => Some("pg_db_role_setting"),
        2609 => Some("pg_description"),
        2610 => Some("pg_index"),
        2611 => Some("pg_inherits"),
        2995 => Some("pg_largeobject_metadata"),
        PG_LANGUAGE_CATALOG_OID => Some("pg_language"),
        2615 => Some("pg_namespace"),
        2616 => Some("pg_opclass"),
        2617 => Some("pg_operator"),
        2618 => Some("pg_rewrite"),
        2620 => Some("pg_trigger"),
        2753 => Some("pg_opfamily"),
        PG_EXTENSION_CATALOG_OID => Some("pg_extension"),
        3118 => Some("pg_foreign_table"),
        3256 => Some("pg_policy"),
        3350 => Some("pg_partitioned_table"),
        3381 => Some("pg_statistic_ext"),
        3394 => Some("pg_init_privs"),
        3429 => Some("pg_statistic_ext_data"),
        3456 => Some("pg_collation"),
        3466 => Some("pg_event_trigger"),
        3501 => Some("pg_enum"),
        3541 => Some("pg_range"),
        3576 => Some("pg_transform"),
        3592 => Some("pg_shseclabel"),
        3596 => Some("pg_seclabel"),
        3600 => Some("pg_ts_dict"),
        3601 => Some("pg_ts_parser"),
        3602 => Some("pg_ts_config"),
        3603 => Some("pg_ts_config_map"),
        3764 => Some("pg_ts_template"),
        6100 => Some("pg_subscription"),
        6102 => Some("pg_subscription_rel"),
        6104 => Some("pg_publication"),
        6106 => Some("pg_publication_rel"),
        6237 => Some("pg_publication_namespace"),
        2224 => Some("pg_sequence"),
        _ => None,
    }
}

pub(crate) fn regclass_relation_name(name: &str) -> String {
    let last_part = name.trim().rsplit('.').next().unwrap_or(name).trim();
    if last_part.len() >= 2 && last_part.starts_with('"') && last_part.ends_with('"') {
        last_part[1..last_part.len() - 1].replace("\"\"", "\"")
    } else {
        last_part.to_string()
    }
}

/// Cheap identity case outside the large cast implementation, whose stable
/// control-flow graph can keep using existing profile-guided training data.
#[inline]
pub(crate) fn cast_value_to_pg_type_fast(value: SqlValue, pg_type: &str) -> Result<SqlValue> {
    if pg_type == "numeric" {
        if let SqlValue::String(text) = &value {
            if bicdb_core::canonical_numeric_parts(text).is_some_and(|(_, whole, fraction)| {
                whole.len() <= crate::typed_value::PG_NUMERIC_MAX_INTEGER_DIGITS as usize
                    && fraction.len()
                        <= crate::typed_value::PG_NUMERIC_MAX_FRACTIONAL_DIGITS as usize
            }) {
                return Ok(value);
            }
        }
    }
    cast_value_to_pg_type(value, pg_type)
}

pub(crate) fn cast_value_to_pg_type(value: SqlValue, pg_type: &str) -> Result<SqlValue> {
    if matches!(value, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    if let Some(element_type) = pg_type.strip_suffix("[]") {
        return cast_value_to_array(value, element_type);
    }
    if let Some(byte) = pg_internal_char_byte(&value) {
        match pg_type {
            "char" => return Ok(pg_internal_char_value(byte)),
            "int4" => return Ok(SqlValue::Int(i64::from(byte as i8))),
            "text" | "varchar" | "bpchar" | "name" => {
                return cast_value_to_pg_type(
                    SqlValue::String(pg_internal_char_text(byte)),
                    pg_type,
                );
            }
            _ => {}
        }
    }
    match pg_type {
        "oid" => match value {
            SqlValue::Int(value) if value < 0 => i32::try_from(value)
                .map(|value| SqlValue::Int(i64::from(value as u32)))
                .map_err(|_| SqlError::numeric_value_out_of_range("OID out of range")),
            SqlValue::Int(value) => u32::try_from(value)
                .map(|value| SqlValue::Int(i64::from(value)))
                .map_err(|_| SqlError::numeric_value_out_of_range("OID out of range")),
            SqlValue::String(value) => parse_pg_oid(&value)
                .map(|value| SqlValue::Int(i64::from(value)))
                .map_err(|error| match error {
                    PgOidParseError::InvalidSyntax => {
                        SqlError::invalid_text_representation("oid", format!("\"{value}\""))
                    }
                    PgOidParseError::OutOfRange => SqlError::numeric_value_out_of_range(format!(
                        "value \"{value}\" is out of range for type oid"
                    )),
                }),
            SqlValue::Float(_) => Err(SqlError::cannot_coerce("cannot cast type numeric to oid")),
            other => Err(SqlError::InvalidSql(format!(
                "cannot cast {} to oid",
                other.to_cell()
            ))),
        },
        pg_type if is_oid_alias_type(pg_type) => match value {
            SqlValue::Int(value) => Ok(SqlValue::Int(value)),
            SqlValue::String(value) => value
                .parse::<u32>()
                .ok()
                .or_else(|| resolve_builtin_oid_alias(pg_type, &value))
                .map(|oid| SqlValue::Int(i64::from(oid)))
                .ok_or_else(|| undefined_oid_alias(pg_type, &value)),
            other => Err(SqlError::InvalidSql(format!(
                "cannot cast {} to {pg_type}",
                other.to_cell()
            ))),
        },
        "bool" => {
            match value {
                SqlValue::Bool(value) => Ok(SqlValue::Bool(value)),
                SqlValue::String(value) => parse_pg_bool_text(&value)
                    .map(SqlValue::Bool)
                    .ok_or_else(|| {
                        SqlError::InvalidTextRepresentation(format!(
                            "invalid input syntax for type boolean: \"{value}\""
                        ))
                    }),
                other => Err(SqlError::InvalidSql(format!(
                    "cannot cast {} to bool",
                    other.to_cell()
                ))),
            }
        }
        "int2" | "int4" | "int8" => {
            let value = match value {
                SqlValue::Int(value) => Ok(value),
                SqlValue::Float(value) => float_to_i64_rounded(value),
                SqlValue::String(value) if is_valid_numeric_text(&value) => {
                    parse_decimal(&value).and_then(decimal_to_i64_rounded)
                }
                SqlValue::String(value) => Err(SqlError::InvalidTextRepresentation(format!(
                    "invalid input syntax for type integer: \"{value}\""
                ))),
                other => Err(SqlError::InvalidSql(format!(
                    "cannot cast {} to integer",
                    other.to_cell()
                ))),
            }
            .map_err(|error| match error {
                SqlError::InvalidSql(message) if message.contains("out of range") => {
                    integer_out_of_range(pg_type)
                }
                error => error,
            })?;
            enforce_integer_range(value, pg_type).map(SqlValue::Int)
        }
        "float4" | "float8" => cast_value_to_float(value, pg_type),
        "numeric" => match value {
            SqlValue::Int(value) => Ok(SqlValue::String(value.to_string())),
            SqlValue::String(value) => PgNumeric::from_postgres_text(&value)
                .map(|value| SqlValue::String(value.to_decimal_text()))
                .map_err(|error| match error {
                    PgCanonicalValueError::NumericOverflow => {
                        SqlError::numeric_value_out_of_range("value overflows numeric format")
                    }
                    _ => SqlError::InvalidTextRepresentation(format!(
                        "invalid input syntax for type numeric: \"{value}\""
                    )),
                }),
            SqlValue::Float(value) => Ok(SqlValue::String(if value.is_nan() {
                "NaN".to_string()
            } else if value == f64::INFINITY {
                "Infinity".to_string()
            } else if value == f64::NEG_INFINITY {
                "-Infinity".to_string()
            } else {
                canonical_float_decimal(value)?
            })),
            other => Err(SqlError::InvalidTextRepresentation(format!(
                "cannot cast {} to numeric",
                other.to_cell()
            ))),
        },
        "money" => {
            let text = match value {
                SqlValue::Int(value) => value.to_string(),
                SqlValue::Float(value) => canonical_float_decimal(value)?,
                SqlValue::String(value) => value,
                other => {
                    return Err(SqlError::InvalidTextRepresentation(format!(
                        "cannot cast {} to money",
                        other.to_cell()
                    )));
                }
            };
            crate::pg_money_cents_from_text(&text)
                .map(crate::pg_money_text_from_cents)
                .map(SqlValue::String)
                .map_err(|error| match error {
                    PgCanonicalValueError::NumericOverflow => SqlError::money_out_of_range(),
                    _ => SqlError::InvalidTextRepresentation(format!(
                        "invalid input syntax for type money: \"{text}\""
                    )),
                })
        }
        "json" => match value {
            SqlValue::JsonText(value) => Ok(SqlValue::JsonText(value)),
            SqlValue::Json(value) => Ok(SqlValue::JsonText(PgJsonText::from_value(value))),
            SqlValue::Geometry(value) => Ok(SqlValue::JsonText(PgJsonText::from_value(
                value.to_geojson_value(),
            ))),
            SqlValue::String(value) => PgJsonText::parse(value.clone())
                .map(SqlValue::JsonText)
                .map_err(|_| {
                    SqlError::invalid_text_representation(
                        "json",
                        format!("invalid input syntax for type json: \"{value}\""),
                    )
                }),
            other => Ok(SqlValue::JsonText(PgJsonText::from_value(
                sql_value_to_json(other),
            ))),
        },
        "jsonb" => {
            let value = match value {
                SqlValue::JsonText(value) => {
                    if value.has_invalid_unicode_escape() {
                        return Err(SqlError::invalid_text_representation(
                            "jsonb",
                            "invalid input syntax for type jsonb: unsupported Unicode escape sequence",
                        ));
                    }
                    value.parsed
                }
                SqlValue::Json(value) => value,
                SqlValue::Geometry(value) => value.to_geojson_value(),
                SqlValue::String(value) => serde_json::from_str(&value).map_err(|_| {
                    SqlError::invalid_text_representation(
                        "jsonb",
                        format!("invalid input syntax for type jsonb: \"{value}\""),
                    )
                })?,
                other => sql_value_to_json(other),
            };
            validate_jsonb_numeric_range(&value)?;
            Ok(SqlValue::Json(value))
        }
        "char" => {
            if let Some(byte) = pg_internal_char_byte(&value) {
                return Ok(pg_internal_char_value(byte));
            }
            let byte = match value {
                SqlValue::Int(value) => i8::try_from(value)
                    .map(|value| value as u8)
                    .map_err(|_| SqlError::numeric_value_out_of_range("\"char\" out of range"))?,
                SqlValue::String(value) => {
                    if value.contains('\0') {
                        return Err(SqlError::data_exception(
                            "22021",
                            "invalid byte sequence for encoding \"UTF8\": 0x00",
                            Some("char".to_string()),
                        ));
                    }
                    value.as_bytes().first().copied().unwrap_or(0)
                }
                other => {
                    return Err(SqlError::cannot_coerce(format!(
                        "cannot cast {} to \"char\"",
                        other.to_cell()
                    )));
                }
            };
            Ok(pg_internal_char_value(byte))
        }
        "vector" => Ok(SqlValue::Json(vector_json(&sql_value_to_vector(&value)?))),
        "bytea" => match value {
            SqlValue::Json(JsonValue::Array(values)) => values
                .into_iter()
                .map(|value| value.as_u64().and_then(|value| u8::try_from(value).ok()))
                .collect::<Option<Vec<_>>>()
                .map(|bytes| SqlValue::String(format_bytea_hex(&bytes)))
                .ok_or_else(|| {
                    SqlError::InvalidTextRepresentation(
                        "invalid byte value in bytea array".to_string(),
                    )
                }),
            value => {
                let text = value.to_cell();
                parse_bytea_text(&text)
                    .map(|bytes| SqlValue::String(format_bytea_hex(&bytes)))
                    .map_err(|error| match error {
                        PgCanonicalValueError::InvalidByteaHex => {
                            SqlError::invalid_parameter_value(format!(
                                "invalid hexadecimal digit in bytea value: {text}"
                            ))
                        }
                        _ => SqlError::invalid_text_representation(
                            "bytea",
                            format!("invalid input syntax for type bytea: \"{text}\""),
                        ),
                    })
            }
        },
        "bit" | "varbit" => {
            let text = value.to_cell();
            PgBitString::from_postgres_text(&text)
                .map(|bits| SqlValue::String(bits.to_bit_text()))
                .map_err(|_| {
                    SqlError::InvalidTextRepresentation(format!(
                        "invalid input syntax for type {pg_type}: \"{text}\""
                    ))
                })
        }
        "interval" => match value {
            SqlValue::String(value) => PgInterval::from_postgres_text(&value)
                .map(|interval| SqlValue::String(render_interval(interval)))
                .map_err(|error| postgres_interval_input_error(&value, error)),
            other => Err(SqlError::Unsupported(format!(
                "cast from {} to interval is not supported",
                other.to_cell()
            ))),
        },
        "cidr" | "inet" | "macaddr" | "macaddr8" | "point" | "line" | "lseg" | "box" | "path"
        | "polygon" | "circle" | "int4range" | "numrange" | "tsrange" | "tstzrange"
        | "daterange" | "int8range" | "int4multirange" | "nummultirange" | "tsmultirange"
        | "tstzmultirange" | "datemultirange" | "int8multirange" | "pg_lsn" | "pg_snapshot"
        | "txid_snapshot" | "xid" | "xid8" | "cid" | "tid" => {
            let text = value.to_cell();
            parse_pg_canonical_special(pg_type, &text)
                .and_then(|value| {
                    value.ok_or_else(|| PgCanonicalValueError::InvalidSpecialValue {
                        pg_type: pg_type.to_string(),
                        value: text.clone(),
                    })
                })
                .map(|value| match value {
                    PgCanonicalValue::Network(network) => {
                        SqlValue::String(network.to_postgres_text())
                    }
                    PgCanonicalValue::MacAddress(address) => {
                        SqlValue::String(address.to_postgres_text())
                    }
                    PgCanonicalValue::Geometric(geometry) => {
                        SqlValue::String(geometry.to_postgres_text())
                    }
                    PgCanonicalValue::Range(range) => {
                        SqlValue::String(render_range_for_session(&range, pg_type))
                    }
                    PgCanonicalValue::Multirange(ranges) => {
                        SqlValue::String(render_multirange_for_session(&ranges, pg_type))
                    }
                    PgCanonicalValue::TransactionId32(value)
                    | PgCanonicalValue::CommandId(value) => SqlValue::Int(i64::from(value)),
                    PgCanonicalValue::TransactionId64(value) => SqlValue::String(value.to_string()),
                    PgCanonicalValue::TupleId(value) => SqlValue::String(value.to_postgres_text()),
                    PgCanonicalValue::Snapshot(snapshot) => {
                        SqlValue::String(snapshot.to_postgres_text())
                    }
                    _ => SqlValue::String(text.trim().to_string()),
                })
                .map_err(|error| postgres_range_input_error(pg_type, &text, error))
        }
        "int2vector" | "oidvector" => {
            let text = value.to_cell();
            let canonical = crate::type_codec::canonical_catalog_vector(pg_type, &text)?;
            let PgCanonicalValue::Array(array) = canonical else {
                unreachable!("catalog vectors use canonical array storage")
            };
            Ok(SqlValue::String(
                array
                    .elements
                    .iter()
                    .map(|element| match element {
                        PgCanonicalValue::Int2(value) => value.to_string(),
                        PgCanonicalValue::Oid(value) => value.to_string(),
                        _ => unreachable!("catalog vector element type was validated"),
                    })
                    .collect::<Vec<_>>()
                    .join(" "),
            ))
        }
        "name" => {
            let SqlValue::String(value) = text_value_without_nul(value.to_cell(), pg_type)? else {
                unreachable!("text coercion always returns a string")
            };
            Ok(SqlValue::String(clip_postgres_name(value)))
        }
        "text" | "varchar" | "bpchar" | "refcursor" => {
            let text = match &value {
                SqlValue::Json(value) if pg_type == "text" => postgres_jsonb_text(value),
                _ => value.to_cell(),
            };
            text_value_without_nul(text, pg_type)
        }
        "xml" => {
            let text = value.to_cell();
            text_value_without_nul(crate::normalize_xml(&text)?, pg_type)
        }
        "jsonpath" => {
            let text = value.to_cell();
            let text = crate::normalize_jsonpath(&text).map_err(|_| {
                SqlError::InvalidTextRepresentation(format!(
                    "invalid input syntax for type jsonpath: \"{text}\""
                ))
            })?;
            text_value_without_nul(text, pg_type)
        }
        "date" => {
            let text = value.to_cell();
            PgDate::from_postgres_text(&text)
                .map(|date| SqlValue::String(date.to_iso_text()))
                .map_err(|error| postgres_date_input_error(&text, error))
        }
        "time" => {
            let text = value.to_cell();
            PgTime::from_postgres_text(&text)
                .map(|time| SqlValue::String(time.to_iso_text()))
                .map_err(|error| postgres_time_input_error(&text, error))
        }
        "timetz" => {
            let text = value.to_cell();
            PgTimeTz::from_postgres_text(&text, current_timezone_offset_seconds())
                .map(|time| SqlValue::String(time.to_iso_text()))
                .map_err(|error| postgres_timetz_input_error(&text, error))
        }
        "timestamp" => {
            let text = value.to_cell();
            PgTimestamp::from_postgres_text(&text, false)
                .map(|timestamp| SqlValue::String(timestamp.to_iso_text(false)))
                .map_err(|error| postgres_timestamp_input_error(&text, error))
        }
        "timestamptz" => {
            let text = value.to_cell();
            parse_timestamptz(&text)
                .map(|timestamp| SqlValue::String(render_timestamptz(timestamp)))
                .map_err(|error| postgres_timestamptz_input_error(&text, error))
        }
        "uuid" => {
            let text = value.to_cell();
            parse_postgres_uuid(&text)
                .map(format_postgres_uuid)
                .map(SqlValue::String)
                .map_err(|_| SqlError::invalid_text_representation("uuid", format!("\"{text}\"")))
        }
        "tsvector" => PgTsVector::from_postgres_text(&value.to_cell())
            .map(|vector| SqlValue::String(vector.to_postgres_text())),
        "tsquery" => PgTsQuery::from_postgres_text(&value.to_cell()).map(SqlValue::TsQuery),
        other => Err(SqlError::Unsupported(format!(
            "cast to {other} is not supported"
        ))),
    }
}

pub(crate) fn oid_value(value: &SqlValue) -> Result<u32> {
    let value = sql_value_i64(value)
        .ok_or_else(|| SqlError::invalid_text_representation("oid", value.to_cell()))?;
    u32::try_from(value).map_err(|_| SqlError::numeric_value_out_of_range("OID out of range"))
}

pub(crate) fn postgres_time_input_error(value: &str, error: PgCanonicalValueError) -> SqlError {
    if matches!(&error, PgCanonicalValueError::InvalidTimezoneOffset(_)) {
        return SqlError::data_exception(
            "22009",
            format!("time zone displacement out of range: \"{value}\""),
            Some("time".to_string()),
        );
    }
    let message = match &error {
        PgCanonicalValueError::TemporalOverflow(_)
        | PgCanonicalValueError::TemporalFieldOverflow(_)
        | PgCanonicalValueError::InvalidTime(_) => {
            format!("date/time field value out of range: \"{value}\"")
        }
        _ => format!("invalid input syntax for type time: \"{value}\""),
    };
    match error {
        PgCanonicalValueError::TemporalOverflow(_)
        | PgCanonicalValueError::TemporalFieldOverflow(_)
        | PgCanonicalValueError::InvalidTime(_) => {
            SqlError::data_exception("22008", message, Some("time".to_string()))
        }
        _ => SqlError::invalid_datetime_format(message),
    }
}

pub(crate) fn postgres_timetz_input_error(value: &str, error: PgCanonicalValueError) -> SqlError {
    if matches!(&error, PgCanonicalValueError::InvalidTimezoneOffset(_)) {
        return SqlError::data_exception(
            "22009",
            format!("time zone displacement out of range: \"{value}\""),
            Some("timetz".to_string()),
        );
    }
    let message = match &error {
        PgCanonicalValueError::TemporalOverflow(_)
        | PgCanonicalValueError::TemporalFieldOverflow(_)
        | PgCanonicalValueError::InvalidTime(_) => {
            format!("date/time field value out of range: \"{value}\"")
        }
        _ => format!("invalid input syntax for type time with time zone: \"{value}\""),
    };
    match error {
        PgCanonicalValueError::TemporalOverflow(_)
        | PgCanonicalValueError::TemporalFieldOverflow(_)
        | PgCanonicalValueError::InvalidTime(_) => {
            SqlError::data_exception("22008", message, Some("timetz".to_string()))
        }
        _ => SqlError::invalid_datetime_format(message),
    }
}

pub(crate) fn postgres_timestamp_input_error(
    value: &str,
    error: PgCanonicalValueError,
) -> SqlError {
    let message = match &error {
        PgCanonicalValueError::TemporalOverflow(_)
        | PgCanonicalValueError::TemporalFieldOverflow(_)
        | PgCanonicalValueError::InvalidTime(_) => {
            format!("date/time field value out of range: \"{value}\"")
        }
        _ => format!("invalid input syntax for type timestamp: \"{value}\""),
    };
    match error {
        PgCanonicalValueError::TemporalOverflow(_)
        | PgCanonicalValueError::TemporalFieldOverflow(_)
        | PgCanonicalValueError::InvalidTime(_) => {
            SqlError::data_exception("22008", message, Some("timestamp".to_string()))
        }
        _ => SqlError::invalid_datetime_format(message),
    }
}

pub(crate) fn postgres_timestamptz_input_error(
    value: &str,
    error: PgCanonicalValueError,
) -> SqlError {
    if let Some(timezone) = unknown_timestamp_timezone(value) {
        return SqlError::invalid_parameter_value(format!(
            "time zone \"{timezone}\" not recognized"
        ));
    }
    let message = match &error {
        PgCanonicalValueError::TemporalOverflow(_)
        | PgCanonicalValueError::TemporalFieldOverflow(_)
        | PgCanonicalValueError::InvalidTime(_) => {
            format!("date/time field value out of range: \"{value}\"")
        }
        PgCanonicalValueError::InvalidTimezoneOffset(_) => {
            format!("time zone displacement out of range: \"{value}\"")
        }
        _ => format!("invalid input syntax for type timestamp with time zone: \"{value}\""),
    };
    match error {
        PgCanonicalValueError::InvalidTimezoneOffset(_) => {
            SqlError::data_exception("22009", message, Some("timestamptz".to_string()))
        }
        PgCanonicalValueError::TemporalOverflow(_)
        | PgCanonicalValueError::TemporalFieldOverflow(_)
        | PgCanonicalValueError::InvalidTime(_) => {
            SqlError::data_exception("22008", message, Some("timestamptz".to_string()))
        }
        _ => SqlError::invalid_datetime_format(message),
    }
}

pub(crate) fn postgres_interval_input_error(value: &str, error: PgCanonicalValueError) -> SqlError {
    match error {
        PgCanonicalValueError::TemporalOverflow(_)
        | PgCanonicalValueError::TemporalFieldOverflow(_) => SqlError::data_exception(
            "22015",
            format!("interval field value out of range: \"{value}\""),
            Some("interval".to_string()),
        ),
        _ => SqlError::invalid_datetime_format(format!(
            "invalid input syntax for type interval: \"{value}\""
        )),
    }
}

pub(crate) fn unknown_timestamp_timezone(value: &str) -> Option<&str> {
    let (timestamp, candidate) = value.trim().rsplit_once(char::is_whitespace)?;
    if !candidate
        .chars()
        .any(|character| character.is_ascii_alphabetic() || character == '/')
        || validate_timezone_name(candidate)
        || PgTimestamp::from_postgres_text(timestamp.trim_end(), false).is_err()
    {
        return None;
    }
    Some(candidate)
}

pub(crate) fn postgres_date_input_error(value: &str, error: PgCanonicalValueError) -> SqlError {
    let message = match &error {
        PgCanonicalValueError::TemporalOverflow(_) => format!("date out of range: \"{value}\""),
        PgCanonicalValueError::TemporalFieldOverflow(_) => {
            format!("date/time field value out of range: \"{value}\"")
        }
        _ => format!("invalid input syntax for type date: \"{value}\""),
    };
    match error {
        PgCanonicalValueError::TemporalOverflow(_)
        | PgCanonicalValueError::TemporalFieldOverflow(_) => {
            SqlError::data_exception("22008", message, Some("date".to_string()))
        }
        _ => SqlError::invalid_datetime_format(message),
    }
}

pub(crate) fn postgres_range_input_error(
    pg_type: &str,
    value: &str,
    error: PgCanonicalValueError,
) -> SqlError {
    match error {
        PgCanonicalValueError::InvalidRangeBounds => SqlError::data_exception(
            "22000",
            "range lower bound must be less than or equal to range upper bound",
            Some(pg_type.to_string()),
        ),
        PgCanonicalValueError::RangeCanonicalOverflow(kind) => SqlError::data_exception(
            "22003",
            format!("{kind} out of range"),
            Some(pg_type.to_string()),
        ),
        PgCanonicalValueError::MacAddressOctetOutOfRange => SqlError::numeric_value_out_of_range(
            format!("invalid octet value in \"macaddr\" value: \"{value}\""),
        ),
        PgCanonicalValueError::FloatOverflow(value) => SqlError::numeric_value_out_of_range(
            format!("\"{value}\" is out of range for type double precision"),
        ),
        PgCanonicalValueError::NumericOverflow => SqlError::numeric_value_out_of_range(format!(
            "value \"{value}\" is out of range for type {pg_type}"
        )),
        PgCanonicalValueError::TemporalOverflow(_) => SqlError::data_exception(
            "22008",
            format!("date out of range: \"{value}\""),
            Some(pg_type.to_string()),
        ),
        _ => SqlError::invalid_text_representation(
            pg_type,
            format!("invalid input syntax for type {pg_type}: \"{value}\""),
        ),
    }
}

pub(crate) fn clip_postgres_name(mut value: String) -> String {
    const MAX_NAME_BYTES: usize = 63;
    if value.len() <= MAX_NAME_BYTES {
        return value;
    }
    let mut end = MAX_NAME_BYTES;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
    value
}

pub(crate) fn text_value_without_nul(value: String, pg_type: &str) -> Result<SqlValue> {
    if value.contains('\0') {
        return Err(SqlError::data_exception(
            "22021",
            "invalid byte sequence for encoding \"UTF8\": 0x00",
            Some(pg_type.to_string()),
        ));
    }
    Ok(SqlValue::String(value))
}

pub(crate) fn cast_value_to_float(value: SqlValue, pg_type: &str) -> Result<SqlValue> {
    let value = match value {
        SqlValue::Int(value) => value as f64,
        SqlValue::Float(value) => value,
        SqlValue::String(value) => parse_postgres_float_input(&value, pg_type)?,
        other => {
            return Err(SqlError::InvalidSql(format!(
                "cannot cast {} to float",
                other.to_cell()
            )));
        }
    };
    if pg_type == "float4" && value.is_finite() {
        let rounded = value as f32;
        if rounded.is_infinite() || value != 0.0 && rounded == 0.0 {
            return Err(float_input_out_of_range(pg_type, &value.to_string()));
        }
        Ok(SqlValue::Float(f64::from(rounded)))
    } else {
        Ok(SqlValue::Float(value))
    }
}

pub(crate) fn parse_postgres_float_input(value: &str, pg_type: &str) -> Result<f64> {
    let trimmed = value.trim();
    let lowered = trimmed.to_ascii_lowercase();
    let special = match lowered.as_str() {
        "nan" => Some(f64::NAN),
        "infinity" | "+infinity" | "inf" | "+inf" => Some(f64::INFINITY),
        "-infinity" | "-inf" => Some(f64::NEG_INFINITY),
        _ => None,
    };
    if let Some(value) = special {
        return Ok(value);
    }
    let parsed = trimmed.parse::<f64>().map_err(|_| {
        SqlError::invalid_text_representation(
            pg_type,
            format!(
                "invalid input syntax for type {}: \"{trimmed}\"",
                float_type_name(pg_type)
            ),
        )
    })?;
    if parsed.is_infinite() || parsed == 0.0 && float_input_has_nonzero_digit(trimmed) {
        return Err(float_input_out_of_range(pg_type, trimmed));
    }
    if pg_type == "float4" {
        let rounded = parsed as f32;
        if rounded.is_infinite() || parsed != 0.0 && rounded == 0.0 {
            return Err(float_input_out_of_range(pg_type, trimmed));
        }
    }
    Ok(parsed)
}

pub(crate) fn float_input_has_nonzero_digit(value: &str) -> bool {
    value
        .split_once(['e', 'E'])
        .map(|(mantissa, _)| mantissa)
        .unwrap_or(value)
        .bytes()
        .any(|byte| matches!(byte, b'1'..=b'9'))
}

pub(crate) fn float_type_name(pg_type: &str) -> &'static str {
    if pg_type == "float4" {
        "real"
    } else {
        "double precision"
    }
}

pub(crate) fn float_input_out_of_range(pg_type: &str, value: &str) -> SqlError {
    SqlError::DataException {
        sqlstate: "22003",
        message: format!(
            "\"{value}\" is out of range for type {}",
            float_type_name(pg_type)
        ),
        data_type: Some(pg_type.to_string()),
    }
}

pub(crate) fn integer_out_of_range(pg_type: &str) -> SqlError {
    let (data_type, message) = match pg_type {
        "int2" | "smallint" => ("smallint", "smallint out of range"),
        "int4" | "int" | "integer" => ("integer", "integer out of range"),
        _ => ("bigint", "bigint out of range"),
    };
    SqlError::DataException {
        sqlstate: "22003",
        message: message.to_string(),
        data_type: Some(data_type.to_string()),
    }
}

pub(crate) fn enforce_integer_range(value: i64, pg_type: &str) -> Result<i64> {
    let in_range = match pg_type {
        "int2" | "smallint" => i16::try_from(value).is_ok(),
        "int4" | "int" | "integer" => i32::try_from(value).is_ok(),
        "int8" | "bigint" => true,
        _ => return Ok(value),
    };
    if in_range {
        Ok(value)
    } else {
        Err(integer_out_of_range(pg_type))
    }
}

pub(crate) fn validate_integer_result_types(result: SqlResult) -> Result<SqlResult> {
    for (index, pg_type) in result.column_types.iter().enumerate() {
        let Some(pg_type) = pg_type.as_deref() else {
            continue;
        };
        if !matches!(
            pg_type,
            "int2" | "smallint" | "int4" | "int" | "integer" | "int8" | "bigint"
        ) {
            continue;
        }
        for row in &result.rows {
            let Some(SqlValue::Int(value)) = row.get(index) else {
                continue;
            };
            enforce_integer_range(*value, pg_type)?;
        }
    }
    Ok(result)
}

pub(crate) fn enforce_integer_value_type(
    value: SqlValue,
    pg_type: Option<&str>,
) -> Result<SqlValue> {
    if let (SqlValue::Int(value), Some(pg_type)) = (&value, pg_type) {
        enforce_integer_range(*value, pg_type)?;
    }
    Ok(value)
}

pub(crate) fn cast_value_to_array(value: SqlValue, element_type: &str) -> Result<SqlValue> {
    cast_value_to_array_with_delimiter(
        value,
        element_type,
        pg_type_delimiter(element_type).unwrap_or(','),
    )
}

pub(crate) fn cast_value_to_array_with_delimiter(
    value: SqlValue,
    element_type: &str,
    delimiter: char,
) -> Result<SqlValue> {
    let (value, lower_bounds) = match value {
        SqlValue::Json(value @ JsonValue::Array(_)) => (value, None),
        SqlValue::Json(JsonValue::Object(mut object)) => {
            let input = object
                .remove("$bicdb_array_input")
                .and_then(|value| value.as_object().cloned())
                .ok_or_else(|| {
                    SqlError::InvalidTextRepresentation(format!(
                        "cannot cast JSON object to {element_type}[]"
                    ))
                })?;
            let lower_bounds = input
                .get("lower_bounds")
                .cloned()
                .and_then(|value| serde_json::from_value::<Vec<i32>>(value).ok())
                .ok_or_else(|| invalid_array_text("internal array lower bounds"))?;
            let value = input
                .get("value")
                .cloned()
                .filter(JsonValue::is_array)
                .ok_or_else(|| invalid_array_text("internal array value"))?;
            (value, Some(lower_bounds))
        }
        SqlValue::String(value) => {
            let parsed = parse_pg_array_value_with_delimiter(&value, delimiter)?;
            (parsed.value, parsed.lower_bounds)
        }
        other => {
            return Err(SqlError::InvalidTextRepresentation(format!(
                "cannot cast {} to {element_type}[]",
                other.to_cell()
            )));
        }
    };
    let casted = cast_array_json(value, element_type)?;
    if let Some(lower_bounds) = lower_bounds {
        return Ok(SqlValue::Json(serde_json::json!({
            "$bicdb_array_input": {
                "lower_bounds": lower_bounds,
                "value": casted,
            }
        })));
    }
    Ok(SqlValue::Json(casted))
}

pub(crate) fn parse_array_literal(value: &str) -> Result<Vec<JsonValue>> {
    match parse_pg_array_value(value)?.value {
        JsonValue::Array(values) => Ok(values),
        _ => unreachable!("array parser always returns an array"),
    }
}

pub(crate) struct ParsedPgArray {
    pub(crate) value: JsonValue,
    pub(crate) lower_bounds: Option<Vec<i32>>,
}

pub(crate) fn parse_pg_array_value(value: &str) -> Result<ParsedPgArray> {
    parse_pg_array_value_with_delimiter(value, ',')
}

pub(crate) fn parse_pg_array_value_with_delimiter(
    value: &str,
    delimiter: char,
) -> Result<ParsedPgArray> {
    if !delimiter.is_ascii() || matches!(delimiter, '{' | '}' | '"' | '\\') {
        return Err(SqlError::InvalidSql(
            "array delimiter must be a safe single-byte character".to_string(),
        ));
    }
    let trimmed = value.trim();
    if trimmed.starts_with('[') && !trimmed.contains('=') {
        let parsed: JsonValue = serde_json::from_str(trimmed).map_err(|error| {
            SqlError::InvalidTextRepresentation(format!(
                "invalid input syntax for array: \"{value}\": {error}"
            ))
        })?;
        return match parsed {
            value @ JsonValue::Array(_) => Ok(ParsedPgArray {
                value,
                lower_bounds: None,
            }),
            _ => Err(SqlError::InvalidTextRepresentation(format!(
                "invalid input syntax for array: \"{value}\""
            ))),
        };
    }
    let (literal, lower_bounds, declared_lengths) = parse_array_dimensions(trimmed, value)?;
    let mut parser = PgArrayLiteralParser::new(literal, value, delimiter as u8);
    let parsed = parser.parse_array()?;
    parser.finish()?;
    let actual = array_json_dimensions(&parsed).ok_or_else(|| invalid_array_text(value))?;
    if actual.len() > 6 {
        return Err(SqlError::data_exception(
            "54000",
            "number of array dimensions exceeds the maximum allowed (6)",
            None,
        ));
    }
    if let Some(declared_lengths) = declared_lengths {
        if actual != declared_lengths {
            return Err(SqlError::InvalidTextRepresentation(format!(
                "array dimensions {:?} do not match array contents {:?}",
                declared_lengths, actual
            )));
        }
    }
    Ok(ParsedPgArray {
        value: parsed,
        lower_bounds,
    })
}

pub(crate) fn cast_array_json(value: JsonValue, element_type: &str) -> Result<JsonValue> {
    if element_type == "vector" {
        return cast_vector_array_json(value);
    }
    match value {
        JsonValue::Array(values) => values
            .into_iter()
            .map(|value| cast_array_json(value, element_type))
            .collect::<Result<Vec<_>>>()
            .map(JsonValue::Array),
        JsonValue::Null => Ok(JsonValue::Null),
        value => {
            cast_value_to_pg_type(json_to_sql_value(&value), element_type).map(sql_value_to_json)
        }
    }
}

pub(crate) fn cast_vector_array_json(value: JsonValue) -> Result<JsonValue> {
    match value {
        JsonValue::Array(values)
            if !values.is_empty() && values.iter().all(JsonValue::is_number) =>
        {
            let vector = sql_value_to_vector(&SqlValue::Json(JsonValue::Array(values)))?;
            Ok(JsonValue::String(vector_json(&vector).to_string()))
        }
        JsonValue::Array(values) => values
            .into_iter()
            .map(cast_vector_array_json)
            .collect::<Result<Vec<_>>>()
            .map(JsonValue::Array),
        JsonValue::Null => Ok(JsonValue::Null),
        value => {
            let vector = sql_value_to_vector(&json_to_sql_value(&value))?;
            Ok(JsonValue::String(vector_json(&vector).to_string()))
        }
    }
}

pub(crate) fn parse_array_dimensions<'a>(
    mut value: &'a str,
    original: &str,
) -> Result<(&'a str, Option<Vec<i32>>, Option<Vec<usize>>)> {
    if !value.starts_with('[') {
        return Ok((value, None, None));
    }
    let mut lower_bounds = Vec::new();
    let mut lengths = Vec::new();
    while value.starts_with('[') {
        let end = value
            .find(']')
            .ok_or_else(|| invalid_array_text(original))?;
        let dimension = &value[1..end];
        let (lower, upper) = dimension
            .split_once(':')
            .ok_or_else(|| invalid_array_text(original))?;
        let lower = lower
            .parse::<i32>()
            .map_err(|_| invalid_array_text(original))?;
        let upper = upper
            .parse::<i32>()
            .map_err(|_| invalid_array_text(original))?;
        if upper < lower {
            return Err(SqlError::data_exception(
                "2202E",
                "upper bound cannot be less than lower bound",
                None,
            ));
        }
        if upper == i32::MAX {
            return Err(SqlError::data_exception(
                "54000",
                format!("array upper bound is too large: {upper}"),
                None,
            ));
        }
        let length = usize::try_from(i64::from(upper) - i64::from(lower) + 1)
            .map_err(|_| invalid_array_text(original))?;
        lower_bounds.push(lower);
        lengths.push(length);
        value = &value[end + 1..];
    }
    value = value
        .strip_prefix('=')
        .ok_or_else(|| invalid_array_text(original))?;
    Ok((value, Some(lower_bounds), Some(lengths)))
}

pub(crate) fn array_json_dimensions(value: &JsonValue) -> Option<Vec<usize>> {
    let JsonValue::Array(values) = value else {
        return Some(Vec::new());
    };
    if values.is_empty() {
        return Some(Vec::new());
    }
    let child = array_json_dimensions(&values[0])?;
    if values
        .iter()
        .skip(1)
        .any(|value| array_json_dimensions(value).as_ref() != Some(&child))
    {
        return None;
    }
    let mut dimensions = Vec::with_capacity(child.len() + 1);
    dimensions.push(values.len());
    dimensions.extend(child);
    Some(dimensions)
}

pub(crate) fn invalid_array_text(value: &str) -> SqlError {
    SqlError::InvalidTextRepresentation(format!("invalid input syntax for array: \"{value}\""))
}

pub(crate) struct PgArrayLiteralParser<'a> {
    value: &'a str,
    original: &'a str,
    position: usize,
    delimiter: u8,
}

impl<'a> PgArrayLiteralParser<'a> {
    pub(crate) fn new(value: &'a str, original: &'a str, delimiter: u8) -> Self {
        Self {
            value,
            original,
            position: 0,
            delimiter,
        }
    }

    pub(crate) fn parse_array(&mut self) -> Result<JsonValue> {
        if self.next() != Some(b'{') {
            return Err(invalid_array_text(self.original));
        }
        let mut values = Vec::new();
        self.skip_whitespace();
        if self.peek() == Some(b'}') {
            self.position += 1;
            return Ok(JsonValue::Array(values));
        }
        loop {
            self.skip_whitespace();
            let value = if self.peek() == Some(b'{') {
                self.parse_array()?
            } else {
                self.parse_element()?
            };
            values.push(value);
            self.skip_whitespace();
            match self.next() {
                Some(delimiter) if delimiter == self.delimiter => {}
                Some(b'}') => break,
                _ => return Err(invalid_array_text(self.original)),
            }
        }
        Ok(JsonValue::Array(values))
    }

    pub(crate) fn parse_element(&mut self) -> Result<JsonValue> {
        if self.peek() == Some(b'"') {
            self.position += 1;
            let mut output = Vec::new();
            loop {
                match self.next() {
                    Some(b'"') => {
                        return String::from_utf8(output)
                            .map(JsonValue::String)
                            .map_err(|_| invalid_array_text(self.original));
                    }
                    Some(b'\\') => output.push(
                        self.next()
                            .ok_or_else(|| invalid_array_text(self.original))?,
                    ),
                    Some(byte) => output.push(byte),
                    None => return Err(invalid_array_text(self.original)),
                }
            }
        }
        let start = self.position;
        while self
            .peek()
            .is_some_and(|byte| byte != self.delimiter && byte != b'}')
        {
            self.position += 1;
        }
        let token = self.value[start..self.position].trim();
        if token.is_empty() {
            return Err(invalid_array_text(self.original));
        }
        if token.eq_ignore_ascii_case("null") {
            Ok(JsonValue::Null)
        } else {
            Ok(JsonValue::String(token.to_string()))
        }
    }

    pub(crate) fn finish(&self) -> Result<()> {
        if self.position == self.value.len() {
            Ok(())
        } else {
            Err(invalid_array_text(self.original))
        }
    }

    pub(crate) fn peek(&self) -> Option<u8> {
        self.value.as_bytes().get(self.position).copied()
    }

    pub(crate) fn next(&mut self) -> Option<u8> {
        let byte = self.peek()?;
        self.position += 1;
        Some(byte)
    }

    pub(crate) fn skip_whitespace(&mut self) {
        while self.peek().is_some_and(|byte| byte.is_ascii_whitespace()) {
            self.position += 1;
        }
    }
}

pub(crate) fn is_valid_numeric_text(value: &str) -> bool {
    matches!(
        PgNumeric::from_postgres_text(value),
        Ok(PgNumeric::Finite { .. })
    )
}

pub(crate) fn select_expr_column_name(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::CompoundFieldAccess { access_chain, .. } => access_chain
            .last()
            .and_then(|access| match access {
                AccessExpr::Dot(Expr::Identifier(ident)) => Some(ident.value.clone()),
                _ => None,
            })
            .unwrap_or_else(|| "?column?".to_string()),
        Expr::TypedString(value) => typed_literal_column_name(&value.data_type),
        Expr::Array(_) => "array".to_string(),
        Expr::Function(function)
            if object_name(&function.name).is_ok_and(|name| {
                name.eq_ignore_ascii_case("bicdb_is_json")
                    || name
                        .to_ascii_lowercase()
                        .starts_with("bicdb_json_array_query_")
            }) =>
        {
            if object_name(&function.name).is_ok_and(|name| {
                name.to_ascii_lowercase()
                    .starts_with("bicdb_json_array_query_")
            }) {
                "json_array".to_string()
            } else {
                "?column?".to_string()
            }
        }
        Expr::Function(function) => object_name(&function.name)
            .map(|name| postgres_xml_function_column_name(&name))
            .unwrap_or_else(|_| "?column?".to_string()),
        Expr::Cast {
            expr, data_type, ..
        } => {
            let inner = select_expr_column_name(expr);
            if inner == "?column?" || cast_originates_from_literal(expr) {
                typed_literal_column_name(data_type)
            } else {
                inner
            }
        }
        _ => "?column?".to_string(),
    }
}

pub(crate) fn row_expr_column_name(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::CompoundIdentifier(idents) => idents
            .last()
            .map(|ident| ident.value.clone())
            .unwrap_or_else(|| "?column?".to_string()),
        Expr::CompoundFieldAccess { access_chain, .. } => access_chain
            .last()
            .and_then(|access| match access {
                AccessExpr::Dot(Expr::Identifier(ident)) => Some(ident.value.clone()),
                _ => None,
            })
            .unwrap_or_else(|| "?column?".to_string()),
        Expr::Function(function)
            if object_name(&function.name).is_ok_and(|name| {
                name.eq_ignore_ascii_case("bicdb_is_json")
                    || name
                        .to_ascii_lowercase()
                        .starts_with("bicdb_json_array_query_")
            }) =>
        {
            if object_name(&function.name).is_ok_and(|name| {
                name.to_ascii_lowercase()
                    .starts_with("bicdb_json_array_query_")
            }) {
                "json_array".to_string()
            } else {
                "?column?".to_string()
            }
        }
        Expr::TypedString(value) => typed_literal_column_name(&value.data_type),
        Expr::Array(_) => "array".to_string(),
        Expr::Function(function) => object_name(&function.name)
            .map(|name| postgres_xml_function_column_name(&name))
            .unwrap_or_else(|_| "?column?".to_string()),
        Expr::Cast {
            expr, data_type, ..
        } => {
            let inner = row_expr_column_name(expr);
            if inner == "?column?" || cast_originates_from_literal(expr) {
                typed_literal_column_name(data_type)
            } else {
                inner
            }
        }
        Expr::Nested(expr) => row_expr_column_name(expr),
        other => other.to_string(),
    }
}

pub(crate) fn cast_originates_from_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Value(_) | Expr::TypedString(_) => true,
        Expr::Cast { expr, .. } | Expr::Nested(expr) | Expr::UnaryOp { expr, .. } => {
            cast_originates_from_literal(expr)
        }
        _ => false,
    }
}

pub(crate) fn typed_literal_column_name(data_type: &DataType) -> String {
    pg_type_from_data_type(data_type)
        .map(|(pg_type, _)| pg_type.trim_end_matches("[]").to_string())
        .unwrap_or_else(|_| {
            data_type
                .to_string()
                .trim_end_matches("[]")
                .rsplit('.')
                .next()
                .unwrap_or("?column?")
                .trim_matches('"')
                .to_string()
        })
}

pub(crate) fn postgres_xml_function_column_name(name: &str) -> String {
    match name
        .rsplit('.')
        .next()
        .unwrap_or(name)
        .to_ascii_lowercase()
        .as_str()
    {
        "bicdb_xmlparse_content" | "bicdb_xmlparse_document" => "xmlparse".to_string(),
        "bicdb_xmlserialize_content" | "bicdb_xmlserialize_document" => "xmlserialize".to_string(),
        "bicdb_xmlexists" => "xmlexists".to_string(),
        "bicdb_xmlelement" => "xmlelement".to_string(),
        "bicdb_xmlpi" => "xmlpi".to_string(),
        "bicdb_xmlroot" => "xmlroot".to_string(),
        other => other.to_string(),
    }
}

pub(crate) fn sql_value_to_json_ref(value: &SqlValue) -> JsonValue {
    match value {
        SqlValue::Null => JsonValue::Null,
        SqlValue::Bool(value) => JsonValue::Bool(*value),
        SqlValue::Int(value) => JsonValue::from(*value),
        SqlValue::Float(value) => JsonValue::from(*value),
        SqlValue::String(value) => {
            serde_json::from_str(value).unwrap_or_else(|_| JsonValue::String(value.clone()))
        }
        SqlValue::TsQuery(value) => JsonValue::String(value.to_postgres_text()),
        SqlValue::JsonText(value) => value.parsed().clone(),
        SqlValue::Json(value) => value.clone(),
        SqlValue::Geometry(value) => value.to_geojson_value(),
        SqlValue::Composite(value) => JsonValue::Object(
            value
                .fields
                .iter()
                .map(|field| (field.name.clone(), sql_value_to_json_ref(&field.value)))
                .collect(),
        ),
    }
}

pub(crate) fn json_contains(left: &JsonValue, right: &JsonValue) -> Result<bool> {
    Ok(match (left, right) {
        (JsonValue::Object(left), JsonValue::Object(right)) => right.iter().all(|(key, value)| {
            left.get(key)
                .is_some_and(|left_value| json_contains_nested(left_value, value))
        }),
        (JsonValue::Array(left), JsonValue::Array(right)) => right.iter().all(|right_value| {
            left.iter().any(|left_value| match right_value {
                JsonValue::Array(_) | JsonValue::Object(_) => {
                    json_contains(left_value, right_value).unwrap_or(false)
                }
                _ => json_values_equal(left_value, right_value),
            })
        }),
        // PostgreSQL's one structural exception: an array can contain a
        // primitive scalar, but only when that scalar is a direct element.
        (JsonValue::Array(left), right) if !right.is_array() && !right.is_object() => left
            .iter()
            .any(|left_value| json_values_equal(left_value, right)),
        _ => json_values_equal(left, right),
    })
}

pub(crate) fn json_contains_nested(left: &JsonValue, right: &JsonValue) -> bool {
    match (left, right) {
        (JsonValue::Object(_), JsonValue::Object(_))
        | (JsonValue::Array(_), JsonValue::Array(_)) => json_contains(left, right).unwrap_or(false),
        _ => json_values_equal(left, right),
    }
}

pub(crate) fn json_values_equal(left: &JsonValue, right: &JsonValue) -> bool {
    match (left, right) {
        (JsonValue::Number(left), JsonValue::Number(right)) => {
            canonical_json_number(left) == canonical_json_number(right)
        }
        (JsonValue::Array(left), JsonValue::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| json_values_equal(left, right))
        }
        (JsonValue::Object(left), JsonValue::Object(right)) => {
            left.len() == right.len()
                && left.iter().all(|(key, left)| {
                    right
                        .get(key)
                        .is_some_and(|right| json_values_equal(left, right))
                })
        }
        _ => left == right,
    }
}

pub(crate) fn jsonb_value_ordering(left: &JsonValue, right: &JsonValue) -> Ordering {
    pub(crate) fn scalar_rank(value: &JsonValue) -> Option<u8> {
        match value {
            JsonValue::Null => Some(0),
            JsonValue::String(_) => Some(1),
            JsonValue::Number(_) => Some(2),
            JsonValue::Bool(_) => Some(3),
            JsonValue::Array(_) | JsonValue::Object(_) => None,
        }
    }

    pub(crate) fn number_ordering(
        left: &serde_json::Number,
        right: &serde_json::Number,
    ) -> Ordering {
        let (left_negative, left_digits, left_exponent) = canonical_json_number(left);
        let (right_negative, right_digits, right_exponent) = canonical_json_number(right);
        if left_digits == "0" || right_digits == "0" {
            return match (left_digits == "0", right_digits == "0") {
                (true, true) => Ordering::Equal,
                (true, false) => {
                    if right_negative {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
                (false, true) => {
                    if left_negative {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                (false, false) => unreachable!(),
            };
        }
        if left_negative != right_negative {
            return if left_negative {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }

        let left_order = SignedDecimalInteger::parse(&left_exponent).add(
            SignedDecimalInteger::from_offset(false, left_digits.len().saturating_sub(1)),
        );
        let right_order = SignedDecimalInteger::parse(&right_exponent).add(
            SignedDecimalInteger::from_offset(false, right_digits.len().saturating_sub(1)),
        );
        let magnitude = left_order.cmp(&right_order).then_with(|| {
            let width = left_digits.len().max(right_digits.len());
            left_digits
                .bytes()
                .chain(std::iter::repeat(b'0'))
                .take(width)
                .cmp(
                    right_digits
                        .bytes()
                        .chain(std::iter::repeat(b'0'))
                        .take(width),
                )
        });
        if left_negative {
            magnitude.reverse()
        } else {
            magnitude
        }
    }

    pub(crate) fn object_entries(
        value: &serde_json::Map<String, JsonValue>,
    ) -> Vec<(&str, &JsonValue)> {
        let mut entries = value
            .iter()
            .map(|(key, value)| (key.as_str(), value))
            .collect::<Vec<_>>();
        entries.sort_unstable_by(|(left, _), (right, _)| {
            left.len()
                .cmp(&right.len())
                .then_with(|| left.as_bytes().cmp(right.as_bytes()))
        });
        entries
    }

    match (left, right) {
        (JsonValue::Array(left), JsonValue::Array(right)) => {
            left.len().cmp(&right.len()).then_with(|| {
                left.iter()
                    .zip(right)
                    .map(|(left, right)| jsonb_value_ordering(left, right))
                    .find(|ordering| *ordering != Ordering::Equal)
                    .unwrap_or(Ordering::Equal)
            })
        }
        (JsonValue::Object(left), JsonValue::Object(right)) => {
            left.len().cmp(&right.len()).then_with(|| {
                object_entries(left)
                    .into_iter()
                    .zip(object_entries(right))
                    .find_map(|((left_key, left_value), (right_key, right_value))| {
                        let ordering = left_key
                            .as_bytes()
                            .cmp(right_key.as_bytes())
                            .then_with(|| jsonb_value_ordering(left_value, right_value));
                        (ordering != Ordering::Equal).then_some(ordering)
                    })
                    .unwrap_or(Ordering::Equal)
            })
        }
        (JsonValue::Object(_), _) => Ordering::Greater,
        (_, JsonValue::Object(_)) => Ordering::Less,
        (JsonValue::Array(left), _) => {
            if left.is_empty() {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (_, JsonValue::Array(right)) => {
            if right.is_empty() {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (JsonValue::String(left), JsonValue::String(right)) => {
            left.as_bytes().cmp(right.as_bytes())
        }
        (JsonValue::Number(left), JsonValue::Number(right)) => number_ordering(left, right),
        (JsonValue::Bool(left), JsonValue::Bool(right)) => left.cmp(right),
        (JsonValue::Null, JsonValue::Null) => Ordering::Equal,
        (left, right) => scalar_rank(left).cmp(&scalar_rank(right)),
    }
}

pub(crate) fn canonical_json_value_key(value: &JsonValue) -> String {
    pub(crate) fn append(value: &JsonValue, output: &mut String) {
        match value {
            JsonValue::Null => output.push('n'),
            JsonValue::Bool(value) => output.push_str(if *value { "b1" } else { "b0" }),
            JsonValue::Number(value) => {
                let (negative, digits, exponent) = canonical_json_number(value);
                output.push('d');
                output.push(if negative { '-' } else { '+' });
                output.push_str(&digits.len().to_string());
                output.push(':');
                output.push_str(&digits);
                output.push(':');
                output.push_str(&exponent.to_string());
            }
            JsonValue::String(value) => {
                output.push('s');
                output.push_str(&value.len().to_string());
                output.push(':');
                output.push_str(value);
            }
            JsonValue::Array(values) => {
                output.push('a');
                output.push_str(&values.len().to_string());
                output.push('[');
                for value in values {
                    append(value, output);
                }
                output.push(']');
            }
            JsonValue::Object(values) => {
                output.push('o');
                output.push_str(&values.len().to_string());
                output.push('{');
                let mut entries = values.iter().collect::<Vec<_>>();
                entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
                for (key, value) in entries {
                    output.push_str(&key.len().to_string());
                    output.push(':');
                    output.push_str(key);
                    append(value, output);
                }
                output.push('}');
            }
        }
    }

    let mut output = String::new();
    append(value, &mut output);
    output
}

/// A deterministic, valid JSON representation for JSONB values used in
/// reversible storage identities. Numeric spellings that PostgreSQL JSONB
/// considers equal produce the same text, including below nested containers.
pub(crate) fn canonical_jsonb_identity_text(value: &JsonValue) -> String {
    pub(crate) fn append(value: &JsonValue, output: &mut String) {
        match value {
            JsonValue::Null => output.push_str("null"),
            JsonValue::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
            JsonValue::Number(value) => {
                let (negative, digits, exponent) = canonical_json_number(value);
                if negative {
                    output.push('-');
                }
                output.push_str(&digits);
                if exponent != "0" {
                    output.push('e');
                    output.push_str(&exponent);
                }
            }
            JsonValue::String(value) => output.push_str(
                &serde_json::to_string(value).expect("serializing a JSON string cannot fail"),
            ),
            JsonValue::Array(values) => {
                output.push('[');
                for (index, value) in values.iter().enumerate() {
                    if index != 0 {
                        output.push(',');
                    }
                    append(value, output);
                }
                output.push(']');
            }
            JsonValue::Object(values) => {
                let mut entries = values.iter().collect::<Vec<_>>();
                entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
                output.push('{');
                for (index, (key, value)) in entries.into_iter().enumerate() {
                    if index != 0 {
                        output.push(',');
                    }
                    output.push_str(
                        &serde_json::to_string(key)
                            .expect("serializing a JSON object key cannot fail"),
                    );
                    output.push(':');
                    append(value, output);
                }
                output.push('}');
            }
        }
    }

    let mut output = String::new();
    append(value, &mut output);
    output
}

pub(crate) const JSONB_NUMERIC_MAX_INTEGER_DIGITS: i64 = 131_072;
pub(crate) const JSONB_NUMERIC_MAX_FRACTIONAL_DIGITS: i64 = 16_383;
// PostgreSQL's numeric parser rejects exponents outside PG_INT32_MAX / 2
// before inspecting whether the mantissa is zero.
pub(crate) const JSONB_NUMERIC_MAX_EXPONENT: i64 = 1_073_741_823;

pub(crate) fn validate_jsonb_numeric_range(value: &JsonValue) -> Result<()> {
    match value {
        JsonValue::Number(number) => validate_jsonb_number(number),
        JsonValue::Array(values) => values.iter().try_for_each(validate_jsonb_numeric_range),
        JsonValue::Object(values) => values.values().try_for_each(validate_jsonb_numeric_range),
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::String(_) => Ok(()),
    }
}

pub(crate) fn validate_jsonb_number(number: &serde_json::Number) -> Result<()> {
    let rendered = number.to_string();
    let unsigned = rendered.strip_prefix('-').unwrap_or(&rendered);
    let (mantissa, exponent) = unsigned
        .split_once(['e', 'E'])
        .map(|(mantissa, exponent)| (mantissa, parse_jsonb_exponent(exponent)))
        .unwrap_or((unsigned, Ok(0)));
    let exponent = exponent?;
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let whole_len = i64::try_from(whole.len()).map_err(|_| jsonb_numeric_range_error())?;
    let fraction_len = i64::try_from(fraction.len()).map_err(|_| jsonb_numeric_range_error())?;

    let display_scale = fraction_len
        .checked_sub(exponent)
        .ok_or_else(jsonb_numeric_range_error)?
        .max(0);
    if display_scale > JSONB_NUMERIC_MAX_FRACTIONAL_DIGITS {
        return Err(jsonb_numeric_range_error());
    }

    let first_nonzero = whole
        .bytes()
        .chain(fraction.bytes())
        .position(|digit| digit != b'0');
    if let Some(first_nonzero) = first_nonzero {
        let decimal_position = whole_len
            .checked_add(exponent)
            .ok_or_else(jsonb_numeric_range_error)?;
        let first_nonzero =
            i64::try_from(first_nonzero).map_err(|_| jsonb_numeric_range_error())?;
        let integer_digits = decimal_position
            .checked_sub(first_nonzero)
            .ok_or_else(jsonb_numeric_range_error)?
            .max(0);
        if integer_digits > JSONB_NUMERIC_MAX_INTEGER_DIGITS {
            return Err(jsonb_numeric_range_error());
        }
    }

    Ok(())
}

pub(crate) fn parse_jsonb_exponent(exponent: &str) -> Result<i64> {
    let (negative, digits) = exponent
        .strip_prefix('-')
        .map(|digits| (true, digits))
        .or_else(|| exponent.strip_prefix('+').map(|digits| (false, digits)))
        .unwrap_or((false, exponent));
    let mut magnitude = 0_i64;
    for digit in digits.bytes() {
        let digit = i64::from(digit - b'0');
        magnitude = magnitude
            .checked_mul(10)
            .and_then(|value| value.checked_add(digit))
            .filter(|value| *value <= JSONB_NUMERIC_MAX_EXPONENT)
            .ok_or_else(jsonb_numeric_range_error)?;
    }
    Ok(if negative { -magnitude } else { magnitude })
}

pub(crate) fn jsonb_numeric_range_error() -> SqlError {
    SqlError::ConstraintViolation {
        sqlstate: "22003",
        message: "value overflows numeric format".to_string(),
        table: None,
        column: None,
        constraint: None,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SignedDecimalInteger {
    negative: bool,
    magnitude: String,
}

impl SignedDecimalInteger {
    pub(crate) fn parse(value: &str) -> Self {
        let (negative, magnitude) = value
            .strip_prefix('-')
            .map(|magnitude| (true, magnitude))
            .or_else(|| value.strip_prefix('+').map(|magnitude| (false, magnitude)))
            .unwrap_or((false, value));
        Self::normalized(negative, magnitude.to_string())
    }

    pub(crate) fn from_offset(negative: bool, magnitude: usize) -> Self {
        Self::normalized(negative, magnitude.to_string())
    }

    pub(crate) fn normalized(negative: bool, magnitude: String) -> Self {
        let magnitude = magnitude.trim_start_matches('0');
        if magnitude.is_empty() {
            Self {
                negative: false,
                magnitude: "0".to_string(),
            }
        } else {
            Self {
                negative,
                magnitude: magnitude.to_string(),
            }
        }
    }

    pub(crate) fn add(self, other: Self) -> Self {
        if self.negative == other.negative {
            return Self::normalized(
                self.negative,
                add_decimal_magnitudes(&self.magnitude, &other.magnitude),
            );
        }
        match compare_decimal_magnitudes(&self.magnitude, &other.magnitude) {
            Ordering::Greater => Self::normalized(
                self.negative,
                subtract_decimal_magnitudes(&self.magnitude, &other.magnitude),
            ),
            Ordering::Less => Self::normalized(
                other.negative,
                subtract_decimal_magnitudes(&other.magnitude, &self.magnitude),
            ),
            Ordering::Equal => Self::normalized(false, "0".to_string()),
        }
    }

    pub(crate) fn cmp(&self, other: &Self) -> Ordering {
        match (self.negative, other.negative) {
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => compare_decimal_magnitudes(&self.magnitude, &other.magnitude),
            (true, true) => compare_decimal_magnitudes(&self.magnitude, &other.magnitude).reverse(),
        }
    }

    pub(crate) fn render(self) -> String {
        if self.negative {
            format!("-{}", self.magnitude)
        } else {
            self.magnitude
        }
    }
}

pub(crate) fn compare_decimal_magnitudes(left: &str, right: &str) -> Ordering {
    left.len()
        .cmp(&right.len())
        .then_with(|| left.as_bytes().cmp(right.as_bytes()))
}

pub(crate) fn add_decimal_magnitudes(left: &str, right: &str) -> String {
    let mut left = left.bytes().rev();
    let mut right = right.bytes().rev();
    let mut output = Vec::with_capacity(left.len().max(right.len()).saturating_add(1));
    let mut carry = 0_u8;
    loop {
        let left = left.next().map(|digit| digit - b'0');
        let right = right.next().map(|digit| digit - b'0');
        if left.is_none() && right.is_none() {
            break;
        }
        let sum = left.unwrap_or(0) + right.unwrap_or(0) + carry;
        output.push(b'0' + sum % 10);
        carry = sum / 10;
    }
    if carry != 0 {
        output.push(b'0' + carry);
    }
    output.reverse();
    String::from_utf8(output).expect("decimal arithmetic emits ASCII digits")
}

pub(crate) fn subtract_decimal_magnitudes(larger: &str, smaller: &str) -> String {
    debug_assert!(compare_decimal_magnitudes(larger, smaller) != Ordering::Less);
    let mut smaller = smaller.bytes().rev();
    let mut borrow = 0_i8;
    let mut output = Vec::with_capacity(larger.len());
    for digit in larger.bytes().rev() {
        let mut digit = (digit - b'0') as i8 - borrow;
        let subtract = smaller
            .next()
            .map(|digit| (digit - b'0') as i8)
            .unwrap_or(0);
        if digit < subtract {
            digit += 10;
            borrow = 1;
        } else {
            borrow = 0;
        }
        output.push(b'0' + (digit - subtract) as u8);
    }
    while output.len() > 1 && output.last() == Some(&b'0') {
        output.pop();
    }
    output.reverse();
    String::from_utf8(output).expect("decimal arithmetic emits ASCII digits")
}

pub(crate) fn canonical_json_number(number: &serde_json::Number) -> (bool, String, String) {
    let rendered = number.to_string();
    let (mantissa, exponent) = rendered.split_once(['e', 'E']).unwrap_or((&rendered, "0"));
    let negative = mantissa.starts_with('-');
    let unsigned = mantissa.strip_prefix(['+', '-']).unwrap_or(mantissa);
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let mut digits = format!("{whole}{fraction}")
        .trim_start_matches('0')
        .to_string();
    if digits.is_empty() {
        return (false, "0".to_string(), "0".to_string());
    }
    let trailing_zeros = digits
        .bytes()
        .rev()
        .take_while(|digit| *digit == b'0')
        .count();
    digits.truncate(digits.len() - trailing_zeros);
    let exponent_offset = if trailing_zeros >= fraction.len() {
        SignedDecimalInteger::from_offset(false, trailing_zeros - fraction.len())
    } else {
        SignedDecimalInteger::from_offset(true, fraction.len() - trailing_zeros)
    };
    let exponent = SignedDecimalInteger::parse(exponent)
        .add(exponent_offset)
        .render();
    (negative, digits, exponent)
}

#[cfg(test)]
mod jsonb_numeric_tests {
    use super::*;

    pub(crate) fn number(value: &str) -> serde_json::Number {
        serde_json::from_str::<JsonValue>(value)
            .unwrap()
            .as_number()
            .unwrap()
            .clone()
    }

    #[test]
    pub(crate) fn canonical_number_keeps_oversized_exponents_distinct() {
        let huge = number("1e999999999999999999999999");
        let ordinary = number("1");
        assert_ne!(
            canonical_json_number(&huge),
            canonical_json_number(&ordinary)
        );
    }

    #[test]
    pub(crate) fn canonical_number_adjusts_arbitrary_signed_exponents_without_overflow() {
        assert_eq!(
            canonical_json_number(&number("1.20e999999999999999999999999")),
            canonical_json_number(&number("12e999999999999999999999998"))
        );
        assert_eq!(
            canonical_json_number(&number("1e-999999999999999999999999")),
            canonical_json_number(&number("10e-1000000000000000000000000"))
        );
    }
}

pub(crate) fn sql_json_path<'a>(value: &'a SqlValue, path: &[String]) -> Option<&'a JsonValue> {
    match value {
        SqlValue::JsonText(value) => json_path(value.parsed(), path),
        SqlValue::Json(value) => json_path(value, path),
        _ => None,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum JsonPathElement {
    Key(String),
    Index(i64),
    Text(String),
}

impl JsonPathElement {
    pub(crate) fn label(&self) -> String {
        match self {
            Self::Key(key) => key.clone(),
            Self::Index(index) => index.to_string(),
            Self::Text(value) => value.clone(),
        }
    }
}

pub(crate) fn json_operator_path(expr: &Expr) -> Result<Vec<JsonPathElement>> {
    json_operator_path_value(eval_constant_expr(expr)?)
}

pub(crate) fn json_operator_path_value(value: SqlValue) -> Result<Vec<JsonPathElement>> {
    match value {
        SqlValue::String(value) => Ok(vec![JsonPathElement::Key(value)]),
        SqlValue::Int(value) => Ok(vec![JsonPathElement::Index(value)]),
        other => Err(SqlError::Unsupported(format!(
            "unsupported JSON path expression {}",
            other.to_cell()
        ))),
    }
}

pub(crate) fn json_access_path(path: &sqlparser::ast::JsonPath) -> Result<Vec<JsonPathElement>> {
    path.path
        .iter()
        .map(|part| match part {
            sqlparser::ast::JsonPathElem::Dot { key, .. } => Ok(JsonPathElement::Key(key.clone())),
            sqlparser::ast::JsonPathElem::Bracket { key }
            | sqlparser::ast::JsonPathElem::ColonBracket { key } => {
                let mut elements = json_operator_path(key)?;
                elements
                    .pop()
                    .ok_or_else(|| SqlError::Unsupported("empty JSON path expression".to_string()))
            }
        })
        .collect()
}

pub(crate) fn sql_json_lookup_path<'a>(
    value: &'a SqlValue,
    path: &[JsonPathElement],
) -> Option<&'a JsonValue> {
    match value {
        SqlValue::JsonText(value) => json_lookup_path(value.parsed(), path),
        SqlValue::Json(value) => json_lookup_path(value, path),
        _ => None,
    }
}

pub(crate) fn json_lookup_path<'a>(
    value: &'a JsonValue,
    path: &[JsonPathElement],
) -> Option<&'a JsonValue> {
    let mut current = value;
    for element in path {
        current = match element {
            JsonPathElement::Key(key) => current.as_object()?.get(key)?,
            JsonPathElement::Index(index) => {
                let values = current.as_array()?;
                values.get(json_array_index(values.len(), *index)?)?
            }
            JsonPathElement::Text(value) => match current {
                JsonValue::Object(values) => values.get(value)?,
                JsonValue::Array(values) => {
                    let index = value.parse::<i64>().ok()?;
                    values.get(json_array_index(values.len(), index)?)?
                }
                _ => return None,
            },
        };
    }
    Some(current)
}

pub(crate) fn json_array_index(len: usize, index: i64) -> Option<usize> {
    if index >= 0 {
        usize::try_from(index).ok().filter(|index| *index < len)
    } else {
        i64::try_from(len)
            .ok()?
            .checked_add(index)
            .and_then(|index| usize::try_from(index).ok())
            .filter(|index| *index < len)
    }
}

pub(crate) fn json_extract_path_value(
    value: &SqlValue,
    path: &[JsonPathElement],
    as_text: bool,
) -> SqlValue {
    if let SqlValue::JsonText(value) = value {
        return crate::jsonb::json_text_path_result(value, path, as_text).unwrap_or(SqlValue::Null);
    }
    let Some(value) = sql_json_lookup_path(value, path) else {
        return SqlValue::Null;
    };
    if !as_text {
        return SqlValue::Json(value.clone());
    }
    match value {
        JsonValue::Null => SqlValue::Null,
        JsonValue::String(value) => SqlValue::String(value.clone()),
        other => SqlValue::String(postgres_jsonb_text(other)),
    }
}

pub(crate) fn eval_json_operator_value(
    left: SqlValue,
    op: &BinaryOperator,
    right: SqlValue,
) -> Result<SqlValue> {
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    reject_invalid_json_unicode(&left)?;
    let path = json_operator_path_value(right)?;
    Ok(json_extract_path_value(
        &left,
        &path,
        matches!(op, BinaryOperator::LongArrow),
    ))
}

pub(crate) fn eval_json_hash_operator_value(
    left: SqlValue,
    op: &BinaryOperator,
    right: SqlValue,
) -> Result<SqlValue> {
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    reject_invalid_json_unicode(&left)?;
    let path = json_text_array_path(&right)?;
    if path.iter().any(Option::is_none) {
        return Ok(SqlValue::Null);
    }
    let path = path
        .into_iter()
        .flatten()
        .map(JsonPathElement::Text)
        .collect::<Vec<_>>();
    Ok(json_extract_path_value(
        &left,
        &path,
        matches!(op, BinaryOperator::HashLongArrow),
    ))
}

pub(crate) fn reject_invalid_json_unicode(value: &SqlValue) -> Result<()> {
    if matches!(value, SqlValue::JsonText(value) if value.has_invalid_unicode_escape()) {
        return Err(SqlError::ConstraintViolation {
            sqlstate: "22P02",
            message: "invalid input syntax for type json: unsupported Unicode escape sequence"
                .to_string(),
            table: None,
            column: None,
            constraint: None,
        });
    }
    Ok(())
}

pub(crate) fn eval_json_existence_value(
    left: SqlValue,
    op: &BinaryOperator,
    right: SqlValue,
) -> Result<SqlValue> {
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let SqlValue::Json(value) = left else {
        return Err(SqlError::InvalidSql(
            "JSON existence operator requires a jsonb left operand".to_string(),
        ));
    };
    let exists = |key: &str| match &value {
        JsonValue::Object(values) => values.contains_key(key),
        JsonValue::Array(values) => values
            .iter()
            .any(|value| value.as_str().is_some_and(|value| value == key)),
        JsonValue::String(value) => value == key,
        _ => false,
    };
    let result = match op {
        BinaryOperator::Question => match right {
            SqlValue::String(key) => exists(&key),
            _ => {
                return Err(SqlError::InvalidSql(
                    "jsonb ? operator requires a text right operand".to_string(),
                ));
            }
        },
        BinaryOperator::QuestionAnd | BinaryOperator::QuestionPipe => {
            let keys = json_text_array_path(&right)?;
            if matches!(op, BinaryOperator::QuestionAnd) {
                keys.iter().flatten().all(|key| exists(key))
            } else {
                keys.iter().any(|key| key.as_deref().is_some_and(&exists))
            }
        }
        _ => unreachable!("existence evaluator called for non-existence operator"),
    };
    Ok(SqlValue::Bool(result))
}

pub(crate) fn eval_json_delete_value(left: SqlValue, right: SqlValue) -> Result<SqlValue> {
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let SqlValue::Json(mut value) = left else {
        return Err(SqlError::InvalidSql(
            "jsonb deletion requires a jsonb left operand".to_string(),
        ));
    };
    match (&mut value, right) {
        (JsonValue::Object(values), SqlValue::Json(JsonValue::Array(keys))) => {
            for key in keys {
                match key {
                    JsonValue::Null => {}
                    JsonValue::String(key) => {
                        values.remove(&key);
                    }
                    _ => {
                        return Err(SqlError::InvalidSql(
                            "jsonb deletion key array must contain only text".to_string(),
                        ));
                    }
                }
            }
        }
        (JsonValue::Array(values), SqlValue::Json(JsonValue::Array(keys))) => {
            let mut removals = Vec::new();
            for key in keys {
                match key {
                    JsonValue::Null => {}
                    JsonValue::String(key) => removals.push(key),
                    _ => {
                        return Err(SqlError::InvalidSql(
                            "jsonb deletion key array must contain only text".to_string(),
                        ));
                    }
                }
            }
            values.retain(|value| {
                !value
                    .as_str()
                    .is_some_and(|value| removals.iter().any(|key| key == value))
            });
        }
        (JsonValue::Object(values), SqlValue::String(key)) => {
            values.remove(&key);
        }
        (JsonValue::Array(values), SqlValue::String(item)) => {
            values.retain(|value| value.as_str() != Some(&item));
        }
        (JsonValue::Array(values), SqlValue::Int(index)) => {
            if let Some(index) = json_array_index(values.len(), index) {
                values.remove(index);
            }
        }
        (JsonValue::Object(_), SqlValue::Int(_)) => {
            return Err(SqlError::InvalidSql(
                "cannot delete from object using integer index".to_string(),
            ));
        }
        (JsonValue::Object(_) | JsonValue::Array(_), _) => {
            return Err(SqlError::InvalidSql(
                "jsonb deletion requires text or integer on the right".to_string(),
            ));
        }
        _ => {
            return Err(SqlError::InvalidSql(
                "cannot delete from scalar".to_string(),
            ));
        }
    }
    Ok(SqlValue::Json(value))
}

pub(crate) fn eval_json_delete_path_value(left: SqlValue, right: SqlValue) -> Result<SqlValue> {
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let SqlValue::Json(mut value) = left else {
        return Err(SqlError::InvalidSql(
            "jsonb path deletion requires a jsonb left operand".to_string(),
        ));
    };
    let path = json_text_array_path(&right)?;
    if !path.is_empty() {
        delete_json_path(&mut value, &path, 0)?;
    }
    Ok(SqlValue::Json(value))
}

pub(crate) fn delete_json_path(
    value: &mut JsonValue,
    path: &[Option<String>],
    level: usize,
) -> Result<()> {
    let Some(element) = path[level].as_deref() else {
        // PostgreSQL treats a null first element as an unmatched path. A null
        // reached after traversing an existing container is an error.
        if level == 0 {
            return Ok(());
        }
        return Err(SqlError::ConstraintViolation {
            sqlstate: "22004",
            message: format!("path element at position {} is null", level + 1),
            table: None,
            column: None,
            constraint: None,
        });
    };
    let last = level + 1 == path.len();
    match value {
        JsonValue::Object(values) => {
            if last {
                values.remove(element);
            } else if let Some(next) = values.get_mut(element) {
                delete_json_path(next, path, level + 1)?;
            }
        }
        JsonValue::Array(values) => {
            if values.is_empty() {
                return Ok(());
            }
            let index = element.parse::<i64>().map_err(|_| {
                SqlError::InvalidTextRepresentation(format!(
                    "path element at position {} is not an integer: \"{element}\"",
                    level + 1
                ))
            })?;
            if let Some(index) = json_array_index(values.len(), index) {
                if last {
                    values.remove(index);
                } else {
                    delete_json_path(&mut values[index], path, level + 1)?;
                }
            }
        }
        _ if level == 0 => {
            return Err(SqlError::InvalidSql(
                "cannot delete path in scalar".to_string(),
            ));
        }
        _ => {}
    }
    Ok(())
}

pub(crate) fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}

#[derive(Clone, Debug)]
pub(crate) struct SqlTemporalContext {
    transaction_timestamp_seconds: i64,
    timezone: String,
    interval_style: String,
}

thread_local! {
    static SQL_TEMPORAL_CONTEXT: RefCell<Option<SqlTemporalContext>> = const { RefCell::new(None) };
}

pub(crate) struct SqlTemporalScope {
    previous: Option<SqlTemporalContext>,
}

impl SqlTemporalScope {
    pub(crate) fn new(
        transaction_timestamp_seconds: i64,
        timezone: impl Into<String>,
        interval_style: impl Into<String>,
    ) -> Self {
        let context = SqlTemporalContext {
            transaction_timestamp_seconds,
            timezone: timezone.into(),
            interval_style: interval_style.into(),
        };
        let previous = SQL_TEMPORAL_CONTEXT.with(|slot| slot.replace(Some(context)));
        Self { previous }
    }
}

impl Drop for SqlTemporalScope {
    fn drop(&mut self) {
        SQL_TEMPORAL_CONTEXT.with(|slot| {
            slot.replace(self.previous.take());
        });
    }
}

pub(crate) fn validate_timezone_name(timezone: &str) -> bool {
    timezone.eq_ignore_ascii_case("UTC")
        || timezone.eq_ignore_ascii_case("GMT")
        || timezone.eq_ignore_ascii_case("Etc/UTC")
        || timezone.parse::<Tz>().is_ok()
        || parse_fixed_timezone_offset(timezone).is_some()
}

pub(crate) fn parse_fixed_timezone_offset(timezone: &str) -> Option<i32> {
    let timezone = timezone.trim();
    let sign = match timezone.as_bytes().first()? {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let body = &timezone[1..];
    let (hours, minutes) = body
        .split_once(':')
        .map(|(hours, minutes)| (hours, minutes))
        .unwrap_or((body, "0"));
    let hours = hours.parse::<i32>().ok()?;
    let minutes = minutes.parse::<i32>().ok()?;
    if hours > 15 || minutes > 59 {
        return None;
    }
    // PostgreSQL interprets numeric session zone names using POSIX signs:
    // +02 means two hours west of UTC, unlike an ISO timestamp suffix.
    Some(-sign * (hours * 3_600 + minutes * 60))
}

pub(crate) fn timestamp_and_timezone() -> (i64, String) {
    SQL_TEMPORAL_CONTEXT
        .with(|slot| {
            slot.borrow().as_ref().map(|context| {
                (
                    context.transaction_timestamp_seconds,
                    context.timezone.clone(),
                )
            })
        })
        .unwrap_or_else(|| (unix_now(), "UTC".to_string()))
}

pub(crate) fn current_timezone_offset_seconds() -> i32 {
    let (seconds, timezone) = timestamp_and_timezone();
    let Some(timestamp) = DateTime::<Utc>::from_timestamp(seconds, 0) else {
        return 0;
    };
    if timezone.eq_ignore_ascii_case("UTC")
        || timezone.eq_ignore_ascii_case("GMT")
        || timezone.eq_ignore_ascii_case("Etc/UTC")
    {
        0
    } else if let Ok(timezone) = timezone.parse::<Tz>() {
        timestamp
            .with_timezone(&timezone)
            .offset()
            .fix()
            .local_minus_utc()
    } else {
        parse_fixed_timezone_offset(&timezone).unwrap_or(0)
    }
}

pub(crate) fn current_timezone_name() -> String {
    timestamp_and_timezone().1
}

pub(crate) fn current_interval_style() -> String {
    SQL_TEMPORAL_CONTEXT
        .with(|slot| {
            slot.borrow()
                .as_ref()
                .map(|context| context.interval_style.clone())
        })
        .unwrap_or_else(|| "postgres".to_string())
}

pub(crate) fn render_interval(interval: PgInterval) -> String {
    interval.to_style_text(&current_interval_style())
}

pub(crate) fn pg_timestamp_from_unix_seconds(seconds: i64) -> PgTimestamp {
    PgTimestamp::Finite((seconds - 946_684_800) * 1_000_000)
}

pub(crate) fn timezone_offset_at_timestamp(timezone: &str, timestamp: PgTimestamp) -> Option<i32> {
    let micros = timestamp.finite_micros()?;
    let unix_micros = micros.checked_add(946_684_800_000_000)?;
    let seconds = unix_micros.div_euclid(1_000_000);
    let subsecond_micros = unix_micros.rem_euclid(1_000_000) as u32;
    let utc = DateTime::<Utc>::from_timestamp(seconds, subsecond_micros * 1_000)?;
    if timezone.eq_ignore_ascii_case("UTC")
        || timezone.eq_ignore_ascii_case("GMT")
        || timezone.eq_ignore_ascii_case("Etc/UTC")
    {
        Some(0)
    } else if let Ok(timezone) = timezone.parse::<Tz>() {
        Some(
            timezone
                .offset_from_utc_datetime(&utc.naive_utc())
                .fix()
                .local_minus_utc(),
        )
    } else {
        parse_fixed_timezone_offset(timezone)
    }
}

pub(crate) fn format_timezone_offset(offset: i32) -> String {
    let sign = if offset < 0 { '-' } else { '+' };
    let magnitude = offset.unsigned_abs();
    let hours = magnitude / 3_600;
    let minutes = magnitude % 3_600 / 60;
    let seconds = magnitude % 60;
    if minutes == 0 && seconds == 0 {
        format!("{sign}{hours:02}")
    } else if seconds == 0 {
        format!("{sign}{hours:02}:{minutes:02}")
    } else {
        format!("{sign}{hours:02}:{minutes:02}:{seconds:02}")
    }
}

pub(crate) fn render_timestamptz_in_zone(timestamp: PgTimestamp, timezone: &str) -> String {
    let Some(micros) = timestamp.finite_micros() else {
        return timestamp.to_iso_text(false);
    };
    let offset = timezone_offset_at_timestamp(timezone, timestamp).unwrap_or(0);
    let local = PgTimestamp::Finite(
        micros
            .checked_add(i64::from(offset) * 1_000_000)
            .unwrap_or(micros),
    )
    .to_iso_text(false);
    let suffix = format_timezone_offset(offset);
    if let Some(local) = local.strip_suffix(" BC") {
        format!("{local}{suffix} BC")
    } else {
        format!("{local}{suffix}")
    }
}

pub(crate) fn render_timestamptz(timestamp: PgTimestamp) -> String {
    render_timestamptz_in_zone(timestamp, &current_timezone_name())
}

pub(crate) fn local_timestamp_offset_seconds(local: PgTimestamp, timezone: &str) -> Option<i32> {
    if timezone.eq_ignore_ascii_case("UTC")
        || timezone.eq_ignore_ascii_case("GMT")
        || timezone.eq_ignore_ascii_case("Etc/UTC")
    {
        return Some(0);
    }
    if let Some(offset) = parse_fixed_timezone_offset(timezone) {
        return Some(offset);
    }
    let timezone = timezone.parse::<Tz>().ok()?;
    let (year, month, day, hour, minute, second, micros) = local.components()?;
    let naive = NaiveDate::from_ymd_opt(year, month, day)?
        .and_hms_micro_opt(hour, minute, second, micros)?;
    match timezone.from_local_datetime(&naive) {
        LocalResult::Single(value) => Some(value.offset().fix().local_minus_utc()),
        LocalResult::Ambiguous(first, second) => {
            let chosen = if first.with_timezone(&Utc) > second.with_timezone(&Utc) {
                first
            } else {
                second
            };
            Some(chosen.offset().fix().local_minus_utc())
        }
        LocalResult::None => {
            let before = naive - ChronoDuration::hours(3);
            match timezone.from_local_datetime(&before) {
                LocalResult::Single(value) => Some(value.offset().fix().local_minus_utc()),
                LocalResult::Ambiguous(first, _) => Some(first.offset().fix().local_minus_utc()),
                LocalResult::None => None,
            }
        }
    }
}

pub(crate) fn split_named_timestamp_zone(value: &str) -> (&str, Option<&str>) {
    let trimmed = value.trim();
    let upper = trimmed.to_ascii_uppercase();
    let era_len = if upper.ends_with(" BC") || upper.ends_with(" AD") {
        3
    } else {
        0
    };
    let without_era = trimmed[..trimmed.len() - era_len].trim_end();
    let Some((timestamp, zone)) = without_era.rsplit_once(char::is_whitespace) else {
        return (trimmed, None);
    };
    let named = zone.eq_ignore_ascii_case("UTC")
        || zone.eq_ignore_ascii_case("GMT")
        || zone.eq_ignore_ascii_case("Etc/UTC")
        || zone.parse::<Tz>().is_ok();
    if !named {
        return (trimmed, None);
    }
    let timestamp = timestamp.trim_end();
    if era_len == 0 {
        (timestamp, Some(zone))
    } else {
        // Era-bearing named-zone inputs are rare and remain handled by the
        // canonical parser's fixed-offset path.
        (trimmed, None)
    }
}

pub(crate) fn timestamp_has_iso_offset(value: &str) -> bool {
    let value = value.trim();
    let time = value
        .find(['T', 't'])
        .map(|index| &value[index + 1..])
        .or_else(|| value.split_whitespace().nth(1))
        .unwrap_or("");
    time.ends_with('Z')
        || time.ends_with('z')
        || time
            .char_indices()
            .any(|(index, ch)| index > 0 && matches!(ch, '+' | '-'))
}

pub(crate) fn parse_timestamptz_in_zone(
    value: &str,
    default_timezone: &str,
) -> std::result::Result<PgTimestamp, PgCanonicalValueError> {
    let lower = value.trim().to_ascii_lowercase();
    if matches!(
        lower.as_str(),
        "epoch" | "infinity" | "+infinity" | "-infinity"
    ) {
        return PgTimestamp::from_postgres_text(value, true);
    }
    if timestamp_has_iso_offset(value) {
        return PgTimestamp::from_postgres_text(value, true);
    }
    let (timestamp, named_timezone) = split_named_timestamp_zone(value);
    let timezone = named_timezone.unwrap_or(default_timezone);
    let local = PgTimestamp::from_postgres_text(timestamp, false)?;
    let Some(local_micros) = local.finite_micros() else {
        return Ok(local);
    };
    let offset = local_timestamp_offset_seconds(local, timezone).ok_or_else(|| {
        PgCanonicalValueError::InvalidTemporal {
            kind: "timestamptz",
            value: value.to_string(),
        }
    })?;
    local_micros
        .checked_sub(i64::from(offset) * 1_000_000)
        .map(PgTimestamp::Finite)
        .ok_or(PgCanonicalValueError::TemporalOverflow("timestamptz"))
}

pub(crate) fn parse_timestamptz(
    value: &str,
) -> std::result::Result<PgTimestamp, PgCanonicalValueError> {
    parse_timestamptz_in_zone(value, &current_timezone_name())
}

pub(crate) fn eval_at_time_zone_value(
    value: SqlValue,
    zone: SqlValue,
    source_type: Option<&str>,
) -> Result<SqlValue> {
    if matches!(value, SqlValue::Null) || matches!(zone, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let zone = zone.to_cell();
    if !validate_timezone_name(&zone) {
        return Err(SqlError::invalid_parameter_value(format!(
            "time zone \"{zone}\" not recognized"
        )));
    }
    let value = value.to_cell();
    let source_is_timestamptz = source_type == Some("timestamptz")
        || (source_type.is_none() && timestamp_has_iso_offset(&value));
    if source_is_timestamptz {
        let timestamp = parse_timestamptz(&value)
            .map_err(|error| postgres_timestamptz_input_error(&value, error))?;
        let Some(micros) = timestamp.finite_micros() else {
            return Ok(SqlValue::String(timestamp.to_iso_text(false)));
        };
        let offset = timezone_offset_at_timestamp(&zone, timestamp).ok_or_else(|| {
            SqlError::invalid_parameter_value(format!("time zone \"{zone}\" not recognized"))
        })?;
        let local = micros
            .checked_add(i64::from(offset) * 1_000_000)
            .map(PgTimestamp::Finite)
            .ok_or_else(|| {
                SqlError::data_exception(
                    "22008",
                    "timestamp out of range",
                    Some("timestamp".to_string()),
                )
            })?;
        return Ok(SqlValue::String(local.to_iso_text(false)));
    }
    let timestamp = parse_timestamptz_in_zone(&value, &zone)
        .map_err(|error| postgres_timestamptz_input_error(&value, error))?;
    Ok(SqlValue::String(render_timestamptz(timestamp)))
}

pub(crate) fn unix_now_timestamp_string() -> String {
    render_timestamptz(pg_timestamp_from_unix_seconds(unix_now()))
}

pub(crate) fn transaction_timestamp_string() -> String {
    let (seconds, _) = timestamp_and_timezone();
    render_timestamptz(pg_timestamp_from_unix_seconds(seconds))
}

pub(crate) fn unix_now_date_string() -> String {
    let (seconds, timezone) = timestamp_and_timezone();
    unix_seconds_to_date_in_timezone(seconds, &timezone)
        .unwrap_or_else(|| unix_seconds_to_date_in_timezone(seconds, "UTC").unwrap())
}

pub(crate) fn unix_seconds_to_date_in_timezone(seconds: i64, timezone: &str) -> Option<String> {
    let timestamp = DateTime::<Utc>::from_timestamp(seconds, 0)?;
    let date = if timezone.eq_ignore_ascii_case("UTC")
        || timezone.eq_ignore_ascii_case("GMT")
        || timezone.eq_ignore_ascii_case("Etc/UTC")
    {
        timestamp.date_naive()
    } else if let Ok(timezone) = timezone.parse::<Tz>() {
        timestamp.with_timezone(&timezone).date_naive()
    } else {
        let offset = FixedOffset::east_opt(parse_fixed_timezone_offset(timezone)?)?;
        timestamp.with_timezone(&offset).date_naive()
    };
    Some(date.format("%Y-%m-%d").to_string())
}

pub(crate) fn unix_seconds_to_utc_timestamp(seconds: i64) -> String {
    let days = seconds.div_euclid(86_400);
    let seconds_of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;
    format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}")
}

pub(crate) fn sql_random_value() -> f64 {
    let seed = || {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos() as u64)
            .unwrap_or(0x9e37_79b9_7f4a_7c15);
        nanos ^ 0xa076_1d64_78bd_642f
    };
    let mut current = SQL_RANDOM_STATE.load(std::sync::atomic::Ordering::Relaxed);
    if current == 0 {
        let initialized = seed().max(1);
        let _ = SQL_RANDOM_STATE.compare_exchange(
            0,
            initialized,
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
        );
        current = SQL_RANDOM_STATE.load(std::sync::atomic::Ordering::Relaxed);
    }
    loop {
        let next = current
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        match SQL_RANDOM_STATE.compare_exchange(
            current,
            next,
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
        ) {
            Ok(_) => return ((next >> 11) as f64) / ((1u64 << 53) as f64),
            Err(observed) => current = observed,
        }
    }
}

/// Epoch-day count for a date-only text (`2026-08-22`). `None` for anything
/// carrying a time component, so timestamp subtraction still yields an
/// interval.
fn date_only_epoch_days(text: &str) -> Option<i32> {
    let text = text.trim();
    if text.contains(':') || text.contains(' ') || text.contains('T') {
        return None;
    }
    PgDate::from_postgres_text(text)
        .ok()
        .and_then(|date| date.epoch_days())
}

pub(crate) fn eval_date_interval_arithmetic(
    left: &SqlValue,
    op: &BinaryOperator,
    right: &SqlValue,
) -> Result<Option<SqlValue>> {
    let (SqlValue::String(left), SqlValue::String(right)) = (left, right) else {
        return Ok(None);
    };
    if let (Ok(left_interval), Ok(right_interval)) = (
        PgInterval::from_postgres_text(left),
        PgInterval::from_postgres_text(right),
    ) {
        let result = match op {
            BinaryOperator::Plus => left_interval.checked_add(right_interval),
            BinaryOperator::Minus => left_interval.checked_sub(right_interval),
            _ => return Ok(None),
        }
        .map_err(|_| {
            SqlError::data_exception(
                "22015",
                "interval out of range",
                Some("interval".to_string()),
            )
        })?;
        return Ok(Some(SqlValue::String(render_interval(result))));
    }
    let left_interval = parse_interval_seconds(left);
    let right_interval = parse_interval_seconds(right);
    let left_temporal = parse_temporal_seconds(left);
    let right_temporal = parse_temporal_seconds(right);
    match (left_interval, right_interval, op) {
        (None, None, BinaryOperator::Minus)
            if left_temporal.is_some() && right_temporal.is_some() =>
        {
            // date - date is an integer day count in PostgreSQL; only
            // timestamp subtraction yields an interval. A date-only text on
            // both sides therefore returns Int, so casts like
            // `(d1 - d2)::BIGINT` keep working when the operands reach this
            // untyped fallback.
            if let (Some(left_days), Some(right_days)) =
                (date_only_epoch_days(left), date_only_epoch_days(right))
            {
                return Ok(Some(SqlValue::Int(i64::from(left_days - right_days))));
            }
            Ok(Some(SqlValue::String(format_interval_seconds(
                left_temporal.expect("guard checked temporal value")
                    - right_temporal.expect("guard checked temporal value"),
            ))))
        }
        (None, None, BinaryOperator::Minus)
            if is_valid_numeric_text(left) && is_valid_numeric_text(right) =>
        {
            let left = left.trim().parse::<i64>().map_err(|error| {
                SqlError::InvalidSql(format!("cannot subtract timestamp {left}: {error}"))
            })?;
            let right = right.trim().parse::<i64>().map_err(|error| {
                SqlError::InvalidSql(format!("cannot subtract timestamp {right}: {error}"))
            })?;
            Ok(Some(SqlValue::String(format_interval_seconds(
                left - right,
            ))))
        }
        (None, None, BinaryOperator::Minus) => {
            match (temporal_epoch_seconds(left), temporal_epoch_seconds(right)) {
                (Some(left), Some(right)) => Ok(Some(SqlValue::String(format_interval_seconds(
                    left - right,
                )))),
                _ => Ok(None),
            }
        }
        (None, Some(seconds), BinaryOperator::Plus) => {
            Ok(Some(add_interval_to_value(left, seconds)?))
        }
        (None, Some(seconds), BinaryOperator::Minus) => {
            Ok(Some(add_interval_to_value(left, -seconds)?))
        }
        (Some(seconds), None, BinaryOperator::Plus) => {
            Ok(Some(add_interval_to_value(right, seconds)?))
        }
        (Some(left), Some(right), BinaryOperator::Plus) => Ok(Some(SqlValue::String(
            format_interval_seconds(left + right),
        ))),
        (Some(left), Some(right), BinaryOperator::Minus) => Ok(Some(SqlValue::String(
            format_interval_seconds(left - right),
        ))),
        _ => Ok(None),
    }
}

pub(crate) fn parse_temporal_seconds(value: &str) -> Option<i64> {
    let trimmed = value.trim();
    let (year, month, day) = parse_date_prefix(trimmed)?;
    let day_seconds = parse_time_prefix(trimmed)
        .map(|(hour, minute, second)| hour as i64 * 3_600 + minute as i64 * 60 + second as i64)
        .unwrap_or(0);
    Some(days_from_civil(year, month, day) * 86_400 + day_seconds)
}

pub(crate) fn eval_extract_value(field: &DateTimeField, value: SqlValue) -> Result<SqlValue> {
    if matches!(value, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let field_name = field.to_string().to_ascii_lowercase();
    let text = value.to_cell();
    if let Ok(interval) = PgInterval::from_postgres_text(&text) {
        return interval
            .extract_field(&field_name)
            .map(SqlValue::String)
            .ok_or_else(|| {
                SqlError::Unsupported(format!("EXTRACT({field} FROM interval) is not supported"))
            });
    }
    if matches!(field, DateTimeField::Epoch) && timestamp_has_iso_offset(&text) {
        let timestamp = parse_timestamptz(&text)
            .map_err(|error| postgres_timestamptz_input_error(&text, error))?;
        return timestamp
            .extract_field(&field_name)
            .map(SqlValue::String)
            .ok_or_else(|| {
                SqlError::Unsupported(
                    "EXTRACT(EPOCH FROM timestamptz) is not supported".to_string(),
                )
            });
    }
    if let Ok(timestamp) = PgTimestamp::from_postgres_text(&text, false) {
        return timestamp
            .extract_field(&field_name)
            .map(SqlValue::String)
            .ok_or_else(|| {
                SqlError::Unsupported(format!("EXTRACT({field} FROM timestamp) is not supported"))
            });
    }
    match field {
        DateTimeField::Epoch => {
            if let Some(seconds) = parse_interval_seconds(&value.to_cell()) {
                return Ok(SqlValue::Int(seconds));
            }
            if is_valid_numeric_text(&value.to_cell()) {
                return value
                    .to_cell()
                    .parse::<f64>()
                    .map(SqlValue::Float)
                    .map_err(|error| {
                        SqlError::InvalidSql(format!(
                            "cannot extract epoch from {}: {error}",
                            value.to_cell()
                        ))
                    });
            }
            Err(SqlError::Unsupported(format!(
                "EXTRACT(EPOCH FROM {}) is not supported",
                value.to_cell()
            )))
        }
        other => Err(SqlError::Unsupported(format!(
            "EXTRACT({other} FROM ...) is not supported"
        ))),
    }
}

pub(crate) fn interval_literal_value<F>(interval: &Interval, mut eval: F) -> Result<SqlValue>
where
    F: FnMut(&Expr) -> Result<SqlValue>,
{
    let mut value = eval(&interval.value)?.to_cell();
    if let Some(field) = &interval.leading_field {
        let bare_number = is_valid_numeric_text(value.trim());
        if bare_number {
            value = format!(
                "{} {}",
                value.trim(),
                field.to_string().to_ascii_lowercase()
            );
        }
    }
    let fields = interval.leading_field.as_ref().map(|leading| {
        interval.last_field.as_ref().map_or_else(
            || leading.to_string().to_ascii_uppercase(),
            |last| format!("{} TO {}", leading, last).to_ascii_uppercase(),
        )
    });
    let precision = interval.fractional_seconds_precision.or_else(|| {
        (interval.leading_field.is_none()
            || matches!(interval.leading_field, Some(DateTimeField::Second)))
        .then_some(interval.leading_precision)
        .flatten()
    });
    if precision.is_some_and(|precision| precision > 6) {
        return Err(SqlError::invalid_parameter_value(
            "interval precision must be between 0 and 6",
        ));
    }
    PgInterval::from_postgres_text(&value)
        .and_then(|value| value.with_typmod(fields.as_deref(), precision.map(|value| value as u8)))
        .map(|value| SqlValue::String(render_interval(value)))
        .map_err(|error| postgres_interval_input_error(&value, error))
}

pub(crate) fn add_interval_to_value(value: &str, seconds: i64) -> Result<SqlValue> {
    if is_valid_numeric_text(value) {
        let base = value.trim().parse::<i64>().map_err(|error| {
            SqlError::InvalidSql(format!(
                "cannot add interval to numeric timestamp {value}: {error}"
            ))
        })?;
        return Ok(SqlValue::String((base + seconds).to_string()));
    }
    add_seconds_to_temporal(value, seconds)
}

pub(crate) fn parse_interval_seconds(value: &str) -> Option<i64> {
    let mut parts = value.split_whitespace().collect::<Vec<_>>();
    if parts.is_empty() {
        return None;
    }
    if parts.len() == 1 && parts[0].contains(':') {
        return parse_hms(parts[0]);
    }
    let mut total = 0_i64;
    while !parts.is_empty() {
        if parts.len() < 2 {
            return None;
        }
        let amount = parts.remove(0).parse::<i64>().ok()?;
        let unit = parts.remove(0).trim_end_matches('s').to_ascii_lowercase();
        let seconds = match unit.as_str() {
            "day" => 86_400,
            "hour" => 3_600,
            "minute" | "min" => 60,
            "second" | "sec" => 1,
            _ => return None,
        };
        total = total.checked_add(amount.checked_mul(seconds)?)?;
    }
    Some(total)
}

pub(crate) fn parse_hms(value: &str) -> Option<i64> {
    let parts = value.split(':').collect::<Vec<_>>();
    if !(2..=3).contains(&parts.len()) {
        return None;
    }
    let hours = parts[0].parse::<i64>().ok()?;
    let minutes = parts[1].parse::<i64>().ok()?;
    let seconds = parts
        .get(2)
        .and_then(|part| part.split('.').next())
        .unwrap_or("0")
        .parse::<i64>()
        .ok()?;
    Some(hours * 3_600 + minutes * 60 + seconds)
}

pub(crate) fn format_interval_seconds(seconds: i64) -> String {
    if seconds % 86_400 == 0 {
        format!("{} days", seconds / 86_400)
    } else {
        format!("{seconds} seconds")
    }
}

pub(crate) fn add_seconds_to_temporal(value: &str, seconds: i64) -> Result<SqlValue> {
    let trimmed = value.trim();
    let Some((year, month, day)) = parse_date_prefix(trimmed) else {
        return Err(SqlError::InvalidSql(format!(
            "cannot add interval to non-date/time value {value}"
        )));
    };
    let day_seconds = parse_time_prefix(trimmed)
        .map(|(hour, minute, second)| hour as i64 * 3_600 + minute as i64 * 60 + second as i64)
        .unwrap_or(0);
    let total_seconds = days_from_civil(year, month, day) * 86_400 + day_seconds + seconds;
    let mut days = total_seconds.div_euclid(86_400);
    let mut seconds_of_day = total_seconds.rem_euclid(86_400);
    if seconds_of_day < 0 {
        days -= 1;
        seconds_of_day += 86_400;
    }
    let (year, month, day) = civil_from_days(days);
    if trimmed.len() <= 10 {
        Ok(SqlValue::String(format!("{year:04}-{month:02}-{day:02}")))
    } else {
        let hour = seconds_of_day / 3_600;
        let minute = (seconds_of_day % 3_600) / 60;
        let second = seconds_of_day % 60;
        Ok(SqlValue::String(format!(
            "{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}"
        )))
    }
}

pub(crate) fn temporal_epoch_seconds(value: &str) -> Option<i64> {
    let trimmed = value.trim();
    let (year, month, day) = parse_date_prefix(trimmed)?;
    let day_seconds = parse_time_prefix(trimmed)
        .map(|(hour, minute, second)| hour as i64 * 3_600 + minute as i64 * 60 + second as i64)
        .unwrap_or(0);
    Some(days_from_civil(year, month, day) * 86_400 + day_seconds)
}

pub(crate) fn parse_date_prefix(value: &str) -> Option<(i32, u32, u32)> {
    let date = value.get(..10)?;
    let mut parts = date.split('-');
    let year = parts.next()?.parse::<i32>().ok()?;
    let month = parts.next()?.parse::<u32>().ok()?;
    let day = parts.next()?.parse::<u32>().ok()?;
    (1..=12).contains(&month).then_some(())?;
    (1..=31).contains(&day).then_some(())?;
    Some((year, month, day))
}

pub(crate) fn parse_time_prefix(value: &str) -> Option<(u32, u32, u32)> {
    let time = value.split_once(' ')?.1;
    let time = time.split(['+', '-']).next().unwrap_or(time);
    let parts = time.split(':').collect::<Vec<_>>();
    if parts.len() < 2 {
        return None;
    }
    let hour = parts[0].parse::<u32>().ok()?;
    let minute = parts[1].parse::<u32>().ok()?;
    let second = parts
        .get(2)
        .and_then(|part| part.split('.').next())
        .unwrap_or("0")
        .parse::<u32>()
        .ok()?;
    Some((hour, minute, second))
}

pub(crate) fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let year = year - i32::from(month <= 2);
    let era = (if year >= 0 { year } else { year - 399 }) / 400;
    let yoe = year - era * 400;
    let month = month as i32;
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day as i32 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146_097 + doe - 719_468) as i64
}

pub(crate) fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let days = days + 719_468;
    let era = (if days >= 0 { days } else { days - 146_096 }) / 146_097;
    let doe = days - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe as i32 + era as i32 * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    (year + i32::from(month <= 2), month as u32, day as u32)
}

pub(crate) fn negate_numeric_value(value: SqlValue) -> Result<SqlValue> {
    match value {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Int(value) => value
            .checked_neg()
            .map(SqlValue::Int)
            .ok_or_else(|| integer_out_of_range("int8")),
        SqlValue::Float(value) => Ok(SqlValue::Float(-value)),
        SqlValue::String(value) if is_valid_numeric_text(&value) => {
            Ok(SqlValue::String(parse_decimal(&value)?.neg().format()))
        }
        other => Err(SqlError::InvalidSql(format!(
            "cannot apply unary minus to {}",
            other.to_cell()
        ))),
    }
}

pub(crate) fn eval_unary_minus_expr_value(
    expr: &Expr,
    value: SqlValue,
    schema: Option<&TableSchema>,
) -> Result<SqlValue> {
    let pg_type = projected_expr_pg_type(expr, schema);
    if matches!(pg_type.as_deref(), Some("money")) {
        return Err(SqlError::undefined_function(
            "operator does not exist: - money",
        ));
    }
    if matches!(pg_type.as_deref(), Some("interval")) {
        if matches!(value, SqlValue::Null) {
            return Ok(SqlValue::Null);
        }
        let text = value.to_cell();
        return PgInterval::from_postgres_text(&text)
            .and_then(PgInterval::checked_neg)
            .map(|interval| SqlValue::String(render_interval(interval)))
            .map_err(|error| postgres_interval_input_error(&text, error));
    }
    if matches!(pg_type.as_deref(), Some("numeric")) {
        return pg_numeric_from_sql_value(value)
            .map(negate_pg_numeric)
            .map(|value| SqlValue::String(value.to_decimal_text()));
    }
    let value = negate_numeric_value(value)
        .map_err(|error| remap_integer_overflow(error, pg_type.as_deref()))?;
    enforce_integer_value_type(value, pg_type.as_deref())
}

pub(crate) fn eval_unary_plus_expr_value(
    expr: &Expr,
    value: SqlValue,
    schema: Option<&TableSchema>,
) -> Result<SqlValue> {
    if matches!(
        projected_expr_pg_type(expr, schema).as_deref(),
        Some("money")
    ) {
        return Err(SqlError::undefined_function(
            "operator does not exist: + money",
        ));
    }
    Ok(value)
}

pub(crate) fn tsquery_from_sql_value(value: &SqlValue) -> Result<PgTsQuery> {
    match value {
        SqlValue::TsQuery(query) => Ok(query.clone()),
        _ => PgTsQuery::from_postgres_text(&value.to_cell()),
    }
}

pub(crate) fn is_builtin_range_type(pg_type: &str) -> bool {
    matches!(
        pg_type,
        "int4range" | "int8range" | "numrange" | "daterange" | "tsrange" | "tstzrange"
    )
}

pub(crate) fn is_builtin_multirange_type(pg_type: &str) -> bool {
    matches!(
        pg_type,
        "int4multirange"
            | "int8multirange"
            | "nummultirange"
            | "datemultirange"
            | "tsmultirange"
            | "tstzmultirange"
    )
}

pub(crate) fn range_subtype_name(range_type: &str) -> Option<&'static str> {
    match range_type {
        "int4range" => Some("int4"),
        "int8range" => Some("int8"),
        "numrange" => Some("numeric"),
        "daterange" => Some("date"),
        "tsrange" => Some("timestamp"),
        "tstzrange" => Some("timestamptz"),
        _ => None,
    }
}

pub(crate) fn range_type_from_multirange(multirange_type: &str) -> Option<&'static str> {
    match multirange_type {
        "int4multirange" => Some("int4range"),
        "int8multirange" => Some("int8range"),
        "nummultirange" => Some("numrange"),
        "datemultirange" => Some("daterange"),
        "tsmultirange" => Some("tsrange"),
        "tstzmultirange" => Some("tstzrange"),
        _ => None,
    }
}

pub(crate) fn range_family(pg_type: &str) -> Option<(&'static str, &'static str)> {
    match pg_type {
        "int4range" | "int4multirange" => Some(("int4range", "int4multirange")),
        "int8range" | "int8multirange" => Some(("int8range", "int8multirange")),
        "numrange" | "nummultirange" => Some(("numrange", "nummultirange")),
        "daterange" | "datemultirange" => Some(("daterange", "datemultirange")),
        "tsrange" | "tsmultirange" => Some(("tsrange", "tsmultirange")),
        "tstzrange" | "tstzmultirange" => Some(("tstzrange", "tstzmultirange")),
        _ => None,
    }
}

pub(crate) fn range_from_sql_value(value: &SqlValue, range_type: &str) -> Result<Option<PgRange>> {
    if matches!(value, SqlValue::Null) {
        return Ok(None);
    }
    let text = value.to_cell();
    parse_pg_canonical_special(range_type, &text)
        .and_then(|value| {
            value.ok_or_else(|| PgCanonicalValueError::InvalidSpecialValue {
                pg_type: range_type.to_string(),
                value: text.clone(),
            })
        })
        .and_then(|value| match value {
            PgCanonicalValue::Range(range) => Ok(range),
            _ => Err(PgCanonicalValueError::InvalidSpecialValue {
                pg_type: range_type.to_string(),
                value: text.clone(),
            }),
        })
        .map(Some)
        .map_err(|error| postgres_range_input_error(range_type, &text, error))
}

pub(crate) fn multirange_from_sql_value(
    value: &SqlValue,
    multirange_type: &str,
) -> Result<Option<Vec<PgRange>>> {
    if matches!(value, SqlValue::Null) {
        return Ok(None);
    }
    let text = value.to_cell();
    parse_pg_canonical_special(multirange_type, &text)
        .and_then(|value| {
            value.ok_or_else(|| PgCanonicalValueError::InvalidSpecialValue {
                pg_type: multirange_type.to_string(),
                value: text.clone(),
            })
        })
        .and_then(|value| match value {
            PgCanonicalValue::Multirange(ranges) => Ok(ranges),
            _ => Err(PgCanonicalValueError::InvalidSpecialValue {
                pg_type: multirange_type.to_string(),
                value: text.clone(),
            }),
        })
        .map(Some)
        .map_err(|error| postgres_range_input_error(multirange_type, &text, error))
}

pub(crate) fn canonical_range_scalar_value(value: &PgCanonicalValue) -> SqlValue {
    match value {
        PgCanonicalValue::Int4(value) => SqlValue::Int(i64::from(*value)),
        PgCanonicalValue::Int8(value) => SqlValue::Int(*value),
        PgCanonicalValue::Numeric(value) => SqlValue::String(value.to_decimal_text()),
        PgCanonicalValue::Date(value) => SqlValue::String(value.to_iso_text()),
        PgCanonicalValue::Timestamp(value) => SqlValue::String(value.to_iso_text(false)),
        PgCanonicalValue::TimestampTz(value) => SqlValue::String(render_timestamptz(*value)),
        _ => SqlValue::Null,
    }
}

#[cfg(test)]
mod numeric_identity_cast_tests {
    use super::*;

    #[test]
    fn canonical_numeric_cast_reuses_owned_text() {
        for text in ["0", "0.00", "12.50", "-0.05", "-12345678901234567890.0010"] {
            let input = text.to_string();
            let allocation = input.as_ptr();
            let SqlValue::String(output) =
                cast_value_to_pg_type_fast(SqlValue::String(input), "numeric").unwrap()
            else {
                panic!("numeric cast must retain its string representation");
            };
            assert_eq!(output, text);
            assert_eq!(
                output.as_ptr(),
                allocation,
                "identity cast must not rebuild {text}"
            );
        }
    }

    #[test]
    fn numeric_cast_matches_full_parser_including_range_limits() {
        let mut inputs = [
            "0",
            "0.00",
            "-0",
            "-0.00",
            "+0012.50",
            "12.",
            ".5",
            "1e3",
            "-1.20e-3",
            " 12.50 ",
            "NaN",
            "INF",
            "-Infinity",
            "",
            "1.2.3",
            "１２",
            "1_000",
            "1e1000000",
            "1e-1000000",
        ]
        .map(str::to_string)
        .to_vec();
        for integer in -100..=100 {
            for fraction in ["", ".0", ".0001", ".1200", ".999999999999999999"] {
                inputs.push(format!("{integer}{fraction}"));
            }
        }
        for digits in [131_072, 131_073] {
            inputs.push("9".repeat(digits));
        }
        for digits in [16_383, 16_384] {
            inputs.push(format!("0.{}", "0".repeat(digits)));
            inputs.push(format!("-0.{}1", "0".repeat(digits - 1)));
        }
        for input in inputs {
            let expected = PgNumeric::from_postgres_text(&input);
            let actual = cast_value_to_pg_type_fast(SqlValue::String(input.clone()), "numeric");
            match expected {
                Ok(expected) => assert_eq!(
                    actual.unwrap(),
                    SqlValue::String(expected.to_decimal_text())
                ),
                Err(PgCanonicalValueError::NumericOverflow) => {
                    assert_eq!(actual.unwrap_err().sqlstate(), "22003");
                }
                Err(_) => assert_eq!(actual.unwrap_err().sqlstate(), "22P02"),
            }
        }
    }
}
