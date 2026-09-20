//! Record materialization helpers: column set/get on records, unique arbiter resolution, index key construction, range literal parsing, record column value extraction, RLS policy expression parsing, graph routing, and spatial/vector literal handling.
//!
//! Extracted verbatim from `lib.rs` (phase-1 mechanical module split).
//! Moved items were bumped to `pub(crate)` so existing call sites keep
//! resolving; `lib.rs` re-exports this module via `pub use records::*;`.

// Glue import: bring every crate-root item (including the root's private
// imports and the other split modules' re-exports) into scope so the moved
// code compiles unchanged.
use crate::*;

pub(crate) const TYPED_STORAGE_KEY: &str = "$bicdb_typed";
const TYPED_STORAGE_VERSION: u8 = 1;

/// Typed-storage index keys are persisted as lowercase hex strings. Records
/// written before 1.0.366 carry them as JSON arrays of byte values; both shapes
/// decode, and `bicdb-core` (`typed_storage_index_label`) accepts both too.
mod index_key_codec {
    use serde::de::{self, Deserializer, SeqAccess, Visitor};
    use serde::Serializer;
    use std::fmt;

    const HEX: &[u8; 16] = b"0123456789abcdef";

    pub(super) fn serialize<S: Serializer>(key: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        let mut text = String::with_capacity(key.len() * 2);
        for byte in key {
            text.push(HEX[(byte >> 4) as usize] as char);
            text.push(HEX[(byte & 0x0f) as usize] as char);
        }
        serializer.serialize_str(&text)
    }

    pub(super) fn decode_hex(text: &str) -> Option<Vec<u8>> {
        if text.len() % 2 != 0 {
            return None;
        }
        text.as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let high = (pair[0] as char).to_digit(16)?;
                let low = (pair[1] as char).to_digit(16)?;
                Some((high * 16 + low) as u8)
            })
            .collect()
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<u8>, D::Error> {
        struct KeyVisitor;

        impl<'de> Visitor<'de> for KeyVisitor {
            type Value = Vec<u8>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a hex string or an array of byte values")
            }

            fn visit_str<E: de::Error>(self, text: &str) -> Result<Self::Value, E> {
                decode_hex(text)
                    .ok_or_else(|| E::custom("typed-storage index key is not valid hex"))
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut key = Vec::with_capacity(seq.size_hint().unwrap_or(0));
                while let Some(byte) = seq.next_element::<u8>()? {
                    key.push(byte);
                }
                Ok(key)
            }
        }

        deserializer.deserialize_any(KeyVisitor)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TypedStorageMigrationReport {
    pub tables_scanned: usize,
    pub records_scanned: usize,
    pub records_rewritten: usize,
    pub values_rewritten: usize,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct StoredNumericValue {
    version: u8,
    pg_type: String,
    /// Structured form written before 1.0.366; current records carry `text`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    value: Option<PgNumeric>,
    /// Canonical decimal text (`PgNumeric::to_decimal_text`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    #[serde(default, with = "index_key_codec")]
    index_key: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct StoredScalarValue {
    version: u8,
    pg_type: String,
    value: PgCanonicalValue,
    #[serde(default, with = "index_key_codec")]
    index_key: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct StoredTemporalValue {
    version: u8,
    pg_type: String,
    /// Structured form written before 1.0.366; current records carry only
    /// `text` (ISO/UTC for timestamptz, PostgreSQL text for interval).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    value: Option<PgCanonicalValue>,
    text: String,
    #[serde(default, with = "index_key_codec")]
    index_key: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct StoredBinaryValue {
    version: u8,
    pg_type: String,
    value: PgCanonicalValue,
    text: String,
    #[serde(default, with = "index_key_codec")]
    index_key: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct StoredArrayValue {
    version: u8,
    pg_type: String,
    value: PgArray,
    legacy: JsonValue,
    #[serde(default, with = "index_key_codec")]
    index_key: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct StoredJsonTextValue {
    version: u8,
    pg_type: String,
    text: String,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct StoredSpecialValue {
    version: u8,
    pg_type: String,
    value: PgCanonicalValue,
    text: String,
    #[serde(default, with = "index_key_codec")]
    index_key: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct StoredUserTypeValue {
    version: u8,
    pg_type: String,
    type_oid: i64,
    value: JsonValue,
    #[serde(default)]
    composite: Option<StoredCompositeValue>,
    #[serde(default, with = "index_key_codec")]
    index_key: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
struct StoredCompositeValue {
    type_oid: Option<u32>,
    type_name: String,
    fields: Vec<StoredCompositeField>,
}

#[derive(Serialize, Deserialize)]
struct StoredCompositeField {
    name: String,
    pg_type: String,
    value: StoredTypedSqlValue,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
enum StoredTypedSqlValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    JsonText(String),
    Json(JsonValue),
    Geometry(Geometry),
    TsQuery(String),
    Composite(Box<StoredCompositeValue>),
}

impl StoredCompositeValue {
    fn from_sql(value: SqlComposite) -> Self {
        Self {
            type_oid: value.type_oid,
            type_name: value.type_name,
            fields: value
                .fields
                .into_iter()
                .map(|field| StoredCompositeField {
                    name: field.name,
                    pg_type: field.pg_type,
                    value: StoredTypedSqlValue::from_sql(field.value),
                })
                .collect(),
        }
    }

    fn into_sql(self) -> Result<SqlComposite> {
        Ok(SqlComposite {
            type_oid: self.type_oid,
            type_name: self.type_name,
            fields: self
                .fields
                .into_iter()
                .map(|field| {
                    Ok(SqlCompositeField {
                        name: field.name,
                        pg_type: field.pg_type,
                        value: field.value.into_sql()?,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

impl StoredTypedSqlValue {
    fn from_sql(value: SqlValue) -> Self {
        match value {
            SqlValue::Null => Self::Null,
            SqlValue::Bool(value) => Self::Bool(value),
            SqlValue::Int(value) => Self::Int(value),
            SqlValue::Float(value) => Self::Float(value),
            SqlValue::String(value) => Self::String(value),
            SqlValue::JsonText(value) => Self::JsonText(value.raw),
            SqlValue::Json(value) => Self::Json(value),
            SqlValue::Geometry(value) => Self::Geometry(value),
            SqlValue::TsQuery(value) => Self::TsQuery(value.to_postgres_text()),
            SqlValue::Composite(value) => {
                Self::Composite(Box::new(StoredCompositeValue::from_sql(value)))
            }
        }
    }

    fn into_sql(self) -> Result<SqlValue> {
        Ok(match self {
            Self::Null => SqlValue::Null,
            Self::Bool(value) => SqlValue::Bool(value),
            Self::Int(value) => SqlValue::Int(value),
            Self::Float(value) => SqlValue::Float(value),
            Self::String(value) => SqlValue::String(value),
            Self::JsonText(value) => SqlValue::JsonText(PgJsonText::parse(value)?),
            Self::Json(value) => SqlValue::Json(value),
            Self::Geometry(value) => SqlValue::Geometry(value),
            Self::TsQuery(value) => SqlValue::TsQuery(PgTsQuery::from_postgres_text(&value)?),
            Self::Composite(value) => SqlValue::Composite(value.into_sql()?),
        })
    }
}

pub(crate) fn set_record_column(
    record: &mut Record,
    schema: Option<&TableSchema>,
    column: &str,
    mut value: SqlValue,
) -> Result<()> {
    let schema_column = schema.and_then(|schema| schema.column(column));
    if schema_column.is_some_and(|column| column.primary_key)
        || (schema.is_none() && column.eq_ignore_ascii_case("id"))
    {
        return Err(SqlError::Unsupported(
            "UPDATE of primary key is not supported".to_string(),
        ));
    }
    let storage_column = schema_column
        .map(|column| column.name.as_str())
        .unwrap_or(column);
    if let Some(column_schema) = schema_column {
        value = cast_value_to_column_type(value, column_schema).map_err(|error| match error {
            error @ (SqlError::ConstraintViolation { .. } | SqlError::DataException { .. }) => {
                error
            }
            error => SqlError::TypeMismatch {
                table: schema.map(|schema| schema.name.clone()).unwrap_or_default(),
                column: column_schema.name.clone(),
                expected: column_schema.pg_type.clone(),
                message: format!(
                    "column \"{}\" of relation \"{}\" expects type {}: {error}",
                    column_schema.name,
                    schema
                        .map(|schema| schema.name.as_str())
                        .unwrap_or_default(),
                    column_schema.pg_type
                ),
            },
        })?;
    }
    if schema_column.is_some_and(|column| is_json_pg_type(&column.pg_type))
        && matches!(value, SqlValue::Null)
    {
        remove_json_object_key(&mut record.metadata, storage_column);
        return Ok(());
    }
    if storage_column.eq_ignore_ascii_case("timestamp") {
        record.timestamp = value.as_f64().map(|value| value as i64);
        return Ok(());
    }
    if is_vector_column(schema, storage_column) {
        let vector = sql_value_to_vector(&value)?;
        record.vector = Some(vector.clone());
        set_json_object_value(&mut record.metadata, storage_column, vector_json(&vector));
        return Ok(());
    }
    if schema_column.is_none() && storage_column.eq_ignore_ascii_case("geometry") {
        record.geometry = Some(spatial_geometry("UPDATE geometry", &value)?);
        return Ok(());
    }
    let stored = sql_value_to_column_storage_json(value, schema_column)?;
    set_json_object_value(&mut record.metadata, storage_column, stored);
    Ok(())
}

pub(crate) fn validate_records_for_write(
    db: &BicDb,
    tx: Option<&Transaction>,
    table: &str,
    schema: &TableSchema,
    records: &[Record],
    allow_existing_same_id: bool,
) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }
    for record in records {
        validate_record_local_constraints(table, schema, record)?;
        validate_foreign_keys(db, tx, table, schema, record)?;
    }
    validate_unique_constraints(db, tx, table, schema, records, allow_existing_same_id)?;
    validate_exclusion_constraints(db, table, schema, records, allow_existing_same_id)
}

/// `validate_records_for_write` for rows whose local constraints (NOT NULL,
/// type, CHECK) were verified as they were built (`InsertValuesTemplate`):
/// only the cross-row and cross-table constraints remain.
pub(crate) fn validate_records_for_write_prechecked(
    db: &BicDb,
    tx: Option<&Transaction>,
    table: &str,
    schema: &TableSchema,
    records: &[Record],
    allow_existing_same_id: bool,
) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }
    for record in records {
        validate_foreign_keys(db, tx, table, schema, record)?;
    }
    validate_unique_constraints(db, tx, table, schema, records, allow_existing_same_id)?;
    validate_exclusion_constraints(db, table, schema, records, allow_existing_same_id)
}

/// Validates UPDATE results. `after_records` are all updated rows;
/// `before_after_records` carries (before, after) pairs only when a unique
/// key can change (see `update_needs_before_image`) — the unique-key change
/// check is the sole consumer of the before image here, and an empty pair
/// list means no unique key was assigned, which is exactly the case where
/// that check has nothing to do.
pub(crate) fn validate_updated_records(
    db: &BicDb,
    tx: Option<&Transaction>,
    table: &str,
    schema: &TableSchema,
    after_records: &[Record],
    before_after_records: &[(Record, Record)],
) -> Result<()> {
    if after_records.is_empty() {
        return Ok(());
    }
    for record in after_records {
        validate_record_local_constraints(table, schema, record)?;
        validate_foreign_keys(db, tx, table, schema, record)?;
    }
    if !before_after_records.is_empty() {
        validate_unique_constraints_for_update(db, tx, table, schema, before_after_records)?;
    }
    validate_exclusion_constraints(db, table, schema, after_records, true)
}

pub(crate) fn validate_record_local_constraints(
    table: &str,
    schema: &TableSchema,
    record: &Record,
) -> Result<()> {
    // One decode per column serves both the NOT NULL and the type check
    // (each used to decode every column from storage JSON on its own); the
    // checks still run in their original order so the reported violation is
    // unchanged.
    // A column whose stored JSON already proves both checks (present,
    // non-null, and of a shape the decoder maps onto the column's type) is
    // not decoded at all: `Verified` stands for a decoded value that is
    // non-null and matches. Everything else decodes as before.
    enum Checked {
        Verified,
        Decoded(SqlValue),
    }
    let values = schema
        .columns
        .iter()
        .map(|column| {
            let needs_null = !column.hidden && (!column.nullable || column.primary_key);
            let needs_type = !column.hidden && !column.primary_key;
            (needs_null || needs_type).then(|| {
                if !column.primary_key
                    && !is_json_pg_type(&column.pg_type)
                    && record
                        .metadata
                        .get(&column.name)
                        .is_some_and(|raw| storage_json_proves_local_checks(raw, column))
                {
                    Checked::Verified
                } else {
                    Checked::Decoded(record_column_value(record, schema, &column.name))
                }
            })
        })
        .collect::<Vec<_>>();
    for (column, value) in schema.columns.iter().zip(values.iter()) {
        if column.hidden || (column.nullable && !column.primary_key) {
            continue;
        }
        if matches!(value, Some(Checked::Decoded(SqlValue::Null))) {
            return Err(constraint_violation(
                "23502",
                format!(
                    "null value in column \"{}\" of relation \"{}\" violates not-null constraint",
                    column.name, table
                ),
                Some(table.to_string()),
                Some(column.name.clone()),
                Some(not_null_constraint_name(table, &column.name)),
            ));
        }
    }
    for (column, value) in schema.columns.iter().zip(values.iter()) {
        if column.hidden || column.primary_key {
            continue;
        }
        let Some(Checked::Decoded(value)) = value else {
            continue;
        };
        if matches!(value, SqlValue::Null) || column_value_matches_type(value, column) {
            continue;
        }
        return Err(SqlError::TypeMismatch {
            table: table.to_string(),
            column: column.name.clone(),
            expected: column.pg_type.clone(),
            message: format!(
                "column \"{}\" of relation \"{}\" expects type {}",
                column.name, table, column.pg_type
            ),
        });
    }
    validate_checks(table, schema, record)
}

pub(crate) fn validate_not_null(table: &str, schema: &TableSchema, record: &Record) -> Result<()> {
    for column in &schema.columns {
        if column.hidden {
            continue;
        }
        if column.nullable && !column.primary_key {
            continue;
        }
        if matches!(
            record_column_value(record, schema, &column.name),
            SqlValue::Null
        ) {
            return Err(constraint_violation(
                "23502",
                format!(
                    "null value in column \"{}\" of relation \"{}\" violates not-null constraint",
                    column.name, table
                ),
                Some(table.to_string()),
                Some(column.name.clone()),
                Some(not_null_constraint_name(table, &column.name)),
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_column_types(
    table: &str,
    schema: &TableSchema,
    record: &Record,
) -> Result<()> {
    for column in &schema.columns {
        if column.hidden || column.primary_key {
            continue;
        }
        let value = record_column_value(record, schema, &column.name);
        if matches!(value, SqlValue::Null) || column_value_matches_type(&value, column) {
            continue;
        }
        return Err(SqlError::TypeMismatch {
            table: table.to_string(),
            column: column.name.clone(),
            expected: column.pg_type.clone(),
            message: format!(
                "column \"{}\" of relation \"{}\" expects type {}",
                column.name, table, column.pg_type
            ),
        });
    }
    Ok(())
}

/// Whether `column_value_matches_type` accepts every non-null value for this
/// type (the `_ => true` arm below).
fn column_type_check_is_trivial(pg_type: &str) -> bool {
    !matches!(
        pg_type,
        "bool"
            | "int2"
            | "int4"
            | "int8"
            | "float4"
            | "float8"
            | "numeric"
            | "tsquery"
            | "text"
            | "uuid"
            | "tsvector"
            | "int4range"
            | "numrange"
            | "tsrange"
            | "tstzrange"
            | "daterange"
            | "int8range"
            | "cidr"
            | "inet"
            | "macaddr"
            | "macaddr8"
            | "date"
            | "time"
            | "timetz"
            | "timestamp"
            | "timestamptz"
            | "interval"
            | "bytea"
            | "bit"
            | "varbit"
            | "json"
            | "jsonb"
            | "vector"
    ) && !is_oid_alias_type(pg_type)
}

/// Whether a non-primary-key, non-JSON column's stored JSON proves, without
/// decoding, that `record_column_value` would yield a non-null value that
/// `column_value_matches_type` accepts. Conservative: `false` means "decode
/// and check", never "fails".
pub(crate) fn storage_json_proves_local_checks(raw: &JsonValue, column: &ColumnSchema) -> bool {
    let pg_type = column.pg_type.as_str();
    match raw {
        JsonValue::Null => false,
        JsonValue::Object(_) => {
            let Some(typed) = raw.get(TYPED_STORAGE_KEY) else {
                return false;
            };
            // The envelope fast decoders yield `SqlValue::String`, which the
            // numeric and temporal arms accept.
            if pg_type == "numeric" {
                typed_numeric_text_fast(typed).is_some()
            } else if is_temporal_storage_type(pg_type) {
                typed.get("version").and_then(JsonValue::as_u64)
                    == Some(u64::from(TYPED_STORAGE_VERSION))
                    && typed.get("pg_type").and_then(JsonValue::as_str) == Some(pg_type)
                    && typed.get("value").is_none()
                    && typed.get("text").is_some_and(JsonValue::is_string)
            } else {
                false
            }
        }
        JsonValue::Bool(_) => pg_type == "bool" || column_type_check_is_trivial(pg_type),
        JsonValue::Number(number) => {
            if column_type_check_is_trivial(pg_type) {
                return true;
            }
            if number.is_i64() {
                matches!(
                    pg_type,
                    "int2" | "int4" | "int8" | "float4" | "float8" | "numeric"
                ) || is_oid_alias_type(pg_type)
            } else {
                matches!(pg_type, "float4" | "float8")
            }
        }
        JsonValue::String(_) => pg_type == "text" || column_type_check_is_trivial(pg_type),
        JsonValue::Array(_) => false,
    }
}

pub(crate) fn column_value_matches_type(value: &SqlValue, column: &ColumnSchema) -> bool {
    match column.pg_type.as_str() {
        "bool" => matches!(value, SqlValue::Bool(_)),
        "int2" | "int4" | "int8" => matches!(value, SqlValue::Int(_)),
        "float4" | "float8" => matches!(value, SqlValue::Int(_) | SqlValue::Float(_)),
        "numeric" => matches!(value, SqlValue::Int(_) | SqlValue::String(_)),
        "tsquery" => matches!(value, SqlValue::TsQuery(_) | SqlValue::String(_)),
        pg_type if is_oid_alias_type(pg_type) => matches!(value, SqlValue::Int(_)),
        "text" | "uuid" | "tsvector" | "int4range" | "numrange" | "tsrange" | "tstzrange"
        | "daterange" | "int8range" | "cidr" | "inet" | "macaddr" | "macaddr8" => {
            matches!(value, SqlValue::String(_))
        }
        "date" | "time" | "timetz" | "timestamp" | "timestamptz" => {
            matches!(value, SqlValue::Int(_) | SqlValue::String(_))
        }
        "interval" => matches!(value, SqlValue::String(_)),
        "bytea" => matches!(
            value,
            SqlValue::String(_) | SqlValue::Json(JsonValue::Array(_))
        ),
        "bit" | "varbit" => matches!(value, SqlValue::String(_)),
        "json" => matches!(value, SqlValue::JsonText(_) | SqlValue::Json(_)),
        "jsonb" => matches!(value, SqlValue::Json(_)),
        "vector" => matches!(value, SqlValue::Json(JsonValue::Array(_))),
        _ => true,
    }
}

pub(crate) fn validate_checks(table: &str, schema: &TableSchema, record: &Record) -> Result<()> {
    for constraint in &schema.constraints {
        let ConstraintSchema::Check {
            name, expression, ..
        } = constraint
        else {
            continue;
        };
        let expr = parse_check_expression(expression)?;
        if matches!(
            eval_schema_predicate_truth(record, schema, &expr)?,
            Some(false)
        ) {
            return Err(constraint_violation(
                "23514",
                format!(
                    "new row for relation \"{}\" violates check constraint \"{}\"",
                    table, name
                ),
                Some(table.to_string()),
                None,
                Some(name.clone()),
            ));
        }
    }
    Ok(())
}

pub(crate) fn eval_schema_predicate_truth(
    record: &Record,
    schema: &TableSchema,
    expression: &Expr,
) -> Result<Option<bool>> {
    let mut projected = record.clone();
    for column in schema.columns.iter().filter(|column| !column.hidden) {
        let Some(stored) = json_value_for_key_case_insensitive(&record.metadata, &column.name)
        else {
            continue;
        };
        if stored.get(TYPED_STORAGE_KEY).is_none() {
            continue;
        }
        let value = storage_json_to_sql_value(stored, &column.pg_type);
        set_json_object_value(
            &mut projected.metadata,
            &column.name,
            sql_value_to_json(value),
        );
    }
    eval_predicate_truth(&projected, expression)
}

pub(crate) fn validate_unique_constraints_for_update(
    db: &BicDb,
    tx: Option<&Transaction>,
    table: &str,
    schema: &TableSchema,
    before_after_records: &[(Record, Record)],
) -> Result<()> {
    for arbiter in unique_arbiters_for_table(db, table, schema).iter() {
        let mut changed_records = Vec::new();
        for (before, after) in before_after_records {
            let before_values = unique_key_values(before, schema, &arbiter.key);
            let after_values = unique_key_values(after, schema, &arbiter.key);
            if !unique_key_values_not_distinct(schema, &arbiter.key, &before_values, &after_values)?
            {
                changed_records.push(after.clone());
            }
        }
        if changed_records.is_empty() {
            continue;
        }
        validate_unique_arbiter_records(db, tx, table, schema, &arbiter, &changed_records, true)?;
    }
    Ok(())
}

pub(crate) fn validate_unique_constraints(
    db: &BicDb,
    tx: Option<&Transaction>,
    table: &str,
    schema: &TableSchema,
    records: &[Record],
    allow_existing_same_id: bool,
) -> Result<()> {
    for arbiter in unique_arbiters_for_table(db, table, schema).iter() {
        validate_unique_arbiter_records(
            db,
            tx,
            table,
            schema,
            &arbiter,
            records,
            allow_existing_same_id,
        )?;
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub(crate) struct UniqueConflictArbiter {
    pub(crate) name: String,
    pub(crate) key: UniqueKey,
    pub(crate) index_name: Option<String>,
}

/// Memoized per thread on (schema generation, index generation); see
/// `catalog_memo`. Every INSERT and UPDATE consults this, and the uncached
/// version clones every index definition in the database each time.
pub(crate) fn unique_arbiters_for_table(
    db: &BicDb,
    table: &str,
    schema: &TableSchema,
) -> Arc<Vec<UniqueConflictArbiter>> {
    crate::catalog_memo::unique_arbiters_shared(db, table, schema)
}

pub(crate) fn unique_arbiters_for_table_uncached(
    db: &BicDb,
    table: &str,
    schema: &TableSchema,
) -> Vec<UniqueConflictArbiter> {
    let executable_indexes = db.index_definitions();
    let mut arbiters = Vec::new();
    let primary_key_columns = primary_key_columns_for_schema(schema);
    if !primary_key_columns.is_empty() {
        let key = UniqueKey::Columns(primary_key_columns);
        arbiters.push(UniqueConflictArbiter {
            name: schema.primary_key_constraint_name(),
            index_name: unique_index_name_for_key(table, schema, &key, &executable_indexes),
            key,
        });
    }
    arbiters.extend(
        schema
            .constraints
            .iter()
            .filter_map(|constraint| match constraint {
                ConstraintSchema::Unique { name, columns, .. } => {
                    if unique_constraint_is_primary_key(schema, name, columns) {
                        return None;
                    }
                    let key = UniqueKey::Columns(columns.clone());
                    Some(UniqueConflictArbiter {
                        name: name.clone(),
                        index_name: unique_index_name_for_key(
                            table,
                            schema,
                            &key,
                            &executable_indexes,
                        ),
                        key,
                    })
                }
                _ => None,
            }),
    );
    for index in &schema.indexes {
        if index.unique {
            let key = executable_indexes
                .iter()
                .find(|definition| {
                    definition.collection.eq_ignore_ascii_case(table)
                        && definition.name.eq_ignore_ascii_case(&index.name)
                })
                .map(|definition| UniqueKey::IndexFields(definition.fields.clone()))
                .unwrap_or_else(|| {
                    UniqueKey::Columns(
                        index
                            .expression
                            .split(',')
                            .map(|part| part.trim().trim_matches('"').to_string())
                            .collect::<Vec<_>>(),
                    )
                });
            arbiters.push(UniqueConflictArbiter {
                name: index.name.clone(),
                index_name: unique_index_name_for_key(table, schema, &key, &executable_indexes),
                key,
            });
        }
    }
    arbiters
}

pub(crate) fn unique_arbiter_for_columns(
    db: &BicDb,
    table: &str,
    schema: &TableSchema,
    columns: &[String],
) -> Result<Option<UniqueConflictArbiter>> {
    for arbiter in unique_arbiters_for_table(db, table, schema).iter() {
        if unique_key_matches_columns(schema, &arbiter.key, columns) {
            return Ok(Some(arbiter.clone()));
        }
    }
    Ok(None)
}

pub(crate) fn validate_unique_arbiter_records(
    db: &BicDb,
    tx: Option<&Transaction>,
    table: &str,
    schema: &TableSchema,
    arbiter: &UniqueConflictArbiter,
    records: &[Record],
    allow_existing_same_id: bool,
) -> Result<()> {
    let mut seen = BTreeMap::<Vec<String>, String>::new();
    for record in records {
        let key_values = unique_key_values(record, schema, &arbiter.key);
        if unique_key_has_null(&key_values) {
            continue;
        }
        let key = typed_unique_key_label(schema, &arbiter.key, &key_values)?;
        if let Some(existing_id) = seen.get(&key) {
            if !allow_existing_same_id || existing_id != &record.id {
                return Err(unique_violation(&arbiter.name));
            }
        } else {
            seen.insert(key, record.id.clone());
        }
    }

    if validate_unique_primary_key_arbiter_records(
        db,
        tx,
        table,
        schema,
        arbiter,
        records,
        allow_existing_same_id,
    )? {
        return Ok(());
    }

    let contains_json = records.iter().any(|record| {
        unique_key_values(record, schema, &arbiter.key)
            .iter()
            .any(|value| matches!(value, SqlValue::Json(_)))
    });
    if arbiter.index_name.is_none() || contains_json {
        let existing_records = match tx {
            Some(tx) => tx.scan_collection_for_integrity_check(table)?,
            None => db.scan_collection_unchecked(table)?,
        };
        sql_profile_full_scan();
        let existing = unique_key_record_ids(schema, &arbiter.key, &existing_records)?;
        for record in records {
            let key_values = unique_key_values(record, schema, &arbiter.key);
            if unique_key_has_null(&key_values) {
                continue;
            }
            let key = typed_unique_key_label(schema, &arbiter.key, &key_values)?;
            if existing.get(&key).is_some_and(|ids| {
                ids.iter()
                    .any(|id| !allow_existing_same_id || id != &record.id)
            }) {
                return Err(unique_violation(&arbiter.name));
            }
        }
        return Ok(());
    }
    let index_name = arbiter.index_name.as_ref().expect("checked above");

    for record in records {
        let key_values = unique_key_values(record, schema, &arbiter.key);
        if unique_key_has_null(&key_values) {
            continue;
        }
        sql_profile_index_lookup();
        let index_values = unique_key_index_values(schema, &arbiter.key, &key_values)?;
        let ids = db.lookup_index_exact(index_name, &index_values)?;
        if ids
            .iter()
            .any(|id| !allow_existing_same_id || id != &record.id)
        {
            return Err(unique_violation(&arbiter.name));
        }
        // The index reflects committed state only. A row this same
        // transaction has already written is invisible to it, which is how a
        // duplicate key inserted twice inside one transaction passed
        // validation and silently overwrote its predecessor. The pending set
        // is bounded by the transaction's own size, so this stays cheap.
        if let Some(tx) = tx {
            for pending in tx.pending_records(table) {
                if pending.id == record.id {
                    continue;
                }
                let pending_values = unique_key_values(&pending, schema, &arbiter.key);
                if unique_key_has_null(&pending_values) {
                    continue;
                }
                if typed_unique_key_label(schema, &arbiter.key, &pending_values)?
                    == typed_unique_key_label(schema, &arbiter.key, &key_values)?
                {
                    return Err(unique_violation(&arbiter.name));
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn non_conflicting_records(
    db: &BicDb,
    tx: Option<&Transaction>,
    table: &str,
    schema: &TableSchema,
    arbiter: &UniqueConflictArbiter,
    records: Vec<Record>,
) -> Result<Vec<Record>> {
    if let Some(columns) = primary_key_columns_for_unique_arbiter(schema, arbiter) {
        if primary_key_requires_typed_identity(schema) {
            let existing_records = match tx {
                Some(tx) => tx.scan_collection_for_integrity_check(table)?,
                None => db.scan_collection_unchecked(table)?,
            };
            sql_profile_full_scan();
            return non_conflicting_records_from_scan(schema, arbiter, records, &existing_records);
        }
        return non_conflicting_records_by_primary_key(db, tx, table, schema, &columns, records);
    }

    let contains_json = records.iter().any(|record| {
        unique_key_values(record, schema, &arbiter.key)
            .iter()
            .any(|value| matches!(value, SqlValue::Json(_)))
    });
    if arbiter.index_name.is_none() || contains_json {
        let existing_records = match tx {
            Some(tx) => tx.scan_collection_for_integrity_check(table)?,
            None => db.scan_collection_unchecked(table)?,
        };
        sql_profile_full_scan();
        return non_conflicting_records_from_scan(schema, arbiter, records, &existing_records);
    }
    let index_name = arbiter.index_name.as_ref().expect("checked above");

    let mut seen = BTreeSet::<Vec<String>>::new();
    let mut inserted = Vec::new();
    for record in records {
        let key_values = unique_key_values(&record, schema, &arbiter.key);
        if !unique_key_has_null(&key_values) {
            let key = typed_unique_key_label(schema, &arbiter.key, &key_values)?;
            if !seen.insert(key) {
                continue;
            }
            sql_profile_index_lookup();
            let index_values = unique_key_index_values(schema, &arbiter.key, &key_values)?;
            if !db.lookup_index_exact(index_name, &index_values)?.is_empty() {
                continue;
            }
            // The index is committed state only. A row this transaction has
            // already written conflicts just as much, and ON CONFLICT must
            // skip it rather than leave validation to reject the statement.
            if let Some(tx) = tx {
                let mut pending_conflict = false;
                for pending in tx.pending_records(table) {
                    let pending_values = unique_key_values(&pending, schema, &arbiter.key);
                    if unique_key_has_null(&pending_values) {
                        continue;
                    }
                    if typed_unique_key_label(schema, &arbiter.key, &pending_values)?
                        == typed_unique_key_label(schema, &arbiter.key, &key_values)?
                    {
                        pending_conflict = true;
                        break;
                    }
                }
                if pending_conflict {
                    continue;
                }
            }
        }
        inserted.push(record);
    }
    Ok(inserted)
}

pub(crate) fn validate_unique_primary_key_arbiter_records(
    db: &BicDb,
    tx: Option<&Transaction>,
    table: &str,
    schema: &TableSchema,
    arbiter: &UniqueConflictArbiter,
    records: &[Record],
    allow_existing_same_id: bool,
) -> Result<bool> {
    let Some(columns) = primary_key_columns_for_unique_arbiter(schema, arbiter) else {
        return Ok(false);
    };
    // Typed identity can differ from the legacy text cell. Rows written before
    // typed IDs may therefore have a different physical id for the same key.
    // Let the caller's canonical full-scan path compare logical values.
    if primary_key_requires_typed_identity(schema) {
        return Ok(false);
    }
    for record in records {
        let key_values = record_column_values(record, schema, &columns);
        if unique_key_has_null(&key_values) {
            continue;
        }
        sql_profile_index_lookup();
        let record_id = record_id_from_column_values(table, schema, &columns, &key_values)?;
        // Through the transaction when there is one: `db.get` sees committed
        // state only, so a row inserted earlier in this same transaction was
        // invisible and the duplicate silently replaced it.
        let existing = match tx {
            Some(tx) => tx.get_for_integrity_check(table, &record_id)?,
            None => db.get_unchecked(table, &record_id)?,
        };
        if existing.is_some_and(|existing| !allow_existing_same_id || existing.id != record.id) {
            return Err(unique_violation(&arbiter.name));
        }
    }
    Ok(true)
}

pub(crate) fn non_conflicting_records_by_primary_key(
    db: &BicDb,
    tx: Option<&Transaction>,
    table: &str,
    schema: &TableSchema,
    columns: &[String],
    records: Vec<Record>,
) -> Result<Vec<Record>> {
    let mut seen = BTreeSet::<Vec<String>>::new();
    let mut inserted = Vec::new();
    for record in records {
        let key_values = record_column_values(&record, schema, columns);
        if !unique_key_has_null(&key_values) {
            let key =
                typed_unique_key_label(schema, &UniqueKey::Columns(columns.to_vec()), &key_values)?;
            if !seen.insert(key) {
                continue;
            }
            sql_profile_index_lookup();
            let record_id = record_id_from_column_values(table, schema, columns, &key_values)?;
            let existing = match tx {
                Some(tx) => tx.get_for_integrity_check(table, &record_id)?,
                None => db.get_unchecked(table, &record_id)?,
            };
            if existing.is_some() {
                continue;
            }
        }
        inserted.push(record);
    }
    Ok(inserted)
}

pub(crate) fn primary_key_columns_for_unique_arbiter(
    schema: &TableSchema,
    arbiter: &UniqueConflictArbiter,
) -> Option<Vec<String>> {
    let primary_key_columns = primary_key_columns_for_schema(schema);
    if primary_key_columns.is_empty()
        || !unique_key_matches_columns(schema, &arbiter.key, &primary_key_columns)
    {
        return None;
    }
    Some(primary_key_columns)
}

pub(crate) fn record_id_from_column_values(
    table: &str,
    schema: &TableSchema,
    columns: &[String],
    values: &[SqlValue],
) -> Result<String> {
    if let Some(id) = record_id_from_primary_key_values(schema, columns, values)? {
        return Ok(id);
    }
    let fields = columns
        .iter()
        .cloned()
        .zip(values.iter().cloned())
        .collect::<BTreeMap<_, _>>();
    record_id_from_fields(table, Some(schema), &fields)
}

/// `record_id_from_fields` for the shape every point plan and insert
/// template passes: exactly the primary-key columns, in constraint order,
/// none NULL. Builds the id straight from the slice — the map form cloned
/// every column name and value into a `BTreeMap` per row. `None` for any
/// other shape (or an error case), which the map form then reports.
fn record_id_from_primary_key_values(
    schema: &TableSchema,
    columns: &[String],
    values: &[SqlValue],
) -> Result<Option<String>> {
    if schema.has_hidden_primary_key() || columns.len() != values.len() {
        return Ok(None);
    }
    let primary_key_name = schema.primary_key_constraint_name();
    let Some(primary_key_columns) =
        schema
            .constraints
            .iter()
            .find_map(|constraint| match constraint {
                ConstraintSchema::Unique { name, columns, .. }
                    if name.eq_ignore_ascii_case(&primary_key_name) =>
                {
                    Some(columns.as_slice())
                }
                _ => None,
            })
    else {
        return Ok(None);
    };
    if primary_key_columns.len() != columns.len()
        || !primary_key_columns
            .iter()
            .zip(columns)
            .all(|(expected, given)| expected == given)
    {
        return Ok(None);
    }
    // Ordinary integer primary keys need no typed identity encoding or JSON
    // escaping. Validate their cast widths first, then render directly into
    // the final key instead of allocating a Vec and one String per component.
    let plain_integers = primary_key_columns
        .iter()
        .zip(values)
        .all(|(column, value)| {
            let SqlValue::Int(value) = value else {
                return false;
            };
            let Some(column) = schema.column(column) else {
                return false;
            };
            let kind = column.pg_type.as_str();
            if ["int8", "bigint"]
                .iter()
                .any(|name| kind.eq_ignore_ascii_case(name))
            {
                true
            } else if ["int4", "int", "integer"]
                .iter()
                .any(|name| kind.eq_ignore_ascii_case(name))
            {
                i32::try_from(*value).is_ok()
            } else if ["int2", "smallint"]
                .iter()
                .any(|name| kind.eq_ignore_ascii_case(name))
            {
                i16::try_from(*value).is_ok()
            } else {
                false
            }
        });
    if plain_integers && !values.is_empty() {
        use std::fmt::Write as _;
        if let [SqlValue::Int(value)] = values {
            return Ok(Some(value.to_string()));
        }
        let mut encoded = String::with_capacity(2 + values.len() * 16);
        encoded.push('[');
        for (index, value) in values.iter().enumerate() {
            let SqlValue::Int(value) = value else {
                unreachable!()
            };
            if index != 0 {
                encoded.push(',');
            }
            write!(&mut encoded, "\"{value}\"").expect("formatting into String");
        }
        encoded.push(']');
        return Ok(Some(encoded));
    }
    let mut cells = Vec::with_capacity(primary_key_columns.len());
    for (column, value) in primary_key_columns.iter().zip(values) {
        if matches!(value, SqlValue::Null) {
            return Ok(None);
        }
        let value = match schema.column(column) {
            Some(column_schema) => cast_value_to_column_type(value.clone(), column_schema)?,
            None => value.clone(),
        };
        let cell = primary_key_identity_cell(schema, column, &value)?;
        if cell.is_empty() {
            return Ok(None);
        }
        cells.push(cell);
    }
    if cells.len() == 1 {
        return Ok(cells.pop());
    }
    serde_json::to_string(&cells)
        .map(Some)
        .map_err(SqlError::from)
}

pub(crate) fn primary_key_identity_cell(
    schema: &TableSchema,
    column: &str,
    value: &SqlValue,
) -> Result<String> {
    let column_schema = schema
        .column(column)
        .ok_or_else(|| SqlError::UndefinedColumn {
            table: schema.name.clone(),
            column: column.to_string(),
        })?;
    if !pg_type_requires_typed_identity(&column_schema.pg_type) {
        return Ok(value.to_cell());
    }
    column_typed_index_label(column_schema, value)
}

pub(crate) fn composite_record_id_prefix_from_values(
    schema: &TableSchema,
    primary_key_columns: &[String],
    values: &[SqlValue],
) -> Result<String> {
    if values.len() > primary_key_columns.len() {
        return Err(SqlError::InvalidSql(
            "primary key prefix has more values than columns".to_string(),
        ));
    }
    let cells = primary_key_columns
        .iter()
        .zip(values)
        .map(|(column, value)| primary_key_identity_cell(schema, column, value))
        .collect::<Result<Vec<_>>>()?;
    let encoded = serde_json::to_string(&cells)?;
    Ok(encoded
        .strip_suffix(']')
        .map(|prefix| format!("{prefix},"))
        .unwrap_or(encoded))
}

pub(crate) fn primary_key_prefix_requires_typed_identity(
    schema: &TableSchema,
    prefix_columns: usize,
) -> bool {
    primary_key_columns_for_schema(schema)
        .iter()
        .take(prefix_columns)
        .filter_map(|column| schema.column(column))
        .any(|column| pg_type_requires_typed_identity(&column.pg_type))
}

pub(crate) fn primary_key_requires_typed_identity(schema: &TableSchema) -> bool {
    let columns = primary_key_columns_for_schema(schema);
    primary_key_prefix_requires_typed_identity(schema, columns.len())
}

pub(crate) fn pg_type_requires_typed_identity(pg_type: &str) -> bool {
    pg_type.ends_with("[]")
        || !matches!(
            pg_type,
            "bool" | "int2" | "int4" | "int8" | "oid" | "name" | "text" | "varchar" | "uuid"
        )
}

pub(crate) fn is_unusable_typed_primary_key_index(
    schema: Option<&TableSchema>,
    index: &IndexDefinition,
) -> bool {
    schema.is_some_and(|schema| {
        primary_key_requires_typed_identity(schema)
            && index
                .name
                .eq_ignore_ascii_case(&schema.primary_key_constraint_name())
    })
}

pub(crate) fn non_conflicting_records_from_scan(
    schema: &TableSchema,
    arbiter: &UniqueConflictArbiter,
    records: Vec<Record>,
    existing_records: &[Record],
) -> Result<Vec<Record>> {
    let existing = unique_key_record_ids(schema, &arbiter.key, existing_records)?;
    let mut seen = BTreeSet::<Vec<String>>::new();
    let mut inserted = Vec::new();
    for record in records {
        let key_values = unique_key_values(&record, schema, &arbiter.key);
        if !unique_key_has_null(&key_values) {
            let key = typed_unique_key_label(schema, &arbiter.key, &key_values)?;
            if existing.contains_key(&key) || !seen.insert(key) {
                continue;
            }
        }
        inserted.push(record);
    }
    Ok(inserted)
}

pub(crate) fn unique_key_record_ids(
    schema: &TableSchema,
    key: &UniqueKey,
    records: &[Record],
) -> Result<BTreeMap<Vec<String>, Vec<String>>> {
    let mut ids = BTreeMap::<Vec<String>, Vec<String>>::new();
    for record in records {
        let values = unique_key_values(record, schema, key);
        if unique_key_has_null(&values) {
            continue;
        }
        ids.entry(typed_unique_key_label(schema, key, &values)?)
            .or_default()
            .push(record.id.clone());
    }
    Ok(ids)
}

pub(crate) fn unique_index_name_for_key(
    table: &str,
    schema: &TableSchema,
    key: &UniqueKey,
    indexes: &[IndexDefinition],
) -> Option<String> {
    indexes
        .iter()
        .find(|index| {
            index.unique
                && index.kind == IndexKind::BTree
                && index.collection.eq_ignore_ascii_case(table)
                && unique_key_matches_index(schema, key, index)
        })
        .map(|index| index.name.clone())
}

pub(crate) fn unique_key_matches_index(
    schema: &TableSchema,
    key: &UniqueKey,
    index: &IndexDefinition,
) -> bool {
    match key {
        UniqueKey::Columns(columns) => index_fields_match_columns(schema, &index.fields, columns),
        UniqueKey::IndexFields(fields) => index_field_lists_match(&index.fields, fields),
    }
}

pub(crate) fn unique_key_matches_columns(
    schema: &TableSchema,
    key: &UniqueKey,
    columns: &[String],
) -> bool {
    match key {
        UniqueKey::Columns(key_columns) => identifier_lists_equal(key_columns, columns),
        UniqueKey::IndexFields(fields) => index_fields_match_columns(schema, fields, columns),
    }
}

pub(crate) fn index_fields_match_columns(
    schema: &TableSchema,
    fields: &[IndexField],
    columns: &[String],
) -> bool {
    fields.len() == columns.len()
        && fields.iter().zip(columns.iter()).all(|(field, column)| {
            index_field_column_name(schema, field)
                .as_ref()
                .is_some_and(|name| name.eq_ignore_ascii_case(column))
        })
}

pub(crate) fn index_field_column_name(schema: &TableSchema, field: &IndexField) -> Option<String> {
    match field {
        IndexField::Id => Some(schema.primary_key().to_string()),
        IndexField::MetadataPath(path) if path.len() == 1 => Some(path[0].clone()),
        _ => None,
    }
}

pub(crate) fn identifier_lists_equal(left: &[String], right: &[String]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(left, right)| left.eq_ignore_ascii_case(right))
}

pub(crate) fn unique_key_has_null(values: &[SqlValue]) -> bool {
    values.iter().any(|value| matches!(value, SqlValue::Null))
}

pub(crate) fn typed_unique_key_label(
    schema: &TableSchema,
    key: &UniqueKey,
    values: &[SqlValue],
) -> Result<Vec<String>> {
    let pg_types = unique_key_pg_types(schema, key);
    let column_schemas = unique_key_column_schemas(schema, key);
    values
        .iter()
        .zip(pg_types.into_iter().zip(column_schemas))
        .map(|(value, (pg_type, column))| match (pg_type, column) {
            (_, Some(column)) if column.user_type.is_some() => {
                column_typed_index_label(column, value)
            }
            (Some(pg_type), _) => pg_typed_index_label(&pg_type, value),
            (None, _) => Ok(match value {
                SqlValue::Json(value) => format!("\0json:{}", canonical_json_value_key(value)),
                value => value.to_cell(),
            }),
        })
        .collect()
}

pub(crate) fn typed_column_key_label(
    schema: &TableSchema,
    columns: &[String],
    values: &[SqlValue],
) -> Result<Vec<String>> {
    typed_unique_key_label(schema, &UniqueKey::Columns(columns.to_vec()), values)
}

pub(crate) fn unique_key_index_values(
    schema: &TableSchema,
    key: &UniqueKey,
    values: &[SqlValue],
) -> Result<Vec<IndexValue>> {
    let pg_types = unique_key_pg_types(schema, key);
    let column_schemas = unique_key_column_schemas(schema, key);
    values
        .iter()
        .zip(pg_types.into_iter().zip(column_schemas))
        .map(|(value, (pg_type, column))| {
            if let Some(column) = column.filter(|column| column.user_type.is_some()) {
                return column_typed_index_label(column, value).map(IndexValue::String);
            }
            if pg_type.as_deref().is_some_and(uses_typed_storage) {
                return pg_typed_index_label(pg_type.as_deref().unwrap(), value)
                    .map(IndexValue::String);
            }
            index_value_from_sql(value.clone())
        })
        .collect::<Result<Vec<_>>>()
}

pub(crate) fn unique_key_column_schemas<'a>(
    schema: &'a TableSchema,
    key: &UniqueKey,
) -> Vec<Option<&'a ColumnSchema>> {
    match key {
        UniqueKey::Columns(columns) => columns.iter().map(|column| schema.column(column)).collect(),
        UniqueKey::IndexFields(fields) => fields
            .iter()
            .map(|field| match field {
                IndexField::Id => schema.column(schema.primary_key()),
                IndexField::MetadataPath(path) if path.len() == 1 => schema.column(&path[0]),
                _ => None,
            })
            .collect(),
    }
}

pub(crate) fn unique_key_pg_types(schema: &TableSchema, key: &UniqueKey) -> Vec<Option<String>> {
    match key {
        UniqueKey::Columns(columns) => columns
            .iter()
            .map(|column| schema.column(column).map(|column| column.pg_type.clone()))
            .collect(),
        UniqueKey::IndexFields(fields) => fields
            .iter()
            .map(|field| index_field_pg_type(schema, field))
            .collect(),
    }
}

pub(crate) fn unique_key_values_not_distinct(
    schema: &TableSchema,
    key: &UniqueKey,
    left: &[SqlValue],
    right: &[SqlValue],
) -> Result<bool> {
    if left.len() != right.len() {
        return Ok(false);
    }
    left.iter()
        .zip(right)
        .zip(
            unique_key_pg_types(schema, key)
                .into_iter()
                .zip(unique_key_column_schemas(schema, key)),
        )
        .try_fold(true, |equal, ((left, right), (pg_type, column))| {
            if !equal {
                return Ok(false);
            }
            if let Some(column) = column.filter(|column| column.user_type.is_some()) {
                return column_typed_not_distinct(column, left, right);
            }
            match pg_type {
                Some(pg_type) => pg_typed_not_distinct(&pg_type, left, right),
                None => Ok(values_not_distinct(left, right)),
            }
        })
}

pub(crate) fn index_field_pg_type(schema: &TableSchema, field: &IndexField) -> Option<String> {
    match field {
        IndexField::Id => schema
            .column(schema.primary_key())
            .map(|column| column.pg_type.clone()),
        IndexField::Timestamp => Some("int8".to_string()),
        IndexField::Geometry => None,
        IndexField::MetadataPath(path) if path.len() == 1 => {
            schema.column(&path[0]).map(|column| column.pg_type.clone())
        }
        IndexField::Lower(_) | IndexField::Trim(_) => Some("text".to_string()),
        IndexField::MetadataPath(_) => None,
    }
}

pub(crate) fn uses_typed_storage(pg_type: &str) -> bool {
    pg_type == "numeric"
        || pg_type.ends_with("[]")
        || matches!(
            pg_type,
            "float4"
                | "float8"
                | "money"
                | "bytea"
                | "bit"
                | "varbit"
                | "date"
                | "time"
                | "timetz"
                | "timestamp"
                | "timestamptz"
                | "interval"
        )
        || is_pg_canonical_special_type(pg_type)
}

/// Rewrites records created before versioned typed-storage envelopes existed.
///
/// Each batch commits atomically through the normal record-write path, which
/// also rebuilds durable index entries. The migration is safe to resume: a
/// second run leaves current envelopes untouched and reports zero rewrites.
pub fn migrate_legacy_typed_storage(
    db: &mut BicDb,
    batch_size: usize,
) -> Result<TypedStorageMigrationReport> {
    if batch_size == 0 {
        return Err(SqlError::InvalidSql(
            "typed-storage migration batch size must be greater than zero".to_string(),
        ));
    }

    let schemas = list_schemas(db)?;
    let mut report = TypedStorageMigrationReport::default();
    for schema in schemas {
        if !schema
            .columns
            .iter()
            .any(|column| !column.hidden && uses_typed_storage(&column.pg_type))
        {
            continue;
        }
        let records = db.scan_collection(&schema.name)?;
        report.tables_scanned += 1;
        report.records_scanned += records.len();

        let mut rewritten = Vec::new();
        for mut record in records {
            let values_rewritten = rewrite_legacy_typed_record(&mut record, &schema)?;
            if values_rewritten > 0 {
                report.records_rewritten += 1;
                report.values_rewritten += values_rewritten;
                rewritten.push(record);
            }
        }

        for batch in rewritten.chunks(batch_size) {
            let mut tx = db.begin_transaction()?;
            tx.batch_insert(&schema.name, batch.iter().cloned())?;
            tx.commit()?;
        }
    }
    Ok(report)
}

fn rewrite_legacy_typed_record(record: &mut Record, schema: &TableSchema) -> Result<usize> {
    let Some(metadata) = record.metadata.as_object_mut() else {
        return Ok(0);
    };
    let mut replacements = Vec::new();
    for column in schema.columns.iter().filter(|column| {
        !column.hidden && column.user_type.is_none() && uses_typed_storage(&column.pg_type)
    }) {
        let Some((key, stored)) = metadata
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(&column.name))
            .map(|(key, value)| (key.clone(), value.clone()))
        else {
            continue;
        };
        if stored.is_null() {
            continue;
        }
        let logical = storage_json_to_sql_value(&stored, &column.pg_type);
        let encoded = sql_value_to_storage_json(logical, Some(&column.pg_type))?;
        if encoded.get(TYPED_STORAGE_KEY).is_some() && encoded != stored {
            replacements.push((key, encoded));
        }
    }
    let rewritten = replacements.len();
    for (key, value) in replacements {
        metadata.insert(key, value);
    }
    Ok(rewritten)
}

#[derive(Clone, Debug)]
pub(crate) enum UniqueKey {
    Columns(Vec<String>),
    IndexFields(Vec<IndexField>),
}

pub(crate) fn unique_key_values(
    record: &Record,
    schema: &TableSchema,
    key: &UniqueKey,
) -> Vec<SqlValue> {
    match key {
        UniqueKey::Columns(columns) => record_column_values(record, schema, columns),
        UniqueKey::IndexFields(fields) => fields
            .iter()
            .map(|field| record_index_field_sql_value(record, field))
            .collect(),
    }
}

pub(crate) fn record_index_field_sql_value(record: &Record, field: &IndexField) -> SqlValue {
    match field {
        IndexField::Id => SqlValue::String(record.id.clone()),
        IndexField::Timestamp => record
            .timestamp
            .map(SqlValue::Int)
            .unwrap_or(SqlValue::Null),
        IndexField::Geometry => record
            .geometry
            .as_ref()
            .cloned()
            .map(SqlValue::Geometry)
            .unwrap_or(SqlValue::Null),
        IndexField::MetadataPath(path) => FieldRef::MetadataPath(path.clone())
            .value(record)
            .unwrap_or(SqlValue::Null),
        IndexField::Lower(inner) => match record_index_field_sql_value(record, inner) {
            SqlValue::Null => SqlValue::Null,
            value => SqlValue::String(value.to_cell().to_ascii_lowercase()),
        },
        IndexField::Trim(inner) => match record_index_field_sql_value(record, inner) {
            SqlValue::Null => SqlValue::Null,
            value => SqlValue::String(value.to_cell().trim().to_string()),
        },
    }
}

pub(crate) fn record_index_key_from_sql_record(
    record: &Record,
    fields: &[IndexField],
) -> Result<Vec<IndexValue>> {
    fields
        .iter()
        .map(|field| index_value_from_sql(record_index_field_sql_value(record, field)))
        .collect()
}

pub(crate) fn index_key_matches_prefix_range_and_filters(
    key: &[IndexValue],
    prefix: &[IndexValue],
    lower: Option<&IndexValue>,
    upper: Option<&IndexValue>,
    filters: &[(usize, IndexValue)],
) -> bool {
    if !key.starts_with(prefix) {
        return false;
    }
    let range_idx = prefix.len();
    let Some(value) = key.get(range_idx) else {
        return false;
    };
    if lower.is_some_and(|lower| value < lower) || upper.is_some_and(|upper| value > upper) {
        return false;
    }
    filters
        .iter()
        .all(|(idx, expected)| key.get(*idx) == Some(expected))
}

pub(crate) fn validate_exclusion_constraints(
    db: &BicDb,
    table: &str,
    schema: &TableSchema,
    records: &[Record],
    allow_existing_same_id: bool,
) -> Result<()> {
    for constraint in &schema.constraints {
        let ConstraintSchema::Exclusion {
            name,
            equal_columns,
            range,
            predicate,
            ..
        } = constraint
        else {
            continue;
        };
        let predicate = exclusion_predicate_expr(predicate)?;
        validate_exclusion_record_pairs(
            table,
            schema,
            name,
            equal_columns,
            range,
            predicate.as_ref(),
            records,
            allow_existing_same_id,
        )?;
        let existing_records = db.scan_collection_unchecked(table)?;
        for record in records {
            for existing in &existing_records {
                if allow_existing_same_id && existing.id == record.id {
                    continue;
                }
                if exclusion_record_pair_conflicts(
                    schema,
                    equal_columns,
                    range,
                    predicate.as_ref(),
                    record,
                    existing,
                )? {
                    return Err(exclusion_violation(table, name));
                }
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn validate_exclusion_record_pairs(
    table: &str,
    schema: &TableSchema,
    name: &str,
    equal_columns: &[String],
    range: &Option<ExclusionRangeSchema>,
    predicate: Option<&Expr>,
    records: &[Record],
    allow_same_id: bool,
) -> Result<()> {
    for (idx, record) in records.iter().enumerate() {
        for other in records.iter().skip(idx + 1) {
            if allow_same_id && other.id == record.id {
                continue;
            }
            if exclusion_record_pair_conflicts(
                schema,
                equal_columns,
                range,
                predicate,
                record,
                other,
            )? {
                return Err(exclusion_violation(table, name));
            }
        }
    }
    Ok(())
}

pub(crate) fn exclusion_record_pair_conflicts(
    schema: &TableSchema,
    equal_columns: &[String],
    range: &Option<ExclusionRangeSchema>,
    predicate: Option<&Expr>,
    left: &Record,
    right: &Record,
) -> Result<bool> {
    if !exclusion_record_applies(left, predicate)? || !exclusion_record_applies(right, predicate)? {
        return Ok(false);
    }
    exclusion_records_conflict(schema, equal_columns, range, left, right)
}

pub(crate) fn exclusion_record_applies(record: &Record, predicate: Option<&Expr>) -> Result<bool> {
    match predicate {
        Some(expr) => Ok(matches!(eval_predicate_truth(record, expr)?, Some(true))),
        None => Ok(true),
    }
}

pub(crate) fn exclusion_records_conflict(
    schema: &TableSchema,
    equal_columns: &[String],
    range: &Option<ExclusionRangeSchema>,
    left: &Record,
    right: &Record,
) -> Result<bool> {
    for column in equal_columns {
        let left_value = record_column_value(left, schema, column);
        let right_value = record_column_value(right, schema, column);
        if matches!(left_value, SqlValue::Null) || matches!(right_value, SqlValue::Null) {
            return Ok(false);
        }
        let equal = schema
            .column(column)
            .map(|column| column_typed_not_distinct(column, &left_value, &right_value))
            .transpose()?
            .unwrap_or_else(|| values_not_distinct(&left_value, &right_value));
        if !equal {
            return Ok(false);
        }
    }
    let Some(range) = range else {
        return Ok(true);
    };
    let Some((left_range, range_type)) = exclusion_record_range_value(left, schema, range)? else {
        return Ok(false);
    };
    let Some((right_range, right_type)) = exclusion_record_range_value(right, schema, range)?
    else {
        return Ok(false);
    };
    let operator = exclusion_range_binary_operator(&range.operator)?;
    match eval_range_binary_value(
        left_range,
        &operator,
        right_range,
        Some(&range_type),
        Some(&right_type),
    )? {
        Some(SqlValue::Bool(conflicts)) => Ok(conflicts),
        Some(SqlValue::Null) | None => Ok(false),
        Some(value) => Err(SqlError::InvalidSql(format!(
            "exclusion operator {} returned {} instead of boolean",
            range.operator,
            value.to_cell()
        ))),
    }
}

pub(crate) fn exclusion_record_range_value(
    record: &Record,
    schema: &TableSchema,
    range: &ExclusionRangeSchema,
) -> Result<Option<(SqlValue, String)>> {
    if let Some(range_column) = range.range_column.as_ref() {
        let value = record_column_value(record, schema, range_column);
        if matches!(value, SqlValue::Null) {
            return Ok(None);
        }
        let pg_type = schema
            .column(range_column)
            .map(|column| column.pg_type.clone())
            .unwrap_or_else(|| range.function.clone());
        return Ok(Some((value, pg_type)));
    }

    let start = record_column_value(record, schema, &range.start_column);
    let end = record_column_value(record, schema, &range.end_column);
    if matches!(start, SqlValue::Null) || matches!(end, SqlValue::Null) {
        return Ok(None);
    }
    let value = eval_range_function_value(
        &range.function,
        &[start, end, SqlValue::String(range.bounds.clone())],
        &[None, None, Some("text".to_string())],
    )?
    .ok_or_else(|| {
        SqlError::undefined_function(format!(
            "function {} for exclusion constraint does not exist",
            range.function
        ))
    })?;
    Ok(Some((value, range.function.clone())))
}

pub(crate) fn exclusion_range_binary_operator(operator: &str) -> Result<BinaryOperator> {
    match operator {
        "&&" => Ok(BinaryOperator::PGOverlap),
        "-|-" => Ok(BinaryOperator::Custom("-|-".to_string())),
        other => Err(SqlError::Unsupported(format!(
            "exclusion operator {other} is not supported"
        ))),
    }
}

pub(crate) fn exclusion_predicate_expr(predicate: &Option<String>) -> Result<Option<Expr>> {
    predicate.as_deref().map(parse_check_expression).transpose()
}

pub(crate) fn validate_foreign_keys(
    db: &BicDb,
    tx: Option<&Transaction>,
    table: &str,
    schema: &TableSchema,
    record: &Record,
) -> Result<()> {
    for constraint in &schema.constraints {
        validate_foreign_key_constraint(db, tx, table, schema, record, constraint)?;
    }
    Ok(())
}

pub(crate) fn validate_foreign_key_constraint(
    db: &BicDb,
    tx: Option<&Transaction>,
    table: &str,
    schema: &TableSchema,
    record: &Record,
    constraint: &ConstraintSchema,
) -> Result<()> {
    let ConstraintSchema::ForeignKey {
        name,
        columns,
        foreign_table,
        referred_columns,
        ..
    } = constraint
    else {
        return Ok(());
    };
    let key = record_column_values(record, schema, columns);
    if key.iter().all(|value| matches!(value, SqlValue::Null)) {
        return Ok(());
    }
    let Some(foreign_schema) = load_schema(db, foreign_table)? else {
        return Err(foreign_key_violation(table, name));
    };
    let mut found = false;
    let foreign_records = match tx {
        Some(tx) => tx.scan_collection_for_integrity_check(foreign_table)?,
        None => db.scan_collection_unchecked(foreign_table)?,
    };
    for foreign_record in foreign_records {
        let foreign_key = record_column_values(&foreign_record, &foreign_schema, referred_columns);
        if typed_column_values_not_distinct(&foreign_schema, referred_columns, &foreign_key, &key)?
        {
            found = true;
            break;
        }
    }
    if !found {
        return Err(foreign_key_violation(table, name));
    }
    Ok(())
}

pub(crate) fn typed_column_values_not_distinct(
    schema: &TableSchema,
    columns: &[String],
    left: &[SqlValue],
    right: &[SqlValue],
) -> Result<bool> {
    if left.len() != right.len() || left.len() != columns.len() {
        return Ok(false);
    }
    for ((left, right), column) in left.iter().zip(right).zip(columns) {
        let equal = schema
            .column(column)
            .map(|column| column_typed_not_distinct(column, left, right))
            .transpose()?
            .unwrap_or_else(|| values_not_distinct(left, right));
        if !equal {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) fn record_column_values(
    record: &Record,
    schema: &TableSchema,
    columns: &[String],
) -> Vec<SqlValue> {
    columns
        .iter()
        .map(|column| record_column_value(record, schema, column))
        .collect()
}

pub(crate) fn record_column_value(record: &Record, schema: &TableSchema, column: &str) -> SqlValue {
    if let Some(schema_column) = schema.column(column).filter(|column| !column.hidden) {
        let value =
            record_schema_column_value(record, schema_column, column == schema.primary_key());
        if schema_column.primary_key {
            return value;
        }
        if column == "id" && matches!(value, SqlValue::Null) && !schema.has_hidden_primary_key() {
            return SqlValue::String(record.id.clone());
        }
        return value;
    }
    FieldRef::from_parts(&[column.to_string()])
        .and_then(|field| field.value(record))
        .unwrap_or(SqlValue::Null)
}

pub(crate) fn record_schema_column_value(
    record: &Record,
    column: &ColumnSchema,
    use_record_id_for_primary_key: bool,
) -> SqlValue {
    if column.primary_key {
        if column.pg_type == "json" {
            if let Some(value) = record.metadata.get(&column.name) {
                return storage_json_to_sql_value(value, "json");
            }
        } else if column.pg_type == "jsonb" {
            if let Some(value) = record.metadata.get(&column.name) {
                return SqlValue::Json(value.clone());
            }
            if let Some(value) = legacy_payload_json_column_value(record, &column.name) {
                return value;
            }
            if use_record_id_for_primary_key {
                return cast_primary_key_record_id(record, &column.pg_type);
            }
            return SqlValue::Null;
        }
        let stored = record_json_column_value_typed(record, &column.name, &column.pg_type);
        if !matches!(stored, SqlValue::Null) {
            return stored;
        }
        if use_record_id_for_primary_key {
            return cast_primary_key_record_id(record, &column.pg_type);
        }
        return SqlValue::Null;
    }
    if column.pg_type == "json" {
        return record
            .metadata
            .get(&column.name)
            .map(|value| storage_json_to_sql_value(value, "json"))
            .or_else(|| legacy_payload_json_column_value(record, &column.name))
            .unwrap_or(SqlValue::Null);
    }
    if column.pg_type == "jsonb" {
        return record
            .metadata
            .get(&column.name)
            .cloned()
            .map(SqlValue::Json)
            .or_else(|| legacy_payload_json_column_value(record, &column.name))
            .unwrap_or(SqlValue::Null);
    }
    let stored = record_json_column_value_typed(record, &column.name, &column.pg_type);
    if !matches!(stored, SqlValue::Null) {
        return stored;
    }
    // Schema column names are lowercase in practice; only allocate the folded
    // copy when one actually carries uppercase (this runs per column per row).
    let name_folded: std::borrow::Cow<'_, str> =
        if column.name.bytes().any(|b| b.is_ascii_uppercase()) {
            std::borrow::Cow::Owned(column.name.to_ascii_lowercase())
        } else {
            std::borrow::Cow::Borrowed(column.name.as_str())
        };
    match name_folded.as_ref() {
        "metadata" => SqlValue::Json(
            record
                .metadata
                .get("metadata")
                .cloned()
                .unwrap_or_else(|| record.metadata.clone()),
        ),
        "timestamp" => record
            .timestamp
            .map(SqlValue::Int)
            .unwrap_or(SqlValue::Null),
        "payload" => record
            .payload
            .as_ref()
            .map(|payload| {
                storage_json_to_sql_value(
                    &JsonValue::Array(payload.iter().map(|byte| JsonValue::from(*byte)).collect()),
                    &column.pg_type,
                )
            })
            .unwrap_or(SqlValue::Null),
        "vector" => record
            .vector
            .as_ref()
            .map(|vector| SqlValue::Json(vector_json(vector)))
            .unwrap_or(SqlValue::Null),
        "geometry" => SqlValue::Null,
        _ => {
            if is_vector_column(None, &column.name) {
                record
                    .vector
                    .as_ref()
                    .map(|vector| SqlValue::Json(vector_json(vector)))
                    .unwrap_or(SqlValue::Null)
            } else {
                SqlValue::Null
            }
        }
    }
}

pub(crate) fn record_json_column_value_typed(
    record: &Record,
    column: &str,
    pg_type: &str,
) -> SqlValue {
    record
        .metadata
        .get(column)
        .map(|value| storage_json_to_sql_value(value, pg_type))
        .unwrap_or(SqlValue::Null)
}

pub(crate) fn is_json_pg_type(pg_type: &str) -> bool {
    matches!(pg_type, "json" | "jsonb")
}

pub(crate) fn payload_bytes_json_value(payload: &[u8]) -> SqlValue {
    SqlValue::Json(JsonValue::Array(
        payload.iter().map(|byte| JsonValue::from(*byte)).collect(),
    ))
}

pub(crate) fn legacy_payload_json_column_value(record: &Record, column: &str) -> Option<SqlValue> {
    (column == "payload")
        .then(|| record.payload.as_deref().map(payload_bytes_json_value))
        .flatten()
}

pub(crate) fn json_value_for_key_case_insensitive<'a>(
    value: &'a JsonValue,
    key: &str,
) -> Option<&'a JsonValue> {
    value.get(key).or_else(|| {
        value.as_object().and_then(|object| {
            object.iter().find_map(|(object_key, value)| {
                object_key.eq_ignore_ascii_case(key).then_some(value)
            })
        })
    })
}

pub(crate) fn cast_primary_key_record_id(record: &Record, pg_type: &str) -> SqlValue {
    cast_value_to_pg_type(SqlValue::String(record.id.clone()), pg_type)
        .unwrap_or_else(|_| SqlValue::String(record.id.clone()))
}

pub(crate) fn rls_allows_record_with_schema(
    engine: &SqlEngine,
    table: &str,
    action: PolicyAction,
    record: &Record,
    schema: Option<&TableSchema>,
) -> Result<bool> {
    match prepare_rls_with_schema(
        engine.db_ref(),
        table,
        action,
        schema,
        &engine.session_gucs,
        engine.security_context.as_ref(),
        false,
    )? {
        PreparedRls::Allow => Ok(true),
        PreparedRls::Filter(filter) => {
            let schema = schema.expect("RLS filter requires a table schema");
            filter.allows(engine, schema, record)
        }
    }
}

// A per-statement RLS decision: either the session bypasses policies for this
// table entirely, or rows must pass the prepared policy filter.
pub(crate) enum PreparedRls {
    Allow,
    Filter(RlsFilter),
}

pub(crate) struct RlsFilter {
    table: String,
    permissive: Vec<(String, Expr)>,
    restrictive: Vec<(String, Expr)>,
}

pub(crate) enum RlsRowVerdict {
    Allowed,
    // The failing restrictive policy's name, when one specifically failed;
    // None when no permissive policy admitted the row.
    Denied(Option<String>),
}

fn row_security_disabled(session_gucs: &HashMap<String, String>) -> bool {
    session_gucs
        .get("row_security")
        .is_some_and(|value| value.eq_ignore_ascii_case("off"))
}

pub(crate) fn row_security_disabled_error(table: &str) -> SqlError {
    SqlError::BicDb(BicDbError::Authorization(format!(
        "query would be affected by row-level security policy for table \"{table}\""
    )))
}

impl RlsFilter {
    pub(crate) fn verdict(
        &self,
        engine: &SqlEngine,
        schema: &TableSchema,
        record: &Record,
    ) -> Result<RlsRowVerdict> {
        let row = row_from_record(&self.table, &self.table, Some(schema), record)?;
        let mut admitted = false;
        for (_, expr) in &self.permissive {
            if engine.eval_row_predicate(&row, expr)? {
                admitted = true;
                break;
            }
        }
        if !admitted {
            return Ok(RlsRowVerdict::Denied(None));
        }
        for (name, expr) in &self.restrictive {
            if !engine.eval_row_predicate(&row, expr)? {
                return Ok(RlsRowVerdict::Denied(Some(name.clone())));
            }
        }
        Ok(RlsRowVerdict::Allowed)
    }

    pub(crate) fn allows(
        &self,
        engine: &SqlEngine,
        schema: &TableSchema,
        record: &Record,
    ) -> Result<bool> {
        Ok(matches!(
            self.verdict(engine, schema, record)?,
            RlsRowVerdict::Allowed
        ))
    }
}

pub(crate) fn prepare_rls_with_schema(
    db: &BicDb,
    table: &str,
    action: PolicyAction,
    schema: Option<&TableSchema>,
    session_gucs: &HashMap<String, String>,
    security_context: Option<&SecurityContext>,
    with_check: bool,
) -> Result<PreparedRls> {
    if security_context.is_some_and(|ctx| ctx.bypass_policy.is_some()) {
        return Ok(PreparedRls::Allow);
    }
    let Some(schema) = schema else {
        return Ok(PreparedRls::Allow);
    };
    if !schema.rls_enabled {
        return Ok(PreparedRls::Allow);
    }
    let user = rls_check_user_from_gucs(session_gucs);
    if load_role_schema(db, &user)?.is_some_and(|role| role.superuser || role.bypass_rls) {
        return Ok(PreparedRls::Allow);
    }
    // Roles whose privileges the user holds via (inherited) membership; used
    // for both the owner bypass and TO-role policy applicability. Computed at
    // most once per prepared statement.
    let mut privilege_closure: Option<BTreeSet<String>> = None;
    let owner = normalize_role_name(schema.owner.as_deref().unwrap_or(BOOTSTRAP_ROLE_NAME));
    if !schema.rls_forced {
        let owns = user == owner
            || privilege_closure
                .get_or_insert(role_privilege_closure(db, &user)?)
                .contains(&owner);
        if owns {
            return Ok(PreparedRls::Allow);
        }
    }
    if row_security_disabled(session_gucs) {
        return Err(row_security_disabled_error(table));
    }
    let mut permissive = Vec::new();
    let mut restrictive = Vec::new();
    for policy in &schema.policies {
        if !policy_applies(policy.command, action) {
            continue;
        }
        if !policy.applies_to_public() {
            let closure = match privilege_closure.as_ref() {
                Some(closure) => closure,
                None => {
                    privilege_closure = Some(role_privilege_closure(db, &user)?);
                    privilege_closure.as_ref().expect("closure just inserted")
                }
            };
            let matches_role = policy
                .roles
                .iter()
                .any(|role| closure.contains(&normalize_role_name(role)));
            if !matches_role {
                continue;
            }
        }
        // A policy with no expression for this side contributes nothing,
        // matching PostgreSQL's treatment of NULL polqual/polwithcheck.
        let expression = if with_check {
            policy.check_expr.as_ref().or(policy.using_expr.as_ref())
        } else {
            policy.using_expr.as_ref()
        };
        let Some(expression) = expression else {
            continue;
        };
        let expr = parse_policy_expression(expression)?;
        if policy.permissive {
            permissive.push((policy.name.clone(), expr));
        } else {
            restrictive.push((policy.name.clone(), expr));
        }
    }
    Ok(PreparedRls::Filter(RlsFilter {
        table: table.to_string(),
        permissive,
        restrictive,
    }))
}

pub(crate) fn validate_policy_clauses(
    command: PolicyCommand,
    has_using: bool,
    has_check: bool,
) -> Result<()> {
    if matches!(command, PolicyCommand::Insert) && has_using {
        return Err(SqlError::InvalidSql(
            "only WITH CHECK expression allowed for INSERT".to_string(),
        ));
    }
    if matches!(command, PolicyCommand::Select | PolicyCommand::Delete) && has_check {
        return Err(SqlError::InvalidSql(
            "WITH CHECK cannot be applied to SELECT or DELETE".to_string(),
        ));
    }
    Ok(())
}

// Best-effort creation-time validation of column references, mirroring
// PostgreSQL's plan-time check. Unqualified identifiers and identifiers
// qualified by the policy's table must name real columns; anything inside a
// subquery (which may reference other tables) is left to evaluation.
pub(crate) fn validate_policy_expression_columns(schema: &TableSchema, expr: &Expr) -> Result<()> {
    fn column_exists(schema: &TableSchema, name: &str) -> bool {
        schema
            .columns
            .iter()
            .any(|column| column.name.eq_ignore_ascii_case(name))
    }

    fn walk(schema: &TableSchema, expr: &Expr) -> Result<()> {
        match expr {
            Expr::Identifier(ident) => {
                let name = ident.value.to_ascii_lowercase();
                if matches!(
                    name.as_str(),
                    "current_user" | "session_user" | "current_role"
                ) || column_exists(schema, &name)
                {
                    Ok(())
                } else {
                    Err(SqlError::InvalidSql(format!(
                        "column \"{name}\" does not exist"
                    )))
                }
            }
            Expr::CompoundIdentifier(parts) => {
                if let [qualifier, column] = parts.as_slice() {
                    if qualifier.value.eq_ignore_ascii_case(&schema.name)
                        && !column_exists(schema, &column.value)
                    {
                        return Err(SqlError::InvalidSql(format!(
                            "column {}.{} does not exist",
                            qualifier.value, column.value
                        )));
                    }
                }
                Ok(())
            }
            Expr::BinaryOp { left, right, .. }
            | Expr::IsDistinctFrom(left, right)
            | Expr::IsNotDistinctFrom(left, right) => {
                walk(schema, left)?;
                walk(schema, right)
            }
            Expr::UnaryOp { expr, .. }
            | Expr::Nested(expr)
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::IsTrue(expr)
            | Expr::IsNotTrue(expr)
            | Expr::IsFalse(expr)
            | Expr::IsNotFalse(expr)
            | Expr::Collate { expr, .. } => walk(schema, expr),
            Expr::Cast { expr, .. } => walk(schema, expr),
            Expr::Between {
                expr, low, high, ..
            } => {
                walk(schema, expr)?;
                walk(schema, low)?;
                walk(schema, high)
            }
            Expr::InList { expr, list, .. } => {
                walk(schema, expr)?;
                list.iter().try_for_each(|item| walk(schema, item))
            }
            Expr::Like { expr, pattern, .. }
            | Expr::ILike { expr, pattern, .. }
            | Expr::SimilarTo { expr, pattern, .. } => {
                walk(schema, expr)?;
                walk(schema, pattern)
            }
            Expr::Function(function) => function_args(function)
                .iter()
                .try_for_each(|arg| walk(schema, arg)),
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(operand) = operand {
                    walk(schema, operand)?;
                }
                for when in conditions {
                    walk(schema, &when.condition)?;
                    walk(schema, &when.result)?;
                }
                if let Some(else_result) = else_result {
                    walk(schema, else_result)?;
                }
                Ok(())
            }
            Expr::Tuple(items) => items.iter().try_for_each(|item| walk(schema, item)),
            Expr::InSubquery { expr, .. } => walk(schema, expr),
            // Subqueries may reference other relations; leave them to
            // evaluation-time resolution.
            Expr::Exists { .. } | Expr::Subquery(_) => Ok(()),
            _ => Ok(()),
        }
    }

    walk(schema, expr)
}

// Whether an UPDATE/DELETE clause reads existing row values. When it does,
// PostgreSQL additionally applies SELECT policies to the rows being read.
// Conservative: unknown constructs count as reads.
pub(crate) fn expr_references_stored_columns(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::JsonAccess { .. } => true,
        Expr::Value(_) | Expr::TypedString(_) => false,
        Expr::BinaryOp { left, right, .. }
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right) => {
            expr_references_stored_columns(left) || expr_references_stored_columns(right)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsTrue(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::IsNotFalse(expr)
        | Expr::Collate { expr, .. }
        | Expr::Cast { expr, .. } => expr_references_stored_columns(expr),
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_references_stored_columns(expr)
                || expr_references_stored_columns(low)
                || expr_references_stored_columns(high)
        }
        Expr::InList { expr, list, .. } => {
            expr_references_stored_columns(expr) || list.iter().any(expr_references_stored_columns)
        }
        Expr::Like { expr, pattern, .. }
        | Expr::ILike { expr, pattern, .. }
        | Expr::SimilarTo { expr, pattern, .. } => {
            expr_references_stored_columns(expr) || expr_references_stored_columns(pattern)
        }
        Expr::Function(function) => function_args(function)
            .iter()
            .any(expr_references_stored_columns),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand
                .as_deref()
                .is_some_and(expr_references_stored_columns)
                || conditions.iter().any(|when| {
                    expr_references_stored_columns(&when.condition)
                        || expr_references_stored_columns(&when.result)
                })
                || else_result
                    .as_deref()
                    .is_some_and(expr_references_stored_columns)
        }
        Expr::Tuple(items) => items.iter().any(expr_references_stored_columns),
        _ => true,
    }
}

pub(crate) fn select_items_reference_stored_columns(items: &[SelectItem]) -> bool {
    items.iter().any(|item| match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            expr_references_stored_columns(expr)
        }
        _ => true,
    })
}

pub(crate) fn policy_applies(command: PolicyCommand, action: PolicyAction) -> bool {
    matches!(
        (command, action),
        (PolicyCommand::All, _)
            | (PolicyCommand::Select, PolicyAction::Select)
            | (PolicyCommand::Insert, PolicyAction::Insert)
            | (PolicyCommand::Update, PolicyAction::Update)
            | (PolicyCommand::Delete, PolicyAction::Delete)
    )
}

pub(crate) fn parse_policy_expression(expression: &str) -> Result<Expr> {
    parse_check_expression(expression)
}

pub(crate) fn rls_permission_denied(table: &str) -> SqlError {
    SqlError::BicDb(BicDbError::Authorization(format!(
        "new row violates row-level security policy for table \"{table}\""
    )))
}

pub(crate) fn rls_check_denied(table: &str, verdict: &RlsRowVerdict) -> SqlError {
    match verdict {
        RlsRowVerdict::Denied(Some(policy)) => SqlError::BicDb(BicDbError::Authorization(format!(
            "new row violates row-level security policy \"{policy}\" for table \"{table}\""
        ))),
        _ => rls_permission_denied(table),
    }
}

// PostgreSQL's ON CONFLICT DO UPDATE message variant: the conflicting
// existing row failed the UPDATE policy's USING expression.
pub(crate) fn rls_using_denied(table: &str) -> SqlError {
    SqlError::BicDb(BicDbError::Authorization(format!(
        "new row violates row-level security policy (USING expression) for table \"{table}\""
    )))
}

pub(crate) fn parse_check_expression(expression: &str) -> Result<Expr> {
    let sql = format!("SELECT 1 WHERE {expression}");
    let mut statements = parse_statements(&sql)?;
    let Some(Statement::Query(query)) = statements.pop() else {
        return Err(SqlError::InvalidSql(format!(
            "invalid CHECK expression {expression}"
        )));
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(SqlError::InvalidSql(format!(
            "invalid CHECK expression {expression}"
        )));
    };
    select
        .selection
        .clone()
        .ok_or_else(|| SqlError::InvalidSql(format!("invalid CHECK expression {expression}")))
}

pub(crate) fn constraint_violation(
    sqlstate: &'static str,
    message: String,
    table: Option<String>,
    column: Option<String>,
    constraint: Option<String>,
) -> SqlError {
    SqlError::ConstraintViolation {
        sqlstate,
        message,
        table,
        column,
        constraint,
    }
}

pub(crate) fn unique_violation(name: &str) -> SqlError {
    constraint_violation(
        "23505",
        format!("duplicate key value violates unique constraint \"{name}\""),
        None,
        None,
        Some(name.to_string()),
    )
}

pub(crate) fn exclusion_violation(table: &str, name: &str) -> SqlError {
    constraint_violation(
        "23P01",
        format!("conflicting key value violates exclusion constraint \"{name}\""),
        Some(table.to_string()),
        None,
        Some(name.to_string()),
    )
}

pub(crate) fn foreign_key_violation(table: &str, name: &str) -> SqlError {
    constraint_violation(
        "23503",
        format!(
            "insert or update on table \"{}\" violates foreign key constraint \"{}\"",
            table, name
        ),
        Some(table.to_string()),
        None,
        Some(name.to_string()),
    )
}

pub(crate) fn is_vector_column(schema: Option<&TableSchema>, column: &str) -> bool {
    column.eq_ignore_ascii_case("vector")
        || column.eq_ignore_ascii_case("embedding")
        || schema
            .and_then(|schema| schema.column(column))
            .is_some_and(|column| column.pg_type == "vector")
}

pub(crate) fn sql_value_to_vector(value: &SqlValue) -> Result<Vec<f32>> {
    let vector = match value {
        SqlValue::String(value) => parse_vector_literal(value),
        SqlValue::Json(JsonValue::Array(values)) => values
            .iter()
            .map(|value| {
                value.as_f64().map(|value| value as f32).ok_or_else(|| {
                    SqlError::InvalidSql("vector literal contains non-numeric value".to_string())
                })
            })
            .collect(),
        _ => Err(SqlError::InvalidSql(
            "vector value must be a JSON array or pgvector-style string".to_string(),
        )),
    }?;
    validate_pg_vector(&vector)?;
    Ok(vector)
}

pub(crate) fn validate_pg_vector(vector: &[f32]) -> Result<()> {
    if vector.is_empty() || vector.len() > 16_000 {
        return Err(SqlError::data_exception(
            "22000",
            format!(
                "vector must have 1 to 16000 dimensions, got {}",
                vector.len()
            ),
            Some("vector".to_string()),
        ));
    }
    if vector.iter().any(|value| !value.is_finite()) {
        return Err(SqlError::data_exception(
            "22000",
            "vector value must be finite",
            Some("vector".to_string()),
        ));
    }
    Ok(())
}

pub(crate) fn eval_spatial_function_value(
    name: &str,
    args: &[SqlValue],
    arg_types: &[Option<String>],
) -> Result<Option<SqlValue>> {
    if let Some(value) = eval_geometric_function_value(name, args, arg_types)? {
        return Ok(Some(value));
    }
    let name = name.strip_prefix("public.").unwrap_or(name);
    let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    if let Some(h3_name) = name.strip_prefix("h3_") {
        return eval_h3_function_value(h3_name, args);
    }
    let Some(name) = name.strip_prefix("st_") else {
        return Ok(None);
    };

    let value = match name {
        "point" => {
            require_arg_count("ST_Point", args, 2)?;
            SqlValue::Geometry(Geometry::point(
                spatial_number("ST_Point", &args[0], "lon")?,
                spatial_number("ST_Point", &args[1], "lat")?,
            )?)
        }
        "geomfromtext" | "geometryfromtext" | "geogfromtext" => {
            // Optional second argument is the SRID; only WGS84 is accepted.
            if args.len() == 2 {
                spatial_require_wgs84("ST_GeomFromText", &args[1])?;
            } else {
                require_arg_count("ST_GeomFromText", args, 1)?;
            }
            let SqlValue::String(wkt) = &args[0] else {
                return Err(SqlError::InvalidSql(
                    "ST_GeomFromText expects WKT text".to_string(),
                ));
            };
            SqlValue::Geometry(Geometry::from_wkt(wkt).map_err(SqlError::from)?)
        }
        "geomfromwkb" | "geomfromewkb" | "wkbtosql" => {
            if args.len() == 2 {
                spatial_require_wgs84("ST_GeomFromWKB", &args[1])?;
            } else {
                require_arg_count("ST_GeomFromWKB", args, 1)?;
            }
            let SqlValue::String(hex_text) = &args[0] else {
                return Err(SqlError::InvalidSql(
                    "ST_GeomFromWKB expects hex-encoded WKB text".to_string(),
                ));
            };
            SqlValue::Geometry(Geometry::from_wkb_hex(hex_text).map_err(SqlError::from)?)
        }
        "geomfromgeojson" => {
            require_arg_count("ST_GeomFromGeoJSON", args, 1)?;
            let SqlValue::String(geojson_text) = &args[0] else {
                return Err(SqlError::InvalidSql(
                    "ST_GeomFromGeoJSON expects GeoJSON text".to_string(),
                ));
            };
            SqlValue::Geometry(Geometry::from_geojson_str(geojson_text).map_err(SqlError::from)?)
        }
        // bytea output rides the text protocol as PostGIS-style hex
        // (`\x...`); a first-class binary bytea arrives with pgwire binary
        // geometry columns.
        "asbinary" | "aswkb" => {
            require_arg_count("ST_AsBinary", args, 1)?;
            SqlValue::String(format!(
                "\\x{}",
                hex::encode(spatial_geometry("ST_AsBinary", &args[0])?.to_wkb())
            ))
        }
        "asewkb" => {
            require_arg_count("ST_AsEWKB", args, 1)?;
            SqlValue::String(format!(
                "\\x{}",
                hex::encode(spatial_geometry("ST_AsEWKB", &args[0])?.to_ewkb())
            ))
        }
        "srid" => {
            require_arg_count("ST_SRID", args, 1)?;
            spatial_geometry("ST_SRID", &args[0])?;
            SqlValue::Int(i64::from(bicdb_core::WGS84_SRID))
        }
        "setsrid" => {
            require_arg_count("ST_SetSRID", args, 2)?;
            spatial_require_wgs84("ST_SetSRID", &args[1])?;
            SqlValue::Geometry(spatial_geometry("ST_SetSRID", &args[0])?)
        }
        // ------------------------------------------------------------------
        // H3 grid analytics (h3-pg-compatible hex cell ids as text).
        // Heatmaps fall out of GROUP BY h3_cell(geom, res).
        // ------------------------------------------------------------------
        "astext" => {
            require_arg_count("ST_AsText", args, 1)?;
            SqlValue::String(spatial_geometry("ST_AsText", &args[0])?.to_wkt())
        }
        "asgeojson" => {
            require_arg_count("ST_AsGeoJSON", args, 1)?;
            SqlValue::String(
                spatial_geometry("ST_AsGeoJSON", &args[0])?
                    .to_geojson()
                    .to_string(),
            )
        }
        "distance" => {
            require_arg_count("ST_Distance", args, 2)?;
            let left = spatial_geometry("ST_Distance", &args[0])?;
            let right = spatial_geometry("ST_Distance", &args[1])?;
            SqlValue::Float(spatial_distance_meters(&left, &right)?)
        }
        "dwithin" => {
            require_arg_count("ST_DWithin", args, 3)?;
            let left = spatial_geometry("ST_DWithin", &args[0])?;
            let right = spatial_geometry("ST_DWithin", &args[1])?;
            let meters = spatial_number("ST_DWithin", &args[2], "meters")?;
            if meters < 0.0 {
                return Err(SqlError::InvalidSql(
                    "ST_DWithin meters must be non-negative".to_string(),
                ));
            }
            SqlValue::Bool(spatial_distance_meters(&left, &right)? <= meters)
        }
        "contains" => {
            require_arg_count("ST_Contains", args, 2)?;
            let left = spatial_geometry("ST_Contains", &args[0])?;
            let right = spatial_geometry("ST_Contains", &args[1])?;
            SqlValue::Bool(spatial_contains(&left, &right)?)
        }
        "relate" => {
            require_arg_count("ST_Relate", args, 2)?;
            let (left, right) = spatial_relatable_pair("ST_Relate", args)?;
            SqlValue::String(de9im_string(&left.relate(&right)))
        }
        "touches" | "crosses" | "overlaps" | "disjoint" | "within" | "covers" | "coveredby"
        | "equals" => {
            let function = match name {
                "touches" => "ST_Touches",
                "crosses" => "ST_Crosses",
                "overlaps" => "ST_Overlaps",
                "disjoint" => "ST_Disjoint",
                "within" => "ST_Within",
                "covers" => "ST_Covers",
                "coveredby" => "ST_CoveredBy",
                _ => "ST_Equals",
            };
            require_arg_count(function, args, 2)?;
            let (left, right) = spatial_relatable_pair(function, args)?;
            let matrix = left.relate(&right);
            SqlValue::Bool(match name {
                "touches" => matrix.is_touches(),
                "crosses" => matrix.is_crosses(),
                "overlaps" => matrix.is_overlaps(),
                "disjoint" => matrix.is_disjoint(),
                "within" => matrix.is_within(),
                "covers" => matrix.is_covers(),
                "coveredby" => matrix.is_coveredby(),
                _ => matrix.is_equal_topo(),
            })
        }
        "union" | "intersection" | "difference" | "symdifference" => {
            let function = match name {
                "union" => "ST_Union",
                "intersection" => "ST_Intersection",
                "difference" => "ST_Difference",
                _ => "ST_SymDifference",
            };
            require_arg_count(function, args, 2)?;
            let left = spatial_area_operand(function, &args[0])?;
            let right = spatial_area_operand(function, &args[1])?;
            let operation = match name {
                "union" => geo::OpType::Union,
                "intersection" => geo::OpType::Intersection,
                "difference" => geo::OpType::Difference,
                _ => geo::OpType::Xor,
            };
            let result = left.boolean_op(&right, operation);
            SqlValue::Geometry(multipolygon_to_geometry(result))
        }
        "simplify" => {
            require_arg_count("ST_Simplify", args, 2)?;
            // Tolerance is in the geometry's units — degrees, matching the
            // geography-first model. Documented, like PostGIS geometry mode.
            let tolerance = spatial_number("ST_Simplify", &args[1], "tolerance")?;
            let geometry = spatial_geometry("ST_Simplify", &args[0])?;
            SqlValue::Geometry(match geometry {
                Geometry::LineString(line) => Geometry::LineString(line.simplify(&tolerance)),
                Geometry::Polygon(polygon) => Geometry::Polygon(polygon.simplify(&tolerance)),
                Geometry::MultiLineString(lines) => {
                    Geometry::MultiLineString(lines.simplify(&tolerance))
                }
                Geometry::MultiPolygon(polygons) => {
                    Geometry::MultiPolygon(polygons.simplify(&tolerance))
                }
                other => other,
            })
        }
        "convexhull" => {
            require_arg_count("ST_ConvexHull", args, 1)?;
            let geometry = spatial_geometry("ST_ConvexHull", &args[0])?;
            SqlValue::Geometry(Geometry::Polygon(to_geo_geometry(&geometry).convex_hull()))
        }
        "centroid" => {
            require_arg_count("ST_Centroid", args, 1)?;
            let geometry = spatial_geometry("ST_Centroid", &args[0])?;
            let centroid = to_geo_geometry(&geometry).centroid().ok_or_else(|| {
                SqlError::Unsupported("ST_Centroid of an empty geometry".to_string())
            })?;
            SqlValue::Geometry(Geometry::Point(centroid))
        }
        "area" => {
            require_arg_count("ST_Area", args, 1)?;
            let geometry = spatial_geometry("ST_Area", &args[0])?;
            // Geodesic square meters on the WGS84 ellipsoid. Orientation is
            // normalized first: a clockwise ring would otherwise measure
            // the Earth-complement of its area.
            use geo::orient::{Direction, Orient};
            let area = match to_geo_geometry(&geometry) {
                GeoGeometry::Polygon(polygon) => {
                    polygon.orient(Direction::Default).geodesic_area_unsigned()
                }
                GeoGeometry::MultiPolygon(polygons) => {
                    polygons.orient(Direction::Default).geodesic_area_unsigned()
                }
                other => other.geodesic_area_unsigned(),
            };
            SqlValue::Float(area)
        }
        "length" | "perimeter" => {
            let function = if name == "length" {
                "ST_Length"
            } else {
                "ST_Perimeter"
            };
            require_arg_count(function, args, 1)?;
            let geometry = spatial_geometry(function, &args[0])?;
            SqlValue::Float(spatial_geodesic_length(&geometry, name == "perimeter"))
        }
        "buffer" => {
            require_arg_count("ST_Buffer", args, 2)?;
            let meters = spatial_number("ST_Buffer", &args[1], "meters")?;
            if meters < 0.0 {
                return Err(SqlError::InvalidSql(
                    "ST_Buffer meters must be non-negative".to_string(),
                ));
            }
            let geometry = spatial_geometry("ST_Buffer", &args[0])?;
            SqlValue::Geometry(spatial_point_buffer(&geometry, meters)?)
        }
        "isvalid" => {
            require_arg_count("ST_IsValid", args, 1)?;
            // Structural validity v1 (finite coords, closed rings, minimum
            // point counts) — enforced at construction, so any Geometry
            // value that exists is structurally valid. Full OGC validity
            // (self-intersection detection) arrives with G11 hardening.
            spatial_geometry("ST_IsValid", &args[0])?;
            SqlValue::Bool(true)
        }
        "intersects" => {
            require_arg_count("ST_Intersects", args, 2)?;
            let left = spatial_geometry("ST_Intersects", &args[0])?;
            let right = spatial_geometry("ST_Intersects", &args[1])?;
            SqlValue::Bool(spatial_intersects(&left, &right))
        }
        "envelope" => {
            require_arg_count("ST_Envelope", args, 1)?;
            SqlValue::Geometry(spatial_envelope(&spatial_geometry(
                "ST_Envelope",
                &args[0],
            )?)?)
        }
        _ => return Ok(None),
    };
    Ok(Some(value))
}

/// The road graph a routing function reads, if the call is a routing call.
///
/// The routing functions resolve a graph NAME to the `{graph}_nodes` and
/// `{graph}_edges` collections and scan them raw, so the relation they read is
/// not visible anywhere in the query's FROM clause — which is exactly why no
/// table gate ever fired for them. Callers use this to authorize the graph
/// before evaluating.
pub(crate) fn routing_function_graph(name: &str, args: &[SqlValue]) -> Option<String> {
    let name = name.strip_prefix("public.").unwrap_or(name);
    let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    if !matches!(name, "shortest_path" | "route_distance" | "optimize_route") {
        return None;
    }
    match args.first() {
        Some(SqlValue::String(graph)) if !graph.trim().is_empty() => Some(graph.trim().to_string()),
        // A malformed or missing graph argument still has to be authorized
        // against something; returning None here would hand back the ungated
        // path. The empty name resolves to no readable relation, so the gate
        // denies and the caller never reaches the scan.
        _ => Some(String::new()),
    }
}

pub(crate) fn eval_routing_function_value(
    db: &BicDb,
    name: &str,
    args: &[SqlValue],
) -> Result<Option<SqlValue>> {
    let name = name.strip_prefix("public.").unwrap_or(name);
    let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    match name {
        "shortest_path" => {
            require_arg_count("shortest_path", args, 3)?;
            let graph = routing_graph_name("shortest_path", &args[0])?;
            let start = spatial_geometry("shortest_path", &args[1])?;
            let end = spatial_geometry("shortest_path", &args[2])?;
            let path = db.shortest_path(graph, &start, &end)?;
            Ok(Some(SqlValue::Json(serde_json::to_value(path)?)))
        }
        "route_distance" => {
            require_arg_count("route_distance", args, 3)?;
            let graph = routing_graph_name("route_distance", &args[0])?;
            let start = spatial_geometry("route_distance", &args[1])?;
            let end = spatial_geometry("route_distance", &args[2])?;
            Ok(Some(SqlValue::Float(
                db.route_distance(graph, &start, &end)?,
            )))
        }
        "optimize_route" => {
            require_arg_count("optimize_route", args, 2)?;
            let graph = routing_graph_name("optimize_route", &args[0])?;
            let stops = routing_stop_array("optimize_route", &args[1])?;
            Ok(Some(SqlValue::Json(serde_json::to_value(
                db.optimize_route(graph, &stops)?,
            )?)))
        }
        _ => Ok(None),
    }
}

pub(crate) fn sql_array_value(values: Vec<SqlValue>) -> Result<SqlValue> {
    Ok(SqlValue::Json(
        values
            .into_iter()
            .map(sql_value_to_json)
            .collect::<Vec<_>>()
            .into(),
    ))
}

pub(crate) fn routing_graph_name<'a>(function: &str, value: &'a SqlValue) -> Result<&'a str> {
    match value {
        SqlValue::String(value) if !value.trim().is_empty() => Ok(value.trim()),
        other => Err(SqlError::InvalidSql(format!(
            "{function} graph name must be non-empty text, got {}",
            other.to_cell()
        ))),
    }
}

pub(crate) fn routing_stop_array(function: &str, value: &SqlValue) -> Result<Vec<Geometry>> {
    match value {
        SqlValue::Json(JsonValue::Array(values)) => values
            .iter()
            .enumerate()
            .map(|(index, value)| routing_stop_json(function, index, value))
            .collect(),
        other => Err(SqlError::InvalidSql(format!(
            "{function} stops must be an ARRAY or JSON array of geometry values, got {}",
            other.to_cell()
        ))),
    }
}

pub(crate) fn routing_stop_json(
    function: &str,
    index: usize,
    value: &JsonValue,
) -> Result<Geometry> {
    match value {
        JsonValue::String(value) => spatial_geometry(function, &SqlValue::String(value.clone())),
        JsonValue::Object(_) => spatial_geometry(function, &SqlValue::Json(value.clone())),
        other => Err(SqlError::InvalidSql(format!(
            "{function} stop {index} must be WKT text or GeoJSON geometry, got {other}"
        ))),
    }
}

pub(crate) fn require_arg_count(function: &str, args: &[SqlValue], expected: usize) -> Result<()> {
    if args.len() == expected {
        Ok(())
    } else {
        Err(SqlError::InvalidSql(format!(
            "{function} expects {expected} argument(s), got {}",
            args.len()
        )))
    }
}

pub(crate) fn spatial_number(function: &str, value: &SqlValue, name: &str) -> Result<f64> {
    sql_value_f64(value).ok_or_else(|| {
        SqlError::InvalidSql(format!(
            "{function} {name} must be numeric, got {}",
            value.to_cell()
        ))
    })
}

/// BicDB geometry is geography-first: the only SRID it speaks is WGS84
/// (4326, with 0 meaning "unspecified"). Anything else is refused loudly —
/// silently treating projected coordinates as degrees corrupts data.
fn spatial_require_wgs84(function: &str, value: &SqlValue) -> Result<()> {
    match value {
        SqlValue::Int(srid) if *srid == 0 || *srid == i64::from(bicdb_core::WGS84_SRID) => Ok(()),
        SqlValue::Int(srid) => Err(SqlError::InvalidSql(format!(
            "{function}: unsupported SRID {srid} (BicDB geometry is geographic WGS84/4326)"
        ))),
        other => Err(SqlError::InvalidSql(format!(
            "{function}: SRID must be an integer, got {other:?}"
        ))),
    }
}

pub(crate) fn spatial_geometry(function: &str, value: &SqlValue) -> Result<Geometry> {
    match value {
        SqlValue::Geometry(value) => Ok(value.clone()),
        SqlValue::String(value) => {
            let trimmed = value.trim();
            if trimmed.starts_with('{') {
                Geometry::from_geojson_str(trimmed).map_err(SqlError::from)
            } else {
                Geometry::from_wkt(trimmed).map_err(SqlError::from)
            }
        }
        SqlValue::Json(value) => {
            Geometry::from_geojson_value(value.clone()).map_err(SqlError::from)
        }
        other => Err(SqlError::InvalidSql(format!(
            "{function} expects a geometry value, WKT text, or GeoJSON object, got {}",
            other.to_cell()
        ))),
    }
}

// ---------------------------------------------------------------------------
// H3 grid analytics. Cell ids travel as h3-pg-style hex text; heatmaps and
// density overlays fall out of `GROUP BY h3_cell(geom, res)`.
// ---------------------------------------------------------------------------

fn h3_resolution_arg(function: &str, value: &SqlValue) -> Result<h3o::Resolution> {
    let SqlValue::Int(resolution) = value else {
        return Err(SqlError::InvalidSql(format!(
            "{function} expects an integer resolution 0-15, got {value:?}"
        )));
    };
    u8::try_from(*resolution)
        .ok()
        .and_then(|value| h3o::Resolution::try_from(value).ok())
        .ok_or_else(|| {
            SqlError::InvalidSql(format!(
                "{function}: resolution must be 0-15, got {resolution}"
            ))
        })
}

fn h3_cell_arg(function: &str, value: &SqlValue) -> Result<h3o::CellIndex> {
    let SqlValue::String(text) = value else {
        return Err(SqlError::InvalidSql(format!(
            "{function} expects an H3 cell id as hex text, got {value:?}"
        )));
    };
    text.parse::<h3o::CellIndex>().map_err(|error| {
        SqlError::InvalidSql(format!("{function}: invalid H3 cell `{text}`: {error}"))
    })
}

fn h3_geometry_center(function: &str, value: &SqlValue) -> Result<h3o::LatLng> {
    let geometry = spatial_geometry(function, value)?;
    let point = match &geometry {
        Geometry::Point(point) => *point,
        other => {
            let Some((min, max)) = other.coordinate_bounds() else {
                return Err(SqlError::Unsupported(format!(
                    "{function} of an empty geometry"
                )));
            };
            geo::Point::new((min[0] + max[0]) / 2.0, (min[1] + max[1]) / 2.0)
        }
    };
    h3o::LatLng::new(point.y(), point.x())
        .map_err(|error| SqlError::InvalidSql(format!("{function}: {error}")))
}

const H3_POLYFILL_CELL_CAP: usize = 100_000;
const H3_POLYFILL_ITERATION_CAP: u64 = 1_000_000;

fn eval_h3_function_value(name: &str, args: &[SqlValue]) -> Result<Option<SqlValue>> {
    let value = match name {
        // h3_cell(geometry, resolution) or h3_cell(lon, lat, resolution)
        "cell" | "lat_lng_to_cell" => {
            let (latlng, resolution) = if args.len() == 3 {
                let lon = spatial_number("h3_cell", &args[0], "lon")?;
                let lat = spatial_number("h3_cell", &args[1], "lat")?;
                (
                    h3o::LatLng::new(lat, lon)
                        .map_err(|error| SqlError::InvalidSql(format!("h3_cell: {error}")))?,
                    h3_resolution_arg("h3_cell", &args[2])?,
                )
            } else {
                require_arg_count("h3_cell", args, 2)?;
                (
                    h3_geometry_center("h3_cell", &args[0])?,
                    h3_resolution_arg("h3_cell", &args[1])?,
                )
            };
            SqlValue::String(latlng.to_cell(resolution).to_string())
        }
        "cell_to_boundary" | "boundary" => {
            require_arg_count("h3_cell_to_boundary", args, 1)?;
            let cell = h3_cell_arg("h3_cell_to_boundary", &args[0])?;
            let boundary = cell.boundary();
            let mut ring: Vec<geo::Coord<f64>> = boundary
                .iter()
                .map(|vertex| geo::Coord {
                    x: vertex.lng(),
                    y: vertex.lat(),
                })
                .collect();
            if let Some(first) = ring.first().copied() {
                ring.push(first);
            }
            SqlValue::Geometry(Geometry::Polygon(geo::Polygon::new(
                geo::LineString::new(ring),
                Vec::new(),
            )))
        }
        "cell_to_center" | "center" | "cell_to_lat_lng" => {
            require_arg_count("h3_cell_to_center", args, 1)?;
            let cell = h3_cell_arg("h3_cell_to_center", &args[0])?;
            let center = h3o::LatLng::from(cell);
            SqlValue::Geometry(Geometry::point(center.lng(), center.lat()).map_err(SqlError::from)?)
        }
        "cell_to_parent" | "parent" => {
            require_arg_count("h3_cell_to_parent", args, 2)?;
            let cell = h3_cell_arg("h3_cell_to_parent", &args[0])?;
            let resolution = h3_resolution_arg("h3_cell_to_parent", &args[1])?;
            let parent = cell.parent(resolution).ok_or_else(|| {
                SqlError::InvalidSql(format!(
                    "h3_cell_to_parent: {resolution} is finer than the cell's resolution"
                ))
            })?;
            SqlValue::String(parent.to_string())
        }
        "resolution" | "get_resolution" => {
            require_arg_count("h3_resolution", args, 1)?;
            let cell = h3_cell_arg("h3_resolution", &args[0])?;
            SqlValue::Int(i64::from(u8::from(cell.resolution())))
        }
        "grid_disk" => {
            require_arg_count("h3_grid_disk", args, 2)?;
            let cell = h3_cell_arg("h3_grid_disk", &args[0])?;
            let SqlValue::Int(k) = args[1] else {
                return Err(SqlError::InvalidSql(
                    "h3_grid_disk expects an integer ring count".to_string(),
                ));
            };
            let k = u32::try_from(k).map_err(|_| {
                SqlError::InvalidSql("h3_grid_disk ring count must be non-negative".to_string())
            })?;
            if k > 100 {
                return Err(SqlError::InvalidSql(
                    "h3_grid_disk ring count is capped at 100".to_string(),
                ));
            }
            let cells: Vec<serde_json::Value> = cell
                .grid_disk::<Vec<_>>(k)
                .into_iter()
                .map(|cell| serde_json::Value::String(cell.to_string()))
                .collect();
            SqlValue::String(serde_json::Value::Array(cells).to_string())
        }
        // Center-containment polyfill (H3 semantics): every cell whose
        // center falls inside the polygon, as a JSON array of hex ids.
        "polygon_to_cells" | "polyfill" => {
            require_arg_count("h3_polygon_to_cells", args, 2)?;
            let geometry = spatial_geometry("h3_polygon_to_cells", &args[0])?;
            let resolution = h3_resolution_arg("h3_polygon_to_cells", &args[1])?;
            let polygons: Vec<geo::Polygon<f64>> = match &geometry {
                Geometry::Polygon(polygon) => vec![polygon.clone()],
                Geometry::MultiPolygon(polygons) => polygons.0.clone(),
                Geometry::Envelope(rect) => vec![rect.to_polygon()],
                other => {
                    return Err(SqlError::Unsupported(format!(
                        "h3_polygon_to_cells requires an areal geometry, got {}",
                        spatial_type_name(other)
                    )));
                }
            };
            let Some((min, max)) = geometry.coordinate_bounds() else {
                return Ok(Some(SqlValue::String("[]".to_string())));
            };
            // Sampling pitch: half the seed cell's minimum boundary span.
            let seed = h3o::LatLng::new((min[1] + max[1]) / 2.0, (min[0] + max[0]) / 2.0)
                .map_err(|error| SqlError::InvalidSql(format!("h3_polygon_to_cells: {error}")))?
                .to_cell(resolution);
            let boundary = seed.boundary();
            let (mut blon_min, mut blon_max, mut blat_min, mut blat_max) =
                (f64::MAX, f64::MIN, f64::MAX, f64::MIN);
            for vertex in boundary.iter() {
                blon_min = blon_min.min(vertex.lng());
                blon_max = blon_max.max(vertex.lng());
                blat_min = blat_min.min(vertex.lat());
                blat_max = blat_max.max(vertex.lat());
            }
            let step = ((blon_max - blon_min).min(blat_max - blat_min) / 2.0).max(1e-7);
            let latitude_steps = ((max[1] - min[1]) / step).ceil() + 3.0;
            let longitude_steps = ((max[0] - min[0]) / step).ceil() + 3.0;
            if !latitude_steps.is_finite()
                || !longitude_steps.is_finite()
                || latitude_steps > H3_POLYFILL_ITERATION_CAP as f64
                || longitude_steps > H3_POLYFILL_ITERATION_CAP as f64
                || (latitude_steps * longitude_steps) > H3_POLYFILL_ITERATION_CAP as f64
            {
                return Err(SqlError::InvalidSql(format!(
                    "h3_polygon_to_cells exceeds the {H3_POLYFILL_ITERATION_CAP}-sample work cap; use a smaller extent or coarser resolution"
                )));
            }
            let mut cells = std::collections::BTreeSet::new();
            let mut iterations = 0_u64;
            let mut lat = min[1] - step;
            while lat <= max[1] + step {
                let mut lon = min[0] - step;
                while lon <= max[0] + step {
                    iterations = iterations.saturating_add(1);
                    if iterations > H3_POLYFILL_ITERATION_CAP {
                        return Err(SqlError::InvalidSql(format!(
                            "h3_polygon_to_cells exceeds the {H3_POLYFILL_ITERATION_CAP}-sample work cap; use a smaller extent or coarser resolution"
                        )));
                    }
                    if let Ok(sample) = h3o::LatLng::new(lat, lon) {
                        let cell = sample.to_cell(resolution);
                        if !cells.contains(&cell) {
                            let center = h3o::LatLng::from(cell);
                            let point = geo::Point::new(center.lng(), center.lat());
                            if polygons.iter().any(|polygon| polygon.contains(&point)) {
                                cells.insert(cell);
                                if cells.len() > H3_POLYFILL_CELL_CAP {
                                    return Err(SqlError::InvalidSql(format!(
                                        "h3_polygon_to_cells exceeds the {H3_POLYFILL_CELL_CAP}-cell cap; use a coarser resolution"
                                    )));
                                }
                            }
                        }
                    }
                    lon += step;
                }
                lat += step;
            }
            let cells: Vec<serde_json::Value> = cells
                .into_iter()
                .map(|cell| serde_json::Value::String(cell.to_string()))
                .collect();
            SqlValue::String(serde_json::Value::Array(cells).to_string())
        }
        _ => return Ok(None),
    };
    Ok(Some(value))
}

/// Renders the DE-9IM matrix in PostGIS ST_Relate order
/// (interior/boundary/exterior of A against those of B).
fn de9im_string(matrix: &geo::relate::IntersectionMatrix) -> String {
    use geo::coordinate_position::CoordPos;
    use geo::dimensions::Dimensions;
    let positions = [CoordPos::Inside, CoordPos::OnBoundary, CoordPos::Outside];
    positions
        .iter()
        .flat_map(|left| {
            positions
                .iter()
                .map(|right| match matrix.get(*left, *right) {
                    Dimensions::Empty => 'F',
                    Dimensions::ZeroDimensional => '0',
                    Dimensions::OneDimensional => '1',
                    Dimensions::TwoDimensional => '2',
                })
        })
        .collect()
}

/// Relate (DE-9IM) operates on concrete geo geometries; envelopes become
/// their polygon and collections are refused (topology over heterogeneous
/// collections is ambiguous — PostGIS refuses too).
fn spatial_relatable_pair(
    function: &str,
    args: &[SqlValue],
) -> Result<(GeoGeometry<f64>, GeoGeometry<f64>)> {
    let convert = |value: &SqlValue| -> Result<GeoGeometry<f64>> {
        let geometry = spatial_geometry(function, value)?;
        match &geometry {
            Geometry::GeometryCollection(_) => Err(SqlError::Unsupported(format!(
                "{function} does not operate on GEOMETRYCOLLECTION"
            ))),
            Geometry::Envelope(rect) => Ok(GeoGeometry::Polygon(rect.to_polygon())),
            other => Ok(to_geo_geometry(other)),
        }
    };
    Ok((convert(&args[0])?, convert(&args[1])?))
}

/// Boolean set operations act on areas; points/lines are refused with a
/// clear message rather than silently dropped.
fn spatial_area_operand(function: &str, value: &SqlValue) -> Result<geo::MultiPolygon<f64>> {
    let geometry = spatial_geometry(function, value)?;
    match geometry {
        Geometry::Polygon(polygon) => Ok(geo::MultiPolygon(vec![polygon])),
        Geometry::MultiPolygon(polygons) => Ok(polygons),
        Geometry::Envelope(rect) => Ok(geo::MultiPolygon(vec![rect.to_polygon()])),
        other => Err(SqlError::Unsupported(format!(
            "{function} requires POLYGON/MULTIPOLYGON operands, got {}",
            spatial_type_name(&other)
        ))),
    }
}

fn multipolygon_to_geometry(polygons: geo::MultiPolygon<f64>) -> Geometry {
    if polygons.0.len() == 1 {
        Geometry::Polygon(polygons.0.into_iter().next().unwrap())
    } else {
        Geometry::MultiPolygon(polygons)
    }
}

/// Geodesic meters. `perimeter` measures ring boundaries of areas; plain
/// length measures linear geometries (areas have zero length, matching
/// PostGIS).
fn spatial_geodesic_length(geometry: &Geometry, perimeter: bool) -> f64 {
    match geometry {
        Geometry::LineString(line) => {
            if perimeter {
                0.0
            } else {
                line.length::<geo::Geodesic>()
            }
        }
        Geometry::MultiLineString(lines) => {
            if perimeter {
                0.0
            } else {
                lines.length::<geo::Geodesic>()
            }
        }
        Geometry::Polygon(polygon) => {
            if perimeter {
                polygon.geodesic_perimeter()
            } else {
                0.0
            }
        }
        Geometry::MultiPolygon(polygons) => {
            if perimeter {
                polygons.geodesic_perimeter()
            } else {
                0.0
            }
        }
        Geometry::Envelope(rect) => {
            if perimeter {
                rect.to_polygon().geodesic_perimeter()
            } else {
                0.0
            }
        }
        Geometry::Point(_) | Geometry::MultiPoint(_) => 0.0,
        Geometry::GeometryCollection(members) => members
            .iter()
            .map(|member| spatial_geodesic_length(member, perimeter))
            .sum(),
    }
}

/// v1 buffer: geodesic circles around point-set geometries (64-segment
/// rings via haversine destination). Line/polygon buffering needs a proper
/// offset algorithm and arrives later in the campaign.
fn spatial_point_buffer(geometry: &Geometry, meters: f64) -> Result<Geometry> {
    let Some(points) = point_set_coordinates(geometry) else {
        return Err(SqlError::Unsupported(
            "ST_Buffer currently supports point-set geometries (POINT/MULTIPOINT); line and polygon buffering arrives with a later campaign item".to_string(),
        ));
    };
    let circle = |center: geo::Point<f64>| -> geo::Polygon<f64> {
        // Counter-clockwise ring (decreasing bearing) so geodesic area
        // measures the circle, not the Earth-complement.
        let ring: Vec<geo::Coord<f64>> = (0..=64)
            .map(|step| {
                let bearing = 360.0 - f64::from(step) * 360.0 / 64.0;
                let destination = Haversine::destination(center, bearing, meters);
                geo::Coord {
                    x: destination.x(),
                    y: destination.y(),
                }
            })
            .collect();
        geo::Polygon::new(geo::LineString::new(ring), Vec::new())
    };
    let mut polygons: Vec<geo::Polygon<f64>> = points.into_iter().map(circle).collect();
    if polygons.len() == 1 {
        Ok(Geometry::Polygon(polygons.pop().unwrap()))
    } else {
        // Dissolve overlapping circles into one area.
        let mut union = geo::MultiPolygon(vec![polygons.pop().unwrap()]);
        for polygon in polygons {
            union = union.union(&geo::MultiPolygon(vec![polygon]));
        }
        Ok(multipolygon_to_geometry(union))
    }
}

pub(crate) fn spatial_distance_meters(left: &Geometry, right: &Geometry) -> Result<f64> {
    // Point-set geometries (POINT, MULTIPOINT, collections thereof) have an
    // exact haversine distance: the minimum over point pairs. Distances
    // involving lines/polygons arrive with the G2 constructive layer.
    let (Some(left_points), Some(right_points)) =
        (point_set_coordinates(left), point_set_coordinates(right))
    else {
        return Err(SqlError::Unsupported(
            "ST_Distance supports point-set geometries (POINT/MULTIPOINT) in this version; result units are meters using a spherical Haversine model".to_string(),
        ));
    };
    if left_points.is_empty() || right_points.is_empty() {
        return Err(SqlError::Unsupported(
            "ST_Distance of an empty geometry".to_string(),
        ));
    }
    let mut best = f64::INFINITY;
    for left in &left_points {
        for right in &right_points {
            best = best.min(Haversine::distance(*left, *right));
        }
    }
    Ok(best)
}

/// The point set of a geometry made purely of points, `None` when it
/// contains lines or polygons.
fn point_set_coordinates(geometry: &Geometry) -> Option<Vec<geo::Point<f64>>> {
    match geometry {
        Geometry::Point(point) => Some(vec![*point]),
        Geometry::MultiPoint(points) => Some(points.iter().copied().collect()),
        Geometry::GeometryCollection(members) => {
            let mut points = Vec::new();
            for member in members {
                points.extend(point_set_coordinates(member)?);
            }
            Some(points)
        }
        _ => None,
    }
}

pub(crate) fn spatial_contains(left: &Geometry, right: &Geometry) -> Result<bool> {
    match (left, right) {
        (Geometry::Polygon(poly), Geometry::Point(point)) => Ok(poly.contains(point)),
        (Geometry::Envelope(rect), Geometry::Point(point)) => Ok(rect.contains(point)),
        _ => Err(SqlError::Unsupported(format!(
            "ST_Contains does not support {} containing {} in BicDB Spatial v0.1",
            spatial_type_name(left),
            spatial_type_name(right)
        ))),
    }
}

pub(crate) fn spatial_intersects(left: &Geometry, right: &Geometry) -> bool {
    to_geo_geometry(left).intersects(&to_geo_geometry(right))
}

pub(crate) fn spatial_envelope(geometry: &Geometry) -> Result<Geometry> {
    match geometry {
        Geometry::Envelope(_) => Ok(geometry.clone()),
        Geometry::Point(point) => {
            Geometry::envelope(point.x(), point.y(), point.x(), point.y()).map_err(SqlError::from)
        }
        Geometry::LineString(line) => {
            let rect = line.bounding_rect().ok_or_else(|| {
                SqlError::Unsupported(
                    "ST_Envelope could not compute LINESTRING envelope".to_string(),
                )
            })?;
            Geometry::envelope(rect.min().x, rect.min().y, rect.max().x, rect.max().y)
                .map_err(SqlError::from)
        }
        Geometry::Polygon(poly) => {
            let rect = poly.bounding_rect().ok_or_else(|| {
                SqlError::Unsupported("ST_Envelope could not compute POLYGON envelope".to_string())
            })?;
            Geometry::envelope(rect.min().x, rect.min().y, rect.max().x, rect.max().y)
                .map_err(SqlError::from)
        }
        Geometry::MultiPoint(_)
        | Geometry::MultiLineString(_)
        | Geometry::MultiPolygon(_)
        | Geometry::GeometryCollection(_) => {
            let (min, max) = geometry.coordinate_bounds().ok_or_else(|| {
                SqlError::Unsupported("ST_Envelope of an empty geometry".to_string())
            })?;
            Geometry::envelope(min[0], min[1], max[0], max[1]).map_err(SqlError::from)
        }
    }
}

pub(crate) fn to_geo_geometry(geometry: &Geometry) -> GeoGeometry<f64> {
    match geometry {
        Geometry::Point(value) => GeoGeometry::Point(*value),
        Geometry::LineString(value) => GeoGeometry::LineString(value.clone()),
        Geometry::Polygon(value) => GeoGeometry::Polygon(value.clone()),
        Geometry::Envelope(value) => GeoGeometry::Rect(*value),
        Geometry::MultiPoint(value) => GeoGeometry::MultiPoint(value.clone()),
        Geometry::MultiLineString(value) => GeoGeometry::MultiLineString(value.clone()),
        Geometry::MultiPolygon(value) => GeoGeometry::MultiPolygon(value.clone()),
        Geometry::GeometryCollection(members) => GeoGeometry::GeometryCollection(
            geo::geometry::GeometryCollection(members.iter().map(to_geo_geometry).collect()),
        ),
    }
}

pub(crate) fn spatial_type_name(geometry: &Geometry) -> &'static str {
    match geometry {
        Geometry::Point(_) => "POINT",
        Geometry::LineString(_) => "LINESTRING",
        Geometry::Polygon(_) => "POLYGON",
        Geometry::Envelope(_) => "ENVELOPE",
        Geometry::MultiPoint(_) => "MULTIPOINT",
        Geometry::MultiLineString(_) => "MULTILINESTRING",
        Geometry::MultiPolygon(_) => "MULTIPOLYGON",
        Geometry::GeometryCollection(_) => "GEOMETRYCOLLECTION",
    }
}

pub(crate) fn parse_vector_literal(value: &str) -> Result<Vec<f32>> {
    let parsed: JsonValue = serde_json::from_str(value).map_err(|error| {
        SqlError::InvalidSql(format!("invalid vector literal {value:?}: {error}"))
    })?;
    let JsonValue::Array(values) = parsed else {
        return Err(SqlError::InvalidSql(
            "vector literal must be a JSON array".to_string(),
        ));
    };
    values
        .iter()
        .map(|value| {
            value.as_f64().map(|value| value as f32).ok_or_else(|| {
                SqlError::InvalidSql("vector literal contains non-numeric value".to_string())
            })
        })
        .collect()
}

pub(crate) fn vector_json(vector: &[f32]) -> JsonValue {
    JsonValue::Array(
        vector
            .iter()
            .filter_map(|value| serde_json::Number::from_f64(*value as f64))
            .map(JsonValue::Number)
            .collect(),
    )
}

pub(crate) fn sql_value_to_json(value: SqlValue) -> JsonValue {
    match value {
        SqlValue::Null => JsonValue::Null,
        SqlValue::Bool(value) => JsonValue::Bool(value),
        SqlValue::Int(value) => JsonValue::from(value),
        SqlValue::Float(value) => JsonValue::from(value),
        SqlValue::String(value) => JsonValue::String(value),
        SqlValue::TsQuery(value) => JsonValue::String(value.to_postgres_text()),
        SqlValue::JsonText(value) => value.parsed,
        SqlValue::Json(value) => value,
        SqlValue::Geometry(value) => value.to_geojson_value(),
        SqlValue::Composite(value) => JsonValue::Object(
            value
                .fields
                .into_iter()
                .map(|field| (field.name, sql_value_to_json(field.value)))
                .collect(),
        ),
    }
}

/// The typed storage envelope for a value: built once, rendered either as
/// a `JsonValue` (the record-building paths) or straight into bytes (the
/// stored-form INSERT/UPDATE paths, which used to build the `Value` tree —
/// two maps and a `String` per key per cell — only to serialize it).
pub(crate) enum StorageEnvelope {
    Plain(JsonValue),
    Json(StoredJsonTextValue),
    Numeric(StoredNumericValue),
    Scalar(StoredScalarValue),
    Array(StoredArrayValue),
    Binary(StoredBinaryValue),
    Temporal(StoredTemporalValue),
    Special(StoredSpecialValue),
    UserType(StoredUserTypeValue),
}

#[derive(Serialize)]
struct TypedEnvelope<'a, T: Serialize> {
    #[serde(rename = "$bicdb_typed")]
    typed: &'a T,
}

impl StorageEnvelope {
    /// The `JsonValue` the record paths store (unchanged bytes on the wire).
    pub(crate) fn into_json(self) -> JsonValue {
        fn wrap<T: Serialize>(stored: T) -> JsonValue {
            serde_json::json!({
                (TYPED_STORAGE_KEY): serde_json::to_value(stored).expect("storage is serializable")
            })
        }
        match self {
            StorageEnvelope::Plain(value) => value,
            StorageEnvelope::Json(stored) => wrap(stored),
            StorageEnvelope::Numeric(stored) => wrap(stored),
            StorageEnvelope::Scalar(stored) => wrap(stored),
            StorageEnvelope::Array(stored) => wrap(stored),
            StorageEnvelope::Binary(stored) => wrap(stored),
            StorageEnvelope::Temporal(stored) => wrap(stored),
            StorageEnvelope::Special(stored) => wrap(stored),
            StorageEnvelope::UserType(stored) => wrap(stored),
        }
    }

    /// The same bytes `into_json().to_string()` would produce, written
    /// directly (serde serializes the struct in field order, exactly as the
    /// `Value` it used to become did).
    pub(crate) fn write_to(&self, out: &mut Vec<u8>) -> Result<()> {
        fn write<T: Serialize>(out: &mut Vec<u8>, stored: &T) -> Result<()> {
            serde_json::to_writer(out, &TypedEnvelope { typed: stored })
                .map_err(|error| SqlError::InvalidSql(format!("storage envelope: {error}")))
        }
        match self {
            StorageEnvelope::Plain(value) => serde_json::to_writer(out, value)
                .map_err(|error| SqlError::InvalidSql(format!("storage value: {error}"))),
            StorageEnvelope::Json(stored) => write(out, stored),
            StorageEnvelope::Numeric(stored) => write(out, stored),
            StorageEnvelope::Scalar(stored) => write(out, stored),
            StorageEnvelope::Array(stored) => write(out, stored),
            StorageEnvelope::Binary(stored) => write(out, stored),
            StorageEnvelope::Temporal(stored) => write(out, stored),
            StorageEnvelope::Special(stored) => write(out, stored),
            StorageEnvelope::UserType(stored) => write(out, stored),
        }
    }
}

macro_rules! envelope_from {
    ($($variant:ident($ty:ty)),* $(,)?) => {
        $(impl From<$ty> for StorageEnvelope {
            fn from(stored: $ty) -> Self {
                StorageEnvelope::$variant(stored)
            }
        })*
    };
}
envelope_from!(
    Json(StoredJsonTextValue),
    Numeric(StoredNumericValue),
    Scalar(StoredScalarValue),
    Array(StoredArrayValue),
    Binary(StoredBinaryValue),
    Temporal(StoredTemporalValue),
    Special(StoredSpecialValue),
    UserType(StoredUserTypeValue),
);

pub(crate) fn sql_value_to_storage_json(
    value: SqlValue,
    pg_type: Option<&str>,
) -> Result<JsonValue> {
    storage_envelope(value, pg_type).map(StorageEnvelope::into_json)
}

pub(crate) fn storage_envelope(value: SqlValue, pg_type: Option<&str>) -> Result<StorageEnvelope> {
    if matches!(value, SqlValue::Null) {
        return Ok(StorageEnvelope::Plain(JsonValue::Null));
    }
    if pg_type == Some("json") {
        let text = value.to_cell();
        PgJsonText::parse(text.clone()).map_err(|_| {
            SqlError::invalid_text_representation(
                "json",
                format!("invalid input syntax for type json: \"{text}\""),
            )
        })?;
        let stored = StoredJsonTextValue {
            version: TYPED_STORAGE_VERSION,
            pg_type: "json".to_string(),
            text,
        };
        return Ok(StorageEnvelope::from(stored));
    }
    if pg_type == Some("numeric") {
        let text = value.to_cell();
        // Canonical text (what the typmod cast just rendered) needs neither
        // the parse nor the renders below: the stored text is itself and the
        // key comes straight from the digits.
        if matches!(value, SqlValue::String(_) | SqlValue::Int(_)) {
            if let Some(index_key) = crate::type_codec::numeric_index_key_from_canonical_text(&text)
            {
                let stored = StoredNumericValue {
                    version: TYPED_STORAGE_VERSION,
                    pg_type: "numeric".to_string(),
                    value: None,
                    text: Some(text),
                    index_key,
                };
                return Ok(StorageEnvelope::from(stored));
            }
        }
        let parsed = PgNumeric::from_postgres_text(&text);
        // The codec's key comes from re-parsing the cast's rendering of the
        // text. When the parsed number renders back to exactly this text, that
        // re-parse is this parse: derive the key from it. Anything else
        // (non-canonical spellings, floats, parse errors) keeps the codec path
        // and its errors.
        let index_key = match (&parsed, &value) {
            (Ok(numeric), SqlValue::String(_) | SqlValue::Int(_))
                if numeric.to_decimal_text() == text =>
            {
                pg_typed_index_key_for_canonical(
                    "numeric",
                    &PgCanonicalValue::Numeric(numeric.clone()),
                )?
            }
            _ => pg_typed_index_key("numeric", &value)?,
        };
        let numeric = parsed.map_err(|_| {
            SqlError::invalid_text_representation(
                "numeric",
                format!("invalid input syntax for type numeric: \"{text}\""),
            )
        })?;
        let stored = StoredNumericValue {
            version: TYPED_STORAGE_VERSION,
            pg_type: "numeric".to_string(),
            value: None,
            text: Some(numeric.to_decimal_text()),
            index_key,
        };
        return Ok(StorageEnvelope::from(stored));
    }
    if let Some(pg_type @ ("float4" | "float8" | "money")) = pg_type {
        let canonical = if pg_type == "money" {
            PgCanonicalValue::Money(crate::pg_money_cents_from_text(&value.to_cell()).map_err(
                |_| {
                    SqlError::invalid_text_representation(
                        pg_type,
                        format!(
                            "invalid input syntax for type money: \"{}\"",
                            value.to_cell()
                        ),
                    )
                },
            )?)
        } else {
            let float = value.as_f64().ok_or_else(|| {
                SqlError::invalid_text_representation(
                    pg_type,
                    format!(
                        "invalid input syntax for type {pg_type}: \"{}\"",
                        value.to_cell()
                    ),
                )
            })?;
            if pg_type == "float4" {
                PgCanonicalValue::Float4(PgFloat4::from_value(float as f32))
            } else {
                PgCanonicalValue::Float8(PgFloat8::from_value(float))
            }
        };
        let stored = StoredScalarValue {
            version: TYPED_STORAGE_VERSION,
            pg_type: pg_type.to_string(),
            value: canonical,
            index_key: pg_typed_index_key(pg_type, &value)?,
        };
        return Ok(StorageEnvelope::from(stored));
    }
    if let Some(pg_type) = pg_type.filter(|pg_type| pg_type.ends_with("[]")) {
        let index_key = pg_typed_index_key(pg_type, &value)?;
        let (legacy, array) = canonical_array_from_sql(value, pg_type)?;
        let stored = StoredArrayValue {
            version: TYPED_STORAGE_VERSION,
            pg_type: pg_type.to_string(),
            value: array,
            legacy,
            index_key,
        };
        return Ok(StorageEnvelope::from(stored));
    }
    if let Some(pg_type @ ("bytea" | "bit" | "varbit")) = pg_type {
        let text = value.to_cell();
        let index_key = pg_typed_index_key(pg_type, &value)?;
        let (canonical, text) = match pg_type {
            "bytea" => {
                let bytes = parse_bytea_text(&text).map_err(|_| {
                    SqlError::invalid_text_representation(
                        "bytea",
                        format!("invalid input syntax for type bytea: \"{text}\""),
                    )
                })?;
                let text = format_bytea_hex(&bytes);
                (PgCanonicalValue::Bytes(bytes), text)
            }
            "bit" | "varbit" => {
                let bits = PgBitString::from_bit_text(&text).map_err(|_| {
                    SqlError::invalid_text_representation(
                        pg_type,
                        format!("invalid input syntax for type {pg_type}: \"{text}\""),
                    )
                })?;
                let text = bits.to_bit_text();
                (PgCanonicalValue::BitString(bits), text)
            }
            _ => unreachable!(),
        };
        let stored = StoredBinaryValue {
            version: TYPED_STORAGE_VERSION,
            pg_type: pg_type.to_string(),
            value: canonical,
            text,
            index_key,
        };
        return Ok(StorageEnvelope::from(stored));
    }
    if let Some(pg_type @ ("date" | "time" | "timetz" | "timestamp" | "timestamptz" | "interval")) =
        pg_type
    {
        let input_text = value.to_cell();
        let canonical = match pg_type {
            "date" => PgDate::from_postgres_text(&input_text).map(PgCanonicalValue::Date),
            "time" => PgTime::from_postgres_text(&input_text).map(PgCanonicalValue::Time),
            "timetz" => PgTimeTz::from_postgres_text(&input_text, 0).map(PgCanonicalValue::TimeTz),
            "timestamp" => {
                PgTimestamp::from_postgres_text(&input_text, false).map(PgCanonicalValue::Timestamp)
            }
            "timestamptz" => parse_timestamptz(&input_text).map(PgCanonicalValue::TimestampTz),
            "interval" => {
                PgInterval::from_postgres_text(&input_text).map(PgCanonicalValue::Interval)
            }
            _ => unreachable!(),
        };
        // One parse serves both the index key and the stored text: for these
        // types the codec's own parse (`canonical_value`) is the parse above,
        // so the key is identical. `date` parses differently in the codec
        // (ISO-only) and a failed parse must surface the codec's error, so
        // both keep the codec path.
        let index_key = match &canonical {
            Ok(canonical) if pg_type != "date" => {
                pg_typed_index_key_for_canonical(pg_type, canonical)?
            }
            _ => pg_typed_index_key(pg_type, &value)?,
        };
        let canonical = canonical.map_err(|error| {
            let message = format!("invalid input syntax for type {pg_type}: \"{input_text}\"");
            match error {
                PgCanonicalValueError::TemporalFieldOverflow(_)
                | PgCanonicalValueError::TemporalOverflow(_) => {
                    SqlError::data_exception("22008", message, Some(pg_type.to_string()))
                }
                _ if pg_type == "date" => SqlError::invalid_datetime_format(message),
                _ => SqlError::invalid_text_representation(pg_type, message),
            }
        })?;
        let text = match &canonical {
            PgCanonicalValue::Date(value) => value.to_iso_text(),
            PgCanonicalValue::Time(value) => value.to_iso_text(),
            PgCanonicalValue::TimeTz(value) => value.to_iso_text(),
            PgCanonicalValue::Timestamp(value) => value.to_iso_text(false),
            PgCanonicalValue::TimestampTz(value) => value.to_iso_text(true),
            PgCanonicalValue::Interval(value) => value.to_postgres_text(),
            _ => input_text.trim().to_string(),
        };
        let stored = StoredTemporalValue {
            version: TYPED_STORAGE_VERSION,
            pg_type: pg_type.to_string(),
            value: None,
            text,
            index_key,
        };
        return Ok(StorageEnvelope::from(stored));
    }
    if let Some(pg_type) = pg_type {
        let text = value.to_cell();
        let index_key = is_pg_canonical_special_type(pg_type)
            .then(|| pg_typed_index_key(pg_type, &value))
            .transpose()?;
        let canonical = match (&value, pg_type) {
            (SqlValue::TsQuery(query), "tsquery") => Some(PgCanonicalValue::TsQuery(query.clone())),
            _ => parse_pg_canonical_special(pg_type, &text).map_err(|_| {
                SqlError::invalid_text_representation(
                    pg_type,
                    format!("invalid input syntax for type {pg_type}: \"{text}\""),
                )
            })?,
        };
        if let Some(canonical) = canonical {
            let text = match &canonical {
                PgCanonicalValue::Network(network) => network.to_postgres_text(),
                PgCanonicalValue::MacAddress(address) => address.to_postgres_text(),
                PgCanonicalValue::Geometric(geometry) => geometry.to_postgres_text(),
                PgCanonicalValue::Range(range) => range.to_postgres_text(),
                PgCanonicalValue::Multirange(ranges) => format_pg_multirange(ranges),
                _ => text,
            };
            let stored = StoredSpecialValue {
                version: TYPED_STORAGE_VERSION,
                pg_type: pg_type.to_string(),
                value: canonical,
                text,
                index_key: index_key.expect("special types have typed index keys"),
            };
            return Ok(StorageEnvelope::from(stored));
        }
    }
    Ok(StorageEnvelope::Plain(sql_value_to_json(value)))
}

pub(crate) fn sql_value_to_column_storage_json(
    value: SqlValue,
    column: Option<&ColumnSchema>,
) -> Result<JsonValue> {
    column_storage_envelope(value, column).map(StorageEnvelope::into_json)
}

/// `sql_value_to_column_storage_json` written straight into `out` — the
/// stored-form INSERT/UPDATE paths' form (no `Value` tree, no `to_string`).
pub(crate) fn sql_value_write_column_storage(
    out: &mut Vec<u8>,
    value: SqlValue,
    column: Option<&ColumnSchema>,
) -> Result<()> {
    column_storage_envelope(value, column)?.write_to(out)
}

pub(crate) fn column_storage_envelope(
    value: SqlValue,
    column: Option<&ColumnSchema>,
) -> Result<StorageEnvelope> {
    if matches!(value, SqlValue::Null) {
        return Ok(StorageEnvelope::Plain(JsonValue::Null));
    }
    if let Some(column) = column.filter(|column| column.user_type.is_some()) {
        let composite = match &value {
            SqlValue::Composite(composite) => {
                Some(StoredCompositeValue::from_sql(composite.clone()))
            }
            _ => None,
        };
        let stored = StoredUserTypeValue {
            version: TYPED_STORAGE_VERSION,
            pg_type: column.pg_type.clone(),
            type_oid: column.type_oid(),
            index_key: column_typed_storage_key(column, &value)?,
            value: sql_value_to_json(value),
            composite,
        };
        return Ok(StorageEnvelope::from(stored));
    }
    storage_envelope(value, column.map(|column| column.pg_type.as_str()))
}

pub(crate) fn canonical_array_from_sql(
    value: SqlValue,
    pg_type: &str,
) -> Result<(JsonValue, PgArray)> {
    let element_type = pg_type.trim_end_matches("[]");
    let (legacy, lower_bounds) = array_storage_input(value, pg_type)?;
    let (lengths, flattened) = flatten_array_json(&legacy).ok_or_else(|| {
        SqlError::InvalidTextRepresentation(
            "multidimensional arrays must have matching dimensions".to_string(),
        )
    })?;
    let lower_bounds = lower_bounds.unwrap_or_else(|| vec![1; lengths.len()]);
    if lower_bounds.len() != lengths.len() {
        return Err(SqlError::InvalidTextRepresentation(format!(
            "array lower bounds have {} dimensions but contents have {}",
            lower_bounds.len(),
            lengths.len()
        )));
    }
    let dimensions = lengths
        .into_iter()
        .zip(lower_bounds)
        .map(|(length, lower_bound)| PgArrayDimension {
            lower_bound,
            length,
        })
        .collect();
    let elements = flattened
        .into_iter()
        .map(|value| canonical_array_element(value, element_type))
        .collect::<Result<Vec<_>>>()?;
    let array = PgArray::new(element_type, dimensions, elements).map_err(|error| {
        SqlError::InvalidTextRepresentation(format!("invalid {pg_type} value: {error}"))
    })?;
    Ok((legacy, array))
}

/// Decodes a `$bicdb_typed` NUMERIC envelope straight from its JSON fields,
/// without cloning the subtree or running serde's tagged-enum machinery.
/// Returns None for anything but a version-1 finite numeric so the generic
/// (serde) path below keeps authority over every other shape.
fn typed_numeric_text_fast(typed: &JsonValue) -> Option<String> {
    if typed.get("version")?.as_u64()? != u64::from(TYPED_STORAGE_VERSION)
        || typed.get("pg_type")?.as_str()? != "numeric"
    {
        return None;
    }
    if let Some(text) = typed.get("text").and_then(JsonValue::as_str) {
        return Some(text.to_string());
    }
    let inner = typed.get("value")?;
    if inner.get("kind")?.as_str()? != "finite" {
        return None;
    }
    let coefficient = inner.get("coefficient")?.as_str()?;
    if coefficient.is_empty() || !coefficient.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let display_scale = i32::try_from(inner.get("display_scale")?.as_i64()?).ok()?;
    let negative = inner.get("negative")?.as_bool()?;
    // Mirrors PgNumeric::to_decimal_text.
    let rendered = if coefficient == "0" && display_scale <= 0 {
        "0".to_string()
    } else if display_scale == 0 {
        coefficient.to_string()
    } else if display_scale < 0 {
        format!(
            "{coefficient}{}",
            "0".repeat(display_scale.unsigned_abs() as usize)
        )
    } else {
        let scale = display_scale as usize;
        if coefficient.len() <= scale {
            format!("0.{}{coefficient}", "0".repeat(scale - coefficient.len()))
        } else {
            let split = coefficient.len() - scale;
            format!("{}.{}", &coefficient[..split], &coefficient[split..])
        }
    };
    Some(if negative {
        format!("-{rendered}")
    } else {
        rendered
    })
}

pub(crate) fn is_temporal_storage_type(pg_type: &str) -> bool {
    matches!(
        pg_type,
        "date" | "time" | "timetz" | "timestamp" | "timestamptz" | "interval"
    )
}

/// Renders the persisted temporal text for the session. Stored text is
/// session-independent (UTC instants, PostgreSQL interval text); timestamptz
/// and interval are re-rendered in the current time zone / IntervalStyle.
pub(crate) fn temporal_storage_text_to_display(pg_type: &str, text: String) -> String {
    match pg_type {
        "timestamptz" => parse_timestamptz_in_zone(&text, "UTC")
            .map(render_timestamptz)
            .unwrap_or(text),
        "interval" => PgInterval::from_postgres_text(&text)
            .map(render_interval)
            .unwrap_or(text),
        _ => text,
    }
}

fn typed_temporal_text_fast(typed: &JsonValue, pg_type: &str) -> Option<String> {
    if typed.get("version")?.as_u64()? != u64::from(TYPED_STORAGE_VERSION)
        || typed.get("pg_type")?.as_str()? != pg_type
        || typed.get("value").is_some()
    {
        return None;
    }
    let text = typed.get("text")?.as_str()?;
    Some(temporal_storage_text_to_display(pg_type, text.to_string()))
}

pub(crate) fn storage_json_to_sql_value(value: &JsonValue, pg_type: &str) -> SqlValue {
    // Every typed cell used to be cloned and trial-deserialized as a user
    // type first, then cloned and deserialized again for its real type. Look
    // the envelope up once, decode NUMERIC (the TPC-C money columns) by hand,
    // and only try the user-type shape when its discriminating field exists.
    let typed_envelope = value.get(TYPED_STORAGE_KEY);
    if pg_type == "numeric" {
        if let Some(text) = typed_envelope.and_then(typed_numeric_text_fast) {
            return SqlValue::String(text);
        }
    } else if is_temporal_storage_type(pg_type) {
        if let Some(text) =
            typed_envelope.and_then(|typed| typed_temporal_text_fast(typed, pg_type))
        {
            return SqlValue::String(text);
        }
    }
    let stored_user_type = typed_envelope
        .filter(|typed| typed.get("type_oid").is_some())
        .and_then(|typed| StoredUserTypeValue::deserialize(typed).ok())
        .filter(|stored| stored.version == TYPED_STORAGE_VERSION);
    if let Some(stored) = stored_user_type {
        if let Some(composite) = stored.composite {
            if let Ok(composite) = composite.into_sql() {
                return SqlValue::Composite(composite);
            }
        }
        return json_to_sql_value(&stored.value);
    }
    if pg_type == "json" {
        let stored = value
            .get(TYPED_STORAGE_KEY)
            .and_then(|value| StoredJsonTextValue::deserialize(value).ok())
            .filter(|stored| stored.version == TYPED_STORAGE_VERSION && stored.pg_type == "json");
        if let Some(stored) = stored {
            if let Ok(value) = PgJsonText::parse(stored.text) {
                return SqlValue::JsonText(value);
            }
        }
    }
    if pg_type == "uuid" {
        if let Some(value) = value.as_str() {
            return parse_postgres_uuid(value)
                .map(format_postgres_uuid)
                .map(SqlValue::String)
                .unwrap_or_else(|_| SqlValue::String(value.to_string()));
        }
    }
    if pg_type == "numeric" {
        let stored = value
            .get(TYPED_STORAGE_KEY)
            .and_then(|value| StoredNumericValue::deserialize(value).ok())
            .filter(|stored| {
                stored.version == TYPED_STORAGE_VERSION && stored.pg_type == "numeric"
            });
        if let Some(stored) = stored {
            let text = stored
                .value
                .map(|value| value.to_decimal_text())
                .or(stored.text);
            if let Some(text) = text {
                return SqlValue::String(text);
            }
        }
    }
    if matches!(pg_type, "float4" | "float8" | "money") {
        let stored = value
            .get(TYPED_STORAGE_KEY)
            .and_then(|value| StoredScalarValue::deserialize(value).ok())
            .filter(|stored| {
                stored.version == TYPED_STORAGE_VERSION
                    && stored.pg_type == pg_type
                    && matches!(
                        (&stored.value, pg_type),
                        (PgCanonicalValue::Float4(_), "float4")
                            | (PgCanonicalValue::Float8(_), "float8")
                            | (PgCanonicalValue::Money(_), "money")
                    )
            });
        if let Some(stored) = stored {
            return match stored.value {
                PgCanonicalValue::Float4(value) => SqlValue::Float(f64::from(value.to_value())),
                PgCanonicalValue::Float8(value) => SqlValue::Float(value.to_value()),
                PgCanonicalValue::Money(value) => {
                    SqlValue::String(crate::pg_money_text_from_cents(value))
                }
                _ => unreachable!("validated float scalar envelope"),
            };
        }
    }
    if pg_type.ends_with("[]") {
        let stored = value
            .get(TYPED_STORAGE_KEY)
            .and_then(|value| StoredArrayValue::deserialize(value).ok())
            .filter(|stored| {
                stored.version == TYPED_STORAGE_VERSION
                    && stored.pg_type == pg_type
                    && stored.value.element_type == pg_type.trim_end_matches("[]")
            });
        if let Some(stored) = stored {
            let output = if matches!(
                stored.value.element_type.as_str(),
                "inet"
                    | "cidr"
                    | "macaddr"
                    | "macaddr8"
                    | "point"
                    | "line"
                    | "lseg"
                    | "box"
                    | "path"
                    | "polygon"
                    | "circle"
            ) {
                postgres_special_array_output(&stored.value)
                    .unwrap_or_else(|| stored.legacy.clone())
            } else {
                stored.legacy.clone()
            };
            let lower_bounds = stored
                .value
                .dimensions
                .iter()
                .map(|dimension| dimension.lower_bound)
                .collect::<Vec<_>>();
            if lower_bounds.iter().any(|lower_bound| *lower_bound != 1) {
                return SqlValue::Json(serde_json::json!({
                    "$bicdb_array_input": {
                        "lower_bounds": lower_bounds,
                        "value": output,
                    }
                }));
            }
            return SqlValue::Json(output);
        }
    }
    if matches!(pg_type, "bytea" | "bit" | "varbit") {
        let stored = value
            .get(TYPED_STORAGE_KEY)
            .and_then(|value| StoredBinaryValue::deserialize(value).ok())
            .filter(|stored| {
                stored.version == TYPED_STORAGE_VERSION
                    && stored.pg_type == pg_type
                    && matches!(
                        (&stored.value, pg_type),
                        (PgCanonicalValue::Bytes(_), "bytea")
                            | (PgCanonicalValue::BitString(_), "bit" | "varbit")
                    )
            });
        if let Some(stored) = stored {
            return SqlValue::String(stored.text);
        }
        if pg_type == "bytea" {
            if let Some(bytes) = value.as_array().and_then(|values| {
                values
                    .iter()
                    .map(|value| value.as_u64().and_then(|value| u8::try_from(value).ok()))
                    .collect::<Option<Vec<_>>>()
            }) {
                return SqlValue::String(format_bytea_hex(&bytes));
            }
        }
    }
    if matches!(
        pg_type,
        "date" | "time" | "timetz" | "timestamp" | "timestamptz" | "interval"
    ) {
        let stored = value
            .get(TYPED_STORAGE_KEY)
            .and_then(|value| StoredTemporalValue::deserialize(value).ok())
            .filter(|stored| {
                stored.version == TYPED_STORAGE_VERSION
                    && stored.pg_type == pg_type
                    && matches!(
                        (&stored.value, pg_type),
                        (None, _)
                            | (Some(PgCanonicalValue::Date(_)), "date")
                            | (Some(PgCanonicalValue::Time(_)), "time")
                            | (Some(PgCanonicalValue::TimeTz(_)), "timetz")
                            | (Some(PgCanonicalValue::Timestamp(_)), "timestamp")
                            | (Some(PgCanonicalValue::TimestampTz(_)), "timestamptz")
                            | (Some(PgCanonicalValue::Interval(_)), "interval")
                    )
            });
        if let Some(stored) = stored {
            return SqlValue::String(match stored.value {
                Some(PgCanonicalValue::TimestampTz(timestamp)) => render_timestamptz(timestamp),
                Some(PgCanonicalValue::Interval(interval)) => render_interval(interval),
                Some(_) => stored.text,
                None => temporal_storage_text_to_display(pg_type, stored.text),
            });
        }
    }
    if is_pg_canonical_special_type(pg_type) {
        let stored = value
            .get(TYPED_STORAGE_KEY)
            .and_then(|value| StoredSpecialValue::deserialize(value).ok())
            .filter(|stored| {
                stored.version == TYPED_STORAGE_VERSION
                    && stored.pg_type == pg_type
                    && canonical_special_matches_type(&stored.value, pg_type)
            });
        if let Some(stored) = stored {
            if let Some(text) = canonical_range_value_text(stored.value.clone(), pg_type) {
                return SqlValue::String(text);
            }
            return match stored.value {
                PgCanonicalValue::Oid(oid) if pg_type == "oid" => SqlValue::Int(i64::from(oid)),
                PgCanonicalValue::OidAlias(alias) if is_oid_alias_type(pg_type) => alias
                    .oid
                    .map(|oid| SqlValue::Int(i64::from(oid)))
                    .or_else(|| alias.symbolic_name.map(SqlValue::String))
                    .unwrap_or(SqlValue::Null),
                PgCanonicalValue::TransactionId32(value) if pg_type == "xid" => {
                    SqlValue::Int(i64::from(value))
                }
                PgCanonicalValue::TransactionId64(value) if pg_type == "xid8" => {
                    SqlValue::String(value.to_string())
                }
                PgCanonicalValue::CommandId(value) if pg_type == "cid" => {
                    SqlValue::Int(i64::from(value))
                }
                PgCanonicalValue::TupleId(value) if pg_type == "tid" => {
                    SqlValue::String(value.to_postgres_text())
                }
                PgCanonicalValue::TsQuery(query) if pg_type == "tsquery" => {
                    SqlValue::TsQuery(query)
                }
                _ => SqlValue::String(stored.text),
            };
        }
    }
    if matches!(
        pg_type,
        "int4range"
            | "numrange"
            | "tsrange"
            | "tstzrange"
            | "daterange"
            | "int8range"
            | "int4multirange"
            | "nummultirange"
            | "tsmultirange"
            | "tstzmultirange"
            | "datemultirange"
            | "int8multirange"
    ) {
        let text = json_to_sql_value(value).to_cell();
        if let Ok(Some(canonical)) = parse_pg_canonical_special(pg_type, &text) {
            if let Some(text) = canonical_range_value_text(canonical, pg_type) {
                return SqlValue::String(text);
            }
        }
    }
    json_to_sql_value(value)
}

/// Decode one engine-owned typed-storage envelope for consumers that read
/// records below the SQL projection layer. Returns `None` for ordinary JSON,
/// so application metadata that merely resembles a scalar is left untouched.
pub fn decode_typed_storage_json(value: &JsonValue) -> Option<JsonValue> {
    let pg_type = value.get(TYPED_STORAGE_KEY)?.get("pg_type")?.as_str()?;
    Some(sql_value_to_json(storage_json_to_sql_value(value, pg_type)))
}

fn postgres_special_array_output(array: &PgArray) -> Option<JsonValue> {
    if array.dimensions.is_empty() {
        return array
            .elements
            .is_empty()
            .then(|| JsonValue::Array(Vec::new()));
    }
    let elements = array
        .elements
        .iter()
        .map(|element| match element {
            PgCanonicalValue::Null => Some(JsonValue::Null),
            PgCanonicalValue::Network(network)
                if matches!(network.kind, PgNetworkKind::Inet | PgNetworkKind::Cidr) =>
            {
                Some(JsonValue::String(network.to_postgres_output_text()))
            }
            PgCanonicalValue::MacAddress(PgMacAddress::Mac48(address))
                if array.element_type == "macaddr" =>
            {
                Some(JsonValue::String(
                    PgMacAddress::Mac48(*address).to_postgres_text(),
                ))
            }
            PgCanonicalValue::MacAddress(PgMacAddress::Mac64(address))
                if array.element_type == "macaddr8" =>
            {
                Some(JsonValue::String(
                    PgMacAddress::Mac64(*address).to_postgres_text(),
                ))
            }
            PgCanonicalValue::Geometric(value)
                if matches!(
                    array.element_type.as_str(),
                    "point" | "line" | "lseg" | "box" | "path" | "polygon" | "circle"
                ) =>
            {
                Some(JsonValue::String(value.to_postgres_text()))
            }
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;
    let mut offset = 0;
    let output = postgres_array_output_dimension(&array.dimensions, &elements, &mut offset)?;
    (offset == elements.len()).then_some(output)
}

fn postgres_array_output_dimension(
    dimensions: &[PgArrayDimension],
    elements: &[JsonValue],
    offset: &mut usize,
) -> Option<JsonValue> {
    let (dimension, remaining) = dimensions.split_first()?;
    let mut values = Vec::with_capacity(dimension.length);
    if remaining.is_empty() {
        for _ in 0..dimension.length {
            values.push(elements.get(*offset)?.clone());
            *offset += 1;
        }
    } else {
        for _ in 0..dimension.length {
            values.push(postgres_array_output_dimension(
                remaining, elements, offset,
            )?);
        }
    }
    Some(JsonValue::Array(values))
}

fn canonical_range_value_text(value: PgCanonicalValue, pg_type: &str) -> Option<String> {
    match value {
        PgCanonicalValue::Range(range) => range
            .canonicalized()
            .ok()
            .map(|range| render_range_for_session(&range, pg_type)),
        PgCanonicalValue::Multirange(ranges) => canonicalize_pg_multirange(ranges)
            .ok()
            .map(|ranges| render_multirange_for_session(&ranges, pg_type)),
        _ => None,
    }
}

fn canonical_special_matches_type(value: &PgCanonicalValue, pg_type: &str) -> bool {
    matches!(
        (value, pg_type),
        (PgCanonicalValue::Network(_), "inet" | "cidr")
            | (PgCanonicalValue::MacAddress(_), "macaddr" | "macaddr8")
            | (
                PgCanonicalValue::Geometric(_),
                "point" | "line" | "lseg" | "box" | "path" | "polygon" | "circle"
            )
            | (
                PgCanonicalValue::Range(_),
                "int4range" | "numrange" | "tsrange" | "tstzrange" | "daterange" | "int8range"
            )
            | (
                PgCanonicalValue::Multirange(_),
                "int4multirange"
                    | "nummultirange"
                    | "tsmultirange"
                    | "tstzmultirange"
                    | "datemultirange"
                    | "int8multirange"
            )
            | (PgCanonicalValue::Oid(_), "oid")
            | (PgCanonicalValue::TransactionId32(_), "xid")
            | (PgCanonicalValue::TransactionId64(_), "xid8")
            | (PgCanonicalValue::CommandId(_), "cid")
            | (PgCanonicalValue::TupleId(_), "tid")
            | (
                PgCanonicalValue::OidAlias(_),
                "regproc"
                    | "regprocedure"
                    | "regoper"
                    | "regoperator"
                    | "regclass"
                    | "regcollation"
                    | "regtype"
                    | "regrole"
                    | "regnamespace"
                    | "regconfig"
                    | "regdictionary"
            )
            | (PgCanonicalValue::Lsn(_), "pg_lsn")
            | (
                PgCanonicalValue::Snapshot(_),
                "pg_snapshot" | "txid_snapshot"
            )
            | (PgCanonicalValue::TsVector(_), "tsvector")
            | (PgCanonicalValue::TsQuery(_), "tsquery")
    )
}

fn array_storage_input(value: SqlValue, pg_type: &str) -> Result<(JsonValue, Option<Vec<i32>>)> {
    let SqlValue::Json(value) = value else {
        return Err(SqlError::InvalidTextRepresentation(format!(
            "invalid input syntax for type {pg_type}"
        )));
    };
    if let Some(input) = value.get("$bicdb_array_input") {
        let legacy = input.get("value").cloned().ok_or_else(|| {
            SqlError::InvalidTextRepresentation(format!("invalid input syntax for type {pg_type}"))
        })?;
        let lower_bounds = serde_json::from_value::<Vec<i32>>(
            input.get("lower_bounds").cloned().ok_or_else(|| {
                SqlError::InvalidTextRepresentation(format!(
                    "invalid input syntax for type {pg_type}"
                ))
            })?,
        )
        .map_err(|_| {
            SqlError::InvalidTextRepresentation(format!("invalid input syntax for type {pg_type}"))
        })?;
        return Ok((legacy, Some(lower_bounds)));
    }
    if value.is_array() {
        Ok((value, None))
    } else {
        Err(SqlError::InvalidTextRepresentation(format!(
            "invalid input syntax for type {pg_type}"
        )))
    }
}

fn flatten_array_json(value: &JsonValue) -> Option<(Vec<usize>, Vec<&JsonValue>)> {
    let JsonValue::Array(values) = value else {
        return Some((Vec::new(), vec![value]));
    };
    if values.is_empty() {
        return Some((Vec::new(), Vec::new()));
    }
    let (child_dimensions, mut flattened) = flatten_array_json(&values[0])?;
    for value in values.iter().skip(1) {
        let (dimensions, elements) = flatten_array_json(value)?;
        if dimensions != child_dimensions {
            return None;
        }
        flattened.extend(elements);
    }
    let mut dimensions = Vec::with_capacity(child_dimensions.len() + 1);
    dimensions.push(values.len());
    dimensions.extend(child_dimensions);
    Some((dimensions, flattened))
}

fn canonical_array_element(value: &JsonValue, element_type: &str) -> Result<PgCanonicalValue> {
    if value.is_null() {
        return Ok(PgCanonicalValue::Null);
    }
    let invalid = || {
        SqlError::InvalidTextRepresentation(format!(
            "invalid input syntax for array element type {element_type}: {value}"
        ))
    };
    Ok(match element_type {
        "bool" => PgCanonicalValue::Bool(value.as_bool().ok_or_else(invalid)?),
        "int2" => PgCanonicalValue::Int2(
            i16::try_from(value.as_i64().ok_or_else(invalid)?).map_err(|_| invalid())?,
        ),
        "int4" => PgCanonicalValue::Int4(
            i32::try_from(value.as_i64().ok_or_else(invalid)?).map_err(|_| invalid())?,
        ),
        "int8" => PgCanonicalValue::Int8(value.as_i64().ok_or_else(invalid)?),
        "float4" => PgCanonicalValue::Float4(PgFloat4::from_value(
            value.as_f64().ok_or_else(invalid)? as f32,
        )),
        "float8" => {
            PgCanonicalValue::Float8(PgFloat8::from_value(value.as_f64().ok_or_else(invalid)?))
        }
        "numeric" => PgCanonicalValue::Numeric(
            PgNumeric::from_decimal_text(value.as_str().ok_or_else(invalid)?)
                .map_err(|_| invalid())?,
        ),
        "money" => PgCanonicalValue::Money(
            crate::pg_money_cents_from_text(value.as_str().ok_or_else(invalid)?)
                .map_err(|_| invalid())?,
        ),
        "bytea" => PgCanonicalValue::Bytes(
            parse_bytea_text(value.as_str().ok_or_else(invalid)?).map_err(|_| invalid())?,
        ),
        "bit" | "varbit" => PgCanonicalValue::BitString(
            PgBitString::from_bit_text(value.as_str().ok_or_else(invalid)?)
                .map_err(|_| invalid())?,
        ),
        "date" => PgCanonicalValue::Date(
            PgDate::from_iso_text(value.as_str().ok_or_else(invalid)?).map_err(|_| invalid())?,
        ),
        "time" => PgCanonicalValue::Time(
            PgTime::from_postgres_text(value.as_str().ok_or_else(invalid)?)
                .map_err(|_| invalid())?,
        ),
        "timetz" => PgCanonicalValue::TimeTz(
            PgTimeTz::from_postgres_text(value.as_str().ok_or_else(invalid)?, 0)
                .map_err(|_| invalid())?,
        ),
        "timestamp" => PgCanonicalValue::Timestamp(
            PgTimestamp::from_postgres_text(value.as_str().ok_or_else(invalid)?, false)
                .map_err(|_| invalid())?,
        ),
        "timestamptz" => PgCanonicalValue::TimestampTz(
            parse_timestamptz(value.as_str().ok_or_else(invalid)?).map_err(|_| invalid())?,
        ),
        "interval" => PgCanonicalValue::Interval(
            PgInterval::from_postgres_text(value.as_str().ok_or_else(invalid)?)
                .map_err(|_| invalid())?,
        ),
        "uuid" => PgCanonicalValue::Uuid(
            parse_postgres_uuid(value.as_str().ok_or_else(invalid)?).map_err(|_| invalid())?,
        ),
        "json" | "jsonb" => PgCanonicalValue::Json(value.clone()),
        "oid" => PgCanonicalValue::Oid(
            u32::try_from(value.as_i64().ok_or_else(invalid)?).map_err(|_| invalid())?,
        ),
        "vector" => PgCanonicalValue::Vector(
            sql_value_to_vector(&json_to_sql_value(value))?
                .into_iter()
                .map(PgFloat4::from_value)
                .collect(),
        ),
        _ => {
            if is_pg_canonical_special_type(element_type) {
                let text = json_to_sql_value(value).to_cell();
                parse_pg_canonical_special(element_type, &text)
                    .map_err(|_| invalid())?
                    .ok_or_else(invalid)?
            } else {
                PgCanonicalValue::Text(value.as_str().ok_or_else(invalid)?.to_string())
            }
        }
    })
}

pub(crate) fn set_json_object_value(metadata: &mut JsonValue, column: &str, value: JsonValue) {
    if !metadata.is_object() {
        *metadata = JsonValue::Object(JsonMap::new());
    }
    if let Some(object) = metadata.as_object_mut() {
        object.insert(column.to_string(), value);
    }
}

pub(crate) fn table_with_joins_name_and_alias(table: &TableWithJoins) -> Result<(String, String)> {
    if !table.joins.is_empty() {
        return Err(SqlError::Unsupported(
            "joins are not supported for UPDATE".to_string(),
        ));
    }
    let TableFactor::Table { name, alias, .. } = &table.relation else {
        return Err(SqlError::Unsupported(
            "UPDATE supports only table names".to_string(),
        ));
    };
    let table = relation_name(name)?;
    let alias = alias
        .as_ref()
        .map(|alias| alias.name.value.clone())
        .unwrap_or_else(|| table.rsplit('.').next().unwrap_or(&table).to_string());
    Ok((table, alias))
}

pub(crate) fn delete_from_table_and_alias(delete: &Delete) -> Result<(String, String)> {
    let tables = match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };
    let [table] = tables.as_slice() else {
        return Err(SqlError::Unsupported(
            "DELETE supports exactly one table".to_string(),
        ));
    };
    table_with_joins_name_and_alias(table)
}

#[cfg(test)]
mod local_check_precheck_tests {
    use super::*;

    /// Whenever the raw-JSON pre-check claims a column, decoding it must give
    /// a non-null value the type check accepts — over every stored shape the
    /// pre-check can meet, including ones it must decline.
    #[test]
    fn precheck_never_claims_what_decoding_would_reject() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut sql = SqlSession::new(&mut db);
        sql.execute(
            "CREATE TABLE shapes (id INT PRIMARY KEY, b BOOL NOT NULL, i INT NOT NULL, \
             big BIGINT, f FLOAT8, n NUMERIC(12,2) NOT NULL, t TEXT NOT NULL, v VARCHAR(10), \
             c CHAR(3), ts TIMESTAMP NOT NULL, tz TIMESTAMPTZ, d DATE, iv INTERVAL, \
             u UUID, by BYTEA, j JSONB, js JSON, arr INT[], oid_col REGCLASS)",
        )
        .unwrap();
        sql.execute(
            "INSERT INTO shapes VALUES (1, true, 7, 9000000000, 1.5, 12.50, 'txt', 'var', 'ab', \
             '2026-09-02 05:28:05', '2026-09-02 05:28:05+00', '2026-09-02', '1 day', \
             '6ba7b810-9dad-11d1-80b4-00c04fd430c8', '\\x0102', '{\"k\":1}', '{\"k\":2}', \
             ARRAY[1,2], 'shapes')",
        )
        .unwrap();
        sql.execute(
            "INSERT INTO shapes (id, b, i, n, t, ts) VALUES (2, false, 0, 0, '', '2000-01-01')",
        )
        .unwrap();
        let schema = load_schema(sql.db_ref(), "shapes").unwrap().unwrap();
        let records = sql.db_ref().scan_collection("shapes").unwrap();
        assert_eq!(records.len(), 2);
        let mut claimed = 0;
        for record in &records {
            // Legacy / hostile shapes the pre-check must not claim.
            let mut hostile = record.clone();
            hostile
                .metadata
                .as_object_mut()
                .unwrap()
                .insert("i".into(), serde_json::json!("not a number"));
            hostile.metadata.as_object_mut().unwrap().insert(
                "ts".into(),
                serde_json::json!({"$bicdb_typed": {"version": 1, "pg_type": "date", "text": "x"}}),
            );
            for candidate in [record, &hostile] {
                for column in &schema.columns {
                    if column.primary_key || is_json_pg_type(&column.pg_type) {
                        continue;
                    }
                    let Some(raw) = candidate.metadata.get(&column.name) else {
                        continue;
                    };
                    if !storage_json_proves_local_checks(raw, column) {
                        continue;
                    }
                    claimed += 1;
                    let decoded = record_column_value(candidate, &schema, &column.name);
                    assert!(
                        !matches!(decoded, SqlValue::Null)
                            && column_value_matches_type(&decoded, column),
                        "pre-check claimed {} = {raw} but decoding gives {decoded:?}",
                        column.name
                    );
                }
            }
        }
        assert!(
            claimed >= 12,
            "pre-check should cover the common shapes, claimed {claimed}"
        );
        assert!(!storage_json_proves_local_checks(
            &serde_json::json!("not a number"),
            schema.column("i").unwrap()
        ));
        assert!(!storage_json_proves_local_checks(
            &serde_json::json!({"$bicdb_typed": {"version": 1, "pg_type": "date", "text": "x"}}),
            schema.column("ts").unwrap()
        ));
        assert!(!storage_json_proves_local_checks(
            &JsonValue::Null,
            schema.column("t").unwrap()
        ));
    }
}

#[cfg(test)]
mod temporal_storage_single_parse_tests {
    use super::*;

    /// The key derived from the single parse must equal the codec's own key
    /// for every temporal type and input shape the encoder accepts.
    #[test]
    fn single_parse_index_keys_match_the_codec() {
        let cases: &[(&str, &[&str])] = &[
            (
                "timestamptz",
                &[
                    "2026-09-02 05:28:05+00",
                    "2026-09-02 05:28:05.25+02:30",
                    "2026-09-02T05:28:05Z",
                    "2026-09-02 05:28:05",
                    "epoch",
                    "infinity",
                    "-infinity",
                    "0100-01-01 00:00:00+00 BC",
                ],
            ),
            (
                "timestamp",
                &[
                    "2026-09-02 05:28:05",
                    "2026-09-02 05:28:05.123456",
                    "2026-09-02T05:28:05",
                    "infinity",
                ],
            ),
            (
                "time",
                &["05:28:05", "05:28:05.5", "23:59:59.999999", "00:00:00"],
            ),
            (
                "timetz",
                &["05:28:05+00", "05:28:05-03:30", "05:28:05.5+02"],
            ),
            (
                "interval",
                &[
                    "1 day",
                    "1 year 2 mons 3 days 04:05:06",
                    "-00:00:01",
                    "P1Y2M3DT4H5M6S",
                    "3 hours 30 minutes",
                ],
            ),
            ("date", &["2026-09-02", "0001-01-01", "2024-02-29"]),
            (
                "numeric",
                &[
                    "12.50",
                    "0",
                    "-7.25",
                    "007.5",
                    "+5",
                    ".5",
                    "5.",
                    "1e3",
                    "-1.5e-2",
                    "NaN",
                    "Infinity",
                    "-Infinity",
                    "-0",
                    "0.000",
                    "123456789012345678901234567890.123456789",
                    " 3 ",
                ],
            ),
        ];
        for (pg_type, inputs) in cases {
            for input in *inputs {
                let value = SqlValue::String((*input).to_string());
                let stored = sql_value_to_storage_json(value.clone(), Some(pg_type)).unwrap();
                let key_hex = stored[TYPED_STORAGE_KEY]["index_key"]
                    .as_str()
                    .unwrap()
                    .to_string();
                let codec_key = pg_typed_index_key(pg_type, &value).unwrap();
                let codec_hex = codec_key
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>();
                assert_eq!(key_hex, codec_hex, "{pg_type} {input}");
            }
        }
        for value in [
            SqlValue::Int(42),
            SqlValue::Int(-7),
            SqlValue::Float(1.5),
            SqlValue::Float(0.1 + 0.2),
        ] {
            let stored = sql_value_to_storage_json(value.clone(), Some("numeric")).unwrap();
            let key_hex = stored[TYPED_STORAGE_KEY]["index_key"]
                .as_str()
                .unwrap()
                .to_string();
            let codec_hex = pg_typed_index_key("numeric", &value)
                .unwrap()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>();
            assert_eq!(key_hex, codec_hex, "numeric {value:?}");
        }
        for input in ["abc", "1.2.3", "--5"] {
            let value = SqlValue::String(input.to_string());
            let new = sql_value_to_storage_json(value.clone(), Some("numeric"))
                .err()
                .map(|e| e.to_string());
            let old = pg_typed_index_key("numeric", &value)
                .err()
                .map(|e| e.to_string());
            assert_eq!(new, old, "numeric error for {input}");
        }
        // Invalid input still fails through the codec's error path.
        for (pg_type, input) in [
            ("timestamptz", "not a time"),
            ("interval", "garbage"),
            ("time", "25:99:00"),
        ] {
            let value = SqlValue::String(input.to_string());
            let new = sql_value_to_storage_json(value.clone(), Some(pg_type))
                .err()
                .map(|e| e.to_string());
            assert!(new.is_some(), "{pg_type} {input} must fail");
        }
    }
}

#[cfg(test)]
mod storage_envelope_writer_tests {
    use super::*;

    #[test]
    fn direct_writer_matches_the_value_tree_rendering() {
        let cases: Vec<(SqlValue, Option<&str>)> = vec![
            (SqlValue::String("12.50".into()), Some("numeric")),
            (SqlValue::Int(7), Some("numeric")),
            (SqlValue::String("-0.005".into()), Some("numeric")),
            (SqlValue::Float(2.5), Some("float8")),
            (SqlValue::Float(1.0e-7), Some("float4")),
            (SqlValue::String("$12.34".into()), Some("money")),
            (SqlValue::Int(42), Some("int4")),
            (SqlValue::Bool(true), Some("bool")),
            (
                SqlValue::String("plain \"quoted\" text\n".into()),
                Some("text"),
            ),
            (SqlValue::String("ünïcödé".into()), Some("varchar")),
            (SqlValue::String("pad".into()), Some("bpchar")),
            (
                SqlValue::String("2026-09-03 18:00:00".into()),
                Some("timestamp"),
            ),
            (
                SqlValue::String("2026-09-03 18:00:00+02".into()),
                Some("timestamptz"),
            ),
            (SqlValue::String("2026-09-03".into()), Some("date")),
            (SqlValue::String("18:00:01.5".into()), Some("time")),
            (SqlValue::String("18:00:01+01".into()), Some("timetz")),
            (SqlValue::String("1 day 02:03:04".into()), Some("interval")),
            (SqlValue::String("\\xdeadbeef".into()), Some("bytea")),
            (SqlValue::String("1011".into()), Some("bit")),
            (SqlValue::String("101".into()), Some("varbit")),
            (
                SqlValue::String("{\"a\": [1, 2, {\"b\": null}]}".into()),
                Some("json"),
            ),
            (SqlValue::String("{1,2,3}".into()), Some("int4[]")),
            (SqlValue::String("{\"x\",\"y z\"}".into()), Some("text[]")),
            (SqlValue::String("192.168.0.1/24".into()), Some("inet")),
            (
                SqlValue::String("08:00:2b:01:02:03".into()),
                Some("macaddr"),
            ),
            (SqlValue::String("[1,10)".into()), Some("int4range")),
            (SqlValue::String("(1,2)".into()), Some("point")),
            (
                SqlValue::String("a1b2c3d4-e5f6-7890-abcd-ef1234567890".into()),
                Some("uuid"),
            ),
            (SqlValue::Int(3), None),
            (SqlValue::String("free".into()), None),
            (SqlValue::Null, Some("numeric")),
        ];
        for (value, pg_type) in cases {
            let expected = match sql_value_to_storage_json(value.clone(), pg_type) {
                Ok(json) => json.to_string(),
                Err(error) => {
                    let written = storage_envelope(value.clone(), pg_type);
                    assert!(written.is_err(), "{value:?} {pg_type:?}: {error}");
                    continue;
                }
            };
            let mut out = Vec::new();
            storage_envelope(value.clone(), pg_type)
                .unwrap()
                .write_to(&mut out)
                .unwrap();
            assert_eq!(
                String::from_utf8(out).unwrap(),
                expected,
                "{value:?} {pg_type:?}"
            );
        }
    }
}

#[cfg(test)]
mod record_id_from_key_values_tests {
    use super::*;
    use bicdb_core::{BicDb, DbConfig};

    #[test]
    fn slice_form_matches_the_map_form() {
        let dir = tempfile::tempdir().unwrap();
        let mut db =
            BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
        let mut sql = crate::SqlSession::new(&mut db);
        sql.execute("CREATE TABLE single (id INT PRIMARY KEY, note TEXT)")
            .unwrap();
        sql.execute(
            "CREATE TABLE composite (no_o_id INT NOT NULL, no_d_id INT NOT NULL, no_w_id INT NOT NULL, \
             note TEXT, PRIMARY KEY (no_w_id, no_d_id, no_o_id))",
        )
        .unwrap();
        sql.execute("CREATE TABLE typed (amount NUMERIC(8,2) NOT NULL, tag TEXT, PRIMARY KEY (amount, tag))")
            .unwrap();
        sql.execute("CREATE TABLE \"Mixed\" (\"Key\" BIGINT PRIMARY KEY)")
            .unwrap();
        sql.execute("CREATE TABLE widths (a SMALLINT, b INT, c BIGINT, PRIMARY KEY (a, b, c))")
            .unwrap();
        drop(sql);
        let mut cases: Vec<(&str, Vec<&str>, Vec<SqlValue>)> = vec![
            ("single", vec!["id"], vec![SqlValue::Int(7)]),
            ("single", vec!["id"], vec![SqlValue::String("8".into())]),
            (
                "single",
                vec!["id", "note"],
                vec![SqlValue::Int(7), SqlValue::String("x".into())],
            ),
            ("single", vec!["note"], vec![SqlValue::String("x".into())]),
            ("single", vec!["id"], vec![SqlValue::Null]),
            (
                "composite",
                vec!["no_w_id", "no_d_id", "no_o_id"],
                vec![SqlValue::Int(1), SqlValue::Int(2), SqlValue::Int(3)],
            ),
            (
                "composite",
                vec!["no_o_id", "no_d_id", "no_w_id"],
                vec![SqlValue::Int(3), SqlValue::Int(2), SqlValue::Int(1)],
            ),
            (
                "composite",
                vec!["no_w_id", "no_d_id"],
                vec![SqlValue::Int(1), SqlValue::Int(2)],
            ),
            (
                "composite",
                vec!["no_w_id", "no_d_id", "no_o_id"],
                vec![SqlValue::Int(1), SqlValue::Null, SqlValue::Int(3)],
            ),
            (
                "typed",
                vec!["amount", "tag"],
                vec![
                    SqlValue::String("12.5".into()),
                    SqlValue::String("a".into()),
                ],
            ),
            (
                "typed",
                vec!["amount", "tag"],
                vec![SqlValue::Int(3), SqlValue::String("".into())],
            ),
            ("Mixed", vec!["Key"], vec![SqlValue::Int(9)]),
            ("Mixed", vec!["key"], vec![SqlValue::Int(9)]),
        ];
        for a in [
            i16::MIN as i64 - 1,
            i16::MIN as i64,
            -1,
            0,
            i16::MAX as i64,
            i16::MAX as i64 + 1,
        ] {
            for b in [
                i32::MIN as i64 - 1,
                i32::MIN as i64,
                0,
                i32::MAX as i64,
                i32::MAX as i64 + 1,
            ] {
                for c in [i64::MIN, -1, 0, i64::MAX] {
                    cases.push((
                        "widths",
                        vec!["a", "b", "c"],
                        vec![SqlValue::Int(a), SqlValue::Int(b), SqlValue::Int(c)],
                    ));
                }
            }
        }
        for (table, columns, values) in cases {
            let schema = load_schema(&db, table).unwrap().unwrap();
            let columns: Vec<String> = columns.into_iter().map(String::from).collect();
            let fields = columns
                .iter()
                .cloned()
                .zip(values.iter().cloned())
                .collect::<BTreeMap<_, _>>();
            let expected = record_id_from_fields(table, Some(&schema), &fields)
                .map_err(|error| error.to_string());
            let actual = record_id_from_column_values(table, &schema, &columns, &values)
                .map_err(|error| error.to_string());
            assert_eq!(actual, expected, "{table} {columns:?} {values:?}");
        }
    }
}
