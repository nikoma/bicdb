//! PostgreSQL's DETAIL lines of constraint violations, which show the key or
//! row values the way the types' output functions print them:
//! `Key (a, b)=(1, x) already exists.`, `Key (x)=(5) is not present in table
//! "p".`, `Failing row contains (1, null, abc).`. They are sent only after the
//! session authorized them ([`ErrorDetail`]).
use crate::records::{record_column_value, UniqueKey};
use crate::*;

/// The longest value text `Failing row contains (...)` shows before `...`
/// (PostgreSQL's `ExecBuildSlotValueDescription`).
const FAILING_ROW_FIELD_BYTES: usize = 64;

/// `value` as the output function of `pg_type` prints it, `null` for NULL.
pub(crate) fn detail_value_text(value: &SqlValue, pg_type: Option<&str>) -> String {
    let pg_type = pg_type.map(|pg_type| pg_type.trim_start_matches("pg_catalog."));
    match value {
        SqlValue::Null => "null".to_string(),
        SqlValue::Bool(true) => "t".to_string(),
        SqlValue::Bool(false) => "f".to_string(),
        SqlValue::Float(float) if matches!(pg_type, Some("float4" | "float8") | None) => {
            postgres_float_text(*float, pg_type.unwrap_or("float8"))
        }
        value if pg_type.is_some_and(|pg_type| pg_type.ends_with("[]")) => {
            crate::postgres_array_text_value(value, ',').unwrap_or_else(|_| value.to_cell())
        }
        SqlValue::Json(json) if pg_type != Some("json") => crate::jsonb::postgres_jsonb_text(json),
        value => value.to_cell(),
    }
}

/// `(a, b)` and `(1, x)` of a key's names and values.
fn key_text(names: &[String], values: &[SqlValue], types: &[Option<String>]) -> (String, String) {
    let values = values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            detail_value_text(
                value,
                types.get(index).and_then(|pg_type| pg_type.as_deref()),
            )
        })
        .collect::<Vec<_>>();
    (names.join(", "), values.join(", "))
}

/// The key columns of a unique key as PostgreSQL names them in a DETAIL;
/// `None` for an expression key this module cannot print exactly.
fn unique_key_names(schema: &TableSchema, key: &UniqueKey) -> Option<Vec<String>> {
    match key {
        UniqueKey::Columns(columns) => Some(columns.clone()),
        UniqueKey::IndexFields(fields) => fields
            .iter()
            .map(|field| index_field_detail_name(schema, field))
            .collect(),
    }
}

fn index_field_detail_name(schema: &TableSchema, field: &IndexField) -> Option<String> {
    if let Some(name) = crate::records::index_field_column_name(schema, field) {
        return Some(name);
    }
    match field {
        IndexField::Lower(inner) => Some(format!(
            "lower({})",
            index_field_detail_name(schema, inner)?
        )),
        IndexField::Upper(inner) => Some(format!(
            "upper({})",
            index_field_detail_name(schema, inner)?
        )),
        _ => None,
    }
}

/// Whether key `left` sorts before `right` by the key's types.
pub(crate) fn key_precedes(
    schema: &TableSchema,
    key: &UniqueKey,
    left: &[SqlValue],
    right: &[SqlValue],
) -> bool {
    let columns = crate::records::unique_key_column_schemas(schema, key);
    let types = crate::records::unique_key_pg_types(schema, key);
    for (index, (left, right)) in left.iter().zip(right).enumerate() {
        let typed = match (columns.get(index).copied().flatten(), types.get(index)) {
            (Some(column), _) => crate::type_codec::column_typed_compare(column, left, right).ok(),
            (None, Some(Some(pg_type))) => pg_typed_compare(pg_type, left, right).ok(),
            _ => None,
        };
        let ordering = typed.unwrap_or_else(|| {
            let less = |a, b| {
                crate::eval::compare_values(a, &sqlparser::ast::BinaryOperator::Lt, b)
                    .ok()
                    .flatten()
                    == Some(true)
            };
            if less(left, right) {
                std::cmp::Ordering::Less
            } else if less(right, left) {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        });
        if ordering != std::cmp::Ordering::Equal {
            return ordering == std::cmp::Ordering::Less;
        }
    }
    false
}

/// The smallest key that two of `records` share under a unique index
/// (what PostgreSQL's sorted index build reports as `is duplicated`).
pub(crate) fn smallest_duplicated_key<'r>(
    schema: &TableSchema,
    key: &UniqueKey,
    records: impl IntoIterator<Item = &'r Record>,
    nulls_not_distinct: bool,
    predicate: Option<&bicdb_core::IndexPredicate>,
) -> Result<Option<Vec<SqlValue>>> {
    let mut seen = std::collections::BTreeSet::new();
    let mut smallest: Option<Vec<SqlValue>> = None;
    for record in records {
        if let Some(predicate) = predicate {
            if !bicdb_core::index_predicate_matches(record, predicate)? {
                continue;
            }
        }
        let values = crate::records::unique_key_values(record, schema, key);
        if !nulls_not_distinct && values.iter().any(|value| matches!(value, SqlValue::Null)) {
            continue;
        }
        let label = crate::records::typed_unique_key_label(schema, key, &values)?;
        if !seen.insert(label) {
            if smallest
                .as_ref()
                .is_none_or(|current| key_precedes(schema, key, &values, current))
            {
                smallest = Some(values);
            }
        }
    }
    Ok(smallest)
}

/// `Key (a)=(1) already exists.` of a unique violation by `values`.
pub(crate) fn unique_key_detail(
    schema: &TableSchema,
    key: &UniqueKey,
    values: &[SqlValue],
    suffix: &str,
) -> Option<ErrorDetail> {
    let names = unique_key_names(schema, key)?;
    let types = crate::records::unique_key_pg_types(schema, key);
    let (names, values) = key_text(&names, values, &types);
    Some(ErrorDetail::values(
        &schema.name,
        format!("Key ({names})=({values}) {suffix}"),
        None,
    ))
}

/// `Key (x)=(5) is not present in table "p".` of a referencing row of
/// `table`, or the value-free form when its values may not be shown.
pub(crate) fn foreign_key_missing_detail(
    table: &str,
    columns: &[String],
    values: &[SqlValue],
    types: &[Option<String>],
    referenced: &str,
) -> ErrorDetail {
    let referenced = catalog_display_name(referenced);
    let (names, values) = key_text(columns, values, types);
    ErrorDetail::values(
        table,
        format!("Key ({names})=({values}) is not present in table \"{referenced}\"."),
        Some(format!("Key is not present in table \"{referenced}\".")),
    )
}

/// `Key (a)=(1) is still referenced from table "c".` of a referenced row of
/// `table`, or the value-free form when its values may not be shown.
pub(crate) fn foreign_key_referenced_detail(
    table: &str,
    columns: &[String],
    values: &[SqlValue],
    types: &[Option<String>],
    referencing: &str,
) -> ErrorDetail {
    let referencing = catalog_display_name(referencing);
    let (names, values) = key_text(columns, values, types);
    ErrorDetail::values(
        table,
        format!("Key ({names})=({values}) is still referenced from table \"{referencing}\"."),
        Some(format!(
            "Key is still referenced from table \"{referencing}\"."
        )),
    )
}

/// `Failing row contains (...)` of a row of `table` that violates a CHECK or
/// NOT NULL constraint: every column in order, each value cut at 64 bytes.
pub(crate) fn failing_row_detail(
    table: &str,
    schema: &TableSchema,
    record: &Record,
) -> ErrorDetail {
    let values = schema
        .columns
        .iter()
        .filter(|column| !column.hidden)
        .map(|column| {
            let text = detail_value_text(
                &record_column_value(record, schema, &column.name),
                Some(&column.pg_type),
            );
            clip_detail_field(text)
        })
        .collect::<Vec<_>>();
    ErrorDetail::values(
        table,
        format!("Failing row contains ({}).", values.join(", ")),
        None,
    )
}

/// [`failing_row_detail`] of a row given as one cell per schema column.
pub(crate) fn failing_cells_detail(
    table: &str,
    schema: &TableSchema,
    cells: &[Option<SqlValue>],
) -> ErrorDetail {
    let values = schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| !column.hidden)
        .map(|(index, column)| {
            let value = cells
                .get(index)
                .cloned()
                .flatten()
                .unwrap_or(SqlValue::Null);
            clip_detail_field(detail_value_text(&value, Some(&column.pg_type)))
        })
        .collect::<Vec<_>>();
    ErrorDetail::values(
        table,
        format!("Failing row contains ({}).", values.join(", ")),
        None,
    )
}

/// PostgreSQL's NOT NULL violation of a row (given by column name) that
/// has no value for primary-key column `column`.
pub(crate) fn missing_primary_key_violation(
    table: &str,
    schema: &TableSchema,
    column: &str,
    fields: &std::collections::BTreeMap<String, SqlValue>,
) -> SqlError {
    let cells = schema
        .columns
        .iter()
        .map(|candidate| {
            fields
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(&candidate.name))
                .map(|(_, value)| value.clone())
        })
        .collect::<Vec<_>>();
    not_null_cells_violation(table, schema, column, &cells)
}

/// PostgreSQL's NOT NULL violation of `column` in a row given as one cell
/// per schema column.
pub(crate) fn not_null_cells_violation(
    table: &str,
    schema: &TableSchema,
    column: &str,
    cells: &[Option<SqlValue>],
) -> SqlError {
    crate::records::constraint_violation(
        "23502",
        format!(
            "null value in column \"{column}\" of relation \"{}\" violates not-null constraint",
            catalog_display_name(table)
        ),
        Some(table.to_string()),
        Some(column.to_string()),
        Some(not_null_constraint_name(table, column)),
    )
    .with_error_detail(failing_cells_detail(table, schema, cells))
}

fn clip_detail_field(text: String) -> String {
    if text.len() <= FAILING_ROW_FIELD_BYTES {
        return text;
    }
    let mut end = FAILING_ROW_FIELD_BYTES;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &text[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_print_as_output_functions_do() {
        assert_eq!(detail_value_text(&SqlValue::Null, Some("int4")), "null");
        assert_eq!(detail_value_text(&SqlValue::Bool(true), Some("bool")), "t");
        assert_eq!(
            detail_value_text(&SqlValue::Float(0.1), Some("float8")),
            "0.1"
        );
        assert_eq!(
            detail_value_text(&SqlValue::Float(1e20), Some("float8")),
            "1e+20"
        );
        assert_eq!(
            detail_value_text(&SqlValue::String("x y".into()), Some("text")),
            "x y"
        );
    }

    #[test]
    fn failing_row_fields_are_cut_at_64_bytes_on_a_character_boundary() {
        assert_eq!(clip_detail_field("abc".into()), "abc");
        let long = "é".repeat(40);
        let clipped = clip_detail_field(long);
        assert_eq!(clipped, format!("{}...", "é".repeat(32)));
    }
}
