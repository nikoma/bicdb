//! PostgreSQL 18 temporal keys: `PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)` and
//! `UNIQUE (id, valid_at WITHOUT OVERLAPS)`.
//!
//! The key is enforced like an exclusion constraint: equality on the leading columns and `&&` on
//! the range or multirange period, which is also how PostgreSQL implements it (a GiST index, and
//! "conflicting key value violates exclusion constraint" on a conflict). It is stored as an
//! exclusion constraint whose access method records the temporal kind, so the catalogs can show
//! it as the PRIMARY KEY / UNIQUE constraint it is. It never becomes BicDB's record key: rows with
//! equal ids and non-overlapping periods must coexist. A PRIMARY KEY makes its columns NOT NULL;
//! an empty period is rejected. Forms this does not implement fail with 0A000.
use super::*;

pub(crate) const TEMPORAL_PRIMARY_KEY_METHOD: &str = "temporal_primary_key";
pub(crate) const TEMPORAL_UNIQUE_METHOD: &str = "temporal_unique";

pub(crate) fn is_temporal_key_method(access_method: &str) -> bool {
    access_method == TEMPORAL_PRIMARY_KEY_METHOD || access_method == TEMPORAL_UNIQUE_METHOD
}

/// `PRIMARY KEY (a, b WITHOUT OVERLAPS)` text for a stored temporal key.
pub(crate) fn temporal_key_definition(
    access_method: &str,
    equal_columns: &[String],
    range: &Option<ExclusionRangeSchema>,
) -> String {
    let kind = if access_method == TEMPORAL_PRIMARY_KEY_METHOD {
        "PRIMARY KEY"
    } else {
        "UNIQUE"
    };
    let mut columns = equal_columns.to_vec();
    if let Some(period) = range.as_ref().and_then(|range| range.range_column.clone()) {
        columns.push(format!("{period} WITHOUT OVERLAPS"));
    }
    format!("{kind} ({})", columns.join(", "))
}

#[derive(Debug, Clone)]
pub(crate) struct TemporalKeyDef {
    name: Option<String>,
    primary: bool,
    columns: Vec<String>,
    period: String,
}

#[derive(Debug)]
pub(crate) enum TemporalKeyDdl {
    /// The CREATE TABLE text without its temporal key elements, and those keys.
    CreateTable {
        table: String,
        rewritten: String,
        keys: Vec<TemporalKeyDef>,
    },
    AlterTable {
        table: String,
        key: TemporalKeyDef,
    },
}

fn contains_without_overlaps(sql: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
    upper.contains("WITHOUT") && upper.contains("OVERLAPS")
}

fn identifier(text: &str) -> Result<String> {
    let (name, rest) = parse_leading_sql_identifier(text)?;
    if !rest.trim().is_empty() {
        return Err(SqlError::Unsupported(format!(
            "WITHOUT OVERLAPS key element {} is not supported",
            text.trim()
        )));
    }
    Ok(if text.trim_start().starts_with('"') {
        name
    } else {
        name.to_ascii_lowercase()
    })
}

/// `[CONSTRAINT name] PRIMARY KEY|UNIQUE (cols, period WITHOUT OVERLAPS)`; None for any other
/// table element.
fn parse_temporal_element(element: &str) -> Result<Option<TemporalKeyDef>> {
    if !contains_without_overlaps(element) {
        return Ok(None);
    }
    let mut rest = element.trim();
    let mut name = None;
    if let Some(after) = strip_prefix_ci(rest, "CONSTRAINT ") {
        let (constraint, after_name) = parse_leading_sql_identifier(after)?;
        name = Some(if after.trim_start().starts_with('"') {
            constraint
        } else {
            constraint.to_ascii_lowercase()
        });
        rest = after_name.trim_start();
    }
    let (primary, after_kind) = if let Some(after) = strip_prefix_ci(rest, "PRIMARY KEY") {
        (true, after)
    } else if let Some(after) = strip_prefix_ci(rest, "UNIQUE") {
        (false, after)
    } else {
        return Ok(None);
    };
    let after_kind = after_kind.trim_start();
    if !after_kind.starts_with('(') {
        return Ok(None);
    }
    let close = find_matching_paren_nested(after_kind, 0).ok_or_else(|| {
        SqlError::InvalidSql("unterminated WITHOUT OVERLAPS key column list".to_string())
    })?;
    let trailing = after_kind[close + 1..].trim();
    if !trailing.is_empty() {
        return Err(SqlError::Unsupported(format!(
            "WITHOUT OVERLAPS key option {trailing} is not supported"
        )));
    }
    let items = split_top_level_commas_nested(&after_kind[1..close]);
    let mut columns = Vec::new();
    let mut period = None;
    for (index, item) in items.iter().enumerate() {
        let words = item.split_whitespace().collect::<Vec<_>>();
        let is_period = words.len() >= 3
            && words[words.len() - 2].eq_ignore_ascii_case("WITHOUT")
            && words[words.len() - 1].eq_ignore_ascii_case("OVERLAPS");
        if is_period {
            if index + 1 != items.len() {
                return Err(SqlError::data_exception_public(
                    "42601",
                    "syntax error at or near \"WITHOUT\"",
                    None,
                ));
            }
            period = Some(identifier(&words[..words.len() - 2].join(" "))?);
        } else {
            columns.push(identifier(item)?);
        }
    }
    let Some(period) = period else {
        return Ok(None);
    };
    Ok(Some(TemporalKeyDef {
        name,
        primary,
        columns,
        period,
    }))
}

pub(crate) fn raw_temporal_key_ddl(sql: &str) -> Result<Option<TemporalKeyDdl>> {
    if !contains_without_overlaps(sql) {
        return Ok(None);
    }
    let statement = trim_sql_statement(sql);
    if let Some(rest) = strip_prefix_ci(statement, "ALTER TABLE ") {
        let mut rest = rest.trim_start();
        for prefix in ["IF EXISTS ", "ONLY "] {
            if let Some(after) = strip_prefix_ci(rest, prefix) {
                rest = after.trim_start();
            }
        }
        let (table, after_table) = parse_leading_sql_identifier(rest)?;
        let Some(after_add) = strip_prefix_ci(after_table.trim_start(), "ADD ") else {
            return Ok(None);
        };
        let Some(key) = parse_temporal_element(after_add)? else {
            return Ok(None);
        };
        return Ok(Some(TemporalKeyDdl::AlterTable {
            table: normalize_object_name(&table),
            key,
        }));
    }
    let upper = statement.to_ascii_uppercase();
    let Some(table_at) = upper.find(" TABLE ") else {
        return Ok(None);
    };
    if !upper.starts_with("CREATE ") {
        return Ok(None);
    }
    let mut rest = statement[table_at + " TABLE ".len()..].trim_start();
    if let Some(after) = strip_prefix_ci(rest, "IF NOT EXISTS ") {
        rest = after.trim_start();
    }
    let (table, after_table) = parse_leading_sql_identifier(rest)?;
    let after_table = after_table.trim_start();
    if !after_table.starts_with('(') {
        return Ok(None);
    }
    let open = statement.len() - after_table.len();
    let close = find_matching_paren_nested(statement, open).ok_or_else(|| {
        SqlError::InvalidSql("unterminated CREATE TABLE element list".to_string())
    })?;
    let mut kept = Vec::new();
    let mut keys = Vec::new();
    for element in split_top_level_commas_nested(&statement[open + 1..close]) {
        match parse_temporal_element(&element)? {
            Some(key) => keys.push(key),
            None => kept.push(element),
        }
    }
    if keys.is_empty() {
        return Ok(None);
    }
    let rewritten = format!(
        "{}({}){}",
        &statement[..open],
        kept.join(", "),
        &statement[close + 1..]
    );
    Ok(Some(TemporalKeyDdl::CreateTable {
        table: normalize_object_name(&table),
        rewritten,
        keys,
    }))
}

impl SqlSession<'_> {
    pub(crate) fn execute_raw_temporal_key_ddl(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        refuse_period_foreign_keys(sql)?;
        let Some(ddl) = raw_temporal_key_ddl(sql)? else {
            return Ok(None);
        };
        match ddl {
            TemporalKeyDdl::CreateTable {
                table,
                rewritten,
                keys,
            } => {
                let result = self.execute(&rewritten)?;
                let attach = (|| {
                    let resolved = resolve_session_relation_name(self.db_ref(), &table)?;
                    for key in &keys {
                        self.attach_temporal_key(&resolved, key)?;
                    }
                    Ok::<_, SqlError>(())
                })();
                if let Err(error) = attach {
                    // The key is part of the statement: without it the table must not remain.
                    let _ = self.execute(&format!("DROP TABLE {table}"));
                    return Err(error);
                }
                Ok(Some(result))
            }
            TemporalKeyDdl::AlterTable { table, key } => {
                let resolved = resolve_session_relation_name(self.db_ref(), &table)?;
                self.require_table_ownership(&resolved, "ALTER TABLE")?;
                self.attach_temporal_key(&resolved, &key)?;
                Ok(Some(SqlResult::command("ALTER TABLE")))
            }
        }
    }

    fn attach_temporal_key(&mut self, table: &str, key: &TemporalKeyDef) -> Result<()> {
        let mut schema = load_schema(self.db_ref(), table)?
            .ok_or_else(|| SqlError::InvalidCollection(table.to_string()))?;
        let display = catalog_display_name(&schema.name);
        if key.columns.is_empty() {
            return Err(SqlError::data_exception_public(
                "42P16",
                "constraint using WITHOUT OVERLAPS needs at least two columns",
                None,
            ));
        }
        for column in key.columns.iter().chain(std::iter::once(&key.period)) {
            if schema.column(column).is_none() {
                return Err(SqlError::data_exception_public(
                    "42703",
                    format!("column \"{column}\" named in key does not exist"),
                    None,
                ));
            }
        }
        let period_type = schema
            .column(&key.period)
            .map(|column| column.pg_type.to_ascii_lowercase())
            .unwrap_or_default();
        if !period_type.ends_with("range") {
            return Err(SqlError::data_exception_public(
                "42804",
                format!(
                    "column \"{}\" in WITHOUT OVERLAPS is not a range or multirange type",
                    key.period
                ),
                None,
            ));
        }
        if key.primary {
            let has_primary = !primary_key_columns_for_schema(&schema).is_empty()
                || schema.constraints.iter().any(|constraint| {
                    matches!(constraint, ConstraintSchema::Exclusion { access_method, .. }
                        if access_method == TEMPORAL_PRIMARY_KEY_METHOD)
                });
            if has_primary {
                return Err(SqlError::data_exception_public(
                    "42P16",
                    format!("multiple primary keys for table \"{display}\" are not allowed"),
                    None,
                ));
            }
        }
        let name = key.name.clone().unwrap_or_else(|| {
            if key.primary {
                format!("{display}_pkey")
            } else {
                let mut parts = vec![display.clone()];
                parts.extend(key.columns.iter().cloned());
                parts.push(key.period.clone());
                parts.push("key".to_string());
                parts.join("_")
            }
        });
        if schema
            .constraints
            .iter()
            .any(|constraint| constraint_name(constraint) == name)
        {
            return Err(SqlError::duplicate_constraint(table, &name));
        }
        if key.primary {
            // A primary key's columns are NOT NULL, as in PostgreSQL. Existing NULLs fail.
            for column in key.columns.iter().chain(std::iter::once(&key.period)) {
                for record in self.db_ref().scan_collection(table)? {
                    if matches!(
                        record_column_value(&record, &schema, column),
                        SqlValue::Null
                    ) {
                        return Err(constraint_violation(
                            "23502",
                            format!(
                                "column \"{column}\" of relation \"{display}\" contains null values"
                            ),
                            Some(display.clone()),
                            Some(column.clone()),
                            None,
                        ));
                    }
                }
                if let Some(definition) = schema
                    .columns
                    .iter_mut()
                    .find(|candidate| candidate.name == *column)
                {
                    definition.nullable = false;
                }
            }
        }
        let constraint = ConstraintSchema::Exclusion {
            name: name.clone(),
            access_method: if key.primary {
                TEMPORAL_PRIMARY_KEY_METHOD
            } else {
                TEMPORAL_UNIQUE_METHOD
            }
            .to_string(),
            equal_columns: key.columns.clone(),
            range: Some(ExclusionRangeSchema {
                function: "range".to_string(),
                operator: "&&".to_string(),
                start_column: key.period.clone(),
                end_column: key.period.clone(),
                bounds: "[)".to_string(),
                range_column: Some(key.period.clone()),
            }),
            predicate: None,
            validated: true,
        };
        // Existing rows: empty periods and overlaps, as for any new constraint.
        let records = self.db_ref().scan_collection(table)?;
        reject_empty_temporal_periods(&schema, &constraint, &records)?;
        validate_existing_constraint(self.db_ref(), table, &schema, &constraint).map_err(
            |error| {
                if error.sqlstate() == "23P01" {
                    SqlError::data_exception_public(
                        "23P01",
                        format!("could not create exclusion constraint \"{name}\""),
                        None,
                    )
                } else {
                    error
                }
            },
        )?;
        schema.constraints.push(constraint);
        self.save_session_schema(&schema)
    }
}

/// PostgreSQL refuses an empty range or multirange as a WITHOUT OVERLAPS value.
pub(crate) fn reject_empty_temporal_periods(
    schema: &TableSchema,
    constraint: &ConstraintSchema,
    records: &[Record],
) -> Result<()> {
    let ConstraintSchema::Exclusion {
        access_method,
        range: Some(range),
        ..
    } = constraint
    else {
        return Ok(());
    };
    if !is_temporal_key_method(access_method) {
        return Ok(());
    }
    let Some(period) = range.range_column.as_ref() else {
        return Ok(());
    };
    for record in records {
        let value = record_column_value(record, schema, period);
        let text = value.to_cell();
        let text = text.trim();
        if !matches!(value, SqlValue::Null) && (text.eq_ignore_ascii_case("empty") || text == "{}")
        {
            return Err(constraint_violation(
                "23514",
                format!(
                    "empty WITHOUT OVERLAPS value found in column \"{period}\" in relation \"{}\"",
                    catalog_display_name(&schema.name)
                ),
                Some(catalog_display_name(&schema.name)),
                Some(period.clone()),
                None,
            ));
        }
    }
    Ok(())
}

/// Temporal foreign keys (`FOREIGN KEY (id, PERIOD valid_at) REFERENCES ...`) are not
/// implemented yet: refuse them explicitly (0A000) rather than with a syntax error.
fn refuse_period_foreign_keys(sql: &str) -> Result<()> {
    let upper = sql.to_ascii_uppercase();
    if !upper.contains("PERIOD") || !(upper.contains("FOREIGN KEY") || upper.contains("REFERENCES"))
    {
        return Ok(());
    }
    // Only a `PERIOD <column>` element of a FOREIGN KEY (...) or REFERENCES t (...) column list
    // counts; a column that is merely named "period" does not.
    let mut lists = Vec::new();
    for (at, _) in upper.match_indices("FOREIGN KEY") {
        lists.push(at + "FOREIGN KEY".len());
    }
    for (at, _) in upper.match_indices("REFERENCES") {
        let rest = &sql[at + "REFERENCES".len()..];
        if let Ok((_, after_name)) = parse_leading_sql_identifier(rest) {
            lists.push(sql.len() - after_name.len());
        }
    }
    for start in lists {
        let rest = sql[start..].trim_start();
        if !rest.starts_with('(') {
            continue;
        }
        let open = sql.len() - rest.len();
        let Some(close) = find_matching_paren_nested(sql, open) else {
            continue;
        };
        let uses_period = split_top_level_commas_nested(&sql[open + 1..close])
            .iter()
            .any(|item| {
                let words = item.split_whitespace().collect::<Vec<_>>();
                words.len() == 2 && words[0].eq_ignore_ascii_case("PERIOD")
            });
        if uses_period {
            return Err(SqlError::Unsupported(
                "foreign keys using PERIOD are not supported yet".to_string(),
            ));
        }
    }
    Ok(())
}
