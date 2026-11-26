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

/// A parsed `FOREIGN KEY (a, PERIOD p) REFERENCES t [(b, PERIOD q)]`.
#[derive(Debug, Clone)]
pub(crate) struct TemporalForeignKeyDef {
    name: Option<String>,
    columns: Vec<String>,
    /// None when the referencing list has no PERIOD element.
    period: Option<String>,
    foreign_table: String,
    /// None: REFERENCES t without a column list (the referenced table's primary key).
    referred: Option<(Vec<String>, Option<String>)>,
    on_delete: Option<String>,
    on_update: Option<String>,
    deferrable: bool,
}

/// A stored temporal foreign key. `columns` and `referred_columns` are the equality parts.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TemporalForeignKey {
    pub(crate) name: String,
    pub(crate) columns: Vec<String>,
    pub(crate) period: String,
    pub(crate) foreign_table: String,
    pub(crate) referred_columns: Vec<String>,
    pub(crate) referred_period: String,
}

#[derive(Debug)]
enum TemporalElement {
    Key(TemporalKeyDef),
    ForeignKey(TemporalForeignKeyDef),
}

#[derive(Debug)]
pub(crate) enum TemporalKeyDdl {
    /// The CREATE TABLE text without its temporal elements, and those elements.
    CreateTable {
        table: String,
        rewritten: String,
        elements: Vec<TemporalElement>,
    },
    AlterTable {
        table: String,
        element: TemporalElement,
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

fn parse_element(element: &str) -> Result<Option<TemporalElement>> {
    if let Some(key) = parse_temporal_element(element)? {
        return Ok(Some(TemporalElement::Key(key)));
    }
    Ok(parse_temporal_foreign_key(element)?.map(TemporalElement::ForeignKey))
}

fn contains_period_reference(sql: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
    upper.contains("PERIOD") && upper.contains("REFERENCES")
}

/// `(a, b, PERIOD p)` -> (["a", "b"], Some("p")).
fn parse_period_list(list: &str) -> Result<(Vec<String>, Option<String>)> {
    let mut columns = Vec::new();
    let mut period = None;
    let items = split_top_level_commas_nested(list);
    for (index, item) in items.iter().enumerate() {
        let trimmed = item.trim();
        let words = trimmed.split_whitespace().collect::<Vec<_>>();
        if words.len() == 2 && words[0].eq_ignore_ascii_case("PERIOD") {
            if index + 1 != items.len() {
                return Err(SqlError::data_exception_public(
                    "42601",
                    "syntax error at or near \"PERIOD\"",
                    None,
                ));
            }
            period = Some(identifier(words[1])?);
        } else {
            columns.push(identifier(trimmed)?);
        }
    }
    Ok((columns, period))
}

/// `[CONSTRAINT n] FOREIGN KEY (list) REFERENCES t [(list)] [ON DELETE a] [ON UPDATE a]
/// [[NOT] DEFERRABLE ...]` where either list uses PERIOD; None otherwise.
fn parse_temporal_foreign_key(element: &str) -> Result<Option<TemporalForeignKeyDef>> {
    if !contains_period_reference(element) {
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
    let Some(after_fk) = strip_prefix_ci(rest, "FOREIGN KEY") else {
        return Ok(None);
    };
    let after_fk = after_fk.trim_start();
    if !after_fk.starts_with('(') {
        return Ok(None);
    }
    let close = find_matching_paren_nested(after_fk, 0)
        .ok_or_else(|| SqlError::InvalidSql("unterminated FOREIGN KEY column list".to_string()))?;
    let (columns, period) = parse_period_list(&after_fk[1..close])?;
    let Some(after_refs) = strip_prefix_ci(after_fk[close + 1..].trim_start(), "REFERENCES ")
    else {
        return Ok(None);
    };
    let (foreign_table, mut tail) = parse_leading_sql_identifier(after_refs)?;
    let foreign_table = if after_refs.trim_start().starts_with('"') {
        foreign_table
    } else {
        normalize_object_name(&foreign_table)
    };
    tail = tail.trim_start();
    let mut referred = None;
    if tail.starts_with('(') {
        let close = find_matching_paren_nested(tail, 0).ok_or_else(|| {
            SqlError::InvalidSql("unterminated REFERENCES column list".to_string())
        })?;
        referred = Some(parse_period_list(&tail[1..close])?);
        tail = tail[close + 1..].trim_start();
    }
    if period.is_none()
        && !referred
            .as_ref()
            .is_some_and(|(_, period)| period.is_some())
    {
        return Ok(None);
    }
    let words = tail
        .split_whitespace()
        .map(str::to_ascii_uppercase)
        .collect::<Vec<_>>();
    let mut on_delete = None;
    let mut on_update = None;
    let mut deferrable = false;
    let mut index = 0;
    let action = |words: &[String], index: &mut usize| -> Result<String> {
        let first = words.get(*index).cloned().unwrap_or_default();
        let two = matches!(first.as_str(), "NO" | "SET");
        let text = if two {
            let second = words.get(*index + 1).cloned().unwrap_or_default();
            *index += 2;
            format!("{first} {second}")
        } else {
            *index += 1;
            first
        };
        Ok(text)
    };
    while index < words.len() {
        match (
            words[index].as_str(),
            words.get(index + 1).map(String::as_str),
        ) {
            ("ON", Some("DELETE")) => {
                index += 2;
                on_delete = Some(action(&words, &mut index)?);
            }
            ("ON", Some("UPDATE")) => {
                index += 2;
                on_update = Some(action(&words, &mut index)?);
            }
            ("NOT", Some("DEFERRABLE")) => index += 2,
            ("DEFERRABLE", _) => {
                deferrable = true;
                index += 1;
            }
            ("INITIALLY", Some("DEFERRED")) => {
                deferrable = true;
                index += 2;
            }
            ("INITIALLY", Some("IMMEDIATE")) => index += 2,
            ("MATCH", Some("SIMPLE")) => index += 2,
            _ => {
                return Err(SqlError::Unsupported(format!(
                    "foreign key option {} with PERIOD is not supported",
                    words[index..].join(" ")
                )))
            }
        }
    }
    Ok(Some(TemporalForeignKeyDef {
        name,
        columns,
        period,
        foreign_table,
        referred,
        on_delete,
        on_update,
        deferrable,
    }))
}

pub(crate) fn raw_temporal_key_ddl(sql: &str) -> Result<Option<TemporalKeyDdl>> {
    if !contains_without_overlaps(sql) && !contains_period_reference(sql) {
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
        let Some(element) = parse_element(after_add)? else {
            return Ok(None);
        };
        return Ok(Some(TemporalKeyDdl::AlterTable {
            table: normalize_object_name(&table),
            element,
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
    let mut elements = Vec::new();
    for element in split_top_level_commas_nested(&statement[open + 1..close]) {
        match parse_element(&element)? {
            Some(parsed) => elements.push(parsed),
            None => kept.push(element),
        }
    }
    if elements.is_empty() {
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
        elements,
    }))
}

impl SqlSession<'_> {
    pub(crate) fn execute_raw_temporal_key_ddl(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let Some(ddl) = raw_temporal_key_ddl(sql)? else {
            refuse_period_foreign_keys(sql)?;
            return Ok(None);
        };
        match ddl {
            TemporalKeyDdl::CreateTable {
                table,
                rewritten,
                elements,
            } => {
                let result = self.execute(&rewritten)?;
                let attach = (|| {
                    let resolved = resolve_session_relation_name(self.db_ref(), &table)?;
                    // Keys first: a foreign key in the same statement may refer to them.
                    for element in &elements {
                        if let TemporalElement::Key(key) = element {
                            self.attach_temporal_key(&resolved, key)?;
                        }
                    }
                    for element in &elements {
                        if let TemporalElement::ForeignKey(foreign_key) = element {
                            self.attach_temporal_foreign_key(&resolved, foreign_key)?;
                        }
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
            TemporalKeyDdl::AlterTable { table, element } => {
                let resolved = resolve_session_relation_name(self.db_ref(), &table)?;
                self.require_table_ownership(&resolved, "ALTER TABLE")?;
                match &element {
                    TemporalElement::Key(key) => self.attach_temporal_key(&resolved, key)?,
                    TemporalElement::ForeignKey(foreign_key) => {
                        self.attach_temporal_foreign_key(&resolved, foreign_key)?
                    }
                }
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

fn invalid_foreign_key(message: impl Into<String>) -> SqlError {
    SqlError::data_exception_public("42830", message, None)
}

/// The temporal key (equality columns, period) that `schema` declares, if any matches.
fn temporal_keys_of(schema: &TableSchema) -> Vec<(bool, Vec<String>, String)> {
    schema
        .constraints
        .iter()
        .filter_map(|constraint| match constraint {
            ConstraintSchema::Exclusion {
                access_method,
                equal_columns,
                range: Some(range),
                ..
            } if is_temporal_key_method(access_method) => Some((
                access_method == TEMPORAL_PRIMARY_KEY_METHOD,
                equal_columns.clone(),
                range.range_column.clone()?,
            )),
            _ => None,
        })
        .collect()
}

fn column_type(schema: &TableSchema, column: &str) -> String {
    schema
        .column(column)
        .map(|column| column.pg_type.to_ascii_lowercase())
        .unwrap_or_default()
}

/// Whether `child_period` lies within the union of `parent_periods` (PostgreSQL checks
/// `range_agg(parent.period) @> child.period`).
fn period_covered(
    child_period: &SqlValue,
    child_type: &str,
    parent_periods: Vec<SqlValue>,
    parent_type: &str,
) -> Result<bool> {
    if parent_periods.is_empty() {
        return Ok(false);
    }
    let Some((_, multirange_type)) = range_family(parent_type) else {
        return Ok(false);
    };
    let union = range_aggregate_values(parent_periods, parent_type, false)?;
    if matches!(union, SqlValue::Null) {
        return Ok(false);
    }
    Ok(matches!(
        eval_range_binary_value(
            union,
            &BinaryOperator::AtArrow,
            child_period.clone(),
            Some(multirange_type),
            Some(child_type),
        )?,
        Some(SqlValue::Bool(true))
    ))
}

/// Child-side check of one written row against every temporal foreign key of its table.
pub(crate) fn validate_temporal_foreign_keys(
    db: &BicDb,
    tx: Option<&Transaction>,
    table: &str,
    schema: &TableSchema,
    record: &Record,
    before: Option<&Record>,
) -> Result<()> {
    if schema.referential_checks_disabled {
        return Ok(());
    }
    for foreign_key in &schema.temporal_foreign_keys {
        let mut columns = foreign_key.columns.clone();
        columns.push(foreign_key.period.clone());
        let values = record_column_values(record, schema, &columns);
        // MATCH SIMPLE: any NULL component exempts the row.
        if values.iter().any(|value| matches!(value, SqlValue::Null)) {
            continue;
        }
        if let Some(before) = before {
            let old = record_column_values(before, schema, &columns);
            if before.id == record.id
                && typed_column_values_not_distinct(schema, &columns, &old, &values)?
            {
                continue;
            }
        }
        let Some(parent) = load_schema_shared(db, &foreign_key.foreign_table)? else {
            return Err(foreign_key_violation(table, &foreign_key.name));
        };
        let key = &values[..foreign_key.columns.len()];
        reserve_foreign_key(tx, &parent, &foreign_key.referred_columns, key)?;
        let mut periods = Vec::new();
        for candidate in current_integrity_records(db, tx, &foreign_key.foreign_table)? {
            let parent_key =
                record_column_values(&candidate, &parent, &foreign_key.referred_columns);
            if typed_column_values_not_distinct(
                &parent,
                &foreign_key.referred_columns,
                &parent_key,
                key,
            )? {
                let period = record_column_value(&candidate, &parent, &foreign_key.referred_period);
                if !matches!(period, SqlValue::Null) {
                    periods.push(period);
                }
            }
        }
        if !period_covered(
            values.last().expect("period value"),
            &column_type(schema, &foreign_key.period),
            periods,
            &column_type(&parent, &foreign_key.referred_period),
        )? {
            return Err(foreign_key_violation(table, &foreign_key.name));
        }
    }
    Ok(())
}

impl SqlSession<'_> {
    fn attach_temporal_foreign_key(
        &mut self,
        table: &str,
        def: &TemporalForeignKeyDef,
    ) -> Result<()> {
        let mut schema = load_schema(self.db_ref(), table)?
            .ok_or_else(|| SqlError::InvalidCollection(table.to_string()))?;
        let display = catalog_display_name(&schema.name);
        let parent_name = resolve_session_relation_name(self.db_ref(), &def.foreign_table)?;
        let parent = load_schema(self.db_ref(), &parent_name)?.ok_or_else(|| {
            SqlError::data_exception_public(
                "42P01",
                format!("relation \"{}\" does not exist", def.foreign_table),
                None,
            )
        })?;
        let parent_display = catalog_display_name(&parent.name);
        // Rows of a partitioned table live in its partitions, which neither side's checks
        // follow yet: refuse rather than enforce the key on only part of the data.
        if [&schema, &parent]
            .iter()
            .any(|side| side.partitioning.is_some() || side.partition_of.is_some())
        {
            return Err(SqlError::RaisedException {
                sqlstate: "0A000".to_string(),
                message: "foreign keys using PERIOD on partitioned tables are not supported yet"
                    .to_string(),
                detail: None,
            });
        }
        let (referred_columns, referred_period) = match &def.referred {
            Some((columns, period)) => (columns.clone(), period.clone()),
            None => match temporal_keys_of(&parent)
                .into_iter()
                .find(|(primary, ..)| *primary)
            {
                Some((_, columns, period)) => (columns, Some(period)),
                None if !primary_key_columns_for_schema(&parent).is_empty() => {
                    (primary_key_columns_for_schema(&parent), None)
                }
                None => {
                    return Err(invalid_foreign_key(format!(
                        "there is no primary key for referenced table \"{parent_display}\""
                    )))
                }
            },
        };
        let (period, referred_period) =
            match (&def.period, referred_period) {
                (Some(period), Some(referred)) => (period.clone(), referred),
                (Some(_), None) => return Err(invalid_foreign_key(
                    "foreign key uses PERIOD on the referencing table but not the referenced table",
                )),
                (None, Some(_)) => return Err(invalid_foreign_key(
                    "foreign key uses PERIOD on the referenced table but not the referencing table",
                )),
                (None, None) => unreachable!("parsed only with a PERIOD"),
            };
        if referred_columns.len() != def.columns.len() {
            return Err(invalid_foreign_key(
                "number of referencing and referenced columns for foreign key disagree",
            ));
        }
        let mut all_referred = referred_columns.clone();
        all_referred.push(referred_period.clone());
        if all_referred.iter().collect::<BTreeSet<_>>().len() != all_referred.len() {
            return Err(invalid_foreign_key(
                "foreign key referenced-columns list must not contain duplicates",
            ));
        }
        let mut all_columns = def.columns.clone();
        all_columns.push(period.clone());
        for column in &all_columns {
            if schema.column(column).is_none() {
                return Err(SqlError::data_exception_public(
                    "42703",
                    format!(
                        "column \"{column}\" referenced in foreign key constraint does not exist"
                    ),
                    None,
                ));
            }
        }
        for column in &all_referred {
            if parent.column(column).is_none() {
                return Err(SqlError::data_exception_public(
                    "42703",
                    format!(
                        "column \"{column}\" referenced in foreign key constraint does not exist"
                    ),
                    None,
                ));
            }
        }
        let matches_key = temporal_keys_of(&parent)
            .into_iter()
            .any(|(_, columns, key_period)| {
                key_period == referred_period
                    && columns.iter().collect::<BTreeSet<_>>()
                        == referred_columns.iter().collect::<BTreeSet<_>>()
            });
        if !matches_key {
            return Err(invalid_foreign_key(format!(
                "there is no unique constraint matching given keys for referenced table \"{parent_display}\""
            )));
        }
        let name = def.name.clone().unwrap_or_else(|| {
            let mut parts = vec![display.clone()];
            parts.extend(all_columns.iter().cloned());
            parts.push("fkey".to_string());
            parts.join("_")
        });
        for (child, referred) in all_columns.iter().zip(&all_referred) {
            if column_type(&schema, child) != column_type(&parent, referred) {
                return Err(SqlError::data_exception_public(
                    "42804",
                    format!("foreign key constraint \"{name}\" cannot be implemented"),
                    None,
                ));
            }
        }
        for (label, action) in [("UPDATE", &def.on_update), ("DELETE", &def.on_delete)] {
            if action
                .as_deref()
                .is_some_and(|action| action != "NO ACTION")
            {
                return Err(SqlError::RaisedException {
                    sqlstate: "0A000".to_string(),
                    message: format!(
                        "unsupported ON {label} action for foreign key constraint using PERIOD"
                    ),
                    detail: None,
                });
            }
        }
        if def.deferrable {
            return Err(SqlError::Unsupported(
                "DEFERRABLE foreign keys using PERIOD are not supported yet".to_string(),
            ));
        }
        if schema
            .constraints
            .iter()
            .any(|constraint| constraint_name(constraint) == name)
            || schema
                .temporal_foreign_keys
                .iter()
                .any(|existing| existing.name == name)
        {
            return Err(SqlError::duplicate_constraint(table, &name));
        }
        let foreign_key = TemporalForeignKey {
            name,
            columns: def.columns.clone(),
            period,
            foreign_table: parent.name.clone(),
            referred_columns,
            referred_period,
        };
        schema.temporal_foreign_keys.push(foreign_key);
        // Existing rows must already satisfy it.
        for record in current_integrity_records(self.db_ref(), self.tx.as_ref(), table)? {
            let single = TableSchema {
                temporal_foreign_keys: vec![schema.temporal_foreign_keys.last().cloned().unwrap()],
                ..schema.clone()
            };
            validate_temporal_foreign_keys(
                self.db_ref(),
                self.tx.as_ref(),
                table,
                &single,
                &record,
                None,
            )?;
        }
        self.save_session_schema(&schema)
    }

    /// Parent-side check: after `removed` rows give way to `added` rows in `table`, every row
    /// of a table whose temporal foreign key references `table` and overlapped a removed period
    /// must still be covered (NO ACTION, checked for the whole batch).
    pub(crate) fn check_temporal_foreign_key_parents(
        &mut self,
        table: &str,
        schema: &TableSchema,
        removed: &[Record],
        added: &[Record],
    ) -> Result<()> {
        if removed.is_empty() {
            return Ok(());
        }
        let children = list_schemas(self.db_ref())?
            .into_iter()
            .filter(|child| {
                child
                    .temporal_foreign_keys
                    .iter()
                    .any(|foreign_key| foreign_key.foreign_table == table)
            })
            .collect::<Vec<_>>();
        if children.is_empty() {
            return Ok(());
        }
        let removed_ids = removed
            .iter()
            .map(|record| record.id.as_str())
            .collect::<BTreeSet<_>>();
        let mut parents = current_integrity_records(self.db_ref(), self.tx.as_ref(), table)?
            .into_iter()
            .filter(|record| !removed_ids.contains(record.id.as_str()))
            .collect::<Vec<_>>();
        parents.extend(added.iter().cloned());
        let parent_display = catalog_display_name(&schema.name);
        for child in &children {
            for foreign_key in child
                .temporal_foreign_keys
                .iter()
                .filter(|fk| fk.foreign_table == table)
            {
                for record in removed {
                    let key = record_column_values(record, schema, &foreign_key.referred_columns);
                    reserve_foreign_key_mutation(
                        self.tx.as_ref(),
                        schema,
                        &foreign_key.referred_columns,
                        &key,
                    )?;
                }
                let mut columns = foreign_key.columns.clone();
                columns.push(foreign_key.period.clone());
                for child_record in
                    current_integrity_records(self.db_ref(), self.tx.as_ref(), &child.name)?
                {
                    let values = record_column_values(&child_record, child, &columns);
                    if values.iter().any(|value| matches!(value, SqlValue::Null)) {
                        continue;
                    }
                    let key = &values[..foreign_key.columns.len()];
                    // Only rows that referenced a removed parent row can be affected.
                    let affected = removed.iter().any(|record| {
                        let parent_key =
                            record_column_values(record, schema, &foreign_key.referred_columns);
                        typed_column_values_not_distinct(
                            schema,
                            &foreign_key.referred_columns,
                            &parent_key,
                            key,
                        )
                        .unwrap_or(true)
                    });
                    if !affected {
                        continue;
                    }
                    let mut periods = Vec::new();
                    for parent in &parents {
                        let parent_key =
                            record_column_values(parent, schema, &foreign_key.referred_columns);
                        if typed_column_values_not_distinct(
                            schema,
                            &foreign_key.referred_columns,
                            &parent_key,
                            key,
                        )? {
                            let period =
                                record_column_value(parent, schema, &foreign_key.referred_period);
                            if !matches!(period, SqlValue::Null) {
                                periods.push(period);
                            }
                        }
                    }
                    if !period_covered(
                        values.last().expect("period value"),
                        &column_type(child, &foreign_key.period),
                        periods,
                        &column_type(schema, &foreign_key.referred_period),
                    )? {
                        return Err(constraint_violation(
                            "23503",
                            format!(
                                "update or delete on table \"{parent_display}\" violates foreign key constraint \"{}\" on table \"{}\"",
                                foreign_key.name,
                                catalog_display_name(&child.name)
                            ),
                            Some(catalog_display_name(&child.name)),
                            None,
                            Some(foreign_key.name.clone()),
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// DROP TABLE of a table that temporal foreign keys reference: refused without CASCADE;
    /// with CASCADE those foreign keys are dropped (the referencing tables stay).
    pub(crate) fn drop_temporal_foreign_keys_referencing(
        &mut self,
        table: &str,
        dropping: &[String],
        cascade: bool,
    ) -> Result<()> {
        for mut child in list_schemas(self.db_ref())? {
            if dropping.iter().any(|name| *name == child.name) || child.name == table {
                continue;
            }
            let before = child.temporal_foreign_keys.len();
            if !child
                .temporal_foreign_keys
                .iter()
                .any(|fk| fk.foreign_table == table)
            {
                continue;
            }
            if !cascade {
                let fk = child
                    .temporal_foreign_keys
                    .iter()
                    .find(|fk| fk.foreign_table == table)
                    .unwrap();
                return Err(SqlError::coded(
                    "2BP01",
                    format!(
                        "cannot drop table {} because other objects depend on it",
                        catalog_display_name(table)
                    ),
                    Some(format!(
                        "constraint {} on table {} depends on table {}",
                        fk.name,
                        catalog_display_name(&child.name),
                        catalog_display_name(table)
                    )),
                    Some("Use DROP ... CASCADE to drop the dependent objects too.".to_string()),
                ));
            }
            let original = child.clone();
            child
                .temporal_foreign_keys
                .retain(|fk| fk.foreign_table != table);
            if child.temporal_foreign_keys.len() != before {
                self.capture_table_state_undo(&original.name, &original)?;
                self.save_session_schema(&child)?;
            }
        }
        Ok(())
    }
}

/// Whether any table's temporal foreign key references `table` (fast write paths that skip
/// parent-side foreign-key work must not skip these).
pub(crate) fn has_inbound_temporal_foreign_keys(db: &BicDb, table: &str) -> Result<bool> {
    Ok(list_schemas_shared(db)?.iter().any(|schema| {
        schema
            .temporal_foreign_keys
            .iter()
            .any(|foreign_key| foreign_key.foreign_table == table)
    }))
}
