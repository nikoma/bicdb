//! SELECT execution helpers: set-operation combination, qualified wildcard expansion, row value lookup, row aggregates (RowAggregate), numeric row extraction, virtual table column lists, and string/oid selection filters.
//!
//! Extracted verbatim from `lib.rs` (phase-1 mechanical module split).
//! Moved items were bumped to `pub(crate)` so existing call sites keep
//! resolving; `lib.rs` re-exports this module via `pub use select_exec::*;`.

// Glue import: bring every crate-root item (including the root's private
// imports and the other split modules' re-exports) into scope so the moved
// code compiles unchanged.
use crate::*;

pub(crate) fn set_expr_insert(expr: &SetExpr) -> Option<&Insert> {
    match expr {
        SetExpr::Insert(Statement::Insert(insert)) => Some(insert),
        SetExpr::Query(query) => set_expr_insert(query.body.as_ref()),
        _ => None,
    }
}

pub(crate) fn set_expr_update(expr: &SetExpr) -> Option<&sqlparser::ast::Update> {
    match expr {
        SetExpr::Update(Statement::Update(update)) => Some(update),
        SetExpr::Query(query) => set_expr_update(query.body.as_ref()),
        _ => None,
    }
}

pub(crate) fn parent_store_path_for_id(
    id: i64,
    parent_by_id: &BTreeMap<i64, Option<i64>>,
    path_cache: &mut BTreeMap<i64, String>,
    visiting: &mut BTreeSet<i64>,
) -> Result<String> {
    if let Some(path) = path_cache.get(&id) {
        return Ok(path.clone());
    }
    if !visiting.insert(id) {
        return Err(SqlError::InvalidSql(
            "cycle detected while computing parent_path".to_string(),
        ));
    }
    let path = match parent_by_id.get(&id).copied().flatten() {
        Some(parent_id) if parent_by_id.contains_key(&parent_id) => {
            let parent_path =
                parent_store_path_for_id(parent_id, parent_by_id, path_cache, visiting)?;
            format!("{parent_path}{id}/")
        }
        _ => format!("{id}/"),
    };
    visiting.remove(&id);
    path_cache.insert(id, path.clone());
    Ok(path)
}

pub(crate) fn set_expr_delete(expr: &SetExpr) -> Option<&Delete> {
    match expr {
        SetExpr::Delete(Statement::Delete(delete)) => Some(delete),
        SetExpr::Query(query) => set_expr_delete(query.body.as_ref()),
        _ => None,
    }
}

pub(crate) fn view_key(name: &str) -> String {
    cte_key(name)
}

pub(crate) fn cte_columns(
    name: &str,
    aliases: &[TableAliasColumnDef],
    result_columns: &[String],
) -> Result<Vec<String>> {
    table_alias_columns(name, name, aliases, result_columns)
}

pub(crate) fn table_alias_columns(
    table: &str,
    alias: &str,
    aliases: &[TableAliasColumnDef],
    result_columns: &[String],
) -> Result<Vec<String>> {
    if aliases.is_empty() {
        return Ok(result_columns.to_vec());
    }
    if aliases.len() != result_columns.len() {
        return Err(SqlError::InvalidSql(format!(
            "table alias \"{alias}\" has {} columns available but {} columns specified for \"{table}\"",
            result_columns.len(),
            aliases.len()
        )));
    }
    Ok(aliases
        .iter()
        .map(|column| column.name.value.clone())
        .collect())
}

pub(crate) fn btree_null_row_for_columns(columns: &[String]) -> BTreeMap<String, SqlValue> {
    columns
        .iter()
        .map(|column| (column.clone(), SqlValue::Null))
        .collect()
}

pub(crate) fn merge_row_set_columns(mut left: Vec<String>, right: Vec<String>) -> Vec<String> {
    for column in right {
        if !left.iter().any(|existing| existing == &column) {
            left.push(column);
        }
    }
    left
}

pub(crate) fn merged_slot_sources(
    left_columns: &[String],
    right_columns: &[String],
) -> Vec<JoinedSlotSource> {
    let mut sources = (0..left_columns.len())
        .map(JoinedSlotSource::Left)
        .collect::<Vec<_>>();
    for (idx, column) in right_columns.iter().enumerate() {
        if !left_columns.iter().any(|existing| existing == column) {
            sources.push(JoinedSlotSource::Right(idx));
        }
    }
    sources
}

pub(crate) fn virtual_table_columns_for_empty_join(table: &str) -> Option<Vec<String>> {
    virtual_table_columns(table).or_else(|| {
        let table = table.strip_prefix("pg_catalog.").unwrap_or(table);
        match table {
            "pg_class" => Some(virtual_row_column_names(pg_class_row(
                0,
                "",
                0,
                None,
                "r",
                0,
                false,
                false,
                0.0,
                0,
                false,
                false,
                false,
                SqlValue::Null,
                SqlValue::Null,
                0,
                false,
                false,
            ))),
            "pg_index" => Some(virtual_row_column_names(pg_index_row(
                0,
                0,
                false,
                false,
                Vec::new(),
                Vec::new(),
                Vec::new(),
                None,
                false,
                None,
                String::new(),
            ))),
            "pg_constraint" => Some(virtual_row_column_names(pg_constraint_row(
                PgConstraintRow {
                    oid: 0,
                    name: String::new(),
                    contype: "c",
                    conrelid: 0,
                    conindid: 0,
                    confrelid: 0,
                    confupdtype: " ",
                    confdeltype: " ",
                    conkey: Vec::new(),
                    confkey: Vec::new(),
                    conbin: None,
                    convalidated: true,
                },
            ))),
            "pg_namespace" => Some(virtual_row_column_names(virtual_row([
                ("oid", SqlValue::Int(0)),
                ("nspname", SqlValue::String(String::new())),
                ("nspowner", SqlValue::Int(10)),
                ("nspacl", SqlValue::Null),
            ]))),
            "pg_type" => Some(virtual_row_column_names(pg_type_row(
                "text", 25, 0, 1009, false,
            ))),
            "pg_enum" => Some(virtual_row_column_names(virtual_row([
                ("oid", SqlValue::Int(0)),
                ("enumtypid", SqlValue::Int(0)),
                ("enumsortorder", SqlValue::Float(0.0)),
                ("enumlabel", SqlValue::String(String::new())),
            ]))),
            _ => None,
        }
    })
}

pub(crate) fn virtual_row_column_names(row: BTreeMap<String, SqlValue>) -> Vec<String> {
    row.into_keys().collect()
}

pub(crate) fn add_virtual_tableoid_column(table: &str, columns: &mut Vec<String>) {
    if virtual_catalog_table_oid(table).is_some()
        && !columns
            .iter()
            .any(|column| column.eq_ignore_ascii_case("tableoid"))
    {
        columns.insert(0, "tableoid".to_string());
    }
}

pub(crate) fn qualified_wildcard_columns(
    qualifier: &SelectItemQualifiedWildcardKind,
    wildcard_columns: &[String],
) -> Result<Vec<(String, String)>> {
    let SelectItemQualifiedWildcardKind::ObjectName(qualifier) = qualifier else {
        return Err(SqlError::Unsupported(format!(
            "unsupported row projection {qualifier}"
        )));
    };
    let mut qualifiers = Vec::new();
    let object_name = object_name(qualifier)?;
    for candidate in [
        object_name.clone(),
        relation_name(qualifier)?,
        unqualified_relation(&object_name),
    ] {
        if !qualifiers
            .iter()
            .any(|existing: &String| existing.eq_ignore_ascii_case(&candidate))
        {
            qualifiers.push(candidate);
        }
    }

    let columns = wildcard_columns
        .iter()
        .filter_map(|column| {
            let (prefix, output) = column.split_once('.')?;
            if matches!(
                output.to_ascii_lowercase().as_str(),
                "tableoid" | "xmin" | "xmax" | "cmin" | "cmax" | "ctid"
            ) {
                return None;
            }
            qualifiers
                .iter()
                .any(|qualifier| prefix.eq_ignore_ascii_case(qualifier))
                .then(|| (column.clone(), output.to_string()))
        })
        .collect::<Vec<_>>();
    if columns.is_empty() && wildcard_columns.iter().all(|column| !column.contains('.')) {
        return Ok(wildcard_columns
            .iter()
            .filter(|column| {
                !matches!(
                    column.to_ascii_lowercase().as_str(),
                    "tableoid" | "xmin" | "xmax" | "cmin" | "cmax" | "ctid"
                )
            })
            .map(|column| (column.clone(), column.clone()))
            .collect());
    }
    if columns.is_empty() {
        return Err(SqlError::InvalidSql(format!(
            "missing FROM-clause entry for table \"{}\"",
            object_name
        )));
    }
    Ok(columns)
}

pub(crate) fn row_wildcard_output_column(column: &str) -> String {
    column.rsplit('.').next().unwrap_or(column).to_string()
}

pub(crate) fn aliased_row_output_columns(alias: &str, columns: &[String]) -> Vec<String> {
    columns
        .iter()
        .map(|column| format!("{alias}.{column}"))
        .collect()
}

pub(crate) fn insert_row_value(
    row: &mut SqlRow,
    table: &str,
    alias: &str,
    column: &str,
    value: SqlValue,
) {
    row.entry(column.to_string())
        .or_insert_with(|| value.clone());
    row.insert(format!("{alias}.{column}"), value.clone());
    if alias != table {
        row.insert(format!("{table}.{column}"), value);
    }
}

pub(crate) fn update_returning_slot_row_from_assignment(
    table: &str,
    alias: &str,
    schema: Option<&TableSchema>,
    record: &Record,
    target_columns: &[String],
    assignment_columns: &[String],
    assignment_row: &[SqlValue],
) -> Result<SlotRow> {
    let target_row = slot_row_from_record(table, alias, schema, record)?;
    let mut row = assignment_row.to_vec();
    let target_len = target_columns
        .len()
        .min(assignment_columns.len())
        .min(row.len());
    for target_idx in 0..target_len {
        row[target_idx] = target_row
            .get(target_idx)
            .cloned()
            .unwrap_or(SqlValue::Null);
    }
    Ok(row)
}

/// `update_returning_slot_row_from_assignment` for the case where only the
/// assigned columns can differ from the candidate's target slots: those
/// columns are decoded from the new record and written into every target
/// slot that names them (alias- and table-qualified); nothing else is
/// touched. `target_columns` is the `row_output_columns` layout of the
/// candidate's leading slots.
pub(crate) fn update_returning_slot_row_patched(
    table: &str,
    alias: &str,
    schema: &TableSchema,
    record: &Record,
    target_columns: &[String],
    assignment_row: &[SqlValue],
    assigned: &[String],
) -> SlotRow {
    let mut row = assignment_row.to_vec();
    for column in assigned {
        let value = record_column_value(record, schema, column);
        for (idx, target) in target_columns.iter().enumerate() {
            let Some((relation, name)) = target.rsplit_once('.') else {
                continue;
            };
            if name == column && (relation == alias || relation == table) {
                if let Some(slot) = row.get_mut(idx) {
                    *slot = value.clone();
                }
            }
        }
    }
    row
}

pub(crate) fn merge_rows(left: &SqlRow, right: &SqlRow) -> SqlRow {
    #[cfg(test)]
    SQL_MERGE_ROWS_CALLS.with(|calls| *calls.borrow_mut() += 1);

    let mut row = left.clone();
    for (key, value) in right {
        if key.contains('.') {
            row.insert(key.clone(), value.clone());
        } else {
            row.entry(key.clone()).or_insert_with(|| value.clone());
        }
    }
    row
}

pub(crate) fn row_value_from_parts(row: &SqlRow, parts: &[String]) -> SqlValue {
    row_value_from_parts_opt(row, parts).unwrap_or(SqlValue::Null)
}

pub(crate) fn row_value_from_parts_opt(row: &SqlRow, parts: &[String]) -> Option<SqlValue> {
    if parts.is_empty() {
        return None;
    }
    if parts.len() == 1 {
        return row_value_for_key(row, &parts[0]).cloned();
    }
    #[cfg(test)]
    SQL_ROW_LOOKUP_COMPOUND_JOINS.with(|joins| *joins.borrow_mut() += 1);
    let full_key = parts.join(".");
    if let Some(value) = row_value_for_key(row, &full_key) {
        return Some(value.clone());
    }
    let qualified_column = format!("{}.{}", parts[0], parts[1]);
    if let Some(value) = row_value_for_key(row, &qualified_column) {
        return if parts.len() == 2 {
            Some(value.clone())
        } else {
            Some(
                sql_json_path(value, &parts[2..])
                    .map(json_to_sql_value)
                    .unwrap_or(SqlValue::Null),
            )
        };
    }
    if parts[0].eq_ignore_ascii_case("metadata") {
        if let Some(value) = row_value_for_key(row, "metadata") {
            return Some(
                sql_json_path(value, &parts[1..])
                    .map(json_to_sql_value)
                    .unwrap_or(SqlValue::Null),
            );
        }
    }
    if let Some(value) = row_value_for_key(row, &parts[0]) {
        if let Some(path_value) = sql_json_path(value, &parts[1..]) {
            return Some(json_to_sql_value(path_value));
        }
    }
    row_value_for_key(row, parts.last().expect("checked non-empty parts")).cloned()
}

pub(crate) fn routine_var_from_name(
    vars: &BTreeMap<String, SqlValue>,
    name: &str,
) -> Option<SqlValue> {
    let normalized = normalize_object_name(name);
    vars.get(&normalized).cloned().or_else(|| {
        vars.iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.clone())
    })
}

pub(crate) fn routine_var_from_ident(
    vars: &BTreeMap<String, SqlValue>,
    ident: &Ident,
) -> Option<SqlValue> {
    if ident.quote_style.is_some() {
        return None;
    }
    routine_var_from_name(vars, &ident.value)
}

pub(crate) fn routine_var_from_parts(
    vars: &BTreeMap<String, SqlValue>,
    parts: &[String],
) -> Result<Option<SqlValue>> {
    let Some((head, tail)) = parts.split_first() else {
        return Ok(None);
    };
    let Some(value) = routine_var_from_name(vars, head) else {
        return Ok(None);
    };
    if tail.is_empty() {
        return Ok(Some(value));
    }
    let SqlValue::Json(value) = value else {
        return Ok(Some(SqlValue::Null));
    };
    Ok(Some(
        json_path_case_insensitive(&value, tail)
            .map(json_to_sql_value)
            .unwrap_or(SqlValue::Null),
    ))
}

pub(crate) fn routine_var_from_value(
    vars: &BTreeMap<String, SqlValue>,
    value: &ValueWithSpan,
) -> Option<SqlValue> {
    let Value::Placeholder(name) = &value.value else {
        return None;
    };
    routine_var_from_name(vars, name)
}

pub(crate) fn row_value_for_key<'a>(row: &'a SqlRow, key: &str) -> Option<&'a SqlValue> {
    row.get(key).or_else(|| {
        row.iter()
            .find_map(|(candidate, value)| candidate.eq_ignore_ascii_case(key).then_some(value))
    })
}

pub(crate) fn pg_indexdef_from_row(
    row: &SqlRow,
    indexrelid_expr: &Expr,
    indexrelid_value: &SqlValue,
) -> Option<String> {
    let expected_oid = sql_value_i64(indexrelid_value)?;
    for (oid_key, definition_key) in pg_indexdef_row_key_candidates(indexrelid_expr) {
        if sql_value_i64(row.get(&oid_key)?)? != expected_oid {
            continue;
        }
        if let SqlValue::String(definition) = row.get(&definition_key)? {
            return Some(definition.clone());
        }
    }
    None
}

pub(crate) fn pg_indexdef_from_slot_row(
    lookup: &SlotRowLookup,
    row: &[SqlValue],
    indexrelid_expr: &Expr,
    indexrelid_value: &SqlValue,
) -> Option<String> {
    let expected_oid = sql_value_i64(indexrelid_value)?;
    for (oid_key, definition_key) in pg_indexdef_row_key_candidates(indexrelid_expr) {
        if sql_value_i64(lookup.value_for_key(row, &oid_key)?)? != expected_oid {
            continue;
        }
        if let SqlValue::String(definition) = lookup.value_for_key(row, &definition_key)? {
            return Some(definition.clone());
        }
    }
    None
}

pub(crate) fn pg_indexdef_row_key_candidates(indexrelid_expr: &Expr) -> Vec<(String, String)> {
    match indexrelid_expr {
        Expr::Identifier(ident) if ident.value.eq_ignore_ascii_case("indexrelid") => {
            vec![("indexrelid".to_string(), "indexdef".to_string())]
        }
        Expr::CompoundIdentifier(idents)
            if idents
                .last()
                .is_some_and(|ident| ident.value.eq_ignore_ascii_case("indexrelid")) =>
        {
            let prefix = idents[..idents.len().saturating_sub(1)]
                .iter()
                .map(|ident| ident.value.as_str())
                .collect::<Vec<_>>()
                .join(".");
            if prefix.is_empty() {
                vec![("indexrelid".to_string(), "indexdef".to_string())]
            } else {
                vec![
                    (format!("{prefix}.indexrelid"), format!("{prefix}.indexdef")),
                    ("indexrelid".to_string(), "indexdef".to_string()),
                ]
            }
        }
        Expr::Nested(expr) => pg_indexdef_row_key_candidates(expr),
        Expr::Cast { expr, .. } => pg_indexdef_row_key_candidates(expr),
        _ => Vec::new(),
    }
}

pub(crate) fn group_by_exprs(select: &Select) -> Result<Vec<Expr>> {
    match &select.group_by {
        GroupByExpr::Expressions(exprs, modifiers) => {
            if !modifiers.is_empty() {
                return Err(SqlError::Unsupported(
                    "GROUP BY modifiers are not supported".to_string(),
                ));
            }
            Ok(exprs.clone())
        }
        GroupByExpr::All(modifiers) => {
            if modifiers.is_empty() {
                Err(SqlError::Unsupported(
                    "GROUP BY ALL is not supported".to_string(),
                ))
            } else {
                Err(SqlError::Unsupported(
                    "GROUP BY ALL modifiers are not supported".to_string(),
                ))
            }
        }
    }
}

pub(crate) fn has_group_by(select: &Select) -> Result<bool> {
    Ok(!group_by_exprs(select)?.is_empty())
}

pub(crate) fn select_item_expr_and_alias(item: &SelectItem) -> Result<(&Expr, Option<String>)> {
    match item {
        SelectItem::UnnamedExpr(expr) => Ok((expr, None)),
        SelectItem::ExprWithAlias { expr, alias } => Ok((expr, Some(alias.value.clone()))),
        other => Err(SqlError::Unsupported(format!(
            "unsupported grouped projection {other}"
        ))),
    }
}

pub(crate) fn resolve_order_by_projection_aliases(
    order_by: Option<&OrderBy>,
    projection: &[SelectItem],
) -> Option<OrderBy> {
    let mut resolved = order_by.cloned()?;
    let OrderByKind::Expressions(expressions) = &mut resolved.kind else {
        return Some(resolved);
    };
    for order in expressions {
        let Expr::Identifier(identifier) = &order.expr else {
            continue;
        };
        let Some(expr) = projection.iter().find_map(|item| match item {
            SelectItem::ExprWithAlias { expr, alias }
                if alias.value.eq_ignore_ascii_case(&identifier.value) =>
            {
                Some(expr)
            }
            _ => None,
        }) else {
            continue;
        };
        order.expr = expr.clone();
    }
    Some(resolved)
}

pub(crate) fn validate_window_functions(select: &Select, query: &Query) -> Result<()> {
    for function in query_window_functions(select, query)? {
        validate_window_function(&function, select)?;
    }
    Ok(())
}

pub(crate) fn window_column_name(function: &Function) -> String {
    format!("__bicdb_window__{function}")
}

pub(crate) fn group_aggregate_column_name(function: &Function) -> String {
    format!("__bicdb_group_aggregate__{function}")
}

pub(crate) fn query_window_functions(select: &Select, query: &Query) -> Result<Vec<Function>> {
    let mut functions = Vec::new();
    for item in &select.projection {
        if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item {
            collect_window_functions(expr, &mut functions);
        }
    }
    if let Some(order_by) = &query.order_by {
        let OrderByKind::Expressions(expressions) = &order_by.kind else {
            return Err(SqlError::Unsupported(
                "ORDER BY ALL is not supported".to_string(),
            ));
        };
        for order in expressions {
            collect_window_functions(&order.expr, &mut functions);
        }
    }
    let mut seen = BTreeSet::new();
    functions.retain(|function| seen.insert(window_column_name(function)));
    Ok(functions)
}

pub(crate) fn select_has_window_functions(select: &Select, query: &Query) -> Result<bool> {
    Ok(!query_window_functions(select, query)?.is_empty())
}

pub(crate) fn query_group_aggregate_functions(
    select: &Select,
    query: &Query,
) -> Result<Vec<Function>> {
    let mut functions = Vec::new();
    for item in &select.projection {
        if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item {
            collect_group_aggregate_functions(expr, &mut functions);
        }
    }
    if let Some(having) = &select.having {
        collect_group_aggregate_functions(having, &mut functions);
    }
    if let Some(order_by) = &query.order_by {
        if let OrderByKind::Expressions(expressions) = &order_by.kind {
            for order in expressions {
                collect_group_aggregate_functions(&order.expr, &mut functions);
            }
        }
    }
    for window in query_window_functions(select, query)? {
        let spec = resolve_window_spec(&window, select)?;
        for expr in &spec.partition_by {
            collect_group_aggregate_functions(expr, &mut functions);
        }
        for order in &spec.order_by {
            collect_group_aggregate_functions(&order.expr, &mut functions);
        }
    }
    let mut seen = BTreeSet::new();
    functions.retain(|function| seen.insert(group_aggregate_column_name(function)));
    Ok(functions)
}

pub(crate) fn collect_group_aggregate_functions(expr: &Expr, out: &mut Vec<Function>) {
    match expr {
        Expr::Function(function) => {
            if function.over.is_none() && is_aggregate_function(function) {
                out.push(function.clone());
            }
            for arg in function_args(function) {
                collect_group_aggregate_functions(&arg, out);
            }
            if let Some(filter) = &function.filter {
                collect_group_aggregate_functions(filter, out);
            }
        }
        Expr::BinaryOp { left, right, .. }
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::AnyOp { left, right, .. }
        | Expr::AllOp { left, right, .. } => {
            collect_group_aggregate_functions(left, out);
            collect_group_aggregate_functions(right, out);
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::Nested(expr)
        | Expr::Collate { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsTrue(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::IsNotFalse(expr)
        | Expr::IsUnknown(expr)
        | Expr::IsNotUnknown(expr)
        | Expr::Extract { expr, .. } => collect_group_aggregate_functions(expr, out),
        Expr::Position { expr, r#in } => {
            collect_group_aggregate_functions(expr, out);
            collect_group_aggregate_functions(r#in, out);
        }
        Expr::InList { expr, list, .. } => {
            collect_group_aggregate_functions(expr, out);
            for expr in list {
                collect_group_aggregate_functions(expr, out);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_group_aggregate_functions(expr, out);
            collect_group_aggregate_functions(low, out);
            collect_group_aggregate_functions(high, out);
        }
        Expr::Like { expr, pattern, .. }
        | Expr::ILike { expr, pattern, .. }
        | Expr::SimilarTo { expr, pattern, .. }
        | Expr::RLike { expr, pattern, .. } => {
            collect_group_aggregate_functions(expr, out);
            collect_group_aggregate_functions(pattern, out);
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                collect_group_aggregate_functions(operand, out);
            }
            for condition in conditions {
                collect_group_aggregate_functions(&condition.condition, out);
                collect_group_aggregate_functions(&condition.result, out);
            }
            if let Some(else_result) = else_result {
                collect_group_aggregate_functions(else_result, out);
            }
        }
        Expr::Array(array) => {
            for expr in &array.elem {
                collect_group_aggregate_functions(expr, out);
            }
        }
        Expr::CompoundFieldAccess { root, access_chain } => {
            collect_group_aggregate_functions(root, out);
            for access in access_chain {
                if let AccessExpr::Dot(expr) = access {
                    collect_group_aggregate_functions(expr, out);
                }
            }
        }
        Expr::Interval(interval) => collect_group_aggregate_functions(&interval.value, out),
        Expr::AtTimeZone {
            timestamp,
            time_zone,
        } => {
            collect_group_aggregate_functions(timestamp, out);
            collect_group_aggregate_functions(time_zone, out);
        }
        Expr::Tuple(exprs) | Expr::Struct { values: exprs, .. } => {
            for expr in exprs {
                collect_group_aggregate_functions(expr, out);
            }
        }
        _ => {}
    }
}

pub(crate) fn collect_window_functions(expr: &Expr, out: &mut Vec<Function>) {
    match expr {
        Expr::Function(function) => {
            if function.over.is_some() {
                out.push(function.clone());
            }
            for arg in function_args(function) {
                collect_window_functions(&arg, out);
            }
            if let Some(filter) = &function.filter {
                collect_window_functions(filter, out);
            }
        }
        Expr::BinaryOp { left, right, .. }
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::AnyOp { left, right, .. }
        | Expr::AllOp { left, right, .. } => {
            collect_window_functions(left, out);
            collect_window_functions(right, out);
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::Nested(expr)
        | Expr::Collate { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsTrue(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::IsNotFalse(expr)
        | Expr::IsUnknown(expr)
        | Expr::IsNotUnknown(expr)
        | Expr::Extract { expr, .. }
        | Expr::Ceil { expr, .. }
        | Expr::Floor { expr, .. }
        | Expr::OuterJoin(expr)
        | Expr::Prior(expr) => collect_window_functions(expr, out),
        Expr::Position { expr, r#in } => {
            collect_window_functions(expr, out);
            collect_window_functions(r#in, out);
        }
        Expr::InList { expr, list, .. } => {
            collect_window_functions(expr, out);
            for expr in list {
                collect_window_functions(expr, out);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_window_functions(expr, out);
            collect_window_functions(low, out);
            collect_window_functions(high, out);
        }
        Expr::Like { expr, pattern, .. }
        | Expr::ILike { expr, pattern, .. }
        | Expr::SimilarTo { expr, pattern, .. }
        | Expr::RLike { expr, pattern, .. } => {
            collect_window_functions(expr, out);
            collect_window_functions(pattern, out);
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                collect_window_functions(operand, out);
            }
            for condition in conditions {
                collect_window_functions(&condition.condition, out);
                collect_window_functions(&condition.result, out);
            }
            if let Some(else_result) = else_result {
                collect_window_functions(else_result, out);
            }
        }
        Expr::Array(array) => {
            for expr in &array.elem {
                collect_window_functions(expr, out);
            }
        }
        Expr::CompoundFieldAccess { root, access_chain } => {
            collect_window_functions(root, out);
            for access in access_chain {
                match access {
                    AccessExpr::Dot(expr) => collect_window_functions(expr, out),
                    AccessExpr::Subscript(subscript) => {
                        collect_window_functions_subscript(subscript, out)
                    }
                }
            }
        }
        Expr::Interval(interval) => collect_window_functions(&interval.value, out),
        Expr::Trim {
            trim_what,
            expr,
            trim_characters,
            ..
        } => {
            if let Some(trim_what) = trim_what {
                collect_window_functions(trim_what, out);
            }
            collect_window_functions(expr, out);
            if let Some(characters) = trim_characters {
                for character in characters {
                    collect_window_functions(character, out);
                }
            }
        }
        Expr::JsonAccess { value, .. }
        | Expr::InSubquery { expr: value, .. }
        | Expr::InUnnest { expr: value, .. }
        | Expr::Prefixed { value, .. }
        | Expr::Named { expr: value, .. } => collect_window_functions(value, out),
        Expr::Convert { expr, styles, .. } => {
            collect_window_functions(expr, out);
            for style in styles {
                collect_window_functions(style, out);
            }
        }
        Expr::AtTimeZone {
            timestamp,
            time_zone,
        } => {
            collect_window_functions(timestamp, out);
            collect_window_functions(time_zone, out);
        }
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            collect_window_functions(expr, out);
            if let Some(from) = substring_from {
                collect_window_functions(from, out);
            }
            if let Some(for_expr) = substring_for {
                collect_window_functions(for_expr, out);
            }
        }
        Expr::Overlay {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
        } => {
            collect_window_functions(expr, out);
            collect_window_functions(overlay_what, out);
            collect_window_functions(overlay_from, out);
            if let Some(overlay_for) = overlay_for {
                collect_window_functions(overlay_for, out);
            }
        }
        Expr::Tuple(exprs) | Expr::Struct { values: exprs, .. } => {
            for expr in exprs {
                collect_window_functions(expr, out);
            }
        }
        Expr::GroupingSets(groups) | Expr::Cube(groups) | Expr::Rollup(groups) => {
            for expr in groups.iter().flatten() {
                collect_window_functions(expr, out);
            }
        }
        Expr::Lambda(lambda) => collect_window_functions(&lambda.body, out),
        _ => {}
    }
}

pub(crate) fn collect_window_functions_subscript(subscript: &Subscript, out: &mut Vec<Function>) {
    match subscript {
        Subscript::Index { index } => collect_window_functions(index, out),
        Subscript::Slice {
            lower_bound,
            upper_bound,
            stride,
        } => {
            if let Some(lower) = lower_bound {
                collect_window_functions(lower, out);
            }
            if let Some(upper) = upper_bound {
                collect_window_functions(upper, out);
            }
            if let Some(stride) = stride {
                collect_window_functions(stride, out);
            }
        }
    }
}

pub(crate) fn validate_window_function(function: &Function, select: &Select) -> Result<()> {
    let name = object_name(&function.name)?.to_ascii_lowercase();
    let name = name.strip_prefix("pg_catalog.").unwrap_or(&name);
    if !matches!(
        name,
        "row_number"
            | "rank"
            | "dense_rank"
            | "percent_rank"
            | "cume_dist"
            | "ntile"
            | "count"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "bool_and"
            | "bool_or"
            | "every"
            | "lag"
            | "lead"
            | "first_value"
            | "last_value"
            | "nth_value"
    ) {
        return Err(SqlError::Unsupported(format!(
            "window function {} is not supported",
            name.to_ascii_uppercase()
        )));
    }
    if function.null_treatment.is_some()
        && !matches!(
            name,
            "lag" | "lead" | "first_value" | "last_value" | "nth_value"
        )
    {
        return Err(SqlError::InvalidSql(format!(
            "NULL treatment is not valid for {}",
            name.to_ascii_uppercase()
        )));
    }
    if !function.within_group.is_empty() {
        return Err(SqlError::Unsupported(
            "WITHIN GROUP is not supported for window functions".to_string(),
        ));
    }
    if function.filter.is_some()
        && !matches!(
            name,
            "count" | "sum" | "avg" | "min" | "max" | "bool_and" | "bool_or" | "every"
        )
    {
        return Err(SqlError::InvalidSql(format!(
            "FILTER is not valid for {}",
            name.to_ascii_uppercase()
        )));
    }
    let arg_count = function_args(function).len();
    let valid_arguments = match name {
        "row_number" | "rank" | "dense_rank" | "percent_rank" | "cume_dist" => arg_count == 0,
        "ntile" | "first_value" | "last_value" => arg_count == 1,
        "nth_value" => arg_count == 2,
        "lag" | "lead" => (1..=3).contains(&arg_count),
        "count" => is_count_star(function) || arg_count == 1,
        "sum" | "avg" | "min" | "max" | "bool_and" | "bool_or" | "every" => arg_count == 1,
        _ => false,
    };
    if !valid_arguments {
        return Err(SqlError::InvalidSql(format!(
            "invalid argument count for {} window function",
            name.to_ascii_uppercase()
        )));
    }
    resolve_window_spec(function, select)?;
    Ok(())
}

pub(crate) fn resolve_window_spec(function: &Function, select: &Select) -> Result<WindowSpec> {
    match function
        .over
        .as_ref()
        .ok_or_else(|| SqlError::InvalidSql("window function is missing OVER".to_string()))?
    {
        WindowType::WindowSpec(spec) => {
            resolve_inline_window_spec(spec, select, &mut BTreeSet::new())
        }
        WindowType::NamedWindow(name) => {
            resolve_named_window_spec(name, select, &mut BTreeSet::new())
        }
    }
}

pub(crate) fn resolve_named_window_spec(
    name: &Ident,
    select: &Select,
    visiting: &mut BTreeSet<String>,
) -> Result<WindowSpec> {
    let key = name.value.to_ascii_lowercase();
    if !visiting.insert(key.clone()) {
        return Err(SqlError::InvalidSql(format!(
            "cyclic named window definition involving {name}"
        )));
    }
    let definition = select
        .named_window
        .iter()
        .find(|definition| definition.0.value.eq_ignore_ascii_case(&name.value))
        .ok_or_else(|| SqlError::InvalidSql(format!("window {name} does not exist")))?;
    let resolved = match &definition.1 {
        NamedWindowExpr::WindowSpec(spec) => resolve_inline_window_spec(spec, select, visiting),
        NamedWindowExpr::NamedWindow(parent) => resolve_named_window_spec(parent, select, visiting),
    };
    visiting.remove(&key);
    resolved
}

pub(crate) fn resolve_inline_window_spec(
    spec: &WindowSpec,
    select: &Select,
    visiting: &mut BTreeSet<String>,
) -> Result<WindowSpec> {
    let Some(parent) = &spec.window_name else {
        return Ok(spec.clone());
    };
    let base = resolve_named_window_spec(parent, select, visiting)?;
    merge_window_specs(base, spec)
}

pub(crate) fn merge_window_specs(mut base: WindowSpec, overlay: &WindowSpec) -> Result<WindowSpec> {
    if !base.partition_by.is_empty() && !overlay.partition_by.is_empty() {
        return Err(SqlError::InvalidSql(
            "cannot override PARTITION BY in an inherited window".to_string(),
        ));
    }
    if !base.order_by.is_empty() && !overlay.order_by.is_empty() {
        return Err(SqlError::InvalidSql(
            "cannot override ORDER BY in an inherited window".to_string(),
        ));
    }
    if base.window_frame.is_some() && overlay.window_frame.is_some() {
        return Err(SqlError::InvalidSql(
            "cannot override the frame clause of an inherited window".to_string(),
        ));
    }
    if !overlay.partition_by.is_empty() {
        base.partition_by = overlay.partition_by.clone();
    }
    if !overlay.order_by.is_empty() {
        base.order_by = overlay.order_by.clone();
    }
    if overlay.window_frame.is_some() {
        base.window_frame = overlay.window_frame.clone();
    }
    base.window_name = None;
    Ok(base)
}

pub(crate) fn order_value_ordering(
    left: &SqlValue,
    right: &SqlValue,
    options: &OrderByOptions,
) -> Ordering {
    let ascending = options.asc != Some(false);
    let nulls_first = options.nulls_first.unwrap_or(!ascending);
    let ordering = match (left, right) {
        (SqlValue::Null, SqlValue::Null) => Ordering::Equal,
        (SqlValue::Null, _) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (_, SqlValue::Null) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        _ => value_ordering(left, right).unwrap_or(Ordering::Equal),
    };
    if ascending || matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        ordering
    } else {
        ordering.reverse()
    }
}

pub(crate) fn typed_order_value_ordering(
    left: &SqlValue,
    left_key: Option<&[u8]>,
    right: &SqlValue,
    right_key: Option<&[u8]>,
    options: &OrderByOptions,
) -> Ordering {
    let ascending = options.asc != Some(false);
    let nulls_first = options.nulls_first.unwrap_or(!ascending);
    let ordering = match (left, right) {
        (SqlValue::Null, SqlValue::Null) => Ordering::Equal,
        (SqlValue::Null, _) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (_, SqlValue::Null) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (SqlValue::Json(left), SqlValue::Json(right)) => jsonb_value_ordering(left, right),
        _ => match (left_key, right_key) {
            (Some(left), Some(right)) => left.cmp(right),
            _ => value_ordering(left, right).unwrap_or(Ordering::Equal),
        },
    };
    if ascending || matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        ordering
    } else {
        ordering.reverse()
    }
}

pub(crate) fn typed_order_expr_value_ordering(
    expr: &Expr,
    left: &SqlValue,
    left_key: Option<&[u8]>,
    right: &SqlValue,
    right_key: Option<&[u8]>,
    options: &OrderByOptions,
) -> Ordering {
    typed_order_collation_value_ordering(
        explicit_expr_collation(expr).as_deref(),
        left,
        left_key,
        right,
        right_key,
        options,
    )
}

pub(crate) fn typed_order_collation_value_ordering(
    collation: Option<&str>,
    left: &SqlValue,
    left_key: Option<&[u8]>,
    right: &SqlValue,
    right_key: Option<&[u8]>,
    options: &OrderByOptions,
) -> Ordering {
    if let (Some(collation), SqlValue::String(left), SqlValue::String(right)) =
        (collation, left, right)
    {
        if let Some(mut ordering) = locale_text_ordering(&collation, left, right) {
            if options.asc == Some(false) {
                ordering = ordering.reverse();
            }
            return ordering;
        }
    }
    typed_order_value_ordering(left, left_key, right, right_key, options)
}

pub(crate) fn window_keys_are_peers(left: &[SqlValue], right: &[SqlValue]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| values_not_distinct(left, right))
}

pub(crate) fn window_ntile(position: usize, row_count: usize, buckets: usize) -> usize {
    let small_bucket_size = row_count / buckets;
    let large_bucket_count = row_count % buckets;
    let large_bucket_size = small_bucket_size + 1;
    let rows_in_large_buckets = large_bucket_count * large_bucket_size;
    if position < rows_in_large_buckets {
        position / large_bucket_size + 1
    } else if small_bucket_size == 0 {
        position + 1
    } else {
        large_bucket_count + (position - rows_in_large_buckets) / small_bucket_size + 1
    }
}

pub(crate) fn window_clamped_frame(
    start: i64,
    end: i64,
    row_count: usize,
) -> Result<Option<(usize, usize)>> {
    let start = start.clamp(0, row_count as i64);
    let end = end.clamp(-1, row_count as i64 - 1);
    Ok((start <= end).then_some((start as usize, end as usize)))
}

pub(crate) fn window_peer_start(
    partition: &[usize],
    order_keys: &[Vec<SqlValue>],
    position: usize,
) -> usize {
    let mut start = position;
    while start > 0
        && window_keys_are_peers(
            &order_keys[partition[position]],
            &order_keys[partition[start - 1]],
        )
    {
        start -= 1;
    }
    start
}

pub(crate) fn window_peer_end(
    partition: &[usize],
    order_keys: &[Vec<SqlValue>],
    position: usize,
) -> usize {
    let mut end = position;
    while end + 1 < partition.len()
        && window_keys_are_peers(
            &order_keys[partition[position]],
            &order_keys[partition[end + 1]],
        )
    {
        end += 1;
    }
    end
}

pub(crate) fn window_peer_groups(
    partition: &[usize],
    order_keys: &[Vec<SqlValue>],
) -> Vec<(usize, usize)> {
    let mut groups = Vec::new();
    let mut start = 0;
    while start < partition.len() {
        let end = window_peer_end(partition, order_keys, start);
        groups.push((start, end));
        start = end + 1;
    }
    groups
}

pub(crate) fn apply_row_limit<T>(rows: &mut Vec<T>, query: &Query) -> Result<()> {
    let Some(limit_clause) = &query.limit_clause else {
        return Ok(());
    };
    let (limit, offset) = match limit_clause {
        LimitClause::LimitOffset {
            limit: Some(limit),
            offset,
            ..
        } => (Some(limit), offset.as_ref().map(|offset| &offset.value)),
        LimitClause::OffsetCommaLimit { offset, limit } => (Some(limit), Some(offset)),
        LimitClause::LimitOffset {
            limit: None,
            offset,
            ..
        } => (None, offset.as_ref().map(|offset| &offset.value)),
    };
    let offset = offset.map(integer_expr).transpose()?.unwrap_or_default();
    if offset > 0 {
        rows.drain(0..offset.min(rows.len()));
    }
    if let Some(limit) = limit {
        if let Some(limit) = optional_count_expr(limit)? {
            rows.truncate(limit);
        }
    }
    Ok(())
}

/// How many rows an ORDER BY must actually rank before LIMIT/OFFSET
/// truncation: the constant `limit + offset` when both are known constants,
/// `None` when unbounded or dynamic. The sort may then keep only this many
/// rows (top-K selection) instead of ranking the full input.
pub(crate) fn order_by_keep_bound(query: &Query) -> Result<Option<usize>> {
    let Some(limit_clause) = &query.limit_clause else {
        return Ok(None);
    };
    let (limit, offset) = match limit_clause {
        LimitClause::LimitOffset {
            limit: Some(limit),
            offset,
            ..
        } => (limit, offset.as_ref().map(|offset| &offset.value)),
        LimitClause::OffsetCommaLimit { offset, limit } => (limit, Some(offset)),
        LimitClause::LimitOffset { limit: None, .. } => return Ok(None),
    };
    let Some(limit) = optional_count_expr(limit)? else {
        return Ok(None);
    };
    let offset = offset.map(integer_expr).transpose()?.unwrap_or_default();
    Ok(Some(limit.saturating_add(offset)))
}

pub(crate) fn query_from_body(body: SetExpr) -> Query {
    Query {
        with: None,
        body: Box::new(body),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: Vec::new(),
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: Vec::new(),
    }
}

pub(crate) fn combine_set_results(
    left: SqlResult,
    op: SetOperator,
    set_quantifier: SetQuantifier,
    right: SqlResult,
) -> Result<SqlResult> {
    if left.columns.len() != right.columns.len() {
        return Err(SqlError::InvalidSql(format!(
            "{op} query column count mismatch: left has {}, right has {}",
            left.columns.len(),
            right.columns.len()
        )));
    }
    match set_quantifier {
        SetQuantifier::None | SetQuantifier::Distinct | SetQuantifier::All => {}
        _ => {
            return Err(SqlError::Unsupported(format!(
                "{op} {set_quantifier} is not supported"
            )));
        }
    }

    let column_types = left.column_types.clone();
    let mut rows = left.rows;
    match op {
        SetOperator::Union if set_quantifier == SetQuantifier::All => {
            rows.extend(right.rows);
        }
        SetOperator::Union => {
            rows.extend(right.rows);
            rows = deduplicate_sql_rows_typed(rows, &column_types)?;
        }
        SetOperator::Intersect => {
            if set_quantifier == SetQuantifier::All {
                return Err(SqlError::Unsupported(
                    "INTERSECT ALL is not supported".to_string(),
                ));
            }
            let mut retained = Vec::new();
            for row in rows {
                let mut matched = false;
                for candidate in &right.rows {
                    if sql_rows_not_distinct_typed(&row, candidate, &column_types)? {
                        matched = true;
                        break;
                    }
                }
                if matched {
                    retained.push(row);
                }
            }
            rows = deduplicate_sql_rows_typed(retained, &column_types)?;
        }
        SetOperator::Except | SetOperator::Minus => {
            if set_quantifier == SetQuantifier::All {
                return Err(SqlError::Unsupported(format!("{op} ALL is not supported")));
            }
            let mut retained = Vec::new();
            for row in rows {
                let mut matched = false;
                for candidate in &right.rows {
                    if sql_rows_not_distinct_typed(&row, candidate, &column_types)? {
                        matched = true;
                        break;
                    }
                }
                if !matched {
                    retained.push(row);
                }
            }
            rows = deduplicate_sql_rows_typed(retained, &column_types)?;
        }
    }
    // PostgreSQL resolves a set-operation result column to the common type of the
    // branches; bicdb uses the first (left) branch's resolved type as the
    // deterministic output type (a value-independent rule). Without this, the
    // wire layer would fall back to per-value guessing and a UNION could report a
    // different OID than the same column read directly.
    Ok(SqlResult::new(left.columns, rows).with_column_types(column_types))
}

pub(crate) fn deduplicate_sql_rows_typed(
    rows: Vec<Vec<SqlValue>>,
    column_types: &[Option<String>],
) -> Result<Vec<Vec<SqlValue>>> {
    let mut deduped: Vec<Vec<SqlValue>> = Vec::new();
    for row in rows {
        let mut duplicate = false;
        for candidate in &deduped {
            if sql_rows_not_distinct_typed(candidate, &row, column_types)? {
                duplicate = true;
                break;
            }
        }
        if !duplicate {
            deduped.push(row);
        }
    }
    Ok(deduped)
}

pub(crate) fn sql_rows_not_distinct_typed(
    left: &[SqlValue],
    right: &[SqlValue],
    column_types: &[Option<String>],
) -> Result<bool> {
    if left.len() != right.len() {
        return Ok(false);
    }
    for (index, (left, right)) in left.iter().zip(right).enumerate() {
        let equal = match column_types.get(index).and_then(Option::as_deref) {
            Some(pg_type) => pg_typed_not_distinct(pg_type, left, right)?,
            None => values_not_distinct(left, right),
        };
        if !equal {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) fn result_row_map(columns: &[String], values: &[SqlValue]) -> Result<SqlRow> {
    if columns.len() != values.len() {
        return Err(SqlError::InvalidSql(format!(
            "result row has {} values for {} columns",
            values.len(),
            columns.len()
        )));
    }
    Ok(columns
        .iter()
        .cloned()
        .zip(values.iter().cloned())
        .collect())
}

pub(crate) fn select_item_needs_row_evaluator(item: &SelectItem) -> bool {
    match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            expr_needs_row_evaluator(expr)
        }
        SelectItem::Wildcard(_) => false,
        _ => true,
    }
}

pub(crate) fn select_item_is_whole_row_reference(
    item: &SelectItem,
    relation: &TableFactor,
) -> bool {
    let expr = match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
        _ => return false,
    };
    let Expr::Identifier(ident) = expr else {
        return false;
    };
    let TableFactor::Table {
        name,
        alias,
        args: None,
        ..
    } = relation
    else {
        return false;
    };
    let binding = alias
        .as_ref()
        .map(|alias| alias.name.value.as_str())
        .or_else(|| {
            name.0
                .last()
                .map(|part| part.as_ident().map(|ident| ident.value.as_str()))?
        })
        .unwrap_or_default();
    ident.value.eq_ignore_ascii_case(binding)
}

pub(crate) fn order_by_needs_row_evaluator(order_by: &OrderBy) -> bool {
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return true;
    };
    expressions
        .iter()
        .any(|order| expr_needs_row_evaluator(&order.expr))
}

pub(crate) fn expr_needs_row_evaluator(expr: &Expr) -> bool {
    match expr {
        Expr::Subquery(_) | Expr::InSubquery { .. } | Expr::Exists { .. } => true,
        // The record projection fast path has only the table schema and cannot
        // distinguish a relation alias (`p.id`) from a nested metadata path.
        // PostgreSQL resolves qualified identifiers against the FROM scope, so
        // keep them on the row evaluator where aliases are bound explicitly.
        // Treating `p.id` as metadata used to return a silent NULL.
        Expr::CompoundIdentifier(_) => true,
        Expr::UnaryOp { .. } => true,
        Expr::BinaryOp { left, right, .. } => {
            if matches!(
                expr,
                Expr::BinaryOp {
                    op: BinaryOperator::Arrow | BinaryOperator::LongArrow,
                    ..
                }
            ) {
                json_operator_path(right).is_err()
                    || expr_needs_row_evaluator(left)
                    || expr_needs_row_evaluator(right)
            } else {
                true
            }
        }
        Expr::Nested(expr)
        | Expr::Cast { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsTrue(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::IsNotFalse(expr)
        | Expr::IsUnknown(expr)
        | Expr::IsNotUnknown(expr) => expr_needs_row_evaluator(expr),
        Expr::InList { expr, list, .. } => {
            expr_needs_row_evaluator(expr) || list.iter().any(expr_needs_row_evaluator)
        }
        Expr::Tuple(_) => true,
        Expr::Like { .. }
        | Expr::ILike { .. }
        | Expr::Between { .. }
        | Expr::AnyOp { .. }
        | Expr::AllOp { .. }
        | Expr::Case { .. }
        | Expr::IsDistinctFrom(_, _)
        | Expr::IsNotDistinctFrom(_, _) => true,
        Expr::Function(function) => !is_spatial_predicate_function(function),
        _ => false,
    }
}

pub(crate) fn expr_contains_volatile_function(expr: &Expr) -> bool {
    match expr {
        Expr::Function(function) => {
            let name = object_name(&function.name)
                .map(|name| name.to_ascii_lowercase())
                .unwrap_or_default();
            matches!(
                name.as_str(),
                "random"
                    | "pg_catalog.random"
                    | "now"
                    | "pg_catalog.now"
                    | "current_timestamp"
                    | "pg_catalog.current_timestamp"
                    | "clock_timestamp"
                    | "pg_catalog.clock_timestamp"
                    | "gen_random_uuid"
                    | "pg_catalog.gen_random_uuid"
                    | "uuid_generate_v4"
                    | "public.uuid_generate_v4"
                    | "uuidv4"
                    | "pg_catalog.uuidv4"
                    | "uuidv7"
                    | "pg_catalog.uuidv7"
            ) || function_args(function)
                .iter()
                .any(expr_contains_volatile_function)
        }
        Expr::Subquery(query) => query_contains_volatile_function(query),
        Expr::BinaryOp { left, right, .. }
        | Expr::AnyOp { left, right, .. }
        | Expr::AllOp { left, right, .. } => {
            expr_contains_volatile_function(left) || expr_contains_volatile_function(right)
        }
        Expr::Nested(expr)
        | Expr::Cast { expr, .. }
        | Expr::Collate { expr, .. }
        | Expr::UnaryOp { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsTrue(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::IsNotFalse(expr)
        | Expr::IsUnknown(expr)
        | Expr::IsNotUnknown(expr) => expr_contains_volatile_function(expr),
        Expr::InList { expr, list, .. } => {
            expr_contains_volatile_function(expr)
                || list.iter().any(expr_contains_volatile_function)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_volatile_function(expr)
                || expr_contains_volatile_function(low)
                || expr_contains_volatile_function(high)
        }
        Expr::Array(array) => array.elem.iter().any(expr_contains_volatile_function),
        Expr::Position { expr, r#in } => {
            expr_contains_volatile_function(expr) || expr_contains_volatile_function(r#in)
        }
        Expr::Extract { expr, .. } => expr_contains_volatile_function(expr),
        Expr::Trim {
            expr,
            trim_what,
            trim_characters,
            ..
        } => {
            expr_contains_volatile_function(expr)
                || trim_what
                    .as_deref()
                    .is_some_and(expr_contains_volatile_function)
                || trim_characters
                    .as_ref()
                    .is_some_and(|exprs| exprs.iter().any(expr_contains_volatile_function))
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand
                .as_deref()
                .is_some_and(expr_contains_volatile_function)
                || conditions.iter().any(|condition| {
                    expr_contains_volatile_function(&condition.condition)
                        || expr_contains_volatile_function(&condition.result)
                })
                || else_result
                    .as_deref()
                    .is_some_and(expr_contains_volatile_function)
        }
        _ => false,
    }
}

pub(crate) fn query_contains_volatile_function(query: &Query) -> bool {
    match query.body.as_ref() {
        SetExpr::Select(select) => {
            select
                .projection
                .iter()
                .any(select_item_contains_volatile_function)
                || select
                    .selection
                    .as_ref()
                    .is_some_and(expr_contains_volatile_function)
        }
        SetExpr::Query(query) => query_contains_volatile_function(query),
        SetExpr::SetOperation { left, right, .. } => {
            set_expr_contains_volatile_function(left) || set_expr_contains_volatile_function(right)
        }
        _ => false,
    }
}

pub(crate) fn set_expr_contains_volatile_function(expr: &SetExpr) -> bool {
    match expr {
        SetExpr::Select(select) => {
            select
                .projection
                .iter()
                .any(select_item_contains_volatile_function)
                || select
                    .selection
                    .as_ref()
                    .is_some_and(expr_contains_volatile_function)
        }
        SetExpr::Query(query) => query_contains_volatile_function(query),
        SetExpr::SetOperation { left, right, .. } => {
            set_expr_contains_volatile_function(left) || set_expr_contains_volatile_function(right)
        }
        _ => false,
    }
}

pub(crate) fn select_item_contains_volatile_function(item: &SelectItem) -> bool {
    match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            expr_contains_volatile_function(expr)
        }
        _ => false,
    }
}

#[derive(Clone, Debug)]
pub(crate) enum RowAggregate {
    CountAll,
    CountExpr(Expr, bool),
    Sum(Expr, bool),
    Avg(Expr, bool),
    Min(Expr),
    Max(Expr),
    BoolAnd(Expr, bool),
    BoolOr(Expr, bool),
    StringAgg {
        expr: Expr,
        delimiter: Expr,
        distinct: bool,
        order_by: Vec<sqlparser::ast::OrderByExpr>,
        filter: Option<Expr>,
    },
    ArrayAgg {
        expr: Expr,
        distinct: bool,
        order_by: Vec<sqlparser::ast::OrderByExpr>,
        filter: Option<Expr>,
    },
    RangeAgg(Expr, bool, bool),
    XmlAgg {
        expr: Expr,
        order_by: Vec<sqlparser::ast::OrderByExpr>,
        filter: Option<Expr>,
    },
    JsonAgg {
        expr: Expr,
        distinct: bool,
        order_by: Vec<sqlparser::ast::OrderByExpr>,
        filter: Option<Expr>,
        binary: bool,
        strict: bool,
        standard: bool,
        returning: Option<DataType>,
    },
    JsonObjectAgg {
        key: Expr,
        value: Expr,
        distinct: bool,
        order_by: Vec<sqlparser::ast::OrderByExpr>,
        filter: Option<Expr>,
        binary: bool,
        strict: bool,
        unique: bool,
        standard: bool,
        returning: Option<DataType>,
    },
}

impl RowAggregate {
    pub(crate) fn from_function(function: &Function) -> Result<Self> {
        let name = object_name(&function.name)?.to_ascii_lowercase();
        if name.ends_with("_encoding_error") {
            return Err(crate::jsonb::json_output_encoding_error());
        }
        let distinct = function_args_are_distinct(function);
        match name.as_str() {
            "count" => {
                if is_count_star(function) {
                    Ok(Self::CountAll)
                } else {
                    Ok(Self::CountExpr(single_function_expr(function)?, distinct))
                }
            }
            name if json_array_aggregate_flags(name).is_some() => {
                let (binary, default_strict) = json_array_aggregate_flags(name).unwrap();
                let standard = name.strip_prefix("pg_catalog.").unwrap_or(name) == "json_arrayagg";
                let (strict, returning) =
                    json_standard_aggregate_options(function, default_strict, standard)?;
                let (expr, distinct, order_by) = json_agg_function_parts(function)?;
                Ok(Self::JsonAgg {
                    expr,
                    distinct,
                    order_by,
                    filter: function.filter.as_deref().cloned(),
                    binary,
                    strict,
                    standard,
                    returning,
                })
            }
            name if json_object_aggregate_flags(name).is_some() => {
                let (binary, default_strict, unique) = json_object_aggregate_flags(name).unwrap();
                let standard = matches!(
                    name.strip_prefix("pg_catalog.").unwrap_or(name),
                    "json_objectagg" | "json_objectagg_unique"
                );
                let (strict, returning) =
                    json_standard_aggregate_options(function, default_strict, standard)?;
                let (key, value, distinct, order_by) = json_object_agg_function_parts(function)?;
                Ok(Self::JsonObjectAgg {
                    key,
                    value,
                    distinct,
                    order_by,
                    filter: function.filter.as_deref().cloned(),
                    binary,
                    strict,
                    unique,
                    standard,
                    returning,
                })
            }
            "xmlagg" | "pg_catalog.xmlagg" => {
                let (expr, distinct, order_by) = json_agg_function_parts(function)?;
                if distinct {
                    return Err(SqlError::undefined_function(
                        "could not identify an equality operator for type xml",
                    ));
                }
                Ok(Self::XmlAgg {
                    expr,
                    order_by,
                    filter: function.filter.as_deref().cloned(),
                })
            }
            "bool_and"
            | "pg_catalog.bool_and"
            | "every"
            | "pg_catalog.every"
            | "bool_or"
            | "pg_catalog.bool_or" => {
                let expr = single_function_expr(function)?;
                if name.ends_with("bool_or") {
                    Ok(Self::BoolOr(expr, distinct))
                } else {
                    Ok(Self::BoolAnd(expr, distinct))
                }
            }
            "string_agg" | "pg_catalog.string_agg" => {
                let (expr, delimiter, distinct, order_by) = string_agg_function_parts(function)?;
                Ok(Self::StringAgg {
                    expr,
                    delimiter,
                    distinct,
                    order_by,
                    filter: function.filter.as_deref().cloned(),
                })
            }
            "range_agg"
            | "pg_catalog.range_agg"
            | "range_intersect_agg"
            | "pg_catalog.range_intersect_agg" => Ok(Self::RangeAgg(
                single_function_expr(function)?,
                distinct,
                name.ends_with("range_intersect_agg"),
            )),
            "array_agg" => {
                let (expr, distinct, order_by) = json_agg_function_parts(function)?;
                Ok(Self::ArrayAgg {
                    expr,
                    distinct,
                    order_by,
                    filter: function.filter.as_deref().cloned(),
                })
            }
            "sum" | "avg" | "min" | "max" => {
                let expr = single_function_expr(function)?;
                match name.as_str() {
                    "sum" => Ok(Self::Sum(expr, distinct)),
                    "avg" => Ok(Self::Avg(expr, distinct)),
                    "min" => Ok(Self::Min(expr)),
                    "max" => Ok(Self::Max(expr)),
                    _ => unreachable!(),
                }
            }
            other => Err(SqlError::Unsupported(format!(
                "aggregate {other} is not supported"
            ))),
        }
    }

    pub(crate) fn column_name(&self) -> String {
        match self {
            Self::CountAll => "COUNT(*)".to_string(),
            Self::CountExpr(expr, distinct) => {
                if *distinct {
                    format!("COUNT(DISTINCT {})", row_expr_column_name(expr))
                } else {
                    format!("COUNT({})", row_expr_column_name(expr))
                }
            }
            Self::Sum(_, _) => "sum".to_string(),
            Self::Avg(_, _) => "avg".to_string(),
            Self::Min(_) => "min".to_string(),
            Self::Max(_) => "max".to_string(),
            Self::BoolAnd(_, _) => "bool_and".to_string(),
            Self::BoolOr(_, _) => "bool_or".to_string(),
            Self::StringAgg { .. } => "string_agg".to_string(),
            Self::ArrayAgg { .. } => "array_agg".to_string(),
            Self::RangeAgg(_, _, intersect) => {
                if *intersect {
                    "range_intersect_agg".to_string()
                } else {
                    "range_agg".to_string()
                }
            }
            Self::XmlAgg { .. } => "xmlagg".to_string(),
            Self::JsonAgg {
                binary,
                strict,
                standard,
                ..
            } => {
                if *standard {
                    return "json_arrayagg".to_string();
                }
                format!(
                    "{}{}",
                    if *binary { "jsonb_agg" } else { "json_agg" },
                    if *strict { "_strict" } else { "" }
                )
            }
            Self::JsonObjectAgg {
                binary,
                strict,
                unique,
                standard,
                ..
            } => {
                if *standard {
                    return "json_objectagg".to_string();
                }
                format!(
                    "{}{}",
                    if *binary {
                        "jsonb_object_agg"
                    } else {
                        "json_object_agg"
                    },
                    match (*unique, *strict) {
                        (true, true) => "_unique_strict",
                        (true, false) => "_unique",
                        (false, true) => "_strict",
                        (false, false) => "",
                    }
                )
            }
        }
    }

    pub(crate) fn evaluate(
        &self,
        engine: &SqlEngine<'_>,
        rows: &[SlotRow],
        columns: &[String],
    ) -> Result<SqlValue> {
        match self {
            Self::CountAll => Ok(SqlValue::Int(rows.len() as i64)),
            Self::CountExpr(expr, distinct) => {
                row_aggregate_count(engine, rows, columns, expr, *distinct).map(SqlValue::Int)
            }
            Self::Sum(expr, distinct) => {
                reject_oid_numeric_aggregate(engine, expr, columns, "sum")?;
                row_sum_value(engine, rows, columns, expr, *distinct)
            }
            Self::Avg(expr, distinct) => {
                reject_oid_numeric_aggregate(engine, expr, columns, "avg")?;
                average_aggregate_values(row_aggregate_values(
                    engine, rows, columns, expr, *distinct, false,
                )?)
            }
            Self::Min(expr) => row_extreme_value(engine, rows, columns, expr, false),
            Self::Max(expr) => row_extreme_value(engine, rows, columns, expr, true),
            Self::BoolAnd(expr, distinct) => bool_aggregate_values(
                row_aggregate_values(engine, rows, columns, expr, *distinct, false)?,
                true,
            ),
            Self::BoolOr(expr, distinct) => bool_aggregate_values(
                row_aggregate_values(engine, rows, columns, expr, *distinct, false)?,
                false,
            ),
            Self::StringAgg {
                expr,
                delimiter,
                distinct,
                order_by,
                filter,
            } => row_string_agg_value(
                engine,
                rows,
                columns,
                expr,
                delimiter,
                *distinct,
                order_by,
                filter.as_ref(),
            ),
            Self::ArrayAgg {
                expr,
                distinct,
                order_by,
                filter,
            } => row_array_agg_value(
                engine,
                rows,
                columns,
                expr,
                *distinct,
                order_by,
                filter.as_ref(),
            ),
            Self::RangeAgg(expr, distinct, intersect) => {
                let input_type = row_columns_expr_pg_type(
                    engine.db_ref(),
                    expr,
                    columns.iter().map(String::as_str),
                )
                .ok_or_else(|| {
                    SqlError::undefined_function(
                        "range aggregate requires a range or multirange argument",
                    )
                })?;
                range_aggregate_values(
                    row_aggregate_values(engine, rows, columns, expr, *distinct, false)?,
                    &input_type,
                    *intersect,
                )
            }
            Self::XmlAgg {
                expr,
                order_by,
                filter,
            } => row_xml_agg_value(engine, rows, columns, expr, order_by, filter.as_ref()),
            Self::JsonAgg {
                expr,
                distinct,
                order_by,
                filter,
                binary,
                strict,
                returning,
                ..
            } => crate::jsonb::apply_json_returning(
                row_json_agg_value(
                    engine,
                    rows,
                    columns,
                    expr,
                    *distinct,
                    order_by,
                    filter.as_ref(),
                    *binary,
                    *strict,
                )?,
                returning.as_ref(),
            ),
            Self::JsonObjectAgg {
                key,
                value,
                distinct,
                order_by,
                filter,
                binary,
                strict,
                unique,
                returning,
                ..
            } => crate::jsonb::apply_json_returning(
                row_json_object_agg_value(
                    engine,
                    rows,
                    columns,
                    key,
                    value,
                    *distinct,
                    order_by,
                    filter.as_ref(),
                    *binary,
                    *strict,
                    *unique,
                )?,
                returning.as_ref(),
            ),
        }
    }

    pub(crate) fn evaluate_bound(
        &self,
        engine: &SqlEngine<'_>,
        rows: &[SlotRow],
        scope: &BoundExprScope,
        context: &BoundRowContext,
    ) -> Result<SqlValue> {
        match self {
            Self::CountAll => Ok(SqlValue::Int(rows.len() as i64)),
            Self::CountExpr(expr, distinct) => {
                let bound = scope.bind(expr);
                row_bound_aggregate_count(engine, rows, expr, bound.as_ref(), context, *distinct)
                    .map(SqlValue::Int)
            }
            Self::Sum(expr, distinct) => {
                reject_oid_numeric_aggregate(engine, expr, context.column_keys(), "sum")?;
                let bound = scope.bind(expr);
                row_sum_bound_value(engine, rows, expr, bound.as_ref(), context, *distinct)
            }
            Self::Avg(expr, distinct) => {
                reject_oid_numeric_aggregate(engine, expr, context.column_keys(), "avg")?;
                let bound = scope.bind(expr);
                average_aggregate_values(row_bound_aggregate_values(
                    engine,
                    rows,
                    expr,
                    bound.as_ref(),
                    context,
                    *distinct,
                    false,
                )?)
            }
            Self::Min(expr) => {
                let bound = scope.bind(expr);
                row_bound_extreme_value(engine, rows, expr, bound.as_ref(), context, false)
            }
            Self::Max(expr) => {
                let bound = scope.bind(expr);
                row_bound_extreme_value(engine, rows, expr, bound.as_ref(), context, true)
            }
            Self::BoolAnd(expr, distinct) => {
                let bound = scope.bind(expr);
                bool_aggregate_values(
                    row_bound_aggregate_values(
                        engine,
                        rows,
                        expr,
                        bound.as_ref(),
                        context,
                        *distinct,
                        false,
                    )?,
                    true,
                )
            }
            Self::BoolOr(expr, distinct) => {
                let bound = scope.bind(expr);
                bool_aggregate_values(
                    row_bound_aggregate_values(
                        engine,
                        rows,
                        expr,
                        bound.as_ref(),
                        context,
                        *distinct,
                        false,
                    )?,
                    false,
                )
            }
            Self::StringAgg {
                expr,
                delimiter,
                distinct,
                order_by,
                filter,
            } => row_bound_string_agg_value(
                engine,
                rows,
                expr,
                scope.bind(expr).as_ref(),
                delimiter,
                scope.bind(delimiter).as_ref(),
                scope,
                context,
                *distinct,
                order_by,
                filter.as_ref(),
                filter
                    .as_ref()
                    .and_then(|filter| scope.bind(filter))
                    .as_ref(),
            ),
            Self::ArrayAgg {
                expr,
                distinct,
                order_by,
                filter,
            } => {
                let bound = scope.bind(expr);
                let filter_bound = filter.as_ref().and_then(|filter| scope.bind(filter));
                row_bound_array_agg_value(
                    engine,
                    rows,
                    expr,
                    bound.as_ref(),
                    scope,
                    context,
                    *distinct,
                    order_by,
                    filter.as_ref(),
                    filter_bound.as_ref(),
                )
            }
            Self::RangeAgg(expr, distinct, intersect) => {
                let input_type = row_columns_expr_pg_type(
                    engine.db_ref(),
                    expr,
                    context.column_keys().iter().map(String::as_str),
                )
                .ok_or_else(|| {
                    SqlError::undefined_function(
                        "range aggregate requires a range or multirange argument",
                    )
                })?;
                let bound = scope.bind(expr);
                range_aggregate_values(
                    row_bound_aggregate_values(
                        engine,
                        rows,
                        expr,
                        bound.as_ref(),
                        context,
                        *distinct,
                        false,
                    )?,
                    &input_type,
                    *intersect,
                )
            }
            Self::XmlAgg {
                expr,
                order_by,
                filter,
            } => row_bound_xml_agg_value(
                engine,
                rows,
                expr,
                scope.bind(expr).as_ref(),
                scope,
                context,
                order_by,
                filter.as_ref(),
                filter
                    .as_ref()
                    .and_then(|filter| scope.bind(filter))
                    .as_ref(),
            ),
            Self::JsonAgg {
                expr,
                distinct,
                order_by,
                filter,
                binary,
                strict,
                returning,
                ..
            } => {
                let bound = scope.bind(expr);
                let filter_bound = filter.as_ref().and_then(|filter| scope.bind(filter));
                crate::jsonb::apply_json_returning(
                    row_bound_json_agg_value(
                        engine,
                        rows,
                        expr,
                        bound.as_ref(),
                        scope,
                        context,
                        *distinct,
                        order_by,
                        filter.as_ref(),
                        filter_bound.as_ref(),
                        *binary,
                        *strict,
                    )?,
                    returning.as_ref(),
                )
            }
            Self::JsonObjectAgg {
                key,
                value,
                distinct,
                order_by,
                filter,
                binary,
                strict,
                unique,
                returning,
                ..
            } => crate::jsonb::apply_json_returning(
                row_bound_json_object_agg_value(
                    engine,
                    rows,
                    key,
                    scope.bind(key).as_ref(),
                    value,
                    scope.bind(value).as_ref(),
                    scope,
                    context,
                    *distinct,
                    order_by,
                    filter.as_ref(),
                    filter
                        .as_ref()
                        .and_then(|filter| scope.bind(filter))
                        .as_ref(),
                    *binary,
                    *strict,
                    *unique,
                )?,
                returning.as_ref(),
            ),
        }
    }
}

fn reject_oid_numeric_aggregate(
    engine: &SqlEngine<'_>,
    expr: &Expr,
    columns: &[String],
    name: &str,
) -> Result<()> {
    if row_columns_expr_pg_type(engine.db_ref(), expr, columns.iter().map(String::as_str))
        .as_deref()
        == Some("oid")
    {
        return Err(SqlError::undefined_function(format!(
            "function {name}(oid) does not exist"
        )));
    }
    Ok(())
}

pub(crate) fn eval_row_or_bound_value(
    engine: &SqlEngine<'_>,
    row: &SlotRow,
    expr: &Expr,
    bound: Option<&BoundExpr>,
    context: &BoundRowContext,
) -> Result<SqlValue> {
    match bound {
        Some(bound) => bound.eval(&BoundExprFrame {
            user_calls: &[],
            db: engine.db,
            columns: BoundExprColumns::Values(row),
            vars: &context.var_values,
        }),
        None => engine.eval_slot_row_value(row, context, expr),
    }
}

pub(crate) fn row_bound_array_agg_value(
    engine: &SqlEngine<'_>,
    rows: &[SlotRow],
    expr: &Expr,
    bound: Option<&BoundExpr>,
    scope: &BoundExprScope,
    context: &BoundRowContext,
    distinct: bool,
    order_by: &[sqlparser::ast::OrderByExpr],
    filter: Option<&Expr>,
    filter_bound: Option<&BoundExpr>,
) -> Result<SqlValue> {
    let order_bounds = order_by
        .iter()
        .map(|order| scope.bind(&order.expr))
        .collect::<Vec<_>>();
    let mut entries = Vec::with_capacity(rows.len());
    for (idx, row) in rows.iter().enumerate() {
        if idx % 1024 == 0 {
            engine.check_cancellation()?;
        }
        if let Some(filter) = filter {
            let accepted = eval_row_or_bound_value(engine, row, filter, filter_bound, context)?;
            if !matches!(sql_value_truth(accepted)?, Some(true)) {
                continue;
            }
        }
        let value = eval_row_or_bound_value(engine, row, expr, bound, context)?;
        let keys = order_by
            .iter()
            .zip(&order_bounds)
            .map(|(order, bound)| {
                eval_row_or_bound_value(engine, row, &order.expr, bound.as_ref(), context)
            })
            .collect::<Result<Vec<_>>>()?;
        entries.push((value, keys));
    }
    finish_array_agg(entries, distinct, order_by)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn row_bound_string_agg_value(
    engine: &SqlEngine<'_>,
    rows: &[SlotRow],
    expr: &Expr,
    bound: Option<&BoundExpr>,
    delimiter: &Expr,
    delimiter_bound: Option<&BoundExpr>,
    scope: &BoundExprScope,
    context: &BoundRowContext,
    distinct: bool,
    order_by: &[sqlparser::ast::OrderByExpr],
    filter: Option<&Expr>,
    filter_bound: Option<&BoundExpr>,
) -> Result<SqlValue> {
    let order_bounds = order_by
        .iter()
        .map(|order| scope.bind(&order.expr))
        .collect::<Vec<_>>();
    let mut entries = Vec::with_capacity(rows.len());
    for (idx, row) in rows.iter().enumerate() {
        if idx % 1024 == 0 {
            engine.check_cancellation()?;
        }
        if let Some(filter) = filter {
            let accepted = eval_row_or_bound_value(engine, row, filter, filter_bound, context)?;
            if !matches!(sql_value_truth(accepted)?, Some(true)) {
                continue;
            }
        }
        let value = eval_row_or_bound_value(engine, row, expr, bound, context)?;
        let delimiter = eval_row_or_bound_value(engine, row, delimiter, delimiter_bound, context)?;
        let keys = order_by
            .iter()
            .zip(&order_bounds)
            .map(|(order, bound)| {
                eval_row_or_bound_value(engine, row, &order.expr, bound.as_ref(), context)
            })
            .collect::<Result<Vec<_>>>()?;
        entries.push((value, delimiter, keys));
    }
    finish_string_agg(entries, distinct, order_by)
}

pub(crate) fn row_bound_json_agg_value(
    engine: &SqlEngine<'_>,
    rows: &[SlotRow],
    expr: &Expr,
    bound: Option<&BoundExpr>,
    scope: &BoundExprScope,
    context: &BoundRowContext,
    distinct: bool,
    order_by: &[sqlparser::ast::OrderByExpr],
    filter: Option<&Expr>,
    filter_bound: Option<&BoundExpr>,
    binary: bool,
    strict: bool,
) -> Result<SqlValue> {
    let order_bounds = order_by
        .iter()
        .map(|order| scope.bind(&order.expr))
        .collect::<Vec<_>>();
    let mut entries = Vec::with_capacity(rows.len());
    for (idx, row) in rows.iter().enumerate() {
        if idx % 1024 == 0 {
            engine.check_cancellation()?;
        }
        if let Some(filter) = filter {
            let accepted = eval_row_or_bound_value(engine, row, filter, filter_bound, context)?;
            if !matches!(sql_value_truth(accepted)?, Some(true)) {
                continue;
            }
        }
        let value = eval_row_or_bound_value(engine, row, expr, bound, context)?;
        let keys = order_by
            .iter()
            .zip(&order_bounds)
            .map(|(order, bound)| {
                eval_row_or_bound_value(engine, row, &order.expr, bound.as_ref(), context)
            })
            .collect::<Result<Vec<_>>>()?;
        entries.push((value, keys));
    }
    finish_json_agg(entries, distinct, order_by, binary, strict)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn row_bound_xml_agg_value(
    engine: &SqlEngine<'_>,
    rows: &[SlotRow],
    expr: &Expr,
    bound: Option<&BoundExpr>,
    scope: &BoundExprScope,
    context: &BoundRowContext,
    order_by: &[sqlparser::ast::OrderByExpr],
    filter: Option<&Expr>,
    filter_bound: Option<&BoundExpr>,
) -> Result<SqlValue> {
    let order_bounds = order_by
        .iter()
        .map(|order| scope.bind(&order.expr))
        .collect::<Vec<_>>();
    let mut entries = Vec::with_capacity(rows.len());
    for (idx, row) in rows.iter().enumerate() {
        if idx % 1024 == 0 {
            engine.check_cancellation()?;
        }
        if let Some(filter) = filter {
            let accepted = eval_row_or_bound_value(engine, row, filter, filter_bound, context)?;
            if !matches!(sql_value_truth(accepted)?, Some(true)) {
                continue;
            }
        }
        let value = eval_row_or_bound_value(engine, row, expr, bound, context)?;
        let keys = order_by
            .iter()
            .zip(&order_bounds)
            .map(|(order, bound)| {
                eval_row_or_bound_value(engine, row, &order.expr, bound.as_ref(), context)
            })
            .collect::<Result<Vec<_>>>()?;
        entries.push((value, keys));
    }
    finish_xml_agg(entries, order_by)
}

pub(crate) fn row_sum_bound_value(
    engine: &SqlEngine<'_>,
    rows: &[SlotRow],
    expr: &Expr,
    bound: Option<&BoundExpr>,
    context: &BoundRowContext,
    distinct: bool,
) -> Result<SqlValue> {
    sum_aggregate_values(row_bound_aggregate_values(
        engine, rows, expr, bound, context, distinct, false,
    )?)
}

pub(crate) fn row_bound_aggregate_values(
    engine: &SqlEngine<'_>,
    rows: &[SlotRow],
    expr: &Expr,
    bound: Option<&BoundExpr>,
    context: &BoundRowContext,
    distinct: bool,
    include_null: bool,
) -> Result<Vec<SqlValue>> {
    let mut values = Vec::new();
    let mut seen = BTreeSet::new();
    for (idx, row) in rows.iter().enumerate() {
        if idx % 1024 == 0 {
            engine.check_cancellation()?;
        }
        let value = eval_row_or_bound_value(engine, row, expr, bound, context)?;
        push_aggregate_value(&mut values, &mut seen, value, distinct, include_null);
    }
    Ok(values)
}

pub(crate) fn row_bound_aggregate_count(
    engine: &SqlEngine<'_>,
    rows: &[SlotRow],
    expr: &Expr,
    bound: Option<&BoundExpr>,
    context: &BoundRowContext,
    distinct: bool,
) -> Result<i64> {
    let mut count = 0_i64;
    let mut seen = BTreeSet::new();
    for (idx, row) in rows.iter().enumerate() {
        if idx % 1024 == 0 {
            engine.check_cancellation()?;
        }
        let value = eval_row_or_bound_value(engine, row, expr, bound, context)?;
        if matches!(value, SqlValue::Null) {
            continue;
        }
        if distinct && !seen.insert(sql_value_distinct_key(&value)) {
            continue;
        }
        count += 1;
    }
    Ok(count)
}

pub(crate) fn row_bound_extreme_value(
    engine: &SqlEngine<'_>,
    rows: &[SlotRow],
    expr: &Expr,
    bound: Option<&BoundExpr>,
    context: &BoundRowContext,
    greatest: bool,
) -> Result<SqlValue> {
    let mut best = SqlValue::Null;
    let pg_type = row_columns_expr_pg_type(
        engine.db_ref(),
        expr,
        context.column_keys().iter().map(String::as_str),
    );
    for (idx, row) in rows.iter().enumerate() {
        if idx % 1024 == 0 {
            engine.check_cancellation()?;
        }
        let value = eval_row_or_bound_value(engine, row, expr, bound, context)?;
        if matches!(value, SqlValue::Null) {
            continue;
        }
        if matches!(best, SqlValue::Null) {
            best = value;
            continue;
        }
        let ordering = if matches!(pg_type.as_deref(), Some("inet" | "cidr")) {
            Some(pg_typed_compare_for_db(
                engine.db_ref(),
                "inet",
                &value,
                &best,
            )?)
        } else if matches!(pg_type.as_deref(), Some("macaddr" | "macaddr8")) {
            Some(pg_typed_compare_for_db(
                engine.db_ref(),
                pg_type.as_deref().unwrap(),
                &value,
                &best,
            )?)
        } else {
            value_ordering(&value, &best)
        };
        if ordering.is_some_and(|ordering| {
            if greatest {
                ordering == Ordering::Greater
            } else {
                ordering == Ordering::Less
            }
        }) {
            best = value;
        }
    }
    Ok(best)
}

pub(crate) fn row_array_agg_value(
    engine: &SqlEngine<'_>,
    rows: &[SlotRow],
    columns: &[String],
    expr: &Expr,
    distinct: bool,
    order_by: &[sqlparser::ast::OrderByExpr],
    filter: Option<&Expr>,
) -> Result<SqlValue> {
    let (_, context) = engine.bound_row_context(columns);
    let mut entries = Vec::with_capacity(rows.len());
    for (idx, row) in rows.iter().enumerate() {
        if idx % 1024 == 0 {
            engine.check_cancellation()?;
        }
        if let Some(filter) = filter {
            if !matches!(
                engine.eval_slot_row_truth(row, &context, filter)?,
                Some(true)
            ) {
                continue;
            }
        }
        let value = engine.eval_slot_row_value(row, &context, expr)?;
        let keys = order_by
            .iter()
            .map(|order| engine.eval_slot_row_value(row, &context, &order.expr))
            .collect::<Result<Vec<_>>>()?;
        entries.push((value, keys));
    }
    finish_array_agg(entries, distinct, order_by)
}

pub(crate) fn row_string_agg_value(
    engine: &SqlEngine<'_>,
    rows: &[SlotRow],
    columns: &[String],
    expr: &Expr,
    delimiter: &Expr,
    distinct: bool,
    order_by: &[sqlparser::ast::OrderByExpr],
    filter: Option<&Expr>,
) -> Result<SqlValue> {
    let (_, context) = engine.bound_row_context(columns);
    let mut entries = Vec::with_capacity(rows.len());
    for (idx, row) in rows.iter().enumerate() {
        if idx % 1024 == 0 {
            engine.check_cancellation()?;
        }
        if let Some(filter) = filter {
            if !matches!(
                engine.eval_slot_row_truth(row, &context, filter)?,
                Some(true)
            ) {
                continue;
            }
        }
        let value = engine.eval_slot_row_value(row, &context, expr)?;
        let delimiter = engine.eval_slot_row_value(row, &context, delimiter)?;
        let keys = order_by
            .iter()
            .map(|order| engine.eval_slot_row_value(row, &context, &order.expr))
            .collect::<Result<Vec<_>>>()?;
        entries.push((value, delimiter, keys));
    }
    finish_string_agg(entries, distinct, order_by)
}

pub(crate) fn row_json_agg_value(
    engine: &SqlEngine<'_>,
    rows: &[SlotRow],
    columns: &[String],
    expr: &Expr,
    distinct: bool,
    order_by: &[sqlparser::ast::OrderByExpr],
    filter: Option<&Expr>,
    binary: bool,
    strict: bool,
) -> Result<SqlValue> {
    let (_, context) = engine.bound_row_context(columns);
    let mut entries = Vec::with_capacity(rows.len());
    for (idx, row) in rows.iter().enumerate() {
        if idx % 1024 == 0 {
            engine.check_cancellation()?;
        }
        if let Some(filter) = filter {
            if !matches!(
                engine.eval_slot_row_truth(row, &context, filter)?,
                Some(true)
            ) {
                continue;
            }
        }
        let value = engine.eval_slot_row_value(row, &context, expr)?;
        let keys = order_by
            .iter()
            .map(|order| engine.eval_slot_row_value(row, &context, &order.expr))
            .collect::<Result<Vec<_>>>()?;
        entries.push((value, keys));
    }
    finish_json_agg(entries, distinct, order_by, binary, strict)
}

pub(crate) fn row_xml_agg_value(
    engine: &SqlEngine<'_>,
    rows: &[SlotRow],
    columns: &[String],
    expr: &Expr,
    order_by: &[sqlparser::ast::OrderByExpr],
    filter: Option<&Expr>,
) -> Result<SqlValue> {
    let (_, context) = engine.bound_row_context(columns);
    let mut entries = Vec::with_capacity(rows.len());
    for (idx, row) in rows.iter().enumerate() {
        if idx % 1024 == 0 {
            engine.check_cancellation()?;
        }
        if let Some(filter) = filter {
            if !matches!(
                engine.eval_slot_row_truth(row, &context, filter)?,
                Some(true)
            ) {
                continue;
            }
        }
        let value = engine.eval_slot_row_value(row, &context, expr)?;
        let keys = order_by
            .iter()
            .map(|order| engine.eval_slot_row_value(row, &context, &order.expr))
            .collect::<Result<Vec<_>>>()?;
        entries.push((value, keys));
    }
    finish_xml_agg(entries, order_by)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn row_bound_json_object_agg_value(
    engine: &SqlEngine<'_>,
    rows: &[SlotRow],
    key_expr: &Expr,
    key_bound: Option<&BoundExpr>,
    value_expr: &Expr,
    value_bound: Option<&BoundExpr>,
    scope: &BoundExprScope,
    context: &BoundRowContext,
    distinct: bool,
    order_by: &[sqlparser::ast::OrderByExpr],
    filter: Option<&Expr>,
    filter_bound: Option<&BoundExpr>,
    binary: bool,
    strict: bool,
    unique: bool,
) -> Result<SqlValue> {
    let order_bounds = order_by
        .iter()
        .map(|order| scope.bind(&order.expr))
        .collect::<Vec<_>>();
    let mut entries = Vec::with_capacity(rows.len());
    for (idx, row) in rows.iter().enumerate() {
        if idx % 1024 == 0 {
            engine.check_cancellation()?;
        }
        if let Some(filter) = filter {
            let accepted = eval_row_or_bound_value(engine, row, filter, filter_bound, context)?;
            if !matches!(sql_value_truth(accepted)?, Some(true)) {
                continue;
            }
        }
        let key = eval_row_or_bound_value(engine, row, key_expr, key_bound, context)?;
        let value = eval_row_or_bound_value(engine, row, value_expr, value_bound, context)?;
        let order_keys = order_by
            .iter()
            .zip(&order_bounds)
            .map(|(order, bound)| {
                eval_row_or_bound_value(engine, row, &order.expr, bound.as_ref(), context)
            })
            .collect::<Result<Vec<_>>>()?;
        entries.push((key, value, order_keys));
    }
    finish_json_object_agg(entries, distinct, order_by, binary, strict, unique)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn row_json_object_agg_value(
    engine: &SqlEngine<'_>,
    rows: &[SlotRow],
    columns: &[String],
    key_expr: &Expr,
    value_expr: &Expr,
    distinct: bool,
    order_by: &[sqlparser::ast::OrderByExpr],
    filter: Option<&Expr>,
    binary: bool,
    strict: bool,
    unique: bool,
) -> Result<SqlValue> {
    let (_, context) = engine.bound_row_context(columns);
    let mut entries = Vec::with_capacity(rows.len());
    for (idx, row) in rows.iter().enumerate() {
        if idx % 1024 == 0 {
            engine.check_cancellation()?;
        }
        if let Some(filter) = filter {
            if !matches!(
                engine.eval_slot_row_truth(row, &context, filter)?,
                Some(true)
            ) {
                continue;
            }
        }
        let key = engine.eval_slot_row_value(row, &context, key_expr)?;
        let value = engine.eval_slot_row_value(row, &context, value_expr)?;
        let order_keys = order_by
            .iter()
            .map(|order| engine.eval_slot_row_value(row, &context, &order.expr))
            .collect::<Result<Vec<_>>>()?;
        entries.push((key, value, order_keys));
    }
    finish_json_object_agg(entries, distinct, order_by, binary, strict, unique)
}

pub(crate) fn finish_json_object_agg(
    mut entries: Vec<(SqlValue, SqlValue, Vec<SqlValue>)>,
    distinct: bool,
    order_by: &[sqlparser::ast::OrderByExpr],
    binary: bool,
    strict: bool,
    unique: bool,
) -> Result<SqlValue> {
    if entries.is_empty() {
        return Ok(SqlValue::Null);
    }
    if !order_by.is_empty() {
        entries.sort_by(|left, right| {
            json_agg_entry_ordering(
                &(SqlValue::Null, left.2.clone()),
                &(SqlValue::Null, right.2.clone()),
                order_by,
            )
        });
    }

    let mut seen_pairs = BTreeSet::new();
    let mut seen_keys = BTreeSet::new();
    let mut binary_object = JsonMap::new();
    let mut text_entries = Vec::with_capacity(entries.len());
    for (key, value, _) in entries {
        if distinct
            && !seen_pairs.insert((sql_value_distinct_key(&key), sql_value_distinct_key(&value)))
        {
            continue;
        }
        let key = json_object_aggregate_key(&key)?;
        if unique && !seen_keys.insert(key.clone()) {
            return Err(SqlError::ConstraintViolation {
                sqlstate: "22030",
                message: format!("duplicate JSON object key value: \"{key}\""),
                table: None,
                column: None,
                constraint: None,
            });
        }
        if strict && matches!(value, SqlValue::Null) {
            continue;
        }
        if binary {
            binary_object.insert(key, sql_value_to_json(value));
        } else {
            text_entries.push(format!(
                "{} : {}",
                serde_json::to_string(&key).expect("JSON object key serialization"),
                json_aggregate_input_text(&value)
            ));
        }
    }
    if binary {
        Ok(SqlValue::Json(JsonValue::Object(binary_object)))
    } else {
        Ok(SqlValue::JsonText(
            PgJsonText::parse(format!("{{ {} }}", text_entries.join(", ")))
                .expect("serialized JSON object aggregate is valid"),
        ))
    }
}

pub(crate) fn finish_array_agg(
    mut entries: Vec<(SqlValue, Vec<SqlValue>)>,
    distinct: bool,
    order_by: &[sqlparser::ast::OrderByExpr],
) -> Result<SqlValue> {
    if entries.is_empty() {
        return Ok(SqlValue::Null);
    }
    if !order_by.is_empty() {
        entries.sort_by(|left, right| json_agg_entry_ordering(left, right, order_by));
    }

    let mut seen = BTreeSet::new();
    let values = entries
        .into_iter()
        .filter_map(|(value, _)| {
            (!distinct || seen.insert(sql_value_distinct_key(&value))).then_some(value)
        })
        .collect::<Vec<_>>();
    let array_input = values
        .iter()
        .find(|value| !matches!(value, SqlValue::Null))
        .is_some_and(|value| {
            array_json_parts(value, "array_agg").is_ok_and(|value| value.is_some())
        });
    if !array_input {
        return sql_array_value(values);
    }

    let mut expected_dimensions = None;
    let mut expected_bounds = None;
    let mut arrays = Vec::with_capacity(values.len());
    for value in values {
        if matches!(value, SqlValue::Null) {
            return Err(SqlError::data_exception(
                "22004",
                "cannot accumulate null arrays",
                None,
            ));
        }
        let (array, bounds) = array_json_parts(&value, "array_agg")?.ok_or_else(|| {
            SqlError::data_exception("2202E", "cannot accumulate non-array values", None)
        })?;
        let dimensions = array_dimensions(array);
        if dimensions.is_empty() {
            return Err(SqlError::data_exception(
                "2202E",
                "cannot accumulate empty arrays",
                None,
            ));
        }
        if expected_dimensions
            .as_ref()
            .is_some_and(|expected| expected != &dimensions)
            || expected_bounds
                .as_ref()
                .is_some_and(|expected| expected != &bounds)
        {
            return Err(SqlError::data_exception(
                "2202E",
                "cannot accumulate arrays of different dimensionality",
                None,
            ));
        }
        expected_dimensions.get_or_insert(dimensions);
        expected_bounds.get_or_insert_with(|| bounds.clone());
        arrays.push(array.clone());
    }
    let mut bounds = vec![1];
    bounds.extend(expected_bounds.unwrap_or_default());
    Ok(array_json_value_with_lower_bounds(
        JsonValue::Array(arrays),
        bounds,
    ))
}

pub(crate) fn finish_string_agg(
    mut entries: Vec<(SqlValue, SqlValue, Vec<SqlValue>)>,
    distinct: bool,
    order_by: &[sqlparser::ast::OrderByExpr],
) -> Result<SqlValue> {
    if !order_by.is_empty() {
        entries.sort_by(|left, right| aggregate_key_ordering(&left.2, &right.2, order_by));
    }

    let mut seen = BTreeSet::new();
    let mut output = String::new();
    let mut saw_value = false;
    for (value, delimiter, _) in entries {
        if matches!(value, SqlValue::Null) {
            continue;
        }
        if distinct
            && !seen.insert((
                sql_value_distinct_key(&value),
                sql_value_distinct_key(&delimiter),
            ))
        {
            continue;
        }
        if saw_value && !matches!(delimiter, SqlValue::Null) {
            output.push_str(&delimiter.to_cell());
        }
        output.push_str(&value.to_cell());
        saw_value = true;
    }
    if saw_value {
        Ok(SqlValue::String(output))
    } else {
        Ok(SqlValue::Null)
    }
}

pub(crate) fn finish_json_agg(
    mut entries: Vec<(SqlValue, Vec<SqlValue>)>,
    distinct: bool,
    order_by: &[sqlparser::ast::OrderByExpr],
    binary: bool,
    strict: bool,
) -> Result<SqlValue> {
    if entries.is_empty() {
        return Ok(SqlValue::Null);
    }
    if !order_by.is_empty() {
        entries.sort_by(|left, right| json_agg_entry_ordering(left, right, order_by));
    }

    let mut seen = BTreeSet::new();
    let mut values = Vec::with_capacity(entries.len());
    for (value, _) in entries {
        if strict && matches!(value, SqlValue::Null) {
            continue;
        }
        if distinct && !seen.insert(sql_value_distinct_key(&value)) {
            continue;
        }
        values.push(value);
    }
    if binary {
        Ok(SqlValue::Json(JsonValue::Array(
            values.into_iter().map(sql_value_to_json).collect(),
        )))
    } else {
        let raw = format!(
            "[{}]",
            values
                .iter()
                .map(json_aggregate_input_text)
                .collect::<Vec<_>>()
                .join(", ")
        );
        Ok(SqlValue::JsonText(
            PgJsonText::parse(raw).expect("serialized JSON aggregate is valid"),
        ))
    }
}

fn json_aggregate_input_text(value: &SqlValue) -> String {
    match value {
        SqlValue::JsonText(value) => value.raw().to_string(),
        SqlValue::Json(value) => value.to_string(),
        value => sql_value_to_json(value.clone()).to_string(),
    }
}

pub(crate) fn json_agg_entry_ordering(
    left: &(SqlValue, Vec<SqlValue>),
    right: &(SqlValue, Vec<SqlValue>),
    order_by: &[sqlparser::ast::OrderByExpr],
) -> Ordering {
    aggregate_key_ordering(&left.1, &right.1, order_by)
}

fn aggregate_key_ordering(
    left_keys: &[SqlValue],
    right_keys: &[SqlValue],
    order_by: &[sqlparser::ast::OrderByExpr],
) -> Ordering {
    for ((left, right), order) in left_keys.iter().zip(right_keys).zip(order_by) {
        let ascending = order.options.asc != Some(false);
        let nulls_first = order.options.nulls_first.unwrap_or(!ascending);
        let ordering = match (
            matches!(left, SqlValue::Null),
            matches!(right, SqlValue::Null),
        ) {
            (true, true) => Ordering::Equal,
            (true, false) => {
                if nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (false, true) => {
                if nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, false) => {
                let ordering = value_ordering(left, right).unwrap_or_else(|| {
                    sql_value_distinct_key(left).cmp(&sql_value_distinct_key(right))
                });
                if ascending {
                    ordering
                } else {
                    ordering.reverse()
                }
            }
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

pub(crate) fn row_sum_value(
    engine: &SqlEngine<'_>,
    rows: &[SlotRow],
    columns: &[String],
    expr: &Expr,
    distinct: bool,
) -> Result<SqlValue> {
    sum_aggregate_values(row_aggregate_values(
        engine, rows, columns, expr, distinct, false,
    )?)
}

pub(crate) fn row_aggregate_column_name(expr: &Expr) -> String {
    row_aggregate_from_expr(expr)
        .map(|(aggregate, _)| aggregate.column_name())
        .unwrap_or_else(|_| row_expr_column_name(expr))
}

pub(crate) fn row_aggregate_values(
    engine: &SqlEngine<'_>,
    rows: &[SlotRow],
    columns: &[String],
    expr: &Expr,
    distinct: bool,
    include_null: bool,
) -> Result<Vec<SqlValue>> {
    let mut values = Vec::new();
    let mut seen = BTreeSet::new();
    let (_, context) = engine.bound_row_context(columns);
    for (idx, row) in rows.iter().enumerate() {
        if idx % 1024 == 0 {
            engine.check_cancellation()?;
        }
        let value = engine.eval_slot_row_value(row, &context, expr)?;
        push_aggregate_value(&mut values, &mut seen, value, distinct, include_null);
    }
    Ok(values)
}

pub(crate) fn row_aggregate_count(
    engine: &SqlEngine<'_>,
    rows: &[SlotRow],
    columns: &[String],
    expr: &Expr,
    distinct: bool,
) -> Result<i64> {
    let mut count = 0_i64;
    let mut seen = BTreeSet::new();
    let (_, context) = engine.bound_row_context(columns);
    for (idx, row) in rows.iter().enumerate() {
        if idx % 1024 == 0 {
            engine.check_cancellation()?;
        }
        let value = engine.eval_slot_row_value(row, &context, expr)?;
        if matches!(value, SqlValue::Null) {
            continue;
        }
        if distinct && !seen.insert(sql_value_distinct_key(&value)) {
            continue;
        }
        count += 1;
    }
    Ok(count)
}

pub(crate) fn push_aggregate_value(
    values: &mut Vec<SqlValue>,
    seen: &mut BTreeSet<SqlValueDistinctKey>,
    value: SqlValue,
    distinct: bool,
    include_null: bool,
) {
    if !include_null && matches!(value, SqlValue::Null) {
        return;
    }
    if distinct && !seen.insert(sql_value_distinct_key(&value)) {
        return;
    }
    values.push(value);
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum SqlValueDistinctKey {
    Null,
    Bool(bool),
    Int(i64),
    Float(u64),
    String(String),
    TsQuery(Vec<u8>),
    Json(String),
    Geometry(String),
    Composite(Vec<SqlValueDistinctKey>),
}

pub(crate) fn sql_value_distinct_key(value: &SqlValue) -> SqlValueDistinctKey {
    match value {
        SqlValue::Null => SqlValueDistinctKey::Null,
        SqlValue::Bool(value) => SqlValueDistinctKey::Bool(*value),
        SqlValue::Int(value) => SqlValueDistinctKey::Int(*value),
        SqlValue::Float(value) => SqlValueDistinctKey::Float(if value.is_nan() {
            f64::NAN.to_bits()
        } else if *value == 0.0 {
            0.0_f64.to_bits()
        } else {
            value.to_bits()
        }),
        SqlValue::String(value) => SqlValueDistinctKey::String(value.clone()),
        SqlValue::TsQuery(value) => SqlValueDistinctKey::TsQuery(value.index_key()),
        SqlValue::JsonText(value) => SqlValueDistinctKey::Json(value.raw().to_string()),
        SqlValue::Json(value) => SqlValueDistinctKey::Json(canonical_json_value_key(value)),
        SqlValue::Geometry(value) => SqlValueDistinctKey::Geometry(value.to_wkt()),
        SqlValue::Composite(value) => SqlValueDistinctKey::Composite(
            value
                .fields
                .iter()
                .map(|field| sql_value_distinct_key(&field.value))
                .collect(),
        ),
    }
}

pub(crate) fn row_extreme_value(
    engine: &SqlEngine<'_>,
    rows: &[SlotRow],
    columns: &[String],
    expr: &Expr,
    greatest: bool,
) -> Result<SqlValue> {
    let mut best = SqlValue::Null;
    let (_, context) = engine.bound_row_context(columns);
    let pg_type = row_columns_expr_pg_type(
        engine.db_ref(),
        expr,
        context.column_keys().iter().map(String::as_str),
    );
    for (idx, row) in rows.iter().enumerate() {
        if idx % 1024 == 0 {
            engine.check_cancellation()?;
        }
        let value = engine.eval_slot_row_value(row, &context, expr)?;
        if matches!(value, SqlValue::Null) {
            continue;
        }
        if matches!(best, SqlValue::Null) {
            best = value;
            continue;
        }
        let ordering = if matches!(pg_type.as_deref(), Some("inet" | "cidr")) {
            Some(pg_typed_compare_for_db(
                engine.db_ref(),
                "inet",
                &value,
                &best,
            )?)
        } else if matches!(pg_type.as_deref(), Some("macaddr" | "macaddr8")) {
            Some(pg_typed_compare_for_db(
                engine.db_ref(),
                pg_type.as_deref().unwrap(),
                &value,
                &best,
            )?)
        } else {
            value_ordering(&value, &best)
        };
        if ordering.is_some_and(|ordering| {
            if greatest {
                ordering == Ordering::Greater
            } else {
                ordering == Ordering::Less
            }
        }) {
            best = value;
        }
    }
    Ok(best)
}

pub(crate) fn row_aggregate_from_expr(expr: &Expr) -> Result<(RowAggregate, Option<&DataType>)> {
    match expr {
        Expr::Function(function) => Ok((RowAggregate::from_function(function)?, None)),
        Expr::Cast {
            expr, data_type, ..
        } => {
            let (aggregate, _) = row_aggregate_from_expr(expr)?;
            Ok((aggregate, Some(data_type)))
        }
        Expr::Nested(expr) => row_aggregate_from_expr(expr),
        _ => Err(SqlError::Unsupported(
            "aggregate SELECT cannot mix raw fields and aggregates without GROUP BY".to_string(),
        )),
    }
}

pub(crate) fn single_function_expr(function: &Function) -> Result<Expr> {
    let args = function_args(function);
    let [arg] = args.as_slice() else {
        return Err(SqlError::Unsupported(format!(
            "{} expects exactly one field argument",
            function.name
        )));
    };
    Ok(arg.clone())
}

/// Namespace exposing durable aggregate projections as ordinary relations:
/// `SELECT ... FROM bicdb_projection.<name> WHERE ... ORDER BY ...`. A
/// namespace rather than a bare name so a projection can never shadow a real
/// table.
pub(crate) const PROJECTION_RELATION_PREFIX: &str = "bicdb_projection.";

pub(crate) fn is_virtual_table(table: &str) -> bool {
    if table.starts_with(PROJECTION_RELATION_PREFIX) || table == "bicdb_projections" {
        return true;
    }
    matches!(
        table,
        "information_schema.tables"
            | "information_schema.columns"
            | "information_schema.domains"
            | "information_schema.element_types"
            | "information_schema.user_defined_types"
            | "information_schema.routines"
            | "information_schema.parameters"
            | "information_schema.table_constraints"
            | "information_schema.key_column_usage"
            | "information_schema.constraint_column_usage"
            | "information_schema.check_constraints"
            | "information_schema.sequences"
            | "information_schema.schemata"
            | "information_schema.role_table_grants"
            | "information_schema.table_privileges"
            | "pg_catalog.pg_database"
            | "pg_catalog.pg_default_acl"
            | "pg_catalog.pg_extension"
            | "pg_catalog.pg_namespace"
            | "pg_catalog.pg_class"
            | "pg_catalog.pg_attribute"
            | "pg_catalog.pg_type"
            | "pg_catalog.pg_foreign_data_wrapper"
            | "pg_catalog.pg_foreign_server"
            | "pg_catalog.pg_foreign_table"
            | "pg_catalog.pg_proc"
            | "pg_catalog.pg_language"
            | "pg_catalog.pg_trigger"
            | "pg_catalog.pg_sequence"
            | "pg_catalog.pg_sequences"
            | "pg_catalog.pg_tables"
            | "pg_catalog.pg_index"
            | "pg_catalog.pg_indexes"
            | "pg_catalog.pg_constraint"
            | "pg_catalog.pg_conversion"
            | "pg_catalog.pg_description"
            | "pg_catalog.pg_seclabel"
            | "pg_catalog.pg_shseclabel"
            | "pg_catalog.pg_seclabels"
            | "pg_catalog.pg_attrdef"
            | "pg_catalog.pg_rewrite"
            | "pg_catalog.pg_init_privs"
            | "pg_catalog.pg_cast"
            | "pg_catalog.pg_transform"
            | "pg_catalog.pg_depend"
            | "pg_catalog.pg_db_role_setting"
            | "pg_catalog.pg_inherits"
            | "pg_catalog.pg_largeobject_metadata"
            | "pg_catalog.pg_partitioned_table"
            | "pg_catalog.pg_statistic_ext"
            | "pg_catalog.pg_statistic_ext_data"
            | "pg_catalog.pg_enum"
            | "pg_catalog.pg_event_trigger"
            | "pg_catalog.pg_range"
            | "pg_catalog.pg_collation"
            | "pg_catalog.pg_am"
            | "pg_catalog.pg_amop"
            | "pg_catalog.pg_amproc"
            | "pg_catalog.pg_operator"
            | "pg_catalog.pg_opclass"
            | "pg_catalog.pg_opfamily"
            | "pg_catalog.pg_ts_config"
            | "pg_catalog.pg_ts_config_map"
            | "pg_catalog.pg_ts_dict"
            | "pg_catalog.pg_ts_parser"
            | "pg_catalog.pg_ts_template"
            | "pg_catalog.pg_tablespace"
            | "pg_catalog.pg_auth_members"
            | "pg_catalog.pg_roles"
            | "pg_catalog.pg_user"
            | "pg_catalog.pg_user_mappings"
            | "pg_catalog.pg_policy"
            | "pg_catalog.pg_policies"
            | "pg_catalog.pg_subscription"
            | "pg_catalog.pg_subscription_rel"
            | "pg_catalog.pg_publication"
            | "pg_catalog.pg_publication_rel"
            | "pg_catalog.pg_publication_namespace"
            | "pg_catalog.pg_timezone_names"
            | "pg_catalog.pg_settings"
            | "pg_catalog.pg_stat_database"
            | "pg_catalog.pg_stat_all_tables"
            | "pg_catalog.pg_stat_user_tables"
            | "pg_catalog.pg_stats"
            | "pg_catalog.pg_control_system"
            | "pg_catalog.pg_locks"
            | "pg_catalog.bicdb_notifications"
            | "pg_database"
            | "pg_default_acl"
            | "pg_extension"
            | "pg_namespace"
            | "pg_class"
            | "pg_attribute"
            | "pg_type"
            | "pg_foreign_data_wrapper"
            | "pg_foreign_server"
            | "pg_foreign_table"
            | "pg_proc"
            | "pg_language"
            | "pg_trigger"
            | "pg_sequence"
            | "pg_sequences"
            | "pg_tables"
            | "pg_index"
            | "pg_indexes"
            | "pg_constraint"
            | "pg_conversion"
            | "pg_description"
            | "pg_seclabel"
            | "pg_shseclabel"
            | "pg_seclabels"
            | "pg_attrdef"
            | "pg_rewrite"
            | "pg_init_privs"
            | "pg_cast"
            | "pg_transform"
            | "pg_depend"
            | "pg_db_role_setting"
            | "pg_inherits"
            | "pg_largeobject_metadata"
            | "pg_partitioned_table"
            | "pg_statistic_ext"
            | "pg_statistic_ext_data"
            | "pg_enum"
            | "pg_event_trigger"
            | "pg_range"
            | "pg_collation"
            | "pg_am"
            | "pg_amop"
            | "pg_amproc"
            | "pg_operator"
            | "pg_opclass"
            | "pg_opfamily"
            | "pg_ts_config"
            | "pg_ts_config_map"
            | "pg_ts_dict"
            | "pg_ts_parser"
            | "pg_ts_template"
            | "pg_tablespace"
            | "pg_auth_members"
            | "pg_roles"
            | "pg_user"
            | "pg_user_mappings"
            | "pg_policy"
            | "pg_policies"
            | "pg_subscription"
            | "pg_subscription_rel"
            | "pg_publication"
            | "pg_publication_rel"
            | "pg_publication_namespace"
            | "pg_timezone_names"
            | "pg_settings"
            | "pg_stat_database"
            | "pg_stat_all_tables"
            | "pg_stat_user_tables"
            | "pg_stats"
            | "pg_control_system"
            | "pg_locks"
            | "bicdb_notifications"
            | "bicdb_replication_status"
            | "bicdb_replication_lag"
            | "bicdb_replication_nodes"
            | "bicdb_replication_errors"
            | "bicdb_consensus_status"
            | GRAPH_NODES_TABLE
            | GRAPH_EDGES_TABLE
    )
}

pub(crate) fn virtual_rows(db: &BicDb, table: &str) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let table = table.strip_prefix("pg_catalog.").unwrap_or(table);
    let mut rows = match table {
        "information_schema.schemata" => Ok(vec![virtual_row([
            ("catalog_name", SqlValue::String("bicdb".to_string())),
            ("schema_name", SqlValue::String("public".to_string())),
        ])]),
        "information_schema.tables" => information_schema_tables(db),
        "information_schema.columns" => information_schema_columns(db),
        "information_schema.domains" => information_schema_domains(db),
        "information_schema.element_types" => information_schema_element_types(db),
        "information_schema.user_defined_types" => information_schema_user_defined_types(db),
        "information_schema.routines" => information_schema_routines(db),
        "information_schema.parameters" => information_schema_parameters(db),
        "information_schema.table_constraints" => information_schema_table_constraints(db),
        "information_schema.key_column_usage" => information_schema_key_column_usage(db),
        "information_schema.constraint_column_usage" => {
            information_schema_constraint_column_usage(db)
        }
        "information_schema.check_constraints" => information_schema_check_constraints(db),
        "information_schema.sequences" => information_schema_sequences(db),
        "information_schema.role_table_grants" | "information_schema.table_privileges" => {
            information_schema_role_table_grants(db)
        }
        GRAPH_NODES_TABLE => Ok(graph_node_rows(db)),
        GRAPH_EDGES_TABLE => Ok(graph_edge_rows(db)),
        "pg_database" => pg_database_rows(db),
        "pg_namespace" => {
            let privileges = list_privileges(db)?;
            let mut namespace_names = [
                "public".to_string(),
                "pg_catalog".to_string(),
                "information_schema".to_string(),
            ]
            .into_iter()
            .collect::<BTreeSet<_>>();
            for namespace in list_namespaces(db)? {
                namespace_names.insert(namespace.name);
            }
            for schema in list_schemas(db)? {
                namespace_names.insert(schema.schema_name);
            }
            let mut rows = Vec::new();
            for namespace in namespace_names {
                let owner = namespace_owner(db, &namespace)?;
                rows.push(virtual_row([
                    ("oid", SqlValue::Int(namespace_oid(&namespace))),
                    ("nspname", SqlValue::String(namespace.clone())),
                    ("nspowner", SqlValue::Int(role_oid(&owner))),
                    (
                        "nspacl",
                        schema_acl_value_from_privileges(&privileges, &namespace)?,
                    ),
                ]));
            }
            Ok(rows)
        }
        "pg_extension" => pg_extension_rows(db),
        "pg_class" => pg_class_rows(db),
        "pg_attribute" => pg_attribute_rows(db),
        "pg_type" => pg_type_rows_for_db(db),
        "pg_proc" => pg_proc_rows(db),
        "pg_language" => Ok(pg_language_rows()),
        "pg_trigger" => pg_trigger_rows(db),
        "pg_sequence" => pg_sequence_rows(db),
        "pg_sequences" => pg_sequences_rows(db),
        "pg_tables" => pg_tables_rows(db),
        "pg_index" => pg_index_rows(db),
        "pg_indexes" => pg_indexes_rows(db),
        "pg_constraint" => pg_constraint_rows(db),
        "pg_description" => Ok(list_user_types(db)?
            .into_iter()
            .filter_map(|user_type| {
                user_type.comment.map(|description| {
                    virtual_row([
                        ("objoid", SqlValue::Int(user_type.oid)),
                        ("classoid", SqlValue::Int(1247)),
                        ("objsubid", SqlValue::Int(0)),
                        ("description", SqlValue::String(description)),
                    ])
                })
            })
            .collect()),
        "pg_seclabel" | "pg_shseclabel" | "pg_seclabels" => Ok(Vec::new()),
        "pg_attrdef" => pg_attrdef_rows(db),
        "pg_rewrite" => pg_rewrite_rows(db),
        "pg_conversion" => Ok(Vec::new()),
        "pg_cast" => pg_cast_rows(db),
        "pg_init_privs" | "pg_transform" => Ok(Vec::new()),
        "pg_depend" => pg_depend_rows(db),
        "pg_db_role_setting" => Ok(Vec::new()),
        "pg_inherits" => pg_inherits_rows(db),
        "pg_largeobject_metadata" => Ok(Vec::new()),
        "pg_partitioned_table" => pg_partitioned_table_rows(db),
        "pg_statistic_ext" | "pg_statistic_ext_data" => Ok(Vec::new()),
        "pg_enum" => pg_enum_rows(db),
        "pg_event_trigger" => Ok(Vec::new()),
        "pg_range" => pg_range_rows(db),
        "pg_stats" => pg_stats_rows(db),
        "pg_collation" => Ok(pg_collation_rows()),
        "pg_am" => Ok(pg_am_rows()),
        "pg_opclass" => Ok(pg_opclass_rows()),
        "pg_opfamily" => Ok(pg_opfamily_rows()),
        "pg_amop" => Ok(pg_amop_rows()),
        "pg_amproc" => Ok(pg_amproc_rows()),
        "pg_operator" => Ok(Vec::new()),
        "pg_ts_config" | "pg_ts_config_map" | "pg_ts_dict" | "pg_ts_parser" | "pg_ts_template" => {
            Ok(Vec::new())
        }
        "pg_foreign_data_wrapper"
        | "pg_foreign_server"
        | "pg_foreign_table"
        | "pg_user_mappings" => Ok(Vec::new()),
        "pg_default_acl" => pg_default_acl_rows(db),
        "pg_tablespace" => Ok(pg_tablespace_rows()),
        "pg_auth_members" => pg_auth_members_rows(db),
        "pg_roles" => pg_roles_rows(db),
        "pg_user" => pg_user_rows(db),
        "pg_policy" => pg_policy_rows(db),
        "pg_policies" => pg_policies_rows(db),
        "pg_subscription" | "pg_subscription_rel" => Ok(Vec::new()),
        "pg_publication" | "pg_publication_rel" | "pg_publication_namespace" => Ok(Vec::new()),
        "pg_timezone_names" => Ok(pg_timezone_names_rows()),
        "pg_settings" => Ok(pg_settings_rows()),
        "pg_stat_database" => pg_stat_database_rows(db),
        "pg_stat_all_tables" | "pg_stat_user_tables" => pg_stat_table_rows(db, None),
        "pg_control_system" => Ok(vec![virtual_row([
            ("pg_control_version", SqlValue::Int(1_300)),
            ("catalog_version_no", SqlValue::Int(202_406_221)),
            ("system_identifier", SqlValue::Int(1_802_028_600_100)),
            (
                "pg_control_last_modified",
                SqlValue::String("2026-06-22 00:00:00+00".to_string()),
            ),
        ])]),
        "pg_locks" => Ok(Vec::new()),
        "bicdb_notifications" => bicdb_notification_rows(db),
        "bicdb_replication_status" => bicdb_replication_status_rows(db),
        "bicdb_replication_lag" => bicdb_replication_lag_rows(db),
        "bicdb_replication_nodes" => Ok(Vec::new()),
        "bicdb_replication_errors" => Ok(Vec::new()),
        "bicdb_consensus_status" => Ok(vec![bicdb_consensus_status_rows(db)]),
        other => Err(SqlError::Unsupported(format!(
            "virtual table {other} is not implemented"
        ))),
    }?;
    if let Some(tableoid) = virtual_catalog_table_oid(table) {
        for row in &mut rows {
            row.entry("tableoid".to_string())
                .or_insert(SqlValue::Int(tableoid));
        }
    }
    Ok(rows)
}

fn pg_default_acl_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let mut grouped = BTreeMap::<(String, String, String), Vec<PrivilegeGrant>>::new();
    for grant in list_default_privileges(db)? {
        let object_code = match grant.object_type {
            PrivilegeObjectType::Table => "r",
            PrivilegeObjectType::Sequence => "S",
            PrivilegeObjectType::Function => "f",
            _ => continue,
        };
        grouped
            .entry((
                grant.grantor.clone(),
                grant.schema_name.clone(),
                object_code.to_string(),
            ))
            .or_default()
            .push(PrivilegeGrant {
                column: None,
                object_type: grant.object_type,
                object_name: String::new(),
                grantee: grant.grantee,
                privilege: grant.privilege,
            });
    }
    grouped
        .into_iter()
        .map(|((grantor, schema_name, object_code), grants)| {
            Ok(virtual_row([
                (
                    "oid",
                    SqlValue::Int(named_relation_oid(&format!(
                        "pg_default_acl:{grantor}:{schema_name}:{object_code}"
                    ))),
                ),
                ("defaclrole", SqlValue::Int(role_oid(&grantor))),
                (
                    "defaclnamespace",
                    SqlValue::Int(if schema_name == "*" {
                        0
                    } else {
                        namespace_oid(&schema_name)
                    }),
                ),
                ("defaclobjtype", SqlValue::String(object_code)),
                ("defaclacl", acl_value_with_grantor(grants, &grantor)?),
            ]))
        })
        .collect()
}

pub(crate) fn virtual_table_columns(table: &str) -> Option<Vec<String>> {
    let table = table.strip_prefix("pg_catalog.").unwrap_or(table);
    match table {
        "information_schema.columns" => Some(
            [
                "table_catalog",
                "table_schema",
                "table_name",
                "column_name",
                "ordinal_position",
                "column_default",
                "is_nullable",
                "data_type",
                "character_maximum_length",
                "character_octet_length",
                "numeric_precision",
                "numeric_precision_radix",
                "numeric_scale",
                "datetime_precision",
                "interval_type",
                "interval_precision",
                "character_set_catalog",
                "character_set_schema",
                "character_set_name",
                "collation_catalog",
                "collation_schema",
                "collation_name",
                "domain_catalog",
                "domain_schema",
                "domain_name",
                "udt_catalog",
                "udt_schema",
                "udt_name",
                "scope_catalog",
                "scope_schema",
                "scope_name",
                "maximum_cardinality",
                "dtd_identifier",
                "is_self_referencing",
                "is_identity",
                "identity_generation",
                "identity_start",
                "identity_increment",
                "identity_maximum",
                "identity_minimum",
                "identity_cycle",
                "is_generated",
                "generation_expression",
                "is_updatable",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "information_schema.domains" => Some(
            [
                "domain_catalog",
                "domain_schema",
                "domain_name",
                "data_type",
                "character_maximum_length",
                "character_octet_length",
                "character_set_catalog",
                "character_set_schema",
                "character_set_name",
                "collation_catalog",
                "collation_schema",
                "collation_name",
                "numeric_precision",
                "numeric_precision_radix",
                "numeric_scale",
                "datetime_precision",
                "interval_type",
                "interval_precision",
                "domain_default",
                "udt_catalog",
                "udt_schema",
                "udt_name",
                "scope_catalog",
                "scope_schema",
                "scope_name",
                "maximum_cardinality",
                "dtd_identifier",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "information_schema.element_types" => Some(
            [
                "object_catalog",
                "object_schema",
                "object_name",
                "object_type",
                "collection_type_identifier",
                "data_type",
                "character_maximum_length",
                "character_octet_length",
                "character_set_catalog",
                "character_set_schema",
                "character_set_name",
                "collation_catalog",
                "collation_schema",
                "collation_name",
                "numeric_precision",
                "numeric_precision_radix",
                "numeric_scale",
                "datetime_precision",
                "interval_type",
                "interval_precision",
                "udt_catalog",
                "udt_schema",
                "udt_name",
                "scope_catalog",
                "scope_schema",
                "scope_name",
                "maximum_cardinality",
                "dtd_identifier",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "information_schema.user_defined_types" => Some(
            [
                "user_defined_type_catalog",
                "user_defined_type_schema",
                "user_defined_type_name",
                "user_defined_type_category",
                "is_instantiable",
                "is_final",
                "ordering_form",
                "ordering_category",
                "ordering_routine_catalog",
                "ordering_routine_schema",
                "ordering_routine_name",
                "reference_type",
                "data_type",
                "character_maximum_length",
                "character_octet_length",
                "character_set_catalog",
                "character_set_schema",
                "character_set_name",
                "collation_catalog",
                "collation_schema",
                "collation_name",
                "numeric_precision",
                "numeric_precision_radix",
                "numeric_scale",
                "datetime_precision",
                "interval_type",
                "interval_precision",
                "source_dtd_identifier",
                "ref_dtd_identifier",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "information_schema.routines" => Some(
            [
                "specific_catalog",
                "specific_schema",
                "specific_name",
                "routine_catalog",
                "routine_schema",
                "routine_name",
                "routine_type",
                "module_catalog",
                "module_schema",
                "module_name",
                "udt_catalog",
                "udt_schema",
                "udt_name",
                "data_type",
                "character_maximum_length",
                "character_octet_length",
                "character_set_catalog",
                "character_set_schema",
                "character_set_name",
                "collation_catalog",
                "collation_schema",
                "collation_name",
                "numeric_precision",
                "numeric_precision_radix",
                "numeric_scale",
                "datetime_precision",
                "interval_type",
                "interval_precision",
                "type_udt_catalog",
                "type_udt_schema",
                "type_udt_name",
                "scope_catalog",
                "scope_schema",
                "scope_name",
                "maximum_cardinality",
                "dtd_identifier",
                "routine_body",
                "routine_definition",
                "external_name",
                "external_language",
                "parameter_style",
                "is_deterministic",
                "sql_data_access",
                "is_null_call",
                "sql_path",
                "schema_level_routine",
                "max_dynamic_result_sets",
                "is_user_defined_cast",
                "is_implicitly_invocable",
                "security_type",
                "to_sql_specific_catalog",
                "to_sql_specific_schema",
                "to_sql_specific_name",
                "as_locator",
                "created",
                "last_altered",
                "new_savepoint_level",
                "is_udt_dependent",
                "result_cast_from_data_type",
                "result_cast_as_locator",
                "result_cast_char_max_length",
                "result_cast_char_octet_length",
                "result_cast_char_set_catalog",
                "result_cast_char_set_schema",
                "result_cast_char_set_name",
                "result_cast_collation_catalog",
                "result_cast_collation_schema",
                "result_cast_collation_name",
                "result_cast_numeric_precision",
                "result_cast_numeric_precision_radix",
                "result_cast_numeric_scale",
                "result_cast_datetime_precision",
                "result_cast_interval_type",
                "result_cast_interval_precision",
                "result_cast_type_udt_catalog",
                "result_cast_type_udt_schema",
                "result_cast_type_udt_name",
                "result_cast_scope_catalog",
                "result_cast_scope_schema",
                "result_cast_scope_name",
                "result_cast_maximum_cardinality",
                "result_cast_dtd_identifier",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "information_schema.parameters" => Some(
            [
                "specific_catalog",
                "specific_schema",
                "specific_name",
                "ordinal_position",
                "parameter_mode",
                "is_result",
                "as_locator",
                "parameter_name",
                "data_type",
                "character_maximum_length",
                "character_octet_length",
                "character_set_catalog",
                "character_set_schema",
                "character_set_name",
                "collation_catalog",
                "collation_schema",
                "collation_name",
                "numeric_precision",
                "numeric_precision_radix",
                "numeric_scale",
                "datetime_precision",
                "interval_type",
                "interval_precision",
                "udt_catalog",
                "udt_schema",
                "udt_name",
                "scope_catalog",
                "scope_schema",
                "scope_name",
                "maximum_cardinality",
                "dtd_identifier",
                "parameter_default",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "bicdb_replication_status" => Some(
            [
                "cluster_id",
                "source_node_id",
                "stream_id",
                "current_commit_seq",
                "last_applied_commit_seq",
                "oldest_available_commit_seq",
                "newest_available_commit_seq",
                "retained_commits",
                "retained_bytes",
                "last_applied_at",
                "last_error",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "bicdb_replication_lag" => Some(
            [
                "source_commit_seq",
                "last_applied_commit_seq",
                "lag_commits",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "bicdb_replication_nodes" => Some(
            ["node_id", "role", "connected", "last_seen_at"]
                .into_iter()
                .map(str::to_string)
                .collect(),
        ),
        "bicdb_replication_errors" => Some(
            ["occurred_at", "node_id", "code", "message"]
                .into_iter()
                .map(str::to_string)
                .collect(),
        ),
        "bicdb_consensus_status" => Some(
            [
                "cluster_id",
                "node_id",
                "role",
                "current_term",
                "leader_id",
                "commit_index",
                "last_applied",
                "last_log_index",
                "voter_count",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "pg_init_privs" => Some(
            ["objoid", "classoid", "objsubid", "privtype", "initprivs"]
                .into_iter()
                .map(str::to_string)
                .collect(),
        ),
        "pg_description" => Some(
            ["objoid", "classoid", "objsubid", "description"]
                .into_iter()
                .map(str::to_string)
                .collect(),
        ),
        "pg_cast" => Some(
            [
                "oid",
                "castsource",
                "casttarget",
                "castfunc",
                "castcontext",
                "castmethod",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "pg_transform" => Some(
            ["oid", "trftype", "trflang", "trffromsql", "trftosql"]
                .into_iter()
                .map(str::to_string)
                .collect(),
        ),
        "pg_rewrite" => Some(
            [
                "oid",
                "rulename",
                "ev_class",
                "ev_type",
                "ev_enabled",
                "is_instead",
                "ev_qual",
                "ev_action",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "pg_language" => Some(
            [
                "oid",
                "lanname",
                "lanowner",
                "lanispl",
                "lanpltrusted",
                "lanplcallfoid",
                "laninline",
                "lanvalidator",
                "lanacl",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "pg_depend" => Some(
            [
                "classid",
                "objid",
                "objsubid",
                "refclassid",
                "refobjid",
                "refobjsubid",
                "deptype",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "pg_seclabel" | "pg_seclabels" => Some(
            ["objoid", "classoid", "objsubid", "provider", "label"]
                .into_iter()
                .map(str::to_string)
                .collect(),
        ),
        "pg_shseclabel" => Some(
            ["objoid", "classoid", "provider", "label"]
                .into_iter()
                .map(str::to_string)
                .collect(),
        ),
        "pg_operator" => Some(
            [
                "oid",
                "oprname",
                "oprnamespace",
                "oprowner",
                "oprkind",
                "oprcanmerge",
                "oprcanhash",
                "oprleft",
                "oprright",
                "oprresult",
                "oprcom",
                "oprnegate",
                "oprcode",
                "oprrest",
                "oprjoin",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "pg_opclass" => Some(
            [
                "oid",
                "opcmethod",
                "opcname",
                "opcnamespace",
                "opcowner",
                "opcfamily",
                "opcintype",
                "opcdefault",
                "opckeytype",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "pg_opfamily" => Some(
            ["oid", "opfmethod", "opfname", "opfnamespace", "opfowner"]
                .into_iter()
                .map(str::to_string)
                .collect(),
        ),
        "pg_amop" => Some(
            [
                "oid",
                "amopfamily",
                "amoplefttype",
                "amoprighttype",
                "amopstrategy",
                "amoppurpose",
                "amopopr",
                "amopmethod",
                "amopsortfamily",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "pg_amproc" => Some(
            [
                "oid",
                "amprocfamily",
                "amproclefttype",
                "amprocrighttype",
                "amprocnum",
                "amproc",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "pg_ts_parser" => Some(
            [
                "oid",
                "prsname",
                "prsnamespace",
                "prsstart",
                "prstoken",
                "prsend",
                "prsheadline",
                "prslextype",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "pg_ts_dict" => Some(
            [
                "oid",
                "dictname",
                "dictnamespace",
                "dictowner",
                "dicttemplate",
                "dictinitoption",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "pg_ts_template" => Some(
            ["oid", "tmplname", "tmplnamespace", "tmplinit", "tmpllexize"]
                .into_iter()
                .map(str::to_string)
                .collect(),
        ),
        "pg_ts_config" => Some(
            ["oid", "cfgname", "cfgnamespace", "cfgowner", "cfgparser"]
                .into_iter()
                .map(str::to_string)
                .collect(),
        ),
        "pg_ts_config_map" => Some(
            ["mapcfg", "maptokentype", "mapseqno", "mapdict"]
                .into_iter()
                .map(str::to_string)
                .collect(),
        ),
        "pg_foreign_data_wrapper" => Some(
            [
                "oid",
                "fdwname",
                "fdwowner",
                "fdwhandler",
                "fdwvalidator",
                "fdwacl",
                "fdwoptions",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "pg_foreign_server" => Some(
            [
                "oid",
                "srvname",
                "srvowner",
                "srvfdw",
                "srvtype",
                "srvversion",
                "srvacl",
                "srvoptions",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "pg_foreign_table" => Some(
            ["ftrelid", "ftserver", "ftoptions"]
                .into_iter()
                .map(str::to_string)
                .collect(),
        ),
        "pg_user_mappings" => Some(
            ["umid", "srvid", "srvname", "umuser", "usename", "umoptions"]
                .into_iter()
                .map(str::to_string)
                .collect(),
        ),
        "pg_auth_members" => Some(
            [
                "roleid",
                "member",
                "grantor",
                "admin_option",
                "inherit_option",
                "set_option",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "pg_subscription" => Some(
            [
                "oid",
                "subdbid",
                "subskiplsn",
                "subname",
                "subowner",
                "subenabled",
                "subbinary",
                "substream",
                "subtwophasestate",
                "subdisableonerr",
                "subpasswordrequired",
                "subrunasowner",
                "subfailover",
                "subconninfo",
                "subslotname",
                "subsynccommit",
                "subpublications",
                "suborigin",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "pg_subscription_rel" => Some(
            ["srsubid", "srrelid", "srsubstate", "srsublsn"]
                .into_iter()
                .map(str::to_string)
                .collect(),
        ),
        "pg_publication" => Some(
            [
                "oid",
                "pubname",
                "pubowner",
                "puballtables",
                "pubinsert",
                "pubupdate",
                "pubdelete",
                "pubtruncate",
                "pubviaroot",
                "pubgencols",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "pg_publication_rel" => Some(
            ["oid", "prpubid", "prrelid", "prqual", "prattrs"]
                .into_iter()
                .map(str::to_string)
                .collect(),
        ),
        "pg_publication_namespace" => Some(
            ["oid", "pnpubid", "pnnspid"]
                .into_iter()
                .map(str::to_string)
                .collect(),
        ),
        "pg_default_acl" => Some(
            [
                "oid",
                "defaclrole",
                "defaclnamespace",
                "defaclobjtype",
                "defaclacl",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "pg_largeobject_metadata" => Some(
            ["oid", "lomowner", "lomacl"]
                .into_iter()
                .map(str::to_string)
                .collect(),
        ),
        "pg_db_role_setting" => Some(
            ["setdatabase", "setrole", "setconfig"]
                .into_iter()
                .map(str::to_string)
                .collect(),
        ),
        "pg_conversion" => Some(
            [
                "oid",
                "conname",
                "connamespace",
                "conowner",
                "conforencoding",
                "contoencoding",
                "conproc",
                "condefault",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "pg_event_trigger" => Some(
            [
                "oid",
                "evtname",
                "evtevent",
                "evtowner",
                "evtfoid",
                "evtenabled",
                "evttags",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "pg_statistic_ext" => Some(
            [
                "oid",
                "stxrelid",
                "stxname",
                "stxnamespace",
                "stxowner",
                "stxstattarget",
                "stxkeys",
                "stxkind",
                "stxexprs",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "pg_statistic_ext_data" => Some(
            [
                "stxoid",
                "stxdinherit",
                "stxdndistinct",
                "stxddependencies",
                "stxdmcv",
                "stxdexpr",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "pg_stats" => Some(
            [
                "schemaname",
                "tablename",
                "attname",
                "inherited",
                "null_frac",
                "avg_width",
                "n_distinct",
                "most_common_vals",
                "most_common_freqs",
                "histogram_bounds",
                "correlation",
                "most_common_elems",
                "most_common_elem_freqs",
                "elem_count_histogram",
                "range_length_histogram",
                "range_empty_frac",
                "range_bounds_histogram",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "pg_tables" => Some(
            [
                "schemaname",
                "tablename",
                "tableowner",
                "tablespace",
                "hasindexes",
                "hasrules",
                "hastriggers",
                "rowsecurity",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "pg_inherits" => Some(
            ["inhrelid", "inhparent", "inhseqno", "inhdetachpending"]
                .into_iter()
                .map(str::to_string)
                .collect(),
        ),
        "pg_partitioned_table" => Some(
            [
                "partrelid",
                "partstrat",
                "partnatts",
                "partdefid",
                "partattrs",
                "partclass",
                "partcollation",
                "partexprs",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "pg_locks" => Some(
            [
                "locktype",
                "database",
                "relation",
                "page",
                "tuple",
                "virtualxid",
                "transactionid",
                "classid",
                "objid",
                "objsubid",
                "virtualtransaction",
                "pid",
                "mode",
                "granted",
                "fastpath",
                "waitstart",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "pg_control_system" => Some(
            [
                "pg_control_version",
                "catalog_version_no",
                "system_identifier",
                "pg_control_last_modified",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "pg_timezone_names" => Some(
            ["name", "abbrev", "utc_offset", "is_dst"]
                .into_iter()
                .map(str::to_string)
                .collect(),
        ),
        "pg_settings" => Some(
            [
                "name",
                "setting",
                "unit",
                "category",
                "short_desc",
                "extra_desc",
                "context",
                "vartype",
                "source",
                "min_val",
                "max_val",
                "enumvals",
                "boot_val",
                "reset_val",
                "sourcefile",
                "sourceline",
                "pending_restart",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "pg_stat_all_tables" | "pg_stat_user_tables" => Some(
            [
                "relid",
                "schemaname",
                "relname",
                "seq_scan",
                "seq_tup_read",
                "idx_scan",
                "idx_tup_fetch",
                "n_tup_ins",
                "n_tup_upd",
                "n_tup_del",
                "n_tup_hot_upd",
                "n_tup_newpage_upd",
                "n_live_tup",
                "n_dead_tup",
                "n_mod_since_analyze",
                "n_ins_since_vacuum",
                "last_vacuum",
                "last_autovacuum",
                "last_analyze",
                "last_autoanalyze",
                "vacuum_count",
                "autovacuum_count",
                "analyze_count",
                "autoanalyze_count",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        "pg_stat_database" => Some(
            [
                "datid",
                "datname",
                "numbackends",
                "xact_commit",
                "xact_rollback",
                "blks_read",
                "blks_hit",
                "tup_returned",
                "tup_fetched",
                "tup_inserted",
                "tup_updated",
                "tup_deleted",
                "conflicts",
                "temp_files",
                "temp_bytes",
                "deadlocks",
                "checksum_failures",
                "checksum_last_failure",
                "blk_read_time",
                "blk_write_time",
                "session_time",
                "active_time",
                "idle_in_transaction_time",
                "sessions",
                "sessions_abandoned",
                "sessions_fatal",
                "sessions_killed",
                "stats_reset",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
        _ => None,
    }
}

pub(crate) fn virtual_table_column_types(table: &str) -> Option<Vec<(String, Option<String>)>> {
    let table = table.strip_prefix("pg_catalog.").unwrap_or(table);
    let columns: &[(&str, &str)] = match table {
        "information_schema.domains" => &[
            ("domain_catalog", "name"),
            ("domain_schema", "name"),
            ("domain_name", "name"),
            ("data_type", "varchar"),
            ("domain_default", "varchar"),
            ("udt_schema", "name"),
            ("udt_name", "name"),
            ("numeric_precision", "int4"),
            ("numeric_scale", "int4"),
            ("dtd_identifier", "name"),
        ],
        "information_schema.columns" => &[
            ("table_catalog", "name"),
            ("table_schema", "name"),
            ("table_name", "name"),
            ("column_name", "name"),
            ("ordinal_position", "int4"),
            ("column_default", "varchar"),
            ("is_nullable", "varchar"),
            ("data_type", "varchar"),
            ("domain_schema", "name"),
            ("domain_name", "name"),
            ("udt_catalog", "name"),
            ("udt_schema", "name"),
            ("udt_name", "name"),
            ("character_maximum_length", "int4"),
            ("character_octet_length", "int4"),
            ("collation_catalog", "name"),
            ("collation_schema", "name"),
            ("collation_name", "name"),
            ("is_identity", "varchar"),
            ("identity_generation", "varchar"),
            ("identity_start", "varchar"),
            ("identity_increment", "varchar"),
            ("identity_maximum", "varchar"),
            ("identity_minimum", "varchar"),
            ("identity_cycle", "varchar"),
            ("dtd_identifier", "name"),
        ],
        "information_schema.element_types" => &[
            ("object_catalog", "name"),
            ("object_schema", "name"),
            ("object_name", "name"),
            ("object_type", "varchar"),
            ("collection_type_identifier", "name"),
            ("data_type", "varchar"),
            ("udt_schema", "name"),
            ("udt_name", "name"),
            ("dtd_identifier", "name"),
        ],
        "information_schema.user_defined_types" => &[
            ("user_defined_type_catalog", "name"),
            ("user_defined_type_schema", "name"),
            ("user_defined_type_name", "name"),
            ("user_defined_type_category", "varchar"),
            ("is_instantiable", "varchar"),
            ("is_final", "varchar"),
            ("ordering_form", "varchar"),
        ],
        "information_schema.routines" => &[
            ("specific_catalog", "name"),
            ("specific_schema", "name"),
            ("specific_name", "name"),
            ("routine_catalog", "name"),
            ("routine_schema", "name"),
            ("routine_name", "name"),
            ("routine_type", "varchar"),
            ("data_type", "varchar"),
            ("type_udt_schema", "name"),
            ("type_udt_name", "name"),
            ("routine_body", "varchar"),
            ("external_language", "varchar"),
            ("parameter_style", "varchar"),
            ("is_deterministic", "varchar"),
            ("sql_data_access", "varchar"),
            ("security_type", "varchar"),
            ("dtd_identifier", "name"),
        ],
        "information_schema.parameters" => &[
            ("specific_catalog", "name"),
            ("specific_schema", "name"),
            ("specific_name", "name"),
            ("ordinal_position", "int4"),
            ("parameter_mode", "varchar"),
            ("is_result", "varchar"),
            ("as_locator", "varchar"),
            ("parameter_name", "name"),
            ("data_type", "varchar"),
            ("udt_schema", "name"),
            ("udt_name", "name"),
            ("dtd_identifier", "name"),
        ],
        "pg_type" => &[
            ("oid", "oid"),
            ("typname", "name"),
            ("typnamespace", "oid"),
            ("typowner", "oid"),
            ("typlen", "int2"),
            ("typbyval", "bool"),
            ("typtype", "char"),
            ("typcategory", "char"),
            ("typispreferred", "bool"),
            ("typisdefined", "bool"),
            ("typdelim", "char"),
            ("typrelid", "oid"),
            ("typelem", "oid"),
            ("typarray", "oid"),
            ("typinput", "regproc"),
            ("typoutput", "regproc"),
            ("typreceive", "regproc"),
            ("typsend", "regproc"),
            ("typmodin", "regproc"),
            ("typmodout", "regproc"),
            ("typanalyze", "regproc"),
            ("typalign", "char"),
            ("typstorage", "char"),
            ("typnotnull", "bool"),
            ("typbasetype", "oid"),
            ("typtypmod", "int4"),
            ("typndims", "int4"),
            ("typcollation", "oid"),
            ("typdefaultbin", "text"),
            ("typdefault", "text"),
        ],
        "pg_cast" => &[
            ("oid", "oid"),
            ("castsource", "oid"),
            ("casttarget", "oid"),
            ("castfunc", "oid"),
            ("castcontext", "char"),
            ("castmethod", "char"),
        ],
        "pg_namespace" => &[
            ("oid", "oid"),
            ("nspname", "name"),
            ("nspowner", "oid"),
            ("nspacl", "text[]"),
        ],
        "pg_range" => &[
            ("rngtypid", "oid"),
            ("rngsubtype", "oid"),
            ("rngmultitypid", "oid"),
            ("rngcollation", "oid"),
            ("rngsubopc", "oid"),
            ("rngcanonical", "regproc"),
            ("rngsubdiff", "regproc"),
        ],
        "pg_stats" => &[
            ("schemaname", "name"),
            ("tablename", "name"),
            ("attname", "name"),
            ("inherited", "bool"),
            ("null_frac", "float4"),
            ("avg_width", "int4"),
            ("n_distinct", "float4"),
            ("most_common_vals", "anyarray"),
            ("most_common_freqs", "float4[]"),
            ("histogram_bounds", "anyarray"),
            ("correlation", "float4"),
            ("most_common_elems", "anyarray"),
            ("most_common_elem_freqs", "float4[]"),
            ("elem_count_histogram", "float4[]"),
            ("range_length_histogram", "anyarray"),
            ("range_empty_frac", "float4"),
            ("range_bounds_histogram", "anyarray"),
        ],
        "pg_attribute" => &[
            ("attrelid", "oid"),
            ("attname", "name"),
            ("atttypid", "oid"),
            ("attstattarget", "int2"),
            ("attlen", "int2"),
            ("attnum", "int2"),
            ("attndims", "int2"),
            ("attcacheoff", "int4"),
            ("atttypmod", "int4"),
            ("attbyval", "bool"),
            ("attalign", "char"),
            ("attstorage", "char"),
            ("attcompression", "char"),
            ("attnotnull", "bool"),
            ("atthasdef", "bool"),
            ("atthasmissing", "bool"),
            ("attidentity", "char"),
            ("attgenerated", "char"),
            ("attisdropped", "bool"),
            ("attislocal", "bool"),
            ("attinhcount", "int2"),
            ("attcollation", "oid"),
            ("attacl", "aclitem[]"),
            ("attoptions", "text[]"),
            ("attfdwoptions", "text[]"),
            ("attmissingval", "anyarray"),
        ],
        "pg_class" => &[
            ("oid", "oid"),
            ("relname", "name"),
            ("relnamespace", "oid"),
            ("reltype", "oid"),
            ("reloftype", "oid"),
            ("relowner", "oid"),
            ("relam", "oid"),
            ("relfilenode", "oid"),
            ("reltablespace", "oid"),
            ("relpages", "int4"),
            ("reltuples", "float4"),
            ("relallvisible", "int4"),
            ("relallfrozen", "int4"),
            ("reltoastrelid", "oid"),
            ("relhasindex", "bool"),
            ("relisshared", "bool"),
            ("relpersistence", "char"),
            ("relkind", "char"),
            ("relnatts", "int2"),
            ("relchecks", "int2"),
            ("relhasrules", "bool"),
            ("relhastriggers", "bool"),
            ("relhassubclass", "bool"),
            ("relrowsecurity", "bool"),
            ("relforcerowsecurity", "bool"),
            ("relispopulated", "bool"),
            ("relreplident", "char"),
            ("relispartition", "bool"),
            ("relrewrite", "oid"),
            ("relfrozenxid", "xid"),
            ("relminmxid", "xid"),
            ("relacl", "aclitem[]"),
            ("reloptions", "text[]"),
            ("relpartbound", "pg_node_tree"),
        ],
        "pg_timezone_names" => &[
            ("name", "text"),
            ("abbrev", "text"),
            ("utc_offset", "interval"),
            ("is_dst", "bool"),
        ],
        "pg_index" => &[
            ("indexrelid", "oid"),
            ("indrelid", "oid"),
            ("indnatts", "int2"),
            ("indnkeyatts", "int2"),
            ("indisunique", "bool"),
            ("indnullsnotdistinct", "bool"),
            ("indisprimary", "bool"),
            ("indisexclusion", "bool"),
            ("indimmediate", "bool"),
            ("indisclustered", "bool"),
            ("indisvalid", "bool"),
            ("indcheckxmin", "bool"),
            ("indisready", "bool"),
            ("indislive", "bool"),
            ("indisreplident", "bool"),
            ("indkey", "int2vector"),
            ("indcollation", "oidvector"),
            ("indclass", "oidvector"),
            ("indoption", "int2vector"),
            ("indexprs", "pg_node_tree"),
            ("indpred", "pg_node_tree"),
            ("indexdef", "text"),
        ],
        "pg_am" => &[
            ("oid", "oid"),
            ("amname", "name"),
            ("amhandler", "regproc"),
            ("amtype", "char"),
        ],
        "pg_constraint" => &[
            ("oid", "oid"),
            ("conname", "name"),
            ("connamespace", "oid"),
            ("contype", "char"),
            ("condeferrable", "bool"),
            ("condeferred", "bool"),
            ("conenforced", "bool"),
            ("convalidated", "bool"),
            ("conrelid", "oid"),
            ("contypid", "oid"),
            ("conindid", "oid"),
            ("conparentid", "oid"),
            ("confrelid", "oid"),
            ("confupdtype", "char"),
            ("confdeltype", "char"),
            ("confmatchtype", "char"),
            ("conislocal", "bool"),
            ("coninhcount", "int2"),
            ("connoinherit", "bool"),
            ("conperiod", "bool"),
            ("conkey", "int2[]"),
            ("confkey", "int2[]"),
            ("conpfeqop", "oid[]"),
            ("conppeqop", "oid[]"),
            ("conffeqop", "oid[]"),
            ("confdelsetcols", "int2[]"),
            ("conexclop", "oid[]"),
            ("conbin", "pg_node_tree"),
        ],
        "pg_locks" => &[
            ("locktype", "text"),
            ("database", "oid"),
            ("relation", "oid"),
            ("page", "int4"),
            ("tuple", "int2"),
            ("virtualxid", "text"),
            ("transactionid", "xid"),
            ("classid", "oid"),
            ("objid", "oid"),
            ("objsubid", "int2"),
            ("virtualtransaction", "text"),
            ("pid", "int4"),
            ("mode", "text"),
            ("granted", "bool"),
            ("fastpath", "bool"),
            ("waitstart", "timestamptz"),
        ],
        "pg_opclass" => &[
            ("oid", "oid"),
            ("opcmethod", "oid"),
            ("opcname", "name"),
            ("opcnamespace", "oid"),
            ("opcowner", "oid"),
            ("opcfamily", "oid"),
            ("opcintype", "oid"),
            ("opcdefault", "bool"),
            ("opckeytype", "oid"),
        ],
        "pg_opfamily" => &[
            ("oid", "oid"),
            ("opfmethod", "oid"),
            ("opfname", "name"),
            ("opfnamespace", "oid"),
            ("opfowner", "oid"),
        ],
        "pg_amop" => &[
            ("oid", "oid"),
            ("amopfamily", "oid"),
            ("amoplefttype", "oid"),
            ("amoprighttype", "oid"),
            ("amopstrategy", "int2"),
            ("amoppurpose", "char"),
            ("amopopr", "oid"),
            ("amopmethod", "oid"),
            ("amopsortfamily", "oid"),
        ],
        "pg_amproc" => &[
            ("oid", "oid"),
            ("amprocfamily", "oid"),
            ("amproclefttype", "oid"),
            ("amprocrighttype", "oid"),
            ("amprocnum", "int2"),
            ("amproc", "regproc"),
        ],
        _ => return None,
    };
    Some(
        columns
            .iter()
            .map(|(name, pg_type)| (name.to_string(), Some(pg_type.to_string())))
            .collect(),
    )
}

pub(crate) fn virtual_rows_with_selection(
    db: &BicDb,
    table: &str,
    alias: &str,
    selection: Option<&Expr>,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let stripped = table.strip_prefix("pg_catalog.").unwrap_or(table);
    if stripped == "information_schema.columns" {
        let schemas =
            string_filter_values_from_selection(selection, alias, stripped, &["table_schema"])?;
        let tables =
            string_filter_values_from_selection(selection, alias, stripped, &["table_name"])?;
        let columns =
            string_filter_values_from_selection(selection, alias, stripped, &["column_name"])?;
        if schemas.is_some() || tables.is_some() || columns.is_some() {
            return information_schema_columns_filtered(
                db,
                schemas.as_ref(),
                tables.as_ref(),
                columns.as_ref(),
            );
        }
    }
    if stripped == "pg_class" {
        let relnames =
            string_filter_values_from_selection(selection, alias, stripped, &["relname"])?;
        let relkinds =
            string_filter_values_from_selection(selection, alias, stripped, &["relkind"])?;
        let mut oids = relation_oid_filters_from_selection_and_pg_class_join(
            db,
            selection,
            alias,
            stripped,
            &["oid"],
        )?;
        if let Some(index_join_oids) =
            pg_class_oid_filters_from_pg_index_join(db, selection, alias)?
        {
            merge_oid_filter(&mut oids, index_join_oids);
        }
        if let Some(relid_join_oids) =
            pg_class_oid_filters_from_joined_relid_filter(db, selection, alias)?
        {
            merge_oid_filter(&mut oids, relid_join_oids);
        }
        if relnames.is_some() || relkinds.is_some() || oids.is_some() {
            return pg_class_rows_filtered(db, relnames.as_ref(), relkinds.as_ref(), oids.as_ref());
        }
    }
    if stripped == "pg_attribute" {
        if let Some(attrelids) = relation_oid_filters_from_selection_and_pg_class_join(
            db,
            selection,
            alias,
            stripped,
            &["attrelid"],
        )? {
            return pg_attribute_rows_for_attrelids(db, &attrelids);
        }
    }
    if stripped == "pg_attrdef" {
        if let Some(adrelids) = relation_oid_filters_from_selection_and_pg_class_join(
            db,
            selection,
            alias,
            stripped,
            &["adrelid"],
        )? {
            return pg_attrdef_rows_for_adrelids(db, &adrelids);
        }
    }
    if stripped == "pg_index" {
        let primary_filter =
            boolean_filter_from_selection(selection, alias, stripped, "indisprimary");
        let indrelids = relation_oid_filters_from_selection_and_pg_class_join(
            db,
            selection,
            alias,
            stripped,
            &["indrelid"],
        )?;
        let indexrelids = relation_oid_filters_from_selection_and_pg_class_join(
            db,
            selection,
            alias,
            stripped,
            &["indexrelid"],
        )?;
        if primary_filter.is_some() || indrelids.is_some() || indexrelids.is_some() {
            return pg_index_rows_filtered(
                db,
                primary_filter,
                indrelids.as_ref(),
                indexrelids.as_ref(),
            );
        }
    }
    if stripped == "pg_indexes" {
        let schema_names =
            string_filter_values_from_selection(selection, alias, stripped, &["schemaname"])?;
        let table_names =
            string_filter_values_from_selection(selection, alias, stripped, &["tablename"])?;
        let index_names =
            string_filter_values_from_selection(selection, alias, stripped, &["indexname"])?;
        if schema_names.is_some() || table_names.is_some() || index_names.is_some() {
            return pg_indexes_rows_filtered(
                db,
                schema_names.as_ref(),
                table_names.as_ref(),
                index_names.as_ref(),
            );
        }
    }
    if stripped == "pg_constraint" {
        let connames =
            string_filter_values_from_selection(selection, alias, stripped, &["conname"])?;
        let contypes =
            string_filter_values_from_selection(selection, alias, stripped, &["contype"])?;
        let conrelids = relation_oid_filters_from_selection_and_pg_class_join(
            db,
            selection,
            alias,
            stripped,
            &["conrelid"],
        )?;
        if connames.is_some() || contypes.is_some() || conrelids.is_some() {
            return pg_constraint_rows_filtered(
                db,
                connames.as_ref(),
                contypes.as_ref(),
                conrelids.as_ref(),
            );
        }
    }
    if stripped == "pg_trigger" {
        let tgnames = string_filter_values_from_selection(selection, alias, stripped, &["tgname"])?;
        let tgrelids = relation_oid_filters_from_selection_and_pg_class_join(
            db,
            selection,
            alias,
            stripped,
            &["tgrelid"],
        )?;
        if tgnames.is_some() || tgrelids.is_some() {
            return pg_trigger_rows_filtered(db, tgnames.as_ref(), tgrelids.as_ref());
        }
    }
    if matches!(stripped, "pg_stat_all_tables" | "pg_stat_user_tables") {
        if let Some(relids) = relation_oid_filters_from_selection_and_pg_class_join(
            db,
            selection,
            alias,
            stripped,
            &["relid"],
        )? {
            return pg_stat_table_rows(db, Some(&relids));
        }
    }
    virtual_rows(db, table)
}

pub(crate) fn relation_oid_filters_from_selection_and_pg_class_join(
    db: &BicDb,
    selection: Option<&Expr>,
    alias: &str,
    table: &str,
    fields: &[&str],
) -> Result<Option<BTreeSet<i64>>> {
    let mut oids = BTreeSet::new();
    if let Some(oid) = relation_oid_filter_from_selection(db, selection, alias, table, fields)? {
        oids.insert(oid);
    }

    let Some(selection) = selection else {
        return Ok((!oids.is_empty()).then_some(oids));
    };
    let table_oids = table_oids(db);
    for term in and_terms(selection) {
        let Some(class_alias) = joined_oid_alias_from_term(term, alias, table, fields) else {
            continue;
        };
        let Some(relnames) = string_filter_values_from_selection(
            Some(selection),
            &class_alias,
            "pg_class",
            &["relname"],
        )?
        else {
            continue;
        };
        for relname in relnames {
            if let Some((_, oid)) = table_oids
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(&relname))
            {
                oids.insert(*oid);
            }
        }
    }

    Ok((!oids.is_empty()).then_some(oids))
}

pub(crate) fn pg_class_oid_filters_from_pg_index_join(
    db: &BicDb,
    selection: Option<&Expr>,
    alias: &str,
) -> Result<Option<BTreeSet<i64>>> {
    let Some(selection) = selection else {
        return Ok(None);
    };
    let mut oids = None;
    for term in and_terms(selection) {
        let Some((pg_index_alias, pg_index_field)) =
            joined_pg_index_alias_from_pg_class_oid_term(term, alias)
        else {
            continue;
        };
        let primary_filter = boolean_filter_from_selection(
            Some(selection),
            &pg_index_alias,
            "pg_index",
            "indisprimary",
        );
        let indrelids = relation_oid_filters_from_selection_and_pg_class_join(
            db,
            Some(selection),
            &pg_index_alias,
            "pg_index",
            &["indrelid"],
        )?;
        let indexrelids = relation_oid_filters_from_selection_and_pg_class_join(
            db,
            Some(selection),
            &pg_index_alias,
            "pg_index",
            &["indexrelid"],
        )?;
        if primary_filter.is_none() && indrelids.is_none() && indexrelids.is_none() {
            continue;
        }

        let rows =
            pg_index_rows_filtered(db, primary_filter, indrelids.as_ref(), indexrelids.as_ref())?;
        let inferred = rows
            .iter()
            .filter_map(|row| sql_value_i64(&virtual_cell(row, &pg_index_field)))
            .collect::<BTreeSet<_>>();
        if !inferred.is_empty() {
            merge_oid_filter(&mut oids, inferred);
        }
    }
    Ok(oids)
}

pub(crate) fn pg_class_oid_filters_from_joined_relid_filter(
    db: &BicDb,
    selection: Option<&Expr>,
    alias: &str,
) -> Result<Option<BTreeSet<i64>>> {
    let Some(selection) = selection else {
        return Ok(None);
    };
    let mut oids = None;
    for term in and_terms(selection) {
        let Some(relid_alias) = joined_relid_alias_from_pg_class_oid_term(term, alias) else {
            continue;
        };
        if let Some(relids) = relation_oid_filters_from_selection_and_pg_class_join(
            db,
            Some(selection),
            &relid_alias,
            &relid_alias,
            &["relid"],
        )? {
            merge_oid_filter(&mut oids, relids);
        }
    }
    Ok(oids)
}

pub(crate) fn merge_oid_filter(existing: &mut Option<BTreeSet<i64>>, inferred: BTreeSet<i64>) {
    if let Some(existing) = existing {
        *existing = existing.intersection(&inferred).copied().collect();
    } else {
        *existing = Some(inferred);
    }
}

pub(crate) fn joined_pg_index_alias_from_pg_class_oid_term(
    expr: &Expr,
    class_alias: &str,
) -> Option<(String, String)> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } if relation_oid_column_matches(left, class_alias, "pg_class", &["oid"]) => {
            qualified_field_alias_and_name(right, &["indexrelid", "indrelid"])
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } if relation_oid_column_matches(right, class_alias, "pg_class", &["oid"]) => {
            qualified_field_alias_and_name(left, &["indexrelid", "indrelid"])
        }
        Expr::Nested(expr) => joined_pg_index_alias_from_pg_class_oid_term(expr, class_alias),
        _ => None,
    }
}

pub(crate) fn joined_relid_alias_from_pg_class_oid_term(
    expr: &Expr,
    class_alias: &str,
) -> Option<String> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } if relation_oid_column_matches(left, class_alias, "pg_class", &["oid"]) => {
            qualified_field_alias_and_name(right, &["relid"]).map(|(alias, _)| alias)
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } if relation_oid_column_matches(right, class_alias, "pg_class", &["oid"]) => {
            qualified_field_alias_and_name(left, &["relid"]).map(|(alias, _)| alias)
        }
        Expr::Nested(expr) => joined_relid_alias_from_pg_class_oid_term(expr, class_alias),
        _ => None,
    }
}

pub(crate) fn qualified_field_alias_and_name(
    expr: &Expr,
    fields: &[&str],
) -> Option<(String, String)> {
    match expr {
        Expr::CompoundIdentifier(idents) if idents.len() >= 2 => {
            let field = idents.last()?;
            if !fields
                .iter()
                .any(|candidate| field.value.eq_ignore_ascii_case(candidate))
            {
                return None;
            }
            idents
                .get(idents.len() - 2)
                .map(|ident| (ident.value.clone(), field.value.clone()))
        }
        Expr::Nested(expr) => qualified_field_alias_and_name(expr, fields),
        _ => None,
    }
}

pub(crate) fn joined_oid_alias_from_term(
    expr: &Expr,
    alias: &str,
    table: &str,
    fields: &[&str],
) -> Option<String> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } if relation_oid_column_matches(left, alias, table, fields) => {
            qualified_field_alias(right, "oid")
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } if relation_oid_column_matches(right, alias, table, fields) => {
            qualified_field_alias(left, "oid")
        }
        Expr::Nested(expr) => joined_oid_alias_from_term(expr, alias, table, fields),
        _ => None,
    }
}

pub(crate) fn qualified_field_alias(expr: &Expr, field: &str) -> Option<String> {
    match expr {
        Expr::CompoundIdentifier(idents) if idents.len() >= 2 => {
            let last = idents.last()?;
            if !last.value.eq_ignore_ascii_case(field) {
                return None;
            }
            idents
                .get(idents.len() - 2)
                .map(|ident| ident.value.clone())
        }
        Expr::Nested(expr) => qualified_field_alias(expr, field),
        _ => None,
    }
}

pub(crate) fn relation_oid_filter_from_selection(
    db: &BicDb,
    selection: Option<&Expr>,
    alias: &str,
    table: &str,
    fields: &[&str],
) -> Result<Option<i64>> {
    let Some(selection) = selection else {
        return Ok(None);
    };
    for term in and_terms(selection) {
        if let Some(oid) = relation_oid_comparison(db, term, alias, table, fields)? {
            return Ok(Some(oid));
        }
    }
    Ok(None)
}

pub(crate) fn string_filter_values_from_selection(
    selection: Option<&Expr>,
    alias: &str,
    table: &str,
    fields: &[&str],
) -> Result<Option<BTreeSet<String>>> {
    let Some(selection) = selection else {
        return Ok(None);
    };
    for term in and_terms(selection) {
        if let Some(values) = string_filter_values_from_term(term, alias, table, fields)? {
            return Ok(Some(values));
        }
    }
    Ok(None)
}

pub(crate) fn string_filter_values_from_term(
    expr: &Expr,
    alias: &str,
    table: &str,
    fields: &[&str],
) -> Result<Option<BTreeSet<String>>> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } if relation_oid_column_matches(left, alias, table, fields) => {
            constant_string_value(right).map(|value| value.map(|value| [value].into()))
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } if relation_oid_column_matches(right, alias, table, fields) => {
            constant_string_value(left).map(|value| value.map(|value| [value].into()))
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } if relation_oid_column_matches(expr, alias, table, fields) => {
            let mut values = BTreeSet::new();
            for item in list {
                let Some(value) = constant_string_value(item)? else {
                    return Ok(None);
                };
                values.insert(value);
            }
            Ok(Some(values))
        }
        Expr::AnyOp {
            left,
            compare_op: BinaryOperator::Eq,
            right,
            ..
        } if relation_oid_column_matches(left, alias, table, fields) => {
            constant_string_array_values(right)
        }
        Expr::Nested(expr) => string_filter_values_from_term(expr, alias, table, fields),
        _ => Ok(None),
    }
}

pub(crate) fn regex_exact_values_from_selection(
    selection: Option<&Expr>,
    alias: &str,
    table: &str,
    fields: &[&str],
) -> Result<Option<BTreeSet<String>>> {
    let Some(selection) = selection else {
        return Ok(None);
    };
    let mut values = None;
    for term in and_terms(selection) {
        if let Some(term_values) = regex_exact_values_from_term(term, alias, table, fields)? {
            values = intersect_optional_string_filters(values, Some(term_values));
        }
    }
    Ok(values)
}

pub(crate) fn regex_exact_values_from_term(
    expr: &Expr,
    alias: &str,
    table: &str,
    fields: &[&str],
) -> Result<Option<BTreeSet<String>>> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::PGRegexMatch,
            right,
        } if relation_oid_column_matches(left, alias, table, fields) => {
            constant_string_value(right).map(|pattern| {
                pattern.and_then(|pattern| exact_values_from_anchored_regex(&pattern))
            })
        }
        Expr::Nested(expr) => regex_exact_values_from_term(expr, alias, table, fields),
        _ => Ok(None),
    }
}

pub(crate) fn exact_values_from_anchored_regex(pattern: &str) -> Option<BTreeSet<String>> {
    let body = pattern.strip_prefix('^')?.strip_suffix('$')?;
    let body = body
        .strip_prefix('(')
        .and_then(|body| body.strip_suffix(')'))
        .unwrap_or(body);
    let mut values = BTreeSet::new();
    for value in body.split('|') {
        if value.is_empty()
            || value
                .chars()
                .any(|ch| !ch.is_ascii_alphanumeric() && ch != '_')
        {
            return None;
        }
        values.insert(value.to_string());
    }
    Some(values)
}

pub(crate) fn intersect_optional_string_filters(
    left: Option<BTreeSet<String>>,
    right: Option<BTreeSet<String>>,
) -> Option<BTreeSet<String>> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.intersection(&right).cloned().collect()),
        (Some(values), None) | (None, Some(values)) => Some(values),
        (None, None) => None,
    }
}

pub(crate) fn column_list_filter_values_from_selection(
    selection: Option<&Expr>,
    alias: &str,
    table: &str,
    fields: &[&str],
) -> Result<Option<BTreeSet<String>>> {
    let Some(selection) = selection else {
        return Ok(None);
    };
    for term in and_terms(selection) {
        if let Some(values) = column_list_filter_values_from_term(term, alias, table, fields)? {
            return Ok(Some(values));
        }
    }
    Ok(None)
}

pub(crate) fn column_list_filter_values_from_term(
    expr: &Expr,
    alias: &str,
    table: &str,
    fields: &[&str],
) -> Result<Option<BTreeSet<String>>> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } if relation_oid_column_matches(left, alias, table, fields) => {
            constant_column_list_value(right).map(|value| value.map(|value| [value].into()))
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } if relation_oid_column_matches(right, alias, table, fields) => {
            constant_column_list_value(left).map(|value| value.map(|value| [value].into()))
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } if relation_oid_column_matches(expr, alias, table, fields) => {
            let mut values = BTreeSet::new();
            for item in list {
                let Some(value) = constant_column_list_value(item)? else {
                    return Ok(None);
                };
                values.insert(value);
            }
            Ok(Some(values))
        }
        Expr::Nested(expr) => column_list_filter_values_from_term(expr, alias, table, fields),
        _ => Ok(None),
    }
}

pub(crate) fn constant_column_list_value(expr: &Expr) -> Result<Option<String>> {
    let value = match eval_constant_expr(expr) {
        Ok(value) => value,
        Err(SqlError::Unsupported(_)) => return Ok(None),
        Err(error) => return Err(error),
    };
    Ok(sql_value_column_list(&value))
}

pub(crate) fn sql_value_column_list(value: &SqlValue) -> Option<String> {
    if let Some(values) = array_like_values(value) {
        let mut columns = Vec::with_capacity(values.len());
        for value in values {
            let column = sql_value_text(&value)?;
            columns.push(column);
        }
        return Some(canonical_column_list(&columns));
    }
    match value {
        SqlValue::String(value) => Some(canonical_column_list_text(value)),
        _ => None,
    }
}

pub(crate) fn constant_string_value(expr: &Expr) -> Result<Option<String>> {
    match eval_constant_expr(expr) {
        Ok(SqlValue::String(value)) => Ok(Some(value)),
        Ok(_) => Ok(None),
        Err(SqlError::Unsupported(_)) => Ok(None),
        Err(error) => Err(error),
    }
}

pub(crate) fn constant_string_array_values(expr: &Expr) -> Result<Option<BTreeSet<String>>> {
    let value = match eval_constant_expr(expr) {
        Ok(value) => value,
        Err(SqlError::Unsupported(_)) => return Ok(None),
        Err(error) => return Err(error),
    };
    let Some(values) = array_like_values(&value) else {
        return Ok(None);
    };
    let mut strings = BTreeSet::new();
    for value in values {
        let Some(value) = sql_value_text(&value) else {
            return Ok(None);
        };
        strings.insert(value);
    }
    Ok(Some(strings))
}

pub(crate) fn relation_oid_comparison(
    db: &BicDb,
    expr: &Expr,
    alias: &str,
    table: &str,
    fields: &[&str],
) -> Result<Option<i64>> {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = expr
    else {
        return Ok(None);
    };
    if relation_oid_column_matches(left, alias, table, fields) {
        return optional_relation_oid_literal_value(db, right);
    }
    if relation_oid_column_matches(right, alias, table, fields) {
        return optional_relation_oid_literal_value(db, left);
    }
    Ok(None)
}

pub(crate) fn relation_oid_column_matches(
    expr: &Expr,
    alias: &str,
    table: &str,
    fields: &[&str],
) -> bool {
    let parts = match expr {
        Expr::Identifier(ident) => vec![ident.value.as_str()],
        Expr::CompoundIdentifier(idents) => {
            idents.iter().map(|ident| ident.value.as_str()).collect()
        }
        Expr::Nested(expr) => return relation_oid_column_matches(expr, alias, table, fields),
        _ => return false,
    };
    let Some(field) = parts.last() else {
        return false;
    };
    if !fields
        .iter()
        .any(|candidate| field.eq_ignore_ascii_case(candidate))
    {
        return false;
    }
    if parts.len() == 1 {
        return true;
    }
    let qualifier = parts[parts.len() - 2];
    qualifier.eq_ignore_ascii_case(alias) || qualifier.eq_ignore_ascii_case(table)
}

pub(crate) fn relation_oid_literal_value(db: &BicDb, expr: &Expr) -> Result<Option<i64>> {
    let value = match expr {
        Expr::Cast {
            expr, data_type, ..
        } if matches!(data_type, DataType::Regclass) => {
            cast_value_with_db(db, eval_constant_expr(expr)?, data_type)?
        }
        Expr::Function(function) => {
            let name = object_name(&function.name)?.to_ascii_lowercase();
            if !matches!(name.as_str(), "to_regclass" | "pg_catalog.to_regclass") {
                return Err(SqlError::Unsupported(format!(
                    "function {} is not supported in relation oid predicates",
                    function.name
                )));
            }
            let args = function_args(function);
            let [arg] = args.as_slice() else {
                return Err(SqlError::InvalidSql(format!(
                    "{} expects exactly one argument",
                    function.name
                )));
            };
            let value = eval_constant_expr(arg)?;
            let SqlValue::String(name) = value else {
                return Ok(None);
            };
            return Ok(resolve_regclass_oid(db, &name));
        }
        Expr::Nested(expr) => return relation_oid_literal_value(db, expr),
        _ => eval_constant_expr(expr)?,
    };
    Ok(sql_value_i64(&value))
}

pub(crate) fn optional_relation_oid_literal_value(db: &BicDb, expr: &Expr) -> Result<Option<i64>> {
    match relation_oid_literal_value(db, expr) {
        Ok(value) => Ok(value),
        Err(SqlError::Unsupported(_)) => Ok(None),
        Err(error) => Err(error),
    }
}

pub(crate) fn boolean_filter_from_selection(
    selection: Option<&Expr>,
    alias: &str,
    table: &str,
    field: &str,
) -> Option<bool> {
    let selection = selection?;
    and_terms(selection)
        .into_iter()
        .find_map(|term| boolean_filter_from_term(term, alias, table, field))
}

pub(crate) fn boolean_filter_from_term(
    expr: &Expr,
    alias: &str,
    table: &str,
    field: &str,
) -> Option<bool> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } if relation_oid_column_matches(left, alias, table, &[field]) => eval_constant_expr(right)
            .ok()
            .and_then(|value| sql_value_bool(&value)),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } if relation_oid_column_matches(right, alias, table, &[field]) => eval_constant_expr(left)
            .ok()
            .and_then(|value| sql_value_bool(&value)),
        Expr::Identifier(_) | Expr::CompoundIdentifier(_)
            if relation_oid_column_matches(expr, alias, table, &[field]) =>
        {
            Some(true)
        }
        Expr::Nested(expr) => boolean_filter_from_term(expr, alias, table, field),
        _ => None,
    }
}

pub(crate) fn information_schema_tables(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let mut rows = Vec::new();
    for collection in user_collection_names(db) {
        rows.push(virtual_row([
            ("table_catalog", SqlValue::String("bicdb".to_string())),
            ("table_schema", SqlValue::String("public".to_string())),
            ("table_name", SqlValue::String(collection)),
            ("table_type", SqlValue::String("BASE TABLE".to_string())),
        ]));
    }
    for view in list_views(db)? {
        rows.push(virtual_row([
            ("table_catalog", SqlValue::String("bicdb".to_string())),
            ("table_schema", SqlValue::String("public".to_string())),
            ("table_name", SqlValue::String(view.name)),
            ("table_type", SqlValue::String("VIEW".to_string())),
        ]));
    }
    for table in graph_virtual_table_names() {
        rows.push(virtual_row([
            ("table_catalog", SqlValue::String("bicdb".to_string())),
            ("table_schema", SqlValue::String("public".to_string())),
            ("table_name", SqlValue::String(table.to_string())),
            ("table_type", SqlValue::String("VIEW".to_string())),
        ]));
    }
    rows.sort_by(|left, right| {
        virtual_cell(left, "table_name")
            .to_cell()
            .cmp(&virtual_cell(right, "table_name").to_cell())
    });
    Ok(rows)
}

pub(crate) fn information_schema_columns(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    information_schema_columns_filtered(db, None, None, None)
}

fn information_schema_declared_type(db: &BicDb, pg_type: &str) -> Result<(String, String, String)> {
    let normalized = pg_type.trim().trim_matches('"');
    let (base, array) = normalized
        .strip_suffix("[]")
        .map(|base| (base, true))
        .unwrap_or((normalized, false));
    let parts = base.split('.').collect::<Vec<_>>();
    let (schema_name, type_name) = match parts.as_slice() {
        [type_name] => ("public", *type_name),
        [schema_name, type_name] => (*schema_name, *type_name),
        _ => ("public", base),
    };
    if let Some(user_type) = load_user_type(db, schema_name, type_name)? {
        return Ok(if array {
            (
                "ARRAY".to_string(),
                user_type.schema_name,
                format!("_{}", user_type.name),
            )
        } else {
            (
                "USER-DEFINED".to_string(),
                user_type.schema_name,
                user_type.name,
            )
        });
    }
    let canonical = pg_type_regtype_name(normalized).unwrap_or_else(|| normalized.to_string());
    if let Some(spec) = i32::try_from(pg_type_oid(&canonical))
        .ok()
        .and_then(pg_array_element_spec_by_oid)
    {
        return Ok((
            "ARRAY".to_string(),
            "pg_catalog".to_string(),
            format!("_{}", spec.name),
        ));
    }
    Ok((
        information_schema_builtin_data_type(&canonical),
        "pg_catalog".to_string(),
        canonical,
    ))
}

fn information_schema_domain_base(
    base_type: &str,
    base_user_type: Option<&UserTypeColumnSchema>,
) -> (String, String, String) {
    if let Some(base_user_type) = base_user_type {
        return (
            base_user_type.information_schema_scalar_data_type(),
            base_user_type.information_schema_scalar_udt_schema(),
            base_user_type.information_schema_scalar_udt_name(),
        );
    }
    (
        information_schema_builtin_data_type(base_type),
        "pg_catalog".to_string(),
        base_type.to_string(),
    )
}

fn information_schema_numeric_metadata(
    type_modifier: Option<&PgTypeModifier>,
) -> (SqlValue, SqlValue, SqlValue) {
    match type_modifier {
        Some(PgTypeModifier::Numeric { precision, scale }) => (
            SqlValue::Int(i64::from(*precision)),
            SqlValue::Int(10),
            SqlValue::Int(i64::from(*scale)),
        ),
        _ => (SqlValue::Null, SqlValue::Null, SqlValue::Null),
    }
}

pub(crate) fn information_schema_domains(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let mut rows = Vec::new();
    for user_type in list_user_types(db)? {
        let UserTypeKind::Domain {
            base_type,
            base_user_type,
            type_modifier,
            collation,
            default_expr,
            ..
        } = &user_type.kind
        else {
            continue;
        };
        let (data_type, udt_schema, udt_name) =
            information_schema_domain_base(base_type, base_user_type.as_deref());
        let (numeric_precision, numeric_precision_radix, numeric_scale) =
            information_schema_numeric_metadata(type_modifier.as_ref());
        rows.push(virtual_row([
            ("domain_catalog", SqlValue::String("bicdb".to_string())),
            ("domain_schema", SqlValue::String(user_type.schema_name)),
            ("domain_name", SqlValue::String(user_type.name)),
            ("data_type", SqlValue::String(data_type)),
            ("character_maximum_length", SqlValue::Null),
            ("character_octet_length", SqlValue::Null),
            ("character_set_catalog", SqlValue::Null),
            ("character_set_schema", SqlValue::Null),
            ("character_set_name", SqlValue::Null),
            (
                "collation_catalog",
                collation
                    .as_ref()
                    .map(|_| SqlValue::String("bicdb".to_string()))
                    .unwrap_or(SqlValue::Null),
            ),
            (
                "collation_schema",
                collation
                    .as_ref()
                    .map(|_| SqlValue::String("pg_catalog".to_string()))
                    .unwrap_or(SqlValue::Null),
            ),
            (
                "collation_name",
                collation
                    .clone()
                    .map(SqlValue::String)
                    .unwrap_or(SqlValue::Null),
            ),
            ("numeric_precision", numeric_precision),
            ("numeric_precision_radix", numeric_precision_radix),
            ("numeric_scale", numeric_scale),
            ("datetime_precision", SqlValue::Null),
            ("interval_type", SqlValue::Null),
            ("interval_precision", SqlValue::Null),
            (
                "domain_default",
                default_expr
                    .clone()
                    .map(SqlValue::String)
                    .unwrap_or(SqlValue::Null),
            ),
            ("udt_catalog", SqlValue::String("bicdb".to_string())),
            ("udt_schema", SqlValue::String(udt_schema)),
            ("udt_name", SqlValue::String(udt_name)),
            ("scope_catalog", SqlValue::Null),
            ("scope_schema", SqlValue::Null),
            ("scope_name", SqlValue::Null),
            ("maximum_cardinality", SqlValue::Null),
            ("dtd_identifier", SqlValue::String("1".to_string())),
        ]));
    }
    Ok(rows)
}

pub(crate) fn information_schema_element_types(
    db: &BicDb,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let mut rows = Vec::new();
    for schema in list_schemas(db)? {
        for (index, column) in schema
            .columns
            .iter()
            .filter(|column| !column.hidden)
            .enumerate()
        {
            if !column.type_is_array() {
                continue;
            }
            let (data_type, udt_schema, udt_name) = if let Some(user_type) = &column.user_type {
                (
                    "USER-DEFINED".to_string(),
                    user_type.schema_name.clone(),
                    user_type.name.clone(),
                )
            } else if let Some(spec) = i32::try_from(column.type_oid())
                .ok()
                .and_then(pg_array_element_spec_by_oid)
            {
                (
                    information_schema_builtin_data_type(spec.name),
                    "pg_catalog".to_string(),
                    spec.name.to_string(),
                )
            } else {
                continue;
            };
            let identifier = (index + 1).to_string();
            rows.push(virtual_row([
                ("object_catalog", SqlValue::String("bicdb".to_string())),
                (
                    "object_schema",
                    SqlValue::String(schema.schema_name.clone()),
                ),
                ("object_name", SqlValue::String(schema.name.clone())),
                ("object_type", SqlValue::String("TABLE".to_string())),
                (
                    "collection_type_identifier",
                    SqlValue::String(identifier.clone()),
                ),
                ("data_type", SqlValue::String(data_type)),
                ("udt_catalog", SqlValue::String("bicdb".to_string())),
                ("udt_schema", SqlValue::String(udt_schema)),
                ("udt_name", SqlValue::String(udt_name)),
                ("dtd_identifier", SqlValue::String(format!("a{identifier}"))),
            ]));
        }
    }
    Ok(rows)
}

pub(crate) fn information_schema_user_defined_types(
    db: &BicDb,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    Ok(list_user_types(db)?
        .into_iter()
        .filter(|user_type| matches!(user_type.kind, UserTypeKind::Composite { .. }))
        .map(|user_type| {
            virtual_row([
                (
                    "user_defined_type_catalog",
                    SqlValue::String("bicdb".to_string()),
                ),
                (
                    "user_defined_type_schema",
                    SqlValue::String(user_type.schema_name),
                ),
                ("user_defined_type_name", SqlValue::String(user_type.name)),
                (
                    "user_defined_type_category",
                    SqlValue::String("STRUCTURED".to_string()),
                ),
                ("is_instantiable", SqlValue::String("YES".to_string())),
                ("is_final", SqlValue::Null),
                ("ordering_form", SqlValue::Null),
                ("ordering_category", SqlValue::Null),
                ("ordering_routine_catalog", SqlValue::Null),
                ("ordering_routine_schema", SqlValue::Null),
                ("ordering_routine_name", SqlValue::Null),
                ("reference_type", SqlValue::Null),
                ("data_type", SqlValue::Null),
                ("character_maximum_length", SqlValue::Null),
                ("character_octet_length", SqlValue::Null),
                ("numeric_precision", SqlValue::Null),
                ("numeric_precision_radix", SqlValue::Null),
                ("numeric_scale", SqlValue::Null),
                ("datetime_precision", SqlValue::Null),
                ("interval_type", SqlValue::Null),
                ("interval_precision", SqlValue::Null),
                ("source_dtd_identifier", SqlValue::Null),
                ("ref_dtd_identifier", SqlValue::Null),
            ])
        })
        .collect())
}

fn information_schema_routine_identity(routine: &RoutineSchema) -> (String, String, String) {
    let (schema_name, routine_name) = routine
        .name
        .rsplit_once('.')
        .map(|(schema, name)| (schema.to_string(), name.to_string()))
        .unwrap_or_else(|| ("public".to_string(), routine.name.clone()));
    let specific_name = format!("{}_{}", routine_name, hash_routine_args(&routine.args));
    (schema_name, routine_name, specific_name)
}

pub(crate) fn information_schema_routines(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let mut rows = Vec::new();
    for routine in list_routines(db)? {
        let (schema_name, routine_name, specific_name) =
            information_schema_routine_identity(&routine);
        let procedure = matches!(routine.kind, RoutineKind::Procedure);
        let (data_type, udt_schema, udt_name) = if procedure {
            (SqlValue::Null, SqlValue::Null, SqlValue::Null)
        } else {
            let (data_type, udt_schema, udt_name) =
                information_schema_declared_type(db, &routine.return_type)?;
            (
                SqlValue::String(data_type),
                SqlValue::String(udt_schema),
                SqlValue::String(udt_name),
            )
        };
        rows.push(virtual_row([
            ("specific_catalog", SqlValue::String("bicdb".to_string())),
            ("specific_schema", SqlValue::String(schema_name.clone())),
            ("specific_name", SqlValue::String(specific_name)),
            ("routine_catalog", SqlValue::String("bicdb".to_string())),
            ("routine_schema", SqlValue::String(schema_name)),
            ("routine_name", SqlValue::String(routine_name)),
            (
                "routine_type",
                SqlValue::String(if procedure { "PROCEDURE" } else { "FUNCTION" }.to_string()),
            ),
            ("data_type", data_type),
            (
                "type_udt_catalog",
                if procedure {
                    SqlValue::Null
                } else {
                    SqlValue::String("bicdb".to_string())
                },
            ),
            ("type_udt_schema", udt_schema),
            ("type_udt_name", udt_name),
            ("routine_body", SqlValue::String("EXTERNAL".to_string())),
            ("routine_definition", SqlValue::String(routine.definition)),
            (
                "external_language",
                SqlValue::String(routine.language.to_ascii_uppercase()),
            ),
            ("parameter_style", SqlValue::String("GENERAL".to_string())),
            ("is_deterministic", SqlValue::String("NO".to_string())),
            ("sql_data_access", SqlValue::String("MODIFIES".to_string())),
            (
                "security_type",
                SqlValue::String(
                    if routine.security_definer {
                        "DEFINER"
                    } else {
                        "INVOKER"
                    }
                    .to_string(),
                ),
            ),
            ("is_null_call", SqlValue::Null),
            ("sql_path", SqlValue::Null),
            ("schema_level_routine", SqlValue::String("YES".to_string())),
            ("max_dynamic_result_sets", SqlValue::Int(0)),
            ("is_user_defined_cast", SqlValue::String("NO".to_string())),
            (
                "is_implicitly_invocable",
                SqlValue::String("NO".to_string()),
            ),
            (
                "dtd_identifier",
                if procedure {
                    SqlValue::Null
                } else {
                    SqlValue::String("0".to_string())
                },
            ),
        ]));
    }
    Ok(rows)
}

pub(crate) fn information_schema_parameters(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let mut rows = Vec::new();
    for routine in list_routines(db)? {
        let (schema_name, _, specific_name) = information_schema_routine_identity(&routine);
        let params = parse_routine_params(&routine.args)?;
        for (index, (param, arg_type)) in params.iter().zip(&routine.arg_types).enumerate() {
            let (data_type, udt_schema, udt_name) =
                information_schema_declared_type(db, &arg_type.pg_type)?;
            let parameter_mode = match param.mode {
                RoutineArgMode::In => "IN",
                RoutineArgMode::Out => "OUT",
                RoutineArgMode::InOut => "INOUT",
            };
            let identifier = (index + 1).to_string();
            rows.push(virtual_row([
                ("specific_catalog", SqlValue::String("bicdb".to_string())),
                ("specific_schema", SqlValue::String(schema_name.clone())),
                ("specific_name", SqlValue::String(specific_name.clone())),
                ("ordinal_position", SqlValue::Int((index + 1) as i64)),
                (
                    "parameter_mode",
                    SqlValue::String(parameter_mode.to_string()),
                ),
                ("is_result", SqlValue::String("NO".to_string())),
                ("as_locator", SqlValue::String("NO".to_string())),
                (
                    "parameter_name",
                    param
                        .name
                        .clone()
                        .map(SqlValue::String)
                        .unwrap_or(SqlValue::Null),
                ),
                ("data_type", SqlValue::String(data_type)),
                ("udt_catalog", SqlValue::String("bicdb".to_string())),
                ("udt_schema", SqlValue::String(udt_schema)),
                ("udt_name", SqlValue::String(udt_name)),
                ("parameter_default", SqlValue::Null),
                ("dtd_identifier", SqlValue::String(identifier)),
            ]));
        }
    }
    Ok(rows)
}

pub(crate) fn information_schema_columns_filtered(
    db: &BicDb,
    schema_names: Option<&BTreeSet<String>>,
    table_names: Option<&BTreeSet<String>>,
    column_names: Option<&BTreeSet<String>>,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let schemas = list_schemas(db)?;
    let mut rows = Vec::new();
    for collection in user_collection_names(db) {
        if !catalog_name_matches(table_names, &collection)
            || !catalog_name_matches(schema_names, "public")
        {
            continue;
        }
        let schema = schemas
            .iter()
            .find(|schema| schema.name.eq_ignore_ascii_case(&collection));
        let columns = schema
            .map(|schema| {
                schema
                    .columns
                    .iter()
                    .filter(|column| !column.hidden)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(default_record_columns);
        for (idx, column) in columns.into_iter().enumerate() {
            if !catalog_name_matches(column_names, &column.name) {
                continue;
            }
            let identity_sequence = if column.identity.is_some() {
                column
                    .default_sequence
                    .as_deref()
                    .map(|name| load_sequence(db, name))
                    .transpose()?
                    .flatten()
            } else {
                None
            };
            let data_type = column.information_schema_data_type();
            let udt_name = column.information_schema_udt_name();
            let udt_schema = column.information_schema_udt_schema();
            let domain = column.information_schema_domain();
            let character_maximum_length = column
                .character_maximum_length()
                .map(SqlValue::Int)
                .unwrap_or(SqlValue::Null);
            let character_octet_length = column
                .character_octet_length()
                .map(SqlValue::Int)
                .unwrap_or(SqlValue::Null);
            let explicit_collation = column.collation.clone();
            rows.push(virtual_row([
                ("table_catalog", SqlValue::String("bicdb".to_string())),
                ("table_schema", SqlValue::String("public".to_string())),
                ("table_name", SqlValue::String(collection.clone())),
                ("column_name", SqlValue::String(column.name)),
                ("ordinal_position", SqlValue::Int((idx + 1) as i64)),
                ("data_type", SqlValue::String(data_type)),
                (
                    "domain_catalog",
                    domain
                        .as_ref()
                        .map(|_| SqlValue::String("bicdb".to_string()))
                        .unwrap_or(SqlValue::Null),
                ),
                (
                    "domain_schema",
                    domain
                        .as_ref()
                        .map(|(schema, _)| SqlValue::String(schema.clone()))
                        .unwrap_or(SqlValue::Null),
                ),
                (
                    "domain_name",
                    domain
                        .as_ref()
                        .map(|(_, name)| SqlValue::String(name.clone()))
                        .unwrap_or(SqlValue::Null),
                ),
                ("udt_catalog", SqlValue::String("bicdb".to_string())),
                ("udt_schema", SqlValue::String(udt_schema)),
                ("udt_name", SqlValue::String(udt_name)),
                ("dtd_identifier", SqlValue::String((idx + 1).to_string())),
                ("is_self_referencing", SqlValue::String("NO".to_string())),
                (
                    "is_generated",
                    SqlValue::String(
                        if column.generated_expr.is_some() {
                            "ALWAYS"
                        } else {
                            "NEVER"
                        }
                        .to_string(),
                    ),
                ),
                (
                    "generation_expression",
                    column
                        .generated_expr
                        .clone()
                        .map(SqlValue::String)
                        .unwrap_or(SqlValue::Null),
                ),
                ("is_updatable", SqlValue::String("YES".to_string())),
                ("character_maximum_length", character_maximum_length),
                ("character_octet_length", character_octet_length),
                (
                    "collation_catalog",
                    explicit_collation
                        .as_ref()
                        .map(|_| SqlValue::String("bicdb".to_string()))
                        .unwrap_or(SqlValue::Null),
                ),
                (
                    "collation_schema",
                    explicit_collation
                        .as_ref()
                        .map(|_| SqlValue::String("pg_catalog".to_string()))
                        .unwrap_or(SqlValue::Null),
                ),
                (
                    "collation_name",
                    explicit_collation
                        .map(SqlValue::String)
                        .unwrap_or(SqlValue::Null),
                ),
                (
                    "is_nullable",
                    SqlValue::String(if column.nullable { "YES" } else { "NO" }.to_string()),
                ),
                (
                    "column_default",
                    if column.identity.is_none() && column.generated_expr.is_none() {
                        column
                            .default_sequence
                            .as_ref()
                            .map(|sequence| {
                                SqlValue::String(format!("nextval('{sequence}'::regclass)"))
                            })
                            .unwrap_or(SqlValue::Null)
                    } else {
                        SqlValue::Null
                    },
                ),
                (
                    "is_identity",
                    SqlValue::String(
                        if column.identity.is_some() {
                            "YES"
                        } else {
                            "NO"
                        }
                        .to_string(),
                    ),
                ),
                (
                    "identity_generation",
                    column
                        .identity
                        .as_ref()
                        .map(|kind| {
                            if kind == "a" {
                                SqlValue::String("ALWAYS".to_string())
                            } else {
                                SqlValue::String("BY DEFAULT".to_string())
                            }
                        })
                        .unwrap_or(SqlValue::Null),
                ),
                (
                    "identity_start",
                    identity_sequence
                        .as_ref()
                        .map(|sequence| SqlValue::String(sequence.start_value.to_string()))
                        .unwrap_or(SqlValue::Null),
                ),
                (
                    "identity_increment",
                    identity_sequence
                        .as_ref()
                        .map(|sequence| SqlValue::String(sequence.increment_by.to_string()))
                        .unwrap_or(SqlValue::Null),
                ),
                (
                    "identity_maximum",
                    identity_sequence
                        .as_ref()
                        .map(|sequence| SqlValue::String(sequence.max_value.to_string()))
                        .unwrap_or(SqlValue::Null),
                ),
                (
                    "identity_minimum",
                    identity_sequence
                        .as_ref()
                        .map(|sequence| SqlValue::String(sequence.min_value.to_string()))
                        .unwrap_or(SqlValue::Null),
                ),
                (
                    "identity_cycle",
                    SqlValue::String(
                        identity_sequence
                            .as_ref()
                            .is_some_and(|sequence| sequence.cycle)
                            .then_some("YES")
                            .unwrap_or("NO")
                            .to_string(),
                    ),
                ),
            ]));
        }
    }
    for view in list_views(db)? {
        if !catalog_name_matches(table_names, &view.name)
            || !catalog_name_matches(schema_names, "public")
        {
            continue;
        }
        for (idx, column) in view.columns.into_iter().enumerate() {
            if !catalog_name_matches(column_names, &column.name) {
                continue;
            }
            let data_type = column.information_schema_data_type();
            let udt_name = column.information_schema_udt_name();
            let udt_schema = column.information_schema_udt_schema();
            let domain = column.information_schema_domain();
            let character_maximum_length = column
                .character_maximum_length()
                .map(SqlValue::Int)
                .unwrap_or(SqlValue::Null);
            let character_octet_length = column
                .character_octet_length()
                .map(SqlValue::Int)
                .unwrap_or(SqlValue::Null);
            rows.push(virtual_row([
                ("table_catalog", SqlValue::String("bicdb".to_string())),
                ("table_schema", SqlValue::String("public".to_string())),
                ("table_name", SqlValue::String(view.name.clone())),
                ("column_name", SqlValue::String(column.name)),
                ("ordinal_position", SqlValue::Int((idx + 1) as i64)),
                ("data_type", SqlValue::String(data_type)),
                (
                    "domain_catalog",
                    domain
                        .as_ref()
                        .map(|_| SqlValue::String("bicdb".to_string()))
                        .unwrap_or(SqlValue::Null),
                ),
                (
                    "domain_schema",
                    domain
                        .as_ref()
                        .map(|(schema, _)| SqlValue::String(schema.clone()))
                        .unwrap_or(SqlValue::Null),
                ),
                (
                    "domain_name",
                    domain
                        .as_ref()
                        .map(|(_, name)| SqlValue::String(name.clone()))
                        .unwrap_or(SqlValue::Null),
                ),
                ("udt_catalog", SqlValue::String("bicdb".to_string())),
                ("udt_schema", SqlValue::String(udt_schema)),
                ("udt_name", SqlValue::String(udt_name)),
                ("dtd_identifier", SqlValue::String((idx + 1).to_string())),
                ("is_self_referencing", SqlValue::String("NO".to_string())),
                ("is_generated", SqlValue::String("NEVER".to_string())),
                ("generation_expression", SqlValue::Null),
                ("is_updatable", SqlValue::String("YES".to_string())),
                ("character_maximum_length", character_maximum_length),
                ("character_octet_length", character_octet_length),
                ("is_nullable", SqlValue::String("YES".to_string())),
                ("column_default", SqlValue::Null),
                ("is_identity", SqlValue::String("NO".to_string())),
                ("identity_generation", SqlValue::Null),
                ("identity_start", SqlValue::Null),
                ("identity_increment", SqlValue::Null),
                ("identity_maximum", SqlValue::Null),
                ("identity_minimum", SqlValue::Null),
                ("identity_cycle", SqlValue::String("NO".to_string())),
            ]));
        }
    }
    for table in graph_virtual_table_names() {
        if !catalog_name_matches(table_names, table)
            || !catalog_name_matches(schema_names, "public")
        {
            continue;
        }
        for (idx, column) in graph_virtual_table_columns(table).into_iter().enumerate() {
            if !catalog_name_matches(column_names, &column.name) {
                continue;
            }
            let data_type = column.information_schema_data_type();
            let udt_name = column.information_schema_udt_name();
            let udt_schema = column.information_schema_udt_schema();
            let character_maximum_length = column
                .character_maximum_length()
                .map(SqlValue::Int)
                .unwrap_or(SqlValue::Null);
            let character_octet_length = column
                .character_octet_length()
                .map(SqlValue::Int)
                .unwrap_or(SqlValue::Null);
            rows.push(virtual_row([
                ("table_catalog", SqlValue::String("bicdb".to_string())),
                ("table_schema", SqlValue::String("public".to_string())),
                ("table_name", SqlValue::String(table.to_string())),
                ("column_name", SqlValue::String(column.name)),
                ("ordinal_position", SqlValue::Int((idx + 1) as i64)),
                ("data_type", SqlValue::String(data_type)),
                ("domain_catalog", SqlValue::Null),
                ("domain_schema", SqlValue::Null),
                ("domain_name", SqlValue::Null),
                ("udt_catalog", SqlValue::String("bicdb".to_string())),
                ("udt_schema", SqlValue::String(udt_schema)),
                ("udt_name", SqlValue::String(udt_name)),
                ("dtd_identifier", SqlValue::String((idx + 1).to_string())),
                ("is_self_referencing", SqlValue::String("NO".to_string())),
                ("is_generated", SqlValue::String("NEVER".to_string())),
                ("generation_expression", SqlValue::Null),
                ("is_updatable", SqlValue::String("YES".to_string())),
                ("character_maximum_length", character_maximum_length),
                ("character_octet_length", character_octet_length),
                ("is_nullable", SqlValue::String("YES".to_string())),
            ]));
        }
    }
    Ok(rows)
}

pub(crate) fn information_schema_table_constraints(
    db: &BicDb,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let schemas = list_schemas(db)?;
    let mut rows = Vec::new();
    for schema in schemas {
        for column in &schema.columns {
            if column.hidden {
                continue;
            }
            if !column.nullable || column.primary_key {
                rows.push(table_constraint_row(
                    &schema.name,
                    &not_null_constraint_name(&schema.name, &column.name),
                    "CHECK",
                ));
            }
        }
        if !primary_key_columns_for_schema(&schema).is_empty() {
            rows.push(table_constraint_row(
                &schema.name,
                &schema.primary_key_constraint_name(),
                "PRIMARY KEY",
            ));
        }
        for constraint in &schema.constraints {
            match constraint {
                ConstraintSchema::Unique { name, columns, .. } => {
                    if unique_constraint_is_primary_key(&schema, name, columns) {
                        continue;
                    }
                    rows.push(table_constraint_row(&schema.name, name, "UNIQUE"));
                }
                ConstraintSchema::Check { name, .. } => {
                    rows.push(table_constraint_row(&schema.name, name, "CHECK"));
                }
                ConstraintSchema::ForeignKey { name, .. } => {
                    rows.push(table_constraint_row(&schema.name, name, "FOREIGN KEY"));
                }
                ConstraintSchema::Exclusion { name, .. } => {
                    rows.push(table_constraint_row(&schema.name, name, "EXCLUDE"));
                }
            }
        }
    }
    Ok(rows)
}

pub(crate) fn information_schema_key_column_usage(
    db: &BicDb,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let schemas = list_schemas(db)?;
    let mut rows = Vec::new();
    for schema in schemas {
        for (idx, column) in primary_key_columns_for_schema(&schema).iter().enumerate() {
            rows.push(key_column_usage_row(
                &schema.name,
                &schema.primary_key_constraint_name(),
                column,
                idx as i64 + 1,
            ));
        }
        for constraint in &schema.constraints {
            match constraint {
                ConstraintSchema::Unique { name, columns, .. } => {
                    if unique_constraint_is_primary_key(&schema, name, columns) {
                        continue;
                    }
                    for (idx, column) in columns.iter().enumerate() {
                        rows.push(key_column_usage_row(
                            &schema.name,
                            name,
                            column,
                            idx as i64 + 1,
                        ));
                    }
                }
                ConstraintSchema::ForeignKey { name, columns, .. } => {
                    for (idx, column) in columns.iter().enumerate() {
                        rows.push(key_column_usage_row(
                            &schema.name,
                            name,
                            column,
                            idx as i64 + 1,
                        ));
                    }
                }
                ConstraintSchema::Check { .. } => {}
                ConstraintSchema::Exclusion { .. } => {}
            }
        }
    }
    Ok(rows)
}

pub(crate) fn information_schema_constraint_column_usage(
    db: &BicDb,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let schemas = list_schemas(db)?;
    let mut rows = Vec::new();
    for schema in schemas {
        for column in &schema.columns {
            if column.hidden {
                continue;
            }
            if !column.nullable || column.primary_key {
                rows.push(constraint_column_usage_row(
                    &schema,
                    &not_null_constraint_name(&schema.name, &column.name),
                    &column.name,
                ));
            }
        }
        if let Some(primary_key) = schema.primary_key_column().filter(|column| !column.hidden) {
            rows.push(constraint_column_usage_row(
                &schema,
                &schema.primary_key_constraint_name(),
                &primary_key.name,
            ));
        }
        for constraint in &schema.constraints {
            match constraint {
                ConstraintSchema::Unique { name, columns, .. }
                | ConstraintSchema::ForeignKey { name, columns, .. } => {
                    for column in columns {
                        rows.push(constraint_column_usage_row(&schema, name, column));
                    }
                }
                ConstraintSchema::Check {
                    name, expression, ..
                } => {
                    for column in check_constraint_referenced_columns(&schema, expression) {
                        rows.push(constraint_column_usage_row(&schema, name, &column));
                    }
                }
                ConstraintSchema::Exclusion { .. } => {}
            }
        }
    }
    Ok(rows)
}
