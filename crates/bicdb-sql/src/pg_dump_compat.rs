//! pg_dump / ORM introspection compatibility: pg_class join row synthesis, column/constraint/proc inventory queries, namespace summaries, and catalog subquery pattern matching.
//!
//! Extracted verbatim from `lib.rs` (phase-1 mechanical module split).
//! Moved items were bumped to `pub(crate)` so existing call sites keep
//! resolving; `lib.rs` re-exports this module via `pub use pg_dump_compat::*;`.

// Glue import: bring every crate-root item (including the root's private
// imports and the other split modules' re-exports) into scope so the moved
// code compiles unchanged.
use crate::*;

pub(crate) fn pg_dump_cast_inventory_columns() -> Vec<String> {
    [
        "tableoid",
        "oid",
        "castsource",
        "casttarget",
        "castfunc",
        "castcontext",
        "castmethod",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub(crate) fn pg_dump_cast_inventory_projection_matches(projection: &[SelectItem]) -> bool {
    projection
        .iter()
        .filter_map(pg_dump_proc_inventory_projection_name)
        .map(|name| name.to_ascii_lowercase())
        .collect::<Vec<_>>()
        == pg_dump_cast_inventory_columns()
}

pub(crate) fn pg_dump_cast_inventory_selection_matches(
    selection: Option<&Expr>,
    cast_alias: &str,
) -> bool {
    let Some(Expr::Exists {
        subquery,
        negated: true,
    }) = selection.map(unwrap_nested_expr)
    else {
        return false;
    };
    let Some((range_select, range_alias)) = select_single_table_alias(subquery, "pg_range") else {
        return false;
    };
    let Some(range_selection) = range_select.selection.as_ref() else {
        return false;
    };
    let terms = and_terms(range_selection);
    terms.len() == 2
        && terms.iter().any(|term| {
            expr_has_column_equality(
                term,
                cast_alias,
                "pg_cast",
                "castsource",
                &range_alias,
                "pg_range",
                "rngtypid",
            )
        })
        && terms.iter().any(|term| {
            expr_has_column_equality(
                term,
                cast_alias,
                "pg_cast",
                "casttarget",
                &range_alias,
                "pg_range",
                "rngmultitypid",
            )
        })
}

pub(crate) fn pg_dump_cast_inventory_rows(db: &BicDb) -> Result<Vec<Vec<SqlValue>>> {
    let automatic_range_casts = pg_range_rows(db)?
        .into_iter()
        .filter_map(|row| {
            Some((
                sql_value_i64(&virtual_cell(&row, "rngtypid"))?,
                sql_value_i64(&virtual_cell(&row, "rngmultitypid"))?,
            ))
        })
        .collect::<BTreeSet<_>>();
    let mut rows = pg_cast_rows(db)?
        .into_iter()
        .filter(|row| {
            let pair = (
                sql_value_i64(&virtual_cell(row, "castsource")).unwrap_or_default(),
                sql_value_i64(&virtual_cell(row, "casttarget")).unwrap_or_default(),
            );
            !automatic_range_casts.contains(&pair)
        })
        .map(|row| {
            vec![
                SqlValue::Int(PG_CAST_CATALOG_OID),
                virtual_cell(&row, "oid"),
                virtual_cell(&row, "castsource"),
                virtual_cell(&row, "casttarget"),
                virtual_cell(&row, "castfunc"),
                virtual_cell(&row, "castcontext"),
                virtual_cell(&row, "castmethod"),
            ]
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| {
        (
            row.get(2).and_then(sql_value_i64).unwrap_or_default(),
            row.get(3).and_then(sql_value_i64).unwrap_or_default(),
        )
    });
    Ok(rows)
}

pub(crate) fn pg_class_indexrelid_join_row(
    class_alias: &str,
    index_alias: &str,
    namespace_alias: Option<&str>,
    class_row: BTreeMap<String, SqlValue>,
    pg_index_row: BTreeMap<String, SqlValue>,
    namespace_row: Option<BTreeMap<String, SqlValue>>,
) -> SqlRow {
    let mut row = row_from_virtual_row("pg_class", class_alias, class_row);
    row = merge_rows(
        &row,
        &row_from_virtual_row("pg_index", index_alias, pg_index_row),
    );
    if let (Some(alias), Some(namespace_row)) = (namespace_alias, namespace_row) {
        row = merge_rows(
            &row,
            &row_from_virtual_row("pg_namespace", alias, namespace_row),
        );
    }
    row
}

pub(crate) fn pg_class_namespace_join_columns(
    class_alias: &str,
    namespace_alias: &str,
) -> Vec<String> {
    let mut columns = aliased_virtual_columns("pg_class", class_alias);
    columns.extend(aliased_virtual_columns("pg_namespace", namespace_alias));
    columns
}

pub(crate) fn pg_class_namespace_join_row(
    class_alias: &str,
    namespace_alias: &str,
    class_row: BTreeMap<String, SqlValue>,
    namespace_row: BTreeMap<String, SqlValue>,
) -> SqlRow {
    let mut row = row_from_virtual_row("pg_class", class_alias, class_row);
    row = merge_rows(
        &row,
        &row_from_virtual_row("pg_namespace", namespace_alias, namespace_row),
    );
    row
}

pub(crate) fn pg_dump_relation_inventory_aliases(
    from: &TableWithJoins,
) -> Result<Option<PgDumpRelationInventoryAliases>> {
    let Some((_, class_alias)) = table_factor_relation_alias(&from.relation, "pg_class")? else {
        return Ok(None);
    };
    if from.joins.len() != 4 {
        return Ok(None);
    }

    let mut depend_alias = None;
    let mut tablespace_alias = None;
    let mut access_method_alias = None;
    let mut toast_class_alias = None;
    for join in &from.joins {
        if !is_left_join_operator(&join.join_operator) {
            return Ok(None);
        }
        let Some(constraint) = join_operator_constraint(&join.join_operator) else {
            return Ok(None);
        };
        if let Some((_, alias)) = table_factor_relation_alias(&join.relation, "pg_depend")? {
            if !join_constraint_has_column_equality(
                constraint,
                &class_alias,
                "pg_class",
                "oid",
                &alias,
                "pg_depend",
                "objid",
            ) {
                return Ok(None);
            }
            if depend_alias.replace(alias).is_some() {
                return Ok(None);
            }
            continue;
        }
        if let Some((_, alias)) = table_factor_relation_alias(&join.relation, "pg_tablespace")? {
            if !join_constraint_has_column_equality(
                constraint,
                &class_alias,
                "pg_class",
                "reltablespace",
                &alias,
                "pg_tablespace",
                "oid",
            ) {
                return Ok(None);
            }
            if tablespace_alias.replace(alias).is_some() {
                return Ok(None);
            }
            continue;
        }
        if let Some((_, alias)) = table_factor_relation_alias(&join.relation, "pg_am")? {
            if !join_constraint_has_column_equality(
                constraint,
                &class_alias,
                "pg_class",
                "relam",
                &alias,
                "pg_am",
                "oid",
            ) {
                return Ok(None);
            }
            if access_method_alias.replace(alias).is_some() {
                return Ok(None);
            }
            continue;
        }
        if let Some((_, alias)) = table_factor_relation_alias(&join.relation, "pg_class")? {
            if alias.eq_ignore_ascii_case(&class_alias)
                || !join_constraint_has_column_equality(
                    constraint,
                    &class_alias,
                    "pg_class",
                    "reltoastrelid",
                    &alias,
                    "pg_class",
                    "oid",
                )
            {
                return Ok(None);
            }
            if toast_class_alias.replace(alias).is_some() {
                return Ok(None);
            }
            continue;
        }
        return Ok(None);
    }

    let (Some(depend), Some(tablespace), Some(access_method), Some(toast_class)) = (
        depend_alias,
        tablespace_alias,
        access_method_alias,
        toast_class_alias,
    ) else {
        return Ok(None);
    };
    Ok(Some(PgDumpRelationInventoryAliases {
        class: class_alias,
        depend,
        tablespace,
        access_method,
        toast_class,
    }))
}

pub(crate) fn pg_dump_relation_inventory_projection_matches(projection: &[SelectItem]) -> bool {
    let output_names = projection
        .iter()
        .filter_map(pg_dump_relation_inventory_projection_name)
        .map(|name| name.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    [
        "tableoid",
        "oid",
        "relname",
        "acldefault",
        "foreignserver",
        "reltablespace",
        "reloptions",
        "checkoption",
        "amname",
        "is_identity_sequence",
        "ispartition",
    ]
    .into_iter()
    .all(|required| output_names.contains(required))
}

pub(crate) fn pg_dump_relation_inventory_projection_name(item: &SelectItem) -> Option<String> {
    match item {
        SelectItem::UnnamedExpr(expr) => Some(row_expr_column_name(expr)),
        SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
        SelectItem::ExprWithAliases { aliases, .. } => {
            aliases.first().map(|alias| alias.value.clone())
        }
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => None,
    }
}

pub(crate) fn pg_dump_relation_inventory_order_by_matches(
    order_by: Option<&OrderBy>,
    class_alias: &str,
) -> bool {
    let Some(order_by) = order_by else {
        return true;
    };
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return false;
    };
    let [order] = expressions.as_slice() else {
        return false;
    };
    if order.options.asc == Some(false) {
        return false;
    }
    relation_oid_column_matches(&order.expr, class_alias, "pg_class", &["oid"])
}

pub(crate) fn pg_dump_relation_inventory_selection_covered_by_relkind(
    selection: Option<&Expr>,
    class_alias: &str,
) -> Result<bool> {
    let Some(selection) = selection else {
        return Ok(false);
    };
    let terms = and_terms(selection);
    if terms.is_empty() {
        return Ok(false);
    }
    for term in terms {
        if string_filter_values_from_term(term, class_alias, "pg_class", &["relkind"])?.is_none() {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) fn pg_dump_column_info_aliases(
    from: &TableWithJoins,
) -> Result<Option<PgDumpColumnInfoAliases>> {
    let [attribute_join, type_join] = from.joins.as_slice() else {
        return Ok(None);
    };
    let Some((source_alias, source_column, attrelids)) =
        pg_dump_column_info_unnest_attrelids(&from.relation)?
    else {
        return Ok(None);
    };

    if !is_reorderable_inner_join(attribute_join) {
        return Ok(None);
    }
    let Some(attribute_constraint) = join_operator_constraint(&attribute_join.join_operator) else {
        return Ok(None);
    };
    let Some((_, attribute_alias)) =
        table_factor_relation_alias(&attribute_join.relation, "pg_attribute")?
    else {
        return Ok(None);
    };
    if !join_constraint_has_column_equality(
        attribute_constraint,
        &source_alias,
        "unnest",
        &source_column,
        &attribute_alias,
        "pg_attribute",
        "attrelid",
    ) {
        return Ok(None);
    }

    if !is_left_join_operator(&type_join.join_operator) {
        return Ok(None);
    }
    let Some(type_constraint) = join_operator_constraint(&type_join.join_operator) else {
        return Ok(None);
    };
    let Some((_, type_alias)) = table_factor_relation_alias(&type_join.relation, "pg_type")? else {
        return Ok(None);
    };
    if !join_constraint_has_column_equality(
        type_constraint,
        &attribute_alias,
        "pg_attribute",
        "atttypid",
        &type_alias,
        "pg_type",
        "oid",
    ) {
        return Ok(None);
    }

    Ok(Some(PgDumpColumnInfoAliases {
        attribute: attribute_alias,
        attrelids,
    }))
}

pub(crate) fn pg_dump_column_info_unnest_attrelids(
    relation: &TableFactor,
) -> Result<Option<(String, String, BTreeSet<i64>)>> {
    let TableFactor::UNNEST {
        alias,
        array_exprs,
        with_offset,
        with_ordinality,
        ..
    } = relation
    else {
        return Ok(None);
    };
    if *with_offset || *with_ordinality {
        return Ok(None);
    }
    let [array_expr] = array_exprs.as_slice() else {
        return Ok(None);
    };

    let alias_name = alias
        .as_ref()
        .map(|alias| alias.name.value.clone())
        .unwrap_or_else(|| "unnest".to_string());
    let source_column = match alias.as_ref().map(|alias| alias.columns.as_slice()) {
        Some([column]) => column.name.value.clone(),
        Some([]) | None => "unnest".to_string(),
        Some(_) => return Ok(None),
    };
    let values = match eval_constant_expr(array_expr).and_then(unnest_values_from_sql) {
        Ok(values) => values,
        Err(SqlError::Unsupported(_)) => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut attrelids = BTreeSet::new();
    for value in values {
        let Some(oid) = sql_value_i64(&value) else {
            return Ok(None);
        };
        attrelids.insert(oid);
    }
    Ok(Some((alias_name, source_column, attrelids)))
}

pub(crate) fn pg_dump_column_info_projection_matches(projection: &[SelectItem]) -> bool {
    projection
        .iter()
        .filter_map(pg_dump_column_info_projection_name)
        .map(|name| name.to_ascii_lowercase())
        .collect::<Vec<_>>()
        == pg_dump_column_info_columns()
}

pub(crate) fn pg_dump_column_info_projection_name(item: &SelectItem) -> Option<String> {
    match item {
        SelectItem::UnnamedExpr(expr) => Some(row_expr_column_name(expr)),
        SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
        SelectItem::ExprWithAliases { aliases, .. } => {
            aliases.first().map(|alias| alias.value.clone())
        }
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => None,
    }
}

pub(crate) fn pg_dump_column_info_selection_matches(
    selection: Option<&Expr>,
    attribute_alias: &str,
) -> bool {
    let Some(selection) = selection else {
        return false;
    };
    let terms = and_terms(selection);
    !terms.is_empty()
        && terms
            .iter()
            .all(|term| pg_dump_column_info_attnum_positive(term, attribute_alias))
}

pub(crate) fn pg_dump_column_info_attnum_positive(expr: &Expr, attribute_alias: &str) -> bool {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Gt,
        right,
    } = unwrap_nested_expr(expr)
    else {
        return false;
    };
    relation_oid_column_matches(left, attribute_alias, "pg_attribute", &["attnum"])
        && eval_constant_expr(right)
            .ok()
            .and_then(|value| sql_value_i64(&value))
            == Some(0)
}

pub(crate) fn pg_dump_column_info_order_by_matches(
    order_by: Option<&OrderBy>,
    attribute_alias: &str,
) -> bool {
    let Some(order_by) = order_by else {
        return false;
    };
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return false;
    };
    let [attrelid, attnum] = expressions.as_slice() else {
        return false;
    };
    if attrelid.options.asc == Some(false) || attnum.options.asc == Some(false) {
        return false;
    }
    relation_oid_column_matches(
        &attrelid.expr,
        attribute_alias,
        "pg_attribute",
        &["attrelid"],
    ) && relation_oid_column_matches(&attnum.expr, attribute_alias, "pg_attribute", &["attnum"])
}

pub(crate) fn pg_dump_column_info_columns() -> Vec<String> {
    [
        "attrelid",
        "attnum",
        "attname",
        "attstattarget",
        "attstorage",
        "typstorage",
        "atthasdef",
        "attisdropped",
        "attlen",
        "attalign",
        "attislocal",
        "atttypname",
        "attoptions",
        "attcollation",
        "attfdwoptions",
        "notnull_name",
        "notnull_comment",
        "notnull_invalidoid",
        "notnull_noinherit",
        "notnull_islocal",
        "attcompression",
        "attidentity",
        "attmissingval",
        "attgenerated",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub(crate) fn pg_dump_column_info_result_rows(
    db: &BicDb,
    attrelids: &BTreeSet<i64>,
) -> Result<Vec<Vec<SqlValue>>> {
    let relation_by_oid = (*table_oids(db))
        .clone()
        .into_iter()
        .map(|(relation, oid)| (oid, relation))
        .collect::<BTreeMap<_, _>>();
    let sequence_oids = list_sequences(db)?
        .into_iter()
        .map(|sequence| sequence_oid(&sequence.name))
        .collect::<BTreeSet<_>>();
    let schemas = relation_schemas(db)?;
    let columns_by_relation = visible_columns_by_relation(&schemas);
    let type_rows_by_oid = virtual_rows_by_oid(pg_type_rows_for_db(db)?);

    let mut rows = Vec::new();
    for attrelid in attrelids {
        if let Some(relation) = relation_by_oid.get(attrelid) {
            if let Some(columns) = columns_by_relation.get(&relation.to_ascii_lowercase()) {
                rows.extend(pg_dump_column_info_rows_for_columns(
                    *attrelid,
                    columns,
                    &type_rows_by_oid,
                ));
            } else {
                let columns = default_record_columns();
                rows.extend(pg_dump_column_info_rows_for_columns(
                    *attrelid,
                    &columns,
                    &type_rows_by_oid,
                ));
            }
        } else if sequence_oids.contains(attrelid) {
            rows.extend(pg_dump_column_info_rows_for_sequence(
                *attrelid,
                &type_rows_by_oid,
            ));
        }
    }
    Ok(rows)
}

pub(crate) fn visible_columns_by_relation(
    schemas: &[TableSchema],
) -> BTreeMap<String, Vec<ColumnSchema>> {
    let mut columns_by_relation = BTreeMap::new();
    for schema in schemas {
        let columns = schema
            .columns
            .iter()
            .filter(|column| !column.hidden)
            .cloned()
            .collect::<Vec<_>>();
        columns_by_relation.insert(schema.name.to_ascii_lowercase(), columns.clone());
        columns_by_relation.insert(
            format!("{}.{}", schema.schema_name, schema.name).to_ascii_lowercase(),
            columns,
        );
    }
    for table in graph_virtual_table_names() {
        columns_by_relation.insert(
            table.to_ascii_lowercase(),
            graph_virtual_table_columns(table),
        );
    }
    columns_by_relation
}

pub(crate) fn pg_dump_column_info_rows_for_columns(
    attrelid: i64,
    columns: &[ColumnSchema],
    type_rows_by_oid: &BTreeMap<i64, BTreeMap<String, SqlValue>>,
) -> Vec<Vec<SqlValue>> {
    columns
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            pg_dump_column_info_row(
                attrelid,
                idx as i64 + 1,
                &column.name,
                &column.pg_type,
                column.type_oid(),
                column.catalog_typmod() as i32,
                !column.nullable || column.primary_key,
                column.default_sequence.is_some(),
                column.identity.as_deref().unwrap_or_default(),
                type_rows_by_oid,
            )
        })
        .collect()
}

pub(crate) fn pg_dump_column_info_rows_for_sequence(
    attrelid: i64,
    type_rows_by_oid: &BTreeMap<i64, BTreeMap<String, SqlValue>>,
) -> Vec<Vec<SqlValue>> {
    [
        ("last_value", "int8"),
        ("log_cnt", "int8"),
        ("is_called", "bool"),
    ]
    .into_iter()
    .enumerate()
    .map(|(idx, (name, pg_type))| {
        pg_dump_column_info_row(
            attrelid,
            idx as i64 + 1,
            name,
            pg_type,
            pg_type_oid(pg_type),
            -1,
            false,
            false,
            "",
            type_rows_by_oid,
        )
    })
    .collect()
}

pub(crate) fn pg_dump_column_info_row(
    attrelid: i64,
    attnum: i64,
    attname: &str,
    pg_type: &str,
    atttypid: i64,
    atttypmod: i32,
    attnotnull: bool,
    atthasdef: bool,
    attidentity: &str,
    type_rows_by_oid: &BTreeMap<i64, BTreeMap<String, SqlValue>>,
) -> Vec<SqlValue> {
    let type_row = type_rows_by_oid.get(&atttypid);
    let type_cell = |column: &str| {
        type_row
            .map(|row| virtual_cell(row, column))
            .unwrap_or(SqlValue::Null)
    };
    let attcollation = SqlValue::Int(type_collation_oid(pg_type));
    let typcollation = type_cell("typcollation");
    let formatted_type = pg_format_type(i32::try_from(atttypid).unwrap_or_default(), atttypmod)
        .or_else(|| {
            let element_oid = sql_value_i64(&type_cell("typelem"))?;
            if element_oid == 0 {
                return sql_value_text(&type_cell("typname"));
            }
            let element = type_rows_by_oid.get(&element_oid)?;
            let element_name = sql_value_text(&virtual_cell(element, "typname"))?;
            Some(format!("{}[]", pg_quote_ident(&element_name)))
        })
        .map(SqlValue::String)
        .unwrap_or_else(|| type_cell("typname"));

    vec![
        SqlValue::Int(attrelid),
        SqlValue::Int(attnum),
        SqlValue::String(attname.to_string()),
        SqlValue::Null,
        type_cell("typstorage"),
        type_cell("typstorage"),
        SqlValue::Bool(atthasdef),
        SqlValue::Bool(false),
        type_cell("typlen"),
        type_cell("typalign"),
        SqlValue::Bool(true),
        formatted_type,
        SqlValue::Null,
        if values_equal(&attcollation, &typcollation) {
            SqlValue::Int(0)
        } else {
            attcollation
        },
        SqlValue::Null,
        if attnotnull {
            SqlValue::String(String::new())
        } else {
            SqlValue::Null
        },
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Bool(false),
        SqlValue::Bool(true),
        SqlValue::String(String::new()),
        SqlValue::String(attidentity.to_string()),
        SqlValue::Null,
        SqlValue::String(String::new()),
    ]
}

pub(crate) fn pg_dump_constraint_inventory_aliases(
    from: &TableWithJoins,
) -> Result<Option<PgDumpConstraintInventoryAliases>> {
    let Some((source_alias, source_column, conrelids)) =
        pg_dump_column_info_unnest_attrelids(&from.relation)?
    else {
        return Ok(None);
    };
    let [constraint_join] = from.joins.as_slice() else {
        return Ok(None);
    };
    if !is_reorderable_inner_join(constraint_join) {
        return Ok(None);
    }
    let Some(constraint) = join_operator_constraint(&constraint_join.join_operator) else {
        return Ok(None);
    };
    let Some((_, constraint_alias)) =
        table_factor_relation_alias(&constraint_join.relation, "pg_constraint")?
    else {
        return Ok(None);
    };
    if !join_constraint_has_column_equality(
        constraint,
        &source_alias,
        "unnest",
        &source_column,
        &constraint_alias,
        "pg_constraint",
        "conrelid",
    ) {
        return Ok(None);
    }
    Ok(Some(PgDumpConstraintInventoryAliases {
        constraint: constraint_alias,
        conrelids,
    }))
}

pub(crate) fn pg_dump_constraint_inventory_projection_kind(
    projection: &[SelectItem],
) -> Option<PgDumpConstraintInventoryKind> {
    let names = projection
        .iter()
        .filter_map(pg_dump_constraint_inventory_projection_name)
        .map(|name| name.to_ascii_lowercase())
        .collect::<Vec<_>>();
    if names == pg_dump_constraint_inventory_check_columns() {
        return Some(PgDumpConstraintInventoryKind::Check);
    }
    if names == pg_dump_constraint_inventory_foreign_key_columns() {
        return Some(PgDumpConstraintInventoryKind::ForeignKey);
    }
    None
}

pub(crate) fn pg_dump_constraint_inventory_projection_name(item: &SelectItem) -> Option<String> {
    match item {
        SelectItem::UnnamedExpr(expr) => Some(row_expr_column_name(expr)),
        SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
        SelectItem::ExprWithAliases { aliases, .. } => {
            aliases.first().map(|alias| alias.value.clone())
        }
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => None,
    }
}

pub(crate) fn pg_dump_constraint_inventory_selection_kind(
    selection: Option<&Expr>,
    constraint_alias: &str,
) -> Option<PgDumpConstraintInventoryKind> {
    let selection = selection?;
    let mut kind = None;
    let mut saw_parent_filter = false;
    for term in and_terms(selection) {
        if let Some(term_kind) =
            pg_dump_constraint_inventory_contype_term_kind(term, constraint_alias)
        {
            if kind.replace(term_kind).is_some() {
                return None;
            }
            continue;
        }
        if pg_dump_constraint_inventory_conparentid_zero(term, constraint_alias) {
            if saw_parent_filter {
                return None;
            }
            saw_parent_filter = true;
            continue;
        }
        return None;
    }
    match kind {
        Some(PgDumpConstraintInventoryKind::Check) if !saw_parent_filter => {
            Some(PgDumpConstraintInventoryKind::Check)
        }
        Some(PgDumpConstraintInventoryKind::ForeignKey) if saw_parent_filter => {
            Some(PgDumpConstraintInventoryKind::ForeignKey)
        }
        _ => None,
    }
}

pub(crate) fn pg_dump_constraint_inventory_contype_term_kind(
    expr: &Expr,
    constraint_alias: &str,
) -> Option<PgDumpConstraintInventoryKind> {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = unwrap_nested_expr(expr)
    else {
        return None;
    };
    let literal =
        if relation_oid_column_matches(left, constraint_alias, "pg_constraint", &["contype"]) {
            eval_constant_expr(right).ok()
        } else if relation_oid_column_matches(
            right,
            constraint_alias,
            "pg_constraint",
            &["contype"],
        ) {
            eval_constant_expr(left).ok()
        } else {
            None
        };
    match literal.as_ref().and_then(sql_value_text)?.as_str() {
        "c" | "C" => Some(PgDumpConstraintInventoryKind::Check),
        "f" | "F" => Some(PgDumpConstraintInventoryKind::ForeignKey),
        _ => None,
    }
}

pub(crate) fn pg_dump_constraint_inventory_conparentid_zero(
    expr: &Expr,
    constraint_alias: &str,
) -> bool {
    catalog_column_constant_matches(
        expr,
        constraint_alias,
        "pg_constraint",
        "conparentid",
        |value| sql_value_i64(value) == Some(0),
    )
}

pub(crate) fn pg_dump_constraint_inventory_order_by_matches(
    order_by: Option<&OrderBy>,
    constraint_alias: &str,
) -> bool {
    let Some(order_by) = order_by else {
        return false;
    };
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return false;
    };
    let [conrelid, conname] = expressions.as_slice() else {
        return false;
    };
    if conrelid.options.asc == Some(false) || conname.options.asc == Some(false) {
        return false;
    }
    relation_oid_column_matches(
        &conrelid.expr,
        constraint_alias,
        "pg_constraint",
        &["conrelid"],
    ) && relation_oid_column_matches(
        &conname.expr,
        constraint_alias,
        "pg_constraint",
        &["conname"],
    )
}

pub(crate) fn pg_dump_constraint_inventory_columns(
    kind: PgDumpConstraintInventoryKind,
) -> Vec<String> {
    match kind {
        PgDumpConstraintInventoryKind::Check => pg_dump_constraint_inventory_check_columns(),
        PgDumpConstraintInventoryKind::ForeignKey => {
            pg_dump_constraint_inventory_foreign_key_columns()
        }
    }
}

pub(crate) fn pg_dump_constraint_inventory_check_columns() -> Vec<String> {
    [
        "tableoid",
        "oid",
        "conrelid",
        "conname",
        "consrc",
        "conislocal",
        "convalidated",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub(crate) fn pg_dump_constraint_inventory_foreign_key_columns() -> Vec<String> {
    [
        "tableoid",
        "oid",
        "conrelid",
        "conname",
        "confrelid",
        "conindid",
        "condef",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub(crate) fn pg_dump_constraint_inventory_result_rows(
    kind: PgDumpConstraintInventoryKind,
    schemas: &[TableSchema],
    table_oids: &BTreeMap<String, i64>,
    conrelids: &BTreeSet<i64>,
) -> Vec<Vec<SqlValue>> {
    match kind {
        PgDumpConstraintInventoryKind::Check => {
            pg_dump_check_constraint_inventory_rows(schemas, table_oids, conrelids)
        }
        PgDumpConstraintInventoryKind::ForeignKey => {
            pg_dump_foreign_key_constraint_inventory_rows(schemas, table_oids, conrelids)
        }
    }
}

pub(crate) fn pg_dump_extension_fk_dependency_aliases(
    from: &TableWithJoins,
) -> Result<Option<(String, String)>> {
    let Some((_, constraint_alias)) = table_factor_relation_alias(&from.relation, "pg_constraint")?
    else {
        return Ok(None);
    };
    let [depend_join] = from.joins.as_slice() else {
        return Ok(None);
    };
    if !is_reorderable_inner_join(depend_join) {
        return Ok(None);
    }
    let Some(join_constraint) = join_operator_constraint(&depend_join.join_operator) else {
        return Ok(None);
    };
    let Some((_, depend_alias)) = table_factor_relation_alias(&depend_join.relation, "pg_depend")?
    else {
        return Ok(None);
    };
    if !join_constraint_has_column_equality(
        join_constraint,
        &depend_alias,
        "pg_depend",
        "objid",
        &constraint_alias,
        "pg_constraint",
        "confrelid",
    ) {
        return Ok(None);
    }
    Ok(Some((constraint_alias, depend_alias)))
}

pub(crate) fn pg_dump_extension_fk_dependency_projection_matches(
    projection: &[SelectItem],
) -> bool {
    projection
        .iter()
        .filter_map(pg_dump_constraint_inventory_projection_name)
        .map(|name| name.to_ascii_lowercase())
        .collect::<Vec<_>>()
        == ["conrelid", "confrelid"]
}

pub(crate) fn pg_dump_extension_fk_dependency_selection_matches(
    selection: Option<&Expr>,
    constraint_alias: &str,
    depend_alias: &str,
) -> bool {
    let Some(selection) = selection else {
        return false;
    };

    let mut saw_foreign_key_constraint = false;
    let mut saw_extension_reference = false;
    let mut saw_class_dependency = false;
    for term in and_terms(selection) {
        if catalog_column_constant_matches(
            term,
            constraint_alias,
            "pg_constraint",
            "contype",
            |value| sql_value_text(value).is_some_and(|text| text.eq_ignore_ascii_case("f")),
        ) {
            saw_foreign_key_constraint = true;
            continue;
        }
        if catalog_column_constant_matches(term, depend_alias, "pg_depend", "refclassid", |value| {
            catalog_oid_value_matches(value, PG_EXTENSION_CATALOG_OID, "pg_extension")
        }) {
            saw_extension_reference = true;
            continue;
        }
        if catalog_column_constant_matches(term, depend_alias, "pg_depend", "classid", |value| {
            catalog_oid_value_matches(value, PG_CLASS_CATALOG_OID, "pg_class")
        }) {
            saw_class_dependency = true;
        }
    }

    saw_foreign_key_constraint && saw_extension_reference && saw_class_dependency
}

pub(crate) fn catalog_oid_value_matches(value: &SqlValue, oid: i64, regclass_name: &str) -> bool {
    if sql_value_i64(value) == Some(oid) {
        return true;
    }
    sql_value_text(value).is_some_and(|text| {
        text.eq_ignore_ascii_case(regclass_name)
            || regclass_relation_name(&text).eq_ignore_ascii_case(regclass_name)
    })
}

pub(crate) fn pg_dump_check_constraint_inventory_rows(
    schemas: &[TableSchema],
    table_oids: &BTreeMap<String, i64>,
    conrelids: &BTreeSet<i64>,
) -> Vec<Vec<SqlValue>> {
    let mut rows = Vec::new();
    for schema in schemas {
        let table_oid = *table_oids.get(&schema.name).unwrap_or(&0);
        if !conrelids.contains(&table_oid) {
            continue;
        }
        let mut constraint_slot = first_table_constraint_slot(schema);
        for constraint in &schema.constraints {
            match constraint {
                ConstraintSchema::Unique { name, columns, .. } => {
                    if !unique_constraint_is_primary_key(schema, name, columns) {
                        constraint_slot += 1;
                    }
                }
                ConstraintSchema::Check {
                    name,
                    expression,
                    validated,
                } => {
                    rows.push(vec![
                        SqlValue::Int(PG_CONSTRAINT_CATALOG_OID),
                        SqlValue::Int(pg_constraint_oid(table_oid, constraint_slot)),
                        SqlValue::Int(table_oid),
                        SqlValue::String(name.clone()),
                        SqlValue::String(pg_check_constraint_definition(expression)),
                        SqlValue::Bool(true),
                        SqlValue::Bool(*validated),
                    ]);
                    constraint_slot += 1;
                }
                ConstraintSchema::ForeignKey { .. } | ConstraintSchema::Exclusion { .. } => {
                    constraint_slot += 1;
                }
            }
        }
    }
    rows.sort_by(|left, right| {
        (
            sql_value_i64(&left[2]).unwrap_or_default(),
            left[3].to_cell(),
        )
            .cmp(&(
                sql_value_i64(&right[2]).unwrap_or_default(),
                right[3].to_cell(),
            ))
    });
    sql_profile_rows_materialized(rows.len(), sql_result_rows_memory_estimate(&rows));
    rows
}

pub(crate) fn pg_dump_foreign_key_constraint_inventory_rows(
    schemas: &[TableSchema],
    table_oids: &BTreeMap<String, i64>,
    conrelids: &BTreeSet<i64>,
) -> Vec<Vec<SqlValue>> {
    let schema_by_name = schemas
        .iter()
        .map(|schema| (schema.name.to_ascii_lowercase(), schema))
        .collect::<BTreeMap<_, _>>();
    let mut rows = Vec::new();
    for schema in schemas {
        let table_oid = *table_oids.get(&schema.name).unwrap_or(&0);
        if !conrelids.contains(&table_oid) {
            continue;
        }
        let mut constraint_slot = first_table_constraint_slot(schema);
        for constraint in &schema.constraints {
            match constraint {
                ConstraintSchema::Unique { name, columns, .. } => {
                    if !unique_constraint_is_primary_key(schema, name, columns) {
                        constraint_slot += 1;
                    }
                }
                ConstraintSchema::Check { .. } | ConstraintSchema::Exclusion { .. } => {
                    constraint_slot += 1;
                }
                ConstraintSchema::ForeignKey {
                    name,
                    columns,
                    foreign_table,
                    referred_columns,
                    ..
                } => {
                    let foreign_oid = *table_oids.get(foreign_table).unwrap_or(&0);
                    rows.push(vec![
                        SqlValue::Int(PG_CONSTRAINT_CATALOG_OID),
                        SqlValue::Int(pg_constraint_oid(table_oid, constraint_slot)),
                        SqlValue::Int(table_oid),
                        SqlValue::String(name.clone()),
                        SqlValue::Int(foreign_oid),
                        SqlValue::Int(0),
                        SqlValue::String(pg_foreign_key_constraint_definition(
                            columns,
                            foreign_table,
                            referred_columns,
                            &schema_by_name,
                        )),
                    ]);
                    constraint_slot += 1;
                }
            }
        }
    }
    rows.sort_by(|left, right| {
        (
            sql_value_i64(&left[2]).unwrap_or_default(),
            left[3].to_cell(),
        )
            .cmp(&(
                sql_value_i64(&right[2]).unwrap_or_default(),
                right[3].to_cell(),
            ))
    });
    sql_profile_rows_materialized(rows.len(), sql_result_rows_memory_estimate(&rows));
    rows
}

pub(crate) fn first_table_constraint_slot(schema: &TableSchema) -> i64 {
    let not_null_constraints = schema
        .columns
        .iter()
        .filter(|column| !column.hidden && (!column.nullable || column.primary_key))
        .count() as i64;
    1 + not_null_constraints
}

pub(crate) fn pg_foreign_key_constraint_definition(
    columns: &[String],
    foreign_table: &str,
    referred_columns: &[String],
    schema_by_name: &BTreeMap<String, &TableSchema>,
) -> String {
    let foreign_schema = schema_by_name.get(&foreign_table.to_ascii_lowercase());
    let foreign_name = match foreign_schema {
        Some(schema) if !schema.schema_name.eq_ignore_ascii_case("public") => {
            format!("{}.{}", schema.schema_name, schema.name)
        }
        Some(schema) => schema.name.clone(),
        None => foreign_table.to_string(),
    };
    format!(
        "FOREIGN KEY ({}) REFERENCES {}({})",
        columns.join(", "),
        foreign_name,
        referred_columns.join(", ")
    )
}

pub(crate) fn pg_dump_proc_inventory_aliases(
    from: &TableWithJoins,
) -> Result<Option<PgDumpProcInventoryAliases>> {
    let Some((_, proc_alias)) = table_factor_relation_alias(&from.relation, "pg_proc")? else {
        return Ok(None);
    };
    let [init_privs_join] = from.joins.as_slice() else {
        return Ok(None);
    };
    if !is_left_join_operator(&init_privs_join.join_operator) {
        return Ok(None);
    }
    let Some(constraint) = join_operator_constraint(&init_privs_join.join_operator) else {
        return Ok(None);
    };
    let Some((_, init_privs_alias)) =
        table_factor_relation_alias(&init_privs_join.relation, "pg_init_privs")?
    else {
        return Ok(None);
    };
    if !pg_dump_proc_init_privs_join_matches(constraint, &proc_alias, &init_privs_alias) {
        return Ok(None);
    }
    Ok(Some(PgDumpProcInventoryAliases {
        proc: proc_alias,
        init_privs: init_privs_alias,
    }))
}

pub(crate) fn pg_dump_proc_init_privs_join_matches(
    constraint: &JoinConstraint,
    proc_alias: &str,
    init_privs_alias: &str,
) -> bool {
    let JoinConstraint::On(expr) = constraint else {
        return false;
    };
    let mut saw_objoid = false;
    let mut saw_classid = false;
    let mut saw_objsubid = false;
    for term in and_terms(expr) {
        if join_constraint_has_column_equality(
            &JoinConstraint::On(term.clone()),
            proc_alias,
            "pg_proc",
            "oid",
            init_privs_alias,
            "pg_init_privs",
            "objoid",
        ) {
            saw_objoid = true;
            continue;
        }
        if catalog_column_constant_matches(
            term,
            init_privs_alias,
            "pg_init_privs",
            "classoid",
            |value| sql_value_text(value).is_some_and(|text| text.eq_ignore_ascii_case("pg_proc")),
        ) {
            saw_classid = true;
            continue;
        }
        if catalog_column_constant_matches(
            term,
            init_privs_alias,
            "pg_init_privs",
            "objsubid",
            |value| sql_value_i64(value) == Some(0),
        ) {
            saw_objsubid = true;
            continue;
        }
        return false;
    }
    saw_objoid && saw_classid && saw_objsubid
}

pub(crate) fn pg_dump_proc_inventory_projection_kind(
    projection: &[SelectItem],
) -> Option<PgDumpProcInventoryKind> {
    let names = projection
        .iter()
        .filter_map(pg_dump_proc_inventory_projection_name)
        .map(|name| name.to_ascii_lowercase())
        .collect::<Vec<_>>();
    if names == pg_dump_proc_function_inventory_columns() {
        return Some(PgDumpProcInventoryKind::Function);
    }
    if names == pg_dump_proc_aggregate_inventory_columns() {
        return Some(PgDumpProcInventoryKind::Aggregate);
    }
    None
}

pub(crate) fn pg_dump_proc_inventory_projection_name(item: &SelectItem) -> Option<String> {
    match item {
        SelectItem::UnnamedExpr(expr) => Some(row_expr_column_name(expr)),
        SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
        SelectItem::ExprWithAliases { aliases, .. } => {
            aliases.first().map(|alias| alias.value.clone())
        }
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => None,
    }
}

pub(crate) fn pg_dump_proc_inventory_selection_kind(
    selection: Option<&Expr>,
    aliases: &PgDumpProcInventoryAliases,
) -> Option<PgDumpProcInventoryKind> {
    let selection = selection?;
    let mut prokind = None;
    let mut saw_visibility = false;
    let mut saw_depend_exclusion = false;
    for term in and_terms(selection) {
        if let Some(kind) = pg_dump_proc_prokind_term_kind(term, &aliases.proc) {
            if prokind.replace(kind).is_some() {
                return None;
            }
            continue;
        }
        if pg_dump_proc_visibility_term_matches(term, aliases) {
            if saw_visibility {
                return None;
            }
            saw_visibility = true;
            continue;
        }
        if pg_dump_proc_not_exists_depend_term_matches(term, aliases) {
            if saw_depend_exclusion {
                return None;
            }
            saw_depend_exclusion = true;
            continue;
        }
        return None;
    }
    match prokind {
        Some(PgDumpProcInventoryKind::Aggregate) if saw_visibility && !saw_depend_exclusion => {
            Some(PgDumpProcInventoryKind::Aggregate)
        }
        Some(PgDumpProcInventoryKind::Function) if saw_visibility && saw_depend_exclusion => {
            Some(PgDumpProcInventoryKind::Function)
        }
        _ => None,
    }
}

pub(crate) fn pg_dump_proc_prokind_term_kind(
    expr: &Expr,
    proc_alias: &str,
) -> Option<PgDumpProcInventoryKind> {
    let Expr::BinaryOp { left, op, right } = unwrap_nested_expr(expr) else {
        return None;
    };
    if !matches!(op, BinaryOperator::Eq | BinaryOperator::NotEq) {
        return None;
    }
    let literal = if relation_oid_column_matches(left, proc_alias, "pg_proc", &["prokind"]) {
        eval_constant_expr(right).ok()
    } else if relation_oid_column_matches(right, proc_alias, "pg_proc", &["prokind"]) {
        eval_constant_expr(left).ok()
    } else {
        None
    };
    let text = literal.as_ref().and_then(sql_value_text)?;
    if !text.eq_ignore_ascii_case("a") {
        return None;
    }
    match op {
        BinaryOperator::Eq => Some(PgDumpProcInventoryKind::Aggregate),
        BinaryOperator::NotEq => Some(PgDumpProcInventoryKind::Function),
        _ => None,
    }
}

pub(crate) fn pg_dump_proc_visibility_term_matches(
    expr: &Expr,
    aliases: &PgDumpProcInventoryAliases,
) -> bool {
    let mut saw_namespace_filter = false;
    for term in or_terms(expr) {
        if pg_dump_proc_namespace_not_catalog_term_matches(term, &aliases.proc) {
            saw_namespace_filter = true;
            continue;
        }
        if pg_dump_proc_exists_over_empty_catalog(term, "pg_cast")
            || pg_dump_proc_exists_over_empty_catalog(term, "pg_transform")
            || pg_dump_proc_acl_distinct_init_privs_term_matches(term, aliases)
        {
            continue;
        }
        return false;
    }
    saw_namespace_filter
}

pub(crate) fn pg_dump_proc_namespace_not_catalog_term_matches(
    expr: &Expr,
    proc_alias: &str,
) -> bool {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::NotEq,
        right,
    } = unwrap_nested_expr(expr)
    else {
        return false;
    };
    (relation_oid_column_matches(left, proc_alias, "pg_proc", &["pronamespace"])
        && pg_catalog_namespace_oid_subquery_matches(right))
        || (relation_oid_column_matches(right, proc_alias, "pg_proc", &["pronamespace"])
            && pg_catalog_namespace_oid_subquery_matches(left))
}

pub(crate) fn pg_catalog_namespace_oid_subquery_matches(expr: &Expr) -> bool {
    let Expr::Subquery(query) = unwrap_nested_expr(expr) else {
        return false;
    };
    let Some((select, namespace_alias)) = select_single_table_alias(query, "pg_namespace") else {
        return false;
    };
    let [projection] = select.projection.as_slice() else {
        return false;
    };
    if pg_dump_proc_inventory_projection_name(projection)
        .is_none_or(|name| !name.eq_ignore_ascii_case("oid"))
    {
        return false;
    }
    let Some(selection) = select.selection.as_ref() else {
        return false;
    };
    let terms = and_terms(selection);
    matches!(
        terms.as_slice(),
        [term] if catalog_column_constant_matches(
            term,
            &namespace_alias,
            "pg_namespace",
            "nspname",
            |value| sql_value_text(value)
                .is_some_and(|text| text.eq_ignore_ascii_case("pg_catalog")),
        )
    )
}

pub(crate) fn pg_dump_proc_exists_over_empty_catalog(expr: &Expr, table: &str) -> bool {
    let Expr::Exists {
        subquery,
        negated: false,
    } = unwrap_nested_expr(expr)
    else {
        return false;
    };
    select_single_table_alias(subquery, table).is_some()
}

pub(crate) fn pg_dump_proc_acl_distinct_init_privs_term_matches(
    expr: &Expr,
    aliases: &PgDumpProcInventoryAliases,
) -> bool {
    let Expr::IsDistinctFrom(left, right) = unwrap_nested_expr(expr) else {
        return false;
    };
    (relation_oid_column_matches(left, &aliases.proc, "pg_proc", &["proacl"])
        && relation_oid_column_matches(right, &aliases.init_privs, "pg_init_privs", &["initprivs"]))
        || (relation_oid_column_matches(right, &aliases.proc, "pg_proc", &["proacl"])
            && relation_oid_column_matches(
                left,
                &aliases.init_privs,
                "pg_init_privs",
                &["initprivs"],
            ))
}

pub(crate) fn pg_dump_proc_not_exists_depend_term_matches(
    expr: &Expr,
    aliases: &PgDumpProcInventoryAliases,
) -> bool {
    let Expr::Exists {
        subquery,
        negated: true,
    } = unwrap_nested_expr(expr)
    else {
        return false;
    };
    let Some((select, depend_alias)) = select_single_table_alias(subquery, "pg_depend") else {
        return false;
    };
    let Some(selection) = select.selection.as_ref() else {
        return false;
    };

    let mut saw_classid = false;
    let mut saw_objid = false;
    let mut saw_deptype = false;
    for term in and_terms(selection) {
        if catalog_column_constant_matches(term, &depend_alias, "pg_depend", "classid", |value| {
            sql_value_text(value).is_some_and(|text| text.eq_ignore_ascii_case("pg_proc"))
        }) {
            saw_classid = true;
            continue;
        }
        if expr_has_column_equality(
            term,
            &depend_alias,
            "pg_depend",
            "objid",
            &aliases.proc,
            "pg_proc",
            "oid",
        ) {
            saw_objid = true;
            continue;
        }
        if catalog_column_constant_matches(term, &depend_alias, "pg_depend", "deptype", |value| {
            sql_value_text(value).is_some_and(|text| text.eq_ignore_ascii_case("i"))
        }) {
            saw_deptype = true;
            continue;
        }
        return false;
    }
    saw_classid && saw_objid && saw_deptype
}

pub(crate) fn select_single_table_alias<'a>(
    query: &'a Query,
    expected_table: &str,
) -> Option<(&'a Select, String)> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let [from] = select.from.as_slice() else {
        return None;
    };
    if !from.joins.is_empty() {
        return None;
    }
    table_factor_relation_alias(&from.relation, expected_table)
        .ok()
        .flatten()
        .map(|(_, alias)| (select.as_ref(), alias))
}

pub(crate) fn catalog_column_constant_matches<F>(
    expr: &Expr,
    alias: &str,
    table: &str,
    field: &str,
    predicate: F,
) -> bool
where
    F: Fn(&SqlValue) -> bool,
{
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = unwrap_nested_expr(expr)
    else {
        return false;
    };
    if relation_oid_column_matches(left, alias, table, &[field]) {
        return eval_constant_expr(right)
            .ok()
            .is_some_and(|value| predicate(&value));
    }
    if relation_oid_column_matches(right, alias, table, &[field]) {
        return eval_constant_expr(left)
            .ok()
            .is_some_and(|value| predicate(&value));
    }
    false
}

pub(crate) fn or_terms(expr: &Expr) -> Vec<&Expr> {
    let mut terms = Vec::new();
    collect_or_terms(expr, &mut terms);
    terms
}

pub(crate) fn collect_or_terms<'a>(expr: &'a Expr, terms: &mut Vec<&'a Expr>) {
    match unwrap_nested_expr(expr) {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => {
            collect_or_terms(left, terms);
            collect_or_terms(right, terms);
        }
        other => terms.push(other),
    }
}

pub(crate) fn pg_dump_proc_inventory_row_matches(
    proc_row: &BTreeMap<String, SqlValue>,
    kind: PgDumpProcInventoryKind,
) -> bool {
    let prokind = sql_value_text(&virtual_cell(proc_row, "prokind")).unwrap_or_default();
    let pronamespace = sql_value_i64(&virtual_cell(proc_row, "pronamespace")).unwrap_or_default();
    let visible_to_pg_dump = pronamespace != namespace_oid("pg_catalog");
    (match kind {
        PgDumpProcInventoryKind::Aggregate => prokind.eq_ignore_ascii_case("a"),
        PgDumpProcInventoryKind::Function => !prokind.eq_ignore_ascii_case("a"),
    }) && visible_to_pg_dump
}

pub(crate) fn pg_dump_proc_inventory_context_row(
    aliases: &PgDumpProcInventoryAliases,
    proc_row: &BTreeMap<String, SqlValue>,
) -> SqlRow {
    let mut row = row_from_virtual_row("pg_proc", &aliases.proc, proc_row.clone());
    row = merge_rows(
        &row,
        &row_from_virtual_row(
            "pg_init_privs",
            &aliases.init_privs,
            pg_init_privs_null_row(),
        ),
    );
    row
}

pub(crate) fn pg_init_privs_null_row() -> BTreeMap<String, SqlValue> {
    btree_null_row_for_columns(
        &virtual_table_columns("pg_init_privs")
            .expect("pg_init_privs virtual columns must be declared"),
    )
}

pub(crate) fn pg_dump_proc_inventory_result_row(
    kind: PgDumpProcInventoryKind,
    proc_row: &BTreeMap<String, SqlValue>,
) -> Vec<SqlValue> {
    let proowner = virtual_cell(proc_row, "proowner");
    let acldefault = eval_acldefault(&[SqlValue::String("f".to_string()), proowner.clone()])
        .unwrap_or(SqlValue::Null);
    match kind {
        PgDumpProcInventoryKind::Aggregate => vec![
            SqlValue::Int(PG_PROC_CATALOG_OID),
            virtual_cell(proc_row, "oid"),
            virtual_cell(proc_row, "proname"),
            virtual_cell(proc_row, "pronamespace"),
            virtual_cell(proc_row, "pronargs"),
            virtual_cell(proc_row, "proargtypes"),
            proowner,
            virtual_cell(proc_row, "proacl"),
            acldefault,
        ],
        PgDumpProcInventoryKind::Function => vec![
            SqlValue::Int(PG_PROC_CATALOG_OID),
            virtual_cell(proc_row, "oid"),
            virtual_cell(proc_row, "proname"),
            virtual_cell(proc_row, "prolang"),
            virtual_cell(proc_row, "pronargs"),
            virtual_cell(proc_row, "proargtypes"),
            virtual_cell(proc_row, "prorettype"),
            virtual_cell(proc_row, "proacl"),
            acldefault,
            virtual_cell(proc_row, "pronamespace"),
            proowner,
        ],
    }
}

pub(crate) fn pg_dump_proc_inventory_columns(kind: PgDumpProcInventoryKind) -> Vec<String> {
    match kind {
        PgDumpProcInventoryKind::Aggregate => pg_dump_proc_aggregate_inventory_columns(),
        PgDumpProcInventoryKind::Function => pg_dump_proc_function_inventory_columns(),
    }
}

pub(crate) fn pg_dump_proc_function_inventory_columns() -> Vec<String> {
    [
        "tableoid",
        "oid",
        "proname",
        "prolang",
        "pronargs",
        "proargtypes",
        "prorettype",
        "proacl",
        "acldefault",
        "pronamespace",
        "proowner",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub(crate) fn pg_dump_proc_aggregate_inventory_columns() -> Vec<String> {
    [
        "tableoid",
        "oid",
        "aggname",
        "aggnamespace",
        "pronargs",
        "proargtypes",
        "proowner",
        "aggacl",
        "acldefault",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub(crate) fn pg_dump_type_query_alias(from: &TableWithJoins) -> Result<Option<String>> {
    if !from.joins.is_empty() {
        return Ok(None);
    }
    table_factor_relation_alias(&from.relation, "pg_type")
        .map(|alias| alias.map(|(_, alias)| alias))
}

pub(crate) fn pg_dump_type_query_projection_matches(projection: &[SelectItem]) -> bool {
    projection
        .iter()
        .filter_map(pg_dump_type_query_projection_name)
        .map(|name| name.to_ascii_lowercase())
        .collect::<Vec<_>>()
        == pg_dump_type_query_columns()
}

pub(crate) fn pg_dump_type_query_projection_name(item: &SelectItem) -> Option<String> {
    match item {
        SelectItem::UnnamedExpr(expr) => Some(row_expr_column_name(expr)),
        SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
        SelectItem::ExprWithAliases { aliases, .. } => {
            aliases.first().map(|alias| alias.value.clone())
        }
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => None,
    }
}

pub(crate) fn pg_dump_type_query_columns() -> Vec<String> {
    [
        "tableoid",
        "oid",
        "typname",
        "typnamespace",
        "typacl",
        "acldefault",
        "typowner",
        "typelem",
        "typrelid",
        "typarray",
        "typrelkind",
        "typtype",
        "typisdefined",
        "isarray",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub(crate) fn pg_dump_type_query_result_row(
    type_row: &BTreeMap<String, SqlValue>,
    type_rows_by_oid: &BTreeMap<i64, BTreeMap<String, SqlValue>>,
    class_rows_by_oid: &Option<BTreeMap<i64, BTreeMap<String, SqlValue>>>,
) -> Vec<SqlValue> {
    let typowner = virtual_cell(type_row, "typowner");
    let acldefault = eval_acldefault(&[SqlValue::String("T".to_string()), typowner.clone()])
        .unwrap_or(SqlValue::Null);
    let typrelid = sql_value_i64(&virtual_cell(type_row, "typrelid")).unwrap_or_default();
    let typelem = sql_value_i64(&virtual_cell(type_row, "typelem")).unwrap_or_default();
    let oid = sql_value_i64(&virtual_cell(type_row, "oid")).unwrap_or_default();
    let typname = sql_value_text(&virtual_cell(type_row, "typname")).unwrap_or_default();
    let typrelkind = if typrelid == 0 {
        SqlValue::String(" ".to_string())
    } else {
        class_rows_by_oid
            .as_ref()
            .and_then(|rows| rows.get(&typrelid))
            .map(|row| virtual_cell(row, "relkind"))
            .unwrap_or(SqlValue::Null)
    };
    let isarray = typname.starts_with('_')
        && typelem != 0
        && type_rows_by_oid.get(&typelem).is_some_and(|element_row| {
            values_equal(&virtual_cell(element_row, "typarray"), &SqlValue::Int(oid))
        });

    vec![
        SqlValue::Int(1247),
        virtual_cell(type_row, "oid"),
        virtual_cell(type_row, "typname"),
        virtual_cell(type_row, "typnamespace"),
        virtual_cell(type_row, "typacl"),
        acldefault,
        typowner,
        virtual_cell(type_row, "typelem"),
        virtual_cell(type_row, "typrelid"),
        virtual_cell(type_row, "typarray"),
        typrelkind,
        virtual_cell(type_row, "typtype"),
        virtual_cell(type_row, "typisdefined"),
        SqlValue::Bool(isarray),
    ]
}

pub(crate) fn pg_dump_relation_inventory_context_row(
    aliases: &PgDumpRelationInventoryAliases,
    class_row: &BTreeMap<String, SqlValue>,
    am_rows_by_oid: &BTreeMap<i64, BTreeMap<String, SqlValue>>,
    tablespace_rows_by_oid: &BTreeMap<i64, BTreeMap<String, SqlValue>>,
) -> SqlRow {
    let mut row = row_from_virtual_row("pg_class", &aliases.class, class_row.clone());
    row = merge_rows(
        &row,
        &row_from_virtual_row("pg_depend", &aliases.depend, pg_depend_null_row()),
    );
    row = merge_rows(
        &row,
        &row_from_virtual_row(
            "pg_tablespace",
            &aliases.tablespace,
            pg_dump_relation_inventory_tablespace_row(class_row, tablespace_rows_by_oid),
        ),
    );
    row = merge_rows(
        &row,
        &row_from_virtual_row(
            "pg_am",
            &aliases.access_method,
            pg_dump_relation_inventory_am_row(class_row, am_rows_by_oid),
        ),
    );
    merge_rows(
        &row,
        &row_from_virtual_row("pg_class", &aliases.toast_class, pg_class_toast_null_row()),
    )
}

pub(crate) fn pg_dump_relation_inventory_result_row(
    class_row: &BTreeMap<String, SqlValue>,
    am_rows_by_oid: &BTreeMap<i64, BTreeMap<String, SqlValue>>,
    tablespace_rows_by_oid: &BTreeMap<i64, BTreeMap<String, SqlValue>>,
) -> Vec<SqlValue> {
    let relkind = sql_value_text(&virtual_cell(class_row, "relkind")).unwrap_or_default();
    let relowner = virtual_cell(class_row, "relowner");
    let default_acl_kind = if relkind.eq_ignore_ascii_case("S") {
        "s"
    } else {
        "r"
    };
    let acldefault = eval_acldefault(&[
        SqlValue::String(default_acl_kind.to_string()),
        relowner.clone(),
    ])
    .unwrap_or(SqlValue::Null);
    let foreignserver = if relkind.eq_ignore_ascii_case("f") {
        SqlValue::Null
    } else {
        SqlValue::Int(0)
    };

    vec![
        SqlValue::Int(PG_CLASS_CATALOG_OID),
        virtual_cell(class_row, "oid"),
        virtual_cell(class_row, "relname"),
        virtual_cell(class_row, "relnamespace"),
        virtual_cell(class_row, "relkind"),
        virtual_cell(class_row, "reltype"),
        relowner,
        virtual_cell(class_row, "relchecks"),
        virtual_cell(class_row, "relhasindex"),
        virtual_cell(class_row, "relhasrules"),
        virtual_cell(class_row, "relpages"),
        virtual_cell(class_row, "reltuples"),
        virtual_cell(class_row, "relallvisible"),
        SqlValue::Int(0),
        virtual_cell(class_row, "relhastriggers"),
        virtual_cell(class_row, "relpersistence"),
        virtual_cell(class_row, "reloftype"),
        virtual_cell(class_row, "relacl"),
        acldefault,
        foreignserver,
        virtual_cell(class_row, "relfrozenxid"),
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        pg_dump_relation_inventory_tablespace_name(class_row, tablespace_rows_by_oid),
        SqlValue::Bool(false),
        virtual_cell(class_row, "relispopulated"),
        virtual_cell(class_row, "relreplident"),
        virtual_cell(class_row, "relrowsecurity"),
        virtual_cell(class_row, "relforcerowsecurity"),
        virtual_cell(class_row, "relminmxid"),
        SqlValue::Null,
        virtual_cell(class_row, "reloptions"),
        SqlValue::Null,
        pg_dump_relation_inventory_am_name(class_row, am_rows_by_oid),
        SqlValue::Bool(false),
        virtual_cell(class_row, "relispartition"),
    ]
}

pub(crate) fn pg_dump_relation_inventory_result_rows_direct(
    db: &BicDb,
    relkinds: &BTreeSet<String>,
) -> Result<Vec<Vec<SqlValue>>> {
    let table_oids = table_oids(db);
    let schemas = relation_schemas(db)?;
    let schema_by_name = schemas
        .iter()
        .map(|schema| (schema.name.to_ascii_lowercase(), schema))
        .collect::<BTreeMap<_, _>>();
    let view_relkinds = list_views(db)?
        .into_iter()
        .map(|view| {
            (
                view.name.to_ascii_lowercase(),
                if view.materialized { "m" } else { "v" },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let graph_names = graph_virtual_table_names()
        .into_iter()
        .collect::<BTreeSet<_>>();
    let privileges = list_privileges(db)?;
    let catalog_indexes = catalog_indexes(&schemas, db);
    let indexed_collections = catalog_indexes
        .iter()
        .map(|index| index.collection.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let trigger_tables = list_triggers(db)?
        .into_iter()
        .map(|trigger| trigger.table_name.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let mut rows = Vec::new();
    for table in catalog_table_names(db) {
        if table.starts_with("__bicdb_") {
            continue;
        }
        let table_key = table.to_ascii_lowercase();
        let table_schema = schema_by_name.get(&table_key).copied();
        let is_graph_view = graph_names.contains(table.as_str());
        if is_graph_view {
            continue;
        }
        let view_relkind = view_relkinds.get(&table_key).copied();
        let is_sql_view = view_relkind.is_some();
        let relkind = if is_graph_view {
            "v"
        } else if let Some(relkind) = view_relkind {
            relkind
        } else if table_schema
            .and_then(|schema| schema.partitioning.as_ref())
            .is_some()
        {
            "p"
        } else {
            "r"
        };
        if !catalog_relkind_matches(Some(relkinds), relkind) {
            continue;
        }
        let has_index = !is_graph_view
            && (table_schema
                .map(primary_key_columns_for_schema)
                .is_some_and(|columns| !columns.is_empty())
                || indexed_collections.contains(&table_key));
        let oid = *table_oids.get(&table).unwrap_or(&0);
        let relchecks = table_schema.map(check_constraint_count).unwrap_or(0);
        rows.push(pg_dump_relation_inventory_result_row_direct(
            oid,
            &table,
            table_schema
                .map(|schema| namespace_oid(&schema.schema_name))
                .unwrap_or(2200),
            relkind,
            has_index,
            is_sql_view,
            reltuples_for_relation(db, &table, false),
            relchecks,
            trigger_tables.contains(&table_key),
            table_schema
                .and_then(|schema| schema.partition_of.as_ref())
                .is_some(),
            table_acl_value_from_privileges(&privileges, &table)?,
            2,
            table_schema
                .map(|schema| schema.rls_enabled)
                .unwrap_or(false),
            table_schema
                .map(|schema| schema.rls_forced)
                .unwrap_or(false),
        ));
    }

    if catalog_relkind_matches(Some(relkinds), "S") {
        for sequence in list_sequences(db)? {
            rows.push(pg_dump_relation_inventory_result_row_direct(
                sequence_oid(&sequence.name),
                &sequence.name,
                2200,
                "S",
                false,
                false,
                1.0,
                0,
                false,
                false,
                SqlValue::Null,
                2,
                false,
                false,
            ));
        }
    }

    rows.sort_by(|left, right| {
        sql_value_i64(&left[1])
            .unwrap_or_default()
            .cmp(&sql_value_i64(&right[1]).unwrap_or_default())
    });
    Ok(rows)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn pg_dump_relation_inventory_result_row_direct(
    oid: i64,
    name: &str,
    relnamespace: i64,
    relkind: &str,
    relhasindex: bool,
    relhasrules: bool,
    reltuples: f64,
    relchecks: i64,
    relhastriggers: bool,
    relispartition: bool,
    relacl: SqlValue,
    relam: i64,
    relrowsecurity: bool,
    relforcerowsecurity: bool,
) -> Vec<SqlValue> {
    let relowner = SqlValue::Int(10);
    let default_acl_kind = if relkind.eq_ignore_ascii_case("S") {
        "s"
    } else {
        "r"
    };
    let acldefault = eval_acldefault(&[
        SqlValue::String(default_acl_kind.to_string()),
        relowner.clone(),
    ])
    .unwrap_or(SqlValue::Null);
    let foreignserver = if relkind.eq_ignore_ascii_case("f") {
        SqlValue::Null
    } else {
        SqlValue::Int(0)
    };

    vec![
        SqlValue::Int(PG_CLASS_CATALOG_OID),
        SqlValue::Int(oid),
        SqlValue::String(name.to_string()),
        SqlValue::Int(relnamespace),
        SqlValue::String(relkind.to_string()),
        SqlValue::Int(0),
        relowner,
        SqlValue::Int(relchecks),
        SqlValue::Bool(relhasindex),
        SqlValue::Bool(relhasrules),
        SqlValue::Int(0),
        SqlValue::Float(reltuples),
        SqlValue::Int(0),
        SqlValue::Int(0),
        SqlValue::Bool(relhastriggers),
        SqlValue::String("p".to_string()),
        SqlValue::Int(0),
        relacl,
        acldefault,
        foreignserver,
        SqlValue::Int(0),
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Bool(false),
        SqlValue::Bool(true),
        SqlValue::String("d".to_string()),
        SqlValue::Bool(relrowsecurity),
        SqlValue::Bool(relforcerowsecurity),
        SqlValue::Int(0),
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        pg_dump_relation_inventory_am_name_from_oid(relam),
        SqlValue::Bool(false),
        SqlValue::Bool(relispartition),
    ]
}

pub(crate) fn pg_dump_relation_inventory_am_name_from_oid(oid: i64) -> SqlValue {
    match oid {
        2 => SqlValue::String("heap".to_string()),
        403 => SqlValue::String("btree".to_string()),
        783 => SqlValue::String("gist".to_string()),
        2742 => SqlValue::String("gin".to_string()),
        _ => SqlValue::Null,
    }
}

pub(crate) fn pg_dump_relation_inventory_columns() -> Vec<String> {
    [
        "tableoid",
        "oid",
        "relname",
        "relnamespace",
        "relkind",
        "reltype",
        "relowner",
        "relchecks",
        "relhasindex",
        "relhasrules",
        "relpages",
        "reltuples",
        "relallvisible",
        "relallfrozen",
        "relhastriggers",
        "relpersistence",
        "reloftype",
        "relacl",
        "acldefault",
        "foreignserver",
        "relfrozenxid",
        "tfrozenxid",
        "toid",
        "toastpages",
        "toast_reloptions",
        "owning_tab",
        "owning_col",
        "reltablespace",
        "relhasoids",
        "relispopulated",
        "relreplident",
        "relrowsecurity",
        "relforcerowsecurity",
        "relminmxid",
        "tminmxid",
        "reloptions",
        "checkoption",
        "amname",
        "is_identity_sequence",
        "ispartition",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub(crate) fn pg_dump_relation_inventory_am_row(
    class_row: &BTreeMap<String, SqlValue>,
    am_rows_by_oid: &BTreeMap<i64, BTreeMap<String, SqlValue>>,
) -> BTreeMap<String, SqlValue> {
    sql_value_i64(&virtual_cell(class_row, "relam"))
        .and_then(|oid| am_rows_by_oid.get(&oid).cloned())
        .unwrap_or_else(pg_am_null_row)
}

pub(crate) fn pg_dump_relation_inventory_am_name(
    class_row: &BTreeMap<String, SqlValue>,
    am_rows_by_oid: &BTreeMap<i64, BTreeMap<String, SqlValue>>,
) -> SqlValue {
    sql_value_i64(&virtual_cell(class_row, "relam"))
        .and_then(|oid| am_rows_by_oid.get(&oid))
        .map(|row| virtual_cell(row, "amname"))
        .unwrap_or(SqlValue::Null)
}

pub(crate) fn pg_dump_relation_inventory_tablespace_row(
    class_row: &BTreeMap<String, SqlValue>,
    tablespace_rows_by_oid: &BTreeMap<i64, BTreeMap<String, SqlValue>>,
) -> BTreeMap<String, SqlValue> {
    sql_value_i64(&virtual_cell(class_row, "reltablespace"))
        .and_then(|oid| tablespace_rows_by_oid.get(&oid).cloned())
        .unwrap_or_else(pg_tablespace_null_row)
}

pub(crate) fn pg_dump_relation_inventory_tablespace_name(
    class_row: &BTreeMap<String, SqlValue>,
    tablespace_rows_by_oid: &BTreeMap<i64, BTreeMap<String, SqlValue>>,
) -> SqlValue {
    sql_value_i64(&virtual_cell(class_row, "reltablespace"))
        .and_then(|oid| tablespace_rows_by_oid.get(&oid))
        .map(|row| virtual_cell(row, "spcname"))
        .unwrap_or(SqlValue::Null)
}

pub(crate) fn pg_depend_null_row() -> BTreeMap<String, SqlValue> {
    virtual_row([
        ("classid", SqlValue::Null),
        ("objid", SqlValue::Null),
        ("objsubid", SqlValue::Null),
        ("refclassid", SqlValue::Null),
        ("refobjid", SqlValue::Null),
        ("refobjsubid", SqlValue::Null),
        ("deptype", SqlValue::Null),
    ])
}

pub(crate) fn pg_am_null_row() -> BTreeMap<String, SqlValue> {
    virtual_row([
        ("oid", SqlValue::Null),
        ("amname", SqlValue::Null),
        ("amhandler", SqlValue::Null),
        ("amtype", SqlValue::Null),
    ])
}

pub(crate) fn pg_tablespace_null_row() -> BTreeMap<String, SqlValue> {
    virtual_row([
        ("oid", SqlValue::Null),
        ("spcname", SqlValue::Null),
        ("spcowner", SqlValue::Null),
        ("spcacl", SqlValue::Null),
        ("spcoptions", SqlValue::Null),
    ])
}

pub(crate) fn pg_class_toast_null_row() -> BTreeMap<String, SqlValue> {
    virtual_row([
        ("oid", SqlValue::Null),
        ("relkind", SqlValue::Null),
        ("relfrozenxid", SqlValue::Null),
        ("relpages", SqlValue::Null),
        ("reloptions", SqlValue::Null),
        ("relminmxid", SqlValue::Null),
    ])
}

pub(crate) fn pg_class_namespace_summary_path_allowed(
    selection: Option<&Expr>,
    projection: Option<&[SelectItem]>,
    order_by: Option<&OrderBy>,
    class_alias: &str,
    namespace_alias: &str,
) -> bool {
    let Some(projection) = projection else {
        return false;
    };
    let allowed = pg_class_namespace_summary_allowed_columns(class_alias, namespace_alias);
    if selection.is_some_and(|expr| !expr_references_only_allowed_columns(expr, &allowed)) {
        return false;
    }
    if projection
        .iter()
        .any(|item| !select_item_references_only_allowed_columns(item, &allowed))
    {
        return false;
    }
    order_by.is_none_or(|order_by| order_by_references_only_allowed_columns(order_by, &allowed))
}

pub(crate) fn active_record_table_name_aliases(
    from: &TableWithJoins,
) -> Result<Option<(String, String)>> {
    let Some((_, class_alias)) = table_factor_relation_alias(&from.relation, "pg_class")? else {
        return Ok(None);
    };
    let [join] = from.joins.as_slice() else {
        return Ok(None);
    };
    if !is_left_join_operator(&join.join_operator) && !is_reorderable_inner_join(join) {
        return Ok(None);
    }
    let Some(constraint) = join_operator_constraint(&join.join_operator) else {
        return Ok(None);
    };
    let Some((_, namespace_alias)) = table_factor_relation_alias(&join.relation, "pg_namespace")?
    else {
        return Ok(None);
    };
    if !join_constraint_has_column_equality(
        constraint,
        &class_alias,
        "pg_class",
        "relnamespace",
        &namespace_alias,
        "pg_namespace",
        "oid",
    ) {
        return Ok(None);
    }
    Ok(Some((class_alias, namespace_alias)))
}

pub(crate) fn active_record_table_name_projection_matches(
    projection: &[SelectItem],
    class_alias: &str,
) -> bool {
    let [item] = projection else {
        return false;
    };
    match item {
        SelectItem::UnnamedExpr(expr) => {
            relation_oid_column_matches(expr, class_alias, "pg_class", &["relname"])
        }
        _ => false,
    }
}

pub(crate) fn active_record_table_name_order_by_matches(
    order_by: Option<&OrderBy>,
    class_alias: &str,
) -> bool {
    let Some(order_by) = order_by else {
        return true;
    };
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return false;
    };
    expressions.iter().all(|order| {
        order.options.asc != Some(false)
            && relation_oid_column_matches(&order.expr, class_alias, "pg_class", &["relname"])
    })
}

pub(crate) fn pg_class_namespace_summary_selection_covered(
    selection: Option<&Expr>,
    class_alias: &str,
    namespace_alias: &str,
) -> Result<bool> {
    let Some(selection) = selection else {
        return Ok(false);
    };
    let terms = and_terms(selection);
    if terms.is_empty() {
        return Ok(false);
    }
    for term in terms {
        if string_filter_values_from_term(term, class_alias, "pg_class", &["relname"])?.is_some()
            || string_filter_values_from_term(term, class_alias, "pg_class", &["relkind"])?
                .is_some()
            || string_filter_values_from_term(term, namespace_alias, "pg_namespace", &["nspname"])?
                .is_some()
            || regex_exact_values_from_term(term, namespace_alias, "pg_namespace", &["nspname"])?
                .is_some()
        {
            continue;
        }
        return Ok(false);
    }
    Ok(true)
}

pub(crate) fn pg_class_namespace_summary_allowed_columns(
    class_alias: &str,
    namespace_alias: &str,
) -> BTreeSet<String> {
    let mut columns = BTreeSet::new();
    for qualifier in [class_alias, "pg_class"] {
        for field in ["oid", "relname", "relnamespace", "relkind"] {
            columns.insert(format!("{}.{}", qualifier.to_ascii_lowercase(), field));
        }
    }
    for qualifier in [namespace_alias, "pg_namespace"] {
        for field in ["oid", "nspname"] {
            columns.insert(format!("{}.{}", qualifier.to_ascii_lowercase(), field));
        }
    }
    for field in ["oid", "relname", "relnamespace", "relkind", "nspname"] {
        columns.insert(field.to_string());
    }
    columns
}

pub(crate) fn select_item_references_only_allowed_columns(
    item: &SelectItem,
    allowed: &BTreeSet<String>,
) -> bool {
    match item {
        SelectItem::UnnamedExpr(expr)
        | SelectItem::ExprWithAlias { expr, .. }
        | SelectItem::ExprWithAliases { expr, .. } => {
            expr_references_only_allowed_columns(expr, allowed)
        }
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => false,
    }
}

pub(crate) fn order_by_references_only_allowed_columns(
    order_by: &OrderBy,
    allowed: &BTreeSet<String>,
) -> bool {
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return false;
    };
    expressions
        .iter()
        .all(|order| expr_references_only_allowed_columns(&order.expr, allowed))
}

pub(crate) fn expr_references_only_allowed_columns(
    expr: &Expr,
    allowed: &BTreeSet<String>,
) -> bool {
    if row_independent_expr(expr) {
        return true;
    }
    match expr {
        Expr::Identifier(ident) => allowed.contains(&ident.value.to_ascii_lowercase()),
        Expr::CompoundIdentifier(idents) => {
            if idents.len() < 2 {
                return false;
            }
            let qualified = format!(
                "{}.{}",
                idents[idents.len() - 2].value.to_ascii_lowercase(),
                idents[idents.len() - 1].value.to_ascii_lowercase()
            );
            allowed.contains(&qualified)
        }
        Expr::BinaryOp { left, right, .. }
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right) => {
            expr_references_only_allowed_columns(left, allowed)
                && expr_references_only_allowed_columns(right, allowed)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::Collate { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsTrue(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::IsNotFalse(expr)
        | Expr::IsUnknown(expr)
        | Expr::IsNotUnknown(expr) => expr_references_only_allowed_columns(expr, allowed),
        Expr::Function(function) => function_args(function)
            .iter()
            .all(|arg| expr_references_only_allowed_columns(arg, allowed)),
        Expr::InList { expr, list, .. } => {
            expr_references_only_allowed_columns(expr, allowed)
                && list
                    .iter()
                    .all(|item| expr_references_only_allowed_columns(item, allowed))
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_references_only_allowed_columns(expr, allowed)
                && expr_references_only_allowed_columns(low, allowed)
                && expr_references_only_allowed_columns(high, allowed)
        }
        Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
            expr_references_only_allowed_columns(left, allowed)
                && expr_references_only_allowed_columns(right, allowed)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            expr_references_only_allowed_columns(expr, allowed)
                && expr_references_only_allowed_columns(pattern, allowed)
        }
        Expr::Value(_) | Expr::TypedString(_) => true,
        _ => false,
    }
}

pub(crate) fn pg_class_namespace_summary_join_rows(
    db: &BicDb,
    class_alias: &str,
    namespace_alias: &str,
    relnames: Option<&BTreeSet<String>>,
    relkinds: Option<&BTreeSet<String>>,
    schema_names: Option<&BTreeSet<String>>,
) -> Result<Vec<SqlRow>> {
    let summaries = list_table_catalog_summaries(db)?
        .into_iter()
        .map(|summary| (summary.name.to_ascii_lowercase(), summary))
        .collect::<BTreeMap<_, _>>();
    let view_relkinds = list_views(db)?
        .into_iter()
        .map(|view| {
            (
                view.name.to_ascii_lowercase(),
                if view.materialized { "m" } else { "v" },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let graph_views = graph_virtual_table_names()
        .into_iter()
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
    let mut rows = Vec::new();
    let relation_oids = table_oids(db);
    for relation in catalog_table_names(db) {
        if !catalog_name_matches(relnames, &relation) {
            continue;
        }
        let relation_key = relation.to_ascii_lowercase();
        let summary = summaries.get(&relation_key);
        let schema_name = summary
            .map(|summary| summary.schema_name.as_str())
            .unwrap_or_else(|| relation_schema_and_name(&relation).0);
        if !catalog_name_matches(schema_names, schema_name) {
            continue;
        }
        let relkind = if graph_views.contains(&relation_key) {
            "v"
        } else if let Some(relkind) = view_relkinds.get(&relation_key).copied() {
            relkind
        } else if summary.is_some_and(|summary| summary.partitioned) {
            "p"
        } else {
            "r"
        };
        if !catalog_relkind_matches(relkinds, relkind) {
            continue;
        }
        rows.push(pg_class_namespace_join_row(
            class_alias,
            namespace_alias,
            pg_class_summary_row(
                relation_oids.get(&relation).copied().unwrap_or_default(),
                &relation,
                schema_name,
                relkind,
            ),
            pg_namespace_summary_row(schema_name),
        ));
    }
    for sequence in list_sequences(db)? {
        if !catalog_name_matches(relnames, &sequence.name) {
            continue;
        }
        if !catalog_name_matches(schema_names, "public") || !catalog_relkind_matches(relkinds, "S")
        {
            continue;
        }
        rows.push(pg_class_namespace_join_row(
            class_alias,
            namespace_alias,
            pg_class_summary_row(sequence_oid(&sequence.name), &sequence.name, "public", "S"),
            pg_namespace_summary_row("public"),
        ));
    }
    Ok(rows)
}

pub(crate) fn active_record_table_name_rows(
    db: &BicDb,
    relnames: Option<&BTreeSet<String>>,
    relkinds: Option<&BTreeSet<String>>,
    schema_names: Option<&BTreeSet<String>>,
) -> Result<Vec<Vec<SqlValue>>> {
    let summaries = list_table_catalog_summaries(db)?
        .into_iter()
        .map(|summary| (summary.name.to_ascii_lowercase(), summary))
        .collect::<BTreeMap<_, _>>();
    let view_relkinds = list_views(db)?
        .into_iter()
        .map(|view| {
            (
                view.name.to_ascii_lowercase(),
                if view.materialized { "m" } else { "v" },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let graph_views = graph_virtual_table_names()
        .into_iter()
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
    let mut rows = Vec::new();
    for relation in catalog_table_names(db) {
        if !catalog_name_matches(relnames, &relation) {
            continue;
        }
        let relation_key = relation.to_ascii_lowercase();
        let summary = summaries.get(&relation_key);
        let schema_name = summary
            .map(|summary| summary.schema_name.as_str())
            .unwrap_or_else(|| relation_schema_and_name(&relation).0);
        if !catalog_name_matches(schema_names, schema_name) {
            continue;
        }
        let relkind = if graph_views.contains(&relation_key) {
            "v"
        } else if let Some(relkind) = view_relkinds.get(&relation_key).copied() {
            relkind
        } else if summary.is_some_and(|summary| summary.partitioned) {
            "p"
        } else {
            "r"
        };
        if !catalog_relkind_matches(relkinds, relkind) {
            continue;
        }
        rows.push(vec![SqlValue::String(relation)]);
    }
    Ok(rows)
}

pub(crate) fn pg_class_summary_row(
    oid: i64,
    relation: &str,
    schema_name: &str,
    relkind: &str,
) -> BTreeMap<String, SqlValue> {
    virtual_row([
        ("oid", SqlValue::Int(oid)),
        ("relname", SqlValue::String(relation.to_string())),
        ("relnamespace", SqlValue::Int(namespace_oid(schema_name))),
        ("relkind", SqlValue::String(relkind.to_string())),
    ])
}

pub(crate) fn pg_namespace_summary_row(schema_name: &str) -> BTreeMap<String, SqlValue> {
    virtual_row([
        ("oid", SqlValue::Int(namespace_oid(schema_name))),
        ("nspname", SqlValue::String(schema_name.to_string())),
    ])
}

pub(crate) fn pg_constraint_foreign_key_join_columns(
    constraint_alias: &str,
    constrained_class_alias: &str,
    referenced_class_alias: &str,
    constrained_attribute_alias: Option<&str>,
    referenced_attribute_alias: Option<&str>,
    namespace_alias: Option<&str>,
) -> Vec<String> {
    let mut columns = aliased_virtual_columns("pg_constraint", constraint_alias);
    columns.extend(aliased_virtual_columns("pg_class", constrained_class_alias));
    columns.extend(aliased_virtual_columns("pg_class", referenced_class_alias));
    if let Some(alias) = constrained_attribute_alias {
        columns.extend(aliased_virtual_columns("pg_attribute", alias));
    }
    if let Some(alias) = referenced_attribute_alias {
        columns.extend(aliased_virtual_columns("pg_attribute", alias));
    }
    if let Some(alias) = namespace_alias {
        columns.extend(aliased_virtual_columns("pg_namespace", alias));
    }
    columns
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn pg_constraint_foreign_key_join_row(
    constraint_alias: &str,
    constrained_class_alias: &str,
    referenced_class_alias: &str,
    constrained_attribute_alias: Option<&str>,
    referenced_attribute_alias: Option<&str>,
    namespace_alias: Option<&str>,
    constraint_row: &BTreeMap<String, SqlValue>,
    constrained_class_row: BTreeMap<String, SqlValue>,
    referenced_class_row: BTreeMap<String, SqlValue>,
    constrained_attribute_row: Option<BTreeMap<String, SqlValue>>,
    referenced_attribute_row: Option<BTreeMap<String, SqlValue>>,
    namespace_row: Option<BTreeMap<String, SqlValue>>,
) -> SqlRow {
    let mut row = row_from_virtual_row("pg_constraint", constraint_alias, constraint_row.clone());
    row = merge_rows(
        &row,
        &row_from_virtual_row("pg_class", constrained_class_alias, constrained_class_row),
    );
    row = merge_rows(
        &row,
        &row_from_virtual_row("pg_class", referenced_class_alias, referenced_class_row),
    );
    if let (Some(alias), Some(attribute_row)) =
        (constrained_attribute_alias, constrained_attribute_row)
    {
        row = merge_rows(
            &row,
            &row_from_virtual_row("pg_attribute", alias, attribute_row),
        );
    }
    if let (Some(alias), Some(attribute_row)) =
        (referenced_attribute_alias, referenced_attribute_row)
    {
        row = merge_rows(
            &row,
            &row_from_virtual_row("pg_attribute", alias, attribute_row),
        );
    }
    if let (Some(alias), Some(namespace_row)) = (namespace_alias, namespace_row) {
        row = merge_rows(
            &row,
            &row_from_virtual_row("pg_namespace", alias, namespace_row),
        );
    }
    row
}

pub(crate) fn pg_constraint_class_namespace_join_columns(
    constraint_alias: &str,
    class_alias: &str,
    namespace_alias: Option<&str>,
) -> Vec<String> {
    let mut columns = aliased_virtual_columns("pg_constraint", constraint_alias);
    columns.extend(aliased_virtual_columns("pg_class", class_alias));
    if let Some(alias) = namespace_alias {
        columns.extend(aliased_virtual_columns("pg_namespace", alias));
    }
    columns
}

pub(crate) fn pg_constraint_class_namespace_join_row(
    constraint_alias: &str,
    class_alias: &str,
    namespace_alias: Option<&str>,
    constraint_row: &BTreeMap<String, SqlValue>,
    class_row: BTreeMap<String, SqlValue>,
    namespace_row: Option<BTreeMap<String, SqlValue>>,
) -> SqlRow {
    let mut row = row_from_virtual_row("pg_constraint", constraint_alias, constraint_row.clone());
    row = merge_rows(
        &row,
        &row_from_virtual_row("pg_class", class_alias, class_row),
    );
    if let (Some(alias), Some(namespace_row)) = (namespace_alias, namespace_row) {
        row = merge_rows(
            &row,
            &row_from_virtual_row("pg_namespace", alias, namespace_row),
        );
    }
    row
}

pub(crate) fn pg_locks_class_join_columns(locks_alias: &str, class_alias: &str) -> Vec<String> {
    let mut columns = aliased_virtual_columns("pg_locks", locks_alias);
    columns.extend(aliased_virtual_columns("pg_class", class_alias));
    columns
}

pub(crate) fn index_oids_for_name_filters(
    db: &BicDb,
    names: Option<&BTreeSet<String>>,
    schemas: Option<&BTreeSet<String>>,
) -> Result<Option<BTreeSet<i64>>> {
    if names.is_none() && schemas.is_none() {
        return Ok(None);
    }

    let table_oids = table_oids(db);
    let relation_schemas = relation_schemas(db)?;
    let mut oids = BTreeSet::new();

    for (table, _primary_key) in primary_key_indexes(&relation_schemas, db) {
        let index_name = primary_key_constraint_name(&relation_schemas, &table);
        let schema_name = schema_name_for_relation(&relation_schemas, &table);
        if catalog_name_matches(names, &index_name) && catalog_name_matches(schemas, &schema_name) {
            let table_oid = *table_oids.get(&table).unwrap_or(&0);
            oids.insert(primary_index_oid(table_oid));
        }
    }

    for index in catalog_indexes(&relation_schemas, db) {
        if catalog_name_matches(names, &index.name)
            && catalog_name_matches(schemas, &index.schema_name)
        {
            oids.insert(secondary_index_oid(&index.schema_name, &index.name));
        }
    }

    Ok(Some(oids))
}

pub(crate) fn pg_class_catalog_rows_by_oid(
    db: &BicDb,
    oids: &BTreeSet<i64>,
) -> Result<BTreeMap<i64, BTreeMap<String, SqlValue>>> {
    if oids.is_empty() {
        return Ok(BTreeMap::new());
    }
    Ok(pg_class_rows_filtered(db, None, None, Some(oids))?
        .into_iter()
        .filter_map(|row| sql_value_i64(&virtual_cell(&row, "oid")).map(|oid| (oid, row)))
        .collect())
}

pub(crate) fn pg_attribute_catalog_row_for_attnum(
    db: &BicDb,
    attrelid: i64,
    attnum: i64,
) -> Result<Option<BTreeMap<String, SqlValue>>> {
    Ok(pg_attribute_rows_for_attrelid(db, attrelid)?
        .into_iter()
        .find(|row| sql_value_i64(&virtual_cell(row, "attnum")) == Some(attnum)))
}

pub(crate) fn pg_namespace_catalog_rows_by_oid(
    db: &BicDb,
) -> Result<BTreeMap<i64, BTreeMap<String, SqlValue>>> {
    Ok(virtual_rows(db, "pg_namespace")?
        .into_iter()
        .filter_map(|row| sql_value_i64(&virtual_cell(&row, "oid")).map(|oid| (oid, row)))
        .collect())
}

pub(crate) fn cte_key(name: &str) -> String {
    name.to_ascii_lowercase()
}
