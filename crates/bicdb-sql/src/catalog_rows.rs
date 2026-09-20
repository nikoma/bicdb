//! pg_catalog/information_schema row builders: pg_attribute, pg_type, pg_proc, pg_index(es), pg_roles, pg_policy, pg_settings, pg_stat_*, bicdb_* status rows, plus ActiveRecord introspection fast paths and catalog view filter types.
//!
//! Extracted verbatim from `lib.rs` (phase-1 mechanical module split).
//! Moved items were bumped to `pub(crate)` so existing call sites keep
//! resolving; `lib.rs` re-exports this module via `pub use catalog_rows::*;`.

// Glue import: bring every crate-root item (including the root's private
// imports and the other split modules' re-exports) into scope so the moved
// code compiles unchanged.
use crate::*;

pub(crate) fn information_schema_check_constraints(
    db: &BicDb,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    Ok(list_schemas(db)?
        .into_iter()
        .flat_map(|schema| {
            schema.constraints.into_iter().filter_map(|constraint| {
                let ConstraintSchema::Check {
                    name, expression, ..
                } = constraint
                else {
                    return None;
                };
                Some(virtual_row([
                    ("constraint_catalog", SqlValue::String("bicdb".to_string())),
                    ("constraint_schema", SqlValue::String("public".to_string())),
                    ("constraint_name", SqlValue::String(name)),
                    ("check_clause", SqlValue::String(expression)),
                ]))
            })
        })
        .collect())
}

pub(crate) fn constraint_column_usage_row(
    schema: &TableSchema,
    constraint: &str,
    column: &str,
) -> BTreeMap<String, SqlValue> {
    virtual_row([
        ("table_catalog", SqlValue::String("bicdb".to_string())),
        ("table_schema", SqlValue::String(schema.schema_name.clone())),
        ("table_name", SqlValue::String(schema.name.clone())),
        ("column_name", SqlValue::String(column.to_string())),
        ("constraint_catalog", SqlValue::String("bicdb".to_string())),
        (
            "constraint_schema",
            SqlValue::String(schema.schema_name.clone()),
        ),
        ("constraint_name", SqlValue::String(constraint.to_string())),
    ])
}

pub(crate) fn check_constraint_referenced_columns(
    schema: &TableSchema,
    expression: &str,
) -> Vec<String> {
    let visible_columns = schema
        .columns
        .iter()
        .filter(|column| !column.hidden)
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let column_names = visible_columns
        .iter()
        .map(|column| column.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();

    let mut references = Vec::new();
    if let Ok(statements) = parse_statements(&format!("SELECT 1 WHERE {expression}")) {
        if let Some(selection) = statements.first().and_then(|statement| match statement {
            Statement::Query(query) => match query.body.as_ref() {
                SetExpr::Select(select) => select.selection.as_ref(),
                _ => None,
            },
            _ => None,
        }) {
            collect_constraint_column_references(selection, &mut references);
        }
    }

    let mut columns = references
        .into_iter()
        .filter_map(|reference| {
            reference
                .last()
                .map(|column| normalize_object_name(column))
                .filter(|column| column_names.contains(column))
        })
        .collect::<BTreeSet<_>>();

    if columns.is_empty() {
        for column in &visible_columns {
            if expression_mentions_identifier(expression, column) {
                columns.insert(column.to_ascii_lowercase());
            }
        }
    }

    visible_columns
        .into_iter()
        .filter(|column| columns.contains(&column.to_ascii_lowercase()))
        .collect()
}

pub(crate) fn collect_constraint_column_references(expr: &Expr, references: &mut Vec<Vec<String>>) {
    match expr {
        Expr::Identifier(ident) => references.push(vec![ident.value.clone()]),
        Expr::CompoundIdentifier(idents) => {
            references.push(idents.iter().map(|ident| ident.value.clone()).collect());
        }
        Expr::Function(function) => {
            for arg in function_args(function) {
                collect_constraint_column_references(&arg, references);
            }
        }
        Expr::BinaryOp { left, right, .. }
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right) => {
            collect_constraint_column_references(left, references);
            collect_constraint_column_references(right, references);
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::Cast { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsTrue(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::IsNotFalse(expr)
        | Expr::IsUnknown(expr)
        | Expr::IsNotUnknown(expr)
        | Expr::Extract { expr, .. }
        | Expr::Collate { expr, .. }
        | Expr::Ceil { expr, .. }
        | Expr::Floor { expr, .. }
        | Expr::Prefixed { value: expr, .. }
        | Expr::Named { expr, .. }
        | Expr::OuterJoin(expr)
        | Expr::Prior(expr) => collect_constraint_column_references(expr, references),
        Expr::Position { expr, r#in } => {
            collect_constraint_column_references(expr, references);
            collect_constraint_column_references(r#in, references);
        }
        Expr::InList { expr, list, .. } => {
            collect_constraint_column_references(expr, references);
            for item in list {
                collect_constraint_column_references(item, references);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_constraint_column_references(expr, references);
            collect_constraint_column_references(low, references);
            collect_constraint_column_references(high, references);
        }
        Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
            collect_constraint_column_references(left, references);
            collect_constraint_column_references(right, references);
        }
        Expr::Like { expr, pattern, .. }
        | Expr::ILike { expr, pattern, .. }
        | Expr::SimilarTo { expr, pattern, .. }
        | Expr::RLike { expr, pattern, .. } => {
            collect_constraint_column_references(expr, references);
            collect_constraint_column_references(pattern, references);
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                collect_constraint_column_references(operand, references);
            }
            for condition in conditions {
                collect_constraint_column_references(&condition.condition, references);
                collect_constraint_column_references(&condition.result, references);
            }
            if let Some(else_result) = else_result {
                collect_constraint_column_references(else_result, references);
            }
        }
        Expr::Array(array) => {
            for expr in &array.elem {
                collect_constraint_column_references(expr, references);
            }
        }
        Expr::CompoundFieldAccess { root, access_chain } => {
            collect_constraint_column_references(root, references);
            for access in access_chain {
                match access {
                    AccessExpr::Dot(expr) => collect_constraint_column_references(expr, references),
                    AccessExpr::Subscript(subscript) => {
                        collect_constraint_subscript_references(subscript, references);
                    }
                }
            }
        }
        Expr::JsonAccess { value, .. } => {
            collect_constraint_column_references(value, references);
        }
        Expr::Interval(interval) => {
            collect_constraint_column_references(&interval.value, references);
        }
        Expr::Trim {
            trim_what,
            expr,
            trim_characters,
            ..
        } => {
            collect_constraint_column_references(expr, references);
            if let Some(trim_what) = trim_what {
                collect_constraint_column_references(trim_what, references);
            }
            if let Some(trim_characters) = trim_characters {
                for character in trim_characters {
                    collect_constraint_column_references(character, references);
                }
            }
        }
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            collect_constraint_column_references(expr, references);
            if let Some(substring_from) = substring_from {
                collect_constraint_column_references(substring_from, references);
            }
            if let Some(substring_for) = substring_for {
                collect_constraint_column_references(substring_for, references);
            }
        }
        Expr::Overlay {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
        } => {
            collect_constraint_column_references(expr, references);
            collect_constraint_column_references(overlay_what, references);
            collect_constraint_column_references(overlay_from, references);
            if let Some(overlay_for) = overlay_for {
                collect_constraint_column_references(overlay_for, references);
            }
        }
        Expr::Tuple(exprs) => {
            for expr in exprs {
                collect_constraint_column_references(expr, references);
            }
        }
        Expr::Struct { values, .. } => {
            for expr in values {
                collect_constraint_column_references(expr, references);
            }
        }
        Expr::Convert { expr, styles, .. } => {
            collect_constraint_column_references(expr, references);
            for style in styles {
                collect_constraint_column_references(style, references);
            }
        }
        Expr::AtTimeZone {
            timestamp,
            time_zone,
        } => {
            collect_constraint_column_references(timestamp, references);
            collect_constraint_column_references(time_zone, references);
        }
        Expr::GroupingSets(groups) | Expr::Cube(groups) | Expr::Rollup(groups) => {
            for expr in groups.iter().flatten() {
                collect_constraint_column_references(expr, references);
            }
        }
        Expr::Lambda(lambda) => collect_constraint_column_references(&lambda.body, references),
        Expr::Value(_)
        | Expr::TypedString(_)
        | Expr::Subquery(_)
        | Expr::InSubquery { .. }
        | Expr::InUnnest { .. }
        | Expr::Exists { .. }
        | Expr::Dictionary(_)
        | Expr::Map(_)
        | Expr::MatchAgainst { .. }
        | Expr::Wildcard(_)
        | Expr::QualifiedWildcard(_, _)
        | Expr::MemberOf(_) => {}
        _ => {}
    }
}

pub(crate) fn collect_constraint_subscript_references(
    subscript: &Subscript,
    references: &mut Vec<Vec<String>>,
) {
    match subscript {
        Subscript::Index { index } => collect_constraint_column_references(index, references),
        Subscript::Slice {
            lower_bound,
            upper_bound,
            stride,
        } => {
            if let Some(lower_bound) = lower_bound {
                collect_constraint_column_references(lower_bound, references);
            }
            if let Some(upper_bound) = upper_bound {
                collect_constraint_column_references(upper_bound, references);
            }
            if let Some(stride) = stride {
                collect_constraint_column_references(stride, references);
            }
        }
    }
}

pub(crate) fn expression_mentions_identifier(expression: &str, identifier: &str) -> bool {
    expression
        .split(|ch: char| !is_ident_char(ch))
        .any(|token| token.eq_ignore_ascii_case(identifier))
}

pub(crate) fn table_constraint_row(
    table: &str,
    name: &str,
    constraint_type: &str,
) -> BTreeMap<String, SqlValue> {
    virtual_row([
        ("constraint_catalog", SqlValue::String("bicdb".to_string())),
        ("constraint_schema", SqlValue::String("public".to_string())),
        ("constraint_name", SqlValue::String(name.to_string())),
        ("table_schema", SqlValue::String("public".to_string())),
        ("table_name", SqlValue::String(table.to_string())),
        (
            "constraint_type",
            SqlValue::String(constraint_type.to_string()),
        ),
        ("is_deferrable", SqlValue::String("NO".to_string())),
        ("initially_deferred", SqlValue::String("NO".to_string())),
        ("enforced", SqlValue::String("YES".to_string())),
    ])
}

pub(crate) fn not_null_constraint_name(table: &str, column: &str) -> String {
    format!("{table}_{column}_not_null")
}

pub(crate) fn key_column_usage_row(
    table: &str,
    constraint: &str,
    column: &str,
    ordinal: i64,
) -> BTreeMap<String, SqlValue> {
    virtual_row([
        ("constraint_catalog", SqlValue::String("bicdb".to_string())),
        ("constraint_schema", SqlValue::String("public".to_string())),
        ("constraint_name", SqlValue::String(constraint.to_string())),
        ("table_catalog", SqlValue::String("bicdb".to_string())),
        ("table_schema", SqlValue::String("public".to_string())),
        ("table_name", SqlValue::String(table.to_string())),
        ("column_name", SqlValue::String(column.to_string())),
        ("ordinal_position", SqlValue::Int(ordinal)),
        ("position_in_unique_constraint", SqlValue::Null),
    ])
}

pub(crate) fn information_schema_sequences(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let mut rows = Vec::new();
    for sequence in list_sequences(db)? {
        let identity_owned = match (&sequence.owned_by_table, &sequence.owned_by_column) {
            (Some(table), Some(column)) => load_schema(db, table)?
                .as_ref()
                .and_then(|schema| schema.column(column))
                .is_some_and(|column| column.identity.is_some()),
            _ => false,
        };
        if identity_owned {
            continue;
        }
        rows.push(virtual_row([
            ("sequence_catalog", SqlValue::String("bicdb".to_string())),
            ("sequence_schema", SqlValue::String("public".to_string())),
            ("sequence_name", SqlValue::String(sequence.name)),
            (
                "data_type",
                SqlValue::String(sequence_data_type_name(&sequence.data_type).to_string()),
            ),
            (
                "start_value",
                SqlValue::String(sequence.start_value.to_string()),
            ),
            (
                "minimum_value",
                SqlValue::String(sequence.min_value.to_string()),
            ),
            (
                "maximum_value",
                SqlValue::String(sequence.max_value.to_string()),
            ),
            (
                "increment",
                SqlValue::String(sequence.increment_by.to_string()),
            ),
            (
                "cycle_option",
                SqlValue::String(if sequence.cycle { "YES" } else { "NO" }.to_string()),
            ),
        ]));
    }
    Ok(rows)
}

pub(crate) fn pg_class_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    pg_class_rows_filtered(db, None, None, None)
}

pub(crate) fn pg_class_rows_filtered(
    db: &BicDb,
    names: Option<&BTreeSet<String>>,
    kinds: Option<&BTreeSet<String>>,
    oids: Option<&BTreeSet<i64>>,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let table_oids = table_oids(db);
    let relation_names_for_oids = relation_names_for_oid_filter(&table_oids, oids);
    let oid_filter_is_only_relations = oids.is_some_and(|oids| {
        relation_names_for_oids
            .as_ref()
            .is_some_and(|relations| relations.len() == oids.len())
    });
    let schemas = if oid_filter_is_only_relations {
        load_relation_schemas_by_name(db, relation_names_for_oids.as_ref().unwrap())?
    } else {
        relation_schemas(db)?
    };
    let schema_by_name = schemas
        .iter()
        .map(|schema| (schema.name.to_ascii_lowercase(), schema))
        .collect::<BTreeMap<_, _>>();
    let views = list_views(db)?;
    let view_relkinds = views
        .iter()
        .map(|view| {
            (
                view.name.to_ascii_lowercase(),
                if view.materialized { "m" } else { "v" },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let view_owners = views
        .iter()
        .map(|view| {
            (
                view.name.to_ascii_lowercase(),
                view.owner.clone().unwrap_or_else(current_role_name),
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
    let triggers = list_triggers(db)?;
    let partition_parent_names = schemas
        .iter()
        .filter_map(|schema| {
            schema
                .partition_of
                .as_ref()
                .map(|partition_of| partition_of.parent_table.to_ascii_lowercase())
        })
        .collect::<BTreeSet<_>>();
    let mut rows = Vec::new();
    let exact_counts = names.is_some() || oids.is_some() || kinds.is_none();
    let catalog_names = if oid_filter_is_only_relations {
        relation_names_for_oids
            .as_ref()
            .unwrap()
            .iter()
            .cloned()
            .collect::<Vec<_>>()
    } else {
        catalog_table_names(db)
    };

    for table in catalog_names {
        let table_key = table.to_ascii_lowercase();
        let table_schema = schema_by_name.get(&table_key).copied();
        let is_graph_view = graph_names.contains(table.as_str());
        let view_relkind = view_relkinds.get(&table_key).copied();
        let is_sql_view = view_relkind.is_some();
        let columns = if is_graph_view {
            graph_virtual_table_columns(&table)
        } else if let Some(schema) = table_schema {
            schema
                .columns
                .iter()
                .filter(|column| !column.hidden)
                .cloned()
                .collect()
        } else {
            default_record_columns()
        };
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
        if !catalog_name_matches(names, &table) || !catalog_relkind_matches(kinds, relkind) {
            continue;
        }
        let has_index = !is_graph_view
            && !is_sql_view
            && (table_schema
                .map(primary_key_columns_for_schema)
                .is_some_and(|columns| !columns.is_empty())
                || indexed_collections.contains(&table_key));
        let oid = *table_oids.get(&table).unwrap_or(&0);
        if oids.is_some_and(|oids| !oids.contains(&oid)) {
            continue;
        }
        let relchecks = table_schema.map(check_constraint_count).unwrap_or(0);
        let has_triggers = triggers
            .iter()
            .any(|trigger| trigger.table_name.eq_ignore_ascii_case(&table));
        let mut row = pg_class_row(
            oid,
            &table,
            table_schema
                .map(|schema| namespace_oid(&schema.schema_name))
                .unwrap_or(2200),
            table_schema.map(TableSchema::row_type_oid),
            relkind,
            columns.len() as i64,
            has_index,
            is_sql_view,
            reltuples_for_relation(db, &table, exact_counts),
            relchecks,
            has_triggers,
            partition_parent_names.contains(&table.to_ascii_lowercase()),
            table_schema
                .and_then(|schema| schema.partition_of.as_ref())
                .is_some(),
            table_schema
                .and_then(|schema| schema.partition_of.as_ref())
                .map(|partition_of| SqlValue::String(partition_of.bound.clone()))
                .unwrap_or(SqlValue::Null),
            table_acl_value_from_privileges(&privileges, &table)?,
            2,
            table_schema
                .map(|schema| schema.rls_enabled)
                .unwrap_or(false),
            table_schema
                .map(|schema| schema.rls_forced)
                .unwrap_or(false),
        );
        let owner = table_schema
            .and_then(|schema| schema.owner.clone())
            .or_else(|| view_owners.get(&table_key).cloned())
            .unwrap_or_else(current_role_name);
        row.insert("relowner".to_string(), SqlValue::Int(role_oid(&owner)));
        rows.push(row);
    }

    for sequence in list_sequences(db)? {
        if !catalog_name_matches(names, &sequence.name) || !catalog_relkind_matches(kinds, "S") {
            continue;
        }
        let oid = sequence_oid(&sequence.name);
        if oids.is_some_and(|oids| !oids.contains(&oid)) {
            continue;
        }
        let mut row = pg_class_row(
            oid,
            &sequence.name,
            2200,
            None,
            "S",
            0,
            false,
            false,
            1.0,
            0,
            false,
            false,
            false,
            SqlValue::Null,
            SqlValue::Null,
            2,
            false,
            false,
        );
        row.insert(
            "relowner".to_string(),
            SqlValue::Int(role_oid(&sequence.owner)),
        );
        rows.push(row);
    }

    for table in user_collection_names(db) {
        let Some(table_schema) = schema_by_name.get(&table.to_ascii_lowercase()).copied() else {
            continue;
        };
        let primary_key_columns = primary_key_columns_for_schema(table_schema);
        if primary_key_columns.is_empty() {
            continue;
        }
        let index_name = table_schema.primary_key_constraint_name();
        if !catalog_name_matches(names, &index_name) || !catalog_relkind_matches(kinds, "i") {
            continue;
        }
        let oid = primary_index_oid(*table_oids.get(&table).unwrap_or(&0));
        if oids.is_some_and(|oids| !oids.contains(&oid)) {
            continue;
        }
        rows.push(pg_class_row(
            oid,
            &index_name,
            namespace_oid(&schema_name_for_relation(&schemas, &table)),
            None,
            "i",
            primary_key_columns.len() as i64,
            false,
            false,
            reltuples_for_relation(db, &table, exact_counts),
            0,
            false,
            false,
            false,
            SqlValue::Null,
            SqlValue::Null,
            403,
            false,
            false,
        ));
    }

    for index in &catalog_indexes {
        if !catalog_name_matches(names, &index.name) || !catalog_relkind_matches(kinds, "i") {
            continue;
        }
        let oid = secondary_index_oid(&index.schema_name, &index.name);
        if oids.is_some_and(|oids| !oids.contains(&oid)) {
            continue;
        }
        rows.push(pg_class_row(
            oid,
            &index.name,
            namespace_oid(&index.schema_name),
            None,
            "i",
            index.relnatts,
            false,
            false,
            reltuples_for_relation(db, &index.collection, exact_counts),
            0,
            false,
            false,
            false,
            SqlValue::Null,
            SqlValue::Null,
            access_method_oid(&index.access_method),
            false,
            false,
        ));
    }

    for user_type in list_user_types(db)? {
        let UserTypeKind::Composite {
            relation_oid,
            attributes,
        } = &user_type.kind
        else {
            continue;
        };
        if !catalog_name_matches(names, &user_type.name)
            || !catalog_relkind_matches(kinds, "c")
            || oids.is_some_and(|oids| !oids.contains(relation_oid))
        {
            continue;
        }
        let row = pg_class_row(
            *relation_oid,
            &user_type.name,
            namespace_oid(&user_type.schema_name),
            Some(user_type.oid),
            "c",
            attributes.len() as i64,
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
        );
        rows.push(row);
    }

    Ok(rows)
}

pub(crate) fn pg_stat_table_rows(
    db: &BicDb,
    relids: Option<&BTreeSet<i64>>,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let table_oids = table_oids(db);
    let mut schemas =
        if let Some(relation_names) = relation_names_for_oid_filter(&table_oids, relids) {
            load_relation_schemas_by_name(db, &relation_names)?
        } else {
            list_schemas(db)?
        };
    schemas.sort_by(|left, right| {
        left.schema_name
            .cmp(&right.schema_name)
            .then_with(|| left.name.cmp(&right.name))
    });
    let mut rows = Vec::new();
    for schema in schemas {
        let Some(oid) = table_oids.get(&schema.name).copied() else {
            continue;
        };
        if relids.is_some_and(|relids| !relids.contains(&oid)) {
            continue;
        }
        let live_tuples = reltuples_for_relation(db, &schema.name, false).max(0.0) as i64;
        rows.push(pg_stat_table_row(
            oid,
            &schema.schema_name,
            &schema.name,
            live_tuples,
        ));
    }
    Ok(rows)
}

pub(crate) fn pg_stat_database_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    Ok(list_databases(db)?
        .into_iter()
        .enumerate()
        .map(|(idx, database)| {
            virtual_row([
                ("datid", SqlValue::Int(1 + idx as i64)),
                ("datname", SqlValue::String(database.name)),
                ("numbackends", SqlValue::Int(0)),
                ("xact_commit", SqlValue::Int(0)),
                ("xact_rollback", SqlValue::Int(0)),
                ("blks_read", SqlValue::Int(0)),
                ("blks_hit", SqlValue::Int(0)),
                ("tup_returned", SqlValue::Int(0)),
                ("tup_fetched", SqlValue::Int(0)),
                ("tup_inserted", SqlValue::Int(0)),
                ("tup_updated", SqlValue::Int(0)),
                ("tup_deleted", SqlValue::Int(0)),
                ("conflicts", SqlValue::Int(0)),
                ("temp_files", SqlValue::Int(0)),
                ("temp_bytes", SqlValue::Int(0)),
                ("deadlocks", SqlValue::Int(0)),
                ("checksum_failures", SqlValue::Null),
                ("checksum_last_failure", SqlValue::Null),
                ("blk_read_time", SqlValue::Float(0.0)),
                ("blk_write_time", SqlValue::Float(0.0)),
                ("session_time", SqlValue::Float(0.0)),
                ("active_time", SqlValue::Float(0.0)),
                ("idle_in_transaction_time", SqlValue::Float(0.0)),
                ("sessions", SqlValue::Int(0)),
                ("sessions_abandoned", SqlValue::Int(0)),
                ("sessions_fatal", SqlValue::Int(0)),
                ("sessions_killed", SqlValue::Int(0)),
                ("stats_reset", SqlValue::Null),
            ])
        })
        .collect())
}

pub(crate) fn pg_stat_table_row(
    relid: i64,
    schemaname: &str,
    relname: &str,
    n_live_tup: i64,
) -> BTreeMap<String, SqlValue> {
    virtual_row([
        ("relid", SqlValue::Int(relid)),
        ("schemaname", SqlValue::String(schemaname.to_string())),
        ("relname", SqlValue::String(catalog_display_name(relname))),
        ("seq_scan", SqlValue::Int(0)),
        ("seq_tup_read", SqlValue::Int(0)),
        ("idx_scan", SqlValue::Int(0)),
        ("idx_tup_fetch", SqlValue::Int(0)),
        ("n_tup_ins", SqlValue::Int(0)),
        ("n_tup_upd", SqlValue::Int(0)),
        ("n_tup_del", SqlValue::Int(0)),
        ("n_tup_hot_upd", SqlValue::Int(0)),
        ("n_tup_newpage_upd", SqlValue::Int(0)),
        ("n_live_tup", SqlValue::Int(n_live_tup)),
        ("n_dead_tup", SqlValue::Int(0)),
        ("n_mod_since_analyze", SqlValue::Int(0)),
        ("n_ins_since_vacuum", SqlValue::Int(0)),
        ("last_vacuum", SqlValue::Null),
        ("last_autovacuum", SqlValue::Null),
        ("last_analyze", SqlValue::Null),
        ("last_autoanalyze", SqlValue::Null),
        ("vacuum_count", SqlValue::Int(0)),
        ("autovacuum_count", SqlValue::Int(0)),
        ("analyze_count", SqlValue::Int(0)),
        ("autoanalyze_count", SqlValue::Int(0)),
    ])
}

pub(crate) fn pg_stats_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let mut rows = Vec::new();
    for schema in relation_schemas(db)? {
        let Some(table_stats) = db.table_statistics(&schema.name) else {
            continue;
        };
        for column in &schema.columns {
            let field = if column.primary_key {
                IndexField::Id
            } else {
                IndexField::MetadataPath(vec![column.name.clone()])
            };
            let Some(stats) = table_column_stats(table_stats, &field) else {
                continue;
            };
            let null_frac = if stats.row_count == 0 {
                0.0
            } else {
                stats.null_count as f64 / stats.row_count as f64
            };
            let n_distinct = if stats.row_count > 0
                && stats.distinct_count as f64 > stats.row_count as f64 * 0.1
            {
                -(stats.distinct_count as f64 / stats.row_count as f64)
            } else {
                stats.distinct_count as f64
            };
            let range = stats.range.as_ref();
            let range_bounds = range
                .filter(|range| !range.bounds_histogram.is_empty())
                .map(|range| {
                    let element_oid = pg_range_statistics_bounds_type(&range.pg_type)
                        .and_then(pg_type_oid_by_name)
                        .unwrap_or(25);
                    pg_stats_anyarray(
                        range
                            .bounds_histogram
                            .iter()
                            .cloned()
                            .map(JsonValue::String)
                            .collect(),
                        element_oid,
                    )
                })
                .unwrap_or(SqlValue::Null);
            let range_lengths = range
                .filter(|range| !range.length_histogram.is_empty())
                .map(|range| {
                    pg_stats_anyarray(
                        range
                            .length_histogram
                            .iter()
                            .map(|length| {
                                length
                                    .parse::<f64>()
                                    .ok()
                                    .and_then(serde_json::Number::from_f64)
                                    .map(JsonValue::Number)
                                    .unwrap_or_else(|| JsonValue::String(length.clone()))
                            })
                            .collect(),
                        701,
                    )
                })
                .unwrap_or(SqlValue::Null);
            let range_empty_frac = range
                .filter(|range| range.sample_count > 0)
                .map(|range| SqlValue::Float(range.empty_count as f64 / range.sample_count as f64))
                .unwrap_or(SqlValue::Null);
            let network = stats.network.as_ref();
            let network_histogram = network
                .filter(|network| !network.samples.is_empty())
                .map(|network| {
                    pg_stats_anyarray(
                        network
                            .samples
                            .iter()
                            .cloned()
                            .map(JsonValue::String)
                            .collect(),
                        pg_type_oid_by_name(&network.pg_type).unwrap_or(869),
                    )
                })
                .unwrap_or(SqlValue::Null);
            let typed = stats.typed.as_ref();
            let typed_element_oid = i32::try_from(column.type_oid()).unwrap_or(25);
            let most_common_vals = typed
                .filter(|typed| !typed.most_common.is_empty())
                .map(|typed| {
                    pg_stats_anyarray(
                        typed
                            .most_common
                            .iter()
                            .map(|value| JsonValue::String(value.value.clone()))
                            .collect(),
                        typed_element_oid,
                    )
                })
                .unwrap_or(SqlValue::Null);
            let most_common_freqs = typed
                .filter(|typed| !typed.most_common.is_empty() && stats.row_count > 0)
                .map(|typed| {
                    pg_stats_anyarray(
                        typed
                            .most_common
                            .iter()
                            .map(|value| {
                                serde_json::Number::from_f64(
                                    value.count as f64 / stats.row_count as f64,
                                )
                                .map(JsonValue::Number)
                                .unwrap_or(JsonValue::Null)
                            })
                            .collect(),
                        700,
                    )
                })
                .unwrap_or(SqlValue::Null);
            let histogram_bounds = typed
                .filter(|typed| !typed.histogram_values.is_empty())
                .map(|typed| {
                    pg_stats_anyarray(
                        typed
                            .histogram_values
                            .iter()
                            .cloned()
                            .map(JsonValue::String)
                            .collect(),
                        typed_element_oid,
                    )
                })
                .unwrap_or(network_histogram);
            let avg_width = typed
                .map(|typed| typed.avg_width)
                .or_else(|| {
                    range
                        .filter(|range| !range.samples.is_empty())
                        .map(|range| {
                            range.samples.iter().map(String::len).sum::<usize>()
                                / range.samples.len()
                        })
                        .or_else(|| {
                            network
                                .filter(|network| !network.samples.is_empty())
                                .map(|network| {
                                    network.samples.iter().map(String::len).sum::<usize>()
                                        / network.samples.len()
                                })
                        })
                })
                .unwrap_or(0);
            rows.push(virtual_row([
                ("schemaname", SqlValue::String(schema.schema_name.clone())),
                (
                    "tablename",
                    SqlValue::String(catalog_display_name(&schema.name)),
                ),
                ("attname", SqlValue::String(column.name.clone())),
                ("inherited", SqlValue::Bool(false)),
                ("null_frac", SqlValue::Float(null_frac)),
                ("avg_width", SqlValue::Int(avg_width as i64)),
                ("n_distinct", SqlValue::Float(n_distinct)),
                ("most_common_vals", most_common_vals),
                ("most_common_freqs", most_common_freqs),
                ("histogram_bounds", histogram_bounds),
                ("correlation", SqlValue::Null),
                ("most_common_elems", SqlValue::Null),
                ("most_common_elem_freqs", SqlValue::Null),
                ("elem_count_histogram", SqlValue::Null),
                ("range_length_histogram", range_lengths),
                ("range_empty_frac", range_empty_frac),
                ("range_bounds_histogram", range_bounds),
            ]));
        }
    }
    Ok(rows)
}

fn pg_stats_anyarray(values: Vec<JsonValue>, element_oid: i32) -> SqlValue {
    SqlValue::Json(serde_json::json!({
        "$bicdb_array_input": {
            "value": values,
            "lower_bounds": [1],
            "element_oid": element_oid,
            "declared_type": "anyarray",
        }
    }))
}

pub(crate) fn catalog_name_matches(names: Option<&BTreeSet<String>>, name: &str) -> bool {
    names.is_none_or(|names| {
        // A relation outside `public` is stored under a schema-isolated
        // physical name but shown, and filtered on, by its logical one.
        let display = catalog_display_name(name);
        names.iter().any(|candidate| {
            candidate.eq_ignore_ascii_case(name) || candidate.eq_ignore_ascii_case(&display)
        })
    })
}

pub(crate) fn catalog_relkind_matches(kinds: Option<&BTreeSet<String>>, relkind: &str) -> bool {
    kinds.is_none_or(|kinds| {
        kinds
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(relkind))
    })
}

pub(crate) fn reltuples_for_relation(db: &BicDb, relation: &str, _exact: bool) -> f64 {
    db.table_statistics(relation)
        .map(|stats| stats.row_count as f64)
        .unwrap_or(0.0)
}

pub(crate) enum CatalogRelationTarget {
    Relation(String),
    Sequence,
    Composite(Vec<ColumnSchema>),
}

pub(crate) fn catalog_relation_target_for_oid(
    db: &BicDb,
    oid: i64,
) -> Result<Option<CatalogRelationTarget>> {
    if let Some((relation, _)) = (*table_oids(db))
        .clone()
        .into_iter()
        .find(|(_, candidate)| *candidate == oid)
    {
        return Ok(Some(CatalogRelationTarget::Relation(relation)));
    }
    if let Some(attributes) = list_user_types(db)?.into_iter().find_map(|user_type| {
        let UserTypeKind::Composite {
            relation_oid,
            attributes,
        } = user_type.kind
        else {
            return None;
        };
        (relation_oid == oid).then(|| composite_attribute_columns(attributes))
    }) {
        return Ok(Some(CatalogRelationTarget::Composite(attributes)));
    }
    Ok(list_sequences(db)?
        .into_iter()
        .find(|sequence| sequence_oid(&sequence.name) == oid)
        .map(|_| CatalogRelationTarget::Sequence))
}

pub(crate) fn catalog_columns_for_relation(
    db: &BicDb,
    relation: &str,
) -> Result<Vec<ColumnSchema>> {
    if graph_virtual_table_names().contains(&relation) {
        return Ok(graph_virtual_table_columns(relation));
    }
    if let Some(schema) = load_schema(db, relation)? {
        return Ok(schema
            .columns
            .into_iter()
            .filter(|column| !column.hidden)
            .collect());
    }
    if let Some(view) = load_view(db, relation)? {
        return Ok(catalog_view_columns(&view));
    }
    Ok(default_record_columns())
}

pub(crate) fn catalog_view_columns(view: &ViewSchema) -> Vec<ColumnSchema> {
    let mut columns = view.columns.clone();
    apply_catalog_view_column_types(&view.name, &view.query_sql, &mut columns);
    columns
}

pub(crate) fn apply_catalog_view_column_types(
    view: &str,
    query_sql: &str,
    columns: &mut [ColumnSchema],
) {
    let lower = query_sql.to_ascii_lowercase();
    if view.eq_ignore_ascii_case("postgres_foreign_keys")
        && lower.contains("pg_constraint")
        && lower.contains("constrained_columns")
        && lower.contains("referenced_columns")
    {
        set_view_column_pg_type(columns, "constrained_columns", "text[]");
        set_view_column_pg_type(columns, "referenced_columns", "text[]");
    }
    if view.eq_ignore_ascii_case("postgres_constraints")
        && lower.contains("pg_constraint")
        && lower.contains("column_names")
    {
        set_view_column_pg_type(columns, "column_names", "text[]");
    }
}

pub(crate) fn set_view_column_pg_type(columns: &mut [ColumnSchema], name: &str, pg_type: &str) {
    if let Some(column) = columns
        .iter_mut()
        .find(|column| column.name.eq_ignore_ascii_case(name))
    {
        column.pg_type = pg_type.to_string();
        column.type_modifier = None;
        column.vector_dim = None;
    }
}

pub(crate) fn active_record_column_definitions_fast_path(
    db: &BicDb,
    normalized: &str,
) -> Result<Option<SqlResult>> {
    let Some(relation) = active_record_column_definitions_relation(normalized) else {
        return Ok(None);
    };
    let relation_exists = table_oids(db).contains_key(&relation);
    if !relation_exists {
        return Ok(Some(SqlResult::new(
            active_record_column_definition_columns(normalized),
            Vec::new(),
        )));
    }
    let include_identity = normalized.contains("attidentity as identity");
    let include_generated = normalized.contains("attgenerated as attgenerated");
    let columns = catalog_columns_for_relation(db, &relation)?;
    let mut rows = Vec::new();
    for column in columns {
        let atttypid = column.type_oid();
        let formatted_type = column.formatted_pg_type();
        let atttypmod = column.catalog_typmod();
        let mut row = vec![
            SqlValue::String(column.name),
            SqlValue::String(formatted_type),
            if let Some(expression) = column.generated_expr.as_ref() {
                SqlValue::String(expression.clone())
            } else if column.identity.is_none() {
                column
                    .default_sequence
                    .as_deref()
                    .map(pg_attrdef_expr)
                    .unwrap_or(SqlValue::Null)
            } else {
                SqlValue::Null
            },
            SqlValue::Bool(!column.nullable || column.primary_key),
            SqlValue::Int(atttypid),
            SqlValue::Int(atttypmod),
            SqlValue::Null,
            SqlValue::Null,
        ];
        if include_identity {
            row.push(SqlValue::String(column.identity.unwrap_or_default()));
        }
        if include_generated {
            row.push(SqlValue::String(
                column
                    .generated_expr
                    .as_ref()
                    .map_or("", |_| "s")
                    .to_string(),
            ));
        }
        rows.push(row);
    }
    Ok(Some(SqlResult::new(
        active_record_column_definition_columns(normalized),
        rows,
    )))
}

pub(crate) fn active_record_column_definitions_relation(normalized: &str) -> Option<String> {
    if !(normalized.starts_with("select a.attname, format_type(a.atttypid, a.atttypmod)")
        || normalized
            .starts_with("select a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod)"))
    {
        return None;
    }
    if !(normalized.contains(" from pg_attribute a ")
        || normalized.contains(" from pg_catalog.pg_attribute a "))
    {
        return None;
    }
    if !(normalized.contains(" join pg_attrdef d ")
        || normalized.contains(" join pg_catalog.pg_attrdef d "))
    {
        return None;
    }
    if !normalized.contains(" where a.attrelid = '") {
        return None;
    }
    if !normalized.contains(" and a.attnum > 0")
        || !normalized.contains("not a.attisdropped")
        || !normalized.contains(" order by a.attnum")
    {
        return None;
    }
    let literal = normalized
        .split(" where a.attrelid = '")
        .nth(1)?
        .split("'::regclass")
        .next()?;
    Some(normalize_object_name(literal))
}

pub(crate) fn active_record_column_definition_columns(normalized: &str) -> Vec<String> {
    let mut columns = vec![
        "attname".to_string(),
        "format_type".to_string(),
        "pg_get_expr".to_string(),
        "attnotnull".to_string(),
        "atttypid".to_string(),
        "atttypmod".to_string(),
        "collname".to_string(),
        "comment".to_string(),
    ];
    if normalized.contains("attidentity as identity") {
        columns.push("identity".to_string());
    }
    if normalized.contains("attgenerated as attgenerated") {
        columns.push("attgenerated".to_string());
    }
    columns
}

pub(crate) fn active_record_primary_key_fast_path(
    db: &BicDb,
    normalized: &str,
) -> Result<Option<SqlResult>> {
    let Some(relation) = active_record_primary_key_relation(normalized) else {
        return Ok(None);
    };
    let Some(schema) = load_schema(db, &relation)? else {
        return Ok(Some(SqlResult::new(
            vec!["attname".to_string()],
            Vec::new(),
        )));
    };
    let rows = primary_key_columns_for_schema(&schema)
        .into_iter()
        .map(|column| vec![SqlValue::String(column)])
        .collect();
    Ok(Some(SqlResult::new(vec!["attname".to_string()], rows)))
}

pub(crate) fn active_record_primary_key_relation(normalized: &str) -> Option<String> {
    if !normalized.starts_with("select a.attname ") {
        return None;
    }
    if !(normalized.contains(" from pg_index i ")
        || normalized.contains(" from pg_catalog.pg_index i "))
    {
        return None;
    }
    if !(normalized.contains(" join pg_attribute a ")
        || normalized.contains(" join pg_catalog.pg_attribute a "))
    {
        return None;
    }
    if !normalized.contains(" on a.attrelid = i.indrelid ") {
        return None;
    }
    if !(normalized.contains(" and a.attnum = any(i.indkey) ")
        || normalized.contains(" and a.attnum = any (i.indkey) "))
    {
        return None;
    }
    if !normalized.contains(" and i.indisprimary") {
        return None;
    }
    if !normalized.contains(" order by array_position(i.indkey, a.attnum)") {
        return None;
    }
    let literal = normalized
        .split(" where i.indrelid = '")
        .nth(1)?
        .split("'::regclass")
        .next()?;
    Some(normalize_object_name(&regclass_relation_name(literal)))
}

pub(crate) fn active_record_foreign_keys_fast_path(
    db: &BicDb,
    normalized: &str,
) -> Result<Option<SqlResult>> {
    let Some(relation) = active_record_foreign_keys_relation(normalized) else {
        return Ok(None);
    };
    let schema_filter = active_record_foreign_keys_schema_filter(normalized);
    let Some(schema) = load_schema(db, &relation)? else {
        return Ok(Some(SqlResult::new(
            active_record_foreign_keys_columns(),
            Vec::new(),
        )));
    };
    if !catalog_name_matches(schema_filter.as_ref(), &schema.schema_name) {
        return Ok(Some(SqlResult::new(
            active_record_foreign_keys_columns(),
            Vec::new(),
        )));
    }

    let table_oids = table_oids(db);
    let conrelid = *table_oids.get(&schema.name).unwrap_or(&0);
    let mut rows = Vec::new();
    for constraint in &schema.constraints {
        let ConstraintSchema::ForeignKey {
            name,
            columns,
            foreign_table,
            referred_columns,
            on_delete,
            on_update,
            validated,
        } = constraint
        else {
            continue;
        };
        let Some(foreign_schema) = load_schema(db, foreign_table)? else {
            continue;
        };
        let confrelid = *table_oids.get(&foreign_schema.name).unwrap_or(&0);
        let conkey = attnums_for_schema_columns(&schema, columns);
        let confkey = attnums_for_schema_columns(&foreign_schema, referred_columns);
        rows.push(vec![
            SqlValue::String(regclass_text_for_schema(&foreign_schema)),
            columns
                .first()
                .cloned()
                .map(SqlValue::String)
                .unwrap_or(SqlValue::Null),
            referred_columns
                .first()
                .cloned()
                .map(SqlValue::String)
                .unwrap_or(SqlValue::Null),
            SqlValue::String(name.clone()),
            SqlValue::String(fk_action_code(*on_update).to_string()),
            SqlValue::String(fk_action_code(*on_delete).to_string()),
            SqlValue::Bool(*validated),
            SqlValue::Bool(false),
            SqlValue::Bool(false),
            constraint_key_value(&conkey),
            constraint_key_value(&confkey),
            SqlValue::Int(conrelid),
            SqlValue::Int(confrelid),
        ]);
    }
    rows.sort_by(|left, right| left[3].to_cell().cmp(&right[3].to_cell()));

    Ok(Some(SqlResult::new(
        active_record_foreign_keys_columns(),
        rows,
    )))
}

pub(crate) fn active_record_foreign_keys_relation(normalized: &str) -> Option<String> {
    if !normalized.starts_with("select t2.oid::regclass::text as to_table") {
        return None;
    }
    for required in [
        " c.confupdtype as on_update",
        " c.confdeltype as on_delete",
        " c.convalidated as valid",
        " c.condeferrable as deferrable",
        " c.condeferred as deferred",
        " c.conrelid, c.confrelid",
        " from pg_constraint c ",
        " join pg_class t1 on c.conrelid = t1.oid ",
        " join pg_class t2 on c.confrelid = t2.oid ",
        " join pg_attribute a1 on a1.attnum = c.conkey[1] and a1.attrelid = t1.oid ",
        " join pg_attribute a2 on a2.attnum = c.confkey[1] and a2.attrelid = t2.oid ",
        " join pg_namespace t3 on c.connamespace = t3.oid ",
        " where c.contype = 'f' ",
        " order by c.conname",
    ] {
        if !normalized.contains(required) {
            return None;
        }
    }
    normalized_quoted_value_after(normalized, " and t1.relname = '")
        .as_deref()
        .map(normalize_object_name)
}

pub(crate) fn active_record_foreign_keys_schema_filter(
    normalized: &str,
) -> Option<BTreeSet<String>> {
    if normalized.contains(" and t3.nspname = any (current_schemas(false))")
        || normalized.contains(" and t3.nspname = any(current_schemas(false))")
    {
        return Some(BTreeSet::from(["public".to_string()]));
    }
    normalized_quoted_value_after(normalized, " and t3.nspname = '")
        .map(|schema| BTreeSet::from([schema]))
}

pub(crate) fn active_record_foreign_keys_columns() -> Vec<String> {
    [
        "to_table",
        "column",
        "primary_key",
        "name",
        "on_update",
        "on_delete",
        "valid",
        "deferrable",
        "deferred",
        "conkey",
        "confkey",
        "conrelid",
        "confrelid",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub(crate) fn active_record_index_invalid_fast_path(
    db: &BicDb,
    normalized: &str,
) -> Result<Option<SqlResult>> {
    let Some(index_name) = active_record_index_invalid_name(normalized) else {
        return Ok(None);
    };
    let schema_filter = active_record_index_invalid_schema_filter(normalized);
    let rows = if catalog_index_name_exists(db, &index_name, schema_filter.as_ref())? {
        vec![vec![SqlValue::Bool(false)]]
    } else {
        Vec::new()
    };
    Ok(Some(SqlResult::new(vec!["?column?".to_string()], rows)))
}

pub(crate) fn active_record_index_invalid_name(normalized: &str) -> Option<String> {
    if !normalized.starts_with("select not i.indisvalid") {
        return None;
    }
    for required in [
        " from pg_class c ",
        " join pg_index i ",
        " on c.oid = i.indexrelid ",
        " join pg_namespace n ",
        " on n.oid = c.relnamespace ",
    ] {
        if !normalized.contains(required) {
            return None;
        }
    }
    normalized_quoted_value_after(normalized, " and c.relname = '")
        .as_deref()
        .map(normalize_object_name)
}

pub(crate) fn active_record_index_invalid_schema_filter(
    normalized: &str,
) -> Option<BTreeSet<String>> {
    if normalized.contains(" n.nspname = current_schema()")
        || normalized.contains(" n.nspname = current_schema() ")
        || normalized.contains(" n.nspname = current_schema")
    {
        return Some(BTreeSet::from(["public".to_string()]));
    }
    normalized_quoted_value_after(normalized, " n.nspname = '")
        .map(|schema| BTreeSet::from([schema]))
}

pub(crate) fn normalized_quoted_value_after(normalized: &str, marker: &str) -> Option<String> {
    normalized
        .split(marker)
        .nth(1)?
        .split('\'')
        .next()
        .map(|value| value.replace("''", "'"))
}

pub(crate) fn attnums_for_schema_columns(schema: &TableSchema, columns: &[String]) -> Vec<i64> {
    columns
        .iter()
        .map(|column| {
            schema
                .columns
                .iter()
                .position(|candidate| candidate.name.eq_ignore_ascii_case(column))
                .map(|idx| idx as i64 + 1)
                .unwrap_or(0)
        })
        .collect()
}

pub(crate) fn regclass_text_for_schema(schema: &TableSchema) -> String {
    if schema.schema_name.eq_ignore_ascii_case("public") {
        schema.name.clone()
    } else {
        format!("{}.{}", schema.schema_name, schema.name)
    }
}

pub(crate) fn catalog_index_name_exists(
    db: &BicDb,
    index_name: &str,
    schema_filter: Option<&BTreeSet<String>>,
) -> Result<bool> {
    Ok(list_schemas(db)?.into_iter().any(|schema| {
        catalog_name_matches(schema_filter, &schema.schema_name)
            && ((schema.primary_key_column().is_some()
                && schema
                    .primary_key_constraint_name()
                    .eq_ignore_ascii_case(index_name))
                || schema
                    .indexes
                    .iter()
                    .any(|index| index.name.eq_ignore_ascii_case(index_name)))
    }))
}

pub(crate) fn pg_attribute_rows_for_attrelid(
    db: &BicDb,
    attrelid: i64,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    match catalog_relation_target_for_oid(db, attrelid)? {
        Some(CatalogRelationTarget::Relation(relation)) => pg_attribute_rows_with_grants(
            db,
            &relation,
            attrelid,
            catalog_columns_for_relation(db, &relation)?,
        ),
        Some(CatalogRelationTarget::Sequence) => Ok(pg_attribute_rows_for_sequence(attrelid)),
        Some(CatalogRelationTarget::Composite(columns)) => {
            Ok(pg_attribute_rows_for_columns(attrelid, columns))
        }
        None => Ok(Vec::new()),
    }
}

pub(crate) fn pg_attribute_rows_for_attrelids(
    db: &BicDb,
    attrelids: &BTreeSet<i64>,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
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
    let composite_columns = list_user_types(db)?
        .into_iter()
        .filter_map(|user_type| {
            let UserTypeKind::Composite {
                relation_oid,
                attributes,
            } = user_type.kind
            else {
                return None;
            };
            Some((relation_oid, composite_attribute_columns(attributes)))
        })
        .collect::<BTreeMap<_, _>>();

    let mut rows = Vec::new();
    for attrelid in attrelids {
        if let Some(relation) = relation_by_oid.get(attrelid) {
            rows.extend(pg_attribute_rows_with_grants(
                db,
                relation,
                *attrelid,
                columns_for_relation(&schemas, relation),
            )?);
        } else if sequence_oids.contains(attrelid) {
            rows.extend(pg_attribute_rows_for_sequence(*attrelid));
        } else if let Some(columns) = composite_columns.get(attrelid) {
            rows.extend(pg_attribute_rows_for_columns(*attrelid, columns.clone()));
        }
    }
    Ok(rows)
}

fn pg_attribute_rows_with_grants(
    db: &BicDb,
    table: &str,
    attrelid: i64,
    columns: Vec<ColumnSchema>,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let grants = list_privileges(db)?;
    let mut rows = pg_attribute_rows_for_columns(attrelid, columns);
    for row in &mut rows {
        let Some(SqlValue::String(column)) = row.get("attname") else {
            continue;
        };
        let acl = acl_value(
            grants
                .iter()
                .filter(|grant| {
                    grant.object_type == PrivilegeObjectType::Table
                        && grant.object_name == table
                        && grant.column.as_deref() == Some(column.as_str())
                })
                .cloned()
                .collect(),
        )?;
        row.insert("attacl".into(), acl);
    }
    Ok(rows)
}

pub(crate) fn pg_attribute_rows_for_columns(
    attrelid: i64,
    columns: Vec<ColumnSchema>,
) -> Vec<BTreeMap<String, SqlValue>> {
    columns
        .into_iter()
        .enumerate()
        .map(|(idx, column)| {
            let atttypmod = column.catalog_typmod();
            let attcollation = column.collation_oid();
            let atttypid = column.type_oid();
            let attlen = column.type_len();
            let attndims = column.catalog_array_ndims();
            let attbyval = column.type_by_value();
            let attalign = column.type_align();
            let attstorage = column.type_storage();
            let attcompression = column
                .compression
                .map_or_else(String::new, |compression| compression.to_string());
            virtual_row([
                ("attrelid", SqlValue::Int(attrelid)),
                ("attname", SqlValue::String(column.name)),
                ("atttypid", SqlValue::Int(atttypid)),
                ("attlen", SqlValue::Int(attlen)),
                ("attnum", SqlValue::Int((idx + 1) as i64)),
                ("atttypmod", SqlValue::Int(atttypmod)),
                ("attndims", SqlValue::Int(attndims)),
                ("attbyval", SqlValue::Bool(attbyval)),
                ("attalign", SqlValue::String(attalign.to_string())),
                ("attstorage", SqlValue::String(attstorage.to_string())),
                ("attcompression", SqlValue::String(attcompression)),
                (
                    "attnotnull",
                    SqlValue::Bool(!column.nullable || column.primary_key),
                ),
                (
                    "atthasdef",
                    SqlValue::Bool(
                        column.generated_expr.is_some()
                            || (column.identity.is_none()
                                && (column.default_sequence.is_some()
                                    || column.default_value.is_some()
                                    || column.default_expr.is_some())),
                    ),
                ),
                ("atthasmissing", SqlValue::Bool(false)),
                (
                    "attidentity",
                    SqlValue::String(column.identity.clone().unwrap_or_default()),
                ),
                (
                    "attgenerated",
                    SqlValue::String(
                        column
                            .generated_expr
                            .as_ref()
                            .map_or("", |_| "s")
                            .to_string(),
                    ),
                ),
                ("attisdropped", SqlValue::Bool(column.hidden)),
                ("attislocal", SqlValue::Bool(true)),
                ("attinhcount", SqlValue::Int(0)),
                ("attcollation", SqlValue::Int(attcollation)),
                ("attstattarget", SqlValue::Null),
                ("attacl", SqlValue::Null),
                ("attoptions", SqlValue::Null),
                ("attfdwoptions", SqlValue::Null),
                ("attmissingval", SqlValue::Null),
            ])
        })
        .collect()
}

pub(crate) fn pg_attribute_rows_for_sequence(attrelid: i64) -> Vec<BTreeMap<String, SqlValue>> {
    [
        ("last_value", "int8"),
        ("log_cnt", "int8"),
        ("is_called", "bool"),
    ]
    .into_iter()
    .enumerate()
    .map(|(idx, (name, pg_type))| {
        virtual_row([
            ("attrelid", SqlValue::Int(attrelid)),
            ("attname", SqlValue::String(name.to_string())),
            ("atttypid", SqlValue::Int(pg_type_oid(pg_type))),
            ("attlen", SqlValue::Int(pg_type_len(pg_type))),
            ("attnum", SqlValue::Int((idx + 1) as i64)),
            ("atttypmod", SqlValue::Int(-1)),
            (
                "attndims",
                SqlValue::Int(if pg_type_is_array(pg_type) { 1 } else { 0 }),
            ),
            ("attbyval", SqlValue::Bool(pg_type_by_value(pg_type))),
            (
                "attalign",
                SqlValue::String(pg_type_align(pg_type).to_string()),
            ),
            (
                "attstorage",
                SqlValue::String(pg_type_storage(pg_type).to_string()),
            ),
            ("attcompression", SqlValue::String(String::new())),
            ("attnotnull", SqlValue::Bool(false)),
            ("atthasdef", SqlValue::Bool(false)),
            ("atthasmissing", SqlValue::Bool(false)),
            ("attidentity", SqlValue::String(String::new())),
            ("attgenerated", SqlValue::String(String::new())),
            ("attisdropped", SqlValue::Bool(false)),
            ("attislocal", SqlValue::Bool(true)),
            ("attinhcount", SqlValue::Int(0)),
            ("attcollation", SqlValue::Int(type_collation_oid(pg_type))),
            ("attstattarget", SqlValue::Null),
            ("attacl", SqlValue::Null),
            ("attoptions", SqlValue::Null),
            ("attfdwoptions", SqlValue::Null),
            ("attmissingval", SqlValue::Null),
        ])
    })
    .collect()
}

pub(crate) fn pg_attribute_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let schemas = relation_schemas(db)?;
    let table_oids = table_oids(db);
    let mut rows = Vec::new();
    for collection in catalog_table_names(db) {
        let columns = columns_for_relation(&schemas, &collection);
        let attrelid = *table_oids.get(&collection).unwrap_or(&0);
        rows.extend(pg_attribute_rows_with_grants(
            db,
            &collection,
            attrelid,
            columns,
        )?);
    }
    for sequence in list_sequences(db)? {
        let attrelid = sequence_oid(&sequence.name);
        rows.extend(pg_attribute_rows_for_sequence(attrelid));
    }
    for user_type in list_user_types(db)? {
        let UserTypeKind::Composite {
            relation_oid,
            attributes,
        } = user_type.kind
        else {
            continue;
        };
        rows.extend(pg_attribute_rows_for_columns(
            relation_oid,
            composite_attribute_columns(attributes),
        ));
    }
    Ok(rows)
}

fn composite_attribute_columns(attributes: Vec<CompositeAttributeSchema>) -> Vec<ColumnSchema> {
    attributes
        .into_iter()
        .enumerate()
        .map(|(index, attribute)| {
            let dropped = attribute.dropped;
            ColumnSchema {
                name: if dropped {
                    format!("........pg.dropped.{}........", index + 1)
                } else {
                    attribute.name
                },
                pg_type: if dropped {
                    String::new()
                } else {
                    attribute.pg_type
                },
                user_type: if dropped { None } else { attribute.user_type },
                collation: if dropped { None } else { attribute.collation },
                type_modifier: if dropped {
                    None
                } else {
                    attribute.type_modifier
                },
                array_ndims: if dropped { 0 } else { attribute.array_ndims },
                compression: None,
                primary_key: false,
                hidden: dropped,
                nullable: true,
                vector_dim: None,
                default_sequence: None,
                default_value: None,
                default_expr: None,
                generated_expr: None,
                identity: None,
            }
        })
        .collect()
}

pub(crate) fn pg_type_rows() -> Vec<BTreeMap<String, SqlValue>> {
    let mut rows = Vec::new();
    for spec in PG_TYPE_SPECS {
        rows.push(pg_type_row(
            spec.name,
            i64::from(spec.oid),
            spec.base_element_type_oid().map(i64::from).unwrap_or(0),
            spec.array_oid.map(i64::from).unwrap_or(0),
            false,
        ));
    }
    for spec in PG_TYPE_SPECS {
        if let Some(array_oid) = spec.array_oid {
            rows.push(pg_type_row(
                &format!("_{}", spec.name),
                i64::from(array_oid),
                i64::from(spec.oid),
                0,
                true,
            ));
        }
    }
    rows
}

pub(crate) fn pg_type_rows_for_db(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let mut rows = pg_type_rows();
    let privileges = list_privileges(db)?;
    if let Some(vector_extension) = list_extensions(db)?
        .into_iter()
        .find(|extension| extension.name.eq_ignore_ascii_case("vector"))
    {
        if let Some(vector_row) = rows
            .iter_mut()
            .find(|row| row.get("typname") == Some(&SqlValue::String("vector".to_string())))
        {
            vector_row.insert(
                "typnamespace".to_string(),
                SqlValue::Int(namespace_oid(&vector_extension.schema)),
            );
        }
    }
    for user_type in list_user_types(db)? {
        let column_type = user_type.column_type(false);
        let mut type_row = pg_type_row(
            &user_type.name,
            user_type.oid,
            0,
            user_type.array_oid,
            false,
        );
        type_row.insert(
            "typnamespace".to_string(),
            SqlValue::Int(namespace_oid(&user_type.schema_name)),
        );
        type_row.insert(
            "typowner".to_string(),
            SqlValue::Int(role_oid(&user_type.owner)),
        );
        type_row.insert(
            "typacl".to_string(),
            type_acl_value_from_privileges(&privileges, &user_type)?,
        );
        type_row.insert(
            "typlen".to_string(),
            SqlValue::Int(column_type.scalar_type_len()),
        );
        type_row.insert(
            "typbyval".to_string(),
            SqlValue::Bool(column_type.scalar_by_value()),
        );
        type_row.insert(
            "typalign".to_string(),
            SqlValue::String(column_type.scalar_align().to_string()),
        );
        type_row.insert(
            "typstorage".to_string(),
            SqlValue::String(column_type.scalar_storage().to_string()),
        );
        type_row.insert(
            "typcollation".to_string(),
            SqlValue::Int(column_type.scalar_collation_oid()),
        );
        type_row.insert(
            "typdelim".to_string(),
            SqlValue::String(column_type.scalar_delimiter().to_string()),
        );
        match &user_type.kind {
            UserTypeKind::Shell => {
                type_row.insert("typtype".to_string(), SqlValue::String("p".to_string()));
                type_row.insert("typcategory".to_string(), SqlValue::String("P".to_string()));
                type_row.insert("typisdefined".to_string(), SqlValue::Bool(false));
                type_row.insert(
                    "typinput".to_string(),
                    SqlValue::String("shell_in".to_string()),
                );
                type_row.insert(
                    "typoutput".to_string(),
                    SqlValue::String("shell_out".to_string()),
                );
                type_row.insert("typreceive".to_string(), SqlValue::String("-".to_string()));
                type_row.insert("typsend".to_string(), SqlValue::String("-".to_string()));
            }
            UserTypeKind::Base {
                input,
                output,
                receive,
                send,
                category,
                preferred,
                default_expr,
                element_type,
                delimiter,
                ..
            } => {
                type_row.insert("typtype".to_string(), SqlValue::String("b".to_string()));
                type_row.insert(
                    "typcategory".to_string(),
                    SqlValue::String(category.to_string()),
                );
                type_row.insert("typispreferred".to_string(), SqlValue::Bool(*preferred));
                type_row.insert(
                    "typinput".to_string(),
                    SqlValue::Int(routine_oid(RoutineKind::Function, input)),
                );
                type_row.insert(
                    "typoutput".to_string(),
                    SqlValue::Int(routine_oid(RoutineKind::Function, output)),
                );
                type_row.insert(
                    "typreceive".to_string(),
                    SqlValue::Int(
                        receive
                            .as_deref()
                            .map(|name| routine_oid(RoutineKind::Function, name))
                            .unwrap_or(0),
                    ),
                );
                type_row.insert(
                    "typsend".to_string(),
                    SqlValue::Int(
                        send.as_deref()
                            .map(|name| routine_oid(RoutineKind::Function, name))
                            .unwrap_or(0),
                    ),
                );
                for column in ["typsubscript", "typmodin", "typmodout", "typanalyze"] {
                    type_row.insert(column.to_string(), SqlValue::Int(0));
                }
                type_row.insert(
                    "typdefault".to_string(),
                    default_expr
                        .as_deref()
                        .map(catalog_base_type_default)
                        .map(SqlValue::String)
                        .unwrap_or(SqlValue::Null),
                );
                type_row.insert(
                    "typdelim".to_string(),
                    SqlValue::String(delimiter.to_string()),
                );
                type_row.insert(
                    "typelem".to_string(),
                    SqlValue::Int(element_type.as_deref().map(pg_type_oid).unwrap_or_default()),
                );
            }
            UserTypeKind::Enum { .. } => {
                type_row.insert("typtype".to_string(), SqlValue::String("e".to_string()));
                type_row.insert("typcategory".to_string(), SqlValue::String("E".to_string()));
                type_row.insert(
                    "typinput".to_string(),
                    SqlValue::String("enum_in".to_string()),
                );
                type_row.insert(
                    "typoutput".to_string(),
                    SqlValue::String("enum_out".to_string()),
                );
                type_row.insert(
                    "typreceive".to_string(),
                    SqlValue::String("enum_recv".to_string()),
                );
                type_row.insert(
                    "typsend".to_string(),
                    SqlValue::String("enum_send".to_string()),
                );
            }
            UserTypeKind::Composite { relation_oid, .. } => {
                type_row.insert("typtype".to_string(), SqlValue::String("c".to_string()));
                type_row.insert("typcategory".to_string(), SqlValue::String("C".to_string()));
                type_row.insert("typrelid".to_string(), SqlValue::Int(*relation_oid));
                type_row.insert(
                    "typinput".to_string(),
                    SqlValue::String("record_in".to_string()),
                );
                type_row.insert(
                    "typoutput".to_string(),
                    SqlValue::String("record_out".to_string()),
                );
                type_row.insert(
                    "typreceive".to_string(),
                    SqlValue::String("record_recv".to_string()),
                );
                type_row.insert(
                    "typsend".to_string(),
                    SqlValue::String("record_send".to_string()),
                );
            }
            UserTypeKind::Domain {
                base_type,
                base_user_type,
                type_modifier,
                default_expr,
                not_null,
                ..
            } => {
                let base_oid = base_user_type
                    .as_deref()
                    .map(UserTypeColumnSchema::type_oid)
                    .unwrap_or_else(|| pg_type_oid(base_type));
                let base_category = base_user_type.as_deref().map_or_else(
                    || pg_type_category(base_type),
                    |base| match &base.kind {
                        UserTypeKind::Shell => 'P',
                        UserTypeKind::Base { category, .. } => *category,
                        UserTypeKind::Enum { .. } => 'E',
                        UserTypeKind::Composite { .. } => 'C',
                        UserTypeKind::Domain { base_type, .. } => pg_type_category(base_type),
                        UserTypeKind::Range { .. } | UserTypeKind::Multirange { .. } => 'R',
                    },
                );
                let base_output = user_type_output_function(base_type, base_user_type.as_deref());
                let base_send = user_type_send_function(base_type, base_user_type.as_deref());
                type_row.insert("typtype".to_string(), SqlValue::String("d".to_string()));
                type_row.insert(
                    "typcategory".to_string(),
                    SqlValue::String(base_category.to_string()),
                );
                type_row.insert("typbasetype".to_string(), SqlValue::Int(base_oid));
                type_row.insert(
                    "typtypmod".to_string(),
                    SqlValue::Int(
                        type_modifier
                            .as_ref()
                            .map(PgTypeModifier::catalog_value)
                            .unwrap_or(-1),
                    ),
                );
                type_row.insert("typnotnull".to_string(), SqlValue::Bool(*not_null));
                type_row.insert(
                    "typdefault".to_string(),
                    default_expr
                        .clone()
                        .map(SqlValue::String)
                        .unwrap_or(SqlValue::Null),
                );
                type_row.insert(
                    "typdefaultbin".to_string(),
                    default_expr
                        .clone()
                        .map(SqlValue::String)
                        .unwrap_or(SqlValue::Null),
                );
                type_row.insert(
                    "typinput".to_string(),
                    SqlValue::String("domain_in".to_string()),
                );
                type_row.insert("typoutput".to_string(), SqlValue::String(base_output));
                type_row.insert(
                    "typreceive".to_string(),
                    SqlValue::String("domain_recv".to_string()),
                );
                type_row.insert("typsend".to_string(), SqlValue::String(base_send));
            }
            UserTypeKind::Range { .. } => {
                type_row.insert("typtype".to_string(), SqlValue::String("r".to_string()));
                type_row.insert("typcategory".to_string(), SqlValue::String("R".to_string()));
                type_row.insert(
                    "typinput".to_string(),
                    SqlValue::String("range_in".to_string()),
                );
                type_row.insert(
                    "typoutput".to_string(),
                    SqlValue::String("range_out".to_string()),
                );
                type_row.insert(
                    "typreceive".to_string(),
                    SqlValue::String("range_recv".to_string()),
                );
                type_row.insert(
                    "typsend".to_string(),
                    SqlValue::String("range_send".to_string()),
                );
                type_row.insert(
                    "typanalyze".to_string(),
                    SqlValue::String("range_typanalyze".to_string()),
                );
            }
            UserTypeKind::Multirange { .. } => {
                type_row.insert("typtype".to_string(), SqlValue::String("m".to_string()));
                type_row.insert("typcategory".to_string(), SqlValue::String("R".to_string()));
                type_row.insert(
                    "typinput".to_string(),
                    SqlValue::String("multirange_in".to_string()),
                );
                type_row.insert(
                    "typoutput".to_string(),
                    SqlValue::String("multirange_out".to_string()),
                );
                type_row.insert(
                    "typreceive".to_string(),
                    SqlValue::String("multirange_recv".to_string()),
                );
                type_row.insert(
                    "typsend".to_string(),
                    SqlValue::String("multirange_send".to_string()),
                );
                type_row.insert(
                    "typanalyze".to_string(),
                    SqlValue::String("multirange_typanalyze".to_string()),
                );
            }
        }
        rows.push(type_row);

        if matches!(&user_type.kind, UserTypeKind::Shell) {
            continue;
        }

        let mut array_row = pg_type_row(
            &format!("_{}", user_type.name),
            user_type.array_oid,
            user_type.oid,
            0,
            true,
        );
        array_row.insert(
            "typnamespace".to_string(),
            SqlValue::Int(namespace_oid(&user_type.schema_name)),
        );
        array_row.insert(
            "typowner".to_string(),
            SqlValue::Int(role_oid(&user_type.owner)),
        );
        array_row.insert(
            "typdelim".to_string(),
            SqlValue::String(column_type.scalar_delimiter().to_string()),
        );
        array_row.insert(
            "typalign".to_string(),
            SqlValue::String(
                if column_type.scalar_align() == 'd' {
                    'd'
                } else {
                    'i'
                }
                .to_string(),
            ),
        );
        array_row.insert(
            "typcollation".to_string(),
            SqlValue::Int(column_type.scalar_collation_oid()),
        );
        rows.push(array_row);
    }
    for schema in relation_schemas(db)? {
        let namespace = namespace_oid(&schema.schema_name);
        let relation_oid = table_oids(db).get(&schema.name).copied().unwrap_or(0);
        let type_oid = schema.row_type_oid();
        let array_oid = schema.row_array_type_oid();
        let mut type_row = pg_type_row(&schema.name, type_oid, 0, array_oid, false);
        type_row.insert("typnamespace".to_string(), SqlValue::Int(namespace));
        type_row.insert("typtype".to_string(), SqlValue::String("c".to_string()));
        type_row.insert("typcategory".to_string(), SqlValue::String("C".to_string()));
        type_row.insert("typrelid".to_string(), SqlValue::Int(relation_oid));
        type_row.insert("typlen".to_string(), SqlValue::Int(-1));
        type_row.insert("typbyval".to_string(), SqlValue::Bool(false));
        type_row.insert("typalign".to_string(), SqlValue::String("d".to_string()));
        type_row.insert("typstorage".to_string(), SqlValue::String("x".to_string()));
        type_row.insert(
            "typinput".to_string(),
            SqlValue::String("record_in".to_string()),
        );
        type_row.insert(
            "typoutput".to_string(),
            SqlValue::String("record_out".to_string()),
        );
        type_row.insert(
            "typreceive".to_string(),
            SqlValue::String("record_recv".to_string()),
        );
        type_row.insert(
            "typsend".to_string(),
            SqlValue::String("record_send".to_string()),
        );
        rows.push(type_row);

        let mut array_row = pg_type_row(&format!("_{}", schema.name), array_oid, type_oid, 0, true);
        array_row.insert("typnamespace".to_string(), SqlValue::Int(namespace));
        array_row.insert("typalign".to_string(), SqlValue::String("d".to_string()));
        rows.push(array_row);
    }
    Ok(rows)
}

fn catalog_base_type_default(default_expr: &str) -> String {
    parse_single_quoted_sql_string(default_expr)
        .ok()
        .flatten()
        .unwrap_or_else(|| default_expr.to_string())
}

pub(crate) fn pg_enum_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let mut rows = Vec::new();
    for user_type in list_user_types(db)? {
        let UserTypeKind::Enum { labels } = user_type.kind else {
            continue;
        };
        rows.extend(labels.into_iter().map(|label| {
            virtual_row([
                ("oid", SqlValue::Int(label.oid)),
                ("enumtypid", SqlValue::Int(user_type.oid)),
                ("enumsortorder", SqlValue::Float(label.sort_order)),
                ("enumlabel", SqlValue::String(label.label)),
            ])
        }));
    }
    Ok(rows)
}

pub(crate) fn pg_range_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let mut rows = PG_TYPE_SPECS
        .iter()
        .filter_map(|spec| {
            Some(virtual_row([
                ("rngtypid", SqlValue::Int(i64::from(spec.oid))),
                (
                    "rngsubtype",
                    SqlValue::Int(i64::from(spec.range_subtype_oid()?)),
                ),
                (
                    "rngmultitypid",
                    SqlValue::Int(i64::from(spec.range_multirange_oid()?)),
                ),
                ("rngcollation", SqlValue::Int(0)),
                (
                    "rngsubopc",
                    SqlValue::Int(i64::from(spec.range_subopclass_oid()?)),
                ),
                (
                    "rngcanonical",
                    SqlValue::Int(i64::from(spec.range_canonical_oid()?)),
                ),
                (
                    "rngsubdiff",
                    SqlValue::Int(i64::from(spec.range_subdiff_oid()?)),
                ),
            ]))
        })
        .collect::<Vec<_>>();
    for user_type in list_user_types(db)? {
        let UserTypeKind::Range {
            value,
            multirange_oid,
            ..
        } = user_type.kind
        else {
            continue;
        };
        rows.push(virtual_row([
            ("rngtypid", SqlValue::Int(user_type.oid)),
            ("rngsubtype", SqlValue::Int(value.subtype_oid)),
            ("rngmultitypid", SqlValue::Int(multirange_oid)),
            (
                "rngcollation",
                SqlValue::Int(
                    value
                        .collation
                        .as_deref()
                        .and_then(collation_oid)
                        .unwrap_or(0),
                ),
            ),
            ("rngsubopc", SqlValue::Int(value.subtype_opclass_oid)),
            ("rngcanonical", SqlValue::Int(value.canonical_oid)),
            ("rngsubdiff", SqlValue::Int(value.subtype_diff_oid)),
        ]));
    }
    Ok(rows)
}

fn user_type_output_function(
    base_type: &str,
    base_user_type: Option<&UserTypeColumnSchema>,
) -> String {
    match base_user_type.map(|base| &base.kind) {
        Some(UserTypeKind::Shell) => "shell_out".to_string(),
        Some(UserTypeKind::Base { output, .. }) => output.clone(),
        Some(UserTypeKind::Enum { .. }) => "enum_out".to_string(),
        Some(UserTypeKind::Composite { .. }) => "record_out".to_string(),
        Some(UserTypeKind::Range { .. }) => "range_out".to_string(),
        Some(UserTypeKind::Multirange { .. }) => "multirange_out".to_string(),
        Some(UserTypeKind::Domain {
            base_type,
            base_user_type,
            ..
        }) => user_type_output_function(base_type, base_user_type.as_deref()),
        None if base_type.ends_with("[]") => "array_out".to_string(),
        None => format!("{base_type}_out"),
    }
}

fn user_type_send_function(
    base_type: &str,
    base_user_type: Option<&UserTypeColumnSchema>,
) -> String {
    match base_user_type.map(|base| &base.kind) {
        Some(UserTypeKind::Shell) => "-".to_string(),
        Some(UserTypeKind::Base { send, .. }) => send.clone().unwrap_or_else(|| "-".to_string()),
        Some(UserTypeKind::Enum { .. }) => "enum_send".to_string(),
        Some(UserTypeKind::Composite { .. }) => "record_send".to_string(),
        Some(UserTypeKind::Range { .. }) => "range_send".to_string(),
        Some(UserTypeKind::Multirange { .. }) => "multirange_send".to_string(),
        Some(UserTypeKind::Domain {
            base_type,
            base_user_type,
            ..
        }) => user_type_send_function(base_type, base_user_type.as_deref()),
        None if base_type.ends_with("[]") => "array_send".to_string(),
        None => format!("{base_type}_send"),
    }
}

pub(crate) fn pg_type_row(
    name: &str,
    oid: i64,
    typelem: i64,
    typarray: i64,
    is_array: bool,
) -> BTreeMap<String, SqlValue> {
    let is_record_array = is_array && typelem == i64::from(pg_type_oid_by_name("record").unwrap());
    let delimiter = if is_array {
        name.strip_prefix('_').and_then(pg_type_delimiter)
    } else {
        pg_type_delimiter(name)
    }
    .unwrap_or(',');
    let type_function = |suffix: &str| {
        if is_array {
            format!("array_{suffix}")
        } else if let Some(spec) = pg_type_spec(name) {
            let direction = match suffix {
                "in" => PgInternalCodecDirection::Input,
                "out" => PgInternalCodecDirection::Output,
                "recv" => PgInternalCodecDirection::Receive,
                "send" => PgInternalCodecDirection::Send,
                _ => unreachable!("unknown PostgreSQL codec direction"),
            };
            spec.catalog_codec_symbol(direction)
                .unwrap_or_else(|| "-".to_string())
        } else {
            format!("{name}_{suffix}")
        }
    };
    virtual_row([
        ("oid", SqlValue::Int(oid)),
        ("typname", SqlValue::String(name.to_string())),
        ("typnamespace", SqlValue::Int(11)),
        ("typowner", SqlValue::Int(10)),
        (
            "typlen",
            SqlValue::Int(if is_array { -1 } else { pg_type_len(name) }),
        ),
        (
            "typbyval",
            SqlValue::Bool(!is_array && pg_type_by_value(name)),
        ),
        (
            "typtype",
            SqlValue::String(
                if is_record_array {
                    'p'
                } else if !is_array {
                    pg_type_spec(name).map_or('b', |spec| spec.kind())
                } else {
                    'b'
                }
                .to_string(),
            ),
        ),
        (
            "typcategory",
            SqlValue::String(
                if is_record_array {
                    'P'
                } else if is_array {
                    'A'
                } else {
                    pg_type_category(name)
                }
                .to_string(),
            ),
        ),
        (
            "typispreferred",
            SqlValue::Bool(!is_array && pg_type_spec(name).is_some_and(|spec| spec.preferred())),
        ),
        ("typisdefined", SqlValue::Bool(true)),
        ("typdelim", SqlValue::String(delimiter.to_string())),
        ("typrelid", SqlValue::Int(0)),
        (
            "typsubscript",
            SqlValue::String(
                if is_array {
                    Some("array_subscript_handler")
                } else {
                    pg_type_spec(name).and_then(|spec| spec.subscript_symbol())
                }
                .unwrap_or("-")
                .to_string(),
            ),
        ),
        ("typelem", SqlValue::Int(typelem)),
        ("typarray", SqlValue::Int(typarray)),
        ("typinput", SqlValue::String(type_function("in"))),
        ("typoutput", SqlValue::String(type_function("out"))),
        ("typreceive", SqlValue::String(type_function("recv"))),
        ("typsend", SqlValue::String(type_function("send"))),
        (
            "typmodin",
            SqlValue::String(
                if is_array {
                    pg_array_type_element(name)
                } else {
                    Some(name)
                }
                .and_then(pg_type_spec)
                .and_then(|spec| spec.typmod_symbols())
                .map(|symbols| symbols.0)
                .unwrap_or("-")
                .to_string(),
            ),
        ),
        (
            "typmodout",
            SqlValue::String(
                if is_array {
                    pg_array_type_element(name)
                } else {
                    Some(name)
                }
                .and_then(pg_type_spec)
                .and_then(|spec| spec.typmod_symbols())
                .map(|symbols| symbols.1)
                .unwrap_or("-")
                .to_string(),
            ),
        ),
        (
            "typanalyze",
            SqlValue::String(
                if is_array {
                    Some("array_typanalyze")
                } else {
                    pg_type_spec(name).and_then(|spec| spec.analyze_symbol())
                }
                .unwrap_or("-")
                .to_string(),
            ),
        ),
        (
            "typalign",
            SqlValue::String(
                if is_array {
                    pg_array_type_element(name)
                        .and_then(pg_type_spec)
                        .map_or('i', |spec| spec.array_alignment())
                } else {
                    pg_type_align(name)
                }
                .to_string(),
            ),
        ),
        (
            "typstorage",
            SqlValue::String(if is_array { 'x' } else { pg_type_storage(name) }.to_string()),
        ),
        ("typnotnull", SqlValue::Bool(false)),
        ("typbasetype", SqlValue::Int(0)),
        ("typtypmod", SqlValue::Int(-1)),
        ("typndims", SqlValue::Int(0)),
        (
            "typcollation",
            SqlValue::Int(if is_array {
                pg_array_type_element(name).map_or(0, type_collation_oid)
            } else {
                type_collation_oid(name)
            }),
        ),
        ("typdefaultbin", SqlValue::Null),
        ("typdefault", SqlValue::Null),
        ("typacl", SqlValue::Null),
    ])
}

pub(crate) fn pg_array_type_element(array_type: &str) -> Option<&'static str> {
    let normalized = array_type.trim().to_ascii_lowercase();
    let element = normalized
        .strip_prefix('_')
        .or_else(|| normalized.strip_suffix("[]"))?;
    pg_type_spec(element).map(|spec| spec.name)
}

pub(crate) fn pg_extension_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let mut rows = vec![virtual_row([
        ("oid", SqlValue::Int(PLPGSQL_EXTENSION_OID)),
        ("extname", SqlValue::String("plpgsql".to_string())),
        ("extowner", SqlValue::Int(10)),
        ("extnamespace", SqlValue::Int(namespace_oid("pg_catalog"))),
        ("extrelocatable", SqlValue::Bool(false)),
        ("extversion", SqlValue::String("1.0".to_string())),
        ("extconfig", SqlValue::Null),
        ("extcondition", SqlValue::Null),
    ])];
    rows.extend(
        list_extensions(db)?
            .into_iter()
            .filter(|extension| !extension.name.eq_ignore_ascii_case("plpgsql"))
            .map(|extension| {
                virtual_row([
                    (
                        "oid",
                        SqlValue::Int(stable_name_hash(90_000, &extension.name)),
                    ),
                    ("extname", SqlValue::String(extension.name.clone())),
                    ("extowner", SqlValue::Int(10)),
                    (
                        "extnamespace",
                        SqlValue::Int(namespace_oid(&extension.schema)),
                    ),
                    ("extrelocatable", SqlValue::Bool(false)),
                    (
                        "extversion",
                        extension
                            .version
                            .map(SqlValue::String)
                            .unwrap_or_else(|| SqlValue::String("1.0".to_string())),
                    ),
                    ("extconfig", SqlValue::Null),
                    ("extcondition", SqlValue::Null),
                ])
            }),
    );
    Ok(rows)
}

pub(crate) fn pg_depend_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let mut rows = [
        (PG_PROC_CATALOG_OID, PLPGSQL_CALL_HANDLER_OID),
        (PG_PROC_CATALOG_OID, PLPGSQL_INLINE_HANDLER_OID),
        (PG_PROC_CATALOG_OID, PLPGSQL_VALIDATOR_OID),
        (PG_LANGUAGE_CATALOG_OID, PLPGSQL_LANGUAGE_OID),
    ]
    .into_iter()
    .map(|(classid, objid)| {
        virtual_row([
            ("classid", SqlValue::Int(classid)),
            ("objid", SqlValue::Int(objid)),
            ("objsubid", SqlValue::Int(0)),
            ("refclassid", SqlValue::Int(PG_EXTENSION_CATALOG_OID)),
            ("refobjid", SqlValue::Int(PLPGSQL_EXTENSION_OID)),
            ("refobjsubid", SqlValue::Int(0)),
            ("deptype", SqlValue::String("e".to_string())),
        ])
    })
    .collect::<Vec<_>>();
    let dependency_row =
        |classid: i64, objid: i64, objsubid: i64, refclassid: i64, refobjid: i64, deptype: &str| {
            virtual_row([
                ("classid", SqlValue::Int(classid)),
                ("objid", SqlValue::Int(objid)),
                ("objsubid", SqlValue::Int(objsubid)),
                ("refclassid", SqlValue::Int(refclassid)),
                ("refobjid", SqlValue::Int(refobjid)),
                ("refobjsubid", SqlValue::Int(0)),
                ("deptype", SqlValue::String(deptype.to_string())),
            ])
        };
    let user_types = list_user_types(db)?;
    let routine_oids = list_routines(db)?
        .into_iter()
        .map(|routine| {
            (
                routine.name.to_ascii_lowercase(),
                routine_oid(routine.kind, &routine.name),
            )
        })
        .collect::<BTreeMap<_, _>>();
    for user_type in &user_types {
        rows.push(dependency_row(
            PG_TYPE_CATALOG_OID,
            user_type.oid,
            0,
            PG_NAMESPACE_CATALOG_OID,
            namespace_oid(&user_type.schema_name),
            "n",
        ));
        if user_type.array_oid != 0 {
            rows.push(dependency_row(
                PG_TYPE_CATALOG_OID,
                user_type.array_oid,
                0,
                PG_TYPE_CATALOG_OID,
                user_type.oid,
                "i",
            ));
        }
        match &user_type.kind {
            UserTypeKind::Base {
                input,
                output,
                receive,
                send,
                ..
            } => {
                for routine_name in [Some(input), Some(output), receive.as_ref(), send.as_ref()]
                    .into_iter()
                    .flatten()
                {
                    if let Some(routine_oid) = routine_oids.get(&routine_name.to_ascii_lowercase())
                    {
                        rows.push(dependency_row(
                            PG_TYPE_CATALOG_OID,
                            user_type.oid,
                            0,
                            PG_PROC_CATALOG_OID,
                            *routine_oid,
                            "n",
                        ));
                    }
                }
            }
            UserTypeKind::Multirange { range_oid, .. } => rows.push(dependency_row(
                PG_TYPE_CATALOG_OID,
                user_type.oid,
                0,
                PG_TYPE_CATALOG_OID,
                *range_oid,
                "i",
            )),
            UserTypeKind::Domain {
                base_user_type: Some(base),
                ..
            } => {
                rows.push(dependency_row(
                    PG_TYPE_CATALOG_OID,
                    user_type.oid,
                    0,
                    PG_TYPE_CATALOG_OID,
                    base.oid,
                    "n",
                ));
                if let UserTypeKind::Base { output, send, .. } = &base.kind {
                    for routine_name in [Some(output), send.as_ref()].into_iter().flatten() {
                        if let Some(routine_oid) =
                            routine_oids.get(&routine_name.to_ascii_lowercase())
                        {
                            rows.push(dependency_row(
                                PG_TYPE_CATALOG_OID,
                                user_type.oid,
                                0,
                                PG_PROC_CATALOG_OID,
                                *routine_oid,
                                "n",
                            ));
                        }
                    }
                }
            }
            UserTypeKind::Composite {
                relation_oid,
                attributes,
            } => {
                rows.push(dependency_row(
                    PG_CLASS_CATALOG_OID,
                    *relation_oid,
                    0,
                    PG_TYPE_CATALOG_OID,
                    user_type.oid,
                    "i",
                ));
                for (index, attribute) in attributes
                    .iter()
                    .enumerate()
                    .filter(|(_, attribute)| !attribute.dropped)
                {
                    if let Some(dependency) = &attribute.user_type {
                        rows.push(dependency_row(
                            PG_CLASS_CATALOG_OID,
                            *relation_oid,
                            index as i64 + 1,
                            PG_TYPE_CATALOG_OID,
                            dependency.oid,
                            "n",
                        ));
                    }
                }
            }
            _ => {}
        }

        if let UserTypeKind::Range {
            value,
            multirange_schema_name,
            multirange_name,
            multirange_oid,
        } = &user_type.kind
        {
            for arg_oids in [
                vec![value.subtype_oid, value.subtype_oid],
                vec![value.subtype_oid, value.subtype_oid, 25],
            ] {
                let signature = format!(
                    "{}.{}({})",
                    user_type.schema_name,
                    user_type.name,
                    arg_oids
                        .iter()
                        .map(i64::to_string)
                        .collect::<Vec<_>>()
                        .join(",")
                );
                let constructor_oid = routine_oid(RoutineKind::Function, &signature);
                for dependency_type in ["i", "n"] {
                    rows.push(dependency_row(
                        PG_PROC_CATALOG_OID,
                        constructor_oid,
                        0,
                        PG_TYPE_CATALOG_OID,
                        user_type.oid,
                        dependency_type,
                    ));
                }
            }

            for arg_oids in [Vec::new(), vec![user_type.oid], vec![user_type.array_oid]] {
                let signature = format!(
                    "{multirange_schema_name}.{multirange_name}({})",
                    arg_oids
                        .iter()
                        .map(i64::to_string)
                        .collect::<Vec<_>>()
                        .join(",")
                );
                let constructor_oid = routine_oid(RoutineKind::Function, &signature);
                for dependency_type in ["i", "n"] {
                    rows.push(dependency_row(
                        PG_PROC_CATALOG_OID,
                        constructor_oid,
                        0,
                        PG_TYPE_CATALOG_OID,
                        *multirange_oid,
                        dependency_type,
                    ));
                }
                if let Some(argument_oid) = arg_oids.first() {
                    rows.push(dependency_row(
                        PG_PROC_CATALOG_OID,
                        constructor_oid,
                        0,
                        PG_TYPE_CATALOG_OID,
                        *argument_oid,
                        "n",
                    ));
                }
            }

            let cast_signature = format!(
                "{multirange_schema_name}.{multirange_name}({})",
                user_type.oid
            );
            let cast_oid = stable_name_hash_wide(
                2_000_000_000,
                &format!("pg_cast:{cast_signature}"),
                100_000_000,
            );
            let cast_function_oid = routine_oid(RoutineKind::Function, &cast_signature);
            for (refclassid, refobjid) in [
                (PG_PROC_CATALOG_OID, cast_function_oid),
                (PG_TYPE_CATALOG_OID, user_type.oid),
                (PG_TYPE_CATALOG_OID, *multirange_oid),
            ] {
                rows.push(dependency_row(
                    PG_CAST_CATALOG_OID,
                    cast_oid,
                    0,
                    refclassid,
                    refobjid,
                    "i",
                ));
            }
        }
    }
    let schemas = list_schemas(db)?;
    let relation_oids = table_oids(db);
    for schema in &schemas {
        let Some(relation_oid) = relation_oids.get(&schema.name).copied() else {
            continue;
        };
        rows.push(dependency_row(
            PG_TYPE_CATALOG_OID,
            schema.row_type_oid(),
            0,
            PG_CLASS_CATALOG_OID,
            relation_oid,
            "i",
        ));
        rows.push(dependency_row(
            PG_TYPE_CATALOG_OID,
            schema.row_array_type_oid(),
            0,
            PG_TYPE_CATALOG_OID,
            schema.row_type_oid(),
            "i",
        ));
        for (index, column) in schema.columns.iter().enumerate() {
            if let Some(user_type) = &column.user_type {
                rows.push(dependency_row(
                    PG_CLASS_CATALOG_OID,
                    relation_oid,
                    index as i64 + 1,
                    PG_TYPE_CATALOG_OID,
                    user_type.oid,
                    "n",
                ));
            }
        }
    }
    for routine in list_routines(db)? {
        for user_type in &user_types {
            if routine_depends_on_catalog_user_type(&routine, user_type) {
                rows.push(dependency_row(
                    PG_PROC_CATALOG_OID,
                    routine_oid(routine.kind, &routine.name),
                    0,
                    PG_TYPE_CATALOG_OID,
                    user_type.oid,
                    "n",
                ));
            }
        }
    }
    rows.sort_by(|left, right| {
        ["classid", "objid", "objsubid", "refclassid", "refobjid"]
            .into_iter()
            .map(|column| virtual_cell(left, column).to_cell())
            .cmp(
                ["classid", "objid", "objsubid", "refclassid", "refobjid"]
                    .into_iter()
                    .map(|column| virtual_cell(right, column).to_cell()),
            )
    });
    rows.dedup();
    Ok(rows)
}

fn routine_depends_on_catalog_user_type(
    routine: &RoutineSchema,
    user_type: &UserTypeSchema,
) -> bool {
    let matches = |declaration: &str| {
        let declaration = declaration.trim().trim_end_matches("[]").trim_matches('"');
        let qualified = format!("{}.{}", user_type.schema_name, user_type.name);
        declaration.eq_ignore_ascii_case(&qualified)
            || (user_type.schema_name == "public"
                && declaration.eq_ignore_ascii_case(&user_type.name))
    };
    matches(&routine.return_type)
        || routine
            .arg_types
            .iter()
            .any(|argument| matches(&argument.pg_type))
}

pub(crate) fn pg_language_rows() -> Vec<BTreeMap<String, SqlValue>> {
    vec![
        pg_language_row(12, "internal", false, false, 0, 0, 2246),
        pg_language_row(13, "c", false, false, 0, 0, 2247),
        pg_language_row(14, "sql", false, true, 0, 0, 2248),
        pg_language_row(
            PLPGSQL_LANGUAGE_OID,
            "plpgsql",
            true,
            true,
            PLPGSQL_CALL_HANDLER_OID,
            PLPGSQL_INLINE_HANDLER_OID,
            PLPGSQL_VALIDATOR_OID,
        ),
    ]
}

pub(crate) fn pg_language_row(
    oid: i64,
    name: &str,
    is_pl: bool,
    is_trusted: bool,
    call_handler: i64,
    inline_handler: i64,
    validator: i64,
) -> BTreeMap<String, SqlValue> {
    virtual_row([
        ("oid", SqlValue::Int(oid)),
        ("lanname", SqlValue::String(name.to_string())),
        ("lanowner", SqlValue::Int(10)),
        ("lanispl", SqlValue::Bool(is_pl)),
        ("lanpltrusted", SqlValue::Bool(is_trusted)),
        ("lanplcallfoid", SqlValue::Int(call_handler)),
        ("laninline", SqlValue::Int(inline_handler)),
        ("lanvalidator", SqlValue::Int(validator)),
        ("lanacl", SqlValue::Null),
    ])
}

pub(crate) fn pg_language_oid(language: &str) -> i64 {
    match language.to_ascii_lowercase().as_str() {
        "internal" => 12,
        "c" => 13,
        "sql" => 14,
        "plpgsql" => PLPGSQL_LANGUAGE_OID,
        _ => 0,
    }
}

pub(crate) const PG_BUILTIN_PROC_ROWS: &[(i64, &str, i64, i64)] = &[
    (3, "heap_tableam_handler", 269, 1),
    (330, "bthandler", 325, 1),
    (331, "hashhandler", 325, 1),
    (332, "gisthandler", 325, 1),
    (333, "ginhandler", 325, 1),
    (334, "spghandler", 325, 1),
    (335, "brinhandler", 325, 1),
    (1000, "current_database", 25, 0),
    (1001, "current_schema", 25, 0),
    (1002, "version", 25, 0),
    (1023, "bicdb_version", 25, 0),
    (1003, "now", 1184, 0),
    (1004, "has_schema_privilege", 16, 0),
    (1005, "has_table_privilege", 16, 0),
    (1006, "pg_get_constraintdef", 25, 0),
    (1007, "pg_get_indexdef", 25, 0),
    (1008, "obj_description", 25, 0),
    (1009, "col_description", 25, 0),
    (1010, "nextval", 20, 0),
    (1011, "currval", 20, 0),
    (1012, "setval", 20, 0),
    (1013, "pg_get_serial_sequence", 25, 0),
    (1014, "format_type", 25, 0),
    (1015, "pg_get_expr", 25, 0),
    (1016, "pg_table_is_visible", 16, 0),
    (1017, "pg_type_is_visible", 16, 0),
    (1018, "pg_function_is_visible", 16, 0),
    (1019, "pg_is_other_temp_schema", 16, 0),
    (1020, "has_column_privilege", 16, 0),
    (1021, "pg_get_userbyid", 25, 0),
    (1022, "has_type_privilege", 16, 0),
    (1024, "has_function_privilege", 16, 0),
    (3432, "gen_random_uuid", 2950, 0),
    (6342, "uuid_extract_timestamp", 1184, 1),
    (6343, "uuid_extract_version", 21, 1),
    (6428, "uuidv4", 2950, 0),
    (6429, "uuidv7", 2950, 0),
    (6430, "uuidv7", 2950, 1),
];

pub(crate) const PG_PLPGSQL_PROC_ROWS: &[(i64, &str, i64, &str, &str, i64)] = &[
    (
        PLPGSQL_CALL_HANDLER_OID,
        "plpgsql_call_handler",
        2280,
        "c",
        "plpgsql_call_handler",
        0,
    ),
    (
        PLPGSQL_INLINE_HANDLER_OID,
        "plpgsql_inline_handler",
        2278,
        "c",
        "plpgsql_inline_handler",
        1,
    ),
    (
        PLPGSQL_VALIDATOR_OID,
        "plpgsql_validator",
        2278,
        "c",
        "plpgsql_validator",
        1,
    ),
];

pub(crate) fn pg_proc_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let mut rows = PG_BUILTIN_PROC_ROWS
        .iter()
        .map(|&(oid, name, return_type, nargs)| {
            let mut row = proc_row(
                oid,
                name,
                11,
                return_type,
                false,
                "f",
                "sql",
                name,
                nargs,
                BOOTSTRAP_ROLE_NAME,
                false,
            );
            if matches!(
                name,
                "gen_random_uuid"
                    | "uuidv4"
                    | "uuidv7"
                    | "uuid_extract_timestamp"
                    | "uuid_extract_version"
            ) {
                row.insert("proisstrict".to_string(), SqlValue::Bool(true));
                row.insert(
                    "provolatile".to_string(),
                    SqlValue::String(
                        if name.starts_with("uuid_extract_") {
                            "i"
                        } else {
                            "v"
                        }
                        .to_string(),
                    ),
                );
            }
            row
        })
        .collect::<Vec<_>>();
    rows.extend(PG_PLPGSQL_PROC_ROWS.iter().map(
        |(oid, name, return_type, language, source, nargs)| {
            proc_row(
                *oid,
                name,
                11,
                *return_type,
                false,
                "f",
                language,
                source,
                *nargs,
                BOOTSTRAP_ROLE_NAME,
                false,
            )
        },
    ));
    for user_type in list_user_types(db)? {
        let UserTypeKind::Range {
            value,
            multirange_schema_name,
            multirange_name,
            multirange_oid,
        } = &user_type.kind
        else {
            continue;
        };
        let namespace = namespace_oid(&user_type.schema_name);
        let mut add_constructor = |name: &str, return_oid: i64, arg_oids: &[i64], source: &str| {
            let signature = format!(
                "{}.{}({})",
                user_type.schema_name,
                name,
                arg_oids
                    .iter()
                    .map(i64::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            );
            let mut row = proc_row(
                routine_oid(RoutineKind::Function, &signature),
                name,
                namespace,
                return_oid,
                false,
                "f",
                "internal",
                source,
                arg_oids.len() as i64,
                &user_type.owner,
                false,
            );
            row.insert(
                "proargtypes".to_string(),
                SqlValue::String(
                    arg_oids
                        .iter()
                        .map(i64::to_string)
                        .collect::<Vec<_>>()
                        .join(" "),
                ),
            );
            row.insert(
                "proisstrict".to_string(),
                SqlValue::Bool(!arg_oids.is_empty()),
            );
            rows.push(row);
        };
        add_constructor(
            &user_type.name,
            user_type.oid,
            &[value.subtype_oid, value.subtype_oid],
            "range_constructor2",
        );
        add_constructor(
            &user_type.name,
            user_type.oid,
            &[value.subtype_oid, value.subtype_oid, 25],
            "range_constructor3",
        );
        let multirange_namespace = namespace_oid(multirange_schema_name);
        let constructors = [
            (Vec::new(), "multirange_constructor0"),
            (vec![user_type.oid], "multirange_constructor1"),
            (vec![user_type.array_oid], "multirange_constructor2"),
        ];
        for (arg_oids, source) in constructors {
            let signature = format!(
                "{multirange_schema_name}.{multirange_name}({})",
                arg_oids
                    .iter()
                    .map(i64::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            );
            let mut row = proc_row(
                routine_oid(RoutineKind::Function, &signature),
                multirange_name,
                multirange_namespace,
                *multirange_oid,
                false,
                "f",
                "internal",
                source,
                arg_oids.len() as i64,
                &user_type.owner,
                false,
            );
            row.insert(
                "proargtypes".to_string(),
                SqlValue::String(
                    arg_oids
                        .iter()
                        .map(i64::to_string)
                        .collect::<Vec<_>>()
                        .join(" "),
                ),
            );
            row.insert(
                "proisstrict".to_string(),
                SqlValue::Bool(!arg_oids.is_empty()),
            );
            rows.push(row);
        }
    }
    for routine in list_routines(db)? {
        let prokind = match routine.kind {
            RoutineKind::Function => "f",
            RoutineKind::Procedure => "p",
        };
        let parsed_params = parse_routine_params(&routine.args).unwrap_or_default();
        let input_arg_oids = routine
            .arg_types
            .iter()
            .enumerate()
            .filter(|(index, _)| {
                parsed_params
                    .get(*index)
                    .is_none_or(|param| param.mode != RoutineArgMode::Out)
            })
            .map(|(_, arg_type)| routine_catalog_type_oid(db, &arg_type.pg_type))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|oid| oid.to_string())
            .collect::<Vec<_>>();
        let mut row = proc_row(
            routine_oid(routine.kind, &routine.name),
            &routine.name,
            namespace_oid(routine_schema_name(&routine)),
            routine_catalog_type_oid(db, &routine.return_type)?,
            routine.returns_set,
            prokind,
            &routine.language,
            routine
                .internal_symbol
                .as_deref()
                .unwrap_or(&routine.definition),
            input_arg_oids.len() as i64,
            routine.owner(),
            routine.security_definer,
        );
        row.insert(
            "pronargdefaults".to_string(),
            SqlValue::Int(
                parsed_params
                    .iter()
                    .filter(|param| {
                        param.mode != RoutineArgMode::Out && param.default_expr.is_some()
                    })
                    .count() as i64,
            ),
        );
        row.insert(
            "proargtypes".to_string(),
            SqlValue::String(input_arg_oids.join(" ")),
        );
        rows.push(row);
    }
    Ok(rows)
}

fn routine_catalog_type_oid(db: &BicDb, pg_type: &str) -> Result<i64> {
    let (name, array) = pg_type
        .strip_suffix("[]")
        .map(|name| (name, true))
        .unwrap_or((pg_type, false));
    let (schema_name, name) = name.rsplit_once('.').unwrap_or(("public", name));
    if let Some(user_type) = load_user_type(db, schema_name, name)? {
        return Ok(if array {
            user_type.array_oid
        } else {
            user_type.oid
        });
    }
    Ok(pg_type_oid(pg_type))
}

pub(crate) fn pg_trigger_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    pg_trigger_rows_filtered(db, None, None)
}

pub(crate) fn pg_trigger_rows_filtered(
    db: &BicDb,
    names: Option<&BTreeSet<String>>,
    relids: Option<&BTreeSet<i64>>,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let table_oids = table_oids(db);
    Ok(list_triggers(db)?
        .into_iter()
        .filter(|trigger| catalog_name_matches(names, &trigger.name))
        .filter(|trigger| {
            relids.is_none_or(|relids| {
                table_oids
                    .get(&trigger.table_name)
                    .is_some_and(|oid| relids.contains(oid))
            })
        })
        .map(|trigger| {
            virtual_row([
                (
                    "oid",
                    SqlValue::Int(trigger_oid(&trigger.table_name, &trigger.name)),
                ),
                (
                    "tgrelid",
                    SqlValue::Int(*table_oids.get(&trigger.table_name).unwrap_or(&0)),
                ),
                ("tgparentid", SqlValue::Int(0)),
                ("tgname", SqlValue::String(trigger.name.clone())),
                (
                    "tgfoid",
                    SqlValue::Int(routine_oid(RoutineKind::Function, &trigger.function_name)),
                ),
                (
                    "tgtype",
                    SqlValue::Int({
                        let mut bits = if trigger.for_each.eq_ignore_ascii_case("row") {
                            1
                        } else {
                            0
                        };
                        if trigger.timing.eq_ignore_ascii_case("before") {
                            bits |= 2;
                        }
                        if trigger.timing.eq_ignore_ascii_case("instead of") {
                            bits |= 64;
                        }
                        for (event, mask) in [
                            ("INSERT", 4),
                            ("DELETE", 8),
                            ("UPDATE", 16),
                            ("TRUNCATE", 32),
                        ] {
                            if trigger.fires_on(event) {
                                bits |= mask;
                            }
                        }
                        bits
                    }),
                ),
                (
                    "tgenabled",
                    SqlValue::String(trigger.pg_enabled_code().to_string()),
                ),
                ("tgisinternal", SqlValue::Bool(false)),
                ("tgconstrrelid", SqlValue::Int(0)),
                ("tgconstrindid", SqlValue::Int(0)),
                ("tgconstraint", SqlValue::Int(0)),
                ("tgdeferrable", SqlValue::Bool(false)),
                ("tginitdeferred", SqlValue::Bool(false)),
                ("tgnargs", SqlValue::Int(0)),
                ("tgattr", SqlValue::String(String::new())),
                ("tgargs", SqlValue::String(String::new())),
                ("tgqual", SqlValue::Null),
                ("tgoldtable", SqlValue::Null),
                ("tgnewtable", SqlValue::Null),
            ])
        })
        .collect())
}

pub(crate) fn bicdb_notification_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    Ok(list_notifications(db)?
        .into_iter()
        .map(|notification| {
            virtual_row([
                ("id", SqlValue::Int(notification.id)),
                ("channel", SqlValue::String(notification.channel)),
                ("payload", SqlValue::String(notification.payload)),
                ("table_name", SqlValue::String(notification.table_name)),
                ("trigger_name", SqlValue::String(notification.trigger_name)),
                (
                    "function_name",
                    SqlValue::String(notification.function_name),
                ),
                ("created_at", SqlValue::Int(notification.created_at)),
            ])
        })
        .collect())
}

/// How a virtual-catalog row names the relation it describes.
pub(crate) enum CatalogRelationKey {
    /// The relation name is in this column.
    Name(&'static str),
    /// The relation's OID is in this column.
    Oid(&'static str),
}

/// Virtual catalogs whose rows carry per-relation tenant data, and the column
/// that ties each row to its relation.
///
/// These are read through `SqlEngine::filter_virtual_rows_for_role`, which is
/// the only place a session's identity is available — the row builders take a
/// bare `&BicDb`. Anything listed here becomes visible only to roles that
/// could SELECT the relation the row belongs to.
pub(crate) fn virtual_catalog_relation_key(table: &str) -> Option<CatalogRelationKey> {
    let stripped = table.strip_prefix("pg_catalog.").unwrap_or(table);
    Some(match stripped {
        "bicdb_notifications" => CatalogRelationKey::Name("table_name"),
        // pg_stats publishes most_common_vals/histogram_bounds — ACTUAL column
        // values, sampled over every row and computed with RLS ignored. It is
        // the single worst leak of the set: a role with no privilege on a
        // table under FORCE ROW LEVEL SECURITY could read its data out of the
        // statistics. PostgreSQL restricts pg_statistic to superusers and
        // filters pg_stats by privilege for exactly this reason.
        "pg_stats" => CatalogRelationKey::Name("tablename"),
        // Policy quals disclose the security logic and any literals embedded
        // in it (tenant ids, keys).
        "pg_policies" => CatalogRelationKey::Name("tablename"),
        "pg_policy" => CatalogRelationKey::Oid("polrelid"),
        // Column defaults routinely carry tokens and secrets.
        "pg_attrdef" => CatalogRelationKey::Oid("adrelid"),
        _ => return None,
    })
}

/// Virtual catalogs that must stay structurally visible — tools and drivers
/// introspect them — but that carry a per-relation STATISTIC a role with no
/// access to the relation has no business reading. The row is kept; the listed
/// columns are zeroed.
///
/// PostgreSQL exposes these broadly, so this is tenancy hardening rather than
/// PostgreSQL parity: row counts are a real signal about another tenant's data
/// (size, growth, whether a probe landed), and nothing needs them to introspect
/// a schema.
pub(crate) fn virtual_catalog_redactions(
    table: &str,
) -> Option<(CatalogRelationKey, &'static [&'static str])> {
    let stripped = table.strip_prefix("pg_catalog.").unwrap_or(table);
    Some(match stripped {
        "pg_class" => (
            CatalogRelationKey::Name("relname"),
            &["reltuples", "relpages"],
        ),
        "pg_stat_all_tables" | "pg_stat_user_tables" => (
            CatalogRelationKey::Name("relname"),
            &[
                "n_live_tup",
                "n_dead_tup",
                "n_tup_ins",
                "n_tup_upd",
                "n_tup_del",
                "n_tup_hot_upd",
                "seq_tup_read",
                "idx_tup_fetch",
            ],
        ),
        _ => return None,
    })
}

/// Zero the listed columns on rows whose relation the role cannot read,
/// preserving the row itself.
pub(crate) fn redact_catalog_rows_for_role(
    db: &BicDb,
    role: &str,
    key: &CatalogRelationKey,
    columns: &[&str],
    mut rows: Vec<BTreeMap<String, SqlValue>>,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    if normalize_role_name(role) == BOOTSTRAP_ROLE_NAME || pg_role_is_superuser(db, role)? {
        return Ok(rows);
    }
    let CatalogRelationKey::Name(relation_column) = key else {
        return Ok(rows);
    };
    for row in &mut rows {
        let table = sql_value_text(&virtual_cell(row, relation_column)).unwrap_or_default();
        if !table.is_empty() && role_has_table_privilege(db, role, &table, "SELECT")? {
            continue;
        }
        for column in columns {
            if let Some(value) = row.get_mut(*column) {
                // Keep the column's type: a driver reading reltuples as a float
                // must not suddenly get an integer.
                *value = match value {
                    SqlValue::Float(_) => SqlValue::Float(0.0),
                    _ => SqlValue::Int(0),
                };
            }
        }
    }
    Ok(rows)
}

/// Per-role visibility for one virtual catalog. Rows whose relation cannot be
/// resolved are dropped rather than shown: an unattributable row cannot be
/// authorized, so the filter fails closed.
pub(crate) fn filter_catalog_rows_for_role(
    db: &BicDb,
    role: &str,
    key: &CatalogRelationKey,
    rows: Vec<BTreeMap<String, SqlValue>>,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    if normalize_role_name(role) == BOOTSTRAP_ROLE_NAME || pg_role_is_superuser(db, role)? {
        return Ok(rows);
    }
    let names_by_oid = match key {
        CatalogRelationKey::Oid(_) => (*table_oids(db))
            .clone()
            .into_iter()
            .map(|(name, oid)| (oid, name))
            .collect::<BTreeMap<i64, String>>(),
        CatalogRelationKey::Name(_) => BTreeMap::new(),
    };
    let mut kept = Vec::with_capacity(rows.len());
    for row in rows {
        let table = match key {
            CatalogRelationKey::Name(column) => {
                sql_value_text(&virtual_cell(&row, column)).unwrap_or_default()
            }
            CatalogRelationKey::Oid(column) => sql_value_i64(&virtual_cell(&row, column))
                .and_then(|oid| names_by_oid.get(&oid).cloned())
                .unwrap_or_default(),
        };
        if !table.is_empty() && role_has_table_privilege(db, role, &table, "SELECT")? {
            kept.push(row);
        }
    }
    Ok(kept)
}

pub(crate) fn bicdb_replication_status_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let watermark = db.replication_watermark();
    let apply_state = db.replication_apply_state();
    let retention = db.replication_retention_status()?;
    Ok(vec![virtual_row([
        ("cluster_id", SqlValue::String(apply_state.cluster_id)),
        (
            "source_node_id",
            apply_state
                .source_node_id
                .map(SqlValue::String)
                .unwrap_or(SqlValue::Null),
        ),
        (
            "stream_id",
            apply_state
                .stream_id
                .map(SqlValue::String)
                .unwrap_or(SqlValue::Null),
        ),
        (
            "current_commit_seq",
            SqlValue::Int(watermark.current_commit_seq as i64),
        ),
        (
            "last_applied_commit_seq",
            SqlValue::Int(watermark.last_applied_commit_seq as i64),
        ),
        (
            "oldest_available_commit_seq",
            SqlValue::Int(retention.oldest_available_commit_seq as i64),
        ),
        (
            "newest_available_commit_seq",
            SqlValue::Int(retention.newest_available_commit_seq as i64),
        ),
        (
            "retained_commits",
            SqlValue::Int(retention.retained_commits as i64),
        ),
        (
            "retained_bytes",
            SqlValue::Int(retention.retained_bytes as i64),
        ),
        (
            "last_applied_at",
            apply_state
                .last_applied_at
                .map(|value| SqlValue::Int(value as i64))
                .unwrap_or(SqlValue::Null),
        ),
        (
            "last_error",
            apply_state
                .last_error
                .map(SqlValue::String)
                .unwrap_or(SqlValue::Null),
        ),
    ])])
}

pub(crate) fn bicdb_replication_lag_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let source = db.current_commit_seq();
    let applied = db.last_applied_commit_seq();
    Ok(vec![virtual_row([
        ("source_commit_seq", SqlValue::Int(source as i64)),
        ("last_applied_commit_seq", SqlValue::Int(applied as i64)),
        (
            "lag_commits",
            SqlValue::Int(source.saturating_sub(applied) as i64),
        ),
    ])])
}

pub(crate) fn bicdb_consensus_status_rows(db: &BicDb) -> BTreeMap<String, SqlValue> {
    let status = db.consensus_status();
    virtual_row([
        ("cluster_id", SqlValue::String(status.cluster_id)),
        ("node_id", SqlValue::String(status.node_id)),
        ("role", SqlValue::String(format!("{:?}", status.role))),
        ("current_term", SqlValue::Int(status.current_term as i64)),
        (
            "leader_id",
            status
                .leader_id
                .map(SqlValue::String)
                .unwrap_or(SqlValue::Null),
        ),
        ("commit_index", SqlValue::Int(status.commit_index as i64)),
        ("last_applied", SqlValue::Int(status.last_applied as i64)),
        (
            "last_log_index",
            SqlValue::Int(status.last_log_index as i64),
        ),
        ("voter_count", SqlValue::Int(status.voters.len() as i64)),
    ])
}

pub(crate) fn pg_attrdef_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let schemas = relation_schemas(db)?;
    let table_oids = table_oids(db);
    let mut rows = Vec::new();
    let mut oid = 70_000_i64;
    for schema in &schemas {
        let table_oid = *table_oids.get(&schema.name).unwrap_or(&0);
        for column in &schema.columns {
            if let Some(expression) = column_attrdef_expression(column) {
                rows.push(virtual_row([
                    ("oid", SqlValue::Int(oid)),
                    ("adrelid", SqlValue::Int(table_oid)),
                    (
                        "adnum",
                        SqlValue::Int(
                            attnum_for_column(&schemas, &schema.name, &column.name).unwrap_or(0),
                        ),
                    ),
                    ("adbin", expression),
                ]));
                oid += 1;
            }
        }
    }
    Ok(rows)
}

pub(crate) fn pg_attrdef_rows_for_adrelids(
    db: &BicDb,
    adrelids: &BTreeSet<i64>,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    if adrelids.is_empty() {
        return Ok(Vec::new());
    }
    let relations = adrelids
        .iter()
        .map(|adrelid| {
            Ok(match catalog_relation_target_for_oid(db, *adrelid)? {
                Some(CatalogRelationTarget::Relation(relation)) => Some((*adrelid, relation)),
                _ => None,
            })
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<BTreeMap<_, _>>();
    if relations.is_empty() {
        return Ok(Vec::new());
    }

    let schemas = relation_schemas(db)?;
    let table_oids = table_oids(db);
    let mut rows = Vec::new();
    let mut oid = 70_000_i64;
    for schema in &schemas {
        let table_oid = *table_oids.get(&schema.name).unwrap_or(&0);
        for column in &schema.columns {
            if let Some(expression) = column_attrdef_expression(column) {
                if relations
                    .get(&table_oid)
                    .is_some_and(|relation| schema.name.eq_ignore_ascii_case(relation))
                {
                    rows.push(virtual_row([
                        ("oid", SqlValue::Int(oid)),
                        ("adrelid", SqlValue::Int(table_oid)),
                        (
                            "adnum",
                            SqlValue::Int(
                                attnum_for_column(&schemas, &schema.name, &column.name)
                                    .unwrap_or(0),
                            ),
                        ),
                        ("adbin", expression),
                    ]));
                }
                oid += 1;
            }
        }
    }
    Ok(rows)
}

pub(crate) fn column_attrdef_expression(column: &ColumnSchema) -> Option<SqlValue> {
    column
        .generated_expr
        .clone()
        .map(SqlValue::String)
        .or_else(|| {
            column
                .default_sequence
                .as_deref()
                .filter(|_| column.identity.is_none())
                .map(pg_attrdef_expr)
        })
        .or_else(|| column.default_expr.clone().map(SqlValue::String))
}

pub(crate) fn pg_attrdef_expr(sequence: &str) -> SqlValue {
    SqlValue::String(format!("nextval('{sequence}'::regclass)"))
}

pub(crate) fn pg_rewrite_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let table_oids = table_oids(db);
    let mut rows = Vec::new();
    for view in list_views(db)? {
        rows.push(virtual_row([
            ("oid", SqlValue::Int(rewrite_rule_oid(&view.name))),
            ("rulename", SqlValue::String("_RETURN".to_string())),
            (
                "ev_class",
                SqlValue::Int(*table_oids.get(&view.name).unwrap_or(&0)),
            ),
            ("ev_type", SqlValue::String("1".to_string())),
            ("ev_enabled", SqlValue::String("O".to_string())),
            ("is_instead", SqlValue::Bool(true)),
            ("ev_qual", SqlValue::String("<>".to_string())),
            ("ev_action", SqlValue::String(view.query_sql)),
        ]));
    }
    Ok(rows)
}

pub(crate) fn pg_am_rows() -> Vec<BTreeMap<String, SqlValue>> {
    vec![
        virtual_row([
            ("oid", SqlValue::Int(2)),
            ("amname", SqlValue::String("heap".to_string())),
            (
                "amhandler",
                SqlValue::String("heap_tableam_handler".to_string()),
            ),
            ("amtype", SqlValue::String("t".to_string())),
        ]),
        virtual_row([
            ("oid", SqlValue::Int(403)),
            ("amname", SqlValue::String("btree".to_string())),
            ("amhandler", SqlValue::String("bthandler".to_string())),
            ("amtype", SqlValue::String("i".to_string())),
        ]),
        virtual_row([
            ("oid", SqlValue::Int(405)),
            ("amname", SqlValue::String("hash".to_string())),
            ("amhandler", SqlValue::String("hashhandler".to_string())),
            ("amtype", SqlValue::String("i".to_string())),
        ]),
        virtual_row([
            ("oid", SqlValue::Int(783)),
            ("amname", SqlValue::String("gist".to_string())),
            ("amhandler", SqlValue::String("gisthandler".to_string())),
            ("amtype", SqlValue::String("i".to_string())),
        ]),
        virtual_row([
            ("oid", SqlValue::Int(2742)),
            ("amname", SqlValue::String("gin".to_string())),
            ("amhandler", SqlValue::String("ginhandler".to_string())),
            ("amtype", SqlValue::String("i".to_string())),
        ]),
        virtual_row([
            ("oid", SqlValue::Int(3580)),
            ("amname", SqlValue::String("brin".to_string())),
            ("amhandler", SqlValue::String("brinhandler".to_string())),
            ("amtype", SqlValue::String("i".to_string())),
        ]),
        virtual_row([
            ("oid", SqlValue::Int(4000)),
            ("amname", SqlValue::String("spgist".to_string())),
            ("amhandler", SqlValue::String("spghandler".to_string())),
            ("amtype", SqlValue::String("i".to_string())),
        ]),
    ]
}

pub(crate) fn pg_tablespace_rows() -> Vec<BTreeMap<String, SqlValue>> {
    vec![
        virtual_row([
            ("oid", SqlValue::Int(1663)),
            ("spcname", SqlValue::String("pg_default".to_string())),
            ("spcowner", SqlValue::Int(10)),
            ("spcacl", SqlValue::Null),
            ("spcoptions", SqlValue::Null),
        ]),
        virtual_row([
            ("oid", SqlValue::Int(1664)),
            ("spcname", SqlValue::String("pg_global".to_string())),
            ("spcowner", SqlValue::Int(10)),
            ("spcacl", SqlValue::Null),
            ("spcoptions", SqlValue::Null),
        ]),
    ]
}

/// Current IANA offsets, including aliases, from the same database used by
/// AT TIME ZONE. Capture one instant so a scan cannot straddle a DST transition.
pub(crate) fn pg_timezone_names_rows() -> Vec<BTreeMap<String, SqlValue>> {
    use chrono::{Offset, Utc};
    use chrono_tz::OffsetComponents;
    let now = Utc::now();
    chrono_tz::TZ_VARIANTS
        .iter()
        .map(|zone| {
            let local = now.with_timezone(zone);
            let seconds = local.offset().fix().local_minus_utc();
            let magnitude = seconds.unsigned_abs();
            let offset = format!(
                "{}{:02}:{:02}:{:02}",
                if seconds < 0 { "-" } else { "" },
                magnitude / 3600,
                (magnitude / 60) % 60,
                magnitude % 60
            );
            virtual_row([
                ("name", SqlValue::String(zone.name().to_string())),
                ("abbrev", SqlValue::String(local.format("%Z").to_string())),
                ("utc_offset", SqlValue::String(offset)),
                (
                    "is_dst",
                    SqlValue::Bool(local.offset().dst_offset() != chrono::Duration::zero()),
                ),
            ])
        })
        .collect()
}

pub(crate) fn pg_settings_rows() -> Vec<BTreeMap<String, SqlValue>> {
    [
        ("client_encoding", "UTF8", "string"),
        ("client_min_messages", "notice", "enum"),
        ("datestyle", "ISO, MDY", "string"),
        ("default_table_access_method", "heap", "string"),
        ("default_tablespace", "", "string"),
        ("extra_float_digits", "1", "integer"),
        ("idle_in_transaction_session_timeout", "0", "integer"),
        ("idle_session_timeout", "0", "integer"),
        ("intervalstyle", "postgres", "enum"),
        ("lock_timeout", "0", "integer"),
        ("restrict_nonsystem_relation_kind", "", "string"),
        ("row_security", "on", "bool"),
        ("search_path", "\"$user\", public", "string"),
        ("standard_conforming_strings", "on", "bool"),
        ("statement_timeout", "0", "integer"),
        ("synchronize_seqscans", "on", "bool"),
        ("timezone", "UTC", "string"),
        ("transaction_isolation", "read committed", "enum"),
        ("transaction_timeout", "0", "integer"),
    ]
    .into_iter()
    .map(|(name, setting, vartype)| pg_settings_row(name, setting, vartype))
    .collect()
}

pub(crate) fn pg_settings_row(
    name: &str,
    setting: &str,
    vartype: &str,
) -> BTreeMap<String, SqlValue> {
    virtual_row([
        ("name", SqlValue::String(name.to_string())),
        ("setting", SqlValue::String(setting.to_string())),
        ("unit", SqlValue::Null),
        (
            "category",
            SqlValue::String("BicDB/PostgreSQL compatibility".to_string()),
        ),
        (
            "short_desc",
            SqlValue::String(format!("BicDB-compatible {name} setting")),
        ),
        ("extra_desc", SqlValue::Null),
        ("context", SqlValue::String("user".to_string())),
        ("vartype", SqlValue::String(vartype.to_string())),
        ("source", SqlValue::String("default".to_string())),
        ("min_val", SqlValue::Null),
        ("max_val", SqlValue::Null),
        ("enumvals", SqlValue::Null),
        ("boot_val", SqlValue::String(setting.to_string())),
        ("reset_val", SqlValue::String(setting.to_string())),
        ("sourcefile", SqlValue::Null),
        ("sourceline", SqlValue::Null),
        ("pending_restart", SqlValue::Bool(false)),
    ])
}

pub(crate) fn pg_roles_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    Ok(list_roles(db)?
        .into_iter()
        .map(|role| {
            virtual_row([
                ("oid", SqlValue::Int(role_oid(&role.name))),
                ("rolname", SqlValue::String(role.name)),
                ("rolsuper", SqlValue::Bool(role.superuser)),
                ("rolinherit", SqlValue::Bool(role.inherit)),
                ("rolcreaterole", SqlValue::Bool(role.create_role)),
                ("rolcreatedb", SqlValue::Bool(role.create_db)),
                ("rolcanlogin", SqlValue::Bool(role.can_login)),
                ("rolreplication", SqlValue::Bool(role.replication)),
                ("rolbypassrls", SqlValue::Bool(role.bypass_rls)),
                ("rolconnlimit", SqlValue::Int(role.connection_limit)),
                ("rolpassword", SqlValue::String("********".to_string())),
                (
                    "rolvaliduntil",
                    role.valid_until
                        .map(SqlValue::String)
                        .unwrap_or(SqlValue::Null),
                ),
                ("rolconfig", SqlValue::Null),
            ])
        })
        .collect())
}

pub(crate) fn pg_user_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    Ok(list_roles(db)?
        .into_iter()
        .filter(|role| role.can_login)
        .map(|role| {
            virtual_row([
                ("usename", SqlValue::String(role.name.clone())),
                ("usesysid", SqlValue::Int(role_oid(&role.name))),
                ("usecreatedb", SqlValue::Bool(role.create_db)),
                ("usesuper", SqlValue::Bool(role.superuser)),
                ("userepl", SqlValue::Bool(role.replication)),
                ("usebypassrls", SqlValue::Bool(role.bypass_rls)),
                ("passwd", SqlValue::String("********".to_string())),
                (
                    "valuntil",
                    role.valid_until
                        .map(SqlValue::String)
                        .unwrap_or(SqlValue::Null),
                ),
                ("useconfig", SqlValue::Null),
            ])
        })
        .collect())
}

pub(crate) fn pg_database_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    Ok(list_databases(db)?
        .into_iter()
        .enumerate()
        .map(|(idx, database)| {
            virtual_row([
                ("oid", SqlValue::Int(1 + idx as i64)),
                ("datname", SqlValue::String(database.name)),
                ("datdba", SqlValue::Int(role_oid(&database.owner))),
                ("encoding", SqlValue::Int(6)),
                ("datlocprovider", SqlValue::String("c".to_string())),
                ("datistemplate", SqlValue::Bool(false)),
                ("datallowconn", SqlValue::Bool(true)),
                ("datconnlimit", SqlValue::Int(-1)),
                ("datfrozenxid", SqlValue::Int(0)),
                ("datminmxid", SqlValue::Int(0)),
                ("dattablespace", SqlValue::Int(1663)),
                ("datcollate", SqlValue::String("C".to_string())),
                ("datctype", SqlValue::String("C".to_string())),
                ("daticulocale", SqlValue::Null),
                ("daticurules", SqlValue::Null),
                ("datcollversion", SqlValue::Null),
                ("datacl", SqlValue::Null),
            ])
        })
        .collect())
}

pub(crate) fn pg_auth_members_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    Ok(list_role_memberships(db)?
        .into_iter()
        .map(|membership| {
            let inherited = membership.inherit_option.unwrap_or(
                load_role_schema(db, &membership.member)?.is_none_or(|role| role.inherit),
            );
            Ok(virtual_row([
                ("roleid", SqlValue::Int(role_oid(&membership.role))),
                ("member", SqlValue::Int(role_oid(&membership.member))),
                ("grantor", SqlValue::Int(role_oid(&membership.grantor))),
                ("admin_option", SqlValue::Bool(membership.admin_option)),
                ("inherit_option", SqlValue::Bool(inherited)),
                ("set_option", SqlValue::Bool(membership.set_option)),
            ]))
        })
        .collect::<Result<Vec<_>>>()?)
}

pub(crate) fn pg_sequence_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    Ok(list_sequences(db)?
        .into_iter()
        .map(|sequence| {
            virtual_row([
                ("seqrelid", SqlValue::Int(sequence_oid(&sequence.name))),
                ("seqtypid", SqlValue::Int(pg_type_oid(&sequence.data_type))),
                ("seqstart", SqlValue::Int(sequence.start_value)),
                ("seqincrement", SqlValue::Int(sequence.increment_by)),
                ("seqmax", SqlValue::Int(sequence.max_value)),
                ("seqmin", SqlValue::Int(sequence.min_value)),
                ("seqcache", SqlValue::Int(sequence.cache_size)),
                ("seqcycle", SqlValue::Bool(sequence.cycle)),
            ])
        })
        .collect())
}

pub(crate) fn pg_collation_rows() -> Vec<BTreeMap<String, SqlValue>> {
    [
        (100, "default", "d", -1, None, None, None),
        (950, "C", "c", -1, Some("C"), Some("C"), None),
        (951, "POSIX", "c", -1, Some("POSIX"), Some("POSIX"), None),
        (962, "ucs_basic", "b", 6, None, None, None),
        (810_001, "en-x-icu", "i", -1, None, None, Some("en")),
    ]
    .into_iter()
    .map(|(oid, name, provider, encoding, collate, ctype, locale)| {
        virtual_row([
            ("oid", SqlValue::Int(oid)),
            ("collname", SqlValue::String(name.to_string())),
            ("collnamespace", SqlValue::Int(11)),
            ("collowner", SqlValue::Int(10)),
            ("collprovider", SqlValue::String(provider.to_string())),
            ("collisdeterministic", SqlValue::Bool(true)),
            ("collencoding", SqlValue::Int(encoding)),
            (
                "collcollate",
                collate
                    .map(|value| SqlValue::String(value.to_string()))
                    .unwrap_or(SqlValue::Null),
            ),
            (
                "collctype",
                ctype
                    .map(|value| SqlValue::String(value.to_string()))
                    .unwrap_or(SqlValue::Null),
            ),
            (
                "colllocale",
                locale
                    .map(|value| SqlValue::String(value.to_string()))
                    .unwrap_or(SqlValue::Null),
            ),
            ("collicurules", SqlValue::Null),
            ("collversion", SqlValue::Null),
        ])
    })
    .collect()
}

pub(crate) fn pg_sequences_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    Ok(list_sequences(db)?
        .into_iter()
        .map(|sequence| {
            virtual_row([
                ("schemaname", SqlValue::String("public".to_string())),
                ("sequencename", SqlValue::String(sequence.name)),
                ("sequenceowner", SqlValue::String(sequence.owner)),
                (
                    "data_type",
                    SqlValue::String(sequence_data_type_name(&sequence.data_type).to_string()),
                ),
                ("start_value", SqlValue::Int(sequence.start_value)),
                ("min_value", SqlValue::Int(sequence.min_value)),
                ("max_value", SqlValue::Int(sequence.max_value)),
                ("increment_by", SqlValue::Int(sequence.increment_by)),
                ("cycle", SqlValue::Bool(sequence.cycle)),
                ("cache_size", SqlValue::Int(sequence.cache_size)),
                ("last_value", SqlValue::Int(sequence.last_value)),
            ])
        })
        .collect())
}

pub(crate) fn pg_policy_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let schemas = list_schemas(db)?;
    let table_oids = table_oids(db);
    let mut rows = Vec::new();
    let mut oid = 70_000_i64;
    for schema in schemas {
        let table_oid = *table_oids.get(&schema.name).unwrap_or(&0);
        for policy in schema.policies {
            let polroles = if policy.applies_to_public() {
                // PUBLIC is oid 0 in pg_policy.polroles.
                "{0}".to_string()
            } else {
                format!(
                    "{{{}}}",
                    policy
                        .roles
                        .iter()
                        .map(|role| role_oid(role).to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                )
            };
            rows.push(virtual_row([
                ("oid", SqlValue::Int(oid)),
                ("polname", SqlValue::String(policy.name)),
                ("polrelid", SqlValue::Int(table_oid)),
                (
                    "polcmd",
                    SqlValue::String(policy.command.pg_code().to_string()),
                ),
                ("polpermissive", SqlValue::Bool(policy.permissive)),
                ("polroles", SqlValue::String(polroles)),
                (
                    "polqual",
                    policy
                        .using_expr
                        .map(SqlValue::String)
                        .unwrap_or(SqlValue::Null),
                ),
                (
                    "polwithcheck",
                    policy
                        .check_expr
                        .map(SqlValue::String)
                        .unwrap_or(SqlValue::Null),
                ),
                ("bicdb_enforced", SqlValue::Bool(policy.enforced)),
            ]));
            oid += 1;
        }
    }
    Ok(rows)
}

pub(crate) fn pg_policies_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let mut rows = Vec::new();
    for schema in list_schemas(db)? {
        for policy in schema.policies {
            let roles = format!("{{{}}}", policy.roles.join(","));
            rows.push(virtual_row([
                ("schemaname", SqlValue::String("public".to_string())),
                ("tablename", SqlValue::String(schema.name.clone())),
                ("policyname", SqlValue::String(policy.name)),
                (
                    "permissive",
                    SqlValue::String(if policy.permissive {
                        "PERMISSIVE".to_string()
                    } else {
                        "RESTRICTIVE".to_string()
                    }),
                ),
                ("roles", SqlValue::String(roles)),
                (
                    "cmd",
                    SqlValue::String(policy.command.pg_policies_command().to_string()),
                ),
                (
                    "qual",
                    policy
                        .using_expr
                        .map(SqlValue::String)
                        .unwrap_or(SqlValue::Null),
                ),
                (
                    "with_check",
                    policy
                        .check_expr
                        .map(SqlValue::String)
                        .unwrap_or(SqlValue::Null),
                ),
                ("bicdb_enforced", SqlValue::Bool(policy.enforced)),
            ]));
        }
    }
    Ok(rows)
}

pub(crate) fn pg_index_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    pg_index_rows_filtered(db, None, None, None)
}

pub(crate) fn pg_index_rows_filtered(
    db: &BicDb,
    primary_filter: Option<bool>,
    relation_oids: Option<&BTreeSet<i64>>,
    index_oids: Option<&BTreeSet<i64>>,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let table_oids = table_oids(db);
    let schemas =
        if let Some(relation_names) = relation_names_for_oid_filter(&table_oids, relation_oids) {
            load_relation_schemas_by_name(db, &relation_names)?
        } else {
            relation_schemas(db)?
        };
    let mut rows = Vec::new();

    for (table, primary_key_columns) in primary_key_indexes(&schemas, db) {
        let table_oid = *table_oids.get(&table).unwrap_or(&0);
        let index_oid = primary_index_oid(table_oid);
        if pg_index_filter_matches(
            index_oid,
            table_oid,
            true,
            primary_filter,
            relation_oids,
            index_oids,
        ) {
            rows.push(pg_index_row(
                index_oid,
                table_oid,
                true,
                true,
                constraint_attnums(&schemas, &table, &primary_key_columns),
                constraint_collation_oids(&schemas, &table, &primary_key_columns),
                default_index_opclass_oids(db, &schemas, &table, &primary_key_columns, "btree")?,
                None,
                false,
                None,
                pg_indexdef_string(
                    &schema_name_for_relation(&schemas, &table),
                    &primary_key_constraint_name(&schemas, &table),
                    &table,
                    true,
                    "btree",
                    &index_expression_for_columns(&primary_key_columns),
                ),
            ));
        }
    }

    for index in catalog_indexes(&schemas, db) {
        let table_oid = *table_oids.get(&index.collection).unwrap_or(&0);
        let index_oid = secondary_index_oid(&index.schema_name, &index.name);
        if pg_index_filter_matches(
            index_oid,
            table_oid,
            false,
            primary_filter,
            relation_oids,
            index_oids,
        ) {
            let definition = catalog_indexdef_string(&index);
            rows.push(pg_index_row(
                index_oid,
                table_oid,
                false,
                index.unique,
                index.indkey,
                index.collations,
                index_operator_class_oids(&index.access_method, &index.operator_classes),
                index.indexprs,
                index.exclusion,
                index.predicate,
                definition,
            ));
        }
    }

    Ok(rows)
}

pub(crate) fn pg_index_filter_matches(
    indexrelid: i64,
    indrelid: i64,
    indisprimary: bool,
    primary_filter: Option<bool>,
    relation_oids: Option<&BTreeSet<i64>>,
    index_oids: Option<&BTreeSet<i64>>,
) -> bool {
    if primary_filter.is_some_and(|expected| expected != indisprimary) {
        return false;
    }
    if relation_oids.is_some_and(|oids| !oids.contains(&indrelid)) {
        return false;
    }
    if index_oids.is_some_and(|oids| !oids.contains(&indexrelid)) {
        return false;
    }
    true
}

pub(crate) fn pg_tables_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let schemas = relation_schemas(db)?;
    let catalog_indexes = catalog_indexes(&schemas, db);
    let triggers = list_triggers(db)?;
    let mut rows = Vec::new();

    for table in user_collection_names(db) {
        let table_schema = schemas
            .iter()
            .find(|schema| schema.name.eq_ignore_ascii_case(&table));
        let hasindexes = primary_key_column(&schemas, &table).is_some()
            || catalog_indexes
                .iter()
                .any(|index| index.collection.eq_ignore_ascii_case(&table));
        let hastriggers = triggers
            .iter()
            .any(|trigger| trigger.table_name.eq_ignore_ascii_case(&table));

        rows.push(virtual_row([
            (
                "schemaname",
                SqlValue::String(
                    table_schema
                        .map(|schema| schema.schema_name.clone())
                        .unwrap_or_else(|| "public".to_string()),
                ),
            ),
            ("tablename", SqlValue::String(catalog_display_name(&table))),
            ("tableowner", SqlValue::String("bicdb".to_string())),
            ("tablespace", SqlValue::Null),
            ("hasindexes", SqlValue::Bool(hasindexes)),
            ("hasrules", SqlValue::Bool(false)),
            ("hastriggers", SqlValue::Bool(hastriggers)),
            (
                "rowsecurity",
                SqlValue::Bool(
                    table_schema
                        .map(|schema| schema.rls_enabled)
                        .unwrap_or(false),
                ),
            ),
        ]));
    }

    rows.sort_by(|left, right| {
        virtual_cell(left, "tablename")
            .to_cell()
            .cmp(&virtual_cell(right, "tablename").to_cell())
    });
    Ok(rows)
}

pub(crate) fn pg_indexes_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    pg_indexes_rows_filtered(db, None, None, None)
}

pub(crate) fn pg_indexes_rows_filtered(
    db: &BicDb,
    schema_names: Option<&BTreeSet<String>>,
    table_names: Option<&BTreeSet<String>>,
    index_names: Option<&BTreeSet<String>>,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let filtered_relation_names = relation_names_for_identifier_filters(None, table_names);
    let schemas = if let Some(relation_names) = filtered_relation_names.as_ref() {
        load_relation_schemas_by_name(db, relation_names)?
    } else {
        relation_schemas(db)?
    };
    let mut rows = Vec::new();

    for (table, primary_key_columns) in primary_key_indexes(&schemas, db) {
        let index_name = primary_key_constraint_name(&schemas, &table);
        let schema_name = schema_name_for_relation(&schemas, &table);
        if !catalog_name_matches(schema_names, &schema_name)
            || !catalog_name_matches(table_names, &table)
            || !catalog_name_matches(index_names, &index_name)
        {
            continue;
        }
        rows.push(pg_indexes_row(
            &schema_name,
            &table,
            &index_name,
            &pg_indexdef_string(
                &schema_name,
                &index_name,
                &table,
                true,
                "btree",
                &index_expression_for_columns(&primary_key_columns),
            ),
        ));
    }

    for index in catalog_indexes(&schemas, db) {
        if !catalog_name_matches(schema_names, &index.schema_name)
            || !catalog_name_matches(table_names, &index.collection)
            || !catalog_name_matches(index_names, &index.name)
        {
            continue;
        }
        rows.push(pg_indexes_row(
            &index.schema_name,
            &index.collection,
            &index.name,
            &catalog_indexdef_string(&index),
        ));
    }

    rows.sort_by(|left, right| {
        virtual_cell(left, "tablename")
            .to_cell()
            .cmp(&virtual_cell(right, "tablename").to_cell())
            .then(
                virtual_cell(left, "indexname")
                    .to_cell()
                    .cmp(&virtual_cell(right, "indexname").to_cell()),
            )
    });
    Ok(rows)
}

/// The name a relation should be shown under in catalog output.
///
/// Tables outside `public` are stored under a schema-isolated physical name;
/// catalogs must show what the user wrote. Identity (OIDs, lookups) keeps
/// using the physical name.
pub(crate) fn catalog_display_name(name: &str) -> String {
    crate::eval::rewrite_helpers::logical_relation_name(name).unwrap_or_else(|| name.to_string())
}

pub(crate) fn pg_indexes_row(
    schema_name: &str,
    table: &str,
    index: &str,
    definition: &str,
) -> BTreeMap<String, SqlValue> {
    virtual_row([
        ("schemaname", SqlValue::String(schema_name.to_string())),
        ("tablename", SqlValue::String(catalog_display_name(table))),
        ("indexname", SqlValue::String(catalog_display_name(index))),
        ("tablespace", SqlValue::Null),
        ("indexdef", SqlValue::String(definition.to_string())),
    ])
}

pub(crate) fn pg_indexdef_for_oid(db: &BicDb, indexrelid: i64) -> Result<Option<String>> {
    let schemas = relation_schemas(db)?;
    let table_oids = table_oids(db);

    for (table, primary_key_columns) in primary_key_indexes(&schemas, db) {
        let table_oid = *table_oids.get(&table).unwrap_or(&0);
        if primary_index_oid(table_oid) == indexrelid {
            let index_name = primary_key_constraint_name(&schemas, &table);
            let schema_name = schema_name_for_relation(&schemas, &table);
            return Ok(Some(pg_indexdef_string(
                &schema_name,
                &index_name,
                &table,
                true,
                "btree",
                &index_expression_for_columns(&primary_key_columns),
            )));
        }
    }

    for index in catalog_indexes(&schemas, db) {
        if secondary_index_oid(&index.schema_name, &index.name) == indexrelid {
            return Ok(Some(catalog_indexdef_string(&index)));
        }
    }

    Ok(None)
}

pub(crate) fn pg_indexdef_string(
    schema_name: &str,
    index_name: &str,
    table: &str,
    unique: bool,
    access_method: &str,
    expression: &str,
) -> String {
    format!(
        "CREATE {}INDEX {} ON {} USING {} ({})",
        if unique { "UNIQUE " } else { "" },
        index_name,
        pg_indexdef_relation_name(schema_name, table),
        access_method,
        normalize_index_expression_for_display(expression)
    )
}

pub(crate) fn catalog_indexdef_string(index: &CatalogIndex) -> String {
    let mut definition = pg_indexdef_string(
        &index.schema_name,
        &index.name,
        &index.collection,
        index.unique,
        &index.access_method,
        &index.expression,
    );
    if let Some(predicate) = index.predicate.as_deref() {
        definition.push_str(&format!(" WHERE ({})", predicate.trim_matches(['(', ')'])));
    }
    definition
}

pub(crate) fn pg_indexdef_relation_name(schema_name: &str, table: &str) -> String {
    // `indexdef` is DDL a user can paste back, so it names the relation the
    // way it was written rather than by its schema-isolated physical name.
    let table = catalog_display_name(table);
    if schema_name.eq_ignore_ascii_case("public") {
        table
    } else {
        format!("{schema_name}.{table}")
    }
}

pub(crate) fn normalize_index_expression_for_display(expression: &str) -> String {
    let mut normalized = String::with_capacity(expression.len());
    let mut chars = expression.chars().peekable();
    while let Some(ch) = chars.next() {
        normalized.push(ch);
        if ch != ':' || chars.peek() != Some(&':') {
            continue;
        }

        normalized.push(chars.next().unwrap());
        while chars.peek().is_some_and(|next| next.is_whitespace()) {
            normalized.push(chars.next().unwrap());
        }

        while chars
            .peek()
            .is_some_and(|next| next.is_ascii_alphanumeric() || *next == '_')
        {
            normalized.push(chars.next().unwrap().to_ascii_lowercase());
        }
    }
    normalized
}

pub(crate) fn pg_constraint_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    pg_constraint_rows_filtered(db, None, None, None)
}

pub(crate) fn pg_constraint_rows_filtered(
    db: &BicDb,
    names: Option<&BTreeSet<String>>,
    types: Option<&BTreeSet<String>>,
    relation_oids: Option<&BTreeSet<i64>>,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let table_oids = table_oids(db);
    let schemas = load_schemas_for_relation_oid_filter(db, &table_oids, relation_oids)?;
    let mut rows =
        pg_constraint_rows_from_schemas(&schemas, &table_oids, names, types, relation_oids);
    if relation_oids.is_none() {
        rows.extend(pg_domain_constraint_rows(db, names, types)?);
    }
    Ok(rows)
}

fn pg_domain_constraint_rows(
    db: &BicDb,
    names: Option<&BTreeSet<String>>,
    types: Option<&BTreeSet<String>>,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let mut rows = Vec::new();
    for user_type in list_user_types(db)? {
        let UserTypeKind::Domain {
            not_null,
            not_null_constraint_name,
            constraints,
            ..
        } = &user_type.kind
        else {
            continue;
        };
        if *not_null {
            let name = not_null_constraint_name
                .clone()
                .unwrap_or_else(|| format!("{}_not_null", user_type.name));
            if catalog_name_matches(names, &name) && catalog_name_matches(types, "n") {
                rows.push(pg_domain_constraint_row(
                    &user_type,
                    0,
                    name,
                    "n",
                    true,
                    SqlValue::Null,
                ));
            }
        }
        for (index, constraint) in constraints.iter().enumerate() {
            if !catalog_name_matches(names, &constraint.name) || !catalog_name_matches(types, "c") {
                continue;
            }
            rows.push(pg_domain_constraint_row(
                &user_type,
                index + 1,
                constraint.name.clone(),
                "c",
                constraint.validated,
                SqlValue::String(constraint.expression.clone()),
            ));
        }
    }
    Ok(rows)
}

fn pg_domain_constraint_row(
    user_type: &UserTypeSchema,
    index: usize,
    name: String,
    constraint_type: &str,
    validated: bool,
    expression: SqlValue,
) -> BTreeMap<String, SqlValue> {
    virtual_row([
        (
            "oid",
            SqlValue::Int(80_000_000 + user_type.oid.saturating_mul(100) + index as i64),
        ),
        ("conname", SqlValue::String(name)),
        (
            "connamespace",
            SqlValue::Int(namespace_oid(&user_type.schema_name)),
        ),
        ("contype", SqlValue::String(constraint_type.to_string())),
        ("condeferrable", SqlValue::Bool(false)),
        ("condeferred", SqlValue::Bool(false)),
        ("conenforced", SqlValue::Bool(true)),
        ("convalidated", SqlValue::Bool(validated)),
        ("conrelid", SqlValue::Int(0)),
        ("contypid", SqlValue::Int(user_type.oid)),
        ("conindid", SqlValue::Int(0)),
        ("conparentid", SqlValue::Int(0)),
        ("confrelid", SqlValue::Int(0)),
        ("confupdtype", SqlValue::String(" ".to_string())),
        ("confdeltype", SqlValue::String(" ".to_string())),
        ("confmatchtype", SqlValue::String("s".to_string())),
        ("conislocal", SqlValue::Bool(true)),
        ("coninhcount", SqlValue::Int(0)),
        ("connoinherit", SqlValue::Bool(false)),
        ("conperiod", SqlValue::Bool(false)),
        ("conkey", SqlValue::Null),
        ("confkey", SqlValue::Null),
        ("conpfeqop", SqlValue::Null),
        ("conppeqop", SqlValue::Null),
        ("conffeqop", SqlValue::Null),
        ("confdelsetcols", SqlValue::Null),
        ("conexclop", SqlValue::Null),
        ("conbin", expression),
    ])
}

pub(crate) fn load_schemas_for_relation_oid_filter(
    db: &BicDb,
    table_oids: &BTreeMap<String, i64>,
    relation_oids: Option<&BTreeSet<i64>>,
) -> Result<Vec<TableSchema>> {
    let Some(relation_names) = relation_names_for_oid_filter(table_oids, relation_oids) else {
        return list_schemas(db);
    };
    if relation_names.len() <= 32 {
        return load_relation_schemas_by_name(db, &relation_names);
    }
    let relation_names = relation_names
        .into_iter()
        .map(|name| name.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let mut schemas = relation_schemas(db)?
        .into_iter()
        .filter(|schema| relation_names.contains(&schema.name.to_ascii_lowercase()))
        .collect::<Vec<_>>();
    schemas.sort_by(|left, right| {
        left.schema_name
            .cmp(&right.schema_name)
            .then_with(|| left.name.cmp(&right.name))
    });
    Ok(schemas)
}

pub(crate) fn pg_constraint_rows_from_schemas(
    schemas: &[TableSchema],
    table_oids: &BTreeMap<String, i64>,
    names: Option<&BTreeSet<String>>,
    types: Option<&BTreeSet<String>>,
    relation_oids: Option<&BTreeSet<i64>>,
) -> Vec<BTreeMap<String, SqlValue>> {
    let mut rows = Vec::new();
    for schema in schemas {
        let primary_key_columns = primary_key_columns_for_schema(schema);
        if primary_key_columns.is_empty() {
            continue;
        }
        let table_oid = *table_oids.get(&schema.name).unwrap_or(&0);
        let name = schema.primary_key_constraint_name();
        if pg_constraint_filter_matches(&name, "p", table_oid, names, types, relation_oids) {
            rows.push(pg_constraint_row(PgConstraintRow {
                oid: pg_constraint_oid(table_oid, 0),
                name,
                contype: "p",
                conrelid: table_oid,
                conindid: primary_index_oid(table_oid),
                confrelid: 0,
                confupdtype: " ",
                confdeltype: " ",
                conkey: constraint_attnums(schemas, &schema.name, &primary_key_columns),
                confkey: Vec::new(),
                conbin: None,
                convalidated: true,
            }));
        }
    }
    for schema in schemas {
        let table_oid = *table_oids.get(&schema.name).unwrap_or(&0);
        let mut constraint_slot = 1_i64;
        for column in &schema.columns {
            if column.hidden {
                continue;
            }
            if !column.nullable || column.primary_key {
                let name = not_null_constraint_name(&schema.name, &column.name);
                if pg_constraint_filter_matches(&name, "n", table_oid, names, types, relation_oids)
                {
                    rows.push(pg_constraint_row(PgConstraintRow {
                        oid: pg_constraint_oid(table_oid, constraint_slot),
                        name,
                        contype: "n",
                        conrelid: table_oid,
                        conindid: 0,
                        confrelid: 0,
                        confupdtype: " ",
                        confdeltype: " ",
                        conkey: vec![
                            attnum_for_column(schemas, &schema.name, &column.name).unwrap_or(0)
                        ],
                        confkey: Vec::new(),
                        conbin: None,
                        convalidated: true,
                    }));
                }
                constraint_slot += 1;
            }
        }
        for constraint in &schema.constraints {
            match constraint {
                ConstraintSchema::Unique {
                    name,
                    columns,
                    validated,
                } => {
                    if unique_constraint_is_primary_key(schema, name, columns) {
                        continue;
                    }
                    if pg_constraint_filter_matches(
                        name,
                        "u",
                        table_oid,
                        names,
                        types,
                        relation_oids,
                    ) {
                        rows.push(pg_constraint_row(PgConstraintRow {
                            oid: pg_constraint_oid(table_oid, constraint_slot),
                            name: name.clone(),
                            contype: "u",
                            conrelid: table_oid,
                            conindid: 0,
                            confrelid: 0,
                            confupdtype: " ",
                            confdeltype: " ",
                            conkey: constraint_attnums(schemas, &schema.name, columns),
                            confkey: Vec::new(),
                            conbin: None,
                            convalidated: *validated,
                        }));
                    }
                    constraint_slot += 1;
                }
                ConstraintSchema::Check {
                    name,
                    expression,
                    validated,
                } => {
                    if pg_constraint_filter_matches(
                        name,
                        "c",
                        table_oid,
                        names,
                        types,
                        relation_oids,
                    ) {
                        rows.push(pg_constraint_row(PgConstraintRow {
                            oid: pg_constraint_oid(table_oid, constraint_slot),
                            name: name.clone(),
                            contype: "c",
                            conrelid: table_oid,
                            conindid: 0,
                            confrelid: 0,
                            confupdtype: " ",
                            confdeltype: " ",
                            conkey: constraint_attnums(
                                schemas,
                                &schema.name,
                                &check_constraint_referenced_columns(schema, expression),
                            ),
                            confkey: Vec::new(),
                            conbin: Some(expression.clone()),
                            convalidated: *validated,
                        }));
                    }
                    constraint_slot += 1;
                }
                ConstraintSchema::ForeignKey {
                    name,
                    columns,
                    foreign_table,
                    referred_columns,
                    on_delete,
                    on_update,
                    validated,
                } => {
                    if pg_constraint_filter_matches(
                        name,
                        "f",
                        table_oid,
                        names,
                        types,
                        relation_oids,
                    ) {
                        rows.push(pg_constraint_row(PgConstraintRow {
                            oid: pg_constraint_oid(table_oid, constraint_slot),
                            name: name.clone(),
                            contype: "f",
                            conrelid: table_oid,
                            conindid: 0,
                            confrelid: *table_oids.get(foreign_table).unwrap_or(&0),
                            confupdtype: fk_action_code(*on_update),
                            confdeltype: fk_action_code(*on_delete),
                            conkey: constraint_attnums(schemas, &schema.name, columns),
                            confkey: constraint_attnums(schemas, foreign_table, referred_columns),
                            conbin: None,
                            convalidated: *validated,
                        }));
                    }
                    constraint_slot += 1;
                }
                ConstraintSchema::Exclusion {
                    name,
                    equal_columns,
                    range,
                    validated,
                    ..
                } => {
                    if pg_constraint_filter_matches(
                        name,
                        "x",
                        table_oid,
                        names,
                        types,
                        relation_oids,
                    ) {
                        let columns = exclusion_constraint_columns(equal_columns, range);
                        rows.push(pg_constraint_row(PgConstraintRow {
                            oid: pg_constraint_oid(table_oid, constraint_slot),
                            name: name.clone(),
                            contype: "x",
                            conrelid: table_oid,
                            conindid: secondary_index_oid(&schema.schema_name, name),
                            confrelid: 0,
                            confupdtype: " ",
                            confdeltype: " ",
                            conkey: constraint_attnums(schemas, &schema.name, &columns),
                            confkey: Vec::new(),
                            conbin: None,
                            convalidated: *validated,
                        }));
                    }
                    constraint_slot += 1;
                }
            }
        }
    }
    rows
}

pub(crate) fn pg_constraint_oid(table_oid: i64, local_slot: i64) -> i64 {
    60_000_000 + table_oid.saturating_mul(10_000) + local_slot
}

pub(crate) fn pg_constraint_filter_matches(
    name: &str,
    contype: &str,
    conrelid: i64,
    names: Option<&BTreeSet<String>>,
    types: Option<&BTreeSet<String>>,
    relation_oids: Option<&BTreeSet<i64>>,
) -> bool {
    if !catalog_name_matches(names, name) || !catalog_name_matches(types, contype) {
        return false;
    }
    match relation_oids {
        Some(oids) => oids.contains(&conrelid),
        None => true,
    }
}

pub(crate) struct PgConstraintRow {
    pub(crate) oid: i64,
    pub(crate) name: String,
    pub(crate) contype: &'static str,
    pub(crate) conrelid: i64,
    pub(crate) conindid: i64,
    pub(crate) confrelid: i64,
    pub(crate) confupdtype: &'static str,
    pub(crate) confdeltype: &'static str,
    pub(crate) conkey: Vec<i64>,
    pub(crate) confkey: Vec<i64>,
    pub(crate) conbin: Option<String>,
    pub(crate) convalidated: bool,
}

pub(crate) fn pg_constraint_row(row: PgConstraintRow) -> BTreeMap<String, SqlValue> {
    virtual_row([
        ("oid", SqlValue::Int(row.oid)),
        ("conname", SqlValue::String(row.name)),
        ("connamespace", SqlValue::Int(2200)),
        ("contype", SqlValue::String(row.contype.to_string())),
        ("condeferrable", SqlValue::Bool(false)),
        ("condeferred", SqlValue::Bool(false)),
        ("conenforced", SqlValue::Bool(true)),
        ("convalidated", SqlValue::Bool(row.convalidated)),
        ("conrelid", SqlValue::Int(row.conrelid)),
        ("contypid", SqlValue::Int(0)),
        ("conindid", SqlValue::Int(row.conindid)),
        ("conparentid", SqlValue::Int(0)),
        ("confrelid", SqlValue::Int(row.confrelid)),
        ("confupdtype", SqlValue::String(row.confupdtype.to_string())),
        ("confdeltype", SqlValue::String(row.confdeltype.to_string())),
        ("confmatchtype", SqlValue::String("s".to_string())),
        ("conislocal", SqlValue::Bool(true)),
        ("coninhcount", SqlValue::Int(0)),
        ("connoinherit", SqlValue::Bool(true)),
        ("conperiod", SqlValue::Bool(false)),
        ("conkey", constraint_key_value(&row.conkey)),
        ("confkey", constraint_key_value(&row.confkey)),
        ("conpfeqop", SqlValue::Null),
        ("conppeqop", SqlValue::Null),
        ("conffeqop", SqlValue::Null),
        ("confdelsetcols", SqlValue::Null),
        ("conexclop", SqlValue::Null),
        (
            "conbin",
            row.conbin.map(SqlValue::String).unwrap_or(SqlValue::Null),
        ),
    ])
}

pub(crate) fn constraint_key_value(attnums: &[i64]) -> SqlValue {
    if attnums.is_empty() {
        SqlValue::Null
    } else {
        SqlValue::String(
            attnums
                .iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(" "),
        )
    }
}

pub(crate) fn constraint_attnums(
    schemas: &[TableSchema],
    table: &str,
    columns: &[String],
) -> Vec<i64> {
    columns
        .iter()
        .map(|column| attnum_for_column(schemas, table, column).unwrap_or(0))
        .collect()
}

pub(crate) fn constraint_collation_oids(
    schemas: &[TableSchema],
    table: &str,
    columns: &[String],
) -> Vec<i64> {
    let schema = schemas
        .iter()
        .find(|schema| schema.name.eq_ignore_ascii_case(table));
    columns
        .iter()
        .map(|name| {
            schema
                .and_then(|schema| schema.column(name))
                .filter(|column| pg_type_is_collatable(&column.pg_type))
                .map(ColumnSchema::collation_oid)
                .unwrap_or(0)
        })
        .collect()
}

pub(crate) fn exclusion_constraint_columns(
    equal_columns: &[String],
    range: &Option<ExclusionRangeSchema>,
) -> Vec<String> {
    let mut columns = equal_columns.to_vec();
    let Some(range) = range else {
        return columns;
    };
    let range_columns = range
        .range_column
        .as_ref()
        .map(|column| vec![column.clone()])
        .unwrap_or_else(|| vec![exclusion_range_expression(range)]);
    for column in range_columns {
        if !columns
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(&column))
        {
            columns.push(column);
        }
    }
    columns
}

pub(crate) fn fk_action_code(action: ForeignKeyAction) -> &'static str {
    match action {
        ForeignKeyAction::NoAction => "a",
        ForeignKeyAction::Restrict => "r",
        ForeignKeyAction::Cascade => "c",
        ForeignKeyAction::SetNull => "n",
        ForeignKeyAction::SetDefault => "d",
    }
}

#[derive(Default)]
pub(crate) struct PostgresForeignKeyViewFilters {
    pub(crate) names: Option<BTreeSet<String>>,
    pub(crate) constrained_table_identifiers: Option<BTreeSet<String>>,
    pub(crate) referenced_table_identifiers: Option<BTreeSet<String>>,
    pub(crate) constrained_table_names: Option<BTreeSet<String>>,
    pub(crate) referenced_table_names: Option<BTreeSet<String>>,
    pub(crate) constrained_columns: Option<BTreeSet<String>>,
    pub(crate) referenced_columns: Option<BTreeSet<String>>,
    pub(crate) on_delete_actions: Option<BTreeSet<String>>,
    pub(crate) on_update_actions: Option<BTreeSet<String>>,
}

impl PostgresForeignKeyViewFilters {
    pub(crate) fn from_selection(
        selection: Option<&Expr>,
        alias: &str,
        view: &str,
    ) -> Result<Self> {
        Ok(Self {
            names: view_string_filters_from_selection(selection, alias, view, &["name"])?,
            constrained_table_identifiers: view_string_filters_from_selection(
                selection,
                alias,
                view,
                &["constrained_table_identifier"],
            )?,
            referenced_table_identifiers: view_string_filters_from_selection(
                selection,
                alias,
                view,
                &["referenced_table_identifier"],
            )?,
            constrained_table_names: view_string_filters_from_selection(
                selection,
                alias,
                view,
                &["constrained_table_name"],
            )?,
            referenced_table_names: view_string_filters_from_selection(
                selection,
                alias,
                view,
                &["referenced_table_name"],
            )?,
            constrained_columns: view_column_list_filters_from_selection(
                selection,
                alias,
                view,
                &["constrained_columns"],
            )?,
            referenced_columns: view_column_list_filters_from_selection(
                selection,
                alias,
                view,
                &["referenced_columns"],
            )?,
            on_delete_actions: view_string_filters_from_selection(
                selection,
                alias,
                view,
                &["on_delete_action"],
            )?,
            on_update_actions: view_string_filters_from_selection(
                selection,
                alias,
                view,
                &["on_update_action"],
            )?,
        })
    }
}

#[derive(Default)]
pub(crate) struct PostgresTriggersViewFilters {
    pub(crate) identifiers: Option<BTreeSet<String>>,
    pub(crate) trigger_names: Option<BTreeSet<String>>,
    pub(crate) table_names: Option<BTreeSet<String>>,
    pub(crate) schema_names: Option<BTreeSet<String>>,
    pub(crate) function_names: Option<BTreeSet<String>>,
}

impl PostgresTriggersViewFilters {
    pub(crate) fn from_selection(
        selection: Option<&Expr>,
        alias: &str,
        view: &str,
    ) -> Result<Self> {
        Ok(Self {
            identifiers: view_string_filters_from_selection(
                selection,
                alias,
                view,
                &["identifier"],
            )?,
            trigger_names: view_string_filters_from_selection(
                selection,
                alias,
                view,
                &["trigger_name"],
            )?,
            table_names: view_string_filters_from_selection(
                selection,
                alias,
                view,
                &["table_name"],
            )?,
            schema_names: view_string_filters_from_selection(
                selection,
                alias,
                view,
                &["schema_name"],
            )?,
            function_names: view_string_filters_from_selection(
                selection,
                alias,
                view,
                &["function_name"],
            )?,
        })
    }
}

#[derive(Default)]
pub(crate) struct PostgresConstraintsViewFilters {
    pub(crate) names: Option<BTreeSet<String>>,
    pub(crate) constraint_types: Option<BTreeSet<String>>,
    pub(crate) table_identifiers: Option<BTreeSet<String>>,
    pub(crate) constraint_valid: Option<bool>,
}

impl PostgresConstraintsViewFilters {
    pub(crate) fn from_selection(
        selection: Option<&Expr>,
        alias: &str,
        view: &str,
    ) -> Result<Self> {
        Ok(Self {
            names: view_string_filters_from_selection(selection, alias, view, &["name"])?,
            constraint_types: view_string_filters_from_selection(
                selection,
                alias,
                view,
                &["constraint_type"],
            )?,
            table_identifiers: view_string_filters_from_selection(
                selection,
                alias,
                view,
                &["table_identifier"],
            )?,
            constraint_valid: boolean_filter_from_selection(
                selection,
                alias,
                view,
                "constraint_valid",
            ),
        })
    }
}

pub(crate) fn view_string_filters_from_selection(
    selection: Option<&Expr>,
    alias: &str,
    view: &str,
    fields: &[&str],
) -> Result<Option<BTreeSet<String>>> {
    string_filter_values_from_selection(selection, alias, view, fields)
}

pub(crate) fn view_column_list_filters_from_selection(
    selection: Option<&Expr>,
    alias: &str,
    view: &str,
    fields: &[&str],
) -> Result<Option<BTreeSet<String>>> {
    column_list_filter_values_from_selection(selection, alias, view, fields)
}

pub(crate) fn postgres_constraints_view_rows_filtered(
    db: &BicDb,
    filters: &PostgresConstraintsViewFilters,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let table_oids = table_oids(db);
    let relation_oids =
        relation_oids_for_name_filters(&table_oids, filters.table_identifiers.as_ref(), None);
    if relation_oids.as_ref().is_some_and(BTreeSet::is_empty) {
        return Ok(Vec::new());
    }

    let schemas = load_schemas_for_relation_oid_filter(db, &table_oids, relation_oids.as_ref())?;
    let schema_by_oid = schemas
        .iter()
        .filter_map(|schema| table_oids.get(&schema.name).map(|oid| (*oid, schema)))
        .collect::<BTreeMap<_, _>>();
    let constraint_rows = pg_constraint_rows_from_schemas(
        &schemas,
        &table_oids,
        filters.names.as_ref(),
        filters.constraint_types.as_ref(),
        relation_oids.as_ref(),
    );
    let mut rows = Vec::new();

    for constraint in constraint_rows {
        let Some(table_oid) = sql_value_i64(&virtual_cell(&constraint, "conrelid")) else {
            continue;
        };
        let Some(schema) = schema_by_oid.get(&table_oid) else {
            continue;
        };
        let table_identifier = format!("{}.{}", schema.schema_name, schema.name);
        if !catalog_name_matches(filters.table_identifiers.as_ref(), &table_identifier) {
            continue;
        }
        let constraint_valid = matches!(
            virtual_cell(&constraint, "convalidated"),
            SqlValue::Bool(true)
        );
        if filters
            .constraint_valid
            .is_some_and(|expected| expected != constraint_valid)
        {
            continue;
        }

        let column_names =
            constraint_column_names(&schemas, &schema.name, &virtual_cell(&constraint, "conkey"));
        let parent_constraint_oid = match virtual_cell(&constraint, "conparentid") {
            SqlValue::Int(0) | SqlValue::Null => SqlValue::Null,
            value => value,
        };
        let definition =
            constraint_definition_value(&constraint, &column_names, &schema_by_oid, &schemas);

        rows.push(virtual_row([
            ("oid", virtual_cell(&constraint, "oid")),
            ("name", virtual_cell(&constraint, "conname")),
            ("constraint_type", virtual_cell(&constraint, "contype")),
            ("constraint_valid", SqlValue::Bool(constraint_valid)),
            ("column_names", sql_string_array_value(&column_names)),
            ("table_identifier", SqlValue::String(table_identifier)),
            ("parent_constraint_oid", parent_constraint_oid),
            ("definition", definition),
        ]));
    }

    rows.sort_by(|left, right| {
        virtual_cell(left, "table_identifier")
            .to_cell()
            .cmp(&virtual_cell(right, "table_identifier").to_cell())
            .then_with(|| {
                virtual_cell(left, "name")
                    .to_cell()
                    .cmp(&virtual_cell(right, "name").to_cell())
            })
    });
    Ok(rows)
}

pub(crate) fn postgres_foreign_key_view_rows_filtered(
    db: &BicDb,
    filters: &PostgresForeignKeyViewFilters,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let schemas = list_schemas(db)?;
    let table_oids = table_oids(db);
    let constrained_oids = relation_oids_for_name_filters(
        &table_oids,
        filters.constrained_table_identifiers.as_ref(),
        filters.constrained_table_names.as_ref(),
    );
    let referenced_oids = relation_oids_for_name_filters(
        &table_oids,
        filters.referenced_table_identifiers.as_ref(),
        filters.referenced_table_names.as_ref(),
    );
    if constrained_oids.as_ref().is_some_and(BTreeSet::is_empty)
        || referenced_oids.as_ref().is_some_and(BTreeSet::is_empty)
    {
        return Ok(Vec::new());
    }

    let oid_to_table = table_oids
        .iter()
        .map(|(table, oid)| (*oid, table.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut rows = Vec::new();
    let fk_types = BTreeSet::from(["f".to_string()]);

    for constraint in pg_constraint_rows_from_schemas(
        &schemas,
        &table_oids,
        filters.names.as_ref(),
        Some(&fk_types),
        constrained_oids.as_ref(),
    ) {
        let Some(constrained_oid) = sql_value_i64(&virtual_cell(&constraint, "conrelid")) else {
            continue;
        };
        let Some(referenced_oid) = sql_value_i64(&virtual_cell(&constraint, "confrelid")) else {
            continue;
        };
        if referenced_oids
            .as_ref()
            .is_some_and(|oids| !oids.contains(&referenced_oid))
        {
            continue;
        }
        let Some(constrained_table) = oid_to_table.get(&constrained_oid) else {
            continue;
        };
        let Some(referenced_table) = oid_to_table.get(&referenced_oid) else {
            continue;
        };
        let (constrained_schema, constrained_name) = relation_schema_and_name(constrained_table);
        let (referenced_schema, referenced_name) = relation_schema_and_name(referenced_table);
        let constrained_identifier = format!("{constrained_schema}.{constrained_name}");
        let referenced_identifier = format!("{referenced_schema}.{referenced_name}");
        if !catalog_name_matches(
            filters.constrained_table_identifiers.as_ref(),
            &constrained_identifier,
        ) || !catalog_name_matches(
            filters.referenced_table_identifiers.as_ref(),
            &referenced_identifier,
        ) || !catalog_name_matches(filters.constrained_table_names.as_ref(), constrained_name)
            || !catalog_name_matches(filters.referenced_table_names.as_ref(), referenced_name)
            || !catalog_name_matches(
                filters.on_delete_actions.as_ref(),
                &virtual_cell(&constraint, "confdeltype").to_cell(),
            )
            || !catalog_name_matches(
                filters.on_update_actions.as_ref(),
                &virtual_cell(&constraint, "confupdtype").to_cell(),
            )
        {
            continue;
        }
        let constrained_columns = foreign_key_column_names(
            &schemas,
            constrained_table,
            &virtual_cell(&constraint, "conkey"),
        );
        let referenced_columns = foreign_key_column_names(
            &schemas,
            referenced_table,
            &virtual_cell(&constraint, "confkey"),
        );
        let constrained_column_list = canonical_column_list(&constrained_columns);
        let referenced_column_list = canonical_column_list(&referenced_columns);
        if !catalog_name_matches(
            filters.constrained_columns.as_ref(),
            &constrained_column_list,
        ) || !catalog_name_matches(filters.referenced_columns.as_ref(), &referenced_column_list)
        {
            continue;
        }

        rows.push(virtual_row([
            ("oid", virtual_cell(&constraint, "oid")),
            ("name", virtual_cell(&constraint, "conname")),
            (
                "constrained_table_identifier",
                SqlValue::String(constrained_identifier),
            ),
            (
                "referenced_table_identifier",
                SqlValue::String(referenced_identifier),
            ),
            (
                "constrained_table_name",
                SqlValue::String(constrained_name.to_string()),
            ),
            (
                "referenced_table_name",
                SqlValue::String(referenced_name.to_string()),
            ),
            (
                "constrained_columns",
                foreign_key_column_list_value(&constrained_columns),
            ),
            (
                "referenced_columns",
                foreign_key_column_list_value(&referenced_columns),
            ),
            ("on_delete_action", virtual_cell(&constraint, "confdeltype")),
            ("on_update_action", virtual_cell(&constraint, "confupdtype")),
            ("is_inherited", SqlValue::Bool(false)),
            ("is_valid", virtual_cell(&constraint, "convalidated")),
            ("parent_oid", SqlValue::Null),
        ]));
    }

    Ok(rows)
}

pub(crate) fn postgres_triggers_view_rows_filtered(
    db: &BicDb,
    filters: &PostgresTriggersViewFilters,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let filtered_relation_names =
        relation_names_for_identifier_filters(None, filters.table_names.as_ref());
    let schemas = if let Some(relation_names) = filtered_relation_names.as_ref() {
        load_relation_schemas_by_name(db, relation_names)?
    } else {
        list_schemas(db)?
    };
    let schema_by_table = schemas
        .iter()
        .map(|schema| (schema.name.to_ascii_lowercase(), schema.schema_name.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut rows = Vec::new();

    for trigger in list_triggers(db)? {
        if !catalog_name_matches(filters.trigger_names.as_ref(), &trigger.name)
            || !catalog_name_matches(filters.table_names.as_ref(), &trigger.table_name)
            || !catalog_name_matches(filters.function_names.as_ref(), &trigger.function_name)
        {
            continue;
        }
        let Some(schema_name) = schema_by_table
            .get(&trigger.table_name.to_ascii_lowercase())
            .cloned()
        else {
            continue;
        };
        let identifier = format!("{schema_name}.{}.{}", trigger.table_name, trigger.name);
        if !catalog_name_matches(filters.schema_names.as_ref(), &schema_name)
            || !catalog_name_matches(filters.identifiers.as_ref(), &identifier)
        {
            continue;
        }

        rows.push(virtual_row([
            ("identifier", SqlValue::String(identifier)),
            ("trigger_name", SqlValue::String(trigger.name)),
            ("table_name", SqlValue::String(trigger.table_name)),
            ("schema_name", SqlValue::String(schema_name)),
            ("function_name", SqlValue::String(trigger.function_name)),
        ]));
    }

    Ok(rows)
}

pub(crate) fn relation_schema_and_name(relation: &str) -> (&str, &str) {
    relation.rsplit_once('.').unwrap_or(("public", relation))
}

pub(crate) fn foreign_key_column_names(
    schemas: &[TableSchema],
    relation: &str,
    key_value: &SqlValue,
) -> Vec<String> {
    constraint_column_names(schemas, relation, key_value)
}

pub(crate) fn constraint_column_names(
    schemas: &[TableSchema],
    relation: &str,
    key_value: &SqlValue,
) -> Vec<String> {
    let columns = columns_for_relation(schemas, relation);
    constraint_attnums_from_value(key_value)
        .into_iter()
        .filter_map(|attnum| {
            usize::try_from(attnum.saturating_sub(1))
                .ok()
                .and_then(|idx| columns.get(idx))
                .map(|column| column.name.clone())
        })
        .collect()
}

pub(crate) fn sql_string_array_value(values: &[String]) -> SqlValue {
    SqlValue::Json(JsonValue::Array(
        values
            .iter()
            .cloned()
            .map(JsonValue::String)
            .collect::<Vec<_>>(),
    ))
}

pub(crate) fn constraint_definition_value(
    constraint: &BTreeMap<String, SqlValue>,
    column_names: &[String],
    schema_by_oid: &BTreeMap<i64, &TableSchema>,
    schemas: &[TableSchema],
) -> SqlValue {
    let columns = column_names.join(", ");
    match virtual_cell(constraint, "contype").to_cell().as_str() {
        "p" => SqlValue::String(format!("PRIMARY KEY ({columns})")),
        "u" => SqlValue::String(format!("UNIQUE ({columns})")),
        "c" => match virtual_cell(constraint, "conbin") {
            SqlValue::String(expr) => SqlValue::String(pg_check_constraint_definition(&expr)),
            _ => SqlValue::Null,
        },
        "f" => {
            let Some(foreign_oid) = sql_value_i64(&virtual_cell(constraint, "confrelid")) else {
                return SqlValue::Null;
            };
            let Some(foreign_schema) = schema_by_oid.get(&foreign_oid) else {
                return SqlValue::Null;
            };
            let referred_columns = constraint_column_names(
                schemas,
                &foreign_schema.name,
                &virtual_cell(constraint, "confkey"),
            );
            let foreign_name = if foreign_schema.schema_name.eq_ignore_ascii_case("public") {
                foreign_schema.name.clone()
            } else {
                format!("{}.{}", foreign_schema.schema_name, foreign_schema.name)
            };
            SqlValue::String(format!(
                "FOREIGN KEY ({columns}) REFERENCES {}({})",
                foreign_name,
                referred_columns.join(", ")
            ))
        }
        "n" => column_names
            .first()
            .map(|column| SqlValue::String(format!("{column} IS NOT NULL")))
            .unwrap_or(SqlValue::Null),
        "x" => SqlValue::String(format!("EXCLUDE ({columns})")),
        _ => SqlValue::Null,
    }
}

pub(crate) fn pg_constraint_definition_for_oid(
    db: &BicDb,
    constraint_oid: i64,
) -> Result<Option<String>> {
    if let Some(constraint) = pg_domain_constraint_rows(db, None, None)?
        .into_iter()
        .find(|constraint| sql_value_i64(&virtual_cell(constraint, "oid")) == Some(constraint_oid))
    {
        return Ok(
            match virtual_cell(&constraint, "contype").to_cell().as_str() {
                "c" => sql_value_text(&virtual_cell(&constraint, "conbin"))
                    .map(|expression| format!("CHECK ({expression})")),
                "n" => Some("NOT NULL".to_string()),
                _ => None,
            },
        );
    }
    let schemas = list_schemas(db)?;
    let table_oids = table_oids(db);
    let schema_by_oid = schemas
        .iter()
        .filter_map(|schema| table_oids.get(&schema.name).map(|oid| (*oid, schema)))
        .collect::<BTreeMap<_, _>>();
    let Some(constraint) = pg_constraint_rows_from_schemas(&schemas, &table_oids, None, None, None)
        .into_iter()
        .find(|constraint| sql_value_i64(&virtual_cell(constraint, "oid")) == Some(constraint_oid))
    else {
        return Ok(None);
    };
    let Some(table_oid) = sql_value_i64(&virtual_cell(&constraint, "conrelid")) else {
        return Ok(None);
    };
    let Some(schema) = schema_by_oid.get(&table_oid) else {
        return Ok(None);
    };
    if virtual_cell(&constraint, "contype").to_cell() == "x" {
        let name = virtual_cell(&constraint, "conname").to_cell();
        if let Some(ConstraintSchema::Exclusion {
            access_method,
            equal_columns,
            range,
            predicate,
            ..
        }) = schema
            .constraints
            .iter()
            .find(|candidate| constraint_name(candidate) == name)
        {
            return Ok(Some(exclusion_constraint_definition(
                access_method,
                equal_columns,
                range,
                predicate.as_deref(),
            )));
        }
    }
    let columns =
        constraint_column_names(&schemas, &schema.name, &virtual_cell(&constraint, "conkey"));
    let SqlValue::String(mut definition) =
        constraint_definition_value(&constraint, &columns, &schema_by_oid, &schemas)
    else {
        return Ok(None);
    };
    if virtual_cell(&constraint, "contype").to_cell() == "f" {
        for (label, field) in [("ON UPDATE", "confupdtype"), ("ON DELETE", "confdeltype")] {
            let action = match virtual_cell(&constraint, field).to_cell().as_str() {
                "r" => Some("RESTRICT"),
                "c" => Some("CASCADE"),
                "n" => Some("SET NULL"),
                "d" => Some("SET DEFAULT"),
                _ => None,
            };
            if let Some(action) = action {
                definition.push_str(&format!(" {label} {action}"));
            }
        }
    }
    Ok(Some(definition))
}

pub(crate) fn exclusion_constraint_definition(
    access_method: &str,
    equal_columns: &[String],
    range: &Option<ExclusionRangeSchema>,
    predicate: Option<&str>,
) -> String {
    let mut elements = equal_columns
        .iter()
        .map(|column| format!("{column} WITH ="))
        .collect::<Vec<_>>();
    if let Some(range) = range {
        elements.push(format!(
            "{} WITH {}",
            exclusion_range_expression(range),
            range.operator
        ));
    }
    let mut definition = format!("EXCLUDE USING {access_method} ({})", elements.join(", "));
    if let Some(predicate) = predicate {
        definition.push_str(&format!(
            " WHERE (({}))",
            predicate.trim_matches(['(', ')'])
        ));
    }
    definition
}

pub(crate) fn pg_check_constraint_definition(expression: &str) -> String {
    let trimmed = expression.trim();
    if !check_expression_may_use_in_list_definition(trimmed) {
        return format!("CHECK ({trimmed})");
    }
    parse_check_expression(expression)
        .ok()
        .and_then(|expr| pg_check_in_list_definition(&expr))
        .unwrap_or_else(|| format!("CHECK ({trimmed})"))
}

pub(crate) fn check_expression_may_use_in_list_definition(expression: &str) -> bool {
    expression
        .split(|ch: char| !is_ident_char(ch))
        .any(|token| token.eq_ignore_ascii_case("in"))
}

pub(crate) fn pg_check_in_list_definition(expr: &Expr) -> Option<String> {
    match unwrap_nested_expr(expr) {
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            let column = check_definition_column_expr(expr)?;
            let values = list
                .iter()
                .map(check_definition_literal_expr)
                .collect::<Option<Vec<_>>>()?;
            Some(format!(
                "CHECK (({} = ANY (ARRAY[{}])))",
                column,
                values.join(", ")
            ))
        }
        _ => None,
    }
}

pub(crate) fn check_definition_column_expr(expr: &Expr) -> Option<String> {
    match unwrap_nested_expr(expr) {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(idents) => idents.last().map(|ident| ident.value.clone()),
        _ => None,
    }
}

pub(crate) fn check_definition_literal_expr(expr: &Expr) -> Option<String> {
    if row_independent_expr(expr) {
        Some(expr.to_string())
    } else {
        None
    }
}

pub(crate) fn constraint_attnums_from_value(value: &SqlValue) -> Vec<i64> {
    match value {
        SqlValue::String(value) => value
            .split_whitespace()
            .filter_map(|part| part.parse().ok())
            .collect(),
        _ => Vec::new(),
    }
}

pub(crate) fn relation_oids_for_name_filters(
    table_oids: &BTreeMap<String, i64>,
    identifiers: Option<&BTreeSet<String>>,
    names: Option<&BTreeSet<String>>,
) -> Option<BTreeSet<i64>> {
    if identifiers.is_none() && names.is_none() {
        return None;
    }
    Some(
        table_oids
            .iter()
            .filter_map(|(relation, oid)| {
                let (schema, name) = relation_schema_and_name(relation);
                let identifier = format!("{schema}.{name}");
                (catalog_name_matches(identifiers, &identifier)
                    && catalog_name_matches(names, name))
                .then_some(*oid)
            })
            .collect(),
    )
}

pub(crate) fn relation_names_for_oid_filter(
    table_oids: &BTreeMap<String, i64>,
    relation_oids: Option<&BTreeSet<i64>>,
) -> Option<BTreeSet<String>> {
    relation_oids.map(|oids| {
        table_oids
            .iter()
            .filter_map(|(relation, oid)| oids.contains(oid).then_some(relation.clone()))
            .collect()
    })
}

pub(crate) fn relation_names_for_identifier_filters(
    identifiers: Option<&BTreeSet<String>>,
    names: Option<&BTreeSet<String>>,
) -> Option<BTreeSet<String>> {
    if identifiers.is_none() && names.is_none() {
        return None;
    }
    let mut relations = BTreeSet::new();
    if let Some(identifiers) = identifiers {
        for identifier in identifiers {
            let (_, name) = relation_schema_and_name(identifier);
            if catalog_name_matches(names, name) {
                relations.insert(normalize_object_name(name));
            }
        }
        return Some(relations);
    }
    if let Some(names) = names {
        for name in names {
            relations.insert(normalize_object_name(name));
        }
    }
    Some(relations)
}

pub(crate) fn load_relation_schemas_by_name(
    db: &BicDb,
    relation_names: &BTreeSet<String>,
) -> Result<Vec<TableSchema>> {
    // The fast path resolves each requested name directly. A filter written
    // against a schema-qualified table gives the LOGICAL name, which no
    // physical lookup can find, so fall back to the full listing and let the
    // name filters (which match either spelling) do the work. Without this,
    // `WHERE tablename = '<name>'` silently returned nothing for every table
    // outside `public`.
    let mut unresolved = false;
    let mut schemas = Vec::new();
    for relation in relation_names {
        if let Some(schema) = load_schema(db, relation)? {
            schemas.push(schema);
        } else if let Some(view) = load_view(db, relation)? {
            let columns = catalog_view_columns(&view);
            schemas.push(TableSchema {
                name: view.name,
                schema_name: "public".to_string(),
                row_type_oid: None,
                row_array_type_oid: None,
                columns,
                primary_key_name: None,
                indexes: view.indexes,
                constraints: Vec::new(),
                rls_enabled: false,
                rls_forced: false,
                policies: Vec::new(),
                owner: None,
                partitioning: None,
                partition_of: None,
            });
        } else {
            unresolved = true;
        }
    }
    if unresolved {
        return relation_schemas(db);
    }
    schemas.sort_by(|left, right| {
        left.schema_name
            .cmp(&right.schema_name)
            .then_with(|| left.name.cmp(&right.name))
    });
    Ok(schemas)
}

pub(crate) fn canonical_column_list(columns: &[String]) -> String {
    columns
        .iter()
        .map(|column| column.trim())
        .filter(|column| !column.is_empty())
        .collect::<Vec<_>>()
        .join(",")
}

pub(crate) fn canonical_column_list_text(columns: &str) -> String {
    let columns = columns.trim();
    if columns.starts_with('{') && columns.ends_with('}') {
        if let Ok(values) = parse_array_literal(columns) {
            return canonical_column_list(
                &values
                    .into_iter()
                    .filter_map(|value| value.as_str().map(str::to_string))
                    .collect::<Vec<_>>(),
            );
        }
    }
    columns
        .split(',')
        .map(str::trim)
        .filter(|column| !column.is_empty())
        .collect::<Vec<_>>()
        .join(",")
}

pub(crate) fn foreign_key_column_list_value(columns: &[String]) -> SqlValue {
    if columns.is_empty() {
        SqlValue::Null
    } else {
        sql_string_array_value(columns)
    }
}

pub(crate) fn postgres_partitioned_table_view_rows_filtered(
    db: &BicDb,
    view: &ViewSchema,
    identifiers: Option<&BTreeSet<String>>,
    names: Option<&BTreeSet<String>>,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let current_schema_only = view
        .query_sql
        .to_ascii_lowercase()
        .contains("current_schema");
    let table_oids = table_oids(db);
    let mut rows = Vec::new();
    let filtered_relation_names = relation_names_for_identifier_filters(identifiers, names);
    let schemas = if let Some(relation_names) = filtered_relation_names.as_ref() {
        load_relation_schemas_by_name(db, relation_names)?
    } else {
        list_schemas(db)?
    };

    for schema in schemas {
        let Some(partitioning) = schema.partitioning.as_ref() else {
            continue;
        };
        if current_schema_only && !schema.schema_name.eq_ignore_ascii_case("public") {
            continue;
        }
        let identifier = format!("{}.{}", schema.schema_name, schema.name);
        if !catalog_name_matches(identifiers, &identifier)
            || !catalog_name_matches(names, &schema.name)
        {
            continue;
        }
        let oid = *table_oids.get(&schema.name).unwrap_or(&0);
        rows.push(virtual_row([
            ("identifier", SqlValue::String(identifier)),
            ("oid", SqlValue::Int(oid)),
            ("schema", SqlValue::String(schema.schema_name.clone())),
            ("name", SqlValue::String(schema.name.clone())),
            ("strategy", SqlValue::String(partitioning.strategy.clone())),
            (
                "key_columns",
                SqlValue::String(partitioning.key_columns.join(",")),
            ),
        ]));
    }
    rows.sort_by(|left, right| {
        virtual_cell(left, "identifier")
            .to_cell()
            .cmp(&virtual_cell(right, "identifier").to_cell())
    });
    Ok(rows)
}

pub(crate) fn postgres_partitions_view_rows_filtered(
    db: &BicDb,
    _view: &ViewSchema,
    identifiers: Option<&BTreeSet<String>>,
    parent_identifiers: Option<&BTreeSet<String>>,
    schema_names: Option<&BTreeSet<String>>,
    names: Option<&BTreeSet<String>>,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let table_oids = table_oids(db);
    let filtered_relation_names = relation_names_for_identifier_filters(identifiers, names);
    let schemas = if let Some(relation_names) = filtered_relation_names.as_ref() {
        load_relation_schemas_by_name(db, relation_names)?
    } else {
        list_schemas(db)?
    };
    let schema_by_name = schemas
        .iter()
        .map(|schema| (schema.name.to_ascii_lowercase(), schema.schema_name.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut rows = Vec::new();

    for schema in schemas {
        let Some(partition_of) = schema.partition_of.as_ref() else {
            continue;
        };
        let display_name = catalog_display_name(&schema.name);
        let parent_schema = schema_by_name
            .get(&partition_of.parent_table.to_ascii_lowercase())
            .map(|schema_name| schema_name.as_str())
            .unwrap_or(partition_of.parent_schema.as_str());
        let identifier = format!("{}.{}", schema.schema_name, display_name);
        let parent_identifier = format!(
            "{}.{}",
            parent_schema,
            catalog_display_name(&partition_of.parent_table)
        );
        if !catalog_name_matches(identifiers, &identifier)
            || !catalog_name_matches(parent_identifiers, &parent_identifier)
            || !catalog_name_matches(schema_names, &schema.schema_name)
            || !catalog_name_matches(names, &display_name)
        {
            continue;
        }
        rows.push(virtual_row([
            ("identifier", SqlValue::String(identifier)),
            (
                "oid",
                SqlValue::Int(*table_oids.get(&schema.name).unwrap_or(&0)),
            ),
            ("schema", SqlValue::String(schema.schema_name.clone())),
            ("name", SqlValue::String(display_name)),
            ("parent_identifier", SqlValue::String(parent_identifier)),
            ("condition", SqlValue::String(partition_of.bound.clone())),
        ]));
    }
    rows.sort_by(|left, right| {
        virtual_cell(left, "identifier")
            .to_cell()
            .cmp(&virtual_cell(right, "identifier").to_cell())
    });
    Ok(rows)
}

#[derive(Default)]
pub(crate) struct PostgresIndexesViewFilters {
    pub(crate) identifiers: Option<BTreeSet<String>>,
    pub(crate) schema_names: Option<BTreeSet<String>>,
    pub(crate) names: Option<BTreeSet<String>>,
    pub(crate) table_names: Option<BTreeSet<String>>,
    pub(crate) access_methods: Option<BTreeSet<String>>,
    pub(crate) unique: Option<bool>,
    pub(crate) valid: Option<bool>,
    pub(crate) exclusion: Option<bool>,
    pub(crate) expression: Option<bool>,
    pub(crate) partial: Option<bool>,
}

impl PostgresIndexesViewFilters {
    pub(crate) fn from_selection(
        selection: Option<&Expr>,
        alias: &str,
        view: &str,
    ) -> Result<Self> {
        Ok(Self {
            identifiers: view_string_filters_from_selection(
                selection,
                alias,
                view,
                &["identifier"],
            )?,
            schema_names: view_string_filters_from_selection(selection, alias, view, &["schema"])?,
            names: view_string_filters_from_selection(selection, alias, view, &["name"])?,
            table_names: view_string_filters_from_selection(
                selection,
                alias,
                view,
                &["tablename"],
            )?,
            access_methods: view_string_filters_from_selection(selection, alias, view, &["type"])?,
            unique: boolean_filter_from_selection(selection, alias, view, "unique"),
            valid: boolean_filter_from_selection(selection, alias, view, "valid_index"),
            exclusion: boolean_filter_from_selection(selection, alias, view, "exclusion"),
            expression: boolean_filter_from_selection(selection, alias, view, "expression"),
            partial: boolean_filter_from_selection(selection, alias, view, "partial"),
        })
    }
}

pub(crate) fn postgres_indexes_view_rows_filtered(
    db: &BicDb,
    filters: &PostgresIndexesViewFilters,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let table_oids = table_oids(db);
    let filtered_relation_names =
        relation_names_for_identifier_filters(None, filters.table_names.as_ref());
    let schemas = if let Some(relation_names) = filtered_relation_names.as_ref() {
        load_relation_schemas_by_name(db, relation_names)?
    } else {
        relation_schemas(db)?
    };
    let mut rows = Vec::new();

    for (table, primary_key_columns) in primary_key_indexes(&schemas, db) {
        let schema_name = schema_name_for_relation(&schemas, &table);
        let index_name = primary_key_constraint_name(&schemas, &table);
        let identifier = format!("{schema_name}.{index_name}");
        let access_method = "btree";
        let unique = true;
        let valid = true;
        let exclusion = false;
        let expression = false;
        let partial = false;
        if !postgres_indexes_view_filter_matches(
            filters,
            &identifier,
            &schema_name,
            &index_name,
            &table,
            access_method,
            unique,
            valid,
            exclusion,
            expression,
            partial,
        ) {
            continue;
        }
        let table_oid = *table_oids.get(&table).unwrap_or(&0);
        let definition = pg_indexdef_string(
            &schema_name,
            &index_name,
            &table,
            unique,
            access_method,
            &index_expression_for_columns(&primary_key_columns),
        );
        rows.push(postgres_indexes_view_row(PostgresIndexesViewRow {
            identifier,
            indexrelid: primary_index_oid(table_oid),
            schema_name: schema_name.clone(),
            name: index_name.clone(),
            table_name: table.clone(),
            access_method: access_method.to_string(),
            unique,
            valid,
            partitioned: false,
            exclusion,
            expression,
            partial,
            definition,
        }));
    }

    for index in catalog_indexes(&schemas, db) {
        let identifier = format!("{}.{}", index.schema_name, index.name);
        let valid = true;
        let exclusion = index.exclusion;
        let expression = index.indexprs.is_some();
        let partial = index.predicate.is_some();
        if !postgres_indexes_view_filter_matches(
            filters,
            &identifier,
            &index.schema_name,
            &index.name,
            &index.collection,
            &index.access_method,
            index.unique,
            valid,
            exclusion,
            expression,
            partial,
        ) {
            continue;
        }
        rows.push(postgres_indexes_view_row(PostgresIndexesViewRow {
            identifier,
            indexrelid: secondary_index_oid(&index.schema_name, &index.name),
            schema_name: index.schema_name.clone(),
            name: index.name.clone(),
            table_name: index.collection.clone(),
            access_method: index.access_method.clone(),
            unique: index.unique,
            valid,
            partitioned: false,
            exclusion,
            expression,
            partial,
            definition: catalog_indexdef_string(&index),
        }));
    }

    rows.sort_by(|left, right| {
        virtual_cell(left, "identifier")
            .to_cell()
            .cmp(&virtual_cell(right, "identifier").to_cell())
    });
    Ok(rows)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn postgres_indexes_view_filter_matches(
    filters: &PostgresIndexesViewFilters,
    identifier: &str,
    schema_name: &str,
    index_name: &str,
    table_name: &str,
    access_method: &str,
    unique: bool,
    valid: bool,
    exclusion: bool,
    expression: bool,
    partial: bool,
) -> bool {
    catalog_name_matches(filters.identifiers.as_ref(), identifier)
        && catalog_name_matches(filters.schema_names.as_ref(), schema_name)
        && catalog_name_matches(filters.names.as_ref(), index_name)
        && catalog_name_matches(filters.table_names.as_ref(), table_name)
        && catalog_name_matches(filters.access_methods.as_ref(), access_method)
        && filters.unique.is_none_or(|expected| expected == unique)
        && filters.valid.is_none_or(|expected| expected == valid)
        && filters
            .exclusion
            .is_none_or(|expected| expected == exclusion)
        && filters
            .expression
            .is_none_or(|expected| expected == expression)
        && filters.partial.is_none_or(|expected| expected == partial)
}

pub(crate) struct PostgresIndexesViewRow {
    pub(crate) identifier: String,
    pub(crate) indexrelid: i64,
    pub(crate) schema_name: String,
    pub(crate) name: String,
    pub(crate) table_name: String,
    pub(crate) access_method: String,
    pub(crate) unique: bool,
    pub(crate) valid: bool,
    pub(crate) partitioned: bool,
    pub(crate) exclusion: bool,
    pub(crate) expression: bool,
    pub(crate) partial: bool,
    pub(crate) definition: String,
}

pub(crate) fn postgres_indexes_view_row(row: PostgresIndexesViewRow) -> BTreeMap<String, SqlValue> {
    virtual_row([
        ("identifier", SqlValue::String(row.identifier)),
        ("indexrelid", SqlValue::Int(row.indexrelid)),
        ("schema", SqlValue::String(row.schema_name)),
        ("name", SqlValue::String(row.name)),
        (
            "tablename",
            SqlValue::String(catalog_display_name(&row.table_name)),
        ),
        ("type", SqlValue::String(row.access_method)),
        ("unique", SqlValue::Bool(row.unique)),
        ("valid_index", SqlValue::Bool(row.valid)),
        ("partitioned", SqlValue::Bool(row.partitioned)),
        ("exclusion", SqlValue::Bool(row.exclusion)),
        ("expression", SqlValue::Bool(row.expression)),
        ("partial", SqlValue::Bool(row.partial)),
        ("definition", SqlValue::String(row.definition)),
        ("ondisk_size_bytes", SqlValue::Int(0)),
    ])
}

pub(crate) fn postgres_sequences_view_rows_filtered(
    db: &BicDb,
    seq_names: Option<&BTreeSet<String>>,
    table_names: Option<&BTreeSet<String>>,
    column_names: Option<&BTreeSet<String>>,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let mut rows = Vec::new();
    for sequence in list_sequences(db)? {
        if !catalog_name_matches(seq_names, &sequence.name) {
            continue;
        }
        if !catalog_optional_name_matches(table_names, sequence.owned_by_table.as_deref())
            || !catalog_optional_name_matches(column_names, sequence.owned_by_column.as_deref())
        {
            continue;
        }
        rows.push(virtual_row([
            ("seq_name", SqlValue::String(sequence.name)),
            (
                "table_name",
                sequence
                    .owned_by_table
                    .map(SqlValue::String)
                    .unwrap_or(SqlValue::Null),
            ),
            (
                "col_name",
                sequence
                    .owned_by_column
                    .map(SqlValue::String)
                    .unwrap_or(SqlValue::Null),
            ),
            ("seq_max", SqlValue::Int(sequence.max_value)),
            ("seq_min", SqlValue::Int(sequence.min_value)),
            ("seq_start", SqlValue::Int(sequence.start_value)),
            ("last_value", SqlValue::Int(sequence.last_value)),
        ]));
    }
    rows.sort_by(|left, right| {
        virtual_cell(left, "seq_name")
            .to_cell()
            .cmp(&virtual_cell(right, "seq_name").to_cell())
    });
    Ok(rows)
}

pub(crate) fn catalog_optional_name_matches(
    names: Option<&BTreeSet<String>>,
    value: Option<&str>,
) -> bool {
    match names {
        Some(names) => value.is_some_and(|value| catalog_name_matches(Some(names), value)),
        None => true,
    }
}

pub(crate) fn graph_node_rows(db: &BicDb) -> Vec<BTreeMap<String, SqlValue>> {
    db.graph_nodes()
        .into_iter()
        .map(|(projection, node)| {
            virtual_row([
                ("projection", SqlValue::String(projection)),
                ("id", SqlValue::String(node.id)),
                ("label", SqlValue::String(node.label)),
                ("properties", SqlValue::Json(node.properties)),
            ])
        })
        .collect()
}

pub(crate) fn graph_edge_rows(db: &BicDb) -> Vec<BTreeMap<String, SqlValue>> {
    db.graph_edges()
        .into_iter()
        .map(|(projection, edge)| {
            virtual_row([
                ("projection", SqlValue::String(projection)),
                ("id", SqlValue::String(edge.id)),
                ("from", SqlValue::String(edge.from)),
                ("to", SqlValue::String(edge.to)),
                ("label", SqlValue::String(edge.label)),
                ("properties", SqlValue::Json(edge.properties)),
                (
                    "timestamp",
                    edge.timestamp.map(SqlValue::Int).unwrap_or(SqlValue::Null),
                ),
            ])
        })
        .collect()
}

pub(crate) fn pg_inherits_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let table_oids = table_oids(db);
    let mut rows = Vec::new();
    for schema in list_schemas(db)? {
        let Some(partition_of) = schema.partition_of.as_ref() else {
            continue;
        };
        let Some(child_oid) = table_oids.get(&schema.name) else {
            continue;
        };
        let Some(parent_oid) = table_oids.get(&partition_of.parent_table) else {
            continue;
        };
        rows.push(virtual_row([
            ("inhrelid", SqlValue::Int(*child_oid)),
            ("inhparent", SqlValue::Int(*parent_oid)),
            ("inhseqno", SqlValue::Int(1)),
            ("inhdetachpending", SqlValue::Bool(false)),
        ]));
    }
    Ok(rows)
}

pub(crate) fn pg_partitioned_table_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let schemas = list_schemas(db)?;
    let table_oids = table_oids(db);
    let mut rows = Vec::new();
    for schema in &schemas {
        let Some(partitioning) = schema.partitioning.as_ref() else {
            continue;
        };
        let Some(partrelid) = table_oids.get(&schema.name) else {
            continue;
        };
        let partattrs = partitioning
            .key_columns
            .iter()
            .filter_map(|column| attnum_for_column(&schemas, &schema.name, column))
            .collect::<Vec<_>>();
        rows.push(virtual_row([
            ("partrelid", SqlValue::Int(*partrelid)),
            (
                "partstrat",
                SqlValue::String(partition_strategy_code(&partitioning.strategy).to_string()),
            ),
            ("partnatts", SqlValue::Int(partattrs.len() as i64)),
            ("partdefid", SqlValue::Int(0)),
            ("partattrs", constraint_key_value(&partattrs)),
            ("partclass", SqlValue::Null),
            ("partcollation", SqlValue::Null),
            ("partexprs", SqlValue::Null),
        ]));
    }
    Ok(rows)
}

pub(crate) fn partition_strategy_code(strategy: &str) -> &'static str {
    match strategy.to_ascii_lowercase().as_str() {
        "list" => "l",
        "range" => "r",
        "hash" => "h",
        _ => "?",
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn pg_class_row(
    oid: i64,
    name: &str,
    relnamespace: i64,
    reltype_oid: Option<i64>,
    relkind: &str,
    relnatts: i64,
    relhasindex: bool,
    relhasrules: bool,
    reltuples: f64,
    relchecks: i64,
    relhastriggers: bool,
    relhassubclass: bool,
    relispartition: bool,
    relpartbound: SqlValue,
    relacl: SqlValue,
    relam: i64,
    relrowsecurity: bool,
    relforcerowsecurity: bool,
) -> BTreeMap<String, SqlValue> {
    // `relname` is what a user reads and what tools filter on, so it carries
    // the logical name. Everything below keeps using the physical `name`: OIDs
    // are derived from it, and changing that would change object identity.
    let display_name = crate::eval::rewrite_helpers::logical_relation_name(name)
        .unwrap_or_else(|| name.to_string());
    virtual_row([
        ("oid", SqlValue::Int(oid)),
        ("relname", SqlValue::String(display_name)),
        ("relnamespace", SqlValue::Int(relnamespace)),
        (
            "reltype",
            SqlValue::Int(reltype_oid.unwrap_or_else(|| {
                if matches!(relkind, "r" | "p" | "v" | "m" | "f") {
                    table_row_type_oid(relnamespace, name)
                } else {
                    0
                }
            })),
        ),
        ("reloftype", SqlValue::Int(0)),
        ("relowner", SqlValue::Int(10)),
        ("relam", SqlValue::Int(relam)),
        ("relfilenode", SqlValue::Int(oid)),
        ("reltablespace", SqlValue::Int(0)),
        ("relpages", SqlValue::Int(0)),
        ("reltuples", SqlValue::Float(reltuples)),
        ("relallvisible", SqlValue::Int(0)),
        ("relallfrozen", SqlValue::Int(0)),
        ("reltoastrelid", SqlValue::Int(0)),
        ("relhasindex", SqlValue::Bool(relhasindex)),
        ("relisshared", SqlValue::Bool(false)),
        ("relpersistence", SqlValue::String("p".to_string())),
        ("relkind", SqlValue::String(relkind.to_string())),
        ("relnatts", SqlValue::Int(relnatts)),
        ("relchecks", SqlValue::Int(relchecks)),
        ("relhasrules", SqlValue::Bool(relhasrules)),
        ("relhastriggers", SqlValue::Bool(relhastriggers)),
        ("relhassubclass", SqlValue::Bool(relhassubclass)),
        ("relrowsecurity", SqlValue::Bool(relrowsecurity)),
        ("relforcerowsecurity", SqlValue::Bool(relforcerowsecurity)),
        ("relispopulated", SqlValue::Bool(true)),
        ("relreplident", SqlValue::String("d".to_string())),
        ("relispartition", SqlValue::Bool(relispartition)),
        ("relrewrite", SqlValue::Int(0)),
        ("relfrozenxid", SqlValue::Int(0)),
        ("relminmxid", SqlValue::Int(0)),
        ("relacl", relacl),
        ("reloptions", SqlValue::Null),
        ("relpartbound", relpartbound),
    ])
}

pub(crate) fn pg_index_row(
    indexrelid: i64,
    indrelid: i64,
    indisprimary: bool,
    indisunique: bool,
    indkey: Vec<i64>,
    indcollation: Vec<i64>,
    indclass: Vec<i64>,
    indexprs: Option<String>,
    indisexclusion: bool,
    indpred: Option<String>,
    indexdef: String,
) -> BTreeMap<String, SqlValue> {
    let indkey = indkey
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(" ");
    let indcollation = indcollation
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(" ");
    let indclass = indclass
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(" ");
    virtual_row([
        ("indexrelid", SqlValue::Int(indexrelid)),
        ("indrelid", SqlValue::Int(indrelid)),
        (
            "indnatts",
            SqlValue::Int(indkey.split_whitespace().count() as i64),
        ),
        (
            "indnkeyatts",
            SqlValue::Int(indkey.split_whitespace().count() as i64),
        ),
        ("indisunique", SqlValue::Bool(indisunique)),
        ("indnullsnotdistinct", SqlValue::Bool(false)),
        ("indisprimary", SqlValue::Bool(indisprimary)),
        ("indisexclusion", SqlValue::Bool(indisexclusion)),
        ("indimmediate", SqlValue::Bool(true)),
        ("indisclustered", SqlValue::Bool(false)),
        ("indisvalid", SqlValue::Bool(true)),
        ("indcheckxmin", SqlValue::Bool(false)),
        ("indisready", SqlValue::Bool(true)),
        ("indislive", SqlValue::Bool(true)),
        ("indisreplident", SqlValue::Bool(false)),
        ("indkey", SqlValue::String(indkey)),
        ("indcollation", SqlValue::String(indcollation)),
        ("indclass", SqlValue::String(indclass)),
        ("indoption", SqlValue::String(String::new())),
        (
            "indexprs",
            indexprs.map(SqlValue::String).unwrap_or(SqlValue::Null),
        ),
        (
            "indpred",
            indpred.map(SqlValue::String).unwrap_or(SqlValue::Null),
        ),
        ("indexdef", SqlValue::String(indexdef)),
    ])
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn proc_row(
    oid: i64,
    name: &str,
    namespace: i64,
    return_type: i64,
    returns_set: bool,
    prokind: &str,
    language: &str,
    source: &str,
    nargs: i64,
    owner: &str,
    security_definer: bool,
) -> BTreeMap<String, SqlValue> {
    virtual_row([
        ("oid", SqlValue::Int(oid)),
        ("proname", SqlValue::String(name.to_string())),
        ("pronamespace", SqlValue::Int(namespace)),
        ("proowner", SqlValue::Int(role_oid(owner))),
        ("prolang", SqlValue::Int(pg_language_oid(language))),
        ("procost", SqlValue::Float(1.0)),
        ("prorows", SqlValue::Float(0.0)),
        ("provariadic", SqlValue::Int(0)),
        ("prosupport", SqlValue::String("-".to_string())),
        ("prokind", SqlValue::String(prokind.to_string())),
        ("prosecdef", SqlValue::Bool(security_definer)),
        ("proleakproof", SqlValue::Bool(false)),
        ("proisstrict", SqlValue::Bool(false)),
        ("proretset", SqlValue::Bool(returns_set)),
        ("provolatile", SqlValue::String("s".to_string())),
        ("proparallel", SqlValue::String("s".to_string())),
        ("pronargs", SqlValue::Int(nargs)),
        ("pronargdefaults", SqlValue::Int(0)),
        ("prorettype", SqlValue::Int(return_type)),
        ("proargtypes", SqlValue::String(String::new())),
        ("proallargtypes", SqlValue::Null),
        ("proargmodes", SqlValue::Null),
        ("proargnames", SqlValue::Null),
        ("proargdefaults", SqlValue::Null),
        ("protrftypes", SqlValue::Null),
        ("prosrc", SqlValue::String(source.to_string())),
        ("probin", SqlValue::Null),
        ("prosqlbody", SqlValue::Null),
        ("proconfig", SqlValue::Null),
        ("proacl", SqlValue::Null),
    ])
}

pub(crate) fn columns_for_relation(schemas: &[TableSchema], relation: &str) -> Vec<ColumnSchema> {
    if graph_virtual_table_names().contains(&relation) {
        return graph_virtual_table_columns(relation);
    }
    let (schema_name, relation_name) = relation_schema_and_name(relation);
    schemas
        .iter()
        .find(|schema| {
            schema.name.eq_ignore_ascii_case(relation)
                || (schema.name.eq_ignore_ascii_case(relation_name)
                    && schema.schema_name.eq_ignore_ascii_case(schema_name))
        })
        .map(|schema| {
            schema
                .columns
                .iter()
                .filter(|column| !column.hidden)
                .cloned()
                .collect()
        })
        .unwrap_or_else(default_record_columns)
}

pub(crate) fn relation_schemas(db: &BicDb) -> Result<Vec<TableSchema>> {
    let mut schemas = list_schemas(db)?;
    schemas.extend(list_views(db)?.into_iter().map(|view| {
        let columns = catalog_view_columns(&view);
        TableSchema {
            name: view.name,
            schema_name: "public".to_string(),
            row_type_oid: None,
            row_array_type_oid: None,
            columns,
            primary_key_name: None,
            indexes: view.indexes,
            constraints: Vec::new(),
            rls_enabled: false,
            rls_forced: false,
            policies: Vec::new(),
            owner: None,
            partitioning: None,
            partition_of: None,
        }
    }));
    Ok(schemas)
}

pub(crate) fn primary_key_column(schemas: &[TableSchema], relation: &str) -> Option<String> {
    primary_key_columns(schemas, relation).into_iter().next()
}

pub(crate) fn primary_key_columns(schemas: &[TableSchema], relation: &str) -> Vec<String> {
    if graph_virtual_table_names().contains(&relation) {
        return Vec::new();
    }
    schemas
        .iter()
        .find(|schema| schema.name.eq_ignore_ascii_case(relation))
        .map(primary_key_columns_for_schema)
        .unwrap_or_default()
}

pub(crate) fn primary_key_constraint_name(schemas: &[TableSchema], relation: &str) -> String {
    schemas
        .iter()
        .find(|schema| schema.name.eq_ignore_ascii_case(relation))
        .map(TableSchema::primary_key_constraint_name)
        .unwrap_or_else(|| default_primary_key_name(relation))
}

pub(crate) fn schema_name_for_relation(schemas: &[TableSchema], relation: &str) -> String {
    schemas
        .iter()
        .find(|schema| schema.name.eq_ignore_ascii_case(relation))
        .map(|schema| schema.schema_name.clone())
        .unwrap_or_else(|| "public".to_string())
}

pub(crate) fn primary_key_columns_for_schema(schema: &TableSchema) -> Vec<String> {
    let primary_key_name = schema.primary_key_constraint_name();
    if let Some(columns) = schema.constraints.iter().find_map(|constraint| {
        if let ConstraintSchema::Unique { name, columns, .. } = constraint {
            if name.eq_ignore_ascii_case(&primary_key_name) {
                return Some(columns.clone());
            }
        }
        None
    }) {
        return columns;
    }
    let marked = schema
        .columns
        .iter()
        .filter(|column| column.primary_key && !column.hidden)
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    if !marked.is_empty() {
        return marked;
    }
    Vec::new()
}

pub(crate) fn primary_key_column_for_field(
    schema: &TableSchema,
    table: &str,
    alias: &str,
    field: &FieldRef,
) -> Option<String> {
    let name = match field {
        FieldRef::PrimaryKey { name, .. }
        | FieldRef::Column(name)
        | FieldRef::TypedColumn { name, .. } => name,
        FieldRef::MetadataPath(path) if path.len() == 1 => &path[0],
        FieldRef::MetadataPath(path) => {
            let (column, qualifier) = path.split_last()?;
            let qualifier = qualifier.join(".");
            let qualifier_relation = qualifier.rsplit('.').next().unwrap_or(&qualifier);
            let table_relation = table.rsplit('.').next().unwrap_or(table);
            if !qualifier_relation.eq_ignore_ascii_case(alias)
                && !qualifier_relation.eq_ignore_ascii_case(table)
                && !qualifier_relation.eq_ignore_ascii_case(table_relation)
            {
                return None;
            }
            column
        }
        _ => return None,
    };
    schema
        .column(name)
        .filter(|column| column.primary_key && !column.hidden)
        .map(|column| column.name.clone())
}

pub(crate) fn primary_key_values_from_record_id(
    schema: &TableSchema,
    primary_key_columns: &[String],
    record_id: &str,
) -> Result<Option<Vec<SqlValue>>> {
    let cells = if primary_key_columns.len() == 1 {
        vec![record_id.to_string()]
    } else {
        match serde_json::from_str::<Vec<String>>(record_id) {
            Ok(cells) => cells,
            Err(_) => return Ok(None),
        }
    };
    if cells.len() != primary_key_columns.len() {
        return Ok(None);
    }
    let values = primary_key_columns
        .iter()
        .zip(cells)
        .map(|(column, cell)| {
            let column_schema = schema
                .column(column)
                .ok_or_else(|| SqlError::UndefinedColumn {
                    table: schema.name.clone(),
                    column: column.clone(),
                })?;
            cast_value_to_pg_type(SqlValue::String(cell), &column_schema.pg_type)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(values))
}

pub(crate) fn primary_key_values_match_bindings(
    values: &[SqlValue],
    bindings: &[Option<SqlValue>],
) -> bool {
    values.len() == bindings.len()
        && values.iter().zip(bindings).all(|(value, binding)| {
            binding
                .as_ref()
                .is_none_or(|bound| values_equal(value, bound))
        })
}

pub(crate) fn primary_key_values_match_prefix(values: &[SqlValue], prefix: &[SqlValue]) -> bool {
    values.len() >= prefix.len()
        && values
            .iter()
            .zip(prefix)
            .all(|(value, bound)| values_equal(value, bound))
}

pub(crate) fn primary_key_value_outside_range(
    value: &SqlValue,
    lower: Option<&SqlValue>,
    upper: Option<&SqlValue>,
) -> bool {
    if matches!(value, SqlValue::Null) {
        return true;
    }
    if let Some(lower) = lower {
        if !matches!(
            value_ordering(value, lower),
            Some(Ordering::Equal | Ordering::Greater)
        ) {
            return true;
        }
    }
    if let Some(upper) = upper {
        if !matches!(
            value_ordering(value, upper),
            Some(Ordering::Equal | Ordering::Less)
        ) {
            return true;
        }
    }
    false
}

pub(crate) fn unique_constraint_is_primary_key(
    schema: &TableSchema,
    name: &str,
    columns: &[String],
) -> bool {
    name.eq_ignore_ascii_case(&schema.primary_key_constraint_name())
        && primary_key_columns_for_schema(schema)
            .iter()
            .map(|column| column.to_ascii_lowercase())
            .eq(columns.iter().map(|column| column.to_ascii_lowercase()))
}

pub(crate) fn primary_key_indexes(
    schemas: &[TableSchema],
    db: &BicDb,
) -> Vec<(String, Vec<String>)> {
    user_collection_names(db)
        .into_iter()
        .filter_map(|table| {
            let columns = primary_key_columns(schemas, &table);
            (!columns.is_empty()).then_some((table, columns))
        })
        .collect()
}

pub(crate) fn index_expression_for_columns(columns: &[String]) -> String {
    columns.join(", ")
}

pub(crate) struct CatalogIndex {
    pub(crate) name: String,
    pub(crate) collection: String,
    pub(crate) schema_name: String,
    pub(crate) unique: bool,
    pub(crate) access_method: String,
    pub(crate) expression: String,
    pub(crate) relnatts: i64,
    pub(crate) indkey: Vec<i64>,
    pub(crate) collations: Vec<i64>,
    pub(crate) operator_classes: Vec<String>,
    pub(crate) indexprs: Option<String>,
    pub(crate) exclusion: bool,
    pub(crate) predicate: Option<String>,
}

pub(crate) fn index_operator_class_oids(
    access_method: &str,
    operator_classes: &[String],
) -> Vec<i64> {
    let method_oid = access_method_oid(access_method);
    operator_classes
        .iter()
        .filter_map(|name| {
            let name = name.rsplit('.').next().unwrap_or(name);
            pg_opclass_specs()
                .iter()
                .find(|spec| spec.method_oid == method_oid && spec.name == name)
                .map(|spec| spec.oid)
        })
        .collect()
}

pub(crate) fn default_index_opclass_oids(
    db: &BicDb,
    schemas: &[TableSchema],
    table: &str,
    columns: &[String],
    access_method: &str,
) -> Result<Vec<i64>> {
    let Some(schema) = schemas
        .iter()
        .find(|schema| schema.name.eq_ignore_ascii_case(table))
    else {
        return Ok(Vec::new());
    };
    columns
        .iter()
        .filter_map(|name| schema.column(name))
        .map(|column| {
            pg_default_opclass_for_type(db, access_method, &column.pg_type)?.map_or_else(
                || {
                    Err(SqlError::undefined_object(format!(
                        "data type {} has no default operator class for access method \"{access_method}\"",
                        column.pg_type
                    )))
                },
                |spec| Ok(spec.oid),
            )
        })
        .collect()
}

pub(crate) fn catalog_indexes(schemas: &[TableSchema], db: &BicDb) -> Vec<CatalogIndex> {
    let executable = db
        .index_definitions()
        .into_iter()
        .map(|definition| (definition.name.to_ascii_lowercase(), definition))
        .collect::<BTreeMap<_, _>>();
    let mut indexes = Vec::new();
    for schema in schemas {
        for index in &schema.indexes {
            if !index.internal_index_names.is_empty() {
                if !index
                    .internal_index_names
                    .iter()
                    .all(|name| executable.contains_key(&name.to_ascii_lowercase()))
                {
                    continue;
                }
                let indkey = index
                    .source_expressions
                    .iter()
                    .map(|expression| {
                        attnum_for_column(schemas, &schema.name, expression.trim_matches('"'))
                            .unwrap_or(0)
                    })
                    .collect::<Vec<_>>();
                indexes.push(CatalogIndex {
                    name: index.name.clone(),
                    collection: schema.name.clone(),
                    schema_name: schema.schema_name.clone(),
                    unique: index.unique,
                    access_method: index.access_method.clone(),
                    expression: index.expression.clone(),
                    relnatts: indkey.len() as i64,
                    indexprs: indkey.contains(&0).then(|| index.expression.clone()),
                    indkey,
                    collations: index.collations.clone(),
                    operator_classes: index.operator_classes.clone(),
                    exclusion: false,
                    predicate: None,
                });
                continue;
            }
            if index.metadata_only {
                indexes.push(CatalogIndex {
                    name: index.name.clone(),
                    collection: schema.name.clone(),
                    schema_name: schema.schema_name.clone(),
                    unique: index.unique,
                    access_method: index.access_method.clone(),
                    expression: index.expression.clone(),
                    relnatts: 1,
                    indkey: vec![0],
                    collations: index.collations.clone(),
                    operator_classes: index.operator_classes.clone(),
                    indexprs: Some(index.expression.clone()),
                    exclusion: false,
                    predicate: None,
                });
                continue;
            }
            let Some(definition) = executable.get(&index.name.to_ascii_lowercase()) else {
                continue;
            };
            let full_text = definition.kind == IndexKind::FullText;
            indexes.push(CatalogIndex {
                name: index.name.clone(),
                collection: definition.collection.clone(),
                schema_name: schema.schema_name.clone(),
                unique: definition.unique,
                access_method: index.access_method.clone(),
                expression: index.expression.clone(),
                relnatts: if full_text {
                    1
                } else {
                    definition.fields.len() as i64
                },
                indkey: if full_text {
                    vec![0]
                } else {
                    definition
                        .fields
                        .iter()
                        .map(|field| {
                            index_field_attnum(schemas, &definition.collection, field).unwrap_or(0)
                        })
                        .collect()
                },
                collations: index.collations.clone(),
                operator_classes: index.operator_classes.clone(),
                indexprs: (full_text || definition.fields.iter().any(index_field_is_expression))
                    .then(|| index.expression.clone()),
                exclusion: false,
                predicate: None,
            });
        }
        for constraint in &schema.constraints {
            let ConstraintSchema::Exclusion {
                name,
                access_method,
                equal_columns,
                range,
                predicate,
                ..
            } = constraint
            else {
                continue;
            };
            let columns = exclusion_constraint_columns(equal_columns, range);
            indexes.push(CatalogIndex {
                name: name.clone(),
                collection: schema.name.clone(),
                schema_name: schema.schema_name.clone(),
                unique: false,
                access_method: access_method.clone(),
                expression: exclusion_index_expression(equal_columns, range),
                relnatts: columns.len() as i64,
                indkey: constraint_attnums(schemas, &schema.name, &columns),
                collations: constraint_collation_oids(schemas, &schema.name, &columns),
                operator_classes: Vec::new(),
                indexprs: range.as_ref().and_then(|range| {
                    range
                        .range_column
                        .is_none()
                        .then(|| exclusion_range_expression(range))
                }),
                exclusion: true,
                predicate: predicate.clone(),
            });
        }
    }
    indexes.sort_by(|left, right| left.name.cmp(&right.name));
    indexes
}

pub(crate) fn exclusion_index_expression(
    equal_columns: &[String],
    range: &Option<ExclusionRangeSchema>,
) -> String {
    equal_columns
        .iter()
        .cloned()
        .chain(range.iter().map(exclusion_range_expression))
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) fn exclusion_range_expression(range: &ExclusionRangeSchema) -> String {
    range.range_column.clone().unwrap_or_else(|| {
        format!(
            "{}({}, {}, '{}')",
            range.function,
            range.start_column,
            range.end_column,
            range.bounds.replace('\'', "''")
        )
    })
}

pub(crate) fn check_constraint_count(schema: &TableSchema) -> i64 {
    schema
        .constraints
        .iter()
        .filter(|constraint| matches!(constraint, ConstraintSchema::Check { .. }))
        .count() as i64
}

thread_local! {
    // db -> ((schema gen, view gen, collection count), table -> oid). Column
    // metadata for every SELECT / RETURNING result asked for this map, which
    // listed and cloned every schema and every view each time.
    static TABLE_OIDS_MEMO: std::cell::RefCell<rustc_hash::FxHashMap<usize, ((u64, u64, usize), std::sync::Arc<BTreeMap<String, i64>>)>> =
        std::cell::RefCell::new(rustc_hash::FxHashMap::default());
}

pub(crate) fn table_oids(db: &BicDb) -> std::sync::Arc<BTreeMap<String, i64>> {
    let key = db as *const BicDb as usize;
    let stamp = (
        db.collection_generation(SCHEMA_COLLECTION),
        db.collection_generation(VIEW_COLLECTION),
        db.collection_count(),
    );
    let hit = TABLE_OIDS_MEMO.with(|memo| {
        memo.borrow()
            .get(&key)
            .filter(|(cached, _)| *cached == stamp)
            .map(|(_, oids)| std::sync::Arc::clone(oids))
    });
    if let Some(hit) = hit {
        return hit;
    }
    let schemas = list_schemas_shared(db).unwrap_or_default();
    let by_name = schemas
        .iter()
        .map(|schema| (schema.name.as_str(), schema))
        .collect::<BTreeMap<_, _>>();
    let oids = std::sync::Arc::new(
        catalog_table_names(db)
            .into_iter()
            .map(|table| {
                let oid = by_name
                    .get(table.as_str())
                    .map(|schema| table_relation_oid(schema))
                    .unwrap_or_else(|| named_relation_oid(&table));
                (table, oid)
            })
            .collect::<BTreeMap<String, i64>>(),
    );
    TABLE_OIDS_MEMO.with(|memo| {
        memo.borrow_mut()
            .insert(key, (stamp, std::sync::Arc::clone(&oids)));
    });
    oids
}

pub(crate) fn attnum_for_column(
    schemas: &[TableSchema],
    relation: &str,
    column: &str,
) -> Option<i64> {
    columns_for_relation(schemas, relation)
        .into_iter()
        .position(|candidate| candidate.name.eq_ignore_ascii_case(column))
        .map(|idx| idx as i64 + 1)
}

pub(crate) fn index_field_attnum(
    schemas: &[TableSchema],
    relation: &str,
    field: &IndexField,
) -> Option<i64> {
    match field {
        IndexField::Id => attnum_for_column(schemas, relation, "id"),
        IndexField::Timestamp => attnum_for_column(schemas, relation, "timestamp"),
        IndexField::Geometry => attnum_for_column(schemas, relation, "geometry"),
        IndexField::MetadataPath(path) if path.len() == 1 => {
            attnum_for_column(schemas, relation, &path[0])
        }
        IndexField::MetadataPath(_) | IndexField::Lower(_) | IndexField::Trim(_) => None,
    }
}
