//! DDL/ALTER TABLE helpers: rename/undo application, constraint add/drop, column defaults from data types, identifier utilities, raw partition DDL parsing, string literal helpers, and pg type mapping from SQL data types.
//!
//! Extracted verbatim from `lib.rs` (phase-1 mechanical module split).
//! Moved items were bumped to `pub(crate)` so existing call sites keep
//! resolving; `lib.rs` re-exports this module via `pub use ddl_alter::*;`.

// Glue import: bring every crate-root item (including the root's private
// imports and the other split modules' re-exports) into scope so the moved
// code compiles unchanged.
use crate::*;

pub(crate) fn apply_rename_table_undo(db: &mut BicDb, undo: DdlUndo) -> Result<()> {
    let DdlUndo::RestoreRenamedTable {
        old_table,
        new_table,
        records,
        indexes,
        schema,
        sequences,
    } = undo
    else {
        return Ok(());
    };
    restore_renamed_table(
        db, &old_table, &new_table, records, indexes, schema, sequences,
    )
}

pub(crate) fn restore_renamed_table(
    db: &mut BicDb,
    old_table: &str,
    new_table: &str,
    records: Vec<Record>,
    indexes: Vec<IndexDefinition>,
    schema: TableSchema,
    sequences: Vec<SequenceSchema>,
) -> Result<()> {
    let _ = db.drop_collection(new_table)?;
    delete_schema(db, new_table)?;

    let old_exists = db
        .collections()
        .iter()
        .any(|collection| collection.name.eq_ignore_ascii_case(old_table));
    if !old_exists {
        db.create_collection(old_table)?;
        if !records.is_empty() {
            db.batch_insert(old_table, records)?;
        }
        for index in indexes {
            db.create_index(index)?;
        }
    }

    save_schema(db, &schema)?;
    for sequence in sequences {
        save_sequence(db, &sequence)?;
    }
    Ok(())
}

pub(crate) fn alter_table_alter_column(
    db: &mut BicDb,
    table: &str,
    schema: &mut TableSchema,
    column: &str,
    op: &AlterColumnOperation,
) -> Result<()> {
    let Some(idx) = schema
        .columns
        .iter()
        .position(|candidate| candidate.name.eq_ignore_ascii_case(column))
    else {
        return Err(SqlError::InvalidSql(format!(
            "column \"{column}\" of relation \"{table}\" does not exist"
        )));
    };
    match op {
        AlterColumnOperation::SetDataType {
            data_type, using, ..
        } => {
            if using
                .as_ref()
                .is_some_and(|expr| !alter_column_using_is_self_cast(expr, column))
            {
                return Err(SqlError::Unsupported(
                    "ALTER COLUMN TYPE USING expressions are not supported".to_string(),
                ));
            }
            let (pg_type, vector_dim) = pg_type_from_data_type(data_type)?;
            let type_modifier = pg_type_modifier_from_data_type(data_type)?;
            let column_name = schema.columns[idx].name.clone();
            schema.columns[idx].pg_type = pg_type;
            schema.columns[idx].type_modifier = type_modifier;
            schema.columns[idx].vector_dim = vector_dim;
            for mut record in db.scan_collection(table)? {
                let current = record_column_value(&record, schema, &column_name);
                if !matches!(current, SqlValue::Null) {
                    let converted = if using.is_some() {
                        cast_value(current, data_type)?
                    } else {
                        cast_value_to_column_type(current, &schema.columns[idx])?
                    };
                    set_record_column(&mut record, Some(schema), &column_name, converted)?;
                    db.insert(table, record)?;
                }
            }
            save_schema(db, schema)
        }
        AlterColumnOperation::SetNotNull => {
            for record in db.scan_collection(table)? {
                if matches!(record_column_value(&record, schema, column), SqlValue::Null) {
                    return Err(constraint_violation(
                        "23502",
                        format!("column \"{column}\" of relation \"{table}\" contains null values"),
                        Some(table.to_string()),
                        Some(column.to_string()),
                        Some(not_null_constraint_name(table, column)),
                    ));
                }
            }
            schema.columns[idx].nullable = false;
            save_schema(db, schema)
        }
        AlterColumnOperation::DropNotNull => {
            if schema.columns[idx].primary_key {
                return Err(SqlError::Unsupported(
                    "ALTER COLUMN DROP NOT NULL for primary key columns is not supported"
                        .to_string(),
                ));
            }
            schema.columns[idx].nullable = true;
            save_schema(db, schema)
        }
        AlterColumnOperation::SetDefault { value } => {
            if schema.columns[idx].identity.is_some() {
                return Err(SqlError::InvalidSql(format!(
                    "column \"{column}\" of relation \"{table}\" is an identity column"
                )));
            }
            if let Some(sequence) = default_sequence_from_expr(value)? {
                load_sequence_required(db, &sequence)?;
                schema.columns[idx].default_sequence = Some(sequence);
                schema.columns[idx].default_value = None;
                schema.columns[idx].default_expr = None;
            } else {
                schema.columns[idx].default_sequence = None;
                let pg_type = schema.columns[idx].pg_type.clone();
                schema.columns[idx].default_value =
                    literal_default_value_from_expr_with_db(db, value, &pg_type)?;
                schema.columns[idx].default_expr = Some(value.to_string());
            }
            save_schema(db, schema)
        }
        AlterColumnOperation::DropDefault => {
            if schema.columns[idx].identity.is_some() {
                return Err(SqlError::InvalidSql(format!(
                    "column \"{column}\" of relation \"{table}\" is an identity column"
                )));
            }
            schema.columns[idx].default_sequence = None;
            schema.columns[idx].default_value = None;
            schema.columns[idx].default_expr = None;
            save_schema(db, schema)
        }
        AlterColumnOperation::AddGenerated { .. } => Err(SqlError::Unsupported(format!(
            "ALTER COLUMN {op} is not supported"
        ))),
    }
}

pub(crate) fn alter_column_using_is_self_cast(expr: &Expr, column: &str) -> bool {
    match unwrap_nested_expr(expr) {
        Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case(column),
        Expr::CompoundIdentifier(idents) => idents
            .last()
            .is_some_and(|ident| ident.value.eq_ignore_ascii_case(column)),
        Expr::Cast { expr, .. } | Expr::Collate { expr, .. } => {
            alter_column_using_is_self_cast(expr, column)
        }
        _ => false,
    }
}

pub(crate) fn sync_partition_children_from_parent(
    db: &mut BicDb,
    parent: &TableSchema,
) -> Result<()> {
    if parent.partitioning.is_none() {
        return Ok(());
    }
    for child in list_schemas_raw(db)? {
        let Some(partition_of) = child.partition_of.as_ref() else {
            continue;
        };
        if !partition_of.parent_table.eq_ignore_ascii_case(&parent.name) {
            continue;
        }
        let synced = schema_with_parent_inheritance(child, parent);
        save_schema(db, &synced)?;
    }
    Ok(())
}

pub(crate) fn alter_table_add_constraint(
    db: &mut BicDb,
    table: &str,
    schema: &mut TableSchema,
    constraint: &TableConstraint,
    not_valid: bool,
) -> Result<Option<(String, String)>> {
    if let TableConstraint::PrimaryKey(primary_key) = constraint {
        reject_unsupported_constraint_characteristics(primary_key.characteristics.as_ref())?;
        let columns = simple_index_column_names(&primary_key.columns)?;
        let existing_columns = primary_key_columns_for_schema(schema);
        if !existing_columns.is_empty() {
            if schema.primary_key_name.is_none() && existing_columns == columns {
                let name = primary_key
                    .name
                    .as_ref()
                    .map(ident_value)
                    .unwrap_or_else(|| default_primary_key_name(table));
                schema.primary_key_name = Some(name);
                return Ok(None);
            }
            return Err(SqlError::InvalidSql(format!(
                "multiple primary keys for table \"{table}\" are not allowed"
            )));
        }
        reject_missing_default_operator_class(db, &schema.columns, &columns, "btree")?;
        let name = primary_key
            .name
            .as_ref()
            .map(ident_value)
            .unwrap_or_else(|| default_primary_key_name(table));
        apply_primary_key_constraint(db, table, schema, name, columns, not_valid)?;
        return Ok(None);
    }

    if let TableConstraint::PrimaryKeyUsingIndex(primary_key) = constraint {
        reject_unsupported_constraint_characteristics(primary_key.characteristics.as_ref())?;
        if not_valid {
            return Err(SqlError::Unsupported(
                "PRIMARY KEY USING INDEX cannot be marked NOT VALID".to_string(),
            ));
        }
        if !primary_key_columns_for_schema(schema).is_empty() {
            return Err(SqlError::InvalidSql(format!(
                "multiple primary keys for table \"{table}\" are not allowed"
            )));
        }
        let index_name = ident_value(&primary_key.index_name);
        let constraint_name = primary_key
            .name
            .as_ref()
            .map(ident_value)
            .unwrap_or_else(|| index_name.clone());
        let index_schema = schema
            .indexes
            .iter()
            .find(|index| index.name == index_name)
            .cloned()
            .ok_or_else(|| {
                SqlError::InvalidSql(format!(
                    "index \"{index_name}\" for relation \"{table}\" does not exist"
                ))
            })?;
        if !index_schema.unique {
            return Err(SqlError::InvalidSql(format!(
                "index \"{index_name}\" is not unique"
            )));
        }
        if index_schema.metadata_only {
            return Err(SqlError::Unsupported(format!(
                "metadata-only index \"{index_name}\" cannot be used for a primary key"
            )));
        }
        if !index_schema.access_method.eq_ignore_ascii_case("btree") {
            return Err(SqlError::Unsupported(format!(
                "primary key index \"{index_name}\" must use btree"
            )));
        }
        if constraint_name != index_name
            && schema
                .indexes
                .iter()
                .any(|index| index.name == constraint_name && index.name != index_name)
        {
            return Err(SqlError::InvalidSql(format!(
                "index \"{constraint_name}\" already exists"
            )));
        }
        if table_constraint_name_exists(schema, &constraint_name) {
            return Err(SqlError::duplicate_constraint(table, &constraint_name));
        }

        let executable = db
            .index_definitions()
            .into_iter()
            .find(|definition| {
                definition.collection.eq_ignore_ascii_case(table)
                    && definition.name.eq_ignore_ascii_case(&index_name)
            })
            .ok_or_else(|| {
                SqlError::InvalidSql(format!(
                    "index \"{index_name}\" for relation \"{table}\" does not have executable index storage"
                ))
            })?;
        if !executable.unique {
            return Err(SqlError::InvalidSql(format!(
                "index \"{index_name}\" is not unique"
            )));
        }
        if executable.kind != IndexKind::BTree {
            return Err(SqlError::Unsupported(format!(
                "primary key index \"{index_name}\" must use btree"
            )));
        }
        let columns = executable
            .fields
            .iter()
            .map(|field| primary_key_column_from_index_field(schema, field))
            .collect::<Result<Vec<_>>>()?;

        let rewrite_promoted_index = columns.len() > 1;
        let promoted_index_fields =
            rewrite_promoted_index.then(|| primary_key_index_fields_for_columns(&columns));
        apply_primary_key_constraint(db, table, schema, constraint_name.clone(), columns, false)?;
        if let Some(fields) = promoted_index_fields {
            rewrite_records_for_primary_key(db, table, schema)?;
            db.drop_index(&executable.name)?;
            db.create_index(IndexDefinition {
                name: constraint_name.clone(),
                collection: table.to_string(),
                fields,
                unique: true,
                kind: IndexKind::BTree,
                predicate: None,
                exclusion: None,
            })?;
        }
        schema.indexes.retain(|index| index.name != index_name);
        if rewrite_promoted_index {
            return Ok(None);
        }
        return Ok(
            (executable.name != constraint_name).then_some((executable.name, constraint_name))
        );
    }

    let mut constraints = table_constraint_schema(table, constraint)?;
    if constraints.is_empty() {
        return Err(SqlError::Unsupported(
            "ALTER TABLE ADD PRIMARY KEY is not supported".to_string(),
        ));
    }
    for constraint in &mut constraints {
        set_constraint_validated(constraint, !not_valid);
    }
    for constraint in &constraints {
        if let ConstraintSchema::Unique { columns, .. } = constraint {
            reject_missing_default_operator_class(db, &schema.columns, columns, "btree")?;
        }
        validate_constraint_columns(db, table, schema, constraint)?;
        let name = constraint_name(constraint);
        if schema
            .constraints
            .iter()
            .any(|candidate| constraint_name(candidate) == name)
        {
            return Err(SqlError::duplicate_constraint(table, name));
        }
        if !not_valid {
            validate_existing_constraint(db, table, schema, constraint)?;
        }
    }
    schema.constraints.extend(constraints);
    Ok(None)
}

pub(crate) fn reject_missing_default_operator_class(
    db: &BicDb,
    schema_columns: &[ColumnSchema],
    indexed_columns: &[String],
    access_method: &str,
) -> Result<()> {
    for name in indexed_columns {
        let Some(column) = schema_columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(name))
        else {
            continue;
        };
        if pg_default_opclass_for_type(db, access_method, &column.pg_type)?.is_none() {
            return Err(SqlError::undefined_object(format!(
                "data type {} has no default operator class for access method \"{access_method}\"",
                column.pg_type
            )));
        }
    }
    Ok(())
}

pub(crate) fn primary_key_index_fields_for_columns(columns: &[String]) -> Vec<IndexField> {
    columns
        .iter()
        .map(|column| IndexField::MetadataPath(vec![column.clone()]))
        .collect()
}

pub(crate) fn rewrite_records_for_primary_key(
    db: &mut BicDb,
    table: &str,
    schema: &TableSchema,
) -> Result<()> {
    let records = db.scan_collection(table)?;
    if records.is_empty() {
        return Ok(());
    }

    let mut rewritten = Vec::with_capacity(records.len());
    let mut old_ids = BTreeSet::new();
    let mut unchanged_ids = BTreeSet::new();
    let mut new_ids = BTreeSet::new();
    for record in records {
        let old_id = record.id.clone();
        old_ids.insert(old_id.clone());
        let fields = record_fields_for_schema(&record, schema);
        let new_record = record_from_fields(table, Some(schema), fields)?;
        if !new_ids.insert(new_record.id.clone()) {
            return Err(unique_violation(&schema.primary_key_constraint_name()));
        }
        if new_record.id == old_id {
            unchanged_ids.insert(old_id.clone());
        }
        rewritten.push((old_id, new_record));
    }

    for (old_id, new_record) in &rewritten {
        if old_id != &new_record.id && unchanged_ids.contains(&new_record.id) {
            return Err(SqlError::InvalidSql(format!(
                "primary key rewrite for relation \"{table}\" would overwrite existing record id \"{}\"",
                new_record.id
            )));
        }
    }

    for old_id in old_ids {
        db.delete(table, &old_id)?;
    }
    db.batch_insert(
        table,
        rewritten
            .into_iter()
            .map(|(_, record)| record)
            .collect::<Vec<_>>(),
    )?;
    Ok(())
}

pub(crate) fn apply_primary_key_constraint(
    db: &BicDb,
    table: &str,
    schema: &mut TableSchema,
    name: String,
    columns: Vec<String>,
    not_valid: bool,
) -> Result<()> {
    let primary_constraint = ConstraintSchema::Unique {
        name: name.clone(),
        columns: columns.clone(),
        validated: !not_valid,
    };
    validate_constraint_columns(db, table, schema, &primary_constraint)?;
    if !not_valid {
        validate_existing_primary_key(db, table, schema, &name, &columns)?;
    }
    schema.columns.retain(|column| {
        !(column.hidden && column.primary_key && column.name == SYNTHETIC_PRIMARY_KEY)
    });
    for column_name in &columns {
        let Some(column) = schema
            .columns
            .iter_mut()
            .find(|column| column.name == *column_name)
        else {
            return Err(SqlError::InvalidSql(format!(
                "column \"{column_name}\" referenced by constraint on relation \"{table}\" does not exist"
            )));
        };
        column.primary_key = true;
        column.nullable = false;
    }
    schema.primary_key_name = Some(name);
    Ok(())
}

pub(crate) fn primary_key_column_from_index_field(
    schema: &TableSchema,
    field: &IndexField,
) -> Result<String> {
    let column_name = match field {
        IndexField::Id => "id",
        IndexField::MetadataPath(path) if path.len() == 1 => &path[0],
        _ => {
            return Err(SqlError::Unsupported(
                "primary key using index requires a simple column index".to_string(),
            ))
        }
    };
    let Some(column) = schema.column(column_name).filter(|column| !column.hidden) else {
        return Err(SqlError::InvalidSql(format!(
            "index field \"{column_name}\" does not reference a visible column"
        )));
    };
    Ok(column.name.clone())
}

pub(crate) fn alter_table_validate_constraint(
    db: &BicDb,
    table: &str,
    schema: &mut TableSchema,
    validated_constraint: &str,
) -> Result<()> {
    let Some(idx) = schema
        .constraints
        .iter()
        .position(|constraint| constraint_name(constraint) == validated_constraint)
    else {
        return Err(SqlError::InvalidSql(format!(
            "constraint \"{validated_constraint}\" of relation \"{table}\" does not exist"
        )));
    };
    validate_existing_constraint(db, table, schema, &schema.constraints[idx])?;
    set_constraint_validated(&mut schema.constraints[idx], true);
    Ok(())
}

pub(crate) fn set_constraint_validated(constraint: &mut ConstraintSchema, validated: bool) {
    match constraint {
        ConstraintSchema::Unique {
            validated: value, ..
        }
        | ConstraintSchema::Check {
            validated: value, ..
        }
        | ConstraintSchema::ForeignKey {
            validated: value, ..
        }
        | ConstraintSchema::Exclusion {
            validated: value, ..
        } => *value = validated,
    }
}

pub(crate) fn alter_table_drop_constraint(
    table: &str,
    schema: &mut TableSchema,
    dropped_constraint: &str,
    if_exists: bool,
) -> Result<Option<String>> {
    let dropped_primary_key = !primary_key_columns_for_schema(schema).is_empty()
        && schema.primary_key_constraint_name() == dropped_constraint;
    let dropped_unique = schema.constraints.iter().any(|constraint| {
        matches!(
            constraint,
            ConstraintSchema::Unique { name, columns, .. }
                if name == dropped_constraint
                    && !unique_constraint_is_primary_key(schema, name, columns)
        )
    });
    let original_len = schema.constraints.len();
    schema
        .constraints
        .retain(|constraint| constraint_name(constraint) != dropped_constraint);
    let dropped_metadata_constraint = schema.constraints.len() != original_len;
    if dropped_primary_key {
        for column in &mut schema.columns {
            column.primary_key = false;
        }
        schema.primary_key_name = None;
        return Ok(Some(dropped_constraint.to_string()));
    }
    if !dropped_metadata_constraint && !if_exists {
        return Err(SqlError::InvalidSql(format!(
            "constraint \"{dropped_constraint}\" of relation \"{table}\" does not exist"
        )));
    }
    Ok(dropped_unique.then(|| dropped_constraint.to_string()))
}

pub(crate) fn alter_table_rename_constraint(
    table: &str,
    schema: &mut TableSchema,
    old_name: &str,
    new_name: &str,
) -> Result<Option<(String, String)>> {
    if old_name == new_name && table_constraint_name_exists(schema, old_name) {
        return Ok(None);
    }
    if table_constraint_name_exists(schema, new_name) {
        return Err(SqlError::duplicate_constraint(table, new_name));
    }
    if !primary_key_columns_for_schema(schema).is_empty()
        && schema.primary_key_constraint_name() == old_name
    {
        schema.primary_key_name = Some(new_name.to_string());
        return Ok(Some((old_name.to_string(), new_name.to_string())));
    }
    let renames_unique_index = schema.constraints.iter().any(|constraint| {
        matches!(
            constraint,
            ConstraintSchema::Unique { name, columns, .. }
                if name == old_name && !unique_constraint_is_primary_key(schema, name, columns)
        )
    });
    if let Some(constraint) = schema
        .constraints
        .iter_mut()
        .find(|constraint| constraint_name(constraint) == old_name)
    {
        set_constraint_name(constraint, new_name.to_string());
        return Ok(renames_unique_index.then(|| (old_name.to_string(), new_name.to_string())));
    }
    Err(SqlError::InvalidSql(format!(
        "constraint \"{old_name}\" of relation \"{table}\" does not exist"
    )))
}

pub(crate) fn table_constraint_name_exists(schema: &TableSchema, name: &str) -> bool {
    (!primary_key_columns_for_schema(schema).is_empty()
        && schema.primary_key_constraint_name() == name)
        || schema
            .constraints
            .iter()
            .any(|constraint| constraint_name(constraint) == name)
}

pub(crate) fn validate_constraint_columns(
    db: &BicDb,
    table: &str,
    schema: &TableSchema,
    constraint: &ConstraintSchema,
) -> Result<()> {
    let columns = match constraint {
        ConstraintSchema::Unique { columns, .. } | ConstraintSchema::ForeignKey { columns, .. } => {
            columns
        }
        ConstraintSchema::Exclusion {
            access_method,
            equal_columns,
            range,
            ..
        } => {
            let mut columns = equal_columns.iter().collect::<Vec<_>>();
            if let Some(range) = range {
                if let Some(range_column) = range.range_column.as_ref() {
                    columns.push(range_column);
                } else {
                    columns.push(&range.start_column);
                    columns.push(&range.end_column);
                }
            }
            for column in columns {
                if schema.column(column).is_none() {
                    return Err(SqlError::InvalidSql(format!(
                        "column \"{column}\" referenced by constraint on relation \"{table}\" does not exist"
                    )));
                }
            }
            if access_method == "spgist" && equal_columns.len() + usize::from(range.is_some()) > 1 {
                return Err(SqlError::Unsupported(
                    "access method \"spgist\" does not support multicolumn indexes".to_string(),
                ));
            }
            if let Some(range_column) = range.as_ref().and_then(|range| range.range_column.as_ref())
            {
                let pg_type = &schema
                    .column(range_column)
                    .expect("exclusion column was validated")
                    .pg_type;
                if !is_builtin_range_type(pg_type) && !is_builtin_multirange_type(pg_type) {
                    return Err(SqlError::undefined_object(format!(
                        "data type {pg_type} has no default operator class for access method \"{access_method}\""
                    )));
                }
                if access_method == "spgist" && is_builtin_multirange_type(pg_type) {
                    return Err(SqlError::undefined_object(format!(
                        "data type {pg_type} has no default operator class for access method \"spgist\""
                    )));
                }
            }
            return Ok(());
        }
        ConstraintSchema::Check { .. } => return Ok(()),
    };
    for column in columns {
        if schema.column(column).is_none() {
            return Err(SqlError::InvalidSql(format!(
                "column \"{column}\" referenced by constraint on relation \"{table}\" does not exist"
            )));
        }
    }
    if let ConstraintSchema::ForeignKey {
        foreign_table,
        referred_columns,
        ..
    } = constraint
    {
        let foreign_schema = load_schema(db, foreign_table)?
            .ok_or_else(|| SqlError::InvalidCollection(foreign_table.clone()))?;
        for column in referred_columns {
            if foreign_schema.column(column).is_none() {
                return Err(SqlError::InvalidSql(format!(
                    "column \"{column}\" referenced by constraint on relation \"{foreign_table}\" does not exist"
                )));
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_existing_constraint(
    db: &BicDb,
    table: &str,
    schema: &TableSchema,
    constraint: &ConstraintSchema,
) -> Result<()> {
    match constraint {
        ConstraintSchema::Unique { name, columns, .. } => {
            let records = db.scan_collection(table)?;
            validate_unique_key_records(table, schema, name, columns, &records, false)
        }
        ConstraintSchema::Check {
            name, expression, ..
        } => {
            let expr = parse_check_expression(expression)?;
            for record in db.scan_collection(table)? {
                if matches!(
                    eval_schema_predicate_truth(&record, schema, &expr)?,
                    Some(false)
                ) {
                    return Err(constraint_violation(
                        "23514",
                        format!(
                            "row for relation \"{}\" violates check constraint \"{}\"",
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
        ConstraintSchema::ForeignKey { name, .. } => {
            for record in db.scan_collection(table)? {
                validate_foreign_key_constraint(db, None, table, schema, &record, constraint)
                    .map_err(|_| foreign_key_violation(table, name))?;
            }
            Ok(())
        }
        ConstraintSchema::Exclusion {
            name,
            equal_columns,
            range,
            predicate,
            ..
        } => {
            let predicate = exclusion_predicate_expr(predicate)?;
            let records = db.scan_collection(table)?;
            validate_exclusion_record_pairs(
                table,
                schema,
                name,
                equal_columns,
                range,
                predicate.as_ref(),
                &records,
                false,
            )
        }
    }
}

pub(crate) fn validate_existing_primary_key(
    db: &BicDb,
    table: &str,
    schema: &TableSchema,
    name: &str,
    columns: &[String],
) -> Result<()> {
    let records = db.scan_collection(table)?;
    validate_unique_key_records(table, schema, name, columns, &records, true)
}

pub(crate) fn validate_unique_key_records(
    table: &str,
    schema: &TableSchema,
    name: &str,
    columns: &[String],
    records: &[Record],
    reject_nulls: bool,
) -> Result<()> {
    let mut seen: BTreeMap<String, String> = BTreeMap::new();
    for record in records {
        let key = record_column_values(record, schema, columns);
        if let Some(null_idx) = key.iter().position(|value| matches!(value, SqlValue::Null)) {
            if reject_nulls {
                let column = columns.get(null_idx).cloned();
                return Err(constraint_violation(
                    "23502",
                    format!(
                        "column \"{}\" of relation \"{}\" contains null values",
                        column.as_deref().unwrap_or(""),
                        table
                    ),
                    Some(table.to_string()),
                    column,
                    Some(name.to_string()),
                ));
            }
            continue;
        }
        let key = key
            .iter()
            .map(SqlValue::to_cell)
            .collect::<Vec<_>>()
            .join("\u{1f}");
        if seen.insert(key, record.id.clone()).is_some() {
            return Err(unique_violation(name));
        }
    }
    Ok(())
}

pub fn integrity_check(db: &mut BicDb) -> Result<SqlIntegrityReport> {
    let mut report = SqlIntegrityReport::default();
    let schemas = list_schemas(db)?;
    let user_collections = user_collection_names(db);
    for collection in &user_collections {
        if schemas
            .iter()
            .all(|schema| !schema.name.eq_ignore_ascii_case(collection))
        {
            report.violations.push(SqlIntegrityViolation {
                check: "schema_catalog".to_string(),
                table: Some(collection.clone()),
                object: None,
                record_id: None,
                message: "user collection is missing SQL schema metadata".to_string(),
            });
        }
    }

    for schema in &schemas {
        report.tables_checked += 1;
        if !user_collections
            .iter()
            .any(|collection| collection.eq_ignore_ascii_case(&schema.name))
        {
            report.violations.push(SqlIntegrityViolation {
                check: "schema_catalog".to_string(),
                table: Some(schema.name.clone()),
                object: None,
                record_id: None,
                message: "SQL schema references a missing collection".to_string(),
            });
            continue;
        }

        for column in &schema.columns {
            if column.primary_key && column.nullable {
                report.violations.push(SqlIntegrityViolation {
                    check: "schema_catalog".to_string(),
                    table: Some(schema.name.clone()),
                    object: Some(column.name.clone()),
                    record_id: None,
                    message: "primary key column is marked nullable".to_string(),
                });
            }
        }

        for constraint in &schema.constraints {
            validate_constraint_columns(db, &schema.name, schema, constraint).unwrap_or_else(
                |error| {
                    report.violations.push(SqlIntegrityViolation {
                        check: "schema_catalog".to_string(),
                        table: Some(schema.name.clone()),
                        object: Some(constraint_name(constraint).to_string()),
                        record_id: None,
                        message: error.to_string(),
                    });
                },
            );
        }

        let records = db.scan_collection(&schema.name)?;
        report.records_checked += records.len();
        let primary_key_columns = primary_key_columns_for_schema(schema);
        if !primary_key_columns.is_empty() {
            check_unique_key_integrity(
                &mut report,
                &schema.name,
                &schema.primary_key_constraint_name(),
                schema,
                &primary_key_columns,
                &records,
                true,
            );
        }
        for constraint in &schema.constraints {
            report.constraints_checked += 1;
            match constraint {
                ConstraintSchema::Unique { name, columns, .. } => {
                    check_unique_key_integrity(
                        &mut report,
                        &schema.name,
                        name,
                        schema,
                        columns,
                        &records,
                        false,
                    );
                }
                ConstraintSchema::Check {
                    name, expression, ..
                } => {
                    let expr = match parse_check_expression(expression) {
                        Ok(expr) => expr,
                        Err(error) => {
                            report.violations.push(SqlIntegrityViolation {
                                check: "check_constraint".to_string(),
                                table: Some(schema.name.clone()),
                                object: Some(name.clone()),
                                record_id: None,
                                message: error.to_string(),
                            });
                            continue;
                        }
                    };
                    for record in &records {
                        match eval_schema_predicate_truth(record, schema, &expr) {
                            Ok(Some(false)) => report.violations.push(SqlIntegrityViolation {
                                check: "check_constraint".to_string(),
                                table: Some(schema.name.clone()),
                                object: Some(name.clone()),
                                record_id: Some(record.id.clone()),
                                message: "record violates check constraint".to_string(),
                            }),
                            Err(error) => report.violations.push(SqlIntegrityViolation {
                                check: "check_constraint".to_string(),
                                table: Some(schema.name.clone()),
                                object: Some(name.clone()),
                                record_id: Some(record.id.clone()),
                                message: error.to_string(),
                            }),
                            _ => {}
                        }
                    }
                }
                ConstraintSchema::ForeignKey { name, .. } => {
                    for record in &records {
                        if let Err(error) = validate_foreign_key_constraint(
                            db,
                            None,
                            &schema.name,
                            schema,
                            record,
                            constraint,
                        ) {
                            report.violations.push(SqlIntegrityViolation {
                                check: "foreign_key".to_string(),
                                table: Some(schema.name.clone()),
                                object: Some(name.clone()),
                                record_id: Some(record.id.clone()),
                                message: error.to_string(),
                            });
                        }
                    }
                }
                ConstraintSchema::Exclusion {
                    name,
                    equal_columns,
                    range,
                    predicate,
                    ..
                } => match exclusion_predicate_expr(predicate).and_then(|predicate| {
                    validate_exclusion_record_pairs(
                        &schema.name,
                        schema,
                        name,
                        equal_columns,
                        range,
                        predicate.as_ref(),
                        &records,
                        false,
                    )
                }) {
                    Ok(()) => {}
                    Err(error) => report.violations.push(SqlIntegrityViolation {
                        check: "exclusion_constraint".to_string(),
                        table: Some(schema.name.clone()),
                        object: Some(name.clone()),
                        record_id: None,
                        message: error.to_string(),
                    }),
                },
            }
        }
    }

    let index_report = db.verify_all_indexes()?;
    report.indexes_checked = index_report.reports.len() + index_report.vector_reports.len();
    for entry in index_report.reports {
        if !entry.verification.valid {
            report.violations.push(SqlIntegrityViolation {
                check: "index".to_string(),
                table: Some(entry.collection),
                object: Some(entry.index_name),
                record_id: None,
                message: format!(
                    "index verification failed: stale={} corrupt={}",
                    entry.stale, entry.corrupt
                ),
            });
        }
    }
    for entry in index_report.vector_reports {
        if !entry.valid {
            report.violations.push(SqlIntegrityViolation {
                check: "index".to_string(),
                table: Some(entry.collection.clone()),
                object: Some(format!("hnsw_{}", entry.collection)),
                record_id: None,
                message: "HNSW vector index verification failed".to_string(),
            });
        }
    }
    report.valid = report.violations.is_empty();
    Ok(report)
}

pub(crate) fn check_unique_key_integrity(
    report: &mut SqlIntegrityReport,
    table: &str,
    name: &str,
    schema: &TableSchema,
    columns: &[String],
    records: &[Record],
    reject_nulls: bool,
) {
    let mut seen: BTreeMap<String, String> = BTreeMap::new();
    for record in records {
        let key = record_column_values(record, schema, columns);
        if key.iter().any(|value| matches!(value, SqlValue::Null)) {
            if reject_nulls {
                report.violations.push(SqlIntegrityViolation {
                    check: "primary_key".to_string(),
                    table: Some(table.to_string()),
                    object: Some(name.to_string()),
                    record_id: Some(record.id.clone()),
                    message: "primary key contains null".to_string(),
                });
            }
            continue;
        }
        let key = key
            .iter()
            .map(SqlValue::to_cell)
            .collect::<Vec<_>>()
            .join("\u{1f}");
        if let Some(first_id) = seen.insert(key, record.id.clone()) {
            report.violations.push(SqlIntegrityViolation {
                check: if reject_nulls {
                    "primary_key".to_string()
                } else {
                    "unique_constraint".to_string()
                },
                table: Some(table.to_string()),
                object: Some(name.to_string()),
                record_id: Some(record.id.clone()),
                message: format!("duplicate key also present in record {first_id}"),
            });
        }
    }
}

pub(crate) fn constraint_name(constraint: &ConstraintSchema) -> &str {
    match constraint {
        ConstraintSchema::Unique { name, .. }
        | ConstraintSchema::Check { name, .. }
        | ConstraintSchema::ForeignKey { name, .. }
        | ConstraintSchema::Exclusion { name, .. } => name,
    }
}

pub(crate) fn set_constraint_name(constraint: &mut ConstraintSchema, new_name: String) {
    match constraint {
        ConstraintSchema::Unique { name, .. }
        | ConstraintSchema::Check { name, .. }
        | ConstraintSchema::ForeignKey { name, .. }
        | ConstraintSchema::Exclusion { name, .. } => *name = new_name,
    }
}

pub(crate) fn column_default_value(column: &ColumnDef) -> Result<Option<SqlValue>> {
    for option in &column.options {
        if let ColumnOption::Default(expr) = &option.option {
            return literal_default_value_from_data_type(expr, &column.data_type);
        }
    }
    Ok(None)
}

pub(crate) fn literal_default_value_from_data_type(
    expr: &Expr,
    data_type: &DataType,
) -> Result<Option<SqlValue>> {
    if default_expr_contains_function(expr) {
        return Ok(None);
    }
    match cast_value(eval_constant_expr(expr)?, data_type) {
        Ok(value) => Ok(Some(value)),
        Err(SqlError::Unsupported(_)) => Ok(None),
        Err(SqlError::DataException {
            sqlstate: "22003", ..
        }) => Ok(None),
        Err(error) => Err(error),
    }
}

pub(crate) fn literal_default_value_from_expr(
    expr: &Expr,
    pg_type: &str,
) -> Result<Option<SqlValue>> {
    if default_expr_contains_function(expr) {
        return Ok(None);
    }
    match cast_value_to_pg_type(eval_constant_expr(expr)?, pg_type) {
        Ok(value) => Ok(Some(value)),
        Err(SqlError::Unsupported(_)) => Ok(None),
        Err(SqlError::DataException {
            sqlstate: "22003", ..
        }) => Ok(None),
        Err(error) => Err(error),
    }
}

pub(crate) fn literal_default_value_from_expr_with_db(
    db: &BicDb,
    expr: &Expr,
    pg_type: &str,
) -> Result<Option<SqlValue>> {
    let array_element_type = pg_type
        .strip_suffix("[]")
        .filter(|element_type| is_oid_alias_type(element_type));
    if !is_oid_alias_type(pg_type) && array_element_type.is_none() {
        return literal_default_value_from_expr(expr, pg_type);
    }
    if default_expr_contains_function(expr) {
        return Ok(None);
    }
    let input = match expr {
        Expr::Nested(expr) | Expr::Collate { expr, .. } => expr.as_ref(),
        Expr::Cast {
            expr, data_type, ..
        } if pg_type_from_data_type(data_type).is_ok_and(|(cast_type, _)| cast_type == pg_type) => {
            expr.as_ref()
        }
        _ => expr,
    };
    match eval_constant_expr(input).and_then(|value| {
        if let Some(element_type) = array_element_type {
            resolve_oid_alias_array_value(db, element_type, value)
        } else {
            resolve_oid_alias_value(db, pg_type, value)
        }
    }) {
        Ok(value) => Ok(Some(value)),
        Err(SqlError::Unsupported(_)) => Ok(None),
        Err(error) => Err(error),
    }
}

pub(crate) fn default_expr_contains_function(expr: &Expr) -> bool {
    match expr {
        Expr::Function(_) => true,
        Expr::Nested(expr)
        | Expr::Cast { expr, .. }
        | Expr::UnaryOp { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr) => default_expr_contains_function(expr),
        Expr::BinaryOp { left, right, .. }
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right) => {
            default_expr_contains_function(left) || default_expr_contains_function(right)
        }
        Expr::Position { expr, r#in } => {
            default_expr_contains_function(expr) || default_expr_contains_function(r#in)
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand
                .as_deref()
                .is_some_and(default_expr_contains_function)
                || conditions.iter().any(|condition| {
                    default_expr_contains_function(&condition.condition)
                        || default_expr_contains_function(&condition.result)
                })
                || else_result
                    .as_deref()
                    .is_some_and(default_expr_contains_function)
        }
        Expr::Array(array) => array.elem.iter().any(default_expr_contains_function),
        Expr::InList { expr, list, .. } => {
            default_expr_contains_function(expr) || list.iter().any(default_expr_contains_function)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            default_expr_contains_function(expr)
                || default_expr_contains_function(low)
                || default_expr_contains_function(high)
        }
        Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
            default_expr_contains_function(left) || default_expr_contains_function(right)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            default_expr_contains_function(expr) || default_expr_contains_function(pattern)
        }
        _ => false,
    }
}

pub(crate) fn column_default_expr(column: &ColumnDef) -> Option<String> {
    column.options.iter().find_map(|option| {
        if let ColumnOption::Default(expr) = &option.option {
            Some(expr.to_string())
        } else {
            None
        }
    })
}

pub(crate) fn columns_from_create_table_like(
    db: &BicDb,
    like_kind: &CreateTableLikeKind,
) -> Result<Vec<ColumnSchema>> {
    let like = create_table_like_clause(like_kind);
    let source_table = relation_name(&like.name)?;
    let source_schema = load_schema(db, &source_table)?
        .ok_or_else(|| SqlError::InvalidCollection(source_table.clone()))?;
    let include_defaults = matches!(like.defaults, Some(CreateTableLikeDefaults::Including));

    Ok(source_schema
        .columns
        .into_iter()
        .filter(|column| !column.hidden)
        .map(|mut column| {
            column.primary_key = false;
            column.default_sequence = None;
            column.identity = None;
            if !include_defaults {
                column.default_value = None;
                column.default_expr = None;
            }
            column
        })
        .collect())
}

pub(crate) fn create_table_like_clause(like_kind: &CreateTableLikeKind) -> &CreateTableLike {
    match like_kind {
        CreateTableLikeKind::Parenthesized(like) | CreateTableLikeKind::Plain(like) => like,
    }
}

#[derive(Clone)]
pub(crate) struct TruncateTarget {
    pub(crate) name: String,
    pub(crate) schema: TableSchema,
}

pub(crate) fn truncate_target_tables(
    db: &BicDb,
    target: &TruncateTableTarget,
    if_exists: bool,
) -> Result<Vec<TruncateTarget>> {
    let table = relation_name(&target.name)?;
    let Some(schema) = load_schema(db, &table)? else {
        if if_exists {
            return Ok(Vec::new());
        }
        return Err(SqlError::InvalidCollection(table));
    };
    let mut targets = vec![TruncateTarget {
        name: table.clone(),
        schema,
    }];
    if !target.only {
        for schema in list_schemas(db)? {
            if schema
                .partition_of
                .as_ref()
                .is_some_and(|partition| partition.parent_table.eq_ignore_ascii_case(&table))
            {
                targets.push(TruncateTarget {
                    name: schema.name.clone(),
                    schema,
                });
            }
        }
    }
    Ok(targets)
}

pub(crate) fn restart_owned_sequences(db: &mut BicDb, schema: &TableSchema) -> Result<()> {
    for column in &schema.columns {
        let Some(sequence_name) = &column.default_sequence else {
            continue;
        };
        let mut sequence = load_sequence_required(db, sequence_name)?;
        sequence.last_value = sequence.start_value;
        sequence.is_called = false;
        save_sequence(db, &sequence)?;
    }
    Ok(())
}

pub(crate) fn remove_record_column(record: &mut Record, column: &str) {
    if column.eq_ignore_ascii_case("timestamp") {
        record.timestamp = None;
    } else if column.eq_ignore_ascii_case("payload") {
        record.payload = None;
    } else if column.eq_ignore_ascii_case("vector")
        || column.eq_ignore_ascii_case("embedding")
        || record
            .metadata
            .get(column)
            .is_some_and(|value| matches!(value, JsonValue::Array(_)))
    {
        record.vector = None;
        remove_json_object_key(&mut record.metadata, column);
    } else {
        remove_json_object_key(&mut record.metadata, column);
    }
}

pub(crate) fn rename_record_column(record: &mut Record, old_column: &str, new_column: &str) {
    if let Some(value) = remove_json_object_key(&mut record.metadata, old_column) {
        set_json_object_value(&mut record.metadata, new_column, value);
    }
}

pub(crate) fn remove_json_object_key(metadata: &mut JsonValue, key: &str) -> Option<JsonValue> {
    metadata.as_object_mut().and_then(|object| {
        let actual = object
            .keys()
            .find(|candidate| candidate.eq_ignore_ascii_case(key))
            .cloned()?;
        object.remove(&actual)
    })
}

pub(crate) fn rename_column_references(
    schema: &mut TableSchema,
    old_column: &str,
    new_column: &str,
) {
    for constraint in &mut schema.constraints {
        match constraint {
            ConstraintSchema::Unique { columns, .. }
            | ConstraintSchema::ForeignKey { columns, .. } => {
                for column in columns {
                    if column.eq_ignore_ascii_case(old_column) {
                        *column = new_column.to_string();
                    }
                }
            }
            ConstraintSchema::Check { expression, .. } => {
                *expression = replace_identifier_token(expression, old_column, new_column);
            }
            ConstraintSchema::Exclusion {
                equal_columns,
                range,
                predicate,
                ..
            } => {
                for column in equal_columns {
                    if column.eq_ignore_ascii_case(old_column) {
                        *column = new_column.to_string();
                    }
                }
                if let Some(range) = range {
                    if range.start_column.eq_ignore_ascii_case(old_column) {
                        range.start_column = new_column.to_string();
                    }
                    if range.end_column.eq_ignore_ascii_case(old_column) {
                        range.end_column = new_column.to_string();
                    }
                }
                if let Some(expression) = predicate {
                    *expression = replace_identifier_token(expression, old_column, new_column);
                }
            }
        }
    }
    for index in &mut schema.indexes {
        index.expression = replace_identifier_token(&index.expression, old_column, new_column);
    }
}

pub(crate) fn replace_identifier_token(input: &str, old: &str, new: &str) -> String {
    input
        .split_inclusive(|ch: char| !ch.is_alphanumeric() && ch != '_')
        .map(|part| {
            let trimmed = part.trim_end_matches(|ch: char| !ch.is_alphanumeric() && ch != '_');
            let suffix = &part[trimmed.len()..];
            if trimmed.eq_ignore_ascii_case(old) {
                format!("{new}{suffix}")
            } else {
                part.to_string()
            }
        })
        .collect()
}

pub(crate) fn column_schema_from_def(db: &BicDb, column: &ColumnDef) -> Result<ColumnSchema> {
    if let Some(pg_type) = pseudo_column_type_name(&column.data_type) {
        let name = ident_value(&column.name);
        return Err(SqlError::data_exception(
            "42P16",
            format!("column \"{name}\" has pseudo-type {pg_type}"),
            Some(pg_type),
        ));
    }
    let (pg_type, vector_dim, user_type) = if let Some(serial_type) = serial_type(&column.data_type)
    {
        (serial_type.to_string(), None, None)
    } else if let Some(user_type) = user_type_column_from_data_type(db, &column.data_type)? {
        (user_type.formatted_name(), None, Some(user_type))
    } else {
        let (pg_type, vector_dim) = pg_type_from_data_type(&column.data_type)?;
        (pg_type, vector_dim, None)
    };
    let primary_key = column
        .options
        .iter()
        .any(|option| matches!(option.option, ColumnOption::PrimaryKey(_)));
    let nullable = !primary_key
        && !column
            .options
            .iter()
            .any(|option| matches!(option.option, ColumnOption::NotNull));
    let collation = column
        .options
        .iter()
        .find_map(|option| match &option.option {
            ColumnOption::Collation(name) => Some(normalize_column_collation(name)),
            _ => None,
        })
        .transpose()?;
    let user_type_is_collatable = user_type
        .as_ref()
        .is_some_and(|user_type| user_type.scalar_collation_oid() != 0);
    if collation.is_some() && !pg_type_is_collatable(&pg_type) && !user_type_is_collatable {
        return Err(SqlError::data_exception(
            "42804",
            format!("collations are not supported by type {pg_type}"),
            Some(pg_type),
        ));
    }
    let default_expr = column_default_expr(column);
    let generated_expr = column
        .options
        .iter()
        .find_map(|option| match &option.option {
            ColumnOption::Generated {
                generation_expr: Some(expr),
                ..
            } => Some(expr.to_string()),
            _ => None,
        });
    Ok(ColumnSchema {
        name: ident_value(&column.name),
        pg_type,
        user_type,
        collation,
        type_modifier: pg_type_modifier_from_data_type(&column.data_type)?,
        array_ndims: array_ndims_from_data_type(&column.data_type),
        compression: None,
        primary_key,
        hidden: false,
        nullable,
        vector_dim,
        default_sequence: None,
        default_value: None,
        default_expr,
        generated_expr,
        identity: None,
    })
}

pub(crate) fn pseudo_column_type_name(data_type: &DataType) -> Option<String> {
    let (scalar, array) = match data_type {
        DataType::Array(
            ArrayElemTypeDef::AngleBracket(element)
            | ArrayElemTypeDef::SquareBracket(element, _)
            | ArrayElemTypeDef::Parenthesis(element),
        ) => (element.as_ref(), true),
        _ => (data_type, false),
    };
    let name = match scalar {
        DataType::Trigger => "trigger".to_string(),
        DataType::Custom(name, _) => normalize_object_name(&name.to_string())
            .rsplit('.')
            .next()
            .unwrap_or_default()
            .to_string(),
        _ => return None,
    };
    let spec = pg_type_spec(&name).filter(|spec| spec.pseudo)?;
    Some(if array && spec.name == "record" {
        "record[]".to_string()
    } else if spec.name == "any" {
        "\"any\"".to_string()
    } else {
        spec.name.to_string()
    })
}

pub(crate) fn array_ndims_from_data_type(data_type: &DataType) -> usize {
    let DataType::Array(element) = data_type else {
        return 0;
    };
    let nested = match element {
        ArrayElemTypeDef::AngleBracket(element)
        | ArrayElemTypeDef::SquareBracket(element, _)
        | ArrayElemTypeDef::Parenthesis(element) => array_ndims_from_data_type(element),
        ArrayElemTypeDef::None => 0,
    };
    nested + 1
}

pub(crate) fn user_type_column_from_data_type(
    db: &BicDb,
    data_type: &DataType,
) -> Result<Option<UserTypeColumnSchema>> {
    if pg_type_from_data_type(data_type).is_ok() {
        return Ok(None);
    }
    let (data_type, array) = match data_type {
        DataType::Array(
            ArrayElemTypeDef::AngleBracket(element)
            | ArrayElemTypeDef::SquareBracket(element, _)
            | ArrayElemTypeDef::Parenthesis(element),
        ) => (element.as_ref(), true),
        _ => (data_type, false),
    };
    let DataType::Custom(name, _) = data_type else {
        return Ok(None);
    };
    let parts = object_name_parts(name);
    let (schema_name, name) = match parts.as_slice() {
        [name] => ("public".to_string(), name.clone()),
        [schema_name, name] => (schema_name.clone(), name.clone()),
        _ => {
            return Err(SqlError::InvalidSql(format!(
                "invalid user-defined type name {}",
                object_name(name)?
            )));
        }
    };
    if let Some(user_type) = load_user_type(db, &schema_name, &name)? {
        if matches!(user_type.kind, UserTypeKind::Shell) {
            return Err(SqlError::data_exception(
                "42704",
                format!("type \"{name}\" is only a shell"),
                Some(name),
            ));
        }
        return Ok(Some(user_type.column_type(array)));
    }
    if let Some(schema) = list_schemas(db)?.into_iter().find(|schema| {
        schema.schema_name.eq_ignore_ascii_case(&schema_name)
            && schema.name.eq_ignore_ascii_case(&name)
    }) {
        return Ok(Some(table_row_type_column(&schema, array)));
    }
    Err(SqlError::undefined_type(if schema_name == "public" {
        name
    } else {
        format!("{schema_name}.{name}")
    }))
}

pub(crate) fn text_column_schema(name: String) -> ColumnSchema {
    ColumnSchema {
        name,
        pg_type: "text".to_string(),
        user_type: None,
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

pub(crate) fn normalize_column_collation(name: &ObjectName) -> Result<String> {
    let name = object_name(name)?;
    let name = name.rsplit('.').next().unwrap_or(&name);
    let canonical = match name.to_ascii_lowercase().as_str() {
        "default" => "default",
        "c" => "C",
        "posix" => "POSIX",
        "ucs_basic" => "ucs_basic",
        "en-x-icu" => "en-x-icu",
        _ => {
            return Err(SqlError::data_exception(
                "42704",
                format!("collation \"{name}\" for encoding UTF8 does not exist"),
                None,
            ))
        }
    };
    Ok(canonical.to_string())
}

pub(crate) fn pg_type_is_collatable(pg_type: &str) -> bool {
    let mut scalar_type = pg_type;
    while let Some(element_type) = scalar_type.strip_suffix("[]") {
        scalar_type = element_type;
    }
    matches!(scalar_type, "text" | "varchar" | "bpchar" | "name")
}

pub(crate) fn view_columns_from_query(query: &Query) -> Result<Vec<ColumnSchema>> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(SqlError::Unsupported(
            "metadata-only CREATE VIEW supports SELECT queries only".to_string(),
        ));
    };
    select
        .projection
        .iter()
        .enumerate()
        .map(|(idx, item)| view_column_from_select_item(item, idx))
        .collect()
}

pub(crate) fn inherit_view_source_column_types(
    db: &BicDb,
    query: &Query,
    columns: &mut [ColumnSchema],
) -> Result<()> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(());
    };
    let Some(from) = select.from.first() else {
        return Ok(());
    };
    let TableFactor::Table { name, .. } = &from.relation else {
        return Ok(());
    };
    let relation = relation_name(name)?;
    let source_columns = if let Some(schema) = load_schema(db, &relation)? {
        schema.columns
    } else if let Some(view) = load_view(db, &relation)? {
        view.columns
    } else {
        return Ok(());
    };

    for (item, target) in select.projection.iter().zip(columns.iter_mut()) {
        let expr = match item {
            SelectItem::ExprWithAlias { expr, .. }
            | SelectItem::ExprWithAliases { expr, .. }
            | SelectItem::UnnamedExpr(expr) => expr,
            _ => continue,
        };
        let source_name = match expr {
            Expr::Identifier(ident) => Some(ident.value.as_str()),
            Expr::CompoundIdentifier(idents) => idents.last().map(|ident| ident.value.as_str()),
            _ => None,
        };
        let Some(source) = source_name.and_then(|source_name| {
            source_columns
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case(source_name))
        }) else {
            continue;
        };
        target.pg_type = source.pg_type.clone();
        target.type_modifier = source.type_modifier.clone();
        target.vector_dim = source.vector_dim;
        target.nullable = source.nullable;
    }
    Ok(())
}

pub(crate) fn view_column_from_select_item(item: &SelectItem, idx: usize) -> Result<ColumnSchema> {
    let name = match item {
        SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
        SelectItem::ExprWithAliases { aliases, expr } => aliases
            .last()
            .map(|alias| alias.value.clone())
            .or_else(|| view_column_name_from_expr(expr))
            .unwrap_or_else(|| format!("column{}", idx + 1)),
        SelectItem::UnnamedExpr(expr) => {
            view_column_name_from_expr(expr).unwrap_or_else(|| format!("column{}", idx + 1))
        }
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
            return Err(SqlError::Unsupported(
                "metadata-only CREATE VIEW cannot infer wildcard columns".to_string(),
            ))
        }
    };
    Ok(text_column_schema(name))
}

pub(crate) fn view_column_name_from_expr(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(idents) => idents.last().map(|ident| ident.value.clone()),
        Expr::Nested(expr) | Expr::Cast { expr, .. } => view_column_name_from_expr(expr),
        Expr::Function(function) => function.name.0.last().map(|part| part.to_string()),
        _ => None,
    }
}

pub(crate) fn column_constraints_from_def(
    table: &str,
    column: &ColumnDef,
    column_schema: &ColumnSchema,
) -> Result<Vec<ConstraintSchema>> {
    let mut constraints = Vec::new();
    for option in &column.options {
        match &option.option {
            ColumnOption::Unique(_) => constraints.push(ConstraintSchema::Unique {
                name: option
                    .name
                    .as_ref()
                    .map(ident_value)
                    .unwrap_or_else(|| format!("{}_{}_key", table, column_schema.name)),
                columns: vec![column_schema.name.clone()],
                validated: true,
            }),
            ColumnOption::Check(check) => constraints.push(ConstraintSchema::Check {
                name: option
                    .name
                    .as_ref()
                    .map(ident_value)
                    .unwrap_or_else(|| format!("{}_{}_check", table, column_schema.name)),
                expression: check.expr.to_string(),
                validated: true,
            }),
            ColumnOption::ForeignKey(foreign_key) => {
                reject_unsupported_constraint_characteristics(
                    foreign_key.characteristics.as_ref(),
                )?;
                let foreign_table = relation_name(&foreign_key.foreign_table)?;
                let referred_columns = if foreign_key.referred_columns.is_empty() {
                    vec!["id".to_string()]
                } else {
                    foreign_key
                        .referred_columns
                        .iter()
                        .map(ident_value)
                        .collect()
                };
                constraints.push(ConstraintSchema::ForeignKey {
                    name: option
                        .name
                        .as_ref()
                        .map(ident_value)
                        .unwrap_or_else(|| format!("{}_{}_fkey", table, column_schema.name)),
                    columns: vec![column_schema.name.clone()],
                    foreign_table,
                    referred_columns,
                    on_delete: foreign_key_action(foreign_key.on_delete.as_ref()),
                    on_update: foreign_key_action(foreign_key.on_update.as_ref()),
                    validated: true,
                });
            }
            _ => {}
        }
    }
    Ok(constraints)
}

pub(crate) fn table_constraint_schema(
    table: &str,
    constraint: &TableConstraint,
) -> Result<Vec<ConstraintSchema>> {
    Ok(match constraint {
        TableConstraint::Unique(unique) => {
            reject_unsupported_constraint_characteristics(unique.characteristics.as_ref())?;
            vec![ConstraintSchema::Unique {
                name: unique.name.as_ref().map(ident_value).unwrap_or_else(|| {
                    format!(
                        "{}_{}_key",
                        table,
                        simple_index_column_names(&unique.columns)
                            .unwrap_or_default()
                            .join("_")
                    )
                }),
                columns: simple_index_column_names(&unique.columns)?,
                validated: true,
            }]
        }
        TableConstraint::Check(check) => vec![ConstraintSchema::Check {
            name: check
                .name
                .as_ref()
                .map(ident_value)
                .unwrap_or_else(|| format!("{}_check", table)),
            expression: check.expr.to_string(),
            validated: true,
        }],
        TableConstraint::ForeignKey(foreign_key) => {
            reject_unsupported_constraint_characteristics(foreign_key.characteristics.as_ref())?;
            vec![ConstraintSchema::ForeignKey {
                name: foreign_key
                    .name
                    .as_ref()
                    .map(ident_value)
                    .unwrap_or_else(|| {
                        format!(
                            "{}_{}_fkey",
                            table,
                            foreign_key
                                .columns
                                .iter()
                                .map(ident_value)
                                .collect::<Vec<_>>()
                                .join("_")
                        )
                    }),
                columns: foreign_key.columns.iter().map(ident_value).collect(),
                foreign_table: relation_name(&foreign_key.foreign_table)?,
                referred_columns: if foreign_key.referred_columns.is_empty() {
                    vec!["id".to_string()]
                } else {
                    foreign_key
                        .referred_columns
                        .iter()
                        .map(ident_value)
                        .collect()
                },
                on_delete: foreign_key_action(foreign_key.on_delete.as_ref()),
                on_update: foreign_key_action(foreign_key.on_update.as_ref()),
                validated: true,
            }]
        }
        TableConstraint::PrimaryKey(primary_key) => {
            reject_unsupported_constraint_characteristics(primary_key.characteristics.as_ref())?;
            vec![ConstraintSchema::Unique {
                name: primary_key
                    .name
                    .as_ref()
                    .map(ident_value)
                    .unwrap_or_else(|| default_primary_key_name(table)),
                columns: simple_index_column_names(&primary_key.columns)?,
                validated: true,
            }]
        }
        other => {
            return Err(SqlError::Unsupported(format!(
                "table constraint {other} is not supported"
            )))
        }
    })
}

pub(crate) fn reject_unsupported_constraint_characteristics(
    characteristics: Option<&ConstraintCharacteristics>,
) -> Result<()> {
    let Some(characteristics) = characteristics else {
        return Ok(());
    };
    if characteristics.enforced == Some(false) {
        return Err(SqlError::Unsupported(
            "NOT ENFORCED constraints are not supported".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn simple_index_column_names(columns: &[IndexColumn]) -> Result<Vec<String>> {
    columns
        .iter()
        .map(|column| match &column.column.expr {
            Expr::Identifier(ident) => Ok(ident_value(ident)),
            other => Err(SqlError::Unsupported(format!(
                "constraint column expression {other} is not supported"
            ))),
        })
        .collect()
}

pub(crate) fn ident_value(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}

pub(crate) fn foreign_key_action(action: Option<&ReferentialAction>) -> ForeignKeyAction {
    match action {
        Some(ReferentialAction::Restrict) => ForeignKeyAction::Restrict,
        Some(ReferentialAction::Cascade) => ForeignKeyAction::Cascade,
        Some(ReferentialAction::SetNull) => ForeignKeyAction::SetNull,
        Some(ReferentialAction::SetDefault) => ForeignKeyAction::SetDefault,
        Some(ReferentialAction::NoAction) | None => ForeignKeyAction::NoAction,
    }
}

pub(crate) fn serial_type(data_type: &DataType) -> Option<&'static str> {
    let rendered = data_type.to_string().to_ascii_lowercase();
    match rendered.as_str() {
        "smallserial" | "serial2" => Some("int2"),
        "serial" | "serial4" => Some("int4"),
        "bigserial" | "serial8" => Some("int8"),
        _ => None,
    }
}

pub(crate) fn identity_options(column: &ColumnDef) -> Result<Option<(String, &[SequenceOptions])>> {
    for option in &column.options {
        if let ColumnOption::Generated {
            generated_as,
            sequence_options,
            generation_expr,
            ..
        } = &option.option
        {
            if generation_expr.is_some() {
                continue;
            }
            let kind = match generated_as {
                GeneratedAs::ByDefault => "d",
                GeneratedAs::Always => "a",
                GeneratedAs::ExpStored => continue,
            };
            return Ok(Some((
                kind.to_string(),
                sequence_options.as_deref().unwrap_or(&[]),
            )));
        }
    }
    Ok(None)
}

pub(crate) fn validate_generated_always_insert(
    schema: &TableSchema,
    columns: &[String],
    source: &Query,
) -> Result<()> {
    for column in &schema.columns {
        if column.identity.as_deref() != Some("a") && column.generated_expr.is_none() {
            continue;
        }
        let Some(position) = columns
            .iter()
            .position(|name| name.eq_ignore_ascii_case(&column.name))
        else {
            continue;
        };
        let only_defaults = match source.body.as_ref() {
            SetExpr::Values(values) => values.rows.iter().all(|row| {
                row.get(position)
                    .is_some_and(|expression| expr_is_default(expression))
            }),
            _ => false,
        };
        if !only_defaults {
            return Err(SqlError::generated_always_violation(format!(
                "cannot insert a non-DEFAULT value into column \"{}\"",
                column.name
            )));
        }
    }
    Ok(())
}

pub(crate) fn sequence_from_options(
    sequence_name: String,
    data_type: &str,
    sequence_options: &[SequenceOptions],
) -> Result<SequenceSchema> {
    let increment_by = sequence_options
        .iter()
        .find_map(|option| match option {
            SequenceOptions::IncrementBy(expr, _) => Some(sequence_option_i64(expr, "INCREMENT")),
            _ => None,
        })
        .transpose()?
        .unwrap_or(1);
    let mut sequence = SequenceSchema::new_typed(sequence_name, data_type, increment_by);
    let defaults = sequence.clone();
    let mut explicit_start = false;
    for option in sequence_options {
        match option {
            SequenceOptions::IncrementBy(expr, _) => {
                sequence.increment_by = sequence_option_i64(expr, "INCREMENT")?;
            }
            SequenceOptions::MinValue(Some(expr)) => {
                sequence.min_value = sequence_option_i64(expr, "MINVALUE")?;
            }
            SequenceOptions::MinValue(None) => {
                sequence.min_value = defaults.min_value;
            }
            SequenceOptions::MaxValue(Some(expr)) => {
                sequence.max_value = sequence_option_i64(expr, "MAXVALUE")?;
            }
            SequenceOptions::MaxValue(None) => {
                sequence.max_value = defaults.max_value;
            }
            SequenceOptions::StartWith(expr, _) => {
                sequence.start_value = sequence_option_i64(expr, "START")?;
                explicit_start = true;
            }
            SequenceOptions::Cache(expr) => {
                sequence.cache_size = sequence_option_i64(expr, "CACHE")?;
            }
            SequenceOptions::Cycle(cycle) => {
                sequence.cycle = *cycle;
            }
        }
    }
    if !explicit_start {
        sequence.start_value = if sequence.increment_by >= 0 {
            sequence.min_value
        } else {
            sequence.max_value
        };
    }
    sequence.last_value = sequence.start_value;
    validate_sequence_schema(&sequence)?;
    Ok(sequence)
}

pub(crate) fn validate_sequence_schema(sequence: &SequenceSchema) -> Result<()> {
    let (type_min, type_max) = sequence_type_limits(&sequence.data_type);
    if sequence.increment_by == 0 {
        return Err(SqlError::invalid_parameter_value(
            "INCREMENT must not be zero",
        ));
    }
    if sequence.increment_by < type_min || sequence.increment_by > type_max {
        return Err(SqlError::invalid_parameter_value(format!(
            "INCREMENT ({}) must be between {type_min} and {type_max}",
            sequence.increment_by
        )));
    }
    if sequence.min_value < type_min || sequence.min_value > type_max {
        return Err(SqlError::invalid_parameter_value(format!(
            "MINVALUE ({}) is out of range for sequence data type {}",
            sequence.min_value,
            sequence_data_type_name(&sequence.data_type)
        )));
    }
    if sequence.max_value < type_min || sequence.max_value > type_max {
        return Err(SqlError::invalid_parameter_value(format!(
            "MAXVALUE ({}) is out of range for sequence data type {}",
            sequence.max_value,
            sequence_data_type_name(&sequence.data_type)
        )));
    }
    if sequence.min_value >= sequence.max_value {
        return Err(SqlError::invalid_parameter_value(format!(
            "MINVALUE ({}) must be less than MAXVALUE ({})",
            sequence.min_value, sequence.max_value
        )));
    }
    validate_sequence_value(sequence, sequence.start_value, "START")?;
    if sequence.cache_size <= 0 {
        return Err(SqlError::invalid_parameter_value(
            "CACHE must be greater than zero",
        ));
    }
    Ok(())
}

pub(crate) fn validate_sequence_value(
    sequence: &SequenceSchema,
    value: i64,
    label: &str,
) -> Result<()> {
    if value < sequence.min_value || value > sequence.max_value {
        return Err(SqlError::invalid_parameter_value(format!(
            "{label} value {value} is out of bounds for sequence \"{}\" ({}..{})",
            sequence.name, sequence.min_value, sequence.max_value
        )));
    }
    Ok(())
}

pub(crate) fn raw_create_sequence(sql: &str) -> Result<Option<(SequenceSchema, bool)>> {
    // The raw compatibility parser normalizes the whole statement. Quoted
    // identifiers must retain their exact identity, so let the AST path handle
    // those statements.
    if sql.contains('"') {
        return Ok(None);
    }
    let normalized = normalize_sql(sql);
    let tokens = normalized.split_whitespace().collect::<Vec<_>>();
    let mut idx = 0;
    if tokens.get(idx) != Some(&"create") {
        return Ok(None);
    }
    idx += 1;
    if matches!(tokens.get(idx), Some(&"temporary") | Some(&"temp")) {
        idx += 1;
    }
    if tokens.get(idx) != Some(&"sequence") {
        return Ok(None);
    }
    idx += 1;
    let mut if_not_exists = false;
    if tokens.get(idx..idx + 3) == Some(&["if", "not", "exists"][..]) {
        if_not_exists = true;
        idx += 3;
    }
    let Some(name) = tokens.get(idx) else {
        return Err(SqlError::InvalidSql(
            "CREATE SEQUENCE expects a sequence name".to_string(),
        ));
    };
    let option_tokens = &tokens[idx + 1..];
    let data_type = option_tokens
        .windows(2)
        .find(|pair| pair[0] == "as")
        .map(|pair| canonical_sequence_data_type(pair[1]))
        .unwrap_or("int8");
    let increment_by = option_tokens
        .iter()
        .position(|token| *token == "increment")
        .map(|position| {
            let mut value_position = position + 1;
            if option_tokens.get(value_position) == Some(&"by") {
                value_position += 1;
            }
            raw_sequence_option_i64(option_tokens.get(value_position), "INCREMENT")
        })
        .transpose()?
        .unwrap_or(1);
    let mut sequence =
        SequenceSchema::new_typed(normalize_sequence_name(name), data_type, increment_by);
    let defaults = sequence.clone();
    let mut explicit_start = false;
    idx += 1;

    while idx < tokens.len() {
        match tokens[idx] {
            "as" => {
                let Some(data_type) = tokens.get(idx + 1) else {
                    return Err(SqlError::InvalidSql(
                        "CREATE SEQUENCE AS expects a data type".to_string(),
                    ));
                };
                if !matches!(*data_type, "smallint" | "integer" | "int" | "bigint") {
                    return Err(SqlError::Unsupported(
                        "CREATE SEQUENCE AS supports only integer types".to_string(),
                    ));
                }
                idx += 2;
            }
            "start" => {
                idx += 1;
                if tokens.get(idx) == Some(&"with") {
                    idx += 1;
                }
                let value = raw_sequence_option_i64(tokens.get(idx), "START")?;
                sequence.start_value = value;
                explicit_start = true;
                idx += 1;
            }
            "increment" => {
                idx += 1;
                if tokens.get(idx) == Some(&"by") {
                    idx += 1;
                }
                sequence.increment_by = raw_sequence_option_i64(tokens.get(idx), "INCREMENT")?;
                idx += 1;
            }
            "minvalue" => {
                sequence.min_value = raw_sequence_option_i64(tokens.get(idx + 1), "MINVALUE")?;
                idx += 2;
            }
            "maxvalue" => {
                sequence.max_value = raw_sequence_option_i64(tokens.get(idx + 1), "MAXVALUE")?;
                idx += 2;
            }
            "no" if tokens.get(idx + 1) == Some(&"minvalue") => {
                sequence.min_value = defaults.min_value;
                idx += 2;
            }
            "no" if tokens.get(idx + 1) == Some(&"maxvalue") => {
                sequence.max_value = defaults.max_value;
                idx += 2;
            }
            "cache" => {
                sequence.cache_size = raw_sequence_option_i64(tokens.get(idx + 1), "CACHE")?;
                idx += 2;
            }
            "cycle" => {
                sequence.cycle = true;
                idx += 1;
            }
            "no" if tokens.get(idx + 1) == Some(&"cycle") => {
                sequence.cycle = false;
                idx += 2;
            }
            "owned" if tokens.get(idx + 1) == Some(&"by") => {
                let Some(owner) = tokens.get(idx + 2) else {
                    return Err(SqlError::InvalidSql(
                        "CREATE SEQUENCE OWNED BY expects an owner".to_string(),
                    ));
                };
                if *owner != "none" {
                    if let Some((table, column)) = owner.rsplit_once('.') {
                        sequence.owned_by_table = Some(normalize_object_name(table));
                        sequence.owned_by_column = Some(column.trim_matches('"').to_string());
                    }
                }
                idx += 3;
            }
            other => {
                return Err(SqlError::Unsupported(format!(
                    "CREATE SEQUENCE option {other} is not supported"
                )))
            }
        }
    }
    if !explicit_start {
        sequence.start_value = if sequence.increment_by >= 0 {
            sequence.min_value
        } else {
            sequence.max_value
        };
    }
    sequence.last_value = sequence.start_value;
    validate_sequence_schema(&sequence)?;
    Ok(Some((sequence, if_not_exists)))
}

pub(crate) fn raw_sequence_option_i64(value: Option<&&str>, label: &str) -> Result<i64> {
    value
        .copied()
        .ok_or_else(|| SqlError::InvalidSql(format!("CREATE SEQUENCE {label} expects an integer")))?
        .parse::<i64>()
        .map_err(|_| SqlError::InvalidSql(format!("CREATE SEQUENCE {label} expects an integer")))
}

pub(crate) fn raw_analyze_target(sql: &str) -> Result<Option<Option<String>>> {
    let Some(mut rest) = strip_prefix_ci(trim_sql_statement(sql), "ANALYZE") else {
        return Ok(None);
    };
    rest = trim_sql_trailing_comments(rest);
    if rest.is_empty() {
        return Ok(Some(None));
    }
    if let Some(after_verbose) = strip_prefix_ci(rest.trim_start(), "VERBOSE") {
        rest = trim_sql_trailing_comments(after_verbose);
        if rest.is_empty() {
            return Ok(Some(None));
        }
    }
    if let Some(after_table) = strip_prefix_ci(rest.trim_start(), "TABLE ") {
        rest = after_table;
    }

    let (target, trailing) = parse_leading_qualified_sql_identifier(rest)?;
    let trailing = trim_sql_trailing_comments(trailing);
    if !trailing.is_empty() {
        return Err(SqlError::Unsupported(format!(
            "unsupported ANALYZE trailing clause `{trailing}`"
        )));
    }
    let (_, table) = identifier_schema_and_name(&target);
    Ok(Some(Some(table)))
}

pub(crate) type SequenceOwner = Option<(String, String)>;

pub(crate) enum RawAlterSequence {
    OwnerTo {
        sequence: String,
        owner: String,
    },
    OwnedBy {
        sequence: String,
        owner: SequenceOwner,
    },
    Restart {
        sequence: String,
        value: Option<i64>,
    },
}

pub(crate) fn raw_alter_sequence(sql: &str) -> Result<Option<RawAlterSequence>> {
    let normalized = normalize_sql(sql);
    let tokens = normalized.split_whitespace().collect::<Vec<_>>();
    if tokens.len() < 4 || tokens.first() != Some(&"alter") || tokens.get(1) != Some(&"sequence") {
        return Ok(None);
    }
    let sequence = normalize_sequence_name(tokens[2]);
    if tokens.get(3..5) == Some(&["owner", "to"][..]) {
        let Some(owner) = tokens.get(5) else {
            return Err(SqlError::InvalidSql(
                "ALTER SEQUENCE OWNER TO expects a role".to_string(),
            ));
        };
        if tokens.len() != 6 {
            return Err(SqlError::Unsupported(
                "ALTER SEQUENCE OWNER TO trailing options are not supported".to_string(),
            ));
        }
        return Ok(Some(RawAlterSequence::OwnerTo {
            sequence,
            owner: normalize_role_name(owner),
        }));
    }
    if tokens.get(3) == Some(&"restart") {
        let has_with = tokens.get(4) == Some(&"with");
        let value_index = if has_with { 5 } else { 4 };
        let value = tokens
            .get(value_index)
            .map(|value| {
                value.parse::<i64>().map_err(|_| {
                    SqlError::InvalidSql("ALTER SEQUENCE RESTART expects an integer".to_string())
                })
            })
            .transpose()?;
        if has_with && value.is_none() {
            return Err(SqlError::InvalidSql(
                "ALTER SEQUENCE RESTART WITH expects an integer".to_string(),
            ));
        }
        if tokens.len() > value_index + usize::from(value.is_some()) {
            return Err(SqlError::Unsupported(
                "ALTER SEQUENCE RESTART trailing options are not supported".to_string(),
            ));
        }
        return Ok(Some(RawAlterSequence::Restart { sequence, value }));
    }
    if tokens.get(3) != Some(&"owned") || tokens.get(4) != Some(&"by") {
        return Ok(None);
    }
    let Some(owner) = tokens.get(5) else {
        return Err(SqlError::InvalidSql(
            "ALTER SEQUENCE OWNED BY expects an owner".to_string(),
        ));
    };
    if *owner == "none" {
        return Ok(Some(RawAlterSequence::OwnedBy {
            sequence,
            owner: None,
        }));
    }
    let Some((table, column)) = owner.rsplit_once('.') else {
        return Err(SqlError::InvalidSql(
            "ALTER SEQUENCE OWNED BY expects table.column".to_string(),
        ));
    };
    Ok(Some(RawAlterSequence::OwnedBy {
        sequence,
        owner: Some((
            normalize_object_name(table),
            column.trim_matches('"').to_string(),
        )),
    }))
}

pub(crate) struct RawAlterIdentityRestart {
    pub(crate) table: String,
    pub(crate) column: String,
    pub(crate) value: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RawColumnCompression {
    pub(crate) column: String,
    pub(crate) compression: Option<char>,
}

pub(crate) struct RawCreateTableCompression {
    pub(crate) rewritten_sql: String,
    pub(crate) columns: Vec<RawColumnCompression>,
}

pub(crate) struct RawAlterColumnCompression {
    pub(crate) table: String,
    pub(crate) column: String,
    pub(crate) compression: Option<char>,
    pub(crate) if_exists: bool,
}

fn raw_compression_method(method: &str) -> Result<Option<char>> {
    match method.to_ascii_lowercase().as_str() {
        "default" => Ok(None),
        "pglz" => Ok(Some('p')),
        "lz4" => Ok(Some('l')),
        _ => Err(SqlError::invalid_parameter_value(format!(
            "invalid compression method \"{method}\""
        ))),
    }
}

fn normalized_raw_identifier(source: &str, parsed: String) -> String {
    if source.trim_start().starts_with('"') {
        parsed
    } else {
        parsed.to_ascii_lowercase()
    }
}

fn find_top_level_sql_keyword(value: &str, keyword: &str) -> Option<usize> {
    let bytes = value.as_bytes();
    let keyword = keyword.as_bytes();
    let mut depth = 0_i32;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut idx = 0;
    while idx < bytes.len() {
        let byte = bytes[idx];
        if in_single_quote {
            if byte == b'\'' {
                if bytes.get(idx + 1) == Some(&b'\'') {
                    idx += 2;
                    continue;
                }
                in_single_quote = false;
            }
            idx += 1;
            continue;
        }
        if in_double_quote {
            if byte == b'"' {
                if bytes.get(idx + 1) == Some(&b'"') {
                    idx += 2;
                    continue;
                }
                in_double_quote = false;
            }
            idx += 1;
            continue;
        }
        match byte {
            b'\'' => in_single_quote = true,
            b'"' => in_double_quote = true,
            b'(' | b'[' => depth += 1,
            b')' | b']' => depth = depth.saturating_sub(1),
            _ if depth == 0 && idx + keyword.len() <= bytes.len() => {
                let candidate = &bytes[idx..idx + keyword.len()];
                let before_boundary = idx == 0
                    || !matches!(bytes[idx - 1], b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_' | b'$');
                let after = idx + keyword.len();
                let after_boundary = after == bytes.len()
                    || !matches!(bytes[after], b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_' | b'$');
                if before_boundary && after_boundary && candidate.eq_ignore_ascii_case(keyword) {
                    return Some(idx);
                }
            }
            _ => {}
        }
        idx += 1;
    }
    None
}

pub(crate) fn raw_create_table_compression(sql: &str) -> Result<Option<RawCreateTableCompression>> {
    let statement = trim_sql_statement(sql);
    if strip_prefix_ci(statement, "CREATE TABLE ").is_none() {
        return Ok(None);
    }
    if split_sql_statements(statement).len() != 1 {
        return Ok(None);
    }
    let Some(open) = statement.find('(') else {
        return Ok(None);
    };
    let Some(close) = find_matching_paren_nested(statement, open) else {
        return Ok(None);
    };
    let body = &statement[open + 1..close];
    let mut rewritten_columns = Vec::new();
    let mut columns = Vec::new();
    for item in split_top_level_commas_nested(body) {
        let (column, after_column) = parse_leading_sql_identifier(&item)?;
        let column_prefix_len = item.len() - after_column.len();
        let Some(compression_idx) = find_top_level_sql_keyword(after_column, "COMPRESSION") else {
            rewritten_columns.push(item);
            continue;
        };
        let after_keyword = &after_column[compression_idx + "COMPRESSION".len()..];
        let (method, after_method) = parse_leading_sql_identifier(after_keyword)?;
        let method = normalized_raw_identifier(after_keyword, method);
        let compression = raw_compression_method(&method)?;
        let rewritten = format!(
            "{}{}{}",
            &item[..column_prefix_len],
            &after_column[..compression_idx],
            after_method
        );
        if find_top_level_sql_keyword(&rewritten[column_prefix_len..], "COMPRESSION").is_some() {
            return Err(SqlError::InvalidSql(format!(
                "multiple compression specifications for column \"{column}\""
            )));
        }
        columns.push(RawColumnCompression {
            column: normalized_raw_identifier(&item[..column_prefix_len], column),
            compression,
        });
        rewritten_columns.push(rewritten.trim().to_string());
    }
    if columns.is_empty() {
        return Ok(None);
    }
    let rewritten_sql = format!(
        "{}({}){}",
        &statement[..open],
        rewritten_columns.join(", "),
        &statement[close + 1..]
    );
    Ok(Some(RawCreateTableCompression {
        rewritten_sql,
        columns,
    }))
}

pub(crate) fn raw_alter_column_compression(sql: &str) -> Result<Option<RawAlterColumnCompression>> {
    let Some(mut rest) = strip_prefix_ci(trim_sql_statement(sql), "ALTER TABLE ") else {
        return Ok(None);
    };
    rest = rest.trim_start();
    let mut if_exists = false;
    if let Some(after_if_exists) = strip_prefix_ci(rest, "IF EXISTS ") {
        if_exists = true;
        rest = after_if_exists.trim_start();
    }
    if let Some(after_only) = strip_prefix_ci(rest, "ONLY ") {
        rest = after_only.trim_start();
    }
    let (target, after_target) = parse_leading_qualified_sql_identifier(rest)?;
    let Some(mut rest) = strip_prefix_ci(after_target.trim_start(), "ALTER ") else {
        return Ok(None);
    };
    rest = rest.trim_start();
    if let Some(after_column) = strip_prefix_ci(rest, "COLUMN ") {
        rest = after_column.trim_start();
    }
    let column_source = rest;
    let (column, after_column) = parse_leading_sql_identifier(rest)?;
    let Some(after_set) = strip_prefix_ci(after_column.trim_start(), "SET COMPRESSION ") else {
        return Ok(None);
    };
    let method_source = after_set.trim_start();
    let (method, trailing) = parse_leading_sql_identifier(method_source)?;
    if has_executable_sql(trailing) {
        return Err(SqlError::InvalidSql(
            "ALTER TABLE ALTER COLUMN SET COMPRESSION has trailing tokens".to_string(),
        ));
    }
    let method = normalized_raw_identifier(method_source, method);
    let (_, table) = identifier_schema_and_name(&target);
    Ok(Some(RawAlterColumnCompression {
        table: normalize_object_name(&table),
        column: normalized_raw_identifier(column_source, column),
        compression: raw_compression_method(&method)?,
        if_exists,
    }))
}

pub(crate) fn raw_alter_identity_restart(sql: &str) -> Result<Option<RawAlterIdentityRestart>> {
    let normalized = normalize_sql(sql);
    let tokens = normalized.split_whitespace().collect::<Vec<_>>();
    if tokens.first() != Some(&"alter") || tokens.get(1) != Some(&"table") {
        return Ok(None);
    }
    let mut idx = 2;
    if tokens.get(idx) == Some(&"only") {
        idx += 1;
    }
    let Some(table) = tokens.get(idx) else {
        return Ok(None);
    };
    idx += 1;
    if tokens.get(idx) != Some(&"alter") {
        return Ok(None);
    }
    idx += 1;
    if tokens.get(idx) == Some(&"column") {
        idx += 1;
    }
    let Some(column) = tokens.get(idx) else {
        return Ok(None);
    };
    idx += 1;
    if tokens.get(idx) != Some(&"restart") {
        return Ok(None);
    }
    idx += 1;
    if tokens.get(idx) == Some(&"with") {
        idx += 1;
    }
    let value = tokens
        .get(idx)
        .map(|value| {
            value.parse::<i64>().map_err(|_| {
                SqlError::InvalidSql(
                    "ALTER TABLE ALTER COLUMN RESTART expects an integer".to_string(),
                )
            })
        })
        .transpose()?;
    idx += usize::from(value.is_some());
    if idx != tokens.len() {
        return Err(SqlError::Unsupported(
            "ALTER TABLE ALTER COLUMN RESTART trailing options are not supported".to_string(),
        ));
    }
    Ok(Some(RawAlterIdentityRestart {
        table: normalize_object_name(table),
        column: column.trim_matches('"').to_string(),
        value,
    }))
}

/// `ALTER TABLE [ONLY] <t> ALTER [COLUMN] <c> ADD GENERATED {ALWAYS|BY
/// DEFAULT} AS IDENTITY [( SEQUENCE NAME <name> ... )]` — the standalone
/// identity attach pg_dump emits. sqlparser rejects the parenthesized
/// sequence options ("Expected: ), found: SEQUENCE"), and even parsed the
/// AlterColumn `AddGenerated` action is Unsupported, so this is handled
/// raw like `raw_alter_identity_restart`.
pub(crate) struct RawAlterAddIdentity {
    pub(crate) table: String,
    pub(crate) column: String,
    /// "a" = ALWAYS, "d" = BY DEFAULT.
    pub(crate) kind: String,
    /// The `SEQUENCE NAME <name>` from the options, if present (pg_dump
    /// pre-creates this sequence and later `setval`s it, so the identity
    /// must bind to exactly this name for restored values to line up).
    pub(crate) sequence_name: Option<String>,
}

pub(crate) fn raw_alter_add_identity(sql: &str) -> Result<Option<RawAlterAddIdentity>> {
    let normalized = normalize_sql(sql);
    let tokens = normalized.split_whitespace().collect::<Vec<_>>();
    if tokens.first() != Some(&"alter") || tokens.get(1) != Some(&"table") {
        return Ok(None);
    }
    let mut idx = 2;
    if tokens.get(idx) == Some(&"only") {
        idx += 1;
    }
    let Some(table) = tokens.get(idx).copied() else {
        return Ok(None);
    };
    idx += 1;
    if tokens.get(idx) != Some(&"alter") {
        return Ok(None);
    }
    idx += 1;
    if tokens.get(idx) == Some(&"column") {
        idx += 1;
    }
    let Some(column) = tokens.get(idx).copied() else {
        return Ok(None);
    };
    idx += 1;
    if tokens.get(idx) != Some(&"add") || tokens.get(idx + 1) != Some(&"generated") {
        return Ok(None);
    }
    idx += 2;
    let kind = match (tokens.get(idx).copied(), tokens.get(idx + 1).copied()) {
        (Some("always"), _) => {
            idx += 1;
            "a"
        }
        (Some("by"), Some("default")) => {
            idx += 2;
            "d"
        }
        _ => return Ok(None),
    };
    if tokens.get(idx) != Some(&"as") || tokens.get(idx + 1) != Some(&"identity") {
        return Ok(None);
    }
    idx += 2;
    // Optional `( SEQUENCE NAME <name> ... )` — scan for the sequence name;
    // the remaining sequence options are already reflected in the sequence
    // pg_dump created separately, so they are not re-parsed here.
    let mut sequence_name = None;
    let rest = &tokens[idx..];
    for window in rest.windows(3) {
        if window[0].trim_start_matches('(') == "sequence" && window[1] == "name" {
            let raw = window[2].trim_matches(|c| c == '(' || c == ')' || c == ',');
            if !raw.is_empty() {
                sequence_name = Some(normalize_object_name(raw));
            }
            break;
        }
    }
    Ok(Some(RawAlterAddIdentity {
        table: normalize_object_name(table),
        column: column.trim_matches('"').to_string(),
        kind: kind.to_string(),
        sequence_name,
    }))
}

pub(crate) enum RawAlterTablePartition {
    Attach {
        parent_table: String,
        parent_schema: String,
        partition_table: String,
        partition_schema: String,
        bound: String,
    },
    Detach {
        partition_table: String,
    },
}

pub(crate) struct RawCreatePartitionTable {
    pub(crate) partition_table: String,
    pub(crate) partition_schema: String,
    pub(crate) parent_table: String,
    pub(crate) parent_schema: String,
    pub(crate) bound: String,
    pub(crate) if_not_exists: bool,
}

pub(crate) struct RawCreateTableLike {
    pub(crate) table: String,
    pub(crate) schema_name: String,
    pub(crate) source_table: String,
    pub(crate) options: RawCreateTableLikeOptions,
    pub(crate) partitioning: Option<PartitioningSchema>,
    pub(crate) if_not_exists: bool,
}

pub(crate) struct RawAlterTableSetSchema {
    pub(crate) table: String,
    pub(crate) schema_name: String,
    pub(crate) if_exists: bool,
}

#[derive(Default)]
pub(crate) struct RawCreateTableLikeOptions {
    pub(crate) include_defaults: bool,
    pub(crate) include_constraints: bool,
    pub(crate) include_indexes: bool,
}

pub(crate) fn raw_alter_table_set_schema(sql: &str) -> Result<Option<RawAlterTableSetSchema>> {
    let Some(mut rest) = strip_prefix_ci(trim_sql_statement(sql), "ALTER TABLE ") else {
        return Ok(None);
    };
    rest = rest.trim_start();
    let mut if_exists = false;
    if let Some(after_if_exists) = strip_prefix_ci(rest, "IF EXISTS ") {
        if_exists = true;
        rest = after_if_exists.trim_start();
    }

    let (target, after_target) = parse_leading_qualified_sql_identifier(rest)?;
    let Some(after_set_schema) = strip_prefix_ci(after_target.trim_start(), "SET SCHEMA ") else {
        return Ok(None);
    };
    let (schema, after_schema) = parse_leading_qualified_sql_identifier(after_set_schema)?;
    if has_executable_sql(after_schema) {
        return Err(SqlError::InvalidSql(
            "ALTER TABLE SET SCHEMA has trailing tokens".to_string(),
        ));
    }

    let (_, table) = identifier_schema_and_name(&target);
    let (_, schema_name) = identifier_schema_and_name(&schema);
    Ok(Some(RawAlterTableSetSchema {
        table: normalize_object_name(&table),
        schema_name: normalize_object_name(&schema_name),
        if_exists,
    }))
}

pub(crate) fn raw_create_table_like(sql: &str) -> Result<Option<RawCreateTableLike>> {
    let Some(mut rest) = strip_prefix_ci(trim_sql_statement(sql), "CREATE TABLE ") else {
        return Ok(None);
    };
    rest = rest.trim_start();
    let mut if_not_exists = false;
    if let Some(after_if_not_exists) = strip_prefix_ci(rest, "IF NOT EXISTS ") {
        if_not_exists = true;
        rest = after_if_not_exists.trim_start();
    }

    let (target, after_target) = parse_leading_qualified_sql_identifier(rest)?;
    let mut rest = after_target.trim_start();
    let Some(after_open) = rest.strip_prefix('(') else {
        return Ok(None);
    };
    let close = find_matching_closing_paren(after_open).ok_or_else(|| {
        SqlError::InvalidSql("unterminated CREATE TABLE LIKE column list".to_string())
    })?;
    let body = after_open[..close].trim();
    let Some(body) = strip_prefix_ci(body, "LIKE ") else {
        return Ok(None);
    };
    let (source, options_sql) = parse_leading_qualified_sql_identifier(body)?;
    let options = raw_create_table_like_options(options_sql)?;
    rest = after_open[close + 1..].trim_start();
    let partitioning = raw_partition_by_clause(rest)?;

    let (schema_name, table) = identifier_schema_and_name(&target);
    let (_, source_table) = identifier_schema_and_name(&source);
    Ok(Some(RawCreateTableLike {
        table,
        schema_name,
        source_table,
        options,
        partitioning,
        if_not_exists,
    }))
}

pub(crate) fn raw_create_table_like_options(sql: &str) -> Result<RawCreateTableLikeOptions> {
    let mut options = RawCreateTableLikeOptions::default();
    let tokens = sql
        .split_whitespace()
        .map(|token| token.trim_matches(',').to_ascii_lowercase())
        .filter(|token| !token.is_empty())
        .collect::<Vec<_>>();
    let mut idx = 0;
    while idx < tokens.len() {
        let include = match tokens[idx].as_str() {
            "including" => true,
            "excluding" => false,
            other => {
                return Err(SqlError::Unsupported(format!(
                    "CREATE TABLE LIKE option `{other}` is not supported"
                )))
            }
        };
        idx += 1;
        let Some(option) = tokens.get(idx).map(String::as_str) else {
            return Err(SqlError::InvalidSql(
                "CREATE TABLE LIKE expects an option after INCLUDING/EXCLUDING".to_string(),
            ));
        };
        match option {
            "all" => {
                options.include_defaults = include;
                options.include_constraints = include;
                options.include_indexes = include;
            }
            "defaults" | "identity" | "generated" => options.include_defaults = include,
            "constraints" => options.include_constraints = include,
            "indexes" => options.include_indexes = include,
            other => {
                return Err(SqlError::Unsupported(format!(
                    "CREATE TABLE LIKE option `{other}` is not supported"
                )))
            }
        }
        idx += 1;
    }
    Ok(options)
}

pub(crate) fn raw_partition_by_clause(sql: &str) -> Result<Option<PartitioningSchema>> {
    let sql = trim_sql_trailing_comments(sql);
    if sql.is_empty() {
        return Ok(None);
    }
    let Some(rest) = strip_prefix_ci(sql, "PARTITION BY ") else {
        return Err(SqlError::Unsupported(format!(
            "unsupported trailing CREATE TABLE LIKE clause `{sql}`"
        )));
    };
    let rest = rest.trim_start();
    let strategy_end = rest
        .char_indices()
        .find(|(_, ch)| !is_ident_char(*ch))
        .map(|(idx, _)| idx)
        .unwrap_or(rest.len());
    let strategy = rest[..strategy_end].to_ascii_lowercase();
    if !matches!(strategy.as_str(), "range" | "list" | "hash") {
        return Err(SqlError::Unsupported(format!(
            "unsupported PARTITION BY strategy {strategy}"
        )));
    }
    let rest = rest[strategy_end..].trim_start();
    let Some(after_open) = rest.strip_prefix('(') else {
        return Err(SqlError::InvalidSql(
            "PARTITION BY expects a parenthesized key list".to_string(),
        ));
    };
    let close = find_matching_closing_paren(after_open)
        .ok_or_else(|| SqlError::InvalidSql("unterminated PARTITION BY key list".to_string()))?;
    let trailing = trim_sql_trailing_comments(&after_open[close + 1..]);
    if !trailing.is_empty() {
        return Err(SqlError::Unsupported(format!(
            "unsupported trailing PARTITION BY clause `{trailing}`"
        )));
    }
    let key_columns = after_open[..close]
        .split(',')
        .map(|part| part.trim().trim_matches('"').to_string())
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if key_columns.is_empty() {
        return Err(SqlError::InvalidSql(
            "PARTITION BY expects at least one key column".to_string(),
        ));
    }
    Ok(Some(PartitioningSchema {
        strategy,
        key_columns,
    }))
}

pub(crate) fn trim_sql_trailing_comments(mut sql: &str) -> &str {
    loop {
        sql = sql.trim();
        if let Some(rest) = sql.strip_prefix("/*") {
            if let Some(end) = rest.find("*/") {
                sql = &rest[end + 2..];
                continue;
            }
        }
        if let Some(rest) = sql.strip_prefix("--") {
            if let Some(newline) = rest.find('\n') {
                sql = &rest[newline + 1..];
                continue;
            }
            return "";
        }
        return sql;
    }
}

pub(crate) fn copied_like_index_name(
    index_name: &str,
    source_table: &str,
    target_table: &str,
) -> String {
    index_name
        .strip_prefix(source_table)
        .map(|suffix| format!("{target_table}{suffix}"))
        .unwrap_or_else(|| format!("{target_table}_{index_name}"))
}

pub(crate) fn raw_create_partition_table(sql: &str) -> Result<Option<RawCreatePartitionTable>> {
    let Some(mut rest) = strip_prefix_ci(trim_sql_statement(sql), "CREATE TABLE ") else {
        return Ok(None);
    };
    rest = rest.trim_start();
    let mut if_not_exists = false;
    if let Some(after_if_not_exists) = strip_prefix_ci(rest, "IF NOT EXISTS ") {
        if_not_exists = true;
        rest = after_if_not_exists.trim_start();
    }

    let (partition, after_partition) = parse_leading_qualified_sql_identifier(rest)?;
    let Some(rest) = strip_prefix_ci(after_partition.trim_start(), "PARTITION OF ") else {
        return Ok(None);
    };
    let (parent, after_parent) = parse_leading_qualified_sql_identifier(rest)?;
    let bound = after_parent.trim();
    if !bound
        .get(.."FOR VALUES".len())
        .is_some_and(|head| head.eq_ignore_ascii_case("FOR VALUES"))
    {
        return Err(SqlError::InvalidSql(
            "CREATE TABLE PARTITION OF expects FOR VALUES".to_string(),
        ));
    }

    let (partition_schema, partition_table) = identifier_schema_and_name(&partition);
    let (parent_schema, parent_table) = identifier_schema_and_name(&parent);
    let partition_schema = normalize_object_name(&partition_schema);
    let parent_schema = normalize_object_name(&parent_schema);
    let partition_table = relation_name_from_parts(&[
        partition_schema.clone(),
        normalize_object_name(&partition_table),
    ])?;
    let parent_table =
        relation_name_from_parts(&[parent_schema.clone(), normalize_object_name(&parent_table)])?;
    Ok(Some(RawCreatePartitionTable {
        partition_table,
        partition_schema,
        parent_table,
        parent_schema,
        bound: bound.to_string(),
        if_not_exists,
    }))
}

pub(crate) fn raw_alter_table_partition_action(
    sql: &str,
) -> Result<Option<RawAlterTablePartition>> {
    let Some(mut rest) = strip_prefix_ci(trim_sql_statement(sql), "ALTER TABLE ") else {
        return Ok(None);
    };
    rest = rest.trim_start();
    if let Some(after_only) = strip_prefix_ci(rest, "ONLY ") {
        rest = after_only.trim_start();
    }
    let (parent, after_parent) = parse_leading_qualified_sql_identifier(rest)?;
    let (parent_schema, parent_table) = identifier_schema_and_name(&parent);
    let parent_schema = normalize_object_name(&parent_schema);
    let parent_table =
        relation_name_from_parts(&[parent_schema.clone(), normalize_object_name(&parent_table)])?;
    let rest = after_parent.trim_start();
    if let Some(after_attach) = strip_prefix_ci(rest, "ATTACH PARTITION ") {
        let (partition, after_partition) = parse_leading_qualified_sql_identifier(after_attach)?;
        let (partition_schema, partition_table) = identifier_schema_and_name(&partition);
        let partition_schema = normalize_object_name(&partition_schema);
        let partition_table = relation_name_from_parts(&[
            partition_schema.clone(),
            normalize_object_name(&partition_table),
        ])?;
        let bound = after_partition.trim();
        if !bound
            .get(.."FOR VALUES".len())
            .is_some_and(|head| head.eq_ignore_ascii_case("FOR VALUES"))
        {
            return Err(SqlError::InvalidSql(
                "ALTER TABLE ATTACH PARTITION expects FOR VALUES".to_string(),
            ));
        }
        return Ok(Some(RawAlterTablePartition::Attach {
            parent_table,
            parent_schema,
            partition_table,
            partition_schema,
            bound: bound.to_string(),
        }));
    }
    if let Some(after_detach) = strip_prefix_ci(rest, "DETACH PARTITION ") {
        let (partition, trailing) = parse_leading_qualified_sql_identifier(after_detach)?;
        if !trailing.trim().is_empty() {
            return Err(SqlError::InvalidSql(format!(
                "unexpected trailing ALTER TABLE DETACH PARTITION clause: {}",
                trailing.trim()
            )));
        }
        let (partition_schema, partition_table) = identifier_schema_and_name(&partition);
        let partition_table = relation_name_from_parts(&[
            normalize_object_name(&partition_schema),
            normalize_object_name(&partition_table),
        ])?;
        return Ok(Some(RawAlterTablePartition::Detach { partition_table }));
    }
    Ok(None)
}

pub(crate) fn raw_partition_ddl_command_tag(sql: &str) -> Result<Option<&'static str>> {
    let statement = trim_sql_statement(sql);
    let normalized = normalize_sql(statement);
    if normalized.starts_with("alter table ")
        && (normalized.contains(" attach partition ") || normalized.contains(" detach partition "))
    {
        return Ok(Some("ALTER TABLE"));
    }

    let Some(rest) = strip_prefix_ci(statement, "ALTER INDEX ") else {
        return Ok(None);
    };
    let (parent, rest) = parse_leading_policy_identifier(rest)?;
    let Some(rest) = strip_prefix_ci(rest.trim_start(), "ATTACH PARTITION ") else {
        return Ok(None);
    };
    let (partition, trailing) = parse_leading_policy_identifier(rest)?;
    if !trailing.trim().is_empty() {
        return Err(SqlError::InvalidSql(format!(
            "unexpected trailing ALTER INDEX ATTACH PARTITION clause: {}",
            trailing.trim()
        )));
    }
    if parent.trim().is_empty() || partition.trim().is_empty() {
        return Err(SqlError::InvalidSql(
            "ALTER INDEX ATTACH PARTITION expects parent and partition index names".to_string(),
        ));
    }
    Ok(Some("ALTER INDEX"))
}

pub(crate) fn raw_alter_table_exclusion_constraint(
    sql: &str,
) -> Result<Option<(String, ConstraintSchema)>> {
    let Some(mut rest) = strip_prefix_ci(trim_sql_statement(sql), "ALTER TABLE ") else {
        return Ok(None);
    };
    rest = rest.trim_start();
    if let Some(after_only) = strip_prefix_ci(rest, "ONLY ") {
        rest = after_only.trim_start();
    }
    let (table, after_table) = parse_leading_policy_identifier(rest)?;
    let table = normalize_object_name(&table);
    let Some(rest) = strip_prefix_ci(after_table.trim_start(), "ADD CONSTRAINT ") else {
        return Ok(None);
    };
    let (name, rest) = parse_leading_policy_identifier(rest)?;
    let Some(mut rest) = strip_prefix_ci(rest.trim_start(), "EXCLUDE ") else {
        return Ok(None);
    };
    rest = rest.trim_start();
    let access_method = if let Some(after_using) = strip_prefix_ci(rest, "USING ") {
        let (method, after_method) = parse_leading_policy_identifier(after_using)?;
        rest = after_method.trim_start();
        normalize_object_name(&method)
    } else {
        "gist".to_string()
    };
    if !matches!(access_method.as_str(), "gist" | "spgist") {
        return Err(SqlError::undefined_object(format!(
            "access method \"{access_method}\" does not support exclusion constraints"
        )));
    }
    if !rest.starts_with('(') {
        return Err(SqlError::InvalidSql(
            "ALTER TABLE EXCLUDE expects a constraint element list".to_string(),
        ));
    }
    let close = find_matching_paren_nested(rest, 0).ok_or_else(|| {
        SqlError::InvalidSql("unterminated exclusion constraint element list".to_string())
    })?;
    let (equal_columns, range) = parse_exclusion_elements(&rest[1..close])?;
    let predicate = exclusion_constraint_predicate(&rest[close + 1..]);
    Ok(Some((
        table,
        ConstraintSchema::Exclusion {
            name,
            access_method,
            equal_columns,
            range,
            predicate,
            validated: true,
        },
    )))
}

pub(crate) fn parse_exclusion_elements(
    elements: &str,
) -> Result<(Vec<String>, Option<ExclusionRangeSchema>)> {
    let mut equal_columns = Vec::new();
    let mut range = None;
    for element in split_top_level_commas_nested(elements) {
        let Some(with_idx) = find_top_level_keyword(&element, "WITH") else {
            return Err(SqlError::InvalidSql(format!(
                "exclusion constraint element {element} is missing WITH operator"
            )));
        };
        let expression = element[..with_idx].trim();
        let operator = element[with_idx + "WITH".len()..]
            .split_whitespace()
            .next()
            .ok_or_else(|| {
                SqlError::InvalidSql(format!(
                    "exclusion constraint element {element} is missing an operator"
                ))
            })?;
        match operator {
            "=" => equal_columns.push(simple_exclusion_column(expression)?),
            "&&" | "-|-" => {
                if range.is_some() {
                    return Err(SqlError::Unsupported(
                        "exclusion constraints with multiple range overlap elements are not supported"
                            .to_string(),
                    ));
                }
                let mut parsed = parse_exclusion_range_expression(expression)?;
                parsed.operator = operator.to_string();
                range = Some(parsed);
            }
            "@>" | "<@" | "<<" | ">>" | "&<" | "&>" | "<>" => {
                return Err(SqlError::data_exception_public(
                    "42809",
                    format!("operator {operator} is not valid for this exclusion constraint"),
                    None,
                ));
            }
            other => {
                return Err(SqlError::Unsupported(format!(
                    "exclusion operator {other} is not supported"
                )))
            }
        }
    }
    Ok((equal_columns, range))
}

pub(crate) fn parse_raw_role_membership_ddl(sql: &str) -> Result<Option<RawRoleMembershipDdl>> {
    let statement = trim_sql_statement(sql);
    if let Some(rest) = strip_prefix_ci(statement, "GRANT ") {
        if find_top_level_keyword(rest, "ON").is_some() {
            return Ok(None);
        }
        let Some(to_idx) = find_top_level_keyword(rest, "TO") else {
            return Ok(None);
        };
        let roles = parse_role_membership_identifiers(&rest[..to_idx])?;
        let mut tail = rest[to_idx + "TO".len()..].trim();
        let mut admin_option = None;
        let mut inherit_option = None;
        let mut set_option = None;
        if let Some(with_idx) = find_top_level_keyword(tail, "WITH") {
            let options = tail[with_idx + 4..].trim();
            if options.eq_ignore_ascii_case("ADMIN OPTION") {
                admin_option = Some(true);
            } else {
                for option in split_top_level_commas_nested(options) {
                    let words = option.split_whitespace().collect::<Vec<_>>();
                    let [name, value] = words.as_slice() else {
                        return Err(SqlError::InvalidSql(
                            "membership options require a name and boolean".into(),
                        ));
                    };
                    let value = match value.to_ascii_lowercase().as_str() {
                        "true" | "option" => true,
                        "false" => false,
                        _ => {
                            return Err(SqlError::InvalidSql(
                                "membership option must be TRUE or FALSE".into(),
                            ))
                        }
                    };
                    let target = match name.to_ascii_lowercase().as_str() {
                        "admin" => &mut admin_option,
                        "inherit" => &mut inherit_option,
                        "set" => &mut set_option,
                        _ => {
                            return Err(SqlError::Unsupported(format!(
                                "unknown membership option {name}"
                            )))
                        }
                    };
                    if target.replace(value).is_some() {
                        return Err(SqlError::InvalidSql(format!(
                            "duplicate membership option {name}"
                        )));
                    }
                }
            }
            tail = tail[..with_idx].trim();
        }
        if find_top_level_keyword(tail, "GRANTED").is_some() {
            return Err(SqlError::Unsupported(
                "GRANTED BY role membership grants are not supported".to_string(),
            ));
        }
        let members = parse_role_membership_identifiers(tail)?;
        return Ok(Some(RawRoleMembershipDdl::Grant {
            roles,
            members,
            admin_option,
            inherit_option,
            set_option,
        }));
    }

    if let Some(mut rest) = strip_prefix_ci(statement, "REVOKE ") {
        if let Some(after_admin) = strip_prefix_ci(rest.trim_start(), "ADMIN OPTION FOR ") {
            rest = after_admin;
        }
        if find_top_level_keyword(rest, "ON").is_some() {
            return Ok(None);
        }
        let Some(from_idx) = find_top_level_keyword(rest, "FROM") else {
            return Ok(None);
        };
        let roles = parse_role_membership_identifiers(&rest[..from_idx])?;
        let mut tail = rest[from_idx + "FROM".len()..].trim();
        for keyword in ["GRANTED", "CASCADE", "RESTRICT"] {
            if let Some(idx) = find_top_level_keyword(tail, keyword) {
                let option = tail[idx..].trim();
                if keyword.eq_ignore_ascii_case("RESTRICT") {
                    tail = tail[..idx].trim();
                    continue;
                }
                return Err(SqlError::Unsupported(format!(
                    "role membership revoke option {option} is not supported"
                )));
            }
        }
        let members = parse_role_membership_identifiers(tail)?;
        return Ok(Some(RawRoleMembershipDdl::Revoke { roles, members }));
    }

    Ok(None)
}

pub fn is_role_membership_ddl(sql: &str) -> bool {
    matches!(parse_raw_role_membership_ddl(sql), Ok(Some(_)))
}

pub fn parse_database_ddl(sql: &str) -> Result<Option<DatabaseDdl>> {
    let statement = trim_sql_statement(sql);
    if let Some(rest) = strip_prefix_ci(statement, "CREATE DATABASE ") {
        let (name, mut rest) = parse_leading_sql_identifier(rest)?;
        let mut owner = None;
        rest = rest.trim_start();
        if let Some(after_with) = strip_prefix_ci(rest, "WITH ") {
            rest = after_with.trim_start();
        }
        while !rest.is_empty() {
            if let Some(after_owner) = strip_prefix_ci(rest, "OWNER") {
                let after_owner = after_owner.trim_start();
                let after_owner = after_owner
                    .strip_prefix('=')
                    .unwrap_or(after_owner)
                    .trim_start();
                let (owner_name, trailing) = parse_leading_sql_identifier(after_owner)?;
                owner = Some(role_spec_name(&owner_name));
                rest = trailing.trim_start();
                continue;
            }
            if let Some(after_template) = strip_prefix_ci(rest, "TEMPLATE") {
                let (_, trailing) = parse_leading_sql_identifier(
                    after_template
                        .trim_start()
                        .strip_prefix('=')
                        .unwrap_or(after_template.trim_start())
                        .trim_start(),
                )?;
                rest = trailing.trim_start();
                continue;
            }
            if let Some(after_encoding) = strip_prefix_ci(rest, "ENCODING") {
                let (_, trailing) = parse_leading_sql_identifier(
                    after_encoding
                        .trim_start()
                        .strip_prefix('=')
                        .unwrap_or(after_encoding.trim_start())
                        .trim_start(),
                )?;
                rest = trailing.trim_start();
                continue;
            }
            return Err(SqlError::Unsupported(format!(
                "CREATE DATABASE option {rest} is not supported"
            )));
        }
        return Ok(Some(DatabaseDdl::Create {
            name: normalize_database_name(&name),
            owner,
        }));
    }

    if let Some(rest) = strip_prefix_ci(statement, "ALTER DATABASE ") {
        let (name, rest) = parse_leading_sql_identifier(rest)?;
        let rest = rest.trim_start();
        if let Some(after_owner) = strip_prefix_ci(rest, "OWNER TO ") {
            let (owner, trailing) = parse_leading_sql_identifier(after_owner)?;
            if !trailing.trim().is_empty() {
                return Err(SqlError::InvalidSql(format!(
                    "unexpected ALTER DATABASE OWNER tail: {}",
                    trailing.trim()
                )));
            }
            return Ok(Some(DatabaseDdl::AlterOwner {
                name: normalize_database_name(&name),
                owner: role_spec_name(&owner),
            }));
        }
        if let Some(after_set) = strip_prefix_ci(rest, "SET TABLESPACE ") {
            let (_, trailing) = parse_leading_sql_identifier(after_set)?;
            if !trailing.trim().is_empty() {
                return Err(SqlError::InvalidSql(format!(
                    "unexpected ALTER DATABASE SET TABLESPACE tail: {}",
                    trailing.trim()
                )));
            }
            return Ok(Some(DatabaseDdl::SetTablespace {
                name: normalize_database_name(&name),
            }));
        }
        return Err(SqlError::Unsupported(format!(
            "ALTER DATABASE option {rest} is not supported"
        )));
    }

    Ok(None)
}

pub(crate) fn parse_raw_alter_schema_owner(sql: &str) -> Result<Option<(String, String)>> {
    let statement = trim_sql_statement(sql);
    let Some(rest) = strip_prefix_ci(statement, "ALTER SCHEMA ") else {
        return Ok(None);
    };
    let (schema, rest) = parse_leading_sql_identifier(rest)?;
    let Some(rest) = strip_prefix_ci(rest.trim_start(), "OWNER TO ") else {
        return Ok(None);
    };
    let (owner, trailing) = parse_leading_sql_identifier(rest)?;
    if !trailing.trim().is_empty() {
        return Err(SqlError::InvalidSql(format!(
            "unexpected ALTER SCHEMA OWNER tail: {}",
            trailing.trim()
        )));
    }
    Ok(Some((schema, role_spec_name(&owner))))
}

pub(crate) fn parse_raw_alter_view_owner(sql: &str) -> Result<Option<(String, String)>> {
    let statement = trim_sql_statement(sql);
    let Some(rest) = strip_prefix_ci(statement, "ALTER VIEW ") else {
        return Ok(None);
    };
    let (view, rest) = parse_leading_qualified_sql_identifier(rest)?;
    let Some(rest) = strip_prefix_ci(rest.trim_start(), "OWNER TO ") else {
        return Ok(None);
    };
    let (owner, trailing) = parse_leading_sql_identifier(rest)?;
    if !trailing.trim().is_empty() {
        return Err(SqlError::InvalidSql(format!(
            "unexpected ALTER VIEW OWNER tail: {}",
            trailing.trim()
        )));
    }
    let (_, view) = identifier_schema_and_name(&view);
    Ok(Some((view, role_spec_name(&owner))))
}

pub(crate) fn normalize_pg_dump_range_zero_options(sql: &str) -> Result<Option<String>> {
    let statement = trim_sql_statement(sql);
    if strip_prefix_ci(statement, "CREATE TYPE ").is_none() {
        return Ok(None);
    }
    let Some(range_idx) = find_top_level_keyword(statement, "AS RANGE") else {
        return Ok(None);
    };
    let after_range = &statement[range_idx + "AS RANGE".len()..];
    let Some(relative_open) = after_range.find('(') else {
        return Ok(None);
    };
    if !after_range[..relative_open].trim().is_empty() {
        return Ok(None);
    }
    let open = range_idx + "AS RANGE".len() + relative_open;
    let close = find_matching_paren_nested(statement, open).ok_or_else(|| {
        SqlError::InvalidSql("unterminated CREATE TYPE AS RANGE options".to_string())
    })?;
    if !statement[close + 1..].trim().is_empty() {
        return Ok(None);
    }

    let mut changed = false;
    let options = split_top_level_commas_nested(&statement[open + 1..close])
        .into_iter()
        .filter(|option| {
            let Some((name, value)) = option.split_once('=') else {
                return true;
            };
            let zero_function = matches!(
                name.trim().to_ascii_lowercase().as_str(),
                "canonical" | "subtype_diff"
            ) && value.trim() == "0";
            changed |= zero_function;
            !zero_function
        })
        .collect::<Vec<_>>();
    if !changed {
        return Ok(None);
    }
    Ok(Some(format!(
        "{}({})",
        &statement[..open],
        options.join(", ")
    )))
}

pub(crate) fn parse_raw_create_extension(sql: &str) -> Result<Option<(ExtensionSchema, bool)>> {
    // The raw parser owns one statement.  Without this guard a simple-query
    // batch beginning with CREATE EXTENSION greedily consumed every following
    // statement as extension options, so a valid BicDB application migration such as
    // `CREATE EXTENSION ...; ALTER DEFAULT PRIVILEGES ...;` failed even though
    // both statements work independently.
    if split_sql_statements(sql).len() != 1 {
        return Ok(None);
    }
    let statement = trim_sql_statement(sql);
    let Some(mut rest) = strip_prefix_ci(statement, "CREATE EXTENSION ") else {
        return Ok(None);
    };
    rest = rest.trim_start();
    let if_not_exists = if let Some(after_if_not_exists) = strip_prefix_ci(rest, "IF NOT EXISTS ") {
        rest = after_if_not_exists.trim_start();
        true
    } else {
        false
    };
    let words = split_sql_words(rest);
    let Some(name_word) = words.first() else {
        return Err(SqlError::InvalidSql(
            "CREATE EXTENSION requires an extension name".to_string(),
        ));
    };
    let mut extension = ExtensionSchema {
        name: unquote_identifier_word(name_word),
        schema: "public".to_string(),
        version: None,
    };
    let mut idx = 1;
    while idx < words.len() {
        match words[idx].to_ascii_uppercase().as_str() {
            "WITH" => {
                idx += 1;
            }
            "SCHEMA" => {
                let Some(schema) = words.get(idx + 1) else {
                    return Err(SqlError::InvalidSql(
                        "CREATE EXTENSION SCHEMA requires a schema name".to_string(),
                    ));
                };
                extension.schema = unquote_identifier_word(schema);
                idx += 2;
            }
            "VERSION" => {
                let Some(version) = words.get(idx + 1) else {
                    return Err(SqlError::InvalidSql(
                        "CREATE EXTENSION VERSION requires a version".to_string(),
                    ));
                };
                extension.version = Some(
                    unquote_identifier_word(version)
                        .trim_matches('\'')
                        .to_string(),
                );
                idx += 2;
            }
            "CASCADE" => {
                idx += 1;
            }
            other => {
                return Err(SqlError::InvalidSql(format!(
                    "unsupported CREATE EXTENSION option {other}"
                )));
            }
        }
    }
    Ok(Some((extension, if_not_exists)))
}

pub(crate) fn parse_role_membership_identifiers(value: &str) -> Result<Vec<String>> {
    let mut identifiers = Vec::new();
    for item in split_top_level_commas_nested(value) {
        if item.trim().is_empty() {
            continue;
        }
        let (identifier, trailing) = parse_leading_sql_identifier(&item)?;
        if !trailing.trim().is_empty() {
            return Err(SqlError::InvalidSql(format!(
                "unexpected role membership identifier tail: {}",
                trailing.trim()
            )));
        }
        // Keep unquoted SQL identity keywords until execution, when the
        // session's effective role is known. Quoted names remain literal roles.
        let keyword = item.trim().to_ascii_uppercase();
        identifiers.push(
            if matches!(
                keyword.as_str(),
                "CURRENT_USER" | "CURRENT_ROLE" | "SESSION_USER"
            ) {
                keyword
            } else {
                normalize_role_name(&identifier)
            },
        );
    }
    if identifiers.is_empty() {
        return Err(SqlError::InvalidSql(
            "role membership statement expects at least one role".to_string(),
        ));
    }
    Ok(identifiers)
}

pub(crate) fn role_spec_name(name: &str) -> String {
    match name.to_ascii_uppercase().as_str() {
        "CURRENT_ROLE" | "CURRENT_USER" | "SESSION_USER" => current_role_name(),
        _ => normalize_role_name(name),
    }
}

pub(crate) fn parse_exclusion_range_expression(expression: &str) -> Result<ExclusionRangeSchema> {
    let expression = strip_balanced_outer_parens(expression.trim());
    let Some(open) = expression.find('(') else {
        let column = simple_exclusion_column(expression)?;
        return Ok(ExclusionRangeSchema {
            function: "range".to_string(),
            operator: default_exclusion_range_operator(),
            start_column: column.clone(),
            end_column: column.clone(),
            bounds: "[)".to_string(),
            range_column: Some(column),
        });
    };
    let function = normalize_object_name(expression[..open].trim());
    if !is_builtin_range_type(&function) {
        return Err(SqlError::Unsupported(format!(
            "exclusion range function {function} is not supported"
        )));
    }
    let close = find_matching_paren_nested(expression, open).ok_or_else(|| {
        SqlError::InvalidSql(format!(
            "unterminated exclusion range expression {expression}"
        ))
    })?;
    if !expression[close + 1..].trim().is_empty() {
        return Err(SqlError::Unsupported(format!(
            "exclusion range expression {expression} is not supported"
        )));
    }
    let args = split_top_level_commas_nested(&expression[open + 1..close]);
    if args.len() < 2 {
        return Err(SqlError::InvalidSql(format!(
            "exclusion range function {function} expects at least start and end arguments"
        )));
    }
    Ok(ExclusionRangeSchema {
        function,
        operator: default_exclusion_range_operator(),
        start_column: simple_exclusion_column(&args[0])?,
        end_column: simple_exclusion_column(&args[1])?,
        bounds: args
            .get(2)
            .and_then(|arg| sql_string_literal_prefix(arg))
            .unwrap_or_else(|| "[)".to_string()),
        range_column: None,
    })
}

pub(crate) fn simple_exclusion_column(expression: &str) -> Result<String> {
    let expression = strip_balanced_outer_parens(expression.trim());
    let expression = expression
        .split_once("::")
        .map(|(base, _)| base.trim())
        .unwrap_or(expression);
    let expression = expression.rsplit('.').next().unwrap_or(expression).trim();
    let column = expression.trim_matches('"');
    if column.is_empty() || !column.chars().all(|ch| ch.is_alphanumeric() || ch == '_') {
        return Err(SqlError::Unsupported(format!(
            "exclusion expression {expression} is not a simple column"
        )));
    }
    Ok(column.to_string())
}

pub(crate) fn exclusion_constraint_predicate(tail: &str) -> Option<String> {
    let where_idx = find_top_level_keyword(tail, "WHERE")?;
    let predicate = tail[where_idx + "WHERE".len()..].trim();
    let end = ["DEFERRABLE", "INITIALLY"]
        .into_iter()
        .filter_map(|keyword| find_top_level_keyword(predicate, keyword))
        .min()
        .unwrap_or(predicate.len());
    let predicate = predicate[..end].trim();
    (!predicate.is_empty()).then(|| predicate.to_string())
}

pub(crate) fn sql_string_literal_prefix(value: &str) -> Option<String> {
    let value = value.trim();
    let rest = value.strip_prefix('\'')?;
    let mut result = String::new();
    let mut chars = rest.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\'' {
            if chars.peek() == Some(&'\'') {
                chars.next();
                result.push('\'');
                continue;
            }
            return Some(result);
        }
        result.push(ch);
    }
    None
}

pub(crate) fn strip_balanced_outer_parens(mut value: &str) -> &str {
    loop {
        let trimmed = value.trim();
        if !trimmed.starts_with('(') {
            return trimmed;
        }
        let Some(close) = find_matching_paren_nested(trimmed, 0) else {
            return trimmed;
        };
        if close != trimmed.len() - 1 {
            return trimmed;
        }
        value = &trimmed[1..close];
    }
}

pub(crate) fn split_top_level_commas_nested(value: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut depth = 0_i32;
    let mut in_string = false;
    let bytes = value.as_bytes();
    let mut idx = 0;
    while idx < bytes.len() {
        let byte = bytes[idx];
        if in_string {
            if byte == b'\'' {
                if bytes.get(idx + 1) == Some(&b'\'') {
                    idx += 2;
                    continue;
                }
                in_string = false;
            }
            idx += 1;
            continue;
        }
        match byte {
            b'\'' => in_string = true,
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            b',' if depth == 0 => {
                parts.push(value[start..idx].trim().to_string());
                start = idx + 1;
            }
            _ => {}
        }
        idx += 1;
    }
    parts.push(value[start..].trim().to_string());
    parts
}

pub(crate) fn find_matching_paren_nested(value: &str, open: usize) -> Option<usize> {
    if value.as_bytes().get(open) != Some(&b'(') {
        return None;
    }
    let bytes = value.as_bytes();
    let mut depth = 0_i32;
    let mut in_string = false;
    let mut idx = open;
    while idx < bytes.len() {
        let byte = bytes[idx];
        if in_string {
            if byte == b'\'' {
                if bytes.get(idx + 1) == Some(&b'\'') {
                    idx += 2;
                    continue;
                }
                in_string = false;
            }
            idx += 1;
            continue;
        }
        match byte {
            b'\'' => in_string = true,
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(idx);
                }
            }
            _ => {}
        }
        idx += 1;
    }
    None
}

pub(crate) fn sequence_option_i64(expr: &Expr, label: &str) -> Result<i64> {
    let SqlValue::Int(value) = eval_constant_expr(expr)? else {
        return Err(SqlError::InvalidSql(format!(
            "CREATE SEQUENCE {label} expects an integer"
        )));
    };
    Ok(value)
}

pub(crate) fn sequence_name_for_column(table: &str, column: &str) -> String {
    format!("{table}_{column}_seq")
}

pub(crate) fn normalize_sequence_name(sequence: &str) -> String {
    let name = sequence.rsplit('.').next().unwrap_or(sequence);
    if name.starts_with('"') && name.ends_with('"') && name.len() >= 2 {
        name[1..name.len() - 1].replace("\"\"", "\"")
    } else {
        name.to_ascii_lowercase()
    }
}

pub(crate) fn is_sequence_function(function: &Function) -> Result<bool> {
    Ok(matches!(
        object_name(&function.name)?.to_ascii_lowercase().as_str(),
        "nextval"
            | "pg_catalog.nextval"
            | "currval"
            | "pg_catalog.currval"
            | "lastval"
            | "pg_catalog.lastval"
            | "setval"
            | "pg_catalog.setval"
    ))
}

pub(crate) fn is_set_config_function(function: &Function) -> Result<bool> {
    Ok(matches!(
        object_name(&function.name)?.to_ascii_lowercase().as_str(),
        "set_config" | "pg_catalog.set_config"
    ))
}

pub(crate) fn sequence_arg(expr: Option<&Expr>, function: &str) -> Result<String> {
    let Some(expr) = expr else {
        return Err(SqlError::InvalidSql(format!(
            "{function} expects a sequence name"
        )));
    };
    // `nextval('seq'::regclass)` — pg_dump's spelling for every SERIAL
    // default. `eval_constant_expr` evaluates casts with no database
    // handle, so a `::regclass` cast can never resolve a real relation and
    // fails with "relation does not exist" even for a sequence that
    // exists. The literal under the cast IS the sequence name, so take it
    // directly; the caller normalizes and then verifies existence.
    if let Expr::Cast {
        expr: inner,
        data_type,
        ..
    } = expr
    {
        if matches!(data_type, DataType::Regclass)
            || data_type.to_string().eq_ignore_ascii_case("regclass")
        {
            return sequence_arg(Some(inner), function);
        }
    }
    match eval_constant_expr(expr)? {
        SqlValue::String(value) => Ok(value),
        other => Err(SqlError::InvalidSql(format!(
            "{function} sequence name must be text, got {}",
            other.to_cell()
        ))),
    }
}

pub(crate) fn default_sequence_from_expr(expr: &Expr) -> Result<Option<String>> {
    match expr {
        Expr::Function(function) if is_sequence_function(function)? => {
            let name = object_name(&function.name)?.to_ascii_lowercase();
            if name != "nextval" && name != "pg_catalog.nextval" {
                return Err(SqlError::Unsupported(
                    "ALTER COLUMN SET DEFAULT supports only nextval(...) sequence defaults"
                        .to_string(),
                ));
            }
            // Normalize to the stored key: `CREATE SEQUENCE` stores via
            // `normalize_sequence_name` (schema stripped, unquoted,
            // lowercased) and the runtime nextval/setval calls resolve the
            // same way, but a DDL-time `nextval('public.foo'::regclass)`
            // default kept its schema qualifier and never matched — the
            // pg_dump SERIAL `SET DEFAULT` restore failure.
            Ok(Some(normalize_sequence_name(&sequence_arg(
                function_args(function).first(),
                "nextval",
            )?)))
        }
        Expr::Cast { expr, .. } => default_sequence_from_expr(expr),
        Expr::Nested(expr) => default_sequence_from_expr(expr),
        _ => Ok(None),
    }
}

pub(crate) fn nextval_sequence_from_expr(expr: &Expr) -> Result<Option<String>> {
    match expr {
        Expr::Function(function) => {
            let name = object_name(&function.name)?.to_ascii_lowercase();
            if !matches!(name.as_str(), "nextval" | "pg_catalog.nextval") {
                return Ok(None);
            }
            // Same normalization as `default_sequence_from_expr`: a
            // schema-qualified nextval default in CREATE TABLE must bind to
            // the stored (unqualified) sequence key.
            sequence_arg(function_args(function).first(), "nextval")
                .map(|name| Some(normalize_sequence_name(&name)))
        }
        Expr::Cast { expr, .. } | Expr::Nested(expr) => nextval_sequence_from_expr(expr),
        _ => Ok(None),
    }
}

pub(crate) fn expr_is_default(expr: &Expr) -> bool {
    matches!(expr, Expr::Identifier(ident) if ident.value.eq_ignore_ascii_case("default"))
}

/// Rendering the `DataType` to text (and lower-casing it) is only needed for
/// the shapes whose name carries the answer — arrays, custom names, vectors,
/// the error message. Every built-in shape resolves structurally: this runs
/// on every cast evaluation (several times, on the routine paths).
pub fn pg_type_from_data_type(data_type: &DataType) -> Result<(String, Option<usize>)> {
    let mapped = match data_type {
        DataType::Bool | DataType::Boolean => ("bool".to_string(), None),
        DataType::Int2(_) | DataType::SmallInt(_) => ("int2".to_string(), None),
        DataType::Int(_) | DataType::Int4(_) | DataType::Integer(_) | DataType::Int32 => {
            ("int4".to_string(), None)
        }
        DataType::Int8(_) | DataType::BigInt(_) | DataType::Int64 => ("int8".to_string(), None),
        DataType::Float4 | DataType::Real | DataType::Float32 => ("float4".to_string(), None),
        DataType::Float8 | DataType::Double(_) | DataType::DoublePrecision | DataType::Float64 => {
            ("float8".to_string(), None)
        }
        DataType::Float(info) => (pg_float_type_from_precision(info)?.to_string(), None),
        DataType::Numeric(_) | DataType::Decimal(_) | DataType::Dec(_) => {
            ("numeric".to_string(), None)
        }
        DataType::Char(_) | DataType::Character(_) => ("bpchar".to_string(), None),
        DataType::CharacterVarying(_)
        | DataType::CharVarying(_)
        | DataType::Varchar(_)
        | DataType::Nvarchar(_) => ("varchar".to_string(), None),
        DataType::Text => ("text".to_string(), None),
        DataType::JSON => ("json".to_string(), None),
        DataType::JSONB => ("jsonb".to_string(), None),
        DataType::Regclass => ("regclass".to_string(), None),
        DataType::TsVector => ("tsvector".to_string(), None),
        DataType::TsQuery => ("tsquery".to_string(), None),
        DataType::Bytea | DataType::Bytes(_) | DataType::Binary(_) | DataType::Varbinary(_) => {
            ("bytea".to_string(), None)
        }
        DataType::Timestamp(_, TimezoneInfo::WithTimeZone | TimezoneInfo::Tz) => {
            ("timestamptz".to_string(), None)
        }
        DataType::Timestamp(_, _) => ("timestamp".to_string(), None),
        DataType::Date => ("date".to_string(), None),
        DataType::Time(_, TimezoneInfo::WithTimeZone | TimezoneInfo::Tz) => {
            ("timetz".to_string(), None)
        }
        DataType::Time(_, _) => ("time".to_string(), None),
        DataType::Interval { .. } => ("interval".to_string(), None),
        DataType::Bit(_) => ("bit".to_string(), None),
        DataType::BitVarying(_) | DataType::VarBit(_) => ("varbit".to_string(), None),
        DataType::Uuid => ("uuid".to_string(), None),
        DataType::Trigger => ("trigger".to_string(), None),
        _ => return pg_type_from_rendered_data_type(data_type),
    };
    if pg_type_oid_by_name(&mapped.0).is_none() {
        return Err(SqlError::Unsupported(format!(
            "column type {data_type} lowered to unregistered type {}",
            mapped.0
        )));
    }
    Ok(mapped)
}

/// The text-driven half of `pg_type_from_data_type`: arrays, `vector(n)`,
/// custom names, enums and the unsupported-type error.
fn pg_type_from_rendered_data_type(data_type: &DataType) -> Result<(String, Option<usize>)> {
    let rendered = data_type.to_string();
    let lower = rendered.to_ascii_lowercase();
    if matches!(lower.as_str(), "timestamptz" | "timestamp with time zone") {
        return Ok(("timestamptz".to_string(), None));
    }
    if matches!(lower.as_str(), "timetz" | "time with time zone") {
        return Ok(("timetz".to_string(), None));
    }
    let mapped = match data_type {
        DataType::Array(_) => (canonical_array_pg_type(&lower)?, None),
        _ if lower.starts_with("vector") => {
            let dim = lower
                .trim_start_matches("vector")
                .trim()
                .trim_start_matches('(')
                .trim_end_matches(')')
                .parse::<usize>()
                .ok();
            ("vector".to_string(), dim)
        }
        DataType::Custom(name, _) => {
            let custom_type = normalize_object_name(&name.to_string());
            let custom_type = custom_type.rsplit('.').next().unwrap_or(&custom_type);
            if custom_type.ends_with("[]")
                || custom_type.ends_with(" array")
                || custom_type.starts_with("array<")
            {
                return Ok((canonical_array_pg_type(custom_type)?, None));
            }
            let canonical = canonical_scalar_pg_type_name(custom_type).ok_or_else(|| {
                SqlError::Unsupported(format!(
                    "column type {custom_type} is not registered; enum, domain, and user-defined types are not supported yet"
                ))
            })?;
            (canonical.to_string(), None)
        }
        DataType::Enum(_, _) => {
            return Err(SqlError::Unsupported(
                "enum, domain, and user-defined column types are not supported".to_string(),
            ))
        }
        _ => {
            return Err(SqlError::Unsupported(format!(
                "column type {rendered} is not supported"
            )))
        }
    };
    if pg_type_oid_by_name(&mapped.0).is_none() {
        return Err(SqlError::Unsupported(format!(
            "column type {rendered} lowered to unregistered type {}",
            mapped.0
        )));
    }
    Ok(mapped)
}

pub(crate) fn pg_float_type_from_precision(info: &ExactNumberInfo) -> Result<&'static str> {
    match info {
        ExactNumberInfo::None => Ok("float8"),
        ExactNumberInfo::Precision(precision) if *precision <= 24 => Ok("float4"),
        ExactNumberInfo::Precision(_) => Ok("float8"),
        ExactNumberInfo::PrecisionAndScale(_, _) => Err(SqlError::Unsupported(
            "FLOAT with scale is not supported".to_string(),
        )),
    }
}

pub(crate) fn pg_type_modifier_from_data_type(
    data_type: &DataType,
) -> Result<Option<PgTypeModifier>> {
    // `vector(n)` arrives as a custom type name; built-in shapes never
    // need the rendered text.
    let rendered = match data_type {
        DataType::Custom(..) => data_type.to_string().to_ascii_lowercase(),
        _ => String::new(),
    };
    if let Some(dimensions) = rendered
        .strip_prefix("vector(")
        .and_then(|value| value.strip_suffix(')'))
        .and_then(|value| value.parse::<u16>().ok())
    {
        if dimensions == 0 || dimensions > 16_000 {
            return Err(SqlError::InvalidSql(format!(
                "vector dimensions must be between 1 and 16000, got {dimensions}"
            )));
        }
        return Ok(Some(PgTypeModifier::Vector { dimensions }));
    }
    match data_type {
        DataType::Numeric(info) | DataType::Decimal(info) | DataType::Dec(info) => {
            let (precision, scale) = match info {
                ExactNumberInfo::None => return Ok(None),
                ExactNumberInfo::Precision(precision) => (*precision, 0),
                ExactNumberInfo::PrecisionAndScale(precision, scale) => (*precision, *scale),
            };
            if !(1..=1000).contains(&precision) || !(-1000..=1000).contains(&scale) {
                return Err(SqlError::InvalidSql(format!(
                    "NUMERIC precision must be 1..1000 and scale must be -1000..1000, got ({precision},{scale})"
                )));
            }
            Ok(Some(PgTypeModifier::Numeric {
                precision: precision as u16,
                scale: scale as i16,
            }))
        }
        DataType::Char(length) | DataType::Character(length) => {
            Ok(Some(PgTypeModifier::Character {
                length: character_type_length(*length, 1)?,
            }))
        }
        DataType::CharacterVarying(length)
        | DataType::CharVarying(length)
        | DataType::Varchar(length)
        | DataType::Nvarchar(length) => length
            .map(|length| {
                Ok(PgTypeModifier::Character {
                    length: character_type_length(Some(length), 1)?,
                })
            })
            .transpose(),
        DataType::Time(precision, _) | DataType::Timestamp(precision, _) => precision
            .map(|precision| {
                if precision > 6 {
                    return Err(SqlError::InvalidSql(format!(
                        "time precision {precision} must be between 0 and 6"
                    )));
                }
                Ok(PgTypeModifier::Temporal {
                    precision: precision as u8,
                })
            })
            .transpose(),
        DataType::Interval { fields, precision } => {
            if precision.is_some_and(|precision| precision > 6) {
                return Err(SqlError::InvalidSql(
                    "interval precision must be between 0 and 6".to_string(),
                ));
            }
            if fields.is_none() && precision.is_none() {
                return Ok(None);
            }
            Ok(Some(PgTypeModifier::Interval {
                fields: fields.map(|fields| fields.to_string()),
                precision: precision.map(|value| value as u8),
            }))
        }
        DataType::Bit(length) => Ok(Some(PgTypeModifier::Bit {
            length: bit_type_length(length.unwrap_or(1), "bit")?,
        })),
        DataType::BitVarying(length) | DataType::VarBit(length) => length
            .map(|length| {
                Ok(PgTypeModifier::Bit {
                    length: bit_type_length(length, "varbit")?,
                })
            })
            .transpose(),
        DataType::Array(element) => match element {
            ArrayElemTypeDef::AngleBracket(element)
            | ArrayElemTypeDef::SquareBracket(element, _)
            | ArrayElemTypeDef::Parenthesis(element) => pg_type_modifier_from_data_type(element),
            ArrayElemTypeDef::None => Ok(None),
        },
        _ => Ok(None),
    }
}

fn character_type_length(length: Option<CharacterLength>, default: u32) -> Result<u32> {
    let length = match length {
        None => u64::from(default),
        Some(CharacterLength::IntegerLength { length, unit: _ }) => length,
        Some(CharacterLength::Max) => {
            return Err(SqlError::Unsupported(
                "PostgreSQL does not support VARCHAR(MAX)".to_string(),
            ))
        }
    };
    if !(1..=10_485_760).contains(&length) {
        return Err(SqlError::InvalidSql(format!(
            "character length {length} must be between 1 and 10485760"
        )));
    }
    Ok(length as u32)
}

fn bit_type_length(length: u64, type_name: &str) -> Result<u32> {
    if !(1..=83_886_080).contains(&length) {
        return Err(SqlError::invalid_parameter_value(if length == 0 {
            format!("length for type {type_name} must be at least 1")
        } else {
            format!("length for type {type_name} cannot exceed 83886080")
        }));
    }
    Ok(length as u32)
}

pub(crate) fn canonical_array_pg_type(rendered_lower: &str) -> Result<String> {
    let mut bracket_inner = rendered_lower.trim();
    let mut saw_bracket = false;
    while let Some(without_close) = bracket_inner.strip_suffix(']') {
        let Some(open) = without_close.rfind('[') else {
            break;
        };
        let declared_length = &without_close[open + 1..];
        if !declared_length.chars().all(|ch| ch.is_ascii_digit()) {
            break;
        }
        bracket_inner = without_close[..open].trim_end();
        saw_bracket = true;
    }
    let inner = saw_bracket
        .then_some(bracket_inner)
        .or_else(|| rendered_lower.strip_suffix(" array"))
        .or_else(|| {
            rendered_lower
                .strip_prefix("array<")
                .and_then(|value| value.strip_suffix('>'))
        })
        .map(str::trim)
        .ok_or_else(|| {
            SqlError::Unsupported(format!(
                "array column type {rendered_lower} is not supported"
            ))
        })?;
    let inner = array_element_type_base(inner);
    let inner = inner
        .strip_prefix("pg_catalog.")
        .unwrap_or(inner)
        .trim_matches('"');
    let element = canonical_scalar_pg_type_name(inner).ok_or_else(|| {
        SqlError::Unsupported(format!("array element type {inner} is not registered"))
    })?;
    if pg_type_spec(element)
        .and_then(|spec| spec.array_oid)
        .is_none()
    {
        return Err(SqlError::Unsupported(format!(
            "array element type {inner} has no PostgreSQL array type"
        )));
    }
    Ok(format!("{element}[]"))
}

pub(crate) fn array_element_type_base(inner: &str) -> &str {
    inner
        .split_once('(')
        .map(|(base, _)| base.trim())
        .unwrap_or(inner)
}

pub(crate) fn canonical_scalar_pg_type_name(name: &str) -> Option<&'static str> {
    pg_type_spec(name)
        .filter(|spec| !spec.pseudo)
        .map(|spec| spec.name)
}

pub(crate) fn record_from_fields(
    table: &str,
    schema: Option<&TableSchema>,
    fields: BTreeMap<String, SqlValue>,
) -> Result<Record> {
    let fields = canonicalize_record_fields(schema, fields);
    let id = record_id_from_fields(table, schema, &fields)?;
    if id.is_empty() {
        return Err(SqlError::InvalidSql(
            "record id must not be empty".to_string(),
        ));
    }
    let primary_key_columns = schema
        .map(primary_key_columns_for_schema)
        .unwrap_or_else(|| vec!["id".to_string()]);
    let omit_primary_key_metadata = primary_key_columns.len() == 1
        && schema
            .and_then(|schema| schema.column(&primary_key_columns[0]))
            .is_none_or(|column| !pg_type_requires_typed_identity(&column.pg_type));

    let mut metadata = JsonMap::new();
    let mut vector = None;
    let mut geometry = None;
    let mut timestamp = None;
    let mut payload = None;

    for (column, value) in fields {
        let column_schema = schema.and_then(|schema| schema.column(&column));
        let json_column = column_schema.is_some_and(|column| is_json_pg_type(&column.pg_type));
        let value = if let Some(column_schema) = column_schema {
            cast_value_to_column_type(value, column_schema).map_err(|error| match error {
                error @ (SqlError::ConstraintViolation { .. } | SqlError::DataException { .. }) => {
                    error
                }
                error => SqlError::TypeMismatch {
                    table: table.to_string(),
                    column: column_schema.name.clone(),
                    expected: column_schema.pg_type.clone(),
                    message: format!(
                        "column \"{}\" of relation \"{}\" expects type {}: {error}",
                        column_schema.name, table, column_schema.pg_type
                    ),
                },
            })?
        } else {
            value
        };
        // Absence represents SQL NULL for JSON columns. A present JSON `null`
        // remains `SqlValue::Json(JsonValue::Null)` and is stored explicitly.
        if json_column && matches!(value, SqlValue::Null) {
            continue;
        }
        if omit_primary_key_metadata
            && primary_key_columns
                .iter()
                .any(|primary_key| column == *primary_key)
        {
            continue;
        }
        if column == "timestamp" {
            if let Some(value) = value.as_f64() {
                timestamp = Some(value as i64);
                continue;
            }
        }
        if is_vector_column(schema, &column) {
            vector = Some(sql_value_to_vector(&value)?);
            metadata.insert(column, vector_json(vector.as_ref().unwrap()));
            continue;
        }
        if column_schema.is_none() && column.eq_ignore_ascii_case("geometry") {
            geometry = Some(spatial_geometry("INSERT geometry", &value)?);
            continue;
        }
        if column == "payload" && !json_column {
            if let SqlValue::Json(JsonValue::Array(values)) = value {
                payload = Some(
                    values
                        .into_iter()
                        .filter_map(|value| value.as_u64().map(|value| value as u8))
                        .collect::<Vec<_>>(),
                );
                continue;
            }
        }
        metadata.insert(
            column,
            sql_value_to_column_storage_json(value, column_schema)?,
        );
    }

    let mut record = Record::new(id).with_metadata(JsonValue::Object(metadata));
    if let Some(vector) = vector {
        record = record.with_vector(vector);
    }
    if let Some(geometry) = geometry {
        record = record.with_geometry(geometry);
    }
    if let Some(timestamp) = timestamp {
        record = record.with_timestamp(timestamp);
    }
    if let Some(payload) = payload {
        record = record.with_payload(payload);
    }
    Ok(record)
}

pub(crate) fn record_from_fields_with_db(
    db: &BicDb,
    table: &str,
    schema: Option<&TableSchema>,
    mut fields: BTreeMap<String, SqlValue>,
) -> Result<Record> {
    if let Some(schema) = schema {
        for (name, value) in &mut fields {
            let Some(column) = schema.column(name) else {
                continue;
            };
            if !matches!(value, SqlValue::Null) {
                if is_oid_alias_type(&column.pg_type) {
                    *value = resolve_oid_alias_value(db, &column.pg_type, value.clone())?;
                } else if let Some(element_type) = column.pg_type.strip_suffix("[]") {
                    if is_oid_alias_type(element_type) {
                        *value = resolve_oid_alias_array_value(db, element_type, value.clone())?;
                    }
                }
            }
        }
    }
    record_from_fields(table, schema, fields)
}

pub(crate) fn resolve_oid_alias_column_value(
    db: &BicDb,
    schema: &TableSchema,
    column: &str,
    value: SqlValue,
) -> Result<SqlValue> {
    let Some(column) = schema.column(column) else {
        return Ok(value);
    };
    if matches!(value, SqlValue::Null) {
        Ok(value)
    } else if is_oid_alias_type(&column.pg_type) {
        resolve_oid_alias_value(db, &column.pg_type, value)
    } else if let Some(element_type) = column.pg_type.strip_suffix("[]") {
        if is_oid_alias_type(element_type) {
            resolve_oid_alias_array_value(db, element_type, value)
        } else {
            Ok(value)
        }
    } else {
        Ok(value)
    }
}

pub(crate) fn record_fields_for_schema(
    record: &Record,
    schema: &TableSchema,
) -> BTreeMap<String, SqlValue> {
    schema
        .columns
        .iter()
        .filter(|column| !column.hidden)
        .map(|column| {
            (
                column.name.clone(),
                record_column_value(record, schema, &column.name),
            )
        })
        .collect()
}

pub(crate) fn record_id_from_fields(
    table: &str,
    schema: Option<&TableSchema>,
    fields: &BTreeMap<String, SqlValue>,
) -> Result<String> {
    let Some(schema) = schema else {
        return fields
            .get("id")
            .map(SqlValue::to_cell)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| SqlError::InvalidSql(format!("INSERT into {table} requires id")));
    };
    if schema.has_hidden_primary_key() {
        return Ok(synthetic_record_id(table));
    }
    let primary_key_columns = primary_key_columns_for_schema(schema);
    if primary_key_columns.is_empty() {
        return Ok(synthetic_record_id(table));
    }
    let [primary_key] = primary_key_columns.as_slice() else {
        return composite_record_id_from_fields(table, schema, fields, &primary_key_columns);
    };
    let value = fields
        .get(primary_key)
        .filter(|value| !matches!(value, SqlValue::Null))
        .ok_or_else(|| {
            SqlError::InvalidSql(format!("INSERT into {table} requires {primary_key}"))
        })?;
    let value = schema
        .column(primary_key)
        .map(|column| cast_value_to_column_type(value.clone(), column))
        .transpose()?
        .unwrap_or_else(|| value.clone());
    let id = primary_key_identity_cell(schema, primary_key, &value)?;
    if id.is_empty() {
        return Err(SqlError::InvalidSql(format!(
            "INSERT into {table} requires {primary_key}"
        )));
    }
    Ok(id)
}

pub(crate) fn composite_record_id_from_fields(
    table: &str,
    schema: &TableSchema,
    fields: &BTreeMap<String, SqlValue>,
    primary_key_columns: &[String],
) -> Result<String> {
    if primary_key_columns.is_empty() {
        return Err(SqlError::InvalidSql(format!(
            "INSERT into {table} requires a primary key"
        )));
    }
    let mut values = Vec::with_capacity(primary_key_columns.len());
    for column in primary_key_columns {
        let field_value = fields
            .get(column)
            .filter(|value| !matches!(value, SqlValue::Null))
            .ok_or_else(|| {
                SqlError::InvalidSql(format!("INSERT into {table} requires {column}"))
            })?;
        let field_value = schema
            .column(column)
            .map(|column| cast_value_to_column_type(field_value.clone(), column))
            .transpose()?
            .unwrap_or_else(|| field_value.clone());
        let value = primary_key_identity_cell(schema, column, &field_value)?;
        if value.is_empty() {
            return Err(SqlError::InvalidSql(format!(
                "INSERT into {table} requires {column}"
            )));
        }
        values.push(value);
    }
    serde_json::to_string(&values).map_err(SqlError::from)
}

pub(crate) fn canonicalize_record_fields(
    schema: Option<&TableSchema>,
    fields: BTreeMap<String, SqlValue>,
) -> BTreeMap<String, SqlValue> {
    let Some(schema) = schema else {
        return fields;
    };
    fields
        .into_iter()
        .map(|(column, value)| {
            let column = schema
                .column(&column)
                .map(|schema_column| schema_column.name.clone())
                .unwrap_or(column);
            (column, value)
        })
        .collect()
}

pub(crate) fn synthetic_record_id(table: &str) -> String {
    let counter = SYNTHETIC_ROW_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("{table}:{nanos}:{counter}")
}

pub(crate) fn conflict_target_columns(
    _table: &str,
    schema: Option<&TableSchema>,
    target: Option<&ConflictTarget>,
) -> Result<Vec<String>> {
    match target {
        Some(ConflictTarget::Columns(columns)) => Ok(columns
            .iter()
            .map(|column| column.value.clone())
            .collect::<Vec<_>>()),
        Some(ConflictTarget::OnConstraint(name)) => {
            let name = relation_name(name)?;
            let Some(schema) = schema else {
                return Err(SqlError::Unsupported(format!(
                    "ON CONFLICT ON CONSTRAINT {name} requires a table schema"
                )));
            };
            if name == schema.primary_key_constraint_name() {
                return Ok(primary_key_columns_for_schema(schema));
            }
            for constraint in &schema.constraints {
                match constraint {
                    ConstraintSchema::Unique {
                        name: constraint_name,
                        columns,
                        ..
                    }
                    | ConstraintSchema::ForeignKey {
                        name: constraint_name,
                        columns,
                        ..
                    } if constraint_name.eq_ignore_ascii_case(&name) => {
                        return Ok(columns.clone());
                    }
                    _ => {}
                }
            }
            Err(SqlError::Unsupported(format!(
                "ON CONFLICT constraint {name} is not supported"
            )))
        }
        None => {
            let columns = schema.map(primary_key_columns_for_schema).ok_or_else(|| {
                SqlError::Unsupported("ON CONFLICT requires a table schema".to_string())
            })?;
            if columns.is_empty() {
                return Err(SqlError::Unsupported(
                    "ON CONFLICT requires a primary key or conflict target".to_string(),
                ));
            }
            Ok(columns)
        }
    }
}

#[cfg(test)]
mod pg_type_resolution_tests {
    use super::*;

    /// The previous, text-driven resolver, kept as the oracle.
    fn reference(data_type: &DataType) -> Result<(String, Option<usize>)> {
        let rendered = data_type.to_string();
        let lower = rendered.to_ascii_lowercase();
        if matches!(lower.as_str(), "timestamptz" | "timestamp with time zone") {
            return Ok(("timestamptz".to_string(), None));
        }
        if matches!(lower.as_str(), "timetz" | "time with time zone") {
            return Ok(("timetz".to_string(), None));
        }
        let mapped = match data_type {
            DataType::Bool | DataType::Boolean => ("bool".to_string(), None),
            DataType::Int2(_) | DataType::SmallInt(_) => ("int2".to_string(), None),
            DataType::Int(_) | DataType::Int4(_) | DataType::Integer(_) | DataType::Int32 => {
                ("int4".to_string(), None)
            }
            DataType::Int8(_) | DataType::BigInt(_) | DataType::Int64 => ("int8".to_string(), None),
            DataType::Float4 | DataType::Real | DataType::Float32 => ("float4".to_string(), None),
            DataType::Float8
            | DataType::Double(_)
            | DataType::DoublePrecision
            | DataType::Float64 => ("float8".to_string(), None),
            DataType::Float(info) => (pg_float_type_from_precision(info)?.to_string(), None),
            DataType::Numeric(_) | DataType::Decimal(_) | DataType::Dec(_) => {
                ("numeric".to_string(), None)
            }
            DataType::Char(_) | DataType::Character(_) => ("bpchar".to_string(), None),
            DataType::CharacterVarying(_)
            | DataType::CharVarying(_)
            | DataType::Varchar(_)
            | DataType::Nvarchar(_) => ("varchar".to_string(), None),
            DataType::Text => ("text".to_string(), None),
            DataType::JSON => ("json".to_string(), None),
            DataType::JSONB => ("jsonb".to_string(), None),
            DataType::Regclass => ("regclass".to_string(), None),
            DataType::TsVector => ("tsvector".to_string(), None),
            DataType::TsQuery => ("tsquery".to_string(), None),
            DataType::Bytea | DataType::Bytes(_) | DataType::Binary(_) | DataType::Varbinary(_) => {
                ("bytea".to_string(), None)
            }
            DataType::Timestamp(_, TimezoneInfo::WithTimeZone | TimezoneInfo::Tz) => {
                ("timestamptz".to_string(), None)
            }
            DataType::Timestamp(_, _) => ("timestamp".to_string(), None),
            DataType::Date => ("date".to_string(), None),
            DataType::Time(_, TimezoneInfo::WithTimeZone | TimezoneInfo::Tz) => {
                ("timetz".to_string(), None)
            }
            DataType::Time(_, _) => ("time".to_string(), None),
            DataType::Interval { .. } => ("interval".to_string(), None),
            DataType::Bit(_) => ("bit".to_string(), None),
            DataType::BitVarying(_) | DataType::VarBit(_) => ("varbit".to_string(), None),
            DataType::Uuid => ("uuid".to_string(), None),
            DataType::Trigger => ("trigger".to_string(), None),
            DataType::Array(_) => (canonical_array_pg_type(&lower)?, None),
            _ if lower.starts_with("vector") => {
                let dim = lower
                    .trim_start_matches("vector")
                    .trim()
                    .trim_start_matches('(')
                    .trim_end_matches(')')
                    .parse::<usize>()
                    .ok();
                ("vector".to_string(), dim)
            }
            DataType::Custom(name, _) => {
                let custom_type = normalize_object_name(&name.to_string());
                let custom_type = custom_type.rsplit('.').next().unwrap_or(&custom_type);
                if custom_type.ends_with("[]")
                    || custom_type.ends_with(" array")
                    || custom_type.starts_with("array<")
                {
                    return Ok((canonical_array_pg_type(custom_type)?, None));
                }
                let canonical = canonical_scalar_pg_type_name(custom_type)
                    .ok_or_else(|| SqlError::Unsupported("unregistered".to_string()))?;
                (canonical.to_string(), None)
            }
            DataType::Enum(_, _) => return Err(SqlError::Unsupported("enum".to_string())),
            _ => return Err(SqlError::Unsupported("unsupported".to_string())),
        };
        if pg_type_oid_by_name(&mapped.0).is_none() {
            return Err(SqlError::Unsupported("unregistered".to_string()));
        }
        Ok(mapped)
    }

    fn data_types(spellings: &[&str]) -> Vec<(String, DataType)> {
        spellings
            .iter()
            .map(|spelling| {
                let statements =
                    parse_statements(&format!("SELECT CAST(NULL AS {spelling})")).unwrap();
                let Statement::Query(query) = &statements[0] else {
                    panic!("query")
                };
                let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
                    panic!("select")
                };
                let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Cast { data_type, .. }) =
                    &select.projection[0]
                else {
                    panic!("cast")
                };
                (spelling.to_string(), data_type.clone())
            })
            .collect()
    }

    #[test]
    fn structural_resolution_matches_the_text_resolver() {
        let spellings = [
            "timestamptz",
            "timestamp with time zone",
            "timestamp without time zone",
            "timestamp(3) with time zone",
            "timestamp",
            "timetz",
            "time with time zone",
            "time",
            "date",
            "interval",
            "int",
            "integer",
            "int4",
            "smallint",
            "int2",
            "bigint",
            "int8",
            "numeric",
            "numeric(8,2)",
            "decimal(12,2)",
            "float",
            "float(10)",
            "float(40)",
            "real",
            "double precision",
            "float8",
            "bool",
            "boolean",
            "varchar",
            "varchar(16)",
            "character varying(24)",
            "char(24)",
            "character(5)",
            "text",
            "name",
            "json",
            "jsonb",
            "bytea",
            "uuid",
            "regclass",
            "regtype",
            "oid",
            "money",
            "inet",
            "int[]",
            "text[]",
            "numeric(5,2)[]",
            "vector(3)",
            "vector",
            "record",
            "tsvector",
            "bit(3)",
            "bit varying(4)",
            "some_unknown_type",
        ];
        for (spelling, data_type) in data_types(&spellings) {
            let expected = reference(&data_type).ok();
            let actual = pg_type_from_data_type(&data_type).ok();
            assert_eq!(actual, expected, "{spelling} ({data_type:?})");
            // The modifier resolver only renders custom names now.
            let modifier_before = {
                let rendered = data_type.to_string().to_ascii_lowercase();
                rendered
                    .strip_prefix("vector(")
                    .and_then(|value| value.strip_suffix(')'))
                    .and_then(|value| value.parse::<u16>().ok())
            };
            let modifier = pg_type_modifier_from_data_type(&data_type).ok().flatten();
            if let Some(dimensions) = modifier_before {
                assert!(
                    matches!(modifier, Some(PgTypeModifier::Vector { dimensions: d }) if d == dimensions),
                    "{spelling}"
                );
            }
        }
    }
}
