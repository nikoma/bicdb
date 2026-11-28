//! Table inheritance (`CREATE TABLE child (...) INHERITS (parent)`): how a
//! child's definition is derived from its parents, and the hierarchy walks
//! used by reads, writes and DDL on a parent.
use crate::*;

fn type_conflict(
    left: &ColumnSchema,
    right: &ColumnSchema,
    postgres: bool,
    both_inherited: bool,
) -> SqlError {
    let column = &right.name;
    if postgres {
        let qualifier = if both_inherited { "inherited " } else { "" };
        return SqlError::coded(
            "42804",
            format!("{qualifier}column \"{column}\" has a type conflict"),
            Some(format!(
                "{} versus {}",
                left.formatted_pg_type(),
                right.formatted_pg_type()
            )),
            None,
        );
    }
    SqlError::data_exception(
        "42804",
        format!(
            "column \"{column}\" has a type conflict: {} versus {}",
            left.pg_type, right.pg_type
        ),
        None,
    )
}

fn check_inherited_collation(
    left: &ColumnSchema,
    right: &ColumnSchema,
    both_inherited: bool,
) -> Result<()> {
    let left_oid = left.collation_oid();
    let right_oid = right.collation_oid();
    if left_oid == right_oid {
        return Ok(());
    }
    let collations = pg_collation_rows();
    let collation_name = |oid| {
        collations
            .iter()
            .find(|row| row.get("oid").and_then(sql_value_i64) == Some(oid))
            .and_then(|row| row.get("collname"))
            .map_or_else(|| oid.to_string(), SqlValue::to_cell)
    };
    let column = &right.name;
    let qualifier = if both_inherited { "inherited " } else { "" };
    Err(SqlError::coded(
        "42P21",
        format!("{qualifier}column \"{column}\" has a collation conflict"),
        Some(format!(
            "\"{}\" versus \"{}\"",
            collation_name(left_oid),
            collation_name(right_oid),
        )),
        None,
    ))
}

pub(crate) fn has_inheritable_default(column: &ColumnSchema) -> bool {
    if column.default_sequence.is_some() {
        return true;
    }
    if let Some(expression) = &column.default_expr {
        // A persisted literal SQL NULL can have no cached value after a
        // round trip. The definition, rather than the cache, identifies it.
        return column.user_type.is_some() || !default_is_literal_null(expression);
    }
    column
        .default_value
        .as_ref()
        .is_some_and(|value| *value != SqlValue::Null || column.user_type.is_some())
}

fn default_is_literal_null(text: &str) -> bool {
    if !text
        .as_bytes()
        .windows(4)
        .any(|word| word.eq_ignore_ascii_case(b"null"))
    {
        return false;
    }
    let Ok(expr) = SqlSession::parse_default_expr(text) else {
        return false;
    };
    let mut current = &expr;
    loop {
        match current {
            Expr::Nested(expr) | Expr::Cast { expr, .. } => current = expr,
            Expr::Value(value) => return value.value == Value::Null,
            _ => return false,
        }
    }
}

pub(crate) fn copy_inherited_default(source: &ColumnSchema, target: &mut ColumnSchema) {
    target.default_value.clone_from(&source.default_value);
    target.default_expr.clone_from(&source.default_expr);
    target.default_sequence.clone_from(&source.default_sequence);
}

fn same_inherited_default(left: &ColumnSchema, right: &ColumnSchema) -> bool {
    if left.default_sequence.is_some() || right.default_sequence.is_some() {
        return left.default_sequence == right.default_sequence;
    }
    match (&left.default_expr, &right.default_expr) {
        (Some(left), Some(right)) => same_inherited_expression(left, right),
        _ => left.default_value == right.default_value,
    }
}

fn same_inherited_expression(left: &str, right: &str) -> bool {
    left == right
        || normalized_default_expr(left)
            .zip(normalized_default_expr(right))
            .is_some_and(|(left, right)| left == right)
}

fn normalized_default_expr(text: &str) -> Option<Expr> {
    SqlSession::parse_default_expr(text)
        .ok()
        .map(normalize_expr)
}

/// Parent and child expressions can differ in source parentheses and in the
/// spelling of an unquoted column. Compare their trees after normalization.
pub(crate) fn same_inherited_check(left: &str, right: &str) -> Result<bool> {
    if left == right {
        return Ok(true);
    }
    Ok(normalize_expr(parse_check_expression(left)?)
        == normalize_expr(parse_check_expression(right)?))
}

fn normalize_expr(mut expression: Expr) -> Expr {
    let normalize_ident = |ident: &mut Ident| {
        if ident.quote_style.is_none() {
            ident.value.make_ascii_lowercase();
        }
        ident.quote_style = None;
    };
    let _ = sqlparser::ast::visit_expressions_mut(&mut expression, |expr| {
        while let Expr::Nested(inner) = expr {
            *expr = std::mem::replace(inner.as_mut(), Expr::Value(Value::Null.into()));
        }
        match expr {
            Expr::Identifier(ident) => normalize_ident(ident),
            Expr::CompoundIdentifier(parts) => parts.iter_mut().for_each(normalize_ident),
            _ => {}
        }
        ControlFlow::<()>::Continue(())
    });
    expression
}

pub(crate) fn column_generation_kind(
    column: &ColumnSchema,
    virtual_columns: &[String],
) -> Option<&'static str> {
    column.generated_expr.as_ref().map(|_| {
        if virtual_columns.contains(&column.name) {
            "VIRTUAL"
        } else {
            "STORED"
        }
    })
}

fn same_type(left: &ColumnSchema, right: &ColumnSchema) -> bool {
    left.pg_type == right.pg_type
        && left.type_modifier == right.type_modifier
        && left.array_ndims == right.array_ndims
}

/// The columns, CHECK constraints and VIRTUAL generated column names a new
/// child takes from its parents, in PostgreSQL's order: each parent's
/// columns in turn, same-named columns merged.
pub(crate) fn inherited_table_definition(
    parents: &[TableSchema],
    postgres: bool,
    expression_conflicts: &mut BTreeSet<String>,
) -> Result<(Vec<ColumnSchema>, Vec<ConstraintSchema>, Vec<String>)> {
    let mut columns: Vec<ColumnSchema> = Vec::new();
    let mut constraints: Vec<ConstraintSchema> = Vec::new();
    let mut virtual_columns = Vec::new();
    for parent in parents {
        for column in parent.columns.iter().filter(|column| !column.hidden) {
            let mut inherited = column.clone();
            // Keys and identity are not inherited; NOT NULL, defaults and
            // generation expressions are. An inherited not-null constraint
            // takes its name from the parent and is valid on the new child.
            inherited.primary_key = false;
            inherited.attributes.not_null_name = None;
            inherited.attributes.not_null_not_valid = false;
            if postgres {
                inherited.attributes.inherited_local = Some(false);
            }
            if parent
                .no_inherit_constraints
                .contains(&not_null_constraint_name(&parent.name, &column.name))
            {
                inherited.nullable = true;
            }
            if inherited.identity.take().is_some() {
                inherited.default_sequence = None;
            }
            if postgres && !has_inheritable_default(&inherited) {
                inherited.default_expr = None;
                inherited.default_value = None;
            }
            let inherited_virtual = parent.virtual_generated_columns.contains(&inherited.name);
            match columns
                .iter_mut()
                .find(|existing| existing.name == inherited.name)
            {
                Some(existing) => {
                    if postgres {
                        crate::statement_context::report_notice(
                            None,
                            "00000",
                            format!(
                                "merging multiple inherited definitions of column \"{}\"",
                                inherited.name
                            ),
                        );
                    }
                    if !same_type(existing, &inherited) {
                        return Err(type_conflict(existing, &inherited, postgres, true));
                    }
                    existing.nullable &= inherited.nullable;
                    if postgres {
                        check_inherited_collation(existing, &inherited, true)?;
                        if column_generation_kind(existing, &virtual_columns)
                            != column_generation_kind(&inherited, &parent.virtual_generated_columns)
                        {
                            return Err(SqlError::data_exception(
                                "42804",
                                format!(
                                    "inherited column \"{}\" has a generation conflict",
                                    inherited.name
                                ),
                                None,
                            ));
                        }
                        let differs = match (&existing.generated_expr, &inherited.generated_expr) {
                            (Some(left), Some(right)) => !same_inherited_expression(left, right),
                            _ if has_inheritable_default(&inherited) => {
                                if has_inheritable_default(existing) {
                                    !same_inherited_default(existing, &inherited)
                                } else {
                                    copy_inherited_default(&inherited, existing);
                                    false
                                }
                            }
                            _ => false,
                        };
                        if differs {
                            expression_conflicts.insert(inherited.name.clone());
                        }
                    }
                }
                None => {
                    if inherited_virtual {
                        virtual_columns.push(inherited.name.clone());
                    }
                    columns.push(inherited);
                }
            }
        }
        for constraint in &parent.constraints {
            let ConstraintSchema::Check {
                name, expression, ..
            } = constraint
            else {
                continue;
            };
            if parent.no_inherit_constraints.contains(name) {
                continue;
            }
            let existing = constraints.iter().find_map(|existing| match existing {
                ConstraintSchema::Check {
                    name: previous,
                    expression,
                    ..
                } if previous == name => Some(expression),
                _ => None,
            });
            if let Some(previous) = existing {
                if postgres && !same_inherited_check(previous, expression)? {
                    return Err(SqlError::data_exception(
                        "42710",
                        format!("check constraint name \"{name}\" appears multiple times but with different expressions"),
                        None,
                    ));
                }
            } else {
                let mut inherited = constraint.clone();
                if postgres {
                    // A new child starts empty, so an enforced check is valid
                    // even if its parent has not validated its existing rows.
                    set_constraint_validated(&mut inherited, true);
                }
                constraints.push(inherited);
            }
        }
    }
    Ok((columns, constraints, virtual_columns))
}

/// A child column that repeats an inherited one: the types must agree, NOT
/// NULL from either side applies. PostgreSQL merges defaults separately
/// after processing the child's explicit defaults and identity clauses.
pub(crate) fn merge_inherited_column(
    inherited: &ColumnSchema,
    own: &mut ColumnSchema,
    postgres: bool,
    moved: bool,
) -> Result<()> {
    if postgres {
        let (prefix, fields) = if moved {
            (
                "moving and merging",
                vec![SqlErrorField::Detail(
                    "User-specified column moved to the position of the inherited column."
                        .to_string(),
                )],
            )
        } else {
            ("merging", Vec::new())
        };
        let _ = crate::statement_context::note_notice(SqlNotice {
            sqlstate: "00000".to_string(),
            message: format!("{prefix} column \"{}\" with inherited definition", own.name),
            fields,
        });
    }
    if !same_type(inherited, own) {
        return Err(type_conflict(inherited, own, postgres, false));
    }
    if postgres {
        own.attributes.inherited_local = Some(true);
        check_inherited_collation(inherited, own, false)?;
        if inherited.generated_expr.is_none() && own.generated_expr.is_some() {
            return Err(SqlError::coded(
                "42611",
                format!(
                    "child column \"{}\" specifies generation expression",
                    own.name
                ),
                None,
                Some(
                    "A child table column cannot be generated unless its parent column is.".into(),
                ),
            ));
        }
    }
    own.nullable &= inherited.nullable;
    if !postgres
        && own.default_value.is_none()
        && own.default_expr.is_none()
        && own.default_sequence.is_none()
    {
        copy_inherited_default(inherited, own);
    }
    if own.generated_expr.is_none() {
        own.generated_expr = inherited.generated_expr.clone();
    }
    Ok(())
}

/// Count immediate parent definitions, including parents that took the
/// column from their own ancestors.
pub(crate) fn inherited_column_counts(
    db: &BicDb,
    child: &TableSchema,
) -> Result<BTreeMap<String, i64>> {
    let mut counts = BTreeMap::new();
    for parent in &child.inherits {
        if let Some(parent) = load_schema_shared(db, parent)? {
            for column in parent.columns.iter().filter(|column| !column.hidden) {
                *counts.entry(column.name.clone()).or_default() += 1;
            }
        }
    }
    Ok(counts)
}

/// A column stays local when its first parent is attached, and becomes
/// local if the only parent that defined it is detached.
pub(crate) fn preserve_local_columns(
    db: &BicDb,
    child: &mut TableSchema,
    changed_parent: &TableSchema,
) -> Result<()> {
    let other_parent_columns = inherited_column_counts(db, child)?;
    for column in child.columns.iter_mut().filter(|column| !column.hidden) {
        if !other_parent_columns.contains_key(&column.name)
            && changed_parent
                .column(&column.name)
                .is_some_and(|parent_column| !parent_column.hidden)
        {
            column.attributes.inherited_local = Some(true);
        }
    }
    Ok(())
}

/// Every table below `parent` in its inheritance tree, depth first.
pub(crate) fn inheritance_descendants(
    db: &BicDb,
    parent: &TableSchema,
) -> Result<Vec<TableSchema>> {
    fn visit(
        db: &BicDb,
        table: &TableSchema,
        seen: &mut BTreeSet<String>,
        out: &mut Vec<TableSchema>,
    ) -> Result<()> {
        for child in &table.inheritance_children {
            if !seen.insert(child.clone()) {
                continue;
            }
            if let Some(child) = load_schema(db, child)? {
                out.push(child.clone());
                visit(db, &child, seen, out)?;
            }
        }
        Ok(())
    }
    let mut out = Vec::new();
    let mut seen = BTreeSet::from([parent.name.clone()]);
    visit(db, parent, &mut seen, &mut out)?;
    Ok(out)
}

impl TableSchema {
    /// Whether reading this relation means reading other heaps too: a
    /// partition tree, or a table other tables inherit from.
    pub(crate) fn has_descendant_tables(&self) -> bool {
        self.partitioning.is_some() || !self.inheritance_children.is_empty()
    }
}
