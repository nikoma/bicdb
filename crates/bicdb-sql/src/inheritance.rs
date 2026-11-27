//! Table inheritance (`CREATE TABLE child (...) INHERITS (parent)`): how a
//! child's definition is derived from its parents, and the hierarchy walks
//! used by reads, writes and DDL on a parent.
use crate::*;

fn type_conflict(column: &str, left: &ColumnSchema, right: &ColumnSchema) -> SqlError {
    SqlError::data_exception(
        "42804",
        format!(
            "column \"{column}\" has a type conflict: {} versus {}",
            left.pg_type, right.pg_type
        ),
        None,
    )
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
            if parent
                .no_inherit_constraints
                .contains(&not_null_constraint_name(&parent.name, &column.name))
            {
                inherited.nullable = true;
            }
            if inherited.identity.take().is_some() {
                inherited.default_sequence = None;
            }
            match columns
                .iter_mut()
                .find(|existing| existing.name == inherited.name)
            {
                Some(existing) => {
                    if !same_type(existing, &inherited) {
                        return Err(type_conflict(&inherited.name, existing, &inherited));
                    }
                    existing.nullable &= inherited.nullable;
                }
                None => {
                    if parent.virtual_generated_columns.contains(&inherited.name) {
                        virtual_columns.push(inherited.name.clone());
                    }
                    columns.push(inherited);
                }
            }
        }
        for constraint in &parent.constraints {
            if let ConstraintSchema::Check { name, .. } = constraint {
                if parent.no_inherit_constraints.contains(name) {
                    continue;
                }
                if !constraints
                    .iter()
                    .any(|existing| constraint_name(existing) == name)
                {
                    constraints.push(constraint.clone());
                }
            }
        }
    }
    Ok((columns, constraints, virtual_columns))
}

/// A child column that repeats an inherited one: the types must agree, NOT
/// NULL from either side applies, and the child's own default wins.
pub(crate) fn merge_inherited_column(
    inherited: &ColumnSchema,
    own: &mut ColumnSchema,
) -> Result<()> {
    if !same_type(inherited, own) {
        return Err(type_conflict(&own.name, inherited, own));
    }
    own.nullable &= inherited.nullable;
    if own.default_value.is_none() && own.default_expr.is_none() && own.default_sequence.is_none() {
        own.default_value = inherited.default_value.clone();
        own.default_expr = inherited.default_expr.clone();
        own.default_sequence = inherited.default_sequence.clone();
    }
    if own.generated_expr.is_none() {
        own.generated_expr = inherited.generated_expr.clone();
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
