//! UPDATE and DELETE on a table other tables inherit from: the statement
//! runs against the table's own rows and then against each descendant, as
//! PostgreSQL's inheritance expansion does. Every descendant has the
//! parent's columns, so the statement text is valid against each of them.
use super::*;
use std::cell::Cell;

thread_local! {
    /// Set while the per-table statements run, so they read one heap each.
    static EXPANDING: Cell<bool> = const { Cell::new(false) };
}

struct ExpansionGuard;

impl ExpansionGuard {
    fn enter() -> Self {
        EXPANDING.with(|flag| flag.set(true));
        ExpansionGuard
    }
}

impl Drop for ExpansionGuard {
    fn drop(&mut self) {
        EXPANDING.with(|flag| flag.set(false));
    }
}

/// The hierarchy an UPDATE or DELETE of `table` reaches, or `None` when the
/// table has no descendants (or the statement is one of the expanded ones).
fn expansion_targets(db: &BicDb, table: &str) -> Result<Option<Vec<TableSchema>>> {
    if EXPANDING.with(Cell::get) {
        return Ok(None);
    }
    let Some(schema) = load_schema(db, table)? else {
        return Ok(None);
    };
    if schema.inheritance_children.is_empty() {
        return Ok(None);
    }
    if schema.rls_enabled || schema.rls_forced || !schema.policies.is_empty() {
        // The parent's row policies would have to govern its children's
        // rows; per-table statements would apply each child's own instead.
        return Err(SqlError::Unsupported(
            "UPDATE and DELETE through an inheritance parent with row-level security are not supported"
                .to_string(),
        ));
    }
    let mut tables = vec![schema.clone()];
    tables.extend(crate::inheritance::inheritance_descendants(db, &schema)?);
    Ok(Some(tables))
}

/// MERGE chooses its target rows from one join snapshot of the named table;
/// expanding it per descendant would act on rows that snapshot never saw.
pub(crate) fn reject_merge_into_inheritance_parent(db: &BicDb, table: &str) -> Result<()> {
    let table = resolve_session_relation_name_if_exists(db, table);
    if load_schema(db, &table)?.is_some_and(|schema| !schema.inheritance_children.is_empty()) {
        return Err(SqlError::Unsupported(
            "MERGE into a table that other tables inherit from is not supported".to_string(),
        ));
    }
    Ok(())
}

/// Points `relation` at `target`, keeping the name the statement used as its
/// alias so qualified column references keep resolving.
fn retarget(relation: &mut TableFactor, target: &TableSchema) {
    if let TableFactor::Table { name, alias, .. } = relation {
        if alias.is_none() {
            let written = object_name_parts(name)
                .last()
                .cloned()
                .unwrap_or_else(|| target.name.clone());
            *alias = Some(TableAlias {
                explicit: true,
                name: Ident::new(written),
                columns: Vec::new(),
                at: None,
            });
        }
        *name = target_object_name(target);
    }
}

/// The quoted, schema-qualified name of `target`.
fn target_object_name(target: &TableSchema) -> ObjectName {
    let mut parts = Vec::new();
    if target.schema_name != "public" {
        parts.push(sqlparser::ast::ObjectNamePart::Identifier(
            Ident::with_quote('"', target.schema_name.clone()),
        ));
    }
    let bare = target.name.rsplit('.').next().unwrap_or(&target.name);
    parts.push(sqlparser::ast::ObjectNamePart::Identifier(
        Ident::with_quote('"', bare),
    ));
    ObjectName(parts)
}

/// Combines the per-table results: counts add up, RETURNING rows follow one
/// another.
fn combine(results: Vec<SqlResult>, verb: &str) -> SqlResult {
    let mut iter = results.into_iter();
    let Some(mut combined) = iter.next() else {
        return SqlResult::command(format!("{verb} 0"));
    };
    let count = |result: &SqlResult| {
        result
            .command_tag
            .as_deref()
            .and_then(|tag| tag.strip_prefix(verb))
            .and_then(|rest| rest.trim().parse::<u64>().ok())
    };
    let mut total = count(&combined);
    for result in iter {
        total = total.zip(count(&result)).map(|(left, right)| left + right);
        combined.rows.extend(result.rows);
        combined.notices.extend(result.notices);
    }
    if combined.command_tag.is_some() {
        combined.command_tag = total.map(|total| format!("{verb} {total}"));
    }
    combined
}

impl<'db> SqlSession<'db> {
    pub(crate) fn execute_inherited_update(
        &mut self,
        update: &sqlparser::ast::Update,
        ctes: &BTreeMap<String, CteResult>,
        match_policy: UpdateMatchPolicy,
    ) -> Result<Option<SqlResult>> {
        let (table, _) = table_with_joins_name_and_alias(&update.table)?;
        let table = resolve_session_relation_name_if_exists(self.db_ref(), &table);
        let Some(targets) = expansion_targets(self.db_ref(), &table)? else {
            return Ok(None);
        };
        let _guard = ExpansionGuard::enter();
        let mut results = Vec::with_capacity(targets.len());
        for target in &targets {
            let mut statement = update.clone();
            retarget(&mut statement.table.relation, target);
            results.push(self.execute_update_with_match_policy(
                &statement,
                ctes.clone(),
                match_policy,
                None,
            )?);
        }
        Ok(Some(combine(results, "UPDATE")))
    }

    pub(crate) fn execute_inherited_delete(
        &mut self,
        delete: &Delete,
        ctes: &BTreeMap<String, CteResult>,
    ) -> Result<Option<SqlResult>> {
        let (table, _) = delete_from_table_and_alias(delete)?;
        let table = resolve_session_relation_name_if_exists(self.db_ref(), &table);
        let Some(targets) = expansion_targets(self.db_ref(), &table)? else {
            return Ok(None);
        };
        let _guard = ExpansionGuard::enter();
        let mut results = Vec::with_capacity(targets.len());
        for target in &targets {
            let mut statement = delete.clone();
            let tables = match &mut statement.from {
                FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
            };
            if let Some(first) = tables.first_mut() {
                retarget(&mut first.relation, target);
            }
            results.push(self.execute_delete_with_ctes(&statement, ctes.clone())?);
        }
        Ok(Some(combine(results, "DELETE")))
    }

    /// DROP TABLE of a table in an inheritance hierarchy: a parent whose
    /// children survive needs CASCADE (which is not supported); a child is
    /// unlinked from its parents.
    pub(crate) fn detach_dropped_inheritance_table(
        &mut self,
        schema: &TableSchema,
        dropping: &[String],
        cascade: bool,
    ) -> Result<()> {
        let surviving = schema
            .inheritance_children
            .iter()
            .filter(|child| !dropping.iter().any(|name| name == *child))
            .count();
        if surviving > 0 {
            if cascade {
                return Err(SqlError::Unsupported(
                    "DROP TABLE ... CASCADE of an inheritance parent is not supported; drop its children first"
                        .to_string(),
                ));
            }
            return Err(SqlError::dependent_objects_still_exist(format!(
                "cannot drop table {} because other objects depend on it",
                schema.name
            )));
        }
        for parent in &schema.inherits {
            if let Some(mut parent) = load_schema(self.db_ref(), parent)? {
                parent
                    .inheritance_children
                    .retain(|child| child != &schema.name);
                self.save_session_schema(&parent)?;
            }
        }
        Ok(())
    }

    /// ALTER TABLE in an inheritance hierarchy. On a parent (without
    /// `ONLY`) column and CHECK changes recurse to every descendant, as in
    /// PostgreSQL; on a child, inherited columns and constraints cannot be
    /// dropped, renamed or retyped. `None` runs the statement normally.
    pub(crate) fn execute_inherited_alter_table(
        &mut self,
        alter_table: &AlterTable,
    ) -> Result<Option<SqlResult>> {
        if EXPANDING.with(Cell::get) {
            return Ok(None);
        }
        let table = resolve_session_relation_name_if_exists(
            self.db_ref(),
            &relation_name(&alter_table.name)?,
        );
        let Some(schema) = load_schema(self.db_ref(), &table)? else {
            return Ok(None);
        };
        if !schema.inherits.is_empty() {
            self.reject_inherited_member_changes(&schema, &alter_table.operations)?;
        }
        if schema.inheritance_children.is_empty() || alter_table.only {
            return Ok(None);
        }
        let recurses = |operation: &AlterTableOperation| {
            matches!(
                operation,
                AlterTableOperation::AddColumn { .. }
                    | AlterTableOperation::DropColumn { .. }
                    | AlterTableOperation::RenameColumn { .. }
                    | AlterTableOperation::AlterColumn {
                        op: AlterColumnOperation::SetDataType { .. }
                            | AlterColumnOperation::SetNotNull
                            | AlterColumnOperation::DropNotNull
                            | AlterColumnOperation::SetDefault { .. }
                            | AlterColumnOperation::DropDefault,
                        ..
                    }
                    | AlterTableOperation::AddConstraint {
                        constraint: TableConstraint::Check(_),
                        ..
                    }
            )
        };
        if !alter_table.operations.iter().any(recurses) {
            return Ok(None);
        }
        if !alter_table.operations.iter().all(recurses) {
            return Err(SqlError::Unsupported(
                "ALTER TABLE on an inheritance parent may combine only column and CHECK changes"
                    .to_string(),
            ));
        }
        let descendants = crate::inheritance::inheritance_descendants(self.db_ref(), &schema)?;
        let _guard = ExpansionGuard::enter();
        let result = self.execute_alter_table(alter_table)?;
        for descendant in descendants {
            let mut statement = alter_table.clone();
            statement.name = target_object_name(&descendant);
            // A child that already has the column added to the parent keeps
            // its own definition (PostgreSQL merges them).
            statement.operations.retain(|operation| match operation {
                AlterTableOperation::AddColumn { column_def, .. } => {
                    descendant.column(&ident_value(&column_def.name)).is_none()
                }
                _ => true,
            });
            if !statement.operations.is_empty() {
                self.execute_alter_table(&statement)?;
            }
        }
        Ok(Some(result))
    }

    fn reject_inherited_member_changes(
        &self,
        schema: &TableSchema,
        operations: &[AlterTableOperation],
    ) -> Result<()> {
        let mut parent_columns = BTreeSet::new();
        let mut parent_constraints = BTreeSet::new();
        for parent in &schema.inherits {
            if let Some(parent) = load_schema(self.db_ref(), parent)? {
                parent_columns.extend(parent.columns.iter().map(|column| column.name.clone()));
                parent_constraints.extend(
                    parent
                        .constraints
                        .iter()
                        .map(|c| constraint_name(c).to_string()),
                );
            }
        }
        let inherited = |name: &Ident| parent_columns.contains(&ident_value(name));
        for operation in operations {
            let refusal = match operation {
                AlterTableOperation::DropColumn { column_names, .. } => column_names
                    .iter()
                    .find(|name| inherited(name))
                    .map(|name| format!("cannot drop inherited column \"{}\"", ident_value(name))),
                AlterTableOperation::RenameColumn {
                    old_column_name, ..
                } if inherited(old_column_name) => Some(format!(
                    "cannot rename inherited column \"{}\"",
                    ident_value(old_column_name)
                )),
                AlterTableOperation::AlterColumn {
                    column_name,
                    op: AlterColumnOperation::SetDataType { .. },
                } if inherited(column_name) => Some(format!(
                    "cannot alter inherited column \"{}\"",
                    ident_value(column_name)
                )),
                AlterTableOperation::DropConstraint { name, .. }
                    if parent_constraints.contains(&ident_value(name)) =>
                {
                    Some(format!(
                        "cannot drop inherited constraint \"{}\" of relation \"{}\"",
                        ident_value(name),
                        schema.name
                    ))
                }
                _ => None,
            };
            if let Some(message) = refusal {
                return Err(SqlError::data_exception("42P16", message, None));
            }
        }
        Ok(())
    }
}
