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

/// Whether `expr` is the `<q>.tableoid = '<t>'::regclass` restriction that
/// `UPDATE ONLY` / `DELETE FROM ONLY` are rewritten into.
fn is_only_restriction(expr: &Expr) -> bool {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = expr
    else {
        return false;
    };
    let column = match left.as_ref() {
        Expr::Identifier(ident) => Some(ident),
        Expr::CompoundIdentifier(parts) => parts.last(),
        _ => None,
    };
    column.is_some_and(|ident| ident.value.eq_ignore_ascii_case("tableoid"))
        && matches!(right.as_ref(), Expr::Cast { expr, data_type, .. }
            if matches!(expr.as_ref(), Expr::Value(_))
                && data_type.to_string().eq_ignore_ascii_case("regclass"))
}

/// Removes the ONLY restriction from a WHERE clause. The statement then runs
/// against the target table's own rows, which is what ONLY asks for.
fn strip_only_restriction(selection: &mut Option<Expr>) -> bool {
    let Some(expr) = selection.as_mut() else {
        return false;
    };
    if is_only_restriction(expr) {
        *selection = None;
        return true;
    }
    if let Expr::BinaryOp {
        left,
        op: BinaryOperator::And,
        right,
    } = expr
    {
        if is_only_restriction(right) {
            *expr = (**left).clone();
            return true;
        }
    }
    false
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
    // A table outside public is stored under an encoded name; the statement
    // names it by its own name in its schema.
    let bare = if target.name.starts_with("__bicdb_s_") {
        catalog_display_name(&target.name)
    } else {
        target
            .name
            .rsplit('.')
            .next()
            .unwrap_or(&target.name)
            .to_string()
    };
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
        if !EXPANDING.with(Cell::get) {
            let mut only = update.clone();
            if strip_only_restriction(&mut only.selection) {
                let _guard = ExpansionGuard::enter();
                return self
                    .execute_update_with_match_policy(&only, ctes.clone(), match_policy, None)
                    .map(Some);
            }
        }
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
        if !EXPANDING.with(Cell::get) {
            let mut only = delete.clone();
            if strip_only_restriction(&mut only.selection) {
                let _guard = ExpansionGuard::enter();
                return self.execute_delete_with_ctes(&only, ctes.clone()).map(Some);
            }
        }
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
                // CASCADE drops the descendants first, as PostgreSQL does.
                for child in schema
                    .inheritance_children
                    .iter()
                    .filter(|child| !dropping.iter().any(|name| name == *child))
                {
                    if let Some(child) = load_schema(self.db_ref(), child)? {
                        let statement =
                            format!("DROP TABLE {} CASCADE", target_object_name(&child));
                        self.execute_inner(&statement)?;
                    }
                }
                return self.detach_from_parents(schema);
            }
            return Err(SqlError::dependent_objects_still_exist(format!(
                "cannot drop table {} because other objects depend on it",
                schema.name
            )));
        }
        self.detach_from_parents(schema)
    }

    fn detach_from_parents(&mut self, schema: &TableSchema) -> Result<()> {
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
                        .map(|c| constraint_name(c).to_string())
                        .filter(|name| !parent.no_inherit_constraints.contains(name)),
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

    /// `ALTER TABLE child INHERIT parent` and `NO INHERIT parent`.
    pub(crate) fn alter_table_inheritance(
        &mut self,
        table: &str,
        if_exists: bool,
        parent: &str,
        inherit: bool,
    ) -> Result<SqlResult> {
        let table = resolve_session_relation_name_if_exists(self.db_ref(), table);
        let Some(mut child) = load_schema(self.db_ref(), &table)? else {
            if if_exists {
                return Ok(SqlResult::command("ALTER TABLE"));
            }
            return Err(SqlError::InvalidCollection(table));
        };
        self.require_table_ownership(&child.name, "ALTER TABLE")?;
        let parent_name = resolve_session_relation_name_if_exists(self.db_ref(), parent);
        let Some(mut parent) = load_schema(self.db_ref(), &parent_name)? else {
            return Err(SqlError::data_exception(
                "42P01",
                format!("relation \"{parent}\" does not exist"),
                None,
            ));
        };
        let postgres = !oracle::is_oracle_database(self.db_ref())?;
        if !inherit {
            if !child.inherits.contains(&parent.name) {
                return Err(SqlError::data_exception(
                    "42P01",
                    format!(
                        "relation \"{}\" is not a parent of relation \"{}\"",
                        parent.name, child.name
                    ),
                    None,
                ));
            }
            child.inherits.retain(|name| name != &parent.name);
            if postgres {
                crate::inheritance::preserve_local_columns(self.db_ref(), &mut child, &parent)?;
            }
            parent
                .inheritance_children
                .retain(|name| name != &child.name);
            self.save_session_schema(&child)?;
            self.save_session_schema(&parent)?;
            return Ok(SqlResult::command("ALTER TABLE"));
        }
        self.require_table_ownership(&parent.name, "ALTER TABLE")?;
        let wrong = |message: String| SqlError::data_exception("42804", message, None);
        if child.of_type.is_some() {
            return Err(SqlError::data_exception(
                "42809",
                "cannot change inheritance of typed table",
                None,
            ));
        }
        if parent.partitioning.is_some() || child.partitioning.is_some() {
            return Err(SqlError::Unsupported(
                "ALTER TABLE ... INHERIT with a partitioned table is not supported".to_string(),
            ));
        }
        if child.inherits.contains(&parent.name) {
            return Err(SqlError::data_exception(
                "42P07",
                format!(
                    "relation \"{}\" would be inherited from more than once",
                    parent.name
                ),
                None,
            ));
        }
        if parent.name == child.name
            || crate::inheritance::inheritance_descendants(self.db_ref(), &child)?
                .iter()
                .any(|descendant| descendant.name == parent.name)
        {
            return Err(SqlError::data_exception(
                "42P07",
                "circular inheritance not allowed".to_string(),
                None,
            ));
        }
        let child_name = relation_name_for_message(&child.name);
        for column in parent.columns.iter().filter(|column| !column.hidden) {
            let Some(own) = child.column(&column.name) else {
                return Err(wrong(format!(
                    "child table is missing column \"{}\"",
                    column.name
                )));
            };
            if own.pg_type != column.pg_type || own.type_modifier != column.type_modifier {
                return Err(wrong(format!(
                    "child table \"{}\" has different type for column \"{}\"",
                    child.name, column.name
                )));
            }
            if postgres && column.collation_oid() != own.collation_oid() {
                return Err(SqlError::data_exception(
                    "42P21",
                    format!(
                        "child table \"{child_name}\" has different collation for column \"{}\"",
                        column.name
                    ),
                    None,
                ));
            }
            if !column.nullable && own.nullable {
                return Err(wrong(format!(
                    "column \"{}\" in child table must be marked NOT NULL",
                    column.name
                )));
            }
            if postgres {
                let parent_kind = crate::inheritance::column_generation_kind(
                    column,
                    &parent.virtual_generated_columns,
                );
                let own_kind = crate::inheritance::column_generation_kind(
                    own,
                    &child.virtual_generated_columns,
                );
                let column = &column.name;
                match (parent_kind, own_kind) {
                    (Some(parent), Some(own)) if parent != own => {
                        return Err(SqlError::coded(
                            "42804",
                            format!("column \"{column}\" inherits from generated column of different kind"),
                            Some(format!("Parent column is {parent}, child column is {own}.")),
                            None,
                        ));
                    }
                    (Some(_), None) => {
                        return Err(wrong(format!(
                            "column \"{column}\" in child table must be a generated column"
                        )));
                    }
                    (None, Some(_)) => {
                        return Err(wrong(format!(
                            "column \"{column}\" in child table must not be a generated column"
                        )));
                    }
                    _ => {}
                }
            }
        }
        for constraint in &parent.constraints {
            let ConstraintSchema::Check {
                name,
                expression: parent_expression,
                validated: parent_validated,
            } = constraint
            else {
                continue;
            };
            if parent.no_inherit_constraints.contains(name) {
                continue;
            }
            let Some(ConstraintSchema::Check { expression, validated, .. }) = child
                .constraints
                .iter()
                .find(|candidate| matches!(candidate, ConstraintSchema::Check { name: existing, .. } if existing == name))
            else {
                return Err(wrong(format!("child table is missing constraint \"{name}\"")));
            };
            if postgres {
                if !crate::inheritance::same_inherited_check(expression, parent_expression)? {
                    return Err(wrong(format!(
                        "child table \"{child_name}\" has different definition for check constraint \"{name}\""
                    )));
                }
                let conflict = if child.no_inherit_constraints.contains(name) {
                    Some("non-inherited")
                } else if *parent_validated && !*validated {
                    Some("NOT VALID")
                } else {
                    None
                };
                if let Some(conflict) = conflict {
                    return Err(SqlError::data_exception(
                        "42P17",
                        format!("constraint \"{name}\" conflicts with {conflict} constraint on child table \"{child_name}\""),
                        None,
                    ));
                }
            }
        }
        if postgres {
            crate::inheritance::preserve_local_columns(self.db_ref(), &mut child, &parent)?;
        }
        child.inherits.push(parent.name.clone());
        parent.inheritance_children.push(child.name.clone());
        self.save_session_schema(&child)?;
        self.save_session_schema(&parent)?;
        Ok(SqlResult::command("ALTER TABLE"))
    }
}
