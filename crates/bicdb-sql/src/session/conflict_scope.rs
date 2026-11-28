//! The proposed row is visible as EXCLUDED in DO UPDATE expressions and its
//! WHERE clause. It has no system columns and is outside RETURNING's scope.
use super::*;
use sqlparser::ast::{SelectItemQualifiedWildcardKind, Visit, Visitor};
use std::ops::ControlFlow;

pub(super) fn validate_excluded_references(
    insert: &Insert,
    table: &str,
    schema: &TableSchema,
) -> Result<()> {
    let Some(OnInsert::OnConflict(conflict)) = &insert.on else {
        return Ok(());
    };
    let target_alias = insert.table_alias.as_ref().map_or_else(
        || catalog_display_name(table),
        |alias| ident_value(&alias.alias),
    );
    let target_is_excluded = target_alias == "excluded";
    let mut scope = ExcludedScope {
        schema,
        target_is_excluded,
        has_update: matches!(conflict.action, OnConflictAction::DoUpdate(_)),
        returning: false,
        local_excluded: Vec::new(),
    };

    if let OnConflictAction::DoUpdate(update) = &conflict.action {
        for assignment in &update.assignments {
            scope.walk(&assignment.value)?;
        }
        if let Some(selection) = &update.selection {
            scope.walk(selection)?;
        }
    }
    if !target_is_excluded {
        if let Some(items) = &insert.returning {
            scope.returning = true;
            for item in items {
                if let Some(error) = scope.item_error(item) {
                    return Err(error);
                }
                scope.walk(item)?;
            }
        }
    }
    Ok(())
}

struct ExcludedScope<'a> {
    schema: &'a TableSchema,
    target_is_excluded: bool,
    has_update: bool,
    returning: bool,
    local_excluded: Vec<bool>,
}

impl ExcludedScope<'_> {
    fn walk(&mut self, value: &impl Visit) -> Result<()> {
        match value.visit(self) {
            ControlFlow::Continue(()) => Ok(()),
            ControlFlow::Break(error) => Err(*error),
        }
    }

    fn item_error(&self, item: &SelectItem) -> Option<SqlError> {
        if let SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::ObjectName(name), _) =
            item
        {
            if excluded_qualifier(name) {
                return self.qualified_error(None);
            }
        }
        None
    }

    fn qualified_error(&self, column: Option<&Ident>) -> Option<SqlError> {
        if self.local_excluded.iter().any(|bound| *bound) {
            return None;
        }
        if self.returning {
            if self.has_update {
                return Some(SqlError::coded(
                    "42P01",
                    "invalid reference to FROM-clause entry for table \"excluded\"",
                    Some("There is an entry for table \"excluded\", but it cannot be referenced from this part of the query.".to_string()),
                    None,
                ));
            }
            return Some(SqlError::data_exception(
                "42P01",
                "missing FROM-clause entry for table \"excluded\"",
                None,
            ));
        }
        if self.target_is_excluded {
            return Some(ambiguous_excluded());
        }
        let name = ident_value(column?);
        self.schema
            .column(&name)
            .is_none_or(|column| column.hidden)
            .then(|| {
                SqlError::data_exception(
                    "42703",
                    format!(
                        "column excluded.{} does not exist",
                        pg_partition_key_identifier(&name)
                    ),
                    None,
                )
            })
    }
}

impl Visitor for ExcludedScope<'_> {
    type Break = Box<SqlError>;

    fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<Self::Break> {
        self.local_excluded
            .push(super::updatable_views::select_qualifiers(select).contains("excluded"));
        if let Some(error) = select
            .projection
            .iter()
            .find_map(|item| self.item_error(item))
        {
            return ControlFlow::Break(Box::new(error));
        }
        ControlFlow::Continue(())
    }

    fn post_visit_select(&mut self, _: &Select) -> ControlFlow<Self::Break> {
        self.local_excluded.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        let error = match expr {
            Expr::CompoundIdentifier(parts)
                if parts.len() >= 2 && ident_value(&parts[0]) == "excluded" =>
            {
                self.qualified_error(parts.get(1))
            }
            Expr::QualifiedWildcard(name, _) if excluded_qualifier(name) => {
                self.qualified_error(None)
            }
            Expr::Identifier(name)
                if self.local_excluded.is_empty()
                    && ident_value(name) == "excluded"
                    && self.schema.column("excluded").is_none() =>
            {
                if self.returning {
                    Some(SqlError::data_exception(
                        "42703",
                        "column \"excluded\" does not exist",
                        None,
                    ))
                } else {
                    self.target_is_excluded.then(ambiguous_excluded)
                }
            }
            _ => None,
        };
        match error {
            Some(error) => ControlFlow::Break(Box::new(error)),
            None => ControlFlow::Continue(()),
        }
    }
}

fn excluded_qualifier(name: &ObjectName) -> bool {
    matches!(name.0.as_slice(), [part]
        if part.as_ident().is_some_and(|ident| ident_value(ident) == "excluded"))
}

fn ambiguous_excluded() -> SqlError {
    SqlError::data_exception("42P09", "table reference \"excluded\" is ambiguous", None)
}
