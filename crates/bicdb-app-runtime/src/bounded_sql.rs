use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;

use bicdb_extension::abi_v2::DatabaseAction;
use sqlparser::ast::{
    Expr, FromTable, ObjectName, Query, Statement, TableFactor, TableObject, Visit, Visitor,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::{AppRuntimeError, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BoundedSqlAnalysis {
    pub relations: BTreeSet<String>,
    pub routines: BTreeSet<String>,
    pub actions: BTreeSet<DatabaseAction>,
    pub parameter_count: usize,
}

#[derive(Default)]
struct CteScope {
    aliases: BTreeMap<String, usize>,
    visible_count: usize,
    // Query addresses identify definition nodes during this immutable walk.
    // Values are visibility limits, not cloned sets of earlier sibling names.
    definition_inputs: BTreeMap<usize, usize>,
    parent_visible_count: Option<usize>,
}

#[derive(Default)]
struct SqlAuthorityVisitor {
    relations: BTreeSet<String>,
    routines: BTreeSet<String>,
    explicit_tables: BTreeSet<String>,
    actions: BTreeSet<DatabaseAction>,
    cte_scopes: Vec<CteScope>,
    parameters: BTreeSet<usize>,
    invalid_parameter: Option<String>,
}

impl SqlAuthorityVisitor {
    fn record_target(&mut self, name: &ObjectName) {
        // DML targets are base objects even if a visible CTE has the same name.
        let name = normalize_name(name);
        self.relations.insert(name.clone());
        self.explicit_tables.insert(name);
    }
}

impl Visitor for SqlAuthorityVisitor {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        let parent_visible_count = self.cte_scopes.last_mut().and_then(|parent| {
            let limit = parent
                .definition_inputs
                .get(&(query as *const Query as usize))?;
            Some(std::mem::replace(&mut parent.visible_count, *limit))
        });
        let mut scope = CteScope {
            parent_visible_count,
            ..CteScope::default()
        };
        if let Some(with) = &query.with {
            scope.visible_count = with.cte_tables.len();
            for (index, cte) in with.cte_tables.iter().enumerate() {
                scope.aliases.insert(cte_name(&cte.alias.name), index);
                // Nonrecursive definitions see earlier siblings and enclosing
                // scopes; recursive definitions see every sibling. Restore the
                // parent's visibility as soon as the definition walk ends.
                scope.definition_inputs.insert(
                    cte.query.as_ref() as *const Query as usize,
                    if with.recursive {
                        with.cte_tables.len()
                    } else {
                        index
                    },
                );
            }
        }
        self.cte_scopes.push(scope);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _: &Query) -> ControlFlow<Self::Break> {
        if let Some(previous) = self
            .cte_scopes
            .pop()
            .and_then(|scope| scope.parent_visible_count)
        {
            if let Some(parent) = self.cte_scopes.last_mut() {
                parent.visible_count = previous;
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<Self::Break> {
        match statement {
            Statement::Query(_) => {
                self.actions.insert(DatabaseAction::Select);
            }
            Statement::Insert(insert) => {
                if let TableObject::TableName(name) = &insert.table {
                    self.record_target(name);
                }
                self.actions.insert(if insert.on.is_some() {
                    DatabaseAction::Upsert
                } else {
                    DatabaseAction::Insert
                });
            }
            Statement::Update(update) => {
                if let TableFactor::Table { name, .. } = &update.table.relation {
                    self.record_target(name);
                }
                self.actions.insert(DatabaseAction::Update);
            }
            Statement::Delete(delete) => {
                let (FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables)) =
                    &delete.from;
                for table in tables {
                    if let TableFactor::Table { name, .. } = &table.relation {
                        self.record_target(name);
                    }
                }
                self.actions.insert(DatabaseAction::Delete);
            }
            Statement::Call(function) => {
                self.actions.insert(DatabaseAction::Execute);
                self.routines.insert(normalize_name(&function.name));
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, table: &TableFactor) -> ControlFlow<Self::Break> {
        match table {
            TableFactor::Table { name, args, .. } => {
                let name = normalize_name(name);
                if args.is_some() {
                    self.actions.insert(DatabaseAction::Execute);
                    self.routines.insert(name);
                } else {
                    self.explicit_tables.insert(name);
                }
            }
            TableFactor::Function { name, .. } => {
                self.actions.insert(DatabaseAction::Execute);
                self.routines.insert(normalize_name(name));
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
        // A schema-qualified name always denotes a base relation. Classify
        // occurrences here: a nested alias must never erase an earlier read.
        let name = relation
            .0
            .first()
            .and_then(|part| part.as_ident())
            .map(cte_name);
        let is_cte = relation.0.len() == 1
            && name.is_some_and(|name| {
                self.cte_scopes.iter().rev().any(|scope| {
                    scope
                        .aliases
                        .get(&name)
                        .is_some_and(|index| *index < scope.visible_count)
                })
            });
        if !is_cte {
            self.relations.insert(normalize_name(relation));
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expression: &Expr) -> ControlFlow<Self::Break> {
        match expression {
            Expr::Function(function) => {
                self.actions.insert(DatabaseAction::Execute);
                self.routines.insert(normalize_name(&function.name));
            }
            Expr::Value(value) => {
                if let sqlparser::ast::Value::Placeholder(parameter) = &value.value {
                    match parameter
                        .strip_prefix('$')
                        .and_then(|value| value.parse::<usize>().ok())
                        .filter(|value| *value > 0)
                    {
                        Some(parameter) => {
                            self.parameters.insert(parameter);
                        }
                        None => self.invalid_parameter = Some(parameter.clone()),
                    }
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

pub(crate) fn analyze_bounded_sql(sql: &str) -> Result<BoundedSqlAnalysis> {
    if sql.len() > 1024 * 1024 {
        return Err(AppRuntimeError::InvalidPackage(
            "declared SQL exceeds the 1 MiB bound".to_string(),
        ));
    }
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, sql).map_err(|error| {
        AppRuntimeError::InvalidPackage(format!("declared SQL does not parse: {error}"))
    })?;
    let [statement] = statements.as_slice() else {
        return Err(AppRuntimeError::InvalidPackage(
            "declared SQL must contain exactly one statement".to_string(),
        ));
    };
    if !matches!(
        statement,
        Statement::Query(_)
            | Statement::Insert(_)
            | Statement::Update(_)
            | Statement::Delete(_)
            | Statement::Call(_)
    ) {
        return Err(AppRuntimeError::InvalidPackage(format!(
            "declared SQL statement `{statement}` is outside the bounded query/DML/CALL profile"
        )));
    }

    let mut visitor = SqlAuthorityVisitor::default();
    let _ = statement.visit(&mut visitor);
    if let Some(parameter) = visitor.invalid_parameter {
        return Err(AppRuntimeError::InvalidPackage(format!(
            "declared SQL parameter `{parameter}` must use a positive PostgreSQL $n placeholder"
        )));
    }
    visitor.relations.retain(|relation| {
        !visitor.routines.contains(relation) || visitor.explicit_tables.contains(relation)
    });
    let parameter_count = visitor.parameters.last().copied().unwrap_or(0);
    // PostgreSQL permits sparse positional references (for example `$1`,
    // `$2`, and `$4` with four bound parameters). The highest placeholder is
    // the prepared statement's arity; unused positions remain valid binds.
    // A relation- and routine-free SELECT is a capability-free projection:
    // its exact signed SQL can evaluate literals, parameters, casts, and
    // operators without granting table or routine authority. Every non-query
    // statement still necessarily records its target relation or routine, and
    // the signed ABI independently restricts empty authority to SELECT only.
    Ok(BoundedSqlAnalysis {
        relations: visitor.relations,
        routines: visitor.routines,
        actions: visitor.actions,
        parameter_count,
    })
}

fn cte_name(name: &sqlparser::ast::Ident) -> String {
    if name.quote_style.is_some() {
        name.value.clone()
    } else {
        name.value.to_ascii_lowercase()
    }
}

fn normalize_name(name: &ObjectName) -> String {
    name.to_string()
        .split('.')
        .map(|part| part.trim_matches('"').to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cte_authority_follows_lexical_scope_and_definition_order() {
        for (sql, expected) in [
            ("SELECT secret FROM secrets WHERE EXISTS (WITH secrets AS (SELECT 1 AS id) SELECT 1 FROM secrets)", vec!["secrets"]),
            ("SELECT * FROM (WITH secrets AS (SELECT 1) SELECT * FROM secrets) s, secrets", vec!["secrets"]),
            ("WITH secrets AS (SELECT * FROM secrets) SELECT * FROM secrets", vec!["secrets"]),
            ("WITH a AS (SELECT * FROM b), b AS (SELECT 1) SELECT * FROM a", vec!["b"]),
            ("WITH a AS (SELECT * FROM base), b AS (SELECT * FROM a) SELECT * FROM b", vec!["base"]),
            ("WITH RECURSIVE a AS (SELECT 1 UNION ALL SELECT * FROM a) SELECT * FROM a", vec![]),
            ("WITH RECURSIVE a AS (SELECT * FROM b), b AS (SELECT * FROM base) SELECT * FROM a", vec!["base"]),
            ("WITH secrets AS (SELECT 1) SELECT * FROM public.secrets", vec!["public.secrets"]),
            ("WITH a AS (SELECT * FROM base) SELECT * FROM a WHERE EXISTS (SELECT * FROM a)", vec!["base"]),
            ("WITH a AS (SELECT * FROM base) SELECT * FROM (WITH a AS (SELECT * FROM a) SELECT * FROM a) s", vec!["base"]),
            ("WITH A AS (SELECT 1) SELECT * FROM a", vec![]),
            (r#"WITH "A" AS (SELECT 1) SELECT * FROM a"#, vec!["a"]),
            ("DELETE FROM secrets WHERE EXISTS (WITH secrets AS (SELECT 1) SELECT * FROM secrets)", vec!["secrets"]),
            ("WITH secrets AS (SELECT 1) DELETE FROM secrets", vec!["secrets"]),
            ("WITH secrets AS (SELECT 1) UPDATE secrets SET id = 2", vec!["secrets"]),
            ("WITH secrets AS (SELECT 1) INSERT INTO secrets SELECT * FROM secrets", vec!["secrets"]),
            ("INSERT INTO secrets SELECT * FROM (WITH secrets AS (SELECT 1) SELECT * FROM secrets) s", vec!["secrets"]),
        ] {
            assert_eq!(
                analyze_bounded_sql(sql).unwrap().relations,
                expected.into_iter().map(str::to_string).collect(),
                "{sql}",
            );
        }
    }

    #[test]
    fn large_sibling_cte_chain_keeps_its_base_authority() {
        let mut sql = "WITH c0 AS (SELECT * FROM base)".to_string();
        for index in 1..3000 {
            sql.push_str(&format!(", c{index} AS (SELECT * FROM c{})", index - 1));
        }
        sql.push_str(" SELECT * FROM c2999");
        assert_eq!(
            analyze_bounded_sql(&sql).unwrap().relations,
            BTreeSet::from(["base".into()])
        );
    }

    #[test]
    fn analyzes_one_bounded_statement_and_rejects_authority_ambiguity() {
        let analysis = analyze_bounded_sql(
            "SELECT city, count(*) FROM doctors WHERE tenant_id = $1 GROUP BY city",
        )
        .unwrap();
        assert_eq!(analysis.relations, BTreeSet::from(["doctors".to_string()]));
        assert_eq!(analysis.routines, BTreeSet::from(["count".to_string()]));
        assert_eq!(
            analysis.actions,
            BTreeSet::from([DatabaseAction::Select, DatabaseAction::Execute])
        );
        assert_eq!(analysis.parameter_count, 1);

        let table_function = analyze_bounded_sql("SELECT * FROM doctor_for_clinic($1)").unwrap();
        assert!(table_function.relations.is_empty());
        assert_eq!(
            table_function.routines,
            BTreeSet::from(["doctor_for_clinic".to_string()])
        );
        assert_eq!(table_function.parameter_count, 1);

        let lateral_function = analyze_bounded_sql(
            "SELECT value FROM doctors CROSS JOIN LATERAL jsonb_array_elements(payload) AS item(value)",
        )
        .unwrap();
        assert_eq!(
            lateral_function.relations,
            BTreeSet::from(["doctors".to_string()])
        );
        assert_eq!(
            lateral_function.routines,
            BTreeSet::from(["jsonb_array_elements".to_string()])
        );

        let procedure = analyze_bounded_sql("CALL carrier_touch($1)").unwrap();
        assert!(procedure.relations.is_empty());
        assert_eq!(
            procedure.routines,
            BTreeSet::from(["carrier_touch".to_string()])
        );

        let projection = analyze_bounded_sql("SELECT $1::text").unwrap();
        assert!(projection.relations.is_empty());
        assert!(projection.routines.is_empty());
        assert_eq!(projection.actions, BTreeSet::from([DatabaseAction::Select]));
        assert_eq!(projection.parameter_count, 1);

        let sparse = analyze_bounded_sql("SELECT $1::text, $4::text").unwrap();
        assert_eq!(sparse.parameter_count, 4);

        assert!(analyze_bounded_sql("SET ROLE root").is_err());
        assert!(analyze_bounded_sql("SELECT * FROM doctors; DELETE FROM doctors").is_err());
        assert_eq!(
            analyze_bounded_sql("SELECT * FROM doctors WHERE id = $2")
                .unwrap()
                .parameter_count,
            2
        );
    }
}
