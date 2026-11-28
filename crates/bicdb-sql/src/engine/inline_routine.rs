//! SQL-standard bodies are analyzed when the routine is defined.
use super::*;
use sqlparser::ast::{ArgMode, CastKind, Visit, Visitor};
use std::ops::ControlFlow;

pub(crate) fn validate_inline_sql_routine(
    engine: &SqlEngine<'_>,
    declaration: &CreateFunction,
    routine: &RoutineSchema,
) -> Result<()> {
    let args = declaration
        .args
        .as_deref()
        .unwrap_or_default()
        .iter()
        .zip(&routine.arg_types)
        .filter(|(arg, _)| arg.mode != Some(ArgMode::Out))
        .collect::<Vec<_>>();
    let mut resolver = ParameterTypeResolver::new(engine);
    resolver.check_comparisons = true;
    resolver.inline_routine_scope = Some(RelationColumns {
        alias: routine.name.clone(),
        row_type: None,
        columns: args
            .iter()
            .filter_map(|(arg, kind)| {
                Some((ident_value(arg.name.as_ref()?), Some(kind.pg_type.clone())))
            })
            .collect(),
    });
    let mut relations = RoutineRelations {
        db: engine.db_ref(),
        ctes: Vec::new(),
    };
    let result_types = match declaration.function_body.as_ref() {
        Some(CreateFunctionBody::Return(expression)) => {
            let mut expression = expression.clone();
            bind_analysis_parameters(&mut expression, &args)?;
            finish_walk(expression.visit(&mut relations))?;
            let scope = resolver
                .inline_routine_scope
                .clone()
                .expect("routine scope");
            let env = std::slice::from_ref(&scope);
            resolver.check_inline_projection_column(&expression, env, &[]);
            resolver.expr(&expression, env, None);
            Some(vec![if crate::planner::is_unknown_literal(&expression) {
                Some("text".to_string())
            } else {
                resolver.expr_type(&expression, env)
            }])
        }
        Some(CreateFunctionBody::AsBeginEnd(block)) => {
            let mut statements = block.statements.clone();
            bind_analysis_parameters(&mut statements, &args)?;
            finish_walk(statements.visit(&mut relations))?;
            for statement in &statements {
                resolver.statement(statement);
            }
            match statements.last() {
                Some(Statement::Query(query)) => engine
                    .query_output_columns(query)
                    .map(|columns| columns.into_iter().map(|(_, kind)| kind).collect()),
                None => Some(Vec::new()),
                // DML RETURNING and composite result validation require their
                // target-column environments.
                Some(_) => None,
            }
        }
        _ => return Ok(()),
    };
    if let Some(error) = resolver.comparison_error {
        return Err(error);
    }
    validate_scalar_result(routine, result_types.as_deref())
}

fn bind_analysis_parameters(
    body: &mut impl sqlparser::ast::VisitMut,
    args: &[(&sqlparser::ast::OperateFunctionArg, &RoutineTypeSchema)],
) -> Result<()> {
    // Substitute types in the analyzer's copy. The persisted body still
    // references the actual arguments supplied at each call.
    finish_walk(sqlparser::ast::visit_expressions_mut(body, |expression| {
        let Some(index) = placeholder_index(expression) else {
            return ControlFlow::Continue(());
        };
        let Some((argument, _)) = index.checked_sub(1).and_then(|index| args.get(index)) else {
            return ControlFlow::Break(SqlError::data_exception_public(
                "42P02",
                format!("there is no parameter ${index}"),
                None,
            ));
        };
        *expression = Expr::Cast {
            kind: CastKind::Cast,
            expr: Box::new(Expr::Value(Value::Null.into())),
            data_type: argument.data_type.clone(),
            array: false,
            format: None,
        };
        ControlFlow::Continue(())
    }))
}

struct RoutineRelations<'a> {
    db: &'a BicDb,
    ctes: Vec<Vec<String>>,
}

impl Visitor for RoutineRelations<'_> {
    type Break = SqlError;

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<SqlError> {
        self.ctes
            .push(query.with.as_ref().map_or_else(Vec::new, |with| {
                with.cte_tables
                    .iter()
                    .map(|cte| ident_value(&cte.alias.name))
                    .collect()
            }));
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _: &Query) -> ControlFlow<SqlError> {
        self.ctes.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<SqlError> {
        let TableFactor::Table {
            name, args: None, ..
        } = factor
        else {
            return ControlFlow::Continue(());
        };
        let Ok(table) = relation_name(name) else {
            return ControlFlow::Continue(());
        };
        if (name.0.len() == 1 && self.ctes.iter().rev().flatten().any(|cte| cte == &table))
            || is_virtual_table(&table)
            || resolve_relation_name_memoized(self.db, &table).is_some()
        {
            return ControlFlow::Continue(());
        }
        match load_sequence(self.db, &table) {
            Ok(Some(_)) => ControlFlow::Continue(()),
            Ok(None) => ControlFlow::Break(SqlError::data_exception_public(
                "42P01",
                format!("relation \"{name}\" does not exist"),
                None,
            )),
            Err(error) => ControlFlow::Break(error),
        }
    }
}

impl ParameterTypeResolver<'_, '_> {
    pub(super) fn check_inline_projection_column(
        &mut self,
        expression: &Expr,
        env: &[RelationColumns],
        from: &[TableWithJoins],
    ) {
        let Expr::Identifier(column) = expression else {
            return;
        };
        let name = ident_value(column);
        if self.comparison_error.is_none()
            && self.expr_type(expression, env).is_none()
            && !env.iter().any(|relation| {
                relation.alias == name || relation.columns.iter().any(|(column, _)| column == &name)
            })
            && self.engine.from_relation_columns(from).is_some()
        {
            self.comparison_error = Some(SqlError::data_exception_public(
                "42703",
                format!("column \"{name}\" does not exist"),
                None,
            ));
        }
    }
}

fn validate_scalar_result(
    routine: &RoutineSchema,
    result_types: Option<&[Option<String>]>,
) -> Result<()> {
    let target = &routine.return_type;
    if target == "void" || target == "record" || pg_type_oid_by_name(target).is_none() {
        return Ok(());
    }
    let Some(result_types) = result_types else {
        return Ok(());
    };
    let detail = match result_types {
        [] => "Function's final statement must be SELECT or INSERT/UPDATE/DELETE/MERGE RETURNING."
            .to_string(),
        [Some(actual)]
            if pg_type_oid_by_name(actual).is_some()
                && !pg_cast_allows(actual, target, PgCastContext::Assignment) =>
        {
            format!(
                "Actual return type is {}.",
                crate::eval::pg_cast_display_type(actual)
            )
        }
        [_] => return Ok(()),
        _ => "Final statement must return exactly one column.".to_string(),
    };
    let declared = crate::eval::pg_cast_display_type(target);
    Err(SqlError::with_detail(
        SqlError::data_exception_public(
            "42P13",
            format!("return type mismatch in function declared to return {declared}"),
            None,
        ),
        detail,
    )
    .with_context_line(format!("SQL function \"{}\"", routine.name)))
}

fn finish_walk(flow: ControlFlow<SqlError>) -> Result<()> {
    match flow {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(error) => Err(error),
    }
}
