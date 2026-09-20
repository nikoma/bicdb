//! Execute PL/pgSQL table functions in the owning session, preserving its
//! transaction, identity, cancellation token and routine EXECUTE permissions.
use crate::*;

fn plpgsql_table_call(
    db: &BicDb,
    factor: &TableFactor,
) -> Result<Option<(String, Vec<Expr>, Option<TableAlias>)>> {
    let (name, args, alias, ordinality) = match factor {
        TableFactor::Function {
            name,
            args,
            alias,
            with_ordinality,
            ..
        } => {
            let args = args
                .iter()
                .map(|arg| match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(expr.clone()),
                    _ => Err(SqlError::Unsupported(
                        "table functions require positional arguments".into(),
                    )),
                })
                .collect::<Result<Vec<_>>>()?;
            (object_name(name)?, args, alias.clone(), *with_ordinality)
        }
        TableFactor::Table {
            name,
            args: Some(args),
            alias,
            with_ordinality,
            ..
        } => (
            object_name(name)?,
            table_function_expr_args(args)?,
            alias.clone(),
            *with_ordinality,
        ),
        _ => return Ok(None),
    };
    let Some(schema) = load_routine(db, RoutineKind::Function, &name)? else {
        return Ok(None);
    };
    if !schema.returns_set || !routine_language_is_plpgsql(&schema) {
        return Ok(None);
    }
    if ordinality {
        return Err(SqlError::Unsupported(
            "PL/pgSQL functions WITH ORDINALITY are not supported".into(),
        ));
    }
    Ok(Some((name, args, alias)))
}

impl<'db> SqlSession<'db> {
    pub(crate) fn query_has_plpgsql_set_functions(&self, query: &Query) -> Result<bool> {
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                if self.query_has_plpgsql_set_functions(&cte.query)? {
                    return Ok(true);
                }
            }
        }
        if let SetExpr::Select(select) = query.body.as_ref() {
            for from in &select.from {
                for factor in std::iter::once(&from.relation)
                    .chain(from.joins.iter().map(|join| &join.relation))
                {
                    if plpgsql_table_call(self.db_ref(), factor)?.is_some() {
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    }

    fn set_function_columns(&self, name: &str) -> Result<(Vec<String>, Vec<Option<String>>)> {
        let routine = resolve_routine_cached(self.db_ref(), RoutineKind::Function, name)?
            .ok_or_else(|| SqlError::InvalidSql(format!("function {name} does not exist")))?;
        let outputs = routine
            .ir
            .params
            .iter()
            .filter(|param| matches!(param.mode, RoutineArgMode::Out | RoutineArgMode::InOut))
            .collect::<Vec<_>>();
        if outputs.is_empty() {
            if routine.schema.return_type == "record" {
                return Err(SqlError::Unsupported(
                    "record set functions require named output columns".into(),
                ));
            }
            return Ok((
                vec![routine.schema.name.clone()],
                vec![Some(routine.schema.return_type.clone())],
            ));
        }
        Ok((
            outputs
                .iter()
                .map(|param| param.name.clone().unwrap_or_default())
                .collect(),
            outputs
                .iter()
                .map(|param| param.type_schema.as_ref().map(|ty| ty.pg_type.clone()))
                .collect(),
        ))
    }

    fn execute_plpgsql_set_function(&mut self, name: &str, args: &[SqlValue]) -> Result<SqlResult> {
        self.ensure_routine_execute_privilege(name)?;
        let routine = resolve_routine_cached(self.db_ref(), RoutineKind::Function, name)?
            .ok_or_else(|| SqlError::InvalidSql(format!("function {name} does not exist")))?;
        let (columns, column_types) = self.set_function_columns(name)?;
        self.execute_with_routine_security(&routine.schema, |session| {
            let previous_ir = session
                .current_routine_ir
                .replace(routine.ir.as_ref() as *const RoutineIR as usize);
            let _type_scope = session.enter_routine_expr_type_scope(&routine.ir);
            let result = (|| {
                let mut frame = session.routine_frame_for_call(&routine.ir, args)?;
                frame.returned_set =
                    Some(SqlResult::new(columns, Vec::new()).with_column_types(column_types));
                session.initialize_routine_frame(&mut frame, &routine.ir.declarations)?;
                if matches!(
                    session.execute_routine_block(
                        &mut frame,
                        &routine.ir.statements,
                        &routine.ir.exception_handlers
                    )?,
                    RoutineControl::Return(Some(_))
                ) {
                    return Err(SqlError::InvalidSql(
                        "set-returning functions cannot RETURN a scalar value".into(),
                    ));
                }
                Ok(frame.returned_set.take().unwrap())
            })();
            session.current_routine_ir = previous_ir;
            result
        })
    }

    pub(crate) fn try_execute_set_function_query(
        &mut self,
        query: &Query,
        ctes: BTreeMap<String, CteResult>,
    ) -> Result<Option<SqlResult>> {
        let SetExpr::Select(select) = query.body.as_ref() else {
            return Ok(None);
        };
        let [from] = select.from.as_slice() else {
            return Ok(None);
        };
        let base_call = plpgsql_table_call(self.db_ref(), &from.relation)?;
        let calls = from
            .joins
            .iter()
            .map(|join| plpgsql_table_call(self.db_ref(), &join.relation))
            .collect::<Result<Vec<_>>>()?;
        if base_call.is_none() && calls.iter().all(Option::is_none) {
            return Ok(None);
        }
        // Validate permissions even when the left relation is empty.
        for call in base_call.iter().chain(calls.iter().flatten()) {
            self.ensure_routine_execute_privilege(&call.0)?;
        }
        if self.tx.is_none() {
            return Err(SqlError::InvalidTransactionState {
                message: "set-returning routine execution requires its statement transaction"
                    .into(),
            });
        }
        let mut rows = if let Some((name, args, alias)) = base_call {
            let values = args
                .iter()
                .map(|expr| self.eval_session_expr(expr))
                .collect::<Result<Vec<_>>>()?;
            let result = self.execute_plpgsql_set_function(&name, &values)?;
            set_function_row_set(&name, alias.as_ref(), result)?
        } else {
            self.sql_engine_with_ctes(ctes.clone())
                .row_set_from_table_factor_with_selection(&from.relation, None, None)?
        };
        for (join, call) in from.joins.iter().zip(calls) {
            let Some((name, args, alias)) = call else {
                rows = self
                    .sql_engine_with_ctes(ctes.clone())
                    .apply_row_join(rows, join, None, None, None)?;
                continue;
            };
            let (outer, constraint) = match &join.join_operator {
                JoinOperator::Join(c) | JoinOperator::Inner(c) | JoinOperator::CrossJoin(c) => {
                    (false, c)
                }
                JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => (true, c),
                _ => {
                    return Err(SqlError::Unsupported(
                        "correlated table functions support inner and left joins".into(),
                    ))
                }
            };
            if !matches!(constraint, JoinConstraint::On(_) | JoinConstraint::None) {
                return Err(SqlError::Unsupported(
                    "correlated table functions support ON constraints".into(),
                ));
            }
            let (columns, _) = self.set_function_columns(&name)?;
            let empty =
                set_function_row_set(&name, alias.as_ref(), SqlResult::new(columns, Vec::new()))?;
            let mut columns = rows.columns.clone();
            columns.extend(empty.columns.clone());
            let mut combined = Vec::new();
            for left in &rows.rows {
                self.cancellation.check()?;
                let values = {
                    let engine = self.sql_engine_with_ctes(ctes.clone());
                    let (_, context) = engine.bound_row_context(&rows.columns);
                    args.iter()
                        .map(|arg| engine.eval_slot_row_value(left, &context, arg))
                        .collect::<Result<Vec<_>>>()?
                };
                let result = self.execute_plpgsql_set_function(&name, &values)?;
                let right = set_function_row_set(&name, alias.as_ref(), result)?;
                let mut matched = false;
                for row in right.rows {
                    let mut joined = left.clone();
                    joined.extend(row);
                    let include = match constraint {
                        JoinConstraint::On(expr) => {
                            let engine = self.sql_engine_with_ctes(ctes.clone());
                            {
                                let (_, context) = engine.bound_row_context(&columns);
                                engine.eval_slot_row_predicate(&joined, &context, expr)?
                            }
                        }
                        _ => true,
                    };
                    if include {
                        matched = true;
                        combined.push(joined);
                    }
                }
                if outer && !matched {
                    let mut joined = left.clone();
                    joined.extend(std::iter::repeat_n(SqlValue::Null, empty.columns.len()));
                    combined.push(joined);
                }
            }
            rows = RowSet {
                columns,
                rows: combined,
            };
        }
        self.sql_engine_with_ctes(ctes)
            .execute_materialized_row_query(select, query, from, rows)
            .map(Some)
    }
}

fn set_function_row_set(
    name: &str,
    alias: Option<&TableAlias>,
    result: SqlResult,
) -> Result<RowSet> {
    let alias_name = alias
        .map(|alias| alias.name.value.as_str())
        .unwrap_or_else(|| name.rsplit('.').next().unwrap_or(name));
    let columns = table_alias_columns(
        name,
        alias_name,
        alias.map(|alias| alias.columns.as_slice()).unwrap_or(&[]),
        &result.columns,
    )?;
    let rows = result
        .rows
        .iter()
        .map(|row| slot_row_from_values(&columns, row))
        .collect();
    Ok(RowSet {
        columns: aliased_row_output_columns(alias_name, &columns),
        rows,
    })
}
