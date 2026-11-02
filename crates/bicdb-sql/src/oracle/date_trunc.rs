//! Resolve preview datetime operations without changing PostgreSQL arithmetic.
use super::*;

pub(super) fn exact_numeric_operand_sql(expr: &Expr) -> String {
    match expr {
        Expr::Value(value) => match &value.value {
            Value::Number(number, _) => {
                format!("CAST('{}' AS NUMERIC)", number.replace('\'', "''"))
            }
            _ => expr.to_string(),
        },
        Expr::Nested(inner) => format!("({})", exact_numeric_operand_sql(inner)),
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => format!("+({})", exact_numeric_operand_sql(expr)),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => format!("-({})", exact_numeric_operand_sql(expr)),
        _ => expr.to_string(),
    }
}

/// Preserve exact month-count literals before the native numeric evaluator.
pub(super) fn lower_calendar_call(expr: &mut Expr) -> Result<()> {
    if let Expr::Function(function) = expr {
        if function.name.0.len() == 1
            && function.name.0[0]
                .as_ident()
                .is_some_and(|ident| ident.value == "ADD_MONTHS")
        {
            function.name = ObjectName::from(vec![Ident::new("bicdb_oracle_add_months")]);
        }
        if object_name_lowercase(&function.name).is_ok_and(|name| name == "bicdb_oracle_add_months")
        {
            let args = function_args(function);
            if let [date, months] = args.as_slice() {
                *expr = expression(&format!(
                    "bicdb_oracle_add_months({date}, CAST({} AS NUMERIC))",
                    exact_numeric_operand_sql(months)
                ))?;
            }
        }
    }
    Ok(())
}

pub(super) fn numeric_trunc_expr(args: &[Expr]) -> Result<Expr> {
    expression(&format!(
        "bicdb_oracle_number_trunc({})",
        args.iter()
            .map(|arg| format!("CAST({} AS NUMERIC)", exact_numeric_operand_sql(arg)))
            .collect::<Vec<_>>()
            .join(",")
    ))
}

pub(super) fn lower(db: &BicDb, sql: &str, oids: Option<&[i32]>) -> Result<String> {
    if raw_do_block_body(sql)?.is_some() || super::procedures::header(sql)?.is_some() {
        return Ok(sql.to_string());
    }
    if !sql.to_ascii_lowercase().contains("trunc")
        && !sql.to_ascii_lowercase().contains("add_months")
        && !sql.contains(['+', '-', '/'])
    {
        return Ok(sql.to_string());
    }
    let mut statements = parse_statements(sql)?;
    if let Some(oids) = oids.filter(|oids| !oids.is_empty()) {
        let _ = visit_expressions_mut(&mut statements, |expr| {
            if let Expr::Value(value) = expr {
                if let Value::Placeholder(name) = &value.value {
                    let oid = name
                        .strip_prefix('$')
                        .and_then(|n| n.parse::<usize>().ok())
                        .and_then(|n| n.checked_sub(1))
                        .and_then(|n| oids.get(n));
                    let kind = match oid {
                        Some(1082) => Some(DataType::Date),
                        Some(1114) => Some(DataType::Timestamp(
                            None,
                            sqlparser::ast::TimezoneInfo::None,
                        )),
                        _ => None,
                    };
                    if let Some(data_type) = kind {
                        *expr = Expr::Cast {
                            kind: sqlparser::ast::CastKind::Cast,
                            expr: Box::new(expr.clone()),
                            data_type,
                            format: None,
                            array: false,
                        };
                    }
                }
            }
            ControlFlow::<()>::Continue(())
        });
    }
    rewrite_with_parameters(db, &mut statements, oids)?;
    Ok(statements
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; "))
}

pub(super) fn rewrite(db: &BicDb, statements: &mut Vec<Statement>) -> Result<()> {
    rewrite_with_parameters(db, statements, None)
}

fn rewrite_with_parameters(
    db: &BicDb,
    statements: &mut Vec<Statement>,
    oids: Option<&[i32]>,
) -> Result<()> {
    struct Resolve<'a> {
        db: &'a BicDb,
        oids: Option<&'a [i32]>,
        queries: Vec<Query>,
        statement_from: Vec<Option<String>>,
    }
    impl Resolve<'_> {
        fn input_type(&self, arg: &Expr) -> Option<String> {
            if let Expr::Nested(inner) = arg {
                return self.input_type(inner);
            }
            if let Some(source) = self
                .queries
                .last()
                .and_then(|query| temporal_columns::in_query(self.db, query, arg))
            {
                return Some(
                    match source {
                        temporal_columns::Source::Date => "date",
                        temporal_columns::Source::Timestamp => "timestamp",
                        temporal_columns::Source::TimestampWithTimeZone => "timestamptz",
                    }
                    .into(),
                );
            }
            if let Expr::Value(value) = arg {
                if let Value::Placeholder(name) = &value.value {
                    if let Some(oid) = name
                        .strip_prefix('$')
                        .and_then(|n| n.parse::<usize>().ok())
                        .and_then(|n| n.checked_sub(1))
                        .and_then(|n| self.oids.and_then(|oids| oids.get(n)))
                    {
                        if *oid != 0 {
                            return pg_type_spec_by_oid(*oid).map(|spec| spec.name.to_string());
                        }
                    }
                    // The Parse pass supplies declared parameter types later.
                    // Do not guess a bind's overload during schema binding.
                    return None;
                }
            }
            let direct = projected_expr_pg_type(arg, None);
            if direct.is_some() {
                return direct;
            }
            let mut probe = if let Some(query) = self.queries.last() {
                let mut query = query.clone();
                let SetExpr::Select(select) = query.body.as_mut() else {
                    return direct;
                };
                select.projection = vec![SelectItem::UnnamedExpr(arg.clone())];
                query.locks.clear();
                query.to_string()
            } else if let Some(Some(from)) = self.statement_from.last() {
                format!("SELECT {arg} FROM {from}")
            } else {
                return direct;
            };
            // For overload selection TRUNC retains its input's type family.
            // This lets an outer query see a datetime produced by a CTE before
            // the inner rewrite has been reflected in the captured query AST.
            if let Ok(mut ast) = parse_statements(&probe) {
                let _ = visit_expressions_mut(&mut ast, |expr| {
                    if let Expr::Function(function) = expr {
                        if object_name_lowercase(&function.name).is_ok_and(|n| n == "trunc") {
                            if let Some(arg) = function_args(function).first() {
                                *expr = (*arg).clone();
                            }
                        }
                    }
                    ControlFlow::<()>::Continue(())
                });
                probe = ast
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; ");
            }
            infer_query_result_types(self.db, &probe)
                .ok()
                .flatten()
                .and_then(|types| types.into_iter().next().flatten())
                .or(direct)
        }
        fn day_operand(&self, expr: &Expr, pg_type: Option<&str>) -> bool {
            match expr {
                Expr::Nested(inner) => return self.day_operand(inner, pg_type),
                Expr::Value(value)
                    if matches!(value.value, Value::Null | Value::SingleQuotedString(_)) =>
                {
                    return true
                }
                Expr::Value(value)
                    if matches!(value.value, Value::Placeholder(_)) && pg_type.is_none() =>
                {
                    return self.oids.is_some()
                }
                _ => {}
            }
            matches!(
                pg_type,
                Some(
                    "int2"
                        | "int4"
                        | "int8"
                        | "numeric"
                        | "float4"
                        | "float8"
                        | "text"
                        | "varchar"
                        | "bpchar"
                )
            )
        }
    }
    impl sqlparser::ast::VisitorMut for Resolve<'_> {
        type Break = SqlError;
        fn pre_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<SqlError> {
            self.statement_from.push(match statement {
                Statement::Update(update) => Some(update.table.to_string()),
                Statement::CreateIndex(index) => Some(index.table_name.to_string()),
                _ => None,
            });
            ControlFlow::Continue(())
        }
        fn post_visit_statement(&mut self, _: &mut Statement) -> ControlFlow<SqlError> {
            self.statement_from.pop();
            ControlFlow::Continue(())
        }
        fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<SqlError> {
            self.queries.push(query.clone());
            ControlFlow::Continue(())
        }
        fn post_visit_query(&mut self, query: &mut Query) -> ControlFlow<SqlError> {
            let previous = self.queries.pop().expect("query snapshot stack");
            if let Some(parent) = self.queries.last_mut() {
                // Outer metadata probes must see the resolved types of CTEs
                // and derived queries after their datetime expressions change.
                struct Refresh<'a> {
                    previous: &'a Query,
                    resolved: &'a Query,
                }
                impl sqlparser::ast::VisitorMut for Refresh<'_> {
                    type Break = ();
                    fn pre_visit_query(&mut self, candidate: &mut Query) -> ControlFlow<()> {
                        if candidate == self.previous {
                            *candidate = self.resolved.clone();
                        }
                        ControlFlow::Continue(())
                    }
                }
                let _ = sqlparser::ast::VisitMut::visit(
                    parent,
                    &mut Refresh {
                        previous: &previous,
                        resolved: query,
                    },
                );
            }
            ControlFlow::Continue(())
        }
        fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<SqlError> {
            if let Err(error) = lower_calendar_call(expr) {
                return ControlFlow::Break(error);
            }
            if let Expr::BinaryOp { left, op, right } = expr {
                if *op == BinaryOperator::Divide {
                    let left_type = self.input_type(left);
                    let right_type = self.input_type(right);
                    let number = |expr: &Expr, kind: Option<&str>| {
                        !matches!(kind, Some("float4" | "float8")) && self.day_operand(expr, kind)
                    };
                    if number(left, left_type.as_deref()) && number(right, right_type.as_deref()) {
                        match expression(&format!(
                            "bicdb_oracle_number_divide(CAST({} AS NUMERIC),CAST({} AS NUMERIC))",
                            exact_numeric_operand_sql(left),
                            exact_numeric_operand_sql(right)
                        )) {
                            Ok(lowered) => *expr = lowered,
                            Err(error) => return ControlFlow::Break(error),
                        }
                        return ControlFlow::Continue(());
                    }
                }
                if matches!(op, BinaryOperator::Plus | BinaryOperator::Minus) {
                    let left_type = self.input_type(left);
                    let right_type = self.input_type(right);
                    let metadata_fenced = match metadata_v2_enabled(self.db) {
                        Ok(enabled) => enabled,
                        Err(error) => return ControlFlow::Break(error),
                    };
                    if *op == BinaryOperator::Minus
                        && metadata_fenced
                        && matches!(left_type.as_deref(), Some("date" | "timestamp"))
                        && matches!(right_type.as_deref(), Some("date" | "timestamp"))
                        && self.queries.last().is_some_and(|query| {
                            temporal_columns::in_query(self.db, query, left).is_none()
                                || temporal_columns::in_query(self.db, query, right).is_none()
                        })
                    {
                        return ControlFlow::Break(unsupported("temporal subtraction requires source logical type provenance; use an explicit DATE or TIMESTAMP conversion"));
                    }
                    if *op == BinaryOperator::Minus
                        && left_type.as_deref() == Some("date")
                        && right_type.as_deref() == Some("date")
                    {
                        match expression(&format!("bicdb_oracle_date_subtract({left},{right})")) {
                            Ok(lowered) => *expr = lowered,
                            Err(error) => return ControlFlow::Break(error),
                        }
                        return ControlFlow::Continue(());
                    }
                    let direction = if *op == BinaryOperator::Minus { -1 } else { 1 };
                    let operands = if matches!(left_type.as_deref(), Some("date" | "timestamp"))
                        && self.day_operand(right, right_type.as_deref())
                    {
                        Some((left.as_ref(), right.as_ref()))
                    } else if *op == BinaryOperator::Plus
                        && matches!(right_type.as_deref(), Some("date" | "timestamp"))
                        && self.day_operand(left, left_type.as_deref())
                    {
                        Some((right.as_ref(), left.as_ref()))
                    } else {
                        None
                    };
                    if let Some((date, days)) = operands {
                        // Stringify literal tokens before NUMERIC conversion so
                        // a day offset does not pass through floating point.
                        match expression(&format!(
                            "bicdb_oracle_date_add_days({date}, CAST({} AS NUMERIC), {direction})",
                            exact_numeric_operand_sql(days)
                        )) {
                            Ok(lowered) => *expr = lowered,
                            Err(error) => return ControlFlow::Break(error),
                        }
                    }
                }
            }
            if let Expr::Function(function) = expr {
                if object_name_lowercase(&function.name).is_ok_and(|n| n == "trunc") {
                    let args = function_args(function);
                    if let Some(arg) = args.first() {
                        if matches!(self.input_type(arg).as_deref(), Some("date" | "timestamp")) {
                            function.name =
                                ObjectName::from(vec![Ident::new("bicdb_oracle_trunc_date")]);
                        } else if matches!(
                            self.input_type(arg).as_deref(),
                            Some(
                                "numeric"
                                    | "int2"
                                    | "int4"
                                    | "int8"
                                    | "text"
                                    | "varchar"
                                    | "bpchar"
                            )
                        ) || args.len() == 2
                            && matches!(self.input_type(arg).as_deref(), Some("float4" | "float8"))
                        {
                            match numeric_trunc_expr(&args) {
                                Ok(lowered) => *expr = lowered,
                                Err(error) => return ControlFlow::Break(error),
                            }
                        }
                    }
                }
            }
            ControlFlow::Continue(())
        }
    }
    match sqlparser::ast::VisitMut::visit(
        statements,
        &mut Resolve {
            db,
            oids,
            queries: Vec::new(),
            statement_from: Vec::new(),
        },
    ) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(error) => Err(error),
    }
}
