//! SQL-visible columns of a join with USING or NATURAL.
use super::*;
use sqlparser::ast::{VisitMut, VisitorMut};
use std::ops::ControlFlow;

#[derive(Clone, Copy)]
enum MergedSide {
    Inner,
    Left,
    Right,
    Full,
}

pub(super) struct MergedColumn {
    name: String,
    expression: Expr,
}

impl SqlEngine<'_> {
    pub(super) fn rewrite_named_join_select(
        &self,
        select: &Select,
    ) -> Result<Option<(Select, Vec<MergedColumn>)>> {
        let [from] = select.from.as_slice() else {
            return Ok(None);
        };
        let [join] = from.joins.as_slice() else {
            return Ok(None);
        };
        let (constraint, side) = match &join.join_operator {
            JoinOperator::Join(constraint) | JoinOperator::Inner(constraint) => {
                (constraint, MergedSide::Inner)
            }
            JoinOperator::Left(constraint) | JoinOperator::LeftOuter(constraint) => {
                (constraint, MergedSide::Left)
            }
            JoinOperator::Right(constraint) | JoinOperator::RightOuter(constraint) => {
                (constraint, MergedSide::Right)
            }
            JoinOperator::FullOuter(constraint) => (constraint, MergedSide::Full),
            _ => return Ok(None),
        };
        if !matches!(
            constraint,
            JoinConstraint::Using(_) | JoinConstraint::Natural
        ) || oracle::is_oracle_database(self.db_ref())?
        {
            return Ok(None);
        }

        let Some(left) = self.named_join_input(&from.relation) else {
            return Ok(None);
        };
        let Some(right) = self.named_join_input(&join.relation) else {
            return Ok(None);
        };
        let names = match constraint {
            JoinConstraint::Using(names) => {
                names.iter().map(object_name).collect::<Result<Vec<_>>>()?
            }
            JoinConstraint::Natural => {
                let mut names = Vec::new();
                for (name, _) in &left.columns {
                    if right.columns.iter().any(|(candidate, _)| candidate == name)
                        && !names.contains(name)
                    {
                        names.push(name.clone());
                    }
                }
                names
            }
            _ => unreachable!(),
        };
        let mut merged = Vec::with_capacity(names.len());
        let mut equality = None;
        for name in &names {
            if merged
                .iter()
                .any(|column: &MergedColumn| column.name == *name)
            {
                return Err(SqlError::data_exception_public(
                    "42701",
                    format!("column name \"{name}\" appears more than once in USING clause"),
                    None,
                ));
            }
            let left_type = using_input_type(&left, name, "left")?;
            let right_type = using_input_type(&right, name, "right")?;
            let common = select_common_pg_type(
                self.db_ref(),
                &[left_type.clone(), right_type.clone()],
                "JOIN/USING",
            )?;
            let left_column = qualified_column(&left.alias, name);
            let right_column = qualified_column(&right.alias, name);
            let predicate = Expr::BinaryOp {
                left: Box::new(left_column.clone()),
                op: BinaryOperator::Eq,
                right: Box::new(right_column.clone()),
            };
            equality = Some(match equality {
                None => predicate,
                Some(previous) => Expr::BinaryOp {
                    left: Box::new(previous),
                    op: BinaryOperator::And,
                    right: Box::new(predicate),
                },
            });
            let from_left = || cast_join_column(left_column, left_type.as_deref(), &common);
            let from_right = || cast_join_column(right_column, right_type.as_deref(), &common);
            let value = match side {
                MergedSide::Right => from_right()?,
                MergedSide::Inner
                    if left_type.as_deref() != Some(common.as_str())
                        && right_type.as_deref() == Some(common.as_str()) =>
                {
                    from_right()?
                }
                MergedSide::Inner | MergedSide::Left => from_left()?,
                MergedSide::Full => {
                    let args = vec![from_left()?, from_right()?]
                        .into_iter()
                        .map(|expr| FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)))
                        .collect();
                    Expr::Function(Function {
                        name: ObjectName::from(vec![Ident::new("coalesce")]),
                        uses_odbc_syntax: false,
                        parameters: FunctionArguments::None,
                        args: FunctionArguments::List(sqlparser::ast::FunctionArgumentList {
                            duplicate_treatment: None,
                            args,
                            clauses: Vec::new(),
                        }),
                        filter: None,
                        null_treatment: None,
                        over: None,
                        within_group: Vec::new(),
                    })
                }
            };
            merged.push(MergedColumn {
                name: name.clone(),
                expression: value,
            });
        }

        let rewritten_constraint = JoinConstraint::On(
            equality.unwrap_or_else(|| Expr::value(sqlparser::ast::Value::Boolean(true))),
        );
        let mut output = select.clone();
        output.from[0].joins[0].join_operator = match side {
            MergedSide::Inner => JoinOperator::Inner(rewritten_constraint),
            MergedSide::Left => JoinOperator::Left(rewritten_constraint),
            MergedSide::Right => JoinOperator::Right(rewritten_constraint),
            MergedSide::Full => JoinOperator::FullOuter(rewritten_constraint),
        };
        output.projection.clear();
        for mut item in select.projection.clone() {
            match item {
                SelectItem::Wildcard(_) => {
                    output.projection.extend(merged.iter().map(|column| {
                        SelectItem::ExprWithAlias {
                            expr: column.expression.clone(),
                            alias: Ident::with_quote('"', &column.name),
                        }
                    }));
                    for input in [&left, &right] {
                        output
                            .projection
                            .extend(input.columns.iter().filter_map(|(name, _)| {
                                (!names.contains(name)).then(|| {
                                    SelectItem::UnnamedExpr(qualified_column(&input.alias, name))
                                })
                            }));
                    }
                    continue;
                }
                SelectItem::UnnamedExpr(ref mut expr) => {
                    if let Expr::Identifier(ident) = expr {
                        if let Some(column) = merged
                            .iter()
                            .find(|column| column.name == ident_value(ident))
                        {
                            item = SelectItem::ExprWithAlias {
                                expr: column.expression.clone(),
                                alias: Ident::with_quote('"', &column.name),
                            };
                        }
                    }
                }
                _ => {}
            }
            let _ = item.visit(&mut ReplaceMergedColumns::new(&merged));
            output.projection.push(item);
        }
        let mut rewrite = ReplaceMergedColumns::new(&merged);
        let _ = output.selection.visit(&mut rewrite);
        let _ = output.group_by.visit(&mut rewrite);
        let _ = output.having.visit(&mut rewrite);
        Ok(Some((output, merged)))
    }

    fn named_join_input(&self, factor: &TableFactor) -> Option<RelationColumns> {
        if !matches!(
            factor,
            TableFactor::Table { args: None, .. } | TableFactor::Derived { .. }
        ) {
            return None;
        }
        let mut env = Vec::new();
        self.collect_relation_columns(factor, &mut env)?;
        let [relation] = <[RelationColumns; 1]>::try_from(env).ok()?;
        (!relation.alias.is_empty()
            && relation
                .columns
                .iter()
                .all(|(name, _)| name != column_check::UNKNOWN_CTE_COLUMN))
        .then_some(relation)
    }

    pub(super) fn rewrite_named_join_query(&self, query: &Query) -> Result<Option<Box<Query>>> {
        let SetExpr::Select(select) = query.body.as_ref() else {
            return Ok(None);
        };
        let Some((select, merged)) = self.rewrite_named_join_select(select)? else {
            return Ok(None);
        };
        let output_names = select
            .projection
            .iter()
            .filter_map(|item| match item {
                SelectItem::ExprWithAlias { alias, .. } => Some(ident_value(alias)),
                SelectItem::UnnamedExpr(expr) => Some(row_expr_column_name(expr)),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        let mut query = Box::new(query.clone());
        query.body = Box::new(SetExpr::Select(Box::new(select)));
        if let Some(OrderBy {
            kind: OrderByKind::Expressions(order),
            ..
        }) = &mut query.order_by
        {
            for order in order {
                if !matches!(&order.expr, Expr::Identifier(ident) if output_names.contains(&ident_value(ident)))
                {
                    let _ = order.expr.visit(&mut ReplaceMergedColumns::new(&merged));
                }
            }
        }
        Ok(Some(query))
    }
}

fn using_input_type(input: &RelationColumns, name: &str, side: &str) -> Result<Option<String>> {
    let mut columns = input
        .columns
        .iter()
        .filter(|(candidate, _)| candidate == name);
    let Some((_, data_type)) = columns.next() else {
        return Err(SqlError::data_exception_public(
            "42703",
            format!("column \"{name}\" specified in USING clause does not exist in {side} table"),
            None,
        ));
    };
    if columns.next().is_some() {
        return Err(SqlError::data_exception_public(
            "42702",
            format!("common column name \"{name}\" appears more than once in {side} table"),
            None,
        ));
    }
    Ok(data_type.clone())
}

fn qualified_column(alias: &str, name: &str) -> Expr {
    Expr::CompoundIdentifier(vec![
        Ident::with_quote('"', alias),
        Ident::with_quote('"', name),
    ])
}

fn cast_join_column(column: Expr, input: Option<&str>, common: &str) -> Result<Expr> {
    if input.is_none_or(|input| input == common) {
        return Ok(column);
    }
    let data_type = Parser::new(&PostgreSqlDialect {})
        .try_with_sql(common)
        .and_then(|mut parser| parser.parse_data_type())
        .map_err(|error| SqlError::InvalidSql(error.to_string()))?;
    Ok(Expr::Cast {
        kind: sqlparser::ast::CastKind::DoubleColon,
        expr: Box::new(column),
        data_type,
        array: false,
        format: None,
    })
}

struct ReplaceMergedColumns<'a> {
    columns: &'a [MergedColumn],
    query_depth: usize,
}

impl<'a> ReplaceMergedColumns<'a> {
    fn new(columns: &'a [MergedColumn]) -> Self {
        Self {
            columns,
            query_depth: 0,
        }
    }
}

impl VisitorMut for ReplaceMergedColumns<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, _: &mut Query) -> ControlFlow<()> {
        self.query_depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _: &mut Query) -> ControlFlow<()> {
        self.query_depth -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        if self.query_depth == 0 {
            if let Expr::Identifier(ident) = expr {
                if let Some(column) = self
                    .columns
                    .iter()
                    .find(|column| column.name == ident_value(ident))
                {
                    *expr = column.expression.clone();
                }
            }
        }
        ControlFlow::Continue(())
    }
}
