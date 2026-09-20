//! Split out of the parent module to keep files digestible; behavior
//! unchanged. Methods live in a separate `impl` block on the same type.
use super::*;
#[allow(unused_imports)]
use crate::*;

fn qualified_session_keyword_column_parts(
    function: &Function,
    normalized_name: &str,
) -> Option<Vec<String>> {
    if !matches!(function.args, sqlparser::ast::FunctionArguments::None) {
        return None;
    }
    let parts = normalized_name
        .split('.')
        .map(str::to_string)
        .collect::<Vec<_>>();
    if parts.len() < 2
        || !matches!(
            parts.last().map(String::as_str),
            Some("current_user" | "current_role" | "session_user" | "user")
        )
    {
        return None;
    }
    Some(parts)
}

impl<'db> SqlEngine<'db> {
    /// `row_columns_expr_pg_type` for a row-context expression, memoized per
    /// routine IR node when this engine runs a statement embedded in a
    /// routine. Predicate evaluation asked for both operand types on EVERY
    /// row; the answer depends only on the node and its statement's column
    /// set, both fixed for the life of the IR. Ad-hoc statements (no IR) keep
    /// the direct computation.
    pub(crate) fn row_expr_pg_type(
        &self,
        expr: &Expr,
        context: &BoundRowContext,
    ) -> std::rc::Rc<Option<String>> {
        let Some(ir) = self.routine_ir else {
            return std::rc::Rc::new(row_columns_expr_pg_type(
                self.db_ref(),
                expr,
                context.column_keys().iter().map(String::as_str),
            ));
        };
        // The routine entry captured the catalog generation once; fall back
        // to a lock read only when no scope is active.
        let generation = crate::eval::expr_type_scope()
            .map(|(generation, _)| generation)
            .unwrap_or_else(|| self.db_ref().collection_generation(ROUTINE_COLLECTION));
        let key = (generation, ir, expr as *const Expr as usize);
        if let Some(hit) = ROW_EXPR_TYPE_MEMO.with(|memo| memo.borrow().get(&key).cloned()) {
            return hit;
        }
        let computed = std::rc::Rc::new(row_columns_expr_pg_type(
            self.db_ref(),
            expr,
            context.column_keys().iter().map(String::as_str),
        ));
        ROW_EXPR_TYPE_MEMO.with(|memo| {
            let mut memo = memo.borrow_mut();
            if memo.len() >= ROW_EXPR_TYPE_MEMO_MAX {
                memo.clear();
            }
            memo.insert(key, std::rc::Rc::clone(&computed));
        });
        computed
    }
}

impl<'db> SqlEngine<'db> {
    /// The type of a slot-row expression: memoized per IR node when the
    /// caller vouched the expression is IR-owned (`memo_operand_types`, see
    /// `execute_update_with_ctes`), inferred directly otherwise.
    pub(crate) fn scoped_slot_row_expr_type(
        &self,
        context: &BoundRowContext,
        expr: &Expr,
    ) -> Option<String> {
        if self.memo_operand_types {
            (*self.row_expr_pg_type(expr, context)).clone()
        } else {
            self.infer_slot_row_expr_type(context.row_lookup.columns(), expr)
        }
    }
}

const ROW_EXPR_TYPE_MEMO_MAX: usize = 65_536;

thread_local! {
    /// CASE nodes whose branch types were already checked for a common type
    /// (keyed like `ROW_EXPR_TYPE_MEMO`, on the WHEN list's address). The
    /// check's only output is the error, so a validated node stays valid for
    /// the catalog generation.
    static CASE_VALIDATED_MEMO: std::cell::RefCell<rustc_hash::FxHashSet<(u64, usize, usize)>> =
        std::cell::RefCell::new(rustc_hash::FxHashSet::default());
}

thread_local! {
    static ROW_EXPR_TYPE_MEMO: std::cell::RefCell<
        rustc_hash::FxHashMap<(u64, usize, usize), std::rc::Rc<Option<String>>>,
    > = std::cell::RefCell::new(rustc_hash::FxHashMap::default());
}

impl<'db> SqlEngine<'db> {
    pub(crate) fn eval_row_predicate(&self, row: &SqlRow, expr: &Expr) -> Result<bool> {
        Ok(self.eval_row_truth(row, expr)?.unwrap_or(false))
    }

    pub(crate) fn eval_row_truth_typed(
        &self,
        row: &SqlRow,
        expr: &Expr,
        env: &[RelationColumns],
    ) -> Result<Option<bool>> {
        match expr {
            Expr::Nested(inner) => self.eval_row_truth_typed(row, inner, env),
            Expr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
                let left = self.eval_row_truth_typed(row, left, env)?;
                if matches!(left, Some(false)) {
                    return Ok(Some(false));
                }
                Ok(sql_and(left, self.eval_row_truth_typed(row, right, env)?))
            }
            Expr::BinaryOp { left, op, right } if *op == BinaryOperator::Or => {
                let left = self.eval_row_truth_typed(row, left, env)?;
                if matches!(left, Some(true)) {
                    return Ok(Some(true));
                }
                Ok(sql_or(left, self.eval_row_truth_typed(row, right, env)?))
            }
            Expr::BinaryOp { left, op, right } if *op == BinaryOperator::AtAt => {
                let left_value = self.eval_row_value(row, left)?;
                let right_value = self.eval_row_value(row, right)?;
                let left_type = resolved_text_search_operand_type(
                    self.infer_env_expr_type(left, env).or_else(|| {
                        row_columns_expr_pg_type(
                            self.db_ref(),
                            left,
                            row.keys().map(String::as_str),
                        )
                    }),
                    left,
                    &left_value,
                );
                let right_type = resolved_text_search_operand_type(
                    self.infer_env_expr_type(right, env).or_else(|| {
                        row_columns_expr_pg_type(
                            self.db_ref(),
                            right,
                            row.keys().map(String::as_str),
                        )
                    }),
                    right,
                    &right_value,
                );
                eval_atat_match_value(
                    left_value,
                    right_value,
                    left_type.as_deref(),
                    right_type.as_deref(),
                )
            }
            Expr::BinaryOp { left, op, right } if *op == BinaryOperator::AtQuestion => {
                eval_jsonpath_operator_value(
                    self.eval_row_value(row, left)?,
                    self.eval_row_value(row, right)?,
                    false,
                )
                .and_then(sql_value_truth)
            }
            Expr::BinaryOp { left, op, right }
                if matches!(
                    op,
                    BinaryOperator::Eq
                        | BinaryOperator::NotEq
                        | BinaryOperator::Gt
                        | BinaryOperator::GtEq
                        | BinaryOperator::Lt
                        | BinaryOperator::LtEq
                ) =>
            {
                let left_type = self.infer_env_expr_type(left, env);
                let right_type = self.infer_env_expr_type(right, env);
                reject_undefined_comparison(left_type.as_deref(), op, right_type.as_deref())?;
                let pg_type = if matches!(left_type.as_deref(), Some("inet" | "cidr"))
                    && matches!(right_type.as_deref(), Some("inet" | "cidr"))
                {
                    Some("inet".to_string())
                } else {
                    left_type.clone().or_else(|| right_type.clone())
                };
                let Some(pg_type) = pg_type else {
                    return self.eval_row_truth(row, expr);
                };
                let left = enforce_integer_value_type(
                    self.eval_row_value(row, left)?,
                    left_type.as_deref(),
                )?;
                let right = enforce_integer_value_type(
                    self.eval_row_value(row, right)?,
                    right_type.as_deref(),
                )?;
                if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
                    return Ok(None);
                }
                let ordering = pg_typed_compare_for_db(self.db_ref(), &pg_type, &left, &right)?;
                Ok(Some(match op {
                    BinaryOperator::Eq => ordering == Ordering::Equal,
                    BinaryOperator::NotEq => ordering != Ordering::Equal,
                    BinaryOperator::Gt => ordering == Ordering::Greater,
                    BinaryOperator::GtEq => ordering != Ordering::Less,
                    BinaryOperator::Lt => ordering == Ordering::Less,
                    BinaryOperator::LtEq => ordering != Ordering::Greater,
                    _ => unreachable!(),
                }))
            }
            _ => self.eval_row_truth(row, expr),
        }
    }

    pub(crate) fn eval_slot_row_predicate(
        &self,
        row: &SlotRow,
        context: &BoundRowContext,
        expr: &Expr,
    ) -> Result<bool> {
        Ok(self
            .eval_slot_row_truth(row, context, expr)?
            .unwrap_or(false))
    }

    pub(crate) fn filter_row_predicate_typed(
        &self,
        rows: Vec<SlotRow>,
        columns: &[String],
        expr: &Expr,
        env: &[RelationColumns],
    ) -> Result<Vec<SlotRow>> {
        let (_scope, context) = self.bound_row_context(columns);
        let mut filtered = Vec::new();
        for (index, row) in rows.into_iter().enumerate() {
            if index % 1024 == 0 {
                self.check_cancellation()?;
            }
            if self
                .eval_slot_row_truth_typed(&row, &context, expr, env)?
                .unwrap_or(false)
            {
                filtered.push(row);
            }
        }
        Ok(filtered)
    }

    pub(crate) fn eval_slot_row_truth_typed(
        &self,
        row: &SlotRow,
        context: &BoundRowContext,
        expr: &Expr,
        env: &[RelationColumns],
    ) -> Result<Option<bool>> {
        match expr {
            Expr::Nested(inner) => self.eval_slot_row_truth_typed(row, context, inner, env),
            Expr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
                let left = self.eval_slot_row_truth_typed(row, context, left, env)?;
                if matches!(left, Some(false)) {
                    return Ok(Some(false));
                }
                Ok(sql_and(
                    left,
                    self.eval_slot_row_truth_typed(row, context, right, env)?,
                ))
            }
            Expr::BinaryOp { left, op, right } if *op == BinaryOperator::Or => {
                let left = self.eval_slot_row_truth_typed(row, context, left, env)?;
                if matches!(left, Some(true)) {
                    return Ok(Some(true));
                }
                Ok(sql_or(
                    left,
                    self.eval_slot_row_truth_typed(row, context, right, env)?,
                ))
            }
            Expr::BinaryOp { left, op, right } if *op == BinaryOperator::AtAt => {
                let left_value = self.eval_slot_row_value(row, context, left)?;
                let right_value = self.eval_slot_row_value(row, context, right)?;
                let left_type = resolved_text_search_operand_type(
                    self.infer_env_expr_type(left, env)
                        .or_else(|| (*self.row_expr_pg_type(left, context)).clone()),
                    left,
                    &left_value,
                );
                let right_type = resolved_text_search_operand_type(
                    self.infer_env_expr_type(right, env)
                        .or_else(|| (*self.row_expr_pg_type(right, context)).clone()),
                    right,
                    &right_value,
                );
                eval_atat_match_value(
                    left_value,
                    right_value,
                    left_type.as_deref(),
                    right_type.as_deref(),
                )
            }
            Expr::BinaryOp { left, op, right } if *op == BinaryOperator::AtQuestion => {
                eval_jsonpath_operator_value(
                    self.eval_slot_row_value(row, context, left)?,
                    self.eval_slot_row_value(row, context, right)?,
                    false,
                )
                .and_then(sql_value_truth)
            }
            Expr::BinaryOp { left, op, right }
                if matches!(
                    op,
                    BinaryOperator::Eq
                        | BinaryOperator::NotEq
                        | BinaryOperator::Gt
                        | BinaryOperator::GtEq
                        | BinaryOperator::Lt
                        | BinaryOperator::LtEq
                ) =>
            {
                let left_type = self.infer_env_expr_type(left, env);
                let right_type = self.infer_env_expr_type(right, env);
                reject_undefined_comparison(left_type.as_deref(), op, right_type.as_deref())?;
                let pg_type = if matches!(left_type.as_deref(), Some("inet" | "cidr"))
                    && matches!(right_type.as_deref(), Some("inet" | "cidr"))
                {
                    Some("inet".to_string())
                } else {
                    left_type.clone().or_else(|| right_type.clone())
                };
                let Some(pg_type) = pg_type else {
                    return self.eval_slot_row_truth(row, context, expr);
                };
                let left = enforce_integer_value_type(
                    self.eval_slot_row_value(row, context, left)?,
                    left_type.as_deref(),
                )?;
                let right = enforce_integer_value_type(
                    self.eval_slot_row_value(row, context, right)?,
                    right_type.as_deref(),
                )?;
                if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
                    return Ok(None);
                }
                let ordering = pg_typed_compare_for_db(self.db_ref(), &pg_type, &left, &right)?;
                Ok(Some(match op {
                    BinaryOperator::Eq => ordering == Ordering::Equal,
                    BinaryOperator::NotEq => ordering != Ordering::Equal,
                    BinaryOperator::Gt => ordering == Ordering::Greater,
                    BinaryOperator::GtEq => ordering != Ordering::Less,
                    BinaryOperator::Lt => ordering == Ordering::Less,
                    BinaryOperator::LtEq => ordering != Ordering::Greater,
                    _ => unreachable!(),
                }))
            }
            _ => self.eval_slot_row_truth(row, context, expr),
        }
    }

    pub(crate) fn eval_slot_row_truth(
        &self,
        row: &SlotRow,
        context: &BoundRowContext,
        expr: &Expr,
    ) -> Result<Option<bool>> {
        match expr {
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::And => {
                    let left = self.eval_slot_row_truth(row, context, left)?;
                    if matches!(left, Some(false)) {
                        return Ok(Some(false));
                    }
                    Ok(sql_and(
                        left,
                        self.eval_slot_row_truth(row, context, right)?,
                    ))
                }
                BinaryOperator::Or => {
                    let left = self.eval_slot_row_truth(row, context, left)?;
                    if matches!(left, Some(true)) {
                        return Ok(Some(true));
                    }
                    Ok(sql_or(left, self.eval_slot_row_truth(row, context, right)?))
                }
                BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq => {
                    let mut eval = |expr: &Expr| self.eval_slot_row_value(row, context, expr);
                    if let Some(truth) = eval_tuple_comparison(left, op, right, &mut eval)? {
                        return Ok(truth);
                    }
                    let left_type = self.row_expr_pg_type(left, context);
                    let right_type = self.row_expr_pg_type(right, context);
                    reject_undefined_comparison(left_type.as_deref(), op, right_type.as_deref())?;
                    let left = enforce_integer_value_type(
                        self.eval_slot_row_value(row, context, left)?,
                        left_type.as_deref(),
                    )?;
                    let right = enforce_integer_value_type(
                        self.eval_slot_row_value(row, context, right)?,
                        right_type.as_deref(),
                    )?;
                    if matches!(left_type.as_deref(), Some("inet" | "cidr"))
                        && matches!(right_type.as_deref(), Some("inet" | "cidr"))
                    {
                        if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
                            return Ok(None);
                        }
                        return Ok(Some(comparison_from_ordering(
                            op,
                            pg_typed_compare("inet", &left, &right)?,
                        )));
                    }
                    if left_type == right_type
                        && matches!(left_type.as_deref(), Some("macaddr" | "macaddr8"))
                    {
                        if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
                            return Ok(None);
                        }
                        return Ok(Some(comparison_from_ordering(
                            op,
                            pg_typed_compare(left_type.as_deref().unwrap(), &left, &right)?,
                        )));
                    }
                    if left_type.as_deref() == Some("pg_lsn") && left_type == right_type {
                        if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
                            return Ok(None);
                        }
                        return Ok(Some(comparison_from_ordering(
                            op,
                            pg_typed_compare("pg_lsn", &left, &right)?,
                        )));
                    }
                    compare_values(&left, op, &right)
                }
                BinaryOperator::PGLikeMatch
                | BinaryOperator::PGILikeMatch
                | BinaryOperator::PGNotLikeMatch
                | BinaryOperator::PGNotILikeMatch
                | BinaryOperator::PGRegexMatch
                | BinaryOperator::PGRegexIMatch
                | BinaryOperator::PGRegexNotMatch
                | BinaryOperator::PGRegexNotIMatch => {
                    let left = self.eval_slot_row_value(row, context, left)?;
                    let right = self.eval_slot_row_value(row, context, right)?;
                    eval_pg_pattern_operator(&left, op, &right)
                }
                BinaryOperator::AtArrow | BinaryOperator::ArrowAt => {
                    let left_type = self.row_expr_pg_type(left, context);
                    let right_type = self.row_expr_pg_type(right, context);
                    reject_unsupported_geometric_binary(
                        op,
                        left_type.as_deref(),
                        right_type.as_deref(),
                    )?;
                    let left = self.eval_slot_row_value(row, context, left)?;
                    let right = self.eval_slot_row_value(row, context, right)?;
                    if let Some(value) = eval_geometric_binary_value(
                        &left,
                        op,
                        &right,
                        left_type.as_deref(),
                        right_type.as_deref(),
                    )? {
                        return sql_value_truth(value);
                    }
                    if let Some(value) = eval_range_binary_value_with_db(
                        self.db_ref(),
                        left.clone(),
                        op,
                        right.clone(),
                        left_type.as_deref(),
                        right_type.as_deref(),
                    )? {
                        return sql_value_truth(value);
                    }
                    eval_containment_truth(left, op, right)
                }
                _ if {
                    let left_type = self.row_expr_pg_type(left, context);
                    let right_type = self.row_expr_pg_type(right, context);
                    geometric_binary_result_pg_type(op, left_type.as_deref(), right_type.as_deref())
                        .as_deref()
                        == Some("bool")
                } =>
                {
                    let left_type = self.row_expr_pg_type(left, context);
                    let right_type = self.row_expr_pg_type(right, context);
                    eval_geometric_binary_value(
                        &self.eval_slot_row_value(row, context, left)?,
                        op,
                        &self.eval_slot_row_value(row, context, right)?,
                        left_type.as_deref(),
                        right_type.as_deref(),
                    )?
                    .ok_or_else(|| {
                        SqlError::Unsupported(format!("unsupported WHERE operator {op}"))
                    })
                    .and_then(sql_value_truth)
                }
                _ if {
                    let left_type = self.row_expr_pg_type(left, context);
                    let right_type = self.row_expr_pg_type(right, context);
                    network_binary_result_pg_type(op, left_type.as_deref(), right_type.as_deref())
                        .as_deref()
                        == Some("bool")
                } =>
                {
                    let left_type = self.row_expr_pg_type(left, context);
                    let right_type = self.row_expr_pg_type(right, context);
                    eval_network_binary_value(
                        &self.eval_slot_row_value(row, context, left)?,
                        op,
                        &self.eval_slot_row_value(row, context, right)?,
                        left_type.as_deref(),
                        right_type.as_deref(),
                    )?
                    .ok_or_else(|| {
                        SqlError::Unsupported(format!("unsupported WHERE operator {op}"))
                    })
                    .and_then(sql_value_truth)
                }
                _ if is_range_set_operator(op) => {
                    let left_value = self.eval_slot_row_value(row, context, left)?;
                    let right_value = self.eval_slot_row_value(row, context, right)?;
                    let left_type = self.row_expr_pg_type(left, context);
                    let right_type = self.row_expr_pg_type(right, context);
                    if let Some(value) = eval_range_binary_value_with_db(
                        self.db_ref(),
                        left_value.clone(),
                        op,
                        right_value.clone(),
                        left_type.as_deref(),
                        right_type.as_deref(),
                    )? {
                        return sql_value_truth(value);
                    }
                    if let Some(value) = eval_array_binary_value(
                        &left_value,
                        op,
                        &right_value,
                        left_type.as_deref(),
                        right_type.as_deref(),
                    )? {
                        return sql_value_truth(value);
                    }
                    Err(SqlError::Unsupported(format!(
                        "unsupported WHERE operator {op}"
                    )))
                }
                BinaryOperator::AtAt => {
                    let left_value = self.eval_slot_row_value(row, context, left)?;
                    let right_value = self.eval_slot_row_value(row, context, right)?;
                    let left_type = resolved_text_search_operand_type(
                        (*self.row_expr_pg_type(left, context)).clone(),
                        left,
                        &left_value,
                    );
                    let right_type = resolved_text_search_operand_type(
                        (*self.row_expr_pg_type(right, context)).clone(),
                        right,
                        &right_value,
                    );
                    eval_atat_match_value(
                        left_value,
                        right_value,
                        left_type.as_deref(),
                        right_type.as_deref(),
                    )
                }
                BinaryOperator::AtQuestion => eval_jsonpath_operator_value(
                    self.eval_slot_row_value(row, context, left)?,
                    self.eval_slot_row_value(row, context, right)?,
                    false,
                )
                .and_then(sql_value_truth),
                BinaryOperator::Question
                | BinaryOperator::QuestionAnd
                | BinaryOperator::QuestionPipe => eval_json_existence_value(
                    self.eval_slot_row_value(row, context, left)?,
                    op,
                    self.eval_slot_row_value(row, context, right)?,
                )
                .and_then(sql_value_truth),
                _ => Err(SqlError::Unsupported(format!(
                    "unsupported WHERE operator {op}"
                ))),
            },
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let mut eval = |expr: &Expr| self.eval_slot_row_value(row, context, expr);
                if let Some(truth) = eval_tuple_in_list_truth(expr, list, *negated, &mut eval)? {
                    return Ok(truth);
                }
                let value = self.eval_slot_row_value(row, context, expr)?;
                let candidates = list
                    .iter()
                    .map(|candidate| self.eval_slot_row_value(row, context, candidate))
                    .collect::<Result<Vec<_>>>()?;
                let matched = candidates
                    .iter()
                    .any(|candidate| values_equal(&value, candidate));
                if matched {
                    Ok(Some(!*negated))
                } else if matches!(value, SqlValue::Null)
                    || candidates
                        .iter()
                        .any(|candidate| matches!(candidate, SqlValue::Null))
                {
                    Ok(None)
                } else {
                    Ok(Some(*negated))
                }
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                if let Some(truth) = self
                    .eval_slot_row_tuple_in_subquery_truth(row, context, expr, subquery, *negated)?
                {
                    return Ok(truth);
                }
                let value = self.eval_slot_row_value(row, context, expr)?;
                let values =
                    self.execute_correlated_slot_subquery_values(row, context, subquery)?;
                let matched = values
                    .iter()
                    .any(|candidate| values_equal(&value, candidate));
                if matched {
                    Ok(Some(!*negated))
                } else if matches!(value, SqlValue::Null)
                    || values
                        .iter()
                        .any(|candidate| matches!(candidate, SqlValue::Null))
                {
                    Ok(None)
                } else {
                    Ok(Some(*negated))
                }
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let mut eval = |expr: &Expr| self.eval_slot_row_value(row, context, expr);
                if let Some(truth) = eval_tuple_between_truth(expr, low, high, *negated, &mut eval)?
                {
                    return Ok(truth);
                }
                eval_between_truth(
                    self.eval_slot_row_value(row, context, expr)?,
                    self.eval_slot_row_value(row, context, low)?,
                    self.eval_slot_row_value(row, context, high)?,
                    *negated,
                )
            }
            Expr::AnyOp {
                left,
                compare_op,
                right,
                ..
            } => eval_quantified_truth(
                self.eval_slot_row_value(row, context, left)?,
                compare_op,
                self.eval_slot_row_value(row, context, right)?,
                false,
            ),
            Expr::AllOp {
                left,
                compare_op,
                right,
            } => eval_quantified_truth(
                self.eval_slot_row_value(row, context, left)?,
                compare_op,
                self.eval_slot_row_value(row, context, right)?,
                true,
            ),
            Expr::IsNull(expr) => Ok(Some(value_is_null_predicate(
                &self.eval_slot_row_value(row, context, expr)?,
            ))),
            Expr::IsNotNull(expr) => Ok(Some(value_is_not_null_predicate(
                &self.eval_slot_row_value(row, context, expr)?,
            ))),
            Expr::IsDistinctFrom(left, right) => {
                let mut eval = |expr: &Expr| self.eval_slot_row_value(row, context, expr);
                if let Some(not_distinct) = eval_tuple_not_distinct(left, right, &mut eval)? {
                    return Ok(Some(!not_distinct));
                }
                Ok(Some(!values_not_distinct(
                    &self.eval_slot_row_value(row, context, left)?,
                    &self.eval_slot_row_value(row, context, right)?,
                )))
            }
            Expr::IsNotDistinctFrom(left, right) => {
                let mut eval = |expr: &Expr| self.eval_slot_row_value(row, context, expr);
                if let Some(not_distinct) = eval_tuple_not_distinct(left, right, &mut eval)? {
                    return Ok(Some(not_distinct));
                }
                Ok(Some(values_not_distinct(
                    &self.eval_slot_row_value(row, context, left)?,
                    &self.eval_slot_row_value(row, context, right)?,
                )))
            }
            Expr::Like {
                negated,
                any,
                expr,
                pattern,
                escape_char,
            } => {
                if *any {
                    return Err(SqlError::Unsupported(
                        "LIKE ANY is not supported".to_string(),
                    ));
                }
                eval_like_values(
                    self.eval_slot_row_value(row, context, expr)?,
                    self.eval_slot_row_value(row, context, pattern)?,
                    *negated,
                    false,
                    escape_char.as_ref(),
                )
            }
            Expr::ILike {
                negated,
                any,
                expr,
                pattern,
                escape_char,
            } => {
                if *any {
                    return Err(SqlError::Unsupported(
                        "ILIKE ANY is not supported".to_string(),
                    ));
                }
                eval_like_values(
                    self.eval_slot_row_value(row, context, expr)?,
                    self.eval_slot_row_value(row, context, pattern)?,
                    *negated,
                    true,
                    escape_char.as_ref(),
                )
            }
            Expr::SimilarTo { .. } => Err(SqlError::Unsupported(
                "SIMILAR TO is not supported".to_string(),
            )),
            Expr::IsTrue(expr) => Ok(Some(matches!(
                self.eval_slot_row_truth(row, context, expr)?,
                Some(true)
            ))),
            Expr::IsNotTrue(expr) => Ok(Some(!matches!(
                self.eval_slot_row_truth(row, context, expr)?,
                Some(true)
            ))),
            Expr::IsFalse(expr) => Ok(Some(matches!(
                self.eval_slot_row_truth(row, context, expr)?,
                Some(false)
            ))),
            Expr::IsNotFalse(expr) => Ok(Some(!matches!(
                self.eval_slot_row_truth(row, context, expr)?,
                Some(false)
            ))),
            Expr::IsUnknown(expr) => Ok(Some(
                self.eval_slot_row_truth(row, context, expr)?.is_none(),
            )),
            Expr::IsNotUnknown(expr) => Ok(Some(
                self.eval_slot_row_truth(row, context, expr)?.is_some(),
            )),
            Expr::Exists { subquery, negated } => {
                let exists = !self
                    .execute_correlated_slot_query(row, context, subquery)?
                    .rows
                    .is_empty();
                Ok(Some(if *negated { !exists } else { exists }))
            }
            Expr::Subquery(query) => {
                sql_value_truth(self.execute_correlated_slot_scalar_subquery(row, context, query)?)
            }
            Expr::Value(_) | Expr::TypedString(_) => {
                sql_value_truth(self.eval_slot_row_value(row, context, expr)?)
            }
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Case { .. } => {
                match self.eval_slot_row_value(row, context, expr)? {
                    SqlValue::Bool(value) => Ok(Some(value)),
                    SqlValue::Null => Ok(None),
                    other => Err(SqlError::Unsupported(format!(
                        "WHERE expression {expr} returned non-boolean {}",
                        other.to_cell()
                    ))),
                }
            }
            Expr::Function(function) => {
                match self.eval_slot_row_function_value(row, context, function)? {
                    SqlValue::Bool(value) => Ok(Some(value)),
                    SqlValue::Null => Ok(None),
                    other => Err(SqlError::Unsupported(format!(
                        "WHERE function {} returned non-boolean {}",
                        function.name,
                        other.to_cell()
                    ))),
                }
            }
            Expr::UnaryOp { op, expr } if op.to_string().eq_ignore_ascii_case("NOT") => {
                Ok(sql_not(self.eval_slot_row_truth(row, context, expr)?))
            }
            Expr::Nested(expr) => self.eval_slot_row_truth(row, context, expr),
            other => Err(SqlError::Unsupported(format!(
                "unsupported WHERE expression {other}"
            ))),
        }
    }

    // CURRENT_USER / CURRENT_ROLE / SESSION_USER are reserved keywords, so
    // like PostgreSQL they resolve to the session identity, never to columns.
    pub(crate) fn session_identity_value(&self, name: &str) -> Option<SqlValue> {
        match name.to_ascii_lowercase().as_str() {
            "current_database" => Some(SqlValue::String(current_database_from_gucs(
                &self.session_gucs,
            ))),
            "current_user" | "current_role" => {
                Some(SqlValue::String(current_user_from_gucs(&self.session_gucs)))
            }
            "session_user" => Some(SqlValue::String(session_user_from_gucs(&self.session_gucs))),
            _ => None,
        }
    }

    pub(crate) fn eval_slot_row_value(
        &self,
        row: &SlotRow,
        context: &BoundRowContext,
        expr: &Expr,
    ) -> Result<SqlValue> {
        match expr {
            Expr::Identifier(ident) => {
                if ident.value.eq_ignore_ascii_case("current_date") {
                    return Ok(SqlValue::String(unix_now_date_string()));
                }
                if ident.value.eq_ignore_ascii_case("current_schema") {
                    return Ok(SqlValue::String("public".to_string()));
                }
                if ident.quote_style.is_none() {
                    if let Some(value) = self.session_identity_value(&ident.value) {
                        return Ok(value);
                    }
                }
                if let Some(value) = context
                    .row_lookup
                    .value_from_parts(row, std::slice::from_ref(&ident.value))
                {
                    return Ok(value);
                }
                if let Some(value) = context
                    .row_lookup
                    .scalar_value_for_relation_alias(row, &ident.value)
                {
                    return Ok(value);
                }
                if let Some(value) = context
                    .row_lookup
                    .composite_value_for_relation_alias(row, &ident.value)
                {
                    return Ok(hydrate_relation_composite(self.db_ref(), value));
                }
                if let Some(value) = routine_var_from_ident(&self.routine_vars, ident) {
                    return Ok(value);
                }
                if let Some(outer_row) = &self.outer_row {
                    if let Some(value) =
                        outer_row.value_from_parts(std::slice::from_ref(&ident.value))
                    {
                        return Ok(value);
                    }
                }
                Ok(SqlValue::Null)
            }
            Expr::Value(value) => routine_var_from_value(&self.routine_vars, value)
                .map(Ok)
                .unwrap_or_else(|| literal_to_value(value)),
            Expr::TypedString(value) => typed_string_to_value_with_db(self.db_ref(), value),
            Expr::CompoundIdentifier(idents) => {
                let parts = idents
                    .iter()
                    .map(|ident| ident.value.clone())
                    .collect::<Vec<_>>();
                if let Some(value) = context.row_lookup.qualified_value_from_parts(row, &parts) {
                    return Ok(value);
                }
                if let Some(outer_row) = &self.outer_row {
                    if let Some(value) = outer_row.value_from_parts(&parts) {
                        return Ok(value);
                    }
                }
                if let Some(value) = routine_var_from_parts(&self.routine_vars, &parts)? {
                    return Ok(value);
                }
                Ok(SqlValue::Null)
            }
            Expr::BinaryOp { left, op, right }
                if matches!(op, BinaryOperator::Arrow | BinaryOperator::LongArrow) =>
            {
                let base = self.eval_slot_row_value(row, context, left)?;
                let path = self.eval_slot_row_value(row, context, right)?;
                eval_json_operator_value(base, op, path)
            }
            Expr::BinaryOp {
                op: BinaryOperator::And | BinaryOperator::Or,
                ..
            } => Ok(self
                .eval_slot_row_truth(row, context, expr)?
                .map(SqlValue::Bool)
                .unwrap_or(SqlValue::Null)),
            Expr::BinaryOp { left, op, right } => {
                let left_value = self.eval_slot_row_value(row, context, left)?;
                let right_value = self.eval_slot_row_value(row, context, right)?;
                // NOT memoized by node address: this arm also evaluates
                // predicates derived per execution (pushed/residual join
                // terms), whose nodes are fresh allocations each time — an
                // address-keyed memo can hand a later, different expression
                // at the same address the wrong operand type.
                // Memoized per IR node only when the caller vouches the
                // expression is IR-owned (never for per-execution derived
                // predicates); otherwise inferred directly.
                let left_type = self.scoped_slot_row_expr_type(context, left);
                let right_type = self.scoped_slot_row_expr_type(context, right);
                if let Some(value) = eval_range_binary_value_with_db(
                    self.db_ref(),
                    left_value.clone(),
                    op,
                    right_value.clone(),
                    left_type.as_deref(),
                    right_type.as_deref(),
                )? {
                    Ok(value)
                } else {
                    eval_binary_expr_value(left, op, right, left_value, right_value, None)
                }
            }
            Expr::JsonAccess { value, path } => {
                let base = self.eval_slot_row_value(row, context, value)?;
                Ok(json_extract_path_value(
                    &base,
                    &json_access_path(path)?,
                    false,
                ))
            }
            Expr::Cast {
                expr, data_type, ..
            } => {
                if let Some(source) = regclass_display_cast_source(expr, data_type) {
                    let value = self.eval_slot_row_value(row, context, source)?;
                    return regclass_text_value(self.db_ref(), value);
                }
                if let Some(source) = regtype_text_cast_source(expr, data_type)? {
                    let value = self.eval_slot_row_value(row, context, source)?;
                    return regtype_text_value(self.db_ref(), value);
                }
                if let Some(source) = regclass_text_cast_source(expr, data_type)? {
                    let value = self.eval_slot_row_value(row, context, source)?;
                    return regclass_text_value(self.db_ref(), value);
                }
                let value = self.eval_slot_row_value(row, context, expr)?;
                if pg_type_from_data_type(data_type).is_ok_and(|(pg_type, _)| {
                    matches!(pg_type.as_str(), "text" | "varchar" | "bpchar" | "name")
                }) {
                    let source_type = self
                        .infer_slot_row_expr_type(context.row_lookup.columns(), expr)
                        .or_else(|| projected_expr_pg_type_with_db(self.db_ref(), expr));
                    if source_type.as_deref() == Some("bpchar") {
                        return Ok(match value {
                            SqlValue::String(value) => {
                                SqlValue::String(value.trim_end_matches(' ').to_string())
                            }
                            value => value,
                        });
                    }
                    if source_type.as_deref().is_some_and(is_oid_alias_type) {
                        return render_oid_alias_value(
                            self.db_ref(),
                            source_type.as_deref().expect("alias source type checked"),
                            &value,
                        )
                        .map(SqlValue::String);
                    }
                    if let Some(element_type) = source_type
                        .as_deref()
                        .and_then(|pg_type| pg_type.strip_suffix("[]"))
                    {
                        return postgres_array_text_value(
                            &value,
                            pg_type_delimiter_with_db(self.db_ref(), element_type)?,
                        )
                        .map(SqlValue::String);
                    }
                }
                cast_expr_value_with_db(self.db_ref(), value, expr, data_type, None)
            }
            Expr::Position { expr, r#in } => eval_position_typed_value(
                self.eval_slot_row_value(row, context, expr)?,
                self.eval_slot_row_value(row, context, r#in)?,
                self.infer_slot_row_expr_type(context.row_lookup.columns(), expr)
                    .as_deref()
                    == Some("bytea")
                    || self
                        .infer_slot_row_expr_type(context.row_lookup.columns(), r#in)
                        .as_deref()
                        == Some("bytea"),
            ),
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => eval_substring_expr(
                expr,
                substring_from.as_deref(),
                substring_for.as_deref(),
                |expr| self.eval_slot_row_value(row, context, expr),
                self.scoped_slot_row_expr_type(context, expr).as_deref() == Some("bytea"),
            ),
            Expr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => eval_overlay_expr(
                expr,
                overlay_what,
                overlay_from,
                overlay_for.as_deref(),
                |expr| self.eval_slot_row_value(row, context, expr),
                self.infer_slot_row_expr_type(context.row_lookup.columns(), expr)
                    .as_deref()
                    == Some("bytea"),
            ),
            Expr::Extract { field, expr, .. } => {
                eval_extract_value(field, self.eval_slot_row_value(row, context, expr)?)
            }
            Expr::Function(function)
                if function.over.is_none() && is_aggregate_function(function) =>
            {
                if let Some(value) = context
                    .row_lookup
                    .value_for_key(row, &group_aggregate_column_name(function))
                {
                    return Ok(value.clone());
                }
                self.eval_slot_row_function_value(row, context, function)
            }
            Expr::Function(function) if function.over.is_some() => context
                .row_lookup
                .value_for_key(row, &window_column_name(function))
                .cloned()
                .ok_or_else(|| {
                    SqlError::InvalidSql(format!(
                        "window function {} was not evaluated",
                        function.name
                    ))
                }),
            Expr::Function(function) => self.eval_slot_row_function_value(row, context, function),
            Expr::Trim {
                trim_where,
                trim_what,
                expr,
                trim_characters,
            } => eval_trim_expr(
                trim_where.as_ref(),
                trim_what.as_deref(),
                expr,
                trim_characters.as_deref(),
                |expr| self.eval_slot_row_value(row, context, expr),
            ),
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => self.eval_slot_row_case(
                row,
                context,
                operand.as_deref(),
                conditions,
                else_result.as_deref(),
            ),
            Expr::Like { .. }
            | Expr::ILike { .. }
            | Expr::InList { .. }
            | Expr::InSubquery { .. }
            | Expr::Between { .. }
            | Expr::AnyOp { .. }
            | Expr::AllOp { .. }
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::IsDistinctFrom(_, _)
            | Expr::IsNotDistinctFrom(_, _)
            | Expr::IsTrue(_)
            | Expr::IsNotTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsNotFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotUnknown(_)
            | Expr::Exists { .. } => self
                .eval_slot_row_truth(row, context, expr)
                .map(|value| value.map(SqlValue::Bool).unwrap_or(SqlValue::Null)),
            Expr::Array(array) => sql_array_value({
                let types = array
                    .elem
                    .iter()
                    .map(|expr| self.infer_slot_row_expr_type(context.row_lookup.columns(), expr))
                    .collect::<Vec<_>>();
                select_common_pg_type(self.db_ref(), &types, "ARRAY")?;
                array
                    .elem
                    .iter()
                    .map(|expr| self.eval_slot_row_value(row, context, expr))
                    .collect::<Result<Vec<_>>>()?
            }),
            Expr::Tuple(exprs) => Ok(anonymous_record_value(
                exprs
                    .iter()
                    .map(|expr| self.eval_slot_row_value(row, context, expr))
                    .collect::<Result<Vec<_>>>()?,
                exprs
                    .iter()
                    .map(|expr| projected_expr_pg_type_with_db(self.db_ref(), expr))
                    .collect(),
            )),
            Expr::CompoundFieldAccess { root, access_chain } => eval_access_chain_expr(
                root,
                access_chain,
                self.row_expr_pg_type(root, context).as_deref() == Some("jsonb"),
                |expr| self.eval_slot_row_value(row, context, expr),
            ),
            Expr::Interval(interval) => interval_literal_value(interval, |expr| {
                self.eval_slot_row_value(row, context, expr)
            }),
            Expr::AtTimeZone {
                timestamp,
                time_zone,
            } => eval_at_time_zone_value(
                self.eval_slot_row_value(row, context, timestamp)?,
                self.eval_slot_row_value(row, context, time_zone)?,
                projected_expr_pg_type_with_db(self.db_ref(), timestamp).as_deref(),
            ),
            Expr::Subquery(query) => {
                self.execute_correlated_slot_scalar_subquery(row, context, query)
            }
            Expr::Nested(expr) => self.eval_slot_row_value(row, context, expr),
            Expr::Collate { expr, .. } => self.eval_slot_row_value(row, context, expr),
            Expr::UnaryOp { op, expr } if op.to_string().eq_ignore_ascii_case("NOT") => self
                .eval_slot_row_truth(row, context, expr)
                .map(sql_not)
                .map(|value| value.map(SqlValue::Bool).unwrap_or(SqlValue::Null)),
            Expr::UnaryOp { op, expr } if op.to_string() == "-" => eval_unary_minus_expr_value(
                expr,
                self.eval_slot_row_value(row, context, expr)?,
                None,
            ),
            Expr::UnaryOp { op, expr } if op.to_string() == "+" => eval_unary_plus_expr_value(
                expr,
                self.eval_slot_row_value(row, context, expr)?,
                None,
            ),
            Expr::UnaryOp { op, expr }
                if matches!(
                    op,
                    UnaryOperator::BitwiseNot | UnaryOperator::PGPrefixFactorial
                ) || is_geometric_unary_operator(op) =>
            {
                eval_unary_bit_not_expr_value(
                    op,
                    expr,
                    self.eval_slot_row_value(row, context, expr)?,
                    None,
                )
            }
            other => Err(SqlError::Unsupported(format!(
                "unsupported value expression {other}"
            ))),
        }
    }

    pub(crate) fn eval_slot_row_case(
        &self,
        row: &SlotRow,
        context: &BoundRowContext,
        operand: Option<&Expr>,
        conditions: &[sqlparser::ast::CaseWhen],
        else_result: Option<&Expr>,
    ) -> Result<SqlValue> {
        // The branch-type check only produces an error; for IR-owned CASE
        // nodes it is done once per catalog generation.
        let validated_key = if self.memo_operand_types {
            self.routine_ir.map(|ir| {
                let generation = crate::eval::expr_type_scope()
                    .map(|(generation, _)| generation)
                    .unwrap_or_else(|| self.db_ref().collection_generation(ROUTINE_COLLECTION));
                (generation, ir, conditions.as_ptr() as usize)
            })
        } else {
            None
        };
        let already_validated = validated_key
            .is_some_and(|key| CASE_VALIDATED_MEMO.with(|memo| memo.borrow().contains(&key)));
        if !already_validated {
            let mut branch_types = Vec::with_capacity(conditions.len() + 1);
            branch_types.push(else_result.and_then(|result| {
                self.infer_slot_row_expr_type(context.row_lookup.columns(), result)
            }));
            branch_types.extend(conditions.iter().map(|condition| {
                self.infer_slot_row_expr_type(context.row_lookup.columns(), &condition.result)
            }));
            select_common_pg_type(self.db_ref(), &branch_types, "CASE")?;
            if let Some(key) = validated_key {
                CASE_VALIDATED_MEMO.with(|memo| {
                    let mut memo = memo.borrow_mut();
                    if memo.len() >= ROW_EXPR_TYPE_MEMO_MAX {
                        memo.clear();
                    }
                    memo.insert(key);
                });
            }
        }
        let operand_value = operand
            .map(|expr| self.eval_slot_row_value(row, context, expr))
            .transpose()?;
        for condition in conditions {
            let matched = if let Some(operand_value) = &operand_value {
                values_equal(
                    operand_value,
                    &self.eval_slot_row_value(row, context, &condition.condition)?,
                )
            } else {
                self.eval_slot_row_truth(row, context, &condition.condition)?
                    .unwrap_or(false)
            };
            if matched {
                return self.eval_slot_row_value(row, context, &condition.result);
            }
        }
        else_result
            .map(|expr| self.eval_slot_row_value(row, context, expr))
            .unwrap_or(Ok(SqlValue::Null))
    }

    pub(crate) fn eval_slot_row_function_value(
        &self,
        row: &SlotRow,
        context: &BoundRowContext,
        function: &Function,
    ) -> Result<SqlValue> {
        if function.over.is_some() {
            return context
                .row_lookup
                .value_for_key(row, &window_column_name(function))
                .cloned()
                .ok_or_else(|| {
                    SqlError::InvalidSql(format!(
                        "window function {} was not evaluated",
                        function.name
                    ))
                });
        }
        let name = object_name(&function.name)?.to_ascii_lowercase();
        if let Some(parts) = qualified_session_keyword_column_parts(function, &name) {
            if let Some(value) = context.row_lookup.qualified_value_from_parts(row, &parts) {
                return Ok(value);
            }
            if let Some(outer_row) = &self.outer_row {
                if let Some(value) = outer_row.value_from_parts(&parts) {
                    return Ok(value);
                }
            }
            return Ok(SqlValue::Null);
        }
        let bare_name = name.strip_prefix("pg_catalog.").unwrap_or(&name);
        if matches!(name.as_str(), "coalesce" | "pg_catalog.coalesce") {
            let arg_types = function_args(function)
                .iter()
                .map(|arg| self.infer_slot_row_expr_type(context.row_lookup.columns(), arg))
                .collect::<Vec<_>>();
            select_common_pg_type(self.db_ref(), &arg_types, "COALESCE")?;
            for arg in function_args(function) {
                let value = self.eval_slot_row_value(row, context, &arg)?;
                if !matches!(value, SqlValue::Null) {
                    return Ok(value);
                }
            }
            return Ok(SqlValue::Null);
        }
        let arg_exprs = function_args(function);
        if let Some((alias, binary)) = whole_row_json_function_alias(&name, &arg_exprs) {
            let pretty = if arg_exprs.len() == 2 {
                match self.eval_slot_row_value(row, context, &arg_exprs[1])? {
                    SqlValue::Null => return Ok(SqlValue::Null),
                    SqlValue::Bool(pretty) => pretty,
                    _ => {
                        return Err(SqlError::Unsupported(
                            "row_to_json pretty argument must be boolean".to_string(),
                        ));
                    }
                }
            } else {
                false
            };
            if let Some(value) =
                whole_slot_row_json_value(alias, context.row_lookup.columns(), row, binary, pretty)?
            {
                return Ok(value);
            }
        }
        if matches!(
            name.as_str(),
            "pg_get_indexdef" | "pg_catalog.pg_get_indexdef"
        ) {
            if let Some(arg) = arg_exprs.first() {
                let indexrelid = self.eval_slot_row_value(row, context, arg)?;
                if let Some(definition) =
                    pg_indexdef_from_slot_row(&context.row_lookup, row, arg, &indexrelid)
                {
                    return Ok(SqlValue::String(definition));
                }
            }
        }
        let args = arg_exprs
            .iter()
            .map(|arg| self.eval_slot_row_value(row, context, arg))
            .collect::<Result<Vec<_>>>()?;
        let arg_types = arg_exprs
            .iter()
            .map(|arg| self.infer_slot_row_expr_type(context.row_lookup.columns(), arg))
            .collect::<Vec<_>>();
        if matches!(bare_name, "nullif" | "greatest" | "least" | "ifnull") {
            select_common_pg_type(self.db_ref(), &arg_types, &bare_name.to_ascii_uppercase())?;
        }
        if is_row_constructor(function) {
            return Ok(anonymous_record_value(args, arg_types));
        }
        if matches!(name.as_str(), "pg_typeof" | "pg_catalog.pg_typeof") {
            return Ok(pg_typeof_result(
                arg_types.first().and_then(Option::as_ref),
                args.first(),
            ));
        }
        if let Some(value) = eval_network_function_value(&name, &args, &arg_types)?
            .or(eval_user_range_function_value(
                self.db_ref(),
                &name,
                &args,
                &arg_types,
            )?)
            .or(eval_range_function_value(&name, &args, &arg_types)?)
        {
            return Ok(value);
        }
        if matches!(
            name.as_str(),
            "pg_get_function_arguments"
                | "pg_catalog.pg_get_function_arguments"
                | "pg_get_function_identity_arguments"
                | "pg_catalog.pg_get_function_identity_arguments"
                | "pg_get_function_result"
                | "pg_catalog.pg_get_function_result"
                | "pg_get_function_sqlbody"
                | "pg_catalog.pg_get_function_sqlbody"
        ) {
            let row_map = slot_row_to_sql_row(context.row_lookup.columns(), row);
            if let Some(value) =
                eval_pg_proc_row_function_value(self.db_ref(), &row_map, &name, &args)?
            {
                return Ok(value);
            }
        }
        if let Some(value) = self.eval_runtime_function_value(&name, &args)? {
            return Ok(value);
        }
        if let Some(value) = eval_session_function_value(&name, &args, &self.session_gucs)? {
            return Ok(value);
        }
        if let Some(value) =
            eval_privilege_function_value(self.db_ref(), &name, &args, &self.session_gucs)?
        {
            return Ok(value);
        }
        if let Some(value) = eval_db_catalog_function_value(
            self.db_ref(),
            &name,
            &args,
            self.tx.map(Transaction::visibility_watermark),
            Some(&self.session_gucs),
        )? {
            return Ok(value);
        }
        if let Some(value) = eval_broker_function_value(
            self.db_ref(),
            &name,
            &args,
            BrokerCaller::Sql {
                context: self.security_context.as_ref(),
                superuser: session_role_is_superuser(self.db_ref(), &self.session_gucs),
            },
            self.tx,
        )? {
            return Ok(value);
        }
        if let Some(value) = eval_json_function_call_value(function, &args)? {
            return Ok(value);
        }
        if let Some(value) = crate::eval_xml_function_value(&name, &args, Some(&arg_types))? {
            return Ok(value);
        }
        if let Some(value) = eval_catalog_function_value(&name, &args) {
            return Ok(value);
        }
        if let Some(value) = eval_fts_function_value(&name, &args, Some(&arg_types))? {
            return Ok(value);
        }
        if let Some(value) = eval_compatibility_function_value_with_db(
            self.db_ref(),
            &name,
            &args,
            Some(&arg_types),
        )? {
            return Ok(value);
        }
        if let Some(value) = self.eval_routing_function_value_authorized(&name, &args)? {
            return Ok(value);
        }
        if let Some(value) = eval_spatial_function_value(&name, &args, &arg_types)? {
            return Ok(value);
        }
        if let Some(value) = self.eval_stored_sql_function_value(&name, &args)? {
            return Ok(value);
        }
        execute_builtin_function(
            function,
            Some(self.db_ref()),
            self.security_context.as_ref(),
            self.tx,
            Some(&self.session_gucs),
        )
        .ok()
        .and_then(first_result_value)
        .ok_or_else(|| {
            SqlError::Unsupported(format!(
                "function {} is not supported without FROM",
                function.name
            ))
        })
    }

    pub(crate) fn execute_correlated_slot_scalar_subquery(
        &self,
        row: &SlotRow,
        context: &BoundRowContext,
        query: &Query,
    ) -> Result<SqlValue> {
        sql_profile_scalar_subquery();
        let started = Instant::now();
        let result = self.execute_correlated_slot_query(row, context, query);
        sql_profile_scalar_subquery_elapsed(started);
        let result = result?;
        scalar_subquery_value(result)
    }

    pub(crate) fn execute_correlated_slot_query(
        &self,
        row: &SlotRow,
        context: &BoundRowContext,
        query: &Query,
    ) -> Result<SqlResult> {
        let outer_row =
            extend_outer_slot_context(self.outer_row.as_ref(), context.row_lookup.columns(), row);
        self.inherit_transaction(SqlEngine::with_ctes_and_context(
            self.db_ref(),
            self.settings,
            self.ctes.clone(),
            self.security_context.clone(),
            self.session_gucs.clone(),
        ))
        .with_shared_routine_vars(self.routine_vars.clone())
        .with_outer_slot_row(outer_row)
        .with_cancellation(self.cancellation.clone())
        .execute_query(query)
    }

    pub(crate) fn execute_correlated_slot_subquery_values(
        &self,
        row: &SlotRow,
        context: &BoundRowContext,
        query: &Query,
    ) -> Result<Vec<SqlValue>> {
        let result = self.execute_correlated_slot_query(row, context, query)?;
        subquery_values(result)
    }

    pub(crate) fn eval_slot_row_tuple_in_subquery_truth(
        &self,
        row: &SlotRow,
        context: &BoundRowContext,
        expr: &Expr,
        subquery: &Query,
        negated: bool,
    ) -> Result<Option<Option<bool>>> {
        let mut eval = |expr: &Expr| self.eval_slot_row_value(row, context, expr);
        let Some(value) = eval_tuple_values(expr, &mut eval)? else {
            return Ok(None);
        };
        let result = self.execute_correlated_slot_query(row, context, subquery)?;
        if result.columns.len() != value.len() {
            return Err(row_value_length_error());
        }
        Ok(Some(eval_tuple_in_rows_truth(
            &value,
            result.rows,
            negated,
        )?))
    }

    pub(crate) fn eval_record_outer_predicate(
        &self,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        record: &Record,
        outer_row: &OuterSlotRow,
        expr: &Expr,
    ) -> Result<Option<bool>> {
        match expr {
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::And => {
                    let left = self.eval_record_outer_predicate(
                        table, alias, schema, record, outer_row, left,
                    )?;
                    if matches!(left, Some(false)) {
                        return Ok(Some(false));
                    }
                    let Some(left) = left else {
                        return Ok(None);
                    };
                    Ok(sql_and(
                        Some(left),
                        self.eval_record_outer_predicate(
                            table, alias, schema, record, outer_row, right,
                        )?,
                    ))
                }
                BinaryOperator::Or => {
                    let left = self.eval_record_outer_predicate(
                        table, alias, schema, record, outer_row, left,
                    )?;
                    if matches!(left, Some(true)) {
                        return Ok(Some(true));
                    }
                    let Some(left) = left else {
                        return Ok(None);
                    };
                    Ok(sql_or(
                        Some(left),
                        self.eval_record_outer_predicate(
                            table, alias, schema, record, outer_row, right,
                        )?,
                    ))
                }
                BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq => {
                    let left_type = projected_expr_pg_type(left, schema);
                    let right_type = projected_expr_pg_type(right, schema);
                    reject_undefined_comparison(left_type.as_deref(), op, right_type.as_deref())?;
                    let Some(left) = self
                        .eval_record_outer_value(table, alias, schema, record, outer_row, left)?
                    else {
                        return Ok(None);
                    };
                    let Some(right) = self
                        .eval_record_outer_value(table, alias, schema, record, outer_row, right)?
                    else {
                        return Ok(None);
                    };
                    if left_type.as_deref() == Some("pg_lsn") && left_type == right_type {
                        return Ok(Some(comparison_from_ordering(
                            op,
                            pg_typed_compare("pg_lsn", &left, &right)?,
                        )));
                    }
                    compare_values(&left, op, &right)
                }
                _ => {
                    let Some(value) = self
                        .eval_record_outer_value(table, alias, schema, record, outer_row, expr)?
                    else {
                        return Ok(None);
                    };
                    sql_value_truth(value)
                }
            },
            Expr::IsNull(expr) => self
                .eval_record_outer_value(table, alias, schema, record, outer_row, expr)
                .map(|value| value.map(|value| value_is_null_predicate(&value))),
            Expr::IsNotNull(expr) => self
                .eval_record_outer_value(table, alias, schema, record, outer_row, expr)
                .map(|value| value.map(|value| value_is_not_null_predicate(&value))),
            Expr::Nested(expr) | Expr::Collate { expr, .. } => {
                self.eval_record_outer_predicate(table, alias, schema, record, outer_row, expr)
            }
            Expr::UnaryOp { op, expr } if op.to_string().eq_ignore_ascii_case("NOT") => self
                .eval_record_outer_predicate(table, alias, schema, record, outer_row, expr)
                .map(sql_not),
            _ => {
                let Some(value) =
                    self.eval_record_outer_value(table, alias, schema, record, outer_row, expr)?
                else {
                    return Ok(None);
                };
                sql_value_truth(value)
            }
        }
    }

    pub(crate) fn eval_record_outer_value(
        &self,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        record: &Record,
        outer_row: &OuterSlotRow,
        expr: &Expr,
    ) -> Result<Option<SqlValue>> {
        match expr {
            Expr::Identifier(ident) => {
                let parts = [ident.value.clone()];
                if let Some(value) =
                    self.record_outer_target_value(table, alias, schema, record, &parts)
                {
                    return Ok(Some(value));
                }
                if let Some(value) = outer_row.value_from_parts(&parts) {
                    return Ok(Some(value));
                }
                Ok(routine_var_from_ident(&self.routine_vars, ident).or(Some(SqlValue::Null)))
            }
            Expr::CompoundIdentifier(idents) => {
                let parts = idents
                    .iter()
                    .map(|ident| ident.value.clone())
                    .collect::<Vec<_>>();
                if let Some(value) =
                    self.record_outer_target_value(table, alias, schema, record, &parts)
                {
                    return Ok(Some(value));
                }
                if let Some(value) = outer_row.value_from_parts(&parts) {
                    return Ok(Some(value));
                }
                routine_var_from_parts(&self.routine_vars, &parts)
            }
            Expr::Value(value) => routine_var_from_value(&self.routine_vars, value)
                .map(Some)
                .map(Ok)
                .unwrap_or_else(|| literal_to_value(value).map(Some)),
            Expr::TypedString(value) => {
                typed_string_to_value_with_db(self.db_ref(), value).map(Some)
            }
            Expr::BinaryOp {
                op: BinaryOperator::And | BinaryOperator::Or,
                ..
            } => self
                .eval_record_outer_predicate(table, alias, schema, record, outer_row, expr)
                .map(|value| value.map(SqlValue::Bool).unwrap_or(SqlValue::Null))
                .map(Some),
            Expr::BinaryOp {
                left: left_expr,
                op,
                right: right_expr,
            } => {
                let Some(left) = self
                    .eval_record_outer_value(table, alias, schema, record, outer_row, left_expr)?
                else {
                    return Ok(None);
                };
                let Some(right) = self
                    .eval_record_outer_value(table, alias, schema, record, outer_row, right_expr)?
                else {
                    return Ok(None);
                };
                eval_binary_expr_value(left_expr, op, right_expr, left, right, schema).map(Some)
            }
            Expr::Cast {
                expr, data_type, ..
            } => {
                let Some(value) =
                    self.eval_record_outer_value(table, alias, schema, record, outer_row, expr)?
                else {
                    return Ok(None);
                };
                cast_expr_value_with_db(self.db_ref(), value, expr, data_type, schema).map(Some)
            }
            Expr::Nested(expr) | Expr::Collate { expr, .. } => {
                self.eval_record_outer_value(table, alias, schema, record, outer_row, expr)
            }
            Expr::UnaryOp { op, expr } if op.to_string() == "-" => {
                let Some(value) =
                    self.eval_record_outer_value(table, alias, schema, record, outer_row, expr)?
                else {
                    return Ok(None);
                };
                eval_unary_minus_expr_value(expr, value, schema).map(Some)
            }
            Expr::UnaryOp { op, expr } if op.to_string() == "+" => {
                let Some(value) =
                    self.eval_record_outer_value(table, alias, schema, record, outer_row, expr)?
                else {
                    return Ok(None);
                };
                eval_unary_plus_expr_value(expr, value, schema).map(Some)
            }
            Expr::UnaryOp { op, expr }
                if matches!(
                    op,
                    UnaryOperator::BitwiseNot | UnaryOperator::PGPrefixFactorial
                ) || is_geometric_unary_operator(op) =>
            {
                let Some(value) =
                    self.eval_record_outer_value(table, alias, schema, record, outer_row, expr)?
                else {
                    return Ok(None);
                };
                eval_unary_bit_not_expr_value(op, expr, value, schema).map(Some)
            }
            _ => Ok(None),
        }
    }

    pub(crate) fn record_outer_target_value(
        &self,
        table: &str,
        alias: &str,
        schema: Option<&TableSchema>,
        record: &Record,
        parts: &[String],
    ) -> Option<SqlValue> {
        let schema = schema?;
        let (field, _) = target_record_field_from_parts(table, alias, Some(schema), parts)?;
        Some(record_column_value(record, schema, &field))
    }

    pub(crate) fn eval_row_truth(&self, row: &SqlRow, expr: &Expr) -> Result<Option<bool>> {
        match expr {
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::And => {
                    let left = self.eval_row_truth(row, left)?;
                    if matches!(left, Some(false)) {
                        return Ok(Some(false));
                    }
                    Ok(sql_and(left, self.eval_row_truth(row, right)?))
                }
                BinaryOperator::Or => {
                    let left = self.eval_row_truth(row, left)?;
                    if matches!(left, Some(true)) {
                        return Ok(Some(true));
                    }
                    Ok(sql_or(left, self.eval_row_truth(row, right)?))
                }
                BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq => {
                    let mut eval = |expr: &Expr| self.eval_row_value(row, expr);
                    if let Some(truth) = eval_tuple_comparison(left, op, right, &mut eval)? {
                        return Ok(truth);
                    }
                    let left_type = row_columns_expr_pg_type(
                        self.db_ref(),
                        left,
                        row.keys().map(String::as_str),
                    );
                    let right_type = row_columns_expr_pg_type(
                        self.db_ref(),
                        right,
                        row.keys().map(String::as_str),
                    );
                    reject_undefined_comparison(left_type.as_deref(), op, right_type.as_deref())?;
                    let left = enforce_integer_value_type(
                        self.eval_row_value(row, left)?,
                        left_type.as_deref(),
                    )?;
                    let right = enforce_integer_value_type(
                        self.eval_row_value(row, right)?,
                        right_type.as_deref(),
                    )?;
                    if matches!(left_type.as_deref(), Some("inet" | "cidr"))
                        && matches!(right_type.as_deref(), Some("inet" | "cidr"))
                    {
                        if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
                            return Ok(None);
                        }
                        return Ok(Some(comparison_from_ordering(
                            op,
                            pg_typed_compare("inet", &left, &right)?,
                        )));
                    }
                    if left_type == right_type
                        && matches!(left_type.as_deref(), Some("macaddr" | "macaddr8"))
                    {
                        if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
                            return Ok(None);
                        }
                        return Ok(Some(comparison_from_ordering(
                            op,
                            pg_typed_compare(left_type.as_deref().unwrap(), &left, &right)?,
                        )));
                    }
                    if left_type.as_deref() == Some("pg_lsn") && left_type == right_type {
                        if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
                            return Ok(None);
                        }
                        return Ok(Some(comparison_from_ordering(
                            op,
                            pg_typed_compare("pg_lsn", &left, &right)?,
                        )));
                    }
                    compare_values(&left, op, &right)
                }
                BinaryOperator::PGLikeMatch
                | BinaryOperator::PGILikeMatch
                | BinaryOperator::PGNotLikeMatch
                | BinaryOperator::PGNotILikeMatch
                | BinaryOperator::PGRegexMatch
                | BinaryOperator::PGRegexIMatch
                | BinaryOperator::PGRegexNotMatch
                | BinaryOperator::PGRegexNotIMatch => {
                    let left = self.eval_row_value(row, left)?;
                    let right = self.eval_row_value(row, right)?;
                    eval_pg_pattern_operator(&left, op, &right)
                }
                BinaryOperator::AtArrow | BinaryOperator::ArrowAt => {
                    let left_type = row_columns_expr_pg_type(
                        self.db_ref(),
                        left,
                        row.keys().map(String::as_str),
                    );
                    let right_type = row_columns_expr_pg_type(
                        self.db_ref(),
                        right,
                        row.keys().map(String::as_str),
                    );
                    reject_unsupported_geometric_binary(
                        op,
                        left_type.as_deref(),
                        right_type.as_deref(),
                    )?;
                    let left = self.eval_row_value(row, left)?;
                    let right = self.eval_row_value(row, right)?;
                    if let Some(value) = eval_geometric_binary_value(
                        &left,
                        op,
                        &right,
                        left_type.as_deref(),
                        right_type.as_deref(),
                    )? {
                        return sql_value_truth(value);
                    }
                    if let Some(value) = eval_range_binary_value_with_db(
                        self.db_ref(),
                        left.clone(),
                        op,
                        right.clone(),
                        left_type.as_deref(),
                        right_type.as_deref(),
                    )? {
                        return sql_value_truth(value);
                    }
                    eval_containment_truth(left, op, right)
                }
                _ if {
                    let left_type = row_columns_expr_pg_type(
                        self.db_ref(),
                        left,
                        row.keys().map(String::as_str),
                    );
                    let right_type = row_columns_expr_pg_type(
                        self.db_ref(),
                        right,
                        row.keys().map(String::as_str),
                    );
                    geometric_binary_result_pg_type(op, left_type.as_deref(), right_type.as_deref())
                        .as_deref()
                        == Some("bool")
                } =>
                {
                    let left_type = row_columns_expr_pg_type(
                        self.db_ref(),
                        left,
                        row.keys().map(String::as_str),
                    );
                    let right_type = row_columns_expr_pg_type(
                        self.db_ref(),
                        right,
                        row.keys().map(String::as_str),
                    );
                    eval_geometric_binary_value(
                        &self.eval_row_value(row, left)?,
                        op,
                        &self.eval_row_value(row, right)?,
                        left_type.as_deref(),
                        right_type.as_deref(),
                    )?
                    .ok_or_else(|| {
                        SqlError::Unsupported(format!("unsupported WHERE operator {op}"))
                    })
                    .and_then(sql_value_truth)
                }
                _ if {
                    let left_type = row_columns_expr_pg_type(
                        self.db_ref(),
                        left,
                        row.keys().map(String::as_str),
                    );
                    let right_type = row_columns_expr_pg_type(
                        self.db_ref(),
                        right,
                        row.keys().map(String::as_str),
                    );
                    network_binary_result_pg_type(op, left_type.as_deref(), right_type.as_deref())
                        .as_deref()
                        == Some("bool")
                } =>
                {
                    let left_type = row_columns_expr_pg_type(
                        self.db_ref(),
                        left,
                        row.keys().map(String::as_str),
                    );
                    let right_type = row_columns_expr_pg_type(
                        self.db_ref(),
                        right,
                        row.keys().map(String::as_str),
                    );
                    eval_network_binary_value(
                        &self.eval_row_value(row, left)?,
                        op,
                        &self.eval_row_value(row, right)?,
                        left_type.as_deref(),
                        right_type.as_deref(),
                    )?
                    .ok_or_else(|| {
                        SqlError::Unsupported(format!("unsupported WHERE operator {op}"))
                    })
                    .and_then(sql_value_truth)
                }
                _ if is_range_set_operator(op) => {
                    let left_value = self.eval_row_value(row, left)?;
                    let right_value = self.eval_row_value(row, right)?;
                    let left_type = row_columns_expr_pg_type(
                        self.db_ref(),
                        left,
                        row.keys().map(String::as_str),
                    );
                    let right_type = row_columns_expr_pg_type(
                        self.db_ref(),
                        right,
                        row.keys().map(String::as_str),
                    );
                    if let Some(value) = eval_range_binary_value_with_db(
                        self.db_ref(),
                        left_value.clone(),
                        op,
                        right_value.clone(),
                        left_type.as_deref(),
                        right_type.as_deref(),
                    )? {
                        return sql_value_truth(value);
                    }
                    if let Some(value) = eval_array_binary_value(
                        &left_value,
                        op,
                        &right_value,
                        left_type.as_deref(),
                        right_type.as_deref(),
                    )? {
                        return sql_value_truth(value);
                    }
                    Err(SqlError::Unsupported(format!(
                        "unsupported WHERE operator {op}"
                    )))
                }
                BinaryOperator::AtAt => {
                    let left_value = self.eval_row_value(row, left)?;
                    let right_value = self.eval_row_value(row, right)?;
                    let left_type = resolved_text_search_operand_type(
                        row_columns_expr_pg_type(
                            self.db_ref(),
                            left,
                            row.keys().map(String::as_str),
                        ),
                        left,
                        &left_value,
                    );
                    let right_type = resolved_text_search_operand_type(
                        row_columns_expr_pg_type(
                            self.db_ref(),
                            right,
                            row.keys().map(String::as_str),
                        ),
                        right,
                        &right_value,
                    );
                    eval_atat_match_value(
                        left_value,
                        right_value,
                        left_type.as_deref(),
                        right_type.as_deref(),
                    )
                }
                BinaryOperator::AtQuestion => eval_jsonpath_operator_value(
                    self.eval_row_value(row, left)?,
                    self.eval_row_value(row, right)?,
                    false,
                )
                .and_then(sql_value_truth),
                BinaryOperator::Question
                | BinaryOperator::QuestionAnd
                | BinaryOperator::QuestionPipe => eval_json_existence_value(
                    self.eval_row_value(row, left)?,
                    op,
                    self.eval_row_value(row, right)?,
                )
                .and_then(sql_value_truth),
                _ => Err(SqlError::Unsupported(format!(
                    "unsupported WHERE operator {op}"
                ))),
            },
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let mut eval = |expr: &Expr| self.eval_row_value(row, expr);
                if let Some(truth) = eval_tuple_in_list_truth(expr, list, *negated, &mut eval)? {
                    return Ok(truth);
                }
                let value = self.eval_row_value(row, expr)?;
                let matched = list
                    .iter()
                    .map(|candidate| self.eval_row_value(row, candidate))
                    .collect::<Result<Vec<_>>>()?
                    .iter()
                    .any(|candidate| values_equal(&value, candidate));
                if matched {
                    Ok(Some(!*negated))
                } else if matches!(value, SqlValue::Null)
                    || list
                        .iter()
                        .map(|candidate| self.eval_row_value(row, candidate))
                        .collect::<Result<Vec<_>>>()?
                        .iter()
                        .any(|candidate| matches!(candidate, SqlValue::Null))
                {
                    Ok(None)
                } else {
                    Ok(Some(*negated))
                }
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                if let Some(truth) =
                    self.eval_row_tuple_in_subquery_truth(row, expr, subquery, *negated)?
                {
                    return Ok(truth);
                }
                let value = self.eval_row_value(row, expr)?;
                let values = self.execute_correlated_subquery_values(row, subquery)?;
                let matched = values
                    .iter()
                    .any(|candidate| values_equal(&value, candidate));
                if matched {
                    Ok(Some(!*negated))
                } else if matches!(value, SqlValue::Null)
                    || values
                        .iter()
                        .any(|candidate| matches!(candidate, SqlValue::Null))
                {
                    Ok(None)
                } else {
                    Ok(Some(*negated))
                }
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let mut eval = |expr: &Expr| self.eval_row_value(row, expr);
                if let Some(truth) = eval_tuple_between_truth(expr, low, high, *negated, &mut eval)?
                {
                    return Ok(truth);
                }
                eval_between_truth(
                    self.eval_row_value(row, expr)?,
                    self.eval_row_value(row, low)?,
                    self.eval_row_value(row, high)?,
                    *negated,
                )
            }
            Expr::AnyOp {
                left,
                compare_op,
                right,
                ..
            } => eval_quantified_truth(
                self.eval_row_value(row, left)?,
                compare_op,
                self.eval_row_value(row, right)?,
                false,
            ),
            Expr::AllOp {
                left,
                compare_op,
                right,
            } => eval_quantified_truth(
                self.eval_row_value(row, left)?,
                compare_op,
                self.eval_row_value(row, right)?,
                true,
            ),
            Expr::IsNull(expr) => Ok(Some(value_is_null_predicate(
                &self.eval_row_value(row, expr)?,
            ))),
            Expr::IsNotNull(expr) => Ok(Some(value_is_not_null_predicate(
                &self.eval_row_value(row, expr)?,
            ))),
            Expr::IsDistinctFrom(left, right) => {
                let mut eval = |expr: &Expr| self.eval_row_value(row, expr);
                if let Some(not_distinct) = eval_tuple_not_distinct(left, right, &mut eval)? {
                    return Ok(Some(!not_distinct));
                }
                Ok(Some(!values_not_distinct(
                    &self.eval_row_value(row, left)?,
                    &self.eval_row_value(row, right)?,
                )))
            }
            Expr::IsNotDistinctFrom(left, right) => {
                let mut eval = |expr: &Expr| self.eval_row_value(row, expr);
                if let Some(not_distinct) = eval_tuple_not_distinct(left, right, &mut eval)? {
                    return Ok(Some(not_distinct));
                }
                Ok(Some(values_not_distinct(
                    &self.eval_row_value(row, left)?,
                    &self.eval_row_value(row, right)?,
                )))
            }
            Expr::Like {
                negated,
                any,
                expr,
                pattern,
                escape_char,
            } => {
                if *any {
                    return Err(SqlError::Unsupported(
                        "LIKE ANY is not supported".to_string(),
                    ));
                }
                eval_like_values(
                    self.eval_row_value(row, expr)?,
                    self.eval_row_value(row, pattern)?,
                    *negated,
                    false,
                    escape_char.as_ref(),
                )
            }
            Expr::ILike {
                negated,
                any,
                expr,
                pattern,
                escape_char,
            } => {
                if *any {
                    return Err(SqlError::Unsupported(
                        "ILIKE ANY is not supported".to_string(),
                    ));
                }
                eval_like_values(
                    self.eval_row_value(row, expr)?,
                    self.eval_row_value(row, pattern)?,
                    *negated,
                    true,
                    escape_char.as_ref(),
                )
            }
            Expr::SimilarTo { .. } => Err(SqlError::Unsupported(
                "SIMILAR TO is not supported".to_string(),
            )),
            Expr::IsTrue(expr) => Ok(Some(matches!(self.eval_row_truth(row, expr)?, Some(true)))),
            Expr::IsNotTrue(expr) => {
                Ok(Some(!matches!(self.eval_row_truth(row, expr)?, Some(true))))
            }
            Expr::IsFalse(expr) => Ok(Some(matches!(self.eval_row_truth(row, expr)?, Some(false)))),
            Expr::IsNotFalse(expr) => Ok(Some(!matches!(
                self.eval_row_truth(row, expr)?,
                Some(false)
            ))),
            Expr::IsUnknown(expr) => Ok(Some(self.eval_row_truth(row, expr)?.is_none())),
            Expr::IsNotUnknown(expr) => Ok(Some(self.eval_row_truth(row, expr)?.is_some())),
            Expr::Exists { subquery, negated } => {
                let exists = !self
                    .execute_correlated_query(row, subquery)?
                    .rows
                    .is_empty();
                Ok(Some(if *negated { !exists } else { exists }))
            }
            Expr::Subquery(query) => {
                sql_value_truth(self.execute_correlated_scalar_subquery(row, query)?)
            }
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Case { .. } => {
                match self.eval_row_value(row, expr)? {
                    SqlValue::Bool(value) => Ok(Some(value)),
                    SqlValue::Null => Ok(None),
                    other => Err(SqlError::Unsupported(format!(
                        "WHERE expression {expr} returned non-boolean {}",
                        other.to_cell()
                    ))),
                }
            }
            Expr::Function(function) => match self.eval_row_function_value(row, function)? {
                SqlValue::Bool(value) => Ok(Some(value)),
                SqlValue::Null => Ok(None),
                other => Err(SqlError::Unsupported(format!(
                    "WHERE function {} returned non-boolean {}",
                    function.name,
                    other.to_cell()
                ))),
            },
            Expr::UnaryOp { op, expr } if op.to_string().eq_ignore_ascii_case("NOT") => {
                Ok(sql_not(self.eval_row_truth(row, expr)?))
            }
            Expr::Value(_) | Expr::TypedString(_) => {
                sql_value_truth(self.eval_row_value(row, expr)?)
            }
            Expr::Nested(expr) => self.eval_row_truth(row, expr),
            other => Err(SqlError::Unsupported(format!(
                "unsupported WHERE expression {other}"
            ))),
        }
    }

    pub(crate) fn eval_row_value(&self, row: &SqlRow, expr: &Expr) -> Result<SqlValue> {
        match expr {
            Expr::Identifier(ident) => {
                if ident.value.eq_ignore_ascii_case("current_date") {
                    return Ok(SqlValue::String(unix_now_date_string()));
                }
                if ident.value.eq_ignore_ascii_case("current_schema") {
                    return Ok(SqlValue::String("public".to_string()));
                }
                if ident.quote_style.is_none() {
                    if let Some(value) = self.session_identity_value(&ident.value) {
                        return Ok(value);
                    }
                }
                if let Some(value) =
                    row_value_from_parts_opt(row, std::slice::from_ref(&ident.value))
                {
                    return Ok(value);
                }
                if let Some(value) = scalar_sql_row_value_for_relation_alias(row, &ident.value) {
                    return Ok(value);
                }
                if let Some(value) = composite_sql_row_value_for_relation_alias(row, &ident.value) {
                    return Ok(hydrate_relation_composite(self.db_ref(), value));
                }
                if let Some(outer_row) = &self.outer_row {
                    if let Some(value) =
                        outer_row.value_from_parts(std::slice::from_ref(&ident.value))
                    {
                        return Ok(value);
                    }
                }
                Ok(routine_var_from_ident(&self.routine_vars, ident).unwrap_or(SqlValue::Null))
            }
            Expr::Value(value) => routine_var_from_value(&self.routine_vars, value)
                .map(Ok)
                .unwrap_or_else(|| literal_to_value(value)),
            Expr::TypedString(value) => typed_string_to_value_with_db(self.db_ref(), value),
            Expr::CompoundIdentifier(idents) => {
                let parts = idents
                    .iter()
                    .map(|ident| ident.value.clone())
                    .collect::<Vec<_>>();
                if let Some(value) = row_value_from_parts_opt(row, &parts) {
                    return Ok(value);
                }
                if let Some(outer_row) = &self.outer_row {
                    if let Some(value) = outer_row.value_from_parts(&parts) {
                        return Ok(value);
                    }
                }
                if let Some(value) = routine_var_from_parts(&self.routine_vars, &parts)? {
                    return Ok(value);
                }
                Ok(SqlValue::Null)
            }
            Expr::BinaryOp { left, op, right }
                if matches!(op, BinaryOperator::Arrow | BinaryOperator::LongArrow) =>
            {
                let base = self.eval_row_value(row, left)?;
                let path = self.eval_row_value(row, right)?;
                eval_json_operator_value(base, op, path)
            }
            Expr::BinaryOp {
                op: BinaryOperator::And | BinaryOperator::Or,
                ..
            } => Ok(self
                .eval_row_truth(row, expr)?
                .map(SqlValue::Bool)
                .unwrap_or(SqlValue::Null)),
            Expr::BinaryOp { left, op, right } => {
                let left_value = self.eval_row_value(row, left)?;
                let right_value = self.eval_row_value(row, right)?;
                let left_type = self.infer_sql_row_expr_type(row, left);
                let right_type = self.infer_sql_row_expr_type(row, right);
                if let Some(value) = eval_range_binary_value_with_db(
                    self.db_ref(),
                    left_value.clone(),
                    op,
                    right_value.clone(),
                    left_type.as_deref(),
                    right_type.as_deref(),
                )? {
                    Ok(value)
                } else {
                    eval_binary_expr_value(left, op, right, left_value, right_value, None)
                }
            }
            Expr::JsonAccess { value, path } => {
                let base = self.eval_row_value(row, value)?;
                Ok(json_extract_path_value(
                    &base,
                    &json_access_path(path)?,
                    false,
                ))
            }
            Expr::Cast {
                expr, data_type, ..
            } => {
                if let Some(source) = regclass_display_cast_source(expr, data_type) {
                    let value = self.eval_row_value(row, source)?;
                    return regclass_text_value(self.db_ref(), value);
                }
                if let Some(source) = regtype_text_cast_source(expr, data_type)? {
                    let value = self.eval_row_value(row, source)?;
                    return regtype_text_value(self.db_ref(), value);
                }
                if let Some(source) = regclass_text_cast_source(expr, data_type)? {
                    let value = self.eval_row_value(row, source)?;
                    return regclass_text_value(self.db_ref(), value);
                }
                let value = self.eval_row_value(row, expr)?;
                if pg_type_from_data_type(data_type).is_ok_and(|(pg_type, _)| {
                    matches!(pg_type.as_str(), "text" | "varchar" | "bpchar" | "name")
                }) {
                    let source_type = self
                        .infer_sql_row_expr_type(row, expr)
                        .or_else(|| projected_expr_pg_type_with_db(self.db_ref(), expr));
                    if source_type.as_deref() == Some("bpchar") {
                        return Ok(match value {
                            SqlValue::String(value) => {
                                SqlValue::String(value.trim_end_matches(' ').to_string())
                            }
                            value => value,
                        });
                    }
                    if source_type.as_deref().is_some_and(is_oid_alias_type) {
                        return render_oid_alias_value(
                            self.db_ref(),
                            source_type.as_deref().expect("alias source type checked"),
                            &value,
                        )
                        .map(SqlValue::String);
                    }
                    if let Some(element_type) = source_type
                        .as_deref()
                        .and_then(|pg_type| pg_type.strip_suffix("[]"))
                    {
                        return postgres_array_text_value(
                            &value,
                            pg_type_delimiter_with_db(self.db_ref(), element_type)?,
                        )
                        .map(SqlValue::String);
                    }
                }
                cast_expr_value_with_db(self.db_ref(), value, expr, data_type, None)
            }
            Expr::Position { expr, r#in } => eval_position_typed_value(
                self.eval_row_value(row, expr)?,
                self.eval_row_value(row, r#in)?,
                projected_expr_pg_type_with_db(self.db_ref(), expr).as_deref() == Some("bytea")
                    || projected_expr_pg_type_with_db(self.db_ref(), r#in).as_deref()
                        == Some("bytea"),
            ),
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => eval_substring_expr(
                expr,
                substring_from.as_deref(),
                substring_for.as_deref(),
                |expr| self.eval_row_value(row, expr),
                projected_expr_pg_type_with_db(self.db_ref(), expr).as_deref() == Some("bytea"),
            ),
            Expr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => eval_overlay_expr(
                expr,
                overlay_what,
                overlay_from,
                overlay_for.as_deref(),
                |expr| self.eval_row_value(row, expr),
                projected_expr_pg_type_with_db(self.db_ref(), expr).as_deref() == Some("bytea"),
            ),
            Expr::Extract { field, expr, .. } => {
                eval_extract_value(field, self.eval_row_value(row, expr)?)
            }
            Expr::Function(function) => self.eval_row_function_value(row, function),
            Expr::Trim {
                trim_where,
                trim_what,
                expr,
                trim_characters,
            } => eval_trim_expr(
                trim_where.as_ref(),
                trim_what.as_deref(),
                expr,
                trim_characters.as_deref(),
                |expr| self.eval_row_value(row, expr),
            ),
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => self.eval_row_case(row, operand.as_deref(), conditions, else_result.as_deref()),
            Expr::Like { .. }
            | Expr::ILike { .. }
            | Expr::InList { .. }
            | Expr::Between { .. }
            | Expr::AnyOp { .. }
            | Expr::AllOp { .. }
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::IsTrue(_)
            | Expr::IsNotTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsNotFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotUnknown(_) => self
                .eval_row_truth(row, expr)
                .map(|value| value.map(SqlValue::Bool).unwrap_or(SqlValue::Null)),
            Expr::Array(array) => sql_array_value(
                array
                    .elem
                    .iter()
                    .map(|expr| self.eval_row_value(row, expr))
                    .collect::<Result<Vec<_>>>()?,
            ),
            Expr::Tuple(exprs) => Ok(anonymous_record_value(
                exprs
                    .iter()
                    .map(|expr| self.eval_row_value(row, expr))
                    .collect::<Result<Vec<_>>>()?,
                exprs
                    .iter()
                    .map(|expr| projected_expr_pg_type_with_db(self.db_ref(), expr))
                    .collect(),
            )),
            Expr::CompoundFieldAccess { root, access_chain } => eval_access_chain_expr(
                root,
                access_chain,
                row_columns_expr_pg_type(self.db_ref(), root, row.keys().map(String::as_str))
                    .as_deref()
                    == Some("jsonb"),
                |expr| self.eval_row_value(row, expr),
            ),
            Expr::Interval(interval) => {
                interval_literal_value(interval, |expr| self.eval_row_value(row, expr))
            }
            Expr::AtTimeZone {
                timestamp,
                time_zone,
            } => eval_at_time_zone_value(
                self.eval_row_value(row, timestamp)?,
                self.eval_row_value(row, time_zone)?,
                projected_expr_pg_type_with_db(self.db_ref(), timestamp).as_deref(),
            ),
            Expr::Subquery(query) => self.execute_correlated_scalar_subquery(row, query),
            Expr::Nested(expr) => self.eval_row_value(row, expr),
            Expr::Collate { expr, collation } => {
                normalize_column_collation(collation)?;
                self.eval_row_value(row, expr)
            }
            Expr::UnaryOp { op, expr } if op.to_string().eq_ignore_ascii_case("NOT") => self
                .eval_row_truth(row, expr)
                .map(sql_not)
                .map(|value| value.map(SqlValue::Bool).unwrap_or(SqlValue::Null)),
            Expr::UnaryOp { op, expr } if op.to_string() == "-" => {
                eval_unary_minus_expr_value(expr, self.eval_row_value(row, expr)?, None)
            }
            Expr::UnaryOp { op, expr } if op.to_string() == "+" => {
                eval_unary_plus_expr_value(expr, self.eval_row_value(row, expr)?, None)
            }
            Expr::UnaryOp { op, expr }
                if matches!(
                    op,
                    UnaryOperator::BitwiseNot | UnaryOperator::PGPrefixFactorial
                ) || is_geometric_unary_operator(op) =>
            {
                eval_unary_bit_not_expr_value(op, expr, self.eval_row_value(row, expr)?, None)
            }
            other => Err(SqlError::Unsupported(format!(
                "unsupported value expression {other}"
            ))),
        }
    }

    pub(crate) fn eval_row_case(
        &self,
        row: &SqlRow,
        operand: Option<&Expr>,
        conditions: &[sqlparser::ast::CaseWhen],
        else_result: Option<&Expr>,
    ) -> Result<SqlValue> {
        let operand_value = operand
            .map(|expr| self.eval_row_value(row, expr))
            .transpose()?;
        for condition in conditions {
            let matched = if let Some(operand_value) = &operand_value {
                values_equal(
                    operand_value,
                    &self.eval_row_value(row, &condition.condition)?,
                )
            } else {
                self.eval_row_truth(row, &condition.condition)?
                    .unwrap_or(false)
            };
            if matched {
                return self.eval_row_value(row, &condition.result);
            }
        }
        else_result
            .map(|expr| self.eval_row_value(row, expr))
            .unwrap_or(Ok(SqlValue::Null))
    }

    pub(crate) fn eval_row_function_value(
        &self,
        row: &SqlRow,
        function: &Function,
    ) -> Result<SqlValue> {
        let name = object_name(&function.name)?.to_ascii_lowercase();
        if let Some(parts) = qualified_session_keyword_column_parts(function, &name) {
            if let Some(value) = row_value_from_parts_opt(row, &parts) {
                return Ok(value);
            }
            if let Some(outer_row) = &self.outer_row {
                if let Some(value) = outer_row.value_from_parts(&parts) {
                    return Ok(value);
                }
            }
            return Ok(SqlValue::Null);
        }
        if matches!(name.as_str(), "coalesce" | "pg_catalog.coalesce") {
            for arg in function_args(function) {
                let value = self.eval_row_value(row, &arg)?;
                if !matches!(value, SqlValue::Null) {
                    return Ok(value);
                }
            }
            return Ok(SqlValue::Null);
        }
        let arg_exprs = function_args(function);
        if let Some((alias, binary)) = whole_row_json_function_alias(&name, &arg_exprs) {
            let pretty = if arg_exprs.len() == 2 {
                match self.eval_row_value(row, &arg_exprs[1])? {
                    SqlValue::Null => return Ok(SqlValue::Null),
                    SqlValue::Bool(pretty) => pretty,
                    _ => {
                        return Err(SqlError::Unsupported(
                            "row_to_json pretty argument must be boolean".to_string(),
                        ));
                    }
                }
            } else {
                false
            };
            if let Some(value) = whole_sql_row_json_value(alias, row, binary, pretty)? {
                return Ok(value);
            }
        }
        if matches!(
            name.as_str(),
            "pg_get_indexdef" | "pg_catalog.pg_get_indexdef"
        ) {
            if let Some(arg) = arg_exprs.first() {
                let indexrelid = self.eval_row_value(row, arg)?;
                if let Some(definition) = pg_indexdef_from_row(row, arg, &indexrelid) {
                    return Ok(SqlValue::String(definition));
                }
            }
        }
        let args = arg_exprs
            .iter()
            .map(|arg| self.eval_row_value(row, arg))
            .collect::<Result<Vec<_>>>()?;
        let arg_types = arg_exprs
            .iter()
            .map(|arg| self.infer_sql_row_expr_type(row, arg))
            .collect::<Vec<_>>();
        if is_row_constructor(function) {
            return Ok(anonymous_record_value(args, arg_types));
        }
        if matches!(name.as_str(), "pg_typeof" | "pg_catalog.pg_typeof") {
            return Ok(pg_typeof_result(
                arg_types.first().and_then(Option::as_ref),
                args.first(),
            ));
        }
        if let Some(value) = eval_network_function_value(&name, &args, &arg_types)?
            .or(eval_user_range_function_value(
                self.db_ref(),
                &name,
                &args,
                &arg_types,
            )?)
            .or(eval_range_function_value(&name, &args, &arg_types)?)
        {
            return Ok(value);
        }
        if let Some(value) = eval_pg_proc_row_function_value(self.db_ref(), row, &name, &args)? {
            return Ok(value);
        }
        if let Some(value) = self.eval_runtime_function_value(&name, &args)? {
            return Ok(value);
        }
        if let Some(value) = eval_session_function_value(&name, &args, &self.session_gucs)? {
            return Ok(value);
        }
        if let Some(value) =
            eval_privilege_function_value(self.db_ref(), &name, &args, &self.session_gucs)?
        {
            return Ok(value);
        }
        if let Some(value) = eval_db_catalog_function_value(
            self.db_ref(),
            &name,
            &args,
            self.tx.map(Transaction::visibility_watermark),
            Some(&self.session_gucs),
        )? {
            return Ok(value);
        }
        if let Some(value) = eval_broker_function_value(
            self.db_ref(),
            &name,
            &args,
            BrokerCaller::Sql {
                context: self.security_context.as_ref(),
                superuser: session_role_is_superuser(self.db_ref(), &self.session_gucs),
            },
            self.tx,
        )? {
            return Ok(value);
        }
        if let Some(value) = eval_json_function_call_value(function, &args)? {
            return Ok(value);
        }
        if let Some(value) = crate::eval_xml_function_value(&name, &args, Some(&arg_types))? {
            return Ok(value);
        }
        if let Some(value) = eval_catalog_function_value(&name, &args) {
            return Ok(value);
        }
        if let Some(value) = eval_fts_function_value(&name, &args, Some(&arg_types))? {
            return Ok(value);
        }
        if let Some(value) = eval_compatibility_function_value_with_db(
            self.db_ref(),
            &name,
            &args,
            Some(&arg_types),
        )? {
            return Ok(value);
        }
        if let Some(value) = self.eval_routing_function_value_authorized(&name, &args)? {
            return Ok(value);
        }
        if let Some(value) = eval_spatial_function_value(&name, &args, &arg_types)? {
            return Ok(value);
        }
        if let Some(value) = self.eval_stored_sql_function_value(&name, &args)? {
            return Ok(value);
        }
        execute_builtin_function(
            function,
            Some(self.db_ref()),
            self.security_context.as_ref(),
            self.tx,
            Some(&self.session_gucs),
        )
        .ok()
        .and_then(first_result_value)
        .ok_or_else(|| {
            SqlError::Unsupported(format!(
                "function {} is not supported without FROM",
                function.name
            ))
        })
    }

    pub(crate) fn eval_stored_sql_function_value(
        &self,
        name: &str,
        args: &[SqlValue],
    ) -> Result<Option<SqlValue>> {
        let Some(routine) = resolve_routine_cached(self.db_ref(), RoutineKind::Function, name)?
        else {
            return Ok(None);
        };
        if !routine.schema.language.eq_ignore_ascii_case("sql") {
            return Ok(None);
        }
        let role = current_user_from_gucs(&self.session_gucs);
        if !role_can_execute_routine(self.db_ref(), &role, name)? {
            return Err(SqlError::BicDb(BicDbError::Authorization(format!(
                "permission denied for function {}",
                normalize_object_name(name)
            ))));
        }
        if routine.schema.returns_set {
            return Err(SqlError::UnsupportedPlpgsqlFeature(format!(
                "set-returning stored routine {} is not implemented",
                routine.schema.name
            )));
        }
        let mut frame =
            RoutineFrame::new_with_symbols(&routine.ir.params, args, &routine.ir.symbol_names)?;
        let mut session_gucs = (*self.session_gucs).clone();
        if routine.schema.security_definer {
            // No recorded owner -> invoker semantics. Leaving the caller's
            // role in place can never grant more than the caller already has.
            if let Some(owner) = routine.schema.definer_owner() {
                session_gucs.insert(CURRENT_ROLE_GUC.to_string(), owner.to_string());
            }
        }
        let routine_engine = self.inherit_transaction(
            SqlEngine::new_with_settings_and_context(
                self.db_ref(),
                self.settings,
                self.security_context.clone(),
            )
            .with_session_gucs(Arc::new(session_gucs))
            .with_shared_routine_vars(frame.materialized_values())
            .with_cancellation(self.cancellation.clone()),
        );
        let mut result = None;
        for statement in &routine.ir.statements {
            let RoutineStmt::Sql(Statement::Query(query)) = statement else {
                return Err(SqlError::UnsupportedPlpgsqlFeature(format!(
                    "SQL function {} contains a non-query statement",
                    routine.schema.name
                )));
            };
            result = Some(routine_engine.execute_query(query)?);
        }
        let value = result
            .and_then(|result| result.rows.into_iter().next())
            .and_then(|row| row.into_iter().next())
            .unwrap_or(SqlValue::Null);
        cast_routine_return_value(value, &routine.schema).map(Some)
    }

    pub(crate) fn eval_runtime_function_value(
        &self,
        name: &str,
        args: &[SqlValue],
    ) -> Result<Option<SqlValue>> {
        let bare_name = name.strip_prefix("pg_catalog.").unwrap_or(name);
        if bare_name == "txid_current" {
            if !args.is_empty() {
                return Err(SqlError::InvalidSql(format!(
                    "txid_current expects no arguments, got {}",
                    args.len()
                )));
            }
            return Ok(self
                .tx
                .map(|transaction| SqlValue::Int(transaction.id().0 as i64)));
        }
        let Some(runtime) = &self.runtime else {
            return Ok(None);
        };
        if bare_name == "pg_backend_pid" {
            if !args.is_empty() {
                return Err(SqlError::InvalidSql(format!(
                    "pg_backend_pid expects no arguments, got {}",
                    args.len()
                )));
            }
            return Ok(Some(SqlValue::Int(i64::from(runtime.backend_pid()))));
        }
        runtime.execute_advisory_lock(bare_name, args, &self.cancellation)
    }

    pub(crate) fn infer_sql_row_expr_type(&self, row: &SqlRow, expr: &Expr) -> Option<String> {
        row_columns_expr_pg_type(self.db_ref(), expr, row.keys().map(String::as_str))
    }

    pub(crate) fn infer_slot_row_expr_type(
        &self,
        columns: &[String],
        expr: &Expr,
    ) -> Option<String> {
        row_columns_expr_pg_type(self.db_ref(), expr, columns.iter().map(String::as_str))
    }

    pub(crate) fn execute_scalar_subquery(&self, query: &Query) -> Result<SqlValue> {
        sql_profile_scalar_subquery();
        let started = Instant::now();
        let result = self.execute_query(query);
        sql_profile_scalar_subquery_elapsed(started);
        let result = result?;
        scalar_subquery_value(result)
    }

    pub(crate) fn execute_correlated_scalar_subquery(
        &self,
        row: &SqlRow,
        query: &Query,
    ) -> Result<SqlValue> {
        sql_profile_scalar_subquery();
        let started = Instant::now();
        let result = self.execute_correlated_query(row, query);
        sql_profile_scalar_subquery_elapsed(started);
        let result = result?;
        scalar_subquery_value(result)
    }

    pub(crate) fn execute_correlated_query(
        &self,
        row: &SqlRow,
        query: &Query,
    ) -> Result<SqlResult> {
        self.inherit_transaction(SqlEngine::with_ctes_and_context(
            self.db_ref(),
            self.settings,
            self.ctes.clone(),
            self.security_context.clone(),
            self.session_gucs.clone(),
        ))
        .with_shared_routine_vars(self.routine_vars.clone())
        .with_outer_slot_row(extend_outer_slot_context_from_sql_row(
            self.outer_row.as_ref(),
            row,
        ))
        .with_cancellation(self.cancellation.clone())
        .execute_query(query)
    }

    pub(crate) fn execute_correlated_subquery_values(
        &self,
        row: &SqlRow,
        query: &Query,
    ) -> Result<Vec<SqlValue>> {
        let result = self.execute_correlated_query(row, query)?;
        subquery_values(result)
    }

    pub(crate) fn eval_row_tuple_in_subquery_truth(
        &self,
        row: &SqlRow,
        expr: &Expr,
        subquery: &Query,
        negated: bool,
    ) -> Result<Option<Option<bool>>> {
        let mut eval = |expr: &Expr| self.eval_row_value(row, expr);
        let Some(value) = eval_tuple_values(expr, &mut eval)? else {
            return Ok(None);
        };
        let result = self.execute_correlated_query(row, subquery)?;
        if result.columns.len() != value.len() {
            return Err(row_value_length_error());
        }
        Ok(Some(eval_tuple_in_rows_truth(
            &value,
            result.rows,
            negated,
        )?))
    }
}
