//! Exact unit aggregates remain inside the authorized database query. No native
//! Decimal subtotal or application list is created before the final unit string.
use super::*;

#[derive(Clone, Debug)]
pub(crate) struct ExactUnitsAggregate {
    pub(crate) value: Expr,
    pub(crate) scale: i64,
}

impl ExactUnitsAggregate {
    pub(crate) fn from_function(function: &Function) -> Result<Self> {
        let args = function_arg_list(function);
        if args.len() != 2
            || function_args_are_distinct(function)
            || function.filter.is_some()
            || function.over.is_some()
            || !function.within_group.is_empty()
            || matches!(&function.args, sqlparser::ast::FunctionArguments::List(list) if !list.clauses.is_empty())
        {
            return Err(SqlError::ExactUnitsParameter(
                "bicdb_exact_sum_units requires Decimal value and one invariant Int scale; DISTINCT, FILTER and window clauses are not supported".into(),
            ));
        }
        // Bound statement parameters have already become typed SQL constants.
        // A per-row scale is intentionally refused, even for an empty relation.
        let SqlValue::Int(scale) = eval_constant_expr(args.get(1).expect("arity checked"))? else {
            return Err(SqlError::ExactUnitsParameter(
                "bicdb_exact_sum_units unit scale requires an exact Int".into(),
            ));
        };
        new_exact_units_sum(scale)?;
        Ok(Self {
            value: args.get(0).expect("arity checked").clone(),
            scale,
        })
    }

    pub(crate) fn validate_type(
        &self,
        mut lookup: impl FnMut(&Expr) -> Option<String>,
    ) -> Result<()> {
        if !matches!(lookup(&self.value).as_deref(), Some("numeric" | "decimal")) {
            return Err(SqlError::ExactUnitsParameter(
                "bicdb_exact_sum_units requires an explicitly typed Decimal query value".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn evaluate<T>(
        &self,
        rows: &[T],
        mut eval: impl FnMut(&T, &Expr) -> Result<SqlValue>,
    ) -> Result<SqlValue> {
        let mut sum = new_exact_units_sum(self.scale)?;
        for row in rows {
            add_exact_units_value(&mut sum, eval(row, &self.value)?)?;
        }
        finish_exact_units_sum(sum)
    }
}

pub(crate) fn new_exact_units_sum(scale: i64) -> Result<bicdb_core::ExactDecimalUnitsSum> {
    bicdb_core::ExactDecimalUnitsSum::new(scale)
        .map_err(|error| SqlError::ExactUnitsParameter(error.to_string()))
}

pub(crate) fn add_exact_units_value(
    sum: &mut bicdb_core::ExactDecimalUnitsSum,
    value: SqlValue,
) -> Result<()> {
    let value = match value {
        SqlValue::String(value) => JsonValue::String(value),
        SqlValue::Int(value) => JsonValue::from(value),
        _ => {
            return Err(SqlError::ExactUnitsParameter(
                "bicdb_exact_sum_units requires nonnull exact Decimal operands; Float, Bool and container coercions are not admitted".to_string(),
            ));
        }
    };
    sum.add(&value)
        .map_err(|error| SqlError::ExactUnitsParameter(error.to_string()))
}

pub(crate) fn finish_exact_units_sum(sum: bicdb_core::ExactDecimalUnitsSum) -> Result<SqlValue> {
    sum.finish()
        .map(SqlValue::String)
        .map_err(|error| SqlError::ExactUnitsParameter(error.to_string()))
}
