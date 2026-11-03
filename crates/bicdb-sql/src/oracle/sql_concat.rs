//! Keep Oracle SQL concatenation shallow without reparsing operand expressions.
use sqlparser::ast::*;

fn plain<'a>(expr: &'a Expr, name: &str) -> Option<&'a FunctionArgumentList> {
    let mut expr = expr;
    while let Expr::Nested(inner) = expr {
        expr = inner.as_ref();
    }
    let Expr::Function(function) = expr else {
        return None;
    };
    let [part] = function.name.0.as_slice() else {
        return None;
    };
    if !part
        .as_ident()
        .is_some_and(|ident| ident.quote_style.is_none() && ident.value.eq_ignore_ascii_case(name))
        || function.uses_odbc_syntax
        || function.parameters != FunctionArguments::None
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return None;
    }
    let FunctionArguments::List(args) = &function.args else {
        return None;
    };
    (args.duplicate_treatment.is_none()
        && args.clauses.is_empty()
        && args
            .args
            .iter()
            .all(|arg| matches!(arg, FunctionArg::Unnamed(FunctionArgExpr::Expr(_)))))
    .then_some(args)
}
fn argument(arg: &FunctionArg) -> &Expr {
    let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg else {
        unreachable!()
    };
    expr
}
fn flattened(expr: &Expr) -> Option<&[FunctionArg]> {
    let args = plain(expr, "nullif")?;
    if args.args.len() != 2
        || !matches!(argument(&args.args[1]),Expr::Value(value) if value.value==Value::SingleQuotedString(String::new()))
    {
        return None;
    }
    let inner = plain(argument(&args.args[0]), "concat")?;
    Some(&inner.args)
}
fn call(name: &str, args: Vec<FunctionArg>) -> Expr {
    Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new(name)]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args,
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}
fn expr_arg(expr: Expr) -> FunctionArg {
    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
}
pub(super) fn lower_expression(expr: &mut Expr) {
    if !matches!(
        expr,
        Expr::BinaryOp {
            op: BinaryOperator::StringConcat,
            ..
        }
    ) {
        return;
    }
    let Expr::BinaryOp { left, right, .. } =
        std::mem::replace(expr, Expr::Value(Value::Null.into()))
    else {
        unreachable!()
    };
    let mut args = Vec::new();
    for operand in [*left, *right] {
        if let Some(flat) = flattened(&operand) {
            args.extend_from_slice(flat);
        } else {
            args.push(expr_arg(operand));
        }
    }
    *expr = call(
        "nullif",
        vec![
            expr_arg(call("concat", args)),
            expr_arg(Expr::Value(Value::SingleQuotedString(String::new()).into())),
        ],
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
    use std::ops::ControlFlow;
    fn lower(source: &str) -> Vec<Statement> {
        let mut statements = Parser::parse_sql(&PostgreSqlDialect {}, source).unwrap();
        let _ = visit_expressions_mut(&mut statements, |expr| {
            lower_expression(expr);
            ControlFlow::<()>::Continue(())
        });
        statements
    }
    #[test]
    fn long_chains_stay_shallow_and_idempotent() {
        let parts: Vec<_> = (0..32).map(|i| format!("'part{i}'")).collect();
        let sql = format!("SELECT {}", parts.join(" || "));
        let first = lower(&sql)[0].to_string();
        assert_eq!(first.matches("nullif(").count(), 1);
        assert_eq!(first.matches("concat(").count(), 1);
        assert!(first.len() < 600);
        assert_eq!(lower(&first)[0].to_string(), first);
    }
    #[test]
    fn scalar_subqueries_and_parentheses_preserve_operand_order() {
        let rewritten=lower("SELECT ('a'||(SELECT COUNT(*) FROM left_rows))||'b'||(SELECT COUNT(*) FROM right_rows)")[0].to_string();
        assert_eq!(rewritten,"SELECT nullif(concat('a', (SELECT COUNT(*) FROM left_rows), 'b', (SELECT COUNT(*) FROM right_rows)), '')");
    }
    #[test]
    fn qualified_or_modified_calls_do_not_lose_their_contract() {
        let rewritten = lower("SELECT NULLIF(app.concat('a','b'),'')||'c'")[0].to_string();
        assert!(rewritten.contains("NULLIF(app.concat('a', 'b'), '')"));
        assert_eq!(rewritten.matches("nullif(").count(), 1);
    }
}
