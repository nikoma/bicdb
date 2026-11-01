//! PostgreSQL catalog vectors participate in ANY/ALL as their element arrays.
use super::*;
use sqlparser::ast::{VisitMut, VisitorMut};
use std::ops::ControlFlow;

impl SqlEngine<'_> {
    pub(crate) fn rewrite_quantified_vectors(
        &self,
        query: &Query,
        select: &Select,
    ) -> Result<Option<Query>> {
        let contains_quantifier = sqlparser::ast::visit_expressions(query, |expression| {
            if matches!(expression, Expr::AnyOp { .. } | Expr::AllOp { .. }) {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        });
        if !contains_quantifier.is_break() {
            return Ok(None);
        }
        let mut rewritten = query.clone();
        let mut rewriter = VectorOperands {
            engine: self,
            env: self.from_relation_columns(&select.from).unwrap_or_default(),
            depth: 0,
            changed: false,
        };
        if let ControlFlow::Break(error) = rewritten.visit(&mut rewriter) {
            return Err(error);
        }
        Ok(rewriter.changed.then_some(rewritten))
    }
}

struct VectorOperands<'a, 'db> {
    engine: &'a SqlEngine<'db>,
    env: Vec<RelationColumns>,
    depth: usize,
    changed: bool,
}

impl VisitorMut for VectorOperands<'_, '_> {
    type Break = SqlError;

    fn pre_visit_query(&mut self, _query: &mut Query) -> ControlFlow<Self::Break> {
        self.depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &mut Query) -> ControlFlow<Self::Break> {
        self.depth -= 1;
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expression: &mut Expr) -> ControlFlow<Self::Break> {
        // Nested SELECTs have their own aliases and are normalized when executed.
        if self.depth != 1 {
            return ControlFlow::Continue(());
        }
        let right = match expression {
            Expr::AnyOp { right, .. } | Expr::AllOp { right, .. } => right,
            _ => return ControlFlow::Continue(()),
        };
        let element_type = match self.engine.infer_env_expr_type(right, &self.env).as_deref() {
            Some("int2vector") => "smallint",
            Some("oidvector") => "oid",
            _ => return ControlFlow::Continue(()),
        };
        // Convert the vector's canonical text to its typed element array. This
        // coercion is based on the resolved SQL type, never a catalog column,
        // alias or client name. Ordinary text remains an invalid ANY operand.
        let source = format!("string_to_array(({right})::text, ' ')::{element_type}[]");
        match SqlSession::parse_default_expr(&source) {
            Ok(expression) => {
                *right = Box::new(expression);
                self.changed = true;
                ControlFlow::Continue(())
            }
            Err(error) => ControlFlow::Break(error),
        }
    }
}
