//! Collection-only infix grammar layered over PostgreSQL parsing.
use sqlparser::ast::{BinaryOperator, Expr, Statement, Value};
use sqlparser::dialect::{Dialect, PostgreSqlDialect, Precedence};
use sqlparser::keywords::Keyword;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::Token;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Operation {
    UnionAll,
    UnionDistinct,
    IntersectAll,
    IntersectDistinct,
    ExceptAll,
    ExceptDistinct,
    IsEmpty,
    IsNotEmpty,
}
impl Operation {
    pub(crate) fn marker(self) -> &'static str {
        match self {
            Self::UnionAll => "bicdb_oracle_multiset_union_all",
            Self::UnionDistinct => "bicdb_oracle_multiset_union_distinct",
            Self::IntersectAll => "bicdb_oracle_multiset_intersect_all",
            Self::IntersectDistinct => "bicdb_oracle_multiset_intersect_distinct",
            Self::ExceptAll => "bicdb_oracle_multiset_except_all",
            Self::ExceptDistinct => "bicdb_oracle_multiset_except_distinct",
            Self::IsEmpty => "bicdb_oracle_multiset_is_empty",
            Self::IsNotEmpty => "bicdb_oracle_multiset_is_not_empty",
        }
    }
    pub(crate) fn from_marker(value: &str) -> Option<Self> {
        [
            Self::UnionAll,
            Self::UnionDistinct,
            Self::IntersectAll,
            Self::IntersectDistinct,
            Self::ExceptAll,
            Self::ExceptDistinct,
            Self::IsEmpty,
            Self::IsNotEmpty,
        ]
        .into_iter()
        .find(|op| op.marker() == value)
    }
}

fn word(token: &Token, value: &str) -> bool {
    matches!(token,Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(value))
}
fn operation(parser: &Parser<'_>) -> Option<Operation> {
    let first = parser.peek_token().token;
    let second = parser.peek_nth_token(1).token;
    if word(&first, "MULTISET") {
        if word(&second, "INTERSECT") {
            return Some(Operation::IntersectAll);
        }
        if word(&second, "UNION") {
            return Some(Operation::UnionAll);
        }
        if word(&second, "EXCEPT") {
            return Some(Operation::ExceptAll);
        }
    }
    if word(&first, "IS") {
        if word(&second, "EMPTY") {
            return Some(Operation::IsEmpty);
        }
        if word(&second, "NOT") && word(&parser.peek_nth_token(2).token, "EMPTY") {
            return Some(Operation::IsNotEmpty);
        }
    }
    None
}

#[derive(Debug)]
pub(crate) struct CollectionDialect {
    query_settings: bool,
}
macro_rules! delegate_pg_bool {
    ($($name:ident),* $(,)?) => { $(fn $name(&self)->bool { PostgreSqlDialect {}.$name() })* };
}
impl Dialect for CollectionDialect {
    fn supports_settings(&self) -> bool {
        self.query_settings
    }
    // Keep PostgreSQL's dialect-specific branches and feature switches. Only
    // the collection infix grammar below differs, inside the preview scope.
    fn dialect(&self) -> std::any::TypeId {
        PostgreSqlDialect {}.dialect()
    }
    fn supports_outer_join_operator(&self) -> bool {
        true
    }
    fn identifier_quote_style(&self, value: &str) -> Option<char> {
        PostgreSqlDialect {}.identifier_quote_style(value)
    }
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        PostgreSqlDialect {}.is_delimited_identifier_start(ch)
    }
    fn is_identifier_start(&self, ch: char) -> bool {
        PostgreSqlDialect {}.is_identifier_start(ch)
    }
    fn is_identifier_part(&self, ch: char) -> bool {
        PostgreSqlDialect {}.is_identifier_part(ch)
    }
    fn is_custom_operator_part(&self, ch: char) -> bool {
        PostgreSqlDialect {}.is_custom_operator_part(ch)
    }
    fn is_reserved_for_identifier(&self, kw: Keyword) -> bool {
        PostgreSqlDialect {}.is_reserved_for_identifier(kw)
    }
    fn prec_value(&self, prec: Precedence) -> u8 {
        PostgreSqlDialect {}.prec_value(prec)
    }
    delegate_pg_bool!(
        supports_unicode_string_literal,
        supports_filter_during_aggregation,
        supports_group_by_expr,
        allow_extract_custom,
        allow_extract_single_quotes,
        supports_create_index_with_clause,
        supports_explain_with_utility_options,
        supports_listen_notify,
        supports_factorial_operator,
        supports_bitwise_shift_operators,
        supports_comment_on,
        supports_load_extension,
        supports_named_fn_args_with_colon_operator,
        supports_named_fn_args_with_expr_name,
        supports_empty_projections,
        supports_nested_comments,
        supports_string_escape_constant,
        supports_numeric_literal_underscores,
        supports_array_typedef_with_brackets,
        supports_geometric_types,
        supports_set_names,
        supports_alter_column_type_using,
        supports_notnull_operator,
        supports_interval_options,
        supports_insert_table_alias,
        supports_create_table_like_parenthesized,
        supports_select_wildcard_with_alias,
        supports_comma_separated_trim,
        supports_xml_expressions,
        supports_comment_optimizer_hint
    );
    fn get_next_precedence(&self, parser: &Parser<'_>) -> Option<Result<u8, ParserError>> {
        let precedence = match operation(parser) {
            Some(Operation::IntersectAll) => self.prec_value(Precedence::MulDivModOp),
            Some(Operation::UnionAll | Operation::ExceptAll) => {
                self.prec_value(Precedence::PlusMinus)
            }
            Some(Operation::IsEmpty | Operation::IsNotEmpty) => self.prec_value(Precedence::Is),
            _ => return PostgreSqlDialect {}.get_next_precedence(parser),
        };
        Some(Ok(precedence))
    }
    fn parse_infix(
        &self,
        parser: &mut Parser<'_>,
        left: &Expr,
        precedence: u8,
    ) -> Option<Result<Expr, ParserError>> {
        let op = operation(parser)?;
        Some((|| {
            parser.next_token();
            parser.next_token();
            let (op, right) = match op {
                Operation::IsEmpty => (op, Expr::Value(Value::Boolean(true).into())),
                Operation::IsNotEmpty => {
                    parser.next_token();
                    (op, Expr::Value(Value::Boolean(true).into()))
                }
                _ => {
                    let distinct = parser.parse_keyword(Keyword::DISTINCT);
                    if !distinct {
                        let _ = parser.parse_keyword(Keyword::ALL);
                    }
                    let op = match (op, distinct) {
                        (Operation::UnionAll, true) => Operation::UnionDistinct,
                        (Operation::IntersectAll, true) => Operation::IntersectDistinct,
                        (Operation::ExceptAll, true) => Operation::ExceptDistinct,
                        _ => op,
                    };
                    (op, parser.parse_subexpr(precedence)?)
                }
            };
            Ok(Expr::BinaryOp {
                left: Box::new(left.clone()),
                op: BinaryOperator::Custom(op.marker().into()),
                right: Box::new(right),
            })
        })())
    }
}

pub(crate) fn parse(sql: &str) -> Result<Vec<Statement>, ParserError> {
    Parser::parse_sql(
        &CollectionDialect {
            query_settings: false,
        },
        sql,
    )
}

pub(crate) fn parse_query_settings(sql: &str) -> Result<Vec<Statement>, ParserError> {
    Parser::parse_sql(
        &CollectionDialect {
            query_settings: true,
        },
        sql,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::{SelectItem, SetExpr, UnaryOperator};
    fn expr(sql: &str) -> Expr {
        let statements = parse(sql).unwrap();
        let Statement::Query(query) = &statements[0] else {
            panic!("query")
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("select")
        };
        let SelectItem::UnnamedExpr(expr) = &select.projection[0] else {
            panic!("projection")
        };
        expr.clone()
    }
    fn op(expr: &Expr) -> Operation {
        let Expr::BinaryOp {
            op: BinaryOperator::Custom(marker),
            ..
        } = expr
        else {
            panic!("collection operator: {expr:?}")
        };
        Operation::from_marker(marker).unwrap()
    }
    #[test]
    fn intersection_precedes_union_and_empty_predicates() {
        let expression = expr("SELECT a MULTISET UNION b MULTISET INTERSECT c IS NOT EMPTY");
        assert_eq!(op(&expression), Operation::IsNotEmpty);
        let Expr::BinaryOp { left, .. } = expression else {
            unreachable!()
        };
        assert_eq!(op(&left), Operation::UnionAll);
        let Expr::BinaryOp { right, .. } = *left else {
            unreachable!()
        };
        assert_eq!(op(&right), Operation::IntersectAll);
    }
    #[test]
    fn distinct_all_and_boolean_precedence_are_explicit() {
        assert_eq!(
            op(&expr("SELECT a MULTISET EXCEPT DISTINCT b")),
            Operation::ExceptDistinct
        );
        assert_eq!(
            op(&expr("SELECT a MULTISET UNION ALL b")),
            Operation::UnionAll
        );
        let expression = expr("SELECT NOT a IS EMPTY OR b IS EMPTY");
        let Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } = expression
        else {
            panic!("OR")
        };
        let Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } = *left
        else {
            panic!("NOT")
        };
        assert_eq!(op(&expr), Operation::IsEmpty);
        assert_eq!(op(&right), Operation::IsEmpty);
    }
    #[test]
    fn ordinary_postgresql_grammar_is_delegated() {
        for sql in [
            "SELECT ARRAY[1,2]::integer[], a->>'name' FROM t WHERE a IS NOT NULL",
            "SELECT count(*) FILTER(WHERE k>0) FROM t",
            "SELECT 1 INTERSECT SELECT 1 UNION SELECT 2",
            "SELECT a AS \"MULTISET\", b AS \"EMPTY\" FROM t",
            "SELECT E'\\n', U&'d\\0061t', 1_000",
        ] {
            assert_eq!(
                parse(sql).unwrap(),
                Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap(),
                "{sql}"
            );
        }
        assert!(Parser::parse_sql(&PostgreSqlDialect {}, "SELECT a MULTISET INTERSECT b").is_err());
    }

    #[test]
    fn collection_grammar_composes_with_preview_outer_join_syntax() {
        let sql="SELECT CASE WHEN a MULTISET INTERSECT b IS NOT EMPTY THEN 1 END FROM t,u WHERE t.k=u.k(+)";
        assert!(parse(sql).is_ok());
        assert!(Parser::parse_sql(&PostgreSqlDialect {}, sql).is_err());
    }
}
