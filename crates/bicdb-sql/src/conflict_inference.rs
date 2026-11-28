//! Preserve a PostgreSQL column inference predicate through sqlparser 0.62.
//! Its ConflictTarget has no expression field for WHERE. An object-name
//! function is a typed carrier in that field, distinct from every legal
//! PostgreSQL constraint identifier. Rendering restores the original grammar.
use crate::*;
use sqlparser::ast::{visit_statements_mut, ObjectNamePart, ObjectNamePartFunction};
use sqlparser::keywords::Keyword;
use std::ops::{ControlFlow, Range};

const CARRIER: &str = "__bicdb_conflict_inference";

struct SourceToken {
    token: Token,
    bytes: Range<usize>,
}

fn source_tokens(sql: &str) -> Option<Vec<SourceToken>> {
    let mut offset = 0;
    let mut tokens = Vec::new();
    for (token, source) in tokens_with_source(&PostgreSqlDialect {}, sql)? {
        let end = offset + source.len();
        if !matches!(token, Token::Whitespace(_)) {
            tokens.push(SourceToken {
                token,
                bytes: offset..end,
            });
        }
        offset = end;
    }
    Some(tokens)
}

fn keyword(tokens: &[SourceToken], index: usize, keyword: Keyword) -> bool {
    matches!(tokens.get(index).map(|token| &token.token), Some(Token::Word(word))
        if word.quote_style.is_none() && word.keyword == keyword)
}

fn open_paren(tokens: &[SourceToken], index: usize) -> bool {
    matches!(
        tokens.get(index).map(|token| &token.token),
        Some(Token::LParen)
    )
}

fn closing_paren(tokens: &[SourceToken], open: usize) -> Option<usize> {
    let mut depth = 0usize;
    for (index, token) in tokens.iter().enumerate().skip(open) {
        match token.token {
            Token::LParen => depth += 1,
            Token::RParen => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(index);
                }
            }
            Token::SemiColon => return None,
            _ => {}
        }
    }
    None
}

pub(crate) struct LoweredInference<'a> {
    pub(crate) sql: String,
    predicates: BTreeMap<String, &'a str>,
}

pub(crate) fn lower(sql: &str) -> Option<LoweredInference<'_>> {
    if !sql
        .as_bytes()
        .windows(8)
        .any(|part| part.eq_ignore_ascii_case(b"conflict"))
    {
        return None;
    }
    let tokens = source_tokens(sql)?;
    let mut predicates = BTreeMap::new();
    let mut output = String::with_capacity(sql.len());
    let mut cursor = 0;
    let mut marker_number = 0;
    for i in 0..tokens.len() {
        if !keyword(&tokens, i, Keyword::ON)
            || !keyword(&tokens, i + 1, Keyword::CONFLICT)
            || !open_paren(&tokens, i + 2)
        {
            continue;
        }
        let Some(close) = closing_paren(&tokens, i + 2) else {
            continue;
        };
        if !keyword(&tokens, close + 1, Keyword::WHERE) {
            continue;
        }
        let start = tokens[close + 1].bytes.end;
        let mut depth = 0usize;
        let Some(action) = (close + 2..tokens.len()).find(|&at| {
            match tokens[at].token {
                Token::LParen | Token::LBracket => depth += 1,
                Token::RParen | Token::RBracket if depth > 0 => depth -= 1,
                Token::RParen | Token::SemiColon => return true,
                _ => {}
            }
            depth == 0 && keyword(&tokens, at, Keyword::DO)
        }) else {
            continue;
        };
        if !keyword(&tokens, action, Keyword::DO)
            || !(keyword(&tokens, action + 1, Keyword::NOTHING)
                || keyword(&tokens, action + 1, Keyword::UPDATE))
        {
            continue;
        }
        let end = tokens[action].bytes.start;
        let replace_from = tokens[close].bytes.start;
        if replace_from < cursor {
            continue;
        }
        let marker = loop {
            let marker = format!("{CARRIER}_{marker_number}");
            marker_number += 1;
            // A quoted identifier with Unicode escapes can spell our marker
            // without containing its literal source text. Inspect decoded tokens.
            if !tokens.iter().any(|token|
                matches!(&token.token, Token::Word(word) if word.value.eq_ignore_ascii_case(&marker))
            ) {
                break marker;
            }
        };
        output.push_str(&sql[cursor..replace_from]);
        output.push_str(&format!(", \"{marker}\") "));
        predicates.insert(marker, &sql[start..end]);
        cursor = end;
    }
    if predicates.is_empty() {
        return None;
    }
    output.push_str(&sql[cursor..]);
    Some(LoweredInference {
        sql: output,
        predicates,
    })
}

impl LoweredInference<'_> {
    pub(crate) fn restore(mut self, statements: &mut Vec<Statement>) -> Result<()> {
        let outcome = visit_statements_mut(statements, |statement| {
            let Statement::Insert(insert) = statement else {
                return ControlFlow::Continue(());
            };
            let Some(OnInsert::OnConflict(conflict)) = &mut insert.on else {
                return ControlFlow::Continue(());
            };
            let Some(ConflictTarget::Columns(columns)) = &mut conflict.conflict_target else {
                return ControlFlow::Continue(());
            };
            let Some(predicate) = columns
                .last()
                .and_then(|column| self.predicates.remove(&column.value))
            else {
                return ControlFlow::Continue(());
            };
            let parsed = match parse_statements(&format!("SELECT ({predicate})")) {
                Ok(parsed) => parsed,
                Err(error) => return ControlFlow::Break(Box::new(error)),
            };
            let [Statement::Query(query)] = parsed.as_slice() else {
                return ControlFlow::Break(Box::new(SqlError::InvalidSql(
                    "expected an index inference predicate".into(),
                )));
            };
            let Some(select) = query.body.as_select() else {
                return ControlFlow::Break(Box::new(SqlError::InvalidSql(
                    "expected an index inference predicate".into(),
                )));
            };
            let [SelectItem::UnnamedExpr(Expr::Nested(predicate))] = select.projection.as_slice()
            else {
                return ControlFlow::Break(Box::new(SqlError::InvalidSql(
                    "expected an index inference predicate".into(),
                )));
            };
            columns.pop();
            let keys = Expr::Tuple(
                std::mem::take(columns)
                    .into_iter()
                    .map(Expr::Identifier)
                    .collect(),
            );
            let args = [keys, *predicate.clone()]
                .into_iter()
                .map(|expr| FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)))
                .collect();
            conflict.conflict_target = Some(ConflictTarget::OnConstraint(ObjectName(vec![
                ObjectNamePart::Function(ObjectNamePartFunction {
                    name: Ident::new(CARRIER),
                    args,
                }),
            ])));
            ControlFlow::Continue(())
        });
        if let ControlFlow::Break(error) = outcome {
            return Err(*error);
        }
        if !self.predicates.is_empty() {
            return Err(SqlError::InvalidSql(
                "could not locate ON CONFLICT target".into(),
            ));
        }
        Ok(())
    }
}

pub(crate) fn column_inference(target: &ConflictTarget) -> Option<(Vec<&Ident>, Option<&Expr>)> {
    match target {
        ConflictTarget::Columns(columns) => Some((columns.iter().collect(), None)),
        ConflictTarget::OnConstraint(ObjectName(parts)) => {
            let [ObjectNamePart::Function(function)] = parts.as_slice() else {
                return None;
            };
            if function.name.value != CARRIER || function.name.quote_style.is_some() {
                return None;
            }
            let [FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Tuple(keys))), FunctionArg::Unnamed(FunctionArgExpr::Expr(predicate))] =
                function.args.as_slice()
            else {
                return None;
            };
            let columns = keys
                .iter()
                .map(|key| match key {
                    Expr::Identifier(name) => Some(name),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            Some((columns, Some(predicate)))
        }
    }
}

pub(crate) fn render(sql: &str) -> Option<String> {
    if !sql.contains(CARRIER) {
        return None;
    }
    let tokens = source_tokens(sql)?;
    let mut output = String::with_capacity(sql.len());
    let mut cursor = 0;
    for i in 0..tokens.len() {
        if !keyword(&tokens, i, Keyword::ON)
            || !keyword(&tokens, i + 1, Keyword::CONFLICT)
            || !keyword(&tokens, i + 2, Keyword::ON)
            || !keyword(&tokens, i + 3, Keyword::CONSTRAINT)
            || !matches!(tokens.get(i + 4).map(|token| &token.token), Some(Token::Word(word)) if word.value == CARRIER && word.quote_style.is_none())
            || !open_paren(&tokens, i + 5)
            || !open_paren(&tokens, i + 6)
        {
            continue;
        }
        let Some(keys_end) = closing_paren(&tokens, i + 6) else {
            continue;
        };
        if !matches!(
            tokens.get(keys_end + 1).map(|token| &token.token),
            Some(Token::Comma)
        ) {
            continue;
        }
        let Some(end) = closing_paren(&tokens, i + 5) else {
            continue;
        };
        let start = tokens[i + 2].bytes.start;
        if start < cursor {
            continue;
        }
        output.push_str(&sql[cursor..start]);
        output.push_str(&sql[tokens[i + 6].bytes.start..tokens[keys_end].bytes.end]);
        output.push_str(" WHERE ");
        output.push_str(sql[tokens[keys_end + 1].bytes.end..tokens[end].bytes.start].trim());
        cursor = tokens[end].bytes.end;
    }
    if cursor == 0 {
        return None;
    }
    output.push_str(&sql[cursor..]);
    Some(output)
}

/// Index predicates see the target table (or its alias) only. Collect the
/// referenced columns for the same SELECT privilege check as the target keys.
pub(crate) fn predicate_columns(
    predicate: &Expr,
    schema: &TableSchema,
    alias: Option<&Ident>,
) -> Result<BTreeSet<String>> {
    use sqlparser::ast::{Visit, Visitor};

    struct References<'a> {
        schema: &'a TableSchema,
        relation: String,
        alias: Option<String>,
        columns: BTreeSet<String>,
    }

    impl References<'_> {
        fn column(&mut self, ident: &Ident) -> Result<()> {
            let name = ident_value(ident);
            if self
                .schema
                .columns
                .iter()
                .any(|column| column.name == name && !column.hidden)
            {
                self.columns.insert(name);
                return Ok(());
            }
            Err(SqlError::data_exception_public(
                "42703",
                format!("column \"{name}\" does not exist"),
                None,
            ))
        }

        fn qualified_column(&mut self, parts: &[Ident]) -> Result<()> {
            let [qualifiers @ .., column] = parts else {
                return Ok(());
            };
            let [prefix @ .., qualifier] = qualifiers else {
                return self.column(column);
            };
            let qualifier = ident_value(qualifier);
            let expected = self.alias.as_deref().unwrap_or(&self.relation);
            let namespace_agrees = match prefix {
                [] => true,
                [namespace] => {
                    self.alias.is_none() && ident_value(namespace) == self.schema.schema_name
                }
                _ => false,
            };
            if qualifier == expected && namespace_agrees {
                return self.column(column);
            }
            if let Some(alias) = &self.alias {
                if qualifier == self.relation {
                    return Err(SqlError::coded(
                        "42P01",
                        format!("invalid reference to FROM-clause entry for table \"{qualifier}\""),
                        None,
                        Some(format!(
                            "Perhaps you meant to reference the table alias \"{alias}\"."
                        )),
                    ));
                }
            }
            Err(SqlError::data_exception_public(
                "42P01",
                format!("missing FROM-clause entry for table \"{qualifier}\""),
                None,
            ))
        }
    }

    impl Visitor for References<'_> {
        type Break = Box<SqlError>;

        fn pre_visit_query(&mut self, _: &Query) -> ControlFlow<Self::Break> {
            ControlFlow::Break(Box::new(SqlError::data_exception_public(
                "0A000",
                "cannot use subquery in index predicate",
                None,
            )))
        }

        fn pre_visit_expr(&mut self, expression: &Expr) -> ControlFlow<Self::Break> {
            let result = match expression {
                Expr::Identifier(ident) => self.column(ident),
                Expr::CompoundIdentifier(parts) => self.qualified_column(parts),
                Expr::Function(function) if function.over.is_some() => {
                    Err(SqlError::data_exception_public(
                        "42P20",
                        "window functions are not allowed in index predicates",
                        None,
                    ))
                }
                Expr::Function(function) if is_aggregate_function(function) => {
                    Err(SqlError::data_exception_public(
                        "42803",
                        "aggregate functions are not allowed in index predicates",
                        None,
                    ))
                }
                _ => Ok(()),
            };
            match result {
                Ok(()) => ControlFlow::Continue(()),
                Err(error) => ControlFlow::Break(Box::new(error)),
            }
        }
    }

    let mut references = References {
        schema,
        relation: catalog_display_name(&schema.name),
        alias: alias.map(ident_value),
        columns: BTreeSet::new(),
    };
    match predicate.visit(&mut references) {
        ControlFlow::Break(error) => Err(*error),
        ControlFlow::Continue(()) => Ok(references.columns),
    }
}
