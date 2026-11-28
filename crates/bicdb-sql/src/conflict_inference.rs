//! Preserve PostgreSQL index inference through sqlparser 0.62.
//! Its ConflictTarget only represents undecorated column names. An object-name
//! function is a typed carrier in that field, distinct from every legal
//! PostgreSQL constraint identifier. Rendering restores the original grammar.
use crate::*;
use sqlparser::ast::{visit_statements_mut, IndexColumn, ObjectNamePart, ObjectNamePartFunction};
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
    targets: BTreeMap<String, (&'a str, Option<&'a str>)>,
}

struct InferenceClause {
    keys: Range<usize>,
    predicate: Option<Range<usize>>,
    target_end: usize,
    action: usize,
}

#[derive(Clone)]
pub(crate) struct InferenceElement {
    pub(crate) expr: Expr,
    pub(crate) operator_class: Option<String>,
}

pub(crate) struct IndexInference {
    pub(crate) elements: Vec<InferenceElement>,
    pub(crate) predicate: Option<Expr>,
}

fn inference_tokens(sql: &str) -> Option<Vec<SourceToken>> {
    if !sql
        .as_bytes()
        .windows(8)
        .any(|part| part.eq_ignore_ascii_case(b"conflict"))
    {
        return None;
    }
    source_tokens(sql)
}

fn clauses(tokens: &[SourceToken]) -> Vec<InferenceClause> {
    let mut result = Vec::new();
    let mut previous_action = 0;
    for i in 0..tokens.len() {
        if i < previous_action
            || !keyword(tokens, i, Keyword::ON)
            || !keyword(tokens, i + 1, Keyword::CONFLICT)
            || !open_paren(tokens, i + 2)
        {
            continue;
        }
        let Some(close) = closing_paren(tokens, i + 2) else {
            continue;
        };
        let key_tokens = &tokens[i + 3..close];
        let simple_columns = key_tokens.len() % 2 == 1
            && key_tokens.iter().enumerate().all(|(position, token)| {
                if position % 2 == 0 {
                    matches!(token.token, Token::Word(_))
                } else {
                    matches!(token.token, Token::Comma)
                }
            });
        let mut action = close + 1;
        let mut predicate = None;
        if keyword(tokens, action, Keyword::WHERE) {
            let mut depth = 0usize;
            let Some(end) = (action + 1..tokens.len()).find(|&at| {
                match tokens[at].token {
                    Token::LParen | Token::LBracket => depth += 1,
                    Token::RParen | Token::RBracket if depth > 0 => depth -= 1,
                    Token::RParen | Token::SemiColon => return true,
                    _ => {}
                }
                depth == 0 && keyword(tokens, at, Keyword::DO)
            }) else {
                continue;
            };
            predicate = Some(tokens[action].bytes.end..tokens[end].bytes.start);
            action = end;
        }
        if (simple_columns && predicate.is_none())
            || !keyword(tokens, action, Keyword::DO)
            || !(keyword(tokens, action + 1, Keyword::NOTHING)
                || keyword(tokens, action + 1, Keyword::UPDATE))
        {
            continue;
        }
        result.push(InferenceClause {
            keys: tokens[i + 2].bytes.end..tokens[close].bytes.start,
            predicate,
            target_end: tokens[close].bytes.end,
            action: tokens[action].bytes.start,
        });
        previous_action = action;
    }
    result
}

pub(crate) fn requires_lowering(sql: &str) -> bool {
    inference_tokens(sql).is_some_and(|tokens| !clauses(&tokens).is_empty())
}

fn blank_preserving_lines(text: &str) -> impl Iterator<Item = char> + '_ {
    text.chars()
        .map(|character| if character == '\n' { '\n' } else { ' ' })
}

/// Keep the source positions for search-path qualifier edits.
pub(crate) fn mask_for_binding(sql: &str) -> Option<String> {
    let tokens = inference_tokens(sql)?;
    let clauses = clauses(&tokens);
    if clauses.is_empty() {
        return None;
    }
    let mut masked = String::with_capacity(sql.len());
    let mut last = 0;
    for clause in clauses {
        masked.push_str(&sql[last..clause.keys.start]);
        let mut marker_added = false;
        for ch in blank_preserving_lines(&sql[clause.keys.clone()]) {
            if ch != '\n' && !marker_added {
                masked.push('x');
                marker_added = true;
            } else {
                masked.push(ch);
            }
        }
        if !marker_added {
            return None;
        }
        masked.push_str(&sql[clause.keys.end..clause.target_end]);
        masked.extend(blank_preserving_lines(
            &sql[clause.target_end..clause.action],
        ));
        last = clause.action;
    }
    masked.push_str(&sql[last..]);
    Some(masked)
}

pub(crate) fn lower(sql: &str) -> Option<LoweredInference<'_>> {
    let tokens = inference_tokens(sql)?;
    let clauses = clauses(&tokens);
    if clauses.is_empty() {
        return None;
    }
    let occupied = tokens
        .iter()
        .filter_map(|token| match &token.token {
            Token::Word(word) => Some(word.value.to_ascii_lowercase()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    let mut targets = BTreeMap::new();
    let mut output = String::with_capacity(sql.len());
    let mut cursor = 0;
    let mut marker_number = 0;
    for clause in clauses {
        let marker = loop {
            let marker = format!("{CARRIER}_{marker_number}");
            marker_number += 1;
            if !occupied.contains(&marker) {
                break marker;
            }
        };
        output.push_str(&sql[cursor..clause.keys.start]);
        output.push_str(&format!("\"{marker}\") "));
        cursor = clause.action;
        targets.insert(
            marker,
            (&sql[clause.keys], clause.predicate.map(|range| &sql[range])),
        );
    }
    output.push_str(&sql[cursor..]);
    Some(LoweredInference {
        sql: output,
        targets,
    })
}

fn parse_index_columns(text: &str) -> Result<Vec<IndexColumn>> {
    let parser_error =
        |error: sqlparser::parser::ParserError| SqlError::InvalidSql(error.to_string());
    let mut parser = Parser::new(&PostgreSqlDialect {})
        .try_with_sql(text)
        .map_err(parser_error)?;
    let columns = parser
        .parse_comma_separated(Parser::parse_create_index_expr)
        .map_err(parser_error)?;
    parser.expect_token(&Token::EOF).map_err(parser_error)?;
    Ok(columns)
}

fn parse_index_elements(text: &str) -> Result<Vec<InferenceElement>> {
    let columns = parse_index_columns(text)?;
    columns
        .into_iter()
        .map(|key| {
            let error = match key.column.options {
                options if options.asc.is_some() => {
                    Some("ASC/DESC is not allowed in ON CONFLICT clause")
                }
                options if options.nulls_first.is_some() => {
                    Some("NULLS FIRST/LAST is not allowed in ON CONFLICT clause")
                }
                _ => None,
            };
            if let Some(error) = error {
                return Err(SqlError::data_exception("42P10", error, None));
            }
            let expression = match &key.column.expr {
                Expr::Collate { expr, .. } => expr.as_ref(),
                expression => expression,
            };
            if !matches!(
                expression,
                Expr::Identifier(_) | Expr::Function(_) | Expr::Nested(_)
            ) || key
                .operator_class
                .as_ref()
                .is_some_and(|class| class.0.iter().any(|part| part.as_ident().is_none()))
            {
                return Err(SqlError::InvalidSql(
                    "invalid ON CONFLICT inference element".into(),
                ));
            }
            Ok(InferenceElement {
                expr: key.column.expr,
                operator_class: key.operator_class.map(|mut name| {
                    for part in &mut name.0 {
                        if let ObjectNamePart::Identifier(ident) = part {
                            if ident.quote_style.is_none() {
                                ident.value.make_ascii_lowercase();
                            }
                        }
                    }
                    name.to_string()
                }),
            })
        })
        .collect()
}

fn parse_index_predicate(text: &str) -> Result<Expr> {
    let invalid = || SqlError::InvalidSql("expected an index inference predicate".into());
    let parsed = parse_statements(&format!("SELECT ({text})"))?;
    let [Statement::Query(query)] = parsed.as_slice() else {
        return Err(invalid());
    };
    let Some(select) = query.body.as_select() else {
        return Err(invalid());
    };
    let [SelectItem::UnnamedExpr(Expr::Nested(predicate))] = select.projection.as_slice() else {
        return Err(invalid());
    };
    Ok(*predicate.clone())
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
            let [marker] = columns.as_slice() else {
                return ControlFlow::Continue(());
            };
            let Some((keys, predicate)) = self.targets.remove(&marker.value) else {
                return ControlFlow::Continue(());
            };
            let target = (|| {
                Ok(index_target(IndexInference {
                    elements: parse_index_elements(keys)?,
                    predicate: predicate.map(parse_index_predicate).transpose()?,
                }))
            })();
            match target {
                Ok(target) => conflict.conflict_target = Some(target),
                Err(error) => return ControlFlow::Break(Box::new(error)),
            }
            ControlFlow::Continue(())
        });
        if let ControlFlow::Break(error) = outcome {
            return Err(*error);
        }
        if !self.targets.is_empty() {
            return Err(SqlError::InvalidSql(
                "could not locate ON CONFLICT target".into(),
            ));
        }
        Ok(())
    }
}

pub(crate) fn index_target(inference: IndexInference) -> ConflictTarget {
    let plain = inference.elements.iter().all(|element| {
        element.operator_class.is_none() && matches!(element.expr, Expr::Identifier(_))
    });
    if plain && inference.predicate.is_none() {
        return ConflictTarget::Columns(
            inference
                .elements
                .into_iter()
                .map(|element| {
                    let Expr::Identifier(column) = element.expr else {
                        unreachable!("plain conflict target was checked");
                    };
                    column
                })
                .collect(),
        );
    }
    let keys = inference.elements.into_iter().map(|element| {
        if plain {
            element.expr
        } else {
            let operator_class = element
                .operator_class
                .map_or(Value::Null, Value::SingleQuotedString);
            Expr::Tuple(vec![element.expr, Expr::Value(operator_class.into())])
        }
    });
    let args = std::iter::once(Expr::Tuple(keys.collect()))
        .chain(inference.predicate)
        .map(|expr| FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)))
        .collect();
    ConflictTarget::OnConstraint(ObjectName(vec![ObjectNamePart::Function(
        ObjectNamePartFunction {
            name: Ident::new(CARRIER),
            args,
        },
    )]))
}

fn encoded_inference(target: &ConflictTarget) -> Option<(&[Expr], Option<&Expr>)> {
    let ConflictTarget::OnConstraint(ObjectName(parts)) = target else {
        return None;
    };
    let [ObjectNamePart::Function(function)] = parts.as_slice() else {
        return None;
    };
    if function.name.value != CARRIER || function.name.quote_style.is_some() {
        return None;
    }
    match function.args.as_slice() {
        [FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Tuple(keys)))] => Some((keys, None)),
        [FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Tuple(keys))), FunctionArg::Unnamed(FunctionArgExpr::Expr(predicate))] => {
            Some((keys, Some(predicate)))
        }
        _ => None,
    }
}

pub(crate) fn column_inference(target: &ConflictTarget) -> Option<(Vec<&Ident>, Option<&Expr>)> {
    if let ConflictTarget::Columns(columns) = target {
        return Some((columns.iter().collect(), None));
    }
    let (keys, predicate) = encoded_inference(target)?;
    let columns = keys
        .iter()
        .map(|key| match key {
            Expr::Identifier(name) => Some(name),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;
    Some((columns, predicate))
}

pub(crate) fn index_inference(target: &ConflictTarget) -> Option<IndexInference> {
    if let Some((columns, predicate)) = column_inference(target) {
        return Some(IndexInference {
            elements: columns
                .into_iter()
                .map(|column| InferenceElement {
                    expr: Expr::Identifier(column.clone()),
                    operator_class: None,
                })
                .collect(),
            predicate: predicate.cloned(),
        });
    }
    let (keys, predicate) = encoded_inference(target)?;
    let elements = keys
        .iter()
        .map(|key| {
            let Expr::Tuple(fields) = key else {
                return None;
            };
            let [expr, Expr::Value(operator_class)] = fields.as_slice() else {
                return None;
            };
            let operator_class = match &operator_class.value {
                Value::Null => None,
                Value::SingleQuotedString(name) => Some(name.clone()),
                _ => return None,
            };
            Some(InferenceElement {
                expr: expr.clone(),
                operator_class,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some(IndexInference {
        elements,
        predicate: predicate.cloned(),
    })
}

pub(crate) fn is_index_inference(target: &ConflictTarget) -> bool {
    matches!(target, ConflictTarget::Columns(_)) || encoded_inference(target).is_some()
}

fn render_keys(sql: &str, tokens: &[SourceToken], open: usize, end: usize) -> Option<String> {
    if !open_paren(tokens, open + 1) {
        return Some(sql[tokens[open].bytes.start..tokens[end].bytes.end].to_string());
    }
    let mut output = String::from("(");
    let mut element = open + 1;
    loop {
        let key_end = closing_paren(tokens, element)?;
        let class = tokens.get(key_end.checked_sub(1)?)?;
        let comma = tokens.get(key_end.checked_sub(2)?)?;
        if !matches!(comma.token, Token::Comma) || key_end >= end {
            return None;
        }
        output.push_str(sql[tokens[element].bytes.end..comma.bytes.start].trim());
        if let Token::SingleQuotedString(name) = &class.token {
            output.push(' ');
            output.push_str(name);
        } else if !keyword(tokens, key_end - 1, Keyword::NULL) {
            return None;
        }
        if key_end + 1 == end {
            output.push(')');
            return Some(output);
        }
        if !matches!(tokens.get(key_end + 1)?.token, Token::Comma)
            || !open_paren(tokens, key_end + 2)
        {
            return None;
        }
        output.push_str(", ");
        element = key_end + 2;
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
        let Some(end) = closing_paren(&tokens, i + 5) else {
            continue;
        };
        let has_predicate = matches!(
            tokens.get(keys_end + 1).map(|token| &token.token),
            Some(Token::Comma)
        );
        if keys_end + 1 != end && !has_predicate {
            continue;
        }
        let start = tokens[i + 2].bytes.start;
        if start < cursor {
            continue;
        }
        let keys = render_keys(sql, &tokens, i + 6, keys_end)?;
        output.push_str(&sql[cursor..start]);
        output.push_str(&keys);
        if has_predicate {
            output.push_str(" WHERE ");
            output.push_str(sql[tokens[keys_end + 1].bytes.end..tokens[end].bytes.start].trim());
        }
        cursor = tokens[end].bytes.end;
    }
    if cursor == 0 {
        return None;
    }
    output.push_str(&sql[cursor..]);
    Some(output)
}

pub(crate) fn no_conflict_arbiter() -> SqlError {
    SqlError::data_exception(
        "42P10",
        "there is no unique or exclusion constraint matching the ON CONFLICT specification",
        None,
    )
}

fn index_key_identity(schema: &TableSchema, expr: &Expr) -> Option<records::ConflictKeyIdentity> {
    let identity_column = |expr: &Expr| match unwrap_nested_expr(expr) {
        Expr::Identifier(column) => Some(ident_value(column)),
        Expr::CompoundIdentifier(parts) => parts.last().map(ident_value),
        _ => None,
    };
    if let Some(column) = identity_column(expr) {
        return Some(records::ConflictKeyIdentity::Column(column));
    }
    let Expr::Function(function) = unwrap_nested_expr(expr) else {
        return None;
    };
    let path = object_name_parts(&function.name);
    let name = match path.as_slice() {
        [name] => name,
        [namespace, name] if namespace == "pg_catalog" => name,
        _ => return None,
    };
    if !matches!(name.as_str(), "lower" | "upper") {
        return None;
    }
    let FunctionArguments::List(list) = &function.args else {
        return None;
    };
    if function.uses_odbc_syntax
        || !matches!(function.parameters, FunctionArguments::None)
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
        || list.duplicate_treatment.is_some()
        || !list.clauses.is_empty()
    {
        return None;
    }
    let arguments = positional_function_args(&function.args)?;
    let [argument] = arguments.as_slice() else {
        return None;
    };
    let column = schema.column(&identity_column(argument)?)?;
    if column.pg_type != "text" || column.user_type.is_some() {
        return None;
    }
    // Resolve qualified and unqualified arguments to the same catalog column.
    let mut function = function.clone();
    let FunctionArguments::List(arguments) = &mut function.args else {
        return None;
    };
    arguments.args = vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
        Expr::Identifier(Ident::with_quote('"', column.name.clone())),
    ))];
    let field = index_field_from_expr_for_schema(schema, &Expr::Function(function)).ok()?;
    Some(records::ConflictKeyIdentity::Expression(field))
}

pub(crate) fn declared_index_keys(
    schema: &TableSchema,
    index: &IndexSchema,
) -> Option<Vec<records::ConflictKeyIdentity>> {
    parse_index_columns(&index.expression)
        .ok()?
        .into_iter()
        .map(|element| {
            let expression = match unwrap_nested_expr(&element.column.expr) {
                Expr::Collate { expr, .. } => expr.as_ref(),
                expression => expression,
            };
            index_key_identity(schema, expression)
        })
        .collect()
}

fn operator_family(class: &str) -> Result<(i64, i32)> {
    let bare = match class.split_once('.') {
        None => Some(class.trim_matches('"')),
        Some((namespace, name)) if namespace.trim_matches('"') == "pg_catalog" => {
            Some(name.trim_matches('"'))
        }
        _ => None,
    };
    let spec = bare.and_then(|bare| {
        pg_opclass_specs()
            .iter()
            .find(|spec| spec.method_oid == access_method_oid("btree") && spec.name == bare)
    });
    spec.map(|spec| (spec.family_oid, spec.input_type_oid))
        .ok_or_else(|| {
            SqlError::undefined_object(format!(
                "operator class \"{class}\" does not exist for access method \"btree\""
            ))
        })
}

pub(crate) fn resolve_index_inference(
    schema: &TableSchema,
    target: &ConflictTarget,
    alias: Option<&Ident>,
) -> Result<(records::ConflictIndexInference, Vec<String>)> {
    let mut read_columns = BTreeSet::new();
    let mut columns = Vec::new();
    let mut keys = Vec::new();
    let owned;
    let predicate = if let Some((simple, predicate)) = column_inference(target) {
        for column in simple {
            let name = ident_value(column);
            if schema.column(&name).is_none_or(|column| column.hidden) {
                return Err(SqlError::data_exception(
                    "42703",
                    format!("column \"{name}\" does not exist"),
                    None,
                ));
            }
            if read_columns.insert(name.clone()) {
                columns.push(name);
            }
        }
        predicate
    } else {
        owned = index_inference(target).ok_or_else(no_conflict_arbiter)?;
        for element in &owned.elements {
            read_columns.extend(predicate_columns(&element.expr, schema, alias)?);
            let operator_family = element
                .operator_class
                .as_deref()
                .map(operator_family)
                .transpose()?;
            let (expr, collation) = match unwrap_nested_expr(&element.expr) {
                Expr::Collate { expr, .. } => (
                    expr.as_ref(),
                    Some(crate::session::index_expr_collation_oid(
                        &schema.columns,
                        &element.expr,
                    )?),
                ),
                expr => (expr, None),
            };
            let identity = index_key_identity(schema, expr).ok_or_else(no_conflict_arbiter)?;
            keys.push(records::ConflictKey {
                identity,
                collation,
                operator_family,
            });
        }
        owned.predicate.as_ref()
    };
    if let Some(predicate) = predicate {
        read_columns.extend(predicate_columns(predicate, schema, alias)?);
    }
    Ok((
        records::ConflictIndexInference {
            columns,
            keys,
            predicate: predicate
                .and_then(|predicate| crate::partial_index::inference_predicate(schema, predicate)),
        },
        read_columns.into_iter().collect(),
    ))
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
