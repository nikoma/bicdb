//! PostgreSQL dynamic SQL values stay bound; diagnostics belong to a call frame.
use crate::*;

#[derive(Clone, Debug)]
pub(crate) enum PostgresControl {
    Execute {
        command: RoutineExpr,
        targets: Option<Vec<String>>,
        strict: bool,
        parameters: Vec<RoutineExpr>,
    },
    RowCount {
        targets: Vec<String>,
    },
}

impl PostgresControl {
    pub(crate) fn targets(&self) -> &[String] {
        match self {
            Self::Execute { targets, .. } => targets.as_deref().unwrap_or(&[]),
            Self::RowCount { targets } => targets,
        }
    }
    pub(crate) fn for_each_expr_mut(&mut self, mut apply: impl FnMut(&mut RoutineExpr)) {
        if let Self::Execute {
            command,
            parameters,
            ..
        } = self
        {
            apply(command);
            for parameter in parameters {
                apply(parameter);
            }
        }
    }
}

fn keyword(token: &Token, expected: &str) -> bool {
    matches!(token,Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(expected))
}
fn text(tokens: &[Token]) -> String {
    tokens
        .iter()
        .map(|token| match token {
            // Token Display does not escape decoded quotes.
            Token::SingleQuotedString(value) | Token::EscapedStringLiteral(value) => {
                pg_quote_literal(value)
            }
            Token::Word(word) if word.quote_style == Some('"') => {
                format!("\"{}\"", word.value.replace('"', "\"\""))
            }
            other => other.to_string(),
        })
        .collect::<Vec<_>>()
        .join(" ")
}
fn expression(tokens: &[Token]) -> Result<RoutineExpr> {
    let mut parser = Parser::new(&PostgreSqlDialect {})
        .try_with_sql(&text(tokens))
        .map_err(|e| SqlError::InvalidSql(e.to_string()))?;
    let expr = parser
        .parse_expr()
        .map_err(|e| SqlError::InvalidSql(e.to_string()))?;
    if parser.peek_token().token != Token::EOF {
        return Err(SqlError::InvalidSql(
            "trailing tokens in dynamic SQL expression".into(),
        ));
    }
    Ok(RoutineExpr::unbound(expr))
}
fn comma_parts(tokens: &[Token]) -> Vec<&[Token]> {
    let mut output = Vec::new();
    let mut start = 0;
    let mut depth = 0usize;
    for (at, token) in tokens.iter().enumerate() {
        match token {
            Token::LParen | Token::LBracket => depth += 1,
            Token::RParen | Token::RBracket => depth = depth.saturating_sub(1),
            Token::Comma if depth == 0 => {
                output.push(&tokens[start..at]);
                start = at + 1;
            }
            _ => {}
        }
    }
    output.push(&tokens[start..]);
    output
}
fn target(tokens: &[Token]) -> Result<String> {
    let [Token::Word(word)] = tokens else {
        return Err(SqlError::UnsupportedPlpgsqlFeature(
            "dynamic INTO and diagnostics require scalar or record variable targets".into(),
        ));
    };
    Ok(if word.quote_style.is_some() {
        word.value.clone()
    } else {
        word.value.to_ascii_lowercase()
    })
}

pub(crate) fn parse(statement: &str) -> Result<Option<PostgresControl>> {
    let first = leading_sql_keyword(statement);
    if !first.is_some_and(|s| s.eq_ignore_ascii_case("EXECUTE") || s.eq_ignore_ascii_case("GET")) {
        return Ok(None);
    }
    let tokens = Tokenizer::new(&PostgreSqlDialect {}, statement)
        .tokenize()
        .map_err(|e| SqlError::InvalidSql(e.to_string()))?
        .into_iter()
        .filter(|t| !matches!(t, Token::Whitespace(_)))
        .collect::<Vec<_>>();
    if first.is_some_and(|s| s.eq_ignore_ascii_case("GET")) {
        let mut at = 1;
        if tokens.get(at).is_some_and(|t| keyword(t, "CURRENT")) {
            at += 1;
        }
        if !tokens.get(at).is_some_and(|t| keyword(t, "DIAGNOSTICS")) {
            return Ok(None);
        }
        let mut targets = Vec::new();
        for part in comma_parts(&tokens[at + 1..]) {
            if part.len() != 3
                || !matches!(part[1], Token::Eq | Token::Assignment)
                || !keyword(&part[2], "ROW_COUNT")
            {
                return Err(SqlError::UnsupportedPlpgsqlFeature(
                    "GET DIAGNOSTICS currently supports ROW_COUNT".into(),
                ));
            }
            targets.push(target(&part[..1])?);
        }
        return Ok(Some(PostgresControl::RowCount { targets }));
    }
    if tokens.get(1).is_some_and(|t| keyword(t, "IMMEDIATE")) {
        return Ok(None);
    }
    let source = &tokens[1..];
    let mut depth = 0usize;
    let mut into = None;
    let mut using = None;
    for (at, token) in source.iter().enumerate() {
        match token {
            Token::LParen | Token::LBracket => depth += 1,
            Token::RParen | Token::RBracket => depth = depth.saturating_sub(1),
            _ if depth == 0
                && at.checked_sub(1).and_then(|i| source.get(i)) != Some(&Token::Period) =>
            {
                if keyword(token, "INTO") {
                    if into.replace(at).is_some() {
                        return Err(SqlError::InvalidSql("duplicate dynamic INTO clause".into()));
                    }
                }
                if keyword(token, "USING") {
                    if using.replace(at).is_some() {
                        return Err(SqlError::InvalidSql(
                            "duplicate dynamic USING clause".into(),
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    if into.is_none() && using.is_none() {
        return Ok(None);
    }
    let end = into.into_iter().chain(using).min().unwrap();
    let command = expression(&source[..end])?;
    let clause_end = |start| {
        into.into_iter()
            .chain(using)
            .filter(|end| *end > start)
            .min()
            .unwrap_or(source.len())
    };
    let mut strict = false;
    let targets = if let Some(at) = into {
        let mut slice = &source[at + 1..clause_end(at)];
        if slice.first().is_some_and(|t| keyword(t, "STRICT")) {
            strict = true;
            slice = &slice[1..];
        }
        Some(
            comma_parts(slice)
                .into_iter()
                .map(target)
                .collect::<Result<Vec<_>>>()?,
        )
    } else {
        None
    };
    let parameters = if let Some(at) = using {
        comma_parts(&source[at + 1..clause_end(at)])
            .into_iter()
            .map(expression)
            .collect::<Result<Vec<_>>>()?
    } else {
        Vec::new()
    };
    Ok(Some(PostgresControl::Execute {
        command,
        targets,
        strict,
        parameters,
    }))
}

pub(crate) fn row_count(result: &SqlResult) -> i64 {
    result
        .command_tag
        .as_deref()
        .and_then(|tag| tag.rsplit(' ').next())
        .and_then(|value| value.parse().ok())
        .unwrap_or(result.rows.len() as i64)
}

impl SqlSession<'_> {
    pub(crate) fn execute_postgres_control(
        &mut self,
        frame: &mut RoutineFrame,
        control: &PostgresControl,
    ) -> Result<RoutineControl> {
        match control {
            PostgresControl::RowCount { targets } => {
                for target in targets {
                    self.assign_routine_value(
                        frame,
                        &RoutineAssignmentTarget::Variable(target.clone()),
                        SqlValue::Int(frame.last_sql_row_count),
                    )?;
                }
                Ok(RoutineControl::Continue)
            }
            PostgresControl::Execute {
                command,
                targets,
                strict,
                parameters,
            } => {
                let command = self.eval_routine_expr(frame, command)?;
                let SqlValue::String(sql) = command else {
                    return Err(SqlError::data_exception(
                        "22004",
                        "query string argument of EXECUTE is null",
                        None,
                    ));
                };
                let parameters = parameters
                    .iter()
                    .map(|parameter| self.eval_routine_expr(frame, parameter))
                    .collect::<Result<Vec<_>>>()?;
                let parsed = parse_single_statement(&sql)?;
                if !matches!(
                    parsed,
                    Statement::Query(_)
                        | Statement::Insert(_)
                        | Statement::Update(_)
                        | Statement::Delete(_)
                        | Statement::Merge(_)
                ) {
                    return Err(SqlError::UnsupportedPlpgsqlFeature(
                        "dynamic SQL with INTO/USING supports SELECT and DML".into(),
                    ));
                }
                let returning_dml = matches!(
                    parsed,
                    Statement::Insert(_)
                        | Statement::Update(_)
                        | Statement::Delete(_)
                        | Statement::Merge(_)
                );
                // PostgreSQL dynamic SQL never substitutes local variable names.
                // Only explicitly supplied USING values are visible as $n.
                let vars = std::mem::replace(
                    &mut self.routine_vars,
                    Arc::new(
                        parameters
                            .into_iter()
                            .enumerate()
                            .map(|(index, value)| (format!("${}", index + 1), value))
                            .collect(),
                    ),
                );
                let slots = self.routine_slots.take();
                let ir = self.current_routine_ir.take();
                let owned = std::mem::replace(&mut self.ir_owned_statement, false);
                let result = self.execute_parsed_statement(&parsed);
                self.routine_vars = vars;
                self.routine_slots = slots;
                self.current_routine_ir = ir;
                self.ir_owned_statement = owned;
                let result = result?;
                frame.last_sql_row_count = row_count(&result);
                if let Some(targets) = targets {
                    if *strict && result.rows.is_empty() {
                        return Err(SqlError::NoDataFound);
                    }
                    if (*strict || returning_dml) && result.rows.len() > 1 {
                        return Err(SqlError::RaisedException {
                            sqlstate: "P0003".into(),
                            message: "query returned more than one row".into(),
                            detail: None,
                        });
                    }
                    assign_routine_targets_with_columns(
                        frame,
                        targets,
                        &result.columns,
                        result.rows.into_iter().next(),
                    )?;
                }
                Ok(RoutineControl::Continue)
            }
        }
    }
}
