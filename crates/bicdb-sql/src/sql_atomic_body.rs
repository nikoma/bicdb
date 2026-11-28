//! SQL-standard function bodies carry statement delimiters inside `BEGIN ATOMIC`.
use crate::*;
use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::BeginEndStatements;
use sqlparser::keywords::Keyword;
use std::ops::Range;

pub(crate) struct AtomicBody {
    pub(crate) span: Range<usize>,
    body: Range<usize>,
}

pub(crate) fn atomic_bodies(sql: &str) -> Vec<AtomicBody> {
    if !sql
        .as_bytes()
        .windows(6)
        .any(|part| part.eq_ignore_ascii_case(b"atomic"))
    {
        return Vec::new();
    }
    let Some(tokens) = tokens_with_source(&PostgreSqlDialect {}, sql) else {
        return Vec::new();
    };
    let mut offset = 0;
    let mut code = Vec::new();
    for (token, source) in tokens {
        let end = offset + source.len();
        if !matches!(token, Token::Whitespace(_)) {
            code.push((token, offset, end));
        }
        offset = end;
    }
    let keyword = |index: usize, keyword| {
        matches!(code.get(index).map(|(token, _, _)| token), Some(Token::Word(word))
            if word.quote_style.is_none() && word.keyword == keyword)
    };
    let is_atomic_start = |i| keyword(i, Keyword::BEGIN) && keyword(i + 1, Keyword::ATOMIC);
    let header = |i| {
        keyword(i, Keyword::CREATE)
            && (keyword(i + 1, Keyword::FUNCTION)
                || (keyword(i + 1, Keyword::OR)
                    && keyword(i + 2, Keyword::REPLACE)
                    && keyword(i + 3, Keyword::FUNCTION)))
    };
    let mut spans = Vec::new();
    let mut at = 0;
    let mut statement_start = 0;
    let mut parentheses = 0usize;
    while at < code.len() {
        match code[at].0 {
            Token::SemiColon if parentheses == 0 => statement_start = at + 1,
            Token::LParen => parentheses += 1,
            Token::RParen => parentheses = parentheses.saturating_sub(1),
            _ => {}
        }
        if parentheses == 0 && is_atomic_start(at) && header(statement_start) {
            let mut end = at + 2;
            let mut command_start = end;
            // CASE … END is an expression within an SQL statement.
            let mut cases = 0usize;
            let mut blocks = 1usize;
            while end < code.len() {
                if matches!(code[end].0, Token::SemiColon) {
                    command_start = end + 1;
                } else if is_atomic_start(end) && header(command_start) {
                    blocks += 1;
                } else if keyword(end, Keyword::CASE) {
                    cases += 1;
                } else if keyword(end, Keyword::END) {
                    if cases > 0 {
                        cases -= 1;
                    } else {
                        blocks -= 1;
                        if blocks == 0 {
                            break;
                        }
                    }
                }
                end += 1;
            }
            let Some((_, end_start, end_end)) = code.get(end) else {
                break;
            };
            spans.push(AtomicBody {
                span: code[at].1..*end_end,
                body: code[at + 1].2..*end_start,
            });
            at = end;
        }
        at += 1;
    }
    spans
}

pub(crate) struct LoweredAtomicBodies<'a> {
    pub(crate) sql: String,
    bodies: Vec<(String, &'a str)>,
}

/// Supply an opaque body for header parsing. The SQL body is parsed and
/// restored separately so its references and parameters retain their scope.
pub(crate) fn lower(sql: &str) -> Option<LoweredAtomicBodies<'_>> {
    let spans = atomic_bodies(sql);
    if spans.is_empty() {
        return None;
    }
    let mut output = String::with_capacity(sql.len());
    let mut bodies = Vec::with_capacity(spans.len());
    let mut cursor = 0;
    let mut next_tag = 0;
    for span in spans {
        let tag = loop {
            let tag = format!("bicdb_sql_body_{next_tag}");
            next_tag += 1;
            if !sql.contains(&format!("${tag}$")) {
                break tag;
            }
        };
        output.push_str(&sql[cursor..span.span.start]);
        output.push_str(&format!("AS ${tag}$${tag}$"));
        bodies.push((tag, &sql[span.body]));
        cursor = span.span.end;
    }
    output.push_str(&sql[cursor..]);
    Some(LoweredAtomicBodies {
        sql: output,
        bodies,
    })
}

impl LoweredAtomicBodies<'_> {
    pub(crate) fn restore(&self, statements: &mut [Statement]) -> Result<()> {
        for statement in statements {
            let Statement::CreateFunction(function) = statement else {
                continue;
            };
            let Some(CreateFunctionBody::AsBeforeOptions {
                body: Expr::Value(value),
                ..
            }) = &function.function_body
            else {
                continue;
            };
            let Value::DollarQuotedString(quoted) = &value.value else {
                continue;
            };
            let Some((_, body)) = self
                .bodies
                .iter()
                .find(|(tag, _)| quoted.tag.as_ref() == Some(tag))
            else {
                continue;
            };
            let mut commands = Vec::new();
            for command in split_sql_statements(body) {
                let tokens =
                    tokens_with_source(&PostgreSqlDialect {}, &command).unwrap_or_default();
                let mut tokens = tokens.iter().peekable();
                let mut start = 0;
                while tokens
                    .peek()
                    .is_some_and(|(token, _)| matches!(token, Token::Whitespace(_)))
                {
                    start += tokens
                        .next()
                        .expect("a whitespace token was checked")
                        .1
                        .len();
                }
                let translated = matches!(tokens.next(), Some((Token::Word(word), _))
                    if word.keyword == Keyword::RETURN && word.quote_style.is_none())
                .then(|| format!("SELECT {}", &command[start + "RETURN".len()..]));
                commands.extend(parse_statements(translated.as_deref().unwrap_or(&command))?);
            }
            function.function_body = Some(CreateFunctionBody::AsBeginEnd(BeginEndStatements {
                begin_token: AttachedToken::empty(),
                statements: commands,
                end_token: AttachedToken::empty(),
            }));
        }
        Ok(())
    }
}

pub(crate) fn render_function(function: &CreateFunction) -> String {
    let Some(CreateFunctionBody::AsBeginEnd(block)) = &function.function_body else {
        return crate::sql_text::sql_text(function);
    };
    let mut header = function.clone();
    header.function_body = None;
    let mut rendered = crate::sql_text::sql_text(&header);
    rendered.push_str(" BEGIN ATOMIC");
    for statement in &block.statements {
        rendered.push_str("\n    ");
        rendered.push_str(&crate::sql_text::sql_text(statement));
        rendered.push(';');
    }
    rendered.push_str("\nEND");
    rendered
}
