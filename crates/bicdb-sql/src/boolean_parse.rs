//! Balance associative Boolean groups before recursive SQL parsing. Generated
//! GridTable filters add two parenthesis levels per saved row. Raising the raw
//! parse limit would also admit exponentially expensive CAST grammar, so only
//! Boolean groups are reassociated, using an iterative token arena. Query,
//! CASE, BETWEEN, argument-list and window grammar remain opaque.
use crate::*;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Operator {
    And,
    Or,
}

enum Node {
    Token(Token),
    Sequence {
        children: Vec<usize>,
        enclosed: bool,
    },
    Boolean {
        op: Operator,
        left: usize,
        right: usize,
    },
}

fn word(token: &Token, expected: &str) -> bool {
    matches!(token, Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(expected))
}

fn sequence(nodes: &mut Vec<Node>, children: Vec<usize>, enclosed: bool) -> usize {
    if children.len() == 1 && (!enclosed || matches!(nodes[children[0]], Node::Boolean { .. })) {
        return children[0];
    }
    let id = nodes.len();
    nodes.push(Node::Sequence { children, enclosed });
    id
}

fn group(nodes: &mut Vec<Node>, children: Vec<usize>, opaque: bool) -> usize {
    // Splitting grammar other than a Boolean expression at OR/AND changes its
    // meaning. In particular BETWEEN owns an AND and CASE owns whole clauses.
    let blocked = opaque
        || children.iter().any(|id| match &nodes[*id] {
            Node::Token(Token::Comma | Token::SemiColon | Token::LBracket | Token::RBracket) => {
                true
            }
            Node::Token(token) => [
                "SELECT",
                "WITH",
                "VALUES",
                "TABLE",
                "CASE",
                "WHEN",
                "THEN",
                "ELSE",
                "END",
                "BETWEEN",
                "AS",
                "FROM",
                "WHERE",
                "GROUP",
                "ORDER",
                "HAVING",
                "LIMIT",
                "OFFSET",
                "UNION",
                "INTERSECT",
                "EXCEPT",
                "FILTER",
                "OVER",
                "PARTITION",
                "WINDOW",
            ]
            .iter()
            .any(|w| word(token, w)),
            _ => false,
        });
    if blocked {
        return sequence(nodes, children, true);
    }
    let op = if children
        .iter()
        .any(|id| matches!(&nodes[*id], Node::Token(t) if word(t, "OR")))
    {
        Some(Operator::Or)
    } else if children
        .iter()
        .any(|id| matches!(&nodes[*id], Node::Token(t) if word(t, "AND")))
    {
        Some(Operator::And)
    } else {
        None
    };
    let Some(op) = op else {
        return sequence(nodes, children, true);
    };
    let keyword = if op == Operator::Or { "OR" } else { "AND" };
    let mut terms = Vec::new();
    let mut current = Vec::new();
    for &id in &children {
        if matches!(&nodes[id], Node::Token(t) if word(t, keyword)) {
            if current.is_empty() {
                return sequence(nodes, children, true);
            }
            terms.push(sequence(nodes, std::mem::take(&mut current), false));
        } else {
            current.push(id);
        }
    }
    if current.is_empty() {
        return sequence(nodes, children, true);
    }
    terms.push(sequence(nodes, current, false));
    let mut result = terms[0];
    for right in terms.into_iter().skip(1) {
        let id = nodes.len();
        nodes.push(Node::Boolean {
            op,
            left: result,
            right,
        });
        result = id;
    }
    result
}

fn argument_group(nodes: &[Node], previous: Option<&usize>) -> bool {
    let Some(previous) = previous else {
        return false;
    };
    match &nodes[*previous] {
        Node::Token(Token::Word(w)) => {
            w.quote_style.is_some()
                || ![
                    "SELECT", "WHERE", "AND", "OR", "NOT", "ON", "HAVING", "WHEN", "THEN", "ELSE",
                ]
                .iter()
                .any(|s| w.value.eq_ignore_ascii_case(s))
        }
        // Adjacent groups may be function-call or subscript grammar. Their
        // inner Boolean children can still be normalized independently.
        Node::Sequence { .. } | Node::Boolean { .. } => true,
        _ => false,
    }
}

fn rewrite(sql: &str) -> Option<String> {
    let tokens = Tokenizer::new(&PostgreSqlDialect {}, sql).tokenize().ok()?;
    // The normal expression-node budget still applies after parsing. Bound
    // this extra representation too, before building the token arena.
    if tokens.len() > max_expression_nodes().saturating_mul(4) {
        return None;
    }
    let mut nodes = Vec::new();
    let mut frames = vec![(Vec::new(), true)];
    for token in tokens {
        match token {
            Token::Whitespace(_) => continue,
            Token::LParen => {
                let opaque = argument_group(&nodes, frames.last()?.0.last());
                frames.push((Vec::new(), opaque));
            }
            Token::RParen => {
                if frames.len() == 1 {
                    return None;
                }
                let (children, opaque) = frames.pop()?;
                let id = group(&mut nodes, children, opaque);
                frames.last_mut()?.0.push(id);
            }
            token => {
                let id = nodes.len();
                nodes.push(Node::Token(token));
                frames.last_mut()?.0.push(id);
            }
        }
    }
    if frames.len() != 1 {
        return None;
    }
    let root = sequence(&mut nodes, frames.pop()?.0, false);
    enum Task {
        Node(usize),
        Text(&'static str),
        Range {
            ids: Rc<Vec<usize>>,
            start: usize,
            end: usize,
            op: Operator,
        },
    }
    let mut tasks = vec![Task::Node(root)];
    let mut result = String::with_capacity(sql.len());
    while let Some(task) = tasks.pop() {
        match task {
            Task::Text(text) => {
                result.push_str(text);
                result.push(' ');
            }
            Task::Node(id) => match &nodes[id] {
                Node::Token(token) => {
                    result.push_str(&crate::eval::rewrite_helpers::token_sql(token));
                    result.push(' ');
                }
                Node::Sequence { children, enclosed } => {
                    if *enclosed {
                        result.push_str("( ");
                        tasks.push(Task::Text(")"));
                    }
                    tasks.extend(children.iter().rev().map(|id| Task::Node(*id)));
                }
                Node::Boolean { op, .. } => {
                    // Flatten only the same operator. This retains mixed
                    // AND/OR, NOT, casts and subquery boundaries and leaf order.
                    let mut pending = vec![id];
                    let mut flat = Vec::new();
                    while let Some(id) = pending.pop() {
                        if let Node::Boolean {
                            op: child_op,
                            left,
                            right,
                        } = &nodes[id]
                        {
                            if op == child_op {
                                pending.push(*right);
                                pending.push(*left);
                                continue;
                            }
                        }
                        flat.push(id);
                    }
                    let end = flat.len();
                    tasks.push(Task::Range {
                        ids: Rc::new(flat),
                        start: 0,
                        end,
                        op: *op,
                    });
                }
            },
            Task::Range {
                ids,
                start,
                end,
                op,
            } => {
                if end - start == 1 {
                    tasks.push(Task::Node(ids[start]));
                    continue;
                }
                let middle = start + (end - start) / 2;
                result.push_str("( ");
                tasks.push(Task::Text(")"));
                tasks.push(Task::Range {
                    ids: ids.clone(),
                    start: middle,
                    end,
                    op,
                });
                tasks.push(Task::Text(if op == Operator::Or { "OR" } else { "AND" }));
                tasks.push(Task::Range {
                    ids,
                    start,
                    end: middle,
                    op,
                });
            }
        }
    }
    Some(result)
}

/// Ordinary SQL keeps its exact parser input. Only an over-depth statement
/// takes the iterative path, and the unchanged parse-depth safety guard must
/// accept the resulting input before any recursive parser sees it.
pub(crate) fn prepare(sql: &str) -> Result<Cow<'_, str>> {
    match reject_deep_parse_nesting(sql) {
        Ok(()) => Ok(Cow::Borrowed(sql)),
        Err(original) => {
            if let Some(rewritten) = rewrite(sql) {
                if reject_deep_parse_nesting(&rewritten).is_ok() {
                    reject_oversized_sql(&rewritten)?;
                    return Ok(Cow::Owned(rewritten));
                }
            }
            Err(original)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_grammar_literals_and_quoted_names_survive_balancing() {
        for term in [
            "x BETWEEN 1 AND 3",
            "CASE WHEN x=1 OR x=2 THEN TRUE ELSE FALSE END",
            "EXISTS (SELECT 1 WHERE TRUE OR FALSE)",
            "'it''s (OR)'='it''s (OR)'",
            "$$AND ( OR )$$ = $$AND ( OR )$$",
            "\"a\"\"OR\"=1",
            "POSITION('x' IN 'xyz')=1",
            "x=1 /* OR ignored */",
            "x=1 -- OR ignored\n",
        ] {
            let sql = format!("SELECT (({term}) OR (FALSE))");
            let rewritten = rewrite(&sql).unwrap();
            let before = Parser::parse_sql(&PostgreSqlDialect {}, &sql).unwrap();
            let after = Parser::parse_sql(&PostgreSqlDialect {}, &rewritten).unwrap();
            assert_eq!(
                before[0].to_string(),
                after[0].to_string(),
                "{sql}\n{rewritten}"
            );
        }
    }
}
