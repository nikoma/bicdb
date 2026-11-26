//! SQL regular expressions: `x [NOT] SIMILAR TO pattern [ESCAPE e]` and
//! `substring(x SIMILAR pattern ESCAPE e)`.
//!
//! PostgreSQL implements `SIMILAR TO` by rewriting it to a POSIX regular
//! expression match, `x ~ similar_to_escape(pattern [, e])`, and so does
//! BicDB: the rewrite happens once after parsing, so every evaluator sees an
//! ordinary `~` / `!~` with the `similar_to_escape` builtin (which is also
//! callable directly, as in PostgreSQL, and is what a view definition shows).

use crate::{Result, SqlError};
use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, Ident, ObjectName, Statement, VisitMut, VisitorMut,
};
use std::ops::ControlFlow;

/// PostgreSQL's `similar_to_escape(pattern, escape)`: translate a SQL
/// regular expression into the equivalent anchored POSIX one. `escape` None
/// is the default backslash; an empty escape disables escaping.
pub(crate) fn similar_to_escape(pattern: &str, escape: Option<&str>) -> Result<String> {
    let escape = match escape {
        None => Some('\\'),
        Some("") => None,
        Some(text) => {
            let mut chars = text.chars();
            match (chars.next(), chars.next()) {
                (Some(ch), None) => Some(ch),
                _ => {
                    return Err(SqlError::data_exception_public(
                        "22025",
                        "invalid escape string",
                        None,
                    ));
                }
            }
        }
    };
    let mut result = String::with_capacity(pattern.len() + 8);
    result.push_str("^(?:");
    let mut after_escape = false;
    let mut in_class = false;
    // Characters seen since the opening `[` (a `^` right after it does not
    // count): a `]` in the first position is a literal member.
    let mut class_position = 0usize;
    let mut quotes = 0usize;
    // Inside a bracket expression, a `[:class:]`, `[.coll.]` or `[=equiv=]`
    // element: its delimiter, so the `]` closing it does not end the
    // bracket expression.
    let mut element: Option<char> = None;
    let chars = pattern.chars().collect::<Vec<_>>();
    for (index, &ch) in chars.iter().enumerate() {
        if let Some(delimiter) = element {
            result.push(ch);
            if ch == ']' && index > 0 && chars[index - 1] == delimiter {
                element = None;
            }
            continue;
        }
        if after_escape {
            if ch == '"' && !in_class {
                match quotes {
                    0 => result.push_str("){1,1}?("),
                    1 => result.push_str("){1,1}(?:"),
                    _ => {
                        return Err(SqlError::data_exception_public(
                            "2201B",
                            "SQL regular expression may not contain more than two escape-double-quote separators",
                            None,
                        ));
                    }
                }
                quotes += 1;
            } else {
                result.push('\\');
                result.push(ch);
            }
            after_escape = false;
        } else if Some(ch) == escape {
            after_escape = true;
        } else if in_class {
            if ch == '\\' {
                result.push('\\');
            }
            result.push(ch);
            if ch == '[' && matches!(chars.get(index + 1), Some(':' | '.' | '=')) {
                element = chars.get(index + 1).copied();
                class_position += 1;
                continue;
            }
            if ch == ']' && class_position > 0 {
                in_class = false;
            } else if !(ch == '^' && class_position == 0) {
                class_position += 1;
            }
        } else {
            match ch {
                '[' => {
                    result.push('[');
                    in_class = true;
                    class_position = 0;
                }
                '%' => result.push_str(".*"),
                '_' => result.push('.'),
                '(' => result.push_str("(?:"),
                '\\' | '.' | '^' | '$' => {
                    result.push('\\');
                    result.push(ch);
                }
                other => result.push(other),
            }
        }
    }
    result.push_str(")$");
    Ok(result)
}

/// The builtin `substring(x SIMILAR p ESCAPE e)` / `substring(x FROM p FOR
/// e)` runs as.
pub(crate) const SIMILAR_SUBSTRING_FUNCTION: &str = "bicdb_similar_substring";

/// PostgreSQL's SQL regular-expression substring: the part of `text` between
/// the pattern's two escape-double-quote separators (the whole text when it
/// has none), or NULL when the pattern does not match the whole text.
pub(crate) fn similar_substring(text: &str, pattern: &str, escape: &str) -> Result<Option<String>> {
    let translated = similar_to_escape(pattern, Some(escape))?;
    let regex = regex::Regex::new(&format!("(?s){translated}")).map_err(|error| {
        SqlError::data_exception_public(
            "2201B",
            format!("invalid regular expression: {error}"),
            None,
        )
    })?;
    Ok(regex.captures(text).map(|captures| {
        captures
            .get(1)
            .or_else(|| captures.get(0))
            .map(|found| found.as_str().to_string())
            .unwrap_or_default()
    }))
}

fn function_call(name: &str, args: Vec<Expr>) -> Expr {
    Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new(name)]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: args
                .into_iter()
                .map(|arg| FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)))
                .collect(),
            clauses: Vec::new(),
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: Vec::new(),
    })
}

struct SimilarToRewriter;

impl VisitorMut for SimilarToRewriter {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        // substring(x FROM pattern FOR escape) with a text escape is the SQL
        // regular-expression form: PostgreSQL runs it as the POSIX
        // substring of the translated pattern (whose escape-double-quote
        // separators become the one capturing group).
        if let Expr::Substring {
            expr: target,
            substring_from: Some(pattern),
            substring_for,
            special: false,
            ..
        } = expr
        {
            if substring_for.as_deref().is_some_and(is_text_expr) {
                let escape = substring_for.take().expect("checked above");
                let take = |slot: &mut Box<Expr>| {
                    std::mem::replace(slot.as_mut(), Expr::value(sqlparser::ast::Value::Null))
                };
                let args = vec![take(target), take(pattern), *escape];
                *expr = function_call(SIMILAR_SUBSTRING_FUNCTION, args);
                return ControlFlow::Continue(());
            }
        }
        if let Some(expanded) = expand_between_symmetric(expr) {
            *expr = expanded;
            return ControlFlow::Continue(());
        }
        if let Expr::SimilarTo {
            negated,
            expr: target,
            pattern,
            escape_char,
        } = expr
        {
            let mut args = vec![std::mem::replace(
                pattern.as_mut(),
                Expr::value(sqlparser::ast::Value::Null),
            )];
            if let Some(escape) = escape_char.take() {
                args.push(Expr::Value(escape));
            }
            *expr = Expr::BinaryOp {
                left: Box::new(Expr::Nested(Box::new(std::mem::replace(
                    target.as_mut(),
                    Expr::value(sqlparser::ast::Value::Null),
                )))),
                op: if *negated {
                    BinaryOperator::PGRegexNotMatch
                } else {
                    BinaryOperator::PGRegexMatch
                },
                right: Box::new(function_call("similar_to_escape", args)),
            };
        }
        ControlFlow::Continue(())
    }
}

/// PostgreSQL's `transformAExprBetween` for `x [NOT] BETWEEN SYMMETRIC a AND
/// b`: `(x BETWEEN a AND b) OR (x BETWEEN b AND a)`, negated as a whole for
/// NOT (the operand is evaluated twice, as in PostgreSQL).
fn expand_between_symmetric(expr: &Expr) -> Option<Expr> {
    let Expr::Between {
        expr: target,
        negated,
        low,
        high,
    } = expr
    else {
        return None;
    };
    let Expr::Function(function) = low.as_ref() else {
        return None;
    };
    if !function
        .name
        .to_string()
        .eq_ignore_ascii_case(crate::parse_compat_postfix::BETWEEN_SYMMETRIC_MARKER)
    {
        return None;
    }
    let FunctionArguments::List(list) = &function.args else {
        return None;
    };
    let [FunctionArg::Unnamed(FunctionArgExpr::Expr(low))] = list.args.as_slice() else {
        return None;
    };
    let between = |low: &Expr, high: &Expr| {
        Expr::Nested(Box::new(Expr::Between {
            expr: target.clone(),
            negated: false,
            low: Box::new(low.clone()),
            high: high.clone().into(),
        }))
    };
    let either = Expr::Nested(Box::new(Expr::BinaryOp {
        left: Box::new(between(low, high)),
        op: BinaryOperator::Or,
        right: Box::new(between(high, low)),
    }));
    Some(if *negated {
        Expr::Nested(Box::new(Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Not,
            expr: Box::new(either),
        }))
    } else {
        either
    })
}

/// A string literal, or an expression cast to a character type.
fn is_text_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Value(value) => matches!(
            value.value,
            sqlparser::ast::Value::SingleQuotedString(_)
                | sqlparser::ast::Value::EscapedStringLiteral(_)
                | sqlparser::ast::Value::DollarQuotedString(_)
        ),
        Expr::Nested(inner) => is_text_expr(inner),
        Expr::Cast { data_type, .. } => matches!(
            data_type,
            sqlparser::ast::DataType::Text
                | sqlparser::ast::DataType::Varchar(_)
                | sqlparser::ast::DataType::CharacterVarying(_)
                | sqlparser::ast::DataType::Char(_)
                | sqlparser::ast::DataType::Character(_)
        ),
        _ => false,
    }
}

/// Rewrite every `SIMILAR TO` of `statements` into its regular-expression form.
pub(crate) fn rewrite_similar_to(statements: &mut [Statement]) {
    for statement in statements {
        let _ = statement.visit(&mut SimilarToRewriter);
    }
}

/// `substring(x SIMILAR pattern ESCAPE e)` is the SQL-standard spelling of
/// `substring(x FROM pattern FOR e)`; sqlparser knows only the latter.
pub(crate) fn rewrite_substring_similar(sql: &str) -> Option<String> {
    let lower = sql.to_ascii_lowercase();
    if !lower.contains("similar") || !lower.contains("substring") {
        return None;
    }
    let bytes = sql.as_bytes();
    let mut edits = Vec::<(usize, usize, &'static str)>::new();
    // Open parentheses: whether each one is a `substring(` call.
    let mut parens = Vec::<bool>::new();
    let mut index = 0usize;
    let word_at = |index: usize, word: &str| {
        lower[index..].starts_with(word)
            && (index == 0 || !is_word_byte(bytes[index - 1]))
            && bytes
                .get(index + word.len())
                .is_none_or(|byte| !is_word_byte(*byte))
    };
    while index < bytes.len() {
        match bytes[index] {
            b'\'' | b'"' => {
                let quote = bytes[index];
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == quote {
                        if bytes.get(index + 1) == Some(&quote) {
                            index += 2;
                            continue;
                        }
                        break;
                    }
                    index += 1;
                }
            }
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/')
                {
                    index += 1;
                }
                index += 1;
            }
            b'$' => {
                if let Some(delimiter) = crate::dollar_quote_delimiter(&sql[index..]) {
                    let body = index + delimiter.len();
                    match sql[body..].find(&delimiter) {
                        Some(end) => index = body + end + delimiter.len() - 1,
                        None => return None,
                    }
                }
            }
            b'(' => {
                let mut end = index;
                while end > 0 && bytes[end - 1].is_ascii_whitespace() {
                    end -= 1;
                }
                let is_substring = end >= "substring".len()
                    && lower[..end].ends_with("substring")
                    && (end == "substring".len()
                        || !is_word_byte(bytes[end - "substring".len() - 1]));
                parens.push(is_substring);
            }
            b')' => {
                parens.pop();
            }
            _ if parens.last() == Some(&true) && word_at(index, "similar") => {
                edits.push((index, index + "similar".len(), "FROM"));
                index += "similar".len();
                continue;
            }
            _ if parens.last() == Some(&true)
                && word_at(index, "escape")
                && edits.last().is_some_and(|(_, _, word)| *word == "FROM") =>
            {
                edits.push((index, index + "escape".len(), "FOR"));
                index += "escape".len();
                continue;
            }
            _ => {}
        }
        index += 1;
    }
    if edits.is_empty() {
        return None;
    }
    let mut output = sql.to_string();
    for (start, end, replacement) in edits.into_iter().rev() {
        output.replace_range(start..end, replacement);
    }
    Some(output)
}

/// `ORDER BY expr USING <` / `USING >` is ascending / descending order by
/// the type's ordering operator; sqlparser knows only ASC/DESC. Other
/// operators are left for the parser to refuse.
pub(crate) fn rewrite_order_by_using(sql: &str) -> Option<String> {
    let lower = sql.to_ascii_lowercase();
    if !lower.contains("using") || !lower.contains("order") {
        return None;
    }
    let bytes = sql.as_bytes();
    let mut output = String::with_capacity(sql.len());
    let mut copied = 0usize;
    let mut index = 0usize;
    let mut in_order_by = false;
    let mut changed = false;
    while index < bytes.len() {
        match bytes[index] {
            b'\'' | b'"' => {
                let quote = bytes[index];
                index += 1;
                while index < bytes.len() && bytes[index] != quote {
                    index += 1;
                }
                index += 1;
                continue;
            }
            b'$' => {
                if let Some(delimiter) = crate::dollar_quote_delimiter(&sql[index..]) {
                    let body = index + delimiter.len();
                    match sql[body..].find(&delimiter) {
                        Some(end) => index = body + end + delimiter.len(),
                        None => return None,
                    }
                    continue;
                }
            }
            b'(' | b')' | b';' => in_order_by = false,
            byte if is_word_byte(byte) && (index == 0 || !is_word_byte(bytes[index - 1])) => {
                let start = index;
                while index < bytes.len() && is_word_byte(bytes[index]) {
                    index += 1;
                }
                let word = &lower[start..index];
                match word {
                    "order" => {
                        let rest = lower[index..].trim_start();
                        in_order_by = rest.starts_with("by")
                            && !rest[2..].starts_with(|ch: char| ch.is_alphanumeric() || ch == '_');
                    }
                    "limit" | "offset" | "fetch" | "for" | "union" | "except" | "intersect" => {
                        in_order_by = false
                    }
                    "using" if in_order_by => {
                        let after = sql[index..].trim_start();
                        let gap = sql[index..].len() - after.len();
                        let (op_len, keyword) =
                            if after.starts_with("<=") || after.starts_with(">=") {
                                (0, "")
                            } else if after.starts_with('<') {
                                (1, "ASC")
                            } else if after.starts_with('>') {
                                (1, "DESC")
                            } else {
                                (0, "")
                            };
                        if op_len == 1 {
                            output.push_str(&sql[copied..start]);
                            output.push_str(keyword);
                            index += gap + op_len;
                            copied = index;
                            changed = true;
                        }
                    }
                    _ => {}
                }
                continue;
            }
            _ => {}
        }
        index += 1;
    }
    if !changed {
        return None;
    }
    output.push_str(&sql[copied..]);
    Some(output)
}

fn is_word_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$' || byte >= 0x80
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn translates_like_postgres() {
        assert_eq!(similar_to_escape("a%", None).unwrap(), "^(?:a.*)$");
        assert_eq!(similar_to_escape("a_c", None).unwrap(), "^(?:a.c)$");
        assert_eq!(similar_to_escape("(a|b)*", None).unwrap(), "^(?:(?:a|b)*)$");
        assert_eq!(similar_to_escape("a.b$", None).unwrap(), "^(?:a\\.b\\$)$");
        assert_eq!(similar_to_escape("a\\%", None).unwrap(), "^(?:a\\%)$");
        assert_eq!(similar_to_escape("a#%", Some("#")).unwrap(), "^(?:a\\%)$");
        assert_eq!(similar_to_escape("a\\b", Some("")).unwrap(), "^(?:a\\\\b)$");
        assert_eq!(
            similar_to_escape("%#\"o_b#\"%", Some("#")).unwrap(),
            "^(?:.*){1,1}?(o.b){1,1}(?:.*)$"
        );
        assert_eq!(similar_to_escape("[%_]x", None).unwrap(), "^(?:[%_]x)$");
        assert_eq!(similar_to_escape("[]a]", None).unwrap(), "^(?:[]a])$");
        // A `[:class:]` element does not end the bracket expression.
        assert_eq!(
            similar_to_escape("_[_[:alpha:]_]_", None).unwrap(),
            "^(?:.[_[:alpha:]_].)$"
        );
        assert_eq!(
            similar_to_escape("%[%[:alnum:]%]%", None).unwrap(),
            "^(?:.*[%[:alnum:]%].*)$"
        );
        assert_eq!(
            similar_to_escape("a", Some("ab")).unwrap_err().sqlstate(),
            "22025"
        );
        assert_eq!(
            similar_to_escape("#\"a#\"b#\"c", Some("#"))
                .unwrap_err()
                .sqlstate(),
            "2201B"
        );
    }

    #[test]
    fn substring_similar_becomes_from_for() {
        assert_eq!(
            rewrite_substring_similar(
                "SELECT substring('foobar' SIMILAR '%#\"o_b#\"%' ESCAPE '#'), 'similar'"
            )
            .unwrap(),
            "SELECT substring('foobar' FROM '%#\"o_b#\"%' FOR '#'), 'similar'"
        );
        assert_eq!(
            similar_substring("foobar", "%#\"o_b#\"%", "#")
                .unwrap()
                .as_deref(),
            Some("oob")
        );
        assert_eq!(
            similar_substring("foobar", "#\"f%#\"", "#")
                .unwrap()
                .as_deref(),
            Some("foobar")
        );
        assert_eq!(similar_substring("foobar", "x%", "#").unwrap(), None);
        assert!(rewrite_substring_similar("SELECT 'a' SIMILAR TO 'a'").is_none());
        assert!(
            rewrite_substring_similar("SELECT substring(x, 1), x SIMILAR TO 'a' FROM t").is_none()
        );
    }

    #[test]
    fn order_by_using_becomes_direction() {
        assert_eq!(
            rewrite_order_by_using("SELECT x FROM t ORDER BY x USING >, y USING < LIMIT 2")
                .unwrap(),
            "SELECT x FROM t ORDER BY x DESC, y ASC LIMIT 2"
        );
        assert!(rewrite_order_by_using("DELETE FROM t USING u WHERE t.a = u.a").is_none());
    }

    #[test]
    fn similar_to_becomes_regex_match() {
        let mut statements = crate::parse_statements(
            "SELECT 'abc' SIMILAR TO 'a%', 'abc' NOT SIMILAR TO 'b#%' ESCAPE '#'",
        )
        .unwrap();
        rewrite_similar_to(&mut statements);
        assert_eq!(
            statements[0].to_string(),
            "SELECT ('abc') ~ similar_to_escape('a%'), ('abc') !~ similar_to_escape('b#%', '#')"
        );
    }
}
