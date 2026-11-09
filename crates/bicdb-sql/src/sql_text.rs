//! SQL text for parsed statements and expressions that reads back as the
//! same tree.
//!
//! sqlparser's `Display` accepts both escaped and decoded string values, and
//! treats a quote that is already doubled (or follows a backslash) as escaped.
//! Our parser decodes values, so a literal whose value holds two adjacent
//! quotes or a backslash before a quote would be written back wrong. Every
//! quote is doubled on a rendering-only copy first, which `Display` then
//! writes verbatim. Text that is stored in the catalog, parsed again or
//! executed must be produced here, never by `to_string()`.
use serde::{de::DeserializeOwned, Serialize};
use sqlparser::ast::{Value, ValueWithSpan, VisitMut, VisitorMut};
use std::ops::ControlFlow;

struct EscapeValues;

impl VisitorMut for EscapeValues {
    type Break = ();

    fn pre_visit_value(&mut self, value: &mut ValueWithSpan) -> ControlFlow<()> {
        match &mut value.value {
            Value::SingleQuotedString(text) | Value::NationalStringLiteral(text) => {
                *text = text.replace('\'', "''");
            }
            Value::DoubleQuotedString(text) => *text = text.replace('"', "\"\""),
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// `node`'s `Display` text where that text identifies it, else `sql_text`:
/// for keys. Only text holding `''` or `\'` can stand for two trees, so the
/// common case keeps the cheap rendering.
pub fn sql_key<T: Clone + VisitMut + std::fmt::Display + Serialize + DeserializeOwned>(
    node: &T,
) -> String {
    let text = node.to_string();
    if text.contains("''")
        || text.contains("\\'")
        || text.contains("\"\"")
        || text.contains("\\\"")
        || text.contains("``")
        || text.contains("\\`")
    {
        sql_text(node)
    } else {
        text
    }
}

/// SQL text for `node` that parses back to the same tree.
pub fn sql_text<T: Clone + VisitMut + std::fmt::Display + Serialize + DeserializeOwned>(
    node: &T,
) -> String {
    let mut copy = node.clone();
    let _ = copy.visit(&mut EscapeValues);
    let text = copy.to_string();
    if !text.contains("\"\"")
        && !text.contains("\\\"")
        && !text.contains("``")
        && !text.contains("\\`")
    {
        return text;
    }
    // The upstream visitor has no Ident callback. Its serde AST preserves every
    // identifier position (including DDL/type names and aliases), allowing a
    // rendering-only fix without maintaining a second partial AST visitor.
    // Ordinary identifiers keep the cheap path above.
    fn escape_identifiers(node: &mut serde_json::Value) {
        match node {
            serde_json::Value::Object(fields) => {
                let quote = fields
                    .get("quote_style")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.chars().next());
                if let (Some(quote @ ('"' | '\'' | '`')), Some(serde_json::Value::String(value))) =
                    (quote, fields.get_mut("value"))
                {
                    *value = value.replace(quote, &quote.to_string().repeat(2));
                }
                for value in fields.values_mut() {
                    escape_identifiers(value);
                }
            }
            serde_json::Value::Array(values) => {
                for value in values {
                    escape_identifiers(value);
                }
            }
            _ => {}
        }
    }
    let mut tree = serde_json::to_value(&copy).expect("SQL AST is serializable");
    escape_identifiers(&mut tree);
    let escaped: T = serde_json::from_value(tree).expect("rendering preserves the SQL AST shape");
    escaped.to_string()
}
