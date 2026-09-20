//! Lossy substring candidates over the existing transactional inverted index.
//! The full LIKE/ILIKE predicate always rechecks candidate records.
use crate::*;

const UNICODE_CANDIDATE: &str = "trgm:unicode";
pub(crate) const TRIGRAM_PROJECTION_PREFIX: &str = "$bicdb_trigram_";

pub(crate) fn trigram_index_terms(value: &SqlValue) -> Result<Vec<String>> {
    let SqlValue::String(text) = value else {
        return if matches!(value, SqlValue::Null) {
            Ok(Vec::new())
        } else {
            Err(SqlError::InvalidSql("trigram index requires text".into()))
        };
    };
    // Keep every non-ASCII value as a candidate. Unicode case folding in the
    // predicate can equate non-ASCII characters with ASCII (e.g. Kelvin sign).
    // Lowercasing alone is not a sound basis for excluding those records.
    if !text.is_ascii() {
        return Ok(vec![UNICODE_CANDIDATE.into()]);
    }
    Ok(text
        .to_ascii_lowercase()
        .as_bytes()
        .windows(3)
        .map(|bytes| format!("trgm:{}", std::str::from_utf8(bytes).unwrap()))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}

fn pattern_trigram(pattern: &str) -> Option<String> {
    if !pattern.is_ascii() {
        return None;
    }
    let mut literal = String::new();
    let mut candidate = None;
    let mut chars = pattern.chars();
    while let Some(ch) = chars.next() {
        match ch {
            '\\' => literal.push(chars.next()?),
            '%' | '_' => literal.clear(),
            ch => literal.push(ch),
        }
        if literal.len() == 3 && candidate.is_none() {
            candidate = Some(format!("trgm:{}", literal.to_ascii_lowercase()));
        }
    }
    candidate
}

impl<'db> SqlEngine<'db> {
    pub(crate) fn trigram_index_candidate(
        &self,
        table: &str,
        alias: &str,
        schema: &TableSchema,
        selection: &Expr,
    ) -> Result<Option<(String, JsonbIndexCandidate)>> {
        if !schema.indexes.iter().any(|index| {
            index
                .operator_classes
                .iter()
                .any(|class| class.rsplit('.').next() == Some("gin_trgm_ops"))
        }) {
            return Ok(None);
        }
        for term in and_terms(selection) {
            let (expr, pattern) = match term {
                Expr::Like {
                    negated: false,
                    any: false,
                    expr,
                    pattern,
                    escape_char: None,
                }
                | Expr::ILike {
                    negated: false,
                    any: false,
                    expr,
                    pattern,
                    escape_char: None,
                } => (expr, pattern),
                _ => continue,
            };
            if !self.expr_references_table(expr, table, alias, Some(schema))?
                || !self.expr_is_bound_without_table_row(pattern)?
            {
                continue;
            }
            let normalized_expr = match expr.as_ref() {
                Expr::CompoundIdentifier(parts)
                    if parts.len() == 2
                        && (parts[0].value.eq_ignore_ascii_case(alias)
                            || parts[0].value.eq_ignore_ascii_case(table)) =>
                {
                    Expr::Identifier(parts[1].clone())
                }
                _ => expr.as_ref().clone(),
            };
            let Some(index) = schema.indexes.iter().find(|index| {
                !index.metadata_only
                    && index.access_method == "gin"
                    && index
                        .operator_classes
                        .iter()
                        .any(|class| class.rsplit('.').next() == Some("gin_trgm_ops"))
                    && crate::engine::full_text_index_expression_matches(
                        index
                            .source_expressions
                            .first()
                            .map(String::as_str)
                            .unwrap_or(&index.expression),
                        &normalized_expr,
                    )
            }) else {
                continue;
            };
            if !sql_index_definitions_for_collection(self.db_ref(), table)
                .iter()
                .any(|definition| {
                    definition.name == index.name && definition.kind == IndexKind::Array
                })
            {
                continue;
            }
            let SqlValue::String(pattern) = self.eval_dynamic_bound_expr(pattern)? else {
                continue;
            };
            let Some(term) = pattern_trigram(&pattern) else {
                continue;
            };
            // A single literal trigram is a necessary condition. Reusing the
            // OR posting lookup also retains all Unicode records for recheck.
            return Ok(Some((
                index.name.clone(),
                JsonbIndexCandidate::Any(vec![term, UNICODE_CANDIDATE.into()]),
            )));
        }
        Ok(None)
    }
}
