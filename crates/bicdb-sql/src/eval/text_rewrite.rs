//! Split out of the parent module to keep files digestible; behavior
//! unchanged. Items are re-exported from the parent via `pub(crate) use`.
use super::*;
#[allow(unused_imports)]
use crate::*;

pub(crate) fn rewrite_is_json_predicates(input: &str) -> Option<String> {
    let tokens = json_predicate_tokens(input);
    let mut replacements = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        if token.word.as_deref() != Some("is") {
            continue;
        }
        let mut cursor = index + 1;
        let negated = tokens
            .get(cursor)
            .is_some_and(|next| next.depth == token.depth && next.word.as_deref() == Some("not"));
        if negated {
            cursor += 1;
        }
        if !tokens
            .get(cursor)
            .is_some_and(|next| next.depth == token.depth && next.word.as_deref() == Some("json"))
        {
            continue;
        }
        cursor += 1;
        let kind = tokens
            .get(cursor)
            .filter(|next| {
                next.depth == token.depth
                    && matches!(
                        next.word.as_deref(),
                        Some("value" | "scalar" | "array" | "object")
                    )
            })
            .and_then(|next| next.word.clone())
            .unwrap_or_else(|| "value".to_string());
        if tokens.get(cursor).is_some_and(|next| {
            next.depth == token.depth
                && matches!(
                    next.word.as_deref(),
                    Some("value" | "scalar" | "array" | "object")
                )
        }) {
            cursor += 1;
        }
        let mut unique = false;
        if tokens.get(cursor).is_some_and(|next| {
            next.depth == token.depth && matches!(next.word.as_deref(), Some("with" | "without"))
        }) && tokens
            .get(cursor + 1)
            .is_some_and(|next| next.depth == token.depth && next.word.as_deref() == Some("unique"))
        {
            unique = tokens[cursor].word.as_deref() == Some("with");
            cursor += 2;
            if tokens.get(cursor).is_some_and(|next| {
                next.depth == token.depth && next.word.as_deref() == Some("keys")
            }) {
                cursor += 1;
            }
        }
        let end = tokens[cursor - 1].end;
        let start = json_predicate_left_start(&tokens, index, token.depth);
        let left = input[start..token.start].trim();
        if left.is_empty() {
            continue;
        }
        let call = format!(
            " bicdb_is_json(({left}), '{kind}', {})",
            if unique { "true" } else { "false" }
        );
        replacements.push((
            start,
            end,
            if negated {
                format!("NOT ({call})")
            } else {
                call
            },
        ));
    }
    if replacements.is_empty() {
        return None;
    }
    let mut output = input.to_string();
    for (start, end, replacement) in replacements.into_iter().rev() {
        output.replace_range(start..end, &replacement);
    }
    Some(output)
}

pub(crate) fn json_predicate_left_start(
    tokens: &[JsonPredicateToken],
    is_index: usize,
    depth: usize,
) -> usize {
    const BOUNDARIES: &[&str] = &[
        "select",
        "where",
        "having",
        "on",
        "when",
        "then",
        "else",
        "and",
        "or",
        "returning",
        "values",
        "set",
    ];
    for token in tokens[..is_index].iter().rev() {
        if token.depth == depth
            && (matches!(token.symbol, Some(b',' | b';'))
                || token
                    .word
                    .as_deref()
                    .is_some_and(|word| BOUNDARIES.contains(&word)))
        {
            return token.end;
        }
        if depth > 0 && token.depth + 1 == depth && token.symbol == Some(b'(') {
            return token.end;
        }
    }
    0
}

pub(crate) fn json_predicate_tokens(input: &str) -> Vec<JsonPredicateToken> {
    let bytes = input.as_bytes();
    let mut tokens = Vec::new();
    let mut index = 0;
    let mut depth = 0;
    while index < bytes.len() {
        if bytes[index].is_ascii_whitespace() {
            index += 1;
            continue;
        }
        if bytes[index] == b'-' && bytes.get(index + 1) == Some(&b'-') {
            index += 2;
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
            continue;
        }
        if bytes[index] == b'/' && bytes.get(index + 1) == Some(&b'*') {
            index += 2;
            while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/') {
                index += 1;
            }
            index = (index + 2).min(bytes.len());
            continue;
        }
        let start = index;
        if matches!(bytes[index], b'\'' | b'"') {
            let quote = bytes[index];
            index += 1;
            while index < bytes.len() {
                if bytes[index] == quote {
                    if bytes.get(index + 1) == Some(&quote) {
                        index += 2;
                    } else {
                        index += 1;
                        break;
                    }
                } else {
                    index += 1;
                }
            }
            tokens.push(JsonPredicateToken {
                start,
                end: index,
                depth,
                word: None,
                symbol: None,
            });
            continue;
        }
        if bytes[index].is_ascii_alphabetic() || bytes[index] == b'_' {
            index += 1;
            while bytes
                .get(index)
                .is_some_and(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$'))
            {
                index += 1;
            }
            tokens.push(JsonPredicateToken {
                start,
                end: index,
                depth,
                word: Some(input[start..index].to_ascii_lowercase()),
                symbol: None,
            });
            continue;
        }
        let symbol = bytes[index];
        if symbol == b')' {
            depth = depth.saturating_sub(1);
        }
        index += 1;
        tokens.push(JsonPredicateToken {
            start,
            end: index,
            depth,
            word: None,
            symbol: Some(symbol),
        });
        if symbol == b'(' {
            depth += 1;
        }
    }
    tokens
}

pub(crate) fn rewrite_json_unique_keys_clauses(input: &str) -> Option<String> {
    let tokens = json_predicate_tokens(input);
    let mut replacements = Vec::new();
    for (index, function) in tokens.iter().enumerate() {
        let Some(name) = function.word.as_deref() else {
            continue;
        };
        if !matches!(name, "json" | "json_object" | "json_objectagg") {
            continue;
        }
        let Some(open) = tokens
            .get(index + 1)
            .filter(|token| token.depth == function.depth && token.symbol == Some(b'('))
        else {
            continue;
        };
        let inner_depth = open.depth + 1;
        let Some(close_index) = tokens[index + 2..]
            .iter()
            .position(|token| token.depth == function.depth && token.symbol == Some(b')'))
            .map(|offset| index + 2 + offset)
        else {
            continue;
        };
        if name == "json_object"
            && !tokens[index + 2..close_index]
                .iter()
                .any(|token| token.depth == inner_depth && token.symbol == Some(b':'))
        {
            continue;
        }
        let mut cursor = index + 2;
        while cursor < close_index {
            let Some(mode) = tokens.get(cursor).filter(|token| {
                token.depth == inner_depth
                    && matches!(token.word.as_deref(), Some("with" | "without"))
            }) else {
                cursor += 1;
                continue;
            };
            if !tokens.get(cursor + 1).is_some_and(|token| {
                token.depth == inner_depth && token.word.as_deref() == Some("unique")
            }) {
                cursor += 1;
                continue;
            }
            let mut end_index = cursor + 1;
            if tokens.get(cursor + 2).is_some_and(|token| {
                token.depth == inner_depth && token.word.as_deref() == Some("keys")
            }) {
                end_index += 1;
            }
            replacements.push((mode.start, tokens[end_index].end, String::new()));
            if mode.word.as_deref() == Some("with") {
                replacements.push((
                    function.start,
                    function.end,
                    match name {
                        "json" => "json_unique",
                        "json_object" => "json_object_unique",
                        _ => "json_objectagg_unique",
                    }
                    .to_string(),
                ));
            }
            break;
        }
    }
    if replacements.is_empty() {
        return None;
    }
    replacements.sort_by_key(|(start, _, _)| *start);
    let mut output = input.to_string();
    for (start, end, replacement) in replacements.into_iter().rev() {
        output.replace_range(start..end, &replacement);
    }
    Some(output)
}

pub(crate) fn rewrite_xml_constructor_once(input: &str) -> Option<String> {
    let tokens = json_predicate_tokens(input);
    let (index, function) = tokens
        .iter()
        .enumerate()
        .filter(|(_, token)| {
            matches!(
                token.word.as_deref(),
                Some("xmlattributes" | "xmlforest" | "xmlelement" | "xmlpi")
            )
        })
        .max_by_key(|(_, token)| token.depth)?;
    let name = function.word.as_deref()?;
    let open = tokens
        .get(index + 1)
        .filter(|token| token.depth == function.depth && token.symbol == Some(b'('))?;
    let close_index = tokens[index + 2..]
        .iter()
        .position(|token| token.depth == function.depth && token.symbol == Some(b')'))
        .map(|offset| index + 2 + offset)?;
    let inner_depth = open.depth + 1;
    let replacement = if matches!(name, "xmlattributes" | "xmlforest") {
        let mut boundaries = vec![open.end];
        boundaries.extend(
            tokens[index + 2..close_index]
                .iter()
                .filter(|token| token.depth == inner_depth && token.symbol == Some(b','))
                .map(|token| token.end),
        );
        let mut ends = tokens[index + 2..close_index]
            .iter()
            .filter(|token| token.depth == inner_depth && token.symbol == Some(b','))
            .map(|token| token.start)
            .collect::<Vec<_>>();
        ends.push(tokens[close_index].start);
        let mut arguments = Vec::new();
        for (start, end) in boundaries.into_iter().zip(ends) {
            let as_token = tokens[index + 2..close_index]
                .iter()
                .filter(|token| {
                    token.start >= start && token.end <= end && token.depth == inner_depth
                })
                .rfind(|token| token.word.as_deref() == Some("as"))?;
            let expression = input[start..as_token.start].trim();
            let alias = input[as_token.end..end].trim().trim_matches('"');
            arguments.push(format!("'{alias}', ({expression})"));
        }
        format!("bicdb_{name}({})", arguments.join(", "))
    } else {
        let first = tokens.get(index + 2)?;
        if first.word.as_deref() != Some("name") {
            return None;
        }
        let identifier = tokens.get(index + 3)?;
        let xml_name = input[identifier.start..identifier.end]
            .trim()
            .trim_matches('"');
        let remainder = input[identifier.end..tokens[close_index].start].trim();
        let remainder = remainder.strip_prefix(',').unwrap_or(remainder).trim();
        let internal = if name == "xmlelement" {
            "bicdb_xmlelement"
        } else {
            "bicdb_xmlpi"
        };
        if remainder.is_empty() {
            format!("{internal}('{xml_name}')")
        } else {
            format!("{internal}('{xml_name}', {remainder})")
        }
    };
    let mut output = input.to_string();
    output.replace_range(function.start..tokens[close_index].end, &replacement);
    Some(output)
}

pub(crate) fn rewrite_json_array_query_constructors(input: &str) -> Option<String> {
    let tokens = json_predicate_tokens(input);
    let mut replacements = Vec::new();
    for (index, function) in tokens.iter().enumerate() {
        if function.word.as_deref() != Some("json_array") {
            continue;
        }
        let Some(open) = tokens
            .get(index + 1)
            .filter(|token| token.depth == function.depth && token.symbol == Some(b'('))
        else {
            continue;
        };
        let inner_depth = open.depth + 1;
        if !tokens.get(index + 2).is_some_and(|token| {
            token.depth == inner_depth && token.word.as_deref() == Some("select")
        }) {
            continue;
        }
        let Some(close_index) = tokens[index + 2..]
            .iter()
            .position(|token| token.depth == function.depth && token.symbol == Some(b')'))
            .map(|offset| index + 2 + offset)
        else {
            continue;
        };
        let returning_index = tokens[index + 2..close_index]
            .iter()
            .rposition(|token| {
                token.depth == inner_depth && token.word.as_deref() == Some("returning")
            })
            .map(|offset| index + 2 + offset);
        let query_end = returning_index
            .map(|index| tokens[index].start)
            .unwrap_or(tokens[close_index].start);
        let query = input[open.end..query_end].trim();
        if query.is_empty() {
            continue;
        }
        let returning = returning_index
            .map(|index| input[tokens[index].start..tokens[close_index].start].trim())
            .unwrap_or("");
        let aggregate_clause = if returning.is_empty() {
            String::new()
        } else {
            format!(" {returning}")
        };
        let returning_type = returning_index
            .and_then(|index| tokens.get(index + 1))
            .and_then(|token| token.word.as_deref())
            .map(|pg_type| match pg_type {
                "character" => "bpchar",
                other => other,
            })
            .unwrap_or("json");
        replacements.push((
            function.start,
            tokens[close_index].end,
            format!(
                "bicdb_json_array_query_{returning_type}((SELECT \
                 json_arrayagg(__bicdb_json_array_value{aggregate_clause}) \
                 FROM ({query}) AS __bicdb_json_array_source(__bicdb_json_array_value)))"
            ),
        ));
    }
    if replacements.is_empty() {
        return None;
    }
    replacements.sort_by_key(|(start, _, _)| *start);
    let mut output = input.to_string();
    for (start, end, replacement) in replacements.into_iter().rev() {
        output.replace_range(start..end, &replacement);
    }
    Some(output)
}

pub(crate) fn rewrite_json_format_clauses(input: &str) -> Option<String> {
    let tokens = json_predicate_tokens(input);
    let mut replacements = Vec::new();
    for (index, format_token) in tokens.iter().enumerate() {
        if format_token.word.as_deref() != Some("format")
            || !tokens.get(index + 1).is_some_and(|token| {
                token.depth == format_token.depth && token.word.as_deref() == Some("json")
            })
        {
            continue;
        }
        let mut end_index = index + 1;
        let encoded = tokens.get(index + 2).is_some_and(|token| {
            token.depth == format_token.depth && token.word.as_deref() == Some("encoding")
        }) && tokens.get(index + 3).is_some_and(|token| {
            token.depth == format_token.depth && token.word.as_deref() == Some("utf8")
        });
        if encoded {
            end_index = index + 3;
        }

        let open_index = tokens[..index]
            .iter()
            .rposition(|token| token.depth + 1 == format_token.depth && token.symbol == Some(b'('));
        let object_value_boundary = open_index.and_then(|open_index| {
            let function_name = open_index
                .checked_sub(1)
                .and_then(|index| tokens[index].word.as_deref());
            matches!(
                function_name,
                Some("json_objectagg" | "json_objectagg_unique")
            )
            .then(|| {
                tokens[open_index + 1..index]
                    .iter()
                    .find(|token| {
                        token.depth == format_token.depth && token.word.as_deref() == Some("value")
                    })
                    .map(|token| token.start)
            })
            .flatten()
        });
        let mut boundary = None;
        let mut returning = false;
        for token in tokens[..index].iter().rev() {
            if token.depth == format_token.depth && token.word.as_deref() == Some("returning") {
                returning = true;
            }
            if (token.depth == format_token.depth && matches!(token.symbol, Some(b',' | b':')))
                || (token.depth + 1 == format_token.depth && token.symbol == Some(b'('))
                || object_value_boundary == Some(token.start)
            {
                boundary = Some(token.end);
                break;
            }
        }
        if returning {
            if encoded {
                let returning_type = tokens[..index]
                    .iter()
                    .rev()
                    .find(|token| {
                        token.depth == format_token.depth
                            && token.word.as_deref() != Some("returning")
                    })
                    .and_then(|token| token.word.as_deref());
                if returning_type != Some("bytea") {
                    if let Some(open_index) = open_index {
                        if let Some(function_token) = open_index
                            .checked_sub(1)
                            .and_then(|index| tokens.get(index))
                        {
                            if let Some(function_name) = function_token.word.as_deref() {
                                replacements.push((
                                    function_token.start,
                                    function_token.end,
                                    format!("{function_name}_encoding_error"),
                                ));
                            }
                        }
                    }
                }
            }
            replacements.push((format_token.start, tokens[end_index].end, String::new()));
            continue;
        }
        let start = boundary.unwrap_or(0);
        let expression = input[start..format_token.start].trim();
        if expression.is_empty() {
            continue;
        }
        replacements.push((
            start,
            tokens[end_index].end,
            format!(
                " bicdb_json_format(({expression}), {})",
                if encoded { "true" } else { "false" }
            ),
        ));
    }
    if replacements.is_empty() {
        return None;
    }
    replacements.sort_by_key(|(start, _, _)| *start);
    let mut output = input.to_string();
    for (start, end, replacement) in replacements.into_iter().rev() {
        output.replace_range(start..end, &replacement);
    }
    Some(output)
}

/// sqlparser 0.62 tokenizes pgvector's `<+>` as separate comparison and
/// arithmetic tokens. `<^` has the same PostgreSQL custom-operator precedence,
/// so use it as an internal AST sentinel until sqlparser accepts `<+>`.
pub(crate) fn rewrite_pgvector_l1_operator(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    let mut output = String::with_capacity(input.len());
    let mut cursor = 0usize;
    let mut idx = 0usize;
    let mut changed = false;
    let mut single_quoted = false;
    let mut double_quoted = false;
    let mut line_comment = false;
    let mut block_comment = false;
    let mut dollar_quote: Option<String> = None;

    while idx < bytes.len() {
        if line_comment {
            if bytes[idx] == b'\n' {
                line_comment = false;
            }
            idx += 1;
            continue;
        }
        if block_comment {
            if bytes[idx] == b'*' && bytes.get(idx + 1) == Some(&b'/') {
                block_comment = false;
                idx += 2;
            } else {
                idx += 1;
            }
            continue;
        }
        if let Some(delimiter) = dollar_quote.as_ref() {
            if input[idx..].starts_with(delimiter) {
                idx += delimiter.len();
                dollar_quote = None;
            } else {
                idx += 1;
            }
            continue;
        }
        if single_quoted {
            if bytes[idx] == b'\'' {
                if bytes.get(idx + 1) == Some(&b'\'') {
                    idx += 2;
                } else {
                    single_quoted = false;
                    idx += 1;
                }
            } else {
                idx += 1;
            }
            continue;
        }
        if double_quoted {
            if bytes[idx] == b'"' {
                if bytes.get(idx + 1) == Some(&b'"') {
                    idx += 2;
                } else {
                    double_quoted = false;
                    idx += 1;
                }
            } else {
                idx += 1;
            }
            continue;
        }

        if bytes.get(idx..idx + 3) == Some(b"<+>") {
            output.push_str(&input[cursor..idx]);
            output.push_str("<^");
            idx += 3;
            cursor = idx;
            changed = true;
            continue;
        }

        match bytes[idx] {
            b'\'' => {
                single_quoted = true;
                idx += 1;
            }
            b'"' => {
                double_quoted = true;
                idx += 1;
            }
            b'-' if bytes.get(idx + 1) == Some(&b'-') => {
                line_comment = true;
                idx += 2;
            }
            b'/' if bytes.get(idx + 1) == Some(&b'*') => {
                block_comment = true;
                idx += 2;
            }
            b'$' => {
                if let Some(delimiter) = dollar_quote_delimiter(&input[idx..]) {
                    idx += delimiter.len();
                    dollar_quote = Some(delimiter);
                } else {
                    idx += 1;
                }
            }
            _ => idx += 1,
        }
    }

    if changed {
        output.push_str(&input[cursor..]);
        Some(output)
    } else {
        None
    }
}

pub(crate) fn rewrite_postgres_interval_precision_literals(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    let mut output = String::with_capacity(input.len());
    let mut cursor = 0_usize;
    let mut idx = 0_usize;
    let mut changed = false;
    let mut single_quoted = false;
    let mut double_quoted = false;
    let mut line_comment = false;
    let mut block_comment = false;
    let mut dollar_quote: Option<String> = None;

    while idx < bytes.len() {
        if line_comment {
            line_comment = bytes[idx] != b'\n';
            idx += 1;
            continue;
        }
        if block_comment {
            if bytes[idx] == b'*' && bytes.get(idx + 1) == Some(&b'/') {
                block_comment = false;
                idx += 2;
            } else {
                idx += 1;
            }
            continue;
        }
        if let Some(delimiter) = dollar_quote.as_ref() {
            if input[idx..].starts_with(delimiter) {
                idx += delimiter.len();
                dollar_quote = None;
            } else {
                idx += 1;
            }
            continue;
        }
        if single_quoted {
            if bytes[idx] == b'\'' {
                if bytes.get(idx + 1) == Some(&b'\'') {
                    idx += 2;
                } else {
                    single_quoted = false;
                    idx += 1;
                }
            } else {
                idx += 1;
            }
            continue;
        }
        if double_quoted {
            if bytes[idx] == b'"' {
                if bytes.get(idx + 1) == Some(&b'"') {
                    idx += 2;
                } else {
                    double_quoted = false;
                    idx += 1;
                }
            } else {
                idx += 1;
            }
            continue;
        }

        if starts_ascii_case_insensitive_at(bytes, idx, b"interval")
            && !idx
                .checked_sub(1)
                .and_then(|previous| bytes.get(previous))
                .is_some_and(|byte| is_sql_identifier_byte(*byte))
            && !bytes
                .get(idx + 8)
                .is_some_and(|byte| is_sql_identifier_byte(*byte))
        {
            let open = skip_ascii_whitespace(input, idx + 8);
            if bytes.get(open) == Some(&b'(') {
                let precision_start = skip_ascii_whitespace(input, open + 1);
                let mut precision_end = precision_start;
                while bytes
                    .get(precision_end)
                    .is_some_and(|byte| byte.is_ascii_digit())
                {
                    precision_end += 1;
                }
                let close = skip_ascii_whitespace(input, precision_end);
                let literal_start = skip_ascii_whitespace(input, close + 1);
                if precision_end > precision_start
                    && bytes.get(close) == Some(&b')')
                    && bytes.get(literal_start) == Some(&b'\'')
                {
                    let mut literal_end = literal_start + 1;
                    while literal_end < bytes.len() {
                        if bytes[literal_end] == b'\'' {
                            if bytes.get(literal_end + 1) == Some(&b'\'') {
                                literal_end += 2;
                                continue;
                            }
                            literal_end += 1;
                            output.push_str(&input[cursor..idx]);
                            output.push_str("CAST(");
                            output.push_str(&input[literal_start..literal_end]);
                            output.push_str(" AS INTERVAL(");
                            output.push_str(&input[precision_start..precision_end]);
                            output.push_str("))");
                            cursor = literal_end;
                            idx = literal_end;
                            changed = true;
                            break;
                        }
                        literal_end += 1;
                    }
                    if idx == literal_end {
                        continue;
                    }
                }
            }
        }

        match bytes[idx] {
            b'\'' => {
                single_quoted = true;
                idx += 1;
            }
            b'"' => {
                double_quoted = true;
                idx += 1;
            }
            b'-' if bytes.get(idx + 1) == Some(&b'-') => {
                line_comment = true;
                idx += 2;
            }
            b'/' if bytes.get(idx + 1) == Some(&b'*') => {
                block_comment = true;
                idx += 2;
            }
            b'$' => {
                if let Some(delimiter) = dollar_quote_delimiter(&input[idx..]) {
                    idx += delimiter.len();
                    dollar_quote = Some(delimiter);
                } else {
                    idx += 1;
                }
            }
            _ => idx += 1,
        }
    }

    if changed {
        output.push_str(&input[cursor..]);
        Some(output)
    } else {
        None
    }
}

pub(crate) fn rewrite_postgres_record_function_aliases(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    let mut output = String::with_capacity(input.len());
    let mut cursor = 0usize;
    let mut idx = 0usize;
    let mut changed = false;
    let mut single_quoted = false;
    let mut double_quoted = false;
    let mut line_comment = false;
    let mut block_comment = false;
    let mut dollar_quote: Option<String> = None;
    let mut paren_stack = Vec::new();
    let mut last_significant: Option<(usize, Option<usize>)> = None;

    while idx < bytes.len() {
        if line_comment {
            if bytes[idx] == b'\n' {
                line_comment = false;
            }
            idx += 1;
            continue;
        }
        if block_comment {
            if bytes[idx] == b'*' && bytes.get(idx + 1) == Some(&b'/') {
                block_comment = false;
                idx += 2;
            } else {
                idx += 1;
            }
            continue;
        }
        if let Some(delimiter) = dollar_quote.as_ref() {
            if input[idx..].starts_with(delimiter) {
                idx += delimiter.len();
                dollar_quote = None;
            } else {
                idx += 1;
            }
            continue;
        }
        if single_quoted {
            if bytes[idx] == b'\'' {
                if bytes.get(idx + 1) == Some(&b'\'') {
                    idx += 2;
                } else {
                    single_quoted = false;
                    idx += 1;
                }
            } else {
                idx += 1;
            }
            continue;
        }
        if double_quoted {
            if bytes[idx] == b'"' {
                double_quoted = false;
            }
            idx += 1;
            continue;
        }

        if starts_ascii_case_insensitive_at(bytes, idx, b"as")
            && !idx
                .checked_sub(1)
                .and_then(|prev| bytes.get(prev))
                .is_some_and(|byte| is_sql_identifier_byte(*byte))
            && !bytes
                .get(idx + 2)
                .is_some_and(|byte| is_sql_identifier_byte(*byte))
        {
            let after_as = skip_ascii_whitespace(input, idx + 2);
            let alias = (bytes.get(after_as) == Some(&b'(')
                && !paren_starts_query_body(input, after_as))
            .then(|| {
                last_significant
                    .and_then(|(last_idx, open_idx)| {
                        (bytes.get(last_idx) == Some(&b')'))
                            .then_some(open_idx)
                            .flatten()
                    })
                    .and_then(|open_idx| function_alias_before_paren(input, open_idx))
            })
            .flatten();
            if let Some(alias) = alias {
                output.push_str(&input[cursor..after_as]);
                output.push_str(&alias);
                output.push(' ');
                cursor = after_as;
                changed = true;
                idx = after_as;
                continue;
            }
        }

        match bytes[idx] {
            b'\'' => {
                single_quoted = true;
                last_significant = Some((idx, None));
                idx += 1;
            }
            b'"' => {
                double_quoted = true;
                last_significant = Some((idx, None));
                idx += 1;
            }
            b'-' if bytes.get(idx + 1) == Some(&b'-') => {
                line_comment = true;
                idx += 2;
            }
            b'/' if bytes.get(idx + 1) == Some(&b'*') => {
                block_comment = true;
                idx += 2;
            }
            b'$' => {
                if let Some(delimiter) = dollar_quote_delimiter(&input[idx..]) {
                    idx += delimiter.len();
                    dollar_quote = Some(delimiter);
                } else {
                    last_significant = Some((idx, None));
                    idx += 1;
                }
            }
            b'(' => {
                paren_stack.push(idx);
                last_significant = Some((idx, None));
                idx += 1;
            }
            b')' => {
                let open_idx = paren_stack.pop();
                last_significant = Some((idx, open_idx));
                idx += 1;
            }
            byte if byte.is_ascii_whitespace() => idx += 1,
            _ => {
                last_significant = Some((idx, None));
                idx += 1;
            }
        }
    }

    if changed {
        output.push_str(&input[cursor..]);
        Some(output)
    } else {
        None
    }
}

pub(crate) fn skip_ascii_whitespace(input: &str, mut idx: usize) -> usize {
    let bytes = input.as_bytes();
    while bytes
        .get(idx)
        .is_some_and(|byte| byte.is_ascii_whitespace())
    {
        idx += 1;
    }
    idx
}

pub(crate) fn paren_starts_query_body(input: &str, open_idx: usize) -> bool {
    let start = skip_ascii_whitespace(input, open_idx + 1);
    [
        "select", "with", "values", "table", "insert", "update", "delete",
    ]
    .into_iter()
    .any(|keyword| {
        input
            .get(start..start + keyword.len())
            .is_some_and(|candidate| candidate.eq_ignore_ascii_case(keyword))
            && input
                .as_bytes()
                .get(start + keyword.len())
                .is_none_or(|byte| !is_sql_identifier_byte(*byte))
    })
}

pub(crate) fn function_alias_before_paren(input: &str, open_idx: usize) -> Option<String> {
    let bytes = input.as_bytes();
    let mut end = open_idx;
    while end > 0 && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    let mut start = end;
    while start > 0 && is_sql_identifier_byte(bytes[start - 1]) {
        start -= 1;
    }
    if start == end {
        return None;
    }
    Some(input[start..end].to_string())
}

pub(crate) fn rewrite_postgres_operator_qualifiers(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    let mut output = String::with_capacity(input.len());
    let mut cursor = 0usize;
    let mut idx = 0usize;
    let mut changed = false;
    let mut single_quoted = false;
    let mut double_quoted = false;
    let mut line_comment = false;
    let mut block_comment = false;
    let mut dollar_quote: Option<String> = None;

    while idx < bytes.len() {
        if line_comment {
            if bytes[idx] == b'\n' {
                line_comment = false;
            }
            idx += 1;
            continue;
        }
        if block_comment {
            if bytes[idx] == b'*' && bytes.get(idx + 1) == Some(&b'/') {
                block_comment = false;
                idx += 2;
            } else {
                idx += 1;
            }
            continue;
        }
        if let Some(delimiter) = dollar_quote.as_ref() {
            if input[idx..].starts_with(delimiter) {
                idx += delimiter.len();
                dollar_quote = None;
            } else {
                idx += 1;
            }
            continue;
        }
        if single_quoted {
            if bytes[idx] == b'\'' {
                if bytes.get(idx + 1) == Some(&b'\'') {
                    idx += 2;
                } else {
                    single_quoted = false;
                    idx += 1;
                }
            } else {
                idx += 1;
            }
            continue;
        }
        if double_quoted {
            if bytes[idx] == b'"' {
                double_quoted = false;
            }
            idx += 1;
            continue;
        }

        if starts_ascii_case_insensitive_at(bytes, idx, b"operator(")
            && !idx
                .checked_sub(1)
                .and_then(|prev| bytes.get(prev))
                .is_some_and(|byte| is_sql_identifier_byte(*byte))
        {
            let content_start = idx + "operator(".len();
            if let Some(close_idx) = bytes[content_start..]
                .iter()
                .position(|byte| *byte == b')')
                .map(|relative| content_start + relative)
            {
                let content = &input[content_start..close_idx];
                if let Some(replacement) = postgres_operator_replacement(content) {
                    output.push_str(&input[cursor..idx]);
                    output.push_str(replacement);
                    idx = close_idx + 1;
                    cursor = idx;
                    changed = true;
                    continue;
                }
            }
        }

        match bytes[idx] {
            b'\'' => {
                single_quoted = true;
                idx += 1;
            }
            b'"' => {
                double_quoted = true;
                idx += 1;
            }
            b'-' if bytes.get(idx + 1) == Some(&b'-') => {
                line_comment = true;
                idx += 2;
            }
            b'/' if bytes.get(idx + 1) == Some(&b'*') => {
                block_comment = true;
                idx += 2;
            }
            b'$' => {
                if let Some(delimiter) = dollar_quote_delimiter(&input[idx..]) {
                    idx += delimiter.len();
                    dollar_quote = Some(delimiter);
                } else {
                    idx += 1;
                }
            }
            _ => idx += 1,
        }
    }

    if changed {
        output.push_str(&input[cursor..]);
        Some(output)
    } else {
        None
    }
}

/// Quote a schema-qualified function named `current_user` for sqlparser.
/// PostgreSQL accepts `schema.current_user()`, but sqlparser treats the final
/// component as the bare CURRENT_USER keyword and rejects its parentheses.
/// Quoting only for the parser preserves PostgreSQL's identifier and runtime
/// resolution semantics.
pub(crate) fn rewrite_qualified_current_user_calls(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    let mut output = String::with_capacity(input.len());
    let mut cursor = 0usize;
    let mut idx = 0usize;
    let mut changed = false;
    let mut single_quoted = false;
    let mut double_quoted = false;
    let mut line_comment = false;
    let mut block_comment = false;
    let mut dollar_quote: Option<String> = None;

    while idx < bytes.len() {
        if line_comment {
            if bytes[idx] == b'\n' {
                line_comment = false;
            }
            idx += 1;
            continue;
        }
        if block_comment {
            if bytes[idx] == b'*' && bytes.get(idx + 1) == Some(&b'/') {
                block_comment = false;
                idx += 2;
            } else {
                idx += 1;
            }
            continue;
        }
        if let Some(delimiter) = dollar_quote.as_ref() {
            if input[idx..].starts_with(delimiter) {
                idx += delimiter.len();
                dollar_quote = None;
            } else {
                idx += 1;
            }
            continue;
        }
        if single_quoted {
            if bytes[idx] == b'\'' {
                if bytes.get(idx + 1) == Some(&b'\'') {
                    idx += 2;
                } else {
                    single_quoted = false;
                    idx += 1;
                }
            } else {
                idx += 1;
            }
            continue;
        }
        if double_quoted {
            if bytes[idx] == b'"' {
                double_quoted = false;
            }
            idx += 1;
            continue;
        }

        if bytes.get(idx.wrapping_sub(1)) == Some(&b'.')
            && starts_ascii_case_insensitive_at(bytes, idx, b"current_user")
        {
            let name_end = idx + "current_user".len();
            let call_open = skip_ascii_whitespace(input, name_end);
            if bytes.get(call_open) == Some(&b'(') {
                output.push_str(&input[cursor..idx]);
                output.push_str("\"current_user\"");
                cursor = name_end;
                idx = name_end;
                changed = true;
                continue;
            }
        }

        match bytes[idx] {
            b'\'' => {
                single_quoted = true;
                idx += 1;
            }
            b'"' => {
                double_quoted = true;
                idx += 1;
            }
            b'-' if bytes.get(idx + 1) == Some(&b'-') => {
                line_comment = true;
                idx += 2;
            }
            b'/' if bytes.get(idx + 1) == Some(&b'*') => {
                block_comment = true;
                idx += 2;
            }
            b'$' => {
                if let Some(delimiter) = dollar_quote_delimiter(&input[idx..]) {
                    idx += delimiter.len();
                    dollar_quote = Some(delimiter);
                } else {
                    idx += 1;
                }
            }
            _ => idx += 1,
        }
    }

    if changed {
        output.push_str(&input[cursor..]);
        Some(output)
    } else {
        None
    }
}

pub(crate) fn starts_ascii_case_insensitive_at(input: &[u8], idx: usize, needle: &[u8]) -> bool {
    input
        .get(idx..idx + needle.len())
        .is_some_and(|candidate| candidate.eq_ignore_ascii_case(needle))
}

pub(crate) fn is_sql_identifier_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$')
}

pub(crate) fn postgres_operator_replacement(content: &str) -> Option<&'static str> {
    let normalized = content.trim();
    let (schema, operator) = normalized
        .rsplit_once('.')
        .map(|(schema, operator)| (Some(schema.trim()), operator.trim()))
        .unwrap_or((None, normalized));

    if schema.is_some_and(|schema| !schema.eq_ignore_ascii_case("pg_catalog")) {
        return None;
    }

    match operator {
        "=" => Some("="),
        ">" => Some(">"),
        "<" => Some("<"),
        ">=" | "=>" => Some(">="),
        "<=" | "=<" => Some("<="),
        "<>" | "!=" => Some("!="),
        "~" => Some("~"),
        "~*" => Some("~*"),
        "!~" => Some("!~"),
        "!~*" => Some("!~*"),
        "~~" => Some("~~"),
        "~~*" => Some("~~*"),
        "!~~" => Some("!~~"),
        "!~~*" => Some("!~~*"),
        _ => None,
    }
}

pub(crate) fn replace_ascii_case_insensitive(
    input: &str,
    needle: &str,
    replacement: &str,
) -> Option<String> {
    let lower = input.to_ascii_lowercase();
    let needle_lower = needle.to_ascii_lowercase();
    let mut cursor = 0;
    let mut changed = false;
    let mut output = String::with_capacity(input.len());

    while let Some(relative) = lower[cursor..].find(&needle_lower) {
        let start = cursor + relative;
        output.push_str(&input[cursor..start]);
        output.push_str(replacement);
        cursor = start + needle.len();
        changed = true;
    }

    if changed {
        output.push_str(&input[cursor..]);
        Some(output)
    } else {
        None
    }
}

#[cfg(test)]
mod interval_parse_compat_tests {
    use super::{
        rewrite_pgvector_l1_operator, rewrite_postgres_interval_precision_literals,
        rewrite_qualified_current_user_calls, rewrite_variadic_builtin_calls,
    };

    #[test]
    fn qualified_current_user_call_rewrite_skips_sql_text() {
        let sql = "SELECT carrier_private.current_user(), \
                   'carrier_private.current_user()', \
                   $body$carrier_private.current_user()$body$ \
                   -- carrier_private.current_user()\n\
                   /* carrier_private.current_user() */";
        let rewritten = rewrite_qualified_current_user_calls(sql).unwrap();
        assert!(rewritten.contains("carrier_private.\"current_user\"()"));
        assert!(rewritten.contains("'carrier_private.current_user()'"));
        assert!(rewritten.contains("$body$carrier_private.current_user()$body$"));
        assert!(rewritten.contains("-- carrier_private.current_user()"));
        assert!(rewritten.contains("/* carrier_private.current_user() */"));
    }

    #[test]
    pub(crate) fn variadic_rewrite_handles_multiple_calls_and_skips_sql_text() {
        let sql = "SELECT concat(VARIADIC ARRAY['a', 'b']), \
                   concat_ws('-', VARIADIC ARRAY['c', 'd']), \
                   'concat(VARIADIC ARRAY[1])', \
                   $tag$format(VARIADIC ARRAY['ignored'])$tag$ \
                   -- concat(VARIADIC ARRAY['ignored'])\n\
                   /* concat_ws(',', VARIADIC ARRAY['ignored']) */";
        let rewritten = rewrite_variadic_builtin_calls(sql).unwrap();
        assert!(
            rewritten.contains("bicdb_variadic_call('concat', ARRAY['a', 'b'])"),
            "{rewritten}"
        );
        assert!(rewritten.contains("bicdb_variadic_call('concat_ws', '-', ARRAY['c', 'd'])"));
        assert!(rewritten.contains("'concat(VARIADIC ARRAY[1])'"));
        assert!(rewritten.contains("$tag$format(VARIADIC ARRAY['ignored'])$tag$"));
        assert!(rewritten.contains("-- concat(VARIADIC ARRAY['ignored'])"));
        assert!(rewritten.contains("/* concat_ws(',', VARIADIC ARRAY['ignored']) */"));
    }

    #[test]
    pub(crate) fn precision_literal_rewrite_skips_quoted_and_commented_text() {
        let sql = "SELECT INTERVAL ( 3 ) '23:59:59.999999', \
                   'INTERVAL(2) ''ignored''', $tag$INTERVAL(1) 'ignored'$tag$ \
                   -- INTERVAL(1) 'ignored'\n\
                   /* INTERVAL(1) 'ignored' */";
        let rewritten = rewrite_postgres_interval_precision_literals(sql).unwrap();
        assert!(rewritten.contains("CAST('23:59:59.999999' AS INTERVAL(3))"));
        assert!(rewritten.contains("'INTERVAL(2) ''ignored'''"));
        assert!(rewritten.contains("$tag$INTERVAL(1) 'ignored'$tag$"));
        assert!(rewritten.contains("-- INTERVAL(1) 'ignored'"));
        assert!(rewritten.contains("/* INTERVAL(1) 'ignored' */"));
    }

    #[test]
    pub(crate) fn pgvector_l1_rewrite_skips_quoted_and_commented_text() {
        let sql = "SELECT embedding <+> '[1,2,3]', '<+>', \"<+>\", \
                   $tag$<+>$tag$ -- <+>\n\
                   /* <+> */";
        let rewritten = rewrite_pgvector_l1_operator(sql).unwrap();
        assert!(rewritten.starts_with("SELECT embedding <^ '[1,2,3]'"));
        assert!(rewritten.contains("'<+>'"));
        assert!(rewritten.contains("\"<+>\""));
        assert!(rewritten.contains("$tag$<+>$tag$"));
        assert!(rewritten.contains("-- <+>"));
        assert!(rewritten.contains("/* <+> */"));
    }
}
