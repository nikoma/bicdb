//! Token-safe rewrites of PostgreSQL syntax the SQL parser does not know,
//! applied before parsing (outside strings, quoted identifiers, comments and
//! dollar quotes):
//!
//! - the postfix null tests `x ISNULL` / `x NOTNULL` (`x IS NULL` /
//!   `x IS NOT NULL`);
//! - `RANGE ... '1 day'::interval PRECEDING` and `interval '1 day' FOLLOWING`
//!   frame offsets, parenthesized so the frame parser reads one expression;
//! - `ALTER ROUTINE` / `DROP ROUTINE`, which name a function or a procedure.

/// One lexical item of SQL text.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Kind {
    Word,
    QuotedIdent,
    String,
    Number,
    Punct,
    Space,
}

#[derive(Debug, Clone, Copy)]
struct Token {
    kind: Kind,
    start: usize,
    end: usize,
}

fn is_word_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$' || byte >= 0x80
}

/// Split `sql` into tokens; comments count as space.
fn tokenize(sql: &str) -> Option<Vec<Token>> {
    let bytes = sql.as_bytes();
    let mut tokens = Vec::new();
    let mut index = 0usize;
    while index < bytes.len() {
        let start = index;
        let byte = bytes[index];
        let kind = if byte.is_ascii_whitespace() {
            while index < bytes.len() && bytes[index].is_ascii_whitespace() {
                index += 1;
            }
            Kind::Space
        } else if byte == b'-' && bytes.get(index + 1) == Some(&b'-') {
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
            Kind::Space
        } else if byte == b'/' && bytes.get(index + 1) == Some(&b'*') {
            let mut depth = 0usize;
            while index < bytes.len() {
                if bytes[index] == b'/' && bytes.get(index + 1) == Some(&b'*') {
                    depth += 1;
                    index += 2;
                } else if bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/') {
                    depth -= 1;
                    index += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    index += 1;
                }
            }
            Kind::Space
        } else if byte == b'\'' || byte == b'"' {
            index += 1;
            loop {
                if index >= bytes.len() {
                    return None;
                }
                if bytes[index] == byte {
                    if bytes.get(index + 1) == Some(&byte) {
                        index += 2;
                        continue;
                    }
                    index += 1;
                    break;
                }
                index += 1;
            }
            if byte == b'"' {
                Kind::QuotedIdent
            } else {
                Kind::String
            }
        } else if byte == b'$' {
            match crate::dollar_quote_delimiter(&sql[index..]) {
                Some(delimiter) => {
                    let body = index + delimiter.len();
                    let end = sql[body..].find(&delimiter)?;
                    index = body + end + delimiter.len();
                    Kind::String
                }
                None => {
                    index += 1;
                    while index < bytes.len() && bytes[index].is_ascii_digit() {
                        index += 1;
                    }
                    Kind::Word
                }
            }
        } else if byte.is_ascii_digit() {
            while index < bytes.len()
                && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'.')
            {
                index += 1;
            }
            Kind::Number
        } else if is_word_byte(byte) {
            // E'...' / U&'...' escape strings start with a word character.
            if (byte == b'E' || byte == b'e') && bytes.get(index + 1) == Some(&b'\'') {
                index += 2;
                loop {
                    if index >= bytes.len() {
                        return None;
                    }
                    if bytes[index] == b'\\' {
                        index += 2;
                        continue;
                    }
                    if bytes[index] == b'\'' {
                        if bytes.get(index + 1) == Some(&b'\'') {
                            index += 2;
                            continue;
                        }
                        index += 1;
                        break;
                    }
                    index += 1;
                }
                Kind::String
            } else {
                while index < bytes.len() && is_word_byte(bytes[index]) {
                    index += 1;
                }
                Kind::Word
            }
        } else {
            index += sql[index..].chars().next().map_or(1, char::len_utf8);
            Kind::Punct
        };
        tokens.push(Token {
            kind,
            start,
            end: index,
        });
    }
    Some(tokens)
}

fn text<'a>(sql: &'a str, token: &Token) -> &'a str {
    &sql[token.start..token.end]
}

fn is_word(sql: &str, token: &Token, word: &str) -> bool {
    token.kind == Kind::Word && text(sql, token).eq_ignore_ascii_case(word)
}

/// Indexes of the non-space tokens.
fn significant(tokens: &[Token]) -> Vec<usize> {
    tokens
        .iter()
        .enumerate()
        .filter(|(_, token)| token.kind != Kind::Space)
        .map(|(index, _)| index)
        .collect()
}

const NOT_EXPRESSION_END: [&str; 12] = [
    "select", "where", "and", "or", "not", "when", "then", "else", "on", "by", "having", "is",
];

/// `x ISNULL` -> `x IS NULL`, `x NOTNULL` -> `x IS NOT NULL`.
fn rewrite_postfix_null_tests(
    sql: &str,
    tokens: &[Token],
    edits: &mut Vec<(usize, usize, String)>,
) {
    let order = significant(tokens);
    for (position, &index) in order.iter().enumerate() {
        let token = &tokens[index];
        let replacement = if is_word(sql, token, "isnull") {
            "IS NULL"
        } else if is_word(sql, token, "notnull") {
            "IS NOT NULL"
        } else {
            continue;
        };
        // A call `isnull(...)` keeps its meaning.
        if order
            .get(position + 1)
            .is_some_and(|&next| text(sql, &tokens[next]) == "(")
        {
            continue;
        }
        let Some(&previous) = position.checked_sub(1).and_then(|p| order.get(p)) else {
            continue;
        };
        let previous = &tokens[previous];
        let ends_expression = match previous.kind {
            Kind::Word => !NOT_EXPRESSION_END
                .iter()
                .any(|word| text(sql, previous).eq_ignore_ascii_case(word)),
            Kind::QuotedIdent | Kind::String | Kind::Number => true,
            Kind::Punct => matches!(text(sql, previous), ")" | "]"),
            Kind::Space => false,
        };
        if ends_expression {
            edits.push((token.start, token.end, replacement.to_string()));
        }
    }
}

/// Window frame offsets that are typed expressions (`'1 day'::interval
/// PRECEDING`, `interval '1 day' FOLLOWING`, `CAST(.. AS interval)
/// PRECEDING`): parenthesized, which the frame parser accepts.
fn rewrite_frame_offsets(sql: &str, tokens: &[Token], edits: &mut Vec<(usize, usize, String)>) {
    let order = significant(tokens);
    for (position, &index) in order.iter().enumerate() {
        let token = &tokens[index];
        if !(is_word(sql, token, "preceding") || is_word(sql, token, "following")) {
            continue;
        }
        // Walk back over the offset expression to BETWEEN / AND / ROWS /
        // RANGE / GROUPS.
        let mut start_position = position;
        let mut depth = 0i32;
        while start_position > 0 {
            let candidate = &tokens[order[start_position - 1]];
            let candidate_text = text(sql, candidate);
            if candidate.kind == Kind::Punct && candidate_text == ")" {
                depth += 1;
            } else if candidate.kind == Kind::Punct && candidate_text == "(" {
                if depth == 0 {
                    break;
                }
                depth -= 1;
            } else if depth == 0
                && candidate.kind == Kind::Word
                && ["between", "and", "rows", "range", "groups"]
                    .iter()
                    .any(|word| candidate_text.eq_ignore_ascii_case(word))
            {
                break;
            }
            start_position -= 1;
        }
        if start_position == position {
            continue;
        }
        let offset_tokens = &order[start_position..position];
        // Plain literals, UNBOUNDED and parenthesized offsets already parse.
        let simple = offset_tokens.len() == 1
            || (offset_tokens.len() >= 2
                && text(sql, &tokens[offset_tokens[0]]) == "("
                && text(sql, &tokens[*offset_tokens.last().expect("non-empty")]) == ")");
        if simple {
            continue;
        }
        let first = &tokens[offset_tokens[0]];
        let last = &tokens[*offset_tokens.last().expect("non-empty")];
        edits.push((first.start, first.start, "(".to_string()));
        edits.push((last.end, last.end, ")".to_string()));
    }
}

/// `x::type COLLATE "C"`: the parser accepts COLLATE after a parenthesized
/// cast only, so the cast chain is parenthesized: `(x::type) COLLATE "C"`.
fn rewrite_collate_after_cast(
    sql: &str,
    tokens: &[Token],
    edits: &mut Vec<(usize, usize, String)>,
) {
    let order = significant(tokens);
    let punct = |position: usize, value: &str| {
        order.get(position).is_some_and(|&index| {
            tokens[index].kind == Kind::Punct && text(sql, &tokens[index]) == value
        })
    };
    // Position of the matching `(` for the `)` at `close`.
    let open_of = |close: usize| -> Option<usize> {
        let mut depth = 0i32;
        let mut position = close;
        loop {
            let token = &tokens[order[position]];
            if token.kind == Kind::Punct {
                match text(sql, token) {
                    ")" => depth += 1,
                    "(" => {
                        depth -= 1;
                        if depth == 0 {
                            return Some(position);
                        }
                    }
                    _ => {}
                }
            }
            position = position.checked_sub(1)?;
        }
    };
    for (position, &index) in order.iter().enumerate() {
        if !is_word(sql, &tokens[index], "collate") || position < 3 {
            continue;
        }
        // Walk back over `:: type` steps.
        let mut cursor = position - 1;
        let mut casts = 0;
        loop {
            // A type: word(s), optional (n), optional [].
            let mut type_end = cursor;
            if punct(type_end, "]") && type_end >= 2 && punct(type_end - 1, "[") {
                type_end -= 2;
            }
            if punct(type_end, ")") {
                let Some(open) = open_of(type_end) else { break };
                if open == 0 {
                    break;
                }
                type_end = open - 1;
            }
            let mut type_start = type_end;
            while tokens[order[type_start]].kind == Kind::Word
                && type_start >= 1
                && tokens[order[type_start - 1]].kind == Kind::Word
                && !punct(type_start - 1, "::")
            {
                // multi-word types such as `double precision`
                type_start -= 1;
            }
            if tokens[order[type_start]].kind != Kind::Word
                && tokens[order[type_start]].kind != Kind::QuotedIdent
            {
                break;
            }
            // `::` is two `:` punctuation tokens.
            if type_start >= 3 && punct(type_start - 1, ":") && punct(type_start - 2, ":") {
                casts += 1;
                cursor = type_start - 3;
                // Another cast step follows if a type precedes `::` again.
                let before = &tokens[order[cursor]];
                if before.kind == Kind::Word
                    && cursor >= 2
                    && punct(cursor - 1, ":")
                    && punct(cursor - 2, ":")
                {
                    continue;
                }
                break;
            }
            break;
        }
        if casts == 0 {
            continue;
        }
        // The operand: a parenthesized group or call, or a (qualified)
        // name or literal.
        let mut start = cursor;
        if punct(start, ")") {
            let Some(open) = open_of(start) else { continue };
            start = open;
            if start >= 1 && tokens[order[start - 1]].kind == Kind::Word {
                start -= 1;
            }
        } else if !matches!(
            tokens[order[start]].kind,
            Kind::Word | Kind::QuotedIdent | Kind::String | Kind::Number
        ) {
            continue;
        }
        while start >= 2
            && punct(start - 1, ".")
            && matches!(
                tokens[order[start - 2]].kind,
                Kind::Word | Kind::QuotedIdent
            )
        {
            start -= 2;
        }
        // A typed literal `text 'x'` keeps its type word.
        if tokens[order[start]].kind == Kind::String
            && start >= 1
            && tokens[order[start - 1]].kind == Kind::Word
            && !NOT_EXPRESSION_END
                .iter()
                .any(|word| text(sql, &tokens[order[start - 1]]).eq_ignore_ascii_case(word))
        {
            start -= 1;
        }
        let begin = tokens[order[start]].start;
        let end = tokens[order[position - 1]].end;
        edits.push((begin, begin, "(".to_string()));
        edits.push((end, end, ")".to_string()));
    }
}

/// Function a prefix operator the parser cannot read is spelled as:
/// `bicdb_prefix_operator('~~~', operand)`, bound by the operator binder.
pub(crate) const PREFIX_OPERATOR_FUNCTION: &str = "bicdb_prefix_operator";

const OPERATOR_CHARS: &str = "+-*/<>=~!@#%^&|`?";

/// Operator names the parser's tokenizer splits into several tokens: a run of
/// at least three `=`/`!` characters with two `=` (`===`, `!==`), or a run of
/// three or more `~` or `@` (`~~~`, `@@@`).
fn split_operator_name(name: &str) -> bool {
    (name.len() >= 3
        && name.bytes().all(|byte| matches!(byte, b'=' | b'!'))
        && name.bytes().filter(|byte| *byte == b'=').count() >= 2)
        || (name.len() >= 3 && name.bytes().all(|byte| byte == b'~'))
        || (name.len() >= 3 && name.bytes().all(|byte| byte == b'@'))
}

/// User-defined operator names the parser splits: infix uses become
/// `OPERATOR("===")` (with their `OPERATOR(schema.name)` namespace kept),
/// prefix uses `bicdb_prefix_operator('~~~', operand)`.
fn rewrite_split_operators(sql: &str, tokens: &[Token], edits: &mut Vec<(usize, usize, String)>) {
    let order = significant(tokens);
    if order.first().is_some_and(|&index| {
        ["create", "drop", "alter", "comment"]
            .iter()
            .any(|word| is_word(sql, &tokens[index], word))
    }) && order
        .get(1)
        .is_some_and(|&index| is_word(sql, &tokens[index], "operator"))
    {
        // Operator DDL: every multi-character operator name is quoted, which
        // the parser reads as one name (`CREATE OPERATOR "===" (...)`).
        quote_operator_ddl_names(sql, tokens, &order, edits);
        return;
    }
    let is_op = |index: usize| {
        tokens[index].kind == Kind::Punct && OPERATOR_CHARS.contains(text(sql, &tokens[index]))
    };
    let mut position = 0;
    while position < order.len() {
        let index = order[position];
        if !is_op(index) {
            position += 1;
            continue;
        }
        // A run of adjacent operator characters.
        let mut end_position = position;
        while end_position + 1 < order.len()
            && is_op(order[end_position + 1])
            && tokens[order[end_position + 1]].start == tokens[order[end_position]].end
        {
            end_position += 1;
        }
        let start = tokens[index].start;
        let end = tokens[order[end_position]].end;
        let name = &sql[start..end];
        let next_position = end_position + 1;
        position = next_position;
        if !split_operator_name(name) {
            continue;
        }
        let previous = position_before(&order, start, tokens);
        // Inside `OPERATOR(schema.name)`: quote the name.
        if previous.is_some_and(|previous| {
            text(sql, &tokens[previous]) == "." || text(sql, &tokens[previous]) == "("
        }) && order
            .get(next_position)
            .is_some_and(|&next| text(sql, &tokens[next]) == ")")
        {
            edits.push((start, end, format!("\"{name}\"")));
            continue;
        }
        let ends_expression = previous.is_some_and(|previous| {
            let previous = &tokens[previous];
            match previous.kind {
                Kind::Word => !NOT_EXPRESSION_END
                    .iter()
                    .any(|word| text(sql, previous).eq_ignore_ascii_case(word)),
                Kind::QuotedIdent | Kind::String | Kind::Number => true,
                Kind::Punct => matches!(text(sql, previous), ")" | "]"),
                Kind::Space => false,
            }
        });
        if ends_expression {
            edits.push((start, end, format!(" OPERATOR(\"{name}\") ")));
            continue;
        }
        // Prefix use: the operand is a literal, a (qualified) name, a call
        // or a parenthesized group.
        let Some(&first) = order.get(next_position) else {
            continue;
        };
        let mut last_position = next_position;
        match tokens[first].kind {
            Kind::String | Kind::Number => {}
            Kind::Word | Kind::QuotedIdent => {
                while last_position + 2 < order.len()
                    && text(sql, &tokens[order[last_position + 1]]) == "."
                    && matches!(
                        tokens[order[last_position + 2]].kind,
                        Kind::Word | Kind::QuotedIdent
                    )
                {
                    last_position += 2;
                }
                if order
                    .get(last_position + 1)
                    .is_some_and(|&next| text(sql, &tokens[next]) == "(")
                {
                    let Some(close) = matching_close(sql, tokens, &order, last_position + 1) else {
                        continue;
                    };
                    last_position = close;
                }
            }
            Kind::Punct if text(sql, &tokens[first]) == "(" => {
                let Some(close) = matching_close(sql, tokens, &order, next_position) else {
                    continue;
                };
                last_position = close;
            }
            _ => continue,
        }
        let operand_end = tokens[order[last_position]].end;
        edits.push((
            start,
            operand_end,
            format!(
                "{PREFIX_OPERATOR_FUNCTION}('{name}', {})",
                &sql[tokens[first].start..operand_end]
            ),
        ));
        position = last_position + 1;
    }
}

fn quote_operator_ddl_names(
    sql: &str,
    tokens: &[Token],
    order: &[usize],
    edits: &mut Vec<(usize, usize, String)>,
) {
    let is_op = |index: usize| {
        tokens[index].kind == Kind::Punct && OPERATOR_CHARS.contains(text(sql, &tokens[index]))
    };
    let mut position = 0;
    while position < order.len() {
        if !is_op(order[position]) {
            position += 1;
            continue;
        }
        let mut end_position = position;
        while end_position + 1 < order.len()
            && is_op(order[end_position + 1])
            && tokens[order[end_position + 1]].start == tokens[order[end_position]].end
        {
            end_position += 1;
        }
        let start = tokens[order[position]].start;
        let end = tokens[order[end_position]].end;
        let name_position = position.checked_sub(1).is_some_and(|previous| {
            let previous = &tokens[order[previous]];
            is_word(sql, previous, "operator")
                || is_word(sql, previous, "exists")
                || text(sql, previous) == "."
        });
        if end_position > position && (name_position || split_operator_name(&sql[start..end])) {
            edits.push((start, end, format!("\"{}\"", &sql[start..end])));
        }
        position = end_position + 1;
    }
}

fn position_before(order: &[usize], start: usize, tokens: &[Token]) -> Option<usize> {
    order
        .iter()
        .rev()
        .copied()
        .find(|&index| tokens[index].end <= start)
}

/// The order position of the `)` matching the `(` at `open`.
fn matching_close(sql: &str, tokens: &[Token], order: &[usize], open: usize) -> Option<usize> {
    let mut depth = 0i32;
    for (position, &index) in order.iter().enumerate().skip(open) {
        if tokens[index].kind != Kind::Punct {
            continue;
        }
        match text(sql, &tokens[index]) {
            "(" => depth += 1,
            ")" => {
                depth -= 1;
                if depth == 0 {
                    return Some(position);
                }
            }
            _ => {}
        }
    }
    None
}

/// Apply all rewrites of this module; `None` when nothing changed.
pub(crate) fn rewrite(sql: &str) -> Option<String> {
    let lower = sql.to_ascii_lowercase();
    if !(lower.contains("isnull")
        || lower.contains("notnull")
        || lower.contains("preceding")
        || lower.contains("following")
        || lower.contains("collate")
        || sql.contains("===")
        || sql.contains("!==")
        || sql.contains("~~~")
        || sql.contains("@@@")
        || lower.contains("operator"))
    {
        return None;
    }
    let tokens = tokenize(sql)?;
    let mut edits = Vec::new();
    rewrite_postfix_null_tests(sql, &tokens, &mut edits);
    rewrite_frame_offsets(sql, &tokens, &mut edits);
    if lower.contains("collate") {
        rewrite_collate_after_cast(sql, &tokens, &mut edits);
    }
    if sql.contains("===")
        || sql.contains("!==")
        || sql.contains("~~~")
        || sql.contains("@@@")
        || lower.contains("operator")
    {
        rewrite_split_operators(sql, &tokens, &mut edits);
    }
    if edits.is_empty() {
        return None;
    }
    edits.sort_by(|left, right| right.0.cmp(&left.0).then(right.1.cmp(&left.1)));
    let mut output = sql.to_string();
    for (start, end, replacement) in edits {
        output.replace_range(start..end, &replacement);
    }
    Some(output)
}

/// `ALTER ROUTINE` / `DROP ROUTINE`: each named routine resolved to the
/// function or procedure of that name, as the statements PostgreSQL runs.
/// Returns the statements to execute, or `None` when `sql` is neither.
pub(crate) fn routine_statements(
    sql: &str,
    kind_of: &dyn Fn(&str) -> Option<&'static str>,
) -> Option<Result<Vec<String>, String>> {
    let tokens = tokenize(sql)?;
    let order = significant(&tokens);
    let first = order.first().map(|&index| &tokens[index])?;
    let second = order.get(1).map(|&index| &tokens[index])?;
    let alter = is_word(sql, first, "alter");
    if !(alter || is_word(sql, first, "drop")) || !is_word(sql, second, "routine") {
        return None;
    }
    let verb = if alter { "ALTER" } else { "DROP" };
    let mut rest = sql[second.end..].trim().trim_end_matches(';').trim_end();
    let mut if_exists = "";
    if !alter && rest.len() >= 9 && rest[..9].eq_ignore_ascii_case("if exists") {
        if_exists = "IF EXISTS ";
        rest = rest[9..].trim_start();
    }
    let lower = rest.to_ascii_lowercase();
    let mut suffix = "";
    if !alter {
        for tail in [" cascade", " restrict"] {
            if lower.ends_with(tail) {
                suffix = &rest[rest.len() - tail.len()..];
                rest = rest[..rest.len() - tail.len()].trim_end();
                break;
            }
        }
    }
    let targets = if alter {
        vec![rest.to_string()]
    } else {
        crate::split_top_level_commas_nested(rest)
    };
    let mut statements = Vec::new();
    for target in targets {
        let target = target.trim();
        let name_end = target
            .find(|ch: char| ch == '(' || ch.is_whitespace())
            .unwrap_or(target.len());
        let name = &target[..name_end];
        match kind_of(name) {
            Some(kind) => statements.push(format!("{verb} {kind} {if_exists}{target}{suffix}")),
            None if !if_exists.is_empty() => {}
            None => return Some(Err(display_signature(target))),
        }
    }
    Some(Ok(statements))
}

/// `name(int, text)` as PostgreSQL names it in errors: `name(integer, text)`.
fn display_signature(target: &str) -> String {
    let (name, args) = match target.find('(') {
        Some(open) => (
            target[..open].trim(),
            target[open + 1..target.rfind(')').unwrap_or(target.len())].trim(),
        ),
        None => (target.trim(), ""),
    };
    let args = crate::split_top_level_commas_nested(args)
        .iter()
        .map(|arg| {
            crate::pg_operators::parse_sql_data_type(arg.trim())
                .ok()
                .and_then(|data_type| crate::pg_type_from_data_type(&data_type).ok())
                .map(|(pg_type, _)| crate::common_type_display(&pg_type))
                .unwrap_or_else(|| arg.trim().to_string())
        })
        .filter(|arg| !arg.is_empty())
        .collect::<Vec<_>>()
        .join(", ");
    format!("{}({args})", name.to_ascii_lowercase())
}

impl crate::SqlSession<'_> {
    /// `ALTER ROUTINE` / `DROP ROUTINE` run as the ALTER/DROP FUNCTION or
    /// PROCEDURE of each named routine.
    pub(crate) fn execute_raw_routine_statement(
        &mut self,
        sql: &str,
    ) -> crate::Result<Option<crate::SqlResult>> {
        if crate::split_sql_statements(sql).len() != 1 {
            return Ok(None);
        }
        let db = self.db_ref();
        let kind_of = |name: &str| -> Option<&'static str> {
            let name = crate::normalize_object_name(name);
            if crate::load_routine(db, crate::RoutineKind::Function, &name)
                .ok()
                .flatten()
                .is_some()
            {
                Some("FUNCTION")
            } else if crate::load_routine(db, crate::RoutineKind::Procedure, &name)
                .ok()
                .flatten()
                .is_some()
            {
                Some("PROCEDURE")
            } else {
                None
            }
        };
        let Some(statements) = routine_statements(sql, &kind_of) else {
            return Ok(None);
        };
        let statements = statements.map_err(|signature| {
            crate::SqlError::undefined_function(format!("function {signature} does not exist"))
        })?;
        let alter = sql.trim_start()[..5].eq_ignore_ascii_case("alter");
        for statement in statements {
            self.execute(&statement)?;
        }
        Ok(Some(crate::SqlResult::command(if alter {
            "ALTER ROUTINE"
        } else {
            "DROP ROUTINE"
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postfix_null_tests() {
        assert_eq!(
            rewrite("select * from t where b isnull and c NOTNULL").unwrap(),
            "select * from t where b IS NULL and c IS NOT NULL"
        );
        assert_eq!(
            rewrite("SELECT (a + 1) isnull, isnull(a, 0), 'isnull'").unwrap(),
            "SELECT (a + 1) IS NULL, isnull(a, 0), 'isnull'"
        );
        assert!(rewrite("SELECT \"isnull\" FROM t").is_none());
    }

    #[test]
    fn split_operators_are_spelled_for_the_parser() {
        assert_eq!(
            rewrite("SELECT 1 === 2, a!==b, 3 OPERATOR(public.===) 4").unwrap(),
            "SELECT 1  OPERATOR(\"===\")  2, a OPERATOR(\"!==\") b, 3 OPERATOR(public.\"===\") 4"
        );
        assert_eq!(
            rewrite("SELECT ~~~ 'abc', ~~~ f(x), ~~~(1)").unwrap(),
            "SELECT bicdb_prefix_operator('~~~', 'abc'), bicdb_prefix_operator('~~~', f(x)), bicdb_prefix_operator('~~~', (1))"
        );
        assert_eq!(
            rewrite("CREATE OPERATOR === (leftarg = int, rightarg = int, function = f, commutator = !==)").unwrap(),
            "CREATE OPERATOR \"===\" (leftarg = int, rightarg = int, function = f, commutator = \"!==\")"
        );
        assert_eq!(
            rewrite("DROP OPERATOR public.@@@ (int, int)").unwrap(),
            "DROP OPERATOR public.\"@@@\" (int, int)"
        );
        assert!(rewrite("SELECT '===' FROM t").is_none());
    }

    #[test]
    fn insert_element_targets_only_rewrite_target_lists() {
        assert_eq!(
            rewrite_insert_element_targets("INSERT INTO ai (id, f[1], c.r) VALUES (1, 2, 3)").unwrap(),
            "INSERT INTO ai (id, \"bicdb$insert$target:f[1]\", \"bicdb$insert$target:c.r\") VALUES (1, 2, 3)"
        );
        for sql in [
            "INSERT INTO holder VALUES(1,ROW(1,'kept')::original,ARRAY[ROW(1,'a')::original])",
            "INSERT INTO t SELECT row.v * 1000",
            "INSERT INTO \"EMPLOYEES\" VALUES (1, 2.4)",
            "INSERT INTO s.t (a, b) VALUES (1, 2)",
        ] {
            assert!(rewrite_insert_element_targets(sql).is_none(), "{sql}");
        }
    }

    #[test]
    fn collate_after_cast_is_parenthesized() {
        assert_eq!(
            rewrite(
                "SELECT 'b'::text COLLATE \"C\" < x ORDER BY tgrelid::regclass::text COLLATE \"C\""
            )
            .unwrap(),
            "SELECT ('b'::text) COLLATE \"C\" < x ORDER BY (tgrelid::regclass::text) COLLATE \"C\""
        );
        assert_eq!(
            rewrite("CREATE INDEX i ON t ((c1::varchar(10) COLLATE \"C\"))").unwrap(),
            "CREATE INDEX i ON t (((c1::varchar(10)) COLLATE \"C\"))"
        );
        assert!(rewrite("SELECT c COLLATE \"C\" FROM t").is_none());
    }

    #[test]
    fn typed_frame_offsets_are_parenthesized() {
        assert_eq!(
            rewrite("sum(x) over (order by d range between '1 year'::interval preceding and interval '1 day' following)").unwrap(),
            "sum(x) over (order by d range between ('1 year'::interval) preceding and (interval '1 day') following)"
        );
        assert!(
            rewrite("sum(x) over (rows between 2 preceding and unbounded following)").is_none()
        );
    }
}

/// What a `REINDEX` names.
#[derive(Debug, PartialEq)]
enum ReindexTarget {
    Index,
    Table,
    Schema,
    Database,
    System,
}

impl crate::SqlSession<'_> {
    /// `REINDEX [(options)] {INDEX|TABLE|SCHEMA|DATABASE|SYSTEM} [CONCURRENTLY]
    /// [name]`. BicDB's indexes are maintained transactionally with their
    /// table and never need rebuilding, so after PostgreSQL's checks
    /// (existence, ownership, transaction-block rules) there is nothing to do.
    pub(crate) fn execute_raw_reindex(
        &mut self,
        sql: &str,
    ) -> crate::Result<Option<crate::SqlResult>> {
        use crate::{SqlError, SqlValue};
        let text = sql.trim().trim_end_matches(';').trim();
        if !(text.len() >= 7 && text[..7].eq_ignore_ascii_case("reindex")) {
            return Ok(None);
        }
        if text[7..].starts_with(|ch: char| ch.is_alphanumeric() || ch == '_')
            || crate::split_sql_statements(sql).len() != 1
        {
            return Ok(None);
        }
        let mut rest = text[7..].trim_start();
        if rest.starts_with('(') {
            let close = rest
                .find(')')
                .ok_or_else(|| SqlError::InvalidSql("unterminated REINDEX options".into()))?;
            for option in rest[1..close].split(',') {
                let words = option.split_whitespace().collect::<Vec<_>>();
                let valid = matches!(
                    words
                        .first()
                        .map(|word| word.to_ascii_lowercase())
                        .as_deref(),
                    Some("verbose" | "concurrently" | "tablespace")
                );
                if !valid {
                    return Err(SqlError::data_exception_public(
                        "42601",
                        format!(
                            "unrecognized REINDEX option \"{}\"",
                            words.first().copied().unwrap_or("")
                        ),
                        None,
                    ));
                }
            }
            rest = rest[close + 1..].trim_start();
        }
        let words = crate::split_sql_words(rest);
        let mut words = words.iter();
        let target = match words
            .next()
            .map(|word| word.to_ascii_lowercase())
            .as_deref()
        {
            Some("index") => ReindexTarget::Index,
            Some("table") => ReindexTarget::Table,
            Some("schema") => ReindexTarget::Schema,
            Some("database") => ReindexTarget::Database,
            Some("system") => ReindexTarget::System,
            _ => {
                return Err(SqlError::data_exception_public(
                    "42601",
                    "syntax error at or near REINDEX",
                    None,
                ))
            }
        };
        let mut concurrently = rest.to_ascii_lowercase().contains("concurrently");
        let mut name = words.next().cloned();
        if name
            .as_deref()
            .is_some_and(|word| word.eq_ignore_ascii_case("concurrently"))
        {
            concurrently = true;
            name = words.next().cloned();
        }
        let in_block = self.tx.is_some();
        let kind_word = match target {
            ReindexTarget::Index => "INDEX",
            ReindexTarget::Table => "TABLE",
            ReindexTarget::Schema => "SCHEMA",
            ReindexTarget::Database => "DATABASE",
            ReindexTarget::System => "SYSTEM",
        };
        if in_block
            && (concurrently
                || matches!(
                    target,
                    ReindexTarget::Schema | ReindexTarget::Database | ReindexTarget::System
                ))
        {
            let what = if concurrently {
                "REINDEX CONCURRENTLY".to_string()
            } else {
                format!("REINDEX {kind_word}")
            };
            return Err(SqlError::data_exception_public(
                "25001",
                format!("{what} cannot run inside a transaction block"),
                None,
            ));
        }
        match target {
            ReindexTarget::Index | ReindexTarget::Table => {
                let name = name.ok_or_else(|| {
                    SqlError::data_exception_public("42601", "REINDEX requires a name", None)
                })?;
                let parts = crate::sql_identifier_parts(&name)?;
                let bare = parts.last().cloned().unwrap_or_default();
                let rows = crate::pg_class_rows(self.db_ref())?;
                let kinds: &[&str] = if target == ReindexTarget::Index {
                    &["i", "I"]
                } else {
                    &["r", "m", "p", "t"]
                };
                let found = rows.iter().find(|row| {
                    row.get("relname").map(SqlValue::to_cell).as_deref() == Some(bare.as_str())
                });
                let Some(found) = found else {
                    return Err(SqlError::data_exception_public(
                        "42P01",
                        format!("relation \"{name}\" does not exist"),
                        None,
                    ));
                };
                let relkind = found
                    .get("relkind")
                    .map(SqlValue::to_cell)
                    .unwrap_or_default();
                if !kinds.contains(&relkind.as_str()) {
                    return Err(SqlError::data_exception_public(
                        "42809",
                        if target == ReindexTarget::Index {
                            format!("\"{name}\" is not an index")
                        } else {
                            format!("\"{name}\" is not a table or materialized view")
                        },
                        None,
                    ));
                }
                if target == ReindexTarget::Table {
                    let relation = crate::relation_name_from_parts(&parts)?;
                    let table =
                        crate::resolve_session_relation_name_if_exists(self.db_ref(), &relation);
                    self.require_table_ownership(&table, "REINDEX")?;
                }
            }
            ReindexTarget::Schema => {
                let name = name.ok_or_else(|| {
                    SqlError::data_exception_public("42601", "REINDEX SCHEMA requires a name", None)
                })?;
                let schema = crate::normalize_object_name(&name);
                if !(matches!(schema.as_str(), "pg_catalog" | "information_schema")
                    || crate::load_namespace(self.db_ref(), &schema)?.is_some())
                {
                    return Err(SqlError::data_exception_public(
                        "3F000",
                        format!("schema \"{schema}\" does not exist"),
                        None,
                    ));
                }
            }
            ReindexTarget::Database | ReindexTarget::System => {
                if let Some(name) = name {
                    let current = self
                        .session_gucs
                        .get("database")
                        .cloned()
                        .unwrap_or_else(|| "bicdb".to_string());
                    if crate::normalize_object_name(&name) != current {
                        return Err(SqlError::data_exception_public(
                            "0A000",
                            "can only reindex the currently open database",
                            None,
                        ));
                    }
                }
            }
        }
        Ok(Some(crate::SqlResult::command("REINDEX")))
    }
}

/// Whether `sql` declares a PostgreSQL 18 temporal key (`... WITHOUT
/// OVERLAPS` in PRIMARY KEY or UNIQUE).
pub(crate) fn declares_temporal_key(sql: &str) -> bool {
    let lower = sql.to_ascii_lowercase();
    if !lower.contains("overlaps") {
        return false;
    }
    let Some(tokens) = tokenize(sql) else {
        return false;
    };
    let order = significant(&tokens);
    let words = order
        .iter()
        .map(|&index| {
            (
                tokens[index].kind,
                text(sql, &tokens[index]).to_ascii_lowercase(),
            )
        })
        .collect::<Vec<_>>();
    let ddl =
        matches!(words.first(), Some((Kind::Word, word)) if word == "create" || word == "alter");
    ddl && words.windows(2).any(|pair| {
        matches!(&pair[0], (Kind::Word, word) if word == "without")
            && matches!(&pair[1], (Kind::Word, word) if word == "overlaps")
    })
}

impl crate::SqlSession<'_> {
    /// Temporal keys are refused explicitly rather than with a syntax error.
    pub(crate) fn refuse_temporal_keys(&self, sql: &str) -> crate::Result<()> {
        if declares_temporal_key(sql) {
            return Err(crate::SqlError::Unsupported(
                "temporal keys (WITHOUT OVERLAPS) are not supported".into(),
            ));
        }
        Ok(())
    }
}

/// `UPDATE t SET c.f = v [, c.g = w]` on a composite column `c`: PostgreSQL
/// assigns the fields; here the column is assigned the row with those fields
/// replaced (`c = ROW(v, (c).g, ...)::type`).
pub(crate) fn bind_composite_field_assignments(
    db: &crate::BicDb,
    statement: &sqlparser::ast::Statement,
) -> crate::Result<Option<sqlparser::ast::Statement>> {
    use sqlparser::ast::{AssignmentTarget, Expr, Ident, ObjectName, Statement, TableFactor};
    let Statement::Update(update) = statement else {
        return Ok(None);
    };
    if !update.assignments.iter().any(|assignment| {
        matches!(&assignment.target, AssignmentTarget::ColumnName(name) if name.0.len() == 2)
    }) {
        return Ok(None);
    }
    let TableFactor::Table { name, alias, .. } = &update.table.relation else {
        return Ok(None);
    };
    let table_parts = crate::object_name_parts(name);
    let qualifier = alias
        .as_ref()
        .map(|alias| {
            crate::object_name_parts(&ObjectName::from(vec![alias.name.clone()]))[0].clone()
        })
        .unwrap_or_else(|| table_parts.last().cloned().unwrap_or_default());
    let relation = crate::relation_name(name)?;
    let table = crate::resolve_session_relation_name_if_exists(db, &relation);
    let Some(schema) = crate::load_schema(db, &table)? else {
        return Ok(None);
    };
    let mut bound = update.clone();
    let mut merged: Vec<(String, Vec<(String, Expr)>)> = Vec::new();
    let mut kept = Vec::new();
    for assignment in bound.assignments.drain(..) {
        let AssignmentTarget::ColumnName(target) = &assignment.target else {
            kept.push(assignment);
            continue;
        };
        let parts = crate::object_name_parts(target);
        let [column, field] = parts.as_slice() else {
            kept.push(assignment);
            continue;
        };
        // `t.col` qualified by the table keeps its meaning.
        if *column == qualifier
            || schema
                .column(column)
                .and_then(|column| column.user_type.as_ref())
                .is_none_or(|user_type| {
                    user_type.array
                        || !matches!(user_type.kind, crate::UserTypeKind::Composite { .. })
                })
        {
            kept.push(assignment);
            continue;
        }
        match merged.iter_mut().find(|(name, _)| name == column) {
            Some((_, fields)) => fields.push((field.clone(), assignment.value)),
            None => merged.push((column.clone(), vec![(field.clone(), assignment.value)])),
        }
    }
    if merged.is_empty() {
        return Ok(None);
    }
    for (column, fields) in merged {
        let user_type = schema
            .column(&column)
            .and_then(|column| column.user_type.clone())
            .expect("checked composite column");
        let crate::UserTypeKind::Composite { attributes, .. } = &user_type.kind else {
            unreachable!("checked composite column");
        };
        for (field, _) in &fields {
            if !attributes
                .iter()
                .any(|attribute| !attribute.dropped && &attribute.name == field)
            {
                return Err(crate::SqlError::data_exception_public(
                    "42703",
                    format!(
                        "cannot assign to field \"{field}\" of column \"{column}\" because there is no such column in data type {}",
                        user_type.formatted_name()
                    ),
                    None,
                ));
            }
        }
        let column_ident = Ident::with_quote('"', column.clone());
        let items = attributes
            .iter()
            .filter(|attribute| !attribute.dropped)
            .map(|attribute| {
                fields
                    .iter()
                    .rev()
                    .find(|(field, _)| *field == attribute.name)
                    .map(|(_, value)| value.to_string())
                    .unwrap_or_else(|| {
                        format!(
                            "(\"{}\").\"{}\"",
                            column.replace('"', "\"\""),
                            attribute.name.replace('"', "\"\"")
                        )
                    })
            })
            .collect::<Vec<_>>();
        let value = crate::parse_routine_expr(&format!(
            "CAST(ROW({}) AS {})",
            items.join(", "),
            user_type.formatted_name()
        ))?;
        kept.push(sqlparser::ast::Assignment {
            target: AssignmentTarget::ColumnName(ObjectName::from(vec![column_ident])),
            value,
        });
    }
    bound.assignments = kept;
    Ok(Some(Statement::Update(bound)))
}

/// Prefix of the placeholder column a subscripted or field INSERT target
/// (`INSERT INTO t (f[1], c.r)`) is parsed as.
pub(crate) const INSERT_TARGET_PREFIX: &str = "bicdb$insert$target:";

/// `INSERT INTO t (a, f[1], c.r) ...`: the parser takes only plain names in
/// the target list, so subscripted and field targets become quoted
/// placeholder names that `bind_insert_element_targets` resolves.
pub(crate) fn rewrite_insert_element_targets(sql: &str) -> Option<String> {
    let tokens = tokenize(sql)?;
    let order = significant(&tokens);
    let word = |position: usize, value: &str| {
        order
            .get(position)
            .is_some_and(|&index| is_word(sql, &tokens[index], value))
    };
    if !(word(0, "insert") && word(1, "into")) {
        return None;
    }
    // The target list follows the (possibly qualified) table name and an
    // optional `AS alias`.
    let is_name = |position: usize| {
        order
            .get(position)
            .is_some_and(|&index| matches!(tokens[index].kind, Kind::Word | Kind::QuotedIdent))
    };
    let mut position = 2;
    if !is_name(position) {
        return None;
    }
    position += 1;
    while order
        .get(position)
        .is_some_and(|&index| text(sql, &tokens[index]) == ".")
        && is_name(position + 1)
    {
        position += 2;
    }
    if word(position, "as") && is_name(position + 1) {
        position += 2;
    }
    let &open = order.get(position)?;
    if text(sql, &tokens[open]) != "(" {
        return None;
    }
    let mut depth = 0i32;
    let mut item_start = tokens[open].end;
    let mut edits = Vec::new();
    let mut current_has_element = false;
    for &index in &order[position..] {
        let token = &tokens[index];
        let value = text(sql, token);
        if token.kind == Kind::Punct {
            match value {
                "(" => {
                    // A target list holds no parenthesized expression.
                    if depth >= 1 {
                        return None;
                    }
                    depth += 1;
                    continue;
                }
                ")" | "," if depth == 1 => {
                    if current_has_element {
                        let item = sql[item_start..token.start].trim();
                        let start = item_start
                            + (sql[item_start..token.start].len()
                                - sql[item_start..token.start].trim_start().len());
                        edits.push((
                            start,
                            start + item.len(),
                            format!("\"{INSERT_TARGET_PREFIX}{}\"", item.replace('"', "\"\"")),
                        ));
                    }
                    current_has_element = false;
                    item_start = token.end;
                    if value == ")" {
                        break;
                    }
                    continue;
                }
                "[" | "." => current_has_element = true,
                "]" | ":" | "-" | "+" => {}
                _ => return None,
            }
        }
        // A SELECT/VALUES/DEFAULT keyword means this was not a target list.
        if depth == 1 && token.kind == Kind::Word {
            let lower = value.to_ascii_lowercase();
            if matches!(lower.as_str(), "select" | "values" | "with") {
                return None;
            }
        }
    }
    if edits.is_empty() {
        return None;
    }
    let mut output = sql.to_string();
    for (start, end, replacement) in edits.into_iter().rev() {
        output.replace_range(start..end, &replacement);
    }
    Some(output)
}

/// Resolve the placeholder targets of `rewrite_insert_element_targets`: each
/// base column is inserted once, its value built from NULL by the element
/// and field assignments in target order (as PostgreSQL builds it).
pub(crate) fn bind_insert_element_targets(
    db: &crate::BicDb,
    statement: &sqlparser::ast::Statement,
) -> crate::Result<Option<sqlparser::ast::Statement>> {
    use sqlparser::ast::{Expr, Ident, ObjectName, SetExpr, Statement};
    let Statement::Insert(insert) = statement else {
        return Ok(None);
    };
    let has_placeholder = insert.columns.iter().any(|column| {
        crate::object_name_parts(column)
            .last()
            .is_some_and(|name| name.starts_with(INSERT_TARGET_PREFIX))
    });
    if !has_placeholder {
        return Ok(None);
    }
    let unsupported = || {
        crate::SqlError::Unsupported(
            "INSERT into array elements or composite fields is supported with VALUES only".into(),
        )
    };
    let Some(source) = insert.source.as_ref() else {
        return Err(unsupported());
    };
    let SetExpr::Values(values) = source.body.as_ref() else {
        return Err(unsupported());
    };
    let sqlparser::ast::TableObject::TableName(table_name) = &insert.table else {
        return Err(unsupported());
    };
    let relation = crate::relation_name(table_name)?;
    let table = crate::resolve_session_relation_name_if_exists(db, &relation);
    let schema = crate::load_schema(db, &table)?;
    // Per target: (base column, element suffix or None for a plain column).
    let targets = insert
        .columns
        .iter()
        .map(|column| {
            let name = crate::object_name_parts(column)
                .last()
                .cloned()
                .unwrap_or_default();
            match name.strip_prefix(INSERT_TARGET_PREFIX) {
                Some(target) => {
                    let split = target
                        .find(|ch: char| ch == '[' || ch == '.')
                        .unwrap_or(target.len());
                    let base = crate::normalize_object_name(target[..split].trim());
                    (base, Some(target[split..].trim().to_string()))
                }
                None => (name, None),
            }
        })
        .collect::<Vec<_>>();
    let mut bases: Vec<String> = Vec::new();
    for (base, _) in &targets {
        if !bases.contains(base) {
            bases.push(base.clone());
        }
    }
    let mut rows = Vec::with_capacity(values.rows.len());
    for parens in &values.rows {
        let row = &parens.content;
        if row.len() != targets.len() {
            return Err(crate::SqlError::InvalidSql(format!(
                "INSERT has {} target columns but {} values",
                targets.len(),
                row.len()
            )));
        }
        let mut built: Vec<(String, String)> = Vec::new();
        for ((base, suffix), value) in targets.iter().zip(row) {
            let value_text = value.to_string();
            let accumulator = built
                .iter()
                .find(|(name, _)| name == base)
                .map(|(_, text)| text.clone());
            let next = match suffix {
                None => value_text,
                Some(suffix) if suffix.starts_with('[') => {
                    let column_type = schema
                        .as_ref()
                        .and_then(|schema| schema.column(base))
                        .map(|column| column.pg_type.clone())
                        .unwrap_or_else(|| "text[]".to_string());
                    let start =
                        accumulator.unwrap_or_else(|| format!("CAST(NULL AS {column_type})"));
                    let update = crate::rewrite_update_array_assignments(&format!(
                        "UPDATE bicdb_target SET \"{}\"{suffix} = ({value_text})",
                        base.replace('"', "\"\"")
                    ))
                    .ok_or_else(unsupported)?;
                    let marker = format!("bicdb_array_assign(\"{}\",", base.replace('"', "\"\""));
                    let at = update.find(&marker).ok_or_else(unsupported)?;
                    format!(
                        "bicdb_array_assign({start},{}",
                        &update[at + marker.len()..]
                    )
                }
                Some(suffix) => {
                    // `.field` of a composite column.
                    let field = crate::normalize_object_name(suffix.trim_start_matches('.'));
                    let user_type = schema
                        .as_ref()
                        .and_then(|schema| schema.column(base))
                        .and_then(|column| column.user_type.clone())
                        .ok_or_else(unsupported)?;
                    let crate::UserTypeKind::Composite { attributes, .. } = &user_type.kind else {
                        return Err(unsupported());
                    };
                    let items = attributes
                        .iter()
                        .filter(|attribute| !attribute.dropped)
                        .map(|attribute| {
                            if attribute.name == field {
                                value_text.clone()
                            } else {
                                match &accumulator {
                                    Some(previous) => format!(
                                        "({previous}).\"{}\"",
                                        attribute.name.replace('"', "\"\"")
                                    ),
                                    None => "NULL".to_string(),
                                }
                            }
                        })
                        .collect::<Vec<_>>();
                    format!(
                        "CAST(ROW({}) AS {})",
                        items.join(", "),
                        user_type.formatted_name()
                    )
                }
            };
            match built.iter_mut().find(|(name, _)| name == base) {
                Some(entry) => entry.1 = next,
                None => built.push((base.clone(), next)),
            }
        }
        let row = bases
            .iter()
            .map(|base| {
                let text = &built
                    .iter()
                    .find(|(name, _)| name == base)
                    .expect("built")
                    .1;
                crate::parse_routine_expr(text)
            })
            .collect::<crate::Result<Vec<Expr>>>()?;
        rows.push(sqlparser::ast::Parens {
            opening_token: parens.opening_token.clone(),
            content: row,
            closing_token: parens.closing_token.clone(),
        });
    }
    let mut bound = insert.clone();
    bound.columns = bases
        .iter()
        .map(|base| ObjectName::from(vec![Ident::with_quote('"', base.clone())]))
        .collect();
    let mut source = (**insert.source.as_ref().expect("checked")).clone();
    if let SetExpr::Values(values) = source.body.as_mut() {
        values.rows = rows;
    }
    bound.source = Some(Box::new(source));
    Ok(Some(Statement::Insert(bound)))
}
