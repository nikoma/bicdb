//! PostgreSQL's ErrorResponse `P` field: the 1-based character position in
//! the client's query string that an error refers to. psql prints it as the
//! `LINE n: ...` line and the caret under it.
//!
//! PostgreSQL reports a position only for errors raised against a location
//! in the statement text: syntax errors, unknown columns, relations and
//! functions, and invalid literals. The positions here are recovered from
//! the error itself (the parser's line and column, or the object or literal
//! the message names) and checked against the client's text, so an error is
//! never given a position PostgreSQL would not report for it.

/// Position for `error` raised by `query`, when PostgreSQL would report one.
pub(crate) fn error_position(query: &str, sqlstate: &str, message: &str) -> Option<usize> {
    if let Some(position) = copy_option_position(query, message) {
        return Some(position);
    }
    let text = QueryText::new(query);
    if let Some(position) = syntax_error_position(&text, sqlstate, message) {
        return Some(position);
    }
    if sqlstate == "42601" {
        return None;
    }
    // Outside DML, PostgreSQL resolves names without a parse location
    // (`ALTER TABLE nosuch ...`, `DROP TYPE nosuch`).
    if !text.is_dml() {
        return None;
    }
    let message = strip_category_prefix(message);
    if let Some(column) = quoted_between(message, "column \"", "\" of relation \"") {
        if message.ends_with("\" does not exist") {
            return text.column_reference(column);
        }
        return None;
    }
    if let Some(name) = quoted_between(message, "column \"", "\" does not exist") {
        return text.column_reference(name);
    }
    if let Some(name) = message
        .strip_prefix("column ")
        .and_then(|rest| rest.strip_suffix(" does not exist"))
        .filter(|name| name.contains('.') && !name.contains('"'))
    {
        return text.column_reference(name);
    }
    if let Some(name) = quoted_between(message, "relation \"", "\" does not exist") {
        return text.relation_reference(name);
    }
    if let Some(name) = message.strip_prefix("collection not found: ") {
        return text.relation_reference(name.trim());
    }
    for prefix in [
        "missing FROM-clause entry for table \"",
        "invalid reference to FROM-clause entry for table \"",
    ] {
        if let Some(name) = quoted_between(message, prefix, "\"") {
            return text.qualifier_reference(name);
        }
    }
    if let Some(name) = quoted_between(message, "type \"", "\" does not exist") {
        return text.word_reference(name);
    }
    if let Some(rest) = message.strip_prefix("function ") {
        if let Some(signature) = rest.strip_suffix(" does not exist") {
            let name = signature.split('(').next().unwrap_or(signature);
            return text.function_reference(name.trim_matches('"'));
        }
    }
    for (prefix, suffix) in [
        ("invalid input syntax for type ", "\""),
        ("value \"", "\" is out of range for type "),
        ("date/time field value out of range: \"", "\""),
        ("interval field value out of range: \"", "\""),
        ("malformed array literal: \"", "\""),
        ("malformed range literal: \"", "\""),
        ("malformed multirange literal: \"", "\""),
        ("malformed record literal: \"", "\""),
    ] {
        if !message.starts_with(prefix) {
            continue;
        }
        let literal = if prefix == "invalid input syntax for type " {
            message
                .split_once(": \"")
                .and_then(|(_, rest)| rest.strip_suffix('"'))
        } else if suffix == "\"" {
            message
                .strip_prefix(prefix)
                .and_then(|rest| rest.strip_suffix(suffix))
        } else {
            message
                .strip_prefix(prefix)
                .and_then(|rest| rest.split_once(suffix))
                .map(|(value, _)| value)
        };
        if let Some(literal) = literal {
            return text.literal_reference(literal);
        }
    }
    // JSON input errors do not quote the text: the literal read as json.
    if matches!(
        message,
        "invalid input syntax for type json" | "unsupported Unicode escape sequence"
    ) {
        return text.json_literal_reference();
    }
    if let Some(value) = message
        .strip_prefix('"')
        .and_then(|rest| rest.split_once("\" is out of range for type "))
        .map(|(value, _)| value)
    {
        return text.literal_reference(value);
    }
    None
}

fn strip_category_prefix(message: &str) -> &str {
    for prefix in ["invalid SQL: ", "unsupported SQL: "] {
        if let Some(rest) = message.strip_prefix(prefix) {
            return rest;
        }
    }
    message
}

fn quoted_between<'a>(message: &'a str, prefix: &str, suffix: &str) -> Option<&'a str> {
    let rest = message.strip_prefix(prefix)?;
    let end = rest.find(suffix)?;
    Some(&rest[..end])
}

fn syntax_error_position(text: &QueryText, sqlstate: &str, message: &str) -> Option<usize> {
    if sqlstate != "42601" {
        return None;
    }
    let detail = message.split("sql parser error: ").nth(1)?;
    let found = detail.split(", found: ").nth(1)?;
    let (token, location) = match found.rsplit_once(" at Line: ") {
        Some((token, location)) => (token.trim(), Some(location)),
        None => (found.trim(), None),
    };
    if token.is_empty() || token == "EOF" {
        return Some(text.chars.len() + 1);
    }
    let location = location?;
    let (line, column) = location.split_once(", Column: ")?;
    let line = line.trim().parse::<usize>().ok()?;
    let column = column
        .trim()
        .trim_end_matches(|c: char| !c.is_ascii_digit())
        .parse::<usize>()
        .ok()?;
    let offset = text.offset_of_line_column(line, column)?;
    if text.starts_with_at(offset, token) {
        return Some(offset + 1);
    }
    // The statement may have been rewritten before parsing; accept only an
    // unambiguous occurrence of the token in the client's text.
    text.unique_token(token).map(|offset| offset + 1)
}

/// The client's statement as characters, with string literals, quoted
/// identifiers and comments marked so lookups only match code.
struct QueryText {
    chars: Vec<char>,
    /// `true` for characters inside a comment or a string literal.
    masked: Vec<bool>,
    /// Start offsets of single-quoted string literals and their contents.
    literals: Vec<(usize, String)>,
}

impl QueryText {
    fn new(query: &str) -> Self {
        let chars = query.chars().collect::<Vec<_>>();
        let mut masked = vec![false; chars.len()];
        let mut literals = Vec::new();
        let mut index = 0;
        while index < chars.len() {
            let c = chars[index];
            let next = chars.get(index + 1).copied();
            if c == '-' && next == Some('-') {
                while index < chars.len() && chars[index] != '\n' {
                    masked[index] = true;
                    index += 1;
                }
                continue;
            }
            if c == '/' && next == Some('*') {
                let mut depth = 0usize;
                while index < chars.len() {
                    if chars[index] == '/' && chars.get(index + 1) == Some(&'*') {
                        depth += 1;
                        masked[index] = true;
                        masked[index + 1] = true;
                        index += 2;
                        continue;
                    }
                    if chars[index] == '*' && chars.get(index + 1) == Some(&'/') {
                        masked[index] = true;
                        masked[index + 1] = true;
                        index += 2;
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                        continue;
                    }
                    masked[index] = true;
                    index += 1;
                }
                continue;
            }
            if c == '\'' {
                // An E'' / U&'' / B'' / X'' prefix belongs to the literal.
                let mut start = index;
                if index > 0 && matches!(chars[index - 1], 'e' | 'E' | 'b' | 'B' | 'x' | 'X') {
                    let before = index.checked_sub(2).map(|i| chars[i]);
                    if before.is_none_or(|before| !is_ident_char(before)) {
                        start = index - 1;
                    }
                }
                let escapes = start < index && matches!(chars[start], 'e' | 'E');
                let mut content = String::new();
                masked[index] = true;
                index += 1;
                while index < chars.len() {
                    let c = chars[index];
                    masked[index] = true;
                    if escapes && c == '\\' && index + 1 < chars.len() {
                        content.push(c);
                        content.push(chars[index + 1]);
                        masked[index + 1] = true;
                        index += 2;
                        continue;
                    }
                    if c == '\'' {
                        if chars.get(index + 1) == Some(&'\'') {
                            content.push('\'');
                            masked[index + 1] = true;
                            index += 2;
                            continue;
                        }
                        index += 1;
                        break;
                    }
                    content.push(c);
                    index += 1;
                }
                literals.push((start, content));
                continue;
            }
            if c == '$' {
                // Dollar-quoted string: $tag$ ... $tag$.
                let mut end = index + 1;
                while end < chars.len() && is_ident_char(chars[end]) && chars[end] != '$' {
                    end += 1;
                }
                let digit_tag = chars.get(index + 1).is_some_and(char::is_ascii_digit);
                if end < chars.len() && chars[end] == '$' && !digit_tag {
                    let tag = chars[index..=end].iter().collect::<String>();
                    let body_start = end + 1;
                    let rest = chars[body_start..].iter().collect::<String>();
                    if let Some(close) = rest.find(&tag) {
                        let close_chars = rest[..close].chars().count();
                        let content = rest[..close].to_string();
                        let stop = body_start + close_chars + tag.chars().count();
                        for flag in &mut masked[index..stop.min(chars.len())] {
                            *flag = true;
                        }
                        literals.push((index, content));
                        index = stop;
                        continue;
                    }
                }
            }
            if c == '"' {
                index += 1;
                while index < chars.len() {
                    if chars[index] == '"' {
                        if chars.get(index + 1) == Some(&'"') {
                            index += 2;
                            continue;
                        }
                        break;
                    }
                    index += 1;
                }
                index += 1;
                continue;
            }
            index += 1;
        }
        Self {
            chars,
            masked,
            literals,
        }
    }

    /// The first keyword of the statement, past comments and parentheses.
    fn first_keyword(&self) -> String {
        let mut index = 0;
        while index < self.chars.len()
            && (self.masked[index] || self.chars[index].is_whitespace() || self.chars[index] == '(')
        {
            index += 1;
        }
        self.chars[index..]
            .iter()
            .take_while(|c| c.is_ascii_alphabetic())
            .collect::<String>()
            .to_ascii_lowercase()
    }

    fn starts_with_keyword(&self, keywords: &[&str]) -> bool {
        let first = self.first_keyword();
        keywords.contains(&first.as_str())
    }

    fn is_dml(&self) -> bool {
        self.starts_with_keyword(&[
            "select", "insert", "update", "delete", "with", "table", "values", "merge",
        ])
    }

    fn offset_of_line_column(&self, line: usize, column: usize) -> Option<usize> {
        let mut current_line = 1;
        let mut line_start = 0;
        for (index, c) in self.chars.iter().enumerate() {
            if current_line == line {
                break;
            }
            if *c == '\n' {
                current_line += 1;
                line_start = index + 1;
            }
        }
        if current_line != line {
            return None;
        }
        let offset = line_start + column.checked_sub(1)?;
        (offset <= self.chars.len()).then_some(offset)
    }

    fn starts_with_at(&self, offset: usize, token: &str) -> bool {
        let token = token.chars().collect::<Vec<_>>();
        offset + token.len() <= self.chars.len()
            && self.chars[offset..offset + token.len()]
                .iter()
                .zip(&token)
                .all(|(left, right)| left.eq_ignore_ascii_case(right))
    }

    /// Code (unmasked) offsets where `word` appears as a whole identifier,
    /// unquoted (case-insensitively) or double-quoted (exactly).
    fn word_offsets(&self, word: &str) -> Vec<usize> {
        let target = word.chars().collect::<Vec<_>>();
        if target.is_empty() {
            return Vec::new();
        }
        let mut offsets = Vec::new();
        let mut index = 0;
        while index < self.chars.len() {
            if self.masked[index] {
                index += 1;
                continue;
            }
            if self.chars[index] == '"' {
                let start = index;
                let mut name = String::new();
                index += 1;
                while index < self.chars.len() {
                    if self.chars[index] == '"' {
                        if self.chars.get(index + 1) == Some(&'"') {
                            name.push('"');
                            index += 2;
                            continue;
                        }
                        break;
                    }
                    name.push(self.chars[index]);
                    index += 1;
                }
                index += 1;
                if name == word {
                    offsets.push(start);
                }
                continue;
            }
            if is_ident_start(self.chars[index]) {
                let start = index;
                while index < self.chars.len() && is_ident_char(self.chars[index]) {
                    index += 1;
                }
                let candidate = &self.chars[start..index];
                if candidate.len() == target.len()
                    && candidate
                        .iter()
                        .zip(&target)
                        .all(|(left, right)| left.to_lowercase().eq(right.to_lowercase()))
                {
                    offsets.push(start);
                }
                continue;
            }
            index += 1;
        }
        offsets
    }

    fn previous_code_char(&self, offset: usize) -> Option<(usize, char)> {
        (0..offset)
            .rev()
            .find(|&index| !self.chars[index].is_whitespace())
            .map(|index| (index, self.chars[index]))
    }

    fn next_code_char(&self, offset: usize) -> Option<char> {
        self.chars[offset..]
            .iter()
            .copied()
            .find(|c| !c.is_whitespace())
    }

    /// Start of the dotted name ending with the word at `offset`.
    fn qualified_start(&self, mut offset: usize) -> usize {
        while let Some((dot, '.')) = self.previous_code_char(offset) {
            let Some((end, c)) = self.previous_code_char(dot) else {
                break;
            };
            if c == '"' {
                let mut start = end;
                while start > 0 && !(self.chars[start - 1] == '"' && start - 1 != end) {
                    start -= 1;
                }
                offset = start.saturating_sub(1);
            } else if is_ident_char(c) {
                let mut start = end;
                while start > 0 && is_ident_char(self.chars[start - 1]) {
                    start -= 1;
                }
                offset = start;
            } else {
                break;
            }
        }
        offset
    }

    fn after_word(&self, offset: usize) -> usize {
        if self.chars.get(offset) == Some(&'"') {
            let mut index = offset + 1;
            while index < self.chars.len() {
                if self.chars[index] == '"' {
                    if self.chars.get(index + 1) == Some(&'"') {
                        index += 2;
                        continue;
                    }
                    return index + 1;
                }
                index += 1;
            }
            return index;
        }
        let mut index = offset;
        while index < self.chars.len() && is_ident_char(self.chars[index]) {
            index += 1;
        }
        index
    }

    /// A column reference: the first occurrence of the name that is not a
    /// function call; a qualified reference is reported at its qualifier.
    fn column_reference(&self, name: &str) -> Option<usize> {
        let (qualifier, column) = match name.rsplit_once('.') {
            Some((qualifier, column)) => (Some(qualifier), column),
            None => (None, name),
        };
        self.word_offsets(column)
            .into_iter()
            .filter(|&offset| self.next_code_char(self.after_word(offset)) != Some('('))
            .find(|&offset| {
                qualifier.is_none_or(|_| matches!(self.previous_code_char(offset), Some((_, '.'))))
            })
            .map(|offset| self.qualified_start(offset) + 1)
    }

    fn relation_reference(&self, name: &str) -> Option<usize> {
        let last = name.rsplit('.').next().unwrap_or(name);
        let qualified = name.contains('.');
        self.word_offsets(last)
            .into_iter()
            .find(|&offset| !qualified || matches!(self.previous_code_char(offset), Some((_, '.'))))
            .map(|offset| self.qualified_start(offset) + 1)
    }

    fn qualifier_reference(&self, name: &str) -> Option<usize> {
        self.word_offsets(name)
            .into_iter()
            .find(|&offset| self.next_code_char(self.after_word(offset)) == Some('.'))
            .map(|offset| self.qualified_start(offset) + 1)
    }

    fn word_reference(&self, name: &str) -> Option<usize> {
        let last = name.rsplit('.').next().unwrap_or(name);
        let last = last.split('(').next().unwrap_or(last).trim();
        let last = last.trim_end_matches("[]");
        self.word_offsets(last)
            .into_iter()
            .next()
            .map(|offset| self.qualified_start(offset) + 1)
    }

    fn function_reference(&self, name: &str) -> Option<usize> {
        let last = name.rsplit('.').next().unwrap_or(name);
        self.word_offsets(last)
            .into_iter()
            .find(|&offset| self.next_code_char(self.after_word(offset)) == Some('('))
            .map(|offset| self.qualified_start(offset) + 1)
    }

    fn literal_reference(&self, value: &str) -> Option<usize> {
        let (offset, _) = self.literals.iter().find(|(_, content)| content == value)?;
        // `'x'::text::bool`: the literal itself is valid text; the failing
        // conversion runs on a value at execution time, with no location.
        (!self.cast_to_text_after_literal(*offset) && !self.literal_read_as_text(*offset))
            .then_some(offset + 1)
    }

    /// Position of the one literal cast to `json`/`jsonb` (`'...'::json`,
    /// `json '...'`), or of an INSERT's only literal.
    fn json_literal_reference(&self) -> Option<usize> {
        let followed_by_json = |offset: usize| {
            let mut index = offset;
            while index < self.chars.len() && self.masked[index] {
                index += 1;
            }
            let rest = self.chars[index..]
                .iter()
                .take(16)
                .collect::<String>()
                .to_ascii_lowercase();
            let rest = rest.trim_start();
            rest.strip_prefix("::").is_some_and(|cast| {
                let cast = cast.trim_start();
                cast.starts_with("json") && !cast.starts_with("jsonpath")
            })
        };
        let preceded_by_json = |offset: usize| {
            let before = self.chars[..offset]
                .iter()
                .collect::<String>()
                .to_ascii_lowercase();
            let before = before.trim_end();
            before.ends_with("json") || before.ends_with("jsonb")
        };
        let candidates = self
            .literals
            .iter()
            .filter(|(offset, _)| followed_by_json(*offset) || preceded_by_json(*offset))
            .collect::<Vec<_>>();
        match candidates.as_slice() {
            [(offset, _)] => Some(offset + 1),
            [] if self.literals.len() == 1
                && self
                    .chars
                    .iter()
                    .collect::<String>()
                    .trim_start()
                    .to_ascii_lowercase()
                    .starts_with("insert") =>
            {
                Some(self.literals[0].0 + 1)
            }
            _ => None,
        }
    }

    /// Whether the literal at `literal` becomes a text value before any
    /// conversion that can fail: an argument of a built-in string function
    /// (`upper('x')::int`), or a column of a derived `(VALUES ...)` table.
    /// A later conversion of that value runs without a location.
    fn literal_read_as_text(&self, literal: usize) -> bool {
        // The unmasked `(` enclosing the literal.
        let mut depth = 0usize;
        let mut index = literal;
        let open = loop {
            if index == 0 {
                return false;
            }
            index -= 1;
            if self.masked[index] {
                continue;
            }
            match self.chars[index] {
                ')' => depth += 1,
                '(' if depth == 0 => break index,
                '(' => depth -= 1,
                _ => {}
            }
        };
        let word_before = |end: usize| -> (String, usize) {
            let mut end = end;
            while end > 0 && self.chars[end - 1].is_whitespace() {
                end -= 1;
            }
            let mut start = end;
            while start > 0 && is_ident_char(self.chars[start - 1]) && !self.masked[start - 1] {
                start -= 1;
            }
            (
                self.chars[start..end]
                    .iter()
                    .collect::<String>()
                    .to_ascii_lowercase(),
                start,
            )
        };
        let (function, start) = word_before(open);
        if matches!(
            function.as_str(),
            "upper"
                | "lower"
                | "initcap"
                | "btrim"
                | "ltrim"
                | "rtrim"
                | "trim"
                | "concat"
                | "concat_ws"
                | "replace"
                | "substr"
                | "substring"
                | "left"
                | "right"
                | "lpad"
                | "rpad"
                | "repeat"
                | "reverse"
                | "translate"
                | "split_part"
                | "format"
                | "md5"
        ) {
            return true;
        }
        // `(VALUES ('x'))`: the row's `(` follows VALUES, which follows `(`.
        if function == "values" {
            let mut before = start;
            while before > 0 && self.chars[before - 1].is_whitespace() {
                before -= 1;
            }
            return before > 0 && self.chars[before - 1] == '(' && !self.masked[before - 1];
        }
        false
    }

    fn cast_to_text_after_literal(&self, literal: usize) -> bool {
        let mut index = literal;
        while index < self.chars.len() && self.masked[index] {
            index += 1;
        }
        let rest = self.chars[index..]
            .iter()
            .collect::<String>()
            .trim_start()
            .to_ascii_lowercase();
        let Some(cast) = rest.strip_prefix("::") else {
            return false;
        };
        let cast = cast.trim_start();
        [
            "text",
            "varchar",
            "name",
            "bpchar",
            "character",
            "char",
            "\"char\"",
        ]
        .iter()
        .any(|target| {
            cast.strip_prefix(target).is_some_and(|after| {
                after
                    .chars()
                    .next()
                    .is_none_or(|c| !(c.is_alphanumeric() || c == '_'))
            })
        })
    }

    fn unique_token(&self, token: &str) -> Option<usize> {
        if token.chars().all(is_ident_char) {
            let offsets = self.word_offsets(token);
            return (offsets.len() == 1).then(|| offsets[0]);
        }
        let mut found = None;
        for offset in 0..self.chars.len() {
            if !self.masked[offset] && self.starts_with_at(offset, token) {
                if found.is_some() {
                    return None;
                }
                found = Some(offset);
            }
        }
        found
    }
}

fn is_ident_start(c: char) -> bool {
    c.is_alphabetic() || c == '_'
}

fn is_ident_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '$'
}

#[cfg(test)]
mod tests {
    use super::error_position;

    #[test]
    fn hidden_dml_alias_error_points_to_the_qualified_reference() {
        let query = "DELETE FROM public.t AS target WHERE public.t.id = 1";
        assert_eq!(
            error_position(
                query,
                "42P01",
                "invalid reference to FROM-clause entry for table \"t\""
            ),
            Some(query.rfind("public.t").unwrap() + 1),
        );
    }

    #[test]
    fn syntax_errors_point_at_the_token_or_end_of_input() {
        let query = "SELECT 1 FROM t WHERE";
        assert_eq!(
            error_position(
                query,
                "42601",
                "invalid SQL: sql parser error: Expected: an expression, found: EOF"
            ),
            Some(query.len() + 1)
        );
        let query = "select * from t where b isnull";
        assert_eq!(
            error_position(
                query,
                "42601",
                "invalid SQL: sql parser error: Expected: end of statement, found: isnull at Line: 1, Column: 25"
            ),
            Some(25)
        );
    }

    #[test]
    fn missing_objects_point_at_their_reference() {
        let query = "SELECT a, nosuch FROM t";
        assert_eq!(
            error_position(query, "42703", "column \"nosuch\" does not exist"),
            Some(11)
        );
        let query = "SELECT t.nosuch FROM t";
        assert_eq!(
            error_position(query, "42703", "column t.nosuch does not exist"),
            Some(8)
        );
        assert_eq!(
            error_position(query, "42703", "column \"t.nosuch\" does not exist"),
            Some(8)
        );
        let query = "SELECT 'nosuch', nosuch FROM t";
        assert_eq!(
            error_position(query, "42703", "column \"nosuch\" does not exist"),
            Some(18)
        );
        let query = "SELECT * FROM public.missing_t";
        assert_eq!(
            error_position(
                query,
                "42P01",
                "relation \"public.missing_t\" does not exist"
            ),
            Some(15)
        );
        let query = "SELECT nosuch_fn(1)";
        assert_eq!(
            error_position(query, "42883", "function nosuch_fn(integer) does not exist"),
            Some(8)
        );
        assert_eq!(
            error_position(
                "INSERT INTO t (a, nosuch) VALUES (1, 2)",
                "42703",
                "column \"nosuch\" of relation \"t\" does not exist"
            ),
            Some(19)
        );
        assert_eq!(
            error_position(
                "ALTER TABLE t DROP COLUMN nosuch",
                "42703",
                "column \"nosuch\" of relation \"t\" does not exist"
            ),
            None
        );
        assert_eq!(
            error_position(
                "DROP TABLE missing_t",
                "42P01",
                "relation \"missing_t\" does not exist"
            ),
            None
        );
        assert_eq!(
            error_position(
                "SELECT * FROM pos_nosuch_rel",
                "42P01",
                "collection not found: pos_nosuch_rel"
            ),
            Some(15)
        );
        assert_eq!(
            error_position(
                "SELECT a, nosuch FROM pos_t",
                "42703",
                "column \"nosuch\" of relation \"pos_t\" does not exist"
            ),
            Some(11)
        );
        // Errors PostgreSQL raises without a location get none.
        assert_eq!(
            error_position("SELECT 1/0", "22012", "division by zero"),
            None
        );
    }

    #[test]
    fn invalid_literals_point_at_the_literal() {
        let query = "SELECT bool 'test' AS error";
        assert_eq!(
            error_position(
                query,
                "22P02",
                "invalid input syntax for type boolean: \"test\""
            ),
            Some(13)
        );
        let query = "INSERT INTO t VALUES ('100000')";
        assert_eq!(
            error_position(
                query,
                "22003",
                "value \"100000\" is out of range for type smallint"
            ),
            Some(23)
        );
        assert_eq!(
            error_position(
                "SELECT '  tru e '::text::boolean AS invalid",
                "22P02",
                "invalid input syntax for type boolean: \"  tru e \""
            ),
            None
        );
        // No literal with that text: the value came from data.
        assert_eq!(
            error_position(
                "SELECT f::int FROM t",
                "22P02",
                "invalid input syntax for type integer: \"x\""
            ),
            None
        );
    }
}

/// PostgreSQL's `parser_errposition` for the COPY option errors that carry
/// one: the option's name in a parenthesized option list (for "conflicting or
/// redundant options", its second occurrence).
fn copy_option_position(query: &str, message: &str) -> Option<usize> {
    let trimmed = query.trim_start();
    if !trimmed
        .get(..5)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("copy "))
    {
        return None;
    }
    let option = if message == "conflicting or redundant options" {
        None
    } else if let Some(name) = quoted_between(message, "option \"", "\" not recognized")
        .or_else(|| quoted_between(message, "argument to option \"", "\" must be"))
    {
        Some(name.to_ascii_lowercase())
    } else if message.starts_with("COPY format \"") && message.ends_with("\" not recognized") {
        Some("format".to_string())
    } else if message.starts_with("COPY ON_ERROR ") {
        Some("on_error".to_string())
    } else if message.starts_with("COPY LOG_VERBOSITY \"") {
        Some("log_verbosity".to_string())
    } else {
        return None;
    };
    // Option names in a parenthesized list: the word after `(` or `,` at
    // depth 1, outside string literals. Byte offsets, then characters.
    let mut names: Vec<(String, usize)> = Vec::new();
    let bytes = query.as_bytes();
    let (mut depth, mut quoted, mut expect_name) = (0_i32, false, false);
    let mut idx = 0;
    while idx < bytes.len() {
        let c = bytes[idx];
        if quoted {
            if c == b'\'' {
                quoted = false;
            }
            idx += 1;
            continue;
        }
        match c {
            b'\'' => quoted = true,
            b'(' => {
                depth += 1;
                expect_name = depth == 1;
            }
            b')' => {
                depth -= 1;
                expect_name = false;
            }
            b',' if depth == 1 => expect_name = true,
            c if c.is_ascii_whitespace() => {}
            c if expect_name && (c.is_ascii_alphabetic() || c == b'_' || c == b'"') => {
                let start = idx;
                let end = if c == b'"' {
                    query[idx + 1..]
                        .find('"')
                        .map_or(bytes.len(), |end| idx + 2 + end)
                } else {
                    query[idx..]
                        .find(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
                        .map_or(bytes.len(), |end| idx + end)
                };
                let word = &query[start..end];
                let name = if c == b'"' {
                    word.trim_matches('"').to_string()
                } else {
                    word.to_ascii_lowercase()
                };
                names.push((name, start));
                expect_name = false;
                idx = end;
                continue;
            }
            _ => expect_name = false,
        }
        idx += 1;
    }
    let byte = match option {
        Some(option) => names.iter().find(|(name, _)| *name == option)?.1,
        None => names
            .iter()
            .enumerate()
            .find_map(|(index, (name, start))| {
                names[..index]
                    .iter()
                    .any(|(earlier, _)| earlier == name)
                    .then_some(*start)
            })?,
    };
    Some(query[..byte].chars().count() + 1)
}
