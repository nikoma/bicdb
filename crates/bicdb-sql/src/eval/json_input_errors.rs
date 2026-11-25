//! PostgreSQL's JSON input errors: the message, DETAIL and CONTEXT lines its
//! JSON lexer and recursive-descent parser report for invalid `json`/`jsonb`
//! text. Only the diagnosis lives here; values are still parsed by the
//! regular JSON reader, which calls [`pg_json_input_error`] when it rejects
//! the text.

use crate::SqlError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Token {
    /// No token lexed yet.
    Invalid,
    ObjectStart,
    ObjectEnd,
    ArrayStart,
    ArrayEnd,
    Comma,
    Colon,
    String,
    Number,
    True,
    False,
    Null,
    End,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JsonError {
    InvalidToken,
    EscapingInvalid,
    EscapingRequired,
    UnicodeEscapeFormat,
    UnicodeHighSurrogate,
    UnicodeLowSurrogate,
    UnicodeCodePointZero,
    ExpectedMore,
    ExpectedEnd,
    ExpectedJson,
    ExpectedString,
    ExpectedArrayNext,
    ExpectedObjectFirst,
    ExpectedColon,
    ExpectedObjectNext,
    TooDeep,
}

#[derive(Clone, Copy)]
enum ParseContext {
    End,
    Value,
    String,
    ArrayNext,
    ObjectStart,
    ObjectLabel,
    ObjectNext,
}

struct Lexer<'a> {
    input: &'a [u8],
    /// Whether string values are de-escaped (jsonb), which adds the
    /// surrogate-pair and `\u0000` checks.
    strings: bool,
    token_start: Option<usize>,
    token_terminator: usize,
    token: Token,
    line_number: usize,
    line_start: usize,
    depth: usize,
}

/// PostgreSQL's maximum nesting before its stack-depth check fires with the
/// default `max_stack_depth` (2MB); far deeper than any real document.
const MAX_DEPTH: usize = 6400;

fn alphanumeric(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte >= 0x80
}

/// Length of the UTF-8 character starting at `byte`.
fn char_len(byte: u8) -> usize {
    match byte {
        0xF0..=0xFF => 4,
        0xE0..=0xEF => 3,
        0xC0..=0xDF => 2,
        _ => 1,
    }
}

impl<'a> Lexer<'a> {
    fn fail_at_char_end(&mut self, at: usize, error: JsonError) -> JsonError {
        let end = at + char_len(self.input.get(at).copied().unwrap_or(0));
        self.token_terminator = end.min(self.input.len());
        error
    }

    fn lex(&mut self) -> Result<(), JsonError> {
        let input = self.input;
        let mut s = self.token_terminator;
        while s < input.len() && matches!(input[s], b' ' | b'\t' | b'\n' | b'\r') {
            if input[s] == b'\n' {
                self.line_number += 1;
                self.line_start = s + 1;
            }
            s += 1;
        }
        self.token_start = Some(s);
        if s >= input.len() {
            self.token_start = None;
            self.token_terminator = s;
            self.token = Token::End;
            return Ok(());
        }
        let single = |token| Some((token, s + 1));
        let punctuation = match input[s] {
            b'{' => single(Token::ObjectStart),
            b'}' => single(Token::ObjectEnd),
            b'[' => single(Token::ArrayStart),
            b']' => single(Token::ArrayEnd),
            b',' => single(Token::Comma),
            b':' => single(Token::Colon),
            _ => None,
        };
        if let Some((token, end)) = punctuation {
            self.token = token;
            self.token_terminator = end;
            return Ok(());
        }
        match input[s] {
            b'"' => {
                self.lex_string(s)?;
                self.token = Token::String;
            }
            b'-' => {
                self.lex_number(s + 1)?;
                self.token = Token::Number;
            }
            b'0'..=b'9' => {
                self.lex_number(s)?;
                self.token = Token::Number;
            }
            _ => {
                let mut p = s;
                while p < input.len() && alphanumeric(input[p]) {
                    p += 1;
                }
                if p == s {
                    self.token_terminator = s + char_len(input[s]).min(input.len() - s);
                    return Err(JsonError::InvalidToken);
                }
                self.token_terminator = p;
                self.token = match &input[s..p] {
                    b"true" => Token::True,
                    b"false" => Token::False,
                    b"null" => Token::Null,
                    _ => return Err(JsonError::InvalidToken),
                };
            }
        }
        Ok(())
    }

    fn lex_number(&mut self, start: usize) -> Result<(), JsonError> {
        let input = self.input;
        let digit = |at: usize| at < input.len() && input[at].is_ascii_digit();
        let mut s = start;
        let mut error = false;
        if s < input.len() && input[s] == b'0' {
            s += 1;
        } else if digit(s) {
            while digit(s) {
                s += 1;
            }
        } else {
            error = true;
        }
        if s < input.len() && input[s] == b'.' {
            s += 1;
            if !digit(s) {
                error = true;
            } else {
                while digit(s) {
                    s += 1;
                }
            }
        }
        if s < input.len() && matches!(input[s], b'e' | b'E') {
            s += 1;
            if s < input.len() && matches!(input[s], b'+' | b'-') {
                s += 1;
            }
            if !digit(s) {
                error = true;
            } else {
                while digit(s) {
                    s += 1;
                }
            }
        }
        while s < input.len() && alphanumeric(input[s]) {
            s += 1;
            error = true;
        }
        self.token_terminator = s;
        if error {
            Err(JsonError::InvalidToken)
        } else {
            Ok(())
        }
    }

    fn lex_string(&mut self, start: usize) -> Result<(), JsonError> {
        let input = self.input;
        let mut s = start;
        let mut high_surrogate: Option<u32> = None;
        loop {
            s += 1;
            if s >= input.len() {
                self.token_terminator = input.len();
                return Err(JsonError::InvalidToken);
            }
            match input[s] {
                b'"' => break,
                b'\\' => {
                    s += 1;
                    if s >= input.len() {
                        self.token_terminator = input.len();
                        return Err(JsonError::InvalidToken);
                    }
                    if input[s] == b'u' {
                        let mut ch = 0u32;
                        for _ in 0..4 {
                            s += 1;
                            if s >= input.len() {
                                self.token_terminator = input.len();
                                return Err(JsonError::InvalidToken);
                            }
                            match (input[s] as char).to_digit(16) {
                                Some(value) => ch = ch * 16 + value,
                                None => {
                                    return Err(
                                        self.fail_at_char_end(s, JsonError::UnicodeEscapeFormat)
                                    )
                                }
                            }
                        }
                        if self.strings {
                            if (0xD800..=0xDBFF).contains(&ch) {
                                if high_surrogate.is_some() {
                                    return Err(
                                        self.fail_at_char_end(s, JsonError::UnicodeHighSurrogate)
                                    );
                                }
                                high_surrogate = Some(ch);
                                continue;
                            } else if (0xDC00..=0xDFFF).contains(&ch) {
                                if high_surrogate.take().is_none() {
                                    return Err(
                                        self.fail_at_char_end(s, JsonError::UnicodeLowSurrogate)
                                    );
                                }
                                continue;
                            }
                            if high_surrogate.is_some() {
                                return Err(
                                    self.fail_at_char_end(s, JsonError::UnicodeLowSurrogate)
                                );
                            }
                            if ch == 0 {
                                return Err(
                                    self.fail_at_char_end(s, JsonError::UnicodeCodePointZero)
                                );
                            }
                        }
                    } else {
                        if self.strings && high_surrogate.is_some() {
                            return Err(self.fail_at_char_end(s, JsonError::UnicodeLowSurrogate));
                        }
                        if !b"\"\\/bfnrt".contains(&input[s]) {
                            self.token_start = Some(s);
                            return Err(self.fail_at_char_end(s, JsonError::EscapingInvalid));
                        }
                    }
                }
                byte => {
                    if self.strings && high_surrogate.is_some() {
                        return Err(self.fail_at_char_end(s, JsonError::UnicodeLowSurrogate));
                    }
                    if byte <= 31 {
                        self.token_terminator = s;
                        return Err(JsonError::EscapingRequired);
                    }
                }
            }
        }
        if self.strings && high_surrogate.is_some() {
            self.token_terminator = s + 1;
            return Err(JsonError::UnicodeLowSurrogate);
        }
        self.token_terminator = s + 1;
        Ok(())
    }

    fn report_parse_error(&self, context: ParseContext) -> JsonError {
        if self.token_start.is_none() || self.token == Token::End {
            return JsonError::ExpectedMore;
        }
        match context {
            ParseContext::End => JsonError::ExpectedEnd,
            ParseContext::Value => JsonError::ExpectedJson,
            ParseContext::String => JsonError::ExpectedString,
            ParseContext::ArrayNext => JsonError::ExpectedArrayNext,
            ParseContext::ObjectStart => JsonError::ExpectedObjectFirst,
            ParseContext::ObjectLabel => JsonError::ExpectedColon,
            ParseContext::ObjectNext => JsonError::ExpectedObjectNext,
        }
    }

    fn expect(&mut self, context: ParseContext, token: Token) -> Result<(), JsonError> {
        if self.token == token {
            self.lex()
        } else {
            Err(self.report_parse_error(context))
        }
    }

    fn parse_value(&mut self) -> Result<(), JsonError> {
        match self.token {
            Token::ObjectStart => self.parse_object(),
            Token::ArrayStart => self.parse_array(),
            Token::String | Token::Number | Token::True | Token::False | Token::Null => self.lex(),
            _ => Err(self.report_parse_error(ParseContext::Value)),
        }
    }

    fn parse_object_field(&mut self) -> Result<(), JsonError> {
        if self.token != Token::String {
            return Err(self.report_parse_error(ParseContext::String));
        }
        self.lex()?;
        self.expect(ParseContext::ObjectLabel, Token::Colon)?;
        self.parse_value()
    }

    fn parse_object(&mut self) -> Result<(), JsonError> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err(JsonError::TooDeep);
        }
        self.lex()?;
        match self.token {
            Token::String => {
                self.parse_object_field()?;
                while self.token == Token::Comma {
                    self.lex()?;
                    self.parse_object_field()?;
                }
            }
            Token::ObjectEnd => {}
            _ => return Err(self.report_parse_error(ParseContext::ObjectStart)),
        }
        self.expect(ParseContext::ObjectNext, Token::ObjectEnd)?;
        self.depth -= 1;
        Ok(())
    }

    fn parse_array(&mut self) -> Result<(), JsonError> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err(JsonError::TooDeep);
        }
        self.lex()?;
        if self.token != Token::ArrayEnd {
            self.parse_value()?;
            while self.token == Token::Comma {
                self.lex()?;
                self.parse_value()?;
            }
        }
        self.expect(ParseContext::ArrayNext, Token::ArrayEnd)?;
        self.depth -= 1;
        Ok(())
    }

    fn token_text(&self) -> String {
        let start = self.token_start.unwrap_or(self.token_terminator);
        String::from_utf8_lossy(
            &self.input[start.min(self.token_terminator)..self.token_terminator],
        )
        .into_owned()
    }

    fn detail(&self, error: JsonError) -> String {
        let token = self.token_text();
        match error {
            JsonError::InvalidToken => format!("Token \"{token}\" is invalid."),
            JsonError::EscapingInvalid => format!("Escape sequence \"\\{token}\" is invalid."),
            JsonError::EscapingRequired => format!(
                "Character with value 0x{:02x} must be escaped.",
                self.input.get(self.token_terminator).copied().unwrap_or(0)
            ),
            JsonError::UnicodeEscapeFormat => {
                "\"\\u\" must be followed by four hexadecimal digits.".to_string()
            }
            JsonError::UnicodeHighSurrogate => {
                "Unicode high surrogate must not follow a high surrogate.".to_string()
            }
            JsonError::UnicodeLowSurrogate => {
                "Unicode low surrogate must follow a high surrogate.".to_string()
            }
            JsonError::UnicodeCodePointZero => "\\u0000 cannot be converted to text.".to_string(),
            JsonError::ExpectedMore => "The input string ended unexpectedly.".to_string(),
            JsonError::ExpectedEnd => format!("Expected end of input, but found \"{token}\"."),
            JsonError::ExpectedJson => format!("Expected JSON value, but found \"{token}\"."),
            JsonError::ExpectedString => format!("Expected string, but found \"{token}\"."),
            JsonError::ExpectedArrayNext => {
                format!("Expected \",\" or \"]\", but found \"{token}\".")
            }
            JsonError::ExpectedObjectFirst => {
                format!("Expected string or \"}}\", but found \"{token}\".")
            }
            JsonError::ExpectedColon => format!("Expected \":\", but found \"{token}\"."),
            JsonError::ExpectedObjectNext => {
                format!("Expected \",\" or \"}}\", but found \"{token}\".")
            }
            JsonError::TooDeep => String::new(),
        }
    }

    /// `JSON data, line N: ...`: the current line up to the end of the
    /// offending token, at most about 50 bytes of it.
    fn context(&self) -> String {
        let line_start = self.line_start.min(self.token_terminator);
        let context_end = self.token_terminator;
        let mut context_start = line_start;
        while context_end - context_start >= 50 {
            context_start += char_len(self.input[context_start]);
        }
        if context_start - line_start <= 3 {
            context_start = line_start;
        }
        let text = String::from_utf8_lossy(&self.input[context_start..context_end]);
        let prefix = if context_start > line_start {
            "..."
        } else {
            ""
        };
        let suffix = if self.token != Token::End
            && context_end < self.input.len()
            && !matches!(self.input[context_end], b'\n' | b'\r')
        {
            "..."
        } else {
            ""
        };
        format!(
            "JSON data, line {}: {prefix}{text}{suffix}",
            self.line_number
        )
    }
}

/// PostgreSQL's error for invalid `json` (`strings` false) or `jsonb`
/// (`strings` true) input text, or `None` when its parser accepts the text.
pub(crate) fn pg_json_input_error(text: &str, strings: bool) -> Option<SqlError> {
    let mut lexer = Lexer {
        input: text.as_bytes(),
        strings,
        token_start: None,
        token_terminator: 0,
        token: Token::Invalid,
        line_number: 1,
        line_start: 0,
        depth: 0,
    };
    let result = lexer
        .lex()
        .and_then(|()| lexer.parse_value())
        .and_then(|()| lexer.expect(ParseContext::End, Token::End));
    let error = result.err()?;
    if error == JsonError::TooDeep {
        return Some(SqlError::with_detail_fields(
            SqlError::data_exception_public("54001", "stack depth limit exceeded", None),
            None,
            Some(crate::max_stack_depth::stack_depth_hint()),
            None,
        ));
    }
    let base = if error == JsonError::UnicodeCodePointZero {
        SqlError::data_exception_public("22P05", "unsupported Unicode escape sequence", None)
    } else {
        SqlError::data_exception_public("22P02", "invalid input syntax for type json", None)
    };
    Some(SqlError::with_detail_fields(
        base,
        Some(lexer.detail(error)),
        None,
        Some(lexer.context()),
    ))
}

/// The error for text the JSON reader rejected: PostgreSQL's diagnosis, or
/// `fallback` when PostgreSQL's parser would have accepted it.
pub(crate) fn json_input_error_or(text: &str, strings: bool, fallback: SqlError) -> SqlError {
    pg_json_input_error(text, strings).unwrap_or(fallback)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SqlErrorField;

    fn diagnose(text: &str) -> (String, Vec<SqlErrorField>) {
        let error = pg_json_input_error(text, false).expect("invalid");
        (error.to_string(), error.fields())
    }

    fn detail_and_context(text: &str) -> (String, String) {
        let (_, fields) = diagnose(text);
        let mut detail = String::new();
        let mut context = String::new();
        for field in fields {
            match field {
                SqlErrorField::Detail(value) => detail = value,
                SqlErrorField::Context(value) => context = value,
                _ => {}
            }
        }
        (detail, context)
    }

    #[test]
    fn json_input_errors_match_postgres() {
        for (text, detail, context) in [
            ("''", "Token \"'\" is invalid.", "JSON data, line 1: '..."),
            (
                "\"abc",
                "Token \"\"abc\" is invalid.",
                "JSON data, line 1: \"abc",
            ),
            (
                "\"abc\ndef\"",
                "Character with value 0x0a must be escaped.",
                "JSON data, line 1: \"abc",
            ),
            (
                "\"\\v\"",
                "Escape sequence \"\\v\" is invalid.",
                "JSON data, line 1: \"\\v...",
            ),
            ("01", "Token \"01\" is invalid.", "JSON data, line 1: 01"),
            ("1f2", "Token \"1f2\" is invalid.", "JSON data, line 1: 1f2"),
            (
                "0.x1",
                "Token \"0.x1\" is invalid.",
                "JSON data, line 1: 0.x1",
            ),
            (
                "[1,2,]",
                "Expected JSON value, but found \"]\".",
                "JSON data, line 1: [1,2,]",
            ),
            (
                "[1,2",
                "The input string ended unexpectedly.",
                "JSON data, line 1: [1,2",
            ),
            (
                "{\"abc\"}",
                "Expected \":\", but found \"}\".",
                "JSON data, line 1: {\"abc\"}",
            ),
            (
                "{1:\"abc\"}",
                "Expected string or \"}\", but found \"1\".",
                "JSON data, line 1: {1...",
            ),
            (
                "{\"abc\",1}",
                "Expected \":\", but found \",\".",
                "JSON data, line 1: {\"abc\",...",
            ),
            (
                "{\"abc\":1,3}",
                "Expected string, but found \"3\".",
                "JSON data, line 1: {\"abc\":1,3...",
            ),
            (
                "true false",
                "Expected end of input, but found \"false\".",
                "JSON data, line 1: true false",
            ),
            (
                "truf",
                "Token \"truf\" is invalid.",
                "JSON data, line 1: truf",
            ),
            (
                "",
                "The input string ended unexpectedly.",
                "JSON data, line 1: ",
            ),
        ] {
            assert_eq!(
                detail_and_context(text),
                (detail.to_string(), context.to_string()),
                "{text:?}"
            );
        }
        assert_eq!(diagnose("[1").0, "invalid input syntax for type json");
        assert!(pg_json_input_error("{\"a\":[1,2.5e3,null,true]}", false).is_none());
    }
}
