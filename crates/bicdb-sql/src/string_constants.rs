//! PostgreSQL string constants, decoded exactly as PostgreSQL's lexer does
//! before any other code sees the statement text.
//!
//! The parser and the many text rewrites that run before it understand
//! standard string constants (`'...'` with `''` for a quote) but not every
//! PostgreSQL form: escape strings (`E'...'`) decode differently in the
//! parser library (no surrogate pairs, no multi-byte `\x`/octal sequences),
//! hand-written scanners that skip quoted text end an escape string at `\'`,
//! and Unicode-escape strings (`U&'...' UESCAPE '!'`), string continuation
//! across lines and `standard_conforming_strings = off` are not understood at
//! all. Every such constant is therefore rewritten here into the standard
//! constant with the same value, so the statement that later stages see has
//! exactly one spelling for each string: what PostgreSQL decoded.
//!
//! Text without such constants is returned unchanged (borrowed).
use crate::{Result, SqlError};
use std::borrow::Cow;

/// How plain `'...'` constants are read, from `standard_conforming_strings`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PlainStrings {
    /// `standard_conforming_strings = on`: backslash is an ordinary character.
    Standard,
    /// `standard_conforming_strings = off`: backslash escapes, as in `E'...'`.
    BackslashEscapes,
}

fn syntax_error(message: impl Into<String>) -> SqlError {
    SqlError::data_exception_public("42601", message, None)
}

fn at_or_near(message: &str, near: &str) -> SqlError {
    if near.is_empty() {
        syntax_error(format!("{message} at end of input"))
    } else {
        syntax_error(format!("{message} at or near \"{near}\""))
    }
}

fn is_ident_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_' || byte >= 0x80
}

fn is_ident_cont(byte: u8) -> bool {
    is_ident_start(byte) || byte.is_ascii_digit() || byte == b'$'
}

/// The `$tag$` delimiter starting at `start`, if one does.
fn dollar_delimiter(bytes: &[u8], start: usize) -> Option<usize> {
    let mut index = start + 1;
    if bytes.get(index) == Some(&b'$') {
        return Some(index + 1);
    }
    if !bytes.get(index).is_some_and(|byte| is_ident_start(*byte)) {
        return None;
    }
    while bytes
        .get(index)
        .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_' || *byte >= 0x80)
    {
        index += 1;
    }
    (bytes.get(index) == Some(&b'$')).then_some(index + 1)
}

/// Where a `--` comment starting at `start` ends (before its newline).
fn line_comment_end(bytes: &[u8], start: usize) -> usize {
    let mut index = start;
    while index < bytes.len() && bytes[index] != b'\n' && bytes[index] != b'\r' {
        index += 1;
    }
    index
}

/// Where a (nesting) block comment starting at `start` ends, if it does.
fn block_comment_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut index = start;
    while index + 1 < bytes.len() {
        if bytes[index] == b'/' && bytes[index + 1] == b'*' {
            depth += 1;
            index += 2;
        } else if bytes[index] == b'*' && bytes[index + 1] == b'/' {
            depth -= 1;
            index += 2;
            if depth == 0 {
                return Some(index);
            }
        } else {
            index += 1;
        }
    }
    None
}

/// The position of the quote that continues a string constant closed just
/// before `start`: whitespace holding at least one newline (comments
/// allowed), then a quote, as PostgreSQL's `quotecontinue` rule reads it.
fn continuation_quote(bytes: &[u8], start: usize) -> Option<usize> {
    let mut index = start;
    loop {
        match bytes.get(index) {
            Some(b' ' | b'\t' | b'\x0c') => index += 1,
            Some(b'-') if bytes.get(index + 1) == Some(&b'-') => {
                index = line_comment_end(bytes, index)
            }
            Some(b'\n' | b'\r') => break,
            _ => return None,
        }
    }
    loop {
        match bytes.get(index) {
            Some(b' ' | b'\t' | b'\n' | b'\r' | b'\x0c' | b'\x0b') => index += 1,
            Some(b'-') if bytes.get(index + 1) == Some(&b'-') => {
                let end = line_comment_end(bytes, index);
                if !matches!(bytes.get(end), Some(b'\n' | b'\r')) {
                    return None;
                }
                index = end;
            }
            Some(b'\'') => return Some(index),
            _ => return None,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Plain,
    Escape,
    Unicode,
}

struct Decoded {
    /// Decoded bytes of the constant (validated as UTF-8 before use).
    value: Vec<u8>,
    /// One past the closing quote of the last continued segment.
    end: usize,
    /// Whether the source spelling differs from the standard spelling.
    changed: bool,
    /// Whether any escape produced a zero or non-ASCII byte.
    saw_non_ascii: bool,
}

fn hex_digit(byte: u8) -> Option<u32> {
    (byte as char).to_digit(16)
}

fn is_surrogate_first(code: u32) -> bool {
    (0xD800..=0xDBFF).contains(&code)
}

fn is_surrogate_second(code: u32) -> bool {
    (0xDC00..=0xDFFF).contains(&code)
}

fn surrogate_pair(first: u32, second: u32) -> u32 {
    ((first & 0x3FF) << 10) + 0x10000 + (second & 0x3FF)
}

fn push_code_point(value: &mut Vec<u8>, code: u32, near: &str) -> Result<()> {
    let character = (code > 0)
        .then(|| char::from_u32(code))
        .flatten()
        .ok_or_else(|| at_or_near("invalid Unicode escape value", near))?;
    let mut buffer = [0u8; 4];
    value.extend_from_slice(character.encode_utf8(&mut buffer).as_bytes());
    Ok(())
}

/// The source text from `start` to the end of the character there.
fn char_text(sql: &str, start: usize) -> &str {
    sql.get(start..)
        .and_then(|rest| rest.chars().next().map(|c| &rest[..c.len_utf8()]))
        .unwrap_or("")
}

/// Decodes the constant whose opening quote is at `quote`. `None` when it is
/// unterminated (the parser then reports that).
fn decode(sql: &str, quote: usize, kind: Kind) -> Result<Option<Decoded>> {
    let bytes = sql.as_bytes();
    let mut value = Vec::new();
    let mut index = quote + 1;
    let mut changed = kind != Kind::Plain;
    let mut saw_non_ascii = false;
    let mut pending_first: Option<u32> = None;
    loop {
        let Some(&byte) = bytes.get(index) else {
            return Ok(None);
        };
        if let Some(first) = pending_first {
            // An escape-string surrogate must be followed by its pair.
            match xeunicode(bytes, index) {
                Some((second, length)) if is_surrogate_second(second) => {
                    let near = &sql[index..index + length];
                    push_code_point(&mut value, surrogate_pair(first, second), near)?;
                    pending_first = None;
                    index += length;
                    continue;
                }
                Some((_, length)) => {
                    return Err(at_or_near(
                        "invalid Unicode surrogate pair",
                        &sql[index..index + length],
                    ))
                }
                None if byte == b'\\' && matches!(bytes.get(index + 1), Some(b'u' | b'U')) => {
                    return Err(invalid_escape_e());
                }
                None => {
                    return Err(at_or_near(
                        "invalid Unicode surrogate pair",
                        char_text(sql, index),
                    ))
                }
            }
        }
        if byte == b'\'' {
            if bytes.get(index + 1) == Some(&b'\'') {
                value.push(b'\'');
                index += 2;
                continue;
            }
            index += 1;
            // A constant continued on the next line is one constant.
            if let Some(next) = continuation_quote(bytes, index) {
                changed = true;
                index = next + 1;
                continue;
            }
            return Ok(Some(Decoded {
                value,
                end: index,
                changed,
                saw_non_ascii,
            }));
        }
        if byte != b'\\' || kind != Kind::Escape {
            value.push(byte);
            index += 1;
            continue;
        }
        changed = true;
        let Some(&next) = bytes.get(index + 1) else {
            return Ok(None);
        };
        match next {
            b'b' => value.push(0x08),
            b'f' => value.push(0x0c),
            b'n' => value.push(b'\n'),
            b'r' => value.push(b'\r'),
            b't' => value.push(b'\t'),
            b'v' => value.push(0x0b),
            b'0'..=b'7' => {
                let mut code = u32::from(next - b'0');
                let mut length = 2;
                while length < 4 {
                    match bytes.get(index + length) {
                        Some(digit @ b'0'..=b'7') => {
                            code = code * 8 + u32::from(digit - b'0');
                            length += 1;
                        }
                        _ => break,
                    }
                }
                let byte = (code & 0xFF) as u8;
                saw_non_ascii |= byte == 0 || byte >= 0x80;
                value.push(byte);
                index += length;
                continue;
            }
            b'x' if bytes.get(index + 2).and_then(|b| hex_digit(*b)).is_some() => {
                let mut code = hex_digit(bytes[index + 2]).unwrap_or(0);
                let mut length = 3;
                if let Some(digit) = bytes.get(index + 3).and_then(|b| hex_digit(*b)) {
                    code = code * 16 + digit;
                    length = 4;
                }
                let byte = code as u8;
                saw_non_ascii |= byte == 0 || byte >= 0x80;
                value.push(byte);
                index += length;
                continue;
            }
            b'u' | b'U' => {
                let (code, length) = xeunicode(bytes, index).ok_or_else(invalid_escape_e)?;
                let near = &sql[index..index + length];
                if is_surrogate_first(code) {
                    pending_first = Some(code);
                } else if is_surrogate_second(code) {
                    return Err(at_or_near("invalid Unicode surrogate pair", near));
                } else {
                    push_code_point(&mut value, code, near)?;
                }
                index += length;
                continue;
            }
            other => {
                // Any other escaped byte stands for itself.
                saw_non_ascii |= other == 0 || other >= 0x80;
                value.push(other);
            }
        }
        index += 2;
    }
}

/// The `\uXXXX` or `\UXXXXXXXX` escape at `start`: its value and length.
fn xeunicode(bytes: &[u8], start: usize) -> Option<(u32, usize)> {
    if bytes.get(start) != Some(&b'\\') {
        return None;
    }
    let digits = match bytes.get(start + 1) {
        Some(b'u') => 4,
        Some(b'U') => 8,
        _ => return None,
    };
    let code = (0..digits).try_fold(0u32, |code, offset| {
        bytes
            .get(start + 2 + offset)
            .and_then(|b| hex_digit(*b))
            .map(|digit| code.wrapping_mul(16).wrapping_add(digit))
    })?;
    Some((code, 2 + digits))
}

fn invalid_escape_e() -> SqlError {
    SqlError::data_exception_public("22025", "invalid Unicode escape", None)
}

/// PostgreSQL's encoding check of a decoded constant.
fn verify_utf8(value: Vec<u8>) -> Result<String> {
    String::from_utf8(value).map_err(|error| {
        let bytes = error.as_bytes();
        let start = error.utf8_error().valid_up_to();
        let lead = bytes[start];
        let length = match lead {
            0xC0..=0xDF => 2,
            0xE0..=0xEF => 3,
            0xF0..=0xF7 => 4,
            _ => 1,
        }
        .min(bytes.len() - start);
        let shown = bytes[start..start + length]
            .iter()
            .map(|byte| format!("0x{byte:02x}"))
            .collect::<Vec<_>>()
            .join(" ");
        SqlError::data_exception_public(
            "22021",
            format!("invalid byte sequence for encoding \"UTF8\": {shown}"),
            None,
        )
    })
}

fn check_nul(value: &str) -> Result<()> {
    if value.contains('\0') {
        return Err(SqlError::data_exception_public(
            "22021",
            "invalid byte sequence for encoding \"UTF8\": 0x00",
            None,
        ));
    }
    Ok(())
}

/// Applies `U&'...'` escapes with escape character `escape`.
fn decode_unicode_escapes(text: &str, escape: char) -> Result<String> {
    let chars = text.chars().collect::<Vec<_>>();
    let mut out = String::with_capacity(text.len());
    let mut index = 0;
    let mut pending_first: Option<u32> = None;
    let invalid_pair = || syntax_error("invalid Unicode surrogate pair");
    let hex = |from: usize, count: usize| -> Option<u32> {
        (0..count).try_fold(0u32, |code, offset| {
            chars
                .get(from + offset)
                .and_then(|c| c.to_digit(16))
                .map(|digit| code * 16 + digit)
        })
    };
    while index < chars.len() {
        if chars[index] != escape {
            if pending_first.is_some() {
                return Err(invalid_pair());
            }
            out.push(chars[index]);
            index += 1;
            continue;
        }
        let (code, length) = if chars.get(index + 1) == Some(&escape) {
            if pending_first.is_some() {
                return Err(invalid_pair());
            }
            out.push(escape);
            index += 2;
            continue;
        } else if let Some(code) = hex(index + 1, 4) {
            (code, 5)
        } else if chars.get(index + 1) == Some(&'+') {
            match hex(index + 2, 6) {
                Some(code) => (code, 8),
                None => return Err(invalid_escape_u()),
            }
        } else {
            return Err(invalid_escape_u());
        };
        if code == 0 || code > 0x10FFFF {
            return Err(syntax_error("invalid Unicode escape value"));
        }
        let code = match pending_first.take() {
            Some(first) if is_surrogate_second(code) => surrogate_pair(first, code),
            Some(_) => return Err(invalid_pair()),
            None if is_surrogate_first(code) => {
                pending_first = Some(code);
                index += length;
                continue;
            }
            None if is_surrogate_second(code) => return Err(invalid_pair()),
            None => code,
        };
        out.push(char::from_u32(code).ok_or_else(|| syntax_error("invalid Unicode escape value"))?);
        index += length;
    }
    if pending_first.is_some() {
        return Err(invalid_pair());
    }
    Ok(out)
}

fn invalid_escape_u() -> SqlError {
    syntax_error("invalid Unicode escape")
}

/// Skips whitespace and comments from `start`.
fn skip_space(bytes: &[u8], mut index: usize) -> usize {
    loop {
        match bytes.get(index) {
            Some(b' ' | b'\t' | b'\n' | b'\r' | b'\x0c' | b'\x0b') => index += 1,
            Some(b'-') if bytes.get(index + 1) == Some(&b'-') => {
                index = line_comment_end(bytes, index)
            }
            Some(b'/') if bytes.get(index + 1) == Some(&b'*') => {
                match block_comment_end(bytes, index) {
                    Some(end) => index = end,
                    None => return index,
                }
            }
            _ => return index,
        }
    }
}

/// A `UESCAPE 'c'` clause after a Unicode-escape constant ending at `start`:
/// its escape character and where the clause ends.
fn uescape_clause(sql: &str, start: usize) -> Result<Option<(char, usize)>> {
    let bytes = sql.as_bytes();
    let keyword = skip_space(bytes, start);
    let is_uescape = sql
        .get(keyword..keyword + 7)
        .is_some_and(|word| word.eq_ignore_ascii_case("uescape"))
        && !bytes.get(keyword + 7).is_some_and(|b| is_ident_cont(*b));
    if !is_uescape {
        return Ok(None);
    }
    let literal = skip_space(bytes, keyword + 7);
    let decoded = match bytes.get(literal) {
        Some(b'\'') => decode(sql, literal, Kind::Plain)?,
        Some(b'e' | b'E') if bytes.get(literal + 1) == Some(&b'\'') => {
            decode(sql, literal + 1, Kind::Escape)?
        }
        _ => None,
    };
    let Some(decoded) = decoded else {
        return Err(at_or_near(
            "UESCAPE must be followed by a simple string literal",
            token_text(sql, literal),
        ));
    };
    let text = verify_utf8(decoded.value)?;
    let mut chars = text.chars();
    match (chars.next(), chars.next()) {
        (Some(escape), None)
            if !escape.is_ascii_hexdigit()
                && !matches!(
                    escape,
                    '+' | '\'' | '"' | ' ' | '\t' | '\n' | '\r' | '\x0c' | '\x0b'
                ) =>
        {
            Ok(Some((escape, decoded.end)))
        }
        _ => Err(at_or_near(
            "invalid Unicode escape character",
            &sql[literal..decoded.end],
        )),
    }
}

/// The text of the token starting at `start`, for error messages.
fn token_text(sql: &str, start: usize) -> &str {
    let bytes = sql.as_bytes();
    let mut end = start;
    while bytes.get(end).is_some_and(|b| is_ident_cont(*b)) {
        end += 1;
    }
    if end == start {
        return char_text(sql, start);
    }
    &sql[start..end]
}

fn unterminated(sql: &str, start: usize) -> SqlError {
    at_or_near("unterminated quoted string", &sql[start..])
}

/// PostgreSQL has no implicit concatenation of constants that are not
/// separated by a newline (that is continuation, already joined): the second
/// one is a syntax error, not part of the first.
fn check_not_adjacent(sql: &str, end: usize) -> Result<()> {
    let bytes = sql.as_bytes();
    let next = skip_space(bytes, end);
    let quote = match bytes.get(next) {
        Some(b'\'') => next,
        Some(b'e' | b'E') if bytes.get(next + 1) == Some(&b'\'') => next + 1,
        _ => return Ok(()),
    };
    let kind = if quote == next {
        Kind::Plain
    } else {
        Kind::Escape
    };
    let near_end = match decode(sql, quote, kind) {
        Ok(Some(decoded)) => decoded.end,
        _ => sql.len(),
    };
    Err(at_or_near("syntax error", &sql[next..near_end]))
}

/// The constant spelled so that it reads back as `value` under `plain`:
/// a standard constant, or (backslash escapes on) an escape string when the
/// value holds a backslash, so normalizing the output again changes nothing.
fn standard_constant(value: &str, plain: PlainStrings) -> String {
    let quoted = value.replace('\'', "''");
    if plain == PlainStrings::BackslashEscapes && value.contains('\\') {
        format!("E'{}'", quoted.replace('\\', "\\\\"))
    } else {
        format!("'{quoted}'")
    }
}

/// Whether the session reads plain constants with backslash escapes.
pub(crate) fn plain_strings(gucs: &std::collections::HashMap<String, String>) -> PlainStrings {
    match gucs.get("standard_conforming_strings").map(String::as_str) {
        Some("off") => PlainStrings::BackslashEscapes,
        _ => PlainStrings::Standard,
    }
}

/// Client statement text with its string constants normalized for the
/// session's `standard_conforming_strings` (PostgreSQL profile only; the
/// Oracle front end has its own literal rules and lowers them itself).
pub fn normalize_client_string_constants<'a>(
    sql: &'a str,
    gucs: &std::collections::HashMap<String, String>,
) -> Result<Cow<'a, str>> {
    if gucs
        .get("bicdb.sql_profile")
        .is_some_and(|profile| crate::oracle::is_oracle_profile(profile))
    {
        return Ok(Cow::Borrowed(sql));
    }
    normalize_string_constants(sql, plain_strings(gucs))
}

/// `sql` with every string constant in PostgreSQL's standard spelling (see
/// the module documentation), or PostgreSQL's error for a malformed one.
pub(crate) fn normalize_string_constants(sql: &str, plain: PlainStrings) -> Result<Cow<'_, str>> {
    let bytes = sql.as_bytes();
    // Fast path: nothing to decode without a quote (or a `U&"..."` name).
    if !bytes.iter().any(|byte| matches!(byte, b'\'' | b'&')) {
        return Ok(Cow::Borrowed(sql));
    }
    let mut out = Writer {
        sql,
        out: None,
        copied: 0,
    };
    let mut index = 0usize;
    while index < bytes.len() {
        let byte = bytes[index];
        match byte {
            b'-' if bytes.get(index + 1) == Some(&b'-') => index = line_comment_end(bytes, index),
            b'/' if bytes.get(index + 1) == Some(&b'*') => match block_comment_end(bytes, index) {
                Some(end) => index = end,
                None => break,
            },
            b'"' => {
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == b'"' {
                        if bytes.get(index + 1) == Some(&b'"') {
                            index += 2;
                            continue;
                        }
                        break;
                    }
                    index += 1;
                }
                index += 1;
            }
            b'$' => match dollar_delimiter(bytes, index) {
                Some(after) => {
                    let delimiter = &sql[index..after];
                    match sql[after..].find(delimiter) {
                        Some(offset) => index = after + offset + delimiter.len(),
                        None => break,
                    }
                }
                None => {
                    index += 1;
                    while bytes.get(index).is_some_and(|b| b.is_ascii_digit()) {
                        index += 1;
                    }
                }
            },
            b'0'..=b'9' => {
                while bytes
                    .get(index)
                    .is_some_and(|b| is_ident_cont(*b) || *b == b'.')
                {
                    index += 1;
                }
            }
            b'\'' => {
                let kind = match plain {
                    PlainStrings::Standard => Kind::Plain,
                    PlainStrings::BackslashEscapes => Kind::Escape,
                };
                let Some(decoded) = decode(sql, index, kind)? else {
                    return Err(unterminated(sql, index));
                };
                let end = decoded.end;
                let changed = decoded.changed;
                let value = verify_utf8(decoded.value)?;
                check_nul(&value)?;
                let text = standard_constant(&value, plain);
                if changed && text != sql[index..end] {
                    out.replace(index, end, &text);
                }
                check_not_adjacent(sql, end)?;
                index = end;
            }
            byte if is_ident_start(byte) => {
                let start = index;
                while bytes.get(index).is_some_and(|b| is_ident_cont(*b)) {
                    index += 1;
                }
                let word = &sql[start..index];
                let next = bytes.get(index).copied();
                if word.eq_ignore_ascii_case("e") && next == Some(b'\'') {
                    let Some(decoded) = decode(sql, index, Kind::Escape)? else {
                        return Err(unterminated(sql, start));
                    };
                    let saw_non_ascii = decoded.saw_non_ascii;
                    let end = decoded.end;
                    let value = verify_utf8(decoded.value)?;
                    if saw_non_ascii {
                        check_nul(&value)?;
                    }
                    out.replace(start, end, &standard_constant(&value, plain));
                    check_not_adjacent(sql, end)?;
                    index = end;
                } else if word.eq_ignore_ascii_case("u")
                    && next == Some(b'&')
                    && matches!(bytes.get(index + 1), Some(b'\'' | b'"'))
                {
                    let quote = index + 1;
                    if bytes[quote] == b'\'' {
                        if plain == PlainStrings::BackslashEscapes {
                            return Err(SqlError::data_exception_public(
                                "0A000",
                                "unsafe use of string constant with Unicode escapes",
                                None,
                            ));
                        }
                        let Some(decoded) = decode(sql, quote, Kind::Unicode)? else {
                            return Err(unterminated(sql, start));
                        };
                        let raw = verify_utf8(decoded.value)?;
                        let (escape, end) =
                            uescape_clause(sql, decoded.end)?.unwrap_or(('\\', decoded.end));
                        let value = decode_unicode_escapes(&raw, escape)?;
                        out.replace(start, end, &standard_constant(&value, plain));
                        check_not_adjacent(sql, end)?;
                        index = end;
                    } else {
                        // U&"..." identifier.
                        let mut close = quote + 1;
                        let mut raw = String::new();
                        loop {
                            match bytes.get(close) {
                                None => return Ok(out.finish()),
                                Some(b'"') if bytes.get(close + 1) == Some(&b'"') => {
                                    raw.push('"');
                                    close += 2;
                                }
                                Some(b'"') => break,
                                Some(_) => {
                                    let text = char_text(sql, close);
                                    raw.push_str(text);
                                    close += text.len();
                                }
                            }
                        }
                        let (escape, end) =
                            uescape_clause(sql, close + 1)?.unwrap_or(('\\', close + 1));
                        let value = decode_unicode_escapes(&raw, escape)?;
                        if value.is_empty() {
                            return Err(at_or_near(
                                "zero-length delimited identifier",
                                &sql[start..close + 1],
                            ));
                        }
                        out.replace(start, end, &format!("\"{}\"", value.replace('"', "\"\"")));
                        index = end;
                    }
                } else if matches!(word, "b" | "B" | "x" | "X") && next == Some(b'\'') {
                    // Bit-string constants hold no escapes.
                    index += 1;
                    while index < bytes.len() && bytes[index] != b'\'' {
                        index += 1;
                    }
                    index += 1;
                } else if matches!(word, "n" | "N")
                    && next == Some(b'\'')
                    && plain == PlainStrings::BackslashEscapes
                {
                    let Some(decoded) = decode(sql, index, Kind::Escape)? else {
                        return Err(unterminated(sql, start));
                    };
                    let end = decoded.end;
                    let value = verify_utf8(decoded.value)?;
                    check_nul(&value)?;
                    // `N'...'` is a bpchar constant; an escape string cannot
                    // carry the N prefix.
                    let text = if value.contains('\\') {
                        format!("{}::bpchar", standard_constant(&value, plain))
                    } else {
                        format!("N{}", standard_constant(&value, plain))
                    };
                    out.replace(start, end, &text);
                    index = end;
                }
            }
            _ => index += 1,
        }
    }
    Ok(out.finish())
}

struct Writer<'a> {
    sql: &'a str,
    out: Option<String>,
    copied: usize,
}

impl<'a> Writer<'a> {
    fn replace(&mut self, from: usize, to: usize, text: &str) {
        let sql = self.sql;
        let buffer = self
            .out
            .get_or_insert_with(|| String::with_capacity(sql.len() + 16));
        buffer.push_str(&sql[self.copied..from]);
        buffer.push_str(text);
        self.copied = to;
    }

    fn finish(self) -> Cow<'a, str> {
        match self.out {
            Some(mut out) => {
                out.push_str(&self.sql[self.copied..]);
                Cow::Owned(out)
            }
            None => Cow::Borrowed(self.sql),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn standard(sql: &str) -> String {
        normalize_string_constants(sql, PlainStrings::Standard)
            .unwrap()
            .into_owned()
    }

    fn error(sql: &str, plain: PlainStrings) -> (String, String) {
        let error = normalize_string_constants(sql, plain).unwrap_err();
        (error.sqlstate().to_string(), error.to_string())
    }

    #[test]
    fn escape_strings_become_standard_constants() {
        assert_eq!(standard(r"SELECT E'a\\b'"), r"SELECT 'a\b'");
        assert_eq!(standard(r"SELECT E'''1 \\''2'''"), r"SELECT '''1 \''2'''");
        assert_eq!(
            standard(r"SELECT e'a\'b', E'a''b'"),
            "SELECT 'a''b', 'a''b'"
        );
        assert_eq!(
            standard(r"SELECT E'\n\t\r\b\f\v'"),
            "SELECT '\n\t\r\u{8}\u{c}\u{b}'"
        );
        assert_eq!(
            standard(r"SELECT E'\101\x42\x4G\q\%'"),
            "SELECT 'AB\u{4}Gq%'"
        );
        assert_eq!(standard(r"SELECT E'\303\251\xc3\xa9'"), "SELECT 'éé'");
        assert_eq!(
            standard(r"SELECT E'\u00e9\U0001F600\uD83D\uDE00'"),
            "SELECT 'é😀😀'"
        );
        assert_eq!(standard(r"SELECT E'\x'"), "SELECT 'x'");
    }

    #[test]
    fn escape_string_errors_match_postgresql() {
        let plain = PlainStrings::Standard;
        assert_eq!(error(r"SELECT E'\u12'", plain).0, "22025");
        assert_eq!(error(r"SELECT E'\U0001F6'", plain).0, "22025");
        assert_eq!(
            error(r"SELECT E'\uD83D'", plain),
            (
                "42601".into(),
                "invalid Unicode surrogate pair at or near \"'\"".into()
            )
        );
        assert_eq!(
            error(r"SELECT E'\uDE00'", plain).1,
            "invalid Unicode surrogate pair at or near \"\\uDE00\""
        );
        assert_eq!(
            error(r"SELECT E'\u0000'", plain).1,
            "invalid Unicode escape value at or near \"\\u0000\""
        );
        assert_eq!(
            error(r"SELECT E'\0'", plain),
            (
                "22021".into(),
                "invalid byte sequence for encoding \"UTF8\": 0x00".into()
            )
        );
        assert_eq!(
            error(r"SELECT E'\xc3'", plain).1,
            "invalid byte sequence for encoding \"UTF8\": 0xc3"
        );
    }

    #[test]
    fn plain_strings_follow_standard_conforming_strings() {
        assert_eq!(standard(r"SELECT 'a\b', 'it''s'"), r"SELECT 'a\b', 'it''s'");
        let off = |sql| {
            normalize_string_constants(sql, PlainStrings::BackslashEscapes)
                .unwrap()
                .into_owned()
        };
        assert_eq!(off(r"SELECT 'a\\b', 'a\'b'"), r"SELECT E'a\\b', 'a''b'");
        // The output reads back the same under either setting.
        assert_eq!(off(r"SELECT E'a\\b', 'a''b'"), r"SELECT E'a\\b', 'a''b'");
        assert_eq!(standard(r"SELECT E'a\\b'"), r"SELECT 'a\b'");
        assert_eq!(off(r"SELECT N'x\\y'"), r"SELECT E'x\\y'::bpchar");
        assert_eq!(off(r"SELECT N'x\ty'"), "SELECT N'x\ty'");
        assert_eq!(
            error(r"SELECT U&'x'", PlainStrings::BackslashEscapes).0,
            "0A000"
        );
    }

    #[test]
    fn unicode_escape_strings_and_identifiers() {
        assert_eq!(standard(r"SELECT U&'d\0061t\+000061'"), "SELECT 'data'");
        assert_eq!(
            standard(r"SELECT U&'d!0061t!+000061!!' UESCAPE '!'"),
            "SELECT 'data!'"
        );
        assert_eq!(standard(r"SELECT u&'a\\b''c'"), r"SELECT 'a\b''c'");
        assert_eq!(
            standard(r#"SELECT 1 AS U&"d\0061t""#),
            r#"SELECT 1 AS "dat""#
        );
        assert_eq!(standard(r"SELECT U&'\D83D\DE00'"), "SELECT '😀'");
        assert_eq!(error(r"SELECT U&'\zz'", PlainStrings::Standard).0, "42601");
        assert_eq!(
            error(r"SELECT U&'x' UESCAPE '+'", PlainStrings::Standard).1,
            "invalid Unicode escape character at or near \"'+'\""
        );
        assert_eq!(
            error(r"SELECT U&'\D83D'", PlainStrings::Standard).1,
            "invalid Unicode surrogate pair"
        );
    }

    #[test]
    fn unterminated_constants_report_postgresql_errors() {
        assert_eq!(
            error(r"SELECT 'a\b', 'a\''", PlainStrings::Standard).1,
            "unterminated quoted string at or near \"'a\\''\""
        );
        assert_eq!(
            error(r"SELECT E'a\'", PlainStrings::Standard).1,
            "unterminated quoted string at or near \"E'a\\'\""
        );
        assert_eq!(
            error(r"SELECT U&'x' UESCAPE 1", PlainStrings::Standard).1,
            "UESCAPE must be followed by a simple string literal at or near \"1\""
        );
    }

    #[test]
    fn continuation_joins_constants_across_lines() {
        assert_eq!(standard("SELECT 'a'\n  'b'"), "SELECT 'ab'");
        assert_eq!(standard("SELECT 'a' -- c\n'b'"), "SELECT 'ab'");
        assert_eq!(standard("SELECT E'a\\n'\n'\\n'"), "SELECT 'a\n\n'");
        // Without a newline it is two constants: a syntax error.
        assert_eq!(
            error("SELECT 'a' 'b'", PlainStrings::Standard),
            ("42601".into(), "syntax error at or near \"'b'\"".into())
        );
        assert_eq!(
            error("SELECT 'a'\n/* c */ E'b'", PlainStrings::Standard).1,
            "syntax error at or near \"E'b'\""
        );
    }

    #[test]
    fn other_quoted_text_is_left_alone() {
        for sql in [
            r#"SELECT "E'x\\'" FROM t"#,
            r"SELECT $$E'a\\b'$$, $q$ E'\n' $q$",
            "SELECT 1 -- E'\\n'\n",
            "SELECT /* E'\\n' /* nested */ */ 1",
            r"SELECT B'101', X'1F', somE'x'",
            r"SELECT $1, a$b",
        ] {
            assert_eq!(standard(sql), sql, "{sql}");
        }
    }
}
