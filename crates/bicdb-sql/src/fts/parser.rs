//! Raw token reporting for the default parser. The text exposed by ts_parse
//! includes punctuation and overlapping tokens, which are not stored lexemes.

use super::{classify_word, scan_while, TextToken};
use unicode_categories::UnicodeCategories;

use crate::{BicDb, Result, SqlError, SqlValue};

pub(crate) fn resolve(db: &BicDb, parser: &SqlValue) -> Result<()> {
    if let SqlValue::Int(oid) = parser {
        return if *oid == 3722 {
            Ok(())
        } else {
            Err(SqlError::data_exception_public(
                "XX000",
                format!("cache lookup failed for text search parser {oid}"),
                None,
            ))
        };
    }

    let input = parser.to_cell();
    let parts = crate::virtual_tables::sql_identifier_parts(input.trim())
        .map_err(|_| SqlError::data_exception_public("42602", "invalid name syntax", None))?;
    if parts.is_empty() {
        return Err(SqlError::data_exception_public(
            "42602",
            "invalid name syntax",
            None,
        ));
    }
    match parts.as_slice() {
        [name] if name == "default" => return Ok(()),
        [schema, name] => {
            if schema == "pg_catalog" && name == "default" {
                return Ok(());
            }
            if !matches!(schema.as_str(), "pg_catalog" | "information_schema")
                && crate::schema_meta::load_namespace(db, schema)?.is_none()
            {
                return Err(SqlError::data_exception_public(
                    "3F000",
                    format!("schema \"{schema}\" does not exist"),
                    None,
                ));
            }
        }
        [..] if parts.len() > 2 => {
            return Err(SqlError::data_exception_public(
                "0A000",
                format!(
                    "cross-database references are not implemented: {}",
                    parts.join(".")
                ),
                None,
            ));
        }
        _ => {}
    }
    Err(SqlError::undefined_object(format!(
        "text search parser \"{}\" does not exist",
        parts.join("."),
    )))
}

pub(crate) fn tokens(mut input: &str) -> Vec<(u8, &str)> {
    let mut rows = Vec::new();
    let mut blank = None;
    while !input.is_empty() {
        if input.starts_with("...") {
            blank.get_or_insert(input);
            input = input.trim_start_matches('.');
            continue;
        }
        if let Some((kind, end)) = token(input) {
            flush_blank(&mut rows, &mut blank, input);
            let text = &input[..end];
            rows.push((kind, text));
            if matches!(kind, 15..=17) {
                expand_hyphenated(&mut rows, text);
            } else if kind == 5 {
                if let Some(slash) = text.find('/') {
                    rows.push((6, &text[..slash]));
                    rows.push((18, &text[slash..]));
                }
            }
            input = &input[end..];
        } else {
            let character = input.chars().next().unwrap();
            if matches!(character, '+' | '-' | '<' | '/' | '&') {
                flush_blank(&mut rows, &mut blank, input);
            }
            blank.get_or_insert(input);
            input = &input[character.len_utf8()..];
        }
    }
    flush_blank(&mut rows, &mut blank, input);
    rows
}

fn flush_blank<'a>(rows: &mut Vec<(u8, &'a str)>, blank: &mut Option<&'a str>, rest: &str) {
    if let Some(start) = blank.take() {
        rows.push((12, &start[..start.len() - rest.len()]));
    }
}

fn expand_hyphenated<'a>(rows: &mut Vec<(u8, &'a str)>, mut word: &'a str) {
    while let Some((part, rest)) = word.split_once('-') {
        rows.push((TextToken::HyphenPart.kind(part), part));
        rows.push((12, &word[part.len()..part.len() + 1]));
        word = rest;
    }
    rows.push((TextToken::HyphenPart.kind(word), word));
}

fn token(input: &str) -> Option<(u8, usize)> {
    let first = input.chars().next()?;
    match first {
        '<' => return tag(input).map(|end| (13, end)),
        '&' => return entity(input).map(|end| (23, end)),
        '+' | '-' => return number(input),
        '/' | '.' => return standalone_path(input).map(|end| (19, end)),
        _ if !first.is_alphanumeric() => return None,
        _ => {}
    }

    let word_end = scan_while(input, 0, word_character);
    if input[..word_end]
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric())
        && input[word_end..].starts_with("://")
    {
        return Some((14, word_end + 3));
    }

    if let Some(end) = email(input, word_end) {
        return Some((4, end));
    }
    if let Some((kind, end)) = number(input) {
        if kind != 22 || end == word_end {
            return Some((kind, end));
        }
    }
    let dotted_end = dotted_name_end(input, word_end);
    if dotted_end > word_end {
        let top_label = input[..dotted_end].rsplit('.').next().unwrap_or_default();
        if top_label.len() >= 2 && top_label.bytes().all(|byte| byte.is_ascii_alphabetic()) {
            let host_end = if input[dotted_end..].starts_with(':') {
                let port_end = scan_while(input, dotted_end + 1, |ch| ch.is_ascii_digit());
                if port_end > dotted_end + 1 {
                    port_end
                } else {
                    dotted_end
                }
            } else {
                dotted_end
            };
            return Some(path_end(input, host_end, true).map_or((6, host_end), |end| (5, end)));
        }
        let first_label = input[..dotted_end].split('.').next().unwrap_or_default();
        if !first_label.contains('-') {
            let end = path_end(input, dotted_end, false).unwrap_or(dotted_end);
            return Some((19, end));
        }
    }
    if let Some(end) = path_end(input, word_end, false) {
        return Some((19, end));
    }
    if let Some(end) = hyphenated_word_end(input, word_end) {
        return Some((TextToken::Hyphenated.kind(&input[..end]), end));
    }
    Some((classify_word(&input[..word_end]), word_end))
}

fn word_character(ch: char) -> bool {
    ch.is_alphanumeric() || ch.is_mark()
}

fn number(input: &str) -> Option<(u8, usize)> {
    let signed = input.starts_with(['+', '-']);
    let start = usize::from(signed);
    let integer_end = scan_while(input, start, |ch| ch.is_ascii_digit());
    if integer_end == start {
        return None;
    }
    let mut end = integer_end;
    let mut fractions = 0;
    while input[end..].starts_with('.') {
        let portion_end = scan_while(input, end + 1, |ch| ch.is_ascii_digit());
        if portion_end == end + 1 {
            break;
        }
        if signed && fractions > 0 {
            return None;
        }
        fractions += 1;
        end = portion_end;
    }
    if fractions <= 1 && input[end..].starts_with(['e', 'E']) {
        let exponent = &input[end + 1..];
        let start = end + 1 + usize::from(exponent.starts_with(['+', '-']));
        let finish = scan_while(input, start, |ch| ch.is_ascii_digit());
        if finish != start {
            return Some((7, finish));
        }
    }
    let kind = match (fractions, signed) {
        (0, true) => 21,
        (0, false) => 22,
        (1, _) => 20,
        _ => 8,
    };
    Some((kind, end))
}

fn hyphenated_word_end(input: &str, first_end: usize) -> Option<usize> {
    let mut end = first_end;
    while input[end..].starts_with('-') {
        let part = end + 1;
        let finish = scan_while(input, part, word_character);
        if finish == part || !input[part..finish].chars().any(char::is_alphabetic) {
            break;
        }
        end = finish;
    }
    (end > first_end).then_some(end)
}

fn dotted_name_end(input: &str, start: usize) -> usize {
    let mut end = start;
    loop {
        let next = scan_while(input, end, |ch| word_character(ch) || ch == '-');
        if !input[next..].starts_with('.')
            || !input[next + 1..].chars().next().is_some_and(word_character)
        {
            return if end == start { start } else { next };
        }
        end = next + 1;
    }
}

fn email(input: &str, word_end: usize) -> Option<usize> {
    let local_end = scan_while(input, word_end, |ch| {
        word_character(ch) || matches!(ch, '-' | '_' | '.')
    });
    if !input[local_end..].starts_with('@') {
        return None;
    }
    let host_start = local_end + 1;
    let host_end = dotted_name_end(input, host_start);
    if host_start == host_end {
        return None;
    }
    let top_label = input[host_start..host_end].rsplit('.').next()?;
    (top_label.len() >= 2 && top_label.bytes().all(|byte| byte.is_ascii_alphabetic()))
        .then_some(host_end)
}

fn standalone_path(input: &str) -> Option<usize> {
    if input.starts_with('/') {
        path_end(input, 0, false)
    } else if input.starts_with("..")
        && !input[2..]
            .chars()
            .next()
            .is_some_and(|ch| matches!(ch, '.' | '/') || word_character(ch))
    {
        Some(2)
    } else {
        None
    }
}

fn path_end(input: &str, start: usize, url: bool) -> Option<usize> {
    if !input[start..].starts_with('/') {
        return None;
    }
    let end = scan_while(input, start + 1, |ch| {
        word_character(ch)
            || matches!(ch, '_' | '-' | '.' | '/' | '~')
            || (url && matches!(ch, '?' | '=' | '&' | '%' | '+' | ':' | '@'))
    });
    let path = input[start + 1..end].trim_end_matches(['.', '/']);
    (!path.is_empty()).then_some(start + 1 + path.len())
}

fn tag(input: &str) -> Option<usize> {
    if let Some(comment) = input.strip_prefix("<!--") {
        return comment.find("-->").map(|end| end + 7);
    }
    let body = input[1..]
        .strip_prefix(['/', '!', '?'])
        .unwrap_or(&input[1..]);
    if !body
        .chars()
        .next()
        .is_some_and(|ch| ch.is_alphabetic() || ch == '_')
    {
        return None;
    }
    let mut quote = None;
    for (offset, ch) in input.char_indices().skip(1) {
        match (quote, ch) {
            (Some(delimiter), ch) if delimiter == ch => quote = None,
            (None, '"' | '\'') => quote = Some(ch),
            (None, '<') => return None,
            (None, '>') => return Some(offset + 1),
            _ => {}
        }
    }
    None
}

fn entity(input: &str) -> Option<usize> {
    let end = if input.starts_with("&#x") || input.starts_with("&#X") {
        scan_while(input, 3, |ch| ch.is_ascii_hexdigit())
    } else if input.starts_with("&#") {
        scan_while(input, 2, |ch| ch.is_ascii_digit())
    } else {
        scan_while(input, 1, |ch| ch.is_ascii_alphanumeric())
    };
    (end > 1
        && input[end - 1..end]
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric())
        && input[end..].starts_with(';'))
    .then_some(end + 1)
}
