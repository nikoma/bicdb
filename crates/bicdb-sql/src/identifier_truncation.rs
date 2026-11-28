//! PostgreSQL's NAMEDATALEN: an identifier longer than 63 bytes is cut to
//! 63 bytes by the lexer, which reports `identifier "..." will be truncated
//! to "..."`. This applies the same cut to statement text before it is
//! parsed. String literals, comments and dollar-quoted bodies are left alone.

/// The longest identifier PostgreSQL keeps (NAMEDATALEN - 1 bytes).
const MAX_IDENTIFIER_BYTES: usize = 63;

/// One identifier the text named in full and the name it becomes.
pub(crate) struct Truncation {
    pub(crate) identifier: String,
    pub(crate) truncated: String,
}

/// `sql` with every over-long identifier cut to 63 bytes, and what was cut;
/// `None` when no identifier is longer than that.
pub(crate) fn truncate_long_identifiers(sql: &str) -> Option<(String, Vec<Truncation>)> {
    let bytes = sql.as_bytes();
    let mut edits: Vec<(usize, usize, String)> = Vec::new();
    let mut truncations = Vec::new();
    let mut at = 0;
    while at < bytes.len() {
        let byte = bytes[at];
        match byte {
            b'\'' => at = skip_string(bytes, at, at > 0 && matches!(bytes[at - 1], b'e' | b'E')),
            b'-' if bytes.get(at + 1) == Some(&b'-') => {
                while at < bytes.len() && bytes[at] != b'\n' {
                    at += 1;
                }
            }
            b'/' if bytes.get(at + 1) == Some(&b'*') => at = skip_block_comment(bytes, at),
            b'$' => at = skip_dollar(sql, at),
            b'"' => {
                let start = at + 1;
                let mut end = start;
                let mut escaped = false;
                while end < bytes.len() {
                    if bytes[end] == b'"' {
                        if bytes.get(end + 1) == Some(&b'"') {
                            escaped = true;
                            end += 2;
                            continue;
                        }
                        break;
                    }
                    end += 1;
                }
                if end < bytes.len() && end - start > MAX_IDENTIFIER_BYTES {
                    let identifier = &sql[start..end];
                    let decoded = if escaped {
                        std::borrow::Cow::Owned(identifier.replace("\"\"", "\""))
                    } else {
                        std::borrow::Cow::Borrowed(identifier)
                    };
                    if decoded.len() > MAX_IDENTIFIER_BYTES {
                        let truncated = clip(&decoded);
                        edits.push((start, end, truncated.replace('"', "\"\"")));
                        truncations.push(Truncation {
                            identifier: decoded.to_string(),
                            truncated: truncated.to_string(),
                        });
                    }
                }
                at = end + 1;
            }
            byte if is_identifier_start(byte) => {
                let start = at;
                while at < bytes.len() && is_identifier_byte(bytes[at]) {
                    at += 1;
                }
                // `E'...'`, `U&'...'`, `B'...'` and `X'...'` introduce strings.
                if at < bytes.len() && bytes[at] == b'\'' {
                    continue;
                }
                if at - start > MAX_IDENTIFIER_BYTES {
                    let written = &sql[start..at];
                    let truncated = clip(written).to_string();
                    truncations.push(Truncation {
                        identifier: written.to_ascii_lowercase(),
                        truncated: truncated.to_ascii_lowercase(),
                    });
                    edits.push((start, at, truncated));
                }
            }
            byte if byte.is_ascii_digit() => {
                while at < bytes.len() && (bytes[at].is_ascii_alphanumeric() || bytes[at] == b'.') {
                    at += 1;
                }
            }
            _ => at += 1,
        }
    }
    if edits.is_empty() {
        return None;
    }
    let mut rewritten = String::with_capacity(sql.len());
    let mut copied = 0;
    for (start, end, replacement) in edits {
        rewritten.push_str(&sql[copied..start]);
        rewritten.push_str(&replacement);
        copied = end;
    }
    rewritten.push_str(&sql[copied..]);
    Some((rewritten, truncations))
}

fn clip(identifier: &str) -> &str {
    let mut end = MAX_IDENTIFIER_BYTES.min(identifier.len());
    while !identifier.is_char_boundary(end) {
        end -= 1;
    }
    &identifier[..end]
}

fn is_identifier_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_' || byte >= 0x80
}

fn is_identifier_byte(byte: u8) -> bool {
    is_identifier_start(byte) || byte.is_ascii_digit() || byte == b'$'
}

fn skip_string(bytes: &[u8], start: usize, backslash_escapes: bool) -> usize {
    let mut at = start + 1;
    while at < bytes.len() {
        match bytes[at] {
            b'\\' if backslash_escapes => at += 2,
            b'\'' if bytes.get(at + 1) == Some(&b'\'') => at += 2,
            b'\'' => return at + 1,
            _ => at += 1,
        }
    }
    at
}

fn skip_block_comment(bytes: &[u8], start: usize) -> usize {
    let mut depth = 0;
    let mut at = start;
    while at + 1 < bytes.len() {
        if bytes[at] == b'/' && bytes[at + 1] == b'*' {
            depth += 1;
            at += 2;
        } else if bytes[at] == b'*' && bytes[at + 1] == b'/' {
            depth -= 1;
            at += 2;
            if depth == 0 {
                return at;
            }
        } else {
            at += 1;
        }
    }
    bytes.len()
}

/// Skips a dollar-quoted string (`$$...$$`, `$tag$...$tag$`); a positional
/// parameter (`$1`) or a lone `$` is one byte.
fn skip_dollar(sql: &str, start: usize) -> usize {
    let bytes = sql.as_bytes();
    let mut end = start + 1;
    while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
        end += 1;
    }
    if bytes.get(end) != Some(&b'$')
        || bytes[start + 1..end]
            .first()
            .is_some_and(u8::is_ascii_digit)
    {
        return start + 1;
    }
    let tag = &sql[start..=end];
    match sql[end + 1..].find(tag) {
        Some(offset) => end + 1 + offset + tag.len(),
        None => bytes.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn long_identifiers_are_cut_to_63_bytes_outside_literals() {
        let long = "a".repeat(70);
        let sql = format!(
            "CREATE TABLE {long}(x int); SELECT '{long}', $${long}$$ FROM \"{long}\" -- {long}"
        );
        let (rewritten, truncations) = truncate_long_identifiers(&sql).unwrap();
        let cut = "a".repeat(63);
        assert_eq!(
            rewritten,
            format!(
                "CREATE TABLE {cut}(x int); SELECT '{long}', $${long}$$ FROM \"{cut}\" -- {long}"
            )
        );
        assert_eq!(truncations.len(), 2);
        assert_eq!(truncations[0].truncated, cut);
        assert!(truncate_long_identifiers("SELECT abc, $1, 'x'").is_none());
    }

    #[test]
    fn escaped_identifier_quotes_keep_following_statements_and_count_as_one_byte() {
        let sql = concat!(
            "CREATE SCHEMA \"quo\"\"te\"; ",
            "CREATE TABLE public.path_probe (value text); ",
            "SET search_path TO \"Partial.Path\", public;"
        );
        assert!(truncate_long_identifiers(sql).is_none());

        let retained = format!("{}\"", "a".repeat(62));
        let full = format!("{retained}suffix");
        let escaped = full.replace('"', "\"\"");
        let sql = format!("SELECT \"{escaped}\"");
        let (rewritten, notices) = truncate_long_identifiers(&sql).unwrap();
        assert_eq!(
            rewritten,
            format!("SELECT \"{}\"", retained.replace('"', "\"\""))
        );
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].identifier, full);
        assert_eq!(notices[0].truncated, retained);
    }

    #[test]
    fn unquoted_identifiers_are_reported_folded() {
        let name = format!("Abc{}", "D".repeat(70));
        let (_, truncations) = truncate_long_identifiers(&format!("SELECT {name}")).unwrap();
        assert_eq!(truncations[0].identifier, name.to_ascii_lowercase());
        assert_eq!(truncations[0].truncated.len(), 63);
    }
}
