//! PostgreSQL permits sequence options in any order; sqlparser expects one
//! fixed order inside IDENTITY. Preserve all other SQL source bytes.
use crate::*;

pub(super) fn rewrite(sql: &str) -> Option<String> {
    if !sql.to_ascii_lowercase().contains("identity") {
        return None;
    }
    let source = tokens_with_source(&PostgreSqlDialect {}, sql)?;
    let significant = source
        .iter()
        .enumerate()
        .filter_map(|(index, (token, _))| (!matches!(token, Token::Whitespace(_))).then_some(index))
        .collect::<Vec<_>>();
    let token = |index: usize| &source[significant[index]].0;
    let word = |index: usize, expected: &str| matches!(token(index), Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(expected));
    let mut offsets = Vec::with_capacity(source.len());
    let mut offset = 0;
    for (_, text) in &source {
        offsets.push(offset);
        offset += text.len();
    }
    let mut edits = Vec::new();
    for identity in 3..significant.len().saturating_sub(1) {
        if !word(identity, "IDENTITY") || *token(identity + 1) != Token::LParen {
            continue;
        }
        let generated = word(identity - 1, "AS")
            && (word(identity - 2, "ALWAYS") && word(identity - 3, "GENERATED")
                || identity >= 4
                    && word(identity - 2, "DEFAULT")
                    && word(identity - 3, "BY")
                    && word(identity - 4, "GENERATED"));
        if !generated {
            continue;
        }
        let close = (identity + 2..significant.len())
            .find(|index| matches!(token(*index), Token::RParen | Token::LParen))?;
        if *token(close) != Token::RParen {
            continue;
        }
        let mut cursor = identity + 2;
        let mut options = Vec::new();
        let mut seen = BTreeSet::new();
        let mut valid = true;
        while cursor < close {
            let start = cursor;
            let no = word(cursor, "NO");
            cursor += usize::from(no);
            if cursor == close {
                valid = false;
                break;
            }
            let (rank, modifier, numeric) = if word(cursor, "INCREMENT") && !no {
                (0, Some("BY"), true)
            } else if word(cursor, "MINVALUE") {
                (1, None, !no)
            } else if word(cursor, "MAXVALUE") {
                (2, None, !no)
            } else if word(cursor, "START") && !no {
                (3, Some("WITH"), true)
            } else if word(cursor, "CACHE") && !no {
                (4, None, true)
            } else if word(cursor, "CYCLE") {
                (5, None, false)
            } else {
                valid = false;
                break;
            };
            cursor += 1;
            if modifier.is_some_and(|modifier| cursor < close && word(cursor, modifier)) {
                cursor += 1;
            }
            if numeric {
                if cursor < close && matches!(token(cursor), Token::Minus | Token::Plus) {
                    cursor += 1;
                }
                if cursor == close || !matches!(token(cursor), Token::Number(_, _)) {
                    valid = false;
                    break;
                }
                cursor += 1;
            }
            if !seen.insert(rank) {
                valid = false;
                break;
            }
            let text = source[significant[start]..significant[cursor]]
                .iter()
                .map(|(_, text)| *text)
                .collect::<String>();
            options.push((rank, text));
        }
        if valid && options.windows(2).any(|pair| pair[0].0 > pair[1].0) {
            options.sort_by_key(|(rank, _)| *rank);
            let body = options
                .into_iter()
                .map(|(_, text)| format!("{text} "))
                .collect::<String>();
            edits.push((
                offsets[significant[identity + 1]] + 1,
                offsets[significant[close]],
                body,
            ));
        }
    }
    if edits.is_empty() {
        return None;
    }
    let mut rewritten = sql.to_string();
    for (start, end, body) in edits.into_iter().rev() {
        rewritten.replace_range(start..end, &body);
    }
    Some(rewritten)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mixed_identity_options_preserve_literals_comments_and_signed_values() {
        let sql = "CREATE TABLE \"é\"(payload TEXT DEFAULT E'IDENTITY(START WITH 1 INCREMENT BY 2)\\\\;''quoted',n BIGINT GENERATED ALWAYS AS IDENTITY(CACHE 8 NO CYCLE START WITH -100 MAXVALUE 1 MINVALUE -1000 INCREMENT BY -5))";
        let rewritten = rewrite(sql).unwrap();
        assert!(rewritten.contains("E'IDENTITY(START WITH 1 INCREMENT BY 2)\\\\;''quoted'"));
        let statements = parse_statements(&rewritten).unwrap();
        let Statement::CreateTable(table) = &statements[0] else {
            panic!("CREATE TABLE lost")
        };
        let (_, options) = identity_options(&table.columns[1]).unwrap().unwrap();
        let sequence = sequence_from_options("test_seq".into(), "int8", options).unwrap();
        assert_eq!(sequence.increment_by, -5);
        assert_eq!(sequence.start_value, -100);
        assert_eq!(sequence.min_value, -1000);
        assert_eq!(sequence.max_value, 1);
        assert_eq!(sequence.cache_size, 8);
        assert!(!sequence.cycle);
    }

    #[test]
    fn option_rewrite_leaves_data_and_already_ordered_or_duplicate_options_alone() {
        for sql in [
            "SELECT 'GENERATED ALWAYS AS IDENTITY(START WITH 1 INCREMENT BY 2)'",
            "CREATE TABLE p(n BIGINT GENERATED ALWAYS AS IDENTITY(INCREMENT BY 5 START WITH 100))",
            "CREATE TABLE p(n BIGINT GENERATED ALWAYS AS IDENTITY(START WITH 100 START WITH 200))",
        ] {
            assert!(rewrite(sql).is_none(), "{sql}");
        }
        let sql = "ALTER TABLE p ADD COLUMN n BIGINT GENERATED ALWAYS AS IDENTITY(START WITH 100 -- keep newline\n INCREMENT BY 5)";
        let rewritten = rewrite(sql).unwrap();
        assert!(rewritten.contains("-- keep newline\n"));
        parse_statements(&rewritten).unwrap();
    }
}
