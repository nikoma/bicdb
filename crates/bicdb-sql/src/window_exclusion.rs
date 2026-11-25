//! Window frame exclusion: `EXCLUDE CURRENT ROW | GROUP | TIES | NO OTHERS`.
//!
//! The SQL parser does not read the clause, so the parse front end removes it
//! and records the mode as a constant first PARTITION BY expression of the
//! same window. A constant partition key never changes partitioning, so a
//! path that did not know the marker would still partition correctly; the
//! window evaluator removes the marker and applies the exclusion to the
//! frame of aggregate and first/last/nth-value functions, as PostgreSQL does.
use crate::*;
use sqlparser::ast::{Value, WindowSpec};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::tokenizer::Token;

const MARKER_PREFIX: &str = "bicdb:window-exclude:";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FrameExclusion {
    NoOthers,
    CurrentRow,
    Group,
    Ties,
}

impl FrameExclusion {
    fn marker(self) -> &'static str {
        match self {
            Self::NoOthers => "no-others",
            Self::CurrentRow => "current-row",
            Self::Group => "group",
            Self::Ties => "ties",
        }
    }

    /// Whether frame position `target` is excluded for the row at
    /// `position`; `peers` tells whether two positions are ORDER BY peers.
    pub(crate) fn excludes(
        self,
        position: usize,
        target: usize,
        peers: impl Fn(usize, usize) -> bool,
    ) -> bool {
        match self {
            Self::NoOthers => false,
            Self::CurrentRow => target == position,
            Self::Group => target == position || peers(position, target),
            Self::Ties => target != position && peers(position, target),
        }
    }
}

/// The exclusion a window spec's marker records, if any. The marker stays
/// in PARTITION BY: as a constant key it does not change partitioning.
pub(crate) fn frame_exclusion(spec: &WindowSpec) -> Option<FrameExclusion> {
    spec.partition_by.iter().find_map(|expr| {
        let Expr::Value(value) = expr else {
            return None;
        };
        let Value::SingleQuotedString(text) = &value.value else {
            return None;
        };
        let mode = text.strip_prefix(MARKER_PREFIX)?;
        [
            FrameExclusion::NoOthers,
            FrameExclusion::CurrentRow,
            FrameExclusion::Group,
            FrameExclusion::Ties,
        ]
        .into_iter()
        .find(|candidate| candidate.marker() == mode)
    })
}

/// Parse-front-end rewrite (see the module comment). Works on tokens, so
/// strings, comments and dollar-quoted bodies are untouched.
pub(crate) fn rewrite_window_exclusion(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    if !bytes
        .windows(7)
        .any(|window| window.eq_ignore_ascii_case(b"exclude"))
    {
        return None;
    }
    let source = tokens_with_source(&PostgreSqlDialect {}, input)?;
    let word = |index: usize| -> Option<String> {
        match source.get(index).map(|(token, _)| token) {
            Some(Token::Word(word)) if word.quote_style.is_none() => {
                Some(word.value.to_ascii_uppercase())
            }
            _ => None,
        }
    };
    let next = |from: usize| -> Option<usize> {
        (from..source.len()).find(|index| !matches!(source[*index].0, Token::Whitespace(_)))
    };
    // Token index -> text inserted before it; removed token ranges.
    let mut inserts: BTreeMap<usize, String> = BTreeMap::new();
    let mut removed: Vec<(usize, usize)> = Vec::new();
    let mut opens: Vec<usize> = Vec::new();
    let mut index = 0;
    while index < source.len() {
        match &source[index].0 {
            Token::LParen => opens.push(index),
            Token::RParen => {
                opens.pop();
            }
            _ => {}
        }
        if word(index).as_deref() == Some("EXCLUDE") {
            // PostgreSQL reads EXCLUDE only after a frame clause.
            let open = opens.last().copied().filter(|open| {
                (open + 1..index).any(|position| {
                    matches!(word(position).as_deref(), Some("ROWS" | "RANGE" | "GROUPS"))
                })
            });
            if let (Some(open), Some(first)) = (open, next(index + 1)) {
                let second = next(first + 1);
                let found = match (word(first).as_deref(), second.and_then(word).as_deref()) {
                    (Some("CURRENT"), Some("ROW")) => {
                        Some((FrameExclusion::CurrentRow, second.unwrap()))
                    }
                    (Some("NO"), Some("OTHERS")) => {
                        Some((FrameExclusion::NoOthers, second.unwrap()))
                    }
                    (Some("GROUP"), _) => Some((FrameExclusion::Group, first)),
                    (Some("TIES"), _) => Some((FrameExclusion::Ties, first)),
                    _ => None,
                };
                if let Some((exclusion, end)) = found {
                    let marker = format!("'{MARKER_PREFIX}{}'", exclusion.marker());
                    // The marker goes first in PARTITION BY, after an
                    // optional base window name.
                    let mut at = next(open + 1)?;
                    if word(at).is_none_or(|first| {
                        !matches!(
                            first.as_str(),
                            "PARTITION" | "ORDER" | "ROWS" | "RANGE" | "GROUPS"
                        )
                    }) && matches!(source[at].0, Token::Word(_))
                    {
                        at = next(at + 1)?;
                    }
                    let text = if word(at).as_deref() == Some("PARTITION") {
                        let by = next(at + 1)?;
                        let after = next(by + 1)?;
                        inserts
                            .entry(after)
                            .or_default()
                            .push_str(&format!("{marker}, "));
                        None
                    } else {
                        Some(format!("PARTITION BY {marker} "))
                    };
                    if let Some(text) = text {
                        inserts.entry(at).or_default().push_str(&text);
                    }
                    removed.push((index, end));
                    index = end + 1;
                    continue;
                }
            }
        }
        index += 1;
    }
    if removed.is_empty() {
        return None;
    }
    let mut out = String::with_capacity(input.len() + 64);
    for (position, (_, text)) in source.iter().enumerate() {
        if let Some(insert) = inserts.get(&position) {
            out.push_str(insert);
        }
        if removed
            .iter()
            .any(|(start, end)| (*start..=*end).contains(&position))
        {
            continue;
        }
        out.push_str(text);
    }
    Some(out)
}
