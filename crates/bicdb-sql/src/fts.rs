use rust_stemmers::{Algorithm as LegacyAlgorithm, Stemmer as LegacyStemmer};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use unicode_categories::UnicodeCategories;
use waken_snowball::{Algorithm, Stemmer};

use crate::eval::builtin_regconfig_name;
use crate::{pg_internal_char_byte, BicDb, Result, SqlEngine, SqlError, SqlValue};

const MAX_LEXEME_BYTES: usize = bicdb_core::MAX_FULL_TEXT_TERM_BYTES;
const MAX_POSITION: u16 = 16_383;
/// Upper bound on lexemes in one wire-decoded tsvector. PostgreSQL's own
/// limit is ~1M positions per vector; this bounds the honest-but-enormous
/// case after the byte-derived bound has caught the dishonest one.
const MAX_TSVECTOR_LEXEMES: usize = 1_000_000;
const MAX_POSITIONS: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PgTsWeight {
    D,
    C,
    B,
    A,
}

impl PgTsWeight {
    fn rank(self) -> u8 {
        match self {
            Self::D => 0,
            Self::C => 1,
            Self::B => 2,
            Self::A => 3,
        }
    }

    fn suffix(self) -> &'static str {
        match self {
            Self::D => "",
            Self::C => "C",
            Self::B => "B",
            Self::A => "A",
        }
    }

    fn parse(value: Option<u8>) -> Option<Self> {
        match value {
            None | Some(b'D') => Some(Self::D),
            Some(b'C') => Some(Self::C),
            Some(b'B') => Some(Self::B),
            Some(b'A') => Some(Self::A),
            _ => None,
        }
    }

    fn query_bit(self) -> u8 {
        1 << self.rank()
    }

    fn from_rank(rank: u8) -> Option<Self> {
        match rank {
            0 => Some(Self::D),
            1 => Some(Self::C),
            2 => Some(Self::B),
            3 => Some(Self::A),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PgTsPosition {
    pub position: u16,
    pub weight: PgTsWeight,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PgTsLexeme {
    pub text: String,
    pub positions: Vec<PgTsPosition>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PgTsVector {
    pub lexemes: Vec<PgTsLexeme>,
}

impl PgTsVector {
    pub fn from_postgres_text(input: &str) -> Result<Self> {
        let mut parser = TsVectorParser::new(input);
        let mut entries = BTreeMap::<String, Vec<PgTsPosition>>::new();
        while let Some((lexeme, positions)) = parser.next_lexeme()? {
            if lexeme.len() > MAX_LEXEME_BYTES {
                return Err(SqlError::data_exception(
                    "54000",
                    format!(
                        "word is too long ({} bytes, max {MAX_LEXEME_BYTES} bytes)",
                        lexeme.len()
                    ),
                    Some("tsvector".to_string()),
                ));
            }
            entries.entry(lexeme).or_default().extend(positions);
        }
        Ok(Self {
            lexemes: entries
                .into_iter()
                .map(|(text, positions)| PgTsLexeme {
                    text,
                    positions: canonical_positions(positions),
                })
                .collect(),
        })
    }

    pub fn to_postgres_text(&self) -> String {
        self.lexemes
            .iter()
            .map(|lexeme| {
                let escaped = lexeme.text.replace('\\', "\\\\").replace('\'', "''");
                if lexeme.positions.is_empty() {
                    format!("'{escaped}'")
                } else {
                    let positions = lexeme
                        .positions
                        .iter()
                        .map(|position| {
                            format!("{}{}", position.position, position.weight.suffix())
                        })
                        .collect::<Vec<_>>()
                        .join(",");
                    format!("'{escaped}':{positions}")
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    pub fn to_postgres_binary(&self) -> Vec<u8> {
        let mut output = Vec::new();
        output.extend_from_slice(&(self.lexemes.len() as i32).to_be_bytes());
        for lexeme in &self.lexemes {
            output.extend_from_slice(lexeme.text.as_bytes());
            output.push(0);
            output.extend_from_slice(&(lexeme.positions.len() as u16).to_be_bytes());
            for position in &lexeme.positions {
                let encoded = (u16::from(position.weight.rank()) << 14) | position.position;
                output.extend_from_slice(&encoded.to_be_bytes());
            }
        }
        output
    }

    pub fn from_postgres_binary(input: &[u8]) -> Result<Self> {
        let mut decoder = TextSearchBinaryDecoder::new(input, "tsvector");
        // A lexeme costs at least a NUL terminator plus its 2-byte position
        // count; a position costs 2 bytes. Both counts come off the wire, so
        // both are bounded against the bytes that remain before anything is
        // reserved (`bicdb_core::parse_budget`).
        let count = decoder.read_count()?;
        let mut lexemes: Vec<PgTsLexeme> = bicdb_core::parse_budget::bounded_vec(
            "tsvector",
            count,
            decoder.remaining_bytes(),
            3,
            MAX_TSVECTOR_LEXEMES,
        )
        .map_err(budget_error)?;
        for _ in 0..count {
            let text = decoder.read_cstring()?;
            let position_count = usize::from(decoder.read_u16()?);
            let mut positions: Vec<PgTsPosition> = bicdb_core::parse_budget::bounded_vec(
                "tsvector positions",
                position_count,
                decoder.remaining_bytes(),
                2,
                usize::from(MAX_POSITION),
            )
            .map_err(budget_error)?;
            for _ in 0..position_count {
                let encoded = decoder.read_u16()?;
                let position = encoded & MAX_POSITION;
                let weight = PgTsWeight::from_rank((encoded >> 14) as u8)
                    .expect("two-bit tsvector weight is always valid");
                if position == 0 {
                    return Err(text_search_binary_error(
                        "tsvector",
                        "position must be positive",
                    ));
                }
                positions.push(PgTsPosition { position, weight });
            }
            lexemes.push(PgTsLexeme {
                text,
                positions: canonical_positions(positions),
            });
        }
        decoder.finish()?;
        Ok(Self { lexemes })
    }

    pub fn strip(&self) -> Self {
        Self {
            lexemes: self
                .lexemes
                .iter()
                .map(|lexeme| PgTsLexeme {
                    text: lexeme.text.clone(),
                    positions: Vec::new(),
                })
                .collect(),
        }
    }

    pub fn concat(&self, right: &Self) -> Self {
        let shift = self
            .lexemes
            .iter()
            .flat_map(|lexeme| lexeme.positions.iter())
            .map(|position| position.position)
            .max()
            .unwrap_or(0);
        let mut entries = self
            .lexemes
            .iter()
            .map(|lexeme| (lexeme.text.clone(), lexeme.positions.clone()))
            .collect::<BTreeMap<_, _>>();
        for lexeme in &right.lexemes {
            entries
                .entry(lexeme.text.clone())
                .or_default()
                .extend(lexeme.positions.iter().map(|position| PgTsPosition {
                    position: position.position.saturating_add(shift).min(MAX_POSITION),
                    weight: position.weight,
                }));
        }
        Self {
            lexemes: entries
                .into_iter()
                .map(|(text, positions)| PgTsLexeme {
                    text,
                    positions: canonical_positions(positions),
                })
                .collect(),
        }
    }

    pub fn index_key(&self) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend((self.storage_size() as u64).to_be_bytes());
        key.extend((self.lexemes.len() as u64).to_be_bytes());
        for lexeme in &self.lexemes {
            key.push(u8::from(lexeme.positions.is_empty()));
            key.extend(lexeme.text.as_bytes());
            key.push(0);
            if !lexeme.positions.is_empty() {
                key.extend((u16::MAX - lexeme.positions.len() as u16).to_be_bytes());
                for position in &lexeme.positions {
                    key.extend((u16::MAX - position.position).to_be_bytes());
                    key.push(3 - position.weight.rank());
                }
            }
        }
        key
    }

    fn storage_size(&self) -> usize {
        let mut data = 0usize;
        for lexeme in &self.lexemes {
            data += lexeme.text.len();
            if !lexeme.positions.is_empty() {
                data = (data + 1) & !1;
                data += 2 + lexeme.positions.len() * 2;
            }
        }
        8 + self.lexemes.len() * 4 + data
    }
}

impl Ord for PgTsVector {
    fn cmp(&self, other: &Self) -> Ordering {
        self.index_key().cmp(&other.index_key())
    }
}

impl PartialOrd for PgTsVector {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PgTsQueryOperand {
    pub text: String,
    pub weights: u8,
    pub prefix: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PgTsQueryNode {
    Operand(PgTsQueryOperand),
    Not(Box<PgTsQueryNode>),
    And(Box<PgTsQueryNode>, Box<PgTsQueryNode>),
    Or(Box<PgTsQueryNode>, Box<PgTsQueryNode>),
    Phrase {
        left: Box<PgTsQueryNode>,
        right: Box<PgTsQueryNode>,
        distance: u16,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PgTsQuery {
    pub root: Option<PgTsQueryNode>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FtsIndexCandidate {
    Term { text: String, prefix: bool },
    And(Box<FtsIndexCandidate>, Box<FtsIndexCandidate>),
    Or(Box<FtsIndexCandidate>, Box<FtsIndexCandidate>),
}

impl PgTsQuery {
    pub fn from_postgres_text(input: &str) -> Result<Self> {
        Self::from_postgres_text_with_limit(input, true)
    }

    fn from_postgres_text_with_limit(input: &str, enforce_lexeme_limit: bool) -> Result<Self> {
        let mut parser = TsQueryParser::new(input, enforce_lexeme_limit);
        parser.skip_whitespace();
        if parser.offset == input.len() {
            return Ok(Self::default());
        }
        let root = parser.parse_or()?;
        parser.skip_whitespace();
        if parser.offset != input.len() {
            return Err(tsquery_syntax_error(input));
        }
        Ok(Self { root: Some(root) })
    }

    pub fn to_postgres_text(&self) -> String {
        self.root
            .as_ref()
            .map(|root| format_tsquery_node(root, 0, false))
            .unwrap_or_default()
    }

    pub fn to_postgres_binary(&self) -> Vec<u8> {
        let mut output = Vec::new();
        output.extend_from_slice(&(self.node_count() as i32).to_be_bytes());
        if let Some(root) = &self.root {
            append_tsquery_binary_node(root, &mut output);
        }
        output
    }

    pub fn from_postgres_binary(input: &[u8]) -> Result<Self> {
        let mut decoder = TextSearchBinaryDecoder::new(input, "tsquery");
        let count = decoder.read_count()?;
        let root = if count == 0 {
            None
        } else {
            let mut remaining = count;
            let mut budget = bicdb_core::parse_budget::ParseBudget::new("tsquery");
            Some(decode_tsquery_binary_node(
                &mut decoder,
                &mut remaining,
                &mut budget,
            )?)
        };
        if count > 0 {
            let mut observed = 0usize;
            if let Some(root) = &root {
                observed = tsquery_node_count(root);
            }
            if observed != count {
                return Err(text_search_binary_error(
                    "tsquery",
                    format!("declared {count} items but decoded {observed}"),
                ));
            }
        }
        decoder.finish()?;
        Ok(Self { root })
    }

    pub fn and(self, right: Self) -> Self {
        self.join(right, PgTsQueryNode::And)
    }

    pub fn or(self, right: Self) -> Self {
        self.join(right, PgTsQueryNode::Or)
    }

    pub fn phrase(self, right: Self, distance: u16) -> Self {
        match (self.root, right.root) {
            (None, root) | (root, None) => Self { root },
            (Some(left), Some(right)) => Self {
                root: Some(PgTsQueryNode::Phrase {
                    left: Box::new(left),
                    right: Box::new(right),
                    distance,
                }),
            },
        }
    }

    pub fn not(self) -> Self {
        Self {
            root: self.root.map(|root| PgTsQueryNode::Not(Box::new(root))),
        }
    }

    pub fn matches(&self, vector: &PgTsVector) -> bool {
        self.root
            .as_ref()
            .is_some_and(|root| matches_query_node(root, vector))
    }

    pub fn index_key(&self) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend((self.node_count() as u32).to_be_bytes());
        key.extend((self.storage_size() as u64).to_be_bytes());
        if let Some(root) = &self.root {
            append_tsquery_node_key(root, &mut key);
        }
        key
    }

    fn join(
        self,
        right: Self,
        constructor: impl FnOnce(Box<PgTsQueryNode>, Box<PgTsQueryNode>) -> PgTsQueryNode,
    ) -> Self {
        match (self.root, right.root) {
            (None, root) | (root, None) => Self { root },
            (Some(left), Some(right)) => Self {
                root: Some(constructor(Box::new(left), Box::new(right))),
            },
        }
    }

    fn node_count(&self) -> usize {
        self.root.as_ref().map(tsquery_node_count).unwrap_or(0)
    }

    pub fn numnode(&self) -> usize {
        self.node_count()
    }

    pub(crate) fn index_candidate(&self) -> Option<FtsIndexCandidate> {
        self.root.as_ref().and_then(fts_index_candidate_node)
    }

    fn storage_size(&self) -> usize {
        let operands = self.root.as_ref().map(tsquery_operand_bytes).unwrap_or(0);
        8 + self.node_count() * 12 + operands
    }
}

fn append_tsquery_binary_node(node: &PgTsQueryNode, output: &mut Vec<u8>) {
    match node {
        PgTsQueryNode::Operand(operand) => {
            output.push(1);
            output.push(operand.weights);
            output.push(u8::from(operand.prefix));
            output.extend_from_slice(operand.text.as_bytes());
            output.push(0);
        }
        PgTsQueryNode::Not(child) => {
            output.extend_from_slice(&[2, 1]);
            append_tsquery_binary_node(child, output);
        }
        PgTsQueryNode::And(left, right) => {
            output.extend_from_slice(&[2, 2]);
            append_tsquery_binary_node(right, output);
            append_tsquery_binary_node(left, output);
        }
        PgTsQueryNode::Or(left, right) => {
            output.extend_from_slice(&[2, 3]);
            append_tsquery_binary_node(right, output);
            append_tsquery_binary_node(left, output);
        }
        PgTsQueryNode::Phrase {
            left,
            right,
            distance,
        } => {
            output.extend_from_slice(&[2, 4]);
            output.extend_from_slice(&distance.to_be_bytes());
            append_tsquery_binary_node(right, output);
            append_tsquery_binary_node(left, output);
        }
    }
}

fn decode_tsquery_binary_node(
    decoder: &mut TextSearchBinaryDecoder<'_>,
    remaining: &mut usize,
    budget: &mut bicdb_core::parse_budget::ParseBudget,
) -> Result<PgTsQueryNode> {
    budget.enter().map_err(budget_error)?;
    let node = decode_tsquery_binary_node_inner(decoder, remaining, budget);
    budget.leave();
    node
}

fn decode_tsquery_binary_node_inner(
    decoder: &mut TextSearchBinaryDecoder<'_>,
    remaining: &mut usize,
    budget: &mut bicdb_core::parse_budget::ParseBudget,
) -> Result<PgTsQueryNode> {
    if *remaining == 0 {
        return Err(text_search_binary_error(
            "tsquery",
            "operator has too few operands",
        ));
    }
    *remaining -= 1;
    match decoder.read_u8()? {
        1 => {
            let weights = decoder.read_u8()?;
            if weights & !0x0f != 0 {
                return Err(text_search_binary_error(
                    "tsquery",
                    format!("invalid weight mask {weights}"),
                ));
            }
            let prefix = match decoder.read_u8()? {
                0 => false,
                1 => true,
                value => {
                    return Err(text_search_binary_error(
                        "tsquery",
                        format!("invalid prefix flag {value}"),
                    ));
                }
            };
            Ok(PgTsQueryNode::Operand(PgTsQueryOperand {
                text: decoder.read_cstring()?,
                weights,
                prefix,
            }))
        }
        2 => match decoder.read_u8()? {
            1 => Ok(PgTsQueryNode::Not(Box::new(decode_tsquery_binary_node(
                decoder, remaining, budget,
            )?))),
            operator @ (2 | 3) => {
                let right = decode_tsquery_binary_node(decoder, remaining, budget)?;
                let left = decode_tsquery_binary_node(decoder, remaining, budget)?;
                Ok(if operator == 2 {
                    PgTsQueryNode::And(Box::new(left), Box::new(right))
                } else {
                    PgTsQueryNode::Or(Box::new(left), Box::new(right))
                })
            }
            4 => {
                let distance = decoder.read_u16()?;
                let right = decode_tsquery_binary_node(decoder, remaining, budget)?;
                let left = decode_tsquery_binary_node(decoder, remaining, budget)?;
                Ok(PgTsQueryNode::Phrase {
                    left: Box::new(left),
                    right: Box::new(right),
                    distance,
                })
            }
            operator => Err(text_search_binary_error(
                "tsquery",
                format!("unknown operator {operator}"),
            )),
        },
        item_type => Err(text_search_binary_error(
            "tsquery",
            format!("unknown item type {item_type}"),
        )),
    }
}

struct TextSearchBinaryDecoder<'a> {
    input: &'a [u8],
    offset: usize,
    pg_type: &'static str,
}

impl<'a> TextSearchBinaryDecoder<'a> {
    fn new(input: &'a [u8], pg_type: &'static str) -> Self {
        Self {
            input,
            offset: 0,
            pg_type,
        }
    }

    /// An item count from the wire, bounded by the bytes that remain.
    ///
    /// The count is attacker-controlled and drives `Vec::with_capacity`; a
    /// declared 0x7FFFFFFF asks for ~100 GB from a 4-byte message. Every
    /// encodable item costs at least one byte, so a count above the
    /// remaining length is malformed by construction — reject it before
    /// reserving rather than aborting the process on allocation failure.
    /// Bytes of encoded input not yet consumed.
    fn remaining_bytes(&self) -> usize {
        self.input.len().saturating_sub(self.offset)
    }

    fn read_count(&mut self) -> Result<usize> {
        let value = i32::from_be_bytes(self.read_exact(4)?.try_into().unwrap());
        let count = usize::try_from(value).map_err(|_| {
            text_search_binary_error(self.pg_type, format!("negative item count {value}"))
        })?;
        let remaining = self.input.len().saturating_sub(self.offset);
        if count > remaining {
            return Err(text_search_binary_error(
                self.pg_type,
                format!("item count {count} exceeds the {remaining} bytes that follow"),
            ));
        }
        Ok(count)
    }

    fn read_u8(&mut self) -> Result<u8> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.read_exact(2)?.try_into().unwrap()))
    }

    fn read_cstring(&mut self) -> Result<String> {
        let remaining = &self.input[self.offset..];
        let length = remaining
            .iter()
            .position(|byte| *byte == 0)
            .ok_or_else(|| {
                text_search_binary_error(self.pg_type, "unterminated text-search lexeme")
            })?;
        let bytes = self.read_exact(length + 1)?;
        std::str::from_utf8(&bytes[..length])
            .map(str::to_string)
            .map_err(|error| text_search_binary_error(self.pg_type, error))
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self.offset.checked_add(length).ok_or_else(|| {
            text_search_binary_error(self.pg_type, "binary value length overflow")
        })?;
        let bytes = self
            .input
            .get(self.offset..end)
            .ok_or_else(|| text_search_binary_error(self.pg_type, "truncated binary value"))?;
        self.offset = end;
        Ok(bytes)
    }

    fn finish(&self) -> Result<()> {
        if self.offset == self.input.len() {
            Ok(())
        } else {
            Err(text_search_binary_error(
                self.pg_type,
                format!("{} trailing bytes", self.input.len() - self.offset),
            ))
        }
    }
}

fn text_search_binary_error(pg_type: &str, detail: impl std::fmt::Display) -> SqlError {
    SqlError::invalid_text_representation(
        pg_type,
        format!("invalid binary representation: {detail}"),
    )
}

fn fts_index_candidate_node(node: &PgTsQueryNode) -> Option<FtsIndexCandidate> {
    match node {
        PgTsQueryNode::Operand(operand) => Some(FtsIndexCandidate::Term {
            text: operand.text.clone(),
            prefix: operand.prefix,
        }),
        PgTsQueryNode::Not(_) => None,
        PgTsQueryNode::And(left, right) | PgTsQueryNode::Phrase { left, right, .. } => {
            match (
                fts_index_candidate_node(left),
                fts_index_candidate_node(right),
            ) {
                (Some(left), Some(right)) => {
                    Some(FtsIndexCandidate::And(Box::new(left), Box::new(right)))
                }
                (Some(candidate), None) | (None, Some(candidate)) => Some(candidate),
                (None, None) => None,
            }
        }
        PgTsQueryNode::Or(left, right) => Some(FtsIndexCandidate::Or(
            Box::new(fts_index_candidate_node(left)?),
            Box::new(fts_index_candidate_node(right)?),
        )),
    }
}

impl Ord for PgTsQuery {
    fn cmp(&self, other: &Self) -> Ordering {
        self.index_key().cmp(&other.index_key())
    }
}

impl PartialOrd for PgTsQuery {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn tsquery_node_count(node: &PgTsQueryNode) -> usize {
    match node {
        PgTsQueryNode::Operand(_) => 1,
        PgTsQueryNode::Not(child) => 1 + tsquery_node_count(child),
        PgTsQueryNode::And(left, right) | PgTsQueryNode::Or(left, right) => {
            1 + tsquery_node_count(left) + tsquery_node_count(right)
        }
        PgTsQueryNode::Phrase { left, right, .. } => {
            1 + tsquery_node_count(left) + tsquery_node_count(right)
        }
    }
}

fn tsquery_operand_bytes(node: &PgTsQueryNode) -> usize {
    match node {
        PgTsQueryNode::Operand(operand) => operand.text.len() + 1,
        PgTsQueryNode::Not(child) => tsquery_operand_bytes(child),
        PgTsQueryNode::And(left, right) | PgTsQueryNode::Or(left, right) => {
            tsquery_operand_bytes(left) + tsquery_operand_bytes(right)
        }
        PgTsQueryNode::Phrase { left, right, .. } => {
            tsquery_operand_bytes(left) + tsquery_operand_bytes(right)
        }
    }
}

fn format_tsquery_node(node: &PgTsQueryNode, parent_priority: u8, right_phrase: bool) -> String {
    match node {
        PgTsQueryNode::Operand(operand) => {
            let escaped = operand.text.replace('\\', "\\\\").replace('\'', "''");
            let mut output = format!("'{escaped}'");
            if operand.prefix || operand.weights != 0 {
                output.push(':');
                if operand.prefix {
                    output.push('*');
                }
                for (weight, letter) in [(3, 'A'), (2, 'B'), (1, 'C'), (0, 'D')] {
                    if operand.weights & (1 << weight) != 0 {
                        output.push(letter);
                    }
                }
            }
            output
        }
        PgTsQueryNode::Not(child) => {
            let priority = 4;
            let output = format!("!{}", format_tsquery_node(child, priority, false));
            parenthesize_tsquery(output, priority < parent_priority)
        }
        PgTsQueryNode::And(left, right) => {
            format_tsquery_binary(left, right, "&", 2, parent_priority, false)
        }
        PgTsQueryNode::Or(left, right) => {
            format_tsquery_binary(left, right, "|", 1, parent_priority, false)
        }
        PgTsQueryNode::Phrase {
            left,
            right,
            distance,
        } => {
            let operator = if *distance == 1 {
                "<->".to_string()
            } else {
                format!("<{distance}>")
            };
            format_tsquery_binary(left, right, &operator, 3, parent_priority, right_phrase)
        }
    }
}

fn format_tsquery_binary(
    left: &PgTsQueryNode,
    right: &PgTsQueryNode,
    operator: &str,
    priority: u8,
    parent_priority: u8,
    right_phrase: bool,
) -> String {
    let output = format!(
        "{} {operator} {}",
        format_tsquery_node(left, priority, false),
        format_tsquery_node(right, priority, priority == 3)
    );
    parenthesize_tsquery(
        output,
        priority < parent_priority || (priority == 3 && right_phrase),
    )
}

fn parenthesize_tsquery(output: String, needed: bool) -> String {
    if needed {
        format!("( {output} )")
    } else {
        output
    }
}

fn append_tsquery_node_key(node: &PgTsQueryNode, key: &mut Vec<u8>) {
    match node {
        PgTsQueryNode::Operand(operand) => {
            key.push(1);
            let signed_order =
                (postgres_legacy_crc32(operand.text.as_bytes()) as i32 as u32) ^ 0x8000_0000;
            key.extend((!signed_order).to_be_bytes());
            key.extend(operand.text.as_bytes());
            key.push(0);
        }
        PgTsQueryNode::Not(child) => {
            key.extend([0, 3, 0xff, 0xfe]);
            append_tsquery_node_key(child, key);
        }
        PgTsQueryNode::And(left, right) => {
            append_tsquery_operator_key(2, left, right, None, key);
        }
        PgTsQueryNode::Or(left, right) => {
            append_tsquery_operator_key(1, left, right, None, key);
        }
        PgTsQueryNode::Phrase {
            left,
            right,
            distance,
        } => append_tsquery_operator_key(0, left, right, Some(*distance), key),
    }
}

fn append_tsquery_operator_key(
    operator_order: u8,
    left: &PgTsQueryNode,
    right: &PgTsQueryNode,
    distance: Option<u16>,
    key: &mut Vec<u8>,
) {
    key.extend([0, operator_order, 0xff, 0xfd]);
    // PostgreSQL's prefix representation stores and compares the right child first.
    append_tsquery_node_key(right, key);
    append_tsquery_node_key(left, key);
    if let Some(distance) = distance {
        key.extend((u16::MAX - distance).to_be_bytes());
    }
}

fn postgres_legacy_crc32(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for byte in bytes {
        let index = ((crc >> 24) as u8 ^ byte) as u32;
        crc = postgres_crc32_table_entry(index) ^ (crc << 8);
    }
    crc ^ u32::MAX
}

fn postgres_crc32_table_entry(mut value: u32) -> u32 {
    for _ in 0..8 {
        value = if value & 1 != 0 {
            (value >> 1) ^ 0xedb8_8320
        } else {
            value >> 1
        };
    }
    value
}

/// The text-search parsers are recursive descent over CLIENT-SUPPLIED text:
/// without a budget, `to_tsquery('((((((…')` recurses once per byte and
/// overflows the thread stack — a hardware fault no `catch_unwind` can
/// contain, so a read-only query would kill the whole process. See
/// `bicdb_core::parse_budget` for the rules these uphold.
fn budget_error(error: bicdb_core::parse_budget::BudgetExceeded) -> SqlError {
    SqlError::InvalidSql(error.message().to_string())
}

struct TsQueryParser<'a> {
    input: &'a str,
    offset: usize,
    enforce_lexeme_limit: bool,
    budget: bicdb_core::parse_budget::ParseBudget,
}

impl<'a> TsQueryParser<'a> {
    fn new(input: &'a str, enforce_lexeme_limit: bool) -> Self {
        Self {
            input,
            offset: 0,
            enforce_lexeme_limit,
            budget: bicdb_core::parse_budget::ParseBudget::new("tsquery"),
        }
    }

    /// Run `body` one nesting level deeper, refusing runaway nesting.
    fn nested<T>(&mut self, body: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        self.budget.enter().map_err(budget_error)?;
        let result = body(self);
        self.budget.leave();
        result
    }

    /// Charge one level of TREE depth for a left-associative combination.
    ///
    /// `a | b | c | …` parses in a loop, so the parser never recurses — but
    /// each step wraps the accumulated node, so N operands build an N-deep
    /// left-leaning tree. The nesting budget only covered constructs that
    /// recurse while parsing (parentheses), and the node budget is a million,
    /// so a few hundred thousand operands passed both and then overflowed the
    /// stack during evaluation and in the tree's own recursive `Drop`. A stack
    /// overflow aborts the process for every tenant and `catch_unwind` cannot
    /// contain it. There is no matching `leave`: the level persists in the
    /// tree.
    fn charge_tree_depth(&mut self) -> Result<()> {
        self.budget.enter().map_err(budget_error)
    }

    fn parse_or(&mut self) -> Result<PgTsQueryNode> {
        let mut node = self.parse_and()?;
        loop {
            self.skip_whitespace();
            if !self.consume_byte(b'|') {
                return Ok(node);
            }
            self.charge_tree_depth()?;
            node = PgTsQueryNode::Or(Box::new(node), Box::new(self.parse_and()?));
        }
    }

    fn parse_and(&mut self) -> Result<PgTsQueryNode> {
        let mut node = self.parse_phrase()?;
        loop {
            self.skip_whitespace();
            if !self.consume_byte(b'&') {
                return Ok(node);
            }
            self.charge_tree_depth()?;
            node = PgTsQueryNode::And(Box::new(node), Box::new(self.parse_phrase()?));
        }
    }

    fn parse_phrase(&mut self) -> Result<PgTsQueryNode> {
        let mut node = self.parse_not()?;
        loop {
            self.skip_whitespace();
            let Some(distance) = self.parse_phrase_operator()? else {
                return Ok(node);
            };
            self.charge_tree_depth()?;
            node = PgTsQueryNode::Phrase {
                left: Box::new(node),
                right: Box::new(self.parse_not()?),
                distance,
            };
        }
    }

    fn parse_not(&mut self) -> Result<PgTsQueryNode> {
        self.skip_whitespace();
        if self.consume_byte(b'!') {
            return Ok(PgTsQueryNode::Not(Box::new(
                self.nested(|parser| parser.parse_not())?,
            )));
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<PgTsQueryNode> {
        self.skip_whitespace();
        if self.consume_byte(b'(') {
            let node = self.nested(|parser| parser.parse_or())?;
            self.skip_whitespace();
            if !self.consume_byte(b')') {
                return Err(tsquery_syntax_error(self.input));
            }
            return Ok(node);
        }
        self.parse_operand().map(PgTsQueryNode::Operand)
    }

    fn parse_operand(&mut self) -> Result<PgTsQueryOperand> {
        self.skip_whitespace();
        let text = if self.current_byte() == Some(b'\'') {
            self.offset += 1;
            self.parse_quoted_operand()?
        } else {
            self.parse_unquoted_operand()?
        };
        if text.is_empty() || (self.enforce_lexeme_limit && text.len() > MAX_LEXEME_BYTES) {
            return Err(tsquery_syntax_error(self.input));
        }
        let mut weights = 0;
        let mut prefix = false;
        if self.consume_byte(b':') {
            loop {
                match self.current_byte() {
                    Some(b'*') => {
                        prefix = true;
                        self.offset += 1;
                    }
                    Some(letter @ (b'A' | b'B' | b'C' | b'D')) => {
                        weights |= PgTsWeight::parse(Some(letter)).unwrap().query_bit();
                        self.offset += 1;
                    }
                    Some(letter @ (b'a' | b'b' | b'c' | b'd')) => {
                        weights |= PgTsWeight::parse(Some(letter.to_ascii_uppercase()))
                            .unwrap()
                            .query_bit();
                        self.offset += 1;
                    }
                    Some(letter) if letter.is_ascii_alphabetic() => {
                        return Err(tsquery_syntax_error(self.input));
                    }
                    _ => break,
                }
            }
        }
        Ok(PgTsQueryOperand {
            text,
            weights,
            prefix,
        })
    }

    fn parse_quoted_operand(&mut self) -> Result<String> {
        let mut output = String::new();
        while self.offset < self.input.len() {
            let character = self.input[self.offset..].chars().next().unwrap();
            self.offset += character.len_utf8();
            match character {
                '\'' if self.current_byte() == Some(b'\'') => {
                    self.offset += 1;
                    output.push('\'');
                }
                '\'' => return Ok(output),
                '\\' => output.push(self.take_escaped_character()?),
                other => output.push(other),
            }
        }
        Err(tsquery_syntax_error(self.input))
    }

    fn parse_unquoted_operand(&mut self) -> Result<String> {
        let mut output = String::new();
        while self.offset < self.input.len() {
            let character = self.input[self.offset..].chars().next().unwrap();
            if character.is_whitespace()
                || matches!(character, ':' | '!' | '&' | '|' | '(' | ')' | '<')
            {
                break;
            }
            self.offset += character.len_utf8();
            if character == '\\' {
                output.push(self.take_escaped_character()?);
            } else {
                output.push(character);
            }
        }
        Ok(output)
    }

    fn parse_phrase_operator(&mut self) -> Result<Option<u16>> {
        if !self.remaining().starts_with('<') {
            return Ok(None);
        }
        if self.remaining().starts_with("<->") {
            self.offset += 3;
            return Ok(Some(1));
        }
        let original = self.offset;
        self.offset += 1;
        let start = self.offset;
        while self
            .current_byte()
            .is_some_and(|byte| byte.is_ascii_digit())
        {
            self.offset += 1;
        }
        if start == self.offset || !self.consume_byte(b'>') {
            self.offset = original;
            return Err(tsquery_syntax_error(self.input));
        }
        let distance = self.input[start..self.offset - 1]
            .parse::<u32>()
            .map_err(|_| tsquery_distance_error())?;
        if distance > 16_384 {
            return Err(tsquery_distance_error());
        }
        Ok(Some(distance as u16))
    }

    fn take_escaped_character(&mut self) -> Result<char> {
        let character = self
            .remaining()
            .chars()
            .next()
            .ok_or_else(|| tsquery_syntax_error(self.input))?;
        self.offset += character.len_utf8();
        Ok(character)
    }

    fn skip_whitespace(&mut self) {
        while let Some(character) = self.remaining().chars().next() {
            if !character.is_whitespace() {
                break;
            }
            self.offset += character.len_utf8();
        }
    }

    fn consume_byte(&mut self, expected: u8) -> bool {
        if self.current_byte() == Some(expected) {
            self.offset += 1;
            true
        } else {
            false
        }
    }

    fn current_byte(&self) -> Option<u8> {
        self.input.as_bytes().get(self.offset).copied()
    }

    fn remaining(&self) -> &'a str {
        &self.input[self.offset..]
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueryMatch {
    No,
    Yes,
    Maybe,
}

#[derive(Clone, Debug, Default)]
struct PhraseData {
    positions: Vec<u16>,
    negate: bool,
    width: u16,
}

fn matches_query_node(node: &PgTsQueryNode, vector: &PgTsVector) -> bool {
    match node {
        PgTsQueryNode::Operand(operand) => operand_matches(operand, vector, None) != QueryMatch::No,
        PgTsQueryNode::Not(child) => !matches_query_node(child, vector),
        PgTsQueryNode::And(left, right) => {
            matches_query_node(left, vector) && matches_query_node(right, vector)
        }
        PgTsQueryNode::Or(left, right) => {
            matches_query_node(left, vector) || matches_query_node(right, vector)
        }
        PgTsQueryNode::Phrase { .. } => {
            let mut data = PhraseData::default();
            phrase_match(node, vector, &mut data) == QueryMatch::Yes
        }
    }
}

fn operand_matches(
    operand: &PgTsQueryOperand,
    vector: &PgTsVector,
    mut data: Option<&mut PhraseData>,
) -> QueryMatch {
    let mut found = false;
    let mut missing_positions = false;
    let mut positions = Vec::new();
    for lexeme in &vector.lexemes {
        let text_matches = if operand.prefix {
            lexeme.text.starts_with(&operand.text)
        } else {
            lexeme.text == operand.text
        };
        if !text_matches {
            continue;
        }
        found = true;
        if lexeme.positions.is_empty() {
            missing_positions = true;
            continue;
        }
        positions.extend(
            lexeme
                .positions
                .iter()
                .filter(|position| {
                    operand.weights == 0 || operand.weights & position.weight.query_bit() != 0
                })
                .map(|position| position.position),
        );
    }
    if data.is_none() {
        return if found && (operand.weights == 0 || missing_positions || !positions.is_empty()) {
            QueryMatch::Yes
        } else {
            QueryMatch::No
        };
    }
    if missing_positions {
        return QueryMatch::Maybe;
    }
    positions.sort_unstable();
    positions.dedup();
    if positions.is_empty() {
        QueryMatch::No
    } else {
        data.as_mut().unwrap().positions = positions;
        QueryMatch::Yes
    }
}

fn phrase_match(node: &PgTsQueryNode, vector: &PgTsVector, data: &mut PhraseData) -> QueryMatch {
    match node {
        PgTsQueryNode::Operand(operand) => operand_matches(operand, vector, Some(data)),
        PgTsQueryNode::Not(child) => match phrase_match(child, vector, data) {
            QueryMatch::No => {
                data.negate = true;
                QueryMatch::Yes
            }
            QueryMatch::Yes if !data.positions.is_empty() => {
                data.negate = !data.negate;
                QueryMatch::Yes
            }
            QueryMatch::Yes if data.negate => {
                data.negate = false;
                QueryMatch::No
            }
            value => value,
        },
        PgTsQueryNode::And(left, right) => phrase_binary(left, right, None, vector, data),
        PgTsQueryNode::Or(left, right) => phrase_or(left, right, vector, data),
        PgTsQueryNode::Phrase {
            left,
            right,
            distance,
        } => phrase_binary(left, right, Some(*distance), vector, data),
    }
}

fn phrase_binary(
    left: &PgTsQueryNode,
    right: &PgTsQueryNode,
    distance: Option<u16>,
    vector: &PgTsVector,
    output: &mut PhraseData,
) -> QueryMatch {
    let mut left_data = PhraseData::default();
    let mut right_data = PhraseData::default();
    let left_match = phrase_match(left, vector, &mut left_data);
    let right_match = phrase_match(right, vector, &mut right_data);
    if left_match == QueryMatch::No || right_match == QueryMatch::No {
        return QueryMatch::No;
    }
    if left_match == QueryMatch::Maybe || right_match == QueryMatch::Maybe {
        return QueryMatch::Maybe;
    }
    let (left_offset, right_offset) = if let Some(distance) = distance {
        output.width = distance
            .saturating_add(left_data.width)
            .saturating_add(right_data.width);
        (distance.saturating_add(right_data.width), 0)
    } else {
        output.width = left_data.width.max(right_data.width);
        (
            output.width - left_data.width,
            output.width - right_data.width,
        )
    };
    let left_positions = shifted_positions(&left_data.positions, left_offset);
    let right_positions = shifted_positions(&right_data.positions, right_offset);
    output.positions = match (left_data.negate, right_data.negate) {
        (true, true) => {
            output.negate = true;
            union_positions(&left_positions, &right_positions)
        }
        (true, false) => difference_positions(&right_positions, &left_positions),
        (false, true) => difference_positions(&left_positions, &right_positions),
        (false, false) => intersect_positions(&left_positions, &right_positions),
    };
    if output.negate || !output.positions.is_empty() {
        QueryMatch::Yes
    } else {
        QueryMatch::No
    }
}

fn phrase_or(
    left: &PgTsQueryNode,
    right: &PgTsQueryNode,
    vector: &PgTsVector,
    output: &mut PhraseData,
) -> QueryMatch {
    let mut left_data = PhraseData::default();
    let mut right_data = PhraseData::default();
    let left_match = phrase_match(left, vector, &mut left_data);
    let right_match = phrase_match(right, vector, &mut right_data);
    if left_match == QueryMatch::Maybe || right_match == QueryMatch::Maybe {
        return QueryMatch::Maybe;
    }
    if left_match == QueryMatch::No && right_match == QueryMatch::No {
        return QueryMatch::No;
    }
    output.width = left_data.width.max(right_data.width);
    let left_positions = shifted_positions(
        &left_data.positions,
        output.width.saturating_sub(left_data.width),
    );
    let right_positions = shifted_positions(
        &right_data.positions,
        output.width.saturating_sub(right_data.width),
    );
    output.positions = match (left_data.negate, right_data.negate) {
        (true, true) => {
            output.negate = true;
            intersect_positions(&left_positions, &right_positions)
        }
        (true, false) => {
            output.negate = true;
            difference_positions(&left_positions, &right_positions)
        }
        (false, true) => {
            output.negate = true;
            difference_positions(&right_positions, &left_positions)
        }
        (false, false) => union_positions(&left_positions, &right_positions),
    };
    QueryMatch::Yes
}

fn shifted_positions(positions: &[u16], offset: u16) -> Vec<u16> {
    positions
        .iter()
        .filter_map(|position| position.checked_add(offset))
        .collect()
}

fn union_positions(left: &[u16], right: &[u16]) -> Vec<u16> {
    let mut output = left.to_vec();
    output.extend_from_slice(right);
    output.sort_unstable();
    output.dedup();
    output
}

fn intersect_positions(left: &[u16], right: &[u16]) -> Vec<u16> {
    left.iter()
        .copied()
        .filter(|position| right.binary_search(position).is_ok())
        .collect()
}

fn difference_positions(left: &[u16], right: &[u16]) -> Vec<u16> {
    left.iter()
        .copied()
        .filter(|position| right.binary_search(position).is_err())
        .collect()
}

fn tsquery_syntax_error(input: &str) -> SqlError {
    SqlError::invalid_text_representation(
        "tsquery",
        format!("syntax error in tsquery: \"{input}\""),
    )
}

fn tsquery_distance_error() -> SqlError {
    SqlError::data_exception(
        "22023",
        "distance in phrase operator must be an integer value between zero and 16384 inclusive",
        Some("tsquery".to_string()),
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TextSearchConfig {
    Simple,
    Snowball {
        algorithm: Algorithm,
        stopwords: &'static str,
    },
}

impl TextSearchConfig {
    fn parse(value: &SqlValue) -> Result<Self> {
        let input = value.to_cell().to_ascii_lowercase();
        let input = input.strip_prefix("pg_catalog.").unwrap_or(&input);
        let value = input
            .parse::<i64>()
            .ok()
            .and_then(builtin_regconfig_name)
            .unwrap_or(input);
        match value {
            "simple" => Ok(Self::Simple),
            "arabic" => Ok(snowball_config(Algorithm::Arabic, "")),
            "armenian" => Ok(snowball_config(Algorithm::Armenian, "")),
            "basque" => Ok(snowball_config(Algorithm::Basque, "")),
            "catalan" => Ok(snowball_config(Algorithm::Catalan, "")),
            "danish" => Ok(snowball_config(
                Algorithm::Danish,
                include_str!("../assets/postgresql-18-stopwords/danish.stop"),
            )),
            "dutch" => Ok(snowball_config(
                Algorithm::Dutch,
                include_str!("../assets/postgresql-18-stopwords/dutch.stop"),
            )),
            "english" => Ok(snowball_config(
                Algorithm::English,
                include_str!("../assets/postgresql-18-stopwords/english.stop"),
            )),
            "estonian" => Ok(snowball_config(Algorithm::Estonian, "")),
            "finnish" => Ok(snowball_config(
                Algorithm::Finnish,
                include_str!("../assets/postgresql-18-stopwords/finnish.stop"),
            )),
            "french" => Ok(snowball_config(
                Algorithm::French,
                include_str!("../assets/postgresql-18-stopwords/french.stop"),
            )),
            "german" => Ok(snowball_config(
                Algorithm::German,
                include_str!("../assets/postgresql-18-stopwords/german.stop"),
            )),
            "greek" => Ok(snowball_config(Algorithm::Greek, "")),
            "hindi" => Ok(snowball_config(Algorithm::Hindi, "")),
            "hungarian" => Ok(snowball_config(
                Algorithm::Hungarian,
                include_str!("../assets/postgresql-18-stopwords/hungarian.stop"),
            )),
            "indonesian" => Ok(snowball_config(Algorithm::Indonesian, "")),
            "irish" => Ok(snowball_config(Algorithm::Irish, "")),
            "italian" => Ok(snowball_config(
                Algorithm::Italian,
                include_str!("../assets/postgresql-18-stopwords/italian.stop"),
            )),
            "lithuanian" => Ok(snowball_config(Algorithm::Lithuanian, "")),
            "nepali" => Ok(snowball_config(
                Algorithm::Nepali,
                include_str!("../assets/postgresql-18-stopwords/nepali.stop"),
            )),
            "norwegian" => Ok(snowball_config(
                Algorithm::Norwegian,
                include_str!("../assets/postgresql-18-stopwords/norwegian.stop"),
            )),
            "portuguese" => Ok(snowball_config(
                Algorithm::Portuguese,
                include_str!("../assets/postgresql-18-stopwords/portuguese.stop"),
            )),
            "romanian" => Ok(snowball_config(Algorithm::Romanian, "")),
            "russian" => Ok(snowball_config(
                Algorithm::Russian,
                include_str!("../assets/postgresql-18-stopwords/russian.stop"),
            )),
            "serbian" => Ok(snowball_config(Algorithm::Serbian, "")),
            "spanish" => Ok(snowball_config(
                Algorithm::Spanish,
                include_str!("../assets/postgresql-18-stopwords/spanish.stop"),
            )),
            "swedish" => Ok(snowball_config(
                Algorithm::Swedish,
                include_str!("../assets/postgresql-18-stopwords/swedish.stop"),
            )),
            "tamil" => Ok(snowball_config(Algorithm::Tamil, "")),
            "turkish" => Ok(snowball_config(
                Algorithm::Turkish,
                include_str!("../assets/postgresql-18-stopwords/turkish.stop"),
            )),
            "yiddish" => Ok(snowball_config(Algorithm::Yiddish, "")),
            _ => Err(SqlError::data_exception(
                "42704",
                format!("text search configuration \"{value}\" does not exist"),
                Some("regconfig".to_string()),
            )),
        }
    }

    fn normalize(self, token: &str) -> Option<String> {
        // Reject before lowercasing/stemming so a multi-megabyte
        // no-whitespace token cannot amplify tokenizer memory. Check again
        // after each transformation because Unicode case conversion and
        // stemming can change the UTF-8 byte length.
        if !bicdb_core::full_text_term_is_indexable(token) {
            return None;
        }
        let token = token.to_lowercase();
        if token.is_empty() || !bicdb_core::full_text_term_is_indexable(&token) {
            return None;
        }
        let normalized = match self {
            Self::Simple => Some(token),
            Self::Snowball { stopwords, .. } if is_stopword(stopwords, &token) => None,
            Self::Snowball { algorithm, .. } => {
                let stem = match algorithm {
                    Algorithm::Dutch => LegacyStemmer::create(LegacyAlgorithm::Dutch)
                        .stem(&token)
                        .into_owned(),
                    Algorithm::Tamil => LegacyStemmer::create(LegacyAlgorithm::Tamil)
                        .stem(&token)
                        .into_owned(),
                    algorithm => Stemmer::new(algorithm).stem(&token).into_owned(),
                };
                Some(stem)
            }
        };
        normalized.filter(|lexeme| bicdb_core::full_text_term_is_indexable(lexeme))
    }
}

fn snowball_config(algorithm: Algorithm, stopwords: &'static str) -> TextSearchConfig {
    TextSearchConfig::Snowball {
        algorithm,
        stopwords,
    }
}

fn english_config() -> TextSearchConfig {
    snowball_config(
        Algorithm::English,
        include_str!("../assets/postgresql-18-stopwords/english.stop"),
    )
}

fn is_stopword(stopwords: &str, token: &str) -> bool {
    stopwords.lines().any(|word| word.trim() == token)
}

fn text_search_tokens(input: &str) -> Vec<(String, u16)> {
    let mut tokens = Vec::new();
    let mut position = 0u16;
    let mut offset = 0usize;
    while offset < input.len() {
        let Some(character) = input[offset..].chars().next() else {
            break;
        };
        if !character.is_alphanumeric() && character != '/' {
            offset += character.len_utf8();
            continue;
        }

        if let Some((end, expanded)) = scan_url(input, offset) {
            for token in expanded {
                push_text_search_token(&mut tokens, &mut position, token);
            }
            offset = end;
            continue;
        }
        if character == '/' {
            let end = scan_while(input, offset, is_path_character);
            push_text_search_token(&mut tokens, &mut position, input[offset..end].to_string());
            offset = end;
            continue;
        }

        let end = scan_while(input, offset, is_text_token_character);
        let candidate = input[offset..end].trim_end_matches('.');
        if candidate.contains('@') && valid_email(candidate) {
            push_text_search_token(&mut tokens, &mut position, candidate.to_string());
        } else if candidate.contains('-') {
            let parts = candidate
                .split('-')
                .filter(|part| !part.is_empty())
                .collect::<Vec<_>>();
            if parts.len() > 1 {
                push_text_search_token(&mut tokens, &mut position, candidate.to_string());
                for part in parts {
                    push_text_search_token(&mut tokens, &mut position, part.to_string());
                }
            } else {
                push_text_search_token(&mut tokens, &mut position, candidate.to_string());
            }
        } else if !candidate.is_empty() {
            push_text_search_token(&mut tokens, &mut position, candidate.to_string());
        }
        offset = end.max(offset + character.len_utf8());
    }
    tokens
}

fn push_text_search_token(tokens: &mut Vec<(String, u16)>, position: &mut u16, token: String) {
    if token.is_empty() {
        return;
    }
    *position = position.saturating_add(1).min(MAX_POSITION);
    tokens.push((token, *position));
}

fn scan_while(input: &str, mut offset: usize, predicate: impl Fn(char) -> bool) -> usize {
    while offset < input.len() {
        let character = input[offset..].chars().next().unwrap();
        if !predicate(character) {
            break;
        }
        offset += character.len_utf8();
    }
    offset
}

fn is_text_token_character(character: char) -> bool {
    character.is_alphanumeric() || character.is_mark() || matches!(character, '.' | '@' | '-')
}

fn is_path_character(character: char) -> bool {
    !character.is_whitespace() && !matches!(character, ',' | ';' | ')' | ']' | '}')
}

fn valid_email(value: &str) -> bool {
    let Some((local, host)) = value.rsplit_once('@') else {
        return false;
    };
    !local.is_empty()
        && host.contains('.')
        && host
            .chars()
            .all(|character| character.is_alphanumeric() || matches!(character, '.' | '-'))
}

fn scan_url(input: &str, offset: usize) -> Option<(usize, Vec<String>)> {
    let protocol_end = input[offset..]
        .find("://")
        .map(|relative| offset + relative)?;
    if protocol_end == offset
        || !input[offset..protocol_end].chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '+' | '-' | '.')
        })
    {
        return None;
    }
    let value_start = protocol_end + 3;
    let end = scan_while(input, value_start, is_path_character);
    if end == value_start {
        return None;
    }
    let value = &input[value_start..end];
    let mut expanded = vec![value.to_string()];
    if let Some(path_offset) = value.find('/') {
        let host = &value[..path_offset];
        let path = &value[path_offset..];
        if !host.is_empty() {
            expanded.push(host.to_string());
        }
        if !path.is_empty() {
            expanded.push(path.to_string());
        }
    }
    Some((end, expanded))
}

fn to_tsvector(config: TextSearchConfig, input: &str) -> PgTsVector {
    let mut entries = BTreeMap::<String, Vec<PgTsPosition>>::new();
    for (token, position) in text_search_tokens(input) {
        if let Some(lexeme) = config.normalize(&token) {
            entries.entry(lexeme).or_default().push(PgTsPosition {
                position,
                weight: PgTsWeight::D,
            });
        }
    }
    PgTsVector {
        lexemes: entries
            .into_iter()
            .map(|(text, positions)| PgTsLexeme { text, positions })
            .collect(),
    }
}

pub(crate) fn application_websearch_rank_cd_english(fields: &[String], query: &str) -> f32 {
    let config = TextSearchConfig::parse(&SqlValue::String("english".to_string()))
        .expect("the built-in English text-search configuration exists");
    let mut document = PgTsVector::default();
    for (index, field) in fields.iter().enumerate() {
        let weight = match index {
            0 => PgTsWeight::A,
            1 => PgTsWeight::B,
            2 => PgTsWeight::C,
            _ => PgTsWeight::D,
        };
        document = document.concat(&setweight(&to_tsvector(config, field), weight, None));
    }
    let query = websearch_tsquery(config, query);
    ts_rank_cd(&document, &query, [0.1, 0.2, 0.4, 1.0], 0)
}

#[derive(Clone, Debug, Default)]
struct NormalizedQueryNode {
    node: Option<PgTsQueryNode>,
    leading_gap: u16,
    trailing_gap: u16,
}

fn normalize_query_node(node: PgTsQueryNode, config: TextSearchConfig) -> NormalizedQueryNode {
    match node {
        PgTsQueryNode::Operand(mut operand) => match config.normalize(&operand.text) {
            Some(text) => {
                operand.text = text;
                NormalizedQueryNode {
                    node: Some(PgTsQueryNode::Operand(operand)),
                    ..Default::default()
                }
            }
            None => NormalizedQueryNode::default(),
        },
        PgTsQueryNode::Not(child) => {
            let child = normalize_query_node(*child, config);
            NormalizedQueryNode {
                node: child.node.map(|node| PgTsQueryNode::Not(Box::new(node))),
                leading_gap: child.leading_gap,
                trailing_gap: child.trailing_gap,
            }
        }
        PgTsQueryNode::And(left, right) => normalize_boolean_query(*left, *right, config, true),
        PgTsQueryNode::Or(left, right) => normalize_boolean_query(*left, *right, config, false),
        PgTsQueryNode::Phrase {
            left,
            right,
            distance,
        } => {
            let left = normalize_query_node(*left, config);
            let right = normalize_query_node(*right, config);
            match (left.node, right.node) {
                (Some(left_node), Some(right_node)) => NormalizedQueryNode {
                    node: Some(PgTsQueryNode::Phrase {
                        left: Box::new(left_node),
                        right: Box::new(right_node),
                        distance: distance
                            .saturating_add(left.trailing_gap)
                            .saturating_add(right.leading_gap),
                    }),
                    leading_gap: left.leading_gap,
                    trailing_gap: right.trailing_gap,
                },
                (Some(node), None) => NormalizedQueryNode {
                    node: Some(node),
                    leading_gap: left.leading_gap,
                    trailing_gap: left.trailing_gap.saturating_add(distance),
                },
                (None, Some(node)) => NormalizedQueryNode {
                    node: Some(node),
                    leading_gap: distance.saturating_add(right.leading_gap),
                    trailing_gap: right.trailing_gap,
                },
                (None, None) => NormalizedQueryNode {
                    node: None,
                    leading_gap: left
                        .leading_gap
                        .saturating_add(distance)
                        .saturating_add(right.leading_gap),
                    trailing_gap: 0,
                },
            }
        }
    }
}

fn normalize_boolean_query(
    left: PgTsQueryNode,
    right: PgTsQueryNode,
    config: TextSearchConfig,
    and: bool,
) -> NormalizedQueryNode {
    let left = normalize_query_node(left, config);
    let right = normalize_query_node(right, config);
    let node = match (left.node, right.node) {
        (Some(left), Some(right)) if and => {
            Some(PgTsQueryNode::And(Box::new(left), Box::new(right)))
        }
        (Some(left), Some(right)) => Some(PgTsQueryNode::Or(Box::new(left), Box::new(right))),
        (Some(node), None) | (None, Some(node)) => Some(node),
        (None, None) => None,
    };
    NormalizedQueryNode {
        node,
        ..Default::default()
    }
}

fn to_tsquery(config: TextSearchConfig, input: &str) -> Result<PgTsQuery> {
    // SQL text-search constructors share the same oversize policy as
    // document tokenization. Explicit `::tsquery` input still enforces the
    // PostgreSQL representation limit in `from_postgres_text`.
    let query = PgTsQuery::from_postgres_text_with_limit(input, false)?;
    Ok(PgTsQuery {
        root: query
            .root
            .and_then(|root| normalize_query_node(root, config).node),
    })
}

fn plain_tsquery(config: TextSearchConfig, input: &str, phrase: bool) -> PgTsQuery {
    let mut terms = text_search_tokens(input)
        .into_iter()
        .filter_map(|(token, position)| {
            config.normalize(&token).map(|text| {
                (
                    PgTsQueryNode::Operand(PgTsQueryOperand {
                        text,
                        weights: 0,
                        prefix: false,
                    }),
                    position,
                )
            })
        });
    let Some((mut root, mut previous_position)) = terms.next() else {
        return PgTsQuery::default();
    };
    for (term, position) in terms {
        root = if phrase {
            PgTsQueryNode::Phrase {
                left: Box::new(root),
                right: Box::new(term),
                distance: position.saturating_sub(previous_position),
            }
        } else {
            PgTsQueryNode::And(Box::new(root), Box::new(term))
        };
        previous_position = position;
    }
    PgTsQuery { root: Some(root) }
}

fn fts_config_and_text(args: &[SqlValue]) -> Result<(TextSearchConfig, &str)> {
    match args {
        [text] => Ok((english_config(), sql_value_str(text)?)),
        [config, text] => Ok((TextSearchConfig::parse(config)?, sql_value_str(text)?)),
        _ => Err(SqlError::InvalidSql(format!(
            "text search function expects 1 or 2 arguments, got {}",
            args.len()
        ))),
    }
}

fn json_text_segments(value: &serde_json::Value, output: &mut Vec<String>) {
    match value {
        serde_json::Value::String(value) => output.push(value.clone()),
        serde_json::Value::Array(values) => {
            for value in values {
                json_text_segments(value, output);
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values() {
                json_text_segments(value, output);
            }
        }
        _ => {}
    }
}

fn json_to_tsvector(config: TextSearchConfig, value: &serde_json::Value) -> PgTsVector {
    let mut segments = Vec::new();
    json_text_segments(value, &mut segments);
    let mut result = PgTsVector::default();
    for (index, segment) in segments.iter().enumerate() {
        if index > 0 {
            result = result.concat(&PgTsVector::from_postgres_text("'_gap':1").unwrap());
        }
        result = result.concat(&to_tsvector(config, segment));
    }
    delete_lexemes(&result, &["_gap".to_string()])
}

#[derive(Clone)]
struct HeadlineToken {
    start: usize,
    end: usize,
    normalized: Option<String>,
    highlight: bool,
}

#[derive(Clone)]
struct HeadlineOptions {
    start_sel: String,
    stop_sel: String,
    max_words: usize,
    min_words: usize,
    highlight_all: bool,
}

impl Default for HeadlineOptions {
    fn default() -> Self {
        Self {
            start_sel: "<b>".to_string(),
            stop_sel: "</b>".to_string(),
            max_words: 35,
            min_words: 15,
            highlight_all: false,
        }
    }
}

fn parse_headline_options(value: Option<&str>) -> Result<HeadlineOptions> {
    let mut options = HeadlineOptions::default();
    let Some(value) = value else {
        return Ok(options);
    };
    for setting in value.split(',') {
        let Some((name, value)) = setting.trim().split_once('=') else {
            return Err(SqlError::data_exception(
                "22023",
                "invalid headline option",
                None,
            ));
        };
        match name.trim().to_ascii_lowercase().as_str() {
            "startsel" => options.start_sel = value.trim().to_string(),
            "stopsel" => options.stop_sel = value.trim().to_string(),
            "maxwords" => {
                options.max_words = value.trim().parse().map_err(|_| {
                    SqlError::data_exception("22023", "invalid MaxWords value", None)
                })?
            }
            "minwords" => {
                options.min_words = value.trim().parse().map_err(|_| {
                    SqlError::data_exception("22023", "invalid MinWords value", None)
                })?
            }
            "highlightall" => {
                options.highlight_all = matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "true" | "on" | "yes" | "1"
                )
            }
            "shortword" | "maxfragments" | "fragmentdelimiter" => {}
            other => {
                return Err(SqlError::data_exception(
                    "22023",
                    format!("unrecognized headline parameter: {other}"),
                    None,
                ));
            }
        }
    }
    if options.min_words > options.max_words {
        return Err(SqlError::data_exception(
            "22023",
            "MinWords should be less than MaxWords",
            None,
        ));
    }
    Ok(options)
}

fn headline_positive_operands(
    node: &PgTsQueryNode,
    negated: bool,
    output: &mut Vec<PgTsQueryOperand>,
) {
    match node {
        PgTsQueryNode::Operand(operand) if !negated => output.push(operand.clone()),
        PgTsQueryNode::Operand(_) => {}
        PgTsQueryNode::Not(child) => headline_positive_operands(child, !negated, output),
        PgTsQueryNode::And(left, right) | PgTsQueryNode::Or(left, right) => {
            headline_positive_operands(left, negated, output);
            headline_positive_operands(right, negated, output);
        }
        PgTsQueryNode::Phrase { left, right, .. } => {
            headline_positive_operands(left, negated, output);
            headline_positive_operands(right, negated, output);
        }
    }
}

fn headline_tokens(
    config: TextSearchConfig,
    text: &str,
    operands: &[PgTsQueryOperand],
) -> Vec<HeadlineToken> {
    let mut output = Vec::new();
    let mut start = None;
    for (offset, character) in text
        .char_indices()
        .chain(std::iter::once((text.len(), ' ')))
    {
        if character.is_alphanumeric() || (character == '\'' && start.is_some()) {
            start.get_or_insert(offset);
        } else if let Some(token_start) = start.take() {
            let normalized = config.normalize(&text[token_start..offset]);
            let highlight = normalized.as_ref().is_some_and(|value| {
                operands.iter().any(|operand| {
                    if operand.prefix {
                        value.starts_with(&operand.text)
                    } else {
                        value == &operand.text
                    }
                })
            });
            output.push(HeadlineToken {
                start: token_start,
                end: offset,
                normalized,
                highlight,
            });
        }
    }
    output
}

fn text_headline(
    config: TextSearchConfig,
    text: &str,
    query: &PgTsQuery,
    options: &HeadlineOptions,
) -> String {
    let mut operands = Vec::new();
    if let Some(root) = &query.root {
        headline_positive_operands(root, false, &mut operands);
    }
    let tokens = headline_tokens(config, text, &operands);
    if tokens.is_empty() {
        return text.to_string();
    }
    let (mut first, mut last) = (0usize, tokens.len() - 1);
    if !options.highlight_all && tokens.len() > options.max_words {
        let mut best = None::<(usize, usize)>;
        for start in 0..tokens.len() {
            for end in start..tokens.len().min(start + options.max_words) {
                let vector = PgTsVector {
                    lexemes: tokens[start..=end]
                        .iter()
                        .enumerate()
                        .filter_map(|(index, token)| {
                            token.normalized.as_ref().map(|text| PgTsLexeme {
                                text: text.clone(),
                                positions: vec![PgTsPosition {
                                    position: (index + 1) as u16,
                                    weight: PgTsWeight::D,
                                }],
                            })
                        })
                        .collect(),
                };
                let vector = canonicalize_vector(vector);
                if query.matches(&vector) {
                    if best.is_none_or(|best| end - start < best.1 - best.0) {
                        best = Some((start, end));
                    }
                    break;
                }
            }
        }
        if let Some((start, end)) = best {
            first = start;
            last = end;
            while last + 1 - first < options.min_words && (first > 0 || last + 1 < tokens.len()) {
                if last + 1 < tokens.len() {
                    last += 1;
                } else {
                    first -= 1;
                }
            }
        } else {
            last = options.max_words.saturating_sub(1).min(tokens.len() - 1);
        }
    }
    render_headline(text, &tokens[first..=last], options)
}

fn canonicalize_vector(vector: PgTsVector) -> PgTsVector {
    let mut entries = BTreeMap::<String, Vec<PgTsPosition>>::new();
    for lexeme in vector.lexemes {
        entries
            .entry(lexeme.text)
            .or_default()
            .extend(lexeme.positions);
    }
    PgTsVector {
        lexemes: entries
            .into_iter()
            .map(|(text, positions)| PgTsLexeme { text, positions })
            .collect(),
    }
}

fn render_headline(text: &str, tokens: &[HeadlineToken], options: &HeadlineOptions) -> String {
    let Some(first) = tokens.first() else {
        return String::new();
    };
    let mut output = String::new();
    let mut cursor = first.start;
    for token in tokens {
        output.push_str(&text[cursor..token.start]);
        if token.highlight {
            output.push_str(&options.start_sel);
        }
        output.push_str(&text[token.start..token.end]);
        if token.highlight {
            output.push_str(&options.stop_sel);
        }
        cursor = token.end;
    }
    if tokens.len() == headline_tokens(TextSearchConfig::Simple, text, &[]).len() {
        output.push_str(&text[cursor..]);
    }
    output
}

fn json_headline(
    config: TextSearchConfig,
    value: &serde_json::Value,
    query: &PgTsQuery,
    options: &HeadlineOptions,
) -> serde_json::Value {
    match value {
        serde_json::Value::String(value) => {
            serde_json::Value::String(text_headline(config, value, query, options))
        }
        serde_json::Value::Array(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|value| json_headline(config, value, query, options))
                .collect(),
        ),
        serde_json::Value::Object(values) => serde_json::Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), json_headline(config, value, query, options)))
                .collect(),
        ),
        value => value.clone(),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RewriteNodeKind {
    Operand(PgTsQueryOperand),
    Not,
    And,
    Or,
    Phrase(u16),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RewriteNode {
    kind: RewriteNodeKind,
    // PostgreSQL's QTNode stores the displayed right child first.
    children: Vec<RewriteNode>,
    no_change: bool,
}

impl RewriteNode {
    fn from_query_node(node: PgTsQueryNode) -> Self {
        match node {
            PgTsQueryNode::Operand(operand) => Self {
                kind: RewriteNodeKind::Operand(operand),
                children: Vec::new(),
                no_change: false,
            },
            PgTsQueryNode::Not(child) => Self {
                kind: RewriteNodeKind::Not,
                children: vec![Self::from_query_node(*child)],
                no_change: false,
            },
            PgTsQueryNode::And(left, right) => Self {
                kind: RewriteNodeKind::And,
                children: vec![Self::from_query_node(*right), Self::from_query_node(*left)],
                no_change: false,
            },
            PgTsQueryNode::Or(left, right) => Self {
                kind: RewriteNodeKind::Or,
                children: vec![Self::from_query_node(*right), Self::from_query_node(*left)],
                no_change: false,
            },
            PgTsQueryNode::Phrase {
                left,
                right,
                distance,
            } => Self {
                kind: RewriteNodeKind::Phrase(distance),
                children: vec![Self::from_query_node(*right), Self::from_query_node(*left)],
                no_change: false,
            },
        }
    }

    fn canonicalize(&mut self) {
        for child in &mut self.children {
            child.canonicalize();
        }
        if matches!(self.kind, RewriteNodeKind::And | RewriteNodeKind::Or) {
            let kind = self.kind.clone();
            let mut flattened = Vec::new();
            for child in std::mem::take(&mut self.children) {
                if child.kind == kind {
                    flattened.extend(child.children);
                } else {
                    flattened.push(child);
                }
            }
            self.children = flattened;
        }
        if !matches!(self.kind, RewriteNodeKind::Phrase(_)) && self.children.len() > 1 {
            self.children.sort_by(rewrite_node_compare);
        }
    }

    fn into_query_node(self) -> Option<PgTsQueryNode> {
        match self.kind {
            RewriteNodeKind::Operand(operand) => Some(PgTsQueryNode::Operand(operand)),
            RewriteNodeKind::Not => self
                .children
                .into_iter()
                .next()
                .and_then(Self::into_query_node)
                .map(|child| PgTsQueryNode::Not(Box::new(child))),
            RewriteNodeKind::And => rewrite_children_to_binary(self.children, PgTsQueryNode::And),
            RewriteNodeKind::Or => rewrite_children_to_binary(self.children, PgTsQueryNode::Or),
            RewriteNodeKind::Phrase(distance) => {
                let mut children = self.children.into_iter();
                let right = children.next().and_then(Self::into_query_node)?;
                let left = children.next().and_then(Self::into_query_node)?;
                Some(PgTsQueryNode::Phrase {
                    left: Box::new(left),
                    right: Box::new(right),
                    distance,
                })
            }
        }
    }
}

fn rewrite_children_to_binary(
    children: Vec<RewriteNode>,
    constructor: impl Fn(Box<PgTsQueryNode>, Box<PgTsQueryNode>) -> PgTsQueryNode + Copy,
) -> Option<PgTsQueryNode> {
    let mut children = children
        .into_iter()
        .filter_map(RewriteNode::into_query_node);
    let mut node = children.next()?;
    for child in children {
        node = constructor(Box::new(child), Box::new(node));
    }
    Some(node)
}

fn rewrite_node_compare(left: &RewriteNode, right: &RewriteNode) -> Ordering {
    fn kind_order(kind: &RewriteNodeKind) -> u8 {
        match kind {
            RewriteNodeKind::Operand(_) => 0,
            RewriteNodeKind::Not => 1,
            RewriteNodeKind::And => 2,
            RewriteNodeKind::Or => 3,
            RewriteNodeKind::Phrase(_) => 4,
        }
    }

    let left_operator = !matches!(left.kind, RewriteNodeKind::Operand(_));
    let right_operator = !matches!(right.kind, RewriteNodeKind::Operand(_));
    match (left_operator, right_operator) {
        (true, false) => return Ordering::Less,
        (false, true) => return Ordering::Greater,
        _ => {}
    }
    match (&left.kind, &right.kind) {
        (RewriteNodeKind::Operand(left), RewriteNodeKind::Operand(right)) => {
            let left_crc = postgres_legacy_crc32(left.text.as_bytes()) as i32;
            let right_crc = postgres_legacy_crc32(right.text.as_bytes()) as i32;
            right_crc
                .cmp(&left_crc)
                .then_with(|| left.text.as_bytes().cmp(right.text.as_bytes()))
        }
        _ => kind_order(&right.kind)
            .cmp(&kind_order(&left.kind))
            .then_with(|| right.children.len().cmp(&left.children.len()))
            .then_with(|| {
                left.children
                    .iter()
                    .zip(&right.children)
                    .find_map(|(left, right)| {
                        let ordering = rewrite_node_compare(left, right);
                        (ordering != Ordering::Equal).then_some(ordering)
                    })
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| match (&left.kind, &right.kind) {
                (RewriteNodeKind::Phrase(left), RewriteNodeKind::Phrase(right)) => right.cmp(left),
                _ => Ordering::Equal,
            }),
    }
}

fn rewrite_tree(
    mut node: RewriteNode,
    target: &RewriteNode,
    substitute: Option<&RewriteNode>,
) -> (Option<RewriteNode>, bool) {
    if node.no_change || rewrite_node_compare(&node, target) != Ordering::Equal {
        if node.no_change
            || std::mem::discriminant(&node.kind) != std::mem::discriminant(&target.kind)
        {
            return rewrite_tree_children(node, target, substitute);
        }
    } else {
        let replacement = substitute.cloned().map(|mut replacement| {
            replacement.no_change = true;
            replacement
        });
        return (replacement, true);
    }

    if matches!(node.kind, RewriteNodeKind::And | RewriteNodeKind::Or)
        && node.children.len() > target.children.len()
        && !target.children.is_empty()
    {
        let mut matched = vec![false; node.children.len()];
        let (mut node_index, mut target_index) = (0, 0);
        while node_index < node.children.len() && target_index < target.children.len() {
            match rewrite_node_compare(&node.children[node_index], &target.children[target_index]) {
                Ordering::Equal => {
                    matched[node_index] = true;
                    node_index += 1;
                    target_index += 1;
                }
                Ordering::Less => node_index += 1,
                Ordering::Greater => break,
            }
        }
        if target_index == target.children.len() {
            node.children = node
                .children
                .into_iter()
                .zip(matched)
                .filter_map(|(child, matched)| (!matched).then_some(child))
                .collect();
            if let Some(substitute) = substitute {
                let mut substitute = substitute.clone();
                substitute.no_change = true;
                node.children.push(substitute);
            }
            node.children.sort_by(rewrite_node_compare);
            return (Some(node), true);
        }
    }
    rewrite_tree_children(node, target, substitute)
}

fn rewrite_tree_children(
    mut node: RewriteNode,
    target: &RewriteNode,
    substitute: Option<&RewriteNode>,
) -> (Option<RewriteNode>, bool) {
    if node.no_change || matches!(node.kind, RewriteNodeKind::Operand(_)) {
        return (Some(node), false);
    }
    let mut changed = false;
    node.children = node
        .children
        .into_iter()
        .filter_map(|child| {
            let (child, child_changed) = rewrite_tree(child, target, substitute);
            changed |= child_changed;
            child
        })
        .collect();
    match node.children.len() {
        0 => (None, changed),
        1 if !matches!(node.kind, RewriteNodeKind::Not) => {
            (node.children.into_iter().next(), changed)
        }
        _ => (Some(node), changed),
    }
}

fn rewrite_query(query: PgTsQuery, target: &PgTsQuery, substitute: &PgTsQuery) -> PgTsQuery {
    if query.root.is_none() || target.root.is_none() {
        return query;
    }
    let mut query = RewriteNode::from_query_node(query.root.unwrap());
    let mut target = RewriteNode::from_query_node(target.root.clone().unwrap());
    query.canonicalize();
    target.canonicalize();
    let substitute = substitute.root.clone().map(RewriteNode::from_query_node);
    PgTsQuery {
        root: rewrite_tree(query, &target, substitute.as_ref())
            .0
            .and_then(RewriteNode::into_query_node),
    }
}

fn sql_value_str(value: &SqlValue) -> Result<&str> {
    match value {
        SqlValue::String(value) => Ok(value),
        _ => Err(SqlError::undefined_function(
            "text search input must be text",
        )),
    }
}

fn sql_value_tsvector(value: &SqlValue) -> Result<PgTsVector> {
    match value {
        SqlValue::String(value) => PgTsVector::from_postgres_text(value),
        _ => Err(SqlError::undefined_function(
            "text search input must be tsvector",
        )),
    }
}

pub(crate) fn fts_index_terms(value: &SqlValue) -> Result<Vec<String>> {
    Ok(sql_value_tsvector(value)?
        .lexemes
        .into_iter()
        .map(|lexeme| lexeme.text)
        .collect())
}

/// Pack one canonical position for the index projection:
/// `position (14 bits) | weight rank (2 bits, high)`.
pub(crate) fn pack_ts_position(position: &PgTsPosition) -> u16 {
    (position.position & 0x3FFF) | ((position.weight.rank() as u16) << 14)
}

/// Unpack [`pack_ts_position`].
pub(crate) fn unpack_ts_position(packed: u16) -> PgTsPosition {
    PgTsPosition {
        position: packed & 0x3FFF,
        weight: PgTsWeight::from_rank((packed >> 14) as u8).unwrap_or(PgTsWeight::D),
    }
}

/// The full-text index projection, POSITIONS INCLUDED (v1):
/// `{"v":1, "len":L, "distinct":D, "lex": {"term": [packed_position…]}}`.
/// `len` is Σ max(positions,1) over ALL lexemes and `distinct` their count —
/// the two document scalars `ts_rank`/`ts_rank_cd` normalization needs
/// (flags 1/2 use the former, 8/16 the latter), so ranking can run from the
/// index without re-parsing the document. The legacy shape (a plain array of
/// term strings) is still read everywhere this object is read.
pub(crate) fn fts_index_projection(value: &SqlValue) -> Result<serde_json::Value> {
    let vector = sql_value_tsvector(value)?;
    let total_positions: u64 = vector
        .lexemes
        .iter()
        .map(|lexeme| lexeme.positions.len().max(1) as u64)
        .sum();
    let mut lex = serde_json::Map::new();
    for lexeme in &vector.lexemes {
        lex.insert(
            lexeme.text.clone(),
            serde_json::Value::Array(
                lexeme
                    .positions
                    .iter()
                    .map(|position| serde_json::Value::from(pack_ts_position(position)))
                    .collect(),
            ),
        );
    }
    Ok(serde_json::json!({
        "v": 1,
        "len": total_positions,
        "distinct": vector.lexemes.len(),
        "lex": serde_json::Value::Object(lex),
    }))
}

/// The projection's raw parts for a doc-terms blob: `(doc_length,
/// doc_distinct, sorted (term, packed positions) pairs)` — what the CREATE
/// INDEX pre-pass hands to `BicDb::encode_fts_doc_terms` instead of
/// embedding a JSON lexeme map into the row.
pub(crate) fn fts_doc_terms_parts(value: &SqlValue) -> Result<(u32, u32, Vec<(String, Vec<u16>)>)> {
    let vector = sql_value_tsvector(value)?;
    let total_positions: u64 = vector
        .lexemes
        .iter()
        .map(|lexeme| lexeme.positions.len().max(1) as u64)
        .sum();
    let mut terms: Vec<(String, Vec<u16>)> = vector
        .lexemes
        .iter()
        .map(|lexeme| {
            (
                lexeme.text.clone(),
                lexeme
                    .positions
                    .iter()
                    .map(pack_ts_position)
                    .collect::<Vec<u16>>(),
            )
        })
        .collect();
    terms.sort_by(|left, right| left.0.cmp(&right.0));
    Ok((total_positions as u32, vector.lexemes.len() as u32, terms))
}

fn sql_value_string_array(value: &SqlValue) -> Result<Vec<String>> {
    match value {
        SqlValue::Json(serde_json::Value::Array(values)) => values
            .iter()
            .map(|value| match value {
                serde_json::Value::String(value) => Ok(value.clone()),
                serde_json::Value::Null => Err(SqlError::data_exception(
                    "22004",
                    "text search array must not contain nulls",
                    None,
                )),
                _ => Err(SqlError::undefined_function(
                    "text search array must contain text values",
                )),
            })
            .collect(),
        _ => Err(SqlError::undefined_function(
            "text search input must be an array",
        )),
    }
}

fn sql_value_weight(value: &SqlValue) -> Result<PgTsWeight> {
    if let Some(byte) = pg_internal_char_byte(value) {
        return match byte.to_ascii_uppercase() {
            b'A' => Ok(PgTsWeight::A),
            b'B' => Ok(PgTsWeight::B),
            b'C' => Ok(PgTsWeight::C),
            b'D' => Ok(PgTsWeight::D),
            _ => Err(SqlError::data_exception(
                "22023",
                "unrecognized weight",
                Some("\"char\"".to_string()),
            )),
        };
    }
    let SqlValue::String(value) = value else {
        return Err(SqlError::undefined_function(
            "text search weight must be a character",
        ));
    };
    let mut characters = value.chars();
    let weight = match (characters.next(), characters.next()) {
        (Some('A' | 'a'), None) => PgTsWeight::A,
        (Some('B' | 'b'), None) => PgTsWeight::B,
        (Some('C' | 'c'), None) => PgTsWeight::C,
        (Some('D' | 'd'), None) => PgTsWeight::D,
        _ => {
            return Err(SqlError::data_exception(
                "22023",
                "unrecognized weight",
                Some("\"char\"".to_string()),
            ));
        }
    };
    Ok(weight)
}

fn setweight(vector: &PgTsVector, weight: PgTsWeight, lexemes: Option<&[String]>) -> PgTsVector {
    PgTsVector {
        lexemes: vector
            .lexemes
            .iter()
            .cloned()
            .map(|mut lexeme| {
                if lexemes.is_none_or(|values| values.iter().any(|value| value == &lexeme.text)) {
                    for position in &mut lexeme.positions {
                        position.weight = weight;
                    }
                }
                lexeme
            })
            .collect(),
    }
}

fn delete_lexemes(vector: &PgTsVector, lexemes: &[String]) -> PgTsVector {
    PgTsVector {
        lexemes: vector
            .lexemes
            .iter()
            .filter(|lexeme| !lexemes.iter().any(|value| value == &lexeme.text))
            .cloned()
            .collect(),
    }
}

fn filter_weights(vector: &PgTsVector, weights: &[PgTsWeight]) -> PgTsVector {
    PgTsVector {
        lexemes: vector
            .lexemes
            .iter()
            .filter_map(|lexeme| {
                let positions = lexeme
                    .positions
                    .iter()
                    .filter(|position| weights.contains(&position.weight))
                    .copied()
                    .collect::<Vec<_>>();
                (!positions.is_empty()).then(|| PgTsLexeme {
                    text: lexeme.text.clone(),
                    positions,
                })
            })
            .collect(),
    }
}

/// Public-to-the-crate face of [`query_operands`]: every operand of a query,
/// NEGATED ONES INCLUDED (they participate in matches() and in the rank).
pub(crate) fn collect_query_operands(node: &PgTsQueryNode, output: &mut Vec<PgTsQueryOperand>) {
    query_operands(node, output);
}

/// Weights-array parsing shared with [`rank_arguments`].
pub(crate) fn rank_weights_from_value(value: &SqlValue) -> Result<[f32; 4]> {
    let values = sql_value_number_array(value)?;
    if values.len() < 4 {
        return Err(SqlError::data_exception(
            "2202E",
            "array of weight is too short",
            None,
        ));
    }
    Ok([values[0], values[1], values[2], values[3]])
}

/// Whether the tree contains an OR — the probe-driven AND intersection only
/// applies to pure conjunctive shapes (And/Phrase/Not over operands).
pub(crate) fn tsquery_has_or(node: &PgTsQueryNode) -> bool {
    match node {
        PgTsQueryNode::Operand(_) => false,
        PgTsQueryNode::Not(child) => tsquery_has_or(child),
        PgTsQueryNode::Or(..) => true,
        PgTsQueryNode::And(left, right) | PgTsQueryNode::Phrase { left, right, .. } => {
            tsquery_has_or(left) || tsquery_has_or(right)
        }
    }
}

/// Operands NOT under a negation — the only sound intersection drivers (a
/// candidate set enumerated from a negated term would be the complement).
pub(crate) fn collect_positive_operands(node: &PgTsQueryNode, output: &mut Vec<PgTsQueryOperand>) {
    match node {
        PgTsQueryNode::Operand(operand) => output.push(operand.clone()),
        PgTsQueryNode::Not(_) => {}
        PgTsQueryNode::Or(left, right)
        | PgTsQueryNode::And(left, right)
        | PgTsQueryNode::Phrase { left, right, .. } => {
            collect_positive_operands(left, output);
            collect_positive_operands(right, output);
        }
    }
}

fn query_operands(node: &PgTsQueryNode, output: &mut Vec<PgTsQueryOperand>) {
    match node {
        PgTsQueryNode::Operand(operand) => output.push(operand.clone()),
        PgTsQueryNode::Not(child) => query_operands(child, output),
        PgTsQueryNode::And(left, right) | PgTsQueryNode::Or(left, right) => {
            query_operands(left, output);
            query_operands(right, output);
        }
        PgTsQueryNode::Phrase { left, right, .. } => {
            query_operands(left, output);
            query_operands(right, output);
        }
    }
}

fn matching_lexemes<'a>(vector: &'a PgTsVector, operand: &PgTsQueryOperand) -> Vec<&'a PgTsLexeme> {
    vector
        .lexemes
        .iter()
        .filter(|lexeme| {
            let text_matches = if operand.prefix {
                lexeme.text.starts_with(&operand.text)
            } else {
                lexeme.text == operand.text
            };
            text_matches
                && (operand.weights == 0
                    || lexeme.positions.is_empty()
                    || lexeme
                        .positions
                        .iter()
                        .any(|position| operand.weights & position.weight.query_bit() != 0))
        })
        .collect()
}

fn ts_rank(vector: &PgTsVector, query: &PgTsQuery, weights: [f32; 4], normalization: i64) -> f32 {
    ts_rank_with_scalars(vector, query, weights, normalization, None)
}

/// The document scalars ts_rank normalization consumes that a SPARSE vector
/// (query lexemes only, reconstructed from index postings) cannot supply:
/// flags 1/2 divide by Σ max(positions,1) over ALL lexemes, flags 8/16 by
/// the distinct-lexeme count. With these injected, ranking a sparse vector
/// is bit-identical to ranking the full document vector.
#[derive(Clone, Copy, Debug)]
pub(crate) struct FtsDocumentScalars {
    pub(crate) length: u32,
    pub(crate) distinct: u32,
}

pub(crate) fn ts_rank_with_scalars(
    vector: &PgTsVector,
    query: &PgTsQuery,
    weights: [f32; 4],
    normalization: i64,
    scalars: Option<FtsDocumentScalars>,
) -> f32 {
    let empty = match scalars {
        Some(scalars) => scalars.distinct == 0,
        None => vector.lexemes.is_empty(),
    };
    if empty || query.root.is_none() {
        return 0.0;
    }
    let mut operands = Vec::new();
    query_operands(
        query.root.as_ref().expect("query root checked"),
        &mut operands,
    );
    operands.sort_by(|left, right| left.text.cmp(&right.text));
    operands.dedup_by(|left, right| left.text == right.text);
    let is_and = matches!(
        query.root,
        Some(PgTsQueryNode::And(..) | PgTsQueryNode::Phrase { .. })
    );
    let mut rank = if is_and && operands.len() >= 2 {
        rank_and(vector, &operands, weights)
    } else {
        rank_or(vector, &operands, weights)
    };
    if rank < 0.0 {
        rank = 1e-20;
    }
    let scalars = scalars.unwrap_or_else(|| FtsDocumentScalars {
        length: vector_length(vector) as u32,
        distinct: vector.lexemes.len() as u32,
    });
    normalize_rank_scalars(rank, scalars, normalization)
}

fn rank_or(vector: &PgTsVector, operands: &[PgTsQueryOperand], weights: [f32; 4]) -> f32 {
    let mut result = 0.0f32;
    for operand in operands {
        for lexeme in matching_lexemes(vector, operand) {
            let positions = if lexeme.positions.is_empty() {
                vec![PgTsPosition {
                    position: 0,
                    weight: PgTsWeight::D,
                }]
            } else {
                lexeme.positions.clone()
            };
            let mut sum = 0.0f32;
            let mut maximum = (-1.0f32, 0usize);
            for (index, position) in positions.iter().enumerate() {
                let weight = weights[position.weight.rank() as usize];
                sum += weight / ((index + 1) * (index + 1)) as f32;
                if weight > maximum.0 {
                    maximum = (weight, index);
                }
            }
            result += (maximum.0 + sum - maximum.0 / ((maximum.1 + 1) * (maximum.1 + 1)) as f32)
                / 1.644_934_1;
        }
    }
    if operands.is_empty() {
        0.0
    } else {
        result / operands.len() as f32
    }
}

fn rank_and(vector: &PgTsVector, operands: &[PgTsQueryOperand], weights: [f32; 4]) -> f32 {
    let mut result = -1.0f32;
    for (index, operand) in operands.iter().enumerate() {
        let current = matching_lexemes(vector, operand);
        for previous in &operands[..index] {
            for left in matching_lexemes(vector, previous) {
                for right in &current {
                    let left_positions = rank_positions(left);
                    let right_positions = rank_positions(right);
                    for left_position in &left_positions {
                        for right_position in &right_positions {
                            let mut distance =
                                left_position.position.abs_diff(right_position.position);
                            if distance == 0 {
                                distance = MAX_POSITION;
                            }
                            let distance_weight = if distance > 100 {
                                1e-30
                            } else {
                                1.0 / (1.005 + 0.05 * ((distance as f32) / 1.5 - 2.0).exp())
                            };
                            let current = (weights[left_position.weight.rank() as usize]
                                * weights[right_position.weight.rank() as usize]
                                * distance_weight)
                                .sqrt();
                            result = if result < 0.0 {
                                current
                            } else {
                                1.0 - (1.0 - result) * (1.0 - current)
                            };
                        }
                    }
                }
            }
        }
    }
    result
}

fn rank_positions(lexeme: &PgTsLexeme) -> Vec<PgTsPosition> {
    if lexeme.positions.is_empty() {
        vec![PgTsPosition {
            position: MAX_POSITION - 1,
            weight: PgTsWeight::D,
        }]
    } else {
        lexeme.positions.clone()
    }
}

fn vector_length(vector: &PgTsVector) -> usize {
    vector
        .lexemes
        .iter()
        .map(|lexeme| lexeme.positions.len().max(1))
        .sum()
}

fn normalize_rank_scalars(mut rank: f32, scalars: FtsDocumentScalars, normalization: i64) -> f32 {
    let length = scalars.length as usize;
    if normalization & 1 != 0 && length > 0 {
        rank /= (length as f32 + 1.0).log2();
    }
    if normalization & 2 != 0 && length > 0 {
        rank /= length as f32;
    }
    if normalization & 8 != 0 && scalars.distinct > 0 {
        rank /= scalars.distinct as f32;
    }
    if normalization & 16 != 0 && scalars.distinct > 0 {
        rank /= (scalars.distinct as f32 + 1.0).log2();
    }
    if normalization & 32 != 0 {
        rank /= rank + 1.0;
    }
    rank
}

#[derive(Clone)]
struct RankedOccurrence {
    text: String,
    position: PgTsPosition,
}

fn ts_rank_cd(
    vector: &PgTsVector,
    query: &PgTsQuery,
    weights: [f32; 4],
    normalization: i64,
) -> f32 {
    ts_rank_cd_with_scalars(vector, query, weights, normalization, None)
}

pub(crate) fn ts_rank_cd_with_scalars(
    vector: &PgTsVector,
    query: &PgTsQuery,
    weights: [f32; 4],
    normalization: i64,
    scalars: Option<FtsDocumentScalars>,
) -> f32 {
    let scalars = scalars.unwrap_or_else(|| FtsDocumentScalars {
        length: vector_length(vector) as u32,
        distinct: vector.lexemes.len() as u32,
    });
    let mut operands = Vec::new();
    if let Some(root) = &query.root {
        query_operands(root, &mut operands);
    }
    let mut occurrences = Vec::<RankedOccurrence>::new();
    for lexeme in &vector.lexemes {
        if operands.iter().any(|operand| {
            (operand.prefix && lexeme.text.starts_with(&operand.text))
                || (!operand.prefix && lexeme.text == operand.text)
        }) {
            occurrences.extend(lexeme.positions.iter().map(|position| RankedOccurrence {
                text: lexeme.text.clone(),
                position: *position,
            }));
        }
    }
    occurrences.sort_by_key(|occurrence| {
        (
            occurrence.position.position,
            occurrence.position.weight.rank(),
        )
    });
    if occurrences.is_empty() {
        return 0.0;
    }

    let mut total = 0.0f64;
    let mut centers = Vec::<f64>::new();
    let mut cursor = 0usize;
    while cursor < occurrences.len() {
        let Some(end) = (cursor..occurrences.len())
            .find(|end| query.matches(&occurrence_vector(&occurrences[cursor..=*end])))
        else {
            break;
        };
        let mut begin = cursor;
        for candidate in (cursor..=end).rev() {
            if query.matches(&occurrence_vector(&occurrences[candidate..=end])) {
                begin = candidate;
                break;
            }
        }
        let cover = &occurrences[begin..=end];
        let inverse_sum = cover
            .iter()
            .map(|occurrence| 1.0 / f64::from(weights[occurrence.position.weight.rank() as usize]))
            .sum::<f64>();
        let cover_weight = cover.len() as f64 / inverse_sum;
        let positional_width = i64::from(cover.last().unwrap().position.position)
            - i64::from(cover.first().unwrap().position.position);
        let mut noise = positional_width - (cover.len() as i64 - 1);
        if noise < 0 {
            noise = (cover.len() as i64 - 1) / 2;
        }
        total += cover_weight / (1 + noise) as f64;
        centers.push(
            f64::from(
                cover.first().unwrap().position.position + cover.last().unwrap().position.position,
            ) / 2.0,
        );
        cursor = begin + 1;
    }
    if normalization & 1 != 0 && scalars.distinct > 0 {
        total /= (scalars.length as f64 + 1.0).ln();
    }
    if normalization & 2 != 0 {
        let length = scalars.length as usize;
        if length > 0 {
            total /= length as f64;
        }
    }
    if normalization & 4 != 0 && centers.len() > 1 {
        let inverse_distances = centers
            .windows(2)
            .filter_map(|window| (window[1] > window[0]).then_some(1.0 / (window[1] - window[0])))
            .sum::<f64>();
        if inverse_distances > 0.0 {
            total /= centers.len() as f64 / inverse_distances;
        }
    }
    if normalization & 8 != 0 && scalars.distinct > 0 {
        total /= scalars.distinct as f64;
    }
    if normalization & 16 != 0 && scalars.distinct > 0 {
        total /= (scalars.distinct as f64 + 1.0).log2();
    }
    if normalization & 32 != 0 {
        total /= total + 1.0;
    }
    total as f32
}

fn occurrence_vector(occurrences: &[RankedOccurrence]) -> PgTsVector {
    let mut lexemes = BTreeMap::<String, Vec<PgTsPosition>>::new();
    for occurrence in occurrences {
        lexemes
            .entry(occurrence.text.clone())
            .or_default()
            .push(occurrence.position);
    }
    PgTsVector {
        lexemes: lexemes
            .into_iter()
            .map(|(text, positions)| PgTsLexeme { text, positions })
            .collect(),
    }
}

fn rank_arguments(args: &[SqlValue]) -> Result<([f32; 4], PgTsVector, PgTsQuery, i64)> {
    let (weights, vector_index) = if matches!(args.first(), Some(SqlValue::Json(_))) {
        let values = sql_value_number_array(&args[0])?;
        if values.len() < 4 {
            return Err(SqlError::data_exception(
                "2202E",
                "array of weight is too short",
                None,
            ));
        }
        ([values[0], values[1], values[2], values[3]], 1)
    } else {
        ([0.1, 0.2, 0.4, 1.0], 0)
    };
    if !(2..=3).contains(&(args.len() - vector_index)) {
        return Err(SqlError::InvalidSql(
            "invalid text rank arguments".to_string(),
        ));
    }
    let vector = sql_value_tsvector(&args[vector_index])?;
    let query = sql_value_tsquery(&args[vector_index + 1])?;
    let normalization = match args.get(vector_index + 2) {
        None => 0,
        Some(SqlValue::Int(value)) => *value,
        Some(_) => {
            return Err(SqlError::undefined_function(
                "text rank normalization must be an integer",
            ));
        }
    };
    Ok((weights, vector, query, normalization))
}

fn sql_value_number_array(value: &SqlValue) -> Result<Vec<f32>> {
    match value {
        SqlValue::Json(serde_json::Value::Array(values)) => values
            .iter()
            .map(|value| {
                value.as_f64().map(|value| value as f32).ok_or_else(|| {
                    SqlError::data_exception(
                        "22004",
                        "array of weight must not contain nulls",
                        None,
                    )
                })
            })
            .collect(),
        _ => Err(SqlError::undefined_function(
            "rank weights must be an array",
        )),
    }
}

pub(crate) fn eval_fts_function_value(
    name: &str,
    args: &[SqlValue],
    arg_types: Option<&[Option<String>]>,
) -> Result<Option<SqlValue>> {
    let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    let first_type = arg_types
        .and_then(|types| types.first())
        .and_then(Option::as_deref);
    let recognized = matches!(
        name,
        "to_tsvector"
            | "to_tsquery"
            | "plainto_tsquery"
            | "phraseto_tsquery"
            | "websearch_to_tsquery"
            | "get_current_ts_config"
            | "numnode"
            | "querytree"
            | "tsquery_phrase"
            | "setweight"
            | "ts_delete"
            | "ts_filter"
            | "array_to_tsvector"
            | "tsvector_to_array"
            | "ts_rank"
            | "ts_rank_cd"
            | "ts_headline"
            | "ts_rewrite"
            | "strip"
    ) || (name == "length" && first_type == Some("tsvector"));
    if !recognized {
        return Ok(None);
    }
    if name == "get_current_ts_config" {
        if !args.is_empty() {
            return Err(SqlError::InvalidSql(format!(
                "{name} expects 0 arguments, got {}",
                args.len()
            )));
        }
        return Ok(Some(SqlValue::String("english".to_string())));
    }
    if args
        .iter()
        .any(|argument| matches!(argument, SqlValue::Null))
    {
        return Ok(Some(SqlValue::Null));
    }
    let value = match name {
        "to_tsvector" => {
            let (config, value) = match args {
                [value] => (english_config(), value),
                [config, value] => (TextSearchConfig::parse(config)?, value),
                _ => {
                    return Err(SqlError::InvalidSql(format!(
                        "{name} expects 1 or 2 arguments, got {}",
                        args.len()
                    )));
                }
            };
            let vector = match value {
                SqlValue::Json(value) => json_to_tsvector(config, value),
                SqlValue::JsonText(value) => json_to_tsvector(config, value.parsed()),
                SqlValue::String(value) => to_tsvector(config, value),
                _ => {
                    return Err(SqlError::undefined_function(
                        "text search input must be text, json, or jsonb",
                    ));
                }
            };
            SqlValue::String(vector.to_postgres_text())
        }
        "to_tsquery" => {
            let (config, text) = fts_config_and_text(args)?;
            SqlValue::TsQuery(to_tsquery(config, text)?)
        }
        "plainto_tsquery" | "phraseto_tsquery" => {
            let (config, text) = fts_config_and_text(args)?;
            SqlValue::TsQuery(plain_tsquery(config, text, name == "phraseto_tsquery"))
        }
        "websearch_to_tsquery" => {
            let (config, text) = fts_config_and_text(args)?;
            SqlValue::TsQuery(websearch_tsquery(config, text))
        }
        "numnode" | "querytree" => {
            require_argument_count(name, args, 1)?;
            let query = sql_value_tsquery(&args[0])?;
            if name == "numnode" {
                SqlValue::Int(query.numnode() as i64)
            } else {
                SqlValue::String(querytree(&query).to_postgres_text())
            }
        }
        "tsquery_phrase" => {
            if !(2..=3).contains(&args.len()) {
                return Err(SqlError::InvalidSql(format!(
                    "{name} expects 2 or 3 arguments, got {}",
                    args.len()
                )));
            }
            let left = sql_value_tsquery(&args[0])?;
            let right = sql_value_tsquery(&args[1])?;
            let distance = args
                .get(2)
                .map(sql_value_u16_distance)
                .transpose()?
                .unwrap_or(1);
            SqlValue::TsQuery(left.phrase(right, distance))
        }
        "setweight" => {
            if !(2..=3).contains(&args.len()) {
                return Err(SqlError::InvalidSql(format!(
                    "{name} expects 2 or 3 arguments, got {}",
                    args.len()
                )));
            }
            let vector = sql_value_tsvector(&args[0])?;
            let weight = sql_value_weight(&args[1])?;
            let lexemes = args.get(2).map(sql_value_string_array).transpose()?;
            SqlValue::String(setweight(&vector, weight, lexemes.as_deref()).to_postgres_text())
        }
        "ts_delete" => {
            require_argument_count(name, args, 2)?;
            let vector = sql_value_tsvector(&args[0])?;
            let lexemes = match &args[1] {
                SqlValue::String(value) => vec![value.clone()],
                value => sql_value_string_array(value)?,
            };
            SqlValue::String(delete_lexemes(&vector, &lexemes).to_postgres_text())
        }
        "ts_filter" => {
            require_argument_count(name, args, 2)?;
            let vector = sql_value_tsvector(&args[0])?;
            let weights = sql_value_string_array(&args[1])?
                .iter()
                .map(|value| sql_value_weight(&SqlValue::String(value.clone())))
                .collect::<Result<Vec<_>>>()?;
            SqlValue::String(filter_weights(&vector, &weights).to_postgres_text())
        }
        "array_to_tsvector" => {
            require_argument_count(name, args, 1)?;
            let mut lexemes = sql_value_string_array(&args[0])?;
            lexemes.retain(|term| bicdb_core::full_text_term_is_indexable(term));
            lexemes.sort();
            lexemes.dedup();
            let vector = PgTsVector {
                lexemes: lexemes
                    .into_iter()
                    .map(|text| PgTsLexeme {
                        text,
                        positions: Vec::new(),
                    })
                    .collect(),
            };
            SqlValue::String(vector.to_postgres_text())
        }
        "tsvector_to_array" => {
            require_argument_count(name, args, 1)?;
            SqlValue::Json(serde_json::Value::Array(
                sql_value_tsvector(&args[0])?
                    .lexemes
                    .into_iter()
                    .map(|lexeme| serde_json::Value::String(lexeme.text))
                    .collect(),
            ))
        }
        "ts_rank" | "ts_rank_cd" => {
            let (weights, vector, query, normalization) = rank_arguments(args)?;
            let rank = if name == "ts_rank" {
                ts_rank(&vector, &query, weights, normalization)
            } else {
                ts_rank_cd(&vector, &query, weights, normalization)
            };
            SqlValue::Float(
                rank.to_string()
                    .parse()
                    .expect("finite float4 rank renders as float8"),
            )
        }
        "ts_headline" => {
            let (config, value_index) = if matches!(args.first(), Some(SqlValue::String(value)) if matches!(value.to_ascii_lowercase().as_str(), "english" | "simple" | "pg_catalog.english" | "pg_catalog.simple"))
                && args.len() >= 3
            {
                (TextSearchConfig::parse(&args[0])?, 1)
            } else {
                (english_config(), 0)
            };
            if !(2..=3).contains(&(args.len() - value_index)) {
                return Err(SqlError::InvalidSql(
                    "invalid ts_headline arguments".to_string(),
                ));
            }
            let query = sql_value_tsquery(&args[value_index + 1])?;
            let options =
                parse_headline_options(args.get(value_index + 2).map(sql_value_str).transpose()?)?;
            match &args[value_index] {
                SqlValue::String(text) => {
                    SqlValue::String(text_headline(config, text, &query, &options))
                }
                SqlValue::Json(value) => {
                    SqlValue::Json(json_headline(config, value, &query, &options))
                }
                SqlValue::JsonText(value) => SqlValue::JsonText(crate::PgJsonText::from_value(
                    json_headline(config, value.parsed(), &query, &options),
                )),
                _ => {
                    return Err(SqlError::undefined_function(
                        "headline input must be text, json, or jsonb",
                    ));
                }
            }
        }
        "ts_rewrite" => {
            require_argument_count(name, args, 3)?;
            SqlValue::TsQuery(rewrite_query(
                sql_value_tsquery(&args[0])?,
                &sql_value_tsquery(&args[1])?,
                &sql_value_tsquery(&args[2])?,
            ))
        }
        "length" | "strip" => {
            require_argument_count(name, args, 1)?;
            let vector = PgTsVector::from_postgres_text(&args[0].to_cell())?;
            if name == "length" {
                SqlValue::Int(vector.lexemes.len() as i64)
            } else {
                SqlValue::String(vector.strip().to_postgres_text())
            }
        }
        _ => unreachable!(),
    };
    Ok(Some(value))
}

pub(crate) fn eval_fts_db_function_value(
    db: &BicDb,
    name: &str,
    args: &[SqlValue],
    session_gucs: Option<&std::collections::HashMap<String, String>>,
) -> Result<Option<SqlValue>> {
    let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    if name == "bicdb_fts_build_status" {
        if args.len() != 1 {
            return Err(SqlError::InvalidSql(format!(
                "bicdb_fts_build_status expects 1 argument (index name), got {}",
                args.len()
            )));
        }
        if matches!(args[0], SqlValue::Null) {
            return Ok(Some(SqlValue::Null));
        }
        let index = sql_value_str(&args[0])?;
        let status = db.full_text_build_lifecycle(&index)?;
        return Ok(Some(SqlValue::Json(serde_json::to_value(status).map_err(
            |error| SqlError::InvalidSql(format!("FTS lifecycle serialization failed: {error}")),
        )?)));
    }
    if name == "bicdb_fts_fold" {
        if args.len() != 1 {
            return Err(SqlError::InvalidSql(format!(
                "bicdb_fts_fold expects 1 argument (index name), got {}",
                args.len()
            )));
        }
        if matches!(args[0], SqlValue::Null) {
            return Ok(Some(SqlValue::Null));
        }
        let index = sql_value_str(&args[0])?;
        let started = std::time::Instant::now();
        let (terms, blocks) = db.compact_full_text_index(&index)?;
        return Ok(Some(SqlValue::String(format!(
            "folded {terms} terms into {blocks} blocks in {:.1}s",
            started.elapsed().as_secs_f64()
        ))));
    }
    if name == "bicdb_fts_route" {
        if args.len() != 2 {
            return Err(SqlError::InvalidSql(format!(
                "bicdb_fts_route expects 2 arguments (index name, terms), got {}",
                args.len()
            )));
        }
        if args
            .iter()
            .any(|argument| matches!(argument, SqlValue::Null))
        {
            return Ok(Some(SqlValue::Null));
        }
        let index = sql_value_str(&args[0])?;
        let terms_text = sql_value_str(&args[1])?;
        let terms: Vec<&str> = terms_text
            .split(|c: char| c == ',' || c.is_whitespace())
            .filter(|term| !term.is_empty())
            .collect();
        return Ok(Some(SqlValue::String(
            db.full_text_route_report(&index, &terms)?,
        )));
    }
    if name != "ts_rewrite" || args.len() != 2 {
        return Ok(None);
    }
    if args
        .iter()
        .any(|argument| matches!(argument, SqlValue::Null))
    {
        return Ok(Some(SqlValue::Null));
    }
    let mut query = sql_value_tsquery(&args[0])?;
    let rewrite_sql = sql_value_str(&args[1])?;
    let normalized = rewrite_sql.trim_start().to_ascii_lowercase();
    if !normalized.starts_with("select ") && !normalized.starts_with("with ") {
        return Err(SqlError::InvalidSql(
            "ts_rewrite query must be a SELECT statement".to_string(),
        ));
    }
    // The caller supplies this SQL, so it must run with the CALLER's
    // identity. A bare `SqlEngine::new` carries no security context and no
    // session GUCs, so its identity resolved to the bootstrap role — which
    // `role_has_table_privilege` short-circuits to "holds SELECT on every
    // relation", and whose ownership defaults also disable RLS on any
    // table with no recorded owner. `ts_rewrite` is not superuser-gated,
    // so any role could read any table by embedding a SELECT here.
    let mut engine = SqlEngine::new(db);
    if let Some(session_gucs) = session_gucs {
        engine = engine.with_session_gucs(std::sync::Arc::new(session_gucs.clone()));
    }
    let rewrites = engine.execute(rewrite_sql)?;
    for row in rewrites.rows {
        if row.len() != 2 {
            return Err(SqlError::data_exception(
                "22023",
                "ts_rewrite query must return two tsquery columns",
                None,
            ));
        }
        query = rewrite_query(
            query,
            &sql_value_tsquery(&row[0])?,
            &sql_value_tsquery(&row[1])?,
        );
    }
    Ok(Some(SqlValue::TsQuery(query)))
}

pub(crate) fn fts_function_pg_type(
    name: &str,
    arg_types: &[Option<String>],
) -> Option<&'static str> {
    match name.strip_prefix("pg_catalog.").unwrap_or(name) {
        "to_tsvector" | "strip" => Some("tsvector"),
        "to_tsquery"
        | "plainto_tsquery"
        | "phraseto_tsquery"
        | "websearch_to_tsquery"
        | "tsquery_phrase" => Some("tsquery"),
        "get_current_ts_config" => Some("regconfig"),
        "numnode" => Some("int4"),
        "querytree" => Some("text"),
        "setweight" | "ts_delete" | "ts_filter" | "array_to_tsvector" => Some("tsvector"),
        "tsvector_to_array" => Some("text[]"),
        "ts_rank" | "ts_rank_cd" => Some("float4"),
        "ts_headline" => arg_types
            .iter()
            .flatten()
            .find_map(|pg_type| match pg_type.as_str() {
                "json" => Some("json"),
                "jsonb" => Some("jsonb"),
                _ => None,
            })
            .or(Some("text")),
        "ts_rewrite" => Some("tsquery"),
        "bicdb_fts_build_status" => Some("jsonb"),
        "bicdb_fts_route" => Some("text"),
        "bicdb_fts_fold" => Some("text"),
        _ => None,
    }
}

fn require_argument_count(name: &str, args: &[SqlValue], expected: usize) -> Result<()> {
    if args.len() == expected {
        Ok(())
    } else {
        Err(SqlError::InvalidSql(format!(
            "{name} expects {expected} arguments, got {}",
            args.len()
        )))
    }
}

/// Extract the plain conjunction of unweighted, non-prefix lexemes from a
/// tsquery, or `None` when any other operator participates. This is exactly
/// the shape a dense posting-list intersection answers without positions:
/// presence of every lexeme decides `@@` with no per-document recheck.
pub(crate) fn conjunctive_plain_lexemes(query: &PgTsQuery) -> Option<Vec<String>> {
    fn walk(node: &PgTsQueryNode, lexemes: &mut Vec<String>) -> bool {
        match node {
            PgTsQueryNode::Operand(operand) => {
                if operand.prefix || operand.weights != 0 {
                    return false;
                }
                lexemes.push(operand.text.clone());
                true
            }
            PgTsQueryNode::And(left, right) => walk(left, lexemes) && walk(right, lexemes),
            PgTsQueryNode::Not(_) | PgTsQueryNode::Or(..) | PgTsQueryNode::Phrase { .. } => false,
        }
    }
    let root = query.root.as_ref()?;
    let mut lexemes = Vec::new();
    walk(root, &mut lexemes).then_some(lexemes)
}

/// Extract the deduplicated lexemes of a tsquery built only from unweighted,
/// non-prefix operands under AND and PHRASE operators, plus whether a phrase
/// node participates. Every such lexeme must be present in a matching
/// document, so the conjunction of all of them is an exact candidate set;
/// phrase distance is then decided by `matches()` over the candidate's
/// positions alone. OR and NOT (and prefix/weight operands, whose candidate
/// sets postings cannot enumerate this way) return `None`.
pub(crate) fn phrase_conjunctive_lexemes(query: &PgTsQuery) -> Option<(Vec<String>, bool)> {
    fn walk(node: &PgTsQueryNode, lexemes: &mut Vec<String>, has_phrase: &mut bool) -> bool {
        match node {
            PgTsQueryNode::Operand(operand) => {
                if operand.prefix || operand.weights != 0 {
                    return false;
                }
                lexemes.push(operand.text.clone());
                true
            }
            PgTsQueryNode::And(left, right) => {
                walk(left, lexemes, has_phrase) && walk(right, lexemes, has_phrase)
            }
            PgTsQueryNode::Phrase { left, right, .. } => {
                *has_phrase = true;
                walk(left, lexemes, has_phrase) && walk(right, lexemes, has_phrase)
            }
            PgTsQueryNode::Not(_) | PgTsQueryNode::Or(..) => false,
        }
    }
    let root = query.root.as_ref()?;
    let mut lexemes = Vec::new();
    let mut has_phrase = false;
    if !walk(root, &mut lexemes, &mut has_phrase) {
        return None;
    }
    lexemes.sort_unstable();
    lexemes.dedup();
    Some((lexemes, has_phrase))
}

/// Assemble the sparse tsvector of one block-scan candidate: each queried
/// term with its packed positions. `matches()` over this vector is exact for
/// queries whose lexemes are all in `terms` — the vector holds every
/// occurrence of every queried lexeme in the document.
pub(crate) fn sparse_candidate_tsvector(
    terms: &[&str],
    positions: &[Option<&[u16]>],
) -> PgTsVector {
    PgTsVector {
        lexemes: terms
            .iter()
            .zip(positions)
            .filter_map(|(term, packed)| {
                packed.map(|packed| PgTsLexeme {
                    text: (*term).to_string(),
                    positions: packed.iter().map(|p| unpack_ts_position(*p)).collect(),
                })
            })
            .collect(),
    }
}

pub(crate) fn sql_value_tsquery(value: &SqlValue) -> Result<PgTsQuery> {
    match value {
        SqlValue::TsQuery(query) => Ok(query.clone()),
        SqlValue::String(value) => PgTsQuery::from_postgres_text(value),
        _ => Err(SqlError::undefined_function(
            "text search input must be tsquery",
        )),
    }
}

fn sql_value_u16_distance(value: &SqlValue) -> Result<u16> {
    let SqlValue::Int(value) = value else {
        return Err(SqlError::undefined_function(
            "tsquery phrase distance must be an integer",
        ));
    };
    u16::try_from(*value)
        .ok()
        .filter(|value| *value <= 16_384)
        .ok_or_else(tsquery_distance_error)
}

fn querytree(query: &PgTsQuery) -> PgTsQuery {
    fn searchable(node: &PgTsQueryNode) -> Option<PgTsQueryNode> {
        match node {
            PgTsQueryNode::Operand(_) => Some(node.clone()),
            PgTsQueryNode::Not(_) => None,
            PgTsQueryNode::And(left, right) => match (searchable(left), searchable(right)) {
                (Some(left), Some(right)) => {
                    Some(PgTsQueryNode::And(Box::new(left), Box::new(right)))
                }
                (Some(node), None) | (None, Some(node)) => Some(node),
                (None, None) => None,
            },
            PgTsQueryNode::Or(left, right) => {
                let (Some(left), Some(right)) = (searchable(left), searchable(right)) else {
                    return None;
                };
                Some(PgTsQueryNode::Or(Box::new(left), Box::new(right)))
            }
            PgTsQueryNode::Phrase {
                left,
                right,
                distance,
            } => {
                let (Some(left), Some(right)) = (searchable(left), searchable(right)) else {
                    return None;
                };
                Some(PgTsQueryNode::Phrase {
                    left: Box::new(left),
                    right: Box::new(right),
                    distance: *distance,
                })
            }
        }
    }

    PgTsQuery {
        root: query.root.as_ref().and_then(searchable),
    }
}

fn websearch_tsquery(config: TextSearchConfig, input: &str) -> PgTsQuery {
    WebSearchParser::new(input, config).parse()
}

pub(crate) fn english_websearch_positive_and_terms(input: &str) -> Option<Vec<String>> {
    fn collect(node: &PgTsQueryNode, terms: &mut Vec<String>) -> bool {
        match node {
            PgTsQueryNode::Operand(operand) if operand.weights == 0 && !operand.prefix => {
                terms.push(operand.text.clone());
                true
            }
            PgTsQueryNode::And(left, right) => collect(left, terms) && collect(right, terms),
            _ => false,
        }
    }

    let query = websearch_tsquery(english_config(), input);
    let root = query.root.as_ref()?;
    let mut terms = Vec::new();
    if !collect(root, &mut terms) {
        return None;
    }
    terms.sort_unstable();
    terms.dedup();
    (terms.len() >= 2).then_some(terms)
}

struct WebSearchParser<'a> {
    input: &'a str,
    offset: usize,
    config: TextSearchConfig,
}

impl<'a> WebSearchParser<'a> {
    fn new(input: &'a str, config: TextSearchConfig) -> Self {
        Self {
            input,
            offset: 0,
            config,
        }
    }

    fn parse(mut self) -> PgTsQuery {
        let mut groups = Vec::<PgTsQuery>::new();
        let mut current = PgTsQuery::default();
        while let Some((query, is_or)) = self.next_term() {
            if is_or {
                groups.push(current);
                current = query;
            } else {
                current = current.and(query);
            }
        }
        groups.push(current);
        groups.into_iter().fold(PgTsQuery::default(), PgTsQuery::or)
    }

    fn next_term(&mut self) -> Option<(PgTsQuery, bool)> {
        self.skip_spaces();
        if self.offset >= self.input.len() {
            return None;
        }
        if self.input[self.offset..].starts_with("OR")
            && self
                .input
                .as_bytes()
                .get(self.offset + 2)
                .is_none_or(|byte| byte.is_ascii_whitespace())
        {
            self.offset += 2;
            self.skip_spaces();
            let (query, _) = self.next_term()?;
            return Some((query, true));
        }
        let negate = self.input.as_bytes().get(self.offset) == Some(&b'-');
        if negate {
            self.offset += 1;
            self.skip_spaces();
        }
        let phrase = self.input.as_bytes().get(self.offset) == Some(&b'"');
        let text = if phrase {
            self.offset += 1;
            let start = self.offset;
            while self.offset < self.input.len() && self.input.as_bytes()[self.offset] != b'"' {
                self.offset += 1;
            }
            let value = &self.input[start..self.offset];
            self.offset = (self.offset + 1).min(self.input.len());
            value
        } else {
            let start = self.offset;
            while self.offset < self.input.len()
                && !self.input.as_bytes()[self.offset].is_ascii_whitespace()
            {
                self.offset += 1;
            }
            &self.input[start..self.offset]
        };
        let query = plain_tsquery(self.config, text, phrase);
        Some((if negate { query.not() } else { query }, false))
    }

    fn skip_spaces(&mut self) {
        while self
            .input
            .as_bytes()
            .get(self.offset)
            .is_some_and(|byte| byte.is_ascii_whitespace())
        {
            self.offset += 1;
        }
    }
}

fn canonical_positions(mut positions: Vec<PgTsPosition>) -> Vec<PgTsPosition> {
    positions.sort_by_key(|position| position.position);
    let mut canonical = Vec::<PgTsPosition>::with_capacity(positions.len().min(MAX_POSITIONS));
    for position in positions {
        if let Some(previous) = canonical.last_mut() {
            if previous.position == position.position {
                if position.weight.rank() > previous.weight.rank() {
                    previous.weight = position.weight;
                }
                continue;
            }
        }
        if canonical.len() == MAX_POSITIONS || position.position == MAX_POSITION {
            if canonical.len() < MAX_POSITIONS {
                canonical.push(position);
            }
            break;
        }
        canonical.push(position);
    }
    canonical
}

struct TsVectorParser<'a> {
    input: &'a str,
    offset: usize,
}

impl<'a> TsVectorParser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, offset: 0 }
    }

    fn next_lexeme(&mut self) -> Result<Option<(String, Vec<PgTsPosition>)>> {
        self.skip_whitespace();
        if self.offset == self.input.len() {
            return Ok(None);
        }
        let lexeme = if self.current_byte() == Some(b'\'') {
            self.offset += 1;
            self.parse_quoted_lexeme()?
        } else {
            self.parse_unquoted_lexeme()?
        };
        if lexeme.is_empty() {
            return Err(tsvector_syntax_error(self.input));
        }
        let positions = if self.current_byte() == Some(b':') {
            self.offset += 1;
            self.parse_positions()?
        } else {
            Vec::new()
        };
        if self.offset < self.input.len()
            && !self.input[self.offset..]
                .chars()
                .next()
                .is_some_and(char::is_whitespace)
        {
            return Err(tsvector_syntax_error(self.input));
        }
        Ok(Some((lexeme, positions)))
    }

    fn parse_quoted_lexeme(&mut self) -> Result<String> {
        let mut output = String::new();
        while self.offset < self.input.len() {
            let character = self.input[self.offset..].chars().next().unwrap();
            self.offset += character.len_utf8();
            match character {
                '\'' if self.current_byte() == Some(b'\'') => {
                    self.offset += 1;
                    output.push('\'');
                }
                '\'' => return Ok(output),
                '\\' => output.push(self.take_escaped_character()?),
                other => output.push(other),
            }
        }
        Err(tsvector_syntax_error(self.input))
    }

    fn parse_unquoted_lexeme(&mut self) -> Result<String> {
        let mut output = String::new();
        while self.offset < self.input.len() {
            let character = self.input[self.offset..].chars().next().unwrap();
            if character.is_whitespace() || character == ':' {
                break;
            }
            self.offset += character.len_utf8();
            if character == '\\' {
                output.push(self.take_escaped_character()?);
            } else {
                output.push(character);
            }
        }
        Ok(output)
    }

    fn parse_positions(&mut self) -> Result<Vec<PgTsPosition>> {
        let mut positions = Vec::new();
        loop {
            let start = self.offset;
            while self
                .current_byte()
                .is_some_and(|byte| byte.is_ascii_digit())
            {
                self.offset += 1;
            }
            if start == self.offset {
                return Err(tsvector_syntax_error(self.input));
            }
            let raw = self.input[start..self.offset]
                .parse::<u64>()
                .map_err(|_| tsvector_position_error(self.input))?;
            if raw == 0 {
                return Err(tsvector_position_error(self.input));
            }
            let weight_byte = self
                .current_byte()
                .filter(|byte| byte.is_ascii_alphabetic());
            let weight =
                PgTsWeight::parse(weight_byte).ok_or_else(|| tsvector_syntax_error(self.input))?;
            if weight_byte.is_some() {
                self.offset += 1;
            }
            positions.push(PgTsPosition {
                position: raw.min(u64::from(MAX_POSITION)) as u16,
                weight,
            });
            if self.current_byte() != Some(b',') {
                break;
            }
            self.offset += 1;
        }
        Ok(positions)
    }

    fn take_escaped_character(&mut self) -> Result<char> {
        let character = self.input[self.offset..]
            .chars()
            .next()
            .ok_or_else(|| tsvector_syntax_error(self.input))?;
        self.offset += character.len_utf8();
        Ok(character)
    }

    fn skip_whitespace(&mut self) {
        while self.offset < self.input.len() {
            let character = self.input[self.offset..].chars().next().unwrap();
            if !character.is_whitespace() {
                break;
            }
            self.offset += character.len_utf8();
        }
    }

    fn current_byte(&self) -> Option<u8> {
        self.input.as_bytes().get(self.offset).copied()
    }
}

fn tsvector_syntax_error(input: &str) -> SqlError {
    SqlError::invalid_text_representation(
        "tsvector",
        format!("syntax error in tsvector: \"{input}\""),
    )
}

fn tsvector_position_error(input: &str) -> SqlError {
    SqlError::invalid_text_representation(
        "tsvector",
        format!("wrong position info in tsvector: \"{input}\""),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_bm25_terms_match_english_websearch_conjunctions() {
        assert_eq!(
            english_websearch_positive_and_terms("running databases"),
            Some(vec!["databas".to_string(), "run".to_string()])
        );
        assert_eq!(english_websearch_positive_and_terms("database"), None);
        assert_eq!(
            english_websearch_positive_and_terms("database OR search"),
            None
        );
        assert_eq!(
            english_websearch_positive_and_terms("database -search"),
            None
        );
        assert_eq!(
            english_websearch_positive_and_terms("\"database search\""),
            None
        );
    }

    #[test]
    fn application_hybrid_rank_uses_english_websearch_and_ordered_weights() {
        let title_match = application_websearch_rank_cd_english(
            &[
                "BicDB application vector database".to_string(),
                "unrelated".to_string(),
            ],
            "database",
        );
        let body_match = application_websearch_rank_cd_english(
            &[
                "unrelated".to_string(),
                "BicDB application vector database".to_string(),
            ],
            "database",
        );
        let stemmed = application_websearch_rank_cd_english(
            &["running databases".to_string(), "".to_string()],
            "run database",
        );
        let missing = application_websearch_rank_cd_english(
            &[
                "BicDB application vector database".to_string(),
                "".to_string(),
            ],
            "absent",
        );

        assert!(title_match > body_match);
        assert!(body_match > 0.0);
        assert!(stemmed > 0.0);
        assert_eq!(missing, 0.0);
    }

    #[test]
    fn tsvector_input_canonicalizes_like_postgres() {
        let vector =
            PgTsVector::from_postgres_text("'z':2,1,1A,2B,4D,3C 'a b':1A,2B,2C,3D 'O''Reilly':4")
                .unwrap();
        assert_eq!(
            vector.to_postgres_text(),
            "'O''Reilly':4 'a b':1A,2B,3 'z':1A,2B,3C,4"
        );
    }

    #[test]
    fn tsvector_concat_shifts_right_positions() {
        let left = PgTsVector::from_postgres_text("'a':1A").unwrap();
        let right = PgTsVector::from_postgres_text("'b':2B").unwrap();
        assert_eq!(left.concat(&right).to_postgres_text(), "'a':1A 'b':3B");
    }

    #[test]
    fn tsquery_input_preserves_postgres_tree_semantics() {
        let cases = [
            ("fat|rat&cat", "'fat' | 'rat' & 'cat'"),
            ("!(fat | rat)", "!( 'fat' | 'rat' )"),
            ("fat <-> rat", "'fat' <-> 'rat'"),
            ("fat <3> rat", "'fat' <3> 'rat'"),
            ("fat:*BA & rat:DC", "'fat':*AB & 'rat':CD"),
            ("a <-> (b <-> c)", "'a' <-> ( 'b' <-> 'c' )"),
            ("(a <-> b) <-> c", "'a' <-> 'b' <-> 'c'"),
        ];
        for (input, expected) in cases {
            assert_eq!(
                PgTsQuery::from_postgres_text(input)
                    .unwrap()
                    .to_postgres_text(),
                expected
            );
        }
    }

    #[test]
    fn tsquery_matching_honors_phrase_prefix_and_weights() {
        let vector = PgTsVector::from_postgres_text("'a':1A 'alpha':2 'b':3B 'cat':4C").unwrap();
        for query in ["a & b", "a <2> b", "al:*", "cat:C", "a | missing"] {
            assert!(PgTsQuery::from_postgres_text(query)
                .unwrap()
                .matches(&vector));
        }
        for query in ["a <-> b", "a:B", "missing", "a & !b"] {
            assert!(!PgTsQuery::from_postgres_text(query)
                .unwrap()
                .matches(&vector));
        }
    }

    #[test]
    fn tsquery_phrase_boolean_locations_match_postgres() {
        let vector = PgTsVector::from_postgres_text("'a':1 'x':1 'b':2 'c':3 'd':4").unwrap();
        let cases = [
            ("a <-> !b", false),
            ("a <-> !x", true),
            ("!a <-> b", false),
            ("!x <-> b", false),
            ("(a | x) <-> b", true),
            ("(a & x) <-> b", true),
            ("(a | c) <-> d", true),
            ("a <-> (b | c <-> d)", true),
            ("a <-> !(x | c)", true),
            ("!!a", true),
        ];
        for (query, expected) in cases {
            assert_eq!(
                PgTsQuery::from_postgres_text(query)
                    .unwrap()
                    .matches(&vector),
                expected,
                "{query}"
            );
        }
    }

    #[test]
    fn tsquery_comparison_ignores_modifiers_but_preserves_tree_shape() {
        let weight_a = PgTsQuery::from_postgres_text("a:A").unwrap();
        let weight_b = PgTsQuery::from_postgres_text("a:B").unwrap();
        assert_eq!(weight_a.cmp(&weight_b), Ordering::Equal);
        let right_nested = PgTsQuery::from_postgres_text("a & (b & c)").unwrap();
        let left_nested = PgTsQuery::from_postgres_text("(a & b) & c").unwrap();
        assert_eq!(right_nested.cmp(&left_nested), Ordering::Less);
    }

    #[test]
    fn postgres_binary_text_search_values_match_postgres_18() {
        let vector = PgTsVector::from_postgres_text("'fat':1A,3B 'rat':2").unwrap();
        let vector_binary = decode_test_hex("00000002666174000002c00180037261740000010002");
        assert_eq!(vector.to_postgres_binary(), vector_binary);
        assert_eq!(
            PgTsVector::from_postgres_binary(&vector_binary).unwrap(),
            vector
        );

        for (text, expected) in [
            ("", "00000000"),
            ("'fat':AB*", "00000001010c0166617400"),
            (
                "'fat' & !'rat'",
                "00000004020202010100007261740001000066617400",
            ),
            (
                "'fat' <3> 'rat'",
                "00000003020400030100007261740001000066617400",
            ),
        ] {
            let query = PgTsQuery::from_postgres_text(text).unwrap();
            let expected = decode_test_hex(expected);
            assert_eq!(query.to_postgres_binary(), expected, "{text}");
            assert_eq!(
                PgTsQuery::from_postgres_binary(&expected).unwrap(),
                query,
                "{text}"
            );
        }
    }

    #[test]
    fn postgres_binary_text_search_values_reject_malformed_payloads() {
        for bytes in [
            &[][..],
            &[0, 0, 0, 1, b'x'][..],
            &[0, 0, 0, 1, b'x', 0, 0, 1, 0, 0][..],
        ] {
            assert!(PgTsVector::from_postgres_binary(bytes).is_err());
        }
        for bytes in [
            &[][..],
            &[0, 0, 0, 1, 2, 2][..],
            &[0, 0, 0, 1, 1, 0, 0, b'x'][..],
        ] {
            assert!(PgTsQuery::from_postgres_binary(bytes).is_err());
        }
    }

    fn decode_test_hex(value: &str) -> Vec<u8> {
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let text = std::str::from_utf8(pair).unwrap();
                u8::from_str_radix(text, 16).unwrap()
            })
            .collect()
    }

    #[test]
    fn fts_function_dispatch_recognizes_constructors() {
        let value = eval_fts_function_value(
            "to_tsvector",
            &[SqlValue::String("The Fat Rats".into())],
            Some(&[None]),
        )
        .unwrap();
        assert_eq!(value, Some(SqlValue::String("'fat':2 'rat':3".to_string())));
    }
}
