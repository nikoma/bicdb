use std::cell::Cell;
use std::cmp::Ordering;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
use regex::RegexBuilder;
use serde_json::{json, Map, Number, Value as JsonValue};

use crate::{Result, SqlError, SqlValue};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    Lax,
    Strict,
}

#[derive(Clone, Debug, PartialEq)]
enum Token {
    Root,
    Current,
    Variable(String),
    String(String),
    Number(String),
    Ident(String),
    Dot,
    Question,
    LParen,
    RParen,
    LBracket,
    RBracket,
    LBrace,
    RBrace,
    Comma,
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
    Not,
    Eof,
}

#[derive(Clone, Debug)]
pub(crate) struct JsonPath {
    mode: Mode,
    expression: Expr,
}

#[derive(Clone, Debug)]
enum Expr {
    Root,
    Current,
    Variable(String),
    Literal(JsonValue),
    Member(Box<Expr>, String),
    MemberWildcard(Box<Expr>),
    Descendant(Box<Expr>, usize, Option<usize>),
    Array(Box<Expr>, Vec<ArraySelector>),
    Filter(Box<Expr>, Box<Expr>),
    Method(Box<Expr>, Method, Vec<Expr>),
    Exists(Box<Expr>),
    Unary(UnaryOp, Box<Expr>),
    Binary(Box<Expr>, BinaryOp, Box<Expr>),
    LikeRegex(Box<Expr>, Box<Expr>, String),
    IsUnknown(Box<Expr>),
}

#[derive(Clone, Debug)]
enum ArraySelector {
    Wildcard,
    Index(Endpoint),
    Range(Endpoint, Endpoint),
}

#[derive(Clone, Debug)]
enum Endpoint {
    Last(i64),
    Absolute(i64),
    Variable(String, i64),
    LastVariable(String, i64),
}

#[derive(Clone, Copy, Debug)]
enum UnaryOp {
    Not,
    Plus,
    Minus,
}

#[derive(Clone, Copy, Debug)]
enum BinaryOp {
    Or,
    And,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    StartsWith,
}

#[derive(Clone, Copy, Debug)]
enum Method {
    Type,
    Size,
    Double,
    Ceiling,
    Floor,
    Abs,
    KeyValue,
    BigInt,
    Boolean,
    Decimal,
    Integer,
    Number,
    String,
    Date,
    Time,
    TimeTz,
    Timestamp,
    TimestampTz,
    DateTime,
}

impl Method {
    fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "type" => Self::Type,
            "size" => Self::Size,
            "double" => Self::Double,
            "ceiling" => Self::Ceiling,
            "floor" => Self::Floor,
            "abs" => Self::Abs,
            "keyvalue" => Self::KeyValue,
            "bigint" => Self::BigInt,
            "boolean" => Self::Boolean,
            "decimal" => Self::Decimal,
            "integer" => Self::Integer,
            "number" => Self::Number,
            "string" => Self::String,
            "date" => Self::Date,
            "time" => Self::Time,
            "time_tz" => Self::TimeTz,
            "timestamp" => Self::Timestamp,
            "timestamp_tz" => Self::TimestampTz,
            "datetime" => Self::DateTime,
            _ => return None,
        })
    }
}

fn syntax_error(message: impl Into<String>) -> SqlError {
    SqlError::data_exception("42601", message, Some("jsonpath".to_string()))
}

fn execution_error(message: impl Into<String>) -> SqlError {
    SqlError::data_exception("2203A", message, Some("jsonpath".to_string()))
}

struct Lexer<'a> {
    input: &'a str,
    offset: usize,
}

impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, offset: 0 }
    }

    fn tokenize(mut self) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();
        loop {
            self.skip_whitespace();
            if self.offset == self.input.len() {
                tokens.push(Token::Eof);
                return Ok(tokens);
            }
            let token = self.next_token()?;
            tokens.push(token);
        }
    }

    fn next_token(&mut self) -> Result<Token> {
        let rest = &self.input[self.offset..];
        let first = rest.chars().next().expect("lexer has input");
        let width = first.len_utf8();
        self.offset += width;
        Ok(match first {
            '$' => {
                if self.consume('"') {
                    Token::Variable(self.quoted_string()?)
                } else if self.peek().is_some_and(is_ident_start) {
                    Token::Variable(self.identifier())
                } else {
                    Token::Root
                }
            }
            '@' => Token::Current,
            '"' => Token::String(self.quoted_string()?),
            '.' => Token::Dot,
            '?' => Token::Question,
            '(' => Token::LParen,
            ')' => Token::RParen,
            '[' => Token::LBracket,
            ']' => Token::RBracket,
            '{' => Token::LBrace,
            '}' => Token::RBrace,
            ',' => Token::Comma,
            '+' => Token::Plus,
            '-' => Token::Minus,
            '*' => Token::Star,
            '/' => Token::Slash,
            '%' => Token::Percent,
            '=' if self.consume('=') => Token::Eq,
            '=' => return Err(syntax_error("single '=' is not allowed in jsonpath")),
            '!' if self.consume('=') => Token::NotEq,
            '!' => Token::Not,
            '<' if self.consume('=') => Token::LtEq,
            '<' => Token::Lt,
            '>' if self.consume('=') => Token::GtEq,
            '>' => Token::Gt,
            '&' if self.consume('&') => Token::And,
            '|' if self.consume('|') => Token::Or,
            character if character.is_ascii_digit() => {
                self.offset -= width;
                Token::Number(self.number()?)
            }
            character if is_ident_start(character) => {
                self.offset -= width;
                Token::Ident(self.identifier())
            }
            character => {
                return Err(syntax_error(format!(
                    "unexpected character '{character}' in jsonpath"
                )))
            }
        })
    }

    fn peek(&self) -> Option<char> {
        self.input[self.offset..].chars().next()
    }

    fn consume(&mut self, expected: char) -> bool {
        if self.peek() == Some(expected) {
            self.offset += expected.len_utf8();
            true
        } else {
            false
        }
    }

    fn skip_whitespace(&mut self) {
        while self.peek().is_some_and(char::is_whitespace) {
            self.offset += self.peek().unwrap().len_utf8();
        }
    }

    fn identifier(&mut self) -> String {
        let start = self.offset;
        while self.peek().is_some_and(is_ident_continue) {
            self.offset += self.peek().unwrap().len_utf8();
        }
        self.input[start..self.offset].to_string()
    }

    fn quoted_string(&mut self) -> Result<String> {
        let start = self.offset - 1;
        let mut escaped = false;
        while let Some(character) = self.peek() {
            self.offset += character.len_utf8();
            if character == '"' && !escaped {
                return serde_json::from_str(&self.input[start..self.offset]).map_err(|error| {
                    syntax_error(format!("invalid quoted jsonpath member: {error}"))
                });
            }
            escaped = character == '\\' && !escaped;
            if character != '\\' {
                escaped = false;
            }
        }
        Err(syntax_error("unterminated quoted string in jsonpath"))
    }

    fn number(&mut self) -> Result<String> {
        let start = self.offset;
        while self.peek().is_some_and(|character| {
            character.is_ascii_digit() || matches!(character, '.' | 'e' | 'E' | '+' | '-')
        }) {
            self.offset += self.peek().unwrap().len_utf8();
        }
        let value = &self.input[start..self.offset];
        value
            .parse::<Number>()
            .map_err(|_| syntax_error(format!("invalid jsonpath number '{value}'")))?;
        Ok(value.to_string())
    }
}

fn is_ident_start(character: char) -> bool {
    character == '_' || character.is_alphabetic()
}

fn is_ident_continue(character: char) -> bool {
    is_ident_start(character) || character.is_ascii_digit()
}

struct Parser {
    tokens: Vec<Token>,
    cursor: usize,
    /// jsonpath is recursive descent over a CLIENT-SUPPLIED path string:
    /// `jsonb_path_exists('{}', '(((((…')` recurses once per token and
    /// overflows the stack without this. See `bicdb_core::parse_budget`.
    budget: bicdb_core::parse_budget::ParseBudget,
}

impl Parser {
    fn parse(input: &str) -> Result<JsonPath> {
        let tokens = Lexer::new(input).tokenize()?;
        let mut parser = Self {
            tokens,
            cursor: 0,
            budget: bicdb_core::parse_budget::ParseBudget::new("jsonpath"),
        };
        let mode = match parser.peek() {
            Token::Ident(value) if value.eq_ignore_ascii_case("strict") => {
                parser.next();
                Mode::Strict
            }
            Token::Ident(value) if value.eq_ignore_ascii_case("lax") => {
                parser.next();
                Mode::Lax
            }
            _ => Mode::Lax,
        };
        let expression = parser.expression(0)?;
        if !matches!(parser.peek(), Token::Eof) {
            return Err(syntax_error(format!(
                "unexpected token {:?} after jsonpath expression",
                parser.peek()
            )));
        }
        Ok(JsonPath { mode, expression })
    }

    fn peek(&self) -> &Token {
        &self.tokens[self.cursor]
    }

    fn next(&mut self) -> Token {
        let token = self.tokens[self.cursor].clone();
        self.cursor += 1;
        token
    }

    /// Nesting-budgeted entry to the expression grammar. Every recursive
    /// descent in `prefix` routes through here.
    fn nested_expression(&mut self, min_binding_power: u8) -> Result<Expr> {
        self.budget
            .enter()
            .map_err(|error| syntax_error(error.message().to_string()))?;
        let expression = self.expression(min_binding_power);
        self.budget.leave();
        expression
    }

    /// Charge one level of TREE depth for a step that wraps the accumulated
    /// expression. These loops are iterative, so the PARSER never recurses —
    /// but each step deepens the tree, and evaluation and the tree's own
    /// recursive `Drop` do recurse over it. No matching `leave`: the level
    /// persists in the tree.
    fn charge_tree_depth(&mut self) -> Result<()> {
        self.budget
            .enter()
            .map_err(|error| syntax_error(error.message().to_string()))
    }

    fn expression(&mut self, min_binding_power: u8) -> Result<Expr> {
        let mut left = self.prefix()?;
        loop {
            while let Some(next) = self.postfix(left.clone())? {
                // Each postfix step wraps the accumulated expression in
                // another node, so a FLAT chain like `$.a.a.a…` or
                // `$[0][0][0]…` builds a tree exactly as deep as the chain is
                // long — even though this loop is iterative and never
                // recurses. The nesting budget only fired on constructs that
                // recurse during parsing (parens, filters), so a few thousand
                // accessors sailed past it and then overflowed the stack in
                // evaluation and in the recursive `Drop` of the tree itself.
                // A stack overflow is a hardware fault, not a panic: it aborts
                // the process for every tenant, and `catch_unwind` cannot hold
                // it. Charge tree depth here, where the depth is actually
                // created, and never `leave` — the level persists in the tree
                // for the rest of the parse.
                self.charge_tree_depth()?;
                left = next;
            }
            if matches!(self.peek(), Token::Ident(value) if value.eq_ignore_ascii_case("starts")) {
                if 5 < min_binding_power {
                    break;
                }
                self.next();
                match self.next() {
                    Token::Ident(value) if value.eq_ignore_ascii_case("with") => {}
                    token => {
                        return Err(syntax_error(format!(
                            "expected WITH after STARTS, found {token:?}"
                        )))
                    }
                }
                let right = self.expression(6)?;
                self.charge_tree_depth()?;
                left = Expr::Binary(Box::new(left), BinaryOp::StartsWith, Box::new(right));
                continue;
            }
            if matches!(self.peek(), Token::Ident(value) if value.eq_ignore_ascii_case("like_regex"))
            {
                if 5 < min_binding_power {
                    break;
                }
                self.next();
                let right = self.expression(6)?;
                let flags = if matches!(self.peek(), Token::Ident(value) if value.eq_ignore_ascii_case("flag"))
                {
                    self.next();
                    match self.next() {
                        Token::String(flags) => flags,
                        token => {
                            return Err(syntax_error(format!(
                                "expected quoted flags after FLAG, found {token:?}"
                            )))
                        }
                    }
                } else {
                    String::new()
                };
                self.charge_tree_depth()?;
                left = Expr::LikeRegex(Box::new(left), Box::new(right), flags);
                continue;
            }
            let (operator, left_power, right_power) = match self.peek() {
                Token::Or => (BinaryOp::Or, 1, 2),
                Token::And => (BinaryOp::And, 3, 4),
                Token::Eq => (BinaryOp::Eq, 5, 6),
                Token::NotEq => (BinaryOp::NotEq, 5, 6),
                Token::Lt => (BinaryOp::Lt, 5, 6),
                Token::LtEq => (BinaryOp::LtEq, 5, 6),
                Token::Gt => (BinaryOp::Gt, 5, 6),
                Token::GtEq => (BinaryOp::GtEq, 5, 6),
                Token::Plus => (BinaryOp::Add, 7, 8),
                Token::Minus => (BinaryOp::Subtract, 7, 8),
                Token::Star => (BinaryOp::Multiply, 9, 10),
                Token::Slash => (BinaryOp::Divide, 9, 10),
                Token::Percent => (BinaryOp::Modulo, 9, 10),
                Token::Ident(value) if value.eq_ignore_ascii_case("is") => {
                    if 5 < min_binding_power {
                        break;
                    }
                    self.next();
                    match self.next() {
                        Token::Ident(value) if value.eq_ignore_ascii_case("unknown") => {
                            self.charge_tree_depth()?;
                            left = Expr::IsUnknown(Box::new(left));
                            continue;
                        }
                        token => {
                            return Err(syntax_error(format!(
                                "expected UNKNOWN after IS, found {token:?}"
                            )))
                        }
                    }
                }
                _ => break,
            };
            if left_power < min_binding_power {
                break;
            }
            self.next();
            let right = self.expression(right_power)?;
            self.charge_tree_depth()?;
            left = Expr::Binary(Box::new(left), operator, Box::new(right));
        }
        Ok(left)
    }

    fn prefix(&mut self) -> Result<Expr> {
        Ok(match self.next() {
            Token::Root => Expr::Root,
            Token::Current => Expr::Current,
            Token::Variable(name) => Expr::Variable(name),
            Token::String(value) => Expr::Literal(JsonValue::String(value)),
            Token::Number(value) => Expr::Literal(JsonValue::Number(
                value
                    .parse()
                    .map_err(|_| syntax_error("invalid jsonpath numeric literal"))?,
            )),
            Token::Ident(value) if value.eq_ignore_ascii_case("true") => {
                Expr::Literal(JsonValue::Bool(true))
            }
            Token::Ident(value) if value.eq_ignore_ascii_case("false") => {
                Expr::Literal(JsonValue::Bool(false))
            }
            Token::Ident(value) if value.eq_ignore_ascii_case("null") => {
                Expr::Literal(JsonValue::Null)
            }
            Token::Ident(value) if value.eq_ignore_ascii_case("exists") => {
                self.expect(Token::LParen)?;
                let expression = self.nested_expression(0)?;
                self.expect(Token::RParen)?;
                Expr::Exists(Box::new(expression))
            }
            Token::LParen => {
                let expression = self.nested_expression(0)?;
                self.expect(Token::RParen)?;
                expression
            }
            Token::Not => Expr::Unary(UnaryOp::Not, Box::new(self.nested_expression(11)?)),
            Token::Plus => Expr::Unary(UnaryOp::Plus, Box::new(self.nested_expression(11)?)),
            Token::Minus => Expr::Unary(UnaryOp::Minus, Box::new(self.nested_expression(11)?)),
            token => {
                return Err(syntax_error(format!(
                    "unexpected token {token:?} in jsonpath expression"
                )))
            }
        })
    }

    fn postfix(&mut self, left: Expr) -> Result<Option<Expr>> {
        match self.peek() {
            Token::Dot => {
                self.next();
                match self.next() {
                    Token::String(name) | Token::Ident(name) => {
                        if matches!(self.peek(), Token::LParen) {
                            self.next();
                            let mut arguments = Vec::new();
                            if !matches!(self.peek(), Token::RParen) {
                                loop {
                                    arguments.push(self.expression(0)?);
                                    if !matches!(self.peek(), Token::Comma) {
                                        break;
                                    }
                                    self.next();
                                }
                            }
                            self.expect(Token::RParen)?;
                            let method = Method::parse(&name).ok_or_else(|| {
                                syntax_error(format!("unknown jsonpath method {name}()"))
                            })?;
                            validate_method_argument_count(method, arguments.len())?;
                            Ok(Some(Expr::Method(Box::new(left), method, arguments)))
                        } else {
                            Ok(Some(Expr::Member(Box::new(left), name)))
                        }
                    }
                    Token::Star => {
                        if matches!(self.peek(), Token::Star) {
                            self.next();
                            let (minimum, maximum) = self.descendant_bounds()?;
                            Ok(Some(Expr::Descendant(Box::new(left), minimum, maximum)))
                        } else {
                            Ok(Some(Expr::MemberWildcard(Box::new(left))))
                        }
                    }
                    token => Err(syntax_error(format!(
                        "expected member, wildcard, or method after '.', found {token:?}"
                    ))),
                }
            }
            Token::LBracket => {
                self.next();
                let selectors = self.array_selectors()?;
                self.expect(Token::RBracket)?;
                Ok(Some(Expr::Array(Box::new(left), selectors)))
            }
            Token::Question => {
                self.next();
                self.expect(Token::LParen)?;
                let predicate = self.expression(0)?;
                self.expect(Token::RParen)?;
                Ok(Some(Expr::Filter(Box::new(left), Box::new(predicate))))
            }
            _ => Ok(None),
        }
    }

    fn descendant_bounds(&mut self) -> Result<(usize, Option<usize>)> {
        if !matches!(self.peek(), Token::LBrace) {
            return Ok((0, None));
        }
        self.next();
        let minimum = self.nonnegative_integer()?;
        let maximum = if matches!(self.peek(), Token::Ident(value) if value.eq_ignore_ascii_case("to"))
        {
            self.next();
            if matches!(self.peek(), Token::Ident(value) if value.eq_ignore_ascii_case("last")) {
                self.next();
                None
            } else {
                Some(self.nonnegative_integer()?)
            }
        } else {
            Some(minimum)
        };
        self.expect(Token::RBrace)?;
        if maximum.is_some_and(|maximum| maximum < minimum) {
            return Err(syntax_error("jsonpath descendant maximum is below minimum"));
        }
        Ok((minimum, maximum))
    }

    fn nonnegative_integer(&mut self) -> Result<usize> {
        match self.next() {
            Token::Number(value) => value
                .parse::<usize>()
                .map_err(|_| syntax_error("jsonpath bound must be a nonnegative integer")),
            token => Err(syntax_error(format!(
                "expected nonnegative integer, found {token:?}"
            ))),
        }
    }

    fn array_selectors(&mut self) -> Result<Vec<ArraySelector>> {
        let mut selectors = Vec::new();
        loop {
            if matches!(self.peek(), Token::Star) {
                self.next();
                selectors.push(ArraySelector::Wildcard);
            } else {
                let start = self.endpoint()?;
                if matches!(self.peek(), Token::Ident(value) if value.eq_ignore_ascii_case("to")) {
                    self.next();
                    selectors.push(ArraySelector::Range(start, self.endpoint()?));
                } else {
                    selectors.push(ArraySelector::Index(start));
                }
            }
            if !matches!(self.peek(), Token::Comma) {
                break;
            }
            self.next();
        }
        Ok(selectors)
    }

    fn endpoint(&mut self) -> Result<Endpoint> {
        let negative = matches!(self.peek(), Token::Minus);
        if negative {
            self.next();
        }
        match self.next() {
            Token::Number(value) => {
                let mut value = value
                    .parse::<i64>()
                    .map_err(|_| syntax_error("array subscript must be an integer"))?;
                if matches!(self.peek(), Token::Minus | Token::Plus) {
                    let subtract = matches!(self.next(), Token::Minus);
                    let Token::Number(offset) = self.next() else {
                        return Err(syntax_error(
                            "array subscript arithmetic requires an integer",
                        ));
                    };
                    let offset = offset.parse::<i64>().map_err(|_| {
                        syntax_error("array subscript arithmetic requires an integer")
                    })?;
                    value = if subtract {
                        value.checked_sub(offset)
                    } else {
                        value.checked_add(offset)
                    }
                    .ok_or_else(|| syntax_error("array subscript is out of range"))?;
                }
                Ok(Endpoint::Absolute(if negative { -value } else { value }))
            }
            Token::Variable(name) => Ok(Endpoint::Variable(name, if negative { -1 } else { 1 })),
            Token::Ident(value) if value.eq_ignore_ascii_case("last") => {
                let mut offset = 0_i64;
                if matches!(self.peek(), Token::Minus | Token::Plus) {
                    let subtract = matches!(self.next(), Token::Minus);
                    match self.next() {
                        Token::Number(value) => {
                            offset = value
                                .parse::<i64>()
                                .map_err(|_| syntax_error("LAST offset must be an integer"))?;
                            if subtract {
                                offset = -offset;
                            }
                        }
                        Token::Variable(name) => {
                            return Ok(Endpoint::LastVariable(name, if subtract { -1 } else { 1 }));
                        }
                        _ => return Err(syntax_error("LAST offset must be an integer")),
                    }
                }
                Ok(Endpoint::Last(offset))
            }
            token => Err(syntax_error(format!(
                "unsupported jsonpath array subscript {token:?}"
            ))),
        }
    }

    fn expect(&mut self, expected: Token) -> Result<()> {
        let actual = self.next();
        if std::mem::discriminant(&actual) == std::mem::discriminant(&expected) {
            Ok(())
        } else {
            Err(syntax_error(format!(
                "expected {expected:?}, found {actual:?}"
            )))
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Truth {
    True,
    False,
    Unknown,
}

struct EvalContext<'a> {
    root: &'a JsonValue,
    current: &'a JsonValue,
    variables: &'a Map<String, JsonValue>,
    mode: Mode,
    timezone_aware: bool,
    keyvalue_counter: &'a Cell<u64>,
}

impl JsonPath {
    pub(crate) fn parse(input: &str) -> Result<Self> {
        Parser::parse(input)
    }

    pub(crate) fn query(
        &self,
        target: &JsonValue,
        variables: &Map<String, JsonValue>,
        timezone_aware: bool,
    ) -> Result<Vec<JsonValue>> {
        let keyvalue_counter = Cell::new(1);
        self.expression.evaluate(&EvalContext {
            root: target,
            current: target,
            variables,
            mode: self.mode,
            timezone_aware,
            keyvalue_counter: &keyvalue_counter,
        })
    }

    fn required_member_keys(&self) -> Vec<String> {
        let mut keys = Vec::new();
        self.expression.required_member_keys(&mut keys);
        keys.sort();
        keys.dedup();
        keys
    }

    fn canonical_text(&self) -> String {
        let expression = self.expression.canonical_text();
        if self.mode == Mode::Strict {
            format!("strict {expression}")
        } else {
            expression
        }
    }
}

impl Expr {
    fn canonical_text(&self) -> String {
        match self {
            Self::Root => "$".to_string(),
            Self::Current => "@".to_string(),
            Self::Variable(name) => format!("${}", quoted(name)),
            Self::Literal(value) => value.to_string(),
            Self::Member(base, name) => format!("{}.{}", base.canonical_text(), quoted(name)),
            Self::MemberWildcard(base) => format!("{}.*", base.canonical_text()),
            Self::Descendant(base, minimum, maximum) => {
                let bounds = match maximum {
                    None if *minimum == 0 => String::new(),
                    None => format!("{{{minimum} to last}}"),
                    Some(maximum) if maximum == minimum => format!("{{{minimum}}}"),
                    Some(maximum) => format!("{{{minimum} to {maximum}}}"),
                };
                format!("{}.**{bounds}", base.canonical_text())
            }
            Self::Array(base, selectors) => format!(
                "{}[{}]",
                base.canonical_text(),
                selectors
                    .iter()
                    .map(ArraySelector::canonical_text)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Self::Filter(base, predicate) => {
                let predicate = predicate.canonical_text();
                if predicate.starts_with('(') && predicate.ends_with(')') {
                    format!("{}?{predicate}", base.canonical_text())
                } else {
                    format!("{}?({predicate})", base.canonical_text())
                }
            }
            Self::Method(base, method, arguments) => format!(
                "{}.{}({})",
                base.canonical_text(),
                method.name(),
                arguments
                    .iter()
                    .map(Self::canonical_text)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Self::Exists(expression) => format!("exists({})", expression.canonical_text()),
            Self::Unary(operator, expression) => format!(
                "{}{}",
                match operator {
                    UnaryOp::Not => "!",
                    UnaryOp::Plus => "+",
                    UnaryOp::Minus => "-",
                },
                expression.canonical_text()
            ),
            Self::Binary(left, operator, right) => format!(
                "({} {} {})",
                left.canonical_text(),
                operator.name(),
                right.canonical_text()
            ),
            Self::LikeRegex(left, pattern, flags) => {
                let flags = if flags.is_empty() {
                    String::new()
                } else {
                    format!(" flag {}", quoted(flags))
                };
                format!(
                    "({} like_regex {}{flags})",
                    left.canonical_text(),
                    pattern.canonical_text()
                )
            }
            Self::IsUnknown(expression) => format!("({} is unknown)", expression.canonical_text()),
        }
    }

    fn required_member_keys(&self, output: &mut Vec<String>) {
        match self {
            Self::Member(base, name) => {
                base.required_member_keys(output);
                output.push(name.clone());
            }
            Self::MemberWildcard(base)
            | Self::Descendant(base, ..)
            | Self::Array(base, ..)
            | Self::Method(base, ..) => base.required_member_keys(output),
            Self::Filter(base, _) => base.required_member_keys(output),
            Self::Exists(expression) | Self::Unary(_, expression) | Self::IsUnknown(expression) => {
                expression.required_member_keys(output)
            }
            Self::Root
            | Self::Current
            | Self::Variable(_)
            | Self::Literal(_)
            | Self::Binary(..)
            | Self::LikeRegex(..) => {}
        }
    }

    fn evaluate(&self, context: &EvalContext<'_>) -> Result<Vec<JsonValue>> {
        match self {
            Self::Root => Ok(vec![context.root.clone()]),
            Self::Current => Ok(vec![context.current.clone()]),
            Self::Variable(name) => context
                .variables
                .get(name)
                .cloned()
                .map(|value| vec![value])
                .ok_or_else(|| execution_error(format!("could not find jsonpath variable {name}"))),
            Self::Literal(value) => Ok(vec![value.clone()]),
            Self::Member(base, name) => {
                let mut output = Vec::new();
                for value in base.evaluate(context)? {
                    member_values(&value, name, context.mode, &mut output)?;
                }
                Ok(output)
            }
            Self::MemberWildcard(base) => {
                let mut output = Vec::new();
                for value in base.evaluate(context)? {
                    match value {
                        JsonValue::Object(object) => output.extend(object.into_values()),
                        JsonValue::Array(values) if context.mode == Mode::Lax => {
                            for value in values {
                                match value {
                                    JsonValue::Object(object) => {
                                        output.extend(object.into_values())
                                    }
                                    _ => output.push(value),
                                }
                            }
                        }
                        _ if context.mode == Mode::Strict => {
                            return Err(execution_error(
                                "jsonpath member wildcard can only be applied to an object",
                            ))
                        }
                        _ => {}
                    }
                }
                Ok(output)
            }
            Self::Descendant(base, minimum, maximum) => {
                let mut output = Vec::new();
                for value in base.evaluate(context)? {
                    descendants(&value, 0, *minimum, *maximum, &mut output);
                }
                Ok(output)
            }
            Self::Array(base, selectors) => {
                let mut output = Vec::new();
                for value in base.evaluate(context)? {
                    match value {
                        JsonValue::Array(values) => {
                            for selector in selectors {
                                selector.select(&values, context, &mut output)?;
                            }
                        }
                        value if context.mode == Mode::Lax => {
                            let values = vec![value];
                            for selector in selectors {
                                selector.select(&values, context, &mut output)?;
                            }
                        }
                        _ => {
                            return Err(execution_error(
                                "jsonpath array accessor can only be applied to an array",
                            ))
                        }
                    }
                }
                Ok(output)
            }
            Self::Filter(base, predicate) => {
                let mut output = Vec::new();
                for value in base.evaluate(context)? {
                    let candidates = match value {
                        JsonValue::Array(values) if context.mode == Mode::Lax => values,
                        value => vec![value],
                    };
                    for value in candidates {
                        let local = EvalContext {
                            current: &value,
                            ..*context
                        };
                        if predicate.truth(&local)? == Truth::True {
                            output.push(value);
                        }
                    }
                }
                Ok(output)
            }
            Self::Method(base, method, arguments) => {
                let mut output = Vec::new();
                for value in base.evaluate(context)? {
                    apply_method(*method, arguments, value, context, &mut output)?;
                }
                Ok(output)
            }
            Self::Exists(expression) => Ok(vec![JsonValue::Bool(
                !expression.evaluate(context)?.is_empty(),
            )]),
            Self::Unary(UnaryOp::Not, expression) => {
                Ok(vec![truth_json(match expression.truth(context)? {
                    Truth::True => Truth::False,
                    Truth::False => Truth::True,
                    Truth::Unknown => Truth::Unknown,
                })])
            }
            Self::Unary(operator @ (UnaryOp::Plus | UnaryOp::Minus), expression) => expression
                .evaluate(context)?
                .into_iter()
                .map(|value| unary_number(*operator, value))
                .collect(),
            Self::Binary(left, operator, right) => match operator {
                BinaryOp::Or | BinaryOp::And => Ok(vec![truth_json(self.truth(context)?)]),
                BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq => {
                    let truth = compare_datetime_expressions(left, *operator, right, context)?
                        .unwrap_or(compare_sequences(
                            &left.evaluate(context)?,
                            *operator,
                            &right.evaluate(context)?,
                            context.mode,
                        )?);
                    Ok(vec![truth_json(truth)])
                }
                BinaryOp::Add
                | BinaryOp::Subtract
                | BinaryOp::Multiply
                | BinaryOp::Divide
                | BinaryOp::Modulo => arithmetic_sequences(
                    &left.evaluate(context)?,
                    *operator,
                    &right.evaluate(context)?,
                ),
                BinaryOp::StartsWith => Ok(vec![truth_json(string_predicate_sequences(
                    &left.evaluate(context)?,
                    &right.evaluate(context)?,
                    |left, right| left.starts_with(right),
                ))]),
            },
            Self::LikeRegex(left, pattern, flags) => {
                let patterns = pattern.evaluate(context)?;
                let [JsonValue::String(pattern)] = patterns.as_slice() else {
                    return Ok(vec![JsonValue::Null]);
                };
                crate::reject_oversized_regex(pattern)?;
                let mut builder = RegexBuilder::new(pattern);
                builder.size_limit(1 << 20).dfa_size_limit(1 << 20);
                for flag in flags.chars() {
                    match flag {
                        'i' => {
                            builder.case_insensitive(true);
                        }
                        'm' => {
                            builder.multi_line(true);
                        }
                        's' => {
                            builder.dot_matches_new_line(true);
                        }
                        'q' => {
                            builder = RegexBuilder::new(&regex::escape(pattern));
                        }
                        'x' => {
                            builder.ignore_whitespace(true);
                        }
                        _ => {
                            return Err(syntax_error(format!(
                                "unrecognized jsonpath regular expression flag '{flag}'"
                            )))
                        }
                    }
                }
                let regex = builder.build().map_err(|error| {
                    execution_error(format!("invalid regular expression: {error}"))
                })?;
                let matched = left
                    .evaluate(context)?
                    .iter()
                    .any(|value| value.as_str().is_some_and(|value| regex.is_match(value)));
                Ok(vec![JsonValue::Bool(matched)])
            }
            Self::IsUnknown(expression) => Ok(vec![JsonValue::Bool(
                expression.truth(context)? == Truth::Unknown,
            )]),
        }
    }

    fn truth(&self, context: &EvalContext<'_>) -> Result<Truth> {
        match self {
            Self::Binary(left, BinaryOp::Or, right) => {
                let left = left.truth(context)?;
                if left == Truth::True {
                    return Ok(Truth::True);
                }
                let right = right.truth(context)?;
                Ok(match (left, right) {
                    (_, Truth::True) => Truth::True,
                    (Truth::False, Truth::False) => Truth::False,
                    _ => Truth::Unknown,
                })
            }
            Self::Binary(left, BinaryOp::And, right) => {
                let left = left.truth(context)?;
                if left == Truth::False {
                    return Ok(Truth::False);
                }
                let right = right.truth(context)?;
                Ok(match (left, right) {
                    (_, Truth::False) => Truth::False,
                    (Truth::True, Truth::True) => Truth::True,
                    _ => Truth::Unknown,
                })
            }
            _ => {
                let values = self.evaluate(context)?;
                if values.len() != 1 {
                    return Ok(Truth::Unknown);
                }
                Ok(match values.first() {
                    Some(JsonValue::Bool(true)) => Truth::True,
                    Some(JsonValue::Bool(false)) => Truth::False,
                    _ => Truth::Unknown,
                })
            }
        }
    }
}

impl ArraySelector {
    fn canonical_text(&self) -> String {
        match self {
            Self::Wildcard => "*".to_string(),
            Self::Index(endpoint) => endpoint.canonical_text(),
            Self::Range(start, end) => {
                format!("{} to {}", start.canonical_text(), end.canonical_text())
            }
        }
    }

    fn select(
        &self,
        values: &[JsonValue],
        context: &EvalContext<'_>,
        output: &mut Vec<JsonValue>,
    ) -> Result<()> {
        match self {
            Self::Wildcard => output.extend_from_slice(values),
            Self::Index(endpoint) => {
                if let Some(index) = endpoint.resolve(values.len(), context)? {
                    output.push(values[index].clone());
                }
            }
            Self::Range(start, end) => {
                let (Some(start), Some(end)) = (
                    start.resolve(values.len(), context)?,
                    end.resolve(values.len(), context)?,
                ) else {
                    return Ok(());
                };
                if start <= end {
                    output.extend_from_slice(&values[start..=end]);
                }
            }
        }
        Ok(())
    }
}

impl Endpoint {
    fn canonical_text(&self) -> String {
        match self {
            Self::Last(0) => "last".to_string(),
            Self::Last(offset) if *offset < 0 => format!("last - {}", offset.unsigned_abs()),
            Self::Last(offset) => format!("last + {offset}"),
            Self::Absolute(index) => index.to_string(),
            Self::Variable(name, 1) => format!("${}", quoted(name)),
            Self::Variable(name, -1) => format!("-${}", quoted(name)),
            Self::Variable(name, multiplier) => {
                format!("({multiplier} * ${})", quoted(name))
            }
            Self::LastVariable(name, -1) => format!("last - ${}", quoted(name)),
            Self::LastVariable(name, 1) => format!("last + ${}", quoted(name)),
            Self::LastVariable(name, multiplier) => {
                format!("last + ({multiplier} * ${})", quoted(name))
            }
        }
    }

    fn resolve(&self, length: usize, context: &EvalContext<'_>) -> Result<Option<usize>> {
        let length =
            i64::try_from(length).map_err(|_| execution_error("jsonpath array is too large"))?;
        let index = match self {
            Self::Absolute(index) => *index,
            Self::Last(offset) => length - 1 + offset,
            Self::Variable(name, multiplier) => variable_integer(context, name)? * multiplier,
            Self::LastVariable(name, multiplier) => {
                length - 1 + variable_integer(context, name)? * multiplier
            }
        };
        Ok((index >= 0 && index < length).then_some(index as usize))
    }
}

impl BinaryOp {
    fn name(self) -> &'static str {
        match self {
            Self::Or => "||",
            Self::And => "&&",
            Self::Eq => "==",
            Self::NotEq => "!=",
            Self::Lt => "<",
            Self::LtEq => "<=",
            Self::Gt => ">",
            Self::GtEq => ">=",
            Self::Add => "+",
            Self::Subtract => "-",
            Self::Multiply => "*",
            Self::Divide => "/",
            Self::Modulo => "%",
            Self::StartsWith => "starts with",
        }
    }
}

impl Method {
    fn name(self) -> &'static str {
        match self {
            Self::Type => "type",
            Self::Size => "size",
            Self::Double => "double",
            Self::Ceiling => "ceiling",
            Self::Floor => "floor",
            Self::Abs => "abs",
            Self::KeyValue => "keyvalue",
            Self::BigInt => "bigint",
            Self::Boolean => "boolean",
            Self::Decimal => "decimal",
            Self::Integer => "integer",
            Self::Number => "number",
            Self::String => "string",
            Self::Date => "date",
            Self::Time => "time",
            Self::TimeTz => "time_tz",
            Self::Timestamp => "timestamp",
            Self::TimestampTz => "timestamp_tz",
            Self::DateTime => "datetime",
        }
    }
}

fn quoted(value: &str) -> String {
    serde_json::to_string(value).expect("JSON string serialization is infallible")
}

fn variable_integer(context: &EvalContext<'_>, name: &str) -> Result<i64> {
    context
        .variables
        .get(name)
        .and_then(JsonValue::as_i64)
        .ok_or_else(|| execution_error(format!("jsonpath variable {name} must be an integer")))
}

fn member_values(
    value: &JsonValue,
    name: &str,
    mode: Mode,
    output: &mut Vec<JsonValue>,
) -> Result<()> {
    match value {
        JsonValue::Object(object) => match object.get(name) {
            Some(value) => output.push(value.clone()),
            None if mode == Mode::Strict => {
                return Err(execution_error(format!(
                    "jsonpath member '{name}' was not found"
                )))
            }
            None => {}
        },
        JsonValue::Array(values) if mode == Mode::Lax => {
            for value in values {
                member_values(value, name, mode, output)?;
            }
        }
        _ if mode == Mode::Strict => {
            return Err(execution_error(
                "jsonpath member accessor can only be applied to an object",
            ))
        }
        _ => {}
    }
    Ok(())
}

fn descendants(
    value: &JsonValue,
    depth: usize,
    minimum: usize,
    maximum: Option<usize>,
    output: &mut Vec<JsonValue>,
) {
    if depth >= minimum && maximum.is_none_or(|maximum| depth <= maximum) {
        output.push(value.clone());
    }
    if maximum.is_some_and(|maximum| depth >= maximum) {
        return;
    }
    match value {
        JsonValue::Array(values) => {
            for value in values {
                descendants(value, depth + 1, minimum, maximum, output);
            }
        }
        JsonValue::Object(object) => {
            for value in object.values() {
                descendants(value, depth + 1, minimum, maximum, output);
            }
        }
        _ => {}
    }
}

fn truth_json(truth: Truth) -> JsonValue {
    match truth {
        Truth::True => JsonValue::Bool(true),
        Truth::False => JsonValue::Bool(false),
        Truth::Unknown => JsonValue::Null,
    }
}

fn compare_sequences(
    left: &[JsonValue],
    operator: BinaryOp,
    right: &[JsonValue],
    mode: Mode,
) -> Result<Truth> {
    let left = comparison_items(left, mode);
    let right = comparison_items(right, mode);
    let mut unknown = false;
    for left in &left {
        for right in &right {
            match compare_json_values(left, right) {
                Some(ordering) => {
                    let matched = match operator {
                        BinaryOp::Eq => ordering == Ordering::Equal,
                        BinaryOp::NotEq => ordering != Ordering::Equal,
                        BinaryOp::Lt => ordering == Ordering::Less,
                        BinaryOp::LtEq => ordering != Ordering::Greater,
                        BinaryOp::Gt => ordering == Ordering::Greater,
                        BinaryOp::GtEq => ordering != Ordering::Less,
                        _ => unreachable!(),
                    };
                    if matched {
                        return Ok(Truth::True);
                    }
                }
                None => unknown = true,
            }
        }
    }
    Ok(if unknown {
        Truth::Unknown
    } else {
        Truth::False
    })
}

fn compare_datetime_expressions(
    left: &Expr,
    operator: BinaryOp,
    right: &Expr,
    context: &EvalContext<'_>,
) -> Result<Option<Truth>> {
    let (Expr::Method(left, left_method, _), Expr::Method(right, right_method, _)) = (left, right)
    else {
        return Ok(None);
    };
    if !is_datetime_method(*left_method) || !is_datetime_method(*right_method) {
        return Ok(None);
    }
    let left = left.evaluate(context)?;
    let right = right.evaluate(context)?;
    let mut comparable = false;
    for left in &left {
        for right in &right {
            let (Some(left), Some(right)) = (left.as_str(), right.as_str()) else {
                continue;
            };
            let Some(ordering) = datetime_key(*left_method, left, context.timezone_aware)?
                .partial_cmp(&datetime_key(*right_method, right, context.timezone_aware)?)
            else {
                continue;
            };
            comparable = true;
            let matched = match operator {
                BinaryOp::Eq => ordering == Ordering::Equal,
                BinaryOp::NotEq => ordering != Ordering::Equal,
                BinaryOp::Lt => ordering == Ordering::Less,
                BinaryOp::LtEq => ordering != Ordering::Greater,
                BinaryOp::Gt => ordering == Ordering::Greater,
                BinaryOp::GtEq => ordering != Ordering::Less,
                _ => unreachable!(),
            };
            if matched {
                return Ok(Some(Truth::True));
            }
        }
    }
    Ok(Some(if comparable {
        Truth::False
    } else {
        Truth::Unknown
    }))
}

fn is_datetime_method(method: Method) -> bool {
    matches!(
        method,
        Method::Date
            | Method::Time
            | Method::TimeTz
            | Method::Timestamp
            | Method::TimestampTz
            | Method::DateTime
    )
}

fn datetime_key(method: Method, value: &str, timezone_aware: bool) -> Result<i128> {
    const DAY_MICROS: i128 = 86_400_000_000;
    if matches!(method, Method::Date) || (matches!(method, Method::DateTime) && value.len() == 10) {
        let date = NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .map_err(|_| execution_error("invalid jsonpath date value"))?;
        return Ok(i128::from(date.to_epoch_days()) * DAY_MICROS);
    }
    if matches!(method, Method::Time | Method::TimeTz)
        || (matches!(method, Method::DateTime) && !value.contains(['-', 'T', ' ']))
    {
        let time = value
            .split(['+', '-', 'Z'])
            .next()
            .and_then(|value| NaiveTime::parse_from_str(value, "%H:%M:%S%.f").ok())
            .ok_or_else(|| execution_error("invalid jsonpath time value"))?;
        let mut micros = i128::from(
            time.signed_duration_since(NaiveTime::MIN)
                .num_microseconds()
                .unwrap_or_default(),
        );
        if matches!(method, Method::TimeTz) {
            micros -= i128::from(timezone_offset_seconds(value)?) * 1_000_000;
        }
        return Ok(micros);
    }
    let normalized = normalize_datetime_offset(value);
    if let Ok(value) = DateTime::parse_from_rfc3339(&normalized) {
        return Ok(i128::from(value.timestamp_micros()));
    }
    let value = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f"))
        .map_err(|_| execution_error("invalid jsonpath timestamp value"))?;
    if !timezone_aware && matches!(method, Method::TimeTz | Method::TimestampTz) {
        return Err(execution_error(
            "timezone-dependent jsonpath comparison requires a _tz function",
        ));
    }
    Ok(i128::from(value.and_utc().timestamp_micros()))
}

fn timezone_offset_seconds(value: &str) -> Result<i32> {
    if value.ends_with('Z') {
        return Ok(0);
    }
    let offset = value
        .rfind(['+', '-'])
        .filter(|offset| *offset > value.find(':').unwrap_or(0))
        .ok_or_else(|| execution_error("jsonpath time zone offset is missing"))?;
    let negative = value.as_bytes()[offset] == b'-';
    let raw = &value[offset + 1..];
    let (hours, minutes) = raw.split_once(':').unwrap_or((raw, "0"));
    let seconds = hours
        .parse::<i32>()
        .ok()
        .zip(minutes.parse::<i32>().ok())
        .filter(|(hours, minutes)| *hours <= 15 && *minutes < 60)
        .map(|(hours, minutes)| hours * 3600 + minutes * 60)
        .ok_or_else(|| execution_error("invalid jsonpath time zone offset"))?;
    Ok(if negative { -seconds } else { seconds })
}

fn normalize_datetime_offset(value: &str) -> String {
    let mut value = value.replace(' ', "T");
    let offset = value.rfind(['+', '-']).filter(|offset| *offset > 9);
    if let Some(offset) = offset {
        if value.len() == offset + 3 {
            value.push_str(":00");
        }
    }
    value
}

fn string_predicate_sequences(
    left: &[JsonValue],
    right: &[JsonValue],
    predicate: impl Fn(&str, &str) -> bool,
) -> Truth {
    let mut comparable = false;
    for left in left {
        for right in right {
            if let (Some(left), Some(right)) = (left.as_str(), right.as_str()) {
                comparable = true;
                if predicate(left, right) {
                    return Truth::True;
                }
            }
        }
    }
    if comparable {
        Truth::False
    } else {
        Truth::Unknown
    }
}

fn comparison_items(values: &[JsonValue], mode: Mode) -> Vec<&JsonValue> {
    if mode == Mode::Strict {
        return values.iter().collect();
    }
    values
        .iter()
        .flat_map(|value| match value {
            JsonValue::Array(values) => values.iter().collect::<Vec<_>>(),
            value => vec![value],
        })
        .collect()
}

fn compare_json_values(left: &JsonValue, right: &JsonValue) -> Option<Ordering> {
    match (left, right) {
        (JsonValue::Null, JsonValue::Null) => Some(Ordering::Equal),
        (JsonValue::Bool(left), JsonValue::Bool(right)) => Some(left.cmp(right)),
        (JsonValue::String(left), JsonValue::String(right)) => Some(left.cmp(right)),
        (JsonValue::Number(left), JsonValue::Number(right)) => crate::pg_typed_compare(
            "numeric",
            &SqlValue::String(left.to_string()),
            &SqlValue::String(right.to_string()),
        )
        .ok(),
        (JsonValue::Array(left), JsonValue::Array(right)) => Some(left.len().cmp(&right.len())),
        (JsonValue::Object(left), JsonValue::Object(right)) => Some(left.len().cmp(&right.len())),
        _ => None,
    }
}

fn unary_number(operator: UnaryOp, value: JsonValue) -> Result<JsonValue> {
    let JsonValue::Number(number) = value else {
        return Err(execution_error(
            "jsonpath numeric operation requires a number",
        ));
    };
    let mut numeric = crate::PgNumeric::from_decimal_text(&number.to_string())
        .map_err(|_| execution_error("invalid jsonpath numeric value"))?;
    if matches!(operator, UnaryOp::Minus) {
        if let crate::PgNumeric::Finite {
            negative,
            coefficient,
            ..
        } = &mut numeric
        {
            *negative = coefficient != "0" && !*negative;
        }
    }
    numeric_json_value(numeric.to_decimal_text())
}

fn arithmetic_sequences(
    left: &[JsonValue],
    operator: BinaryOp,
    right: &[JsonValue],
) -> Result<Vec<JsonValue>> {
    if left.len() != 1 || right.len() != 1 {
        return Err(execution_error(
            "jsonpath arithmetic operands must contain one numeric item",
        ));
    }
    let (JsonValue::Number(left), JsonValue::Number(right)) = (&left[0], &right[0]) else {
        return Err(execution_error(
            "jsonpath arithmetic operands must be numeric",
        ));
    };
    let operator = match operator {
        BinaryOp::Add => sqlparser::ast::BinaryOperator::Plus,
        BinaryOp::Subtract => sqlparser::ast::BinaryOperator::Minus,
        BinaryOp::Multiply => sqlparser::ast::BinaryOperator::Multiply,
        BinaryOp::Divide => sqlparser::ast::BinaryOperator::Divide,
        BinaryOp::Modulo => sqlparser::ast::BinaryOperator::Modulo,
        _ => unreachable!(),
    };
    let result = crate::eval_pg_numeric_arithmetic(
        SqlValue::String(left.to_string()),
        &operator,
        SqlValue::String(right.to_string()),
    )?;
    Ok(vec![numeric_json_value(result.to_cell())?])
}

fn numeric_json_value(value: String) -> Result<JsonValue> {
    value
        .parse::<Number>()
        .map(JsonValue::Number)
        .map_err(|_| execution_error("invalid jsonpath numeric result"))
}

fn finite_json_number(value: f64) -> Result<JsonValue> {
    if value.fract() == 0.0 && value >= i64::MIN as f64 && value <= i64::MAX as f64 {
        return Ok(JsonValue::Number(Number::from(value as i64)));
    }
    Number::from_f64(value)
        .map(JsonValue::Number)
        .ok_or_else(|| execution_error("jsonpath numeric result is not finite"))
}

fn exact_json_number(value: &str) -> Result<JsonValue> {
    let numeric = crate::PgNumeric::from_decimal_text(value)
        .map_err(|_| execution_error("jsonpath numeric conversion requires a numeric string"))?;
    numeric_json_value(numeric.to_decimal_text())
}

fn increment_decimal_digits(value: &str) -> String {
    let mut digits = value.as_bytes().to_vec();
    for digit in digits.iter_mut().rev() {
        if *digit < b'9' {
            *digit += 1;
            return String::from_utf8(digits).expect("decimal digits are UTF-8");
        }
        *digit = b'0';
    }
    let mut incremented = String::with_capacity(digits.len() + 1);
    incremented.push('1');
    incremented.push_str(std::str::from_utf8(&digits).expect("decimal digits are UTF-8"));
    incremented
}

fn exact_numeric_method(value: &JsonValue, method: Method) -> Result<JsonValue> {
    let JsonValue::Number(value) = value else {
        return Err(execution_error(
            "jsonpath numeric operation requires a number",
        ));
    };
    let numeric = crate::PgNumeric::from_decimal_text(&value.to_string())
        .map_err(|_| execution_error("jsonpath numeric operation requires a number"))?;
    let crate::PgNumeric::Finite {
        negative,
        coefficient,
        display_scale,
    } = numeric
    else {
        return Err(execution_error("jsonpath numeric result is not finite"));
    };
    if matches!(method, Method::Abs) {
        return numeric_json_value(
            crate::PgNumeric::finite(false, coefficient, display_scale)
                .expect("an existing numeric remains valid without its sign")
                .to_decimal_text(),
        );
    }
    if display_scale <= 0 {
        return numeric_json_value(
            crate::PgNumeric::finite(negative, coefficient, display_scale)
                .expect("an existing numeric remains valid")
                .to_decimal_text(),
        );
    }

    let integer_len = coefficient.len().saturating_sub(display_scale as usize);
    let integer = if integer_len == 0 {
        "0"
    } else {
        &coefficient[..integer_len]
    };
    let has_fraction = coefficient[integer_len..]
        .bytes()
        .any(|digit| digit != b'0');
    let away_from_zero = has_fraction
        && matches!(
            (method, negative),
            (Method::Ceiling, false) | (Method::Floor, true)
        );
    let integer = if away_from_zero {
        increment_decimal_digits(integer)
    } else {
        integer.to_string()
    };
    numeric_json_value(
        crate::PgNumeric::finite(negative, integer, 0)
            .expect("integer digits form a valid numeric")
            .to_decimal_text(),
    )
}

fn apply_method(
    method: Method,
    arguments: &[Expr],
    value: JsonValue,
    context: &EvalContext<'_>,
    output: &mut Vec<JsonValue>,
) -> Result<()> {
    let result = match method {
        Method::Type => JsonValue::String(
            match value {
                JsonValue::Null => "null",
                JsonValue::Bool(_) => "boolean",
                JsonValue::Number(_) => "number",
                JsonValue::String(_) => "string",
                JsonValue::Array(_) => "array",
                JsonValue::Object(_) => "object",
            }
            .to_string(),
        ),
        Method::Size => JsonValue::Number(Number::from(match &value {
            JsonValue::Array(values) => values.len(),
            _ if context.mode == Mode::Lax => 1,
            _ => {
                return Err(execution_error(
                    "jsonpath size() method requires an array in strict mode",
                ))
            }
        })),
        Method::Double => match value {
            JsonValue::Number(_) => value,
            JsonValue::String(value) => {
                finite_json_number(value.parse::<f64>().map_err(|_| {
                    execution_error("jsonpath numeric conversion requires a numeric string")
                })?)?
            }
            _ => {
                return Err(execution_error(
                    "jsonpath value cannot be converted to a number",
                ))
            }
        },
        Method::Number => match value {
            JsonValue::Number(_) => value,
            JsonValue::String(value) => exact_json_number(&value)?,
            _ => {
                return Err(execution_error(
                    "jsonpath value cannot be converted to a number",
                ))
            }
        },
        Method::Decimal => {
            let text = match value {
                JsonValue::Number(value) => value.to_string(),
                JsonValue::String(value) => value,
                _ => {
                    return Err(execution_error(
                        "jsonpath value cannot be converted to a decimal",
                    ))
                }
            };
            let mut numeric = crate::PgNumeric::from_decimal_text(&text)
                .map_err(|_| execution_error("jsonpath decimal conversion failed"))?;
            if !arguments.is_empty() {
                let precision = method_integer_argument(&arguments[0], context)?;
                let scale = if arguments.len() == 2 {
                    method_integer_argument(&arguments[1], context)?
                } else {
                    0
                };
                numeric = numeric
                    .with_typmod(
                        u16::try_from(precision).map_err(|_| {
                            execution_error("jsonpath decimal precision is out of range")
                        })?,
                        i16::try_from(scale).map_err(|_| {
                            execution_error("jsonpath decimal scale is out of range")
                        })?,
                    )
                    .map_err(|_| execution_error("jsonpath decimal value is out of range"))?;
            }
            JsonValue::Number(
                numeric
                    .to_decimal_text()
                    .parse()
                    .map_err(|_| execution_error("jsonpath decimal conversion failed"))?,
            )
        }
        Method::Ceiling | Method::Floor | Method::Abs => exact_numeric_method(&value, method)?,
        Method::BigInt | Method::Integer => {
            let text = match value {
                JsonValue::Number(value) => value.to_string(),
                JsonValue::String(value) => value,
                _ => return Err(execution_error("jsonpath integer conversion failed")),
            };
            let number = text
                .parse::<i64>()
                .map_err(|_| execution_error("jsonpath integer conversion failed"))?;
            if matches!(method, Method::Integer) && i32::try_from(number).is_err() {
                return Err(execution_error("jsonpath integer value is out of range"));
            }
            JsonValue::Number(Number::from(number))
        }
        Method::Boolean => match value {
            JsonValue::Bool(_) => value,
            JsonValue::String(value) if value.eq_ignore_ascii_case("true") => JsonValue::Bool(true),
            JsonValue::String(value) if value.eq_ignore_ascii_case("false") => {
                JsonValue::Bool(false)
            }
            _ => return Err(execution_error("jsonpath boolean conversion failed")),
        },
        Method::String => JsonValue::String(match value {
            JsonValue::String(value) => value,
            value => value.to_string(),
        }),
        Method::KeyValue => {
            let JsonValue::Object(object) = value else {
                return Err(execution_error(
                    "jsonpath keyvalue() method requires an object",
                ));
            };
            let id = context.keyvalue_counter.get();
            context.keyvalue_counter.set(id.saturating_add(1));
            output.extend(
                object
                    .into_iter()
                    .map(|(key, value)| json!({"id": id, "key": key, "value": value})),
            );
            return Ok(());
        }
        Method::Date
        | Method::Time
        | Method::TimeTz
        | Method::Timestamp
        | Method::TimestampTz
        | Method::DateTime => {
            let JsonValue::String(value) = value else {
                return Err(execution_error(
                    "jsonpath datetime method requires a string",
                ));
            };
            validate_datetime_method(method, &value, context.timezone_aware)?;
            JsonValue::String(value)
        }
    };
    output.push(result);
    Ok(())
}

fn validate_datetime_method(method: Method, value: &str, timezone_aware: bool) -> Result<()> {
    let _ = timezone_aware;
    let valid_date = || {
        value.len() >= 10
            && value.as_bytes().get(4) == Some(&b'-')
            && value.as_bytes().get(7) == Some(&b'-')
            && value[..4].parse::<u16>().is_ok()
            && value[5..7].parse::<u8>().is_ok()
            && value[8..10].parse::<u8>().is_ok()
    };
    let valid_time = || value.contains(':');
    let valid = match method {
        Method::Date => valid_date() && value.len() == 10,
        Method::Time => valid_time() && !value.ends_with('Z') && !has_timezone_offset(value),
        Method::TimeTz => valid_time() && (value.ends_with('Z') || has_timezone_offset(value)),
        Method::Timestamp => valid_date() && value.contains('T') && !has_timezone_offset(value),
        Method::TimestampTz => {
            valid_date()
                && value.contains('T')
                && (value.ends_with('Z') || has_timezone_offset(value))
        }
        Method::DateTime => valid_date() || valid_time(),
        _ => true,
    };
    if !valid {
        return Err(execution_error(format!(
            "invalid datetime value for jsonpath method: {value}"
        )));
    }
    Ok(())
}

fn validate_method_argument_count(method: Method, actual: usize) -> Result<()> {
    let valid = match method {
        Method::Decimal => actual <= 2,
        Method::DateTime => actual <= 1,
        _ => actual == 0,
    };
    valid
        .then_some(())
        .ok_or_else(|| syntax_error("invalid number of jsonpath method arguments"))
}

fn method_integer_argument(expression: &Expr, context: &EvalContext<'_>) -> Result<i64> {
    let values = expression.evaluate(context)?;
    let [JsonValue::Number(value)] = values.as_slice() else {
        return Err(execution_error(
            "jsonpath method argument must be an integer",
        ));
    };
    value
        .as_i64()
        .ok_or_else(|| execution_error("jsonpath method argument must be an integer"))
}

fn has_timezone_offset(value: &str) -> bool {
    value
        .rfind(['+', '-'])
        .is_some_and(|offset| offset > value.find('T').unwrap_or(0))
}

fn jsonpath_args(
    name: &str,
    args: &[SqlValue],
) -> Result<Option<(JsonValue, JsonPath, Map<String, JsonValue>, bool)>> {
    if !(2..=4).contains(&args.len()) {
        return Err(SqlError::undefined_function(format!(
            "function {name} expects 2 to 4 arguments"
        )));
    }
    if args.iter().any(|value| matches!(value, SqlValue::Null)) {
        return Ok(None);
    }
    let target = match &args[0] {
        SqlValue::Json(value) => value.clone(),
        SqlValue::JsonText(value) => value.parsed().clone(),
        value => serde_json::from_str(&value.to_cell())
            .map_err(|_| SqlError::invalid_text_representation("jsonb", value.to_cell()))?,
    };
    let raw_path = match &args[1] {
        SqlValue::String(value) => value,
        value => {
            return Err(SqlError::invalid_text_representation(
                "jsonpath",
                value.to_cell(),
            ))
        }
    };
    let path = JsonPath::parse(raw_path)?;
    let variables = match args.get(2) {
        None => Map::new(),
        Some(SqlValue::Json(JsonValue::Object(object))) => object.clone(),
        Some(SqlValue::JsonText(value)) => value
            .parsed()
            .as_object()
            .cloned()
            .ok_or_else(|| execution_error("jsonpath vars argument must be an object"))?,
        Some(value) => serde_json::from_str::<JsonValue>(&value.to_cell())
            .ok()
            .and_then(|value| value.as_object().cloned())
            .ok_or_else(|| execution_error("jsonpath vars argument must be an object"))?,
    };
    let silent = match args.get(3) {
        None => false,
        Some(SqlValue::Bool(value)) => *value,
        Some(_) => {
            return Err(SqlError::undefined_function(format!(
                "function {name} silent argument must be boolean"
            )))
        }
    };
    Ok(Some((target, path, variables, silent)))
}

pub(crate) fn eval_jsonpath_function_value(
    name: &str,
    args: &[SqlValue],
) -> Result<Option<SqlValue>> {
    let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    if matches!(name, "jsonpath_in" | "jsonpath_out" | "jsonpath_send") {
        if args.len() != 1 {
            return Err(SqlError::undefined_function(format!(
                "function {name} expects one argument"
            )));
        }
        if matches!(args[0], SqlValue::Null) {
            return Ok(Some(SqlValue::Null));
        }
        let canonical = normalize_jsonpath(&args[0].to_cell())?;
        if name == "jsonpath_send" {
            let mut bytes = vec![1];
            bytes.extend_from_slice(canonical.as_bytes());
            return Ok(Some(SqlValue::String(crate::format_bytea_hex(&bytes))));
        }
        return Ok(Some(SqlValue::String(canonical)));
    }
    let timezone_aware = name.ends_with("_tz");
    let base_name = match name.strip_suffix("_tz").unwrap_or(name) {
        "jsonb_path_exists_opr" => "jsonb_path_exists",
        "jsonb_path_match_opr" => "jsonb_path_match",
        name => name,
    };
    if !matches!(
        base_name,
        "jsonb_path_exists"
            | "jsonb_path_match"
            | "jsonb_path_query_array"
            | "jsonb_path_query_first"
    ) {
        return Ok(None);
    }
    let Some((target, path, variables, silent)) = jsonpath_args(name, args)? else {
        return Ok(Some(SqlValue::Null));
    };
    let result = path.query(&target, &variables, timezone_aware);
    let values = match result {
        Ok(values) => values,
        Err(_) if silent => Vec::new(),
        Err(error) => return Err(error),
    };
    let value = match base_name {
        "jsonb_path_exists" => SqlValue::Bool(!values.is_empty()),
        "jsonb_path_match" => match values.as_slice() {
            [JsonValue::Bool(value)] => SqlValue::Bool(*value),
            [JsonValue::Null] | [] => SqlValue::Null,
            _ if silent => SqlValue::Null,
            _ => {
                return Err(execution_error(
                    "single boolean result is expected from jsonpath predicate",
                ))
            }
        },
        "jsonb_path_query_array" => SqlValue::Json(JsonValue::Array(values)),
        "jsonb_path_query_first" => values
            .into_iter()
            .next()
            .map(SqlValue::Json)
            .unwrap_or(SqlValue::Null),
        _ => unreachable!(),
    };
    Ok(Some(value))
}

pub(crate) fn eval_jsonpath_operator_value(
    target: SqlValue,
    path: SqlValue,
    predicate: bool,
) -> Result<SqlValue> {
    if matches!(target, SqlValue::Null) || matches!(path, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    eval_jsonpath_function_value(
        if predicate {
            "jsonb_path_match"
        } else {
            "jsonb_path_exists"
        },
        &[
            target,
            path,
            SqlValue::Json(json!({})),
            SqlValue::Bool(true),
        ],
    )?
    .ok_or_else(|| SqlError::Unsupported("jsonpath operator is unavailable".to_string()))
}

pub(crate) fn eval_jsonpath_query_values(name: &str, args: &[SqlValue]) -> Result<Vec<SqlValue>> {
    let timezone_aware = name
        .strip_prefix("pg_catalog.")
        .unwrap_or(name)
        .ends_with("_tz");
    let Some((target, path, variables, silent)) = jsonpath_args(name, args)? else {
        return Ok(Vec::new());
    };
    match path.query(&target, &variables, timezone_aware) {
        Ok(values) => Ok(values.into_iter().map(SqlValue::Json).collect()),
        Err(_) if silent => Ok(Vec::new()),
        Err(error) => Err(error),
    }
}

pub(crate) fn jsonpath_function_pg_type(name: &str) -> Option<&'static str> {
    let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    let name = name.strip_suffix("_tz").unwrap_or(name);
    match name {
        "jsonpath_in" => Some("jsonpath"),
        "jsonpath_out" => Some("cstring"),
        "jsonpath_send" => Some("bytea"),
        "jsonb_path_exists_opr" | "jsonb_path_match_opr" => Some("bool"),
        "jsonb_path_exists" | "jsonb_path_match" => Some("bool"),
        "jsonb_path_query_array" | "jsonb_path_query_first" | "jsonb_path_query" => Some("jsonb"),
        _ => None,
    }
}

pub(crate) fn normalize_jsonpath(input: &str) -> Result<String> {
    JsonPath::parse(input).map(|path| path.canonical_text())
}

pub(crate) fn jsonpath_index_terms(value: &SqlValue) -> Result<Vec<String>> {
    let SqlValue::String(path) = value else {
        return Ok(Vec::new());
    };
    Ok(JsonPath::parse(path)?
        .required_member_keys()
        .into_iter()
        .map(|key| crate::jsonb_index_key_token(&key))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(target: JsonValue, path: &str) -> Vec<JsonValue> {
        JsonPath::parse(path)
            .unwrap()
            .query(&target, &Map::new(), false)
            .unwrap()
    }

    #[test]
    fn members_arrays_filters_and_variables_execute() {
        let target = json!({"a": [1, 2, 3]});
        let path = JsonPath::parse("$.a[*] ? (@ >= $min)").unwrap();
        let variables = Map::from_iter([("min".to_string(), json!(2))]);
        assert_eq!(
            path.query(&target, &variables, false).unwrap(),
            vec![json!(2), json!(3)]
        );
    }

    #[test]
    fn array_ranges_last_and_methods_execute() {
        assert_eq!(
            query(json!({"a": [10, 20, 30]}), "$.a[0 to last - 1]"),
            vec![json!(10), json!(20)]
        );
        assert_eq!(query(json!({"a": [1, 2]}), "$.a.size()"), vec![json!(2)]);
        assert_eq!(query(json!({"a": 1.2}), "$.a.ceiling()"), vec![json!(2)]);
    }

    #[test]
    fn exact_numeric_methods_do_not_round_through_f64() {
        let target: JsonValue = serde_json::from_str(
            r#"{"positive":9007199254740993.1,"negative":-9007199254740993.1,"text":"9007199254740993.1"}"#,
        )
        .unwrap();
        assert_eq!(
            query(target.clone(), "$.positive.ceiling()")[0].to_string(),
            "9007199254740994"
        );
        assert_eq!(
            query(target.clone(), "$.negative.floor()")[0].to_string(),
            "-9007199254740994"
        );
        assert_eq!(
            query(target.clone(), "$.negative.abs()")[0].to_string(),
            "9007199254740993.1"
        );
        assert_eq!(
            query(target, "$.text.number()")[0].to_string(),
            "9007199254740993.1"
        );
    }

    #[test]
    fn strict_mode_reports_structural_errors_while_lax_suppresses_them() {
        assert!(query(json!({}), "$.missing").is_empty());
        assert!(JsonPath::parse("strict $.missing")
            .unwrap()
            .query(&json!({}), &Map::new(), false)
            .is_err());
    }

    #[test]
    fn invalid_paths_are_rejected_at_input() {
        assert!(JsonPath::parse("$.a[").is_err());
        assert!(JsonPath::parse("$.unknown()").is_err());
        assert!(JsonPath::parse("$.a = 1").is_err());
    }
}
