//! COPY options as PostgreSQL 18 reads and checks them (`copy_opt_list`,
//! `copy_generic_opt_list` and `ProcessCopyOptions`), and the text and CSV
//! formats they shape: DELIMITER, NULL, QUOTE, ESCAPE, FORCE_QUOTE,
//! FORCE_NOT_NULL, FORCE_NULL, HEADER [MATCH] and ENCODING (UTF8, SQL_ASCII,
//! LATIN1).
//!
//! Options whose effect BicDB does not implement are refused with 0A000
//! after PostgreSQL's own validation, before COPY mode starts: FREEZE (true),
//! DEFAULT, ON_ERROR IGNORE (and so REJECT_LIMIT), and the other encodings.
//! An option is never accepted and then ignored.
use super::*;
#[allow(unused_imports)]
use crate::*;
use sqlparser::tokenizer::{Token, Tokenizer};

/// One option as the grammar builds it (PostgreSQL's DefElem).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CopyDefElem {
    pub(crate) name: String,
    pub(crate) arg: Option<CopyArg>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CopyArg {
    Str(String),
    Num(String),
    Star,
    List(Vec<String>),
}

/// Columns named by FORCE_QUOTE / FORCE_NOT_NULL / FORCE_NULL.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) enum ForceColumns {
    #[default]
    None,
    All,
    Named(Vec<String>),
}

impl ForceColumns {
    fn is_set(&self) -> bool {
        !matches!(self, Self::None)
    }
}

/// The processed options. `*_flags` are filled per target column by
/// [`CopyOptions::resolve_columns`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CopyOptions {
    pub(crate) csv: bool,
    pub(crate) binary: bool,
    pub(crate) header: bool,
    pub(crate) header_match: bool,
    pub(crate) delimiter: u8,
    pub(crate) null: String,
    pub(crate) quote: u8,
    pub(crate) escape: u8,
    pub(crate) force_quote: ForceColumns,
    pub(crate) force_not_null: ForceColumns,
    pub(crate) force_null: ForceColumns,
    pub(crate) force_quote_flags: Vec<bool>,
    pub(crate) force_not_null_flags: Vec<bool>,
    pub(crate) force_null_flags: Vec<bool>,
    /// File encoding LATIN1 (ISO 8859-1); otherwise UTF8.
    pub(crate) latin1: bool,
}

impl Default for CopyOptions {
    fn default() -> Self {
        Self::for_format(false)
    }
}

impl CopyOptions {
    /// PostgreSQL's defaults for text (`csv` false) or CSV.
    pub(crate) fn for_format(csv: bool) -> Self {
        Self {
            csv,
            binary: false,
            header: false,
            header_match: false,
            delimiter: if csv { b',' } else { b'\t' },
            null: if csv {
                String::new()
            } else {
                "\\N".to_string()
            },
            quote: b'"',
            escape: b'"',
            force_quote: ForceColumns::None,
            force_not_null: ForceColumns::None,
            force_null: ForceColumns::None,
            force_quote_flags: Vec::new(),
            force_not_null_flags: Vec::new(),
            force_null_flags: Vec::new(),
            latin1: false,
        }
    }

    /// Resolve the FORCE_* column lists against the columns COPY reads or
    /// writes, with PostgreSQL's errors.
    pub(crate) fn resolve_columns(
        &mut self,
        targets: &[String],
        all: &[String],
        relation: Option<&str>,
    ) -> Result<()> {
        let resolve = |force: &ForceColumns, option: &str| -> Result<Vec<bool>> {
            Ok(match force {
                ForceColumns::None => vec![false; targets.len()],
                ForceColumns::All => vec![true; targets.len()],
                ForceColumns::Named(names) => {
                    let mut flags = vec![false; targets.len()];
                    for name in names {
                        match targets.iter().position(|target| target == name) {
                            Some(index) => flags[index] = true,
                            None if !all.contains(name) => {
                                return Err(coded(
                                    "42703",
                                    match relation {
                                        Some(relation) => format!(
                                            "column \"{name}\" of relation \"{}\" does not exist",
                                            relation.rsplit('.').next().unwrap_or(relation)
                                        ),
                                        None => format!("column \"{name}\" does not exist"),
                                    },
                                ))
                            }
                            None => {
                                return Err(coded(
                                    "42P10",
                                    format!("{option} column \"{name}\" not referenced by COPY"),
                                ))
                            }
                        }
                    }
                    flags
                }
            })
        };
        self.force_quote_flags = resolve(&self.force_quote, "FORCE_QUOTE")?;
        self.force_not_null_flags = resolve(&self.force_not_null, "FORCE_NOT_NULL")?;
        self.force_null_flags = resolve(&self.force_null, "FORCE_NULL")?;
        Ok(())
    }
}

fn coded(sqlstate: &'static str, message: impl Into<String>) -> PgWireError {
    PgWireError::Sql(SqlError::coded_public(sqlstate, message, None, None))
}

fn syntax_error_at(token: Option<&Token>) -> PgWireError {
    coded(
        "42601",
        match token {
            Some(token) => format!("syntax error at or near \"{token}\""),
            None => "syntax error at end of input".to_string(),
        },
    )
}

/// The option text after the COPY endpoint (the legacy list, or `WITH`
/// followed by either form), as DefElems.
pub(crate) fn parse_copy_option_list(text: &str) -> Result<Vec<CopyDefElem>> {
    let tokens = Tokenizer::new(&PostgreSqlDialect {}, text)
        .tokenize()
        .map_err(|error| coded("42601", error.to_string()))?
        .into_iter()
        .filter(|token| !matches!(token, Token::Whitespace(_) | Token::SemiColon))
        .collect::<Vec<_>>();
    let mut parser = OptionParser { tokens, pos: 0 };
    let mut defs = Vec::new();
    // `[USING] DELIMITERS 'x'` (before WITH, very old syntax).
    if parser.peek_word("using") && parser.peek_word_at(1, "delimiters") {
        parser.pos += 1;
    }
    if parser.peek_word("delimiters") {
        parser.pos += 1;
        defs.push(CopyDefElem {
            name: "delimiter".into(),
            arg: Some(CopyArg::Str(parser.string()?)),
        });
    }
    if parser.peek_word("with") {
        parser.pos += 1;
    }
    if parser.peek() == Some(&Token::LParen) {
        parser.pos += 1;
        loop {
            defs.push(parser.generic_elem()?);
            match parser.next() {
                Some(Token::Comma) => continue,
                Some(Token::RParen) => break,
                other => return Err(syntax_error_at(other.as_ref())),
            }
        }
        if let Some(token) = parser.peek() {
            return Err(syntax_error_at(Some(token)));
        }
        return Ok(defs);
    }
    while parser.peek().is_some() {
        defs.push(parser.legacy_item()?);
    }
    Ok(defs)
}

struct OptionParser {
    tokens: Vec<Token>,
    pos: usize,
}

impl OptionParser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.pos).cloned();
        self.pos += 1;
        token
    }

    fn peek_word_at(&self, offset: usize, word: &str) -> bool {
        matches!(self.tokens.get(self.pos + offset),
            Some(Token::Word(w)) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(word))
    }

    fn peek_word(&self, word: &str) -> bool {
        self.peek_word_at(0, word)
    }

    fn expect_word(&mut self, word: &str) -> Result<()> {
        if self.peek_word(word) {
            self.pos += 1;
            Ok(())
        } else {
            Err(syntax_error_at(self.peek()))
        }
    }

    /// A string constant (`Sconst`).
    fn string(&mut self) -> Result<String> {
        match self.next() {
            Some(Token::SingleQuotedString(value))
            | Some(Token::EscapedStringLiteral(value))
            | Some(Token::NationalStringLiteral(value)) => Ok(value),
            Some(Token::DollarQuotedString(value)) => Ok(value.value),
            other => Err(syntax_error_at(other.as_ref())),
        }
    }

    /// `opt_as Sconst`.
    fn as_string(&mut self) -> Result<String> {
        if self.peek_word("as") {
            self.pos += 1;
        }
        self.string()
    }

    /// An identifier as the grammar folds it.
    fn identifier(&mut self) -> Result<String> {
        match self.next() {
            Some(Token::Word(word)) => Ok(if word.quote_style.is_some() {
                word.value
            } else {
                word.value.to_lowercase()
            }),
            other => Err(syntax_error_at(other.as_ref())),
        }
    }

    /// `columnList` (unparenthesized, comma separated) or `'*'`.
    fn force_columns(&mut self) -> Result<CopyArg> {
        if self.peek() == Some(&Token::Mul) {
            self.pos += 1;
            return Ok(CopyArg::Star);
        }
        let mut columns = vec![self.identifier()?];
        while self.peek() == Some(&Token::Comma) {
            self.pos += 1;
            columns.push(self.identifier()?);
        }
        Ok(CopyArg::List(columns))
    }

    fn legacy_item(&mut self) -> Result<CopyDefElem> {
        let def = |name: &str, arg: CopyArg| CopyDefElem {
            name: name.to_string(),
            arg: Some(arg),
        };
        let Some(Token::Word(word)) = self.peek().cloned() else {
            return Err(syntax_error_at(self.peek()));
        };
        if word.quote_style.is_some() {
            return Err(syntax_error_at(self.peek()));
        }
        self.pos += 1;
        Ok(match word.value.to_ascii_lowercase().as_str() {
            "binary" => def("format", CopyArg::Str("binary".into())),
            "freeze" => def("freeze", CopyArg::Str("true".into())),
            "delimiter" => def("delimiter", CopyArg::Str(self.as_string()?)),
            "null" => def("null", CopyArg::Str(self.as_string()?)),
            "csv" => def("format", CopyArg::Str("csv".into())),
            "header" => def("header", CopyArg::Str("true".into())),
            "quote" => def("quote", CopyArg::Str(self.as_string()?)),
            "escape" => def("escape", CopyArg::Str(self.as_string()?)),
            "encoding" => def("encoding", CopyArg::Str(self.string()?)),
            "force" => {
                if self.peek_word("quote") {
                    self.pos += 1;
                    def("force_quote", self.force_columns()?)
                } else if self.peek_word("not") {
                    self.pos += 1;
                    self.expect_word("null")?;
                    def("force_not_null", self.force_columns()?)
                } else if self.peek_word("null") {
                    self.pos += 1;
                    def("force_null", self.force_columns()?)
                } else {
                    return Err(syntax_error_at(self.peek()));
                }
            }
            _ => {
                self.pos -= 1;
                return Err(syntax_error_at(self.peek()));
            }
        })
    }

    /// `ColLabel copy_generic_opt_arg`.
    fn generic_elem(&mut self) -> Result<CopyDefElem> {
        let name = self.identifier()?;
        let arg = match self.peek().cloned() {
            None | Some(Token::Comma) | Some(Token::RParen) => None,
            Some(Token::Mul) => {
                self.pos += 1;
                Some(CopyArg::Star)
            }
            Some(Token::Number(value, _)) => {
                self.pos += 1;
                Some(CopyArg::Num(value))
            }
            Some(Token::Minus) | Some(Token::Plus) => {
                let sign = if self.next() == Some(Token::Minus) {
                    "-"
                } else {
                    ""
                };
                match self.next() {
                    Some(Token::Number(value, _)) => Some(CopyArg::Num(format!("{sign}{value}"))),
                    other => return Err(syntax_error_at(other.as_ref())),
                }
            }
            Some(Token::LParen) => {
                self.pos += 1;
                let mut items = Vec::new();
                loop {
                    items.push(match self.next() {
                        Some(Token::Word(word)) if word.quote_style.is_none() => {
                            word.value.to_lowercase()
                        }
                        Some(Token::Word(word)) => word.value,
                        Some(Token::SingleQuotedString(value)) => value,
                        other => return Err(syntax_error_at(other.as_ref())),
                    });
                    match self.next() {
                        Some(Token::Comma) => continue,
                        Some(Token::RParen) => break,
                        other => return Err(syntax_error_at(other.as_ref())),
                    }
                }
                Some(CopyArg::List(items))
            }
            Some(Token::Word(_)) => Some(CopyArg::Str(self.identifier()?)),
            Some(
                Token::SingleQuotedString(_)
                | Token::EscapedStringLiteral(_)
                | Token::NationalStringLiteral(_)
                | Token::DollarQuotedString(_),
            ) => Some(CopyArg::Str(self.string()?)),
            other => return Err(syntax_error_at(other.as_ref())),
        };
        Ok(CopyDefElem { name, arg })
    }
}

/// PostgreSQL's `defGetString`.
fn def_string(def: &CopyDefElem) -> Result<String> {
    match &def.arg {
        None => Err(coded("42601", format!("{} requires a parameter", def.name))),
        Some(CopyArg::Str(value) | CopyArg::Num(value)) => Ok(value.clone()),
        Some(CopyArg::Star) => Ok("*".to_string()),
        Some(CopyArg::List(items)) => Ok(items.join(".")),
    }
}

/// PostgreSQL's `defGetBoolean`.
fn def_boolean(def: &CopyDefElem) -> Result<bool> {
    let error = || coded("42601", format!("{} requires a Boolean value", def.name));
    match &def.arg {
        None => Ok(true),
        Some(CopyArg::Num(value)) => match value.as_str() {
            "0" => Ok(false),
            "1" => Ok(true),
            _ => Err(error()),
        },
        Some(CopyArg::Str(value)) => match value.to_ascii_lowercase().as_str() {
            "true" | "on" => Ok(true),
            "false" | "off" => Ok(false),
            _ => Err(error()),
        },
        _ => Err(error()),
    }
}

fn conflicting() -> PgWireError {
    coded("42601", "conflicting or redundant options")
}

/// Encodings by PostgreSQL's normalized name (lowercase, alphanumerics
/// only), as `pg_char_to_encoding` knows them: `Some(true)` UTF8,
/// `Some(false)` LATIN1, `None` another valid encoding.
fn encoding_kind(name: &str) -> std::result::Result<Option<bool>, ()> {
    let clean: String = name
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .map(|ch| ch.to_ascii_lowercase())
        .collect();
    match clean.as_str() {
        // SQL_ASCII is never converted to or from, so it reads and writes
        // the server's UTF-8 as UTF8 does.
        "utf8" | "unicode" | "sqlascii" => Ok(Some(true)),
        "latin1" | "iso88591" => Ok(Some(false)),
        "abc" | "alt" | "big5" | "euccn" | "eucjis2004" | "eucjp" | "euckr" | "euctw"
        | "gb18030" | "gbk" | "iso88592" | "iso88593" | "iso88594" | "iso88595" | "iso88596"
        | "iso88597" | "iso88598" | "iso88599" | "iso885910" | "iso885913" | "iso885914"
        | "iso885915" | "iso885916" | "johab" | "koi8" | "koi8r" | "koi8u" | "latin2"
        | "latin3" | "latin4" | "latin5" | "latin6" | "latin7" | "latin8" | "latin9"
        | "latin10" | "mskanji" | "muleinternal" | "shiftjis" | "shiftjis2004" | "sjis"
        | "tcvn" | "tcvn5712" | "uhc" | "vscii" | "win" | "win1250" | "win1251" | "win1252"
        | "win1253" | "win1254" | "win1255" | "win1256" | "win1257" | "win1258" | "win866"
        | "win874" | "win932" | "win936" | "win949" | "win950" | "windows1250" | "windows1251"
        | "windows1252" | "windows1253" | "windows1254" | "windows1255" | "windows1256"
        | "windows1257" | "windows1258" | "windows866" | "windows874" | "windows932"
        | "windows936" | "windows949" | "windows950" => Ok(None),
        _ => Err(()),
    }
}

/// PostgreSQL's `ProcessCopyOptions`, then BicDB's refusals of what it does
/// not implement.
pub(crate) fn process_copy_options(defs: &[CopyDefElem], is_from: bool) -> Result<CopyOptions> {
    let mut format_specified = false;
    let mut csv = false;
    let mut binary = false;
    let mut freeze: Option<bool> = None;
    let mut delimiter: Option<String> = None;
    let mut null: Option<String> = None;
    let mut default_print: Option<String> = None;
    let mut header: Option<(bool, bool)> = None;
    let mut quote: Option<String> = None;
    let mut escape: Option<String> = None;
    let mut force_quote = ForceColumns::None;
    let mut force_not_null = ForceColumns::None;
    let mut force_null = ForceColumns::None;
    let mut encoding: Option<Option<bool>> = None;
    let mut on_error_ignore: Option<bool> = None;
    let mut log_verbosity_specified = false;
    let mut reject_limit: Option<i64> = None;

    let force_list = |def: &CopyDefElem| -> Result<ForceColumns> {
        match &def.arg {
            Some(CopyArg::Star) => Ok(ForceColumns::All),
            Some(CopyArg::List(columns)) => Ok(ForceColumns::Named(columns.clone())),
            _ => Err(coded(
                "22023",
                format!(
                    "argument to option \"{}\" must be a list of column names",
                    def.name
                ),
            )),
        }
    };

    for def in defs {
        match def.name.as_str() {
            "format" => {
                let value = def_string(def)?;
                if format_specified {
                    return Err(conflicting());
                }
                format_specified = true;
                match value.as_str() {
                    "text" => {}
                    "csv" => csv = true,
                    "binary" => binary = true,
                    _ => {
                        return Err(coded(
                            "22023",
                            format!("COPY format \"{value}\" not recognized"),
                        ))
                    }
                }
            }
            "freeze" => {
                if freeze.is_some() {
                    return Err(conflicting());
                }
                freeze = Some(def_boolean(def)?);
            }
            "delimiter" => {
                if delimiter.is_some() {
                    return Err(conflicting());
                }
                delimiter = Some(def_string(def)?);
            }
            "null" => {
                if null.is_some() {
                    return Err(conflicting());
                }
                null = Some(def_string(def)?);
            }
            "default" => {
                if default_print.is_some() {
                    return Err(conflicting());
                }
                default_print = Some(def_string(def)?);
            }
            "header" => {
                if header.is_some() {
                    return Err(conflicting());
                }
                header = Some(header_choice(def, is_from)?);
            }
            "quote" => {
                if quote.is_some() {
                    return Err(conflicting());
                }
                quote = Some(def_string(def)?);
            }
            "escape" => {
                if escape.is_some() {
                    return Err(conflicting());
                }
                escape = Some(def_string(def)?);
            }
            "force_quote" => {
                if force_quote.is_set() {
                    return Err(conflicting());
                }
                force_quote = force_list(def)?;
            }
            "force_not_null" => {
                if force_not_null.is_set() {
                    return Err(conflicting());
                }
                force_not_null = force_list(def)?;
            }
            "force_null" => {
                if force_null.is_set() {
                    return Err(conflicting());
                }
                force_null = force_list(def)?;
            }
            "encoding" => {
                if encoding.is_some() {
                    return Err(conflicting());
                }
                let name = def_string(def)?;
                encoding = Some(encoding_kind(&name).map_err(|()| {
                    coded(
                        "22023",
                        format!(
                            "argument to option \"{}\" must be a valid encoding name",
                            def.name
                        ),
                    )
                })?);
            }
            "on_error" => {
                if on_error_ignore.is_some() {
                    return Err(conflicting());
                }
                let value = def_string(def)?;
                if !is_from {
                    return Err(coded("22023", "COPY ON_ERROR cannot be used with COPY TO"));
                }
                on_error_ignore = Some(match value.to_ascii_lowercase().as_str() {
                    "stop" => false,
                    "ignore" => true,
                    _ => {
                        return Err(coded(
                            "22023",
                            format!("COPY ON_ERROR \"{value}\" not recognized"),
                        ))
                    }
                });
            }
            "log_verbosity" => {
                if log_verbosity_specified {
                    return Err(conflicting());
                }
                log_verbosity_specified = true;
                let value = def_string(def)?;
                if !matches!(
                    value.to_ascii_lowercase().as_str(),
                    "silent" | "default" | "verbose"
                ) {
                    return Err(coded(
                        "22023",
                        format!("COPY LOG_VERBOSITY \"{value}\" not recognized"),
                    ));
                }
            }
            "reject_limit" => {
                if reject_limit.is_some() {
                    return Err(conflicting());
                }
                let value = match &def.arg {
                    None => {
                        return Err(coded(
                            "42601",
                            format!("{} requires a numeric value", def.name),
                        ))
                    }
                    Some(CopyArg::Num(value) | CopyArg::Str(value)) => value.clone(),
                    _ => {
                        return Err(coded(
                            "42601",
                            format!("{} requires a numeric value", def.name),
                        ))
                    }
                };
                let limit = value.parse::<i64>().map_err(|_| {
                    coded(
                        "22P02",
                        format!("invalid input syntax for type bigint: \"{value}\""),
                    )
                })?;
                if limit <= 0 {
                    return Err(coded(
                        "22023",
                        format!("REJECT_LIMIT ({limit}) must be greater than zero"),
                    ));
                }
                reject_limit = Some(limit);
            }
            other => return Err(coded("42601", format!("option \"{other}\" not recognized"))),
        }
    }

    let unsupported = |message: &str| coded("0A000", message.to_string());
    if binary && delimiter.is_some() {
        return Err(coded("42601", "cannot specify DELIMITER in BINARY mode"));
    }
    if binary && null.is_some() {
        return Err(coded("42601", "cannot specify NULL in BINARY mode"));
    }
    if binary && default_print.is_some() {
        return Err(coded("42601", "cannot specify DEFAULT in BINARY mode"));
    }
    let delimiter = delimiter.unwrap_or_else(|| if csv { "," } else { "\t" }.to_string());
    let null = null.unwrap_or_else(|| if csv { "" } else { "\\N" }.to_string());
    let (quote, escape) = if csv {
        let quote = quote.unwrap_or_else(|| "\"".to_string());
        let escape = escape.unwrap_or_else(|| quote.clone());
        (Some(quote), Some(escape))
    } else {
        (quote, escape)
    };
    if delimiter.len() != 1 {
        return Err(unsupported(
            "COPY delimiter must be a single one-byte character",
        ));
    }
    if delimiter.contains(['\r', '\n']) {
        return Err(coded(
            "22023",
            "COPY delimiter cannot be newline or carriage return",
        ));
    }
    if null.contains(['\r', '\n']) {
        return Err(coded(
            "22023",
            "COPY null representation cannot use newline or carriage return",
        ));
    }
    if default_print
        .as_deref()
        .is_some_and(|default| default.contains(['\r', '\n']))
    {
        return Err(coded(
            "22023",
            "COPY default representation cannot use newline or carriage return",
        ));
    }
    let delim = delimiter.as_bytes()[0];
    if !csv && b"\\.abcdefghijklmnopqrstuvwxyz0123456789".contains(&delim) {
        return Err(coded(
            "22023",
            format!("COPY delimiter cannot be \"{delimiter}\""),
        ));
    }
    let header_on = header.is_some_and(|(on, _)| on);
    if binary && header_on {
        return Err(unsupported("cannot specify HEADER in BINARY mode"));
    }
    if !csv && quote.is_some() {
        return Err(unsupported("COPY QUOTE requires CSV mode"));
    }
    if let Some(quote) = &quote {
        if quote.len() != 1 {
            return Err(unsupported(
                "COPY quote must be a single one-byte character",
            ));
        }
        if quote.as_bytes()[0] == delim {
            return Err(coded("22023", "COPY delimiter and quote must be different"));
        }
    }
    if !csv && escape.is_some() {
        return Err(unsupported("COPY ESCAPE requires CSV mode"));
    }
    if escape.as_ref().is_some_and(|escape| escape.len() != 1) {
        return Err(unsupported(
            "COPY escape must be a single one-byte character",
        ));
    }
    if !csv && force_quote.is_set() {
        return Err(unsupported("COPY FORCE_QUOTE requires CSV mode"));
    }
    if force_quote.is_set() && is_from {
        return Err(unsupported(
            "COPY FORCE_QUOTE cannot be used with COPY FROM",
        ));
    }
    if !csv && force_not_null.is_set() {
        return Err(unsupported("COPY FORCE_NOT_NULL requires CSV mode"));
    }
    if force_not_null.is_set() && !is_from {
        return Err(coded(
            "22023",
            "COPY FORCE_NOT_NULL cannot be used with COPY TO",
        ));
    }
    if !csv && force_null.is_set() {
        return Err(unsupported("COPY FORCE_NULL requires CSV mode"));
    }
    if force_null.is_set() && !is_from {
        return Err(coded(
            "22023",
            "COPY FORCE_NULL cannot be used with COPY TO",
        ));
    }
    if null.as_bytes().contains(&delim) {
        return Err(coded(
            "22023",
            "COPY delimiter character must not appear in the NULL specification",
        ));
    }
    if let Some(quote) = &quote {
        if csv && null.as_bytes().contains(&quote.as_bytes()[0]) {
            return Err(coded(
                "22023",
                "CSV quote character must not appear in the NULL specification",
            ));
        }
    }
    if freeze == Some(true) && !is_from {
        return Err(coded("22023", "COPY FREEZE cannot be used with COPY TO"));
    }
    if let Some(default) = &default_print {
        if !is_from {
            return Err(unsupported("COPY DEFAULT cannot be used with COPY TO"));
        }
        if default.as_bytes().contains(&delim) {
            return Err(unsupported(
                "COPY delimiter character must not appear in the DEFAULT specification",
            ));
        }
        if let Some(quote) = &quote {
            if csv && default.as_bytes().contains(&quote.as_bytes()[0]) {
                return Err(unsupported(
                    "CSV quote character must not appear in the DEFAULT specification",
                ));
            }
        }
        if *default == null {
            return Err(unsupported(
                "NULL specification and DEFAULT specification cannot be the same",
            ));
        }
    }
    let ignore = on_error_ignore == Some(true);
    if binary && ignore {
        return Err(coded(
            "42601",
            "only ON_ERROR STOP is allowed in BINARY mode",
        ));
    }
    if reject_limit.is_some() && !ignore {
        return Err(coded(
            "22023",
            "COPY REJECT_LIMIT requires ON_ERROR to be set to IGNORE",
        ));
    }

    // PostgreSQL's checks passed: refuse what BicDB does not implement.
    if freeze == Some(true) {
        return Err(unsupported("COPY FREEZE is not supported"));
    }
    if default_print.is_some() {
        return Err(unsupported("COPY DEFAULT is not supported"));
    }
    if ignore {
        return Err(unsupported("COPY ON_ERROR IGNORE is not supported"));
    }
    let latin1 = match encoding {
        None | Some(Some(true)) => false,
        Some(Some(false)) => true,
        Some(None) => {
            return Err(unsupported(
                "COPY ENCODING other than UTF8 and LATIN1 is not supported",
            ))
        }
    };

    let (header, header_match) = header.unwrap_or((false, false));
    Ok(CopyOptions {
        csv,
        binary,
        header,
        header_match,
        delimiter: delim,
        null,
        quote: quote.map_or(b'"', |quote| quote.as_bytes()[0]),
        escape: escape.map_or(b'"', |escape| escape.as_bytes()[0]),
        force_quote,
        force_not_null,
        force_null,
        force_quote_flags: Vec::new(),
        force_not_null_flags: Vec::new(),
        force_null_flags: Vec::new(),
        latin1,
    })
}

/// PostgreSQL's `defGetCopyHeaderChoice`: (header, match).
fn header_choice(def: &CopyDefElem, is_from: bool) -> Result<(bool, bool)> {
    match &def.arg {
        None => return Ok((true, false)),
        Some(CopyArg::Num(value)) => match value.as_str() {
            "0" => return Ok((false, false)),
            "1" => return Ok((true, false)),
            _ => {}
        },
        Some(_) => {
            let value = def_string(def)?;
            match value.to_ascii_lowercase().as_str() {
                "true" | "on" => return Ok((true, false)),
                "false" | "off" => return Ok((false, false)),
                "match" => {
                    if !is_from {
                        return Err(coded(
                            "0A000",
                            format!("cannot use \"{value}\" with HEADER in COPY TO"),
                        ));
                    }
                    return Ok((true, true));
                }
                _ => {}
            }
        }
    }
    Err(coded(
        "42601",
        format!("{} requires a Boolean value or \"match\"", def.name),
    ))
}

// ---------------------------------------------------------------------------
// Text format (CopyReadAttributesText / CopyAttributeOutText)

fn invalid_utf8(bytes: &[u8]) -> PgWireError {
    let shown = bytes
        .iter()
        .take(2)
        .map(|byte| format!("0x{byte:02x}"))
        .collect::<Vec<_>>()
        .join(" ");
    coded(
        "22021",
        format!("invalid byte sequence for encoding \"UTF8\": {shown}"),
    )
}

/// One text-format line into fields: split at the delimiter (a backslash
/// escapes it), NULL where the raw field equals the NULL string, and the
/// backslash sequences decoded (`\b \f \n \r \t \v`, octal, `\x` hex).
pub(crate) fn parse_text_fields(line: &str, opts: &CopyOptions) -> Result<Vec<Option<String>>> {
    let bytes = line.as_bytes();
    let mut fields = Vec::new();
    let mut idx = 0;
    loop {
        let start = idx;
        let mut out = Vec::new();
        let mut found_delim = false;
        while idx < bytes.len() {
            let c = bytes[idx];
            idx += 1;
            if c == opts.delimiter {
                found_delim = true;
                break;
            }
            if c == b'\\' {
                if idx >= bytes.len() {
                    break;
                }
                let next = bytes[idx];
                idx += 1;
                match next {
                    b'0'..=b'7' => {
                        let mut value = u32::from(next - b'0');
                        for _ in 0..2 {
                            match bytes.get(idx) {
                                Some(digit @ b'0'..=b'7') => {
                                    value = value * 8 + u32::from(digit - b'0');
                                    idx += 1;
                                }
                                _ => break,
                            }
                        }
                        out.push((value & 0xff) as u8);
                    }
                    b'x' => match bytes.get(idx).copied().filter(u8::is_ascii_hexdigit) {
                        Some(first) => {
                            idx += 1;
                            let mut value = hex_value(first);
                            if let Some(second) =
                                bytes.get(idx).copied().filter(u8::is_ascii_hexdigit)
                            {
                                idx += 1;
                                value = value * 16 + hex_value(second);
                            }
                            out.push(value);
                        }
                        None => out.push(b'x'),
                    },
                    b'b' => out.push(0x08),
                    b'f' => out.push(0x0c),
                    b'n' => out.push(b'\n'),
                    b'r' => out.push(b'\r'),
                    b't' => out.push(b'\t'),
                    b'v' => out.push(0x0b),
                    other => out.push(other),
                }
                continue;
            }
            out.push(c);
        }
        let raw_end = if found_delim { idx - 1 } else { idx };
        let raw = &bytes[start..raw_end];
        if raw == opts.null.as_bytes() {
            fields.push(None);
        } else {
            if out.contains(&0) {
                return Err(invalid_utf8(&[0]));
            }
            fields.push(Some(String::from_utf8(out).map_err(|error| {
                let bad = error.as_bytes()[error.utf8_error().valid_up_to()..].to_vec();
                invalid_utf8(&bad)
            })?));
        }
        if !found_delim {
            return Ok(fields);
        }
    }
}

fn hex_value(digit: u8) -> u8 {
    (digit as char).to_digit(16).unwrap_or(0) as u8
}

/// A value in text format (`None`: the NULL string).
pub(crate) fn render_text_value(value: Option<&str>, opts: &CopyOptions, out: &mut String) {
    let Some(value) = value else {
        out.push_str(&opts.null);
        return;
    };
    let delim = opts.delimiter as char;
    for ch in value.chars() {
        match ch {
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{b}' => out.push_str("\\v"),
            '\\' => out.push_str("\\\\"),
            ch if ch == delim => {
                out.push('\\');
                out.push(ch);
            }
            ch => out.push(ch),
        }
    }
}

// ---------------------------------------------------------------------------
// CSV format (CopyReadAttributesCSV / CopyAttributeOutCSV)

/// One CSV record into fields, with FORCE_NOT_NULL / FORCE_NULL applied.
pub(crate) fn parse_csv_fields(line: &str, opts: &CopyOptions) -> Result<Vec<Option<String>>> {
    let bytes = line.as_bytes();
    let (delim, quote, escape) = (opts.delimiter, opts.quote, opts.escape);
    let mut fields = Vec::new();
    let mut idx = 0;
    loop {
        let start = idx;
        let mut out = Vec::new();
        let mut saw_quote = false;
        let mut found_delim = false;
        let mut end;
        'field: loop {
            // Outside quotes.
            loop {
                end = idx;
                if idx >= bytes.len() {
                    break 'field;
                }
                let c = bytes[idx];
                idx += 1;
                if c == delim {
                    found_delim = true;
                    break 'field;
                }
                if c == quote {
                    saw_quote = true;
                    break;
                }
                out.push(c);
            }
            // Inside quotes.
            loop {
                if idx >= bytes.len() {
                    return Err(coded("22P04", "unterminated CSV quoted field"));
                }
                let c = bytes[idx];
                idx += 1;
                if c == escape {
                    if let Some(&next) = bytes.get(idx) {
                        if next == escape || next == quote {
                            out.push(next);
                            idx += 1;
                            continue;
                        }
                    }
                }
                if c == quote {
                    break;
                }
                out.push(c);
            }
        }
        let raw = &bytes[start..end];
        let column = fields.len();
        let is_null_text = raw == opts.null.as_bytes();
        let mut value = if !saw_quote && is_null_text {
            None
        } else {
            Some(String::from_utf8(out).map_err(|error| {
                let bad = error.as_bytes()[error.utf8_error().valid_up_to()..].to_vec();
                invalid_utf8(&bad)
            })?)
        };
        if value.is_none()
            && opts
                .force_not_null_flags
                .get(column)
                .copied()
                .unwrap_or(false)
        {
            value = Some(opts.null.clone());
        } else if value.as_deref() == Some(opts.null.as_str())
            && opts.force_null_flags.get(column).copied().unwrap_or(false)
        {
            value = None;
        }
        fields.push(value);
        if !found_delim {
            return Ok(fields);
        }
    }
}

/// A value in CSV (`None`: the NULL string, unquoted).
pub(crate) fn render_csv_value(
    value: Option<&str>,
    force_quote: bool,
    single_column: bool,
    opts: &CopyOptions,
    out: &mut String,
) {
    let Some(value) = value else {
        out.push_str(&opts.null);
        return;
    };
    let (delim, quote, escape) = (
        opts.delimiter as char,
        opts.quote as char,
        opts.escape as char,
    );
    let use_quote = force_quote
        || value == opts.null
        || (single_column && value == "\\.")
        || value.contains(|ch| ch == delim || ch == quote || ch == '\n' || ch == '\r');
    if !use_quote {
        out.push_str(value);
        return;
    }
    out.push(quote);
    for ch in value.chars() {
        if ch == quote || ch == escape {
            out.push(escape);
        }
        out.push(ch);
    }
    out.push(quote);
}

/// A rendered text/CSV line in the file encoding.
pub(crate) fn encode_copy_line(line: String, opts: &CopyOptions) -> Result<Vec<u8>> {
    if !opts.latin1 {
        return Ok(line.into_bytes());
    }
    let mut bytes = Vec::with_capacity(line.len());
    for ch in line.chars() {
        match u8::try_from(u32::from(ch)) {
            Ok(byte) => bytes.push(byte),
            Err(_) => {
                let mut buffer = [0_u8; 4];
                let sequence = ch
                    .encode_utf8(&mut buffer)
                    .bytes()
                    .map(|byte| format!("0x{byte:02x}"))
                    .collect::<Vec<_>>()
                    .join(" ");
                return Err(coded(
                    "22P05",
                    format!(
                        "character with byte sequence {sequence} in encoding \"UTF8\" has no equivalent in encoding \"LATIN1\""
                    ),
                ));
            }
        }
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(sql: &str, is_from: bool) -> Result<CopyOptions> {
        process_copy_options(&parse_copy_option_list(sql)?, is_from)
    }

    fn sqlstate(result: Result<CopyOptions>) -> String {
        match result {
            Err(PgWireError::Sql(error)) => error.sqlstate().to_string(),
            other => panic!("expected an SQL error, got {other:?}"),
        }
    }

    #[test]
    fn both_syntaxes_produce_the_same_options() {
        let legacy = opts(
            "WITH CSV HEADER DELIMITER AS ';' NULL 'nil' QUOTE '''' ESCAPE '\\' FORCE NOT NULL a, b",
            true,
        )
        .unwrap();
        let generic = opts(
            "(FORMAT csv, HEADER, DELIMITER ';', NULL 'nil', QUOTE '''', ESCAPE '\\', FORCE_NOT_NULL (a, b))",
            true,
        )
        .unwrap();
        assert_eq!(legacy, generic);
        assert_eq!(legacy.delimiter, b';');
        assert_eq!(legacy.quote, b'\'');
        assert_eq!(legacy.escape, b'\\');
        assert_eq!(
            legacy.force_not_null,
            ForceColumns::Named(vec!["a".into(), "b".into()])
        );
        assert!(opts("USING DELIMITERS '|'", true).unwrap().delimiter == b'|');
        assert!(opts("(header match)", true).unwrap().header_match);
    }

    #[test]
    fn postgresql_validation_errors() {
        for (sql, from, state) in [
            ("(format xml)", true, "22023"),
            ("(format csv, format csv)", true, "42601"),
            ("(delimiter ',,')", true, "0A000"),
            ("(delimiter 'a')", true, "22023"),
            ("(format csv, quote 'ab')", true, "0A000"),
            ("(quote '\"')", true, "0A000"),
            ("(format csv, delimiter '\"')", true, "22023"),
            ("(format binary, delimiter ',')", true, "42601"),
            ("(format binary, header)", true, "0A000"),
            ("(header match)", false, "0A000"),
            ("(header maybe)", true, "42601"),
            ("(format csv, force_quote *)", true, "0A000"),
            ("(format csv, force_not_null *)", false, "22023"),
            ("(force_null (a))", true, "0A000"),
            ("(null 'a,b', delimiter ',')", true, "22023"),
            ("(encoding 'nonsense')", true, "22023"),
            ("(encoding 'EUC_JP')", true, "0A000"),
            ("(on_error ignore)", false, "22023"),
            ("(on_error maybe)", true, "22023"),
            ("(reject_limit 5)", true, "22023"),
            ("(reject_limit 0, on_error ignore)", true, "22023"),
            ("(on_error ignore)", true, "0A000"),
            ("(default 'd')", true, "0A000"),
            ("(freeze)", true, "0A000"),
            ("(freeze)", false, "22023"),
            ("(log_verbosity loud)", true, "22023"),
            ("(bogus 1)", true, "42601"),
            ("(delimiter)", true, "42601"),
        ] {
            assert_eq!(sqlstate(opts(sql, from)), state, "{sql} from={from}");
        }
        for sql in [
            "(freeze false, on_error stop, log_verbosity verbose, encoding 'UTF-8')",
            "(encoding 'latin1')",
            "",
        ] {
            assert!(opts(sql, true).is_ok(), "{sql}");
        }
    }

    #[test]
    fn text_fields_follow_postgresql() {
        let text = CopyOptions::for_format(false);
        assert_eq!(
            parse_text_fields("a\\tb\t\\N\t\\x41\\101\\\t", &text).unwrap(),
            vec![Some("a\tb".into()), None, Some("AA\t".into())]
        );
        let mut pipe = CopyOptions::for_format(false);
        pipe.delimiter = b'|';
        pipe.null = "NULL".into();
        assert_eq!(
            parse_text_fields("x\\|y|NULL|", &pipe).unwrap(),
            vec![Some("x|y".into()), None, Some(String::new())]
        );
        let mut out = String::new();
        render_text_value(Some("a|b\\c\u{8}"), &pipe, &mut out);
        assert_eq!(out, "a\\|b\\\\c\\b");
    }

    #[test]
    fn csv_fields_follow_postgresql() {
        let mut csv = CopyOptions::for_format(true);
        assert_eq!(
            parse_csv_fields("1,,\"\",\"a\"\"b\",x\"y\"z", &csv).unwrap(),
            vec![
                Some("1".into()),
                None,
                Some(String::new()),
                Some("a\"b".into()),
                Some("xyz".into()),
            ]
        );
        assert_eq!(
            sqlstate(parse_csv_fields("\"open", &csv).map(|_| CopyOptions::default())),
            "22P04"
        );
        csv.quote = b'\'';
        csv.escape = b'\\';
        assert_eq!(
            parse_csv_fields("'it\\'s',b", &csv).unwrap(),
            vec![Some("it's".into()), Some("b".into())]
        );
        let mut out = String::new();
        render_csv_value(Some("it's"), false, false, &csv, &mut out);
        assert_eq!(out, "'it\\'s'");
    }
}
