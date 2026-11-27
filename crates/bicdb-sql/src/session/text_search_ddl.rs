//! CREATE / ALTER / DROP TEXT SEARCH { PARSER | TEMPLATE | DICTIONARY |
//! CONFIGURATION }.
//!
//! Dictionaries are validated through their template's init method when they
//! are created or altered, as PostgreSQL does, so a dictionary BicDB cannot
//! execute (Ispell, thesaurus, or a template of other functions) is refused
//! at creation instead of silently behaving like another one.
use crate::fts::{compile_dictionary_object, DEFAULT_PARSER_TOKEN_TYPES};
use crate::text_search_catalog::*;
use crate::*;
use sqlparser::tokenizer::{Token, Tokenizer};

fn ts_syntax_error(near: Option<&Token>) -> SqlError {
    let near = match near {
        None => "end of input".to_string(),
        Some(token) => format!("\"{token}\""),
    };
    SqlError::data_exception("42601", format!("syntax error at or near {near}"), None)
}

struct Tokens {
    tokens: Vec<Token>,
    position: usize,
}

impl Tokens {
    fn new(sql: &str) -> Result<Self> {
        let tokens = Tokenizer::new(&PostgreSqlDialect {}, sql)
            .tokenize()
            .map_err(|error| SqlError::data_exception("42601", error.to_string(), None))?
            .into_iter()
            .filter(|token| !matches!(token, Token::Whitespace(_) | Token::EOF))
            .collect();
        Ok(Self {
            tokens,
            position: 0,
        })
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.position)
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.position).cloned();
        if token.is_some() {
            self.position += 1;
        }
        token
    }

    fn peek_keyword(&self, keyword: &str) -> bool {
        matches!(self.peek(), Some(Token::Word(word))
            if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(keyword))
    }

    fn keyword(&mut self, keyword: &str) -> bool {
        let found = self.peek_keyword(keyword);
        if found {
            self.position += 1;
        }
        found
    }

    fn expect_keyword(&mut self, keyword: &str) -> Result<()> {
        if self.keyword(keyword) {
            Ok(())
        } else {
            Err(ts_syntax_error(self.peek()))
        }
    }

    fn expect(&mut self, expected: &Token) -> Result<()> {
        if self.peek() == Some(expected) {
            self.position += 1;
            Ok(())
        } else {
            Err(ts_syntax_error(self.peek()))
        }
    }

    fn consume(&mut self, expected: &Token) -> bool {
        let found = self.peek() == Some(expected);
        if found {
            self.position += 1;
        }
        found
    }

    /// An identifier: quoted keeps its spelling, unquoted is lower-cased.
    fn ident(&mut self) -> Result<String> {
        match self.peek() {
            Some(Token::Word(word)) => {
                let value = if word.quote_style.is_some() {
                    word.value.clone()
                } else {
                    word.value.to_lowercase()
                };
                self.position += 1;
                Ok(value)
            }
            other => Err(ts_syntax_error(other)),
        }
    }

    fn qualified_name(&mut self) -> Result<Vec<String>> {
        let mut parts = vec![self.ident()?];
        while self.consume(&Token::Period) {
            parts.push(self.ident()?);
        }
        if parts.len() > 3 {
            return Err(SqlError::data_exception(
                "42601",
                format!(
                    "improper qualified name (too many dotted names): {}",
                    parts.join(".")
                ),
                None,
            ));
        }
        Ok(parts)
    }

    fn name_list(&mut self) -> Result<Vec<Vec<String>>> {
        let mut names = vec![self.qualified_name()?];
        while self.consume(&Token::Comma) {
            names.push(self.qualified_name()?);
        }
        Ok(names)
    }

    fn end(&mut self) -> Result<()> {
        while self.consume(&Token::SemiColon) {}
        match self.peek() {
            None => Ok(()),
            other => Err(ts_syntax_error(other)),
        }
    }
}

#[derive(Clone, Debug)]
enum DefValue {
    Name(Vec<String>),
    Text(String),
    Number(String),
}

#[derive(Clone, Debug)]
struct DefElem {
    name: String,
    value: Option<DefValue>,
}

impl DefElem {
    fn option(&self) -> Result<TsOption> {
        let (value, numeric) = match &self.value {
            None => {
                return Err(SqlError::data_exception(
                    "42601",
                    format!("{} requires a parameter", self.name),
                    None,
                ))
            }
            Some(DefValue::Name(parts)) => (parts.join("."), false),
            Some(DefValue::Text(text)) => (text.clone(), false),
            Some(DefValue::Number(number)) => (number.clone(), true),
        };
        Ok(TsOption {
            name: self.name.clone(),
            value,
            numeric,
        })
    }

    /// The value as a qualified name (`template = pg_catalog.simple`).
    fn name_value(&self) -> Result<Vec<String>> {
        match &self.value {
            Some(DefValue::Name(parts)) => Ok(parts.clone()),
            Some(DefValue::Text(text)) => {
                Ok(split_ts_name(text).unwrap_or_else(|| vec![text.clone()]))
            }
            Some(DefValue::Number(number)) => Ok(vec![number.clone()]),
            None => Err(SqlError::data_exception(
                "42601",
                format!("{} requires a parameter", self.name),
                None,
            )),
        }
    }
}

/// `( name [= value] [, ...] )`.
fn definition_list(tokens: &mut Tokens) -> Result<Vec<DefElem>> {
    tokens.expect(&Token::LParen)?;
    let mut elements = Vec::new();
    loop {
        let name = tokens.ident()?;
        let value = if tokens.consume(&Token::Eq) {
            if matches!(tokens.peek(), Some(Token::Word(_))) {
                Some(DefValue::Name(tokens.qualified_name()?))
            } else {
                Some(match tokens.next() {
                    Some(
                        Token::SingleQuotedString(text)
                        | Token::EscapedStringLiteral(text)
                        | Token::NationalStringLiteral(text),
                    ) => DefValue::Text(text),
                    Some(Token::DollarQuotedString(text)) => DefValue::Text(text.value),
                    Some(Token::Number(number, _)) => DefValue::Number(number),
                    Some(Token::Minus) => match tokens.next() {
                        Some(Token::Number(number, _)) => DefValue::Number(format!("-{number}")),
                        other => return Err(ts_syntax_error(other.as_ref())),
                    },
                    Some(Token::Plus) => match tokens.next() {
                        Some(Token::Number(number, _)) => DefValue::Number(number),
                        other => return Err(ts_syntax_error(other.as_ref())),
                    },
                    other => return Err(ts_syntax_error(other.as_ref())),
                })
            }
        } else {
            None
        };
        elements.push(DefElem { name, value });
        if tokens.consume(&Token::Comma) {
            continue;
        }
        tokens.expect(&Token::RParen)?;
        return Ok(elements);
    }
}

fn kind_keyword(tokens: &mut Tokens) -> Result<TsKind> {
    for kind in [
        TsKind::Parser,
        TsKind::Template,
        TsKind::Dictionary,
        TsKind::Configuration,
    ] {
        if tokens.keyword(kind.keyword()) {
            return Ok(kind);
        }
    }
    Err(ts_syntax_error(tokens.peek()))
}

fn undefined(kind: TsKind, parts: &[String]) -> SqlError {
    SqlError::data_exception(
        "42704",
        format!("{} \"{}\" does not exist", kind.noun(), parts.join(".")),
        None,
    )
}

fn already_exists(kind: TsKind, name: &str, schema: &str) -> SqlError {
    SqlError::data_exception(
        "42710",
        format!(
            "{} \"{name}\" already exists in schema \"{schema}\"",
            kind.noun()
        ),
        None,
    )
}

fn insufficient_privilege(message: String) -> SqlError {
    SqlError::coded("42501", message, None, None)
}

/// The catalog index whose uniqueness a duplicate CREATE violates.
fn duplicate_object(kind: TsKind, name: &str, schema: &str) -> SqlError {
    let (index, name_column, namespace_column) = match kind {
        TsKind::Parser => ("pg_ts_parser_prsname_index", "prsname", "prsnamespace"),
        TsKind::Template => ("pg_ts_template_tmplname_index", "tmplname", "tmplnamespace"),
        TsKind::Dictionary => ("pg_ts_dict_dictname_index", "dictname", "dictnamespace"),
        TsKind::Configuration => ("pg_ts_config_cfgname_index", "cfgname", "cfgnamespace"),
    };
    SqlError::coded(
        "23505",
        format!("duplicate key value violates unique constraint \"{index}\""),
        Some(format!(
            "Key ({name_column}, {namespace_column})=({name}, {}) already exists.",
            namespace_oid(schema)
        )),
        None,
    )
}

/// The token type ids of `names` for the default parser, without duplicates.
fn token_types(names: &[String]) -> Result<Vec<(i32, String)>> {
    let mut types: Vec<(i32, String)> = Vec::new();
    for name in names {
        if types.iter().any(|(_, seen)| seen == name) {
            continue;
        }
        let Some(index) = DEFAULT_PARSER_TOKEN_TYPES
            .iter()
            .position(|(alias, _)| alias == name)
        else {
            return Err(SqlError::data_exception(
                "22023",
                format!("token type \"{name}\" does not exist"),
                None,
            ));
        };
        types.push((index as i32 + 1, name.clone()));
    }
    Ok(types)
}

fn token_name_list(tokens: &mut Tokens) -> Result<Vec<String>> {
    let mut names = vec![tokens.ident()?];
    while tokens.consume(&Token::Comma) {
        names.push(tokens.ident()?);
    }
    Ok(names)
}

/// A support function named by CREATE TEXT SEARCH PARSER/TEMPLATE: one of
/// the built-in methods taking `arguments`.
fn support_function(name_parts: &[String], arguments: &[&str], returns: i64) -> Result<String> {
    let (schema, name) = match name_parts {
        [name] => (None, name.as_str()),
        [schema, name] | [_, schema, name] => (Some(schema.as_str()), name.as_str()),
        _ => return Err(ts_syntax_error(None)),
    };
    let nargs = arguments.len() as i64;
    let found = TEXT_SEARCH_SUPPORT_FUNCTIONS
        .iter()
        .find(|(_, candidate, _, count)| *candidate == name && *count == nargs);
    match found {
        Some((_, _, result, _)) if schema.is_none_or(|schema| schema == "pg_catalog") => {
            if *result != returns {
                let returns = if returns == 2278 { "void" } else { "internal" };
                return Err(SqlError::data_exception(
                    "42P17",
                    format!(
                        "function {name}({}) should return type {returns}",
                        arguments.join(", ")
                    ),
                    None,
                ));
            }
            Ok(name.to_string())
        }
        _ => Err(SqlError::undefined_function(format!(
            "function {}({}) does not exist",
            name_parts.join("."),
            arguments.join(", ")
        ))),
    }
}

fn is_builtin_object(kind: TsKind, oid: i64) -> bool {
    builtin_objects()
        .iter()
        .any(|object| object.kind == kind && object.oid == oid)
}

/// The objects that depend on `object` (directly and through other
/// dependents), dependents last-to-first in drop order.
fn dependents(scope: &TsScope, object: &TsObject) -> Vec<TsObject> {
    let mut found: Vec<TsObject> = Vec::new();
    let mut pending = vec![object.clone()];
    while let Some(current) = pending.pop() {
        for candidate in scope.objects() {
            let depends = match (current.kind, candidate.kind) {
                (TsKind::Template, TsKind::Dictionary) => candidate.template == current.oid,
                (TsKind::Parser, TsKind::Configuration) => candidate.parser == current.oid,
                (TsKind::Dictionary, TsKind::Configuration) => candidate
                    .mappings
                    .iter()
                    .any(|(_, dictionaries)| dictionaries.contains(&current.oid)),
                _ => false,
            };
            if depends
                && candidate.oid != object.oid
                && !found.iter().any(|seen| seen.oid == candidate.oid)
            {
                found.push(candidate.clone());
                pending.push(candidate.clone());
            }
        }
    }
    found
}

impl SqlSession<'_> {
    fn ts_scope(&self) -> Result<TsScope> {
        Ok(TsScope::new(
            user_ts_objects(self.db_ref())?,
            crate::pg_operators::effective_search_path(&self.session_gucs),
        ))
    }

    fn store_ts_object(&mut self, object: &TsObject) -> Result<()> {
        self.record_ts_undo(&object.key())?;
        save_ts_object(self.db_mut()?, object)
    }

    fn remove_ts_object(&mut self, object: &TsObject) -> Result<()> {
        self.record_ts_undo(&object.key())?;
        delete_ts_object(self.db_mut()?, &object.key())
    }

    fn record_ts_undo(&mut self, key: &str) -> Result<()> {
        if self.tx.is_some() {
            let previous = user_ts_objects(self.db_ref())?
                .iter()
                .find(|object| object.key() == key)
                .cloned();
            self.ddl_undo.push(DdlUndo::RestoreTextSearchObject {
                key: key.to_string(),
                previous: previous.map(Box::new),
            });
        }
        Ok(())
    }

    /// The schema a new object goes to: the named one, else the first
    /// existing schema of the search path.
    fn ts_creation_schema(&self, parts: &[String]) -> Result<(String, String)> {
        let (schema, name) = match parts {
            [name] => (None, name.clone()),
            [schema, name] | [_, schema, name] => (Some(schema.clone()), name.clone()),
            _ => return Err(ts_syntax_error(None)),
        };
        let schema = match schema {
            Some(schema) => {
                self.require_ts_schema(&schema)?;
                schema
            }
            None => {
                let mut chosen = None;
                for schema in crate::pg_operators::effective_search_path(&self.session_gucs) {
                    if schema == "pg_catalog" {
                        continue;
                    }
                    if schema == "public" || load_namespace(self.db_ref(), &schema)?.is_some() {
                        chosen = Some(schema);
                        break;
                    }
                }
                chosen.ok_or_else(|| {
                    SqlError::data_exception_public(
                        "3F000",
                        "no schema has been selected to create in",
                        None,
                    )
                })?
            }
        };
        if schema != "pg_catalog" {
            self.require_schema_create_privilege(&schema)?;
        }
        Ok((schema, name))
    }

    fn require_ts_schema(&self, schema: &str) -> Result<()> {
        if matches!(schema, "public" | "pg_catalog")
            || load_namespace(self.db_ref(), schema)?.is_some()
        {
            return Ok(());
        }
        Err(SqlError::data_exception_public(
            "3F000",
            format!("schema \"{schema}\" does not exist"),
            None,
        ))
    }

    /// Ownership (dictionaries, configurations) or superuser (parsers,
    /// templates, which have no owner) for ALTER and DROP.
    fn require_ts_owner(&self, object: &TsObject, written: &[String], action: &str) -> Result<()> {
        if self.current_user_is_superuser()? {
            return Ok(());
        }
        match &object.owner {
            Some(owner) if object.kind.has_owner() => {
                if self.current_user_holds_role(owner)? {
                    return Ok(());
                }
                Err(insufficient_privilege(format!(
                    "must be owner of {} {}",
                    object.kind.noun(),
                    written.join(".")
                )))
            }
            _ if action == "rename" => Err(insufficient_privilege(format!(
                "must be superuser to rename {} {}",
                object.kind.noun(),
                written.join(".")
            ))),
            _ => Err(insufficient_privilege("must be superuser".to_string())),
        }
    }

    pub(crate) fn execute_raw_text_search_ddl(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        if !sql
            .as_bytes()
            .windows(6)
            .any(|window| window.eq_ignore_ascii_case(b"search"))
        {
            return Ok(None);
        }
        let mut tokens = Tokens::new(sql)?;
        let verb = if tokens.keyword("create") {
            "create"
        } else if tokens.keyword("alter") {
            "alter"
        } else if tokens.keyword("drop") {
            "drop"
        } else {
            return Ok(None);
        };
        if !(tokens.keyword("text") && tokens.keyword("search")) {
            return Ok(None);
        }
        let kind = kind_keyword(&mut tokens)?;
        let result = match verb {
            "create" => self.create_ts_object(kind, &mut tokens)?,
            "alter" => self.alter_ts_object(kind, &mut tokens)?,
            _ => self.drop_ts_objects(kind, &mut tokens)?,
        };
        Ok(Some(result))
    }

    fn create_ts_object(&mut self, kind: TsKind, tokens: &mut Tokens) -> Result<SqlResult> {
        let parts = tokens.qualified_name()?;
        let definition = definition_list(tokens)?;
        tokens.end()?;
        if matches!(kind, TsKind::Parser | TsKind::Template) && !self.current_user_is_superuser()? {
            return Err(insufficient_privilege(format!(
                "must be superuser to create {}s",
                kind.noun()
            )));
        }
        let (schema, name) = self.ts_creation_schema(&parts)?;
        let scope = self.ts_scope()?;
        let owner = current_user_from_gucs(&self.session_gucs);
        let mut object = TsObject {
            kind,
            oid: 0,
            schema: schema.clone(),
            name: name.clone(),
            owner: kind.has_owner().then(|| owner.clone()),
            methods: Vec::new(),
            template: 0,
            options: Vec::new(),
            parser: 0,
            mappings: Vec::new(),
        };
        match kind {
            TsKind::Parser => {
                let mut methods = vec![String::new(); 5];
                for element in &definition {
                    let (slot, arguments, returns): (usize, &[&str], i64) = match element
                        .name
                        .as_str()
                    {
                        "start" => (0, &["internal", "integer"], 2281),
                        "gettoken" => (1, &["internal", "internal", "internal"], 2281),
                        "end" => (2, &["internal"], 2278),
                        "headline" => (3, &["internal", "internal", "tsquery"], 2281),
                        "lextypes" => (4, &["internal"], 2281),
                        other => {
                            return Err(SqlError::data_exception(
                                "42601",
                                format!("text search parser parameter \"{other}\" not recognized"),
                                None,
                            ))
                        }
                    };
                    methods[slot] = support_function(&element.name_value()?, arguments, returns)?;
                }
                for (slot, method) in [(0, "start"), (1, "gettoken"), (2, "end"), (4, "lextypes")] {
                    if methods[slot].is_empty() {
                        return Err(SqlError::data_exception(
                            "42P17",
                            format!("text search parser {method} method is required"),
                            None,
                        ));
                    }
                }
                let default = [
                    "prsd_start",
                    "prsd_nexttoken",
                    "prsd_end",
                    "",
                    "prsd_lextype",
                ];
                if methods
                    .iter()
                    .zip(default)
                    .enumerate()
                    .any(|(slot, (method, expected))| slot != 3 && method != expected)
                {
                    return Err(SqlError::Unsupported(
                        "text search parsers other than the default parser's methods are not supported"
                            .to_string(),
                    ));
                }
                object.methods = methods;
            }
            TsKind::Template => {
                let mut methods = vec![String::new(); 2];
                for element in &definition {
                    let (slot, arguments): (usize, &[&str]) = match element.name.as_str() {
                        "init" => (0, &["internal"]),
                        "lexize" => (1, &["internal", "internal", "internal", "internal"]),
                        other => {
                            return Err(SqlError::data_exception(
                                "42601",
                                format!(
                                    "text search template parameter \"{other}\" not recognized"
                                ),
                                None,
                            ))
                        }
                    };
                    methods[slot] = support_function(&element.name_value()?, arguments, 2281)?;
                }
                if methods[1].is_empty() {
                    return Err(SqlError::data_exception(
                        "42P17",
                        "text search template lexize method is required",
                        None,
                    ));
                }
                object.methods = methods;
            }
            TsKind::Dictionary => {
                let mut template = None;
                let mut options = Vec::new();
                for element in &definition {
                    if element.name == "template" {
                        let parts = element.name_value()?;
                        template = Some(
                            scope
                                .resolve(TsKind::Template, &parts)
                                .ok_or_else(|| undefined(TsKind::Template, &parts))?
                                .oid,
                        );
                    } else {
                        options.push(element.clone());
                    }
                }
                let Some(template) = template else {
                    return Err(SqlError::data_exception(
                        "42P17",
                        "text search template is required",
                        None,
                    ));
                };
                object.template = template;
                self.set_dictionary_options(&scope, &mut object, &options)?;
            }
            TsKind::Configuration => {
                let mut parser = None;
                let mut source = None;
                for element in &definition {
                    let parts = element.name_value()?;
                    match element.name.as_str() {
                        "parser" => {
                            parser = Some(
                                scope
                                    .resolve(TsKind::Parser, &parts)
                                    .ok_or_else(|| undefined(TsKind::Parser, &parts))?
                                    .oid,
                            )
                        }
                        "copy" => {
                            source = Some(
                                scope
                                    .resolve(TsKind::Configuration, &parts)
                                    .ok_or_else(|| undefined(TsKind::Configuration, &parts))?
                                    .clone(),
                            )
                        }
                        other => {
                            return Err(SqlError::data_exception(
                                "42601",
                                format!(
                                    "text search configuration parameter \"{other}\" not recognized"
                                ),
                                None,
                            ))
                        }
                    }
                }
                if parser.is_some() && source.is_some() {
                    return Err(SqlError::data_exception(
                        "42601",
                        "cannot specify both PARSER and COPY options",
                        None,
                    ));
                }
                match (parser, source) {
                    (_, Some(source)) => {
                        object.parser = source.parser;
                        object.mappings = source.mappings;
                    }
                    (Some(parser), None) => object.parser = parser,
                    (None, None) => {
                        return Err(SqlError::data_exception(
                            "42P17",
                            "text search parser is required",
                            None,
                        ))
                    }
                }
            }
        }
        if scope.in_schema(kind, &schema, &name).is_some() {
            return Err(duplicate_object(kind, &name, &schema));
        }
        object.oid = allocate_user_type_oids(self.db_mut()?, 1)?[0];
        self.store_ts_object(&object)?;
        Ok(SqlResult::command(format!(
            "CREATE TEXT SEARCH {}",
            kind.keyword()
        )))
    }

    /// Validates `elements` as `object`'s options through its template's
    /// init method and stores them.
    fn set_dictionary_options(
        &self,
        scope: &TsScope,
        object: &mut TsObject,
        elements: &[DefElem],
    ) -> Result<()> {
        // Valueless options fail when serialized, after validation.
        let mut options = Vec::new();
        let mut valueless = None;
        for element in elements {
            match element.option() {
                Ok(option) => options.push(option),
                Err(error) => {
                    valueless.get_or_insert(error);
                    options.push(TsOption {
                        name: element.name.clone(),
                        value: "true".to_string(),
                        numeric: false,
                    });
                }
            }
        }
        object.options = options;
        compile_dictionary_object(scope, object)?;
        if let Some(error) = valueless {
            return Err(error);
        }
        Ok(())
    }

    fn resolve_for_alter(
        &self,
        scope: &TsScope,
        kind: TsKind,
        parts: &[String],
    ) -> Result<TsObject> {
        scope
            .resolve(kind, parts)
            .cloned()
            .ok_or_else(|| undefined(kind, parts))
    }

    fn alter_ts_object(&mut self, kind: TsKind, tokens: &mut Tokens) -> Result<SqlResult> {
        let parts = tokens.qualified_name()?;
        let scope = self.ts_scope()?;
        let tag = format!("ALTER TEXT SEARCH {}", kind.keyword());
        if tokens.keyword("rename") {
            tokens.expect_keyword("to")?;
            let new_name = tokens.ident()?;
            tokens.end()?;
            let mut object = self.resolve_for_alter(&scope, kind, &parts)?;
            self.require_ts_owner(&object, &parts, "rename")?;
            if scope.in_schema(kind, &object.schema, &new_name).is_some() {
                return Err(already_exists(kind, &new_name, &object.schema));
            }
            object.name = new_name;
            self.store_ts_object(&object)?;
            return Ok(SqlResult::command(tag));
        }
        if tokens.keyword("set") {
            tokens.expect_keyword("schema")?;
            let schema = tokens.ident()?;
            tokens.end()?;
            let mut object = self.resolve_for_alter(&scope, kind, &parts)?;
            self.require_ts_owner(&object, &parts, "alter")?;
            self.require_ts_schema(&schema)?;
            if schema == object.schema {
                return Ok(SqlResult::command(tag));
            }
            if scope.in_schema(kind, &schema, &object.name).is_some() {
                return Err(already_exists(kind, &object.name, &schema));
            }
            if !self.current_user_is_superuser()? {
                self.require_schema_create_privilege(&schema)?;
            }
            object.schema = schema;
            self.store_ts_object(&object)?;
            return Ok(SqlResult::command(tag));
        }
        if kind.has_owner() && tokens.keyword("owner") {
            tokens.expect_keyword("to")?;
            let owner = if tokens.keyword("current_user") || tokens.keyword("current_role") {
                current_user_from_gucs(&self.session_gucs)
            } else if tokens.keyword("session_user") {
                session_user_from_gucs(&self.session_gucs)
            } else {
                normalize_role_name(&tokens.ident()?)
            };
            tokens.end()?;
            let mut object = self.resolve_for_alter(&scope, kind, &parts)?;
            ensure_known_role(&list_roles(self.db_ref())?, &owner)?;
            if object.owner.as_deref() == Some(owner.as_str()) {
                return Ok(SqlResult::command(tag));
            }
            self.require_ts_owner(&object, &parts, "alter")?;
            if !self.current_user_is_superuser()? {
                let current = current_user_from_gucs(&self.session_gucs);
                if !settable_role_closure(self.db_ref(), &current)?.contains(&owner) {
                    return Err(insufficient_privilege(format!(
                        "must be able to SET ROLE \"{owner}\""
                    )));
                }
                if object.schema != "pg_catalog" {
                    self.require_schema_create_privilege(&object.schema)?;
                }
            }
            object.owner = Some(owner);
            self.store_ts_object(&object)?;
            return Ok(SqlResult::command(tag));
        }
        match kind {
            TsKind::Dictionary if tokens.peek() == Some(&Token::LParen) => {
                let changes = definition_list(tokens)?;
                tokens.end()?;
                let mut object = self.resolve_for_alter(&scope, kind, &parts)?;
                self.require_ts_owner(&object, &parts, "alter")?;
                let mut elements = object
                    .options
                    .iter()
                    .map(|option| DefElem {
                        name: option.name.clone(),
                        value: Some(if option.numeric {
                            DefValue::Number(option.value.clone())
                        } else {
                            DefValue::Text(option.value.clone())
                        }),
                    })
                    .collect::<Vec<_>>();
                for change in changes {
                    elements.retain(|element| element.name != change.name);
                    if change.value.is_some() {
                        elements.push(change);
                    }
                }
                self.set_dictionary_options(&scope, &mut object, &elements)?;
                self.store_ts_object(&object)?;
                Ok(SqlResult::command(tag))
            }
            TsKind::Configuration => {
                let mut object = self.resolve_for_alter(&scope, kind, &parts)?;
                self.alter_mapping(&scope, &mut object, &parts, tokens)?;
                self.store_ts_object(&object)?;
                Ok(SqlResult::command(tag))
            }
            _ => Err(ts_syntax_error(tokens.peek())),
        }
    }

    fn alter_mapping(
        &self,
        scope: &TsScope,
        object: &mut TsObject,
        parts: &[String],
        tokens: &mut Tokens,
    ) -> Result<()> {
        let resolve_dictionaries = |names: &[Vec<String>]| -> Result<Vec<i64>> {
            names
                .iter()
                .map(|parts| {
                    scope
                        .resolve(TsKind::Dictionary, parts)
                        .map(|dictionary| dictionary.oid)
                        .ok_or_else(|| undefined(TsKind::Dictionary, parts))
                })
                .collect()
        };
        if tokens.keyword("drop") {
            tokens.expect_keyword("mapping")?;
            let missing_ok = if tokens.keyword("if") {
                tokens.expect_keyword("exists")?;
                true
            } else {
                false
            };
            tokens.expect_keyword("for")?;
            let names = token_name_list(tokens)?;
            tokens.end()?;
            self.require_ts_owner(object, parts, "alter")?;
            for (token, name) in token_types(&names)? {
                let before = object.mappings.len();
                object.mappings.retain(|(mapped, _)| *mapped != token);
                if object.mappings.len() == before {
                    if !missing_ok {
                        return Err(SqlError::data_exception(
                            "42704",
                            format!("mapping for token type \"{name}\" does not exist"),
                            None,
                        ));
                    }
                    crate::statement_context::report_notice(
                        None,
                        "00000",
                        format!("mapping for token type \"{name}\" does not exist, skipping"),
                    );
                }
            }
            return Ok(());
        }
        let add = if tokens.keyword("add") {
            true
        } else if tokens.keyword("alter") {
            false
        } else {
            return Err(ts_syntax_error(tokens.peek()));
        };
        tokens.expect_keyword("mapping")?;
        let names = if tokens.keyword("for") {
            Some(token_name_list(tokens)?)
        } else {
            None
        };
        if !add && tokens.keyword("replace") {
            let old = tokens.qualified_name()?;
            tokens.expect_keyword("with")?;
            let new = tokens.qualified_name()?;
            tokens.end()?;
            self.require_ts_owner(object, parts, "alter")?;
            let types = match &names {
                Some(names) => Some(token_types(names)?),
                None => None,
            };
            let ids = resolve_dictionaries(&[old, new])?;
            for (token, dictionaries) in &mut object.mappings {
                if types
                    .as_ref()
                    .is_some_and(|types| !types.iter().any(|(id, _)| id == token))
                {
                    continue;
                }
                for dictionary in dictionaries.iter_mut() {
                    if *dictionary == ids[0] {
                        *dictionary = ids[1];
                    }
                }
            }
            return Ok(());
        }
        let Some(names) = names else {
            return Err(ts_syntax_error(tokens.peek()));
        };
        tokens.expect_keyword("with")?;
        let dictionaries = tokens.name_list()?;
        tokens.end()?;
        self.require_ts_owner(object, parts, "alter")?;
        let types = token_types(&names)?;
        let ids = resolve_dictionaries(&dictionaries)?;
        for (token, _) in &types {
            match object
                .mappings
                .iter_mut()
                .find(|(mapped, _)| mapped == token)
            {
                // The catalog's (mapcfg, maptokentype, mapseqno) key.
                Some(_) if add => {
                    return Err(SqlError::coded(
                        "23505",
                        "duplicate key value violates unique constraint \"pg_ts_config_map_index\"",
                        Some(format!(
                            "Key (mapcfg, maptokentype, mapseqno)=({}, {token}, 1) already exists.",
                            object.oid
                        )),
                        None,
                    ));
                }
                Some((_, existing)) => *existing = ids.clone(),
                None => object.mappings.push((*token, ids.clone())),
            }
        }
        object.mappings.sort_by_key(|(token, _)| *token);
        Ok(())
    }

    fn drop_ts_objects(&mut self, kind: TsKind, tokens: &mut Tokens) -> Result<SqlResult> {
        let if_exists = if tokens.keyword("if") {
            tokens.expect_keyword("exists")?;
            true
        } else {
            false
        };
        let names = tokens.name_list()?;
        let cascade = if tokens.keyword("cascade") {
            true
        } else {
            tokens.keyword("restrict");
            false
        };
        tokens.end()?;
        let scope = self.ts_scope()?;
        let mut targets: Vec<(TsObject, Vec<String>)> = Vec::new();
        for parts in names {
            let Some(object) = scope.resolve(kind, &parts).cloned() else {
                if !if_exists {
                    return Err(undefined(kind, &parts));
                }
                if parts.len() > 1 && !self.ts_schema_exists(&parts[parts.len() - 2])? {
                    crate::statement_context::report_notice(
                        None,
                        "00000",
                        format!(
                            "schema \"{}\" does not exist, skipping",
                            parts[parts.len() - 2]
                        ),
                    );
                } else {
                    crate::statement_context::notice_skipping_missing(
                        kind.noun(),
                        &parts.join("."),
                    );
                }
                continue;
            };
            if is_builtin_object(kind, object.oid) {
                return Err(SqlError::dependent_objects_still_exist(format!(
                    "cannot drop {} {} because it is required by the database system",
                    kind.noun(),
                    object.name
                )));
            }
            self.require_ts_owner(&object, &parts, "drop")?;
            targets.push((object, parts));
        }
        let mut cascaded: Vec<TsObject> = Vec::new();
        for (object, _) in &targets {
            for dependent in dependents(&scope, object) {
                if targets
                    .iter()
                    .any(|(target, _)| target.oid == dependent.oid)
                    || cascaded.iter().any(|seen| seen.oid == dependent.oid)
                {
                    continue;
                }
                if !cascade {
                    return Err(SqlError::coded(
                        "2BP01",
                        format!(
                            "cannot drop {} {} because other objects depend on it",
                            object.kind.noun(),
                            object.name
                        ),
                        Some(format!(
                            "{} {} depends on {} {}",
                            dependent.kind.noun(),
                            scope.display_name(&dependent),
                            object.kind.noun(),
                            scope.display_name(object)
                        )),
                        Some("Use DROP ... CASCADE to drop the dependent objects too.".to_string()),
                    ));
                }
                cascaded.push(dependent);
            }
        }
        if !cascaded.is_empty() {
            crate::statement_context::notice_drop_cascades(
                &cascaded
                    .iter()
                    .map(|object| format!("{} {}", object.kind.noun(), scope.display_name(object)))
                    .collect::<Vec<_>>(),
            );
        }
        for object in cascaded
            .iter()
            .chain(targets.iter().map(|(object, _)| object))
        {
            self.remove_ts_object(object)?;
        }
        Ok(SqlResult::command(format!(
            "DROP TEXT SEARCH {}",
            kind.keyword()
        )))
    }

    fn ts_schema_exists(&self, schema: &str) -> Result<bool> {
        Ok(matches!(schema, "public" | "pg_catalog")
            || load_namespace(self.db_ref(), schema)?.is_some())
    }

    /// The text search objects of `schema` and the objects elsewhere that
    /// depend on them, for DROP SCHEMA.
    pub(crate) fn schema_ts_objects(&self, schema: &str) -> Result<Vec<TsObject>> {
        let scope = self.ts_scope()?;
        let mut objects = scope
            .user
            .iter()
            .filter(|object| object.schema == schema)
            .cloned()
            .collect::<Vec<_>>();
        for object in objects.clone() {
            for dependent in dependents(&scope, &object) {
                if !objects.iter().any(|seen| seen.key() == dependent.key()) {
                    objects.push(dependent);
                }
            }
        }
        Ok(objects)
    }

    /// `<kind> <name>` of a text search object, as DROP lists it.
    pub(crate) fn ts_object_description(&self, object: &TsObject) -> Result<String> {
        Ok(format!(
            "{} {}",
            object.kind.noun(),
            self.ts_scope()?.display_name(object)
        ))
    }

    /// Removes `objects` (DROP SCHEMA ... CASCADE).
    pub(crate) fn drop_schema_ts_objects(&mut self, objects: &[TsObject]) -> Result<()> {
        for object in objects {
            self.remove_ts_object(object)?;
        }
        Ok(())
    }
}
