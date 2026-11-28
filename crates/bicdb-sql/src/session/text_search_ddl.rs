//! DDL for authored text search configurations and dispatch of catalog commands.
use crate::fts::authored::{self, AuthoredConfig, Catalog, Configuration};
use crate::*;
use sqlparser::tokenizer::{Token, Tokenizer};

const OBJECT: &str = "text search configuration";

pub(super) type Tokens<'a> = &'a [Token];

pub(super) fn keyword(token: &Token, value: &str) -> bool {
    matches!(token, Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(value))
}

pub(super) fn syntax_error(token: Option<&Token>) -> SqlError {
    let near = token.map_or_else(|| "end of input".to_owned(), |token| format!("\"{token}\""));
    SqlError::data_exception("42601", format!("syntax error at or near {near}"), None)
}

pub(super) fn ident(tokens: Tokens<'_>) -> Result<(String, Tokens<'_>)> {
    let Some((Token::Word(word), rest)) = tokens.split_first() else {
        return Err(syntax_error(tokens.first()));
    };
    let name = if word.quote_style.is_some() {
        word.value.clone()
    } else {
        word.value.to_ascii_lowercase()
    };
    Ok((name, rest))
}

pub(super) fn object_name_from_tokens(tokens: Tokens<'_>) -> Result<(String, Tokens<'_>)> {
    let (first, rest) = ident(tokens)?;
    if let [Token::Period, suffix @ ..] = rest {
        let (second, rest) = ident(suffix)?;
        Ok((
            format!("{}.{}", pg_quote_ident(&first), pg_quote_ident(&second)),
            rest,
        ))
    } else {
        Ok((pg_quote_ident(&first), rest))
    }
}

fn unknown_config(name: &str) -> SqlError {
    SqlError::data_exception("42704", format!("{OBJECT} \"{name}\" does not exist"), None)
}

impl SqlSession<'_> {
    pub(crate) fn execute_raw_text_search_ddl(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        if !is_text_search_ddl(sql) {
            return Ok(None);
        }
        let mut tokens = Tokenizer::new(&PostgreSqlDialect {}, sql)
            .tokenize()
            .map_err(|error| SqlError::data_exception("42601", error.to_string(), None))?
            .into_iter()
            .filter(|token| !matches!(token, Token::Whitespace(_) | Token::EOF))
            .collect::<Vec<_>>();
        let first = tokens
            .iter()
            .position(|token| token != &Token::SemiColon)
            .unwrap_or(tokens.len());
        tokens.drain(..first);
        while tokens.last() == Some(&Token::SemiColon) {
            tokens.pop();
        }
        let [action, text, search, configuration, rest @ ..] = tokens.as_slice() else {
            return Ok(None);
        };
        if !keyword(text, "text") || !keyword(search, "search") {
            return Ok(None);
        }
        if keyword(configuration, "dictionary") {
            return self.execute_text_search_dictionary(action, rest).map(Some);
        }
        if !keyword(configuration, "configuration") {
            return Ok(None);
        }
        if keyword(action, "create") {
            self.create_text_search_configuration(rest).map(Some)
        } else if keyword(action, "drop") {
            self.drop_text_search_configurations(rest).map(Some)
        } else if keyword(action, "alter") {
            self.alter_text_search_configuration(rest).map(Some)
        } else {
            Ok(None)
        }
    }

    pub(super) fn text_search_creation_schema(&self, explicit: Option<&str>) -> Result<String> {
        let schema = if let Some(schema) = explicit {
            schema.to_owned()
        } else {
            let role = current_user_from_gucs(&self.session_gucs);
            let path = self
                .session_gucs
                .get("search_path")
                .map_or("\"$user\", public", String::as_str);
            let mut choice = None;
            for schema in crate::search_path::parse_search_path(path, &role) {
                if !crate::search_path::is_temp_schema(&schema)
                    && (matches!(schema.as_str(), "pg_catalog" | "information_schema")
                        || load_namespace(self.db_ref(), &schema)?.is_some())
                {
                    choice = Some(schema);
                    break;
                }
            }
            choice.ok_or_else(|| {
                SqlError::data_exception_public(
                    "3F000",
                    "no schema has been selected to create in",
                    None,
                )
            })?
        };
        if !matches!(schema.as_str(), "pg_catalog" | "information_schema")
            && load_namespace(self.db_ref(), &schema)?.is_none()
        {
            return Err(SqlError::data_exception_public(
                "3F000",
                format!("schema \"{schema}\" does not exist"),
                None,
            ));
        }
        self.require_schema_create_privilege(&schema)?;
        Ok(schema)
    }

    fn create_text_search_configuration(&mut self, tokens: Tokens<'_>) -> Result<SqlResult> {
        let (target, tokens) = object_name_from_tokens(tokens)?;
        let [Token::LParen, option, Token::Eq, source @ ..] = tokens else {
            return Err(syntax_error(tokens.first()));
        };
        let (source, rest) = match source {
            [Token::SingleQuotedString(source), rest @ ..] => (pg_quote_ident(source), rest),
            _ => object_name_from_tokens(source)?,
        };
        if rest != [Token::RParen] {
            return Err(syntax_error(rest.first()));
        }
        let parts = authored::qualified_name(&target).expect("parsed configuration name");
        let name = parts.last().expect("configuration name").clone();
        let schema =
            self.text_search_creation_schema((parts.len() == 2).then(|| parts[0].as_str()))?;
        let catalog = Catalog::load(self.db_ref(), &self.session_gucs)?;
        let (source_oid, mappings) = if keyword(option, "copy") {
            let source = catalog
                .resolve(&source)?
                .ok_or_else(|| unknown_config(&source))?;
            (source.source_oid(), source.mappings())
        } else if keyword(option, "parser") {
            let parts = authored::qualified_name(&source).expect("parsed parser name");
            let name = parts.join(".");
            crate::fts::is_default_text_search_parser(&SqlValue::String(name))?;
            (0, Some(Arc::new(authored::DictionaryMappings::new())))
        } else {
            let (name, _) = ident(std::slice::from_ref(option))?;
            return Err(SqlError::data_exception_public(
                "42601",
                format!("text search configuration parameter \"{name}\" not recognized"),
                None,
            ));
        };
        self.require_free_text_search_name(&catalog, &schema, &name)?;
        let oid = allocate_user_type_oids(self.db_mut()?, 1)?[0];
        let config = AuthoredConfig {
            oid: u32::try_from(oid)
                .map_err(|_| SqlError::InvalidSql("configuration OID out of range".into()))?,
            schema,
            name,
            owner: current_user_from_gucs(&self.session_gucs),
            source_oid,
            mappings,
        };
        self.save_text_search_configuration(&config, None)?;
        Ok(SqlResult::command("CREATE TEXT SEARCH CONFIGURATION"))
    }

    fn require_free_text_search_name(
        &self,
        catalog: &Catalog,
        schema: &str,
        name: &str,
    ) -> Result<()> {
        if catalog.in_schema(schema, name).is_some() {
            return Err(SqlError::data_exception(
                "23505",
                "duplicate key value violates unique constraint \"pg_ts_config_cfgname_index\"",
                Some(format!(
                    "Key (cfgname, cfgnamespace)=({name}, {}) already exists.",
                    namespace_oid(schema)
                )),
            ));
        }
        Ok(())
    }

    fn require_text_search_owner(&self, owner: &str, written: &str) -> Result<()> {
        if self.current_user_is_superuser()? || self.current_user_holds_role(owner)? {
            return Ok(());
        }
        Err(SqlError::data_exception_public(
            "42501",
            format!("must be owner of {OBJECT} {written}"),
            None,
        ))
    }

    fn changeable_text_search_configuration(
        &self,
        config: Configuration<'_>,
        written: &str,
    ) -> Result<AuthoredConfig> {
        match config {
            Configuration::Authored(config) => {
                self.require_text_search_owner(&config.owner, written)?;
                Ok(config.clone())
            }
            Configuration::Builtin(_) => {
                self.require_text_search_owner(BOOTSTRAP_ROLE_NAME, written)?;
                Err(SqlError::data_exception_public(
                    "0A000",
                    format!("cannot change built-in {OBJECT} {written}"),
                    None,
                ))
            }
        }
    }

    fn alter_text_search_configuration(&mut self, tokens: Tokens<'_>) -> Result<SqlResult> {
        let (written, rest) = object_name_from_tokens(tokens)?;
        let catalog = Catalog::load(self.db_ref(), &self.session_gucs)?;
        let config = catalog
            .resolve(&written)?
            .ok_or_else(|| unknown_config(&written))?;
        let previous = self.changeable_text_search_configuration(config, &written)?;
        let mut config = previous.clone();
        match rest {
            [rename, to, target @ ..] if keyword(rename, "rename") && keyword(to, "to") => {
                let (name, rest) = ident(target)?;
                if !rest.is_empty() {
                    return Err(syntax_error(rest.first()));
                }
                self.require_schema_create_privilege(&config.schema)?;
                self.require_free_text_search_name(&catalog, &config.schema, &name)?;
                config.name = name;
            }
            [set, schema, target @ ..] if keyword(set, "set") && keyword(schema, "schema") => {
                let (schema, rest) = ident(target)?;
                if !rest.is_empty() {
                    return Err(syntax_error(rest.first()));
                }
                if config.schema != schema {
                    let schema = self.text_search_creation_schema(Some(&schema))?;
                    self.require_free_text_search_name(&catalog, &schema, &config.name)?;
                    config.schema = schema;
                }
            }
            [owner, to, target @ ..] if keyword(owner, "owner") && keyword(to, "to") => {
                let (name, rest) = ident(target)?;
                if !rest.is_empty() {
                    return Err(syntax_error(rest.first()));
                }
                let current = current_user_from_gucs(&self.session_gucs);
                let role = match target.first() {
                    Some(token)
                        if keyword(token, "current_user") || keyword(token, "current_role") =>
                    {
                        current.clone()
                    }
                    Some(token) if keyword(token, "session_user") => {
                        session_user_from_gucs(&self.session_gucs)
                    }
                    _ => normalize_role_name(&name),
                };
                ensure_known_role(&list_roles(self.db_ref())?, &role)?;
                self.require_settable_new_owner(&role, &format!("{OBJECT} {written}"))?;
                let database = current_database_from_gucs(&self.session_gucs);
                if !self.current_user_is_superuser()?
                    && role_can_create_in_schema(
                        self.db_ref(),
                        &role,
                        &config.schema,
                        Some(&database),
                    )? != Some(true)
                {
                    return Err(SqlError::data_exception_public(
                        "42501",
                        format!("permission denied for schema {}", config.schema),
                        None,
                    ));
                }
                config.owner = role;
            }
            [action, mapping, tail @ ..] if keyword(mapping, "mapping") => {
                config.mappings =
                    Some(self.change_text_search_mapping(&catalog, &config, action, tail)?);
            }
            _ => return Err(syntax_error(rest.first())),
        }
        self.save_text_search_configuration(&config, Some(previous))?;
        Ok(SqlResult::command("ALTER TEXT SEARCH CONFIGURATION"))
    }

    fn change_text_search_mapping(
        &self,
        catalog: &Catalog,
        config: &AuthoredConfig,
        action: &Token,
        tokens: Tokens<'_>,
    ) -> Result<Arc<authored::DictionaryMappings>> {
        let mut mappings = config.effective_mappings();
        let mappings_mut = Arc::make_mut(&mut mappings);
        if keyword(action, "drop") {
            let (if_exists, tokens) = match tokens {
                [first, second, rest @ ..] if keyword(first, "if") && keyword(second, "exists") => {
                    (true, rest)
                }
                _ => (false, tokens),
            };
            let (types, rest) = parse_mapping_token_types(tokens)?;
            if !rest.is_empty() {
                return Err(syntax_error(rest.first()));
            }
            for (id, alias) in types {
                if mappings_mut.remove(&id).is_none() {
                    let message = format!("mapping for token type \"{alias}\" does not exist");
                    if if_exists {
                        crate::statement_context::report_notice(
                            None,
                            "00000",
                            format!("{message}, skipping"),
                        );
                    } else {
                        return Err(SqlError::data_exception_public("42704", message, None));
                    }
                }
            }
        } else if keyword(action, "add") || keyword(action, "alter") {
            if let [replace, rest @ ..] = tokens {
                if keyword(action, "alter") && keyword(replace, "replace") {
                    replace_text_search_dictionary(catalog, mappings_mut.values_mut(), rest)?;
                    return Ok(mappings);
                }
            }
            let (types, rest) = parse_mapping_token_types(tokens)?;
            match rest {
                [with, dictionaries @ ..] if keyword(with, "with") => {
                    let dictionaries = parse_mapping_dictionaries(catalog, dictionaries)?;
                    for (id, _) in types {
                        if keyword(action, "add") && mappings_mut.contains_key(&id) {
                            return Err(SqlError::coded(
                                "23505",
                                "duplicate key value violates unique constraint \"pg_ts_config_map_index\"",
                                Some(format!("Key (mapcfg, maptokentype, mapseqno)=({}, {id}, 1) already exists.", config.oid)),
                                None,
                            ));
                        }
                        mappings_mut.insert(id, dictionaries.clone());
                    }
                }
                [replace, rest @ ..] if keyword(action, "alter") && keyword(replace, "replace") => {
                    let selected = types.into_iter().map(|(id, _)| id).collect::<BTreeSet<_>>();
                    replace_text_search_dictionary(
                        catalog,
                        mappings_mut.iter_mut().filter_map(|(id, dictionaries)| {
                            selected.contains(id).then_some(dictionaries)
                        }),
                        rest,
                    )?;
                }
                _ => return Err(syntax_error(rest.first())),
            }
        } else {
            return Err(syntax_error(Some(action)));
        }
        Ok(mappings)
    }

    fn drop_text_search_configurations(&mut self, tokens: Tokens<'_>) -> Result<SqlResult> {
        let (if_exists, mut tokens) = match tokens {
            [first, second, rest @ ..] if keyword(first, "if") && keyword(second, "exists") => {
                (true, rest)
            }
            _ => (false, tokens),
        };
        let catalog = Catalog::load(self.db_ref(), &self.session_gucs)?;
        let mut remove = Vec::new();
        loop {
            let (name, rest) = object_name_from_tokens(tokens)?;
            if let Some(config) = catalog.resolve(&name)? {
                let config = self.changeable_text_search_configuration(config, &name)?;
                if !remove
                    .iter()
                    .any(|previous: &AuthoredConfig| previous.oid == config.oid)
                {
                    remove.push(config);
                }
            } else if if_exists {
                crate::statement_context::notice_skipping_missing(OBJECT, &name);
            } else {
                return Err(unknown_config(&name));
            }
            match rest {
                [Token::Comma, rest @ ..] => tokens = rest,
                [] => break,
                [behavior] if keyword(behavior, "cascade") || keyword(behavior, "restrict") => {
                    break
                }
                _ => return Err(syntax_error(rest.first())),
            }
        }
        for config in remove {
            self.remove_text_search_configuration(config)?;
        }
        Ok(SqlResult::command("DROP TEXT SEARCH CONFIGURATION"))
    }

    fn save_text_search_configuration(
        &mut self,
        config: &AuthoredConfig,
        previous: Option<AuthoredConfig>,
    ) -> Result<()> {
        authored::store(self.db_mut()?, config)?;
        if self.tx.is_some() {
            self.ddl_undo.push(DdlUndo::RestoreTextSearchConfiguration {
                oid: config.oid,
                previous: previous.map(Box::new),
            });
        }
        Ok(())
    }

    pub(crate) fn remove_text_search_configuration(
        &mut self,
        config: AuthoredConfig,
    ) -> Result<()> {
        authored::delete(self.db_mut()?, config.oid)?;
        if self.tx.is_some() {
            self.ddl_undo.push(DdlUndo::RestoreTextSearchConfiguration {
                oid: config.oid,
                previous: Some(Box::new(config)),
            });
        }
        Ok(())
    }
}

fn parse_mapping_token_types(tokens: Tokens<'_>) -> Result<(Vec<(u8, String)>, Tokens<'_>)> {
    let [keyword_for, tokens @ ..] = tokens else {
        return Err(syntax_error(tokens.first()));
    };
    let mut tokens = tokens;
    if !keyword(keyword_for, "for") {
        return Err(syntax_error(Some(keyword_for)));
    }
    let mut result = Vec::new();
    loop {
        let (alias, rest) = ident(tokens)?;
        let id = crate::fts::DEFAULT_PARSER_TOKEN_TYPES
            .iter()
            .position(|(token, _)| *token == alias)
            .map(|index| (index + 1) as u8)
            .ok_or_else(|| {
                SqlError::data_exception_public(
                    "22023",
                    format!("alias \"{alias}\" is not a valid token type"),
                    None,
                )
            })?;
        result.push((id, alias));
        if let [Token::Comma, tail @ ..] = rest {
            tokens = tail;
        } else {
            return Ok((result, rest));
        }
    }
}

fn text_search_dictionary_oid<'a>(
    catalog: &Catalog,
    tokens: Tokens<'a>,
) -> Result<(u32, Tokens<'a>)> {
    let (written, rest) = object_name_from_tokens(tokens)?;
    let oid = catalog
        .dictionary_oid(&written)?
        .ok_or_else(|| super::text_search_dictionaries::unknown_dictionary(&written))?;
    Ok((oid, rest))
}

fn parse_mapping_dictionaries(catalog: &Catalog, mut tokens: Tokens<'_>) -> Result<Vec<u32>> {
    let mut ids = Vec::new();
    loop {
        let (id, rest) = text_search_dictionary_oid(catalog, tokens)?;
        ids.push(id);
        match rest {
            [Token::Comma, tail @ ..] => tokens = tail,
            [] => return Ok(ids),
            _ => return Err(syntax_error(rest.first())),
        }
    }
}

fn replace_text_search_dictionary<'a>(
    catalog: &Catalog,
    mappings: impl Iterator<Item = &'a mut Vec<u32>>,
    tokens: Tokens<'_>,
) -> Result<()> {
    let (old, rest) = text_search_dictionary_oid(catalog, tokens)?;
    let [with, catalogue @ ..] = rest else {
        return Err(syntax_error(rest.first()));
    };
    if !keyword(with, "with") {
        return Err(syntax_error(Some(with)));
    }
    let (new, tail) = text_search_dictionary_oid(catalog, catalogue)?;
    if !tail.is_empty() {
        return Err(syntax_error(tail.first()));
    }
    for oid in mappings.flatten() {
        if *oid == old {
            *oid = new;
        }
    }
    Ok(())
}

pub(crate) fn is_text_search_ddl(sql: &str) -> bool {
    let Some(action) = leading_sql_keyword(sql) else {
        return false;
    };
    if !(action.eq_ignore_ascii_case("create")
        || action.eq_ignore_ascii_case("alter")
        || action.eq_ignore_ascii_case("drop"))
    {
        return false;
    }
    let after =
        |input: &str, word: &str| word.as_ptr() as usize - input.as_ptr() as usize + word.len();
    let mut rest = &sql[after(sql, action)..];
    for expected in ["text", "search"] {
        let Some(word) = leading_sql_keyword(rest) else {
            return false;
        };
        if !word.eq_ignore_ascii_case(expected) {
            return false;
        }
        rest = &rest[after(rest, word)..];
    }
    leading_sql_keyword(rest).is_some_and(|word| {
        word.eq_ignore_ascii_case("configuration") || word.eq_ignore_ascii_case("dictionary")
    })
}
