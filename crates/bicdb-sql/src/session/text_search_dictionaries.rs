use super::text_search_ddl::{ident, keyword, object_name_from_tokens, syntax_error, Tokens};
use crate::fts::authored::{self, AuthoredConfig, AuthoredDictionary, Catalog};
use crate::fts::dictionary;
use crate::*;
use sqlparser::tokenizer::Token;

const KIND: &str = "text search dictionary";

pub(super) fn unknown_dictionary(written: &str) -> SqlError {
    SqlError::data_exception_public(
        "42704",
        format!("{KIND} \"{written}\" does not exist"),
        None,
    )
}

impl SqlSession<'_> {
    pub(super) fn execute_text_search_dictionary(
        &mut self,
        action: &Token,
        tokens: Tokens<'_>,
    ) -> Result<SqlResult> {
        if keyword(action, "create") {
            self.create_text_search_dictionary(tokens)
        } else if keyword(action, "alter") {
            self.alter_text_search_dictionary(tokens)
        } else if keyword(action, "drop") {
            self.drop_text_search_dictionaries(tokens)
        } else {
            Err(syntax_error(Some(action)))
        }
    }

    fn create_text_search_dictionary(&mut self, tokens: Tokens<'_>) -> Result<SqlResult> {
        let (target, rest) = object_name_from_tokens(tokens)?;
        let parts = authored::qualified_name(&target).expect("parsed dictionary name");
        let name = parts.last().expect("dictionary name").clone();
        let schema =
            self.text_search_creation_schema((parts.len() == 2).then(|| parts[0].as_str()))?;
        let catalog = Catalog::load(self.db_ref(), &self.session_gucs)?;
        require_free_name(&catalog, &schema, &name)?;

        let mut options = parse_options(rest)?;
        let Some(template_position) = options.iter().position(|(name, _)| name == "template")
        else {
            return Err(SqlError::data_exception_public(
                "42P17",
                "text search template is required",
                None,
            ));
        };
        let (_, template) = options.remove(template_position);
        let template = template.ok_or_else(|| {
            SqlError::data_exception_public("42601", "template requires a parameter", None)
        })?;
        let template_oid = dictionary::template_oid(&template).ok_or_else(|| {
            SqlError::data_exception_public(
                "42704",
                format!("text search template \"{template}\" does not exist"),
                None,
            )
        })?;

        let mut dictionary = AuthoredDictionary {
            oid: 0,
            schema,
            name,
            owner: current_user_from_gucs(&self.session_gucs),
            template: template_oid,
            options: Vec::new(),
        };
        apply_options(&mut dictionary, options, true)?;
        dictionary.oid = u32::try_from(allocate_user_type_oids(self.db_mut()?, 1)?[0])
            .map_err(|_| SqlError::numeric_value_out_of_range("dictionary OID out of range"))?;
        self.save_text_search_dictionary(&dictionary, None)?;
        Ok(SqlResult::command("CREATE TEXT SEARCH DICTIONARY"))
    }

    fn alter_text_search_dictionary(&mut self, tokens: Tokens<'_>) -> Result<SqlResult> {
        let (written, rest) = object_name_from_tokens(tokens)?;
        let catalog = Catalog::load(self.db_ref(), &self.session_gucs)?;
        let oid = catalog
            .dictionary_oid(&written)?
            .ok_or_else(|| unknown_dictionary(&written))?;
        let previous = self.require_authored_dictionary(&catalog, oid, &written)?;
        let mut dictionary = previous.clone();
        match rest {
            [rename, to, target @ ..] if keyword(rename, "rename") && keyword(to, "to") => {
                let (name, rest) = ident(target)?;
                if !rest.is_empty() {
                    return Err(syntax_error(rest.first()));
                }
                self.require_schema_create_privilege(&dictionary.schema)?;
                require_free_name(&catalog, &dictionary.schema, &name)?;
                dictionary.name = name;
            }
            [set, schema, target @ ..] if keyword(set, "set") && keyword(schema, "schema") => {
                let (schema, rest) = ident(target)?;
                if !rest.is_empty() {
                    return Err(syntax_error(rest.first()));
                }
                if dictionary.schema != schema {
                    let schema = self.text_search_creation_schema(Some(&schema))?;
                    require_free_name(&catalog, &schema, &dictionary.name)?;
                    dictionary.schema = schema;
                }
            }
            [Token::LParen, ..] => apply_options(&mut dictionary, parse_options(rest)?, false)?,
            _ => return Err(syntax_error(rest.first())),
        }
        self.save_text_search_dictionary(&dictionary, Some(previous))?;
        Ok(SqlResult::command("ALTER TEXT SEARCH DICTIONARY"))
    }

    fn require_authored_dictionary(
        &self,
        catalog: &Catalog,
        oid: u32,
        written: &str,
    ) -> Result<AuthoredDictionary> {
        let dictionary = catalog.find_dictionary(oid);
        let owner = dictionary.map_or(BOOTSTRAP_ROLE_NAME, |dictionary| dictionary.owner.as_str());
        if !self.current_user_is_superuser()? && !self.current_user_holds_role(owner)? {
            return Err(SqlError::data_exception_public(
                "42501",
                format!("must be owner of {KIND} {written}"),
                None,
            ));
        }
        dictionary.cloned().ok_or_else(|| {
            SqlError::data_exception_public(
                "0A000",
                format!("cannot change built-in {KIND} {written}"),
                None,
            )
        })
    }

    fn drop_text_search_dictionaries(&mut self, tokens: Tokens<'_>) -> Result<SqlResult> {
        let (if_exists, mut tokens) = match tokens {
            [first, second, tail @ ..] if keyword(first, "if") && keyword(second, "exists") => {
                (true, tail)
            }
            _ => (false, tokens),
        };
        let catalog = Catalog::load(self.db_ref(), &self.session_gucs)?;
        let mut dictionaries = BTreeMap::new();
        let cascade = loop {
            let (written, rest) = object_name_from_tokens(tokens)?;
            if let Some(oid) = catalog.dictionary_oid(&written)? {
                let dictionary = self.require_authored_dictionary(&catalog, oid, &written)?;
                dictionaries.insert(oid, (written, dictionary));
            } else if if_exists {
                crate::statement_context::notice_skipping_missing(KIND, &written);
            } else {
                return Err(unknown_dictionary(&written));
            }
            match rest {
                [Token::Comma, tail @ ..] => tokens = tail,
                [] => break false,
                [behavior] if keyword(behavior, "restrict") => break false,
                [behavior] if keyword(behavior, "cascade") => break true,
                _ => return Err(syntax_error(rest.first())),
            }
        };
        let mut dependent = Vec::new();
        let mut notices = Vec::new();
        for config in authored::list(self.db_ref())?.iter() {
            if let Some((written, _)) = config
                .mappings
                .as_ref()
                .into_iter()
                .flat_map(|mappings| mappings.values().flatten())
                .find_map(|oid| dictionaries.get(oid))
            {
                let name = catalog
                    .display_name(i64::from(config.oid))
                    .unwrap_or_else(|| config.qualified_name());
                if !cascade {
                    return Err(SqlError::coded(
                        "2BP01",
                        format!("cannot drop {KIND} {written} because other objects depend on it"),
                        Some(format!(
                            "text search configuration {name} depends on {KIND} {written}"
                        )),
                        Some("Use DROP ... CASCADE to drop the dependent objects too.".into()),
                    ));
                }
                notices.push(format!("text search configuration {name}"));
                dependent.push(config.clone());
            }
        }
        crate::statement_context::notice_drop_cascades(&notices);
        for config in dependent {
            self.remove_text_search_configuration(config)?;
        }
        for (_, (_, dictionary)) in dictionaries {
            self.remove_text_search_dictionary(dictionary)?;
        }
        Ok(SqlResult::command("DROP TEXT SEARCH DICTIONARY"))
    }

    fn save_text_search_dictionary(
        &mut self,
        dictionary: &AuthoredDictionary,
        previous: Option<AuthoredDictionary>,
    ) -> Result<()> {
        authored::store_dictionary(self.db_mut()?, dictionary)?;
        if self.tx.is_some() {
            self.ddl_undo.push(DdlUndo::RestoreTextSearchDictionary {
                oid: dictionary.oid,
                previous: previous.map(Box::new),
            });
        }
        Ok(())
    }

    pub(crate) fn remove_text_search_dictionary(
        &mut self,
        dictionary: AuthoredDictionary,
    ) -> Result<()> {
        authored::delete_dictionary(self.db_mut()?, dictionary.oid)?;
        if self.tx.is_some() {
            self.ddl_undo.push(DdlUndo::RestoreTextSearchDictionary {
                oid: dictionary.oid,
                previous: Some(Box::new(dictionary)),
            });
        }
        Ok(())
    }
}

pub(super) fn dictionary_dependencies(config: &AuthoredConfig, candidates: &BTreeSet<u32>) -> bool {
    config.mappings.as_ref().is_some_and(|mappings| {
        mappings
            .values()
            .flatten()
            .any(|oid| candidates.contains(oid))
    })
}

fn require_free_name(catalog: &Catalog, schema: &str, name: &str) -> Result<()> {
    if catalog.dictionary_in_schema(schema, name).is_some() {
        return Err(SqlError::coded(
            "23505",
            "duplicate key value violates unique constraint \"pg_ts_dict_dictname_index\"",
            Some(format!(
                "Key (dictname, dictnamespace)=({name}, {}) already exists.",
                namespace_oid(schema)
            )),
            None,
        ));
    }
    Ok(())
}

type Options = Vec<(String, Option<String>)>;

fn parse_options(tokens: Tokens<'_>) -> Result<Options> {
    let [Token::LParen, mut tokens @ ..] = tokens else {
        return Err(syntax_error(tokens.first()));
    };
    let mut options = Vec::new();
    loop {
        let (name, rest) = ident(tokens)?;
        let (value, rest) = if let [Token::Eq, tail @ ..] = rest {
            let (value, more) = match tail {
                [Token::SingleQuotedString(value), more @ ..] => (value.clone(), more),
                [Token::Number(value, _), more @ ..] => (value.clone(), more),
                _ => object_name_from_tokens(tail)?,
            };
            (Some(value), more)
        } else {
            (None, rest)
        };
        options.push((name, value));
        match rest {
            [Token::Comma, tail @ ..] => tokens = tail,
            [Token::RParen] => return Ok(options),
            _ => return Err(syntax_error(rest.first())),
        }
    }
}

fn apply_options(
    dictionary: &mut AuthoredDictionary,
    options: Options,
    creating: bool,
) -> Result<()> {
    for (name, value) in options {
        let existing = dictionary
            .options
            .iter()
            .position(|(option, _)| option == &name);
        match (existing, value) {
            (Some(_), Some(_)) if creating => {
                return Err(SqlError::invalid_parameter_value(format!(
                    "multiple \"{name}\" parameters"
                )))
            }
            (Some(index), Some(value)) => dictionary.options[index].1 = value,
            (Some(index), None) => {
                dictionary.options.remove(index);
            }
            (None, Some(value)) => dictionary.options.push((name, value)),
            (None, None) if creating => {
                return Err(SqlError::data_exception_public(
                    "42601",
                    format!("{name} requires a parameter"),
                    None,
                ))
            }
            (None, None) => {}
        }
    }
    dictionary::validate(dictionary)
}
