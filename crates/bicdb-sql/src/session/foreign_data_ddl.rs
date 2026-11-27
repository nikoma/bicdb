//! CREATE / ALTER / DROP FOREIGN DATA WRAPPER, SERVER and USER MAPPING,
//! GRANT / REVOKE USAGE on them, COMMENT ON them and IMPORT FOREIGN SCHEMA.
//! The SQL parser reads none of these, so they are parsed here from tokens.
use crate::foreign_data::{
    acl_grant, acl_new_owner, acl_revoke, apply_option_changes, best_grantor, load_foreign_catalog,
    role_has_usage, role_is_owner, save_foreign_catalog, AclItem, ForeignCatalog,
    ForeignDataWrapper, ForeignServer, OptionAction, OptionChange, OptionContext, UserMapping,
    POSTGRESQL_FDW_VALIDATOR,
};
use crate::*;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::tokenizer::{Location, Token, Tokenizer, Whitespace};

/// A cursor over one statement's tokens, whitespace and comments dropped.
struct Tokens {
    tokens: Vec<(Token, Location)>,
    position: usize,
}

impl Tokens {
    fn new(sql: &str) -> Result<Self> {
        let dialect = PostgreSqlDialect {};
        let spanned = Tokenizer::new(&dialect, sql)
            .tokenize_with_location()
            .map_err(|error| SqlError::InvalidSql(format!("sql parser error: {error}")))?;
        let tokens = spanned
            .into_iter()
            .filter(|token| {
                !matches!(
                    token.token,
                    Token::Whitespace(
                        Whitespace::Space
                            | Whitespace::Tab
                            | Whitespace::Newline
                            | Whitespace::SingleLineComment { .. }
                            | Whitespace::MultiLineComment(_)
                    )
                )
            })
            .map(|token| (token.token, token.span.start))
            .collect();
        Ok(Self {
            tokens,
            position: 0,
        })
    }

    fn peek(&self) -> &Token {
        self.tokens
            .get(self.position)
            .map_or(&Token::EOF, |(token, _)| token)
    }

    fn peek_at(&self, offset: usize) -> &Token {
        self.tokens
            .get(self.position + offset)
            .map_or(&Token::EOF, |(token, _)| token)
    }

    fn advance(&mut self) {
        self.position += 1;
    }

    fn word_at(&self, offset: usize, keyword: &str) -> bool {
        matches!(self.peek_at(offset), Token::Word(word)
            if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(keyword))
    }

    fn peek_keyword(&self, keyword: &str) -> bool {
        self.word_at(0, keyword)
    }

    fn eat_keyword(&mut self, keyword: &str) -> bool {
        if self.peek_keyword(keyword) {
            self.advance();
            return true;
        }
        false
    }

    /// Consumes `keywords` only if all of them come next.
    fn eat_keywords(&mut self, keywords: &[&str]) -> bool {
        if keywords
            .iter()
            .enumerate()
            .all(|(offset, keyword)| self.word_at(offset, keyword))
        {
            self.position += keywords.len();
            return true;
        }
        false
    }

    fn expect_keyword(&mut self, keyword: &str) -> Result<()> {
        if self.eat_keyword(keyword) {
            return Ok(());
        }
        Err(self.error())
    }

    fn eat(&mut self, token: &Token) -> bool {
        if self.peek() == token {
            self.advance();
            return true;
        }
        false
    }

    fn expect(&mut self, token: &Token) -> Result<()> {
        if self.eat(token) {
            return Ok(());
        }
        Err(self.error())
    }

    fn at_end(&self) -> bool {
        match self.peek() {
            Token::EOF => true,
            Token::SemiColon => matches!(self.peek_at(1), Token::EOF),
            _ => false,
        }
    }

    fn expect_end(&self) -> Result<()> {
        if self.at_end() {
            return Ok(());
        }
        Err(self.error())
    }

    /// `syntax error at or near "<token>"` (or `at end of input`) at the
    /// current token, with its position.
    fn error(&self) -> SqlError {
        match self.tokens.get(self.position) {
            None | Some((Token::EOF, _)) => SqlError::InvalidSql(
                "sql parser error: Expected: more input, found: EOF".to_string(),
            ),
            Some((token, location)) => SqlError::InvalidSql(format!(
                "sql parser error: Expected: valid syntax, found: {token} at Line: {}, Column: {}",
                location.line, location.column
            )),
        }
    }

    /// An identifier: unquoted folds to lower case.
    fn identifier(&mut self) -> Result<String> {
        match self.peek().clone() {
            Token::Word(word) => {
                self.advance();
                Ok(if word.quote_style.is_some() {
                    word.value
                } else {
                    word.value.to_ascii_lowercase()
                })
            }
            _ => Err(self.error()),
        }
    }

    /// A possibly qualified name, as its parts.
    fn qualified_name(&mut self) -> Result<Vec<String>> {
        let mut parts = vec![self.identifier()?];
        while self.eat(&Token::Period) {
            parts.push(self.identifier()?);
        }
        Ok(parts)
    }

    fn string_literal(&mut self) -> Result<String> {
        match self.peek().clone() {
            Token::SingleQuotedString(text) | Token::EscapedStringLiteral(text) => {
                self.advance();
                Ok(text)
            }
            _ => Err(self.error()),
        }
    }

    /// `name [, ...]`.
    fn identifier_list(&mut self) -> Result<Vec<String>> {
        let mut names = vec![self.identifier()?];
        while self.eat(&Token::Comma) {
            names.push(self.identifier()?);
        }
        Ok(names)
    }

    /// `OPTIONS ( [ADD | SET | DROP] name ['value'] [, ...] )`; the actions
    /// are part of the ALTER forms only.
    fn options(&mut self, alter: bool) -> Result<Vec<OptionChange>> {
        self.expect(&Token::LParen)?;
        let mut changes = Vec::new();
        loop {
            let mut action = OptionAction::Add;
            if alter && matches!(self.peek_at(1), Token::Word(_)) {
                if self.eat_keyword("add") {
                    action = OptionAction::Add;
                } else if self.eat_keyword("set") {
                    action = OptionAction::Set;
                } else if self.eat_keyword("drop") {
                    action = OptionAction::Drop;
                }
            }
            let name = self.identifier()?;
            let value = if action == OptionAction::Drop {
                None
            } else {
                Some(self.string_literal()?)
            };
            changes.push(OptionChange {
                action,
                name,
                value,
            });
            if !self.eat(&Token::Comma) {
                break;
            }
        }
        self.expect(&Token::RParen)?;
        if !alter {
            let mut seen = BTreeSet::new();
            for change in &changes {
                if !seen.insert(change.name.clone()) {
                    return Err(SqlError::data_exception_public(
                        "42710",
                        format!("option \"{}\" provided more than once", change.name),
                        None,
                    ));
                }
            }
        }
        Ok(changes)
    }

    /// A role specification: a role name, CURRENT_USER, CURRENT_ROLE,
    /// SESSION_USER, USER or (when allowed) PUBLIC. `None` is PUBLIC.
    fn role_spec(&mut self, allow_public: bool) -> Result<RoleSpec> {
        if let Token::Word(word) = self.peek().clone() {
            if word.quote_style.is_none() {
                let spec = match word.value.to_ascii_lowercase().as_str() {
                    "current_user" | "current_role" | "user" => Some(RoleSpec::CurrentUser),
                    "session_user" => Some(RoleSpec::SessionUser),
                    "public" if allow_public => Some(RoleSpec::Public),
                    _ => None,
                };
                if let Some(spec) = spec {
                    self.advance();
                    return Ok(spec);
                }
            }
        }
        Ok(RoleSpec::Name(self.identifier()?))
    }
}

#[derive(Clone, Debug)]
enum RoleSpec {
    Public,
    CurrentUser,
    SessionUser,
    Name(String),
}

fn conflicting_options() -> SqlError {
    SqlError::data_exception_public("42601", "conflicting or redundant options", None)
}

fn wrapper_missing(name: &str) -> SqlError {
    SqlError::undefined_object(format!("foreign-data wrapper \"{name}\" does not exist"))
}

fn server_missing(name: &str) -> SqlError {
    SqlError::undefined_object(format!("server \"{name}\" does not exist"))
}

fn permission_denied(message: String, hint: Option<&str>) -> SqlError {
    SqlError::coded("42501", message, None, hint.map(str::to_string))
}

fn must_own_wrapper(name: &str) -> SqlError {
    permission_denied(
        format!("must be owner of foreign-data wrapper {name}"),
        None,
    )
}

fn must_own_server(name: &str) -> SqlError {
    permission_denied(format!("must be owner of foreign server {name}"), None)
}

fn no_wrapper_usage(name: &str) -> SqlError {
    permission_denied(
        format!("permission denied for foreign-data wrapper {name}"),
        None,
    )
}

fn no_server_usage(name: &str) -> SqlError {
    permission_denied(format!("permission denied for foreign server {name}"), None)
}

fn mapping_name(mapping: &UserMapping) -> String {
    format!(
        "user mapping for {} on server {}",
        mapping.role_display(),
        mapping.server
    )
}

/// `HANDLER f | NO HANDLER | VALIDATOR f | NO VALIDATOR`, each at most once.
#[derive(Default)]
struct FunctionOptions {
    handler: Option<Option<Vec<String>>>,
    validator: Option<Option<Vec<String>>>,
}

impl FunctionOptions {
    fn parse(tokens: &mut Tokens) -> Result<Self> {
        let mut options = Self::default();
        loop {
            let (is_handler, function) = if tokens.eat_keywords(&["no", "handler"]) {
                (true, None)
            } else if tokens.eat_keywords(&["no", "validator"]) {
                (false, None)
            } else if tokens.eat_keyword("handler") {
                (true, Some(tokens.qualified_name()?))
            } else if tokens.eat_keyword("validator") {
                (false, Some(tokens.qualified_name()?))
            } else {
                return Ok(options);
            };
            let slot = if is_handler {
                &mut options.handler
            } else {
                &mut options.validator
            };
            if slot.is_some() {
                return Err(conflicting_options());
            }
            *slot = Some(function);
        }
    }

    fn is_empty(&self) -> bool {
        self.handler.is_none() && self.validator.is_none()
    }
}

impl SqlSession<'_> {
    fn current_role(&self) -> String {
        current_user_from_gucs(&self.session_gucs)
    }

    fn resolve_role_spec(&self, spec: &RoleSpec) -> Option<String> {
        match spec {
            RoleSpec::Public => None,
            RoleSpec::CurrentUser => Some(current_user_from_gucs(&self.session_gucs)),
            RoleSpec::SessionUser => Some(session_user_from_gucs(&self.session_gucs)),
            RoleSpec::Name(name) => Some(name.clone()),
        }
    }

    fn role_exists_named(&self, role: &str) -> Result<bool> {
        Ok(role == BOOTSTRAP_ROLE_NAME || load_role_schema(self.db_ref(), role)?.is_some())
    }

    fn require_role_exists(&self, role: &str) -> Result<()> {
        if self.role_exists_named(role)? {
            return Ok(());
        }
        Err(SqlError::UndefinedRole {
            name: role.to_string(),
        })
    }

    /// Saves `catalog`, keeping the previous one for ROLLBACK.
    fn store_foreign_catalog(&mut self, catalog: ForeignCatalog) -> Result<()> {
        if self.tx.is_some() {
            let previous = load_foreign_catalog(self.db_ref())?;
            self.ddl_undo.push(DdlUndo::RestoreForeignCatalog {
                previous: Box::new(previous.as_ref().clone()),
            });
        }
        save_foreign_catalog(self.db_mut()?, &catalog)
    }

    /// The handler a wrapper names: no BicDB function can return
    /// `fdw_handler`, so naming one is refused rather than recorded.
    fn check_fdw_handler(&self, name: &[String]) -> Result<()> {
        let display = name.join(".");
        let bare = name.last().cloned().unwrap_or_default();
        let routines = routines_named(self.db_ref(), RoutineKind::Function, &bare)?;
        let Some(routine) = routines.iter().find(|routine| routine.args.is_empty()) else {
            return Err(SqlError::undefined_function(format!(
                "function {display}() does not exist"
            )));
        };
        if !routine
            .formatted_return_type()
            .eq_ignore_ascii_case("fdw_handler")
        {
            return Err(SqlError::data_exception_public(
                "42809",
                format!("function {display} must return type fdw_handler"),
                None,
            ));
        }
        Err(SqlError::Unsupported(
            "foreign-data wrapper handlers are not supported".to_string(),
        ))
    }

    /// The validator a wrapper names: only the built-in
    /// `postgresql_fdw_validator` exists.
    fn check_fdw_validator(&self, name: &[String]) -> Result<String> {
        let bare = name.last().cloned().unwrap_or_default();
        let builtin = bare == POSTGRESQL_FDW_VALIDATOR
            && (name.len() == 1 || (name.len() == 2 && name[0] == "pg_catalog"));
        if builtin {
            return Ok(POSTGRESQL_FDW_VALIDATOR.to_string());
        }
        let routines = routines_named(self.db_ref(), RoutineKind::Function, &bare)?;
        if routines.iter().any(|routine| routine.args.len() == 2) {
            return Err(SqlError::Unsupported(
                "user-defined foreign-data wrapper validators are not supported".to_string(),
            ));
        }
        Err(SqlError::undefined_function(format!(
            "function {}(text[], oid) does not exist",
            name.join(".")
        )))
    }

    /// Entry point: `Some` when `sql` is one of these statements.
    pub(crate) fn execute_raw_foreign_data(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let head = sql.trim_start();
        let first = head
            .split(|ch: char| !ch.is_ascii_alphabetic())
            .next()
            .unwrap_or_default();
        if !["create", "alter", "drop", "grant", "revoke", "import"]
            .iter()
            .any(|verb| first.eq_ignore_ascii_case(verb))
        {
            return Ok(None);
        }
        let mut tokens = Tokens::new(sql)?;
        if tokens.eat_keywords(&["create", "foreign", "data", "wrapper"]) {
            return self.create_wrapper(&mut tokens).map(Some);
        }
        if tokens.eat_keywords(&["alter", "foreign", "data", "wrapper"]) {
            return self.alter_wrapper(&mut tokens).map(Some);
        }
        if tokens.eat_keywords(&["drop", "foreign", "data", "wrapper"]) {
            return self.drop_wrappers(&mut tokens).map(Some);
        }
        if tokens.eat_keywords(&["create", "server"]) {
            return self.create_server(&mut tokens).map(Some);
        }
        if tokens.eat_keywords(&["alter", "server"]) {
            return self.alter_server(&mut tokens).map(Some);
        }
        if tokens.eat_keywords(&["drop", "server"]) {
            return self.drop_servers(&mut tokens).map(Some);
        }
        if tokens.eat_keywords(&["create", "user", "mapping"]) {
            return self.create_user_mapping(&mut tokens).map(Some);
        }
        if tokens.eat_keywords(&["alter", "user", "mapping"]) {
            return self.alter_user_mapping(&mut tokens).map(Some);
        }
        if tokens.eat_keywords(&["drop", "user", "mapping"]) {
            return self.drop_user_mapping(&mut tokens).map(Some);
        }
        if tokens.eat_keywords(&["import", "foreign", "schema"]) {
            return self.import_foreign_schema(&mut tokens).map(Some);
        }
        if tokens.peek_keyword("grant") || tokens.peek_keyword("revoke") {
            return self.foreign_privileges(&mut tokens);
        }
        Ok(None)
    }

    fn create_wrapper(&mut self, tokens: &mut Tokens) -> Result<SqlResult> {
        let name = tokens.identifier()?;
        let functions = FunctionOptions::parse(tokens)?;
        let options = if tokens.eat_keyword("options") {
            tokens.options(false)?
        } else {
            Vec::new()
        };
        tokens.expect_end()?;
        if !self.current_user_is_superuser()? {
            return Err(permission_denied(
                format!("permission denied to create foreign-data wrapper \"{name}\""),
                Some("Must be superuser to create a foreign-data wrapper."),
            ));
        }
        let mut catalog = load_foreign_catalog(self.db_ref())?.as_ref().clone();
        if catalog.wrapper(&name).is_some() {
            return Err(SqlError::data_exception_public(
                "42710",
                format!("foreign-data wrapper \"{name}\" already exists"),
                None,
            ));
        }
        if let Some(Some(handler)) = &functions.handler {
            self.check_fdw_handler(handler)?;
        }
        let validator = match &functions.validator {
            Some(Some(validator)) => Some(self.check_fdw_validator(validator)?),
            _ => None,
        };
        let options =
            apply_option_changes(&[], &options, OptionContext::Wrapper, validator.as_deref())?;
        let oid = catalog.allocate_oid("wrapper");
        catalog.wrappers.push(ForeignDataWrapper {
            name,
            oid,
            owner: self.current_role(),
            validator,
            options,
            acl: None,
            comment: None,
        });
        self.store_foreign_catalog(catalog)?;
        Ok(SqlResult::command("CREATE FOREIGN DATA WRAPPER"))
    }

    fn alter_wrapper(&mut self, tokens: &mut Tokens) -> Result<SqlResult> {
        let name = tokens.identifier()?;
        let mut catalog = load_foreign_catalog(self.db_ref())?.as_ref().clone();
        if tokens.eat_keywords(&["owner", "to"]) {
            let owner = tokens.role_spec(false)?;
            tokens.expect_end()?;
            let owner = self.resolve_role_spec(&owner).unwrap_or_default();
            let Some(wrapper) = catalog.wrapper(&name) else {
                return Err(wrapper_missing(&name));
            };
            if wrapper.owner != owner {
                if !self.current_user_is_superuser()? {
                    return Err(permission_denied(
                        format!(
                            "permission denied to change owner of foreign-data wrapper \"{name}\""
                        ),
                        Some("Must be superuser to change owner of a foreign-data wrapper."),
                    ));
                }
                self.require_role_exists(&owner)?;
                if !pg_role_is_superuser(self.db_ref(), &owner)? {
                    return Err(permission_denied(
                        format!(
                            "permission denied to change owner of foreign-data wrapper \"{name}\""
                        ),
                        Some("The owner of a foreign-data wrapper must be a superuser."),
                    ));
                }
                let wrapper = catalog.wrapper_mut(&name).expect("wrapper found above");
                let previous = std::mem::replace(&mut wrapper.owner, owner.clone());
                acl_new_owner(&mut wrapper.acl, &previous, &owner);
                self.store_foreign_catalog(catalog)?;
            }
            return Ok(SqlResult::command("ALTER FOREIGN DATA WRAPPER"));
        }
        if tokens.eat_keywords(&["rename", "to"]) {
            let new_name = tokens.identifier()?;
            tokens.expect_end()?;
            let Some(wrapper) = catalog.wrapper(&name) else {
                return Err(wrapper_missing(&name));
            };
            if catalog.wrapper(&new_name).is_some() {
                return Err(SqlError::data_exception_public(
                    "42710",
                    format!("foreign-data wrapper \"{new_name}\" already exists"),
                    None,
                ));
            }
            if !role_is_owner(self.db_ref(), &self.current_role(), &wrapper.owner)? {
                return Err(must_own_wrapper(&name));
            }
            for server in &mut catalog.servers {
                if server.wrapper == name {
                    server.wrapper = new_name.clone();
                }
            }
            catalog
                .wrapper_mut(&name)
                .expect("wrapper found above")
                .name = new_name;
            self.store_foreign_catalog(catalog)?;
            return Ok(SqlResult::command("ALTER FOREIGN DATA WRAPPER"));
        }
        let functions = FunctionOptions::parse(tokens)?;
        let options = if tokens.eat_keyword("options") {
            Some(tokens.options(true)?)
        } else {
            None
        };
        if functions.is_empty() && options.is_none() {
            return Err(tokens.error());
        }
        tokens.expect_end()?;
        if !self.current_user_is_superuser()? {
            return Err(permission_denied(
                format!("permission denied to alter foreign-data wrapper \"{name}\""),
                Some("Must be superuser to alter a foreign-data wrapper."),
            ));
        }
        let Some(wrapper) = catalog.wrapper(&name).cloned() else {
            return Err(wrapper_missing(&name));
        };
        if let Some(Some(handler)) = &functions.handler {
            self.check_fdw_handler(handler)?;
        }
        let validator = match &functions.validator {
            Some(Some(validator)) => Some(self.check_fdw_validator(validator)?),
            Some(None) => None,
            None => wrapper.validator.clone(),
        };
        if functions.handler.is_some() {
            crate::statement_context::report_notice(
                Some("WARNING"),
                "01000",
                "changing the foreign-data wrapper handler can change behavior of existing foreign tables",
            );
        }
        if matches!(functions.validator, Some(Some(_))) {
            crate::statement_context::report_notice(
                Some("WARNING"),
                "01000",
                "changing the foreign-data wrapper validator can cause the options for dependent objects to become invalid",
            );
        }
        let options = match &options {
            Some(changes) => apply_option_changes(
                &wrapper.options,
                changes,
                OptionContext::Wrapper,
                validator.as_deref(),
            )?,
            None => wrapper.options.clone(),
        };
        let target = catalog.wrapper_mut(&name).expect("wrapper found above");
        target.validator = validator;
        target.options = options;
        self.store_foreign_catalog(catalog)?;
        Ok(SqlResult::command("ALTER FOREIGN DATA WRAPPER"))
    }

    /// `[IF EXISTS] name [, ...] [CASCADE | RESTRICT]`.
    fn drop_target_list(tokens: &mut Tokens) -> Result<(bool, Vec<String>, bool)> {
        let if_exists = tokens.eat_keywords(&["if", "exists"]);
        let names = tokens.identifier_list()?;
        let cascade = if tokens.eat_keyword("cascade") {
            true
        } else {
            tokens.eat_keyword("restrict");
            false
        };
        tokens.expect_end()?;
        Ok((if_exists, names, cascade))
    }

    fn drop_wrappers(&mut self, tokens: &mut Tokens) -> Result<SqlResult> {
        let (if_exists, names, cascade) = Self::drop_target_list(tokens)?;
        let mut catalog = load_foreign_catalog(self.db_ref())?.as_ref().clone();
        let mut dropped = Vec::new();
        for name in names {
            let Some(wrapper) = catalog.wrapper(&name) else {
                if if_exists {
                    crate::statement_context::notice_skipping_missing(
                        "foreign-data wrapper",
                        &name,
                    );
                    continue;
                }
                return Err(wrapper_missing(&name));
            };
            if !role_is_owner(self.db_ref(), &self.current_role(), &wrapper.owner)? {
                return Err(must_own_wrapper(&name));
            }
            dropped.push(name);
        }
        let mut dependents = Vec::new();
        let mut details = Vec::new();
        for name in &dropped {
            for server in catalog
                .servers
                .iter()
                .filter(|server| server.wrapper == *name)
            {
                dependents.push(format!("server {}", server.name));
                details.push(format!(
                    "server {} depends on foreign-data wrapper {name}",
                    server.name
                ));
                for mapping in catalog
                    .mappings
                    .iter()
                    .filter(|mapping| mapping.server == server.name)
                {
                    dependents.push(mapping_name(mapping));
                    details.push(format!(
                        "{} depends on server {}",
                        mapping_name(mapping),
                        server.name
                    ));
                }
            }
        }
        if !dependents.is_empty() && !cascade {
            return Err(SqlError::coded(
                "2BP01",
                format!(
                    "cannot drop foreign-data wrapper {} because other objects depend on it",
                    dropped[0]
                ),
                Some(details.join("\n")),
                Some("Use DROP ... CASCADE to drop the dependent objects too.".to_string()),
            ));
        }
        if dropped.is_empty() {
            return Ok(SqlResult::command("DROP FOREIGN DATA WRAPPER"));
        }
        crate::statement_context::notice_drop_cascades(&dependents);
        let servers = catalog
            .servers
            .iter()
            .filter(|server| dropped.contains(&server.wrapper))
            .map(|server| server.name.clone())
            .collect::<Vec<_>>();
        catalog
            .mappings
            .retain(|mapping| !servers.contains(&mapping.server));
        catalog
            .servers
            .retain(|server| !dropped.contains(&server.wrapper));
        catalog
            .wrappers
            .retain(|wrapper| !dropped.contains(&wrapper.name));
        self.store_foreign_catalog(catalog)?;
        Ok(SqlResult::command("DROP FOREIGN DATA WRAPPER"))
    }

    fn create_server(&mut self, tokens: &mut Tokens) -> Result<SqlResult> {
        let if_not_exists = tokens.eat_keywords(&["if", "not", "exists"]);
        let name = tokens.identifier()?;
        let server_type = if tokens.eat_keyword("type") {
            Some(tokens.string_literal()?)
        } else {
            None
        };
        let version = if tokens.eat_keyword("version") {
            Some(tokens.string_literal()?)
        } else {
            None
        };
        if !tokens.eat_keywords(&["foreign", "data", "wrapper"]) {
            return Err(tokens.error());
        }
        let wrapper_name = tokens.identifier()?;
        let options = if tokens.eat_keyword("options") {
            tokens.options(false)?
        } else {
            Vec::new()
        };
        tokens.expect_end()?;
        let mut catalog = load_foreign_catalog(self.db_ref())?.as_ref().clone();
        if catalog.server(&name).is_some() {
            if if_not_exists {
                crate::statement_context::report_notice(
                    None,
                    "42710",
                    format!("server \"{name}\" already exists, skipping"),
                );
                return Ok(SqlResult::command("CREATE SERVER"));
            }
            return Err(SqlError::data_exception_public(
                "42710",
                format!("server \"{name}\" already exists"),
                None,
            ));
        }
        let Some(wrapper) = catalog.wrapper(&wrapper_name).cloned() else {
            return Err(wrapper_missing(&wrapper_name));
        };
        let owner = self.current_role();
        if !role_has_usage(self.db_ref(), &owner, &wrapper.acl, &wrapper.owner)? {
            return Err(no_wrapper_usage(&wrapper.name));
        }
        let options = apply_option_changes(
            &[],
            &options,
            OptionContext::Server,
            wrapper.validator.as_deref(),
        )?;
        let oid = catalog.allocate_oid("server");
        catalog.servers.push(ForeignServer {
            name,
            oid,
            owner,
            wrapper: wrapper.name,
            server_type,
            version,
            options,
            acl: None,
            comment: None,
        });
        self.store_foreign_catalog(catalog)?;
        Ok(SqlResult::command("CREATE SERVER"))
    }

    fn alter_server(&mut self, tokens: &mut Tokens) -> Result<SqlResult> {
        let name = tokens.identifier()?;
        let mut catalog = load_foreign_catalog(self.db_ref())?.as_ref().clone();
        let current = self.current_role();
        if tokens.eat_keywords(&["owner", "to"]) {
            let owner = tokens.role_spec(false)?;
            tokens.expect_end()?;
            let owner = self.resolve_role_spec(&owner).unwrap_or_default();
            let Some(server) = catalog.server(&name).cloned() else {
                return Err(server_missing(&name));
            };
            if server.owner != owner {
                self.require_role_exists(&owner)?;
                if !self.current_user_is_superuser()? {
                    if !role_is_owner(self.db_ref(), &current, &server.owner)? {
                        return Err(must_own_server(&name));
                    }
                    if !settable_role_closure(self.db_ref(), &current)?.contains(&owner) {
                        return Err(permission_denied(
                            format!("must be able to SET ROLE \"{owner}\""),
                            None,
                        ));
                    }
                    let wrapper = catalog
                        .wrapper(&server.wrapper)
                        .cloned()
                        .ok_or_else(|| wrapper_missing(&server.wrapper))?;
                    if !role_has_usage(self.db_ref(), &owner, &wrapper.acl, &wrapper.owner)? {
                        return Err(no_wrapper_usage(&wrapper.name));
                    }
                }
                let target = catalog.server_mut(&name).expect("server found above");
                let previous = std::mem::replace(&mut target.owner, owner.clone());
                acl_new_owner(&mut target.acl, &previous, &owner);
                self.store_foreign_catalog(catalog)?;
            }
            return Ok(SqlResult::command("ALTER SERVER"));
        }
        if tokens.eat_keywords(&["rename", "to"]) {
            let new_name = tokens.identifier()?;
            tokens.expect_end()?;
            let Some(server) = catalog.server(&name) else {
                return Err(server_missing(&name));
            };
            if catalog.server(&new_name).is_some() {
                return Err(SqlError::data_exception_public(
                    "42710",
                    format!("server \"{new_name}\" already exists"),
                    None,
                ));
            }
            if !role_is_owner(self.db_ref(), &current, &server.owner)? {
                return Err(must_own_server(&name));
            }
            for mapping in &mut catalog.mappings {
                if mapping.server == name {
                    mapping.server = new_name.clone();
                }
            }
            catalog.server_mut(&name).expect("server found above").name = new_name;
            self.store_foreign_catalog(catalog)?;
            return Ok(SqlResult::command("ALTER SERVER"));
        }
        let version = if tokens.eat_keyword("version") {
            if tokens.eat_keyword("null") {
                Some(None)
            } else {
                Some(Some(tokens.string_literal()?))
            }
        } else {
            None
        };
        let options = if tokens.eat_keyword("options") {
            Some(tokens.options(true)?)
        } else {
            None
        };
        if version.is_none() && options.is_none() {
            return Err(tokens.error());
        }
        tokens.expect_end()?;
        let Some(server) = catalog.server(&name).cloned() else {
            return Err(server_missing(&name));
        };
        if !role_is_owner(self.db_ref(), &current, &server.owner)? {
            return Err(must_own_server(&name));
        }
        let options = match &options {
            Some(changes) => {
                let validator = catalog
                    .wrapper(&server.wrapper)
                    .and_then(|wrapper| wrapper.validator.clone());
                apply_option_changes(
                    &server.options,
                    changes,
                    OptionContext::Server,
                    validator.as_deref(),
                )?
            }
            None => server.options.clone(),
        };
        let target = catalog.server_mut(&name).expect("server found above");
        if let Some(version) = version {
            target.version = version;
        }
        target.options = options;
        self.store_foreign_catalog(catalog)?;
        Ok(SqlResult::command("ALTER SERVER"))
    }

    fn drop_servers(&mut self, tokens: &mut Tokens) -> Result<SqlResult> {
        let (if_exists, names, cascade) = Self::drop_target_list(tokens)?;
        let mut catalog = load_foreign_catalog(self.db_ref())?.as_ref().clone();
        let current = self.current_role();
        let mut dropped = Vec::new();
        for name in names {
            let Some(server) = catalog.server(&name) else {
                if if_exists {
                    crate::statement_context::notice_skipping_missing("server", &name);
                    continue;
                }
                return Err(server_missing(&name));
            };
            if !role_is_owner(self.db_ref(), &current, &server.owner)? {
                return Err(must_own_server(&name));
            }
            dropped.push(name);
        }
        let dependents = catalog
            .mappings
            .iter()
            .filter(|mapping| dropped.contains(&mapping.server))
            .collect::<Vec<_>>();
        if !dependents.is_empty() && !cascade {
            let details = dependents
                .iter()
                .map(|mapping| {
                    format!(
                        "{} depends on server {}",
                        mapping_name(mapping),
                        mapping.server
                    )
                })
                .collect::<Vec<_>>();
            return Err(SqlError::coded(
                "2BP01",
                format!(
                    "cannot drop server {} because other objects depend on it",
                    dropped[0]
                ),
                Some(details.join("\n")),
                Some("Use DROP ... CASCADE to drop the dependent objects too.".to_string()),
            ));
        }
        if dropped.is_empty() {
            return Ok(SqlResult::command("DROP SERVER"));
        }
        let cascaded = dependents
            .iter()
            .map(|mapping| mapping_name(mapping))
            .collect::<Vec<_>>();
        crate::statement_context::notice_drop_cascades(&cascaded);
        catalog
            .mappings
            .retain(|mapping| !dropped.contains(&mapping.server));
        catalog
            .servers
            .retain(|server| !dropped.contains(&server.name));
        self.store_foreign_catalog(catalog)?;
        Ok(SqlResult::command("DROP SERVER"))
    }

    /// `FOR role SERVER name`, returning the mapped role (`None` is PUBLIC)
    /// and the server name.
    fn mapping_target(tokens: &mut Tokens) -> Result<(RoleSpec, String)> {
        tokens.expect_keyword("for")?;
        let role = tokens.role_spec(true)?;
        tokens.expect_keyword("server")?;
        let server = tokens.identifier()?;
        Ok((role, server))
    }

    /// PostgreSQL's `user_mapping_ddl_aclcheck`: the server's owner manages
    /// every mapping; a role with USAGE on it manages its own.
    fn check_mapping_privilege(&self, role: Option<&str>, server: &ForeignServer) -> Result<()> {
        let current = self.current_role();
        if role_is_owner(self.db_ref(), &current, &server.owner)? {
            return Ok(());
        }
        let own_mapping =
            role.is_some_and(|role| normalize_role_name(role) == normalize_role_name(&current));
        if !own_mapping {
            return Err(must_own_server(&server.name));
        }
        if !role_has_usage(self.db_ref(), &current, &server.acl, &server.owner)? {
            return Err(no_server_usage(&server.name));
        }
        Ok(())
    }

    fn create_user_mapping(&mut self, tokens: &mut Tokens) -> Result<SqlResult> {
        let if_not_exists = tokens.eat_keywords(&["if", "not", "exists"]);
        let (role, server_name) = Self::mapping_target(tokens)?;
        let options = if tokens.eat_keyword("options") {
            tokens.options(false)?
        } else {
            Vec::new()
        };
        tokens.expect_end()?;
        let role = self.resolve_role_spec(&role);
        if let Some(role) = &role {
            self.require_role_exists(role)?;
        }
        let mut catalog = load_foreign_catalog(self.db_ref())?.as_ref().clone();
        let Some(server) = catalog.server(&server_name).cloned() else {
            return Err(server_missing(&server_name));
        };
        self.check_mapping_privilege(role.as_deref(), &server)?;
        let display = role.clone().unwrap_or_else(|| "public".to_string());
        if catalog.mapping(role.as_deref(), &server_name).is_some() {
            if if_not_exists {
                crate::statement_context::report_notice(
                    None,
                    "42710",
                    format!(
                        "user mapping for \"{display}\" already exists for server \"{server_name}\", skipping"
                    ),
                );
                return Ok(SqlResult::command("CREATE USER MAPPING"));
            }
            return Err(SqlError::data_exception_public(
                "42710",
                format!(
                    "user mapping for \"{display}\" already exists for server \"{server_name}\""
                ),
                None,
            ));
        }
        let validator = catalog
            .wrapper(&server.wrapper)
            .and_then(|wrapper| wrapper.validator.clone());
        let options = apply_option_changes(
            &[],
            &options,
            OptionContext::UserMapping,
            validator.as_deref(),
        )?;
        let oid = catalog.allocate_oid("user mapping");
        catalog.mappings.push(UserMapping {
            oid,
            role,
            server: server_name,
            options,
        });
        self.store_foreign_catalog(catalog)?;
        Ok(SqlResult::command("CREATE USER MAPPING"))
    }

    fn alter_user_mapping(&mut self, tokens: &mut Tokens) -> Result<SqlResult> {
        let (role, server_name) = Self::mapping_target(tokens)?;
        tokens.expect_keyword("options")?;
        let changes = tokens.options(true)?;
        tokens.expect_end()?;
        let role = self.resolve_role_spec(&role);
        if let Some(role) = &role {
            self.require_role_exists(role)?;
        }
        let mut catalog = load_foreign_catalog(self.db_ref())?.as_ref().clone();
        let Some(server) = catalog.server(&server_name).cloned() else {
            return Err(server_missing(&server_name));
        };
        let Some(mapping) = catalog.mapping(role.as_deref(), &server_name).cloned() else {
            return Err(SqlError::undefined_object(format!(
                "user mapping for \"{}\" does not exist for server \"{server_name}\"",
                role.as_deref().unwrap_or("public")
            )));
        };
        self.check_mapping_privilege(role.as_deref(), &server)?;
        let validator = catalog
            .wrapper(&server.wrapper)
            .and_then(|wrapper| wrapper.validator.clone());
        let options = apply_option_changes(
            &mapping.options,
            &changes,
            OptionContext::UserMapping,
            validator.as_deref(),
        )?;
        catalog
            .mappings
            .retain(|candidate| candidate.oid != mapping.oid);
        catalog.mappings.push(UserMapping { options, ..mapping });
        self.store_foreign_catalog(catalog)?;
        Ok(SqlResult::command("ALTER USER MAPPING"))
    }

    fn drop_user_mapping(&mut self, tokens: &mut Tokens) -> Result<SqlResult> {
        let if_exists = tokens.eat_keywords(&["if", "exists"]);
        let (role, server_name) = Self::mapping_target(tokens)?;
        tokens.expect_end()?;
        let role = self.resolve_role_spec(&role);
        if let Some(role) = &role {
            if !self.role_exists_named(role)? {
                if if_exists {
                    crate::statement_context::report_notice(
                        None,
                        "00000",
                        format!("role \"{role}\" does not exist, skipping"),
                    );
                    return Ok(SqlResult::command("DROP USER MAPPING"));
                }
                return Err(SqlError::UndefinedRole { name: role.clone() });
            }
        }
        let mut catalog = load_foreign_catalog(self.db_ref())?.as_ref().clone();
        let Some(server) = catalog.server(&server_name).cloned() else {
            if if_exists {
                crate::statement_context::notice_skipping_missing("server", &server_name);
                return Ok(SqlResult::command("DROP USER MAPPING"));
            }
            return Err(server_missing(&server_name));
        };
        let display = role.clone().unwrap_or_else(|| "public".to_string());
        let Some(mapping) = catalog.mapping(role.as_deref(), &server_name).cloned() else {
            if if_exists {
                crate::statement_context::report_notice(
                    None,
                    "00000",
                    format!(
                        "user mapping for \"{display}\" does not exist for server \"{server_name}\", skipping"
                    ),
                );
                return Ok(SqlResult::command("DROP USER MAPPING"));
            }
            return Err(SqlError::undefined_object(format!(
                "user mapping for \"{display}\" does not exist for server \"{server_name}\""
            )));
        };
        self.check_mapping_privilege(role.as_deref(), &server)?;
        catalog
            .mappings
            .retain(|candidate| candidate.oid != mapping.oid);
        self.store_foreign_catalog(catalog)?;
        Ok(SqlResult::command("DROP USER MAPPING"))
    }

    /// `IMPORT FOREIGN SCHEMA remote [LIMIT TO | EXCEPT (t, ...)] FROM SERVER
    /// s INTO local [OPTIONS (...)]`: needs a wrapper handler, which no
    /// wrapper here has.
    fn import_foreign_schema(&mut self, tokens: &mut Tokens) -> Result<SqlResult> {
        tokens.identifier()?;
        if tokens.eat_keywords(&["limit", "to"]) || tokens.eat_keyword("except") {
            tokens.expect(&Token::LParen)?;
            tokens.identifier_list()?;
            tokens.expect(&Token::RParen)?;
        }
        if !tokens.eat_keywords(&["from", "server"]) {
            return Err(tokens.error());
        }
        let server_name = tokens.identifier()?;
        tokens.expect_keyword("into")?;
        let schema = tokens.identifier()?;
        if tokens.eat_keyword("options") {
            tokens.options(false)?;
        }
        tokens.expect_end()?;
        let catalog = load_foreign_catalog(self.db_ref())?;
        let Some(server) = catalog.server(&server_name) else {
            return Err(server_missing(&server_name));
        };
        if !role_has_usage(
            self.db_ref(),
            &self.current_role(),
            &server.acl,
            &server.owner,
        )? {
            return Err(no_server_usage(&server.name));
        }
        if schema != "public" && load_namespace(self.db_ref(), &schema)?.is_none() {
            return Err(SqlError::data_exception_public(
                "3F000",
                format!("schema \"{schema}\" does not exist"),
                None,
            ));
        }
        Err(SqlError::object_not_in_prerequisite_state(format!(
            "foreign-data wrapper \"{}\" has no handler",
            server.wrapper
        )))
    }

    /// `GRANT | REVOKE ... ON FOREIGN DATA WRAPPER | FOREIGN SERVER ...`;
    /// `None` for privileges on other objects.
    fn foreign_privileges(&mut self, tokens: &mut Tokens) -> Result<Option<SqlResult>> {
        let grant = tokens.eat_keyword("grant");
        if !grant {
            tokens.expect_keyword("revoke")?;
        }
        let grant_option_only = !grant && tokens.eat_keywords(&["grant", "option", "for"]);
        let start = tokens.position;
        // Find ON FOREIGN DATA WRAPPER / ON FOREIGN SERVER before parsing the
        // privilege list, so other GRANTs are left alone.
        let mut on = None;
        while !tokens.at_end() {
            if tokens.eat_keywords(&["on", "foreign", "data", "wrapper"]) {
                on = Some(true);
                break;
            }
            if tokens.eat_keywords(&["on", "foreign", "server"]) {
                on = Some(false);
                break;
            }
            if tokens.peek_keyword("on") {
                return Ok(None);
            }
            tokens.advance();
        }
        let Some(wrapper) = on else {
            return Ok(None);
        };
        let after_on = tokens.position;
        tokens.position = start;
        let kind = if wrapper {
            "foreign-data wrapper"
        } else {
            "foreign server"
        };
        loop {
            if tokens.eat_keyword("all") {
                tokens.eat_keyword("privileges");
            } else if !tokens.eat_keyword("usage") {
                let privilege = match tokens.peek() {
                    Token::Word(word) => word.value.to_ascii_uppercase(),
                    _ => return Err(tokens.error()),
                };
                return Err(SqlError::data_exception_public(
                    "0LP01",
                    format!("invalid privilege type {privilege} for {kind}"),
                    None,
                ));
            }
            if !tokens.eat(&Token::Comma) {
                break;
            }
        }
        if tokens.position != after_on - if wrapper { 4 } else { 3 } {
            return Err(tokens.error());
        }
        tokens.position = after_on;
        let names = tokens.identifier_list()?;
        tokens.expect_keyword(if grant { "to" } else { "from" })?;
        let mut grantees = Vec::new();
        loop {
            tokens.eat_keyword("group");
            grantees.push(tokens.role_spec(true)?);
            if !tokens.eat(&Token::Comma) {
                break;
            }
        }
        let with_grant_option = grant && tokens.eat_keywords(&["with", "grant", "option"]);
        if tokens.eat_keywords(&["granted", "by"]) {
            let grantor = tokens.role_spec(false)?;
            let grantor = self.resolve_role_spec(&grantor).unwrap_or_default();
            if normalize_role_name(&grantor) != normalize_role_name(&self.current_role()) {
                return Err(SqlError::Unsupported(
                    "grantor must be current user".to_string(),
                ));
            }
        }
        let cascade = !grant && tokens.eat_keyword("cascade");
        if !grant && !cascade {
            tokens.eat_keyword("restrict");
        }
        tokens.expect_end()?;
        let grantees = grantees
            .iter()
            .map(|spec| self.resolve_role_spec(spec))
            .collect::<Vec<_>>();
        for grantee in grantees.iter().flatten() {
            self.require_role_exists(grantee)?;
        }
        if with_grant_option && grantees.iter().any(Option::is_none) {
            return Err(SqlError::data_exception_public(
                "0LP01",
                "grant options can only be granted to roles",
                None,
            ));
        }
        let current = self.current_role();
        let mut catalog = load_foreign_catalog(self.db_ref())?.as_ref().clone();
        for name in &names {
            let (acl, owner) = if wrapper {
                let found = catalog.wrapper(name).ok_or_else(|| wrapper_missing(name))?;
                (found.acl.clone(), found.owner.clone())
            } else {
                let found = catalog.server(name).ok_or_else(|| server_missing(name))?;
                (found.acl.clone(), found.owner.clone())
            };
            let mut acl: Option<Vec<AclItem>> = acl;
            match best_grantor(self.db_ref(), &current, &acl, &owner)? {
                Some(grantor) => {
                    if grant {
                        acl_grant(&mut acl, &owner, &grantor, &grantees, with_grant_option);
                    } else {
                        acl_revoke(
                            &mut acl,
                            &owner,
                            &grantor,
                            &grantees,
                            grant_option_only,
                            cascade,
                        )?;
                    }
                }
                None => {
                    if !role_has_usage(self.db_ref(), &current, &acl, &owner)? {
                        return Err(if wrapper {
                            no_wrapper_usage(name)
                        } else {
                            no_server_usage(name)
                        });
                    }
                    crate::statement_context::report_notice(
                        Some("WARNING"),
                        if grant { "01007" } else { "01006" },
                        if grant {
                            format!("no privileges were granted for \"{name}\"")
                        } else {
                            format!("no privileges could be revoked for \"{name}\"")
                        },
                    );
                    continue;
                }
            }
            if wrapper {
                catalog.wrapper_mut(name).expect("wrapper found above").acl = acl;
            } else {
                catalog.server_mut(name).expect("server found above").acl = acl;
            }
        }
        self.store_foreign_catalog(catalog)?;
        Ok(Some(SqlResult::command(if grant {
            "GRANT"
        } else {
            "REVOKE"
        })))
    }

    /// `COMMENT ON FOREIGN DATA WRAPPER name IS ...` and `COMMENT ON SERVER`.
    /// `None` for comments on other objects.
    pub(crate) fn execute_foreign_data_comment(&mut self, statement: &str) -> Result<Option<()>> {
        let mut tokens = Tokens::new(statement)?;
        if !tokens.eat_keywords(&["comment", "on"]) {
            return Ok(None);
        }
        let wrapper = if tokens.eat_keywords(&["foreign", "data", "wrapper"]) {
            true
        } else if tokens.eat_keyword("server") {
            false
        } else {
            return Ok(None);
        };
        let name = tokens.identifier()?;
        tokens.expect_keyword("is")?;
        let comment = if tokens.eat_keyword("null") {
            None
        } else {
            Some(tokens.string_literal()?).filter(|comment| !comment.is_empty())
        };
        tokens.expect_end()?;
        let current = self.current_role();
        let mut catalog = load_foreign_catalog(self.db_ref())?.as_ref().clone();
        if wrapper {
            let found = catalog
                .wrapper(&name)
                .ok_or_else(|| wrapper_missing(&name))?;
            if !role_is_owner(self.db_ref(), &current, &found.owner)? {
                return Err(must_own_wrapper(&name));
            }
            if let Some(target) = catalog.wrappers.iter_mut().find(|w| w.name == name) {
                target.comment = comment;
            }
        } else {
            let found = catalog.server(&name).ok_or_else(|| server_missing(&name))?;
            if !role_is_owner(self.db_ref(), &current, &found.owner)? {
                return Err(must_own_server(&name));
            }
            if let Some(target) = catalog.servers.iter_mut().find(|s| s.name == name) {
                target.comment = comment;
            }
        }
        self.store_foreign_catalog(catalog)?;
        Ok(Some(()))
    }
}
