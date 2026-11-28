//! Event triggers: `CREATE / ALTER / DROP EVENT TRIGGER`, `pg_event_trigger`
//! and the firing of `ddl_command_start` and `ddl_command_end` triggers.
//!
//! A DDL command's tag is derived from its text the way PostgreSQL names
//! it; commands PostgreSQL does not fire event triggers for (global objects,
//! transaction control, DML) never fire. `ddl_command_end` triggers run in
//! the command's transaction, so a trigger that raises an error undoes the
//! command, as in PostgreSQL.
//!
//! `sql_drop`, `table_rewrite` and `login` triggers cannot be fired
//! faithfully (BicDB does not collect the dropped objects or rewrite
//! reasons, and logins do not run SQL triggers), so creating one is refused
//! with 0A000 rather than creating a trigger that never fires. For the same
//! reason `pg_event_trigger_ddl_commands()` refuses inside a trigger.
use crate::*;
use sqlparser::tokenizer::{Token, Tokenizer};

pub(crate) const EVENT_TRIGGER_COLLECTION: &str = "__bicdb_pg_event_triggers";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct EventTriggerSchema {
    pub(crate) name: String,
    pub(crate) oid: i64,
    pub(crate) event: String,
    pub(crate) owner: String,
    /// The trigger function's routine name, as the catalog keys it.
    pub(crate) function: String,
    pub(crate) function_oid: i64,
    /// `pg_event_trigger.evtenabled`: O (origin and local), D, R or A.
    pub(crate) enabled: char,
    /// Upper-case command tags of `WHEN TAG IN (...)`.
    #[serde(default)]
    pub(crate) tags: Option<Vec<String>>,
}

/// Command tags PostgreSQL fires `ddl_command_*` event triggers for.
const EVENT_TRIGGER_TAGS: &[&str] = &[
    "ALTER ACCESS METHOD",
    "ALTER AGGREGATE",
    "ALTER CAST",
    "ALTER COLLATION",
    "ALTER CONSTRAINT",
    "ALTER CONVERSION",
    "ALTER DEFAULT PRIVILEGES",
    "ALTER DOMAIN",
    "ALTER EXTENSION",
    "ALTER FOREIGN DATA WRAPPER",
    "ALTER FOREIGN TABLE",
    "ALTER FUNCTION",
    "ALTER INDEX",
    "ALTER LANGUAGE",
    "ALTER LARGE OBJECT",
    "ALTER MATERIALIZED VIEW",
    "ALTER OPERATOR",
    "ALTER OPERATOR CLASS",
    "ALTER OPERATOR FAMILY",
    "ALTER POLICY",
    "ALTER PROCEDURE",
    "ALTER PUBLICATION",
    "ALTER ROUTINE",
    "ALTER RULE",
    "ALTER SCHEMA",
    "ALTER SEQUENCE",
    "ALTER SERVER",
    "ALTER STATISTICS",
    "ALTER SUBSCRIPTION",
    "ALTER TABLE",
    "ALTER TEXT SEARCH CONFIGURATION",
    "ALTER TEXT SEARCH DICTIONARY",
    "ALTER TEXT SEARCH PARSER",
    "ALTER TEXT SEARCH TEMPLATE",
    "ALTER TRANSFORM",
    "ALTER TRIGGER",
    "ALTER TYPE",
    "ALTER USER MAPPING",
    "ALTER VIEW",
    "COMMENT",
    "CREATE ACCESS METHOD",
    "CREATE AGGREGATE",
    "CREATE CAST",
    "CREATE COLLATION",
    "CREATE CONSTRAINT",
    "CREATE CONVERSION",
    "CREATE DOMAIN",
    "CREATE EXTENSION",
    "CREATE FOREIGN DATA WRAPPER",
    "CREATE FOREIGN TABLE",
    "CREATE FUNCTION",
    "CREATE INDEX",
    "CREATE LANGUAGE",
    "CREATE MATERIALIZED VIEW",
    "CREATE OPERATOR",
    "CREATE OPERATOR CLASS",
    "CREATE OPERATOR FAMILY",
    "CREATE POLICY",
    "CREATE PROCEDURE",
    "CREATE PUBLICATION",
    "CREATE ROUTINE",
    "CREATE RULE",
    "CREATE SCHEMA",
    "CREATE SEQUENCE",
    "CREATE SERVER",
    "CREATE STATISTICS",
    "CREATE SUBSCRIPTION",
    "CREATE TABLE",
    "CREATE TABLE AS",
    "CREATE TEXT SEARCH CONFIGURATION",
    "CREATE TEXT SEARCH DICTIONARY",
    "CREATE TEXT SEARCH PARSER",
    "CREATE TEXT SEARCH TEMPLATE",
    "CREATE TRANSFORM",
    "CREATE TRIGGER",
    "CREATE TYPE",
    "CREATE USER MAPPING",
    "CREATE VIEW",
    "DROP ACCESS METHOD",
    "DROP AGGREGATE",
    "DROP CAST",
    "DROP COLLATION",
    "DROP CONSTRAINT",
    "DROP CONVERSION",
    "DROP DOMAIN",
    "DROP EXTENSION",
    "DROP FOREIGN DATA WRAPPER",
    "DROP FOREIGN TABLE",
    "DROP FUNCTION",
    "DROP INDEX",
    "DROP LANGUAGE",
    "DROP MATERIALIZED VIEW",
    "DROP OPERATOR",
    "DROP OPERATOR CLASS",
    "DROP OPERATOR FAMILY",
    "DROP OWNED",
    "DROP POLICY",
    "DROP PROCEDURE",
    "DROP PUBLICATION",
    "DROP ROUTINE",
    "DROP RULE",
    "DROP SCHEMA",
    "DROP SEQUENCE",
    "DROP SERVER",
    "DROP STATISTICS",
    "DROP SUBSCRIPTION",
    "DROP TABLE",
    "DROP TEXT SEARCH CONFIGURATION",
    "DROP TEXT SEARCH DICTIONARY",
    "DROP TEXT SEARCH PARSER",
    "DROP TEXT SEARCH TEMPLATE",
    "DROP TRANSFORM",
    "DROP TRIGGER",
    "DROP TYPE",
    "DROP USER MAPPING",
    "DROP VIEW",
    "GRANT",
    "IMPORT FOREIGN SCHEMA",
    "LOGIN",
    "REFRESH MATERIALIZED VIEW",
    "REINDEX",
    "REVOKE",
    "SECURITY LABEL",
    "SELECT INTO",
];

/// Command tags that exist but never fire event triggers.
const OTHER_COMMAND_TAGS: &[&str] = &[
    "ALTER DATABASE",
    "ALTER EVENT TRIGGER",
    "ALTER ROLE",
    "ALTER SYSTEM",
    "ALTER TABLESPACE",
    "ANALYZE",
    "BEGIN",
    "CALL",
    "CHECKPOINT",
    "CLOSE",
    "CLOSE CURSOR",
    "CLOSE CURSOR ALL",
    "CLUSTER",
    "COMMIT",
    "COMMIT PREPARED",
    "COPY",
    "COPY FROM",
    "CREATE DATABASE",
    "CREATE EVENT TRIGGER",
    "CREATE ROLE",
    "CREATE TABLESPACE",
    "DEALLOCATE",
    "DEALLOCATE ALL",
    "DECLARE CURSOR",
    "DELETE",
    "DISCARD",
    "DISCARD ALL",
    "DISCARD PLANS",
    "DISCARD SEQUENCES",
    "DISCARD TEMP",
    "DO",
    "DROP DATABASE",
    "DROP EVENT TRIGGER",
    "DROP ROLE",
    "DROP TABLESPACE",
    "EXECUTE",
    "EXPLAIN",
    "FETCH",
    "GRANT ROLE",
    "INSERT",
    "LISTEN",
    "LOAD",
    "LOCK TABLE",
    "MERGE",
    "MOVE",
    "NOTIFY",
    "PREPARE",
    "PREPARE TRANSACTION",
    "REASSIGN OWNED",
    "RELEASE",
    "RESET",
    "REVOKE ROLE",
    "ROLLBACK",
    "ROLLBACK PREPARED",
    "SAVEPOINT",
    "SELECT",
    "SELECT FOR KEY SHARE",
    "SELECT FOR NO KEY UPDATE",
    "SELECT FOR SHARE",
    "SELECT FOR UPDATE",
    "SET",
    "SET CONSTRAINTS",
    "SHOW",
    "START TRANSACTION",
    "TRUNCATE TABLE",
    "UNLISTEN",
    "UPDATE",
    "VACUUM",
];

fn event_trigger_error(code: &'static str, message: impl Into<String>) -> SqlError {
    SqlError::data_exception(code, message.into(), None)
}

fn undefined_event_trigger(name: &str) -> SqlError {
    event_trigger_error("42704", format!("event trigger \"{name}\" does not exist"))
}

fn syntax_error_near(token: Option<&Token>) -> SqlError {
    match token {
        Some(token) => event_trigger_error("42601", format!("syntax error at or near \"{token}\"")),
        None => event_trigger_error("42601", "syntax error at end of input"),
    }
}

thread_local! {
    static CATALOG: RefCell<Option<(u64, u64, Arc<Vec<EventTriggerSchema>>)>> =
        const { RefCell::new(None) };
    /// The event being fired on this thread, while a trigger function runs
    /// (for the `pg_event_trigger_*` functions).
    static FIRING: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) };
}

/// The event triggers of `db`, in name order.
pub(crate) fn list_event_triggers(db: &BicDb) -> Result<Arc<Vec<EventTriggerSchema>>> {
    let database = db.instance_id();
    let generation = db.collection_generation(EVENT_TRIGGER_COLLECTION);
    if let Some(list) = CATALOG.with(|cache| {
        cache.borrow().as_ref().and_then(|(id, cached, list)| {
            (*id == database && *cached == generation).then(|| list.clone())
        })
    }) {
        return Ok(list);
    }
    let records = match db.scan_collection(EVENT_TRIGGER_COLLECTION) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => Vec::new(),
        Err(error) => return Err(error.into()),
    };
    let mut list = records
        .into_iter()
        .map(|record| serde_json::from_value(record.metadata).map_err(SqlError::from))
        .collect::<Result<Vec<EventTriggerSchema>>>()?;
    list.sort_by(|left, right| left.name.cmp(&right.name));
    let list = Arc::new(list);
    CATALOG.with(|cache| *cache.borrow_mut() = Some((database, generation, list.clone())));
    Ok(list)
}

fn save_event_trigger(db: &mut BicDb, trigger: &EventTriggerSchema) -> Result<()> {
    db.create_collection(EVENT_TRIGGER_COLLECTION)?;
    db.insert(
        EVENT_TRIGGER_COLLECTION,
        Record::new(trigger.oid.to_string()).with_metadata(serde_json::to_value(trigger)?),
    )?;
    Ok(())
}

fn delete_event_trigger(db: &mut BicDb, oid: i64) -> Result<()> {
    match db.delete(EVENT_TRIGGER_COLLECTION, &oid.to_string()) {
        Ok(_) | Err(BicDbError::CollectionNotFound(_)) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn restore_event_trigger(
    db: &mut BicDb,
    oid: i64,
    previous: Option<&EventTriggerSchema>,
) -> Result<()> {
    match previous {
        Some(trigger) => save_event_trigger(db, trigger),
        None => delete_event_trigger(db, oid),
    }
}

/// `pg_event_trigger` rows.
pub(crate) fn pg_event_trigger_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    Ok(list_event_triggers(db)?
        .iter()
        .map(|trigger| {
            virtual_row([
                ("oid", SqlValue::Int(trigger.oid)),
                ("evtname", SqlValue::String(trigger.name.clone())),
                ("evtevent", SqlValue::String(trigger.event.clone())),
                ("evtowner", SqlValue::Int(role_oid(&trigger.owner))),
                ("evtfoid", SqlValue::Int(trigger.function_oid)),
                ("evtenabled", SqlValue::String(trigger.enabled.to_string())),
                (
                    "evttags",
                    match &trigger.tags {
                        Some(tags) => {
                            crate::array_value(tags.iter().cloned().map(SqlValue::String).collect())
                        }
                        None => SqlValue::Null,
                    },
                ),
            ])
        })
        .collect())
}

pub(crate) const PG_EVENT_TRIGGER_COLUMNS: [(&str, &str); 7] = [
    ("oid", "oid"),
    ("evtname", "name"),
    ("evtevent", "name"),
    ("evtowner", "oid"),
    ("evtfoid", "oid"),
    ("evtenabled", "char"),
    ("evttags", "text[]"),
];

/// Upper-case words of the statement's leading tokens (identifiers and
/// keywords, with `(` and `)` as markers), enough to name its command.
fn leading_words(sql: &str, limit: usize) -> Vec<String> {
    let Ok(tokens) = Tokenizer::new(&PostgreSqlDialect {}, sql).tokenize() else {
        return Vec::new();
    };
    let mut words = Vec::new();
    for token in tokens {
        match token {
            Token::Whitespace(_) => continue,
            Token::Word(word) if word.quote_style.is_none() => {
                words.push(word.value.to_ascii_uppercase())
            }
            Token::LParen => words.push("(".to_string()),
            Token::RParen => words.push(")".to_string()),
            Token::SemiColon | Token::EOF => break,
            _ => words.push(String::new()),
        }
        if words.len() >= limit {
            break;
        }
    }
    words
}

/// Whether `word` appears at parenthesis depth 0 after `start`.
fn has_top_level_word(words: &[String], start: usize, word: &str) -> bool {
    let mut depth = 0usize;
    for candidate in words.iter().skip(start) {
        match candidate.as_str() {
            "(" => depth += 1,
            ")" => depth = depth.saturating_sub(1),
            other if depth == 0 && other == word => return true,
            _ => {}
        }
    }
    false
}

/// The longest event-trigger tag that `words` (already starting with the
/// verb) begin with.
fn longest_tag(words: &[&str]) -> Option<&'static str> {
    EVENT_TRIGGER_TAGS
        .iter()
        .filter(|tag| {
            let parts = tag.split(' ').collect::<Vec<_>>();
            parts.len() <= words.len() && parts.iter().zip(words).all(|(left, right)| left == right)
        })
        .max_by_key(|tag| tag.len())
        .copied()
}

/// The command tag of `sql` when PostgreSQL fires `ddl_command_*` event
/// triggers for it.
pub(crate) fn event_trigger_command_tag(sql: &str) -> Option<&'static str> {
    let words = leading_words(sql, 512);
    let first = words.first()?.as_str();
    match first {
        "CREATE" => {
            let mut rest = words[1..].iter().map(String::as_str).collect::<Vec<_>>();
            if rest.starts_with(&["OR", "REPLACE"]) {
                rest.drain(..2);
            }
            while matches!(
                rest.first(),
                Some(
                    &"TEMP"
                        | &"TEMPORARY"
                        | &"UNLOGGED"
                        | &"GLOBAL"
                        | &"LOCAL"
                        | &"UNIQUE"
                        | &"TRUSTED"
                        | &"PROCEDURAL"
                        | &"DEFAULT"
                        | &"RECURSIVE"
                )
            ) {
                rest.remove(0);
            }
            if rest.starts_with(&["CONSTRAINT", "TRIGGER"]) {
                rest.remove(0);
            }
            if matches!(rest.first(), Some(&"USER" | &"GROUP" | &"ROLE"))
                && rest.get(1) != Some(&"MAPPING")
            {
                return None;
            }
            let mut candidate = vec!["CREATE"];
            candidate.extend(rest.iter().copied());
            let tag = longest_tag(&candidate)?;
            if tag == "CREATE TABLE"
                && (has_top_level_word(&words, 3, "AS") || has_top_level_word(&words, 3, "EXECUTE"))
            {
                return Some("CREATE TABLE AS");
            }
            Some(tag)
        }
        "ALTER" => {
            let rest = words.iter().map(String::as_str).collect::<Vec<_>>();
            if matches!(rest.get(1), Some(&"USER" | &"GROUP" | &"ROLE"))
                && rest.get(2) != Some(&"MAPPING")
            {
                return None;
            }
            if rest.get(1) == Some(&"PROCEDURAL") {
                let mut adjusted = rest.clone();
                adjusted.remove(1);
                return longest_tag(&adjusted);
            }
            longest_tag(&rest)
        }
        "DROP" => {
            let rest = words.iter().map(String::as_str).collect::<Vec<_>>();
            if matches!(rest.get(1), Some(&"USER" | &"GROUP" | &"ROLE"))
                && rest.get(2) != Some(&"MAPPING")
            {
                return None;
            }
            if rest.get(1) == Some(&"PROCEDURAL") {
                let mut adjusted = rest.clone();
                adjusted.remove(1);
                return longest_tag(&adjusted);
            }
            longest_tag(&rest)
        }
        "COMMENT" | "SECURITY" => {
            // Comments and labels on shared objects do not fire.
            let object = words
                .iter()
                .skip_while(|word| *word != "ON")
                .nth(1)
                .map(String::as_str);
            if matches!(
                object,
                Some("DATABASE" | "ROLE" | "TABLESPACE" | "EVENT" | "SUBSCRIPTION")
            ) {
                return None;
            }
            if first == "COMMENT" {
                Some("COMMENT")
            } else {
                (words.get(1).map(String::as_str) == Some("LABEL")).then_some("SECURITY LABEL")
            }
        }
        "GRANT" | "REVOKE" => {
            // Role membership grants and privileges on shared objects
            // do not fire.
            let on = words.iter().position(|word| word == "ON")?;
            if matches!(
                words.get(on + 1).map(String::as_str),
                Some("DATABASE" | "TABLESPACE" | "PARAMETER")
            ) {
                return None;
            }
            Some(if first == "GRANT" { "GRANT" } else { "REVOKE" })
        }
        "IMPORT" => {
            (words.get(1).map(String::as_str) == Some("FOREIGN")).then_some("IMPORT FOREIGN SCHEMA")
        }
        "REFRESH" => (words.get(1).map(String::as_str) == Some("MATERIALIZED"))
            .then_some("REFRESH MATERIALIZED VIEW"),
        "REINDEX" => {
            let target = words
                .iter()
                .skip(1)
                .find(|word| !word.is_empty() && word.as_str() != "(")
                .map(String::as_str);
            (!matches!(target, Some("DATABASE" | "SYSTEM"))).then_some("REINDEX")
        }
        "SELECT" => {
            let mut depth = 0usize;
            for word in words.iter().skip(1) {
                match word.as_str() {
                    "(" => depth += 1,
                    ")" => depth = depth.saturating_sub(1),
                    "FROM" | "WHERE" | "UNION" | "GROUP" | "ORDER" | "LIMIT" if depth == 0 => {
                        return None
                    }
                    "INTO" if depth == 0 => return Some("SELECT INTO"),
                    _ => {}
                }
            }
            None
        }
        _ => None,
    }
}

impl SqlSession<'_> {
    fn require_event_trigger_owner(&self, trigger: &EventTriggerSchema) -> Result<()> {
        if self.current_user_is_superuser()? || self.current_user_holds_role(&trigger.owner)? {
            return Ok(());
        }
        Err(SqlError::coded(
            "42501",
            format!("must be owner of event trigger {}", trigger.name),
            None,
            None,
        ))
    }

    fn record_event_trigger_undo(&mut self, oid: i64) -> Result<()> {
        if self.tx.is_some() {
            let previous = list_event_triggers(self.db_ref())?
                .iter()
                .find(|trigger| trigger.oid == oid)
                .cloned();
            self.ddl_undo.push(DdlUndo::RestoreEventTrigger {
                oid,
                previous: previous.map(Box::new),
            });
        }
        Ok(())
    }

    fn store_event_trigger(&mut self, trigger: &EventTriggerSchema) -> Result<()> {
        self.record_event_trigger_undo(trigger.oid)?;
        save_event_trigger(self.db_mut()?, trigger)
    }

    fn remove_event_trigger(&mut self, oid: i64) -> Result<()> {
        self.record_event_trigger_undo(oid)?;
        delete_event_trigger(self.db_mut()?, oid)
    }

    fn find_event_trigger(&self, name: &str) -> Result<Option<EventTriggerSchema>> {
        Ok(list_event_triggers(self.db_ref())?
            .iter()
            .find(|trigger| trigger.name == name)
            .cloned())
    }

    pub(crate) fn execute_raw_event_trigger_ddl(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let words = leading_words(sql, 4);
        let verb = match words.first().map(String::as_str) {
            Some(verb @ ("CREATE" | "ALTER" | "DROP" | "COMMENT")) => verb.to_string(),
            _ => return Ok(None),
        };
        let is_event_trigger = |offset: usize| {
            words.get(offset).map(String::as_str) == Some("EVENT")
                && words.get(offset + 1).map(String::as_str) == Some("TRIGGER")
        };
        let matches = match verb.as_str() {
            "COMMENT" => words.get(1).map(String::as_str) == Some("ON") && is_event_trigger(2),
            _ => is_event_trigger(1),
        };
        if !matches {
            return Ok(None);
        }
        let mut tokens = Tokenizer::new(&PostgreSqlDialect {}, sql)
            .tokenize()
            .map_err(|error| event_trigger_error("42601", error.to_string()))?
            .into_iter()
            .filter(|token| !matches!(token, Token::Whitespace(_) | Token::EOF))
            .collect::<Vec<_>>();
        while tokens.last() == Some(&Token::SemiColon) {
            tokens.pop();
        }
        let mut cursor = EventTokens {
            tokens,
            position: 0,
        };
        let result = match verb.as_str() {
            "CREATE" => {
                cursor.skip(3);
                self.create_event_trigger(&mut cursor)?
            }
            "ALTER" => {
                cursor.skip(3);
                self.alter_event_trigger(&mut cursor)?
            }
            "DROP" => {
                cursor.skip(3);
                self.drop_event_trigger(&mut cursor)?
            }
            _ => {
                cursor.skip(4);
                self.comment_on_event_trigger(&mut cursor)?
            }
        };
        Ok(Some(result))
    }

    fn create_event_trigger(&mut self, cursor: &mut EventTokens) -> Result<SqlResult> {
        let name = cursor.ident()?;
        cursor.expect_keyword("ON")?;
        let event = cursor.ident()?;
        let mut filters: Vec<(String, Vec<String>)> = Vec::new();
        if cursor.keyword("WHEN") {
            loop {
                let variable = cursor.ident()?;
                cursor.expect_keyword("IN")?;
                cursor.expect(&Token::LParen)?;
                let mut values = Vec::new();
                loop {
                    match cursor.next() {
                        Some(Token::SingleQuotedString(value)) => values.push(value),
                        other => return Err(syntax_error_near(other.as_ref())),
                    }
                    if !cursor.consume(&Token::Comma) {
                        break;
                    }
                }
                cursor.expect(&Token::RParen)?;
                filters.push((variable, values));
                if !cursor.keyword("AND") {
                    break;
                }
            }
        }
        cursor.expect_keyword("EXECUTE")?;
        if !(cursor.keyword("FUNCTION") || cursor.keyword("PROCEDURE")) {
            return Err(syntax_error_near(cursor.peek()));
        }
        let mut function_parts = vec![cursor.ident()?];
        while cursor.consume(&Token::Period) {
            function_parts.push(cursor.ident()?);
        }
        cursor.expect(&Token::LParen)?;
        if !cursor.consume(&Token::RParen) {
            return Err(syntax_error_near(cursor.peek()));
        }
        cursor.end()?;

        if !self.current_user_is_superuser()? {
            return Err(SqlError::coded(
                "42501",
                format!("permission denied to create event trigger \"{name}\""),
                None,
                Some("Must be superuser to create an event trigger.".to_string()),
            ));
        }
        if !matches!(
            event.as_str(),
            "ddl_command_start" | "ddl_command_end" | "sql_drop" | "login" | "table_rewrite"
        ) {
            return Err(event_trigger_error(
                "42601",
                format!("unrecognized event name \"{event}\""),
            ));
        }
        let mut tags = None;
        for (variable, values) in filters {
            if variable != "tag" {
                return Err(event_trigger_error(
                    "42601",
                    format!("unrecognized filter variable \"{variable}\""),
                ));
            }
            if tags.is_some() {
                return Err(event_trigger_error(
                    "42601",
                    "filter variable \"tag\" specified more than once",
                ));
            }
            tags = Some(values);
        }
        if let Some(values) = &tags {
            match event.as_str() {
                "ddl_command_start" | "ddl_command_end" | "sql_drop" => {
                    for value in values {
                        let upper = value.to_ascii_uppercase();
                        if !EVENT_TRIGGER_TAGS.contains(&upper.as_str()) {
                            if OTHER_COMMAND_TAGS.contains(&upper.as_str()) {
                                return Err(SqlError::Unsupported(format!(
                                    "event triggers are not supported for {value}"
                                )));
                            }
                            return Err(SqlError::data_exception(
                                "42601",
                                format!(
                                    "filter value \"{value}\" not recognized for filter variable \"tag\""
                                ),
                                None,
                            ));
                        }
                    }
                }
                "table_rewrite" => {
                    for value in values {
                        let upper = value.to_ascii_uppercase();
                        if !matches!(
                            upper.as_str(),
                            "ALTER TABLE" | "ALTER MATERIALIZED VIEW" | "ALTER TYPE"
                        ) {
                            if EVENT_TRIGGER_TAGS.contains(&upper.as_str())
                                || OTHER_COMMAND_TAGS.contains(&upper.as_str())
                            {
                                return Err(SqlError::Unsupported(format!(
                                    "event triggers are not supported for {value}"
                                )));
                            }
                            return Err(SqlError::data_exception(
                                "42601",
                                format!(
                                    "filter value \"{value}\" not recognized for filter variable \"tag\""
                                ),
                                None,
                            ));
                        }
                    }
                }
                _ => {
                    return Err(SqlError::Unsupported(
                        "tag filtering is not supported for login event triggers".to_string(),
                    ))
                }
            }
        }
        if self.find_event_trigger(&name)?.is_some() {
            return Err(event_trigger_error(
                "42710",
                format!("event trigger \"{name}\" already exists"),
            ));
        }
        let function = function_parts.join(".");
        let routine_name = function_parts.last().cloned().unwrap_or_default();
        let routine = resolve_routine_cached(self.db_ref(), RoutineKind::Function, &function)?;
        let Some(routine) = routine else {
            let builtin = crate::builtin_signatures::PG_CATALOG_FUNCTION_SIGNATURES
                .iter()
                .any(|(candidate, arguments)| *candidate == routine_name && arguments.is_empty());
            if builtin {
                return Err(event_trigger_error(
                    "42P17",
                    format!("function {function} must return type event_trigger"),
                ));
            }
            return Err(SqlError::undefined_function(format!(
                "function {function}() does not exist"
            )));
        };
        if !routine
            .schema
            .return_type
            .eq_ignore_ascii_case("event_trigger")
        {
            return Err(event_trigger_error(
                "42P17",
                format!("function {function} must return type event_trigger"),
            ));
        }
        if matches!(event.as_str(), "sql_drop" | "table_rewrite" | "login") {
            return Err(SqlError::Unsupported(format!(
                "event triggers on {event} are not supported"
            )));
        }
        let oid = allocate_user_type_oids(self.db_mut()?, 1)?[0];
        let trigger = EventTriggerSchema {
            name,
            oid,
            event,
            owner: current_user_from_gucs(&self.session_gucs),
            function: routine.schema.name.clone(),
            function_oid: catalog_routine_oid(&routine.schema),
            enabled: 'O',
            tags: tags.map(|values| {
                values
                    .into_iter()
                    .map(|value| value.to_ascii_uppercase())
                    .collect()
            }),
        };
        self.store_event_trigger(&trigger)?;
        Ok(SqlResult::command("CREATE EVENT TRIGGER"))
    }

    fn alter_event_trigger(&mut self, cursor: &mut EventTokens) -> Result<SqlResult> {
        let name = cursor.ident()?;
        let mut trigger = self
            .find_event_trigger(&name)?
            .ok_or_else(|| undefined_event_trigger(&name))?;
        if cursor.keyword("DISABLE") {
            cursor.end()?;
            self.require_event_trigger_owner(&trigger)?;
            trigger.enabled = 'D';
        } else if cursor.keyword("ENABLE") {
            trigger.enabled = if cursor.keyword("REPLICA") {
                'R'
            } else if cursor.keyword("ALWAYS") {
                'A'
            } else {
                'O'
            };
            cursor.end()?;
            self.require_event_trigger_owner(&trigger)?;
        } else if cursor.keyword("RENAME") {
            cursor.expect_keyword("TO")?;
            let new_name = cursor.ident()?;
            cursor.end()?;
            self.require_event_trigger_owner(&trigger)?;
            if self.find_event_trigger(&new_name)?.is_some() {
                return Err(event_trigger_error(
                    "42710",
                    format!("event trigger \"{new_name}\" already exists"),
                ));
            }
            trigger.name = new_name;
        } else if cursor.keyword("OWNER") {
            cursor.expect_keyword("TO")?;
            let owner = if cursor.keyword("CURRENT_USER") || cursor.keyword("CURRENT_ROLE") {
                current_user_from_gucs(&self.session_gucs)
            } else if cursor.keyword("SESSION_USER") {
                session_user_from_gucs(&self.session_gucs)
            } else {
                normalize_role_name(&cursor.ident()?)
            };
            cursor.end()?;
            ensure_known_role(&list_roles(self.db_ref())?, &owner)?;
            if trigger.owner == owner {
                return Ok(SqlResult::command("ALTER EVENT TRIGGER"));
            }
            self.require_event_trigger_owner(&trigger)?;
            let superuser = owner == BOOTSTRAP_ROLE_NAME
                || load_role_schema(self.db_ref(), &owner)?.is_some_and(|role| role.superuser);
            if !superuser {
                return Err(SqlError::coded(
                    "42501",
                    format!(
                        "permission denied to change owner of event trigger \"{}\"",
                        trigger.name
                    ),
                    None,
                    Some("The owner of an event trigger must be a superuser.".to_string()),
                ));
            }
            trigger.owner = owner;
        } else {
            return Err(syntax_error_near(cursor.peek()));
        }
        self.store_event_trigger(&trigger)?;
        Ok(SqlResult::command("ALTER EVENT TRIGGER"))
    }

    fn drop_event_trigger(&mut self, cursor: &mut EventTokens) -> Result<SqlResult> {
        let if_exists = if cursor.keyword("IF") {
            cursor.expect_keyword("EXISTS")?;
            true
        } else {
            false
        };
        let name = cursor.ident()?;
        if !cursor.keyword("CASCADE") {
            cursor.keyword("RESTRICT");
        }
        cursor.end()?;
        let Some(trigger) = self.find_event_trigger(&name)? else {
            if if_exists {
                crate::statement_context::notice_skipping_missing("event trigger", &name);
                return Ok(SqlResult::command("DROP EVENT TRIGGER"));
            }
            return Err(undefined_event_trigger(&name));
        };
        self.require_event_trigger_owner(&trigger)?;
        self.remove_event_trigger(trigger.oid)?;
        Ok(SqlResult::command("DROP EVENT TRIGGER"))
    }

    fn comment_on_event_trigger(&mut self, cursor: &mut EventTokens) -> Result<SqlResult> {
        let name = cursor.ident()?;
        cursor.expect_keyword("IS")?;
        match cursor.next() {
            Some(Token::SingleQuotedString(_)) => {}
            Some(Token::Word(word)) if word.value.eq_ignore_ascii_case("null") => {}
            other => return Err(syntax_error_near(other.as_ref())),
        }
        cursor.end()?;
        let trigger = self
            .find_event_trigger(&name)?
            .ok_or_else(|| undefined_event_trigger(&name))?;
        self.require_event_trigger_owner(&trigger)?;
        Ok(SqlResult::command("COMMENT"))
    }

    /// Whether `role` owns an event trigger (DROP ROLE).
    pub(crate) fn role_owns_event_trigger(&self, role: &str) -> Result<bool> {
        Ok(list_event_triggers(self.db_ref())?
            .iter()
            .any(|trigger| trigger.owner.eq_ignore_ascii_case(role)))
    }

    /// The enabled triggers of `event` that fire for `tag` under the
    /// session's `session_replication_role`, in name order.
    fn event_triggers_for(&self, event: &str, tag: &str) -> Result<Vec<EventTriggerSchema>> {
        let triggers = list_event_triggers(self.db_ref())?;
        if triggers.is_empty() {
            return Ok(Vec::new());
        }
        let replica = self
            .session_gucs
            .get("session_replication_role")
            .is_some_and(|role| role.eq_ignore_ascii_case("replica"));
        Ok(triggers
            .iter()
            .filter(|trigger| trigger.event == event)
            .filter(|trigger| match trigger.enabled {
                'A' => true,
                'R' => replica,
                'O' => !replica,
                _ => false,
            })
            .filter(|trigger| {
                trigger
                    .tags
                    .as_ref()
                    .is_none_or(|tags| tags.iter().any(|candidate| candidate == tag))
            })
            .cloned()
            .collect())
    }

    fn fire_event_triggers(
        &mut self,
        event: &'static str,
        tag: &str,
        triggers: &[EventTriggerSchema],
    ) -> Result<()> {
        for trigger in triggers {
            let vars = vec![
                ("tg_event".to_string(), SqlValue::String(event.to_string())),
                ("tg_tag".to_string(), SqlValue::String(tag.to_string())),
            ];
            FIRING.with(|firing| firing.borrow_mut().push(event));
            let depth = std::mem::replace(&mut self.event_trigger_ddl_depth, 0);
            let result = self.call_trigger_function(
                &trigger.name,
                &trigger.function,
                &vars,
                &super::row_triggers::TriggerRecordShape::default(),
                None,
            );
            self.event_trigger_ddl_depth = depth;
            FIRING.with(|firing| {
                firing.borrow_mut().pop();
            });
            result?;
        }
        Ok(())
    }

    /// Runs `command` (a statement with event-trigger tag `tag`) with the
    /// `ddl_command_start` and `ddl_command_end` triggers that match it.
    pub(crate) fn with_ddl_event_triggers(
        &mut self,
        tag: Option<&'static str>,
        command: impl FnOnce(&mut Self) -> Result<SqlResult>,
    ) -> Result<SqlResult> {
        let Some(tag) = tag.filter(|_| self.event_trigger_ddl_depth == 0) else {
            return command(self);
        };
        if list_event_triggers(self.db_ref())?.is_empty() {
            return command(self);
        }
        let start = self.event_triggers_for("ddl_command_start", tag)?;
        let end = self.event_triggers_for("ddl_command_end", tag)?;
        if start.is_empty() && end.is_empty() {
            return command(self);
        }
        self.fire_event_triggers("ddl_command_start", tag, &start)?;
        self.event_trigger_ddl_depth += 1;
        let result = if end.is_empty() {
            command(self)
        } else {
            // The end triggers run in the command's transaction: an error
            // they raise undoes the command.
            self.with_statement_transaction(|session| {
                let result = command(session)?;
                session.event_trigger_ddl_depth -= 1;
                let fired = session.fire_event_triggers("ddl_command_end", tag, &end);
                session.event_trigger_ddl_depth += 1;
                fired.map(|()| result)
            })
        };
        self.event_trigger_ddl_depth -= 1;
        result
    }
}

/// The event an event trigger function is running for on this thread.
pub(crate) fn current_trigger_event() -> Option<&'static str> {
    FIRING.with(|firing| firing.borrow().last().copied())
}

/// `pg_event_trigger_ddl_commands()`, `pg_event_trigger_dropped_objects()`,
/// `pg_event_trigger_table_rewrite_oid()` and `..._reason()`.
pub(crate) fn event_trigger_function_error(name: &str) -> Option<SqlError> {
    let event = current_trigger_event();
    let protocol =
        |message: &str| Some(SqlError::data_exception("39P03", message.to_string(), None));
    match name {
        "pg_event_trigger_ddl_commands" => match event {
            Some("ddl_command_end") => Some(SqlError::Unsupported(
                "pg_event_trigger_ddl_commands() is not supported".to_string(),
            )),
            _ => protocol("pg_event_trigger_ddl_commands() can only be called in an event trigger function"),
        },
        "pg_event_trigger_dropped_objects" => {
            protocol("pg_event_trigger_dropped_objects() can only be called in a sql_drop event trigger function")
        }
        "pg_event_trigger_table_rewrite_oid" => protocol(
            "pg_event_trigger_table_rewrite_oid() can only be called in a table_rewrite event trigger function",
        ),
        "pg_event_trigger_table_rewrite_reason" => protocol(
            "pg_event_trigger_table_rewrite_reason() can only be called in a table_rewrite event trigger function",
        ),
        _ => None,
    }
}

struct EventTokens {
    tokens: Vec<Token>,
    position: usize,
}

impl EventTokens {
    fn skip(&mut self, count: usize) {
        self.position = (self.position + count).min(self.tokens.len());
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

    fn keyword(&mut self, keyword: &str) -> bool {
        let found = matches!(self.peek(), Some(Token::Word(word))
            if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(keyword));
        if found {
            self.position += 1;
        }
        found
    }

    fn expect_keyword(&mut self, keyword: &str) -> Result<()> {
        if self.keyword(keyword) {
            Ok(())
        } else {
            Err(syntax_error_near(self.peek()))
        }
    }

    fn consume(&mut self, token: &Token) -> bool {
        let found = self.peek() == Some(token);
        if found {
            self.position += 1;
        }
        found
    }

    fn expect(&mut self, token: &Token) -> Result<()> {
        if self.consume(token) {
            Ok(())
        } else {
            Err(syntax_error_near(self.peek()))
        }
    }

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
            other => Err(syntax_error_near(other)),
        }
    }

    fn end(&mut self) -> Result<()> {
        match self.peek() {
            None => Ok(()),
            other => Err(syntax_error_near(other)),
        }
    }
}
