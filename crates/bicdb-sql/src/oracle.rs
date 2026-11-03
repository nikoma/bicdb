//! Explicit Oracle SQL profiles. They lower into native PostgreSQL
//! statements; the default parser, value codecs and transaction engine do not
//! change. This profile is not a claim of general Oracle compatibility.
use crate::*;
use sqlparser::{dialect::OracleDialect, keywords::Keyword};

mod anchored_types;
pub mod assessment;
pub(crate) mod autonomous;
pub(crate) mod character_columns;
#[cfg(test)]
mod character_tests;
pub(crate) mod collections;
mod date_trunc;
mod decode;
mod dictionary;
mod dual;
pub(crate) mod dynamic_sql;
pub(crate) mod errors;
mod function_attributes;
pub(crate) mod hierarchy;
mod namespaces;
pub(crate) mod national;
mod nvl;
mod outer_joins;
pub(crate) mod procedures;
pub(crate) mod routine_arithmetic;
pub(crate) mod routine_characters;
pub(crate) mod routines;
mod rownum;
mod source_tokens;
pub(crate) mod syntax;
pub(crate) mod temporal_columns;
pub(crate) mod text_patterns;

pub const ORACLE_HR_PROFILE: &str = "oracle-hr-v1";
pub const ORACLE_PREVIEW_PROFILE: &str = "oracle-preview-v1";
pub const POSTGRES_PROFILE: &str = "postgres";
const PROFILE_COLLECTION: &str = "__bicdb_sql_compatibility";

fn metadata_v2() -> JsonValue {
    serde_json::json!({"format":2,"profile":ORACLE_PREVIEW_PROFILE,"features":["oracle_temporal_columns_v1"]})
}

fn metadata_v3() -> JsonValue {
    serde_json::json!({"format":3,"profile":ORACLE_PREVIEW_PROFILE,"features":["oracle_temporal_columns_v1","oracle_character_columns_v1"]})
}
fn metadata_v4() -> JsonValue {
    serde_json::json!({"format":4,"profile":ORACLE_PREVIEW_PROFILE,"features":["oracle_temporal_columns_v1","oracle_character_columns_v1","oracle_hierarchical_queries_v1"]})
}
fn metadata_v5() -> JsonValue {
    serde_json::json!({"format":5,"profile":ORACLE_PREVIEW_PROFILE,"features":["oracle_temporal_columns_v1","oracle_character_columns_v1","oracle_hierarchical_queries_v1","oracle_nested_table_types_v1"]})
}
fn metadata_v5_enabled(db: &BicDb) -> Result<bool> {
    match db.get(PROFILE_COLLECTION, "profile") {
        Err(BicDbError::CollectionNotFound(_)) => Ok(false),
        Err(error) => Err(error.into()),
        Ok(record) => Ok(record.is_some_and(|r| r.metadata == metadata_v5())),
    }
}
fn enable_metadata_v5(db: &mut BicDb) -> Result<()> {
    if !preview_routine_language_enabled(db)? {
        return Err(unsupported(
            "Oracle collection types require the durable preview profile",
        ));
    }
    if !metadata_v5_enabled(db)? {
        db.insert(
            PROFILE_COLLECTION,
            Record::new("profile").with_metadata(metadata_v5()),
        )?;
    }
    Ok(())
}
pub(crate) fn prepare_hierarchy_save(db: &mut BicDb, sql: &str) -> Result<()> {
    if !preview_routine_language_enabled(db)? {
        return Ok(());
    }
    let source = tokens(sql)?;
    if !source
        .windows(2)
        .any(|w| word(&w[0], "CONNECT") && word(&w[1], "BY"))
    {
        return Ok(());
    }
    if !metadata_v5_enabled(db)? {
        db.insert(
            PROFILE_COLLECTION,
            Record::new("profile").with_metadata(metadata_v4()),
        )?;
    }
    Ok(())
}
fn metadata_v3_enabled(db: &BicDb) -> Result<bool> {
    Ok(db.get(PROFILE_COLLECTION, "profile")?.is_some_and(|r| {
        r.metadata == metadata_v3() || r.metadata == metadata_v4() || r.metadata == metadata_v5()
    }))
}
fn enable_metadata_v3(db: &mut BicDb) -> Result<()> {
    if !preview_routine_language_enabled(db)? {
        return Err(unsupported(
            "Oracle character columns require the durable preview profile",
        ));
    }
    if !metadata_v3_enabled(db)? {
        db.insert(
            PROFILE_COLLECTION,
            Record::new("profile").with_metadata(metadata_v3()),
        )?;
    }
    Ok(())
}
fn matches_profile(metadata: &JsonValue, profile: &str) -> bool {
    *metadata == serde_json::json!({"format":1,"profile":profile})
        || profile == ORACLE_PREVIEW_PROFILE
            && (*metadata == metadata_v2()
                || *metadata == metadata_v3()
                || *metadata == metadata_v4()
                || *metadata == metadata_v5())
}

fn metadata_v2_enabled(db: &BicDb) -> Result<bool> {
    match db.get(PROFILE_COLLECTION, "profile") {
        Ok(Some(record)) => Ok(record.metadata == metadata_v2()
            || record.metadata == metadata_v3()
            || record.metadata == metadata_v4()
            || record.metadata == metadata_v5()),
        Ok(None) | Err(BicDbError::CollectionNotFound(_)) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn enable_metadata_v2(db: &mut BicDb) -> Result<()> {
    if !preview_routine_language_enabled(db)? {
        return Err(unsupported(
            "Oracle temporal columns require the durable preview profile",
        ));
    }
    if !metadata_v2_enabled(db)? {
        db.insert(
            PROFILE_COLLECTION,
            Record::new("profile").with_metadata(metadata_v2()),
        )?;
    }
    Ok(())
}
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V1: &str = "bicdb_oracle_preview_v1";
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V2: &str = "bicdb_oracle_preview_v2";
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V3: &str = "bicdb_oracle_preview_v3";
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V4: &str = "bicdb_oracle_preview_v4";
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V5: &str = "bicdb_oracle_preview_v5";
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V6: &str = "bicdb_oracle_preview_v6";
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V7: &str = "bicdb_oracle_preview_v7";
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V8: &str = "bicdb_oracle_preview_v8";
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V9: &str = "bicdb_oracle_preview_v9";
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V10: &str = "bicdb_oracle_preview_v10";
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V11: &str = "bicdb_oracle_preview_v11";
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V12: &str = "bicdb_oracle_preview_v12";
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V13: &str = "bicdb_oracle_preview_v13";
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V14: &str = "bicdb_oracle_preview_v14";
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V15: &str = "bicdb_oracle_preview_v15";
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V16: &str = "bicdb_oracle_preview_v16";
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V17: &str = "bicdb_oracle_preview_v17";
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V18: &str = "bicdb_oracle_preview_v18";
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V19: &str = "bicdb_oracle_preview_v19";
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V20: &str = "bicdb_oracle_preview_v20";
pub(crate) const PREVIEW_ROUTINE_LANGUAGE: &str = "bicdb_oracle_preview_v21";
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V1_OID: i64 = 999_400;
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V2_OID: i64 = 999_401;
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V3_OID: i64 = 999_402;
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V4_OID: i64 = 999_403;
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V5_OID: i64 = 999_404;
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V6_OID: i64 = 999_405;
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V7_OID: i64 = 999_406;
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V8_OID: i64 = 999_407;
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V9_OID: i64 = 999_408;
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V10_OID: i64 = 999_409;
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V11_OID: i64 = 999_410;
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V12_OID: i64 = 999_411;
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V13_OID: i64 = 999_412;
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V14_OID: i64 = 999_413;
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V15_OID: i64 = 999_414;
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V16_OID: i64 = 999_415;
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V17_OID: i64 = 999_416;
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V18_OID: i64 = 999_417;
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V19_OID: i64 = 999_418;
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_V20_OID: i64 = 999_419;
pub(crate) const PREVIEW_ROUTINE_LANGUAGE_OID: i64 = 999_420;

pub(crate) fn preview_routine_language_version(language: &str) -> Option<u8> {
    match language
        .trim()
        .trim_matches('\'')
        .to_ascii_lowercase()
        .as_str()
    {
        PREVIEW_ROUTINE_LANGUAGE_V1 => Some(1),
        PREVIEW_ROUTINE_LANGUAGE_V2 => Some(2),
        PREVIEW_ROUTINE_LANGUAGE_V3 => Some(3),
        PREVIEW_ROUTINE_LANGUAGE_V4 => Some(4),
        PREVIEW_ROUTINE_LANGUAGE_V5 => Some(5),
        PREVIEW_ROUTINE_LANGUAGE_V6 => Some(6),
        PREVIEW_ROUTINE_LANGUAGE_V7 => Some(7),
        PREVIEW_ROUTINE_LANGUAGE_V8 => Some(8),
        PREVIEW_ROUTINE_LANGUAGE_V9 => Some(9),
        PREVIEW_ROUTINE_LANGUAGE_V10 => Some(10),
        PREVIEW_ROUTINE_LANGUAGE_V11 => Some(11),
        PREVIEW_ROUTINE_LANGUAGE_V12 => Some(12),
        PREVIEW_ROUTINE_LANGUAGE_V13 => Some(13),
        PREVIEW_ROUTINE_LANGUAGE_V14 => Some(14),
        PREVIEW_ROUTINE_LANGUAGE_V15 => Some(15),
        PREVIEW_ROUTINE_LANGUAGE_V16 => Some(16),
        PREVIEW_ROUTINE_LANGUAGE_V17 => Some(17),
        PREVIEW_ROUTINE_LANGUAGE_V18 => Some(18),
        PREVIEW_ROUTINE_LANGUAGE_V19 => Some(19),
        PREVIEW_ROUTINE_LANGUAGE_V20 => Some(20),
        PREVIEW_ROUTINE_LANGUAGE => Some(21),
        _ => None,
    }
}

pub(crate) fn typed_preview_routine_language(language: &str) -> bool {
    preview_routine_language_version(language).is_some_and(|version| version >= 3)
}

pub(crate) fn preview_routine_language_enabled(db: &BicDb) -> Result<bool> {
    match db.get(PROFILE_COLLECTION, "profile") {
        Err(BicDbError::CollectionNotFound(_)) => Ok(false),
        Err(error) => Err(error.into()),
        Ok(record) => {
            Ok(record
                .is_some_and(|record| matches_profile(&record.metadata, ORACLE_PREVIEW_PROFILE)))
        }
    }
}

pub(crate) fn validate_routine_language(db: &BicDb, language: &str) -> Result<()> {
    if preview_routine_language_version(language).is_some()
        && !preview_routine_language_enabled(db)?
    {
        return Err(unsupported(
            "Oracle preview routine language requires its durable database profile",
        ));
    }
    Ok(())
}

/// Persist the non-default profile with the database, including native backups.
/// Existing PostgreSQL databases cannot be silently converted by a serve flag.
pub fn ensure_database_profile(db: &mut BicDb, profile: &str) -> Result<()> {
    if !is_known_profile(profile) {
        return Err(unsupported(format!("unknown SQL profile {profile}")));
    }
    if db
        .collections()
        .iter()
        .any(|c| c.name == PROFILE_COLLECTION)
    {
        let marker = db
            .get(PROFILE_COLLECTION, "profile")?
            .ok_or_else(|| unsupported("SQL profile marker is missing"))?;
        if !matches_profile(&marker.metadata, profile) {
            return Err(unsupported(
                "database SQL profile does not match server configuration",
            ));
        }
        return Ok(());
    }
    if profile == POSTGRES_PROFILE {
        return Ok(());
    }
    if !list_schemas(db)?.is_empty() || db.collections().iter().any(|c| !c.name.starts_with("__")) {
        return Err(unsupported(
            "Oracle profile must be provisioned on an empty database",
        ));
    }
    db.create_collection(PROFILE_COLLECTION)?;
    db.insert(
        PROFILE_COLLECTION,
        bicdb_core::Record::new("profile")
            .with_metadata(serde_json::json!({"format":1,"profile":profile})),
    )?;
    Ok(())
}

pub fn is_oracle_profile(profile: &str) -> bool {
    matches!(profile, ORACLE_HR_PROFILE | ORACLE_PREVIEW_PROFILE)
}

pub fn is_known_profile(profile: &str) -> bool {
    profile == POSTGRES_PROFILE || is_oracle_profile(profile)
}

/// Constraint semantics follow the durable database profile, never a mutable
/// connection setting. Explicit PostgreSQL NULLS clauses can select a policy.
pub(crate) fn default_unique_null_policy(db: &BicDb) -> Result<UniqueNullPolicy> {
    match db.get(PROFILE_COLLECTION, "profile") {
        Err(BicDbError::CollectionNotFound(_)) => Ok(UniqueNullPolicy::Distinct),
        Err(error) => Err(error.into()),
        Ok(Some(record)) if matches_profile(&record.metadata, ORACLE_PREVIEW_PROFILE) => {
            Ok(UniqueNullPolicy::AllNullsDistinct)
        }
        Ok(Some(record))
            if record.metadata == serde_json::json!({"format":1,"profile":ORACLE_HR_PROFILE}) =>
        {
            Ok(UniqueNullPolicy::Distinct)
        }
        Ok(Some(record))
            if record.metadata == serde_json::json!({"format":1,"profile":POSTGRES_PROFILE}) =>
        {
            Ok(UniqueNullPolicy::Distinct)
        }
        _ => Err(unsupported("invalid or missing durable SQL profile marker")),
    }
}

/// The preview profile grows independently of the bounded HR profile. Its
/// database marker and startup opt-in prevent accidental profile switching.
pub fn lower_profile_sql(db: &BicDb, sql: &str, profile: &str) -> Result<String> {
    lower_profile_sql_in_namespace(db, sql, profile, None)
}

fn lower_profile_sql_in_namespace(
    db: &BicDb,
    sql: &str,
    profile: &str,
    namespace: Option<&str>,
) -> Result<String> {
    if !is_oracle_profile(profile) {
        return Err(unsupported(
            "Oracle front end requires an Oracle SQL profile",
        ));
    }
    let lowered = lower_oracle_sql(db, sql, profile == ORACLE_PREVIEW_PROFILE, namespace).map_err(
        |error| match error {
            SqlError::Unsupported(message) if profile == ORACLE_PREVIEW_PROFILE => {
                SqlError::Unsupported(message.replacen(ORACLE_HR_PROFILE, profile, 1))
            }
            other => other,
        },
    )?;
    if profile == ORACLE_PREVIEW_PROFILE {
        if collections::declaration(db, &lowered)?.is_some() {
            return Ok(lowered);
        }
        let lowered = date_trunc::lower(db, &lowered, None)?;
        let lowered = lower_decode(db, &lowered, None)?;
        date_trunc::lower(db, &lowered, None)
    } else {
        Ok(lowered)
    }
}

/// Resolve datetime overloads using explicit PostgreSQL Parse parameter OIDs.
pub fn lower_typed_native_sql(
    db: &BicDb,
    sql: &str,
    profile: &str,
    oids: &[i32],
) -> Result<String> {
    if profile == ORACLE_PREVIEW_PROFILE {
        if collections::declaration(db, sql)?.is_some() {
            return Ok(sql.to_string());
        }
        let lowered = date_trunc::lower(db, sql, Some(oids))?;
        let lowered = lower_decode(db, &lowered, Some(oids))?;
        date_trunc::lower(db, &lowered, Some(oids))
    } else {
        Ok(sql.to_string())
    }
}

/// Resolve preview application relations in one explicitly selected schema.
/// The bounded HR profile keeps its public-namespace contract. Native table
/// privileges still apply after qualification; selecting a schema grants none.
pub fn lower_session_sql(
    db: &BicDb,
    sql: &str,
    profile: &str,
    search_path: Option<&str>,
    user: &str,
) -> Result<String> {
    if profile != ORACLE_PREVIEW_PROFILE {
        return lower_profile_sql(db, sql, profile);
    }
    let schema = preview_schema(db, search_path, user)?.unwrap_or_else(default_schema_name);
    let lowered = lower_profile_sql_in_namespace(db, sql, profile, Some(&schema))?;
    // Anonymous blocks are raw routine source, not a sqlparser Statement.
    // Their scoped SQL is namespace-bound by the interpreter before execution.
    if collections::declaration(db, &lowered)?.is_some() {
        return Ok(lowered);
    }
    if raw_do_block_body(&lowered)?.is_some() {
        return Ok(lowered);
    }
    if let Some(function) = procedures::header(&lowered)? {
        let mut statement = Statement::CreateFunction(function);
        namespaces::qualify(&mut statement, &schema);
        let Statement::CreateFunction(function) = statement else {
            unreachable!()
        };
        return procedures::render(function);
    }
    let mut statements = parse_statements(&lowered)?;
    namespaces::qualify_with_calls(&mut statements, &schema);
    collections::lower_constructors(db, &mut statements, Some(&schema))?;
    date_trunc::rewrite(db, &mut statements)?;
    routine_arithmetic::lower_sql_builtins(db, &mut statements, None)?;
    date_trunc::rewrite(db, &mut statements)?;
    lower_query_locks(db, &mut statements)?;
    Ok(statements
        .iter()
        .map(render_sql)
        .collect::<Vec<_>>()
        .join("; "))
}

fn lower_decode(db: &BicDb, sql: &str, oids: Option<&[i32]>) -> Result<String> {
    if procedures::header(sql)?.is_some() {
        return Ok(sql.to_string());
    }
    if ![
        "decode",
        "nvl",
        "chr",
        "to_char",
        "bicdb_oracle_national_value",
        "bicdb_oracle_trim",
        "bicdb_oracle_regexp_substr",
        "bicdb_oracle_regexp_replace",
    ]
    .iter()
    .any(|name| sql.to_ascii_lowercase().contains(name))
        || raw_do_block_body(sql)?.is_some()
    {
        return Ok(sql.to_string());
    }
    let mut statements = parse_statements(sql)?;
    // CREATE FUNCTION bodies are opaque strings here. Their compiler capability
    // marker controls DECODE when the routine is compiled, including on reload.
    routine_arithmetic::lower_sql_builtins(db, &mut statements, oids)?;
    Ok(statements
        .iter()
        .map(render_sql)
        .collect::<Vec<_>>()
        .join("; "))
}

pub(super) fn lower_query_locks<T: sqlparser::ast::VisitMut>(
    db: &BicDb,
    node: &mut T,
) -> Result<()> {
    struct Locks<'a>(&'a BicDb);
    impl sqlparser::ast::VisitorMut for Locks<'_> {
        type Break = SqlError;
        fn post_visit_query(&mut self, query: &mut Query) -> ControlFlow<SqlError> {
            match lower_column_locks(self.0, query) {
                Ok(()) => ControlFlow::Continue(()),
                Err(error) => ControlFlow::Break(error),
            }
        }
    }
    if let ControlFlow::Break(error) = sqlparser::ast::VisitMut::visit(node, &mut Locks(db)) {
        return Err(error);
    }
    Ok(())
}

fn lower_column_locks(db: &BicDb, query: &mut Query) -> Result<()> {
    if !query.locks.iter().any(|lock| lock.of.is_some()) {
        return Ok(());
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(unsupported("FOR UPDATE OF requires a base-table SELECT"));
    };
    let mut sources = Vec::new();
    for from in &select.from {
        for table in
            std::iter::once(&from.relation).chain(from.joins.iter().map(|join| &join.relation))
        {
            if let TableFactor::Table {
                name,
                alias,
                args: None,
                ..
            } = table
            {
                let physical = relation_name(name)?;
                if let Some(schema) = load_schema(db, &physical)? {
                    let parts = object_name_parts(name);
                    let alias = alias
                        .as_ref()
                        .map(|a| ident_value(&a.name))
                        .unwrap_or_else(|| parts.last().unwrap().clone());
                    sources.push((parts, alias, schema));
                }
            }
        }
    }
    for lock in &mut query.locks {
        let Some(of) = lock.of.as_ref() else {
            continue;
        };
        let parts =
            of.0.iter()
                .filter_map(|part| part.as_ident())
                .map(|id| {
                    if id.quote_style.is_some() {
                        id.value.clone()
                    } else {
                        id.value.to_uppercase()
                    }
                })
                .collect::<Vec<_>>();
        let Some((column, qualifier)) = parts.split_last() else {
            return Err(unsupported("empty FOR UPDATE OF column"));
        };
        let owners = sources
            .iter()
            .filter(|(name, alias, schema)| {
                schema.column(column).is_some()
                    && (qualifier.is_empty()
                        || qualifier == [alias.clone()]
                        || name
                            .last()
                            .is_some_and(|table| qualifier == [table.clone()])
                        || qualifier == name.as_slice())
            })
            .collect::<Vec<_>>();
        if owners.len() != 1 {
            return Err(unsupported(format!(
                "FOR UPDATE OF column {of} has no unique base-table owner"
            )));
        }
        lock.of = Some(ObjectName::from(vec![Ident::with_quote('"', &owners[0].1)]));
    }
    Ok(())
}

pub(crate) fn preview_schema(db: &BicDb, path: Option<&str>, user: &str) -> Result<Option<String>> {
    if let Some(path) = path {
        let mut path_tokens = Tokenizer::new(&PostgreSqlDialect {}, path)
            .tokenize()
            .map_err(|e| unsupported(format!("invalid search_path: {e}")))?
            .into_iter()
            .filter(|t| !matches!(t, Token::Whitespace(_)))
            .peekable();
        let mut candidates = Vec::new();
        while let Some(token) = path_tokens.next() {
            let Token::Word(name) = token else {
                return Err(unsupported("invalid search_path schema name"));
            };
            candidates.push(if name.value == "$user" {
                user.to_uppercase()
            } else if name.quote_style.is_some() {
                name.value
            } else if name.value.eq_ignore_ascii_case("public") {
                "public".into()
            } else {
                name.value.to_uppercase()
            });
            if path_tokens.peek().is_some() && path_tokens.next() != Some(Token::Comma) {
                return Err(unsupported("invalid search_path separator"));
            }
        }
        if candidates.is_empty() {
            return Err(unsupported(
                "preview requires a nonempty application schema selection",
            ));
        }
        for schema in &candidates {
            if schema == "public" || load_namespace(db, schema)?.is_some() {
                return Ok(Some(schema.clone()));
            }
        }
        // Preserve an explicitly selected missing namespace: a query must fail
        // there instead of falling through into another application's tables.
        return Ok(candidates.into_iter().next());
    }
    let schema = user.to_uppercase();
    Ok(load_namespace(db, &schema)?.map(|_| schema))
}

fn unsupported(message: impl Into<String>) -> SqlError {
    SqlError::Unsupported(format!("{ORACLE_HR_PROFILE}: {}", message.into()))
}

fn word(token: &Token, value: &str) -> bool {
    matches!(token, Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(value))
}

fn tokens(sql: &str) -> Result<Vec<Token>> {
    Tokenizer::new(&OracleDialect {}, sql)
        .tokenize_with_location()
        .map(source_tokens::ranges)
        .map_err(|e| unsupported(e.to_string()))
}

fn render_token(token: &Token) -> String {
    // Tokenizer values are decoded; Token::Display deliberately prints them
    // verbatim. Re-render through the AST's quoting rules before reparsing SQL.
    match token {
        Token::SingleQuotedString(value) => format!("'{}'", value.replace('\'', "''")),
        Token::NationalStringLiteral(value) => format!("N'{}'", value.replace('\'', "''")),
        Token::Word(word) if word.quote_style == Some('"') => {
            format!("\"{}\"", word.value.replace('"', "\"\""))
        }
        _ => token.to_string(),
    }
}

/// sqlparser's generic Display accepts both escaped and decoded values and
/// treats adjacent quotes as already escaped. Our parser uses decoded values;
/// double every quote on a serialization-only copy before invoking Display.
fn render_sql<T: Clone + sqlparser::ast::VisitMut + std::fmt::Display>(node: &T) -> String {
    let mut copy = node.clone();
    let _ = visit_expressions_mut(&mut copy, |expr| {
        if let Expr::Value(value) = expr {
            match &mut value.value {
                Value::SingleQuotedString(text) | Value::NationalStringLiteral(text) => {
                    *text = text.replace('\'', "''")
                }
                _ => {}
            }
        }
        ControlFlow::<()>::Continue(())
    });
    copy.to_string()
}

fn join(tokens: &[Token]) -> String {
    tokens
        .iter()
        .map(render_token)
        .collect::<Vec<_>>()
        .join(" ")
}

/// Lower Oracle-profile source before pgwire parameter inference, Describe or
/// execution. The server fixes the profile for the lifetime of the database.
pub fn lower_hr_sql(db: &BicDb, sql: &str) -> Result<String> {
    lower_oracle_sql(db, sql, false, None)
}

fn lower_oracle_sql(
    db: &BicDb,
    sql: &str,
    preview: bool,
    namespace: Option<&str>,
) -> Result<String> {
    let mut source = tokens(sql)?;
    if source.is_empty() {
        return Ok(String::new());
    }
    // Normalize each ordinary statement independently, including physical DDL
    // suffixes. Routine/block semicolons belong to their procedural body.
    let block = (word(&source[0], "BEGIN")
        && (!preview || source.get(1) != Some(&Token::SemiColon))
        || word(&source[0], "DECLARE") && source.iter().any(|t| word(t, "BEGIN")))
        && source.iter().any(|t| word(t, "END"));
    let routine_declaration = word(&source[0], "CREATE")
        && source
            .iter()
            .take(4)
            .any(|token| word(token, "PROCEDURE") || word(token, "FUNCTION"));
    let procedural = routine_declaration || block;
    if preview && !procedural && source[..source.len() - 1].contains(&Token::SemiColon) {
        return source
            .split(|token| *token == Token::SemiColon)
            .filter(|statement| !statement.is_empty())
            .map(|statement| lower_oracle_sql(db, &join(statement), preview, namespace))
            .collect::<Result<Vec<_>>>()
            .map(|statements| statements.join("; "));
    }
    if preview {
        if let Some(sql) = collections::lower_declaration(db, &source, namespace)? {
            return Ok(sql);
        }
    }
    // PostgreSQL drivers issue this transport housekeeping command even when
    // the application uses Oracle SQL. Prepared names are protocol identifiers;
    // preserve native case rules rather than folding them as application names.
    if word(&source[0], "DEALLOCATE") {
        let statements = parse_statements(sql)?;
        if let [Statement::Deallocate { .. }] = statements.as_slice() {
            return Ok(statements[0].to_string());
        }
        return Err(unsupported("expected one DEALLOCATE statement"));
    }
    // Transaction modes belong to the PG transport grammar. Keep their tokens
    // contextual rather than reserving ISOLATION/LEVEL as Oracle identifiers.
    if preview
        && (word(&source[0], "SET") && source.get(1).is_some_and(|t| word(t, "TRANSACTION"))
            || word(&source[0], "BEGIN") && !procedural)
    {
        if parse_transaction_control_for_profile(sql, ORACLE_PREVIEW_PROFILE)?.is_some() {
            return Ok(parse_statements(sql)?[0].to_string());
        }
    }
    if source
        .iter()
        .any(|t| word(t, "PACKAGE") || !preview && word(t, "AUTONOMOUS_TRANSACTION"))
    {
        return Err(unsupported(
            "packages and HR-profile autonomous transactions are not implemented",
        ));
    }
    if !preview
        && source
            .windows(2)
            .any(|w| word(&w[0], "CONNECT") && word(&w[1], "BY"))
    {
        return Err(unsupported("hierarchical queries are not implemented"));
    }
    if word(&source[0], "ALTER") && source.get(1).is_some_and(|t| word(t, "SESSION")) {
        return Err(unsupported(
            "ALTER SESSION requires an explicit migration adaptation",
        ));
    }
    if preview
        && word(&source[0], "ALTER")
        && source.get(1).is_some_and(|token| word(token, "TABLE"))
    {
        if let Some(modify) = source.iter().position(|token| word(token, "MODIFY")) {
            let preserve_date = preview_routine_language_enabled(db)?;
            let prefix = canonical_tokens_with_routine_types(
                db,
                &source[..modify],
                true,
                preserve_date,
                namespace,
            )?;
            let tail = source[modify + 1..]
                .strip_suffix(&[Token::SemiColon])
                .unwrap_or(&source[modify + 1..]);
            let tail =
                if tail.first() == Some(&Token::LParen) && tail.last() == Some(&Token::RParen) {
                    &tail[1..tail.len() - 1]
                } else {
                    tail
                };
            let groups = split_top_level(tail);
            if groups.len() != 1 {
                return Err(unsupported(
                    "multi-column MODIFY requires atomic conversion support",
                ));
            }
            let canonical = canonical_tokens_with_routine_types(
                db,
                &groups[0],
                true,
                preserve_date,
                namespace,
            )?;
            let definitions = parse_statements(&format!(
                "CREATE TABLE source_type_probe ({})",
                join(&canonical)
            ))?;
            let [Statement::CreateTable(table)] = definitions.as_slice() else {
                return Err(unsupported("invalid MODIFY declaration"));
            };
            let [column] = table.columns.as_slice() else {
                return Err(unsupported("MODIFY requires one column declaration"));
            };
            if !column.options.is_empty() || !table.constraints.is_empty() {
                return Err(unsupported(
                    "MODIFY defaults and constraints require an explicit ALTER operation",
                ));
            }
            return lower_native(
                db,
                &format!(
                    "{} ALTER COLUMN {} TYPE {}",
                    join(&prefix),
                    column.name,
                    column.data_type
                ),
                true,
                namespace,
            );
        }
    }
    if preview && word(&source[0], "CREATE") && source.iter().take(3).any(|t| word(t, "TABLE")) {
        // Oracle's physical table cache hint does not select BicDB's storage
        // cache. Exported logical schemas declare the adaptation explicitly.
        if source.last() == Some(&Token::SemiColon) {
            source.pop();
        }
        if source
            .last()
            .is_some_and(|t| word(t, "CACHE") || word(t, "NOCACHE"))
        {
            source.pop();
        }
        // Native constraints are enabled. Disabled constraints require a
        // separate migration policy and are not accepted by this lowering.
        if source.iter().any(|t| word(t, "DISABLE")) {
            return Err(unsupported(
                "disabled constraints require an explicit migration policy",
            ));
        }
        source.retain(|t| !word(t, "ENABLE"));
    }
    let routine = routine_declaration
        && source
            .iter()
            .take(4)
            .any(|token| word(token, "PROCEDURE") || (preview && word(token, "FUNCTION")));
    if routine || block {
        return lower_block(db, &source, routine, preview, namespace);
    }
    let mut canonical = canonical_tokens_with_routine_types(
        db,
        &source,
        preview,
        preview && preview_routine_language_enabled(db)?,
        namespace,
    )?;
    expand_references(db, &mut canonical)?;
    if canonical.len() > 3 && word(&canonical[0], "CREATE") && word(&canonical[1], "SEQUENCE") {
        fn rank(t: &Token) -> Option<usize> {
            [
                "INCREMENT",
                "MINVALUE",
                "MAXVALUE",
                "START",
                "CACHE",
                "NO",
                "CYCLE",
            ]
            .iter()
            .position(|s| word(t, s))
        }
        if let Some(first) = (3..canonical.len()).find(|i| rank(&canonical[*i]).is_some()) {
            let starts = (first..canonical.len())
                .filter(|i| rank(&canonical[*i]).is_some())
                .collect::<Vec<_>>();
            let mut options = starts
                .iter()
                .enumerate()
                .map(|(i, start)| {
                    let end = starts.get(i + 1).copied().unwrap_or(canonical.len());
                    (
                        rank(&canonical[*start]).unwrap(),
                        canonical[*start..end].to_vec(),
                    )
                })
                .collect::<Vec<_>>();
            // NO CYCLE is a single option, not two independent clauses.
            for i in (1..options.len()).rev() {
                if options[i - 1].1.last().is_some_and(|t| word(t, "NO")) {
                    let tail = options.remove(i).1;
                    options[i - 1].1.extend(tail);
                }
            }
            options.sort_by_key(|(rank, _)| *rank);
            canonical.truncate(first);
            for (_, option) in options {
                canonical.extend(option);
            }
        }
    }
    // Oracle's parenthesized ADD list becomes separate native ALTERs. Each is
    // ordinary authorized DDL; no constraint is silently dropped.
    if canonical.len() > 5 && word(&canonical[0], "ALTER") && word(&canonical[1], "TABLE") {
        if let Some(add) = canonical.iter().position(|t| word(t, "ADD")) {
            if canonical.get(add + 1) == Some(&Token::LParen) {
                let end = canonical
                    .iter()
                    .rposition(|t| *t == Token::RParen)
                    .ok_or_else(|| unsupported("unclosed ALTER ADD list"))?;
                let prefix = join(&canonical[..add + 1]);
                let groups = split_top_level(&canonical[add + 2..end]);
                return groups
                    .iter()
                    .map(|group| {
                        lower_native(db, &format!("{prefix} {}", join(group)), preview, namespace)
                    })
                    .collect::<Result<Vec<_>>>()
                    .map(|parts| parts.join("; "));
            }
        }
    }
    lower_native(db, &join(&canonical), preview, namespace)
}

fn expand_references(db: &BicDb, source: &mut Vec<Token>) -> Result<()> {
    let mut insertions = Vec::new();
    for i in 0..source.len() {
        if !word(&source[i], "REFERENCES") {
            continue;
        }
        let mut end = i + 2;
        while source.get(end) == Some(&Token::Period) {
            end += 2;
        }
        if source.get(end) == Some(&Token::LParen) {
            continue;
        }
        let identifiers = source[i + 1..end]
            .iter()
            .filter_map(|token| match token {
                Token::Word(word) => Some(match word.quote_style {
                    Some(q) => Ident::with_quote(q, word.value.clone()),
                    None => Ident::new(word.value.clone()),
                }),
                _ => None,
            })
            .collect::<Vec<_>>();
        let name = relation_name(&ObjectName::from(identifiers))?;
        let schema = load_schema(db, &name)?
            .ok_or_else(|| unsupported(format!("unknown referenced relation {name}")))?;
        let pending = if source.get(2) == source.get(i + 1) {
            source
                .windows(3)
                .position(|w| word(&w[0], "PRIMARY") && word(&w[1], "KEY") && w[2] == Token::LParen)
                .and_then(|start| {
                    source[start + 3..]
                        .iter()
                        .position(|t| *t == Token::RParen)
                        .map(|len| source[start + 3..start + 3 + len].to_vec())
                })
        } else {
            None
        };
        let columns = if let Some(columns) = pending {
            columns
        } else {
            if schema.has_hidden_primary_key() {
                return Err(unsupported("REFERENCES requires a declared primary key"));
            }
            let keys = primary_key_columns_for_schema(&schema);
            let sql = keys
                .iter()
                .map(|key| format!("\"{}\"", key.replace('"', "\"\"")))
                .collect::<Vec<_>>()
                .join(",");
            tokens(&sql)?
        };
        let mut inserted = vec![Token::LParen];
        inserted.extend(columns);
        inserted.push(Token::RParen);
        insertions.push((end, inserted));
    }
    for (index, tokens) in insertions.into_iter().rev() {
        source.splice(index..index, tokens);
    }
    Ok(())
}

fn split_top_level(source: &[Token]) -> Vec<&[Token]> {
    let (mut depth, mut start) = (0, 0);
    let mut groups = Vec::new();
    for (i, t) in source.iter().enumerate() {
        match t {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            Token::Comma if depth == 0 => {
                groups.push(&source[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    groups.push(&source[start..]);
    groups
}

fn syntax_word(value: &str, preview: bool) -> bool {
    if preview
        && matches!(
            value,
            "SCHEMA"
                | "BYTEA"
                | "COPY"
                | "STDIN"
                | "STDOUT"
                | "FORMAT"
                | "BINARY"
                | "DELIMITER"
                | "HEADER"
                | "JSON"
                | "VALUE"
                | "SCALAR"
                | "ARRAY"
                | "OBJECT"
                | "KEYS"
                | "CURSOR"
                | "OPEN"
                | "CLOSE"
                | "DAY"
                | "HOUR"
                | "MINUTE"
                | "SECOND"
                | "YEAR"
                | "MONTH"
        )
    {
        return true;
    }
    matches!(
        value,
        "CREATE"
            | "OR"
            | "REPLACE"
            | "TABLE"
            | "VIEW"
            | "INDEX"
            | "UNIQUE"
            | "ALTER"
            | "ADD"
            | "DROP"
            | "COLUMN"
            | "CONSTRAINT"
            | "PRIMARY"
            | "KEY"
            | "FOREIGN"
            | "REFERENCES"
            | "CHECK"
            | "NOT"
            | "NULL"
            | "DEFAULT"
            | "ON"
            | "DELETE"
            | "UPDATE"
            | "INSERT"
            | "INTO"
            | "VALUES"
            | "SET"
            | "SELECT"
            | "DISTINCT"
            | "ALL"
            | "FROM"
            | "WHERE"
            | "AND"
            | "AS"
            | "JOIN"
            | "INNER"
            | "LEFT"
            | "RIGHT"
            | "FULL"
            | "OUTER"
            | "CROSS"
            | "USING"
            | "GROUP"
            | "BY"
            | "HAVING"
            | "ORDER"
            | "ASC"
            | "DESC"
            | "NULLS"
            | "FIRST"
            | "LAST"
            | "LIMIT"
            | "OFFSET"
            | "FETCH"
            | "ROWS"
            | "ROW"
            | "ONLY"
            | "WITH"
            | "UNION"
            | "INTERSECT"
            | "EXCEPT"
            | "CASE"
            | "WHEN"
            | "THEN"
            | "ELSE"
            | "END"
            | "IS"
            | "IN"
            | "BETWEEN"
            | "LIKE"
            | "EXISTS"
            | "TRUE"
            | "FALSE"
            | "CAST"
            | "TEXT"
            | "VARCHAR"
            | "CHAR"
            | "NUMERIC"
            | "DECIMAL"
            | "INT"
            | "INTEGER"
            | "BIGINT"
            | "SMALLINT"
            | "BOOLEAN"
            | "TIMESTAMP"
            | "INTERVAL"
            | "TIME"
            | "ZONE"
            | "WITHOUT"
            | "PRECISION"
            | "SEQUENCE"
            | "START"
            | "INCREMENT"
            | "MINVALUE"
            | "MAXVALUE"
            | "CACHE"
            | "CYCLE"
            | "NO"
            | "COMMENT"
            | "COMMIT"
            | "ROLLBACK"
            | "BEGIN"
            | "SAVEPOINT"
            | "TO"
            | "TRANSACTION"
            | "READ"
            | "WRITE"
            | "FOR"
            | "OF"
            | "NOWAIT"
            | "SKIP"
            | "LOCKED"
            | "PROCEDURE"
            | "FUNCTION"
            | "LANGUAGE"
            | "RETURN"
            | "RETURNS"
            | "DECLARE"
            | "IF"
            | "ELSIF"
            | "LOOP"
            | "WHILE"
            | "EXIT"
            | "CONTINUE"
            | "CALL"
            | "RAISE"
            | "EXCEPTION"
            | "PERFORM"
            | "INOUT"
            | "OUT"
            | "DO"
            | "RESET"
            | "DISCARD"
            | "SHOW"
            | "LOCAL"
            | "CASCADE"
            | "RESTRICT"
            | "TRUNCATE"
            | "RENAME"
            | "ENABLE"
            | "DISABLE"
            | "TRIGGER"
            | "AFTER"
            | "BEFORE"
            | "EACH"
            | "EXECUTE"
            | "OVER"
            | "PARTITION"
            | "RANGE"
            | "CURRENT"
            | "UNBOUNDED"
            | "PRECEDING"
            | "FOLLOWING"
    )
}

fn canonical_tokens_with_routine_types(
    db: &BicDb,
    source: &[Token],
    preview: bool,
    preserve_date: bool,
    namespace: Option<&str>,
) -> Result<Vec<Token>> {
    let deterministic = if preview {
        function_attributes::deterministic_positions(source)
    } else {
        BTreeSet::new()
    };
    let hierarchical = preview
        && source
            .windows(2)
            .any(|w| word(&w[0], "CONNECT") && word(&w[1], "BY"));
    let mut pieces = Vec::new();
    let mut decode_scope = Vec::new();
    let mut in_decode = vec![false; source.len()];
    let mut nvl_scope = Vec::new();
    let mut in_nvl = vec![false; source.len()];
    for (index, token) in source.iter().enumerate() {
        if *token == Token::LParen {
            let opens_decode = index >= 1
                && (word(&source[index - 1], "DECODE") || word(&source[index - 1], "NVL"))
                && (index < 2 || source[index - 2] != Token::Period);
            decode_scope.push(opens_decode || decode_scope.last().copied().unwrap_or(false));
            let opens_nvl = index >= 1
                && word(&source[index - 1], "NVL")
                && (index < 2 || source[index - 2] != Token::Period);
            nvl_scope.push(opens_nvl || nvl_scope.last().copied().unwrap_or(false));
        }
        in_decode[index] = preview && decode_scope.last().copied().unwrap_or(false);
        in_nvl[index] = preview && nvl_scope.last().copied().unwrap_or(false);
        if *token == Token::RParen {
            decode_scope.pop();
            nvl_scope.pop();
        }
    }
    let mut i = 0;
    while i < source.len() {
        let t = &source[i];
        if deterministic.contains(&i) {
            pieces.push("IMMUTABLE".into());
            i += 1;
            continue;
        }

        if hierarchical
            && word(t, "ORDER")
            && source.get(i + 1).is_some_and(|t| word(t, "SIBLINGS"))
            && source.get(i + 2).is_some_and(|t| word(t, "BY"))
        {
            pieces.push("ORDER BY bicdb_oracle_sibling_order(),".into());
            i += 3;
            continue;
        }
        if hierarchical
            && (i == 0 || source[i - 1] != Token::Period)
            && (word(t, "CONNECT")
                || word(t, "PRIOR")
                || word(t, "NOCYCLE")
                || word(t, "LEVEL")
                || word(t, "CONNECT_BY_ISCYCLE"))
        {
            pieces.push(t.to_string().to_ascii_uppercase());
            i += 1;
            continue;
        }
        // This is declaration syntax only. Do not reserve either identifier
        // globally or reinterpret a quoted variable/type declaration as a pragma.
        if preview
            && word(t, "PRAGMA")
            && source
                .get(i + 1)
                .is_some_and(|t| word(t, "AUTONOMOUS_TRANSACTION"))
            && source.get(i + 2) == Some(&Token::SemiColon)
        {
            pieces.push("PRAGMA AUTONOMOUS_TRANSACTION".into());
            i += 2;
            continue;
        }
        if preview && word(t, "EXECUTE") && source.get(i + 1).is_some_and(|t| word(t, "IMMEDIATE"))
        {
            pieces.push("EXECUTE IMMEDIATE".into());
            i += 2;
            continue;
        }
        if let Some((reference, consumed)) = source_tokens::sequence(&source[i..], namespace)? {
            pieces.push(reference);
            i += consumed;
            continue;
        }
        if let Some((data_type, consumed)) =
            anchored_types::lower(db, &source[i..], namespace, preview, preserve_date)?
        {
            pieces.push(data_type);
            i += consumed;
            continue;
        }
        if let Token::Word(w) = t {
            let upper = w.value.to_ascii_uppercase();
            if w.quote_style.is_none() {
                if preview
                    && i >= 2
                    && source[i - 1] == Token::LParen
                    && word(&source[i - 2], "EXTRACT")
                    && (i < 3 || source[i - 3] != Token::Period)
                {
                    // EXTRACT's field is grammar, not an application column.
                    // Let the native parser validate the field vocabulary.
                    pieces.push(upper);
                    i += 1;
                    continue;
                }
                if preview
                    && matches!(upper.as_str(), "VARCHAR2" | "CHAR")
                    && source.get(i + 1) == Some(&Token::LParen)
                    && matches!(source.get(i + 2), Some(Token::Number(..)))
                    && source
                        .get(i + 3)
                        .is_some_and(|t| word(t, "CHAR") || word(t, "BYTE"))
                    && source.get(i + 4) == Some(&Token::RParen)
                {
                    let name = if upper == "VARCHAR2" {
                        "VARCHAR"
                    } else {
                        "CHAR"
                    };
                    let unit = if word(&source[i + 3], "CHAR") {
                        "CHARACTERS"
                    } else {
                        "OCTETS"
                    };
                    pieces.push(format!("{name}({} {unit})", source[i + 2]));
                    i += 5;
                    continue;
                }
                match upper.as_str() {
                    "TYPE" if preview && i == 1 && word(&source[0], "DROP") => pieces.push("TYPE".into()),
                    "SYS_GUID" if preview
                        && (i == 0 || source[i - 1] != Token::Period)
                        && source.get(i + 1) == Some(&Token::LParen)
                        && source.get(i + 2) == Some(&Token::RParen) => {
                        // RAW(16), generated independently on every evaluation.
                        // Keep qualified application functions in their namespace.
                        pieces.push("gen_random_bytes(16)".into());
                        i += 3;
                        continue;
                    }
                    "MULTISET" if preview && source.get(i + 1).is_some_and(|token| ["INTERSECT","UNION","EXCEPT"].iter().any(|value| word(token,value))) => pieces.push("MULTISET".into()),
                    "EMPTY" if preview && i >= 1 && (word(&source[i-1],"IS") || (i >= 2 && word(&source[i-1],"NOT") && word(&source[i-2],"IS"))) => pieces.push("EMPTY".into()),
                    "TRIM" | "REGEXP_SUBSTR" | "REGEXP_REPLACE" if preview
                        && (i == 0 || source[i - 1] != Token::Period)
                        && source.get(i + 1) == Some(&Token::LParen) => pieces.push(w.value.to_ascii_lowercase()),
                    "BOTH" | "LEADING" | "TRAILING" if preview && i >= 2
                        && source[i - 1] == Token::LParen && word(&source[i - 2], "TRIM")
                        && (i < 3 || source[i - 3] != Token::Period) => pieces.push(upper),
                    "CLOB" if preview => pieces.push("TEXT".into()),
                    "BLOB" if preview => pieces.push("BYTEA".into()),
                    "OCTET_LENGTH" if preview => pieces.push("octet_length".into()),
                    "NUMBER" => pieces.push("NUMERIC".into()),
                    "VARCHAR2" => pieces.push("VARCHAR".into()),
                    "DATE" if preserve_date || in_decode[i] => pieces.push("DATE".into()),
                    "DATE" => pieces.push("TIMESTAMP(0)".into()),
                    "NOCACHE" => pieces.push("CACHE 1".into()),
                    "NOCYCLE" => pieces.push("NO CYCLE".into()),
                    "SYSDATE" if preserve_date || in_decode[i] => pieces.push("bicdb_oracle_date_coerce(CAST(date_trunc('second', clock_timestamp()) AS TIMESTAMP))".into()),
                    "SYSDATE" => pieces.push(
                        "CAST(date_trunc('second', clock_timestamp()) AS TIMESTAMP(0))".into(),
                    ),
                    "NVL" if !preview => pieces.push("coalesce".into()),
                    "NVL" if source.get(i+1)==Some(&Token::LParen)
                        && (i==0 || source[i-1]!=Token::Period) => pieces.push("nvl".into()),
                    "TO_DATE" if source.get(i + 1) == Some(&Token::LParen) => {
                        pieces.push("bicdb_oracle_to_date".into())
                    }
                    "DELETE" if preview && !source.get(i+1).is_some_and(|t|word(t,"FROM"))
                        && (i==0 || source[i-1]==Token::SemiColon || ["BEGIN","THEN","ELSE","LOOP"].iter().any(|kw|word(&source[i-1],kw))) => {
                        pieces.push("DELETE FROM".into());
                    }
                    "EXTRACT" if preview && source.get(i + 1) == Some(&Token::LParen) => {
                        pieces.push("extract".into())
                    }
                    "ORGANIZATION" => {
                        return Err(unsupported(
                            "index-organized storage requires an explicit heap-storage migration",
                        ))
                    }
                    _ if source.get(i + 1) == Some(&Token::LParen)
                        && matches!(
                            upper.as_str(),
                            "TO_DATE"
                                | "TO_CHAR"
                                | "TO_NUMBER"
                                | "COALESCE"
                                | "NULLIF"
                                | "COUNT"
                                | "SUM"
                                | "AVG"
                                | "MIN"
                                | "MAX"
                                | "ROUND"
                                | "TRUNC"
                                | "UPPER"
                                | "LOWER"
                                | "LENGTH"
                                | "SUBSTR"
                                | "REPLACE"
                                | "CONCAT"
                                | "CURRENT_SETTING"
                                | "SET_CONFIG"
                                | "PG_TYPEOF"
                        ) =>
                    {
                        pieces.push(w.value.to_lowercase())
                    }
                    _ if !syntax_word(&upper, preview) => pieces.push(format!(
                        "\"{}\"",
                        w.value.to_uppercase().replace('"', "\"\"")
                    )),
                    _ => pieces.push(render_token(t)),
                }
            } else {
                pieces.push(render_token(t));
            }
        } else if matches!(t,Token::SingleQuotedString(value) if value.is_empty()) {
            pieces.push(
                if in_nvl[i] {
                    "CAST(NULL AS VARCHAR)"
                } else {
                    "NULL"
                }
                .into(),
            );
        } else {
            pieces.push(render_token(t));
        }
        i += 1;
    }
    Tokenizer::new(&PostgreSqlDialect {}, &pieces.join(" "))
        .tokenize_with_location()
        .map(source_tokens::ranges)
        .map_err(|e| unsupported(e.to_string()))
}

fn lower_block(
    db: &BicDb,
    source: &[Token],
    routine: bool,
    preview: bool,
    namespace: Option<&str>,
) -> Result<String> {
    if !preview && source.iter().any(|t| *t == Token::StringConcat) {
        return Err(unsupported("procedural concatenation is not yet supported"));
    }
    // A final slash is the SQL*Plus submission delimiter, not part of PL/SQL.
    let source = source.strip_suffix(&[Token::Div]).unwrap_or(source);
    let definition_namespace = if routine {
        anchored_types::definition_namespace(source, namespace)?
    } else {
        namespace.map(str::to_string)
    };
    let canonical = canonical_tokens_with_routine_types(
        db,
        source,
        preview,
        preview && (routine || preview_routine_language_enabled(db)?),
        definition_namespace.as_deref(),
    )?;
    let (header, body) = if routine {
        let mut depth = 0;
        let boundary = canonical
            .iter()
            .position(|t| {
                match t {
                    Token::LParen => depth += 1,
                    Token::RParen => depth -= 1,
                    _ => {}
                }
                depth == 0 && (word(t, "IS") || word(t, "AS"))
            })
            .ok_or_else(|| unsupported("routine requires IS or AS"))?;
        let mut header = canonical[..boundary].to_vec();
        if let Some(open) = header.iter().position(|t| *t == Token::LParen) {
            let mut depth = 1;
            let close = (open + 1..header.len())
                .find(|i| {
                    match header[*i] {
                        Token::LParen => depth += 1,
                        Token::RParen => depth -= 1,
                        _ => {}
                    }
                    depth == 0
                })
                .ok_or_else(|| unsupported("unclosed routine argument list"))?;
            let mut args = Vec::new();
            for (index, group) in split_top_level(&header[open + 1..close]).iter().enumerate() {
                if index > 0 {
                    args.push(Token::Comma);
                }
                if group
                    .get(1)
                    .is_some_and(|t| word(t, "IN") || word(t, "OUT"))
                {
                    if word(&group[1], "IN") && group.get(2).is_some_and(|t| word(t, "OUT")) {
                        args.push(Token::Word(sqlparser::tokenizer::Word {
                            value: "INOUT".into(),
                            quote_style: None,
                            keyword: Keyword::INOUT,
                        }));
                        args.push(group[0].clone());
                        args.extend_from_slice(&group[3..]);
                    } else {
                        args.extend([group[1].clone(), group[0].clone()]);
                        args.extend_from_slice(&group[2..]);
                    }
                } else {
                    args.extend_from_slice(group);
                }
            }
            header.splice(open + 1..close, args);
        }
        if let Some(function) = header.iter().position(|t| word(t, "FUNCTION")) {
            let returns = header
                .iter()
                .position(|t| word(t, "RETURN"))
                .ok_or_else(|| unsupported("function requires a RETURN type"))?;
            header[returns] = Token::Word(sqlparser::tokenizer::Word {
                value: "RETURNS".into(),
                quote_style: None,
                keyword: Keyword::RETURNS,
            });
            // Oracle omits parentheses for parameterless functions; native
            // PostgreSQL declarations require an explicit empty argument list.
            let mut end_name = function + 2;
            while header.get(end_name) == Some(&Token::Period) {
                end_name += 2;
            }
            if end_name == returns {
                header.splice(end_name..end_name, [Token::LParen, Token::RParen]);
            }
        }
        (Some(header), &canonical[boundary + 1..])
    } else {
        (None, canonical.as_slice())
    };
    let mut body = body.to_vec();
    if routine
        && !body
            .first()
            .is_some_and(|t| word(t, "BEGIN") || word(t, "DECLARE"))
    {
        body.insert(
            0,
            Token::Word(sqlparser::tokenizer::Word {
                value: "DECLARE".into(),
                quote_style: None,
                keyword: Keyword::DECLARE,
            }),
        );
    }
    if preview {
        lower_cursor_declarations(&mut body)?;
        if let Some(header) = &header {
            anchored_types::constrain_numeric_returns(header, &mut body)?;
        }
    }
    // Oracle permits END procedure_name; PL/pgSQL uses END; here.
    if let Some(end) = body.iter().rposition(|t| word(t, "END")) {
        if matches!(body.get(end + 1), Some(Token::Word(_)))
            && body.get(end + 2) == Some(&Token::SemiColon)
        {
            body.remove(end + 1);
        }
    }
    // A standalone PL/SQL procedure invocation becomes native CALL. Expressions
    // and assignments are untouched; this is token based, never string regex.
    for i in (1..body.len()).rev() {
        let statement_start = ["BEGIN", "THEN", "ELSE", "LOOP"]
            .iter()
            .any(|name| word(&body[i - 1], name))
            || body[i - 1] == Token::SemiColon;
        if !statement_start || !matches!(&body[i],Token::Word(w) if w.quote_style.is_some()) {
            continue;
        }
        let mut end_name = i + 1;
        while body.get(end_name) == Some(&Token::Period)
            && matches!(body.get(end_name + 1), Some(Token::Word(_)))
        {
            end_name += 2;
        }
        if body.get(end_name) == Some(&Token::LParen) {
            let mut depth = 0usize;
            let close = (end_name..body.len()).find(|index| {
                match body[*index] {
                    Token::LParen => depth += 1,
                    Token::RParen => depth = depth.saturating_sub(1),
                    _ => {}
                }
                depth == 0
            });
            // A parenthesized collection subscript on an assignment's left
            // side is not a procedure invocation. CALL applies only to the
            // complete standalone expression before its statement delimiter.
            if !close.is_some_and(|close| body.get(close + 1) == Some(&Token::SemiColon)) {
                continue;
            }
            body.insert(
                i,
                Token::Word(sqlparser::tokenizer::Word {
                    value: "CALL".to_string(),
                    quote_style: None,
                    keyword: Keyword::CALL,
                }),
            );
        }
    }
    let body = join(&body);
    let delimiter = (0..100)
        .map(|n| format!("$bicdb_oracle_{n}$"))
        .find(|s| !body.contains(s))
        .ok_or_else(|| unsupported("procedure delimiter exhausted"))?;
    let language = if preview {
        PREVIEW_ROUTINE_LANGUAGE
    } else {
        "plpgsql"
    };
    Ok(match header {
        Some(header) => format!(
            "{} LANGUAGE {language} AS {delimiter}{body}{delimiter}",
            join(&header)
        ),
        None if preview => format!("DO {delimiter}{body}{delimiter} LANGUAGE {language}"),
        None => format!("DO {delimiter}{body}{delimiter}"),
    })
}

fn lower_cursor_declarations(body: &mut [Token]) -> Result<()> {
    for index in 0..body.len() {
        let declaration_start =
            index == 0 || word(&body[index - 1], "DECLARE") || body[index - 1] == Token::SemiColon;
        if !declaration_start || !word(&body[index], "CURSOR") {
            continue;
        }
        if !matches!(body.get(index + 1), Some(Token::Word(_))) {
            return Err(unsupported("cursor declaration requires an identifier"));
        }
        let mut separator = index + 2;
        if body.get(separator) == Some(&Token::LParen) {
            let mut depth = 0;
            while separator < body.len() {
                match body[separator] {
                    Token::LParen => depth += 1,
                    Token::RParen => {
                        depth -= 1;
                        if depth == 0 {
                            separator += 1;
                            break;
                        }
                    }
                    _ => {}
                }
                separator += 1;
            }
        }
        if !body.get(separator).is_some_and(|token| word(token, "IS")) {
            return Err(unsupported(
                "Oracle cursor declarations require IS followed by a query",
            ));
        }
        body.swap(index, index + 1);
        body[separator] = Token::Word(sqlparser::tokenizer::Word {
            value: "FOR".into(),
            quote_style: None,
            keyword: Keyword::FOR,
        });
    }
    Ok(())
}

fn expression(sql: &str) -> Result<Expr> {
    let mut statements = Parser::parse_sql(&PostgreSqlDialect {}, &format!("SELECT {sql}"))
        .map_err(|e| unsupported(e.to_string()))?;
    match statements.remove(0) {
        Statement::Query(query) => match *query.body {
            SetExpr::Select(mut select) => match select.projection.remove(0) {
                SelectItem::UnnamedExpr(expr) => Ok(expr),
                _ => Err(unsupported("invalid expression")),
            },
            _ => Err(unsupported("invalid expression")),
        },
        _ => Err(unsupported("invalid expression")),
    }
}

fn lower_native(db: &BicDb, sql: &str, preview: bool, namespace: Option<&str>) -> Result<String> {
    let rewritten = preview.then(|| rewrite_is_json_predicates(sql)).flatten();
    let sql = rewritten.as_deref().unwrap_or(sql);
    let _grammar = syntax::Scope::new(preview);
    let _multisets = collections::multisets::Scope::new(preview);
    let mut statements = parse_statements(sql)?;
    if preview {
        syntax::rewrite(db, &mut statements, namespace)?;
    }
    if preview {
        collections::lower_constructors(db, &mut statements, namespace)?;
        collections::multisets::lower(db, &mut statements, namespace)?;
    }
    if preview {
        struct Aliases;
        impl sqlparser::ast::VisitorMut for Aliases {
            type Break = ();
            fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
                if let SetExpr::Select(select) = query.body.as_mut() {
                    for item in &mut select.projection {
                        if let SelectItem::ExprWithAlias { alias, .. } = item {
                            // Words retained as grammar tokens (VALUE, DAY,
                            // YEAR, ...) are identifiers in this position.
                            if alias.quote_style.is_none() {
                                *alias = Ident::with_quote('"', alias.value.to_ascii_uppercase());
                            }
                        }
                    }
                }
                ControlFlow::Continue(())
            }
        }
        let _ = sqlparser::ast::VisitMut::visit(&mut statements, &mut Aliases);
    }
    let result = visit_expressions_mut(&mut statements, |expr| {
        if preview {
            if let Err(error) = text_patterns::lower_expression(expr) {
                return ControlFlow::Break(error);
            }
            if let Err(error) = temporal_columns::lower_date_expression(expr) {
                return ControlFlow::Break(error);
            }
        }
        if let Expr::BinaryOp {
            left,
            op: BinaryOperator::StringConcat,
            right,
        } = expr
        {
            match expression(&format!("NULLIF(concat({left},{right}), '')")) {
                Ok(lowered) => *expr = lowered,
                Err(e) => return ControlFlow::Break(e),
            }
        }
        ControlFlow::Continue(())
    });
    if let ControlFlow::Break(e) = result {
        return Err(e);
    }
    rownum::lower(&mut statements)?;
    dual::lower(&mut statements, true)?;
    Ok(statements
        .iter()
        .map(render_sql)
        .collect::<Vec<_>>()
        .join("; "))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn preview_table_qualifiers_survive_schema_binding_in_filters_and_correlations() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        for source in [
            "CREATE SCHEMA app_data",
            "CREATE TABLE app_data.accounts(k NUMBER PRIMARY KEY, client_k NUMBER, active CHAR(1))",
            "CREATE TABLE app_data.clients(k NUMBER PRIMARY KEY, active CHAR(1))",
            "CREATE TABLE app_data.language_rows(language_rows VARCHAR2(20))",
            "INSERT INTO app_data.accounts VALUES(1,10,'Y'),(2,20,'Y')",
            "INSERT INTO app_data.clients VALUES(10,'Y'),(20,'N')",
            "INSERT INTO app_data.language_rows VALUES('en_US')",
        ] {
            let lowered = lower_profile_sql(&db, source, ORACLE_PREVIEW_PROFILE).unwrap();
            SqlSession::new(&mut db).execute(&lowered).unwrap();
        }
        for source in [
            "SELECT accounts.k FROM accounts WHERE accounts.active='Y' AND EXISTS(SELECT 1 FROM clients WHERE clients.k=accounts.client_k AND clients.active='Y')",
            "SELECT accounts.k FROM accounts JOIN clients ON clients.k=accounts.client_k WHERE clients.active='Y'",
            "SELECT a.k FROM accounts a WHERE a.active='Y' AND EXISTS(SELECT 1 FROM clients c WHERE c.k=a.client_k AND c.active='Y')",
        ] {
            let lowered = lower_session_sql(&db, source, ORACLE_PREVIEW_PROFILE, Some("app_data"), "bicdb").unwrap();
            let result = SqlSession::new(&mut db).execute(&lowered).unwrap();
            assert_eq!(result.rows.len(), 1, "{source}");
            assert_eq!(result.rows[0][0].to_cell(), "1");
        }
        let lowered = lower_session_sql(
            &db,
            "SELECT accounts.* FROM accounts WHERE accounts.k=1",
            ORACLE_PREVIEW_PROFILE,
            Some("app_data"),
            "bicdb",
        )
        .unwrap();
        let result = SqlSession::new(&mut db).execute(&lowered).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.columns.len(), 3);
        for source in [
            "UPDATE accounts SET active='N' WHERE accounts.k=1",
            "DELETE FROM accounts WHERE accounts.k=2",
        ] {
            let lowered = lower_session_sql(
                &db,
                source,
                ORACLE_PREVIEW_PROFILE,
                Some("app_data"),
                "bicdb",
            )
            .unwrap();
            SqlSession::new(&mut db).execute(&lowered).unwrap();
        }
        let lowered = lower_session_sql(
            &db,
            "SELECT accounts.k FROM accounts WHERE accounts.active='N'",
            ORACLE_PREVIEW_PROFILE,
            Some("app_data"),
            "bicdb",
        )
        .unwrap();
        let result = SqlSession::new(&mut db).execute(&lowered).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0].to_cell(), "1");
        let lowered = lower_session_sql(
            &db,
            "SELECT language_rows FROM language_rows",
            ORACLE_PREVIEW_PROFILE,
            Some("app_data"),
            "bicdb",
        )
        .unwrap();
        let result = SqlSession::new(&mut db).execute(&lowered).unwrap();
        assert_eq!(result.column_types, vec![Some("varchar".into())]);
        assert_eq!(result.rows[0][0].to_cell(), "en_US");
        let mut native = SqlSession::new(&mut db);
        native.execute("CREATE TABLE word_list(word_list varchar(20)); INSERT INTO word_list VALUES('value')").unwrap();
        let result = native
            .execute("SELECT word_list FROM word_list AS word_list")
            .unwrap();
        assert_eq!(result.column_types, vec![Some("varchar".into())]);
        assert_eq!(result.rows[0][0].to_cell(), "value");
    }
    #[test]
    fn preview_cursor_declarations_and_named_for_loops_execute() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        for source in [
            "CREATE TABLE cursor_input(k NUMBER,bucket NUMBER,v NUMBER)",
            "INSERT INTO cursor_input VALUES(1,7,11),(2,7,22),(3,8,44)",
            "CREATE FUNCTION cursor_total(p_bucket IN NUMBER) RETURN NUMBER IS total NUMBER:=0; CURSOR c IS SELECT v FROM cursor_input WHERE bucket=p_bucket ORDER BY k; BEGIN FOR r IN c LOOP total:=total+r.v; END LOOP; RETURN total; END;"
        ] {
            let lowered=lower_session_sql(session.db_ref(),source,ORACLE_PREVIEW_PROFILE,None,"bicdb").unwrap();
            session.execute(&lowered).unwrap();
        }
        for (bucket, expected) in [(7, "33"), (8, "44"), (9, "0"), (7, "33")] {
            let lowered = lower_session_sql(
                session.db_ref(),
                &format!("SELECT cursor_total({bucket}) FROM DUAL"),
                ORACLE_PREVIEW_PROFILE,
                None,
                "bicdb",
            )
            .unwrap();
            assert_eq!(
                session.execute(&lowered).unwrap().rows[0][0].to_cell(),
                expected
            );
        }
        for (name, call) in [("cursor_args", "c(p=>42)"), ("cursor_missing_arg", "c")] {
            let definition = format!("CREATE FUNCTION {name} RETURN NUMBER IS CURSOR c(p NUMBER) IS SELECT p AS v FROM DUAL; BEGIN FOR r IN {call} LOOP RETURN r.v; END LOOP; RETURN 0; END;");
            let lowered =
                lower_profile_sql(session.db_ref(), &definition, ORACLE_PREVIEW_PROFILE).unwrap();
            session.execute(&lowered).unwrap();
        }
        let valid = lower_profile_sql(
            session.db_ref(),
            "SELECT cursor_args() FROM DUAL",
            ORACLE_PREVIEW_PROFILE,
        )
        .unwrap();
        assert_eq!(session.execute(&valid).unwrap().rows[0][0].to_cell(), "42");
        let invalid = lower_profile_sql(
            session.db_ref(),
            "SELECT cursor_missing_arg() FROM DUAL",
            ORACLE_PREVIEW_PROFILE,
        )
        .unwrap();
        assert_eq!(session.execute(&invalid).unwrap_err().sqlstate(), "42601");
    }

    #[test]
    fn oracle_procedure_drop_does_not_enter_the_declaration_lowerer() {
        for profile in [ORACLE_HR_PROFILE] {
            let root = tempfile::tempdir().unwrap();
            let mut db = BicDb::open(root.path()).unwrap();
            ensure_database_profile(&mut db, profile).unwrap();
            for source in [
                "CREATE PROCEDURE cleanup_proc(p_value IN NUMBER) AS BEGIN RETURN; END;",
                "DROP PROCEDURE cleanup_proc",
                "DROP PROCEDURE IF EXISTS cleanup_proc",
            ] {
                let sql = lower_session_sql(&db, source, profile, None, "bicdb").unwrap();
                SqlSession::new(&mut db).execute(&sql).unwrap();
            }
            assert!(load_routine(&db, RoutineKind::Procedure, "cleanup_proc")
                .unwrap()
                .is_none());
        }
    }

    #[test]
    fn routine_drop_respects_ownership_qualified_keys_and_transaction_rollback() {
        fn execute(session: &mut SqlSession<'_>, profile: &str, source: &str) -> Result<SqlResult> {
            let sql = if profile == POSTGRES_PROFILE {
                source.to_string()
            } else {
                lower_session_sql(session.db_ref(), source, profile, None, "bicdb")?
            };
            session.execute(&sql)
        }
        for profile in [POSTGRES_PROFILE, ORACLE_PREVIEW_PROFILE] {
            let root = tempfile::tempdir().unwrap();
            let mut db = BicDb::open(root.path()).unwrap();
            ensure_database_profile(&mut db, profile).unwrap();
            let mut session = SqlSession::new(&mut db);
            for source in [
                "CREATE ROLE routine_owner",
                "CREATE ROLE routine_other",
                "CREATE SCHEMA routine_space AUTHORIZATION routine_owner",
                "SET ROLE routine_owner",
            ] {
                session.execute(source).unwrap();
            }
            let function = if profile == POSTGRES_PROFILE {
                "CREATE FUNCTION routine_space.drop_fn() RETURNS INT LANGUAGE plpgsql AS $$BEGIN RETURN 7; END$$"
            } else {
                r#"CREATE FUNCTION "routine_space".drop_fn RETURN NUMBER AS BEGIN RETURN 7; END;"#
            };
            execute(&mut session, profile, function).unwrap();
            // Provision this procedure with the backend API. Preview Oracle
            // procedure creation remains outside this deletion regression.
            session.execute("CREATE PROCEDURE routine_space.drop_proc(p_in INT) LANGUAGE plpgsql AS $$BEGIN RETURN; END$$").unwrap();
            session
                .execute("GRANT EXECUTE ON FUNCTION drop_fn() TO routine_other")
                .unwrap();
            session.execute("SET ROLE routine_other").unwrap();
            let query = if profile == POSTGRES_PROFILE {
                "SELECT drop_fn()"
            } else {
                "SELECT drop_fn() FROM DUAL"
            };
            assert_eq!(
                execute(&mut session, profile, query).unwrap().rows[0][0].to_cell(),
                "7"
            );
            for source in [
                r#"DROP FUNCTION "routine_space".drop_fn"#,
                r#"DROP PROCEDURE "routine_space".drop_proc"#,
            ] {
                let error = execute(&mut session, profile, source).unwrap_err();
                assert_eq!(error.sqlstate(), "42501", "{profile}: {error}");
            }
            session.execute("SET ROLE routine_owner").unwrap();
            for source in [
                r#"DROP FUNCTION "wrong_space".drop_fn"#,
                r#"DROP PROCEDURE "wrong_space".drop_proc"#,
            ] {
                assert!(execute(&mut session, profile, source).is_err());
            }
            execute(&mut session,profile,r#"DROP FUNCTION IF EXISTS "wrong_space".drop_fn CASCADE; DROP PROCEDURE IF EXISTS "wrong_space".drop_proc"#).unwrap();
            assert!(
                load_routine(session.db_ref(), RoutineKind::Function, "drop_fn")
                    .unwrap()
                    .is_some()
            );
            assert!(
                load_routine(session.db_ref(), RoutineKind::Procedure, "drop_proc")
                    .unwrap()
                    .is_some()
            );
            for source in [
                r#"DROP FUNCTION "routine_space".drop_fn, "routine_space".missing_fn"#,
                r#"DROP PROCEDURE "routine_space".drop_proc, "routine_space".missing_proc"#,
            ] {
                assert!(execute(&mut session, profile, source).is_err());
                assert!(
                    load_routine(session.db_ref(), RoutineKind::Function, "drop_fn")
                        .unwrap()
                        .is_some()
                );
                assert!(
                    load_routine(session.db_ref(), RoutineKind::Procedure, "drop_proc")
                        .unwrap()
                        .is_some()
                );
            }
            session.execute("BEGIN").unwrap();
            execute(&mut session,profile,r#"DROP FUNCTION "routine_space".drop_fn; DROP PROCEDURE "routine_space".drop_proc"#).unwrap();
            assert!(
                load_routine(session.db_ref(), RoutineKind::Function, "drop_fn")
                    .unwrap()
                    .is_none()
            );
            assert!(
                load_routine(session.db_ref(), RoutineKind::Procedure, "drop_proc")
                    .unwrap()
                    .is_none()
            );
            session.execute("ROLLBACK").unwrap();
            assert!(
                load_routine(session.db_ref(), RoutineKind::Function, "drop_fn")
                    .unwrap()
                    .is_some()
            );
            assert!(
                load_routine(session.db_ref(), RoutineKind::Procedure, "drop_proc")
                    .unwrap()
                    .is_some()
            );
            assert_eq!(
                session.execute("SELECT drop_fn()").unwrap().rows[0][0].to_cell(),
                "7"
            );
            execute(&mut session,profile,r#"DROP FUNCTION "routine_space".drop_fn; DROP PROCEDURE "routine_space".drop_proc"#).unwrap();
            assert!(
                load_routine(session.db_ref(), RoutineKind::Function, "drop_fn")
                    .unwrap()
                    .is_none()
            );
            assert!(
                load_routine(session.db_ref(), RoutineKind::Procedure, "drop_proc")
                    .unwrap()
                    .is_none()
            );
            execute(&mut session,profile,r#"DROP FUNCTION IF EXISTS "routine_space".drop_fn; DROP PROCEDURE IF EXISTS "routine_space".drop_proc"#).unwrap();
        }
    }

    #[test]
    fn preview_stored_functions_lower_returns_and_local_declarations() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        for source in [
            "CREATE OR REPLACE FUNCTION day_now RETURN DATE AS BEGIN RETURN SYSDATE; END; /",
            "CREATE OR REPLACE FUNCTION pick_initial(p_text VARCHAR2, p_pos NUMBER) RETURN VARCHAR2 IS BEGIN RETURN SUBSTR(p_text,p_pos,1); END pick_initial;",
            "CREATE OR REPLACE FUNCTION numeric_shift(p_input IN NUMBER DEFAULT CAST(4 AS NUMBER)) RETURN NUMBER IS delta NUMBER := 1; BEGIN RETURN p_input+delta; END numeric_shift;",
        ] {
            let sql = lower_profile_sql(&db, source, ORACLE_PREVIEW_PROFILE).unwrap();
            SqlSession::new(&mut db).execute(&sql).unwrap();
        }
        for (sql, expected) in [
            ("SELECT pick_initial('Sample',2) FROM DUAL", "a"),
            ("SELECT numeric_shift(9) FROM DUAL", "10"),
            ("SELECT numeric_shift() FROM DUAL", "5"),
        ] {
            let lowered = lower_profile_sql(&db, sql, ORACLE_PREVIEW_PROFILE).unwrap();
            let result = SqlSession::new(&mut db).execute(&lowered).unwrap();
            assert_eq!(result.rows[0][0].to_cell(), expected);
        }
        let lowered =
            lower_profile_sql(&db, "SELECT day_now() FROM DUAL", ORACLE_PREVIEW_PROFILE).unwrap();
        let result = SqlSession::new(&mut db).execute(&lowered).unwrap();
        assert_eq!(result.column_types, vec![Some("timestamp".into())]);
        assert!(!matches!(result.rows[0][0], SqlValue::Null));
        let mut native = SqlSession::new(&mut db);
        native.execute(r#"CREATE FUNCTION pg_quoted_arg("Arg" text) RETURNS text LANGUAGE plpgsql AS $$BEGIN RETURN substr("Arg",2,1); END$$"#).unwrap();
        assert_eq!(
            native
                .execute("SELECT pg_quoted_arg('Sample')")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "a"
        );
        let autonomous = lower_profile_sql(&db,
            "CREATE FUNCTION independent_id RETURN NUMBER AS PRAGMA AUTONOMOUS_TRANSACTION; BEGIN RETURN 1; END;",
            ORACLE_PREVIEW_PROFILE).unwrap();
        assert!(autonomous.contains(PREVIEW_ROUTINE_LANGUAGE));
        SqlSession::new(&mut db).execute(&autonomous).unwrap();
        let call = lower_profile_sql(
            &db,
            "SELECT independent_id() FROM DUAL",
            ORACLE_PREVIEW_PROFILE,
        )
        .unwrap();
        assert_eq!(
            SqlSession::new(&mut db).execute(&call).unwrap().rows[0][0].to_cell(),
            "1"
        );
        assert!(lower_profile_sql(&db,
            "CREATE FUNCTION independent_id RETURN NUMBER AS PRAGMA AUTONOMOUS_TRANSACTION; BEGIN RETURN 1; END;",
            ORACLE_HR_PROFILE).is_err());
        assert!(lower_profile_sql(
            &db,
            "CREATE FUNCTION not_hr RETURN NUMBER AS BEGIN RETURN 1; END;",
            ORACLE_HR_PROFILE
        )
        .is_err());
    }

    #[test]
    fn preview_routine_datetime_trunc_uses_declared_parameters_and_locals() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        for source in [
            "CREATE FUNCTION routine_calendar(p_date DATE,p_month NUMBER) RETURN DATE IS result DATE:=TRUNC(p_date); BEGIN IF p_month=1 THEN result:=TRUNC(p_date,'MM'); ELSIF p_month=2 THEN RETURN TRUNC(TRUNC(result,'MM'),'YYYY'); END IF; RETURN result; END;",
            "CREATE FUNCTION routine_numeric(p_number NUMBER) RETURN NUMBER IS result NUMBER:=TRUNC(p_number); BEGIN RETURN result; END;",
        ] {
            let lowered=lower_profile_sql(session.db_ref(),source,ORACLE_PREVIEW_PROFILE).unwrap();
            assert!(lowered.contains(PREVIEW_ROUTINE_LANGUAGE));
            session.execute(&lowered).unwrap();
        }
        for (date, mode, expected) in [
            ("2024-02-29 23:59:58", 0, "2024-02-29 00:00:00"),
            ("2024-02-29 23:59:58", 1, "2024-02-01 00:00:00"),
            ("2024-02-29 23:59:58", 2, "2024-01-01 00:00:00"),
            ("2025-03-02 12:34:56", 0, "2025-03-02 00:00:00"),
        ] {
            let sql = lower_profile_sql(
                session.db_ref(),
                &format!("SELECT routine_calendar(TIMESTAMP '{date}',{mode}) FROM DUAL"),
                ORACLE_PREVIEW_PROFILE,
            )
            .unwrap();
            assert_eq!(
                session.execute(&sql).unwrap().rows[0][0].to_cell(),
                expected
            );
        }
        let sql = lower_profile_sql(
            session.db_ref(),
            "SELECT routine_numeric(-123.75) FROM DUAL",
            ORACLE_PREVIEW_PROFILE,
        )
        .unwrap();
        assert_eq!(session.execute(&sql).unwrap().rows[0][0].to_cell(), "-123");
        for source in [
            "SELECT routine_numeric(NULL) FROM DUAL",
            "SELECT routine_calendar(NULL,0) FROM DUAL",
            "SELECT routine_calendar(NULL,2) FROM DUAL",
        ] {
            let sql = lower_profile_sql(session.db_ref(), source, ORACLE_PREVIEW_PROFILE).unwrap();
            assert_eq!(session.execute(&sql).unwrap().rows[0][0], SqlValue::Null);
        }
        assert_eq!(
            session
                .execute(
                    "SELECT trunc(CAST(NULL AS numeric)),trunc(CAST(NULL AS double precision))"
                )
                .unwrap()
                .rows[0],
            vec![SqlValue::Null, SqlValue::Null]
        );
        session.execute("CREATE FUNCTION native_trunc(p_date TIMESTAMP) RETURNS TIMESTAMP LANGUAGE plpgsql AS $$BEGIN RETURN trunc(p_date); END$$").unwrap();
        assert!(session
            .execute("SELECT native_trunc(TIMESTAMP '2024-02-29 23:59:58')")
            .is_err());
    }

    #[test]
    fn preview_routine_language_requires_durable_profile_and_has_catalog_identity() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        let ddl=format!("CREATE FUNCTION language_check() RETURNS INT LANGUAGE {PREVIEW_ROUTINE_LANGUAGE} AS $$BEGIN RETURN 7; END$$");
        assert!(SqlSession::new(&mut db).execute(&ddl).is_err());
        assert!(!preview_routine_language_enabled(&db).unwrap());
        ensure_database_profile(&mut db, ORACLE_HR_PROFILE).unwrap();
        assert!(SqlSession::new(&mut db).execute(&ddl).is_err());
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        session.execute(&ddl).unwrap();
        assert_eq!(
            session.execute("SELECT language_check()").unwrap().rows[0][0],
            SqlValue::Int(7)
        );
        let result=session.execute("SELECT l.lanname,p.prolang=l.oid FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_language l ON p.prolang=l.oid WHERE p.proname='language_check'").unwrap();
        assert_eq!(
            result.rows,
            vec![vec![
                SqlValue::String(PREVIEW_ROUTINE_LANGUAGE.into()),
                SqlValue::Bool(true)
            ]]
        );
    }

    #[test]
    fn preview_routine_variable_types_do_not_retype_embedded_query_columns() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        for source in [
            "CREATE TABLE numeric_shadow(p_date NUMBER)",
            "INSERT INTO numeric_shadow VALUES(123.75)",
            "CREATE FUNCTION shadow_trunc(p_date DATE) RETURN NUMBER IS result NUMBER; BEGIN SELECT TRUNC(p_date) INTO result FROM numeric_shadow; RETURN result; END;",
            "CREATE FUNCTION subquery_trunc(p_date DATE) RETURN NUMBER IS BEGIN RETURN (SELECT TRUNC(p_date) FROM numeric_shadow); END;",
        ] {
            let sql=lower_profile_sql(session.db_ref(),source,ORACLE_PREVIEW_PROFILE).unwrap();session.execute(&sql).unwrap();
        }
        for function in ["shadow_trunc", "subquery_trunc"] {
            let sql = lower_profile_sql(
                session.db_ref(),
                &format!("SELECT {function}(TIMESTAMP '2024-02-29 23:59:58') FROM DUAL"),
                ORACLE_PREVIEW_PROFILE,
            )
            .unwrap();
            assert_eq!(session.execute(&sql).unwrap().rows[0][0].to_cell(), "123");
        }
    }

    #[test]
    fn preview_routine_namespace_binds_cursor_queries_and_ctes() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        for source in [
            r#"CREATE SCHEMA "App.Space""#,
            r#"CREATE TABLE "App.Space".scope_values(bucket NUMBER,v NUMBER)"#,
            r#"INSERT INTO "App.Space".scope_values VALUES(7,11),(7,22),(8,44)"#,
            "CREATE TABLE scope_values(bucket NUMBER,v NUMBER)",
            "INSERT INTO scope_values VALUES(7,999)",
            r#"CREATE FUNCTION "App.Space".namespace_total(p_bucket NUMBER) RETURN NUMBER IS total NUMBER:=0; CURSOR c IS WITH local_rows AS (SELECT scope_values.v FROM scope_values WHERE bucket=p_bucket) SELECT v FROM local_rows; BEGIN FOR r IN c LOOP total:=total+r.v; END LOOP; RETURN total; END;"#,
        ] {
            let sql = lower_session_sql(
                session.db_ref(),
                source,
                ORACLE_PREVIEW_PROFILE,
                None,
                "bicdb",
            )
            .unwrap();
            session.execute(&sql).unwrap();
        }
        for (bucket, expected) in [(7, "33"), (8, "44"), (9, "0"), (7, "33")] {
            let sql = lower_session_sql(
                session.db_ref(),
                &format!("SELECT namespace_total({bucket}) FROM DUAL"),
                ORACLE_PREVIEW_PROFILE,
                None,
                "bicdb",
            )
            .unwrap();
            assert_eq!(
                session.execute(&sql).unwrap().rows[0][0].to_cell(),
                expected
            );
        }
        let sql=lower_session_sql(session.db_ref(),"CREATE FUNCTION implicit_total(p_bucket NUMBER) RETURN NUMBER IS result NUMBER; BEGIN SELECT SUM(v) INTO result FROM scope_values WHERE bucket=p_bucket; RETURN result; END;",ORACLE_PREVIEW_PROFILE,Some(r#""App.Space""#),"bicdb").unwrap();
        session.execute(&sql).unwrap();
        assert_eq!(
            session.execute("SELECT implicit_total(7)").unwrap().rows[0][0].to_cell(),
            "33"
        );
        let routine = load_routine(session.db_ref(), RoutineKind::Function, "implicit_total")
            .unwrap()
            .unwrap();
        assert_eq!(routine.schema, "App.Space");
    }

    #[test]
    fn preview_routine_v2_preserves_old_versions_and_exact_drop_namespace() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        session.execute(r#"CREATE SCHEMA "Bound"; CREATE TABLE "Bound".numbers(v INT); INSERT INTO "Bound".numbers VALUES(2); CREATE TABLE numbers(v INT); INSERT INTO numbers VALUES(99)"#).unwrap();
        for (name, language, expected) in [
            ("native_value", "plpgsql", 99),
            ("legacy_value", PREVIEW_ROUTINE_LANGUAGE_V1, 99),
            ("previous_bound_value", PREVIEW_ROUTINE_LANGUAGE_V2, 2),
            ("bound_value", PREVIEW_ROUTINE_LANGUAGE, 2),
        ] {
            session.execute(&format!(r#"CREATE FUNCTION "Bound".{name}() RETURNS INT LANGUAGE {language} AS $$DECLARE result INT; BEGIN SELECT v INTO result FROM numbers; RETURN result; END$$"#)).unwrap();
            assert_eq!(
                session.execute(&format!("SELECT {name}()")).unwrap().rows[0][0],
                SqlValue::Int(expected)
            );
        }
        let rows=session.execute("SELECT n.nspname FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON p.pronamespace=n.oid WHERE p.proname='bound_value'").unwrap();
        assert_eq!(rows.rows, vec![vec![SqlValue::String("Bound".into())]]);
        assert!(session
            .execute(r#"DROP FUNCTION "bound".bound_value"#)
            .is_err());
        session
            .execute(r#"DROP FUNCTION IF EXISTS "bound".bound_value"#)
            .unwrap();
        assert_eq!(
            session.execute("SELECT bound_value()").unwrap().rows[0][0],
            SqlValue::Int(2)
        );
        session.execute("BEGIN").unwrap();
        session
            .execute(r#"DROP FUNCTION "Bound".bound_value"#)
            .unwrap();
        session.execute("ROLLBACK").unwrap();
        assert_eq!(
            session.execute("SELECT bound_value()").unwrap().rows[0][0],
            SqlValue::Int(2)
        );
        session
            .execute(r#"DROP FUNCTION "Bound".bound_value"#)
            .unwrap();
        assert!(
            load_routine(session.db_ref(), RoutineKind::Function, "bound_value")
                .unwrap()
                .is_none()
        );
        session.execute(&format!("CREATE FUNCTION dynamic_scope() RETURNS INT LANGUAGE {PREVIEW_ROUTINE_LANGUAGE} AS $$BEGIN EXECUTE 'SELECT 7'; RETURN 7; END$$")).unwrap();
        assert_eq!(
            session
                .execute("SELECT dynamic_scope()")
                .unwrap_err()
                .sqlstate(),
            "0A000"
        );
        session.execute("DROP FUNCTION dynamic_scope").unwrap();
    }

    #[test]
    fn preview_routine_namespace_binding_preserves_native_table_privileges() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        session.execute(r#"CREATE SCHEMA "Bound"; CREATE TABLE "Bound".numbers(v INT); INSERT INTO "Bound".numbers VALUES(2)"#).unwrap();
        session.execute(&format!(r#"CREATE FUNCTION "Bound".bound_read() RETURNS INT LANGUAGE {PREVIEW_ROUTINE_LANGUAGE} AS $$DECLARE result INT; BEGIN SELECT v INTO result FROM numbers; RETURN result; END$$"#)).unwrap();
        for source in [
            "CREATE ROLE scope_reader",
            r#"GRANT USAGE ON SCHEMA "Bound" TO scope_reader"#,
            r#"GRANT EXECUTE ON FUNCTION "Bound".bound_read TO scope_reader"#,
            "SET ROLE scope_reader",
        ] {
            session.execute(source).unwrap();
        }
        assert_eq!(
            session
                .execute("SELECT bound_read()")
                .unwrap_err()
                .sqlstate(),
            "42501"
        );
        session.execute("RESET ROLE").unwrap();
        session
            .execute(r#"GRANT SELECT ON "Bound".numbers TO scope_reader"#)
            .unwrap();
        session.execute("SET ROLE scope_reader").unwrap();
        assert_eq!(
            session.execute("SELECT bound_read()").unwrap().rows[0][0],
            SqlValue::Int(2)
        );
    }

    #[test]
    fn postgres_routine_namespace_lookup_retains_native_behavior() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, POSTGRES_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        session.execute(r#"CREATE SCHEMA "Bound"; CREATE TABLE "Bound".numbers(v INT); INSERT INTO "Bound".numbers VALUES(2); CREATE TABLE numbers(v INT); INSERT INTO numbers VALUES(99)"#).unwrap();
        session.execute(r#"CREATE FUNCTION "Bound".native_value() RETURNS INT LANGUAGE plpgsql AS $$DECLARE result INT; BEGIN SELECT v INTO result FROM numbers; RETURN result; END$$"#).unwrap();
        assert_eq!(
            session.execute("SELECT native_value()").unwrap().rows[0][0],
            SqlValue::Int(99)
        );
        assert!(!preview_routine_language_enabled(session.db_ref()).unwrap());
    }

    #[test]
    fn preview_week_numeric_truncation_is_exact_and_dual_keeps_cte_scope() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        for source in ["CREATE TABLE one_row(dummy VARCHAR2(1))","INSERT INTO one_row VALUES('Y')","CREATE FUNCTION dummy_value RETURN VARCHAR2 IS result VARCHAR2(1); BEGIN SELECT DUMMY INTO result FROM DUAL; RETURN result; END;","CREATE FUNCTION cte_dummy RETURN VARCHAR2 IS result VARCHAR2(1); BEGIN WITH DUAL AS (SELECT dummy FROM one_row) SELECT dummy INTO result FROM DUAL; RETURN result; END;","CREATE FUNCTION numeric_clip(p_value NUMBER,p_scale NUMBER) RETURN NUMBER IS BEGIN RETURN TRUNC(p_value,p_scale); END;","CREATE FUNCTION sql_numeric_clip(p_value NUMBER,p_scale NUMBER) RETURN NUMBER IS result NUMBER; BEGIN SELECT TRUNC(p_value,p_scale) INTO result FROM DUAL; RETURN result; END;"] {
            let sql=lower_profile_sql(session.db_ref(),source,ORACLE_PREVIEW_PROFILE).unwrap();session.execute(&sql).unwrap();
        }
        for (source, expected) in [
            ("SELECT dummy_value() FROM DUAL", "X"),
            ("SELECT cte_dummy() FROM DUAL", "Y"),
            (
                "WITH DUAL AS (SELECT dummy FROM one_row) SELECT dummy FROM DUAL",
                "Y",
            ),
            (
                "SELECT TRUNC(CAST('123456789012345678901234.56789' AS NUMBER),2) FROM DUAL",
                "123456789012345678901234.56",
            ),
            (
                "SELECT TRUNC(CAST('123456789012345678901234.56789' AS NUMBER)) FROM DUAL",
                "123456789012345678901234",
            ),
            ("SELECT numeric_clip(123.456,1.9) FROM DUAL", "123.45"),
            ("SELECT numeric_clip(123.456,-1.9) FROM DUAL", "100"),
            ("SELECT numeric_clip(123.456,0.5) FROM DUAL", "123.4"),
            ("SELECT numeric_clip(123.456,-0.5) FROM DUAL", "120"),
            ("SELECT sql_numeric_clip(123.456,1.9) FROM DUAL", "123.4"),
            ("SELECT TRUNC(123.456,1.9) FROM DUAL", "123.4"),
            ("SELECT TRUNC(123.456,-1.9) FROM DUAL", "120"),
        ] {
            let sql = lower_profile_sql(session.db_ref(), source, ORACLE_PREVIEW_PROFILE).unwrap();
            assert_eq!(
                session.execute(&sql).unwrap().rows[0][0].to_cell(),
                expected,
                "{source}"
            );
        }
    }

    #[test]
    fn postgres_week_scaled_numeric_truncation_preserves_precision_and_scale() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        for (sql, expected) in [
            (
                "SELECT trunc(CAST('123456789012345678901234.56789' AS NUMERIC),2)",
                "123456789012345678901234.56",
            ),
            ("SELECT trunc(-123.456,2)", "-123.45"),
            ("SELECT trunc(123.456,-2)", "100"),
            ("SELECT trunc(1.2,4)", "1.2000"),
            ("SELECT trunc(0.001,2)", "0.00"),
            ("SELECT trunc(123.456,-2147483648)", "0"),
            ("SELECT trunc(CAST('NaN' AS NUMERIC),2)", "NaN"),
        ] {
            assert_eq!(
                session.execute(sql).unwrap().rows[0][0].to_cell(),
                expected,
                "{sql}"
            );
            assert_eq!(
                infer_query_result_types(session.db_ref(), sql).unwrap(),
                Some(vec![Some("numeric".into())])
            );
        }
        assert!(session.execute("SELECT trunc(123.456,1.9)").is_err());
        assert!(session
            .execute("SELECT trunc(CAST(123.456 AS DOUBLE PRECISION),2)")
            .is_err());
    }

    #[test]
    fn preview_week_truncation_and_procedural_null_formats_are_distinct() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        for source in [
            "CREATE FUNCTION week_clip(p_date DATE,p_fmt VARCHAR2) RETURN DATE IS BEGIN RETURN TRUNC(p_date,p_fmt); END;",
            "CREATE FUNCTION sql_clip(p_date DATE,p_fmt VARCHAR2) RETURN DATE IS result DATE; BEGIN SELECT TRUNC(p_date,p_fmt) INTO result FROM DUAL; RETURN result; END;",
            "CREATE FUNCTION number_clip(p_value NUMBER,p_scale NUMBER) RETURN NUMBER IS BEGIN RETURN TRUNC(p_value,p_scale); END;",
        ] {let sql=lower_profile_sql(session.db_ref(),source,ORACLE_PREVIEW_PROFILE).unwrap();session.execute(&sql).unwrap();}
        for (date, format, expected) in [
            ("2024-02-29 12:34:56.9", "W", "2024-02-29 00:00:00"),
            ("2024-02-29 12:34:56.9", "WW", "2024-02-26 00:00:00"),
            ("2024-02-29 12:34:56.9", "D", "2024-02-25 00:00:00"),
            ("2024-02-29 12:34:56.9", "DY", "2024-02-25 00:00:00"),
            ("2024-02-29 12:34:56.9", "DAY", "2024-02-25 00:00:00"),
            ("2023-01-01 12:34:56", "WW", "2023-01-01 00:00:00"),
            ("2023-01-01 12:34:56", "IW", "2022-12-26 00:00:00"),
            ("2021-01-01 12:34:56", "IYYY", "2019-12-30 00:00:00"),
            ("2023-01-01 12:34:56", "IY", "2022-01-03 00:00:00"),
        ] {
            for name in ["TRUNC", "week_clip", "sql_clip"] {
                let sql = lower_profile_sql(
                    session.db_ref(),
                    &format!("SELECT {name}(TIMESTAMP '{date}','{format}') FROM DUAL"),
                    ORACLE_PREVIEW_PROFILE,
                )
                .unwrap();
                assert_eq!(
                    session.execute(&sql).unwrap().rows[0][0].to_cell(),
                    expected,
                    "{sql}"
                );
            }
        }
        for source in [
            "SELECT TRUNC(TIMESTAMP '2024-02-29 12:34:56',NULL) FROM DUAL",
            "SELECT sql_clip(TIMESTAMP '2024-02-29 12:34:56',NULL) FROM DUAL",
            "SELECT week_clip(NULL,NULL) FROM DUAL",
            "SELECT number_clip(123.456,NULL) FROM DUAL",
        ] {
            let sql = lower_profile_sql(session.db_ref(), source, ORACLE_PREVIEW_PROFILE).unwrap();
            assert_eq!(
                session.execute(&sql).unwrap().rows[0][0],
                SqlValue::Null,
                "{source}"
            );
        }
        let sql = lower_profile_sql(
            session.db_ref(),
            "SELECT week_clip(TIMESTAMP '2024-02-29 12:34:56',NULL) FROM DUAL",
            ORACLE_PREVIEW_PROFILE,
        )
        .unwrap();
        assert_eq!(session.execute(&sql).unwrap_err().sqlstate(), "22023");
        session.execute(&format!("CREATE FUNCTION legacy_clip(p_date DATE,p_fmt VARCHAR) RETURNS DATE LANGUAGE {PREVIEW_ROUTINE_LANGUAGE_V3} AS $$BEGIN RETURN TRUNC(p_date,p_fmt); END$$")).unwrap();
        assert_eq!(
            session
                .execute("SELECT legacy_clip(TIMESTAMP '2024-02-29 12:34:56',NULL)")
                .unwrap()
                .rows[0][0],
            SqlValue::Null
        );
    }

    #[test]
    fn postgres_week_and_null_format_semantics_remain_native() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session.execute("CREATE FUNCTION native_clip(p_date TIMESTAMP,p_fmt TEXT) RETURNS TIMESTAMP LANGUAGE plpgsql AS $$BEGIN RETURN date_trunc(p_fmt,p_date); END$$").unwrap();
        assert_eq!(
            session
                .execute("SELECT native_clip(TIMESTAMP '2024-02-29 12:34:56.9','week')")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "2024-02-26 00:00:00"
        );
        for sql in [
            "SELECT date_trunc(NULL,TIMESTAMP '2024-02-29 12:34:56.9')",
            "SELECT native_clip(TIMESTAMP '2024-02-29 12:34:56.9',NULL)",
            "SELECT trunc(123.456,CAST(NULL AS INT))",
        ] {
            assert_eq!(session.execute(sql).unwrap().rows[0][0], SqlValue::Null);
        }
    }

    #[test]
    fn preview_add_months_uses_oracle_month_ends_and_exact_month_counts() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        let sql = lower_session_sql(session.db_ref(), "CREATE FUNCTION calendar_shift(p_date DATE,p_months NUMBER) RETURN DATE IS BEGIN RETURN ADD_MONTHS(p_date,p_months)+1; END;", ORACLE_PREVIEW_PROFILE,None,"bicdb").unwrap();
        session.execute(&sql).unwrap();
        for (source,expected) in [
            ("SELECT ADD_MONTHS(TIMESTAMP '2024-01-31 12:34:56.9',1) FROM DUAL", "2024-02-29 12:34:56"),
            ("SELECT ADD_MONTHS(TIMESTAMP '2024-02-29 12:34:56',1) FROM DUAL", "2024-03-31 12:34:56"),
            ("SELECT ADD_MONTHS(TIMESTAMP '2024-02-28 12:34:56',1) FROM DUAL", "2024-03-28 12:34:56"),
            ("SELECT ADD_MONTHS(TIMESTAMP '1900-02-28 12:34:56',1) FROM DUAL", "1900-03-31 12:34:56"),
            ("SELECT ADD_MONTHS(TIMESTAMP '2000-02-29 12:34:56',-12) FROM DUAL", "1999-02-28 12:34:56"),
            ("SELECT ADD_MONTHS(TIMESTAMP '2024-03-31 12:34:56',-1.9) FROM DUAL", "2024-02-29 12:34:56"),
            ("SELECT ADD_MONTHS(TIMESTAMP '2024-01-15 12:34:56',1.999999999999999999999) FROM DUAL", "2024-02-15 12:34:56"),
            ("SELECT ADD_MONTHS(TIMESTAMP '2024-01-15 12:34:56',-0.999999999999999999999) FROM DUAL", "2024-01-15 12:34:56"),
            ("SELECT ADD_MONTHS(TIMESTAMP '2024-01-15 12:34:56','1.9') FROM DUAL", "2024-02-15 12:34:56"),
            ("SELECT calendar_shift(TIMESTAMP '2024-02-29 12:34:56',1) FROM DUAL", "2024-04-01 12:34:56"),
        ] {
            let sql = lower_profile_sql(session.db_ref(),source,ORACLE_PREVIEW_PROFILE).unwrap();
            assert_eq!(session.execute(&sql).unwrap().rows[0][0].to_cell(),expected,"{source}");
            assert_eq!(infer_query_result_types(session.db_ref(),&sql).unwrap(),Some(vec![Some("timestamp".into())]));
        }
        for source in [
            "SELECT ADD_MONTHS(NULL,1) FROM DUAL",
            "SELECT ADD_MONTHS(TIMESTAMP '2024-01-01 00:00:00',NULL) FROM DUAL",
        ] {
            let sql = lower_profile_sql(session.db_ref(), source, ORACLE_PREVIEW_PROFILE).unwrap();
            assert_eq!(session.execute(&sql).unwrap().rows[0][0], SqlValue::Null);
        }
        for source in ["SELECT ADD_MONTHS(TIMESTAMP '9999-12-31 00:00:00',1) FROM DUAL", "SELECT ADD_MONTHS(TIMESTAMP '2024-01-01 00:00:00',9999999999999999999999999999) FROM DUAL"] {
            let sql = lower_profile_sql(session.db_ref(),source,ORACLE_PREVIEW_PROFILE).unwrap();
            assert_eq!(session.execute(&sql).unwrap_err().sqlstate(),"22008");
        }
    }

    #[test]
    fn preview_qualified_calendar_function_names_are_not_builtin_calls() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        for source in ["CREATE TABLE ADD_MONTHS(k NUMBER)", "INSERT INTO ADD_MONTHS(k) VALUES(7)", "CREATE SCHEMA local_calendar", "CREATE FUNCTION local_calendar.ADD_MONTHS(p_date DATE,p_months NUMBER) RETURN DATE IS BEGIN RETURN p_date; END;"] {
            let sql=lower_session_sql(session.db_ref(),source,ORACLE_PREVIEW_PROFILE,None,"bicdb").unwrap();session.execute(&sql).unwrap();
        }
        let sql = lower_session_sql(
            session.db_ref(),
            "SELECT local_calendar.ADD_MONTHS(TIMESTAMP '2024-02-29 12:34:56.9',1) FROM DUAL",
            ORACLE_PREVIEW_PROFILE,
            None,
            "bicdb",
        )
        .unwrap();
        let table_query = lower_session_sql(
            session.db_ref(),
            "SELECT k FROM ADD_MONTHS",
            ORACLE_PREVIEW_PROFILE,
            None,
            "bicdb",
        )
        .unwrap();
        assert_eq!(
            session.execute(&table_query).unwrap().rows[0][0].to_cell(),
            "7"
        );
        assert!(!sql.contains("bicdb_oracle_add_months"));
        assert_eq!(
            session.execute(&sql).unwrap().rows[0][0].to_cell(),
            "2024-02-29 12:34:56"
        );
    }

    #[test]
    fn postgres_month_arithmetic_and_user_function_names_remain_native() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session.execute("CREATE FUNCTION add_months(p_date TIMESTAMP,p_months NUMERIC) RETURNS TIMESTAMP LANGUAGE plpgsql AS $$BEGIN RETURN p_date; END$$").unwrap();
        for (sql, expected) in [
            (
                "SELECT TIMESTAMP '2024-02-29 12:34:56.9'+INTERVAL '1 month'",
                "2024-03-29 12:34:56.9",
            ),
            (
                "SELECT add_months(TIMESTAMP '2024-02-29 12:34:56.9',1)",
                "2024-02-29 12:34:56.9",
            ),
        ] {
            assert_eq!(session.execute(sql).unwrap().rows[0][0].to_cell(), expected);
        }
        drop(session);
        ensure_database_profile(&mut db, POSTGRES_PROFILE).unwrap();
    }

    #[test]
    fn preview_routine_arithmetic_preserves_date_timestamp_distinctions() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        for source in [
            "CREATE FUNCTION date_shift(p_date DATE,p_days NUMBER) RETURN DATE IS BEGIN RETURN p_date+p_days; END;",
            "CREATE FUNCTION date_delta(p_left DATE,p_right DATE) RETURN NUMBER IS BEGIN RETURN p_left-p_right; END;",
            "CREATE FUNCTION stamp_delta(p_left TIMESTAMP,p_right TIMESTAMP) RETURN INTERVAL DAY TO SECOND IS BEGIN RETURN p_left-p_right; END;",
            "CREATE FUNCTION date_cast(p_stamp TIMESTAMP) RETURN DATE IS BEGIN RETURN CAST(p_stamp AS DATE); END;",
            "CREATE FUNCTION stamp_round(p_stamp TIMESTAMP) RETURN TIMESTAMP IS result TIMESTAMP(0):=p_stamp; BEGIN RETURN result; END;",
            "CREATE FUNCTION date_literal RETURN DATE IS BEGIN RETURN DATE '2024-02-29'+1; END;",
        ] {let sql=lower_session_sql(session.db_ref(),source,ORACLE_PREVIEW_PROFILE,None,"bicdb").unwrap();session.execute(&sql).unwrap();}
        for (source,expected) in [
            ("SELECT date_shift(TIMESTAMP '2024-02-29 23:59:59.9',0) FROM DUAL","2024-02-29 23:59:59"),
            ("SELECT date_shift(TIMESTAMP '2024-02-29 23:59:59.9',0.25) FROM DUAL","2024-03-01 05:59:59"),
            ("SELECT date_shift(TIMESTAMP '2024-03-01 12:00:00',-1) FROM DUAL","2024-02-29 12:00:00"),
            ("SELECT date_cast(TIMESTAMP '2024-02-29 23:59:59.9') FROM DUAL","2024-02-29 23:59:59"),
            ("SELECT stamp_round(TIMESTAMP '2024-02-29 23:59:59.9') FROM DUAL","2024-03-01 00:00:00"),
            ("SELECT date_literal() FROM DUAL","2024-03-01 00:00:00"),
            ("SELECT date_delta(TIMESTAMP '2024-03-01 00:00:01',TIMESTAMP '2024-03-01 00:00:00') FROM DUAL","0.00001157407407407407407407407407407407407407"),
        ] {let sql=lower_profile_sql(session.db_ref(),source,ORACLE_PREVIEW_PROFILE).unwrap();assert_eq!(session.execute(&sql).unwrap().rows[0][0].to_cell(),expected,"{source}");}
        let sql=lower_profile_sql(session.db_ref(),"SELECT stamp_delta(TIMESTAMP '2024-03-01 00:00:01.123456',TIMESTAMP '2024-03-01 00:00:00.123455') FROM DUAL",ORACLE_PREVIEW_PROFILE).unwrap();
        let result = session.execute(&sql).unwrap();
        assert_eq!(
            infer_query_result_types(session.db_ref(), &sql).unwrap(),
            Some(vec![Some("interval".into())])
        );
        assert_eq!(
            PgInterval::from_postgres_text(&result.rows[0][0].to_cell()).unwrap(),
            PgInterval {
                months: 0,
                days: 0,
                micros: 1_000_001
            }
        );
        for source in [
            "SELECT date_shift(NULL,1) FROM DUAL",
            "SELECT date_shift(TIMESTAMP '2024-03-01 00:00:00',NULL) FROM DUAL",
            "SELECT date_delta(NULL,NULL) FROM DUAL",
        ] {
            let sql = lower_profile_sql(session.db_ref(), source, ORACLE_PREVIEW_PROFILE).unwrap();
            assert_eq!(session.execute(&sql).unwrap().rows[0][0], SqlValue::Null);
        }
    }

    #[test]
    fn preview_routine_cursor_operand_types_recompile_after_schema_changes() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        for source in ["CREATE TABLE offsets(k NUMBER,days NUMBER)","INSERT INTO offsets VALUES(1,2)","CREATE FUNCTION row_shift(p_date DATE,p_key NUMBER) RETURN DATE IS result DATE:=p_date; CURSOR c IS SELECT * FROM offsets WHERE k=p_key; BEGIN FOR r IN c LOOP result:=result+r.days; END LOOP; RETURN result; END;"] {let sql=lower_session_sql(session.db_ref(),source,ORACLE_PREVIEW_PROFILE,None,"bicdb").unwrap();session.execute(&sql).unwrap();}
        let call = lower_profile_sql(
            session.db_ref(),
            "SELECT row_shift(TIMESTAMP '2024-02-28 12:34:56',1) FROM DUAL",
            ORACLE_PREVIEW_PROFILE,
        )
        .unwrap();
        assert_eq!(
            session.execute(&call).unwrap().rows[0][0].to_cell(),
            "2024-03-01 12:34:56"
        );
        let sql = lower_profile_sql(
            session.db_ref(),
            "ALTER TABLE offsets RENAME COLUMN days TO other_days",
            ORACLE_PREVIEW_PROFILE,
        )
        .unwrap();
        session.execute(&sql).unwrap();
        assert_eq!(session.execute(&call).unwrap_err().sqlstate(), "0A000");
    }

    #[test]
    fn typed_routines_reject_unresolved_temporal_provenance_and_precision() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        for source in ["CREATE TABLE temporal_rows(k NUMBER,happened DATE)","CREATE FUNCTION unresolved_delta(p_date DATE) RETURN NUMBER IS result NUMBER:=0; CURSOR c IS SELECT happened FROM temporal_rows; BEGIN FOR r IN c LOOP result:=p_date-r.happened; END LOOP; RETURN result; END;","CREATE FUNCTION overprecise_stamp RETURN TIMESTAMP IS BEGIN RETURN TIMESTAMP '2024-01-01 23:59:59.9999999'; END;"] {let sql=lower_profile_sql(session.db_ref(),source,ORACLE_PREVIEW_PROFILE).unwrap();session.execute(&sql).unwrap();}
        let table = relation_name_from_parts(&["public".into(), "TEMPORAL_ROWS".into()]).unwrap();
        let mut schema = load_schema(session.db_ref(), &table).unwrap().unwrap();
        schema
            .columns
            .iter_mut()
            .find(|column| column.name == "HAPPENED")
            .unwrap()
            .type_modifier = Some(PgTypeModifier::Temporal { precision: 0 });
        save_schema(session.db_mut().unwrap(), &schema).unwrap();
        for source in [
            "SELECT unresolved_delta(TIMESTAMP '2024-01-01 00:00:00') FROM DUAL",
            "SELECT overprecise_stamp() FROM DUAL",
        ] {
            let sql = lower_profile_sql(session.db_ref(), source, ORACLE_PREVIEW_PROFILE).unwrap();
            assert_eq!(session.execute(&sql).unwrap_err().sqlstate(), "0A000");
        }
    }

    #[test]
    fn postgres_date_casts_and_timestamp_arithmetic_remain_native() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session.execute("CREATE FUNCTION native_date_cast(p_stamp TIMESTAMP) RETURNS DATE LANGUAGE plpgsql AS $$BEGIN RETURN CAST(p_stamp AS DATE); END$$").unwrap();
        let result = session
            .execute("SELECT native_date_cast(TIMESTAMP '2024-02-29 23:59:59.9')")
            .unwrap();
        assert_eq!(
            infer_query_result_types(
                session.db_ref(),
                "SELECT native_date_cast(TIMESTAMP '2024-02-29 23:59:59.9')"
            )
            .unwrap(),
            Some(vec![Some("date".into())])
        );
        assert_eq!(result.rows[0][0].to_cell(), "2024-02-29");
        assert_eq!(
            session
                .execute("SELECT CAST(TIMESTAMP '2024-02-29 23:59:59.9' AS TIMESTAMP(0))")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "2024-03-01 00:00:00"
        );
        assert!(session
            .execute("SELECT TIMESTAMP '2024-03-01 00:00:00'+1")
            .is_err());
        let result=session.execute("SELECT TIMESTAMP '2024-03-01 00:00:01.123456'-TIMESTAMP '2024-03-01 00:00:00.123455'").unwrap();
        assert_eq!(infer_query_result_types(session.db_ref(),"SELECT TIMESTAMP '2024-03-01 00:00:01.123456'-TIMESTAMP '2024-03-01 00:00:00.123455'").unwrap(),Some(vec![Some("interval".into())]));
        assert_eq!(
            PgInterval::from_postgres_text(&result.rows[0][0].to_cell()).unwrap(),
            PgInterval {
                months: 0,
                days: 0,
                micros: 1_000_001
            }
        );
    }
    #[test]
    fn preview_number_division_keeps_fractional_results_and_oracle_mantissa_precision() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        for (source, expected) in [
            ("SELECT 7/2 FROM DUAL", "3.5".to_string()),
            ("SELECT 1/3 FROM DUAL", format!("0.{}", "3".repeat(40))),
            ("SELECT -1/3 FROM DUAL", format!("-0.{}", "3".repeat(40))),
            (
                "SELECT 22/7 FROM DUAL",
                "3.14285714285714285714285714285714285714".into(),
            ),
            (
                "SELECT 111/7 FROM DUAL",
                "15.85714285714285714285714285714285714286".into(),
            ),
            (
                "SELECT CASE WHEN 1=1 THEN 7/2 ELSE 1/0 END FROM DUAL",
                "3.5".into(),
            ),
        ] {
            let sql = lower_profile_sql(&db, source, ORACLE_PREVIEW_PROFILE).unwrap();
            let result = SqlSession::new(&mut db)
                .execute(&sql)
                .unwrap_or_else(|e| panic!("{source}: {sql}: {e}"));
            let value = result.rows[0][0].to_cell();
            let value = if value.contains('.') {
                value.trim_end_matches('0').trim_end_matches('.')
            } else {
                &value
            };
            assert_eq!(value, expected, "{source}");
            assert_eq!(result.column_types, vec![Some("numeric".into())]);
        }
        for source in [
            "SELECT 1/0 FROM DUAL",
            "SELECT 1e125/0.1 FROM DUAL",
            "SELECT 1e-130/100 FROM DUAL",
        ] {
            let sql = lower_profile_sql(&db, source, ORACLE_PREVIEW_PROFILE).unwrap();
            assert!(SqlSession::new(&mut db).execute(&sql).is_err(), "{source}");
        }
        let sql =
            lower_profile_sql(&db, "SELECT NULL/0 FROM DUAL", ORACLE_PREVIEW_PROFILE).unwrap();
        assert!(matches!(
            SqlSession::new(&mut db).execute(&sql).unwrap().rows[0][0],
            SqlValue::Null
        ));
        for (source,expected) in [
            ("SELECT TO_CHAR(TIMESTAMP '2000-01-01 00:00:00'+1/86400,'YYYY-MM-DD HH24:MI:SS') FROM DUAL","2000-01-01 00:00:01"),
            ("SELECT TO_CHAR(TIMESTAMP '2000-01-01 00:00:00'+0.5/86400,'YYYY-MM-DD HH24:MI:SS') FROM DUAL","2000-01-01 00:00:01"),
            ("SELECT TO_CHAR(TIMESTAMP '2000-01-01 00:00:00'-0.5/86400,'YYYY-MM-DD HH24:MI:SS') FROM DUAL","1999-12-31 23:59:59"),
        ] {
            let sql=lower_profile_sql(&db,source,ORACLE_PREVIEW_PROFILE).unwrap();
            assert_eq!(SqlSession::new(&mut db).execute(&sql).unwrap().rows[0][0].to_cell(),expected,"{source}: {sql}");
        }
        assert_eq!(
            SqlSession::new(&mut db).execute("SELECT 7/2").unwrap().rows[0][0].to_cell(),
            "3"
        );
    }

    #[test]
    fn preview_numeric_day_offsets_round_exactly_and_leave_postgres_arithmetic_alone() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        for (source, expected) in [
            (
                "SELECT TIMESTAMP '2000-01-01 12:00:00.9'+1 FROM DUAL",
                "2000-01-02 12:00:00",
            ),
            (
                "SELECT 0.25+TIMESTAMP '2000-01-01 12:00:00' FROM DUAL",
                "2000-01-01 18:00:00",
            ),
            (
                "SELECT TIMESTAMP '2000-01-01 00:00:00'-0.25 FROM DUAL",
                "1999-12-31 18:00:00",
            ),
            (
                "SELECT TIMESTAMP '2000-01-01 00:00:00'+0.000005787037037037037037 FROM DUAL",
                "2000-01-01 00:00:00",
            ),
            (
                "SELECT TIMESTAMP '2000-01-01 00:00:00'+0.000005787037037037037038 FROM DUAL",
                "2000-01-01 00:00:01",
            ),
            (
                "SELECT TIMESTAMP '2000-01-01 00:00:00'-0.000005787037037037037038 FROM DUAL",
                "1999-12-31 23:59:59",
            ),
            (
                "SELECT TIMESTAMP '2000-01-01 00:00:00'+'1' FROM DUAL",
                "2000-01-02 00:00:00",
            ),
        ] {
            let sql = lower_profile_sql(&db, source, ORACLE_PREVIEW_PROFILE).unwrap();
            let result = SqlSession::new(&mut db)
                .execute(&sql)
                .unwrap_or_else(|error| panic!("{source}: {sql}: {error}"));
            assert_eq!(result.rows[0][0].to_cell(), expected, "{source}: {sql}");
            assert_eq!(result.column_types, vec![Some("timestamp".into())]);
        }
        for source in [
            "SELECT TIMESTAMP '2000-01-01 00:00:00'+NULL FROM DUAL",
            "SELECT NULL+TIMESTAMP '2000-01-01 00:00:00' FROM DUAL",
        ] {
            let sql = lower_profile_sql(&db, source, ORACLE_PREVIEW_PROFILE).unwrap();
            assert!(matches!(
                SqlSession::new(&mut db).execute(&sql).unwrap().rows[0][0],
                SqlValue::Null
            ));
        }
        for source in [
            "SELECT TIMESTAMP '9999-12-31 00:00:00'+1 FROM DUAL",
            "SELECT TIMESTAMP '2000-01-01 00:00:00'+'invalid' FROM DUAL",
        ] {
            let sql = lower_profile_sql(&db, source, ORACLE_PREVIEW_PROFILE).unwrap();
            assert!(SqlSession::new(&mut db).execute(&sql).is_err());
        }
        for source in [
            "CREATE SCHEMA offsets",
            "CREATE TABLE offsets.events(k NUMBER PRIMARY KEY,happened DATE,days NUMBER)",
            "INSERT INTO offsets.events VALUES(1,TIMESTAMP '2000-01-01 12:00:00',0.25)",
        ] {
            let sql = lower_profile_sql(&db, source, ORACLE_PREVIEW_PROFILE).unwrap();
            SqlSession::new(&mut db).execute(&sql).unwrap();
        }
        for source in ["SELECT happened+days FROM events", "WITH days AS (SELECT happened+days AS next_day FROM events) SELECT next_day+1 FROM days"] {
            let sql=lower_session_sql(&db,source,ORACLE_PREVIEW_PROFILE,Some("offsets"),"bicdb").unwrap();
            let value=SqlSession::new(&mut db).execute(&sql).unwrap_or_else(|error|panic!("{source}: {sql}: {error}")).rows[0][0].to_cell();
            assert_eq!(value,if source.starts_with("WITH") {"2000-01-02 18:00:00"} else {"2000-01-01 18:00:00"},"{sql}");
        }
        assert!(SqlSession::new(&mut db)
            .execute("SELECT TIMESTAMP '2000-01-01 00:00:00'+1")
            .is_err());
        assert_eq!(
            SqlSession::new(&mut db).execute("SELECT 7/2").unwrap().rows[0][0].to_cell(),
            "3"
        );
    }

    #[test]
    fn preview_extract_fields_and_case_aggregates_keep_their_grammar() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        for source in [
            "CREATE TABLE extract_events(k NUMBER PRIMARY KEY, happened TIMESTAMP(6), year NUMBER)",
            "INSERT INTO extract_events VALUES(1,TIMESTAMP '2002-02-03 04:05:06',1999),(2,TIMESTAMP '2024-03-04 05:06:07',1998)",
        ] {
            let sql = lower_profile_sql(&db, source, ORACLE_PREVIEW_PROFILE).unwrap();
            SqlSession::new(&mut db).execute(&sql).unwrap();
        }
        for (sql, expected) in [
            ("SELECT EXTRACT(YEAR FROM happened),year FROM extract_events WHERE k=1",vec!["2002","1999"]),
            ("SELECT SUM(CASE WHEN EXTRACT(YEAR FROM happened)=2002 THEN 1 ELSE 0 END), SUM(CASE WHEN EXTRACT(MONTH FROM happened)=3 THEN 1 ELSE 0 END) FROM extract_events",vec!["1","1"]),
            ("SELECT EXTRACT(DAY FROM happened),EXTRACT(HOUR FROM happened),EXTRACT(MINUTE FROM happened),EXTRACT(SECOND FROM happened) FROM extract_events WHERE k=1",vec!["3","4","5","6.000000"]),
            ("SELECT EXTRACT(YEAR FROM TO_DATE('2002-02-03','YYYY-MM-DD')) FROM DUAL",vec!["2002"]),
        ] {
            let lowered = lower_profile_sql(&db, sql, ORACLE_PREVIEW_PROFILE).unwrap();
            let result = SqlSession::new(&mut db).execute(&lowered).unwrap();
            assert_eq!(result.rows[0].iter().map(SqlValue::to_cell).collect::<Vec<_>>(),expected, "{sql}");
        }
        let invalid = lower_profile_sql(
            &db,
            "SELECT EXTRACT(unknown_part FROM happened) FROM extract_events",
            ORACLE_PREVIEW_PROFILE,
        );
        assert!(match invalid {
            Err(_) => true,
            Ok(sql) => SqlSession::new(&mut db).execute(&sql).is_err(),
        });
        let native = SqlSession::new(&mut db)
            .execute("SELECT EXTRACT(YEAR FROM TIMESTAMP '2024-03-04 05:06:07')")
            .unwrap();
        assert_eq!(native.rows[0][0].to_cell(), "2024");
    }

    #[test]
    fn preview_datetime_trunc_uses_column_types_and_keeps_postgres_trunc_numeric() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        for source in [
            "CREATE SCHEMA calendar",
            "CREATE TABLE calendar.events(k NUMBER PRIMARY KEY, happened DATE)",
            "INSERT INTO calendar.events VALUES(1,TIMESTAMP '2024-02-29 23:59:58')",
        ] {
            let sql = lower_profile_sql(&db, source, ORACLE_PREVIEW_PROFILE).unwrap();
            SqlSession::new(&mut db).execute(&sql).unwrap();
        }
        for source in [
            "SELECT TRUNC(happened,'MM') FROM events",
            "WITH days AS (SELECT TRUNC(happened) AS d FROM events) SELECT TRUNC(d,'MM') FROM days",
        ] {
            let sql = lower_session_sql(
                &db,
                source,
                ORACLE_PREVIEW_PROFILE,
                Some("calendar"),
                "bicdb",
            )
            .unwrap();
            let result = SqlSession::new(&mut db).execute(&sql).unwrap();
            assert_eq!(result.column_types, vec![Some("timestamp".into())]);
            assert_eq!(result.rows[0][0].to_cell(), "2024-02-01 00:00:00");
        }
        let index = lower_session_sql(
            &db,
            "CREATE INDEX event_day ON events(TRUNC(happened))",
            ORACLE_PREVIEW_PROFILE,
            Some("calendar"),
            "bicdb",
        )
        .unwrap();
        assert!(index.contains("bicdb_oracle_trunc_date"));
        SqlSession::new(&mut db).execute(&index).unwrap();
        let indexed = lower_session_sql(
            &db,
            "SELECT k FROM events WHERE TRUNC(happened)=TIMESTAMP '2024-02-29 00:00:00'",
            ORACLE_PREVIEW_PROFILE,
            Some("calendar"),
            "bicdb",
        )
        .unwrap();
        assert_eq!(
            SqlSession::new(&mut db)
                .execute(&indexed)
                .unwrap()
                .rows
                .len(),
            1
        );
        let update = lower_session_sql(
            &db,
            "UPDATE events SET happened=TRUNC(happened)",
            ORACLE_PREVIEW_PROFILE,
            Some("calendar"),
            "bicdb",
        )
        .unwrap();
        SqlSession::new(&mut db).execute(&update).unwrap();
        let result = SqlSession::new(&mut db)
            .execute("SELECT \"HAPPENED\" FROM \"CALENDAR\".\"EVENTS\"")
            .unwrap();
        assert_eq!(result.rows[0][0].to_cell(), "2024-02-29 00:00:00");
        assert_eq!(
            SqlSession::new(&mut db)
                .execute("SELECT trunc(123.75)")
                .unwrap()
                .rows[0][0],
            SqlValue::Float(123.0)
        );
        assert!(SqlSession::new(&mut db)
            .execute("SELECT trunc(TIMESTAMP '2024-02-29 23:59:58')")
            .is_err());
        let invalid = lower_profile_sql(
            &db,
            "SELECT TRUNC(TIMESTAMP '2024-02-29 23:59:58',NULL,NULL) FROM DUAL",
            ORACLE_PREVIEW_PROFILE,
        )
        .unwrap();
        assert!(SqlSession::new(&mut db).execute(&invalid).is_err());
    }
    #[test]
    fn preview_session_schema_resolves_tables_and_preserves_cte_and_quoted_names() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        for source in [
            "CREATE SCHEMA red",
            "CREATE SCHEMA blue",
            "CREATE TABLE red.entries(k NUMBER PRIMARY KEY, value VARCHAR2(20))",
            "CREATE TABLE blue.entries(k NUMBER PRIMARY KEY, value VARCHAR2(20))",
            "INSERT INTO red.entries VALUES(1,'red')",
            "INSERT INTO blue.entries VALUES(1,'blue')",
        ] {
            let sql = lower_profile_sql(&db, source, ORACLE_PREVIEW_PROFILE).unwrap();
            SqlSession::new(&mut db).execute(&sql).unwrap();
        }
        for (path, expected) in [("\"RED\"", "red"), ("\"BLUE\"", "blue")] {
            let sql = lower_session_sql(
                &db,
                "WITH chosen AS (SELECT value FROM entries WHERE k=1) SELECT value FROM chosen",
                ORACLE_PREVIEW_PROFILE,
                Some(path),
                "bicdb",
            )
            .unwrap();
            assert_eq!(
                SqlSession::new(&mut db).execute(&sql).unwrap().rows,
                vec![vec![SqlValue::String(expected.into())]]
            );
        }
        let own = lower_session_sql(
            &db,
            "SELECT value FROM entries",
            ORACLE_PREVIEW_PROFILE,
            None,
            "red",
        )
        .unwrap();
        assert_eq!(
            SqlSession::new(&mut db).execute(&own).unwrap().rows,
            vec![vec![SqlValue::String("red".into())]]
        );
        let missing = lower_session_sql(
            &db,
            "SELECT value FROM entries",
            ORACLE_PREVIEW_PROFILE,
            Some("\"ABSENT\""),
            "red",
        )
        .unwrap();
        assert!(SqlSession::new(&mut db).execute(&missing).is_err());
        assert!(lower_session_sql(
            &db,
            "SELECT value FROM entries",
            ORACLE_PREVIEW_PROFILE,
            Some(""),
            "red"
        )
        .is_err());
    }
    #[test]
    fn preview_has_a_separate_durable_profile_and_executes_logical_schema_checks() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        assert!(ensure_database_profile(&mut db, ORACLE_HR_PROFILE).is_err());
        assert!(ensure_database_profile(&mut db, POSTGRES_PROFILE).is_err());
        for sql in [
            "CREATE SCHEMA app",
            "CREATE TABLE app.docs(id NUMBER PRIMARY KEY, label VARCHAR2(2 CHAR), payload BLOB, document CLOB CHECK(document IS JSON)) CACHE",
            r#"INSERT INTO app.docs VALUES(1, 'éé', CAST('\x00ff' AS BYTEA), '{"ok":true}')"#,
        ] {
            let lowered = lower_profile_sql(&db, sql, ORACLE_PREVIEW_PROFILE).unwrap();
            SqlSession::new(&mut db).execute(&lowered).unwrap();
        }
        let query = lower_profile_sql(
            &db,
            "SELECT octet_length(label), octet_length(payload) FROM app.docs",
            ORACLE_PREVIEW_PROFILE,
        )
        .unwrap();
        assert_eq!(
            SqlSession::new(&mut db).execute(&query).unwrap().rows,
            vec![vec![SqlValue::Int(4), SqlValue::Int(2)]]
        );
        let invalid = lower_profile_sql(
            &db,
            "INSERT INTO app.docs VALUES(2, 'ok', NULL, 'invalid json')",
            ORACLE_PREVIEW_PROFILE,
        )
        .unwrap();
        assert_eq!(
            SqlSession::new(&mut db)
                .execute(&invalid)
                .unwrap_err()
                .sqlstate(),
            "23514"
        );
        assert!(lower_profile_sql(
            &db,
            "CREATE TABLE disabled_test(id NUMBER PRIMARY KEY DISABLE)",
            ORACLE_PREVIEW_PROFILE
        )
        .is_err());
    }

    #[test]
    fn hr_to_date_preserves_time_without_changing_postgres_to_date() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        for (source, expected) in [
            (
                "SELECT TO_DATE('2020-02-29 12:34:56','YYYY-MM-DD HH24:MI:SS') FROM DUAL",
                "2020-02-29 12:34:56",
            ),
            (
                "SELECT TO_DATE('2020-02-29','YYYY-MM-DD') FROM DUAL",
                "2020-02-29 00:00:00",
            ),
        ] {
            let lowered = lower_hr_sql(&db, source).unwrap();
            let result = SqlSession::new(&mut db).execute(&lowered).unwrap();
            assert_eq!(result.rows[0][0].to_cell(), expected);
            assert_eq!(result.column_types, vec![Some("timestamp".into())]);
        }
        let mut session = SqlSession::new(&mut db);
        let result = session
            .execute("SELECT to_date('2020-02-29 12:34:56','YYYY-MM-DD HH24:MI:SS')")
            .unwrap();
        assert_eq!(result.rows[0][0].to_cell(), "2020-02-29");
        assert_eq!(result.column_types, vec![Some("date".into())]);
    }

    #[test]
    fn hr_front_end_preserves_postgres_and_lowers_oracle() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        let sql=lower_hr_sql(&db,"CREATE TABLE employees (employee_id NUMBER(6) PRIMARY KEY, name VARCHAR2(20), hired DATE)").unwrap();
        SqlSession::new(&mut db).execute(&sql).unwrap();
        let insert = lower_hr_sql(
            &db,
            "INSERT INTO employees VALUES (1, '', DATE '2020-01-02')",
        )
        .unwrap();
        SqlSession::new(&mut db).execute(&insert).unwrap();
        let query = lower_hr_sql(
            &db,
            "SELECT NVL(name, 'unknown'), 'A' || NULL FROM employees",
        )
        .unwrap();
        let result = SqlSession::new(&mut db).execute(&query).unwrap();
        assert_eq!(
            result.rows,
            vec![vec![
                SqlValue::String("unknown".into()),
                SqlValue::String("A".into())
            ]]
        );
        let dual = lower_hr_sql(&db, "SELECT DUMMY FROM DUAL").unwrap();
        assert_eq!(
            SqlSession::new(&mut db).execute(&dual).unwrap().rows,
            vec![vec![SqlValue::String("X".into())]]
        );
        assert_eq!(
            SqlSession::new(&mut db)
                .execute("SELECT '' IS NULL")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Bool(false)]]
        );
    }

    #[test]
    fn published_hr_schema_and_data_execute() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../examples/oracle/hr/migration.json"))
                .unwrap();
        for (index, sql) in fixture["statements"].as_array().unwrap().iter().enumerate() {
            let source = sql.as_str().unwrap();
            let lowered = lower_hr_sql(&db, source)
                .unwrap_or_else(|e| panic!("lower #{index}: {e}; source: {source}"));
            SqlSession::new(&mut db)
                .execute(&lowered)
                .unwrap_or_else(|e| panic!("execute #{index}: {e}; lowered: {lowered}"));
        }
        let query = lower_hr_sql(&db, "SELECT COUNT(*) FROM employees").unwrap();
        let result = SqlSession::new(&mut db).execute(&query).unwrap();
        assert_eq!(result.rows, vec![vec![SqlValue::Int(107)]]);
        let join=lower_hr_sql(&db,"SELECT e.department_id, d.department_id FROM employees e JOIN departments d ON e.department_id=d.department_id WHERE e.employee_id=100").unwrap();
        assert_eq!(
            SqlSession::new(&mut db).execute(&join).unwrap().rows,
            vec![vec![
                SqlValue::String("90".into()),
                SqlValue::String("90".into())
            ]]
        );
        let view = lower_hr_sql(&db, "SELECT COUNT(*) FROM emp_details_view").unwrap();
        assert_eq!(
            SqlSession::new(&mut db).execute(&view).unwrap().rows,
            vec![vec![SqlValue::Int(106)]]
        );
    }

    #[test]
    fn profile_is_durable_and_cannot_convert_existing_postgres_tables() {
        let root = tempfile::tempdir().unwrap();
        {
            let mut db = BicDb::open(root.path()).unwrap();
            ensure_database_profile(&mut db, ORACLE_HR_PROFILE).unwrap();
        }
        let mut db = BicDb::open(root.path()).unwrap();
        assert!(ensure_database_profile(&mut db, POSTGRES_PROFILE).is_err());
        ensure_database_profile(&mut db, ORACLE_HR_PROFILE).unwrap();
        assert!(ensure_database_profile(&mut db, "unknown").is_err());
        let postgres = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(postgres.path()).unwrap();
        SqlSession::new(&mut db)
            .execute("CREATE TABLE existing(id int PRIMARY KEY)")
            .unwrap();
        assert!(ensure_database_profile(&mut db, ORACLE_HR_PROFILE).is_err());
        ensure_database_profile(&mut db, POSTGRES_PROFILE).unwrap();
    }
}
