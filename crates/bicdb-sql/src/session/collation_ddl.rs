//! CREATE / ALTER / DROP COLLATION.
use crate::collations::{
    canonical_icu_locale, delete_collation, icu_locale_is_supported, load_collation,
    save_collation, CollationSchema,
};
use crate::*;
use sqlparser::ast::{
    AlterCollation, AlterCollationOperation, CreateCollation, CreateCollationDefinition,
};

const BUILTIN_COLLATIONS: [&str; 5] = ["default", "C", "POSIX", "ucs_basic", "en-x-icu"];

fn collation_identity(name: &ObjectName) -> Result<(String, String)> {
    let full = object_name(name)?;
    Ok(match full.rsplit_once('.') {
        Some((schema, name)) => (schema.to_string(), name.to_string()),
        None => ("public".to_string(), full),
    })
}

fn option_text(value: &Expr) -> Result<String> {
    match value {
        Expr::Value(value) => match &value.value {
            sqlparser::ast::Value::SingleQuotedString(text)
            | sqlparser::ast::Value::DoubleQuotedString(text) => Ok(text.clone()),
            sqlparser::ast::Value::Boolean(flag) => Ok(flag.to_string()),
            other => Ok(other.to_string()),
        },
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        other => Ok(other.to_string()),
    }
}

fn undefined_collation(name: &str) -> SqlError {
    SqlError::data_exception(
        "42704",
        format!("collation \"{name}\" for encoding UTF8 does not exist"),
        None,
    )
}

impl SqlSession<'_> {
    fn record_collation_undo(&mut self, schema_name: &str, name: &str) -> Result<()> {
        if self.tx.is_some() {
            let previous = load_collation(self.db_ref(), schema_name, name)?;
            self.ddl_undo.push(DdlUndo::RestoreCollation {
                schema_name: schema_name.to_string(),
                name: name.to_string(),
                previous: previous.map(Box::new),
            });
        }
        Ok(())
    }

    fn store_collation(&mut self, collation: &CollationSchema) -> Result<()> {
        self.record_collation_undo(&collation.schema_name, &collation.name)?;
        save_collation(self.db_mut()?, collation)
    }

    fn remove_collation(&mut self, schema_name: &str, name: &str) -> Result<()> {
        self.record_collation_undo(schema_name, name)?;
        delete_collation(self.db_mut()?, schema_name, name)
    }

    fn collation_name_taken(&self, schema_name: &str, name: &str) -> Result<bool> {
        Ok(load_collation(self.db_ref(), schema_name, name)?.is_some()
            || (matches!(schema_name, "public" | "pg_catalog")
                && BUILTIN_COLLATIONS
                    .iter()
                    .any(|builtin| builtin.eq_ignore_ascii_case(name)))
            || (schema_name == "pg_catalog"
                && crate::collations::builtin_icu_collation(name).is_some()))
    }

    pub(crate) fn execute_create_collation(
        &mut self,
        create: &CreateCollation,
    ) -> Result<SqlResult> {
        let (schema_name, name) = collation_identity(&create.name)?;
        if schema_name != "public" && load_namespace(self.db_ref(), &schema_name)?.is_none() {
            return Err(SqlError::data_exception(
                "3F000",
                format!("schema \"{schema_name}\" does not exist"),
                None,
            ));
        }
        if self.collation_name_taken(&schema_name, &name)? {
            if create.if_not_exists {
                return Ok(SqlResult::command("CREATE COLLATION"));
            }
            return Err(SqlError::data_exception(
                "42710",
                format!("collation \"{name}\" already exists"),
                None,
            ));
        }
        let (provider, locale, deterministic) = match &create.definition {
            CreateCollationDefinition::From(existing) => {
                let (existing_schema, existing_name) = collation_identity(existing)?;
                match existing_name.to_ascii_lowercase().as_str() {
                    "c" | "posix" | "default" | "ucs_basic" => {
                        // Byte order: the libc C locale.
                        ('c', "C".to_string(), true)
                    }
                    "en-x-icu" => ('i', "en".to_string(), true),
                    _ => {
                        let can_use_builtin = existing_schema == "pg_catalog"
                            || object_name_parts(existing).len() == 1;
                        if let Some(builtin) = can_use_builtin
                            .then(|| crate::collations::builtin_icu_collation(&existing_name))
                            .flatten()
                        {
                            ('i', builtin.locale.to_string(), true)
                        } else {
                            let source =
                                load_collation(self.db_ref(), &existing_schema, &existing_name)?
                                    .ok_or_else(|| undefined_collation(&existing_name))?;
                            (source.provider, source.locale, source.deterministic)
                        }
                    }
                }
            }
            CreateCollationDefinition::Options(options) => {
                let mut provider = None;
                let mut locale = None;
                let mut lc_collate = None;
                let mut lc_ctype = None;
                let mut deterministic = true;
                for option in options {
                    let SqlOption::KeyValue { key, value } = option else {
                        return Err(SqlError::InvalidSql(format!(
                            "unsupported CREATE COLLATION option {option}"
                        )));
                    };
                    let value = option_text(value)?;
                    match key.value.to_ascii_lowercase().as_str() {
                        "provider" => provider = Some(value.to_ascii_lowercase()),
                        "locale" => locale = Some(value),
                        "lc_collate" => lc_collate = Some(value),
                        "lc_ctype" => lc_ctype = Some(value),
                        "deterministic" => {
                            deterministic = match value.to_ascii_lowercase().as_str() {
                                "true" | "on" | "yes" | "1" => true,
                                "false" | "off" | "no" | "0" => false,
                                _ => {
                                    return Err(SqlError::data_exception(
                                        "22023",
                                        format!("deterministic requires a Boolean value"),
                                        None,
                                    ))
                                }
                            }
                        }
                        "version" => {}
                        "rules" => {
                            return Err(SqlError::Unsupported(
                                "CREATE COLLATION with ICU tailoring rules is not supported".into(),
                            ))
                        }
                        other => {
                            return Err(SqlError::data_exception(
                                "42601",
                                format!("collation attribute \"{other}\" not recognized"),
                                None,
                            ))
                        }
                    }
                }
                let provider = match provider.as_deref() {
                    None | Some("libc") => 'c',
                    Some("icu") => 'i',
                    Some("builtin") => {
                        return Err(SqlError::Unsupported(
                            "the builtin collation provider is not supported".into(),
                        ))
                    }
                    Some(other) => {
                        return Err(SqlError::data_exception(
                            "22023",
                            format!("unrecognized collation provider: {other}"),
                            None,
                        ))
                    }
                };
                let locale = locale
                    .or_else(|| lc_collate.clone())
                    .or_else(|| lc_ctype.clone());
                let Some(locale) = locale else {
                    return Err(SqlError::data_exception(
                        "42P17",
                        if provider == 'i' {
                            "parameter \"locale\" must be specified"
                        } else {
                            "parameter \"lc_collate\" must be specified"
                        },
                        None,
                    ));
                };
                if provider == 'c' {
                    if !deterministic {
                        return Err(SqlError::data_exception(
                            "0A000",
                            "nondeterministic collations not supported with this provider",
                            None,
                        ));
                    }
                    // Only the byte-order locales have a platform-independent
                    // meaning; others would sort differently from PostgreSQL.
                    match locale.as_str() {
                        "C" | "POSIX" | "C.UTF-8" | "C.utf8" => ('c', locale, true),
                        _ => {
                            return Err(SqlError::Unsupported(format!(
                                "libc collation locale \"{locale}\" is not supported; use provider = icu"
                            )))
                        }
                    }
                } else {
                    let canonical = canonical_icu_locale(&locale)?;
                    if !icu_locale_is_supported(&canonical) {
                        return Err(SqlError::data_exception(
                            "22023",
                            format!("could not open collator for locale \"{locale}\""),
                            None,
                        ));
                    }
                    ('i', canonical, deterministic)
                }
            }
        };
        let oid = allocate_user_type_oids(self.db_mut()?, 1)?[0];
        self.store_collation(&CollationSchema {
            name,
            schema_name,
            oid,
            owner: current_user_from_gucs(&self.session_gucs),
            provider,
            locale,
            deterministic,
        })?;
        Ok(SqlResult::command("CREATE COLLATION"))
    }

    /// Columns (of any table) whose stored collation is `name`.
    fn collation_dependents(&self, name: &str) -> Result<Vec<String>> {
        let mut dependents = Vec::new();
        for schema in list_schemas_raw(self.db_ref())? {
            for column in &schema.columns {
                if column.collation.as_deref() == Some(name) {
                    dependents.push(format!("column {} of table {}", column.name, schema.name));
                }
            }
        }
        Ok(dependents)
    }

    pub(crate) fn execute_drop_collation(
        &mut self,
        if_exists: bool,
        names: &[ObjectName],
        cascade: bool,
    ) -> Result<SqlResult> {
        for name in names {
            let (schema_name, bare) = collation_identity(name)?;
            let Some(collation) = load_collation(self.db_ref(), &schema_name, &bare)? else {
                if if_exists {
                    crate::statement_context::notice_skipping_missing("collation", &bare);
                    continue;
                }
                return Err(undefined_collation(&bare));
            };
            self.require_object_ownership(
                &collation.owner,
                &format!("collation {bare}"),
                "drop it",
            )?;
            let dependents = self.collation_dependents(&bare)?;
            if let Some(first) = dependents.first() {
                if cascade {
                    return Err(SqlError::Unsupported(
                        "DROP COLLATION ... CASCADE of collations used by columns is not supported"
                            .into(),
                    ));
                }
                return Err(SqlError::data_exception(
                    "2BP01",
                    format!("cannot drop collation {bare} because other objects depend on it (detail: {first} depends on collation {bare})"),
                    None,
                ));
            }
            self.remove_collation(&schema_name, &bare)?;
        }
        Ok(SqlResult::command("DROP COLLATION"))
    }

    pub(crate) fn execute_alter_collation(&mut self, alter: &AlterCollation) -> Result<SqlResult> {
        let (schema_name, name) = collation_identity(&alter.name)?;
        let collation = load_collation(self.db_ref(), &schema_name, &name)?
            .ok_or_else(|| undefined_collation(&name))?;
        self.require_object_ownership(&collation.owner, &format!("collation {name}"), "alter it")?;
        match &alter.operation {
            AlterCollationOperation::RefreshVersion => {}
            AlterCollationOperation::RenameTo { new_name } => {
                let new_name = ident_value(new_name);
                if self.collation_name_taken(&schema_name, &new_name)? {
                    return Err(SqlError::data_exception(
                        "42710",
                        format!(
                            "collation \"{new_name}\" for encoding \"UTF8\" already exists in schema \"{schema_name}\""
                        ),
                        None,
                    ));
                }
                if !self.collation_dependents(&name)?.is_empty() {
                    return Err(SqlError::Unsupported(
                        "renaming a collation used by columns is not supported".into(),
                    ));
                }
                self.remove_collation(&schema_name, &name)?;
                self.store_collation(&CollationSchema {
                    name: new_name,
                    ..collation
                })?;
            }
            AlterCollationOperation::OwnerTo(owner) => {
                let owner = owner.to_string();
                self.store_collation(&CollationSchema {
                    owner: owner.trim_matches('"').to_string(),
                    ..collation
                })?;
            }
            AlterCollationOperation::SetSchema { .. } => {
                return Err(SqlError::Unsupported(
                    "ALTER COLLATION ... SET SCHEMA is not supported".into(),
                ))
            }
        }
        Ok(SqlResult::command("ALTER COLLATION"))
    }
}
