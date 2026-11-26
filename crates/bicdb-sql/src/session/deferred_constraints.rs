//! Named foreign-key and unique-constraint timing, transaction-local and
//! validated before commit.
use super::*;

#[derive(Clone, Debug)]
pub(crate) enum ConstraintCommand {
    Alter {
        table: String,
        name: String,
        if_exists: bool,
        deferrable: Option<bool>,
        initially_deferred: Option<bool>,
    },
    Set {
        names: Option<Vec<Vec<String>>>,
        deferred: bool,
    },
    /// `ALTER TABLE t ALTER CONSTRAINT c [NO] INHERIT[, ...]` (not-null
    /// constraints only), applied in order.
    Inherit {
        table: String,
        actions: Vec<(String, bool)>,
        if_exists: bool,
    },
}

struct Tokens {
    values: Vec<Token>,
    at: usize,
}
impl Tokens {
    fn keyword(&mut self, value: &str) -> bool {
        if matches!(self.values.get(self.at), Some(Token::Word(word)) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(value))
        {
            self.at += 1;
            true
        } else {
            false
        }
    }
    fn require(&mut self, value: &str) -> Result<()> {
        if self.keyword(value) {
            Ok(())
        } else {
            Err(SqlError::InvalidSql(format!(
                "expected {value} in constraint command"
            )))
        }
    }
    fn identifier(&mut self) -> Result<Vec<String>> {
        let mut parts = Vec::new();
        loop {
            let Some(Token::Word(word)) = self.values.get(self.at) else {
                return Err(SqlError::InvalidSql(
                    "expected constraint identifier".into(),
                ));
            };
            parts.push(if word.quote_style.is_some() {
                word.value.clone()
            } else {
                word.value.to_ascii_lowercase()
            });
            self.at += 1;
            if self.values.get(self.at) != Some(&Token::Period) {
                break;
            }
            self.at += 1;
        }
        Ok(parts)
    }
    fn finish(&self) -> Result<()> {
        if self.at == self.values.len() {
            Ok(())
        } else {
            Err(SqlError::InvalidSql(
                "unexpected trailing tokens in constraint command".into(),
            ))
        }
    }
}

pub(crate) fn parse_constraint_command(sql: &str) -> Result<Option<ConstraintCommand>> {
    if !leading_sql_keyword(sql).is_some_and(|keyword| {
        keyword.eq_ignore_ascii_case("ALTER") || keyword.eq_ignore_ascii_case("SET")
    }) {
        return Ok(None);
    }
    let mut values = Tokenizer::new(&PostgreSqlDialect {}, sql)
        .tokenize()
        .map_err(|error| SqlError::InvalidSql(error.to_string()))?
        .into_iter()
        .filter(|token| !matches!(token, Token::Whitespace(_)))
        .collect::<Vec<_>>();
    if values.last() == Some(&Token::SemiColon) {
        values.pop();
    }
    if values.iter().any(|token| matches!(token, Token::SemiColon)) {
        return Ok(None); // The session's ordinary splitter owns statement lists.
    }
    let mut tokens = Tokens { values, at: 0 };
    if tokens.keyword("ALTER") && tokens.keyword("TABLE") {
        let if_exists = if tokens.keyword("IF") {
            tokens.require("EXISTS")?;
            true
        } else {
            false
        };
        tokens.keyword("ONLY");
        let table = tokens.identifier()?;
        if !tokens.keyword("ALTER") || !tokens.keyword("CONSTRAINT") {
            return Ok(None);
        }
        let name = tokens.identifier()?;
        if name.len() != 1 {
            return Err(SqlError::InvalidSql(
                "ALTER CONSTRAINT requires an unqualified constraint name".into(),
            ));
        }
        // `[NO] INHERIT`, possibly several `ALTER CONSTRAINT c [NO] INHERIT`
        // actions separated by commas.
        fn inherit_action(tokens: &mut Tokens) -> Option<bool> {
            let word = |token: Option<&Token>, expected: &str| {
                matches!(token, Some(Token::Word(word))
                    if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(expected))
            };
            if word(tokens.values.get(tokens.at), "INHERIT") {
                tokens.at += 1;
                Some(true)
            } else if word(tokens.values.get(tokens.at), "NO")
                && word(tokens.values.get(tokens.at + 1), "INHERIT")
            {
                tokens.at += 2;
                Some(false)
            } else {
                None
            }
        }
        if let Some(inherit) = inherit_action(&mut tokens) {
            let mut actions = vec![(name[0].clone(), inherit)];
            while tokens.values.get(tokens.at) == Some(&Token::Comma) {
                tokens.at += 1;
                if !tokens.keyword("ALTER") || !tokens.keyword("CONSTRAINT") {
                    return Ok(None);
                }
                let name = tokens.identifier()?;
                let [name] = name.as_slice() else {
                    return Ok(None);
                };
                let Some(inherit) = inherit_action(&mut tokens) else {
                    return Ok(None);
                };
                actions.push((name.clone(), inherit));
            }
            tokens.finish()?;
            return Ok(Some(ConstraintCommand::Inherit {
                table: relation_name_from_parts(&table)?,
                actions,
                if_exists,
            }));
        }
        let deferrable = if tokens.keyword("NOT") {
            tokens.require("DEFERRABLE")?;
            Some(false)
        } else if tokens.keyword("DEFERRABLE") {
            Some(true)
        } else {
            None
        };
        let initially_deferred = if tokens.keyword("INITIALLY") {
            if tokens.keyword("DEFERRED") {
                Some(true)
            } else {
                tokens.require("IMMEDIATE")?;
                Some(false)
            }
        } else {
            None
        };
        if deferrable.is_none() && initially_deferred.is_none() {
            return Err(SqlError::InvalidSql(
                "ALTER CONSTRAINT requires constraint timing".into(),
            ));
        }
        tokens.finish()?;
        return Ok(Some(ConstraintCommand::Alter {
            table: relation_name_from_parts(&table)?,
            name: name[0].clone(),
            if_exists,
            deferrable,
            initially_deferred,
        }));
    }
    tokens.at = 0;
    if !tokens.keyword("SET") || !tokens.keyword("CONSTRAINTS") {
        return Ok(None);
    }
    let names = if tokens.keyword("ALL") {
        None
    } else {
        let mut names = Vec::new();
        loop {
            let name = tokens.identifier()?;
            if name.len() > 2 {
                return Err(SqlError::InvalidSql(
                    "constraint names may be schema qualified".into(),
                ));
            }
            names.push(name);
            if tokens.values.get(tokens.at) != Some(&Token::Comma) {
                break;
            }
            tokens.at += 1;
        }
        Some(names)
    };
    let deferred = if tokens.keyword("DEFERRED") {
        true
    } else {
        tokens.require("IMMEDIATE")?;
        false
    };
    tokens.finish()?;
    Ok(Some(ConstraintCommand::Set { names, deferred }))
}

pub(crate) fn constraint_key(constraint: &ConstraintSchema) -> String {
    match constraint {
        ConstraintSchema::ForeignKey { constraint_id, .. } => constraint_id.clone(),
        _ => match constraint.unique_deferral() {
            Some(deferral) => deferral.constraint_id.clone(),
            None => unreachable!(
                "only foreign keys and deferrable unique constraints carry deferred identities"
            ),
        },
    }
}

/// Whether the constraint is DEFERRABLE, and so may change timing.
fn constraint_is_deferrable(constraint: &ConstraintSchema) -> bool {
    match constraint {
        ConstraintSchema::ForeignKey { deferrable, .. } => *deferrable,
        _ => constraint.unique_deferral().is_some(),
    }
}

/// Whether the constraint class has transaction-local timing at all.
fn constraint_has_timing(constraint: &ConstraintSchema) -> bool {
    matches!(
        constraint,
        ConstraintSchema::ForeignKey { .. }
            | ConstraintSchema::Unique { .. }
            | ConstraintSchema::UniqueWithNullPolicy { .. }
    )
}

pub(crate) fn unique_is_deferred(tx: &Transaction, deferral: &UniqueDeferral) -> bool {
    tx.constraint_deferred(&deferral.constraint_id)
        .unwrap_or(deferral.initially_deferred)
}

pub(crate) fn queue_unique_check(
    tx: &Transaction,
    table: &str,
    deferral: &UniqueDeferral,
    record_id: &str,
) {
    tx.push_deferred_hook(serde_json::json!({"unique_check": {"table": table, "constraint_id": deferral.constraint_id, "record_id": record_id}}));
}

#[derive(Deserialize)]
struct UniqueCheck {
    table: String,
    constraint_id: String,
    record_id: String,
}

fn parse_unique_check(hook: &JsonValue) -> Result<Option<UniqueCheck>> {
    let Some(check) = hook.get("unique_check") else {
        return Ok(None);
    };
    serde_json::from_value(check.clone())
        .map(Some)
        .map_err(|error| SqlError::InvalidSql(format!("malformed deferred unique check: {error}")))
}

/// Checks the final state of every row queued by deferred unique constraints
/// (all of them, or only those in `only`), one batch per constraint.
pub(crate) fn validate_unique_hooks(
    db: &BicDb,
    tx: Option<&Transaction>,
    hooks: &[JsonValue],
    only: Option<&BTreeSet<String>>,
) -> Result<()> {
    let mut pending = BTreeMap::<String, (String, BTreeSet<String>)>::new();
    for hook in hooks {
        let Some(check) = parse_unique_check(hook)? else {
            continue;
        };
        if only.is_some_and(|only| !only.contains(&check.constraint_id)) {
            continue;
        }
        pending
            .entry(check.constraint_id)
            .or_insert_with(|| (check.table, BTreeSet::new()))
            .1
            .insert(check.record_id);
    }
    for (constraint_id, (table, record_ids)) in pending {
        let matches = |schema: &TableSchema| {
            schema.constraints.iter().any(|constraint| {
                constraint
                    .unique_deferral()
                    .is_some_and(|deferral| deferral.constraint_id == constraint_id)
            })
        };
        let mut schema = load_schema(db, &table)?.filter(|schema| matches(schema));
        if schema.is_none() {
            // DDL may rename the relation while checks are pending; a dropped
            // constraint intentionally has no remaining match.
            schema = list_schemas(db)?.into_iter().find(|schema| matches(schema));
        }
        let Some(schema) = schema else {
            continue;
        };
        let arbiters = unique_arbiters_for_table(db, &schema.name, &schema);
        let Some(arbiter) = arbiters.iter().find(|arbiter| {
            arbiter
                .deferral
                .as_ref()
                .is_some_and(|deferral| deferral.constraint_id == constraint_id)
        }) else {
            continue;
        };
        let mut records = Vec::with_capacity(record_ids.len());
        for record_id in &record_ids {
            if let Some(record) = current_integrity_record(db, tx, &schema.name, record_id)? {
                records.push(record.as_ref().clone());
            }
        }
        validate_unique_arbiter_records(db, tx, &schema.name, &schema, arbiter, &records, true)?;
    }
    Ok(())
}

pub(crate) fn foreign_key_is_deferred(
    tx: Option<&Transaction>,
    _table: &str,
    constraint: &ConstraintSchema,
) -> bool {
    let ConstraintSchema::ForeignKey {
        deferrable,
        initially_deferred,
        ..
    } = constraint
    else {
        return false;
    };
    *deferrable
        && tx.is_some_and(|tx| {
            tx.constraint_deferred(&constraint_key(constraint))
                .unwrap_or(*initially_deferred)
        })
}

pub(crate) fn queue_foreign_key_check(
    tx: &Transaction,
    table: &str,
    constraint: &ConstraintSchema,
    record_id: &str,
) {
    tx.push_deferred_hook(serde_json::json!({"foreign_key_check": {"table": table, "constraint_id": constraint_key(constraint), "record_id": record_id}}));
}

#[derive(Deserialize)]
struct ForeignKeyCheck {
    table: String,
    constraint_id: String,
    record_id: String,
}

pub(crate) fn validate_foreign_key_hook(
    db: &BicDb,
    tx: Option<&Transaction>,
    hook: &JsonValue,
) -> Result<bool> {
    if crate::session::temporal_keys::validate_temporal_foreign_key_hook(db, tx, hook)? {
        return Ok(true);
    }
    let Some(check) = hook.get("foreign_key_check") else {
        return Ok(false);
    };
    let check: ForeignKeyCheck = serde_json::from_value(check.clone()).map_err(|error| {
        SqlError::InvalidSql(format!("malformed deferred foreign-key check: {error}"))
    })?;
    if check.constraint_id.is_empty() {
        return Err(SqlError::InvalidSql(
            "deferred foreign key has no stable identity".into(),
        ));
    }
    let mut schema = load_schema(db, &check.table)?;
    if !schema.as_ref().is_some_and(|schema| schema.constraints.iter().any(|constraint| matches!(constraint, ConstraintSchema::ForeignKey { constraint_id, .. } if constraint_id == &check.constraint_id))) {
        // DDL may rename the relation or the constraint while checks are
        // pending. A dropped constraint intentionally has no remaining match.
        schema = list_schemas(db)?.into_iter().find(|schema| schema.constraints.iter().any(|constraint| matches!(constraint, ConstraintSchema::ForeignKey { constraint_id, .. } if constraint_id == &check.constraint_id)));
    }
    let Some(schema) = schema else {
        return Ok(true);
    };
    let Some(constraint) = schema.constraints.iter().find(|constraint| matches!(constraint, ConstraintSchema::ForeignKey { constraint_id, .. } if constraint_id == &check.constraint_id)) else { return Ok(true); };
    let record = current_integrity_record(db, tx, &schema.name, &check.record_id)?;
    if let Some(record) = record {
        validate_foreign_key_constraint_immediate(
            db,
            tx,
            &schema.name,
            &schema,
            &record,
            constraint,
        )?;
    }
    Ok(true)
}

impl<'db> SqlSession<'db> {
    pub(crate) fn defer_inbound_foreign_key(
        &self,
        schema: &TableSchema,
        name: &str,
        record: &Record,
    ) -> Result<bool> {
        let Some(constraint) = schema
            .constraints
            .iter()
            .find(|constraint| constraint_name(constraint) == name)
        else {
            return Ok(false);
        };
        if foreign_key_is_deferred(self.tx.as_ref(), &schema.name, constraint) {
            queue_foreign_key_check(
                self.tx.as_ref().unwrap(),
                &schema.name,
                constraint,
                &record.id,
            );
            return Ok(true);
        }
        Ok(false)
    }

    pub(crate) fn execute_raw_constraint_command(
        &mut self,
        sql: &str,
    ) -> Result<Option<SqlResult>> {
        let Some(command) = parse_constraint_command(sql)? else {
            return Ok(None);
        };
        self.execute_constraint_command(command).map(Some)
    }

    /// PostgreSQL 18's `ALTER CONSTRAINT ... [NO] INHERIT`, which applies to
    /// not-null constraints only. NO INHERIT leaves the children's copies as
    /// their own constraints; INHERIT adds the constraint to every
    /// descendant, checking its rows.
    fn alter_not_null_inheritance(
        &mut self,
        table: &str,
        name: &str,
        if_exists: bool,
        inherit: bool,
    ) -> Result<SqlResult> {
        let table = resolve_session_relation_name_if_exists(self.db_ref(), table);
        let Some(mut schema) = load_schema(self.db_ref(), &table)? else {
            return if if_exists {
                Ok(SqlResult::command("ALTER TABLE"))
            } else {
                Err(SqlError::InvalidCollection(table))
            };
        };
        self.require_table_ownership(&schema.name, "ALTER TABLE")?;
        let display = schema
            .name
            .rsplit('.')
            .next()
            .unwrap_or(&schema.name)
            .to_string();
        let column = schema
            .columns
            .iter()
            .filter(|column| !column.hidden && (!column.nullable || column.primary_key))
            .find(|column| {
                not_null_constraint_name(&schema.name, &column.name) == name
                    || not_null_constraint_name(&display, &column.name) == name
            })
            .map(|column| column.name.clone());
        let Some(column) = column else {
            // A child's copy of a parent's not-null constraint carries the
            // parent's name and cannot change on its own.
            for parent in &schema.inherits {
                let Some(parent_schema) = load_schema(self.db_ref(), parent)? else {
                    continue;
                };
                let inherited = parent_schema.columns.iter().any(|column| {
                    !column.hidden
                        && (!column.nullable || column.primary_key)
                        && not_null_constraint_name(&parent_schema.name, &column.name) == name
                });
                if inherited {
                    return Err(SqlError::data_exception_public(
                        "42P16",
                        format!(
                            "cannot alter inherited constraint \"{name}\" on relation \"{display}\""
                        ),
                        None,
                    ));
                }
            }
            let exists = crate::ddl_alter::table_constraint_name_exists(&schema, name);
            return Err(if exists {
                SqlError::data_exception_public(
                    "42809",
                    format!("constraint \"{name}\" of relation \"{display}\" is not a not-null constraint"),
                    None,
                )
            } else {
                SqlError::undefined_object(format!(
                    "constraint \"{name}\" of relation \"{display}\" does not exist"
                ))
            });
        };
        let key = not_null_constraint_name(&schema.name, &column);
        let listed = schema.no_inherit_constraints.contains(&key);
        if inherit {
            if listed {
                schema.no_inherit_constraints.retain(|entry| *entry != key);
                self.save_session_schema(&schema)?;
            }
            for child in crate::inheritance::inheritance_descendants(self.db_ref(), &schema)? {
                let nullable = child
                    .columns
                    .iter()
                    .any(|candidate| candidate.name == column && candidate.nullable);
                if nullable {
                    self.execute(&format!(
                        "ALTER TABLE ONLY {} ALTER COLUMN {} SET NOT NULL",
                        child
                            .name
                            .split('.')
                            .map(pg_quote_ident)
                            .collect::<Vec<_>>()
                            .join("."),
                        pg_quote_ident(&column)
                    ))?;
                }
            }
        } else if !listed {
            schema.no_inherit_constraints.push(key);
            self.save_session_schema(&schema)?;
        }
        Ok(SqlResult::command("ALTER TABLE"))
    }

    fn execute_constraint_command(&mut self, command: ConstraintCommand) -> Result<SqlResult> {
        match command {
            ConstraintCommand::Alter {
                table,
                name,
                if_exists,
                deferrable,
                initially_deferred,
            } => {
                let table = resolve_session_relation_name(self.db_ref(), &table)?;
                let Some(mut schema) = load_schema(self.db_ref(), &table)? else {
                    return if if_exists {
                        Ok(SqlResult::command("ALTER TABLE"))
                    } else {
                        Err(SqlError::InvalidCollection(table))
                    };
                };
                self.require_table_ownership(&table, "ALTER TABLE")?;
                if let Some(foreign_key) = schema
                    .temporal_foreign_keys
                    .iter_mut()
                    .find(|foreign_key| foreign_key.name == name)
                {
                    if foreign_key.constraint_id.is_empty() {
                        foreign_key.constraint_id = uuid::Uuid::new_v4().to_string();
                    }
                    if let Some(value) = deferrable {
                        foreign_key.deferrable = value;
                        if !value {
                            foreign_key.initially_deferred = false;
                        }
                    }
                    if let Some(value) = initially_deferred {
                        foreign_key.initially_deferred = value;
                    }
                    if foreign_key.initially_deferred && !foreign_key.deferrable {
                        return Err(SqlError::InvalidSql(
                            "INITIALLY DEFERRED requires DEFERRABLE".into(),
                        ));
                    }
                    self.save_session_schema(&schema)?;
                    return Ok(SqlResult::command("ALTER TABLE"));
                }
                let constraint = schema
                    .constraints
                    .iter_mut()
                    .find(|constraint| constraint_name(constraint) == name)
                    .ok_or_else(|| SqlError::RaisedException {
                        sqlstate: "42704".into(),
                        message: format!("constraint {name} does not exist"),
                        detail: None,
                    })?;
                let ConstraintSchema::ForeignKey {
                    deferrable: flag,
                    initially_deferred: initial,
                    constraint_id,
                    ..
                } = constraint
                else {
                    return Err(SqlError::Unsupported(
                        "ALTER CONSTRAINT timing is supported only for foreign keys".into(),
                    ));
                };
                if constraint_id.is_empty() {
                    *constraint_id = uuid::Uuid::new_v4().to_string();
                }
                if let Some(value) = deferrable {
                    *flag = value;
                    if !value {
                        *initial = false;
                    }
                }
                if let Some(value) = initially_deferred {
                    *initial = value;
                }
                if *initial && !*flag {
                    return Err(SqlError::InvalidSql(
                        "INITIALLY DEFERRED requires DEFERRABLE".into(),
                    ));
                }
                self.save_session_schema(&schema)?;
                Ok(SqlResult::command("ALTER TABLE"))
            }
            ConstraintCommand::Inherit {
                table,
                actions,
                if_exists,
            } => {
                let mut result = SqlResult::command("ALTER TABLE");
                for (name, inherit) in actions {
                    result = self.alter_not_null_inheritance(&table, &name, if_exists, inherit)?;
                }
                Ok(result)
            }
            ConstraintCommand::Set { names, deferred } => {
                let schemas = list_schemas(self.db_ref())?;
                let mut selected = Vec::new();
                if let Some(names) = names {
                    for parts in names {
                        let name = parts.last().unwrap();
                        let namespaces = if parts.len() == 2 {
                            vec![parts[0].clone()]
                        } else {
                            self.session_gucs
                                .get("search_path")
                                .map(|value| {
                                    value
                                        .split(',')
                                        .map(|part| part.trim().trim_matches('"').to_owned())
                                        .filter(|part| !part.is_empty())
                                        .collect()
                                })
                                .unwrap_or_else(|| vec!["public".into()])
                        };
                        let mut matched = false;
                        for namespace in namespaces {
                            let namespace = if namespace == "$user" {
                                current_user_from_gucs(&self.session_gucs)
                            } else {
                                namespace
                            };
                            for schema in schemas
                                .iter()
                                .filter(|schema| schema.schema_name == namespace)
                            {
                                for constraint in schema
                                    .constraints
                                    .iter()
                                    .filter(|constraint| constraint_name(constraint) == name)
                                {
                                    matched = true;
                                    if !constraint_has_timing(constraint) {
                                        return Err(SqlError::Unsupported("SET CONSTRAINTS timing is supported only for foreign keys and unique constraints".into()));
                                    }
                                    if !constraint_is_deferrable(constraint) {
                                        // PostgreSQL permits an explicit no-op IMMEDIATE;
                                        // Oracle rejects either mode for a non-deferrable FK.
                                        if !deferred
                                            && !crate::oracle::is_preview_database(self.db_ref())?
                                        {
                                            continue;
                                        }
                                        return Err(SqlError::RaisedException {
                                            sqlstate: "42809".into(),
                                            message: format!("constraint {name} is not deferrable"),
                                            detail: None,
                                        });
                                    }
                                    selected.push(constraint_key(constraint));
                                }
                                for foreign_key in schema
                                    .temporal_foreign_keys
                                    .iter()
                                    .filter(|foreign_key| foreign_key.name == *name)
                                {
                                    matched = true;
                                    if !foreign_key.deferrable
                                        || foreign_key.constraint_id.is_empty()
                                    {
                                        if !deferred {
                                            continue;
                                        }
                                        return Err(SqlError::RaisedException {
                                            sqlstate: "42809".into(),
                                            message: format!("constraint {name} is not deferrable"),
                                            detail: None,
                                        });
                                    }
                                    selected.push(foreign_key.constraint_id.clone());
                                }
                            }
                            if matched {
                                break;
                            }
                        }
                        if !matched {
                            return Err(SqlError::RaisedException {
                                sqlstate: "42704".into(),
                                message: format!("constraint {name} does not exist"),
                                detail: None,
                            });
                        }
                    }
                } else {
                    // Existing deferred triggers have separate firing semantics;
                    // do not silently ignore ALL when it would change them too.
                    if crate::catalog_memo::triggers_shared(self.db_ref())?
                        .iter()
                        .any(|trigger| trigger.is_constraint)
                    {
                        return Err(SqlError::Unsupported("SET CONSTRAINTS ALL with deferred constraint triggers is not supported".into()));
                    }
                    for schema in &schemas {
                        for constraint in &schema.constraints {
                            if constraint_has_timing(constraint)
                                && constraint_is_deferrable(constraint)
                            {
                                selected.push(constraint_key(constraint));
                            }
                        }
                        for foreign_key in &schema.temporal_foreign_keys {
                            if foreign_key.deferrable && !foreign_key.constraint_id.is_empty() {
                                selected.push(foreign_key.constraint_id.clone());
                            }
                        }
                    }
                }
                if let Some(tx) = self.tx.as_ref() {
                    if !deferred {
                        let selected: BTreeSet<_> = selected.iter().cloned().collect();
                        let hooks = tx.deferred_hooks_snapshot();
                        validate_unique_hooks(self.db_ref(), Some(tx), &hooks, Some(&selected))?;
                        for hook in hooks {
                            if let Some(id) =
                                crate::session::temporal_keys::temporal_foreign_key_hook_id(&hook)
                            {
                                if selected.contains(&id) {
                                    validate_foreign_key_hook(self.db_ref(), Some(tx), &hook)?;
                                }
                                continue;
                            }
                            if let Some(check) = hook.get("foreign_key_check") {
                                let check: ForeignKeyCheck = serde_json::from_value(check.clone())
                                    .map_err(|error| SqlError::InvalidSql(error.to_string()))?;
                                if selected.contains(&check.constraint_id) {
                                    validate_foreign_key_hook(self.db_ref(), Some(tx), &hook)?;
                                }
                            }
                        }
                    }
                    for key in selected {
                        tx.set_constraint_deferred(key, deferred);
                    }
                }
                Ok(SqlResult::command("SET CONSTRAINTS"))
            }
        }
    }
}
