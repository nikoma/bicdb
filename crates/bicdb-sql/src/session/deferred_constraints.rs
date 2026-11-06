//! Named foreign-key timing, transaction-local and validated before commit.
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
        _ => unreachable!("only foreign keys carry deferred identities"),
    }
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
                                    let ConstraintSchema::ForeignKey { deferrable, .. } =
                                        constraint
                                    else {
                                        return Err(SqlError::Unsupported("SET CONSTRAINTS timing is supported only for foreign keys".into()));
                                    };
                                    if !*deferrable {
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
                            if let ConstraintSchema::ForeignKey {
                                deferrable: true, ..
                            } = constraint
                            {
                                selected.push(constraint_key(constraint));
                            }
                        }
                    }
                }
                if let Some(tx) = self.tx.as_ref() {
                    if !deferred {
                        let selected: BTreeSet<_> = selected.iter().cloned().collect();
                        for hook in tx.deferred_hooks_snapshot() {
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
