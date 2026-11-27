//! `DROP OWNED BY role [, ...] [CASCADE | RESTRICT]` and
//! `REASSIGN OWNED BY role [, ...] TO new_role`.
//!
//! DROP OWNED drops every object of the current database owned by the
//! roles (views, tables, sequences, routines, types, schemas), revokes the
//! privileges granted to them, removes their default privileges and takes
//! them out of row-security policies (dropping a policy left with no role).
//! REASSIGN OWNED gives those objects to the new role. As in PostgreSQL the
//! caller needs the privileges of every role named (and, for REASSIGN, of
//! the new owner); a superuser has them. Each statement is atomic.
use super::*;
use sqlparser::tokenizer::{Token, Tokenizer};

#[derive(Debug, PartialEq)]
enum OwnedDdl {
    Drop { roles: Vec<String>, cascade: bool },
    Reassign { roles: Vec<String>, to: String },
}

fn keyword(token: Option<&&Token>, word: &str) -> bool {
    matches!(token, Some(Token::Word(w)) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(word))
}

/// A role specification: a name, or CURRENT_ROLE/CURRENT_USER/SESSION_USER.
enum RoleSpec {
    Name(String),
    Current,
    Session,
}

fn role_spec(token: Option<&&Token>) -> Option<RoleSpec> {
    match token? {
        Token::Word(word) if word.quote_style == Some('"') => {
            Some(RoleSpec::Name(word.value.clone()))
        }
        Token::Word(word) if word.quote_style.is_none() => {
            Some(match word.value.to_ascii_lowercase().as_str() {
                "current_role" | "current_user" => RoleSpec::Current,
                "session_user" => RoleSpec::Session,
                other => RoleSpec::Name(other.to_string()),
            })
        }
        _ => None,
    }
}

fn parse(sql: &str) -> Option<(Vec<RoleSpec>, Option<RoleSpec>, bool, bool)> {
    let head = sql.trim_start();
    let starts =
        |word: &str| head.len() >= word.len() && head[..word.len()].eq_ignore_ascii_case(word);
    if !(starts("DROP") || starts("REASSIGN")) || !sql.to_ascii_lowercase().contains("owned") {
        return None;
    }
    let raw = Tokenizer::new(&sqlparser::dialect::PostgreSqlDialect {}, sql)
        .tokenize()
        .ok()?;
    let mut tokens: Vec<&Token> = raw
        .iter()
        .filter(|token| !matches!(token, Token::Whitespace(_)))
        .collect();
    while tokens.last() == Some(&&Token::SemiColon) {
        tokens.pop();
    }
    let reassign = keyword(tokens.first(), "REASSIGN");
    if !(reassign || keyword(tokens.first(), "DROP"))
        || !keyword(tokens.get(1), "OWNED")
        || !keyword(tokens.get(2), "BY")
    {
        return None;
    }
    let mut roles = Vec::new();
    let mut at = 3;
    loop {
        roles.push(role_spec(tokens.get(at))?);
        at += 1;
        if tokens.get(at) == Some(&&Token::Comma) {
            at += 1;
        } else {
            break;
        }
    }
    if reassign {
        if !keyword(tokens.get(at), "TO") || tokens.len() != at + 2 {
            return None;
        }
        return Some((roles, Some(role_spec(tokens.get(at + 1))?), false, true));
    }
    let cascade = match tokens.get(at) {
        None => false,
        Some(_) if keyword(tokens.get(at), "CASCADE") && tokens.len() == at + 1 => true,
        Some(_) if keyword(tokens.get(at), "RESTRICT") && tokens.len() == at + 1 => false,
        _ => return None,
    };
    Some((roles, None, cascade, false))
}

/// `"schema"."name"` for a relation's storage name.
fn relation_sql(storage: &str) -> String {
    let quote = super::not_null_constraints::quote;
    catalog_display_name(storage)
        .split('.')
        .map(quote)
        .collect::<Vec<_>>()
        .join(".")
}

impl<'a> SqlSession<'a> {
    pub(crate) fn execute_raw_owned_objects(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let Some((specs, to, cascade, reassign)) = parse(sql) else {
            return Ok(None);
        };
        let resolve = |session: &Self, spec: RoleSpec| -> Result<String> {
            let role = match spec {
                RoleSpec::Name(name) => normalize_role_name(&name),
                RoleSpec::Current => current_user_from_gucs(&session.session_gucs),
                RoleSpec::Session => session_user_from_gucs(&session.session_gucs),
            };
            if !role_exists(session.db_ref(), &role)? {
                return Err(SqlError::UndefinedRole { name: role });
            }
            Ok(role)
        };
        let mut roles = Vec::new();
        for spec in specs {
            roles.push(resolve(self, spec)?);
        }
        let ddl = match to {
            Some(spec) => OwnedDdl::Reassign {
                roles,
                to: resolve(self, spec)?,
            },
            None => OwnedDdl::Drop { roles, cascade },
        };
        let _ = reassign;
        let superuser = self.current_user_is_superuser()?;
        let (roles, verb) = match &ddl {
            OwnedDdl::Drop { roles, .. } => (roles.clone(), "drop"),
            OwnedDdl::Reassign { roles, .. } => (roles.clone(), "reassign"),
        };
        for role in &roles {
            if !superuser && !self.current_user_holds_role(role)? {
                return Err(SqlError::with_detail(
                    SqlError::BicDb(BicDbError::Authorization(format!(
                        "permission denied to {verb} objects"
                    ))),
                    format!("Only roles with privileges of role \"{role}\" may {verb} objects owned by it."),
                ));
            }
        }
        if let OwnedDdl::Reassign { to, .. } = &ddl {
            if !superuser && !self.current_user_holds_role(to)? {
                return Err(SqlError::with_detail(
                    SqlError::BicDb(BicDbError::Authorization(
                        "permission denied to reassign objects".to_string(),
                    )),
                    format!(
                        "Only roles with privileges of role \"{to}\" may reassign objects to it."
                    ),
                ));
            }
        }
        let implicit = self.tx.is_none();
        if implicit {
            self.execute_inner("BEGIN")?;
        }
        let outcome = match ddl {
            OwnedDdl::Drop { roles, cascade } => self.drop_owned(&roles, cascade),
            OwnedDdl::Reassign { roles, to } => self.reassign_owned(&roles, &to),
        };
        if implicit {
            if outcome.is_ok() {
                self.execute_inner("COMMIT")?;
            } else {
                let _ = self.execute_inner("ROLLBACK");
            }
        }
        outcome.map(Some)
    }

    fn drop_owned(&mut self, roles: &[String], cascade: bool) -> Result<SqlResult> {
        let owned = |owner: Option<&str>| {
            owner.is_some_and(|owner| roles.iter().any(|role| role.eq_ignore_ascii_case(owner)))
        };
        let suffix = if cascade { " CASCADE" } else { "" };
        let quote = super::not_null_constraints::quote;
        let mut statements = Vec::new();
        let views = list_views(self.db_ref())?
            .into_iter()
            .filter(|view| owned(view.owner.as_deref()))
            .collect::<Vec<_>>();
        for materialized in [true, false] {
            let names = views
                .iter()
                .filter(|view| view.materialized == materialized)
                .map(|view| relation_sql(&view.name))
                .collect::<Vec<_>>();
            if !names.is_empty() {
                let kind = if materialized {
                    "MATERIALIZED VIEW"
                } else {
                    "VIEW"
                };
                statements.push(format!("DROP {kind} {}{suffix}", names.join(", ")));
            }
        }
        let tables = list_schemas(self.db_ref())?
            .into_iter()
            .filter(|schema| owned(schema.owner.as_deref()) && schema.partition_of.is_none())
            .collect::<Vec<_>>();
        let table_names = tables
            .iter()
            .map(|schema| schema.name.clone())
            .collect::<Vec<_>>();
        if !tables.is_empty() {
            statements.push(format!(
                "DROP TABLE {}{suffix}",
                tables
                    .iter()
                    .map(|schema| relation_sql(&schema.name))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        let sequences = list_sequences(self.db_ref())?
            .into_iter()
            .filter(|sequence| {
                owned(Some(&sequence.owner))
                    && !sequence
                        .owned_by_table
                        .as_ref()
                        .is_some_and(|table| table_names.contains(table))
            })
            .map(|sequence| -> Result<String> {
                Ok(format!(
                    "{}.{}",
                    quote(&sequence_catalog_namespace(self.db_ref(), &sequence.name)?),
                    quote(&sequence_catalog_name(&sequence.name))
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        if !sequences.is_empty() {
            statements.push(format!("DROP SEQUENCE {}{suffix}", sequences.join(", ")));
        }
        for routine in list_routines(self.db_ref())? {
            if !owned(routine.owner.as_deref()) {
                continue;
            }
            let kind = match routine.kind {
                RoutineKind::Function => "FUNCTION",
                RoutineKind::Procedure => "PROCEDURE",
            };
            let bare = routine.name.rsplit('.').next().unwrap_or(&routine.name);
            statements.push(format!(
                "DROP {kind} {}.{}({}){suffix}",
                quote(&routine.schema),
                quote(bare),
                routine
                    .arg_types
                    .iter()
                    .map(|arg| arg.pg_type.clone())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        for user_type in list_user_types(self.db_ref())? {
            if !owned(Some(&user_type.owner)) {
                continue;
            }
            let kind = if matches!(user_type.kind, UserTypeKind::Domain { .. }) {
                "DOMAIN"
            } else {
                "TYPE"
            };
            statements.push(format!(
                "DROP {kind} {}.{}{suffix}",
                quote(&user_type.schema_name),
                quote(&user_type.name)
            ));
        }
        for namespace in list_namespaces(self.db_ref())? {
            if namespace.dropped || !owned(Some(&namespace.owner)) {
                continue;
            }
            statements.push(format!("DROP SCHEMA {}{suffix}", quote(&namespace.name)));
        }
        for statement in statements {
            self.execute_inner(&statement)?;
        }
        // Privileges granted to the roles, and their default privileges.
        let is_role = |name: &str| roles.iter().any(|role| role.eq_ignore_ascii_case(name));
        for grant in list_privileges(self.db_ref())? {
            if is_role(&grant.grantee) {
                self.delete_session_privilege(&grant)?;
            }
        }
        for grant in list_default_privileges(self.db_ref())? {
            if is_role(&grant.grantee) || is_role(&grant.grantor) {
                self.delete_session_default_privilege(&grant)?;
            }
        }
        // Row-security policies naming the roles.
        for mut schema in list_schemas(self.db_ref())? {
            let before = schema.policies.len();
            let mut changed = false;
            for policy in &mut schema.policies {
                let kept = policy
                    .roles
                    .iter()
                    .filter(|role| !is_role(role))
                    .cloned()
                    .collect::<Vec<_>>();
                if kept.len() != policy.roles.len() {
                    policy.roles = kept;
                    changed = true;
                }
            }
            schema.policies.retain(|policy| !policy.roles.is_empty());
            if changed || schema.policies.len() != before {
                self.save_session_schema(&schema)?;
            }
        }
        Ok(SqlResult::command("DROP OWNED"))
    }

    fn reassign_owned(&mut self, roles: &[String], to: &str) -> Result<SqlResult> {
        let owned = |owner: &str| roles.iter().any(|role| role.eq_ignore_ascii_case(owner));
        if list_databases(self.db_ref())?
            .iter()
            .any(|database| owned(&database.owner))
        {
            return Err(SqlError::Unsupported(
                "REASSIGN OWNED of a role that owns a database".to_string(),
            ));
        }
        for mut schema in list_schemas(self.db_ref())? {
            if schema.owner.as_deref().is_some_and(owned) {
                schema.owner = Some(to.to_string());
                self.save_session_schema(&schema)?;
            }
        }
        for mut view in list_views(self.db_ref())? {
            if view.owner.as_deref().is_some_and(owned) {
                view.owner = Some(to.to_string());
                self.save_session_view(&view)?;
            }
        }
        for mut sequence in list_sequences(self.db_ref())? {
            if owned(&sequence.owner) {
                sequence.owner = to.to_string();
                self.save_session_sequence(&sequence)?;
            }
        }
        for mut routine in list_routines(self.db_ref())? {
            if routine.owner.as_deref().is_some_and(owned) {
                routine.owner = Some(to.to_string());
                self.save_session_routine(&routine)?;
            }
        }
        for mut user_type in list_user_types(self.db_ref())? {
            if owned(&user_type.owner) {
                user_type.owner = to.to_string();
                self.save_session_user_type(&user_type)?;
            }
        }
        for namespace in list_namespaces(self.db_ref())? {
            if namespace.dropped || !owned(&namespace.owner) {
                continue;
            }
            let previous = namespace.clone();
            save_namespace(
                self.db_mut()?,
                NamespaceSchema {
                    owner: to.to_string(),
                    ..namespace
                },
            )?;
            if self.tx.is_some() {
                self.ddl_undo.push(DdlUndo::RestoreNamespace {
                    namespace: previous,
                });
            }
        }
        Ok(SqlResult::command("REASSIGN OWNED"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_owned_statements() {
        let (roles, to, cascade, reassign) = parse("DROP OWNED BY a, \"B\" CASCADE").unwrap();
        assert_eq!(roles.len(), 2);
        assert!(to.is_none() && cascade && !reassign);
        let (_, to, _, reassign) = parse("REASSIGN OWNED BY CURRENT_USER TO b").unwrap();
        assert!(to.is_some() && reassign);
        assert!(parse("DROP OWNED a").is_none());
        assert!(parse("DROP TABLE owned").is_none());
    }
}
