//! SQL front end of two-phase commit (`PREPARE TRANSACTION`, `COMMIT
//! PREPARED`, `ROLLBACK PREPARED`). The state lives in the core; see
//! docs/postgresql-two-phase-commit.md.
use crate::*;

/// One two-phase commit command and its global transaction identifier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TwoPhaseCommand {
    Prepare(String),
    CommitPrepared(String),
    RollbackPrepared(String),
}

impl TwoPhaseCommand {
    pub fn command_tag(&self) -> &'static str {
        match self {
            Self::Prepare(_) => "PREPARE TRANSACTION",
            Self::CommitPrepared(_) => "COMMIT PREPARED",
            Self::RollbackPrepared(_) => "ROLLBACK PREPARED",
        }
    }
}

fn keyword(token: Option<&Token>, value: &str) -> bool {
    matches!(token, Some(Token::Word(word))
        if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(value))
}

/// Recognize a two-phase command. `None` for every other statement; a
/// malformed identifier is PostgreSQL's syntax error.
pub fn parse_two_phase_command(sql: &str) -> Result<Option<TwoPhaseCommand>> {
    let head = sql.trim_start();
    let first = head
        .split(|c: char| !c.is_ascii_alphabetic())
        .next()
        .unwrap_or_default();
    if !["prepare", "commit", "rollback"]
        .iter()
        .any(|word| first.eq_ignore_ascii_case(word))
    {
        return Ok(None);
    }
    let Ok(tokens) = Tokenizer::new(&PostgreSqlDialect {}, sql).tokenize() else {
        return Ok(None);
    };
    let tokens = tokens
        .into_iter()
        .filter(|token| !matches!(token, Token::Whitespace(_)))
        .collect::<Vec<_>>();
    let kind = if keyword(tokens.first(), "prepare") && keyword(tokens.get(1), "transaction") {
        0
    } else if keyword(tokens.first(), "commit") && keyword(tokens.get(1), "prepared") {
        1
    } else if keyword(tokens.first(), "rollback") && keyword(tokens.get(1), "prepared") {
        2
    } else {
        return Ok(None);
    };
    let rest = &tokens[2..];
    let rest = match rest.last() {
        Some(Token::SemiColon) => &rest[..rest.len() - 1],
        _ => rest,
    };
    let gid = match rest {
        [Token::SingleQuotedString(gid)] | [Token::EscapedStringLiteral(gid)] => gid.clone(),
        [] => {
            return Err(SqlError::InvalidSql(
                "sql parser error: syntax error at end of input".into(),
            ))
        }
        [token, ..] => {
            return Err(SqlError::InvalidSql(format!(
                "sql parser error: syntax error at or near \"{token}\""
            )))
        }
    };
    Ok(Some(match kind {
        0 => TwoPhaseCommand::Prepare(gid),
        1 => TwoPhaseCommand::CommitPrepared(gid),
        _ => TwoPhaseCommand::RollbackPrepared(gid),
    }))
}

/// The role, superuser status and database PostgreSQL uses for two-phase
/// commit (`GetUserId()`: the current user, after SET ROLE).
pub fn two_phase_identity(
    db: &BicDb,
    session_gucs: &HashMap<String, String>,
) -> Result<(String, bool, String)> {
    let user = current_user_from_gucs(session_gucs);
    let superuser = pg_role_is_superuser(db, &user)?;
    Ok((user, superuser, current_database_from_gucs(session_gucs)))
}

/// PostgreSQL's refusal for a transaction whose effects cannot be prepared.
pub fn cannot_prepare(what: &str) -> SqlError {
    SqlError::data_exception(
        "0A000",
        format!("cannot PREPARE a transaction that {what}"),
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_the_three_commands_and_nothing_else() {
        assert_eq!(
            parse_two_phase_command("PREPARE TRANSACTION 'a''b';").unwrap(),
            Some(TwoPhaseCommand::Prepare("a'b".into()))
        );
        assert_eq!(
            parse_two_phase_command("commit prepared 'x'").unwrap(),
            Some(TwoPhaseCommand::CommitPrepared("x".into()))
        );
        assert_eq!(
            parse_two_phase_command("ROLLBACK PREPARED 'x'").unwrap(),
            Some(TwoPhaseCommand::RollbackPrepared("x".into()))
        );
        for other in [
            "PREPARE p AS SELECT 1",
            "COMMIT",
            "ROLLBACK TO SAVEPOINT s",
            "select 1",
        ] {
            assert_eq!(parse_two_phase_command(other).unwrap(), None, "{other}");
        }
        assert!(parse_two_phase_command("COMMIT PREPARED").is_err());
        assert!(parse_two_phase_command("COMMIT PREPARED foo").is_err());
    }
}

impl SqlSession<'_> {
    /// Embedded-session two-phase commit (the server handles these commands
    /// on its connections before they reach a session).
    pub(crate) fn execute_two_phase(&mut self, command: TwoPhaseCommand) -> Result<SqlResult> {
        self.require_local_transaction_control()?;
        let gid = match &command {
            TwoPhaseCommand::Prepare(gid) => gid.clone(),
            other => {
                if self.tx.is_some() {
                    return Err(SqlError::data_exception(
                        "25001",
                        format!(
                            "{} cannot run inside a transaction block",
                            other.command_tag()
                        ),
                        None,
                    ));
                }
                let (user, superuser, database) =
                    two_phase_identity(self.db_ref(), &self.session_gucs)?;
                let requester = bicdb_core::PreparedTransactionRequester {
                    user: &user,
                    superuser,
                    database: &database,
                };
                match other {
                    TwoPhaseCommand::CommitPrepared(gid) => {
                        self.db_ref().commit_prepared(gid, requester)?
                    }
                    TwoPhaseCommand::RollbackPrepared(gid) => {
                        self.db_ref().rollback_prepared(gid, requester)?
                    }
                    TwoPhaseCommand::Prepare(_) => unreachable!(),
                };
                return Ok(SqlResult::command(other.command_tag()));
            }
        };
        if self.tx.is_none() {
            let mut result = SqlResult::command("ROLLBACK");
            result.notices.push(SqlNotice {
                sqlstate: "25P01".to_string(),
                message: "there is no transaction in progress".to_string(),
                fields: vec![SqlErrorField::Severity("WARNING".to_string())],
            });
            return Ok(result);
        }
        let refusal = if !self.ddl_undo.is_empty() {
            Some(cannot_prepare(
                "has changed the catalog (two-phase commit of DDL is not supported)",
            ))
        } else if !self.pending_notifications.is_empty() {
            Some(cannot_prepare("has executed LISTEN, UNLISTEN, or NOTIFY"))
        } else if self.sql_cursors.has_uncommitted_holdable() {
            Some(cannot_prepare("has created a cursor WITH HOLD"))
        } else {
            None
        };
        if let Some(refusal) = refusal {
            self.rollback_current_transaction()?;
            return Err(refusal);
        }
        if self
            .tx
            .as_ref()
            .is_some_and(Transaction::has_deferred_hooks)
        {
            if let Err(error) = self.fire_deferred_row_triggers() {
                self.rollback_current_transaction()?;
                return Err(error);
            }
        }
        let identity = match two_phase_identity(self.db_ref(), &self.session_gucs) {
            Ok(identity) => identity,
            Err(error) => {
                self.rollback_current_transaction()?;
                return Err(error);
            }
        };
        let mut tx = self.tx.take().expect("checked open transaction");
        let prepared = self
            .db_ref()
            .prepare_transaction(&mut tx, &gid, &identity.0, &identity.2);
        self.pg_large_objects.clear();
        self.savepoints.clear();
        match prepared {
            Ok(_) => {
                drop(tx);
                self.commit_guc_transaction();
                Ok(SqlResult::command("PREPARE TRANSACTION"))
            }
            Err(error) => {
                let _ = tx.rollback();
                self.rollback_guc_transaction();
                Err(error.into())
            }
        }
    }
}
