//! Constraint grammar positions must not turn ordinary identifiers into keywords.
use super::*;

fn identifier_end(source: &[Token], mut at: usize) -> Option<usize> {
    if !matches!(source.get(at), Some(Token::Word(_))) {
        return None;
    }
    at += 1;
    while source.get(at) == Some(&Token::Period)
        && matches!(source.get(at + 1), Some(Token::Word(_)))
    {
        at += 2;
    }
    Some(at)
}

pub(super) fn syntax_positions(source: &[Token]) -> BTreeSet<usize> {
    let mut positions = BTreeSet::new();
    let end = source.len() - usize::from(source.last() == Some(&Token::SemiColon));
    if source.first().is_some_and(|t| word(t, "SET"))
        && source.get(1).is_some_and(|t| word(t, "CONSTRAINTS"))
    {
        positions.insert(1);
        if end > 2
            && ["DEFERRED", "IMMEDIATE"]
                .iter()
                .any(|value| word(&source[end - 1], value))
        {
            positions.insert(end - 1);
        }
        return positions;
    }
    let alter = source.first().is_some_and(|t| word(t, "ALTER"));
    if !(alter || source.first().is_some_and(|t| word(t, "CREATE")))
        || !source.get(1).is_some_and(|t| word(t, "TABLE"))
    {
        return positions;
    }
    if alter {
        if let Some(at) = identifier_end(source, 2) {
            if source.get(at).is_some_and(|t| word(t, "VALIDATE"))
                && source.get(at + 1).is_some_and(|t| word(t, "CONSTRAINT"))
            {
                positions.insert(at);
            }
        }
    }
    for (index, token) in source.iter().enumerate() {
        if !word(token, "REFERENCES") || index > 0 && source[index - 1] == Token::Period {
            continue;
        }
        let Some(mut at) = identifier_end(source, index + 1) else {
            continue;
        };
        if source.get(at) == Some(&Token::LParen) {
            let mut depth = 1usize;
            at += 1;
            while at < source.len() && depth > 0 {
                match source[at] {
                    Token::LParen => depth += 1,
                    Token::RParen => depth -= 1,
                    _ => {}
                }
                at += 1;
            }
            if depth != 0 {
                continue;
            }
        }
        // Only the suffix of a REFERENCES clause supplies these grammar words.
        // Constraint/table/column names and expression literals stay identifiers.
        while at < source.len() {
            if word(&source[at], "DEFERRABLE") {
                positions.insert(at);
                at += 1;
            } else if word(&source[at], "NOT")
                && source
                    .get(at + 1)
                    .is_some_and(|t| word(t, "DEFERRABLE") || word(t, "VALID"))
            {
                positions.insert(at + 1);
                at += 2;
            } else if word(&source[at], "INITIALLY")
                && source
                    .get(at + 1)
                    .is_some_and(|t| word(t, "DEFERRED") || word(t, "IMMEDIATE"))
            {
                positions.extend([at, at + 1]);
                at += 2;
            } else if word(&source[at], "ON")
                && source
                    .get(at + 1)
                    .is_some_and(|t| word(t, "DELETE") || word(t, "UPDATE"))
            {
                at += 2;
                if source
                    .get(at)
                    .is_some_and(|t| word(t, "CASCADE") || word(t, "RESTRICT"))
                {
                    positions.insert(at);
                    at += 1;
                } else if source.get(at).is_some_and(|t| word(t, "NO"))
                    && source.get(at + 1).is_some_and(|t| word(t, "ACTION"))
                {
                    positions.extend([at, at + 1]);
                    at += 2;
                } else if source.get(at).is_some_and(|t| word(t, "SET"))
                    && source
                        .get(at + 1)
                        .is_some_and(|t| word(t, "NULL") || word(t, "DEFAULT"))
                {
                    at += 2;
                } else {
                    break;
                }
            } else {
                break;
            }
        }
    }
    positions
}

pub(super) fn timing_sql(sql: &str, namespace: Option<&str>) -> Result<Option<String>> {
    let Some(crate::session::deferred_constraints::ConstraintCommand::Set { names, deferred }) =
        crate::session::deferred_constraints::parse_constraint_command(sql)?
    else {
        return Ok(None);
    };
    let selected = if let Some(names) = names {
        names
            .into_iter()
            .map(|mut parts| {
                if parts.len() > 2 {
                    return Err(SqlError::InvalidSql(
                        "constraint names require at most schema and name".into(),
                    ));
                }
                if parts.len() == 1 {
                    if let Some(schema) = namespace {
                        parts.insert(0, schema.to_owned());
                    }
                }
                Ok(parts
                    .into_iter()
                    .map(|part| Ident::with_quote('"', part).to_string())
                    .collect::<Vec<_>>()
                    .join("."))
            })
            .collect::<Result<Vec<_>>>()?
            .join(", ")
    } else {
        "ALL".into()
    };
    Ok(Some(format!(
        "SET CONSTRAINTS {selected} {}",
        if deferred { "DEFERRED" } else { "IMMEDIATE" }
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(session: &mut SqlSession<'_>, sql: &str) -> Result<SqlResult> {
        let lowered =
            lower_session_sql(session.db_ref(), sql, ORACLE_PREVIEW_PROFILE, None, "bicdb")?;
        session.execute(&lowered)
    }

    #[test]
    fn oracle_constraint_grammar_preserves_identifiers_and_deferred_checks() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        run(
            &mut session,
            "CREATE TABLE p(deferrable INTEGER PRIMARY KEY)",
        )
        .unwrap();
        run(
            &mut session,
            "CREATE TABLE c(id INTEGER PRIMARY KEY,pid INTEGER)",
        )
        .unwrap();
        run(&mut session, "ALTER TABLE c ADD CONSTRAINT deferred FOREIGN KEY(pid) REFERENCES p(deferrable) DEFERRABLE INITIALLY DEFERRED").unwrap();
        run(&mut session, "BEGIN").unwrap();
        run(&mut session, "INSERT INTO c VALUES(1,2)").unwrap();
        run(&mut session, "INSERT INTO p VALUES(2)").unwrap();
        run(&mut session, "SET CONSTRAINTS deferred IMMEDIATE").unwrap();
        run(&mut session, "COMMIT").unwrap();
        run(&mut session, "CREATE TABLE timing_names(deferred INTEGER,deferrable INTEGER,initially INTEGER,immediate INTEGER,constraints INTEGER,cascade INTEGER,valid INTEGER,validate INTEGER)").unwrap();
        run(
            &mut session,
            "INSERT INTO timing_names VALUES(1,2,3,4,5,6,7,8)",
        )
        .unwrap();
        let result=run(&mut session, "SELECT deferred,deferrable,initially,immediate,constraints,cascade,valid,validate FROM timing_names").unwrap();
        assert_eq!(
            result.rows[0]
                .iter()
                .map(SqlValue::to_cell)
                .collect::<Vec<_>>(),
            vec!["1", "2", "3", "4", "5", "6", "7", "8"]
        );
        run(
            &mut session,
            "CREATE TABLE d(id INTEGER PRIMARY KEY,pid INTEGER)",
        )
        .unwrap();
        run(&mut session, "ALTER TABLE d ADD CONSTRAINT cascading_fk FOREIGN KEY(pid) REFERENCES p(deferrable) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED NOT VALID").unwrap();
        run(
            &mut session,
            "ALTER TABLE d VALIDATE CONSTRAINT cascading_fk",
        )
        .unwrap();
        run(&mut session,"ALTER TABLE c ADD CONSTRAINT fixed_fk FOREIGN KEY(pid) REFERENCES p(deferrable) NOT DEFERRABLE").unwrap();
        run(&mut session, "BEGIN").unwrap();
        assert_eq!(
            run(&mut session, "SET CONSTRAINTS fixed_fk IMMEDIATE")
                .unwrap_err()
                .sqlstate(),
            "42809"
        );
        run(&mut session, "ROLLBACK").unwrap();
    }

    #[test]
    fn postgres_information_schema_reports_actual_foreign_key_timing() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session.execute("CREATE TABLE p(id INTEGER PRIMARY KEY); CREATE TABLE c(id INTEGER PRIMARY KEY,pid INTEGER,CONSTRAINT timing_fk FOREIGN KEY(pid) REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)").unwrap();
        let row=session.execute("SELECT is_deferrable,initially_deferred FROM information_schema.table_constraints WHERE constraint_name='timing_fk'").unwrap().rows.remove(0);
        assert_eq!(
            row,
            vec![
                SqlValue::String("YES".into()),
                SqlValue::String("YES".into())
            ]
        );
    }

    #[test]
    fn postgres_named_nondeferrable_constraint_uses_wrong_object_type_sqlstate() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session.execute("CREATE TABLE p(id INTEGER PRIMARY KEY); CREATE TABLE c(id INTEGER PRIMARY KEY,pid INTEGER,CONSTRAINT fixed_fk FOREIGN KEY(pid) REFERENCES p(id)); BEGIN").unwrap();
        assert_eq!(
            session
                .execute("SET CONSTRAINTS fixed_fk DEFERRED")
                .unwrap_err()
                .sqlstate(),
            "42809"
        );
        session
            .execute("ROLLBACK; BEGIN; SET CONSTRAINTS fixed_fk IMMEDIATE")
            .unwrap();
        assert_eq!(
            session
                .execute("INSERT INTO c VALUES(1,999)")
                .unwrap_err()
                .sqlstate(),
            "23503"
        );
        session.execute("ROLLBACK").unwrap();
    }
}
