//! Table access methods: `CREATE TABLE ... USING method`, `CREATE TABLE ...
//! USING method AS query`, `CREATE MATERIALIZED VIEW ... USING method` and
//! `ALTER TABLE ... SET ACCESS METHOD {method | DEFAULT}`.
//!
//! BicDB has one table storage, which `pg_class.relam` reports as `heap`
//! (PostgreSQL's default), so `heap` is accepted and changes nothing. Any
//! other name fails as PostgreSQL fails for it: an index access method is
//! "not of type TABLE" (42809), anything else does not exist (42704).
use super::*;
use sqlparser::tokenizer::{Token, Tokenizer};

const INDEX_ACCESS_METHODS: &[&str] = &["btree", "hash", "gist", "gin", "spgist", "brin"];

fn keyword(token: &Token, word: &str) -> bool {
    matches!(token, Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(word))
}

fn method_name(token: &Token) -> Option<String> {
    match token {
        Token::Word(word) if word.quote_style.is_some() => Some(word.value.clone()),
        Token::Word(word) => Some(word.value.to_ascii_lowercase()),
        _ => None,
    }
}

fn check_method(method: &str) -> Result<()> {
    if method == "heap" {
        return Ok(());
    }
    if INDEX_ACCESS_METHODS.contains(&method) {
        return Err(SqlError::data_exception_public(
            "42809",
            format!("access method \"{method}\" is not of type TABLE"),
            None,
        ));
    }
    Err(SqlError::undefined_object(format!(
        "access method \"{method}\" does not exist"
    )))
}

enum AccessMethodDdl {
    /// The statement without its `USING heap`.
    Create(String),
    /// `ALTER TABLE ... SET ACCESS METHOD heap | DEFAULT`: nothing to change.
    AlterNoop { table: Vec<String>, if_exists: bool },
}

fn parse(sql: &str) -> Result<Option<AccessMethodDdl>> {
    let head = sql.trim_start();
    let lower = sql.to_ascii_lowercase();
    if !(head.len() >= 6
        && (head[..6].eq_ignore_ascii_case("CREATE") || head[..5].eq_ignore_ascii_case("ALTER")))
        || !(lower.contains("using") || lower.contains("access"))
    {
        return Ok(None);
    }
    let Ok(raw) = Tokenizer::new(&sqlparser::dialect::PostgreSqlDialect {}, sql).tokenize() else {
        return Ok(None);
    };
    let sig: Vec<usize> = raw
        .iter()
        .enumerate()
        .filter(|(_, token)| !matches!(token, Token::Whitespace(_)))
        .map(|(index, _)| index)
        .collect();
    let at = |position: usize| sig.get(position).map(|&index| &raw[index]);
    if at(0).is_some_and(|t| keyword(t, "ALTER")) {
        if !at(1).is_some_and(|t| keyword(t, "TABLE")) {
            return Ok(None);
        }
        let mut position = 2;
        let mut if_exists = false;
        if at(2).is_some_and(|t| keyword(t, "IF")) && at(3).is_some_and(|t| keyword(t, "EXISTS")) {
            if_exists = true;
            position = 4;
        }
        if at(position).is_some_and(|t| keyword(t, "ONLY")) {
            position += 1;
        }
        let mut table = Vec::new();
        loop {
            let Some(part) = at(position).and_then(method_name) else {
                return Ok(None);
            };
            table.push(part);
            position += 1;
            if at(position) == Some(&Token::Period) {
                position += 1;
            } else {
                break;
            }
        }
        if !(at(position).is_some_and(|t| keyword(t, "SET"))
            && at(position + 1).is_some_and(|t| keyword(t, "ACCESS"))
            && at(position + 2).is_some_and(|t| keyword(t, "METHOD")))
        {
            return Ok(None);
        }
        let Some(method) = at(position + 3) else {
            return Ok(None);
        };
        let rest = sig.len() - (position + 4);
        if rest != 0 && !(rest == 1 && at(position + 4) == Some(&Token::SemiColon)) {
            return Ok(None);
        }
        if !keyword(method, "DEFAULT") {
            check_method(&method_name(method).unwrap_or_default())?;
        }
        return Ok(Some(AccessMethodDdl::AlterNoop { table, if_exists }));
    }
    if !at(0).is_some_and(|t| keyword(t, "CREATE")) {
        return Ok(None);
    }
    // Find TABLE or MATERIALIZED VIEW among the leading words.
    let Some(kind_at) = (1..sig.len().min(6))
        .find(|&position| at(position).is_some_and(|t| keyword(t, "TABLE") || keyword(t, "VIEW")))
    else {
        return Ok(None);
    };
    if at(kind_at).is_some_and(|t| keyword(t, "VIEW"))
        && !at(kind_at - 1).is_some_and(|t| keyword(t, "MATERIALIZED"))
    {
        return Ok(None);
    }
    // USING at depth zero before a top-level AS (the query of CTAS / a
    // materialized view, which may itself contain JOIN ... USING).
    let mut depth = 0i64;
    for position in kind_at + 1..sig.len() {
        let token = &raw[sig[position]];
        match token {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            _ if depth == 0 && keyword(token, "AS") => return Ok(None),
            _ if depth == 0 && keyword(token, "USING") => {
                let Some(method) = at(position + 1).and_then(method_name) else {
                    return Ok(None);
                };
                check_method(&method)?;
                let (from, to) = (sig[position], sig[position + 1]);
                let sql = raw
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| *index < from || *index > to)
                    .map(|(_, token)| crate::eval::rewrite_helpers::token_sql(token))
                    .collect();
                return Ok(Some(AccessMethodDdl::Create(sql)));
            }
            _ => {}
        }
    }
    Ok(None)
}

impl<'a> SqlSession<'a> {
    pub(crate) fn execute_raw_table_access_method(
        &mut self,
        sql: &str,
    ) -> Result<Option<SqlResult>> {
        if sql.trim_end().trim_end_matches(';').contains(';') {
            return Ok(None);
        }
        match parse(sql)? {
            None => Ok(None),
            Some(AccessMethodDdl::Create(stripped)) => self.execute_inner(&stripped).map(Some),
            Some(AccessMethodDdl::AlterNoop { table, if_exists }) => {
                let name = crate::eval::rewrite_helpers::relation_name_from_parts(&table)?;
                let resolved = resolve_session_relation_name_if_exists(self.db_ref(), &name);
                if load_schema(self.db_ref(), &resolved)?.is_none() {
                    if if_exists {
                        crate::statement_context::notice_skipping_missing(
                            "relation",
                            &relation_name_for_message(&name),
                        );
                        return Ok(Some(SqlResult::command("ALTER TABLE")));
                    }
                    return Err(SqlError::data_exception_public(
                        "42P01",
                        format!(
                            "relation \"{}\" does not exist",
                            relation_name_for_message(&name)
                        ),
                        None,
                    ));
                }
                self.require_table_ownership(&resolved, "ALTER TABLE")?;
                Ok(Some(SqlResult::command("ALTER TABLE")))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_heap_and_refuses_other_methods() {
        let Some(AccessMethodDdl::Create(sql)) =
            parse("CREATE TABLE t (a int) USING heap WITH (fillfactor = 50)").unwrap()
        else {
            panic!("not parsed");
        };
        assert_eq!(sql, "CREATE TABLE t (a int)  WITH (fillfactor = 50)");
        assert!(parse("CREATE TABLE t AS SELECT * FROM a JOIN b USING (x)")
            .unwrap()
            .is_none());
        assert!(matches!(
            parse("CREATE TABLE t (a int) USING btree"),
            Err(error) if error.sqlstate() == "42809"
        ));
        assert!(matches!(
            parse("CREATE TABLE t (a int) USING nosuch"),
            Err(error) if error.sqlstate() == "42704"
        ));
    }
}
