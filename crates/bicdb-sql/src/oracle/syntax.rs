//! Scoped Oracle grammar, kept out of native PostgreSQL routine parsing.
use super::*;

thread_local! { static ORACLE_GRAMMAR: std::cell::Cell<bool> = const {std::cell::Cell::new(false)}; }
pub(crate) struct Scope(bool);
impl Scope {
    pub(crate) fn new(enabled: bool) -> Self {
        Self(ORACLE_GRAMMAR.with(|v| v.replace(enabled)))
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        ORACLE_GRAMMAR.with(|v| v.set(self.0));
    }
}
pub(crate) fn enabled() -> bool {
    ORACLE_GRAMMAR.with(std::cell::Cell::get)
}

pub(crate) fn fallback(sql: &str) -> Result<Option<Vec<Statement>>> {
    if !enabled() {
        return Ok(None);
    }
    let source = tokens(sql)?;
    if !source
        .windows(3)
        .any(|w| w == [Token::LParen, Token::Plus, Token::RParen])
    {
        return Ok(None);
    }
    Parser::parse_sql(&sqlparser::dialect::OracleDialect {}, sql)
        .map(Some)
        .map_err(|e| SqlError::InvalidSql(e.to_string()))
}

pub(super) fn rewrite<T: sqlparser::ast::VisitMut>(
    db: &BicDb,
    node: &mut T,
    namespace: Option<&str>,
) -> Result<()> {
    super::outer_joins::lower(db, node, namespace)?;
    super::hierarchy::bind_pseudocolumns(node)?;
    Ok(())
}
