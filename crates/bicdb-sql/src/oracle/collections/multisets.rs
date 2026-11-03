//! Named nested-table set operations and predicates, isolated from native SQL.
use super::*;
mod grammar;
mod lowering;
mod values;
pub(crate) use lowering::lower_expression;
pub(crate) use values::{evaluate, is_empty};

thread_local! { static ENABLED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
pub(crate) struct Scope(bool);
impl Scope {
    pub(crate) fn new(enabled: bool) -> Self {
        Self(ENABLED.with(|flag| flag.replace(enabled)))
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        ENABLED.with(|flag| flag.set(self.0));
    }
}
pub(crate) fn enabled() -> bool {
    ENABLED.with(std::cell::Cell::get)
}
pub(crate) fn fallback(sql: &str) -> Result<Option<Vec<Statement>>> {
    if !enabled() {
        return Ok(None);
    }
    let source = tokens(sql)?;
    if !source.windows(2).any(|pair| {
        word(&pair[0], "MULTISET")
            && ["INTERSECT", "UNION", "EXCEPT"]
                .iter()
                .any(|name| word(&pair[1], name))
    }) && !source
        .windows(2)
        .any(|pair| word(&pair[0], "IS") && word(&pair[1], "EMPTY"))
        && !source.windows(3).any(|group| {
            word(&group[0], "IS") && word(&group[1], "NOT") && word(&group[2], "EMPTY")
        })
    {
        return Ok(None);
    }
    grammar::parse(sql)
        .map(Some)
        .map_err(|error| SqlError::InvalidSql(error.to_string()))
}

pub(crate) fn lower<T: sqlparser::ast::VisitMut>(
    db: &BicDb,
    node: &mut T,
    namespace: Option<&str>,
) -> Result<()> {
    let empty = BTreeMap::new();
    match visit_expressions_mut(node, |expr| {
        match lower_expression(db, expr, &empty, namespace, false) {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(error),
        }
    }) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(error) => Err(error),
    }
}

#[cfg(test)]
mod tests;
