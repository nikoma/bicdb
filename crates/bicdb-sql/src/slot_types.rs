//! Declared types of the relations a query's FROM clause materialized.
//!
//! Slot rows name their columns `qualifier.column` but carry no types: a
//! date and a timestamp both travel as text. Every comparison needs the
//! operands' SQL types to apply PostgreSQL's cross-type rules (a date
//! equals the timestamp at its midnight; `char(n)` ignores trailing blanks;
//! NUMERIC compares by value), so each relation the executor materializes
//! (base table, view, CTE reference, derived table) is recorded here under
//! its qualifiers, in a frame scoped to the query being executed.
//!
//! Lookups search the innermost frame first, as SQL name resolution does:
//! an inner subquery's alias shadows an outer one. A qualifier registered
//! twice in one frame with different column types answers "unknown" rather
//! than guessing, so the caller falls back to its untyped-safe path.

use crate::{CteResult, TableSchema};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

#[derive(Clone)]
enum RelationTypes {
    Schema(Arc<TableSchema>),
    Columns(Rc<[(String, Option<String>)]>),
}

impl RelationTypes {
    /// `None` = the relation has no such column; `Some(None)` = it has the
    /// column but its type is unknown.
    fn column(&self, column: &str) -> Option<Option<String>> {
        match self {
            Self::Schema(schema) => schema
                .column(column)
                .map(|column| {
                    // A column with an explicit collation compares by that
                    // collation; its declared type alone does not decide.
                    column
                        .collation
                        .as_deref()
                        .is_none_or(|collation| collation == "default")
                        .then(|| column.pg_type.clone())
                })
                .or_else(|| {
                    crate::postgres_system_column_pg_type(column).map(|kind| Some(kind.to_string()))
                }),
            Self::Columns(columns) => columns
                .iter()
                .find(|(name, _)| name == column)
                .map(|(_, pg_type)| pg_type.clone()),
        }
    }
}

#[derive(Default)]
struct Frame {
    relations: Vec<(String, RelationTypes)>,
}

thread_local! {
    static FRAMES: RefCell<Vec<Frame>> = const { RefCell::new(Vec::new()) };
}

/// Keeps a frame open for the duration of one query execution.
pub(crate) struct SlotTypeFrame(std::marker::PhantomData<Rc<()>>);

impl Drop for SlotTypeFrame {
    fn drop(&mut self) {
        FRAMES.with(|frames| {
            frames.borrow_mut().pop();
        });
    }
}

pub(crate) fn push_frame() -> SlotTypeFrame {
    FRAMES.with(|frames| frames.borrow_mut().push(Frame::default()));
    SlotTypeFrame(std::marker::PhantomData)
}

fn register(qualifier: &str, types: RelationTypes) {
    FRAMES.with(|frames| {
        if let Some(frame) = frames.borrow_mut().last_mut() {
            frame
                .relations
                .push((qualifier.to_ascii_lowercase(), types));
        }
    });
}

/// A base table (or partition) scanned under `qualifier`.
pub(crate) fn register_table(qualifier: &str, schema: &Arc<TableSchema>) {
    register(qualifier, RelationTypes::Schema(Arc::clone(schema)));
}

/// A relation materialized from a query result (derived table, view, CTE):
/// `columns` are its visible column names, `types` the result's column types
/// aligned with them (shorter or empty = unknown).
pub(crate) fn register_columns(qualifier: &str, columns: &[String], types: &[Option<String>]) {
    let columns = columns
        .iter()
        .enumerate()
        .map(|(index, name)| (name.clone(), types.get(index).cloned().flatten()))
        .collect::<Rc<[_]>>();
    register(qualifier, RelationTypes::Columns(columns));
}

pub(crate) fn register_cte(qualifier: &str, columns: &[String], cte: &CteResult) {
    register_columns(qualifier, columns, &cte.column_types);
}

/// A schema-qualified table reference cannot bind to an alias of a different
/// physical relation. The row layout alone loses that distinction: scanning
/// `other AS source` produces `source.payload` slots, which must not satisfy
/// `public.source.payload` from an outer query. Consult only this query's frame;
/// an outer row retains its own scope and is tried separately by the evaluator.
pub(crate) fn current_schema_qualifier_matches(parts: &[String]) -> Option<bool> {
    let [_, qualifier, _] = parts else {
        return None;
    };
    let relation = crate::relation_name_from_parts(&parts[..2]).ok()?;
    FRAMES.with(|frames| {
        let frames = frames.borrow();
        let frame = frames.last()?;
        let mut matches = None;
        for (alias, types) in &frame.relations {
            if !alias.eq_ignore_ascii_case(qualifier) {
                continue;
            }
            let same = matches!(types, RelationTypes::Schema(schema) if schema.name == relation);
            matches = Some(matches.unwrap_or(true) && same);
        }
        matches
    })
}

/// The declared type of `qualifier.column` as registered by the innermost
/// frame that knows `qualifier`: `None` = no frame registered the qualifier
/// (the caller may resolve it another way), `Some(None)` = registered but
/// unknown or ambiguous, `Some(Some(type))` = the column's type.
pub(crate) fn lookup(qualifier: &str, column: &str) -> Option<Option<String>> {
    FRAMES.with(|frames| {
        let frames = frames.borrow();
        for frame in frames.iter().rev() {
            let mut found: Option<Option<String>> = None;
            for (name, types) in &frame.relations {
                if !name.eq_ignore_ascii_case(qualifier) {
                    continue;
                }
                let pg_type = types.column(column).flatten();
                found = Some(match found {
                    None => pg_type,
                    Some(previous) if previous == pg_type => previous,
                    Some(_) => None,
                });
            }
            if found.is_some() {
                return found;
            }
        }
        None
    })
}

/// `lookup` for a slot column key (`qualifier.column`).
pub(crate) fn lookup_key(key: &str) -> Option<Option<String>> {
    // A quoted column name can itself contain dots. Try registered relation
    // qualifiers before interpreting a dot inside the column as a boundary.
    for (index, _) in key.match_indices('.').rev() {
        if let Some(found) = lookup(&key[..index], &key[index + 1..]) {
            return Some(found);
        }
    }
    None
}
