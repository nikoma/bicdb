//! Synchronous statement context shared by nested row and routine evaluators.
use crate::{Result, SqlError, SqlNotice, SqlResult};
use std::cell::{Cell, RefCell};
use std::rc::Rc;

thread_local! {
    static DESCRIBING: Cell<bool> = const { Cell::new(false) };
    static STATEMENT_TRANSACTION: Cell<Option<u64>> = const { Cell::new(None) };
    static EFFECTS: RefCell<Option<Rc<Effects>>> = const { RefCell::new(None) };
}

#[derive(Default)]
struct Effects {
    independent_execution: Cell<bool>,
    independent_commits: Cell<bool>,
    latest_sequence: Cell<u64>,
    notices: RefCell<Vec<SqlNotice>>,
    notice_bytes: Cell<usize>,
}

/// Prevent independently committing routines from executing during metadata
/// discovery. The host must hold this scope only during synchronous Describe.
pub struct SqlDescriptionScope(bool, std::marker::PhantomData<Rc<()>>);
impl SqlDescriptionScope {
    pub fn enter() -> Self {
        Self(
            DESCRIBING.with(|c| c.replace(true)),
            std::marker::PhantomData,
        )
    }
}
impl Drop for SqlDescriptionScope {
    fn drop(&mut self) {
        DESCRIBING.with(|c| c.set(self.0));
    }
}
/// Whether a statement is already executing on this thread (SQL run now is
/// part of it, not a client statement).
pub(crate) fn in_statement() -> bool {
    EFFECTS.with(|current| current.borrow().is_some())
}
pub(crate) fn describing() -> bool {
    DESCRIBING.with(Cell::get)
}

pub(crate) struct StatementScope {
    _join_budget: crate::join_budget::Scope,
    effects: Rc<Effects>,
    root: bool,
}
impl StatementScope {
    pub(crate) fn enter() -> Self {
        EFFECTS.with(|current| {
            let root = current.borrow().is_none();
            if root {
                *current.borrow_mut() = Some(Rc::new(Effects::default()));
            }
            Self {
                _join_budget: crate::join_budget::Scope::enter(),
                effects: current.borrow().as_ref().unwrap().clone(),
                root,
            }
        })
    }
    pub(crate) fn independently_committed(&self) -> bool {
        self.effects.independent_commits.get()
    }
    pub(crate) fn independent_execution(&self) -> bool {
        self.effects.independent_execution.get()
    }
    pub(crate) fn latest_sequence(&self) -> u64 {
        self.effects.latest_sequence.get()
    }
    /// The notices reported so far; none for a nested scope (they belong to
    /// the enclosing statement).
    pub(crate) fn take_notices(&self) -> Vec<SqlNotice> {
        if self.root {
            self.effects.notice_bytes.set(0);
            self.effects.notices.take()
        } else {
            Vec::new()
        }
    }
    pub(crate) fn finish(&self, result: Result<SqlResult>) -> Result<SqlResult> {
        if !self.root {
            return result;
        }
        let notices = self.effects.notices.take();
        if notices.is_empty() {
            return result;
        }
        match result {
            Ok(mut result) => {
                result.notices.extend(notices);
                Ok(result)
            }
            Err(error) => Err(SqlError::WithNotices {
                error: Box::new(error),
                notices,
            }),
        }
    }
}

pub(crate) fn note_notice(notice: SqlNotice) -> Result<()> {
    EFFECTS.with(|current| {
        let current = current.borrow();
        let effects = current
            .as_ref()
            .ok_or_else(|| SqlError::Unsupported("NOTICE requires a statement context".into()))?;
        let bytes = effects
            .notice_bytes
            .get()
            .saturating_add(notice.memory_estimate());
        if effects.notices.borrow().len() >= 1024 || bytes > 1024 * 1024 {
            return Err(SqlError::resource_limit(
                "54000",
                "statement NOTICE output exceeds the bounded diagnostic buffer",
            ));
        }
        effects.notice_bytes.set(bytes);
        effects.notices.borrow_mut().push(notice);
        Ok(())
    })
}
impl Drop for StatementScope {
    fn drop(&mut self) {
        if self.root {
            EFFECTS.with(|c| {
                c.borrow_mut().take();
            });
        }
    }
}
pub(crate) fn note_independent_commit(sequence: u64) {
    EFFECTS.with(|c| {
        if let Some(e) = c.borrow().as_ref() {
            e.independent_commits.set(true);
            e.latest_sequence.set(e.latest_sequence.get().max(sequence));
        }
    });
}
pub(crate) fn note_independent_execution() {
    EFFECTS.with(|c| {
        if let Some(e) = c.borrow().as_ref() {
            e.independent_execution.set(true);
        }
    });
}
pub(crate) fn latest_independent_commit() -> u64 {
    EFFECTS.with(|c| c.borrow().as_ref().map_or(0, |e| e.latest_sequence.get()))
}

/// Reports a PostgreSQL NOTICE (or, with `severity`, a WARNING or INFO) of
/// the running statement, such as `table "t" does not exist, skipping`.
/// Outside a statement context (internal catalog work) it is dropped.
pub(crate) fn report_notice(severity: Option<&str>, sqlstate: &str, message: impl Into<String>) {
    let fields = severity
        .map(|severity| vec![crate::SqlErrorField::Severity(severity.to_string())])
        .unwrap_or_default();
    let _ = note_notice(SqlNotice {
        sqlstate: sqlstate.to_string(),
        message: message.into(),
        fields,
    });
}

/// `<kind> "<name>" does not exist, skipping` of `DROP ... IF EXISTS`.
pub(crate) fn notice_skipping_missing(kind: &str, name: &str) {
    report_notice(
        None,
        "00000",
        format!("{kind} \"{name}\" does not exist, skipping"),
    );
}

/// PostgreSQL's notice of the objects `DROP ... CASCADE` also drops:
/// `drop cascades to <object>`, or for several, `drop cascades to N other
/// objects` with one `drop cascades to <object>` DETAIL line each.
pub(crate) fn notice_drop_cascades(objects: &[String]) {
    let (message, fields) = match objects {
        [] => return,
        [object] => (format!("drop cascades to {object}"), Vec::new()),
        objects => (
            format!("drop cascades to {} other objects", objects.len()),
            vec![crate::SqlErrorField::Detail(
                objects
                    .iter()
                    .map(|object| format!("drop cascades to {object}"))
                    .collect::<Vec<_>>()
                    .join("\n"),
            )],
        ),
    };
    let _ = note_notice(SqlNotice {
        sqlstate: "00000".to_string(),
        message,
        fields,
    });
}

/// `relation "x" already exists, skipping` of `CREATE ... IF NOT EXISTS`.
pub(crate) fn relation_exists_skipping(name: &str) {
    report_notice(
        None,
        "42P07",
        format!(
            "relation \"{}\" already exists, skipping",
            crate::relation_name_for_message(name)
        ),
    );
}

/// The transaction of the statement this thread is executing, so catalog
/// code without a session handle can tell its own row locks from another
/// transaction's (see `schema_meta::guard_new_foreign_keys`).
pub(crate) struct StatementTransactionScope(Option<u64>);

impl StatementTransactionScope {
    pub(crate) fn enter(transaction: Option<u64>) -> Self {
        Self(STATEMENT_TRANSACTION.with(|cell| cell.replace(transaction)))
    }
}

impl Drop for StatementTransactionScope {
    fn drop(&mut self) {
        STATEMENT_TRANSACTION.with(|cell| cell.set(self.0));
    }
}

pub(crate) fn statement_transaction() -> Option<u64> {
    STATEMENT_TRANSACTION.with(Cell::get)
}
