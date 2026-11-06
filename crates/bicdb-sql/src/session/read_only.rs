//! Transaction access modes are enforced at SQL execution, including nested
//! routine statements and nontransactional sequence allocation.
use super::*;

pub(super) const SETTING: &str = "transaction_read_only";
pub(super) fn violation() -> SqlError {
    SqlError::data_exception(
        "25006",
        "command is not permitted in a read-only transaction",
        None,
    )
}

/// Transport hosts validate BEGIN separately from creating the core transaction.
pub fn parse_transaction_read_only(sql: &str) -> Result<Option<bool>> {
    let statements = parse_statements(sql)?;
    let [statement] = statements.as_slice() else {
        return Ok(None);
    };
    let modes = match statement {
        Statement::StartTransaction { modes, .. }
        | Statement::Set(Set::SetTransaction {
            modes,
            session: false,
            ..
        }) => modes,
        _ => return Ok(None),
    };
    Ok(modes.iter().find_map(|mode| match mode {
        TransactionMode::AccessMode(TransactionAccessMode::ReadOnly) => Some(true),
        TransactionMode::AccessMode(TransactionAccessMode::ReadWrite) => Some(false),
        _ => None,
    }))
}

impl SqlSessionGucState {
    pub fn transaction_read_only(&self) -> bool {
        self.session_gucs
            .get(SETTING)
            .is_some_and(|value| matches!(value.to_ascii_lowercase().as_str(), "on" | "true" | "1"))
    }
    pub fn set_transaction_read_only_label(&mut self, read_only: bool) -> Result<()> {
        // Absence already means read-write. Keep ordinary BEGIN's GUC snapshot
        // unchanged, including its bounded connection-memory accounting.
        if !read_only && !self.session_gucs.contains_key(SETTING) {
            return Ok(());
        }
        self.begin_transaction();
        let previous = self.session_gucs.get(SETTING).cloned();
        self.local_restore.entry(SETTING.into()).or_insert(previous);
        Arc::make_mut(&mut self.session_gucs)
            .insert(SETTING.into(), if read_only { "on" } else { "off" }.into());
        Ok(())
    }
}

impl SqlSession<'_> {
    pub(crate) fn transaction_read_only(&self) -> bool {
        self.session_gucs
            .get(SETTING)
            .is_some_and(|value| matches!(value.to_ascii_lowercase().as_str(), "on" | "true" | "1"))
    }
    pub(crate) fn require_transaction_write(&self) -> Result<()> {
        if self.transaction_read_only() {
            Err(violation())
        } else {
            Ok(())
        }
    }
    pub(crate) fn ensure_read_only_query(&self, query: &Query) -> Result<()> {
        if self.transaction_read_only() && (query_may_write(query) || query_has_row_locks(query)) {
            return Err(violation());
        }
        Ok(())
    }
    pub(crate) fn ensure_read_only_statement(&self, statement: &Statement) -> Result<()> {
        if !self.transaction_read_only() {
            return Ok(());
        }
        if self.routine_slots.is_some()
            && matches!(
                statement,
                Statement::StartTransaction { .. }
                    | Statement::Commit { .. }
                    | Statement::Rollback { .. }
                    | Statement::Savepoint { .. }
                    | Statement::ReleaseSavepoint { .. }
            )
        {
            return Err(violation());
        }
        match statement {
            Statement::Query(query) => self.ensure_read_only_query(query),
            Statement::ShowVariable { .. }
            | Statement::Set(_)
            | Statement::Reset(_)
            | Statement::StartTransaction { .. }
            | Statement::Commit { .. }
            | Statement::Rollback { .. }
            | Statement::Savepoint { .. }
            | Statement::ReleaseSavepoint { .. } => Ok(()),
            // Unclassified/raw commands cannot silently acquire write access.
            _ => Err(violation()),
        }
    }
    pub(crate) fn set_transaction_read_only_label(&mut self, read_only: bool) -> Result<()> {
        let mut state = self.take_session_guc_state();
        let result = state.set_transaction_read_only_label(read_only);
        self.install_session_guc_state(state);
        result
    }
}
