//! Versioned supplied-package output with connection-local, nontransactional state.
use super::*;
use std::collections::VecDeque;
use std::sync::{Mutex, MutexGuard};

#[derive(Clone, Copy, Debug)]
pub(crate) enum Operation {
    Enable,
    Disable,
    Put,
    PutLine,
    NewLine,
    GetLine,
}

impl Operation {
    fn name(self) -> &'static str {
        match self {
            Self::Enable => "enable",
            Self::Disable => "disable",
            Self::Put => "put",
            Self::PutLine => "put_line",
            Self::NewLine => "new_line",
            Self::GetLine => "get_line",
        }
    }

    fn parameters(self) -> Result<Vec<RoutineParam>> {
        let declarations: &[&str] = match self {
            Self::Enable => &["buffer_size numeric DEFAULT 20000"],
            Self::Put | Self::PutLine => &["a text"],
            Self::GetLine => &["OUT line text", "OUT status numeric"],
            Self::Disable | Self::NewLine => &[],
        };
        parse_routine_params(
            &declarations
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>(),
        )
    }
}

const OPERATIONS: [Operation; 6] = [
    Operation::Enable,
    Operation::Disable,
    Operation::Put,
    Operation::PutLine,
    Operation::NewLine,
    Operation::GetLine,
];

fn public_operation(function: &Function) -> Option<Operation> {
    let names = function
        .name
        .0
        .iter()
        .map(|part| {
            part.as_ident().map(|id| {
                if id.quote_style.is_some() {
                    id.value.clone()
                } else {
                    id.value.to_uppercase()
                }
            })
        })
        .collect::<Option<Vec<_>>>()?;
    let name = match names
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["DBMS_OUTPUT", name] | ["SYS", "DBMS_OUTPUT", name] => name.to_string(),
        _ => return None,
    };
    OPERATIONS
        .into_iter()
        .find(|op| op.name().to_uppercase() == name)
}

fn arguments(function: &Function, op: Operation) -> Result<Vec<RoutineExpr>> {
    let params = op.parameters()?;
    procedure_actual_arguments(function, &params, false)?
        .into_iter()
        .zip(params)
        .map(|(arg, param)| {
            arg.cloned()
                .or(param.default_expr)
                .map(RoutineExpr::unbound)
                .ok_or_else(|| unsupported("missing output package argument"))
        })
        .collect()
}

/// Called only for compiler versions which explicitly opt in to this package.
pub(crate) fn lower(
    statements: &mut [RoutineStmt],
    handlers: &mut [RoutineExceptionHandler],
) -> Result<()> {
    for statement in statements {
        match statement {
            RoutineStmt::Sql(Statement::Call(function)) if public_operation(function).is_some() => {
                let op = public_operation(function).unwrap();
                *statement = RoutineStmt::OracleBuiltin {
                    builtin: errors::Builtin::Output(op),
                    arguments: arguments(function, op)?,
                };
            }
            RoutineStmt::Block { body, handlers, .. } => lower(body, handlers)?,
            RoutineStmt::If {
                then_body,
                else_body,
                ..
            } => {
                lower(then_body, &mut [])?;
                lower(else_body, &mut [])?;
            }
            RoutineStmt::Case {
                arms, else_body, ..
            } => {
                for body in arms {
                    lower(body, &mut [])?;
                }
                if let Some(body) = else_body {
                    lower(body, &mut [])?;
                }
            }
            RoutineStmt::ForLoop { body, .. }
            | RoutineStmt::ForeachLoop { body, .. }
            | RoutineStmt::QueryForLoop { body, .. }
            | RoutineStmt::CursorForLoop { body, .. }
            | RoutineStmt::Loop { body }
            | RoutineStmt::WhileLoop { body, .. } => lower(body, &mut [])?,
            _ => {}
        }
    }
    for handler in handlers {
        lower(&mut handler.statements, &mut [])?;
    }
    Ok(())
}

pub(crate) fn lower_calls<T: sqlparser::ast::VisitMut>(node: &mut T) -> Result<()> {
    struct Calls;
    impl sqlparser::ast::VisitorMut for Calls {
        type Break = SqlError;
        fn pre_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<SqlError> {
            if let Statement::Call(function) = statement {
                if let Some(op) = public_operation(function) {
                    // Validate named/default arguments before changing identity.
                    if let Err(error) = arguments(function, op) {
                        return ControlFlow::Break(error);
                    }
                    function.name = ObjectName::from(vec![
                        Ident::new("pg_catalog"),
                        Ident::new(format!("bicdb_oracle_output_{}_v25", op.name())),
                    ]);
                }
            }
            ControlFlow::Continue(())
        }
    }
    if let ControlFlow::Break(error) = sqlparser::ast::VisitMut::visit(node, &mut Calls) {
        return Err(error);
    }
    Ok(())
}

#[derive(Clone, Debug, Default)]
pub(crate) struct State(Arc<Mutex<Option<Box<Buffer>>>>);

// The shared slot exists before the first effect, so cloned transaction and
// statement state observes initialization too. Native connections do not pay
// for an Oracle buffer until an output operation actually needs one.
struct BufferGuard<'a>(MutexGuard<'a, Option<Box<Buffer>>>);
impl std::ops::Deref for BufferGuard<'_> {
    type Target = Buffer;
    fn deref(&self) -> &Buffer {
        self.0.as_deref().expect("initialized output buffer")
    }
}
impl std::ops::DerefMut for BufferGuard<'_> {
    fn deref_mut(&mut self) -> &mut Buffer {
        self.0.as_deref_mut().expect("initialized output buffer")
    }
}

fn shared_allocation_bytes() -> usize {
    std::mem::size_of::<Mutex<Option<Box<Buffer>>>>() + 2 * std::mem::size_of::<usize>()
}

#[derive(Debug)]
struct Buffer {
    enabled: bool,
    limit: Option<usize>,
    lines: VecDeque<String>,
    queued_allocated_bytes: usize,
    partial: String,
    // Successfully written bytes since the last ENABLE/read-then-write reset;
    // consumed/discarded lines do not refund this Oracle API counter.
    bytes: usize,
    read: bool,
    memory_limit: usize,
}

impl Default for Buffer {
    fn default() -> Self {
        Self {
            enabled: false,
            limit: Some(20000),
            lines: VecDeque::new(),
            queued_allocated_bytes: 0,
            partial: String::new(),
            bytes: 0,
            read: false,
            memory_limit: std::env::var("BICDB_ORACLE_OUTPUT_MAX_BYTES")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(64 * 1024 * 1024),
        }
    }
}

fn memory_error() -> SqlError {
    SqlError::data_exception(
        "53200",
        "Oracle output session memory budget exceeded",
        None,
    )
}

impl Buffer {
    fn memory_estimate(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(self.partial.capacity())
            .saturating_add(
                self.lines
                    .capacity()
                    .saturating_mul(std::mem::size_of::<String>()),
            )
            .saturating_add(self.queued_allocated_bytes)
    }
    fn purge(&mut self) {
        self.lines = VecDeque::new();
        self.queued_allocated_bytes = 0;
        self.partial = String::new();
        self.bytes = 0;
        self.read = false;
    }
    fn put(&mut self, text: &str, newline: bool) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        if self.read {
            self.purge();
        }
        let line_bytes = self.partial.len().saturating_add(text.len());
        if line_bytes > 32767 {
            // A line error discards the unfinished line, but does not refund
            // bytes charged since ENABLE. Buffer overflow, in contrast,
            // preserves the unfinished line for a smaller subsequent append.
            self.partial.clear();
            return Err(errors::database_error(
                -20000,
                "P0001",
                "ORU-10028: line length overflow, limit of 32767 bytes per line",
            ));
        }
        if self
            .limit
            .is_some_and(|limit| self.bytes.saturating_add(text.len()) > limit)
        {
            return Err(errors::database_error(
                -20000,
                "P0001",
                &format!(
                    "ORU-10027: buffer overflow, limit of {} bytes",
                    self.limit.unwrap()
                ),
            ));
        }
        // Includes container overhead, so empty lines cannot bypass the host's
        // resource budget. ENABLE(NULL) removes the Oracle API's byte limit,
        // not the deployment's memory budget; exhaustion is a distinct error.
        let queue_capacity = if newline && self.lines.len() == self.lines.capacity() {
            self.lines.capacity().max(2).saturating_mul(2)
        } else {
            self.lines.capacity()
        };
        let queue_growth = queue_capacity
            .saturating_sub(self.lines.capacity())
            .saturating_mul(std::mem::size_of::<String>());
        let text_growth = line_bytes.saturating_sub(self.partial.capacity());
        if self
            .memory_estimate()
            .saturating_add(shared_allocation_bytes())
            .saturating_add(queue_growth)
            .saturating_add(text_growth)
            > self.memory_limit
        {
            return Err(memory_error());
        }
        self.partial
            .try_reserve_exact(text.len())
            .map_err(|_| memory_error())?;
        if queue_capacity > self.lines.capacity() {
            // Explicit geometric growth keeps allocation and accounting bounded
            // before reservation, while avoiding a scan of every queued line.
            self.lines
                .try_reserve_exact(queue_capacity - self.lines.len())
                .map_err(|_| memory_error())?;
        }
        self.partial.push_str(text);
        self.bytes += text.len();
        if newline {
            let line = std::mem::take(&mut self.partial);
            self.queued_allocated_bytes += line.capacity();
            self.lines.push_back(line);
        }
        Ok(())
    }
    fn get(&mut self) -> (SqlValue, SqlValue) {
        self.read = true;
        if self.enabled {
            if let Some(line) = self.lines.pop_front() {
                self.queued_allocated_bytes -= line.capacity();
                return (
                    if line.is_empty() {
                        SqlValue::Null
                    } else {
                        SqlValue::String(line)
                    },
                    SqlValue::Int(0),
                );
            }
        }
        (SqlValue::Null, SqlValue::Int(1))
    }
}

impl State {
    fn buffer(&self) -> Result<BufferGuard<'_>> {
        let mut slot = self
            .0
            .lock()
            .map_err(|_| SqlError::data_exception("XX000", "output buffer lock poisoned", None))?;
        slot.get_or_insert_with(|| Box::new(Buffer::default()));
        Ok(BufferGuard(slot))
    }
    pub(crate) fn memory_estimate(&self) -> usize {
        self.0
            .lock()
            .map(|slot| {
                shared_allocation_bytes().saturating_add(
                    slot.as_ref()
                        .map(|buffer| buffer.memory_estimate())
                        .unwrap_or_default(),
                )
            })
            .unwrap_or(usize::MAX)
    }
    pub(crate) fn reset(&self) -> Result<()> {
        let mut slot = self
            .0
            .lock()
            .map_err(|_| SqlError::data_exception("XX000", "output buffer lock poisoned", None))?;
        if let Some(buffer) = slot.as_mut() {
            buffer.purge();
            buffer.enabled = false;
            buffer.limit = Some(20000);
        }
        Ok(())
    }
    fn apply(&self, op: Operation, values: &[SqlValue]) -> Result<Option<(SqlValue, SqlValue)>> {
        // Binding precedes the package's disabled fast path.
        let text = match op {
            Operation::Put | Operation::PutLine => match &values[0] {
                SqlValue::Null => String::new(),
                SqlValue::String(s) => s.clone(),
                SqlValue::Int(_) | SqlValue::Float(_) => values[0].to_cell(),
                _ => {
                    return Err(unsupported(
                        "output input requires a character or numeric scalar",
                    ))
                }
            },
            _ => String::new(),
        };
        if text.len() > 32767 {
            return Err(errors::database_error(
                -6502,
                "22001",
                "PL/SQL: numeric or value error: character string buffer too small",
            ));
        }
        let limit = if matches!(op, Operation::Enable) {
            if matches!(values[0], SqlValue::Null) {
                None
            } else {
                Some(
                    collections::integer_argument(&values[0])?
                        .unwrap()
                        .clamp(2000, 1_000_000) as usize,
                )
            }
        } else {
            None
        };
        let mut buffer = self.buffer()?;
        match op {
            Operation::Enable => {
                buffer.limit = limit;
                // ENABLE retains complete and partial lines, resets only the
                // API byte accounting, and honors the most recent limit.
                buffer.bytes = 0;
                buffer.enabled = true;
            }
            Operation::Disable => {
                buffer.purge();
                buffer.enabled = false;
            }
            Operation::Put | Operation::PutLine | Operation::NewLine => {
                buffer.put(&text, !matches!(op, Operation::Put))?
            }
            Operation::GetLine => return Ok(Some(buffer.get())),
        }
        Ok(None)
    }
}

fn destination(expr: &Expr, frame: &RoutineFrame) -> Result<RoutineAssignmentTarget> {
    // OUT validation precedes buffer consumption. More general collection and
    // record-field destinations require their own typed assignment validation.
    let Expr::Identifier(id) = expr else {
        return Err(unsupported(
            "output destination requires a declared scalar variable",
        ));
    };
    let name = normalize_object_name(&ident_value(id));
    if !frame.slot_ids.contains_key(&name) && !frame.values.contains_key(&name) {
        return Err(SqlError::data_exception(
            "42804",
            "output destination requires a declared variable",
            None,
        ));
    }
    Ok(RoutineAssignmentTarget::Variable(name))
}

pub(crate) fn invoke(
    session: &mut SqlSession<'_>,
    frame: &mut RoutineFrame,
    op: Operation,
    args: &[RoutineExpr],
) -> Result<RoutineControl> {
    session.cancellation.check()?;
    let targets = if matches!(op, Operation::GetLine) {
        Some((
            destination(&args[0].expr, frame)?,
            destination(&args[1].expr, frame)?,
        ))
    } else {
        None
    };
    let values = if targets.is_some() || crate::statement_context::describing() {
        Vec::new()
    } else {
        args.iter()
            .map(|arg| session.eval_routine_expr(frame, arg))
            .collect::<Result<Vec<_>>>()?
    };
    let output = if crate::statement_context::describing() {
        matches!(op, Operation::GetLine).then_some((SqlValue::Null, SqlValue::Null))
    } else {
        session.oracle_output.apply(op, &values)?
    };
    if let Some((line, status)) = output {
        let (a, b) = targets.unwrap();
        session.assign_routine_value(frame, &a, line)?;
        session.assign_routine_value(frame, &b, status)?;
    }
    Ok(RoutineControl::Continue)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> (tempfile::TempDir, BicDb) {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        (root, db)
    }
    fn run(session: &mut SqlSession<'_>, sql: &str) -> Result<SqlResult> {
        let sql = lower_session_sql(session.db_ref(), sql, ORACLE_PREVIEW_PROFILE, None, "bicdb")?;
        session.execute(&sql)
    }
    fn get(session: &mut SqlSession<'_>) -> Vec<String> {
        run(session, "CALL DBMS_OUTPUT.GET_LINE(NULL,NULL)")
            .unwrap()
            .rows[0]
            .iter()
            .map(SqlValue::to_cell)
            .collect()
    }

    #[test]
    fn output_state_is_lazy_but_initialization_is_shared_with_prior_snapshots() {
        let state = State::default();
        let snapshot = state.clone();
        let empty = state.memory_estimate();
        assert!(state.0.lock().unwrap().is_none());
        state.reset().unwrap();
        assert!(state.0.lock().unwrap().is_none());
        state.apply(Operation::Enable, &[SqlValue::Null]).unwrap();
        state
            .apply(
                Operation::PutLine,
                &[SqlValue::String("shared initialization".into())],
            )
            .unwrap();
        assert!(snapshot.memory_estimate() > empty);
        assert_eq!(
            snapshot.apply(Operation::GetLine, &[]).unwrap(),
            Some((
                SqlValue::String("shared initialization".into()),
                SqlValue::Int(0)
            ))
        );
        assert_eq!(
            state.apply(Operation::GetLine, &[]).unwrap(),
            Some((SqlValue::Null, SqlValue::Int(1)))
        );
    }

    #[test]
    fn output_buffers_use_named_defaults_partial_lines_and_nontransactional_state() {
        let (_root, mut db) = setup();
        let mut s = SqlSession::new(&mut db);
        run(&mut s, "CALL DBMS_OUTPUT.PUT_LINE('disabled')").unwrap();
        assert_eq!(get(&mut s), ["", "1"]);
        for sql in [
            "CALL SYS.DBMS_OUTPUT.ENABLE(buffer_size=>NULL)",
            "BEGIN",
            "SAVEPOINT output_mark",
            "CALL DBMS_OUTPUT.PUT(a=>'left')",
            "CALL DBMS_OUTPUT.PUT_LINE(a=>'right')",
            "ROLLBACK TO SAVEPOINT output_mark",
            "ROLLBACK",
        ] {
            run(&mut s, sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        assert_eq!(get(&mut s), ["leftright", "0"]);
        for sql in [
            "CALL DBMS_OUTPUT.PUT_LINE('one')",
            "CALL DBMS_OUTPUT.PUT_LINE('two')",
        ] {
            run(&mut s, sql).unwrap();
        }
        assert_eq!(get(&mut s), ["one", "0"]);
        run(&mut s, "CALL DBMS_OUTPUT.PUT_LINE('new')").unwrap();
        assert_eq!(get(&mut s), ["new", "0"]);
        run(&mut s, "CALL DBMS_OUTPUT.NEW_LINE()").unwrap();
        assert_eq!(get(&mut s), ["", "0"]);
        let state = s.session_guc_state();
        drop(s);
        let mut s = SqlSession::new(&mut db).with_session_guc_state(state);
        run(&mut s, "CALL DBMS_OUTPUT.PUT_LINE('retained')").unwrap();
        assert_eq!(get(&mut s), ["retained", "0"]);
        s.execute("DISCARD ALL").unwrap();
        run(&mut s, "CALL DBMS_OUTPUT.PUT_LINE('disabled again')").unwrap();
        assert_eq!(get(&mut s), ["", "1"]);
    }

    #[test]
    fn output_survives_nested_calls_autonomous_rollback_and_sql_row_functions() {
        let (_root, mut db) = setup();
        let mut s = SqlSession::new(&mut db);
        for sql in ["CREATE TABLE output_rows(id NUMBER)", "INSERT INTO output_rows VALUES(1),(2),(3)",
            "CREATE PROCEDURE output_child IS PRAGMA AUTONOMOUS_TRANSACTION;BEGIN DBMS_OUTPUT.PUT_LINE('child');ROLLBACK;END;",
            "CREATE PROCEDURE output_parent IS BEGIN DBMS_OUTPUT.PUT_LINE('parent');output_child();END;",
            "CREATE FUNCTION output_value(p NUMBER) RETURN NUMBER IS BEGIN DBMS_OUTPUT.PUT_LINE(p);RETURN p;END;",
            "CREATE PROCEDURE output_get(line OUT VARCHAR2,status OUT NUMBER) IS BEGIN DBMS_OUTPUT.GET_LINE(line,status);END;",
            "CREATE FUNCTION output_bad_argument RETURN NUMBER IS BEGIN RAISE_APPLICATION_ERROR(-20100,'evaluated argument');RETURN 1;END;",
            "CREATE PROCEDURE output_describe_argument IS BEGIN DBMS_OUTPUT.PUT_LINE(output_bad_argument());END;",
            "CALL DBMS_OUTPUT.ENABLE()", "CALL output_parent()"] {
            run(&mut s,sql).unwrap_or_else(|e|panic!("{sql}: {e}"));
        }
        assert_eq!(get(&mut s), ["parent", "0"]);
        assert_eq!(get(&mut s), ["child", "0"]);
        let rows = run(&mut s, "SELECT output_value(id) FROM output_rows")
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 3);
        let mut messages = (0..3).map(|_| get(&mut s)[0].clone()).collect::<Vec<_>>();
        messages.sort();
        assert_eq!(messages, ["1", "2", "3"]);
        run(&mut s, "CALL DBMS_OUTPUT.PUT_LINE('out variable')").unwrap();
        let row = run(&mut s, "CALL output_get(NULL,NULL)").unwrap().rows[0]
            .iter()
            .map(SqlValue::to_cell)
            .collect::<Vec<_>>();
        assert_eq!(row, ["out variable", "0"]);
        run(&mut s, "CALL DBMS_OUTPUT.PUT_LINE('descriptor retained')").unwrap();
        let scope = crate::statement_context::SqlDescriptionScope::enter();
        run(&mut s, "CALL DBMS_OUTPUT.PUT_LINE('describe only')").unwrap();
        run(&mut s, "CALL output_parent()").unwrap();
        run(&mut s, "CALL DBMS_OUTPUT.PUT_LINE(output_bad_argument())").unwrap();
        run(&mut s, "CALL output_describe_argument()").unwrap();
        run(&mut s, "CALL output_get(NULL,NULL)").unwrap();
        drop(scope);
        assert_eq!(get(&mut s), ["descriptor retained", "0"]);
        assert_eq!(get(&mut s), ["", "1"]);
    }

    #[test]
    fn output_enable_and_overflow_follow_independent_byte_accounting() {
        let (_root, mut db) = setup();
        let mut s = SqlSession::new(&mut db);
        run(&mut s, "CALL DBMS_OUTPUT.ENABLE(NULL)").unwrap();
        run(&mut s, "CALL DBMS_OUTPUT.ENABLE(2000)").unwrap();
        assert!(run(
            &mut s,
            &format!("CALL DBMS_OUTPUT.PUT_LINE('{}')", "x".repeat(2500))
        )
        .unwrap_err()
        .to_string()
        .contains("ORU-10027"));
        for sql in [
            "CALL DBMS_OUTPUT.DISABLE()".to_string(),
            "CALL DBMS_OUTPUT.ENABLE(2000)".into(),
            format!("CALL DBMS_OUTPUT.PUT('{}')", "x".repeat(1000)),
            "CALL DBMS_OUTPUT.ENABLE(2000)".into(),
            format!("CALL DBMS_OUTPUT.PUT_LINE('{}')", "y".repeat(1500)),
        ] {
            run(&mut s, &sql).unwrap();
        }
        assert_eq!(get(&mut s)[0].len(), 2500);
        for sql in [
            "CALL DBMS_OUTPUT.DISABLE()".to_string(),
            "CALL DBMS_OUTPUT.ENABLE(2000)".into(),
            format!("CALL DBMS_OUTPUT.PUT_LINE('{}')", "x".repeat(1000)),
            format!("CALL DBMS_OUTPUT.PUT('{}')", "y".repeat(600)),
        ] {
            run(&mut s, &sql).unwrap();
        }
        assert!(run(
            &mut s,
            &format!("CALL DBMS_OUTPUT.PUT('{}')", "z".repeat(500))
        )
        .unwrap_err()
        .to_string()
        .contains("ORU-10027"));
        run(
            &mut s,
            &format!("CALL DBMS_OUTPUT.PUT_LINE('{}')", "w".repeat(100)),
        )
        .unwrap();
        assert_eq!(get(&mut s)[0].len(), 1000);
        assert_eq!(get(&mut s)[0].len(), 700);
    }

    #[test]
    fn output_errors_recover_and_empty_lines_cannot_escape_memory_accounting() {
        let (_root, mut db) = setup();
        let mut s = SqlSession::new(&mut db);
        run(&mut s, "CALL DBMS_OUTPUT.ENABLE(NULL)").unwrap();
        run(&mut s, "CALL DBMS_OUTPUT.PUT_LINE('complete')").unwrap();
        run(
            &mut s,
            &format!("CALL DBMS_OUTPUT.PUT('{}')", "x".repeat(32000)),
        )
        .unwrap();
        let error = run(
            &mut s,
            &format!("CALL DBMS_OUTPUT.PUT('{}')", "y".repeat(1000)),
        )
        .unwrap_err();
        assert!(error.to_string().contains("ORA-20000: ORU-10028"));
        run(&mut s, "CALL DBMS_OUTPUT.PUT_LINE('tail')").unwrap();
        assert_eq!(get(&mut s), ["complete", "0"]);
        assert_eq!(get(&mut s), ["tail", "0"]);
        let state = State::default();
        {
            let mut b = state.buffer().unwrap();
            b.enabled = true;
            b.limit = None;
            b.memory_limit = 4096;
        }
        let before = state.memory_estimate();
        let mut count = 0;
        loop {
            match state.apply(Operation::NewLine, &[]) {
                Ok(_) => count += 1,
                Err(e) => {
                    assert_eq!(e.sqlstate(), "53200");
                    break;
                }
            }
        }
        assert!(count > 0 && count < 200);
        assert!(state.memory_estimate() > before);
        state.reset().unwrap();
        assert_eq!(state.memory_estimate(), before);
    }

    #[test]
    fn output_version_fences_do_not_change_old_routines_or_native_procedure_names() {
        let (_root, mut db) = setup();
        let mut s = SqlSession::new(&mut db);
        let sql = lower_session_sql(
            s.db_ref(),
            "CREATE PROCEDURE legacy_output IS BEGIN DBMS_OUTPUT.PUT_LINE('legacy');END;",
            ORACLE_PREVIEW_PROFILE,
            None,
            "bicdb",
        )
        .unwrap();
        s.execute(&sql.replace(PREVIEW_ROUTINE_LANGUAGE, PREVIEW_ROUTINE_LANGUAGE_V24))
            .unwrap();
        let error = run(&mut s, "CALL legacy_output()").unwrap_err();
        assert!(error.to_string().contains("does not exist"));
        run(&mut s, "CALL DBMS_OUTPUT.ENABLE()").unwrap();
        run(&mut s, "CALL DBMS_OUTPUT.PUT_LINE('private')").unwrap();
        drop(s);
        let mut other = SqlSession::new(&mut db);
        run(&mut other, "CALL DBMS_OUTPUT.ENABLE()").unwrap();
        assert_eq!(get(&mut other), ["", "1"]);
        for sql in ["CREATE SCHEMA dbms_output", "CREATE TABLE native_output(v text)",
            "CREATE PROCEDURE dbms_output.put_line(a text) LANGUAGE plpgsql AS $$BEGIN INSERT INTO native_output VALUES(a);END$$",
            "CALL dbms_output.put_line('native')"] {other.execute(sql).unwrap_or_else(|e|panic!("{sql}: {e}"));}
        assert_eq!(
            other.execute("SELECT v FROM native_output").unwrap().rows,
            vec![vec![SqlValue::String("native".into())]]
        );
    }
}

impl SqlSession<'_> {
    pub(crate) fn execute_output_call(&mut self, function: &Function) -> Result<Option<SqlResult>> {
        let names = object_name_parts(&function.name);
        let [schema, name] = names.as_slice() else {
            return Ok(None);
        };
        if schema != "pg_catalog" {
            return Ok(None);
        }
        let Some(op) = OPERATIONS
            .into_iter()
            .find(|op| *name == format!("bicdb_oracle_output_{}_v25", op.name()))
        else {
            return Ok(None);
        };
        if !preview_routine_language_enabled(self.db_ref())? {
            return Err(unsupported(
                "output calls require the durable Oracle preview profile",
            ));
        }
        let args = arguments(function, op)?;
        let values = if matches!(op, Operation::GetLine) || crate::statement_context::describing() {
            Vec::new()
        } else {
            args.iter()
                .map(|arg| self.eval_session_expr(&arg.expr))
                .collect::<Result<Vec<_>>>()?
        };
        let output = if crate::statement_context::describing() {
            matches!(op, Operation::GetLine).then_some((SqlValue::Null, SqlValue::Null))
        } else {
            self.oracle_output.apply(op, &values)?
        };
        Ok(Some(if let Some((line, status)) = output {
            SqlResult::new(
                vec!["line".into(), "status".into()],
                vec![vec![line, status]],
            )
            .with_column_types(vec![Some("text".into()), Some("numeric".into())])
        } else {
            SqlResult::command("CALL")
        }))
    }
}
