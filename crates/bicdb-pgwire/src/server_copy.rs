//! Server-side COPY: `COPY ... FROM/TO 'file'` and `COPY ... FROM/TO PROGRAM 'cmd'`.
//!
//! Three independent gates, all of which must pass:
//!
//! 1. The SQL role, as in PostgreSQL: the effective role (after `SET ROLE`) must
//!    have the privileges of `pg_read_server_files` (COPY FROM a file),
//!    `pg_write_server_files` (COPY TO a file) or `pg_execute_server_program`
//!    (COPY PROGRAM). A superuser has all three.
//! 2. The operator: file COPY is off unless the server was started with one or
//!    more `--copy-server-files-dir` directories, and COPY PROGRAM is off unless
//!    it was started with `--allow-copy-program`. Neither is a SQL setting, so no
//!    SQL role (not even a superuser) can widen them: BicDB runs as a shared
//!    cloud service and inside applications, where the SQL superuser is not the
//!    person who owns the host.
//! 3. The path: a file must lie below an allowed directory. The path must be
//!    absolute and free of `..`; it is opened component by component from the
//!    allowed directory with `O_NOFOLLOW`, so no symbolic link below the allowed
//!    directory is ever followed, including one swapped in after any check. The
//!    final file must be a regular file with a single hard link.
//!
//! The data itself goes through the same code as COPY FROM STDIN / TO STDOUT
//! (`begin_copy_in` + `copy_in_data` + `finish_copy_in`, and `render_copy_out`).
//! Only the top-level statement of a client session reaches this module: COPY
//! inside a routine, trigger or script is executed by the SQL engine, which has
//! no file or program endpoint and refuses it.
use super::*;
#[allow(unused_imports)]
use crate::*;
use std::ffi::{OsStr, OsString};
use std::path::Component;
use std::process::{Child, Command, ExitStatus, Stdio};

/// `PATH` of a COPY PROGRAM child. Its environment is otherwise empty, so the
/// server's own environment (credentials, keys, tokens) is not inherited.
const COPY_PROGRAM_PATH: &str = "/usr/local/bin:/usr/bin:/bin";

/// Operator controls for server-side COPY. Set at server start only; the
/// default enables nothing.
#[derive(Clone, Debug, Default)]
pub struct ServerCopyConfig {
    roots: Vec<CopyRoot>,
    allow_program: bool,
}

#[derive(Clone, Debug)]
struct CopyRoot {
    canonical: PathBuf,
    /// Components of the canonical path and of the path as configured: a
    /// statement may name the directory either way.
    spellings: Vec<Vec<OsString>>,
}

impl ServerCopyConfig {
    /// `file_dirs`: directories below which COPY may read and write server
    /// files (each must be an existing absolute directory; it is canonicalized
    /// now). `allow_program`: whether COPY PROGRAM may run commands. The two
    /// are independent.
    pub fn new(file_dirs: &[PathBuf], allow_program: bool) -> io::Result<Self> {
        let mut roots = Vec::new();
        for dir in file_dirs {
            if !dir.is_absolute() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "server-side COPY directory {} must be an absolute path",
                        dir.display()
                    ),
                ));
            }
            let canonical = fs::canonicalize(dir).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!("server-side COPY directory {}: {error}", dir.display()),
                )
            })?;
            if !canonical.is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "server-side COPY directory {} is not a directory",
                        dir.display()
                    ),
                ));
            }
            let mut spellings = vec![normal_components(&canonical)];
            let configured = normal_components(dir);
            if !configured.is_empty() && !spellings.contains(&configured) {
                spellings.push(configured);
            }
            roots.push(CopyRoot {
                canonical,
                spellings,
            });
        }
        Ok(Self {
            roots,
            allow_program,
        })
    }

    /// The canonical directories file COPY may use (empty: file COPY is off).
    pub fn file_dirs(&self) -> Vec<&Path> {
        self.roots
            .iter()
            .map(|root| root.canonical.as_path())
            .collect()
    }

    pub fn program_allowed(&self) -> bool {
        self.allow_program
    }
}

/// Lexically normal components of an absolute path; `None` entries (`..`)
/// never reach here because callers reject them first.
fn normal_components(path: &Path) -> Vec<OsString> {
    path.components()
        .filter_map(|component| match component {
            Component::Normal(part) => Some(part.to_os_string()),
            _ => None,
        })
        .collect()
}

fn copy_error(
    sqlstate: &'static str,
    message: impl Into<String>,
    detail: Option<String>,
    hint: Option<String>,
) -> PgWireError {
    PgWireError::Sql(SqlError::coded_public(sqlstate, message, detail, hint))
}

const ANYONE_HINT: &str =
    "Anyone can COPY to stdout or from stdin. psql's \\copy command also works for anyone.";

/// Gate 1 (the SQL role) and gate 2 (the operator), in that order: a role
/// PostgreSQL would refuse gets PostgreSQL's error whatever the server allows.
fn authorize(
    server: &Arc<PgWireServer>,
    state: &ConnectionState,
    copy: &CopyStatement,
) -> Result<()> {
    let from = copy.direction == CopyDirection::FromStdin;
    let (role, message, action) = match (&copy.endpoint, from) {
        (CopyEndpoint::Program(_), _) => (
            "pg_execute_server_program",
            "permission denied to COPY to or from an external program",
            "COPY to or from an external program",
        ),
        (_, true) => (
            "pg_read_server_files",
            "permission denied to COPY from a file",
            "COPY from a file",
        ),
        (_, false) => (
            "pg_write_server_files",
            "permission denied to COPY to a file",
            "COPY to a file",
        ),
    };
    let allowed = {
        let db = server.read_db_for_transaction_progress()?;
        bicdb_sql::pg_session_has_privileges_of_role(&db, state.session_state.session_gucs(), role)?
    };
    if !allowed {
        return Err(copy_error(
            "42501",
            message,
            Some(format!(
                "Only roles with privileges of the \"{role}\" role may {action}."
            )),
            Some(ANYONE_HINT.to_string()),
        ));
    }
    // 0A000, as PostgreSQL reports a statement its operator turned off
    // ("ALTER SYSTEM is not allowed in this environment"): no SQL privilege
    // can change the answer, only the server's configuration.
    match &copy.endpoint {
        CopyEndpoint::Program(_) if !server.config.server_copy.allow_program => Err(copy_error(
            "0A000",
            "COPY to or from an external program is not allowed in this environment",
            Some("The server was not started with --allow-copy-program.".to_string()),
            Some(ANYONE_HINT.to_string()),
        )),
        CopyEndpoint::File(_) if server.config.server_copy.roots.is_empty() => Err(copy_error(
            "0A000",
            "COPY to or from a file is not allowed in this environment",
            Some(
                "The server was not started with a --copy-server-files-dir directory.".to_string(),
            ),
            Some(ANYONE_HINT.to_string()),
        )),
        _ => Ok(()),
    }
}

/// Run a COPY whose endpoint is a server file or program and write its
/// CommandComplete. Entered only from `execute_server_query`.
pub(crate) fn execute_server_side_copy(
    stream: &mut ClientStream,
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
    copy: CopyStatement,
) -> Result<QueryFlow> {
    if state.failed_transaction {
        return Err(PgWireError::InFailedTransaction);
    }
    authorize(server, state, &copy)?;
    let from = copy.direction == CopyDirection::FromStdin;
    let (kind, target) = match &copy.endpoint {
        CopyEndpoint::File(path) => ("file", path.clone()),
        CopyEndpoint::Program(command) => ("program", command.clone()),
        CopyEndpoint::Client => unreachable!("client COPY is handled by execute_server_query"),
    };
    // Audit trail for a privileged operation: who, which way, what.
    let current_role = state
        .session_state
        .session_gucs()
        .get("role")
        .filter(|role| !role.is_empty() && role.as_str() != "none")
        .cloned()
        .unwrap_or_else(|| state.user.clone());
    eprintln!(
        "bicdb: server-side COPY {} {kind} {target:?} by role {current_role:?} (session user {:?}, connection {})",
        if from { "FROM" } else { "TO" },
        state.user,
        state.connection_id,
    );
    let count = if from {
        copy_from_server(server, state, copy)?
    } else {
        copy_to_server(server, state, copy)?
    };
    for notice in std::mem::take(&mut state.pending_notices) {
        notice_response(stream, &notice)?;
    }
    command_complete(stream, &format!("COPY {count}"))?;
    Ok(QueryFlow::Complete)
}

fn cancellation_of(server: &Arc<PgWireServer>, state: &ConnectionState) -> CancellationToken {
    CancellationToken::new(state.cancel_token.clone(), statement_deadline(server))
}

fn statement_deadline(server: &Arc<PgWireServer>) -> Option<Instant> {
    (!server.config.query_timeout.is_zero()).then(|| Instant::now() + server.config.query_timeout)
}

fn copy_from_server(
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
    copy: CopyStatement,
) -> Result<usize> {
    let endpoint = copy.endpoint.clone();
    // The target is resolved first (a missing table is reported before the
    // file is opened), as PostgreSQL's DoCopy opens the relation first.
    begin_copy_in(server, state, copy)?;
    let fed = (|| -> Result<()> {
        match &endpoint {
            CopyEndpoint::File(path) => {
                let mut file = open_server_file(&server.config.server_copy, path, Access::Read)?;
                let cancellation = cancellation_of(server, state);
                let mut buffer = vec![0_u8; 64 * 1024];
                loop {
                    cancellation
                        .check()
                        .map_err(|error| classify_bicdb_interrupt(server, state, error))?;
                    let read = file.read(&mut buffer).map_err(|error| {
                        file_access_error(
                            &error,
                            format!("could not read from COPY file: {}", os_message(&error)),
                            None,
                        )
                    })?;
                    if read == 0 {
                        return Ok(());
                    }
                    copy_in_data(state, &buffer[..read])?;
                }
            }
            CopyEndpoint::Program(command) => {
                let deadline = statement_deadline(server);
                let mut program =
                    CopyProgram::spawn(command, true, state.cancel_token.clone(), deadline)?;
                let mut stdout = program
                    .child
                    .stdout
                    .take()
                    .ok_or_else(|| PgWireError::Server("COPY program has no output".into()))?;
                let mut buffer = vec![0_u8; 64 * 1024];
                loop {
                    let read = match stdout.read(&mut buffer) {
                        Ok(read) => read,
                        Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                        Err(error) => {
                            program.check_cancelled(server, state)?;
                            return Err(file_access_error(
                                &error,
                                format!("could not read from COPY program: {}", os_message(&error)),
                                None,
                            ));
                        }
                    };
                    if read == 0 {
                        break;
                    }
                    copy_in_data(state, &buffer[..read])?;
                }
                drop(stdout);
                program.check_cancelled(server, state)?;
                let status = program.finish()?;
                program.check_cancelled(server, state)?;
                // PostgreSQL ignores SIGPIPE once all the output was read.
                let sigpipe = {
                    use std::os::unix::process::ExitStatusExt;
                    status.signal() == Some(libc::SIGPIPE)
                };
                if !status.success() && !sigpipe {
                    return Err(program_failed(command, status));
                }
                Ok(())
            }
            CopyEndpoint::Client => unreachable!(),
        }
    })();
    if let Err(error) = fed {
        abort_copy_in(server, state)?;
        return Err(error);
    }
    finish_copy_in(server, state).and_then(|count| {
        if let Some(seq) = bicdb_core::take_thread_commit_seq() {
            server.sync_quorum.wait_for(seq)?;
        }
        Ok(count)
    })
}

fn copy_to_server(
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
    copy: CopyStatement,
) -> Result<usize> {
    let result = execute_copy_query(server, state, &copy)?;
    let rows = result.rows.len();
    match &copy.endpoint {
        CopyEndpoint::File(path) => {
            let file = open_server_file(&server.config.server_copy, path, Access::Write)?;
            let mut writer = BufWriter::with_capacity(64 * 1024, file);
            let write_error = |error: io::Error| {
                file_access_error(
                    &error,
                    format!("could not write to COPY file: {}", os_message(&error)),
                    None,
                )
            };
            let bytes = render_copy_out(server, state, &copy, &result, |payload| {
                writer.write_all(payload).map_err(write_error)
            })?;
            writer.flush().map_err(write_error)?;
            server.record_streamed_rows(rows as u64, bytes);
        }
        CopyEndpoint::Program(command) => {
            let deadline = statement_deadline(server);
            let mut program =
                CopyProgram::spawn(command, false, state.cancel_token.clone(), deadline)?;
            let stdin = program
                .child
                .stdin
                .take()
                .ok_or_else(|| PgWireError::Server("COPY program has no input".into()))?;
            let mut writer = BufWriter::with_capacity(64 * 1024, stdin);
            let mut broken_pipe = None;
            let rendered = render_copy_out(server, state, &copy, &result, |payload| {
                if broken_pipe.is_some() {
                    return Ok(());
                }
                if let Err(error) = writer.write_all(payload) {
                    broken_pipe = Some(error);
                }
                Ok(())
            });
            if broken_pipe.is_none() {
                if let Err(error) = writer.flush() {
                    broken_pipe = Some(error);
                }
            }
            // Close the program's input so it sees end of data.
            drop(writer);
            program.check_cancelled(server, state)?;
            let bytes = rendered?;
            let status = program.finish()?;
            program.check_cancelled(server, state)?;
            if !status.success() {
                return Err(program_failed(command, status));
            }
            if let Some(error) = broken_pipe {
                return Err(file_access_error(
                    &error,
                    format!("could not write to COPY program: {}", os_message(&error)),
                    None,
                ));
            }
            server.record_streamed_rows(rows as u64, bytes);
        }
        CopyEndpoint::Client => unreachable!(),
    }
    Ok(rows)
}

fn program_failed(command: &str, status: ExitStatus) -> PgWireError {
    copy_error(
        "38000",
        format!("program \"{command}\" failed"),
        Some(wait_result_text(status)),
        None,
    )
}

/// PostgreSQL's `wait_result_to_str`.
fn wait_result_text(status: ExitStatus) -> String {
    use std::os::unix::process::ExitStatusExt;
    if let Some(code) = status.code() {
        return match code {
            126 => "command not executable".to_string(),
            127 => "command not found".to_string(),
            code => format!("child process exited with exit code {code}"),
        };
    }
    if let Some(signal) = status.signal() {
        // SAFETY: strsignal returns a pointer to a static or thread-local
        // NUL-terminated string, copied out immediately.
        let name = unsafe {
            let text = libc::strsignal(signal);
            if text.is_null() {
                String::new()
            } else {
                std::ffi::CStr::from_ptr(text)
                    .to_string_lossy()
                    .into_owned()
            }
        };
        return format!("child process was terminated by signal {signal}: {name}");
    }
    "child process exited with unrecognized status".to_string()
}

/// A COPY PROGRAM child: `/bin/sh -c <command>` in its own process group,
/// with an empty environment apart from `PATH` and `/` as its directory. A
/// watcher kills the whole group when the query is cancelled or times out,
/// which also unblocks a read from or write to the program. The child is
/// always reaped: by `finish`, or by `Drop` after killing it.
struct CopyProgram {
    child: Child,
    watcher_done: Arc<AtomicBool>,
    killed: Arc<AtomicBool>,
    watcher: Option<thread::JoinHandle<()>>,
    cancel: Arc<AtomicBool>,
    deadline: Option<Instant>,
    reaped: bool,
}

impl CopyProgram {
    fn spawn(
        command: &str,
        from: bool,
        cancel: Arc<AtomicBool>,
        deadline: Option<Instant>,
    ) -> Result<Self> {
        use std::os::unix::process::CommandExt;
        let mut process = Command::new("/bin/sh");
        process
            .arg("-c")
            .arg(command)
            .env_clear()
            .env("PATH", COPY_PROGRAM_PATH)
            .current_dir("/")
            .stdin(if from { Stdio::null() } else { Stdio::piped() })
            .stdout(if from { Stdio::piped() } else { Stdio::null() })
            .stderr(Stdio::inherit())
            .process_group(0);
        let child = process.spawn().map_err(|error| {
            file_access_error(
                &error,
                format!(
                    "could not execute command \"{command}\": {}",
                    os_message(&error)
                ),
                None,
            )
        })?;
        let watcher_done = Arc::new(AtomicBool::new(false));
        let killed = Arc::new(AtomicBool::new(false));
        let pid = child.id() as libc::pid_t;
        let watcher = {
            let done = watcher_done.clone();
            let killed = killed.clone();
            let cancel = cancel.clone();
            thread::Builder::new()
                .name("bicdb-copy-program".into())
                .spawn(move || {
                    while !done.load(Ordering::SeqCst) {
                        if cancel.load(Ordering::SeqCst)
                            || deadline.is_some_and(|deadline| Instant::now() >= deadline)
                        {
                            killed.store(true, Ordering::SeqCst);
                            // SAFETY: the group leader is not reaped before this
                            // thread is joined, so the group id is still ours.
                            unsafe {
                                libc::kill(-pid, libc::SIGKILL);
                            }
                            return;
                        }
                        thread::sleep(Duration::from_millis(20));
                    }
                })
                .ok()
        };
        Ok(Self {
            child,
            watcher_done,
            killed,
            watcher,
            cancel,
            deadline,
            reaped: false,
        })
    }

    fn stop_watcher(&mut self) {
        self.watcher_done.store(true, Ordering::SeqCst);
        if let Some(watcher) = self.watcher.take() {
            let _ = watcher.join();
        }
    }

    /// The query's cancellation or timeout error once the watcher killed the
    /// program for it.
    fn check_cancelled(&self, server: &Arc<PgWireServer>, state: &ConnectionState) -> Result<()> {
        if !self.killed.load(Ordering::SeqCst) {
            return Ok(());
        }
        let token = CancellationToken::new(self.cancel.clone(), self.deadline);
        match token.check() {
            Err(error) => Err(classify_bicdb_interrupt(server, state, error)),
            Ok(()) => Err(PgWireError::Sql(SqlError::coded_public(
                "57014",
                "canceling statement due to user request",
                None,
                None,
            ))),
        }
    }

    /// Wait for the program to exit (it has seen end of input, or closed its
    /// output) and reap it.
    fn finish(&mut self) -> Result<ExitStatus> {
        let status = self.child.wait();
        self.reaped = true;
        self.stop_watcher();
        status.map_err(|error| {
            file_access_error(
                &error,
                format!(
                    "could not close pipe to external command: {}",
                    os_message(&error)
                ),
                None,
            )
        })
    }
}

impl Drop for CopyProgram {
    fn drop(&mut self) {
        if !self.reaped {
            // An error stopped the COPY early: end the whole program group.
            // SAFETY: the child has not been reaped, so its group id is ours.
            unsafe {
                libc::kill(-(self.child.id() as libc::pid_t), libc::SIGKILL);
            }
            let _ = self.child.wait();
        }
        self.stop_watcher();
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Access {
    Read,
    Write,
}

/// The text of `%m` for an OS error, without Rust's "(os error N)" suffix.
fn os_message(error: &io::Error) -> String {
    let text = error.to_string();
    match text.rfind(" (os error ") {
        Some(idx) => text[..idx].to_string(),
        None => text,
    }
}

/// PostgreSQL's `errcode_for_file_access`.
fn file_access_sqlstate(error: &io::Error) -> &'static str {
    match error.raw_os_error() {
        Some(libc::EPERM | libc::EACCES | libc::EROFS) => "42501",
        Some(libc::ENOENT) => "58P01",
        Some(libc::EEXIST) => "58P02",
        Some(libc::ENOTDIR | libc::EISDIR | libc::EMLINK) => "42809",
        Some(libc::ENOSPC) => "53100",
        Some(libc::ENFILE | libc::EMFILE) => "53000",
        Some(libc::EIO) => "58030",
        Some(libc::ENAMETOOLONG) => "42622",
        _ => "XX000",
    }
}

fn file_access_error(error: &io::Error, message: String, hint: Option<String>) -> PgWireError {
    copy_error(file_access_sqlstate(error), message, None, hint)
}

/// Open `raw` for COPY below one of the operator's directories (gate 3).
///
/// Guarantees: the path is absolute, contains no `..`, and lies lexically
/// below an allowed directory; every component below that directory is opened
/// with `openat(..., O_NOFOLLOW)` relative to the previous one, so a symbolic
/// link anywhere below the allowed directory is refused even if it is created
/// between statements or during this walk; the file is a regular file with
/// one hard link (a hard link could name a file outside the directory). A
/// file written by COPY TO is created with mode 0644 (before the umask) and
/// truncated only after these checks. The allowed directory itself and its
/// ancestors are the operator's to protect.
fn open_server_file(config: &ServerCopyConfig, raw: &str, access: Access) -> Result<File> {
    let verb = match access {
        Access::Read => "reading",
        Access::Write => "writing",
    };
    let refuse = |reason: &str| {
        copy_error(
            "42501",
            format!("could not open file \"{raw}\" for {verb}: {reason}"),
            None,
            Some(
                "Server-side COPY reads and writes only regular files below the directories \
                 the server was started with (--copy-server-files-dir)."
                    .to_string(),
            ),
        )
    };
    let path = Path::new(raw);
    if !path.is_absolute() {
        return Err(match access {
            Access::Write => copy_error(
                "42602",
                "relative path not allowed for COPY to file",
                None,
                None,
            ),
            Access::Read => refuse("relative paths are not allowed for server-side COPY"),
        });
    }
    let mut parts: Vec<&OsStr> = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => parts.push(part),
            Component::ParentDir => return Err(refuse("\"..\" is not allowed in the path")),
            Component::RootDir | Component::CurDir | Component::Prefix(_) => {}
        }
    }
    let parts = parts.as_slice();
    let Some((root, rest)) = config
        .roots
        .iter()
        .flat_map(|root| {
            root.spellings.iter().filter_map(move |spelling| {
                (parts.len() >= spelling.len()
                    && spelling.iter().zip(parts).all(|(a, b)| a.as_os_str() == *b))
                .then(|| (root, &parts[spelling.len()..]))
            })
        })
        .min_by_key(|(_, rest)| rest.len())
    else {
        return Err(refuse(
            "the path is not below a directory allowed for server-side COPY",
        ));
    };
    if rest.is_empty() {
        return Err(match access {
            Access::Read => copy_error("42809", format!("\"{raw}\" is a directory"), None, None),
            Access::Write => copy_error(
                "42809",
                format!("could not open file \"{raw}\" for writing: Is a directory"),
                None,
                None,
            ),
        });
    }
    open_beneath(&root.canonical, rest, raw, access, &refuse)
}

#[cfg(unix)]
fn open_beneath(
    root: &Path,
    rest: &[&OsStr],
    raw: &str,
    access: Access,
    refuse: &dyn Fn(&str) -> PgWireError,
) -> Result<File> {
    use std::ffi::CString;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::os::unix::ffi::OsStrExt;

    let verb = match access {
        Access::Read => "reading",
        Access::Write => "writing",
    };
    let open_error = |error: io::Error| {
        let hint =
            matches!(error.raw_os_error(), Some(libc::ENOENT | libc::EACCES)).then(
                || match access {
                    Access::Read => {
                        "COPY FROM instructs the PostgreSQL server process to read a file. \
                                 You may want a client-side facility such as psql's \\copy."
                            .to_string()
                    }
                    Access::Write => {
                        "COPY TO instructs the PostgreSQL server process to write a file. \
                                  You may want a client-side facility such as psql's \\copy."
                            .to_string()
                    }
                },
            );
        file_access_error(
            &error,
            format!(
                "could not open file \"{raw}\" for {verb}: {}",
                os_message(&error)
            ),
            hint,
        )
    };
    let c_name = |part: &OsStr| {
        CString::new(part.as_bytes()).map_err(|_| refuse("the path contains a NUL byte"))
    };
    // A failed O_NOFOLLOW open: say "symbolic link" when that is what it was.
    let is_symlink = |dir: &OwnedFd, name: &CString| -> bool {
        // SAFETY: plain fstatat into a zeroed stat buffer.
        unsafe {
            let mut stat: libc::stat = std::mem::zeroed();
            libc::fstatat(
                dir.as_raw_fd(),
                name.as_ptr(),
                &mut stat,
                libc::AT_SYMLINK_NOFOLLOW,
            ) == 0
                && (stat.st_mode & libc::S_IFMT) == libc::S_IFLNK
        }
    };
    let symlink_refusal = || refuse("symbolic links are not followed by server-side COPY");

    let root_name = CString::new(root.as_os_str().as_bytes())
        .map_err(|_| refuse("the path contains a NUL byte"))?;
    // SAFETY: open(2) on a NUL-terminated path; the result is checked.
    let fd = unsafe {
        libc::open(
            root_name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(open_error(io::Error::last_os_error()));
    }
    // SAFETY: fd is a freshly opened descriptor we own.
    let mut dir = unsafe { OwnedFd::from_raw_fd(fd) };
    let (last, intermediate) = rest.split_last().expect("non-empty path below the root");
    for part in intermediate {
        let name = c_name(part)?;
        // SAFETY: openat(2) relative to a directory descriptor we own.
        let fd = unsafe {
            libc::openat(
                dir.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            let error = io::Error::last_os_error();
            if is_symlink(&dir, &name) {
                return Err(symlink_refusal());
            }
            return Err(open_error(error));
        }
        // SAFETY: fd is a freshly opened descriptor we own.
        dir = unsafe { OwnedFd::from_raw_fd(fd) };
    }
    let name = c_name(last)?;
    let flags = libc::O_NOFOLLOW
        | libc::O_CLOEXEC
        | libc::O_NONBLOCK
        | match access {
            Access::Read => libc::O_RDONLY,
            Access::Write => libc::O_WRONLY | libc::O_CREAT,
        };
    // SAFETY: openat(2) relative to a directory descriptor we own.
    let fd = unsafe { libc::openat(dir.as_raw_fd(), name.as_ptr(), flags, 0o644 as libc::c_uint) };
    if fd < 0 {
        let error = io::Error::last_os_error();
        if is_symlink(&dir, &name) {
            return Err(symlink_refusal());
        }
        return Err(open_error(error));
    }
    // SAFETY: fd is a freshly opened descriptor we own.
    let file = unsafe { File::from_raw_fd(fd) };
    let metadata = file.metadata().map_err(open_error)?;
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.is_dir() {
            return Err(copy_error(
                "42809",
                format!("\"{raw}\" is a directory"),
                None,
                None,
            ));
        }
        if !metadata.is_file() {
            return Err(copy_error(
                "42809",
                format!("\"{raw}\" is not a regular file"),
                None,
                None,
            ));
        }
        if metadata.nlink() > 1 {
            return Err(refuse("the file has more than one hard link"));
        }
    }
    // Back to blocking I/O; O_NONBLOCK only kept a FIFO from blocking the open.
    // SAFETY: fcntl(2) on a descriptor we own.
    unsafe {
        let current = libc::fcntl(file.as_raw_fd(), libc::F_GETFL);
        if current >= 0 {
            libc::fcntl(file.as_raw_fd(), libc::F_SETFL, current & !libc::O_NONBLOCK);
        }
    }
    if access == Access::Write {
        file.set_len(0).map_err(open_error)?;
    }
    Ok(file)
}

#[cfg(not(unix))]
fn open_beneath(
    _root: &Path,
    _rest: &[&OsStr],
    _raw: &str,
    _access: Access,
    _refuse: &dyn Fn(&str) -> PgWireError,
) -> Result<File> {
    Err(copy_error(
        "0A000",
        "server-side COPY to or from a file is not supported on this platform",
        None,
        None,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_string_literals() {
        assert_eq!(
            parse_copy_string_literal("'/tmp/a''b' csv"),
            Some(("/tmp/a'b".to_string(), " csv"))
        );
        assert_eq!(
            parse_copy_string_literal("E'/tmp/a\\'b\\\\c'"),
            Some(("/tmp/a'b\\c".to_string(), ""))
        );
        assert_eq!(
            parse_copy_string_literal("$q$echo 'x'$q$ (format csv)"),
            Some(("echo 'x'".to_string(), " (format csv)"))
        );
        assert_eq!(parse_copy_string_literal("'unterminated"), None);
        assert_eq!(parse_copy_string_literal("stdin"), None);
    }

    #[test]
    fn copy_endpoints_and_options_exclude_the_literal() {
        let copy = parse_copy_statement("COPY t FROM '/tmp/binary/csv header.txt'")
            .unwrap()
            .unwrap();
        assert_eq!(
            copy.endpoint,
            CopyEndpoint::File("/tmp/binary/csv header.txt".to_string())
        );
        assert!(!copy.binary && !copy.header && copy.format == CopyFormat::Text);
        let copy =
            parse_copy_statement("copy t to program 'gzip > /x/y' with (format csv, header)")
                .unwrap()
                .unwrap();
        assert_eq!(
            copy.endpoint,
            CopyEndpoint::Program("gzip > /x/y".to_string())
        );
        assert!(copy.header && copy.format == CopyFormat::Csv);
        let copy = parse_copy_statement("COPY (SELECT 1) TO STDOUT")
            .unwrap()
            .unwrap();
        assert_eq!(copy.endpoint, CopyEndpoint::Client);
        let copy = parse_copy_statement("COPY t FROM STDOUT").unwrap().unwrap();
        assert_eq!(copy.endpoint, CopyEndpoint::Client);
        assert!(parse_copy_statement("COPY t FROM nowhere").is_err());
        // The pre-9.0 `COPY BINARY table` form; a table named `binary`.
        let copy = parse_copy_statement("COPY BINARY stud TO '/x'")
            .unwrap()
            .unwrap();
        assert!(copy.binary);
        assert_eq!(copy.relation.as_deref(), Some("stud"));
        let copy = parse_copy_statement("COPY binary FROM STDIN")
            .unwrap()
            .unwrap();
        assert_eq!(copy.relation.as_deref(), Some("binary"));
        assert!(!copy.binary);
        // A table named `program` is still a table.
        let copy = parse_copy_statement("COPY program FROM STDIN")
            .unwrap()
            .unwrap();
        assert_eq!(copy.relation.as_deref(), Some("program"));
    }

    #[test]
    fn exit_status_texts_match_postgresql() {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(
            wait_result_text(ExitStatus::from_raw(3 << 8)),
            "child process exited with exit code 3"
        );
        assert_eq!(
            wait_result_text(ExitStatus::from_raw(127 << 8)),
            "command not found"
        );
        assert!(wait_result_text(ExitStatus::from_raw(9))
            .starts_with("child process was terminated by signal 9: "));
    }

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "bicdb-server-copy-{name}-{}-{}",
            std::process::id(),
            unix_timestamp()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn sqlstate(error: PgWireError) -> String {
        match error {
            PgWireError::Sql(error) => error.sqlstate().to_string(),
            other => panic!("unexpected error {other:?}"),
        }
    }

    #[test]
    fn open_server_file_confines_paths() {
        let base = scratch("confine");
        let root = base.join("allowed");
        let outside = base.join("outside");
        fs::create_dir_all(root.join("sub")).unwrap();
        fs::create_dir_all(&outside).unwrap();
        fs::write(outside.join("secret"), "secret").unwrap();
        fs::write(root.join("sub/data.txt"), "1\n").unwrap();
        std::os::unix::fs::symlink(outside.join("secret"), root.join("link")).unwrap();
        std::os::unix::fs::symlink(&outside, root.join("dirlink")).unwrap();
        fs::hard_link(outside.join("secret"), root.join("hard")).unwrap();
        let config = ServerCopyConfig::new(&[root.clone()], false).unwrap();
        let p = |rel: &str| format!("{}/{rel}", root.display());

        let mut text = String::new();
        open_server_file(&config, &p("sub/data.txt"), Access::Read)
            .unwrap()
            .read_to_string(&mut text)
            .unwrap();
        assert_eq!(text, "1\n");
        // `..` escapes, even ones that would land back inside.
        assert_eq!(
            sqlstate(
                open_server_file(&config, &p("sub/../sub/data.txt"), Access::Read).unwrap_err()
            ),
            "42501"
        );
        assert_eq!(
            sqlstate(open_server_file(&config, &p("../outside/secret"), Access::Read).unwrap_err()),
            "42501"
        );
        // Absolute paths outside every allowed directory.
        let secret = format!("{}", outside.join("secret").display());
        assert_eq!(
            sqlstate(open_server_file(&config, &secret, Access::Read).unwrap_err()),
            "42501"
        );
        assert_eq!(
            sqlstate(open_server_file(&config, "/etc/passwd", Access::Read).unwrap_err()),
            "42501"
        );
        // Symbolic links: a file link, a directory link, a dangling write target.
        for target in ["link", "dirlink/secret"] {
            let error = open_server_file(&config, &p(target), Access::Read).unwrap_err();
            assert!(
                format!("{error}").contains("symbolic link"),
                "{target}: {error}"
            );
            assert_eq!(sqlstate(error), "42501");
        }
        let error = open_server_file(&config, &p("link"), Access::Write).unwrap_err();
        assert_eq!(sqlstate(error), "42501");
        assert_eq!(
            fs::read_to_string(outside.join("secret")).unwrap(),
            "secret"
        );
        // A second hard link to a file outside.
        assert_eq!(
            sqlstate(open_server_file(&config, &p("hard"), Access::Write).unwrap_err()),
            "42501"
        );
        assert_eq!(
            fs::read_to_string(outside.join("secret")).unwrap(),
            "secret"
        );
        // Relative paths, missing files, directories.
        assert_eq!(
            sqlstate(open_server_file(&config, "rel.txt", Access::Write).unwrap_err()),
            "42602"
        );
        assert_eq!(
            sqlstate(open_server_file(&config, "rel.txt", Access::Read).unwrap_err()),
            "42501"
        );
        assert_eq!(
            sqlstate(open_server_file(&config, &p("missing"), Access::Read).unwrap_err()),
            "58P01"
        );
        assert_eq!(
            sqlstate(open_server_file(&config, &p("sub"), Access::Read).unwrap_err()),
            "42809"
        );
        assert_eq!(
            sqlstate(open_server_file(&config, &p("sub"), Access::Write).unwrap_err()),
            "42809"
        );
        // Writing creates, then truncates.
        open_server_file(&config, &p("sub/out.txt"), Access::Write)
            .unwrap()
            .write_all(b"long contents")
            .unwrap();
        open_server_file(&config, &p("sub/out.txt"), Access::Write)
            .unwrap()
            .write_all(b"x")
            .unwrap();
        assert_eq!(fs::read_to_string(root.join("sub/out.txt")).unwrap(), "x");
        // No file COPY at all without a directory.
        assert!(ServerCopyConfig::default().file_dirs().is_empty());
        assert!(ServerCopyConfig::new(&[PathBuf::from("relative")], false).is_err());
        let _ = fs::remove_dir_all(&base);
    }
}
