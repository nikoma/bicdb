//! Exclusive ownership of a page-store directory.
//!
//! # Why this is not optional
//!
//! A [`crate::PagedStore`] holds authoritative state in memory between
//! checkpoints: dirty pages in its buffer pool, the WAL region it intends to
//! truncate, and a meta page recording tree roots and transaction watermarks.
//! Two stores over one directory each believe they own all three. Neither
//! observes the other's writes, and the last one to flush overwrites the
//! other's — so a transaction that returned `Ok` from `commit` can vanish.
//!
//! No amount of care inside the engine fixes that; the only fix is to refuse the
//! second open. This is the same reason PostgreSQL keeps `postmaster.pid` and
//! why SQLite locks its database file.
//!
//! # Why an OS lock rather than a lock file
//!
//! The obvious implementation — create a file, refuse if it exists, delete it on
//! close — breaks the property that makes this engine worth having. A process
//! killed with `SIGKILL` cannot delete anything, so its lock file survives, and
//! every crash would then demand manual intervention before the database could
//! reopen. Crash recovery that requires an operator is not crash recovery.
//!
//! On Unix, a process record lock provides the cross-process claim and is
//! released by the kernel when the process exits *however* it exits. Unlike
//! `flock`, a process record lock is not inherited across `fork()`. That matters
//! in a multi-threaded server or test runner: a concurrently spawned child must
//! not keep a database locked after its owning handle has completed `close()`.
//! An inode-keyed process registry supplies the same-process exclusion that
//! process record locks deliberately do not provide. `tests/kill_recovery.rs`
//! exercises crash release, while this module's fork regression exercises the
//! child-inheritance boundary.
//!
//! # Windows
//!
//! Windows gets the same two guarantees from the open itself: the lock file is
//! requested with `share_mode(0)`, so the kernel refuses every other handle to
//! it and drops the claim when the process exits. See `open_lock_file`.

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::collections::HashSet;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(unix)]
use std::sync::{Mutex, OnceLock};

use crate::error::{PageError, Result};

/// Name of the lock file inside a store directory.
const LOCK_FILE: &str = "store.lock";

/// An exclusive claim on one page-store directory, released on drop (or on
/// process exit, by the kernel).
#[derive(Debug)]
pub struct DirectoryLock {
    path: PathBuf,
    /// Held open for as long as the lock is held. Closing the descriptor
    /// releases the kernel claim on Unix and the exclusive share on Windows.
    file: Option<File>,
    #[cfg(unix)]
    identity: DirectoryIdentity,
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct DirectoryIdentity {
    device: u64,
    inode: u64,
}

#[cfg(unix)]
fn process_locks() -> &'static Mutex<HashSet<DirectoryIdentity>> {
    static LOCKS: OnceLock<Mutex<HashSet<DirectoryIdentity>>> = OnceLock::new();
    LOCKS.get_or_init(|| Mutex::new(HashSet::new()))
}

#[cfg(unix)]
fn process_locks_guard() -> std::sync::MutexGuard<'static, HashSet<DirectoryIdentity>> {
    process_locks()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl DirectoryLock {
    /// Take the lock for `dir`, or fail if another store already holds it.
    ///
    /// Never blocks. Waiting would turn "you have a bug" into "your process
    /// hangs", and there is no case where waiting for another writer to finish
    /// is what the caller wanted.
    pub fn acquire(dir: &Path) -> Result<Self> {
        let path = dir.join(LOCK_FILE);

        #[cfg(unix)]
        let identity = {
            // Reserve by directory inode before opening store.lock. Traditional
            // process record locks are released when this process closes *any*
            // descriptor for the locked inode, so opening the file before the
            // same-process check would let a refused second opener accidentally
            // release the incumbent's cross-process claim when its File drops.
            let metadata = std::fs::metadata(dir).map_err(|source| PageError::io(dir, source))?;
            let identity = DirectoryIdentity {
                device: metadata.dev(),
                inode: metadata.ino(),
            };
            if !process_locks_guard().insert(identity) {
                return Err(PageError::AlreadyOpen { path });
            }
            identity
        };

        let file = match open_lock_file(&path) {
            Ok(file) => file,
            Err(error) => {
                #[cfg(unix)]
                process_locks_guard().remove(&identity);
                return Err(error);
            }
        };
        if let Err(error) = try_lock_exclusive(&file, &path) {
            #[cfg(unix)]
            process_locks_guard().remove(&identity);
            return Err(error);
        }
        Ok(Self {
            path,
            file: Some(file),
            #[cfg(unix)]
            identity,
        })
    }

    /// Path of the lock file, for diagnostics.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(unix)]
impl Drop for DirectoryLock {
    fn drop(&mut self) {
        // Close first, then publish the in-process release. Reversing this order
        // lets another thread acquire a process record lock that closing our old
        // descriptor would immediately and silently release.
        drop(self.file.take());
        process_locks_guard().remove(&self.identity);
    }
}

#[cfg(unix)]
fn try_lock_exclusive(file: &File, path: &Path) -> Result<()> {
    use std::os::unix::io::AsRawFd;

    // SAFETY: `libc::flock` is a C POD structure; all-zero is the valid
    // unlocked/default state on supported Unix targets. Assigning only the
    // POSIX fields also keeps this portable to targets whose libc adds fields.
    let mut lock: libc::flock = unsafe { std::mem::zeroed() };
    lock.l_type = libc::F_WRLCK as _;
    lock.l_whence = libc::SEEK_SET as _;
    lock.l_start = 0;
    lock.l_len = 0;
    // SAFETY: `file` remains open for the call and `lock` points to a fully
    // initialized write-lock description. F_SETLK is deliberately nonblocking.
    let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETLK, &mut lock) };
    if rc == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    // POSIX permits either EACCES or EAGAIN for a conflicting record lock.
    if matches!(
        error.raw_os_error(),
        Some(libc::EACCES) | Some(libc::EAGAIN)
    ) {
        return Err(PageError::AlreadyOpen {
            path: path.to_path_buf(),
        });
    }
    Err(PageError::io(path, error))
}

/// Windows takes the lock in `open` itself, by requesting the file with no
/// sharing: the kernel then refuses any other handle to it. That gives the same
/// two properties `flock` gives on unix — no second owner, and the claim is
/// released when the process dies however it dies — without a third-party
/// crate. `LockFileEx` would work too; exclusive sharing is the same guarantee
/// with less surface.
#[cfg(windows)]
fn try_lock_exclusive(_file: &File, _path: &Path) -> Result<()> {
    Ok(())
}

/// Open the lock file, requesting exclusive access on Windows.
#[cfg(windows)]
fn open_lock_file(path: &Path) -> Result<File> {
    use std::os::windows::fs::OpenOptionsExt;

    // share_mode(0): no other process may open this file for read, write, or
    // delete while this handle lives. A second opener fails with
    // ERROR_SHARING_VIOLATION, which is this crate's `AlreadyOpen`.
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .share_mode(0)
        .open(path)
        .map_err(|source| {
            // 32 = ERROR_SHARING_VIOLATION, 33 = ERROR_LOCK_VIOLATION.
            if matches!(source.raw_os_error(), Some(32) | Some(33)) {
                PageError::AlreadyOpen {
                    path: path.to_path_buf(),
                }
            } else {
                PageError::io(path, source)
            }
        })
}

#[cfg(all(test, unix))]
mod tests {
    use super::DirectoryLock;
    use std::os::unix::io::RawFd;

    fn pipe() -> [RawFd; 2] {
        let mut descriptors = [-1; 2];
        // SAFETY: `descriptors` has room for exactly the two fds pipe writes.
        assert_eq!(unsafe { libc::pipe(descriptors.as_mut_ptr()) }, 0);
        descriptors
    }

    fn close(fd: RawFd) {
        // SAFETY: every caller owns the descriptor in this process and closes it
        // at most once. Errors during test cleanup cannot affect lock semantics.
        let _ = unsafe { libc::close(fd) };
    }

    #[test]
    fn a_forked_child_does_not_extend_the_parent_lock_lifetime() {
        let dir = tempfile::tempdir().unwrap();
        let lock = DirectoryLock::acquire(dir.path()).unwrap();
        let ready = pipe();
        let release = pipe();

        // SAFETY: the child performs only async-signal-safe libc syscalls before
        // `_exit`; it never allocates, locks a Rust mutex, or runs destructors.
        let child = unsafe { libc::fork() };
        assert!(
            child >= 0,
            "fork failed: {}",
            std::io::Error::last_os_error()
        );
        if child == 0 {
            close(ready[0]);
            close(release[1]);
            let marker = [1u8];
            // SAFETY: the fds and one-byte buffers remain valid for each call.
            unsafe {
                libc::write(ready[1], marker.as_ptr().cast(), marker.len());
                let mut signal = [0u8];
                libc::read(release[0], signal.as_mut_ptr().cast(), signal.len());
                libc::_exit(0);
            }
        }

        close(ready[1]);
        close(release[0]);
        let mut marker = [0u8];
        // SAFETY: the child owns the other pipe end and the buffer is writable.
        assert_eq!(
            unsafe { libc::read(ready[0], marker.as_mut_ptr().cast(), marker.len()) },
            1
        );
        assert_eq!(marker, [1]);

        drop(lock);
        let reopened = DirectoryLock::acquire(dir.path())
            .expect("a forked child must not retain the parent's released lock");
        drop(reopened);

        // SAFETY: the parent owns these pipe ends and `child` is its live child.
        assert_eq!(
            unsafe { libc::write(release[1], marker.as_ptr().cast(), marker.len()) },
            1
        );
        let mut status = 0;
        assert_eq!(unsafe { libc::waitpid(child, &mut status, 0) }, child);
        assert!(libc::WIFEXITED(status));
        assert_eq!(libc::WEXITSTATUS(status), 0);
        close(ready[0]);
        close(release[1]);
    }
}

#[cfg(not(windows))]
fn open_lock_file(path: &Path) -> Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(|source| PageError::io(path, source))
}

/// A platform with neither `flock` nor Windows sharing modes cannot enforce
/// single ownership, and without it a second opener silently discards this
/// one's committed transactions. Refusing to open is the safe answer.
#[cfg(not(any(unix, windows)))]
fn try_lock_exclusive(_file: &File, path: &Path) -> Result<()> {
    Err(PageError::io(
        path,
        std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "exclusive store locking is implemented for unix and windows only",
        ),
    ))
}
