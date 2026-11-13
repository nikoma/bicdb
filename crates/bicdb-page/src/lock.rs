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

/// OS-observed physical directory birth data, not database or peer authority.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct DirectoryPhysicalBirth {
    device: u64,
    inode: u64,
    created_seconds: u64,
    created_nanos: u32,
}

/// Read-only refusal preflight. A positive observation acquires no claim.
pub fn physical_directory_birth(dir: &Path) -> Result<Option<DirectoryPhysicalBirth>> {
    #[cfg(unix)]
    {
        let metadata = std::fs::metadata(dir).map_err(|e| PageError::io(dir, e))?;
        let created = metadata
            .created()
            .map_err(|e| PageError::io(dir, e))?
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| {
                PageError::io(dir, std::io::Error::new(std::io::ErrorKind::InvalidData, e))
            })?;
        Ok(Some(DirectoryPhysicalBirth {
            device: metadata.dev(),
            inode: metadata.ino(),
            created_seconds: created.as_secs(),
            created_nanos: created.subsec_nanos(),
        }))
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(None)
    }
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

    /// Identity of this actual claimed directory. Refuse path replacement
    /// while the old directory's lock remains held.
    pub fn physical_birth(&self) -> Result<Option<DirectoryPhysicalBirth>> {
        let dir = self
            .path
            .parent()
            .expect("store lock has a parent directory");
        #[cfg(unix)]
        {
            let metadata = std::fs::metadata(dir).map_err(|e| PageError::io(dir, e))?;
            if metadata.dev() != self.identity.device || metadata.ino() != self.identity.inode {
                return Err(PageError::io(
                    dir,
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "claimed directory was replaced",
                    ),
                ));
            }
        }
        physical_directory_birth(dir)
    }
}

/// Whether some handle currently owns a store directory, observed without
/// taking, creating or modifying anything. Used by the read-only store
/// verifier, which must refuse to read a store a live writer is changing
/// but must never create `store.lock` itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum DirectoryLockProbe {
    /// There is no `store.lock` file (the store was never opened by an engine
    /// that locks, or the file was removed). Nothing can hold it.
    NoLockFile,
    /// The lock file exists and no process holds its record lock.
    Unlocked,
    /// Another process holds the record lock.
    HeldByProcess { pid: i64 },
    /// A handle in THIS process owns the directory.
    HeldInThisProcess,
    /// The platform cannot test the lock without taking it.
    Unknown,
}

impl DirectoryLockProbe {
    pub fn is_held(&self) -> bool {
        matches!(self, Self::HeldByProcess { .. } | Self::HeldInThisProcess)
    }
}

/// Test the directory lock without acquiring it. Opens the lock file
/// read-only (never creating it) and asks the kernel with `F_GETLK`, which
/// reports a conflicting lock and changes nothing.
#[cfg(unix)]
pub fn probe_directory_lock(dir: &Path) -> Result<DirectoryLockProbe> {
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::io::AsRawFd;

    let metadata = std::fs::metadata(dir).map_err(|source| PageError::io(dir, source))?;
    let identity = DirectoryIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    };
    // Keep acquisition in this process excluded until the probe descriptor has
    // closed. Closing *any* descriptor for an inode releases this process's
    // POSIX locks on that inode. A check followed by an unlocked open could
    // therefore drop a writer's lock if it acquired ownership in between.
    let process_guard = process_locks_guard();
    if process_guard.contains(&identity) {
        return Ok(DirectoryLockProbe::HeldInThisProcess);
    }
    let path = dir.join(LOCK_FILE);
    match std::fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.nlink() > 1 => return Err(PageError::io(&path,
            std::io::Error::new(std::io::ErrorKind::InvalidInput,
                "refusing a hard-linked store lock: probing it could release another store's POSIX lock"))),
        Ok(_) => {},
        Err(error) if error.kind() == std::io::ErrorKind::NotFound =>
            return Ok(DirectoryLockProbe::NoLockFile),
        Err(error) => return Err(PageError::io(&path, error)),
    }
    let file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(&path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(DirectoryLockProbe::NoLockFile)
        }
        Err(error) => return Err(PageError::io(&path, error)),
    };
    if !file
        .metadata()
        .map_err(|error| PageError::io(&path, error))?
        .is_file()
    {
        return Err(PageError::io(
            &path,
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "store lock is not a regular file",
            ),
        ));
    }
    // SAFETY: see `try_lock_exclusive`; all-zero is a valid `flock`.
    let mut lock: libc::flock = unsafe { std::mem::zeroed() };
    lock.l_type = libc::F_WRLCK as _;
    lock.l_whence = libc::SEEK_SET as _;
    lock.l_start = 0;
    lock.l_len = 0;
    // SAFETY: `file` is open for the call and `lock` is initialized. F_GETLK
    // only reports; it never places or releases a lock.
    let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETLK, &mut lock) };
    if rc != 0 {
        return Err(PageError::io(&path, std::io::Error::last_os_error()));
    }
    if i32::from(lock.l_type) == libc::F_UNLCK {
        Ok(DirectoryLockProbe::Unlocked)
    } else {
        Ok(DirectoryLockProbe::HeldByProcess {
            pid: i64::from(lock.l_pid),
        })
    }
}

#[cfg(not(unix))]
pub fn probe_directory_lock(_dir: &Path) -> Result<DirectoryLockProbe> {
    Ok(DirectoryLockProbe::Unknown)
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
    use std::os::unix::io::AsRawFd;
    use std::os::unix::io::RawFd;
    use std::sync::atomic::{AtomicBool, Ordering};

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

    fn assert_kernel_exclusion(fd: RawFd) {
        // SAFETY: the child performs only async-signal-safe libc operations.
        let child = unsafe { libc::fork() };
        assert!(child >= 0);
        if child == 0 {
            let mut lock: libc::flock = unsafe { std::mem::zeroed() };
            lock.l_type = libc::F_WRLCK as _;
            lock.l_whence = libc::SEEK_SET as _;
            let rc = unsafe { libc::fcntl(fd, libc::F_SETLK, &mut lock) };
            unsafe { libc::_exit(if rc == -1 { 0 } else { 1 }) };
        }
        let mut status = 0;
        // SAFETY: child belongs to this process and status is writable.
        assert_eq!(unsafe { libc::waitpid(child, &mut status, 0) }, child);
        assert!(
            libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
            "read-only verification released another store's OS lock"
        );
    }

    #[test]
    fn hard_linked_inputs_cannot_release_another_store_lock() {
        for name in ["store.lock", "store.pages"] {
            let owner_dir = tempfile::tempdir().unwrap();
            let copy_dir = tempfile::tempdir().unwrap();
            let owner = DirectoryLock::acquire(owner_dir.path()).unwrap();
            std::fs::hard_link(
                owner_dir.path().join("store.lock"),
                copy_dir.path().join(name),
            )
            .unwrap();
            assert!(crate::verify_store_read_only(
                copy_dir.path(),
                &crate::StoreVerifyOptions::default()
            )
            .is_err());
            assert_kernel_exclusion(owner.file.as_ref().unwrap().as_raw_fd());
        }
    }

    #[test]
    fn concurrent_read_only_probes_preserve_the_writers_cross_process_lock() {
        let dir = tempfile::tempdir().unwrap();
        // Ensure probes can open an existing lock file between ownerships.
        drop(DirectoryLock::acquire(dir.path()).unwrap());
        let stop = AtomicBool::new(false);
        std::thread::scope(|scope| {
            struct StopOnDrop<'a>(&'a AtomicBool);
            impl Drop for StopOnDrop<'_> {
                fn drop(&mut self) {
                    self.0.store(true, Ordering::Relaxed);
                }
            }
            let _stop_on_exit = StopOnDrop(&stop);
            for _ in 0..4 {
                scope.spawn(|| {
                    while !stop.load(Ordering::Relaxed) {
                        super::probe_directory_lock(dir.path()).unwrap();
                    }
                });
            }
            for _ in 0..200 {
                let owner = DirectoryLock::acquire(dir.path()).unwrap();
                let fd = owner.file.as_ref().unwrap().as_raw_fd();
                // SAFETY: the child only calls async-signal-safe libc functions
                // and never allocates or acquires a Rust lock after fork.
                let child = unsafe { libc::fork() };
                assert!(child >= 0);
                if child == 0 {
                    let mut lock: libc::flock = unsafe { std::mem::zeroed() };
                    lock.l_type = libc::F_WRLCK as _;
                    lock.l_whence = libc::SEEK_SET as _;
                    let rc = unsafe { libc::fcntl(fd, libc::F_SETLK, &mut lock) };
                    unsafe { libc::_exit(if rc == -1 { 0 } else { 1 }) };
                }
                let mut status = 0;
                // SAFETY: child is this process's child and status is writable.
                assert_eq!(unsafe { libc::waitpid(child, &mut status, 0) }, child);
                if !libc::WIFEXITED(status) || libc::WEXITSTATUS(status) != 0 {
                    stop.store(true, Ordering::Relaxed);
                    panic!("a read-only probe released the writer's OS lock");
                }
                drop(owner);
            }
            stop.store(true, Ordering::Relaxed);
        });
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
