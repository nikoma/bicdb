//! One physical-file cut shared by Core and Page.
//!
//! This is filesystem exclusion, not transaction or replication authority.
//! Every scope is acquired before the locks used by the operation it protects.
//! Reentrancy retains the very same locked descriptor; a path or a boolean is
//! never sufficient to skip a kernel lock.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::rc::{Rc, Weak};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::{PageError, Result};

#[cfg(feature = "physical-gate-observation")]
#[doc(hidden)]
pub mod observation;

pub const PHYSICAL_GATE_FILE: &str = ".backup-write.lock";
pub const PHYSICAL_GATE_READER_FEATURE: &str = "physical_backup_gate_v1";
const WAIT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Identity {
    volume: u64,
    file: u64,
}

#[derive(Debug)]
struct LockedFile {
    file: File,
    path: PathBuf,
    identity: Identity,
    exclusive: bool,
    _binding: Arc<GateBinding>,
    // Last: the real File closes before liveness becomes false, including
    // a Send/Sync outer EX dropped on a different thread.
    #[cfg(feature = "physical-gate-observation")]
    _observation: Option<observation::GuardLifetime>,
}

thread_local! {
    static HELD: RefCell<Vec<Weak<LockedFile>>> = const { RefCell::new(Vec::new()) };
}

/// The shared scope is deliberately !Send: only this thread may reenter it.
#[derive(Debug)]
pub struct PhysicalWriteGuard {
    locked: Rc<LockedFile>,
}

/// The outer backup may move between threads. It registers no thread-local
/// permission until an explicit lexical borrow is made on the current thread.
#[derive(Debug)]
pub struct PhysicalBackupGuard {
    locked: Arc<LockedFile>,
}

#[derive(Debug)]
pub struct PhysicalBackupBorrow<'a> {
    _guard: PhysicalWriteGuard,
    _owner: PhantomData<&'a PhysicalBackupGuard>,
}

/// A stable gate identity bound at open, rather than rediscovered after a file
/// was removed or replaced. Standalone Page stores use their own directory;
/// Core initializes the enclosing root gate before opening Page.
#[derive(Clone, Debug)]
pub struct PhysicalGate {
    binding: Arc<GateBinding>,
}

#[derive(Debug)]
struct GateBinding {
    path: PathBuf,
    identity: Identity,
}

fn bindings() -> &'static Mutex<HashMap<PathBuf, std::sync::Weak<GateBinding>>> {
    static BINDINGS: OnceLock<Mutex<HashMap<PathBuf, std::sync::Weak<GateBinding>>>> =
        OnceLock::new();
    BINDINGS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn bound(path: &Path) -> Option<Arc<GateBinding>> {
    bindings()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(path)
        .and_then(std::sync::Weak::upgrade)
}

fn failure(path: &Path, detail: impl std::fmt::Display) -> PageError {
    PageError::BackupSnapshot {
        reason: format!("physical I/O gate {}: {detail}", path.display()),
    }
}

impl PhysicalGate {
    /// Retain an already-bound actual identity for denial/admission checks.
    /// This returns no kernel lock and cannot authorize physical IO.
    pub fn retained_at_root(root: &Path) -> Option<Self> {
        let path = std::fs::canonicalize(root)
            .or_else(|_| {
                if root.is_absolute() {
                    Ok(root.to_path_buf())
                } else {
                    std::env::current_dir().map(|current| current.join(root))
                }
            })
            .ok()?
            .join(PHYSICAL_GATE_FILE);
        bound(&path).map(|binding| Self { binding })
    }

    pub fn at_root(root: &Path) -> Result<Self> {
        Self::at_root_mode(root, true)
    }

    fn at_root_mode(root: &Path, create: bool) -> Result<Self> {
        let root = if root.as_os_str().is_empty() {
            Path::new(".")
        } else {
            root
        };
        std::fs::create_dir_all(root).map_err(|e| PageError::io(root, e))?;
        let root = std::fs::canonicalize(root).map_err(|e| PageError::io(root, e))?;
        let path = root.join(PHYSICAL_GATE_FILE);
        if let Some(binding) = bound(&path) {
            return Ok(Self { binding });
        }
        let observed = if let Some(locked) = HELD.with(|held| {
            held.borrow()
                .iter()
                .filter_map(Weak::upgrade)
                .find(|entry| entry.path == path)
        }) {
            identity(&locked.file, &path)?
        } else {
            identity(&open(&path, create)?, &path)?
        };
        let candidate = Arc::new(GateBinding {
            path: path.clone(),
            identity: observed,
        });
        // No physical IO under the process registry lock. Concurrent binders
        // still choose one actual inode identity before admitting an operation.
        let binding = {
            let mut registry = bindings()
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            registry.retain(|_, entry| entry.strong_count() != 0);
            if let Some(current) = registry.get(&path).and_then(std::sync::Weak::upgrade) {
                current
            } else {
                registry.insert(path, Arc::downgrade(&candidate));
                candidate
            }
        };
        Ok(Self { binding })
    }

    /// Resolve once, before any page/WAL/pool lock. Symlinks never enroll a
    /// different root. Subsequent operations validate this exact inode.
    pub fn for_file(path: &Path) -> Result<Self> {
        let parent = path
            .parent()
            .ok_or_else(|| failure(path, "file has no parent"))?;
        for root in parent.ancestors() {
            let metadata_path = root.join("format.json");
            match File::open(&metadata_path) {
                Ok(mut file) => {
                    use std::io::Read;
                    if file
                        .metadata()
                        .map_err(|e| PageError::io(&metadata_path, e))?
                        .len()
                        > 1024 * 1024
                    {
                        return Err(failure(
                            &metadata_path,
                            "Core format exceeds the physical-binding bound",
                        ));
                    }
                    let mut bytes = Vec::new();
                    file.take(1024 * 1024 + 1)
                        .read_to_end(&mut bytes)
                        .map_err(|e| PageError::io(&metadata_path, e))?;
                    if bytes.len() > 1024 * 1024 {
                        return Err(failure(
                            &metadata_path,
                            "Core format grew beyond the binding bound",
                        ));
                    }
                    let metadata: serde_json::Value =
                        serde_json::from_slice(&bytes).map_err(|e| failure(&metadata_path, e))?;
                    let enrolled = metadata
                        .get("feature_flags")
                        .and_then(|flags| flags.as_array())
                        .is_some_and(|flags| {
                            flags
                                .iter()
                                .any(|flag| flag.as_str() == Some(PHYSICAL_GATE_READER_FEATURE))
                        });
                    if enrolled {
                        let gate = root.join(PHYSICAL_GATE_FILE);
                        // Existing-only: a removed enrolled inode never becomes
                        // a newly created, unrelated coordination domain.
                        let _ = gate;
                        return Self::at_root_mode(root, false);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(PageError::io(metadata_path, e)),
            }
        }
        for root in parent.ancestors() {
            let candidate = root.join(PHYSICAL_GATE_FILE);
            match std::fs::symlink_metadata(&candidate) {
                Ok(_) => return Self::at_root_mode(root, false),
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(PageError::io(candidate, e)),
            }
        }
        Self::at_root(parent)
    }

    fn validate(&self) -> Result<()> {
        let file = open(&self.binding.path, false)?;
        if identity(&file, &self.binding.path)? != self.binding.identity {
            return Err(failure(&self.binding.path, "stable inode was replaced"));
        }
        Ok(())
    }

    pub fn shared(&self) -> Result<PhysicalWriteGuard> {
        if let Some(locked) = HELD.with(|held| {
            let mut held = held.borrow_mut();
            held.retain(|entry| entry.strong_count() != 0);
            held.iter()
                .filter_map(Weak::upgrade)
                .find(|entry| entry.identity == self.binding.identity)
        }) {
            #[cfg(not(windows))]
            self.validate()?;
            return Ok(PhysicalWriteGuard { locked });
        }
        let locked = Rc::new(self.acquire(false)?);
        HELD.with(|held| held.borrow_mut().push(Rc::downgrade(&locked)));
        Ok(PhysicalWriteGuard { locked })
    }

    pub fn exclusive(&self) -> Result<PhysicalBackupGuard> {
        // An implicit shared-to-exclusive upgrade could wait on this thread's
        // own descriptor. Refuse before blocking rather than drop its scope.
        let nested = HELD.with(|held| {
            held.borrow()
                .iter()
                .filter_map(Weak::upgrade)
                .any(|entry| entry.identity == self.binding.identity)
        });
        if nested {
            return Err(failure(
                &self.binding.path,
                "exclusive acquisition inside a physical scope",
            ));
        }
        Ok(PhysicalBackupGuard {
            locked: Arc::new(self.acquire(true)?),
        })
    }

    fn acquire(&self, exclusive: bool) -> Result<LockedFile> {
        let started = Instant::now();
        loop {
            if let Some(file) = try_acquire(&self.binding.path, exclusive)? {
                if identity(&file, &self.binding.path)? != self.binding.identity {
                    return Err(failure(&self.binding.path, "stable inode was replaced"));
                }
                // On Windows an EX share-mode handle intentionally refuses a
                // second open. The opened handle's identity is the validation.
                #[cfg(not(windows))]
                self.validate()?;
                #[cfg(feature = "physical-gate-observation")]
                let observed = observation::actual_guard(&self.binding.path, exclusive, &file);
                let locked = LockedFile {
                    file,
                    path: self.binding.path.clone(),
                    identity: self.binding.identity,
                    exclusive,
                    _binding: Arc::clone(&self.binding),
                    #[cfg(feature = "physical-gate-observation")]
                    _observation: observed,
                };
                #[cfg(feature = "physical-gate-observation")]
                if let Some(marker) = &locked._observation {
                    marker.observe_acquired();
                }
                return Ok(locked);
            }
            #[cfg(feature = "physical-gate-observation")]
            observation::observe_refused(&self.binding.path, exclusive);
            if started.elapsed() >= WAIT {
                return Err(failure(&self.binding.path, "timed out before physical I/O"));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

impl PhysicalBackupGuard {
    pub fn borrow(&self) -> Result<PhysicalBackupBorrow<'_>> {
        if !self.locked.exclusive {
            return Err(failure(
                Path::new(PHYSICAL_GATE_FILE),
                "borrow requires actual EX",
            ));
        }
        // A duplicated descriptor retains the SAME open file description and
        // kernel lock. It does not reacquire SH underneath our own EX.
        let locked = Rc::new(LockedFile {
            file: self
                .locked
                .file
                .try_clone()
                .map_err(|e| PageError::io(PHYSICAL_GATE_FILE, e))?,
            identity: self.locked.identity,
            path: self.locked.path.clone(),
            exclusive: true,
            _binding: Arc::clone(&self.locked._binding),
            // This is a duplicate of the actual EX descriptor, not a newly
            // acquired SH. The original marker continues to observe its owner.
            #[cfg(feature = "physical-gate-observation")]
            _observation: None,
        });
        HELD.with(|held| held.borrow_mut().push(Rc::downgrade(&locked)));
        Ok(PhysicalBackupBorrow {
            _guard: PhysicalWriteGuard { locked },
            _owner: PhantomData,
        })
    }
}

#[cfg(unix)]
fn identity(file: &File, path: &Path) -> Result<Identity> {
    use std::os::unix::fs::MetadataExt;
    let metadata = file.metadata().map_err(|e| PageError::io(path, e))?;
    Ok(Identity {
        volume: metadata.dev(),
        file: metadata.ino(),
    })
}

#[cfg(windows)]
fn identity(file: &File, path: &Path) -> Result<Identity> {
    use std::os::windows::io::AsRawHandle;
    #[repr(C)]
    struct Information {
        attributes: u32,
        created: [u32; 2],
        accessed: [u32; 2],
        written: [u32; 2],
        volume: u32,
        size_high: u32,
        size_low: u32,
        links: u32,
        index_high: u32,
        index_low: u32,
    }
    #[link(name = "kernel32")]
    extern "system" {
        fn GetFileInformationByHandle(
            handle: *mut std::ffi::c_void,
            information: *mut Information,
        ) -> i32;
    }
    let mut info = std::mem::MaybeUninit::<Information>::uninit();
    if unsafe { GetFileInformationByHandle(file.as_raw_handle(), info.as_mut_ptr()) } == 0 {
        return Err(PageError::io(path, io::Error::last_os_error()));
    }
    let info = unsafe { info.assume_init() };
    Ok(Identity {
        volume: u64::from(info.volume),
        file: (u64::from(info.index_high) << 32) | u64::from(info.index_low),
    })
}

#[cfg(not(any(unix, windows)))]
fn identity(_file: &File, path: &Path) -> Result<Identity> {
    // Do not turn an unsupported identity/locking implementation into a
    // successful physical backup. Native platform support is required here.
    Err(failure(
        path,
        "physical gate identity is unsupported on this platform",
    ))
}

fn open(path: &Path, create: bool) -> Result<File> {
    match std::fs::symlink_metadata(path) {
        Ok(m) if !m.is_file() || m.file_type().is_symlink() => {
            return Err(failure(path, "not a regular non-symlink file"))
        }
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(PageError::io(path, e)),
    }
    let mut options = OpenOptions::new();
    options
        .create(create)
        .truncate(false)
        .read(true)
        .write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW).mode(0o600);
    }
    let file = options.open(path).map_err(|e| PageError::io(path, e))?;
    if !file
        .metadata()
        .map_err(|e| PageError::io(path, e))?
        .is_file()
    {
        return Err(failure(path, "not a regular file"));
    }
    Ok(file)
}

#[cfg(unix)]
fn try_acquire(path: &Path, exclusive: bool) -> Result<Option<File>> {
    let file = open(path, false)?;
    use std::os::fd::AsRawFd;
    let operation = if exclusive {
        libc::LOCK_EX
    } else {
        libc::LOCK_SH
    };
    if unsafe { libc::flock(file.as_raw_fd(), operation | libc::LOCK_NB) } == 0 {
        return Ok(Some(file));
    }
    let error = io::Error::last_os_error();
    if matches!(
        error.raw_os_error(),
        Some(libc::EWOULDBLOCK) | Some(libc::EAGAIN)
    ) {
        return Ok(None);
    }
    Err(PageError::io(path, error))
}

#[cfg(windows)]
fn try_acquire(path: &Path, exclusive: bool) -> Result<Option<File>> {
    use std::os::windows::fs::OpenOptionsExt;
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    if exclusive {
        options.share_mode(0);
    }
    match options.open(path) {
        Ok(file) => Ok(Some(file)),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::PermissionDenied | io::ErrorKind::WouldBlock
            ) =>
        {
            Ok(None)
        }
        Err(error) => Err(PageError::io(path, error)),
    }
}

#[cfg(not(any(unix, windows)))]
fn try_acquire(path: &Path, _exclusive: bool) -> Result<Option<File>> {
    Err(failure(
        path,
        "physical gate locking is unsupported on this platform",
    ))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn nested_shared_retains_the_actual_lock_until_the_last_scope_drops() {
        let directory = tempfile::tempdir().unwrap();
        let gate = PhysicalGate::at_root(directory.path()).unwrap();
        let outer = gate.shared().unwrap();
        let inner = gate.shared().unwrap();
        assert!(Rc::ptr_eq(&outer.locked, &inner.locked));
        assert!(try_acquire(&gate.binding.path, true).unwrap().is_none());
        drop(outer);
        assert!(try_acquire(&gate.binding.path, true).unwrap().is_none());
        drop(inner);
        assert!(try_acquire(&gate.binding.path, true).unwrap().is_some());
    }

    #[test]
    fn borrowed_ex_reenters_only_the_actual_locked_file_and_other_threads_stay_excluded() {
        fn require_send_sync<T: Send + Sync>() {}
        require_send_sync::<PhysicalBackupGuard>();
        let directory = tempfile::tempdir().unwrap();
        let gate = PhysicalGate::at_root(directory.path()).unwrap();
        let exclusive = gate.exclusive().unwrap();
        let borrowed = exclusive.borrow().unwrap();
        let nested = gate.shared().unwrap();
        assert!(nested.locked.exclusive);
        let observed = gate.clone();
        std::thread::spawn(move || {
            assert!(try_acquire(&observed.binding.path, false)
                .unwrap()
                .is_none());
            assert!(try_acquire(&observed.binding.path, true).unwrap().is_none());
        })
        .join()
        .unwrap();
        drop(nested);
        drop(borrowed);
        // Dropping the borrow cannot unlock its still-owned original EX.
        assert!(try_acquire(&gate.binding.path, false).unwrap().is_none());
        drop(exclusive);
        assert!(try_acquire(&gate.binding.path, false).unwrap().is_some());
    }

    #[test]
    fn retained_binding_refuses_replacement_even_when_the_old_file_is_still_locked() {
        let directory = tempfile::tempdir().unwrap();
        let gate = PhysicalGate::at_root(directory.path()).unwrap();
        let original = gate.shared().unwrap();
        std::fs::rename(&gate.binding.path, directory.path().join("original-inode")).unwrap();
        drop(open(&gate.binding.path, true).unwrap());
        let rebound = PhysicalGate::at_root(directory.path()).unwrap();
        assert_eq!(gate.binding.identity, rebound.binding.identity);
        assert!(gate.shared().is_err());
        assert!(rebound.shared().is_err());
        drop(original);
        assert!(gate.shared().is_err());
    }

    #[test]
    fn page_latches_retain_physical_scope_through_read_write_and_upgrade() {
        use crate::{BufferPool, BufferPoolOptions, PageStore, PageStoreOptions, PageType};
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            PageStore::open(
                directory.path().join("store.pages"),
                PageStoreOptions::default(),
            )
            .unwrap(),
        );
        let id = store.allocate(PageType::Heap).unwrap();
        let pool = BufferPool::new(Arc::clone(&store), BufferPoolOptions::default()).unwrap();
        let gate_path = store.physical_gate().binding.path.clone();
        let read = pool.get(id).unwrap();
        assert!(try_acquire(&gate_path, true).unwrap().is_none());
        drop(read);
        let upgradable = pool.get_upgradable(id).unwrap();
        assert!(try_acquire(&gate_path, true).unwrap().is_none());
        let write = upgradable.upgrade();
        assert!(try_acquire(&gate_path, true).unwrap().is_none());
        drop(write);
        assert_eq!(pool.dirty_page_count(), 1);
        assert!(try_acquire(&gate_path, true).unwrap().is_some());
    }
    #[test]
    fn actual_page_fence_precedes_recovery_and_raw_superblock_writes_cannot_erase_it() {
        use crate::{PageStore, PageStoreOptions, PAGE_HEADER_BYTES, SUPERBLOCK_PAGE_ID};
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("store.pages");
        let store = PageStore::open(&path, PageStoreOptions::default()).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let magic = &bytes[PAGE_HEADER_BYTES..PAGE_HEADER_BYTES + 8];
        let old_profiles = [*b"BICDBPG\0", *b"BICDBPG2", *b"BICDBPG3", *b"BICDBPG4"];
        assert!(!old_profiles.iter().any(|old| magic == old));
        let mut old_image = bytes[..store.page_size() as usize].to_vec();
        old_image[PAGE_HEADER_BYTES..PAGE_HEADER_BYTES + 8].copy_from_slice(&old_profiles[0]);
        store
            .write_page(SUPERBLOCK_PAGE_ID, &mut old_image)
            .unwrap();
        store.sync().unwrap();
        let after = std::fs::read(&path).unwrap();
        assert_eq!(&after[PAGE_HEADER_BYTES..PAGE_HEADER_BYTES + 8], magic);
        drop(store);
        PageStore::open_existing_auto(&path, true).unwrap();
        // Real bytes/normal new reader, with the explicit prior capability
        // set simulated above; no claim of running an old binary.
    }
}
