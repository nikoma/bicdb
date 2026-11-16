//! Atomic no-clobber directory rename. Unsupported kernels/filesystems return
//! their original error; no check-then-replacing-rename fallback is safe here.
use super::*;

#[cfg(any(target_os = "linux", target_vendor = "apple"))]
pub(super) fn rename(from: &Path, to: &Path) -> Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let from = CString::new(from.as_os_str().as_bytes())
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    let to = CString::new(to.as_os_str().as_bytes())
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    #[cfg(target_os = "linux")]
    // Direct kernel entry also supports musl/older glibc without a renameat2
    // symbol-version dependency. Both CString buffers remain live; descriptors
    // name the caller's paths and flags forbid replacing ANY existing target.
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD as libc::c_long,
            from.as_ptr(),
            libc::AT_FDCWD as libc::c_long,
            to.as_ptr(),
            libc::RENAME_NOREPLACE as libc::c_long,
        )
    };
    #[cfg(target_vendor = "apple")]
    // Native Darwin RENAME_EXCL is the atomic destination-absence condition.
    let result = unsafe { libc::renamex_np(from.as_ptr(), to.as_ptr(), libc::RENAME_EXCL) };
    if result != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

#[cfg(windows)]
pub(super) fn rename(from: &Path, to: &Path) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;
    fn path(path: &Path) -> std::io::Result<Vec<u16>> {
        let mut value = path.as_os_str().encode_wide().collect::<Vec<_>>();
        if value.contains(&0) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "rename path contains NUL",
            ));
        }
        value.push(0);
        Ok(value)
    }
    #[link(name = "kernel32")]
    extern "system" {
        fn MoveFileExW(existing: *const u16, destination: *const u16, flags: u32) -> i32;
    }
    let from = path(from)?;
    let to = path(to)?;
    // Zero forbids both REPLACE_EXISTING and COPY_ALLOWED. Same-volume native
    // directory rename either succeeds or retains its original refusal.
    if unsafe { MoveFileExW(from.as_ptr(), to.as_ptr(), 0) } == 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_vendor = "apple", windows)))]
pub(super) fn rename(_from: &Path, _to: &Path) -> Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "bootstrap needs a native atomic no-replace directory rename on this platform",
    )
    .into())
}
