//! Positioned file I/O, across platforms.
//!
//! The page store addresses every byte by absolute offset and never by the
//! file's cursor — that is what lets many threads share one handle without
//! serializing on a seek. The standard library spells this differently per
//! platform, so the difference is isolated here rather than sprinkled through
//! the store.
//!
//! # The one semantic difference, and why it is safe
//!
//! Unix `pread`/`pwrite` leave the file cursor untouched. Windows
//! `seek_read`/`seek_write` **move** it. That would be a real hazard if any
//! code path mixed positioned and cursor-relative access on the same handle —
//! two threads would race for the cursor's value.
//!
//! It is safe here only because nothing in this crate ever reads the cursor:
//! [`PageStore`](crate::manager::PageStore) opens its handle and issues
//! exclusively positioned reads and writes. **Adding a cursor-relative
//! `read`/`write`/`seek` on that handle would silently reintroduce the race on
//! Windows while remaining correct on Unix** — so if you need one, open a
//! separate handle.
//!
//! Both platforms may transfer fewer bytes than requested, so both functions
//! return the count and callers must not assume a full transfer.

use std::fs::File;
use std::io;

/// Read into `buffer` starting at absolute `offset`, returning the byte count.
#[cfg(unix)]
pub(crate) fn read_at(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
    use std::os::unix::fs::FileExt;
    file.read_at(buffer, offset)
}

/// Write `buffer` at absolute `offset`, returning the byte count.
#[cfg(unix)]
pub(crate) fn write_at(file: &File, buffer: &[u8], offset: u64) -> io::Result<usize> {
    use std::os::unix::fs::FileExt;
    file.write_at(buffer, offset)
}

#[cfg(windows)]
pub(crate) fn read_at(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
    use std::os::windows::fs::FileExt;
    file.seek_read(buffer, offset)
}

#[cfg(windows)]
pub(crate) fn write_at(file: &File, buffer: &[u8], offset: u64) -> io::Result<usize> {
    use std::os::windows::fs::FileExt;
    file.seek_write(buffer, offset)
}

// WASI would spell this `std::os::wasi::fs::FileExt` (fd_pread/fd_pwrite),
// but that trait is still unstable (rust-lang/rust#71213). Seek-then-read is
// the data race described above ONLY under concurrency, and wasm32-wasip1 has
// no threads: the browser build runs the whole store on one worker. If wasi
// threads ever land here, replace this with FileExt before enabling them.
#[cfg(target_os = "wasi")]
pub(crate) fn read_at(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = file;
    f.seek(SeekFrom::Start(offset))?;
    f.read(buffer)
}

#[cfg(target_os = "wasi")]
pub(crate) fn write_at(file: &File, buffer: &[u8], offset: u64) -> io::Result<usize> {
    use std::io::{Seek, SeekFrom, Write};
    let mut f = file;
    f.seek(SeekFrom::Start(offset))?;
    f.write(buffer)
}

// No other fallback arm on purpose. A platform without positioned I/O cannot
// run this store correctly, and emulating it with seek-then-read would be a
// data race rather than a limitation — better a compile error naming the
// platform than a build that corrupts pages under concurrency.
