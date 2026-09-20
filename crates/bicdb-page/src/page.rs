//! On-disk page format.
//!
//! Phase 1 of `docs/server-paged-storage-todo.md`: "an explicit on-disk page
//! format with a new BicDB format version: magic, format version, page type,
//! 64-bit page ID, generation, checksum, LSN, free-space metadata, and
//! encryption/compression flags."
//!
//! # Layout
//!
//! Every page begins with a fixed [`PAGE_HEADER_BYTES`]-byte header and ends
//! with an 8-byte trailer. All multi-byte integers are little-endian.
//!
//! ```text
//! offset  size  field
//!      0     4  magic          b"BPG1"
//!      4     2  format_version u16
//!      6     1  page_type      u8
//!      7     1  flags          u8   bit0 compressed, bit1 encrypted
//!      8     8  page_id        u64
//!     16     8  generation     u64  bumped on every durable write
//!     24     8  lsn            u64  write-ahead log position
//!     32     4  checksum       u32  crc32c over the page, checksum field zeroed
//!     36     2  free_start     u16  end of the slot directory
//!     38     2  free_end       u16  start of tuple data (grows down)
//!     40     2  slot_count     u16
//!     42     2  reserved
//!     44     8  next_page      u64  overflow chain / free-list link
//!     52     4  payload_len    u32  bytes of payload on an overflow page
//!     56     8  reserved
//!  len-8     8  trailer        u64  copy of `generation`
//! ```
//!
//! # Why both a checksum and a trailer
//!
//! The checksum catches bit rot and misdirected writes. It does *not* reliably
//! catch a torn write, because a page half-written by a crash can still contain
//! a checksum consistent with whichever half survived if the writer was
//! interrupted before updating it — and on a re-used page, the surviving half
//! may be a perfectly valid older page.
//!
//! The trailer closes that hole: `generation` is written at both ends of the
//! page, so a write torn anywhere in between leaves head and tail disagreeing.
//! Sector-atomicity assumptions are not required — only that the device does not
//! reorder within a single `pwrite`, which is the same assumption a torn-page
//! check in any page-based engine makes.

use crate::error::{PageError, Result};

/// A 64-bit page identifier. Page 0 is always the superblock.
///
/// 64-bit throughout, per the roadmap's requirement that accidental
/// `usize`/32-bit truncation be impossible on a 32-bit build: a `PageId` never
/// converts to `usize` except through [`PageId::file_offset`], which returns
/// `u64`.
pub type PageId = u64;

/// The superblock, always page 0.
pub const SUPERBLOCK_PAGE_ID: PageId = 0;

/// Magic bytes at the start of every page.
pub const PAGE_MAGIC: [u8; 4] = *b"BPG1";

/// Current page format version. Bumped when the layout above changes
/// incompatibly; [`PageHeader::decode`] refuses anything higher.
pub const PAGE_FORMAT_VERSION: u16 = 1;

/// Fixed header size. Chosen as a round 64 bytes: one cache line, and it leaves
/// room to add fields without moving the payload.
pub const PAGE_HEADER_BYTES: usize = 64;

/// Trailer size — the torn-write guard.
pub const PAGE_TRAILER_BYTES: usize = 8;

/// Default page size. 8 KiB matches the granularity most filesystems and NVMe
/// devices handle efficiently, and is the size to beat when the roadmap's
/// "benchmark candidate page sizes instead of assuming one" work runs; see
/// [`crate::PageStoreOptions::page_size`].
pub const DEFAULT_PAGE_SIZE: u32 = 8192;

pub const MIN_PAGE_SIZE: u32 = 512;
pub const MAX_PAGE_SIZE: u32 = 1024 * 1024;

/// What a page holds. Stored as a `u8`; unknown values are rejected on read so a
/// page written by a newer binary cannot be silently misinterpreted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum PageType {
    /// File header: page size, page count, free-list head.
    Superblock = 1,
    /// Slotted heap page holding variable-size records.
    Heap = 2,
    /// Continuation page for a value too large for one heap page.
    Overflow = 3,
    /// B+ tree interior node.
    BTreeInterior = 4,
    /// B+ tree leaf node.
    BTreeLeaf = 5,
    /// On the free list, contents meaningless.
    Free = 6,
}

impl PageType {
    pub fn from_u8(value: u8) -> Option<Self> {
        Some(match value {
            1 => Self::Superblock,
            2 => Self::Heap,
            3 => Self::Overflow,
            4 => Self::BTreeInterior,
            5 => Self::BTreeLeaf,
            6 => Self::Free,
            _ => return None,
        })
    }
}

/// Page flags. Compression and encryption are recorded per page so they can be
/// applied at page boundaries while preserving random access, per the roadmap's
/// "encrypt and compress at page/extent boundaries" item.
pub mod flags {
    pub const COMPRESSED: u8 = 0b0000_0001;
    pub const ENCRYPTED: u8 = 0b0000_0010;
}

/// Validate a page size: power of two within bounds. Enforced at creation so no
/// arithmetic downstream has to defend against a ragged size.
pub fn validate_page_size(page_size: u32) -> Result<()> {
    if page_size < MIN_PAGE_SIZE
        || page_size > MAX_PAGE_SIZE
        || !page_size.is_power_of_two()
        || (page_size as usize) <= PAGE_HEADER_BYTES + PAGE_TRAILER_BYTES
    {
        return Err(PageError::InvalidPageSize(page_size));
    }
    Ok(())
}

/// Byte offset of a page within its file. Returns `u64` so a >4 GiB file cannot
/// be truncated by a `usize` on a 32-bit target.
#[inline]
pub fn file_offset(page_id: PageId, page_size: u32) -> u64 {
    page_id * u64::from(page_size)
}

/// The parsed page header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageHeader {
    pub page_id: PageId,
    pub page_type: PageType,
    pub format_version: u16,
    pub flags: u8,
    pub generation: u64,
    pub lsn: u64,
    pub free_start: u16,
    pub free_end: u16,
    pub slot_count: u16,
    pub next_page: PageId,
    pub payload_len: u32,
}

impl PageHeader {
    pub fn new(page_id: PageId, page_type: PageType, page_size: u32) -> Self {
        Self {
            page_id,
            page_type,
            format_version: PAGE_FORMAT_VERSION,
            flags: 0,
            generation: 0,
            lsn: 0,
            free_start: PAGE_HEADER_BYTES as u16,
            free_end: (page_size as usize - PAGE_TRAILER_BYTES) as u16,
            slot_count: 0,
            next_page: 0,
            payload_len: 0,
        }
    }

    pub fn is_compressed(&self) -> bool {
        self.flags & flags::COMPRESSED != 0
    }

    pub fn is_encrypted(&self) -> bool {
        self.flags & flags::ENCRYPTED != 0
    }

    /// Write this header into the front of `page`. Does not compute the
    /// checksum — see [`finalize`].
    pub fn encode(&self, page: &mut [u8]) {
        debug_assert!(page.len() >= PAGE_HEADER_BYTES + PAGE_TRAILER_BYTES);
        page[0..4].copy_from_slice(&PAGE_MAGIC);
        page[4..6].copy_from_slice(&self.format_version.to_le_bytes());
        page[6] = self.page_type as u8;
        page[7] = self.flags;
        page[8..16].copy_from_slice(&self.page_id.to_le_bytes());
        page[16..24].copy_from_slice(&self.generation.to_le_bytes());
        page[24..32].copy_from_slice(&self.lsn.to_le_bytes());
        page[32..36].copy_from_slice(&0u32.to_le_bytes()); // checksum, filled by finalize
        page[36..38].copy_from_slice(&self.free_start.to_le_bytes());
        page[38..40].copy_from_slice(&self.free_end.to_le_bytes());
        page[40..42].copy_from_slice(&self.slot_count.to_le_bytes());
        page[42..44].copy_from_slice(&0u16.to_le_bytes());
        page[44..52].copy_from_slice(&self.next_page.to_le_bytes());
        page[52..56].copy_from_slice(&self.payload_len.to_le_bytes());
        page[56..64].copy_from_slice(&0u64.to_le_bytes());
    }

    /// Parse a header, verifying magic, version, and page type but **not** the
    /// checksum or trailer — those are [`verify`], which needs the whole page.
    pub fn decode(page: &[u8], path: &std::path::Path) -> Result<Self> {
        if page.len() < PAGE_HEADER_BYTES + PAGE_TRAILER_BYTES {
            return Err(PageError::InvalidPageSize(page.len() as u32));
        }
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&page[0..4]);
        if magic != PAGE_MAGIC {
            return Err(PageError::NotAPageFile {
                path: path.to_path_buf(),
                found: magic,
            });
        }
        let format_version = u16::from_le_bytes([page[4], page[5]]);
        if format_version > PAGE_FORMAT_VERSION {
            return Err(PageError::UnsupportedVersion {
                path: path.to_path_buf(),
                found: format_version,
                supported: PAGE_FORMAT_VERSION,
            });
        }
        let page_id = u64::from_le_bytes(page[8..16].try_into().unwrap());
        let page_type = PageType::from_u8(page[6]).ok_or(PageError::UnexpectedPageType {
            page_id,
            found: page[6],
            expected: 0,
        })?;
        Ok(Self {
            page_id,
            page_type,
            format_version,
            flags: page[7],
            generation: u64::from_le_bytes(page[16..24].try_into().unwrap()),
            lsn: u64::from_le_bytes(page[24..32].try_into().unwrap()),
            free_start: u16::from_le_bytes([page[36], page[37]]),
            free_end: u16::from_le_bytes([page[38], page[39]]),
            slot_count: u16::from_le_bytes([page[40], page[41]]),
            next_page: u64::from_le_bytes(page[44..52].try_into().unwrap()),
            payload_len: u32::from_le_bytes(page[52..56].try_into().unwrap()),
        })
    }
}

/// crc32c over a page with the checksum field treated as zero.
fn compute_checksum(page: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&page[0..32]);
    hasher.update(&[0u8; 4]);
    hasher.update(&page[36..]);
    hasher.finalize()
}

fn stored_checksum(page: &[u8]) -> u32 {
    u32::from_le_bytes(page[32..36].try_into().unwrap())
}

/// Stamp the generation trailer and checksum. Call immediately before writing a
/// page to disk; nothing may modify the buffer afterwards.
pub fn finalize(page: &mut [u8], generation: u64) {
    let len = page.len();
    page[16..24].copy_from_slice(&generation.to_le_bytes());
    page[len - PAGE_TRAILER_BYTES..].copy_from_slice(&generation.to_le_bytes());
    let checksum = compute_checksum(page);
    page[32..36].copy_from_slice(&checksum.to_le_bytes());
}

/// Verify a page read from disk: checksum first, then the torn-write trailer.
///
/// Checksum is checked first deliberately. A torn write usually also breaks the
/// checksum, and "checksum mismatch" is the more actionable report; the torn
/// check catches the narrower case where the surviving halves happen to be
/// individually consistent.
pub fn verify(page: &[u8], page_id: PageId) -> Result<()> {
    let stored = stored_checksum(page);
    let computed = compute_checksum(page);
    if stored != computed {
        return Err(PageError::ChecksumMismatch {
            page_id,
            stored,
            computed,
        });
    }
    let header_generation = u64::from_le_bytes(page[16..24].try_into().unwrap());
    let trailer = u64::from_le_bytes(page[page.len() - PAGE_TRAILER_BYTES..].try_into().unwrap());
    if header_generation != trailer {
        return Err(PageError::TornPage {
            page_id,
            header: header_generation,
            trailer,
        });
    }
    Ok(())
}

/// Bytes available for payload on a page of this size.
#[inline]
pub fn usable_bytes(page_size: u32) -> usize {
    page_size as usize - PAGE_HEADER_BYTES - PAGE_TRAILER_BYTES
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn blank(page_size: u32, page_type: PageType) -> Vec<u8> {
        let mut page = vec![0u8; page_size as usize];
        PageHeader::new(7, page_type, page_size).encode(&mut page);
        finalize(&mut page, 1);
        page
    }

    #[test]
    fn header_round_trips() {
        let page_size = DEFAULT_PAGE_SIZE;
        let mut page = vec![0u8; page_size as usize];
        let mut header = PageHeader::new(42, PageType::Heap, page_size);
        header.lsn = 9_876_543_210;
        header.next_page = u64::MAX / 3;
        header.slot_count = 17;
        header.payload_len = 1234;
        header.flags = flags::COMPRESSED | flags::ENCRYPTED;
        header.encode(&mut page);
        finalize(&mut page, 5);

        let decoded = PageHeader::decode(&page, Path::new("t")).unwrap();
        assert_eq!(decoded.page_id, 42);
        assert_eq!(decoded.page_type, PageType::Heap);
        assert_eq!(decoded.lsn, 9_876_543_210);
        assert_eq!(decoded.next_page, u64::MAX / 3);
        assert_eq!(decoded.slot_count, 17);
        assert_eq!(decoded.payload_len, 1234);
        assert!(decoded.is_compressed());
        assert!(decoded.is_encrypted());
        assert_eq!(decoded.generation, 5);
    }

    #[test]
    fn verify_accepts_a_freshly_finalized_page() {
        let page = blank(DEFAULT_PAGE_SIZE, PageType::Heap);
        verify(&page, 7).unwrap();
    }

    #[test]
    fn a_single_flipped_bit_anywhere_is_caught() {
        // Walk the whole page rather than sampling: an offset excluded from the
        // checksum would be a silent corruption hole, and only exhaustive
        // coverage proves there isn't one.
        let original = blank(1024, PageType::Heap);
        for offset in 0..original.len() {
            let mut page = original.clone();
            page[offset] ^= 0b0000_1000;
            let result = verify(&page, 7);
            assert!(
                result.is_err(),
                "flipping byte {offset} went undetected — it is outside the checksum"
            );
        }
    }

    #[test]
    fn a_torn_write_is_caught_even_when_the_checksum_agrees() {
        // Simulate the nasty case: the tail of an older generation survives while
        // the head is new, and the attacker-equivalent (a crash) leaves a
        // checksum that matches the bytes present.
        let mut page = blank(1024, PageType::Heap);
        let len = page.len();
        page[16..24].copy_from_slice(&99u64.to_le_bytes()); // new head generation
        page[len - PAGE_TRAILER_BYTES..].copy_from_slice(&1u64.to_le_bytes()); // old tail
        let checksum = compute_checksum(&page);
        page[32..36].copy_from_slice(&checksum.to_le_bytes()); // consistent checksum

        match verify(&page, 7) {
            Err(PageError::TornPage {
                header, trailer, ..
            }) => {
                assert_eq!(header, 99);
                assert_eq!(trailer, 1);
            }
            other => panic!("torn page not detected: {other:?}"),
        }
    }

    #[test]
    fn foreign_bytes_are_not_mistaken_for_a_page() {
        let page = vec![0x5au8; 1024];
        let error = PageHeader::decode(&page, Path::new("t.db")).unwrap_err();
        assert!(matches!(error, PageError::NotAPageFile { .. }));
        assert!(error.is_corruption());
    }

    #[test]
    fn a_newer_format_version_is_refused_rather_than_guessed_at() {
        let mut page = blank(1024, PageType::Heap);
        page[4..6].copy_from_slice(&(PAGE_FORMAT_VERSION + 1).to_le_bytes());
        finalize(&mut page, 1);
        let error = PageHeader::decode(&page, Path::new("t.db")).unwrap_err();
        assert!(matches!(error, PageError::UnsupportedVersion { .. }));
    }

    #[test]
    fn an_unknown_page_type_is_refused() {
        let mut page = blank(1024, PageType::Heap);
        page[6] = 200;
        finalize(&mut page, 1);
        let error = PageHeader::decode(&page, Path::new("t.db")).unwrap_err();
        assert!(matches!(error, PageError::UnexpectedPageType { .. }));
    }

    #[test]
    fn page_size_validation_rejects_ragged_and_extreme_sizes() {
        validate_page_size(512).unwrap();
        validate_page_size(4096).unwrap();
        validate_page_size(DEFAULT_PAGE_SIZE).unwrap();
        validate_page_size(MAX_PAGE_SIZE).unwrap();

        for bad in [0, 1, 100, 513, 4095, 6144, MAX_PAGE_SIZE * 2] {
            assert!(validate_page_size(bad).is_err(), "{bad} should be rejected");
        }
    }

    #[test]
    fn file_offsets_stay_64_bit_past_four_gigabytes() {
        // The truncation this guards against is silent and only shows up on data
        // sets nobody wants to debug on, so it is asserted arithmetically here
        // and again against a real sparse file in tests/large_offsets.rs.
        let page_size = DEFAULT_PAGE_SIZE;
        let four_gib = 4u64 * 1024 * 1024 * 1024;
        let page_at_4gib = four_gib / u64::from(page_size);
        assert_eq!(file_offset(page_at_4gib, page_size), four_gib);

        let one_tib = 1024u64 * 1024 * 1024 * 1024;
        let page_at_1tib = one_tib / u64::from(page_size);
        assert_eq!(file_offset(page_at_1tib, page_size), one_tib);

        // And past the 32-bit page-id boundary.
        let huge = u64::from(u32::MAX) + 1_000;
        assert_eq!(file_offset(huge, page_size), huge * u64::from(page_size));
    }

    #[test]
    fn usable_bytes_excludes_header_and_trailer() {
        assert_eq!(
            usable_bytes(DEFAULT_PAGE_SIZE),
            DEFAULT_PAGE_SIZE as usize - PAGE_HEADER_BYTES - PAGE_TRAILER_BYTES
        );
    }
}
