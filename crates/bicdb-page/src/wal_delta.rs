//! Experimental page-delta redo records. See `docs/page-delta-redo-design.md`.
//!
//! **Not enabled anywhere by default.** Two independent locks must both be
//! open before a single delta is written:
//!
//! 1. the process gate `BICDB_PAGE_WAL_DELTAS`, which parses as on only for the
//!    exact value `1` ([`gate_from_env`]); and
//! 2. the store itself was CREATED as a disposable experimental store
//!    (`PagedStoreOptions::experimental_page_deltas_disposable`), which writes
//!    the sticky `BICDBPG5` superblock marker at creation and only then.
//!
//! An existing or production store never acquires the marker, whatever the
//! environment says, and a store carrying the marker cannot be opened by an
//! engine that predates this format. It is neither upgradeable nor
//! downgradeable: it is a disposable test store.
//!
//! This module holds the pure parts: the gate parser, the run encoding, the
//! masked checksums and the replay applier. The WAL and buffer-pool halves
//! live in `wal.rs` and `pool.rs`.

use crate::page::{PAGE_HEADER_BYTES, PAGE_TRAILER_BYTES};

/// The environment variable that is the process half of the two locks.
pub const GATE_ENV: &str = "BICDB_PAGE_WAL_DELTAS";

/// Parse the gate value. Only the exact string `1` is on.
pub fn gate_value_enables(value: Option<&str>) -> bool {
    value == Some("1")
}

/// Read the gate from the environment now. Called once per store open.
pub fn gate_from_env() -> bool {
    gate_value_enables(std::env::var(GATE_ENV).ok().as_deref())
}

/// Bytes `page::finalize` stamps on every physical write: generation,
/// checksum and trailer. A delta never carries them and both checksums
/// ignore them, because replay re-stamps them through `write_page`.
#[inline]
pub(crate) fn is_masked(offset: usize, page_len: usize) -> bool {
    (16..24).contains(&offset)
        || (32..36).contains(&offset)
        || offset >= page_len - PAGE_TRAILER_BYTES
}

/// CRC32 of `page` with the stamped fields treated as zero.
pub(crate) fn masked_crc(page: &[u8]) -> u32 {
    let len = page.len();
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&page[0..16]);
    hasher.update(&[0u8; 8]);
    hasher.update(&page[24..32]);
    hasher.update(&[0u8; 4]);
    hasher.update(&page[36..len - PAGE_TRAILER_BYTES]);
    hasher.finalize()
}

/// Fixed payload prefix: base_lsn u64, base_crc u32, result_crc u32,
/// run_count u32.
pub(crate) const DELTA_PREFIX_BYTES: usize = 8 + 4 + 4 + 4;
/// Per-run header: offset u32, len u16.
const RUN_HEADER_BYTES: usize = 4 + 2;
/// Runs closer than this merge into one.
const MERGE_GAP: usize = 8;

/// Encode the delta payload turning `base` into `current`, or `None` when it
/// would exceed half a page (a full image is then cheaper to replay).
pub(crate) fn encode(base_lsn: u64, base: &[u8], current: &[u8]) -> Option<Vec<u8>> {
    debug_assert_eq!(base.len(), current.len());
    let len = current.len();
    if base.len() != len || len < PAGE_HEADER_BYTES + PAGE_TRAILER_BYTES {
        return None;
    }
    let limit = len / 2;
    // Collect differing ranges outside the masked fields.
    let mut runs: Vec<(usize, usize)> = Vec::new();
    let body_end = len - PAGE_TRAILER_BYTES;
    let mut i = 0;
    while i < body_end {
        // Skip equal 64-byte blocks with one comparison. Past the header no
        // block overlaps a masked field before the trailer.
        if i >= PAGE_HEADER_BYTES && i + 64 <= body_end && base[i..i + 64] == current[i..i + 64] {
            i += 64;
            continue;
        }
        if is_masked(i, len) || base[i] == current[i] {
            i += 1;
            continue;
        }
        let start = i;
        while i < body_end && !is_masked(i, len) && base[i] != current[i] {
            i += 1;
        }
        match runs.last_mut() {
            Some((_, last_end))
                if start - *last_end <= MERGE_GAP
                    && !(*last_end..start).any(|at| is_masked(at, len)) =>
            {
                *last_end = i;
            }
            _ => runs.push((start, i)),
        }
    }
    let mut payload = Vec::with_capacity(DELTA_PREFIX_BYTES + runs.len() * 16);
    payload.extend_from_slice(&base_lsn.to_le_bytes());
    payload.extend_from_slice(&masked_crc(base).to_le_bytes());
    payload.extend_from_slice(&masked_crc(current).to_le_bytes());
    payload.extend_from_slice(&0u32.to_le_bytes());
    let mut count = 0u32;
    for (start, end) in runs {
        let mut at = start;
        while at < end {
            let take = (end - at).min(u16::MAX as usize);
            payload.extend_from_slice(&(at as u32).to_le_bytes());
            payload.extend_from_slice(&(take as u16).to_le_bytes());
            payload.extend_from_slice(&current[at..at + take]);
            count += 1;
            at += take;
            if payload.len() > limit {
                return None;
            }
        }
    }
    payload[16..20].copy_from_slice(&count.to_le_bytes());
    Some(payload)
}

/// A decoded delta header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DeltaHeader {
    pub base_lsn: u64,
    pub base_crc: u32,
    pub result_crc: u32,
    pub run_count: u32,
}

pub(crate) fn header(payload: &[u8]) -> Option<DeltaHeader> {
    if payload.len() < DELTA_PREFIX_BYTES {
        return None;
    }
    Some(DeltaHeader {
        base_lsn: u64::from_le_bytes(payload[0..8].try_into().unwrap()),
        base_crc: u32::from_le_bytes(payload[8..12].try_into().unwrap()),
        result_crc: u32::from_le_bytes(payload[12..16].try_into().unwrap()),
        run_count: u32::from_le_bytes(payload[16..20].try_into().unwrap()),
    })
}

/// Validate a delta payload's structure against `page_len` without applying
/// it: runs ascending, non-overlapping, inside the page, clear of the masked
/// fields, and exactly consuming the payload.
pub(crate) fn validate(payload: &[u8], page_len: usize) -> Result<DeltaHeader, String> {
    apply_runs(payload, page_len, None)
}

/// Apply the runs of `payload` to `page` in place. Structure is validated as
/// in [`validate`] before any byte is written.
pub(crate) fn apply(payload: &[u8], page: &mut [u8]) -> Result<DeltaHeader, String> {
    let len = page.len();
    apply_runs(payload, len, None)?;
    apply_runs(payload, len, Some(page))
}

fn apply_runs(
    payload: &[u8],
    page_len: usize,
    mut page: Option<&mut [u8]>,
) -> Result<DeltaHeader, String> {
    let header = header(payload).ok_or("delta payload shorter than its header")?;
    let mut at = DELTA_PREFIX_BYTES;
    let mut previous_end = 0usize;
    for _ in 0..header.run_count {
        if payload.len() < at + RUN_HEADER_BYTES {
            return Err("delta run header past the end of the payload".into());
        }
        let offset = u32::from_le_bytes(payload[at..at + 4].try_into().unwrap()) as usize;
        let len = u16::from_le_bytes(payload[at + 4..at + 6].try_into().unwrap()) as usize;
        at += RUN_HEADER_BYTES;
        if len == 0
            || offset < previous_end
            || offset.checked_add(len).is_none_or(|end| end > page_len)
            || payload.len() < at + len
        {
            return Err(format!("delta run {offset}+{len} is malformed"));
        }
        if (offset..offset + len).any(|byte| is_masked(byte, page_len)) {
            return Err(format!("delta run {offset}+{len} overlaps a stamped field"));
        }
        if let Some(page) = page.as_deref_mut() {
            page[offset..offset + len].copy_from_slice(&payload[at..at + len]);
        }
        previous_end = offset + len;
        at += len;
    }
    if at != payload.len() {
        return Err("delta payload has trailing bytes".into());
    }
    Ok(header)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(fill: u8) -> Vec<u8> {
        vec![fill; 512]
    }

    #[test]
    fn only_the_exact_value_one_enables_the_gate() {
        assert!(gate_value_enables(Some("1")));
        for value in ["0", "true", " 1", "1 ", "on", "yes", "", "01"] {
            assert!(!gate_value_enables(Some(value)), "{value:?}");
        }
        assert!(!gate_value_enables(None));
    }

    #[test]
    fn encode_then_apply_reproduces_the_page_outside_stamped_fields() {
        let base = page(7);
        let mut current = base.clone();
        current[100..140].copy_from_slice(&[9; 40]);
        current[300] = 1;
        current[20] = 99; // stamped (generation): never carried
        current[510] = 42; // trailer: never carried
        let payload = encode(5, &base, &current).unwrap();
        let mut replay = base.clone();
        let header = apply(&payload, &mut replay).unwrap();
        assert_eq!(header.base_lsn, 5);
        assert_eq!(header.base_crc, masked_crc(&base));
        assert_eq!(masked_crc(&replay), header.result_crc);
        assert_eq!(masked_crc(&replay), masked_crc(&current));
        assert_eq!(&replay[100..140], &current[100..140]);
        assert_eq!(replay[20], 7);
    }

    #[test]
    fn identical_pages_encode_an_empty_delta() {
        let base = page(3);
        let payload = encode(1, &base, &base).unwrap();
        assert_eq!(payload.len(), DELTA_PREFIX_BYTES);
        assert_eq!(header(&payload).unwrap().run_count, 0);
    }

    #[test]
    fn a_large_change_falls_back_to_a_full_image() {
        let base = page(0);
        let current = page(1);
        assert!(encode(1, &base, &current).is_none());
    }

    #[test]
    fn malformed_runs_are_rejected_before_any_byte_is_written() {
        let base = page(0);
        let mut current = base.clone();
        current[200] = 1;
        let mut payload = encode(1, &base, &current).unwrap();
        // Point the run past the end of the page.
        payload[DELTA_PREFIX_BYTES..DELTA_PREFIX_BYTES + 4].copy_from_slice(&600u32.to_le_bytes());
        let mut target = base.clone();
        assert!(apply(&payload, &mut target).is_err());
        assert_eq!(target, base);
        // A run over the checksum field.
        let mut payload = encode(1, &base, &current).unwrap();
        payload[DELTA_PREFIX_BYTES..DELTA_PREFIX_BYTES + 4].copy_from_slice(&33u32.to_le_bytes());
        assert!(validate(&payload, 512).is_err());
        // Trailing garbage.
        let mut payload = encode(1, &base, &current).unwrap();
        payload.push(0);
        assert!(validate(&payload, 512).is_err());
    }
}
