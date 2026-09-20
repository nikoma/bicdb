//! Packed full-text segments: physical-format v2.
//!
//! The v1 representation stored every posting block, impact block and
//! dictionary entry as a row in a paged B-tree keyspace — 18.1 million keyed
//! objects on a 123k-document corpus, each paying a key, a slot, MVCC headers
//! and page slack. Measured against Tantivy on identical documents that came
//! to 13x the storage and 21x the build time (`docs/fts-lean-storage-campaign.md`).
//!
//! A segment is the same information as packed immutable files:
//!
//! ```text
//! <paged>/fts-segments/<physical>/
//!   postings.dat    concatenated pk-ordered posting blocks, term-major
//!   impacts.dat     concatenated impact-ordered blocks, term-major
//!   terms.dat       front-coded term directory + per-block metadata
//!   manifest.json   format version, byte counts, SHA-256 per file
//! ```
//!
//! **The block bytes are byte-identical to v1.** The builder hands this module
//! the same `encode_numeric_posting_run` / `encode_compact_impact_block`
//! output it used to write into the keyspace; only WHERE they live changes.
//! That is what makes the equivalence gate meaningful: a v2 read produces the
//! same bytes v1 would have, so every downstream kernel behaves identically.
//!
//! The index remains DERIVED state. A segment is rebuilt from authoritative
//! records, never repaired; a damaged file is refused, not reinterpreted.
//!
//! Mutability contract: the segment is the immutable base. Post-build writes
//! land in the transactional tail exactly as before, and a FOLD writes the
//! merged term back into the paged keyspace — which then **overrides the
//! segment for that term** (see `PagedRecords`). Tombstones and tails are
//! consulted above this layer and are unchanged.

use std::fs::{self, File};
use std::io::{BufWriter, Read, Write};
#[cfg(not(any(unix, windows)))]
use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{BicDbError, Result};
use crate::fts_format::FullTextTermStatistics;

/// Positioned read that fills the whole buffer, portable across the
/// per-platform `FileExt` traits (`read_exact_at` on Unix; `seek_read`
/// on Windows, which does not guarantee a full read and needs the loop).
#[cfg(unix)]
fn read_exact_at(file: &File, buffer: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buffer, offset)
}

#[cfg(windows)]
fn read_exact_at(file: &File, buffer: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    let mut filled = 0usize;
    while filled < buffer.len() {
        let read = file.seek_read(&mut buffer[filled..], offset + filled as u64)?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ));
        }
        filled += read;
    }
    Ok(())
}

/// WASI and other targets do not expose Unix/Windows `FileExt`. BicDB's WASM
/// runtime is single-threaded, so a cloned handle plus an explicit seek is a
/// safe portable fallback for the same bounded positional read contract.
#[cfg(not(any(unix, windows)))]
fn read_exact_at(file: &File, buffer: &mut [u8], offset: u64) -> std::io::Result<()> {
    let mut positioned = file.try_clone()?;
    positioned.seek(SeekFrom::Start(offset))?;
    positioned.read_exact(buffer)
}

pub(crate) const FTS_SEGMENT_FORMAT: u32 = 6;

/// `maximum_contribution` is a pruning UPPER bound, so a lossy encoding is
/// legal as long as it only ever rounds UP: results stay identical, the
/// worst case is a slightly less eager skip. One log-scale byte replaces
/// the f32 — 3.7% granularity across (1e-4, 2.0], 16 MB across a real
/// dictionary.
const CONTRIBUTION_FLOOR: f32 = 1e-4;
const CONTRIBUTION_STEPS: f32 = 255.0;
const CONTRIBUTION_CEILING: f32 = 2.0;

pub(crate) fn quantize_contribution(value: f32) -> u8 {
    if !value.is_finite() || value >= CONTRIBUTION_CEILING {
        return u8::MAX;
    }
    if value <= CONTRIBUTION_FLOOR {
        return 0;
    }
    let ratio = (CONTRIBUTION_CEILING / CONTRIBUTION_FLOOR).ln() / CONTRIBUTION_STEPS;
    let mut step = ((value / CONTRIBUTION_FLOOR).ln() / ratio).ceil() as i64;
    step = step.clamp(0, 255);
    // Float guard: the bound must never round below the value.
    while (step as u8) < u8::MAX && dequantize_contribution(step as u8) < value {
        step += 1;
    }
    step as u8
}

pub(crate) fn dequantize_contribution(step: u8) -> f32 {
    if step == u8::MAX {
        return CONTRIBUTION_CEILING;
    }
    let ratio = (CONTRIBUTION_CEILING / CONTRIBUTION_FLOOR).ln() / CONTRIBUTION_STEPS;
    CONTRIBUTION_FLOOR * (f32::from(step) * ratio).exp()
}
const DOCS_FILE: &str = "docs.dat";
const MANIFEST_FILE: &str = "manifest.json";
const POSTINGS_FILE: &str = "postings.dat";
const IMPACTS_FILE: &str = "impacts.dat";
const TERMS_FILE: &str = "terms.dat";
const PK_STATE_FILE: &str = "pk.state.json";
const IMPACT_STATE_FILE: &str = "impact.state.json";
const TERMS_PK_TMP: &str = "terms-pk.tmp";
const TERMS_IMPACT_TMP: &str = "terms-impact.tmp";
const TERMS_FOOTER_MAGIC: &[u8; 8] = b"BFTSTRM1";
/// Front-coding resets (lcp = 0) every this many terms, and the restart
/// offsets are what binary search descends. 64 keeps the linear tail short
/// while the restart table stays ~1/64th of a pointer per term.
const TERM_RESTART_INTERVAL: usize = 64;
/// Decoded term metadata cached per reader. Popular terms have thousands of
/// block metas; re-decoding them per fetch would tax exactly the terms that
/// are queried most.
const TERM_META_CACHE_CAP: usize = 4096;

/// `postings.dat` -> `postings.part0007.dat`, `terms-pk.tmp` ->
/// `terms-pk.part0007.tmp`: one range-merge part's slice of a final file.
fn part_file_name(base: &str, part: usize) -> String {
    match base.rsplit_once('.') {
        Some((stem, extension)) => format!("{stem}.part{part:04}.{extension}"),
        None => format!("{base}.part{part:04}"),
    }
}

fn segment_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Index(message.into())
}

/// An immutable segment file held readable: memory-mapped when the `mmap`
/// feature is on (published files are never modified in place, and a DROP
/// unlinks the directory — the mapping stays valid until the reader drops),
/// read resident otherwise. Mapping the 165 MB term directory instead of
/// reading it removes the ~38 ms first-query cost per process.
pub(crate) struct ResidentBytes {
    #[cfg(feature = "mmap")]
    mapping: Option<memmap2::Mmap>,
    owned: Vec<u8>,
}

impl ResidentBytes {
    fn open(path: &Path, what: &str) -> Result<Self> {
        let file = File::open(path)
            .map_err(|error| segment_error(format!("cannot open segment {what}: {error}")))?;
        #[cfg(feature = "mmap")]
        {
            // SAFETY: segment files are immutable once published; truncation
            // concurrent with a mapped read cannot happen because replacement
            // goes through a new directory + registry swap, never in place.
            if let Ok(mapping) = unsafe { memmap2::Mmap::map(&file) } {
                #[cfg(unix)]
                let _ = mapping.advise(memmap2::Advice::Random);
                return Ok(Self {
                    mapping: Some(mapping),
                    owned: Vec::new(),
                });
            }
        }
        let mut owned = Vec::new();
        let mut file = file;
        file.read_to_end(&mut owned)
            .map_err(|error| segment_error(format!("cannot read segment {what}: {error}")))?;
        Ok(Self {
            #[cfg(feature = "mmap")]
            mapping: None,
            owned,
        })
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        #[cfg(feature = "mmap")]
        if let Some(mapping) = &self.mapping {
            return &mapping[..];
        }
        &self.owned
    }
}

impl std::ops::Deref for ResidentBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl std::fmt::Debug for ResidentBytes {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "ResidentBytes({} bytes)", self.as_slice().len())
    }
}

// ---------------------------------------------------------------------------
// docs.dat backing: resident vs. bounded-lazy
//
// `docs.dat` is a dense array of 8 bytes per document (doc_length | doc_distinct
// << 32), read at random during BM25 scoring. Holding it fully resident is
// 8 bytes * documents per SEGMENT — ~600 MiB at 75M docs — and that memory is
// unreclaimable anonymous when the segment lives on tmpfs/overlay (an mmap of a
// tmpfs file is anonymous) or when the `mmap` feature is unavailable. Across
// hundreds of open shards that is the whole node envelope.
//
// `LazyDocs` serves the same random reads from the file through a bounded LRU
// of fixed blocks, so the resident footprint per reader is capped at
// `block_bytes * max_blocks` (default 16 MiB) regardless of corpus size. Opt in
// with `BICDB_FTS_DOCS_LAZY=1`; the resident path stays the default so existing
// deployments and the hot mmap path are unchanged.

const DOCS_BLOCK_BYTES: u64 = 64 * 1024; // multiple of 8: an 8-byte doc entry never straddles a block

fn fts_docs_lazy_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("BICDB_FTS_DOCS_LAZY")
            .map(|value| matches!(value.as_str(), "1" | "on" | "true" | "yes"))
            .unwrap_or(false)
    })
}

fn fts_docs_cache_blocks() -> usize {
    static BLOCKS: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *BLOCKS.get_or_init(|| {
        std::env::var("BICDB_FTS_DOCS_CACHE_BLOCKS")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|blocks| *blocks > 0)
            .unwrap_or(256) // 256 * 64 KiB = 16 MiB per reader
    })
}

struct DocsBlockCache {
    blocks: FxHashMap<u64, Arc<[u8]>>,
    order: std::collections::VecDeque<u64>,
    max_blocks: usize,
}

/// File-backed docs table with a bounded LRU block cache. Random `doc()` reads
/// touch at most `max_blocks` resident blocks regardless of corpus size.
pub(crate) struct LazyDocs {
    file: File,
    len: u64,
    cache: parking_lot::Mutex<DocsBlockCache>,
}

impl LazyDocs {
    fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)
            .map_err(|error| segment_error(format!("cannot open segment docs: {error}")))?;
        let len = file
            .metadata()
            .map_err(|error| segment_error(format!("cannot stat segment docs: {error}")))?
            .len();
        Ok(Self {
            file,
            len,
            cache: parking_lot::Mutex::new(DocsBlockCache {
                blocks: FxHashMap::default(),
                order: std::collections::VecDeque::new(),
                max_blocks: fts_docs_cache_blocks(),
            }),
        })
    }

    fn block(&self, block_no: u64) -> Result<Arc<[u8]>> {
        {
            let mut cache = self.cache.lock();
            if let Some(block) = cache.blocks.get(&block_no).cloned() {
                // Move to most-recently-used.
                if let Some(pos) = cache.order.iter().position(|value| *value == block_no) {
                    cache.order.remove(pos);
                }
                cache.order.push_back(block_no);
                return Ok(block);
            }
        }
        let start = block_no * DOCS_BLOCK_BYTES;
        let this_len = DOCS_BLOCK_BYTES.min(self.len.saturating_sub(start)) as usize;
        let mut buffer = vec![0u8; this_len];
        read_exact_at(&self.file, &mut buffer, start)
            .map_err(|error| segment_error(format!("cannot read segment docs block: {error}")))?;
        let block: Arc<[u8]> = Arc::from(buffer);
        let mut cache = self.cache.lock();
        cache.blocks.insert(block_no, Arc::clone(&block));
        cache.order.push_back(block_no);
        while cache.order.len() > cache.max_blocks {
            if let Some(evicted) = cache.order.pop_front() {
                cache.blocks.remove(&evicted);
            }
        }
        Ok(block)
    }

    fn doc(&self, doc_id: u64) -> Result<(u32, u32)> {
        let offset = doc_id
            .checked_mul(8)
            .ok_or_else(|| segment_error("slim posting references a document outside docs.dat"))?;
        if offset
            .checked_add(8)
            .map(|end| end > self.len)
            .unwrap_or(true)
        {
            return Err(segment_error(
                "slim posting references a document outside docs.dat",
            ));
        }
        let block_no = offset / DOCS_BLOCK_BYTES;
        let block = self.block(block_no)?;
        let within = (offset - block_no * DOCS_BLOCK_BYTES) as usize;
        let bytes = &block[within..within + 8];
        Ok((
            u32::from_le_bytes(bytes[0..4].try_into().expect("4 bytes")),
            u32::from_le_bytes(bytes[4..8].try_into().expect("4 bytes")),
        ))
    }
}

/// Owned backing for a reader's docs.dat: fully resident, or bounded-lazy.
pub(crate) enum DocsBacking {
    Resident(ResidentBytes),
    Lazy(LazyDocs),
}

impl DocsBacking {
    fn open(path: &Path) -> Result<Self> {
        if fts_docs_lazy_enabled() {
            Ok(DocsBacking::Lazy(LazyDocs::open(path)?))
        } else {
            Ok(DocsBacking::Resident(ResidentBytes::open(path, "docs")?))
        }
    }

    /// A borrowing view suitable for the block decoders.
    pub(crate) fn view(&self) -> Docs<'_> {
        match self {
            DocsBacking::Resident(resident) => Docs::Slice(resident.as_slice()),
            DocsBacking::Lazy(lazy) => Docs::Lazy(lazy),
        }
    }
}

/// A borrowing view over a docs table for the block decoders: either a resident
/// slice (zero cost) or a bounded-lazy file reader. `doc()` returns
/// `(doc_length, doc_distinct)` for a document id.
pub(crate) enum Docs<'a> {
    Slice(&'a [u8]),
    Lazy(&'a LazyDocs),
}

impl Docs<'_> {
    pub(crate) fn doc(&self, doc_id: u64) -> Result<(u32, u32)> {
        match self {
            Docs::Slice(bytes) => {
                let doc_at = usize::try_from(doc_id)
                    .ok()
                    .and_then(|slot| slot.checked_mul(8))
                    .ok_or_else(|| {
                        segment_error("slim posting references a document outside docs.dat")
                    })?;
                let doc = bytes.get(doc_at..doc_at + 8).ok_or_else(|| {
                    segment_error("slim posting references a document outside docs.dat")
                })?;
                Ok((
                    u32::from_le_bytes(doc[0..4].try_into().expect("4 bytes")),
                    u32::from_le_bytes(doc[4..8].try_into().expect("4 bytes")),
                ))
            }
            Docs::Lazy(lazy) => lazy.doc(doc_id),
        }
    }
}

// ---------------------------------------------------------------------------
// term directory backing: resident vs. bounded-lazy
//
// The term directory (`terms.dat`) is front-coded in fixed restart windows:
// every window resets front-coding and the pk/impact base runs, so a window is
// independently decodable from its own bytes. At corpus scale the directory is
// ~165 MiB PER SEGMENT and, like docs.dat, is held fully resident — the second
// half of the ~1.2 GiB/shard anonymous footprint that crashed the search node.
//
// `LazyTerms` serves each restart window from the file through a bounded LRU,
// so a lookup (binary search + one window decode) or a cursor scan touches at
// most `max_windows` resident windows regardless of directory size. Opt in with
// `BICDB_FTS_TERMS_LAZY=1`.

fn fts_terms_lazy_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("BICDB_FTS_TERMS_LAZY")
            .map(|value| matches!(value.as_str(), "1" | "on" | "true" | "yes"))
            .unwrap_or(false)
    })
}

fn fts_terms_cache_windows() -> usize {
    static WINDOWS: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *WINDOWS.get_or_init(|| {
        std::env::var("BICDB_FTS_TERMS_CACHE_WINDOWS")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|windows| *windows > 0)
            .unwrap_or(1024) // restart windows are a few KiB; 1024 ~ a few MiB
    })
}

struct TermsWindowCache {
    windows: FxHashMap<usize, Arc<[u8]>>,
    order: std::collections::VecDeque<usize>,
    max_windows: usize,
}

/// File-backed term directory with a bounded LRU of restart windows.
pub(crate) struct LazyTerms {
    file: File,
    cache: parking_lot::Mutex<TermsWindowCache>,
}

impl LazyTerms {
    fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)
            .map_err(|error| segment_error(format!("cannot open segment terms: {error}")))?;
        Ok(Self {
            file,
            cache: parking_lot::Mutex::new(TermsWindowCache {
                windows: FxHashMap::default(),
                order: std::collections::VecDeque::new(),
                max_windows: fts_terms_cache_windows(),
            }),
        })
    }

    fn window(&self, restart_idx: usize, start: usize, end: usize) -> Result<Arc<[u8]>> {
        {
            let mut cache = self.cache.lock();
            if let Some(window) = cache.windows.get(&restart_idx).cloned() {
                if let Some(pos) = cache.order.iter().position(|value| *value == restart_idx) {
                    cache.order.remove(pos);
                }
                cache.order.push_back(restart_idx);
                return Ok(window);
            }
        }
        let len = end
            .checked_sub(start)
            .ok_or_else(|| segment_error("segment term directory restart offsets are inverted"))?;
        let mut buffer = vec![0u8; len];
        read_exact_at(&self.file, &mut buffer, start as u64)
            .map_err(|error| segment_error(format!("cannot read segment terms window: {error}")))?;
        let window: Arc<[u8]> = Arc::from(buffer);
        let mut cache = self.cache.lock();
        cache.windows.insert(restart_idx, Arc::clone(&window));
        cache.order.push_back(restart_idx);
        while cache.order.len() > cache.max_windows {
            if let Some(evicted) = cache.order.pop_front() {
                cache.windows.remove(&evicted);
            }
        }
        Ok(window)
    }
}

/// Owned backing for the term directory: fully resident, or bounded-lazy.
pub(crate) enum TermsBacking {
    Resident(ResidentBytes),
    Lazy(LazyTerms),
}

impl TermsBacking {
    fn open(path: &Path) -> Result<Self> {
        if fts_terms_lazy_enabled() {
            Ok(TermsBacking::Lazy(LazyTerms::open(path)?))
        } else {
            Ok(TermsBacking::Resident(ResidentBytes::open(path, "terms")?))
        }
    }

    /// Total directory length, for footer parsing at open (resident only path
    /// needs the whole slice; lazy reads the footer separately).
    fn len(&self) -> usize {
        match self {
            TermsBacking::Resident(resident) => resident.as_slice().len(),
            TermsBacking::Lazy(_) => 0,
        }
    }
}

/// A restart window's bytes: borrowed from the resident directory (zero copy)
/// or a shared owned buffer read lazily from the file. Decodes with
/// window-relative offsets — a restart window is self-contained.
pub(crate) enum WindowBytes<'a> {
    Borrowed(&'a [u8]),
    Shared(Arc<[u8]>),
}

impl std::ops::Deref for WindowBytes<'_> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            WindowBytes::Borrowed(bytes) => bytes,
            WindowBytes::Shared(bytes) => bytes,
        }
    }
}

// ---------------------------------------------------------------------------
// varint
// ---------------------------------------------------------------------------

fn write_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn read_varint(bytes: &[u8], at: &mut usize) -> Result<u64> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *bytes
            .get(*at)
            .ok_or_else(|| segment_error("segment varint runs past end of buffer"))?;
        *at += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 64 {
            return Err(segment_error("segment varint exceeds 64 bits"));
        }
    }
}

// ---------------------------------------------------------------------------
// slim block codec (format 2)
// ---------------------------------------------------------------------------
//
// A stored pk block is only what cannot be recomputed:
//
// ```text
// varint posting_count
// varint doc_stream_len
// doc_stream               BP128 ascending doc-id deltas (the v1 codec)
// per posting: varint position_count, then zigzag-delta varints of the
//              packed u16 positions (order-preserving; packed values are NOT
//              monotonic because the field weight lives in the high bits)
// ```
//
// `doc_length` and `doc_distinct` are DOCUMENT properties; v1 stored them on
// every posting — ~4 bytes multiplied by the document's term count. They now
// live once per document in `docs.dat`, and the reader re-encodes a decoded
// slim block through the ORIGINAL v1 encoder, whose entire header
// (max_impact, max_rank, max_tf, weight_mask) is a deterministic function of
// the postings. Re-encoded bytes are therefore bit-identical to what v1
// stored, and nothing above this layer can tell the difference.

/// A BP128 frame holds at most 128 values behind two header bytes, so one
/// byte of encoded stream can never expand to more than this many values.
/// Used to reject impossible counts before they size an allocation.
const BP128_STREAM_MAX_PER_BYTE: usize = crate::paged_collection::BP128_FRAME_VALUES;

/// Frame-pack a value sequence of any length into `output`.
fn pack_bp128_stream(values: &[u64], output: &mut Vec<u8>) {
    use crate::paged_collection::{pack_bp128_frame, BP128_FRAME_VALUES};
    for frame in values.chunks(BP128_FRAME_VALUES) {
        pack_bp128_frame(frame, output);
    }
}

/// Decode exactly `count` frame-packed values appended to `output`.
fn unpack_bp128_stream(bytes: &[u8], count: usize, output: &mut Vec<u64>) -> Option<()> {
    use crate::paged_collection::unpack_bp128_frame;
    // The count comes from segment bytes, which bit-rot or a tampered file
    // can set to anything. A BP128 frame encodes at most 128 values and
    // costs at least two header bytes, so a count beyond what these bytes
    // could encode is corruption — refuse before reserving rather than
    // aborting the process on a failed allocation
    // (`crate::parse_budget`).
    let encodable = bytes
        .len()
        .saturating_mul(crate::paged_collection::BP128_FRAME_VALUES);
    if count > encodable {
        return None;
    }
    output.clear();
    output.try_reserve(count).ok()?;
    let mut cursor = 0usize;
    let mut frame = Vec::new();
    while output.len() < count {
        unpack_bp128_frame(bytes, &mut cursor, &mut frame)?;
        if output.len() + frame.len() > count {
            return None;
        }
        output.extend_from_slice(&frame);
    }
    if cursor != bytes.len() {
        return None;
    }
    Some(())
}

fn encode_slim_pk_block(postings: &[crate::paged_collection::NumericBlockPosting]) -> Vec<u8> {
    use crate::paged_collection::{push_varint, zigzag_encode};
    // Single-posting blocks — every df=1 term, millions of them — skip the
    // stream framing entirely: [count=1][varint doc][varint pos_count]
    // [u16 first][zigzag varint gaps...]. Four frame headers on one posting
    // cost more than the posting.
    if let [posting] = postings {
        let mut out = Vec::with_capacity(12);
        push_varint(&mut out, 1);
        push_varint(&mut out, posting.document_id);
        push_varint(&mut out, posting.packed_positions.len() as u64);
        if let Some((first, rest)) = posting.packed_positions.split_first() {
            out.extend_from_slice(&first.to_le_bytes());
            let mut previous = i64::from(*first);
            for packed in rest {
                let current = i64::from(*packed);
                push_varint(&mut out, zigzag_encode(current - previous));
                previous = current;
            }
        }
        SLIM_BLOCKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        SLIM_TOTAL_BYTES.fetch_add(out.len() as u64, std::sync::atomic::Ordering::Relaxed);
        return out;
    }
    // Columnar: four frame-packed streams instead of interleaved varints.
    // Bit widths settle per stream — position COUNTS (a few bits) no longer
    // share bytes with position VALUES, and each posting's large first
    // position is delta-chained against the previous posting's rather than
    // stored near-absolute. Score-only decode reads ids + counts and skips
    // the two position streams by their length prefixes without touching a
    // single position byte.
    let mut identifier_deltas = Vec::with_capacity(postings.len());
    let mut position_counts = Vec::with_capacity(postings.len());
    let mut first_positions = Vec::with_capacity(postings.len());
    let mut gap_deltas = Vec::new();
    let mut previous_document = 0u64;
    for posting in postings {
        debug_assert!(posting.document_id >= previous_document, "pk blocks ascend");
        identifier_deltas.push(posting.document_id - previous_document);
        previous_document = posting.document_id;
        position_counts.push(posting.packed_positions.len() as u64);
        if let Some((first, rest)) = posting.packed_positions.split_first() {
            // ABSOLUTE, not delta-chained: consecutive postings are
            // different documents, so their first positions are unrelated —
            // measured, the zigzag chain cost 19.3 bits/value where the raw
            // 16-bit-bounded absolute frames at ~12-16.
            first_positions.push(u64::from(*first));
            let mut previous_position = i64::from(*first);
            for packed in rest {
                let current = i64::from(*packed);
                gap_deltas.push(zigzag_encode(current - previous_position));
                previous_position = current;
            }
        }
    }
    let mut out = Vec::with_capacity(postings.len() * 4);
    push_varint(&mut out, postings.len() as u64);
    let mut stream = Vec::with_capacity(postings.len() * 2);
    for (index, values) in [
        identifier_deltas.as_slice(),
        position_counts.as_slice(),
        first_positions.as_slice(),
        gap_deltas.as_slice(),
    ]
    .into_iter()
    .enumerate()
    {
        stream.clear();
        pack_bp128_stream(values, &mut stream);
        push_varint(&mut out, stream.len() as u64);
        out.extend_from_slice(&stream);
        SLIM_STREAM_BYTES[index]
            .fetch_add(stream.len() as u64, std::sync::atomic::Ordering::Relaxed);
        SLIM_STREAM_VALUES[index]
            .fetch_add(values.len() as u64, std::sync::atomic::Ordering::Relaxed);
    }
    SLIM_BLOCKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    SLIM_TOTAL_BYTES.fetch_add(out.len() as u64, std::sync::atomic::Ordering::Relaxed);
    out
}

/// Build-time accounting for the slim codec (BICDB_FTS_SLIM_STATS=1).
static SLIM_STREAM_BYTES: [std::sync::atomic::AtomicU64; 4] = [
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
];
static SLIM_STREAM_VALUES: [std::sync::atomic::AtomicU64; 4] = [
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
];
static SLIM_BLOCKS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static SLIM_TOTAL_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub(crate) fn report_slim_stats() {
    use std::sync::atomic::Ordering::Relaxed;
    if std::env::var("BICDB_FTS_SLIM_STATS").as_deref() != Ok("1") {
        return;
    }
    let names = ["ids", "counts", "firsts", "gaps"];
    let blocks = SLIM_BLOCKS.load(Relaxed).max(1);
    let total = SLIM_TOTAL_BYTES.load(Relaxed);
    eprintln!("[slim] blocks={blocks} total={:.1} MB", total as f64 / 1e6);
    let mut streams = 0u64;
    for index in 0..4 {
        let bytes = SLIM_STREAM_BYTES[index].load(Relaxed);
        let values = SLIM_STREAM_VALUES[index].load(Relaxed);
        streams += bytes;
        eprintln!(
            "[slim]   {:<7} {:>8.1} MB  {:>10} values  {:.2} B/value",
            names[index],
            bytes as f64 / 1e6,
            values,
            bytes as f64 / values.max(1) as f64
        );
    }
    eprintln!(
        "[slim]   framing overhead (len varints + count varint): {:.1} MB",
        (total - streams) as f64 / 1e6
    );
}

/// Score-only decode of a slim block: document ids, per-document fields and
/// term frequency (= position count), skipping the position bytes without
/// materializing them. The BM25 leader stream lives on this.
pub(crate) fn decode_slim_pk_block_scores(
    bytes: &[u8],
    docs: &Docs<'_>,
) -> Result<Vec<crate::paged_collection::NumericBlockScorePosting>> {
    use crate::paged_collection::read_varint as pc_read_varint;
    let corrupt = || segment_error("corrupt slim posting block");
    let mut cursor = 0usize;
    let count = pc_read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as usize;
    if count == 1 {
        let document_id = pc_read_varint(bytes, &mut cursor).ok_or_else(corrupt)?;
        let term_frequency =
            (pc_read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as u32).max(1);
        let (doc_length, doc_distinct) = docs.doc(document_id)?;
        return Ok(vec![crate::paged_collection::NumericBlockScorePosting {
            document_id,
            doc_length,
            doc_distinct,
            term_frequency,
        }]);
    }
    let mut identifiers = Vec::new();
    let mut position_counts = Vec::new();
    for (stream_index, target) in [(0usize, &mut identifiers), (1, &mut position_counts)] {
        let _ = stream_index;
        let length = pc_read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as usize;
        let stream = bytes
            .get(cursor..cursor.checked_add(length).ok_or_else(corrupt)?)
            .ok_or_else(corrupt)?;
        unpack_bp128_stream(stream, count, target).ok_or_else(corrupt)?;
        cursor += length;
    }
    // The two position streams are never touched: this is the BM25 leader
    // path, and skipping them is two length reads.
    let mut postings = Vec::with_capacity(count);
    let mut document_id = 0u64;
    for index in 0..count {
        document_id += identifiers[index];
        let (doc_length, doc_distinct) = docs.doc(document_id)?;
        postings.push(crate::paged_collection::NumericBlockScorePosting {
            document_id,
            doc_length,
            doc_distinct,
            term_frequency: (position_counts[index] as u32).max(1),
        });
    }
    Ok(postings)
}

/// Stored form of a MULTI-block term's impact sidecar block. The v1 compact
/// bytes zigzag-delta the whole id sequence, so every BP128 frame pays for
/// the big negative jumps at impact-bucket boundaries; splitting into
/// descending-bucket RUNS keeps in-run deltas ascending and narrow.
/// Measured on the retained corpus: 109 -> 79 bytes per block. The reader
/// re-encodes the exact v1 bytes through
/// `encode_compact_impact_block_from_ids`, so kernels see what they always
/// saw.
pub(crate) fn encode_slim_impact_block(
    document_ids: &[u64],
    max_impact: u16,
    max_rank: f32,
) -> Vec<u8> {
    use crate::paged_collection::{push_varint, zigzag_encode};
    let mut runs: Vec<usize> = Vec::new();
    let mut run_len = 0usize;
    let mut previous = None::<u64>;
    for id in document_ids {
        if previous.is_some_and(|previous| *id < previous) {
            runs.push(run_len);
            run_len = 0;
        }
        run_len += 1;
        previous = Some(*id);
    }
    if run_len > 0 {
        runs.push(run_len);
    }
    let mut first_deltas = Vec::with_capacity(runs.len());
    let mut in_run_deltas = Vec::with_capacity(document_ids.len());
    let mut at = 0usize;
    let mut previous_first = 0i64;
    for length in &runs {
        let first = document_ids[at] as i64;
        first_deltas.push(zigzag_encode(first - previous_first));
        previous_first = first;
        for index in at + 1..at + length {
            in_run_deltas.push(document_ids[index] - document_ids[index - 1]);
        }
        at += length;
    }
    let mut out = Vec::with_capacity(document_ids.len() * 2);
    push_varint(&mut out, document_ids.len() as u64);
    push_varint(&mut out, runs.len() as u64);
    push_varint(&mut out, u64::from(max_impact));
    out.extend_from_slice(&max_rank.to_bits().to_le_bytes());
    for length in &runs {
        push_varint(&mut out, *length as u64);
    }
    let mut stream = Vec::new();
    for values in [first_deltas.as_slice(), in_run_deltas.as_slice()] {
        stream.clear();
        pack_bp128_stream(values, &mut stream);
        push_varint(&mut out, stream.len() as u64);
        out.extend_from_slice(&stream);
    }
    out
}

/// Inverse of [`encode_slim_impact_block`]: (ids in sidecar order,
/// max_impact, max_rank).
pub(crate) fn decode_slim_impact_block(bytes: &[u8]) -> Result<(Vec<u64>, u16, f32)> {
    use crate::paged_collection::{read_varint as pc_read_varint, zigzag_decode};
    let corrupt = || segment_error("corrupt slim impact block");
    let mut cursor = 0usize;
    let count = pc_read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as usize;
    let run_count = pc_read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as usize;
    let max_impact = u16::try_from(pc_read_varint(bytes, &mut cursor).ok_or_else(corrupt)?)
        .map_err(|_| corrupt())?;
    let rank_bits = bytes
        .get(cursor..cursor + 4)
        .ok_or_else(corrupt)?
        .try_into()
        .expect("4 bytes");
    cursor += 4;
    let max_rank = f32::from_bits(u32::from_le_bytes(rank_bits));
    // `run_count` and `count` are segment bytes: bound both against what the
    // remaining input could encode (a run length costs at least one varint
    // byte) before either sizes an allocation.
    let remaining = bytes.len().saturating_sub(cursor);
    if run_count > remaining || count > remaining.saturating_mul(BP128_STREAM_MAX_PER_BYTE) {
        return Err(corrupt());
    }
    let mut run_lengths = Vec::new();
    run_lengths.try_reserve(run_count).map_err(|_| corrupt())?;
    let mut total = 0usize;
    for _ in 0..run_count {
        let length = pc_read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as usize;
        if length == 0 {
            return Err(corrupt());
        }
        total += length;
        run_lengths.push(length);
    }
    if total != count {
        return Err(corrupt());
    }
    let mut first_deltas = Vec::new();
    let mut in_run_deltas = Vec::new();
    for (index, target) in [(0usize, &mut first_deltas), (1, &mut in_run_deltas)] {
        let expected = if index == 0 {
            run_count
        } else {
            count - run_count
        };
        let length = pc_read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as usize;
        let stream = bytes
            .get(cursor..cursor.checked_add(length).ok_or_else(corrupt)?)
            .ok_or_else(corrupt)?;
        unpack_bp128_stream(stream, expected, target).ok_or_else(corrupt)?;
        cursor += length;
    }
    if cursor != bytes.len() {
        return Err(corrupt());
    }
    let mut ids = Vec::with_capacity(count);
    let mut previous_first = 0i64;
    let mut gap_at = 0usize;
    for (run, length) in run_lengths.iter().enumerate() {
        let first = previous_first + zigzag_decode(first_deltas[run]);
        previous_first = first;
        let mut current = u64::try_from(first).map_err(|_| corrupt())?;
        ids.push(current);
        for _ in 1..*length {
            current += in_run_deltas[gap_at];
            gap_at += 1;
            ids.push(current);
        }
    }
    Ok((ids, max_impact, max_rank))
}

/// Decode a slim block, rehydrating per-document fields from `docs`.
pub(crate) fn decode_slim_pk_block(
    bytes: &[u8],
    docs: &Docs<'_>,
) -> Result<Vec<crate::paged_collection::NumericBlockPosting>> {
    use crate::paged_collection::{read_varint as pc_read_varint, zigzag_decode};
    let corrupt = || segment_error("corrupt slim posting block");
    let mut cursor = 0usize;
    let count = pc_read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as usize;
    if count == 1 {
        let document_id = pc_read_varint(bytes, &mut cursor).ok_or_else(corrupt)?;
        let positions_len = pc_read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as usize;
        let mut packed_positions = Vec::with_capacity(positions_len);
        if positions_len > 0 {
            let first = bytes
                .get(cursor..cursor + 2)
                .ok_or_else(corrupt)?
                .try_into()
                .expect("2 bytes");
            cursor += 2;
            let mut current = i64::from(u16::from_le_bytes(first));
            packed_positions.push(u16::try_from(current).map_err(|_| corrupt())?);
            for _ in 1..positions_len {
                current += zigzag_decode(pc_read_varint(bytes, &mut cursor).ok_or_else(corrupt)?);
                packed_positions.push(u16::try_from(current).map_err(|_| corrupt())?);
            }
        }
        if cursor != bytes.len() {
            return Err(corrupt());
        }
        let (doc_length, doc_distinct) = docs.doc(document_id)?;
        return Ok(vec![crate::paged_collection::NumericBlockPosting {
            document_id,
            doc_length,
            doc_distinct,
            packed_positions,
        }]);
    }
    let mut identifiers = Vec::new();
    let mut position_counts = Vec::new();
    let mut first_positions = Vec::new();
    let mut gap_deltas = Vec::new();
    let mut streams: [(&mut Vec<u64>, Option<usize>); 4] = [
        (&mut identifiers, Some(count)),
        (&mut position_counts, Some(count)),
        (&mut first_positions, None),
        (&mut gap_deltas, None),
    ];
    let mut with_positions = 0usize;
    let mut total_gaps = 0usize;
    for (index, (target, expected)) in streams.iter_mut().enumerate() {
        let length = pc_read_varint(bytes, &mut cursor).ok_or_else(corrupt)? as usize;
        let stream = bytes
            .get(cursor..cursor.checked_add(length).ok_or_else(corrupt)?)
            .ok_or_else(corrupt)?;
        let expected = match expected {
            Some(expected) => *expected,
            // Stream sizes for firsts/gaps derive from the counts stream,
            // which is decoded by the time they are reached.
            None if index == 2 => with_positions,
            None => total_gaps,
        };
        unpack_bp128_stream(stream, expected, target).ok_or_else(corrupt)?;
        cursor += length;
        if index == 1 {
            with_positions = target.iter().filter(|value| **value > 0).count();
            total_gaps = target
                .iter()
                .map(|value| (*value as usize).saturating_sub(1))
                .sum();
        }
    }
    if cursor != bytes.len() {
        return Err(corrupt());
    }
    let mut postings = Vec::with_capacity(count);
    let mut document_id = 0u64;
    let mut first_at = 0usize;
    let mut gap_at = 0usize;
    for index in 0..count {
        document_id += identifiers[index];
        let positions_len = position_counts[index] as usize;
        let mut packed_positions = Vec::with_capacity(positions_len);
        if positions_len > 0 {
            let first = i64::try_from(*first_positions.get(first_at).ok_or_else(corrupt)?)
                .map_err(|_| corrupt())?;
            first_at += 1;
            let mut current = first;
            packed_positions.push(u16::try_from(current).map_err(|_| corrupt())?);
            for _ in 1..positions_len {
                current += zigzag_decode(*gap_deltas.get(gap_at).ok_or_else(corrupt)?);
                gap_at += 1;
                packed_positions.push(u16::try_from(current).map_err(|_| corrupt())?);
            }
        }
        let (doc_length, doc_distinct) = docs.doc(document_id)?;
        postings.push(crate::paged_collection::NumericBlockPosting {
            document_id,
            doc_length,
            doc_distinct,
            packed_positions,
        });
    }
    Ok(postings)
}

// ---------------------------------------------------------------------------
// manifest
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ManifestFile {
    name: String,
    bytes: u64,
    sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SegmentManifest {
    format: u32,
    term_count: u64,
    files: Vec<ManifestFile>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct PhaseState {
    bytes: u64,
    sha256: String,
    #[serde(default)]
    docs_bytes: u64,
    #[serde(default)]
    docs_sha256: String,
    /// Bytes each parallel range-merge part contributed to the data file;
    /// empty for a sequential merge. Publish uses these to rebase the
    /// part-local block offsets recorded in the part tmp files.
    #[serde(default)]
    part_data_bytes: Vec<u64>,
}

fn write_json_atomic(path: &Path, value: &impl Serialize, fsync: bool) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(|error| segment_error(format!("segment state encode failed: {error}")))?;
    crate::storage::write_atomic(path, &bytes, fsync)
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let bytes = fs::read(path)
        .map_err(|error| segment_error(format!("cannot read {}: {error}", path.display())))?;
    serde_json::from_slice(&bytes)
        .map_err(|error| segment_error(format!("cannot parse {}: {error}", path.display())))
}

/// Buffered writer that hashes and counts everything written through it.
struct HashedWriter {
    inner: BufWriter<File>,
    hasher: Sha256,
    written: u64,
}

impl HashedWriter {
    fn create(path: &Path) -> Result<Self> {
        let file = File::create(path)
            .map_err(|error| segment_error(format!("cannot create {}: {error}", path.display())))?;
        Ok(Self {
            inner: BufWriter::new(file),
            hasher: Sha256::new(),
            written: 0,
        })
    }

    fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        self.inner
            .write_all(bytes)
            .map_err(|error| segment_error(format!("segment write failed: {error}")))?;
        self.hasher.update(bytes);
        self.written += bytes.len() as u64;
        Ok(())
    }

    fn finish(mut self, fsync: bool) -> Result<PhaseState> {
        self.inner
            .flush()
            .map_err(|error| segment_error(format!("segment flush failed: {error}")))?;
        if fsync {
            self.inner
                .get_ref()
                .sync_all()
                .map_err(|error| segment_error(format!("segment fsync failed: {error}")))?;
        }
        Ok(PhaseState {
            bytes: self.written,
            sha256: hex::encode(self.hasher.finalize()),
            docs_bytes: 0,
            docs_sha256: String::new(),
            part_data_bytes: Vec::new(),
        })
    }
}

// ---------------------------------------------------------------------------
// writer
// ---------------------------------------------------------------------------

/// Streaming two-phase segment writer.
///
/// The build's external merge produces blocks in sorted term order twice —
/// once document-id-ordered, once impact-ordered — as two separate phases
/// that may be separated by a process restart. Each phase therefore streams
/// to its own data file plus a temporary per-term metadata file, and
/// `publish` zips the two metadata streams (both in the same term order)
/// into the final directory. Re-entering a phase truncates and redoes it,
/// which is the same restart contract the build checkpoint already gives
/// every other phase.
pub(crate) struct FtsSegmentWriter {
    dir: PathBuf,
    fsync: bool,
    data: HashedWriter,
    meta: BufWriter<File>,
    order_impact: bool,
    current_term: Option<Vec<u8>>,
    // Current-term accumulators.
    block_metas: Vec<u8>,
    block_count: u64,
    previous_last_doc: u64,
    term_base: u64,
    terms_written: u64,
    /// doc_id -> (doc_length | doc_distinct << 32), dense. Written to
    /// `docs.dat` when the pk phase finishes. The per-posting copies v1
    /// carried multiplied these 8 bytes by the document's term count.
    docs: Vec<u64>,
}

/// Per-term statistics the pk pass computes while merging.
pub(crate) struct PkTermStats {
    pub document_frequency: u64,
    pub collection_frequency: u64,
    pub maximum_contribution: f32,
    pub first_doc_id: Option<u64>,
    pub last_doc_id: Option<u64>,
}

impl FtsSegmentWriter {
    /// Begin (or restart) the pk phase for `physical` under `segments_root`.
    pub(crate) fn begin_pk(
        segments_root: &Path,
        physical: &str,
        fsync: bool,
        document_count: u64,
    ) -> Result<Self> {
        let dir = segments_root.join(physical);
        fs::create_dir_all(&dir)
            .map_err(|error| segment_error(format!("cannot create segment dir: {error}")))?;
        // Restarting the phase invalidates everything downstream of it.
        for stale in [TERMS_FILE, MANIFEST_FILE, PK_STATE_FILE, DOCS_FILE] {
            let _ = fs::remove_file(dir.join(stale));
        }
        let documents = usize::try_from(document_count)
            .map_err(|_| segment_error("document count exceeds this platform"))?;
        let data = HashedWriter::create(&dir.join(POSTINGS_FILE))?;
        let meta = BufWriter::new(File::create(dir.join(TERMS_PK_TMP)).map_err(|error| {
            segment_error(format!("cannot create segment term metadata: {error}"))
        })?);
        Ok(Self {
            dir,
            fsync,
            data,
            meta,
            order_impact: false,
            current_term: None,
            block_metas: Vec::new(),
            block_count: 0,
            previous_last_doc: 0,
            term_base: 0,
            terms_written: 0,
            docs: vec![0u64; documents],
        })
    }

    /// Begin the pk side of PART `part` of a parallel range merge: its own
    /// data file and term-metadata tmp, assembled into the final files by
    /// [`assemble_parts`]. Part writers keep no docs table — each returns
    /// its own and the assembly merges them.
    pub(crate) fn begin_pk_part(
        segments_root: &Path,
        physical: &str,
        fsync: bool,
        document_count: u64,
        part: usize,
    ) -> Result<Self> {
        let dir = segments_root.join(physical);
        fs::create_dir_all(&dir)
            .map_err(|error| segment_error(format!("cannot create segment dir: {error}")))?;
        if part == 0 {
            for stale in [TERMS_FILE, MANIFEST_FILE, PK_STATE_FILE, DOCS_FILE] {
                let _ = fs::remove_file(dir.join(stale));
            }
        }
        let documents = usize::try_from(document_count)
            .map_err(|_| segment_error("document count exceeds this platform"))?;
        let data = HashedWriter::create(&dir.join(part_file_name(POSTINGS_FILE, part)))?;
        let meta = BufWriter::new(
            File::create(dir.join(part_file_name(TERMS_PK_TMP, part))).map_err(|error| {
                segment_error(format!("cannot create segment term metadata: {error}"))
            })?,
        );
        Ok(Self {
            dir,
            fsync,
            data,
            meta,
            order_impact: false,
            current_term: None,
            block_metas: Vec::new(),
            block_count: 0,
            previous_last_doc: 0,
            term_base: 0,
            terms_written: 0,
            docs: vec![0u64; documents],
        })
    }

    /// Impact-side sibling of [`Self::begin_pk_part`].
    pub(crate) fn begin_impact_part(
        segments_root: &Path,
        physical: &str,
        fsync: bool,
        part: usize,
    ) -> Result<Self> {
        let dir = segments_root.join(physical);
        if part == 0 {
            for stale in [TERMS_FILE, MANIFEST_FILE, IMPACT_STATE_FILE] {
                let _ = fs::remove_file(dir.join(stale));
            }
        }
        let data = HashedWriter::create(&dir.join(part_file_name(IMPACTS_FILE, part)))?;
        let meta = BufWriter::new(
            File::create(dir.join(part_file_name(TERMS_IMPACT_TMP, part))).map_err(|error| {
                segment_error(format!("cannot create segment term metadata: {error}"))
            })?,
        );
        Ok(Self {
            dir,
            fsync,
            data,
            meta,
            order_impact: true,
            current_term: None,
            block_metas: Vec::new(),
            block_count: 0,
            previous_last_doc: 0,
            term_base: 0,
            terms_written: 0,
            docs: Vec::new(),
        })
    }

    /// Close a part writer: flush data and metadata, hand back the docs
    /// table for merging. No state json — [`assemble_parts`] writes it once
    /// the final files exist.
    pub(crate) fn finish_part(mut self) -> Result<Vec<u64>> {
        report_slim_stats();
        if self.current_term.is_some() {
            return Err(segment_error("segment part finished mid-term"));
        }
        self.meta
            .flush()
            .map_err(|error| segment_error(format!("segment metadata flush failed: {error}")))?;
        // Parts are transient: fsync happens once on the assembled files.
        let _ = self.data.finish(false)?;
        Ok(std::mem::take(&mut self.docs))
    }

    /// Record a term whose whole sidecar is elided: one tmp record with zero
    /// blocks, no bytes written. The reader resynthesizes from the pk block.
    pub(crate) fn record_elided_impact_term(&mut self, term: &[u8]) -> Result<()> {
        debug_assert!(self.order_impact);
        self.start_term(term)?;
        self.end_impact_term(term)
    }

    /// Begin the impact side of the single-pass build: the merge generates
    /// both orders together and decides elision itself.
    pub(crate) fn begin_impact_single_pass(
        segments_root: &Path,
        physical: &str,
        fsync: bool,
    ) -> Result<Self> {
        let dir = segments_root.join(physical);
        for stale in [TERMS_FILE, MANIFEST_FILE, IMPACT_STATE_FILE] {
            let _ = fs::remove_file(dir.join(stale));
        }
        let data = HashedWriter::create(&dir.join(IMPACTS_FILE))?;
        let meta = BufWriter::new(File::create(dir.join(TERMS_IMPACT_TMP)).map_err(|error| {
            segment_error(format!("cannot create segment term metadata: {error}"))
        })?);
        Ok(Self {
            dir,
            fsync,
            data,
            meta,
            order_impact: true,
            current_term: None,
            block_metas: Vec::new(),
            block_count: 0,
            previous_last_doc: 0,
            term_base: 0,
            terms_written: 0,
            docs: Vec::new(),
        })
    }

    fn start_term(&mut self, term: &[u8]) -> Result<()> {
        if let Some(current) = &self.current_term {
            if current.as_slice() >= term {
                return Err(segment_error(
                    "segment terms must arrive in strictly ascending order",
                ));
            }
        }
        self.current_term = Some(term.to_vec());
        self.block_metas.clear();
        self.block_count = 0;
        self.previous_last_doc = 0;
        self.term_base = self.data.written;
        Ok(())
    }

    /// Append one pk-ordered block from its decoded postings. Stores the slim
    /// form and records the per-document fields; returns the stored length.
    ///
    /// `max_term_frequency` uses the SAME formula the v1 encoder writes into
    /// its rank header, so the boundary stream served from term metadata is
    /// indistinguishable from one read out of v1 value prefixes.
    pub(crate) fn append_pk_postings(
        &mut self,
        term: &[u8],
        postings: &[crate::paged_collection::NumericBlockPosting],
        max_impact: u16,
        max_rank: f32,
    ) -> Result<u64> {
        debug_assert!(!self.order_impact);
        let last_document_id = postings
            .last()
            .ok_or_else(|| segment_error("empty posting block"))?
            .document_id;
        if self.current_term.as_deref() != Some(term) {
            self.start_term(term)?;
        }
        let mut max_term_frequency = 1usize;
        let mut weight_mask = 0u8;
        for posting in postings {
            max_term_frequency = max_term_frequency.max(posting.packed_positions.len().max(1));
            if posting.packed_positions.is_empty() {
                weight_mask |= 1;
            } else {
                for packed in &posting.packed_positions {
                    weight_mask |= 1 << ((packed >> 14) & 0x3);
                }
            }
            let index = usize::try_from(posting.document_id)
                .map_err(|_| segment_error("document id exceeds this platform"))?;
            let slot = self.docs.get_mut(index).ok_or_else(|| {
                segment_error("posting references a document beyond the checkpoint count")
            })?;
            *slot = u64::from(posting.doc_length) | (u64::from(posting.doc_distinct) << 32);
        }
        let bytes = encode_slim_pk_block(postings);
        let delta = last_document_id
            .checked_sub(self.previous_last_doc)
            .ok_or_else(|| segment_error("posting blocks must ascend by document id"))?;
        self.previous_last_doc = last_document_id;
        write_varint(&mut self.block_metas, delta);
        write_varint(&mut self.block_metas, bytes.len() as u64);
        write_varint(&mut self.block_metas, max_term_frequency as u64);
        write_varint(&mut self.block_metas, u64::from(max_impact));
        self.block_metas
            .extend_from_slice(&max_rank.to_bits().to_le_bytes());
        self.block_metas.push(weight_mask);
        self.block_count += 1;
        self.data.write_all(&bytes)?;
        Ok(bytes.len() as u64)
    }

    /// Append one impact-ordered block from its id sequence, storing the
    /// slim run-split form. The reader re-encodes exact v1 bytes.
    pub(crate) fn append_impact_ids(
        &mut self,
        term: &[u8],
        document_ids: &[u64],
        max_impact: u16,
        max_rank: f32,
    ) -> Result<()> {
        debug_assert!(self.order_impact);
        if self.current_term.as_deref() != Some(term) {
            self.start_term(term)?;
        }
        let bytes = encode_slim_impact_block(document_ids, max_impact, max_rank);
        write_varint(&mut self.block_metas, bytes.len() as u64);
        self.block_count += 1;
        self.data.write_all(&bytes)
    }

    /// Close the current term on the pk pass with its merged statistics.
    pub(crate) fn end_pk_term(&mut self, term: &[u8], stats: &PkTermStats) -> Result<()> {
        debug_assert!(!self.order_impact);
        if self.current_term.as_deref() != Some(term) {
            // A term with zero blocks cannot occur: the merge only finishes
            // terms it saw postings for.
            return Err(segment_error("pk term finished without any blocks"));
        }
        let mut record = Vec::with_capacity(self.block_metas.len() + term.len() + 64);
        write_varint(&mut record, term.len() as u64);
        record.extend_from_slice(term);
        write_varint(&mut record, stats.document_frequency);
        write_varint(&mut record, stats.collection_frequency);
        record.extend_from_slice(&stats.maximum_contribution.to_le_bytes());
        write_varint(&mut record, stats.first_doc_id.unwrap_or(0));
        write_varint(&mut record, stats.last_doc_id.unwrap_or(0));
        write_varint(&mut record, self.term_base);
        write_varint(&mut record, self.block_count);
        record.extend_from_slice(&self.block_metas);
        self.meta
            .write_all(&record)
            .map_err(|error| segment_error(format!("segment metadata write failed: {error}")))?;
        self.terms_written += 1;
        self.current_term = None;
        Ok(())
    }

    /// Close the current term on the impact pass.
    pub(crate) fn end_impact_term(&mut self, term: &[u8]) -> Result<()> {
        debug_assert!(self.order_impact);
        if self.current_term.as_deref() != Some(term) {
            return Err(segment_error("impact term finished without any blocks"));
        }
        let mut record = Vec::with_capacity(self.block_metas.len() + term.len() + 24);
        write_varint(&mut record, term.len() as u64);
        record.extend_from_slice(term);
        write_varint(&mut record, self.term_base);
        write_varint(&mut record, self.block_count);
        record.extend_from_slice(&self.block_metas);
        self.meta
            .write_all(&record)
            .map_err(|error| segment_error(format!("segment metadata write failed: {error}")))?;
        self.terms_written += 1;
        self.current_term = None;
        Ok(())
    }

    /// Finish the current phase durably.
    pub(crate) fn finish_phase(mut self) -> Result<()> {
        if self.current_term.is_some() {
            return Err(segment_error("segment phase finished mid-term"));
        }
        self.meta
            .flush()
            .map_err(|error| segment_error(format!("segment metadata flush failed: {error}")))?;
        if self.fsync {
            self.meta.get_ref().sync_all().map_err(|error| {
                segment_error(format!("segment metadata fsync failed: {error}"))
            })?;
        }
        let state_file = if self.order_impact {
            IMPACT_STATE_FILE
        } else {
            PK_STATE_FILE
        };
        let mut state = self.data.finish(self.fsync)?;
        if !self.order_impact {
            let mut docs = HashedWriter::create(&self.dir.join(DOCS_FILE))?;
            let mut buffer = Vec::with_capacity(8 * 1024);
            for packed in &self.docs {
                buffer.extend_from_slice(&packed.to_le_bytes());
                if buffer.len() >= 8 * 1024 {
                    docs.write_all(&buffer)?;
                    buffer.clear();
                }
            }
            if !buffer.is_empty() {
                docs.write_all(&buffer)?;
            }
            let docs_state = docs.finish(self.fsync)?;
            state.docs_bytes = docs_state.bytes;
            state.docs_sha256 = docs_state.sha256;
        }
        write_json_atomic(&self.dir.join(state_file), &state, self.fsync)?;
        Ok(())
    }
}

/// Assemble a parallel range merge: concatenate the part data files into
/// the final `postings.dat`/`impacts.dat` (hashing while copying), write the
/// merged docs table, emit both state jsons, and record how many bytes each
/// part contributed so publish can rebase the part-local offsets in the
/// term metadata. Part files are removed on success.
pub(crate) fn assemble_parts(
    segments_root: &Path,
    physical: &str,
    parts: usize,
    docs: Vec<u64>,
    fsync: bool,
) -> Result<()> {
    let dir = segments_root.join(physical);
    let concat = |base: &str| -> Result<(PhaseState, Vec<u64>)> {
        let mut out = HashedWriter::create(&dir.join(base))?;
        let mut part_bytes = Vec::with_capacity(parts);
        for part in 0..parts {
            let path = dir.join(part_file_name(base, part));
            let mut file = File::open(&path).map_err(|error| {
                segment_error(format!("cannot open segment part `{base}` {part}: {error}"))
            })?;
            let before = out.written;
            let mut buffer = [0u8; 256 * 1024];
            loop {
                let read = file
                    .read(&mut buffer)
                    .map_err(|error| segment_error(format!("segment part read failed: {error}")))?;
                if read == 0 {
                    break;
                }
                out.write_all(&buffer[..read])?;
            }
            part_bytes.push(out.written - before);
        }
        Ok((out.finish(fsync)?, part_bytes))
    };
    let (mut pk_state, pk_part_bytes) = concat(POSTINGS_FILE)?;
    let (impact_state, impact_part_bytes) = concat(IMPACTS_FILE)?;

    let mut docs_writer = HashedWriter::create(&dir.join(DOCS_FILE))?;
    let mut buffer = Vec::with_capacity(8 * 1024);
    for packed in &docs {
        buffer.extend_from_slice(&packed.to_le_bytes());
        if buffer.len() >= 8 * 1024 {
            docs_writer.write_all(&buffer)?;
            buffer.clear();
        }
    }
    if !buffer.is_empty() {
        docs_writer.write_all(&buffer)?;
    }
    let docs_state = docs_writer.finish(fsync)?;
    pk_state.docs_bytes = docs_state.bytes;
    pk_state.docs_sha256 = docs_state.sha256;
    pk_state.part_data_bytes = pk_part_bytes;
    let impact_state = PhaseState {
        part_data_bytes: impact_part_bytes,
        ..impact_state
    };
    write_json_atomic(&dir.join(PK_STATE_FILE), &pk_state, fsync)?;
    write_json_atomic(&dir.join(IMPACT_STATE_FILE), &impact_state, fsync)?;
    for part in 0..parts {
        let _ = fs::remove_file(dir.join(part_file_name(POSTINGS_FILE, part)));
        let _ = fs::remove_file(dir.join(part_file_name(IMPACTS_FILE, part)));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// publish: zip the two metadata passes into terms.dat + manifest
// ---------------------------------------------------------------------------

struct TmpReader {
    bytes: Vec<u8>,
    at: usize,
}

impl TmpReader {
    fn open(path: &Path) -> Result<Self> {
        let mut bytes = Vec::new();
        File::open(path)
            .map_err(|error| segment_error(format!("cannot open {}: {error}", path.display())))?
            .read_to_end(&mut bytes)
            .map_err(|error| segment_error(format!("cannot read {}: {error}", path.display())))?;
        Ok(Self { bytes, at: 0 })
    }

    fn done(&self) -> bool {
        self.at >= self.bytes.len()
    }
}

/// Publish the segment: merge the pk and impact metadata streams — which the
/// two passes produced in the same sorted term order — into the final
/// front-coded directory, then write the checksummed manifest.
pub(crate) fn publish_segment(segments_root: &Path, physical: &str, fsync: bool) -> Result<u64> {
    let dir = segments_root.join(physical);
    // Idempotent: a crash between a completed publish and the generation
    // flip re-enters this phase with the temporaries already consumed. The
    // manifest is the durable record of that completion.
    if dir.join(MANIFEST_FILE).exists() && !dir.join(PK_STATE_FILE).exists() {
        let manifest: SegmentManifest = read_json(&dir.join(MANIFEST_FILE))?;
        return Ok(manifest.term_count);
    }
    let pk_state: PhaseState = read_json(&dir.join(PK_STATE_FILE))?;
    let impact_state: PhaseState = read_json(&dir.join(IMPACT_STATE_FILE))?;
    // A parallel range merge leaves one tmp pair per part, each recording
    // PART-LOCAL data offsets; the assembly recorded how many data bytes
    // each part contributed, which rebases them here. A sequential merge is
    // the one-part special case with zero offsets.
    let tmp_pairs: Vec<(String, String, u64, u64)> = if pk_state.part_data_bytes.is_empty() {
        vec![(TERMS_PK_TMP.to_string(), TERMS_IMPACT_TMP.to_string(), 0, 0)]
    } else {
        if impact_state.part_data_bytes.len() != pk_state.part_data_bytes.len() {
            return Err(segment_error(
                "segment part manifests disagree; the build is torn",
            ));
        }
        let mut pairs = Vec::with_capacity(pk_state.part_data_bytes.len());
        let mut pk_offset = 0u64;
        let mut impact_offset = 0u64;
        for part in 0..pk_state.part_data_bytes.len() {
            pairs.push((
                part_file_name(TERMS_PK_TMP, part),
                part_file_name(TERMS_IMPACT_TMP, part),
                pk_offset,
                impact_offset,
            ));
            pk_offset += pk_state.part_data_bytes[part];
            impact_offset += impact_state.part_data_bytes[part];
        }
        pairs
    };

    let mut terms = HashedWriter::create(&dir.join(TERMS_FILE))?;
    let mut restarts: Vec<u64> = Vec::new();
    let mut previous_term: Vec<u8> = Vec::new();
    let mut term_count = 0u64;
    let mut record = Vec::with_capacity(256);

    // Bases are implicit: term N's blocks start where term N-1's ended.
    // Restart records carry the absolute bases so binary search can anchor;
    // every other term derives them while the reader walks the window. The
    // tmp files still record per-term bases, which lets this zip VERIFY the
    // accumulation instead of trusting it.
    let mut running_pk_base = 0u64;
    let mut running_impact_base = 0u64;
    for (pk_tmp, impact_tmp, part_pk_offset, part_impact_offset) in &tmp_pairs {
        let mut pk = TmpReader::open(&dir.join(pk_tmp))?;
        let mut impact = TmpReader::open(&dir.join(impact_tmp))?;
        while !pk.done() {
            // pk record
            let term_len = read_varint(&pk.bytes, &mut pk.at)? as usize;
            let term_end = pk.at + term_len;
            let term = pk
                .bytes
                .get(pk.at..term_end)
                .ok_or_else(|| segment_error("pk term metadata is truncated"))?
                .to_vec();
            pk.at = term_end;
            let df = read_varint(&pk.bytes, &mut pk.at)?;
            let cf = read_varint(&pk.bytes, &mut pk.at)?;
            let max_contribution = &pk.bytes[pk.at..pk.at + 4];
            let max_contribution: [u8; 4] = max_contribution.try_into().expect("4 bytes");
            pk.at += 4;
            let first = read_varint(&pk.bytes, &mut pk.at)?;
            let last = read_varint(&pk.bytes, &mut pk.at)?;
            let base = read_varint(&pk.bytes, &mut pk.at)? + part_pk_offset;
            if base != running_pk_base {
                return Err(segment_error(
                    "segment pk metadata does not accumulate; the build is torn",
                ));
            }
            let block_count = read_varint(&pk.bytes, &mut pk.at)?;
            let mut pk_metas = Vec::with_capacity(64);
            let mut pk_bytes_total = 0u64;
            let mut derived_last = 0u64;
            // Single-block terms drop the stored v1 header pair — the reader
            // recomputes it from the postings once and the block cache holds it.
            // Multi-block terms keep it so scattered probes stay computation-free.
            let keep_headers = block_count > 1;
            for _ in 0..block_count {
                let delta = read_varint(&pk.bytes, &mut pk.at)?;
                let len = read_varint(&pk.bytes, &mut pk.at)?;
                let tf = read_varint(&pk.bytes, &mut pk.at)?;
                let max_impact = read_varint(&pk.bytes, &mut pk.at)?;
                let rank_bits = pk
                    .bytes
                    .get(pk.at..pk.at + 4)
                    .ok_or_else(|| segment_error("pk term metadata is truncated"))?;
                let rank_bits: [u8; 4] = rank_bits.try_into().expect("4 bytes");
                pk.at += 4;
                let weight_mask = *pk
                    .bytes
                    .get(pk.at)
                    .ok_or_else(|| segment_error("pk term metadata is truncated"))?;
                pk.at += 1;
                write_varint(&mut pk_metas, delta);
                write_varint(&mut pk_metas, len);
                write_varint(&mut pk_metas, tf);
                if keep_headers {
                    write_varint(&mut pk_metas, max_impact);
                    pk_metas.extend_from_slice(&rank_bits);
                    pk_metas.push(weight_mask);
                }
                pk_bytes_total += len;
                derived_last += delta;
            }
            // The reader reconstructs both bounds: last from the delta chain,
            // first from the span. Verify the reconstruction against the
            // absolutes the pk pass recorded before the absolutes are dropped.
            if derived_last != last {
                return Err(segment_error(
                    "segment pk block deltas do not reach the recorded last \
                 document; the build is torn",
                ));
            }
            let span = last.checked_sub(first).ok_or_else(|| {
                segment_error("segment pk term bounds are inverted; the build is torn")
            })?;

            // matching impact record — the passes merged the same runs, so the
            // term sets are identical and identically ordered. A mismatch means a
            // torn phase, and it is refused rather than skipped.
            if impact.done() {
                return Err(segment_error("impact metadata ended before pk metadata"));
            }
            let impact_term_len = read_varint(&impact.bytes, &mut impact.at)? as usize;
            let impact_term_end = impact.at + impact_term_len;
            let impact_term = impact
                .bytes
                .get(impact.at..impact_term_end)
                .ok_or_else(|| segment_error("impact term metadata is truncated"))?;
            if impact_term != term.as_slice() {
                return Err(segment_error(
                    "segment pk and impact passes disagree about the term order; \
                 the build must be restarted",
                ));
            }
            impact.at = impact_term_end;
            let impact_base = read_varint(&impact.bytes, &mut impact.at)? + part_impact_offset;
            if impact_base != running_impact_base {
                return Err(segment_error(
                    "segment impact metadata does not accumulate; the build is torn",
                ));
            }
            let impact_blocks = read_varint(&impact.bytes, &mut impact.at)?;
            let mut impact_metas = Vec::with_capacity(16);
            let mut impact_bytes_total = 0u64;
            for _ in 0..impact_blocks {
                let len = read_varint(&impact.bytes, &mut impact.at)?;
                write_varint(&mut impact_metas, len);
                impact_bytes_total += len;
            }

            // front-code the term against its predecessor, restarting the prefix
            // chain on the restart interval so binary search has anchors.
            let restart = term_count as usize % TERM_RESTART_INTERVAL == 0;
            if restart {
                restarts.push(terms.written);
            }
            let lcp = if restart {
                0
            } else {
                previous_term
                    .iter()
                    .zip(term.iter())
                    .take_while(|(a, b)| a == b)
                    .count()
            };
            record.clear();
            write_varint(&mut record, lcp as u64);
            write_varint(&mut record, (term.len() - lcp) as u64);
            record.extend_from_slice(&term[lcp..]);
            if restart {
                // Only restart anchors carry absolute bases; the window walk
                // derives every other term's bases by accumulating block lengths.
                write_varint(&mut record, running_pk_base);
                write_varint(&mut record, running_impact_base);
            }
            write_varint(&mut record, df);
            write_varint(&mut record, cf);
            record.push(quantize_contribution(f32::from_le_bytes(max_contribution)));
            write_varint(&mut record, block_count);
            record.extend_from_slice(&pk_metas);
            // Span, not absolute: `first = last - span`, and last comes free
            // from the delta chain. Single-posting terms — the majority of any
            // real dictionary — store one zero byte here.
            write_varint(&mut record, span);
            write_varint(&mut record, impact_blocks);
            record.extend_from_slice(&impact_metas);
            terms.write_all(&record)?;
            running_pk_base += pk_bytes_total;
            running_impact_base += impact_bytes_total;
            previous_term = term;
            term_count += 1;
        }
        if !impact.done() {
            return Err(segment_error("pk metadata ended before impact metadata"));
        }
    }

    // footer: restart offsets, count, magic.
    let mut footer = Vec::with_capacity(restarts.len() * 8 + 12);
    for offset in &restarts {
        footer.extend_from_slice(&offset.to_le_bytes());
    }
    footer.extend_from_slice(&(restarts.len() as u32).to_le_bytes());
    footer.extend_from_slice(TERMS_FOOTER_MAGIC);
    terms.write_all(&footer)?;
    let terms_state = terms.finish(fsync)?;

    let manifest = SegmentManifest {
        format: FTS_SEGMENT_FORMAT,
        term_count,
        files: vec![
            ManifestFile {
                name: POSTINGS_FILE.to_string(),
                bytes: pk_state.bytes,
                sha256: pk_state.sha256.clone(),
            },
            ManifestFile {
                name: DOCS_FILE.to_string(),
                bytes: pk_state.docs_bytes,
                sha256: pk_state.docs_sha256.clone(),
            },
            ManifestFile {
                name: IMPACTS_FILE.to_string(),
                bytes: impact_state.bytes,
                sha256: impact_state.sha256,
            },
            ManifestFile {
                name: TERMS_FILE.to_string(),
                bytes: terms_state.bytes,
                sha256: terms_state.sha256,
            },
        ],
    };
    write_json_atomic(&dir.join(MANIFEST_FILE), &manifest, fsync)?;
    if fsync {
        if let Ok(handle) = File::open(&dir) {
            let _ = handle.sync_all();
        }
    }
    for (pk_tmp, impact_tmp, _, _) in &tmp_pairs {
        let _ = fs::remove_file(dir.join(pk_tmp));
        let _ = fs::remove_file(dir.join(impact_tmp));
    }
    for tmp in [
        TERMS_PK_TMP,
        TERMS_IMPACT_TMP,
        PK_STATE_FILE,
        IMPACT_STATE_FILE,
    ] {
        let _ = fs::remove_file(dir.join(tmp));
    }
    Ok(term_count)
}

/// The readable state of one physical index: one final segment, or — while
/// a progressive build is running — its published sub-segments in document-
/// range order. Sub-segments live at `<physical>/sub-NNNN/` and each is a
/// complete ordinary segment; the invariant that makes the set composable
/// is that their document-id ranges are DISJOINT and ASCENDING in sub
/// order, so per-term postings, boundaries and statistics all combine by
/// concatenation or summation.
pub(crate) struct SegmentSet {
    readers: Vec<Arc<FtsSegmentReader>>,
}

impl std::fmt::Debug for SegmentSet {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "SegmentSet({} readers)", self.readers.len())
    }
}

impl SegmentSet {
    pub(crate) fn open(segments_root: &Path, physical: &str) -> Result<Option<Self>> {
        // A final segment always wins: the progressive build deletes its
        // sub-segments only after the final manifest is durable, so both
        // present = final is authoritative.
        if let Some(reader) = FtsSegmentReader::open(segments_root, physical)? {
            return Ok(Some(Self {
                readers: vec![Arc::new(reader)],
            }));
        }
        let dir = segments_root.join(physical);
        let mut subs: Vec<(u32, PathBuf)> = Vec::new();
        if let Ok(entries) = fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy().into_owned();
                if let Some(index) = name.strip_prefix("sub-").and_then(|n| n.parse().ok()) {
                    if entry.path().join(MANIFEST_FILE).exists() {
                        subs.push((index, entry.path()));
                    }
                }
            }
        }
        if subs.is_empty() {
            return Ok(None);
        }
        subs.sort_by_key(|(index, _)| *index);
        let mut readers = Vec::with_capacity(subs.len());
        for (index, _) in &subs {
            let reader =
                FtsSegmentReader::open(&dir, &format!("sub-{index:04}"))?.ok_or_else(|| {
                    segment_error(format!(
                        "progressive sub-segment {index} of `{physical}` vanished mid-open"
                    ))
                })?;
            readers.push(Arc::new(reader));
        }
        Ok(Some(Self { readers }))
    }

    pub(crate) fn readers(&self) -> &[Arc<FtsSegmentReader>] {
        &self.readers
    }

    /// The single reader of a finished build, if this is one.
    pub(crate) fn sole(&self) -> Option<&Arc<FtsSegmentReader>> {
        match self.readers.as_slice() {
            [reader] => Some(reader),
            _ => None,
        }
    }

    pub(crate) fn term_count(&self) -> u64 {
        self.readers.iter().map(|reader| reader.term_count).sum()
    }

    /// Combined per-term statistics across the set: sums for frequencies and
    /// block counts, extremes for bounds — exactly what the final merged
    /// segment would report for the same postings.
    pub(crate) fn term_statistics(&self, term: &[u8]) -> Result<Option<FullTextTermStatistics>> {
        let mut combined: Option<FullTextTermStatistics> = None;
        for reader in &self.readers {
            let Some(meta) = reader.term_meta(term)? else {
                continue;
            };
            let statistics = meta.statistics();
            combined = Some(match combined {
                None => statistics,
                Some(mut sum) => {
                    sum.document_frequency += statistics.document_frequency;
                    sum.collection_frequency += statistics.collection_frequency;
                    sum.posting_block_count += statistics.posting_block_count;
                    sum.impact_block_count += statistics.impact_block_count;
                    sum.posting_bytes += statistics.posting_bytes;
                    sum.impact_bytes += statistics.impact_bytes;
                    sum.first_doc_id = match (sum.first_doc_id, statistics.first_doc_id) {
                        (Some(a), Some(b)) => Some(a.min(b)),
                        (a, b) => a.or(b),
                    };
                    sum.last_doc_id = match (sum.last_doc_id, statistics.last_doc_id) {
                        (Some(a), Some(b)) => Some(a.max(b)),
                        (a, b) => a.or(b),
                    };
                    sum.maximum_contribution = sum
                        .maximum_contribution
                        .max(statistics.maximum_contribution);
                    sum
                }
            });
        }
        Ok(combined)
    }

    /// Bounded lexicographic dictionary scan over a published segment set.
    pub(crate) fn term_dictionary_page(
        &self,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, FullTextTermStatistics)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let lower = after.unwrap_or_default();
        let mut cursor = self.terms_from(lower)?;
        let mut entries = Vec::with_capacity(limit);
        while entries.len() < limit {
            let Some((term, metas)) = cursor.next_term()? else {
                break;
            };
            if after.is_some_and(|after| term.as_slice() <= after) {
                continue;
            }
            let mut combined = FullTextTermStatistics::default();
            for (_, meta) in metas {
                let statistics = meta.statistics();
                combined.document_frequency += statistics.document_frequency;
                combined.collection_frequency += statistics.collection_frequency;
                combined.posting_block_count += statistics.posting_block_count;
                combined.impact_block_count += statistics.impact_block_count;
                combined.posting_bytes += statistics.posting_bytes;
                combined.impact_bytes += statistics.impact_bytes;
                combined.first_doc_id = match (combined.first_doc_id, statistics.first_doc_id) {
                    (Some(left), Some(right)) => Some(left.min(right)),
                    (left, right) => left.or(right),
                };
                combined.last_doc_id = match (combined.last_doc_id, statistics.last_doc_id) {
                    (Some(left), Some(right)) => Some(left.max(right)),
                    (left, right) => left.or(right),
                };
                combined.maximum_contribution = combined
                    .maximum_contribution
                    .max(statistics.maximum_contribution);
            }
            entries.push((term, combined));
        }
        Ok(entries)
    }
}

/// k-way term cursor over a [`SegmentSet`]: yields each term once, in
/// ascending order, with every sub-segment's meta (paired with its reader)
/// in document-range order.
pub(crate) struct SetTermCursor {
    cursors: Vec<SegmentTermCursor>,
    pending: Vec<Option<(Vec<u8>, Arc<SegmentTermMeta>)>>,
}

impl SegmentSet {
    pub(crate) fn terms_from(&self, lower: &[u8]) -> Result<SetTermCursor> {
        let mut cursors = Vec::with_capacity(self.readers.len());
        let mut pending = Vec::with_capacity(self.readers.len());
        for reader in &self.readers {
            let mut cursor = reader.terms_from(lower)?;
            pending.push(cursor.next_term()?);
            cursors.push(cursor);
        }
        Ok(SetTermCursor { cursors, pending })
    }
}

impl SetTermCursor {
    /// The next term with its (reader, meta) pairs in sub order.
    pub(crate) fn next_term(
        &mut self,
    ) -> Result<Option<(Vec<u8>, Vec<(Arc<FtsSegmentReader>, Arc<SegmentTermMeta>)>)>> {
        let minimum = self
            .pending
            .iter()
            .filter_map(|slot| slot.as_ref().map(|(term, _)| term.clone()))
            .min();
        let Some(term) = minimum else {
            return Ok(None);
        };
        let mut metas = Vec::new();
        for index in 0..self.pending.len() {
            let matches = self.pending[index]
                .as_ref()
                .is_some_and(|(pending, _)| *pending == term);
            if matches {
                let (_, meta) = self.pending[index].take().expect("checked");
                metas.push((Arc::clone(self.cursors[index].reader()), meta));
                self.pending[index] = self.cursors[index].next_term()?;
            }
        }
        Ok(Some((term, metas)))
    }
}

/// Remove a physical index's progressive sub-segment directories, leaving
/// the final segment files in place.
pub(crate) fn remove_progressive_subs(segments_root: &Path, physical: &str) {
    let dir = segments_root.join(physical);
    if let Ok(entries) = fs::read_dir(&dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            if name.to_string_lossy().starts_with("sub-") {
                let _ = fs::remove_dir_all(entry.path());
            }
        }
    }
}

/// Remove a segment directory. Best-effort by design: the caller has already
/// made the removal safe (the generation is unpublished), and readers holding
/// open file handles keep them across the unlink.
pub(crate) fn remove_segment(segments_root: &Path, physical: &str) {
    let _ = fs::remove_dir_all(segments_root.join(physical));
}

// ---------------------------------------------------------------------------
// reader
// ---------------------------------------------------------------------------

/// Decoded per-term metadata: everything a query needs to locate blocks
/// without touching the data files.
#[derive(Clone, Debug)]
pub(crate) struct SegmentTermMeta {
    pub document_frequency: u64,
    pub collection_frequency: u64,
    pub maximum_contribution: f32,
    pub first_doc_id: u64,
    pub last_doc_id: u64,
    pk_base: u64,
    /// Per block: (last_document_id, byte offset from pk_base, byte length,
    /// max term frequency).
    pk_blocks: Arc<[(u64, u64, u32, u32)]>,
    /// Per block for MULTI-block terms: the v1 header's (max_impact,
    /// max_rank bits, weight_mask). Stored so the scattered-probe path —
    /// which touches hundreds of blocks of exactly these terms — serves
    /// header metadata without touching postings. Empty for single-block
    /// terms: their one-off recompute is cheap and the block cache keeps it.
    pk_headers: Arc<[(u16, u32, u8)]>,
    impact_base: u64,
    /// Per block: (byte offset from impact_base, byte length).
    impact_blocks: Arc<[(u64, u32)]>,
}

impl SegmentTermMeta {
    pub(crate) fn statistics(&self) -> FullTextTermStatistics {
        FullTextTermStatistics {
            document_frequency: self.document_frequency,
            collection_frequency: self.collection_frequency,
            posting_block_count: self.pk_blocks.len() as u32,
            // An empty impact list means the sidecar was elided, not that it
            // does not exist: the reader synthesizes exactly one block. The
            // logical count matters — a zero here declines the seeking path.
            impact_block_count: (self.impact_blocks.len() as u32).max(1),
            posting_bytes: self
                .pk_blocks
                .iter()
                .map(|(_, _, len, _)| u64::from(*len))
                .sum(),
            impact_bytes: self
                .impact_blocks
                .iter()
                .map(|(_, len)| u64::from(*len))
                .sum(),
            first_doc_id: Some(self.first_doc_id),
            last_doc_id: Some(self.last_doc_id),
            maximum_contribution: self.maximum_contribution,
        }
    }
}

/// An open packed segment. Immutable; safe to share across sessions.
impl std::fmt::Debug for FtsSegmentReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FtsSegmentReader")
            .field("term_count", &self.term_count)
            .finish_non_exhaustive()
    }
}

pub(crate) struct FtsSegmentReader {
    postings: File,
    impacts: File,
    postings_bytes: u64,
    impacts_bytes: u64,
    /// doc_id -> (doc_length, doc_distinct), 8 bytes LE each. Fully resident,
    /// or bounded-lazy under `BICDB_FTS_DOCS_LAZY` (see `DocsBacking`).
    docs: Arc<DocsBacking>,
    /// The term directory. Fully resident, or bounded-lazy under
    /// `BICDB_FTS_TERMS_LAZY` (see `TermsBacking`). Decoded one self-contained
    /// restart window at a time via `window_bytes`.
    terms: TermsBacking,
    /// Total on-disk size of the term directory, for storage accounting.
    terms_bytes: u64,
    /// Byte offsets of front-coding restarts inside `terms`.
    restarts: Vec<u64>,
    terms_body_len: usize,
    pub(crate) term_count: u64,
    meta_cache: parking_lot::Mutex<MetaCache>,
}

struct MetaCache {
    map: FxHashMap<Vec<u8>, Arc<SegmentTermMeta>>,
    order: std::collections::VecDeque<Vec<u8>>,
}

/// Parse the term-directory footer (restart offsets + count + magic) from a
/// resident slice. Returns `(restarts, terms_body_len)`.
fn parse_terms_footer_from_slice(terms_bytes: &[u8]) -> Result<(Vec<u64>, usize)> {
    if terms_bytes.len() < 12 {
        return Err(segment_error("segment term directory is too short"));
    }
    let magic_at = terms_bytes.len() - 8;
    if &terms_bytes[magic_at..] != TERMS_FOOTER_MAGIC {
        return Err(segment_error(
            "segment term directory has no footer; refusing a torn segment",
        ));
    }
    let count_at = magic_at - 4;
    let restart_count =
        u32::from_le_bytes(terms_bytes[count_at..magic_at].try_into().expect("4 bytes")) as usize;
    let restarts_at = count_at
        .checked_sub(restart_count * 8)
        .ok_or_else(|| segment_error("segment term directory footer is inconsistent"))?;
    let mut restarts = Vec::with_capacity(restart_count);
    for index in 0..restart_count {
        let at = restarts_at + index * 8;
        restarts.push(u64::from_le_bytes(
            terms_bytes[at..at + 8].try_into().expect("8 bytes"),
        ));
    }
    Ok((restarts, restarts_at))
}

/// Parse the same footer from the file tail, for the bounded-lazy path — it
/// never holds the whole directory resident. Returns `(restarts, terms_body_len)`.
fn parse_terms_footer_from_file(file: &File, len: u64) -> Result<(Vec<u64>, usize)> {
    if len < 12 {
        return Err(segment_error("segment term directory is too short"));
    }
    let mut tail = [0u8; 12];
    read_exact_at(file, &mut tail, len - 12)
        .map_err(|error| segment_error(format!("cannot read segment terms footer: {error}")))?;
    if &tail[4..] != TERMS_FOOTER_MAGIC {
        return Err(segment_error(
            "segment term directory has no footer; refusing a torn segment",
        ));
    }
    let restart_count = u32::from_le_bytes(tail[0..4].try_into().expect("4 bytes")) as usize;
    let count_at = (len as usize) - 12;
    let restarts_at = count_at
        .checked_sub(restart_count * 8)
        .ok_or_else(|| segment_error("segment term directory footer is inconsistent"))?;
    let mut restart_bytes = vec![0u8; restart_count * 8];
    read_exact_at(file, &mut restart_bytes, restarts_at as u64)
        .map_err(|error| segment_error(format!("cannot read segment terms restarts: {error}")))?;
    let mut restarts = Vec::with_capacity(restart_count);
    for index in 0..restart_count {
        let at = index * 8;
        restarts.push(u64::from_le_bytes(
            restart_bytes[at..at + 8].try_into().expect("8 bytes"),
        ));
    }
    Ok((restarts, restarts_at))
}

impl FtsSegmentReader {
    /// The bytes of one self-contained restart window (front-coding and base
    /// runs reset at every restart), borrowed from the resident directory or
    /// read lazily from the file through a bounded LRU.
    fn window_bytes(&self, restart_idx: usize) -> Result<WindowBytes<'_>> {
        let start = self.restarts[restart_idx] as usize;
        let end = self
            .restarts
            .get(restart_idx + 1)
            .map(|offset| *offset as usize)
            .unwrap_or(self.terms_body_len);
        match &self.terms {
            TermsBacking::Resident(resident) => {
                Ok(WindowBytes::Borrowed(&resident.as_slice()[start..end]))
            }
            TermsBacking::Lazy(lazy) => {
                Ok(WindowBytes::Shared(lazy.window(restart_idx, start, end)?))
            }
        }
    }

    /// Open and validate a published segment. Sizes are always checked
    /// against the manifest; checksums are verified by [`Self::verify`],
    /// which costs a full read and is for operators and tests.
    pub(crate) fn open(segments_root: &Path, physical: &str) -> Result<Option<Self>> {
        let dir = segments_root.join(physical);
        let manifest_path = dir.join(MANIFEST_FILE);
        if !manifest_path.exists() {
            return Ok(None);
        }
        let manifest: SegmentManifest = read_json(&manifest_path)?;
        if manifest.format != FTS_SEGMENT_FORMAT {
            // Format 1 stored legacy block bytes verbatim; format 2 stores
            // the slim form. One reader, one format: earlier segments rebuild
            // rather than accreting parse paths.
            return Err(segment_error(format!(
                "segment `{physical}` uses format {} where this build implements \
                 format {FTS_SEGMENT_FORMAT}; REINDEX to rebuild it",
                manifest.format
            )));
        }
        for file in &manifest.files {
            let actual = fs::metadata(dir.join(&file.name))
                .map_err(|error| {
                    segment_error(format!(
                        "segment `{physical}` is missing `{}`: {error}; REINDEX to rebuild it",
                        file.name
                    ))
                })?
                .len();
            if actual != file.bytes {
                return Err(segment_error(format!(
                    "segment `{physical}` file `{}` is {actual} bytes where the manifest \
                     records {}; refusing a torn segment — REINDEX to rebuild it",
                    file.name, file.bytes
                )));
            }
        }

        let postings = File::open(dir.join(POSTINGS_FILE))
            .map_err(|error| segment_error(format!("cannot open segment postings: {error}")))?;
        let impacts = File::open(dir.join(IMPACTS_FILE))
            .map_err(|error| segment_error(format!("cannot open segment impacts: {error}")))?;
        let docs_backing = DocsBacking::open(&dir.join(DOCS_FILE))?;
        let terms_backing = TermsBacking::open(&dir.join(TERMS_FILE))?;
        let (restarts, restarts_at) = match &terms_backing {
            TermsBacking::Resident(resident) => parse_terms_footer_from_slice(resident.as_slice())?,
            TermsBacking::Lazy(lazy) => {
                let terms_len = lazy
                    .file
                    .metadata()
                    .map_err(|error| segment_error(format!("cannot stat segment terms: {error}")))?
                    .len();
                parse_terms_footer_from_file(&lazy.file, terms_len)?
            }
        };
        let postings_bytes = manifest
            .files
            .iter()
            .find(|file| file.name == POSTINGS_FILE)
            .map(|file| file.bytes)
            .unwrap_or(0);
        let impacts_bytes = manifest
            .files
            .iter()
            .find(|file| file.name == IMPACTS_FILE)
            .map(|file| file.bytes)
            .unwrap_or(0);
        let terms_bytes_total = manifest
            .files
            .iter()
            .find(|file| file.name == TERMS_FILE)
            .map(|file| file.bytes)
            .unwrap_or(0);
        Ok(Some(Self {
            postings,
            impacts,
            postings_bytes,
            impacts_bytes,
            docs: Arc::new(docs_backing),
            terms: terms_backing,
            terms_bytes: terms_bytes_total,
            restarts,
            terms_body_len: restarts_at,
            term_count: manifest.term_count,
            meta_cache: parking_lot::Mutex::new(MetaCache {
                map: FxHashMap::default(),
                order: std::collections::VecDeque::new(),
            }),
        }))
    }

    /// (postings, impacts, terms) file sizes, for storage accounting.
    pub(crate) fn file_bytes(&self) -> (u64, u64, u64) {
        (self.postings_bytes, self.impacts_bytes, self.terms_bytes)
    }

    /// Verify every file against its manifest checksum. Full-read cost, by
    /// design an explicit operation rather than an open-time tax.
    pub(crate) fn verify(segments_root: &Path, physical: &str) -> Result<()> {
        let dir = segments_root.join(physical);
        let manifest: SegmentManifest = read_json(&dir.join(MANIFEST_FILE))?;
        for file in &manifest.files {
            let mut hasher = Sha256::new();
            let mut handle = File::open(dir.join(&file.name))
                .map_err(|error| segment_error(format!("cannot open `{}`: {error}", file.name)))?;
            let mut buffer = vec![0u8; 1 << 20];
            loop {
                let read = handle.read(&mut buffer).map_err(|error| {
                    segment_error(format!("cannot read `{}`: {error}", file.name))
                })?;
                if read == 0 {
                    break;
                }
                hasher.update(&buffer[..read]);
            }
            if hex::encode(hasher.finalize()) != file.sha256 {
                return Err(segment_error(format!(
                    "segment file `{}` failed its checksum; the segment is corrupt and must \
                     be rebuilt with REINDEX",
                    file.name
                )));
            }
        }
        Ok(())
    }

    /// Decode the term record beginning at `at`. Bases are restart-anchored:
    /// a record at window position 0 carries them absolutely, every other
    /// record derives them from the runners the walk accumulates — term N's
    /// blocks start exactly where term N-1's ended, so storing bases per term
    /// was 9-10 bytes of pure redundancy each.
    fn decode_record(
        &self,
        bytes: &[u8],
        at: &mut usize,
        previous: &mut Vec<u8>,
        window_position: usize,
        pk_base_run: &mut u64,
        impact_base_run: &mut u64,
    ) -> Result<Arc<SegmentTermMeta>> {
        let lcp = read_varint(bytes, at)? as usize;
        let suffix_len = read_varint(bytes, at)? as usize;
        if lcp > previous.len() {
            return Err(segment_error("segment term front-coding is inconsistent"));
        }
        previous.truncate(lcp);
        let suffix_end = *at + suffix_len;
        previous.extend_from_slice(
            bytes
                .get(*at..suffix_end)
                .ok_or_else(|| segment_error("segment term directory is truncated"))?,
        );
        *at = suffix_end;
        if window_position == 0 {
            *pk_base_run = read_varint(bytes, at)?;
            *impact_base_run = read_varint(bytes, at)?;
        }

        let document_frequency = read_varint(bytes, at)?;
        let collection_frequency = read_varint(bytes, at)?;
        let maximum_contribution = dequantize_contribution(
            *bytes
                .get(*at)
                .ok_or_else(|| segment_error("segment term directory is truncated"))?,
        );
        *at += 1;
        let pk_base = *pk_base_run;
        let pk_count = read_varint(bytes, at)? as usize;
        let mut pk_blocks = Vec::with_capacity(pk_count);
        let mut pk_headers = Vec::with_capacity(if pk_count > 1 { pk_count } else { 0 });
        let mut running_doc = 0u64;
        let mut running_offset = 0u64;
        for _ in 0..pk_count {
            let delta = read_varint(bytes, at)?;
            let len = read_varint(bytes, at)? as u32;
            let max_tf = read_varint(bytes, at)? as u32;
            if pk_count > 1 {
                let max_impact = read_varint(bytes, at)? as u16;
                let rank_bits = bytes
                    .get(*at..*at + 4)
                    .ok_or_else(|| segment_error("segment term directory is truncated"))?
                    .try_into()
                    .expect("4 bytes");
                *at += 4;
                let weight_mask = *bytes
                    .get(*at)
                    .ok_or_else(|| segment_error("segment term directory is truncated"))?;
                *at += 1;
                pk_headers.push((max_impact, u32::from_le_bytes(rank_bits), weight_mask));
            }
            running_doc += delta;
            pk_blocks.push((running_doc, running_offset, len, max_tf));
            running_offset += u64::from(len);
        }
        let last_doc_id = running_doc;
        let span = read_varint(bytes, at)?;
        let first_doc_id = last_doc_id
            .checked_sub(span)
            .ok_or_else(|| segment_error("segment term span exceeds its last document id"))?;
        let impact_base = *impact_base_run;
        let impact_count = read_varint(bytes, at)? as usize;
        let mut impact_blocks = Vec::with_capacity(impact_count);
        let mut impact_offset = 0u64;
        for _ in 0..impact_count {
            let len = read_varint(bytes, at)? as u32;
            impact_blocks.push((impact_offset, len));
            impact_offset += u64::from(len);
        }
        *pk_base_run += running_offset;
        *impact_base_run += impact_offset;
        Ok(Arc::new(SegmentTermMeta {
            document_frequency,
            collection_frequency,
            maximum_contribution,
            first_doc_id,
            last_doc_id,
            pk_base,
            pk_blocks: pk_blocks.into(),
            pk_headers: pk_headers.into(),
            impact_base,
            impact_blocks: impact_blocks.into(),
        }))
    }

    /// The term fully decodable at a restart offset (lcp is 0 there).
    fn term_at_restart(&self, restart: usize) -> Result<Vec<u8>> {
        let window = self.window_bytes(restart)?;
        let bytes: &[u8] = &window;
        let mut at = 0usize;
        let lcp = read_varint(bytes, &mut at)?;
        if lcp != 0 {
            return Err(segment_error("segment restart entry has a nonzero prefix"));
        }
        let suffix_len = read_varint(bytes, &mut at)? as usize;
        bytes
            .get(at..at + suffix_len)
            .map(<[u8]>::to_vec)
            .ok_or_else(|| segment_error("segment term directory is truncated"))
    }

    /// Look up one term's metadata.
    pub(crate) fn term_meta(&self, term: &[u8]) -> Result<Option<Arc<SegmentTermMeta>>> {
        if self.restarts.is_empty() {
            return Ok(None);
        }
        {
            let cache = self.meta_cache.lock();
            if let Some(meta) = cache.map.get(term) {
                return Ok(Some(Arc::clone(meta)));
            }
        }
        // Binary search the restarts for the last restart term <= target.
        let mut low = 0usize;
        let mut high = self.restarts.len();
        while high - low > 1 {
            let mid = (low + high) / 2;
            if self.term_at_restart(mid)?.as_slice() <= term {
                low = mid;
            } else {
                high = mid;
            }
        }
        if self.term_at_restart(low)?.as_slice() > term {
            return Ok(None);
        }
        // Linear front-decode within the restart window (self-contained).
        let window = self.window_bytes(low)?;
        let bytes: &[u8] = &window;
        let mut at = 0usize;
        let mut current = Vec::new();
        let mut pk_base_run = 0u64;
        let mut impact_base_run = 0u64;
        for window_position in 0..TERM_RESTART_INTERVAL {
            if at >= bytes.len() {
                return Ok(None);
            }
            let meta = self.decode_record(
                bytes,
                &mut at,
                &mut current,
                window_position,
                &mut pk_base_run,
                &mut impact_base_run,
            )?;
            match current.as_slice().cmp(term) {
                std::cmp::Ordering::Less => continue,
                std::cmp::Ordering::Equal => {
                    let mut cache = self.meta_cache.lock();
                    if cache.map.len() >= TERM_META_CACHE_CAP {
                        if let Some(evicted) = cache.order.pop_front() {
                            cache.map.remove(&evicted);
                        }
                    }
                    cache.map.insert(term.to_vec(), Arc::clone(&meta));
                    cache.order.push_back(term.to_vec());
                    return Ok(Some(meta));
                }
                std::cmp::Ordering::Greater => return Ok(None),
            }
        }
        Ok(None)
    }

    fn read_range(&self, file: &File, offset: u64, len: usize) -> Result<Vec<u8>> {
        let mut buffer = vec![0u8; len];
        read_exact_at(file, &mut buffer, offset)
            .map_err(|error| segment_error(format!("segment read failed: {error}")))?;
        Ok(buffer)
    }

    /// Pk-ordered blocks whose last document id is `>= document_id`, up to
    /// `max_blocks`, exactly as the paged batch lookup returns them. Adjacent
    /// blocks are one contiguous range in the file, so the whole batch is a
    /// single read.
    pub(crate) fn pk_blocks_from(
        &self,
        meta: &SegmentTermMeta,
        document_id: u64,
        max_blocks: usize,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        let start = meta
            .pk_blocks
            .partition_point(|(last, _, _, _)| *last < document_id);
        let end = (start + max_blocks.max(1)).min(meta.pk_blocks.len());
        if start >= end {
            return Ok(Vec::new());
        }
        let first = &meta.pk_blocks[start];
        let last = &meta.pk_blocks[end - 1];
        let range_start = meta.pk_base + first.1;
        let range_len = (last.1 + u64::from(last.2) - first.1) as usize;
        let bytes = self.read_range(&self.postings, range_start, range_len)?;
        let mut out = Vec::with_capacity(end - start);
        for (index, (last_doc, offset, len, _)) in meta.pk_blocks[start..end].iter().enumerate() {
            let begin = (offset - first.1) as usize;
            let slim = &bytes[begin..begin + *len as usize];
            // Rehydrate document properties from docs.dat and re-encode into
            // the exact bytes the paged store would hold: the v1 header is a
            // deterministic function of the postings, so downstream kernels
            // (and the equivalence gate) cannot tell where a block came from.
            let postings = decode_slim_pk_block(slim, &self.docs.view())?;
            let encoded = match meta.pk_headers.get(start + index) {
                // Multi-block terms carry the header pair, so the scattered
                // probes those terms attract skip the per-posting rank loop.
                Some((max_impact, rank_bits, _weight_mask)) => {
                    let encoded = crate::paged_collection::encode_numeric_posting_block(
                        &postings,
                        *max_impact,
                        f32::from_bits(*rank_bits),
                    );
                    debug_assert_eq!(
                        encoded,
                        crate::db::encode_numeric_posting_run(&postings).1,
                        "stored v1 header pair drifted from the recompute"
                    );
                    encoded
                }
                None => crate::db::encode_numeric_posting_run(&postings).1,
            };
            let encoded_last = postings.last().map(|posting| posting.document_id);
            if encoded_last != Some(*last_doc) {
                return Err(segment_error(
                    "segment block boundary does not match its directory entry; \
                     REINDEX to rebuild the segment",
                ));
            }
            out.push((*last_doc, encoded));
        }
        Ok(out)
    }

    /// The per-document field table, shared into fetched slim blocks so they
    /// can decode away from the reader.
    pub(crate) fn docs_handle(&self) -> Arc<DocsBacking> {
        Arc::clone(&self.docs)
    }

    /// Slim-native batch fetch: the stored slim bytes plus, for multi-block
    /// terms, the v1 header triple served from the term directory. No v1
    /// bytes are materialized — the seeking kernels decode slim directly.
    /// Single-block terms return `None` metadata; their one-off header
    /// recompute during decode is cheap and lands in the block cache.
    pub(crate) fn pk_slim_blocks_from(
        &self,
        meta: &SegmentTermMeta,
        document_id: u64,
        max_blocks: usize,
    ) -> Result<Vec<(u64, Vec<u8>, u32, Option<(f32, u8)>)>> {
        let start = meta
            .pk_blocks
            .partition_point(|(last, _, _, _)| *last < document_id);
        let end = (start + max_blocks.max(1)).min(meta.pk_blocks.len());
        if start >= end {
            return Ok(Vec::new());
        }
        let first = &meta.pk_blocks[start];
        let last = &meta.pk_blocks[end - 1];
        let range_start = meta.pk_base + first.1;
        let range_len = (last.1 + u64::from(last.2) - first.1) as usize;
        let bytes = self.read_range(&self.postings, range_start, range_len)?;
        let mut out = Vec::with_capacity(end - start);
        for (index, (last_doc, offset, len, max_tf)) in
            meta.pk_blocks[start..end].iter().enumerate()
        {
            let begin = (offset - first.1) as usize;
            let slim = bytes[begin..begin + *len as usize].to_vec();
            let header = meta
                .pk_headers
                .get(start + index)
                .map(|(_, rank_bits, weight_mask)| (f32::from_bits(*rank_bits), *weight_mask));
            out.push((*last_doc, slim, *max_tf, header));
        }
        Ok(out)
    }

    /// Boundary stream: (last document id, max term frequency) per block from
    /// `document_id`, straight from resident metadata — no file I/O at all,
    /// where the paged path reads a 64-byte value prefix per block.
    pub(crate) fn pk_boundaries_from(
        &self,
        meta: &SegmentTermMeta,
        document_id: u64,
        max_blocks: usize,
    ) -> Vec<(u64, u32)> {
        let start = meta
            .pk_blocks
            .partition_point(|(last, _, _, _)| *last < document_id);
        meta.pk_blocks[start..]
            .iter()
            .take(max_blocks.max(1))
            .map(|(last, _, _, max_tf)| (*last, *max_tf))
            .collect()
    }

    /// Every pk block of the term, in document-id order.
    pub(crate) fn pk_blocks_all(&self, meta: &SegmentTermMeta) -> Result<Vec<(u64, Vec<u8>)>> {
        self.pk_blocks_from(meta, 0, meta.pk_blocks.len().max(1))
    }

    /// Stream impact blocks for the term in impact order. Each `next()` reads
    /// only that block's byte range; common terms can have multi-gigabyte
    /// sidecars and must not be materialized before score-first pruning can
    /// stop the scan.
    pub(crate) fn impact_blocks(
        self: &Arc<Self>,
        meta: Arc<SegmentTermMeta>,
    ) -> Result<SegmentImpactBlockCursor> {
        if meta.impact_blocks.is_empty() {
            // Elided sidecar: the writer drops the impact copy of any term
            // whose postings fit one pk block, because it is a pure function
            // of that block. Regenerate it through the build's own
            // comparator and encoder — byte-identical by construction.
            if meta.pk_blocks.len() != 1 {
                return Err(segment_error(
                    "segment term has no impact blocks but several pk blocks; \
                     REINDEX to rebuild the segment",
                ));
            }
            let (_, offset, len, _) = meta.pk_blocks[0];
            let slim = self.read_range(&self.postings, meta.pk_base + offset, len as usize)?;
            let mut postings = decode_slim_pk_block(&slim, &self.docs.view())?;
            let sidecar = crate::db::synthesize_impact_sidecar(&mut postings);
            return Ok(SegmentImpactBlockCursor {
                reader: Arc::clone(self),
                meta,
                next: 0,
                synthesized: Some(sidecar),
                failed: false,
            });
        }
        Ok(SegmentImpactBlockCursor {
            reader: Arc::clone(self),
            meta,
            next: 0,
            synthesized: None,
            failed: false,
        })
    }

    /// Iterate terms in sorted order starting at the first term `>= lower`.
    /// The caller stops when its prefix no longer matches. The cursor owns an
    /// `Arc` of the reader so it can be embedded in merge iterators.
    pub(crate) fn terms_from(self: &Arc<Self>, lower: &[u8]) -> Result<SegmentTermCursor> {
        if self.restarts.is_empty() {
            return Ok(SegmentTermCursor {
                reader: Arc::clone(self),
                current_restart: usize::MAX,
                at: 0,
                current: Vec::new(),
                pending: None,
                window_position: 0,
                pk_base_run: 0,
                impact_base_run: 0,
            });
        }
        let mut low = 0usize;
        let mut high = self.restarts.len();
        while high - low > 1 {
            let mid = (low + high) / 2;
            if self.term_at_restart(mid)?.as_slice() <= lower {
                low = mid;
            } else {
                high = mid;
            }
        }
        let mut cursor = SegmentTermCursor {
            reader: Arc::clone(self),
            current_restart: low,
            at: 0,
            current: Vec::new(),
            pending: None,
            window_position: 0,
            pk_base_run: 0,
            impact_base_run: 0,
        };
        // Advance to the first term >= lower.
        loop {
            match cursor.peek()? {
                Some((term, _)) if term < lower => {
                    cursor.pending = None;
                }
                _ => break,
            }
        }
        Ok(cursor)
    }
}

pub(crate) struct SegmentImpactBlockCursor {
    reader: Arc<FtsSegmentReader>,
    meta: Arc<SegmentTermMeta>,
    next: usize,
    synthesized: Option<Vec<u8>>,
    failed: bool,
}

impl Iterator for SegmentImpactBlockCursor {
    type Item = Result<(u32, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        if let Some(sidecar) = self.synthesized.take() {
            self.next = 1;
            return Some(Ok((0, sidecar)));
        }
        let seq = self.next;
        let &(offset, len) = self.meta.impact_blocks.get(seq)?;
        self.next += 1;
        let result = self
            .reader
            .read_range(
                &self.reader.impacts,
                self.meta.impact_base + offset,
                len as usize,
            )
            .and_then(|slim| {
                let (ids, max_impact, max_rank) = decode_slim_impact_block(&slim)?;
                Ok((
                    seq as u32,
                    crate::paged_collection::encode_compact_impact_block_from_ids(
                        &ids, max_impact, max_rank,
                    ),
                ))
            });
        if result.is_err() {
            self.failed = true;
        }
        Some(result)
    }
}

/// Forward cursor over the term directory. Owns its reader.
pub(crate) struct SegmentTermCursor {
    reader: Arc<FtsSegmentReader>,
    /// Restart window currently being decoded; `usize::MAX` once exhausted.
    current_restart: usize,
    /// Offset WITHIN the current restart window (windows are self-contained).
    at: usize,
    current: Vec<u8>,
    pending: Option<Arc<SegmentTermMeta>>,
    /// Records consumed since the last restart; 0 means the next record
    /// carries absolute bases.
    window_position: usize,
    pk_base_run: u64,
    impact_base_run: u64,
}

impl SegmentTermCursor {
    pub(crate) fn reader(&self) -> &Arc<FtsSegmentReader> {
        &self.reader
    }

    /// The current (term, meta) without consuming it.
    fn peek(&mut self) -> Result<Option<(&[u8], Arc<SegmentTermMeta>)>> {
        if self.pending.is_none() {
            let reader = Arc::clone(&self.reader);
            // Skip over any exhausted restart windows, advancing to the next.
            loop {
                if self.current_restart == usize::MAX
                    || self.current_restart >= reader.restarts.len()
                {
                    self.current_restart = usize::MAX;
                    return Ok(None);
                }
                let window_len = reader.window_bytes(self.current_restart)?.len();
                if self.at < window_len {
                    break;
                }
                self.current_restart += 1;
                self.at = 0;
                self.window_position = 0;
            }
            let window = reader.window_bytes(self.current_restart)?;
            let meta = reader.decode_record(
                &window,
                &mut self.at,
                &mut self.current,
                self.window_position,
                &mut self.pk_base_run,
                &mut self.impact_base_run,
            )?;
            self.window_position += 1;
            self.pending = Some(meta);
        }
        Ok(self
            .pending
            .as_ref()
            .map(|meta| (self.current.as_slice(), Arc::clone(meta))))
    }

    /// Consume and return the next (term, meta).
    pub(crate) fn next_term(&mut self) -> Result<Option<(Vec<u8>, Arc<SegmentTermMeta>)>> {
        let next = self.peek()?.map(|(term, meta)| (term.to_vec(), meta));
        self.pending = None;
        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lazy_docs_matches_resident_including_eviction() {
        // A docs.dat spanning many blocks so the bounded cache must evict.
        let doc_count: u64 = 40_000; // 40k * 8 = 320 KiB = ~5 blocks at 64 KiB
        let mut bytes = vec![0u8; (doc_count * 8) as usize];
        for id in 0..doc_count {
            let doc_length = (id as u32).wrapping_mul(3).wrapping_add(1);
            let doc_distinct = (id as u32) % 997 + 1;
            let packed = u64::from(doc_length) | (u64::from(doc_distinct) << 32);
            let at = (id * 8) as usize;
            bytes[at..at + 8].copy_from_slice(&packed.to_le_bytes());
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("docs.dat");
        std::fs::write(&path, &bytes).unwrap();

        // A tiny cache (2 blocks) guarantees eviction across the scan.
        let lazy = LazyDocs {
            file: File::open(&path).unwrap(),
            len: bytes.len() as u64,
            cache: parking_lot::Mutex::new(DocsBlockCache {
                blocks: FxHashMap::default(),
                order: std::collections::VecDeque::new(),
                max_blocks: 2,
            }),
        };
        let resident = Docs::Slice(&bytes);
        let lazy_view = Docs::Lazy(&lazy);

        // Forward scan, then a random-ish revisit pattern that forces re-reads
        // of evicted blocks.
        let mut order: Vec<u64> = (0..doc_count).collect();
        order.extend((0..doc_count).step_by(7));
        order.extend((0..doc_count).rev().step_by(13));
        for id in order {
            assert_eq!(
                lazy_view.doc(id).unwrap(),
                resident.doc(id).unwrap(),
                "lazy != resident at doc {id}"
            );
        }
        // The cache stayed bounded.
        assert!(lazy.cache.lock().blocks.len() <= 2);

        // Out-of-range ids fail on both paths.
        assert!(lazy_view.doc(doc_count).is_err());
        assert!(resident.doc(doc_count).is_err());
    }

    fn fake_block(payload: &[u8]) -> Vec<u8> {
        // A minimal value carrying a parseable rank header is built by the
        // real encoder in paged_collection; unit tests here only exercise the
        // directory machinery, so they synthesize blocks through the real
        // encoder in the integration tests instead. This helper produces an
        // opaque payload for pure read/write plumbing checks.
        payload.to_vec()
    }

    #[test]
    fn varints_round_trip() {
        let mut out = Vec::new();
        let values = [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX / 3];
        for value in values {
            write_varint(&mut out, value);
        }
        let mut at = 0usize;
        for value in values {
            assert_eq!(read_varint(&out, &mut at).unwrap(), value);
        }
        assert_eq!(at, out.len());
    }

    #[test]
    fn a_torn_terms_directory_is_refused() {
        let _ = fake_block(b"x");
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        fs::create_dir_all(root.join("seg")).unwrap();
        fs::write(root.join("seg").join(TERMS_FILE), b"garbage-no-footer").unwrap();
        fs::write(
            root.join("seg").join(MANIFEST_FILE),
            serde_json::to_vec(&SegmentManifest {
                format: FTS_SEGMENT_FORMAT,
                term_count: 0,
                files: vec![ManifestFile {
                    name: TERMS_FILE.to_string(),
                    bytes: 17,
                    sha256: "00".into(),
                }],
            })
            .unwrap(),
        )
        .unwrap();
        fs::write(root.join("seg").join(POSTINGS_FILE), b"").unwrap();
        fs::write(root.join("seg").join(IMPACTS_FILE), b"").unwrap();
        fs::write(root.join("seg").join(DOCS_FILE), b"").unwrap();
        let error = FtsSegmentReader::open(root, "seg").unwrap_err().to_string();
        assert!(
            error.contains("footer") || error.contains("missing"),
            "{error}"
        );
    }

    #[test]
    fn a_size_mismatch_is_refused_at_open() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        fs::create_dir_all(root.join("seg")).unwrap();
        fs::write(root.join("seg").join(POSTINGS_FILE), b"abc").unwrap();
        fs::write(
            root.join("seg").join(MANIFEST_FILE),
            serde_json::to_vec(&SegmentManifest {
                format: FTS_SEGMENT_FORMAT,
                term_count: 0,
                files: vec![ManifestFile {
                    name: POSTINGS_FILE.to_string(),
                    bytes: 999,
                    sha256: "00".into(),
                }],
            })
            .unwrap(),
        )
        .unwrap();
        let error = FtsSegmentReader::open(root, "seg").unwrap_err().to_string();
        assert!(error.contains("torn segment"), "{error}");
    }

    #[test]
    fn an_unknown_format_is_refused_with_rebuild_guidance() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        fs::create_dir_all(root.join("seg")).unwrap();
        fs::write(
            root.join("seg").join(MANIFEST_FILE),
            serde_json::to_vec(&SegmentManifest {
                format: 999,
                term_count: 0,
                files: vec![],
            })
            .unwrap(),
        )
        .unwrap();
        let error = FtsSegmentReader::open(root, "seg").unwrap_err().to_string();
        assert!(error.contains("REINDEX"), "{error}");
    }

    #[test]
    fn slim_blocks_round_trip_including_document_properties() {
        use crate::paged_collection::NumericBlockPosting;
        // Positions pack as (weight << 14) | position, so streams DESCEND
        // across field boundaries — exactly what the zigzag delta must absorb.
        let mut docs = Vec::new();
        let postings: Vec<NumericBlockPosting> = (0..300u64)
            .map(|index| NumericBlockPosting {
                document_id: index * 7 + 3,
                doc_length: 100 + index as u32,
                doc_distinct: 40 + (index as u32 % 9),
                packed_positions: if index % 5 == 0 {
                    vec![(3 << 14) | 5, (3 << 14) | 90, (1 << 14) | 2]
                } else {
                    vec![(2 << 14) | (index as u16 % 1000)]
                },
            })
            .collect();
        for posting in &postings {
            let slot = posting.document_id as usize * 8;
            if docs.len() < slot + 8 {
                docs.resize(slot + 8, 0);
            }
            let packed = u64::from(posting.doc_length) | (u64::from(posting.doc_distinct) << 32);
            docs[slot..slot + 8].copy_from_slice(&packed.to_le_bytes());
        }
        let slim = encode_slim_pk_block(&postings);
        let decoded = decode_slim_pk_block(&slim, &Docs::Slice(&docs)).unwrap();
        assert_eq!(decoded, postings);
        // The stored form must be smaller than the postings' position+id
        // payload alone would be in v1 (5 metadata bytes per posting gone).
        assert!(
            slim.len() < postings.len() * 8,
            "slim block is not slim: {}",
            slim.len()
        );
    }

    #[test]
    fn missing_segment_is_none_not_error() {
        let dir = tempfile::tempdir().unwrap();
        assert!(FtsSegmentReader::open(dir.path(), "absent")
            .unwrap()
            .is_none());
    }

    #[test]
    fn impact_cursor_reads_one_block_at_a_time() {
        let first = encode_slim_impact_block(&[1, 3, 5], 900, 0.9);
        let second = encode_slim_impact_block(&[2, 4, 6], 700, 0.7);
        let dir = tempfile::tempdir().unwrap();
        let impacts_path = dir.path().join(IMPACTS_FILE);
        let postings_path = dir.path().join(POSTINGS_FILE);
        fs::write(&impacts_path, [&first[..], &second[..]].concat()).unwrap();
        fs::write(&postings_path, []).unwrap();
        let reader = Arc::new(FtsSegmentReader {
            postings: File::open(&postings_path).unwrap(),
            impacts: File::open(&impacts_path).unwrap(),
            postings_bytes: 0,
            impacts_bytes: (first.len() + second.len()) as u64,
            docs: Arc::new(DocsBacking::Resident(ResidentBytes {
                #[cfg(feature = "mmap")]
                mapping: None,
                owned: Vec::new(),
            })),
            terms: TermsBacking::Resident(ResidentBytes {
                #[cfg(feature = "mmap")]
                mapping: None,
                owned: Vec::new(),
            }),
            terms_bytes: 0,
            restarts: Vec::new(),
            terms_body_len: 0,
            term_count: 1,
            meta_cache: parking_lot::Mutex::new(MetaCache {
                map: FxHashMap::default(),
                order: std::collections::VecDeque::new(),
            }),
        });
        let meta = Arc::new(SegmentTermMeta {
            document_frequency: 6,
            collection_frequency: 6,
            maximum_contribution: 0.9,
            first_doc_id: 1,
            last_doc_id: 6,
            pk_base: 0,
            pk_blocks: Arc::from([]),
            pk_headers: Arc::from([]),
            impact_base: 0,
            impact_blocks: Arc::from([
                (0, first.len() as u32),
                (first.len() as u64, second.len() as u32),
            ]),
        });

        // A cursor must not touch the second block while it is being created
        // or while the first item is read. Truncating the already-open file
        // makes an eager whole-term read fail immediately, while a true
        // block stream returns block zero and fails only when advanced again.
        let mut cursor = reader.impact_blocks(meta).unwrap();
        File::options()
            .write(true)
            .open(&impacts_path)
            .unwrap()
            .set_len(first.len() as u64)
            .unwrap();
        assert_eq!(cursor.next().unwrap().unwrap().0, 0);
        assert!(cursor.next().unwrap().is_err());
    }
}

#[cfg(test)]
mod contribution_quantization_tests {
    use super::*;

    #[test]
    fn the_quantized_bound_never_rounds_below_the_value() {
        let mut value = 1e-6f32;
        while value < 1.5 {
            let bound = dequantize_contribution(quantize_contribution(value));
            assert!(
                bound >= value,
                "bound {bound} fell below {value} — an invalid pruning bound"
            );
            assert!(
                bound <= (value * 1.05).max(1e-4 + f32::EPSILON),
                "bound {bound} is more than 5% above {value} — too loose"
            );
            value *= 1.01;
        }
        assert_eq!(quantize_contribution(f32::NAN), u8::MAX);
        assert!(dequantize_contribution(u8::MAX) >= 1.0);
    }
}

#[cfg(test)]
mod slim_impact_tests {
    use super::*;

    #[test]
    fn slim_impact_blocks_round_trip_and_regenerate_exact_v1_bytes() {
        // Sidecar order: impact bucket descending, doc ascending — three
        // runs with resets, the shape that made whole-sequence zigzag pay
        // wide frames.
        let ids = vec![5u64, 9, 1_200, 3, 77, 4_000_000, 2, 8, 10];
        let slim = encode_slim_impact_block(&ids, 991, 0.4375);
        let (decoded, max_impact, max_rank) = decode_slim_impact_block(&slim).unwrap();
        assert_eq!(decoded, ids);
        assert_eq!(max_impact, 991);
        assert_eq!(max_rank, 0.4375);
        let regenerated = crate::paged_collection::encode_compact_impact_block_from_ids(
            &decoded, max_impact, max_rank,
        );
        let direct =
            crate::paged_collection::encode_compact_impact_block_from_ids(&ids, 991, 0.4375);
        assert_eq!(regenerated, direct, "v1 regeneration drifted");
    }
}
