//! Versioned full-text generation metadata and query instrumentation.
//!
//! Posting codecs live in `paged_collection`; this module contains the small
//! stable records that query planning may read without touching posting lists.

use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{BicDbError, Result};

pub const FTS_GENERATION_FORMAT_VERSION: u32 = 3;
pub const FTS_MIN_READ_FORMAT_VERSION: u32 = 2;
pub const MAX_FULL_TEXT_STORED_TEXT_BYTES: usize = 64 * 1024 * 1024;
const STORED_TEXT_FORMAT_VERSION: u8 = 1;
const STORED_TEXT_PLAIN: u8 = 0;
const STORED_TEXT_ZSTD: u8 = 1;

pub const fn full_text_generation_format_is_readable(version: u32) -> bool {
    version >= FTS_MIN_READ_FORMAT_VERSION && version <= FTS_GENERATION_FORMAT_VERSION
}

/// A native BM25F field. The numeric values deliberately match PostgreSQL's
/// tsvector weights D, C, B and A so existing packed positions remain
/// compatible while callers no longer need to repeat important text.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum FullTextField {
    #[default]
    Body = 0,
    Auxiliary = 1,
    Heading = 2,
    Title = 3,
}

impl FullTextField {
    pub(crate) const fn slot(self) -> usize {
        self as usize
    }
}

/// One already-normalized term in a native full-text field.
///
/// Positions are field-local and must fit in 14 bits. An empty position list
/// means that the term occurs once but carries no phrase position, matching a
/// stripped tsvector lexeme.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FullTextTermInput {
    pub term: String,
    #[serde(default)]
    pub positions: Vec<u16>,
}

/// An analyzed field supplied directly to BicDB's external-memory builder.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FullTextFieldInput {
    pub field: FullTextField,
    #[serde(default)]
    pub terms: Vec<FullTextTermInput>,
}

/// One exact-match filter attached to a directly built full-text document.
///
/// Filter values are not searchable text and never contribute to document
/// length or ranking. They are encoded as reserved, generation-local posting
/// lists so immutable/sealed indexes can apply facets during candidate
/// generation without materializing duplicate collection rows.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FullTextFilterInput {
    pub name: String,
    pub value: String,
}

/// A document that can be indexed without inserting a duplicate table row.
///
/// `stored_text` is optional retrieval material. It is compressed and stored
/// in the FTS generation, not in the source collection. The caller retains
/// responsibility for analysis (normalization, stemming and stop words), so
/// indexing and query analysis can share the same application vocabulary.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FullTextDocumentInput {
    pub primary_key: String,
    #[serde(default)]
    pub fields: Vec<FullTextFieldInput>,
    #[serde(default)]
    pub filters: Vec<FullTextFilterInput>,
    #[serde(default)]
    pub stored_text: Option<Vec<u8>>,
}

pub(crate) fn full_text_filter_term(name: &str, value: &str) -> Result<String> {
    if name.is_empty() || value.is_empty() {
        return Err(BicDbError::Index(
            "full-text filter names and values must not be empty".to_string(),
        ));
    }
    let mut digest = Sha256::new();
    digest.update(b"bicdb-full-text-filter-v1\0");
    digest.update((name.len() as u64).to_le_bytes());
    digest.update(name.as_bytes());
    digest.update((value.len() as u64).to_le_bytes());
    digest.update(value.as_bytes());
    Ok(format!(
        "\0bicdb-filter-v1:{}",
        hex::encode(digest.finalize())
    ))
}

/// Logical live bytes in one page-store namespace. Page headers, free space
/// and MVCC history are intentionally excluded; checkpoint/vacuum metrics
/// account for those physical costs separately.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageNamespaceAccounting {
    pub entries: u64,
    pub key_bytes: u64,
    pub value_bytes: u64,
}

impl StorageNamespaceAccounting {
    pub const fn total_bytes(self) -> u64 {
        self.key_bytes.saturating_add(self.value_bytes)
    }

    pub(crate) fn add(&mut self, key_bytes: usize, value_bytes: usize) {
        self.entries = self.entries.saturating_add(1);
        self.key_bytes = self.key_bytes.saturating_add(key_bytes as u64);
        self.value_bytes = self.value_bytes.saturating_add(value_bytes as u64);
    }

    pub(crate) fn merge(&mut self, other: Self) {
        self.entries = self.entries.saturating_add(other.entries);
        self.key_bytes = self.key_bytes.saturating_add(other.key_bytes);
        self.value_bytes = self.value_bytes.saturating_add(other.value_bytes);
    }
}

/// Namespace-by-namespace storage accounting for one published FTS index.
/// Structural decomposition of a namespace's KEY bytes.
///
/// The question each field answers: is this byte carrying information, or is
/// it already implied by the tree the entry lives in?
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyForensics {
    pub entries: u64,
    pub total_bytes: u64,
    /// 3 bytes of namespace tag, per key. Implied by the subtree.
    pub namespace_bytes: u64,
    /// 2-byte big-endian length of the index name, per key. Implied.
    pub length_prefix_bytes: u64,
    /// The physical index name, per key. Implied by the subtree.
    pub index_name_bytes: u64,
    /// The `0x00 0x00` term/suffix separator, per key.
    pub framing_bytes: u64,
    /// Document or block identifier, per key. Genuine information.
    pub suffix_bytes: u64,
    /// The term itself, including escaping. Genuine information.
    pub term_bytes: u64,
    /// Of `term_bytes`, how many are `0xFF` escape bytes.
    pub escape_bytes: u64,
}

impl KeyForensics {
    /// Bytes that carry information the subtree does not already imply.
    pub fn required_bytes(self) -> u64 {
        self.term_bytes.saturating_add(self.suffix_bytes)
    }

    /// Bytes implied by the tree the entry lives in.
    pub fn implied_bytes(self) -> u64 {
        self.namespace_bytes
            .saturating_add(self.length_prefix_bytes)
            .saturating_add(self.index_name_bytes)
            .saturating_add(self.framing_bytes)
    }

    pub fn merge(&mut self, other: Self) {
        self.entries += other.entries;
        self.total_bytes += other.total_bytes;
        self.namespace_bytes += other.namespace_bytes;
        self.length_prefix_bytes += other.length_prefix_bytes;
        self.index_name_bytes += other.index_name_bytes;
        self.framing_bytes += other.framing_bytes;
        self.suffix_bytes += other.suffix_bytes;
        self.term_bytes += other.term_bytes;
        self.escape_bytes += other.escape_bytes;
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FullTextStorageAccounting {
    pub logical_index: String,
    pub physical_index: String,
    pub row_data: StorageNamespaceAccounting,
    pub document_terms: StorageNamespaceAccounting,
    pub term_dictionary: StorageNamespaceAccounting,
    pub document_ids: StorageNamespaceAccounting,
    pub postings: StorageNamespaceAccounting,
    pub impact_metadata: StorageNamespaceAccounting,
    pub document_statistics: StorageNamespaceAccounting,
    pub stored_text: StorageNamespaceAccounting,
}

impl FullTextStorageAccounting {
    pub fn total_bytes(&self) -> u64 {
        [
            self.row_data,
            self.document_terms,
            self.term_dictionary,
            self.document_ids,
            self.postings,
            self.impact_metadata,
            self.document_statistics,
            self.stored_text,
        ]
        .into_iter()
        .map(StorageNamespaceAccounting::total_bytes)
        .fold(0, u64::saturating_add)
    }
}

pub(crate) fn encode_stored_text(input: &[u8]) -> Result<Vec<u8>> {
    if input.len() > MAX_FULL_TEXT_STORED_TEXT_BYTES {
        return Err(BicDbError::Index(format!(
            "full-text stored text is {} bytes; maximum is {}",
            input.len(),
            MAX_FULL_TEXT_STORED_TEXT_BYTES
        )));
    }
    #[cfg(feature = "compression")]
    let (codec, payload) = {
        if input.len() >= 256 {
            let compressed = zstd::stream::encode_all(input, 3)?;
            if compressed.len() < input.len() {
                (STORED_TEXT_ZSTD, compressed)
            } else {
                (STORED_TEXT_PLAIN, input.to_vec())
            }
        } else {
            (STORED_TEXT_PLAIN, input.to_vec())
        }
    };
    #[cfg(not(feature = "compression"))]
    let (codec, payload) = (STORED_TEXT_PLAIN, input.to_vec());
    let mut output = Vec::with_capacity(10 + payload.len());
    output.push(STORED_TEXT_FORMAT_VERSION);
    output.push(codec);
    output.extend_from_slice(&(input.len() as u64).to_le_bytes());
    output.extend_from_slice(&payload);
    Ok(output)
}

pub(crate) fn decode_stored_text(input: &[u8]) -> Result<Vec<u8>> {
    let header = input.get(..10).ok_or_else(|| {
        BicDbError::PagedStorage("corrupt full-text stored-text envelope".to_string())
    })?;
    if header[0] != STORED_TEXT_FORMAT_VERSION {
        return Err(BicDbError::PagedStorage(
            "unsupported full-text stored-text version".to_string(),
        ));
    }
    let expected = u64::from_le_bytes(header[2..10].try_into().expect("fixed header"));
    if expected > MAX_FULL_TEXT_STORED_TEXT_BYTES as u64 {
        return Err(BicDbError::PagedStorage(
            "full-text stored text exceeds decoded-size limit".to_string(),
        ));
    }
    let payload = &input[10..];
    let decoded = match header[1] {
        STORED_TEXT_PLAIN => payload.to_vec(),
        STORED_TEXT_ZSTD => {
            #[cfg(feature = "compression")]
            {
                use std::io::Read;
                let decoder = zstd::stream::read::Decoder::new(payload)?;
                let mut limited = decoder.take(MAX_FULL_TEXT_STORED_TEXT_BYTES as u64 + 1);
                let mut decoded = Vec::with_capacity(expected as usize);
                limited.read_to_end(&mut decoded)?;
                decoded
            }
            #[cfg(not(feature = "compression"))]
            {
                return Err(BicDbError::PagedStorage(
                    "stored text uses zstd but BicDB was built without compression".to_string(),
                ));
            }
        }
        _ => {
            return Err(BicDbError::PagedStorage(
                "unsupported full-text stored-text codec".to_string(),
            ));
        }
    };
    if decoded.len() != expected as usize {
        return Err(BicDbError::PagedStorage(
            "full-text stored-text decoded length mismatch".to_string(),
        ));
    }
    Ok(decoded)
}

/// One compact dictionary record per normalized term.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FullTextTermStatistics {
    pub document_frequency: u64,
    pub collection_frequency: u64,
    pub posting_block_count: u32,
    pub impact_block_count: u32,
    pub posting_bytes: u64,
    pub impact_bytes: u64,
    pub first_doc_id: Option<u64>,
    pub last_doc_id: Option<u64>,
    pub maximum_contribution: f32,
}

/// One lexeme and its aggregate statistics from a published packed FTS generation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FullTextTermDictionaryEntry {
    pub term: String,
    pub statistics: FullTextTermStatistics,
}

/// One fixed-size record per physical FTS generation.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FullTextCollectionStatistics {
    pub format_version: u32,
    pub document_count: u64,
    pub total_document_length: u64,
    pub average_document_length: f64,
    pub field_total_lengths: [u64; 4],
    pub term_count: u64,
    pub next_document_id: u64,
}

/// Length normalization scalars stored once per document.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FullTextDocumentStatistics {
    pub document_length: u32,
    pub distinct_terms: u32,
    /// Token counts by tsvector weight (D, C, B, A).
    pub field_lengths: [u32; 4],
}

/// Process-wide counters for proving query-planning and posting-I/O behavior.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FullTextQueryInstrumentation {
    /// Ranked queries that reached the parallel planner.
    pub ranked_queries_planned: u64,
    /// Ranked queries that actually got at least one extra worker.
    pub ranked_queries_run_parallel: u64,
    /// Extra workers requested across all ranked queries.
    pub ranked_workers_wanted: u64,
    /// Extra workers the global permit pool actually granted. A large gap
    /// against `ranked_workers_wanted` is permit starvation, not slow code.
    pub ranked_workers_granted: u64,
    pub dictionary_lookups: u64,
    pub dictionary_misses: u64,
    pub posting_blocks_read: u64,
    pub posting_bytes_read: u64,
    pub postings_decoded: u64,
    pub document_id_intersections: u64,
    pub simd_intersections: u64,
    pub wand_candidates_scored: u64,
    pub wand_postings_skipped: u64,
    pub block_max_candidates_pruned: u64,
    pub filter_candidates_rejected: u64,
    pub posting_keys_counted_for_planning: u64,
    pub block_cache_hits: u64,
    pub block_cache_misses: u64,
    pub prefetched_blocks: u64,
    /// Posting-block upper document ids read directly from B-tree keys. These
    /// are the compact skip entries used by BM25 shallow seeks; reading one
    /// does not resolve or decode the corresponding posting payload.
    #[serde(default)]
    pub posting_block_boundaries_read: u64,
    /// Boolean counts answered entirely from dense posting-block
    /// intersection — no rows fetched, no text re-tokenized.
    #[serde(default)]
    pub boolean_block_counts: u64,
    /// Conjunctive position scans: phrase counts and unranked `@@` SELECTs
    /// served from posting blocks with per-candidate position rechecks.
    #[serde(default)]
    pub conjunctive_block_scans: u64,
    /// Ranked BM25 conjunctions served by layering unfolded tail postings
    /// and tombstones over the sealed blocks instead of declining.
    #[serde(default)]
    pub tail_merged_bm25_queries: u64,
}

static DICTIONARY_LOOKUPS: AtomicU64 = AtomicU64::new(0);
static DICTIONARY_MISSES: AtomicU64 = AtomicU64::new(0);
static POSTING_BLOCKS_READ: AtomicU64 = AtomicU64::new(0);
static POSTING_BYTES_READ: AtomicU64 = AtomicU64::new(0);
static POSTINGS_DECODED: AtomicU64 = AtomicU64::new(0);
static DOCUMENT_ID_INTERSECTIONS: AtomicU64 = AtomicU64::new(0);
static SIMD_INTERSECTIONS: AtomicU64 = AtomicU64::new(0);
static WAND_CANDIDATES_SCORED: AtomicU64 = AtomicU64::new(0);
static WAND_POSTINGS_SKIPPED: AtomicU64 = AtomicU64::new(0);
static BLOCK_MAX_CANDIDATES_PRUNED: AtomicU64 = AtomicU64::new(0);
static FILTER_CANDIDATES_REJECTED: AtomicU64 = AtomicU64::new(0);
static POSTING_KEYS_COUNTED_FOR_PLANNING: AtomicU64 = AtomicU64::new(0);
static BLOCK_CACHE_HITS: AtomicU64 = AtomicU64::new(0);
static BLOCK_CACHE_MISSES: AtomicU64 = AtomicU64::new(0);
static PREFETCHED_BLOCKS: AtomicU64 = AtomicU64::new(0);
static POSTING_BLOCK_BOUNDARIES_READ: AtomicU64 = AtomicU64::new(0);
static BOOLEAN_BLOCK_COUNTS: AtomicU64 = AtomicU64::new(0);
static CONJUNCTIVE_BLOCK_SCANS: AtomicU64 = AtomicU64::new(0);
static TAIL_MERGED_BM25: AtomicU64 = AtomicU64::new(0);
static RANKED_QUERIES_PLANNED: AtomicU64 = AtomicU64::new(0);
static RANKED_QUERIES_RUN_PARALLEL: AtomicU64 = AtomicU64::new(0);
static RANKED_WORKERS_WANTED: AtomicU64 = AtomicU64::new(0);
static RANKED_WORKERS_GRANTED: AtomicU64 = AtomicU64::new(0);

pub fn full_text_query_instrumentation() -> FullTextQueryInstrumentation {
    FullTextQueryInstrumentation {
        dictionary_lookups: DICTIONARY_LOOKUPS.load(Ordering::Relaxed),
        dictionary_misses: DICTIONARY_MISSES.load(Ordering::Relaxed),
        posting_blocks_read: POSTING_BLOCKS_READ.load(Ordering::Relaxed),
        posting_bytes_read: POSTING_BYTES_READ.load(Ordering::Relaxed),
        postings_decoded: POSTINGS_DECODED.load(Ordering::Relaxed),
        document_id_intersections: DOCUMENT_ID_INTERSECTIONS.load(Ordering::Relaxed),
        simd_intersections: SIMD_INTERSECTIONS.load(Ordering::Relaxed),
        ranked_queries_planned: RANKED_QUERIES_PLANNED.load(Ordering::Relaxed),
        ranked_queries_run_parallel: RANKED_QUERIES_RUN_PARALLEL.load(Ordering::Relaxed),
        ranked_workers_wanted: RANKED_WORKERS_WANTED.load(Ordering::Relaxed),
        ranked_workers_granted: RANKED_WORKERS_GRANTED.load(Ordering::Relaxed),
        wand_candidates_scored: WAND_CANDIDATES_SCORED.load(Ordering::Relaxed),
        wand_postings_skipped: WAND_POSTINGS_SKIPPED.load(Ordering::Relaxed),
        block_max_candidates_pruned: BLOCK_MAX_CANDIDATES_PRUNED.load(Ordering::Relaxed),
        filter_candidates_rejected: FILTER_CANDIDATES_REJECTED.load(Ordering::Relaxed),
        posting_keys_counted_for_planning: POSTING_KEYS_COUNTED_FOR_PLANNING
            .load(Ordering::Relaxed),
        block_cache_hits: BLOCK_CACHE_HITS.load(Ordering::Relaxed),
        block_cache_misses: BLOCK_CACHE_MISSES.load(Ordering::Relaxed),
        prefetched_blocks: PREFETCHED_BLOCKS.load(Ordering::Relaxed),
        posting_block_boundaries_read: POSTING_BLOCK_BOUNDARIES_READ.load(Ordering::Relaxed),
        boolean_block_counts: BOOLEAN_BLOCK_COUNTS.load(Ordering::Relaxed),
        conjunctive_block_scans: CONJUNCTIVE_BLOCK_SCANS.load(Ordering::Relaxed),
        tail_merged_bm25_queries: TAIL_MERGED_BM25.load(Ordering::Relaxed),
    }
}

pub(crate) fn record_posting_block_boundaries(count: usize) {
    POSTING_BLOCK_BOUNDARIES_READ.fetch_add(count as u64, Ordering::Relaxed);
}

pub(crate) fn record_boolean_block_count() {
    BOOLEAN_BLOCK_COUNTS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_conjunctive_block_scan() {
    CONJUNCTIVE_BLOCK_SCANS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_tail_merged_bm25() {
    TAIL_MERGED_BM25.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_dictionary_lookup(found: bool) {
    DICTIONARY_LOOKUPS.fetch_add(1, Ordering::Relaxed);
    if !found {
        DICTIONARY_MISSES.fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) fn record_posting_block_read(bytes: usize, postings: usize) {
    POSTING_BLOCKS_READ.fetch_add(1, Ordering::Relaxed);
    POSTING_BYTES_READ.fetch_add(bytes as u64, Ordering::Relaxed);
    POSTINGS_DECODED.fetch_add(postings as u64, Ordering::Relaxed);
}

pub(crate) fn record_planning_key_count(count: usize) {
    POSTING_KEYS_COUNTED_FOR_PLANNING.fetch_add(count as u64, Ordering::Relaxed);
}

pub(crate) fn record_document_id_intersection(simd: bool) {
    DOCUMENT_ID_INTERSECTIONS.fetch_add(1, Ordering::Relaxed);
    if simd {
        SIMD_INTERSECTIONS.fetch_add(1, Ordering::Relaxed);
    }
}

/// Record what a ranked query's parallel plan asked for and what it got.
///
/// Parallelism here is *requested*, not guaranteed: extra worker threads come
/// from a global permit pool, so a query issued while the node is busy runs
/// with fewer workers — or none. Without this counter, an operator sees only
/// "the query was slow" and has to infer starvation. `wanted` versus
/// `granted` shows it directly.
pub(crate) fn record_ranked_plan(wanted: usize, granted: usize) {
    RANKED_QUERIES_PLANNED.fetch_add(1, Ordering::Relaxed);
    if granted > 0 {
        RANKED_QUERIES_RUN_PARALLEL.fetch_add(1, Ordering::Relaxed);
    }
    RANKED_WORKERS_WANTED.fetch_add(wanted as u64, Ordering::Relaxed);
    RANKED_WORKERS_GRANTED.fetch_add(granted as u64, Ordering::Relaxed);
}

pub(crate) fn record_wand(scored: usize, skipped: usize, block_max_pruned: usize) {
    WAND_CANDIDATES_SCORED.fetch_add(scored as u64, Ordering::Relaxed);
    WAND_POSTINGS_SKIPPED.fetch_add(skipped as u64, Ordering::Relaxed);
    BLOCK_MAX_CANDIDATES_PRUNED.fetch_add(block_max_pruned as u64, Ordering::Relaxed);
}

pub(crate) fn record_filter_rejections(count: usize) {
    FILTER_CANDIDATES_REJECTED.fetch_add(count as u64, Ordering::Relaxed);
}

pub(crate) fn record_block_cache(hit: bool) {
    if hit {
        BLOCK_CACHE_HITS.fetch_add(1, Ordering::Relaxed);
    } else {
        BLOCK_CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) fn record_prefetched_blocks(count: usize) {
    PREFETCHED_BLOCKS.fetch_add(count as u64, Ordering::Relaxed);
}

pub(crate) fn encode_term_statistics(value: &FullTextTermStatistics) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(BicDbError::from)
}

pub(crate) fn decode_term_statistics(bytes: &[u8]) -> Result<FullTextTermStatistics> {
    serde_json::from_slice(bytes).map_err(BicDbError::from)
}

pub(crate) fn encode_collection_statistics(
    value: &FullTextCollectionStatistics,
) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(BicDbError::from)
}

pub(crate) fn decode_collection_statistics(bytes: &[u8]) -> Result<FullTextCollectionStatistics> {
    serde_json::from_slice(bytes).map_err(BicDbError::from)
}

pub(crate) fn encode_document_statistics(value: &FullTextDocumentStatistics) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(BicDbError::from)
}

pub(crate) fn decode_document_statistics(bytes: &[u8]) -> Result<FullTextDocumentStatistics> {
    serde_json::from_slice(bytes).map_err(BicDbError::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sealed_filter_terms_are_stable_and_validate_input() {
        let first = full_text_filter_term("domain", "example.com").unwrap();
        let repeated = full_text_filter_term("domain", "example.com").unwrap();
        let other_name = full_text_filter_term("host", "example.com").unwrap();

        assert_eq!(first, repeated);
        assert_ne!(first, other_name);
        assert!(first.starts_with("\0bicdb-filter-v1:"));
        assert!(full_text_filter_term("", "example.com").is_err());
        assert!(full_text_filter_term("domain", "").is_err());
    }

    #[test]
    fn stored_text_round_trips_and_is_bounded() {
        let source = "BicDB serves this page directly. "
            .repeat(2_000)
            .into_bytes();
        let encoded = encode_stored_text(&source).unwrap();
        assert_eq!(decode_stored_text(&encoded).unwrap(), source);
        #[cfg(feature = "compression")]
        assert!(encoded.len() < source.len() / 10);

        let oversized = vec![0; MAX_FULL_TEXT_STORED_TEXT_BYTES + 1];
        assert!(encode_stored_text(&oversized).is_err());
    }

    #[test]
    fn generation_v2_remains_readable_after_v3_publication() {
        assert!(full_text_generation_format_is_readable(2));
        assert!(full_text_generation_format_is_readable(3));
        assert!(!full_text_generation_format_is_readable(1));
        assert!(!full_text_generation_format_is_readable(4));
    }
}
