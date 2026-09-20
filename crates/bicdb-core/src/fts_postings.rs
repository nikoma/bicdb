//! Search-oriented numeric posting primitives.
//!
//! The intersection entry point dispatches to a small architecture-specific
//! equality probe when available and retains a portable galloping fallback.
//! The algorithm intentionally advances through the longer list in blocks:
//! skewed posting lists avoid a linear walk while similarly sized lists stay
//! cache-friendly.

use std::{collections::VecDeque, sync::Arc};

/// One document returned by the numeric multi-term posting path.
#[derive(Clone, Debug, PartialEq)]
pub struct FullTextConjunctivePosting {
    pub document_id: u64,
    pub primary_key: String,
    pub document_length: u32,
    pub document_distinct_terms: u32,
    /// One slot per requested term; `None` means that optional ranking term
    /// is absent. Required slots are guaranteed to be present.
    pub term_positions: Vec<Option<Vec<u16>>>,
}

/// Ranked result from the multi-term Block-Max WAND path.
#[derive(Clone, Debug, PartialEq)]
pub struct FullTextRankedPosting {
    pub document_id: u64,
    pub primary_key: String,
    pub score: f32,
    pub document_length: u32,
    pub document_distinct_terms: u32,
    pub term_positions: Vec<Option<Vec<u16>>>,
}

#[derive(Clone)]
pub(crate) struct EncodedPostingBlock {
    pub last_document_id: u64,
    pub bytes: Arc<[u8]>,
    /// `Some` when `bytes` is a slim segment block: the docs table plus the
    /// header metadata the term directory serves. `None` means v1 bytes.
    /// Every parse of `bytes` in this module goes through the methods below,
    /// which branch on this — a v1 header parse of slim bytes is refused,
    /// never misread.
    pub slim: Option<crate::paged_collection::SlimBlockContext>,
    pub cache: Option<(
        Arc<crate::fts_cache::FullTextBlockCache>,
        crate::fts_cache::FullTextBlockCacheKey,
    )>,
    pub prefetched: Option<Arc<[crate::paged_collection::NumericBlockPosting]>>,
    pub cached_rank_metadata: Option<(f32, u32, u8)>,
    pub prefetch_blocks: usize,
}

impl EncodedPostingBlock {
    /// Decode the block's score postings in its native representation. Does
    /// not populate the full-postings cache; the BM25 leader stream decodes
    /// scores in bulk and expands full blocks only for winners.
    fn decode_score_postings(
        &self,
    ) -> crate::Result<Vec<crate::paged_collection::NumericBlockScorePosting>> {
        match &self.slim {
            Some(context) => {
                let postings = crate::fts_segment::decode_slim_pk_block_scores(
                    &self.bytes,
                    &context.docs.view(),
                )?;
                crate::fts_format::record_posting_block_read(self.bytes.len(), postings.len());
                Ok(postings)
            }
            None => crate::paged_collection::decode_numeric_posting_block_scores(&self.bytes),
        }
    }

    /// Read-only variant for cursors that consult metadata strictly after
    /// decoding the block (decode populates the cache). A slim block whose
    /// metadata is not yet known cannot be served through `&self` and is
    /// refused loudly rather than misparsed as v1.
    fn rank_metadata_ref(&self) -> crate::Result<(f32, u32, u8)> {
        if let Some(metadata) = self.cached_rank_metadata {
            return Ok(metadata);
        }
        match &self.slim {
            Some(context) => match context.header {
                Some((max_rank, weight_mask)) => {
                    Ok((max_rank, context.max_term_frequency, weight_mask))
                }
                None => Err(crate::BicDbError::PagedStorage(
                    "slim posting block metadata read before decode".to_string(),
                )),
            },
            None => {
                let header = crate::paged_collection::numeric_posting_block_header(&self.bytes)
                    .ok_or_else(|| {
                        crate::BicDbError::PagedStorage("corrupt numeric posting block".to_string())
                    })?;
                Ok((
                    header.max_rank,
                    header.max_term_frequency,
                    header.weight_mask,
                ))
            }
        }
    }

    /// The v1 header triple (max_rank, max_term_frequency, weight_mask),
    /// without decoding when the metadata is already known. A slim block
    /// with no stored header — a single-block term — decodes once and keeps
    /// the postings, which its consumers were about to need anyway.
    fn rank_metadata(&mut self) -> crate::Result<(f32, u32, u8)> {
        if let Some(metadata) = self.cached_rank_metadata {
            return Ok(metadata);
        }
        match &self.slim {
            Some(context) => {
                if let Some((max_rank, weight_mask)) = context.header {
                    let metadata = (max_rank, context.max_term_frequency, weight_mask);
                    self.cached_rank_metadata = Some(metadata);
                    return Ok(metadata);
                }
                decode_encoded_block(self)?;
                self.cached_rank_metadata.ok_or_else(|| {
                    crate::BicDbError::PagedStorage(
                        "slim posting block decode left no header metadata".to_string(),
                    )
                })
            }
            None => {
                let header = crate::paged_collection::numeric_posting_block_header(&self.bytes)
                    .ok_or_else(|| {
                        crate::BicDbError::PagedStorage("corrupt numeric posting block".to_string())
                    })?;
                let metadata = (
                    header.max_rank,
                    header.max_term_frequency,
                    header.weight_mask,
                );
                self.cached_rank_metadata = Some(metadata);
                Ok(metadata)
            }
        }
    }
}

pub(crate) struct WandHit {
    pub document_id: u64,
    pub score: f32,
    pub document_length: u32,
    pub document_distinct_terms: u32,
    pub term_positions: Vec<Option<Vec<u16>>>,
}

fn decode_encoded_block(
    block: &mut EncodedPostingBlock,
) -> crate::Result<Arc<[crate::paged_collection::NumericBlockPosting]>> {
    if let Some(postings) = block.prefetched.as_ref() {
        return Ok(Arc::clone(postings));
    }
    if let Some((cache, key)) = block.cache.as_ref() {
        if let Some(cached) = cache.get(key) {
            block.bytes = Arc::clone(&cached.bytes);
            block.prefetched = Some(Arc::clone(&cached.postings));
            block.cached_rank_metadata = Some((
                cached.max_rank,
                cached.max_term_frequency,
                cached.weight_mask,
            ));
            return Ok(Arc::clone(&cached.postings));
        }
    }
    let (postings, metadata): (
        Arc<[crate::paged_collection::NumericBlockPosting]>,
        (f32, u32, u8),
    ) = match &block.slim {
        Some(context) => {
            let postings =
                crate::fts_segment::decode_slim_pk_block(&block.bytes, &context.docs.view())?;
            crate::fts_format::record_posting_block_read(block.bytes.len(), postings.len());
            let metadata = match context.header {
                Some((max_rank, weight_mask)) => {
                    (max_rank, context.max_term_frequency, weight_mask)
                }
                // Single-block term: the directory stores no header; compute
                // the v1 triple from the postings exactly as the encoder
                // would. Runs once, then lives in the block cache.
                None => crate::db::numeric_run_metadata(&postings),
            };
            (Arc::from(postings), metadata)
        }
        None => {
            let postings = Arc::from(crate::paged_collection::decode_numeric_posting_block(
                &block.bytes,
            )?);
            let header = crate::paged_collection::numeric_posting_block_header(&block.bytes)
                .ok_or_else(|| {
                    crate::BicDbError::PagedStorage("corrupt numeric posting block".to_string())
                })?;
            (
                postings,
                (
                    header.max_rank,
                    header.max_term_frequency,
                    header.weight_mask,
                ),
            )
        }
    };
    if let Some((cache, key)) = block.cache.as_ref() {
        let cached = cache.insert(
            key.clone(),
            Arc::clone(&block.bytes),
            Arc::clone(&postings),
            metadata.0,
            metadata.1,
            metadata.2,
        );
        block.bytes = Arc::clone(&cached.bytes);
        block.prefetched = Some(Arc::clone(&cached.postings));
        block.cached_rank_metadata = Some((
            cached.max_rank,
            cached.max_term_frequency,
            cached.weight_mask,
        ));
        return Ok(Arc::clone(&cached.postings));
    }
    block.prefetched = Some(Arc::clone(&postings));
    block.cached_rank_metadata = Some(metadata);
    Ok(postings)
}

struct TermCursor {
    blocks: Vec<EncodedPostingBlock>,
    block: usize,
    postings: Arc<[crate::paged_collection::NumericBlockPosting]>,
    posting: usize,
    global_upper_bound: f32,
    term_divisor: f32,
}

impl TermCursor {
    fn new(
        blocks: Vec<EncodedPostingBlock>,
        global_upper_bound: f32,
        term_divisor: f32,
    ) -> crate::Result<Self> {
        let mut cursor = Self {
            blocks,
            block: 0,
            postings: Arc::from([]),
            posting: 0,
            global_upper_bound,
            term_divisor,
        };
        cursor.load_block()?;
        Ok(cursor)
    }

    fn load_block(&mut self) -> crate::Result<()> {
        self.postings = Arc::from([]);
        self.posting = 0;
        while self.block < self.blocks.len() {
            self.postings = self.decode_block(self.block)?;
            if !self.postings.is_empty() {
                self.prefetch_upcoming()?;
                return Ok(());
            }
            self.block += 1;
        }
        Ok(())
    }

    fn decode_block(
        &mut self,
        index: usize,
    ) -> crate::Result<Arc<[crate::paged_collection::NumericBlockPosting]>> {
        decode_encoded_block(&mut self.blocks[index])
    }

    fn prefetch_upcoming(&mut self) -> crate::Result<()> {
        let count = self
            .blocks
            .get(self.block)
            .map(|block| block.prefetch_blocks)
            .unwrap_or(0)
            .min(self.blocks.len().saturating_sub(self.block + 1));
        if count == 0 {
            return Ok(());
        }
        // Decoding inline beats fanning out: spawning an OS thread costs an
        // order of magnitude more than decoding the block it would decode.
        // `decode_encoded_block` performs the same cache probe/insert the
        // worker closure used to.
        let mut prefetched = 0usize;
        for index in self.block + 1..self.block + 1 + count {
            if self.blocks[index].prefetched.is_some() {
                continue;
            }
            decode_encoded_block(&mut self.blocks[index])?;
            prefetched += 1;
        }
        crate::fts_format::record_prefetched_blocks(prefetched);
        Ok(())
    }

    fn current(&self) -> Option<&crate::paged_collection::NumericBlockPosting> {
        self.postings.get(self.posting)
    }

    fn current_block_upper_bound(&self) -> crate::Result<f32> {
        let Some(block) = self.blocks.get(self.block) else {
            return Ok(0.0);
        };
        Ok(block.rank_metadata_ref()?.0 / self.term_divisor)
    }

    fn current_block_rank_metadata(&self) -> crate::Result<(u32, u8)> {
        let Some(block) = self.blocks.get(self.block) else {
            return Ok((0, 0));
        };
        let (_, max_term_frequency, weight_mask) = block.rank_metadata_ref()?;
        Ok((max_term_frequency, weight_mask))
    }

    fn current_block_last_document_id(&self) -> Option<u64> {
        self.blocks
            .get(self.block)
            .map(|block| block.last_document_id)
    }

    fn advance(&mut self) -> crate::Result<()> {
        self.posting += 1;
        if self.posting >= self.postings.len() {
            self.block += 1;
            self.load_block()?;
        }
        Ok(())
    }

    fn advance_to(&mut self, target: u64) -> crate::Result<usize> {
        let mut skipped = 0usize;
        loop {
            let Some(current) = self.current() else {
                return Ok(skipped);
            };
            if current.document_id >= target {
                return Ok(skipped);
            }
            let last_document_id = self.blocks[self.block].last_document_id;
            if last_document_id < target {
                skipped = skipped.saturating_add(self.postings.len() - self.posting);
                self.block += 1;
                self.load_block()?;
                continue;
            }
            let offset = self.postings[self.posting..]
                .partition_point(|posting| posting.document_id < target);
            skipped = skipped.saturating_add(offset);
            self.posting += offset;
            if self.posting >= self.postings.len() {
                self.block += 1;
                self.load_block()?;
            }
            return Ok(skipped);
        }
    }
}

/// A ranked cursor which fetches only the document-order block containing a
/// requested id. This matters for web-scale conjunctions: eagerly collecting
/// every block for two common terms makes query setup proportional to both
/// complete posting lists before top-k pruning can begin.
struct SeekingTermCursor<'source> {
    fetch: Box<dyn FnMut(u64) -> crate::Result<Vec<EncodedPostingBlock>> + 'source>,
    block: Option<EncodedPostingBlock>,
    upcoming: VecDeque<EncodedPostingBlock>,
    postings: Arc<[crate::paged_collection::NumericBlockPosting]>,
    posting: usize,
}

impl<'source> SeekingTermCursor<'source> {
    fn new(
        fetch: Box<dyn FnMut(u64) -> crate::Result<Vec<EncodedPostingBlock>> + 'source>,
    ) -> crate::Result<Self> {
        let mut cursor = Self {
            fetch,
            block: None,
            upcoming: VecDeque::new(),
            postings: Arc::from([]),
            posting: 0,
        };
        cursor.load_block_at(0)?;
        Ok(cursor)
    }

    fn load_block_at(&mut self, mut target: u64) -> crate::Result<()> {
        self.block = None;
        self.postings = Arc::from([]);
        self.posting = 0;
        loop {
            while self
                .upcoming
                .front()
                .is_some_and(|block| block.last_document_id < target)
            {
                self.upcoming.pop_front();
            }
            if self.upcoming.is_empty() {
                self.upcoming.extend((self.fetch)(target)?);
            }
            let Some(mut block) = self.upcoming.pop_front() else {
                return Ok(());
            };
            let postings = decode_encoded_block(&mut block)?;
            if !postings.is_empty() {
                let posting = postings.partition_point(|posting| posting.document_id < target);
                if posting < postings.len() {
                    self.block = Some(block);
                    self.postings = postings;
                    self.posting = posting;
                    return Ok(());
                }
            }
            let Some(next) = block.last_document_id.checked_add(1) else {
                return Ok(());
            };
            target = next;
        }
    }

    fn current(&self) -> Option<&crate::paged_collection::NumericBlockPosting> {
        self.postings.get(self.posting)
    }

    fn current_block_rank_metadata(&self) -> crate::Result<(u32, u8)> {
        let Some(block) = self.block.as_ref() else {
            return Ok((0, 0));
        };
        let (_, max_term_frequency, weight_mask) = block.rank_metadata_ref()?;
        Ok((max_term_frequency, weight_mask))
    }

    fn current_block_last_document_id(&self) -> Option<u64> {
        self.block.as_ref().map(|block| block.last_document_id)
    }

    fn advance(&mut self) -> crate::Result<()> {
        self.posting += 1;
        if self.posting < self.postings.len() {
            return Ok(());
        }
        let next = self
            .block
            .as_ref()
            .and_then(|block| block.last_document_id.checked_add(1));
        match next {
            Some(target) => self.load_block_at(target),
            None => {
                self.block = None;
                self.postings = Arc::from([]);
                self.posting = 0;
                Ok(())
            }
        }
    }

    fn advance_to(&mut self, target: u64) -> crate::Result<usize> {
        let Some(current) = self.current() else {
            return Ok(0);
        };
        if current.document_id >= target {
            return Ok(0);
        }
        let remaining = self.postings.len().saturating_sub(self.posting);
        if self
            .current_block_last_document_id()
            .is_some_and(|last| last < target)
        {
            self.load_block_at(target)?;
            return Ok(remaining);
        }
        let offset =
            self.postings[self.posting..].partition_point(|posting| posting.document_id < target);
        self.posting += offset;
        Ok(offset)
    }
}

/// BM25's leader cursor decodes only document ids, lengths and term
/// frequencies. Position vectors are the largest allocation in a common-term
/// scan and BM25 does not need them for intersection or scoring; a full block
/// is expanded only when a candidate enters the retained top-k.
struct Bm25SeekingTermCursor<'source> {
    fetch: Box<dyn FnMut(u64, usize) -> crate::Result<Vec<EncodedPostingBlock>> + 'source>,
    block: Option<EncodedPostingBlock>,
    upcoming: VecDeque<EncodedPostingBlock>,
    postings: Arc<[crate::paged_collection::NumericBlockScorePosting]>,
    posting: usize,
}

impl<'source> Bm25SeekingTermCursor<'source> {
    fn new(
        fetch: Box<dyn FnMut(u64, usize) -> crate::Result<Vec<EncodedPostingBlock>> + 'source>,
    ) -> crate::Result<Self> {
        let mut cursor = Self {
            fetch,
            block: None,
            upcoming: VecDeque::new(),
            postings: Arc::from([]),
            posting: 0,
        };
        cursor.load_block_at(0)?;
        Ok(cursor)
    }

    fn load_block_at(&mut self, mut target: u64) -> crate::Result<()> {
        self.block = None;
        self.postings = Arc::from([]);
        self.posting = 0;
        loop {
            while self
                .upcoming
                .front()
                .is_some_and(|block| block.last_document_id < target)
            {
                self.upcoming.pop_front();
            }
            if self.upcoming.is_empty() {
                self.upcoming
                    .extend((self.fetch)(target, BM25_LEADER_BLOCK_BATCH)?);
            }
            let Some(block) = self.upcoming.pop_front() else {
                return Ok(());
            };
            let postings: Arc<[crate::paged_collection::NumericBlockScorePosting]> =
                Arc::from(block.decode_score_postings()?);
            if !postings.is_empty() {
                let posting = postings.partition_point(|posting| posting.document_id < target);
                if posting < postings.len() {
                    self.block = Some(block);
                    self.postings = postings;
                    self.posting = posting;
                    return Ok(());
                }
            }
            let Some(next) = block.last_document_id.checked_add(1) else {
                return Ok(());
            };
            target = next;
        }
    }

    fn current(&self) -> Option<&crate::paged_collection::NumericBlockScorePosting> {
        self.postings.get(self.posting)
    }

    fn current_block_rank_metadata(&mut self) -> crate::Result<(u32, u8)> {
        let Some(block) = self.block.as_mut() else {
            return Ok((0, 0));
        };
        let (_, max_term_frequency, weight_mask) = block.rank_metadata()?;
        Ok((max_term_frequency, weight_mask))
    }

    fn current_block_bm25_max(
        &self,
        inverse_document_frequency: f32,
        average_document_length: f64,
        parameters: crate::fts_scoring::Bm25Parameters,
    ) -> f32 {
        self.postings
            .iter()
            .map(|posting| {
                crate::fts_scoring::bm25_term_score(
                    posting.term_frequency,
                    inverse_document_frequency,
                    posting.doc_length,
                    average_document_length,
                    parameters,
                )
            })
            .fold(0.0f32, f32::max)
    }

    fn current_block_last_document_id(&self) -> Option<u64> {
        self.block.as_ref().map(|block| block.last_document_id)
    }

    fn advance(&mut self) -> crate::Result<()> {
        self.posting += 1;
        if self.posting < self.postings.len() {
            return Ok(());
        }
        let next = self
            .block
            .as_ref()
            .and_then(|block| block.last_document_id.checked_add(1));
        match next {
            Some(target) => self.load_block_at(target),
            None => {
                self.block = None;
                self.postings = Arc::from([]);
                self.posting = 0;
                Ok(())
            }
        }
    }

    fn advance_to(&mut self, target: u64) -> crate::Result<usize> {
        let Some(current) = self.current() else {
            return Ok(0);
        };
        if current.document_id >= target {
            return Ok(0);
        }
        let remaining = self.postings.len().saturating_sub(self.posting);
        if self
            .current_block_last_document_id()
            .is_some_and(|last| last < target)
        {
            self.load_block_at(target)?;
            return Ok(remaining);
        }
        let offset =
            self.postings[self.posting..].partition_point(|posting| posting.document_id < target);
        self.posting += offset;
        Ok(offset)
    }

    fn positions(&mut self, document_id: u64) -> crate::Result<Vec<u16>> {
        let block = self.block.as_mut().ok_or_else(|| {
            crate::BicDbError::PagedStorage(
                "BM25 winner has no active leader posting block".to_string(),
            )
        })?;
        let postings = decode_encoded_block(block)?;
        postings
            .binary_search_by_key(&document_id, |posting| posting.document_id)
            .ok()
            .and_then(|slot| postings.get(slot))
            .map(|posting| posting.packed_positions.clone())
            .ok_or_else(|| {
                crate::BicDbError::PagedStorage(format!(
                    "BM25 winner {document_id} is absent from its leader posting block"
                ))
            })
    }
}

/// A shallow-seeking cursor for secondary BM25 terms. New numeric indexes use
/// the posting block's upper document id from the B-tree key as a compact skip
/// stream, so moving between blocks does not resolve the MVCC heap value. The
/// encoded posting payload is fetched only when scoring or membership testing
/// actually reaches that block. Callers without a key-only source retain the
/// compatible encoded-block cursor.
/// Document-order batch for the BM25 leader stream, which always advances
/// sequentially.
const BM25_LEADER_BLOCK_BATCH: usize = 256;
/// Secondary batch when no key-only boundary stream is available (the
/// pre-boundary compatibility path fetched payloads in bulk).
const BM25_SECONDARY_FALLBACK_BLOCK_BATCH: usize = 256;
/// Ceiling for the adaptive secondary payload batch. Uniform-impact plateaus
/// load payloads back to back and want amortized descents; scattered drivers
/// want single blocks so unrelated payloads are never resolved.
const BM25_SECONDARY_PAYLOAD_BATCH_MAX: usize = 16;

struct LazySeekingTermCursor<'source> {
    fetch: Box<dyn FnMut(u64, usize) -> crate::Result<Vec<EncodedPostingBlock>> + 'source>,
    fetch_boundaries: Option<Box<dyn FnMut(u64) -> crate::Result<Vec<(u64, u32)>> + 'source>>,
    block: Option<EncodedPostingBlock>,
    boundary: Option<u64>,
    boundary_max_term_frequency: u32,
    upcoming: VecDeque<EncodedPostingBlock>,
    upcoming_boundaries: VecDeque<(u64, u32)>,
    postings: Option<Arc<[crate::paged_collection::NumericBlockScorePosting]>>,
    bm25_max: Option<f32>,
    payload_batch: usize,
    payload_batch_primed: bool,
}

impl<'source> LazySeekingTermCursor<'source> {
    fn new(
        fetch: Box<dyn FnMut(u64, usize) -> crate::Result<Vec<EncodedPostingBlock>> + 'source>,
        fetch_boundaries: Option<Box<dyn FnMut(u64) -> crate::Result<Vec<(u64, u32)>> + 'source>>,
    ) -> Self {
        Self {
            fetch,
            fetch_boundaries,
            block: None,
            boundary: None,
            boundary_max_term_frequency: 0,
            upcoming: VecDeque::new(),
            upcoming_boundaries: VecDeque::new(),
            postings: None,
            bm25_max: None,
            payload_batch: 1,
            payload_batch_primed: false,
        }
    }

    fn clear_current(&mut self) {
        self.block = None;
        self.boundary = None;
        self.boundary_max_term_frequency = 0;
        self.postings = None;
        self.bm25_max = None;
    }

    fn shallow_seek_block(&mut self, target: u64) -> crate::Result<bool> {
        if self
            .current_block_last_document_id()
            .is_some_and(|last_document_id| last_document_id >= target)
        {
            return Ok(true);
        }
        self.clear_current();

        if let Some(fetch_boundaries) = self.fetch_boundaries.as_mut() {
            while self
                .upcoming_boundaries
                .front()
                .is_some_and(|(last_document_id, _)| *last_document_id < target)
            {
                self.upcoming_boundaries.pop_front();
            }
            if self.upcoming_boundaries.is_empty() {
                self.upcoming_boundaries.extend(fetch_boundaries(target)?);
            }
            if let Some((last_document_id, max_term_frequency)) =
                self.upcoming_boundaries.pop_front()
            {
                self.boundary = Some(last_document_id);
                self.boundary_max_term_frequency = max_term_frequency;
            }
            return Ok(self.boundary.is_some());
        }

        while self
            .upcoming
            .front()
            .is_some_and(|block| block.last_document_id < target)
        {
            self.upcoming.pop_front();
        }
        if self.upcoming.is_empty() {
            self.upcoming
                .extend((self.fetch)(target, BM25_SECONDARY_FALLBACK_BLOCK_BATCH)?);
        }
        self.block = self.upcoming.pop_front();
        self.boundary = self.block.as_ref().map(|block| block.last_document_id);
        self.boundary_max_term_frequency = self
            .block
            .as_ref()
            .and_then(|block| match &block.slim {
                Some(context) => Some(context.max_term_frequency),
                None => crate::paged_collection::numeric_posting_block_rank_metadata(&block.bytes)
                    .map(|(max_term_frequency, _)| max_term_frequency),
            })
            .unwrap_or(0);
        Ok(self.block.is_some())
    }

    fn load_block_payload(&mut self) -> crate::Result<bool> {
        if self.block.is_some() {
            return Ok(true);
        }
        let Some(boundary) = self.boundary else {
            return Ok(false);
        };
        // Uniform-impact terms defeat the coarse ceiling, so consecutive
        // windows all load payloads; fetching one block per B-tree descent
        // measurably regressed that shape, while a fixed batch resolved
        // unrelated payloads under scattered drivers. The batch adapts:
        // sequential consumption (the queue drained with nothing discarded)
        // doubles it toward the ceiling, a jump that discards queued blocks
        // resets it to one.
        let mut discarded = 0usize;
        while self
            .upcoming
            .front()
            .is_some_and(|block| block.last_document_id < boundary)
        {
            self.upcoming.pop_front();
            discarded += 1;
        }
        if self.upcoming.is_empty() {
            self.payload_batch = if !self.payload_batch_primed || discarded > 0 {
                1
            } else {
                (self.payload_batch * 2).min(BM25_SECONDARY_PAYLOAD_BATCH_MAX)
            };
            self.payload_batch_primed = true;
            self.upcoming
                .extend((self.fetch)(boundary, self.payload_batch)?);
        }
        if let Some(block) = self.upcoming.pop_front() {
            // A key-only scan may encounter an MVCC-dead key after an online
            // fold. The value lookup skips it and returns the next visible
            // block; adopting that block's real boundary preserves progress.
            self.boundary = Some(block.last_document_id);
            match &block.slim {
                Some(context) => {
                    self.boundary_max_term_frequency = context.max_term_frequency;
                }
                None => {
                    if let Some((max_term_frequency, _)) =
                        crate::paged_collection::numeric_posting_block_rank_metadata(&block.bytes)
                    {
                        self.boundary_max_term_frequency = max_term_frequency;
                    }
                }
            }
            self.block = Some(block);
            return Ok(true);
        }
        Ok(false)
    }

    fn current_block_last_document_id(&self) -> Option<u64> {
        self.boundary
            .or_else(|| self.block.as_ref().map(|block| block.last_document_id))
    }

    fn current_block_max_term_frequency(&self) -> u32 {
        self.boundary_max_term_frequency
    }

    fn current_block_bm25_max(
        &mut self,
        inverse_document_frequency: f32,
        average_document_length: f64,
        parameters: crate::fts_scoring::Bm25Parameters,
    ) -> crate::Result<f32> {
        if let Some(maximum) = self.bm25_max {
            return Ok(maximum);
        }
        if !self.load_block_payload()? {
            return Ok(0.0);
        }
        if self.postings.is_none() {
            let block = self.block.as_ref().expect("loaded secondary block");
            self.postings = Some(Arc::from(block.decode_score_postings()?));
        }
        let maximum = self
            .postings
            .as_ref()
            .expect("decoded BM25 score block")
            .iter()
            .map(|posting| {
                crate::fts_scoring::bm25_term_score(
                    posting.term_frequency,
                    inverse_document_frequency,
                    posting.doc_length,
                    average_document_length,
                    parameters,
                )
            })
            .fold(0.0f32, f32::max);
        self.bm25_max = Some(maximum);
        Ok(maximum)
    }

    fn seek(
        &mut self,
        mut target: u64,
    ) -> crate::Result<Option<crate::paged_collection::NumericBlockScorePosting>> {
        loop {
            if !self.shallow_seek_block(target)? {
                return Ok(None);
            }
            if !self.load_block_payload()? {
                let Some(next) = self
                    .current_block_last_document_id()
                    .and_then(|last_document_id| last_document_id.checked_add(1))
                else {
                    return Ok(None);
                };
                self.clear_current();
                target = next;
                continue;
            }
            if self.postings.is_none() {
                let block = self.block.as_ref().expect("loaded secondary block");
                self.postings = Some(Arc::from(block.decode_score_postings()?));
            }
            let postings = self.postings.as_ref().expect("decoded secondary block");
            let posting = postings.partition_point(|posting| posting.document_id < target);
            if posting < postings.len() {
                return Ok(postings.get(posting).cloned());
            }
            let Some(next) = self
                .current_block_last_document_id()
                .and_then(|last_document_id| last_document_id.checked_add(1))
            else {
                return Ok(None);
            };
            self.clear_current();
            target = next;
        }
    }

    fn positions(&mut self, document_id: u64) -> crate::Result<Vec<u16>> {
        let block = self.block.as_mut().ok_or_else(|| {
            crate::BicDbError::PagedStorage(
                "BM25 winner has no active secondary posting block".to_string(),
            )
        })?;
        let postings = decode_encoded_block(block)?;
        postings
            .binary_search_by_key(&document_id, |posting| posting.document_id)
            .ok()
            .and_then(|slot| postings.get(slot))
            .map(|posting| posting.packed_positions.clone())
            .ok_or_else(|| {
                crate::BicDbError::PagedStorage(format!(
                    "BM25 winner {document_id} is absent from its secondary posting block"
                ))
            })
    }
}

/// Document-at-a-time WAND with global MaxScore pivots and a per-candidate
/// block-max gate. Scores are PostgreSQL-compatible `ts_rank` OR scores:
/// additive single-term contributions divided by the distinct term count.
pub(crate) fn block_max_wand_top_k(
    blocks_by_term: Vec<Vec<EncodedPostingBlock>>,
    global_upper_bounds: &[f32],
    weights: [f32; 4],
    keep: usize,
    filter: Option<&crate::fts_filters::FullTextDocumentFilter>,
) -> crate::Result<Vec<WandHit>> {
    block_max_wand_top_k_in_range(
        blocks_by_term,
        global_upper_bounds,
        weights,
        keep,
        filter,
        0,
        u64::MAX,
    )
}

/// Blocks that can contain a document in `[low, high)`.
///
/// Blocks are document-ordered and each records its LAST document id, so
/// block `i` covers `(last[i-1], last[i]]`. Cloning is cheap — the payload is
/// an `Arc<[u8]>` — so a straddling block is shared between partitions rather
/// than copied or split.
fn blocks_overlapping(
    blocks: &[EncodedPostingBlock],
    low: u64,
    high: u64,
) -> Vec<EncodedPostingBlock> {
    let mut selected = Vec::new();
    let mut block_start = 0u64;
    for block in blocks {
        if block_start >= high {
            break;
        }
        if block.last_document_id >= low {
            selected.push(block.clone());
        }
        block_start = block.last_document_id.saturating_add(1);
    }
    selected
}

/// Below this much work the thread hand-off costs more than it saves.
const MIN_BLOCKS_FOR_PARALLEL_WAND: usize = 64;

/// Block-Max WAND executed across `partitions` document ranges concurrently.
///
/// This is intra-QUERY parallelism: one query using many cores, rather than
/// many queries each using one. At corpus scale it is the difference between
/// a search that is bounded by a single core and one bounded by the machine.
///
/// The traversal is a pure function of already-loaded blocks — no database
/// handle, no lock, no I/O — which is why it parallelises without touching
/// storage, MVCC or the on-disk format.
///
/// **The cost is honest and worth stating**: partitioning weakens WAND's
/// pruning, because each partition must establish its own top-k threshold
/// rather than sharing one global threshold. Total work therefore RISES with
/// the partition count even as wall-clock falls. That trade only pays when
/// there is enough work to amortise it, which is what the block-count floor
/// above enforces.
pub(crate) fn block_max_wand_top_k_parallel(
    blocks_by_term: Vec<Vec<EncodedPostingBlock>>,
    global_upper_bounds: &[f32],
    weights: [f32; 4],
    keep: usize,
    filter: Option<&crate::fts_filters::FullTextDocumentFilter>,
    partitions: usize,
) -> crate::Result<Vec<WandHit>> {
    let total_blocks: usize = blocks_by_term.iter().map(|blocks| blocks.len()).sum();
    if partitions <= 1 || total_blocks < MIN_BLOCKS_FOR_PARALLEL_WAND {
        return block_max_wand_top_k(blocks_by_term, global_upper_bounds, weights, keep, filter);
    }
    let highest_document = blocks_by_term
        .iter()
        .filter_map(|blocks| blocks.last().map(|block| block.last_document_id))
        .max()
        .unwrap_or(0);
    if highest_document == 0 {
        return block_max_wand_top_k(blocks_by_term, global_upper_bounds, weights, keep, filter);
    }

    let span = (highest_document / partitions as u64).saturating_add(1);
    let shards: Vec<(u64, u64, Vec<Vec<EncodedPostingBlock>>)> = (0..partitions)
        .map(|partition| {
            let low = (partition as u64).saturating_mul(span);
            let high = low.saturating_add(span);
            let blocks = blocks_by_term
                .iter()
                .map(|blocks| blocks_overlapping(blocks, low, high))
                .collect();
            (low, high, blocks)
        })
        .filter(
            |(_, _, blocks): &(u64, u64, Vec<Vec<EncodedPostingBlock>>)| {
                blocks.iter().any(|term| !term.is_empty())
            },
        )
        .collect();

    let results: Vec<crate::Result<Vec<WandHit>>> = std::thread::scope(|scope| {
        let handles: Vec<_> = shards
            .into_iter()
            .map(|(low, high, blocks)| {
                scope.spawn(move || {
                    block_max_wand_top_k_in_range(
                        blocks,
                        global_upper_bounds,
                        weights,
                        keep,
                        filter,
                        low,
                        high,
                    )
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap_or_else(|_| Ok(Vec::new())))
            .collect()
    });

    let mut merged = Vec::with_capacity(keep * 2);
    for result in results {
        merged.extend(result?);
    }
    // Same ordering the serial path uses: score descending, document id
    // ascending as the tiebreak, so a parallel run is indistinguishable from
    // a serial one.
    merged.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.document_id.cmp(&right.document_id))
    });
    merged.truncate(keep);
    Ok(merged)
}

/// Block-Max WAND restricted to the document range `[low, high)`.
///
/// The restriction is what makes a single query parallelisable without
/// touching the on-disk format: the document space is split into disjoint
/// ranges, each traversed independently, and the per-range top-k lists are
/// merged. Blocks straddling a boundary appear in both partitions, and the
/// range guard is what keeps a document from being emitted twice.
pub(crate) fn block_max_wand_top_k_in_range(
    blocks_by_term: Vec<Vec<EncodedPostingBlock>>,
    global_upper_bounds: &[f32],
    weights: [f32; 4],
    keep: usize,
    filter: Option<&crate::fts_filters::FullTextDocumentFilter>,
    low: u64,
    high: u64,
) -> crate::Result<Vec<WandHit>> {
    if blocks_by_term.is_empty() || keep == 0 || low >= high {
        return Ok(Vec::new());
    }
    let term_count = blocks_by_term.len();
    let divisor = term_count as f32;
    let mut cursors = blocks_by_term
        .into_iter()
        .enumerate()
        .map(|(slot, blocks)| {
            TermCursor::new(
                blocks,
                global_upper_bounds.get(slot).copied().unwrap_or(1.0) / divisor,
                divisor,
            )
        })
        .collect::<crate::Result<Vec<_>>>()?;
    let mut best = Vec::<WandHit>::with_capacity(keep);
    let mut scored = 0usize;
    let mut skipped = 0usize;
    let mut block_max_pruned = 0usize;

    loop {
        let mut order: Vec<usize> = cursors
            .iter()
            .enumerate()
            .filter_map(|(slot, cursor)| cursor.current().map(|_| slot))
            .collect();
        if order.is_empty() {
            break;
        }
        order.sort_unstable_by_key(|slot| {
            cursors[*slot]
                .current()
                .map(|posting| posting.document_id)
                .unwrap_or(u64::MAX)
        });
        let threshold = if best.len() < keep {
            f32::NEG_INFINITY
        } else {
            best.last()
                .map(|hit| hit.score)
                .unwrap_or(f32::NEG_INFINITY)
        };
        let mut upper_bound = 0.0f32;
        let mut pivot = None;
        for &slot in &order {
            upper_bound += cursors[slot].global_upper_bound;
            if best.len() < keep || upper_bound >= threshold {
                pivot = cursors[slot].current().map(|posting| posting.document_id);
                break;
            }
        }
        let Some(pivot_document) = pivot else {
            break;
        };
        let smallest_document = cursors[order[0]]
            .current()
            .expect("active cursor")
            .document_id;
        // Documents below the partition start belong to the previous
        // partition; documents at or beyond its end belong to the next one.
        // The pivot advances monotonically, so `>= high` ends this partition.
        if pivot_document >= high {
            break;
        }
        if pivot_document < low {
            for slot in 0..cursors.len() {
                skipped = skipped.saturating_add(cursors[slot].advance_to(low)?);
            }
            continue;
        }
        if smallest_document != pivot_document {
            for &slot in &order {
                let document_id = cursors[slot].current().expect("active cursor").document_id;
                if document_id >= pivot_document {
                    break;
                }
                skipped = skipped.saturating_add(cursors[slot].advance_to(pivot_document)?);
            }
            continue;
        }

        let matching: Vec<usize> = order
            .iter()
            .copied()
            .take_while(|slot| {
                cursors[*slot]
                    .current()
                    .is_some_and(|posting| posting.document_id == pivot_document)
            })
            .collect();
        if filter.is_some_and(|filter| !filter.contains(pivot_document)) {
            crate::fts_format::record_filter_rejections(1);
            for slot in matching {
                cursors[slot].advance()?;
            }
            continue;
        }
        let mut candidate_upper_bound = 0.0f32;
        for &slot in &matching {
            candidate_upper_bound += cursors[slot].current_block_upper_bound()?;
        }
        if best.len() >= keep && candidate_upper_bound < threshold {
            block_max_pruned += 1;
            for slot in matching {
                cursors[slot].advance()?;
            }
            continue;
        }

        let mut score = 0.0f32;
        let mut document_length = 0u32;
        let mut document_distinct_terms = 0u32;
        let mut positions = vec![None; term_count];
        for &slot in &matching {
            let posting = cursors[slot].current().expect("matching cursor");
            if document_length == 0 {
                document_length = posting.doc_length;
                document_distinct_terms = posting.doc_distinct;
            }
            score += crate::db::fts_rank_single_term(&posting.packed_positions, weights) / divisor;
            positions[slot] = Some(posting.packed_positions.clone());
        }
        scored += 1;
        let position = best
            .binary_search_by(|existing| {
                score
                    .partial_cmp(&existing.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| existing.document_id.cmp(&pivot_document))
            })
            .unwrap_or_else(|position| position);
        if position < keep {
            best.insert(
                position,
                WandHit {
                    document_id: pivot_document,
                    score,
                    document_length,
                    document_distinct_terms,
                    term_positions: positions,
                },
            );
            best.truncate(keep);
        }
        for slot in matching {
            cursors[slot].advance()?;
        }
    }
    crate::fts_format::record_wand(scored, skipped, block_max_pruned);
    Ok(best)
}

trait RankedAndCursor {
    fn current(&self) -> Option<&crate::paged_collection::NumericBlockPosting>;
    fn current_block_rank_metadata(&self) -> crate::Result<(u32, u8)>;
    fn current_block_last_document_id(&self) -> Option<u64>;
    fn advance(&mut self) -> crate::Result<()>;
    fn advance_to(&mut self, target: u64) -> crate::Result<usize>;
}

impl RankedAndCursor for TermCursor {
    fn current(&self) -> Option<&crate::paged_collection::NumericBlockPosting> {
        self.current()
    }

    fn current_block_rank_metadata(&self) -> crate::Result<(u32, u8)> {
        self.current_block_rank_metadata()
    }

    fn current_block_last_document_id(&self) -> Option<u64> {
        self.current_block_last_document_id()
    }

    fn advance(&mut self) -> crate::Result<()> {
        self.advance()
    }

    fn advance_to(&mut self, target: u64) -> crate::Result<usize> {
        self.advance_to(target)
    }
}

impl RankedAndCursor for SeekingTermCursor<'_> {
    fn current(&self) -> Option<&crate::paged_collection::NumericBlockPosting> {
        self.current()
    }

    fn current_block_rank_metadata(&self) -> crate::Result<(u32, u8)> {
        self.current_block_rank_metadata()
    }

    fn current_block_last_document_id(&self) -> Option<u64> {
        self.current_block_last_document_id()
    }

    fn advance(&mut self) -> crate::Result<()> {
        self.advance()
    }

    fn advance_to(&mut self, target: u64) -> crate::Result<usize> {
        self.advance_to(target)
    }
}

/// Block-Max Ranked AND over already-collected blocks. Retained for focused
/// codec tests and callers which already own compact in-memory posting lists.
#[cfg(test)]
pub(crate) fn block_max_ranked_and_top_k(
    blocks_by_term: Vec<Vec<EncodedPostingBlock>>,
    weights: [f32; 4],
    keep: usize,
    filter: Option<&crate::fts_filters::FullTextDocumentFilter>,
) -> crate::Result<Vec<WandHit>> {
    if blocks_by_term.len() < 2 || keep == 0 {
        return Ok(Vec::new());
    }
    let cursors = blocks_by_term
        .into_iter()
        .map(|blocks| TermCursor::new(blocks, 1.0, 1.0))
        .collect::<crate::Result<Vec<_>>>()?;
    block_max_ranked_and_top_k_with_cursors(cursors, weights, keep, filter)
}

/// Block-Max Ranked AND whose term sources seek directly to the block that
/// can contain a requested document id. Query setup and galloping therefore
/// read only blocks visited by the intersection instead of materializing both
/// complete posting lists.
pub(crate) fn block_max_ranked_and_top_k_seeking<'source>(
    sources: Vec<Box<dyn FnMut(u64) -> crate::Result<Vec<EncodedPostingBlock>> + 'source>>,
    weights: [f32; 4],
    keep: usize,
    filter: Option<&crate::fts_filters::FullTextDocumentFilter>,
) -> crate::Result<Vec<WandHit>> {
    if sources.len() < 2 || keep == 0 {
        return Ok(Vec::new());
    }
    let cursors = sources
        .into_iter()
        .map(SeekingTermCursor::new)
        .collect::<crate::Result<Vec<_>>>()?;
    block_max_ranked_and_top_k_with_cursors(cursors, weights, keep, filter)
}

/// Exact single-term BM25 over document-ordered blocks: postings stream
/// through the score-only decoder, whole blocks are skipped once the
/// retained kth score exceeds first the header's term-frequency ceiling and
/// then the exact block maximum, and positions decode only for hits that
/// enter the top-k. Single-term queries previously had NO block path at all
/// and fell to the exhaustive accumulator — a full positions decode of
/// every posting of the term, which on a broad word over a large corpus is
/// seconds of cold IO for a top-100.
pub(crate) fn block_max_bm25_single_term_seeking<'source>(
    source: Box<dyn FnMut(u64, usize) -> crate::Result<Vec<EncodedPostingBlock>> + 'source>,
    inverse_document_frequency: f32,
    average_document_length: f64,
    parameters: crate::fts_scoring::Bm25Parameters,
    keep: usize,
    filter: Option<&crate::fts_filters::FullTextDocumentFilter>,
) -> crate::Result<Vec<WandHit>> {
    if keep == 0 {
        return Ok(Vec::new());
    }
    let mut cursor = Bm25SeekingTermCursor::new(source)?;
    let mut best: Vec<WandHit> = Vec::with_capacity(keep);
    let mut scored = 0usize;
    let mut skipped = 0usize;
    let mut block_max_pruned = 0usize;
    let mut gated_block: Option<u64> = None;
    while let Some(posting) = cursor.current().copied() {
        if let Some(filter) = filter {
            let Some(allowed) = filter.next_at_or_after(posting.document_id) else {
                break;
            };
            if allowed != posting.document_id {
                crate::fts_format::record_filter_rejections(1);
                skipped = skipped.saturating_add(cursor.advance_to(allowed)?);
                continue;
            }
        }
        let threshold = if best.len() < keep {
            f32::NEG_INFINITY
        } else {
            best.last()
                .map(|hit| hit.score)
                .unwrap_or(f32::NEG_INFINITY)
        };
        let block_end = cursor
            .current_block_last_document_id()
            .expect("active single-term cursor has a block");
        if best.len() >= keep && gated_block != Some(block_end) {
            gated_block = Some(block_end);
            let (max_term_frequency, _) = cursor.current_block_rank_metadata()?;
            let ceiling = crate::fts_scoring::bm25_term_score(
                max_term_frequency.max(1),
                inverse_document_frequency,
                0,
                average_document_length,
                parameters,
            );
            let retire = ceiling <= threshold
                || cursor.current_block_bm25_max(
                    inverse_document_frequency,
                    average_document_length,
                    parameters,
                ) <= threshold;
            if retire {
                block_max_pruned = block_max_pruned.saturating_add(1);
                let Some(next) = block_end.checked_add(1) else {
                    break;
                };
                skipped = skipped.saturating_add(cursor.advance_to(next)?);
                continue;
            }
        }
        if filter.is_some_and(|filter| !filter.contains(posting.document_id)) {
            crate::fts_format::record_filter_rejections(1);
            cursor.advance()?;
            continue;
        }
        let score = crate::fts_scoring::bm25_term_score(
            posting.term_frequency,
            inverse_document_frequency,
            posting.doc_length,
            average_document_length,
            parameters,
        );
        if score > threshold {
            let position = best
                .binary_search_by(|existing| {
                    score
                        .partial_cmp(&existing.score)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then_with(|| existing.document_id.cmp(&posting.document_id))
                })
                .unwrap_or_else(|position| position);
            if position < keep {
                scored = scored.saturating_add(1);
                let positions = cursor.positions(posting.document_id)?;
                best.insert(
                    position,
                    WandHit {
                        document_id: posting.document_id,
                        score,
                        document_length: posting.doc_length,
                        document_distinct_terms: posting.doc_distinct,
                        term_positions: vec![Some(positions)],
                    },
                );
                best.truncate(keep);
            }
        } else {
            skipped = skipped.saturating_add(1);
        }
        cursor.advance()?;
    }
    crate::fts_format::record_wand(scored, skipped, block_max_pruned);
    Ok(best)
}

/// Exact conjunctive BM25 over document-ordered blocks. The first source must
/// be the least-frequent term. Its postings drive score-first candidate
/// generation; secondary posting payloads stay encoded until a leader score
/// can still beat the current top-k threshold.
pub(crate) fn block_max_bm25_and_top_k_seeking<'source>(
    mut sources: Vec<
        Box<dyn FnMut(u64, usize) -> crate::Result<Vec<EncodedPostingBlock>> + 'source>,
    >,
    secondary_boundary_sources: Option<
        Vec<Box<dyn FnMut(u64) -> crate::Result<Vec<(u64, u32)>> + 'source>>,
    >,
    inverse_document_frequencies: &[f32],
    average_document_length: f64,
    parameters: crate::fts_scoring::Bm25Parameters,
    keep: usize,
    filter: Option<&crate::fts_filters::FullTextDocumentFilter>,
    initial_best: Vec<WandHit>,
) -> crate::Result<Vec<WandHit>> {
    if sources.len() < 2 || sources.len() != inverse_document_frequencies.len() || keep == 0 {
        return Ok(Vec::new());
    }
    let leader_source = sources.remove(0);
    let mut leader = Bm25SeekingTermCursor::new(leader_source)?;
    let mut secondaries = match secondary_boundary_sources {
        Some(boundary_sources) if boundary_sources.len() == sources.len() => sources
            .into_iter()
            .zip(boundary_sources)
            .map(|(source, boundaries)| LazySeekingTermCursor::new(source, Some(boundaries)))
            .collect::<Vec<_>>(),
        Some(_) => return Ok(Vec::new()),
        None => sources
            .into_iter()
            .map(|source| LazySeekingTermCursor::new(source, None))
            .collect::<Vec<_>>(),
    };
    let seeded_document_ids = initial_best
        .iter()
        .map(|hit| hit.document_id)
        .collect::<rustc_hash::FxHashSet<_>>();
    let mut best = initial_best;
    best.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.document_id.cmp(&right.document_id))
    });
    best.truncate(keep);
    let mut scored = 0usize;
    let mut skipped = 0usize;
    let mut block_max_pruned = 0usize;

    while let Some(document_id) = leader.current().map(|posting| posting.document_id) {
        if let Some(filter) = filter {
            let Some(allowed) = filter.next_at_or_after(document_id) else {
                break;
            };
            if allowed != document_id {
                crate::fts_format::record_filter_rejections(1);
                skipped = skipped.saturating_add(leader.advance_to(allowed)?);
                continue;
            }
        }
        let mut threshold = if best.len() < keep {
            f32::NEG_INFINITY
        } else {
            best.last()
                .map(|hit| hit.score)
                .unwrap_or(f32::NEG_INFINITY)
        };

        let leader_metadata = leader.current_block_rank_metadata()?;
        let mut window_end = leader
            .current_block_last_document_id()
            .expect("active BM25 leader has a block");
        let leader_block_max = crate::fts_scoring::bm25_term_score(
            leader_metadata.0.max(1),
            inverse_document_frequencies[0],
            0,
            average_document_length,
            parameters,
        );
        let mut secondary_block_max = Vec::with_capacity(secondaries.len());
        for (slot, secondary) in secondaries.iter_mut().enumerate() {
            if !secondary.shallow_seek_block(document_id)? {
                crate::fts_format::record_wand(scored, skipped, block_max_pruned);
                return Ok(best);
            }
            window_end = window_end.min(
                secondary
                    .current_block_last_document_id()
                    .expect("active BM25 secondary has a block"),
            );
            // The shallow scan reads only the fixed rank-header prefix, not
            // the posting payload. Its maximum term frequency supplies the
            // same conservative pre-top-k ceiling as the old full-value scan;
            // the exact maximum below is needed only for competitive windows.
            secondary_block_max.push(crate::fts_scoring::bm25_term_score(
                secondary.current_block_max_term_frequency().max(1),
                inverse_document_frequencies[slot + 1],
                0,
                average_document_length,
                parameters,
            ));
        }
        let mut secondary_block_max_sum = secondary_block_max.iter().sum::<f32>();
        if best.len() >= keep && leader_block_max + secondary_block_max_sum <= threshold {
            block_max_pruned = block_max_pruned.saturating_add(1);
            let Some(next) = window_end.checked_add(1) else {
                break;
            };
            skipped = skipped.saturating_add(leader.advance_to(next)?);
            continue;
        }

        // The v3 on-disk header predates BM25 and carries only maximum term
        // frequency. That is a safe but deliberately loose bound because it
        // cannot account for document-length normalization. Once top-k is
        // populated, derive Tantivy-style exact block maxima from the bytes
        // already fetched. This remains compatible with existing indexes and
        // avoids decoding/cloning secondary postings for blocks which cannot
        // win.
        if best.len() >= keep {
            let exact_leader_block_max = leader.current_block_bm25_max(
                inverse_document_frequencies[0],
                average_document_length,
                parameters,
            );
            for (slot, secondary) in secondaries.iter_mut().enumerate() {
                secondary_block_max[slot] = secondary.current_block_bm25_max(
                    inverse_document_frequencies[slot + 1],
                    average_document_length,
                    parameters,
                )?;
            }
            secondary_block_max_sum = secondary_block_max.iter().sum::<f32>();
            if exact_leader_block_max + secondary_block_max_sum <= threshold {
                block_max_pruned = block_max_pruned.saturating_add(1);
                let Some(next) = window_end.checked_add(1) else {
                    break;
                };
                skipped = skipped.saturating_add(leader.advance_to(next)?);
                continue;
            }
        }

        let mut secondary_suffix_max = vec![0.0f32; secondaries.len()];
        let mut running = 0.0f32;
        for slot in (0..secondaries.len()).rev() {
            secondary_suffix_max[slot] = running;
            running += secondary_block_max[slot];
        }

        while let Some(posting) = leader.current().cloned() {
            if posting.document_id > window_end {
                break;
            }
            let candidate = posting.document_id;
            if seeded_document_ids.contains(&candidate) {
                leader.advance()?;
                continue;
            }
            if filter.is_some_and(|filter| !filter.contains(candidate)) {
                crate::fts_format::record_filter_rejections(1);
                let Some(allowed) = filter.and_then(|filter| filter.next_at_or_after(candidate))
                else {
                    crate::fts_format::record_wand(scored, skipped, block_max_pruned);
                    return Ok(best);
                };
                skipped = skipped.saturating_add(leader.advance_to(allowed)?);
                continue;
            }
            let mut score = crate::fts_scoring::bm25_term_score(
                posting.term_frequency,
                inverse_document_frequencies[0],
                posting.doc_length,
                average_document_length,
                parameters,
            );
            if best.len() >= keep && score + secondary_block_max_sum <= threshold {
                skipped = skipped.saturating_add(1);
                leader.advance()?;
                continue;
            }

            let mut matched = true;
            for (slot, secondary) in secondaries.iter_mut().enumerate() {
                let Some(secondary_posting) = secondary.seek(candidate)? else {
                    matched = false;
                    break;
                };
                if secondary_posting.document_id != candidate {
                    matched = false;
                    break;
                }
                score += crate::fts_scoring::bm25_term_score(
                    secondary_posting.term_frequency,
                    inverse_document_frequencies[slot + 1],
                    secondary_posting.doc_length,
                    average_document_length,
                    parameters,
                );
                if best.len() >= keep && score + secondary_suffix_max[slot] <= threshold {
                    matched = false;
                    break;
                }
            }
            if matched && score > threshold {
                scored = scored.saturating_add(1);
                let position = best
                    .binary_search_by(|existing| {
                        score
                            .partial_cmp(&existing.score)
                            .unwrap_or(std::cmp::Ordering::Equal)
                            .then_with(|| existing.document_id.cmp(&candidate))
                    })
                    .unwrap_or_else(|position| position);
                if position < keep {
                    let mut positions = Vec::with_capacity(secondaries.len() + 1);
                    positions.push(Some(leader.positions(candidate)?));
                    for secondary in &mut secondaries {
                        positions.push(Some(secondary.positions(candidate)?));
                    }
                    best.insert(
                        position,
                        WandHit {
                            document_id: candidate,
                            score,
                            document_length: posting.doc_length,
                            document_distinct_terms: posting.doc_distinct,
                            term_positions: positions,
                        },
                    );
                    best.truncate(keep);
                    if best.len() >= keep {
                        threshold = best
                            .last()
                            .map(|hit| hit.score)
                            .unwrap_or(f32::NEG_INFINITY);
                    }
                }
            } else if !matched {
                skipped = skipped.saturating_add(1);
            }
            leader.advance()?;
        }
    }
    crate::fts_format::record_wand(scored, skipped, block_max_pruned);
    Ok(best)
}

fn block_max_ranked_and_top_k_with_cursors<C: RankedAndCursor>(
    mut cursors: Vec<C>,
    weights: [f32; 4],
    keep: usize,
    filter: Option<&crate::fts_filters::FullTextDocumentFilter>,
) -> crate::Result<Vec<WandHit>> {
    let mut best = Vec::<WandHit>::with_capacity(keep);
    let mut scored = 0usize;
    let mut skipped = 0usize;
    let mut block_max_pruned = 0usize;

    loop {
        let Some(target) = cursors
            .iter()
            .map(RankedAndCursor::current)
            .collect::<Option<Vec<_>>>()
            .map(|postings| {
                postings
                    .into_iter()
                    .map(|posting| posting.document_id)
                    .max()
                    .expect("at least two cursors")
            })
        else {
            break;
        };
        let mut aligned = true;
        for cursor in &mut cursors {
            skipped = skipped.saturating_add(cursor.advance_to(target)?);
            if cursor
                .current()
                .is_none_or(|posting| posting.document_id != target)
            {
                aligned = false;
            }
        }
        if !aligned {
            continue;
        }
        if filter.is_some_and(|filter| !filter.contains(target)) {
            crate::fts_format::record_filter_rejections(1);
            for cursor in &mut cursors {
                cursor.advance()?;
            }
            continue;
        }

        let threshold = if best.len() < keep {
            f32::NEG_INFINITY
        } else {
            best.last()
                .map(|hit| hit.score)
                .unwrap_or(f32::NEG_INFINITY)
        };
        // `rank_and` is a noisy-OR of contributions in [0, 1], so 1.0 is
        // its absolute ceiling. Cursors advance in document-id order and the
        // winner heap prefers lower ids on equal scores; once kth place has
        // saturated, no later document can enter the result set.
        if threshold >= 1.0 {
            break;
        }
        let metadata = cursors
            .iter()
            .map(RankedAndCursor::current_block_rank_metadata)
            .collect::<crate::Result<Vec<_>>>()?;
        let maximum_weight = |mask: u8| {
            (0..4)
                .filter(|weight| mask & (1 << weight) != 0)
                .map(|weight| weights[weight])
                .fold(0.0f32, f32::max)
        };
        // `rank_and` is a noisy-OR over every cross-term position pair.
        // The header records each block's maximum tf and weight mask, so
        // assuming distance weight 1.0 yields a conservative upper bound
        // without decoding a candidate's positions.
        let mut remaining_probability = 1.0f64;
        for index in 1..metadata.len() {
            for previous in &metadata[..index] {
                let pair_max =
                    (maximum_weight(metadata[index].1) * maximum_weight(previous.1)).sqrt();
                let pairs = u64::from(metadata[index].0).saturating_mul(u64::from(previous.0));
                if pairs > i32::MAX as u64 || pair_max >= 1.0 {
                    remaining_probability = 0.0;
                } else {
                    remaining_probability *= (1.0f64 - f64::from(pair_max)).powi(pairs as i32);
                }
            }
        }
        let candidate_upper_bound = (1.0 - remaining_probability) as f32;
        if best.len() >= keep && candidate_upper_bound <= threshold {
            block_max_pruned += 1;
            // The metadata bound applies through the earliest current block
            // boundary. Jump every cursor beyond that whole interval; merely
            // advancing one aligned posting turns "block max" into a linear
            // document-at-a-time scan on common terms.
            let boundary = cursors
                .iter()
                .filter_map(RankedAndCursor::current_block_last_document_id)
                .min()
                .expect("aligned cursors have current blocks");
            let Some(next) = boundary.checked_add(1) else {
                break;
            };
            for cursor in &mut cursors {
                skipped = skipped.saturating_add(cursor.advance_to(next)?);
            }
            continue;
        }

        let owned_positions: Vec<Vec<u16>> = cursors
            .iter()
            .map(|cursor| {
                cursor
                    .current()
                    .expect("aligned cursor")
                    .packed_positions
                    .clone()
            })
            .collect();
        let borrowed_positions: Vec<Option<&[u16]>> = owned_positions
            .iter()
            .map(|positions| Some(positions.as_slice()))
            .collect();
        let score = crate::db::fts_rank_conjunctive(&borrowed_positions, weights);
        let first = cursors[0].current().expect("aligned cursor");
        scored += 1;
        let position = best
            .binary_search_by(|existing| {
                score
                    .partial_cmp(&existing.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| existing.document_id.cmp(&target))
            })
            .unwrap_or_else(|position| position);
        if position < keep {
            best.insert(
                position,
                WandHit {
                    document_id: target,
                    score,
                    document_length: first.doc_length,
                    document_distinct_terms: first.doc_distinct,
                    term_positions: owned_positions.into_iter().map(Some).collect(),
                },
            );
            best.truncate(keep);
        }
        for cursor in &mut cursors {
            cursor.advance()?;
        }
    }
    crate::fts_format::record_wand(scored, skipped, block_max_pruned);
    Ok(best)
}

/// Intersect two ascending, duplicate-free document-id lists.
pub fn intersect_sorted_document_ids(left: &[u64], right: &[u64]) -> Vec<u64> {
    let mut intersection = Vec::with_capacity(left.len().min(right.len()));
    intersect_sorted_document_ids_into(left, right, &mut intersection);
    intersection
}

/// Allocation-reusing form of [`intersect_sorted_document_ids`].
pub fn intersect_sorted_document_ids_into(
    left: &[u64],
    right: &[u64],
    intersection: &mut Vec<u64>,
) {
    intersection.clear();
    crate::fts_format::record_document_id_intersection(simd_available());
    let (needles, haystack) = if left.len() <= right.len() {
        (left, right)
    } else {
        (right, left)
    };
    let mut cursor = 0usize;
    for &needle in needles {
        if simd_equal_at_or_after(haystack, cursor, needle) {
            while cursor < haystack.len() && haystack[cursor] < needle {
                cursor += 1;
            }
        } else {
            cursor = galloping_lower_bound(haystack, cursor, needle);
        }
        if cursor >= haystack.len() {
            break;
        }
        if cursor < haystack.len() && haystack[cursor] == needle {
            intersection.push(needle);
            cursor += 1;
        }
    }
}

fn simd_available() -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        return std::arch::is_x86_feature_detected!("avx2");
    }
    #[cfg(target_arch = "aarch64")]
    {
        return true;
    }
    #[allow(unreachable_code)]
    false
}

fn galloping_lower_bound(values: &[u64], start: usize, needle: u64) -> usize {
    if start >= values.len() || values[start] >= needle {
        return start;
    }
    let remaining = values.len() - start;
    let mut step = 1usize;
    while step < remaining && values[start + step] < needle {
        step = step.saturating_mul(2);
    }
    let low = start + step / 2 + 1;
    let high = (start + step + 1).min(values.len());
    low + values[low..high].partition_point(|value| *value < needle)
}

#[inline]
fn simd_equal_at_or_after(values: &[u64], start: usize, needle: u64) -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx2") {
            // SAFETY: runtime feature detection guards AVX2 and the helper
            // only performs unaligned loads within the checked slice.
            return unsafe { avx2_equal_at_or_after(values, start, needle) };
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        // AArch64 guarantees NEON.
        // SAFETY: the helper only performs loads within the checked slice.
        return unsafe { neon_equal_at_or_after(values, start, needle) };
    }
    values
        .get(start..)
        .and_then(|tail| tail.get(..tail.len().min(4)))
        .is_some_and(|window| window.contains(&needle))
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_equal_at_or_after(values: &[u64], start: usize, needle: u64) -> bool {
    use std::arch::x86_64::{
        __m256i, _mm256_cmpeq_epi64, _mm256_loadu_si256, _mm256_movemask_epi8, _mm256_set1_epi64x,
    };
    if values.len().saturating_sub(start) < 4 {
        return values[start..].contains(&needle);
    }
    // SAFETY: four u64 lanes are available and loadu accepts unaligned input.
    let lanes = unsafe { _mm256_loadu_si256(values.as_ptr().add(start).cast::<__m256i>()) };
    let target = _mm256_set1_epi64x(needle as i64);
    _mm256_movemask_epi8(_mm256_cmpeq_epi64(lanes, target)) != 0
}

#[cfg(target_arch = "aarch64")]
unsafe fn neon_equal_at_or_after(values: &[u64], start: usize, needle: u64) -> bool {
    use std::arch::aarch64::{vceqq_u64, vdupq_n_u64, vgetq_lane_u64, vld1q_u64};
    if values.len().saturating_sub(start) < 2 {
        return values[start..].contains(&needle);
    }
    // SAFETY: two u64 lanes are available; vld1q_u64 permits this pointer.
    let lanes = unsafe { vld1q_u64(values.as_ptr().add(start)) };
    let equal = vceqq_u64(lanes, vdupq_n_u64(needle));
    vgetq_lane_u64(equal, 0) != 0 || vgetq_lane_u64(equal, 1) != 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn posting(
        document_id: u64,
        positions: &[u16],
    ) -> crate::paged_collection::NumericBlockPosting {
        crate::paged_collection::NumericBlockPosting {
            document_id,
            doc_length: 20,
            doc_distinct: 4,
            packed_positions: positions.to_vec(),
        }
    }

    fn block(postings: Vec<crate::paged_collection::NumericBlockPosting>) -> EncodedPostingBlock {
        let max_rank = postings
            .iter()
            .map(|posting| {
                crate::db::fts_rank_single_term(&posting.packed_positions, [0.1, 0.2, 0.4, 1.0])
            })
            .fold(0.0f32, f32::max);
        EncodedPostingBlock {
            slim: None,
            last_document_id: postings.last().unwrap().document_id,
            bytes: Arc::from(crate::paged_collection::encode_numeric_posting_block(
                &postings, 0, max_rank,
            )),
            cache: None,
            prefetched: None,
            cached_rank_metadata: None,
            prefetch_blocks: 0,
        }
    }

    /// Build `terms` posting lists over `documents` documents, chunked into
    /// blocks, with deterministic pseudo-random term occurrence.
    fn corpus(terms: usize, documents: u64, block_size: u64) -> Vec<Vec<EncodedPostingBlock>> {
        (0..terms)
            .map(|term| {
                let mut blocks = Vec::new();
                let mut current = Vec::new();
                for document_id in 1..=documents {
                    // Deterministic, uneven selectivity per term: rarer terms
                    // for higher indices, which is what makes WAND pruning
                    // actually engage.
                    let stride = (term as u64 + 2) * 3 - 1;
                    if document_id % stride != 0 {
                        continue;
                    }
                    let heat = ((document_id.wrapping_mul(2_654_435_761)) % 7) as u16 + 1;
                    let positions: Vec<u16> = (0..heat).collect();
                    current.push(posting(document_id, &positions));
                    if current.len() as u64 >= block_size {
                        blocks.push(block(std::mem::take(&mut current)));
                    }
                }
                if !current.is_empty() {
                    blocks.push(block(current));
                }
                blocks
            })
            .collect()
    }

    fn upper_bounds(count: usize) -> Vec<f32> {
        vec![4.0; count]
    }

    /// THE acceptance property: a parallel search must be indistinguishable
    /// from a serial one. Partitioning changes how the work is scheduled, and
    /// it weakens WAND pruning, but it must never change the ANSWER.
    #[test]
    fn parallel_wand_returns_exactly_what_serial_wand_returns() {
        let documents = 20_000u64;
        for terms in [2usize, 3, 5] {
            for keep in [1usize, 10, 50] {
                let bounds = upper_bounds(terms);
                let serial = block_max_wand_top_k(
                    corpus(terms, documents, 128),
                    &bounds,
                    [0.1, 0.2, 0.4, 1.0],
                    keep,
                    None,
                )
                .unwrap();
                for partitions in [2usize, 4, 8, 16] {
                    let parallel = block_max_wand_top_k_parallel(
                        corpus(terms, documents, 128),
                        &bounds,
                        [0.1, 0.2, 0.4, 1.0],
                        keep,
                        None,
                        partitions,
                    )
                    .unwrap();
                    assert_eq!(
                        serial.len(),
                        parallel.len(),
                        "terms={terms} keep={keep} partitions={partitions}: hit count differs"
                    );
                    for (left, right) in serial.iter().zip(parallel.iter()) {
                        assert_eq!(
                            left.document_id, right.document_id,
                            "terms={terms} keep={keep} partitions={partitions}: \
                             document order differs"
                        );
                        assert!(
                            (left.score - right.score).abs() < 1e-6,
                            "terms={terms} keep={keep} partitions={partitions}: \
                             score differs for document {}",
                            left.document_id
                        );
                    }
                }
            }
        }
    }

    /// A document straddling a partition boundary must be scored once, not
    /// twice and not zero times. Blocks are shared between partitions, so
    /// this is the case the range guard exists for.
    #[test]
    fn documents_on_partition_boundaries_are_emitted_exactly_once() {
        let blocks = corpus(2, 5_000, 64);
        let bounds = upper_bounds(2);
        let hits =
            block_max_wand_top_k_parallel(blocks, &bounds, [0.1, 0.2, 0.4, 1.0], 2_000, None, 8)
                .unwrap();
        let mut seen: Vec<u64> = hits.iter().map(|hit| hit.document_id).collect();
        let total = seen.len();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(
            total,
            seen.len(),
            "a document was emitted by two partitions"
        );
    }

    /// Small queries must not pay for threads they cannot use.
    #[test]
    fn a_small_query_stays_serial() {
        let blocks = corpus(2, 200, 128);
        let total: usize = blocks.iter().map(|term| term.len()).sum();
        assert!(total < MIN_BLOCKS_FOR_PARALLEL_WAND);
        let bounds = upper_bounds(2);
        let serial =
            block_max_wand_top_k(corpus(2, 200, 128), &bounds, [0.1, 0.2, 0.4, 1.0], 10, None)
                .unwrap();
        let parallel =
            block_max_wand_top_k_parallel(blocks, &bounds, [0.1, 0.2, 0.4, 1.0], 10, None, 16)
                .unwrap();
        assert_eq!(serial.len(), parallel.len());
    }

    /// The measurement that decides whether index segmentation is worth
    /// building: can ONE query use N cores?
    ///
    /// Ignored by default because it builds a large corpus. Run with:
    /// `cargo test -p bicdb-core --lib --release parallel_wand_scaling -- --ignored --nocapture`
    #[test]
    #[ignore = "scaling measurement, not a correctness check"]
    fn parallel_wand_scaling_report() {
        let documents = 4_000_000u64;
        let terms = 3usize;
        let keep = 20usize;
        let bounds = upper_bounds(terms);

        let built = std::time::Instant::now();
        let template = corpus(terms, documents, 128);
        let blocks: usize = template.iter().map(|term| term.len()).sum();
        let postings: usize = template
            .iter()
            .flat_map(|term| term.iter())
            .map(|_| 128usize)
            .sum();
        println!(
            "corpus: {documents} documents, {terms} terms, {blocks} blocks (~{postings} postings), \
             built in {:.2?}",
            built.elapsed()
        );

        let serial_started = std::time::Instant::now();
        let serial =
            block_max_wand_top_k(template.clone(), &bounds, [0.1, 0.2, 0.4, 1.0], keep, None)
                .unwrap();
        let serial_elapsed = serial_started.elapsed();
        println!("serial            {:>10.2?}   (baseline)", serial_elapsed);

        for partitions in [2usize, 4, 8, 16, 32] {
            let started = std::time::Instant::now();
            let parallel = block_max_wand_top_k_parallel(
                template.clone(),
                &bounds,
                [0.1, 0.2, 0.4, 1.0],
                keep,
                None,
                partitions,
            )
            .unwrap();
            let elapsed = started.elapsed();
            let speedup = serial_elapsed.as_secs_f64() / elapsed.as_secs_f64();
            println!(
                "partitions {partitions:>3}    {:>10.2?}   {speedup:.2}x",
                elapsed
            );
            assert_eq!(
                serial.iter().map(|hit| hit.document_id).collect::<Vec<_>>(),
                parallel
                    .iter()
                    .map(|hit| hit.document_id)
                    .collect::<Vec<_>>(),
                "partitions={partitions} changed the answer"
            );
        }
        println!(
            "cores available: {:?}",
            std::thread::available_parallelism()
        );
    }

    #[test]
    fn intersection_handles_balanced_and_skewed_lists() {
        assert_eq!(
            intersect_sorted_document_ids(&[1, 3, 5, 8, 13], &[0, 1, 2, 3, 8, 21]),
            vec![1, 3, 8]
        );
        let broad: Vec<u64> = (0..50_000).collect();
        assert_eq!(
            intersect_sorted_document_ids(&[7, 8_192, 49_999, 90_000], &broad),
            vec![7, 8_192, 49_999]
        );
        assert!(intersect_sorted_document_ids(&[], &broad).is_empty());
    }

    #[test]
    fn block_max_wand_matches_exhaustive_or_top_k() {
        let first = vec![
            posting(1, &[1]),
            posting(2, &[1, 2, 3, 4]),
            posting(5, &[1, 2]),
        ];
        let second = vec![
            posting(2, &[1]),
            posting(3, &[1, 2, 3, 4, 5]),
            posting(5, &[1]),
        ];
        let upper = [&first, &second]
            .map(|postings| {
                postings
                    .iter()
                    .map(|posting| {
                        crate::db::fts_rank_single_term(
                            &posting.packed_positions,
                            [0.1, 0.2, 0.4, 1.0],
                        )
                    })
                    .fold(0.0f32, f32::max)
            })
            .to_vec();
        let hits = block_max_wand_top_k(
            vec![vec![block(first.clone())], vec![block(second.clone())]],
            &upper,
            [0.1, 0.2, 0.4, 1.0],
            3,
            None,
        )
        .unwrap();

        let mut exhaustive = Vec::new();
        for document_id in [1, 2, 3, 5] {
            let score = [&first, &second]
                .iter()
                .filter_map(|postings| {
                    postings
                        .iter()
                        .find(|posting| posting.document_id == document_id)
                })
                .map(|posting| {
                    crate::db::fts_rank_single_term(&posting.packed_positions, [0.1, 0.2, 0.4, 1.0])
                        / 2.0
                })
                .sum::<f32>();
            exhaustive.push((document_id, score));
        }
        exhaustive.sort_by(|left, right| {
            right
                .1
                .partial_cmp(&left.1)
                .unwrap()
                .then_with(|| left.0.cmp(&right.0))
        });
        exhaustive.truncate(3);
        assert_eq!(
            hits.iter()
                .map(|hit| (hit.document_id, hit.score))
                .collect::<Vec<_>>(),
            exhaustive
        );
    }

    #[test]
    fn block_max_ranked_and_matches_exhaustive_intersection() {
        let first = vec![
            posting(1, &[1]),
            posting(2, &[1, 4, 8]),
            posting(5, &[1, 40]),
        ];
        let second = vec![
            posting(2, &[2, 5]),
            posting(3, &[1, 2, 3]),
            posting(5, &[39, 41]),
        ];
        let hits = block_max_ranked_and_top_k(
            vec![vec![block(first.clone())], vec![block(second.clone())]],
            [0.1, 0.2, 0.4, 1.0],
            2,
            None,
        )
        .unwrap();
        let mut exhaustive = [2u64, 5]
            .into_iter()
            .map(|document_id| {
                let left = first
                    .iter()
                    .find(|posting| posting.document_id == document_id)
                    .unwrap();
                let right = second
                    .iter()
                    .find(|posting| posting.document_id == document_id)
                    .unwrap();
                (
                    document_id,
                    crate::db::fts_rank_conjunctive(
                        &[
                            Some(left.packed_positions.as_slice()),
                            Some(right.packed_positions.as_slice()),
                        ],
                        [0.1, 0.2, 0.4, 1.0],
                    ),
                )
            })
            .collect::<Vec<_>>();
        exhaustive.sort_by(|left, right| {
            right
                .1
                .partial_cmp(&left.1)
                .unwrap()
                .then_with(|| left.0.cmp(&right.0))
        });
        assert_eq!(
            hits.iter()
                .map(|hit| (hit.document_id, hit.score))
                .collect::<Vec<_>>(),
            exhaustive
        );
    }

    #[test]
    fn seeking_block_max_bm25_matches_exhaustive_intersection() {
        fn with_length(
            document_id: u64,
            term_frequency: usize,
            document_length: u32,
        ) -> crate::paged_collection::NumericBlockPosting {
            crate::paged_collection::NumericBlockPosting {
                document_id,
                doc_length: document_length,
                doc_distinct: 4,
                packed_positions: (0..term_frequency as u16).collect(),
            }
        }

        let left = vec![
            with_length(1, 1, 80),
            with_length(2, 6, 300),
            with_length(5, 2, 40),
            with_length(8, 12, 900),
        ];
        let right = vec![
            with_length(2, 2, 300),
            with_length(3, 10, 50),
            with_length(5, 3, 40),
            with_length(8, 1, 900),
        ];
        let blocks_by_term = [left.clone(), right.clone()].map(|postings| {
            Arc::new(
                postings
                    .chunks(2)
                    .map(|postings| block(postings.to_vec()))
                    .collect::<Vec<_>>(),
            )
        });
        let sources = blocks_by_term
            .into_iter()
            .map(|blocks| {
                Box::new(move |target: u64, _batch: usize| {
                    let first =
                        blocks.partition_point(|candidate| candidate.last_document_id < target);
                    Ok(blocks.get(first..).unwrap_or_default().to_vec())
                })
                    as Box<dyn FnMut(u64, usize) -> crate::Result<Vec<EncodedPostingBlock>>>
            })
            .collect();
        let inverse_document_frequencies = [1.25, 2.5];
        let average_document_length = 200.0;
        let parameters = crate::fts_scoring::Bm25Parameters::default();
        let hits = block_max_bm25_and_top_k_seeking(
            sources,
            None,
            &inverse_document_frequencies,
            average_document_length,
            parameters,
            2,
            None,
            Vec::new(),
        )
        .unwrap();

        let mut exhaustive = [2u64, 5, 8]
            .into_iter()
            .map(|document_id| {
                let postings = [&left, &right].map(|postings| {
                    postings
                        .iter()
                        .find(|posting| posting.document_id == document_id)
                        .unwrap()
                });
                let score = postings
                    .iter()
                    .zip(inverse_document_frequencies)
                    .map(|(posting, inverse_document_frequency)| {
                        crate::fts_scoring::bm25_term_score(
                            posting.packed_positions.len() as u32,
                            inverse_document_frequency,
                            posting.doc_length,
                            average_document_length,
                            parameters,
                        )
                    })
                    .sum::<f32>();
                (document_id, score)
            })
            .collect::<Vec<_>>();
        exhaustive.sort_by(|left, right| {
            right
                .1
                .partial_cmp(&left.1)
                .unwrap()
                .then_with(|| left.0.cmp(&right.0))
        });
        exhaustive.truncate(2);

        assert_eq!(
            hits.iter()
                .map(|hit| (hit.document_id, hit.score))
                .collect::<Vec<_>>(),
            exhaustive
        );
        for hit in &hits {
            let expected = [&left, &right]
                .map(|postings| {
                    postings
                        .iter()
                        .find(|posting| posting.document_id == hit.document_id)
                        .unwrap()
                        .packed_positions
                        .clone()
                })
                .into_iter()
                .map(Some)
                .collect::<Vec<_>>();
            assert_eq!(hit.term_positions, expected);
        }
    }

    #[test]
    fn seeking_block_max_bm25_batches_at_41m_document_id_scale() {
        use std::{cell::Cell, rc::Rc};

        const MAX_DOCUMENT_ID: u64 = 41_000_000;
        const BLOCK_STEP: u64 = 10_000;
        const BLOCK_BATCH: u64 = 256;

        let source = |loads: Rc<Cell<usize>>| {
            Box::new(move |target: u64, batch: usize| {
                loads.set(loads.get() + 1);
                let first = if target == 0 {
                    0
                } else {
                    target.div_ceil(BLOCK_STEP) * BLOCK_STEP
                };
                Ok((0..batch as u64)
                    .map(|offset| first.saturating_add(offset * BLOCK_STEP))
                    .take_while(|document_id| *document_id <= MAX_DOCUMENT_ID)
                    .map(|document_id| {
                        let positions = if document_id < 1_000_000 {
                            (0..24).collect::<Vec<_>>()
                        } else {
                            vec![0]
                        };
                        block(vec![posting(document_id, &positions)])
                    })
                    .collect::<Vec<_>>())
            }) as Box<dyn FnMut(u64, usize) -> crate::Result<Vec<EncodedPostingBlock>>>
        };
        let boundary_source = |loads: Rc<Cell<usize>>| {
            Box::new(move |target: u64| {
                loads.set(loads.get() + 1);
                let first = if target == 0 {
                    0
                } else {
                    target.div_ceil(BLOCK_STEP) * BLOCK_STEP
                };
                Ok((0..BLOCK_BATCH)
                    .map(|offset| first.saturating_add(offset * BLOCK_STEP))
                    .take_while(|document_id| *document_id <= MAX_DOCUMENT_ID)
                    .map(|document_id| {
                        let max_term_frequency = if document_id < 1_000_000 { 24u32 } else { 1u32 };
                        (document_id, max_term_frequency)
                    })
                    .collect::<Vec<_>>())
            }) as Box<dyn FnMut(u64) -> crate::Result<Vec<(u64, u32)>>>
        };
        let left_loads = Rc::new(Cell::new(0));
        let right_payload_loads = Rc::new(Cell::new(0));
        let right_boundary_loads = Rc::new(Cell::new(0));
        let hits = block_max_bm25_and_top_k_seeking(
            vec![
                source(Rc::clone(&left_loads)),
                source(Rc::clone(&right_payload_loads)),
            ],
            Some(vec![boundary_source(Rc::clone(&right_boundary_loads))]),
            &[1.0, 1.0],
            20.0,
            crate::fts_scoring::Bm25Parameters::default(),
            100,
            None,
            Vec::new(),
        )
        .unwrap();

        assert_eq!(hits.len(), 100);
        assert_eq!(
            hits.iter().map(|hit| hit.document_id).collect::<Vec<_>>(),
            (0..100).map(|slot| slot * BLOCK_STEP).collect::<Vec<_>>()
        );
        assert!(
            left_loads.get() <= 18 && right_boundary_loads.get() <= 18,
            "4,101 posting blocks should require about 17 batched skip descents"
        );
        assert!(
            right_payload_loads.get() <= 12,
            "shallow seeking must not fetch the noncompetitive secondary tail"
        );
    }

    #[test]
    fn seeking_block_max_bm25_matches_exhaustive_on_dense_varied_blocks() {
        fn generated_posting(
            document_id: u64,
            salt: u64,
        ) -> crate::paged_collection::NumericBlockPosting {
            let term_frequency = 1 + ((document_id * 17 + salt * 13) % 24) as usize;
            let document_length =
                term_frequency as u32 + 10 + ((document_id * 97 + salt * 31) % 1_400) as u32;
            crate::paged_collection::NumericBlockPosting {
                document_id,
                doc_length: document_length,
                doc_distinct: document_length.min(600),
                packed_positions: (0..term_frequency as u16)
                    .map(|position| position.saturating_mul(3))
                    .collect(),
            }
        }

        let terms = [
            (0..4_000)
                .filter(|document_id| document_id % 2 == 0)
                .map(|document_id| generated_posting(document_id, 1))
                .collect::<Vec<_>>(),
            (0..4_000)
                .filter(|document_id| document_id % 3 != 0)
                .map(|document_id| generated_posting(document_id, 2))
                .collect::<Vec<_>>(),
            (0..4_000)
                .filter(|document_id| document_id % 5 != 0)
                .map(|document_id| generated_posting(document_id, 3))
                .collect::<Vec<_>>(),
        ];
        let inverse_document_frequencies = [1.1, 1.7, 2.3];
        let average_document_length = 420.0;
        let parameters = crate::fts_scoring::Bm25Parameters::default();

        for keep in [1, 7, 100] {
            let sources = terms
                .iter()
                .map(|postings| {
                    let blocks = Arc::new(
                        postings
                            .chunks(31)
                            .map(|postings| block(postings.to_vec()))
                            .collect::<Vec<_>>(),
                    );
                    Box::new(move |target: u64, _batch: usize| {
                        let first =
                            blocks.partition_point(|candidate| candidate.last_document_id < target);
                        Ok(blocks
                            .get(first..)
                            .unwrap_or_default()
                            .iter()
                            .take(8)
                            .cloned()
                            .collect())
                    })
                        as Box<dyn FnMut(u64, usize) -> crate::Result<Vec<EncodedPostingBlock>>>
                })
                .collect();
            let hits = block_max_bm25_and_top_k_seeking(
                sources,
                None,
                &inverse_document_frequencies,
                average_document_length,
                parameters,
                keep,
                None,
                Vec::new(),
            )
            .unwrap();

            let mut exhaustive = terms[0]
                .iter()
                .filter_map(|first| {
                    let postings = [
                        Some(first),
                        terms[1]
                            .binary_search_by_key(&first.document_id, |posting| posting.document_id)
                            .ok()
                            .and_then(|slot| terms[1].get(slot)),
                        terms[2]
                            .binary_search_by_key(&first.document_id, |posting| posting.document_id)
                            .ok()
                            .and_then(|slot| terms[2].get(slot)),
                    ];
                    let postings = postings.into_iter().collect::<Option<Vec<_>>>()?;
                    let score = postings
                        .iter()
                        .zip(inverse_document_frequencies)
                        .map(|(posting, inverse_document_frequency)| {
                            crate::fts_scoring::bm25_term_score(
                                posting.packed_positions.len().max(1) as u32,
                                inverse_document_frequency,
                                posting.doc_length,
                                average_document_length,
                                parameters,
                            )
                        })
                        .sum::<f32>();
                    Some((first.document_id, score, postings))
                })
                .collect::<Vec<_>>();
            exhaustive.sort_by(|left, right| {
                right
                    .1
                    .partial_cmp(&left.1)
                    .unwrap()
                    .then_with(|| left.0.cmp(&right.0))
            });
            exhaustive.truncate(keep);

            assert_eq!(
                hits.iter()
                    .map(|hit| (hit.document_id, hit.score))
                    .collect::<Vec<_>>(),
                exhaustive
                    .iter()
                    .map(|(document_id, score, _)| (*document_id, *score))
                    .collect::<Vec<_>>()
            );
            for (hit, (_, _, postings)) in hits.iter().zip(&exhaustive) {
                assert_eq!(
                    hit.term_positions,
                    postings
                        .iter()
                        .map(|posting| Some(posting.packed_positions.clone()))
                        .collect::<Vec<_>>()
                );
            }
        }
    }

    #[test]
    fn seeking_ranked_and_does_not_scan_the_common_terms_complete_posting_list() {
        use std::{cell::Cell, rc::Rc};

        const MAX_DOCUMENT_ID: u64 = 1_000_000;
        const SPARSE_STEP: u64 = 10_000;
        const DENSE_BLOCK_DOCUMENTS: u64 = 128;

        let sparse_loads = Rc::new(Cell::new(0usize));
        let sparse_loads_for_source = Rc::clone(&sparse_loads);
        let sparse = Box::new(move |target: u64| {
            sparse_loads_for_source.set(sparse_loads_for_source.get() + 1);
            let document_id = if target == 0 {
                0
            } else {
                target.div_ceil(SPARSE_STEP) * SPARSE_STEP
            };
            if document_id > MAX_DOCUMENT_ID {
                return Ok(Vec::new());
            }
            let positions = if document_id == 0 {
                (1..=24).collect::<Vec<_>>()
            } else {
                vec![1]
            };
            Ok(vec![block(vec![posting(document_id, &positions)])])
        });

        let dense_loads = Rc::new(Cell::new(0usize));
        let dense_loads_for_source = Rc::clone(&dense_loads);
        let dense = Box::new(move |target: u64| {
            dense_loads_for_source.set(dense_loads_for_source.get() + 1);
            if target > MAX_DOCUMENT_ID {
                return Ok(Vec::new());
            }
            let first = (target / DENSE_BLOCK_DOCUMENTS) * DENSE_BLOCK_DOCUMENTS;
            let last = MAX_DOCUMENT_ID.min(first + DENSE_BLOCK_DOCUMENTS - 1);
            let postings = (first..=last)
                .map(|document_id| {
                    let positions = if document_id == 0 {
                        (2..=25).collect::<Vec<_>>()
                    } else {
                        vec![2]
                    };
                    posting(document_id, &positions)
                })
                .collect();
            Ok(vec![block(postings)])
        });

        let sources: Vec<Box<dyn FnMut(u64) -> crate::Result<Vec<EncodedPostingBlock>>>> =
            vec![sparse, dense];
        let hits =
            block_max_ranked_and_top_k_seeking(sources, [0.1, 0.2, 0.4, 1.0], 1, None).unwrap();

        assert_eq!(
            hits.iter().map(|hit| hit.document_id).collect::<Vec<_>>(),
            vec![0]
        );
        assert!(sparse_loads.get() <= 103, "one seek per sparse block");
        assert!(
            dense_loads.get() <= 103,
            "dense list must gallop to sparse candidates, not read its ~7,813 blocks"
        );
    }

    #[test]
    fn seeking_ranked_and_batches_adjacent_driver_blocks() {
        use std::{cell::Cell, rc::Rc};

        // Match the production Wikipedia corpus' document-id scale without
        // allocating a 41-million-entry posting list. The sparse operand has
        // 4,101 blocks spread over that full key space.
        const MAX_DOCUMENT_ID: u64 = 41_000_000;
        const DRIVER_STEP: u64 = 10_000;
        const DRIVER_BATCH: u64 = 16;
        const DENSE_BLOCK_DOCUMENTS: u64 = 128;

        let driver_loads = Rc::new(Cell::new(0usize));
        let driver_loads_for_source = Rc::clone(&driver_loads);
        let driver = Box::new(move |target: u64| {
            driver_loads_for_source.set(driver_loads_for_source.get() + 1);
            let first = if target == 0 {
                0
            } else {
                target.div_ceil(DRIVER_STEP) * DRIVER_STEP
            };
            Ok((0..DRIVER_BATCH)
                .map(|offset| first.saturating_add(offset * DRIVER_STEP))
                .take_while(|document_id| *document_id <= MAX_DOCUMENT_ID)
                .map(|document_id| block(vec![posting(document_id, &[1])]))
                .collect::<Vec<_>>())
        });

        let dense_loads = Rc::new(Cell::new(0usize));
        let dense_loads_for_source = Rc::clone(&dense_loads);
        let dense = Box::new(move |target: u64| {
            dense_loads_for_source.set(dense_loads_for_source.get() + 1);
            if target > MAX_DOCUMENT_ID {
                return Ok(Vec::new());
            }
            let first = (target / DENSE_BLOCK_DOCUMENTS) * DENSE_BLOCK_DOCUMENTS;
            let last = MAX_DOCUMENT_ID.min(first + DENSE_BLOCK_DOCUMENTS - 1);
            Ok(vec![block(
                (first..=last)
                    .map(|document_id| posting(document_id, &[2]))
                    .collect(),
            )])
        });

        let sources: Vec<Box<dyn FnMut(u64) -> crate::Result<Vec<EncodedPostingBlock>>>> =
            vec![driver, dense];
        let hits =
            block_max_ranked_and_top_k_seeking(sources, [0.1, 0.2, 0.4, 1.0], 100, None).unwrap();

        assert_eq!(hits.len(), 100);
        assert!(
            driver_loads.get() <= 258,
            "4,101 adjacent driver blocks should take about 257 range descents, got {}",
            driver_loads.get()
        );
        assert!(
            dense_loads.get() <= 4_102,
            "the dense operand must still seek instead of scanning every dense block"
        );
    }

    #[test]
    fn seeking_ranked_and_stops_at_saturated_top_k() {
        use std::{cell::Cell, rc::Rc};

        const MAX_DOCUMENT_ID: u64 = 41_000_000;
        const BLOCK_DOCUMENTS: u64 = 128;
        const A_WEIGHT: u16 = 3 << 14;

        let left_positions = (1..=24)
            .map(|position| A_WEIGHT | position)
            .collect::<Vec<_>>();
        let right_positions = (25..=48)
            .map(|position| A_WEIGHT | position)
            .collect::<Vec<_>>();
        assert_eq!(
            crate::db::fts_rank_conjunctive(
                &[Some(&left_positions), Some(&right_positions)],
                [0.1, 0.2, 0.4, 1.0],
            ),
            1.0,
            "the regression corpus must saturate rank_and"
        );

        let source = |positions: Vec<u16>, loads: Rc<Cell<usize>>| {
            Box::new(move |target: u64| {
                loads.set(loads.get() + 1);
                if target > MAX_DOCUMENT_ID {
                    return Ok(Vec::new());
                }
                let first = (target / BLOCK_DOCUMENTS) * BLOCK_DOCUMENTS;
                let last = MAX_DOCUMENT_ID.min(first + BLOCK_DOCUMENTS - 1);
                Ok(vec![block(
                    (first..=last)
                        .map(|document_id| posting(document_id, &positions))
                        .collect(),
                )])
            }) as Box<dyn FnMut(u64) -> crate::Result<Vec<EncodedPostingBlock>>>
        };
        let left_loads = Rc::new(Cell::new(0));
        let right_loads = Rc::new(Cell::new(0));
        let hits = block_max_ranked_and_top_k_seeking(
            vec![
                source(left_positions, Rc::clone(&left_loads)),
                source(right_positions, Rc::clone(&right_loads)),
            ],
            [0.1, 0.2, 0.4, 1.0],
            100,
            None,
        )
        .unwrap();

        assert_eq!(hits.len(), 100);
        assert_eq!(
            hits.iter().map(|hit| hit.document_id).collect::<Vec<_>>(),
            (0..100).collect::<Vec<_>>()
        );
        assert_eq!(left_loads.get(), 1);
        assert_eq!(right_loads.get(), 1);
    }
}
