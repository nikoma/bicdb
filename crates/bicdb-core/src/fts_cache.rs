//! Byte-bounded hot cache for decoded immutable FTS posting blocks.

use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::Mutex;
use rustc_hash::FxHashMap;

use crate::paged_collection::NumericBlockPosting;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) struct FullTextBlockCacheKey {
    pub generation: Arc<str>,
    pub term: Arc<[u8]>,
    pub last_document_id: u64,
}

#[derive(Clone)]
pub(crate) struct CachedPostingBlock {
    pub bytes: Arc<[u8]>,
    pub postings: Arc<[NumericBlockPosting]>,
    pub max_rank: f32,
    pub max_term_frequency: u32,
    pub weight_mask: u8,
    charge: usize,
}

impl CachedPostingBlock {
    pub fn uncached(
        bytes: Arc<[u8]>,
        postings: Arc<[NumericBlockPosting]>,
        max_rank: f32,
        max_term_frequency: u32,
        weight_mask: u8,
    ) -> CachedPostingBlock {
        CachedPostingBlock {
            bytes,
            postings,
            max_rank,
            max_term_frequency,
            weight_mask,
            charge: 0,
        }
    }
}

#[derive(Default)]
struct CacheState {
    entries: FxHashMap<FullTextBlockCacheKey, Arc<CachedPostingBlock>>,
    lru: VecDeque<FullTextBlockCacheKey>,
    resident_bytes: usize,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct DictionaryCacheKey {
    generation: Arc<str>,
    term: Arc<[u8]>,
}

struct DictionaryCacheState {
    entries: FxHashMap<DictionaryCacheKey, (crate::fts_format::FullTextTermStatistics, usize)>,
    lru: VecDeque<DictionaryCacheKey>,
    resident_bytes: usize,
}

impl Default for DictionaryCacheState {
    fn default() -> Self {
        Self {
            entries: FxHashMap::default(),
            lru: VecDeque::new(),
            resident_bytes: 0,
        }
    }
}

/// Shared LRU. The configured byte ceiling includes encoded bytes, decoded
/// posting structs and position arrays, plus cache-key storage.
#[derive(Default)]
pub(crate) struct FullTextBlockCache {
    block_capacity_bytes: usize,
    dictionary_capacity_bytes: usize,
    state: Mutex<CacheState>,
    dictionary: Mutex<DictionaryCacheState>,
}

impl std::fmt::Debug for FullTextBlockCache {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock();
        formatter
            .debug_struct("FullTextBlockCache")
            .field(
                "capacity_bytes",
                &self
                    .block_capacity_bytes
                    .saturating_add(self.dictionary_capacity_bytes),
            )
            .field("resident_bytes", &state.resident_bytes)
            .field("entries", &state.entries.len())
            .finish()
    }
}

impl FullTextBlockCache {
    pub fn new(capacity_bytes: usize) -> Self {
        let dictionary_capacity_bytes = (capacity_bytes / 8).min(4 * 1024 * 1024);
        Self {
            block_capacity_bytes: capacity_bytes.saturating_sub(dictionary_capacity_bytes),
            dictionary_capacity_bytes,
            state: Mutex::new(CacheState::default()),
            dictionary: Mutex::new(DictionaryCacheState::default()),
        }
    }

    /// Drop everything cached for one generation. A progressive build keeps
    /// its physical name while its readable content changes — sub-segments
    /// appear, then the final segment replaces them — so both posting blocks
    /// and dictionary statistics cached under that name are stale the moment
    /// the segment set re-registers.
    pub fn purge_generation(&self, generation: &str) {
        {
            let mut state = self.state.lock();
            let stale: Vec<FullTextBlockCacheKey> = state
                .entries
                .keys()
                .filter(|key| key.generation.as_ref() == generation)
                .cloned()
                .collect();
            for key in stale {
                if let Some(entry) = state.entries.remove(&key) {
                    state.resident_bytes = state.resident_bytes.saturating_sub(entry.bytes.len());
                }
            }
            state
                .lru
                .retain(|queued| queued.generation.as_ref() != generation);
        }
        let mut dictionary = self.dictionary.lock();
        let stale: Vec<DictionaryCacheKey> = dictionary
            .entries
            .keys()
            .filter(|key| key.generation.as_ref() == generation)
            .cloned()
            .collect();
        for key in stale {
            if let Some((_, bytes)) = dictionary.entries.remove(&key) {
                dictionary.resident_bytes = dictionary.resident_bytes.saturating_sub(bytes);
            }
        }
        dictionary
            .lru
            .retain(|queued| queued.generation.as_ref() != generation);
    }

    pub fn get(&self, key: &FullTextBlockCacheKey) -> Option<Arc<CachedPostingBlock>> {
        if self.block_capacity_bytes == 0 {
            crate::fts_format::record_block_cache(false);
            return None;
        }
        let mut state = self.state.lock();
        let found = state.entries.get(key).cloned();
        crate::fts_format::record_block_cache(found.is_some());
        if found.is_some() {
            if let Some(position) = state.lru.iter().position(|candidate| candidate == key) {
                state.lru.remove(position);
            }
            state.lru.push_back(key.clone());
        }
        found
    }

    pub fn insert(
        &self,
        key: FullTextBlockCacheKey,
        bytes: Arc<[u8]>,
        postings: Arc<[NumericBlockPosting]>,
        max_rank: f32,
        max_term_frequency: u32,
        weight_mask: u8,
    ) -> Arc<CachedPostingBlock> {
        let charge = cache_charge(&key, &bytes, &postings);
        let candidate = Arc::new(CachedPostingBlock {
            bytes,
            postings,
            max_rank,
            max_term_frequency,
            weight_mask,
            charge,
        });
        if self.block_capacity_bytes == 0 || charge > self.block_capacity_bytes {
            return candidate;
        }
        let mut state = self.state.lock();
        if let Some(existing) = state.entries.get(&key).cloned() {
            return existing;
        }
        while state.resident_bytes.saturating_add(charge) > self.block_capacity_bytes {
            let Some(oldest) = state.lru.pop_front() else {
                break;
            };
            if let Some(removed) = state.entries.remove(&oldest) {
                state.resident_bytes = state.resident_bytes.saturating_sub(removed.charge);
            }
        }
        state.resident_bytes = state.resident_bytes.saturating_add(charge);
        state.lru.push_back(key.clone());
        state.entries.insert(key, Arc::clone(&candidate));
        candidate
    }

    pub fn get_term(
        &self,
        generation: &Arc<str>,
        term: &Arc<[u8]>,
    ) -> Option<crate::fts_format::FullTextTermStatistics> {
        if self.dictionary_capacity_bytes == 0 {
            return None;
        }
        let key = DictionaryCacheKey {
            generation: Arc::clone(generation),
            term: Arc::clone(term),
        };
        let mut state = self.dictionary.lock();
        let found = state.entries.get(&key).map(|(value, _)| value.clone());
        if found.is_some() {
            crate::fts_format::record_dictionary_lookup(true);
            if let Some(position) = state.lru.iter().position(|candidate| candidate == &key) {
                state.lru.remove(position);
            }
            state.lru.push_back(key);
        }
        found
    }

    pub fn insert_term(
        &self,
        generation: Arc<str>,
        term: Arc<[u8]>,
        statistics: crate::fts_format::FullTextTermStatistics,
    ) {
        let key = DictionaryCacheKey { generation, term };
        let charge = std::mem::size_of::<DictionaryCacheKey>()
            .saturating_add(key.generation.len())
            .saturating_add(key.term.len())
            .saturating_add(std::mem::size_of::<crate::fts_format::FullTextTermStatistics>());
        if self.dictionary_capacity_bytes == 0 || charge > self.dictionary_capacity_bytes {
            return;
        }
        let mut state = self.dictionary.lock();
        if state.entries.contains_key(&key) {
            return;
        }
        while state.resident_bytes.saturating_add(charge) > self.dictionary_capacity_bytes {
            let Some(oldest) = state.lru.pop_front() else {
                break;
            };
            if let Some((_, removed_charge)) = state.entries.remove(&oldest) {
                state.resident_bytes = state.resident_bytes.saturating_sub(removed_charge);
            }
        }
        state.resident_bytes = state.resident_bytes.saturating_add(charge);
        state.lru.push_back(key.clone());
        state.entries.insert(key, (statistics, charge));
    }

    #[cfg(test)]
    fn resident_bytes(&self) -> usize {
        self.state.lock().resident_bytes
    }
}

fn cache_charge(
    key: &FullTextBlockCacheKey,
    bytes: &[u8],
    postings: &[NumericBlockPosting],
) -> usize {
    key.generation
        .len()
        .saturating_add(key.term.len())
        .saturating_add(std::mem::size_of::<FullTextBlockCacheKey>())
        .saturating_add(bytes.len())
        .saturating_add(
            postings
                .len()
                .saturating_mul(std::mem::size_of::<NumericBlockPosting>()),
        )
        .saturating_add(
            postings
                .iter()
                .map(|posting| {
                    posting
                        .packed_positions
                        .capacity()
                        .saturating_mul(std::mem::size_of::<u16>())
                })
                .sum::<usize>(),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn posting(document_id: u64) -> NumericBlockPosting {
        NumericBlockPosting {
            document_id,
            doc_length: 1,
            doc_distinct: 1,
            packed_positions: vec![1],
        }
    }

    #[test]
    fn cache_never_exceeds_its_explicit_budget() {
        let cache = FullTextBlockCache::new(320);
        for document_id in 0..20 {
            let key = FullTextBlockCacheKey {
                generation: Arc::from("generation"),
                term: Arc::from([b'x'].as_slice()),
                last_document_id: document_id,
            };
            cache.insert(
                key,
                Arc::from(vec![0u8; 64]),
                Arc::from(vec![posting(document_id)]),
                0.0,
                1,
                1,
            );
            assert!(cache.resident_bytes() <= 320);
        }
    }
}
