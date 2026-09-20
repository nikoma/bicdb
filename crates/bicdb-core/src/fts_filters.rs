//! Adaptive native filters for ranked full-text retrieval.

use crate::error::{BicDbError, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
enum FullTextFilterStorage {
    Dense(Vec<u64>),
    Sparse(Vec<u64>),
}

/// Allowed internal document ids for one FTS generation.
///
/// Sparse facets such as a website domain retain sorted document ids, while
/// dense facets use a bitset. This prevents one low-cardinality `site:` query
/// from allocating one bit for every document in a very large shard.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FullTextDocumentFilter {
    document_count: u64,
    storage: FullTextFilterStorage,
    cardinality: u64,
}

impl FullTextDocumentFilter {
    /// Construct a dense filter without expanding its set bits into document ids.
    ///
    /// The bitset must contain exactly `ceil(document_count / 64)` words, and
    /// unused high bits in its final word must remain clear.
    pub fn from_dense_words(document_count: u64, words: Vec<u64>) -> Result<Self> {
        let expected_words = usize::try_from(document_count.div_ceil(64)).map_err(|_| {
            BicDbError::Index(
                "full-text dense filter word count does not fit this platform".to_string(),
            )
        })?;
        if words.len() != expected_words {
            return Err(BicDbError::Index(format!(
                "full-text dense filter has {} words; expected {expected_words} for {document_count} documents",
                words.len()
            )));
        }
        let trailing_bits = document_count % 64;
        if trailing_bits != 0
            && words
                .last()
                .is_some_and(|word| word & !((1u64 << trailing_bits) - 1) != 0)
        {
            return Err(BicDbError::Index(
                "full-text dense filter has set bits beyond its document count".to_string(),
            ));
        }
        let cardinality = words.iter().map(|word| u64::from(word.count_ones())).sum();
        Ok(Self {
            document_count,
            storage: FullTextFilterStorage::Dense(words),
            cardinality,
        })
    }

    pub fn from_document_ids(
        document_count: u64,
        document_ids: impl IntoIterator<Item = u64>,
    ) -> Self {
        let mut document_ids = document_ids
            .into_iter()
            .filter(|document_id| *document_id < document_count)
            .collect::<Vec<_>>();
        document_ids.sort_unstable();
        document_ids.dedup();
        let cardinality = document_ids.len() as u64;
        let dense_words = document_count.div_ceil(64) as usize;
        let use_sparse = document_ids
            .len()
            .saturating_mul(std::mem::size_of::<u64>())
            < dense_words.saturating_mul(std::mem::size_of::<u64>());
        let storage = if use_sparse {
            FullTextFilterStorage::Sparse(document_ids)
        } else {
            let mut words = vec![0u64; dense_words];
            for document_id in document_ids {
                words[(document_id / 64) as usize] |= 1u64 << (document_id % 64);
            }
            FullTextFilterStorage::Dense(words)
        };
        Self {
            document_count,
            storage,
            cardinality,
        }
    }

    pub fn contains(&self, document_id: u64) -> bool {
        if document_id >= self.document_count {
            return false;
        }
        match &self.storage {
            FullTextFilterStorage::Dense(words) => {
                words[(document_id / 64) as usize] & (1u64 << (document_id % 64)) != 0
            }
            FullTextFilterStorage::Sparse(document_ids) => {
                document_ids.binary_search(&document_id).is_ok()
            }
        }
    }

    pub fn document_count(&self) -> u64 {
        self.document_count
    }

    pub fn cardinality(&self) -> u64 {
        self.cardinality
    }

    /// First allowed id at or after `from`, without enumerating rejected ids.
    pub fn next_at_or_after(&self, from: u64) -> Option<u64> {
        if from >= self.document_count || self.cardinality == 0 {
            return None;
        }
        match &self.storage {
            FullTextFilterStorage::Sparse(ids) => {
                ids.get(ids.partition_point(|id| *id < from)).copied()
            }
            FullTextFilterStorage::Dense(words) => {
                let mut slot = (from / 64) as usize;
                let mut word = words[slot] & (u64::MAX << (from % 64));
                loop {
                    if word != 0 {
                        return Some(slot as u64 * 64 + u64::from(word.trailing_zeros()));
                    }
                    slot += 1;
                    word = *words.get(slot)?;
                }
            }
        }
    }

    pub fn intersect(&self, other: &Self) -> Self {
        let document_count = self.document_count.min(other.document_count);
        if let (FullTextFilterStorage::Dense(left), FullTextFilterStorage::Dense(right)) =
            (&self.storage, &other.storage)
        {
            let words: Vec<u64> = left.iter().zip(right).map(|(a, b)| a & b).collect();
            let cardinality = words.iter().map(|word| u64::from(word.count_ones())).sum();
            return Self {
                document_count,
                storage: FullTextFilterStorage::Dense(words),
                cardinality,
            };
        }
        let (driver, probe) = if self.cardinality <= other.cardinality {
            (self, other)
        } else {
            (other, self)
        };
        let document_ids = driver
            .iter_document_ids()
            .filter(|document_id| *document_id < document_count && probe.contains(*document_id));
        Self::from_document_ids(document_count, document_ids)
    }

    pub fn union(&self, other: &Self) -> Self {
        let document_count = self.document_count.max(other.document_count);
        Self::from_document_ids(
            document_count,
            self.iter_document_ids().chain(other.iter_document_ids()),
        )
    }

    fn iter_document_ids(&self) -> Box<dyn Iterator<Item = u64> + '_> {
        match &self.storage {
            FullTextFilterStorage::Dense(words) => Box::new(
                words
                    .iter()
                    .enumerate()
                    .flat_map(|(word_index, word)| {
                        let mut remaining = *word;
                        std::iter::from_fn(move || {
                            if remaining == 0 {
                                return None;
                            }
                            let bit = remaining.trailing_zeros();
                            remaining &= remaining - 1;
                            Some(word_index as u64 * 64 + u64::from(bit))
                        })
                    })
                    .filter(move |document_id| *document_id < self.document_count),
            ),
            FullTextFilterStorage::Sparse(document_ids) => Box::new(document_ids.iter().copied()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seeking_and_intersection_match_membership_for_dense_sparse_and_unequal_domains() {
        for count in [0, 1, 63, 64, 65, 130, 4097] {
            for stride in [1, 3, 127] {
                let ids: Vec<u64> = (0..count).filter(|id| id % stride == 0).collect();
                let adaptive = FullTextDocumentFilter::from_document_ids(count, ids.clone());
                let mut words = vec![0; count.div_ceil(64) as usize];
                for id in &ids {
                    words[(id / 64) as usize] |= 1 << (id % 64);
                }
                let dense = FullTextDocumentFilter::from_dense_words(count, words).unwrap();
                for filter in [&adaptive, &dense] {
                    for from in 0..=count + 1 {
                        assert_eq!(
                            filter.next_at_or_after(from),
                            ids.iter().copied().find(|id| *id >= from)
                        );
                    }
                    assert_eq!(filter.next_at_or_after(u64::MAX), None);
                    for other_count in [count / 2, count + 65] {
                        let other = FullTextDocumentFilter::from_document_ids(
                            other_count,
                            (0..other_count).filter(|id| id % 2 == 0),
                        );
                        for intersection in [filter.intersect(&other), other.intersect(filter)] {
                            let expected: Vec<_> = ids
                                .iter()
                                .copied()
                                .filter(|id| other.contains(*id))
                                .collect();
                            assert_eq!(intersection.document_count(), count.min(other_count));
                            assert_eq!(intersection.cardinality(), expected.len() as u64);
                            assert_eq!(
                                intersection.iter_document_ids().collect::<Vec<_>>(),
                                expected
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn dense_filter_membership_and_boolean_operations() {
        let left = FullTextDocumentFilter::from_document_ids(130, [1, 64, 129]);
        let right = FullTextDocumentFilter::from_document_ids(130, [2, 64, 129]);
        assert!(left.contains(64));
        assert!(!left.contains(65));
        assert_eq!(left.intersect(&right).cardinality(), 2);
        assert_eq!(left.union(&right).cardinality(), 4);
    }

    #[test]
    fn sparse_filter_does_not_allocate_by_corpus_size() {
        let filter = FullTextDocumentFilter::from_document_ids(2_100_000_000, [7, 42, 9_000]);
        assert_eq!(filter.cardinality(), 3);
        assert!(matches!(filter.storage, FullTextFilterStorage::Sparse(_)));
        assert!(filter.contains(42));
        assert!(!filter.contains(43));
    }

    #[test]
    fn dense_filter_is_selected_for_dense_values() {
        let filter = FullTextDocumentFilter::from_document_ids(128, 0..128);
        assert!(matches!(filter.storage, FullTextFilterStorage::Dense(_)));
        assert!(filter.contains(127));
    }

    #[test]
    fn dense_words_are_validated_without_expanding_document_ids() {
        let filter = FullTextDocumentFilter::from_dense_words(65, vec![0b101, 1]).unwrap();
        assert_eq!(filter.document_count(), 65);
        assert_eq!(filter.cardinality(), 3);
        assert!(filter.contains(0));
        assert!(filter.contains(2));
        assert!(filter.contains(64));
        assert!(!filter.contains(63));

        assert!(FullTextDocumentFilter::from_dense_words(65, vec![0]).is_err());
        assert!(FullTextDocumentFilter::from_dense_words(65, vec![0, 2]).is_err());
        assert!(FullTextDocumentFilter::from_dense_words(0, Vec::new()).is_ok());
    }
}
