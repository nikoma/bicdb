//! BM25 and BM25F scoring over persisted FTS corpus statistics.

use serde::{Deserialize, Serialize};
use std::sync::OnceLock;

/// Okapi BM25 tuning parameters.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Bm25Parameters {
    pub k1: f32,
    pub b: f32,
}

impl Default for Bm25Parameters {
    fn default() -> Self {
        Self { k1: 1.2, b: 0.75 }
    }
}

/// BM25F field boosts and length-normalization parameters. Field order is
/// tsvector D, C, B, A.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Bm25fParameters {
    pub k1: f32,
    pub field_boosts: [f32; 4],
    pub field_b: [f32; 4],
}

impl Default for Bm25fParameters {
    fn default() -> Self {
        Self {
            k1: 1.2,
            field_boosts: [1.0, 1.5, 2.0, 3.0],
            field_b: [0.75; 4],
        }
    }
}

/// Robertson/Sparck Jones IDF with the common non-negative BM25 form.
pub fn bm25_inverse_document_frequency(document_count: u64, document_frequency: u64) -> f32 {
    if document_count == 0 || document_frequency == 0 {
        return 0.0;
    }
    let documents = document_count as f64;
    let frequency = document_frequency.min(document_count) as f64;
    (1.0 + (documents - frequency + 0.5) / (frequency + 0.5)).ln() as f32
}

/// One term's BM25 contribution.
pub fn bm25_term_score(
    term_frequency: u32,
    inverse_document_frequency: f32,
    document_length: u32,
    average_document_length: f64,
    parameters: Bm25Parameters,
) -> f32 {
    if term_frequency == 0 || inverse_document_frequency <= 0.0 {
        return 0.0;
    }
    let average = average_document_length.max(1.0) as f32;
    let normalized_length = document_length as f32 / average;
    let denominator = term_frequency as f32
        + parameters.k1 * (1.0 - parameters.b + parameters.b * normalized_length);
    inverse_document_frequency * (term_frequency as f32 * (parameters.k1 + 1.0))
        / denominator.max(f32::MIN_POSITIVE)
}

/// Largest unweighted (D-field) term frequency which can occur at or below
/// an impact bucket. Impact sidecars are ordered by the default-weight
/// single-term `ts_rank`; for an all-D generation that ordering is monotone
/// in term frequency. The lookup lets BM25 threshold retrieval translate the
/// next unread impact block into a conservative term-score ceiling.
pub(crate) fn unweighted_term_frequency_ceiling(impact_bucket: u16) -> u32 {
    static BUCKETS: OnceLock<Vec<u16>> = OnceLock::new();
    let buckets = BUCKETS.get_or_init(|| {
        const ZETA_2: f32 = 1.644_934_1;
        const D_WEIGHT: f32 = 0.1;
        let mut values = Vec::with_capacity(u16::MAX as usize);
        let mut reciprocal_square_sum = 0.0f32;
        for frequency in 1..=u16::MAX as usize {
            reciprocal_square_sum += 1.0 / (frequency * frequency) as f32;
            let rank = D_WEIGHT * reciprocal_square_sum / ZETA_2;
            values.push((rank.clamp(0.0, 1.0).sqrt() * 65_535.0).min(65_535.0) as u16);
        }
        values
    });
    buckets.partition_point(|bucket| *bucket <= impact_bucket) as u32
}

/// One term's BM25F contribution. Term frequencies from each field are
/// normalized by that field's own average length before boosts are applied.
pub fn bm25f_term_score(
    field_term_frequencies: [u32; 4],
    inverse_document_frequency: f32,
    field_lengths: [u32; 4],
    average_field_lengths: [f64; 4],
    parameters: Bm25fParameters,
) -> f32 {
    if inverse_document_frequency <= 0.0 {
        return 0.0;
    }
    let mut weighted_frequency = 0.0f32;
    for field in 0..4 {
        if field_term_frequencies[field] == 0 {
            continue;
        }
        let average = average_field_lengths[field].max(1.0) as f32;
        let length_ratio = field_lengths[field] as f32 / average;
        let normalization =
            1.0 - parameters.field_b[field] + parameters.field_b[field] * length_ratio;
        weighted_frequency += parameters.field_boosts[field] * field_term_frequencies[field] as f32
            / normalization.max(f32::MIN_POSITIVE);
    }
    if weighted_frequency == 0.0 {
        return 0.0;
    }
    inverse_document_frequency * (weighted_frequency * (parameters.k1 + 1.0))
        / (parameters.k1 + weighted_frequency)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bm25_rewards_tf_rarity_and_shorter_documents() {
        let parameters = Bm25Parameters::default();
        let rare = bm25_inverse_document_frequency(1_000_000, 10);
        let common = bm25_inverse_document_frequency(1_000_000, 100_000);
        assert!(rare > common);
        assert!(
            bm25_term_score(5, rare, 100, 100.0, parameters)
                > bm25_term_score(1, rare, 100, 100.0, parameters)
        );
        assert!(
            bm25_term_score(2, rare, 50, 100.0, parameters)
                > bm25_term_score(2, rare, 500, 100.0, parameters)
        );
    }

    #[test]
    fn unweighted_impact_bucket_inverts_to_a_sound_tf_ceiling() {
        for frequency in [1usize, 2, 3, 8, 32, 256, u16::MAX as usize] {
            let packed = vec![0u16; frequency];
            let bucket = crate::db::fts_impact_bucket(&packed);
            assert!(unweighted_term_frequency_ceiling(bucket) >= frequency as u32);
            if bucket > 0 {
                assert!(unweighted_term_frequency_ceiling(bucket - 1) < frequency as u32);
            }
        }
    }

    #[test]
    fn impact_bm25_bound_terminates_on_a_41m_document_corpus() {
        const DOCUMENTS: u64 = 41_156_375;
        let parameters = Bm25Parameters::default();
        let average_length = 220.0;
        let idfs = [
            bm25_inverse_document_frequency(DOCUMENTS, 9_000_000),
            bm25_inverse_document_frequency(DOCUMENTS, 12_000_000),
        ];
        let winning_score = idfs
            .iter()
            .map(|idf| bm25_term_score(18, *idf, 80, average_length, parameters))
            .sum::<f32>();
        let next_bucket = crate::db::fts_impact_bucket(&[0]);
        let maximum_unseen_frequency = unweighted_term_frequency_ceiling(next_bucket);
        let unseen_ceiling = idfs
            .iter()
            .map(|idf| {
                bm25_term_score(
                    maximum_unseen_frequency,
                    *idf,
                    0,
                    average_length,
                    parameters,
                )
            })
            .sum::<f32>();
        assert!(
            winning_score > unseen_ceiling,
            "top-100 high-impact conjunctions must retire the remaining 41M-doc streams"
        );
    }

    #[test]
    fn bm25f_applies_field_boosts_and_field_lengths() {
        let parameters = Bm25fParameters::default();
        let body = bm25f_term_score([1, 0, 0, 0], 2.0, [100, 10, 10, 10], [100.0; 4], parameters);
        let title = bm25f_term_score([0, 0, 0, 1], 2.0, [100, 10, 10, 10], [100.0; 4], parameters);
        assert!(title > body);
    }
}
