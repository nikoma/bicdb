//! G7: approximate, **mergeable** measures for aggregate projections.
//!
//! `COUNT`/`SUM`/`AVG` are exact and retractable. Distinct counts and
//! percentiles are *holistic* — they cannot be maintained exactly in a fixed
//! cell without keeping the whole multiset — so they are approximated by
//! sketches and **labelled approximate at the query surface** (`approx_*`
//! column names). A `P95` that silently returns an estimate is a reporting
//! bug even when the sketch is behaving.
//!
//! Both sketches here are associatively mergeable, which is the invariant the
//! roadmap depends on: per-shard partial states must combine at a coordinator
//! with no re-scan (docs §8).
//!
//! **Neither is retractable.** Removing a value from an HLL register or a
//! reservoir is not defined, so a projection carrying sketch measures cannot
//! retract them on an update — it must rebuild the affected cell. That
//! limitation is deliberate and enforced, not papered over.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// HyperLogLog with 2^14 registers: ~1.6% standard error, 16 KiB dense — but
/// stored sparsely, so a cell holding a handful of distinct values costs a
/// handful of bytes rather than 16 KiB.
const HLL_PRECISION: u32 = 14;
const HLL_REGISTERS: usize = 1 << HLL_PRECISION;
/// Above this many occupied registers the sparse form stops paying.
const HLL_SPARSE_LIMIT: usize = 512;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HyperLogLog {
    /// (register index, value) pairs while sparse; dense once it outgrows the
    /// limit. Sorted, so equality and merging are deterministic.
    sparse: Vec<(u32, u8)>,
    dense: Option<Vec<u8>>,
}

fn hash64(value: &str) -> u64 {
    let digest = Sha256::digest(value.as_bytes());
    u64::from_le_bytes(digest[..8].try_into().expect("8 bytes"))
}

impl HyperLogLog {
    pub fn add(&mut self, value: &str) {
        self.observe(hash64(value));
    }

    fn observe(&mut self, hash: u64) {
        let index = (hash >> (64 - HLL_PRECISION)) as u32;
        // Leading-zero count of the remaining bits, +1, capped to fit a u8.
        let remaining = (hash << HLL_PRECISION) | (1 << (HLL_PRECISION - 1));
        let rank = (remaining.leading_zeros() + 1).min(u8::MAX as u32) as u8;
        self.set(index, rank);
    }

    fn set(&mut self, index: u32, rank: u8) {
        if let Some(dense) = &mut self.dense {
            let slot = &mut dense[index as usize];
            if rank > *slot {
                *slot = rank;
            }
            return;
        }
        match self.sparse.binary_search_by_key(&index, |(at, _)| *at) {
            Ok(position) => {
                if rank > self.sparse[position].1 {
                    self.sparse[position].1 = rank;
                }
            }
            Err(position) => self.sparse.insert(position, (index, rank)),
        }
        if self.sparse.len() > HLL_SPARSE_LIMIT {
            let mut dense = vec![0u8; HLL_REGISTERS];
            for (at, value) in self.sparse.drain(..) {
                dense[at as usize] = value;
            }
            self.dense = Some(dense);
        }
    }

    /// Associative merge — the property distribution depends on.
    pub fn merge(&mut self, other: &Self) {
        if let Some(dense) = &other.dense {
            for (index, rank) in dense.iter().enumerate() {
                if *rank > 0 {
                    self.set(index as u32, *rank);
                }
            }
        }
        for (index, rank) in &other.sparse {
            self.set(*index, *rank);
        }
    }

    pub fn estimate(&self) -> f64 {
        let mut registers = vec![0u8; HLL_REGISTERS];
        if let Some(dense) = &self.dense {
            registers.copy_from_slice(dense);
        }
        for (index, rank) in &self.sparse {
            registers[*index as usize] = *rank;
        }
        let zeros = registers.iter().filter(|rank| **rank == 0).count();
        // Linear counting is markedly more accurate while the sketch is
        // sparse, which is the common case for a well-chosen grain.
        if zeros > 0 {
            let estimate = HLL_REGISTERS as f64 * (HLL_REGISTERS as f64 / zeros as f64).ln();
            if estimate <= 2.5 * HLL_REGISTERS as f64 {
                return estimate;
            }
        }
        let harmonic: f64 = registers
            .iter()
            .map(|rank| 2f64.powi(-(*rank as i32)))
            .sum();
        const ALPHA: f64 = 0.7213 / (1.0 + 1.079 / HLL_REGISTERS as f64);
        ALPHA * (HLL_REGISTERS as f64).powi(2) / harmonic
    }

    pub fn is_empty(&self) -> bool {
        self.sparse.is_empty() && self.dense.is_none()
    }
}

/// Bounded quantile sketch using **bottom-k sampling**.
///
/// Each observation gets a hash-derived priority and the sketch keeps the k
/// smallest — a uniform random sample of the stream that is deterministic
/// (same stream, same sample) and associatively mergeable (union, then keep
/// the bottom k again).
///
/// The first attempt here kept both tails and thinned the middle, which is
/// deterministic but **biased**: on ascending input the retained tails drift
/// upward and the median of 1..=50,000 came out at 49,871. Percentiles that
/// are confidently wrong are worse than percentiles that are absent, so the
/// biased version was discarded rather than tuned.
///
/// A production KLL/t-digest would give tighter tail guarantees; this
/// interface does not change when one lands.
const RESERVOIR_CAPACITY: usize = 1024;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Quantiles {
    /// `(priority, value)`, ordered by priority, truncated to capacity.
    samples: Vec<(u64, f64)>,
    /// Every value offered, including those not retained.
    seen: u64,
}

/// Cheap, well-mixed hash: distinct occurrences of the same value must get
/// distinct priorities, or repeated values would be sampled as one.
fn mix(value: f64, occurrence: u64) -> u64 {
    let mut hash = value.to_bits() ^ occurrence.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    hash ^ (hash >> 33)
}

impl Quantiles {
    pub fn add(&mut self, value: f64) {
        let priority = mix(value, self.seen);
        self.seen += 1;
        self.insert(priority, value);
    }

    /// Add a value whose priority is derived from a **stable per-record
    /// identity** rather than from arrival order.
    ///
    /// This is what makes the sketch a bottom-k sample of the *set of
    /// (record, value) pairs* instead of a sample of an insertion sequence,
    /// and it buys two properties the projection layer depends on:
    ///
    /// * **Rebuild equals incremental.** Recomputing a cell by scanning its
    ///   rows in slot order yields the identical sample, so a sketch that was
    ///   maintained incrementally and one that was rebuilt after a retract
    ///   cannot disagree — which is the only way the reconcile oracle can
    ///   check them at all.
    /// * **Merge equals whole.** Bottom-k of a union is the union of the
    ///   bottom-ks, so per-shard sketches combine at a coordinator without
    ///   re-reading the base data.
    ///
    /// With `add`, both properties are false: the same value inserted in a
    /// different order gets a different priority.
    pub fn add_keyed(&mut self, value: f64, identity: u64) {
        self.seen += 1;
        self.insert(mix(value, identity), value);
    }

    fn insert(&mut self, priority: u64, value: f64) {
        if self.samples.len() >= RESERVOIR_CAPACITY {
            // Only values beating the current worst priority can enter.
            if priority >= self.samples[self.samples.len() - 1].0 {
                return;
            }
            self.samples.pop();
        }
        let position = self
            .samples
            .binary_search_by(|(probe, _)| probe.cmp(&priority))
            .unwrap_or_else(|position| position);
        self.samples.insert(position, (priority, value));
    }

    /// Associative: union the samples, keep the bottom k. Merge order does
    /// not affect the result.
    pub fn merge(&mut self, other: &Self) {
        for (priority, value) in &other.samples {
            self.insert(*priority, *value);
        }
        self.seen += other.seen;
    }

    /// `None` when nothing has been recorded — distinct from a percentile of
    /// zero.
    pub fn percentile(&self, fraction: f64) -> Option<f64> {
        if self.samples.is_empty() {
            return None;
        }
        let mut ordered: Vec<f64> = self.samples.iter().map(|(_, value)| *value).collect();
        ordered.sort_by(f64::total_cmp);
        // Nearest-rank: the smallest value at or above the requested fraction
        // of the ordered set, which is what P50/P95 conventionally mean.
        let fraction = fraction.clamp(0.0, 1.0);
        let rank = (fraction * ordered.len() as f64).ceil() as usize;
        ordered
            .get(rank.saturating_sub(1).min(ordered.len() - 1))
            .copied()
    }

    pub fn count(&self) -> u64 {
        self.seen
    }

    /// Whether the sketch retained every value it was offered — i.e. whether
    /// its percentiles are exact rather than sampled.
    pub fn is_exact(&self) -> bool {
        self.seen as usize == self.samples.len()
    }
}

/// Deterministic hash of a dimension value, for distinct-counting.
///
/// Deliberately NOT `std::hash::Hash` + `DefaultHasher`: that hasher's output
/// is explicitly not stable across releases, and a distinct-count whose
/// registers shift under a toolchain upgrade would silently change historical
/// answers. This encoding is fixed: a type tag, then the payload.
pub fn stable_hash_bytes(tag: u8, payload: &[u8]) -> u64 {
    // FNV-1a over the tagged encoding, then an avalanche finalizer so nearby
    // inputs land in unrelated registers.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    hash ^= tag as u64;
    hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    for byte in payload {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    hash ^ (hash >> 33)
}

impl HyperLogLog {
    /// Add a value already reduced to its stable hash. The projection stores
    /// that hash in the input row, so a rebuild never has to re-read the
    /// original string.
    pub fn add_hash(&mut self, hash: u64) {
        self.observe(hash);
    }
}
