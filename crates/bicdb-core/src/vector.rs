use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap};
use std::hint::black_box;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use wide::f32x8;

use crate::error::{BicDbError, Result};
use crate::record::{Record, StoredRecord};
use crate::CancellationToken;

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum VectorMetric {
    #[default]
    Cosine,
    Dot,
    L2,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct JsonFilter {
    equals: BTreeMap<String, Value>,
}

impl JsonFilter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn eq(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.equals.insert(key.into(), value.into());
        self
    }

    pub fn matches(&self, metadata: &Value) -> bool {
        self.equals.iter().all(|(key, expected)| {
            metadata
                .as_object()
                .and_then(|object| object.get(key))
                .is_some_and(|actual| actual == expected)
        })
    }

    pub fn equals_keys(&self) -> impl Iterator<Item = &str> {
        self.equals.keys().map(String::as_str)
    }

    pub fn equals_value(&self, key: &str) -> Option<&Value> {
        self.equals.get(key)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct VectorSearchResult {
    pub record: Record,
    pub score: f32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct VectorSearchProfile {
    pub candidate_records: usize,
    pub vectors_read: usize,
    pub dim: usize,
    pub top_k: usize,
    pub read_vectors: Duration,
    pub similarity: Duration,
    pub top_k_heap: Duration,
    pub final_sort: Duration,
    pub total: Duration,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProfiledVectorSearch {
    pub results: Vec<VectorSearchResult>,
    pub profile: VectorSearchProfile,
}

#[derive(Clone, Debug)]
struct HeapItem {
    score: f32,
    record: Record,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.score.total_cmp(&other.score) == Ordering::Equal && self.record.id == other.record.id
    }
}

impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .score
            .total_cmp(&self.score)
            .then_with(|| other.record.id.cmp(&self.record.id))
    }
}

pub(crate) fn validate_vector(vector: &[f32]) -> Result<()> {
    if vector.is_empty() {
        return Err(BicDbError::EmptyVector);
    }

    if vector.iter().any(|value| !value.is_finite()) {
        return Err(BicDbError::NonFiniteVectorValue);
    }

    Ok(())
}

pub fn dot_product(left: &[f32], right: &[f32]) -> Result<f32> {
    validate_vector_pair(left, right)?;
    Ok(dot_unchecked(left, right))
}

pub fn l2_distance(left: &[f32], right: &[f32]) -> Result<f32> {
    validate_vector_pair(left, right)?;
    Ok(l2_distance_unchecked(left, right))
}

pub fn cosine_similarity(left: &[f32], right: &[f32]) -> Result<f32> {
    validate_vector_pair(left, right)?;
    Ok(cosine_similarity_unchecked(left, right))
}

pub(crate) fn search<'a, I>(
    collection: &str,
    records: I,
    query: &[f32],
    top_k: usize,
    filter: Option<&JsonFilter>,
    metric: VectorMetric,
) -> Result<Vec<VectorSearchResult>>
where
    I: IntoIterator<Item = &'a StoredRecord>,
{
    search_cancellable(
        collection,
        records,
        query,
        top_k,
        filter,
        metric,
        &CancellationToken::uncancelable(),
    )
}

pub(crate) fn search_cancellable<'a, I>(
    collection: &str,
    records: I,
    query: &[f32],
    top_k: usize,
    filter: Option<&JsonFilter>,
    metric: VectorMetric,
    cancellation: &CancellationToken,
) -> Result<Vec<VectorSearchResult>>
where
    I: IntoIterator<Item = &'a StoredRecord>,
{
    validate_vector(query)?;
    if top_k == 0 {
        return Err(BicDbError::InvalidTopK);
    }

    let mut heap = BinaryHeap::with_capacity(top_k.saturating_add(1));

    for (idx, stored) in records.into_iter().enumerate() {
        if idx % 1024 == 0 {
            cancellation.check()?;
        }
        // Fast pre-filter on the raw scalar fields before paying to materialize
        // the heavy metadata Value, which is only needed for the JSON filter and
        // the returned record.
        let Some(vector) = stored.vector.as_deref() else {
            continue;
        };
        if vector.len() != query.len() {
            return Err(BicDbError::DimensionMismatch {
                collection: collection.to_string(),
                expected: vector.len(),
                actual: query.len(),
            });
        }
        let record = stored.to_record()?;
        if filter.is_some_and(|filter| !filter.matches(&record.metadata)) {
            continue;
        }

        let score = score(metric, query, vector);
        if heap.len() < top_k {
            heap.push(HeapItem {
                score,
                record: record.clone(),
            });
            continue;
        }

        if heap.peek().is_some_and(|worst| score > worst.score) {
            heap.pop();
            heap.push(HeapItem {
                score,
                record: record.clone(),
            });
        }
    }
    cancellation.check()?;

    let mut results = heap
        .into_iter()
        .map(|item| VectorSearchResult {
            record: item.record,
            score: item.score,
        })
        .collect::<Vec<_>>();
    cancellation.check()?;
    results.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| left.record.id.cmp(&right.record.id))
    });
    cancellation.check()?;
    Ok(results)
}

pub(crate) fn profile_search<'a, I>(
    collection: &str,
    records: I,
    query: &[f32],
    top_k: usize,
    filter: Option<&JsonFilter>,
    metric: VectorMetric,
) -> Result<ProfiledVectorSearch>
where
    I: IntoIterator<Item = &'a StoredRecord> + Clone,
{
    validate_vector(query)?;
    if top_k == 0 {
        return Err(BicDbError::InvalidTopK);
    }

    let total_started = Instant::now();
    let read_started = Instant::now();
    let mut candidate_records = 0;
    let mut vectors_read = 0;
    for stored in records.clone() {
        candidate_records += 1;
        let record = stored.to_record()?;
        if filter.is_some_and(|filter| !filter.matches(&record.metadata)) {
            continue;
        }
        let Some(vector) = record.vector.as_deref() else {
            continue;
        };
        if vector.len() != query.len() {
            return Err(BicDbError::DimensionMismatch {
                collection: collection.to_string(),
                expected: vector.len(),
                actual: query.len(),
            });
        }
        black_box(vector.as_ptr());
        vectors_read += 1;
    }
    let read_vectors = read_started.elapsed();

    let similarity_started = Instant::now();
    let mut score_acc = 0.0_f32;
    for stored in records.clone() {
        let record = stored.to_record()?;
        if filter.is_some_and(|filter| !filter.matches(&record.metadata)) {
            continue;
        }
        let Some(vector) = record.vector.as_deref() else {
            continue;
        };
        score_acc += score(metric, query, vector);
    }
    black_box(score_acc);
    let similarity = similarity_started.elapsed();

    let (mut results, top_k_heap, final_sort) =
        search_with_heap_timing(collection, records, query, top_k, filter, metric)?;

    Ok(ProfiledVectorSearch {
        profile: VectorSearchProfile {
            candidate_records,
            vectors_read,
            dim: query.len(),
            top_k,
            read_vectors,
            similarity,
            top_k_heap,
            final_sort,
            total: total_started.elapsed(),
        },
        results: std::mem::take(&mut results),
    })
}

fn search_with_heap_timing<'a, I>(
    collection: &str,
    records: I,
    query: &[f32],
    top_k: usize,
    filter: Option<&JsonFilter>,
    metric: VectorMetric,
) -> Result<(Vec<VectorSearchResult>, Duration, Duration)>
where
    I: IntoIterator<Item = &'a StoredRecord>,
{
    let mut heap = BinaryHeap::with_capacity(top_k.saturating_add(1));
    let mut top_k_heap = Duration::ZERO;

    for stored in records {
        let Some(vector) = stored.vector.as_deref() else {
            continue;
        };
        if vector.len() != query.len() {
            return Err(BicDbError::DimensionMismatch {
                collection: collection.to_string(),
                expected: vector.len(),
                actual: query.len(),
            });
        }
        let record = stored.to_record()?;
        if filter.is_some_and(|filter| !filter.matches(&record.metadata)) {
            continue;
        }

        let score = score(metric, query, vector);
        let heap_started = Instant::now();
        if heap.len() < top_k {
            heap.push(HeapItem {
                score,
                record: record.clone(),
            });
            top_k_heap += heap_started.elapsed();
            continue;
        }

        if heap.peek().is_some_and(|worst| score > worst.score) {
            heap.pop();
            heap.push(HeapItem {
                score,
                record: record.clone(),
            });
        }
        top_k_heap += heap_started.elapsed();
    }

    let mut results = heap
        .into_iter()
        .map(|item| VectorSearchResult {
            record: item.record,
            score: item.score,
        })
        .collect::<Vec<_>>();
    let sort_started = Instant::now();
    results.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| left.record.id.cmp(&right.record.id))
    });
    let final_sort = sort_started.elapsed();
    Ok((results, top_k_heap, final_sort))
}

pub(crate) fn score(metric: VectorMetric, query: &[f32], vector: &[f32]) -> f32 {
    match metric {
        VectorMetric::Cosine => cosine_similarity_unchecked(query, vector),
        VectorMetric::Dot => dot_unchecked(query, vector),
        VectorMetric::L2 => -l2_distance_unchecked(query, vector),
    }
}

pub(crate) fn score_with_norm(
    metric: VectorMetric,
    query: &[f32],
    query_norm: f32,
    vector: &[f32],
    vector_norm: f32,
) -> f32 {
    match metric {
        VectorMetric::Cosine => {
            cosine_similarity_with_norms(query, query_norm, vector, vector_norm)
        }
        VectorMetric::Dot => dot_unchecked(query, vector),
        VectorMetric::L2 => -l2_distance_unchecked(query, vector),
    }
}

fn validate_vector_pair(left: &[f32], right: &[f32]) -> Result<()> {
    validate_vector(left)?;
    validate_vector(right)?;
    if left.len() != right.len() {
        return Err(BicDbError::DimensionMismatch {
            collection: "<vector>".to_string(),
            expected: left.len(),
            actual: right.len(),
        });
    }
    Ok(())
}

fn cosine_similarity_unchecked(left: &[f32], right: &[f32]) -> f32 {
    let (dot_product, left_squared_norm, right_squared_norm) = dot_and_squared_norms(left, right);
    let left_norm = left_squared_norm.sqrt();
    let right_norm = right_squared_norm.sqrt();
    cosine_from_dot_and_norms(dot_product, left_norm, right_norm)
}

fn cosine_similarity_with_norms(
    query: &[f32],
    query_norm: f32,
    vector: &[f32],
    vector_norm: f32,
) -> f32 {
    cosine_from_dot_and_norms(dot_unchecked(query, vector), query_norm, vector_norm)
}

fn cosine_from_dot_and_norms(dot_product: f32, left_norm: f32, right_norm: f32) -> f32 {
    if left_norm <= f32::EPSILON || right_norm <= f32::EPSILON {
        return 0.0;
    }
    dot_product / (left_norm * right_norm)
}

pub(crate) fn vector_norm(vector: &[f32]) -> f32 {
    squared_norm_unchecked(vector).sqrt()
}

pub(crate) fn dot_unchecked(left: &[f32], right: &[f32]) -> f32 {
    let mut acc = f32x8::from([0.0; 8]);
    let mut left_chunks = left.chunks_exact(8);
    let mut right_chunks = right.chunks_exact(8);

    for (left, right) in left_chunks.by_ref().zip(right_chunks.by_ref()) {
        let left = f32x8::from([
            left[0], left[1], left[2], left[3], left[4], left[5], left[6], left[7],
        ]);
        let right = f32x8::from([
            right[0], right[1], right[2], right[3], right[4], right[5], right[6], right[7],
        ]);
        acc += left * right;
    }

    acc.reduce_add()
        + left_chunks
            .remainder()
            .iter()
            .zip(right_chunks.remainder().iter())
            .map(|(left, right)| left * right)
            .sum::<f32>()
}

fn squared_norm_unchecked(vector: &[f32]) -> f32 {
    let mut acc = f32x8::from([0.0; 8]);
    let mut chunks = vector.chunks_exact(8);

    for chunk in chunks.by_ref() {
        let values = f32x8::from([
            chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
        ]);
        acc += values * values;
    }

    acc.reduce_add()
        + chunks
            .remainder()
            .iter()
            .map(|value| value * value)
            .sum::<f32>()
}

fn l2_distance_unchecked(left: &[f32], right: &[f32]) -> f32 {
    let mut acc = f32x8::from([0.0; 8]);
    let mut left_chunks = left.chunks_exact(8);
    let mut right_chunks = right.chunks_exact(8);

    for (left, right) in left_chunks.by_ref().zip(right_chunks.by_ref()) {
        let left = f32x8::from([
            left[0], left[1], left[2], left[3], left[4], left[5], left[6], left[7],
        ]);
        let right = f32x8::from([
            right[0], right[1], right[2], right[3], right[4], right[5], right[6], right[7],
        ]);
        let diff = left - right;
        acc += diff * diff;
    }

    (acc.reduce_add()
        + left_chunks
            .remainder()
            .iter()
            .zip(right_chunks.remainder().iter())
            .map(|(left, right)| {
                let diff = left - right;
                diff * diff
            })
            .sum::<f32>())
    .sqrt()
}

fn dot_and_squared_norms(left: &[f32], right: &[f32]) -> (f32, f32, f32) {
    let mut dot_acc = f32x8::from([0.0; 8]);
    let mut left_acc = f32x8::from([0.0; 8]);
    let mut right_acc = f32x8::from([0.0; 8]);
    let mut left_chunks = left.chunks_exact(8);
    let mut right_chunks = right.chunks_exact(8);

    for (left, right) in left_chunks.by_ref().zip(right_chunks.by_ref()) {
        let left = f32x8::from([
            left[0], left[1], left[2], left[3], left[4], left[5], left[6], left[7],
        ]);
        let right = f32x8::from([
            right[0], right[1], right[2], right[3], right[4], right[5], right[6], right[7],
        ]);
        dot_acc += left * right;
        left_acc += left * left;
        right_acc += right * right;
    }

    let mut dot_tail = 0.0;
    let mut left_tail = 0.0;
    let mut right_tail = 0.0;
    for (left, right) in left_chunks.remainder().iter().zip(right_chunks.remainder()) {
        dot_tail += left * right;
        left_tail += left * left;
        right_tail += right * right;
    }

    (
        dot_acc.reduce_add() + dot_tail,
        left_acc.reduce_add() + left_tail,
        right_acc.reduce_add() + right_tail,
    )
}
