use std::collections::{BTreeSet, HashMap, HashSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{BicDbError, Result};
use crate::vector::{self, VectorMetric};
use crate::CancellationToken;

const HNSW_PERSISTENCE_VERSION: u32 = 1;
const MAX_LEVEL: usize = 32;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HnswIndexConfig {
    pub m: usize,
    pub ef_construction: usize,
    pub ef_search: usize,
    pub distance: VectorMetric,
}

struct SearchLayerRequest<'a> {
    query: &'a [f32],
    query_norm: f32,
    entry: usize,
    ef: usize,
    layer: usize,
    metric: VectorMetric,
    cancellation: &'a CancellationToken,
}

impl Default for HnswIndexConfig {
    fn default() -> Self {
        Self {
            m: 16,
            ef_construction: 100,
            ef_search: 50,
            distance: VectorMetric::Cosine,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HnswIndexVerifyReport {
    pub collection: String,
    pub indexed_vectors: usize,
    pub live_vectors: usize,
    pub tombstoned_vectors: usize,
    pub dimension: Option<usize>,
    pub index_size_bytes: u64,
    pub valid: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct HnswSearchHit {
    pub record_id: String,
    pub score: f32,
}

#[derive(Clone, Debug)]
pub(crate) struct HnswVector {
    pub record_id: String,
    pub vector: Vec<f32>,
}

#[derive(Clone, Debug)]
pub(crate) struct HnswIndex {
    config: HnswIndexConfig,
    dim: Option<usize>,
    nodes: Vec<HnswNode>,
    id_to_node: HashMap<String, usize>,
    entry_point: Option<usize>,
    max_level: usize,
}

#[derive(Clone, Debug)]
struct HnswNode {
    record_id: String,
    vector: Vec<f32>,
    norm: f32,
    level: usize,
    neighbors: Vec<Vec<usize>>,
    deleted: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct PersistedHnswIndex {
    version: u32,
    config: HnswIndexConfig,
    dim: Option<usize>,
    entry_point: Option<String>,
    nodes: Vec<PersistedHnswNode>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedHnswNode {
    record_id: String,
    level: usize,
    deleted: bool,
    neighbors: Vec<Vec<String>>,
}

#[derive(Clone, Copy, Debug)]
struct ScoredNode {
    index: usize,
    score: f32,
}

impl HnswIndexConfig {
    pub(crate) fn validate(self) -> Result<Self> {
        if self.m < 2 {
            return Err(BicDbError::Index("HNSW m must be at least 2".to_string()));
        }
        if self.ef_construction < self.m {
            return Err(BicDbError::Index(format!(
                "HNSW ef_construction must be >= m ({} < {})",
                self.ef_construction, self.m
            )));
        }
        if self.ef_search == 0 {
            return Err(BicDbError::Index(
                "HNSW ef_search must be greater than zero".to_string(),
            ));
        }
        Ok(self)
    }
}

impl HnswIndex {
    /// Resident cost of this graph, split so the roadmap's Phase 6 question is
    /// directly readable: how much is duplicated vector payload (which a paged
    /// vector store would reclaim) versus graph topology (which it would not).
    pub(crate) fn residency(&self, collection: &str) -> crate::residency::HnswResidency {
        use crate::residency::{string_bytes, table_bytes, vec_bytes};

        let mut out = crate::residency::HnswResidency {
            collection: collection.to_string(),
            node_count: self.nodes.len() as u64,
            graph_bytes: vec_bytes::<HnswNode>(self.nodes.capacity()),
            identity_bytes: table_bytes::<String, usize>(self.id_to_node.capacity()),
            vectors_bytes: 0,
        };
        for node in &self.nodes {
            out.vectors_bytes += vec_bytes::<f32>(node.vector.capacity());
            out.graph_bytes += vec_bytes::<Vec<usize>>(node.neighbors.capacity());
            for level in &node.neighbors {
                out.graph_bytes += vec_bytes::<usize>(level.capacity());
            }
            // Counted twice on purpose: the id is owned by the node and again as
            // a key in `id_to_node`.
            out.identity_bytes += string_bytes(&node.record_id) * 2;
        }
        out
    }

    pub(crate) fn build(
        collection: &str,
        config: HnswIndexConfig,
        mut vectors: Vec<HnswVector>,
    ) -> Result<Self> {
        let config = config.validate()?;
        validate_vectors(collection, &vectors)?;
        vectors.sort_by(|left, right| left.record_id.cmp(&right.record_id));

        let mut index = Self {
            config,
            dim: vectors.first().map(|vector| vector.vector.len()),
            nodes: Vec::with_capacity(vectors.len()),
            id_to_node: HashMap::with_capacity(vectors.len()),
            entry_point: None,
            max_level: 0,
        };
        for vector in vectors {
            index.insert_vector(vector)?;
        }
        Ok(index)
    }

    pub(crate) fn from_persisted(
        collection: &str,
        persisted: PersistedHnswIndex,
        vectors: Vec<HnswVector>,
    ) -> Result<Self> {
        if persisted.version != HNSW_PERSISTENCE_VERSION {
            return Err(BicDbError::Index(format!(
                "unsupported HNSW index version {} for collection `{collection}`",
                persisted.version
            )));
        }
        let config = persisted.config.validate()?;
        validate_vectors(collection, &vectors)?;
        let vector_map = vectors
            .into_iter()
            .map(|vector| (vector.record_id, vector.vector))
            .collect::<HashMap<_, _>>();

        let mut nodes = Vec::with_capacity(persisted.nodes.len());
        let mut all_ids = HashMap::with_capacity(persisted.nodes.len());
        let mut id_to_node = HashMap::new();
        for persisted_node in &persisted.nodes {
            let vector = vector_map
                .get(&persisted_node.record_id)
                .cloned()
                .unwrap_or_default();
            let deleted = persisted_node.deleted || vector.is_empty();
            let node_index = nodes.len();
            all_ids.insert(persisted_node.record_id.clone(), node_index);
            if !deleted {
                id_to_node.insert(persisted_node.record_id.clone(), node_index);
            }
            nodes.push(HnswNode {
                record_id: persisted_node.record_id.clone(),
                norm: vector::vector_norm(&vector),
                vector,
                level: persisted_node.level.min(MAX_LEVEL),
                neighbors: vec![Vec::new(); persisted_node.level.min(MAX_LEVEL) + 1],
                deleted,
            });
        }

        for (node_index, persisted_node) in persisted.nodes.iter().enumerate() {
            let layers = persisted_node
                .neighbors
                .iter()
                .take(nodes[node_index].neighbors.len())
                .map(|layer| {
                    layer
                        .iter()
                        .filter_map(|record_id| all_ids.get(record_id).copied())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            for (layer, neighbors) in layers.into_iter().enumerate() {
                nodes[node_index].neighbors[layer] = neighbors;
            }
        }

        let entry_point = persisted
            .entry_point
            .as_ref()
            .and_then(|record_id| id_to_node.get(record_id).copied())
            .or_else(|| live_entry_point(&nodes));
        let max_level = nodes
            .iter()
            .filter(|node| !node.deleted)
            .map(|node| node.level)
            .max()
            .unwrap_or_default();

        let index = Self {
            config,
            dim: persisted
                .dim
                .or_else(|| vector_map.values().next().map(Vec::len)),
            nodes,
            id_to_node,
            entry_point,
            max_level,
        };
        index.validate_query_dimension(collection, index.dim.unwrap_or(1), false)?;
        Ok(index)
    }

    pub(crate) fn config(&self) -> HnswIndexConfig {
        self.config
    }

    pub(crate) fn search_cancellable(
        &self,
        collection: &str,
        query: &[f32],
        top_k: usize,
        ef_search: usize,
        metric: VectorMetric,
        cancellation: &CancellationToken,
    ) -> Result<Vec<HnswSearchHit>> {
        vector::validate_vector(query)?;
        if top_k == 0 {
            return Err(BicDbError::InvalidTopK);
        }
        if metric != self.config.distance {
            return Err(BicDbError::Index(format!(
                "HNSW index for `{collection}` uses {:?}, requested {:?}",
                self.config.distance, metric
            )));
        }
        self.validate_query_dimension(collection, query.len(), true)?;

        let Some(mut current) = self.live_entry_point() else {
            return Ok(Vec::new());
        };
        let query_norm = vector::vector_norm(query);
        for layer in (1..=self.max_level).rev() {
            cancellation.check()?;
            current = self.greedy_search_layer(query, query_norm, current, layer, metric);
        }

        let ef = ef_search.max(self.config.ef_search).max(top_k);
        let mut candidates = self.search_layer_cancellable(SearchLayerRequest {
            query,
            query_norm,
            entry: current,
            ef,
            layer: 0,
            metric,
            cancellation,
        })?;
        cancellation.check()?;
        candidates.sort_by(|left, right| {
            right.score.total_cmp(&left.score).then_with(|| {
                self.nodes[left.index]
                    .record_id
                    .cmp(&self.nodes[right.index].record_id)
            })
        });
        cancellation.check()?;
        candidates.truncate(top_k);
        Ok(candidates
            .into_iter()
            .filter(|candidate| !self.nodes[candidate.index].deleted)
            .map(|candidate| HnswSearchHit {
                record_id: self.nodes[candidate.index].record_id.clone(),
                score: candidate.score,
            })
            .collect())
    }

    pub(crate) fn tombstone(&mut self, record_id: &str) -> bool {
        let Some(index) = self.id_to_node.remove(record_id) else {
            return false;
        };
        self.nodes[index].deleted = true;
        if self.entry_point == Some(index) {
            self.entry_point = self.live_entry_point();
            self.max_level = self
                .nodes
                .iter()
                .filter(|node| !node.deleted)
                .map(|node| node.level)
                .max()
                .unwrap_or_default();
        }
        true
    }

    pub(crate) fn verify(
        &self,
        collection: &str,
        vectors: &[HnswVector],
        index_size_bytes: u64,
    ) -> HnswIndexVerifyReport {
        let expected = vectors
            .iter()
            .map(|vector| vector.record_id.as_str())
            .collect::<BTreeSet<_>>();
        let actual = self
            .nodes
            .iter()
            .filter(|node| !node.deleted)
            .map(|node| node.record_id.as_str())
            .collect::<BTreeSet<_>>();
        let graph_valid = self.nodes.iter().enumerate().all(|(index, node)| {
            node.neighbors.len() == node.level + 1
                && node.neighbors.iter().all(|layer| {
                    layer
                        .iter()
                        .all(|neighbor| *neighbor < self.nodes.len() && *neighbor != index)
                })
        });
        HnswIndexVerifyReport {
            collection: collection.to_string(),
            indexed_vectors: actual.len(),
            live_vectors: expected.len(),
            tombstoned_vectors: self.nodes.iter().filter(|node| node.deleted).count(),
            dimension: self.dim,
            index_size_bytes,
            valid: expected == actual && graph_valid,
        }
    }

    pub(crate) fn to_persisted(&self) -> PersistedHnswIndex {
        PersistedHnswIndex {
            version: HNSW_PERSISTENCE_VERSION,
            config: self.config,
            dim: self.dim,
            entry_point: self
                .entry_point
                .map(|index| self.nodes[index].record_id.clone()),
            nodes: self
                .nodes
                .iter()
                .map(|node| PersistedHnswNode {
                    record_id: node.record_id.clone(),
                    level: node.level,
                    deleted: node.deleted,
                    neighbors: node
                        .neighbors
                        .iter()
                        .map(|layer| {
                            layer
                                .iter()
                                .map(|index| self.nodes[*index].record_id.clone())
                                .collect()
                        })
                        .collect(),
                })
                .collect(),
        }
    }

    pub(crate) fn memory_estimate_bytes(&self) -> u64 {
        self.nodes
            .iter()
            .map(|node| {
                node.record_id.len() as u64
                    + node.vector.len() as u64 * std::mem::size_of::<f32>() as u64
                    + node
                        .neighbors
                        .iter()
                        .map(|layer| layer.len() as u64 * 8)
                        .sum::<u64>()
                    + 64
            })
            .sum()
    }

    fn insert_vector(&mut self, vector: HnswVector) -> Result<()> {
        let level = deterministic_level(&vector.record_id, self.config.m);
        let node_index = self.nodes.len();
        let norm = vector::vector_norm(&vector.vector);

        if self.entry_point.is_none() {
            self.nodes.push(HnswNode {
                record_id: vector.record_id.clone(),
                vector: vector.vector,
                norm,
                level,
                neighbors: vec![Vec::new(); level + 1],
                deleted: false,
            });
            self.id_to_node.insert(vector.record_id, node_index);
            self.entry_point = Some(node_index);
            self.max_level = level;
            return Ok(());
        }

        let mut current = self.live_entry_point().unwrap();
        for layer in ((level + 1)..=self.max_level).rev() {
            current = self.greedy_search_layer(
                &vector.vector,
                norm,
                current,
                layer,
                self.config.distance,
            );
        }

        let upper_layer = level.min(self.max_level);
        let mut selected_by_layer = vec![Vec::<usize>::new(); level + 1];
        for layer in (0..=upper_layer).rev() {
            let candidates = self.search_layer(
                &vector.vector,
                norm,
                current,
                self.config.ef_construction,
                layer,
                self.config.distance,
            );
            let selected = self.select_neighbors(&vector.vector, norm, candidates, self.config.m);
            if let Some(first) = selected.first().copied() {
                current = first;
            }
            selected_by_layer[layer] = selected;
        }

        self.nodes.push(HnswNode {
            record_id: vector.record_id.clone(),
            vector: vector.vector,
            norm,
            level,
            neighbors: vec![Vec::new(); level + 1],
            deleted: false,
        });
        self.id_to_node.insert(vector.record_id, node_index);

        for (layer, selected) in selected_by_layer.into_iter().enumerate() {
            for neighbor in selected {
                self.connect_bidirectional(node_index, neighbor, layer);
            }
        }

        if level > self.max_level {
            self.entry_point = Some(node_index);
            self.max_level = level;
        }
        Ok(())
    }

    fn live_entry_point(&self) -> Option<usize> {
        self.entry_point
            .filter(|index| !self.nodes[*index].deleted)
            .or_else(|| live_entry_point(&self.nodes))
    }

    fn validate_query_dimension(
        &self,
        collection: &str,
        actual: usize,
        require_existing: bool,
    ) -> Result<()> {
        match self.dim {
            Some(expected) if expected != actual => Err(BicDbError::DimensionMismatch {
                collection: collection.to_string(),
                expected,
                actual,
            }),
            None if require_existing => Ok(()),
            _ => Ok(()),
        }
    }

    fn greedy_search_layer(
        &self,
        query: &[f32],
        query_norm: f32,
        mut current: usize,
        layer: usize,
        metric: VectorMetric,
    ) -> usize {
        if self.nodes[current].deleted || self.nodes[current].neighbors.len() <= layer {
            return current;
        }
        let mut current_score = self.score_node(query, query_norm, current, metric);
        loop {
            let mut improved = false;
            for neighbor in self.nodes[current].neighbors[layer].iter().copied() {
                if self.nodes[neighbor].deleted || self.nodes[neighbor].neighbors.len() <= layer {
                    continue;
                }
                let score = self.score_node(query, query_norm, neighbor, metric);
                if score > current_score {
                    current = neighbor;
                    current_score = score;
                    improved = true;
                }
            }
            if !improved {
                return current;
            }
        }
    }

    fn search_layer(
        &self,
        query: &[f32],
        query_norm: f32,
        entry: usize,
        ef: usize,
        layer: usize,
        metric: VectorMetric,
    ) -> Vec<ScoredNode> {
        self.search_layer_cancellable(SearchLayerRequest {
            query,
            query_norm,
            entry,
            ef,
            layer,
            metric,
            cancellation: &CancellationToken::uncancelable(),
        })
        .unwrap_or_default()
    }

    fn search_layer_cancellable(&self, request: SearchLayerRequest<'_>) -> Result<Vec<ScoredNode>> {
        let SearchLayerRequest {
            query,
            query_norm,
            entry,
            ef,
            layer,
            metric,
            cancellation,
        } = request;
        let mut visited = HashSet::from([entry]);
        let entry_score = self.score_node(query, query_norm, entry, metric);
        let mut candidates = vec![ScoredNode {
            index: entry,
            score: entry_score,
        }];
        let mut results = candidates.clone();

        while !candidates.is_empty() {
            cancellation.check()?;
            candidates.sort_by(|left, right| right.score.total_cmp(&left.score));
            let candidate = candidates.remove(0);
            let worst = results
                .iter()
                .map(|node| node.score)
                .reduce(f32::min)
                .unwrap_or(f32::NEG_INFINITY);
            if results.len() >= ef && candidate.score < worst {
                break;
            }
            if self.nodes[candidate.index].neighbors.len() <= layer {
                continue;
            }
            for neighbor in self.nodes[candidate.index].neighbors[layer].iter().copied() {
                if !visited.insert(neighbor) || self.nodes[neighbor].deleted {
                    continue;
                }
                let score = self.score_node(query, query_norm, neighbor, metric);
                let should_keep = results.len() < ef
                    || score
                        > results
                            .iter()
                            .map(|node| node.score)
                            .reduce(f32::min)
                            .unwrap_or(f32::NEG_INFINITY);
                if should_keep {
                    candidates.push(ScoredNode {
                        index: neighbor,
                        score,
                    });
                    results.push(ScoredNode {
                        index: neighbor,
                        score,
                    });
                    results.sort_by(|left, right| right.score.total_cmp(&left.score));
                    results.truncate(ef);
                }
            }
        }

        Ok(results
            .into_iter()
            .filter(|node| !self.nodes[node.index].deleted)
            .collect::<Vec<_>>())
    }

    fn select_neighbors(
        &self,
        query: &[f32],
        query_norm: f32,
        mut candidates: Vec<ScoredNode>,
        limit: usize,
    ) -> Vec<usize> {
        candidates.sort_by(|left, right| {
            right.score.total_cmp(&left.score).then_with(|| {
                self.nodes[left.index]
                    .record_id
                    .cmp(&self.nodes[right.index].record_id)
            })
        });
        candidates.truncate(limit);
        candidates
            .into_iter()
            .filter(|candidate| {
                vector::validate_vector(&self.nodes[candidate.index].vector).is_ok()
                    && self
                        .score_node(query, query_norm, candidate.index, self.config.distance)
                        .is_finite()
            })
            .map(|candidate| candidate.index)
            .collect()
    }

    fn connect_bidirectional(&mut self, left: usize, right: usize, layer: usize) {
        add_unique_neighbor(&mut self.nodes[left].neighbors[layer], right);
        add_unique_neighbor(&mut self.nodes[right].neighbors[layer], left);
        self.prune_neighbors(left, layer);
        self.prune_neighbors(right, layer);
    }

    fn prune_neighbors(&mut self, node: usize, layer: usize) {
        if self.nodes[node].neighbors[layer].len() <= self.config.m {
            return;
        }
        let query = self.nodes[node].vector.clone();
        let query_norm = self.nodes[node].norm;
        let mut scored = self.nodes[node].neighbors[layer]
            .iter()
            .copied()
            .map(|neighbor| ScoredNode {
                index: neighbor,
                score: self.score_node(&query, query_norm, neighbor, self.config.distance),
            })
            .collect::<Vec<_>>();
        scored.sort_by(|left, right| right.score.total_cmp(&left.score));
        scored.truncate(self.config.m);
        self.nodes[node].neighbors[layer] = scored.into_iter().map(|node| node.index).collect();
    }

    fn score_node(&self, query: &[f32], query_norm: f32, node: usize, metric: VectorMetric) -> f32 {
        vector::score_with_norm(
            metric,
            query,
            query_norm,
            &self.nodes[node].vector,
            self.nodes[node].norm,
        )
    }
}

fn validate_vectors(collection: &str, vectors: &[HnswVector]) -> Result<()> {
    let mut dim = None;
    let mut ids = HashSet::with_capacity(vectors.len());
    for vector in vectors {
        if !ids.insert(vector.record_id.as_str()) {
            return Err(BicDbError::Index(format!(
                "duplicate vector record `{}` in HNSW index for `{collection}`",
                vector.record_id
            )));
        }
        vector::validate_vector(&vector.vector)?;
        match dim {
            Some(expected) if expected != vector.vector.len() => {
                return Err(BicDbError::DimensionMismatch {
                    collection: collection.to_string(),
                    expected,
                    actual: vector.vector.len(),
                });
            }
            Some(_) => {}
            None => dim = Some(vector.vector.len()),
        }
    }
    Ok(())
}

fn deterministic_level(record_id: &str, m: usize) -> usize {
    let digest = Sha256::digest(record_id.as_bytes());
    let mut level = 0;
    for chunk in digest.chunks(8) {
        let mut bytes = [0_u8; 8];
        bytes[..chunk.len()].copy_from_slice(chunk);
        let value = u64::from_le_bytes(bytes);
        if value % (m as u64) == 0 {
            level += 1;
            if level >= MAX_LEVEL {
                break;
            }
        } else {
            break;
        }
    }
    level
}

fn live_entry_point(nodes: &[HnswNode]) -> Option<usize> {
    nodes
        .iter()
        .enumerate()
        .filter(|(_, node)| !node.deleted)
        .max_by_key(|(_, node)| node.level)
        .map(|(index, _)| index)
}

fn add_unique_neighbor(neighbors: &mut Vec<usize>, candidate: usize) {
    if !neighbors.contains(&candidate) {
        neighbors.push(candidate);
    }
}
