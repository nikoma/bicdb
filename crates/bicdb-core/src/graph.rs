use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::error::{BicDbError, Result};
use crate::event::StoredEvent;
use crate::record::Record;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct GraphNode {
    pub id: String,
    pub label: String,
    pub properties: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct GraphEdge {
    pub id: String,
    pub from: String,
    pub to: String,
    pub label: String,
    pub properties: Value,
    pub timestamp: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphProjection {
    pub name: String,
    #[serde(default)]
    pub node_sources: Vec<GraphNodeSource>,
    #[serde(default)]
    pub edge_sources: Vec<GraphEdgeSource>,
    #[serde(default)]
    pub event_node_sources: Vec<GraphEventNodeSource>,
    #[serde(default)]
    pub event_edge_sources: Vec<GraphEventEdgeSource>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphNodeSource {
    pub collection: String,
    pub label: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphEdgeSource {
    pub collection: String,
    pub from_field: GraphEndpoint,
    pub to_field: GraphEndpoint,
    pub label: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum GraphEndpoint {
    Field(String),
    Record { label: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphEventNodeSource {
    pub stream: String,
    pub event_type: String,
    pub label: String,
    pub id_field: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphEventEdgeSource {
    pub stream: String,
    pub event_type: String,
    pub from_field: String,
    pub to_field: String,
    pub label: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct GraphProjectionData {
    pub name: String,
    pub definition: GraphProjection,
    pub nodes: BTreeMap<String, GraphNode>,
    pub edges: BTreeMap<String, GraphEdge>,
}

impl crate::residency::ResidentBytes for GraphProjectionData {
    fn heap_bytes(&self) -> u64 {
        use crate::residency::string_bytes;

        // Node/edge payloads carry JSON `Value` trees whose deep size is not
        // cheaply walkable; the keyed spine and identity strings are counted, and
        // the payloads are approximated by their inline size. Graph projections
        // are a cold path, so a coarse figure here is acceptable — it is recorded
        // as its own category precisely so an unexpectedly large one is visible.
        let nodes = self.nodes.len() as u64 * (std::mem::size_of::<(String, GraphNode)>() as u64)
            + self.nodes.keys().map(|k| string_bytes(k)).sum::<u64>();
        let edges = self.edges.len() as u64 * (std::mem::size_of::<(String, GraphEdge)>() as u64)
            + self.edges.keys().map(|k| string_bytes(k)).sum::<u64>();
        string_bytes(&self.name) + nodes + edges
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphPath {
    pub nodes: Vec<String>,
    pub edges: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphVerifyReport {
    pub projection: String,
    pub stored_nodes: usize,
    pub rebuilt_nodes: usize,
    pub stored_edges: usize,
    pub rebuilt_edges: usize,
    pub graph_size_bytes: u64,
    pub valid: bool,
}

impl GraphProjection {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            node_sources: Vec::new(),
            edge_sources: Vec::new(),
            event_node_sources: Vec::new(),
            event_edge_sources: Vec::new(),
        }
    }

    pub fn nodes_from(mut self, collection: impl Into<String>, label: impl Into<String>) -> Self {
        self.node_sources.push(GraphNodeSource {
            collection: collection.into(),
            label: label.into(),
        });
        self
    }

    pub fn edge_from_field(
        mut self,
        collection: impl Into<String>,
        from_field: impl Into<String>,
        to_field: impl Into<String>,
        label: impl Into<String>,
    ) -> Self {
        self.edge_sources.push(GraphEdgeSource {
            collection: collection.into(),
            from_field: GraphEndpoint::Field(from_field.into()),
            to_field: GraphEndpoint::Field(to_field.into()),
            label: label.into(),
        });
        self
    }

    pub fn edge_to_record(
        mut self,
        collection: impl Into<String>,
        from_field: impl Into<String>,
        record_label: impl Into<String>,
        label: impl Into<String>,
    ) -> Self {
        self.edge_sources.push(GraphEdgeSource {
            collection: collection.into(),
            from_field: GraphEndpoint::Field(from_field.into()),
            to_field: GraphEndpoint::Record {
                label: record_label.into(),
            },
            label: label.into(),
        });
        self
    }

    pub fn edge_from_record(
        mut self,
        collection: impl Into<String>,
        record_label: impl Into<String>,
        to_field: impl Into<String>,
        label: impl Into<String>,
    ) -> Self {
        self.edge_sources.push(GraphEdgeSource {
            collection: collection.into(),
            from_field: GraphEndpoint::Record {
                label: record_label.into(),
            },
            to_field: GraphEndpoint::Field(to_field.into()),
            label: label.into(),
        });
        self
    }

    pub fn nodes_from_events(
        mut self,
        stream: impl Into<String>,
        event_type: impl Into<String>,
        label: impl Into<String>,
        id_field: impl Into<String>,
    ) -> Self {
        self.event_node_sources.push(GraphEventNodeSource {
            stream: stream.into(),
            event_type: event_type.into(),
            label: label.into(),
            id_field: id_field.into(),
        });
        self
    }

    pub fn edge_from_event_fields(
        mut self,
        stream: impl Into<String>,
        event_type: impl Into<String>,
        from_field: impl Into<String>,
        to_field: impl Into<String>,
        label: impl Into<String>,
    ) -> Self {
        self.event_edge_sources.push(GraphEventEdgeSource {
            stream: stream.into(),
            event_type: event_type.into(),
            from_field: from_field.into(),
            to_field: to_field.into(),
            label: label.into(),
        });
        self
    }

    pub(crate) fn collection_names(&self) -> BTreeSet<String> {
        self.node_sources
            .iter()
            .map(|source| source.collection.clone())
            .chain(
                self.edge_sources
                    .iter()
                    .map(|source| source.collection.clone()),
            )
            .collect()
    }

    pub(crate) fn stream_names(&self) -> BTreeSet<String> {
        self.event_node_sources
            .iter()
            .map(|source| source.stream.clone())
            .chain(
                self.event_edge_sources
                    .iter()
                    .map(|source| source.stream.clone()),
            )
            .collect()
    }
}

impl GraphProjectionData {
    pub(crate) fn build(
        definition: GraphProjection,
        records_by_collection: &HashMap<String, Vec<Record>>,
        events_by_stream: &HashMap<String, Vec<StoredEvent>>,
    ) -> Result<Self> {
        let mut graph = Self {
            name: definition.name.clone(),
            definition,
            nodes: BTreeMap::new(),
            edges: BTreeMap::new(),
        };
        graph.add_record_nodes(records_by_collection);
        graph.add_record_edges(records_by_collection);
        graph.add_event_nodes(events_by_stream);
        graph.add_event_edges(events_by_stream);
        Ok(graph)
    }

    pub fn neighbors(&self, node_id: &str) -> Vec<GraphNode> {
        let mut ids = BTreeSet::new();
        for edge in self.edges.values() {
            if edge.from == node_id {
                ids.insert(edge.to.clone());
            } else if edge.to == node_id {
                ids.insert(edge.from.clone());
            }
        }
        ids.into_iter()
            .filter_map(|id| self.nodes.get(&id).cloned())
            .collect()
    }

    pub fn edges(&self, node_id: &str) -> Vec<GraphEdge> {
        self.edges
            .values()
            .filter(|edge| edge.from == node_id || edge.to == node_id)
            .cloned()
            .collect()
    }

    pub fn path(&self, from: &str, to: &str, max_depth: usize) -> Option<GraphPath> {
        if from == to {
            return self.nodes.contains_key(from).then(|| GraphPath {
                nodes: vec![from.to_string()],
                edges: Vec::new(),
            });
        }
        if max_depth == 0 || !self.nodes.contains_key(from) || !self.nodes.contains_key(to) {
            return None;
        }

        let mut visited = HashSet::from([from.to_string()]);
        let mut queue = VecDeque::from([GraphPath {
            nodes: vec![from.to_string()],
            edges: Vec::new(),
        }]);

        while let Some(path) = queue.pop_front() {
            let current = path.nodes.last()?;
            if path.edges.len() >= max_depth {
                continue;
            }
            for edge in self.edges(current) {
                let next = if edge.from == *current {
                    edge.to.clone()
                } else {
                    edge.from.clone()
                };
                if !visited.insert(next.clone()) {
                    continue;
                }
                let mut next_path = path.clone();
                next_path.nodes.push(next.clone());
                next_path.edges.push(edge.id.clone());
                if next == to {
                    return Some(next_path);
                }
                queue.push_back(next_path);
            }
        }
        None
    }

    pub fn traverse(&self, start: &str, edge_label: &str, depth: usize) -> Vec<GraphNode> {
        if depth == 0 {
            return self.nodes.get(start).cloned().into_iter().collect();
        }

        let mut frontier = BTreeSet::from([start.to_string()]);
        let mut reached = BTreeSet::new();
        for _ in 0..depth {
            let mut next_frontier = BTreeSet::new();
            for node in &frontier {
                for edge in self.edges.values() {
                    if edge.from == *node && edge.label == edge_label {
                        reached.insert(edge.to.clone());
                        next_frontier.insert(edge.to.clone());
                    }
                }
            }
            if next_frontier.is_empty() {
                break;
            }
            frontier = next_frontier;
        }
        reached
            .into_iter()
            .filter_map(|id| self.nodes.get(&id).cloned())
            .collect()
    }

    pub fn graph_size_bytes(&self) -> u64 {
        serde_json::to_vec(self)
            .map(|bytes| bytes.len() as u64)
            .unwrap_or(0)
    }

    fn add_record_nodes(&mut self, records_by_collection: &HashMap<String, Vec<Record>>) {
        for source in self.definition.node_sources.clone() {
            let Some(records) = records_by_collection.get(&source.collection) else {
                continue;
            };
            for record in records {
                self.nodes.insert(
                    node_id(&source.label, &record.id),
                    GraphNode {
                        id: node_id(&source.label, &record.id),
                        label: source.label.clone(),
                        properties: record.metadata.clone(),
                    },
                );
            }
        }
    }

    fn add_record_edges(&mut self, records_by_collection: &HashMap<String, Vec<Record>>) {
        for source in self.definition.edge_sources.clone() {
            let Some(records) = records_by_collection.get(&source.collection) else {
                continue;
            };
            for record in records {
                let Some(from) = endpoint_for_record(&source.from_field, record) else {
                    continue;
                };
                let Some(to) = endpoint_for_record(&source.to_field, record) else {
                    continue;
                };
                self.ensure_endpoint_node(&from);
                self.ensure_endpoint_node(&to);
                let id = format!(
                    "{}:{}:{}:{}:{}",
                    source.collection, record.id, source.label, from, to
                );
                self.edges.insert(
                    id.clone(),
                    GraphEdge {
                        id,
                        from,
                        to,
                        label: source.label.clone(),
                        properties: record.metadata.clone(),
                        timestamp: record.timestamp,
                    },
                );
            }
        }
    }

    fn add_event_nodes(&mut self, events_by_stream: &HashMap<String, Vec<StoredEvent>>) {
        for source in self.definition.event_node_sources.clone() {
            let Some(events) = events_by_stream.get(&source.stream) else {
                continue;
            };
            for stored in events
                .iter()
                .filter(|stored| stored.event.event_type == source.event_type)
            {
                let Some(raw_id) = json_field_string(&stored.event.payload, &source.id_field)
                else {
                    continue;
                };
                let id = normalize_endpoint(&source.label, &raw_id);
                self.nodes.insert(
                    id.clone(),
                    GraphNode {
                        id,
                        label: source.label.clone(),
                        properties: stored.event.payload.clone(),
                    },
                );
            }
        }
    }

    fn add_event_edges(&mut self, events_by_stream: &HashMap<String, Vec<StoredEvent>>) {
        for source in self.definition.event_edge_sources.clone() {
            let Some(events) = events_by_stream.get(&source.stream) else {
                continue;
            };
            for stored in events
                .iter()
                .filter(|stored| stored.event.event_type == source.event_type)
            {
                let Some(from) = event_endpoint(&stored.event.payload, &source.from_field) else {
                    continue;
                };
                let Some(to) = event_endpoint(&stored.event.payload, &source.to_field) else {
                    continue;
                };
                self.ensure_endpoint_node(&from);
                self.ensure_endpoint_node(&to);
                let id = format!(
                    "event:{}:{}:{}:{}:{}",
                    source.stream, stored.offset, source.label, from, to
                );
                self.edges.insert(
                    id.clone(),
                    GraphEdge {
                        id,
                        from,
                        to,
                        label: source.label.clone(),
                        properties: stored.event.payload.clone(),
                        timestamp: Some(stored.event.timestamp),
                    },
                );
            }
        }
    }

    fn ensure_endpoint_node(&mut self, id: &str) {
        if self.nodes.contains_key(id) {
            return;
        }
        let label = id.split_once(':').map(|(label, _)| label).unwrap_or("Node");
        self.nodes.insert(
            id.to_string(),
            GraphNode {
                id: id.to_string(),
                label: label.to_string(),
                properties: json!({}),
            },
        );
    }
}

fn endpoint_for_record(endpoint: &GraphEndpoint, record: &Record) -> Option<String> {
    match endpoint {
        GraphEndpoint::Field(field) => {
            if field == "id" {
                return Some(record.id.clone());
            }
            let raw = record_field_string(record, field)?;
            Some(normalize_endpoint(&infer_label(field), &raw))
        }
        GraphEndpoint::Record { label } => Some(node_id(label, &record.id)),
    }
}

fn event_endpoint(payload: &Value, field: &str) -> Option<String> {
    let raw = json_field_string(payload, field)?;
    Some(normalize_endpoint(&infer_label(field), &raw))
}

fn record_field_string(record: &Record, field: &str) -> Option<String> {
    match field {
        "id" => Some(record.id.clone()),
        "timestamp" => record.timestamp.map(|timestamp| timestamp.to_string()),
        other => json_field_string(&record.metadata, other).or_else(|| {
            record
                .metadata
                .get("metadata")
                .and_then(|metadata| json_field_string(metadata, other))
        }),
    }
}

fn json_field_string(value: &Value, path: &str) -> Option<String> {
    let mut current = value;
    for part in path.split('.') {
        current = current.get(part)?;
    }
    Some(match current {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Null => return None,
        other => other.to_string(),
    })
}

fn normalize_endpoint(label: &str, raw: &str) -> String {
    if raw.contains(':') {
        raw.to_string()
    } else {
        node_id(label, raw)
    }
}

fn node_id(label: &str, id: &str) -> String {
    format!("{label}:{id}")
}

fn infer_label(field: &str) -> String {
    let field = field
        .split('.')
        .next_back()
        .unwrap_or(field)
        .strip_suffix("_id")
        .unwrap_or(field);
    let mut label = String::new();
    let mut uppercase_next = true;
    for ch in field.chars() {
        if !ch.is_ascii_alphanumeric() {
            uppercase_next = true;
            continue;
        }
        if uppercase_next {
            label.push(ch.to_ascii_uppercase());
            uppercase_next = false;
        } else {
            label.push(ch);
        }
    }
    if label.is_empty() {
        "Node".to_string()
    } else {
        label
    }
}

pub(crate) fn graph_error(message: impl Into<String>) -> BicDbError {
    BicDbError::ProjectionError(message.into())
}
