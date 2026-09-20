use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::db::BicDb;
use crate::error::{BicDbError, Result};
use crate::event::Event;
use crate::record::Record;
use crate::vector;

pub const DEFAULT_MEMORY_COLLECTION: &str = "bicdb_memories";
pub const MEMORY_EVENT_STREAM: &str = "bicdb.memory";

const DAY_SECONDS: i64 = 86_400;
const DEFAULT_RECENCY_HALF_LIFE_SECONDS: f32 = 30.0 * DAY_SECONDS as f32;
const DEFAULT_DECAY_HALF_LIFE_SECONDS: f32 = 60.0 * DAY_SECONDS as f32;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum MemoryType {
    Semantic,
    Episodic,
    Procedural,
    Preference,
    Fact,
    Goal,
    Task,
    Conversation,
    Observation,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Memory {
    pub id: String,
    pub memory_type: MemoryType,
    pub agent_id: String,
    pub user_id: Option<String>,
    pub content: String,
    pub embedding: Option<Vec<f32>>,
    pub importance: f32,
    pub confidence: f32,
    pub created_at: i64,
    pub updated_at: i64,
    pub expires_at: Option<i64>,
    pub metadata: Value,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub struct MemoryScoringWeights {
    pub similarity: f32,
    pub importance: f32,
    pub recency: f32,
    pub confidence: f32,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct MemoryRecallOptions {
    pub agent_id: Option<String>,
    pub user_id: Option<String>,
    pub memory_type: Option<MemoryType>,
    pub now: Option<i64>,
    pub include_expired: bool,
    pub weights: MemoryScoringWeights,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MemoryRecallResult {
    pub memory: Memory,
    pub score: f32,
    pub similarity: f32,
    pub importance: f32,
    pub recency: f32,
    pub confidence: f32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ConversationMessage {
    pub memory_id: String,
    pub role: String,
    pub content: String,
    pub timestamp: i64,
    pub metadata: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentWorkspace {
    pub agent_id: String,
    pub user_id: Option<String>,
    pub goals: Vec<Memory>,
    pub tasks: Vec<Memory>,
    pub knowledge: Vec<Memory>,
    pub memories: Vec<Memory>,
    pub recent_context: Vec<Memory>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentWorkspaceSnapshot {
    pub agent_id: String,
    pub user_id: Option<String>,
    pub goals: Vec<Memory>,
    pub tasks: Vec<Memory>,
    pub knowledge: Vec<Memory>,
    pub memories: Vec<Memory>,
    pub recent_context: Vec<Memory>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MemoryTimeline {
    pub user_id: String,
    pub yesterday: Vec<Memory>,
    pub last_week: Vec<Memory>,
    pub last_month: Vec<Memory>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MemorySummaryInput {
    pub id: String,
    pub agent_id: String,
    pub user_id: Option<String>,
    pub content: String,
    pub source_memory_ids: Vec<String>,
    pub embedding: Option<Vec<f32>>,
    pub importance: f32,
    pub confidence: f32,
    pub created_at: i64,
    pub metadata: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MemorySummary {
    pub summary: Memory,
    pub source_memory_ids: Vec<String>,
    pub source_count: usize,
}

pub struct MemoryStore<'db> {
    db: &'db mut BicDb,
    collection: String,
    recency_half_life_seconds: f32,
    decay_half_life_seconds: f32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredMemory {
    memory_type: MemoryType,
    agent_id: String,
    user_id: Option<String>,
    content: String,
    importance: f32,
    confidence: f32,
    created_at: i64,
    updated_at: i64,
    expires_at: Option<i64>,
    metadata: Value,
    reinforcement_count: u32,
    last_reinforced_at: Option<i64>,
    forgotten: bool,
}

#[derive(Clone, Debug)]
struct MemoryState {
    memory: Memory,
    reinforcement_count: u32,
    last_reinforced_at: Option<i64>,
    forgotten: bool,
}

#[derive(Clone, Debug)]
struct MemoryHeapItem {
    result: MemoryRecallResult,
    sequence: usize,
}

impl PartialEq for MemoryHeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.result.score.total_cmp(&other.result.score) == Ordering::Equal
            && self.sequence == other.sequence
    }
}

impl Eq for MemoryHeapItem {}

impl PartialOrd for MemoryHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MemoryHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .result
            .score
            .total_cmp(&self.result.score)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

impl BicDb {
    pub fn memory(&mut self) -> MemoryStore<'_> {
        MemoryStore::new(self)
    }
}

impl Memory {
    pub fn new(
        id: impl Into<String>,
        memory_type: MemoryType,
        agent_id: impl Into<String>,
        content: impl Into<String>,
    ) -> Self {
        let now = unix_timestamp();
        Self {
            id: id.into(),
            memory_type,
            agent_id: agent_id.into(),
            user_id: None,
            content: content.into(),
            embedding: None,
            importance: 0.5,
            confidence: 1.0,
            created_at: now,
            updated_at: now,
            expires_at: None,
            metadata: Value::Object(Map::new()),
        }
    }

    pub fn with_user_id(mut self, user_id: impl Into<String>) -> Self {
        self.user_id = Some(user_id.into());
        self
    }

    pub fn with_embedding(mut self, embedding: Vec<f32>) -> Self {
        self.embedding = Some(embedding);
        self
    }

    pub fn with_importance(mut self, importance: f32) -> Self {
        self.importance = importance;
        self
    }

    pub fn with_confidence(mut self, confidence: f32) -> Self {
        self.confidence = confidence;
        self
    }

    pub fn with_created_at(mut self, created_at: i64) -> Self {
        self.created_at = created_at;
        self.updated_at = created_at;
        self
    }

    pub fn with_updated_at(mut self, updated_at: i64) -> Self {
        self.updated_at = updated_at;
        self
    }

    pub fn with_expires_at(mut self, expires_at: i64) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    pub fn with_metadata(mut self, metadata: Value) -> Self {
        self.metadata = metadata;
        self
    }
}

impl Default for MemoryScoringWeights {
    fn default() -> Self {
        Self {
            similarity: 0.60,
            importance: 0.20,
            recency: 0.15,
            confidence: 0.05,
        }
    }
}

impl MemoryRecallOptions {
    pub fn with_agent_id(mut self, agent_id: impl Into<String>) -> Self {
        self.agent_id = Some(agent_id.into());
        self
    }

    pub fn with_user_id(mut self, user_id: impl Into<String>) -> Self {
        self.user_id = Some(user_id.into());
        self
    }

    pub fn with_memory_type(mut self, memory_type: MemoryType) -> Self {
        self.memory_type = Some(memory_type);
        self
    }

    pub fn with_now(mut self, now: i64) -> Self {
        self.now = Some(now);
        self
    }

    pub fn include_expired(mut self, include_expired: bool) -> Self {
        self.include_expired = include_expired;
        self
    }

    pub fn with_weights(mut self, weights: MemoryScoringWeights) -> Self {
        self.weights = weights;
        self
    }
}

impl AgentWorkspace {
    pub fn snapshot(&self) -> AgentWorkspaceSnapshot {
        AgentWorkspaceSnapshot {
            agent_id: self.agent_id.clone(),
            user_id: self.user_id.clone(),
            goals: self.goals.clone(),
            tasks: self.tasks.clone(),
            knowledge: self.knowledge.clone(),
            memories: self.memories.clone(),
            recent_context: self.recent_context.clone(),
        }
    }

    pub fn restore(snapshot: AgentWorkspaceSnapshot) -> Self {
        Self {
            agent_id: snapshot.agent_id,
            user_id: snapshot.user_id,
            goals: snapshot.goals,
            tasks: snapshot.tasks,
            knowledge: snapshot.knowledge,
            memories: snapshot.memories,
            recent_context: snapshot.recent_context,
        }
    }

    pub fn restore_from(&mut self, snapshot: AgentWorkspaceSnapshot) {
        *self = Self::restore(snapshot);
    }
}

impl<'db> MemoryStore<'db> {
    fn new(db: &'db mut BicDb) -> Self {
        Self {
            db,
            collection: DEFAULT_MEMORY_COLLECTION.to_string(),
            recency_half_life_seconds: DEFAULT_RECENCY_HALF_LIFE_SECONDS,
            decay_half_life_seconds: DEFAULT_DECAY_HALF_LIFE_SECONDS,
        }
    }

    pub fn with_collection(mut self, collection: impl Into<String>) -> Self {
        self.collection = collection.into();
        self
    }

    pub fn with_recency_half_life_seconds(mut self, seconds: f32) -> Self {
        self.recency_half_life_seconds = seconds.max(1.0);
        self
    }

    pub fn with_decay_half_life_seconds(mut self, seconds: f32) -> Self {
        self.decay_half_life_seconds = seconds.max(1.0);
        self
    }

    pub fn remember(&mut self, memory: Memory) -> Result<Memory> {
        self.ensure_collection()?;
        let memory = normalize_memory(memory)?;
        let record = memory_to_record(&memory, 0, None, false)?;
        self.db.insert(&self.collection, record)?;
        self.emit_memory_event("MemoryCreated", event_payload(&memory))?;
        Ok(memory)
    }

    pub fn get(&self, memory_id: &str) -> Result<Option<Memory>> {
        let Some(record) = self.get_record(memory_id)? else {
            return Ok(None);
        };
        let state = record_to_memory_state(&record)?;
        if state.forgotten || is_expired(&state.memory, unix_timestamp()) {
            return Ok(None);
        }
        Ok(Some(state.memory))
    }

    pub fn recall(
        &self,
        query_embedding: &[f32],
        top_k: usize,
        options: MemoryRecallOptions,
    ) -> Result<Vec<MemoryRecallResult>> {
        validate_query(query_embedding, top_k)?;
        validate_weights(options.weights)?;
        let now = options.now.unwrap_or_else(unix_timestamp);
        let states = self.active_states(now, options.include_expired)?;
        if let Some(expected) = states
            .iter()
            .find_map(|state| state.memory.embedding.as_ref().map(Vec::len))
        {
            if expected != query_embedding.len() {
                return Err(BicDbError::DimensionMismatch {
                    collection: self.collection.clone(),
                    expected,
                    actual: query_embedding.len(),
                });
            }
        }

        let mut heap = BinaryHeap::with_capacity(top_k.saturating_add(1));
        for (sequence, state) in states.into_iter().enumerate() {
            if !matches_options(&state.memory, &options) {
                continue;
            }
            let result = score_memory(
                state.memory,
                query_embedding,
                now,
                options.weights,
                self.recency_half_life_seconds,
            );
            heap.push(MemoryHeapItem { result, sequence });
            if heap.len() > top_k {
                heap.pop();
            }
        }

        let mut results = heap.into_iter().map(|item| item.result).collect::<Vec<_>>();
        results.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| right.memory.updated_at.cmp(&left.memory.updated_at))
                .then_with(|| left.memory.id.cmp(&right.memory.id))
        });
        Ok(results)
    }

    pub fn recall_recent(&self, agent_id: &str, limit: usize, now: i64) -> Result<Vec<Memory>> {
        let mut memories = self
            .active_states(now, false)?
            .into_iter()
            .map(|state| state.memory)
            .filter(|memory| memory.agent_id == agent_id)
            .collect::<Vec<_>>();
        sort_recent(&mut memories);
        memories.truncate(limit);
        Ok(memories)
    }

    pub fn recall_by_type(
        &self,
        agent_id: &str,
        memory_type: MemoryType,
        limit: usize,
        now: i64,
    ) -> Result<Vec<Memory>> {
        let mut memories = self
            .active_states(now, false)?
            .into_iter()
            .map(|state| state.memory)
            .filter(|memory| memory.agent_id == agent_id && memory.memory_type == memory_type)
            .collect::<Vec<_>>();
        sort_recent(&mut memories);
        memories.truncate(limit);
        Ok(memories)
    }

    pub fn recall_for_user(
        &self,
        agent_id: &str,
        user_id: &str,
        limit: usize,
        now: i64,
    ) -> Result<Vec<Memory>> {
        let mut memories = self
            .active_states(now, false)?
            .into_iter()
            .map(|state| state.memory)
            .filter(|memory| {
                memory.agent_id == agent_id && memory.user_id.as_deref() == Some(user_id)
            })
            .collect::<Vec<_>>();
        sort_recent(&mut memories);
        memories.truncate(limit);
        Ok(memories)
    }

    pub fn reinforce(&mut self, memory_id: &str, amount: f32, now: i64) -> Result<Option<Memory>> {
        let Some(record) = self.get_record(memory_id)? else {
            return Ok(None);
        };
        let mut state = record_to_memory_state(&record)?;
        state.memory.importance = clamp_unit(state.memory.importance + amount.max(0.0));
        state.memory.confidence = clamp_unit(state.memory.confidence + amount.max(0.0) * 0.25);
        state.memory.updated_at = now;
        state.reinforcement_count = state.reinforcement_count.saturating_add(1);
        state.last_reinforced_at = Some(now);

        let updated = normalize_memory(state.memory)?;
        let record = memory_to_record(
            &updated,
            state.reinforcement_count,
            state.last_reinforced_at,
            false,
        )?;
        self.db.insert(&self.collection, record)?;
        self.emit_memory_event("MemoryReinforced", event_payload(&updated))?;
        Ok(Some(updated))
    }

    pub fn forget(&mut self, memory_id: &str) -> Result<bool> {
        let Some(record) = self.get_record(memory_id)? else {
            return Ok(false);
        };
        let state = record_to_memory_state(&record)?;
        let deleted = self.db.delete(&self.collection, memory_id)?;
        if deleted {
            self.emit_memory_event("MemoryForgotten", event_payload(&state.memory))?;
        }
        Ok(deleted)
    }

    pub fn expire(&mut self, now: i64) -> Result<Vec<Memory>> {
        let expired = self
            .all_states()?
            .into_iter()
            .filter(|state| !state.forgotten && is_expired(&state.memory, now))
            .collect::<Vec<_>>();
        let mut removed = Vec::with_capacity(expired.len());
        for state in expired {
            if self.db.delete(&self.collection, &state.memory.id)? {
                self.emit_memory_event("MemoryExpired", event_payload(&state.memory))?;
                removed.push(state.memory);
            }
        }
        Ok(removed)
    }

    pub fn decay(&mut self, now: i64) -> Result<usize> {
        let states = self.active_states(now, false)?;
        let mut changed = 0;
        for mut state in states {
            let old_importance = state.memory.importance;
            let old_confidence = state.memory.confidence;
            let slowdown =
                1.0 + old_importance.max(0.0) * 2.0 + state.reinforcement_count as f32 * 0.5;
            let half_life = (self.decay_half_life_seconds * slowdown).max(1.0);
            let age = (now - state.memory.updated_at).max(0) as f32;
            let factor = 2_f32.powf(-(age / half_life));
            state.memory.importance = clamp_unit(state.memory.importance * factor);
            state.memory.confidence = clamp_unit(state.memory.confidence * (0.9 + 0.1 * factor));

            if (old_importance - state.memory.importance).abs() < f32::EPSILON
                && (old_confidence - state.memory.confidence).abs() < f32::EPSILON
            {
                continue;
            }

            let record = memory_to_record(
                &state.memory,
                state.reinforcement_count,
                state.last_reinforced_at,
                state.forgotten,
            )?;
            self.db.insert(&self.collection, record)?;
            self.emit_memory_event("MemoryDecayed", event_payload(&state.memory))?;
            changed += 1;
        }
        Ok(changed)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn ingest_message(
        &mut self,
        id: impl Into<String>,
        agent_id: impl Into<String>,
        user_id: Option<&str>,
        conversation_id: impl Into<String>,
        role: impl Into<String>,
        content: impl Into<String>,
        embedding: Option<Vec<f32>>,
        timestamp: i64,
    ) -> Result<Memory> {
        let conversation_id = conversation_id.into();
        let role = role.into();
        let mut metadata = Map::new();
        metadata.insert(
            "conversation_id".to_string(),
            Value::String(conversation_id),
        );
        metadata.insert("role".to_string(), Value::String(role));

        let mut memory = Memory::new(id, MemoryType::Conversation, agent_id, content)
            .with_created_at(timestamp)
            .with_metadata(Value::Object(metadata));
        memory.user_id = user_id.map(str::to_string);
        memory.embedding = embedding;
        self.remember(memory)
    }

    pub fn chat_history(
        &self,
        agent_id: &str,
        conversation_id: &str,
        limit: Option<usize>,
        now: i64,
    ) -> Result<Vec<ConversationMessage>> {
        let mut messages = self
            .active_states(now, false)?
            .into_iter()
            .map(|state| state.memory)
            .filter(|memory| {
                memory.agent_id == agent_id
                    && memory.memory_type == MemoryType::Conversation
                    && metadata_string(&memory.metadata, "conversation_id") == Some(conversation_id)
            })
            .map(|memory| ConversationMessage {
                memory_id: memory.id,
                role: metadata_string(&memory.metadata, "role")
                    .unwrap_or("unknown")
                    .to_string(),
                content: memory.content,
                timestamp: memory.created_at,
                metadata: memory.metadata,
            })
            .collect::<Vec<_>>();
        messages.sort_by(|left, right| {
            left.timestamp
                .cmp(&right.timestamp)
                .then_with(|| left.memory_id.cmp(&right.memory_id))
        });
        if let Some(limit) = limit {
            if messages.len() > limit {
                messages = messages.split_off(messages.len() - limit);
            }
        }
        Ok(messages)
    }

    pub fn reconstruct_conversation(
        &self,
        agent_id: &str,
        conversation_id: &str,
        now: i64,
    ) -> Result<Vec<ConversationMessage>> {
        self.chat_history(agent_id, conversation_id, None, now)
    }

    pub fn workspace(
        &self,
        agent_id: &str,
        user_id: Option<&str>,
        now: i64,
    ) -> Result<AgentWorkspace> {
        let mut memories = self
            .active_states(now, false)?
            .into_iter()
            .map(|state| state.memory)
            .filter(|memory| {
                memory.agent_id == agent_id
                    && user_id
                        .map(|user_id| memory.user_id.as_deref() == Some(user_id))
                        .unwrap_or(true)
            })
            .collect::<Vec<_>>();
        sort_recent(&mut memories);

        let mut goals = memories
            .iter()
            .filter(|memory| memory.memory_type == MemoryType::Goal)
            .cloned()
            .collect::<Vec<_>>();
        let mut tasks = memories
            .iter()
            .filter(|memory| memory.memory_type == MemoryType::Task)
            .cloned()
            .collect::<Vec<_>>();
        let mut knowledge = memories
            .iter()
            .filter(|memory| {
                matches!(
                    memory.memory_type,
                    MemoryType::Semantic
                        | MemoryType::Procedural
                        | MemoryType::Preference
                        | MemoryType::Fact
                        | MemoryType::Observation
                )
            })
            .cloned()
            .collect::<Vec<_>>();
        knowledge.sort_by(|left, right| {
            right
                .importance
                .total_cmp(&left.importance)
                .then_with(|| right.updated_at.cmp(&left.updated_at))
                .then_with(|| left.id.cmp(&right.id))
        });
        sort_recent(&mut goals);
        sort_recent(&mut tasks);

        let mut recent_context = memories
            .iter()
            .filter(|memory| {
                matches!(
                    memory.memory_type,
                    MemoryType::Conversation | MemoryType::Episodic | MemoryType::Observation
                )
            })
            .take(20)
            .cloned()
            .collect::<Vec<_>>();
        sort_recent(&mut recent_context);

        Ok(AgentWorkspace {
            agent_id: agent_id.to_string(),
            user_id: user_id.map(str::to_string),
            goals,
            tasks,
            knowledge,
            memories,
            recent_context,
        })
    }

    pub fn timeline(&self, user_id: &str, now: i64) -> Result<MemoryTimeline> {
        let mut memories = self
            .active_states(now, false)?
            .into_iter()
            .map(|state| state.memory)
            .filter(|memory| memory.user_id.as_deref() == Some(user_id))
            .collect::<Vec<_>>();
        sort_recent(&mut memories);
        let yesterday_start = now.saturating_sub(DAY_SECONDS);
        let week_start = now.saturating_sub(7 * DAY_SECONDS);
        let month_start = now.saturating_sub(30 * DAY_SECONDS);

        Ok(MemoryTimeline {
            user_id: user_id.to_string(),
            yesterday: memories
                .iter()
                .filter(|memory| memory.created_at >= yesterday_start)
                .cloned()
                .collect(),
            last_week: memories
                .iter()
                .filter(|memory| memory.created_at >= week_start)
                .cloned()
                .collect(),
            last_month: memories
                .iter()
                .filter(|memory| memory.created_at >= month_start)
                .cloned()
                .collect(),
        })
    }

    pub fn summarize(&mut self, input: MemorySummaryInput) -> Result<MemorySummary> {
        if input.source_memory_ids.is_empty() {
            return Err(BicDbError::Memory(
                "summary must reference at least one memory".to_string(),
            ));
        }
        let mut metadata = object_or_empty(input.metadata);
        metadata.insert(
            "summary_of".to_string(),
            Value::Array(
                input
                    .source_memory_ids
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            ),
        );

        let mut summary = Memory::new(
            input.id,
            MemoryType::Semantic,
            input.agent_id,
            input.content,
        )
        .with_created_at(input.created_at)
        .with_importance(input.importance)
        .with_confidence(input.confidence)
        .with_metadata(Value::Object(metadata));
        summary.user_id = input.user_id;
        summary.embedding = input.embedding;

        let summary = self.remember(summary)?;
        self.emit_memory_event(
            "MemorySummarized",
            json!({
                "memory_id": summary.id,
                "agent_id": summary.agent_id,
                "user_id": summary.user_id,
                "source_memory_ids": input.source_memory_ids,
                "source_count": input.source_memory_ids.len(),
            }),
        )?;
        let source_count = input.source_memory_ids.len();
        Ok(MemorySummary {
            summary,
            source_memory_ids: input.source_memory_ids,
            source_count,
        })
    }

    fn ensure_collection(&mut self) -> Result<()> {
        self.db.create_collection(&self.collection)
    }

    fn get_record(&self, memory_id: &str) -> Result<Option<Record>> {
        match self.db.get(&self.collection, memory_id) {
            Ok(record) => Ok(record.map(|record| record.as_ref().clone())),
            Err(BicDbError::CollectionNotFound(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn all_states(&self) -> Result<Vec<MemoryState>> {
        match self.db.scan_collection(&self.collection) {
            Ok(records) => records.iter().map(record_to_memory_state).collect(),
            Err(BicDbError::CollectionNotFound(_)) => Ok(Vec::new()),
            Err(error) => Err(error),
        }
    }

    fn active_states(&self, now: i64, include_expired: bool) -> Result<Vec<MemoryState>> {
        Ok(self
            .all_states()?
            .into_iter()
            .filter(|state| !state.forgotten)
            .filter(|state| include_expired || !is_expired(&state.memory, now))
            .collect())
    }

    fn emit_memory_event(&mut self, event_type: &str, payload: Value) -> Result<u64> {
        self.db.events_mut().append(
            Event::new(MEMORY_EVENT_STREAM, event_type, payload)
                .with_metadata(json!({"subsystem": "memory"})),
        )
    }
}

fn memory_to_record(
    memory: &Memory,
    reinforcement_count: u32,
    last_reinforced_at: Option<i64>,
    forgotten: bool,
) -> Result<Record> {
    let stored = StoredMemory {
        memory_type: memory.memory_type,
        agent_id: memory.agent_id.clone(),
        user_id: memory.user_id.clone(),
        content: memory.content.clone(),
        importance: memory.importance,
        confidence: memory.confidence,
        created_at: memory.created_at,
        updated_at: memory.updated_at,
        expires_at: memory.expires_at,
        metadata: memory.metadata.clone(),
        reinforcement_count,
        last_reinforced_at,
        forgotten,
    };
    let mut record = Record::new(&memory.id)
        .with_metadata(serde_json::to_value(stored)?)
        .with_timestamp(memory.updated_at);
    if let Some(embedding) = memory.embedding.clone() {
        record = record.with_vector(embedding);
    }
    Ok(record)
}

fn record_to_memory_state(record: &Record) -> Result<MemoryState> {
    let stored: StoredMemory = serde_json::from_value(record.metadata.clone())?;
    let embedding = record
        .vector
        .as_ref()
        .filter(|embedding| !embedding.is_empty())
        .cloned();
    Ok(MemoryState {
        memory: Memory {
            id: record.id.clone(),
            memory_type: stored.memory_type,
            agent_id: stored.agent_id,
            user_id: stored.user_id,
            content: stored.content,
            embedding,
            importance: stored.importance,
            confidence: stored.confidence,
            created_at: stored.created_at,
            updated_at: stored.updated_at,
            expires_at: stored.expires_at,
            metadata: stored.metadata,
        },
        reinforcement_count: stored.reinforcement_count,
        last_reinforced_at: stored.last_reinforced_at,
        forgotten: stored.forgotten,
    })
}

fn normalize_memory(mut memory: Memory) -> Result<Memory> {
    validate_memory(&memory)?;
    memory.importance = clamp_unit(memory.importance);
    memory.confidence = clamp_unit(memory.confidence);
    if memory.updated_at < memory.created_at {
        memory.updated_at = memory.created_at;
    }
    Ok(memory)
}

fn validate_memory(memory: &Memory) -> Result<()> {
    if memory.id.trim().is_empty() {
        return Err(BicDbError::EmptyRecordId);
    }
    if memory.agent_id.trim().is_empty() {
        return Err(BicDbError::Memory("agent_id must not be empty".to_string()));
    }
    if memory.content.trim().is_empty() {
        return Err(BicDbError::Memory("content must not be empty".to_string()));
    }
    if !memory.importance.is_finite() || !memory.confidence.is_finite() {
        return Err(BicDbError::Memory(
            "importance and confidence must be finite".to_string(),
        ));
    }
    if let Some(embedding) = memory.embedding.as_deref() {
        validate_embedding(embedding)?;
    }
    Ok(())
}

fn validate_query(query_embedding: &[f32], top_k: usize) -> Result<()> {
    if top_k == 0 {
        return Err(BicDbError::InvalidTopK);
    }
    validate_embedding(query_embedding)
}

fn validate_embedding(embedding: &[f32]) -> Result<()> {
    if embedding.is_empty() {
        return Err(BicDbError::EmptyVector);
    }
    if embedding.iter().any(|value| !value.is_finite()) {
        return Err(BicDbError::NonFiniteVectorValue);
    }
    Ok(())
}

fn validate_weights(weights: MemoryScoringWeights) -> Result<()> {
    if !weights.similarity.is_finite()
        || !weights.importance.is_finite()
        || !weights.recency.is_finite()
        || !weights.confidence.is_finite()
    {
        return Err(BicDbError::Memory(
            "memory scoring weights must be finite".to_string(),
        ));
    }
    Ok(())
}

fn score_memory(
    memory: Memory,
    query_embedding: &[f32],
    now: i64,
    weights: MemoryScoringWeights,
    recency_half_life_seconds: f32,
) -> MemoryRecallResult {
    let raw_similarity = memory
        .embedding
        .as_deref()
        .filter(|embedding| embedding.len() == query_embedding.len())
        .and_then(|embedding| vector::cosine_similarity(embedding, query_embedding).ok())
        .unwrap_or(0.0);
    let similarity = ((raw_similarity + 1.0) * 0.5).clamp(0.0, 1.0);
    let importance = clamp_unit(memory.importance);
    let confidence = clamp_unit(memory.confidence);
    let recency = recency_score(memory.updated_at, now, recency_half_life_seconds);
    let score = similarity * weights.similarity
        + importance * weights.importance
        + recency * weights.recency
        + confidence * weights.confidence;

    MemoryRecallResult {
        memory,
        score,
        similarity,
        importance,
        recency,
        confidence,
    }
}

fn recency_score(updated_at: i64, now: i64, half_life_seconds: f32) -> f32 {
    let age = (now - updated_at).max(0) as f32;
    2_f32
        .powf(-(age / half_life_seconds.max(1.0)))
        .clamp(0.0, 1.0)
}

fn matches_options(memory: &Memory, options: &MemoryRecallOptions) -> bool {
    options
        .agent_id
        .as_ref()
        .map(|agent_id| &memory.agent_id == agent_id)
        .unwrap_or(true)
        && options
            .user_id
            .as_ref()
            .map(|user_id| memory.user_id.as_ref() == Some(user_id))
            .unwrap_or(true)
        && options
            .memory_type
            .map(|memory_type| memory.memory_type == memory_type)
            .unwrap_or(true)
}

fn is_expired(memory: &Memory, now: i64) -> bool {
    memory
        .expires_at
        .map(|expires_at| expires_at <= now)
        .unwrap_or(false)
}

fn event_payload(memory: &Memory) -> Value {
    json!({
        "memory_id": memory.id,
        "memory_type": memory.memory_type,
        "agent_id": memory.agent_id,
        "user_id": memory.user_id,
        "created_at": memory.created_at,
        "updated_at": memory.updated_at,
        "expires_at": memory.expires_at,
    })
}

fn sort_recent(memories: &mut [Memory]) {
    memories.sort_by(|left, right| {
        right
            .updated_at
            .cmp(&left.updated_at)
            .then_with(|| right.created_at.cmp(&left.created_at))
            .then_with(|| left.id.cmp(&right.id))
    });
}

fn metadata_string<'a>(metadata: &'a Value, key: &str) -> Option<&'a str> {
    metadata.as_object()?.get(key)?.as_str()
}

fn object_or_empty(value: Value) -> Map<String, Value> {
    match value {
        Value::Object(map) => map,
        value => {
            let mut map = Map::new();
            if !value.is_null() {
                map.insert("value".to_string(), value);
            }
            map
        }
    }
}

fn clamp_unit(value: f32) -> f32 {
    value.clamp(0.0, 1.0)
}

fn unix_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}
