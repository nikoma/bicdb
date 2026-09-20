use bicdb_core::{
    AgentWorkspace, BicDb, DbConfig, Memory, MemoryRecallOptions, MemoryScoringWeights,
    MemorySummaryInput, MemoryType, MEMORY_EVENT_STREAM,
};
use serde_json::json;

fn open_temp() -> (tempfile::TempDir, BicDb) {
    let temp = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(temp.path(), DbConfig::default().with_fsync(false)).unwrap();
    (temp, db)
}

#[test]
fn remember_and_weighted_recall_rank_agent_memories() {
    let (_temp, mut db) = open_temp();
    let now = 1_710_000_000;

    {
        let mut memory = db.memory();
        memory
            .remember(
                Memory::new(
                    "pref-yoga",
                    MemoryType::Preference,
                    "coach",
                    "User likes yoga",
                )
                .with_user_id("user-1")
                .with_embedding(vec![1.0, 0.0, 0.0])
                .with_importance(0.9)
                .with_confidence(0.8)
                .with_created_at(now - 60),
            )
            .unwrap();
        memory
            .remember(
                Memory::new(
                    "task-call",
                    MemoryType::Task,
                    "coach",
                    "Call patient tomorrow",
                )
                .with_user_id("user-1")
                .with_embedding(vec![0.0, 1.0, 0.0])
                .with_importance(0.2)
                .with_confidence(0.7)
                .with_created_at(now - 10),
            )
            .unwrap();
    }

    let results = db
        .memory()
        .recall(
            &[1.0, 0.0, 0.0],
            2,
            MemoryRecallOptions::default()
                .with_agent_id("coach")
                .with_user_id("user-1")
                .with_now(now),
        )
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].memory.id, "pref-yoga");
    assert!(results[0].score > results[1].score);

    let events = db.events().read(MEMORY_EVENT_STREAM);
    assert_eq!(events.len(), 2);
    assert!(events
        .iter()
        .all(|event| event.event.event_type == "MemoryCreated"));
}

#[test]
fn recall_helpers_filter_recent_type_and_user() {
    let (_temp, mut db) = open_temp();
    let now = 1_710_000_000;
    {
        let mut memory = db.memory();
        memory
            .remember(
                Memory::new("goal", MemoryType::Goal, "agent", "Reach 10k practitioners")
                    .with_user_id("user-1")
                    .with_created_at(now - 10),
            )
            .unwrap();
        memory
            .remember(
                Memory::new(
                    "fact",
                    MemoryType::Fact,
                    "agent",
                    "Patient uses metric units",
                )
                .with_user_id("user-1")
                .with_created_at(now - 20),
            )
            .unwrap();
        memory
            .remember(
                Memory::new("other", MemoryType::Fact, "agent", "Other user fact")
                    .with_user_id("user-2")
                    .with_created_at(now - 5),
            )
            .unwrap();
    }

    let memory = db.memory();
    assert_eq!(memory.recall_recent("agent", 2, now).unwrap().len(), 2);
    assert_eq!(
        memory
            .recall_by_type("agent", MemoryType::Goal, 10, now)
            .unwrap()[0]
            .id,
        "goal"
    );
    assert_eq!(
        memory
            .recall_for_user("agent", "user-1", 10, now)
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn reinforce_decay_forget_and_expire_update_state_and_events() {
    let (_temp, mut db) = open_temp();
    let now = 1_710_000_000;
    {
        let mut memory = db.memory();
        memory
            .remember(
                Memory::new("old", MemoryType::Semantic, "agent", "Old fact")
                    .with_importance(0.8)
                    .with_confidence(0.6)
                    .with_created_at(now - 90 * 86_400),
            )
            .unwrap();
        memory.reinforce("old", 0.1, now).unwrap();
        let reinforced = memory.get("old").unwrap().unwrap();
        assert!(reinforced.importance > 0.8);

        let decayed = memory.decay(now + 90 * 86_400).unwrap();
        assert_eq!(decayed, 1);
        let decayed_memory = memory.get("old").unwrap().unwrap();
        assert!(decayed_memory.importance < reinforced.importance);

        memory
            .remember(
                Memory::new("temp", MemoryType::Task, "agent", "Temporary task")
                    .with_created_at(now - 1)
                    .with_expires_at(now),
            )
            .unwrap();
        let expired = memory.expire(now + 1).unwrap();
        assert_eq!(expired.len(), 1);
        assert!(memory.get("temp").unwrap().is_none());

        assert!(memory.forget("old").unwrap());
        assert!(memory.get("old").unwrap().is_none());
    }

    let event_types = db
        .events()
        .read(MEMORY_EVENT_STREAM)
        .into_iter()
        .map(|event| event.event.event_type)
        .collect::<Vec<_>>();
    assert!(event_types.contains(&"MemoryCreated".to_string()));
    assert!(event_types.contains(&"MemoryReinforced".to_string()));
    assert!(event_types.contains(&"MemoryDecayed".to_string()));
    assert!(event_types.contains(&"MemoryExpired".to_string()));
    assert!(event_types.contains(&"MemoryForgotten".to_string()));
}

#[test]
fn conversation_history_reconstructs_messages_in_order() {
    let (_temp, mut db) = open_temp();
    let mut memory = db.memory();

    memory
        .ingest_message(
            "msg-1",
            "health-agent",
            Some("john"),
            "visit-1",
            "user",
            "My HRV was low yesterday",
            None,
            100,
        )
        .unwrap();
    memory
        .ingest_message(
            "msg-2",
            "health-agent",
            Some("john"),
            "visit-1",
            "assistant",
            "Let's compare it with your blood pressure history",
            None,
            101,
        )
        .unwrap();

    let history = memory
        .chat_history("health-agent", "visit-1", Some(10), 200)
        .unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].role, "user");
    assert_eq!(
        history[1].content,
        "Let's compare it with your blood pressure history"
    );

    let replay = memory
        .reconstruct_conversation("health-agent", "visit-1", 200)
        .unwrap();
    assert_eq!(replay, history);
}

#[test]
fn workspace_timeline_and_summary_preserve_context() {
    let (_temp, mut db) = open_temp();
    let now = 1_710_000_000;
    {
        let mut memory = db.memory();
        memory
            .remember(
                Memory::new("goal", MemoryType::Goal, "agent", "Lower blood pressure")
                    .with_user_id("john")
                    .with_created_at(now - 60),
            )
            .unwrap();
        memory
            .remember(
                Memory::new("task", MemoryType::Task, "agent", "Schedule follow-up")
                    .with_user_id("john")
                    .with_created_at(now - 2 * 86_400),
            )
            .unwrap();
        memory
            .remember(
                Memory::new("bp", MemoryType::Observation, "agent", "BP was 130/85")
                    .with_user_id("john")
                    .with_created_at(now - 6 * 86_400),
            )
            .unwrap();

        let summary = memory
            .summarize(MemorySummaryInput {
                id: "summary-1".to_string(),
                agent_id: "agent".to_string(),
                user_id: Some("john".to_string()),
                content: "John is working on blood pressure control.".to_string(),
                source_memory_ids: vec!["goal".to_string(), "task".to_string(), "bp".to_string()],
                embedding: None,
                importance: 0.9,
                confidence: 0.8,
                created_at: now,
                metadata: json!({"kind": "session_summary"}),
            })
            .unwrap();
        assert_eq!(summary.source_memory_ids.len(), 3);
        assert_eq!(summary.summary.metadata["summary_of"][0], "goal");
    }

    let memory = db.memory();
    let workspace = memory.workspace("agent", Some("john"), now).unwrap();
    assert_eq!(workspace.goals.len(), 1);
    assert_eq!(workspace.tasks.len(), 1);
    assert!(workspace
        .knowledge
        .iter()
        .any(|memory| memory.id == "summary-1"));

    let snapshot = workspace.snapshot();
    let restored = AgentWorkspace::restore(snapshot);
    assert_eq!(restored.agent_id, "agent");
    assert_eq!(restored.goals[0].id, "goal");

    let timeline = memory.timeline("john", now).unwrap();
    assert_eq!(timeline.yesterday.len(), 2);
    assert_eq!(timeline.last_week.len(), 4);
    assert_eq!(timeline.last_month.len(), 4);

    let event_types = db
        .events()
        .read(MEMORY_EVENT_STREAM)
        .into_iter()
        .map(|event| event.event.event_type)
        .collect::<Vec<_>>();
    assert!(event_types.contains(&"MemorySummarized".to_string()));
}

#[test]
fn scoring_weights_are_configurable() {
    let (_temp, mut db) = open_temp();
    let now = 1_710_000_000;
    {
        let mut memory = db.memory();
        memory
            .remember(
                Memory::new(
                    "similar",
                    MemoryType::Fact,
                    "agent",
                    "Similar low confidence",
                )
                .with_embedding(vec![1.0, 0.0])
                .with_importance(0.1)
                .with_confidence(0.1)
                .with_created_at(now),
            )
            .unwrap();
        memory
            .remember(
                Memory::new(
                    "important",
                    MemoryType::Fact,
                    "agent",
                    "Important but less similar",
                )
                .with_embedding(vec![0.0, 1.0])
                .with_importance(1.0)
                .with_confidence(1.0)
                .with_created_at(now),
            )
            .unwrap();
    }

    let default_top = db
        .memory()
        .recall(&[1.0, 0.0], 1, MemoryRecallOptions::default().with_now(now))
        .unwrap()[0]
        .memory
        .id
        .clone();
    let weighted_top = db
        .memory()
        .recall(
            &[1.0, 0.0],
            1,
            MemoryRecallOptions::default()
                .with_now(now)
                .with_weights(MemoryScoringWeights {
                    similarity: 0.1,
                    importance: 0.7,
                    recency: 0.0,
                    confidence: 0.2,
                }),
        )
        .unwrap()[0]
        .memory
        .id
        .clone();

    assert_eq!(default_top, "similar");
    assert_eq!(weighted_top, "important");
}
