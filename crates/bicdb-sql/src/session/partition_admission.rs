//! Local partition-upgrade inspection. A snapshot hash binds an operator's
//! decisions to actual local records; it is never portable origin authority.
use crate::*;
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PartitionPolicyDecision {
    KeepStored,
    PreserveHistoricalEffective,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionRowRelocation {
    pub source: String,
    pub record_id: String,
    pub destination: String,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PartitionUpgradePlan {
    pub snapshot: String,
    #[serde(default)]
    pub policies: BTreeMap<String, PartitionPolicyDecision>,
    #[serde(default)]
    pub row_relocations: Vec<PartitionRowRelocation>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartitionPolicyConflict {
    pub relation: String,
    pub stored: JsonValue,
    pub historical_effective: JsonValue,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartitionInvalidRow {
    pub source: String,
    pub record_id: String,
    pub reason: String,
    pub suggested_destination: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartitionUpgradeInspection {
    pub snapshot: String,
    pub legacy_relations: Vec<String>,
    pub policy_decisions_required: Vec<PartitionPolicyConflict>,
    pub invalid_rows: Vec<PartitionInvalidRow>,
    pub parent_heap_rows: usize,
    pub acknowledged_rows_examined: usize,
}

#[derive(Clone)]
struct Node {
    raw: Record,
    schema: TableSchema,
    legacy: bool,
}
struct Snapshot {
    nodes: BTreeMap<String, Node>,
    triggers: Vec<Record>,
    inspection: PartitionUpgradeInspection,
}
fn required(message: impl Into<String>) -> SqlError {
    SqlError::object_not_in_prerequisite_state(format!("SQL partition upgrade: {}", message.into()))
}
fn affected(record: &Record) -> bool {
    ["partitioning", "partition_of"].iter().any(|name| {
        record
            .metadata
            .get(*name)
            .is_some_and(|value| !value.is_null())
    })
}
fn policy_state(schema: &TableSchema) -> JsonValue {
    serde_json::json!({"rls_enabled":schema.rls_enabled,"rls_forced":schema.rls_forced,"policies":schema.policies})
}
fn canonical(value: JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(values) => {
            let sorted = values.into_iter().collect::<BTreeMap<_, _>>();
            JsonValue::Object(
                sorted
                    .into_iter()
                    .map(|(key, value)| (key, canonical(value)))
                    .collect(),
            )
        }
        JsonValue::Array(values) => JsonValue::Array(values.into_iter().map(canonical).collect()),
        other => other,
    }
}
fn hash_item(hasher: &mut Sha256, value: impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec(&canonical(serde_json::to_value(value)?))?;
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
    Ok(())
}
fn scan(db: &BicDb, collection: &str, mut visit: impl FnMut(Record) -> Result<()>) -> Result<()> {
    let mut cursor = None;
    loop {
        let rows = match db.scan_collection_batch_after(
            collection,
            cursor.as_deref(),
            128,
            8 * 1024 * 1024,
        ) {
            Ok(rows) => rows,
            Err(BicDbError::CollectionNotFound(_)) => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        if rows.is_empty() {
            return Ok(());
        }
        cursor = rows.last().map(|row| row.id.clone());
        for row in rows {
            visit(row)?;
        }
    }
}
fn root_node<'a>(name: &str, nodes: &'a BTreeMap<String, Node>) -> Result<&'a Node> {
    let mut node = nodes
        .get(name)
        .ok_or_else(|| required("missing partition relation"))?;
    let mut seen = BTreeSet::new();
    while let Some(link) = &node.schema.partition_of {
        if !seen.insert(node.schema.name.clone()) {
            return Err(required("cyclic partition ancestry"));
        }
        let parent = nodes
            .get(&link.parent_table)
            .ok_or_else(|| required(format!("missing exact parent {}", link.parent_table)))?;
        if parent.schema.schema_name != link.parent_schema || parent.schema.partitioning.is_none() {
            return Err(required("partition parent namespace/type mismatch"));
        }
        node = parent;
    }
    Ok(node)
}
fn gather(db: &BicDb) -> Result<Snapshot> {
    db.check_sql_partition_upgrade_quiescent()?;
    let initial_sequence = db.last_commit_seq();
    let mut hasher = Sha256::new();
    hash_item(&mut hasher, "sql_partition_upgrade_snapshot_v1")?;
    let mut nodes = BTreeMap::new();
    let mut retained_bytes = 0usize;
    scan(db, SCHEMA_COLLECTION, |raw| {
        if !affected(&raw) {
            return Ok(());
        }
        let schema: TableSchema = serde_json::from_value(raw.metadata.clone())?;
        if raw.id != schema.name {
            return Err(required("schema key does not match its authored identity"));
        }
        let mut legacy = false;
        for field in ["partitioning", "partition_of"] {
            if let Some(value) = raw.metadata.get(field).filter(|v| !v.is_null()) {
                match value.get("execution_version") {
                    None | Some(JsonValue::Null) => legacy = true,
                    Some(value) if value.as_u64() == Some(1) => {}
                    Some(_) => {
                        return Err(required("unknown or malformed partition execution version"))
                    }
                }
            }
        }
        retained_bytes =
            retained_bytes.saturating_add(serde_json::to_vec(&raw)?.len().saturating_mul(4));
        if retained_bytes > db.query_work_memory_bytes() {
            return Err(required(
                "inspection metadata exceeds configured work memory",
            ));
        }
        if nodes
            .insert(
                schema.name.clone(),
                Node {
                    raw,
                    schema,
                    legacy,
                },
            )
            .is_some()
        {
            return Err(required("duplicate exact partition identity"));
        }
        Ok(())
    })?;
    for node in nodes.values() {
        root_node(&node.schema.name, &nodes)?;
        hash_item(&mut hasher, (&node.schema.name, &node.raw))?;
    }
    let mut metas = db
        .collections()
        .into_iter()
        .filter(|meta| nodes.contains_key(&meta.name))
        .collect::<Vec<_>>();
    metas.sort_by(|a, b| a.name.cmp(&b.name));
    for meta in metas {
        hash_item(&mut hasher, meta)?;
    }
    let mut definitions = db
        .index_definitions()
        .into_iter()
        .filter(|index| nodes.contains_key(&index.collection))
        .collect::<Vec<_>>();
    definitions.sort_by(|a, b| a.name.cmp(&b.name));
    hash_item(&mut hasher, definitions)?;
    let mut triggers = Vec::new();
    scan(db, TRIGGER_COLLECTION, |raw| {
        let trigger: TriggerSchema = serde_json::from_value(raw.metadata.clone())?;
        if nodes.contains_key(&trigger.table_name) {
            hash_item(&mut hasher, (&raw.id, &raw))?;
            triggers.push(raw);
        }
        Ok(())
    })?;
    let mut conflicts = Vec::new();
    let mut invalid = Vec::new();
    let mut parent_rows = 0usize;
    let mut rows_examined = 0usize;
    for node in nodes.values() {
        if node.legacy && node.schema.partition_of.is_some() {
            let stored = policy_state(&node.schema);
            // The actual old loader substitutes the immediate parent's raw
            // policy state, not an invented recursive inheritance rule.
            let link = node.schema.partition_of.as_ref().unwrap();
            let historical = policy_state(
                &nodes
                    .get(&link.parent_table)
                    .ok_or_else(|| required("missing policy parent"))?
                    .schema,
            );
            if stored != historical {
                conflicts.push(PartitionPolicyConflict {
                    relation: node.schema.name.clone(),
                    stored,
                    historical_effective: historical,
                });
            }
        }
        hash_item(&mut hasher, (&node.schema.name, "rows"))?;
        scan(db, &node.schema.name, |row| {
            rows_examined += 1;
            hash_item(&mut hasher, &row)?;
            if node.schema.partitioning.is_some() {
                parent_rows += 1;
                if !node.legacy {
                    return Err(required(
                        "certified partitioned relation retains physical parent-heap rows",
                    ));
                }
                if let Err(error) = partition::route_record(db, &node.schema, &row) {
                    invalid.push(PartitionInvalidRow {
                        source: node.schema.name.clone(),
                        record_id: row.id,
                        reason: error.to_string(),
                        suggested_destination: None,
                    });
                }
            } else if let Err(error) = partition::validate_record(db, &node.schema, &row) {
                let destination = partition::route_record(
                    db,
                    &root_node(&node.schema.name, &nodes)?.schema,
                    &row,
                )
                .ok()
                .map(|schema| schema.name);
                invalid.push(PartitionInvalidRow {
                    source: node.schema.name.clone(),
                    record_id: row.id,
                    reason: error.to_string(),
                    suggested_destination: destination,
                });
            }
            if invalid
                .len()
                .saturating_mul(1024)
                .saturating_add(retained_bytes)
                > db.query_work_memory_bytes()
            {
                return Err(required(
                    "inspection problem list exceeds configured work memory",
                ));
            }
            Ok(())
        })?;
    }
    db.check_sql_partition_upgrade_quiescent()?;
    if db.last_commit_seq() != initial_sequence {
        return Err(required(
            "dataset changed during inspection; retry against a quiescent snapshot",
        ));
    }
    let inspection = PartitionUpgradeInspection {
        snapshot: hex::encode(hasher.finalize()),
        legacy_relations: nodes
            .values()
            .filter(|node| node.legacy)
            .map(|node| node.schema.name.clone())
            .collect(),
        policy_decisions_required: conflicts,
        invalid_rows: invalid,
        parent_heap_rows: parent_rows,
        acknowledged_rows_examined: rows_examined,
    };
    Ok(Snapshot {
        nodes,
        triggers,
        inspection,
    })
}

/// Read-only local inspection. The host must exclude other writers; the hash
/// rejects changed inputs when a later explicit migration plan is applied.
/// Protected Native collections retain their normal context/grant checks.
pub fn inspect_partition_upgrade(db: &BicDb) -> Result<PartitionUpgradeInspection> {
    Ok(gather(db)?.inspection)
}
