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
    pub database_root: std::path::PathBuf,
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
    // Prevent accidental reuse in another local directory. This supplements
    // content binding; a path never supplies backing-birth/replica authority.
    let database_root = db.data_path().canonicalize().map_err(BicDbError::from)?;
    hash_item(&mut hasher, &database_root)?;
    let mut nodes = BTreeMap::new();
    let mut retained_bytes = 0usize;
    scan(db, SCHEMA_COLLECTION, |raw| {
        hash_item(&mut hasher, (&raw.id, &raw))?;
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
    // Role membership, namespace grants and called routine/type definitions
    // are part of the reviewed policy/constraint environment too. Stream them
    // into the digest; do not retain unrelated business rows or secrets.
    for collection in [
        NAMESPACE_COLLECTION,
        ROLE_COLLECTION,
        ROLE_MEMBERSHIP_COLLECTION,
        PRIVILEGE_COLLECTION,
        DEFAULT_PRIVILEGE_COLLECTION,
        ROUTINE_COLLECTION,
        USER_TYPE_COLLECTION,
        VIEW_COLLECTION,
    ] {
        hash_item(&mut hasher, collection)?;
        scan(db, collection, |raw| {
            hash_item(&mut hasher, (&raw.id, &raw))
        })?;
    }
    let mut metas = db.collections().into_iter().collect::<Vec<_>>();
    metas.sort_by(|a, b| a.name.cmp(&b.name));
    for meta in metas {
        hash_item(&mut hasher, meta)?;
    }
    let mut definitions = db.index_definitions().into_iter().collect::<Vec<_>>();
    definitions.sort_by(|a, b| a.name.cmp(&b.name));
    hash_item(&mut hasher, definitions)?;
    let mut triggers = Vec::new();
    scan(db, TRIGGER_COLLECTION, |raw| {
        let trigger: TriggerSchema = serde_json::from_value(raw.metadata.clone())?;
        if nodes.contains_key(&trigger.table_name) {
            retained_bytes =
                retained_bytes.saturating_add(serde_json::to_vec(&raw)?.len().saturating_mul(4));
            if retained_bytes > db.query_work_memory_bytes() {
                return Err(required(
                    "inspection trigger metadata exceeds configured work memory",
                ));
            }
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
                retained_bytes = retained_bytes
                    .saturating_add(serde_json::to_vec(&stored)?.len().saturating_mul(4))
                    .saturating_add(serde_json::to_vec(&historical)?.len().saturating_mul(4));
                if retained_bytes > db.query_work_memory_bytes() {
                    return Err(required(
                        "inspection policy report exceeds configured work memory",
                    ));
                }
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
        database_root,
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

const CERTIFICATE_COLLECTION: &str = "__bicdb_pg_partition_certificate";
const POLICY_ARCHIVE_COLLECTION: &str = "__bicdb_pg_partition_policy_archive";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartitionUpgradeApplied {
    pub source_snapshot: String,
    pub moved_rows: usize,
    pub policy_decisions: usize,
    pub created_trigger_clones: usize,
}

fn planned_destination(
    db: &BicDb,
    snapshot: &Snapshot,
    source: &Node,
    row: &Record,
    relocations: &BTreeMap<(String, String), String>,
) -> Result<Option<String>> {
    if let Some(destination) = relocations.get(&(source.schema.name.clone(), row.id.clone())) {
        let target = snapshot
            .nodes
            .get(destination)
            .ok_or_else(|| required("planned destination is missing"))?;
        if target.schema.partitioning.is_some()
            || root_node(destination, &snapshot.nodes)?.schema.name
                != root_node(&source.schema.name, &snapshot.nodes)?.schema.name
        {
            return Err(required(
                "planned destination is outside the exact source tree or is not a leaf",
            ));
        }
        partition::validate_record(db, &target.schema, row)
            .map_err(|error| required(error.to_string()))?;
        return Ok(Some(destination.clone()));
    }
    if source.schema.partitioning.is_some() {
        return partition::route_record(db, &source.schema, row).map(|target| Some(target.name));
    }
    Ok(None)
}

/// Apply one reviewed local snapshot. This API requires the host's exclusive
/// database access; it never grants a missing Native context or mutation grant.
/// SQL entry-point admission and automatic startup integration are separate.
pub fn apply_partition_upgrade(
    db: &mut BicDb,
    plan: Option<&PartitionUpgradePlan>,
) -> Result<PartitionUpgradeApplied> {
    let snapshot = gather(db)?;
    if snapshot.inspection.legacy_relations.is_empty() {
        return Err(required("there is no legacy partition transition to apply"));
    }
    if plan.is_some_and(|plan| plan.snapshot != snapshot.inspection.snapshot) {
        return Err(required(
            "operator snapshot changed or belongs to another directory; inspect again",
        ));
    }
    let decisions = plan.map(|plan| &plan.policies).cloned().unwrap_or_default();
    let required_policies = snapshot
        .inspection
        .policy_decisions_required
        .iter()
        .map(|conflict| conflict.relation.clone())
        .collect::<BTreeSet<_>>();
    if decisions.keys().cloned().collect::<BTreeSet<_>>() != required_policies {
        return Err(required(
            "explicit decisions for exactly the reported ambiguous child policies are required",
        ));
    }
    let mut relocations = BTreeMap::new();
    for relocation in plan
        .map(|plan| plan.row_relocations.as_slice())
        .unwrap_or(&[])
    {
        if relocations
            .insert(
                (relocation.source.clone(), relocation.record_id.clone()),
                relocation.destination.clone(),
            )
            .is_some()
        {
            return Err(required("duplicate relocation action"));
        }
    }
    let invalid = snapshot
        .inspection
        .invalid_rows
        .iter()
        .map(|row| (row.source.clone(), row.record_id.clone()))
        .collect::<BTreeSet<_>>();
    if relocations.keys().cloned().collect::<BTreeSet<_>>() != invalid {
        return Err(required(
            "explicit relocations for exactly the reported invalid ACKs are required",
        ));
    }
    partition::validate_existing_tree(
        db,
        &snapshot
            .nodes
            .values()
            .map(|node| node.schema.clone())
            .collect::<Vec<_>>(),
    )?;
    let mut schema_records = Vec::new();
    let mut cached_schemas = Vec::new();
    let mut archives = Vec::new();
    let migration_id = uuid::Uuid::new_v4().to_string();
    for node in snapshot.nodes.values() {
        let mut raw = node.raw.clone();
        for field in ["partitioning", "partition_of"] {
            if let Some(value) = raw.metadata.get_mut(field).filter(|value| !value.is_null()) {
                value["execution_version"] = serde_json::json!(1);
            }
        }
        if let Some(decision) = decisions.get(&node.schema.name) {
            archives.push(Record::new(format!("{migration_id}:{}",node.schema.name)).with_metadata(serde_json::json!({"migration_id":migration_id,"relation":node.schema.name,"original_record":node.raw,"decision":decision,"source_snapshot":snapshot.inspection.snapshot})));
            if *decision == PartitionPolicyDecision::PreserveHistoricalEffective {
                let conflict = snapshot
                    .inspection
                    .policy_decisions_required
                    .iter()
                    .find(|conflict| conflict.relation == node.schema.name)
                    .unwrap();
                for field in ["rls_enabled", "rls_forced", "policies"] {
                    raw.metadata[field] = conflict.historical_effective[field].clone();
                }
            }
        }
        cached_schemas.push(serde_json::from_value::<TableSchema>(raw.metadata.clone())?);
        schema_records.push(raw);
    }
    // Plan the complete inherited trigger forest without adopting an authored
    // same-name child trigger. Preserve already valid clone modes/identities.
    let mut triggers = BTreeMap::new();
    let mut original_keys = BTreeSet::new();
    for raw in &snapshot.triggers {
        let trigger: TriggerSchema = serde_json::from_value(raw.metadata.clone())?;
        if raw.id != trigger_key(&trigger.table_name, &trigger.name) {
            return Err(required("ambiguous trigger catalog key"));
        }
        original_keys.insert(raw.id.clone());
        triggers.insert(raw.id.clone(), trigger);
    }
    let mut queue = snapshot
        .nodes
        .values()
        .filter(|node| node.schema.partition_of.is_none())
        .map(|node| node.schema.name.clone())
        .collect::<Vec<_>>();
    let mut position = 0;
    while position < queue.len() {
        let parent = &snapshot.nodes[&queue[position]].schema;
        position += 1;
        let parents = triggers
            .values()
            .filter(|trigger| {
                trigger.table_name == parent.name && trigger.for_each.eq_ignore_ascii_case("row")
            })
            .cloned()
            .collect::<Vec<_>>();
        for child in snapshot.nodes.values().filter(|child| {
            child.schema.partition_of.as_ref().is_some_and(|link| {
                link.parent_table == parent.name && link.parent_schema == parent.schema_name
            })
        }) {
            queue.push(child.schema.name.clone());
            for trigger in &parents {
                let clone = super::partition_triggers::clone_for_partition(trigger, &child.schema)?;
                let key = trigger_key(&clone.table_name, &clone.name);
                if let Some(existing) = triggers.get(&key) {
                    if existing.table_name != clone.table_name
                        || existing.name != clone.name
                        || existing.partition_parent.as_ref().is_none_or(|link| {
                            link.table_name != trigger.table_name
                                || link.trigger_name != trigger.name
                        })
                        || existing.function_name != clone.function_name
                        || existing.event != clone.event
                        || existing.timing != clone.timing
                        || existing.arguments != clone.arguments
                    {
                        return Err(required("an independently authored or inconsistent child trigger conflicts with required inheritance"));
                    }
                } else {
                    triggers.insert(key, clone);
                }
            }
        }
    }
    let trigger_records = triggers
        .into_iter()
        .filter(|(key, _)| !original_keys.contains(key))
        .map(|(key, trigger)| Ok(Record::new(key).with_metadata(serde_json::to_value(trigger)?)))
        .collect::<Result<Vec<_>>>()?;
    // Preflight every proposed destination and primary-key collision before
    // activating the sticky fence. Never collapse two acknowledged records.
    let mut destinations = BTreeSet::new();
    let mut destination_bytes = 0usize;
    for source in snapshot.nodes.values() {
        scan(db, &source.schema.name, |row| {
            if let Some(target) = planned_destination(db, &snapshot, source, &row, &relocations)? {
                destination_bytes = destination_bytes
                    .saturating_add(target.len())
                    .saturating_add(row.id.len())
                    .saturating_add(128);
                if destination_bytes > db.query_work_memory_bytes() {
                    return Err(required(
                        "upgrade destination plan exceeds configured work memory",
                    ));
                }
                if target == source.schema.name
                    || !destinations.insert((target.clone(), row.id.clone()))
                {
                    return Err(required("duplicate or stationary relocation"));
                }
                if db.get(&target, &row.id)?.is_some()
                    && !relocations.contains_key(&(target.clone(), row.id.clone()))
                {
                    return Err(required("two acknowledged records conflict in the destination; reconcile explicitly"));
                }
            }
            Ok(())
        })?;
    }
    db.enable_sql_partition_execution_v1()?;
    db.create_collection(CERTIFICATE_COLLECTION)?;
    if !archives.is_empty() {
        db.create_collection(POLICY_ARCHIVE_COLLECTION)?;
    }
    if !trigger_records.is_empty() {
        ensure_trigger_collection(db)?;
    }
    let moved_rows = destinations.len();
    let clone_count = trigger_records.len();
    let applied = PartitionUpgradeApplied {
        source_snapshot: snapshot.inspection.snapshot.clone(),
        moved_rows,
        policy_decisions: decisions.len(),
        created_trigger_clones: clone_count,
    };
    let certificate=Record::new("current").with_metadata(serde_json::json!({"version":1,"reader_capability":"sql_partition_execution_v1","migration_id":migration_id,"applied":applied}));
    let mut tx = db.begin_transaction()?;
    for source in snapshot.nodes.values() {
        scan(db, &source.schema.name, |row| {
            if planned_destination(db, &snapshot, source, &row, &relocations)?.is_some() {
                tx.delete(&source.schema.name, &row.id)?;
            }
            Ok(())
        })?;
    }
    for source in snapshot.nodes.values() {
        scan(db, &source.schema.name, |row| {
            if let Some(target) = planned_destination(db, &snapshot, source, &row, &relocations)? {
                if tx.get(&target, &row.id)?.is_some() {
                    return Err(required("pending destination collision"));
                }
                tx.batch_insert(&target, vec![row])?;
            }
            Ok(())
        })?;
    }
    for raw in &schema_records {
        tx.batch_insert(SCHEMA_COLLECTION, vec![raw.clone()])?;
    }
    if !archives.is_empty() {
        tx.batch_insert(POLICY_ARCHIVE_COLLECTION, archives)?;
    }
    if !trigger_records.is_empty() {
        tx.batch_insert(TRIGGER_COLLECTION, trigger_records)?;
    }
    {
        let engine = SqlEngine::new(db).with_transaction(&tx);
        for source in snapshot.nodes.values() {
            scan(db, &source.schema.name, |row| {
                if let Some(target) =
                    planned_destination(db, &snapshot, source, &row, &relocations)?
                {
                    let schema = &snapshot.nodes[&target].schema;
                    let schema = if let Some(link) = &schema.partition_of {
                        schema_with_parent_inheritance(
                            schema.clone(),
                            &snapshot.nodes[&link.parent_table].schema,
                        )
                    } else {
                        schema.clone()
                    };
                    validate_records_for_write(
                        db,
                        Some(&tx),
                        &target,
                        &schema,
                        std::slice::from_ref(&row),
                        true,
                        Some(&engine),
                    )?;
                }
                Ok(())
            })?;
        }
    }
    tx.batch_insert(CERTIFICATE_COLLECTION, vec![certificate])?;
    tx.commit()?;
    sql_schema_list_cache_invalidate(db);
    for schema in cached_schemas {
        sql_schema_cache_set(db, &schema.name, Some(Arc::new(schema.clone())));
    }
    Ok(applied)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn legacy_constraint_preflight_matches_native_pg_before_fence_or_publication() {
        for mode in [
            bicdb_core::StorageMode::EmbeddedMemory,
            bicdb_core::StorageMode::ServerPaged,
        ] {
            for ddl in [
                "CREATE TABLE p(k INT PRIMARY KEY,payload TEXT) PARTITION BY RANGE(payload)",
                "CREATE TABLE p(k INT,payload TEXT,UNIQUE(payload)) PARTITION BY RANGE(k)",
            ] {
                let root = tempfile::tempdir().unwrap();
                let mut db = BicDb::open_with_config(
                    root.path(),
                    bicdb_core::DbConfig::default()
                        .with_storage_mode(mode.clone())
                        .with_fsync(true)
                        .with_paged_buffer_pool_bytes(8 * 1024 * 1024),
                )
                .unwrap();
                // This models metadata accepted by the preceding parser. Real
                // old-binary row/policy fixtures are qualified independently.
                SqlSession::new(&mut db).execute(ddl).unwrap();
                let before = db.collection_generation(SCHEMA_COLLECTION);
                let error = apply_partition_upgrade(&mut db, None).unwrap_err();
                assert_eq!(error.sqlstate(), "0A000");
                assert!(!db.sql_partition_execution_feature_active());
                assert_eq!(db.collection_generation(SCHEMA_COLLECTION), before);
                db.close().unwrap();
            }
        }
    }
}
