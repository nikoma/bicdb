//! Local partition-upgrade inspection. A snapshot hash binds an operator's
//! decisions to actual local records; it is never portable origin authority.
use crate::*;
use sha2::{Digest, Sha256};
mod catalog;
mod preflight;

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
            (db.query_work_memory_bytes() / 16).clamp(1, 8 * 1024 * 1024),
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
        let schema: TableSchema = serde_json::from_value(raw.metadata.clone())?;
        if raw.id != schema.name {
            return Err(required("schema key does not match its authored identity"));
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
    // These empty bookkeeping collections can survive interruption after the
    // fence and before the atomic publication. Their existence is not a
    // change to the operator-reviewed inputs or evidence of completion.
    metas.retain(|meta| {
        meta.name != CERTIFICATE_COLLECTION && meta.name != POLICY_ARCHIVE_COLLECTION
    });
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

pub(crate) const CERTIFICATE_COLLECTION: &str = "__bicdb_pg_partition_certificate";
const POLICY_ARCHIVE_COLLECTION: &str = "__bicdb_pg_partition_policy_archive";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartitionUpgradeApplied {
    pub source_snapshot: String,
    pub moved_rows: usize,
    pub policy_decisions: usize,
    pub created_trigger_clones: usize,
    #[serde(default)]
    pub already_applied: bool,
}

#[derive(Clone, Serialize, Deserialize)]
struct UpgradeCertificate {
    version: u32,
    reader_capability: String,
    migration_id: String,
    operator_directory: std::path::PathBuf,
    plan_digest: String,
    catalog_digest: String,
    applied: PartitionUpgradeApplied,
}

fn plan_digest(plan: &PartitionUpgradePlan) -> Result<String> {
    let mut hasher = Sha256::new();
    hash_item(&mut hasher, "sql_partition_upgrade_plan_v1")?;
    hash_item(&mut hasher, plan)?;
    Ok(hex::encode(hasher.finalize()))
}

fn read_certificate(db: &BicDb) -> Result<Option<UpgradeCertificate>> {
    let raw = match db.get(CERTIFICATE_COLLECTION, "current") {
        Ok(raw) => raw,
        Err(BicDbError::CollectionNotFound(_)) => None,
        Err(error) => return Err(error.into()),
    };
    let Some(raw) = raw else { return Ok(None) };
    let certificate: UpgradeCertificate = serde_json::from_value(raw.metadata.clone())
        .map_err(|_| required("malformed or unsupported completion certificate"))?;
    if certificate.version != 1
        || certificate.reader_capability != "sql_partition_execution_v1"
        || certificate.migration_id.is_empty()
        || certificate.plan_digest.len() != 64
        || certificate.catalog_digest.len() != 64
        || !db.sql_partition_execution_feature_active()
    {
        return Err(required("unknown or unfenced completion certificate"));
    }
    Ok(Some(certificate))
}

/// Refuse legacy or incomplete partition state without modifying the store.
/// Hosts with a writable exclusive handle can call ensure_sql_partition_admission.
pub fn check_sql_partition_admission(db: &BicDb) -> Result<()> {
    if catalog::needs_repair(db)? {
        return Err(required("exclusive upgrade is required before SQL access; inspect_partition_upgrade reports required operator decisions"));
    }
    Ok(())
}

fn activate_empty(db: &mut BicDb) -> Result<()> {
    // Validate the complete resulting set, including stationary ACKs, against
    // the actual proposed v1 descriptors before even the sticky reader fence.
    // Existing index contents are not proof that historical rows are valid.
    preflight::validate(db, &snapshot, &cached_schemas, &relocations)?;
    db.enable_sql_partition_execution_v1()?;
    db.create_collection(CERTIFICATE_COLLECTION)?;
    let snapshot = "fresh_empty_partition_activation_v1".to_string();
    let plan = PartitionUpgradePlan {
        snapshot: snapshot.clone(),
        ..Default::default()
    };
    let certificate = UpgradeCertificate {
        version: 1,
        reader_capability: "sql_partition_execution_v1".into(),
        migration_id: uuid::Uuid::new_v4().to_string(),
        operator_directory: db.data_path().canonicalize().map_err(BicDbError::from)?,
        plan_digest: plan_digest(&plan)?,
        catalog_digest: catalog::empty_digest()?,
        applied: PartitionUpgradeApplied {
            source_snapshot: snapshot,
            moved_rows: 0,
            policy_decisions: 0,
            created_trigger_clones: 0,
            already_applied: false,
        },
    };
    db.insert(
        CERTIFICATE_COLLECTION,
        Record::new("current").with_metadata(serde_json::to_value(certificate)?),
    )?;
    Ok(())
}

/// Default exclusive admission, also used by hosts before opening a listener.
/// Ambiguous policies or invalid ACKs require the explicit reviewed plan API.
pub fn ensure_sql_partition_admission(db: &mut BicDb) -> Result<()> {
    if !catalog::needs_repair(db)? {
        return Ok(());
    }
    if db.is_read_only_standby() {
        return Err(required(
            "partition upgrade must complete on a writable primary before standby admission",
        ));
    }
    let snapshot = gather(db)?;
    if snapshot.nodes.is_empty() {
        activate_empty(db)?;
    } else {
        apply_partition_upgrade(db, None)?;
    }
    check_sql_partition_admission(db)
}

/// Prepare a newly authored descriptor only after the sticky reader fence and
/// empty/current certificate exist. Existing raw legacy descriptors are repaired
/// by admission, never adopted by this helper.
pub(crate) fn prepare_partition_descriptor(db: &mut BicDb, schema: &mut TableSchema) -> Result<()> {
    if schema.partitioning.is_none() && schema.partition_of.is_none() {
        return Ok(());
    }
    partition::validate_definition(schema)?;
    ensure_sql_partition_admission(db)?;
    if !db.sql_partition_execution_feature_active() {
        activate_empty(db)?;
    }
    for version in schema
        .partitioning
        .iter_mut()
        .map(|p| &mut p.execution_version)
        .chain(
            schema
                .partition_of
                .iter_mut()
                .map(|p| &mut p.execution_version),
        )
    {
        if version.is_some_and(|version| version != 1) {
            return Err(required("unknown partition descriptor version"));
        }
        *version = Some(1);
    }
    Ok(())
}

pub(crate) fn certificate_for_catalog_changes(
    db: &BicDb,
    changes: &[(&'static str, String, Option<Record>)],
) -> Result<Option<Record>> {
    catalog::certificate_for_changes(db, changes)
}

pub(crate) fn publish_schema_change(db: &mut BicDb, id: String, raw: Option<Record>) -> Result<()> {
    publish_catalog_changes(db, vec![(SCHEMA_COLLECTION, id, raw)])
}

pub(crate) fn publish_catalog_changes(
    db: &mut BicDb,
    mut changes: Vec<(&'static str, String, Option<Record>)>,
) -> Result<()> {
    if let Some(certificate) = certificate_for_catalog_changes(db, &changes)? {
        changes.push((CERTIFICATE_COLLECTION, "current".into(), Some(certificate)));
    }
    let mut tx = db.begin_transaction()?;
    for (collection, id, raw) in changes {
        if let Some(raw) = raw {
            tx.batch_insert(collection, vec![raw])?;
        } else {
            tx.delete(collection, &id)?;
        }
    }
    tx.commit()?;
    Ok(())
}

impl SqlSession<'_> {
    pub(super) fn admit_partition_execution(&mut self) -> Result<()> {
        if catalog::needs_repair(self.db_ref())? {
            if self.tx.is_some() {
                return Err(required("partition upgrade requires rollback of the pending transaction and exclusive admission"));
            }
            ensure_sql_partition_admission(self.db_mut()?)?;
        }
        Ok(())
    }
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
    if let Some(certificate) = read_certificate(db)? {
        if !snapshot.inspection.legacy_relations.is_empty()
            || !snapshot.inspection.invalid_rows.is_empty()
        {
            return Err(required(
                "completed catalog contains uncertified partition state",
            ));
        }
        check_sql_partition_admission(db)?;
        if let Some(plan) = plan {
            if certificate.operator_directory != snapshot.inspection.database_root
                || plan.snapshot != certificate.applied.source_snapshot
                || plan_digest(plan)? != certificate.plan_digest
            {
                return Err(required(
                    "retry does not match the completed local operator plan",
                ));
            }
        }
        let mut applied = certificate.applied;
        applied.already_applied = true;
        return Ok(applied);
    }
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
        already_applied: false,
    };
    let automatic_plan = PartitionUpgradePlan {
        snapshot: snapshot.inspection.snapshot.clone(),
        ..Default::default()
    };
    let certificate =
        Record::new("current").with_metadata(serde_json::to_value(UpgradeCertificate {
            version: 1,
            reader_capability: "sql_partition_execution_v1".into(),
            migration_id,
            operator_directory: snapshot.inspection.database_root.clone(),
            plan_digest: plan_digest(plan.unwrap_or(&automatic_plan))?,
            catalog_digest: {
                let nodes = schema_records
                    .iter()
                    .map(|raw| (raw.id.clone(), raw.clone()))
                    .collect();
                let mut triggers = snapshot
                    .triggers
                    .iter()
                    .map(|raw| (raw.id.clone(), raw.clone()))
                    .collect::<BTreeMap<_, _>>();
                for raw in &trigger_records {
                    triggers.insert(raw.id.clone(), raw.clone());
                }
                catalog::digest(&nodes, &triggers)?
            },
            applied: applied.clone(),
        })?);
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
    fn legacy_fixture(path: &std::path::Path, mode: bicdb_core::StorageMode) -> BicDb {
        let mut db = BicDb::open_with_config(path, config(mode)).unwrap();
        let mut session = SqlSession::new(&mut db);
        for statement in [
            "CREATE TABLE p(k INT PRIMARY KEY,payload TEXT)",
            "CREATE TABLE low(k INT PRIMARY KEY,payload TEXT)",
            "CREATE TABLE high(k INT PRIMARY KEY,payload TEXT)",
            "INSERT INTO low VALUES(2,'low-ack')",
            "INSERT INTO high VALUES(12,'high-ack')",
        ] {
            session.execute(statement).unwrap();
        }
        drop(session);
        // Model old raw descriptors without invoking the new fresh-DDL
        // activation. Genuine old-binary fixtures remain separate evidence.
        for (name, field, value) in [
            (
                "p",
                "partitioning",
                serde_json::json!({"strategy":"range","key_columns":["k"]}),
            ),
            (
                "low",
                "partition_of",
                serde_json::json!({"parent_table":"p","parent_schema":"public","bound":"FOR VALUES FROM (0) TO (10)"}),
            ),
            (
                "high",
                "partition_of",
                serde_json::json!({"parent_table":"p","parent_schema":"public","bound":"FOR VALUES FROM (10) TO (20)"}),
            ),
        ] {
            let mut raw = (*db.get(SCHEMA_COLLECTION, name).unwrap().unwrap()).clone();
            raw.metadata[field] = value;
            db.insert(SCHEMA_COLLECTION, raw.clone()).unwrap();
            cache_saved_schema(&db, &serde_json::from_value(raw.metadata).unwrap());
        }
        db
    }
    fn config(mode: bicdb_core::StorageMode) -> bicdb_core::DbConfig {
        bicdb_core::DbConfig::default()
            .with_storage_mode(mode)
            .with_fsync(true)
            .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
    }
    fn plan(db: &BicDb) -> PartitionUpgradePlan {
        PartitionUpgradePlan {
            snapshot: inspect_partition_upgrade(db).unwrap().snapshot,
            ..Default::default()
        }
    }

    fn store_legacy_schema(db: &mut BicDb, schema: TableSchema) {
        db.insert(
            SCHEMA_COLLECTION,
            Record::new(schema.name.clone()).with_metadata(serde_json::to_value(&schema).unwrap()),
        )
        .unwrap();
        cache_saved_schema(db, &schema);
    }

    fn physical_snapshot(db: &BicDb) -> JsonValue {
        let mut rows = BTreeMap::new();
        for name in [SCHEMA_COLLECTION, "p", "low", "high"] {
            let mut records = db.scan_collection(name).unwrap();
            records.sort_by(|a, b| a.id.cmp(&b.id));
            rows.insert(name, records);
        }
        serde_json::to_value(rows).unwrap()
    }

    fn assert_upgrade_refuses_without_publication(db: &mut BicDb) {
        let before = physical_snapshot(db);
        let sequence = db.last_commit_seq();
        let error = apply_partition_upgrade(db, None).unwrap_err();
        assert_eq!(error.sqlstate(), "55000", "{error}");
        assert!(!db.sql_partition_execution_feature_active());
        assert_eq!(db.last_commit_seq(), sequence);
        assert_eq!(physical_snapshot(db), before);
        assert!(!db.has_collection(CERTIFICATE_COLLECTION));
    }

    #[test]
    fn stationary_leaf_check_is_validated_before_any_upgrade_publication() {
        for mode in [
            bicdb_core::StorageMode::EmbeddedMemory,
            bicdb_core::StorageMode::ServerPaged,
        ] {
            let root = tempfile::tempdir().unwrap();
            let mut db = legacy_fixture(root.path(), mode.clone());
            let mut schema: TableSchema = serde_json::from_value(
                db.get(SCHEMA_COLLECTION, "low")
                    .unwrap()
                    .unwrap()
                    .metadata
                    .clone(),
            )
            .unwrap();
            schema.constraints.push(ConstraintSchema::Check {
                name: "low_payload_check".into(),
                expression: "payload <> 'low-ack'".into(),
                validated: true,
            });
            store_legacy_schema(&mut db, schema);
            assert_upgrade_refuses_without_publication(&mut db);
            db.close().unwrap();
            for _ in 0..2 {
                let mut db = BicDb::open_with_config(root.path(), config(mode.clone())).unwrap();
                assert_upgrade_refuses_without_publication(&mut db);
                db.close().unwrap();
            }
        }
    }

    #[test]
    fn stationary_leaf_not_null_is_validated_before_any_upgrade_publication() {
        for mode in [
            bicdb_core::StorageMode::EmbeddedMemory,
            bicdb_core::StorageMode::ServerPaged,
        ] {
            let root = tempfile::tempdir().unwrap();
            let mut db = legacy_fixture(root.path(), mode.clone());
            let mut schema: TableSchema = serde_json::from_value(
                db.get(SCHEMA_COLLECTION, "low")
                    .unwrap()
                    .unwrap()
                    .metadata
                    .clone(),
            )
            .unwrap();
            schema
                .columns
                .iter_mut()
                .find(|column| column.name == "payload")
                .unwrap()
                .nullable = false;
            store_legacy_schema(&mut db, schema);
            let mut row = db.scan_collection("low").unwrap().remove(0);
            row.metadata["payload"] = JsonValue::Null;
            db.insert("low", row).unwrap();
            assert_upgrade_refuses_without_publication(&mut db);
            db.close().unwrap();
        }
    }

    #[test]
    fn stationary_leaf_unique_is_validated_before_any_upgrade_publication() {
        for mode in [
            bicdb_core::StorageMode::EmbeddedMemory,
            bicdb_core::StorageMode::ServerPaged,
        ] {
            let root = tempfile::tempdir().unwrap();
            let mut db = legacy_fixture(root.path(), mode.clone());
            let mut schema: TableSchema = serde_json::from_value(
                db.get(SCHEMA_COLLECTION, "low")
                    .unwrap()
                    .unwrap()
                    .metadata
                    .clone(),
            )
            .unwrap();
            schema.constraints.push(ConstraintSchema::Unique {
                name: "low_payload_key".into(),
                columns: vec!["payload".into()],
                validated: true,
            });
            store_legacy_schema(&mut db, schema);
            db.insert(
                "low",
                Record::new("3").with_metadata(serde_json::json!({"k":3,"payload":"low-ack"})),
            )
            .unwrap();
            assert_upgrade_refuses_without_publication(&mut db);
            db.close().unwrap();
        }
    }

    #[test]
    fn stationary_leaf_foreign_key_is_validated_before_any_upgrade_publication() {
        for mode in [
            bicdb_core::StorageMode::EmbeddedMemory,
            bicdb_core::StorageMode::ServerPaged,
        ] {
            let root = tempfile::tempdir().unwrap();
            let mut db = legacy_fixture(root.path(), mode.clone());
            let mut schema: TableSchema = serde_json::from_value(
                db.get(SCHEMA_COLLECTION, "low")
                    .unwrap()
                    .unwrap()
                    .metadata
                    .clone(),
            )
            .unwrap();
            schema.constraints.push(ConstraintSchema::ForeignKey {
                name: "low_k_fkey".into(),
                constraint_id: "legacy-low-fk".into(),
                columns: vec!["k".into()],
                foreign_table: "high".into(),
                referred_columns: vec!["k".into()],
                on_delete: ForeignKeyAction::NoAction,
                on_update: ForeignKeyAction::NoAction,
                validated: true,
                deferrable: true,
                initially_deferred: true,
            });
            store_legacy_schema(&mut db, schema);
            assert_upgrade_refuses_without_publication(&mut db);
            db.close().unwrap();
        }
    }

    #[test]
    fn stationary_leaf_native_identity_must_match_its_declared_primary_key() {
        for mode in [
            bicdb_core::StorageMode::EmbeddedMemory,
            bicdb_core::StorageMode::ServerPaged,
        ] {
            let root = tempfile::tempdir().unwrap();
            let mut db = legacy_fixture(root.path(), mode.clone());
            let mut row = db.scan_collection("low").unwrap().remove(0);
            db.delete("low", &row.id).unwrap();
            row.id = "shadow-native-key".into();
            row.metadata["k"] = serde_json::json!(2);
            db.insert("low", row).unwrap();
            assert_upgrade_refuses_without_publication(&mut db);
            db.close().unwrap();
        }
    }

    #[test]
    fn interrupted_activation_and_completed_retry_preserve_acks_and_markers() {
        for mode in [
            bicdb_core::StorageMode::EmbeddedMemory,
            bicdb_core::StorageMode::ServerPaged,
        ] {
            let root = tempfile::tempdir().unwrap();
            let mut db = legacy_fixture(root.path(), mode.clone());
            let plan = plan(&db);
            db.enable_sql_partition_execution_v1().unwrap();
            db.create_collection(CERTIFICATE_COLLECTION).unwrap();
            db.create_collection(POLICY_ARCHIVE_COLLECTION).unwrap();
            assert_eq!(
                inspect_partition_upgrade(&db).unwrap().snapshot,
                plan.snapshot
            );
            db.close().unwrap();
            let mut db = BicDb::open_with_config(root.path(), config(mode.clone())).unwrap();
            let applied = apply_partition_upgrade(&mut db, Some(&plan)).unwrap();
            assert!(!applied.already_applied);
            assert_eq!(applied.moved_rows, 0);
            for relation in ["p", "low", "high"] {
                // Generic saves must retain the marker; otherwise the next
                // reopen would reinterpret a completed relation as legacy.
                let schema = load_schema(&db, relation).unwrap().unwrap();
                save_schema(&mut db, &schema).unwrap();
                let raw = db.get(SCHEMA_COLLECTION, relation).unwrap().unwrap();
                let field = if relation == "p" {
                    "partitioning"
                } else {
                    "partition_of"
                };
                assert_eq!(
                    raw.metadata[field]["execution_version"],
                    serde_json::json!(1)
                );
            }
            db.close().unwrap();
            for _ in 0..2 {
                let mut db = BicDb::open_with_config(root.path(), config(mode.clone())).unwrap();
                let sequence = db.last_commit_seq();
                let certificate = db.get(CERTIFICATE_COLLECTION, "current").unwrap().unwrap();
                let repeated = apply_partition_upgrade(&mut db, Some(&plan)).unwrap();
                assert!(repeated.already_applied);
                assert_eq!(repeated.source_snapshot, applied.source_snapshot);
                assert_eq!(
                    db.last_commit_seq(),
                    sequence,
                    "receipt retry cannot publish again"
                );
                assert_eq!(
                    db.get(CERTIFICATE_COLLECTION, "current")
                        .unwrap()
                        .unwrap()
                        .metadata,
                    certificate.metadata
                );
                assert_eq!(
                    db.scan_collection("low").unwrap().len()
                        + db.scan_collection("high").unwrap().len(),
                    2
                );
                assert!(inspect_partition_upgrade(&db)
                    .unwrap()
                    .legacy_relations
                    .is_empty());
                let mut changed_plan = plan.clone();
                changed_plan
                    .policies
                    .insert("high".into(), PartitionPolicyDecision::KeepStored);
                assert_eq!(
                    apply_partition_upgrade(&mut db, Some(&changed_plan))
                        .unwrap_err()
                        .sqlstate(),
                    "55000"
                );
                assert_eq!(db.last_commit_seq(), sequence);
                db.close().unwrap();
            }
        }
    }

    #[test]
    fn stale_and_wrong_directory_plans_refuse_before_the_reader_fence() {
        for mode in [
            bicdb_core::StorageMode::EmbeddedMemory,
            bicdb_core::StorageMode::ServerPaged,
        ] {
            let first = tempfile::tempdir().unwrap();
            let second = tempfile::tempdir().unwrap();
            let mut db = legacy_fixture(first.path(), mode.clone());
            let plan = plan(&db);
            let mut other = legacy_fixture(second.path(), mode);
            let sequence = other.last_commit_seq();
            assert_eq!(
                apply_partition_upgrade(&mut other, Some(&plan))
                    .unwrap_err()
                    .sqlstate(),
                "55000"
            );
            assert!(!other.sql_partition_execution_feature_active());
            assert_eq!(other.last_commit_seq(), sequence);
            other.close().unwrap();
            // An external Native writer changes the reviewed legacy input.
            // SQL now performs admission before it could execute this write.
            let mut changed = db.scan_collection("low").unwrap().remove(0);
            changed.metadata["payload"] = serde_json::json!("new-ack");
            db.insert("low", changed).unwrap();
            let sequence = db.last_commit_seq();
            assert_eq!(
                apply_partition_upgrade(&mut db, Some(&plan))
                    .unwrap_err()
                    .sqlstate(),
                "55000"
            );
            assert!(!db.sql_partition_execution_feature_active());
            assert_eq!(db.last_commit_seq(), sequence);
            let rows = db.scan_collection("low").unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].metadata["payload"], serde_json::json!("new-ack"));
            db.close().unwrap();
        }
    }

    #[test]
    fn unknown_or_unfenced_certificates_cannot_be_overwritten_or_adopted() {
        for mode in [
            bicdb_core::StorageMode::EmbeddedMemory,
            bicdb_core::StorageMode::ServerPaged,
        ] {
            for unknown in [false, true] {
                let root = tempfile::tempdir().unwrap();
                let mut db = legacy_fixture(root.path(), mode.clone());
                let plan = plan(&db);
                db.create_collection(CERTIFICATE_COLLECTION).unwrap();
                let record = Record::new("current").with_metadata(serde_json::json!({
                    "version":if unknown { 2 } else { 1 },
                    "reader_capability":"sql_partition_execution_v1",
                    "migration_id":"not-authorized-by-a-local-reader-fence",
                    "operator_directory":root.path(),
                    "plan_digest":plan_digest(&plan).unwrap(),
                    "applied":{"source_snapshot":plan.snapshot,"moved_rows":0,"policy_decisions":0,"created_trigger_clones":0}
                }));
                db.insert(CERTIFICATE_COLLECTION, record.clone()).unwrap();
                let sequence = db.last_commit_seq();
                assert_eq!(
                    apply_partition_upgrade(&mut db, Some(&plan))
                        .unwrap_err()
                        .sqlstate(),
                    "55000"
                );
                assert!(!db.sql_partition_execution_feature_active());
                assert_eq!(db.last_commit_seq(), sequence);
                assert_eq!(
                    db.get(CERTIFICATE_COLLECTION, "current")
                        .unwrap()
                        .unwrap()
                        .metadata,
                    record.metadata
                );
                db.close().unwrap();
            }
        }
    }

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
                let (ordinary, partitioning) = ddl.split_once(" PARTITION BY RANGE(").unwrap();
                SqlSession::new(&mut db).execute(ordinary).unwrap();
                let mut raw = (*db.get(SCHEMA_COLLECTION, "p").unwrap().unwrap()).clone();
                raw.metadata["partitioning"] = serde_json::json!({"strategy":"range","key_columns":[partitioning.trim_end_matches(')')]});
                db.insert(SCHEMA_COLLECTION, raw).unwrap();
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
