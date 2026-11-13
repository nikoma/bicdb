//! Local semantic admission memo. No portable origin or Native permission is
//! inferred from this cache; physical reads/writes retain their own gates.
use super::*;
use bicdb_core::CollectionCacheStamp;
use std::sync::{Mutex as CacheMutex, OnceLock};

type Stamps = [Option<CollectionCacheStamp>; 3];
const MAX_CACHE_BYTES: usize = 64 * 1024 * 1024;
const MAX_ENTRIES: usize = 128;

#[derive(Clone)]
struct View {
    stamps: Stamps,
    capability: bool,
    nodes: BTreeMap<String, Record>,
    triggers: BTreeMap<String, Record>,
    certificate: Option<UpgradeCertificate>,
    bytes: usize,
    digest: String,
    needs_repair: bool,
}
static CACHE: OnceLock<CacheMutex<BTreeMap<u64, Arc<View>>>> = OnceLock::new();
fn cache() -> &'static CacheMutex<BTreeMap<u64, Arc<View>>> {
    CACHE.get_or_init(|| CacheMutex::new(BTreeMap::new()))
}
fn stamps(db: &BicDb) -> Result<Stamps> {
    Ok([
        db.collection_cache_stamp(SCHEMA_COLLECTION)?,
        db.collection_cache_stamp(TRIGGER_COLLECTION)?,
        db.collection_cache_stamp(CERTIFICATE_COLLECTION)?,
    ])
}
fn retain(db: &BicDb, view: Arc<View>) {
    let mut cache = cache().lock().unwrap_or_else(|error| error.into_inner());
    cache.remove(&db.instance_id());
    while cache.len() >= MAX_ENTRIES
        || cache
            .values()
            .map(|entry| entry.bytes)
            .sum::<usize>()
            .saturating_add(view.bytes)
            > MAX_CACHE_BYTES
    {
        let Some(first) = cache.keys().next().copied() else {
            break;
        };
        cache.remove(&first);
    }
    if view.bytes <= MAX_CACHE_BYTES {
        cache.insert(db.instance_id(), view);
    }
}

fn descriptor(raw: &Record) -> Result<Option<TableSchema>> {
    if !affected(raw) {
        return Ok(None);
    }
    for field in ["partitioning", "partition_of"] {
        if let Some(value) = raw.metadata.get(field).filter(|value| !value.is_null()) {
            match value.get("execution_version") {
                None | Some(JsonValue::Null) => {}
                Some(value) if value.as_u64() == Some(1) => {}
                _ => return Err(required("unknown or malformed partition execution version")),
            }
        }
    }
    let schema: TableSchema = serde_json::from_value(raw.metadata.clone())
        .map_err(|_| required("malformed partition descriptor"))?;
    if schema.name != raw.id {
        return Err(required("partition descriptor key/identity mismatch"));
    }
    Ok(Some(schema))
}
fn charge(raw: &Record) -> Result<usize> {
    Ok(serde_json::to_vec(raw)?
        .len()
        .saturating_mul(4)
        .saturating_add(raw.id.len().saturating_mul(4))
        .saturating_add(512))
}
fn put(
    db: &BicDb,
    rows: &mut BTreeMap<String, Record>,
    raw: Record,
    bytes: &mut usize,
) -> Result<()> {
    if let Some(previous) = rows.get(&raw.id) {
        *bytes = bytes.saturating_sub(charge(previous)?);
    }
    *bytes = bytes.saturating_add(charge(&raw)?);
    if *bytes > db.query_work_memory_bytes().min(MAX_CACHE_BYTES) {
        return Err(required(
            "partition admission metadata exceeds configured work memory",
        ));
    }
    rows.insert(raw.id.clone(), raw);
    Ok(())
}
fn legacy(schema: &TableSchema) -> bool {
    schema
        .partitioning
        .as_ref()
        .is_some_and(|p| p.execution_version.is_none())
        || schema
            .partition_of
            .as_ref()
            .is_some_and(|p| p.execution_version.is_none())
}

pub(super) fn digest(
    nodes: &BTreeMap<String, Record>,
    triggers: &BTreeMap<String, Record>,
) -> Result<String> {
    let mut hasher = Sha256::new();
    hash_item(&mut hasher, "sql_partition_catalog_v1")?;
    hash_item(&mut hasher, nodes)?;
    hash_item(&mut hasher, triggers)?;
    Ok(hex::encode(hasher.finalize()))
}

fn view(db: &BicDb) -> Result<Arc<View>> {
    db.check_recovery_state()?;
    let initial = stamps(db)?;
    let prior = cache()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(&db.instance_id())
        .cloned();
    if let Some(prior) = &prior {
        if prior.stamps == initial
            && prior.capability == db.sql_partition_execution_feature_active()
        {
            return Ok(Arc::clone(prior));
        }
    }
    let mut nodes = prior
        .as_ref()
        .map_or_else(BTreeMap::new, |prior| prior.nodes.clone());
    let mut bytes = nodes.values().try_fold(1024usize, |bytes, raw| {
        Ok::<_, SqlError>(bytes.saturating_add(charge(raw)?))
    })?;
    let changes = match prior.as_ref().and_then(|prior| prior.stamps[0].as_ref()) {
        Some(previous) => db.collection_cache_changes_since(SCHEMA_COLLECTION, previous)?,
        None => None,
    };
    if let Some(changes) = changes {
        for id in changes
            .into_iter()
            .map(|change| change.record_id)
            .collect::<BTreeSet<_>>()
        {
            match db.get(SCHEMA_COLLECTION, &id)? {
                Some(raw) if descriptor(&raw)?.is_some() => {
                    put(db, &mut nodes, (*raw).clone(), &mut bytes)?;
                }
                _ => {
                    if let Some(old) = nodes.remove(&id) {
                        bytes = bytes.saturating_sub(charge(&old)?);
                    }
                }
            }
        }
    } else {
        nodes.clear();
        bytes = 1024;
        if db.has_collection(SCHEMA_COLLECTION) {
            scan(db, SCHEMA_COLLECTION, |raw| {
                if descriptor(&raw)?.is_some() {
                    put(db, &mut nodes, raw, &mut bytes)?;
                }
                Ok(())
            })?;
        }
    }
    let mut triggers = BTreeMap::new();
    if !nodes.is_empty() && db.has_collection(TRIGGER_COLLECTION) {
        scan(db, TRIGGER_COLLECTION, |raw| {
            if raw
                .metadata
                .get("table_name")
                .and_then(JsonValue::as_str)
                .is_some_and(|name| nodes.contains_key(name))
            {
                let trigger: TriggerSchema = serde_json::from_value(raw.metadata.clone())
                    .map_err(|_| required("malformed partition trigger"))?;
                if raw.id != trigger_key(&trigger.table_name, &trigger.name) {
                    return Err(required("partition trigger key/identity mismatch"));
                }
                put(db, &mut triggers, raw, &mut bytes)?;
            }
            Ok(())
        })?;
    }
    let certificate = read_certificate(db)?;
    if bytes > db.query_work_memory_bytes().min(MAX_CACHE_BYTES) {
        return Err(required(
            "partition admission metadata exceeds configured work memory",
        ));
    }
    if stamps(db)? != initial {
        return Err(required("catalog changed during admission; retry"));
    }
    let digest = digest(&nodes, &triggers)?;
    let mut view = View {
        stamps: initial,
        capability: db.sql_partition_execution_feature_active(),
        nodes,
        triggers,
        certificate,
        bytes,
        digest,
        needs_repair: false,
    };
    view.needs_repair = validate(db, &view)?;
    let view = Arc::new(view);
    retain(db, Arc::clone(&view));
    Ok(view)
}

fn validate(db: &BicDb, view: &View) -> Result<bool> {
    let schemas = view
        .nodes
        .values()
        .map(|raw| descriptor(raw).map(Option::unwrap))
        .collect::<Result<Vec<_>>>()?;
    let has_legacy = schemas.iter().any(legacy);
    let has_current = schemas.iter().any(|schema| {
        schema
            .partitioning
            .as_ref()
            .is_some_and(|p| p.execution_version.is_some())
            || schema
                .partition_of
                .as_ref()
                .is_some_and(|p| p.execution_version.is_some())
    });
    if has_legacy {
        if view.certificate.is_some() || has_current {
            return Err(required(
                "mixed or imported uncertified partition state requires explicit recovery",
            ));
        }
        return Ok(true);
    }
    if let Some(certificate) = &view.certificate {
        if certificate.catalog_digest != view.digest {
            return Err(required(
                "partition catalog does not match its atomic completion certificate",
            ));
        }
        partition::validate_existing_tree(db, &schemas)?;
        let nodes = schemas
            .iter()
            .map(|schema| (schema.name.as_str(), schema))
            .collect::<BTreeMap<_, _>>();
        for schema in &schemas {
            let mut cursor = schema;
            let mut seen = BTreeSet::new();
            while let Some(link) = &cursor.partition_of {
                if !seen.insert(cursor.name.clone()) {
                    return Err(required("cyclic certified partition tree"));
                }
                let parent = nodes
                    .get(link.parent_table.as_str())
                    .ok_or_else(|| required("missing certified partition parent"))?;
                if parent.schema_name != link.parent_schema || parent.partitioning.is_none() {
                    return Err(required(
                        "certified partition parent namespace/type mismatch",
                    ));
                }
                cursor = parent;
            }
        }
        return Ok(false);
    }
    if !view.nodes.is_empty() {
        return Err(required(
            "versioned partition descriptor has no completion certificate",
        ));
    }
    Ok(db.sql_partition_execution_feature_active())
}

pub(super) fn needs_repair(db: &BicDb) -> Result<bool> {
    Ok(view(db)?.needs_repair)
}

/// Overlay a complete catalog publication and bind its exact affected records
/// in the very same Native transaction. Ordinary table changes do not rewrite
/// the certificate or scan ordinary business data.
pub(super) fn certificate_for_changes(
    db: &BicDb,
    changes: &[(&'static str, String, Option<Record>)],
) -> Result<Option<Record>> {
    let view = view(db)?;
    if view.needs_repair {
        return Err(required(
            "partition catalog needs upgrade before DDL publication",
        ));
    }
    let mut nodes = view.nodes.clone();
    let mut triggers = view.triggers.clone();
    let mut bytes = view.bytes;
    for (collection, id, raw) in changes {
        if *collection == SCHEMA_COLLECTION {
            match raw {
                Some(raw) if descriptor(raw)?.is_some() => {
                    let schema = descriptor(raw)?.unwrap();
                    if legacy(&schema) {
                        return Err(required(
                            "new partition publication lacks execution version",
                        ));
                    }
                    partition::validate_definition(&schema)?;
                    put(db, &mut nodes, raw.clone(), &mut bytes)?;
                }
                _ => {
                    if let Some(old) = nodes.remove(id) {
                        bytes = bytes.saturating_sub(charge(&old)?);
                    }
                }
            }
        }
    }
    // An attachment can expose an already-authored local trigger that was not
    // relevant to the prior forest; include it rather than adopting its lineage.
    if nodes.keys().any(|name| !view.nodes.contains_key(name))
        && db.has_collection(TRIGGER_COLLECTION)
    {
        scan(db, TRIGGER_COLLECTION, |raw| {
            if raw
                .metadata
                .get("table_name")
                .and_then(JsonValue::as_str)
                .is_some_and(|name| nodes.contains_key(name))
            {
                put(db, &mut triggers, raw, &mut bytes)?;
            }
            Ok(())
        })?;
    }
    for (collection, id, raw) in changes {
        if *collection == TRIGGER_COLLECTION {
            match raw {
                Some(raw) => {
                    put(db, &mut triggers, raw.clone(), &mut bytes)?;
                }
                None => {
                    if let Some(old) = triggers.remove(id) {
                        bytes = bytes.saturating_sub(charge(&old)?);
                    }
                }
            }
        }
    }
    triggers.retain(|_, raw| {
        raw.metadata
            .get("table_name")
            .and_then(JsonValue::as_str)
            .is_some_and(|name| nodes.contains_key(name))
    });
    let proposed = digest(&nodes, &triggers)?;
    if proposed == view.digest {
        return Ok(None);
    }
    let mut certificate = view
        .certificate
        .clone()
        .ok_or_else(|| required("partition publication has no activated certificate"))?;
    certificate.catalog_digest = proposed;
    // A certificate describes the complete proposed tree. Never attest a
    // parent/child column mismatch merely because each descriptor parses.
    validate(
        db,
        &View {
            stamps: view.stamps.clone(),
            capability: view.capability,
            nodes,
            triggers,
            certificate: Some(certificate.clone()),
            bytes,
            digest: certificate.catalog_digest.clone(),
            needs_repair: false,
        },
    )?;
    Ok(Some(
        Record::new("current").with_metadata(serde_json::to_value(certificate)?),
    ))
}

pub(super) fn empty_digest() -> Result<String> {
    digest(&BTreeMap::new(), &BTreeMap::new())
}
