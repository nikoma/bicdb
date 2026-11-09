//! Startup-only migration of legacy ASCII case-expression indexes.
//! A durable ownership record precedes each shadow, and readiness follows every
//! authoritative catalog publication. SQL remains fenced until startup finishes.
use super::*;
use bicdb_core::{
    IndexPredicate, PagedBTreeBuildAdvance, PagedBTreeBuildLimits, ResourceDemand,
    ResourceGovernor, ResourceGovernorConfig,
};

const PREFIX: &str = "oracle_case_migration_v1:";
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Intent {
    format: u32,
    old: IndexDefinition,
    shadow: IndexDefinition,
}

fn national(schema: Option<&TableSchema>, field: &IndexField) -> bool {
    match field {
        IndexField::MetadataPath(path) if path.len() == 1 => schema.is_some_and(|schema| {
            schema.columns.iter().any(|column| {
                column.name == path[0]
                    && matches!(
                        column.type_modifier,
                        Some(PgTypeModifier::OracleNationalColumnV1 { .. })
                    )
            })
        }),
        IndexField::OracleUpperNationalV1(_) | IndexField::OracleLowerNationalV1(_) => true,
        IndexField::Lower(inner)
        | IndexField::Upper(inner)
        | IndexField::Trim(inner)
        | IndexField::Coalesce { field: inner, .. } => national(schema, inner),
        IndexField::FirstNonNull(fields) => fields.iter().any(|field| national(schema, field)),
        _ => false,
    }
}
fn convert_field(schema: Option<&TableSchema>, field: &mut IndexField) {
    let is_national = national(schema, field);
    match field {
        IndexField::Upper(inner) => {
            convert_field(schema, inner);
            *field = if is_national {
                IndexField::OracleUpperNationalV1(inner.clone())
            } else {
                IndexField::OracleUpperV1(inner.clone())
            };
        }
        IndexField::Lower(inner) => {
            convert_field(schema, inner);
            *field = if is_national {
                IndexField::OracleLowerNationalV1(inner.clone())
            } else {
                IndexField::OracleLowerV1(inner.clone())
            };
        }
        IndexField::Trim(inner)
        | IndexField::TimestampDay(inner)
        | IndexField::Coalesce { field: inner, .. } => convert_field(schema, inner),
        IndexField::FirstNonNull(fields) => fields
            .iter_mut()
            .for_each(|field| convert_field(schema, field)),
        _ => {}
    }
}
fn convert_predicate(schema: Option<&TableSchema>, predicate: &mut IndexPredicate) {
    match predicate {
        IndexPredicate::Field { field } => convert_field(schema, field),
        IndexPredicate::Literal { .. } => {}
        IndexPredicate::Unary { value, .. } => convert_predicate(schema, value),
        IndexPredicate::Binary { left, right, .. } => {
            convert_predicate(schema, left);
            convert_predicate(schema, right);
        }
    }
}
fn replacement(db: &BicDb, old: &IndexDefinition) -> Result<IndexDefinition> {
    let schema = load_schema_raw(db, &old.collection)?;
    let mut new = old.clone();
    new.fields
        .iter_mut()
        .for_each(|field| convert_field(schema.as_ref(), field));
    if let Some(predicate) = &mut new.predicate {
        convert_predicate(schema.as_ref(), predicate);
    }
    // Oracle has no exclusion-constraint syntax. Preserve such a native index
    // rather than silently dropping constraint semantics during an upgrade.
    if new != *old && old.exclusion.is_some() {
        return Err(unsupported(
            "Oracle case migration requires an independently qualified exclusion-index rebuild",
        ));
    }
    Ok(new)
}

fn finish(db: &mut BicDb, key: &str, intent: Intent) -> Result<()> {
    let mut expected = replacement(db, &intent.old)?;
    expected.name.clone_from(&intent.shadow.name);
    if intent.format != 1
        || intent.shadow != expected
        || intent.old.name == intent.shadow.name
        || !intent.shadow.name.starts_with("__bicdb_oracle_case_")
    {
        return Err(unsupported(
            "invalid Oracle case migration ownership record",
        ));
    }
    let mut published = intent.shadow.clone();
    published.name.clone_from(&intent.old.name);
    let definitions = db.index_definitions();
    let current = definitions
        .iter()
        .find(|definition| definition.name == intent.old.name);
    if current != Some(&intent.old) && current != Some(&published) {
        return Err(unsupported("Oracle case migration source identity changed"));
    }
    let shadow = definitions
        .iter()
        .find(|definition| definition.name == intent.shadow.name);
    if shadow.is_some_and(|definition| definition != &intent.shadow) {
        return Err(unsupported("Oracle case migration shadow identity changed"));
    }
    if db.is_server_paged() {
        let limits = PagedBTreeBuildLimits::default();
        let state = match db.load_paged_btree_build(&intent.shadow.name, limits) {
            Ok(state) => {
                if state.definition != intent.shadow {
                    return Err(unsupported("Oracle case migration build identity changed"));
                }
                state
            }
            Err(BicDbError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                db.begin_paged_btree_build(intent.shadow.clone(), limits, 100)?
            }
            Err(error) => return Err(error.into()),
        };
        let state = if state.phase == bicdb_core::PagedBTreeBuildPhase::Complete {
            state
        } else {
            let started = state.updated_at_ms;
            let governor = ResourceGovernor::new(ResourceGovernorConfig::default(), started)?;
            let demand = ResourceDemand {
                memory_bytes: limits.max_batch_bytes as u64,
                io_bytes: limits.max_batch_bytes as u64,
                cpu_slots: 1,
                io_charge_bytes: 1,
            };
            let mut now = started
                .checked_add(1)
                .ok_or_else(|| unsupported("Oracle case migration clock exhausted"))?;
            loop {
                if let PagedBTreeBuildAdvance::Complete(state) = db
                    .advance_paged_btree_build_governed(
                        &intent.shadow.name,
                        limits,
                        &governor,
                        demand,
                        now,
                    )?
                {
                    break state;
                }
                now = now
                    .checked_add(1)
                    .ok_or_else(|| unsupported("Oracle case migration clock exhausted"))?;
            }
        };
        db.publish_oracle_case_v1_index_replacement(&intent.old, &state)?;
    } else {
        if current == Some(&intent.old) && shadow.is_none() {
            db.create_index(intent.shadow.clone())?;
        }
        db.publish_embedded_oracle_case_v1_index_replacement(&intent.old, &intent.shadow)?;
    }
    db.check_recovery_state()?;
    db.delete(PROFILE_COLLECTION, key)?;
    Ok(())
}

pub(super) fn ensure(db: &mut BicDb) -> Result<()> {
    if !is_oracle_database(db)? {
        return Ok(());
    }
    db.check_recovery_state()?;
    let marker = db.get(PROFILE_COLLECTION, simple_case::READY_ID)?;
    if marker
        .as_ref()
        .is_some_and(|record| record.metadata != simple_case::ready_metadata())
    {
        return Err(unsupported("invalid Oracle case readiness marker"));
    }
    db.enable_oracle_simple_case_v1()?;
    // Finish owned work before discovering new migrations. Publication retries
    // must retain the original source and exact physical build identity.
    for record in db.scan_collection(PROFILE_COLLECTION)? {
        if record.id.starts_with(PREFIX) {
            finish(
                db,
                &record.id,
                serde_json::from_value(record.metadata.clone())?,
            )?;
        }
    }
    for old in db.index_definitions() {
        let mut shadow = replacement(db, &old)?;
        if shadow == old {
            continue;
        }
        if marker.is_some() {
            return Err(unsupported(
                "legacy case index appeared after Oracle case readiness",
            ));
        }
        shadow.name = format!("__bicdb_oracle_case_{}", uuid::Uuid::new_v4().simple());
        let key = format!("{PREFIX}{}", old.name);
        let intent = Intent {
            format: 1,
            old,
            shadow,
        };
        db.insert(
            PROFILE_COLLECTION,
            Record::new(&key).with_metadata(serde_json::to_value(&intent)?),
        )?;
        finish(db, &key, intent)?;
    }
    if marker.is_none() {
        db.insert(
            PROFILE_COLLECTION,
            Record::new(simple_case::READY_ID).with_metadata(simple_case::ready_metadata()),
        )?;
    }
    db.check_recovery_state()?;
    Ok(())
}
