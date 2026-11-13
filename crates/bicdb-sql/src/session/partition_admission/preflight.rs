//! Stream the complete proposed ACK set without trusting historical indexes.
//! No row/catalog/fence write and no Native authority mint occurs here.
use super::*;

type Schemas = BTreeMap<String, TableSchema>;
type Relocations = BTreeMap<(String, String), String>;

fn effective_schemas(proposed: &[TableSchema]) -> Result<Schemas> {
    let mut out = Schemas::new();
    while out.len() < proposed.len() {
        let before = out.len();
        for schema in proposed {
            if out.contains_key(&schema.name) {
                continue;
            }
            let effective = if let Some(link) = &schema.partition_of {
                let Some(parent) = out.get(&link.parent_table) else {
                    continue;
                };
                schema_with_parent_inheritance(schema.clone(), parent)
            } else {
                schema.clone()
            };
            out.insert(schema.name.clone(), effective);
        }
        if out.len() == before {
            return Err(required("missing or cyclic proposed partition ancestry"));
        }
    }
    Ok(out)
}

fn belongs_to<'a>(schemas: &'a Schemas, mut name: &'a str, ancestor: &str) -> bool {
    loop {
        if name == ancestor {
            return true;
        }
        let Some(link) = schemas
            .get(name)
            .and_then(|schema| schema.partition_of.as_ref())
        else {
            return false;
        };
        name = &link.parent_table;
    }
}

fn visit_rows(
    db: &BicDb,
    snapshot: &Snapshot,
    schemas: &Schemas,
    relocations: &Relocations,
    mut visit: impl FnMut(&TableSchema, &Record, bool) -> Result<()>,
) -> Result<()> {
    for source in snapshot.nodes.values() {
        scan(db, &source.schema.name, |mut row| {
            let destination = planned_destination(db, snapshot, source, &row, relocations)?;
            let moved = destination.is_some();
            let target = destination.as_deref().unwrap_or(&source.schema.name);
            let schema = schemas
                .get(target)
                .ok_or_else(|| required("missing proposed ACK destination"))?;
            if schema.partitioning.is_some() {
                return Err(required("proposed ACK remains in a virtual parent heap"));
            }
            // Changing which column falls back to the Native ID must never
            // reinterpret an acknowledged visible value. Reconciliation is an
            // explicit operator action, not an invented rekey or dropped row.
            for column in source.schema.columns.iter().filter(|column| !column.hidden) {
                let before = record_column_value(&row, &source.schema, &column.name);
                let after = record_column_value(&row, schema, &column.name);
                if !typed_column_values_not_distinct(
                    schema,
                    std::slice::from_ref(&column.name),
                    &[before],
                    &[after],
                )? {
                    return Err(required(format!("ACK {}/{} would change column {} interpretation; reconcile its physical key explicitly",source.schema.name,row.id,column.name)));
                }
            }
            partition::materialize_primary_key_fields(&source.schema, &mut row)?;
            visit(schema, &row, moved)
        })?;
    }
    Ok(())
}

pub(super) fn validate(
    db: &BicDb,
    snapshot: &Snapshot,
    proposed: &[TableSchema],
    relocations: &Relocations,
) -> Result<()> {
    let schemas = effective_schemas(proposed)?;
    // SQL NOT VALID permits existing rows to violate CHECK/FK constraints.
    // Moving a row still requires the destination's ordinary write checks.
    let stationary = schemas
        .iter()
        .map(|(name, schema)| {
            let mut schema = schema.clone();
            schema.constraints.retain(|constraint| {
                !matches!(
                    constraint,
                    ConstraintSchema::Check {
                        validated: false,
                        ..
                    }
                )
            });
            (name.clone(), schema)
        })
        .collect::<Schemas>();
    let arbiters = schemas
        .iter()
        .map(|(name, schema)| {
            (
                name.clone(),
                unique_arbiters_for_table_uncached(db, name, schema),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut seen = BTreeSet::new();
    let mut retained_bytes = 0usize;
    let engine = SqlEngine::new(db);
    visit_rows(db,snapshot,&schemas,relocations, |schema,row,moved| {
        let checked = if moved {schema} else {&stationary[&schema.name]};
        validate_record_local_constraints(&schema.name,checked,row,Some(&engine))?;
        partition::validate_record(db,schema,row)?;
        if !schema.has_hidden_primary_key() {
            let columns = primary_key_columns_for_schema(schema);
            let values = record_column_values(row,schema,&columns);
            let canonical = record_id_from_column_values(&schema.name,schema,&columns,&values)?;
            if canonical != row.id {
                return Err(required(format!("ACK {}/{} does not match its declared physical primary key; reconcile explicitly",schema.name,row.id)));
            }
        }
        for (index,arbiter) in arbiters[&schema.name].iter().enumerate() {
            let values = unique_key_values(row,schema,&arbiter.key);
            if arbiter.skips_null_key(&values) || !arbiter.applies_to(row)? { continue; }
            let key = typed_unique_key_label(schema,&arbiter.key,&values)?;
            retained_bytes = retained_bytes.saturating_add(schema.name.len()).saturating_add(key.iter().map(String::len).sum::<usize>()).saturating_add(key.len().saturating_mul(32)).saturating_add(192);
            if retained_bytes > db.query_work_memory_bytes() {
                return Err(required("complete ACK uniqueness plan exceeds configured work memory"));
            }
            if !seen.insert((schema.name.clone(),index,key)) {
                return Err(unique_violation(schema,&arbiter.name));
            }
        }
        for constraint in &schema.constraints {
            match constraint {
                ConstraintSchema::ForeignKey {columns,foreign_table,referred_columns,validated,..}
                    if !schema.referential_checks_disabled && (moved || *validated) => {
                    let key = record_column_values(row,schema,columns);
                    if key.iter().any(|value| matches!(value,SqlValue::Null)) { continue; }
                    let foreign = match schemas.get(foreign_table) {
                        Some(schema) => schema.clone(),
                        None => load_schema(db,foreign_table)?.ok_or_else(|| required("missing referenced relation in ACK preflight"))?,
                    };
                    let mut found = false;
                    let mut consider = |candidate: &Record| -> Result<()> {
                        let candidate_key = record_column_values(candidate,&foreign,referred_columns);
                        found |= typed_column_values_not_distinct(&foreign,referred_columns,&candidate_key,&key)?;
                        Ok(())
                    };
                    if schemas.contains_key(foreign_table) {
                        visit_rows(db,snapshot,&schemas,relocations,|target,candidate,_| {
                            if belongs_to(&schemas,&target.name,foreign_table) { consider(candidate)?; }
                            Ok(())
                        })?;
                    } else {
                        scan(db,foreign_table,|candidate| consider(&candidate))?;
                    }
                    if !found {return Err(required(format!("ACK {}/{} violates foreign key {}",schema.name,row.id,constraint_name(constraint))));}
                }
                ConstraintSchema::Exclusion {name,equal_columns,range,predicate,..} => {
                    let predicate = exclusion_predicate_expr(predicate)?;
                    visit_rows(db,snapshot,&schemas,relocations,|target,other,_| {
                        if target.name == schema.name && row.id != other.id
                            && exclusion_record_pair_conflicts(schema,equal_columns,range,predicate.as_ref(),row,other)? {
                            return Err(exclusion_violation(&schema.name,name));
                        }
                        Ok(())
                    })?;
                }
                _ => {}
            }
        }
        Ok(())
    }).map_err(|error| required(format!("complete proposed ACK validation failed: {error}")))
}
