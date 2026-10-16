//! Logical application copies keep only public SQL state and owned counters.
use super::*;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ApplicationCopyReport {
    pub tables: usize,
    pub records: u64,
    pub sequences: usize,
}

/// Copy public application tables into an unpublished database with no public
/// tables. The host must quiesce the source across this call. This executes no
/// migration, trigger or application code. Roles, grants, provider settings,
/// routines, events and database identity are never copied. A failure may leave
/// the target partially populated, so callers must publish it atomically only
/// after successful validation.
pub fn copy_application_tables(
    source: &BicDb,
    target: &mut BicDb,
) -> Result<ApplicationCopyReport> {
    let invalid = |message: &str| SqlError::Unsupported(message.to_owned());
    if target
        .collections()
        .iter()
        .any(|table| !table.name.starts_with("__bic"))
    {
        return Err(invalid(
            "application copy requires a target without public tables",
        ));
    }
    for catalog in [
        USER_TYPE_COLLECTION,
        ROUTINE_COLLECTION,
        TRIGGER_COLLECTION,
        VIEW_COLLECTION,
    ] {
        if source
            .collections()
            .iter()
            .any(|table| table.name == catalog)
            && !source
                .scan_collection_batch_after(catalog, None, 1, 8 * 1024 * 1024)?
                .is_empty()
        {
            return Err(invalid("application copies require tables without user type, routine, trigger or view dependencies"));
        }
    }
    let mut schemas = Vec::new();
    let mut collections = Vec::new();
    for table in source
        .collections()
        .into_iter()
        .filter(|table| !table.name.starts_with("__bic"))
    {
        if table.policy.is_some() || table.mutation_policy.is_some() {
            return Err(invalid(
                "protected collections require a separate authorized export",
            ));
        }
        let schema = load_schema_raw(source, &table.name)?.ok_or_else(|| {
            invalid("application copies require a SQL schema for each public collection")
        })?;
        if schema.name != table.name
            || schema.schema_name != "public"
            || schema.rls_enabled
            || schema.rls_forced
            || !schema.policies.is_empty()
            || schema.partitioning.is_some()
            || schema.partition_of.is_some()
            || schema
                .owner
                .as_deref()
                .is_some_and(|owner| owner != BOOTSTRAP_ROLE_NAME)
        {
            return Err(invalid("application copies require unpartitioned public tables owned by the application without row policies"));
        }
        schemas.push(schema);
        collections.push(table);
    }
    let names = collections
        .iter()
        .map(|table| table.name.clone())
        .collect::<BTreeSet<_>>();
    for schema in &schemas {
        for constraint in &schema.constraints {
            if let ConstraintSchema::ForeignKey { foreign_table, .. } = constraint {
                let table = foreign_table
                    .strip_prefix("public.")
                    .unwrap_or(foreign_table);
                if !names.contains(table) {
                    return Err(invalid(
                        "application copy foreign keys must remain inside the copied public tables",
                    ));
                }
            }
        }
    }
    let sequences = list_sequences(source)?
        .into_iter()
        .filter(|sequence| {
            !sequence.name.starts_with("__bic")
                && !sequence.name.contains('.')
                && sequence.owner == BOOTSTRAP_ROLE_NAME
                && sequence
                    .owned_by_table
                    .as_ref()
                    .is_none_or(|table| names.contains(table))
        })
        .collect::<Vec<_>>();
    let sequence_names = sequences
        .iter()
        .map(|sequence| sequence.name.as_str())
        .collect::<BTreeSet<_>>();
    for schema in &schemas {
        if schema
            .columns
            .iter()
            .filter_map(|column| column.default_sequence.as_deref())
            .any(|sequence| !sequence_names.contains(sequence))
        {
            return Err(invalid(
                "application copy sequences must remain inside the public application schema",
            ));
        }
    }
    for table in &collections {
        target.create_collection_with_mode(&table.name, table.mode.clone())?;
    }
    for schema in &schemas {
        save_schema(target, schema)?;
    }
    for sequence in &sequences {
        save_sequence(target, sequence)?;
    }
    let mut report = ApplicationCopyReport {
        tables: schemas.len(),
        sequences: sequences.len(),
        records: 0,
    };
    for table in &collections {
        report.records += source.copy_unprotected_collection_to(&table.name, target)?;
    }
    // All data arrives before indexes are rebuilt, so foreign-key cycles and
    // insertion order cannot execute SQL effects while copying the snapshot.
    for index in source
        .index_definitions()
        .into_iter()
        .filter(|index| names.contains(&index.collection))
    {
        target.create_index(index)?;
    }
    for table in &collections {
        if let Some(configuration) = source.vector_index_config(&table.name) {
            target.create_vector_index(&table.name, configuration)?;
        }
    }
    target.flush()?;
    Ok(report)
}
