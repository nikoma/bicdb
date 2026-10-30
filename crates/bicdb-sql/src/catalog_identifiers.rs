//! Read-only checks for SQL catalog identities, including legacy metadata.
use crate::*;

fn unique_oid(
    seen: &mut BTreeMap<i64, String>,
    catalog: &str,
    oid: i64,
    object: String,
) -> Result<()> {
    if oid == 0 || u32::try_from(oid).is_err() {
        return Err(SqlError::InvalidSql(format!(
            "SQL catalog {catalog} has invalid OID {oid} for {object}"
        )));
    }
    if let Some(previous) = seen.insert(oid, object.clone()) {
        return Err(SqlError::InvalidSql(format!(
            "SQL catalog {catalog} OID {oid} aliases {previous} and {object}"
        )));
    }
    Ok(())
}

/// Check SQL catalog OID bounds and uniqueness without evaluating stored SQL
/// or changing data. Physical checksums alone cannot detect identical bad
/// catalog identities on a primary and its restored/replicated copies.
pub fn verify_catalog_identifiers(db: &BicDb) -> Result<()> {
    // Historical SQL metadata must not publish a user relation over a native
    // catalog collection, even when normal catalog enumeration hides it.
    for schema in list_schemas(db)? {
        require_user_relation_name(&schema.name)?;
    }
    for view in list_views(db)? {
        require_user_relation_name(&view.name)?;
    }
    let schemas = relation_schemas(db)?;
    let user_types = list_user_types(db)?;
    let mut types = BTreeMap::new();
    let mut namespaces = [
        "public".into(),
        "pg_catalog".into(),
        "information_schema".into(),
    ]
    .into_iter()
    .collect::<BTreeSet<String>>();
    for namespace in list_namespaces(db)? {
        namespaces.insert(namespace.name);
    }
    for schema in &schemas {
        namespaces.insert(schema.schema_name.clone());
        let name = format!(
            "{}.{}",
            schema.schema_name,
            catalog_display_name(&schema.name)
        );
        unique_oid(&mut types, "pg_type", schema.row_type_oid(), name.clone())?;
        unique_oid(
            &mut types,
            "pg_type",
            schema.row_array_type_oid(),
            format!("{name}[]"),
        )?;
    }
    for user_type in &user_types {
        let name = format!("{}.{}", user_type.schema_name, user_type.name);
        unique_oid(&mut types, "pg_type", user_type.oid, name.clone())?;
        // A shell type deliberately has no array type yet.
        if user_type.array_oid != 0 {
            unique_oid(
                &mut types,
                "pg_type",
                user_type.array_oid,
                format!("{name}[]"),
            )?;
        }
        namespaces.insert(user_type.schema_name.clone());
    }
    let mut namespace_ids = BTreeMap::new();
    for namespace in namespaces {
        unique_oid(
            &mut namespace_ids,
            "pg_namespace",
            namespace_oid(&namespace),
            namespace,
        )?;
    }
    let table_oids = table_oids(db);
    let mut relations = BTreeMap::new();
    for (name, oid) in table_oids.iter() {
        unique_oid(&mut relations, "pg_class", *oid, format!("relation {name}"))?;
    }
    for (table, _) in primary_key_indexes(&schemas, db) {
        let oid = table_oids
            .get(&table)
            .ok_or_else(|| SqlError::InvalidSql(format!("missing relation OID for {table}")))?;
        unique_oid(
            &mut relations,
            "pg_class",
            primary_index_oid(*oid),
            format!("primary index of {table}"),
        )?;
    }
    for index in catalog_indexes(&schemas, db) {
        unique_oid(
            &mut relations,
            "pg_class",
            secondary_index_oid(&index.schema_name, &index.name),
            format!("index {}.{}", index.schema_name, index.name),
        )?;
    }
    for sequence in list_sequences(db)? {
        unique_oid(
            &mut relations,
            "pg_class",
            sequence_oid(&sequence.name),
            format!("sequence {}", sequence.name),
        )?;
    }
    for user_type in user_types {
        if let UserTypeKind::Composite { relation_oid, .. } = user_type.kind {
            unique_oid(
                &mut relations,
                "pg_class",
                relation_oid,
                format!("composite {}.{}", user_type.schema_name, user_type.name),
            )?;
        }
    }
    validate_constraint_catalog_oids(db)
}
