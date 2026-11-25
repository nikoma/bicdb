//! `pg_partition_tree`, `pg_partition_ancestors` and `pg_partition_root`.
use crate::*;

/// The partitioned tables and partitions, as (relation, parent).
fn partition_parents(db: &BicDb) -> Result<Vec<(String, Option<String>)>> {
    let mut relations = Vec::new();
    for schema in list_schemas(db)? {
        let parent = schema.partition_of.as_ref().map(|partition| {
            let qualified = if partition.parent_schema.is_empty()
                || partition.parent_schema == "public"
                || partition.parent_table.contains('.')
            {
                partition.parent_table.clone()
            } else {
                format!("{}.{}", partition.parent_schema, partition.parent_table)
            };
            resolve_session_relation_name_if_exists(db, &qualified)
        });
        if parent.is_some() || schema.partitioning.is_some() {
            relations.push((schema.name.clone(), parent));
        }
    }
    Ok(relations)
}

fn relation_from_value(db: &BicDb, value: &SqlValue) -> Option<String> {
    match value {
        SqlValue::Null => None,
        SqlValue::Int(oid) => table_oids(db)
            .iter()
            .find(|(_, table_oid)| **table_oid == *oid)
            .map(|(name, _)| name.clone()),
        other => sql_value_text(other)
            .map(|name| resolve_session_relation_name_if_exists(db, &normalize_object_name(&name))),
    }
}

fn regclass(name: &str) -> SqlValue {
    SqlValue::String(catalog_display_name(name))
}

/// `pg_partition_root(rel)`: the topmost partitioned table, NULL for a
/// relation that is neither partitioned nor a partition.
pub(crate) fn partition_root(db: &BicDb, value: &SqlValue) -> Result<SqlValue> {
    let Some(relation) = relation_from_value(db, value) else {
        return Ok(SqlValue::Null);
    };
    let relations = partition_parents(db)?;
    if !relations.iter().any(|(name, _)| *name == relation) {
        return Ok(SqlValue::Null);
    }
    let mut current = relation;
    for _ in 0..64 {
        match relations
            .iter()
            .find(|(name, _)| *name == current)
            .and_then(|(_, parent)| parent.clone())
        {
            Some(parent) => current = parent,
            None => break,
        }
    }
    Ok(regclass(&current))
}

/// Rows and columns of `pg_partition_tree(rel)` / `pg_partition_ancestors(rel)`.
pub(crate) fn table_function(
    db: &BicDb,
    function: &str,
    args: &[SqlValue],
) -> Result<Option<(Vec<String>, Vec<Vec<SqlValue>>)>> {
    let function = function.strip_prefix("pg_catalog.").unwrap_or(function);
    if !matches!(function, "pg_partition_tree" | "pg_partition_ancestors") {
        return Ok(None);
    }
    if args.len() != 1 {
        return Err(SqlError::undefined_function(format!(
            "function {function} expects one argument"
        )));
    }
    let relations = partition_parents(db)?;
    let relation = relation_from_value(db, &args[0]);
    let known = relation
        .as_ref()
        .filter(|relation| relations.iter().any(|(name, _)| name == *relation));
    if function == "pg_partition_ancestors" {
        let mut rows = Vec::new();
        if let Some(relation) = known {
            let mut current = Some(relation.clone());
            while let Some(name) = current {
                if rows.len() > 64 {
                    break;
                }
                rows.push(vec![regclass(&name)]);
                current = relations
                    .iter()
                    .find(|(candidate, _)| *candidate == name)
                    .and_then(|(_, parent)| parent.clone());
            }
        }
        return Ok(Some((vec!["relid".to_string()], rows)));
    }
    let mut rows = Vec::new();
    if let Some(root) = known {
        // Breadth first from the given relation, children in creation order.
        let mut queue = vec![(root.clone(), 0_i64)];
        let mut index = 0;
        while index < queue.len() && queue.len() <= 100_000 {
            let (name, level) = queue[index].clone();
            index += 1;
            let parent = relations
                .iter()
                .find(|(candidate, _)| *candidate == name)
                .and_then(|(_, parent)| parent.clone());
            let mut children = relations
                .iter()
                .filter(|(_, parent)| parent.as_deref() == Some(name.as_str()))
                .map(|(child, _)| child.clone())
                .collect::<Vec<_>>();
            // PostgreSQL lists siblings in OID (creation) order; relation
            // OIDs here are name-derived, so name order is the stable one.
            children.sort();
            let is_leaf =
                load_schema_shared(db, &name)?.is_some_and(|schema| schema.partitioning.is_none());
            rows.push(vec![
                regclass(&name),
                parent.map_or(SqlValue::Null, |parent| regclass(&parent)),
                SqlValue::Bool(is_leaf),
                SqlValue::Int(level),
            ]);
            queue.extend(children.into_iter().map(|child| (child, level + 1)));
        }
    }
    Ok(Some((
        vec![
            "relid".to_string(),
            "parentrelid".to_string(),
            "isleaf".to_string(),
            "level".to_string(),
        ],
        rows,
    )))
}
