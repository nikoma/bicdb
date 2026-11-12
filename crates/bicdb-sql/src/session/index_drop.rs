//! Resolve index DDL in its namespace and protect constraint-owned backings.
use super::*;

impl<'db> SqlSession<'db> {
    fn resolve_index_for_drop(&self, name: &ObjectName) -> Result<Option<(String, String)>> {
        let parts = object_name_parts(name);
        let (logical, namespaces): (&str, Vec<String>) = match parts.as_slice() {
            [index] => (
                index,
                crate::search_path::active_path()
                    .map(|path| path.to_vec())
                    .unwrap_or_else(|| vec!["public".into()]),
            ),
            [namespace, index] => (index, vec![namespace.clone()]),
            _ => {
                return Err(SqlError::InvalidSql(format!(
                    "index name must have one or two identifiers: {name}"
                )))
            }
        };
        let schemas = list_schemas(self.db_ref())?;
        let views = list_views(self.db_ref())?;
        let definitions = self.db_ref().index_definitions();
        for namespace in namespaces {
            // CREATE INDEX may store a qualified index under the encoded
            // relation identity. Keep that exact identity for the later
            // removal; comparing only the visible last identifier misses it.
            let qualified = relation_name_from_parts(&[namespace.clone(), logical.into()])?;
            let identities = BTreeSet::from([logical.to_string(), qualified]);
            let mut candidates = BTreeSet::new();
            for schema in &schemas {
                let schema_namespace = if schema.schema_name.is_empty() {
                    "public"
                } else {
                    &schema.schema_name
                };
                if schema_namespace != namespace {
                    continue;
                }
                for identity in &identities {
                    let storage = sql_index_storage_name(&namespace, identity);
                    if (!primary_key_columns_for_schema(schema).is_empty()
                        && schema.primary_key_constraint_name() == *identity)
                        || schema.constraints.iter().any(|constraint| matches!(constraint,
                            ConstraintSchema::Unique { name, .. } | ConstraintSchema::UniqueWithNullPolicy { name, .. } | ConstraintSchema::Exclusion { name, .. } if name == identity))
                        || schema.indexes.iter().any(|index| &index.name == identity)
                        || definitions.iter().any(|index| index.name == storage && index.collection == schema.name)
                    { candidates.insert((identity.clone(), schema.name.clone())); }
                }
            }
            for view in &views {
                if relation_catalog_namespace(self.db_ref(), &view.name)? == namespace {
                    for identity in &identities {
                        let storage = sql_index_storage_name(&namespace, identity);
                        if view.indexes.iter().any(|index| &index.name == identity)
                            || definitions
                                .iter()
                                .any(|index| index.name == storage && index.collection == view.name)
                        {
                            candidates.insert((identity.clone(), view.name.clone()));
                        }
                    }
                }
            }
            // Native-only collections retain their existing public-namespace
            // SQL exposure. A namespace-qualified SQL relation must match its
            // actual schema/view above, never another namespace's index name.
            if namespace == "public" {
                for index in &definitions {
                    if index.name == sql_index_storage_name(&namespace, logical)
                        && !schemas.iter().any(|schema| schema.name == index.collection)
                        && !views.iter().any(|view| view.name == index.collection)
                    {
                        candidates.insert((logical.to_string(), index.collection.clone()));
                    }
                }
            }
            match candidates.len() {
                0 => {}
                1 => return Ok(candidates.into_iter().next()),
                _ => {
                    return Err(SqlError::InvalidSql(format!(
                        "index {name} has ambiguous owning relations"
                    )))
                }
            }
        }
        Ok(None)
    }

    pub(super) fn execute_drop_indexes(
        &mut self,
        names: &[ObjectName],
        if_exists: bool,
    ) -> Result<SqlResult> {
        let mut planned = BTreeSet::new();
        // Resolve and authorize the entire statement before removing any
        // Native executable or stored descriptor. IF EXISTS does not override
        // a dependency, and CASCADE cannot drop a constraint's own index.
        for name in names {
            let Some((index, table)) = self.resolve_index_for_drop(name)? else {
                if if_exists {
                    continue;
                }
                return Err(SqlError::InvalidSql(format!("index `{name}` not found")));
            };
            self.require_table_ownership(&table, "drop an index on it")?;
            if let Some(schema) = load_schema(self.db_ref(), &table)? {
                let primary = !primary_key_columns_for_schema(&schema).is_empty()
                    && schema.primary_key_constraint_name() == index;
                let key_owned = schema.constraints.iter().any(|constraint| matches!(constraint,
                    ConstraintSchema::Unique { name, .. } | ConstraintSchema::UniqueWithNullPolicy { name, .. } if name == &index));
                let exclusion_owned = schema.constraints.iter().any(|constraint| {
                    matches!(constraint,
                    ConstraintSchema::Exclusion { name, .. } if name == &index)
                });
                if (primary || key_owned) && oracle::is_oracle_database(self.db_ref())? {
                    return Err(oracle::errors::database_error(
                        -2429,
                        "2BP01",
                        "cannot drop index used for enforcement of unique/primary key",
                    ));
                }
                if primary || key_owned || exclusion_owned {
                    return Err(SqlError::dependent_objects_still_exist(format!("cannot drop index {index} because constraint {index} on table {} requires it", catalog_display_name(&table))));
                }
            }
            planned.insert((table, index));
        }
        for (table, index) in planned {
            let existed = self.drop_session_index(&index, Some(&table))?;
            if !existed && !if_exists {
                return Err(SqlError::InvalidSql(format!("index `{index}` not found")));
            }
        }
        Ok(SqlResult::command("DROP INDEX"))
    }
}
