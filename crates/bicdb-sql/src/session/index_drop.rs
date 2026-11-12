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
            let storage = sql_index_storage_name(&namespace, logical);
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
                if (!primary_key_columns_for_schema(schema).is_empty() && schema.primary_key_constraint_name() == logical)
                    || schema.constraints.iter().any(|constraint| matches!(constraint,
                        ConstraintSchema::Unique { name, .. } | ConstraintSchema::UniqueWithNullPolicy { name, .. } | ConstraintSchema::Exclusion { name, .. } if name == logical))
                    || schema.indexes.iter().any(|index| index.name == logical)
                    || definitions.iter().any(|index| index.name == storage && index.collection == schema.name)
                { candidates.insert(schema.name.clone()); }
            }
            for view in &views {
                if relation_catalog_namespace(self.db_ref(), &view.name)? == namespace
                    && (view.indexes.iter().any(|index| index.name == logical)
                        || definitions
                            .iter()
                            .any(|index| index.name == storage && index.collection == view.name))
                {
                    candidates.insert(view.name.clone());
                }
            }
            // Native-only collections retain their existing public-namespace
            // SQL exposure. A namespace-qualified SQL relation must match its
            // actual schema/view above, never another namespace's index name.
            if namespace == "public" {
                for index in &definitions {
                    if index.name == storage
                        && !schemas.iter().any(|schema| schema.name == index.collection)
                        && !views.iter().any(|view| view.name == index.collection)
                    {
                        candidates.insert(index.collection.clone());
                    }
                }
            }
            match candidates.len() {
                0 => {}
                1 => {
                    return Ok(Some((
                        logical.to_string(),
                        candidates.into_iter().next().unwrap(),
                    )))
                }
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
                let owned = schema.constraints.iter().any(|constraint| matches!(constraint,
                    ConstraintSchema::Unique { name, .. } | ConstraintSchema::UniqueWithNullPolicy { name, .. } | ConstraintSchema::Exclusion { name, .. } if name == &index));
                if primary || owned {
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
