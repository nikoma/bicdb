//! Indexes on partitioned tables, kept as PostgreSQL keeps them: an index on
//! a partitioned table has one attached index on every partition (created,
//! or an equivalent existing one adopted), new and attached partitions get
//! theirs, an attached index cannot be dropped on its own and dropping the
//! parent's index drops the whole tree.
use super::*;

impl<'db> SqlSession<'db> {
    pub(crate) fn execute_create_index(&mut self, create_index: &CreateIndex) -> Result<SqlResult> {
        let table = relation_name(&create_index.table_name)?;
        let Some(schema) = load_schema(self.db_ref(), &table)? else {
            return self.execute_create_index_on_relation(create_index);
        };
        if schema.partitioning.is_none() {
            return self.execute_create_index_on_relation(create_index);
        }
        self.require_table_ownership(&table, "create an index on it")?;
        if create_index.unique {
            let columns = create_index
                .columns
                .iter()
                .map(|column| match &column.column.expr {
                    Expr::Identifier(ident) => normalize_ident(ident),
                    // An expression column never covers a key column.
                    other => format!("\u{0}{other}"),
                })
                .collect::<Vec<_>>();
            partition::unique_covers_key(&schema, "UNIQUE", &columns)?;
        }
        let before = schema
            .indexes
            .iter()
            .map(|index| index.name.clone())
            .collect::<BTreeSet<_>>();
        let result = self.execute_create_index_on_relation(create_index)?;
        let schema = load_schema(self.db_ref(), &table)?
            .ok_or_else(|| SqlError::InvalidCollection(table.clone()))?;
        // IF NOT EXISTS on an existing name creates nothing to propagate.
        let Some(index) = schema
            .indexes
            .iter()
            .find(|index| !before.contains(&index.name))
            .cloned()
        else {
            return Ok(result);
        };
        for child in partition::children(self.db_ref(), &schema)? {
            self.attach_partition_index(&child, &index)?;
        }
        Ok(result)
    }

    /// Give a new or newly attached partition every index of its parent.
    pub(crate) fn inherit_partition_indexes(
        &mut self,
        child: &str,
        parent: &TableSchema,
    ) -> Result<()> {
        for index in parent
            .indexes
            .iter()
            .filter(|index| !index.metadata_only)
            .cloned()
            .collect::<Vec<_>>()
        {
            let child = load_schema(self.db_ref(), child)?
                .ok_or_else(|| SqlError::InvalidCollection(child.to_string()))?;
            self.attach_partition_index(&child, &index)?;
        }
        Ok(())
    }

    /// Attach `parent_index` to `child`: adopt an equivalent unattached index
    /// (PostgreSQL's ATTACH PARTITION matching), else build one named as
    /// PostgreSQL's ChooseIndexName would. A partitioned child recurses.
    fn attach_partition_index(
        &mut self,
        child: &TableSchema,
        parent_index: &IndexSchema,
    ) -> Result<()> {
        if let Some(existing) = child.indexes.iter().find(|index| {
            index.partition_parent.is_none()
                && index.expression == parent_index.expression
                && index.unique == parent_index.unique
                && index.access_method == parent_index.access_method
                && index.predicate == parent_index.predicate
        }) {
            let name = existing.name.clone();
            let mut updated = child.clone();
            for index in &mut updated.indexes {
                if index.name == name {
                    index.partition_parent = Some(parent_index.name.clone());
                }
            }
            self.save_session_schema(&updated)?;
            if updated.partitioning.is_some() {
                let index = updated
                    .indexes
                    .iter()
                    .find(|index| index.name == name)
                    .cloned()
                    .expect("adopted index");
                for grandchild in partition::children(self.db_ref(), &updated)? {
                    if !grandchild
                        .indexes
                        .iter()
                        .any(|candidate| candidate.partition_parent.as_deref() == Some(&name))
                    {
                        self.attach_partition_index(&grandchild, &index)?;
                    }
                }
            }
            return Ok(());
        }
        let name = self.choose_partition_index_name(child, parent_index)?;
        let sql = format!(
            "CREATE {}INDEX {} ON {} USING {} ({}){}",
            if parent_index.unique { "UNIQUE " } else { "" },
            quote_identifier_if_needed(&name),
            relation_sql_name(child),
            parent_index.access_method,
            parent_index.expression,
            parent_index
                .predicate
                .as_ref()
                .map(|predicate| format!(" WHERE {predicate}"))
                .unwrap_or_default()
        );
        let statement = Parser::parse_sql(&PostgreSqlDialect {}, &sql)
            .map_err(|error| SqlError::InvalidSql(error.to_string()))?;
        let [Statement::CreateIndex(create_index)] = statement.as_slice() else {
            return Err(SqlError::InvalidSql(format!(
                "cannot rebuild partition index from {sql}"
            )));
        };
        self.execute_create_index(create_index)?;
        let mut updated = load_schema(self.db_ref(), &child.name)?
            .ok_or_else(|| SqlError::InvalidCollection(child.name.clone()))?;
        for index in &mut updated.indexes {
            if index.name == name {
                index.partition_parent = Some(parent_index.name.clone());
            }
        }
        self.save_session_schema(&updated)
    }

    /// PostgreSQL's ChooseIndexName for a partition's index:
    /// `<table>_<columns>_idx`, numbered on collision.
    fn choose_partition_index_name(
        &self,
        child: &TableSchema,
        parent_index: &IndexSchema,
    ) -> Result<String> {
        self.choose_default_index_name(child, &parent_index.expression)
    }

    /// The name PostgreSQL gives an unnamed index of `schema` on
    /// `expression` (its column list): `<table>_<columns>_idx`, numbered on
    /// collision with any relation or index name.
    pub(crate) fn choose_default_index_name(
        &self,
        schema: &TableSchema,
        expression: &str,
    ) -> Result<String> {
        let table = catalog_display_name(&schema.name);
        let table = table.rsplit('.').next().unwrap_or(&table).to_string();
        let columns = index_name_columns(expression);
        let base = truncate_identifier(&format!("{table}_{columns}"), 63 - "_idx".len());
        let mut taken = BTreeSet::new();
        for schema in list_schemas(self.db_ref())? {
            taken.insert(schema.name.clone());
            taken.insert(schema.primary_key_constraint_name());
            for index in schema.indexes {
                taken.insert(index.name);
            }
        }
        for definition in self.db_ref().index_definitions() {
            taken.insert(definition.name.clone());
        }
        let mut candidate = format!("{base}_idx");
        let mut suffix = 0;
        while taken.contains(&candidate) {
            suffix += 1;
            candidate = format!("{base}_idx{suffix}");
        }
        Ok(candidate)
    }

    /// DROP INDEX rules for partition indexes: an attached one is required by
    /// its parent (2BP01); dropping a parent's index drops its descendants.
    pub(crate) fn partition_index_drop_plan(
        &self,
        table: &str,
        index: &str,
    ) -> Result<Vec<(String, String)>> {
        let Some(schema) = load_schema(self.db_ref(), table)? else {
            return Ok(Vec::new());
        };
        if let Some(parent) = schema
            .indexes
            .iter()
            .find(|candidate| candidate.name == index)
            .and_then(|candidate| candidate.partition_parent.clone())
        {
            return Err(SqlError::dependent_objects_still_exist(format!(
                "cannot drop index {index} because index {parent} requires it"
            )));
        }
        let mut out = Vec::new();
        let mut pending = vec![(schema, index.to_string())];
        while let Some((parent, name)) = pending.pop() {
            if parent.partitioning.is_none() {
                continue;
            }
            for child in partition::children(self.db_ref(), &parent)? {
                if let Some(found) = child
                    .indexes
                    .iter()
                    .find(|candidate| candidate.partition_parent.as_deref() == Some(&name))
                {
                    out.push((child.name.clone(), found.name.clone()));
                    pending.push((child.clone(), found.name.clone()));
                }
            }
        }
        Ok(out)
    }
}

fn normalize_ident(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}

/// ChooseIndexColumnNames: a column keeps its name, an expression is named
/// by its function (`lower`) or `expr`, joined with `_`.
fn index_name_columns(expression: &str) -> String {
    let parsed = Parser::new(&PostgreSqlDialect {})
        .try_with_sql(expression)
        .and_then(|mut parser| parser.parse_comma_separated(Parser::parse_order_by_expr));
    let Ok(columns) = parsed else {
        return "expr".into();
    };
    columns
        .iter()
        .map(|column| match &column.expr {
            Expr::Identifier(ident) => normalize_ident(ident),
            Expr::Function(function) => object_name(&function.name)
                .map(|name| {
                    name.rsplit('.')
                        .next()
                        .unwrap_or(&name)
                        .to_ascii_lowercase()
                })
                .unwrap_or_else(|_| "expr".into()),
            _ => "expr".into(),
        })
        .collect::<Vec<_>>()
        .join("_")
}

fn truncate_identifier(value: &str, max: usize) -> String {
    let mut out = String::new();
    for ch in value.chars() {
        if out.len() + ch.len_utf8() > max {
            break;
        }
        out.push(ch);
    }
    out
}

fn quote_identifier_if_needed(name: &str) -> String {
    pg_partition_key_identifier(name)
}

/// The SQL name of a stored relation, schema-qualified outside `public`.
fn relation_sql_name(schema: &TableSchema) -> String {
    let display = catalog_display_name(&schema.name);
    let local = display.rsplit('.').next().unwrap_or(&display).to_string();
    if schema.schema_name.is_empty() || schema.schema_name == "public" {
        pg_partition_key_identifier(&local)
    } else {
        format!(
            "{}.{}",
            pg_partition_key_identifier(&schema.schema_name),
            pg_partition_key_identifier(&local)
        )
    }
}

impl<'db> SqlSession<'db> {
    /// CHECK constraints added to a partitioned table hold on every
    /// partition, as PostgreSQL adds them there (validating existing rows of
    /// each partition unless NOT VALID). Other constraint kinds keep their
    /// own propagation rules.
    pub(crate) fn propagate_partition_checks(
        &mut self,
        parent: &TableSchema,
        added: &[ConstraintSchema],
        not_valid: bool,
    ) -> Result<()> {
        let checks = added
            .iter()
            .filter(|constraint| matches!(constraint, ConstraintSchema::Check { .. }))
            .cloned()
            .collect::<Vec<_>>();
        if checks.is_empty() {
            return Ok(());
        }
        for mut child in partition::physical_tables(self.db_ref(), parent)?
            .into_iter()
            .skip(1)
        {
            self.capture_table_state_undo(&child.name.clone(), &child)?;
            for constraint in &checks {
                let name = constraint_name(constraint);
                if child
                    .constraints
                    .iter()
                    .any(|existing| constraint_name(existing) == name)
                {
                    continue;
                }
                if !not_valid {
                    validate_existing_constraint(self.db_ref(), &child.name, &child, constraint)?;
                }
                child.constraints.push(constraint.clone());
            }
            self.save_session_schema(&child)?;
        }
        Ok(())
    }
}
